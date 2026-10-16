//! Backup and restore against real SQLite and filesystem stores, in process
//! so the moment between the metadata snapshot and the blob copy can be
//! controlled (ADR 0026). The e2e crate drives the real binaries.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use leep_core::{
    BlobStore, Changeset, ChangesetId, FileChange, FileKind, Hash, RefUpdateContext, RepoPath,
};
use leep_derive::gc::{self, Clock, GcOptions};
use leep_derive::integrity::Quiet;
use leep_derive::Repo;
use leep_meta::snapshot::SnapshotInfo;
use leep_meta::SchemaMode;
use leep_server::backup::{
    self, BackupOptions, Hooks, NoHooks, RestoreOptions, VerifyBackupOptions,
};

struct Ahead;

#[async_trait]
impl Clock for Ahead {
    fn now(&self) -> i64 {
        gc::SystemClock.now() + 60
    }
    async fn sleep(&self, _: Duration) {}
}

struct Site {
    dir: PathBuf,
    meta_url: String,
    blobs_url: String,
    repo: Repo,
}

impl Site {
    async fn new(root: &Path, name: &str) -> Site {
        let dir = root.join(name);
        std::fs::create_dir_all(dir.join("blobs")).unwrap();
        let meta_url = leep_meta::snapshot::sqlite_file_url(&dir.join("meta.db"), true);
        let blobs_url = format!("file://{}", dir.join("blobs").display());
        let repo = Repo::new(
            leep_store::open_blob_store(&blobs_url).unwrap(),
            leep_meta::open_meta_store(&meta_url).await.unwrap(),
        );
        Site {
            dir,
            meta_url,
            blobs_url,
            repo,
        }
    }

    async fn commit(
        &self,
        parents: &[ChangesetId],
        files: &[(&str, &str)],
        msg: &str,
    ) -> ChangesetId {
        let mut changes = BTreeMap::new();
        for (p, text) in files {
            let content = self
                .repo
                .put_blob(Bytes::copy_from_slice(text.as_bytes()))
                .await
                .unwrap();
            changes.insert(
                RepoPath::new(*p).unwrap(),
                FileChange::Modify {
                    content,
                    kind: FileKind::Regular,
                    size: text.len() as u64,
                },
            );
        }
        self.repo
            .create_changeset(Changeset {
                parents: parents.to_vec(),
                author: "t".into(),
                timestamp: 1,
                message: msg.into(),
                changes,
                extra: BTreeMap::new(),
            })
            .await
            .unwrap()
    }

    async fn advance(&self, name: &str, to: ChangesetId) {
        let cur = self.repo.meta().get_ref(name).await.unwrap();
        assert!(self
            .repo
            .meta()
            .cas_ref_logged(name, cur, Some(to), &RefUpdateContext::new("land"))
            .await
            .unwrap());
    }

    /// Landed history, an unreachable changeset with blobs of its own, a tag,
    /// derived rows, and orphan blobs that GC may delete.
    async fn populate(&self) -> (ChangesetId, ChangesetId, Vec<Hash>) {
        let a = self
            .commit(
                &[],
                &[("src/a.rs", "fn a() {}\n"), ("README", "hello\n")],
                "a",
            )
            .await;
        self.advance("main", a).await;
        let b = self.commit(&[a], &[("src/b.rs", "fn b() {}\n")], "b").await;
        self.advance("main", b).await;
        let side = self
            .commit(
                &[a],
                &[("only/in/side.txt", "unreachable but stored\n")],
                "side",
            )
            .await;
        self.advance("tags/v1", a).await;
        self.repo.derive_search(b).await.unwrap();
        let mut orphans = Vec::new();
        for i in 0..5 {
            orphans.push(
                self.repo
                    .put_blob(Bytes::from(format!("orphan {i}")))
                    .await
                    .unwrap(),
            );
        }
        (b, side, orphans)
    }
}

fn opts(site: &Site, out: &Path) -> BackupOptions {
    BackupOptions {
        meta_url: site.meta_url.clone(),
        source_blobs: leep_store::open_blob_store(&site.blobs_url).unwrap(),
        out: out.to_path_buf(),
        dest_blobs: Some(
            leep_store::open_blob_store(&format!("file://{}", out.join("blobs").display()))
                .unwrap(),
        ),
        dest_blobs_location: out.join("blobs").display().to_string(),
        check: true,
        rehash: true,
        concurrency: 4,
        max_problems: 100,
        name: None,
    }
}

fn quiet() -> Arc<dyn leep_derive::integrity::Progress> {
    Arc::new(Quiet)
}

fn restore_opts(from: &Path, dest: &Site) -> RestoreOptions {
    RestoreOptions {
        from: from.to_path_buf(),
        snapshot: None,
        meta_url: dest.meta_url.clone(),
        meta_mode: SchemaMode::Migrate,
        target_blobs: leep_store::open_blob_store(&dest.blobs_url).unwrap(),
        source_blobs: Some(
            leep_store::open_blob_store(&format!("file://{}", from.join("blobs").display()))
                .unwrap(),
        ),
        force: false,
        resume: false,
        accept_failed_check: false,
        check: true,
        rehash: true,
        concurrency: 4,
        max_problems: 100,
    }
}

/// What garbage collection does between the two halves of a backup: land
/// more work (new blobs, a new tip), then condemn and collect everything
/// unreferenced with no waiting.
struct GcBetween {
    site: Arc<Site>,
    tip: ChangesetId,
}

#[async_trait]
impl Hooks for GcBetween {
    async fn after_meta_snapshot(&self, _: &SnapshotInfo) {
        let c = self
            .site
            .commit(
                &[self.tip],
                &[("after/snapshot.txt", "landed after the snapshot\n")],
                "late",
            )
            .await;
        self.site.advance("main", c).await;
        let o = GcOptions {
            grace: Duration::ZERO,
            fence: Duration::ZERO,
            unsafe_no_fence: true,
            dry_run: false,
        };
        gc::condemn(&self.site.repo, &o, &Ahead).await.unwrap();
        let r = gc::collect(&self.site.repo, &o, &Ahead).await.unwrap();
        assert!(r.deleted >= 5, "gc should have deleted the orphans: {r:?}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gc_between_the_halves_cannot_break_a_backup() {
    let root = tempfile::tempdir().unwrap();
    let site = Arc::new(Site::new(root.path(), "src").await);
    let (tip, side, orphans) = site.populate().await;
    let out = root.path().join("backup");

    let outcome = backup::backup(
        opts(&site, &out),
        &GcBetween {
            site: site.clone(),
            tip,
        },
        quiet(),
    )
    .await
    .unwrap();
    assert!(outcome.ok(), "{:?}", outcome.problems);
    let m = &outcome.manifest;
    assert_eq!(m.check.mode, "closure+rehash");
    assert_eq!(m.counts.changesets, 3); // a, b, side: nothing from after the snapshot
    assert_eq!(m.refs["main"], tip.0.to_hex());
    // The orphans were deleted before the copy listed the source.
    for o in &orphans {
        assert!(!out
            .join("blobs")
            .join(&o.to_hex()[..2])
            .join(&o.to_hex()[2..])
            .exists());
    }
    // The unreachable changeset's blob is in the backup.
    let side_tree = site.repo.root_tree(side).await.unwrap();
    assert!(
        leep_store::open_blob_store(&format!("file://{}", out.join("blobs").display()))
            .unwrap()
            .contains(&side_tree)
            .await
            .unwrap()
    );

    // Restore, and compare with the source as of the snapshot.
    let dest = Site::new(root.path(), "restored").await;
    let restored = backup::restore(restore_opts(&out, &dest), quiet())
        .await
        .unwrap();
    assert!(
        restored.report.as_ref().unwrap().ok(),
        "{:?}",
        restored.report.unwrap().problems
    );
    assert_eq!(restored.imported.changesets, 3);
    assert_eq!(dest.repo.meta().get_ref("main").await.unwrap(), Some(tip));
    assert!(dest.repo.meta().get_ref("tags/v1").await.unwrap().is_some());
    assert_eq!(dest.repo.list_files(tip, "").await.unwrap().len(), 3);
    assert_eq!(
        dest.repo
            .read_file(side, &RepoPath::new("only/in/side.txt").unwrap())
            .await
            .unwrap()
            .unwrap()
            .1,
        "unreachable but stored\n"
    );
    // The source moved on; the restore is the snapshot, not the source.
    assert_ne!(site.repo.meta().get_ref("main").await.unwrap(), Some(tip));
    assert!(dest.dir.join("meta.db").exists());
}

/// Deletes a blob a changeset in the snapshot references, as a GC bug or an
/// operator running `gc --unsafe-no-fence` under a live server could.
struct LoseReferenced {
    blobs: Arc<dyn BlobStore>,
    victim: Hash,
}

#[async_trait]
impl Hooks for LoseReferenced {
    async fn after_meta_snapshot(&self, _: &SnapshotInfo) {
        self.blobs.delete(&self.victim).await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_referenced_blob_lost_between_the_halves_fails_the_check_and_blocks_restore() {
    let root = tempfile::tempdir().unwrap();
    let site = Site::new(root.path(), "src").await;
    let (tip, _, _) = site.populate().await;
    let out = root.path().join("backup");
    let victim = Hash::of_blob(b"fn b() {}\n");
    assert!(site.repo.blobs().contains(&victim).await.unwrap());

    let outcome = backup::backup(
        opts(&site, &out),
        &LoseReferenced {
            blobs: site.repo.blobs().clone(),
            victim,
        },
        quiet(),
    )
    .await
    .unwrap();
    assert!(!outcome.ok());
    assert!(
        outcome.problems.iter().any(|p| p.code == "missing_blob"),
        "{:?}",
        outcome.problems
    );
    assert!(!outcome.manifest.check.ok);
    assert!(outcome.manifest.check.problems >= 1);
    let _ = tip;

    let dest = Site::new(root.path(), "restored").await;
    let err = backup::restore(restore_opts(&out, &dest), quiet())
        .await
        .err()
        .expect("restore must refuse")
        .to_string();
    assert!(err.contains("closure check failed"), "{err}");
    // Nothing was written.
    assert!(dest.repo.meta().list_refs().await.unwrap().is_empty());
    // Explicitly accepted, it restores and the restore's own check reports it.
    let mut o = restore_opts(&out, &dest);
    o.accept_failed_check = true;
    let r = backup::restore(o, quiet()).await.unwrap();
    assert!(!r.report.unwrap().ok());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn backups_are_incremental_and_snapshots_independent() {
    let root = tempfile::tempdir().unwrap();
    let site = Site::new(root.path(), "src").await;
    let (tip, _, _) = site.populate().await;
    let out = root.path().join("backup");

    let mut o1 = opts(&site, &out);
    o1.name = Some("20260101T000000Z".into());
    let first = backup::backup(o1, &NoHooks, quiet()).await.unwrap();
    assert!(first.ok());
    assert!(first.manifest.blobs.copied > 0);
    assert_eq!(first.manifest.blobs.already_present, 0);

    let c = site.commit(&[tip], &[("new.txt", "new\n")], "c").await;
    site.advance("main", c).await;
    let mut o2 = opts(&site, &out);
    o2.name = Some("20260102T000000Z".into());
    let second = backup::backup(o2, &NoHooks, quiet()).await.unwrap();
    assert!(second.ok());
    // Only the new blob and tree nodes crossed; everything else was skipped.
    assert!(second.manifest.blobs.copied < first.manifest.blobs.copied);
    assert_eq!(
        second.manifest.blobs.already_present + second.manifest.blobs.copied,
        second.manifest.blobs.listed
    );
    // A name that exists is refused rather than overwritten.
    let mut o3 = opts(&site, &out);
    o3.name = Some("20260102T000000Z".into());
    assert!(backup::backup(o3, &NoHooks, quiet()).await.is_err());

    // Newest by default, either by name.
    let (_, newest) = backup::find_snapshot(&out, None).unwrap();
    assert_eq!(newest.name, "20260102T000000Z");
    let (_, older) = backup::find_snapshot(&out, Some("20260101T000000Z")).unwrap();
    assert_eq!(older.refs["main"], tip.0.to_hex());
    assert!(backup::find_snapshot(&out, Some("nope")).is_err());

    // An interrupted run (no manifest) is not a snapshot.
    std::fs::create_dir_all(out.join("snapshots/20260103T000000Z")).unwrap();
    std::fs::write(out.join("snapshots/20260103T000000Z/meta.db"), b"half").unwrap();
    let (_, still) = backup::find_snapshot(&out, None).unwrap();
    assert_eq!(still.name, "20260102T000000Z");

    // The old snapshot still restores after the pool grew.
    let dest = Site::new(root.path(), "restored").await;
    let mut r = restore_opts(&out, &dest);
    r.snapshot = Some("20260101T000000Z".into());
    let r = backup::restore(r, quiet()).await.unwrap();
    assert!(r.report.unwrap().ok());
    assert_eq!(dest.repo.meta().get_ref("main").await.unwrap(), Some(tip));

    let v = backup::verify_backup(
        VerifyBackupOptions {
            from: out.clone(),
            snapshot: None,
            blobs: leep_store::open_blob_store(&format!("file://{}", out.join("blobs").display()))
                .unwrap(),
            rehash: true,
            max_problems: 10,
        },
        &Quiet,
    )
    .await
    .unwrap();
    assert!(v.report.ok());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restore_refuses_what_it_should() {
    let root = tempfile::tempdir().unwrap();
    let site = Site::new(root.path(), "src").await;
    site.populate().await;
    let out = root.path().join("backup");
    let b = backup::backup(opts(&site, &out), &NoHooks, quiet())
        .await
        .unwrap();
    assert!(b.ok());
    let snap = b.snapshot_dir.clone();

    // A non-empty metadata store.
    let dest = Site::new(root.path(), "d1").await;
    site_commit_stray(&dest).await;
    let err = backup::restore(restore_opts(&out, &dest), quiet())
        .await
        .err()
        .unwrap()
        .to_string();
    assert!(err.contains("metadata store is not empty"), "{err}");
    // Untouched: only the stray ref is there.
    let refs: Vec<_> = dest
        .repo
        .meta()
        .list_refs()
        .await
        .unwrap()
        .into_keys()
        .collect();
    assert_eq!(refs, ["stray"]);
    // Forced, it replaces it.
    let mut o = restore_opts(&out, &dest);
    o.force = true;
    backup::restore(o, quiet()).await.unwrap();
    assert!(dest.repo.meta().get_ref("main").await.unwrap().is_some());
    assert!(dest.repo.meta().get_ref("stray").await.unwrap().is_none());

    // A non-empty blob store with empty metadata: refused, then resumable.
    let d2 = Site::new(root.path(), "d2").await;
    d2.repo
        .blobs()
        .put(Bytes::from_static(b"stray"))
        .await
        .unwrap();
    let err = backup::restore(restore_opts(&out, &d2), quiet())
        .await
        .err()
        .unwrap()
        .to_string();
    assert!(err.contains("blob store is not empty"), "{err}");
    let mut o = restore_opts(&out, &d2);
    o.resume = true;
    backup::restore(o, quiet()).await.unwrap();
    // ...but --resume does not excuse a non-empty metadata store.
    let mut o = restore_opts(&out, &d2);
    o.resume = true;
    assert!(backup::restore(o, quiet()).await.is_err());

    // A backup from a newer schema, a newer manifest format, and a damaged file.
    let manifest_path = snap.join(backup::MANIFEST_FILE);
    let original = std::fs::read_to_string(&manifest_path).unwrap();
    let edit = |find: &str, replace: &str| {
        let text = original.replace(find, replace);
        assert_ne!(text, original, "{find}");
        std::fs::write(&manifest_path, text).unwrap();
    };
    let d3 = Site::new(root.path(), "d3").await;
    edit("\"schema_version\": 1", "\"schema_version\": 99");
    let err = backup::restore(restore_opts(&out, &d3), quiet())
        .await
        .err()
        .unwrap()
        .to_string();
    assert!(err.contains("newer than this server"), "{err}");
    edit("\"format\": 1", "\"format\": 2");
    let err = backup::restore(restore_opts(&out, &d3), quiet())
        .await
        .err()
        .unwrap()
        .to_string();
    assert!(err.contains("manifest format 2"), "{err}");
    std::fs::write(&manifest_path, &original).unwrap();
    let meta_file = snap.join(backup::META_FILE);
    let good = std::fs::read(&meta_file).unwrap();
    let mut bad = good.clone();
    let mid = bad.len() / 2;
    bad[mid] ^= 0xff;
    std::fs::write(&meta_file, &bad).unwrap();
    let err = backup::restore(restore_opts(&out, &d3), quiet())
        .await
        .err()
        .unwrap()
        .to_string();
    assert!(err.contains("does not match its manifest"), "{err}");
    assert!(backup::verify_backup(
        VerifyBackupOptions {
            from: out.clone(),
            snapshot: None,
            blobs: leep_store::open_blob_store(&format!("file://{}", out.join("blobs").display()))
                .unwrap(),
            rehash: false,
            max_problems: 10,
        },
        &Quiet,
    )
    .await
    .is_err());
    std::fs::write(&meta_file, &good).unwrap();
    // Intact again, it restores.
    backup::restore(restore_opts(&out, &d3), quiet())
        .await
        .unwrap();
}

async fn site_commit_stray(s: &Site) {
    let c = s.commit(&[], &[("stray.txt", "stray\n")], "stray").await;
    s.advance("stray", c).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn metadata_only_backup_is_checked_against_the_blobs_you_provide() {
    let root = tempfile::tempdir().unwrap();
    let site = Site::new(root.path(), "src").await;
    let (tip, _, _) = site.populate().await;
    let out = root.path().join("backup");
    let mut o = opts(&site, &out);
    o.dest_blobs = None;
    o.rehash = false;
    let b = backup::backup(o, &NoHooks, quiet()).await.unwrap();
    assert!(b.ok(), "checked against the source blobs");
    assert!(!b.manifest.blobs.included);

    // Without blobs it cannot restore, and says why.
    let dest = Site::new(root.path(), "restored").await;
    let mut r = restore_opts(&out, &dest);
    r.source_blobs = None;
    let err = backup::restore(r, quiet()).await.err().unwrap().to_string();
    assert!(err.contains("--no-blobs"), "{err}");
    // Pointed at a copy of the blobs (what `aws s3 sync` would make), it does.
    let copy = root.path().join("synced");
    leep_store::sync::sync_blobs(
        &*site.repo.blobs().clone(),
        &*leep_store::open_blob_store(&format!("file://{}", copy.display())).unwrap(),
        &Default::default(),
    )
    .await
    .unwrap();
    let mut r = restore_opts(&out, &dest);
    r.source_blobs =
        Some(leep_store::open_blob_store(&format!("file://{}", copy.display())).unwrap());
    let r = backup::restore(r, quiet()).await.unwrap();
    assert!(r.report.unwrap().ok());
    assert_eq!(dest.repo.meta().get_ref("main").await.unwrap(), Some(tip));
}

#[tokio::test]
async fn backup_never_creates_a_missing_source() {
    let root = tempfile::tempdir().unwrap();
    let missing = format!("sqlite://{}/nope.db?mode=rwc", root.path().display());
    let err = backup::check_source_exists(&missing, &format!("file://{}", root.path().display()))
        .unwrap_err()
        .to_string();
    assert!(err.contains("does not exist"), "{err}");
    assert!(!root.path().join("nope.db").exists());
    assert!(backup::check_source_exists("mem://", "file:///tmp").is_err());
}
