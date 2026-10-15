//! Blob garbage collection.
//!
//! Changesets are never deleted, so everything they reference is live: every
//! `Modify` content hash, plus every tree node and file blob reachable from
//! each changeset's root tree. Any other blob is garbage, provided it is older
//! than a grace window that protects uploads whose changeset is not stored
//! yet. See ADR 0008.
//!
//! The mark set holds one 32-byte hash per live blob and tree node, so memory
//! grows linearly with the number of distinct live blobs (roughly 50 bytes
//! each). Subtrees shared between snapshots are walked once.

use std::collections::HashSet;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use leep_core::{ChangesetId, Error, FileChange, Hash, Result, TreeEntry};

use crate::Repo;

/// Page size for changeset and blob listings.
const PAGE: usize = 1000;

/// Derived-data kind recording when a client was last told a blob already
/// exists (value: unix seconds, big-endian). A client that hears "present"
/// skips the upload and references the blob in a changeset it creates later,
/// so the blob's own modification time says nothing about that use. GC keeps
/// claimed blobs for the grace window like freshly uploaded ones.
pub const BLOB_CLAIM: &str = "blob-claim-v1";

fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs() as i64)
}

impl Repo {
    /// The subset of `hashes` not stored. Every hash reported present is
    /// claimed first (see [`BLOB_CLAIM`]), so a concurrent GC cannot delete
    /// a blob a client was just told it may reuse.
    pub async fn missing_blobs(&self, hashes: &[Hash]) -> Result<Vec<Hash>> {
        let now = bytes::Bytes::copy_from_slice(&unix_now().to_be_bytes());
        let mut missing = Vec::new();
        for h in hashes {
            if self.blobs().contains(h).await? {
                self.meta().put_derived(BLOB_CLAIM, h, now.clone()).await?;
            } else {
                missing.push(*h);
            }
        }
        Ok(missing)
    }

    async fn claimed_since(&self, hash: &Hash, cutoff: i64) -> Result<bool> {
        Ok(match self.meta().get_derived(BLOB_CLAIM, hash).await? {
            Some(v) => <[u8; 8]>::try_from(v.as_ref())
                .map(|b| i64::from_be_bytes(b) >= cutoff)
                // An unreadable claim errs on the side of keeping the blob.
                .unwrap_or(true),
            None => false,
        })
    }
}

#[derive(Clone, Debug)]
pub struct GcOptions {
    /// Unreferenced blobs modified within this window before the run started
    /// are kept. Zero disables the protection, including for blobs whose
    /// modification time is unknown.
    pub grace: Duration,
    /// Report what would be deleted without deleting.
    pub dry_run: bool,
}

impl Default for GcOptions {
    fn default() -> Self {
        GcOptions {
            grace: Duration::from_secs(24 * 3600),
            dry_run: false,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct GcReport {
    pub changesets_scanned: u64,
    /// Distinct blobs and tree nodes referenced by stored changesets.
    pub marked: u64,
    pub blobs_scanned: u64,
    /// Blobs deleted, or that would be in a dry run.
    pub deleted: u64,
    pub deleted_bytes: u64,
    /// Unreferenced blobs kept because they are younger than the grace window.
    pub kept_by_grace: u64,
    /// Unreferenced blobs kept because the store reports no modification time.
    pub kept_unknown_mtime: u64,
    /// Unreferenced old blobs kept because a client was recently told they
    /// exist and may be about to reference them.
    pub kept_by_claim: u64,
}

/// Collect garbage using the current time as the start of the run.
pub async fn run(repo: &Repo, opts: &GcOptions) -> Result<GcReport> {
    run_at(repo, opts, unix_now()).await
}

/// Like [`run`] with an explicit start time (unix seconds), for tests.
pub async fn run_at(repo: &Repo, opts: &GcOptions, started_at: i64) -> Result<GcReport> {
    let mut report = GcReport::default();
    let marked = mark(repo, &mut report).await?;
    report.marked = marked.len() as u64;
    sweep(repo, opts, started_at, &marked, &mut report).await?;
    Ok(report)
}

/// Every blob and tree node referenced by any stored changeset. Any error
/// aborts before the sweep, so an incomplete mark set never deletes anything.
async fn mark(repo: &Repo, report: &mut GcReport) -> Result<HashSet<Hash>> {
    let mut marked: HashSet<Hash> = HashSet::new();
    let mut after: Option<ChangesetId> = None;
    loop {
        let page = repo.meta().list_changesets(after, PAGE).await?;
        let Some(last) = page.last().copied() else {
            return Ok(marked);
        };
        after = Some(last);
        for id in page {
            report.changesets_scanned += 1;
            let cs = repo.get_changeset(id).await?;
            for change in cs.changes.values() {
                if let FileChange::Modify { content, .. } = change {
                    marked.insert(*content);
                }
            }
            // Tree and blob hashes are domain-separated, so one set doubles as
            // the visited set: a tree already in it was fully walked (or is
            // queued below) via an earlier snapshot.
            let mut stack = vec![repo.root_tree(id).await?];
            while let Some(tree) = stack.pop() {
                if !marked.insert(tree) {
                    continue;
                }
                for entry in repo.read_tree(tree).await?.entries.into_values() {
                    match entry {
                        TreeEntry::File { content, .. } => {
                            marked.insert(content);
                        }
                        TreeEntry::Dir { tree } => {
                            if !marked.contains(&tree) {
                                stack.push(tree);
                            }
                        }
                    }
                }
            }
        }
    }
}

async fn sweep(
    repo: &Repo,
    opts: &GcOptions,
    started_at: i64,
    marked: &HashSet<Hash>,
    report: &mut GcReport,
) -> Result<()> {
    let grace = i64::try_from(opts.grace.as_secs()).unwrap_or(i64::MAX);
    let cutoff = started_at.saturating_sub(grace);
    let mut after: Option<Hash> = None;
    loop {
        let page = repo.blobs().list_page(after, PAGE).await?;
        let Some(last) = page.last().map(|m| m.hash) else {
            return Ok(());
        };
        after = Some(last);
        for blob in page {
            report.blobs_scanned += 1;
            if marked.contains(&blob.hash) {
                continue;
            }
            match blob.modified {
                Some(t) if t > cutoff => {
                    report.kept_by_grace += 1;
                    continue;
                }
                None if grace > 0 => {
                    report.kept_unknown_mtime += 1;
                    continue;
                }
                _ => {}
            }
            // Checked last, immediately before deleting, to keep the window
            // against a concurrent claim as small as possible.
            if repo.claimed_since(&blob.hash, cutoff).await? {
                report.kept_by_claim += 1;
                continue;
            }
            if !opts.dry_run {
                repo.blobs().delete(&blob.hash).await?;
            }
            report.deleted += 1;
            report.deleted_bytes += blob.size;
        }
    }
}

/// Parse `0`, `90s`, `30m`, `24h` or `7d`. A bare number is seconds.
pub fn parse_duration(s: &str) -> Result<Duration> {
    let s = s.trim();
    let bad = || Error::Invalid(format!("bad duration {s:?}; use e.g. 0, 90s, 30m, 24h, 7d"));
    let (num, unit) = match s.char_indices().find(|(_, c)| !c.is_ascii_digit()) {
        Some((i, _)) => s.split_at(i),
        None => (s, "s"),
    };
    let n: u64 = num.parse().map_err(|_| bad())?;
    let mult = match unit {
        "s" => 1,
        "m" => 60,
        "h" => 3600,
        "d" => 86400,
        _ => return Err(bad()),
    };
    n.checked_mul(mult).map(Duration::from_secs).ok_or_else(bad)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use bytes::Bytes;
    use leep_core::api::{Authorship, MergeRequest, MergeResponse};
    use leep_core::memory::{MemoryBlobStore, MemoryMetaStore};
    use leep_core::{BlobStore, Changeset, FileKind, MetaStore, RepoPath, TreeNode};

    use super::*;

    const HOUR: i64 = 3600;

    fn now() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64
    }

    fn setup() -> (Repo, Arc<MemoryBlobStore>) {
        let blobs = Arc::new(MemoryBlobStore::new());
        let repo = Repo::new(blobs.clone(), Arc::new(MemoryMetaStore::new()));
        (repo, blobs)
    }

    fn opts(grace_hours: u64, dry_run: bool) -> GcOptions {
        GcOptions {
            grace: Duration::from_secs(grace_hours * 3600),
            dry_run,
        }
    }

    fn rp(s: &str) -> RepoPath {
        RepoPath::new(s).unwrap()
    }

    /// `Some(text)` writes a file, `None` deletes it.
    async fn commit(
        r: &Repo,
        parents: &[ChangesetId],
        files: &[(&str, Option<&str>)],
    ) -> ChangesetId {
        let mut changes = BTreeMap::new();
        for (p, c) in files {
            let ch = match c {
                Some(t) => FileChange::Modify {
                    content: r
                        .blobs()
                        .put(Bytes::copy_from_slice(t.as_bytes()))
                        .await
                        .unwrap(),
                    kind: FileKind::Regular,
                    size: t.len() as u64,
                },
                None => FileChange::Delete,
            };
            changes.insert(rp(p), ch);
        }
        r.create_changeset(Changeset {
            parents: parents.to_vec(),
            author: "t".into(),
            timestamp: 1,
            message: "m".into(),
            changes,
            extra: BTreeMap::new(),
        })
        .await
        .unwrap()
    }

    async fn merge(r: &Repo, ours: ChangesetId, theirs: ChangesetId) -> ChangesetId {
        let resp = r
            .merge(MergeRequest {
                parents: [ours, theirs],
                message: "merge".into(),
                authorship: Authorship {
                    author: "m".into(),
                    timestamp: Some(5),
                },
                extra: BTreeMap::new(),
            })
            .await
            .unwrap();
        match resp {
            MergeResponse::Merged { id } => id,
            other => panic!("expected clean merge: {other:?}"),
        }
    }

    /// Age every blob in the store past any grace window.
    async fn age_all(blobs: &MemoryBlobStore) {
        for m in blobs.list_page(None, usize::MAX).await.unwrap() {
            blobs.set_modified(&m.hash, Some(now() - 100 * HOUR));
        }
    }

    async fn all_ids(r: &Repo) -> Vec<ChangesetId> {
        r.meta().list_changesets(None, usize::MAX).await.unwrap()
    }

    /// Every file of every changeset, as (id, path, bytes), read through the
    /// public read path.
    async fn read_everything(r: &Repo) -> Vec<(ChangesetId, String, Bytes)> {
        let mut out = Vec::new();
        for id in all_ids(r).await {
            for f in r.list_files(id, "").await.unwrap() {
                let (_, data) = r
                    .read_file(id, &f.path)
                    .await
                    .unwrap()
                    .expect("listed file is readable");
                out.push((id, f.path.to_string(), data));
            }
        }
        out.sort();
        out
    }

    /// Branching history with merges, deletes, nested dirs, and a file that
    /// exists only in old snapshots.
    async fn build_history(r: &Repo) -> Vec<ChangesetId> {
        let base = commit(
            r,
            &[],
            &[
                ("a.txt", Some("a1\n")),
                ("gone.txt", Some("gone\n")),
                ("d/x.txt", Some("x1\n")),
                ("d/e/y.txt", Some("y1\n")),
            ],
        )
        .await;
        let left1 = commit(r, &[base], &[("a.txt", Some("a2\n")), ("gone.txt", None)]).await;
        let left2 = commit(
            r,
            &[left1],
            &[("d/x.txt", Some("x2\n")), ("l.txt", Some("l\n"))],
        )
        .await;
        let right1 = commit(
            r,
            &[base],
            &[("d/e/y.txt", Some("y2\n")), ("r/new.txt", Some("n\n"))],
        )
        .await;
        let right2 = commit(
            r,
            &[right1],
            &[("r/new.txt", None), ("r2.txt", Some("r2\n"))],
        )
        .await;
        let m1 = merge(r, left2, right2).await;
        let tip = commit(
            r,
            &[m1],
            &[("d/e/y.txt", None), ("final.txt", Some("fin\n"))],
        )
        .await;
        vec![base, left1, left2, right1, right2, m1, tip]
    }

    #[tokio::test]
    async fn never_deletes_anything_reachable_from_history() {
        let (r, blobs) = setup();
        let ids = build_history(&r).await;
        r.meta().cas_ref("main", None, Some(ids[6])).await.unwrap();
        // Unlanded changeset: stored but referenced by no ref.
        let unlanded = commit(&r, &[ids[0]], &[("only-here.txt", Some("unlanded\n"))]).await;
        let before = read_everything(&r).await;
        let blobs_before = blobs.len();
        assert!(before.iter().any(|(_, p, _)| p == "gone.txt"));
        age_all(&blobs).await;

        // No garbage yet: grace 0 must delete nothing at all.
        let report = run(&r, &opts(0, false)).await.unwrap();
        assert_eq!(report.deleted, 0);
        assert_eq!(report.changesets_scanned, 8);
        assert_eq!(report.blobs_scanned as usize, blobs_before);
        assert_eq!(report.marked as usize, blobs_before);
        assert_eq!(blobs.len(), blobs_before);

        // Add garbage of every kind, then collect.
        let orphan = r.blobs().put(Bytes::from("orphan upload")).await.unwrap();
        let orphan_tree = TreeNode {
            entries: BTreeMap::from([(
                "f".to_string(),
                TreeEntry::File {
                    content: orphan,
                    kind: FileKind::Regular,
                    size: 13,
                },
            )]),
        };
        r.blobs()
            .put_with_hash(orphan_tree.hash(), Bytes::from(orphan_tree.encode()))
            .await
            .unwrap();
        // A commit that fails validation after uploading its blob.
        let failed = Changeset {
            parents: vec![ids[0]],
            author: "t".into(),
            timestamp: 1,
            message: "bad".into(),
            changes: BTreeMap::from([
                (
                    rp("never.txt"),
                    FileChange::Modify {
                        content: r.blobs().put(Bytes::from("failed commit")).await.unwrap(),
                        kind: FileKind::Regular,
                        size: 13,
                    },
                ),
                (rp("absent.txt"), FileChange::Delete),
            ]),
            extra: BTreeMap::new(),
        };
        assert!(r.create_changeset(failed).await.is_err());
        age_all(&blobs).await;
        let report = run(&r, &opts(24, false)).await.unwrap();
        assert_eq!(report.deleted, 3, "{report:?}");
        assert_eq!(blobs.len(), blobs_before);
        assert!(!blobs.contains(&orphan).await.unwrap());
        assert!(!blobs.contains(&orphan_tree.hash()).await.unwrap());

        assert_eq!(read_everything(&r).await, before);
        assert!(r
            .read_file(unlanded, &rp("only-here.txt"))
            .await
            .unwrap()
            .is_some());
        // Idempotent.
        let again = run(&r, &opts(0, false)).await.unwrap();
        assert_eq!(again.deleted, 0);
        assert_eq!(read_everything(&r).await, before);
    }

    #[tokio::test]
    async fn derived_trees_are_rebuilt_not_trusted_missing() {
        // Tree nodes of old snapshots survive even when only the changesets
        // (not derived root trees) were stored, e.g. after a derived-data wipe.
        let (r, blobs) = setup();
        let ids = build_history(&r).await;
        let before = read_everything(&r).await;
        // A fresh Repo over the same blobs but a meta store with only the
        // changesets: root trees are derived during the mark phase.
        let meta = Arc::new(MemoryMetaStore::new());
        for id in &ids {
            meta.put_changeset(&r.get_changeset(*id).await.unwrap())
                .await
                .unwrap();
        }
        let r2 = Repo::new(blobs.clone(), meta);
        age_all(&blobs).await;
        let report = run(&r2, &opts(0, false)).await.unwrap();
        assert_eq!(report.deleted, 0);
        assert_eq!(read_everything(&r2).await, before);
    }

    #[tokio::test]
    async fn grace_window_protects_young_orphans() {
        let (r, blobs) = setup();
        commit(&r, &[], &[("a.txt", Some("a\n"))]).await;
        let orphan = r.blobs().put(Bytes::from("fresh upload")).await.unwrap();
        age_all(&blobs).await;
        blobs.set_modified(&orphan, Some(now() - HOUR));

        let report = run(&r, &opts(24, false)).await.unwrap();
        assert_eq!((report.deleted, report.kept_by_grace), (0, 1));
        assert!(blobs.contains(&orphan).await.unwrap());

        // Re-uploading a stale orphan restarts its clock.
        blobs.set_modified(&orphan, Some(now() - 30 * HOUR));
        r.blobs().put(Bytes::from("fresh upload")).await.unwrap();
        let report = run(&r, &opts(24, false)).await.unwrap();
        assert_eq!((report.deleted, report.kept_by_grace), (0, 1));

        blobs.set_modified(&orphan, Some(now() - 25 * HOUR));
        let report = run(&r, &opts(24, false)).await.unwrap();
        assert_eq!((report.deleted, report.deleted_bytes), (1, 12));
        assert!(!blobs.contains(&orphan).await.unwrap());
    }

    #[tokio::test]
    async fn orphan_becomes_live_if_a_changeset_references_it() {
        let (r, blobs) = setup();
        let base = commit(&r, &[], &[("a.txt", Some("a\n"))]).await;
        let h = r.blobs().put(Bytes::from("late commit")).await.unwrap();
        age_all(&blobs).await;
        // The client finally commits, long after upload.
        let id = r
            .create_changeset(Changeset {
                parents: vec![base],
                author: "t".into(),
                timestamp: 1,
                message: "late".into(),
                changes: BTreeMap::from([(
                    rp("late.txt"),
                    FileChange::Modify {
                        content: h,
                        kind: FileKind::Regular,
                        size: 11,
                    },
                )]),
                extra: BTreeMap::new(),
            })
            .await
            .unwrap();
        let report = run(&r, &opts(0, false)).await.unwrap();
        assert_eq!(report.deleted, 0);
        assert!(r.read_file(id, &rp("late.txt")).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn dry_run_deletes_nothing() {
        let (r, blobs) = setup();
        commit(&r, &[], &[("a.txt", Some("a\n"))]).await;
        let orphan = r.blobs().put(Bytes::from("orphan")).await.unwrap();
        age_all(&blobs).await;
        let n = blobs.len();
        let report = run(&r, &opts(0, true)).await.unwrap();
        assert_eq!((report.deleted, report.deleted_bytes), (1, 6));
        assert_eq!(blobs.len(), n);
        assert!(blobs.contains(&orphan).await.unwrap());
        let report = run(&r, &opts(0, false)).await.unwrap();
        assert_eq!(report.deleted, 1);
        assert_eq!(blobs.len(), n - 1);
    }

    #[tokio::test]
    async fn unknown_mtime_is_kept_unless_grace_is_zero() {
        let (r, blobs) = setup();
        commit(&r, &[], &[("a.txt", Some("a\n"))]).await;
        let orphan = r.blobs().put(Bytes::from("mystery")).await.unwrap();
        blobs.set_modified(&orphan, None);
        let report = run(&r, &opts(24, false)).await.unwrap();
        assert_eq!((report.deleted, report.kept_unknown_mtime), (0, 1));
        assert!(blobs.contains(&orphan).await.unwrap());
        let report = run(&r, &opts(0, false)).await.unwrap();
        assert_eq!(report.deleted, 1);
        assert!(!blobs.contains(&orphan).await.unwrap());
    }

    #[tokio::test]
    async fn pages_through_more_than_one_page_of_blobs() {
        let (r, blobs) = setup();
        commit(&r, &[], &[("a.txt", Some("a\n"))]).await;
        for i in 0..(PAGE * 2 + 17) {
            r.blobs()
                .put(Bytes::from(format!("orphan {i}")))
                .await
                .unwrap();
        }
        age_all(&blobs).await;
        let report = run(&r, &opts(1, false)).await.unwrap();
        assert_eq!(report.deleted as usize, PAGE * 2 + 17);
        assert_eq!(report.blobs_scanned as usize, PAGE * 2 + 17 + 2);
        assert_eq!(blobs.len(), 2);
    }

    #[tokio::test]
    async fn missing_tree_node_aborts_without_deleting() {
        let (r, blobs) = setup();
        let id = commit(&r, &[], &[("d/a.txt", Some("a\n"))]).await;
        let orphan = r.blobs().put(Bytes::from("orphan")).await.unwrap();
        age_all(&blobs).await;
        let sub = match r
            .read_tree(r.root_tree(id).await.unwrap())
            .await
            .unwrap()
            .entries["d"]
        {
            TreeEntry::Dir { tree } => tree,
            _ => unreachable!(),
        };
        blobs.delete(&sub).await.unwrap();
        assert!(run(&r, &opts(0, false)).await.is_err());
        assert!(blobs.contains(&orphan).await.unwrap());
    }

    #[test]
    fn durations() {
        assert_eq!(parse_duration("0").unwrap(), Duration::ZERO);
        assert_eq!(parse_duration("90s").unwrap(), Duration::from_secs(90));
        assert_eq!(parse_duration("30m").unwrap(), Duration::from_secs(1800));
        assert_eq!(parse_duration("24h").unwrap(), Duration::from_secs(86400));
        assert_eq!(parse_duration("7d").unwrap(), Duration::from_secs(604800));
        assert_eq!(parse_duration("45").unwrap(), Duration::from_secs(45));
        for bad in ["", "h", "-1h", "1.5h", "5x", "1h30m"] {
            assert!(parse_duration(bad).is_err(), "{bad}");
        }
    }

    /// A client told "present" may reference an old orphan without uploading
    /// it again; GC must not delete it out from under the coming changeset.
    #[tokio::test]
    async fn claimed_orphan_survives_until_claim_expires() {
        let (repo, blobs) = setup();
        let orphan = blobs
            .put(Bytes::from_static(b"shared content\n"))
            .await
            .unwrap();
        blobs.set_modified(&orphan, Some(now() - 100 * HOUR));
        let absent = Hash::of_blob(b"never uploaded");

        assert_eq!(
            repo.missing_blobs(&[orphan, absent]).await.unwrap(),
            [absent]
        );
        assert!(repo
            .meta()
            .get_derived(BLOB_CLAIM, &absent)
            .await
            .unwrap()
            .is_none());

        let r = run_at(&repo, &opts(24, false), now()).await.unwrap();
        assert_eq!((r.deleted, r.kept_by_claim), (0, 1));
        assert!(blobs.contains(&orphan).await.unwrap());

        // The client's changeset arrives after the run: the blob is live now.
        commit_existing(&repo, orphan, 15).await;
        let r = run_at(&repo, &opts(24, false), now() + 48 * HOUR)
            .await
            .unwrap();
        assert_eq!(r.deleted, 0);

        // A claim that is never followed up expires with the grace window.
        let stale = blobs.put(Bytes::from_static(b"abandoned\n")).await.unwrap();
        blobs.set_modified(&stale, Some(now() - 100 * HOUR));
        repo.missing_blobs(&[stale]).await.unwrap();
        let r = run_at(&repo, &opts(24, false), now() + 48 * HOUR)
            .await
            .unwrap();
        assert_eq!((r.deleted, r.kept_by_claim), (1, 0));
        assert!(!blobs.contains(&stale).await.unwrap());
    }

    async fn commit_existing(r: &Repo, content: Hash, size: u64) {
        let mut changes = BTreeMap::new();
        changes.insert(
            rp("reused.txt"),
            FileChange::Modify {
                content,
                kind: FileKind::Regular,
                size,
            },
        );
        r.create_changeset(Changeset {
            parents: vec![],
            author: "t".into(),
            timestamp: 1,
            message: "reuse".into(),
            changes,
            extra: BTreeMap::new(),
        })
        .await
        .unwrap();
    }
}
