//! The automatic delta repack of the background task (ADR 0100): packs
//! written whole (imports, land batches) and loose objects are compressed
//! against their previous versions once a threshold is passed, never below
//! it; the bytes waiting for cleanup stay under their cap; an object store
//! gets a full repack, at most once per interval; and garbage collection
//! running beside it deletes and rescues what it should.

use std::collections::{BTreeMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use leep_core::memory::{check_blob_store_contract, MemoryMetaStore};
use leep_core::{BlobStore, Changeset, ChangesetId, FileChange, FileKind, Hash, MetaStore};
use leep_core::{RepoPath, TreeEntry};
use leep_derive::gc::{self, Clock, GcOptions};
use leep_derive::Repo;
use leep_server::background_repack::{
    AutoOutcome, AutoRepackOptions, BackgroundRepack, BackgroundRepackOptions, PackStore,
};
use leep_store::{FsBlobStore, ObjectBlobStore, PackWrites};

const FILES: [&str; 3] = ["src/lib.rs", "src/deep/mod.rs", "README.md"];

/// Version `v` of `file`: each a small edit of the one before.
fn version(file: &str, v: usize) -> String {
    let mut s = format!("// {file}\n");
    for i in 0..=v {
        s.push_str(&format!(
            "fn item_{i}() {{ let value = {i} * 7; println!(\"{file} {{value}}\"); }}\n"
        ));
    }
    s
}

/// Commit `files` on `parent`, their contents uploaded already.
async fn commit(repo: &Repo, parent: Option<ChangesetId>, files: &[(&str, String)]) -> ChangesetId {
    let mut changes = BTreeMap::new();
    for (path, body) in files {
        changes.insert(
            RepoPath::new(*path).unwrap(),
            FileChange::Modify {
                content: Hash::of_blob(body.as_bytes()),
                kind: FileKind::Regular,
                size: body.len() as u64,
            },
        );
    }
    repo.create_changeset(Changeset {
        parents: parent.into_iter().collect(),
        author: "agent".into(),
        timestamp: 1,
        message: format!("{:?}", files.iter().map(|f| f.0).collect::<Vec<_>>()),
        changes,
        extra: BTreeMap::new(),
    })
    .await
    .unwrap()
}

/// What an import leaves: `versions` versions of each file, every content
/// written by one `put_many` (a pack of whole frames when the store packs
/// such batches), then one changeset per version on `main` (their tree
/// nodes loose).
async fn import(repo: &Repo, meta: &Arc<MemoryMetaStore>, versions: usize) -> ChangesetId {
    let items: Vec<(Hash, Bytes)> = (0..versions)
        .flat_map(|v| FILES.iter().map(move |f| Bytes::from(version(f, v))))
        .map(|b| (Hash::of_blob(&b), b))
        .collect();
    repo.blobs().put_many(items).await.unwrap();
    let mut tip = None;
    for v in 0..versions {
        let files: Vec<(&str, String)> = FILES.iter().map(|f| (*f, version(f, v))).collect();
        tip = Some(commit(repo, tip, &files).await);
    }
    let tip = tip.unwrap();
    let old = meta.get_ref("main").await.unwrap();
    assert!(meta.cas_ref("main", old, Some(tip)).await.unwrap());
    tip
}

/// One more version of each file on `main`, uploaded loose.
async fn land_one(repo: &Repo, meta: &Arc<MemoryMetaStore>, v: usize) {
    let head = meta.get_ref("main").await.unwrap();
    let mut files = Vec::new();
    for f in FILES {
        let body = version(f, v);
        repo.blobs().put(Bytes::from(body.clone())).await.unwrap();
        files.push((f, body));
    }
    let tip = commit(repo, head, &files).await;
    assert!(meta.cas_ref("main", head, Some(tip)).await.unwrap());
}

/// Every changeset `main` reaches, with every node and content it names,
/// read through `blobs`.
async fn assert_main_readable(blobs: Arc<dyn BlobStore>, meta: &Arc<MemoryMetaStore>, what: &str) {
    let repo = Repo::new(blobs.clone(), meta.clone());
    let head = meta.get_ref("main").await.unwrap().unwrap();
    let mut seen = HashSet::new();
    let mut stack = vec![head];
    while let Some(id) = stack.pop() {
        if !seen.insert(id) {
            continue;
        }
        stack.extend(repo.get_changeset(id).await.unwrap().parents);
        let mut trees = vec![repo.root_tree(id).await.unwrap()];
        while let Some(t) = trees.pop() {
            let node = repo
                .read_tree(t)
                .await
                .unwrap_or_else(|e| panic!("{what}: tree {t} of {id}: {e}"));
            for entry in node.entries.values() {
                match entry {
                    TreeEntry::Dir { tree } => trees.push(*tree),
                    TreeEntry::File { content, .. } => {
                        let b = blobs
                            .get(content)
                            .await
                            .unwrap()
                            .unwrap_or_else(|| panic!("{what}: {content} of {id} missing"));
                        assert_eq!(Hash::of_blob(&b), *content, "{what}");
                    }
                }
            }
        }
    }
}

/// Bytes of the loose objects and published packs under `root`.
fn store_bytes(root: &Path) -> u64 {
    let mut total = 0;
    let mut stack = vec![root.to_path_buf()];
    while let Some(d) = stack.pop() {
        for e in std::fs::read_dir(&d).into_iter().flatten().flatten() {
            let p = e.path();
            if p.is_dir() {
                stack.push(p);
            } else {
                total += e.metadata().map_or(0, |m| m.len());
            }
        }
    }
    total
}

fn copy_dir(from: &Path, to: &Path) {
    for e in std::fs::read_dir(from).unwrap().flatten() {
        let (src, dst) = (e.path(), to.join(e.file_name()));
        if src.is_dir() {
            std::fs::create_dir_all(&dst).unwrap();
            copy_dir(&src, &dst);
        } else {
            std::fs::copy(&src, &dst).unwrap();
        }
    }
}

/// The pack bytes `leep-server repack --full` makes of the store at `root`.
async fn full_repack_bytes(root: &tempfile::TempDir, meta: &Arc<MemoryMetaStore>) -> u64 {
    let store = FsBlobStore::new(root.path());
    let repo = Repo::new(Arc::new(store.clone()), meta.clone());
    let order = leep_derive::pack_order::pack_order(&repo).await.unwrap();
    let opts = leep_store::RepackOptions {
        threads: 1,
        ..leep_store::RepackOptions::default()
    };
    let p = store.repack_publish(order.objects, opts).await.unwrap();
    p.report().pack_bytes
}

fn fs_store(root: &Path) -> FsBlobStore {
    FsBlobStore::new(root).with_pack_writes(PackWrites {
        min_objects: 4,
        ..PackWrites::default()
    })
}

fn opts(auto: AutoRepackOptions) -> BackgroundRepackOptions {
    BackgroundRepackOptions {
        fence: Duration::ZERO,
        auto: Some(auto),
        ..BackgroundRepackOptions::default()
    }
}

fn eager() -> AutoRepackOptions {
    AutoRepackOptions {
        objects: 1,
        check_interval: Duration::ZERO,
        ..AutoRepackOptions::default()
    }
}

fn ran(o: AutoOutcome) -> leep_store::RepackReport {
    match o {
        AutoOutcome::Ran(r) => *r,
        other => panic!("expected a run, got {other:?}"),
    }
}

/// A whole-frame import pack and loose tree nodes become one pack of
/// deltas, several times smaller, in one run; the next check finds nothing
/// to do, and the store reads and passes the contract.
#[tokio::test]
async fn whole_import_packs_are_compressed_once() {
    let dir = tempfile::tempdir().unwrap();
    let store = fs_store(dir.path());
    let meta = Arc::new(MemoryMetaStore::new());
    let repo = Repo::new(Arc::new(store.clone()), meta.clone());
    import(&repo, &meta, 60).await;
    let sizes = store.pack_sizes().unwrap();
    assert_eq!(sizes.len(), 1, "the import's contents are one pack");
    let before = store_bytes(dir.path());
    let copy = tempfile::tempdir().unwrap();
    copy_dir(dir.path(), copy.path());

    let bg = BackgroundRepack::new(PackStore::Fs(store.clone()), opts(eager()));
    let walk = bg.walk_repo(meta.clone());
    let mut bg = bg.with_repo(walk);
    let tick = bg.tick().await.unwrap();
    let r = ran(tick.auto);
    assert_eq!(r.packs_merged, 1, "{r:?}");
    assert!(r.loose_before >= 60, "tree nodes were loose: {r:?}");
    // Only the first version at each path keeps its frame.
    assert!(r.frames_reused <= 3, "{r:?}");
    assert!(r.deltas > 3 * r.whole, "{r:?}");
    // As small as `leep-server repack --full` makes the same store.
    let full = full_repack_bytes(&copy, &meta).await;
    assert!(
        r.pack_bytes as f64 <= full as f64 * 1.05,
        "{} bytes, a full repack {full}",
        r.pack_bytes
    );
    bg.finish_all().await.unwrap();
    assert_eq!(store.loose_stats().unwrap(), (0, 0));
    let ids: HashSet<String> = store
        .pack_sizes()
        .unwrap()
        .into_iter()
        .map(|p| p.id)
        .collect();
    let details = store.pack_details(&ids).unwrap();
    assert_eq!(details.len(), 1);
    assert!(!details[0].whole());
    assert!(
        r.pack_bytes * 2 < r.pack_bytes_before + r.loose_bytes_before,
        "{r:?}"
    );
    assert!(store_bytes(dir.path()) < before);

    // Nothing waits now, its own pack included.
    assert_eq!(bg.tick().await.unwrap().auto, AutoOutcome::Nothing);
    assert_main_readable(Arc::new(FsBlobStore::new(dir.path())), &meta, "repacked").await;
    check_blob_store_contract(&FsBlobStore::new(dir.path())).await;
    assert_main_readable(Arc::new(FsBlobStore::new(dir.path())), &meta, "contract").await;
}

/// New versions landed loose after a repack are compressed against each
/// other; the first new version at each path has its base in the older
/// pack, which the geometric rule leaves alone here, so it is the only
/// one stored whole (ADR 0039: a pack is self-contained).
#[tokio::test]
async fn later_runs_store_one_whole_frame_per_path() {
    let dir = tempfile::tempdir().unwrap();
    let store = fs_store(dir.path());
    let meta = Arc::new(MemoryMetaStore::new());
    let repo = Repo::new(Arc::new(store.clone()), meta.clone());
    import(&repo, &meta, 30).await;
    let bg = BackgroundRepack::new(PackStore::Fs(store.clone()), opts(eager()));
    let walk = bg.walk_repo(meta.clone());
    let mut bg = bg.with_repo(walk);
    ran(bg.tick().await.unwrap().auto);
    bg.finish_all().await.unwrap();
    for v in 30..34 {
        land_one(&repo, &meta, v).await;
    }
    let r = ran(bg.tick().await.unwrap().auto);
    bg.finish_all().await.unwrap();
    // Each land: three contents and three tree nodes (/, src, src/deep).
    assert_eq!((r.loose_before, r.packs_merged), (24, 0), "{r:?}");
    // One whole frame per path (small tree nodes may be smaller whole).
    assert!(r.whole >= 6 && r.deltas > r.whole, "{r:?}");
    assert_main_readable(Arc::new(FsBlobStore::new(dir.path())), &meta, "second run").await;
}

/// Below every threshold nothing runs and nothing is written; a check
/// waits for its interval; and objects that waited `max_age` run anyway.
/// The clock is tokio's, paused: nothing here measures the wall.
#[tokio::test(start_paused = true)]
async fn nothing_runs_below_the_thresholds_until_max_age() {
    let dir = tempfile::tempdir().unwrap();
    let store = fs_store(dir.path());
    let meta = Arc::new(MemoryMetaStore::new());
    let repo = Repo::new(Arc::new(store.clone()), meta.clone());
    let none = meta.get_ref("main").await.unwrap();
    let body = version("a.txt", 3);
    repo.blobs().put(Bytes::from(body.clone())).await.unwrap();
    let tip = commit(&repo, None, &[("a.txt", body)]).await;
    assert!(meta.cas_ref("main", none, Some(tip)).await.unwrap());
    let (loose, loose_bytes) = store.loose_stats().unwrap();
    assert_eq!(loose, 2, "a content and a root node");
    let auto = AutoRepackOptions {
        objects: loose + 1,
        bytes: loose_bytes + 1,
        max_age: Some(Duration::from_secs(600)),
        check_interval: Duration::from_secs(60),
        ..AutoRepackOptions::default()
    };
    let bg = BackgroundRepack::new(PackStore::Fs(store.clone()), opts(auto.clone()));
    let walk = bg.walk_repo(meta.clone());
    let mut bg = bg.with_repo(walk);
    let want = AutoOutcome::BelowThreshold {
        objects: loose,
        bytes: loose_bytes,
    };
    assert_eq!(bg.tick().await.unwrap().auto, want);
    assert_eq!(bg.tick().await.unwrap().auto, AutoOutcome::NotChecked);
    tokio::time::advance(Duration::from_secs(61)).await;
    assert_eq!(bg.tick().await.unwrap().auto, want);
    assert_eq!(store.pack_count().unwrap(), 0, "nothing was written");
    assert_eq!(store.loose_stats().unwrap().0, loose);
    tokio::time::advance(Duration::from_secs(600)).await;
    let r = ran(bg.tick().await.unwrap().auto);
    assert_eq!(r.objects, loose);

    // The count alone triggers a run, and so do the bytes alone.
    for (objects, bytes) in [(loose, u64::MAX), (u64::MAX, loose_bytes)] {
        let dir = tempfile::tempdir().unwrap();
        let store = fs_store(dir.path());
        let meta = Arc::new(MemoryMetaStore::new());
        let repo = Repo::new(Arc::new(store.clone()), meta.clone());
        let body = version("a.txt", 3);
        repo.blobs().put(Bytes::from(body.clone())).await.unwrap();
        let tip = commit(&repo, None, &[("a.txt", body)]).await;
        assert!(meta.cas_ref("main", None, Some(tip)).await.unwrap());
        let auto = AutoRepackOptions {
            objects,
            bytes,
            max_age: None,
            ..auto.clone()
        };
        let bg = BackgroundRepack::new(PackStore::Fs(store.clone()), opts(auto));
        let walk = bg.walk_repo(meta.clone());
        let mut bg = bg.with_repo(walk);
        ran(bg.tick().await.unwrap().auto);
    }
}

/// Cleanups waiting on more bytes than the cap (a share of the live
/// store) hold the next run back until they finish; the fence stays as it
/// is.
#[tokio::test]
async fn bytes_waiting_for_cleanup_hold_the_next_run() {
    let dir = tempfile::tempdir().unwrap();
    let store = fs_store(dir.path());
    let meta = Arc::new(MemoryMetaStore::new());
    let repo = Repo::new(Arc::new(store.clone()), meta.clone());
    import(&repo, &meta, 20).await;
    let o = BackgroundRepackOptions {
        fence: Duration::from_secs(3600),
        pending_share: 0.0,
        min_pending_bytes: 1,
        ..opts(eager())
    };
    assert_eq!(o.pending_cap(1 << 40), 1);
    let bg = BackgroundRepack::new(PackStore::Fs(store.clone()), o);
    let walk = bg.walk_repo(meta.clone());
    let mut bg = bg.with_repo(walk);
    ran(bg.tick().await.unwrap().auto);
    assert!(bg.pending_bytes() > 1);
    land_one(&repo, &meta, 20).await;
    let tick = bg.tick().await.unwrap();
    assert_eq!(tick.auto, AutoOutcome::Held("pending"));
    assert!(store.loose_stats().unwrap().0 > 0, "left loose meanwhile");
    // The cleanup is due once its fence passes; here it is forced.
    assert_eq!(bg.finish_all().await.unwrap(), 1);
    ran(bg.tick().await.unwrap().auto);
    bg.finish_all().await.unwrap();
    assert_main_readable(Arc::new(FsBlobStore::new(dir.path())), &meta, "held").await;

    // The cap scales with the store between its floor and its ceiling.
    let d = BackgroundRepackOptions::default();
    assert_eq!(d.pending_cap(0), 64 << 20);
    assert_eq!(d.pending_cap(1 << 30), 512 << 20);
    assert_eq!(d.pending_cap(1 << 40), 4 << 30);
}

/// An object store has no incremental repack: the task runs a full one,
/// whole import pack and loose objects included, and the next only after
/// its interval.
#[tokio::test]
async fn an_object_store_gets_a_full_repack_at_most_once_per_interval() {
    let dir = tempfile::tempdir().unwrap();
    let store = ObjectBlobStore::local(dir.path())
        .unwrap()
        .with_pack_writes(PackWrites {
            min_objects: 4,
            ..PackWrites::default()
        });
    let meta = Arc::new(MemoryMetaStore::new());
    let repo = Repo::new(Arc::new(store.clone()), meta.clone());
    import(&repo, &meta, 30).await;
    assert_eq!(store.pack_count().await.unwrap(), 1);
    let auto = AutoRepackOptions {
        object_store_interval: Duration::from_secs(3600),
        ..eager()
    };
    let bg = BackgroundRepack::new(PackStore::Object(store.clone()), opts(auto));
    let walk = bg.walk_repo(meta.clone());
    let mut bg = bg.with_repo(walk);
    let r = ran(bg.tick().await.unwrap().auto);
    assert_eq!(r.packs_merged, 1, "{r:?}");
    assert!(r.loose_before > 0 && r.deltas > 3 * r.whole, "{r:?}");
    bg.finish_all().await.unwrap();
    assert_eq!(store.loose_stats().await.unwrap(), (0, 0));
    land_one(&repo, &meta, 30).await;
    assert_eq!(bg.tick().await.unwrap().auto, AutoOutcome::Held("interval"));
    // A task whose interval has passed (here: a new one) runs again.
    let bg = BackgroundRepack::new(PackStore::Object(store.clone()), opts(eager()));
    let walk = bg.walk_repo(meta.clone());
    let mut bg = bg.with_repo(walk);
    let r = ran(bg.tick().await.unwrap().auto);
    assert_eq!(r.loose_before, 6, "{r:?}");
    bg.finish_all().await.unwrap();
    let fresh = ObjectBlobStore::local(dir.path()).unwrap();
    assert_main_readable(Arc::new(fresh.clone()), &meta, "object store").await;
    check_blob_store_contract(&fresh).await;
}

/// A clock a minute ahead, so blobs written just now are older than the
/// start of a run.
struct Ahead;

#[async_trait]
impl Clock for Ahead {
    fn now(&self) -> i64 {
        gc::SystemClock.now() + 60
    }
    async fn sleep(&self, _: Duration) {}
}

fn gc_opts() -> GcOptions {
    GcOptions {
        grace: Duration::ZERO,
        fence: Duration::ZERO,
        unsafe_no_fence: true,
        ..GcOptions::default()
    }
}

fn orphans(tag: &str) -> Vec<Bytes> {
    (0..20)
        .map(|i| Bytes::from(format!("orphan {tag} {i}\n")))
        .collect()
}

/// GC beside the delta repack: objects condemned before a run and
/// collected while it writes stay deleted (tombstoned in the new pack as
/// late deletes), objects uploaded again after their condemnation survive,
/// and everything `main` reaches stays readable. Run with several seeds of
/// interleaving: collect before, during and after the run's publish.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gc_beside_the_delta_repack_deletes_and_rescues_what_it_should() {
    for order in 0..3 {
        let dir = tempfile::tempdir().unwrap();
        let store = fs_store(dir.path());
        let meta = Arc::new(MemoryMetaStore::new());
        let repo = Repo::new(Arc::new(store.clone()), meta.clone());
        import(&repo, &meta, 20).await;
        let doomed = repo.put_blobs(orphans("doomed")).await.unwrap();
        let saved = repo.put_blobs(orphans("saved")).await.unwrap();
        let lone = orphans("lone").remove(0);
        let lone_hash = repo.put_blob(lone.clone()).await.unwrap();
        assert!(store.is_loose(&lone_hash));
        let condemned = gc::condemn(&repo, &gc_opts(), &Ahead).await.unwrap();
        assert!(condemned.condemned >= 41, "{condemned:?}");
        // Rescued: uploaded again after the condemnation, as a pack and
        // loose.
        repo.put_blobs(orphans("saved")).await.unwrap();
        repo.put_blob(lone.clone()).await.unwrap();

        let bg = BackgroundRepack::new(PackStore::Fs(store.clone()), opts(eager()));
        let walk = bg.walk_repo(meta.clone());
        let mut bg = bg.with_repo(walk);
        match order {
            0 => {
                gc::collect(&repo, &gc_opts(), &Ahead).await.unwrap();
                ran(bg.tick().await.unwrap().auto);
            }
            1 => {
                let g = gc_opts();
                let (tick, collected) = tokio::join!(bg.tick(), gc::collect(&repo, &g, &Ahead));
                collected.unwrap();
                ran(tick.unwrap().auto);
            }
            _ => {
                ran(bg.tick().await.unwrap().auto);
                gc::collect(&repo, &gc_opts(), &Ahead).await.unwrap();
            }
        }
        bg.finish_all().await.unwrap();
        let what = format!("order {order}");
        let fresh = FsBlobStore::new(dir.path());
        for h in &doomed {
            assert!(!fresh.contains(h).await.unwrap(), "{what}: {h} collected");
            assert!(fresh.get(h).await.unwrap().is_none(), "{what}");
        }
        for h in saved.iter().chain([&lone_hash]) {
            assert!(fresh.contains(h).await.unwrap(), "{what}: {h} rescued");
        }
        assert_main_readable(Arc::new(fresh), &meta, &what).await;
        // A second run (whole packs from the rescue uploads remain) keeps
        // all of that true.
        let _ = bg.tick().await.unwrap();
        bg.finish_all().await.unwrap();
        let fresh = FsBlobStore::new(dir.path());
        for h in &doomed {
            assert!(!fresh.contains(h).await.unwrap(), "{what}, again: {h}");
        }
        for h in saved.iter().chain([&lone_hash]) {
            assert!(fresh.contains(h).await.unwrap(), "{what}, again: {h}");
        }
        assert_main_readable(Arc::new(fresh), &meta, &what).await;
    }
}

/// With the delta repack on, packs written whole merge only with each
/// other, so a whole frame never hides in a delta pack before it is
/// compressed.
#[tokio::test]
async fn whole_packs_and_delta_packs_merge_apart() {
    let dir = tempfile::tempdir().unwrap();
    let store = fs_store(dir.path());
    let meta = Arc::new(MemoryMetaStore::new());
    let repo = Repo::new(Arc::new(store.clone()), meta.clone());
    import(&repo, &meta, 10).await;
    let never = AutoRepackOptions {
        objects: u64::MAX,
        bytes: u64::MAX,
        max_age: None,
        check_interval: Duration::ZERO,
        ..AutoRepackOptions::default()
    };
    // One delta pack first.
    let bg = BackgroundRepack::new(PackStore::Fs(store.clone()), opts(eager()));
    let walk = bg.walk_repo(meta.clone());
    let mut bg = bg.with_repo(walk);
    ran(bg.tick().await.unwrap().auto);
    bg.finish_all().await.unwrap();
    // Then many small whole packs, as land batches write them.
    for b in 0..8 {
        let items: Vec<(Hash, Bytes)> = (0..5)
            .map(|i| {
                let d = Bytes::from(format!("batch {b} object {i}\n"));
                (Hash::of_blob(&d), d)
            })
            .collect();
        store.put_many(items).await.unwrap();
    }
    let o = BackgroundRepackOptions {
        trigger_packs: 2,
        fanout: 2,
        ..opts(never)
    };
    let bg = BackgroundRepack::new(PackStore::Fs(store.clone()), o);
    let walk = bg.walk_repo(meta.clone());
    let mut bg = bg.with_repo(walk);
    let tick = bg.tick().await.unwrap();
    assert!(!tick.merged.is_empty(), "{tick:?}");
    bg.finish_all().await.unwrap();
    let ids: HashSet<String> = store
        .pack_sizes()
        .unwrap()
        .into_iter()
        .map(|p| p.id)
        .collect();
    let details = store.pack_details(&ids).unwrap();
    let deltas = details.iter().filter(|d| d.deltas > 0).count();
    assert_eq!(deltas, 1, "the delta pack merged with nothing: {details:?}");
    let whole: u64 = details
        .iter()
        .filter(|d| d.whole())
        .map(|d| d.objects)
        .sum();
    assert_eq!(whole, 40, "every land object is still in a whole pack");
}

/// The loose copies a run packed stay on disk until its cleanup, a fence
/// later: they are not counted as waiting, and no run packs them again
/// meanwhile, even with the byte cap far away.
#[tokio::test]
async fn loose_copies_waiting_for_cleanup_are_not_packed_twice() {
    let dir = tempfile::tempdir().unwrap();
    let store = fs_store(dir.path());
    let meta = Arc::new(MemoryMetaStore::new());
    let repo = Repo::new(Arc::new(store.clone()), meta.clone());
    import(&repo, &meta, 10).await;
    let o = BackgroundRepackOptions {
        fence: Duration::from_secs(3600),
        min_pending_bytes: 1 << 40,
        max_pending_bytes: 1 << 40,
        ..opts(eager())
    };
    let bg = BackgroundRepack::new(PackStore::Fs(store.clone()), o);
    let walk = bg.walk_repo(meta.clone());
    let mut bg = bg.with_repo(walk);
    let r = ran(bg.tick().await.unwrap().auto);
    assert!(r.loose_before > 0);
    assert!(
        store.loose_stats().unwrap().0 >= r.loose_before,
        "still on disk"
    );
    assert_eq!(bg.tick().await.unwrap().auto, AutoOutcome::Nothing);
    land_one(&repo, &meta, 10).await;
    assert_eq!(bg.tick().await.unwrap().auto, AutoOutcome::Held("pending"));
    assert_eq!(store.pack_count().unwrap(), 1, "no second pack");
    bg.finish_all().await.unwrap();
    let r = ran(bg.tick().await.unwrap().auto);
    assert_eq!(r.loose_before, 6, "only the new land: {r:?}");
    bg.finish_all().await.unwrap();
    assert_main_readable(Arc::new(FsBlobStore::new(dir.path())), &meta, "twice").await;
}
