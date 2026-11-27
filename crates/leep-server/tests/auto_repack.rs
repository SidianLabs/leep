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
    AutoOutcome, AutoRepackOptions, BackgroundRepack, BackgroundRepackOptions, ConsolidateOptions,
    ConsolidateOutcome, PackStore,
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
    full_repack(root, meta).await.pack_bytes
}

/// What `leep-server repack --full` makes of the store at `root`.
async fn full_repack(
    root: &tempfile::TempDir,
    meta: &Arc<MemoryMetaStore>,
) -> leep_store::RepackReport {
    let store = FsBlobStore::new(root.path());
    let repo = Repo::new(Arc::new(store.clone()), meta.clone());
    let order = leep_derive::pack_order::pack_order(&repo).await.unwrap();
    let opts = leep_store::RepackOptions {
        threads: 1,
        ..leep_store::RepackOptions::default()
    };
    let p = store.repack_publish(order.objects, opts).await.unwrap();
    p.report().clone()
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

fn no_consolidation() -> AutoRepackOptions {
    AutoRepackOptions {
        consolidate: None,
        ..eager()
    }
}

/// Consolidation with only the ratio rule, at `1 / ratio`.
fn consolidating(ratio: u64) -> AutoRepackOptions {
    AutoRepackOptions {
        consolidate: Some(ConsolidateOptions {
            ratio,
            age: None,
            ..ConsolidateOptions::default()
        }),
        ..eager()
    }
}

fn task(
    store: &FsBlobStore,
    meta: &Arc<MemoryMetaStore>,
    o: BackgroundRepackOptions,
) -> BackgroundRepack {
    let bg = BackgroundRepack::new(PackStore::Fs(store.clone()), o);
    let walk = bg.walk_repo(meta.clone());
    bg.with_repo(walk)
}

fn consolidated(o: ConsolidateOutcome) -> (leep_store::RepackReport, u64, u64) {
    match o {
        ConsolidateOutcome::Ran {
            report,
            gain,
            walked,
        } => (*report, gain, walked),
        other => panic!("expected a consolidation, got {other:?}"),
    }
}

/// Frames stored whole across the store's packs.
fn whole_frames(store: &FsBlobStore) -> u64 {
    let ids: HashSet<String> = store
        .pack_sizes()
        .unwrap()
        .into_iter()
        .map(|p| p.id)
        .collect();
    let details = store.pack_details(&ids).unwrap();
    details.iter().map(|d| d.objects - d.deltas).sum()
}

/// An import compressed by one delta run, then `rounds` rounds of two
/// lands, each compressed by a run of its own without consolidation: each
/// run stores the first new version at each path whole, its previous
/// version being in an older pack.
async fn heads_world(dir: &Path, rounds: usize) -> (FsBlobStore, Arc<MemoryMetaStore>, Repo) {
    let store = fs_store(dir);
    let meta = Arc::new(MemoryMetaStore::new());
    let repo = Repo::new(Arc::new(store.clone()), meta.clone());
    import(&repo, &meta, 30).await;
    let mut bg = task(&store, &meta, opts(no_consolidation()));
    ran(bg.tick().await.unwrap().auto);
    bg.finish_all().await.unwrap();
    for round in 0..rounds {
        for v in 0..2 {
            land_one(&repo, &meta, 30 + 2 * round + v).await;
        }
        let tick = bg.tick().await.unwrap();
        ran(tick.auto);
        assert_eq!(tick.consolidate, ConsolidateOutcome::NotChecked);
        bg.finish_all().await.unwrap();
    }
    (store, meta, repo)
}

/// The first new version at each path that a delta run stored whole (its
/// base in an older pack, ADR 0039) is compressed against that base once
/// the consolidation merges the packs (ADR 0107): afterwards only the first
/// versions are whole, the store is as small as `repack --full` makes it,
/// and neither the next check nor a restarted task rewrites it again.
#[tokio::test]
async fn after_a_consolidation_only_first_versions_are_whole() {
    let dir = tempfile::tempdir().unwrap();
    let (store, meta, _repo) = heads_world(dir.path(), 4).await;
    let before = whole_frames(&store);
    // Each land: three contents and three tree nodes (/, src, src/deep),
    // whole at the first version of each run.
    assert!(before >= 6 * 5, "{before} whole frames");
    let copy = tempfile::tempdir().unwrap();
    copy_dir(dir.path(), copy.path());

    // Any gain is worth it here (the world is small).
    let mut bg = task(&store, &meta, opts(consolidating(u64::MAX)));
    let tick = bg.tick().await.unwrap();
    assert_eq!(tick.auto, AutoOutcome::Nothing);
    let (r, gain, walked) = consolidated(tick.consolidate);
    assert!(r.packs_merged >= 2 && gain > 0 && walked > 0, "{r:?}");
    bg.finish_all().await.unwrap();
    assert_eq!(store.pack_sizes().unwrap().len(), 1);
    // As a full repack: the first versions, and the small tree nodes a
    // delta does not shrink.
    let full = full_repack(&copy, &meta).await;
    let after = whole_frames(&store);
    assert!(
        after <= full.whole && after < before,
        "{after} whole (was {before}, a full repack {}), {r:?}",
        full.whole
    );
    assert!(
        r.pack_bytes as f64 <= full.pack_bytes as f64 * 1.05,
        "{} bytes, a full repack {}",
        r.pack_bytes,
        full.pack_bytes
    );
    assert_main_readable(
        Arc::new(FsBlobStore::new(dir.path())),
        &meta,
        "consolidated",
    )
    .await;

    // Nothing more to gain: no rewrite, now or after a restart.
    let tick = bg.tick().await.unwrap();
    assert_eq!(tick.consolidate, ConsolidateOutcome::Nothing);
    let mut bg = task(&store, &meta, opts(consolidating(u64::MAX)));
    let tick = bg.tick().await.unwrap();
    assert!(
        matches!(
            tick.consolidate,
            ConsolidateOutcome::Nothing | ConsolidateOutcome::Below { gain: 0, .. }
        ),
        "{:?}",
        tick.consolidate
    );
    check_blob_store_contract(&FsBlobStore::new(dir.path())).await;
    assert_main_readable(Arc::new(FsBlobStore::new(dir.path())), &meta, "contract").await;
}

/// New files have no previous version: however many runs store them
/// whole, they never make a consolidation worth it. Only the tree nodes
/// that name them have bases to gain against.
#[tokio::test]
async fn new_files_alone_never_trigger_a_rewrite() {
    let dir = tempfile::tempdir().unwrap();
    let store = fs_store(dir.path());
    let meta = Arc::new(MemoryMetaStore::new());
    let repo = Repo::new(Arc::new(store.clone()), meta.clone());
    let mut bg = task(&store, &meta, opts(consolidating(8)));
    let mut last = None;
    for round in 0..6 {
        let mut files = Vec::new();
        for i in 0..4 {
            let body: String = (0..300)
                .map(|l| {
                    format!(
                        "{round}.{i}.{l}: {}\n",
                        Hash::of_blob(format!("{round} {i} {l}").as_bytes())
                    )
                })
                .collect();
            repo.blobs().put(Bytes::from(body.clone())).await.unwrap();
            files.push((format!("new/{round}/{i}.txt"), body));
        }
        let refs: Vec<(&str, String)> =
            files.iter().map(|(p, b)| (p.as_str(), b.clone())).collect();
        let head = meta.get_ref("main").await.unwrap();
        let tip = commit(&repo, head, &refs).await;
        assert!(meta.cas_ref("main", head, Some(tip)).await.unwrap());
        ran(bg.tick().await.unwrap().auto);
        bg.finish_all().await.unwrap();
        let tick = bg.tick().await.unwrap();
        assert_eq!(tick.auto, AutoOutcome::Nothing);
        match tick.consolidate {
            ConsolidateOutcome::Nothing => {}
            ConsolidateOutcome::Below { gain, .. } => last = Some(gain),
            other => panic!("round {round}: {other:?}"),
        }
    }
    let whole: u64 = {
        let ids: HashSet<String> = store
            .pack_sizes()
            .unwrap()
            .into_iter()
            .map(|p| p.id)
            .collect();
        store
            .pack_details(&ids)
            .unwrap()
            .iter()
            .map(|d| d.whole_bytes)
            .sum()
    };
    let gain = last.expect("a walk judged the packs");
    assert!(gain * 20 < whole, "only tree nodes gain: {gain} of {whole}");
}

/// The ratio rule runs a consolidation at once; below it, the gain is
/// remembered (no walk while the packs stay the same) and the age rule
/// runs it once the gain has waited long enough.
#[tokio::test(start_paused = true)]
async fn the_ratio_and_age_rules() {
    let dir = tempfile::tempdir().unwrap();
    let (store, meta, _) = heads_world(dir.path(), 2).await;
    let copy = tempfile::tempdir().unwrap();
    copy_dir(dir.path(), copy.path());
    let aged = |ratio| AutoRepackOptions {
        consolidate: Some(ConsolidateOptions {
            ratio,
            age: Some(Duration::from_secs(600)),
            min_gain: 1,
            ..ConsolidateOptions::default()
        }),
        ..eager()
    };

    // Below the ratio (all of the packs' bytes), it waits, without
    // walking again while the packs stay the same.
    let mut bg = task(&store, &meta, opts(aged(1)));
    let tick = bg.tick().await.unwrap();
    let ConsolidateOutcome::Below { gain, walked } = tick.consolidate else {
        panic!("{:?}", tick.consolidate)
    };
    assert!(gain > 0 && walked > 0, "{gain} {walked}");
    tokio::time::advance(Duration::from_secs(300)).await;
    let tick = bg.tick().await.unwrap();
    assert_eq!(
        tick.consolidate,
        ConsolidateOutcome::Below { gain, walked: 0 }
    );
    tokio::time::advance(Duration::from_secs(301)).await;
    let (_, g, _) = consolidated(bg.tick().await.unwrap().consolidate);
    assert_eq!(g, gain);
    bg.finish_all().await.unwrap();
    assert_main_readable(Arc::new(FsBlobStore::new(dir.path())), &meta, "age").await;

    // A ratio the gain reaches runs at once; with no age rule, one it
    // does not reach never runs.
    let store = fs_store(copy.path());
    let mut bg = task(&store, &meta, opts(consolidating(1)));
    let tick = bg.tick().await.unwrap();
    assert!(
        matches!(
            tick.consolidate,
            ConsolidateOutcome::Nothing | ConsolidateOutcome::Below { .. }
        ),
        "{:?}",
        tick.consolidate
    );
    tokio::time::advance(Duration::from_secs(3600)).await;
    let tick = bg.tick().await.unwrap();
    assert!(!matches!(tick.consolidate, ConsolidateOutcome::Ran { .. }));
    let ratio = (store
        .pack_sizes()
        .unwrap()
        .iter()
        .map(|p| p.bytes)
        .sum::<u64>()
        / gain)
        .max(1);
    let mut bg = task(&store, &meta, opts(consolidating(ratio + 1)));
    let (_, g, _) = consolidated(bg.tick().await.unwrap().consolidate);
    assert_eq!(g, gain);
    bg.finish_all().await.unwrap();
    assert_main_readable(Arc::new(FsBlobStore::new(copy.path())), &meta, "ratio").await;
}

/// The largest pack's history is walked once per process (it may hold
/// frames older merges left whole); after that, a consolidation walks only
/// the changesets written since it.
#[tokio::test]
async fn the_walk_stops_at_the_largest_pack() {
    let dir = tempfile::tempdir().unwrap();
    let store = fs_store(dir.path());
    let meta = Arc::new(MemoryMetaStore::new());
    let repo = Repo::new(Arc::new(store.clone()), meta.clone());
    import(&repo, &meta, 60).await;
    let mut bg = task(&store, &meta, opts(consolidating(u64::MAX)));
    // The check after the run walks the largest pack's own history:
    // nothing there has a base to gain against.
    let tick = bg.tick().await.unwrap();
    ran(tick.auto);
    bg.finish_all().await.unwrap();
    let ConsolidateOutcome::Below { gain: 0, walked } = tick.consolidate else {
        panic!("{:?}", tick.consolidate)
    };
    assert!(walked >= 60, "{walked}");
    for v in 60..63 {
        land_one(&repo, &meta, v).await;
    }
    let before = whole_frames(&store);
    let tick = bg.tick().await.unwrap();
    let run = ran(tick.auto);
    let (r, gain, walked) = consolidated(tick.consolidate);
    assert_eq!(walked, 3, "only the three new changesets: {r:?}");
    assert!(gain > 0 && r.packs_merged == 2, "{r:?}");
    bg.finish_all().await.unwrap();
    // The run stored the first new version at each path whole.
    assert!(r.whole < before + run.whole, "{r:?} {run:?}");
    assert_main_readable(Arc::new(FsBlobStore::new(dir.path())), &meta, "walk").await;
}

/// A consolidation may pass the cap on bytes waiting for cleanup when no
/// other cleanup waits (a store that is mostly one pack could never
/// consolidate otherwise), and is held while a large one does; delta runs
/// then wait for it, so a busy store still consolidates.
#[tokio::test]
async fn a_consolidation_passes_the_pending_cap_only_alone() {
    let dir = tempfile::tempdir().unwrap();
    let (store, meta, repo) = heads_world(dir.path(), 2).await;
    let o = BackgroundRepackOptions {
        fence: Duration::from_secs(3600),
        pending_share: 0.0,
        min_pending_bytes: 1,
        ..opts(consolidating(u64::MAX))
    };
    let mut bg = task(&store, &meta, o);
    consolidated(bg.tick().await.unwrap().consolidate);
    assert_eq!(bg.pending(), 1);
    bg.finish_all().await.unwrap();
    // A delta pack `leep-server repack` wrote (heads included), then a
    // merge waiting for its fence.
    for v in 40..42 {
        land_one(&repo, &meta, v).await;
    }
    let walk = Repo::new(Arc::new(store.clone()), meta.clone());
    let loose: HashSet<Hash> = store
        .loose_listing()
        .unwrap()
        .into_iter()
        .map(|(h, _)| h)
        .collect();
    let order = leep_derive::pack_order::pack_order_loose(&walk, &|h: &Hash| loose.contains(h))
        .await
        .unwrap();
    let manual = leep_store::RepackOptions {
        mode: leep_store::RepackMode::Incremental {
            factor: 2,
            max_packs: 8,
        },
        fold_only: Some(Vec::new()),
        mark: true,
        ..leep_store::RepackOptions::default()
    };
    store
        .repack_publish(order.objects, manual)
        .await
        .unwrap()
        .finish()
        .await
        .unwrap();
    for b in 0..4 {
        let items: Vec<(Hash, Bytes)> = (0..5)
            .map(|i| Bytes::from(format!("land batch {b} object {i}\n")))
            .map(|d| (Hash::of_blob(&d), d))
            .collect();
        store.put_many(items).await.unwrap();
    }
    // No cleanup counts as small here, so the delta run's holds it.
    let mut bg = task(
        &store,
        &meta,
        BackgroundRepackOptions {
            fence: Duration::from_secs(3600),
            pending_share: 0.0,
            min_pending_bytes: 1,
            small_merge_bytes: 0,
            ..opts(consolidating(u64::MAX))
        },
    );
    let tick = bg.tick().await.unwrap();
    ran(tick.auto);
    assert_eq!(tick.consolidate, ConsolidateOutcome::Held("pending"));
    // Delta runs wait for the held consolidation: no new cleanup meanwhile.
    for v in 42..44 {
        land_one(&repo, &meta, v).await;
    }
    let tick = bg.tick().await.unwrap();
    assert_eq!(tick.auto, AutoOutcome::Held("consolidation"));
    assert_eq!(tick.consolidate, ConsolidateOutcome::Held("pending"));
    bg.finish_all().await.unwrap();
    let tick = bg.tick().await.unwrap();
    consolidated(tick.consolidate);
    bg.finish_all().await.unwrap();
    ran(bg.tick().await.unwrap().auto);
    bg.finish_all().await.unwrap();
    assert_main_readable(Arc::new(FsBlobStore::new(dir.path())), &meta, "pending").await;
}

/// Frames the writer keeps whole because their base is at the chain cap
/// (every 64th version of a file edited often) gain nothing: a restarted
/// task, walking the largest pack's own history, does not rewrite it.
#[tokio::test]
async fn whole_frames_at_the_chain_cap_are_not_a_gain() {
    let dir = tempfile::tempdir().unwrap();
    let store = fs_store(dir.path());
    let meta = Arc::new(MemoryMetaStore::new());
    let repo = Repo::new(Arc::new(store.clone()), meta.clone());
    import(&repo, &meta, 150).await;
    let mut bg = task(&store, &meta, opts(consolidating(u64::MAX)));
    let r = ran(bg.tick().await.unwrap().auto);
    bg.finish_all().await.unwrap();
    // Three files of 150 versions: chains of 64 start whole twice more.
    assert!(r.whole >= 6 + 6, "{r:?}");
    for restart in 0..2 {
        let mut bg = task(&store, &meta, opts(consolidating(u64::MAX)));
        let tick = bg.tick().await.unwrap();
        assert!(
            matches!(
                tick.consolidate,
                ConsolidateOutcome::Nothing | ConsolidateOutcome::Below { gain: 0, .. }
            ),
            "restart {restart}: {:?}",
            tick.consolidate
        );
    }
}

/// Objects GC deleted from delta packs count toward the gain: a
/// consolidation drops their bytes (dead segment blobs, history the
/// changeset sweep removed) instead of leaving them until `repack --full`.
#[tokio::test]
async fn deleted_objects_in_delta_packs_are_dropped() {
    let dir = tempfile::tempdir().unwrap();
    let (store, meta, repo) = heads_world(dir.path(), 1).await;
    // Uploads no ref reaches, packed whole by a delta run, then deleted.
    let junk: Vec<Bytes> = (0..40)
        .map(|i| Bytes::from(format!("{i}: {}\n", "unreferenced bytes ".repeat(200 + i))))
        .collect();
    let hashes = repo.put_blobs(junk).await.unwrap();
    let mut bg = task(&store, &meta, opts(consolidating(8)));
    ran(bg.tick().await.unwrap().auto);
    bg.finish_all().await.unwrap();
    for h in &hashes {
        store.delete(h).await.unwrap();
    }
    let tick = bg.tick().await.unwrap();
    let (r, gain, _) = consolidated(tick.consolidate);
    assert_eq!(r.dropped_tombstoned, 40, "{r:?}");
    assert!(gain >= r.pack_bytes_before / 8, "{gain} {r:?}");
    bg.finish_all().await.unwrap();
    let fresh = FsBlobStore::new(dir.path());
    for h in &hashes {
        assert!(!fresh.contains(h).await.unwrap());
    }
    assert_main_readable(Arc::new(fresh), &meta, "dropped").await;
}

/// A delta pack over the size limit is never rewritten in the background:
/// the others are judged without it (their heads based in it gain nothing
/// then), and a larger limit takes it in.
#[tokio::test]
async fn packs_over_the_limit_are_left_alone() {
    let dir = tempfile::tempdir().unwrap();
    let (store, meta, _) = heads_world(dir.path(), 2).await;
    let mut sizes = store.pack_sizes().unwrap();
    sizes.sort_by_key(|p| p.bytes);
    assert!(sizes.len() >= 2, "{sizes:?}");
    let largest = sizes.last().unwrap().clone();
    let copy = tempfile::tempdir().unwrap();
    copy_dir(dir.path(), copy.path());
    let limited = |max_bytes| AutoRepackOptions {
        consolidate: Some(ConsolidateOptions {
            ratio: u64::MAX,
            age: None,
            max_bytes,
            ..ConsolidateOptions::default()
        }),
        ..eager()
    };
    let mut bg = task(&store, &meta, opts(limited(largest.bytes - 1)));
    let tick = bg.tick().await.unwrap();
    if let ConsolidateOutcome::Ran { report, .. } = &tick.consolidate {
        assert_eq!(report.packs_merged as usize, sizes.len() - 1, "{report:?}");
    }
    bg.finish_all().await.unwrap();
    assert!(store
        .pack_sizes()
        .unwrap()
        .iter()
        .any(|p| p.id == largest.id));
    let sizes = store.pack_sizes().unwrap();
    let mut bg = task(&store, &meta, opts(limited(largest.bytes)));
    let (r, _, _) = consolidated(bg.tick().await.unwrap().consolidate);
    assert_eq!(r.packs_merged as usize, sizes.len(), "{r:?}");
    bg.finish_all().await.unwrap();
    assert_main_readable(Arc::new(FsBlobStore::new(dir.path())), &meta, "limit").await;

    // The same through the budget: one consolidation rewrites at most half
    // the ceiling on bytes waiting for cleanup, the largest packs left out,
    // so the exemption from the cap never lets more through.
    let store = fs_store(copy.path());
    let sizes = store.pack_sizes().unwrap();
    let rest = sizes.iter().map(|p| p.bytes).sum::<u64>() - largest.bytes;
    let o = BackgroundRepackOptions {
        max_pending_bytes: 2 * rest,
        ..opts(limited(u64::MAX))
    };
    let mut bg = task(&store, &meta, o);
    let tick = bg.tick().await.unwrap();
    if let ConsolidateOutcome::Ran { report, .. } = &tick.consolidate {
        assert_eq!(report.packs_merged as usize, sizes.len() - 1, "{report:?}");
        assert!(bg.pending_bytes() <= rest, "{report:?}");
    }
    bg.finish_all().await.unwrap();
    assert!(store
        .pack_sizes()
        .unwrap()
        .iter()
        .any(|p| p.id == largest.id));
    assert_main_readable(Arc::new(FsBlobStore::new(copy.path())), &meta, "budget").await;
}

/// A store whose one delta pack holds frames whole next to their bases,
/// as merges that copy frames (and servers before ADR 0107) left them: the
/// first check walks that pack's own history and rewrites it alone, with
/// no manual `repack --full`; afterwards nothing more is gained.
#[tokio::test]
async fn a_single_pack_left_by_older_merges_is_consolidated() {
    let dir = tempfile::tempdir().unwrap();
    let (store, meta, _) = heads_world(dir.path(), 3).await;
    // A merge that copies frames, as the tiered merges and older servers
    // did: one pack, its heads whole beside their bases.
    let ids: Vec<String> = store
        .pack_sizes()
        .unwrap()
        .into_iter()
        .map(|p| p.id)
        .collect();
    assert!(ids.len() >= 3, "{ids:?}");
    let merge = leep_store::RepackOptions {
        mode: leep_store::RepackMode::Planned { loose: false },
        packs: ids,
        mark: true,
        threads: 1,
        ..leep_store::RepackOptions::default()
    };
    store
        .repack_publish(Vec::<(Hash, Option<Hash>)>::new(), merge)
        .await
        .unwrap()
        .finish()
        .await
        .unwrap();
    assert_eq!(store.pack_sizes().unwrap().len(), 1);
    let before = whole_frames(&store);
    let copy = tempfile::tempdir().unwrap();
    copy_dir(dir.path(), copy.path());

    let mut bg = task(&store, &meta, opts(consolidating(u64::MAX)));
    let tick = bg.tick().await.unwrap();
    assert_eq!(tick.auto, AutoOutcome::Nothing);
    let (r, gain, walked) = consolidated(tick.consolidate);
    assert_eq!(r.packs_merged, 1, "{r:?}");
    assert!(gain > 0 && walked > 0, "{gain} {walked}");
    bg.finish_all().await.unwrap();
    let after = whole_frames(&store);
    let full = full_repack(&copy, &meta).await;
    assert!(
        after < before && after <= full.whole,
        "{after} whole (was {before}, a full repack {})",
        full.whole
    );
    assert_main_readable(Arc::new(FsBlobStore::new(dir.path())), &meta, "single").await;
    for restart in 0..2 {
        let mut bg = task(&store, &meta, opts(consolidating(u64::MAX)));
        let tick = bg.tick().await.unwrap();
        assert!(
            matches!(
                tick.consolidate,
                ConsolidateOutcome::Nothing | ConsolidateOutcome::Below { gain: 0, .. }
            ),
            "restart {restart}: {:?}",
            tick.consolidate
        );
    }
    check_blob_store_contract(&FsBlobStore::new(dir.path())).await;
}

/// The age rule holds across restarts: a gain below the ratio rule runs
/// once the packs holding it have waited the age (the largest pack's file
/// time), even in a process that has just started, as after an upgrade
/// that restarts the server more often than the age.
#[tokio::test]
async fn the_age_rule_survives_restarts() {
    let dir = tempfile::tempdir().unwrap();
    let (store, meta, _) = heads_world(dir.path(), 3).await;
    let ids: Vec<String> = store
        .pack_sizes()
        .unwrap()
        .into_iter()
        .map(|p| p.id)
        .collect();
    let merge = leep_store::RepackOptions {
        mode: leep_store::RepackMode::Planned { loose: false },
        packs: ids,
        mark: true,
        threads: 1,
        ..leep_store::RepackOptions::default()
    };
    store
        .repack_publish(Vec::<(Hash, Option<Hash>)>::new(), merge)
        .await
        .unwrap()
        .finish()
        .await
        .unwrap();
    let day = Duration::from_secs(24 * 3600);
    // The ratio rule never fires (the gain is not all of the bytes); the
    // age rule's floor is any gain.
    let aged = || AutoRepackOptions {
        consolidate: Some(ConsolidateOptions {
            ratio: 1,
            age: Some(day),
            min_gain: 1,
            ..ConsolidateOptions::default()
        }),
        ..eager()
    };
    let mut bg = task(&store, &meta, opts(aged()));
    let tick = bg.tick().await.unwrap();
    assert!(
        matches!(tick.consolidate, ConsolidateOutcome::Below { gain, .. } if gain > 0),
        "{:?}",
        tick.consolidate
    );
    // As if the pack were written two days ago: a new process runs it.
    let id = &store.pack_sizes().unwrap()[0].id;
    let file = std::fs::File::options()
        .write(true)
        .open(dir.path().join("packs").join(format!("{id}.pack")))
        .unwrap();
    file.set_modified(std::time::SystemTime::now() - 2 * day)
        .unwrap();
    drop(file);
    let mut bg = task(&store, &meta, opts(aged()));
    let (r, _, _) = consolidated(bg.tick().await.unwrap().consolidate);
    assert_eq!(r.packs_merged, 1, "{r:?}");
    bg.finish_all().await.unwrap();
    // The pack it wrote is new: no age-rule rewrite again at once.
    let mut bg = task(&store, &meta, opts(aged()));
    let tick = bg.tick().await.unwrap();
    assert!(
        !matches!(tick.consolidate, ConsolidateOutcome::Ran { .. }),
        "{:?}",
        tick.consolidate
    );
    assert_main_readable(Arc::new(FsBlobStore::new(dir.path())), &meta, "aged").await;
}

/// A consolidation whose walk fails is not tried again at every check:
/// it waits a minute (or the check interval), twice that after a second
/// failure in a row, and runs once the walk works again.
#[tokio::test(start_paused = true)]
async fn a_failed_consolidation_backs_off_then_runs() {
    use leep_testkit::{Fault, FaultPlan, FaultyMetaStore, Op};
    let dir = tempfile::tempdir().unwrap();
    let (store, meta, _) = heads_world(dir.path(), 2).await;
    let plan = FaultPlan::new(7);
    plan.rate(&[Op::ListRefs], 1000, Fault::Fail);
    let faulty: Arc<dyn MetaStore> = Arc::new(FaultyMetaStore::new(meta.clone(), plan.clone()));
    let bg = BackgroundRepack::new(PackStore::Fs(store.clone()), opts(consolidating(u64::MAX)));
    let walk = bg.walk_repo(faulty);
    let mut bg = bg.with_repo(walk);

    let tick = bg.tick().await.unwrap();
    assert!(
        matches!(tick.consolidate, ConsolidateOutcome::Failed(_)),
        "{:?}",
        tick.consolidate
    );
    let walks = plan.calls(Op::ListRefs);
    // Within the first minute: no walk.
    tokio::time::advance(Duration::from_secs(59)).await;
    assert_eq!(
        bg.tick().await.unwrap().consolidate,
        ConsolidateOutcome::NotChecked
    );
    assert_eq!(plan.calls(Op::ListRefs), walks);
    // Then one, which fails again: the next wait is two minutes.
    tokio::time::advance(Duration::from_secs(2)).await;
    assert!(matches!(
        bg.tick().await.unwrap().consolidate,
        ConsolidateOutcome::Failed(_)
    ));
    tokio::time::advance(Duration::from_secs(100)).await;
    assert_eq!(
        bg.tick().await.unwrap().consolidate,
        ConsolidateOutcome::NotChecked
    );
    // Fixed: the walk after the wait runs the consolidation.
    plan.clear();
    tokio::time::advance(Duration::from_secs(21)).await;
    consolidated(bg.tick().await.unwrap().consolidate);
    bg.finish_all().await.unwrap();
    assert_main_readable(Arc::new(FsBlobStore::new(dir.path())), &meta, "retried").await;
}

/// After each delta run the consolidation judges the packs again, but walks
/// only once the whole bytes written since its last walk reach an eighth of
/// the ratio rule's floor: otherwise each run would walk all the history
/// since the largest pack again, quadratic in what is written.
#[tokio::test]
async fn delta_runs_do_not_walk_again_until_enough_is_written() {
    let dir = tempfile::tempdir().unwrap();
    let store = fs_store(dir.path());
    let meta = Arc::new(MemoryMetaStore::new());
    let repo = Repo::new(Arc::new(store.clone()), meta.clone());
    import(&repo, &meta, 60).await;
    // The ratio rule's floor is all of the packs' bytes, never reached;
    // the age rule's is any gain, so every check judges the packs.
    let judging = AutoRepackOptions {
        consolidate: Some(ConsolidateOptions {
            ratio: 1,
            age: Some(Duration::from_secs(365 * 24 * 3600)),
            min_gain: 1,
            ..ConsolidateOptions::default()
        }),
        ..eager()
    };
    let mut bg = task(&store, &meta, opts(judging));
    ran(bg.tick().await.unwrap().auto);
    bg.finish_all().await.unwrap();
    let (mut walks, mut skipped, mut skipped_folds) = (0, 0, 0);
    for v in 60..68 {
        land_one(&repo, &meta, v).await;
        let tick = bg.tick().await.unwrap();
        let run = ran(tick.auto);
        bg.finish_all().await.unwrap();
        match tick.consolidate {
            ConsolidateOutcome::Below { walked: 0, .. } => {
                skipped += 1;
                // A fold moved judged frames into a new pack: no new bytes.
                skipped_folds += u32::from(run.packs_merged > 0);
            }
            ConsolidateOutcome::Below { .. } => walks += 1,
            other => panic!("{other:?}"),
        }
    }
    assert!(skipped > 0 && walks > 0, "{walks} walks, {skipped} skipped");
    assert!(
        skipped_folds > 0,
        "{walks} walks, {skipped} skipped, none after a fold"
    );
    assert_main_readable(Arc::new(FsBlobStore::new(dir.path())), &meta, "skips").await;
}

/// A consolidation held by cleanups past two fences lets one delta run go,
/// and that run re-arms the hold: the next delta runs wait for the
/// consolidation again, which runs once the cleanups' fences pass. Before,
/// the first hold's timer ran out for good, and a store with a delta run at
/// every check kept one of its own cleanups waiting and never consolidated.
#[tokio::test(start_paused = true)]
async fn a_held_consolidation_holds_delta_runs_again_after_one_goes() {
    let dir = tempfile::tempdir().unwrap();
    let (store, meta, repo) = heads_world(dir.path(), 2).await;
    let total: u64 = store.pack_sizes().unwrap().iter().map(|p| p.bytes).sum();
    let fence = Duration::from_secs(100);
    let o = BackgroundRepackOptions {
        fence,
        // Merges of every two whole packs, none of them small.
        trigger_packs: 1,
        fanout: 2,
        small_merge_bytes: 0,
        // A cap that a delta run's cleanup stays under, but not with the
        // consolidation's packs beside it.
        pending_share: 0.0,
        min_pending_bytes: total,
        ..opts(consolidating(u64::MAX))
    };
    let mut bg = task(&store, &meta, o);
    let step = Duration::from_secs(30);
    let mut v = 40;
    let land = |v: &mut usize| {
        *v += 1;
        let (repo, meta) = (repo.clone(), meta.clone());
        let n = *v;
        async move { land_one(&repo, &meta, n).await }
    };
    land(&mut v).await;
    let tick = bg.tick().await.unwrap();
    ran(tick.auto);
    assert_eq!(tick.consolidate, ConsolidateOutcome::Held("pending"));
    assert!(
        bg.pending_bytes() < total,
        "{} of {total}",
        bg.pending_bytes()
    );
    // Land packs keep coming and their merges keep a cleanup waiting:
    // delta runs wait, for two fences at most.
    let mut forced = false;
    for b in 0..10u32 {
        tokio::time::advance(step).await;
        for k in 0..2u32 {
            let items: Vec<(Hash, Bytes)> = (0..5u32)
                .map(|i| Bytes::from(format!("land batch {b}.{k} object {i}\n")))
                .map(|d| (Hash::of_blob(&d), d))
                .collect();
            store.put_many(items).await.unwrap();
        }
        let tick = bg.tick().await.unwrap();
        assert!(
            !matches!(tick.consolidate, ConsolidateOutcome::Ran { .. }),
            "{:?}",
            tick.consolidate
        );
        match tick.auto {
            AutoOutcome::Held("consolidation") => {}
            AutoOutcome::Ran(_) => {
                forced = true;
                break;
            }
            other => panic!("batch {b}: {other:?}"),
        }
    }
    assert!(forced, "a delta run went after two fences");
    // The land packs stop; loose objects keep coming at every check. The
    // hold is armed again, so delta runs wait and the consolidation runs
    // once the waiting cleanups are done.
    let mut consolidated = false;
    for _ in 0..8 {
        tokio::time::advance(step).await;
        land(&mut v).await;
        let tick = bg.tick().await.unwrap();
        if matches!(tick.consolidate, ConsolidateOutcome::Ran { .. }) {
            consolidated = true;
            break;
        }
        assert_eq!(tick.auto, AutoOutcome::Held("consolidation"));
    }
    assert!(consolidated, "the consolidation ran");
    bg.finish_all().await.unwrap();
    assert_main_readable(Arc::new(FsBlobStore::new(dir.path())), &meta, "re-armed").await;
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
/// whole import pack and loose objects included, once the bytes waiting
/// are a large enough share of the store (never on age alone), and the
/// next only after its interval, which the bucket keeps: a restarted task
/// (or another replica) waits for it too.
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
    let task = |auto: AutoRepackOptions| {
        let bg = BackgroundRepack::new(PackStore::Object(store.clone()), opts(auto));
        let walk = bg.walk_repo(meta.clone());
        bg.with_repo(walk)
    };
    // The default floor (64 MiB) is far above this store: no full repack,
    // even with every object past its age.
    let mut bg = task(AutoRepackOptions {
        max_age: Some(Duration::ZERO),
        ..eager()
    });
    assert!(matches!(
        bg.tick().await.unwrap().auto,
        AutoOutcome::BelowThreshold { .. }
    ));
    let auto = AutoRepackOptions {
        object_store_interval: Duration::from_secs(3600),
        object_store_min_bytes: 0,
        ..eager()
    };
    let mut bg = task(auto.clone());
    let r = ran(bg.tick().await.unwrap().auto);
    assert_eq!(r.packs_merged, 1, "{r:?}");
    assert!(r.loose_before > 0 && r.deltas > 3 * r.whole, "{r:?}");
    bg.finish_all().await.unwrap();
    assert_eq!(store.loose_stats().await.unwrap(), (0, 0));
    assert!(store.last_full_repack().await.unwrap().is_some());
    land_one(&repo, &meta, 30).await;
    assert_eq!(bg.tick().await.unwrap().auto, AutoOutcome::Held("interval"));
    // A restart (a new task, a new store handle) waits for the interval too.
    let fresh = ObjectBlobStore::local(dir.path()).unwrap();
    let bg2 = BackgroundRepack::new(PackStore::Object(fresh.clone()), opts(auto.clone()));
    let walk = bg2.walk_repo(meta.clone());
    let mut bg2 = bg2.with_repo(walk);
    assert_eq!(
        bg2.tick().await.unwrap().auto,
        AutoOutcome::Held("interval")
    );
    // Once it has passed (here: an interval of zero), it runs again.
    let mut bg = task(AutoRepackOptions {
        object_store_interval: Duration::ZERO,
        ..auto
    });
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
/// later: they are not counted as waiting, and a run meanwhile packs only
/// what came since, so no object is packed twice.
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
    // The next run packs only the new land's objects, not the copies the
    // first one packed and has not deleted yet.
    let r = ran(bg.tick().await.unwrap().auto);
    assert_eq!(r.loose_before, 6, "only the new land: {r:?}");
    bg.finish_all().await.unwrap();
    assert_eq!(store.loose_stats().unwrap().0, 0);
    let entries: u64 = store.pack_sizes().unwrap().iter().map(|p| p.objects).sum();
    assert_eq!(entries, r.packed_before + 6, "each object once");
    assert_main_readable(Arc::new(FsBlobStore::new(dir.path())), &meta, "twice").await;
}

/// A history whose contents came in several whole packs (an import in
/// batches). Returns the number of packs.
async fn import_in_packs(repo: &Repo, meta: &Arc<MemoryMetaStore>, versions: usize, per: usize) {
    for chunk in (0..versions).collect::<Vec<_>>().chunks(per) {
        let items: Vec<(Hash, Bytes)> = chunk
            .iter()
            .flat_map(|&v| FILES.iter().map(move |f| Bytes::from(version(f, v))))
            .map(|b| (Hash::of_blob(&b), b))
            .collect();
        repo.blobs().put_many(items).await.unwrap();
    }
    let mut tip = None;
    for v in 0..versions {
        let files: Vec<(&str, String)> = FILES.iter().map(|f| (*f, version(f, v))).collect();
        tip = Some(commit(repo, tip, &files).await);
    }
    let old = meta.get_ref("main").await.unwrap();
    assert!(meta.cas_ref("main", old, tip).await.unwrap());
}

/// More waiting than one run may take runs as several batches, oldest
/// packs first, one a tick: merges run in the ticks between, a restart
/// keeps the batches already published, and in the end nothing waits.
#[tokio::test]
async fn a_large_backlog_runs_in_batches_with_merges_between() {
    let dir = tempfile::tempdir().unwrap();
    let store = fs_store(dir.path());
    let meta = Arc::new(MemoryMetaStore::new());
    let repo = Repo::new(Arc::new(store.clone()), meta.clone());
    import_in_packs(&repo, &meta, 48, 6).await;
    assert_eq!(store.pack_sizes().unwrap().len(), 8);
    let o = BackgroundRepackOptions {
        trigger_packs: 1,
        fanout: 2,
        ..opts(AutoRepackOptions {
            max_batch_packs: 2,
            max_batch_objects: 60,
            ..eager()
        })
    };
    let task = |o: BackgroundRepackOptions| {
        let bg = BackgroundRepack::new(PackStore::Fs(store.clone()), o);
        let walk = bg.walk_repo(meta.clone());
        bg.with_repo(walk)
    };
    let mut bg = task(o.clone());
    let tick = bg.tick().await.unwrap();
    // The other whole packs merged with each other in the same tick.
    let mut merged_between = tick.merged.len();
    let first = ran(tick.auto);
    assert_eq!(first.packs_merged, 2, "the two oldest packs: {first:?}");
    assert!(
        first.loose_before <= 60 - 36,
        "loose objects within the cap: {first:?}"
    );
    bg.finish_all().await.unwrap();
    // A restart: the new task carries on, it does not start over.
    let mut bg = task(o);
    let mut runs = 1;
    for _ in 0..20 {
        let tick = bg.tick().await.unwrap();
        bg.finish_all().await.unwrap();
        match tick.auto {
            AutoOutcome::Ran(r) => {
                assert!(r.objects > 0, "{r:?}");
                runs += 1;
                merged_between += tick.merged.len();
            }
            AutoOutcome::Nothing => break,
            other => panic!("{other:?}"),
        }
    }
    assert!(runs >= 4, "{runs} batches");
    assert!(merged_between > 0, "merges ran between batches");
    assert_eq!(bg.tick().await.unwrap().auto, AutoOutcome::Nothing);
    assert_eq!(store.loose_stats().unwrap().0, 0);
    let ids: HashSet<String> = store
        .pack_sizes()
        .unwrap()
        .into_iter()
        .map(|p| p.id)
        .collect();
    let considered = store.considered_packs().unwrap();
    for d in store.pack_details(&ids).unwrap() {
        assert!(
            !d.whole() || considered.contains(&d.id),
            "{d:?} waits still"
        );
    }
    assert_main_readable(Arc::new(FsBlobStore::new(dir.path())), &meta, "batches").await;
}

/// With the cap on bytes waiting for cleanup hit (a long fence, a tiny
/// cap), merges of small land packs still run, so the pack count stays
/// bounded under land load; without that exemption it grows.
#[tokio::test]
async fn small_merges_keep_the_pack_count_bounded_past_the_cap() {
    let o = BackgroundRepackOptions {
        trigger_packs: 4,
        fence: Duration::from_secs(3600),
        pending_share: 0.0,
        min_pending_bytes: 1,
        auto: Some(AutoRepackOptions {
            objects: u64::MAX,
            bytes: u64::MAX,
            max_age: None,
            ..AutoRepackOptions::default()
        }),
        ..BackgroundRepackOptions::default()
    };
    let (most, pending) = land_load(o.clone()).await;
    assert!(pending > o.pending_cap(0), "the cap was hit");
    assert!(most <= o.trigger_packs + 8, "{most} packs searched at most");
    let (control, _) = land_load(BackgroundRepackOptions {
        small_merge_bytes: 0,
        ..o
    })
    .await;
    assert!(control > 3 * most, "without the exemption: {control}");
}

/// 40 rounds of three small land packs each, a tick after each round: the
/// most packs searched after a tick, and the bytes left waiting.
async fn land_load(o: BackgroundRepackOptions) -> (usize, u64) {
    let dir = tempfile::tempdir().unwrap();
    let store = fs_store(dir.path());
    let meta = Arc::new(MemoryMetaStore::new());
    let repo = Repo::new(Arc::new(store.clone()), meta.clone());
    import(&repo, &meta, 4).await;
    let bg = BackgroundRepack::new(PackStore::Fs(store.clone()), o);
    let walk = bg.walk_repo(meta.clone());
    let mut bg = bg.with_repo(walk);
    let mut most = 0;
    for round in 0..40 {
        for b in 0..3 {
            let items: Vec<(Hash, Bytes)> = (0..5)
                .map(|i| {
                    let d = Bytes::from(format!("round {round} batch {b} object {i}\n"));
                    (Hash::of_blob(&d), d)
                })
                .collect();
            store.put_many(items).await.unwrap();
        }
        let tick = bg.tick().await.unwrap();
        most = most.max(tick.packs);
    }
    (most, bg.pending_bytes())
}

/// A pack the delta repack wrote with no delta (nothing to delta against)
/// is recorded in `packs/considered`: a restarted task does not compress
/// it again, and the merge of such packs inherits the mark.
#[tokio::test]
async fn considered_packs_are_not_compressed_again_after_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let store = fs_store(dir.path());
    let meta = Arc::new(MemoryMetaStore::new());
    let repo = Repo::new(Arc::new(store.clone()), meta.clone());
    let task = |o: BackgroundRepackOptions| {
        let bg = BackgroundRepack::new(PackStore::Fs(store.clone()), o);
        let walk = bg.walk_repo(meta.clone());
        bg.with_repo(walk)
    };
    let unrelated = |n: usize| -> Vec<(Hash, Bytes)> {
        (0..10)
            .map(|i| Bytes::from(format!("batch {n} file {i}\n")))
            .map(|b| (Hash::of_blob(&b), b))
            .collect()
    };
    // A single-commit import of unrelated files: nothing to delta.
    store.put_many(unrelated(0)).await.unwrap();
    let files: Vec<(String, String)> = (0..10)
        .map(|i| (format!("f{i}.txt"), format!("batch 0 file {i}\n")))
        .collect();
    let refs: Vec<(&str, String)> = files.iter().map(|(p, b)| (p.as_str(), b.clone())).collect();
    let tip = commit(&repo, None, &refs).await;
    assert!(meta.cas_ref("main", None, Some(tip)).await.unwrap());
    let mut bg = task(opts(eager()));
    let r = ran(bg.tick().await.unwrap().auto);
    assert_eq!(r.deltas, 0, "{r:?}");
    bg.finish_all().await.unwrap();
    let first = r.pack_id.clone().unwrap();
    assert!(store.considered_packs().unwrap().contains(&first));
    // A restart: nothing waits.
    let mut bg = task(opts(eager()));
    assert_eq!(bg.tick().await.unwrap().auto, AutoOutcome::Nothing);

    // Another considered pack beside it (a repack marked it), then a
    // merge of the two: its output is considered too.
    store.put_many(unrelated(1)).await.unwrap();
    let whole: Vec<String> = store
        .pack_sizes()
        .unwrap()
        .into_iter()
        .map(|p| p.id)
        .filter(|id| *id != first)
        .collect();
    let marked = leep_store::RepackOptions {
        mode: leep_store::RepackMode::Planned { loose: false },
        packs: whole,
        mark: true,
        ..leep_store::RepackOptions::default()
    };
    store
        .repack_publish(Vec::new(), marked)
        .await
        .unwrap()
        .finish()
        .await
        .unwrap();
    assert_eq!(store.considered_packs().unwrap().len(), 2);
    let mut bg = task(BackgroundRepackOptions {
        trigger_packs: 1,
        fanout: 2,
        ..opts(eager())
    });
    let tick = bg.tick().await.unwrap();
    assert_eq!(tick.auto, AutoOutcome::Nothing);
    assert_eq!(tick.merged.len(), 1, "{tick:?}");
    bg.finish_all().await.unwrap();
    let merged = tick.merged[0].pack_id.clone().unwrap();
    assert!(store.considered_packs().unwrap().contains(&merged));
    let mut bg = task(opts(eager()));
    assert_eq!(bg.tick().await.unwrap().auto, AutoOutcome::Nothing);
}

/// A corrupt loose object is left in place by a run and never counted as
/// waiting again, so it does not trigger a run at every check.
#[tokio::test]
async fn corrupt_loose_objects_do_not_keep_triggering_runs() {
    let dir = tempfile::tempdir().unwrap();
    let store = fs_store(dir.path());
    let meta = Arc::new(MemoryMetaStore::new());
    let repo = Repo::new(Arc::new(store.clone()), meta.clone());
    import(&repo, &meta, 4).await;
    let bad = Hash::of_blob(b"what the file should hold");
    let hex = bad.to_hex();
    std::fs::create_dir_all(dir.path().join(&hex[..2])).unwrap();
    std::fs::write(
        dir.path().join(&hex[..2]).join(&hex[2..]),
        b"something else",
    )
    .unwrap();
    let auto = AutoRepackOptions {
        max_age: Some(Duration::ZERO),
        ..eager()
    };
    let bg = BackgroundRepack::new(PackStore::Fs(store.clone()), opts(auto));
    let walk = bg.walk_repo(meta.clone());
    let mut bg = bg.with_repo(walk);
    let r = ran(bg.tick().await.unwrap().auto);
    assert_eq!(r.corrupt, vec![bad], "{r:?}");
    bg.finish_all().await.unwrap();
    assert_eq!(store.loose_stats().unwrap().0, 1, "left for an operator");
    for _ in 0..3 {
        assert_eq!(bg.tick().await.unwrap().auto, AutoOutcome::Nothing);
    }
}

/// A loose object larger than the room a batch leaves is still packed
/// (the first object always fits), and the task then goes quiet instead of
/// running empty batches at every tick.
#[tokio::test]
async fn a_loose_object_larger_than_the_batch_is_packed() {
    let dir = tempfile::tempdir().unwrap();
    let store = fs_store(dir.path());
    let meta = Arc::new(MemoryMetaStore::new());
    let repo = Repo::new(Arc::new(store.clone()), meta.clone());
    let big = Bytes::from(version("big.txt", 2_000));
    repo.blobs().put(big.clone()).await.unwrap();
    let tip = commit(
        &repo,
        None,
        &[("big.txt", String::from_utf8(big.to_vec()).unwrap())],
    )
    .await;
    assert!(meta.cas_ref("main", None, Some(tip)).await.unwrap());
    let bg = BackgroundRepack::new(
        PackStore::Fs(store.clone()),
        opts(AutoRepackOptions {
            max_batch_bytes: 64,
            ..eager()
        }),
    );
    let walk = bg.walk_repo(meta.clone());
    let mut bg = bg.with_repo(walk);
    let mut packed = 0;
    for _ in 0..6 {
        match bg.tick().await.unwrap().auto {
            AutoOutcome::Ran(r) => {
                assert!(r.loose_before >= 1, "an empty batch: {r:?}");
                packed += r.loose_before;
            }
            AutoOutcome::Nothing => break,
            other => panic!("{other:?}"),
        }
        bg.finish_all().await.unwrap();
    }
    assert_eq!(packed, 2, "the content and its root node");
    assert_eq!(store.loose_stats().unwrap().0, 0);
    assert_eq!(bg.tick().await.unwrap().auto, AutoOutcome::Nothing);
}

/// Loose objects no ref reaches (abandoned uploads), more than one batch
/// takes, are packed whole over several batches, each packing some.
#[tokio::test]
async fn unreachable_loose_objects_over_the_cap_are_packed_in_batches() {
    let dir = tempfile::tempdir().unwrap();
    let store = fs_store(dir.path());
    let meta = Arc::new(MemoryMetaStore::new());
    let repo = Repo::new(Arc::new(store.clone()), meta.clone());
    import(&repo, &meta, 2).await;
    for i in 0..25 {
        repo.blobs()
            .put(Bytes::from(format!("abandoned upload {i}\n")))
            .await
            .unwrap();
    }
    let bg = BackgroundRepack::new(
        PackStore::Fs(store.clone()),
        opts(AutoRepackOptions {
            max_batch_objects: 10,
            ..eager()
        }),
    );
    let walk = bg.walk_repo(meta.clone());
    let mut bg = bg.with_repo(walk);
    let mut runs = 0;
    for _ in 0..20 {
        match bg.tick().await.unwrap().auto {
            AutoOutcome::Ran(r) => {
                assert!(r.objects >= 1 && r.loose_before <= 10, "{r:?}");
                runs += 1;
            }
            AutoOutcome::Nothing => break,
            other => panic!("{other:?}"),
        }
        bg.finish_all().await.unwrap();
    }
    assert!(runs >= 3, "{runs} batches");
    assert_eq!(
        store.loose_stats().unwrap().0,
        0,
        "every abandoned upload packed"
    );
}

/// Loose objects whose bytes start like a segment blob.
fn loose_segments(store: &FsBlobStore, root: &Path) -> Vec<Hash> {
    let prefixes = leep_derive::segment_blob_prefixes();
    store
        .loose_listing()
        .unwrap()
        .into_iter()
        .map(|(h, _)| h)
        .filter(|h| {
            let hex = h.to_hex();
            let b = std::fs::read(root.join(&hex[..2]).join(&hex[2..])).unwrap();
            prefixes.iter().any(|p| b.starts_with(p))
        })
        .collect()
}

/// Segment blobs (GC's mark segments here, and one shaped like a derived
/// segment) stay loose through delta runs (ADR 0108): no run packs them,
/// they never count as waiting, so they trigger no run (even with every
/// object past its age), and GC deletes their bytes at once. A file a
/// changeset names that starts the same way (stored before ADR 0041 refused
/// such uploads) is still packed and reads back byte for byte.
#[tokio::test]
async fn segment_blobs_stay_loose_and_never_trigger_runs() {
    let dir = tempfile::tempdir().unwrap();
    let store = fs_store(dir.path());
    let meta = Arc::new(MemoryMetaStore::new());
    let repo = Repo::new(Arc::new(store.clone()), meta.clone());
    import(&repo, &meta, 4).await;
    // A file on main that starts like a derived segment, written past the
    // upload check as an old store might hold it.
    let odd = format!(
        "leep:derived-segment-data-v1\0{}",
        "not a segment ".repeat(30)
    );
    store.put(Bytes::from(odd.clone())).await.unwrap();
    let head = meta.get_ref("main").await.unwrap();
    let tip = commit(&repo, head, &[("odd.bin", odd.clone())]).await;
    assert!(meta.cas_ref("main", head, Some(tip)).await.unwrap());
    let odd_hash = Hash::of_blob(odd.as_bytes());
    // GC writes its mark segments loose.
    gc::condemn(&repo, &gc_opts(), &Ahead).await.unwrap();
    gc::collect(&repo, &gc_opts(), &Ahead).await.unwrap();
    let derived = Bytes::from(format!(
        "leep:derived-segment-index-v1\0{}",
        "rows ".repeat(80)
    ));
    let derived_hash = store.put(derived).await.unwrap();
    let segs = loose_segments(&store, dir.path());
    assert!(segs.len() >= 3, "a mark segment too: {segs:?}");
    assert!(segs.contains(&derived_hash) && segs.contains(&odd_hash));

    let auto = AutoRepackOptions {
        max_age: Some(Duration::ZERO),
        ..eager()
    };
    let mut bg = task(&store, &meta, opts(auto));
    let r = ran(bg.tick().await.unwrap().auto);
    let expect: Vec<Hash> = segs.iter().copied().filter(|h| *h != odd_hash).collect();
    // Never read, so never reported: the task left them out of the batch.
    assert!(r.left_loose.is_empty(), "{r:?}");
    bg.finish_all().await.unwrap();
    assert!(!store.is_loose(&odd_hash), "the file main names is packed");
    let fresh = FsBlobStore::new(dir.path());
    assert_eq!(
        fresh.get(&odd_hash).await.unwrap().unwrap(),
        Bytes::from(odd.clone())
    );
    let (loose, _) = store.loose_stats().unwrap();
    assert_eq!(loose as usize, expect.len());
    assert_eq!(loose_segments(&store, dir.path()), expect);
    for _ in 0..3 {
        assert_eq!(bg.tick().await.unwrap().auto, AutoOutcome::Nothing);
    }
    // A new segment does not trigger a run either, not even once.
    let more = Bytes::from(format!("leep:gc-mark-segment-v1\0{}", "m".repeat(64)));
    let more_hash = store.put(more).await.unwrap();
    assert_eq!(bg.tick().await.unwrap().auto, AutoOutcome::Nothing);
    // A full repack given the server's prefixes (as `leep-server repack`
    // is) leaves them loose as well.
    let order = leep_derive::pack_order::pack_order(&repo).await.unwrap();
    let full = leep_store::RepackOptions {
        leave_loose: leep_store::LeaveLoose::new(leep_derive::segment_blob_prefixes()),
        ..leep_store::RepackOptions::default()
    };
    let p = store.repack_publish(order.objects, full).await.unwrap();
    let r = p.finish().await.unwrap();
    let mut all = expect.clone();
    all.push(more_hash);
    all.sort();
    assert_eq!(r.left_loose, all, "{r:?}");
    assert!(store.is_loose(&more_hash));
    // GC's delete unlinks them: their bytes leave the store at once.
    let before = store_bytes(dir.path());
    let mut sizes = 0;
    for h in expect.iter().chain([&more_hash]) {
        sizes += store.get(h).await.unwrap().unwrap().len() as u64;
        store.delete(h).await.unwrap();
    }
    assert_eq!(store_bytes(dir.path()), before - sizes);
    assert_eq!(store.loose_stats().unwrap(), (0, 0));
    assert_main_readable(Arc::new(FsBlobStore::new(dir.path())), &meta, "segments").await;
}

/// An object store's full repack, run by the task, leaves segment blobs
/// loose (ADR 0108), and they do not count toward the bytes waiting, so
/// they never bring the next full repack nearer.
#[tokio::test]
async fn an_object_stores_full_repack_leaves_segment_blobs_loose() {
    let dir = tempfile::tempdir().unwrap();
    let store = ObjectBlobStore::local(dir.path())
        .unwrap()
        .with_pack_writes(PackWrites {
            min_objects: 4,
            ..PackWrites::default()
        });
    let meta = Arc::new(MemoryMetaStore::new());
    let repo = Repo::new(Arc::new(store.clone()), meta.clone());
    import(&repo, &meta, 10).await;
    let mut segs = Vec::new();
    for (i, kind) in ["gc-mark-segment-v1", "derived-segment-data-v1"]
        .iter()
        .enumerate()
    {
        let b = Bytes::from(format!("leep:{kind}\0{}", format!("{i} ").repeat(100)));
        segs.push(store.put(b).await.unwrap());
    }
    segs.sort();
    let auto = AutoRepackOptions {
        object_store_interval: Duration::ZERO,
        object_store_min_bytes: 0,
        ..eager()
    };
    let bg = BackgroundRepack::new(PackStore::Object(store.clone()), opts(auto));
    let walk = bg.walk_repo(meta.clone());
    let mut bg = bg.with_repo(walk);
    let r = ran(bg.tick().await.unwrap().auto);
    assert_eq!(r.left_loose, segs, "{r:?}");
    bg.finish_all().await.unwrap();
    assert_eq!(store.loose_stats().await.unwrap().0, 2);
    // Only segments are loose: nothing waits, with no threshold at all.
    for _ in 0..2 {
        assert_eq!(bg.tick().await.unwrap().auto, AutoOutcome::Nothing);
    }
    for h in &segs {
        store.delete(h).await.unwrap();
    }
    assert_eq!(store.loose_stats().await.unwrap(), (0, 0));
    let fresh = ObjectBlobStore::local(dir.path()).unwrap();
    assert_main_readable(Arc::new(fresh.clone()), &meta, "object store").await;
    check_blob_store_contract(&fresh).await;
}

/// Bytes GC deleted from delta packs that stay under the ratio rule are
/// still dropped once they pass the age rule's floor and have waited the
/// age (ADR 0107 section 4): a store's deleted history and dead segments
/// leave the packs in bounded time, not only at `repack --full`. Without
/// the age rule they would wait.
#[tokio::test]
async fn deleted_bytes_under_the_ratio_are_dropped_after_the_age() {
    for age in [None, Some(Duration::from_secs(2))] {
        let dir = tempfile::tempdir().unwrap();
        let (store, meta, repo) = heads_world(dir.path(), 0).await;
        // Uploads no ref reaches, hard to compress, packed by a delta run.
        let junk: Vec<Bytes> = (0..40u32)
            .map(|i| {
                let s: String = (0..40u32)
                    .map(|j| Hash::of_blob(&(i * 1000 + j).to_be_bytes()).to_hex())
                    .collect();
                Bytes::from(s)
            })
            .collect();
        let hashes = repo.put_blobs(junk).await.unwrap();
        let aged = AutoRepackOptions {
            consolidate: Some(ConsolidateOptions {
                // The ratio rule would need the whole packs back.
                ratio: 1,
                age,
                min_gain: 1,
                ..ConsolidateOptions::default()
            }),
            ..eager()
        };
        let mut bg = task(&store, &meta, opts(aged));
        ran(bg.tick().await.unwrap().auto);
        bg.finish_all().await.unwrap();
        for h in &hashes {
            store.delete(h).await.unwrap();
        }
        let tick = bg.tick().await.unwrap();
        let packs: u64 = store.pack_sizes().unwrap().iter().map(|p| p.bytes).sum();
        match (age, tick.consolidate) {
            (Some(_), ConsolidateOutcome::Below { gain, .. }) => {
                assert!(gain >= packs / 64 && gain < packs, "{gain} of {packs}")
            }
            (None, ConsolidateOutcome::Nothing | ConsolidateOutcome::Below { .. }) => {}
            (_, other) => panic!("{age:?}: {other:?}"),
        }
        tokio::time::sleep(Duration::from_millis(2_500)).await;
        let tick = bg.tick().await.unwrap();
        match age {
            None => assert!(
                matches!(
                    tick.consolidate,
                    ConsolidateOutcome::Nothing | ConsolidateOutcome::Below { .. }
                ),
                "{:?}",
                tick.consolidate
            ),
            Some(_) => {
                let (r, _, _) = consolidated(tick.consolidate);
                assert_eq!(r.dropped_tombstoned, 40, "{r:?}");
                bg.finish_all().await.unwrap();
                let fresh = FsBlobStore::new(dir.path());
                for h in &hashes {
                    assert!(!fresh.contains(h).await.unwrap());
                }
                let ids = store.pack_sizes().unwrap().into_iter().map(|p| p.id);
                assert_eq!(store.tombstoned_bytes(&ids.collect()).unwrap(), 0);
                assert_main_readable(Arc::new(fresh), &meta, "dropped").await;
            }
        }
    }
}

/// A check reads the first bytes of at most `sniff_per_check` loose
/// objects; those not read yet count as waiting until a later check reads
/// them, and one that cannot be read (here a directory where a loose file
/// should be) is tried again at each check without keeping the others'
/// answers from being kept.
#[tokio::test]
async fn segment_sniffs_are_capped_per_check_and_survive_a_failed_read() {
    let dir = tempfile::tempdir().unwrap();
    let store = fs_store(dir.path());
    let meta = Arc::new(MemoryMetaStore::new());
    for i in 0..7 {
        let b = format!("leep:gc-mark-segment-v1\0{}", format!("{i} ").repeat(40));
        store.put(Bytes::from(b)).await.unwrap();
    }
    let bad = Hash::of_blob(b"unreadable");
    let hex = bad.to_hex();
    std::fs::create_dir_all(dir.path().join(&hex[..2]).join(&hex[2..])).unwrap();
    let auto = AutoRepackOptions {
        objects: u64::MAX,
        bytes: u64::MAX,
        max_age: None,
        consolidate: None,
        sniff_per_check: 2,
        ..eager()
    };
    let mut bg = task(&store, &meta, opts(auto));
    let mut waiting = Vec::new();
    for _ in 0..8 {
        match bg.tick().await.unwrap().auto {
            AutoOutcome::BelowThreshold { objects, .. } => waiting.push(objects),
            other => panic!("{other:?}"),
        }
    }
    // Eight loose objects; each check reads two at most (the unreadable one
    // among them until it sorts last), so the count falls by two at most.
    assert!(waiting[0] >= 6, "{waiting:?}");
    for w in waiting.windows(2) {
        assert!(w[0] >= w[1] && w[0] - w[1] <= 2, "{waiting:?}");
    }
    assert_eq!(
        *waiting.last().unwrap(),
        1,
        "only the unreadable one: {waiting:?}"
    );
}
