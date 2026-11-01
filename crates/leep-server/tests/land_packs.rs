//! Land batches written as packs (ADR 0068), on a real filesystem store:
//! a pack write that fails or is killed at any step never leaves a stored
//! changeset naming a missing object; garbage collection deletes and
//! rescues objects in land packs as it does loose ones; and the background
//! repack merges land packs while lands go on, with reads staying correct.

use std::collections::{BTreeMap, HashSet};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use leep_core::api::LandResponse;
use leep_core::memory::MemoryMetaStore;
use leep_core::{
    BlobStore, Changeset, ChangesetId, Error, FileChange, FileKind, Hash, MetaStore, RepoPath,
    Result, TreeEntry,
};
use leep_derive::gc::{self, Clock, GcOptions};
use leep_derive::Repo;
use leep_server::background_repack::{BackgroundRepack, BackgroundRepackOptions, PackStore};
use leep_store::{FsBlobStore, PackWriteStep, PackWrites, RepackMode, RepackOptions};

fn rp(s: &str) -> RepoPath {
    RepoPath::new(s).unwrap()
}

/// Upload `files` and commit them on `parent`, retrying errors (the faults
/// injected), as a client would.
async fn commit(
    repo: &Repo,
    parent: Option<ChangesetId>,
    files: &[(String, String)],
) -> ChangesetId {
    for _ in 0..200 {
        if let Ok(id) = try_commit(repo, parent, files).await {
            return id;
        }
    }
    panic!("commit of {files:?} kept failing");
}

async fn try_commit(
    repo: &Repo,
    parent: Option<ChangesetId>,
    files: &[(String, String)],
) -> Result<ChangesetId> {
    let mut changes = BTreeMap::new();
    for (path, body) in files {
        let content = repo.put_blob(Bytes::from(body.clone())).await?;
        changes.insert(
            rp(path),
            FileChange::Modify {
                content,
                kind: FileKind::Regular,
                size: body.len() as u64,
            },
        );
    }
    repo.create_changeset(Changeset {
        parents: parent.into_iter().collect(),
        author: "agent".into(),
        timestamp: 1,
        message: format!("{files:?}"),
        changes,
        extra: BTreeMap::new(),
    })
    .await
}

/// The files agent `a` changes in round `r`: one in its own directory, one
/// in a directory every agent writes (so each land rewrites a wide node),
/// and one in a directory of its group.
fn files(a: usize, r: usize) -> Vec<(String, String)> {
    vec![
        (
            format!("agents/a{a}/r{r}.txt"),
            format!("agent {a} round {r}\n"),
        ),
        (
            format!("teams/t{a}.txt"),
            format!("team file of {a}, round {r}\n"),
        ),
        (
            format!("groups/g{}/a{a}.txt", a % 4),
            format!("group {} agent {a} round {r}\n", a % 4),
        ),
    ]
}

/// Land `cs` on `main`, retrying errors (the faults injected) and
/// returning what landed.
async fn land_retrying(repo: &Repo, cs: ChangesetId) -> ChangesetId {
    for _ in 0..200 {
        match repo.land(cs, "main").await {
            Ok(LandResponse::Landed { landed, .. }) => return landed,
            Ok(other) => panic!("land of {cs}: {other:?}"),
            Err(_) => tokio::task::yield_now().await,
        }
    }
    panic!("land of {cs} kept failing");
}

/// Every changeset `main` reaches, with every tree node and content it
/// names, read through a new store on the same directory (another process,
/// or this one after a restart).
async fn assert_main_readable(root: &Path, meta: &Arc<MemoryMetaStore>, what: &str) -> usize {
    let blobs = Arc::new(FsBlobStore::new(root));
    let repo = Repo::new(blobs.clone(), meta.clone());
    let Some(head) = meta.get_ref("main").await.unwrap() else {
        return 0;
    };
    let mut seen = HashSet::new();
    let mut stack = vec![head];
    let mut objects = 0;
    while let Some(id) = stack.pop() {
        if !seen.insert(id) {
            continue;
        }
        let cs = repo.get_changeset(id).await.unwrap();
        stack.extend(cs.parents.iter().copied());
        let mut trees = vec![repo.root_tree(id).await.unwrap()];
        while let Some(t) = trees.pop() {
            let node = repo
                .read_tree(t)
                .await
                .unwrap_or_else(|e| panic!("{what}: tree {t} of {id}: {e}"));
            objects += 1;
            for entry in node.entries.values() {
                match entry {
                    TreeEntry::Dir { tree } => trees.push(*tree),
                    TreeEntry::File { content, size, .. } => {
                        let b =
                            blobs.get(content).await.unwrap().unwrap_or_else(|| {
                                panic!("{what}: content {content} of {id} missing")
                            });
                        assert_eq!(b.len() as u64, *size, "{what}");
                        assert_eq!(Hash::of_blob(&b), *content, "{what}");
                        objects += 1;
                    }
                }
            }
        }
    }
    objects
}

fn packs_in(root: &Path) -> usize {
    match std::fs::read_dir(root.join("packs")) {
        Ok(rd) => rd
            .filter(|e| {
                e.as_ref()
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .ends_with(".idx")
            })
            .count(),
        Err(_) => 0,
    }
}

/// A seeded coin for the pack write hook.
struct Coin(AtomicU64);

impl Coin {
    fn flip(&self, one_in: u64) -> bool {
        let mut x = self.0.load(Ordering::Relaxed);
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0.store(x, Ordering::Relaxed);
        x.is_multiple_of(one_in)
    }
}

fn seeds() -> u64 {
    std::env::var("LEEP_FAULT_SEEDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(12)
}

/// A pack write that fails at any step (as a kill there would leave it: a
/// temp file, a pack without an index, a published pack whose writer never
/// returned) leaves no stored changeset naming a missing object, and a
/// repack afterwards cleans up what it left.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pack_writes_failing_at_any_step_leave_no_changeset_naming_a_missing_object() {
    let hits: Arc<Mutex<HashSet<String>>> = Arc::default();
    for seed in 1..=seeds() {
        let dir = tempfile::tempdir().unwrap();
        let coin = Arc::new(Coin(AtomicU64::new(
            seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1,
        )));
        let failing = Arc::new(AtomicBool::new(true));
        let hook = {
            let (coin, failing, hits) = (coin.clone(), failing.clone(), hits.clone());
            Arc::new(move |step: PackWriteStep| {
                if failing.load(Ordering::Relaxed) && coin.flip(4) {
                    hits.lock().unwrap().insert(format!("{step:?}"));
                    return Err(Error::Storage(format!("injected: killed at {step:?}")));
                }
                Ok(())
            })
        };
        let blobs = FsBlobStore::new(dir.path()).with_pack_writes(PackWrites {
            min_objects: 4,
            hook: Some(hook),
            ..PackWrites::default()
        });
        let meta = Arc::new(MemoryMetaStore::new());
        let repo = Repo::new(Arc::new(blobs.clone()), meta.clone());
        let base = commit(&repo, None, &[("README".into(), "base\n".into())]).await;
        assert!(meta.cas_ref("main", None, Some(base)).await.unwrap());
        for round in 0..3 {
            let head = meta.get_ref("main").await.unwrap();
            let mut stacks = Vec::new();
            for a in 0..12 {
                stacks.push(commit(&repo, head, &files(a, round)).await);
            }
            let lands: Vec<_> = stacks
                .into_iter()
                .map(|cs| {
                    let repo = repo.clone();
                    tokio::spawn(async move { land_retrying(&repo, cs).await })
                })
                .collect();
            for l in lands {
                l.await.unwrap();
            }
            let what = format!("seed {seed} round {round}");
            assert_main_readable(dir.path(), &meta, &what).await;
        }
        // Without faults, the next round's batches are packs.
        failing.store(false, Ordering::Relaxed);
        let before = packs_in(dir.path());
        let head = meta.get_ref("main").await.unwrap();
        let mut stacks = Vec::new();
        for a in 0..12 {
            stacks.push(commit(&repo, head, &files(a, 9)).await);
        }
        let lands: Vec<_> = stacks
            .into_iter()
            .map(|cs| {
                let repo = repo.clone();
                tokio::spawn(async move { land_retrying(&repo, cs).await })
            })
            .collect();
        for l in lands {
            l.await.unwrap();
        }
        assert!(
            packs_in(dir.path()) > before,
            "seed {seed}: land batches wrote packs"
        );
        assert_main_readable(dir.path(), &meta, &format!("seed {seed}")).await;

        // What the failed writes left (temp files, packs without an index)
        // a repack deletes; every object stays readable.
        let published = blobs
            .repack_publish(
                Vec::new(),
                RepackOptions {
                    mode: RepackMode::Incremental {
                        factor: 2,
                        max_packs: 4,
                    },
                    threads: 1,
                    ..RepackOptions::default()
                },
            )
            .await
            .unwrap();
        published.finish().await.unwrap();
        let junk: Vec<String> = std::fs::read_dir(dir.path().join("packs"))
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|n| {
                n.starts_with(".tmp-")
                    || n.strip_suffix(".pack").is_some_and(|id| {
                        !dir.path().join("packs").join(format!("{id}.idx")).exists()
                    })
            })
            .collect();
        assert!(junk.is_empty(), "seed {seed}: {junk:?}");
        assert_main_readable(dir.path(), &meta, &format!("seed {seed}, repacked")).await;
    }
    let hits = hits.lock().unwrap();
    for step in ["PackSynced", "PackRenamed", "IndexSynced", "Published"] {
        assert!(hits.contains(step), "a write failed at {step}: {hits:?}");
    }
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

/// Objects in land packs are condemned and collected like loose ones
/// (tombstoned in their pack), referenced ones survive, and an upload of a
/// condemned object (into a new pack, or loose) rescues it.
#[tokio::test]
async fn gc_collects_and_rescues_objects_in_land_packs() {
    let dir = tempfile::tempdir().unwrap();
    let blobs = FsBlobStore::new(dir.path()).with_pack_writes(PackWrites {
        min_objects: 4,
        ..PackWrites::default()
    });
    let meta = Arc::new(MemoryMetaStore::new());
    let repo = Repo::new(Arc::new(blobs.clone()), meta.clone());
    let base = commit(&repo, None, &[("README".into(), "base\n".into())]).await;
    assert!(meta.cas_ref("main", None, Some(base)).await.unwrap());
    let stacks: Vec<_> = futures_util::future::join_all((0..8).map(|a| {
        let repo = repo.clone();
        async move { commit(&repo, Some(base), &files(a, 0)).await }
    }))
    .await;
    let lands: Vec<_> = stacks
        .into_iter()
        .map(|cs| {
            let repo = repo.clone();
            tokio::spawn(async move { land_retrying(&repo, cs).await })
        })
        .collect();
    for l in lands {
        l.await.unwrap();
    }
    // Unreferenced objects written as one pack, and more to rescue.
    let doomed = repo.put_blobs(orphans("doomed")).await.unwrap();
    let saved = repo.put_blobs(orphans("saved")).await.unwrap();
    let packed = packs_in(dir.path());
    assert!(packed >= 2, "the orphans went into packs");
    for h in doomed.iter().chain(&saved) {
        assert!(!blobs.is_loose(h), "{h} is packed");
    }

    let condemned = gc::condemn(&repo, &gc_opts(), &Ahead).await.unwrap();
    assert!(condemned.condemned >= 40, "{condemned:?}");
    // Uploaded again after the condemnation: once more as a pack, and one
    // alone (loose).
    repo.put_blobs(orphans("saved")).await.unwrap();
    let lone = orphans("doomed").remove(0);
    repo.put_blob(lone.clone()).await.unwrap();
    let collected = gc::collect(&repo, &gc_opts(), &Ahead).await.unwrap();
    assert!(collected.deleted >= 19, "{collected:?}");

    let fresh = FsBlobStore::new(dir.path());
    for h in &doomed[1..] {
        assert!(!fresh.contains(h).await.unwrap(), "{h} collected");
        assert!(fresh.get(h).await.unwrap().is_none());
    }
    assert!(fresh.contains(&doomed[0]).await.unwrap(), "rescued loose");
    for h in &saved {
        assert!(fresh.contains(h).await.unwrap(), "{h} rescued by its pack");
    }
    assert_main_readable(dir.path(), &meta, "after gc").await;
    // The tombstoned copies are dropped by a full repack; the rest stays.
    let published = blobs
        .repack_publish(Vec::new(), RepackOptions::default())
        .await
        .unwrap();
    let report = published.finish().await.unwrap();
    assert!(report.dropped_tombstoned >= 19, "{report:?}");
    assert_main_readable(dir.path(), &meta, "after a full repack").await;
    let fresh = FsBlobStore::new(dir.path());
    for h in &saved {
        assert!(fresh.contains(h).await.unwrap());
    }
}

/// The background repack merges land packs into few while lands go on,
/// and every read stays correct, in this process and in a new one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_background_repack_merges_land_packs_while_lands_continue() {
    let dir = tempfile::tempdir().unwrap();
    let blobs = FsBlobStore::new(dir.path()).with_pack_writes(PackWrites {
        min_objects: 4,
        ..PackWrites::default()
    });
    let meta = Arc::new(MemoryMetaStore::new());
    let repo = Repo::new(Arc::new(blobs.clone()), meta.clone());
    let base = commit(&repo, None, &[("README".into(), "base\n".into())]).await;
    assert!(meta.cas_ref("main", None, Some(base)).await.unwrap());
    let mut bg = BackgroundRepack::new(
        PackStore::Fs(blobs.clone()),
        BackgroundRepackOptions {
            trigger_packs: 4,
            fence: Duration::ZERO,
            ..BackgroundRepackOptions::default()
        },
    );
    let stop = Arc::new(AtomicBool::new(false));
    let landed = Arc::new(AtomicU64::new(0));
    let lander = {
        let (repo, stop, landed) = (repo.clone(), stop.clone(), landed.clone());
        tokio::spawn(async move {
            let mut round = 0;
            while !stop.load(Ordering::Relaxed) || round < 6 {
                let head = repo.meta().get_ref("main").await.unwrap();
                let mut stacks = Vec::new();
                for a in 0..10 {
                    stacks.push(commit(&repo, head, &files(a, round)).await);
                }
                let lands: Vec<_> = stacks
                    .into_iter()
                    .map(|cs| {
                        let repo = repo.clone();
                        tokio::spawn(async move { land_retrying(&repo, cs).await })
                    })
                    .collect();
                for l in lands {
                    l.await.unwrap();
                    landed.fetch_add(1, Ordering::Relaxed);
                }
                round += 1;
            }
        })
    };
    let mut merges = Vec::new();
    for _ in 0..200 {
        let tick = bg.tick().await.unwrap();
        assert!(
            tick.packs <= 12,
            "{} packs searched after a tick",
            tick.packs
        );
        merges.extend(tick.merged);
        // Reads through the store the lands and merges share.
        assert_main_readable_through(&repo, &meta).await;
        if merges.len() >= 3 {
            stop.store(true, Ordering::Relaxed);
        }
        if lander.is_finished() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    stop.store(true, Ordering::Relaxed);
    lander.await.unwrap();
    bg.tick().await.unwrap();
    bg.finish_all().await.unwrap();
    assert!(!merges.is_empty(), "the trigger fired");
    let merged: u64 = merges.iter().map(|r| r.packs_merged).sum();
    assert!(merged >= 4, "{merges:?}");
    // Few packs remain searched, fewer than were written.
    let now = blobs.pack_count().unwrap();
    // At most `trigger_packs`, or three per size class in use.
    assert!(now <= 12, "{now} packs after merging {merged}");
    assert_eq!(bg.pending(), 0);
    assert!(landed.load(Ordering::Relaxed) >= 60);
    assert_main_readable(dir.path(), &meta, "after merging").await;
}

/// [`assert_main_readable`] through `repo`'s own store: what this process
/// reads while its own repack supersedes packs.
async fn assert_main_readable_through(repo: &Repo, meta: &Arc<MemoryMetaStore>) {
    let Some(head) = meta.get_ref("main").await.unwrap() else {
        return;
    };
    let mut trees = vec![repo.root_tree(head).await.unwrap()];
    while let Some(t) = trees.pop() {
        let node = repo.read_tree(t).await.unwrap();
        for entry in node.entries.values() {
            match entry {
                TreeEntry::Dir { tree } => trees.push(*tree),
                TreeEntry::File { content, .. } => {
                    assert!(repo.blobs().get(content).await.unwrap().is_some());
                }
            }
        }
    }
}
