//! Operation budgets (ADR 0081): the storage work of each hot flow, counted
//! in process over memory stores and checked against [`BUDGETS`] both ways.
//!
//! Wall time on a shared CI runner says little; these counts say the same
//! on every machine. A create that makes two metadata calls at once, or a
//! land batch that asks its four-connection lane for six, fails here before
//! any benchmark runs. Every flow runs on a new `Repo` over stores already
//! filled (a new process, nothing cached), the case where a server makes
//! the most calls.
//!
//! When a line fails, the message says which way. Over budget: find the
//! call that grew in the usage printed beside it; raise the line only if
//! the growth is intended, and say why in the commit. Under budget by more
//! than 20%: lower the line to what was measured, so the gain is kept.

use std::collections::BTreeMap;
use std::sync::Arc;

use bytes::Bytes;
use leep_core::api::LandResponse;
use leep_core::lane::Lane;
use leep_core::memory::{MemoryBlobStore, MemoryMetaStore};
use leep_core::{BlobStore, Changeset, ChangesetId, FileChange, FileKind, MetaStore, RepoPath};
use leep_testkit::budget::{actor, check, Budget, Limit, Measured, Usage};

use crate::land_batch_tests::{build, long_history, make, queue_in_order, release, Gate};
use crate::search::SearchQuery;
use crate::Repo;

const TABLE_AT: &str = "BUDGETS in crates/leep-derive/src/budget_tests.rs";

/// The most each flow may ask of its stores. The flows run on one thread
/// (`#[tokio::test]`), so every count is the same on every run and every
/// machine, and the lines are exact. `meta in flight` is the connections a
/// pooled backend lends the flow at once.
#[rustfmt::skip]
const BUDGETS: &[Limit] = &[
    // One changeset of one file on a 300-deep history: the parents, one
    // read of every derived input, the skiplist built one level at a time
    // (nine single reads on a new process), one write. One metadata call
    // at a time (ADR 0067; ADR 0079 section 3).
    Limit::fixed("create", "meta calls", 13),
    Limit::fixed("create", "meta in flight", 1),
    Limit::fixed("create", "blob calls", 6),
    // Eight creates at once, each as its own actor: still one call at a
    // time each, so the shared pool sees one ask per create.
    Limit::fixed("create x8", "meta in flight per create", 1),
    // A land batch of k changesets made on the head of a 1,024-deep
    // history, behind a no-op land in a batch of its own (ADR 0032, 0072,
    // 0079): one changeset read per land, the rest per batch. The land
    // lane's reads may overlap up to the land pool's default size of 4
    // (ADR 0067); ADR 0079's first version asked for 5 or 6.
    Limit::linear("land batch", "meta calls", 37, 1),
    Limit::fixed("land batch", "blob calls", 6),
    Limit::fixed("land batch", "land lane in flight", 4),
    // The first 100 entries of a log without a path (ADR 0030): changesets
    // and their summary rows a page at a time.
    Limit::fixed("log 100", "meta calls", 26),
    Limit::fixed("log 100", "meta in flight", 4),
    // A cold blame of a file with v versions on a 2,000-deep linear
    // history: about three reads per version, none for the depth (ADR
    // 0065, ADR 0077).
    Limit::linear("blame", "meta calls", 9, 3),
    Limit::fixed("blame", "meta in flight", 1),
    // A literal search of a 256-file tree on a new process, deriving the
    // trigram index on the way (ADR 0014): every blob read once.
    Limit::fixed("search", "meta calls", 30),
    Limit::fixed("search", "blob calls", 297),
    // One blob: the condemnation read beside the claim (ADR 0008, 0012). Then
    // 16 in one request: one pack, one flush, two metadata calls.
    Limit::fixed("upload", "meta calls", 2),
    Limit::fixed("upload", "meta in flight", 2),
    Limit::fixed("upload x16", "meta calls", 2),
    Limit::fixed("upload x16", "blob calls", 2),
];

fn budget_check(flow: &str, measured: &Measured, usage: &dyn std::fmt::Display) {
    check(BUDGETS, TABLE_AT, flow, measured, usage);
}

/// A repository over `blobs` and `meta` whose calls `Budget` counts.
fn counted(blobs: Arc<dyn BlobStore>, meta: Arc<dyn MetaStore>) -> (Repo, Arc<Budget>) {
    let b = Budget::new(blobs, meta);
    (Repo::new(b.clone(), b.clone()), b)
}

fn plain() -> (Repo, Arc<dyn BlobStore>, Arc<dyn MetaStore>) {
    let blobs: Arc<dyn BlobStore> = Arc::new(MemoryBlobStore::new());
    let meta: Arc<dyn MetaStore> = Arc::new(MemoryMetaStore::new());
    (Repo::new(blobs.clone(), meta.clone()), blobs, meta)
}

fn one_file(path: &str, content: leep_core::Hash, size: usize) -> BTreeMap<RepoPath, FileChange> {
    let mut changes = BTreeMap::new();
    changes.insert(
        RepoPath::new(path).unwrap(),
        FileChange::Modify {
            content,
            kind: FileKind::Regular,
            size: size as u64,
        },
    );
    changes
}

fn changeset(parent: ChangesetId, changes: BTreeMap<RepoPath, FileChange>) -> Changeset {
    Changeset {
        parents: vec![parent],
        author: "t".into(),
        timestamp: 1_700_000_000,
        message: "x".into(),
        changes,
        extra: BTreeMap::new(),
    }
}

#[tokio::test]
async fn create_budget() {
    let (r, blobs, meta) = plain();
    build(&r, &[]).await;
    let head = long_history(&r, 300, 0).await;
    let body = Bytes::from_static(b"1\n");
    let content = r.put_blob(body.clone()).await.unwrap();
    let (fresh, b) = counted(blobs, meta);
    b.start();
    fresh
        .create_changeset(changeset(head, one_file("x/1", content, body.len())))
        .await
        .unwrap();
    let u = b.stop();
    budget_check(
        "create",
        &[
            ("meta calls", 0, u.meta_calls()),
            ("meta in flight", 0, u.meta_in_flight as u64),
            ("blob calls", 0, u.blob_calls()),
        ],
        &u,
    );
}

/// Creates side by side each hold one connection at a time: with a
/// thousand agents creating, the shared pool sees a thousand asks, not two
/// thousand (the regression of ADR 0079's first version).
#[tokio::test]
async fn concurrent_creates_budget() {
    const ACTORS: [&str; 8] = ["c0", "c1", "c2", "c3", "c4", "c5", "c6", "c7"];
    let (r, blobs, meta) = plain();
    build(&r, &[]).await;
    let head = long_history(&r, 300, 0).await;
    let mut specs = Vec::new();
    for (i, name) in ACTORS.iter().enumerate() {
        let body = Bytes::from(format!("{i}\n"));
        let content = r.put_blob(body.clone()).await.unwrap();
        specs.push((
            *name,
            changeset(head, one_file(&format!("x/{i}"), content, body.len())),
        ));
    }
    let (fresh, b) = counted(blobs, meta);
    b.start();
    let creates = specs.into_iter().map(|(name, cs)| {
        let fresh = fresh.clone();
        tokio::spawn(actor(name, async move { fresh.create_changeset(cs).await }))
    });
    for t in creates.collect::<Vec<_>>() {
        t.await.unwrap().unwrap();
    }
    let u = b.stop();
    let per_create = ACTORS.iter().map(|a| u.actor_in_flight(a)).max().unwrap();
    assert_eq!(
        u.actor_in_flight(""),
        0,
        "a create made metadata calls on a task of its own, which the \
         per-create count cannot see; give that task the caller's actor:\n{u}"
    );
    budget_check(
        "create x8",
        &[("meta in flight per create", 0, per_create as u64)],
        &u,
    );
}

/// One land batch of `k` changesets made on the head of a deep history,
/// on a new process: everything the two batches (the no-op blocker's and
/// theirs) asked of the stores.
async fn land_batch(k: usize) -> Usage {
    let gate = Gate::new();
    let blobs: Arc<dyn BlobStore> = Arc::new(MemoryBlobStore::new());
    let r = Repo::new(blobs.clone(), gate.clone());
    build(&r, &[]).await;
    let head = long_history(&r, 1024, 0).await;
    let mut lands = Vec::new();
    for i in 0..k {
        let file = format!("n/{i}");
        lands.push((
            make(&r, &[head], &[(file, Some(format!("{i}\n")))], "n").await,
            None,
        ));
    }
    let (fresh, b) = counted(blobs, gate.clone());
    b.start();
    let (blocker, tasks) = queue_in_order(&fresh, &gate, head, &lands).await;
    release(&gate);
    blocker.await.unwrap().unwrap();
    for t in tasks {
        assert!(matches!(
            t.await.unwrap().unwrap(),
            LandResponse::Landed { .. }
        ));
    }
    let u = b.stop();
    let batches = fresh.land_queues.batches.lock().unwrap().clone();
    assert_eq!(batches.len(), 2, "the lands made one batch: {batches:?}");
    u
}

#[tokio::test]
async fn land_batch_budget() {
    let mut measured = Vec::new();
    let mut usages = String::new();
    for k in [8u64, 32] {
        let u = land_batch(k as usize).await;
        measured.push(("meta calls", k, u.meta_calls()));
        measured.push(("blob calls", k, u.blob_calls()));
        measured.push((
            "land lane in flight",
            0,
            u.lane_in_flight(Lane::Land) as u64,
        ));
        usages.push_str(&format!("k = {k}:\n{u}"));
    }
    budget_check("land batch", &measured, &usages);
}

#[tokio::test]
async fn log_budget() {
    let (r, blobs, meta) = plain();
    build(&r, &[]).await;
    let head = long_history(&r, 300, 0).await;
    let (fresh, b) = counted(blobs, meta);
    b.start();
    let entries = fresh.log(head, None, 100).await.unwrap();
    let u = b.stop();
    assert_eq!(entries.len(), 100);
    budget_check(
        "log 100",
        &[
            ("meta calls", 0, u.meta_calls()),
            ("meta in flight", 0, u.meta_in_flight as u64),
        ],
        &u,
    );
}

#[tokio::test]
async fn blame_budget() {
    let mut measured = Vec::new();
    let mut usages = String::new();
    for versions in [8u64, 16] {
        let (u, b) =
            crate::deep_blame_tests::sparse_file_blame(2000, versions as usize, true).await;
        assert_eq!(b.1.len(), versions as usize);
        measured.push(("meta calls", versions, u.meta_calls()));
        measured.push(("meta in flight", versions, u.meta_in_flight as u64));
        usages.push_str(&format!("{versions} versions:\n{u}"));
    }
    budget_check("blame", &measured, &usages);
}

#[tokio::test]
async fn search_budget() {
    let (r, blobs, meta) = plain();
    build(&r, &[]).await;
    let head = r.meta().get_ref("main").await.unwrap().unwrap();
    let mut changes = BTreeMap::new();
    for i in 0..256 {
        let body = Bytes::from(format!("fn f{i}() {{ needle_{} }}\n", i % 7));
        let h = r.put_blob(body.clone()).await.unwrap();
        changes.extend(one_file(&format!("src/m{}/f{i}.rs", i % 16), h, body.len()));
    }
    let tip = r.create_changeset(changeset(head, changes)).await.unwrap();
    let (fresh, b) = counted(blobs, meta);
    b.start();
    let found = fresh
        .search(tip, &SearchQuery::new("needle_3"))
        .await
        .unwrap();
    let u = b.stop();
    assert!(!found.matches.is_empty());
    budget_check(
        "search",
        &[
            ("meta calls", 0, u.meta_calls()),
            ("blob calls", 0, u.blob_calls()),
        ],
        &u,
    );
}

#[tokio::test]
async fn upload_budget() {
    let (_, blobs, meta) = plain();
    let (r, b) = counted(blobs, meta);
    b.start();
    r.put_blob(Bytes::from_static(b"one blob\n")).await.unwrap();
    let u = b.stop();
    budget_check(
        "upload",
        &[
            ("meta calls", 0, u.meta_calls()),
            ("meta in flight", 0, u.meta_in_flight as u64),
        ],
        &u,
    );
    let many: Vec<Bytes> = (0..16)
        .map(|i| Bytes::from(format!("blob {i}\n")))
        .collect();
    b.start();
    r.put_blobs(many).await.unwrap();
    let u = b.stop();
    budget_check(
        "upload x16",
        &[
            ("meta calls", 0, u.meta_calls()),
            ("blob calls", 0, u.blob_calls()),
        ],
        &u,
    );
}
