//! Trees created without their nodes (ADR 0069): every read answers as if
//! the create had stored them, lands land the same changesets, and GC,
//! sweeps and verification treat the missing nodes as derivable, not lost.
//!
//! Most tests run one history twice, on an eager repository and on a lazy
//! one with stores of their own, and compare what cold handles (empty
//! caches, so every unstored node is derived from the stores) answer.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use leep_core::api::{Authorship, MergeRequest};
use leep_core::memory::{MemoryBlobStore, MemoryMetaStore};
use leep_core::{
    BlobStore, Changeset, ChangesetId, FileChange, FileKind, Hash, RepoPath, TreeNode,
};
use leep_testkit::Rng;

use crate::fault_tests::TestClock;
use crate::lazy_tree::MAX_LAZY_DEPTH;
use crate::Repo;

struct Pair {
    eager: Repo,
    lazy: Repo,
    eager_blobs: Arc<MemoryBlobStore>,
    lazy_blobs: Arc<MemoryBlobStore>,
}

fn pair() -> Pair {
    let eager_blobs = Arc::new(MemoryBlobStore::new());
    let lazy_blobs = Arc::new(MemoryBlobStore::new());
    Pair {
        eager: Repo::new(eager_blobs.clone(), Arc::new(MemoryMetaStore::new())),
        lazy: Repo::new(lazy_blobs.clone(), Arc::new(MemoryMetaStore::new()))
            .with_lazy_create_trees(true),
        eager_blobs,
        lazy_blobs,
    }
}

/// A handle on `r`'s stores with nothing cached.
fn cold(r: &Repo) -> Repo {
    Repo::new(r.blobs().clone(), r.meta().clone())
}

fn rp(s: &str) -> RepoPath {
    RepoPath::new(s).unwrap()
}

fn cs(parents: &[ChangesetId], changes: BTreeMap<RepoPath, FileChange>, msg: &str) -> Changeset {
    Changeset {
        parents: parents.to_vec(),
        author: "t".into(),
        timestamp: 1,
        message: msg.into(),
        changes,
        extra: BTreeMap::new(),
    }
}

async fn changes(r: &Repo, files: &[(&str, Option<&str>)]) -> BTreeMap<RepoPath, FileChange> {
    let mut out = BTreeMap::new();
    for (p, c) in files {
        let ch = match c {
            Some(t) => {
                let content = r
                    .put_blob(Bytes::copy_from_slice(t.as_bytes()))
                    .await
                    .unwrap();
                FileChange::Modify {
                    content,
                    kind: FileKind::Regular,
                    size: t.len() as u64,
                }
            }
            None => FileChange::Delete,
        };
        out.insert(rp(p), ch);
    }
    out
}

impl Pair {
    /// Create the same changeset on both; both must agree on the outcome.
    async fn commit(
        &self,
        parents: &[ChangesetId],
        files: &[(&str, Option<&str>)],
        msg: &str,
    ) -> Option<ChangesetId> {
        let a = self
            .eager
            .create_changeset(cs(parents, changes(&self.eager, files).await, msg))
            .await;
        let b = self
            .lazy
            .create_changeset(cs(parents, changes(&self.lazy, files).await, msg))
            .await;
        assert_eq!(format!("{a:?}"), format!("{b:?}"), "create {files:?}");
        a.ok()
    }

    async fn land(&self, id: ChangesetId) -> String {
        let a = self.eager.land(id, "main").await;
        let b = self.lazy.land(id, "main").await;
        let (a, b) = (format!("{a:?}"), format!("{b:?}"));
        assert_eq!(a, b, "land {id}");
        a
    }
}

/// What a client can read of `id`, from a cold handle on `r`.
async fn reads(r: &Repo, id: ChangesetId) -> String {
    let r = cold(r);
    let c = r.get_changeset(id).await.unwrap();
    let mut out = format!("root {}\n", r.root_tree(id).await.unwrap());
    let files = r.list_files(id, "").await.unwrap();
    out += &format!("files {files:?}\n");
    out += &format!("dir {:?}\n", r.list_dir(id, "").await.map(|t| t.entries));
    out += &format!(
        "diff {:?}\n",
        r.diff(c.parents.first().copied(), id, "").await
    );
    for f in &files {
        let data = r.read_file(id, &f.path).await.unwrap().map(|(_, b)| b);
        out += &format!("cat {} {data:?}\n", f.path);
        let blame = r.blame(id, &f.path).await.unwrap();
        out += &format!("blame {} {:?}\n", f.path, blame.lines);
        let (last, _) = r.last_change(id, f.path.as_str()).await.unwrap();
        out += &format!("last {} {}\n", f.path, last.id);
    }
    let log: Vec<ChangesetId> = r
        .log(id, Some("d"), 20)
        .await
        .unwrap()
        .into_iter()
        .map(|i| i.id)
        .collect();
    out += &format!("log d {log:?}\n");
    let hits = r
        .search(id, &crate::SearchQuery::new("line"))
        .await
        .unwrap();
    out += &format!("search {:?}\n", hits.matches);
    out
}

async fn root_stored(blobs: &MemoryBlobStore, root: Hash) -> bool {
    root == TreeNode::empty_hash() || blobs.contains(&root).await.unwrap()
}

/// Tree nodes in a store: every stored blob that decodes as one.
async fn tree_nodes(blobs: &MemoryBlobStore) -> usize {
    let mut n = 0;
    for m in blobs.list_page(None, usize::MAX).await.unwrap() {
        let b = blobs.get(&m.hash).await.unwrap().unwrap();
        if TreeNode::decode(&b).is_ok_and(|t| t.hash() == m.hash) {
            n += 1;
        }
    }
    n
}

#[tokio::test]
async fn a_lazy_create_writes_no_tree_node_and_reads_the_same() {
    let p = pair();
    let base = p
        .commit(
            &[],
            &[
                ("d/a/one.txt", Some("line one\n")),
                ("top.txt", Some("t\n")),
            ],
            "base",
        )
        .await
        .unwrap();
    let (eager_nodes, lazy_nodes) = (
        tree_nodes(&p.eager_blobs).await,
        tree_nodes(&p.lazy_blobs).await,
    );
    assert_eq!(eager_nodes, 3, "the root, d and d/a");
    assert_eq!(lazy_nodes, 0, "a lazy create writes no tree node");
    let blobs_before = p
        .lazy_blobs
        .list_page(None, usize::MAX)
        .await
        .unwrap()
        .len();
    let next = p
        .commit(
            &[base],
            &[
                ("d/b/two.txt", Some("line two\n")),
                ("top.txt", None),
                ("d/a/one.txt", Some("line one, again\n")),
            ],
            "next",
        )
        .await
        .unwrap();
    // Only the two contents were uploaded; nothing else was written.
    assert_eq!(
        p.lazy_blobs
            .list_page(None, usize::MAX)
            .await
            .unwrap()
            .len(),
        blobs_before + 2
    );
    assert_eq!(tree_nodes(&p.lazy_blobs).await, 0);
    assert!(tree_nodes(&p.eager_blobs).await > eager_nodes);
    for id in [base, next] {
        assert_eq!(reads(&p.eager, id).await, reads(&p.lazy, id).await);
    }
    // The same checks and errors as a stored create.
    let bad = p
        .commit(&[next], &[("top.txt", None)], "delete absent")
        .await;
    assert!(bad.is_none());
    let clash = p
        .commit(&[next], &[("d/b", Some("x"))], "file over dir")
        .await;
    assert!(clash.is_none());
}

#[tokio::test]
async fn a_lazy_run_is_bounded_and_derives_from_cold_handles() {
    let p = pair();
    let mut tip = p
        .commit(&[], &[("d/f0", Some("line 0\n"))], "c0")
        .await
        .unwrap();
    let mut ids = vec![tip];
    for i in 1..30 {
        let path = format!("d/s{}/f{i}", i % 4);
        let body = format!("line {i}\n");
        tip = p
            .commit(&[tip], &[(path.as_str(), Some(body.as_str()))], "c")
            .await
            .unwrap();
        ids.push(tip);
    }
    // Every (MAX_LAZY_DEPTH + 1)th create stores its tree, whole.
    let stored: Vec<bool> = {
        let mut v = Vec::new();
        for id in &ids {
            v.push(root_stored(&p.lazy_blobs, p.lazy.root_tree(*id).await.unwrap()).await);
        }
        v
    };
    let period = MAX_LAZY_DEPTH as usize + 1;
    for (i, s) in stored.iter().enumerate() {
        assert_eq!(*s, i % period == period - 1, "changeset {i}");
    }
    for id in &ids {
        assert_eq!(reads(&p.eager, *id).await, reads(&p.lazy, *id).await);
    }
    // A cold read of the tip derives at most the run above a stored tree.
    let c = cold(&p.lazy);
    c.list_files(tip, "").await.unwrap();
    assert!((c.lazy_tree_derivations() as usize) < period);
    assert!(c.lazy_tree_derivations() > 0);
}

#[tokio::test]
async fn a_fast_forward_land_stores_the_tree_and_a_copy_reads_the_same() {
    let p = pair();
    let base = p
        .commit(&[], &[("d/f", Some("line base\n"))], "base")
        .await
        .unwrap();
    assert!(p.land(base).await.contains("rebased: false"));
    let root = p.lazy.root_tree(base).await.unwrap();
    assert!(
        root_stored(&p.lazy_blobs, root).await,
        "the ref's tree is stored"
    );
    // A stack of two lazy creates, fast-forwarded: the tip's tree is stored.
    let a = p
        .commit(&[base], &[("d/a", Some("line a\n"))], "a")
        .await
        .unwrap();
    let b = p
        .commit(&[a], &[("e/b", Some("line b\n"))], "b")
        .await
        .unwrap();
    p.land(b).await;
    assert!(root_stored(&p.lazy_blobs, p.lazy.root_tree(b).await.unwrap()).await);
    assert!(!root_stored(&p.lazy_blobs, p.lazy.root_tree(a).await.unwrap()).await);
    // Overlapping changes from an old base: the slow path merges them.
    let side = p
        .commit(&[base], &[("d/f", Some("line base\nside\n"))], "side")
        .await
        .unwrap();
    let on_top = p
        .commit(&[side], &[("d/c", Some("line c\n"))], "on top")
        .await
        .unwrap();
    let landed = p.land(on_top).await;
    assert!(landed.contains("rebased: true"), "{landed}");
    let head = p.eager.meta().get_ref("main").await.unwrap().unwrap();
    assert_eq!(head, p.lazy.meta().get_ref("main").await.unwrap().unwrap());
    for id in [base, a, b, side, on_top, head] {
        assert_eq!(reads(&p.eager, id).await, reads(&p.lazy, id).await);
    }
}

/// Random histories of creates (on the head, on unlanded changesets, on old
/// changesets), lands, merges and rebases, in both modes: every answer, the
/// head and what every changeset reads are the same.
#[tokio::test]
async fn random_histories_answer_the_same_eager_and_lazy() {
    for seed in 1..=12u64 {
        let p = pair();
        let mut rng = Rng::new(seed);
        let mut known: Vec<ChangesetId> = Vec::new();
        let paths = ["d/a", "d/b", "d/x/c", "e/f", "e/y/g", "top"];
        for step in 0..60 {
            let head = p.eager.meta().get_ref("main").await.unwrap();
            match rng.below(10) {
                0..=4 => {
                    let parent = match (rng.below(3), head) {
                        (0, _) | (_, None) if !known.is_empty() => {
                            Some(known[rng.below(known.len())])
                        }
                        (_, h) => h,
                    };
                    let parents: Vec<ChangesetId> = parent.into_iter().collect();
                    let existing = match parent {
                        Some(id) => p.eager.list_files(id, "").await.unwrap(),
                        None => Vec::new(),
                    };
                    let mut files: Vec<(String, Option<String>)> = Vec::new();
                    for _ in 0..1 + rng.below(3) {
                        if !existing.is_empty() && rng.below(4) == 0 {
                            let f = &existing[rng.below(existing.len())];
                            files.push((f.path.to_string(), None));
                        } else {
                            let path = paths[rng.below(paths.len())].to_string();
                            files.push((path, Some(format!("line {seed} {step}\n"))));
                        }
                    }
                    files.sort();
                    files.dedup_by(|a, b| a.0 == b.0);
                    let files: Vec<(&str, Option<&str>)> = files
                        .iter()
                        .map(|(p, c)| (p.as_str(), c.as_deref()))
                        .collect();
                    if let Some(id) = p.commit(&parents, &files, &format!("s{step}")).await {
                        known.push(id);
                    }
                }
                5..=7 if !known.is_empty() => {
                    let id = known[rng.below(known.len())];
                    let r = p.land(id).await;
                    if let Some(h) = p.eager.meta().get_ref("main").await.unwrap() {
                        if !r.contains("Conflicts") {
                            known.push(h);
                        }
                    }
                }
                8 if known.len() >= 2 => {
                    let ours = known[rng.below(known.len())];
                    let theirs = known[rng.below(known.len())];
                    let req = || MergeRequest {
                        parents: [ours, theirs],
                        message: "merge".into(),
                        authorship: Authorship {
                            author: "t".into(),
                            timestamp: Some(1),
                        },
                        extra: BTreeMap::new(),
                    };
                    let a = p.eager.merge(req()).await;
                    let b = p.lazy.merge(req()).await;
                    assert_eq!(format!("{a:?}"), format!("{b:?}"), "seed {seed} merge");
                    if let Ok(leep_core::api::MergeResponse::Merged { id }) = a {
                        known.push(id);
                    }
                }
                9 if !known.is_empty() && head.is_some() => {
                    let id = known[rng.below(known.len())];
                    let onto = head.unwrap();
                    let a = p.eager.rebase(id, onto).await;
                    let b = p.lazy.rebase(id, onto).await;
                    assert_eq!(format!("{a:?}"), format!("{b:?}"), "seed {seed} rebase");
                    if let Ok(leep_core::api::RebaseResponse::Rebased { id }) = a {
                        known.push(id);
                    }
                }
                _ => {}
            }
        }
        assert_eq!(
            p.eager.meta().list_refs().await.unwrap(),
            p.lazy.meta().list_refs().await.unwrap(),
            "seed {seed}"
        );
        for id in &known {
            assert_eq!(
                reads(&p.eager, *id).await,
                reads(&p.lazy, *id).await,
                "seed {seed}: {id}"
            );
        }
        assert!(
            tree_nodes(&p.lazy_blobs).await < tree_nodes(&p.eager_blobs).await,
            "seed {seed}"
        );
    }
}

/// A history with unlanded lazy stacks, a fast-forward and copies.
async fn mixed_history(p: &Pair) -> Vec<ChangesetId> {
    let base = p
        .commit(
            &[],
            &[("d/a", Some("line a\n")), ("e/b", Some("line b\n"))],
            "base",
        )
        .await
        .unwrap();
    p.land(base).await;
    let mut ids = vec![base];
    let mut tip = base;
    for i in 0..5 {
        let path = format!("d/s{i}");
        let body = format!("line {i}\n");
        tip = p
            .commit(&[tip], &[(path.as_str(), Some(body.as_str()))], "stack")
            .await
            .unwrap();
        ids.push(tip);
    }
    let other = p
        .commit(&[base], &[("e/b", Some("line b2\n"))], "other")
        .await
        .unwrap();
    ids.push(other);
    p.land(other).await;
    let abandoned = p
        .commit(&[base], &[("e/gone", Some("line gone\n"))], "abandoned")
        .await
        .unwrap();
    ids.push(abandoned);
    p.land(tip).await;
    ids.push(p.eager.meta().get_ref("main").await.unwrap().unwrap());
    ids
}

#[tokio::test]
async fn verify_accepts_derivable_trees_and_reports_a_wrong_root() {
    use crate::integrity::{verify, Quiet, VerifyOptions};
    let p = pair();
    let ids = mixed_history(&p).await;
    let opts = VerifyOptions {
        full: true,
        rehash: true,
        ..VerifyOptions::default()
    };
    let eager = verify(&p.eager, &opts, &Quiet).await.unwrap();
    let lazy = verify(&p.lazy, &opts, &Quiet).await.unwrap();
    assert!(eager.ok(), "{:#?}", eager.problems);
    assert!(lazy.ok(), "{:#?}", lazy.problems);
    assert_eq!(eager.stats.lazy_tree_nodes, 0);
    assert!(lazy.stats.lazy_tree_nodes > 0);
    assert_eq!(eager.stats.tree_nodes, lazy.stats.tree_nodes);
    assert_eq!(eager.stats.blobs_checked, lazy.stats.blobs_checked);
    // A lazy changeset whose root_tree row is wrong does not derive.
    let abandoned = ids[ids.len() - 2];
    let mut bad = p.lazy.root_tree(abandoned).await.unwrap();
    bad.0[31] ^= 1;
    p.lazy
        .meta()
        .put_derived(
            "root_tree",
            &abandoned.0,
            Bytes::copy_from_slice(bad.as_bytes()),
        )
        .await
        .unwrap();
    let r = verify(&cold(&p.lazy), &opts, &Quiet).await.unwrap();
    assert!(!r.ok());
    assert!(
        r.problems.iter().any(|pr| pr.code == "missing_tree"),
        "{:#?}",
        r.problems
    );
    let err = cold(&p.lazy).list_files(abandoned, "").await.unwrap_err();
    assert!(err.to_string().contains("corrupt tree"), "{err:?}");
}

#[tokio::test]
async fn gc_sweeps_and_reports_keep_and_count_what_eager_mode_does() {
    use crate::changeset_sweep::{self, SweepOptions};
    use crate::gc::{self, GcOptions, Mark};
    use crate::gc_unreachable::{self, UnreachableOptions};
    let p = pair();
    let ids = mixed_history(&p).await;
    let later = leep_core::store::unix_now() + 10 * 86_400;
    let sweep = SweepOptions {
        grace: Duration::from_secs(86_400),
        fence: Duration::from_secs(900),
        batch: 3,
        rate: 0,
        max_delete: u64::MAX,
        index_fence: Duration::from_secs(60),
        tombstone_retention: Duration::from_secs(86_400),
        ..SweepOptions::default()
    };
    let unreachable = UnreachableOptions {
        now: later,
        grace: Duration::from_secs(86_400),
        ref_log_retention: Duration::from_secs(86_400),
        exact_trees: true,
    };
    let eager_report = gc_unreachable::report(&p.eager, &unreachable)
        .await
        .unwrap();
    let lazy_report = gc_unreachable::report(&p.lazy, &unreachable).await.unwrap();
    assert_eq!(lazy_report.missing_nodes, 0);
    for (e, l) in [
        (&eager_report.log_kept, &lazy_report.log_kept),
        (&eager_report.log_pruned, &lazy_report.log_pruned),
    ] {
        assert_eq!(e.reclaimable.changesets, l.reclaimable.changesets);
        assert_eq!(e.reclaimable.contents, l.reclaimable.contents);
        assert!(l.reclaimable.tree_nodes <= e.reclaimable.tree_nodes);
    }
    let blob_gc = GcOptions {
        grace: Duration::ZERO,
        fence: Duration::from_secs(600),
        unsafe_no_fence: false,
        dry_run: false,
        backup_fence: None,
    };
    for r in [&p.eager, &p.lazy] {
        for mode in [Mark::Full, Mark::Incremental] {
            gc::run_with(r, &blob_gc, mode, &TestClock::at(later))
                .await
                .unwrap();
        }
    }
    for id in &ids {
        assert_eq!(reads(&p.eager, *id).await, reads(&p.lazy, *id).await);
    }
    for r in [&p.eager, &p.lazy] {
        changeset_sweep::condemn(r, &sweep, &TestClock::at(later))
            .await
            .unwrap();
        changeset_sweep::collect(r, &sweep, &TestClock::at(later + 901))
            .await
            .unwrap();
        gc::run_with(r, &blob_gc, Mark::Incremental, &TestClock::at(later + 1000))
            .await
            .unwrap();
    }
    let mut left = 0;
    for id in &ids {
        let (e, l) = (
            p.eager.meta().get_changeset(id).await.unwrap(),
            p.lazy.meta().get_changeset(id).await.unwrap(),
        );
        assert_eq!(e.is_some(), l.is_some(), "{id}");
        if e.is_some() {
            left += 1;
            assert_eq!(reads(&p.eager, *id).await, reads(&p.lazy, *id).await);
        }
    }
    assert!(
        left < ids.len(),
        "the sweep deleted the abandoned changeset"
    );
    let opts = crate::integrity::VerifyOptions {
        full: true,
        ..Default::default()
    };
    let v = crate::integrity::verify(&p.lazy, &opts, &crate::integrity::Quiet)
        .await
        .unwrap();
    assert!(v.ok(), "{:#?}", v.problems);
}

/// A slow-path land of a lazy create, on a handle whose caches never saw
/// the create (another server, or nodes evicted): its tree is derived once,
/// not once per node the merge reads.
#[tokio::test]
async fn a_slow_path_land_derives_the_lazy_tree_once() {
    let blobs = Arc::new(MemoryBlobStore::new());
    let meta = Arc::new(MemoryMetaStore::new());
    let r = Repo::new(blobs.clone(), meta.clone()).with_lazy_create_trees(true);
    let mut files: Vec<(String, String)> = Vec::new();
    for d in 0..20 {
        for f in 0..10 {
            files.push((format!("src/d{d}/sub/f{f}.rs"), format!("line {d} {f}\n")));
        }
    }
    let refs: Vec<(&str, Option<&str>)> = files
        .iter()
        .map(|(p, c)| (p.as_str(), Some(c.as_str())))
        .collect();
    let base = r
        .create_changeset(cs(&[], changes(&r, &refs).await, "base"))
        .await
        .unwrap();
    r.land(base, "main").await.unwrap();
    // Lazy creates on the base, each touching three deep directories.
    let mut lands = Vec::new();
    for i in 0..6 {
        let edits: Vec<(String, String)> = (0..3)
            .map(|k| {
                (
                    format!("src/d{}/sub/f{i}.rs", (i * 3 + k) % 20),
                    format!("line edit {i} {k}\n"),
                )
            })
            .collect();
        let refs: Vec<(&str, Option<&str>)> = edits
            .iter()
            .map(|(p, c)| (p.as_str(), Some(c.as_str())))
            .collect();
        lands.push(
            r.create_changeset(cs(&[base], changes(&r, &refs).await, &format!("e{i}")))
                .await
                .unwrap(),
        );
    }
    // Every land after the first replays the slow way onto a moved head.
    // No cache at all: a busy server's creates churn its derived-node
    // cache faster than a land's merge reads one tree.
    let lander = Repo::new(blobs, meta)
        .with_land_fast_path(false)
        .with_cache_budget(0);
    for (i, id) in lands.iter().enumerate() {
        let before = lander.lazy_tree_derivations();
        let resp = lander.land(*id, "main").await.unwrap();
        let derived = lander.lazy_tree_derivations() - before;
        assert!(
            matches!(resp, leep_core::api::LandResponse::Landed { .. }),
            "{resp:?}"
        );
        assert!(derived <= 1, "land {i} derived {derived} trees");
    }
}

/// Counts the reads that found nothing: on the filesystem store each one
/// rescans the packs.
struct MissCounting {
    inner: Arc<MemoryBlobStore>,
    misses: std::sync::atomic::AtomicUsize,
}

#[async_trait::async_trait]
impl BlobStore for MissCounting {
    async fn put_with_hash(&self, hash: Hash, data: Bytes) -> leep_core::Result<()> {
        self.inner.put_with_hash(hash, data).await
    }
    async fn get(&self, hash: &Hash) -> leep_core::Result<Option<Bytes>> {
        let got = self.inner.get(hash).await?;
        if got.is_none() {
            self.misses
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        Ok(got)
    }
    /// The exact retry of a miss `get` counted already (ADR 0070).
    async fn get_exact(&self, hash: &Hash) -> leep_core::Result<Option<Bytes>> {
        self.inner.get_exact(hash).await
    }
    async fn contains(&self, hash: &Hash) -> leep_core::Result<bool> {
        self.inner.contains(hash).await
    }
    async fn list_page(
        &self,
        after: Option<Hash>,
        limit: usize,
    ) -> leep_core::Result<Vec<leep_core::BlobMeta>> {
        self.inner.list_page(after, limit).await
    }
    async fn delete(&self, hash: &Hash) -> leep_core::Result<()> {
        self.inner.delete(hash).await
    }
}

/// A slow-path land of a lazy create asks the store for none of its
/// unstored nodes when this process made the create (and for its root alone
/// when another did), and stores no more nodes than the directories its
/// copy rewrites: what it costs follows the paths it changes, not the size
/// of the tree.
#[tokio::test]
async fn a_slow_path_land_stays_off_the_store_and_stores_only_its_paths() {
    use std::sync::atomic::Ordering::Relaxed;
    let mem = Arc::new(MemoryBlobStore::new());
    let blobs = Arc::new(MissCounting {
        inner: mem.clone(),
        misses: Default::default(),
    });
    let meta = Arc::new(MemoryMetaStore::new());
    let r = Repo::new(blobs.clone(), meta.clone())
        .with_lazy_create_trees(true)
        .with_land_fast_path(false);
    let mut files: Vec<(String, String)> = Vec::new();
    for a in 0..30 {
        for b in 0..10 {
            files.push((format!("src/m{a}/s{b}/f.rs"), format!("line {a} {b}\n")));
        }
    }
    let refs: Vec<(&str, Option<&str>)> = files
        .iter()
        .map(|(p, c)| (p.as_str(), Some(c.as_str())))
        .collect();
    let base = r
        .create_changeset(cs(&[], changes(&r, &refs).await, "base"))
        .await
        .unwrap();
    r.land(base, "main").await.unwrap();
    let mut made = Vec::new();
    for i in 0..4 {
        let path = format!("src/m{i}/s{i}/new.rs");
        let body = format!("line {i}\n");
        made.push(
            r.create_changeset(cs(&[base], changes(&r, &[(&path, Some(&body))]).await, "e"))
                .await
                .unwrap(),
        );
    }
    let other = Repo::new(blobs.clone(), meta.clone()).with_land_fast_path(false);
    for (i, id) in made.iter().enumerate() {
        let lander = if i % 2 == 0 { &r } else { &other };
        let (misses, stored) = (
            blobs.misses.load(Relaxed),
            mem.list_page(None, usize::MAX).await.unwrap().len(),
        );
        let resp = lander.land(*id, "main").await.unwrap();
        assert!(
            matches!(resp, leep_core::api::LandResponse::Landed { rebased, .. } if rebased == (i > 0)),
            "{resp:?}"
        );
        let missed = blobs.misses.load(Relaxed) - misses;
        let added = mem.list_page(None, usize::MAX).await.unwrap().len() - stored;
        let allowed_misses = if i % 2 == 0 { 0 } else { 1 };
        assert!(missed <= allowed_misses, "land {i}: {missed} store misses");
        // The root, src, src/m<i> and src/m<i>/s<i>: the copy's, or the
        // fast-forwarded tree's.
        assert!(added <= 4, "land {i} stored {added} objects");
    }
}
