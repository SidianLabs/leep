//! Fault tolerance of `land`, derived data, GC and the ref log (ADR 0021).
//!
//! Every test runs the real `Repo` over in-memory stores wrapped in
//! `leep-testkit`'s fault injectors, then checks invariants through a second,
//! fault-free `Repo` over the same stores. A failing assertion names the seed;
//! rerun with that seed to replay the same fault stream.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use leep_core::api::LandResponse;
use leep_core::memory::{MemoryBlobStore, MemoryMetaStore};
use leep_core::{
    BlobStore, Changeset, ChangesetId, Error, FileChange, FileKind, MetaStore, RepoPath, Result,
};
use leep_testkit::{is_injected, Fault, FaultPlan, FaultyBlobStore, FaultyMetaStore, Op, Rng};

use crate::Repo;

/// Stores shared by any number of faulty "servers" and one clean verifier.
struct World {
    blobs: Arc<MemoryBlobStore>,
    meta: Arc<MemoryMetaStore>,
    plan: Arc<FaultPlan>,
    /// Reads the same data with no faults.
    clean: Repo,
}

impl World {
    fn new(seed: u64) -> World {
        let blobs = Arc::new(MemoryBlobStore::new());
        let meta = Arc::new(MemoryMetaStore::new());
        World {
            clean: Repo::new(blobs.clone(), meta.clone()),
            plan: FaultPlan::new(seed),
            blobs,
            meta,
        }
    }

    /// A server instance: its own `Repo` (so its own land locks) over faulty
    /// views of the shared stores.
    fn server(&self) -> Repo {
        self.server_with(&self.plan)
    }

    /// Like [`World::server`], with faults from `plan` instead.
    fn server_with(&self, plan: &Arc<FaultPlan>) -> Repo {
        Repo::new(
            Arc::new(FaultyBlobStore::new(self.blobs.clone(), plan.clone())),
            Arc::new(FaultyMetaStore::new(self.meta.clone(), plan.clone())),
        )
    }
}

fn rp(s: &str) -> RepoPath {
    RepoPath::new(s).unwrap()
}

/// Run `f` until it succeeds, the way a client retries a failed request.
/// Only injected faults are retried; a real error is a bug and panics.
async fn retry<T, Fut: Future<Output = Result<T>>>(
    seed: u64,
    what: &str,
    mut f: impl FnMut() -> Fut,
) -> T {
    for _ in 0..400 {
        match f().await {
            Ok(v) => return v,
            Err(e) if is_injected(&e) => {}
            Err(e) => panic!("seed {seed}: {what}: unexpected error: {e}"),
        }
    }
    panic!("seed {seed}: {what}: still failing after 400 tries");
}

/// Upload one file and commit it on `parent`, retrying through faults.
async fn commit_file(
    seed: u64,
    repo: &Repo,
    parent: Option<ChangesetId>,
    path: &str,
    body: &str,
    msg: &str,
) -> ChangesetId {
    let data = Bytes::from(body.to_string());
    let content = retry(seed, "upload", || repo.put_blob(data.clone())).await;
    let cs = Changeset {
        parents: parent.into_iter().collect(),
        author: "agent".into(),
        timestamp: 7,
        message: msg.into(),
        changes: BTreeMap::from([(
            rp(path),
            FileChange::Modify {
                content,
                kind: FileKind::Regular,
                size: body.len() as u64,
            },
        )]),
        extra: BTreeMap::new(),
    };
    retry(seed, "create changeset", || {
        repo.create_changeset(cs.clone())
    })
    .await
}

/// Every changeset reachable from `head`, found iteratively.
async fn history(r: &Repo, head: ChangesetId) -> Vec<ChangesetId> {
    let mut seen = HashSet::from([head]);
    let mut out = Vec::new();
    let mut stack = vec![head];
    while let Some(id) = stack.pop() {
        out.push(id);
        for p in r
            .get_changeset(id)
            .await
            .expect("history is readable")
            .parents
        {
            if seen.insert(p) {
                stack.push(p);
            }
        }
    }
    out
}

/// Everything a ref reaches is readable: each changeset in its history, its
/// whole tree and every file blob. Returns the history.
async fn assert_ref_sound(seed: u64, r: &Repo, name: &str) -> Vec<ChangesetId> {
    let head = r
        .meta()
        .get_ref(name)
        .await
        .unwrap()
        .unwrap_or_else(|| panic!("seed {seed}: ref {name} is gone"));
    let ids = history(r, head).await;
    for id in &ids {
        let files = r
            .list_files(*id, "")
            .await
            .unwrap_or_else(|e| panic!("seed {seed}: tree of {id} unreadable: {e}"));
        for f in files {
            let (_, data) = r
                .read_file(*id, &f.path)
                .await
                .unwrap_or_else(|e| panic!("seed {seed}: {id}:{} unreadable: {e}", f.path))
                .expect("listed file exists");
            assert_eq!(data.len() as u64, f.size, "seed {seed}: size of {}", f.path);
        }
    }
    ids
}

/// The ref log replays to the current refs: per ref, entries chain
/// (`old` is the previous `new`) and end at the ref's value.
async fn assert_ref_log_replays(seed: u64, meta: &dyn MetaStore) {
    let mut log = meta.ref_log(None, None, usize::MAX).await.unwrap();
    log.reverse();
    let mut replay: BTreeMap<String, ChangesetId> = BTreeMap::new();
    for e in &log {
        assert_eq!(
            replay.get(&e.name).copied(),
            e.old,
            "seed {seed}: log entry {} of {} does not continue the previous one",
            e.seq,
            e.name
        );
        match e.new {
            Some(n) => replay.insert(e.name.clone(), n),
            None => replay.remove(&e.name),
        };
    }
    assert_eq!(
        replay,
        meta.list_refs().await.unwrap(),
        "seed {seed}: ref log does not replay to the refs"
    );
}

// ---------------------------------------------------------------------------
// Land

const AGENTS: usize = 12;

/// Seeds to run: `LEEP_FAULT_SEED=n` replays one, `LEEP_FAULT_SEEDS=n` runs
/// `1..=n`, and the default is a short sweep.
fn seeds() -> std::ops::RangeInclusive<u64> {
    let var = |k: &str| std::env::var(k).ok().and_then(|v| v.parse::<u64>().ok());
    match (var("LEEP_FAULT_SEED"), var("LEEP_FAULT_SEEDS")) {
        (Some(one), _) => one..=one,
        (None, Some(n)) => 1..=n,
        _ => 1..=25,
    }
}

/// What one simulated agent was told.
struct Agent {
    n: usize,
    /// Paths and bodies of its stack, oldest first.
    files: Vec<(String, String)>,
    /// The id every `land` reply that said success named.
    reported: Vec<ChangesetId>,
}

async fn run_agent(seed: u64, repo: Repo, n: usize, steps: usize, per_try: Duration) -> Agent {
    // Start from whatever main is now, like a fresh checkout.
    let mut parent = retry(seed, "read main", || repo.meta().get_ref("main")).await;
    let mut files = Vec::new();
    for k in 0..steps {
        let path = format!("agent-{n}/step-{k}.txt");
        let body = format!("agent {n} step {k}\n");
        parent = Some(commit_file(seed, &repo, parent, &path, &body, &format!("a{n}s{k}")).await);
        files.push((path, body));
    }
    let top = parent.expect("at least one step");
    let mut reported = Vec::new();
    for _ in 0..1000 {
        // A client that gives up after `per_try` and asks again, which also
        // cancels the request wherever it happens to be.
        match tokio::time::timeout(per_try, repo.land(top, "main")).await {
            Ok(Ok(LandResponse::Landed { landed, .. })) => {
                reported.push(landed);
                break;
            }
            Ok(Ok(LandResponse::Conflicts { .. })) => {
                panic!("seed {seed}: disjoint files conflict")
            }
            Ok(Err(e)) if is_injected(&e) => {}
            Ok(Err(e)) => panic!("seed {seed}: agent {n}: land failed for real: {e}"),
            Err(_) => {}
        }
    }
    assert!(!reported.is_empty(), "seed {seed}: agent {n} never landed");
    Agent { n, files, reported }
}

/// Many agents landing stacks through several server instances while the
/// stores fail, stall and answer ambiguously. Nothing a land reported may be
/// lost, nothing may land twice, and everything reachable stays readable.
async fn lands_under_faults(seed: u64) {
    let w = World::new(seed);
    let ten_min = Duration::from_secs(600);
    w.plan
        .jitter(3)
        .rate(&[Op::CasRef], 150, Fault::FailAfter)
        .rate(&[Op::CasRef], 80, Fault::Fail)
        .rate(&[Op::CasRef], 30, Fault::Stall(ten_min))
        .rate(Op::WRITES, 15, Fault::FailAfter)
        .rate(Op::WRITES, 15, Fault::Fail)
        .rate(Op::META, 3, Fault::Fail)
        .rate(Op::BLOB, 3, Fault::Fail)
        .rate(Op::META, 10, Fault::Delay(Duration::from_millis(5)));
    let servers: Vec<Repo> = (0..3).map(|_| w.server()).collect();
    let mut rng = Rng::new(seed ^ 0xA5A5);
    let mut tasks = Vec::new();
    for n in 0..AGENTS {
        let repo = servers[n % servers.len()].clone();
        let steps = 1 + rng.below(3);
        tasks.push(tokio::spawn(run_agent(
            seed,
            repo,
            n,
            steps,
            Duration::from_secs(60),
        )));
    }
    let mut agents = Vec::new();
    for t in tasks {
        agents.push(t.await.unwrap());
    }
    // Guards against a plan that injects nothing. How many land on a given
    // seed depends on task scheduling and on how many store calls a land
    // makes: from about 10 to 100 per run, since a batch stores its replays
    // in one write (ADR 0043).
    assert!(
        w.plan.injected().total() > 5,
        "seed {seed}: the plan injected almost nothing"
    );

    w.plan.pause();
    let ids = assert_ref_sound(seed, &w.clean, "main").await;
    let head = w.meta.get_ref("main").await.unwrap().unwrap();

    // Every reported success is in history, and so is every file it carried.
    for a in &agents {
        for landed in &a.reported {
            assert!(
                w.clean.is_ancestor(*landed, head).await.unwrap(),
                "seed {seed}: agent {} was told {landed} landed but it is not in history",
                a.n
            );
        }
        for (path, body) in &a.files {
            let (_, data) = w
                .clean
                .read_file(head, &rp(path))
                .await
                .unwrap()
                .unwrap_or_else(|| panic!("seed {seed}: {path} missing at main"));
            assert_eq!(&data[..], body.as_bytes(), "seed {seed}: {path} content");
        }
    }

    // Each commit appears once: a retried land adds no copy of its stack.
    let expected: usize = agents.iter().map(|a| a.files.len()).sum();
    let mut by_message: HashMap<String, usize> = HashMap::new();
    for id in &ids {
        *by_message
            .entry(w.clean.get_changeset(*id).await.unwrap().message)
            .or_default() += 1;
    }
    let dups: Vec<_> = by_message.iter().filter(|(_, c)| **c > 1).collect();
    assert!(
        dups.is_empty(),
        "seed {seed}: duplicated in history: {dups:?}"
    );
    assert_eq!(ids.len(), expected, "seed {seed}: history length");
    assert_ref_log_replays(seed, &*w.meta).await;
}

#[tokio::test(start_paused = true)]
async fn lands_under_faults_lose_nothing_and_duplicate_nothing() {
    for seed in seeds() {
        lands_under_faults(seed).await;
    }
}

/// A land whose commit is answered with an error after it applied: the
/// caller may retry, and the retry must find its own work already landed.
#[tokio::test(start_paused = true)]
async fn ambiguous_cas_is_resolved_not_replayed() {
    for in_call_retry in [true, false] {
        let w = World::new(1);
        let clean = &w.clean;
        let base = commit_file(1, clean, None, "base.txt", "base\n", "base").await;
        w.meta.cas_ref("main", None, Some(base)).await.unwrap();
        // A two-commit stack forked from base, and another land that moves main.
        let s1 = commit_file(1, clean, Some(base), "a/1.txt", "1\n", "s1").await;
        let s2 = commit_file(1, clean, Some(s1), "a/2.txt", "2\n", "s2").await;
        let other = commit_file(1, clean, Some(base), "other.txt", "o\n", "other").await;
        w.meta
            .cas_ref("main", Some(base), Some(other))
            .await
            .unwrap();

        // The first commit attempt applies and then reports an error.
        w.plan.script(Op::CasRef, 1, Fault::FailAfter);
        let server = w.server();
        let first = server.land(s2, "main").await;
        let landed_once = w.meta.get_ref("main").await.unwrap().unwrap();
        assert_ne!(landed_once, other, "the ambiguous commit did apply");
        if in_call_retry {
            // The land notices its own commit and reports success.
            match first.unwrap() {
                LandResponse::Landed {
                    landed, rebased, ..
                } => {
                    assert_eq!(landed, landed_once);
                    assert!(rebased);
                }
                other => panic!("{other:?}"),
            }
        } else {
            assert!(first.is_err() || matches!(first, Ok(LandResponse::Landed { .. })));
        }

        // The client asks again, through a fresh server (its answer was lost).
        let again = w.server().land(s2, "main").await.unwrap();
        match again {
            LandResponse::Landed { landed, .. } => {
                assert!(clean.is_ancestor(landed, landed_once).await.unwrap());
            }
            other => panic!("{other:?}"),
        }
        let head = w.meta.get_ref("main").await.unwrap().unwrap();
        assert_eq!(head, landed_once, "a repeated land moves nothing");
        let ids = assert_ref_sound(1, clean, "main").await;
        // base, other, s1', s2'.
        assert_eq!(ids.len(), 4, "no duplicate of the stack");
        assert_ref_log_replays(1, &*w.meta).await;
        assert_eq!(w.meta.ref_log(None, None, 100).await.unwrap().len(), 3);
    }
}

/// The request is cancelled after the commit applied (a timeout that fires
/// during the response), and the client lands the same changeset again.
#[tokio::test(start_paused = true)]
async fn land_cancelled_after_commit_is_idempotent() {
    let w = World::new(2);
    let clean = &w.clean;
    let base = commit_file(2, clean, None, "base.txt", "base\n", "base").await;
    w.meta.cas_ref("main", None, Some(base)).await.unwrap();
    let feature = commit_file(2, clean, Some(base), "f.txt", "f\n", "feature").await;
    let other = commit_file(2, clean, Some(base), "o.txt", "o\n", "other").await;
    w.meta
        .cas_ref("main", Some(base), Some(other))
        .await
        .unwrap();

    w.plan
        .script(Op::CasRef, 1, Fault::Stall(Duration::from_secs(3600)));
    let timed_out =
        tokio::time::timeout(Duration::from_secs(30), w.server().land(feature, "main")).await;
    assert!(timed_out.is_err(), "the land was cancelled");
    let after_cancel = w.meta.get_ref("main").await.unwrap().unwrap();
    assert_ne!(after_cancel, other, "the commit applied before the cancel");

    // Another agent lands meanwhile, then the first client asks again.
    let third = commit_file(2, clean, Some(after_cancel), "t.txt", "t\n", "third").await;
    w.meta
        .cas_ref("main", Some(after_cancel), Some(third))
        .await
        .unwrap();
    match w.server().land(feature, "main").await.unwrap() {
        LandResponse::Landed { landed, .. } => assert_eq!(landed, after_cancel),
        other => panic!("{other:?}"),
    }
    assert_eq!(w.meta.get_ref("main").await.unwrap(), Some(third));
    let ids = assert_ref_sound(2, clean, "main").await;
    assert_eq!(ids.len(), 4, "base, other, feature, third");
    assert_ref_log_replays(2, &*w.meta).await;
}

/// A commit that keeps failing is reported, not retried forever, and leaves
/// the ref alone; the same land succeeds once the store recovers.
#[tokio::test(start_paused = true)]
async fn land_reports_a_commit_that_keeps_failing() {
    let w = World::new(3);
    let base = commit_file(3, &w.clean, None, "base.txt", "base\n", "base").await;
    w.meta.cas_ref("main", None, Some(base)).await.unwrap();
    let next = commit_file(3, &w.clean, Some(base), "n.txt", "n\n", "next").await;

    w.plan.rate(&[Op::CasRef], 1000, Fault::Fail);
    let err = w.server().land(next, "main").await.unwrap_err();
    assert!(is_injected(&err), "{err}");
    assert_eq!(w.plan.calls(Op::CasRef), 3, "bounded retries");
    assert_eq!(w.meta.get_ref("main").await.unwrap(), Some(base));

    w.plan.clear();
    match w.server().land(next, "main").await.unwrap() {
        LandResponse::Landed { landed, .. } => assert_eq!(landed, next),
        other => panic!("{other:?}"),
    }
    assert_ref_log_replays(3, &*w.meta).await;
}

// ---------------------------------------------------------------------------
// Derived data

/// A meta store that remembers every derived row written through it and can
/// flip one bit of one row on read. Sits below the fault injector so it sees
/// what really reached storage.
struct Tamper {
    inner: Arc<dyn MetaStore>,
    written: std::sync::Mutex<BTreeMap<(String, leep_core::Hash), Bytes>>,
    /// A key written twice with different values, which derived data forbids.
    conflicts: std::sync::Mutex<Vec<(String, leep_core::Hash)>>,
    flip: std::sync::Mutex<Option<(String, leep_core::Hash, usize)>>,
}

impl Tamper {
    fn new(inner: Arc<dyn MetaStore>) -> Arc<Tamper> {
        Arc::new(Tamper {
            inner,
            written: Default::default(),
            conflicts: Default::default(),
            flip: Default::default(),
        })
    }

    fn rows(&self) -> BTreeMap<(String, leep_core::Hash), Bytes> {
        self.written.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl MetaStore for Tamper {
    async fn put_changeset(&self, cs: &Changeset) -> Result<ChangesetId> {
        self.inner.put_changeset(cs).await
    }
    async fn get_changeset(&self, id: &ChangesetId) -> Result<Option<Changeset>> {
        self.inner.get_changeset(id).await
    }
    async fn list_changesets(
        &self,
        after: Option<ChangesetId>,
        limit: usize,
    ) -> Result<Vec<ChangesetId>> {
        self.inner.list_changesets(after, limit).await
    }
    async fn get_ref(&self, name: &str) -> Result<Option<ChangesetId>> {
        self.inner.get_ref(name).await
    }
    async fn cas_ref_logged(
        &self,
        name: &str,
        expected: Option<ChangesetId>,
        new: Option<ChangesetId>,
        ctx: &leep_core::RefUpdateContext,
    ) -> Result<bool> {
        self.inner.cas_ref_logged(name, expected, new, ctx).await
    }
    async fn ref_log(
        &self,
        name: Option<&str>,
        before_seq: Option<u64>,
        limit: usize,
    ) -> Result<Vec<leep_core::RefLogEntry>> {
        self.inner.ref_log(name, before_seq, limit).await
    }
    async fn list_refs(&self) -> Result<BTreeMap<String, ChangesetId>> {
        self.inner.list_refs().await
    }
    async fn get_derived(&self, kind: &str, key: &leep_core::Hash) -> Result<Option<Bytes>> {
        let row = self.inner.get_derived(kind, key).await?;
        let flip = self.flip.lock().unwrap().clone();
        Ok(match (row, flip) {
            (Some(b), Some((k, h, bit))) if k == kind && h == *key && bit / 8 < b.len() => {
                let mut v = b.to_vec();
                v[bit / 8] ^= 1 << (bit % 8);
                Some(Bytes::from(v))
            }
            (row, _) => row,
        })
    }
    async fn put_derived(&self, kind: &str, key: &leep_core::Hash, value: Bytes) -> Result<()> {
        let k = (kind.to_string(), *key);
        if let Some(old) = self
            .written
            .lock()
            .unwrap()
            .insert(k.clone(), value.clone())
        {
            if old != value {
                self.conflicts.lock().unwrap().push(k);
            }
        }
        self.inner.put_derived(kind, key, value).await
    }
}

/// A branching history with a merge, deletes and nested directories, built
/// through `r`. Returns the ids oldest first.
async fn small_history(seed: u64, r: &Repo) -> Vec<ChangesetId> {
    async fn commit(
        seed: u64,
        r: &Repo,
        parents: &[ChangesetId],
        files: &[(&str, Option<&str>)],
    ) -> ChangesetId {
        let mut changes = BTreeMap::new();
        for (p, c) in files {
            let ch = match c {
                Some(t) => FileChange::Modify {
                    content: retry(seed, "upload", || r.put_blob(Bytes::from(t.to_string()))).await,
                    kind: FileKind::Regular,
                    size: t.len() as u64,
                },
                None => FileChange::Delete,
            };
            changes.insert(rp(p), ch);
        }
        let cs = Changeset {
            parents: parents.to_vec(),
            author: "t".into(),
            timestamp: 1,
            message: format!("c{}", files.len()),
            changes,
            extra: BTreeMap::new(),
        };
        retry(seed, "commit", || r.create_changeset(cs.clone())).await
    }
    let a = commit(
        seed,
        r,
        &[],
        &[("a.txt", Some("needle a\n")), ("d/x.txt", Some("x1\n"))],
    )
    .await;
    let b = commit(
        seed,
        r,
        &[a],
        &[("d/x.txt", Some("x2 needle\n")), ("d/e/y.txt", Some("y\n"))],
    )
    .await;
    let c = commit(
        seed,
        r,
        &[a],
        &[("a.txt", Some("a3\n")), ("n.txt", Some("n\n"))],
    )
    .await;
    let d = commit(
        seed,
        r,
        &[b],
        &[("d/e/y.txt", None), ("z.txt", Some("z\n"))],
    )
    .await;
    let m = match r
        .merge(leep_core::api::MergeRequest {
            parents: [d, c],
            message: "merge".into(),
            authorship: leep_core::api::Authorship {
                author: "m".into(),
                timestamp: Some(5),
            },
            extra: BTreeMap::new(),
        })
        .await
        .unwrap()
    {
        leep_core::api::MergeResponse::Merged { id } => id,
        other => panic!("{other:?}"),
    };
    let tip = commit(
        seed,
        r,
        &[m],
        &[
            ("d/x.txt", Some("x3\n")),
            ("last.txt", Some("end needle\n")),
        ],
    )
    .await;
    vec![a, b, c, d, m, tip]
}

/// Generation, root tree and (path, content) of every file, for one changeset.
type Snapshot = (u64, leep_core::Hash, Vec<(String, leep_core::Hash)>);

/// Everything the derived kinds feed, as a comparable value.
#[derive(Debug, PartialEq)]
struct Answers {
    per_changeset: Vec<Snapshot>,
    ancestry: Vec<(bool, Option<ChangesetId>)>,
    last_changes: Vec<Option<ChangesetId>>,
    search: Vec<(String, u32)>,
}

async fn answers(r: &Repo, ids: &[ChangesetId]) -> Result<Answers> {
    let mut per = Vec::new();
    for id in ids {
        let files = r
            .list_files(*id, "")
            .await?
            .into_iter()
            .map(|f| (f.path.to_string(), f.content))
            .collect();
        per.push((r.generation(*id).await?, r.root_tree(*id).await?, files));
    }
    let mut ancestry = Vec::new();
    for a in ids {
        for b in ids {
            ancestry.push((r.is_ancestor(*a, *b).await?, r.merge_base(*a, *b).await?));
        }
    }
    let tip = *ids.last().unwrap();
    let mut last_changes = Vec::new();
    for path in ["a.txt", "d", "d/x.txt", "d/e/y.txt", "z.txt"] {
        last_changes.push(match r.last_change(tip, path).await {
            Ok((info, _)) => Some(info.id),
            Err(Error::NotFound(_)) => None,
            Err(e) => return Err(e),
        });
    }
    let search = r
        .search(tip, &crate::SearchQuery::new("needle"))
        .await?
        .matches
        .into_iter()
        .map(|m| (m.path.to_string(), m.line_number as u32))
        .collect();
    Ok(Answers {
        per_changeset: per,
        ancestry,
        last_changes,
        search,
    })
}

/// A copy of `src`'s changesets with no derived rows, over the same blobs.
async fn without_derived(src: &Repo) -> (Arc<MemoryMetaStore>, Vec<ChangesetId>) {
    let meta = Arc::new(MemoryMetaStore::new());
    let ids = src.meta().list_changesets(None, usize::MAX).await.unwrap();
    for id in &ids {
        meta.put_changeset(&src.get_changeset(*id).await.unwrap())
            .await
            .unwrap();
    }
    (meta, ids)
}

/// One derivation entry point per `step`, for `id`.
async fn derive_step(r: &Repo, ids: &[ChangesetId], id: ChangesetId, step: usize) -> Result<()> {
    match step {
        0 => r.root_tree(id).await.map(drop),
        1 => r.generation(id).await.map(drop),
        2 => r.history_root(id).await.map(drop),
        3 => r.derive_search(id).await.map(drop),
        4 => r.is_ancestor(ids[0], id).await.map(drop),
        _ => r.merge_base(ids[1], id).await.map(drop),
    }
}

const DERIVE_STEPS: usize = 6;

/// Derivation that fails midway leaves only missing rows, never wrong ones,
/// and a retry completes with the same answers.
#[tokio::test]
async fn derivation_under_faults_leaves_no_wrong_rows() {
    // Ground truth: every row a fault-free derivation writes.
    let blobs = Arc::new(MemoryBlobStore::new());
    let truth_meta = Tamper::new(Arc::new(MemoryMetaStore::new()));
    let truth = Repo::new(blobs.clone(), truth_meta.clone());
    let ids = small_history(0, &truth).await;
    let want = answers(&truth, &ids).await.unwrap();
    for id in &ids {
        for step in 0..DERIVE_STEPS {
            derive_step(&truth, &ids, *id, step).await.unwrap();
        }
    }
    let truth_rows = truth_meta.rows();
    assert!(truth_rows.len() > 30, "the history derives many rows");

    for seed in 1..=60u64 {
        let (bare, _) = without_derived(&truth).await;
        let spy = Tamper::new(bare);
        let plan = FaultPlan::new(seed);
        plan.rate(Op::META, 40, Fault::Fail)
            .rate(&[Op::PutDerived], 40, Fault::FailAfter)
            .rate(Op::BLOB, 20, Fault::Fail);
        let repo = Repo::new(
            Arc::new(FaultyBlobStore::new(blobs.clone(), plan.clone())),
            Arc::new(FaultyMetaStore::new(spy.clone(), plan.clone())),
        );
        let mut failures = 0;
        // One derivation at a time, retried like a client would, with every
        // stored row checked after each attempt.
        for id in &ids {
            for step in 0..DERIVE_STEPS {
                loop {
                    let result = derive_step(&repo, &ids, *id, step).await;
                    for (key, row) in spy.rows() {
                        assert_eq!(
                            truth_rows.get(&key),
                            Some(&row),
                            "seed {seed}: wrong derived row {} {} after {failures} failures",
                            key.0,
                            key.1
                        );
                    }
                    match result {
                        Ok(()) => break,
                        Err(e) if is_injected(&e) => failures += 1,
                        Err(e) => panic!("seed {seed}: {e}"),
                    }
                    assert!(failures < 5000, "seed {seed}: derivation never completes");
                }
            }
        }
        assert!(failures > 0, "seed {seed}: no fault hit a derivation");
        plan.pause();
        let got = answers(&repo, &ids).await.unwrap();
        assert_eq!(got, want, "seed {seed}");
        assert!(spy.conflicts.lock().unwrap().is_empty(), "seed {seed}");
    }
}

/// A blob store that flips one bit of one blob on read.
struct FlipBlob {
    inner: Arc<dyn BlobStore>,
    target: leep_core::Hash,
    bit: usize,
}

#[async_trait::async_trait]
impl BlobStore for FlipBlob {
    async fn put_with_hash(&self, hash: leep_core::Hash, data: Bytes) -> Result<()> {
        self.inner.put_with_hash(hash, data).await
    }
    async fn get(&self, hash: &leep_core::Hash) -> Result<Option<Bytes>> {
        Ok(self.inner.get(hash).await?.map(|b| {
            if *hash != self.target || self.bit / 8 >= b.len() {
                return b;
            }
            let mut v = b.to_vec();
            v[self.bit / 8] ^= 1 << (self.bit % 8);
            Bytes::from(v)
        }))
    }
    async fn list_page(
        &self,
        after: Option<leep_core::Hash>,
        limit: usize,
    ) -> Result<Vec<leep_core::BlobMeta>> {
        self.inner.list_page(after, limit).await
    }
    async fn delete(&self, hash: &leep_core::Hash) -> Result<()> {
        self.inner.delete(hash).await
    }
}

#[derive(Default, Debug)]
struct Outcomes {
    detected: u32,
    unchanged: u32,
    wrong: u32,
}

impl Outcomes {
    fn record(&mut self, got: Result<Answers>, want: &Answers) {
        match got {
            Err(_) => self.detected += 1,
            Ok(a) if a == *want => self.unchanged += 1,
            Ok(_) => self.wrong += 1,
        }
    }
}

/// Row kinds, and tree nodes, whose damage must never change an answer: each
/// is content-addressed or points at content-addressed data, so a flipped bit
/// surfaces as an error. The other derived kinds carry no checksum yet (ADR
/// 0021), so a flip there can change an answer silently; those are only held
/// to never panicking.
const VERIFIED: &[&str] = &[
    "root_tree",
    "history-tree-v3",
    "history-node-v3",
    "tree node",
];

/// Flip single bits of stored derived rows and tree nodes, one at a time, and
/// ask every question they feed.
async fn flip_outcomes() -> BTreeMap<String, Outcomes> {
    let blobs = Arc::new(MemoryBlobStore::new());
    let truth_meta = Tamper::new(Arc::new(MemoryMetaStore::new()));
    let truth = Repo::new(blobs.clone(), truth_meta.clone());
    let ids = small_history(0, &truth).await;
    let want = Arc::new(answers(&truth, &ids).await.unwrap());
    for id in &ids {
        for step in 0..DERIVE_STEPS {
            derive_step(&truth, &ids, *id, step).await.unwrap();
        }
    }
    let rows = truth_meta.rows();
    let mut rng = Rng::new(99);
    let mut tally: BTreeMap<String, Outcomes> = BTreeMap::new();
    // Short rows are flipped bit by bit, long ones at a seeded sample.
    let mut bits_of = |len: usize| -> Vec<usize> {
        if len <= 40 {
            (0..len * 8).collect()
        } else {
            (0..24).map(|_| rng.below(len * 8)).collect()
        }
    };
    let run = |repo: Repo, ids: Vec<ChangesetId>, what: String| async move {
        tokio::spawn(async move { answers(&repo, &ids).await })
            .await
            .unwrap_or_else(|e| panic!("panic on {what}: {e}"))
    };
    for ((kind, key), row) in &rows {
        for bit in bits_of(row.len()) {
            let flipper = Tamper::new(truth_meta.clone());
            *flipper.flip.lock().unwrap() = Some((kind.clone(), *key, bit));
            let got = run(
                Repo::new(blobs.clone(), flipper),
                ids.clone(),
                format!("bit {bit} of {kind} {key}"),
            )
            .await;
            tally.entry(kind.clone()).or_default().record(got, &want);
        }
    }
    let mut after = None;
    loop {
        let page = blobs.list_page(after, 100).await.unwrap();
        let Some(last) = page.last().map(|m| m.hash) else {
            break;
        };
        after = Some(last);
        for m in page {
            let data = blobs.get(&m.hash).await.unwrap().unwrap();
            if leep_core::Hash::of_object(leep_core::TreeNode::KIND, &data) != m.hash {
                continue;
            }
            for bit in bits_of(data.len()) {
                let flipper = Arc::new(FlipBlob {
                    inner: blobs.clone(),
                    target: m.hash,
                    bit,
                });
                let got = run(
                    Repo::new(flipper, truth_meta.clone()),
                    ids.clone(),
                    format!("bit {bit} of tree node {}", m.hash),
                )
                .await;
                tally
                    .entry("tree node".into())
                    .or_default()
                    .record(got, &want);
            }
        }
    }
    tally
}

#[tokio::test]
async fn damaged_rows_and_tree_nodes_are_detected_where_they_can_be() {
    let tally = flip_outcomes().await;
    for kind in VERIFIED {
        let o = tally
            .get(*kind)
            .unwrap_or_else(|| panic!("no {kind} rows swept"));
        assert!(o.detected > 0, "{kind}: no flip was even noticed: {o:?}");
        assert_eq!(
            o.wrong, 0,
            "{kind}: a flipped bit changed an answer silently: {o:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// Garbage collection

use crate::gc::{self, Clock, GcOptions};

const HOUR: i64 = 3600;
const FENCE: i64 = 600;

fn now() -> i64 {
    leep_core::store::unix_now()
}

/// A clock that moves only when told to or when GC sleeps.
struct TestClock(std::sync::Mutex<i64>);

impl TestClock {
    fn at(t: i64) -> Self {
        TestClock(std::sync::Mutex::new(t))
    }
}

#[async_trait::async_trait]
impl Clock for TestClock {
    fn now(&self) -> i64 {
        *self.0.lock().unwrap()
    }

    async fn sleep(&self, d: Duration) {
        *self.0.lock().unwrap() += d.as_secs() as i64;
    }
}

fn gc_opts() -> GcOptions {
    GcOptions {
        grace: Duration::from_secs(HOUR as u64),
        fence: Duration::from_secs(FENCE as u64),
        unsafe_no_fence: false,
        dry_run: false,
    }
}

/// Make every blob old enough for any grace window.
async fn age_all(blobs: &MemoryBlobStore) {
    for m in blobs.list_page(None, usize::MAX).await.unwrap() {
        blobs.set_modified(&m.hash, Some(now() - 100 * HOUR));
    }
}

/// No stored changeset may point at a tree node or blob that is gone.
async fn assert_no_dangling(seed: u64, w: &World) {
    for id in w
        .clean
        .meta()
        .list_changesets(None, usize::MAX)
        .await
        .unwrap()
    {
        let files = w
            .clean
            .list_files(id, "")
            .await
            .unwrap_or_else(|e| panic!("seed {seed}: tree of {id} lost: {e}"));
        for f in files {
            assert!(
                w.blobs.contains(&f.content).await.unwrap(),
                "seed {seed}: {id}:{} lost its blob",
                f.path
            );
        }
    }
}

fn commit_of(content: leep_core::Hash, size: u64, path: &str) -> Changeset {
    Changeset {
        parents: vec![],
        author: "t".into(),
        timestamp: 1,
        message: format!("adds {path}"),
        changes: BTreeMap::from([(
            rp(path),
            FileChange::Modify {
                content,
                kind: FileKind::Regular,
                size,
            },
        )]),
        extra: BTreeMap::new(),
    }
}

/// A collect that deleted a blob and died before recording it must not
/// leave the blob unusable: a client uploading it again gets an answer.
#[tokio::test(start_paused = true)]
async fn collect_dying_after_a_delete_does_not_wedge_the_blob() {
    for crash in [Fault::Fail, Fault::FailAfter] {
        let w = World::new(5);
        commit_file(5, &w.clean, None, "a.txt", "a\n", "a").await;
        let orphan = w.blobs.put(Bytes::from("orphan")).await.unwrap();
        age_all(&w.blobs).await;
        let t = now() + 1000;
        gc::condemn(&w.clean, &gc_opts(), &TestClock::at(t))
            .await
            .unwrap();

        // Collect writes "deleting", deletes, then writes "deleted": die at
        // that last write, with or without it landing.
        w.plan.script(Op::PutDerived, 2, crash);
        let died = gc::collect(&w.server(), &gc_opts(), &TestClock::at(t + FENCE)).await;
        assert!(died.is_err());
        assert!(
            !w.blobs.contains(&orphan).await.unwrap(),
            "the delete happened"
        );

        // If "deleted" was never recorded, an upload of those bytes is told to
        // retry until a collect runs again (ADR 0012), having stored them
        // first. If it was recorded, the upload just rewrites the blob.
        let blocked = w.clean.put_blob(Bytes::from("orphan")).await;
        match crash {
            Fault::Fail => assert!(matches!(blocked, Err(Error::Conflict(_))), "{blocked:?}"),
            _ => assert!(blocked.is_ok(), "{blocked:?}"),
        }
        assert!(w.blobs.contains(&orphan).await.unwrap());

        // The next collect has nothing to delete, and closes the announcement.
        let done = gc::collect(&w.clean, &gc_opts(), &TestClock::at(t + 2 * FENCE))
            .await
            .unwrap();
        assert_eq!(done.deleted, 0);
        let up = w.clean.put_blob(Bytes::from("orphan")).await.unwrap();
        assert_eq!(up, orphan);
        assert!(w.clean.missing_blobs(&[orphan]).await.unwrap().is_empty());
        w.clean
            .create_changeset(commit_of(orphan, 6, "b.txt"))
            .await
            .unwrap();
        assert_no_dangling(5, &w).await;
    }
}

/// Errors a client answers by trying again: injected faults, a blob GC has
/// condemned ("missing blob") and one it is deleting (409).
fn tolerated(e: &Error) -> bool {
    is_injected(e)
        || matches!(e, Error::Invalid(m) if m.starts_with("missing blob"))
        || matches!(e, Error::Conflict(_))
}

/// Upload `data` and commit it at `path`, as the CLI does. Gives up quietly
/// after many tries; only what it reports as stored is checked by callers.
async fn upload_and_commit(seed: u64, repo: Repo, data: String, path: String) -> bool {
    for _ in 0..40 {
        let h = match repo.put_blob(Bytes::from(data.clone())).await {
            Ok(h) => h,
            Err(e) if tolerated(&e) => continue,
            Err(e) => panic!("seed {seed}: upload: {e}"),
        };
        match repo
            .create_changeset(commit_of(h, data.len() as u64, &path))
            .await
        {
            Ok(_) => return true,
            Err(e) if tolerated(&e) => {}
            Err(e) => panic!("seed {seed}: commit: {e}"),
        }
    }
    false
}

/// Reuse a blob the way the CLI dedups: ask which are missing, and commit the
/// ones the server says it has without uploading them.
async fn reuse_and_commit(seed: u64, repo: Repo, data: String, path: String) {
    let h = leep_core::Hash::of_blob(data.as_bytes());
    match repo.missing_blobs(&[h]).await {
        Ok(m) if m.is_empty() => {}
        Ok(_) => return,
        Err(e) if tolerated(&e) => return,
        Err(e) => panic!("seed {seed}: missing: {e}"),
    }
    match repo
        .create_changeset(commit_of(h, data.len() as u64, &path))
        .await
    {
        Ok(_) => {}
        Err(e) if tolerated(&e) => {}
        Err(e) => panic!("seed {seed}: reuse commit: {e}"),
    }
}

/// Condemn and collect while the stores fail and the GC process is killed at
/// random points, with clients uploading, reusing and committing blobs
/// throughout. A stored changeset never loses data, and a later clean run
/// still collects exactly the garbage.
async fn gc_under_faults(seed: u64) {
    let w = World::new(seed);
    small_history(seed, &w.clean).await;
    let mut rng = Rng::new(seed ^ 0x6C);
    let names: Vec<String> = (0..6).map(|i| format!("orphan {seed} {i}")).collect();
    for n in &names {
        if rng.below(3) > 0 {
            w.blobs.put(Bytes::from(n.clone())).await.unwrap();
        }
    }
    age_all(&w.blobs).await;

    // The GC process also sees damaged reads; clients do not, because a
    // damaged read there could store a wrong derived row.
    let gc_plan = FaultPlan::new(seed ^ 1);
    gc_plan
        .jitter(2)
        .rate(Op::WRITES, 30, Fault::FailAfter)
        .rate(Op::WRITES, 30, Fault::Fail)
        .rate(Op::META, 2, Fault::Fail)
        .rate(Op::BLOB, 2, Fault::Fail)
        .rate(&[Op::GetDerived, Op::Get], 3, Fault::Corrupt);
    let client_plan = FaultPlan::new(seed ^ 2);
    client_plan
        .jitter(2)
        .rate(Op::WRITES, 20, Fault::FailAfter)
        .rate(Op::WRITES, 20, Fault::Fail)
        .rate(Op::META, 5, Fault::Fail);
    let (gc_repo, client_repo) = (w.server_with(&gc_plan), w.server_with(&client_plan));

    let mut t = now() + 1000;
    for step in 0..14 {
        let mut clients = Vec::new();
        for k in 0..rng.below(3) {
            let n = names[rng.below(names.len())].clone();
            let path = format!("c{step}-{k}.txt");
            let repo = client_repo.clone();
            clients.push(if rng.below(2) == 0 {
                tokio::spawn(async move {
                    upload_and_commit(seed, repo, n, path).await;
                })
            } else {
                tokio::spawn(reuse_and_commit(seed, repo, n, path))
            });
        }
        let kill_after = if rng.below(3) == 0 {
            rng.below(60)
        } else {
            usize::MAX
        };
        let clock = TestClock::at(t);
        let (r, opts) = (gc_repo.clone(), gc_opts());
        let gc_run = tokio::spawn(async move {
            // Dying mid-run is the point; the result is not.
            if t % 2 == 0 {
                let _ =
                    leep_testkit::cancel_after_polls(gc::condemn(&r, &opts, &clock), kill_after)
                        .await;
            } else {
                let _ =
                    leep_testkit::cancel_after_polls(gc::collect(&r, &opts, &clock), kill_after)
                        .await;
            }
        });
        gc_run.await.unwrap();
        for c in clients {
            c.await.unwrap();
        }
        t += if rng.below(2) == 0 { FENCE + 1 } else { 5 };
        assert_no_dangling(seed, &w).await;
    }

    // A fault-free pass finishes whatever the dead ones left, and then only
    // referenced data remains.
    t += 3 * HOUR;
    let clock = TestClock::at(t);
    gc::condemn(&w.clean, &gc_opts(), &clock).await.unwrap();
    t += FENCE + 1;
    gc::collect(&w.clean, &gc_opts(), &TestClock::at(t))
        .await
        .unwrap();
    assert_no_dangling(seed, &w).await;
    let left = gc::condemn(
        &w.clean,
        &GcOptions {
            dry_run: true,
            ..gc_opts()
        },
        &TestClock::at(t + HOUR),
    )
    .await
    .unwrap();
    assert_eq!(
        (left.condemned, left.already_condemned),
        (0, 0),
        "seed {seed}: garbage survived a clean collect: {left:?}"
    );

    // No blob is left unusable by a dead run: an upload either works or is
    // told to retry until one more collect has run, and then works.
    for n in &names {
        let mut up = w.clean.put_blob(Bytes::from(n.clone())).await;
        if matches!(up, Err(Error::Conflict(_))) {
            t += FENCE + 1;
            gc::collect(&w.clean, &gc_opts(), &TestClock::at(t))
                .await
                .unwrap();
            up = w.clean.put_blob(Bytes::from(n.clone())).await;
        }
        up.unwrap_or_else(|e| panic!("seed {seed}: {n:?} is unusable after a collect: {e}"));
    }
    assert_no_dangling(seed, &w).await;
}

#[tokio::test(start_paused = true)]
async fn gc_under_faults_never_loses_referenced_data() {
    for seed in seeds() {
        gc_under_faults(seed).await;
    }
}

/// Incremental marking (ADR 0033) killed anywhere: the GC process dies at
/// random points and its writes fail before or after landing, while clients
/// commit new files deep in the tree. After every round the record left
/// behind must still agree with a full mark, so the next incremental run
/// never stops at a tree node whose subtree was not recorded, and nothing
/// referenced is lost.
async fn incremental_mark_under_faults(seed: u64) {
    let w = World::new(seed);
    small_history(seed, &w.clean).await;
    let mut rng = Rng::new(seed ^ 0x33);
    let gc_plan = FaultPlan::new(seed ^ 3);
    gc_plan
        .jitter(2)
        .rate(&[Op::PutDerived], 25, Fault::FailAfter)
        .rate(&[Op::PutDerived], 25, Fault::Fail)
        .rate(Op::META, 2, Fault::Fail)
        .rate(&[Op::GetDerived, Op::Get], 3, Fault::Corrupt);
    let gc_repo = w.server_with(&gc_plan);
    let mut t = now() + 1000;
    let mut tips: Vec<ChangesetId> = w
        .clean
        .meta()
        .list_changesets(None, usize::MAX)
        .await
        .unwrap();
    for round in 0..10 {
        let mut clients = Vec::new();
        for k in 0..rng.below(4) {
            let repo = w.clean.clone();
            let parent = tips[rng.below(tips.len())];
            let path = format!("p{}/q{}/r{round}-{k}.txt", rng.below(3), rng.below(2));
            let body = format!("seed {seed} round {round} file {k} v{}\n", rng.below(3));
            clients.push(tokio::spawn(async move {
                commit_file(seed, &repo, Some(parent), &path, &body, "deep").await
            }));
        }
        if rng.below(4) == 0 {
            w.blobs
                .put(Bytes::from(format!("orphan {seed} {round}")))
                .await
                .unwrap();
            age_all(&w.blobs).await;
        }
        let kill_after = if rng.below(4) == 0 {
            usize::MAX
        } else {
            rng.below(80)
        };
        let (r, opts, clock) = (gc_repo.clone(), gc_opts(), TestClock::at(t));
        let collecting = rng.below(2) == 0;
        let gc_run = tokio::spawn(async move {
            // Dying mid-run is the point; the result is not.
            if collecting {
                let _ =
                    leep_testkit::cancel_after_polls(gc::collect(&r, &opts, &clock), kill_after)
                        .await;
            } else {
                let _ =
                    leep_testkit::cancel_after_polls(gc::condemn(&r, &opts, &clock), kill_after)
                        .await;
            }
        });
        gc_run.await.unwrap();
        for c in clients {
            tips.push(c.await.unwrap());
        }
        t += FENCE + 1;
        if let Err(e) = gc::check_incremental_mark(&w.clean).await {
            panic!("seed {seed} round {round}: {e}");
        }
        assert_no_dangling(seed, &w).await;
    }
    // A clean cycle afterwards collects the garbage and keeps the history.
    gc::condemn(&w.clean, &gc_opts(), &TestClock::at(t + HOUR))
        .await
        .unwrap();
    gc::collect(&w.clean, &gc_opts(), &TestClock::at(t + HOUR + FENCE))
        .await
        .unwrap();
    assert_no_dangling(seed, &w).await;
}

#[tokio::test(start_paused = true)]
async fn incremental_mark_survives_being_killed_anywhere() {
    for seed in seeds() {
        incremental_mark_under_faults(seed).await;
    }
}
