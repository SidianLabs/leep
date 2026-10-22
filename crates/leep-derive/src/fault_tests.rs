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
    BlobStore, Changeset, ChangesetId, Error, FileChange, FileKind, LandId, MetaStore, RepoPath,
    Result,
};
use leep_testkit::{is_injected, Fault, FaultPlan, FaultyBlobStore, FaultyMetaStore, Op, Rng};

use crate::Repo;

/// Land admission per ref (ADR 0060), in the same sweep.
#[path = "admission_fault_tests.rs"]
mod admission;

/// The changeset sweep (ADR 0059 phase 2), in the same sweep.
#[path = "sweep_fault_tests.rs"]
mod sweep;

/// Stores shared by any number of faulty "servers" and one clean verifier.
pub(crate) struct World {
    pub(crate) blobs: Arc<MemoryBlobStore>,
    pub(crate) meta: Arc<MemoryMetaStore>,
    pub(crate) plan: Arc<FaultPlan>,
    /// Reads the same data with no faults.
    pub(crate) clean: Repo,
}

impl World {
    fn new(seed: u64) -> World {
        Self::with_meta(seed, MemoryMetaStore::new())
    }

    /// Like [`World::new`], over `meta` (for example one with a lease clock).
    pub(crate) fn with_meta(seed: u64, meta: MemoryMetaStore) -> World {
        let blobs = Arc::new(MemoryBlobStore::new());
        let meta = Arc::new(meta);
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
    pub(crate) fn server_with(&self, plan: &Arc<FaultPlan>) -> Repo {
        Repo::new(
            Arc::new(FaultyBlobStore::new(self.blobs.clone(), plan.clone())),
            Arc::new(FaultyMetaStore::new(self.meta.clone(), plan.clone())),
        )
    }
}

pub(crate) fn rp(s: &str) -> RepoPath {
    RepoPath::new(s).unwrap()
}

/// Run `f` until it succeeds, the way a client retries a failed request.
/// Only injected faults are retried; a real error is a bug and panics.
pub(crate) async fn retry<T, Fut: Future<Output = Result<T>>>(
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
pub(crate) async fn commit_file(
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
pub(crate) async fn history(r: &Repo, head: ChangesetId) -> Vec<ChangesetId> {
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
pub(crate) async fn assert_ref_sound(seed: u64, r: &Repo, name: &str) -> Vec<ChangesetId> {
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
pub(crate) async fn assert_ref_log_replays(seed: u64, meta: &dyn MetaStore) {
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
/// `n` seeds from `LEEP_FAULT_SEED_START` (default 1), and the default is a
/// short sweep. A start lets the nightly sweep cover new seeds every night.
pub(crate) fn seeds() -> std::ops::RangeInclusive<u64> {
    let var = |k: &str| std::env::var(k).ok().and_then(|v| v.parse::<u64>().ok());
    let start = var("LEEP_FAULT_SEED_START").unwrap_or(1);
    match (var("LEEP_FAULT_SEED"), var("LEEP_FAULT_SEEDS")) {
        (Some(one), _) => one..=one,
        (None, Some(n)) => start..=start + n.max(1) - 1,
        _ => start..=start + 24,
    }
}

/// What one simulated agent was told.
pub(crate) struct Agent {
    pub(crate) n: usize,
    /// Paths and bodies of its stack, oldest first.
    pub(crate) files: Vec<(String, String)>,
    /// The id every `land` reply that said success named.
    pub(crate) reported: Vec<ChangesetId>,
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
    // Half the agents name their land (ADR 0057), the rest rely on the
    // landed-copy scan.
    let land_id = n
        .is_multiple_of(2)
        .then(|| LandId::from_bytes(&[n as u8 + 1; 16]).unwrap());
    let mut reported = Vec::new();
    for _ in 0..1000 {
        // A client that gives up after `per_try` and asks again, which also
        // cancels the request wherever it happens to be.
        let land = repo.land_keyed(top, "main", None, true, land_id.clone());
        match tokio::time::timeout(per_try, land).await {
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
    assert_lands_kept(seed, &w, &agents).await;
}

/// After agents stop: everything `main` reaches is readable, every land an
/// agent was told about is in its history with every file of its stack,
/// nothing landed twice, and the ref log replays to the refs.
pub(crate) async fn assert_lands_kept(seed: u64, w: &World, agents: &[Agent]) {
    let ids = assert_ref_sound(seed, &w.clean, "main").await;
    let head = w.meta.get_ref("main").await.unwrap().unwrap();

    // Every reported success is in history, and so is every file it carried.
    for a in agents {
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

/// Move `main` up by `n` changesets of other servers' lands, each on the
/// last, so a retry's rebased copy ends up `n` changesets below the head.
async fn land_others(w: &World, n: usize) {
    let mut head = w.meta.get_ref("main").await.unwrap();
    for i in 0..n {
        let next = commit_file(0, &w.clean, head, &format!("others/{i}"), "o\n", "other").await;
        let r = w.clean.land_as(next, "main", Some("other")).await.unwrap();
        assert!(matches!(r, LandResponse::Landed { rebased: false, .. }));
        head = Some(next);
    }
}

/// A land whose commit applied but whose answer never reached the client
/// (cancelled after the commit), retried under its land id after more lands
/// than the landed-copy scan reads (ADR 0057): the retry answers with the
/// original result and moves nothing. The same retry without the id lands
/// a second copy, which is the window the id closes.
#[tokio::test(start_paused = true)]
async fn a_late_retry_with_a_land_id_returns_the_original_result() {
    let w = World::new(3);
    let clean = &w.clean;
    let base = commit_file(3, clean, None, "base.txt", "base\n", "base").await;
    w.meta.cas_ref("main", None, Some(base)).await.unwrap();
    let feature = commit_file(3, clean, Some(base), "f.txt", "f\n", "feature").await;
    let other = commit_file(3, clean, Some(base), "o.txt", "o\n", "other").await;
    w.meta
        .cas_ref("main", Some(base), Some(other))
        .await
        .unwrap();
    let id = LandId::from_bytes(&[42; 16]).unwrap();

    w.plan
        .script(Op::CasRef, 1, Fault::Stall(Duration::from_secs(3600)));
    let server = w.server();
    let land = server.land_keyed(feature, "main", None, true, Some(id.clone()));
    assert!(
        tokio::time::timeout(Duration::from_secs(30), land)
            .await
            .is_err(),
        "the client never heard back"
    );
    let copy = w.meta.get_ref("main").await.unwrap().unwrap();
    assert_ne!(copy, other, "the commit applied before the cancel");

    land_others(&w, 1100).await;
    let head = w.meta.get_ref("main").await.unwrap();
    let entries = w
        .meta
        .ref_log(Some("main"), None, usize::MAX)
        .await
        .unwrap()
        .len();
    let again = w
        .server()
        .land_keyed(feature, "main", None, true, Some(id.clone()))
        .await
        .unwrap();
    assert_eq!(
        again,
        LandResponse::Landed {
            landed: copy,
            rebased: true,
            attempts: 1
        }
    );
    assert_eq!(w.meta.get_ref("main").await.unwrap(), head, "nothing moved");
    assert_eq!(
        w.meta
            .ref_log(Some("main"), None, usize::MAX)
            .await
            .unwrap()
            .len(),
        entries
    );

    // The id names one changeset.
    let stranger = commit_file(3, clean, Some(base), "s.txt", "s\n", "stranger").await;
    assert!(matches!(
        w.server()
            .land_keyed(stranger, "main", None, true, Some(id))
            .await,
        Err(Error::LandIdReused { requested, .. }) if requested == stranger
    ));
    assert_eq!(w.meta.get_ref("main").await.unwrap(), head);

    // Without the id the copy is out of the scan's reach: a second copy.
    match w.server().land(feature, "main").await.unwrap() {
        LandResponse::Landed { landed, .. } => assert_ne!(landed, copy),
        other => panic!("{other:?}"),
    }
    assert_ref_log_replays(3, &*w.meta).await;
}

/// The same land sent twice at once through two paths, as a client does
/// when it gives up on one server while that server's leader is still
/// landing it and sends it to another: one copy lands, and both requests
/// are answered with it.
#[tokio::test(start_paused = true)]
async fn a_land_sent_twice_through_two_servers_lands_once() {
    for delayed in [1, 2] {
        let w = World::new(4);
        let clean = &w.clean;
        let base = commit_file(4, clean, None, "base.txt", "base\n", "base").await;
        w.meta.cas_ref("main", None, Some(base)).await.unwrap();
        let feature = commit_file(4, clean, Some(base), "f.txt", "f\n", "feature").await;
        let other = commit_file(4, clean, Some(base), "o.txt", "o\n", "other").await;
        w.meta
            .cas_ref("main", Some(base), Some(other))
            .await
            .unwrap();
        let id = LandId::from_bytes(&[7; 16]).unwrap();
        let calls = w.plan.calls(Op::CasRef);
        // Hold one path's commit until the other has landed, within the
        // batch's lifetime (a batch past it answers retryably).
        w.plan.script(
            Op::CasRef,
            calls + delayed,
            Fault::Delay(Duration::from_secs(45)),
        );

        let (leader, follower) = (w.server(), w.server());
        let first = {
            let id = id.clone();
            tokio::spawn(async move {
                leader
                    .land_keyed(feature, "main", None, true, Some(id))
                    .await
            })
        };
        tokio::time::sleep(Duration::from_secs(5)).await;
        // Given up on the first path: the same land, elsewhere.
        let second = follower
            .land_keyed(feature, "main", None, true, Some(id))
            .await
            .unwrap();
        let first = first.await.unwrap().unwrap();
        let landed = |r: &LandResponse| match r {
            LandResponse::Landed { landed, .. } => *landed,
            other => panic!("{other:?}"),
        };
        assert_eq!(
            landed(&first),
            landed(&second),
            "both answered with one copy"
        );
        let head = w.meta.get_ref("main").await.unwrap().unwrap();
        assert_eq!(head, landed(&first));
        let ids = assert_ref_sound(4, clean, "main").await;
        assert_eq!(ids.len(), 3, "base, other, one copy of feature");
        let lands: Vec<_> = w
            .meta
            .ref_log(Some("main"), None, 100)
            .await
            .unwrap()
            .into_iter()
            .filter(|e| e.operation == "land")
            .collect();
        assert_eq!(lands.len(), 1, "one move for the land");
        assert_ref_log_replays(4, &*w.meta).await;
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
pub(crate) struct Tamper {
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
    async fn find_lands(
        &self,
        name: &str,
        ids: &[leep_core::LandId],
    ) -> Result<Vec<Option<leep_core::LandRecord>>> {
        self.inner.find_lands(name, ids).await
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
    async fn delete_derived_kind(&self, kind: &str) -> Result<u64> {
        self.written.lock().unwrap().retain(|(k, _), _| k != kind);
        self.inner.delete_derived_kind(kind).await
    }
    fn supports_leases(&self) -> bool {
        self.inner.supports_leases()
    }
    async fn lease(
        &self,
        key: &leep_core::LeaseKey,
        timing: leep_core::LeaseTiming,
    ) -> Result<Option<leep_core::Lease>> {
        self.inner.lease(key, timing).await
    }
    async fn acquire_lease(
        &self,
        claim: &leep_core::LeaseClaim<'_>,
        timing: leep_core::LeaseTiming,
    ) -> Result<leep_core::Lease> {
        self.inner.acquire_lease(claim, timing).await
    }
    async fn renew_leases(
        &self,
        holder: &str,
        held: &[(leep_core::LeaseKey, u64)],
        timing: leep_core::LeaseTiming,
    ) -> Result<Vec<Option<Duration>>> {
        self.inner.renew_leases(holder, held, timing).await
    }
    async fn release_lease(
        &self,
        key: &leep_core::LeaseKey,
        holder: &str,
        epoch: u64,
    ) -> Result<bool> {
        self.inner.release_lease(key, holder, epoch).await
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
    for path in [
        "a.txt",
        "d",
        "d/x.txt",
        "d/e/y.txt",
        "z.txt",
        "w0/f0.txt",
        "w3/f9.txt",
    ] {
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

/// A history five lanes wide: mainline and four branches, each editing its
/// own directory and a shared one under its own author, branches restarting
/// from mainline now and then, mainline merging a branch every ten steps
/// and an octopus merge of every lane at the end. Derivation has several
/// changesets ready at once. Returns every id, the octopus last.
async fn lanes_history(r: &Repo, n: usize) -> Vec<ChangesetId> {
    async fn commit(
        r: &Repo,
        parents: &[ChangesetId],
        author: &str,
        files: &[(String, String)],
    ) -> ChangesetId {
        let mut changes = BTreeMap::new();
        for (path, body) in files {
            let content = r.put_blob(Bytes::from(body.clone())).await.unwrap();
            changes.insert(
                rp(path),
                FileChange::Modify {
                    content,
                    kind: FileKind::Regular,
                    size: body.len() as u64,
                },
            );
        }
        let cs = Changeset {
            parents: parents.to_vec(),
            author: author.into(),
            timestamp: 1,
            message: format!("{} files", files.len()),
            changes,
            extra: BTreeMap::new(),
        };
        r.create_changeset(cs).await.unwrap()
    }
    let root = commit(r, &[], "Root", &[("shared/s0".into(), "0".into())]).await;
    let mut ids = vec![root];
    let mut tips = [root; 5];
    for i in 1..n {
        let lane = i % 5;
        let id = if lane == 0 && i % 10 == 0 {
            let other = tips[(i / 10) % 4 + 1];
            let files = [(format!("m/f{}", i % 13), i.to_string())];
            commit(r, &[tips[0], other], "Merger", &files).await
        } else {
            let files = [
                (format!("l{lane}/f{}", i % 17), i.to_string()),
                (format!("shared/s{}", i % 7), format!("{lane}.{i}")),
            ];
            commit(r, &[tips[lane]], &format!("Dev {lane}"), &files).await
        };
        tips[lane] = id;
        ids.push(id);
        if lane != 0 && i % 37 == 0 {
            tips[lane] = tips[0];
        }
    }
    let mut parents: Vec<ChangesetId> = Vec::new();
    for t in tips {
        if !parents.contains(&t) {
            parents.push(t);
        }
    }
    ids.push(commit(r, &parents, "Octopus", &[]).await);
    ids
}

/// Deriving on several cores (ADR 0050) under faults stores rows only after
/// their parents' rows and a changeset's value only after the rows it names,
/// so whatever a failed run leaves is where a retry, a restarted server or
/// a query resumes, and they end with the rows a fault-free serial run
/// writes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn parallel_derivation_under_faults_stores_rows_after_their_parents() {
    use crate::derived::{
        derive, DerivedKind, HistoryTree, LogFilterIndex, HISTORY_NODE_KIND, LOG_FILTER_NAME_KIND,
    };
    let blobs = Arc::new(MemoryBlobStore::new());
    let truth_meta = Tamper::new(Arc::new(MemoryMetaStore::new()));
    let truth = Repo::new(blobs.clone(), truth_meta.clone()).with_derive_jobs(1);
    let ids = lanes_history(&truth, 300).await;
    let tip = *ids.last().unwrap();
    derive::<HistoryTree>(&truth, tip).await.unwrap();
    derive::<LogFilterIndex>(&truth, tip).await.unwrap();
    let truth_rows = truth_meta.rows();
    let mut parents: HashMap<leep_core::Hash, Vec<ChangesetId>> = HashMap::new();
    for id in &ids {
        parents.insert(id.0, truth.get_changeset(*id).await.unwrap().parents);
    }
    let chained = [HistoryTree::NAME, LogFilterIndex::NAME];
    let compared = [
        HistoryTree::NAME,
        HISTORY_NODE_KIND,
        LogFilterIndex::NAME,
        LOG_FILTER_NAME_KIND,
    ];

    // Each seed derives 300 changesets several times over; every third
    // seed keeps a sweep's time near the other tests'.
    for seed in seeds().step_by(3) {
        let (bare, _) = without_derived(&truth).await;
        let spy = Tamper::new(bare);
        let plan = FaultPlan::new(seed);
        // Rows reach the store a window of 256 changesets at a time, so an
        // attempt keeps only the windows it finished: faults are rare enough
        // for one to get through, and often enough to hit most runs, and
        // writes partway.
        plan.rate(Op::META, 2, Fault::Fail)
            .rate(&[Op::PutDerived], 4, Fault::FailAfter)
            .rate(Op::BLOB, 1, Fault::Fail);
        let repo = Repo::new(
            Arc::new(FaultyBlobStore::new(blobs.clone(), plan.clone())),
            Arc::new(FaultyMetaStore::new(spy.clone(), plan.clone())),
        )
        .with_derive_jobs(4);
        let mut failures = 0;
        loop {
            let (a, b) = futures_util::join!(
                derive::<HistoryTree>(&repo, tip),
                derive::<LogFilterIndex>(&repo, tip)
            );
            let rows = spy.rows();
            for ((kind, key), row) in &rows {
                assert_eq!(
                    truth_rows.get(&(kind.clone(), *key)),
                    Some(row),
                    "seed {seed}: wrong row {kind} {key}"
                );
                if chained.contains(&kind.as_str()) {
                    for p in &parents[key] {
                        assert!(
                            rows.contains_key(&(kind.clone(), p.0)),
                            "seed {seed}: the {kind} row of {key} is stored before its parent {p}'s"
                        );
                    }
                }
            }
            match (a, b) {
                (Ok(_), Ok(_)) => break,
                (a, b) => {
                    for e in [a.err(), b.err()].into_iter().flatten() {
                        assert!(is_injected(&e), "seed {seed}: {e}");
                    }
                    failures += 1;
                }
            }
            assert!(failures < 5000, "seed {seed}: derivation never completes");
        }
        assert!(failures > 0, "seed {seed}: no fault hit a derivation");
        let rows = spy.rows();
        for kind in compared {
            let of = |m: &BTreeMap<(String, leep_core::Hash), Bytes>| {
                m.iter()
                    .filter(|((k, _), _)| k == kind)
                    .map(|((_, h), v)| (*h, v.clone()))
                    .collect::<Vec<_>>()
            };
            assert!(
                of(&rows) == of(&truth_rows),
                "seed {seed}: {kind} rows differ"
            );
        }
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
    "history-tree-v4",
    "history-node-v4",
    "tree node",
];

/// Flip single bits of stored derived rows and tree nodes, one at a time, and
/// ask every question they feed.
async fn flip_outcomes() -> BTreeMap<String, Outcomes> {
    let blobs = Arc::new(MemoryBlobStore::new());
    let truth_meta = Tamper::new(Arc::new(MemoryMetaStore::new()));
    let truth = Repo::new(blobs.clone(), truth_meta.clone());
    let mut ids = small_history(0, &truth).await;
    // A changeset touching enough directories that its history pack takes
    // more than one part, so `history-node-v4` rows are swept too.
    let mut changes = BTreeMap::new();
    for i in 0..24 {
        let text = format!("wide {i}\n");
        let content = truth.put_blob(Bytes::from(text.clone())).await.unwrap();
        changes.insert(
            rp(&format!("w{}/f{i}.txt", i % 6)),
            FileChange::Modify {
                content,
                kind: FileKind::Regular,
                size: text.len() as u64,
            },
        );
    }
    let wide = truth
        .create_changeset(Changeset {
            parents: vec![*ids.last().unwrap()],
            author: "t".into(),
            timestamp: 1,
            message: "wide".into(),
            changes,
            extra: BTreeMap::new(),
        })
        .await
        .unwrap();
    ids.push(wide);
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
pub(crate) struct TestClock(std::sync::Mutex<i64>);

impl TestClock {
    pub(crate) fn at(t: i64) -> Self {
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
        backup_fence: None,
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

/// A store whose lease clock follows tokio's, so a paused test moves both.
fn clocked_meta() -> MemoryMetaStore {
    let base = tokio::time::Instant::now();
    MemoryMetaStore::new().with_lease_clock(Arc::new(move || {
        1_000_000 + base.elapsed().as_millis() as i64
    }))
}

/// A copy of `src`'s changesets with no derived rows, on a lease clock that
/// follows tokio's.
async fn clocked_copy(src: &Repo) -> Arc<MemoryMetaStore> {
    let meta = Arc::new(clocked_meta());
    for id in src.meta().list_changesets(None, usize::MAX).await.unwrap() {
        meta.put_changeset(&src.get_changeset(id).await.unwrap())
            .await
            .unwrap();
    }
    meta
}

/// One index server of [`index_holders_under_faults`]: a candidate for the
/// path-history and log-filter leases, warming in `lease` mode.
struct IndexServer {
    leases: Arc<crate::indexing::IndexLeases>,
    warmer: crate::warm::Warmer,
    plan: Arc<FaultPlan>,
}

impl IndexServer {
    fn new(blobs: &Arc<MemoryBlobStore>, meta: &Arc<Tamper>, seed: u64, n: u64) -> IndexServer {
        use crate::indexing::{IndexGroup, IndexLeaseConfig, IndexLeases};
        let plan = FaultPlan::new(seed * 1000 + n);
        plan.rate(Op::META, 2, Fault::Fail)
            .rate(&[Op::PutDerived], 4, Fault::FailAfter)
            .rate(&[Op::Lease], 60, Fault::Fail)
            .rate(&[Op::Lease], 20, Fault::FailAfter);
        let faulty: Arc<dyn MetaStore> = Arc::new(FaultyMetaStore::new(meta.clone(), plan.clone()));
        let repo = Repo::new(
            Arc::new(FaultyBlobStore::new(blobs.clone(), plan.clone())),
            faulty.clone(),
        )
        .with_derive_jobs(3);
        let cfg = IndexLeaseConfig::new(format!("server-{n}"), "")
            .with_candidates(vec![IndexGroup::History, IndexGroup::LogFilter]);
        let leases = IndexLeases::new(faulty, cfg);
        let warmer = crate::warm::Warmer::new(repo, false)
            .with_leases(leases.clone())
            .with_full_listing_every(Duration::ZERO);
        IndexServer {
            leases,
            warmer,
            plan,
        }
    }
}

/// ADR 0058's lease fault sweep. Index servers in `lease` mode take turns
/// at random: renewing (or taking) leases, warming, warming while another
/// warms, being killed at a random point of a wavefront (their lease left
/// to go stale), and being paused past the stale time (the store's clock
/// jumps), all under store faults, lease calls included. After every step
/// each stored row is the fault-free row and has its parents' rows, and in
/// the end the rows are the fault-free run's.
async fn index_holders_under_faults(
    seed: u64,
    truth: &Repo,
    truth_rows: &RowsByKey,
    parents: &HashMap<leep_core::Hash, Vec<ChangesetId>>,
    heads: &[ChangesetId],
) {
    use crate::derived::{
        DerivedKind, HistoryTree, LogFilterIndex, HISTORY_NODE_KIND, LOG_FILTER_NAME_KIND,
    };
    let bare = clocked_copy(truth).await;
    for (i, h) in heads.iter().enumerate() {
        bare.cas_ref(&format!("lane{i}"), None, Some(*h))
            .await
            .unwrap();
    }
    let spy = Tamper::new(bare);
    let blobs = Arc::new(MemoryBlobStore::new());
    for id in truth
        .meta()
        .list_changesets(None, usize::MAX)
        .await
        .unwrap()
    {
        for (_, change) in truth.get_changeset(id).await.unwrap().changes {
            if let FileChange::Modify { content, .. } = change {
                let data = truth.blobs().get(&content).await.unwrap().unwrap();
                blobs.put(data).await.unwrap();
            }
        }
    }
    let mut rng = Rng::new(seed);
    let mut servers = vec![
        IndexServer::new(&blobs, &spy, seed, 0),
        IndexServer::new(&blobs, &spy, seed, 1),
    ];
    let mut next = 2;
    let chained = [HistoryTree::NAME, LogFilterIndex::NAME];
    let compared = [
        HistoryTree::NAME,
        HISTORY_NODE_KIND,
        LogFilterIndex::NAME,
        LOG_FILTER_NAME_KIND,
    ];
    let check = |step: usize| {
        let rows = spy.rows();
        for ((kind, key), row) in &rows {
            assert_eq!(
                truth_rows.get(&(kind.clone(), *key)),
                Some(row),
                "seed {seed}, step {step}: wrong row {kind} {key}"
            );
            if chained.contains(&kind.as_str()) {
                for p in &parents[key] {
                    assert!(
                        rows.contains_key(&(kind.clone(), p.0)),
                        "seed {seed}, step {step}: the {kind} row of {key} without its parent {p}'s"
                    );
                }
            }
        }
        assert!(spy.conflicts.lock().unwrap().is_empty(), "seed {seed}");
        compared.iter().all(|kind| {
            let of = |m: &RowsByKey| {
                m.iter()
                    .filter(|((k, _), _)| k == kind)
                    .map(|((_, h), v)| (*h, v.clone()))
                    .collect::<Vec<_>>()
            };
            of(&rows) == of(truth_rows)
        })
    };
    let mut kills = 0;
    for step in 0..600 {
        let i = rng.below(servers.len());
        match rng.below(10) {
            0..=2 => servers[i].leases.tick().await,
            3..=5 => {
                let _ = servers[i].warmer.pass().await;
            }
            6 => {
                // Both warm at once: whoever holds a group derives it.
                let (a, b) = servers.split_at_mut(1);
                let _ = futures_util::join!(a[0].warmer.pass(), b[0].warmer.pass());
            }
            7 => {
                // Killed at a random point of a pass: the pass is dropped
                // once the store has seen a random number more reads, and
                // the lease is left to go stale.
                let target = servers[i].plan.calls(Op::GetDerived) + 1 + rng.below(400) as u64;
                let plan = servers[i].plan.clone();
                let killer = async move {
                    while plan.calls(Op::GetDerived) < target {
                        tokio::task::yield_now().await;
                    }
                };
                tokio::select! {
                    _ = servers[i].warmer.pass() => {}
                    () = killer => {}
                }
                servers[i].leases.halt();
                servers[i] = IndexServer::new(&blobs, &spy, seed, next);
                next += 1;
                kills += 1;
            }
            8 => {
                // Paused past the stale time: another may take over while
                // this one still trusts its view for a moment.
                tokio::time::advance(Duration::from_secs(7)).await;
            }
            _ => tokio::time::advance(Duration::from_millis(1500)).await,
        }
        if check(step) && step > 40 {
            break;
        }
    }
    // Then a calm finish: faults off, and the leases settle.
    for s in &servers {
        s.plan.clear();
    }
    for round in 0..20 {
        tokio::time::advance(Duration::from_secs(7)).await;
        for s in &mut servers {
            s.leases.tick().await;
            let _ = s.warmer.pass().await;
        }
        if check(10_000 + round) {
            let epoch = spy
                .lease(
                    &leep_core::LeaseKey::index(HistoryTree::NAME),
                    leep_core::LeaseTiming {
                        ttl: Duration::from_secs(15),
                        stale_after: Duration::from_secs(6),
                    },
                )
                .await
                .unwrap()
                .map_or(0, |l| l.epoch);
            eprintln!("seed {seed}: {kills} kills, history lease epoch {epoch}");
            return;
        }
    }
    panic!("seed {seed}: the indexes never completed");
}

type RowsByKey = BTreeMap<(String, leep_core::Hash), Bytes>;

#[tokio::test(start_paused = true)]
async fn index_holders_killed_paused_and_racing_store_the_fault_free_rows() {
    use crate::derived::{derive, HistoryTree, LogFilterIndex};
    let blobs = Arc::new(MemoryBlobStore::new());
    let truth_meta = Tamper::new(Arc::new(MemoryMetaStore::new()));
    let truth = Repo::new(blobs.clone(), truth_meta.clone()).with_derive_jobs(1);
    let ids = lanes_history(&truth, 160).await;
    let tip = *ids.last().unwrap();
    // Heads: the octopus and a few older lane tips, so passes have several.
    let heads = vec![tip, ids[40], ids[97], ids[133]];
    for h in &heads {
        derive::<HistoryTree>(&truth, *h).await.unwrap();
        derive::<LogFilterIndex>(&truth, *h).await.unwrap();
    }
    let truth_rows = truth_meta.rows();
    let mut parents: HashMap<leep_core::Hash, Vec<ChangesetId>> = HashMap::new();
    for id in &ids {
        parents.insert(id.0, truth.get_changeset(*id).await.unwrap().parents);
    }
    for seed in seeds().step_by(5) {
        index_holders_under_faults(seed, &truth, &truth_rows, &parents, &heads).await;
    }
}

/// ADR 0058 section 6: a tool that drops an index takes its lease and waits
/// the fence before deleting, so a write a paused former holder still had
/// in flight lands before the drop, not after it. Without the fence the
/// late write leaves rows whose parents' rows the drop removed.
#[tokio::test(start_paused = true)]
async fn a_fence_holds_a_drop_back_until_a_paused_holder_write_lands() {
    use crate::derived::{derive, DerivedKind, HistoryTree, HISTORY_NODE_KIND};
    use crate::indexing::{IndexGroup, IndexLeaseConfig, IndexLeases, ToolLease};
    for fence in [Duration::from_secs(5), Duration::ZERO] {
        let blobs = Arc::new(MemoryBlobStore::new());
        let meta = Arc::new(clocked_meta());
        let clean = Repo::new(blobs.clone(), meta.clone());
        let ids = lanes_history(&clean, 200).await;
        let tip = *ids.last().unwrap();
        // The lower part is indexed already; the holder derives the rest.
        derive::<HistoryTree>(&clean, ids[100]).await.unwrap();
        meta.cas_ref("main", None, Some(tip)).await.unwrap();
        // The holder's writes reach the store 8 s late.
        let plan = FaultPlan::new(1);
        plan.script(Op::PutDerived, 1, Fault::Delay(Duration::from_secs(8)));
        let faulty: Arc<dyn MetaStore> = Arc::new(FaultyMetaStore::new(meta.clone(), plan.clone()));
        let holder = Repo::new(blobs.clone(), faulty.clone());
        let leases = IndexLeases::new(
            faulty,
            IndexLeaseConfig::new("holder", "").with_candidates(vec![IndexGroup::History]),
        );
        leases.tick().await;
        assert!(leases.holds(IndexGroup::History));
        let mut warmer = crate::warm::Warmer::new(holder, false).with_leases(leases.clone());
        let warming = tokio::spawn(async move {
            let _ = warmer.pass().await;
        });
        // The holder reaches its delayed write, then pauses (no renewals);
        // once its lease is stale the tool takes it, waits the fence, and
        // drops the kind.
        while plan.calls(Op::PutDerived) == 0 {
            tokio::task::yield_now().await;
        }
        tokio::time::advance(Duration::from_secs(7)).await;
        let say = |_: &str| {};
        let tool = ToolLease::take(
            meta.clone(),
            "repair-drop",
            &[IndexGroup::History],
            false,
            fence,
            &say,
        )
        .await
        .unwrap();
        for k in [HistoryTree::NAME, HISTORY_NODE_KIND] {
            meta.delete_derived_kind(k).await.unwrap();
        }
        warming.await.unwrap();
        let left = meta
            .list_derived(HistoryTree::NAME, None, usize::MAX)
            .await
            .unwrap();
        let mut orphans = 0;
        for (key, _) in &left {
            for p in clean
                .get_changeset(ChangesetId(*key))
                .await
                .unwrap()
                .parents
            {
                if meta
                    .get_derived(HistoryTree::NAME, &p.0)
                    .await
                    .unwrap()
                    .is_none()
                {
                    orphans += 1;
                }
            }
        }
        tool.release().await;
        if fence.is_zero() {
            assert!(
                orphans > 0,
                "without a fence the late write leaves orphaned rows"
            );
        } else {
            assert_eq!(left.len(), 0, "the late write landed before the drop");
        }
    }
}

// ---------------------------------------------------------------------------
// Changeset garbage collection, the writers' side (ADR 0059, phase 1)

/// Faults on every call for the servers of the changeset GC tests: failures
/// before and after writes apply, and failed and slow reads.
fn rescue_faults(plan: &FaultPlan) {
    plan.jitter(2)
        .rate(Op::WRITES, 60, Fault::FailAfter)
        .rate(Op::WRITES, 60, Fault::Fail)
        .rate(Op::META, 40, Fault::Fail)
        .rate(Op::BLOB, 20, Fault::Fail)
        .rate(Op::META, 20, Fault::Delay(Duration::from_millis(5)));
}

/// `ids` are all rescued (not condemned any more) and readable.
async fn assert_rescued(seed: u64, w: &World, ids: &[ChangesetId]) {
    for id in ids {
        assert!(
            !crate::changeset_gc::is_condemned_for_test(&w.clean, *id).await,
            "seed {seed}: {id} is still condemned"
        );
        w.clean.get_changeset(*id).await.unwrap();
    }
}

/// A chain `base <- a <- b` with `main` at `base`, and `a` and `b`
/// condemned, as a sweep finds an abandoned stack.
async fn condemned_stack(seed: u64, w: &World) -> (ChangesetId, ChangesetId, ChangesetId) {
    let base = commit_file(seed, &w.clean, None, "base.txt", "base\n", "base").await;
    w.meta.cas_ref("main", None, Some(base)).await.unwrap();
    let a = commit_file(seed, &w.clean, Some(base), "s/a.txt", "a\n", "a").await;
    let b = commit_file(seed, &w.clean, Some(a), "s/b.txt", "b\n", "b").await;
    crate::changeset_gc::condemn_for_test(&w.clean, &[a, b], 1_000).await;
    (base, a, b)
}

/// A changeset created on a condemned parent rescues the parent and its
/// condemned ancestors before it reads them, and only those; with a delete
/// of the parent announced, the create waits and then answers 409, and goes
/// through once collect abandons the delete.
#[tokio::test(start_paused = true)]
async fn create_on_condemned_parent_rescues_its_ancestry() {
    use crate::gc::Doom;
    for seed in seeds() {
        let w = World::new(seed);
        let (base, a, b) = condemned_stack(seed, &w).await;
        let unrelated = commit_file(seed, &w.clean, Some(base), "u.txt", "u\n", "u").await;
        crate::changeset_gc::condemn_for_test(&w.clean, &[unrelated], 1_000).await;
        rescue_faults(&w.plan);
        let server = w.server();
        let child = commit_file(seed, &server, Some(b), "s/c.txt", "c\n", "c").await;
        w.plan.pause();
        assert_rescued(seed, &w, &[a, b, child]).await;
        assert!(
            crate::changeset_gc::is_condemned_for_test(&w.clean, unrelated).await,
            "seed {seed}: a changeset off the child's ancestry was rescued"
        );
        for id in [a, b] {
            let row = w
                .meta
                .get_derived(crate::changeset_gc::CS_RESCUE, &id.0)
                .await
                .unwrap();
            assert_eq!(
                row.as_deref(),
                Some(&1_000i64.to_be_bytes()[..]),
                "seed {seed}: the rescue names the condemnation it answers"
            );
        }
    }

    let w = World::new(1);
    let (_, _, b) = condemned_stack(1, &w).await;
    crate::changeset_gc::doom_for_test(&w.clean, b, Doom::Deleting).await;
    let server = w.server();
    let data = Bytes::from_static(b"c\n");
    let content = server.put_blob(data.clone()).await.unwrap();
    let cs = Changeset {
        parents: vec![b],
        author: "agent".into(),
        timestamp: 7,
        message: "c".into(),
        changes: BTreeMap::from([(
            rp("s/c.txt"),
            FileChange::Modify {
                content,
                kind: FileKind::Regular,
                size: 2,
            },
        )]),
        extra: BTreeMap::new(),
    };
    let started = tokio::time::Instant::now();
    let err = server.create_changeset(cs.clone()).await.unwrap_err();
    assert!(matches!(err, Error::Conflict(_)), "{err}");
    assert!(started.elapsed() >= Duration::from_secs(30), "waited first");
    crate::changeset_gc::doom_for_test(&w.clean, b, Doom::Abandoned).await;
    let id = server.create_changeset(cs).await.unwrap();
    assert_rescued(1, &w, &[b, id]).await;
}

/// Landing a condemned changeset, rebased or fast-forwarded, rescues it
/// and its whole unlanded stack before the ref moves.
#[tokio::test(start_paused = true)]
async fn land_of_condemned_original_is_rescued() {
    for seed in seeds() {
        for rebased in [false, true] {
            let w = World::new(seed);
            let (base, a, b) = condemned_stack(seed, &w).await;
            if rebased {
                let other = commit_file(seed, &w.clean, Some(base), "o.txt", "o\n", "o").await;
                w.meta
                    .cas_ref("main", Some(base), Some(other))
                    .await
                    .unwrap();
            }
            rescue_faults(&w.plan);
            let server = w.server();
            let id = LandId::from_bytes(&[seed as u8; 16]).unwrap();
            let landed = retry(seed, "land", || {
                server.land_keyed(b, "main", None, true, Some(id.clone()))
            })
            .await;
            w.plan.pause();
            let LandResponse::Landed {
                landed, rebased: r, ..
            } = landed
            else {
                panic!("seed {seed}: {landed:?}");
            };
            assert_eq!(r, rebased, "seed {seed}");
            assert_eq!(w.meta.get_ref("main").await.unwrap(), Some(landed));
            assert_rescued(seed, &w, &[a, b]).await;
            let ids = assert_ref_sound(seed, &w.clean, "main").await;
            assert_eq!(ids.len(), if rebased { 4 } else { 3 }, "seed {seed}");
            assert_ref_log_replays(seed, &*w.meta).await;
        }
    }
}

/// A keyed land retried after its original was reclaimed (ADR 0059 section
/// 5) is answered from its record and moves nothing; an id that never
/// landed, or a retry without an id, finds nothing to land (404), and an id
/// recorded for another changeset is refused (400).
#[tokio::test(start_paused = true)]
async fn keyed_retry_of_reclaimed_original_is_answered_from_its_record() {
    for seed in seeds() {
        let w = World::new(seed);
        let clean = &w.clean;
        let base = commit_file(seed, clean, None, "base.txt", "base\n", "base").await;
        w.meta.cas_ref("main", None, Some(base)).await.unwrap();
        let feature = commit_file(seed, clean, Some(base), "f.txt", "f\n", "feature").await;
        let other = commit_file(seed, clean, Some(base), "o.txt", "o\n", "other").await;
        w.meta
            .cas_ref("main", Some(base), Some(other))
            .await
            .unwrap();
        let id = LandId::from_bytes(&[seed as u8; 16]).unwrap();
        let first = clean
            .land_keyed(feature, "main", None, true, Some(id.clone()))
            .await
            .unwrap();
        let LandResponse::Landed { landed: copy, .. } = first else {
            panic!("{first:?}");
        };
        assert_ne!(copy, feature, "rebased");
        land_others(&w, 3).await;
        // A sweep reclaims the original once the copy is past its scan
        // window; here it is simply gone.
        assert!(w.meta.remove_changeset(&feature));
        let head = w.meta.get_ref("main").await.unwrap();
        let entries = w.meta.ref_log(None, None, usize::MAX).await.unwrap().len();

        rescue_faults(&w.plan);
        let server = w.server();
        let again = retry(seed, "retry", || {
            server.land_keyed(feature, "main", None, true, Some(id.clone()))
        })
        .await;
        assert_eq!(
            again,
            LandResponse::Landed {
                landed: copy,
                rebased: true,
                attempts: 1
            },
            "seed {seed}"
        );
        let refused = |what: &str, r: Result<LandResponse>, want: fn(&Error) -> bool| match r {
            Err(e) if want(&e) => {}
            other => panic!("seed {seed}: {what}: {other:?}"),
        };
        // 20 bytes: no seed's 16-byte id.
        let unknown = LandId::from_bytes(&[0xEE; 20]).unwrap();
        let not_found = |e: &Error| matches!(e, Error::NotFound(_));
        let reused = |e: &Error| matches!(e, Error::LandIdReused { .. });
        loop {
            match server
                .land_keyed(feature, "main", None, true, Some(unknown.clone()))
                .await
            {
                Err(e) if is_injected(&e) => continue,
                r => {
                    refused("an id never recorded", r, not_found);
                    break;
                }
            }
        }
        loop {
            match server.land(feature, "main").await {
                Err(e) if is_injected(&e) => continue,
                r => {
                    refused("no id", r, not_found);
                    break;
                }
            }
        }
        // The same id, asked for a changeset that is gone too.
        let gone = commit_file(seed, clean, Some(base), "g.txt", "g\n", "gone").await;
        assert!(w.meta.remove_changeset(&gone));
        loop {
            match server
                .land_keyed(gone, "main", None, true, Some(id.clone()))
                .await
            {
                Err(e) if is_injected(&e) => continue,
                r => {
                    refused("the id of another land", r, reused);
                    break;
                }
            }
        }
        w.plan.pause();
        assert_eq!(w.meta.get_ref("main").await.unwrap(), head, "seed {seed}");
        assert_eq!(
            w.meta.ref_log(None, None, usize::MAX).await.unwrap().len(),
            entries,
            "seed {seed}: nothing moved"
        );
    }
}

/// Every way of moving a ref to a condemned changeset rescues it first:
/// creating a ref at it, and moving one back to it as `leep undo` does.
#[tokio::test(start_paused = true)]
async fn ref_move_to_condemned_changeset_rescues() {
    for seed in seeds() {
        let w = World::new(seed);
        let (base, a, b) = condemned_stack(seed, &w).await;
        rescue_faults(&w.plan);
        let server = w.server();
        let ctx = leep_core::RefUpdateContext::new("cas");
        let moved = retry(seed, "create ref", || {
            server.cas_ref_checked("feature", None, Some(b), &ctx)
        })
        .await;
        // A retry after an applied-then-failed swap loses to its own move.
        assert!(moved || w.meta.get_ref("feature").await.unwrap() == Some(b));
        w.plan.pause();
        assert_rescued(seed, &w, &[a, b]).await;

        // Undo: `main` moved off `base`, which a sweep then condemned (it
        // stays a log root in the real sweep; the rescue is what matters).
        w.plan.clear();
        let next = commit_file(seed, &w.clean, Some(base), "n.txt", "n\n", "n").await;
        w.meta
            .cas_ref("main", Some(base), Some(next))
            .await
            .unwrap();
        crate::changeset_gc::condemn_for_test(&w.clean, &[base], 2_000).await;
        rescue_faults(&w.plan);
        w.plan.resume();
        let server = w.server();
        let undo = leep_core::RefUpdateContext::new("undo");
        let (meta, srv, undo) = (&w.meta, &server, &undo);
        retry(seed, "undo", move || async move {
            match meta.get_ref("main").await? {
                Some(h) if h == base => Ok(true),
                cur => srv.cas_ref_checked("main", cur, Some(base), undo).await,
            }
        })
        .await;
        w.plan.pause();
        assert_rescued(seed, &w, &[base]).await;
        assert_ref_log_replays(seed, &*w.meta).await;
    }
}

/// The other writers that name an existing changeset rescue it, and its
/// condemned ancestors, before they store anything: a merge of it, a rebase
/// of it, and a signature or review of it.
#[tokio::test(start_paused = true)]
async fn other_writers_naming_a_condemned_changeset_rescue_it() {
    use leep_core::api::{Authorship, MergeRequest, MergeResponse, RebaseResponse};
    use leep_core::review::{Review, Verdict};
    use leep_core::signature::{key_id, Signature};

    #[derive(Clone, Copy, Debug)]
    enum Hook {
        Merge,
        Rebase,
        Sign,
        Review,
    }
    for seed in seeds() {
        for hook in [Hook::Merge, Hook::Rebase, Hook::Sign, Hook::Review] {
            let w = World::new(seed);
            let (base, a, b) = condemned_stack(seed, &w).await;
            let other = commit_file(seed, &w.clean, Some(base), "o.txt", "o\n", "o").await;
            rescue_faults(&w.plan);
            let server = w.server();
            let what = format!("{hook:?}");
            match hook {
                Hook::Merge => {
                    let r = retry(seed, &what, || {
                        server.merge(MergeRequest {
                            parents: [b, other],
                            message: "merge".into(),
                            authorship: Authorship {
                                author: "m".into(),
                                timestamp: Some(5),
                            },
                            extra: BTreeMap::new(),
                        })
                    })
                    .await;
                    assert!(
                        matches!(r, MergeResponse::Merged { .. }),
                        "seed {seed}: {r:?}"
                    );
                }
                Hook::Rebase => {
                    let r = retry(seed, &what, || server.rebase(b, other)).await;
                    assert!(
                        matches!(r, RebaseResponse::Rebased { .. }),
                        "seed {seed}: {r:?}"
                    );
                }
                Hook::Sign => {
                    let key = [9; 32];
                    let sig = Signature {
                        key_id: key_id(&key),
                        public_key: key,
                        sig: [1; 64],
                    };
                    retry(seed, &what, || server.put_signature(b, &sig)).await;
                }
                Hook::Review => {
                    let review = Review {
                        reviewer: "alice".into(),
                        verdict: Verdict::Approve,
                        comment: String::new(),
                        timestamp: 1,
                    };
                    retry(seed, &what, || server.put_review(b, &review)).await;
                }
            }
            w.plan.pause();
            assert_rescued(seed, &w, &[a, b]).await;
        }
    }
}

/// The client's request is cancelled after its batch's first attempt, a
/// sweep condemns the land's stack, and the batch commits on a later
/// attempt: the attempt's own fence read, not the request's, rescues it.
#[tokio::test(start_paused = true)]
async fn cancelled_land_whose_batch_commits_later_is_fenced() {
    let w = World::new(5);
    let clean = &w.clean;
    let base = commit_file(5, clean, None, "base.txt", "base\n", "base").await;
    w.meta.cas_ref("main", None, Some(base)).await.unwrap();
    let a = commit_file(5, clean, Some(base), "s/a.txt", "a\n", "a").await;
    let b = commit_file(5, clean, Some(a), "s/b.txt", "b\n", "b").await;
    let server = w.server();
    // The request reads the flag (down) and caches it, as a request does.
    assert!(!server.changeset_fence_active(false).await.unwrap());
    // The first attempt's swap is held, and loses to another writer.
    let calls = w.plan.calls(Op::CasRef);
    w.plan
        .script(Op::CasRef, calls + 1, Fault::Delay(Duration::from_secs(5)));
    let land = {
        let server = server.clone();
        tokio::spawn(async move {
            tokio::time::timeout(Duration::from_secs(1), server.land(b, "main")).await
        })
    };
    while w.plan.calls(Op::CasRef) == calls {
        tokio::task::yield_now().await;
    }
    assert!(land.await.unwrap().is_err(), "the client gave up");
    let other = commit_file(5, clean, Some(base), "o.txt", "o\n", "other").await;
    w.meta
        .cas_ref("main", Some(base), Some(other))
        .await
        .unwrap();
    crate::changeset_gc::condemn_for_test(clean, &[a, b], 3_000).await;
    // The batch goes on: its next attempt rescues, then lands.
    let landed = loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let head = w.meta.get_ref("main").await.unwrap().unwrap();
        if head != other {
            break head;
        }
    };
    assert_eq!(
        clean.rebased_origin(landed).await.unwrap(),
        Some(b),
        "the batch landed the cancelled land"
    );
    assert_rescued(5, &w, &[a, b]).await;
    assert_eq!(assert_ref_sound(5, clean, "main").await.len(), 4);
    assert_ref_log_replays(5, &*w.meta).await;
}

/// A batch still trying past `MAX_BATCH_LIFETIME` answers its lands with a
/// retryable conflict and moves nothing; the keyed resend lands once.
#[tokio::test(start_paused = true)]
async fn batch_past_its_deadline_answers_retryably() {
    let w = World::new(6);
    let clean = &w.clean;
    let base = commit_file(6, clean, None, "base.txt", "base\n", "base").await;
    w.meta.cas_ref("main", None, Some(base)).await.unwrap();
    let feature = commit_file(6, clean, Some(base), "f.txt", "f\n", "feature").await;
    let server = w.server();
    let id = LandId::from_bytes(&[6; 16]).unwrap();
    // The first attempt's swap is held past the lifetime and loses.
    let calls = w.plan.calls(Op::CasRef);
    w.plan.script(
        Op::CasRef,
        calls + 1,
        Fault::Delay(crate::land::MAX_BATCH_LIFETIME + Duration::from_secs(1)),
    );
    let land = {
        let (server, id) = (server.clone(), id.clone());
        tokio::spawn(async move {
            server
                .land_keyed(feature, "main", None, true, Some(id))
                .await
        })
    };
    while w.plan.calls(Op::CasRef) == calls {
        tokio::task::yield_now().await;
    }
    let other = commit_file(6, clean, Some(base), "o.txt", "o\n", "other").await;
    w.meta
        .cas_ref("main", Some(base), Some(other))
        .await
        .unwrap();
    let err = land.await.unwrap().unwrap_err();
    assert!(matches!(err, Error::Conflict(_)), "{err}");
    assert!(err.to_string().contains("retry"), "{err}");
    assert_eq!(w.meta.get_ref("main").await.unwrap(), Some(other));
    // The resend, under the same id, lands once.
    let again = server
        .land_keyed(feature, "main", None, true, Some(id.clone()))
        .await
        .unwrap();
    let LandResponse::Landed { landed, .. } = again else {
        panic!("{again:?}");
    };
    let resent = server
        .land_keyed(feature, "main", None, true, Some(id))
        .await
        .unwrap();
    assert!(matches!(resent, LandResponse::Landed { landed: l, .. } if l == landed));
    assert_eq!(assert_ref_sound(6, clean, "main").await.len(), 3);
    assert_ref_log_replays(6, &*w.meta).await;
}

/// A store whose history keeps changing (commits, keyed and unkeyed lands,
/// branches made and deleted, changesets ageing past grace and young again),
/// marked by runs that fail and are killed at random points: the rows only
/// ever claim closures that are stored, and every completed run, and a full
/// one, finds exactly what reachability from scratch finds.
async fn reach_under_faults(seed: u64) {
    use crate::changeset_reach::{check_reach_rows, mark, unreachable_oracle, ReachOptions};
    let w = World::new(seed);
    let clean = &w.clean;
    let base = commit_file(seed, clean, None, "base.txt", "base\n", "base").await;
    w.meta.cas_ref("main", None, Some(base)).await.unwrap();
    let opts = ReachOptions {
        now: leep_core::store::unix_now() + 30 * 86_400,
        grace: Duration::from_secs(7 * 86_400),
        full: false,
    };
    let plan = FaultPlan::new(seed ^ 59);
    plan.jitter(2)
        .rate(&[Op::PutDerived], 40, Fault::FailAfter)
        .rate(&[Op::PutDerived], 40, Fault::Fail)
        .rate(Op::META, 15, Fault::Fail)
        .rate(&[Op::GetDerived], 5, Fault::Corrupt);
    let gc = w.server_with(&plan);
    let mut rng = Rng::new(seed ^ 0x59);
    let mut tips = vec![base];
    let mut n = 0;
    for round in 0..12 {
        for _ in 0..1 + rng.below(4) {
            n += 1;
            let tip = tips[rng.below(tips.len())];
            match rng.below(7) {
                0..=2 => {
                    let path = format!("r{round}/f{n}.txt");
                    let c =
                        commit_file(seed, clean, Some(tip), &path, "x\n", &format!("c{n}")).await;
                    tips.push(c);
                }
                3 | 4 => {
                    let id = (n % 2 == 0).then(|| LandId::from_bytes(&[n as u8; 16]).unwrap());
                    match clean.land_keyed(tip, "main", None, true, id).await.unwrap() {
                        LandResponse::Landed { landed, .. } => tips.push(landed),
                        other => panic!("seed {seed}: {other:?}"),
                    }
                }
                5 => {
                    let name = format!("b{}", rng.below(3));
                    let cur = w.meta.get_ref(&name).await.unwrap();
                    let new = (cur.is_none() || rng.below(2) == 0).then_some(tip);
                    w.meta.cas_ref(&name, cur, new).await.unwrap();
                }
                _ => {
                    let at = if rng.below(2) == 0 {
                        opts.now as u64
                    } else {
                        0
                    };
                    w.meta.set_stored_at(&tip, at);
                }
            }
        }
        let kill_after = if rng.below(3) == 0 {
            usize::MAX
        } else {
            rng.below(60)
        };
        // Dying mid-run is the point; the result is not.
        let _ = leep_testkit::cancel_after_polls(mark(&gc, &opts), kill_after).await;
        if let Err(e) = check_reach_rows(clean).await {
            panic!("seed {seed} round {round}: {e}");
        }
        if rng.below(2) == 0 {
            let run = mark(clean, &opts).await.unwrap();
            assert_eq!(
                run.unreachable,
                unreachable_oracle(clean, &opts).await,
                "seed {seed} round {round}"
            );
        }
    }
    let want = unreachable_oracle(clean, &opts).await;
    assert!(
        !want.is_empty() || tips.len() < 4,
        "seed {seed}: the history left nothing unreachable to check against"
    );
    let run = mark(clean, &opts).await.unwrap();
    assert_eq!(run.unreachable, want, "seed {seed}: incremental");
    let full = mark(
        clean,
        &ReachOptions {
            full: true,
            ..opts.clone()
        },
    )
    .await
    .unwrap();
    assert_eq!(full.unreachable, want, "seed {seed}: full");
    check_reach_rows(clean).await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn incremental_reach_rows_agree_with_full_reachability() {
    for seed in seeds() {
        reach_under_faults(seed).await;
    }
}

// ---- search rows: a stored filter always has its subtree stored ----

/// Lets the first `n` lease checks of a derivation pass.
struct Countdown(std::sync::atomic::AtomicUsize);

#[async_trait::async_trait]
impl crate::repo::WriteGuard for Countdown {
    async fn allowed(&self) -> bool {
        use std::sync::atomic::Ordering::SeqCst;
        self.0
            .fetch_update(SeqCst, SeqCst, |n| n.checked_sub(1))
            .is_ok()
    }
}

/// Search derivation stopped anywhere, by a `put_derived_many` that fails
/// after storing any prefix of its rows or by the index lease lost before
/// any write of any level, leaves every stored filter with its whole
/// subtree stored: `stale_target` and the bounded queries' checks never
/// trust a filter over missing rows. A later derivation completes the rows,
/// byte for byte those `recompute_tree_row` gives.
#[tokio::test]
async fn search_rows_stay_closed_when_derivation_stops_anywhere() {
    use crate::search::tests::{
        assert_closed, assert_rows_recomputed, commit_files, index_some, level_tree, probed,
    };
    use std::sync::atomic::Ordering::SeqCst;
    let (mut failed, mut lost) = (0, 0);
    for seed in seeds() {
        let mut rng = Rng::new(seed);
        let files = level_tree(seed, 20 + rng.below(80));
        let partial = rng.below(2) == 0;
        // An uninterrupted derivation, for its number of writes.
        let (r, probe) = probed();
        let c = commit_files(&r, &[], &files).await;
        let root = r.root_tree(c).await.unwrap();
        if partial {
            index_some(&r, seed, root).await;
        }
        let before = probe.puts.load(SeqCst);
        r.derive_search(c).await.unwrap();
        let writes = probe.puts.load(SeqCst) - before;
        if writes == 0 {
            continue;
        }
        let what = format!("seed {seed}");

        // A write fails, after storing a prefix of its rows.
        let (r, probe) = probed();
        commit_files(&r, &[], &files).await;
        if partial {
            index_some(&r, seed, root).await;
        }
        let k = 1 + rng.below(writes as usize) as u64;
        let prefix = rng.below(64);
        *probe.fail.lock().unwrap() = Some((probe.puts.load(SeqCst) + k, prefix));
        let err = r.derive_search(c).await.unwrap_err();
        assert!(matches!(err, Error::Storage(_)), "{what}: {err}");
        *probe.fail.lock().unwrap() = None;
        assert_closed(&r, root, &format!("{what}, write {k} of {writes} failed")).await;
        failed += 1;
        r.derive_search(c).await.unwrap();
        assert_closed(&r, root, &what).await;
        assert_rows_recomputed(&r, root, &what).await;

        // The lease is lost after `n` writes.
        let (r, _) = probed();
        commit_files(&r, &[], &files).await;
        if partial {
            index_some(&r, seed, root).await;
        }
        let n = rng.below(writes as usize);
        let guarded = r.with_write_guard(Arc::new(Countdown(n.into())));
        let err = guarded.derive_search(c).await.unwrap_err();
        assert!(err.to_string().contains("lease was lost"), "{what}: {err}");
        assert_closed(&r, root, &format!("{what}, lease lost after {n} writes")).await;
        lost += 1;
        r.derive_search(c).await.unwrap();
        assert_closed(&r, root, &what).await;
        assert_rows_recomputed(&r, root, &what).await;
    }
    assert!(
        failed > 0 && lost > 0,
        "{failed} failed writes, {lost} lost leases"
    );
}
