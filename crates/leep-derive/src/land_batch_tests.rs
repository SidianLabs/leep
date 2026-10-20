//! Group commit of lands (ADR 0032): every land in a batch gets the answer it
//! would get if the lands ran one after another, and the ref log reads as if
//! they did.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use leep_core::api::LandResponse;
use leep_core::memory::{MemoryBlobStore, MemoryMetaStore};
use leep_core::{
    Changeset, ChangesetId, FileChange, FileKind, Hash, MetaStore, RefLogEntry, RefStep,
    RefUpdateContext, RepoPath, Result,
};
use tokio::sync::watch;

use crate::Repo;

/// A memory store whose ref reads and swaps can be held shut, which lets a
/// test queue lands in a known order, and which can let another writer move
/// the ref just before the next swap.
struct Gate {
    inner: MemoryMetaStore,
    reads: watch::Sender<bool>,
    swaps: watch::Sender<bool>,
    waiting_reads: AtomicUsize,
    waiting_swaps: AtomicUsize,
    swap_calls: AtomicUsize,
    chain_calls: AtomicUsize,
    /// `put_changesets_with_derived` calls: one per batch that stored replays.
    batch_writes: AtomicUsize,
    /// `put_changeset_with_derived` calls.
    single_writes: AtomicUsize,
    /// Moved in by "another server" right before the next swap.
    intruder: Mutex<Option<(Option<ChangesetId>, ChangesetId)>>,
}

impl Gate {
    fn new() -> Arc<Gate> {
        Arc::new(Gate {
            inner: MemoryMetaStore::new(),
            reads: watch::channel(true).0,
            swaps: watch::channel(true).0,
            waiting_reads: AtomicUsize::new(0),
            waiting_swaps: AtomicUsize::new(0),
            swap_calls: AtomicUsize::new(0),
            chain_calls: AtomicUsize::new(0),
            batch_writes: AtomicUsize::new(0),
            single_writes: AtomicUsize::new(0),
            intruder: Mutex::new(None),
        })
    }

    async fn pass(open: &watch::Sender<bool>, waiting: &AtomicUsize) {
        let mut rx = open.subscribe();
        if *rx.borrow() {
            return;
        }
        waiting.fetch_add(1, Ordering::SeqCst);
        while !*rx.borrow_and_update() {
            rx.changed().await.unwrap();
        }
        waiting.fetch_sub(1, Ordering::SeqCst);
    }

    async fn before_swap(&self, name: &str) {
        Self::pass(&self.swaps, &self.waiting_swaps).await;
        let intruder = self.intruder.lock().unwrap().take();
        if let Some((from, to)) = intruder {
            let ctx = RefUpdateContext::new("cas").message("another server");
            assert!(self
                .inner
                .cas_ref_logged(name, from, Some(to), &ctx)
                .await
                .unwrap());
        }
    }
}

#[async_trait]
impl MetaStore for Gate {
    async fn put_changeset(&self, cs: &Changeset) -> Result<ChangesetId> {
        self.inner.put_changeset(cs).await
    }
    async fn put_changeset_with_derived(
        &self,
        cs: &Changeset,
        rows: &[leep_core::DerivedRow],
    ) -> Result<ChangesetId> {
        self.single_writes.fetch_add(1, Ordering::SeqCst);
        self.inner.put_changeset_with_derived(cs, rows).await
    }
    async fn put_changesets_with_derived(
        &self,
        changesets: &[Changeset],
        rows: &[leep_core::DerivedRow],
    ) -> Result<()> {
        self.batch_writes.fetch_add(1, Ordering::SeqCst);
        self.inner
            .put_changesets_with_derived(changesets, rows)
            .await
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
        Self::pass(&self.reads, &self.waiting_reads).await;
        self.inner.get_ref(name).await
    }
    async fn cas_ref_logged(
        &self,
        name: &str,
        expected: Option<ChangesetId>,
        new: Option<ChangesetId>,
        ctx: &RefUpdateContext,
    ) -> Result<bool> {
        self.before_swap(name).await;
        self.swap_calls.fetch_add(1, Ordering::SeqCst);
        self.inner.cas_ref_logged(name, expected, new, ctx).await
    }
    async fn cas_ref_logged_chain(
        &self,
        name: &str,
        expected: Option<ChangesetId>,
        steps: &[RefStep],
    ) -> Result<usize> {
        self.before_swap(name).await;
        self.chain_calls.fetch_add(1, Ordering::SeqCst);
        self.inner.cas_ref_logged_chain(name, expected, steps).await
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
    ) -> Result<Vec<RefLogEntry>> {
        self.inner.ref_log(name, before_seq, limit).await
    }
    async fn list_refs(&self) -> Result<BTreeMap<String, ChangesetId>> {
        self.inner.list_refs().await
    }
    async fn get_derived(&self, kind: &str, key: &Hash) -> Result<Option<Bytes>> {
        self.inner.get_derived(kind, key).await
    }
    async fn put_derived(&self, kind: &str, key: &Hash, value: Bytes) -> Result<()> {
        self.inner.put_derived(kind, key, value).await
    }
}

fn plain_repo() -> Repo {
    Repo::new(
        Arc::new(MemoryBlobStore::new()),
        Arc::new(MemoryMetaStore::new()),
    )
}

fn gated_repo() -> (Repo, Arc<Gate>) {
    let gate = Gate::new();
    let r = Repo::new(Arc::new(MemoryBlobStore::new()), gate.clone());
    (r, gate)
}

/// One changeset to create: parents by index into the script (or the base),
/// and the files it writes (`None` deletes).
#[derive(Clone)]
struct Spec {
    parents: Vec<Parent>,
    files: Vec<(String, Option<String>)>,
    msg: String,
}

#[derive(Clone, Copy)]
enum Parent {
    Base,
    Script(usize),
}

/// Create the base (landed on `main`) and every spec, in order. The same
/// script gives the same ids in any repository.
async fn build(r: &Repo, script: &[Spec]) -> (ChangesetId, Vec<ChangesetId>) {
    let base = make(
        r,
        &[],
        &[("base.txt".into(), Some("base\n".into()))],
        "base",
    )
    .await;
    r.meta().cas_ref("main", None, Some(base)).await.unwrap();
    let mut ids: Vec<ChangesetId> = Vec::new();
    for s in script {
        let parents: Vec<_> = s
            .parents
            .iter()
            .map(|p| match p {
                Parent::Base => base,
                Parent::Script(i) => ids[*i],
            })
            .collect();
        ids.push(make(r, &parents, &s.files, &s.msg).await);
    }
    (base, ids)
}

async fn make(
    r: &Repo,
    parents: &[ChangesetId],
    files: &[(String, Option<String>)],
    msg: &str,
) -> ChangesetId {
    let mut changes = BTreeMap::new();
    for (path, body) in files {
        let change = match body {
            Some(b) => {
                let h = r
                    .put_blob(Bytes::from(b.clone().into_bytes()))
                    .await
                    .unwrap();
                FileChange::Modify {
                    content: h,
                    kind: FileKind::Regular,
                    size: b.len() as u64,
                }
            }
            None => FileChange::Delete,
        };
        changes.insert(RepoPath::new(path).unwrap(), change);
    }
    r.create_changeset(Changeset {
        parents: parents.to_vec(),
        author: "t".into(),
        timestamp: 1_700_000_000,
        message: msg.into(),
        changes,
        extra: BTreeMap::new(),
    })
    .await
    .unwrap()
}

fn spec(parents: &[Parent], files: &[(&str, Option<&str>)], msg: &str) -> Spec {
    Spec {
        parents: parents.to_vec(),
        files: files
            .iter()
            .map(|(p, b)| (p.to_string(), b.map(str::to_string)))
            .collect(),
        msg: msg.into(),
    }
}

/// A land's answer in comparable form.
fn outcome(r: &Result<LandResponse>) -> String {
    match r {
        Ok(resp) => format!("{resp:?}"),
        Err(e) => format!("error: {e}"),
    }
}

fn log_steps(entries: &[RefLogEntry]) -> Vec<String> {
    entries
        .iter()
        .rev()
        .map(|e| {
            format!(
                "{:?} -> {:?} by {:?}: {} {:?}",
                e.old, e.new, e.actor, e.operation, e.message
            )
        })
        .collect()
}

async fn wait_for(mut done: impl FnMut() -> bool) {
    for _ in 0..10_000 {
        if done() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    panic!("condition never held");
}

type Pending = tokio::task::JoinHandle<Result<LandResponse>>;

/// Queue `lands` on `main` in this order behind a held head read, so they
/// form one batch once `release` opens it. A no-op land of `base` goes first
/// and holds the leader in its own batch.
async fn queue_in_order(
    r: &Repo,
    gate: &Gate,
    base: ChangesetId,
    lands: &[(ChangesetId, Option<&'static str>)],
) -> (Pending, Vec<Pending>) {
    gate.reads.send_replace(false);
    let blocker = {
        let r = r.clone();
        tokio::spawn(async move { r.land(base, "main").await })
    };
    wait_for(|| gate.waiting_reads.load(Ordering::SeqCst) == 1).await;
    let mut tasks = Vec::new();
    for (i, &(cs, actor)) in lands.iter().enumerate() {
        let r2 = r.clone();
        tasks.push(tokio::spawn(
            async move { r2.land_as(cs, "main", actor).await },
        ));
        wait_for(|| r.land_queues.pending("main") == i + 1).await;
    }
    (blocker, tasks)
}

fn release(gate: &Gate) {
    gate.reads.send_replace(true);
}

/// The answers and ref log of landing `order` one at a time on a fresh
/// repository built from the same script.
async fn sequential(
    script: &[Spec],
    order: &[(usize, Option<&'static str>)],
) -> (Vec<String>, Vec<String>, Option<ChangesetId>) {
    let r = plain_repo();
    let (_, ids) = build(&r, script).await;
    let mut answers = Vec::new();
    for &(i, actor) in order {
        answers.push(outcome(&r.land_as(ids[i], "main", actor).await));
    }
    let log = log_steps(&r.meta().ref_log(Some("main"), None, 10_000).await.unwrap());
    (answers, log, r.meta().get_ref("main").await.unwrap())
}

/// A batch mixing every kind of answer: stacked fast-forwards, replays, a
/// stack replayed whole, the same changeset twice and one already landed.
fn mixed_script() -> Vec<Spec> {
    use Parent::*;
    vec![
        spec(&[Base], &[("a/1", Some("1\n"))], "a1"), // 0: fast-forward
        spec(&[Script(0)], &[("a/2", Some("2\n"))], "a2"), // 1: stacks on 0
        spec(&[Base], &[("b/1", Some("b\n"))], "b1"), // 2: replay
        spec(&[Script(2)], &[("b/2", Some("b2\n"))], "b2"), // 3: stack of two
        spec(&[Base], &[("c", Some("c\n"))], "c"),    // 4: replay
        spec(&[Script(1)], &[("a/3", Some("3\n"))], "a3"), // 5: stacks on 1
        spec(&[Base], &[("base.txt", Some("changed\n"))], "bt"), // 6: replay
    ]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_batch_answers_each_land_as_landing_them_in_turn_would() {
    let script = mixed_script();
    let order: Vec<(usize, Option<&'static str>)> = vec![
        (0, Some("w0")),
        (1, Some("w1")),
        (2, None),
        (3, Some("w3")),
        (1, Some("again")), // already in the ref: no move
        (4, Some("w4")),
        (3, Some("dup")), // landed earlier in this batch as a copy
        (5, Some("w5")),
        (6, Some("w6")),
    ];
    let (r, gate) = gated_repo();
    let (base, ids) = build(&r, &script).await;
    let lands: Vec<_> = order.iter().map(|&(i, a)| (ids[i], a)).collect();
    let (blocker, tasks) = queue_in_order(&r, &gate, base, &lands).await;
    release(&gate);
    assert!(matches!(
        blocker.await.unwrap().unwrap(),
        LandResponse::Landed { rebased: false, .. }
    ));
    let mut answers = Vec::new();
    for t in tasks {
        answers.push(outcome(&t.await.unwrap()));
    }
    let (want, want_log, want_head) = sequential(&script, &order).await;
    assert_eq!(answers, want);
    let log = r.meta().ref_log(Some("main"), None, 10_000).await.unwrap();
    assert_eq!(
        log_steps(&log),
        want_log,
        "one entry per land that moved the ref"
    );
    assert_eq!(r.meta().get_ref("main").await.unwrap(), want_head);

    let batches = r.land_queues.batches.lock().unwrap().clone();
    assert_eq!(
        batches,
        vec![vec![base], lands.iter().map(|l| l.0).collect()]
    );
    // The batch moved the ref with one atomic chained swap.
    assert_eq!(gate.chain_calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        gate.swap_calls.load(Ordering::SeqCst),
        1,
        "only the setup's"
    );
    // Fast-forwards stacked: 0 and 1 landed as themselves.
    assert!(answers[0].contains("rebased: false") && answers[1].contains("rebased: false"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_conflicting_or_refused_land_mid_batch_is_answered_alone() {
    use Parent::*;
    let script = vec![
        spec(&[Base], &[("x", Some("1\n"))], "x1"),  // 0
        spec(&[Base], &[("x", Some("2\n"))], "x2"),  // 1: conflicts with 0
        spec(&[Base], &[("y", Some("y\n"))], "y"),   // 2
        spec(&[Base], &[("z", Some("z\n"))], "z"),   // 3: side of a merge
        spec(&[Script(2), Script(3)], &[], "merge"), // 4: refused: a merge
        spec(&[Base], &[("w", Some("w\n"))], "w"),   // 5
    ];
    let order: Vec<(usize, Option<&'static str>)> =
        vec![(0, None), (1, None), (2, None), (4, None), (5, None)];
    let (r, gate) = gated_repo();
    let (base, ids) = build(&r, &script).await;
    let lands: Vec<_> = order.iter().map(|&(i, a)| (ids[i], a)).collect();
    let (blocker, tasks) = queue_in_order(&r, &gate, base, &lands).await;
    release(&gate);
    blocker.await.unwrap().unwrap();
    let mut results = Vec::new();
    for t in tasks {
        results.push(t.await.unwrap());
    }
    let first = match &results[0] {
        Ok(LandResponse::Landed { landed, .. }) => *landed,
        other => panic!("{other:?}"),
    };
    match &results[1] {
        Ok(LandResponse::Conflicts { onto, .. }) => assert_eq!(*onto, first),
        other => panic!("expected conflicts on the first land's result: {other:?}"),
    }
    assert!(matches!(results[2], Ok(LandResponse::Landed { .. })));
    assert!(matches!(results[3], Err(leep_core::Error::Invalid(_))));
    assert!(matches!(results[4], Ok(LandResponse::Landed { .. })));

    let answers: Vec<_> = results.iter().map(outcome).collect();
    let (want, want_log, want_head) = sequential(&script, &order).await;
    assert_eq!(answers, want);
    let log = r.meta().ref_log(Some("main"), None, 10_000).await.unwrap();
    assert_eq!(log_steps(&log), want_log);
    assert_eq!(r.meta().get_ref("main").await.unwrap(), want_head);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_swap_lost_to_another_server_redoes_the_batch_on_its_head() {
    use Parent::*;
    let script = vec![
        spec(&[Base], &[("a", Some("a\n"))], "a"),
        spec(&[Base], &[("b", Some("b\n"))], "b"),
        spec(&[Script(1)], &[("b2", Some("b2\n"))], "b2"),
        spec(&[Base], &[("c", Some("c\n"))], "c"),
        // Written by the other server.
        spec(&[Base], &[("other", Some("o\n"))], "other"),
    ];
    let order: Vec<(usize, Option<&'static str>)> =
        vec![(0, Some("w0")), (2, Some("w2")), (3, Some("w3"))];
    let (r, gate) = gated_repo();
    let (base, ids) = build(&r, &script).await;
    let lands: Vec<_> = order.iter().map(|&(i, a)| (ids[i], a)).collect();
    let (blocker, tasks) = queue_in_order(&r, &gate, base, &lands).await;
    *gate.intruder.lock().unwrap() = Some((Some(base), ids[4]));
    release(&gate);
    blocker.await.unwrap().unwrap();
    let mut answers = Vec::new();
    for t in tasks {
        let resp = t.await.unwrap().unwrap();
        let LandResponse::Landed { attempts, .. } = resp else {
            panic!("{resp:?}");
        };
        assert_eq!(attempts, 2, "the batch was redone once");
        answers.push(outcome(&Ok(resp)).replace("attempts: 2", "attempts: 1"));
    }
    assert_eq!(gate.chain_calls.load(Ordering::SeqCst), 2);

    // The same as landing each in turn after the other server's move.
    let oracle = plain_repo();
    let (_, oids) = build(&oracle, &script).await;
    oracle
        .meta()
        .cas_ref_logged(
            "main",
            Some(base),
            Some(oids[4]),
            &RefUpdateContext::new("cas").message("another server"),
        )
        .await
        .unwrap();
    let mut want = Vec::new();
    for &(i, actor) in &order {
        want.push(outcome(&oracle.land_as(oids[i], "main", actor).await));
    }
    assert_eq!(answers, want);
    let head = r.meta().get_ref("main").await.unwrap();
    assert_eq!(head, oracle.meta().get_ref("main").await.unwrap());
    assert!(r.is_ancestor(ids[4], head.unwrap()).await.unwrap());
    let log = r.meta().ref_log(Some("main"), None, 100).await.unwrap();
    let want_log = oracle
        .meta()
        .ref_log(Some("main"), None, 100)
        .await
        .unwrap();
    assert_eq!(log_steps(&log), log_steps(&want_log));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_land_cancelled_while_queued_is_dropped_from_its_batch() {
    use Parent::*;
    let script = vec![
        spec(&[Base], &[("a", Some("a\n"))], "a"),
        spec(&[Base], &[("b", Some("b\n"))], "b"),
        spec(&[Base], &[("c", Some("c\n"))], "c"),
    ];
    let (r, gate) = gated_repo();
    let (base, ids) = build(&r, &script).await;
    let lands: Vec<_> = ids.iter().map(|&id| (id, None)).collect();
    let (blocker, mut tasks) = queue_in_order(&r, &gate, base, &lands).await;
    let cancelled = tasks.remove(1);
    cancelled.abort();
    assert!(cancelled.await.unwrap_err().is_cancelled());
    release(&gate);
    blocker.await.unwrap().unwrap();
    for t in tasks {
        assert!(matches!(
            t.await.unwrap().unwrap(),
            LandResponse::Landed { .. }
        ));
    }
    let head = r.meta().get_ref("main").await.unwrap().unwrap();
    let files = r.list_files(head, "").await.unwrap();
    let names: Vec<_> = files.iter().map(|f| f.path.to_string()).collect();
    assert_eq!(names, ["a", "base.txt", "c"], "b was never landed");
    let batches = r.land_queues.batches.lock().unwrap().clone();
    assert_eq!(batches[1], vec![ids[0], ids[2]]);
    assert_eq!(
        r.meta()
            .ref_log(Some("main"), None, 100)
            .await
            .unwrap()
            .len(),
        3,
        "base and one entry per landed request"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_land_cancelled_after_its_batch_replayed_it_still_lands() {
    use Parent::*;
    let script = vec![
        spec(&[Base], &[("a", Some("a\n"))], "a"),
        spec(&[Base], &[("b", Some("b\n"))], "b"),
    ];
    let (r, gate) = gated_repo();
    let (base, ids) = build(&r, &script).await;
    gate.swaps.send_replace(false);
    let lands: Vec<_> = ids.iter().map(|&id| (id, None)).collect();
    let (blocker, tasks) = queue_in_order(&r, &gate, base, &lands).await;
    release(&gate);
    blocker.await.unwrap().unwrap();
    // Both are replayed and the batch waits on its swap; both callers leave.
    wait_for(|| gate.waiting_swaps.load(Ordering::SeqCst) == 1).await;
    for t in tasks {
        t.abort();
        assert!(t.await.unwrap_err().is_cancelled());
    }
    gate.swaps.send_replace(true);
    wait_for(|| gate.chain_calls.load(Ordering::SeqCst) == 1).await;
    wait_for(|| r.land_queues.pending("main") == 0).await;
    // A retry of either finds its land done and moves nothing.
    for id in ids {
        let resp = r.land(id, "main").await.unwrap();
        assert!(matches!(resp, LandResponse::Landed { .. }), "{resp:?}");
    }
    let head = r.meta().get_ref("main").await.unwrap().unwrap();
    assert_eq!(r.list_files(head, "").await.unwrap().len(), 3);
    assert_eq!(
        r.meta()
            .ref_log(Some("main"), None, 100)
            .await
            .unwrap()
            .len(),
        3
    );
}

#[tokio::test]
async fn a_lone_land_swaps_with_a_single_logged_cas() {
    let script = vec![spec(&[Parent::Base], &[("a", Some("a\n"))], "a")];
    let (r, gate) = gated_repo();
    let (_, ids) = build(&r, &script).await;
    r.land_as(ids[0], "main", Some("solo")).await.unwrap();
    assert_eq!(
        gate.swap_calls.load(Ordering::SeqCst),
        2,
        "the setup's and the land's"
    );
    assert_eq!(gate.chain_calls.load(Ordering::SeqCst), 0);
    let log = r.meta().ref_log(Some("main"), None, 1).await.unwrap();
    assert_eq!(log[0].actor.as_deref(), Some("solo"));
}

/// Small deterministic generator, so a failing seed can be rerun.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

/// Agents land random stacks at once, with overlapping files so some
/// conflict. Replaying the batches' order one land at a time must give the
/// same answers, ref log and head.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn random_concurrent_lands_match_landing_in_turn() {
    let (mut batched, mut conflicts) = (0, 0);
    for seed in 1..=30u64 {
        let mut rng = Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
        let agents = 2 + rng.below(14);
        let mut script = Vec::new();
        let mut tips = Vec::new();
        for a in 0..agents {
            let depth = 1 + rng.below(3);
            let mut parent = Parent::Base;
            for d in 0..depth {
                let path = if rng.below(4) == 0 {
                    format!("shared/{}", rng.below(3))
                } else {
                    format!("own/{a}/{d}")
                };
                script.push(spec(
                    &[parent],
                    &[(path.as_str(), Some(&format!("{seed} {a} {d}\n")))],
                    &format!("agent {a} step {d}"),
                ));
                parent = Parent::Script(script.len() - 1);
            }
            tips.push(script.len() - 1);
        }
        let r = plain_repo();
        let (_, ids) = build(&r, &script).await;
        let tasks: Vec<_> = tips
            .iter()
            .map(|&i| {
                let r = r.clone();
                let cs = ids[i];
                let yields = rng.below(4);
                tokio::spawn(async move {
                    for _ in 0..yields {
                        tokio::task::yield_now().await;
                    }
                    (cs, outcome(&r.land(cs, "main").await))
                })
            })
            .collect();
        let mut got = BTreeMap::new();
        for t in tasks {
            let (cs, answer) = t.await.unwrap();
            got.insert(cs, answer);
        }
        let batches = r.land_queues.batches.lock().unwrap().clone();
        batched += batches.iter().filter(|b| b.len() > 1).count();
        conflicts += got.values().filter(|a| a.starts_with("Conflicts")).count();
        let order: Vec<ChangesetId> = batches.into_iter().flatten().collect();
        assert_eq!(order.len(), agents, "seed {seed}: every land in one batch");
        let index = |cs: ChangesetId| ids.iter().position(|&i| i == cs).unwrap();
        let seq_order: Vec<_> = order.iter().map(|&cs| (index(cs), None)).collect();
        let (want, want_log, want_head) = sequential(&script, &seq_order).await;
        let answers: Vec<_> = order.iter().map(|cs| got[cs].clone()).collect();
        assert_eq!(answers, want, "seed {seed}");
        let log = r.meta().ref_log(Some("main"), None, 10_000).await.unwrap();
        assert_eq!(log_steps(&log), want_log, "seed {seed}");
        assert_eq!(
            r.meta().get_ref("main").await.unwrap(),
            want_head,
            "seed {seed}"
        );
    }
    assert!(
        batched > 10,
        "lands were batched ({batched} batches of several)"
    );
    assert!(conflicts > 5, "some lands conflicted ({conflicts})");
}

/// The batch replays into memory and stores everything with one metadata
/// write after its blobs (ADR 0043), not one write per replay.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_batch_stores_its_replays_in_one_metadata_write() {
    use Parent::*;
    let script = vec![
        spec(&[Base], &[("a", Some("a\n"))], "a"),
        spec(&[Base], &[("b", Some("b\n"))], "b"),
        spec(&[Script(1)], &[("b2", Some("b2\n"))], "b2"),
        spec(&[Base], &[("c", Some("c\n"))], "c"),
    ];
    let (r, gate) = gated_repo();
    let (base, ids) = build(&r, &script).await;
    let lands: Vec<_> = [0, 2, 3].iter().map(|&i| (ids[i], None)).collect();
    let (blocker, tasks) = queue_in_order(&r, &gate, base, &lands).await;
    let singles = gate.single_writes.load(Ordering::SeqCst);
    release(&gate);
    blocker.await.unwrap().unwrap();
    for t in tasks {
        assert!(matches!(
            t.await.unwrap().unwrap(),
            LandResponse::Landed { .. }
        ));
    }
    assert_eq!(gate.batch_writes.load(Ordering::SeqCst), 1);
    assert_eq!(gate.single_writes.load(Ordering::SeqCst), singles);
    let head = r.meta().get_ref("main").await.unwrap().unwrap();
    assert_stored(&r, head).await;
}

/// Every changeset in `head`'s history is stored with its root tree,
/// generation and skiplist rows, and every tree node under its root tree is
/// in the blob store itself (not only in a cache).
async fn assert_stored(r: &Repo, head: ChangesetId) {
    use crate::derived::{DerivedKind, Generation, RootTree, Skiplist};
    let mut stack = vec![head];
    let mut seen = std::collections::HashSet::new();
    while let Some(id) = stack.pop() {
        if !seen.insert(id) {
            continue;
        }
        let cs = r.meta().get_changeset(&id).await.unwrap().expect("stored");
        for kind in [RootTree::NAME_STR, Generation::NAME, Skiplist::NAME] {
            assert!(
                r.meta().get_derived(kind, &id.0).await.unwrap().is_some(),
                "{kind} row of {id}"
            );
        }
        let mut trees = vec![r.root_tree(id).await.unwrap()];
        while let Some(t) = trees.pop() {
            let bytes = r.blobs().get(&t).await.unwrap().expect("tree stored");
            let node = leep_core::TreeNode::decode(&bytes).unwrap();
            for e in node.entries.values() {
                if let leep_core::TreeEntry::Dir { tree } = e {
                    trees.push(*tree);
                }
            }
        }
        stack.extend(cs.parents);
    }
}

/// What a store holds for `head`'s history: each changeset and its rows.
async fn stored_history(r: &Repo, head: ChangesetId) -> Vec<String> {
    use crate::derived::{DerivedKind, Generation, RootTree, Skiplist};
    let mut out = Vec::new();
    let mut stack = vec![head];
    let mut seen = std::collections::HashSet::new();
    while let Some(id) = stack.pop() {
        if !seen.insert(id) {
            continue;
        }
        let cs = r.meta().get_changeset(&id).await.unwrap().unwrap();
        let mut line = format!("{id} {:?}", cs.encode());
        for kind in [
            RootTree::NAME_STR,
            Generation::NAME,
            Skiplist::NAME,
            crate::REBASED_ORIGIN_KIND,
        ] {
            let v = r.meta().get_derived(kind, &id.0).await.unwrap();
            line.push_str(&format!(" {kind}={v:?}"));
        }
        out.push(line);
        stack.extend(cs.parents);
    }
    out.sort();
    out
}

/// Staged batches store byte for byte what lands replayed straight into the
/// stores, one at a time in the batches' order, store: the same changesets,
/// rows, trees, answers, ref log and head.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn staged_batches_store_what_unstaged_lands_store() {
    let mut batched = 0;
    for seed in 1..=20u64 {
        let mut rng = Rng(seed.wrapping_mul(0xA24B_AED4_963E_E407) | 1);
        let agents = 2 + rng.below(12);
        let mut script = Vec::new();
        let mut tips = Vec::new();
        for a in 0..agents {
            let mut parent = Parent::Base;
            for d in 0..1 + rng.below(3) {
                let path = match rng.below(5) {
                    0 => format!("shared/{}", rng.below(3)),
                    1 => format!("deep/{}/x/y/{a}", rng.below(2)),
                    _ => format!("own/{a}/{d}"),
                };
                script.push(spec(
                    &[parent],
                    &[(path.as_str(), Some(&format!("{seed} {a} {d}\n")))],
                    &format!("agent {a} step {d}"),
                ));
                parent = Parent::Script(script.len() - 1);
            }
            tips.push(script.len() - 1);
        }
        let r = plain_repo();
        let (_, ids) = build(&r, &script).await;
        let tasks: Vec<_> = tips
            .iter()
            .map(|&i| {
                let r = r.clone();
                let cs = ids[i];
                tokio::spawn(async move { (cs, outcome(&r.land(cs, "main").await)) })
            })
            .collect();
        let mut got = BTreeMap::new();
        for t in tasks {
            let (cs, answer) = t.await.unwrap();
            got.insert(cs, answer);
        }
        let batches = r.land_queues.batches.lock().unwrap().clone();
        batched += batches.iter().filter(|b| b.len() > 1).count();
        let order: Vec<ChangesetId> = batches.into_iter().flatten().collect();

        let oracle = plain_repo();
        let (_, oids) = build(&oracle, &script).await;
        assert_eq!(oids, ids);
        let mut want = Vec::new();
        for cs in &order {
            want.push(outcome(&oracle.land_unstaged(*cs, "main", None).await));
        }
        let answers: Vec<_> = order.iter().map(|cs| got[cs].clone()).collect();
        assert_eq!(answers, want, "seed {seed}");
        let head = r.meta().get_ref("main").await.unwrap().unwrap();
        assert_eq!(
            Some(head),
            oracle.meta().get_ref("main").await.unwrap(),
            "seed {seed}"
        );
        assert_eq!(
            log_steps(&r.meta().ref_log(Some("main"), None, 10_000).await.unwrap()),
            log_steps(
                &oracle
                    .meta()
                    .ref_log(Some("main"), None, 10_000)
                    .await
                    .unwrap()
            ),
            "seed {seed}"
        );
        assert_eq!(
            stored_history(&r, head).await,
            stored_history(&oracle, head).await,
            "seed {seed}"
        );
        assert_stored(&r, head).await;
    }
    assert!(
        batched > 5,
        "lands were batched ({batched} batches of several)"
    );
}

/// Blob writes that fail while armed.
struct FailingBlobs {
    inner: MemoryBlobStore,
    failures: AtomicUsize,
}

#[async_trait]
impl leep_core::BlobStore for FailingBlobs {
    async fn put_with_hash(&self, hash: Hash, data: Bytes) -> Result<()> {
        let left = self.failures.load(Ordering::SeqCst);
        if left > 0 {
            self.failures.store(left - 1, Ordering::SeqCst);
            return Err(leep_core::Error::Storage("injected".into()));
        }
        self.inner.put_with_hash(hash, data).await
    }
    async fn get(&self, hash: &Hash) -> Result<Option<Bytes>> {
        self.inner.get(hash).await
    }
    async fn list_page(
        &self,
        after: Option<Hash>,
        limit: usize,
    ) -> Result<Vec<leep_core::BlobMeta>> {
        self.inner.list_page(after, limit).await
    }
    async fn delete(&self, hash: &Hash) -> Result<()> {
        self.inner.delete(hash).await
    }
}

/// A batch whose objects cannot be stored never moves the ref: the failure
/// counts as a failed commit, so the batch is redone from a fresh read and
/// lands once storing works, or answers the error after three failures with
/// the ref and the changesets untouched. A batch that only conflicts stores
/// nothing.
#[tokio::test]
async fn a_batch_that_cannot_store_its_replays_moves_nothing() {
    use Parent::*;
    let script = vec![
        spec(&[Base], &[("a", Some("a\n"))], "a"),
        spec(&[Base], &[("b", Some("b\n"))], "b"),
        spec(&[Base], &[("a", Some("other\n"))], "conflicts with a"),
    ];
    let blobs = Arc::new(FailingBlobs {
        inner: MemoryBlobStore::new(),
        failures: AtomicUsize::new(0),
    });
    let meta = Arc::new(MemoryMetaStore::new());
    let r = Repo::new(blobs.clone(), meta.clone());
    let (base, ids) = build(&r, &script).await;
    r.land(ids[0], "main").await.unwrap();
    let after_a = r.meta().get_ref("main").await.unwrap();
    let stored = meta.list_changesets(None, 1000).await.unwrap().len();

    // Every attempt fails: the error is answered and nothing moved or stored.
    blobs.failures.store(usize::MAX, Ordering::SeqCst);
    let err = r.land(ids[1], "main").await.unwrap_err();
    assert!(matches!(err, leep_core::Error::Storage(_)), "{err}");
    assert_eq!(r.meta().get_ref("main").await.unwrap(), after_a);
    assert_eq!(
        meta.list_changesets(None, 1000).await.unwrap().len(),
        stored
    );

    // One failure: the batch is redone and lands.
    blobs.failures.store(1, Ordering::SeqCst);
    let resp = r.land(ids[1], "main").await.unwrap();
    let LandResponse::Landed {
        landed,
        rebased: true,
        attempts: 2,
    } = resp
    else {
        panic!("{resp:?}");
    };
    assert_eq!(r.meta().get_ref("main").await.unwrap(), Some(landed));
    assert_stored(&r, landed).await;
    let log = r.meta().ref_log(Some("main"), None, 10).await.unwrap();
    assert_eq!(log.len(), 3, "base, a and b, each once");

    // A conflict writes nothing at all.
    let stored = meta.list_changesets(None, 1000).await.unwrap().len();
    let resp = r.land(ids[2], "main").await.unwrap();
    assert!(matches!(resp, LandResponse::Conflicts { .. }), "{resp:?}");
    assert_eq!(
        meta.list_changesets(None, 1000).await.unwrap().len(),
        stored
    );
    assert!(r.is_ancestor(base, landed).await.unwrap());
}

/// [`queue_in_order`] for lands that carry land ids.
async fn queue_keyed(
    r: &Repo,
    gate: &Gate,
    base: ChangesetId,
    lands: &[(ChangesetId, Option<leep_core::LandId>)],
) -> (Pending, Vec<Pending>) {
    gate.reads.send_replace(false);
    let blocker = {
        let r = r.clone();
        tokio::spawn(async move { r.land(base, "main").await })
    };
    wait_for(|| gate.waiting_reads.load(Ordering::SeqCst) == 1).await;
    let mut tasks = Vec::new();
    for (i, (cs, id)) in lands.iter().cloned().enumerate() {
        let r2 = r.clone();
        tasks.push(tokio::spawn(async move {
            r2.land_keyed(cs, "main", None, true, id).await
        }));
        wait_for(|| r.land_queues.pending("main") == i + 1).await;
    }
    (blocker, tasks)
}

/// Land ids in one batch (ADR 0057): a duplicate of a land earlier in the
/// batch is answered with that land's result once the batch commits, an id
/// reused for another changeset is refused, an id recorded by an earlier
/// batch answers with its result, and none of it touches the other lands.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn land_ids_in_a_batch_are_settled_per_land() {
    use leep_core::LandId;
    use Parent::*;
    let script = vec![
        spec(&[Base], &[("u/1", Some("1\n"))], "u1"), // 0
        spec(&[Base], &[("x", Some("x\n"))], "x"),    // 1: keyed
        spec(&[Base], &[("u/2", Some("2\n"))], "u2"), // 2
        spec(&[Base], &[("y", Some("y\n"))], "y"),    // 3: reuses x's id
        spec(&[Base], &[("u/3", Some("3\n"))], "u3"), // 4
        spec(&[Base], &[("k", Some("k\n"))], "k"),    // 5: landed before
        spec(&[Base], &[("u/4", Some("4\n"))], "u4"), // 6
    ];
    let (r, gate) = gated_repo();
    let (_, ids) = build(&r, &script).await;
    let key = |n: u8| Some(LandId::from_bytes(&[n; 16]).unwrap());
    let earlier = r
        .land_keyed(ids[5], "main", None, true, key(5))
        .await
        .unwrap();
    let base = r.meta().get_ref("main").await.unwrap().unwrap();
    let lands = vec![
        (ids[0], None),
        (ids[1], key(1)),
        (ids[2], None),
        (ids[1], key(1)), // duplicate in the batch
        (ids[3], key(1)), // id reused for another changeset
        (ids[4], key(9)),
        (ids[5], key(5)), // recorded by an earlier batch
        (ids[6], None),
    ];
    let (blocker, tasks) = queue_keyed(&r, &gate, base, &lands).await;
    release(&gate);
    blocker.await.unwrap().unwrap();
    let mut results = Vec::new();
    for t in tasks {
        results.push(t.await.unwrap());
    }
    let landed = |i: usize| match &results[i] {
        Ok(LandResponse::Landed { landed, .. }) => *landed,
        other => panic!("land {i}: {other:?}"),
    };
    for i in [0, 2, 5, 7] {
        landed(i);
    }
    assert_eq!(
        landed(1),
        landed(3),
        "the duplicate gets the original's copy"
    );
    assert!(
        matches!(&results[4], Err(leep_core::Error::Invalid(m)) if m.contains("land id")),
        "{:?}",
        results[4]
    );
    match (&earlier, &results[6]) {
        (LandResponse::Landed { landed: a, .. }, Ok(LandResponse::Landed { landed: b, .. })) => {
            assert_eq!(a, b)
        }
        other => panic!("{other:?}"),
    }
    let batches = r.land_queues.batches.lock().unwrap().clone();
    assert_eq!(batches.last().unwrap().len(), lands.len(), "one batch");
    let log = r.meta().ref_log(Some("main"), None, 100).await.unwrap();
    let moves = log.iter().filter(|e| e.seq > 2).count();
    assert_eq!(moves, 5, "u1, x, u2, u3 (keyed 9), u4: one move each");
    let found = r
        .meta()
        .find_lands("main", &[key(1).unwrap(), key(9).unwrap()])
        .await
        .unwrap();
    assert_eq!(found[0].unwrap().landed, landed(1));
    assert_eq!(found[1].unwrap().landed, landed(5));
}
