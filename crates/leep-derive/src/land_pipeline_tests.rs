//! Pipelined land batches (ADR 0072): a batch replays onto the head the
//! batch ahead's commit will leave while that commit runs. The answers, ref
//! log and head are those of landing the batches' lands one after another;
//! a commit ahead that fails, loses its swap or runs out of lease drops the
//! replays staged behind it, which are done again from the ref, so nothing
//! is lost and nothing lands twice.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use leep_core::api::LandResponse;
use leep_core::memory::{MemoryBlobStore, MemoryMetaStore};
use leep_core::{
    BlobStore, Changeset, ChangesetId, Error, Hash, LandId, LandRecord, MetaStore, RefLogEntry,
    RefStep, RefUpdateContext, Result,
};
use tokio::sync::watch;

use crate::land_batch_tests::{
    build, log_steps, make, outcome, plain_repo, spec, wait_for, Parent, Rng, Spec,
};
use crate::Repo;

/// A memory store whose ref reads and swaps can be held shut, whose
/// commits can take a while, and in front of whose next swap "another
/// server" can move the ref.
struct Hold {
    inner: MemoryMetaStore,
    reads: watch::Sender<bool>,
    swaps: watch::Sender<bool>,
    waiting_reads: AtomicUsize,
    waiting_swaps: AtomicUsize,
    /// How long each commit (changeset write, swap) takes.
    delay: Duration,
    intruder: Mutex<Option<(Option<ChangesetId>, ChangesetId)>>,
}

impl Hold {
    fn new(delay: Duration) -> Arc<Hold> {
        Arc::new(Hold {
            inner: MemoryMetaStore::new(),
            reads: watch::channel(true).0,
            swaps: watch::channel(true).0,
            waiting_reads: AtomicUsize::new(0),
            waiting_swaps: AtomicUsize::new(0),
            delay,
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

    async fn slow(&self) {
        if !self.delay.is_zero() {
            tokio::time::sleep(self.delay).await;
        }
    }

    async fn before_swap(&self, name: &str) {
        Self::pass(&self.swaps, &self.waiting_swaps).await;
        self.slow().await;
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
impl MetaStore for Hold {
    async fn put_changeset(&self, cs: &Changeset) -> Result<ChangesetId> {
        self.inner.put_changeset(cs).await
    }
    async fn put_changeset_with_derived(
        &self,
        cs: &Changeset,
        rows: &[leep_core::DerivedRow],
    ) -> Result<ChangesetId> {
        self.inner.put_changeset_with_derived(cs, rows).await
    }
    async fn put_changesets_with_derived(
        &self,
        changesets: &[Changeset],
        rows: &[leep_core::DerivedRow],
    ) -> Result<()> {
        self.slow().await;
        self.inner
            .put_changesets_with_derived(changesets, rows)
            .await
    }
    async fn get_changeset(&self, id: &ChangesetId) -> Result<Option<Changeset>> {
        self.inner.get_changeset(id).await
    }
    async fn get_changesets(&self, ids: &[ChangesetId]) -> Result<Vec<Option<Changeset>>> {
        self.inner.get_changesets(ids).await
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
        self.inner.cas_ref_logged(name, expected, new, ctx).await
    }
    async fn cas_ref_logged_chain(
        &self,
        name: &str,
        expected: Option<ChangesetId>,
        steps: &[RefStep],
    ) -> Result<usize> {
        self.before_swap(name).await;
        self.inner.cas_ref_logged_chain(name, expected, steps).await
    }
    async fn find_lands(&self, name: &str, ids: &[LandId]) -> Result<Vec<Option<LandRecord>>> {
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
    async fn get_derived_many(&self, kind: &str, keys: &[Hash]) -> Result<Vec<Option<Bytes>>> {
        self.inner.get_derived_many(kind, keys).await
    }
    async fn put_derived(&self, kind: &str, key: &Hash, value: Bytes) -> Result<()> {
        self.inner.put_derived(kind, key, value).await
    }
}

/// Blob writes that can be held shut, and then fail.
struct HoldBlobs {
    inner: MemoryBlobStore,
    puts: watch::Sender<bool>,
    waiting_puts: AtomicUsize,
    failures: AtomicUsize,
}

impl HoldBlobs {
    fn new() -> Arc<HoldBlobs> {
        Arc::new(HoldBlobs {
            inner: MemoryBlobStore::new(),
            puts: watch::channel(true).0,
            waiting_puts: AtomicUsize::new(0),
            failures: AtomicUsize::new(0),
        })
    }

    async fn before_put(&self) -> Result<()> {
        Hold::pass(&self.puts, &self.waiting_puts).await;
        let failed = self
            .failures
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
            .is_ok();
        if failed {
            return Err(Error::Storage("injected".into()));
        }
        Ok(())
    }
}

#[async_trait]
impl BlobStore for HoldBlobs {
    async fn put_with_hash(&self, hash: Hash, data: Bytes) -> Result<()> {
        self.before_put().await?;
        self.inner.put_with_hash(hash, data).await
    }
    async fn put_many(&self, items: Vec<(Hash, Bytes)>) -> Result<Vec<Hash>> {
        self.before_put().await?;
        self.inner.put_many(items).await
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

fn held_repo() -> (Repo, Arc<Hold>, Arc<HoldBlobs>) {
    let meta = Hold::new(Duration::ZERO);
    let blobs = HoldBlobs::new();
    let r = Repo::new(blobs.clone(), meta.clone());
    (r, meta, blobs)
}

type Pending = tokio::task::JoinHandle<Result<LandResponse>>;

fn land(r: &Repo, cs: ChangesetId) -> Pending {
    let r = r.clone();
    tokio::spawn(async move { r.land(cs, "main").await })
}

fn speculated(r: &Repo) -> Vec<(Vec<ChangesetId>, bool)> {
    r.land_queues.speculated.lock().unwrap().clone()
}

fn batches(r: &Repo) -> usize {
    r.land_queues.batches.lock().unwrap().len()
}

fn landed(resp: &LandResponse) -> ChangesetId {
    match resp {
        LandResponse::Landed { landed, .. } => *landed,
        other => panic!("{other:?}"),
    }
}

fn attempts(resp: &LandResponse) -> u32 {
    match resp {
        LandResponse::Landed { attempts, .. } => *attempts,
        other => panic!("{other:?}"),
    }
}

/// Three disjoint changesets on the base, and one more that "another
/// server" lands.
fn disjoint() -> Vec<Spec> {
    use Parent::*;
    vec![
        spec(&[Base], &[("a", Some("a\n"))], "a"),
        spec(&[Base], &[("b", Some("b\n"))], "b"),
        spec(&[Base], &[("c", Some("c\n"))], "c"),
        spec(&[Base], &[("x", Some("x\n"))], "x"),
    ]
}

/// Land `a` and hold its swap, then land `b`, which replays onto `a`'s
/// staged head while `a` waits. Returns once `b`'s replay is done.
async fn stage_behind(r: &Repo, meta: &Hold, a: ChangesetId, b: ChangesetId) -> (Pending, Pending) {
    wait_for(|| r.land_queues.idle("main")).await;
    meta.swaps.send_replace(false);
    let ta = land(r, a);
    wait_for(|| meta.waiting_swaps.load(Ordering::SeqCst) == 1).await;
    let tb = land(r, b);
    wait_for(|| batches(r) == 2).await;
    (ta, tb)
}

/// The batch behind is kept when the commit ahead applies: it commits on
/// the head that commit left, after it, without reading the ref.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_batch_staged_behind_a_commit_that_applies_is_kept() {
    let (r, meta, _) = held_repo();
    let (base, ids) = build(&r, &disjoint()).await;
    let (ta, tb) = stage_behind(&r, &meta, ids[0], ids[1]).await;
    assert!(speculated(&r).is_empty(), "b waits for a's commit");
    meta.swaps.send_replace(true);
    let a = ta.await.unwrap().unwrap();
    let b = tb.await.unwrap().unwrap();
    assert_eq!(speculated(&r), vec![(vec![ids[1]], true)]);
    assert_eq!(
        a,
        LandResponse::Landed {
            landed: ids[0],
            rebased: false,
            attempts: 1
        }
    );
    let b_copy = r.get_changeset(landed(&b)).await.unwrap();
    assert_eq!(
        b_copy.parents,
        vec![ids[0]],
        "b replayed onto a's staged head"
    );
    assert_eq!(attempts(&b), 1);
    let (want, want_log, want_head) = sequential(&disjoint(), &[(0, None), (1, None)], false).await;
    assert_eq!(vec![outcome(&Ok(a)), outcome(&Ok(b))], want);
    let log = r.meta().ref_log(Some("main"), None, 100).await.unwrap();
    assert_eq!(log_steps(&log), want_log);
    assert_eq!(r.meta().get_ref("main").await.unwrap(), want_head);
    crate::land_batch_tests::assert_stored_pub(&r, want_head.unwrap()).await;
    let _ = base;
}

/// The commit ahead loses its swap to another server: the batch behind is
/// dropped, waits for the batch ahead to land on the new head, and is done
/// again from there. Each lands once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lost_swap_ahead_drops_the_batch_staged_behind() {
    let (r, meta, _) = held_repo();
    let (base, ids) = build(&r, &disjoint()).await;
    let (ta, tb) = stage_behind(&r, &meta, ids[0], ids[1]).await;
    *meta.intruder.lock().unwrap() = Some((Some(base), ids[3]));
    meta.swaps.send_replace(true);
    let a = ta.await.unwrap().unwrap();
    let b = tb.await.unwrap().unwrap();
    assert_eq!(speculated(&r), vec![(vec![ids[1]], false)]);
    assert_eq!(attempts(&a), 2, "a redone on x");
    assert_eq!(attempts(&b), 1, "the dropped attempt does not count");
    let b_copy = r.get_changeset(landed(&b)).await.unwrap();
    assert_eq!(
        b_copy.parents,
        vec![landed(&a)],
        "b redone on a's real head"
    );
    // As if x landed first, then a, then b.
    let oracle = plain_repo();
    let (obase, oids) = build(&oracle, &disjoint()).await;
    oracle
        .meta()
        .cas_ref_logged(
            "main",
            Some(obase),
            Some(oids[3]),
            &RefUpdateContext::new("cas").message("another server"),
        )
        .await
        .unwrap();
    let oa = oracle.land(oids[0], "main").await.unwrap();
    let ob = oracle.land(oids[1], "main").await.unwrap();
    assert_eq!(landed(&a), landed(&oa));
    assert_eq!(landed(&b), landed(&ob));
    let log = r.meta().ref_log(Some("main"), None, 100).await.unwrap();
    let want = oracle
        .meta()
        .ref_log(Some("main"), None, 100)
        .await
        .unwrap();
    assert_eq!(log_steps(&log), log_steps(&want));
    crate::land_batch_tests::assert_stored_pub(&r, landed(&b)).await;
}

/// The commit ahead cannot store its objects: the batch behind is dropped
/// before it stores or answers anything, and both land once storing works.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_flush_ahead_drops_the_batch_staged_behind() {
    use Parent::*;
    // Replays (not fast-forwards), so the commit ahead has objects to store.
    let script = vec![
        spec(&[Base], &[("o", Some("o\n"))], "o"),
        spec(&[Base], &[("a", Some("a\n"))], "a"),
        spec(&[Base], &[("b", Some("b\n"))], "b"),
    ];
    let (r, meta, blobs) = held_repo();
    let (_, ids) = build(&r, &script).await;
    r.land(ids[0], "main").await.unwrap();
    // o's answer comes before its batch task ends: a land arriving in that
    // window would replay behind o's finished commit and be recorded as
    // speculated, which this test does not expect.
    wait_for(|| r.land_queues.idle("main")).await;
    blobs.puts.send_replace(false);
    let ta = land(&r, ids[1]);
    wait_for(|| blobs.waiting_puts.load(Ordering::SeqCst) == 1).await;
    let tb = land(&r, ids[2]);
    wait_for(|| batches(&r) == 3).await;
    blobs.failures.store(1, Ordering::SeqCst);
    blobs.puts.send_replace(true);
    let a = ta.await.unwrap().unwrap();
    let b = tb.await.unwrap().unwrap();
    assert_eq!(speculated(&r), vec![(vec![ids[2]], false)]);
    assert_eq!(attempts(&a), 2);
    assert_eq!(attempts(&b), 1);
    let (want, want_log, want_head) =
        sequential(&script, &[(0, None), (1, None), (2, None)], false).await;
    assert_eq!(landed(&b), want_head.unwrap());
    assert_eq!(outcome(&Ok(b.clone())), want[2]);
    let log = r.meta().ref_log(Some("main"), None, 100).await.unwrap();
    assert_eq!(log_steps(&log), want_log);
    crate::land_batch_tests::assert_stored_pub(&r, landed(&b)).await;
    let _ = meta;
}

/// Answers that need no move (already landed, a conflict, a refusal) rest
/// on the head the batch ahead leaves, so a batch staged behind it holds
/// them until that commit applies.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn answers_staged_behind_wait_for_the_commit_ahead() {
    use Parent::*;
    let script = vec![
        spec(&[Base], &[("a", Some("a\n"))], "a"),
        spec(&[Base], &[("a", Some("other\n"))], "conflicts with a"),
        spec(&[Base], &[("x", Some("x\n"))], "x"),
    ];
    for lose in [false, true] {
        let (r, meta, _) = held_repo();
        let (base, ids) = build(&r, &script).await;
        let (ta, tb) = stage_behind(&r, &meta, ids[0], ids[1]).await;
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(
            !tb.is_finished(),
            "the conflict is not answered before a's commit"
        );
        if lose {
            *meta.intruder.lock().unwrap() = Some((Some(base), ids[2]));
        }
        meta.swaps.send_replace(true);
        let a = ta.await.unwrap().unwrap();
        let b = tb.await.unwrap().unwrap();
        assert_eq!(speculated(&r), vec![(vec![ids[1]], !lose)]);
        let LandResponse::Conflicts { onto, .. } = b else {
            panic!("{b:?}");
        };
        assert_eq!(onto, landed(&a), "the conflict is against a's real head");
    }
}

/// The landed-copy scan and land ids see the batch ahead's moves before
/// they are stored: a resend of a land from the batch ahead, keyed or not,
/// is answered with its copy instead of landing twice.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_resend_staged_behind_its_original_lands_once() {
    use Parent::*;
    let script = vec![
        spec(&[Base], &[("o", Some("o\n"))], "o"),
        spec(&[Base], &[("a", Some("a\n"))], "a"),
    ];
    for keyed in [false, true] {
        let (r, meta, _) = held_repo();
        let (_, ids) = build(&r, &script).await;
        r.land(ids[0], "main").await.unwrap();
        let id = keyed.then(|| LandId::from_bytes(&[7; 16]).unwrap());
        let send = || {
            let (r, id) = (r.clone(), id.clone());
            let cs = ids[1];
            tokio::spawn(async move { r.land_keyed(cs, "main", None, true, id).await })
        };
        wait_for(|| r.land_queues.idle("main")).await;
        meta.swaps.send_replace(false);
        let first = send();
        wait_for(|| meta.waiting_swaps.load(Ordering::SeqCst) == 1).await;
        let again = send();
        wait_for(|| batches(&r) == 3).await;
        meta.swaps.send_replace(true);
        let first = first.await.unwrap().unwrap();
        let again = again.await.unwrap().unwrap();
        assert_eq!(speculated(&r), vec![(vec![ids[1]], true)], "keyed {keyed}");
        assert_eq!(landed(&first), landed(&again), "keyed {keyed}");
        let log = r.meta().ref_log(Some("main"), None, 100).await.unwrap();
        assert_eq!(log.len(), 3, "keyed {keyed}: base, o and a once");
    }
}

/// With `--land-pipeline off` a batch never replays behind a commit.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pipelining_off_waits_for_each_commit() {
    let (r, meta, _) = held_repo();
    let r = r.with_land_pipeline(false);
    let (_, ids) = build(&r, &disjoint()).await;
    wait_for(|| r.land_queues.idle("main")).await;
    meta.swaps.send_replace(false);
    let ta = land(&r, ids[0]);
    wait_for(|| meta.waiting_swaps.load(Ordering::SeqCst) == 1).await;
    let tb = land(&r, ids[1]);
    wait_for(|| r.land_queues.pending("main") == 1).await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(batches(&r), 1, "b is not taken while a commits");
    meta.swaps.send_replace(true);
    ta.await.unwrap().unwrap();
    tb.await.unwrap().unwrap();
    assert!(speculated(&r).is_empty());
}

/// A batch whose replays take longer than the limit closes early: the
/// lands it did not reach go back to the front of the queue, in order,
/// for the next batch, and every answer is as before.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn slow_replays_close_a_batch_early() {
    for limit in [Some(Duration::from_micros(1)), None] {
        let (r, meta, _) = held_repo();
        let r = r.with_land_batch_replay_limit(limit);
        let (base, ids) = build(&r, &disjoint()).await;
        wait_for(|| r.land_queues.idle("main")).await;
        meta.reads.send_replace(false);
        let blocker = land(&r, base);
        wait_for(|| meta.waiting_reads.load(Ordering::SeqCst) == 1).await;
        let mut tasks = Vec::new();
        for (i, &cs) in ids[..3].iter().enumerate() {
            tasks.push(land(&r, cs));
            wait_for(|| r.land_queues.pending("main") == i + 1).await;
        }
        meta.reads.send_replace(true);
        blocker.await.unwrap().unwrap();
        let mut got = Vec::new();
        for t in tasks {
            got.push(outcome(&t.await.unwrap()));
        }
        let taken = r.land_queues.batches.lock().unwrap().clone();
        let want_batches: Vec<Vec<ChangesetId>> = match limit {
            Some(_) => vec![vec![base], vec![ids[0]], vec![ids[1]], vec![ids[2]]],
            None => vec![vec![base], ids[..3].to_vec()],
        };
        assert_eq!(taken, want_batches, "limit {limit:?}");
        let (want, want_log, want_head) =
            sequential(&disjoint(), &[(0, None), (1, None), (2, None)], false).await;
        assert_eq!(got, want, "limit {limit:?}");
        let log = r.meta().ref_log(Some("main"), None, 100).await.unwrap();
        assert_eq!(log_steps(&log), want_log, "limit {limit:?}");
        assert_eq!(r.meta().get_ref("main").await.unwrap(), want_head);
    }
}

/// A herd at a fresh server is admitted whole: before any batch ended the
/// seconds bound has no batch time to count in and refuses nothing. Once a
/// batch time is known the bound holds.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_herd_at_a_fresh_queue_is_not_refused_for_lack_of_a_batch_time() {
    let (r, meta, _) = held_repo();
    let r = r
        .with_land_queue_seconds(Some(Duration::from_secs(5)))
        .with_land_max_batch(2);
    let mut script = Vec::new();
    for i in 0..40 {
        script.push(spec(
            &[Parent::Base],
            &[(format!("f/{i}").as_str(), Some("x\n"))],
            &format!("{i}"),
        ));
    }
    let (base, ids) = build(&r, &script).await;
    wait_for(|| r.land_queues.idle("main")).await;
    meta.reads.send_replace(false);
    let blocker = land(&r, base);
    wait_for(|| meta.waiting_reads.load(Ordering::SeqCst) == 1).await;
    // 20 batches of 2 behind a batch with no time yet: far more than 5 s
    // at the 1 s a batch counted as before.
    let mut tasks = Vec::new();
    for (i, &cs) in ids[..30].iter().enumerate() {
        tasks.push(land(&r, cs));
        wait_for(|| r.land_queues.pending("main") == i + 1).await;
    }
    meta.reads.send_replace(true);
    blocker.await.unwrap().unwrap();
    for t in tasks {
        assert!(matches!(t.await.unwrap(), Ok(LandResponse::Landed { .. })));
    }
    // A slow batch time now: 5 s holds two batches of 2, the one in flight
    // included.
    r.land_queues.set_batch_secs("main", 2.5);
    wait_for(|| r.land_queues.idle("main")).await;
    meta.reads.send_replace(false);
    let blocker = land(&r, base);
    wait_for(|| meta.waiting_reads.load(Ordering::SeqCst) == 1).await;
    let first = land(&r, ids[30]);
    let second = land(&r, ids[31]);
    wait_for(|| r.land_queues.pending("main") == 2).await;
    let refused = r.land(ids[32], "main").await;
    assert!(matches!(refused, Err(Error::Refused { .. })), "{refused:?}");
    meta.reads.send_replace(true);
    blocker.await.unwrap().unwrap();
    first.await.unwrap().unwrap();
    second.await.unwrap().unwrap();
}

/// The answers, ref log and head of landing `order` (script index, land
/// id) one at a time on a fresh repository built from the same script.
async fn sequential(
    script: &[Spec],
    order: &[(usize, Option<LandId>)],
    lazy: bool,
) -> (Vec<String>, Vec<String>, Option<ChangesetId>) {
    let r = plain_repo().with_lazy_create_trees(lazy);
    let (_, ids) = build(&r, script).await;
    let mut answers = Vec::new();
    for (i, id) in order {
        answers.push(outcome(
            &r.land_keyed(ids[*i], "main", None, true, id.clone()).await,
        ));
    }
    let log = log_steps(&r.meta().ref_log(Some("main"), None, 10_000).await.unwrap());
    (answers, log, r.meta().get_ref("main").await.unwrap())
}

/// Agents land random stacks at random times, onto a store whose commits
/// take a few milliseconds, so batches pipeline: with overlapping files
/// (conflicts and slow-path merges), disjoint ones (the fast path), land
/// ids, resends and creates with and without their trees. Landing the
/// batches' lands one at a time, in the order they were replayed, must give
/// the same answers, ref log and head.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pipelined_lands_match_landing_in_turn() {
    let (mut kept, mut conflicts, mut fast) = (0, 0, 0);
    for seed in 1..=24u64 {
        let mut rng = Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
        let lazy = seed % 2 == 0;
        let fast_path = seed % 3 != 0;
        let agents = 8 + rng.below(24);
        let mut script = Vec::new();
        let mut tips = Vec::new();
        for a in 0..agents {
            let depth = 1 + rng.below(3);
            let mut parent = Parent::Base;
            for d in 0..depth {
                let path = if rng.below(5) == 0 {
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
        // Each agent lands its tip; some name the land, some send it twice.
        let mut lands: Vec<(usize, Option<LandId>)> = Vec::new();
        for (a, &tip) in tips.iter().enumerate() {
            let id = (rng.below(2) == 0).then(|| LandId::from_bytes(&[a as u8 + 1; 16]).unwrap());
            lands.push((tip, id.clone()));
            if rng.below(6) == 0 {
                lands.push((tip, id));
            }
        }
        let meta = Hold::new(Duration::from_millis(2));
        let r = Repo::new(Arc::new(MemoryBlobStore::new()), meta)
            .with_lazy_create_trees(lazy)
            .with_land_fast_path(fast_path);
        let (_, ids) = build(&r, &script).await;
        let tasks: Vec<_> = lands
            .iter()
            .map(|(i, id)| {
                let (r, cs, id) = (r.clone(), ids[*i], id.clone());
                let wait = rng.below(40) as u64;
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_millis(wait)).await;
                    let answer = r.land_keyed(cs, "main", None, true, id.clone()).await;
                    ((cs, id), outcome(&answer))
                })
            })
            .collect();
        let mut got: BTreeMap<(ChangesetId, Option<LandId>), Vec<String>> = BTreeMap::new();
        for t in tasks {
            let (k, answer) = t.await.unwrap();
            got.entry(k).or_default().push(answer);
        }
        let s = speculated(&r);
        assert!(
            s.iter().all(|(_, k)| *k),
            "seed {seed}: no other writer, nothing dropped"
        );
        kept += s.len();
        if fast_path {
            fast += r
                .land_queues
                .lookups
                .lock()
                .unwrap()
                .iter()
                .filter(|(_, l)| *l == crate::land_fast::Lookup::Hit)
                .count();
        }
        // The batches name changesets; a changeset sent twice (with the
        // same id or none) gets the same answer either time, so the order
        // of the sends of one changeset does not matter.
        let order: Vec<ChangesetId> = r
            .land_queues
            .batches
            .lock()
            .unwrap()
            .iter()
            .flatten()
            .copied()
            .collect();
        assert_eq!(
            order.len(),
            lands.len(),
            "seed {seed}: every land replayed once"
        );
        let index = |cs: ChangesetId| ids.iter().position(|&i| i == cs).unwrap();
        let mut sends: BTreeMap<ChangesetId, Vec<Option<LandId>>> = BTreeMap::new();
        for (i, id) in &lands {
            sends.entry(ids[*i]).or_default().push(id.clone());
        }
        let seq_order: Vec<(usize, Option<LandId>)> = order
            .iter()
            .map(|&cs| (index(cs), sends.get_mut(&cs).unwrap().remove(0)))
            .collect();
        let (want, want_log, want_head) = sequential(&script, &seq_order, lazy).await;
        let mut want_by: BTreeMap<(ChangesetId, Option<LandId>), Vec<String>> = BTreeMap::new();
        for ((i, id), answer) in seq_order.iter().zip(&want) {
            want_by
                .entry((ids[*i], id.clone()))
                .or_default()
                .push(answer.clone());
        }
        for v in got.values_mut().chain(want_by.values_mut()) {
            v.sort();
        }
        assert_eq!(got, want_by, "seed {seed}");
        conflicts += want.iter().filter(|a| a.starts_with("Conflicts")).count();
        let log = r.meta().ref_log(Some("main"), None, 10_000).await.unwrap();
        assert_eq!(log_steps(&log), want_log, "seed {seed}");
        let head = r.meta().get_ref("main").await.unwrap();
        assert_eq!(head, want_head, "seed {seed}");
        // A lazy create's stack below its tip keeps its trees unstored.
        if !lazy {
            crate::land_batch_tests::assert_stored_pub(&r, head.unwrap()).await;
        }
    }
    assert!(kept > 20, "batches were pipelined ({kept} kept)");
    assert!(conflicts > 5, "some lands conflicted ({conflicts})");
    assert!(fast > 20, "the fast path was taken ({fast})");
}

/// A land of `make` on a repo, for tests that need a changeset made after
/// the build.
#[allow(dead_code)]
async fn extra(r: &Repo, parent: ChangesetId, path: &str) -> ChangesetId {
    make(r, &[parent], &[(path.into(), Some("x\n".into()))], path).await
}
