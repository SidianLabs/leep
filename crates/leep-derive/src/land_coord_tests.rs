//! Lands coordinated across simulated servers (ADR 0056): several `Repo`s,
//! each with its own land queues and [`LandCoordinator`], over one memory
//! store whose lease clock is tokio's, so a paused runtime moves leases
//! along with every timer. Forwards go straight to the other `Repo`, and can
//! be dropped, lose their answer after the leader landed, or find the
//! server dead. Every test ends with the checks of the uncoordinated fault
//! sweep: nothing an agent was told landed is lost, nothing lands twice, and
//! the ref log replays.

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use leep_core::api::LandResponse;
use leep_core::memory::MemoryMetaStore;
use leep_core::{
    Changeset, ChangesetId, Error, Hash, LandId, LandRecord, Lease, LeaseClaim, LeaseKey,
    LeaseTiming, MetaStore, RefLogEntry, RefStep, RefUpdateContext, Result,
};
use leep_testkit::{is_injected, Fault, FaultPlan, FaultyBlobStore, FaultyMetaStore, Op, Rng};
use tokio::sync::watch;
use tokio::time::Instant;

use crate::fault_tests::{assert_lands_kept, commit_file, retry, seeds, Agent, World};
use crate::land_coord::{Coordinated, CoordinatorConfig, Forwarded, LandCoordinator, LandForward};
use crate::Repo;

pub(crate) const REF: &str = "main";
/// An advertised address no forward reaches.
const UNREACHABLE: &str = "unreachable";

/// Holds every call while paused, as a stopped process makes none.
struct Pausable {
    inner: Arc<dyn MetaStore>,
    paused: watch::Sender<bool>,
}

impl Pausable {
    async fn gate(&self) {
        let mut rx = self.paused.subscribe();
        while *rx.borrow_and_update() {
            if rx.changed().await.is_err() {
                return;
            }
        }
    }
}

#[async_trait]
impl MetaStore for Pausable {
    async fn put_changeset(&self, cs: &Changeset) -> Result<ChangesetId> {
        self.gate().await;
        self.inner.put_changeset(cs).await
    }
    async fn get_changeset(&self, id: &ChangesetId) -> Result<Option<Changeset>> {
        self.gate().await;
        self.inner.get_changeset(id).await
    }
    async fn get_changesets(&self, ids: &[ChangesetId]) -> Result<Vec<Option<Changeset>>> {
        self.gate().await;
        self.inner.get_changesets(ids).await
    }
    async fn list_changesets(
        &self,
        after: Option<ChangesetId>,
        limit: usize,
    ) -> Result<Vec<ChangesetId>> {
        self.gate().await;
        self.inner.list_changesets(after, limit).await
    }
    async fn get_ref(&self, name: &str) -> Result<Option<ChangesetId>> {
        self.gate().await;
        self.inner.get_ref(name).await
    }
    async fn cas_ref_logged(
        &self,
        name: &str,
        expected: Option<ChangesetId>,
        new: Option<ChangesetId>,
        ctx: &RefUpdateContext,
    ) -> Result<bool> {
        self.gate().await;
        self.inner.cas_ref_logged(name, expected, new, ctx).await
    }
    async fn cas_ref_logged_chain(
        &self,
        name: &str,
        expected: Option<ChangesetId>,
        steps: &[RefStep],
    ) -> Result<usize> {
        self.gate().await;
        self.inner.cas_ref_logged_chain(name, expected, steps).await
    }
    async fn ref_log(
        &self,
        name: Option<&str>,
        before_seq: Option<u64>,
        limit: usize,
    ) -> Result<Vec<RefLogEntry>> {
        self.gate().await;
        self.inner.ref_log(name, before_seq, limit).await
    }
    async fn list_refs(&self) -> Result<BTreeMap<String, ChangesetId>> {
        self.gate().await;
        self.inner.list_refs().await
    }
    async fn get_derived(&self, kind: &str, key: &Hash) -> Result<Option<Bytes>> {
        self.gate().await;
        self.inner.get_derived(kind, key).await
    }
    async fn get_derived_many(&self, kind: &str, keys: &[Hash]) -> Result<Vec<Option<Bytes>>> {
        self.gate().await;
        self.inner.get_derived_many(kind, keys).await
    }
    async fn put_derived(&self, kind: &str, key: &Hash, value: Bytes) -> Result<()> {
        self.gate().await;
        self.inner.put_derived(kind, key, value).await
    }
    async fn put_derived_many(&self, rows: &[leep_core::DerivedRow]) -> Result<()> {
        self.gate().await;
        self.inner.put_derived_many(rows).await
    }
    async fn put_changeset_with_derived(
        &self,
        cs: &Changeset,
        rows: &[leep_core::DerivedRow],
    ) -> Result<ChangesetId> {
        self.gate().await;
        self.inner.put_changeset_with_derived(cs, rows).await
    }
    async fn put_changesets_with_derived(
        &self,
        changesets: &[Changeset],
        rows: &[leep_core::DerivedRow],
    ) -> Result<()> {
        self.gate().await;
        self.inner
            .put_changesets_with_derived(changesets, rows)
            .await
    }
    async fn find_lands(&self, name: &str, ids: &[LandId]) -> Result<Vec<Option<LandRecord>>> {
        self.gate().await;
        self.inner.find_lands(name, ids).await
    }
    fn supports_leases(&self) -> bool {
        self.inner.supports_leases()
    }
    async fn lease(&self, key: &LeaseKey, timing: LeaseTiming) -> Result<Option<Lease>> {
        self.gate().await;
        self.inner.lease(key, timing).await
    }
    async fn acquire_lease(&self, claim: &LeaseClaim<'_>, timing: LeaseTiming) -> Result<Lease> {
        self.gate().await;
        self.inner.acquire_lease(claim, timing).await
    }
    async fn renew_leases(
        &self,
        holder: &str,
        held: &[(LeaseKey, u64)],
        timing: LeaseTiming,
    ) -> Result<Vec<Option<Duration>>> {
        self.gate().await;
        self.inner.renew_leases(holder, held, timing).await
    }
    async fn release_lease(&self, key: &LeaseKey, holder: &str, epoch: u64) -> Result<bool> {
        self.gate().await;
        self.inner.release_lease(key, holder, epoch).await
    }
}

/// One simulated server process.
pub(crate) struct Node {
    pub(crate) address: String,
    pub(crate) repo: Repo,
    pub(crate) coord: Arc<LandCoordinator>,
    pause: Arc<Pausable>,
    dead: watch::Sender<bool>,
}

impl Node {
    pub(crate) fn is_dead(&self) -> bool {
        *self.dead.borrow()
    }

    /// Run `f` as a request to this server: abandoned, as a reset
    /// connection, if the server dies first.
    pub(crate) async fn call<T>(&self, f: impl Future<Output = T>) -> Option<T> {
        if self.is_dead() {
            return None;
        }
        let mut rx = self.dead.subscribe();
        tokio::select! {
            v = f => Some(v),
            _ = rx.wait_for(|d| *d) => None,
        }
    }

    /// SIGKILL: requests in flight are reset, batches stop wherever they
    /// are, and the lease is neither renewed nor released.
    pub(crate) fn kill(&self) {
        self.dead.send_replace(true);
        self.coord.halt();
        self.repo.land_queues.abort_leaders();
    }

    pub(crate) fn set_paused(&self, paused: bool) {
        self.pause.paused.send_replace(paused);
    }
}

/// The servers, and what happens to forwards between them.
pub(crate) struct Net {
    pub(crate) nodes: Vec<Arc<Node>>,
    rng: Mutex<Rng>,
    /// Forwards reset before the leader saw them, per mille.
    drop: u32,
    /// Forwards whose answer is lost after the leader landed, per mille.
    lose: u32,
    lost: AtomicUsize,
}

pub(crate) struct Fwd {
    pub(crate) net: Arc<Net>,
    pub(crate) cs: ChangesetId,
    pub(crate) land_id: Option<LandId>,
}

/// Never called: a forwarded land is never forwarded again.
pub(crate) struct NoForward;

#[async_trait]
impl LandForward<Result<LandResponse>> for NoForward {
    async fn forward(&self, _: &Lease) -> Forwarded<Result<LandResponse>> {
        panic!("a land forwarded to this server was forwarded again");
    }
}

#[async_trait]
impl LandForward<Result<LandResponse>> for Fwd {
    async fn forward(&self, lease: &Lease) -> Forwarded<Result<LandResponse>> {
        let Some(node) = self
            .net
            .nodes
            .iter()
            .find(|n| n.address == lease.address && n.address != UNREACHABLE)
        else {
            return Forwarded::failed("connect", "no server at that address");
        };
        let (drop, lose) = {
            let mut r = self.net.rng.lock().unwrap();
            (r.chance(self.net.drop), r.chance(self.net.lose))
        };
        if drop {
            return Forwarded::failed("reset", "reset before it was sent");
        }
        let out = node
            .call(node.repo.land_coordinated(
                self.cs,
                REF,
                None,
                true,
                self.land_id.clone(),
                true,
                &NoForward,
            ))
            .await;
        if lose {
            self.net.lost.fetch_add(1, Ordering::Relaxed);
            return Forwarded::failed("reset", "answer lost");
        }
        match out {
            None => Forwarded::failed("reset", "server died"),
            Some(Ok(Coordinated::Landed(r))) => Forwarded::Answered(Ok(r)),
            Some(Ok(Coordinated::NotLeader)) => Forwarded::NotLeader,
            Some(Ok(Coordinated::Forwarded(_))) => unreachable!("NoForward panics"),
            Some(Err(e)) => Forwarded::Answered(Err(e)),
        }
    }
}

/// A world whose lease clock is tokio's.
pub(crate) fn world(seed: u64) -> World {
    let origin = Instant::now();
    World::with_meta(
        seed,
        MemoryMetaStore::new()
            .with_lease_clock(Arc::new(move || origin.elapsed().as_millis() as i64)),
    )
}

pub(crate) fn node(
    w: &World,
    plan: &Arc<FaultPlan>,
    i: usize,
    address: String,
    tune: impl FnOnce(&mut CoordinatorConfig),
) -> Arc<Node> {
    bounded_node(w, plan, i, address, tune, None)
}

/// [`node`] with `--land-queue-batches` (ADR 0060).
pub(crate) fn bounded_node(
    w: &World,
    plan: &Arc<FaultPlan>,
    i: usize,
    address: String,
    tune: impl FnOnce(&mut CoordinatorConfig),
    queue_batches: Option<usize>,
) -> Arc<Node> {
    let pause = Arc::new(Pausable {
        inner: w.meta.clone(),
        paused: watch::channel(false).0,
    });
    // Faults outside the pause, so a paused server stops even mid-delay.
    let meta: Arc<dyn MetaStore> = Arc::new(FaultyMetaStore::new(pause.clone(), plan.clone()));
    let mut cfg = CoordinatorConfig::new(format!("holder-{i}"), address.clone());
    tune(&mut cfg);
    let coord = LandCoordinator::new(meta.clone(), cfg);
    coord.start();
    let repo = Repo::new(
        Arc::new(FaultyBlobStore::new(w.blobs.clone(), plan.clone())),
        meta,
    )
    .with_land_coordinator(coord.clone())
    .with_land_queue_batches(queue_batches);
    Arc::new(Node {
        address,
        repo,
        coord,
        pause,
        dead: watch::channel(false).0,
    })
}

pub(crate) fn net(nodes: Vec<Arc<Node>>, seed: u64, drop: u32, lose: u32) -> Arc<Net> {
    Arc::new(Net {
        nodes,
        rng: Mutex::new(Rng::new(seed ^ 0x5EED)),
        drop,
        lose,
        lost: AtomicUsize::new(0),
    })
}

/// What an agent saw besides its answers.
#[derive(Default)]
struct Seen {
    /// Most attempts any of its successful answers reported.
    max_attempts: u32,
    /// Errors it was answered with and retried.
    errors: Vec<String>,
    /// When its land was answered.
    landed_at: Option<Instant>,
}

/// Commit a stack and land it through server `n % servers`, moving to the
/// next live server when one dies, until a land is answered with success.
async fn agent(seed: u64, net: Arc<Net>, w_clean: Repo, n: usize, steps: usize) -> (Agent, Seen) {
    let mut parent = retry(seed, "read main", || w_clean.meta().get_ref(REF)).await;
    let mut files = Vec::new();
    for k in 0..steps {
        let path = format!("agent-{n}/step-{k}.txt");
        let body = format!("agent {n} step {k}\n");
        parent =
            Some(commit_file(seed, &w_clean, parent, &path, &body, &format!("a{n}s{k}")).await);
        files.push((path, body));
    }
    let top = parent.expect("at least one step");
    // Half the agents name their land (ADR 0057); it must pass through
    // forwarding unchanged.
    let land_id = n.is_multiple_of(2).then(|| {
        let mut id = [0u8; 16];
        id[..8].copy_from_slice(&seed.to_le_bytes());
        id[8..].copy_from_slice(&(n as u64).to_le_bytes());
        LandId::from_bytes(&id).unwrap()
    });
    let mut seen = Seen::default();
    let mut reported = Vec::new();
    for t in 0..2000 {
        let node = &net.nodes[(n + t) % net.nodes.len()];
        if node.is_dead() {
            continue;
        }
        let fwd = Fwd {
            net: net.clone(),
            cs: top,
            land_id: land_id.clone(),
        };
        let call = node.call(node.repo.land_coordinated(
            top,
            REF,
            None,
            true,
            land_id.clone(),
            false,
            &fwd,
        ));
        let answer = match tokio::time::timeout(Duration::from_secs(60), call).await {
            Err(_) | Ok(None) => continue,
            Ok(Some(Ok(Coordinated::Landed(r)))) => Ok(r),
            Ok(Some(Ok(Coordinated::Forwarded(r)))) => r,
            Ok(Some(Ok(Coordinated::NotLeader))) => panic!("seed {seed}: NotLeader to a client"),
            Ok(Some(Err(e))) => Err(e),
        };
        match answer {
            Ok(LandResponse::Landed {
                landed, attempts, ..
            }) => {
                seen.max_attempts = seen.max_attempts.max(attempts);
                seen.landed_at = Some(Instant::now());
                reported.push(landed);
                break;
            }
            Ok(LandResponse::Conflicts { .. }) => panic!("seed {seed}: disjoint files conflict"),
            Err(e) if retryable(&e) => seen.errors.push(e.to_string()),
            Err(e) => panic!("seed {seed}: agent {n}: land failed for real: {e}"),
        }
    }
    assert!(!reported.is_empty(), "seed {seed}: agent {n} never landed");
    (Agent { n, files, reported }, seen)
}

/// Answers a client retries: injected store faults, a land that kept losing
/// or could not reach the ref's coordinator (409), a land whose leader task
/// died with its server.
fn retryable(e: &Error) -> bool {
    is_injected(e)
        || matches!(e, Error::Conflict(_))
        || matches!(e, Error::Other(m) if m.contains("abandoned"))
}

async fn run_agents(
    seed: u64,
    w: &World,
    net: &Arc<Net>,
    agents: usize,
) -> Vec<tokio::task::JoinHandle<(Agent, Seen)>> {
    let mut rng = Rng::new(seed ^ 0xA5A5);
    (0..agents)
        .map(|n| {
            let steps = 1 + rng.below(2);
            tokio::spawn(agent(seed, net.clone(), w.clean.clone(), n, steps))
        })
        .collect()
}

async fn join(tasks: Vec<tokio::task::JoinHandle<(Agent, Seen)>>) -> (Vec<Agent>, Vec<Seen>) {
    let mut agents = Vec::new();
    let mut seen = Vec::new();
    for t in tasks {
        let (a, s) = t.await.unwrap();
        agents.push(a);
        seen.push(s);
    }
    (agents, seen)
}

pub(crate) fn leader(net: &Net) -> Option<&Arc<Node>> {
    net.nodes
        .iter()
        .find(|n| !n.is_dead() && n.coord.held_epoch(REF).is_some())
}

pub(crate) async fn wait_for_leader(net: &Net) -> Arc<Node> {
    for _ in 0..10_000 {
        if let Some(l) = leader(net) {
            return l.clone();
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    panic!("no server took the lease");
}

pub(crate) async fn lease_now(w: &World) -> Lease {
    w.meta
        .lease(
            &LeaseKey::land(REF),
            LeaseTiming {
                ttl: Duration::from_secs(2),
                stale_after: Duration::from_secs(2),
            },
        )
        .await
        .unwrap()
        .expect("a lease row")
}

pub(crate) async fn seed_main(seed: u64, w: &World) {
    let base = commit_file(seed, &w.clean, None, "base.txt", "base\n", "base").await;
    w.meta.cas_ref(REF, None, Some(base)).await.unwrap();
}

/// With no faults, servers never race: one takes the lease, the others
/// forward, and every land is answered on its batch's first attempt.
#[tokio::test(start_paused = true)]
async fn coordinated_servers_never_race_the_swap() {
    let seed = 1;
    let w = world(seed);
    let nodes: Vec<_> = (0..3)
        .map(|i| node(&w, &w.plan, i, format!("node-{i}"), |_| {}))
        .collect();
    let net = net(nodes, seed, 0, 0);
    let (agents, seen) = join(run_agents(seed, &w, &net, 24).await).await;
    assert_lands_kept(seed, &w, &agents).await;
    for (a, s) in agents.iter().zip(&seen) {
        assert_eq!(s.max_attempts, 1, "agent {} lost a swap", a.n);
        assert!(s.errors.is_empty(), "agent {}: {:?}", a.n, s.errors);
    }
    for n in &net.nodes {
        assert_eq!(n.coord.uncoordinated_lands(), 0);
    }
    let lease = lease_now(&w).await;
    assert_eq!(lease.epoch, 1, "the lease never changed hands");
}

/// Forwards to a live leader lose their answers or are reset: the follower
/// asks the leader again and never lands on its own, the retried land
/// finds its first landing, and no other land in the batch is failed.
#[tokio::test(start_paused = true)]
async fn follower_timeouts_with_a_live_leader_land_once() {
    for seed in 1..=8 {
        let w = world(seed);
        let nodes: Vec<_> = (0..3)
            .map(|i| node(&w, &w.plan, i, format!("node-{i}"), |_| {}))
            .collect();
        let net = net(nodes, seed, 100, 300);
        let (agents, seen) = join(run_agents(seed, &w, &net, 24).await).await;
        assert!(
            net.lost.load(Ordering::Relaxed) > 0,
            "seed {seed}: no answer was lost"
        );
        assert_lands_kept(seed, &w, &agents).await;
        for (a, s) in agents.iter().zip(&seen) {
            assert!(
                s.errors.is_empty(),
                "seed {seed}: agent {} was failed: {:?}",
                a.n,
                s.errors
            );
        }
        for n in &net.nodes {
            assert_eq!(
                n.coord.uncoordinated_lands(),
                0,
                "seed {seed}: a server landed without the lease"
            );
        }
        assert_eq!(lease_now(&w).await.epoch, 1, "seed {seed}");
    }
}

/// The leader stops for longer than its lease (a pause, a long stall) with
/// a batch half done. Another server takes the expired lease and lands
/// everything; when the old leader resumes, its swap loses or is harmless,
/// and it hands its lands back instead of retrying.
#[tokio::test(start_paused = true)]
async fn a_leader_paused_past_its_lease_hands_over() {
    for seed in 1..=6 {
        let w = world(seed);
        // Swaps take a while, so the pause lands mid-batch.
        w.plan.rate(
            &[Op::CasRef],
            1000,
            Fault::Delay(Duration::from_millis(100)),
        );
        let nodes: Vec<_> = (0..3)
            .map(|i| node(&w, &w.plan, i, format!("node-{i}"), |_| {}))
            .collect();
        let net = net(nodes, seed, 0, 0);
        let tasks = run_agents(seed, &w, &net, 12).await;
        let old = wait_for_leader(&net).await;
        tokio::time::sleep(Duration::from_millis(seed * 5)).await;
        old.set_paused(true);
        tokio::time::sleep(Duration::from_secs(4)).await;
        old.set_paused(false);
        let (agents, _) = join(tasks).await;
        w.plan.pause();
        assert_lands_kept(seed, &w, &agents).await;
        let lease = lease_now(&w).await;
        assert!(lease.epoch >= 2, "seed {seed}: another server took over");
        assert_ne!(lease.holder, old.coord.config().holder, "seed {seed}");
    }
}

/// The leader is killed at a random point of a batch. Its lease is neither
/// renewed nor released; another server takes it once it goes stale, and
/// every agent lands within the lease time plus two seconds of the kill.
#[tokio::test(start_paused = true)]
async fn a_killed_leader_is_replaced_within_the_lease_time() {
    for seed in seeds() {
        let w = world(seed);
        w.plan
            .jitter(3)
            .rate(&[Op::CasRef], 1000, Fault::Delay(Duration::from_millis(30)))
            .rate(Op::META, 20, Fault::Delay(Duration::from_millis(5)));
        let nodes: Vec<_> = (0..3)
            .map(|i| node(&w, &w.plan, i, format!("node-{i}"), |_| {}))
            .collect();
        let net = net(nodes, seed, 0, 0);
        let tasks = run_agents(seed, &w, &net, 12).await;
        let old = wait_for_leader(&net).await;
        let mut rng = Rng::new(seed);
        tokio::time::sleep(Duration::from_millis(rng.below(60) as u64)).await;
        old.kill();
        let killed = Instant::now();
        let ttl = old.coord.config().ttl;
        let (agents, seen) = join(tasks).await;
        assert_lands_kept(seed, &w, &agents).await;
        for (a, s) in agents.iter().zip(&seen) {
            let at = s.landed_at.expect("landed");
            assert!(
                at <= killed + ttl + Duration::from_secs(2),
                "seed {seed}: agent {} landed {:?} after the kill",
                a.n,
                at.duration_since(killed)
            );
        }
        let lease = lease_now(&w).await;
        assert_ne!(lease.holder, old.coord.config().holder, "seed {seed}");
    }
}

/// A leader that advertises an address nobody can reach keeps its lease:
/// followers are not allowed to take a live lease, so it never flaps. They
/// give up with 409 instead of landing on their own, and the leader's own
/// lands go on.
#[tokio::test(start_paused = true)]
async fn a_wrong_advertise_url_does_not_flap_the_lease() {
    let seed = 3;
    let w = world(seed);
    seed_main(seed, &w).await;
    let wait = Duration::from_secs(3);
    let nodes: Vec<_> = (0..3)
        .map(|i| {
            let address = if i == 0 {
                UNREACHABLE.to_string()
            } else {
                format!("node-{i}")
            };
            node(&w, &w.plan, i, address, |c| c.forward_wait = wait)
        })
        .collect();
    let net = net(nodes, seed, 0, 0);
    let bad = net.nodes[0].clone();
    // The misconfigured server takes the lease with its own land.
    let first = commit_file(
        seed,
        &w.clean,
        w.meta.get_ref(REF).await.unwrap(),
        "x",
        "x\n",
        "x",
    )
    .await;
    match bad
        .repo
        .land_coordinated(first, REF, None, true, None, false, &NoForward)
        .await
        .unwrap()
    {
        Coordinated::Landed(LandResponse::Landed { .. }) => {}
        _ => panic!("the first land is the leader's"),
    }
    assert_eq!(bad.coord.held_epoch(REF), Some(1));
    let head = w.meta.get_ref(REF).await.unwrap();
    let other = commit_file(seed, &w.clean, head, "y", "y\n", "y").await;
    let follower = net.nodes[1].clone();
    let started = Instant::now();
    let r = follower
        .repo
        .land_coordinated(
            other,
            REF,
            None,
            true,
            None,
            false,
            &Fwd {
                net: net.clone(),
                cs: other,
                land_id: None,
            },
        )
        .await;
    assert!(
        matches!(r, Err(Error::Conflict(_))),
        "gave up with 409, not {:?}",
        r.as_ref().map(|c| match c {
            Coordinated::Landed(l) => format!("landed here {l:?}"),
            Coordinated::Forwarded(f) =>
                format!("forwarded {:?}", f.as_ref().map_err(|e| e.to_string())),
            Coordinated::NotLeader => "not leader".into(),
        })
    );
    assert!(started.elapsed() >= wait);
    assert_eq!(w.meta.get_ref(REF).await.unwrap(), head, "nothing landed");
    assert_eq!(follower.coord.uncoordinated_lands(), 0);
    let lease = lease_now(&w).await;
    assert_eq!((lease.holder.as_str(), lease.epoch), ("holder-0", 1));
    // The leader still lands its own.
    let mine = commit_file(seed, &w.clean, head, "z", "z\n", "z").await;
    assert!(matches!(
        bad.repo
            .land_coordinated(mine, REF, None, true, None, false, &NoForward)
            .await
            .unwrap(),
        Coordinated::Landed(LandResponse::Landed { .. })
    ));
}

/// A server that shuts down gives its lease up, so the next land elsewhere
/// takes it at once instead of waiting for it to expire; a server idle on a
/// ref releases it after `idle_release`.
#[tokio::test(start_paused = true)]
async fn leases_are_released_on_shutdown_and_when_idle() {
    let seed = 2;
    let w = world(seed);
    seed_main(seed, &w).await;
    let a = node(&w, &w.plan, 0, "node-0".into(), |_| {});
    let b = node(&w, &w.plan, 1, "node-1".into(), |_| {});
    let net = net(vec![a.clone(), b.clone()], seed, 0, 0);
    let land = |n: Arc<Node>, file: &'static str| {
        let w_clean = w.clean.clone();
        let net = net.clone();
        async move {
            let head = w_clean.meta().get_ref(REF).await.unwrap();
            let cs = commit_file(seed, &w_clean, head, file, "f\n", file).await;
            let fwd = Fwd {
                net,
                cs,
                land_id: None,
            };
            match n
                .repo
                .land_coordinated(cs, REF, None, true, None, false, &fwd)
                .await
                .unwrap()
            {
                Coordinated::Landed(_) => "here",
                Coordinated::Forwarded(_) => "forwarded",
                Coordinated::NotLeader => "not leader",
            }
        }
    };
    assert_eq!(land(a.clone(), "a1").await, "here");
    assert_eq!(land(b.clone(), "b1").await, "forwarded");
    let t = Instant::now();
    a.coord.shutdown().await;
    // A server that gave its lease up never batches a land itself again; the
    // client sends it elsewhere.
    assert_eq!(land(a.clone(), "a2").await, "not leader");
    assert_eq!(land(b.clone(), "b2").await, "here");
    assert!(t.elapsed() < Duration::from_millis(100), "taken at once");
    assert_eq!(lease_now(&w).await.epoch, 2);

    tokio::time::sleep(b.coord.config().idle_release + Duration::from_secs(2)).await;
    assert_eq!(b.coord.held_epoch(REF), None, "released when idle");
    assert!(!lease_now(&w).await.is_live());
}

/// Reads of the lease fail on every server but the leader. Lands keep going
/// to the leader on the last lease each server read, and none is batched
/// anywhere else (H1).
#[tokio::test(start_paused = true)]
async fn failing_lease_reads_keep_lands_on_the_leader() {
    let mut failed_reads = 0;
    for seed in seeds() {
        let w = world(seed);
        seed_main(seed, &w).await;
        // Swaps take a while, so the lands span many lease reads.
        w.plan.rate(
            &[Op::CasRef],
            1000,
            Fault::Delay(Duration::from_millis(300)),
        );
        let flaky = FaultPlan::new(seed);
        flaky.pause();
        flaky.rate(&[Op::Lease], 500, Fault::Fail);
        let nodes: Vec<_> = (0..3)
            .map(|i| {
                let plan = if i == 0 { &w.plan } else { &flaky };
                node(&w, plan, i, format!("node-{i}"), |_| {})
            })
            .collect();
        let net = net(nodes, seed, 0, 0);
        // The leader takes the lease, and every server reads it once.
        for (i, n) in net.nodes.iter().enumerate() {
            let head = w.meta.get_ref(REF).await.unwrap();
            let file = format!("warm-{i}");
            let cs = commit_file(seed, &w.clean, head, &file, "w\n", &file).await;
            let fwd = Fwd {
                net: net.clone(),
                cs,
                land_id: None,
            };
            n.repo
                .land_coordinated(cs, REF, None, true, None, false, &fwd)
                .await
                .unwrap();
        }
        assert_eq!(net.nodes[0].coord.held_epoch(REF), Some(1));
        flaky.resume();
        let mut tasks = Vec::new();
        for n in 0..24 {
            tasks.push(tokio::spawn(agent(
                seed,
                net.clone(),
                w.clean.clone(),
                n,
                1,
            )));
        }
        for t in tasks {
            t.await.unwrap();
        }
        failed_reads += flaky.injected().total();
        for n in &net.nodes {
            assert_eq!(
                n.coord.uncoordinated_lands(),
                0,
                "seed {seed}: a land was batched without the lease"
            );
        }
        assert_eq!(lease_now(&w).await.epoch, 1, "seed {seed}");
    }
    // How many reads a seed makes depends on scheduling; across the sweep
    // the case must have been exercised.
    assert!(failed_reads > 0, "no lease read failed");
}

/// Store faults, lost and reset forwards, and a leader killed or paused at
/// a random point, over the usual seed sweep.
#[tokio::test(start_paused = true)]
async fn coordinated_lands_under_faults_lose_nothing_and_duplicate_nothing() {
    for seed in seeds() {
        let w = world(seed);
        let ten_min = Duration::from_secs(600);
        w.plan
            .jitter(3)
            .rate(&[Op::CasRef], 100, Fault::FailAfter)
            .rate(&[Op::CasRef], 50, Fault::Fail)
            .rate(&[Op::CasRef], 20, Fault::Stall(ten_min))
            .rate(&[Op::CasRef], 300, Fault::Delay(Duration::from_millis(20)))
            .rate(Op::WRITES, 10, Fault::FailAfter)
            .rate(Op::WRITES, 10, Fault::Fail)
            .rate(Op::META, 3, Fault::Fail)
            .rate(&[Op::Lease], 20, Fault::Fail)
            .rate(&[Op::Lease], 10, Fault::Delay(Duration::from_millis(300)));
        let nodes: Vec<_> = (0..3)
            .map(|i| node(&w, &w.plan, i, format!("node-{i}"), |_| {}))
            .collect();
        let net = net(nodes, seed, 30, 60);
        let tasks = run_agents(seed, &w, &net, 12).await;
        let mut rng = Rng::new(seed ^ 0x1EAD);
        let event = rng.below(3);
        if event > 0 {
            let old = wait_for_leader(&net).await;
            tokio::time::sleep(Duration::from_millis(rng.below(80) as u64)).await;
            if event == 1 {
                old.kill();
            } else {
                old.set_paused(true);
                tokio::time::sleep(Duration::from_secs(3)).await;
                old.set_paused(false);
            }
        }
        let (agents, _) = join(tasks).await;
        w.plan.pause();
        assert_lands_kept(seed, &w, &agents).await;
    }
}

/// ADR 0059 §10: lands forwarded to the ref's leader while a changeset
/// sweep collects the stacks they land. Every stack is condemned first, as
/// a sweep finds abandoned work; each land then either rescues its stack in
/// the leader's batch and lands, or finds it deleted and lands nothing. No
/// ref ever points at a deleted changeset, and no stored changeset loses a
/// parent.
#[tokio::test(start_paused = true)]
async fn sweep_racing_a_forwarded_land() {
    use crate::changeset_sweep::{self, SweepOptions};
    for seed in seeds() {
        let w = world(seed);
        seed_main(seed, &w).await;
        let base = w.meta.get_ref(REF).await.unwrap();
        let mut stacks = Vec::new();
        for n in 0..6 {
            let a = commit_file(seed, &w.clean, base, &format!("s{n}/a"), "a\n", "a").await;
            let b = commit_file(seed, &w.clean, Some(a), &format!("s{n}/b"), "b\n", "b").await;
            stacks.push((a, b));
        }
        let t = leep_core::store::unix_now() + 10 * 86_400;
        let all: Vec<ChangesetId> = stacks.iter().flat_map(|(a, b)| [*a, *b]).collect();
        crate::changeset_gc::condemn_for_test(&w.clean, &all, t).await;
        w.plan
            .jitter(2)
            .rate(Op::META, 3, Fault::Fail)
            .rate(Op::WRITES, 5, Fault::FailAfter);
        let nodes: Vec<_> = (0..3)
            .map(|i| node(&w, &w.plan, i, format!("node-{i}"), |_| {}))
            .collect();
        let net = net(nodes, seed, 0, 0);
        let lands: Vec<_> = stacks
            .iter()
            .enumerate()
            .map(|(n, (_, top))| {
                let (net, top) = (net.clone(), *top);
                // Through every server in turn: whichever does not hold
                // the lease forwards to the one that does.
                let via = net.nodes[n % net.nodes.len()].clone();
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_millis(n as u64 * 3)).await;
                    for _ in 0..200 {
                        let fwd = Fwd {
                            net: net.clone(),
                            cs: top,
                            land_id: None,
                        };
                        let r = via
                            .call(
                                via.repo
                                    .land_coordinated(top, REF, None, true, None, false, &fwd),
                            )
                            .await;
                        let answer = match r {
                            Some(Ok(Coordinated::Landed(r))) => Ok(r),
                            Some(Ok(Coordinated::Forwarded(r))) => r,
                            Some(Ok(Coordinated::NotLeader)) => panic!("NotLeader to a client"),
                            Some(Err(e)) => Err(e),
                            None => continue,
                        };
                        match answer {
                            Ok(LandResponse::Landed { landed, .. }) => return Some(landed),
                            Ok(other) => panic!("seed {seed}: {other:?}"),
                            Err(e) if retryable(&e) => {}
                            // Deleted before the land rescued it.
                            Err(Error::NotFound(_) | Error::Invalid(_)) => return None,
                            Err(e) => panic!("seed {seed}: land failed for real: {e}"),
                        }
                    }
                    None
                })
            })
            .collect();
        let o = SweepOptions {
            grace: Duration::from_secs(86_400),
            batch: 1,
            rate: 0,
            ..SweepOptions::default()
        };
        let clock_t = t + o.fence.as_secs() as i64 + 1;
        // Collect through a store that yields between calls, starting at a
        // seeded moment among the lands.
        let gc_plan = FaultPlan::new(seed ^ 0x9C);
        gc_plan.jitter(3);
        let gc_repo = w.server_with(&gc_plan);
        let start = Rng::new(seed).below(20) as u64;
        let collect = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(start)).await;
            let clock = crate::fault_tests::TestClock::at(clock_t);
            changeset_sweep::collect(&gc_repo, &o, &clock).await
        });
        let collected = collect
            .await
            .unwrap()
            .unwrap_or_else(|e| panic!("seed {seed}: collect: {e}"));
        let mut landed = Vec::new();
        for l in lands {
            landed.push(l.await.unwrap());
        }
        w.plan.pause();
        let head = w.meta.get_ref(REF).await.unwrap().unwrap();
        for ((a, b), got) in stacks.iter().zip(&landed) {
            match got {
                Some(id) => {
                    assert!(w.clean.is_ancestor(*id, head).await.unwrap(), "seed {seed}");
                }
                None => assert!(
                    w.meta.get_changeset(b).await.unwrap().is_none(),
                    "seed {seed}: {b} still stored but its land gave up ({collected:?})"
                ),
            }
            if w.meta.get_changeset(b).await.unwrap().is_some() {
                assert!(
                    w.meta.get_changeset(a).await.unwrap().is_some(),
                    "seed {seed}"
                );
            }
        }
        crate::fault_tests::assert_ref_sound(seed, &w.clean, REF).await;
        crate::fault_tests::assert_ref_log_replays(seed, &*w.meta).await;
        for id in w.meta.list_changesets(None, usize::MAX).await.unwrap() {
            for p in w.meta.get_changeset(&id).await.unwrap().unwrap().parents {
                assert!(
                    w.meta.get_changeset(&p).await.unwrap().is_some(),
                    "seed {seed}: {id} lost its parent {p}"
                );
            }
        }
    }
}
