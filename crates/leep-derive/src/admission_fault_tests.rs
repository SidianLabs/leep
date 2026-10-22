//! Land admission per ref (ADR 0060, section 2) under the fault sweep's
//! checks: nothing an agent was told landed is lost, nothing lands twice,
//! and the ref log replays. A land refused at a full queue did nothing, so
//! agents send it again after its `Retry-After`, keyed or not.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use leep_core::api::{LandResponse, REFUSED_ADMISSION};
use leep_core::{ChangesetId, Error, LandId, Lease, MetaStore};
use leep_testkit::{is_injected, Fault, Op, Rng};
use tokio::time::Instant;

use super::{assert_lands_kept, assert_ref_log_replays, commit_file, retry, seeds, Agent, World};
use crate::land_coord::{Coordinated, Forwarded, LandForward};
use crate::land_coord_tests::{
    bounded_node, leader, lease_now, net, seed_main, wait_for_leader, world, Fwd, Net, NoForward,
    Node, REF,
};
use crate::Repo;

/// The wait a refusal asks for, or `None` for any other answer. Every
/// refusal must be an admission refusal with a wait inside its bounds.
fn refused_wait(e: &Error) -> Option<Duration> {
    let Error::Refused {
        refused,
        retry_after_ms,
        message,
    } = e
    else {
        return None;
    };
    assert_eq!(*refused, REFUSED_ADMISSION, "{message}");
    assert!(
        (1_000..=10_000).contains(retry_after_ms),
        "Retry-After {retry_after_ms} ms"
    );
    assert!(message.contains("nothing was done"), "{message}");
    Some(Duration::from_millis(*retry_after_ms))
}

/// The wait plus up to half of it again, as `leep land` does.
fn jittered(wait: Duration, rng: &mut Rng) -> Duration {
    wait.mul_f64(1.0 + rng.below(1000) as f64 / 2000.0)
}

/// Answers a client sends again besides a refusal: injected store faults
/// and a land that kept losing or ran past its batch's lifetime (409).
fn retryable(e: &Error) -> bool {
    is_injected(e) || matches!(e, Error::Conflict(_))
}

/// Commit a stack of `steps` files on `main` and land it through `repo`
/// until it is answered with success: resending after a refusal (waiting
/// its `Retry-After`), a fault or a timeout. Half the agents name their
/// land. Returns what it was told and how often it was refused.
async fn admitted_agent(seed: u64, repo: Repo, n: usize, steps: usize) -> (Agent, u64) {
    let mut parent = retry(seed, "read main", || repo.meta().get_ref(REF)).await;
    let mut files = Vec::new();
    for k in 0..steps {
        let path = format!("agent-{n}/step-{k}.txt");
        let body = format!("agent {n} step {k}\n");
        parent = Some(commit_file(seed, &repo, parent, &path, &body, &format!("a{n}s{k}")).await);
        files.push((path, body));
    }
    let top = parent.expect("at least one step");
    let land_id = n
        .is_multiple_of(2)
        .then(|| LandId::from_bytes(&[n as u8 + 1; 16]).unwrap());
    let mut rng = Rng::new(seed ^ (n as u64 + 1));
    let mut refusals = 0;
    for _ in 0..2000 {
        let land = repo.land_keyed(top, REF, None, true, land_id.clone());
        match tokio::time::timeout(Duration::from_secs(60), land).await {
            Ok(Ok(LandResponse::Landed { landed, .. })) => {
                return (
                    Agent {
                        n,
                        files,
                        reported: vec![landed],
                    },
                    refusals,
                );
            }
            Ok(Ok(LandResponse::Conflicts { .. })) => {
                panic!("seed {seed}: disjoint files conflict")
            }
            Ok(Err(e)) => match refused_wait(&e) {
                Some(wait) => {
                    refusals += 1;
                    tokio::time::sleep(jittered(wait, &mut rng)).await;
                }
                None if retryable(&e) => {}
                None => panic!("seed {seed}: agent {n}: land failed for real: {e}"),
            },
            Err(_) => {}
        }
    }
    panic!("seed {seed}: agent {n} never landed");
}

/// Lands refused at a full queue, then sent again under their ids (or
/// without one), through two servers whose batches lose CAS races to each
/// other and fail their commits. A refused land does nothing: nothing is
/// lost or landed twice, and the ref log replays.
async fn refused_lands_under_faults(seed: u64) -> u64 {
    let w = World::new(seed);
    w.plan
        .jitter(3)
        .rate(&[Op::CasRef], 100, Fault::FailAfter)
        .rate(&[Op::CasRef], 60, Fault::Fail)
        .rate(&[Op::CasRef], 600, Fault::Delay(Duration::from_millis(40)))
        .rate(Op::WRITES, 10, Fault::FailAfter)
        .rate(Op::WRITES, 10, Fault::Fail)
        .rate(Op::META, 3, Fault::Fail)
        .rate(Op::BLOB, 3, Fault::Fail);
    let servers: Vec<Repo> = (0..2)
        .map(|_| w.server().with_land_queue_batches(Some(1)))
        .collect();
    let mut rng = Rng::new(seed ^ 0xAD31);
    let tasks: Vec<_> = (0..16)
        .map(|n| {
            let repo = servers[n % servers.len()].clone();
            tokio::spawn(admitted_agent(seed, repo, n, 1 + rng.below(2)))
        })
        .collect();
    let mut agents = Vec::new();
    let mut refusals = 0;
    for t in tasks {
        let (a, r) = t.await.unwrap();
        agents.push(a);
        refusals += r;
    }
    w.plan.pause();
    assert_lands_kept(seed, &w, &agents).await;
    refusals
}

#[tokio::test(start_paused = true)]
async fn admission_refused_lands_nothing() {
    let mut refusals = 0;
    for seed in seeds() {
        refusals += refused_lands_under_faults(seed).await;
    }
    // How many are refused on a seed depends on scheduling; across the
    // sweep the case must have been exercised.
    assert!(refusals > 0, "no land was refused");
}

/// `main` with one changeset, and `n` changesets on it that change
/// disjoint files.
async fn lands_to_make(w: &World, n: usize) -> Vec<ChangesetId> {
    seed_main(1, w).await;
    let base = w.meta.get_ref(REF).await.unwrap();
    let mut out = Vec::new();
    for i in 0..n {
        let f = format!("f{i}.txt");
        out.push(commit_file(1, &w.clean, base, &f, "x\n", &f).await);
    }
    out
}

/// Hold the next ref swap for `hold`, start a land of `cs` through `repo`,
/// and wait until its batch is replaying (its swap entered).
async fn hold_a_batch(
    w: &World,
    repo: &Repo,
    cs: ChangesetId,
    hold: Duration,
) -> tokio::task::JoinHandle<leep_core::Result<LandResponse>> {
    let swaps = w.plan.calls(Op::CasRef);
    w.plan.script(Op::CasRef, swaps + 1, Fault::Delay(hold));
    let r = repo.clone();
    let task = tokio::spawn(async move { r.land(cs, REF).await });
    while w.plan.calls(Op::CasRef) == swaps {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    task
}

/// Spawn lands of `ids` through `repo` and wait until all are queued.
async fn queue_lands(
    repo: &Repo,
    ids: &[ChangesetId],
) -> Vec<tokio::task::JoinHandle<leep_core::Result<LandResponse>>> {
    let before = repo.land_queues.pending(REF);
    let tasks = ids
        .iter()
        .map(|cs| {
            let (r, cs) = (repo.clone(), *cs);
            tokio::spawn(async move { r.land(cs, REF).await })
        })
        .collect();
    while repo.land_queues.pending(REF) < before + ids.len() {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    tasks
}

fn landed(r: &LandResponse) -> ChangesetId {
    match r {
        LandResponse::Landed { landed, .. } => *landed,
        other => panic!("{other:?}"),
    }
}

/// Every meta call the plan has counted.
fn meta_calls(w: &World) -> u64 {
    Op::META.iter().map(|op| w.plan.calls(*op)).sum()
}

/// A queue at its bound refuses new lands at once, without reading the
/// store, and drops none of the lands it holds: they all land once the
/// batch ahead of them commits. A land that joins a batch still filling is
/// admitted. With the bound off the same land queues.
#[tokio::test(start_paused = true)]
async fn queued_lands_are_never_dropped_and_off_admits_every_land() {
    const FULL: usize = crate::land::MAX_BATCH;
    for bound in [Some(2), None] {
        let w = World::new(1);
        let ids = lands_to_make(&w, FULL + 2).await;
        let server = w.server().with_land_queue_batches(bound);
        let held = hold_a_batch(&w, &server, ids[0], Duration::from_secs(5)).await;
        // One batch replaying and one waiting, filled one land at a time:
        // each joins the waiting batch, so none is refused.
        let mut queued = Vec::new();
        for cs in &ids[1..=FULL] {
            queued.extend(queue_lands(&server, std::slice::from_ref(cs)).await);
        }
        assert_eq!(server.land_queues.batches(REF), 2);

        let calls = meta_calls(&w);
        let started = Instant::now();
        let last = server.land(ids[FULL + 1], REF).await;
        match bound {
            Some(_) => {
                let e = last.expect_err("refused at the bound");
                assert_eq!(refused_wait(&e), Some(Duration::from_secs(2)), "{e}");
                assert_eq!(started.elapsed(), Duration::ZERO, "refused at once");
                assert_eq!(meta_calls(&w), calls, "an unkeyed refusal reads nothing");
            }
            None => assert!(matches!(last, Ok(LandResponse::Landed { .. }))),
        }

        let mut heads = vec![landed(&held.await.unwrap().unwrap())];
        for t in queued {
            heads.push(landed(&t.await.unwrap().unwrap()));
        }
        let head = w.meta.get_ref(REF).await.unwrap().unwrap();
        for h in &heads {
            assert!(w.clean.is_ancestor(*h, head).await.unwrap(), "{bound:?}");
        }
        let lands = w
            .meta
            .ref_log(Some(REF), None, 100)
            .await
            .unwrap()
            .into_iter()
            .filter(|e| e.operation == "land")
            .count();
        assert_eq!(lands, 1 + FULL + usize::from(bound.is_none()), "{bound:?}");
        assert_eq!(server.land_queues.batches(REF), 0, "the queue drained");
        assert_ref_log_replays(1, &*w.meta).await;
    }
}

/// A keyed land refused at a full queue whose first send landed gets the
/// recorded answer, not a 503, with one `find_lands` and nothing more; an
/// id the ref recorded for another changeset is refused as reused; a new
/// id is refused and records nothing.
#[tokio::test(start_paused = true)]
async fn admission_answers_recorded_land() {
    let w = World::new(2);
    let ids = lands_to_make(&w, 4).await;
    let server = w.server().with_land_queue_batches(Some(1));
    let id = LandId::from_bytes(&[9; 16]).unwrap();
    let first = server
        .land_keyed(ids[0], REF, None, true, Some(id.clone()))
        .await
        .unwrap();

    let held = hold_a_batch(&w, &server, ids[1], Duration::from_secs(5)).await;
    assert_eq!(server.land_queues.batches(REF), 1, "full at a bound of 1");
    let head = w.meta.get_ref(REF).await.unwrap();
    let entries = w.meta.ref_log(Some(REF), None, 100).await.unwrap().len();

    let (calls, finds) = (meta_calls(&w), w.plan.calls(Op::FindLands));
    let again = server
        .land_keyed(ids[0], REF, None, true, Some(id.clone()))
        .await
        .unwrap();
    assert_eq!(landed(&again), landed(&first));
    assert_eq!(w.plan.calls(Op::FindLands), finds + 1);
    assert_eq!(meta_calls(&w), calls + 1, "one read: the land id");

    let reused = server
        .land_keyed(ids[2], REF, None, true, Some(id))
        .await
        .unwrap_err();
    assert!(
        matches!(reused, Error::LandIdReused { requested, .. } if requested == ids[2]),
        "{reused}"
    );

    let fresh = LandId::from_bytes(&[10; 16]).unwrap();
    let refused = server
        .land_keyed(ids[3], REF, None, true, Some(fresh.clone()))
        .await
        .unwrap_err();
    assert!(refused_wait(&refused).is_some(), "{refused}");
    assert_eq!(w.meta.get_ref(REF).await.unwrap(), head, "nothing moved");
    assert_eq!(
        w.meta.ref_log(Some(REF), None, 100).await.unwrap().len(),
        entries
    );
    assert_eq!(
        w.meta
            .find_lands(REF, std::slice::from_ref(&fresh))
            .await
            .unwrap(),
        vec![None],
        "a refused land records nothing"
    );

    held.await.unwrap().unwrap();
    // The id stays free: the resend lands.
    let resent = server
        .land_keyed(ids[3], REF, None, true, Some(fresh.clone()))
        .await
        .unwrap();
    let record =
        w.meta.find_lands(REF, &[fresh]).await.unwrap()[0].expect("recorded with the move");
    assert_eq!(record.landed, landed(&resent));
    assert_ref_log_replays(2, &*w.meta).await;
}

/// Counts the forwards a follower makes.
struct Counting {
    inner: Fwd,
    sent: AtomicUsize,
}

#[async_trait]
impl LandForward<leep_core::Result<LandResponse>> for Counting {
    async fn forward(&self, lease: &Lease) -> Forwarded<leep_core::Result<LandResponse>> {
        self.sent.fetch_add(1, Ordering::SeqCst);
        self.inner.forward(lease).await
    }
}

/// A forwarded land counts against the leader's bound. The follower relays
/// the leader's refusal as an answered forward, unchanged, after one
/// forward: it never batches the land itself and never treats the refusal
/// as a failed forward to retry.
#[tokio::test(start_paused = true)]
async fn forwarded_land_refused_by_full_leader() {
    let seed = 5;
    let w = world(seed);
    let ids = lands_to_make(&w, 3).await;
    let nodes: Vec<_> = (0..2)
        .map(|i| bounded_node(&w, &w.plan, i, format!("node-{i}"), |_| {}, Some(1)))
        .collect();
    let net = net(nodes, seed, 0, 0);
    let (leader, follower) = (net.nodes[0].clone(), net.nodes[1].clone());
    // The leader takes the lease with a land of its own, then holds a batch.
    match leader
        .repo
        .land_coordinated(ids[0], REF, None, true, None, false, &NoForward)
        .await
        .unwrap()
    {
        Coordinated::Landed(LandResponse::Landed { .. }) => {}
        _ => panic!("the first land is the leader's"),
    }
    let swaps = w.plan.calls(Op::CasRef);
    w.plan
        .script(Op::CasRef, swaps + 1, Fault::Delay(Duration::from_secs(5)));
    let held = {
        let leader = leader.clone();
        let cs = ids[1];
        tokio::spawn(async move {
            leader
                .repo
                .land_coordinated(cs, REF, None, true, None, false, &NoForward)
                .await
        })
    };
    while leader.repo.land_queues.batches(REF) == 0 || w.plan.calls(Op::CasRef) == swaps {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }

    let fwd = Counting {
        inner: Fwd {
            net: net.clone(),
            cs: ids[2],
            land_id: None,
        },
        sent: AtomicUsize::new(0),
    };
    let started = Instant::now();
    let r = follower
        .repo
        .land_coordinated(ids[2], REF, None, true, None, false, &fwd)
        .await
        .unwrap();
    let Coordinated::Forwarded(Err(e)) = r else {
        panic!("not relayed as the leader's refusal");
    };
    assert!(refused_wait(&e).is_some(), "{e}");
    assert_eq!(started.elapsed(), Duration::ZERO, "refused at once");
    assert_eq!(fwd.sent.load(Ordering::SeqCst), 1, "forwarded once");
    assert_eq!(follower.coord.uncoordinated_lands(), 0);
    assert_eq!(
        follower.repo.land_queues.batches(REF),
        0,
        "never batched here"
    );
    assert_eq!(follower.repo.land_queues.pending(REF), 0);

    held.await.unwrap().unwrap();
    // Resent once the leader has room, it lands, through the leader.
    let r = follower
        .repo
        .land_coordinated(ids[2], REF, None, true, None, false, &fwd)
        .await
        .unwrap();
    assert!(matches!(
        r,
        Coordinated::Forwarded(Ok(LandResponse::Landed { .. }))
    ));
    assert_eq!(follower.coord.uncoordinated_lands(), 0);
    assert_eq!(lease_now(&w).await.epoch, 1);
    assert_ref_log_replays(seed, &*w.meta).await;
}

/// What a coordinated agent saw.
#[derive(Default)]
struct Seen {
    refusals: u64,
    landed_at: Option<Instant>,
}

/// Commit a stack and land it through server `n % servers`, moving to the
/// next live server when one dies and resending after a refusal, until a
/// land is answered with success.
async fn coordinated_agent(seed: u64, net: Arc<Net>, w_clean: Repo, n: usize) -> (Agent, Seen) {
    let mut parent = retry(seed, "read main", || w_clean.meta().get_ref(REF)).await;
    let mut files = Vec::new();
    for k in 0..1 + n % 2 {
        let path = format!("agent-{n}/step-{k}.txt");
        let body = format!("agent {n} step {k}\n");
        parent =
            Some(commit_file(seed, &w_clean, parent, &path, &body, &format!("a{n}s{k}")).await);
        files.push((path, body));
    }
    let top = parent.expect("at least one step");
    let land_id = n.is_multiple_of(2).then(|| {
        let mut id = [0u8; 16];
        id[..8].copy_from_slice(&seed.to_le_bytes());
        id[8..].copy_from_slice(&(n as u64).to_le_bytes());
        LandId::from_bytes(&id).unwrap()
    });
    let mut rng = Rng::new(seed ^ (n as u64 + 7));
    let mut seen = Seen::default();
    for t in 0..4000 {
        let node: &Arc<Node> = &net.nodes[(n + t) % net.nodes.len()];
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
            Ok(LandResponse::Landed { landed, .. }) => {
                seen.landed_at = Some(Instant::now());
                return (
                    Agent {
                        n,
                        files,
                        reported: vec![landed],
                    },
                    seen,
                );
            }
            Ok(LandResponse::Conflicts { .. }) => panic!("seed {seed}: disjoint files conflict"),
            Err(e) => match refused_wait(&e) {
                Some(wait) => {
                    seen.refusals += 1;
                    tokio::time::sleep(jittered(wait, &mut rng)).await;
                }
                None if retryable(&e)
                    || matches!(&e, Error::Other(m) if m.contains("abandoned")) => {}
                None => panic!("seed {seed}: agent {n}: land failed for real: {e}"),
            },
        }
    }
    panic!("seed {seed}: agent {n} never landed");
}

fn bounded_net(w: &World, seed: u64, bound: usize, drop: u32, lose: u32) -> Arc<Net> {
    let nodes: Vec<_> = (0..3)
        .map(|i| bounded_node(w, &w.plan, i, format!("node-{i}"), |_| {}, Some(bound)))
        .collect();
    net(nodes, seed, drop, lose)
}

/// Forwarded and direct lands refused by a full leader under store faults
/// and lost forwards: agents resend, nothing is batched off the leader, and
/// nothing is lost or landed twice.
#[tokio::test(start_paused = true)]
async fn forwarded_lands_refused_under_faults_land_once() {
    let mut refusals = 0;
    for seed in seeds() {
        let w = world(seed);
        w.plan
            .jitter(3)
            .rate(&[Op::CasRef], 60, Fault::FailAfter)
            .rate(&[Op::CasRef], 40, Fault::Fail)
            .rate(&[Op::CasRef], 600, Fault::Delay(Duration::from_millis(40)))
            .rate(Op::WRITES, 10, Fault::Fail)
            .rate(Op::META, 3, Fault::Fail);
        let net = bounded_net(&w, seed, 1, 20, 40);
        let tasks: Vec<_> = (0..16)
            .map(|n| tokio::spawn(coordinated_agent(seed, net.clone(), w.clean.clone(), n)))
            .collect();
        let mut agents = Vec::new();
        for t in tasks {
            let (a, s) = t.await.unwrap();
            agents.push(a);
            refusals += s.refusals;
        }
        w.plan.pause();
        assert_lands_kept(seed, &w, &agents).await;
        for n in &net.nodes {
            assert_eq!(
                n.coord.uncoordinated_lands(),
                0,
                "seed {seed}: a land was batched without the lease"
            );
        }
    }
    assert!(refusals > 0, "no land was refused");
}

/// Agents in [`leader_killed_with_full_queue`]: more than one batch holds.
const AGENTS_OVER_A_BATCH: usize = crate::land::MAX_BATCH + 8;

/// The leader is killed with its queue at the bound. Lands it held are
/// reset, lands it refused are resent after their wait, and another
/// server takes over once the lease goes stale: every agent lands within
/// the lease TTL plus two seconds of the takeover, through the new
/// leader's own admission.
#[tokio::test(start_paused = true)]
async fn leader_killed_with_full_queue() {
    for seed in seeds() {
        let w = world(seed);
        w.plan
            .jitter(3)
            .rate(&[Op::CasRef], 1000, Fault::Delay(Duration::from_millis(30)))
            .rate(Op::META, 20, Fault::Delay(Duration::from_millis(5)));
        // Two batches: one replaying and one full one waiting. More agents
        // than fit, so some are refused.
        let bound = 2;
        let net = bounded_net(&w, seed, bound, 0, 0);
        let tasks: Vec<_> = (0..AGENTS_OVER_A_BATCH)
            .map(|n| tokio::spawn(coordinated_agent(seed, net.clone(), w.clean.clone(), n)))
            .collect();
        let old = wait_for_leader(&net).await;
        // At the bound: a batch replaying and lands waiting.
        let mut waited = 0;
        while old.repo.land_queues.batches(REF) < bound {
            tokio::time::sleep(Duration::from_millis(1)).await;
            waited += 1;
            assert!(waited < 10_000, "seed {seed}: the queue never filled");
        }
        old.kill();
        let killed = Instant::now();
        let ttl = old.coord.config().ttl;
        // When another server first leads the ref.
        let takeover = {
            let net = net.clone();
            tokio::spawn(async move {
                loop {
                    if leader(&net).is_some() {
                        return Instant::now();
                    }
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            })
        };
        let (agents, seen): (Vec<_>, Vec<_>) = {
            let mut a = Vec::new();
            let mut s = Vec::new();
            for t in tasks {
                let (x, y) = t.await.unwrap();
                a.push(x);
                s.push(y);
            }
            (a, s)
        };
        assert_lands_kept(seed, &w, &agents).await;
        let lease = lease_now(&w).await;
        assert_ne!(lease.holder, old.coord.config().holder, "seed {seed}");
        let takeover = takeover.await.unwrap();
        assert!(takeover <= killed + ttl + ttl, "seed {seed}: no takeover");
        for (a, s) in agents.iter().zip(&seen) {
            let at = s.landed_at.expect("landed");
            assert!(
                at <= takeover + ttl + Duration::from_secs(2),
                "seed {seed}: agent {} landed {:?} after the takeover",
                a.n,
                at.saturating_duration_since(takeover)
            );
        }
    }
}
