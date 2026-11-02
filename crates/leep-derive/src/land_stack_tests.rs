//! Bounds on one land's replay (#158): a stack of unlanded changesets
//! longer than [`MAX_LAND_STACK`] is refused up front, and a replay that
//! runs past [`REPLAY_BUDGET`] answers that land alone with a retryable
//! past-deadline error, so it never holds the ref's queue for a whole batch
//! lifetime.

use std::time::Duration;

use leep_core::api::LandResponse;
use leep_core::memory::MemoryMetaStore;
use leep_core::{ChangesetId, Error, MetaStore};
use leep_testkit::{Fault, FaultPlan, Op};
use tokio::time::Instant;

use crate::fault_tests::{commit_file, World};
use crate::land::{MAX_BATCH_LIFETIME, MAX_LAND_STACK, REPLAY_BUDGET};
use crate::tests::{commit, repo};

/// A chain of `n` changesets on `from`, one file each.
async fn chain(r: &crate::Repo, from: ChangesetId, n: usize) -> ChangesetId {
    let mut tip = from;
    for i in 0..n {
        let path = format!("chain/{i}.txt");
        tip = commit(r, &[tip], &[(path.as_str(), Some("x\n"))], "c").await;
    }
    tip
}

/// A 5,000-deep unlanded stack onto a ref that moved since its base is
/// refused with a 400 that says to land it in pieces, without replaying
/// any of it; one over the cap is refused, and exactly the cap lands.
#[tokio::test]
async fn a_stack_longer_than_the_cap_is_refused() {
    let r = repo();
    let base = commit(&r, &[], &[("base.txt", Some("b\n"))], "base").await;
    r.meta().cas_ref("main", None, Some(base)).await.unwrap();
    let moved = commit(&r, &[base], &[("moved.txt", Some("m\n"))], "moved").await;
    r.meta()
        .cas_ref("main", Some(base), Some(moved))
        .await
        .unwrap();

    let long = chain(&r, base, 5_000).await;
    let err = r.land(long, "main").await.expect_err("too long");
    let Error::Invalid(msg) = &err else {
        panic!("{err}")
    };
    assert!(msg.contains("land it in pieces"), "{msg}");
    assert!(msg.contains(&MAX_LAND_STACK.to_string()), "{msg}");
    assert_eq!(r.meta().get_ref("main").await.unwrap(), Some(moved));

    // One over the cap is refused; exactly the cap lands.
    let log = r.log(long, None, 5_000).await.unwrap();
    let over = log[5_000 - MAX_LAND_STACK - 1].id;
    assert!(matches!(r.land(over, "main").await, Err(Error::Invalid(_))));
    let first = log[5_000 - MAX_LAND_STACK].id;
    let LandResponse::Landed { landed, .. } = r.land(first, "main").await.unwrap() else {
        panic!("a stack of exactly the cap lands")
    };
    assert_eq!(
        r.log(landed, None, 5_000).await.unwrap().len(),
        MAX_LAND_STACK + 2
    );
    assert_eq!(r.meta().get_ref("main").await.unwrap(), Some(landed));
}

/// Over a blob store whose every read takes 200 ms, a 300-deep stack needs
/// a minute or more to replay (each rebase reads trees). It answers past-deadline once its budget is
/// spent, well inside a batch's lifetime, and a small land queued behind
/// it lands without waiting the replay out.
#[tokio::test(start_paused = true)]
async fn a_slow_replay_answers_past_deadline_and_frees_the_queue() {
    let w = World::with_meta(1, MemoryMetaStore::new());
    let clean = &w.clean;
    let base = commit_file(1, clean, None, "base.txt", "b\n", "base").await;
    w.meta.cas_ref("main", None, Some(base)).await.unwrap();
    let moved = commit_file(1, clean, Some(base), "moved.txt", "m\n", "moved").await;
    w.meta
        .cas_ref("main", Some(base), Some(moved))
        .await
        .unwrap();
    let long = chain(clean, base, 300).await;
    let small = commit_file(1, clean, Some(moved), "small.txt", "s\n", "small").await;

    let plan = FaultPlan::new(1);
    plan.rate(&[Op::Get], 1000, Fault::Delay(Duration::from_millis(200)));
    let server = w.server_with(&plan);
    let started = Instant::now();
    let slow = tokio::spawn({
        let server = server.clone();
        async move { server.land(long, "main").await }
    });
    // Queued behind the slow land's batch.
    tokio::time::sleep(Duration::from_secs(1)).await;
    let quick = tokio::spawn({
        let server = server.clone();
        async move {
            loop {
                match server.land(small, "main").await {
                    Err(Error::Refused { retry_after_ms, .. }) => {
                        tokio::time::sleep(Duration::from_millis(retry_after_ms)).await
                    }
                    other => return (other, Instant::now()),
                }
            }
        }
    });

    let err = slow.await.unwrap().expect_err("past its budget");
    let took = started.elapsed();
    assert!(matches!(err, Error::Conflict(_)), "{err}");
    assert!(err.to_string().contains("retry"), "{err}");
    assert!(
        took >= REPLAY_BUDGET && took < MAX_BATCH_LIFETIME,
        "answered after {took:?}"
    );

    let (landed, at) = quick.await.unwrap();
    let LandResponse::Landed { landed, .. } = landed.unwrap() else {
        panic!("the small land is not delayed by the replay")
    };
    assert!(
        at - started < MAX_BATCH_LIFETIME,
        "the small land waited {:?}",
        at - started
    );
    plan.pause();
    assert_eq!(w.meta.get_ref("main").await.unwrap(), Some(landed));
    assert!(!clean.is_ancestor(long, landed).await.unwrap());
}
