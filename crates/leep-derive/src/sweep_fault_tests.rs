//! The changeset sweep (ADR 0059 phase 2) under faults and kills: condemn
//! and collect never delete what a root reaches, never leave a stored
//! changeset without its parent, and finish what a killed run left.

use leep_core::api::LandResponse;

use super::*;
use crate::changeset_gc::{
    condemn_for_test, is_condemned_for_test, CS_CONDEMNED, CS_DOOM, CS_RESCUE, GC_FENCE_ACTIVE,
};
use crate::changeset_sweep::{self, SweepOptions, CS_TOMBSTONE};

const DAY: i64 = 86_400;

/// Well past any grace for what a test stores now.
fn later() -> i64 {
    leep_core::store::unix_now() + 10 * DAY
}

fn opts() -> SweepOptions {
    SweepOptions {
        grace: Duration::from_secs(DAY as u64),
        fence: Duration::from_secs(15 * 60),
        batch: 3,
        rate: 0,
        max_delete: u64::MAX,
        index_fence: Duration::from_secs(60),
        tombstone_retention: Duration::from_secs(DAY as u64),
        ..SweepOptions::default()
    }
}

/// Condemn at `t`, collect a fence later; returns the collect's report.
async fn sweep(r: &Repo, o: &SweepOptions, t: i64) -> changeset_sweep::SweepCollectReport {
    changeset_sweep::condemn(r, o, &TestClock::at(t))
        .await
        .unwrap();
    changeset_sweep::collect(r, o, &TestClock::at(t + o.fence.as_secs() as i64 + 1))
        .await
        .unwrap()
}

async fn stored(w: &World, id: ChangesetId) -> bool {
    w.meta.get_changeset(&id).await.unwrap().is_some()
}

/// Every stored changeset's parents are stored.
async fn assert_closed(seed: u64, w: &World) {
    for id in w.meta.list_changesets(None, usize::MAX).await.unwrap() {
        let cs = w.meta.get_changeset(&id).await.unwrap().unwrap();
        for p in cs.parents {
            assert!(
                stored(w, p).await,
                "seed {seed}: stored {id} names {p}, which was deleted"
            );
        }
    }
}

/// Every root is stored with its whole ancestry and readable trees: every
/// ref, and both targets of every ref log entry.
async fn assert_roots_kept(seed: u64, w: &World) {
    for name in w.meta.list_refs().await.unwrap().keys() {
        assert_ref_sound(seed, &w.clean, name).await;
    }
    for e in w.meta.ref_log(None, None, usize::MAX).await.unwrap() {
        for id in [e.old, e.new].into_iter().flatten() {
            assert!(
                stored(w, id).await,
                "seed {seed}: ref log entry {} names {id}, which was deleted",
                e.seq
            );
            history(&w.clean, id).await;
        }
    }
}

/// A commit through `repo` that may be refused because its parent was
/// condemned or deleted under it, as a client sees.
async fn try_commit(
    repo: &Repo,
    parent: Option<ChangesetId>,
    path: &str,
    body: &str,
    extra: Option<(&str, &str)>,
) -> Option<ChangesetId> {
    for _ in 0..40 {
        let content = match repo.put_blob(Bytes::from(body.to_string())).await {
            Ok(h) => h,
            Err(e) if is_injected(&e) || matches!(e, Error::Conflict(_)) => continue,
            Err(e) => panic!("upload: {e}"),
        };
        let mut cs = Changeset {
            parents: parent.into_iter().collect(),
            author: "agent".into(),
            timestamp: 7,
            message: path.into(),
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
        if let Some((k, v)) = extra {
            cs.extra.insert(k.into(), v.into());
        }
        match repo.create_changeset(cs).await {
            Ok(id) => return Some(id),
            Err(e) if is_injected(&e) || matches!(e, Error::Conflict(_)) => continue,
            // The parent was deleted: the client starts again elsewhere.
            Err(Error::Invalid(m)) if m.contains("missing") || m.contains("unknown parent") => {
                return None
            }
            Err(Error::NotFound(_)) => return None,
            Err(e) => panic!("create: {e}"),
        }
    }
    None
}

/// Land `cs` on `main` until it is answered; `None` if it is refused for
/// good (its changeset was deleted, a conflict).
async fn try_land(repo: &Repo, cs: ChangesetId, id: Option<LandId>) -> Option<ChangesetId> {
    for _ in 0..40 {
        match repo.land_keyed(cs, "main", None, true, id.clone()).await {
            Ok(LandResponse::Landed { landed, .. }) => return Some(landed),
            Ok(_) => return None,
            Err(e) if is_injected(&e) || matches!(e, Error::Conflict(_)) => continue,
            Err(Error::NotFound(_) | Error::Invalid(_)) => return None,
            Err(e) => panic!("land: {e}"),
        }
    }
    None
}

/// What clients do between sweeps: land on `main` (keyed and not, so
/// rebased originals pile up), abandon work, build on abandoned work,
/// snapshot, and make, move back and delete a branch.
async fn client_round(seed: u64, repo: Repo, round: usize, rng_seed: u64) {
    let mut rng = Rng::new(rng_seed);
    let head = repo.meta().get_ref("main").await.ok().flatten();
    let mut made: Vec<ChangesetId> = Vec::new();
    for k in 0..3 {
        let path = format!("r{round}/k{k}.txt");
        let body = format!("seed {seed} round {round} op {k}\n");
        match rng.below(6) {
            0 | 1 => {
                if let Some(c) = try_commit(&repo, head, &path, &body, None).await {
                    let id = (rng.below(2) == 0).then(|| {
                        let mut b = [7u8; 16];
                        b[..4].copy_from_slice(&[
                            seed as u8,
                            (seed >> 8) as u8,
                            round as u8,
                            k as u8,
                        ]);
                        LandId::from_bytes(&b).unwrap()
                    });
                    try_land(&repo, c, id).await;
                    made.push(c);
                }
            }
            2 => {
                // Abandoned, or built on something abandoned before.
                let parent = made.last().copied().or(head);
                if let Some(c) = try_commit(&repo, parent, &path, &body, None).await {
                    made.push(c);
                }
            }
            3 => {
                let snap = Some((leep_core::changeset::EXTRA_SNAPSHOT, "1"));
                try_commit(&repo, head, &path, &body, snap).await;
            }
            4 => {
                if let Some(&c) = made.first() {
                    let ctx = leep_core::RefUpdateContext::new("cas");
                    let name = format!("b{round}");
                    for _ in 0..40 {
                        match repo.cas_ref_checked(&name, None, Some(c), &ctx).await {
                            Ok(_) => break,
                            Err(e) if is_injected(&e) || matches!(e, Error::Conflict(_)) => {}
                            Err(_) => break,
                        }
                    }
                    if rng.below(2) == 0 {
                        let now = repo.meta().get_ref(&name).await.ok().flatten();
                        let _ = repo.cas_ref_checked(&name, now, None, &ctx).await;
                    }
                }
            }
            _ => {
                // Build on a changeset made earlier in the round and land it:
                // the land rescues the stack under it.
                if let Some(&p) = made.first() {
                    if let Some(c) = try_commit(&repo, Some(p), &path, &body, None).await {
                        try_land(&repo, c, None).await;
                    }
                }
            }
        }
    }
}

/// Random swarms of keyed and unkeyed lands, stacks, branch moves and
/// deletes and snapshots, with condemn and collect killed at random points
/// and every store call liable to fail before or after it applies. After
/// every round every root is stored with its ancestry and every stored
/// changeset's parents are stored; a clean sweep at the end leaves nothing
/// condemned and the flag down.
#[tokio::test(start_paused = true)]
async fn unreachable_sweep_never_deletes_a_reachable_changeset() {
    let mut deleted_under_faults = 0u64;
    for seed in seeds() {
        let w = World::new(seed);
        let base = commit_file(seed, &w.clean, None, "base.txt", "base\n", "base").await;
        w.meta.cas_ref("main", None, Some(base)).await.unwrap();
        let gc_plan = FaultPlan::new(seed ^ 0x5EE9);
        gc_plan
            .jitter(2)
            .rate(Op::WRITES, 30, Fault::FailAfter)
            .rate(Op::WRITES, 30, Fault::Fail)
            .rate(&[Op::DeleteChangesets], 60, Fault::FailAfter)
            .rate(&[Op::DeleteChangesets], 60, Fault::Fail)
            .rate(Op::META, 3, Fault::Fail)
            .rate(&[Op::GetDerived], 3, Fault::Corrupt);
        let client_plan = FaultPlan::new(seed ^ 0xC1);
        client_plan
            .jitter(2)
            .rate(Op::WRITES, 20, Fault::FailAfter)
            .rate(Op::WRITES, 20, Fault::Fail)
            .rate(Op::META, 5, Fault::Fail);
        let (gc_repo, client_repo) = (w.server_with(&gc_plan), w.server_with(&client_plan));
        let mut rng = Rng::new(seed ^ 0x77);
        let o = opts();
        let mut t = later();
        for round in 0..8 {
            let clients: Vec<_> = (0..1 + rng.below(3))
                .map(|c| {
                    tokio::spawn(client_round(
                        seed,
                        client_repo.clone(),
                        round * 4 + c,
                        seed ^ (round * 4 + c) as u64,
                    ))
                })
                .collect();
            let kill_after = if rng.below(3) == 0 {
                rng.below(200)
            } else {
                usize::MAX
            };
            let (r, oo) = (gc_repo.clone(), o.clone());
            let condemn = rng.below(2) == 0;
            let clock = TestClock::at(t);
            tokio::spawn(async move {
                if condemn {
                    let _ = leep_testkit::cancel_after_polls(
                        changeset_sweep::condemn(&r, &oo, &clock),
                        kill_after,
                    )
                    .await;
                } else {
                    let _ = leep_testkit::cancel_after_polls(
                        changeset_sweep::collect(&r, &oo, &clock),
                        kill_after,
                    )
                    .await;
                }
            })
            .await
            .unwrap();
            for c in clients {
                c.await.unwrap();
            }
            t += o.fence.as_secs() as i64 + 1;
            assert_closed(seed, &w).await;
            assert_roots_kept(seed, &w).await;
        }
        // Clean runs finish what the killed ones left. A run killed in its
        // last moments may hold the lease; a fresh process would wait it out.
        t += 3600;
        let stones_before = w.meta.count_derived(CS_TOMBSTONE).await.unwrap();
        sweep(&w.clean, &o, t).await;
        let last = sweep(&w.clean, &o, t + 3600).await;
        // Deletes made by the faulty, killed runs themselves.
        deleted_under_faults += stones_before;
        assert_closed(seed, &w).await;
        assert_roots_kept(seed, &w).await;
        assert_ref_log_replays(seed, &*w.meta).await;
        assert_eq!(last.condemned_pending, 0, "seed {seed}: {last:?}");
        assert!(last.flag_cleared, "seed {seed}: flag left up");
        assert_eq!(last.deleted, 0, "seed {seed}: garbage survived: {last:?}");
    }
    if seeds().count() >= 10 {
        assert!(
            deleted_under_faults > 0,
            "the faulty runs never deleted anything"
        );
    }
}

/// A changeset collect skips (rescued, too young, kept) keeps every
/// condemned ancestor, and a later collect deletes the lot once nothing
/// keeps them.
#[tokio::test(start_paused = true)]
async fn collect_keeps_ancestors_of_every_changeset_it_skips() {
    let w = World::new(1);
    let base = commit_file(1, &w.clean, None, "base.txt", "b\n", "base").await;
    w.meta.cas_ref("main", None, Some(base)).await.unwrap();
    let a = commit_file(1, &w.clean, Some(base), "a", "a\n", "a").await;
    let b = commit_file(1, &w.clean, Some(a), "b", "b\n", "b").await;
    let c = commit_file(1, &w.clean, Some(b), "c", "c\n", "c").await;
    let o = opts();
    let t = later();
    changeset_sweep::condemn(&w.clean, &o, &TestClock::at(t))
        .await
        .unwrap();
    // A writer answers the condemnation of the tip only: as a crashed
    // rescue that wrote its last row first would have.
    let ct = w
        .meta
        .get_derived(CS_CONDEMNED, &c.0)
        .await
        .unwrap()
        .unwrap();
    w.meta.put_derived(CS_RESCUE, &c.0, ct).await.unwrap();
    let r = changeset_sweep::collect(&w.clean, &o, &TestClock::at(t + 901))
        .await
        .unwrap();
    assert_eq!(r.deleted, 0, "{r:?}");
    assert_eq!(r.kept_by_descendant, 2, "{r:?}");
    for id in [a, b, c] {
        assert!(stored(&w, id).await);
    }
    // The tip condemned again later than its ancestors: still too young
    // when they are due, so it keeps them.
    condemn_for_test(&w.clean, &[c], t + 600).await;
    // As a condemn that ended at `t` would have left the flag.
    w.meta
        .put_derived(
            GC_FENCE_ACTIVE,
            &crate::changeset_gc::fence_flag_key(),
            Bytes::copy_from_slice(&t.to_be_bytes()),
        )
        .await
        .unwrap();
    let r = changeset_sweep::collect(&w.clean, &o, &TestClock::at(t + 1000))
        .await
        .unwrap();
    assert_eq!((r.deleted, r.fence_pending), (0, 1), "{r:?}");
    assert_closed(1, &w).await;
    // Once due, all three go, children first.
    let r = changeset_sweep::collect(&w.clean, &o, &TestClock::at(t + 2000))
        .await
        .unwrap();
    assert_eq!(r.deleted, 3, "{r:?}");
    assert!(r.flag_cleared);
    assert_closed(1, &w).await;
    assert!(stored(&w, base).await);
}

/// A chain of unreachable changesets, collected with `max_delete` of every
/// size from 1: each partial run leaves only ancestors of what it kept, so
/// every intermediate state is closed under ancestry, and the next run
/// finishes. Also with a batch of one, so each delete is its own
/// transaction.
#[tokio::test(start_paused = true)]
async fn collect_stopped_by_max_delete_is_closed_under_ancestry() {
    for cap in 1..=7u64 {
        let w = World::new(cap);
        let base = commit_file(cap, &w.clean, None, "base.txt", "b\n", "base").await;
        w.meta.cas_ref("main", None, Some(base)).await.unwrap();
        let mut chain = Vec::new();
        let mut p = base;
        for i in 0..6 {
            p = commit_file(cap, &w.clean, Some(p), &format!("f{i}"), "x\n", "c").await;
            chain.push(p);
        }
        // A side branch off the middle of the chain.
        let side = commit_file(cap, &w.clean, Some(chain[2]), "side", "s\n", "s").await;
        let o = SweepOptions {
            max_delete: cap,
            batch: 1,
            ..opts()
        };
        let t = later();
        let r = sweep(&w.clean, &o, t).await;
        assert_eq!(r.deleted, cap.min(7), "cap {cap}: {r:?}");
        assert_eq!(r.left_by_cap, 7u64.saturating_sub(cap), "cap {cap}");
        assert_closed(cap, &w).await;
        assert!(
            !r.flag_cleared || cap >= 7,
            "cap {cap}: flag cleared with work left"
        );
        let r = sweep(&w.clean, &opts(), t + 3600).await;
        assert_closed(cap, &w).await;
        for id in chain.iter().chain([&side]) {
            assert!(!stored(&w, *id).await, "cap {cap}: {id} left after {r:?}");
        }
    }
}

/// Collect orders deletes by parent edges: with the `generation` and
/// `root_tree` rows of the condemned chain gone, it still deletes children
/// before parents.
#[tokio::test(start_paused = true)]
async fn collect_orders_without_generation_rows() {
    for cap in 1..=4u64 {
        let w = World::new(cap);
        let base = commit_file(cap, &w.clean, None, "base.txt", "b\n", "base").await;
        w.meta.cas_ref("main", None, Some(base)).await.unwrap();
        let a = commit_file(cap, &w.clean, Some(base), "a", "a\n", "a").await;
        let b = commit_file(cap, &w.clean, Some(a), "b", "b\n", "b").await;
        let c = commit_file(cap, &w.clean, Some(b), "c", "c\n", "c").await;
        let keys: Vec<leep_core::Hash> = [a, b, c].iter().map(|id| id.0).collect();
        w.meta
            .delete_derived_keys(&["generation", "root_tree", "skiplist"], &keys)
            .await
            .unwrap();
        let o = SweepOptions {
            max_delete: cap,
            batch: 1,
            ..opts()
        };
        let r = sweep(&w.clean, &o, later()).await;
        assert_eq!(r.deleted, cap.min(3), "cap {cap}: {r:?}");
        assert_closed(cap, &w).await;
        let left: Vec<ChangesetId> = {
            let mut v = Vec::new();
            for id in [a, b, c] {
                if stored(&w, id).await {
                    v.push(id);
                }
            }
            v
        };
        // Only a prefix of the chain from its root is left.
        assert_eq!(left, [a, b, c][..left.len()].to_vec(), "cap {cap}");
    }
}

/// Past its copy's scan window and grace, the original of a rebased land
/// is deleted: reading it is 404, the copy still names it as `via`
/// (`rebased_origin`, `rebase_chain`), and a keyed retry of the land is
/// answered from its record.
#[tokio::test(start_paused = true)]
async fn get_of_deleted_original_is_404_and_copy_via_answers() {
    let w = World::new(3);
    let base = commit_file(3, &w.clean, None, "base.txt", "b\n", "base").await;
    w.meta.cas_ref("main", None, Some(base)).await.unwrap();
    let feature = commit_file(3, &w.clean, Some(base), "f.txt", "f\n", "feature").await;
    let other = commit_file(3, &w.clean, Some(base), "o.txt", "o\n", "other").await;
    w.meta
        .cas_ref("main", Some(base), Some(other))
        .await
        .unwrap();
    let id = LandId::from_bytes(&[3; 16]).unwrap();
    let LandResponse::Landed { landed: copy, .. } = w
        .clean
        .land_keyed(feature, "main", None, true, Some(id.clone()))
        .await
        .unwrap()
    else {
        panic!("not landed");
    };
    assert_ne!(copy, feature);
    let mut head = copy;
    for i in 0..crate::land::LANDED_COPY_SCAN {
        let next = commit_file(3, &w.clean, Some(head), "n.txt", &format!("{i}\n"), "n").await;
        w.meta
            .cas_ref("main", Some(head), Some(next))
            .await
            .unwrap();
        head = next;
    }
    let r = sweep(&w.clean, &opts(), later()).await;
    assert_eq!(r.deleted, 1, "{r:?}");
    assert!(matches!(
        w.clean.get_changeset(feature).await,
        Err(Error::NotFound(_))
    ));
    assert_eq!(w.clean.rebased_origin(copy).await.unwrap(), Some(feature));
    assert_eq!(w.clean.rebase_chain(copy).await.unwrap(), vec![feature]);
    let again = w
        .clean
        .land_keyed(feature, "main", None, true, Some(id))
        .await
        .unwrap();
    assert!(
        matches!(again, LandResponse::Landed { landed, .. } if landed == copy),
        "{again:?}"
    );
    assert_eq!(w.meta.get_ref("main").await.unwrap(), Some(head));
    assert_closed(3, &w).await;
    // The tombstone keeps the deleted root tree for gc --full, and the
    // tombstone pass removed the rest of its rows.
    let stone = w.meta.get_derived(CS_TOMBSTONE, &feature.0).await.unwrap();
    let (_, root) = changeset_sweep::decode_tombstone(&stone.unwrap()).unwrap();
    assert!(root.is_some());
    for kind in crate::gc_unreachable::PER_CHANGESET_KINDS {
        assert!(w
            .meta
            .get_derived(kind, &feature.0)
            .await
            .unwrap()
            .is_none());
    }
}

/// A backup restored after a sweep brings deleted changesets back with the
/// fence rows they had when it was taken. Nothing reaches them, so the next
/// sweep condemns and deletes them again; their deleted state and
/// tombstones are only bookkeeping.
#[tokio::test(start_paused = true)]
async fn restored_backup_recondemns() {
    let w = World::new(4);
    let base = commit_file(4, &w.clean, None, "base.txt", "b\n", "base").await;
    w.meta.cas_ref("main", None, Some(base)).await.unwrap();
    let a = commit_file(4, &w.clean, Some(base), "a", "a\n", "a").await;
    let b = commit_file(4, &w.clean, Some(a), "b", "b\n", "b").await;
    let saved = [
        w.meta.get_changeset(&a).await.unwrap().unwrap(),
        w.meta.get_changeset(&b).await.unwrap().unwrap(),
    ];
    let t = later();
    let r = sweep(&w.clean, &opts(), t).await;
    assert_eq!(r.deleted, 2);
    // The restore: the changesets as they were before the condemnation.
    for cs in &saved {
        w.meta.put_changeset(cs).await.unwrap();
    }
    assert_closed(4, &w).await;
    let r = sweep(&w.clean, &opts(), t + 2 * DAY).await;
    assert_eq!(r.deleted, 2, "{r:?}");
    assert!(!stored(&w, a).await && !stored(&w, b).await);
    // Restored once more, now with a doom left announced: a writer that
    // names it rescues it and the next sweep keeps it.
    for cs in &saved {
        w.meta.put_changeset(cs).await.unwrap();
    }
    changeset_sweep::condemn(&w.clean, &opts(), &TestClock::at(t + 4 * DAY))
        .await
        .unwrap();
    assert!(is_condemned_for_test(&w.clean, b).await);
    let child = commit_file(4, &w.clean, Some(b), "c", "c\n", "c").await;
    w.meta.cas_ref("keep", None, Some(child)).await.unwrap();
    let r = changeset_sweep::collect(&w.clean, &opts(), &TestClock::at(t + 5 * DAY))
        .await
        .unwrap();
    assert_eq!(r.deleted, 0, "{r:?}");
    assert_roots_kept(4, &w).await;
}

/// The handshake: a writer that rescues while collect is between announcing
/// and deleting makes collect abandon that changeset and keep its
/// ancestors; one that comes after the delete finds it missing.
#[tokio::test(start_paused = true)]
async fn collect_killed_between_doom_and_delete_resumes() {
    for seed in seeds() {
        let w = World::new(seed);
        let base = commit_file(seed, &w.clean, None, "base.txt", "b\n", "base").await;
        w.meta.cas_ref("main", None, Some(base)).await.unwrap();
        let a = commit_file(seed, &w.clean, Some(base), "a", "a\n", "a").await;
        let b = commit_file(seed, &w.clean, Some(a), "b", "b\n", "b").await;
        let o = opts();
        let t = later();
        changeset_sweep::condemn(&w.clean, &o, &TestClock::at(t))
            .await
            .unwrap();
        // Kill a collect at every early point of its run.
        let kill = 1 + (seed as usize % 120);
        let _ = leep_testkit::cancel_after_polls(
            changeset_sweep::collect(&w.clean, &o, &TestClock::at(t + 901)),
            kill,
        )
        .await;
        assert_closed(seed, &w).await;
        // A writer naming `b` now either rescues it or finds it gone.
        let child = try_commit(&w.clean, Some(b), "c", "c\n", None).await;
        if let Some(c) = child {
            w.meta.cas_ref("keep", None, Some(c)).await.unwrap();
        }
        let r = changeset_sweep::collect(&w.clean, &o, &TestClock::at(t + 2000))
            .await
            .unwrap();
        assert_closed(seed, &w).await;
        assert_roots_kept(seed, &w).await;
        if child.is_some() {
            assert!(stored(&w, a).await && stored(&w, b).await, "seed {seed}");
        } else {
            assert!(!stored(&w, b).await, "seed {seed}: {r:?}");
        }
        // No delete is left announced.
        for id in [a, b] {
            if let Some(v) = w.meta.get_derived(CS_DOOM, &id.0).await.unwrap() {
                assert_ne!(v.last(), Some(&0), "seed {seed}: {id} still deleting");
            }
        }
    }
}

/// Per-changeset rows written for a changeset after it was deleted (a late
/// index write) are removed by the tombstone pass, and tombstones are
/// dropped once past their retention.
#[tokio::test(start_paused = true)]
async fn indexer_write_after_delete_is_removed_by_the_tombstone_pass() {
    let w = World::new(5);
    let base = commit_file(5, &w.clean, None, "base.txt", "b\n", "base").await;
    w.meta.cas_ref("main", None, Some(base)).await.unwrap();
    let a = commit_file(5, &w.clean, Some(base), "a", "a\n", "a").await;
    let t = later();
    sweep(&w.clean, &opts(), t).await;
    assert!(!stored(&w, a).await);
    w.meta
        .put_derived("history-tree-v4", &a.0, Bytes::from_static(b"late"))
        .await
        .unwrap();
    let r = sweep(&w.clean, &opts(), t + 3600).await;
    assert!(r.tombstone_rows_deleted >= 1, "{r:?}");
    assert!(w
        .meta
        .get_derived("history-tree-v4", &a.0)
        .await
        .unwrap()
        .is_none());
    assert!(w
        .meta
        .get_derived(CS_TOMBSTONE, &a.0)
        .await
        .unwrap()
        .is_some());
    let r = sweep(&w.clean, &opts(), t + 3 * DAY).await;
    assert_eq!(r.tombstones_dropped, 1, "{r:?}");
    assert!(w
        .meta
        .get_derived(CS_TOMBSTONE, &a.0)
        .await
        .unwrap()
        .is_none());
    assert!(w
        .meta
        .get_derived(GC_FENCE_ACTIVE, &crate::changeset_gc::fence_flag_key())
        .await
        .unwrap()
        .is_some_and(|v| v.as_ref() == 0i64.to_be_bytes()));
}

/// One GC at a time, the other way round from blob GC's test: a changeset
/// condemn or collect refuses while blob collect holds the lease.
#[tokio::test(start_paused = true)]
async fn changeset_sweep_refuses_during_blob_collect() {
    let w = World::new(6);
    let held = crate::gc_lease::GcLease::acquire(w.clean.meta(), "blob-collect")
        .await
        .unwrap()
        .unwrap();
    let clock = TestClock::at(later());
    for e in [
        changeset_sweep::condemn(&w.clean, &opts(), &clock)
            .await
            .unwrap_err(),
        changeset_sweep::collect(&w.clean, &opts(), &clock)
            .await
            .unwrap_err(),
    ] {
        assert!(e.to_string().contains("blob-collect"), "{e}");
    }
    held.release().await.unwrap();
    changeset_sweep::condemn(&w.clean, &opts(), &clock)
        .await
        .unwrap();
}

/// `gc --full` keeps the tree nodes and contents of changesets the sweep
/// deleted within `--backup-fence`, so a backup that snapshotted them
/// before the delete still finds them; past the fence it frees them.
#[tokio::test(start_paused = true)]
async fn gc_full_keeps_trees_of_changesets_deleted_within_the_backup_fence() {
    use crate::gc::{self, GcOptions, Mark};
    let w = World::new(7);
    let base = commit_file(7, &w.clean, None, "base.txt", "b\n", "base").await;
    w.meta.cas_ref("main", None, Some(base)).await.unwrap();
    let gone = commit_file(7, &w.clean, Some(base), "only/here.txt", "unique\n", "g").await;
    let root = w.clean.root_tree(gone).await.unwrap();
    let t = later();
    let r = sweep(&w.clean, &opts(), t).await;
    assert_eq!(r.deleted, 1);
    age_all(&w.blobs).await;
    let full = |fence: Option<Duration>| GcOptions {
        grace: Duration::from_secs(3600),
        fence: Duration::from_secs(600),
        backup_fence: fence,
        ..GcOptions::default()
    };
    let day = Duration::from_secs(DAY as u64);
    // Without the fence a full mark would condemn the deleted tree.
    let dry = GcOptions {
        dry_run: true,
        ..full(None)
    };
    let would = gc::condemn_with(&w.clean, &dry, Mark::Full, &TestClock::at(t + 3600))
        .await
        .unwrap();
    assert!(would.condemned >= 2, "{would:?}");
    // Within the fence: a full condemn and collect keep the deleted tree.
    gc::condemn_with(
        &w.clean,
        &full(Some(day)),
        Mark::Full,
        &TestClock::at(t + 3600),
    )
    .await
    .unwrap();
    gc::collect_with(
        &w.clean,
        &full(Some(day)),
        Mark::Full,
        &TestClock::at(t + 7200),
    )
    .await
    .unwrap();
    assert!(w.blobs.contains(&root).await.unwrap(), "kept for a backup");
    // Past it, they are freed.
    let late = t + 3 * DAY;
    gc::condemn_with(&w.clean, &full(Some(day)), Mark::Full, &TestClock::at(late))
        .await
        .unwrap();
    gc::collect_with(
        &w.clean,
        &full(Some(day)),
        Mark::Full,
        &TestClock::at(late + 3600),
    )
    .await
    .unwrap();
    assert!(
        !w.blobs.contains(&root).await.unwrap(),
        "freed past the fence"
    );
    assert_ref_sound(7, &w.clean, "main").await;
}

/// A clock for a slow condemn: the first reading is when the run started,
/// every later one is `then` (the mark took `then - start`).
struct SlowMarkClock {
    first: std::sync::Mutex<Option<i64>>,
    then: i64,
}

#[async_trait::async_trait]
impl crate::gc::Clock for SlowMarkClock {
    fn now(&self) -> i64 {
        self.first.lock().unwrap().take().unwrap_or(self.then)
    }

    async fn sleep(&self, _: Duration) {}
}

/// Review blocking item 1: condemn stamped its rows with the time the run
/// *started*, before its long mark. With condemn at 02:13, a 25-minute
/// mark, and collect at 02:43 (fence 15m), a request at 02:37 found no
/// condemnation of X, condemn wrote X's row at 02:38 stamped 02:13, and
/// collect deleted X under the request's ref move. The condemnation must
/// carry the time it was written, so the fence runs from then.
#[tokio::test(start_paused = true)]
async fn condemnation_is_stamped_when_written_not_when_condemn_started() {
    let w = World::new(8);
    let base = commit_file(8, &w.clean, None, "base.txt", "b\n", "base").await;
    w.meta.cas_ref("main", None, Some(base)).await.unwrap();
    let x = commit_file(8, &w.clean, Some(base), "x", "x\n", "x").await;
    let o = opts();
    let t = later();
    // 02:13 start, the rows written at 02:38.
    let clock = SlowMarkClock {
        first: std::sync::Mutex::new(Some(t)),
        then: t + 25 * 60,
    };
    changeset_sweep::condemn(&w.clean, &o, &clock)
        .await
        .unwrap();
    // 02:37: a request read X's fence state and saw nothing; at 02:43
    // collect runs; at 02:47 the request moves a ref to X.
    let collected = changeset_sweep::collect(&w.clean, &o, &TestClock::at(t + 30 * 60)).await;
    let ctx = leep_core::RefUpdateContext::new("cas");
    w.meta
        .cas_ref_logged("keep", None, Some(x), &ctx)
        .await
        .unwrap();
    assert!(
        stored(&w, x).await,
        "X deleted within a fence of its condemnation row being written: {collected:?}"
    );
    assert_roots_kept(8, &w).await;
}

/// Collect refuses to start within a fence of the end of the last condemn,
/// recorded in the flag row, whatever times the rows carry.
#[tokio::test(start_paused = true)]
async fn collect_refuses_within_a_fence_of_the_last_condemn() {
    let w = World::new(9);
    let base = commit_file(9, &w.clean, None, "base.txt", "b\n", "base").await;
    w.meta.cas_ref("main", None, Some(base)).await.unwrap();
    let x = commit_file(9, &w.clean, Some(base), "x", "x\n", "x").await;
    let o = opts();
    let t = later();
    changeset_sweep::condemn(&w.clean, &o, &TestClock::at(t))
        .await
        .unwrap();
    let e = changeset_sweep::collect(&w.clean, &o, &TestClock::at(t + 899))
        .await
        .unwrap_err();
    assert!(
        matches!(e, Error::Conflict(_)) && e.to_string().contains("--fence"),
        "{e}"
    );
    assert!(stored(&w, x).await);
    let r = changeset_sweep::collect(&w.clean, &o, &TestClock::at(t + 900))
        .await
        .unwrap();
    assert_eq!(r.deleted, 1, "{r:?}");
}

/// Review item 3: a client that posts a condemned changeset again (the
/// same body, so the same id) inside the fence was answered 200, but only
/// its parents were rescued, so collect deleted it and a later read was
/// 404. Storing it again must rescue the changeset itself.
#[tokio::test(start_paused = true)]
async fn reposting_a_condemned_changeset_rescues_it() {
    let w = World::new(10);
    let base = commit_file(10, &w.clean, None, "base.txt", "b\n", "base").await;
    w.meta.cas_ref("main", None, Some(base)).await.unwrap();
    let x = commit_file(10, &w.clean, Some(base), "x", "x\n", "x").await;
    let body = w.meta.get_changeset(&x).await.unwrap().unwrap();
    let o = opts();
    let t = later();
    changeset_sweep::condemn(&w.clean, &o, &TestClock::at(t))
        .await
        .unwrap();
    assert!(is_condemned_for_test(&w.clean, x).await);
    // A server whose cached flag is fresh, as after the 10 s TTL.
    let server = w.server_with(&FaultPlan::new(10));
    assert_eq!(server.create_changeset(body).await.unwrap(), x);
    let r = changeset_sweep::collect(&w.clean, &o, &TestClock::at(t + 901))
        .await
        .unwrap();
    assert!(stored(&w, x).await, "re-posted changeset deleted: {r:?}");
    w.clean.get_changeset(x).await.unwrap();
}
