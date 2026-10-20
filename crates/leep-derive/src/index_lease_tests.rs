//! Index leases and `lease` mode warming (ADR 0058 section 2).

use std::collections::BTreeMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use leep_core::memory::{check_lease_contract, MemoryBlobStore, MemoryMetaStore};
use leep_core::{Changeset, ChangesetId, FileChange, FileKind, LeaseKey, MetaStore, RepoPath};

use crate::derived::{DerivedKind, HistoryTree, LastMerge, LogFilterIndex};
use crate::indexing::{parse_holder_meta, IndexGroup, IndexLeaseConfig, IndexLeases, ToolLease};
use crate::warm::Warmer;
use crate::Repo;

/// A memory store whose lease clock follows tokio's.
fn clocked() -> Arc<MemoryMetaStore> {
    let base = tokio::time::Instant::now();
    Arc::new(MemoryMetaStore::new().with_lease_clock(Arc::new(move || {
        1_000_000 + base.elapsed().as_millis() as i64
    })))
}

async fn commit(r: &Repo, parents: &[ChangesetId], path: &str, body: &str) -> ChangesetId {
    let content = r.blobs().put(Bytes::from(body.to_string())).await.unwrap();
    r.create_changeset(Changeset {
        parents: parents.to_vec(),
        author: "agent".into(),
        timestamp: 1,
        message: body.into(),
        changes: BTreeMap::from([(
            RepoPath::new(path).unwrap(),
            FileChange::Modify {
                content,
                kind: FileKind::Regular,
                size: body.len() as u64,
            },
        )]),
        extra: BTreeMap::new(),
    })
    .await
    .unwrap()
}

async fn indexed(r: &Repo, kind: &str, id: ChangesetId) -> bool {
    r.meta().get_derived(kind, &id.0).await.unwrap().is_some()
}

/// A server in `lease` mode on `meta`, competing for every group.
fn server(
    meta: &Arc<MemoryMetaStore>,
    blobs: &Arc<MemoryBlobStore>,
    name: &str,
) -> (Repo, Arc<IndexLeases>, Warmer) {
    let repo = Repo::new(blobs.clone(), meta.clone());
    let leases = IndexLeases::new(
        meta.clone(),
        IndexLeaseConfig::new(name, format!("http://{name}:7070"))
            .with_candidates(IndexGroup::ALL.to_vec()),
    );
    let warmer = Warmer::new(repo.clone(), true)
        .with_leases(leases.clone())
        .with_full_listing_every(Duration::from_secs(3600));
    (repo, leases, warmer)
}

#[tokio::test]
async fn the_index_scope_passes_the_lease_contract() {
    check_lease_contract(&MemoryMetaStore::new()).await;
}

/// One server holds each group's lease and warms it; another server in
/// `lease` mode leaves it alone, so each range is derived once. The holder
/// publishes its rate and backlog in the lease's `meta`.
#[tokio::test(start_paused = true)]
async fn only_the_holder_warms_a_group() {
    let meta = clocked();
    let blobs = Arc::new(MemoryBlobStore::new());
    let (a, la, mut wa) = server(&meta, &blobs, "a");
    let (b, lb, mut wb) = server(&meta, &blobs, "b");
    la.tick().await;
    lb.tick().await;
    assert_eq!(la.held().len(), 3);
    assert!(lb.held().is_empty());
    let mut tip = commit(&a, &[], "f", "0").await;
    for i in 1..30 {
        tip = commit(&a, &[tip], "f", &i.to_string()).await;
    }
    meta.cas_ref("main", None, Some(tip)).await.unwrap();
    let report = wb.pass().await.unwrap();
    assert_eq!(report.warmed, 0, "a non-holder warms nothing");
    assert!(!indexed(&b, HistoryTree::NAME, tip).await);
    let report = wa.pass().await.unwrap();
    assert_eq!(report.warmed, 1);
    for kind in [HistoryTree::NAME, LogFilterIndex::NAME] {
        assert!(indexed(&a, kind, tip).await, "{kind}");
    }
    assert!(a.derive_calls.load(Ordering::Relaxed) >= 60);
    assert_eq!(b.derive_calls.load(Ordering::Relaxed), 0);
    // The next renewal publishes the rate and backlog.
    la.tick().await;
    let l = meta
        .lease(&LeaseKey::index(HistoryTree::NAME), la.config().timing())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (l.holder.as_str(), l.address.as_str()),
        ("a", "http://a:7070")
    );
    assert!(parse_holder_meta(&l.meta).rate > 0.0, "{}", l.meta);
    assert_eq!(l.key.repo_id, 0);
}

/// A holder that dies keeps its leases until they are stale (6 s without a
/// renewal); then another candidate takes them on its next renewal and its
/// next pass warms what moved meanwhile. A graceful shutdown hands them over
/// at once.
#[tokio::test(start_paused = true)]
async fn a_candidate_takes_over_a_stale_lease_and_warms_what_was_missed() {
    let meta = clocked();
    let blobs = Arc::new(MemoryBlobStore::new());
    let (a, la, mut wa) = server(&meta, &blobs, "a");
    let (_b, lb, mut wb) = server(&meta, &blobs, "b");
    la.tick().await;
    let base = commit(&a, &[], "f", "0").await;
    meta.cas_ref("main", None, Some(base)).await.unwrap();
    wa.pass().await.unwrap();
    assert!(indexed(&a, HistoryTree::NAME, base).await);
    // `a` dies.
    la.halt();
    let next = commit(&a, &[base], "f", "1").await;
    meta.cas_ref("main", Some(base), Some(next)).await.unwrap();
    tokio::time::advance(Duration::from_secs(3)).await;
    lb.tick().await;
    assert!(lb.held().is_empty(), "a live lease is never taken");
    tokio::time::advance(Duration::from_secs(4)).await;
    lb.tick().await;
    assert_eq!(lb.held().len(), 3);
    assert_eq!(lb.held_epoch(IndexGroup::History), Some(2));
    let report = wb.pass().await.unwrap();
    assert!(report.warmed >= 1, "{report:?}");
    assert!(indexed(&a, HistoryTree::NAME, next).await);
    drop(wa);
    // Shutdown gives the leases up at once.
    lb.shutdown().await;
    let (_c, lc, _) = server(&meta, &blobs, "c");
    lc.tick().await;
    assert_eq!(lc.held().len(), 3);
}

/// A holder that loses its lease while deriving stops at its next write:
/// what it wrote is whole changesets after their parents, and the new
/// holder finishes the range.
#[tokio::test(start_paused = true)]
async fn a_holder_that_loses_its_lease_stops_writing() {
    use leep_testkit::{Fault, FaultPlan, FaultyMetaStore, Op};
    let meta = clocked();
    let blobs = Arc::new(MemoryBlobStore::new());
    let clean = Repo::new(blobs.clone(), meta.clone());
    let mut tip = commit(&clean, &[], "f", "0").await;
    let mut ids = vec![tip];
    for i in 1..700 {
        tip = commit(&clean, &[tip], &format!("d{}/f", i % 9), &i.to_string()).await;
        ids.push(tip);
    }
    meta.cas_ref("main", None, Some(tip)).await.unwrap();
    // `a`'s first window reaches the store 8 s late; meanwhile it pauses
    // and `b` takes its leases.
    let plan = FaultPlan::new(3);
    plan.script(Op::PutDerived, 1, Fault::Delay(Duration::from_secs(8)));
    let faulty: Arc<dyn MetaStore> = Arc::new(FaultyMetaStore::new(meta.clone(), plan.clone()));
    let la = IndexLeases::new(
        faulty.clone(),
        IndexLeaseConfig::new("a", "").with_candidates(vec![IndexGroup::History]),
    );
    la.tick().await;
    let mut wa = Warmer::new(Repo::new(blobs.clone(), faulty), false).with_leases(la.clone());
    let (_, lb, mut wb) = server(&meta, &blobs, "b");
    let pass = tokio::spawn(async move { wa.pass().await });
    while plan.calls(Op::PutDerived) == 0 {
        tokio::task::yield_now().await;
    }
    tokio::time::advance(Duration::from_secs(7)).await;
    lb.tick().await;
    assert!(lb.holds(IndexGroup::History));
    let _ = pass.await.unwrap();
    let written = meta
        .list_derived(HistoryTree::NAME, None, usize::MAX)
        .await
        .unwrap();
    assert!(
        !written.is_empty() && written.len() < ids.len(),
        "{} of {} written",
        written.len(),
        ids.len()
    );
    for (key, _) in &written {
        for p in clean
            .get_changeset(ChangesetId(*key))
            .await
            .unwrap()
            .parents
        {
            assert!(indexed(&clean, HistoryTree::NAME, p).await);
        }
    }
    wb.pass().await.unwrap();
    assert!(indexed(&clean, HistoryTree::NAME, tip).await);
    assert!(indexed(&clean, LogFilterIndex::NAME, tip).await);
}

/// A tool refuses an index whose lease a live server holds, unless told to
/// ignore it, and takes it once the server is gone.
#[tokio::test(start_paused = true)]
async fn tools_refuse_a_live_index_lease() {
    let meta = clocked();
    let blobs = Arc::new(MemoryBlobStore::new());
    let (_, la, _) = server(&meta, &blobs, "a");
    la.tick().await;
    let say = |_: &str| {};
    let groups = [IndexGroup::History];
    let refused = ToolLease::take(meta.clone(), "tool", &groups, false, Duration::ZERO, &say).await;
    assert!(matches!(refused, Err(leep_core::Error::Invalid(_))));
    let beside = ToolLease::take(meta.clone(), "tool", &groups, true, Duration::ZERO, &say)
        .await
        .unwrap();
    assert!(beside.groups().is_empty());
    la.shutdown().await;
    let t = tokio::time::Instant::now();
    let tool = ToolLease::take(
        meta.clone(),
        "tool",
        &groups,
        false,
        Duration::from_secs(60),
        &say,
    )
    .await
    .unwrap();
    assert_eq!(tool.groups(), &groups);
    assert!(
        t.elapsed() >= Duration::from_secs(60),
        "the fence was waited"
    );
    // While the tool holds it, servers stay away.
    let (_, lb, _) = server(&meta, &blobs, "b");
    lb.tick().await;
    assert!(!lb.holds(IndexGroup::History));
    tool.release().await;
    lb.tick().await;
    assert!(lb.holds(IndexGroup::History));
}

/// `last_merge-v1` is written with each changeset (ADR 0058 section 1), with
/// the value a derivation computes, and a missing parent row is bridged by
/// the grandparent's, never derived.
#[tokio::test]
async fn last_merge_is_written_with_each_changeset() {
    let r = Repo::new(
        Arc::new(MemoryBlobStore::new()),
        Arc::new(MemoryMetaStore::new()),
    );
    let root = commit(&r, &[], "a", "0").await;
    let x = commit(&r, &[root], "a", "1").await;
    let y = commit(&r, &[root], "b", "1").await;
    let m = commit(&r, &[x, y], "c", "1").await;
    let after = commit(&r, &[m], "c", "2").await;
    let later = commit(&r, &[after], "c", "3").await;
    let calls = r.derive_calls.load(Ordering::Relaxed);
    let value = |r: Repo, id: ChangesetId| async move {
        let v = r
            .meta()
            .get_derived(LastMerge::NAME, &id.0)
            .await
            .unwrap()?;
        Some(u64::from_be_bytes(v[..].try_into().unwrap()))
    };
    let gm = r.generation(m).await.unwrap();
    for (id, want) in [(root, 0), (x, 0), (y, 0), (m, gm), (after, gm), (later, gm)] {
        assert_eq!(value(r.clone(), id).await, Some(want));
    }
    assert_eq!(
        r.derive_calls.load(Ordering::Relaxed),
        calls,
        "nothing derived"
    );

    // Rows dropped: a child of a non-merge parent without a row takes the
    // grandparent's; further down it is left to derivation.
    r.meta().delete_derived_kind(LastMerge::NAME).await.unwrap();
    let c1 = commit(&r, &[later], "c", "4").await;
    assert_eq!(value(r.clone(), c1).await, None);
    let c2 = commit(&r, &[m], "d", "1").await;
    assert_eq!(
        value(r.clone(), c2).await,
        Some(gm),
        "the parent is a merge"
    );
    let fresh = Repo::new(r.blobs().clone(), r.meta().clone());
    let derived = crate::derived::derive::<LastMerge>(&fresh, c1)
        .await
        .unwrap();
    assert_eq!(derived[..], gm.to_be_bytes());
    let c3 = commit(&r, &[c1], "c", "5").await;
    assert_eq!(value(r.clone(), c3).await, Some(gm));
}
