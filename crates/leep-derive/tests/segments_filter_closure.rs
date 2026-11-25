//! `content-v1` keeps "a stored directory filter means its whole subtree's
//! filters are stored" (search/index.rs, `tree_row_unstored`), which the
//! warmer, `derive_search`, `search_start_stored` and `stale_target` rely
//! on: a merge never leaves a directory filter out, live or not.
//!
//! The scenario: a root filter dead when the family first lists it is never
//! sealed and stays a row. A subdirectory it shares with a live head is
//! sealed and its row retired; then that subdirectory dies. A ref created at
//! the old revision (a hotfix branch) must still find the subdirectory's
//! filter, so its searches answer.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use leep_core::memory::{MemoryBlobStore, MemoryMetaStore};
use leep_core::{Changeset, ChangesetId, FileChange, FileKind, Hash, RepoPath, TreeEntry};
use leep_derive::derived_segments::{compact_pass, CompactOptions, CompactState, Features, Mode};
use leep_derive::Repo;

const TREE_KIND: &str = "tree-trigrams-v2";

fn features() -> Features {
    let log: Arc<Mutex<Vec<&'static str>>> = Arc::default();
    let seen = log.clone();
    Features {
        record: Arc::new(move |name| {
            let log = log.clone();
            Box::pin(async move {
                log.lock().unwrap().push(name);
                Ok(())
            })
        }),
        recorded: Arc::new(move |name| {
            let seen = seen.clone();
            Box::pin(async move { Ok(seen.lock().unwrap().contains(&name)) })
        }),
    }
}

async fn commit(r: &Repo, parents: &[ChangesetId], files: &[(String, String)]) -> ChangesetId {
    let mut changes = BTreeMap::new();
    for (p, body) in files {
        let data = Bytes::from(body.clone().into_bytes());
        let content = r.blobs().put(data.clone()).await.unwrap();
        changes.insert(
            RepoPath::new(p.as_str()).unwrap(),
            FileChange::Modify {
                content,
                kind: FileKind::Regular,
                size: data.len() as u64,
            },
        );
    }
    r.create_changeset(Changeset {
        parents: parents.to_vec(),
        author: "t".into(),
        timestamp: 1,
        message: format!("c{}", files.len()),
        changes,
        extra: BTreeMap::new(),
    })
    .await
    .unwrap()
}

async fn dir(r: &Repo, id: ChangesetId, name: &str) -> Hash {
    let root = r.root_tree(id).await.unwrap();
    let node = r.read_tree(root).await.unwrap();
    match node.entries.iter().find(|(n, _)| n.as_str() == name) {
        Some((_, TreeEntry::Dir { tree })) => *tree,
        _ => panic!("no directory {name}"),
    }
}

fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

async fn settle(r: &Repo, opts: &CompactOptions, st: &mut CompactState, f: &Features) {
    for _ in 0..12 {
        compact_pass(r, opts, st, Some(f)).await.unwrap();
    }
}

#[tokio::test]
async fn a_stored_root_filter_keeps_its_subdirectory_filter() {
    let r = Repo::new(
        Arc::new(MemoryBlobStore::new()),
        Arc::new(MemoryMetaStore::new()),
    );
    let mut files = vec![(
        "a/x.rs".to_string(),
        "fn a_x() { alpha_marker }\n".repeat(20),
    )];
    for i in 0..6 {
        files.push((
            format!("b/y{i}.rs"),
            format!("fn b_y{i}() {{ beta_marker_{i} }}\n").repeat(20),
        ));
    }
    // C0: the old revision. Indexed while it is the head, as the warmer does.
    let c0 = commit(&r, &[], &files).await;
    r.meta().cas_ref("main", None, Some(c0)).await.unwrap();
    r.derive_search(c0).await.unwrap();
    let (root0, b0) = (r.root_tree(c0).await.unwrap(), dir(&r, c0, "b").await);

    // C1 changes a/: root0 is superseded, b0 is still below the head.
    let c1 = commit(
        &r,
        &[c0],
        &[("a/x.rs".to_string(), "fn a_x() { alpha_two }\n".repeat(20))],
    )
    .await;
    r.meta().cas_ref("main", Some(c0), Some(c1)).await.unwrap();
    r.derive_search(c1).await.unwrap();
    assert_eq!(dir(&r, c1, "b").await, b0);

    // The operator turns `retire` on for content-v1 now: only the current
    // head is inside the window (keep = 0), so root0 is dead and b0 is live.
    let f = features();
    let opts = CompactOptions {
        content_keep: Some(Duration::ZERO),
        content_skew: Duration::ZERO,
        content_now: Some(now() + 10),
        ..CompactOptions::small(Mode::Retire)
    };
    let mut st = CompactState::default();
    settle(&r, &opts, &mut st, &f).await;
    let row = |k: Hash| {
        let r = r.clone();
        async move { r.row_meta().get_derived(TREE_KIND, &k).await.unwrap() }
    };
    let read = |k: Hash| {
        let r = r.clone();
        async move { r.meta().get_derived(TREE_KIND, &k).await.unwrap() }
    };
    assert!(
        row(root0).await.is_some(),
        "root0: dead, never sealed, a row for ever"
    );
    assert!(
        row(b0).await.is_none(),
        "b0: sealed while live, its row retired"
    );
    assert!(read(b0).await.is_some(), "b0: answered by the segment");

    // C2 changes b/: b0 dies. The next passes seal C2's rows and merge the
    // small segments, leaving the dead b0 out.
    let c2 = commit(
        &r,
        &[c1],
        &[("b/y0.rs".to_string(), "fn b_y0() { beta_two }\n".repeat(20))],
    )
    .await;
    r.meta().cas_ref("main", Some(c1), Some(c2)).await.unwrap();
    r.derive_search(c2).await.unwrap();
    settle(&r, &opts, &mut st, &f).await;

    let fresh = Repo::new(r.blobs().clone(), r.row_meta().clone());
    let root_stored = fresh.meta().get_derived(TREE_KIND, &root0).await.unwrap();
    let b_stored = fresh.meta().get_derived(TREE_KIND, &b0).await.unwrap();
    assert!(
        root_stored.is_some(),
        "the old root's filter is still stored"
    );
    assert!(
        b_stored.is_some(),
        "the root's filter is stored, so its subdirectory's filter is too"
    );

    // A hotfix branch at the old revision: the warmer (derive_search) finds
    // the root stored and derives nothing, so the head stays half indexed.
    fresh
        .meta()
        .cas_ref("hotfix", None, Some(c0))
        .await
        .unwrap();
    let rep = fresh.derive_search(c0).await.unwrap();
    assert_eq!(rep.tree_rows, 0, "the head is indexed");
    assert!(fresh
        .meta()
        .get_derived(TREE_KIND, &b0)
        .await
        .unwrap()
        .is_some());
}

/// Under `--indexing lease` (bounded queries, ADR 0058) a search of the
/// branch at the old revision is admitted (its root filter is stored) and
/// finds every directory's filter, in mode `retire` as in mode `build`;
/// were one missing, a bounded scan could not discover its 300 files and
/// would answer 503 `indexing` on every retry. Blob rows a merge left out
/// are derived again by retention's backfill, which walks the heads.
async fn bounded_search_of_a_branch_at_an_old_revision(mode: Mode) -> Vec<Result<usize, String>> {
    use leep_derive::indexing::{
        Budgets, Freshness, IndexGroup, IndexLeaseConfig, IndexLeases, IndexMode, IndexPolicy,
    };
    let meta = Arc::new(MemoryMetaStore::new());
    let blobs = Arc::new(MemoryBlobStore::new());
    let r = Repo::new(blobs.clone(), meta.clone());
    let mut files = vec![(
        "a/x.rs".to_string(),
        "fn a_x() { alpha_marker }\n".repeat(20),
    )];
    // More files in b/ than a bounded scan may find in filter-less
    // directories with a file budget of zero (256).
    for i in 0..300 {
        files.push((
            format!("b/y{i}.rs"),
            format!("fn b_y{i}() {{ beta_marker_{i} }}\n").repeat(3),
        ));
    }
    let c0 = commit(&r, &[], &files).await;
    r.meta().cas_ref("main", None, Some(c0)).await.unwrap();
    r.derive_search(c0).await.unwrap();
    let c1 = commit(
        &r,
        &[c0],
        &[("a/x.rs".to_string(), "fn a_x() { alpha_two }\n".repeat(20))],
    )
    .await;
    r.meta().cas_ref("main", Some(c0), Some(c1)).await.unwrap();
    r.derive_search(c1).await.unwrap();
    let f = features();
    let opts = CompactOptions {
        content_keep: Some(Duration::ZERO),
        content_skew: Duration::ZERO,
        content_now: Some(now() + 10),
        ..CompactOptions::small(mode)
    };
    let mut st = CompactState::default();
    settle(&r, &opts, &mut st, &f).await;
    let c2 = commit(
        &r,
        &[c1],
        &[("b/y0.rs".to_string(), "fn b_y0() { beta_two }\n".repeat(3))],
    )
    .await;
    r.meta().cas_ref("main", Some(c1), Some(c2)).await.unwrap();
    r.derive_search(c2).await.unwrap();
    settle(&r, &opts, &mut st, &f).await;

    // A server in lease mode holding the search lease, with no file budget
    // (the default is 512 files; the bound scales with it).
    let leases = IndexLeases::new(
        meta.clone(),
        IndexLeaseConfig::new("s", "http://s:7070").with_candidates(IndexGroup::ALL.to_vec()),
    );
    leases.tick().await;
    let policy = IndexPolicy::new(IndexMode::Lease, leases.clone()).with_budgets(Budgets {
        search_files: 0,
        search_bytes: 0,
        ..Budgets::default()
    });
    let s = Repo::new(blobs.clone(), meta.clone()).with_index_policy(Arc::new(policy));
    s.meta().cas_ref("hotfix", None, Some(c0)).await.unwrap();
    // The warmer indexes the new head, and retention's backfill derives the
    // blob rows a merge left out (its filters are all stored).
    s.derive_search(c0).await.unwrap();
    use leep_derive::search::retention::{retention_pass, RetentionOptions, RetentionState};
    retention_pass(
        &s,
        &RetentionOptions::default(),
        now(),
        &mut RetentionState::default(),
    )
    .await
    .unwrap()
    .expect("the retention lease");
    let q = leep_derive::SearchQuery::new("beta_marker_7");
    let mut out = Vec::new();
    for _ in 0..3 {
        out.push(
            match s
                .search_page_indexed("hotfix", &q, None, Freshness::Retry)
                .await
            {
                Ok(a) => Ok(a.value.matches.len()),
                Err(e) => Err(e.to_string()),
            },
        );
        // Between retries the warmer runs again.
        s.derive_search(c0).await.unwrap();
    }
    out
}

#[tokio::test]
async fn bounded_search_of_a_branch_at_an_old_revision_answers() {
    for mode in [Mode::Build, Mode::Retire] {
        let got = bounded_search_of_a_branch_at_an_old_revision(mode).await;
        assert!(
            got.iter().all(|r| r.as_ref().is_ok_and(|n| *n > 0)),
            "{mode}: {got:?}"
        );
    }
}
