//! The warmer's ref-log tail (ADR 0058, phase 0) on the database backends:
//! it misses no move across a gap in `seq` left by a rolled-back log insert,
//! and a page of the tail that ends exactly at the last entry it saw is not
//! mistaken for a gap too long to read.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use leep_core::memory::MemoryBlobStore;
use leep_core::{
    BlobStore, Changeset, ChangesetId, FileChange, FileKind, LandId, LandKey, MetaStore,
    RefUpdateContext, RepoPath,
};
use leep_derive::derived::{DerivedKind, HistoryTree};
use leep_derive::warm::{Warmer, TAIL_PAGE};
use leep_derive::Repo;
use leep_meta::{PostgresMetaStore, SqliteMetaStore};

async fn commit(r: &Repo, parent: Option<ChangesetId>, msg: &str) -> ChangesetId {
    let content = r.blobs().put(Bytes::from(msg.to_string())).await.unwrap();
    r.create_changeset(Changeset {
        parents: parent.into_iter().collect(),
        author: "agent".into(),
        timestamp: 1,
        message: msg.into(),
        changes: BTreeMap::from([(
            RepoPath::new(format!("f/{msg}")).unwrap(),
            FileChange::Modify {
                content,
                kind: FileKind::Regular,
                size: msg.len() as u64,
            },
        )]),
        extra: BTreeMap::new(),
    })
    .await
    .unwrap()
}

async fn indexed(r: &Repo, id: ChangesetId) -> bool {
    r.meta()
        .get_derived(HistoryTree::NAME, &id.0)
        .await
        .unwrap()
        .is_some()
}

fn land(id: u8, changeset: ChangesetId) -> RefUpdateContext {
    RefUpdateContext::new("land").land(Some(LandKey {
        id: LandId::from_bytes(&[id; 16]).unwrap(),
        changeset,
    }))
}

async fn tail(meta: Arc<dyn MetaStore>, expect_gap: bool) {
    let blobs: Arc<dyn BlobStore> = Arc::new(MemoryBlobStore::new());
    let r = Repo::new(blobs, meta.clone());
    // Only the first pass lists refs, so later passes must find moves in
    // the ref log.
    let mut w = Warmer::new(r.clone(), false).with_full_listing_every(Duration::from_secs(3600));
    let base = commit(&r, None, "base").await;
    assert!(meta
        .cas_ref_logged("main", None, Some(base), &land(1, base))
        .await
        .unwrap());
    assert_eq!(w.pass().await.unwrap().warmed, 1);

    // A move whose log insert fails (the land id is already used on this
    // ref) rolls back after taking a `seq`.
    let next = commit(&r, Some(base), "next").await;
    assert!(meta
        .cas_ref_logged("main", Some(base), Some(next), &land(1, next))
        .await
        .is_err());
    assert_eq!(meta.get_ref("main").await.unwrap(), Some(base));
    assert!(meta
        .cas_ref_logged("main", Some(base), Some(next), &land(2, next))
        .await
        .unwrap());
    let seqs: Vec<u64> = meta
        .ref_log(None, None, 10)
        .await
        .unwrap()
        .iter()
        .map(|e| e.seq)
        .collect();
    if expect_gap {
        assert!(seqs[0] > seqs[1] + 1, "a seq was skipped: {seqs:?}");
    }
    let report = w.pass().await.unwrap();
    assert_eq!((report.refs, report.warmed), (1, 1), "found across the gap");
    assert!(indexed(&r, next).await);

    // Exactly one page of new entries: the page is full, and the next page
    // starts at the entry the last pass saw.
    let mut heads = Vec::new();
    for i in 0..TAIL_PAGE {
        let id = commit(&r, Some(next), &format!("r{i}")).await;
        assert!(meta
            .cas_ref(&format!("r/{i:03}"), None, Some(id))
            .await
            .unwrap());
        heads.push(id);
    }
    let report = w.pass().await.unwrap();
    assert_eq!(
        (report.refs, report.warmed),
        (TAIL_PAGE, TAIL_PAGE),
        "read from the tail, not a listing of all {} refs",
        TAIL_PAGE + 1
    );
    for id in heads {
        assert!(indexed(&r, id).await);
    }
    // Nothing new: nothing looked at.
    assert_eq!(w.pass().await.unwrap().refs, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tail_on_postgres() {
    let Ok(url) = std::env::var("LEEP_TEST_POSTGRES_URL") else {
        eprintln!("skipping: LEEP_TEST_POSTGRES_URL not set");
        return;
    };
    let schema = format!("leep_test_warm_tail_{}", std::process::id());
    let meta = PostgresMetaStore::connect_with_schema(&url, &schema)
        .await
        .unwrap();
    tail(Arc::new(meta), true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tail_on_sqlite() {
    let dir = tempfile::tempdir().unwrap();
    let url = format!("sqlite://{}?mode=rwc", dir.path().join("meta.db").display());
    let meta = SqliteMetaStore::connect(&url).await.unwrap();
    tail(Arc::new(meta), false).await;
}
