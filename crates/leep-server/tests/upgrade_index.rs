//! `upgrade-index`'s plan and run (leep_derive::integrity::upgrade) against
//! the SQLite and Postgres backends, whose counts and drop walk differ from
//! the in-memory store's: a retired kind's rows and a missing current kind
//! are both planned, the run derives one and drops the other and nothing
//! else, a second run has nothing to do, and a run stopped part way is
//! finished by the next.

use std::collections::BTreeMap;
use std::sync::Arc;

use bytes::Bytes;
use leep_core::memory::MemoryBlobStore;
use leep_core::{
    BlobStore, Changeset, ChangesetId, FileChange, FileKind, Hash, MetaStore, RepoPath,
};
use leep_derive::integrity::{
    drop_kinds, plan_upgrade, run_upgrade, IndexStatus, Quiet, UpgradeOptions,
};
use leep_derive::Repo;
use leep_meta::{PostgresMetaStore, SqliteMetaStore};
use leep_testkit::{Fault, FaultPlan, FaultyMetaStore, Op};

const CURRENT: &[&str] = &[
    "generation",
    "skiplist",
    "last_merge-v1",
    "root_tree",
    "history-tree-v4",
    "history-node-v4",
    "log-filter-v1",
];

async fn commit(r: &Repo, parents: &[ChangesetId], path: &str, body: String) -> ChangesetId {
    let content = r.blobs().put(Bytes::from(body.clone())).await.unwrap();
    let mut changes = BTreeMap::new();
    changes.insert(
        RepoPath::new(path).unwrap(),
        FileChange::Modify {
            content,
            kind: FileKind::Regular,
            size: body.len() as u64,
        },
    );
    r.create_changeset(Changeset {
        parents: parents.to_vec(),
        author: format!("dev{}", body.len() % 3),
        timestamp: 1,
        message: body,
        changes,
        extra: BTreeMap::new(),
    })
    .await
    .unwrap()
}

/// `n` changesets on `main` with a merge every 50, and `feature` part way.
async fn history(r: &Repo, n: u32) {
    let mut tip = commit(r, &[], "a.txt", "0\n".into()).await;
    let mut side = tip;
    for i in 1..n {
        let path = format!("d{}/f{}.txt", i % 5, i % 11);
        if i % 50 == 0 {
            side = commit(r, &[side], "side.txt", format!("side {i}\n")).await;
            tip = commit(r, &[tip, side], &path, format!("merge {i}\n")).await;
        } else {
            tip = commit(r, &[tip], &path, format!("{i}\n")).await;
        }
        if i == n / 2 {
            assert!(r.meta().cas_ref("feature", None, Some(tip)).await.unwrap());
        }
    }
    assert!(r.meta().cas_ref("main", None, Some(tip)).await.unwrap());
}

async fn dump(meta: &dyn MetaStore, kinds: &[&str]) -> Vec<(String, Hash, Bytes)> {
    let mut out = Vec::new();
    for kind in kinds {
        for (k, v) in meta.list_derived(kind, None, usize::MAX).await.unwrap() {
            out.push((kind.to_string(), k, v));
        }
    }
    out
}

fn opts(drop_retired: bool) -> UpgradeOptions {
    UpgradeOptions {
        jobs: 4,
        drop_retired,
    }
}

async fn upgrade_workflow(meta: Arc<dyn MetaStore>) {
    let blobs: Arc<dyn BlobStore> = Arc::new(MemoryBlobStore::new());
    let repo = Repo::new(blobs.clone(), meta.clone());
    history(&repo, 400).await;

    // Everything derived once, as a server would: the rows to come back to.
    let plan = plan_upgrade(&repo, &Quiet).await.unwrap();
    assert!(plan.reachable >= 400, "{plan:#?}");
    run_upgrade(&repo, &plan, &opts(false), &Quiet)
        .await
        .unwrap();
    let want = dump(&*meta, CURRENT).await;
    let blame_key = Hash([42; 32]);
    meta.put_derived("blame", &blame_key, Bytes::from_static(b"kept"))
        .await
        .unwrap();

    // An old store: no v4 index, and v3 rows an older server wrote.
    drop_kinds(&repo, &["history-tree-v4"], false, &Quiet)
        .await
        .unwrap();
    let ids: Vec<Hash> = meta
        .list_derived("generation", None, usize::MAX)
        .await
        .unwrap()
        .into_iter()
        .map(|(k, _)| k)
        .collect();
    for (i, id) in ids.iter().enumerate() {
        meta.put_derived("history-tree-v3", id, vec![3; 64].into())
            .await
            .unwrap();
        let node = Hash::of_blob(&(i as u64).to_le_bytes());
        meta.put_derived("history-node-v3", &node, vec![4; 500].into())
            .await
            .unwrap();
    }

    let repo = Repo::new(blobs.clone(), meta.clone());
    let plan = plan_upgrade(&repo, &Quiet).await.unwrap();
    assert_eq!(plan.to_derive(), ["history-tree-v4"], "{plan:#?}");
    let tree = plan
        .current
        .iter()
        .find(|k| k.kind == "history-tree-v4")
        .unwrap();
    assert_eq!(tree.status, IndexStatus::Missing);
    assert_eq!(tree.missing, plan.reachable);
    assert_eq!(plan.to_drop(), ["history-tree-v3"]);
    let n = ids.len() as u64;
    assert_eq!(plan.retired[0].rows["history-tree-v3"], n);
    assert_eq!(plan.retired[0].rows["history-node-v3"], n);
    assert_eq!(plan.retired[0].value_bytes, Some(n * 564));

    // Stopped part way by a failing write, as a kill would stop it.
    let faults = FaultPlan::new(11);
    faults.script(Op::PutDerived, 40, Fault::Fail);
    let faulty = Repo::new(
        blobs.clone(),
        Arc::new(FaultyMetaStore::new(meta.clone(), faults.clone())),
    );
    assert!(run_upgrade(&faulty, &plan, &opts(true), &Quiet)
        .await
        .is_err());
    assert_eq!(faults.injected().total(), 1);
    let part = meta.count_derived("history-tree-v4").await.unwrap();
    assert!(part < plan.reachable, "{part}");
    // The retired rows stay until the current ones are complete.
    assert_eq!(meta.count_derived("history-tree-v3").await.unwrap(), n);

    let repo = Repo::new(blobs.clone(), meta.clone());
    let plan = plan_upgrade(&repo, &Quiet).await.unwrap();
    assert_eq!(plan.to_derive(), ["history-tree-v4"]);
    let r = run_upgrade(&repo, &plan, &opts(true), &Quiet)
        .await
        .unwrap();
    assert_eq!(r.derived["history-tree-v4"], plan.reachable - part);
    let dropped = r.dropped.as_ref().unwrap();
    assert_eq!(dropped.rows["history-tree-v3"], n);
    assert_eq!(dropped.rows["history-node-v3"], n);
    assert!(r.after.nothing_to_do(), "{:#?}", r.after);
    assert_eq!(dump(&*meta, CURRENT).await, want);
    assert_eq!(
        meta.get_derived("blame", &blame_key)
            .await
            .unwrap()
            .unwrap(),
        Bytes::from_static(b"kept")
    );
    for k in ["history-tree-v3", "history-node-v3"] {
        assert_eq!(meta.count_derived(k).await.unwrap(), 0, "{k}");
    }

    // Nothing to do, and doing it changes nothing.
    let plan = plan_upgrade(&repo, &Quiet).await.unwrap();
    assert!(plan.nothing_to_do(), "{plan:#?}");
    let r = run_upgrade(&repo, &plan, &opts(true), &Quiet)
        .await
        .unwrap();
    assert!(r.derived.is_empty() && r.dropped.is_none());
    assert_eq!(dump(&*meta, CURRENT).await, want);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn upgrade_index_on_sqlite() {
    let dir = tempfile::tempdir().unwrap();
    let url = format!("sqlite://{}?mode=rwc", dir.path().join("m.db").display());
    let meta = Arc::new(SqliteMetaStore::connect(&url).await.unwrap());
    upgrade_workflow(meta).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn upgrade_index_on_postgres() {
    let Ok(url) = std::env::var("LEEP_TEST_POSTGRES_URL") else {
        eprintln!("skipping: LEEP_TEST_POSTGRES_URL not set");
        return;
    };
    let schema = format!("leep_test_upgrade_{}", std::process::id());
    let meta = Arc::new(
        PostgresMetaStore::connect_with_schema(&url, &schema)
            .await
            .unwrap(),
    );
    upgrade_workflow(meta).await;
}
