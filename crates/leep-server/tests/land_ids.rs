//! Land ids (ADR 0057) on every metadata backend: a retry under the same id
//! after more intervening lands than the landed-copy scan reads answers with
//! the original result and lands nothing, an id names one changeset, and the
//! same land sent at once through several servers lands once.

use std::collections::BTreeMap;
use std::sync::Arc;

use bytes::Bytes;
use leep_core::api::LandResponse;
use leep_core::memory::{MemoryBlobStore, MemoryMetaStore};
use leep_core::{
    BlobStore, Changeset, ChangesetId, Error, FileChange, FileKind, LandId, MetaStore, RepoPath,
};
use leep_derive::Repo;
use leep_meta::{PostgresMetaStore, SqliteMetaStore};

/// Above `land`'s landed-copy scan (1024 changesets).
const INTERVENING: usize = 1100;

async fn commit(r: &Repo, parent: Option<ChangesetId>, path: &str, msg: &str) -> ChangesetId {
    let content = r.blobs().put(Bytes::from(msg.to_string())).await.unwrap();
    r.create_changeset(Changeset {
        parents: parent.into_iter().collect(),
        author: "agent".into(),
        timestamp: 1,
        message: msg.into(),
        changes: BTreeMap::from([(
            RepoPath::new(path).unwrap(),
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

fn landed(r: &LandResponse) -> ChangesetId {
    match r {
        LandResponse::Landed { landed, .. } => *landed,
        other => panic!("{other:?}"),
    }
}

async fn land_ids(meta: Arc<dyn MetaStore>) {
    let blobs: Arc<dyn BlobStore> = Arc::new(MemoryBlobStore::new());
    let server = || Repo::new(blobs.clone(), meta.clone());
    let r = server();
    let base = commit(&r, None, "base", "base").await;
    r.land(base, "main").await.unwrap();
    let feature = commit(&r, Some(base), "feature", "feature").await;
    let other = commit(&r, Some(base), "other", "other").await;
    r.land(other, "main").await.unwrap();

    let id = LandId::from_bytes(&[1; 32]).unwrap();
    let first = r
        .land_keyed(feature, "main", None, true, Some(id.clone()))
        .await
        .unwrap();
    let copy = landed(&first);
    assert_ne!(copy, feature, "rebased onto other");

    let mut head = copy;
    for i in 0..INTERVENING {
        let next = commit(&r, Some(head), &format!("n/{i}"), &format!("n{i}")).await;
        r.land_as(next, "main", Some("others")).await.unwrap();
        head = next;
    }
    let entries = meta
        .ref_log(Some("main"), None, usize::MAX)
        .await
        .unwrap()
        .len();
    // A different server instance, as a client retrying elsewhere would reach.
    let again = server()
        .land_keyed(feature, "main", None, true, Some(id.clone()))
        .await
        .unwrap();
    assert_eq!(landed(&again), copy, "the original result");
    assert!(matches!(again, LandResponse::Landed { rebased: true, .. }));
    assert_eq!(
        meta.get_ref("main").await.unwrap(),
        Some(head),
        "nothing landed"
    );
    assert_eq!(
        meta.ref_log(Some("main"), None, usize::MAX)
            .await
            .unwrap()
            .len(),
        entries
    );

    // One id names one changeset.
    let stranger = commit(&r, Some(base), "stranger", "stranger").await;
    let e = r
        .land_keyed(stranger, "main", None, true, Some(id.clone()))
        .await
        .unwrap_err();
    assert!(matches!(e, Error::Invalid(_)), "{e}");

    // Ids are per ref: the same id lands on another ref.
    let on_dev = r
        .land_keyed(stranger, "dev", None, true, Some(id))
        .await
        .unwrap();
    assert_eq!(landed(&on_dev), stranger);

    // The same land sent at once through several servers lands once, and
    // every request is answered with that one copy.
    let racer = commit(&r, Some(base), "racer", "racer").await;
    let id = LandId::from_bytes(&[2; 16]).unwrap();
    let tasks: Vec<_> = (0..8)
        .map(|_| {
            let s = server();
            let id = id.clone();
            tokio::spawn(async move { s.land_keyed(racer, "main", None, true, Some(id)).await })
        })
        .collect();
    let mut answers = Vec::new();
    for t in tasks {
        answers.push(landed(&t.await.unwrap().unwrap()));
    }
    assert!(answers.windows(2).all(|w| w[0] == w[1]), "{answers:?}");
    let moves = meta
        .ref_log(Some("main"), None, usize::MAX)
        .await
        .unwrap()
        .into_iter()
        .filter(|e| e.message.as_deref() == Some(&format!("landed {racer}")))
        .count();
    assert_eq!(moves, 1, "one move for the racing land");
    assert_eq!(meta.get_ref("main").await.unwrap(), Some(answers[0]));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn land_ids_on_memory() {
    land_ids(Arc::new(MemoryMetaStore::new())).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn land_ids_on_sqlite() {
    let dir = tempfile::tempdir().unwrap();
    let url = format!("sqlite://{}?mode=rwc", dir.path().join("m.db").display());
    land_ids(Arc::new(SqliteMetaStore::connect(&url).await.unwrap())).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn land_ids_on_postgres() {
    let Ok(url) = std::env::var("LEEP_TEST_POSTGRES_URL") else {
        eprintln!("skipping: LEEP_TEST_POSTGRES_URL not set");
        return;
    };
    let schema = format!("leep_test_land_ids_{}", std::process::id());
    let meta = PostgresMetaStore::connect_with_schema(&url, &schema)
        .await
        .unwrap();
    land_ids(Arc::new(meta)).await;
}
