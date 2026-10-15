use std::collections::BTreeMap;
use std::sync::Arc;

use leep_core::memory::check_meta_store_contract;
use leep_core::{Changeset, MetaStore};

use super::*;

fn cs(n: i64) -> Changeset {
    Changeset {
        parents: vec![],
        author: "t".into(),
        timestamp: n,
        message: format!("m{n}"),
        changes: BTreeMap::new(),
        extra: BTreeMap::new(),
    }
}

/// 32 tasks race a CAS from the same expected value; exactly one wins.
async fn check_cas_race(store: Arc<dyn MetaStore>) {
    let base = store.put_changeset(&cs(0)).await.unwrap();
    assert!(store.cas_ref("main", None, Some(base)).await.unwrap());
    let mut tasks = Vec::new();
    for i in 1..=32 {
        let store = store.clone();
        tasks.push(tokio::spawn(async move {
            let id = store.put_changeset(&cs(i)).await.unwrap();
            store.cas_ref("main", Some(base), Some(id)).await.unwrap()
        }));
    }
    let mut wins = 0;
    for t in tasks {
        wins += t.await.unwrap() as usize;
    }
    assert_eq!(wins, 1);
    // Same race for creation of an absent ref.
    let mut tasks = Vec::new();
    for i in 1..=32 {
        let store = store.clone();
        tasks.push(tokio::spawn(async move {
            let id = store.put_changeset(&cs(i)).await.unwrap();
            store.cas_ref("new", None, Some(id)).await.unwrap()
        }));
    }
    let mut wins = 0;
    for t in tasks {
        wins += t.await.unwrap() as usize;
    }
    assert_eq!(wins, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqlite_memory() {
    let s = SqliteMetaStore::connect("sqlite::memory:").await.unwrap();
    check_meta_store_contract(&s).await;
    check_cas_race(Arc::new(s)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqlite_file_persists_and_races() {
    let dir = std::env::temp_dir().join(format!("leep-meta-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let url = format!("sqlite://{}?mode=rwc", dir.join("meta.db").display());
    let s = SqliteMetaStore::connect(&url).await.unwrap();
    check_meta_store_contract(&s).await;
    check_cas_race(Arc::new(SqliteMetaStore::connect(&url).await.unwrap())).await;
    let id = s.put_changeset(&cs(99)).await.unwrap();
    drop(s);
    // Reopen: idempotent migration, data survives.
    let s = SqliteMetaStore::connect(&url).await.unwrap();
    assert_eq!(s.get_changeset(&id).await.unwrap(), Some(cs(99)));
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn sqlite_detects_corruption() {
    let s = SqliteMetaStore::connect("sqlite::memory:").await.unwrap();
    let id = s.put_changeset(&cs(1)).await.unwrap();
    let other = cs(2).encode();
    sqlx::query("UPDATE changesets SET data = ?")
        .bind(other)
        .execute(&s.pool)
        .await
        .unwrap();
    assert!(matches!(
        s.get_changeset(&id).await,
        Err(leep_core::Error::Storage(_))
    ));
}

#[tokio::test]
async fn open_dispatch() {
    let m = open_meta_store("mem://").await.unwrap();
    check_meta_store_contract(&*m).await;
    let m = open_meta_store("sqlite::memory:").await.unwrap();
    check_meta_store_contract(&*m).await;
    assert!(open_meta_store("bogus://x").await.is_err());
}

fn pg_url() -> Option<String> {
    let url = std::env::var("LEEP_TEST_POSTGRES_URL").ok();
    if url.is_none() {
        eprintln!("skipping: LEEP_TEST_POSTGRES_URL not set");
    }
    url
}

fn schema(name: &str) -> String {
    format!("leep_test_{name}_{}", std::process::id())
}

async fn pg_store(name: &str) -> Option<(PostgresMetaStore, String)> {
    let url = pg_url()?;
    let s = PostgresMetaStore::connect_with_schema(&url, &schema(name))
        .await
        .unwrap();
    Some((s, url))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_contract() {
    let Some((s, url)) = pg_store("contract").await else {
        return;
    };
    check_meta_store_contract(&s).await;
    // Re-running migrations is a no-op.
    PostgresMetaStore::connect_with_schema(&url, &schema("contract"))
        .await
        .unwrap();
    let m = open_meta_store(&url).await.unwrap();
    m.list_refs().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_cas_race() {
    let Some((s, _)) = pg_store("race").await else {
        return;
    };
    check_cas_race(Arc::new(s)).await;
}
