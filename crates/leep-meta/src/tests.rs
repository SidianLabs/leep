use std::collections::BTreeMap;
use std::sync::Arc;

use leep_core::memory::check_meta_store_contract;
use leep_core::{Changeset, MetaStore, RefUpdateContext};

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

/// 32 tasks race read-then-CAS on one ref until each has won twice: every
/// win has exactly one log entry, losses have none, and the log replays to
/// the final value.
async fn check_ref_log_race(store: Arc<dyn MetaStore>) {
    let ctx = RefUpdateContext::new("race").actor(Some("racer"));
    let base = store.put_changeset(&cs(500)).await.unwrap();
    assert!(store
        .cas_ref_logged("race", None, Some(base), &ctx)
        .await
        .unwrap());
    let mut tasks = Vec::new();
    for i in 0..32i64 {
        let store = store.clone();
        let ctx = ctx.clone();
        tasks.push(tokio::spawn(async move {
            let mut wins = 0;
            let mut losses = 0;
            while wins < 2 {
                let cur = store.get_ref("race").await.unwrap();
                let id = store
                    .put_changeset(&cs(1000 + i * 10 + wins + losses))
                    .await
                    .unwrap();
                if store
                    .cas_ref_logged("race", cur, Some(id), &ctx)
                    .await
                    .unwrap()
                {
                    wins += 1;
                } else {
                    losses += 1;
                }
            }
            (wins, losses)
        }));
    }
    let mut wins = 1;
    for t in tasks {
        wins += t.await.unwrap().0;
    }
    let entries = store.ref_log(Some("race"), None, 10_000).await.unwrap();
    assert_eq!(entries.len() as i64, wins, "one entry per successful CAS");
    let mut prev = None;
    for e in entries.iter().rev() {
        assert_eq!(e.old, prev, "entry {} continues the previous one", e.seq);
        prev = e.new;
    }
    assert_eq!(
        prev,
        store.get_ref("race").await.unwrap(),
        "replays to head"
    );
    assert!(entries.windows(2).all(|w| w[0].seq > w[1].seq));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqlite_memory() {
    let s = SqliteMetaStore::connect("sqlite::memory:").await.unwrap();
    check_meta_store_contract(&s).await;
    check_cas_race(Arc::new(s)).await;
    let s = SqliteMetaStore::connect("sqlite::memory:").await.unwrap();
    check_ref_log_race(Arc::new(s)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn memory_ref_log_race() {
    check_ref_log_race(open_meta_store("mem://").await.unwrap()).await;
}

/// The schema before the ref log existed; a database made by that version
/// must open and start logging.
const OLD_SQLITE_SCHEMA: &[&str] = &[
    "CREATE TABLE changesets (id BLOB PRIMARY KEY, data BLOB NOT NULL)",
    "CREATE TABLE refs (name TEXT PRIMARY KEY, target BLOB NOT NULL)",
    "CREATE TABLE derived (kind TEXT NOT NULL, key BLOB NOT NULL, \
     value BLOB NOT NULL, PRIMARY KEY (kind, key))",
];

/// Moves made after the upgrade are logged; the first one's `old` is the
/// value the ref had before the upgrade.
async fn check_upgraded(store: &dyn MetaStore, old_head: leep_core::ChangesetId) {
    assert_eq!(store.get_ref("legacy").await.unwrap(), Some(old_head));
    assert!(store.ref_log(None, None, 10).await.unwrap().is_empty());
    let next = store.put_changeset(&cs(2)).await.unwrap();
    assert!(store
        .cas_ref_logged(
            "legacy",
            Some(old_head),
            Some(next),
            &RefUpdateContext::new("land")
        )
        .await
        .unwrap());
    let log = store.ref_log(Some("legacy"), None, 10).await.unwrap();
    assert_eq!(log.len(), 1);
    assert_eq!((log[0].old, log[0].new), (Some(old_head), Some(next)));
    check_meta_store_contract(store).await;
}

#[tokio::test]
async fn sqlite_old_schema_upgrades() {
    let dir = std::env::temp_dir().join(format!("leep-meta-old-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let url = format!("sqlite://{}?mode=rwc", dir.join("old.db").display());
    let old = cs(1);
    let old_head = old.id();
    {
        use std::str::FromStr;
        let opts = sqlx::sqlite::SqliteConnectOptions::from_str(&url).unwrap();
        let pool = sqlx::SqlitePool::connect_with(opts).await.unwrap();
        for sql in OLD_SQLITE_SCHEMA {
            sqlx::query(sql).execute(&pool).await.unwrap();
        }
        sqlx::query("INSERT INTO changesets (id, data) VALUES (?, ?)")
            .bind(old_head.0.as_bytes().as_slice())
            .bind(old.encode())
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO refs (name, target) VALUES ('legacy', ?)")
            .bind(old_head.0.as_bytes().as_slice())
            .execute(&pool)
            .await
            .unwrap();
        pool.close().await;
    }
    let s = SqliteMetaStore::connect(&url).await.unwrap();
    check_upgraded(&s, old_head).await;
    // Opening again keeps the log.
    drop(s);
    let s = SqliteMetaStore::connect(&url).await.unwrap();
    assert!(!s.ref_log(None, None, 10).await.unwrap().is_empty());
    std::fs::remove_dir_all(&dir).ok();
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
    let Some((s, _)) = pg_store("lograce").await else {
        return;
    };
    check_ref_log_race(Arc::new(s)).await;
}

#[tokio::test]
async fn postgres_old_schema_upgrades() {
    let Some(url) = pg_url() else {
        return;
    };
    let name = schema("old");
    let old = cs(1);
    let old_head = old.id();
    {
        use std::str::FromStr;
        let opts = sqlx::postgres::PgConnectOptions::from_str(&url).unwrap();
        // One connection, so the search_path set below applies to every statement.
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect_with(opts)
            .await
            .unwrap();
        let q = format!("\"{name}\"");
        for sql in [
            format!("DROP SCHEMA IF EXISTS {q} CASCADE"),
            format!("CREATE SCHEMA {q}"),
            format!("SET search_path TO {q}"),
            "CREATE TABLE changesets (id BYTEA PRIMARY KEY, data BYTEA NOT NULL)".into(),
            "CREATE TABLE refs (name TEXT PRIMARY KEY, target BYTEA NOT NULL)".into(),
            "CREATE TABLE derived (kind TEXT NOT NULL, key BYTEA NOT NULL, \
             value BYTEA NOT NULL, PRIMARY KEY (kind, key))"
                .into(),
        ] {
            sqlx::query(&sql).execute(&pool).await.unwrap();
        }
        sqlx::query("INSERT INTO changesets (id, data) VALUES ($1, $2)")
            .bind(old_head.0.as_bytes().as_slice())
            .bind(old.encode())
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO refs (name, target) VALUES ('legacy', $1)")
            .bind(old_head.0.as_bytes().as_slice())
            .execute(&pool)
            .await
            .unwrap();
        pool.close().await;
    }
    let s = PostgresMetaStore::connect_with_schema(&url, &name)
        .await
        .unwrap();
    check_upgraded(&s, old_head).await;
}
