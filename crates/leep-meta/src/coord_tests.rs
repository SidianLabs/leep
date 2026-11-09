//! The coordination contract (ADR 0095) on both SQL backends.

use leep_core::coord::{CoordStore, SessionOrigin, SessionUpdate};
use leep_core::memory::check_coord_contract;
use leep_core::MetaStore;

use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqlite_memory_passes_the_coord_contract() {
    let s = SqliteMetaStore::connect("sqlite::memory:").await.unwrap();
    check_coord_contract(&*s.coord_store().unwrap()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqlite_file_passes_the_coord_contract() {
    let dir = std::env::temp_dir().join(format!("leep-coord-{}", std::process::id()));
    std::fs::remove_dir_all(&dir).ok();
    std::fs::create_dir_all(&dir).unwrap();
    let url = format!("sqlite://{}?mode=rwc", dir.join("meta.db").display());
    let s = SqliteMetaStore::connect(&url).await.unwrap();
    check_coord_contract(&*s.coord_store().unwrap()).await;
    drop(s);
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_passes_the_coord_contract() {
    let Some((s, _schema)) = crate::tests::pg_store("coord").await else {
        eprintln!("skipping: LEEP_TEST_POSTGRES_URL is not set");
        return;
    };
    check_coord_contract(&*s.coord_store().unwrap()).await;
}

/// A store opened with a schema from before migration 0009 keeps no
/// sessions; everything else works as before.
#[tokio::test]
async fn an_older_schema_has_no_coord_store() {
    let migs = &SQLITE_MIGRATIONS[..8];
    let s = SqliteMetaStore::connect_with_migrations("sqlite::memory:", SchemaMode::Migrate, migs)
        .await
        .unwrap();
    assert!(s.coord_store().is_none());
    let s = SqliteMetaStore::connect("sqlite::memory:").await.unwrap();
    let c = s.coord_store().unwrap();
    let u = SessionUpdate::new("ada", "k", SessionOrigin::Declared, 1);
    assert!(c.upsert_session(&u).await.unwrap().1);
}

/// Paths compare bytewise on Postgres too: a directory's range scan holds
/// what is below it and nothing beside it, whatever the database's
/// collation would say about `/`, `-` and `.`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_path_ranges_are_bytewise() {
    let Some((s, _schema)) = crate::tests::pg_store("coordcoll").await else {
        return;
    };
    path_ranges(&*s.coord_store().unwrap()).await;
    let s = SqliteMetaStore::connect("sqlite::memory:").await.unwrap();
    path_ranges(&*s.coord_store().unwrap()).await;
}

async fn path_ranges(c: &dyn CoordStore) {
    use leep_core::coord::SessionFilter;
    let paths = ["a-b/x", "a.b/x", "a/b", "a/b-c", "a0", "A/b", "a b/x"];
    for (n, p) in paths.iter().enumerate() {
        let mut u = SessionUpdate::new("ada", &format!("k{n}"), SessionOrigin::Inferred, 1);
        u.touch = vec![p.to_string()];
        c.upsert_session(&u).await.unwrap();
    }
    let under = |p: &str| SessionFilter {
        path: Some(p.into()),
        limit: 100,
        ..Default::default()
    };
    let found = |v: Vec<leep_core::coord::Session>| {
        let mut t: Vec<String> = v.into_iter().flat_map(|s| s.touched).collect();
        t.sort();
        t
    };
    assert_eq!(
        found(c.list_sessions(&under("a")).await.unwrap()),
        ["a/b", "a/b-c"]
    );
    assert_eq!(
        found(c.list_sessions(&under("a/b")).await.unwrap()),
        ["a/b"]
    );
    assert_eq!(
        found(c.list_sessions(&under("a-b")).await.unwrap()),
        ["a-b/x"]
    );
    assert_eq!(
        found(c.list_sessions(&under("a/b/c/d")).await.unwrap()),
        ["a/b"]
    );
}
