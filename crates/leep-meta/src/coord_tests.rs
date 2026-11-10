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

/// A store opened with the schema of migration 0009 keeps sessions but no
/// notes (ADR 0098): note calls answer 404, and the sweep does nothing.
#[tokio::test]
async fn a_schema_before_notes_keeps_no_notes() {
    use leep_core::coord::{NoteFilter, NoteSweep};
    let migs = &SQLITE_MIGRATIONS[..9];
    let s = SqliteMetaStore::connect_with_migrations("sqlite::memory:", SchemaMode::Migrate, migs)
        .await
        .unwrap();
    let c = s.coord_store().unwrap();
    let u = SessionUpdate::new("ada", "k", SessionOrigin::Declared, 1);
    assert!(c.upsert_session(&u).await.unwrap().1);
    let f = NoteFilter {
        limit: 10,
        ..Default::default()
    };
    assert!(matches!(
        c.list_notes(&f).await,
        Err(leep_core::Error::NotFound(_))
    ));
    assert!(matches!(
        c.get_note(1).await,
        Err(leep_core::Error::NotFound(_))
    ));
    assert_eq!(c.sweep_notes(1, 10).await.unwrap(), NoteSweep::default());
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

/// Beats lost (Postgres empties the UNLOGGED table after a crash) never
/// expire live sessions: the sweep first gives each active session
/// without a beat one at `now`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lost_beats_do_not_expire_live_sessions() {
    let s = SqliteMetaStore::connect("sqlite::memory:").await.unwrap();
    let c = s.coord_store().unwrap();
    let id = lose_beats_setup(&*c).await;
    sqlx::query("DELETE FROM session_beats")
        .execute(&s.pool)
        .await
        .unwrap();
    lose_beats_check(&*c, id).await;
    if let Some((s, _schema)) = crate::tests::pg_store("coordbeats").await {
        let c = s.coord_store().unwrap();
        let id = lose_beats_setup(&*c).await;
        sqlx::query("DELETE FROM session_beats")
            .execute(&s.pool)
            .await
            .unwrap();
        lose_beats_check(&*c, id).await;
    }
}

const T0: i64 = 1_700_000_000;

async fn lose_beats_setup(c: &dyn CoordStore) -> u64 {
    let u = SessionUpdate::new("ada", "k", SessionOrigin::Declared, T0);
    c.upsert_session(&u).await.unwrap().0.id
}

async fn lose_beats_check(c: &dyn CoordStore, id: u64) {
    use leep_core::coord::{SessionState, EXPIRE_AFTER_SECS};
    let later = T0 + EXPIRE_AFTER_SECS + 5;
    let r = c.sweep_sessions(later, 100).await.unwrap();
    assert_eq!(r.expired, 0, "{r:?}");
    let s = c.get_session(id).await.unwrap().unwrap();
    assert_eq!((s.state, s.last_seen), (SessionState::Active, later));
    // Without new beats it expires a day after that.
    let r = c
        .sweep_sessions(later + EXPIRE_AFTER_SECS + 1, 100)
        .await
        .unwrap();
    assert_eq!(r.expired, 1);
}
