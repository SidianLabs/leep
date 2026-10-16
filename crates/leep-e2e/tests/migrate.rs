//! Schema migrations through the real `leep-server` binary (ADR 0023): an
//! un-versioned database made by the code before `schema_migrations` is
//! adopted by `migrate`, `--no-migrate` refuses until then, a database from a
//! newer server is refused, and a server started on the result serves the
//! data that was already there.

mod common;

use std::process::Command;
use std::str::FromStr;

use common::*;

fn server(stores: &Stores, args: &[&str]) -> std::process::Output {
    Command::new(bin("leep-server"))
        .args(["--meta", &stores.meta, "--blobs", &stores.blobs])
        .args(args)
        .env("RUST_LOG", "warn")
        .output()
        .expect("spawn leep-server")
}

/// The database as the code before ADR 0023 left it: tables, no history.
fn make_unversioned(url: &str) -> leep_core::ChangesetId {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let cs = leep_core::Changeset {
            parents: vec![],
            author: "old".into(),
            timestamp: 1,
            message: "from the old days".into(),
            changes: Default::default(),
            extra: Default::default(),
        };
        let id = cs.id();
        let opts = sqlx::sqlite::SqliteConnectOptions::from_str(url).unwrap();
        let pool = sqlx::SqlitePool::connect_with(opts).await.unwrap();
        for sql in [
            "CREATE TABLE IF NOT EXISTS changesets (id BLOB PRIMARY KEY, data BLOB NOT NULL)",
            "CREATE TABLE IF NOT EXISTS refs (name TEXT PRIMARY KEY, target BLOB NOT NULL)",
            "CREATE TABLE IF NOT EXISTS derived (kind TEXT NOT NULL, key BLOB NOT NULL, \
             value BLOB NOT NULL, PRIMARY KEY (kind, key))",
            "CREATE TABLE IF NOT EXISTS ref_log (seq INTEGER PRIMARY KEY AUTOINCREMENT, \
             name TEXT NOT NULL, old_target BLOB, new_target BLOB, time INTEGER NOT NULL, \
             actor TEXT, operation TEXT NOT NULL, message TEXT)",
            "CREATE INDEX IF NOT EXISTS ref_log_name ON ref_log (name, seq)",
        ] {
            sqlx::query(sql).execute(&pool).await.unwrap();
        }
        sqlx::query("INSERT INTO changesets (id, data) VALUES (?, ?)")
            .bind(id.0.as_bytes().as_slice())
            .bind(cs.encode())
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO refs (name, target) VALUES ('main', ?)")
            .bind(id.0.as_bytes().as_slice())
            .execute(&pool)
            .await
            .unwrap();
        pool.close().await;
        id
    })
}

#[test]
fn migrate_adopts_an_unversioned_database() {
    let _g = serial();
    let dir = tempfile::tempdir().unwrap();
    let stores = Stores::sqlite(dir.path());
    let head = make_unversioned(&stores.meta);

    let out = server(&stores, &["migrate", "--status"]);
    assert_ok(&out, "migrate --status");
    assert!(stdout(&out).contains("to adopt"), "{}", stdout(&out));
    // Read-only: still no history.
    let out = server(&stores, &["migrate", "--dry-run"]);
    assert_ok(&out, "migrate --dry-run");

    // --no-migrate (flag or variable) refuses to run anything on it.
    for cmd in [
        &["gc", "--dry-run"][..],
        &["verify"],
        &["repair", "drop", "--kind", "generation", "--dry-run"],
    ] {
        let out = server(&stores, &[&["--no-migrate"][..], cmd].concat());
        assert!(!out.status.success(), "{cmd:?}");
        assert!(
            stderr(&out).contains("pending"),
            "{cmd:?}: {}",
            stderr(&out)
        );
    }
    let out = Command::new(bin("leep-server"))
        .args(["--meta", &stores.meta, "--blobs", &stores.blobs])
        .args(["gc", "--dry-run"])
        .env("LEEP_NO_MIGRATE", "1")
        .output()
        .unwrap();
    assert!(!out.status.success());

    let out = server(&stores, &["migrate"]);
    assert_ok(&out, "migrate");
    assert!(
        stdout(&out).contains("migrated from v0 to v1"),
        "{}",
        stdout(&out)
    );
    let out = server(&stores, &["migrate", "--status"]);
    assert!(
        stdout(&out).contains("up to date at v1"),
        "{}",
        stdout(&out)
    );

    // Now a server that must not migrate starts, and serves the old data.
    let srv = Server::start_with(free_port(), &stores, dir.path(), &["--no-migrate"]);
    let (code, _, body) = http_get(srv.addr, "/v1/refs/main", None);
    assert_eq!(code, 200, "{body}");
    assert!(body.contains(&head.to_string()), "{body}");
}

#[test]
fn server_refuses_a_newer_schema() {
    let _g = serial();
    let dir = tempfile::tempdir().unwrap();
    let stores = Stores::sqlite(dir.path());
    assert_ok(&server(&stores, &["migrate"]), "migrate");
    // A later server recorded a migration this one has never heard of.
    let url = stores.meta.clone();
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let opts = sqlx::sqlite::SqliteConnectOptions::from_str(&url).unwrap();
        let pool = sqlx::SqlitePool::connect_with(opts).await.unwrap();
        sqlx::query(
            "INSERT INTO schema_migrations (version, name, applied_at, checksum) \
             VALUES (2, 'future', 1, 'x')",
        )
        .execute(&pool)
        .await
        .unwrap();
        pool.close().await;
    });
    for args in [
        &["gc", "--dry-run"][..],
        &["migrate"],
        &["--no-migrate", "gc"],
    ] {
        let out = server(&stores, args);
        assert!(!out.status.success(), "{args:?}");
        assert!(
            stderr(&out).contains("database schema v2 is newer than this server (max v1)"),
            "{args:?}: {}",
            stderr(&out)
        );
    }
}
