//! Snapshot export and import (ADR 0026) across both backends.

use std::sync::Arc;

use bytes::Bytes;
use leep_core::{Hash, MetaStore, RefUpdateContext};
use sqlx::Connection;

use super::snapshot::*;
use super::tests::{cs, pg_url};
use super::*;

/// A store with changesets, several refs with moves and deletes, and derived
/// rows of two kinds.
async fn populate(store: &dyn MetaStore) {
    let ctx = RefUpdateContext::new("land")
        .actor(Some("alice"))
        .message("m");
    let mut prev = None;
    for i in 0..25 {
        let id = store.put_changeset(&cs(i)).await.unwrap();
        assert!(store
            .cas_ref_logged("main", prev, Some(id), &ctx)
            .await
            .unwrap());
        prev = Some(id);
        store
            .put_derived("gen", &id.0, Bytes::from(vec![i as u8; 3]))
            .await
            .unwrap();
    }
    let side = store.put_changeset(&cs(100)).await.unwrap();
    assert!(store.cas_ref("tags/v1", None, Some(side)).await.unwrap());
    assert!(store.cas_ref("gone", None, Some(side)).await.unwrap());
    assert!(store.cas_ref("gone", Some(side), None).await.unwrap());
    store
        .put_derived(
            "blob-claim-v1",
            &Hash::of_blob(b"x"),
            Bytes::from_static(b"1234"),
        )
        .await
        .unwrap();
}

/// Everything a store holds, in comparable form.
async fn dump(store: &dyn MetaStore) -> (Vec<String>, Vec<String>, Vec<String>, Vec<String>) {
    let mut ids = Vec::new();
    let mut after = None;
    loop {
        let page = store.list_changesets(after, 7).await.unwrap();
        let Some(last) = page.last().copied() else {
            break;
        };
        after = Some(last);
        for id in page {
            let c = store.get_changeset(&id).await.unwrap().unwrap();
            ids.push(format!("{id:?} {}", c.message));
        }
    }
    let refs = store
        .list_refs()
        .await
        .unwrap()
        .into_iter()
        .map(|(n, t)| format!("{n} {t:?}"))
        .collect();
    let log = store
        .ref_log(None, None, 10_000)
        .await
        .unwrap()
        .into_iter()
        .map(|e| format!("{e:?}"))
        .collect();
    let mut derived = Vec::new();
    for kind in ["gen", "blob-claim-v1"] {
        let mut after = None;
        loop {
            let page = store.list_derived(kind, after, 7).await.unwrap();
            let Some(last) = page.last().map(|p| p.0) else {
                break;
            };
            after = Some(last);
            for (k, v) in page {
                derived.push(format!("{kind} {k:?} {v:?}"));
            }
        }
    }
    (ids, refs, log, derived)
}

fn sqlite_url(dir: &std::path::Path, name: &str) -> String {
    sqlite_file_url(&dir.join(name), true)
}

#[tokio::test]
async fn sqlite_round_trip_keeps_everything_and_the_ref_log_sequence() {
    let dir = tempfile::tempdir().unwrap();
    let src = open_meta_store(&sqlite_url(dir.path(), "src.db"))
        .await
        .unwrap();
    populate(&*src).await;
    // The newest log entry is gone (as if pruned): `seq` must still not be reused.
    let last = src.ref_log(None, None, 1).await.unwrap()[0].seq;
    sqlx::query(&format!("DELETE FROM ref_log WHERE seq = {last}"))
        .execute(
            &mut sqlx::SqliteConnection::connect(&sqlite_url(dir.path(), "src.db"))
                .await
                .unwrap(),
        )
        .await
        .unwrap();

    let snap = dir.path().join("snap.db");
    let info = export_snapshot(&sqlite_url(dir.path(), "src.db"), &snap)
        .await
        .unwrap();
    assert_eq!(info.changesets, 26);
    assert_eq!(info.refs.len(), 2);
    assert_eq!(info.derived, 26);
    assert_eq!(info.schema_version, 1);
    assert_eq!(info.ref_log_last_seq, last as u64);
    assert!(info.ref_log < last as u64);
    // One self-contained file.
    assert!(snap.is_file());
    assert!(!dir.path().join("snap.db-wal").exists());
    assert!(!dir.path().join("snap.db.partial").exists());
    assert_eq!(snapshot_schema_version(&snap).await.unwrap(), 1);

    let dest_url = sqlite_url(dir.path(), "restored.db");
    let imported = import_snapshot(&snap, &dest_url, false, SchemaMode::Migrate)
        .await
        .unwrap();
    assert_eq!(imported.changesets, 26);
    assert_eq!(imported.ref_log, info.ref_log);
    let dest = open_meta_store(&dest_url).await.unwrap();
    assert_eq!(dump(&*src).await, dump(&*dest).await);
    // The next ref move gets the next sequence number, never a reused one.
    let id = dest.put_changeset(&cs(900)).await.unwrap();
    assert!(dest.cas_ref("fresh", None, Some(id)).await.unwrap());
    assert_eq!(
        dest.ref_log(None, None, 1).await.unwrap()[0].seq,
        last as u64 + 1
    );
}

#[tokio::test]
async fn import_refuses_a_non_empty_destination_unless_forced() {
    let dir = tempfile::tempdir().unwrap();
    let src = open_meta_store(&sqlite_url(dir.path(), "src.db"))
        .await
        .unwrap();
    populate(&*src).await;
    let snap = dir.path().join("snap.db");
    export_snapshot(&sqlite_url(dir.path(), "src.db"), &snap)
        .await
        .unwrap();
    // Exporting over an existing file is refused too.
    assert!(export_snapshot(&sqlite_url(dir.path(), "src.db"), &snap)
        .await
        .is_err());

    let dest_url = sqlite_url(dir.path(), "dest.db");
    let dest = open_meta_store(&dest_url).await.unwrap();
    let stray = dest.put_changeset(&cs(7777)).await.unwrap();
    let err = import_snapshot(&snap, &dest_url, false, SchemaMode::Migrate)
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("not empty"), "{err}");
    // Refused, and nothing changed.
    assert!(dest.get_changeset(&stray).await.unwrap().is_some());
    assert!(dest.list_refs().await.unwrap().is_empty());

    import_snapshot(&snap, &dest_url, true, SchemaMode::Migrate)
        .await
        .unwrap();
    assert!(dest.get_changeset(&stray).await.unwrap().is_none());
    assert_eq!(dump(&*src).await, dump(&*dest).await);
}

#[tokio::test]
async fn export_never_writes_to_or_creates_the_source() {
    let dir = tempfile::tempdir().unwrap();
    let missing = dir.path().join("nope.db");
    let url = format!("sqlite://{}?mode=rw", missing.display());
    assert!(export_snapshot(&url, &dir.path().join("a.db"))
        .await
        .is_err());
    assert!(!missing.exists());
    assert!(!dir.path().join("a.db").exists());
    assert!(!dir.path().join("a.db.partial").exists());
    assert!(export_snapshot("mem://", &dir.path().join("b.db"))
        .await
        .is_err());
}

/// Writers keep landing while the snapshot is read: the snapshot is one
/// point in time, so each ref's newest log entry is its tip, and every tip's
/// changeset is in it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn snapshot_during_writes_is_one_point_in_time() {
    let dir = tempfile::tempdir().unwrap();
    let src_url = sqlite_url(dir.path(), "src.db");
    let src: Arc<dyn MetaStore> = open_meta_store(&src_url).await.unwrap();
    populate(&*src).await;
    for i in 0..400 {
        src.put_changeset(&cs(10_000 + i)).await.unwrap();
    }
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let writer = {
        let (src, stop) = (src.clone(), stop.clone());
        tokio::spawn(async move {
            let mut n = 50_000;
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                n += 1;
                let id = src.put_changeset(&cs(n)).await.unwrap();
                let name = format!("w/{}", n % 3);
                let cur = src.get_ref(&name).await.unwrap();
                let _ = src.cas_ref(&name, cur, Some(id)).await.unwrap();
            }
        })
    };
    let mut infos = Vec::new();
    for i in 0..3 {
        let snap = dir.path().join(format!("snap{i}.db"));
        infos.push((
            snap.clone(),
            export_snapshot(&src_url, &snap).await.unwrap(),
        ));
    }
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    writer.await.unwrap();
    for (snap, info) in infos {
        let opened = open_snapshot(&snap).await.unwrap();
        let m = open_meta_store(&opened.url).await.unwrap();
        for (name, tip) in &info.refs {
            assert_eq!(m.get_ref(name).await.unwrap(), Some(*tip));
            assert!(m.get_changeset(tip).await.unwrap().is_some(), "{name}");
            let newest = m.ref_log(Some(name), None, 1).await.unwrap();
            assert_eq!(newest[0].new, Some(*tip), "{name}");
        }
    }
}

#[tokio::test]
async fn schema_version_rules_follow_adr_0023() {
    let dir = tempfile::tempdir().unwrap();
    let src = open_meta_store(&sqlite_url(dir.path(), "src.db"))
        .await
        .unwrap();
    populate(&*src).await;
    let snap = dir.path().join("snap.db");
    export_snapshot(&sqlite_url(dir.path(), "src.db"), &snap)
        .await
        .unwrap();

    let edit = |name: &str, sql: &str| {
        let path = dir.path().join(name);
        std::fs::copy(&snap, &path).unwrap();
        let sql = sql.to_string();
        let url = sqlite_file_url(&path, false);
        async move {
            sqlx::query(&sql)
                .execute(&mut sqlx::SqliteConnection::connect(&url).await.unwrap())
                .await
                .unwrap();
            path
        }
    };
    // From a newer release: refused, and the destination is untouched.
    let newer = edit(
        "newer.db",
        "INSERT INTO schema_migrations VALUES (99, 'future', 1, 'abc')",
    )
    .await;
    let dest_url = sqlite_url(dir.path(), "d1.db");
    let err = import_snapshot(&newer, &dest_url, false, SchemaMode::Migrate)
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("newer"), "{err}");
    let d1 = open_meta_store(&dest_url).await.unwrap();
    assert!(d1.list_refs().await.unwrap().is_empty());
    assert_eq!(snapshot_schema_version(&newer).await.unwrap(), 99);

    // From an un-versioned database (before ADR 0023): adopted, then restored.
    let older = edit("older.db", "DROP TABLE schema_migrations").await;
    let dest_url = sqlite_url(dir.path(), "d2.db");
    import_snapshot(&older, &dest_url, false, SchemaMode::Migrate)
        .await
        .unwrap();
    let d2 = open_meta_store(&dest_url).await.unwrap();
    assert_eq!(d2.schema_version().await.unwrap(), Some(1));
    assert_eq!(dump(&*src).await, dump(&*d2).await);

    // Not a snapshot at all.
    let junk = dir.path().join("junk.db");
    std::fs::write(&junk, b"not a database").unwrap();
    assert!(import_snapshot(
        &junk,
        &sqlite_url(dir.path(), "d3.db"),
        false,
        SchemaMode::Migrate
    )
    .await
    .is_err());
}

/// A throwaway Postgres database.
struct PgDb {
    admin: String,
    name: String,
    url: String,
}

impl PgDb {
    async fn create(tag: &str) -> Option<PgDb> {
        let admin = pg_url()?;
        let name = format!("leep_snap_{tag}_{}", std::process::id());
        let mut c = sqlx::PgConnection::connect(&admin).await.unwrap();
        sqlx::query(&format!("DROP DATABASE IF EXISTS {name} WITH (FORCE)"))
            .execute(&mut c)
            .await
            .unwrap();
        sqlx::query(&format!("CREATE DATABASE {name}"))
            .execute(&mut c)
            .await
            .unwrap();
        let root = admin.split('?').next().unwrap().rsplit_once('/').unwrap().0;
        let url = format!("{root}/{name}");
        Some(PgDb { admin, name, url })
    }

    async fn drop_db(self) {
        let mut c = sqlx::PgConnection::connect(&self.admin).await.unwrap();
        sqlx::query(&format!(
            "DROP DATABASE IF EXISTS {} WITH (FORCE)",
            self.name
        ))
        .execute(&mut c)
        .await
        .unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_to_sqlite_to_postgres() {
    let Some(a) = PgDb::create("a").await else {
        return;
    };
    let b = PgDb::create("b").await.unwrap();
    let src = open_meta_store(&a.url).await.unwrap();
    populate(&*src).await;
    let last = src.ref_log(None, None, 1).await.unwrap()[0].seq;
    let dir = tempfile::tempdir().unwrap();

    // Postgres to a snapshot file, and from it to a different Postgres.
    let snap = dir.path().join("pg.db");
    let info = export_snapshot(&a.url, &snap).await.unwrap();
    assert_eq!(info.changesets, 26);
    assert_eq!(info.ref_log_last_seq, last);
    import_snapshot(&snap, &b.url, false, SchemaMode::Migrate)
        .await
        .unwrap();
    let dest = open_meta_store(&b.url).await.unwrap();
    assert_eq!(dump(&*src).await, dump(&*dest).await);
    let id = dest.put_changeset(&cs(900)).await.unwrap();
    assert!(dest.cas_ref("fresh", None, Some(id)).await.unwrap());
    assert_eq!(dest.ref_log(None, None, 1).await.unwrap()[0].seq, last + 1);
    // A second import into the now non-empty database is refused; forced, it replaces.
    assert!(import_snapshot(&snap, &b.url, false, SchemaMode::Migrate)
        .await
        .is_err());
    import_snapshot(&snap, &b.url, true, SchemaMode::Migrate)
        .await
        .unwrap();
    assert_eq!(dump(&*src).await, dump(&*dest).await);

    // Postgres snapshot into SQLite, and a SQLite snapshot into Postgres.
    let sq = sqlite_url(dir.path(), "from-pg.db");
    import_snapshot(&snap, &sq, false, SchemaMode::Migrate)
        .await
        .unwrap();
    let sqm = open_meta_store(&sq).await.unwrap();
    assert_eq!(dump(&*src).await, dump(&*sqm).await);
    let snap2 = dir.path().join("sq.db");
    export_snapshot(&sq, &snap2).await.unwrap();
    import_snapshot(&snap2, &b.url, true, SchemaMode::Migrate)
        .await
        .unwrap();
    assert_eq!(dump(&*src).await, dump(&*dest).await);

    drop((src, dest));
    a.drop_db().await;
    b.drop_db().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_snapshot_during_writes_is_one_point_in_time() {
    let Some(a) = PgDb::create("w").await else {
        return;
    };
    let src: Arc<dyn MetaStore> = open_meta_store(&a.url).await.unwrap();
    populate(&*src).await;
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let writer = {
        let (src, stop) = (src.clone(), stop.clone());
        tokio::spawn(async move {
            let mut n = 50_000;
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                n += 1;
                let id = src.put_changeset(&cs(n)).await.unwrap();
                let name = format!("w/{}", n % 3);
                let cur = src.get_ref(&name).await.unwrap();
                let _ = src.cas_ref(&name, cur, Some(id)).await.unwrap();
            }
        })
    };
    let dir = tempfile::tempdir().unwrap();
    let mut infos = Vec::new();
    for i in 0..3 {
        let snap = dir.path().join(format!("snap{i}.db"));
        infos.push((snap.clone(), export_snapshot(&a.url, &snap).await.unwrap()));
    }
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    writer.await.unwrap();
    for (snap, info) in infos {
        let opened = open_snapshot(&snap).await.unwrap();
        let m = open_meta_store(&opened.url).await.unwrap();
        for (name, tip) in &info.refs {
            assert_eq!(m.get_ref(name).await.unwrap(), Some(*tip));
            assert!(m.get_changeset(tip).await.unwrap().is_some());
            assert_eq!(
                m.ref_log(Some(name), None, 1).await.unwrap()[0].new,
                Some(*tip)
            );
        }
    }
    drop(src);
    a.drop_db().await;
}
