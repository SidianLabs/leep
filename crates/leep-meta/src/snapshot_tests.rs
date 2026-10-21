//! Snapshot export and import (ADR 0026) across both backends.

use std::sync::Arc;

use bytes::Bytes;
use leep_core::{Hash, LandId, LandKey, MetaStore, RefUpdateContext};
use sqlx::Connection;

use super::snapshot::*;
use super::tests::{cs, pg_url};
use super::*;

fn land_id(i: i64) -> LandId {
    LandId::from_bytes(&[i as u8 + 1; 16]).unwrap()
}

/// A store with changesets, several refs with moves and deletes, land ids
/// on the even moves of `main`, and derived rows of two kinds.
async fn populate(store: &dyn MetaStore) {
    let mut prev = None;
    for i in 0..25 {
        let id = store.put_changeset(&cs(i)).await.unwrap();
        let ctx = RefUpdateContext::new("land")
            .actor(Some("alice"))
            .message("m")
            .land((i % 2 == 0).then(|| LandKey {
                id: land_id(i),
                changeset: id,
            }));
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

type Dump = (
    Vec<String>,
    Vec<String>,
    Vec<String>,
    Vec<String>,
    Vec<String>,
);

/// A dump without its land ids, for a copy that went through a schema from
/// before them (ADR 0057).
fn without_lands(d: Dump) -> (Vec<String>, Vec<String>, Vec<String>, Vec<String>) {
    (d.0, d.1, d.2, d.3)
}

/// Everything a store holds, in comparable form.
async fn dump(store: &dyn MetaStore) -> Dump {
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
    // Land ids go with the other land data, so `without_lands` drops them.
    let mut log = Vec::new();
    let mut log_lands = Vec::new();
    for mut e in store.ref_log(None, None, 10_000).await.unwrap() {
        if let Some(id) = e.land_id.take() {
            log_lands.push(format!("{} {id} {:?}", e.seq, e.land_changeset.take()));
        }
        log.push(format!("{e:?}"));
    }
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
    let want: Vec<LandId> = (0..25).map(land_id).collect();
    let mut lands: Vec<String> = store
        .find_lands("main", &want)
        .await
        .unwrap()
        .into_iter()
        .map(|l| format!("{l:?}"))
        .collect();
    lands.extend(log_lands);
    (ids, refs, log, derived, lands)
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
    assert_eq!(info.schema_version, latest_schema_version());
    assert_eq!(info.ref_log_last_seq, last as u64);
    assert!(info.ref_log < last as u64);
    // One self-contained file.
    assert!(snap.is_file());
    assert!(!dir.path().join("snap.db-wal").exists());
    assert!(!dir.path().join("snap.db.partial").exists());
    assert_eq!(
        snapshot_schema_version(&snap).await.unwrap(),
        latest_schema_version()
    );

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
            let mut c = sqlx::SqliteConnection::connect(&url).await.unwrap();
            for stmt in sql.split(';') {
                sqlx::query(stmt).execute(&mut c).await.unwrap();
            }
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
    let older = edit(
        "older.db",
        "DROP TABLE schema_migrations; DROP TABLE changeset_dicts; DROP TABLE store_features; \
         DROP INDEX ref_log_land_id; ALTER TABLE ref_log DROP COLUMN land_changeset; \
         ALTER TABLE ref_log DROP COLUMN land_id; \
         DROP TABLE leases",
    )
    .await;
    let dest_url = sqlite_url(dir.path(), "d2.db");
    import_snapshot(&older, &dest_url, false, SchemaMode::Migrate)
        .await
        .unwrap();
    let d2 = open_meta_store(&dest_url).await.unwrap();
    assert_eq!(
        d2.schema_version().await.unwrap(),
        Some(latest_schema_version())
    );
    assert_eq!(
        without_lands(dump(&*src).await),
        without_lands(dump(&*d2).await)
    );

    // From v1, the schema before migration 2 (which changes nothing in
    // SQLite) and 3: migrated on the scratch copy, then restored.
    let v1 = edit(
        "v1.db",
        "DROP TABLE changeset_dicts; DROP TABLE store_features; DROP TABLE leases; DROP INDEX ref_log_land_id; ALTER TABLE ref_log DROP COLUMN land_changeset; \
         ALTER TABLE ref_log DROP COLUMN land_id; \
         DELETE FROM schema_migrations WHERE version > 1",
    )
    .await;
    assert_eq!(snapshot_schema_version(&v1).await.unwrap(), 1);
    let dest_url = sqlite_url(dir.path(), "d4.db");
    import_snapshot(&v1, &dest_url, false, SchemaMode::Migrate)
        .await
        .unwrap();
    let d4 = open_meta_store(&dest_url).await.unwrap();
    assert_eq!(
        d4.schema_version().await.unwrap(),
        Some(latest_schema_version())
    );
    assert_eq!(
        without_lands(dump(&*src).await),
        without_lands(dump(&*d4).await)
    );

    // From v2, before `changeset_dicts` (ADR 0053): the table is created on
    // the scratch copy, empty, and the plain rows restore as they were.
    let v2 = edit(
        "v2.db",
        "DROP TABLE changeset_dicts; DROP TABLE store_features; DROP TABLE leases; DROP INDEX ref_log_land_id; ALTER TABLE ref_log DROP COLUMN land_changeset; \
         ALTER TABLE ref_log DROP COLUMN land_id; \
         DELETE FROM schema_migrations WHERE version > 2",
    )
    .await;
    assert_eq!(snapshot_schema_version(&v2).await.unwrap(), 2);
    let dest_url = sqlite_url(dir.path(), "d5.db");
    import_snapshot(&v2, &dest_url, false, SchemaMode::Migrate)
        .await
        .unwrap();
    let d5 = open_meta_store(&dest_url).await.unwrap();
    assert_eq!(
        d5.schema_version().await.unwrap(),
        Some(latest_schema_version())
    );
    assert_eq!(
        without_lands(dump(&*src).await),
        without_lands(dump(&*d5).await)
    );

    // From v3, before `store_features` (ADR 0055): created on the scratch
    // copy; the source had no dictionary, so it names no feature.
    let v3 = edit(
        "v3.db",
        "DROP TABLE store_features; DROP TABLE leases; DROP INDEX ref_log_land_id; ALTER TABLE ref_log DROP COLUMN land_changeset; \
         ALTER TABLE ref_log DROP COLUMN land_id; \
         DELETE FROM schema_migrations WHERE version > 3",
    )
    .await;
    assert_eq!(snapshot_schema_version(&v3).await.unwrap(), 3);
    let dest_url = sqlite_url(dir.path(), "d6.db");
    import_snapshot(&v3, &dest_url, false, SchemaMode::Migrate)
        .await
        .unwrap();
    let d6 = open_meta_store(&dest_url).await.unwrap();
    assert_eq!(
        d6.schema_version().await.unwrap(),
        Some(latest_schema_version())
    );
    assert_eq!(
        without_lands(dump(&*src).await),
        without_lands(dump(&*d6).await)
    );
    assert!(stored_features(&dest_url).await.is_empty());

    // From v4, before land ids (ADR 0057): the columns are added on the
    // scratch copy, and every restored entry has none.
    let v4 = edit(
        "v4.db",
        "DROP INDEX ref_log_land_id; ALTER TABLE ref_log DROP COLUMN land_changeset; \
         ALTER TABLE ref_log DROP COLUMN land_id; DROP TABLE leases; \
         DELETE FROM schema_migrations WHERE version > 4",
    )
    .await;
    assert_eq!(snapshot_schema_version(&v4).await.unwrap(), 4);
    let dest_url = sqlite_url(dir.path(), "d7.db");
    import_snapshot(&v4, &dest_url, false, SchemaMode::Migrate)
        .await
        .unwrap();
    let d7 = open_meta_store(&dest_url).await.unwrap();
    assert_eq!(
        d7.schema_version().await.unwrap(),
        Some(latest_schema_version())
    );
    assert_eq!(
        without_lands(dump(&*src).await),
        without_lands(dump(&*d7).await)
    );
    assert!(dump(&*d7).await.4.iter().all(|l| l == "None"));
    assert!(dump(&*src).await.4.iter().any(|l| l != "None"));

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

/// Every changeset row's stored bytes, in id order.
async fn stored_rows(url: &str) -> Vec<(Vec<u8>, Vec<u8>)> {
    let q = "SELECT id, data FROM changesets ORDER BY id";
    if url.starts_with("sqlite:") {
        let mut c = sqlx::SqliteConnection::connect(url).await.unwrap();
        sqlx::query_as(q).fetch_all(&mut c).await.unwrap()
    } else {
        let mut c = sqlx::PgConnection::connect(url).await.unwrap();
        sqlx::query_as(q).fetch_all(&mut c).await.unwrap()
    }
}

/// The `store_features` rows (ADR 0055), by name.
async fn stored_features(url: &str) -> Vec<(String, i64)> {
    let q = "SELECT name, added_at FROM store_features ORDER BY name";
    if url.starts_with("sqlite:") {
        let mut c = sqlx::SqliteConnection::connect(url).await.unwrap();
        sqlx::query_as(q).fetch_all(&mut c).await.unwrap()
    } else {
        let mut c = sqlx::PgConnection::connect(url).await.unwrap();
        sqlx::query_as(q).fetch_all(&mut c).await.unwrap()
    }
}

/// Add history worth compressing to the store at `url` and compress it
/// (ADR 0053); returns the history.
async fn compacted(url: &str, store: &dyn MetaStore) -> Vec<leep_core::Changeset> {
    let history = crate::compact_tests::history(crate::compact::MIN_TRAINING_ROWS, 4);
    for chunk in history.chunks(200) {
        store.put_changesets_with_derived(chunk, &[]).await.unwrap();
    }
    let r = crate::compact::compact_changesets(
        url,
        SchemaMode::RequireCurrent,
        &Default::default(),
        &mut |_| {},
    )
    .await
    .unwrap();
    assert_eq!(r.dictionary, 1);
    history
}

/// Compressed changeset rows and their dictionary travel with a snapshot
/// byte for byte, and the restored store reads them and writes new ones.
#[tokio::test]
async fn compressed_changesets_and_their_dictionary_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let src_url = sqlite_url(dir.path(), "src.db");
    let src = open_meta_store(&src_url).await.unwrap();
    populate(&*src).await;
    let history = compacted(&src_url, &*src).await;
    let snap = dir.path().join("snap.db");
    let info = export_snapshot(&src_url, &snap).await.unwrap();
    assert_eq!(info.changeset_dicts, 1);
    assert_eq!(info.changesets, 26 + history.len() as u64);
    let rows = stored_rows(&src_url).await;
    assert_eq!(stored_rows(&sqlite_file_url(&snap, false)).await, rows);
    let features = stored_features(&src_url).await;
    assert_eq!(features.len(), 1);
    assert_eq!(
        stored_features(&sqlite_file_url(&snap, false)).await,
        features
    );

    let dest_url = sqlite_url(dir.path(), "restored.db");
    import_snapshot(&snap, &dest_url, false, SchemaMode::Migrate)
        .await
        .unwrap();
    assert_eq!(stored_rows(&dest_url).await, rows);
    assert_eq!(stored_features(&dest_url).await, features);
    let dest = open_meta_store(&dest_url).await.unwrap();
    assert_eq!(dump(&*src).await, dump(&*dest).await);
    for c in &history {
        assert_eq!(dest.get_changeset(&c.id()).await.unwrap().as_ref(), Some(c));
    }
    let next = crate::compact_tests::history(3, 11);
    dest.put_changesets_with_derived(&next, &[]).await.unwrap();
    let after = stored_rows(&dest_url).await;
    for c in &next {
        let row = &after.iter().find(|r| r.0 == c.id().0 .0).unwrap().1;
        assert!(crate::codec::Codec::uses(row, 1));
    }
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
    let history = compacted(&a.url, &*src).await;
    let rows = stored_rows(&a.url).await;
    let last = src.ref_log(None, None, 1).await.unwrap()[0].seq;
    let dir = tempfile::tempdir().unwrap();

    // Postgres to a snapshot file, and from it to a different Postgres.
    let snap = dir.path().join("pg.db");
    let info = export_snapshot(&a.url, &snap).await.unwrap();
    assert_eq!(info.changesets, 26 + history.len() as u64);
    assert_eq!(info.changeset_dicts, 1);
    assert_eq!(info.ref_log_last_seq, last);
    import_snapshot(&snap, &b.url, false, SchemaMode::Migrate)
        .await
        .unwrap();
    let dest = open_meta_store(&b.url).await.unwrap();
    assert_eq!(dump(&*src).await, dump(&*dest).await);
    // The restore analyzed what it loaded: a table never analyzed or
    // vacuumed has `reltuples = -1` (ADR 0042).
    let mut c = sqlx::PgConnection::connect(&b.url).await.unwrap();
    let tuples: f32 =
        sqlx::query_scalar("SELECT reltuples FROM pg_class WHERE relname = 'changesets'")
            .fetch_one(&mut c)
            .await
            .unwrap();
    assert_eq!(tuples, 26.0 + history.len() as f32);
    assert_eq!(stored_rows(&b.url).await, rows);
    let features = stored_features(&a.url).await;
    assert_eq!(features.len(), 1);
    assert_eq!(stored_features(&b.url).await, features);
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
    assert_eq!(stored_rows(&sq).await, rows);
    assert_eq!(stored_features(&sq).await, features);
    let snap2 = dir.path().join("sq.db");
    export_snapshot(&sq, &snap2).await.unwrap();
    import_snapshot(&snap2, &b.url, true, SchemaMode::Migrate)
        .await
        .unwrap();
    assert_eq!(dump(&*src).await, dump(&*dest).await);
    assert_eq!(stored_features(&b.url).await, features);

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
