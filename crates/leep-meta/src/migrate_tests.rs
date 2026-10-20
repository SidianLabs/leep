//! Tests of the schema migrator (ADR 0023) against both backends.
//!
//! # Adding a migration
//!
//! Every new migration `N` adds, in this file:
//!
//! 1. A fixture for the schema it upgrades from, so the upgrade is tested
//!    against data written by the previous version. Add a [`Stage`]: its DDL
//!    for each backend (copy it from `leep-meta/migrations/` or from
//!    `sqlite3 .schema` of a real database) and extend `Data`/`load` with rows
//!    for any table or column the previous version had, using the SQL that
//!    version used. While no release is tagged, "the previous version" is the
//!    schema of the commit before yours.
//! 2. A check in `verify` that the data still reads back, and that the new
//!    schema works, after `upgrade_from_every_stage` runs.
//!
//! Once releases are tagged, there is one `Stage` per tagged version's schema
//! (the test is named "from every earlier version" for that reason).
//! Stages are never edited or removed, like the migrations themselves.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;

use leep_core::memory::check_meta_store_contract;
use leep_core::{
    Changeset, ChangesetId, FileChange, FileKind, Hash, MetaStore, RefUpdateContext, RepoPath,
};
use sqlx::Row;

use crate::tests::{pg_url, schema};
use crate::*;

// ---- Backends under test ---------------------------------------------------

enum Db {
    Sqlite { url: String, dir: PathBuf },
    Pg { url: String, schema: String },
}

enum Arg {
    Bytes(Vec<u8>),
    NullBytes,
    Text(String),
    NullText,
    Int(i64),
}

impl Db {
    fn sqlite(name: &str) -> Db {
        let dir = std::env::temp_dir().join(format!("leep-mig-{name}-{}", std::process::id()));
        std::fs::remove_dir_all(&dir).ok();
        std::fs::create_dir_all(&dir).unwrap();
        let url = format!("sqlite://{}?mode=rwc", dir.join("meta.db").display());
        Db::Sqlite { url, dir }
    }

    async fn pg(name: &str) -> Option<Db> {
        let url = pg_url()?;
        let db = Db::Pg {
            url,
            schema: schema(&format!("mig{name}")),
        };
        // Created here: `CREATE SCHEMA IF NOT EXISTS` itself races when
        // several connections run it at once.
        db.exec_raw(&[
            &format!("DROP SCHEMA IF EXISTS {} CASCADE", db.quoted()),
            &format!("CREATE SCHEMA {}", db.quoted()),
        ])
        .await;
        Some(db)
    }

    fn quoted(&self) -> String {
        match self {
            Db::Pg { schema, .. } => format!("\"{schema}\""),
            Db::Sqlite { .. } => unreachable!(),
        }
    }

    fn kind(&self) -> &'static str {
        match self {
            Db::Sqlite { .. } => "sqlite",
            Db::Pg { .. } => "postgres",
        }
    }

    /// The compiled list for this backend.
    fn compiled(&self) -> &'static [Migration] {
        match self {
            Db::Sqlite { .. } => SQLITE_MIGRATIONS,
            Db::Pg { .. } => POSTGRES_MIGRATIONS,
        }
    }

    async fn open(&self, migs: &[Migration], mode: SchemaMode) -> Result<Arc<dyn MetaStore>> {
        Ok(match self {
            Db::Sqlite { url, .. } => {
                Arc::new(SqliteMetaStore::connect_with_migrations(url, mode, migs).await?)
            }
            Db::Pg { url, schema } => Arc::new(
                PostgresMetaStore::connect_with_migrations(url, Some(schema), mode, migs).await?,
            ),
        })
    }

    async fn migrate(&self, migs: &[Migration], dry_run: bool) -> Result<MigrationReport> {
        match self {
            Db::Sqlite { url, .. } => SqliteMetaStore::migrate_with(url, migs, dry_run).await,
            Db::Pg { url, schema } => {
                PostgresMetaStore::migrate_with(url, Some(schema), migs, dry_run).await
            }
        }
    }

    async fn report(&self, migs: &[Migration]) -> MigrationReport {
        self.migrate(migs, true).await.unwrap()
    }

    /// Run statements the way the old code did, on one connection, with
    /// `?` placeholders (rewritten to `$n` for Postgres).
    async fn exec(&self, sql: &str, args: Vec<Arg>) {
        match self {
            Db::Sqlite { url, .. } => {
                let opts = sqlx::sqlite::SqliteConnectOptions::from_str(url).unwrap();
                let pool = sqlx::SqlitePool::connect_with(opts).await.unwrap();
                let mut q = sqlx::query(sql);
                for a in args {
                    q = match a {
                        Arg::Bytes(b) => q.bind(b),
                        Arg::NullBytes => q.bind(None::<Vec<u8>>),
                        Arg::Text(t) => q.bind(t),
                        Arg::NullText => q.bind(None::<String>),
                        Arg::Int(i) => q.bind(i),
                    };
                }
                q.execute(&pool).await.unwrap();
                pool.close().await;
            }
            Db::Pg { url, schema } => {
                let mut n = 0;
                let mut numbered = String::new();
                for c in sql.chars() {
                    if c == '?' {
                        n += 1;
                        numbered.push_str(&format!("${n}"));
                    } else {
                        numbered.push(c);
                    }
                }
                let opts = sqlx::postgres::PgConnectOptions::from_str(url).unwrap();
                let schema = schema.clone();
                let pool = sqlx::postgres::PgPoolOptions::new()
                    .max_connections(1)
                    .after_connect(move |conn, _| {
                        let set = format!("SET search_path TO \"{schema}\"");
                        Box::pin(async move {
                            sqlx::query(&set).execute(&mut *conn).await?;
                            Ok(())
                        })
                    })
                    .connect_with(opts)
                    .await
                    .unwrap();
                let mut q = sqlx::query(&numbered);
                for a in args {
                    q = match a {
                        Arg::Bytes(b) => q.bind(b),
                        Arg::NullBytes => q.bind(None::<Vec<u8>>),
                        Arg::Text(t) => q.bind(t),
                        Arg::NullText => q.bind(None::<String>),
                        Arg::Int(i) => q.bind(i),
                    };
                }
                q.execute(&pool).await.unwrap();
                pool.close().await;
            }
        }
    }

    /// Statements without parameters.
    async fn exec_raw(&self, sql: &[&str]) {
        for s in sql {
            match self {
                Db::Pg { url, .. } if s.contains("SCHEMA") => {
                    let opts = sqlx::postgres::PgConnectOptions::from_str(url).unwrap();
                    let pool = sqlx::PgPool::connect_with(opts).await.unwrap();
                    sqlx::query(s).execute(&pool).await.unwrap();
                    pool.close().await;
                }
                Db::Pg { .. } => self.exec(s, vec![]).await,
                Db::Sqlite { .. } => self.exec(s, vec![]).await,
            }
        }
    }

    /// Tables and named indexes, excluding `schema_migrations`, primary key
    /// indexes and engine internals.
    async fn objects(&self) -> BTreeSet<String> {
        let names: Vec<String> = match self {
            Db::Sqlite { url, .. } => {
                let opts = sqlx::sqlite::SqliteConnectOptions::from_str(url).unwrap();
                let pool = sqlx::SqlitePool::connect_with(opts).await.unwrap();
                let rows = sqlx::query(
                    "SELECT name FROM sqlite_master WHERE type IN ('table','index') \
                     AND name NOT LIKE 'sqlite_%'",
                )
                .fetch_all(&pool)
                .await
                .unwrap();
                pool.close().await;
                rows.into_iter().map(|r| r.get(0)).collect()
            }
            Db::Pg { url, schema } => {
                let opts = sqlx::postgres::PgConnectOptions::from_str(url).unwrap();
                let pool = sqlx::PgPool::connect_with(opts).await.unwrap();
                let rows = sqlx::query(
                    "SELECT tablename::text FROM pg_tables WHERE schemaname = $1 \
                     UNION ALL SELECT indexname::text FROM pg_indexes WHERE schemaname = $1",
                )
                .bind(schema)
                .fetch_all(&pool)
                .await
                .unwrap();
                pool.close().await;
                rows.into_iter().map(|r| r.get(0)).collect()
            }
        };
        names
            .into_iter()
            .filter(|n| n != "schema_migrations" && !n.ends_with("_pkey"))
            .collect()
    }

    async fn has_schema_migrations(&self) -> bool {
        // A dry run of an empty list never reads the table; ask the catalog.
        let report = self.report(self.compiled()).await;
        !report.applied.is_empty()
    }

    async fn cleanup(self) {
        match &self {
            Db::Sqlite { dir, .. } => {
                std::fs::remove_dir_all(dir).ok();
            }
            Db::Pg { .. } => {
                let q = self.quoted();
                self.exec_raw(&[&format!("DROP SCHEMA IF EXISTS {q} CASCADE")])
                    .await;
            }
        }
    }
}

async fn dbs(name: &str) -> Vec<Db> {
    let mut v = vec![Db::sqlite(name)];
    v.extend(Db::pg(name).await);
    v
}

// ---- Migration lists with extra steps ---------------------------------------

/// Newest compiled version (the lists agree, see
/// `compiled_lists_are_consistent`); the injected steps below follow it.
const LATEST: i64 = POSTGRES_MIGRATIONS.len() as i64;

/// Steps that need no backend-specific SQL.
const ONCE: Migration = Migration {
    version: LATEST + 1,
    name: "once",
    sql: "CREATE TABLE once (n INTEGER); INSERT INTO once (n) VALUES (1);",
    satisfied_by: &[],
};
const WIDGETS: Migration = Migration {
    version: LATEST + 2,
    name: "widgets",
    sql: "CREATE TABLE widgets (id INTEGER PRIMARY KEY, label TEXT NOT NULL); \
          INSERT INTO widgets (id, label) VALUES (1, 'one'); \
          ALTER TABLE widgets ADD COLUMN size INTEGER; \
          UPDATE widgets SET size = 7;",
    satisfied_by: &[],
};
/// Creates a table, then fails: both must roll back.
const HALF: Migration = Migration {
    version: LATEST + 2,
    name: "half",
    sql: "CREATE TABLE half (x INTEGER); INSERT INTO no_such_table VALUES (1);",
    satisfied_by: &[],
};

fn with(base: &[Migration], extra: &[Migration]) -> Vec<Migration> {
    base.iter().chain(extra).copied().collect()
}

// ---- Historic schemas and their data ---------------------------------------

/// A database as an older version of the code left it.
struct Stage {
    name: &'static str,
    sqlite: &'static [&'static str],
    postgres: &'static [&'static str],
    ref_log: bool,
}

/// DDL copied from the code of the commit named in each comment.
const STAGES: &[Stage] = &[
    // Before ADR 0018: three tables, no ref log.
    Stage {
        name: "pre-ref-log",
        sqlite: &[
            "CREATE TABLE changesets (id BLOB PRIMARY KEY, data BLOB NOT NULL)",
            "CREATE TABLE refs (name TEXT PRIMARY KEY, target BLOB NOT NULL)",
            "CREATE TABLE derived (kind TEXT NOT NULL, key BLOB NOT NULL, \
             value BLOB NOT NULL, PRIMARY KEY (kind, key))",
        ],
        postgres: &[
            "CREATE TABLE changesets (id BYTEA PRIMARY KEY, data BYTEA NOT NULL)",
            "CREATE TABLE refs (name TEXT PRIMARY KEY, target BYTEA NOT NULL)",
            "CREATE TABLE derived (kind TEXT NOT NULL, key BYTEA NOT NULL, \
             value BYTEA NOT NULL, PRIMARY KEY (kind, key))",
        ],
        ref_log: false,
    },
    // dba4a34: the `CREATE TABLE IF NOT EXISTS` list that ran on every open,
    // before `schema_migrations` existed.
    Stage {
        name: "unversioned",
        sqlite: &[
            "CREATE TABLE IF NOT EXISTS changesets (id BLOB PRIMARY KEY, data BLOB NOT NULL)",
            "CREATE TABLE IF NOT EXISTS refs (name TEXT PRIMARY KEY, target BLOB NOT NULL)",
            "CREATE TABLE IF NOT EXISTS derived (kind TEXT NOT NULL, key BLOB NOT NULL, \
             value BLOB NOT NULL, PRIMARY KEY (kind, key))",
            "CREATE TABLE IF NOT EXISTS ref_log (seq INTEGER PRIMARY KEY AUTOINCREMENT, \
             name TEXT NOT NULL, old_target BLOB, new_target BLOB, time INTEGER NOT NULL, \
             actor TEXT, operation TEXT NOT NULL, message TEXT)",
            "CREATE INDEX IF NOT EXISTS ref_log_name ON ref_log (name, seq)",
        ],
        postgres: &[
            "CREATE TABLE IF NOT EXISTS changesets (id BYTEA PRIMARY KEY, data BYTEA NOT NULL)",
            "CREATE TABLE IF NOT EXISTS refs (name TEXT PRIMARY KEY, target BYTEA NOT NULL)",
            "CREATE TABLE IF NOT EXISTS derived (kind TEXT NOT NULL, key BYTEA NOT NULL, \
             value BYTEA NOT NULL, PRIMARY KEY (kind, key))",
            "CREATE TABLE IF NOT EXISTS ref_log (seq BIGSERIAL PRIMARY KEY, name TEXT NOT NULL, \
             old_target BYTEA, new_target BYTEA, time BIGINT NOT NULL, actor TEXT, \
             operation TEXT NOT NULL, message TEXT)",
            "CREATE INDEX IF NOT EXISTS ref_log_name ON ref_log (name, seq)",
        ],
        ref_log: true,
    },
];

struct LogRow {
    name: &'static str,
    old: Option<usize>,
    new: Option<usize>,
    time: i64,
    actor: Option<&'static str>,
    operation: &'static str,
    message: Option<&'static str>,
}

/// What every stage is populated with.
struct Data {
    changesets: Vec<Changeset>,
    refs: Vec<(&'static str, usize)>,
    derived: Vec<(&'static str, Hash, Vec<u8>)>,
    log: Vec<LogRow>,
}

fn data() -> Data {
    let mut changesets: Vec<Changeset> = Vec::new();
    for n in 0..5i64 {
        let mut changes = BTreeMap::new();
        changes.insert(
            RepoPath::new(format!("src/f{n}.rs")).unwrap(),
            FileChange::Modify {
                content: Hash::of_blob(format!("content {n}").as_bytes()),
                kind: FileKind::Regular,
                size: 9,
            },
        );
        let mut extra = BTreeMap::new();
        extra.insert("agent.model".to_string(), format!("m{n}"));
        changesets.push(Changeset {
            parents: changesets.last().map(|c| vec![c.id()]).unwrap_or_default(),
            author: format!("author{n}"),
            timestamp: 1_700_000_000 + n,
            message: format!("change {n}"),
            changes,
            extra,
        });
    }
    let row = |name, old, new, time, actor, operation, message| LogRow {
        name,
        old,
        new,
        time,
        actor,
        operation,
        message,
    };
    Data {
        changesets,
        refs: vec![("trunk", 2), ("feature/x", 1)],
        derived: (0..3u8)
            .map(|i| {
                (
                    "root_tree",
                    Hash::of_blob(&[i]),
                    vec![i, i + 1, i + 2, 0, 255],
                )
            })
            .collect(),
        log: vec![
            row(
                "trunk",
                None,
                Some(0),
                1_700_000_100,
                Some("ci"),
                "land",
                None,
            ),
            row("trunk", Some(0), Some(1), 1_700_000_200, None, "cas", None),
            row(
                "feature/x",
                None,
                Some(1),
                1_700_000_250,
                Some("agent-7"),
                "import",
                Some("from git"),
            ),
            row(
                "trunk",
                Some(1),
                Some(2),
                1_700_000_300,
                Some("ci"),
                "land",
                Some("with a message"),
            ),
        ],
    }
}

fn id_bytes(d: &Data, i: usize) -> Vec<u8> {
    d.changesets[i].id().0.as_bytes().to_vec()
}

/// Write the data with the SQL the old code used.
async fn load(db: &Db, stage: &Stage, d: &Data) {
    let ddl = match db {
        Db::Sqlite { .. } => stage.sqlite,
        Db::Pg { .. } => stage.postgres,
    };
    db.exec_raw(ddl).await;
    for (i, cs) in d.changesets.iter().enumerate() {
        db.exec(
            "INSERT INTO changesets (id, data) VALUES (?, ?)",
            vec![Arg::Bytes(id_bytes(d, i)), Arg::Bytes(cs.encode().to_vec())],
        )
        .await;
    }
    for (name, i) in &d.refs {
        db.exec(
            "INSERT INTO refs (name, target) VALUES (?, ?)",
            vec![Arg::Text(name.to_string()), Arg::Bytes(id_bytes(d, *i))],
        )
        .await;
    }
    for (kind, key, value) in &d.derived {
        db.exec(
            "INSERT INTO derived (kind, key, value) VALUES (?, ?, ?)",
            vec![
                Arg::Text(kind.to_string()),
                Arg::Bytes(key.as_bytes().to_vec()),
                Arg::Bytes(value.clone()),
            ],
        )
        .await;
    }
    if stage.ref_log {
        for r in &d.log {
            let id = |o: Option<usize>| o.map_or(Arg::NullBytes, |i| Arg::Bytes(id_bytes(d, i)));
            let text = |o: Option<&str>| o.map_or(Arg::NullText, |t| Arg::Text(t.to_string()));
            db.exec(
                "INSERT INTO ref_log (name, old_target, new_target, time, actor, operation, \
                 message) VALUES (?, ?, ?, ?, ?, ?, ?)",
                vec![
                    Arg::Text(r.name.to_string()),
                    id(r.old),
                    id(r.new),
                    Arg::Int(r.time),
                    text(r.actor),
                    Arg::Text(r.operation.to_string()),
                    text(r.message),
                ],
            )
            .await;
        }
    }
}

/// Everything written before the upgrade reads back through `MetaStore`, the
/// store keeps working, and the ref log carries on from where it was.
async fn verify(store: &dyn MetaStore, stage: &Stage, d: &Data, version: u64) {
    let ctx = format!("stage {}", stage.name);
    for cs in &d.changesets {
        assert_eq!(
            store.get_changeset(&cs.id()).await.unwrap().as_ref(),
            Some(cs),
            "{ctx}"
        );
    }
    let listed = store.list_changesets(None, 100).await.unwrap();
    assert_eq!(listed.len(), d.changesets.len(), "{ctx}");
    let refs = store.list_refs().await.unwrap();
    let want: BTreeMap<String, ChangesetId> = d
        .refs
        .iter()
        .map(|(n, i)| (n.to_string(), d.changesets[*i].id()))
        .collect();
    assert_eq!(refs, want, "{ctx}");
    for (kind, key, value) in &d.derived {
        assert_eq!(
            store
                .get_derived(kind, key)
                .await
                .unwrap()
                .unwrap()
                .as_ref(),
            value.as_slice(),
            "{ctx}"
        );
    }
    let log = store.ref_log(None, None, 100).await.unwrap();
    if stage.ref_log {
        assert_eq!(log.len(), d.log.len(), "{ctx}");
        for (e, r) in log.iter().rev().zip(&d.log) {
            let id = |o: Option<usize>| o.map(|i| d.changesets[i].id());
            assert_eq!(e.name, r.name, "{ctx}");
            assert_eq!((e.old, e.new), (id(r.old), id(r.new)), "{ctx}");
            assert_eq!(e.time, r.time, "{ctx}");
            assert_eq!(e.actor.as_deref(), r.actor, "{ctx}");
            assert_eq!(e.operation, r.operation, "{ctx}");
            assert_eq!(e.message.as_deref(), r.message, "{ctx}");
        }
        assert!(log.windows(2).all(|w| w[0].seq > w[1].seq), "{ctx}");
    } else {
        assert!(log.is_empty(), "{ctx}: no history before the log existed");
    }
    assert_eq!(
        store.schema_version().await.unwrap(),
        Some(version),
        "{ctx}"
    );

    // New writes work and continue the log (the sequence was not reset).
    let head = d.changesets[2].id();
    let next = store
        .put_changeset(&crate::tests::cs(424242))
        .await
        .unwrap();
    assert!(store
        .cas_ref_logged(
            "trunk",
            Some(head),
            Some(next),
            &RefUpdateContext::new("land")
        )
        .await
        .unwrap());
    let after = store.ref_log(Some("trunk"), None, 1).await.unwrap();
    assert_eq!(
        (after[0].old, after[0].new),
        (Some(head), Some(next)),
        "{ctx}"
    );
    if let Some(prev) = log.first() {
        assert!(after[0].seq > prev.seq, "{ctx}: seq continues");
    }
    check_meta_store_contract(store).await;
    let timing = leep_core::LeaseTiming {
        ttl: std::time::Duration::from_secs(2),
        stale_after: std::time::Duration::from_secs(2),
    };
    let key = leep_core::LeaseKey::land("main");
    let claim = leep_core::LeaseClaim {
        key: &key,
        holder: "verify",
        address: "http://127.0.0.1:1",
        protocol: 1,
        meta: "",
    };
    let lease = store.acquire_lease(&claim, timing).await.unwrap();
    assert_eq!((lease.holder.as_str(), lease.epoch), ("verify", 1), "{ctx}");
}

fn full_schema() -> BTreeSet<String> {
    [
        "changesets",
        "changeset_dicts",
        "store_features",
        "leases",
        "refs",
        "derived",
        "ref_log",
        "ref_log_name",
        "ref_log_land_id",
    ]
    .map(String::from)
    .into()
}

// ---- Tests ---------------------------------------------------------------

/// A new database gets the whole schema, version 1 and one history row with
/// the right checksum; opening again changes nothing.
#[tokio::test]
async fn fresh_database_migrates_once() {
    for db in dbs("fresh").await {
        let kind = db.kind();
        let before = db.report(db.compiled()).await;
        assert_eq!(before.from, 0, "{kind}");
        assert_eq!(before.pending.len(), LATEST as usize, "{kind}");
        assert!(before.adopted.is_empty(), "{kind}");
        assert!(!before.performed, "{kind}");
        assert!(db.objects().await.is_empty(), "dry run wrote nothing");

        let s = db.open(db.compiled(), SchemaMode::Migrate).await.unwrap();
        assert_eq!(
            s.schema_version().await.unwrap(),
            Some(LATEST as u64),
            "{kind}"
        );
        assert_eq!(db.objects().await, full_schema(), "{kind}");
        check_meta_store_contract(&*s).await;

        let after = db.report(db.compiled()).await;
        assert!(after.is_current(), "{kind}");
        assert_eq!(after.applied.len(), LATEST as usize, "{kind}");
        for (row, m) in after.applied.iter().zip(db.compiled()) {
            assert_eq!(
                (row.version, row.name.as_str()),
                (m.version, m.name),
                "{kind}"
            );
            assert_eq!(row.checksum, m.checksum(), "{kind}");
            assert!(row.applied_at > 1_700_000_000, "{kind}");
        }

        // Reopening is a no-op: the row is not rewritten.
        drop(s);
        let s = db.open(db.compiled(), SchemaMode::Migrate).await.unwrap();
        assert_eq!(
            db.report(db.compiled()).await.applied,
            after.applied,
            "{kind}"
        );
        drop(s);
        db.cleanup().await;
    }
}

/// Databases left by older code are adopted or finished, keep all their
/// data, and end at the current version with the same schema as a new one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn upgrade_from_every_stage() {
    for stage in STAGES {
        for db in dbs(&format!("up{}", stage.name.replace('-', ""))).await {
            let kind = db.kind();
            let d = data();
            load(&db, stage, &d).await;

            // What would happen, without doing it.
            let plan = db.report(db.compiled()).await;
            assert_eq!(plan.from, 0, "{kind} {}", stage.name);
            // Only the baseline is ever adopted; every later step runs.
            let versions = |m: &[MigrationRef]| m.iter().map(|m| m.version).collect::<Vec<_>>();
            if stage.ref_log {
                assert_eq!(versions(&plan.adopted), [1], "{kind} {}: adopt", stage.name);
                assert_eq!(
                    versions(&plan.pending),
                    (2..=LATEST).collect::<Vec<_>>(),
                    "{kind} {}",
                    stage.name
                );
            } else {
                assert_eq!(
                    versions(&plan.pending),
                    (1..=LATEST).collect::<Vec<_>>(),
                    "{kind} {}: finish",
                    stage.name
                );
                assert!(plan.adopted.is_empty(), "{kind} {}", stage.name);
            }
            assert!(
                !db.has_schema_migrations().await,
                "{kind} {}: dry run wrote nothing",
                stage.name
            );
            // --no-migrate refuses an un-versioned database and leaves it alone.
            let err = db
                .open(db.compiled(), SchemaMode::RequireCurrent)
                .await
                .err()
                .expect("pending");
            assert!(err.to_string().contains("pending"), "{err}");
            assert!(!db.has_schema_migrations().await, "{kind}");

            let s = db.open(db.compiled(), SchemaMode::Migrate).await.unwrap();
            verify(&*s, stage, &d, LATEST as u64).await;
            assert_eq!(db.objects().await, full_schema(), "{kind} {}", stage.name);
            let rep = db.report(db.compiled()).await;
            assert!(rep.is_current(), "{kind}");
            assert_eq!(rep.applied[0].checksum, db.compiled()[0].checksum());
            drop(s);
            // And now --no-migrate starts.
            db.open(db.compiled(), SchemaMode::RequireCurrent)
                .await
                .unwrap();
            db.cleanup().await;
        }
    }
}

/// A database at v1, as the binary before migration 2 left it (its compiled
/// prefix, data written with that version's SQL), upgrades with its data
/// intact. On Postgres the large tables then carry migration 2's analyze
/// thresholds, and `analyze` refreshes their statistics (ADR 0042).
#[tokio::test]
async fn upgrade_from_v1_sets_analyze_thresholds() {
    for db in dbs("fromv1").await {
        let kind = db.kind();
        let d = data();
        drop(
            db.open(&db.compiled()[..1], SchemaMode::Migrate)
                .await
                .unwrap(),
        );
        // The unversioned DDL is `IF NOT EXISTS`, so only the rows go in.
        load(&db, &STAGES[1], &d).await;
        let plan = db.report(db.compiled()).await;
        assert_eq!(plan.from, 1, "{kind}");
        assert_eq!(
            plan.pending.iter().map(|m| m.version).collect::<Vec<_>>(),
            (2..=LATEST).collect::<Vec<_>>(),
            "{kind}"
        );
        let s = db.open(db.compiled(), SchemaMode::Migrate).await.unwrap();
        verify(&*s, &STAGES[1], &d, LATEST as u64).await;
        drop(s);
        if let Db::Pg { url, schema } = &db {
            let store = PostgresMetaStore::connect_with_migrations(
                url,
                Some(schema),
                SchemaMode::RequireCurrent,
                db.compiled(),
            )
            .await
            .unwrap();
            let rows: Vec<(String, Option<Vec<String>>)> = sqlx::query_as(
                "SELECT c.relname::text, c.reloptions FROM pg_class c \
                 JOIN pg_namespace n ON n.oid = c.relnamespace \
                 WHERE n.nspname = $1 AND c.relname IN ('changesets', 'derived') ORDER BY 1",
            )
            .bind(schema)
            .fetch_all(&store.pool)
            .await
            .unwrap();
            let opts = |scale: &str| {
                Some(vec![
                    format!("autovacuum_analyze_scale_factor={scale}"),
                    "autovacuum_analyze_threshold=10000".to_string(),
                ])
            };
            assert_eq!(rows[0].0, "changesets");
            assert_eq!(rows[0].1, opts("0.02"));
            assert_eq!(rows[1].0, "derived");
            assert_eq!(rows[1].1, opts("0.01"));
            store.analyze().await.unwrap();
            let tuples: Vec<f32> = sqlx::query_scalar(
                "SELECT c.reltuples FROM pg_class c \
                 JOIN pg_namespace n ON n.oid = c.relnamespace \
                 WHERE n.nspname = $1 AND c.relname IN ('changesets', 'derived') ORDER BY c.relname",
            )
            .bind(schema)
            .fetch_all(&store.pool)
            .await
            .unwrap();
            let counts: (f32, f32) = sqlx::query_as(
                "SELECT (SELECT count(*) FROM changesets)::real, \
                 (SELECT count(*) FROM derived)::real",
            )
            .fetch_one(&store.pool)
            .await
            .unwrap();
            assert_eq!(tuples, [counts.0, counts.1], "{kind}");
            store.pool.close().await;
        }
        db.cleanup().await;
    }
}

/// A database at v2, as the binary before migration 3 left it, upgrades with
/// its changesets readable, gains an empty `changeset_dicts`, and stores
/// changesets as before until a compaction trains a dictionary (ADR 0053).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn upgrade_from_v2_adds_changeset_dicts() {
    for db in dbs("fromv2").await {
        let kind = db.kind();
        let d = data();
        drop(
            db.open(&db.compiled()[..2], SchemaMode::Migrate)
                .await
                .unwrap(),
        );
        load(&db, &STAGES[1], &d).await;
        let plan = db.report(db.compiled()).await;
        assert_eq!(plan.from, 2, "{kind}");
        assert_eq!(
            plan.pending.iter().map(|m| m.version).collect::<Vec<_>>(),
            (3..=LATEST).collect::<Vec<_>>(),
            "{kind}"
        );
        let s = db.open(db.compiled(), SchemaMode::Migrate).await.unwrap();
        verify(&*s, &STAGES[1], &d, LATEST as u64).await;
        assert_eq!(db.objects().await, full_schema(), "{kind}");
        drop(s);

        // Enough history to train on, in rows as v2 wrote them (canonical
        // bytes, as v3 still writes them before a dictionary exists).
        let more = crate::compact_tests::history(crate::compact::MIN_TRAINING_ROWS, 2);
        for chunk in more.chunks(250) {
            let values = vec!["(?, ?)"; chunk.len()].join(", ");
            let args = chunk
                .iter()
                .flat_map(|c| [Arg::Bytes(c.id().0 .0.to_vec()), Arg::Bytes(c.encode())])
                .collect();
            db.exec(
                &format!("INSERT INTO changesets (id, data) VALUES {values}"),
                args,
            )
            .await;
        }
        let opts = crate::compact::CompactOptions::default();
        let r = match &db {
            Db::Sqlite { url, .. } => {
                let s = SqliteMetaStore::connect(url).await.unwrap();
                assert!(s.has_dicts && s.codec.newest_id() == 0, "{kind}");
                crate::compact::run(&s, &opts, &mut |_| {}).await.unwrap()
            }
            Db::Pg { url, schema } => {
                let s = PostgresMetaStore::connect_with_migrations(
                    url,
                    Some(schema),
                    SchemaMode::Migrate,
                    db.compiled(),
                )
                .await
                .unwrap();
                assert!(s.has_dicts && s.codec.newest_id() == 0, "{kind}");
                let r = crate::compact::run(&s, &opts, &mut |_| {}).await.unwrap();
                s.pool.close().await;
                r
            }
        };
        assert_eq!(r.dictionary, 1, "{kind}");
        // The stage's changesets, verify's and the contract's, and the new
        // history; at least the history is worth compressing.
        assert!(r.rows > (d.changesets.len() + more.len()) as u64, "{kind}");
        assert!(r.rewritten >= more.len() as u64, "{kind}");
        let s = db
            .open(db.compiled(), SchemaMode::RequireCurrent)
            .await
            .unwrap();
        for c in d.changesets.iter().chain(&more) {
            assert_eq!(
                s.get_changeset(&c.id()).await.unwrap().as_ref(),
                Some(c),
                "{kind}"
            );
        }
        drop(s);
        db.cleanup().await;
    }
}

/// A database at v3 upgrades to `store_features` (ADR 0055). One with no
/// dictionary names no feature; one compacted at v3 (a dictionary exists)
/// gets `changeset-zstd-dict` from the migration. A store naming a feature
/// this binary does not know is refused on open, before any row is read.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn upgrade_from_v3_adds_store_features() {
    use crate::features::{CHANGESET_ZSTD_DICT, FEATURES_SCHEMA_VERSION};
    let v3 = FEATURES_SCHEMA_VERSION as usize - 1;
    for compacted in [false, true] {
        let name = if compacted { "fromv3c" } else { "fromv3" };
        for db in dbs(name).await {
            let kind = db.kind();
            let d = data();
            drop(
                db.open(&db.compiled()[..v3], SchemaMode::Migrate)
                    .await
                    .unwrap(),
            );
            load(&db, &STAGES[1], &d).await;
            if compacted {
                let samples: Vec<Vec<u8>> =
                    crate::compact_tests::history(crate::compact::MIN_TRAINING_ROWS, 4)
                        .iter()
                        .map(Changeset::encode)
                        .collect();
                let dict = codec::train(&samples, 16 << 10, 1).unwrap();
                db.exec(
                    "INSERT INTO changeset_dicts (id, created_at, data) VALUES (?, ?, ?)",
                    vec![Arg::Int(1), Arg::Int(1_700_000_000), Arg::Bytes(dict)],
                )
                .await;
            }
            let plan = db.report(db.compiled()).await;
            assert_eq!(plan.from, v3 as u64, "{kind}");
            let s = db.open(db.compiled(), SchemaMode::Migrate).await.unwrap();
            verify(&*s, &STAGES[1], &d, LATEST as u64).await;
            assert_eq!(db.objects().await, full_schema(), "{kind}");
            drop(s);
            let features = |db: &Db| {
                let (url, schema) = match db {
                    Db::Sqlite { url, .. } => (url.clone(), None),
                    Db::Pg { url, schema } => (url.clone(), Some(schema.clone())),
                };
                async move {
                    match schema {
                        None => {
                            let s = SqliteMetaStore::connect(&url).await.unwrap();
                            s.features().await.unwrap()
                        }
                        Some(schema) => {
                            let s = PostgresMetaStore::connect_with_schema(&url, &schema)
                                .await
                                .unwrap();
                            let f = s.features().await.unwrap();
                            s.pool.close().await;
                            f
                        }
                    }
                }
            };
            let want: Vec<String> = if compacted {
                vec![CHANGESET_ZSTD_DICT.into()]
            } else {
                vec![]
            };
            assert_eq!(features(&db).await, want, "{kind}");

            // A newer binary's feature locks this one out, in either mode.
            db.exec(
                "INSERT INTO store_features (name, added_at) VALUES (?, ?)",
                vec![Arg::Text("from-the-future".into()), Arg::Int(1)],
            )
            .await;
            for mode in [SchemaMode::Migrate, SchemaMode::RequireCurrent] {
                let err = db.open(db.compiled(), mode).await.err().unwrap();
                assert!(
                    err.to_string().contains("(from-the-future)"),
                    "{kind}: {err}"
                );
            }
            db.exec(
                "DELETE FROM store_features WHERE name = ?",
                vec![Arg::Text("from-the-future".into())],
            )
            .await;
            drop(
                db.open(db.compiled(), SchemaMode::RequireCurrent)
                    .await
                    .unwrap(),
            );
            db.cleanup().await;
        }
    }
}

/// A database at v4, with a ref log written by v4 servers (no land ids),
/// upgrades to land ids (ADR 0057): its entries read back unchanged, find no
/// land, and new moves record and refuse land ids as the contract says.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn upgrade_from_v4_adds_land_ids() {
    use leep_core::{LandId, LandKey};
    for db in dbs("fromv4").await {
        let kind = db.kind();
        let d = data();
        drop(
            db.open(&db.compiled()[..4], SchemaMode::Migrate)
                .await
                .unwrap(),
        );
        load(&db, &STAGES[1], &d).await;
        let plan = db.report(db.compiled()).await;
        assert_eq!(plan.from, 4, "{kind}");
        assert_eq!(
            plan.pending.iter().map(|m| m.name).collect::<Vec<_>>(),
            ["land_ids", "leases"],
            "{kind}"
        );
        let s = db.open(db.compiled(), SchemaMode::Migrate).await.unwrap();
        let id = LandId::from_bytes(&[3; 16]).unwrap();
        assert_eq!(
            s.find_lands("trunk", std::slice::from_ref(&id))
                .await
                .unwrap(),
            [None],
            "{kind}"
        );
        verify(&*s, &STAGES[1], &d, LATEST as u64).await;
        assert_eq!(db.objects().await, full_schema(), "{kind}");
        // A keyed move on the upgraded store: recorded with its entry.
        let (head, next) = (
            s.get_ref("trunk").await.unwrap().unwrap(),
            d.changesets[3].id(),
        );
        let ctx = RefUpdateContext::new("land").land(Some(LandKey {
            id: id.clone(),
            changeset: next,
        }));
        assert!(s
            .cas_ref_logged("trunk", Some(head), Some(next), &ctx)
            .await
            .unwrap());
        let found = s.find_lands("trunk", &[id]).await.unwrap();
        assert_eq!(found[0].unwrap().landed, next, "{kind}");
        drop(s);
        db.cleanup().await;
    }
}

/// On Postgres the land id columns and index may be built by hand before
/// migration 0005, the index CONCURRENTLY so ref moves are not blocked
/// (OPERATIONS.md, "Upgrading"); the migration then finds them and succeeds.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn land_id_index_built_by_hand_before_migration_5() {
    use leep_core::{LandId, LandKey};
    let Some(db) = Db::pg("v4hand").await else {
        return;
    };
    let d = data();
    drop(
        db.open(&db.compiled()[..4], SchemaMode::Migrate)
            .await
            .unwrap(),
    );
    load(&db, &STAGES[1], &d).await;
    db.exec_raw(&[
        "ALTER TABLE ref_log ADD COLUMN IF NOT EXISTS land_id BYTEA",
        "ALTER TABLE ref_log ADD COLUMN IF NOT EXISTS land_changeset BYTEA",
        "CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS ref_log_land_id \
         ON ref_log (name, land_id) WHERE land_id IS NOT NULL",
    ])
    .await;
    let s = db.open(db.compiled(), SchemaMode::Migrate).await.unwrap();
    verify(&*s, &STAGES[1], &d, LATEST as u64).await;
    assert_eq!(db.objects().await, full_schema());
    let id = LandId::from_bytes(&[5; 16]).unwrap();
    let head = s.get_ref("trunk").await.unwrap().unwrap();
    let next = d.changesets[3].id();
    let ctx = RefUpdateContext::new("land").land(Some(LandKey {
        id: id.clone(),
        changeset: next,
    }));
    assert!(s
        .cas_ref_logged("trunk", Some(head), Some(next), &ctx)
        .await
        .unwrap());
    assert_eq!(
        s.find_lands("trunk", &[id]).await.unwrap()[0]
            .unwrap()
            .landed,
        next
    );
    drop(s);
    db.cleanup().await;
}

/// The multi-step path: an un-versioned database adopts v1 and then runs the
/// steps after it; so does a database at v1; so does one one step behind.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_step_upgrades() {
    for db in dbs("steps").await {
        let kind = db.kind();
        let list = with(db.compiled(), &[ONCE, WIDGETS]);
        let stage = &STAGES[1];
        let d = data();
        load(&db, stage, &d).await;

        let plan = db.report(&list).await;
        assert_eq!(
            plan.adopted.iter().map(|m| m.version).collect::<Vec<_>>(),
            [1],
            "{kind}"
        );
        assert_eq!(
            plan.pending.iter().map(|m| m.version).collect::<Vec<_>>(),
            (2..=LATEST + 2).collect::<Vec<_>>(),
            "{kind}"
        );
        let s = db.open(&list, SchemaMode::Migrate).await.unwrap();
        verify(&*s, stage, &d, LATEST as u64 + 2).await;
        drop(s);
        let rep = db.report(&list).await;
        assert_eq!(
            rep.applied.iter().map(|a| a.version).collect::<Vec<_>>(),
            (1..=LATEST + 2).collect::<Vec<_>>(),
            "{kind}"
        );
        db.exec(
            "INSERT INTO widgets (id, label, size) VALUES (2, 'two', 3)",
            vec![],
        )
        .await;
        db.cleanup().await;
    }
    // From each earlier versioned state: a database made by an earlier
    // binary (its compiled prefix) upgrades with data intact.
    for db in dbs("steps2").await {
        let kind = db.kind();
        let list = with(db.compiled(), &[ONCE, WIDGETS]);
        for from in 1..list.len() {
            let d = data();
            let s = db.open(&list[..from], SchemaMode::Migrate).await.unwrap();
            for cs in &d.changesets {
                s.put_changeset(cs).await.unwrap();
            }
            for (name, i) in &d.refs {
                let id = d.changesets[*i].id();
                assert!(s.cas_ref(name, None, Some(id)).await.unwrap());
            }
            drop(s);
            let s = db.open(&list, SchemaMode::Migrate).await.unwrap();
            assert_eq!(
                s.schema_version().await.unwrap(),
                Some(LATEST as u64 + 2),
                "{kind} from {from}"
            );
            assert_eq!(s.list_refs().await.unwrap().len(), 2, "{kind} from {from}");
            for cs in &d.changesets {
                assert_eq!(s.get_changeset(&cs.id()).await.unwrap().as_ref(), Some(cs));
            }
            drop(s);
            db.cleanup_data(&list).await;
        }
        db.cleanup().await;
    }
}

impl Db {
    /// Back to an empty database, keeping the backend.
    async fn cleanup_data(&self, _list: &[Migration]) {
        match self {
            Db::Sqlite { dir, .. } => {
                for e in std::fs::read_dir(dir).unwrap() {
                    std::fs::remove_file(e.unwrap().path()).ok();
                }
            }
            Db::Pg { .. } => {
                let q = self.quoted();
                self.exec_raw(&[&format!("DROP SCHEMA IF EXISTS {q} CASCADE")])
                    .await;
            }
        }
    }
}

/// A database newer than the binary is refused before anything is written,
/// whatever the mode.
#[tokio::test]
async fn newer_database_is_refused() {
    for db in dbs("newer").await {
        let kind = db.kind();
        let newer = with(db.compiled(), &[ONCE]);
        db.open(&newer, SchemaMode::Migrate).await.unwrap();
        let before = db.report(&newer).await;
        for mode in [SchemaMode::Migrate, SchemaMode::RequireCurrent] {
            let err = db
                .open(db.compiled(), mode)
                .await
                .err()
                .expect("must refuse");
            let msg = err.to_string();
            assert!(
                msg.contains(&format!(
                    "database schema v{} is newer than this server (max v{LATEST})",
                    LATEST + 1
                )) && msg.contains("upgrade the server"),
                "{kind}: {msg}"
            );
        }
        assert!(db.migrate(db.compiled(), false).await.is_err(), "{kind}");
        assert_eq!(db.report(&newer).await, before, "{kind}: untouched");
        db.cleanup().await;
    }
}

/// Editing a migration after it was applied is caught.
#[tokio::test]
async fn edited_migration_is_refused() {
    for db in dbs("edited").await {
        let kind = db.kind();
        db.open(db.compiled(), SchemaMode::Migrate).await.unwrap();
        let mut edited = db.compiled().to_vec();
        edited[0].sql =
            "CREATE TABLE IF NOT EXISTS changesets (id BLOB PRIMARY KEY, data BLOB NOT NULL);";
        for mode in [SchemaMode::Migrate, SchemaMode::RequireCurrent] {
            let err = db.open(&edited, mode).await.err().expect("must refuse");
            let msg = err.to_string();
            assert!(
                msg.contains("migration v1 (initial)") && msg.contains("immutable"),
                "{kind}: {msg}"
            );
        }
        // The CRLF form of the same files is the same migrations.
        let crlf: Vec<Migration> = db
            .compiled()
            .iter()
            .map(|m| Migration {
                sql: Box::leak(m.sql.replace('\n', "\r\n").into_boxed_str()),
                ..*m
            })
            .collect();
        for (a, b) in crlf.iter().zip(db.compiled()) {
            assert_eq!(a.checksum(), b.checksum());
        }
        db.open(&crlf, SchemaMode::Migrate).await.unwrap();
        db.cleanup().await;
    }
}

/// A recorded migration the binary has never heard of, or a hole in the
/// history, is refused.
#[tokio::test]
async fn inconsistent_history_is_refused() {
    for db in dbs("gap").await {
        let kind = db.kind();
        let list = with(db.compiled(), &[ONCE, WIDGETS]);
        db.open(&list, SchemaMode::Migrate).await.unwrap();
        db.exec(
            &format!(
                "DELETE FROM schema_migrations WHERE version = {}",
                LATEST + 1
            ),
            vec![],
        )
        .await;
        let err = db
            .open(&list, SchemaMode::Migrate)
            .await
            .err()
            .expect("hole");
        assert!(
            err.to_string().contains(&format!(
                "v{} is missing but v{} is applied",
                LATEST + 1,
                LATEST + 2
            )),
            "{kind}: {err}"
        );
        // A list that is not 1, 2, 3... is a programming error, caught early.
        let bad = [ONCE];
        assert!(db.migrate(&bad, true).await.is_err(), "{kind}");
        db.cleanup().await;
    }
}

/// A migration that fails midway leaves nothing behind: not its earlier
/// statements, not the migrations before it in the same run, not even the
/// history table.
#[tokio::test]
async fn failed_migration_rolls_back_fully() {
    for db in dbs("rollback").await {
        let kind = db.kind();
        // On a new database: the compiled steps and `once` succeed, `half` fails.
        let bad = with(db.compiled(), &[ONCE, HALF]);
        let err = db
            .open(&bad, SchemaMode::Migrate)
            .await
            .err()
            .expect("fails");
        assert!(
            err.to_string()
                .contains(&format!("migration v{} (half) failed", LATEST + 2)),
            "{err}"
        );
        assert!(db.objects().await.is_empty(), "{kind}: nothing created");
        let rep = db.report(db.compiled()).await;
        assert!(rep.applied.is_empty() && rep.from == 0, "{kind}");

        // On a database at the compiled version: still there, with its data.
        let s = db.open(db.compiled(), SchemaMode::Migrate).await.unwrap();
        let id = s.put_changeset(&crate::tests::cs(1)).await.unwrap();
        drop(s);
        assert!(db.open(&bad, SchemaMode::Migrate).await.is_err(), "{kind}");
        assert_eq!(
            db.objects().await,
            full_schema(),
            "{kind}: no once, no half"
        );
        let rep = db.report(db.compiled()).await;
        assert_eq!(rep.applied.len(), LATEST as usize, "{kind}");
        let s = db.open(db.compiled(), SchemaMode::Migrate).await.unwrap();
        assert!(s.get_changeset(&id).await.unwrap().is_some(), "{kind}");
        drop(s);

        // Fixing the list lets the same database move on.
        let good = with(db.compiled(), &[ONCE, WIDGETS]);
        let s = db.open(&good, SchemaMode::Migrate).await.unwrap();
        assert_eq!(
            s.schema_version().await.unwrap(),
            Some(LATEST as u64 + 2),
            "{kind}"
        );
        drop(s);
        db.cleanup().await;
    }
}

/// Many migrators at once (separate pools, as separate servers would be)
/// apply each step exactly once; the rest find the work done.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_migrators_apply_once() {
    for db in dbs("race").await {
        let kind = db.kind();
        let db = Arc::new(db);
        let list = Arc::new(with(db.compiled(), &[ONCE, WIDGETS]));
        let mut tasks = Vec::new();
        for _ in 0..8 {
            let (db, list) = (db.clone(), list.clone());
            tasks.push(tokio::spawn(async move {
                db.migrate(&list, false).await.unwrap()
            }));
        }
        let mut performed = 0;
        for t in tasks {
            let r = t.await.unwrap();
            assert_eq!(r.to, LATEST as u64 + 2, "{kind}");
            performed += r.performed as usize;
        }
        assert_eq!(performed, 1, "{kind}: exactly one migrator did the work");
        let rep = db.report(&list).await;
        assert_eq!(
            rep.applied.iter().map(|a| a.version).collect::<Vec<_>>(),
            (1..=LATEST + 2).collect::<Vec<_>>(),
            "{kind}"
        );
        // `once` holds the single row its migration inserted.
        let s = db.open(&list, SchemaMode::Migrate).await.unwrap();
        assert_eq!(s.schema_version().await.unwrap(), Some(LATEST as u64 + 2));
        drop(s);
        db.exec("INSERT INTO once (n) VALUES (2)", vec![]).await;
        let db = Arc::try_unwrap(db).ok().unwrap();
        db.cleanup().await;
    }
}

/// Servers racing to start on an un-versioned database adopt it once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_adoption_records_once() {
    for db in dbs("adoptrace").await {
        let kind = db.kind();
        let d = data();
        load(&db, &STAGES[1], &d).await;
        let db = Arc::new(db);
        let mut tasks = Vec::new();
        for _ in 0..8 {
            let db = db.clone();
            tasks.push(tokio::spawn(async move {
                db.migrate(db.compiled(), false).await.unwrap()
            }));
        }
        let mut performed = 0;
        for t in tasks {
            performed += t.await.unwrap().performed as usize;
        }
        assert_eq!(performed, 1, "{kind}");
        let rep = db.report(db.compiled()).await;
        assert_eq!(rep.applied.len(), LATEST as usize, "{kind}");
        let db = Arc::try_unwrap(db).ok().unwrap();
        let s = db.open(db.compiled(), SchemaMode::Migrate).await.unwrap();
        verify(&*s, &STAGES[1], &d, LATEST as u64).await;
        drop(s);
        db.cleanup().await;
    }
}

/// The compiled lists are well formed and the two backends agree on versions
/// and names, so `schema_version` means the same thing everywhere.
#[test]
fn compiled_lists_are_consistent() {
    assert_eq!(SQLITE_MIGRATIONS.len(), POSTGRES_MIGRATIONS.len());
    for (i, (a, b)) in SQLITE_MIGRATIONS
        .iter()
        .zip(POSTGRES_MIGRATIONS)
        .enumerate()
    {
        assert_eq!(a.version, i as i64 + 1);
        assert_eq!((a.version, a.name), (b.version, b.name));
        assert_eq!(a.satisfied_by.is_empty(), b.satisfied_by.is_empty());
        if i > 0 {
            assert!(
                a.satisfied_by.is_empty(),
                "only the baseline adopts (v{})",
                a.version
            );
        }
    }
}

/// `migrate_meta_store` and `open_meta_store_with` dispatch by URL.
#[tokio::test]
async fn url_dispatch() {
    let db = Db::sqlite("url");
    let Db::Sqlite { url, .. } = &db else {
        unreachable!()
    };
    let dry = migrate_meta_store(url, true).await.unwrap();
    assert_eq!(
        (dry.from, dry.to, dry.latest, dry.performed),
        (0, 0, LATEST as u64, false)
    );
    assert!(open_meta_store_with(url, SchemaMode::RequireCurrent)
        .await
        .is_err());
    let done = migrate_meta_store(url, false).await.unwrap();
    assert_eq!(
        (done.from, done.to, done.performed),
        (0, LATEST as u64, true)
    );
    let s = open_meta_store_with(url, SchemaMode::RequireCurrent)
        .await
        .unwrap();
    assert_eq!(s.schema_version().await.unwrap(), Some(LATEST as u64));
    assert!(migrate_meta_store("mem://", false).await.is_err());
    // Only Postgres has statistics to refresh.
    assert!(!analyze_meta_store(url).await.unwrap());
    assert!(!analyze_meta_store("mem://").await.unwrap());
    assert!(analyze_meta_store("redis://x").await.is_err());
    assert_eq!(
        open_meta_store("mem://")
            .await
            .unwrap()
            .schema_version()
            .await
            .unwrap(),
        None
    );
    db.cleanup().await;
}
