use std::collections::{BTreeMap, BTreeSet};
use std::str::FromStr;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use leep_core::prefix::IdPrefix;
use leep_core::store::unix_now;
use leep_core::{
    check_land_key, check_ref_chain, Changeset, ChangesetId, DeleteStep, DerivedRow, Hash, LandId,
    LandRecord, MetaStore, RefLogEntry, RefStep, RefUpdateContext, Result, RowCount,
};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{AssertSqlSafe, PgConnection, PgPool, Row};

use crate::codec::Codec;
use crate::drop::{self, nothing_left, Cursor, KEY_BATCH};
use crate::migrate::{self, AppliedMigration, Migration, MigrationReport, Session};
use crate::{
    changesets_in_order, derived_columns, land_columns, lands_in_order, log_entry,
    log_insert_error, storage, to_hash, to_id, unique, values_in_order, LandRow, LogRow,
    SchemaMode,
};

/// The compiled migration list (ADR 0023). Append only.
pub const MIGRATIONS: &[Migration] = &[
    Migration {
        version: 1,
        name: "initial",
        sql: include_str!("../migrations/postgres/0001_initial.sql"),
        satisfied_by: &["changesets", "refs", "derived", "ref_log", "ref_log_name"],
    },
    Migration {
        version: 2,
        name: "analyze_thresholds",
        sql: include_str!("../migrations/postgres/0002_analyze_thresholds.sql"),
        satisfied_by: &[],
    },
    Migration {
        version: 3,
        name: "changeset_dicts",
        sql: include_str!("../migrations/postgres/0003_changeset_dicts.sql"),
        satisfied_by: &[],
    },
    Migration {
        version: 4,
        name: "store_features",
        sql: include_str!("../migrations/postgres/0004_store_features.sql"),
        satisfied_by: &[],
    },
    Migration {
        version: 5,
        name: "land_ids",
        sql: include_str!("../migrations/postgres/0005_land_ids.sql"),
        satisfied_by: &[],
    },
    Migration {
        version: 6,
        name: "leases",
        sql: include_str!("../migrations/postgres/0006_leases.sql"),
        satisfied_by: &[],
    },
    Migration {
        version: 7,
        name: "changeset_stored_at",
        sql: include_str!("../migrations/postgres/0007_changeset_stored_at.sql"),
        satisfied_by: &[],
    },
];

/// Every table a bulk load fills, for [`PostgresMetaStore::analyze`].
pub(crate) const ANALYZE_TABLES: &str = "ANALYZE changesets, refs, derived, ref_log";

/// Serializes migrators. Servers older than ADR 0023 take it too, around
/// their `CREATE TABLE IF NOT EXISTS`.
const MIGRATE_LOCK: i64 = 7164737;

/// How long a migration waits for a table lock. `ALTER TABLE` takes ACCESS
/// EXCLUSIVE and queues behind any open transaction on the table (a backup
/// snapshot, `gc --unreachable --dry-run`); while it waits, every reader and
/// writer queues behind it. Failing fast keeps the store serving.
const MIGRATION_LOCK_TIMEOUT: &str = "5s";

/// Taken after a ref changes and before its log row is inserted, so `seq`
/// order is commit order. Nothing waits on a row lock while holding it.
/// The index warmer's ref-log tail (`leep_derive::warm`) depends on this:
/// it reads entries above the last `seq` it saw, which is only complete if
/// no transaction can commit a lower `seq` after a higher one is visible.
/// `seq` may still have gaps (a rolled-back insert consumes its value).
const LOG_LOCK: i64 = 7164738;

/// Connections reserved for lease calls of every scope: one renewal
/// statement per process and scope every renew interval, plus the odd
/// claim or release.
const LEASE_POOL_SIZE: u32 = 2;

pub struct PostgresMetaStore {
    pub(crate) pool: PgPool,
    /// Lease calls only (ADR 0056), every scope, so a renewal never waits
    /// for a connection behind data traffic: a lease not renewed in time is
    /// taken over. Connects on first use.
    pub(crate) lease_pool: PgPool,
    /// Changeset dictionaries (ADR 0053).
    pub(crate) codec: Codec,
    /// Whether the schema has `changeset_dicts`: tests open stores with
    /// shorter migration lists.
    pub(crate) has_dicts: bool,
    /// Whether `changesets` has `stored_at` (ADR 0059).
    pub(crate) has_stored_at: bool,
    /// Whether the schema has `store_features` (ADR 0055).
    pub(crate) has_features: bool,
}

impl PostgresMetaStore {
    /// Connect and migrate, using the connection's default schema.
    pub async fn connect(url: &str) -> Result<Self> {
        Self::connect_with(url, SchemaMode::Migrate).await
    }

    /// Connect; with [`SchemaMode::RequireCurrent`] refuse instead of
    /// migrating when the schema is not current.
    pub async fn connect_with(url: &str, mode: SchemaMode) -> Result<Self> {
        Self::connect_with_pool(url, mode, crate::DEFAULT_POSTGRES_POOL_SIZE).await
    }

    /// [`connect_with`](Self::connect_with), opening at most `pool_size`
    /// connections.
    pub async fn connect_with_pool(url: &str, mode: SchemaMode, pool_size: u32) -> Result<Self> {
        Self::open(url, None, mode, MIGRATIONS, pool_size, None).await
    }

    /// [`connect_with_pool`](Self::connect_with_pool), with every statement
    /// on the data pool bounded by `statement_timeout` (Postgres cancels it
    /// and the call fails). Lease calls, migrations and schema creation are
    /// not bounded.
    pub async fn connect_with_options(
        url: &str,
        mode: SchemaMode,
        pool_size: u32,
        statement_timeout: Option<Duration>,
    ) -> Result<Self> {
        Self::open(url, None, mode, MIGRATIONS, pool_size, statement_timeout).await
    }

    /// A store in `schema` whose data pool bounds statements, for tests of
    /// that bound.
    #[doc(hidden)]
    pub async fn connect_with_schema_and_timeout(
        url: &str,
        schema: &str,
        statement_timeout: Duration,
    ) -> Result<Self> {
        Self::open(
            url,
            Some(schema.to_string()),
            SchemaMode::Migrate,
            MIGRATIONS,
            crate::DEFAULT_POSTGRES_POOL_SIZE,
            Some(statement_timeout),
        )
        .await
    }

    /// Like [`connect`](Self::connect), but keeps all tables in `schema`
    /// (created if missing). Lets several stores share one database.
    pub async fn connect_with_schema(url: &str, schema: &str) -> Result<Self> {
        Self::open(
            url,
            Some(schema.to_string()),
            SchemaMode::Migrate,
            MIGRATIONS,
            crate::DEFAULT_POSTGRES_POOL_SIZE,
            None,
        )
        .await
    }

    /// [`connect_with_schema`](Self::connect_with_schema) with a mode and an
    /// explicit migration list, so tests can inject steps.
    #[doc(hidden)]
    pub async fn connect_with_migrations(
        url: &str,
        schema: Option<&str>,
        mode: SchemaMode,
        migrations: &[Migration],
    ) -> Result<Self> {
        Self::open(
            url,
            schema.map(str::to_string),
            mode,
            migrations,
            crate::DEFAULT_POSTGRES_POOL_SIZE,
            None,
        )
        .await
    }

    /// Apply pending migrations, or with `dry_run` only report them.
    pub async fn migrate(url: &str, dry_run: bool) -> Result<MigrationReport> {
        Self::migrate_with(url, None, MIGRATIONS, dry_run).await
    }

    #[doc(hidden)]
    pub async fn migrate_with(
        url: &str,
        schema: Option<&str>,
        migrations: &[Migration],
        dry_run: bool,
    ) -> Result<MigrationReport> {
        let pool = Self::pool(
            url,
            schema.map(str::to_string),
            crate::DEFAULT_POSTGRES_POOL_SIZE,
            None,
        )
        .await?;
        let r = Self::run(&pool, migrations, !dry_run).await;
        pool.close().await;
        r
    }

    /// Refresh the planner statistics of every table. Autovacuum would get
    /// there, but only a minute or more after a bulk load ends, and the
    /// queries that run right after one (deriving indexes over what was just
    /// imported) are the ones a stale estimate hurts (ADR 0042).
    pub async fn analyze(&self) -> Result<()> {
        use sqlx::Executor;
        self.pool
            .execute(ANALYZE_TABLES)
            .await
            .map(|_| ())
            .map_err(storage)
    }

    async fn open(
        url: &str,
        schema: Option<String>,
        mode: SchemaMode,
        migrations: &[Migration],
        pool_size: u32,
        statement_timeout: Option<Duration>,
    ) -> Result<Self> {
        let pool = Self::pool(url, schema.clone(), pool_size, statement_timeout).await?;
        let report = Self::run(&pool, migrations, mode == SchemaMode::Migrate).await?;
        if mode == SchemaMode::RequireCurrent {
            migrate::require_current(&report)?;
        }
        // Never bounded: a renewal that waits on a busy database should be
        // late, not cancelled, and the lease TTL already bounds it.
        let lease_pool = Self::options(schema, LEASE_POOL_SIZE, None)
            .connect_lazy_with(PgConnectOptions::from_str(url).map_err(storage)?);
        let store = Self::with_pool(pool, lease_pool, migrations);
        crate::features::check(&store.features().await?)?;
        store.load_dicts().await?;
        Ok(store)
    }

    async fn pool(
        url: &str,
        schema: Option<String>,
        max: u32,
        statement_timeout: Option<Duration>,
    ) -> Result<PgPool> {
        let opts = PgConnectOptions::from_str(url).map_err(storage)?;
        if let Some(schema) = &schema {
            let quoted = quote_ident(schema);
            let admin = PgPoolOptions::new()
                .max_connections(1)
                .connect_with(opts.clone())
                .await
                .map_err(storage)?;
            sqlx::query(AssertSqlSafe(format!(
                "CREATE SCHEMA IF NOT EXISTS {quoted}"
            )))
            .execute(&admin)
            .await
            .map_err(storage)?;
            admin.close().await;
        }
        Self::options(schema, max, statement_timeout)
            .connect_with(opts)
            .await
            .map_err(storage)
    }

    /// `statement_timeout` is set per connection because sqlx never cancels
    /// a statement whose future is dropped: without it, a query of a request
    /// cut off by the server's request timeout runs on in Postgres to the
    /// end, holding its connection and locks.
    fn options(
        schema: Option<String>,
        max: u32,
        statement_timeout: Option<Duration>,
    ) -> PgPoolOptions {
        let timeout = statement_timeout
            .filter(|t| !t.is_zero())
            .map(|t| format!("SET statement_timeout = {}", t.as_millis().max(1)));
        PgPoolOptions::new()
            .max_connections(max.max(1))
            .after_connect(move |conn, _| {
                let set = schema
                    .as_ref()
                    .map(|s| format!("SET search_path TO {}", quote_ident(s)));
                let timeout = timeout.clone();
                Box::pin(async move {
                    if let Some(set) = set {
                        sqlx::query(AssertSqlSafe(set)).execute(&mut *conn).await?;
                    }
                    if let Some(timeout) = timeout {
                        sqlx::query(AssertSqlSafe(timeout))
                            .execute(&mut *conn)
                            .await?;
                    }
                    Ok(())
                })
            })
    }

    /// One transaction under the advisory lock, so servers starting together
    /// migrate once and the rest find it done. A read-only look takes no lock.
    async fn run(pool: &PgPool, migrations: &[Migration], write: bool) -> Result<MigrationReport> {
        let mut tx = pool.begin().await.map_err(storage)?;
        // A migration may rewrite a large table, and waits for another
        // server's; the data pool's statement bound is not meant for it.
        sqlx::query("SET LOCAL statement_timeout = 0")
            .execute(&mut *tx)
            .await
            .map_err(storage)?;
        if write {
            sqlx::query("SELECT pg_advisory_xact_lock($1)")
                .bind(MIGRATE_LOCK)
                .execute(&mut *tx)
                .await
                .map_err(storage)?;
            // After the advisory lock, which may rightly wait for another
            // migrator.
            sqlx::query(AssertSqlSafe(format!(
                "SET LOCAL lock_timeout = '{MIGRATION_LOCK_TIMEOUT}'"
            )))
            .execute(&mut *tx)
            .await
            .map_err(storage)?;
        }
        let report = migrate::drive(&mut PgSession(&mut tx), migrations, write).await?;
        tx.commit().await.map_err(storage)?;
        Ok(report)
    }
}

/// Kinds with fewer rows than this are counted, not estimated; counting
/// this many takes milliseconds.
const EXACT_UP_TO: i64 = 100_000;

/// `reltuples`, and the most common values of `kind` with their frequencies.
type StatsRow = (f64, Option<Vec<String>>, Option<Vec<f64>>);

impl PostgresMetaStore {
    /// Rows of `kind` by the last `ANALYZE`: the table's row estimate times
    /// the kind's frequency among its most common values. `None` when the
    /// table was never analyzed or the kind is not among them.
    async fn planner_rows(&self, kind: &str) -> Result<Option<u64>> {
        let row: Option<StatsRow> = sqlx::query_as(
            "SELECT c.reltuples::float8, s.most_common_vals::text::text[], \
                    s.most_common_freqs::float8[] \
             FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
             LEFT JOIN pg_stats s ON s.schemaname = n.nspname AND s.tablename = c.relname \
                 AND s.attname = 'kind' \
             WHERE c.oid = 'derived'::regclass",
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(storage)?;
        let Some((tuples, Some(vals), Some(freqs))) = row else {
            return Ok(None);
        };
        if tuples <= 0.0 {
            return Ok(None);
        }
        Ok(vals
            .iter()
            .zip(&freqs)
            .find(|(v, _)| v.as_str() == kind)
            .map(|(_, f)| (tuples * f).round() as u64))
    }
}

struct PgSession<'a>(&'a mut PgConnection);

#[async_trait]
impl Session for PgSession<'_> {
    async fn objects(&mut self) -> Result<BTreeSet<String>> {
        let rows = sqlx::query(
            "SELECT tablename::text FROM pg_tables WHERE schemaname = current_schema() \
             UNION ALL \
             SELECT indexname::text FROM pg_indexes WHERE schemaname = current_schema()",
        )
        .fetch_all(&mut *self.0)
        .await
        .map_err(storage)?;
        Ok(rows.into_iter().map(|r| r.get(0)).collect())
    }

    async fn applied(&mut self) -> Result<Vec<AppliedMigration>> {
        let rows = sqlx::query(
            "SELECT version, name, applied_at, checksum FROM schema_migrations ORDER BY version",
        )
        .fetch_all(&mut *self.0)
        .await
        .map_err(storage)?;
        Ok(rows
            .into_iter()
            .map(|r| AppliedMigration {
                version: r.get(0),
                name: r.get(1),
                applied_at: r.get(2),
                checksum: r.get(3),
            })
            .collect())
    }

    async fn create_table(&mut self) -> Result<()> {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS schema_migrations (version BIGINT PRIMARY KEY, \
             name TEXT NOT NULL, applied_at BIGINT NOT NULL, checksum TEXT NOT NULL)",
        )
        .execute(&mut *self.0)
        .await
        .map_err(storage)?;
        Ok(())
    }

    async fn execute(&mut self, sql: &'static str) -> Result<()> {
        use sqlx::Executor;
        // A bare string takes the multi-statement path.
        self.0.execute(sql).await.map(|_| ()).map_err(|e| {
            // 55P03 lock_not_available: `lock_timeout` expired.
            if e.as_database_error().and_then(|d| d.code()).as_deref() == Some("55P03") {
                storage(format!(
                    "could not lock a table within {MIGRATION_LOCK_TIMEOUT}: a long \
                     transaction holds it (a backup, gc --unreachable --dry-run?). \
                     Nothing was changed; retry when it has finished ({e})"
                ))
            } else {
                storage(e)
            }
        })
    }

    async fn record(&mut self, m: &AppliedMigration) -> Result<()> {
        sqlx::query(
            "INSERT INTO schema_migrations (version, name, applied_at, checksum) \
             VALUES ($1, $2, $3, $4)",
        )
        .bind(m.version)
        .bind(&m.name)
        .bind(m.applied_at)
        .bind(&m.checksum)
        .execute(&mut *self.0)
        .await
        .map_err(storage)?;
        Ok(())
    }
}

fn quote_ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

impl PostgresMetaStore {
    fn with_pool(pool: PgPool, lease_pool: PgPool, migrations: &[Migration]) -> Self {
        Self {
            pool,
            lease_pool,
            codec: Codec::default(),
            has_dicts: migrations
                .iter()
                .any(|m| m.version >= crate::compact::DICTS_SCHEMA_VERSION),
            has_features: migrations
                .iter()
                .any(|m| m.version >= crate::features::FEATURES_SCHEMA_VERSION),
            has_stored_at: migrations
                .iter()
                .any(|m| m.version >= crate::STORED_AT_SCHEMA_VERSION),
        }
    }

    /// The format features the store names (ADR 0055).
    pub async fn features(&self) -> Result<Vec<String>> {
        if !self.has_features {
            return Ok(Vec::new());
        }
        sqlx::query_scalar("SELECT name FROM store_features ORDER BY name")
            .fetch_all(&self.pool)
            .await
            .map_err(storage)
    }

    /// Record that the store's rows may use `name`; idempotent.
    pub(crate) async fn add_feature(&self, name: &str) -> Result<()> {
        if !self.has_features {
            return Err(leep_core::Error::Invalid(format!(
                "the schema predates store features (v{}); run `leep-server migrate` first",
                crate::features::FEATURES_SCHEMA_VERSION
            )));
        }
        sqlx::query(
            "INSERT INTO store_features (name, added_at) VALUES ($1, $2) ON CONFLICT DO NOTHING",
        )
        .bind(name)
        .bind(unix_now())
        .execute(&self.pool)
        .await
        .map_err(storage)?;
        Ok(())
    }

    /// Learn the changeset dictionaries added since this store last looked.
    pub(crate) async fn load_dicts(&self) -> Result<()> {
        if !self.has_dicts {
            return Ok(());
        }
        let ids: Vec<i64> = sqlx::query_scalar("SELECT id FROM changeset_dicts ORDER BY id")
            .fetch_all(&self.pool)
            .await
            .map_err(storage)?;
        let mut rows = Vec::new();
        for id in ids {
            if u32::try_from(id).is_ok_and(|i| self.codec.knows(i)) {
                continue;
            }
            let data: Vec<u8> =
                sqlx::query_scalar("SELECT data FROM changeset_dicts WHERE id = $1")
                    .bind(id)
                    .fetch_one(&self.pool)
                    .await
                    .map_err(storage)?;
            rows.push((id, data));
        }
        self.codec.add(rows)?;
        self.codec.mark_checked();
        Ok(())
    }

    /// The row bytes for `cs`, with the newest dictionary any writer could
    /// know of a minute ago.
    async fn encode(&self, cs: &Changeset) -> Result<Vec<u8>> {
        if self.has_dicts && self.codec.due() {
            self.load_dicts().await?;
        }
        Ok(self.codec.encode(cs))
    }

    /// Decode fetched rows, first learning any dictionary they name that
    /// this store has not seen (a compaction ran since it started).
    async fn decode_rows(
        &self,
        ids: &[ChangesetId],
        rows: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Result<Vec<Option<Changeset>>> {
        if self
            .codec
            .missing(rows.iter().map(|r| r.1.as_slice()))
            .is_some()
        {
            self.load_dicts().await?;
        }
        changesets_in_order(&self.codec, ids, rows.into_iter())
    }
}

/// Run first in a walk step's transaction; see [`PostgresMetaStore::walk_step`].
pub(crate) const NO_INDEX_PATHS: &str = "SELECT set_config('enable_indexscan', 'off', true), \
     set_config('enable_indexonlyscan', 'off', true), \
     set_config('enable_bitmapscan', 'off', true)";

/// One walk step: the rows of kinds `$1` on pages `[$2, $3)`, counted per kind.
pub(crate) const WALK_DELETE: &str = "WITH gone AS (DELETE FROM derived \
     WHERE ctid >= $2::tid AND ctid < $3::tid AND kind = ANY($1) RETURNING kind) \
     SELECT kind, count(*) FROM gone GROUP BY kind";

impl PostgresMetaStore {
    /// Pages in `derived` now; a walk ends when it reaches the current end.
    async fn derived_pages(&self) -> Result<u64> {
        let n: i64 = sqlx::query_scalar(
            "SELECT pg_relation_size('derived') / current_setting('block_size')::bigint",
        )
        .fetch_one(&self.pool)
        .await
        .map_err(storage)?;
        Ok(n as u64)
    }

    /// Delete the rows of `kinds` on pages `[at, at + span)`. Index paths are
    /// switched off for the statement: with an estimate of a row or two for
    /// a kind (ADR 0042) Postgres would otherwise walk the whole kind
    /// through the primary key for every page range.
    async fn walk_step(&self, kinds: &[&str], at: u64, span: u64) -> Result<DeleteStep> {
        let pages = self.derived_pages().await?;
        let mut deleted = vec![0; kinds.len()];
        if at >= pages {
            return Ok(DeleteStep {
                deleted,
                next: None,
                done: pages,
                total: pages,
                unit: "pages",
            });
        }
        // Block numbers are 32 bits; the last range ends past every block.
        let end = (at + span).min(u64::from(u32::MAX));
        let started = std::time::Instant::now();
        let mut tx = self.pool.begin().await.map_err(storage)?;
        sqlx::query(NO_INDEX_PATHS)
            .execute(&mut *tx)
            .await
            .map_err(storage)?;
        let gone: Vec<(String, i64)> = sqlx::query_as(WALK_DELETE)
            .bind(kinds)
            .bind(format!("({at},0)"))
            .bind(format!("({end},0)"))
            .fetch_all(&mut *tx)
            .await
            .map_err(storage)?;
        tx.commit().await.map_err(storage)?;
        drop::tally(
            kinds,
            &mut deleted,
            gone.into_iter().map(|(k, n)| (k, n as u64)),
        );
        let next = Cursor::Walk {
            at: end,
            span: drop::next_span(span, started.elapsed()),
        };
        Ok(DeleteStep {
            deleted,
            next: Some(next.encode()),
            done: end.min(pages),
            total: pages,
            unit: "pages",
        })
    }

    /// Delete the next [`KEY_BATCH`] rows of `kinds[kind]` after `after`, by
    /// one ordered walk of the primary key deleted by `ctid`: a plan no
    /// estimate can change (ADR 0042). The cursor moves past every row the
    /// batch selected, so dead index entries of earlier batches are not
    /// walked again.
    async fn keys_step(
        &self,
        kinds: &[&str],
        kind: usize,
        after: Vec<u8>,
        done: u64,
        total: u64,
    ) -> Result<DeleteStep> {
        let mut deleted = vec![0; kinds.len()];
        let Some(name) = kinds.get(kind) else {
            return Ok(nothing_left(kinds));
        };
        let (last, n): (Option<Vec<u8>>, i64) = sqlx::query_as(
            "WITH batch AS MATERIALIZED ( \
               SELECT ctid, key FROM derived WHERE kind = $1 AND key > $2 ORDER BY key LIMIT $3), \
             gone AS (DELETE FROM derived \
               WHERE ctid = ANY(ARRAY(SELECT ctid FROM batch)) RETURNING 1) \
             SELECT (SELECT key FROM batch ORDER BY key DESC LIMIT 1), (SELECT count(*) FROM gone)",
        )
        .bind(name)
        .bind(after)
        .bind(KEY_BATCH)
        .fetch_one(&self.pool)
        .await
        .map_err(storage)?;
        deleted[kind] = n as u64;
        let done = done + n as u64;
        let next = match last {
            Some(key) => Some(Cursor::Keys {
                kind,
                after: key,
                done,
                total,
            }),
            None if kind + 1 < kinds.len() => Some(Cursor::Keys {
                kind: kind + 1,
                after: Vec::new(),
                done,
                total,
            }),
            None => None,
        };
        Ok(DeleteStep {
            deleted,
            next: next.map(|c| c.encode()),
            done,
            total: total.max(done),
            unit: "rows",
        })
    }
}

#[async_trait]
impl MetaStore for PostgresMetaStore {
    async fn put_changeset(&self, cs: &Changeset) -> Result<ChangesetId> {
        let id = cs.id();
        let data = self.encode(cs).await?;
        let q = if self.has_stored_at {
            sqlx::query(
                "INSERT INTO changesets (id, data, stored_at) VALUES ($1, $2, $3) \
                 ON CONFLICT DO NOTHING",
            )
            .bind(id.0.as_bytes().as_slice())
            .bind(data)
            .bind(crate::stored_now())
        } else {
            sqlx::query("INSERT INTO changesets (id, data) VALUES ($1, $2) ON CONFLICT DO NOTHING")
                .bind(id.0.as_bytes().as_slice())
                .bind(data)
        };
        q.execute(&self.pool).await.map_err(storage)?;
        Ok(id)
    }

    /// One `= ANY($1)` query whatever the number of ids; rows from before
    /// migration 0007 read as stored when it was applied.
    async fn changesets_stored_at(&self, ids: &[ChangesetId]) -> Result<Vec<Option<u64>>> {
        if !self.has_stored_at {
            return Err(leep_core::Error::Invalid(
                "the schema predates changesets.stored_at (v7); run `leep-server migrate` first"
                    .into(),
            ));
        }
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let keys: Vec<&[u8]> = unique(ids.iter().map(|i| i.0.as_bytes().as_slice()));
        let rows = sqlx::query(
            "SELECT id, CASE WHEN stored_at >= 0 THEN stored_at ELSE (SELECT applied_at \
             FROM schema_migrations WHERE version = $2) END FROM changesets WHERE id = ANY($1)",
        )
        .bind(keys)
        .bind(crate::STORED_AT_SCHEMA_VERSION)
        .fetch_all(&self.pool)
        .await
        .map_err(storage)?;
        crate::stored_at_in_order(ids, rows.iter().map(|r| (r.get(0), r.get(1))))
    }

    async fn get_changeset(&self, id: &ChangesetId) -> Result<Option<Changeset>> {
        let row = sqlx::query("SELECT data FROM changesets WHERE id = $1")
            .bind(id.0.as_bytes().as_slice())
            .fetch_optional(&self.pool)
            .await
            .map_err(storage)?;
        let Some(row) = row else { return Ok(None) };
        let data: Vec<u8> = row.get(0);
        let id_bytes = id.0.as_bytes().to_vec();
        Ok(self
            .decode_rows(std::slice::from_ref(id), vec![(id_bytes, data)])
            .await?
            .pop()
            .flatten())
    }

    /// One `= ANY($1)` query whatever the number of ids.
    async fn get_changesets(&self, ids: &[ChangesetId]) -> Result<Vec<Option<Changeset>>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let keys: Vec<&[u8]> = unique(ids.iter().map(|i| i.0.as_bytes().as_slice()));
        let rows = sqlx::query("SELECT id, data FROM changesets WHERE id = ANY($1)")
            .bind(keys)
            .fetch_all(&self.pool)
            .await
            .map_err(storage)?;
        self.decode_rows(ids, rows.iter().map(|r| (r.get(0), r.get(1))).collect())
            .await
    }

    /// An empty `after` sorts below every 32-byte id, so `None` needs no
    /// separate query.
    async fn list_changesets(
        &self,
        after: Option<ChangesetId>,
        limit: usize,
    ) -> Result<Vec<ChangesetId>> {
        let after = after.map_or_else(Vec::new, |i| i.0 .0.to_vec());
        let rows = sqlx::query("SELECT id FROM changesets WHERE id > $1 ORDER BY id LIMIT $2")
            .bind(after)
            .bind(i64::try_from(limit).unwrap_or(i64::MAX))
            .fetch_all(&self.pool)
            .await
            .map_err(storage)?;
        rows.into_iter()
            .map(|r| to_id(&r.get::<Vec<u8>, _>(0)))
            .collect()
    }

    /// One bounded range scan of the primary key.
    async fn changesets_with_prefix(
        &self,
        prefix: &IdPrefix,
        limit: usize,
    ) -> Result<Vec<ChangesetId>> {
        let rows = sqlx::query(
            "SELECT id FROM changesets WHERE id >= $1 AND id <= $2 ORDER BY id LIMIT $3",
        )
        .bind(prefix.lower().0 .0.to_vec())
        .bind(prefix.upper().0 .0.to_vec())
        .bind(i64::try_from(limit).unwrap_or(i64::MAX))
        .fetch_all(&self.pool)
        .await
        .map_err(storage)?;
        rows.into_iter()
            .map(|r| to_id(&r.get::<Vec<u8>, _>(0)))
            .collect()
    }

    async fn get_ref(&self, name: &str) -> Result<Option<ChangesetId>> {
        let row = sqlx::query("SELECT target FROM refs WHERE name = $1")
            .bind(name)
            .fetch_optional(&self.pool)
            .await
            .map_err(storage)?;
        row.map(|r| to_id(&r.get::<Vec<u8>, _>(0))).transpose()
    }

    async fn cas_ref_logged(
        &self,
        name: &str,
        expected: Option<ChangesetId>,
        new: Option<ChangesetId>,
        ctx: &RefUpdateContext,
    ) -> Result<bool> {
        check_land_key(new, ctx)?;
        if expected == new {
            return Ok(self.get_ref(name).await? == expected);
        }
        let exp = expected.map(|i| i.0 .0.to_vec());
        let new = new.map(|i| i.0 .0.to_vec());
        let (land_id, land_changeset) = land_columns(ctx);
        let mut tx = self.pool.begin().await.map_err(storage)?;
        let q = match (&exp, &new) {
            (None, Some(n)) => sqlx::query(
                "INSERT INTO refs (name, target) VALUES ($1, $2) ON CONFLICT DO NOTHING",
            )
            .bind(name)
            .bind(n),
            (Some(e), Some(n)) => {
                sqlx::query("UPDATE refs SET target = $1 WHERE name = $2 AND target = $3")
                    .bind(n)
                    .bind(name)
                    .bind(e)
            }
            (Some(e), None) => sqlx::query("DELETE FROM refs WHERE name = $1 AND target = $2")
                .bind(name)
                .bind(e),
            (None, None) => unreachable!("expected == new returned above"),
        };
        if q.execute(&mut *tx).await.map_err(storage)?.rows_affected() != 1 {
            tx.rollback().await.map_err(storage)?;
            return Ok(false);
        }
        sqlx::query("SELECT pg_advisory_xact_lock($1)")
            .bind(LOG_LOCK)
            .execute(&mut *tx)
            .await
            .map_err(storage)?;
        // The land columns only when used: a move without a land id writes
        // the row a v4 schema expects.
        let sql = if ctx.land.is_some() {
            "INSERT INTO ref_log (name, old_target, new_target, time, actor, operation, message, \
             land_id, land_changeset) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)"
        } else {
            "INSERT INTO ref_log (name, old_target, new_target, time, actor, operation, message) \
             VALUES ($1, $2, $3, $4, $5, $6, $7)"
        };
        let mut q = sqlx::query(sql)
            .bind(name)
            .bind(exp)
            .bind(new)
            .bind(unix_now())
            .bind(ctx.actor.as_deref())
            .bind(&ctx.operation)
            .bind(ctx.message.as_deref());
        if ctx.land.is_some() {
            q = q.bind(land_id).bind(land_changeset);
        }
        q.execute(&mut *tx)
            .await
            .map_err(|e| log_insert_error(name, e))?;
        tx.commit().await.map_err(storage)?;
        Ok(true)
    }

    fn supports_leases(&self) -> bool {
        true
    }

    async fn lease(
        &self,
        key: &leep_core::LeaseKey,
        timing: leep_core::LeaseTiming,
    ) -> Result<Option<leep_core::Lease>> {
        crate::leases::pg_get(&self.lease_pool, key, timing).await
    }

    async fn acquire_lease(
        &self,
        claim: &leep_core::LeaseClaim<'_>,
        timing: leep_core::LeaseTiming,
    ) -> Result<leep_core::Lease> {
        crate::leases::pg_acquire(&self.lease_pool, claim, timing).await
    }

    async fn renew_leases(
        &self,
        holder: &str,
        held: &[(leep_core::LeaseKey, u64)],
        timing: leep_core::LeaseTiming,
    ) -> Result<Vec<Option<std::time::Duration>>> {
        crate::leases::pg_renew(&self.lease_pool, holder, held, timing).await
    }

    async fn release_lease(
        &self,
        key: &leep_core::LeaseKey,
        holder: &str,
        epoch: u64,
    ) -> Result<bool> {
        crate::leases::pg_release(&self.lease_pool, key, holder, epoch).await
    }

    async fn cas_ref_logged_chain(
        &self,
        name: &str,
        expected: Option<ChangesetId>,
        steps: &[RefStep],
    ) -> Result<usize> {
        check_ref_chain(expected, steps)?;
        let exp = expected.map(|i| i.0 .0.to_vec());
        let last = steps.last().and_then(|s| s.new).map(|i| i.0 .0.to_vec());
        // The ref update is the transaction's first statement, as in
        // `cas_ref_logged`.
        let mut tx = self.pool.begin().await.map_err(storage)?;
        let q = match (&exp, &last) {
            (None, Some(n)) => sqlx::query(
                "INSERT INTO refs (name, target) VALUES ($1, $2) ON CONFLICT DO NOTHING",
            )
            .bind(name)
            .bind(n),
            (Some(e), Some(n)) => {
                sqlx::query("UPDATE refs SET target = $1 WHERE name = $2 AND target = $3")
                    .bind(n)
                    .bind(name)
                    .bind(e)
            }
            (Some(e), None) => sqlx::query("DELETE FROM refs WHERE name = $1 AND target = $2")
                .bind(name)
                .bind(e),
            (None, None) => unreachable!("check_ref_chain refuses a chain back to the start"),
        };
        if q.execute(&mut *tx).await.map_err(storage)?.rows_affected() != 1 {
            tx.rollback().await.map_err(storage)?;
            return Ok(0);
        }
        sqlx::query("SELECT pg_advisory_xact_lock($1)")
            .bind(LOG_LOCK)
            .execute(&mut *tx)
            .await
            .map_err(storage)?;
        // One statement; rows take increasing `seq` in the order given.
        let time = unix_now();
        let mut old = exp;
        // The land columns only when used, as in `cas_ref_logged`.
        let with_land = steps.iter().any(|s| s.ctx.land.is_some());
        let mut insert = sqlx::QueryBuilder::new(if with_land {
            "INSERT INTO ref_log (name, old_target, new_target, time, actor, operation, message, \
             land_id, land_changeset) "
        } else {
            "INSERT INTO ref_log (name, old_target, new_target, time, actor, operation, message) "
        });
        insert.push_values(steps, |mut row, step| {
            let new = step.new.map(|i| i.0 .0.to_vec());
            row.push_bind(name)
                .push_bind(std::mem::replace(&mut old, new.clone()))
                .push_bind(new)
                .push_bind(time)
                .push_bind(step.ctx.actor.as_deref())
                .push_bind(step.ctx.operation.as_str())
                .push_bind(step.ctx.message.as_deref());
            if with_land {
                let (land_id, land_changeset) = land_columns(&step.ctx);
                row.push_bind(land_id).push_bind(land_changeset);
            }
        });
        insert
            .build()
            .execute(&mut *tx)
            .await
            .map_err(|e| log_insert_error(name, e))?;
        tx.commit().await.map_err(storage)?;
        Ok(steps.len())
    }

    async fn find_lands(&self, name: &str, ids: &[LandId]) -> Result<Vec<Option<LandRecord>>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let keys: Vec<&[u8]> = ids.iter().map(|i| i.as_bytes()).collect();
        let rows: Vec<LandRow> = sqlx::query_as(
            "SELECT land_id, land_changeset, new_target, seq FROM ref_log \
             WHERE name = $1 AND land_id = ANY($2) AND land_id IS NOT NULL",
        )
        .bind(name)
        .bind(&keys)
        .fetch_all(&self.pool)
        .await
        .map_err(storage)?;
        lands_in_order(ids, rows)
    }

    async fn ref_log(
        &self,
        name: Option<&str>,
        before_seq: Option<u64>,
        limit: usize,
    ) -> Result<Vec<RefLogEntry>> {
        let before = before_seq.map(|s| i64::try_from(s).unwrap_or(i64::MAX));
        let rows: Vec<LogRow> = sqlx::query_as(
            "SELECT seq, name, old_target, new_target, time, actor, operation, message, \
             land_id, land_changeset FROM ref_log WHERE ($1::text IS NULL OR name = $1) \
             AND ($2::bigint IS NULL OR seq < $2) ORDER BY seq DESC LIMIT $3",
        )
        .bind(name)
        .bind(before)
        .bind(i64::try_from(limit).unwrap_or(i64::MAX))
        .fetch_all(&self.pool)
        .await
        .map_err(storage)?;
        rows.into_iter().map(log_entry).collect()
    }

    async fn list_refs(&self) -> Result<BTreeMap<String, ChangesetId>> {
        let rows = sqlx::query("SELECT name, target FROM refs")
            .fetch_all(&self.pool)
            .await
            .map_err(storage)?;
        rows.into_iter()
            .map(|r| Ok((r.get::<String, _>(0), to_id(&r.get::<Vec<u8>, _>(1))?)))
            .collect()
    }

    async fn get_derived(&self, kind: &str, key: &Hash) -> Result<Option<Bytes>> {
        let row = sqlx::query("SELECT value FROM derived WHERE kind = $1 AND key = $2")
            .bind(kind)
            .bind(key.as_bytes().as_slice())
            .fetch_optional(&self.pool)
            .await
            .map_err(storage)?;
        Ok(row.map(|r| Bytes::from(r.get::<Vec<u8>, _>(0))))
    }

    /// One `= ANY($2)` query whatever the number of keys. The kind goes in
    /// through a sub-select so stale statistics can't steer the plan: a kind
    /// that grew since the last `ANALYZE` is estimated at a row or two, and
    /// the planner then scans every row of the kind instead of looking the
    /// keys up (2.3 s instead of 3 ms per lookup on Linux).
    async fn get_derived_many(&self, kind: &str, keys: &[Hash]) -> Result<Vec<Option<Bytes>>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let wanted: Vec<&[u8]> = unique(keys.iter().map(|k| k.as_bytes().as_slice()));
        let rows = sqlx::query(
            "SELECT key, value FROM derived WHERE kind = (SELECT $1::text) AND key = ANY($2)",
        )
        .bind(kind)
        .bind(wanted)
        .fetch_all(&self.pool)
        .await
        .map_err(storage)?;
        Ok(values_in_order(
            keys,
            rows.iter().map(|r| (r.get(0), r.get(1))),
        ))
    }

    /// An empty `after` sorts below every 32-byte key, so `None` needs no
    /// separate query; `(kind, key)` is the primary key, so this is a range scan.
    async fn list_derived(
        &self,
        kind: &str,
        after: Option<Hash>,
        limit: usize,
    ) -> Result<Vec<(Hash, Bytes)>> {
        let after = after.map_or_else(Vec::new, |h| h.0.to_vec());
        let rows = sqlx::query(
            "SELECT key, value FROM derived WHERE kind = $1 AND key > $2 ORDER BY key LIMIT $3",
        )
        .bind(kind)
        .bind(after)
        .bind(i64::try_from(limit).unwrap_or(i64::MAX))
        .fetch_all(&self.pool)
        .await
        .map_err(storage)?;
        rows.into_iter()
            .map(|r| {
                Ok((
                    to_hash(&r.get::<Vec<u8>, _>(0))?,
                    Bytes::from(r.get::<Vec<u8>, _>(1)),
                ))
            })
            .collect()
    }

    /// An index-only scan of the primary key: no value is read.
    async fn count_derived(&self, kind: &str) -> Result<u64> {
        let n: i64 = sqlx::query_scalar("SELECT count(*) FROM derived WHERE kind = $1")
            .bind(kind)
            .fetch_one(&self.pool)
            .await
            .map_err(storage)?;
        Ok(n as u64)
    }

    /// Up to [`EXACT_UP_TO`] rows are counted; past that, the planner's
    /// statistics give the kind's share of the table (ADR 0052). Counting
    /// a large kind reads its whole index: about 30 s a kind on all of
    /// Linux.
    async fn estimate_derived(&self, kind: &str) -> Result<RowCount> {
        let n: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM (SELECT 1 FROM derived WHERE kind = $1 LIMIT $2) t",
        )
        .bind(kind)
        .bind(EXACT_UP_TO)
        .fetch_one(&self.pool)
        .await
        .map_err(storage)?;
        if n < EXACT_UP_TO {
            return Ok(RowCount {
                rows: n as u64,
                exact: true,
            });
        }
        match self.planner_rows(kind).await? {
            Some(est) => Ok(RowCount {
                rows: est.max(EXACT_UP_TO as u64),
                exact: false,
            }),
            // Not analyzed since the kind grew: count.
            None => Ok(RowCount {
                rows: self.count_derived(kind).await?,
                exact: true,
            }),
        }
    }

    /// See [`crate::drop`]: the first step counts the rows and the table's
    /// pages and picks a walk by `ctid` range or key-order batches.
    async fn delete_derived_step(
        &self,
        kinds: &[&str],
        cursor: Option<&[u8]>,
    ) -> Result<DeleteStep> {
        let cursor = match cursor {
            Some(b) => Cursor::decode(b)?,
            None => {
                let rows: i64 =
                    sqlx::query_scalar("SELECT count(*) FROM derived WHERE kind = ANY($1)")
                        .bind(kinds)
                        .fetch_one(&self.pool)
                        .await
                        .map_err(storage)?;
                match drop::plan(rows as u64, self.derived_pages().await?) {
                    Some(c) => c,
                    None => return Ok(nothing_left(kinds)),
                }
            }
        };
        match cursor {
            Cursor::Walk { at, span } => self.walk_step(kinds, at, span).await,
            Cursor::Keys {
                kind,
                after,
                done,
                total,
            } => self.keys_step(kinds, kind, after, done, total).await,
        }
    }

    /// Every derived upsert here skips a row whose stored value is already
    /// the same bytes: derivation is deterministic, so a repeated or racing
    /// write would otherwise only add a dead row version and WAL (ADR 0058).
    async fn put_derived(&self, kind: &str, key: &Hash, value: Bytes) -> Result<()> {
        sqlx::query(
            "INSERT INTO derived (kind, key, value) VALUES ($1, $2, $3) \
             ON CONFLICT (kind, key) DO UPDATE SET value = excluded.value \
             WHERE derived.value IS DISTINCT FROM excluded.value",
        )
        .bind(kind)
        .bind(key.as_bytes().as_slice())
        .bind(value.as_ref())
        .execute(&self.pool)
        .await
        .map_err(storage)?;
        Ok(())
    }

    /// One `INSERT ... SELECT FROM UNNEST` statement, so the batch is atomic.
    /// Rows go in `(kind, key)` order: every batched write then locks rows in
    /// one global order, so two batches sharing keys (claims for the same
    /// blob, say) wait for each other instead of deadlocking.
    async fn put_derived_many(&self, rows: &[DerivedRow]) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let (kinds, keys, values) = derived_columns(rows);
        sqlx::query(
            "INSERT INTO derived (kind, key, value) \
             SELECT * FROM UNNEST($1::text[], $2::bytea[], $3::bytea[]) ORDER BY 1, 2 \
             ON CONFLICT (kind, key) DO UPDATE SET value = excluded.value \
             WHERE derived.value IS DISTINCT FROM excluded.value",
        )
        .bind(kinds)
        .bind(keys)
        .bind(values)
        .execute(&self.pool)
        .await
        .map_err(storage)?;
        Ok(())
    }

    /// One statement: the changeset insert is a data-modifying `WITH`, which
    /// Postgres runs to completion even though nothing reads it, so the
    /// changeset and its rows commit together or not at all. Rows are ordered
    /// as in [`put_derived_many`](Self::put_derived_many); the changeset row
    /// is a key no other statement locks along with derived rows.
    async fn put_changeset_with_derived(
        &self,
        cs: &Changeset,
        rows: &[DerivedRow],
    ) -> Result<ChangesetId> {
        let id = cs.id();
        let data = self.encode(cs).await?;
        let (kinds, keys, values) = derived_columns(rows);
        let insert = if self.has_stored_at {
            "INSERT INTO changesets (id, data, stored_at) VALUES ($1, $2, $6)"
        } else {
            "INSERT INTO changesets (id, data) VALUES ($1, $2)"
        };
        let sql = format!(
            "WITH c AS ({insert} ON CONFLICT DO NOTHING) \
             INSERT INTO derived (kind, key, value) \
             SELECT * FROM UNNEST($3::text[], $4::bytea[], $5::bytea[]) ORDER BY 1, 2 \
             ON CONFLICT (kind, key) DO UPDATE SET value = excluded.value \
             WHERE derived.value IS DISTINCT FROM excluded.value"
        );
        let mut q = sqlx::query(AssertSqlSafe(sql))
            .bind(id.0.as_bytes().as_slice())
            .bind(data)
            .bind(kinds)
            .bind(keys)
            .bind(values);
        if self.has_stored_at {
            q = q.bind(crate::stored_now());
        }
        q.execute(&self.pool).await.map_err(storage)?;
        Ok(id)
    }

    /// One statement, as for `put_changeset_with_derived`, with the
    /// changesets in id order so concurrent batches lock them in one order.
    async fn put_changesets_with_derived(
        &self,
        changesets: &[Changeset],
        rows: &[DerivedRow],
    ) -> Result<()> {
        if changesets.is_empty() && rows.is_empty() {
            return Ok(());
        }
        let mut encoded: Vec<(ChangesetId, Vec<u8>)> = Vec::with_capacity(changesets.len());
        for c in changesets {
            encoded.push((c.id(), self.encode(c).await?));
        }
        let ids: Vec<&[u8]> = encoded
            .iter()
            .map(|(id, _)| id.0.as_bytes().as_slice())
            .collect();
        let data: Vec<&[u8]> = encoded.iter().map(|(_, d)| d.as_slice()).collect();
        let (kinds, keys, values) = derived_columns(rows);
        let insert = if self.has_stored_at {
            "INSERT INTO changesets (id, data, stored_at) \
             SELECT i, d, $6 FROM UNNEST($1::bytea[], $2::bytea[]) AS u (i, d) ORDER BY 1"
        } else {
            "INSERT INTO changesets (id, data) \
             SELECT * FROM UNNEST($1::bytea[], $2::bytea[]) ORDER BY 1"
        };
        let sql = format!(
            "WITH c AS ({insert} ON CONFLICT DO NOTHING) \
             INSERT INTO derived (kind, key, value) \
             SELECT * FROM UNNEST($3::text[], $4::bytea[], $5::bytea[]) ORDER BY 1, 2 \
             ON CONFLICT (kind, key) DO UPDATE SET value = excluded.value \
             WHERE derived.value IS DISTINCT FROM excluded.value"
        );
        let mut q = sqlx::query(AssertSqlSafe(sql))
            .bind(ids)
            .bind(data)
            .bind(kinds)
            .bind(keys)
            .bind(values);
        if self.has_stored_at {
            q = q.bind(crate::stored_now());
        }
        q.execute(&self.pool).await.map_err(storage)?;
        Ok(())
    }

    async fn schema_version(&self) -> Result<Option<u64>> {
        let v: Option<i64> = sqlx::query_scalar("SELECT MAX(version) FROM schema_migrations")
            .fetch_one(&self.pool)
            .await
            .map_err(storage)?;
        Ok(Some(v.unwrap_or(0) as u64))
    }
}
