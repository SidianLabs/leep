use std::collections::{BTreeMap, BTreeSet};
use std::str::FromStr;

use async_trait::async_trait;
use bytes::Bytes;
use leep_core::prefix::IdPrefix;
use leep_core::store::unix_now;
use leep_core::{
    check_land_key, check_ref_chain, Changeset, ChangesetId, DeleteStep, DerivedRow, Hash, LandId,
    LandRecord, MetaStore, RefLogEntry, RefStep, RefUpdateContext, Result,
};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use sqlx::{Row, SqliteConnection, SqlitePool};

use crate::codec::Codec;
use crate::drop::{self, nothing_left, Cursor, KEY_BATCH};
use crate::migrate::{self, AppliedMigration, Migration, MigrationReport, Session};
use crate::{
    changesets_in_order, land_columns, lands_in_order, log_entry, log_insert_error, placeholders,
    storage, to_hash, to_id, unique, values_in_order, LandRow, LogRow, SchemaMode, BATCH_PARAMS,
};

/// The compiled migration list (ADR 0023). Append only.
pub const MIGRATIONS: &[Migration] = &[
    Migration {
        version: 1,
        name: "initial",
        sql: include_str!("../migrations/sqlite/0001_initial.sql"),
        satisfied_by: &["changesets", "refs", "derived", "ref_log", "ref_log_name"],
    },
    Migration {
        version: 2,
        name: "analyze_thresholds",
        sql: include_str!("../migrations/sqlite/0002_analyze_thresholds.sql"),
        satisfied_by: &[],
    },
    Migration {
        version: 3,
        name: "changeset_dicts",
        sql: include_str!("../migrations/sqlite/0003_changeset_dicts.sql"),
        satisfied_by: &[],
    },
    Migration {
        version: 4,
        name: "store_features",
        sql: include_str!("../migrations/sqlite/0004_store_features.sql"),
        satisfied_by: &[],
    },
    Migration {
        version: 5,
        name: "land_ids",
        sql: include_str!("../migrations/sqlite/0005_land_ids.sql"),
        satisfied_by: &[],
    },
    Migration {
        version: 6,
        name: "land_leases",
        sql: include_str!("../migrations/sqlite/0006_land_leases.sql"),
        satisfied_by: &[],
    },
];

pub struct SqliteMetaStore {
    pub(crate) pool: SqlitePool,
    /// Changeset dictionaries (ADR 0053).
    pub(crate) codec: Codec,
    /// Whether the schema has `changeset_dicts`: tests open stores with
    /// shorter migration lists.
    pub(crate) has_dicts: bool,
    /// Whether the schema has `store_features` (ADR 0055).
    pub(crate) has_features: bool,
}

impl SqliteMetaStore {
    /// Connect and migrate. `url` is e.g. `sqlite://path?mode=rwc` or
    /// `sqlite::memory:` (single connection so every query sees one DB).
    pub async fn connect(url: &str) -> Result<Self> {
        Self::connect_with(url, SchemaMode::Migrate).await
    }

    /// Connect; with [`SchemaMode::RequireCurrent`] refuse instead of
    /// migrating when the schema is not current.
    pub async fn connect_with(url: &str, mode: SchemaMode) -> Result<Self> {
        Self::connect_with_migrations(url, mode, MIGRATIONS).await
    }

    /// Like [`connect_with`](Self::connect_with) with an explicit migration
    /// list, so tests can inject steps.
    #[doc(hidden)]
    pub async fn connect_with_migrations(
        url: &str,
        mode: SchemaMode,
        migrations: &[Migration],
    ) -> Result<Self> {
        let pool = Self::pool(url).await?;
        let report = Self::run(&pool, migrations, mode == SchemaMode::Migrate).await?;
        if mode == SchemaMode::RequireCurrent {
            migrate::require_current(&report)?;
        }
        let store = Self::with_pool(pool, migrations);
        crate::features::check(&store.features().await?)?;
        store.load_dicts().await?;
        Ok(store)
    }

    /// Apply pending migrations, or with `dry_run` only report them.
    pub async fn migrate(url: &str, dry_run: bool) -> Result<MigrationReport> {
        Self::migrate_with(url, MIGRATIONS, dry_run).await
    }

    #[doc(hidden)]
    pub async fn migrate_with(
        url: &str,
        migrations: &[Migration],
        dry_run: bool,
    ) -> Result<MigrationReport> {
        let pool = Self::pool(url).await?;
        let r = Self::run(&pool, migrations, !dry_run).await;
        pool.close().await;
        r
    }

    /// A new database file at the current schema, through one connection in
    /// rollback-journal mode: a snapshot is written once, by one writer, and
    /// ends as a single file with no write-ahead log to carry along.
    pub(crate) async fn create_single(url: &str) -> Result<Self> {
        let opts = SqliteConnectOptions::from_str(url)
            .map_err(storage)?
            .journal_mode(SqliteJournalMode::Delete)
            .busy_timeout(std::time::Duration::from_secs(30));
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(opts)
            .await
            .map_err(storage)?;
        Self::run(&pool, MIGRATIONS, true).await?;
        Ok(Self::with_pool(pool, MIGRATIONS))
    }

    async fn pool(url: &str) -> Result<SqlitePool> {
        let opts = SqliteConnectOptions::from_str(url).map_err(storage)?;
        let memory = url.contains(":memory:") || url.contains("mode=memory");
        let (opts, max) = if memory {
            (opts, 1)
        } else {
            (
                opts.journal_mode(SqliteJournalMode::Wal)
                    .busy_timeout(BUSY_TIMEOUT),
                8,
            )
        };
        let mut pool = SqlitePoolOptions::new().max_connections(max);
        if memory {
            // Dropping the only connection would discard the database.
            pool = pool
                .min_connections(1)
                .idle_timeout(None)
                .max_lifetime(None);
        }
        // Switching a new file to WAL needs an exclusive lock, and SQLite can
        // report a busy database at once instead of waiting out the busy
        // timeout, so processes opening a fresh file together retry here.
        let deadline = std::time::Instant::now() + BUSY_TIMEOUT;
        loop {
            match pool.clone().connect_with(opts.clone()).await {
                Err(e) if is_busy(&e) && std::time::Instant::now() < deadline => {
                    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                }
                r => return r.map_err(storage),
            }
        }
    }

    /// One transaction. `BEGIN IMMEDIATE` takes the write lock first, so
    /// concurrent migrators queue (up to the busy timeout) and the later ones
    /// find the work done.
    async fn run(
        pool: &SqlitePool,
        migrations: &[Migration],
        write: bool,
    ) -> Result<MigrationReport> {
        let mut tx = if write {
            pool.begin_with("BEGIN IMMEDIATE").await
        } else {
            pool.begin().await
        }
        .map_err(storage)?;
        let report = migrate::drive(&mut SqliteSession(&mut tx), migrations, write).await?;
        tx.commit().await.map_err(storage)?;
        Ok(report)
    }
}

/// How long a writer waits for the database lock before giving up.
const BUSY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// `SQLITE_BUSY`: another connection holds a lock this one needs.
fn is_busy(e: &sqlx::Error) -> bool {
    matches!(e, sqlx::Error::Database(d) if d.code().as_deref() == Some("5"))
}

struct SqliteSession<'a>(&'a mut SqliteConnection);

#[async_trait]
impl Session for SqliteSession<'_> {
    async fn objects(&mut self) -> Result<BTreeSet<String>> {
        let rows = sqlx::query("SELECT name FROM sqlite_master WHERE type IN ('table', 'index')")
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
            "CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER PRIMARY KEY, \
             name TEXT NOT NULL, applied_at INTEGER NOT NULL, checksum TEXT NOT NULL)",
        )
        .execute(&mut *self.0)
        .await
        .map_err(storage)?;
        Ok(())
    }

    async fn execute(&mut self, sql: &'static str) -> Result<()> {
        use sqlx::Executor;
        // A bare string takes the multi-statement path.
        self.0.execute(sql).await.map(|_| ()).map_err(storage)
    }

    async fn record(&mut self, m: &AppliedMigration) -> Result<()> {
        sqlx::query(
            "INSERT INTO schema_migrations (version, name, applied_at, checksum) \
             VALUES (?, ?, ?, ?)",
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

impl SqliteMetaStore {
    fn with_pool(pool: SqlitePool, migrations: &[Migration]) -> Self {
        Self {
            pool,
            codec: Codec::default(),
            has_dicts: migrations
                .iter()
                .any(|m| m.version >= crate::compact::DICTS_SCHEMA_VERSION),
            has_features: migrations
                .iter()
                .any(|m| m.version >= crate::features::FEATURES_SCHEMA_VERSION),
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
            "INSERT INTO store_features (name, added_at) VALUES (?, ?) ON CONFLICT DO NOTHING",
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
            let data: Vec<u8> = sqlx::query_scalar("SELECT data FROM changeset_dicts WHERE id = ?")
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

    /// One past the largest `rowid` in `derived`; a walk ends there.
    async fn rowid_end(&self) -> Result<u64> {
        let n: Option<i64> = sqlx::query_scalar("SELECT max(rowid) FROM derived")
            .fetch_one(&self.pool)
            .await
            .map_err(storage)?;
        Ok(n.map_or(0, |n| n as u64 + 1))
    }

    /// Delete the rows of `kinds` with `rowid` in `[at, at + span)`. `+kind`
    /// keeps SQLite on the `rowid` range instead of the key index.
    async fn walk_step(&self, kinds: &[&str], at: u64, span: u64) -> Result<DeleteStep> {
        let rowids = self.rowid_end().await?;
        let mut deleted = vec![0; kinds.len()];
        if at >= rowids {
            return Ok(DeleteStep {
                deleted,
                next: None,
                done: rowids,
                total: rowids,
                unit: "rowids",
            });
        }
        let end = (at + span).min(i64::MAX as u64);
        let started = std::time::Instant::now();
        let sql = format!(
            "DELETE FROM derived WHERE rowid >= ? AND rowid < ? AND +kind IN ({}) RETURNING kind",
            placeholders(kinds.len())
        );
        let mut q = sqlx::query_scalar::<_, String>(&sql)
            .bind(at as i64)
            .bind(end as i64);
        for k in kinds {
            q = q.bind(*k);
        }
        let gone = q.fetch_all(&self.pool).await.map_err(storage)?;
        drop::tally(kinds, &mut deleted, gone.into_iter().map(|k| (k, 1)));
        let next = Cursor::Walk {
            at: end,
            span: drop::next_span(span, started.elapsed()),
        };
        Ok(DeleteStep {
            deleted,
            next: Some(next.encode()),
            done: end.min(rowids),
            total: rowids,
            unit: "rowids",
        })
    }

    /// Delete the next [`KEY_BATCH`] rows of `kinds[kind]` after `after`.
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
        let keys: Vec<Vec<u8>> = sqlx::query_scalar(
            "DELETE FROM derived WHERE kind = ?1 AND key IN \
             (SELECT key FROM derived WHERE kind = ?1 AND key > ?2 ORDER BY key LIMIT ?3) \
             RETURNING key",
        )
        .bind(name)
        .bind(after)
        .bind(KEY_BATCH)
        .fetch_all(&self.pool)
        .await
        .map_err(storage)?;
        deleted[kind] = keys.len() as u64;
        let done = done + keys.len() as u64;
        let next = match keys.into_iter().max() {
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
impl MetaStore for SqliteMetaStore {
    async fn put_changeset(&self, cs: &Changeset) -> Result<ChangesetId> {
        let id = cs.id();
        let data = self.encode(cs).await?;
        sqlx::query("INSERT INTO changesets (id, data) VALUES (?, ?) ON CONFLICT DO NOTHING")
            .bind(id.0.as_bytes().as_slice())
            .bind(data)
            .execute(&self.pool)
            .await
            .map_err(storage)?;
        Ok(id)
    }

    async fn get_changeset(&self, id: &ChangesetId) -> Result<Option<Changeset>> {
        let row = sqlx::query("SELECT data FROM changesets WHERE id = ?")
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

    /// One `IN (...)` query per [`BATCH_PARAMS`] distinct ids.
    async fn get_changesets(&self, ids: &[ChangesetId]) -> Result<Vec<Option<Changeset>>> {
        let keys: Vec<&[u8]> = unique(ids.iter().map(|i| i.0.as_bytes().as_slice()));
        let mut found: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(keys.len());
        for chunk in keys.chunks(BATCH_PARAMS) {
            let sql = format!(
                "SELECT id, data FROM changesets WHERE id IN ({})",
                placeholders(chunk.len())
            );
            let mut q = sqlx::query(&sql);
            for k in chunk {
                q = q.bind(*k);
            }
            let rows = q.fetch_all(&self.pool).await.map_err(storage)?;
            found.extend(rows.iter().map(|r| (r.get(0), r.get(1))));
        }
        self.decode_rows(ids, found).await
    }

    /// An empty `after` sorts below every 32-byte id, so `None` needs no
    /// separate query.
    async fn list_changesets(
        &self,
        after: Option<ChangesetId>,
        limit: usize,
    ) -> Result<Vec<ChangesetId>> {
        let after = after.map_or_else(Vec::new, |i| i.0 .0.to_vec());
        let rows = sqlx::query("SELECT id FROM changesets WHERE id > ? ORDER BY id LIMIT ?")
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
        let rows =
            sqlx::query("SELECT id FROM changesets WHERE id >= ? AND id <= ? ORDER BY id LIMIT ?")
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
        let row = sqlx::query("SELECT target FROM refs WHERE name = ?")
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
        // The ref update is the transaction's first statement, so SQLite takes
        // the write lock up front and never has to upgrade a read lock.
        let mut tx = self.pool.begin().await.map_err(storage)?;
        let q = match (&exp, &new) {
            (None, Some(n)) => {
                sqlx::query("INSERT INTO refs (name, target) VALUES (?, ?) ON CONFLICT DO NOTHING")
                    .bind(name)
                    .bind(n)
            }
            (Some(e), Some(n)) => {
                sqlx::query("UPDATE refs SET target = ? WHERE name = ? AND target = ?")
                    .bind(n)
                    .bind(name)
                    .bind(e)
            }
            (Some(e), None) => sqlx::query("DELETE FROM refs WHERE name = ? AND target = ?")
                .bind(name)
                .bind(e),
            (None, None) => unreachable!("expected == new returned above"),
        };
        if q.execute(&mut *tx).await.map_err(storage)?.rows_affected() != 1 {
            tx.rollback().await.map_err(storage)?;
            return Ok(false);
        }
        // The land columns only when used: a move without a land id writes
        // the row a v4 schema expects.
        let sql = if ctx.land.is_some() {
            "INSERT INTO ref_log (name, old_target, new_target, time, actor, operation, message, \
             land_id, land_changeset) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
        } else {
            "INSERT INTO ref_log (name, old_target, new_target, time, actor, operation, message) \
             VALUES (?, ?, ?, ?, ?, ?, ?)"
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

    fn supports_land_leases(&self) -> bool {
        true
    }

    async fn land_lease(
        &self,
        ref_name: &str,
        timing: leep_core::LeaseTiming,
    ) -> Result<Option<leep_core::LandLease>> {
        crate::leases::sqlite_get(&self.pool, ref_name, timing).await
    }

    async fn acquire_land_lease(
        &self,
        claim: &leep_core::LeaseClaim<'_>,
        timing: leep_core::LeaseTiming,
    ) -> Result<leep_core::LandLease> {
        crate::leases::sqlite_acquire(&self.pool, claim, timing).await
    }

    async fn renew_land_leases(
        &self,
        holder: &str,
        held: &[(String, u64)],
        timing: leep_core::LeaseTiming,
    ) -> Result<Vec<Option<std::time::Duration>>> {
        crate::leases::sqlite_renew(&self.pool, holder, held, timing).await
    }

    async fn release_land_lease(&self, ref_name: &str, holder: &str, epoch: u64) -> Result<bool> {
        crate::leases::sqlite_release(&self.pool, ref_name, holder, epoch).await
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
            (None, Some(n)) => {
                sqlx::query("INSERT INTO refs (name, target) VALUES (?, ?) ON CONFLICT DO NOTHING")
                    .bind(name)
                    .bind(n)
            }
            (Some(e), Some(n)) => {
                sqlx::query("UPDATE refs SET target = ? WHERE name = ? AND target = ?")
                    .bind(n)
                    .bind(name)
                    .bind(e)
            }
            (Some(e), None) => sqlx::query("DELETE FROM refs WHERE name = ? AND target = ?")
                .bind(name)
                .bind(e),
            (None, None) => unreachable!("check_ref_chain refuses a chain back to the start"),
        };
        if q.execute(&mut *tx).await.map_err(storage)?.rows_affected() != 1 {
            tx.rollback().await.map_err(storage)?;
            return Ok(0);
        }
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
        let mut rows: Vec<LandRow> = Vec::new();
        for chunk in ids.chunks(BATCH_PARAMS - 1) {
            let sql = format!(
                "SELECT land_id, land_changeset, new_target, seq FROM ref_log \
                 WHERE name = ? AND land_id IN ({}) AND land_id IS NOT NULL",
                placeholders(chunk.len())
            );
            let mut q = sqlx::query_as(&sql).bind(name);
            for id in chunk {
                q = q.bind(id.as_bytes());
            }
            rows.extend(q.fetch_all(&self.pool).await.map_err(storage)?);
        }
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
            "SELECT seq, name, old_target, new_target, time, actor, operation, message \
             FROM ref_log WHERE (? IS NULL OR name = ?) AND (? IS NULL OR seq < ?) \
             ORDER BY seq DESC LIMIT ?",
        )
        .bind(name)
        .bind(name)
        .bind(before)
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
        let row = sqlx::query("SELECT value FROM derived WHERE kind = ? AND key = ?")
            .bind(kind)
            .bind(key.as_bytes().as_slice())
            .fetch_optional(&self.pool)
            .await
            .map_err(storage)?;
        Ok(row.map(|r| Bytes::from(r.get::<Vec<u8>, _>(0))))
    }

    /// One `IN (...)` query per [`BATCH_PARAMS`] distinct keys.
    async fn get_derived_many(&self, kind: &str, keys: &[Hash]) -> Result<Vec<Option<Bytes>>> {
        let wanted: Vec<&[u8]> = unique(keys.iter().map(|k| k.as_bytes().as_slice()));
        let mut found: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(wanted.len());
        for chunk in wanted.chunks(BATCH_PARAMS) {
            let sql = format!(
                "SELECT key, value FROM derived WHERE kind = ? AND key IN ({})",
                placeholders(chunk.len())
            );
            let mut q = sqlx::query(&sql).bind(kind);
            for k in chunk {
                q = q.bind(*k);
            }
            let rows = q.fetch_all(&self.pool).await.map_err(storage)?;
            found.extend(rows.iter().map(|r| (r.get(0), r.get(1))));
        }
        Ok(values_in_order(keys, found.into_iter()))
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
            "SELECT key, value FROM derived WHERE kind = ? AND key > ? ORDER BY key LIMIT ?",
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

    async fn count_derived(&self, kind: &str) -> Result<u64> {
        let n: i64 = sqlx::query_scalar("SELECT count(*) FROM derived WHERE kind = ?")
            .bind(kind)
            .fetch_one(&self.pool)
            .await
            .map_err(storage)?;
        Ok(n as u64)
    }

    /// As in Postgres (see [`crate::drop`]), walking `rowid` ranges.
    async fn delete_derived_step(
        &self,
        kinds: &[&str],
        cursor: Option<&[u8]>,
    ) -> Result<DeleteStep> {
        let cursor = match cursor {
            Some(b) => Cursor::decode(b)?,
            None => {
                let sql = format!(
                    "SELECT count(*) FROM derived WHERE kind IN ({})",
                    placeholders(kinds.len())
                );
                let mut q = sqlx::query_scalar::<_, i64>(&sql);
                for k in kinds {
                    q = q.bind(*k);
                }
                let rows = q.fetch_one(&self.pool).await.map_err(storage)?;
                match drop::plan(rows as u64, self.rowid_end().await?) {
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

    async fn put_derived(&self, kind: &str, key: &Hash, value: Bytes) -> Result<()> {
        sqlx::query(
            "INSERT INTO derived (kind, key, value) VALUES (?, ?, ?) \
             ON CONFLICT (kind, key) DO UPDATE SET value = excluded.value",
        )
        .bind(kind)
        .bind(key.as_bytes().as_slice())
        .bind(value.as_ref())
        .execute(&self.pool)
        .await
        .map_err(storage)?;
        Ok(())
    }

    /// One transaction, so one commit (and one sync) for the whole batch.
    async fn put_derived_many(&self, rows: &[DerivedRow]) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let mut tx = self.pool.begin().await.map_err(storage)?;
        put_rows(&mut tx, rows).await?;
        tx.commit().await.map_err(storage)
    }

    /// One transaction whose first statement is a write, so SQLite takes the
    /// write lock up front.
    async fn put_changeset_with_derived(
        &self,
        cs: &Changeset,
        rows: &[DerivedRow],
    ) -> Result<ChangesetId> {
        let id = cs.id();
        let data = self.encode(cs).await?;
        let mut tx = self.pool.begin().await.map_err(storage)?;
        sqlx::query("INSERT INTO changesets (id, data) VALUES (?, ?) ON CONFLICT DO NOTHING")
            .bind(id.0.as_bytes().as_slice())
            .bind(data)
            .execute(&mut *tx)
            .await
            .map_err(storage)?;
        put_rows(&mut tx, rows).await?;
        tx.commit().await.map_err(storage)?;
        Ok(id)
    }

    /// One transaction, as for `put_changeset_with_derived`.
    async fn put_changesets_with_derived(
        &self,
        changesets: &[Changeset],
        rows: &[DerivedRow],
    ) -> Result<()> {
        if changesets.is_empty() && rows.is_empty() {
            return Ok(());
        }
        let mut encoded = Vec::with_capacity(changesets.len());
        for cs in changesets {
            encoded.push((cs.id(), self.encode(cs).await?));
        }
        let mut tx = self.pool.begin().await.map_err(storage)?;
        for (id, data) in &encoded {
            sqlx::query("INSERT INTO changesets (id, data) VALUES (?, ?) ON CONFLICT DO NOTHING")
                .bind(id.0.as_bytes().as_slice())
                .bind(data.as_slice())
                .execute(&mut *tx)
                .await
                .map_err(storage)?;
        }
        put_rows(&mut tx, rows).await?;
        tx.commit().await.map_err(storage)
    }

    async fn schema_version(&self) -> Result<Option<u64>> {
        let v: Option<i64> = sqlx::query_scalar("SELECT MAX(version) FROM schema_migrations")
            .fetch_one(&self.pool)
            .await
            .map_err(storage)?;
        Ok(Some(v.unwrap_or(0) as u64))
    }
}

/// Statements on one connection are local calls in SQLite, so the rows go one
/// statement each; the transaction around them is what costs.
async fn put_rows(conn: &mut SqliteConnection, rows: &[DerivedRow]) -> Result<()> {
    for r in rows {
        sqlx::query(
            "INSERT INTO derived (kind, key, value) VALUES (?, ?, ?) \
             ON CONFLICT (kind, key) DO UPDATE SET value = excluded.value",
        )
        .bind(&r.kind)
        .bind(r.key.as_bytes().as_slice())
        .bind(r.value.as_ref())
        .execute(&mut *conn)
        .await
        .map_err(storage)?;
    }
    Ok(())
}
