use std::collections::{BTreeMap, BTreeSet};
use std::str::FromStr;

use async_trait::async_trait;
use bytes::Bytes;
use leep_core::store::unix_now;
use leep_core::{Changeset, ChangesetId, Hash, MetaStore, RefLogEntry, RefUpdateContext, Result};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use sqlx::{Row, SqliteConnection, SqlitePool};

use crate::migrate::{self, AppliedMigration, Migration, MigrationReport, Session};
use crate::{decode_checked, log_entry, storage, to_hash, to_id, LogRow, SchemaMode, DELETE_BATCH};

/// The compiled migration list (ADR 0023). Append only.
pub const MIGRATIONS: &[Migration] = &[Migration {
    version: 1,
    name: "initial",
    sql: include_str!("../migrations/sqlite/0001_initial.sql"),
    satisfied_by: &["changesets", "refs", "derived", "ref_log", "ref_log_name"],
}];

pub struct SqliteMetaStore {
    pub(crate) pool: SqlitePool,
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
        Ok(Self { pool })
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
        Ok(Self { pool })
    }

    async fn pool(url: &str) -> Result<SqlitePool> {
        let opts = SqliteConnectOptions::from_str(url).map_err(storage)?;
        let memory = url.contains(":memory:") || url.contains("mode=memory");
        let (opts, max) = if memory {
            (opts, 1)
        } else {
            (
                opts.journal_mode(SqliteJournalMode::Wal)
                    .busy_timeout(std::time::Duration::from_secs(30)),
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
        pool.connect_with(opts).await.map_err(storage)
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

#[async_trait]
impl MetaStore for SqliteMetaStore {
    async fn put_changeset(&self, cs: &Changeset) -> Result<ChangesetId> {
        let id = cs.id();
        sqlx::query("INSERT INTO changesets (id, data) VALUES (?, ?) ON CONFLICT DO NOTHING")
            .bind(id.0.as_bytes().as_slice())
            .bind(cs.encode())
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
        row.map(|r| decode_checked(id, &r.get::<Vec<u8>, _>(0)))
            .transpose()
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
        if expected == new {
            return Ok(self.get_ref(name).await? == expected);
        }
        let exp = expected.map(|i| i.0 .0.to_vec());
        let new = new.map(|i| i.0 .0.to_vec());
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
        sqlx::query(
            "INSERT INTO ref_log (name, old_target, new_target, time, actor, operation, message) \
             VALUES (?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(name)
        .bind(exp)
        .bind(new)
        .bind(unix_now())
        .bind(ctx.actor.as_deref())
        .bind(&ctx.operation)
        .bind(ctx.message.as_deref())
        .execute(&mut *tx)
        .await
        .map_err(storage)?;
        tx.commit().await.map_err(storage)?;
        Ok(true)
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

    async fn delete_derived_kind(&self, kind: &str) -> Result<u64> {
        let mut total = 0;
        loop {
            let n = sqlx::query(
                "DELETE FROM derived WHERE kind = ? AND key IN \
                 (SELECT key FROM derived WHERE kind = ? LIMIT ?)",
            )
            .bind(kind)
            .bind(kind)
            .bind(DELETE_BATCH)
            .execute(&self.pool)
            .await
            .map_err(storage)?
            .rows_affected();
            total += n;
            if n == 0 {
                return Ok(total);
            }
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

    async fn schema_version(&self) -> Result<Option<u64>> {
        let v: Option<i64> = sqlx::query_scalar("SELECT MAX(version) FROM schema_migrations")
            .fetch_one(&self.pool)
            .await
            .map_err(storage)?;
        Ok(Some(v.unwrap_or(0) as u64))
    }
}
