use std::collections::BTreeMap;
use std::str::FromStr;

use async_trait::async_trait;
use bytes::Bytes;
use leep_core::store::unix_now;
use leep_core::{Changeset, ChangesetId, Hash, MetaStore, RefLogEntry, RefUpdateContext, Result};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use sqlx::{Row, SqlitePool};

use crate::{decode_checked, log_entry, storage, to_hash, to_id, LogRow, DELETE_BATCH};

const SCHEMA: &[&str] = &[
    "CREATE TABLE IF NOT EXISTS changesets (id BLOB PRIMARY KEY, data BLOB NOT NULL)",
    "CREATE TABLE IF NOT EXISTS refs (name TEXT PRIMARY KEY, target BLOB NOT NULL)",
    "CREATE TABLE IF NOT EXISTS derived (kind TEXT NOT NULL, key BLOB NOT NULL, \
     value BLOB NOT NULL, PRIMARY KEY (kind, key))",
    // AUTOINCREMENT so a seq is never reused, even after rows are pruned.
    "CREATE TABLE IF NOT EXISTS ref_log (seq INTEGER PRIMARY KEY AUTOINCREMENT, \
     name TEXT NOT NULL, old_target BLOB, new_target BLOB, time INTEGER NOT NULL, \
     actor TEXT, operation TEXT NOT NULL, message TEXT)",
    "CREATE INDEX IF NOT EXISTS ref_log_name ON ref_log (name, seq)",
];

pub struct SqliteMetaStore {
    pub(crate) pool: SqlitePool,
}

impl SqliteMetaStore {
    /// Connect and migrate. `url` is e.g. `sqlite://path?mode=rwc` or
    /// `sqlite::memory:` (single connection so every query sees one DB).
    pub async fn connect(url: &str) -> Result<Self> {
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
        let pool = pool.connect_with(opts).await.map_err(storage)?;
        for sql in SCHEMA {
            sqlx::query(sql).execute(&pool).await.map_err(storage)?;
        }
        Ok(Self { pool })
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
}
