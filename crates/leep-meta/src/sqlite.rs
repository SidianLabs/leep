use std::collections::BTreeMap;
use std::str::FromStr;

use async_trait::async_trait;
use bytes::Bytes;
use leep_core::{Changeset, ChangesetId, Hash, MetaStore, Result};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use sqlx::{Row, SqlitePool};

use crate::{decode_checked, storage, to_id};

const SCHEMA: &[&str] = &[
    "CREATE TABLE IF NOT EXISTS changesets (id BLOB PRIMARY KEY, data BLOB NOT NULL)",
    "CREATE TABLE IF NOT EXISTS refs (name TEXT PRIMARY KEY, target BLOB NOT NULL)",
    "CREATE TABLE IF NOT EXISTS derived (kind TEXT NOT NULL, key BLOB NOT NULL, \
     value BLOB NOT NULL, PRIMARY KEY (kind, key))",
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

    async fn cas_ref(
        &self,
        name: &str,
        expected: Option<ChangesetId>,
        new: Option<ChangesetId>,
    ) -> Result<bool> {
        let exp = expected.map(|i| i.0 .0.to_vec());
        let new = new.map(|i| i.0 .0.to_vec());
        let q = match (exp, new) {
            (None, None) => return Ok(self.get_ref(name).await?.is_none()),
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
        };
        let res = q.execute(&self.pool).await.map_err(storage)?;
        Ok(res.rows_affected() == 1)
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
