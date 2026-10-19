//! Compress stored changeset rows with a dictionary trained on the store's
//! own history (ADR 0052): `leep-server compact-changesets`.
//!
//! A row's bytes are an encoding of an immutable value, and every reader
//! checks what it decodes against the row's id, so rewriting a row in place
//! is safe beside running servers of this version: a reader sees the old
//! bytes or the new ones, and both decode to the same changeset.

use async_trait::async_trait;
use leep_core::{Error, Result};
use sqlx::Row;

use crate::codec::{self, Codec};
use crate::{storage, to_id, PostgresMetaStore, SchemaMode, SqliteMetaStore};

/// The schema version that adds `changeset_dicts`.
pub const DICTS_SCHEMA_VERSION: i64 = 3;

/// Fewer changesets than this are not worth a dictionary: zstd needs
/// samples to train on, and a small store saves little.
pub const MIN_TRAINING_ROWS: usize = 1000;

/// What [`compact_changesets`] does.
#[derive(Debug, Clone)]
pub struct CompactOptions {
    /// Largest dictionary to train, in bytes.
    pub dict_size: usize,
    /// Changesets to train on. Ids are hashes, so the first ones in id order
    /// are a uniform sample of history.
    pub samples: usize,
    /// Train a new dictionary even if one exists, and recompress every row
    /// that does not use it. Otherwise only plain rows are compressed.
    pub retrain: bool,
    /// Report what would change; write nothing.
    pub dry_run: bool,
    /// Rows read and rewritten per statement.
    pub batch: usize,
}

impl Default for CompactOptions {
    fn default() -> Self {
        CompactOptions {
            dict_size: 64 << 10,
            samples: 50_000,
            retrain: false,
            dry_run: false,
            batch: 500,
        }
    }
}

/// What a compaction did (or, with `dry_run`, would do).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CompactReport {
    /// The dictionary rows are compressed with; 0 when there is none.
    pub dictionary: u32,
    /// Whether this run trained it.
    pub trained: bool,
    /// Bytes of the trained dictionary.
    pub dictionary_bytes: u64,
    /// Changesets it was trained on.
    pub samples: u64,
    /// Changeset rows looked at.
    pub rows: u64,
    /// Rows whose bytes changed.
    pub rewritten: u64,
    /// `data` bytes of the rows looked at, before and after.
    pub bytes_before: u64,
    pub bytes_after: u64,
}

/// Compress the changeset rows of the store at `url`; `progress` sees the
/// running totals after each batch.
pub async fn compact_changesets(
    url: &str,
    mode: SchemaMode,
    opts: &CompactOptions,
    progress: &mut (dyn FnMut(&CompactReport) + Send),
) -> Result<CompactReport> {
    if url.starts_with("sqlite:") {
        let store = SqliteMetaStore::connect_with(url, mode).await?;
        let r = run(&store, opts, progress).await;
        store.pool.close().await;
        r
    } else if url.starts_with("postgres://") || url.starts_with("postgresql://") {
        let store = PostgresMetaStore::connect_with(url, mode).await?;
        let r = run(&store, opts, progress).await;
        store.pool.close().await;
        r
    } else if url.starts_with("mem://") {
        Err(Error::Invalid(
            "mem:// keeps changesets in memory, uncompressed".into(),
        ))
    } else {
        Err(Error::Invalid(format!("unsupported meta store url: {url}")))
    }
}

type Pair = (Vec<u8>, Vec<u8>);

/// The statements a compaction needs from a backend.
#[async_trait]
pub(crate) trait Rows: Sync {
    fn codec(&self) -> &Codec;
    fn has_dicts(&self) -> bool;
    async fn load_dicts(&self) -> Result<()>;
    async fn page(&self, after: &[u8], limit: usize) -> Result<Vec<Pair>>;
    async fn max_dict(&self) -> Result<i64>;
    /// Insert a dictionary; `false` if another run took the id first.
    async fn insert_dict(&self, id: i64, data: &[u8]) -> Result<bool>;
    async fn rewrite(&self, rows: &[Pair]) -> Result<()>;
}

pub(crate) async fn run(
    store: &dyn Rows,
    opts: &CompactOptions,
    progress: &mut (dyn FnMut(&CompactReport) + Send),
) -> Result<CompactReport> {
    if !store.has_dicts() {
        return Err(Error::Invalid(format!(
            "the schema predates changeset dictionaries (v{DICTS_SCHEMA_VERSION}); run `leep-server migrate` first"
        )));
    }
    store.load_dicts().await?;
    let mut report = CompactReport::default();
    // With dry_run, a trained dictionary lives only in this codec, which
    // then writes nothing but the report.
    let scratch;
    let mut codec = store.codec();
    if opts.retrain || codec.newest_id() == 0 {
        let samples = sample(store, opts.samples, opts.batch).await?;
        if samples.len() < MIN_TRAINING_ROWS {
            return Ok(report);
        }
        let id = store.max_dict().await? + 1;
        let id32 =
            u32::try_from(id).map_err(|_| Error::Storage("dictionary ids exhausted".into()))?;
        let dict = codec::train(&samples, opts.dict_size, id32)?;
        if opts.dry_run {
            scratch = Codec::default();
            scratch.add(vec![(id, dict.clone())])?;
            codec = &scratch;
        } else {
            if !store.insert_dict(id, &dict).await? {
                return Err(Error::Conflict(format!(
                    "changeset dictionary {id} was added by another compaction; run again"
                )));
            }
            store.load_dicts().await?;
        }
        report.trained = true;
        report.dictionary_bytes = dict.len() as u64;
        report.samples = samples.len() as u64;
    }
    report.dictionary = codec.newest_id();
    let target = report.dictionary;
    let mut after = Vec::new();
    loop {
        let page = store.page(&after, opts.batch).await?;
        let Some(last) = page.last() else { break };
        after = last.0.clone();
        if store
            .codec()
            .missing(page.iter().map(|r| r.1.as_slice()))
            .is_some()
        {
            store.load_dicts().await?;
        }
        let mut changed = Vec::new();
        for (id, data) in &page {
            report.rows += 1;
            report.bytes_before += data.len() as u64;
            let keep = Codec::uses(data, target)
                || (!opts.retrain && codec::frame_dict_id(data).is_some_and(|d| d != 0));
            if keep {
                report.bytes_after += data.len() as u64;
                continue;
            }
            // Read with every stored dictionary; write with the target, which
            // a dry run holds only in its own codec.
            let cs = store.codec().decode_checked(&to_id(id)?, data)?;
            let new = codec.encode(&cs);
            report.bytes_after += new.len() as u64;
            if new != *data {
                changed.push((id.clone(), new));
            }
        }
        report.rewritten += changed.len() as u64;
        if !opts.dry_run && !changed.is_empty() {
            store.rewrite(&changed).await?;
        }
        progress(&report);
    }
    Ok(report)
}

/// Canonical bytes of up to `n` changesets, the first in id order, each
/// checked against its id.
async fn sample(store: &dyn Rows, n: usize, batch: usize) -> Result<Vec<Vec<u8>>> {
    let mut out = Vec::new();
    let mut after = Vec::new();
    while out.len() < n {
        let page = store.page(&after, batch.min(n - out.len())).await?;
        let Some(last) = page.last() else { break };
        after = last.0.clone();
        for (id, data) in &page {
            out.push(store.codec().decode_checked(&to_id(id)?, data)?.encode());
        }
    }
    Ok(out)
}

#[async_trait]
impl Rows for PostgresMetaStore {
    fn codec(&self) -> &Codec {
        &self.codec
    }

    fn has_dicts(&self) -> bool {
        self.has_dicts
    }

    async fn load_dicts(&self) -> Result<()> {
        PostgresMetaStore::load_dicts(self).await
    }

    async fn page(&self, after: &[u8], limit: usize) -> Result<Vec<Pair>> {
        let rows =
            sqlx::query("SELECT id, data FROM changesets WHERE id > $1 ORDER BY id LIMIT $2")
                .bind(after)
                .bind(i64::try_from(limit).unwrap_or(i64::MAX))
                .fetch_all(&self.pool)
                .await
                .map_err(storage)?;
        Ok(rows.iter().map(|r| (r.get(0), r.get(1))).collect())
    }

    async fn max_dict(&self) -> Result<i64> {
        sqlx::query_scalar("SELECT COALESCE(MAX(id), 0) FROM changeset_dicts")
            .fetch_one(&self.pool)
            .await
            .map_err(storage)
    }

    async fn insert_dict(&self, id: i64, data: &[u8]) -> Result<bool> {
        let r = sqlx::query(
            "INSERT INTO changeset_dicts (id, created_at, data) VALUES ($1, $2, $3) \
             ON CONFLICT DO NOTHING",
        )
        .bind(id)
        .bind(leep_core::store::unix_now())
        .bind(data)
        .execute(&self.pool)
        .await
        .map_err(storage)?;
        Ok(r.rows_affected() == 1)
    }

    async fn rewrite(&self, rows: &[Pair]) -> Result<()> {
        let ids: Vec<&[u8]> = rows.iter().map(|r| r.0.as_slice()).collect();
        let data: Vec<&[u8]> = rows.iter().map(|r| r.1.as_slice()).collect();
        sqlx::query(
            "UPDATE changesets SET data = v.data \
             FROM UNNEST($1::bytea[], $2::bytea[]) AS v(id, data) WHERE changesets.id = v.id",
        )
        .bind(ids)
        .bind(data)
        .execute(&self.pool)
        .await
        .map_err(storage)?;
        Ok(())
    }
}

#[async_trait]
impl Rows for SqliteMetaStore {
    fn codec(&self) -> &Codec {
        &self.codec
    }

    fn has_dicts(&self) -> bool {
        self.has_dicts
    }

    async fn load_dicts(&self) -> Result<()> {
        SqliteMetaStore::load_dicts(self).await
    }

    async fn page(&self, after: &[u8], limit: usize) -> Result<Vec<Pair>> {
        let rows = sqlx::query("SELECT id, data FROM changesets WHERE id > ? ORDER BY id LIMIT ?")
            .bind(after)
            .bind(i64::try_from(limit).unwrap_or(i64::MAX))
            .fetch_all(&self.pool)
            .await
            .map_err(storage)?;
        Ok(rows.iter().map(|r| (r.get(0), r.get(1))).collect())
    }

    async fn max_dict(&self) -> Result<i64> {
        sqlx::query_scalar("SELECT COALESCE(MAX(id), 0) FROM changeset_dicts")
            .fetch_one(&self.pool)
            .await
            .map_err(storage)
    }

    async fn insert_dict(&self, id: i64, data: &[u8]) -> Result<bool> {
        let r = sqlx::query(
            "INSERT INTO changeset_dicts (id, created_at, data) VALUES (?, ?, ?) \
             ON CONFLICT DO NOTHING",
        )
        .bind(id)
        .bind(leep_core::store::unix_now())
        .bind(data)
        .execute(&self.pool)
        .await
        .map_err(storage)?;
        Ok(r.rows_affected() == 1)
    }

    async fn rewrite(&self, rows: &[Pair]) -> Result<()> {
        let mut tx = self.pool.begin().await.map_err(storage)?;
        for (id, data) in rows {
            sqlx::query("UPDATE changesets SET data = ? WHERE id = ?")
                .bind(data.as_slice())
                .bind(id.as_slice())
                .execute(&mut *tx)
                .await
                .map_err(storage)?;
        }
        tx.commit().await.map_err(storage)
    }
}
