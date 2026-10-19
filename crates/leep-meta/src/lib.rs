//! `MetaStore` backends: SQLite (single node, tests) and Postgres
//! (production). Both keep changesets, refs and derived data in three tables
//! and implement `cas_ref` as a single conditional SQL statement. A fourth
//! table, `ref_log`, records every ref move in the same transaction. A fifth,
//! `changeset_dicts`, holds the dictionaries changeset rows are compressed
//! with ([`compact`], ADR 0052). The schema is versioned by forward-only
//! migrations ([`migrate`], ADR 0023).

use std::sync::Arc;

use leep_core::{Error, MetaStore, Result};

mod codec;
pub mod compact;
pub mod migrate;
mod postgres;
pub mod snapshot;
mod sqlite;

pub use migrate::{AppliedMigration, Migration, MigrationRef, MigrationReport};
pub use postgres::{PostgresMetaStore, MIGRATIONS as POSTGRES_MIGRATIONS};
pub use sqlite::{SqliteMetaStore, MIGRATIONS as SQLITE_MIGRATIONS};

/// What opening a store does about pending schema migrations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaMode {
    /// Apply them (the default).
    Migrate,
    /// Refuse to open unless there are none (`--no-migrate`). Writes nothing.
    RequireCurrent,
}

/// Open a meta store from a URL: `sqlite:...`, `postgres://...`,
/// `postgresql://...` or `mem://` (in-memory, not persistent). Pending schema
/// migrations are applied.
pub async fn open_meta_store(url: &str) -> Result<Arc<dyn MetaStore>> {
    open_meta_store_with(url, SchemaMode::Migrate).await
}

/// [`open_meta_store`] with a choice of what to do about pending migrations.
/// Either way, a database newer than this binary is refused.
pub async fn open_meta_store_with(url: &str, mode: SchemaMode) -> Result<Arc<dyn MetaStore>> {
    if url.starts_with("sqlite:") {
        Ok(Arc::new(SqliteMetaStore::connect_with(url, mode).await?))
    } else if url.starts_with("postgres://") || url.starts_with("postgresql://") {
        Ok(Arc::new(PostgresMetaStore::connect_with(url, mode).await?))
    } else if url.starts_with("mem://") {
        Ok(Arc::new(leep_core::memory::MemoryMetaStore::new()))
    } else {
        Err(Error::Invalid(format!("unsupported meta store url: {url}")))
    }
}

/// Apply pending migrations to the database at `url`, or with `dry_run` only
/// report what would happen. Safe beside running servers of this version.
pub async fn migrate_meta_store(url: &str, dry_run: bool) -> Result<MigrationReport> {
    if url.starts_with("sqlite:") {
        SqliteMetaStore::migrate(url, dry_run).await
    } else if url.starts_with("postgres://") || url.starts_with("postgresql://") {
        PostgresMetaStore::migrate(url, dry_run).await
    } else if url.starts_with("mem://") {
        Err(Error::Invalid("mem:// has no schema to migrate".into()))
    } else {
        Err(Error::Invalid(format!("unsupported meta store url: {url}")))
    }
}

/// After a bulk load into the store at `url`, refresh the planner statistics
/// of its tables so the queries that follow do not plan from estimates made
/// before the load (ADR 0042). Postgres only; returns whether it ran. SQLite
/// and `mem://` need nothing, and an unknown URL is refused as elsewhere.
pub async fn analyze_meta_store(url: &str) -> Result<bool> {
    if url.starts_with("postgres://") || url.starts_with("postgresql://") {
        let store = PostgresMetaStore::connect_with(url, SchemaMode::RequireCurrent).await?;
        let r = store.analyze().await;
        store.pool.close().await;
        r.map(|()| true)
    } else if url.starts_with("sqlite:") || url.starts_with("mem://") {
        Ok(false)
    } else {
        Err(Error::Invalid(format!("unsupported meta store url: {url}")))
    }
}

fn storage(e: impl std::fmt::Display) -> Error {
    Error::Storage(e.to_string())
}

fn to_id(bytes: &[u8]) -> Result<leep_core::ChangesetId> {
    let arr: [u8; 32] = bytes
        .try_into()
        .map_err(|_| storage("bad id length in db"))?;
    Ok(leep_core::ChangesetId(leep_core::Hash(arr)))
}

fn to_hash(bytes: &[u8]) -> Result<leep_core::Hash> {
    let arr: [u8; 32] = bytes
        .try_into()
        .map_err(|_| storage("bad key length in db"))?;
    Ok(leep_core::Hash(arr))
}

/// Distinct keys per SQLite batched read: well below its bound-parameter
/// limit (32,766 since 3.32, 999 before), with room for the other binds.
const BATCH_PARAMS: usize = 500;

/// `?, ?, ...` for `n` SQLite parameters.
fn placeholders(n: usize) -> String {
    let mut s = "?,".repeat(n);
    s.pop();
    s
}

/// Distinct keys in first-seen order, so a batched read sends each once.
fn unique<'a>(keys: impl Iterator<Item = &'a [u8]>) -> Vec<&'a [u8]> {
    let mut seen = std::collections::HashSet::new();
    keys.filter(|k| seen.insert(*k)).collect()
}

/// Answer a batched changeset read in request order from `(id, data)` rows,
/// decoding and checking each stored row once.
fn changesets_in_order(
    codec: &codec::Codec,
    ids: &[leep_core::ChangesetId],
    rows: impl Iterator<Item = (Vec<u8>, Vec<u8>)>,
) -> Result<Vec<Option<leep_core::Changeset>>> {
    let mut found = std::collections::HashMap::new();
    for (id, data) in rows {
        let id = to_id(&id)?;
        let cs = codec.decode_checked(&id, &data)?;
        found.insert(id, cs);
    }
    Ok(ids.iter().map(|id| found.get(id).cloned()).collect())
}

/// Answer a batched derived read in request order from `(key, value)` rows.
fn values_in_order(
    keys: &[leep_core::Hash],
    rows: impl Iterator<Item = (Vec<u8>, Vec<u8>)>,
) -> Vec<Option<bytes::Bytes>> {
    let found: std::collections::HashMap<Vec<u8>, bytes::Bytes> =
        rows.map(|(k, v)| (k, bytes::Bytes::from(v))).collect();
    keys.iter()
        .map(|k| found.get(k.as_bytes().as_slice()).cloned())
        .collect()
}

/// The rows of a batched derived write as three columns, keeping only the
/// last row for each `(kind, key)`: that is what writing them in order leaves,
/// and Postgres refuses to update one row twice in a statement.
fn derived_columns(rows: &[leep_core::DerivedRow]) -> (Vec<&str>, Vec<&[u8]>, Vec<&[u8]>) {
    let mut seen = std::collections::HashSet::new();
    let mut last: Vec<&leep_core::DerivedRow> = rows
        .iter()
        .rev()
        .filter(|r| seen.insert((r.kind.as_str(), r.key)))
        .collect();
    last.reverse();
    (
        last.iter().map(|r| r.kind.as_str()).collect(),
        last.iter().map(|r| r.key.as_bytes().as_slice()).collect(),
        last.iter().map(|r| r.value.as_ref()).collect(),
    )
}

/// Rows `delete_derived_kind` removes per statement, so dropping a large kind
/// is many short transactions, not one that holds locks for minutes.
const DELETE_BATCH: i64 = 10_000;

/// The row layout both backends select from `ref_log`.
type LogRow = (
    i64,
    String,
    Option<Vec<u8>>,
    Option<Vec<u8>>,
    i64,
    Option<String>,
    String,
    Option<String>,
);

fn log_entry(row: LogRow) -> Result<leep_core::RefLogEntry> {
    let (seq, name, old, new, time, actor, operation, message) = row;
    Ok(leep_core::RefLogEntry {
        seq: u64::try_from(seq).map_err(|_| storage("negative ref log seq in db"))?,
        name,
        old: old.map(|b| to_id(&b)).transpose()?,
        new: new.map(|b| to_id(&b)).transpose()?,
        time,
        actor,
        operation,
        message,
    })
}

#[cfg(test)]
mod compact_tests;
#[cfg(test)]
mod migrate_tests;
#[cfg(test)]
mod snapshot_tests;
#[cfg(test)]
mod tests;
