//! `MetaStore` backends: SQLite (single node, tests) and Postgres
//! (production). Both keep changesets, refs and derived data in three tables
//! and implement `cas_ref` as a single conditional SQL statement. A fourth
//! table, `ref_log`, records every ref move in the same transaction.

use std::sync::Arc;

use leep_core::{Error, MetaStore, Result};

mod postgres;
mod sqlite;

pub use postgres::PostgresMetaStore;
pub use sqlite::SqliteMetaStore;

/// Open a meta store from a URL: `sqlite:...`, `postgres://...`,
/// `postgresql://...` or `mem://` (in-memory, not persistent).
pub async fn open_meta_store(url: &str) -> Result<Arc<dyn MetaStore>> {
    if url.starts_with("sqlite:") {
        Ok(Arc::new(SqliteMetaStore::connect(url).await?))
    } else if url.starts_with("postgres://") || url.starts_with("postgresql://") {
        Ok(Arc::new(PostgresMetaStore::connect(url).await?))
    } else if url.starts_with("mem://") {
        Ok(Arc::new(leep_core::memory::MemoryMetaStore::new()))
    } else {
        Err(Error::Invalid(format!("unsupported meta store url: {url}")))
    }
}

fn storage(e: impl std::fmt::Display) -> Error {
    Error::Storage(e.to_string())
}

/// Decode stored changeset bytes and check they hash to `id`.
fn decode_checked(id: &leep_core::ChangesetId, data: &[u8]) -> Result<leep_core::Changeset> {
    let cs = leep_core::Changeset::decode(data)?;
    if cs.id() != *id {
        return Err(Error::Storage(format!(
            "corrupt changeset row {}",
            id.0.to_hex()
        )));
    }
    Ok(cs)
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
mod tests;
