//! `MetaStore` backends: SQLite (single node, tests) and Postgres
//! (production). Both keep changesets, refs and derived data in three tables
//! and implement `cas_ref` as a single conditional SQL statement.

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

#[cfg(test)]
mod tests;
