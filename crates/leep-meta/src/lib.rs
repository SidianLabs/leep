//! `MetaStore` backends: SQLite (single node, tests) and Postgres
//! (production). Both keep changesets, refs and derived data in three tables
//! and implement `cas_ref` as a single conditional SQL statement. A fourth
//! table, `ref_log`, records every ref move in the same transaction. A fifth,
//! `changeset_dicts`, holds the dictionaries changeset rows are compressed
//! with ([`compact`], ADR 0053), and a sixth, `store_features`, the format
//! features rows use, which servers check on open ([`features`], ADR 0055).
//! The schema is versioned by forward-only migrations ([`migrate`], ADR
//! 0023).

use std::sync::Arc;

use leep_core::{Error, MetaStore, Result};

mod codec;
pub mod compact;
mod drop;
pub mod features;
mod leases;
pub mod migrate;
mod postgres;
pub mod snapshot;
pub mod space;
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

/// Connections a Postgres store opens at most unless told otherwise
/// (`leep-server --meta-pool-size`).
pub const DEFAULT_POSTGRES_POOL_SIZE: u32 = 16;

/// [`open_meta_store`] with a choice of what to do about pending migrations.
/// Either way, a database newer than this binary is refused.
pub async fn open_meta_store_with(url: &str, mode: SchemaMode) -> Result<Arc<dyn MetaStore>> {
    open_meta_store_with_pool(url, mode, DEFAULT_POSTGRES_POOL_SIZE).await
}

/// [`open_meta_store_with`], with at most `pool_size` connections to
/// Postgres. SQLite keeps its own small pool: it has one writer at a time.
pub async fn open_meta_store_with_pool(
    url: &str,
    mode: SchemaMode,
    pool_size: u32,
) -> Result<Arc<dyn MetaStore>> {
    if url.starts_with("sqlite:") {
        Ok(Arc::new(SqliteMetaStore::connect_with(url, mode).await?))
    } else if url.starts_with("postgres://") || url.starts_with("postgresql://") {
        Ok(Arc::new(
            PostgresMetaStore::connect_with_pool(url, mode, pool_size).await?,
        ))
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

/// The migration that added `changesets.stored_at` (ADR 0059). Rows stored
/// before it have none and read as stored when it was applied.
pub const STORED_AT_SCHEMA_VERSION: i64 = 7;

/// Unix seconds now, for `changesets.stored_at`.
fn stored_now() -> i64 {
    leep_core::store::unix_now()
}

/// Answer a batched store-time read in request order from `(id, stored_at)`
/// rows. The queries already read a NULL or negative `stored_at` (a writer
/// with a clock before 1970) as the migration's `applied_at`, so a bad clock
/// cannot make a changeset look older than the column.
fn stored_at_in_order(
    ids: &[leep_core::ChangesetId],
    rows: impl Iterator<Item = (Vec<u8>, Option<i64>)>,
) -> Result<Vec<Option<u64>>> {
    let mut found = std::collections::HashMap::new();
    for (id, at) in rows {
        let at = at.ok_or_else(|| {
            storage("changeset stored before migration 0007 and no applied_at for it")
        })?;
        found.insert(to_id(&id)?, u64::try_from(at).unwrap_or(0));
    }
    Ok(ids.iter().map(|id| found.get(id).copied()).collect())
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
    // The last request for an id takes the decoded changeset; only repeats
    // clone it. A long log reads thousands, and cloning each was as costly as
    // decoding it.
    let mut uses: std::collections::HashMap<&leep_core::ChangesetId, usize> =
        std::collections::HashMap::with_capacity(ids.len());
    for id in ids {
        *uses.entry(id).or_default() += 1;
    }
    Ok(ids
        .iter()
        .map(|id| {
            let left = uses.get_mut(id).expect("counted");
            *left -= 1;
            if *left > 0 {
                found.get(id).cloned()
            } else {
                found.remove(id)
            }
        })
        .collect())
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

/// The `land_id` and `land_changeset` columns of a move's log entry
/// (ADR 0057).
fn land_columns(ctx: &leep_core::RefUpdateContext) -> (Option<Vec<u8>>, Option<Vec<u8>>) {
    match &ctx.land {
        Some(l) => (
            Some(l.id.as_bytes().to_vec()),
            Some(l.changeset.0 .0.to_vec()),
        ),
        None => (None, None),
    }
}

/// The error of a ref log insert: one that hit `ref_log_land_id` recorded a
/// land id twice for the ref, which the contract makes `Conflict`.
fn log_insert_error(name: &str, e: sqlx::Error) -> Error {
    if e.as_database_error()
        .is_some_and(|d| d.is_unique_violation())
    {
        return Error::Conflict(format!(
            "a land id of this move is already recorded for ref {name:?}"
        ));
    }
    storage(e)
}

/// `(land_id, land_changeset, new_target, seq)` as selected by `find_lands`.
type LandRow = (Vec<u8>, Option<Vec<u8>>, Option<Vec<u8>>, i64);

/// Answer `find_lands` in request order.
fn lands_in_order(
    ids: &[leep_core::LandId],
    rows: Vec<LandRow>,
) -> Result<Vec<Option<leep_core::LandRecord>>> {
    let mut found = std::collections::HashMap::new();
    for (id, cs, new, seq) in rows {
        let (Some(cs), Some(new)) = (cs, new) else {
            return Err(storage("ref log row has a land id but no changesets"));
        };
        found.insert(
            id,
            leep_core::LandRecord {
                changeset: to_id(&cs)?,
                landed: to_id(&new)?,
                seq: u64::try_from(seq).map_err(|_| storage("negative ref log seq in db"))?,
            },
        );
    }
    Ok(ids
        .iter()
        .map(|id| found.get(id.as_bytes()).copied())
        .collect())
}

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
