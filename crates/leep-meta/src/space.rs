//! Sizes of the metadata store, for `leep-server upgrade-index` to say how
//! much disk a derivation needs and what a drop frees. Read-only, except
//! [`vacuum_analyze_derived`], which runs Postgres's non-blocking `VACUUM`.

use std::path::PathBuf;

use leep_core::{Error, Result};
use sqlx::Row;

use crate::{storage, PostgresMetaStore, SchemaMode, SqliteMetaStore};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Backend {
    Postgres,
    Sqlite,
    Memory,
}

/// What could be learned; a field is `None` when the database does not say
/// (a Postgres role that may not read `data_directory`, a SQLite without
/// `dbstat`).
#[derive(Clone, Debug)]
pub struct StorageFacts {
    pub backend: Backend,
    /// The whole database (Postgres) or file, with its WAL (SQLite).
    pub database_bytes: Option<u64>,
    /// The `derived` table with its indexes and TOAST.
    pub derived_bytes: Option<u64>,
    /// Free pages inside a SQLite file, reused before the file grows.
    pub reusable_bytes: Option<u64>,
    /// Where the data lives on this machine, when this process can see it.
    pub data_dir: Option<PathBuf>,
    /// The SQLite database file.
    pub file: Option<PathBuf>,
}

fn is_postgres(url: &str) -> bool {
    url.starts_with("postgres://") || url.starts_with("postgresql://")
}

/// Sizes of the store at `url`. Opens it without migrating.
pub async fn storage_facts(url: &str) -> Result<StorageFacts> {
    if is_postgres(url) {
        let store = PostgresMetaStore::connect_with(url, SchemaMode::RequireCurrent).await?;
        let r = postgres_facts(&store).await;
        store.pool.close().await;
        r
    } else if url.starts_with("sqlite:") {
        let store = SqliteMetaStore::connect_with(url, SchemaMode::RequireCurrent).await?;
        let r = sqlite_facts(&store).await;
        store.pool.close().await;
        r
    } else if url.starts_with("mem://") {
        Ok(StorageFacts {
            backend: Backend::Memory,
            database_bytes: None,
            derived_bytes: None,
            reusable_bytes: None,
            data_dir: None,
            file: None,
        })
    } else {
        Err(Error::Invalid(format!("unsupported meta store url: {url}")))
    }
}

async fn postgres_facts(store: &PostgresMetaStore) -> Result<StorageFacts> {
    let row = sqlx::query(
        "SELECT pg_database_size(current_database()), \
         pg_total_relation_size('derived'::regclass)",
    )
    .fetch_one(&store.pool)
    .await
    .map_err(storage)?;
    let database: i64 = row.try_get(0).map_err(storage)?;
    let derived: i64 = row.try_get(1).map_err(storage)?;
    // Only superusers and pg_read_all_settings may read it; without it the
    // caller cannot check free space and says so.
    let data_dir = sqlx::query_scalar::<_, String>("SELECT current_setting('data_directory')")
        .fetch_one(&store.pool)
        .await
        .ok()
        .map(PathBuf::from)
        .filter(|p| p.is_dir());
    Ok(StorageFacts {
        backend: Backend::Postgres,
        database_bytes: Some(database as u64),
        derived_bytes: Some(derived as u64),
        reusable_bytes: None,
        data_dir,
        file: None,
    })
}

async fn sqlite_facts(store: &SqliteMetaStore) -> Result<StorageFacts> {
    let pool = &store.pool;
    let file: Option<String> =
        sqlx::query_scalar("SELECT file FROM pragma_database_list WHERE name = 'main'")
            .fetch_optional(pool)
            .await
            .map_err(storage)?;
    let page_size: i64 = sqlx::query_scalar("PRAGMA page_size")
        .fetch_one(pool)
        .await
        .map_err(storage)?;
    let pages: i64 = sqlx::query_scalar("PRAGMA page_count")
        .fetch_one(pool)
        .await
        .map_err(storage)?;
    let free: i64 = sqlx::query_scalar("PRAGMA freelist_count")
        .fetch_one(pool)
        .await
        .map_err(storage)?;
    // dbstat is compiled into the bundled SQLite; a system one may lack it.
    let derived: Option<i64> = sqlx::query_scalar(
        "SELECT sum(pgsize) FROM dbstat WHERE name = 'derived' \
         OR tbl_name = 'derived'",
    )
    .fetch_one(pool)
    .await
    .ok()
    .flatten();
    let file = file.filter(|f| !f.is_empty()).map(PathBuf::from);
    let wal = file
        .as_ref()
        .and_then(|f| std::fs::metadata(format!("{}-wal", f.display())).ok())
        .map_or(0, |m| m.len());
    Ok(StorageFacts {
        backend: Backend::Sqlite,
        database_bytes: Some((page_size * pages) as u64 + wal),
        derived_bytes: derived.map(|d| d as u64),
        reusable_bytes: Some((page_size * free) as u64),
        data_dir: file.as_ref().and_then(|f| f.parent().map(PathBuf::from)),
        file,
    })
}

/// `VACUUM (ANALYZE) derived` on Postgres: marks the space of deleted rows
/// for reuse and refreshes statistics without locking out reads or writes
/// (unlike `VACUUM FULL`). Returns whether it ran; SQLite and `mem://` have
/// no such step (SQLite reuses free pages itself, and its `VACUUM` locks the
/// whole database).
pub async fn vacuum_analyze_derived(url: &str) -> Result<bool> {
    if is_postgres(url) {
        use sqlx::Executor;
        let store = PostgresMetaStore::connect_with(url, SchemaMode::RequireCurrent).await?;
        let r = store
            .pool
            .execute("VACUUM (ANALYZE) derived")
            .await
            .map(|_| true)
            .map_err(storage);
        store.pool.close().await;
        r
    } else if url.starts_with("sqlite:") || url.starts_with("mem://") {
        Ok(false)
    } else {
        Err(Error::Invalid(format!("unsupported meta store url: {url}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use leep_core::{Hash, MetaStore};

    #[tokio::test]
    async fn sqlite_sizes_grow_with_derived_rows() {
        let dir = tempfile::tempdir().unwrap();
        let url = format!("sqlite://{}?mode=rwc", dir.path().join("m.db").display());
        let store = SqliteMetaStore::connect(&url).await.unwrap();
        let before = storage_facts(&url).await.unwrap();
        assert_eq!(before.backend, Backend::Sqlite);
        assert_eq!(before.data_dir.as_deref(), Some(dir.path()));
        for i in 0..200u8 {
            store
                .put_derived("x", &Hash([i; 32]), vec![i; 4000].into())
                .await
                .unwrap();
        }
        let after = storage_facts(&url).await.unwrap();
        assert!(after.database_bytes.unwrap() >= before.database_bytes.unwrap() + 800_000);
        if let Some(d) = after.derived_bytes {
            assert!(d >= 800_000, "{d}");
        }
        assert!(!vacuum_analyze_derived(&url).await.unwrap());
    }

    #[tokio::test]
    async fn postgres_reports_sizes_and_vacuums() {
        let Some(url) = crate::tests::pg_url() else {
            return;
        };
        PostgresMetaStore::connect(&url).await.unwrap();
        let f = storage_facts(&url).await.unwrap();
        assert_eq!(f.backend, Backend::Postgres);
        assert!(f.database_bytes.unwrap() > 0);
        assert!(f.derived_bytes.unwrap() > 0);
        assert!(vacuum_analyze_derived(&url).await.unwrap());
    }
}

/// Logical sizes of what the metadata store holds: key and value bytes, not
/// pages, so they compare across backends and with encoded sizes. Postgres's
/// `pg_database_size` is physical and 2.5 to 3.5 times larger (ADR 0059).
#[derive(Clone, Debug, Default)]
pub struct LogicalSizes {
    /// Changeset rows and the stored bytes of `id` and `data` (compressed
    /// rows count compressed, ADR 0053).
    pub changesets: (u64, u64),
    /// Ref log rows and bytes (Postgres: `pg_column_size` of the row, as
    /// `leep-swarm` measures; SQLite: the sum of the column lengths).
    pub ref_log: (u64, u64),
    pub refs: (u64, u64),
    /// Per derived kind: rows and bytes of key plus value.
    pub derived: Vec<(String, u64, u64)>,
    /// The database as stored, when the backend says.
    pub physical_bytes: Option<u64>,
}

/// [`LogicalSizes`] of the store at `url`, with one aggregate query per
/// table (one `GROUP BY kind` for `derived`). Opens it without migrating and
/// only reads; `None` for `mem://`.
pub async fn logical_sizes(url: &str) -> Result<Option<LogicalSizes>> {
    if is_postgres(url) {
        let store = PostgresMetaStore::connect_with(url, SchemaMode::RequireCurrent).await?;
        let r = sizes(
            &store.pool,
            [
                "SELECT COUNT(*)::BIGINT, COALESCE(SUM(octet_length(id) + octet_length(data)), 0)::BIGINT FROM changesets",
                "SELECT COUNT(*)::BIGINT, COALESCE(SUM(pg_column_size(ref_log.*)), 0)::BIGINT FROM ref_log",
                "SELECT COUNT(*)::BIGINT, COALESCE(SUM(pg_column_size(refs.*)), 0)::BIGINT FROM refs",
                "SELECT kind, COUNT(*)::BIGINT, SUM(octet_length(key) + octet_length(value))::BIGINT \
                 FROM derived GROUP BY kind ORDER BY kind",
                "SELECT pg_database_size(current_database())::BIGINT",
            ],
        )
        .await;
        store.pool.close().await;
        r.map(Some)
    } else if url.starts_with("sqlite:") {
        let store = SqliteMetaStore::connect_with(url, SchemaMode::RequireCurrent).await?;
        let r = sizes(
            &store.pool,
            [
                "SELECT COUNT(*), COALESCE(SUM(length(id) + length(data)), 0) FROM changesets",
                "SELECT COUNT(*), COALESCE(SUM(8 + length(name) + COALESCE(length(old_target), 0) \
                 + COALESCE(length(new_target), 0) + 8 + COALESCE(length(actor), 0) \
                 + length(operation) + COALESCE(length(message), 0) \
                 + COALESCE(length(land_id), 0) + COALESCE(length(land_changeset), 0)), 0) \
                 FROM ref_log",
                "SELECT COUNT(*), COALESCE(SUM(length(name) + length(target)), 0) FROM refs",
                "SELECT kind, COUNT(*), SUM(length(key) + length(value)) FROM derived \
                 GROUP BY kind ORDER BY kind",
                "SELECT page_count * page_size FROM pragma_page_count, pragma_page_size",
            ],
        )
        .await;
        store.pool.close().await;
        r.map(Some)
    } else if url.starts_with("mem://") {
        Ok(None)
    } else {
        Err(Error::Invalid(format!("unsupported meta store url: {url}")))
    }
}

async fn sizes<DB>(pool: &sqlx::Pool<DB>, q: [&str; 5]) -> Result<LogicalSizes>
where
    DB: sqlx::Database,
    for<'c> &'c sqlx::Pool<DB>: sqlx::Executor<'c, Database = DB>,
    for<'q> <DB as sqlx::Database>::Arguments<'q>: sqlx::IntoArguments<'q, DB>,
    i64: sqlx::Type<DB> + for<'r> sqlx::Decode<'r, DB>,
    String: sqlx::Type<DB> + for<'r> sqlx::Decode<'r, DB>,
    usize: sqlx::ColumnIndex<DB::Row>,
{
    let pair = |(a, b): (i64, i64)| (a.max(0) as u64, b.max(0) as u64);
    let changesets = pair(
        sqlx::query_as(q[0])
            .fetch_one(pool)
            .await
            .map_err(storage)?,
    );
    let ref_log = pair(
        sqlx::query_as(q[1])
            .fetch_one(pool)
            .await
            .map_err(storage)?,
    );
    let refs = pair(
        sqlx::query_as(q[2])
            .fetch_one(pool)
            .await
            .map_err(storage)?,
    );
    let rows: Vec<(String, i64, i64)> = sqlx::query_as(q[3])
        .fetch_all(pool)
        .await
        .map_err(storage)?;
    let physical: Option<i64> = sqlx::query_scalar(q[4]).fetch_one(pool).await.ok();
    Ok(LogicalSizes {
        changesets,
        ref_log,
        refs,
        derived: rows
            .into_iter()
            .map(|(k, n, b)| (k, n.max(0) as u64, b.max(0) as u64))
            .collect(),
        physical_bytes: physical.map(|p| p.max(0) as u64),
    })
}
