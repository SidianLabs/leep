//! Consistent logical snapshots of a metadata store, and their restore
//! (ADR 0026).
//!
//! A snapshot is a SQLite database file with the normal schema, written from
//! one read transaction on the source: `REPEATABLE READ` on Postgres, a WAL
//! read transaction on SQLite. Every row in it was committed at one instant,
//! whichever backend it came from, so a ref log entry and the ref it moved
//! always appear together. Because it is an ordinary Leep metadata database,
//! `verify` and the server open it unchanged, and it restores into either
//! backend.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use leep_core::{ChangesetId, Error, Result};
use sqlx::{PgPool, Postgres, QueryBuilder, Row, Sqlite, SqlitePool, Transaction};

use crate::{
    migrate, storage, to_id, LogRow, PostgresMetaStore, SchemaMode, SqliteMetaStore,
    POSTGRES_MIGRATIONS, SQLITE_MIGRATIONS,
};

/// Rows read or written per statement. Changeset rows can be large, so the
/// batch is modest.
const PAGE: usize = 500;

/// What a snapshot holds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotInfo {
    /// Newest schema migration applied to the source (and to the snapshot).
    pub schema_version: u64,
    /// Unix seconds when the read transaction took its snapshot. Every blob
    /// the snapshot references was in the blob store at this time.
    pub taken_at: i64,
    pub changesets: u64,
    /// Changeset compression dictionaries (ADR 0052), which the rows that
    /// use them cannot be read without.
    pub changeset_dicts: u64,
    /// Ref tips at the snapshot.
    pub refs: BTreeMap<String, ChangesetId>,
    pub derived: u64,
    pub ref_log: u64,
    /// Highest ref log `seq` ever issued, which can exceed the highest entry
    /// that still exists.
    pub ref_log_last_seq: u64,
}

/// Say which step failed: "database is locked" alone does not.
fn at<T>(stage: &str, r: Result<T>) -> Result<T> {
    r.map_err(|e| match e {
        Error::Storage(m) => Error::Storage(format!("{stage}: {m}")),
        Error::Other(m) => Error::Other(format!("{stage}: {m}")),
        other => other,
    })
}

/// Newest schema version this binary knows, for checking a backup against it.
pub fn latest_schema_version() -> u64 {
    // Both lists are kept in step; a divergence is a bug the migrate tests catch.
    migrate::latest(SQLITE_MIGRATIONS).max(migrate::latest(POSTGRES_MIGRATIONS))
}

/// `sqlite://` URL for a file, with the characters that mean something in a
/// URL escaped.
pub fn sqlite_file_url(path: &Path, create: bool) -> String {
    let mut out = String::from("sqlite://");
    for b in path.to_string_lossy().bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'/' | b'.' | b'_' | b'-') {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out.push_str(if create { "?mode=rwc" } else { "?mode=rw" });
    out
}

fn is_sqlite(url: &str) -> bool {
    url.starts_with("sqlite:")
}

fn is_postgres(url: &str) -> bool {
    url.starts_with("postgres://") || url.starts_with("postgresql://")
}

type Pair = (Vec<u8>, Vec<u8>);
type DerivedRow = (String, Vec<u8>, Vec<u8>);
/// `changeset_dicts`: id, created_at, data.
type DictRow = (i64, i64, Vec<u8>);

/// Rows of one read transaction on the source.
#[async_trait]
trait Source: Send {
    async fn schema_version(&mut self) -> Result<u64>;
    async fn changesets(&mut self, after: &[u8], limit: usize) -> Result<Vec<Pair>>;
    async fn changeset_dicts(&mut self) -> Result<Vec<DictRow>>;
    async fn refs(&mut self) -> Result<Vec<(String, Vec<u8>)>>;
    async fn derived(&mut self, after: &(String, Vec<u8>), limit: usize)
        -> Result<Vec<DerivedRow>>;
    async fn ref_log(&mut self, after_seq: i64, limit: usize) -> Result<Vec<LogRow>>;
    async fn ref_log_last_seq(&mut self) -> Result<i64>;
}

/// One write transaction on the destination.
#[async_trait]
trait Sink: Send {
    async fn is_empty(&mut self) -> Result<bool>;
    async fn clear(&mut self) -> Result<()>;
    async fn put_changesets(&mut self, rows: &[Pair]) -> Result<()>;
    async fn put_changeset_dicts(&mut self, rows: &[DictRow]) -> Result<()>;
    async fn put_refs(&mut self, rows: &[(String, Vec<u8>)]) -> Result<()>;
    async fn put_derived(&mut self, rows: &[DerivedRow]) -> Result<()>;
    async fn put_ref_log(&mut self, rows: &[LogRow]) -> Result<()>;
    async fn set_ref_log_last_seq(&mut self, seq: i64) -> Result<()>;
    async fn commit(self: Box<Self>) -> Result<()>;
}

/// Stream everything from `src` into `dst`, in a stable order, and report it.
async fn copy(src: &mut dyn Source, dst: &mut dyn Sink) -> Result<SnapshotInfo> {
    let mut info = SnapshotInfo {
        schema_version: src.schema_version().await?,
        taken_at: leep_core::store::unix_now(),
        changesets: 0,
        changeset_dicts: 0,
        refs: BTreeMap::new(),
        derived: 0,
        ref_log: 0,
        ref_log_last_seq: 0,
    };
    let dicts = src.changeset_dicts().await?;
    info.changeset_dicts = dicts.len() as u64;
    dst.put_changeset_dicts(&dicts).await?;
    let mut after = Vec::new();
    loop {
        let page = src.changesets(&after, PAGE).await?;
        let Some(last) = page.last() else { break };
        after = last.0.clone();
        info.changesets += page.len() as u64;
        dst.put_changesets(&page).await?;
    }
    let refs = src.refs().await?;
    for (name, target) in &refs {
        info.refs.insert(name.clone(), to_id(target)?);
    }
    dst.put_refs(&refs).await?;
    let mut after = (String::new(), Vec::new());
    loop {
        let page = src.derived(&after, PAGE).await?;
        let Some(last) = page.last() else { break };
        after = (last.0.clone(), last.1.clone());
        info.derived += page.len() as u64;
        dst.put_derived(&page).await?;
    }
    let mut after = 0i64;
    loop {
        let page = src.ref_log(after, PAGE).await?;
        let Some(last) = page.last() else { break };
        after = last.0;
        info.ref_log += page.len() as u64;
        dst.put_ref_log(&page).await?;
    }
    let last_seq = src.ref_log_last_seq().await?.max(after);
    info.ref_log_last_seq = u64::try_from(last_seq).map_err(|_| storage("negative ref log seq"))?;
    dst.set_ref_log_last_seq(last_seq).await?;
    Ok(info)
}

/// Write a consistent snapshot of the metadata store at `src_url` to the new
/// SQLite file `dest`. The source is only read, and never migrated: a source
/// with pending migrations is refused (run `leep-server migrate` first), as is
/// one newer than this binary. `dest` must not exist; it is written under a
/// temporary name and renamed into place when complete and durable, as one
/// self-contained file (no `-wal` file belongs to it).
pub async fn export_snapshot(src_url: &str, dest: &Path) -> Result<SnapshotInfo> {
    if dest.exists() {
        return Err(Error::Invalid(format!("{} already exists", dest.display())));
    }
    let mut partial = dest.as_os_str().to_owned();
    partial.push(".partial");
    let partial = PathBuf::from(partial);
    remove_sqlite_files(&partial);
    let res = export_to(src_url, &partial).await;
    let info = match res {
        Ok(i) => i,
        Err(e) => {
            remove_sqlite_files(&partial);
            return Err(e);
        }
    };
    let sync = (|| -> std::io::Result<()> {
        std::fs::File::open(&partial)?.sync_all()?;
        std::fs::rename(&partial, dest)?;
        if let Some(dir) = dest.parent().filter(|d| !d.as_os_str().is_empty()) {
            std::fs::File::open(dir)?.sync_all()?;
        }
        Ok(())
    })();
    sync.map_err(storage)?;
    Ok(info)
}

fn remove_sqlite_files(path: &Path) {
    for suffix in ["", "-wal", "-shm", "-journal"] {
        let mut p = path.as_os_str().to_owned();
        p.push(suffix);
        let _ = std::fs::remove_file(PathBuf::from(p));
    }
}

async fn export_to(src_url: &str, partial: &Path) -> Result<SnapshotInfo> {
    // The destination is created first: if it cannot be, nothing has been
    // read, and the snapshot instant is as late as possible.
    let dest_url = sqlite_file_url(partial, true);
    let dest = at(
        "creating the snapshot file",
        SqliteMetaStore::create_single(&dest_url).await,
    )?;
    let mut sink = at(
        "creating the snapshot file",
        SqliteSink::begin(&dest.pool).await,
    )?;
    let info = if is_sqlite(src_url) {
        let store = at(
            "opening the source metadata store",
            SqliteMetaStore::connect_with(src_url, SchemaMode::RequireCurrent).await,
        )?;
        let mut src = at(
            "starting the snapshot read",
            SqliteSource::begin(&store.pool).await,
        )?;
        at("copying rows", copy(&mut src, &mut sink).await)?
    } else if is_postgres(src_url) {
        let store = at(
            "opening the source metadata store",
            PostgresMetaStore::connect_with(src_url, SchemaMode::RequireCurrent).await,
        )?;
        let mut src = at(
            "starting the snapshot read",
            PgSource::begin(&store.pool).await,
        )?;
        at("copying rows", copy(&mut src, &mut sink).await)?
    } else {
        return Err(Error::Invalid(format!(
            "cannot snapshot a metadata store at {src_url:?}: only sqlite: and postgres:// are supported"
        )));
    };
    at(
        "committing the snapshot file",
        Box::new(sink).commit().await,
    )?;
    dest.pool.close().await;
    Ok(info)
}

/// Schema version recorded in a snapshot file, without changing the file.
pub async fn snapshot_schema_version(file: &Path) -> Result<u64> {
    use sqlx::sqlite::SqliteConnectOptions;
    use sqlx::ConnectOptions;
    let mut conn = SqliteConnectOptions::new()
        .filename(file)
        .read_only(true)
        .immutable(true)
        .connect()
        .await
        .map_err(storage)?;
    let v: Option<i64> = sqlx::query_scalar("SELECT MAX(version) FROM schema_migrations")
        .fetch_one(&mut conn)
        .await
        .map_err(|e| Error::Invalid(format!("{} is not a Leep snapshot: {e}", file.display())))?;
    Ok(v.unwrap_or(0) as u64)
}

/// What an import wrote.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ImportInfo {
    pub changesets: u64,
    pub refs: u64,
    pub derived: u64,
    pub ref_log: u64,
}

/// A scratch copy of a snapshot file that is deleted when dropped, so the
/// original is never opened for writing (the media may be read-only, and
/// opening would change the file the manifest hashed).
struct Scratch(PathBuf);

impl Drop for Scratch {
    fn drop(&mut self) {
        if let Some(dir) = self.0.parent() {
            let _ = std::fs::remove_dir_all(dir);
        }
    }
}

fn scratch_copy(src: &Path) -> Result<Scratch> {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.subsec_nanos());
    let dir = std::env::temp_dir().join(format!("leep-snapshot-{}-{nanos}", std::process::id()));
    std::fs::create_dir_all(&dir).map_err(storage)?;
    let path = dir.join("meta.db");
    let scratch = Scratch(path.clone());
    std::fs::copy(src, &path).map_err(|e| {
        Error::Storage(format!(
            "copying {} to {}: {e}",
            src.display(),
            path.display()
        ))
    })?;
    Ok(scratch)
}

/// Open a snapshot file for reading as a metadata store URL, through a scratch
/// copy that is migrated if older (ADR 0023) and refused if newer. The URL is
/// valid while the returned guard lives.
pub struct OpenedSnapshot {
    _scratch: Scratch,
    pub url: String,
}

pub async fn open_snapshot(file: &Path) -> Result<OpenedSnapshot> {
    let scratch = scratch_copy(file)?;
    let url = sqlite_file_url(&scratch.0, false);
    SqliteMetaStore::connect_with(&url, SchemaMode::Migrate)
        .await
        .map_err(|e| match e {
            Error::Other(m) | Error::Storage(m) => Error::Other(format!("snapshot: {m}")),
            other => other,
        })?
        .pool
        .close()
        .await;
    Ok(OpenedSnapshot {
        _scratch: scratch,
        url,
    })
}

/// Whether the store at `url` holds no changesets, refs, derived rows or ref
/// log entries, so a restore can tell before it copies anything. The schema
/// is created or migrated as usual (`mode`).
pub async fn destination_is_empty(url: &str, mode: SchemaMode) -> Result<bool> {
    if is_sqlite(url) {
        let store = SqliteMetaStore::connect_with(url, mode).await?;
        SqliteSink::begin(&store.pool).await?.is_empty().await
    } else if is_postgres(url) {
        let store = PostgresMetaStore::connect_with(url, mode).await?;
        PgSink::begin(&store.pool).await?.is_empty().await
    } else {
        Err(Error::Invalid(format!(
            "cannot restore into a metadata store at {url:?}: only sqlite: and postgres:// are supported"
        )))
    }
}

/// Load the snapshot file `src` into the metadata store at `dest_url`, in one
/// transaction: either every row arrives or none does. The destination must
/// hold no changesets, refs, derived rows or ref log entries unless `force`,
/// which deletes them first (inside the same transaction). The destination
/// schema is created or migrated as usual (`mode`); a snapshot from a newer
/// schema is refused and an older one is migrated first.
pub async fn import_snapshot(
    src: &Path,
    dest_url: &str,
    force: bool,
    mode: SchemaMode,
) -> Result<ImportInfo> {
    let opened = open_snapshot(src).await?;
    let source_store =
        SqliteMetaStore::connect_with(&opened.url, SchemaMode::RequireCurrent).await?;
    let mut source = SqliteSource::begin(&source_store.pool).await?;
    let mut pg = None;
    let mut sink: Box<dyn Sink> = if is_sqlite(dest_url) {
        let store = SqliteMetaStore::connect_with(dest_url, mode).await?;
        Box::new(SqliteSink::begin(&store.pool).await?)
    } else if is_postgres(dest_url) {
        let store = pg.insert(PostgresMetaStore::connect_with(dest_url, mode).await?);
        Box::new(PgSink::begin(&store.pool).await?)
    } else {
        return Err(Error::Invalid(format!(
            "cannot restore into a metadata store at {dest_url:?}: only sqlite: and postgres:// are supported"
        )));
    };
    if !sink.is_empty().await? {
        if !force {
            return Err(Error::Invalid(
                "the destination metadata store is not empty (it has changesets, refs, derived \
                 data or ref log entries); restore into a fresh database, or pass --force to \
                 delete what is there"
                    .into(),
            ));
        }
        sink.clear().await?;
    }
    let info = copy(&mut source, &mut *sink).await?;
    sink.commit().await?;
    // Every row arrived after the tables were last analyzed (ADR 0042). The
    // restore has committed, so a failure here is only worth a warning.
    if let Some(store) = pg {
        if let Err(e) = store.analyze().await {
            tracing::warn!(error = %e, "restore: ANALYZE after loading failed");
        }
    }
    Ok(ImportInfo {
        changesets: info.changesets,
        refs: info.refs.len() as u64,
        derived: info.derived,
        ref_log: info.ref_log,
    })
}

// --- SQLite -----------------------------------------------------------------

struct SqliteSource(Transaction<'static, Sqlite>);

impl SqliteSource {
    /// A deferred transaction takes its snapshot at its first read, so the
    /// first statement is the schema version.
    async fn begin(pool: &SqlitePool) -> Result<Self> {
        let mut s = SqliteSource(pool.begin().await.map_err(storage)?);
        s.schema_version().await?;
        Ok(s)
    }
}

#[async_trait]
impl Source for SqliteSource {
    async fn schema_version(&mut self) -> Result<u64> {
        let v: Option<i64> = sqlx::query_scalar("SELECT MAX(version) FROM schema_migrations")
            .fetch_one(&mut *self.0)
            .await
            .map_err(storage)?;
        Ok(v.unwrap_or(0) as u64)
    }

    async fn changeset_dicts(&mut self) -> Result<Vec<DictRow>> {
        sqlx::query_as("SELECT id, created_at, data FROM changeset_dicts ORDER BY id")
            .fetch_all(&mut *self.0)
            .await
            .map_err(storage)
    }

    async fn changesets(&mut self, after: &[u8], limit: usize) -> Result<Vec<Pair>> {
        let rows = sqlx::query("SELECT id, data FROM changesets WHERE id > ? ORDER BY id LIMIT ?")
            .bind(after)
            .bind(limit as i64)
            .fetch_all(&mut *self.0)
            .await
            .map_err(storage)?;
        Ok(rows.into_iter().map(|r| (r.get(0), r.get(1))).collect())
    }

    async fn refs(&mut self) -> Result<Vec<(String, Vec<u8>)>> {
        let rows = sqlx::query("SELECT name, target FROM refs ORDER BY name")
            .fetch_all(&mut *self.0)
            .await
            .map_err(storage)?;
        Ok(rows.into_iter().map(|r| (r.get(0), r.get(1))).collect())
    }

    async fn derived(
        &mut self,
        after: &(String, Vec<u8>),
        limit: usize,
    ) -> Result<Vec<DerivedRow>> {
        let rows = sqlx::query(
            "SELECT kind, key, value FROM derived WHERE (kind, key) > (?, ?) \
             ORDER BY kind, key LIMIT ?",
        )
        .bind(&after.0)
        .bind(&after.1)
        .bind(limit as i64)
        .fetch_all(&mut *self.0)
        .await
        .map_err(storage)?;
        Ok(rows
            .into_iter()
            .map(|r| (r.get(0), r.get(1), r.get(2)))
            .collect())
    }

    async fn ref_log(&mut self, after_seq: i64, limit: usize) -> Result<Vec<LogRow>> {
        sqlx::query_as(
            "SELECT seq, name, old_target, new_target, time, actor, operation, message \
             FROM ref_log WHERE seq > ? ORDER BY seq LIMIT ?",
        )
        .bind(after_seq)
        .bind(limit as i64)
        .fetch_all(&mut *self.0)
        .await
        .map_err(storage)
    }

    async fn ref_log_last_seq(&mut self) -> Result<i64> {
        let v: Option<i64> =
            sqlx::query_scalar("SELECT seq FROM sqlite_sequence WHERE name = 'ref_log'")
                .fetch_optional(&mut *self.0)
                .await
                .map_err(storage)?;
        Ok(v.unwrap_or(0))
    }
}

struct SqliteSink(Transaction<'static, Sqlite>);

impl SqliteSink {
    async fn begin(pool: &SqlitePool) -> Result<Self> {
        // The write lock is taken now, so a destination another process is
        // writing to is reported before any row is read.
        Ok(SqliteSink(
            pool.begin_with("BEGIN IMMEDIATE").await.map_err(storage)?,
        ))
    }
}

#[async_trait]
impl Sink for SqliteSink {
    async fn is_empty(&mut self) -> Result<bool> {
        let n: i64 = sqlx::query_scalar(
            "SELECT (SELECT COUNT(*) FROM (SELECT 1 FROM changesets LIMIT 1)) \
             + (SELECT COUNT(*) FROM (SELECT 1 FROM refs LIMIT 1)) \
             + (SELECT COUNT(*) FROM (SELECT 1 FROM derived LIMIT 1)) \
             + (SELECT COUNT(*) FROM (SELECT 1 FROM ref_log LIMIT 1)) \
             + (SELECT COUNT(*) FROM (SELECT 1 FROM changeset_dicts LIMIT 1))",
        )
        .fetch_one(&mut *self.0)
        .await
        .map_err(storage)?;
        Ok(n == 0)
    }

    async fn clear(&mut self) -> Result<()> {
        for t in [
            "changesets",
            "changeset_dicts",
            "refs",
            "derived",
            "ref_log",
        ] {
            sqlx::query(&format!("DELETE FROM {t}"))
                .execute(&mut *self.0)
                .await
                .map_err(storage)?;
        }
        Ok(())
    }

    async fn put_changeset_dicts(&mut self, rows: &[DictRow]) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let mut q =
            QueryBuilder::<Sqlite>::new("INSERT INTO changeset_dicts (id, created_at, data) ");
        q.push_values(rows, |mut b, (id, at, data)| {
            b.push_bind(id).push_bind(at).push_bind(data);
        });
        q.build().execute(&mut *self.0).await.map_err(storage)?;
        Ok(())
    }

    async fn put_changesets(&mut self, rows: &[Pair]) -> Result<()> {
        let mut q = QueryBuilder::<Sqlite>::new("INSERT INTO changesets (id, data) ");
        q.push_values(rows, |mut b, (id, data)| {
            b.push_bind(id).push_bind(data);
        });
        q.build().execute(&mut *self.0).await.map_err(storage)?;
        Ok(())
    }

    async fn put_refs(&mut self, rows: &[(String, Vec<u8>)]) -> Result<()> {
        for chunk in rows.chunks(PAGE) {
            let mut q = QueryBuilder::<Sqlite>::new("INSERT INTO refs (name, target) ");
            q.push_values(chunk, |mut b, (n, t)| {
                b.push_bind(n).push_bind(t);
            });
            q.build().execute(&mut *self.0).await.map_err(storage)?;
        }
        Ok(())
    }

    async fn put_derived(&mut self, rows: &[DerivedRow]) -> Result<()> {
        let mut q = QueryBuilder::<Sqlite>::new("INSERT INTO derived (kind, key, value) ");
        q.push_values(rows, |mut b, (k, key, v)| {
            b.push_bind(k).push_bind(key).push_bind(v);
        });
        q.build().execute(&mut *self.0).await.map_err(storage)?;
        Ok(())
    }

    async fn put_ref_log(&mut self, rows: &[LogRow]) -> Result<()> {
        let mut q = QueryBuilder::<Sqlite>::new(
            "INSERT INTO ref_log (seq, name, old_target, new_target, time, actor, operation, message) ",
        );
        q.push_values(rows, |mut b, r| {
            b.push_bind(r.0)
                .push_bind(&r.1)
                .push_bind(&r.2)
                .push_bind(&r.3)
                .push_bind(r.4)
                .push_bind(&r.5)
                .push_bind(&r.6)
                .push_bind(&r.7);
        });
        q.build().execute(&mut *self.0).await.map_err(storage)?;
        Ok(())
    }

    async fn set_ref_log_last_seq(&mut self, seq: i64) -> Result<()> {
        let n = sqlx::query("UPDATE sqlite_sequence SET seq = ? WHERE name = 'ref_log'")
            .bind(seq)
            .execute(&mut *self.0)
            .await
            .map_err(storage)?
            .rows_affected();
        if n == 0 && seq > 0 {
            sqlx::query("INSERT INTO sqlite_sequence (name, seq) VALUES ('ref_log', ?)")
                .bind(seq)
                .execute(&mut *self.0)
                .await
                .map_err(storage)?;
        }
        Ok(())
    }

    async fn commit(self: Box<Self>) -> Result<()> {
        self.0.commit().await.map_err(storage)
    }
}

// --- Postgres ---------------------------------------------------------------

struct PgSource(Transaction<'static, Postgres>);

impl PgSource {
    /// `REPEATABLE READ` takes its snapshot at the first statement, and every
    /// later read sees exactly that state.
    async fn begin(pool: &PgPool) -> Result<Self> {
        let tx = pool
            .begin_with("BEGIN ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            .await
            .map_err(storage)?;
        let mut s = PgSource(tx);
        s.schema_version().await?;
        Ok(s)
    }
}

#[async_trait]
impl Source for PgSource {
    async fn schema_version(&mut self) -> Result<u64> {
        let v: Option<i64> = sqlx::query_scalar("SELECT MAX(version) FROM schema_migrations")
            .fetch_one(&mut *self.0)
            .await
            .map_err(storage)?;
        Ok(v.unwrap_or(0) as u64)
    }

    async fn changeset_dicts(&mut self) -> Result<Vec<DictRow>> {
        sqlx::query_as("SELECT id, created_at, data FROM changeset_dicts ORDER BY id")
            .fetch_all(&mut *self.0)
            .await
            .map_err(storage)
    }

    async fn changesets(&mut self, after: &[u8], limit: usize) -> Result<Vec<Pair>> {
        let rows =
            sqlx::query("SELECT id, data FROM changesets WHERE id > $1 ORDER BY id LIMIT $2")
                .bind(after)
                .bind(limit as i64)
                .fetch_all(&mut *self.0)
                .await
                .map_err(storage)?;
        Ok(rows.into_iter().map(|r| (r.get(0), r.get(1))).collect())
    }

    async fn refs(&mut self) -> Result<Vec<(String, Vec<u8>)>> {
        let rows = sqlx::query("SELECT name, target FROM refs ORDER BY name")
            .fetch_all(&mut *self.0)
            .await
            .map_err(storage)?;
        Ok(rows.into_iter().map(|r| (r.get(0), r.get(1))).collect())
    }

    async fn derived(
        &mut self,
        after: &(String, Vec<u8>),
        limit: usize,
    ) -> Result<Vec<DerivedRow>> {
        let rows = sqlx::query(
            "SELECT kind, key, value FROM derived WHERE (kind, key) > ($1, $2) \
             ORDER BY kind, key LIMIT $3",
        )
        .bind(&after.0)
        .bind(&after.1)
        .bind(limit as i64)
        .fetch_all(&mut *self.0)
        .await
        .map_err(storage)?;
        Ok(rows
            .into_iter()
            .map(|r| (r.get(0), r.get(1), r.get(2)))
            .collect())
    }

    async fn ref_log(&mut self, after_seq: i64, limit: usize) -> Result<Vec<LogRow>> {
        sqlx::query_as(
            "SELECT seq, name, old_target, new_target, time, actor, operation, message \
             FROM ref_log WHERE seq > $1 ORDER BY seq LIMIT $2",
        )
        .bind(after_seq)
        .bind(limit as i64)
        .fetch_all(&mut *self.0)
        .await
        .map_err(storage)
    }

    async fn ref_log_last_seq(&mut self) -> Result<i64> {
        let seq: Option<String> =
            sqlx::query_scalar("SELECT pg_get_serial_sequence('ref_log', 'seq')")
                .fetch_one(&mut *self.0)
                .await
                .map_err(storage)?;
        let Some(seq) = seq else { return Ok(0) };
        // The name comes from the catalog, already quoted where it needs to be.
        let row = sqlx::query(&format!("SELECT last_value, is_called FROM {seq}"))
            .fetch_one(&mut *self.0)
            .await
            .map_err(storage)?;
        let (last, called): (i64, bool) = (row.get(0), row.get(1));
        Ok(if called { last } else { 0 })
    }
}

struct PgSink(Transaction<'static, Postgres>);

impl PgSink {
    async fn begin(pool: &PgPool) -> Result<Self> {
        Ok(PgSink(pool.begin().await.map_err(storage)?))
    }
}

#[async_trait]
impl Sink for PgSink {
    async fn is_empty(&mut self) -> Result<bool> {
        // Writers are excluded so that "empty" stays true until commit. A
        // concurrent server inserting meanwhile would be a deployment error;
        // the lock turns it into a wait, not a corrupt mix.
        sqlx::query("LOCK TABLE changesets, changeset_dicts, refs, derived, ref_log IN ACCESS EXCLUSIVE MODE")
            .execute(&mut *self.0)
            .await
            .map_err(storage)?;
        let n: i64 = sqlx::query_scalar(
            "SELECT (SELECT COUNT(*) FROM (SELECT 1 FROM changesets LIMIT 1) a) \
             + (SELECT COUNT(*) FROM (SELECT 1 FROM refs LIMIT 1) b) \
             + (SELECT COUNT(*) FROM (SELECT 1 FROM derived LIMIT 1) c) \
             + (SELECT COUNT(*) FROM (SELECT 1 FROM ref_log LIMIT 1) d) \
             + (SELECT COUNT(*) FROM (SELECT 1 FROM changeset_dicts LIMIT 1) e)",
        )
        .fetch_one(&mut *self.0)
        .await
        .map_err(storage)?;
        Ok(n == 0)
    }

    async fn clear(&mut self) -> Result<()> {
        for t in [
            "changesets",
            "changeset_dicts",
            "refs",
            "derived",
            "ref_log",
        ] {
            sqlx::query(&format!("DELETE FROM {t}"))
                .execute(&mut *self.0)
                .await
                .map_err(storage)?;
        }
        Ok(())
    }

    async fn put_changeset_dicts(&mut self, rows: &[DictRow]) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let mut q =
            QueryBuilder::<Postgres>::new("INSERT INTO changeset_dicts (id, created_at, data) ");
        q.push_values(rows, |mut b, (id, at, data)| {
            b.push_bind(id).push_bind(at).push_bind(data);
        });
        q.build().execute(&mut *self.0).await.map_err(storage)?;
        Ok(())
    }

    async fn put_changesets(&mut self, rows: &[Pair]) -> Result<()> {
        let mut q = QueryBuilder::<Postgres>::new("INSERT INTO changesets (id, data) ");
        q.push_values(rows, |mut b, (id, data)| {
            b.push_bind(id).push_bind(data);
        });
        q.build().execute(&mut *self.0).await.map_err(storage)?;
        Ok(())
    }

    async fn put_refs(&mut self, rows: &[(String, Vec<u8>)]) -> Result<()> {
        for chunk in rows.chunks(PAGE) {
            let mut q = QueryBuilder::<Postgres>::new("INSERT INTO refs (name, target) ");
            q.push_values(chunk, |mut b, (n, t)| {
                b.push_bind(n).push_bind(t);
            });
            q.build().execute(&mut *self.0).await.map_err(storage)?;
        }
        Ok(())
    }

    async fn put_derived(&mut self, rows: &[DerivedRow]) -> Result<()> {
        let mut q = QueryBuilder::<Postgres>::new("INSERT INTO derived (kind, key, value) ");
        q.push_values(rows, |mut b, (k, key, v)| {
            b.push_bind(k).push_bind(key).push_bind(v);
        });
        q.build().execute(&mut *self.0).await.map_err(storage)?;
        Ok(())
    }

    async fn put_ref_log(&mut self, rows: &[LogRow]) -> Result<()> {
        let mut q = QueryBuilder::<Postgres>::new(
            "INSERT INTO ref_log (seq, name, old_target, new_target, time, actor, operation, message) ",
        );
        q.push_values(rows, |mut b, r| {
            b.push_bind(r.0)
                .push_bind(&r.1)
                .push_bind(&r.2)
                .push_bind(&r.3)
                .push_bind(r.4)
                .push_bind(&r.5)
                .push_bind(&r.6)
                .push_bind(&r.7);
        });
        q.build().execute(&mut *self.0).await.map_err(storage)?;
        Ok(())
    }

    async fn set_ref_log_last_seq(&mut self, seq: i64) -> Result<()> {
        // `is_called = false` makes the next value `1` for an empty log.
        sqlx::query("SELECT setval(pg_get_serial_sequence('ref_log', 'seq'), $1, $2)")
            .bind(seq.max(1))
            .bind(seq > 0)
            .execute(&mut *self.0)
            .await
            .map_err(storage)?;
        Ok(())
    }

    async fn commit(self: Box<Self>) -> Result<()> {
        self.0.commit().await.map_err(storage)
    }
}
