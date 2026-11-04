//! Throwaway Postgres schemas and databases for tests, here and in the
//! crates that test against `LEEP_TEST_POSTGRES_URL` (leep-server, the
//! end-to-end suite, the soak harness). Each is dropped when its guard goes
//! out of scope, a panicking test's included: a schema left per test and run
//! filled a shared test database with thousands of them.
//!
//! The drop runs on a thread of its own with its own runtime, so it works
//! from `Drop` inside a test's runtime, and never panics while a test is
//! already unwinding (that would abort the whole test binary).

use std::future::Future;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use sqlx::{AssertSqlSafe, Connection, PgConnection};

/// The variable naming the server tests may create schemas and databases on.
pub const URL_VAR: &str = "LEEP_TEST_POSTGRES_URL";

/// The admin URL, or `None` after printing a note when it is unset.
pub fn admin_url() -> Option<String> {
    let url = std::env::var(URL_VAR).ok();
    if url.is_none() {
        eprintln!("skipping: {URL_VAR} not set");
    }
    url
}

/// `{prefix}_{pid}_{n}`: unique among the guards of this process, so two
/// tests never share one, and among live processes by the pid.
fn unique(prefix: &str) -> String {
    static NEXT: AtomicU32 = AtomicU32::new(0);
    let name = format!(
        "{prefix}_{}_{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    );
    assert!(
        name.len() <= 63
            && name
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_'),
        "not a plain Postgres identifier: {name}"
    );
    name
}

/// Runs `f` to completion on a thread and runtime of its own.
fn blocking<T: Send + 'static>(f: impl Future<Output = T> + Send + 'static) -> T {
    std::thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("cleanup runtime")
            .block_on(f)
    })
    .join()
    .unwrap_or_else(|p| std::panic::resume_unwind(p))
}

async fn exec(admin: &str, sql: &[String]) -> Result<(), sqlx::Error> {
    let mut c = PgConnection::connect(admin).await?;
    for s in sql {
        sqlx::query(AssertSqlSafe(s.as_str()))
            .execute(&mut c)
            .await?;
    }
    c.close().await
}

/// Reports a failed drop: a panic that fails the test when it is not
/// already failing, a note otherwise.
fn cleanup_failed(what: &str, e: impl std::fmt::Display) {
    if std::thread::panicking() {
        eprintln!("could not drop {what}: {e}");
    } else {
        panic!("could not drop {what}: {e}");
    }
}

/// `admin` with only the database name swapped, so user, host, port and
/// query parameters (a Unix-socket `host=`) carry over. A URL with no path
/// gets one, and a `dbname=` query parameter (which sqlx lets override the
/// path) is replaced too so the new name wins.
pub fn replace_db(admin: &str, db: &str) -> String {
    let (base, query) = match admin.split_once('?') {
        Some((b, q)) => (b, Some(q)),
        None => (admin, None),
    };
    let after_scheme = base.find("://").map_or(0, |i| i + 3);
    let authority_end = base[after_scheme..]
        .find('/')
        .map_or(base.len(), |i| after_scheme + i);
    let root = &base[..authority_end];
    let mut out = format!("{root}/{db}");
    if let Some(q) = query {
        let q = q
            .split('&')
            .map(|kv| match kv.split_once('=') {
                Some(("dbname", _)) => format!("dbname={db}"),
                _ => kv.to_string(),
            })
            .collect::<Vec<_>>()
            .join("&");
        out.push('?');
        out.push_str(&q);
    }
    out
}

/// A schema of its own in the admin URL's database, for a test that opens
/// stores with `connect_with_schema*`. Dropped with everything in it.
pub struct TestSchema {
    admin: String,
    pub name: String,
    /// The admin URL with the schema's name as `application_name`, so the
    /// drop can end the connections of stores the test left open: one idle
    /// in a transaction would hold its locks and the drop would wait forever.
    pub url: String,
}

impl TestSchema {
    /// A fresh `{prefix}_{pid}_{n}` schema, or `None` when
    /// `LEEP_TEST_POSTGRES_URL` is unset.
    pub fn create(prefix: &str) -> Option<TestSchema> {
        let admin = admin_url()?;
        let name = unique(prefix);
        // The name of a killed earlier process with this pid may be left.
        let sql = vec![
            format!("DROP SCHEMA IF EXISTS {name} CASCADE"),
            format!("CREATE SCHEMA {name}"),
        ];
        let a = admin.clone();
        blocking(async move { exec(&a, &sql).await })
            .unwrap_or_else(|e| panic!("creating schema {name}: {e}"));
        let sep = if admin.contains('?') { '&' } else { '?' };
        let url = format!("{admin}{sep}application_name={name}");
        Some(TestSchema { admin, name, url })
    }

    fn drop_now(&self) -> Result<(), sqlx::Error> {
        let (admin, name) = (self.admin.clone(), self.name.clone());
        blocking(async move {
            let mut c = PgConnection::connect(&admin).await?;
            let mut tries = 0;
            loop {
                sqlx::query(
                    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
                     WHERE application_name = $1 AND pid <> pg_backend_pid()",
                )
                .bind(&name)
                .execute(&mut c)
                .await?;
                sqlx::query("SET lock_timeout = '5s'")
                    .execute(&mut c)
                    .await?;
                let dropped = sqlx::query(AssertSqlSafe(format!(
                    "DROP SCHEMA IF EXISTS {name} CASCADE"
                )))
                .execute(&mut c)
                .await;
                match dropped {
                    // A connection opened after the terminate, by a pool
                    // still alive, may hold a lock; end it and try again.
                    Err(sqlx::Error::Database(e))
                        if e.code().as_deref() == Some("55P03") && tries < 5 =>
                    {
                        tries += 1;
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                    r => break r.map(drop),
                }
            }?;
            c.close().await
        })
    }
}

impl Drop for TestSchema {
    fn drop(&mut self) {
        if let Err(e) = self.drop_now() {
            cleanup_failed(&format!("schema {}", self.name), e);
        }
    }
}

/// A database of its own, for a test that opens a store by URL alone (and
/// so in its `public` schema), or runs a server on it.
pub struct TestDb {
    admin: String,
    pub name: String,
    pub url: String,
    keep: bool,
}

impl TestDb {
    /// A fresh `{prefix}_{pid}_{n}` database, or `None` when
    /// `LEEP_TEST_POSTGRES_URL` is unset.
    pub fn create(prefix: &str) -> Option<TestDb> {
        let admin = admin_url()?;
        Some(Self::create_in(&admin, prefix).unwrap_or_else(|e| panic!("creating a database: {e}")))
    }

    /// A fresh `{prefix}_{pid}_{n}` database on the server of `admin`.
    pub fn create_in(admin: &str, prefix: &str) -> Result<TestDb, sqlx::Error> {
        let name = unique(prefix);
        // The name of a killed earlier process with this pid may be left.
        let sql = vec![
            format!("DROP DATABASE IF EXISTS {name} WITH (FORCE)"),
            format!("CREATE DATABASE {name}"),
        ];
        let a = admin.to_string();
        blocking(async move { exec(&a, &sql).await })?;
        Ok(TestDb {
            admin: admin.to_string(),
            url: replace_db(admin, &name),
            name,
            keep: false,
        })
    }

    /// Leaves the database in place when the guard drops, for a run kept
    /// for inspection.
    pub fn keep(&mut self) {
        self.keep = true;
    }

    /// Drops the database now, returning the error that a drop by `Drop`
    /// would panic with.
    pub fn close(mut self) -> Result<(), sqlx::Error> {
        self.keep = true;
        self.drop_now()
    }

    fn drop_now(&self) -> Result<(), sqlx::Error> {
        let admin = self.admin.clone();
        // FORCE ends the connections a test or its servers left open.
        let sql = vec![format!(
            "DROP DATABASE IF EXISTS {} WITH (FORCE)",
            self.name
        )];
        blocking(async move { exec(&admin, &sql).await })
    }
}

impl Drop for TestDb {
    fn drop(&mut self) {
        if self.keep {
            return;
        }
        if let Err(e) = self.drop_now() {
            cleanup_failed(&format!("database {}", self.name), e);
        }
    }
}

/// Whether a schema named `name` exists in the admin URL's database, for
/// tests that the guards leave nothing behind.
pub fn schema_exists(admin: &str, name: &str) -> bool {
    let (admin, name) = (admin.to_string(), name.to_string());
    blocking(async move {
        let mut c = PgConnection::connect(&admin).await?;
        let n: i64 = sqlx::query_scalar("SELECT count(*) FROM pg_namespace WHERE nspname = $1")
            .bind(&name)
            .fetch_one(&mut c)
            .await?;
        Ok::<_, sqlx::Error>(n > 0)
    })
    .unwrap()
}

/// Whether a database named `name` exists on the admin URL's server.
pub fn database_exists(admin: &str, name: &str) -> bool {
    let (admin, name) = (admin.to_string(), name.to_string());
    blocking(async move {
        let mut c = PgConnection::connect(&admin).await?;
        let n: i64 = sqlx::query_scalar("SELECT count(*) FROM pg_database WHERE datname = $1")
            .bind(&name)
            .fetch_one(&mut c)
            .await?;
        Ok::<_, sqlx::Error>(n > 0)
    })
    .unwrap()
}
