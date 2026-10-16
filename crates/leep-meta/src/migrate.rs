//! Versioned, forward-only schema migrations (ADR 0023).
//!
//! A backend ships an ordered, append-only list of [`Migration`]s compiled into
//! the binary. The `schema_migrations` table records which ones a database has
//! had applied, with a checksum of each one's SQL. The driver below is shared
//! by both backends; each supplies a [`Session`] over one transaction.

use std::collections::BTreeSet;

use async_trait::async_trait;
use leep_core::{Error, Hash, Result};

/// One schema change. Once released, never edited, reordered or removed.
#[derive(Debug, Clone, Copy)]
pub struct Migration {
    /// 1-based, contiguous, strictly increasing within a list.
    pub version: i64,
    pub name: &'static str,
    /// One or more statements, run in a single transaction.
    pub sql: &'static str,
    /// Tables and indexes whose presence shows an un-versioned database
    /// already has this migration's effect (adoption). Empty for every
    /// migration after the baseline: those never adopt.
    pub satisfied_by: &'static [&'static str],
}

impl Migration {
    /// BLAKE3 of the SQL with line endings normalized, so a checkout with
    /// CRLF does not look like an edited migration.
    pub fn checksum(&self) -> String {
        Hash::of_blob(self.sql.replace("\r\n", "\n").as_bytes()).to_hex()
    }
}

/// A row of `schema_migrations`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppliedMigration {
    pub version: i64,
    pub name: String,
    /// Unix seconds.
    pub applied_at: i64,
    pub checksum: String,
}

/// A migration a run would apply, or did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MigrationRef {
    pub version: i64,
    pub name: &'static str,
    pub sql: &'static str,
}

impl From<&Migration> for MigrationRef {
    fn from(m: &Migration) -> Self {
        Self {
            version: m.version,
            name: m.name,
            sql: m.sql,
        }
    }
}

/// What a migration run found, and (when `performed`) did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrationReport {
    /// Rows of `schema_migrations` before the run, oldest first.
    pub applied: Vec<AppliedMigration>,
    /// Migrations an un-versioned database already satisfies; recorded
    /// without running their SQL.
    pub adopted: Vec<MigrationRef>,
    /// Migrations whose SQL runs.
    pub pending: Vec<MigrationRef>,
    /// Newest applied version before the run (0 = none).
    pub from: u64,
    /// Newest applied version after it; equals `from` unless `performed`.
    pub to: u64,
    /// The newest version this binary knows.
    pub latest: u64,
    /// False for a read-only look (`--status`, `--dry-run`, `--no-migrate`).
    pub performed: bool,
}

impl MigrationReport {
    /// Nothing to apply or record.
    pub fn is_current(&self) -> bool {
        self.adopted.is_empty() && self.pending.is_empty()
    }
}

/// One transaction on a database, as the driver needs it.
#[async_trait]
pub(crate) trait Session: Send {
    /// Names of the tables and indexes in the current schema.
    async fn objects(&mut self) -> Result<BTreeSet<String>>;
    async fn applied(&mut self) -> Result<Vec<AppliedMigration>>;
    async fn create_table(&mut self) -> Result<()>;
    async fn execute(&mut self, sql: &'static str) -> Result<()>;
    async fn record(&mut self, m: &AppliedMigration) -> Result<()>;
}

pub(crate) const TABLE: &str = "schema_migrations";

fn refuse(msg: String) -> Error {
    Error::Other(msg)
}

fn check_list(migs: &[Migration]) -> Result<()> {
    for (i, m) in migs.iter().enumerate() {
        if m.version != i as i64 + 1 {
            return Err(Error::Other(format!(
                "migration list is not 1, 2, 3...: {} is at position {}",
                m.version,
                i + 1
            )));
        }
    }
    Ok(())
}

pub(crate) fn latest(migs: &[Migration]) -> u64 {
    migs.last().map_or(0, |m| m.version as u64)
}

struct Plan {
    adopt: Vec<usize>,
    run: Vec<usize>,
}

/// Decide what to do, or refuse: a schema this binary does not understand
/// must never be written to.
fn plan(
    migs: &[Migration],
    applied: &[AppliedMigration],
    objects: &BTreeSet<String>,
) -> Result<Plan> {
    let newest = latest(migs) as i64;
    let max_applied = applied.iter().map(|a| a.version).max().unwrap_or(0);
    if max_applied > newest {
        return Err(refuse(format!(
            "database schema v{max_applied} is newer than this server (max v{newest}); \
             upgrade the server"
        )));
    }
    for a in applied {
        let m = migs
            .iter()
            .find(|m| m.version == a.version)
            .ok_or_else(|| refuse(format!("database records unknown migration v{}", a.version)))?;
        if m.checksum() != a.checksum {
            return Err(refuse(format!(
                "migration v{} ({}) differs from the one applied to this database \
                 (recorded checksum {}, compiled {}); applied migrations are immutable",
                a.version,
                a.name,
                a.checksum,
                m.checksum()
            )));
        }
    }
    let have: BTreeSet<i64> = applied.iter().map(|a| a.version).collect();
    let pending: Vec<usize> = (0..migs.len())
        .filter(|&i| !have.contains(&migs[i].version))
        .collect();
    if let Some(&i) = pending.iter().find(|&&i| migs[i].version < max_applied) {
        return Err(refuse(format!(
            "migration v{} is missing but v{max_applied} is applied",
            migs[i].version
        )));
    }
    // Adoption: only a database with no history, and only while every
    // migration so far is visibly in place. The first one that is not, and
    // everything after it, runs.
    let legacy = applied.is_empty()
        && migs
            .iter()
            .flat_map(|m| m.satisfied_by)
            .any(|n| objects.contains(*n));
    let mut plan = Plan {
        adopt: vec![],
        run: vec![],
    };
    let mut adopting = legacy;
    for i in pending {
        let m = &migs[i];
        adopting = adopting
            && !m.satisfied_by.is_empty()
            && m.satisfied_by.iter().all(|n| objects.contains(*n));
        if adopting {
            plan.adopt.push(i);
        } else {
            plan.run.push(i);
        }
    }
    Ok(plan)
}

/// Run inside a transaction the caller opened (and, in Postgres, locked).
/// With `write` false it only reads. An error leaves the caller to roll back.
pub(crate) async fn drive(
    s: &mut dyn Session,
    migs: &[Migration],
    write: bool,
) -> Result<MigrationReport> {
    check_list(migs)?;
    let objects = s.objects().await?;
    let applied = if objects.contains(TABLE) {
        s.applied().await?
    } else {
        vec![]
    };
    let plan = plan(migs, &applied, &objects)?;
    let from = applied.iter().map(|a| a.version).max().unwrap_or(0) as u64;
    let latest = latest(migs);
    let refs = |ix: &[usize]| ix.iter().map(|&i| MigrationRef::from(&migs[i])).collect();
    let mut report = MigrationReport {
        adopted: refs(&plan.adopt),
        pending: refs(&plan.run),
        applied,
        from,
        to: from,
        latest,
        performed: false,
    };
    if !write || report.is_current() {
        return Ok(report);
    }
    s.create_table().await?;
    let now = leep_core::store::unix_now();
    // Migrations in version order, adopted ones first by construction.
    let mut order: Vec<(usize, bool)> = plan.adopt.iter().map(|&i| (i, false)).collect();
    order.extend(plan.run.iter().map(|&i| (i, true)));
    for (i, run) in order {
        let m = &migs[i];
        if run {
            tracing::info!(
                version = m.version,
                name = m.name,
                "applying schema migration"
            );
            s.execute(m.sql).await.map_err(|e| {
                Error::Storage(format!("migration v{} ({}) failed: {e}", m.version, m.name))
            })?;
        } else {
            tracing::info!(
                version = m.version,
                name = m.name,
                "adopting existing schema"
            );
        }
        s.record(&AppliedMigration {
            version: m.version,
            name: m.name.to_string(),
            applied_at: now,
            checksum: m.checksum(),
        })
        .await?;
    }
    report.to = latest;
    report.performed = true;
    Ok(report)
}

/// `--no-migrate`: a server may start only on a fully migrated database.
pub(crate) fn require_current(report: &MigrationReport) -> Result<()> {
    if report.is_current() {
        return Ok(());
    }
    let n = report.adopted.len() + report.pending.len();
    let first = report
        .adopted
        .iter()
        .chain(&report.pending)
        .map(|m| m.version)
        .min()
        .unwrap_or(0);
    Err(refuse(format!(
        "{n} schema migration(s) pending (database is at v{}, this server needs v{}, \
         first pending v{first}); run `leep-server migrate`, or start without --no-migrate",
        report.from, report.latest
    )))
}
