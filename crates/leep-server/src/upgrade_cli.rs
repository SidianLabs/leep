//! `leep-server upgrade-index`: the "Upgrading" steps for derived indexes
//! (OPERATIONS.md) as one command. It plans from counts, derives what this
//! version reads and is missing, drops retired kinds only when told that no
//! older server reads them, and prints how to give the space back. It is an
//! orchestration of `repair rederive --missing` and `repair drop`; see
//! `leep_derive::integrity::upgrade`.
//!
//! Exit codes: 0 done or nothing to do, 1 the plan has work (without
//! `--yes`) or a run left work undone, 2 could not run.

use std::path::{Path, PathBuf};

use anyhow::Context;
use clap::Args;
use leep_derive::integrity::{
    self, plan_upgrade, run_upgrade, CurrentKind, DropReport, IndexStatus, UpgradeOptions,
    UpgradePlan, UpgradeReport,
};
use leep_meta::space::{Backend, StorageFacts};
use serde::Serialize;

use crate::repair::{bytes_human, open, print_json, Stderr, EXIT_COULD_NOT_RUN};

const EXIT_DONE: i32 = 0;
const EXIT_WORK: i32 = 1;

#[derive(Args)]
pub struct UpgradeIndexArgs {
    /// Print what would happen and change nothing (the default without
    /// --yes). Exit status 0 when there is nothing to do, 1 when there is.
    #[arg(long, conflicts_with = "yes")]
    plan: bool,
    /// Carry out the plan. Safe beside running servers and resumable: stop
    /// it at any point and run it again.
    #[arg(long)]
    yes: bool,
    /// Changesets derived at once (ADR 0050). Default: the cores, up to 8.
    #[arg(long, value_parser = clap::value_parser!(u32).range(1..=256))]
    jobs: Option<u32>,
    /// Also delete the rows of retired kinds (such as history-tree-v3), after
    /// the current ones are complete. Requires --old-servers-stopped.
    #[arg(long, requires = "old_servers_stopped")]
    drop_retired: bool,
    /// Acknowledge that no server of an older version runs against this
    /// store any more: those servers read the retired kinds.
    #[arg(long)]
    old_servers_stopped: bool,
    /// Drop the retired kinds before deriving, for a disk that cannot hold
    /// both; queries derive on demand until the run has caught up.
    #[arg(long, requires = "drop_retired")]
    drop_first: bool,
    /// After dropping, run `VACUUM (ANALYZE) derived` (Postgres; it does not
    /// block reads or writes) so the freed space is reused at once.
    #[arg(long)]
    vacuum: bool,
    /// Start even when the free disk space looks smaller than the estimate.
    #[arg(long)]
    skip_space_check: bool,
    #[arg(long)]
    json: bool,
}

/// Changesets a second one job derives, with `--missing`, on Postgres
/// (PERFORMANCE.md, "Parallel derivation"; the cheap kinds are rough), and
/// value bytes per changeset when a kind has no rows to sample (Linux:
/// PERFORMANCE.md, "All of Linux", "Index size: packs", "Filtered log").
fn kind_profile(kind: &str) -> (f64, u64) {
    match kind {
        "history-tree-v4" => (150.0, 2_800),
        "log-filter-v1" => (1_500.0, 900),
        "skiplist" => (2_000.0, 600),
        "root_tree" => (500.0, 40),
        _ => (2_000.0, 16),
    }
}

/// Postgres tuple and primary-key entry per row, roughly.
const ROW_OVERHEAD: u64 = 120;
/// Rows a second `repair drop` deletes: SQLite and cold Postgres at the low
/// end, a warm cache at the high (PERFORMANCE.md, "Dropping a derived kind").
const DROP_RATE: (f64, f64) = (25_000.0, 140_000.0);

#[derive(Debug, Default, Serialize)]
struct Estimates {
    /// Changesets to derive, summed over kinds.
    derive_rows: u64,
    /// With every job on a free core / with one free core.
    derive_secs: (f64, f64),
    derive_bytes: u64,
    drop_rows: u64,
    drop_value_bytes: u64,
    drop_secs: (f64, f64),
    database_bytes: Option<u64>,
    derived_table_bytes: Option<u64>,
    data_dir: Option<PathBuf>,
    free_bytes: Option<u64>,
    /// The free space is known and smaller than the estimate.
    short: bool,
    warnings: Vec<String>,
}

fn estimate(
    plan: &UpgradePlan,
    facts: Option<&StorageFacts>,
    free: Option<u64>,
    jobs: usize,
    cores: usize,
    drop_first: bool,
) -> Estimates {
    let mut e = Estimates::default();
    let parallel = jobs.min(cores).max(1) as f64;
    for k in plan
        .current
        .iter()
        .filter(|k| k.status != IndexStatus::Complete)
    {
        let (rate, default_bytes) = kind_profile(k.kind);
        let per = k.value_bytes_per_changeset.unwrap_or(default_bytes);
        e.derive_rows += k.missing;
        e.derive_bytes += k.missing * (per + ROW_OVERHEAD);
        e.derive_secs.0 += k.missing as f64 / (rate * parallel);
        e.derive_secs.1 += k.missing as f64 / rate;
    }
    for r in &plan.retired {
        e.drop_rows += r.total_rows();
        e.drop_value_bytes += r.value_bytes.unwrap_or(0);
    }
    e.drop_secs = (
        e.drop_rows as f64 / DROP_RATE.1,
        e.drop_rows as f64 / DROP_RATE.0,
    );
    if let Some(f) = facts {
        e.database_bytes = f.database_bytes;
        e.derived_table_bytes = f.derived_bytes;
        e.data_dir = f.data_dir.clone();
    }
    e.free_bytes = free;
    // Postgres keeps deleted rows' space until a vacuum, and a dropped
    // kind's space is only reused by later rows after one.
    let reusable = if drop_first {
        e.drop_value_bytes
    } else {
        facts.and_then(|f| f.reusable_bytes).unwrap_or(0)
    };
    let need = e.derive_bytes.saturating_sub(reusable);
    if let Some(free) = free {
        if need > free {
            e.short = true;
            e.warnings.push(format!(
                "the missing rows need about {} and only {} is free{}; free space, or drop the \
                 retired rows first (--drop-retired --old-servers-stopped --drop-first --vacuum)",
                bytes_human(need),
                bytes_human(free),
                at(&e.data_dir)
            ));
        } else if need.saturating_mul(3) / 2 > free {
            e.warnings.push(format!(
                "free space is tight: about {} needed, {} free{}",
                bytes_human(need),
                bytes_human(free),
                at(&e.data_dir)
            ));
        }
    }
    e
}

fn at(dir: &Option<PathBuf>) -> String {
    dir.as_ref()
        .map(|d| format!(" on {}", d.display()))
        .unwrap_or_default()
}

/// Space available to an unprivileged user on the filesystem holding
/// `dir`, from `df` (no unsafe code, no new dependency); `None` when it
/// cannot tell.
fn free_bytes(dir: &Path) -> Option<u64> {
    let out = std::process::Command::new("df")
        .args(["-Pk"])
        .arg(dir)
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    parse_df(&String::from_utf8_lossy(&out.stdout))
}

fn parse_df(out: &str) -> Option<u64> {
    let line = out.lines().nth(1)?;
    let avail: u64 = line.split_whitespace().nth(3)?.parse().ok()?;
    Some(avail * 1024)
}

/// "42s", "18m", "2h45m"
fn human_secs(s: f64) -> String {
    let s = s.round() as u64;
    match s {
        0..=99 => format!("{s}s"),
        100..=5999 => format!("{}m", (s + 30) / 60),
        _ => {
            let m = (s + 30) / 60;
            format!("{}h{:02}m", m / 60, m % 60)
        }
    }
}

fn span(secs: (f64, f64)) -> String {
    let (a, b) = (human_secs(secs.0), human_secs(secs.1));
    if a == b {
        format!("about {a}")
    } else {
        format!("roughly {a} to {b}")
    }
}

fn status_word(k: &CurrentKind) -> &'static str {
    match k.status {
        IndexStatus::Complete => "complete",
        IndexStatus::Missing => "missing",
        IndexStatus::Partial => "partial",
    }
}

fn reclaim_hint(backend: Option<Backend>, facts: Option<&StorageFacts>) -> Vec<String> {
    match backend {
        Some(Backend::Postgres) => vec![
            "Postgres reuses the freed pages once `derived` is vacuumed: autovacuum does it soon \
             after a large drop, or run `VACUUM (ANALYZE) derived` now (or pass --vacuum); \
             neither blocks servers."
                .into(),
            "To give the space back to the operating system, in a maintenance window run \
             `VACUUM FULL derived` (it locks the table and needs room for a copy of what \
             remains), or `pg_repack --table=derived` (no long lock); then `ANALYZE derived`."
                .into(),
        ],
        Some(Backend::Sqlite) => {
            let file = facts
                .and_then(|f| f.file.as_ref())
                .map(|d| d.display().to_string())
                .unwrap_or_else(|| "<meta.db>".into());
            vec![
                "SQLite reuses the freed pages for new rows.".into(),
                format!(
                    "To shrink the file, stop the servers and run `sqlite3 {file} \
                     VACUUM` (it locks the database and needs room for a copy)."
                ),
            ]
        }
        _ => Vec::new(),
    }
}

fn print_plan(
    plan: &UpgradePlan,
    e: &Estimates,
    args: &UpgradeIndexArgs,
    jobs: usize,
    backend: Option<Backend>,
    facts: Option<&StorageFacts>,
) {
    println!(
        "upgrade-index: {} ref(s), {} changesets reachable (read in {:.1}s)",
        plan.refs, plan.reachable, plan.elapsed_secs
    );
    if plan.unreadable > 0 {
        println!(
            "note: {} changeset(s) could not be read and are left out; `leep-server verify` \
             names them",
            plan.unreadable
        );
    }
    println!();
    println!(
        "current indexes        {:>12} {:>12}  status",
        "rows", "missing"
    );
    for k in &plan.current {
        let size = match (k.status, k.value_bytes_per_changeset) {
            (IndexStatus::Complete, _) => String::new(),
            (_, per) => format!(
                "  needs about {}",
                bytes_human(k.missing * (per.unwrap_or(kind_profile(k.kind).1) + ROW_OVERHEAD))
            ),
        };
        println!(
            "  {:<20} {:>12} {:>12}  {}{size}",
            k.kind,
            k.rows,
            k.missing,
            status_word(k)
        );
    }
    if !plan.retired.is_empty() {
        println!();
        println!("retired indexes (servers older than this one still read them)");
        for r in &plan.retired {
            let parts: Vec<String> = r.rows.iter().map(|(k, n)| format!("{k} {n}")).collect();
            println!(
                "  {:<20} {} rows ({}), {}",
                r.kind,
                r.total_rows(),
                parts.join(", "),
                r.value_bytes.map_or("size unknown".into(), |b| format!(
                    "about {} of values",
                    bytes_human(b)
                ))
            );
        }
    }
    println!();
    let mut storage = Vec::new();
    match backend {
        Some(Backend::Postgres) => storage.push("postgres".to_string()),
        Some(Backend::Sqlite) => storage.push("sqlite".to_string()),
        Some(Backend::Memory) => storage.push("in memory".to_string()),
        None => storage.push("sizes unknown".to_string()),
    }
    if let Some(b) = e.database_bytes {
        storage.push(format!("database {}", bytes_human(b)));
    }
    if let Some(b) = e.derived_table_bytes {
        storage.push(format!("derived table {}", bytes_human(b)));
    }
    match (e.free_bytes, &e.data_dir) {
        (Some(f), dir) => storage.push(format!("{} free{}", bytes_human(f), at(dir))),
        (None, _) if backend == Some(Backend::Postgres) => storage.push(
            "free space unknown (the database's data directory is not visible from here)".into(),
        ),
        _ => {}
    }
    println!("storage: {}", storage.join(", "));
    for w in &e.warnings {
        println!("warning: {w}");
    }
    if plan.nothing_to_do() {
        return;
    }
    println!();
    println!("steps:");
    let mut n = 0;
    let to_drop = plan.to_drop();
    let drop_names = || {
        plan.retired
            .iter()
            .flat_map(|r| r.rows.keys().cloned())
            .collect::<Vec<_>>()
            .join(" ")
    };
    let drop_line = |n: usize| {
        let mut s = format!(
            "  {n}. drop {} ({} rows, {}); safe beside servers of this version",
            drop_names(),
            e.drop_rows,
            span(e.drop_secs)
        );
        if backend == Some(Backend::Postgres) {
            if let Some(t) = e.derived_table_bytes {
                s += &format!(
                    "\n     it reads `derived` once and writes about as much WAL ({})",
                    bytes_human(t)
                );
            }
        }
        if !args.drop_retired {
            s += "\n     skipped: add --drop-retired --old-servers-stopped once no older server \
                  runs against this store";
        }
        s
    };
    if !to_drop.is_empty() && args.drop_first {
        n += 1;
        println!("{}", drop_line(n));
    }
    let derive = plan.to_derive();
    if !derive.is_empty() {
        n += 1;
        println!(
            "  {n}. derive the missing rows of {} with {jobs} job(s), as `repair rederive \
             --missing` does; resumable",
            derive.join(" ")
        );
        println!(
            "     about {} changeset rows, {} ({jobs} free core(s) to one); needs about {}",
            e.derive_rows,
            span(e.derive_secs),
            bytes_human(e.derive_bytes)
        );
    }
    if !to_drop.is_empty() && !args.drop_first {
        n += 1;
        println!("{}", drop_line(n));
    }
    if !to_drop.is_empty() {
        n += 1;
        println!("  {n}. reclaim space:");
        for line in reclaim_hint(backend, facts) {
            println!("     {line}");
        }
    }
    if !args.yes {
        let mut cmd = format!("leep-server upgrade-index --yes --jobs {jobs}");
        if !to_drop.is_empty() {
            cmd += if args.drop_retired {
                " --drop-retired --old-servers-stopped"
            } else {
                " [--drop-retired --old-servers-stopped]"
            };
        }
        println!();
        println!("run it: {cmd}");
        println!(
            "separately, once no older server or tool uses the store: changeset rows can be \
         compressed (`leep-server compact-changesets --dry-run`, ADR 0053)"
        );
    }
}

fn print_nothing(plan: &UpgradePlan) {
    println!(
        "upgrade-index: nothing to do: every index this version reads is complete for the {} \
         changesets reachable from {} ref(s), and no retired rows remain",
        plan.reachable, plan.refs
    );
}

fn print_report(
    r: &UpgradeReport,
    first: Option<&DropReport>,
    vacuumed: bool,
    backend: Option<Backend>,
    facts: Option<&StorageFacts>,
) {
    println!();
    println!("upgrade-index: done in {:.1}s", r.elapsed_secs);
    for (kind, n) in &r.derived {
        println!("  derived {n} {kind} row(s)");
    }
    let mut dropped = false;
    for d in first.into_iter().chain(r.dropped.as_ref()) {
        for (kind, n) in &d.rows {
            println!("  dropped {n} {kind} row(s)");
            dropped = true;
        }
    }
    if vacuumed {
        println!("  ran VACUUM (ANALYZE) derived");
    }
    let left = r.after.to_derive();
    if !left.is_empty() {
        println!(
            "still incomplete: {}; run again, and `leep-server verify` if it stays so",
            left.join(" ")
        );
    }
    if !r.after.retired.is_empty() {
        println!(
            "retired rows remain: {}; once no older server runs against this store, run \
             `leep-server upgrade-index --yes --drop-retired --old-servers-stopped`",
            r.after.to_drop().join(" ")
        );
    }
    if dropped {
        println!("reclaim space:");
        for line in reclaim_hint(backend, facts) {
            println!("  {line}");
        }
    }
    if left.is_empty() && r.after.retired.is_empty() {
        println!("the indexes are up to date");
    }
}

#[derive(Serialize)]
struct PlanJson<'a> {
    nothing_to_do: bool,
    plan: &'a UpgradePlan,
    estimates: &'a Estimates,
}

#[derive(Serialize)]
struct RunJson<'a> {
    plan: &'a UpgradePlan,
    estimates: &'a Estimates,
    dropped_first: Option<&'a DropReport>,
    vacuumed: bool,
    report: Option<&'a UpgradeReport>,
}

pub async fn run_upgrade_index(meta: &str, blobs: &str, args: UpgradeIndexArgs) -> i32 {
    match inner(meta, blobs, args).await {
        Ok(code) => code,
        Err(e) => {
            eprintln!("error: {e:#}");
            EXIT_COULD_NOT_RUN
        }
    }
}

async fn inner(meta: &str, blobs: &str, args: UpgradeIndexArgs) -> anyhow::Result<i32> {
    let jobs = args
        .jobs
        .map_or_else(leep_derive::default_derive_jobs, |j| j as usize);
    let cores = std::thread::available_parallelism().map_or(1, |n| n.get());
    let repo = open(meta, blobs).await?;
    let plan = plan_upgrade(&repo, &Stderr)
        .await
        .context("planning the upgrade")?;
    let facts = match leep_meta::space::storage_facts(meta).await {
        Ok(f) => Some(f),
        Err(e) => {
            eprintln!("upgrade-index: could not read the store's size: {e}");
            None
        }
    };
    let backend = facts.as_ref().map(|f| f.backend);
    let free = facts
        .as_ref()
        .and_then(|f| f.data_dir.as_deref())
        .and_then(free_bytes);
    let est = estimate(&plan, facts.as_ref(), free, jobs, cores, args.drop_first);

    if !args.yes || plan.nothing_to_do() {
        let code = if plan.nothing_to_do() {
            EXIT_DONE
        } else {
            EXIT_WORK
        };
        if args.json {
            print_json(
                true,
                code,
                &PlanJson {
                    nothing_to_do: plan.nothing_to_do(),
                    plan: &plan,
                    estimates: &est,
                },
            )?;
        } else if plan.nothing_to_do() {
            print_nothing(&plan);
        } else {
            print_plan(&plan, &est, &args, jobs, backend, facts.as_ref());
        }
        return Ok(code);
    }

    if !args.json {
        print_plan(&plan, &est, &args, jobs, backend, facts.as_ref());
    }
    if est.short && !args.skip_space_check {
        anyhow::bail!(
            "not starting: the disk looks too small for the missing rows (see the warning); \
             free space, use --drop-first, or pass --skip-space-check"
        );
    }
    let mut dropped_first = None;
    let mut vacuumed = false;
    if args.drop_retired && args.drop_first && !plan.to_drop().is_empty() {
        dropped_first = Some(integrity::drop_kinds(&repo, &plan.to_drop(), false, &Stderr).await?);
        if args.vacuum {
            vacuumed |= leep_meta::space::vacuum_analyze_derived(meta).await?;
        }
    }
    let opts = UpgradeOptions {
        jobs,
        drop_retired: args.drop_retired && !args.drop_first,
    };
    let report = run_upgrade(&repo, &plan, &opts, &Stderr).await?;
    if args.vacuum && report.dropped.is_some() {
        vacuumed |= leep_meta::space::vacuum_analyze_derived(meta).await?;
    }
    let code = if report.after.to_derive().is_empty() {
        EXIT_DONE
    } else {
        EXIT_WORK
    };
    if args.json {
        print_json(
            code == EXIT_DONE,
            code,
            &RunJson {
                plan: &plan,
                estimates: &est,
                dropped_first: dropped_first.as_ref(),
                vacuumed,
                report: Some(&report),
            },
        )?;
    } else {
        print_report(
            &report,
            dropped_first.as_ref(),
            vacuumed,
            backend,
            facts.as_ref(),
        );
    }
    Ok(code)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn df_output_is_parsed() {
        let out = "Filesystem     1024-blocks      Used Available Capacity Mounted on\n\
                   /dev/vda         263174212  37748736   3355443      92% /\n";
        assert_eq!(parse_df(out), Some(3_355_443 * 1024));
        assert_eq!(parse_df("nonsense"), None);
    }

    #[test]
    fn durations_read_as_people_say_them() {
        assert_eq!(human_secs(42.4), "42s");
        assert_eq!(human_secs(1_080.0), "18m");
        assert_eq!(human_secs(9_900.0), "2h45m");
        assert_eq!(span((60.0, 60.0)), "about 60s");
        assert_eq!(span((1_260.0, 9_900.0)), "roughly 21m to 2h45m");
    }
}
