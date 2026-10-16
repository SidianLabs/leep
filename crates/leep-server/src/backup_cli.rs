//! `leep-server backup`, `restore` and `backup-verify` (ADR 0026): the command
//! line over `leep_server::backup`. Like `verify` they open the stores
//! directly and print progress on stderr and the report on stdout.
//!
//! Exit codes: 0 done and clean, 1 done but the check found problems (a
//! backup is still sealed; a restore is still loaded), 2 could not run.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context;
use clap::Args;
use leep_derive::integrity::{Problem, Progress, VerifyReport};
use leep_server::backup::{
    self, BackupOptions, BackupOutcome, Manifest, NoHooks, RestoreOptions, VerifyBackupOptions,
};
use serde::Serialize;

pub const EXIT_CLEAN: i32 = 0;
pub const EXIT_PROBLEMS: i32 = 1;
pub const EXIT_COULD_NOT_RUN: i32 = 2;

#[derive(Args)]
pub struct BackupArgs {
    /// Backup directory. Holds `blobs/` (shared by every snapshot, grown
    /// incrementally) and `snapshots/<UTC time>/` (one per run). Created if
    /// missing. Uses --meta and --blobs as the source.
    #[arg(long, value_name = "DIR")]
    out: PathBuf,
    /// Copy blobs to this store instead of `<out>/blobs` (a directory,
    /// `file://` or `s3://` URL). Keep it the same for every run: only blobs
    /// it lacks are copied.
    #[arg(long, value_name = "URL", conflicts_with = "no_blobs")]
    blobs_to: Option<String>,
    /// Back up metadata only; the blobs are protected some other way (bucket
    /// replication or versioning, `aws s3 sync`). Run `backup-verify` against
    /// them afterwards: it is the only proof the two halves match.
    #[arg(long)]
    no_blobs: bool,
    /// Skip the closure check (walk every changeset of the snapshot and
    /// confirm each blob and tree it names was copied). The snapshot is then
    /// sealed unchecked and `restore` refuses it unless told otherwise.
    #[arg(long)]
    no_check: bool,
    /// Also re-hash the bytes of every referenced blob in the check.
    #[arg(long, conflicts_with = "no_check")]
    rehash: bool,
    /// Blobs copied at once; each is held in memory while copied.
    #[arg(long, default_value_t = 8)]
    concurrency: usize,
    /// Name of the snapshot directory; default the current UTC time.
    #[arg(long)]
    name: Option<String>,
    /// Problems listed in the report; the rest are only counted.
    #[arg(long, default_value_t = 1000)]
    max_problems: usize,
    /// Write Prometheus text-file metrics here (node_exporter's textfile
    /// directory), including the time of the last successful backup.
    #[arg(long, value_name = "FILE")]
    metrics_file: Option<PathBuf>,
    /// Print the report as one JSON object.
    #[arg(long)]
    json: bool,
    /// Wait this long between the metadata snapshot and the blob copy (10s,
    /// 2m). For drills only: it widens the window in which garbage collection
    /// and new uploads can interleave, so a drill proves the order is safe.
    #[arg(long, hide = true, value_parser = parse_pause, value_name = "DURATION")]
    pause_after_snapshot: Option<std::time::Duration>,
}

fn parse_pause(s: &str) -> Result<std::time::Duration, String> {
    leep_derive::gc::parse_duration(s).map_err(|e| e.to_string())
}

/// Sleeps between the two halves of a backup (`--pause-after-snapshot`).
struct Pause(std::time::Duration);

#[async_trait::async_trait]
impl backup::Hooks for Pause {
    async fn after_meta_snapshot(&self, _: &leep_meta::snapshot::SnapshotInfo) {
        eprintln!("backup: pausing {:?} between the halves", self.0);
        tokio::time::sleep(self.0).await;
    }
}

#[derive(Args)]
pub struct RestoreArgs {
    /// Backup directory (its newest sealed snapshot is restored) or one
    /// snapshot directory. Restores into --meta and --blobs.
    #[arg(long, value_name = "DIR")]
    from: PathBuf,
    /// Restore this snapshot (a name under `snapshots/`) instead of the newest.
    #[arg(long)]
    snapshot: Option<String>,
    /// Read blobs from this store instead of `<from>/blobs`.
    #[arg(long, value_name = "URL")]
    blobs_from: Option<String>,
    /// Allow non-empty targets: delete the metadata that is there first, and
    /// accept blobs that are there. Stop every server using the target first.
    #[arg(long)]
    force: bool,
    /// Continue an interrupted restore: allow a non-empty blob store while the
    /// metadata store is still empty.
    #[arg(long)]
    resume: bool,
    /// Restore a snapshot whose backup-time check found problems or was skipped.
    #[arg(long)]
    accept_failed_check: bool,
    /// Skip the check of the restored stores.
    #[arg(long)]
    no_check: bool,
    /// Also re-hash the bytes of every referenced blob in the check.
    #[arg(long, conflicts_with = "no_check")]
    rehash: bool,
    #[arg(long, default_value_t = 8)]
    concurrency: usize,
    #[arg(long, default_value_t = 1000)]
    max_problems: usize,
    #[arg(long)]
    json: bool,
}

#[derive(Args)]
pub struct VerifyBackupArgs {
    /// Backup directory (its newest sealed snapshot is checked) or one
    /// snapshot directory.
    #[arg(long, value_name = "DIR")]
    from: PathBuf,
    #[arg(long)]
    snapshot: Option<String>,
    /// Blobs to check against instead of `<from>/blobs`.
    #[arg(long, value_name = "URL")]
    blobs_from: Option<String>,
    /// Re-hash the bytes of every referenced blob.
    #[arg(long)]
    rehash: bool,
    #[arg(long, default_value_t = 1000)]
    max_problems: usize,
    #[arg(long)]
    json: bool,
}

struct Stderr;

impl Progress for Stderr {
    fn info(&self, message: &str) {
        eprintln!("{message}");
    }
}

fn pool_url(dir: &std::path::Path) -> String {
    format!("file://{}", dir.display())
}

fn bytes_human(n: u64) -> String {
    const UNITS: [&str; 4] = ["B", "KiB", "MiB", "GiB"];
    let mut v = n as f64;
    let mut u = 0;
    while v >= 1024.0 && u < UNITS.len() - 1 {
        v /= 1024.0;
        u += 1;
    }
    if u == 0 {
        format!("{n} B")
    } else {
        format!("{v:.1} {}", UNITS[u])
    }
}

fn print_problems(problems: &[Problem], total: u64) {
    if total == 0 {
        return;
    }
    println!();
    println!(
        "PROBLEMS: {total}{}",
        if (problems.len() as u64) < total {
            format!(", the first {} are listed (--max-problems)", problems.len())
        } else {
            String::new()
        }
    );
    for p in problems {
        println!("  {p}");
    }
}

fn print_check(r: &VerifyReport) {
    println!(
        "check: {} changesets, {} tree nodes, {} blobs checked, {} re-hashed ({})",
        r.stats.changesets_reachable + r.stats.changesets_other,
        r.stats.tree_nodes,
        r.stats.blobs_checked,
        r.stats.blobs_rehashed,
        bytes_human(r.stats.bytes_rehashed)
    );
}

fn print_manifest(m: &Manifest) {
    println!("snapshot: {}", m.name);
    println!(
        "taken at: {} (unix {}), schema v{}, from {}",
        backup::utc_stamp(m.snapshot_taken_at),
        m.snapshot_taken_at,
        m.schema_version,
        m.source_backend
    );
    println!(
        "metadata: {} changesets, {} refs, {} derived rows, {} ref log entries ({})",
        m.counts.changesets,
        m.counts.refs,
        m.counts.derived,
        m.counts.ref_log,
        bytes_human(m.meta_bytes)
    );
    for (name, tip) in &m.refs {
        println!("  ref {name} -> {tip}");
    }
}

#[derive(Serialize)]
struct BackupJson<'a> {
    ok: bool,
    exit_code: i32,
    snapshot_dir: String,
    manifest: &'a Manifest,
    problems: &'a [Problem],
    problems_total: u64,
}

pub async fn run_backup(meta: &str, blobs: &str, args: BackupArgs) -> i32 {
    match backup_inner(meta, blobs, args).await {
        Ok(code) => code,
        Err(e) => {
            eprintln!("error: {e:#}");
            EXIT_COULD_NOT_RUN
        }
    }
}

async fn backup_inner(meta: &str, blobs: &str, args: BackupArgs) -> anyhow::Result<i32> {
    backup::check_source_exists(meta, blobs)?;
    let source = leep_store::open_blob_store(blobs).context("opening the source blob store")?;
    let (dest, location) = if args.no_blobs {
        (None, String::new())
    } else {
        let url = args
            .blobs_to
            .clone()
            .unwrap_or_else(|| pool_url(&args.out.join(backup::BLOBS_DIR)));
        if let Some(dir) = url.strip_prefix("file://") {
            std::fs::create_dir_all(dir).with_context(|| format!("creating {dir}"))?;
        }
        if url == blobs {
            anyhow::bail!("the backup's blob store is the source blob store; refusing");
        }
        (
            Some(leep_store::open_blob_store(&url).context("opening the backup blob store")?),
            url,
        )
    };
    let pause = args.pause_after_snapshot.map(Pause);
    let outcome = backup::backup(
        BackupOptions {
            meta_url: meta.to_string(),
            source_blobs: source,
            out: args.out.clone(),
            dest_blobs: dest,
            dest_blobs_location: location,
            check: !args.no_check,
            rehash: args.rehash,
            concurrency: args.concurrency,
            max_problems: args.max_problems,
            name: args.name,
        },
        match &pause {
            Some(p) => p,
            None => &NoHooks,
        },
        Arc::new(Stderr),
    )
    .await?;
    if let Some(path) = &args.metrics_file {
        backup::write_textfile_metrics(path, &outcome)
            .with_context(|| format!("writing {}", path.display()))?;
    }
    let code = if outcome.ok() || args.no_check {
        EXIT_CLEAN
    } else {
        EXIT_PROBLEMS
    };
    if args.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&BackupJson {
                ok: code == EXIT_CLEAN,
                exit_code: code,
                snapshot_dir: outcome.snapshot_dir.display().to_string(),
                manifest: &outcome.manifest,
                problems: &outcome.problems,
                problems_total: outcome.problems_total,
            })?
        );
    } else {
        print_backup(&outcome, code);
    }
    Ok(code)
}

fn print_backup(o: &BackupOutcome, code: i32) {
    let m = &o.manifest;
    print_manifest(m);
    if m.blobs.included {
        println!(
            "blobs: {} listed, {} already in the backup, {} copied ({}), {} deleted by gc meanwhile, {} only in the backup",
            m.blobs.listed,
            m.blobs.already_present,
            m.blobs.copied,
            bytes_human(m.blobs.bytes_copied),
            m.blobs.vanished,
            m.blobs.only_in_backup
        );
        println!("blobs at: {}", m.blobs.location);
        for h in &m.blobs.corrupt {
            println!("WARNING: source blob {h} does not hash to its name; not copied");
        }
    } else {
        println!("blobs: not copied (--no-blobs); run backup-verify once they are in place");
    }
    println!(
        "check: {}{}",
        m.check.mode,
        if m.check.mode == "skipped" {
            String::new()
        } else {
            format!(
                ": {} changesets, {} tree nodes, {} blobs checked, {} re-hashed",
                m.check.changesets,
                m.check.tree_nodes,
                m.check.blobs_checked,
                m.check.blobs_rehashed
            )
        }
    );
    print_problems(&o.problems, o.problems_total);
    println!("directory: {}", o.snapshot_dir.display());
    println!("elapsed: {:.1}s", m.elapsed_secs);
    println!(
        "result: {}",
        match code {
            EXIT_CLEAN if m.check.mode == "skipped" => "sealed, not checked".to_string(),
            EXIT_CLEAN => "sealed and checked".to_string(),
            _ => format!(
                "sealed, but the check found {} problem(s): this snapshot is not safe to rely on",
                m.check.problems
            ),
        }
    );
}

pub async fn run_restore(meta: &str, blobs: &str, no_migrate: bool, args: RestoreArgs) -> i32 {
    match restore_inner(meta, blobs, no_migrate, args).await {
        Ok(code) => code,
        Err(e) => {
            eprintln!("error: {e:#}");
            EXIT_COULD_NOT_RUN
        }
    }
}

#[derive(Serialize)]
struct RestoreJson<'a> {
    ok: bool,
    exit_code: i32,
    manifest: &'a Manifest,
    blobs_listed: u64,
    blobs_copied: u64,
    blob_bytes_copied: u64,
    changesets: u64,
    refs: u64,
    derived: u64,
    ref_log: u64,
    elapsed_secs: f64,
    check: Option<&'a VerifyReport>,
}

async fn restore_inner(
    meta: &str,
    blobs: &str,
    no_migrate: bool,
    args: RestoreArgs,
) -> anyhow::Result<i32> {
    crate::ensure_local_dirs(meta, blobs)?;
    let (dir, _) = backup::find_snapshot(&args.from, args.snapshot.as_deref())?;
    let source_url = args
        .blobs_from
        .clone()
        .unwrap_or_else(|| pool_url(&backup::pool_dir(&dir)));
    if source_url == blobs {
        anyhow::bail!("the backup's blob store is the destination blob store; refusing");
    }
    let source = leep_store::open_blob_store(&source_url).context("opening the backup blobs")?;
    let target = leep_store::open_blob_store(blobs).context("opening the blob store")?;
    let mode = if no_migrate {
        leep_meta::SchemaMode::RequireCurrent
    } else {
        leep_meta::SchemaMode::Migrate
    };
    let o = backup::restore(
        RestoreOptions {
            from: args.from,
            snapshot: args.snapshot,
            meta_url: meta.to_string(),
            meta_mode: mode,
            target_blobs: target,
            source_blobs: Some(source),
            force: args.force,
            resume: args.resume,
            accept_failed_check: args.accept_failed_check,
            check: !args.no_check,
            rehash: args.rehash,
            concurrency: args.concurrency,
            max_problems: args.max_problems,
        },
        Arc::new(Stderr),
    )
    .await?;
    let ok = o.report.as_ref().is_none_or(|r| r.ok());
    let code = if ok { EXIT_CLEAN } else { EXIT_PROBLEMS };
    let (listed, copied, bytes) = o
        .blobs
        .as_ref()
        .map_or((0, 0, 0), |b| (b.listed, b.copied, b.bytes_copied));
    if args.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&RestoreJson {
                ok,
                exit_code: code,
                manifest: &o.manifest,
                blobs_listed: listed,
                blobs_copied: copied,
                blob_bytes_copied: bytes,
                changesets: o.imported.changesets,
                refs: o.imported.refs,
                derived: o.imported.derived,
                ref_log: o.imported.ref_log,
                elapsed_secs: o.elapsed_secs,
                check: o.report.as_ref(),
            })?
        );
    } else {
        print_manifest(&o.manifest);
        println!(
            "restored: {} changesets, {} refs, {} derived rows, {} ref log entries",
            o.imported.changesets, o.imported.refs, o.imported.derived, o.imported.ref_log
        );
        println!(
            "blobs: {listed} in the backup, {copied} copied ({})",
            bytes_human(bytes)
        );
        match &o.report {
            Some(r) => {
                print_check(r);
                print_problems(&r.problems, r.problems_total);
            }
            None => println!("check: skipped"),
        }
        println!("elapsed: {:.1}s", o.elapsed_secs);
        println!(
            "result: {}",
            if ok {
                "restored; run `leep-server verify --full --rehash` before serving traffic"
            } else {
                "restored, but the check found problems"
            }
        );
    }
    Ok(code)
}

pub async fn run_verify_backup(args: VerifyBackupArgs) -> i32 {
    match verify_backup_inner(args).await {
        Ok(code) => code,
        Err(e) => {
            eprintln!("error: {e:#}");
            EXIT_COULD_NOT_RUN
        }
    }
}

#[derive(Serialize)]
struct VerifyBackupJson<'a> {
    ok: bool,
    exit_code: i32,
    manifest: &'a Manifest,
    check: &'a VerifyReport,
}

async fn verify_backup_inner(args: VerifyBackupArgs) -> anyhow::Result<i32> {
    let (dir, _) = backup::find_snapshot(&args.from, args.snapshot.as_deref())?;
    let url = args
        .blobs_from
        .clone()
        .unwrap_or_else(|| pool_url(&backup::pool_dir(&dir)));
    if let Some(d) = url.strip_prefix("file://") {
        if !std::path::Path::new(d).is_dir() {
            anyhow::bail!("blob directory {d} does not exist");
        }
    }
    let blobs = leep_store::open_blob_store(&url).context("opening the backup blobs")?;
    let o = backup::verify_backup(
        VerifyBackupOptions {
            from: args.from,
            snapshot: args.snapshot,
            blobs,
            rehash: args.rehash,
            max_problems: args.max_problems,
        },
        &Stderr,
    )
    .await?;
    let code = if o.report.ok() {
        EXIT_CLEAN
    } else {
        EXIT_PROBLEMS
    };
    if args.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&VerifyBackupJson {
                ok: o.report.ok(),
                exit_code: code,
                manifest: &o.manifest,
                check: &o.report,
            })?
        );
    } else {
        print_manifest(&o.manifest);
        print_check(&o.report);
        print_problems(&o.report.problems, o.report.problems_total);
        println!(
            "result: {}",
            if o.report.ok() {
                "clean".to_string()
            } else {
                format!("{} problem(s) found", o.report.problems_total)
            }
        );
    }
    Ok(code)
}
