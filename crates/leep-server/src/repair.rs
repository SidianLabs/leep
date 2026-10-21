//! `leep-server verify` and `leep-server repair`: offline checks of a store
//! and repair of derived data (ADR 0025). Like `gc` they open the stores
//! directly, never through the HTTP API, and are safe beside running servers:
//! `verify` writes nothing, and `repair` writes only derived rows, which are
//! pure functions of content.
//!
//! Exit codes: 0 clean, 1 problems found (or, for `repair --dry-run`, rows
//! that need repair), 2 could not run (bad arguments, a refusal, an
//! unreachable store). Progress goes to stderr; the report goes to stdout.

use std::sync::Arc;

use anyhow::Context;
use clap::{Args, Subcommand};
use ed25519_dalek::{Signature as DalekSignature, VerifyingKey};
use leep_core::signature::signing_message;
use leep_derive::integrity::{
    self, kinds, DropReport, Problem, Progress, RederiveOptions, RederiveReport, Scope,
    VerifyOptions, VerifyReport,
};
use leep_derive::Repo;
use serde::Serialize;

/// The index-lease fence of tools that delete or rebuild index rows (ADR
/// 0058 section 6).
#[derive(Args, Clone)]
pub struct IndexFenceArgs {
    /// Go ahead although a server holds the lease of an index this touches
    /// (duplicate work, correct rows). Without it the tool refuses. Not
    /// allowed with `repair drop` or `upgrade-index --drop-first`: a drop
    /// beside a deriving server is the race the lease and fence prevent.
    #[arg(long)]
    pub ignore_lease: bool,
    /// After taking the index leases, wait this long before touching rows,
    /// so a former holder's write still in flight lands first (0, 30s, 2m).
    /// Must exceed the stale time (6 s) plus the longest staged write.
    #[arg(long, default_value = "60s", value_parser = crate::parse_grace)]
    pub index_fence: std::time::Duration,
}

/// Take the index leases of the groups `kinds` belong to, for a tool, and
/// wait the fence. `None` when no kind is an index or the store has no
/// leases (servers on it run in `local` mode, which takes none).
pub(crate) async fn fence_indexes(
    repo: &Repo,
    kinds: &[&str],
    fence: &IndexFenceArgs,
    tool: &str,
) -> anyhow::Result<Option<leep_derive::indexing::ToolLease>> {
    use leep_derive::indexing::IndexGroup;
    let mut groups: Vec<IndexGroup> = kinds
        .iter()
        .filter_map(|k| IndexGroup::of_kind(k))
        .collect();
    groups.sort();
    groups.dedup();
    if groups.is_empty() || !repo.meta().supports_leases() {
        return Ok(None);
    }
    let mut id = [0u8; 8];
    getrandom::fill(&mut id).map_err(|e| anyhow::anyhow!("random holder id: {e}"))?;
    let holder = format!("{tool}-{}", hex::encode(id));
    let say = |m: &str| eprintln!("{tool}: {m}");
    let lease = leep_derive::indexing::ToolLease::take(
        repo.meta().clone(),
        &holder,
        &groups,
        fence.ignore_lease,
        fence.index_fence,
        &say,
    )
    .await?;
    Ok(Some(lease))
}

/// Give a tool's index leases up, saying so if one was lost meanwhile.
pub(crate) async fn release_fence(lease: Option<leep_derive::indexing::ToolLease>, tool: &str) {
    if let Some(l) = lease {
        if !l.still_held().await {
            eprintln!(
                "{tool}: warning: lost an index lease while running; another server may have \
                 derived beside it (rows stay correct)"
            );
        }
        l.release().await;
    }
}

pub const EXIT_CLEAN: i32 = 0;
pub const EXIT_PROBLEMS: i32 = 1;
pub const EXIT_COULD_NOT_RUN: i32 = 2;

#[derive(Args)]
pub struct VerifyArgs {
    /// Check the history of this ref (repeatable); every ref by default.
    #[arg(long = "ref", value_name = "REF")]
    refs: Vec<String>,
    /// Check every stored changeset, not only those reachable from refs, and
    /// recompute every derived row instead of a sample.
    #[arg(long)]
    full: bool,
    /// Re-hash the bytes of every reachable blob and compare them with the
    /// blob's address, not only check that it exists. Reads all file content.
    /// (Named `--rehash` because `--blobs` is the blob store URL.)
    #[arg(long)]
    rehash: bool,
    /// Derived kinds to check (`all`, `none`, or names such as `generation`
    /// `skiplist` `root_tree`); kinds they depend on are checked too.
    /// Default: all.
    #[arg(long, num_args = 1.., value_name = "KIND")]
    derived: Vec<String>,
    /// Rows per kind (and changesets) recomputed when not --full.
    #[arg(long, default_value_t = 256)]
    sample: usize,
    /// Problems listed in the report; the rest are only counted.
    #[arg(long, default_value_t = 1000)]
    max_problems: usize,
    /// Print the report as one JSON object.
    #[arg(long)]
    json: bool,
}

#[derive(Args)]
pub struct RepairArgs {
    #[command(subcommand)]
    cmd: RepairCmd,
}

#[derive(Subcommand)]
enum RepairCmd {
    /// Recompute derived rows of a kind and write the ones that are missing or
    /// wrong. Safe to repeat and to run beside servers.
    Rederive {
        /// Kind to recompute (repeatable, or `all`); kinds it depends on are
        /// recomputed too. `verify --help` lists them.
        #[arg(long = "kind", required = true, num_args = 1.., value_name = "KIND")]
        kinds: Vec<String>,
        /// Every stored changeset (the default).
        #[arg(long, conflicts_with = "refs")]
        all: bool,
        /// Only the history of this ref (repeatable).
        #[arg(long = "ref", value_name = "REF")]
        refs: Vec<String>,
        /// Report what would change and write nothing (one changeset at a
        /// time).
        #[arg(long)]
        dry_run: bool,
        /// Changesets recomputed at once (ADR 0050). Default: the cores, up
        /// to 8.
        #[arg(long, value_parser = clap::value_parser!(u32).range(1..=256))]
        jobs: Option<u32>,
        /// Only derive rows that are missing, as the warmer would, without
        /// checking stored ones: an initial build that resumes where a
        /// stopped run left off.
        #[arg(long, conflicts_with = "dry_run")]
        missing: bool,
        #[arg(long)]
        json: bool,
        #[command(flatten)]
        fence: IndexFenceArgs,
    },
    /// Delete every row of a kind so it derives again on demand. Refused for
    /// kinds that history cannot regenerate.
    Drop {
        #[arg(long = "kind", required = true, num_args = 1.., value_name = "KIND")]
        kinds: Vec<String>,
        /// Count the rows and delete nothing.
        #[arg(long)]
        dry_run: bool,
        #[arg(long)]
        json: bool,
        #[command(flatten)]
        fence: IndexFenceArgs,
    },
}

pub(crate) struct Stderr;

impl Progress for Stderr {
    fn info(&self, message: &str) {
        eprintln!("{message}");
    }
}

/// Fail early, and without creating anything, when the stores are not there:
/// a mistyped path must not "verify clean" against a fresh empty database.
fn check_exists(meta: &str, blobs: Option<&str>) -> anyhow::Result<()> {
    if let Some(rest) = meta.strip_prefix("sqlite:") {
        let path = rest.trim_start_matches("//");
        let path = path.split('?').next().unwrap_or(path);
        if path != ":memory:" && !path.is_empty() && !std::path::Path::new(path).exists() {
            anyhow::bail!(
                "metadata database {path} does not exist (this command never creates one)"
            );
        }
    } else if meta.starts_with("mem://") {
        anyhow::bail!("a mem:// store lives in one process, so there is nothing to check");
    }
    if let Some(blobs) = blobs {
        if let Some(dir) = blobs.strip_prefix("file://") {
            if !std::path::Path::new(dir).is_dir() {
                anyhow::bail!(
                    "blob directory {dir} does not exist (this command never creates one)"
                );
            }
        } else if blobs.starts_with("mem://") {
            anyhow::bail!("a mem:// store lives in one process, so there is nothing to check");
        }
    }
    Ok(())
}

pub(crate) async fn open(meta: &str, blobs: &str) -> anyhow::Result<Repo> {
    check_exists(meta, Some(blobs))?;
    let blobs = leep_store::open_blob_store(blobs).context("opening blob store")?;
    let meta = crate::open_meta(meta).await.context("opening meta store")?;
    Ok(crate::new_repo(blobs, meta))
}

/// Kind names from the command line: `all`, `none`, or names from the table.
fn parse_kinds(names: &[String], what: Which) -> anyhow::Result<Vec<&'static str>> {
    let mut out: Vec<&'static str> = Vec::new();
    for name in names {
        match name.as_str() {
            "all" if what == Which::Verifiable => out.extend(kinds::kinds().iter().map(|k| k.name)),
            "all" => out.extend(
                kinds::kinds()
                    .iter()
                    .filter(|k| kinds::check_rederivable(k.name).is_ok())
                    .map(|k| k.name),
            ),
            "none" if what == Which::Verifiable => {}
            other => match kinds::lookup(other) {
                Some(k) => out.push(k.name),
                None => anyhow::bail!("unknown kind {other:?}; known kinds: {}", kinds::names()),
            },
        }
    }
    out.dedup();
    Ok(out)
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Which {
    Verifiable,
    Rederivable,
}

fn signature_check() -> integrity::SignatureCheck {
    Arc::new(|sig, id| {
        let key = VerifyingKey::from_bytes(&sig.public_key)
            .map_err(|_| "not a valid Ed25519 public key".to_string())?;
        key.verify_strict(&signing_message(id), &DalekSignature::from_bytes(&sig.sig))
            .map_err(|_| "signature does not verify".to_string())
    })
}

pub(crate) fn bytes_human(n: u64) -> String {
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
    if (problems.len() as u64) < total {
        println!(
            "PROBLEMS: {total} found, the first {} are listed (--max-problems)",
            problems.len()
        );
    } else {
        println!("PROBLEMS: {total}");
    }
    for p in problems {
        println!("  [{}] {}", p.code, p.message);
        let mut ctx = Vec::new();
        for (k, v) in [
            ("kind", &p.kind),
            ("key", &p.key),
            ("changeset", &p.changeset),
            ("path", &p.path),
            ("ref", &p.name),
        ] {
            if let Some(v) = v {
                ctx.push(format!("{k}={v}"));
            }
        }
        if !ctx.is_empty() {
            println!("      {}", ctx.join(" "));
        }
    }
}

#[derive(Serialize)]
struct Envelope<'a, T: Serialize> {
    ok: bool,
    exit_code: i32,
    #[serde(flatten)]
    report: &'a T,
}

pub(crate) fn print_json<T: Serialize>(ok: bool, exit_code: i32, report: &T) -> anyhow::Result<()> {
    println!(
        "{}",
        serde_json::to_string_pretty(&Envelope {
            ok,
            exit_code,
            report
        })?
    );
    Ok(())
}

fn print_verify(r: &VerifyReport) {
    println!("verify: {} mode, {:.1}s", r.mode, r.elapsed_secs);
    println!("refs checked: {} ({})", r.stats.refs, r.refs.join(", "));
    println!(
        "changesets: {} reachable from the refs, {} others",
        r.stats.changesets_reachable, r.stats.changesets_other
    );
    println!("tree nodes: {}", r.stats.tree_nodes);
    println!(
        "blobs: {} checked, {} re-hashed ({})",
        r.stats.blobs_checked,
        r.stats.blobs_rehashed,
        bytes_human(r.stats.bytes_rehashed)
    );
    println!(
        "ref log: {} entries, {} refs with none",
        r.stats.reflog_entries, r.stats.reflog_refs_without_entries
    );
    if !r.derived.is_empty() {
        println!("derived rows:");
        for (kind, s) in &r.derived {
            println!(
                "  {kind:<18} checked {}, missing {}, mismatched {}, unverifiable {}, skipped {}",
                s.checked, s.missing, s.mismatched, s.unverifiable, s.skipped
            );
        }
    }
    if !r.notes.is_empty() {
        println!("notes:");
        for n in r.notes.iter().take(20) {
            println!("  {n}");
        }
        if r.notes.len() > 20 {
            println!("  ... and {} more", r.notes.len() - 20);
        }
    }
    print_problems(&r.problems, r.problems_total);
    if r.ok() {
        println!("result: clean");
    } else {
        println!("result: {} problem(s) found", r.problems_total);
    }
}

pub async fn run_verify(meta: &str, blobs: &str, args: VerifyArgs) -> i32 {
    match verify_inner(meta, blobs, args).await {
        Ok(code) => code,
        Err(e) => {
            eprintln!("error: {e:#}");
            EXIT_COULD_NOT_RUN
        }
    }
}

async fn verify_inner(meta: &str, blobs: &str, args: VerifyArgs) -> anyhow::Result<i32> {
    let derived = if args.derived.is_empty() {
        parse_kinds(&["all".to_string()], Which::Verifiable)?
    } else {
        parse_kinds(&args.derived, Which::Verifiable)?
    };
    let repo = open(meta, blobs).await?;
    let opts = VerifyOptions {
        refs: args.refs,
        full: args.full,
        rehash: args.rehash,
        derived,
        sample: args.sample,
        max_problems: args.max_problems,
        signature_check: Some(signature_check()),
    };
    let report = integrity::verify(&repo, &opts, &Stderr).await?;
    let code = if report.ok() {
        EXIT_CLEAN
    } else {
        EXIT_PROBLEMS
    };
    if args.json {
        print_json(report.ok(), code, &report)?;
    } else {
        print_verify(&report);
    }
    Ok(code)
}

pub async fn run_repair(meta: &str, blobs: &str, args: RepairArgs) -> i32 {
    match repair_inner(meta, blobs, args).await {
        Ok(code) => code,
        Err(e) => {
            eprintln!("error: {e:#}");
            EXIT_COULD_NOT_RUN
        }
    }
}

fn print_rederive(r: &RederiveReport) {
    let verb = if r.dry_run { "would write" } else { "wrote" };
    println!(
        "rederive{}: {} ({:.1}s)",
        if r.dry_run { " (dry run)" } else { "" },
        r.kinds.join(" "),
        r.elapsed_secs
    );
    for (kind, k) in &r.kind_reports {
        println!(
            "  {kind:<18} examined {}, already right {}, {verb} {} missing and {} wrong, \
             could not recompute {}, subject gone {}",
            k.examined, k.matched, k.filled, k.overwritten, k.failed, k.skipped
        );
    }
    for n in &r.notes {
        println!("note: {n}");
    }
    print_problems(&r.problems, r.problems_total);
    println!(
        "result: {} row(s) {}, {} could not be recomputed",
        r.changed(),
        if r.dry_run { "to write" } else { "written" },
        r.failed()
    );
}

fn print_drop(r: &DropReport) {
    println!(
        "drop{}: ({:.1}s)",
        if r.dry_run { " (dry run)" } else { "" },
        r.elapsed_secs
    );
    for (kind, n) in &r.rows {
        println!(
            "  {kind:<18} {n} row(s) {}",
            if r.dry_run {
                "would be deleted"
            } else {
                "deleted"
            }
        );
    }
    println!("they derive again on demand");
}

async fn repair_inner(meta: &str, blobs: &str, args: RepairArgs) -> anyhow::Result<i32> {
    match args.cmd {
        RepairCmd::Rederive {
            kinds: names,
            all: _,
            refs,
            dry_run,
            jobs,
            missing,
            json,
            fence,
        } => {
            let kinds = parse_kinds(&names, Which::Rederivable)?;
            let repo = open(meta, blobs).await?;
            // `--missing` derives what a lease holder would: take its
            // lease, so the two do not derive the same rows (ADR 0058).
            let lease = if missing {
                fence_indexes(&repo, &kinds, &fence, "repair rederive").await?
            } else {
                None
            };
            let opts = RederiveOptions {
                kinds,
                scope: if refs.is_empty() {
                    Scope::All
                } else {
                    Scope::Refs(refs)
                },
                dry_run,
                max_problems: 1000,
                jobs: jobs.map_or_else(leep_derive::default_derive_jobs, |j| j as usize),
                missing_only: missing,
            };
            let report = integrity::rederive(&repo, &opts, &Stderr).await;
            release_fence(lease, "repair rederive").await;
            let report = report?;
            let bad = report.failed() > 0 || (dry_run && report.changed() > 0);
            let code = if bad { EXIT_PROBLEMS } else { EXIT_CLEAN };
            if json {
                print_json(!bad, code, &report)?;
            } else {
                print_rederive(&report);
            }
            Ok(code)
        }
        RepairCmd::Drop {
            kinds: names,
            dry_run,
            json,
            fence,
        } => {
            // Dropping needs only metadata; the blob store is never read.
            check_exists(meta, None)?;
            let meta = crate::open_meta(meta).await.context("opening meta store")?;
            let repo = Repo::new(Arc::new(leep_core::memory::MemoryBlobStore::new()), meta);
            let names: Vec<&str> = names.iter().map(String::as_str).collect();
            // A drop racing derivation could leave rows whose ancestors'
            // nodes are gone: hold the index leases, after the fence.
            if fence.ignore_lease && !dry_run {
                anyhow::bail!(
                    "--ignore-lease is not allowed with repair drop: dropping rows while a \
                     server derives them can leave rows whose parents' rows are gone; stop the \
                     server or switch it to --indexing off"
                );
            }
            let lease = if dry_run {
                None
            } else {
                fence_indexes(&repo, &names, &fence, "repair drop").await?
            };
            if let Some(l) = &lease {
                if !l.still_held().await {
                    release_fence(lease, "repair drop").await;
                    anyhow::bail!("lost the index lease before dropping; nothing was deleted");
                }
            }
            let report = integrity::drop_kinds(&repo, &names, dry_run, &Stderr).await;
            release_fence(lease, "repair drop").await;
            let report = report?;
            if json {
                print_json(true, EXIT_CLEAN, &report)?;
            } else {
                print_drop(&report);
            }
            Ok(EXIT_CLEAN)
        }
    }
}
