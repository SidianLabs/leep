// Output goes through `leep_core::outln!`, which exits quietly when the
// reader of stdout has gone instead of panicking like `println!`.
#![warn(clippy::print_stdout)]

use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use leep_core::outln;
use leep_derive::Repo;
use leep_git::mirror::{self, CommitStyle, RefSpec};
use leep_git::{ExportOptions, ImportOptions};

#[derive(Parser)]
#[command(name = "leep-git", about = "Move history between git and Leep")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Import a git revision (and its ancestry) into Leep.
    Import {
        /// Metadata store URL (sqlite:, postgres://, mem://), as the server's.
        #[arg(long, env = "LEEP_META")]
        meta: String,
        /// Blob store URL (file://, s3://, mem://), as the server's.
        #[arg(long, env = "LEEP_BLOBS")]
        blobs: String,
        /// The git repository: a work tree or a bare or `.git` directory.
        #[arg(long)]
        git_dir: PathBuf,
        /// Git revision to import with its ancestry.
        #[arg(long, default_value = "HEAD")]
        rev: String,
        /// Fast-forward this Leep ref to the imported head.
        #[arg(long)]
        set_ref: Option<String>,
    },
    /// Export a Leep revision (ref name or changeset id) as a git branch.
    Export {
        /// Metadata store URL (sqlite:, postgres://, mem://), as the server's.
        #[arg(long, env = "LEEP_META")]
        meta: String,
        /// Blob store URL (file://, s3://, mem://), as the server's.
        #[arg(long, env = "LEEP_BLOBS")]
        blobs: String,
        #[arg(long)]
        rev: String,
        #[arg(long)]
        git_dir: PathBuf,
        #[arg(long, default_value = "main")]
        branch: String,
        /// Add `Leep-Agent-*` trailers from `agent.*` metadata.
        #[arg(long)]
        provenance_trailers: bool,
    },
    /// Keep a git remote in step with a Leep server (ADR 0046).
    Mirror {
        #[command(subcommand)]
        cmd: MirrorCmd,
    },
}

#[derive(Subcommand)]
enum MirrorCmd {
    /// Run the mirror daemon: push Leep refs to the remote as they move and,
    /// with --import, land fast-forward pushes from the remote in Leep.
    Run(Box<RunArgs>),
    /// Print the state a mirror last recorded in its work directory. Exits 0
    /// when every ref is in sync or catching up, 1 when one needs attention,
    /// 2 when there is no status to read.
    Status {
        #[arg(long)]
        work_dir: PathBuf,
        /// Print the status document as JSON.
        #[arg(long)]
        json: bool,
    },
}

#[derive(clap::Args)]
struct RunArgs {
    /// Leep server URL.
    #[arg(long, env = "LEEP_SERVER")]
    server: String,
    /// File holding the Leep token (LEEP_TOKEN also works). Never pass a
    /// token on the command line, where process listings show it.
    #[arg(long)]
    token_file: Option<PathBuf>,
    /// Git remote: an https, ssh or file URL, or a path. Credentials come
    /// from git (credential helper, SSH agent); any in the URL are redacted
    /// from logs and status.
    #[arg(long)]
    remote: String,
    /// State directory: the local git repository, the commit map, the
    /// status file and a lock (one mirror per directory).
    #[arg(long)]
    work_dir: PathBuf,
    /// Leep ref to mirror, as NAME or NAME:GIT_REF (repeatable). NAME maps
    /// to refs/heads/NAME and tags/T to refs/tags/T.
    #[arg(long = "ref", value_name = "SPEC")]
    refs: Vec<String>,
    /// Also mirror every Leep tag to refs/tags/.
    #[arg(long)]
    tags: bool,
    /// Also mirror every Leep ref under PREFIX (such as review/) to the
    /// branch of the same name, as refs appear (repeatable). Push-only: a
    /// branch someone pushed to is reported, never imported or overwritten.
    /// A ref deleted in Leep has its branch deleted with a lease.
    #[arg(long = "ref-prefix", value_name = "PREFIX")]
    ref_prefixes: Vec<String>,
    /// Land commits pushed to the remote in Leep when they fast-forward the
    /// Leep ref, through the server's land policy.
    #[arg(long)]
    import: bool,
    /// Leep ref whose backward moves (undo) may be pushed with a lease
    /// (repeatable). Never main or a tag.
    #[arg(long, value_name = "NAME")]
    allow_rewind: Vec<String>,
    /// Branch prefix the mirror owns; on divergence Leep's side of BRANCH is
    /// pushed to PREFIX/BRANCH for someone to merge.
    #[arg(long, default_value = "leep-diverged/")]
    diverged_prefix: String,
    /// When both sides of a ref moved: `report` (push Leep's side to the
    /// diverged branch for a person to merge) or `merge` (import the
    /// remote's side, merge on the server and land the merge when it is
    /// clean and the land policy allows; report otherwise). Needs --import.
    #[arg(long, default_value = "report")]
    on_divergence: mirror::OnDivergence,
    /// Author of the mirror's merges. Part of each merge's id: keep it fixed
    /// for the life of a mirror.
    #[arg(long, default_value = mirror::DEFAULT_MERGE_AUTHOR)]
    merge_author: String,
    /// Land branches pushed to PREFIX<ref>/<name> onto the Leep ref <ref>
    /// through the server's land (rebased like an agent's land), then delete
    /// them. Needs --import.
    #[arg(long)]
    land_branches: bool,
    /// Branch prefix of land branches (with --land-branches).
    #[arg(long, default_value = "leep-land/")]
    land_prefix: String,
    /// Add `Leep-Agent-*` trailers. Changes commit ids: keep it fixed for
    /// the life of a mirror.
    #[arg(long)]
    provenance_trailers: bool,
    /// Do not add the `Leep-Changeset:` trailer to commits made in Leep.
    /// Changes commit ids: keep it fixed for the life of a mirror.
    #[arg(long)]
    no_changeset_trailer: bool,
    /// How often to ask the server whether a ref moved.
    #[arg(long, default_value = "1s", value_parser = parse_duration)]
    interval: std::time::Duration,
    /// How often to read the remote's refs (POST /poke polls at once).
    #[arg(long, default_value = "15s", value_parser = parse_duration)]
    git_poll: std::time::Duration,
    /// How often to re-read every Leep ref even if the ref log is quiet.
    #[arg(long, default_value = "60s", value_parser = parse_duration)]
    reconcile: std::time::Duration,
    /// Longest a git command may run before it is killed.
    #[arg(long, default_value = "10m", value_parser = parse_duration)]
    git_timeout: std::time::Duration,
    /// Longest wait between retries of a failing ref.
    #[arg(long, default_value = "5m", value_parser = parse_duration)]
    max_backoff: std::time::Duration,
    /// Serve GET /metrics, GET /status, GET /health and POST /poke here.
    #[arg(long, env = "LEEP_MIRROR_LISTEN")]
    listen: Option<std::net::SocketAddr>,
    /// Run cycles until nothing changes, then exit 0 if every ref is in
    /// sync or pending and 1 otherwise.
    #[arg(long)]
    once: bool,
}

/// `500ms`, `15s`, `10m`, `1h`, or plain seconds.
fn parse_duration(s: &str) -> std::result::Result<std::time::Duration, String> {
    let (num, unit) = s
        .find(|c: char| !c.is_ascii_digit())
        .map_or((s, "s"), |i| s.split_at(i));
    let n: u64 = num.parse().map_err(|_| format!("bad duration {s:?}"))?;
    let ms = match unit {
        "ms" => n,
        "s" => n * 1000,
        "m" => n * 60_000,
        "h" => n * 3_600_000,
        _ => return Err(format!("bad duration {s:?}: use ms, s, m or h")),
    };
    Ok(std::time::Duration::from_millis(ms))
}

fn read_token(file: Option<&PathBuf>) -> Result<Option<String>> {
    if let Some(f) = file {
        let t = std::fs::read_to_string(f)
            .with_context(|| format!("reading token file {}", f.display()))?;
        return Ok(Some(t.trim().to_string()).filter(|t| !t.is_empty()));
    }
    Ok(std::env::var("LEEP_TOKEN").ok().filter(|t| !t.is_empty()))
}

/// The mirror is synchronous (blocking HTTP and git subprocesses), so it runs
/// outside the async runtime the import and export use.
fn run_mirror(cmd: MirrorCmd) -> Result<i32> {
    match cmd {
        MirrorCmd::Status { work_dir, json } => {
            let path = work_dir.join("status.json");
            let s = match mirror::read_status(&path) {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("no mirror status at {}: {e}", path.display());
                    return Ok(2);
                }
            };
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0);
            let (healthy, problems) = mirror::status_healthy(&s, now);
            if json {
                outln!("{}", serde_json::to_string_pretty(&s)?);
            } else {
                outln!(
                    "mirror pid {} {} -> {}, updated {}s ago",
                    s.pid,
                    s.server,
                    s.remote,
                    now.saturating_sub(s.updated)
                );
                for r in &s.refs {
                    let short = |v: &Option<String>| {
                        v.as_deref()
                            .map_or("-".to_string(), |v| v[..12.min(v.len())].to_string())
                    };
                    outln!(
                        "  {:<24} {:<16} leep {} git {} lag {}s{}",
                        r.leep_ref,
                        r.state.label(),
                        short(&r.leep),
                        short(&r.remote),
                        r.lag(now),
                        if r.detail.is_empty() {
                            String::new()
                        } else {
                            format!("\n      {}", r.detail)
                        }
                    );
                }
                for b in &s.land_branches {
                    outln!(
                        "  land branch {} -> {} {} at {}{}",
                        b.branch.trim_start_matches("refs/heads/"),
                        b.leep_ref.as_deref().unwrap_or("?"),
                        b.state.label(),
                        &b.commit[..12.min(b.commit.len())],
                        if b.detail.is_empty() {
                            String::new()
                        } else {
                            format!("\n      {}", b.detail)
                        }
                    );
                }
                for d in &s.deleted_branches {
                    outln!(
                        "  deleted {} at {} {}s ago (deleted in Leep)",
                        d.git_ref.trim_start_matches("refs/heads/"),
                        &d.commit[..12.min(d.commit.len())],
                        now.saturating_sub(d.at)
                    );
                }
                for p in &problems {
                    eprintln!("attention: {p}");
                }
            }
            Ok(if healthy { 0 } else { 1 })
        }
        MirrorCmd::Run(a) => {
            let token = read_token(a.token_file.as_ref())?;
            let refs = a
                .refs
                .iter()
                .map(|r| RefSpec::parse(r))
                .collect::<Result<Vec<_>>>()?;
            let cfg = mirror::Config {
                server: a.server.clone(),
                token: token.clone(),
                remote: a.remote,
                work_dir: a.work_dir,
                refs,
                tags: a.tags,
                ref_prefixes: a.ref_prefixes,
                import: a.import,
                allow_rewind: a.allow_rewind,
                diverged_prefix: a.diverged_prefix,
                on_divergence: a.on_divergence,
                merge_author: a.merge_author,
                land_prefix: a.land_branches.then_some(a.land_prefix),
                style: CommitStyle {
                    provenance_trailers: a.provenance_trailers,
                    changeset_trailer: !a.no_changeset_trailer,
                },
                interval: a.interval,
                git_poll: a.git_poll,
                reconcile: a.reconcile,
                git_timeout: a.git_timeout,
                max_backoff: a.max_backoff,
                listen: a.listen,
            };
            let client = leep_cli::client::Client::with_token(&a.server, token.as_deref());
            let mut m = mirror::Mirror::open(cfg, &client)?;
            if a.once {
                return Ok(if m.run_once() { 0 } else { 1 });
            }
            let wake = std::sync::Arc::new(mirror::status::Wake::default());
            if let Some(addr) = a.listen {
                let local = mirror::status::serve(addr, m.status.clone(), wake.clone())
                    .with_context(|| format!("listening on {addr}"))?;
                eprintln!("leep-git mirror: serving /metrics, /status and /poke on {local}");
            }
            m.run(&wake)
        }
    }
}

async fn open(meta: &str, blobs: &str) -> Result<Repo> {
    let meta = leep_meta::open_meta_store(meta)
        .await
        .context("opening meta store")?;
    let blobs = leep_store::open_blob_store(blobs).context("opening blob store")?;
    Ok(Repo::new(blobs, meta))
}

/// glibc gives each thread that contends for the heap an arena of its own,
/// up to eight per CPU, and each keeps the memory freed in it. An import
/// frees blob buffers on threads other than the ones that read them, so
/// that kept memory grew with the history: importing Linux to v2.6.16 ended
/// at 243 MB of anonymous memory with the default and 75 MB with two
/// arenas, and ran faster. glibc reads the limit only at startup, so the
/// process starts itself again with it set. Other allocators ignore it.
#[cfg(all(target_os = "linux", target_env = "gnu"))]
fn limit_malloc_arenas() {
    use std::os::unix::process::CommandExt;
    if std::env::var_os("MALLOC_ARENA_MAX").is_some() {
        return;
    }
    let Ok(exe) = std::env::current_exe() else {
        return;
    };
    let mut args = std::env::args_os();
    let arg0 = args.next().unwrap_or_else(|| exe.clone().into_os_string());
    let err = std::process::Command::new(&exe)
        .arg0(arg0)
        .args(args)
        .env("MALLOC_ARENA_MAX", "2")
        .exec();
    eprintln!("warning: could not restart with MALLOC_ARENA_MAX=2: {err}");
}

#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
fn limit_malloc_arenas() {}

fn main() -> Result<()> {
    limit_malloc_arenas();
    let cli = Cli::parse();
    if let Cmd::Mirror { cmd } = cli.cmd {
        let code = run_mirror(cmd)?;
        std::process::exit(code);
    }
    tokio::runtime::Runtime::new()?.block_on(run(cli.cmd))
}

async fn run(cmd: Cmd) -> Result<()> {
    match cmd {
        Cmd::Import {
            meta,
            blobs,
            git_dir,
            rev,
            set_ref,
        } => {
            let repo = open(&meta, &blobs).await?;
            let r = leep_git::import(&repo, &git_dir, &rev, &ImportOptions { set_ref }).await?;
            outln!(
                "imported {} commits ({} already present), {} blobs, {} submodules skipped",
                r.commits_imported,
                r.commits_skipped,
                r.blobs_uploaded,
                r.submodules_skipped
            );
            if let Some(h) = r.head {
                outln!("head {h}");
            }
            // Indexes are usually derived right after an import, from
            // statistics that predate it (ADR 0042). The import itself is
            // done, so a failure only warns.
            if r.commits_imported > 0 {
                if let Err(e) = leep_meta::analyze_meta_store(&meta).await {
                    eprintln!("warning: ANALYZE after import failed: {e}");
                }
            }
        }
        Cmd::Export {
            meta,
            blobs,
            rev,
            git_dir,
            branch,
            provenance_trailers,
        } => {
            let repo = open(&meta, &blobs).await?;
            let id = repo.resolve(&rev).await?;
            let r = leep_git::export(
                &repo,
                id,
                &git_dir,
                &branch,
                &ExportOptions {
                    provenance_trailers,
                },
            )
            .await?;
            outln!(
                "exported {} commits ({} already present), {} blobs",
                r.commits_written,
                r.commits_skipped,
                r.blobs_written
            );
            if let Some(h) = r.head {
                outln!("head {h}");
            }
        }
        Cmd::Mirror { .. } => unreachable!("handled before the runtime starts"),
    }
    Ok(())
}
