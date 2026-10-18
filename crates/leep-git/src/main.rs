use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use leep_derive::Repo;
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
        #[arg(long)]
        meta: String,
        #[arg(long)]
        blobs: String,
        #[arg(long)]
        git_dir: PathBuf,
        #[arg(long, default_value = "HEAD")]
        rev: String,
        /// Fast-forward this Leep ref to the imported head.
        #[arg(long)]
        set_ref: Option<String>,
    },
    /// Export a Leep revision (ref name or changeset id) as a git branch.
    Export {
        #[arg(long)]
        meta: String,
        #[arg(long)]
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
    tokio::runtime::Runtime::new()?.block_on(run())
}

async fn run() -> Result<()> {
    match Cli::parse().cmd {
        Cmd::Import {
            meta,
            blobs,
            git_dir,
            rev,
            set_ref,
        } => {
            let repo = open(&meta, &blobs).await?;
            let r = leep_git::import(&repo, &git_dir, &rev, &ImportOptions { set_ref }).await?;
            println!(
                "imported {} commits ({} already present), {} blobs, {} submodules skipped",
                r.commits_imported, r.commits_skipped, r.blobs_uploaded, r.submodules_skipped
            );
            if let Some(h) = r.head {
                println!("head {h}");
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
            println!(
                "exported {} commits ({} already present), {} blobs",
                r.commits_written, r.commits_skipped, r.blobs_written
            );
            if let Some(h) = r.head {
                println!("head {h}");
            }
        }
    }
    Ok(())
}
