//! Argument parsing and dispatch.

use std::process::ExitCode;

use clap::{Parser, Subcommand};

use crate::cache::Cache;
use crate::client::Client;
use crate::commands::{
    amend, branch, checkout, commit, diff, history, init, keygen, land, merge, pick, rebase,
    reflog, reset, resolve, restore, review, search, stash, status, Ctx, Remote,
};
use crate::error::{CliError, ErrorKind, Result};
use crate::state::{resolve_token, WorkCopy};

const DEFAULT_SERVER: &str = "http://127.0.0.1:8080";

/// Shown under the top-level help; the full contract is docs/CLI.md.
const EXIT_CODES_HELP: &str = "\
Exit codes (ADR 0033):
  0  success
  1  generic failure (bad input, refused operation, authentication);
     also \"no\" from `search` and `review status`
  2  usage: the command line does not parse
  3  server or network failure; running the command again may work
  4  not found (revision, ref, branch, tag, file, stash)
  5  conflict: resolve the conflicts, or retry a lost race
  6  nothing to do (`commit`, `amend`, `stash push` with nothing to record)
  130 interrupted";

#[derive(Parser, Debug)]
#[command(
    name = "leep",
    version,
    about = "Client for the Leep version control server",
    after_help = EXIT_CODES_HELP
)]
pub struct Cli {
    /// Server URL (overrides the working copy config).
    #[arg(long, global = true, env = "LEEP_SERVER")]
    pub server: Option<String>,
    /// Bearer token (or set LEEP_TOKEN; falls back to `.leep/config.json`).
    #[arg(long, global = true, hide_env_values = true)]
    pub token: Option<String>,
    /// Print the raw API response as JSON; an error becomes
    /// `{"error":{"code","message","hint","retryable"}}` on stdout.
    #[arg(long, global = true)]
    pub json: bool,
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    /// Create a working copy.
    Init(init::InitArgs),
    /// Materialize a revision.
    Checkout(checkout::CheckoutArgs),
    /// Show added, modified, deleted and conflicted files.
    Status,
    /// Show changes (working copy vs base, or `FROM TO` on the server).
    Diff(diff::DiffArgs),
    /// Create an author signing key for `leep commit --sign`.
    Keygen(keygen::KeygenArgs),
    /// Record local changes as a changeset.
    Commit(commit::CommitArgs),
    /// Fold uncommitted changes and a new message into an unlanded base.
    Amend(amend::AmendArgs),
    /// Shelve uncommitted changes and bring them back later.
    Stash(stash::StashArgs),
    /// Discard uncommitted changes to files, or restore them from a revision.
    Restore(restore::RestoreArgs),
    /// Mark conflicted files as resolved so they can be committed.
    Resolve(resolve::ResolveArgs),
    /// Move the base (default: undo the last commit, keeping its changes).
    Reset(reset::ResetArgs),
    /// Merge another revision into the working copy (a local commit).
    Merge(merge::MergeArgs),
    /// Move local commits onto another revision.
    Rebase(rebase::RebaseArgs),
    /// Copy one changeset onto the working copy's base.
    CherryPick(pick::CherryPickArgs),
    /// Undo one changeset with a new local commit.
    Revert(pick::RevertArgs),
    /// Land the current changeset on the target ref.
    Land(land::LandArgs),
    /// Move to the head of the target ref, keeping uncommitted work.
    Sync(checkout::SyncArgs),
    /// List, create or delete branches.
    Branch(branch::BranchArgs),
    /// Switch the working copy to another branch.
    Switch(branch::SwitchArgs),
    /// List, create or delete tags (immutable named revisions).
    Tag(branch::TagArgs),
    /// Show history.
    Log(history::LogArgs),
    /// Show the newest changeset that changed a file or directory.
    LastChange(history::LastChangeArgs),
    /// Show one changeset.
    Show(history::ShowArgs),
    /// Print a file at a revision.
    Cat(history::CatArgs),
    /// Search file contents at a revision on the server, like grep.
    Search(search::SearchArgs),
    /// Show who last changed each line.
    Blame(history::BlameArgs),
    /// Summarize which agents and authors made the changesets.
    Provenance(history::ProvenanceArgs),
    /// List refs.
    Refs,
    /// Show how refs moved: who, when, and from where to where.
    Reflog(reflog::ReflogArgs),
    /// Move a ref back to where it was before its last (or a chosen) move.
    Undo(reflog::UndoArgs),
    /// Approve or reject changesets, and see what a changeset needs to land.
    #[command(subcommand)]
    Review(review::ReviewCmd),
    /// List the reviews a changeset has (same as `review list`).
    Reviews(review::ListArgs),
}

pub fn main() -> ExitCode {
    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(e) => return usage_error(e),
    };
    let json = cli.json;
    match run(cli) {
        Ok(code) => ExitCode::from(code as u8),
        Err(e) => {
            e.report(json);
            ExitCode::from(e.exit_code() as u8)
        }
    }
}

/// clap's own output for help and version; for a real usage error also the
/// JSON document when `--json` is on the command line, which clap could not
/// parse for us.
fn usage_error(e: clap::Error) -> ExitCode {
    let code = e.exit_code();
    let json = std::env::args_os().skip(1).any(|a| a == "--json");
    if code == 0 || !json {
        let _ = e.print();
        return ExitCode::from(code as u8);
    }
    let text = e.render().to_string();
    let mut lines = text.lines().map(str::trim).filter(|l| !l.is_empty());
    let message = lines
        .next()
        .unwrap_or("bad command line")
        .trim_start_matches("error: ")
        .to_string();
    let rest: Vec<&str> = lines.collect();
    let mut err = CliError::new(ErrorKind::Usage, message);
    if !rest.is_empty() {
        err = err.with_hint(rest.join("\n"));
    }
    err.report(true);
    ExitCode::from(code as u8)
}

fn env_token() -> Option<String> {
    std::env::var("LEEP_TOKEN").ok()
}

pub fn run(cli: Cli) -> Result<i32> {
    if let Command::Keygen(args) = &cli.command {
        return keygen::run(args);
    }
    let cache = Cache::from_env()?;
    if let Command::Init(args) = &cli.command {
        let server = cli.server.as_deref().unwrap_or(DEFAULT_SERVER);
        let token = resolve_token(cli.token.as_deref(), env_token().as_deref(), None);
        // Only an explicit `--token` is persisted, never an ambient env var.
        let store = cli.token.as_deref().is_some_and(|t| !t.trim().is_empty());
        return init::run(args, server, token, store, cache, cli.json).map(|(_, code)| code);
    }

    let wc = WorkCopy::discover(&std::env::current_dir()?);
    let server_for = |wc: Option<&WorkCopy>| -> Result<String> {
        cli.server
            .clone()
            .or_else(|| wc.map(|w| w.config.server.clone()))
            .ok_or_else(|| CliError::user("no server configured; use --server or LEEP_SERVER"))
    };

    // Server-only commands work outside a working copy when a server is
    // given. Reviews and undo write to the server but touch no local state.
    let read_only = matches!(
        cli.command,
        Command::Log(_)
            | Command::LastChange(_)
            | Command::Show(_)
            | Command::Cat(_)
            | Command::Blame(_)
            | Command::Search(_)
            | Command::Provenance(_)
            | Command::Refs
            | Command::Reflog(_)
            | Command::Undo(_)
            | Command::Review(_)
            | Command::Reviews(_)
    );
    if read_only && wc.is_err() {
        let token = resolve_token(cli.token.as_deref(), env_token().as_deref(), None);
        let client = Client::with_token(&server_for(None)?, token.as_deref());
        let remote = Remote {
            client: &client,
            cache: &cache,
            base: None,
            target_ref: "main",
            json: cli.json,
        };
        return read_only_cmd(&cli.command, &remote);
    }

    let wc = wc?;
    let token = resolve_token(
        cli.token.as_deref(),
        env_token().as_deref(),
        wc.config.token.as_deref(),
    );
    let client = Client::with_token(&server_for(Some(&wc))?, token.as_deref());
    let mut ctx = Ctx {
        wc,
        client,
        cache,
        json: cli.json,
    };
    match &cli.command {
        Command::Init(_) | Command::Keygen(_) => unreachable!("handled above"),
        Command::Checkout(a) => checkout::checkout(&mut ctx, a),
        Command::Status => status::run(&ctx),
        Command::Diff(a) => diff::run(&ctx, a),
        Command::Commit(a) => commit::run(&mut ctx, a),
        Command::Amend(a) => amend::run(&mut ctx, a),
        Command::Stash(a) => stash::run(&mut ctx, a),
        Command::Restore(a) => restore::run(&mut ctx, a),
        Command::Resolve(a) => resolve::run(&mut ctx, a),
        Command::Reset(a) => reset::run(&mut ctx, a),
        Command::Merge(a) => merge::run(&mut ctx, a),
        Command::Rebase(a) => rebase::run(&mut ctx, a),
        Command::CherryPick(a) => pick::cherry_pick(&mut ctx, a),
        Command::Revert(a) => pick::revert(&mut ctx, a),
        Command::Land(a) => land::run(&mut ctx, a),
        Command::Sync(a) => checkout::sync(&mut ctx, a),
        Command::Branch(a) => branch::branch(&ctx, a),
        Command::Switch(a) => branch::switch(&mut ctx, a),
        Command::Tag(a) => branch::tag(&ctx, a),
        other => read_only_cmd(other, &ctx.remote()),
    }
}

fn read_only_cmd(cmd: &Command, r: &Remote) -> Result<i32> {
    match cmd {
        Command::Log(a) => history::log(r, a),
        Command::LastChange(a) => history::last_change(r, a),
        Command::Show(a) => history::show(r, a),
        Command::Cat(a) => history::cat(r, a),
        Command::Blame(a) => history::blame(r, a),
        Command::Search(a) => search::run(r, a),
        Command::Provenance(a) => history::provenance(r, a),
        Command::Refs => history::refs(r),
        Command::Reflog(a) => reflog::reflog(r, a),
        Command::Undo(a) => reflog::undo(r, a),
        Command::Review(c) => review::run(r, c),
        Command::Reviews(a) => review::list(r, a),
        _ => unreachable!("not a read-only command"),
    }
}
