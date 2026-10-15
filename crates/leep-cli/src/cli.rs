//! Argument parsing and dispatch.

use std::process::ExitCode;

use clap::{Parser, Subcommand};

use crate::cache::Cache;
use crate::client::Client;
use crate::commands::{
    branch, checkout, commit, diff, history, init, keygen, land, review, search, status, Ctx,
    Remote,
};
use crate::error::{CliError, Result};
use crate::state::{resolve_token, WorkCopy};

const DEFAULT_SERVER: &str = "http://127.0.0.1:8080";

#[derive(Parser, Debug)]
#[command(
    name = "leep",
    version,
    about = "Client for the Leep version control server"
)]
pub struct Cli {
    /// Server URL (overrides the working copy config).
    #[arg(long, global = true, env = "LEEP_SERVER")]
    pub server: Option<String>,
    /// Bearer token (or set LEEP_TOKEN; falls back to `.leep/config.json`).
    #[arg(long, global = true, hide_env_values = true)]
    pub token: Option<String>,
    /// Print the raw API response as JSON.
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
    /// Land the current changeset on the target ref.
    Land(land::LandArgs),
    /// Move to the head of the target ref.
    Sync,
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
    /// Approve or reject changesets, and see what a changeset needs to land.
    #[command(subcommand)]
    Review(review::ReviewCmd),
    /// List the reviews a changeset has (same as `review list`).
    Reviews(review::ListArgs),
}

pub fn main() -> ExitCode {
    let cli = Cli::parse();
    match run(cli) {
        Ok(code) => ExitCode::from(code as u8),
        Err(e) => {
            eprintln!("leep: {e}");
            ExitCode::from(e.exit_code() as u8)
        }
    }
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
    // given. Reviews write to the server but touch no local state.
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
        Command::Land(a) => land::run(&mut ctx, a),
        Command::Sync => checkout::sync(&mut ctx),
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
        Command::Review(c) => review::run(r, c),
        Command::Reviews(a) => review::list(r, a),
        _ => unreachable!("not a read-only command"),
    }
}
