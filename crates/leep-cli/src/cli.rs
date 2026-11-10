//! Argument parsing and dispatch.

use std::ffi::OsString;
use std::process::ExitCode;
use std::sync::{Arc, Mutex};

use leep_core::api::Notice;

use clap::{CommandFactory, FromArgMatches, Parser, Subcommand};

use crate::cache::Cache;
use crate::client::Client;
use crate::commands::{
    amend, bisect, branch, checkout, checks, commit, config, context, diff, history, init, keygen,
    land, merge, note, pick, rebase, reflog, reset, resolve, restore, review, search, session,
    stash, status, watch, Ctx, Remote,
};
use crate::error::{CliError, ErrorKind, Result};
use crate::help;
use crate::state::{resolve_token, WorkCopy};
use crate::term;

const DEFAULT_SERVER: &str = "http://127.0.0.1:8080";

#[derive(Parser, Debug)]
#[command(
    name = "leep",
    version,
    about = "Client for the Leep version control server",
    after_help = help::EXIT_CODES
)]
pub struct Cli {
    /// Server URL (overrides the working copy config).
    #[arg(long, global = true, env = "LEEP_SERVER", hide = true)]
    pub server: Option<String>,
    /// Bearer token (or set LEEP_TOKEN; falls back to `.leep/config.json`).
    #[arg(long, global = true, hide_env_values = true, hide = true)]
    pub token: Option<String>,
    /// Print the raw API response as JSON; an error becomes
    /// `{"error":{"code","message","hint","retryable"}}` on stdout.
    #[arg(long, global = true, hide = true)]
    pub json: bool,
    /// Colour human output: auto (on a terminal, honouring NO_COLOR and
    /// CLICOLOR_FORCE), always or never.
    #[arg(
        long,
        global = true,
        hide = true,
        value_enum,
        default_value_t,
        value_name = "WHEN"
    )]
    pub color: term::ColorChoice,
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    /// Create a working copy.
    #[command(after_help = help::INIT)]
    Init(init::InitArgs),
    /// Materialize a revision.
    #[command(after_help = help::CHECKOUT)]
    Checkout(checkout::CheckoutArgs),
    /// Show added, modified, deleted and conflicted files.
    #[command(after_help = help::STATUS)]
    Status,
    /// Show changes (working copy vs base, one changeset, or `FROM TO`).
    #[command(after_help = help::DIFF)]
    Diff(diff::DiffArgs),
    /// Create an author signing key for `leep commit --sign`.
    Keygen(keygen::KeygenArgs),
    /// Read or set Leep's own settings (the author of new changesets).
    #[command(after_help = help::CONFIG)]
    Config(config::ConfigArgs),
    /// Record local changes as a changeset.
    #[command(after_help = help::COMMIT)]
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
    #[command(after_help = help::LAND)]
    Land(land::LandArgs),
    /// Move to the head of the target ref, keeping uncommitted work.
    #[command(after_help = help::SYNC)]
    Sync(checkout::SyncArgs),
    /// List, create or delete branches.
    #[command(after_help = help::BRANCH)]
    Branch(branch::BranchArgs),
    /// Switch the working copy to another branch.
    #[command(after_help = help::SWITCH)]
    Switch(branch::SwitchArgs),
    /// List, create or delete tags (immutable named revisions).
    Tag(branch::TagArgs),
    /// Show history.
    #[command(after_help = help::LOG)]
    Log(history::LogArgs),
    /// Find the changeset that introduced a bug by binary search.
    #[command(subcommand, after_help = help::BISECT)]
    Bisect(bisect::BisectCmd),
    /// Show the newest changeset that changed a file or directory.
    LastChange(history::LastChangeArgs),
    /// Show one changeset.
    #[command(after_help = help::SHOW)]
    Show(history::ShowArgs),
    /// Print a file at a revision.
    Cat(history::CatArgs),
    /// Search file contents at a revision on the server, like grep.
    #[command(after_help = help::SEARCH)]
    Search(search::SearchArgs),
    /// Show who last changed each line.
    #[command(after_help = help::BLAME)]
    Blame(history::BlameArgs),
    /// Summarize which agents and authors made the changesets.
    Provenance(history::ProvenanceArgs),
    /// List refs.
    Refs,
    /// Show how refs moved: who, when, and from where to where.
    Reflog(reflog::ReflogArgs),
    /// Move a ref back to where it was before its last (or a chosen) move.
    #[command(after_help = help::UNDO)]
    Undo(reflog::UndoArgs),
    /// Approve or reject changesets, and see what a changeset needs to land.
    #[command(subcommand, after_help = help::REVIEW)]
    Review(review::ReviewCmd),
    /// List the reviews a changeset has (same as `review list`).
    Reviews(review::ListArgs),
    /// Post CI check results, see a ref's known-good `green/<ref>`, and list
    /// the suspects for a failure.
    #[command(subcommand, after_help = help::CHECKS)]
    Checks(checks::ChecksCmd),
    /// Say what you are working on, so other agents see it before it lands.
    #[command(subcommand, after_help = help::SESSION)]
    Session(session::SessionCmd),
    /// Show who is working on or near a path, before anything lands.
    #[command(after_help = help::WHO)]
    Who(session::WhoArgs),
    /// Write and read notes other agents should know about a file, a
    /// directory or a changeset.
    #[command(subcommand, after_help = help::NOTE)]
    Note(note::NoteCmd),
    /// Print coordination events as they happen: sessions starting,
    /// changing paths and ending, notes added, on or around paths.
    #[command(after_help = help::WATCH)]
    Watch(watch::WatchArgs),
    /// Show what the server knows that bears on a task: files, owners,
    /// last changes, work in flight, ref health, possible duplicates.
    #[command(after_help = help::CONTEXT)]
    Context(context::ContextArgs),
    /// Start work on a task: declare your session and show its context.
    #[command(subcommand, after_help = help::WORK)]
    Work(context::WorkCmd),
}

/// The command line definition with the help text of [`help`].
pub fn command() -> clap::Command {
    help::decorate(Cli::command())
}

pub fn main() -> ExitCode {
    let parsed = command()
        .try_get_matches_from(join_wait_values(std::env::args_os().collect()))
        .and_then(|m| Cli::from_arg_matches(&m));
    let cli = match parsed {
        Ok(cli) => cli,
        Err(e) => return usage_error(e),
    };
    let json = cli.json;
    term::init(cli.color);
    match run(cli) {
        Ok(code) => ExitCode::from(code as u8),
        Err(e) => {
            e.report(json);
            ExitCode::from(e.exit_code() as u8)
        }
    }
}

/// `--wait SECS` for the `review` commands whose `--wait` takes an
/// optional value. clap takes such a value only as `--wait=SECS`, since a
/// bare `--wait` may come right before the positional revision. Here a
/// `--wait` followed by a duration ([`parse_secs`]) becomes `--wait=SECS`
/// when the command still has its revision elsewhere (`inbox` has none), so
/// `review status --wait 600 REV`, `review status REV --wait 600` and
/// `review status --wait REV` all mean what they say.
pub fn join_wait_values(args: Vec<OsString>) -> Vec<OsString> {
    /// Options that take their value as the next argument.
    const VALUED: &[&str] = &[
        "--server",
        "--token",
        "--color",
        "--ref",
        "--to",
        "-m",
        "--message",
        "-n",
        "--limit",
    ];
    let text = |a: &OsString| a.to_str().map(str::to_string);
    // Positional words before `--`, skipping option values, with indices.
    let mut words: Vec<(usize, String)> = Vec::new();
    let mut i = 1;
    while i < args.len() {
        let Some(a) = text(&args[i]) else {
            words.push((i, String::new()));
            i += 1;
            continue;
        };
        if a == "--" {
            break;
        }
        if VALUED.contains(&a.as_str()) {
            i += 2;
            continue;
        }
        if !a.starts_with('-') || a == "-" {
            words.push((i, a));
        }
        i += 1;
    }
    let sub = match words.as_slice() {
        [(_, cmd), (_, sub), ..] if cmd == "review" => sub.as_str(),
        _ => return args,
    };
    let needs_rev = match sub {
        "status" | "request" => true,
        "inbox" => false,
        _ => return args,
    };
    let after_sub = words[1].0;
    let waits: Vec<usize> = (after_sub + 1..args.len().saturating_sub(1))
        .take_while(|&i| args[i] != "--")
        .filter(|&i| {
            args[i] == "--wait" && text(&args[i + 1]).is_some_and(|v| parse_secs(&v).is_ok())
        })
        .collect();
    if waits.is_empty() {
        return args;
    }
    let others = words
        .iter()
        .filter(|(i, _)| *i > after_sub && !waits.contains(&(i - 1)))
        .count();
    if needs_rev && others == 0 {
        return args;
    }
    let mut out = Vec::with_capacity(args.len());
    let mut i = 0;
    while i < args.len() {
        if waits.contains(&i) {
            let mut joined = OsString::from("--wait=");
            joined.push(&args[i + 1]);
            out.push(joined);
            i += 2;
        } else {
            out.push(args[i].clone());
            i += 1;
        }
    }
    out
}

/// A `--wait` duration: seconds, or a number with `s`, `m` or `h`.
pub fn parse_secs(s: &str) -> std::result::Result<u64, String> {
    let (num, unit) = match s.char_indices().last() {
        Some((i, c @ ('s' | 'm' | 'h'))) => (&s[..i], c),
        _ => (s, 's'),
    };
    let n: u64 = num
        .parse()
        .ok()
        .filter(|_| !num.is_empty() && num.bytes().all(|b| b.is_ascii_digit()))
        .ok_or_else(|| format!("{s:?} is no duration: give seconds, or a number with s, m or h"))?;
    let mul = match unit {
        'm' => 60,
        'h' => 3600,
        _ => 1,
    };
    n.checked_mul(mul)
        .ok_or_else(|| format!("{s:?} is too long a wait"))
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

/// Run `cli`, then show on stderr the overlap notices its answers carried
/// (ADR 0096), escaped: they name other agents' work.
pub fn run(cli: Cli) -> Result<i32> {
    let sink: Arc<Mutex<Vec<Notice>>> = Arc::default();
    let out = run_with(cli, &sink);
    let notices = std::mem::take(&mut *sink.lock().unwrap());
    for line in session::notice_lines(&notices) {
        eprintln!("{line}");
    }
    out
}

fn run_with(cli: Cli, sink: &Arc<Mutex<Vec<Notice>>>) -> Result<i32> {
    if let Command::Keygen(args) = &cli.command {
        return keygen::run(args);
    }
    if let Command::Config(args) = &cli.command {
        return config::run(args, cli.json);
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

    // Sessions work in a working copy or, with LEEP_AGENT_SESSION, outside
    // one; `start` keeps the key it makes in the working copy.
    if let Command::Session(_)
    | Command::Who(_)
    | Command::Note(_)
    | Command::Watch(_)
    | Command::Context(_)
    | Command::Work(_) = &cli.command
    {
        let mut wc = wc.ok();
        let token = resolve_token(
            cli.token.as_deref(),
            env_token().as_deref(),
            wc.as_ref().and_then(|w| w.config.token.as_deref()),
        );
        let client = Client::with_token(&server_for(wc.as_ref())?, token.as_deref())
            .with_session(session::session_key(None, wc.as_ref()).as_deref())
            .with_notice_sink(sink.clone());
        let target_ref = wc
            .as_ref()
            .map_or_else(|| "main".to_string(), |w| w.config.target_ref.clone());
        let remote = Remote {
            client: &client,
            cache: &cache,
            base: wc.as_ref().and_then(|w| w.state.base),
            target_ref: &target_ref,
            json: cli.json,
        };
        return match &cli.command {
            Command::Session(c) => session::run(&remote, c, wc.as_mut()),
            Command::Who(a) => session::who(&remote, a, wc.as_ref()),
            Command::Note(c) => note::run(&remote, c),
            Command::Watch(a) => watch::watch(&remote, a),
            Command::Context(a) => context::context(&remote, a),
            Command::Work(c) => context::run(&remote, c, wc.as_mut()),
            _ => unreachable!("matched above"),
        };
    }

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
            | Command::Checks(_)
    );
    if read_only && wc.is_err() {
        let token = resolve_token(cli.token.as_deref(), env_token().as_deref(), None);
        let client = Client::with_token(&server_for(None)?, token.as_deref())
            .with_session(session::session_key(None, None).as_deref())
            .with_notice_sink(sink.clone());
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
    // Every request is a heartbeat of the agent's session (ADR 0095), and
    // a commit names where it is meant to land, so a session inferred from
    // it hears of lands there.
    let client = Client::with_token(&server_for(Some(&wc))?, token.as_deref())
        .with_session(session::session_key(None, Some(&wc)).as_deref())
        .with_target_ref(Some(&wc.config.target_ref))
        .with_notice_sink(sink.clone());
    let mut ctx = Ctx {
        wc,
        client,
        cache,
        json: cli.json,
    };
    match &cli.command {
        Command::Commit(_)
        | Command::Amend(_)
        | Command::Land(_)
        | Command::Merge(_)
        | Command::Rebase(_)
        | Command::CherryPick(_)
        | Command::Revert(_)
        | Command::Sync(_) => ctx.refuse_bisecting()?,
        _ => {}
    }
    match &cli.command {
        Command::Init(_) | Command::Keygen(_) | Command::Config(_) => {
            unreachable!("handled above")
        }
        Command::Checkout(a) => checkout::checkout(&mut ctx, a),
        Command::Bisect(c) => bisect::run(&mut ctx, c),
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
        Command::Checks(c) => checks::run(r, c),
        _ => unreachable!("not a read-only command"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn join(args: &[&str]) -> Vec<String> {
        let args = std::iter::once("leep")
            .chain(args.iter().copied())
            .map(OsString::from)
            .collect();
        join_wait_values(args)
            .into_iter()
            .skip(1)
            .map(|a| a.into_string().unwrap())
            .collect()
    }

    #[test]
    fn a_wait_value_after_a_space_is_joined_only_when_it_is_no_revision() {
        assert_eq!(
            join(&["review", "status", "abc", "--wait", "600"]),
            ["review", "status", "abc", "--wait=600"]
        );
        assert_eq!(
            join(&["review", "status", "--wait", "10m", "abc", "--ref", "main"]),
            ["review", "status", "--wait=10m", "abc", "--ref", "main"]
        );
        // The number is the revision: nothing else is.
        assert_eq!(
            join(&["review", "status", "--wait", "1234"]),
            ["review", "status", "--wait", "1234"]
        );
        // No duration: a bare --wait before the revision.
        assert_eq!(
            join(&["review", "request", "--wait", "abc", "-m", "5"]),
            ["review", "request", "--wait", "abc", "-m", "5"]
        );
        assert_eq!(
            join(&["--server", "x", "review", "inbox", "--wait", "1"]),
            ["--server", "x", "review", "inbox", "--wait=1"]
        );
        assert_eq!(
            join(&["review", "inbox", "--wait", "--mine"]),
            ["review", "inbox", "--wait", "--mine"]
        );
        // Other commands, and anything after `--`, are left alone.
        for args in [
            &["log", "--wait", "5"][..],
            &["review", "approve", "abc", "--wait", "5"],
            &["review", "status", "abc", "--", "--wait", "5"],
        ] {
            assert_eq!(join(args), args);
        }
    }

    #[test]
    fn joined_waits_parse() {
        let parse = |args: &[&str]| {
            let args = std::iter::once("leep").chain(args.iter().copied());
            let args = join_wait_values(args.map(OsString::from).collect());
            Cli::from_arg_matches(&command().try_get_matches_from(args).unwrap()).unwrap()
        };
        let Command::Review(review::ReviewCmd::Status(s)) =
            parse(&["review", "status", "--wait", "2m", "abc"]).command
        else {
            panic!()
        };
        assert_eq!((s.rev.as_str(), s.wait), ("abc", Some(120)));
        let Command::Review(review::ReviewCmd::Status(s)) =
            parse(&["review", "status", "--wait", "abc"]).command
        else {
            panic!()
        };
        assert_eq!((s.rev.as_str(), s.wait), ("abc", Some(3600)));
    }

    #[test]
    fn durations_take_seconds_minutes_and_hours() {
        assert_eq!(parse_secs("90"), Ok(90));
        assert_eq!(parse_secs("90s"), Ok(90));
        assert_eq!(parse_secs("2m"), Ok(120));
        assert_eq!(parse_secs("1h"), Ok(3600));
        assert_eq!(parse_secs("0"), Ok(0));
        for bad in ["", "m", "-5", "+5", "1.5", "abc", "5d", "1 m"] {
            assert!(parse_secs(bad).is_err(), "{bad}");
        }
    }
}
