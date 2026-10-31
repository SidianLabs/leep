//! `leep bisect`: find the changeset that introduced a bug by binary search,
//! checking out one candidate at a time.

use std::collections::HashSet;
use std::process::{Command, Stdio};

use clap::{Args, Subcommand};
use leep_core::api::ChangesetInfo;
use leep_core::outln;
use leep_core::ChangesetId;
use serde::Serialize;

use super::{print_json, Ctx};
use crate::bisect::{self, BisectState, History, Mark, Next, Node};
use crate::client::{Client, LogQuery};
use crate::error::{CliError, Result};
use crate::fmt::short;
use crate::materialize::materialize;
use crate::status::{status, Change};

#[derive(Subcommand, Debug)]
pub enum BisectCmd {
    /// Start bisecting, optionally marking the bad and good changesets.
    Start(StartArgs),
    /// Mark a changeset bad (default: the one checked out).
    Bad(BadArgs),
    /// Mark changesets good (default: the one checked out).
    Good(RevsArgs),
    /// Mark changesets untestable (default: the one checked out).
    Skip(RevsArgs),
    /// End the bisect and return to the base and target ref it started at.
    Reset,
    /// Show the marks made so far.
    Log,
    /// Mark each candidate by running a command: exit 0 good, 125 skip,
    /// 1 to 127 bad, anything else stops.
    Run(RunArgs),
}

#[derive(Args, Debug)]
pub struct StartArgs {
    /// A bad revision, usually where the bug shows.
    pub bad: Option<String>,
    /// Good revisions, where the bug does not show.
    pub good: Vec<String>,
}

#[derive(Args, Debug)]
pub struct BadArgs {
    pub rev: Option<String>,
}

#[derive(Args, Debug)]
pub struct RevsArgs {
    pub revs: Vec<String>,
}

#[derive(Args, Debug)]
pub struct RunArgs {
    /// The command and its arguments, run in the current directory.
    #[arg(required = true, trailing_var_arg = true, allow_hyphen_values = true)]
    pub cmd: Vec<String>,
}

/// The server's logs, as [`bisect::candidates`] reads them.
struct Server<'a>(&'a Client);

impl History for Server<'_> {
    fn page(
        &mut self,
        from: &ChangesetId,
        after: Option<&str>,
        limit: u32,
    ) -> Result<(Vec<Node>, Option<String>)> {
        let q = LogQuery {
            limit: Some(limit),
            after: after.map(str::to_string),
            ..LogQuery::default()
        };
        let page = self.0.log_summary(from, &q)?;
        let nodes = page
            .entries
            .into_iter()
            .map(|e| Node {
                id: e.id,
                parents: e.parents,
                generation: e.generation,
            })
            .collect();
        Ok((nodes, page.next))
    }

    fn generation(&mut self, id: &ChangesetId) -> Result<u64> {
        Ok(self.0.get_changeset(id)?.generation)
    }
}

pub fn run(ctx: &mut Ctx, cmd: &BisectCmd) -> Result<i32> {
    match cmd {
        BisectCmd::Start(a) => start(ctx, a),
        BisectCmd::Bad(a) => mark(ctx, Mark::Bad, a.rev.as_slice()),
        BisectCmd::Good(a) => mark(ctx, Mark::Good, &a.revs),
        BisectCmd::Skip(a) => mark(ctx, Mark::Skip, &a.revs),
        BisectCmd::Reset => reset(ctx),
        BisectCmd::Log => log(ctx),
        BisectCmd::Run(a) => run_cmd(ctx, a),
    }
}

fn load(ctx: &Ctx) -> Result<BisectState> {
    BisectState::load(&ctx.wc.root)?
        .ok_or_else(|| CliError::user("not bisecting; `leep bisect start` begins"))
}

/// Each step checks out another changeset, which would overwrite edits to
/// tracked files; untracked files (build output) may stay.
fn require_tracked_clean(ctx: &Ctx) -> Result<()> {
    ctx.refuse_in_progress()?;
    let st = status(&ctx.wc)?;
    st.refuse_conflicts()?;
    if let Some(e) = st.entries.iter().find(|e| e.change != Change::Added) {
        return Err(CliError::user(format!(
            "{} has uncommitted changes that checking out another changeset would \
             overwrite; `leep restore {}` discards them",
            e.path, e.path
        )));
    }
    Ok(())
}

fn subject(info: &ChangesetInfo) -> String {
    info.changeset
        .message
        .lines()
        .next()
        .unwrap_or("")
        .to_string()
}

fn record(ctx: &Ctx, bs: &mut BisectState, op: Mark, rev: &str) -> Result<()> {
    let id = ctx.remote().resolve(rev)?;
    let info = ctx.client.get_changeset(&id)?;
    bs.mark(op, id, subject(&info))
}

fn start(ctx: &mut Ctx, args: &StartArgs) -> Result<i32> {
    if bisect::in_progress(&ctx.wc.root) {
        return Err(CliError::user(
            "a bisect is already in progress; `leep bisect reset` ends it",
        ));
    }
    ctx.require_clean()?;
    let base = ctx.wc.state.base.ok_or_else(|| {
        CliError::user("the working copy has no base yet; `leep checkout` a revision first")
    })?;
    let mut bs = BisectState {
        original_base: base,
        original_ref: ctx.wc.config.target_ref.clone(),
        bad: None,
        good: vec![],
        skip: vec![],
        log: vec![],
    };
    if let Some(b) = &args.bad {
        record(ctx, &mut bs, Mark::Bad, b)?;
    }
    for g in &args.good {
        record(ctx, &mut bs, Mark::Good, g)?;
    }
    let out = advance(ctx, &mut bs)?;
    report(ctx, &bs, &out)
}

fn mark(ctx: &mut Ctx, op: Mark, revs: &[String]) -> Result<i32> {
    let mut bs = load(ctx)?;
    require_tracked_clean(ctx)?;
    let at = ["@".to_string()];
    let revs = if revs.is_empty() { &at[..] } else { revs };
    for r in revs {
        record(ctx, &mut bs, op, r)?;
    }
    let out = advance(ctx, &mut bs)?;
    report(ctx, &bs, &out)
}

/// The result of a step.
enum Outcome {
    /// A bad or a good changeset is still to be marked.
    Waiting,
    /// `info` is checked out for testing.
    Testing {
        info: Box<ChangesetInfo>,
        remaining: usize,
        steps: u32,
    },
    Found(Box<ChangesetInfo>),
    OnlySkipped(Vec<ChangesetId>),
}

/// Work out the next step from the marks, save them, and check out the
/// next candidate. Nothing is saved if the marks make no sense.
fn advance(ctx: &mut Ctx, bs: &mut BisectState) -> Result<Outcome> {
    let Some(bad) = bs.bad.filter(|_| !bs.good.is_empty()) else {
        bs.save(&ctx.wc.root)?;
        return Ok(Outcome::Waiting);
    };
    let c = bisect::candidates(&mut Server(&ctx.client), bad, &bs.good)?;
    if c.nodes.is_empty() {
        return Err(CliError::user(format!(
            "bad changeset {} is in the history of a good one; were they marked the \
             wrong way round? `leep bisect log` shows the marks",
            short(bad)
        )));
    }
    let skip: HashSet<ChangesetId> = bs.skip.iter().copied().collect();
    match bisect::choose(&c, &skip) {
        Next::Test {
            id,
            remaining,
            steps,
        } => {
            let info = ctx.client.get_changeset(&id)?;
            bs.save(&ctx.wc.root)?;
            if ctx.wc.state.base != Some(id) {
                materialize(ctx, id, false)?;
            }
            Ok(Outcome::Testing {
                info: Box::new(info),
                remaining,
                steps,
            })
        }
        Next::Found(id) => {
            let info = ctx.client.get_changeset(&id)?;
            let logged = bs
                .log
                .last()
                .is_some_and(|e| e.op == Mark::FirstBad && e.id == id);
            if !logged {
                bs.mark(Mark::FirstBad, id, subject(&info))?;
            }
            bs.save(&ctx.wc.root)?;
            Ok(Outcome::Found(Box::new(info)))
        }
        Next::OnlySkipped(ids) => {
            bs.save(&ctx.wc.root)?;
            Ok(Outcome::OnlySkipped(ids))
        }
    }
}

#[derive(Serialize)]
struct StepJson<'a> {
    /// `waiting`, `testing`, `found` or `only_skipped`.
    status: &'static str,
    bad: Option<ChangesetId>,
    good: &'a [ChangesetId],
    skip: &'a [ChangesetId],
    #[serde(skip_serializing_if = "Option::is_none")]
    current: Option<ChangesetId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    remaining: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    steps: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    first_bad: Option<&'a ChangesetInfo>,
    #[serde(skip_serializing_if = "Option::is_none")]
    candidates: Option<&'a [ChangesetId]>,
    #[serde(skip_serializing_if = "Option::is_none")]
    runs: Option<&'a [RunJson]>,
}

#[derive(Serialize)]
struct RunJson {
    id: ChangesetId,
    exit_code: i32,
    result: &'static str,
}

fn step_json<'a>(
    bs: &'a BisectState,
    out: &'a Outcome,
    runs: Option<&'a [RunJson]>,
) -> StepJson<'a> {
    let mut j = StepJson {
        status: "waiting",
        bad: bs.bad,
        good: &bs.good,
        skip: &bs.skip,
        current: None,
        remaining: None,
        steps: None,
        first_bad: None,
        candidates: None,
        runs,
    };
    match out {
        Outcome::Waiting => {}
        Outcome::Testing {
            info,
            remaining,
            steps,
        } => {
            j.status = "testing";
            j.current = Some(info.id);
            j.remaining = Some(*remaining);
            j.steps = Some(*steps);
        }
        Outcome::Found(info) => {
            j.status = "found";
            j.first_bad = Some(info);
        }
        Outcome::OnlySkipped(ids) => {
            j.status = "only_skipped";
            j.candidates = Some(ids);
        }
    }
    j
}

/// Exit code of an outcome: 1 when only skipped changesets are left, since
/// the search could not finish.
fn code(out: &Outcome) -> i32 {
    match out {
        Outcome::OnlySkipped(_) => 1,
        _ => 0,
    }
}

fn report(ctx: &Ctx, bs: &BisectState, out: &Outcome) -> Result<i32> {
    if ctx.json {
        print_json(&step_json(bs, out, None))?;
    } else {
        print_outcome(ctx, bs, out)?;
    }
    Ok(code(out))
}

fn print_outcome(ctx: &Ctx, bs: &BisectState, out: &Outcome) -> Result<()> {
    let plural = |n: usize, one: &str, many: &str| {
        if n == 1 {
            format!("{n} {one}")
        } else {
            format!("{n} {many}")
        }
    };
    match out {
        Outcome::Waiting => match (bs.bad, bs.good.is_empty()) {
            (None, true) => outln!("status: waiting for both good and bad changesets"),
            (None, false) => outln!(
                "status: waiting for bad changeset, {} known",
                plural(bs.good.len(), "good changeset", "good changesets")
            ),
            _ => outln!("status: waiting for good changeset(s), bad changeset known"),
        },
        Outcome::Testing {
            info,
            remaining,
            steps,
        } => {
            outln!(
                "Bisecting: {} left to test after this (roughly {})",
                plural(*remaining, "revision", "revisions"),
                plural(*steps as usize, "step", "steps")
            );
            outln!("[{}] {}", info.id, subject(info));
        }
        Outcome::Found(info) => {
            outln!("{} is the first bad changeset", info.id);
            let text = super::history::show_text(&ctx.remote(), info)?;
            leep_core::out!("{text}");
        }
        Outcome::OnlySkipped(ids) => {
            outln!("There are only 'skip'ped changesets left to test.");
            outln!("The first bad changeset could be any of:");
            for id in ids {
                outln!("{id}");
            }
            outln!("We cannot bisect more!");
        }
    }
    Ok(())
}

fn reset(ctx: &mut Ctx) -> Result<i32> {
    let Some(bs) = BisectState::load(&ctx.wc.root)? else {
        return Err(CliError::nothing_to_do("not bisecting"));
    };
    require_tracked_clean(ctx)?;
    let stats = materialize(ctx, bs.original_base, false)?;
    if ctx.wc.config.target_ref != bs.original_ref {
        ctx.wc.config.target_ref = bs.original_ref.clone();
        ctx.wc.save_config()?;
    }
    BisectState::remove(&ctx.wc.root)?;
    if ctx.json {
        print_json(&serde_json::json!({
            "base": bs.original_base,
            "target_ref": bs.original_ref,
            "written": stats.written,
            "removed": stats.removed,
        }))?;
    } else {
        outln!(
            "Back at {} on {} ({} written, {} removed)",
            short(bs.original_base),
            bs.original_ref,
            stats.written,
            stats.removed
        );
    }
    Ok(0)
}

fn log(ctx: &Ctx) -> Result<i32> {
    let bs = load(ctx)?;
    if ctx.json {
        print_json(&serde_json::json!({
            "original_base": bs.original_base,
            "original_ref": bs.original_ref,
            "bad": bs.bad,
            "good": bs.good,
            "skip": bs.skip,
            "entries": bs.log,
        }))?;
        return Ok(0);
    }
    outln!("leep bisect start");
    for e in &bs.log {
        outln!("# {}: [{}] {}", e.op.name(), e.id, e.subject);
        if e.op != Mark::FirstBad {
            outln!("leep bisect {} {}", e.op.name(), e.id);
        }
    }
    Ok(0)
}

fn run_cmd(ctx: &mut Ctx, args: &RunArgs) -> Result<i32> {
    let mut bs = load(ctx)?;
    if bs.bad.is_none() || bs.good.is_empty() {
        return Err(CliError::user(
            "`leep bisect run` needs a bad and a good changeset; mark them with \
             `leep bisect bad` and `leep bisect good`",
        ));
    }
    let shown = args.cmd.join(" ");
    let mut runs = Vec::new();
    loop {
        require_tracked_clean(ctx)?;
        let current = ctx
            .wc
            .state
            .base
            .ok_or_else(|| CliError::user("the working copy has no base"))?;
        if !ctx.json {
            outln!("running {shown}");
        }
        let _ = std::io::Write::flush(&mut std::io::stdout());
        let mut cmd = Command::new(&args.cmd[0]);
        cmd.args(&args.cmd[1..]);
        if ctx.json {
            // stdout holds exactly one JSON document.
            cmd.stdout(Stdio::from(std::io::stderr()));
        }
        let status = cmd
            .status()
            .map_err(|e| CliError::user(format!("bisect run failed: cannot run {shown}: {e}")))?;
        let Some(exit_code) = status.code() else {
            return Err(CliError::user(format!(
                "bisect run failed: {shown} was killed by a signal"
            )));
        };
        let op = match exit_code {
            0 => Mark::Good,
            125 => Mark::Skip,
            1..=127 => Mark::Bad,
            _ => {
                return Err(CliError::user(format!(
                    "bisect run failed: exit code {exit_code} from {shown} is < 0 or >= 128"
                )))
            }
        };
        let info = ctx.client.get_changeset(&current)?;
        bs.mark(op, current, subject(&info))?;
        runs.push(RunJson {
            id: current,
            exit_code,
            result: op.name(),
        });
        let out = advance(ctx, &mut bs)?;
        if let Outcome::Testing { .. } = out {
            if !ctx.json {
                print_outcome(ctx, &bs, &out)?;
            }
            continue;
        }
        if ctx.json {
            print_json(&step_json(&bs, &out, Some(&runs)))?;
        } else {
            print_outcome(ctx, &bs, &out)?;
            if matches!(out, Outcome::Found(_)) {
                outln!("bisect found the first bad changeset");
            }
        }
        return Ok(code(&out));
    }
}
