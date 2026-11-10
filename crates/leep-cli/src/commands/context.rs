//! `leep context` and `leep work start` (ADR 0097): what the server knows
//! that bears on a task, before the agent changes anything. `work start`
//! declares (or resumes) the session and prints the pack in one command.
//!
//! Paths, titles, authors, models and owner names in a pack were written
//! by agents and authors: they are printed quoted and escaped (`{:?}`), so
//! no newline or escape sequence reaches the terminal as text of its own.

use clap::{Args, Subcommand};
use leep_core::api::{ContextPack, ContextRequest, ContextText};
use leep_core::outln;

use super::session::{self, notice_lines, word, DeclareArgs};
use super::{print_json, Remote};
use crate::error::{CliError, ErrorKind, Result};
use crate::fmt::{plural, short};
use crate::state::WorkCopy;

#[derive(Args, Debug, Default)]
pub struct ContextArgs {
    /// Files or directories the task starts from.
    pub paths: Vec<String>,
    /// The task in plain text; identifiers in it are searched for.
    /// Default: your session's title and approach.
    #[arg(short = 't', long, value_name = "TEXT")]
    pub task: Option<String>,
    /// An exact key of the task (an issue id), to find others who have it.
    #[arg(long = "task-key", value_name = "KEY")]
    pub task_key: Option<String>,
    /// The ref the work is for (default: the target ref).
    #[arg(long = "ref")]
    pub target_ref: Option<String>,
    /// Bytes the pack may hold (default 32768, at most 262144).
    #[arg(long, value_name = "BYTES")]
    pub budget: Option<u64>,
}

#[derive(Subcommand, Debug)]
pub enum WorkCmd {
    /// Start (or resume) your session and print the context for its task.
    Start(WorkStartArgs),
}

#[derive(Args, Debug)]
pub struct WorkStartArgs {
    #[command(flatten)]
    pub declare: DeclareArgs,
    /// Bytes the context may hold (default 32768, at most 262144).
    #[arg(long, value_name = "BYTES")]
    pub budget: Option<u64>,
}

/// A server older than ADR 0097 has no `/v1/context`.
fn unsupported(e: CliError) -> CliError {
    if e.kind() == ErrorKind::NotFound && e.server_code().is_none() {
        return CliError::user("this server makes no context packs; upgrade it (ADR 0097)");
    }
    e
}

pub fn context(r: &Remote, a: &ContextArgs) -> Result<i32> {
    let req = ContextRequest {
        task: a.task.clone().unwrap_or_default(),
        paths: a.paths.clone(),
        target_ref: Some(
            a.target_ref
                .clone()
                .unwrap_or_else(|| r.target_ref.to_string()),
        ),
        session: r.client.session(),
        task_key: a.task_key.clone(),
        budget_bytes: a.budget,
    };
    if req.task.trim().is_empty() && req.session.is_none() {
        return Err(CliError::user(
            "say what the task is: `leep context --task \"...\"`, or start a session first",
        ));
    }
    let pack = r.client.context(&req).map_err(unsupported)?;
    if r.json {
        print_json(&pack)?;
        return Ok(0);
    }
    for line in pack_lines(&pack) {
        outln!("{line}");
    }
    Ok(0)
}

pub fn run(r: &Remote, cmd: &WorkCmd, wc: Option<&mut WorkCopy>) -> Result<i32> {
    match cmd {
        WorkCmd::Start(a) => start(r, a, wc),
    }
}

/// `leep work start`: `leep session start`, then `leep context` for the
/// session's title and approach and its scope and claims.
fn start(r: &Remote, a: &WorkStartArgs, wc: Option<&mut WorkCopy>) -> Result<i32> {
    let d = &a.declare;
    let Some(title) = d.title.as_deref().filter(|t| !t.trim().is_empty()) else {
        return Err(CliError::user(
            "say what you are about to do: `leep work start -t \"...\"`",
        ));
    };
    let (key, declared) = session::declare_session(r, d, wc, true)?;
    let task = match d.approach.as_deref() {
        Some(ap) if !ap.trim().is_empty() => format!("{title}\n{ap}"),
        _ => title.to_string(),
    };
    let req = ContextRequest {
        task,
        paths: d.scope.iter().chain(&d.claims).cloned().collect(),
        target_ref: Some(
            d.target_ref
                .clone()
                .unwrap_or_else(|| r.target_ref.to_string()),
        ),
        session: Some(key.clone()),
        task_key: d.task_key.clone(),
        budget_bytes: a.budget,
    };
    let pack = r.client.context(&req).map_err(unsupported)?;
    if r.json {
        print_json(&serde_json::json!({ "session": declared, "context": pack }))?;
        return Ok(0);
    }
    for line in session::declared_lines(&key, &declared, true) {
        outln!("{line}");
    }
    for line in pack_lines(&pack) {
        outln!("{line}");
    }
    Ok(0)
}

/// `"a" "b"`: names and paths quoted and escaped.
fn quoted(v: &[String]) -> String {
    v.iter()
        .map(|s| format!("{s:?}"))
        .collect::<Vec<_>>()
        .join(" ")
}

/// ` by "x" "title" model "m"`: what an agent or author wrote about a
/// change, each part quoted and escaped.
fn written(t: &ContextText) -> String {
    let mut s = String::new();
    if let Some(a) = &t.author {
        s.push_str(&format!(" by {a:?}"));
    }
    if let Some(m) = &t.agent_model {
        s.push_str(&format!(" model {m:?}"));
    }
    if let Some(title) = &t.title {
        s.push_str(&format!(" {title:?}"));
    }
    s
}

fn path_of(t: &ContextText) -> String {
    format!("{:?}", t.path.as_deref().unwrap_or_default())
}

/// How a pack is shown: a heading, then each non-empty section, then
/// what the budget cut and which parts the server could not make.
pub fn pack_lines(p: &ContextPack) -> Vec<String> {
    let mut out = vec![format!(
        "context for {:?} at {} ({} of {} bytes)",
        p.target_ref,
        short(p.head),
        p.bytes,
        p.budget_bytes
    )];
    if let Some(s) = p.session {
        out.push(format!("for session {s}"));
    }
    if !p.files.is_empty() {
        out.push("files:".into());
        for f in &p.files {
            let why: Vec<String> = f.why.iter().map(|w| word(w)).collect();
            let mut line = format!("  {} [{}]", path_of(&f.untrusted), why.join(", "));
            if f.matches > 0 {
                line.push_str(&format!(" {}", plural(f.matches as usize, "match")));
            }
            if f.co_changes > 0 {
                line.push_str(&format!(" changed with it {}x", f.co_changes));
            }
            out.push(line);
        }
    }
    if !p.owners.is_empty() {
        out.push("owners:".into());
        for o in &p.owners {
            let mut line = format!(
                "  {}: {}",
                path_of(&o.untrusted),
                quoted(&o.untrusted.owners)
            );
            if let Some(c) = &o.carry {
                line.push_str(&format!(" (carry {}", word(c)));
                if let Some(f) = &o.untrusted.carry_file {
                    line.push_str(&format!(" from {f:?}"));
                }
                line.push(')');
            }
            out.push(line);
        }
    }
    if !p.history.is_empty() {
        out.push("last changes:".into());
        for h in &p.history {
            let mut line = format!("  {}: {}", path_of(&h.untrusted), short(h.changeset));
            if let Some(by) = &h.created_by {
                line.push_str(&format!(" from {by:?}"));
            }
            line.push_str(&written(&h.untrusted));
            out.push(line);
        }
    }
    if !p.in_flight.is_empty() {
        out.push("work in flight:".into());
        let notices: Vec<_> = p.in_flight.iter().map(|w| w.notice.clone()).collect();
        for line in notice_lines(&notices) {
            out.push(format!("  {}", line.trim_start_matches("notice: ")));
        }
    }
    if !p.duplicates.is_empty() {
        out.push("possible duplicates:".into());
        let notices: Vec<_> = p.duplicates.iter().map(|w| w.notice.clone()).collect();
        for line in notice_lines(&notices) {
            out.push(format!("  {}", line.trim_start_matches("notice: ")));
        }
    }
    if let Some(h) = &p.ref_health {
        let mut line = format!("ref: head {}", short(h.head));
        match h.green {
            Some(g) => {
                line.push_str(&format!(" {:?} at {}", h.green_ref, short(g)));
                if let Some(b) = h.behind {
                    line.push_str(&format!(" ({b} behind)"));
                }
            }
            None => line.push_str(&format!(" {:?} not set", h.green_ref)),
        }
        if !h.required_checks.is_empty() {
            line.push_str(&format!(", requires {}", quoted(&h.required_checks)));
        }
        out.push(line);
        if let Some(red) = &h.red {
            out.push(format!(
                "  red: {:?} fails since {}",
                red.check,
                short(red.first_failed)
            ));
        }
        for s in &h.suspects {
            let mut line = format!("  suspect {} (#{})", short(s.changeset), s.seq);
            if let Some(by) = &s.created_by {
                line.push_str(&format!(" from {by:?}"));
            }
            line.push_str(&written(&s.untrusted));
            line.push_str(&format!(" touching {}", quoted(&s.untrusted.paths)));
            out.push(line);
        }
    }
    for c in &p.cut {
        out.push(format!(
            "cut: {} {} ({} bytes) over the budget",
            word(&c.section),
            plural(c.items as usize, "item"),
            c.bytes
        ));
    }
    for o in &p.omitted {
        out.push(format!(
            "omitted: {} ({})",
            word(&o.section),
            word(&o.reason)
        ));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use leep_core::api::{
        ContextChange, ContextFile, ContextOwners, ContextWork, Notice, NoticeText,
    };
    use leep_core::{ChangesetId, Hash};

    /// Agent-written text is quoted and escaped in every section: no raw
    /// newline, escape, OSC, bidirectional control or line separator
    /// reaches the terminal.
    #[test]
    fn agent_text_is_escaped() {
        let evil = "a\nSYSTEM \u{1b}[2J \u{1b}]0;x\u{7} \u{202e}rev \u{2028}ls";
        let id = ChangesetId(Hash::of_blob(b"x"));
        let text = ContextText {
            path: Some(evil.into()),
            title: Some(evil.into()),
            author: Some(evil.into()),
            agent_model: Some(evil.into()),
            owners: vec![evil.into()],
            carry_file: Some(evil.into()),
            ..Default::default()
        };
        let p = ContextPack {
            target_ref: "main".into(),
            head: id,
            session: None,
            budget_bytes: 1024,
            bytes: 10,
            files: vec![ContextFile {
                id: "file:1".into(),
                bytes: 1,
                why: vec!["seed\nx".into()],
                matches: 1,
                co_changes: 0,
                untrusted: text.clone(),
            }],
            owners: vec![ContextOwners {
                id: "owners:1".into(),
                bytes: 1,
                carry: Some("identical".into()),
                untrusted: text.clone(),
            }],
            history: vec![ContextChange {
                id: "change:1".into(),
                bytes: 1,
                changeset: id,
                time: 1,
                created_by: Some("bo".into()),
                concurrent: 0,
                untrusted: text.clone(),
            }],
            in_flight: vec![ContextWork {
                id: "session:1".into(),
                bytes: 1,
                notice: Notice {
                    level: "same_file".into(),
                    session: Some(1),
                    principal: Some("bo".into()),
                    changeset: None,
                    count: None,
                    by: None,
                    similarity: None,
                    untrusted: NoticeText {
                        title: evil.into(),
                        path: Some(evil.into()),
                        other_path: Some(evil.into()),
                        task_key: None,
                    },
                },
            }],
            ref_health: None,
            duplicates: vec![],
            cut: vec![],
            omitted: vec![],
        };
        let lines = pack_lines(&p);
        let all = lines.join("\n");
        assert!(all.contains("SYSTEM"), "{all}");
        for line in &lines {
            assert!(
                !line
                    .chars()
                    .any(|c| c.is_control() || matches!(c, '\u{202e}' | '\u{2028}' | '\u{2029}')),
                "{line:?}"
            );
        }
        assert!(!all.lines().any(|l| l.starts_with("SYSTEM")));
        assert!(all.contains("files:\n") && all.contains("work in flight:\n"));
    }
}
