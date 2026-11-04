//! `leep checks`: post CI check results, see where a ref's known-good
//! `green/<ref>` stands, and list the suspects for a failure (ADR 0091).

use clap::{Args, Subcommand, ValueEnum};
use leep_core::api::{CulpritsResponse, Outcome, PostCheckRequest, RefChecksResponse};
use leep_core::outln;

use super::{print_json, Remote};
use crate::error::Result;
use crate::fmt::{plural, short, utc_time};

#[derive(Subcommand, Debug)]
pub enum ChecksCmd {
    /// Record a check result for a changeset (CI tokens only: the token's
    /// `checks` must name the check, and it may not have created the
    /// changeset). A pass that completes a ref's required checks moves its
    /// `green/<ref>`.
    Post(PostArgs),
    /// Show a changeset's check results and the refs whose green holds it.
    Status(StatusArgs),
    /// Show a ref's head, its known-good `green/<ref>`, how far behind that
    /// is, and whether the head is red (exit 1 when red).
    Ref(RefArgs),
    /// List the suspects for a failure: the lands in the range above the
    /// known-good ref (or --good) up to --bad (default: the head). A suspect
    /// is a land in the range, not a verdict.
    Culprits(CulpritsArgs),
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum OutcomeArg {
    Pass,
    Fail,
}

#[derive(Args, Debug)]
pub struct PostArgs {
    /// Changeset id, ref name, or `@` for the working copy base.
    pub rev: String,
    /// The check's name, such as `test`.
    pub check: String,
    #[arg(value_enum)]
    pub outcome: OutcomeArg,
    /// Where the run's log is.
    #[arg(long)]
    pub url: Option<String>,
    /// One short line about the run.
    #[arg(short = 'm', long)]
    pub summary: Option<String>,
}

#[derive(Args, Debug)]
pub struct StatusArgs {
    /// Default: `@`, the working copy base.
    #[arg(default_value = "@")]
    pub rev: String,
}

#[derive(Args, Debug)]
pub struct RefArgs {
    /// Default: the target ref.
    #[arg(value_name = "REF")]
    pub name: Option<String>,
}

#[derive(Args, Debug)]
pub struct CulpritsArgs {
    /// Default: the target ref.
    #[arg(value_name = "REF")]
    pub name: Option<String>,
    /// The failing changeset (default: the ref's head).
    #[arg(long, value_name = "REV")]
    pub bad: Option<String>,
    /// The last changeset known to pass (default: `green/<ref>`); name one
    /// to look below green.
    #[arg(long, value_name = "REV")]
    pub good: Option<String>,
}

pub fn run(r: &Remote, cmd: &ChecksCmd) -> Result<i32> {
    match cmd {
        ChecksCmd::Post(a) => post(r, a),
        ChecksCmd::Status(a) => status(r, a),
        ChecksCmd::Ref(a) => for_ref(r, a),
        ChecksCmd::Culprits(a) => culprits(r, a),
    }
}

fn post(r: &Remote, a: &PostArgs) -> Result<i32> {
    let id = r.resolve(&a.rev)?;
    let outcome = match a.outcome {
        OutcomeArg::Pass => Outcome::Pass,
        OutcomeArg::Fail => Outcome::Fail,
    };
    let resp = r.client.post_check(
        &id,
        &PostCheckRequest {
            check: a.check.clone(),
            outcome,
            url: a.url.clone().unwrap_or_default(),
            summary: a.summary.clone().unwrap_or_default(),
        },
    )?;
    if r.json {
        return print_json(&resp).map(|_| 0);
    }
    outln!(
        "Recorded {} {} on {}",
        resp.check,
        resp.outcome.as_str(),
        short(id)
    );
    for p in &resp.promoted {
        outln!(
            "{} moved to {} (every required check passed)",
            p.green_ref,
            short(p.to)
        );
    }
    Ok(0)
}

fn status(r: &Remote, a: &StatusArgs) -> Result<i32> {
    let id = r.resolve(&a.rev)?;
    let resp = r.client.checks(&id)?;
    if r.json {
        return print_json(&resp).map(|_| 0);
    }
    if resp.checks.is_empty() {
        outln!("{}: no check results", short(id));
    }
    for c in &resp.checks {
        let mut line = format!(
            "{:<5} {:<20} by {}  {}",
            c.outcome.as_str(),
            c.check,
            c.by,
            utc_time(c.timestamp)
        );
        if !c.url.is_empty() {
            line.push_str(&format!("  {}", c.url));
        }
        outln!("{line}");
        if !c.summary.is_empty() {
            outln!("      {}", c.summary);
        }
    }
    for name in &resp.green_on {
        outln!("known good on {name} (green/{name} holds it)");
    }
    Ok(0)
}

fn for_ref(r: &Remote, a: &RefArgs) -> Result<i32> {
    let name = a.name.as_deref().unwrap_or(r.target_ref);
    let s = r.client.ref_checks(name)?;
    let red = s.red.is_some();
    if r.json {
        print_json(&s)?;
    } else {
        for line in ref_lines(&s) {
            outln!("{line}");
        }
    }
    Ok(if red { 1 } else { 0 })
}

fn ref_lines(s: &RefChecksResponse) -> Vec<String> {
    let mut out = vec![format!("{} at {}", s.name, short(s.head))];
    match (s.green, s.behind) {
        (None, _) if s.required.is_empty() => out.push(format!(
            "{} has no known-good ref (no required checks)",
            s.name
        )),
        (None, _) => out.push(format!("{}: not promoted yet", s.green_ref)),
        (Some(g), Some(0)) => out.push(format!("{} at {} (the head)", s.green_ref, short(g))),
        (Some(g), Some(n)) => out.push(format!(
            "{} at {}, {} behind",
            s.green_ref,
            short(g),
            plural(n as usize, "changeset")
        )),
        (Some(g), None) => out.push(format!(
            "{} at {}, off the head's history",
            s.green_ref,
            short(g)
        )),
    }
    if !s.required.is_empty() {
        out.push(format!("required: {}", s.required.join(", ")));
    }
    if let Some(red) = &s.red {
        out.push(format!(
            "{} is red since {} ({}); leep checks culprits {}",
            s.name,
            short(red.first_failed),
            red.check,
            s.name
        ));
    }
    out
}

fn culprits(r: &Remote, a: &CulpritsArgs) -> Result<i32> {
    let name = a.name.as_deref().unwrap_or(r.target_ref);
    let bad = a.bad.as_deref().map(|b| r.resolve(b)).transpose()?;
    let good = a.good.as_deref().map(|g| r.resolve(g)).transpose()?;
    let resp = r.client.culprits(name, bad.as_ref(), good.as_ref())?;
    if r.json {
        return print_json(&resp).map(|_| 0);
    }
    for line in culprit_lines(&resp) {
        outln!("{line}");
    }
    Ok(0)
}

fn culprit_lines(c: &CulpritsResponse) -> Vec<String> {
    let from = match c.good {
        Some(g) => format!("above {}", short(g)),
        None => "from the start of the log".to_string(),
    };
    let mut out = vec![format!(
        "Suspects for {} on {}: {} in the range {from}, newest first",
        short(c.bad),
        c.name,
        plural(c.suspects.len(), "land")
    )];
    for s in &c.suspects {
        let who = s
            .created_by
            .as_deref()
            .or(s.actor.as_deref())
            .unwrap_or("-");
        let mut line = format!(
            "  {:>6}  {}  {:<12} {}",
            s.seq,
            short(s.changeset),
            who,
            s.title
        );
        if let Some(m) = &s.agent_model {
            line.push_str(&format!("  [{m}]"));
        }
        out.push(line);
        let more = if s.more_paths { ", ..." } else { "" };
        let shown: Vec<&str> = s.paths.iter().take(5).map(|p| p.as_str()).collect();
        if !shown.is_empty() {
            let rest = s.paths.len().saturating_sub(shown.len());
            let tail = if rest > 0 {
                format!(" and {rest} more")
            } else {
                String::new()
            };
            out.push(format!("          {}{tail}{more}", shown.join(", ")));
        }
    }
    if c.truncated {
        out.push(
            "(stopped at the walk's bound before reaching the good changeset; pass --bad lower)"
                .to_string(),
        );
    }
    out.push(
        "A suspect is a land in the range, not a verdict: a land that fails only together \
         with a later one shows the later one too. Rerun before blaming."
            .to_string(),
    );
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use leep_core::api::{RedState, Suspect};
    use leep_core::{ChangesetId, Hash, RepoPath};

    fn id(n: u8) -> ChangesetId {
        ChangesetId(Hash([n; 32]))
    }

    #[test]
    fn ref_lines_say_green_behind_and_red() {
        let mut s = RefChecksResponse {
            name: "main".into(),
            head: id(2),
            green_ref: "green/main".into(),
            green: Some(id(1)),
            required: vec!["test".into()],
            behind: Some(3),
            red: None,
        };
        let text = ref_lines(&s).join("\n");
        assert!(text.contains("3 changesets behind"), "{text}");
        assert!(text.contains("required: test"), "{text}");
        s.red = Some(RedState {
            first_failed: id(2),
            check: "test".into(),
            since_seq: None,
        });
        let text = ref_lines(&s).join("\n");
        assert!(text.contains("red since"), "{text}");
        assert!(text.contains("leep checks culprits main"), "{text}");
    }

    #[test]
    fn suspects_are_never_called_the_culprit() {
        let c = CulpritsResponse {
            name: "main".into(),
            good: Some(id(1)),
            bad: id(3),
            suspects: vec![Suspect {
                changeset: id(3),
                seq: 7,
                operation: "land".into(),
                actor: Some("agent-7".into()),
                time: 0,
                land_id: None,
                land_changeset: None,
                created_by: None,
                author: "a".into(),
                title: "parser: speed up".into(),
                agent_model: None,
                paths: vec![RepoPath::new("src/p.rs").unwrap()],
                more_paths: false,
            }],
            truncated: true,
        };
        let text = culprit_lines(&c).join("\n");
        assert!(text.contains("Suspects for"), "{text}");
        assert!(
            text.contains("agent-7") && text.contains("src/p.rs"),
            "{text}"
        );
        assert!(text.contains("not a verdict"), "{text}");
        assert!(text.contains("bound"), "{text}");
        assert!(!text.to_lowercase().contains("the culprit"), "{text}");
    }
}
