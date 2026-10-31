//! `leep status`.

use std::collections::{HashMap, HashSet};

use leep_core::api::RenameEntry;
use leep_core::outln;
use leep_core::rename::RenameOptions;
use leep_core::ChangesetId;
use serde::Serialize;

use super::{print_json, Ctx};
use crate::error::Result;
use crate::fmt::{plural, rename_line, short};
use crate::status::{status, working_renames, Change, Status};

#[derive(Serialize)]
struct Report {
    base: Option<String>,
    target_ref: String,
    changes: Vec<Entry>,
    stashes: usize,
    /// A merge waiting for its commit: the changeset being merged in.
    #[serde(skip_serializing_if = "Option::is_none")]
    merging: Option<String>,
    /// A cherry-pick or revert waiting for its commit: the changeset it is
    /// about, and which of the two it is.
    #[serde(skip_serializing_if = "Option::is_none")]
    picking: Option<Picking>,
    /// Whether the base is in the target ref's history; `None` when the
    /// server could not be asked or the ref does not exist.
    on_target: Option<bool>,
    /// Commits between the base and where it leaves the target's history.
    ahead: Option<u64>,
    /// Commits the target gained since the base left its history; only when
    /// the two have diverged.
    #[serde(skip_serializing_if = "Option::is_none")]
    behind: Option<u64>,
    /// Deleted and added files that look like one moved file. `changes` still
    /// lists both paths, so readers that ignore this see what they always did.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    renames: Vec<RenameEntry>,
}

#[derive(Serialize)]
struct Picking {
    from: String,
    operation: &'static str,
}

#[derive(Serialize)]
struct Entry {
    path: String,
    status: Change,
}

/// Where the base stands against the target ref. Counts are generation
/// differences, exact for a linear history.
enum Relation {
    OnTarget,
    /// The target's head is in the base's history.
    Ahead(u64),
    /// Each side has commits the other lacks.
    Diverged {
        ahead: u64,
        behind: u64,
    },
    /// No common ancestor at all.
    Unrelated,
}

/// Best effort: `status` must keep working offline, so any failure is `None`.
fn relation(ctx: &Ctx, base: ChangesetId) -> Option<Relation> {
    let head = ctx.client.get_ref(&ctx.wc.config.target_ref).ok()?.target?;
    let fork = ctx.client.merge_base(&base, &head).ok()?.base;
    let Some(f) = fork else {
        return Some(Relation::Unrelated);
    };
    if f == base {
        return Some(Relation::OnTarget);
    }
    let gen = |id| ctx.client.get_changeset(&id).ok().map(|c| c.generation);
    let low = gen(f)?;
    let ahead = gen(base)?.saturating_sub(low);
    if f == head {
        return Some(Relation::Ahead(ahead));
    }
    let behind = gen(head)?.saturating_sub(low);
    Some(Relation::Diverged { ahead, behind })
}

/// Renames among the working copy's deleted and added files.
pub fn renames(ctx: &Ctx, st: &Status) -> Vec<RenameEntry> {
    working_renames(&ctx.wc, st, RenameOptions::default(), |h| {
        ctx.cache.fetch(&ctx.client, h).ok()
    })
    .renames
}

pub fn run(ctx: &Ctx) -> Result<i32> {
    let st = status(&ctx.wc)?;
    let rel = ctx.wc.state.base.and_then(|b| relation(ctx, b));
    let stashes = ctx.wc.state.stashes.len();
    let renames = renames(ctx, &st);
    if ctx.json {
        print_json(&Report {
            base: ctx.wc.state.base.map(|b| b.to_string()),
            target_ref: ctx.wc.config.target_ref.clone(),
            changes: st
                .entries
                .iter()
                .map(|e| Entry {
                    path: e.path.clone(),
                    status: e.change,
                })
                .collect(),
            stashes,
            merging: ctx
                .wc
                .state
                .merge_in_progress
                .as_ref()
                .map(|m| m.other.to_string()),
            picking: ctx.wc.state.pick_in_progress.as_ref().map(|p| Picking {
                from: p.from.to_string(),
                operation: if p.revert { "revert" } else { "cherry-pick" },
            }),
            on_target: rel.as_ref().map(|r| matches!(r, Relation::OnTarget)),
            ahead: match rel {
                Some(Relation::Ahead(n) | Relation::Diverged { ahead: n, .. }) => Some(n),
                _ => None,
            },
            behind: match rel {
                Some(Relation::Diverged { behind, .. }) => Some(behind),
                _ => None,
            },
            renames,
        })?;
        return Ok(0);
    }
    let target = &ctx.wc.config.target_ref;
    match ctx.wc.state.base {
        Some(b) => outln!("base {} (target {target})", short(b)),
        None => outln!("no base yet (target {target})"),
    }
    match rel {
        Some(Relation::Ahead(n)) => {
            outln!("base is {} ahead of {target}", plural(n as usize, "commit"))
        }
        Some(Relation::Diverged { ahead, behind }) => outln!(
            "base is {} ahead, {} behind {target}",
            plural(ahead as usize, "commit"),
            plural(behind as usize, "commit")
        ),
        Some(Relation::Unrelated) => outln!("base shares no history with {target}"),
        _ => {}
    }
    if let Some(m) = &ctx.wc.state.merge_in_progress {
        outln!(
            "merging {} (fix the conflicts, then `leep commit`; `leep merge --abort` cancels)",
            short(m.other)
        );
    }
    if let Some(p) = &ctx.wc.state.pick_in_progress {
        let (what, ing) = if p.revert {
            ("revert", "reverting")
        } else {
            ("cherry-pick", "cherry-picking")
        };
        outln!(
            "{ing} {} (fix the conflicts, then `leep commit`; `leep {what} --abort` cancels)",
            short(p.from)
        );
    }
    if stashes > 0 {
        outln!(
            "{stashes} {}",
            if stashes == 1 { "stash" } else { "stashes" }
        );
    }
    if st.is_clean() {
        outln!("nothing to commit, working copy clean");
    }
    for line in change_lines(&st, &renames) {
        let note = match line.strip_prefix("C ") {
            Some(p) if ctx.wc.state.unresolved.iter().any(|u| u == p) => {
                " (no text merge: kept your version)"
            }
            _ => "",
        };
        outln!("{}{note}", paint_letter(&line));
    }
    if st.has_conflicts() {
        outln!(
            "unresolved conflicts: fix the markers, or `leep resolve PATH` once you have \
             chosen; `leep cat @ PATH` shows the other side"
        );
    }
    Ok(0)
}

/// One line per change; a detected rename takes the place of its delete and
/// add.
pub fn change_lines(st: &Status, renames: &[RenameEntry]) -> Vec<String> {
    let mut out = Vec::new();
    let moved_from: HashSet<&str> = renames.iter().map(|r| r.from.as_str()).collect();
    let moved_to: HashMap<&str, _> = renames.iter().map(|r| (r.to.as_str(), r)).collect();
    for e in &st.entries {
        match (e.change, moved_to.get(e.path.as_str())) {
            (Change::Deleted, _) if moved_from.contains(e.path.as_str()) => {}
            (Change::Added, Some(r)) => {
                out.push(rename_line(r.from.as_str(), &e.path, r.similarity));
            }
            _ => out.push(format!("{} {}", e.change.letter(), e.path)),
        }
    }
    out
}

/// `X path` with the status letter coloured; other lines (renames) as is.
fn paint_letter(line: &str) -> String {
    let mut chars = line.chars();
    match (chars.next(), chars.next()) {
        (Some(c @ ('A' | 'M' | 'D' | 'C')), Some(' ')) => {
            format!("{}{}", crate::term::letter(c), &line[1..])
        }
        _ => line.to_string(),
    }
}
