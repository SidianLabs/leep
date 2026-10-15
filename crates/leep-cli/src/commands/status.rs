//! `leep status`.

use std::collections::{HashMap, HashSet};

use leep_core::api::RenameEntry;
use leep_core::rename::RenameOptions;
use serde::Serialize;

use super::{print_json, Ctx};
use crate::error::Result;
use crate::fmt::{rename_line, short};
use crate::status::{status, working_renames, Change, Status};

#[derive(Serialize)]
struct Report {
    base: Option<String>,
    target_ref: String,
    changes: Vec<Entry>,
    /// Deleted and added files that look like one moved file. `changes` still
    /// lists both paths, so readers that ignore this see what they always did.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    renames: Vec<RenameEntry>,
}

#[derive(Serialize)]
struct Entry {
    path: String,
    status: Change,
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
            renames,
        })?;
        return Ok(0);
    }
    match ctx.wc.state.base {
        Some(b) => println!("base {} (target {})", short(b), ctx.wc.config.target_ref),
        None => println!("no base yet (target {})", ctx.wc.config.target_ref),
    }
    if st.is_clean() {
        println!("nothing to commit, working copy clean");
    }
    for line in change_lines(&st, &renames) {
        println!("{line}");
    }
    if st.has_conflicts() {
        println!("unresolved conflicts: fix the marked files, then `leep commit`");
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
