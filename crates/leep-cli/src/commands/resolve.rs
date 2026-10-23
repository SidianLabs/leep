//! `leep resolve`: say that a conflicted file is settled.
//!
//! Needed for conflicts without a text merge, where the local version was
//! kept and there are no markers to delete. Marker files are checked first, so
//! a stray `<<<<<<<` is not committed by accident.

use clap::Args;

use super::restore::{repo_spec, within};
use super::{print_json, Ctx};
use crate::error::{CliError, Result};
use crate::fmt::plural;
use crate::status::still_conflicted;
use leep_core::outln;

#[derive(Args, Debug)]
pub struct ResolveArgs {
    /// Conflicted files or directories (relative to the current directory).
    #[arg(required = true)]
    pub paths: Vec<String>,
    /// Mark files resolved even if conflict markers are still in them.
    #[arg(long)]
    pub force: bool,
}

pub fn run(ctx: &mut Ctx, args: &ResolveArgs) -> Result<i32> {
    let specs: Vec<String> = args
        .paths
        .iter()
        .map(|p| repo_spec(ctx, p))
        .collect::<Result<_>>()?;
    let state = &ctx.wc.state;
    let picked: Vec<String> = state
        .conflicts
        .iter()
        .chain(&state.unresolved)
        .filter(|p| specs.iter().any(|s| within(p, s)))
        .cloned()
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();
    if picked.is_empty() {
        return Err(CliError::user("no conflicted file matches"));
    }
    if !args.force {
        let marked: Vec<&String> = picked
            .iter()
            .filter(|p| still_conflicted(&ctx.wc, p))
            .collect();
        if !marked.is_empty() {
            return Err(CliError::conflict(format!(
                "conflict markers remain in {}; remove them, or use --force",
                marked
                    .iter()
                    .map(|p| p.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            )));
        }
    }
    ctx.wc.state.conflicts.retain(|p| !picked.contains(p));
    ctx.wc.state.unresolved.retain(|p| !picked.contains(p));
    ctx.wc.save_state()?;
    if ctx.json {
        return print_json(&serde_json::json!({ "resolved": picked })).map(|()| 0);
    }
    for p in &picked {
        outln!("resolved {p}");
    }
    outln!("Marked {} resolved", plural(picked.len(), "file"));
    Ok(0)
}
