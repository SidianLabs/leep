//! `leep rebase`: move the working copy's unlanded commits onto another
//! revision. Local only; no ref moves, and nothing changes if a commit does
//! not apply.

use clap::Args;

use super::{print_json, Ctx};
use crate::error::{CliError, Result};
use crate::fmt::{plural, short};
use crate::overlay::{self, move_stack, Moved};

#[derive(Args, Debug, Default)]
pub struct RebaseArgs {
    /// Revision to rebase onto: branch, tag, changeset id or `@`
    /// (default: the head of the target ref).
    pub onto: Option<String>,
}

pub fn run(ctx: &mut Ctx, args: &RebaseArgs) -> Result<i32> {
    ctx.require_clean()?;
    let base = ctx
        .wc
        .state
        .base
        .ok_or_else(|| CliError::user("nothing to rebase: the working copy has no base yet"))?;
    let onto = match args.onto.as_deref() {
        Some(r) => ctx.remote().resolve(r)?,
        None => {
            let name = &ctx.wc.config.target_ref;
            ctx.client
                .get_ref(name)?
                .target
                .ok_or_else(|| CliError::user(format!("ref {name} does not exist")))?
        }
    };

    let (status, tip, count) = match move_stack(ctx, base, onto)? {
        Moved::Contained if base == onto => ("up_to_date", base, 0),
        // Nothing local to move: the base is an ancestor, so this is a
        // fast-forward, like `sync`.
        Moved::Contained => ("fast_forward", onto, 0),
        // The stack already sits on `onto`.
        Moved::Moved { tip, .. } if tip == base => ("up_to_date", base, 0),
        Moved::Moved { tip, count } => ("rebased", tip, count),
        Moved::Conflict { commit, conflicts } => {
            return Err(conflict_error(ctx, commit, onto, &conflicts)?)
        }
    };
    let stats = if tip == base {
        Default::default()
    } else {
        overlay::move_to(ctx, tip)?
    };
    if ctx.json {
        return print_json(&serde_json::json!({
            "status": status, "base": tip, "previous": base, "onto": onto,
            "rebased_commits": count, "written": stats.written, "removed": stats.removed,
        }))
        .map(|()| 0);
    }
    match status {
        "up_to_date" => println!("Already up to date with {}", short(onto)),
        "fast_forward" => println!(
            "Fast-forwarded {} to {} ({} written, {} removed)",
            short(base),
            short(onto),
            stats.written,
            stats.removed
        ),
        _ => println!(
            "Rebased {} onto {}: base is now {} ({} written, {} removed)",
            plural(count, "local commit"),
            short(onto),
            short(tip),
            stats.written,
            stats.removed
        ),
    }
    Ok(0)
}

/// The commit that conflicts, the files, and how to settle it by hand.
fn conflict_error(
    ctx: &Ctx,
    commit: leep_core::ChangesetId,
    onto: leep_core::ChangesetId,
    conflicts: &[leep_core::api::Conflict],
) -> Result<CliError> {
    let msg = ctx.client.get_changeset(&commit)?.changeset.message;
    let paths: Vec<&str> = conflicts.iter().map(|c| c.path.as_str()).collect();
    Ok(CliError::user(format!(
        "local commit {} (\"{}\") conflicts with {} in {}; nothing changed.\n\
         To settle it by hand: `leep checkout {onto}`, then `leep cherry-pick {commit}` for each local \
         commit, oldest first, resolving the conflicts as they come up.",
        short(commit),
        msg.lines().next().unwrap_or(""),
        short(onto),
        paths.join(", ")
    )))
}
