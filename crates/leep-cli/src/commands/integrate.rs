//! What `merge`, `cherry-pick` and `revert` share: signing the result, saying
//! what is left to resolve, and aborting.
//!
//! All of them leave a conflicted result as uncommitted changes on top of the
//! unchanged base, with `State::merge_in_progress` or `pick_in_progress` set;
//! the next `leep commit` finishes the job (ADR 0019).

use std::path::PathBuf;

use leep_core::api::AddSignatureResponse;
use leep_core::outln;
use leep_core::ChangesetId;

use super::{print_json, Ctx};
use crate::error::{CliError, Result};
use crate::fmt::{plural, short};
use crate::materialize::{materialize, remove_file};
use crate::overlay::Plan;
use crate::sign::AuthorKey;
use crate::status::status;

/// Load `--sign` before any server call, so a bad key fails early.
pub fn load_key(path: &Option<PathBuf>) -> Result<Option<AuthorKey>> {
    path.as_deref()
        .filter(|p| !p.as_os_str().is_empty())
        .map(AuthorKey::load)
        .transpose()
}

/// Sign `id` if a key was given. Runs before the disk is touched: on failure
/// nothing local changed and a retry only makes another changeset.
pub fn sign(
    ctx: &Ctx,
    key: &Option<AuthorKey>,
    id: ChangesetId,
) -> Result<Option<AddSignatureResponse>> {
    key.as_ref()
        .map(|k| {
            ctx.client.add_signature(&id, &k.sign(&id)).map_err(|e| {
                CliError::user(format!(
                    "created {id} but could not sign it: {e}; nothing changed locally, retry"
                ))
            })
        })
        .transpose()
}

/// Paths the plan creates that its base lacks, for `--abort`.
pub fn added_paths(plan: &Plan) -> Vec<String> {
    plan.want
        .keys()
        .filter(|p| !plan.base_files.contains_key(*p))
        .cloned()
        .collect()
}

/// List the conflicts a plan wrote. `other` is the changeset whose side was
/// not kept, so `leep cat` can show it.
pub fn print_conflicts(plan: &Plan, other: ChangesetId) {
    for p in &plan.conflicts {
        outln!("  C {p} (markers written)");
    }
    for p in &plan.unmerged {
        outln!(
            "  C {p} (no text merge: kept your version; `leep cat {other} {p}` shows the other side, \
             `leep restore --source {other} {p}` takes it)"
        );
    }
}

/// Throw away an unfinished merge, cherry-pick or revert: the files go back to
/// the base and the files the operation created are removed.
pub fn abort(ctx: &mut Ctx, what: &str, added: &[String]) -> Result<i32> {
    let base = ctx
        .wc
        .state
        .base
        .ok_or_else(|| CliError::user("the working copy has no base"))?;
    let discarded = status(&ctx.wc)?.entries.len();
    for p in added {
        remove_file(&ctx.wc.root, &ctx.wc.abs_for_write(p)?)?;
    }
    // Forced: edits made while resolving are what an abort throws away.
    materialize(ctx, base, true)?;
    if ctx.json {
        print_json(&serde_json::json!({
            "status": "aborted", "operation": what, "base": base, "discarded": discarded,
        }))?;
    } else {
        outln!(
            "Aborted the {what}; the working copy is back at {} ({} discarded)",
            short(base),
            plural(discarded, "change")
        );
    }
    Ok(0)
}
