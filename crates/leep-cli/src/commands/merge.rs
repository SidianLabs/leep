//! `leep merge`: merge another revision into the working copy's base.
//!
//! Produces a local, unlanded commit; no ref moves. A clean merge becomes the
//! new base. Conflicts leave a merge in progress: the base stays the first
//! parent, the files hold the clean changes plus conflict markers, and the
//! next `leep commit` adds the other side as the second parent (ADR 0019).

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use clap::Args;
use leep_core::api::{Authorship, Conflict, MergeRequest, MergeResponse};
use leep_core::{ChangesetId, FileChange, RepoPath};

use super::integrate::{abort, added_paths, load_key, print_conflicts, sign};
use super::{print_json, Ctx};
use crate::error::{CliError, Result};
use crate::fmt::short;
use crate::overlay::{self, Keep, Pending};
use crate::snapshot;
use crate::state::MergeState;

#[derive(Args, Debug, Default)]
pub struct MergeArgs {
    /// Revision to merge in: branch, tag, changeset id or `@`.
    #[arg(required_unless_present = "abort")]
    pub rev: Option<String>,
    /// Message for the merge commit (default: `Merge REV into <target ref>`).
    #[arg(short, long, conflicts_with = "abort")]
    pub message: Option<String>,
    /// Always create a merge commit, even when the base could fast-forward.
    #[arg(long, conflicts_with = "abort")]
    pub no_ff: bool,
    /// Give up a merge that stopped on conflicts and restore the working copy.
    #[arg(long)]
    pub abort: bool,
    /// Author of a clean merge commit (default: $LEEP_AUTHOR, else $USER).
    #[arg(long)]
    pub author: Option<String>,
    /// Sign a clean merge commit with this private key file. After a conflict,
    /// sign the commit with `leep commit --sign`.
    #[arg(long, env = "LEEP_SIGNING_KEY", value_name = "KEYFILE")]
    pub sign: Option<PathBuf>,
}

pub fn run(ctx: &mut Ctx, args: &MergeArgs) -> Result<i32> {
    if args.abort {
        let Some(m) = ctx.wc.state.merge_in_progress.clone() else {
            return Err(CliError::user("no merge is in progress"));
        };
        return abort(ctx, "merge", &m.added);
    }
    let key = load_key(&args.sign)?;
    ctx.require_clean()?;
    let base =
        ctx.wc.state.base.ok_or_else(|| {
            CliError::user("nothing to merge into: the working copy has no base yet")
        })?;
    let name = args.rev.as_deref().unwrap_or_default();
    let other = ctx.remote().resolve(name)?;
    let fork = ctx.client.merge_base(&base, &other)?.base;

    if fork == Some(other) {
        if ctx.json {
            print_json(
                &serde_json::json!({ "status": "up_to_date", "base": base, "other": other }),
            )?;
        } else {
            println!(
                "Already up to date: {} is in the history of {}",
                short(other),
                short(base)
            );
        }
        return Ok(0);
    }
    if fork == Some(base) && !args.no_ff {
        let stats = overlay::move_to(ctx, other)?;
        if ctx.json {
            print_json(&serde_json::json!({
                "status": "fast_forward", "base": other, "previous": base, "other": other,
                "written": stats.written, "removed": stats.removed,
            }))?;
        } else {
            println!(
                "Fast-forwarded {} to {} ({} written, {} removed)",
                short(base),
                short(other),
                stats.written,
                stats.removed
            );
        }
        return Ok(0);
    }

    let message = args
        .message
        .clone()
        .unwrap_or_else(|| default_message(name, other, &ctx.wc.config.target_ref));
    let resp = ctx.client.merge(&MergeRequest {
        parents: [base, other],
        message: message.clone(),
        authorship: Authorship {
            author: snapshot::author(args.author.as_deref()),
            timestamp: Some(snapshot::now_secs()),
        },
        extra: BTreeMap::new(),
    })?;
    match resp {
        MergeResponse::Merged { id } => {
            let signed = sign(ctx, &key, id)?;
            let stats = overlay::move_to(ctx, id)?;
            if ctx.json {
                print_json(&serde_json::json!({
                    "status": "merged", "base": id, "previous": base, "other": other,
                    "written": stats.written, "removed": stats.removed,
                    "signed": signed.is_some(),
                }))?;
            } else {
                println!(
                    "Merged {} into {}: {} ({} written, {} removed)",
                    short(other),
                    short(base),
                    short(id),
                    stats.written,
                    stats.removed
                );
                if let Some(s) = signed {
                    println!("signed as {} (key {})", s.name, s.key_id);
                }
            }
            Ok(0)
        }
        MergeResponse::Conflicts { conflicts, clean } => {
            check_clean_reported(ctx, fork, other, base, &conflicts, &clean)?;
            let base_files = crate::materialize::list_files(ctx, &base)?;
            let mut plan =
                overlay::plan_conflicts(ctx, base, base_files, conflicts, clean, Keep::Ours)?;
            plan.pending = Some(Pending::Merge(MergeState {
                other,
                message: message.clone(),
                added: added_paths(&plan),
            }));
            let stats = overlay::apply(ctx, &plan)?;
            if ctx.json {
                print_json(&serde_json::json!({
                    "status": "conflicts", "base": base, "other": other, "message": message,
                    "written": stats.written, "removed": stats.removed,
                    "conflicts": plan.conflicts, "unmerged": plan.unmerged,
                }))?;
            } else {
                println!(
                    "Cannot merge {} cleanly: {} conflicting",
                    short(other),
                    crate::fmt::plural(plan.conflicts.len() + plan.unmerged.len(), "file")
                );
                print_conflicts(&plan, other);
                println!(
                    "Resolve them (`leep resolve PATH` once chosen), then `leep commit` to finish the \
                     merge, or `leep merge --abort`."
                );
            }
            Ok(1)
        }
    }
}

fn default_message(rev: &str, other: ChangesetId, target_ref: &str) -> String {
    let name = if rev == "@" || rev.len() == 64 {
        short(other)
    } else {
        rev.to_string()
    };
    format!("Merge {name} into {target_ref}")
}

/// A server that predates the `clean` field answers conflicts with an empty
/// map. Using that would drop the other side's non-conflicting changes from
/// the merge commit without a word, so check that every path the other side
/// changed is accounted for: reported as clean or conflicting, or already
/// identical in the base. Nothing local has changed when this fails.
fn check_clean_reported(
    ctx: &Ctx,
    fork: Option<ChangesetId>,
    other: ChangesetId,
    base: ChangesetId,
    conflicts: &[Conflict],
    clean: &BTreeMap<RepoPath, FileChange>,
) -> Result<()> {
    let known: BTreeSet<&RepoPath> = conflicts
        .iter()
        .map(|c| &c.path)
        .chain(clean.keys())
        .collect();
    for e in ctx.client.diff(fork.as_ref(), &other, None)?.entries {
        if known.contains(&e.path) {
            continue;
        }
        let ours = ctx
            .client
            .files(&base, e.path.as_str())?
            .files
            .into_iter()
            .find(|f| f.path == e.path);
        let same = match (&ours, &e.new) {
            (None, None) => true,
            (Some(a), Some(b)) => a.content == b.content && a.kind == b.kind,
            _ => false,
        };
        if !same {
            return Err(CliError::user(
                "the server did not report the cleanly merged files along with the conflicts \
                 (it is older than this client); upgrade the server to merge with conflicts",
            ));
        }
    }
    Ok(())
}
