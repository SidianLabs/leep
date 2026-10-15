//! `leep cherry-pick` and `leep revert`: replay one changeset's change on the
//! working copy's base, as a local, unlanded commit.
//!
//! Both use `POST /v1/rebase`, which merges three ways with the changeset's
//! first parent as the merge base. A cherry-pick rebases the changeset itself.
//! A revert first stores a temporary changeset that holds the inverse of the
//! change (parent = the reverted changeset, so the three-way merge sees the
//! original change as the base) and rebases that (ADR 0019).

use std::collections::BTreeMap;
use std::path::PathBuf;

use clap::Args;
use leep_core::api::{RebaseRequest, RebaseResponse};
use leep_core::changeset::EXTRA_REVERT_OF;
use leep_core::{Changeset, ChangesetId, FileChange};

use super::integrate::{abort, added_paths, load_key, print_conflicts, sign};
use super::{print_json, Ctx};
use crate::error::{CliError, Result};
use crate::fmt::short;
use crate::materialize::list_files;
use crate::overlay::{self, contained_in, Keep, Pending};
use crate::sign::AuthorKey;
use crate::snapshot;
use crate::state::PickState;

#[derive(Args, Debug, Default)]
pub struct CherryPickArgs {
    /// Changeset to copy onto the base: branch, tag, changeset id or `@`.
    #[arg(required_unless_present = "abort")]
    pub rev: Option<String>,
    /// Give up a cherry-pick that stopped on conflicts and restore the
    /// working copy.
    #[arg(long)]
    pub abort: bool,
    /// Sign a clean result with this private key file. After a conflict, sign
    /// the commit with `leep commit --sign`.
    #[arg(long, env = "LEEP_SIGNING_KEY", value_name = "KEYFILE")]
    pub sign: Option<PathBuf>,
}

#[derive(Args, Debug, Default)]
pub struct RevertArgs {
    /// Changeset to undo: branch, tag, changeset id or `@`.
    #[arg(required_unless_present = "abort")]
    pub rev: Option<String>,
    /// Give up a revert that stopped on conflicts and restore the working copy.
    #[arg(long)]
    pub abort: bool,
    /// Author of the revert (default: $LEEP_AUTHOR, else $USER).
    #[arg(long)]
    pub author: Option<String>,
    /// Sign a clean result with this private key file. After a conflict, sign
    /// the commit with `leep commit --sign`.
    #[arg(long, env = "LEEP_SIGNING_KEY", value_name = "KEYFILE")]
    pub sign: Option<PathBuf>,
}

pub fn cherry_pick(ctx: &mut Ctx, args: &CherryPickArgs) -> Result<i32> {
    if args.abort {
        return abort_pick(ctx, false);
    }
    let key = load_key(&args.sign)?;
    ctx.require_clean()?;
    let base = base_of(ctx, "cherry-pick onto")?;
    let rev = ctx
        .remote()
        .resolve(args.rev.as_deref().unwrap_or_default())?;
    let picked = ctx.client.get_changeset(&rev)?.changeset;
    if picked.parents.len() > 1 {
        return Err(CliError::user(format!(
            "{} is a merge; cherry-picking a merge is not supported",
            short(rev)
        )));
    }
    if contained_in(ctx, rev, base)? {
        return Err(CliError::user(format!(
            "{} is already in the history of the base {}",
            short(rev),
            short(base)
        )));
    }
    let resp = ctx.client.rebase(&RebaseRequest {
        changeset: rev,
        onto: base,
    })?;
    let first_line = picked.message.lines().next().unwrap_or("").to_string();
    finish(
        ctx,
        &Pick {
            from: rev,
            message: picked.message,
            summary: first_line,
            revert: false,
            temp: None,
        },
        base,
        resp,
        &key,
    )
}

pub fn revert(ctx: &mut Ctx, args: &RevertArgs) -> Result<i32> {
    if args.abort {
        return abort_pick(ctx, true);
    }
    let key = load_key(&args.sign)?;
    ctx.require_clean()?;
    let base = base_of(ctx, "revert in")?;
    let rev = ctx
        .remote()
        .resolve(args.rev.as_deref().unwrap_or_default())?;
    let reverted = ctx.client.get_changeset(&rev)?.changeset;
    if reverted.parents.len() > 1 {
        return Err(CliError::user(format!(
            "{} is a merge; reverting a merge needs a mainline, which is not supported yet",
            short(rev)
        )));
    }
    if !contained_in(ctx, rev, base)? {
        return Err(CliError::user(format!(
            "{} is not in the history of the base {}; there is nothing to revert",
            short(rev),
            short(base)
        )));
    }

    // The inverse of what the changeset did, path by path. The tree diff skips
    // paths the changeset recorded without changing them.
    let diff = ctx
        .client
        .diff(reverted.parents.first(), &rev, None)?
        .entries;
    let changes: BTreeMap<_, _> = diff
        .into_iter()
        .map(|e| {
            let inverse = match e.old {
                Some(old) => FileChange::Modify {
                    content: old.content,
                    kind: old.kind,
                    size: old.size,
                },
                None => FileChange::Delete,
            };
            (e.path, inverse)
        })
        .collect();
    if changes.is_empty() {
        return Err(CliError::user(format!(
            "{} changes no files; there is nothing to revert",
            short(rev)
        )));
    }
    let summary = reverted.message.lines().next().unwrap_or("").to_string();
    let message = format!("Revert \"{summary}\"\n\nThis reverts {rev}.");
    let inverse = ctx.client.create_changeset(&Changeset {
        parents: vec![rev],
        author: snapshot::author(args.author.as_deref()),
        timestamp: snapshot::now_secs(),
        message: message.clone(),
        changes,
        extra: BTreeMap::from([(EXTRA_REVERT_OF.to_string(), rev.to_string())]),
    })?;
    let resp = ctx.client.rebase(&RebaseRequest {
        changeset: inverse,
        onto: base,
    })?;
    finish(
        ctx,
        &Pick {
            from: rev,
            message,
            summary,
            revert: true,
            temp: Some(inverse),
        },
        base,
        resp,
        &key,
    )
}

/// What is being replayed.
struct Pick {
    from: ChangesetId,
    message: String,
    /// First line of the original message, for output.
    summary: String,
    revert: bool,
    /// The changeset that was rebased when it is not `from`: a revert's
    /// inverse.
    temp: Option<ChangesetId>,
}

impl Pick {
    fn verb(&self) -> &'static str {
        if self.revert {
            "revert"
        } else {
            "cherry-pick"
        }
    }
}

fn base_of(ctx: &Ctx, what: &str) -> Result<ChangesetId> {
    ctx.wc
        .state
        .base
        .ok_or_else(|| CliError::user(format!("cannot {what}: the working copy has no base yet")))
}

fn finish(
    ctx: &mut Ctx,
    pick: &Pick,
    base: ChangesetId,
    resp: RebaseResponse,
    key: &Option<AuthorKey>,
) -> Result<i32> {
    let verb = pick.verb();
    match resp {
        RebaseResponse::Rebased { id } => {
            let signed = sign(ctx, key, id)?;
            let stats = overlay::move_to(ctx, id)?;
            if ctx.json {
                print_json(&serde_json::json!({
                    "status": "picked", "operation": verb, "base": id, "previous": base,
                    "from": pick.from, "changeset": pick.temp,
                    "written": stats.written, "removed": stats.removed,
                    "signed": signed.is_some(),
                }))?;
            } else {
                println!(
                    "{} {} as {} ({} written, {} removed)",
                    if pick.revert {
                        "Reverted"
                    } else {
                        "Cherry-picked"
                    },
                    short(pick.from),
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
        RebaseResponse::Conflicts { conflicts, clean } => {
            let base_files = list_files(ctx, &base)?;
            let mut plan =
                overlay::plan_conflicts(ctx, base, base_files, conflicts, clean, Keep::Ours)?;
            plan.pending = Some(Pending::Pick(PickState {
                from: pick.from,
                message: pick.message.clone(),
                revert: pick.revert,
                added: added_paths(&plan),
            }));
            let stats = overlay::apply(ctx, &plan)?;
            // For a revert the other side is the inverse change.
            let other = pick.temp.unwrap_or(pick.from);
            if ctx.json {
                print_json(&serde_json::json!({
                    "status": "conflicts", "operation": verb, "base": base,
                    "from": pick.from, "changeset": pick.temp, "message": pick.message,
                    "written": stats.written, "removed": stats.removed,
                    "conflicts": plan.conflicts, "unmerged": plan.unmerged,
                }))?;
            } else {
                println!(
                    "Cannot {verb} {} (\"{}\") cleanly: {} conflicting",
                    short(pick.from),
                    pick.summary,
                    crate::fmt::plural(plan.conflicts.len() + plan.unmerged.len(), "file")
                );
                print_conflicts(&plan, other);
                println!(
                    "Resolve them (`leep resolve PATH` once chosen), then `leep commit` to record \
                     the {verb}, or `leep {verb} --abort`."
                );
            }
            Ok(1)
        }
    }
}

fn abort_pick(ctx: &mut Ctx, revert: bool) -> Result<i32> {
    let what = if revert { "revert" } else { "cherry-pick" };
    match ctx.wc.state.pick_in_progress.clone() {
        Some(p) if p.revert == revert => abort(ctx, what, &p.added),
        Some(p) => Err(CliError::user(format!(
            "the operation in progress is a {}; use `leep {} --abort`",
            if p.revert { "revert" } else { "cherry-pick" },
            if p.revert { "revert" } else { "cherry-pick" },
        ))),
        None => Err(CliError::user(format!("no {what} is in progress"))),
    }
}
