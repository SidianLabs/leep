//! `leep branch`, `leep switch` and `leep tag`: everyday ref management.
//!
//! Branches are refs outside `tags/`; tags live under `tags/` and the server
//! never lets one move. Every write is a compare-and-swap, so a concurrent
//! change fails loudly instead of being overwritten.

use std::collections::BTreeMap;

use clap::Args;
use leep_core::api::CasRefRequest;
use leep_core::outln;
use leep_core::refname::{self, is_tag, tag_ref, TAGS_PREFIX};
use leep_core::ChangesetId;

use super::{print_json, Ctx};
use crate::error::{CliError, Result};
use crate::fmt::short;
use crate::materialize::materialize;

#[derive(Args, Debug)]
pub struct BranchArgs {
    /// Branch to create (or delete with `-d`/`-D`); omit to list branches.
    pub name: Option<String>,
    /// Revision to create the branch at (default: the working copy base).
    pub rev: Option<String>,
    /// Delete the branch if its head is already in the target ref's history.
    #[arg(short = 'd', long = "delete", conflicts_with = "force_delete")]
    pub delete: bool,
    /// Delete the branch even if it is not merged.
    #[arg(short = 'D')]
    pub force_delete: bool,
}

#[derive(Args, Debug)]
pub struct SwitchArgs {
    /// Branch to switch to.
    pub name: String,
    /// With `-c`: revision to create the branch at (default: the base).
    #[arg(requires = "create")]
    pub rev: Option<String>,
    /// Create the branch first.
    #[arg(short = 'c', long = "create")]
    pub create: bool,
}

#[derive(Args, Debug)]
pub struct TagArgs {
    /// Tag to create (or delete with `-d`); omit to list tags. Give the
    /// short name: `v1` is stored as `tags/v1`.
    pub name: Option<String>,
    /// Revision to tag (default: the working copy base).
    pub rev: Option<String>,
    /// Delete the tag.
    #[arg(short = 'd', long = "delete")]
    pub delete: bool,
}

pub fn branch(ctx: &Ctx, args: &BranchArgs) -> Result<i32> {
    let Some(name) = args.name.as_deref() else {
        if args.delete || args.force_delete || args.rev.is_some() {
            return Err(CliError::user("a branch name is required"));
        }
        return list_branches(ctx);
    };
    if args.delete || args.force_delete {
        if args.rev.is_some() {
            return Err(CliError::user("`branch -d` takes only a branch name"));
        }
        return delete_branch(ctx, name, args.force_delete);
    }
    let id = create_branch(ctx, name, args.rev.as_deref())?;
    if ctx.json {
        print_json(&serde_json::json!({ "name": name, "target": id }))?;
    } else {
        outln!("Created branch {name} at {}", short(id));
    }
    Ok(0)
}

pub fn switch(ctx: &mut Ctx, args: &SwitchArgs) -> Result<i32> {
    let name = args.name.as_str();
    ctx.require_clean()?;
    let head = if args.create {
        create_branch(ctx, name, args.rev.as_deref())?
    } else {
        if is_tag(name) {
            return Err(CliError::user(format!(
                "{name} is a tag, not a branch; check it out with `leep checkout {name}`"
            )));
        }
        ctx.client.get_ref(name)?.target.ok_or_else(|| {
            CliError::not_found(format!(
                "branch {name} does not exist; create it with `leep switch -c {name}`"
            ))
        })?
    };
    let left = ctx.wc.state.base;
    let stats = materialize(ctx, head, false)?;
    ctx.wc.config.target_ref = name.to_string();
    ctx.wc.save_config()?;
    if ctx.json {
        return print_json(&serde_json::json!({
            "target_ref": name, "base": head,
            "written": stats.written, "removed": stats.removed
        }))
        .map(|()| 0);
    }
    outln!(
        "Switched to branch {name} at {} ({} written, {} removed)",
        short(head),
        stats.written,
        stats.removed
    );
    if let Some(left) = left.filter(|l| *l != head) {
        // A changeset nothing points at yet is easy to lose track of.
        // A failed query only skips this note; the switch already happened.
        if contained_in(ctx, left, head).is_ok_and(|merged| !merged) {
            eprintln!(
                "note: {} is not in {name}'s history; return to it with \
                 `leep checkout {left}` or keep it with `leep branch <name> {left}`",
                short(left)
            );
        }
    }
    Ok(0)
}

pub fn tag(ctx: &Ctx, args: &TagArgs) -> Result<i32> {
    let Some(short_name) = args.name.as_deref() else {
        if args.delete || args.rev.is_some() {
            return Err(CliError::user("a tag name is required"));
        }
        return list_tags(ctx);
    };
    let full = tag_ref(short_name);
    if args.delete {
        if args.rev.is_some() {
            return Err(CliError::user("`tag -d` takes only a tag name"));
        }
        let head = ctx
            .client
            .get_ref(&full)?
            .target
            .ok_or_else(|| CliError::not_found(format!("tag {short_name} does not exist")))?;
        swap(ctx, &full, Some(head), None, || {
            format!("tag {short_name} changed while deleting it (409 conflict); run `leep tag` and retry")
        })?;
        if ctx.json {
            print_json(&serde_json::json!({ "name": short_name, "deleted": head }))?;
        } else {
            outln!("Deleted tag {short_name} (was {})", short(head));
        }
        return Ok(0);
    }
    let id = target_rev(ctx, args.rev.as_deref())?;
    refname::validate(&full)?;
    swap(ctx, &full, None, Some(id), || {
        format!(
            "tag {short_name} already exists; tags never move, delete it with \
             `leep tag -d {short_name}` first"
        )
    })?;
    if ctx.json {
        print_json(&serde_json::json!({ "name": short_name, "target": id }))?;
    } else {
        outln!("Created tag {short_name} at {}", short(id));
    }
    Ok(0)
}

/// Create branch `name` at `rev` (default: the base) and return its head.
fn create_branch(ctx: &Ctx, name: &str, rev: Option<&str>) -> Result<ChangesetId> {
    if is_tag(name) {
        return Err(CliError::user(format!(
            "{TAGS_PREFIX} is reserved for tags; create one with `leep tag`"
        )));
    }
    refname::validate(name)?;
    let id = target_rev(ctx, rev)?;
    swap(ctx, name, None, Some(id), || {
        format!("branch {name} already exists")
    })?;
    Ok(id)
}

fn delete_branch(ctx: &Ctx, name: &str, force: bool) -> Result<i32> {
    if is_tag(name) {
        return Err(CliError::user(format!(
            "{name} is a tag; delete it with `leep tag -d {}`",
            &name[TAGS_PREFIX.len()..]
        )));
    }
    let target = ctx.wc.config.target_ref.as_str();
    if name == target {
        return Err(CliError::user(format!(
            "cannot delete branch {name}: it is this working copy's target; \
             switch to another branch first"
        )));
    }
    let head = ctx
        .client
        .get_ref(name)?
        .target
        .ok_or_else(|| CliError::not_found(format!("branch {name} does not exist")))?;
    if !force {
        let merged = match ctx.client.get_ref(target)?.target {
            Some(t) => contained_in(ctx, head, t)?,
            None => false,
        };
        if !merged {
            return Err(CliError::user(format!(
                "branch {name} is not fully merged into {target}; \
                 use `leep branch -D {name}` to delete it anyway"
            )));
        }
    }
    swap(ctx, name, Some(head), None, || {
        format!("branch {name} moved while deleting it (409 conflict); run `leep branch` and retry")
    })?;
    if ctx.json {
        print_json(&serde_json::json!({ "name": name, "deleted": head }))?;
    } else {
        outln!("Deleted branch {name} (was {})", short(head));
    }
    Ok(0)
}

fn list_branches(ctx: &Ctx) -> Result<i32> {
    let mut resp = ctx.client.refs()?;
    resp.refs.retain(|n, _| !is_tag(n));
    if ctx.json {
        print_json(&resp)?;
        return Ok(0);
    }
    for line in branch_lines(&resp.refs, &ctx.wc.config.target_ref) {
        outln!("{line}");
    }
    Ok(0)
}

/// One line per branch, `*` marking the working copy's target. A target that
/// does not exist yet (a fresh repository) is still shown.
fn branch_lines(refs: &BTreeMap<String, ChangesetId>, current: &str) -> Vec<String> {
    let mut lines: Vec<String> = refs
        .iter()
        .map(|(name, id)| {
            let mark = if name == current { '*' } else { ' ' };
            format!("{mark} {name} {}", short(id))
        })
        .collect();
    if !refs.contains_key(current) {
        lines.push(format!("* {current} (not created yet)"));
    }
    lines
}

fn list_tags(ctx: &Ctx) -> Result<i32> {
    let mut resp = ctx.client.refs()?;
    resp.refs.retain(|n, _| is_tag(n));
    if ctx.json {
        print_json(&resp)?;
        return Ok(0);
    }
    for (name, id) in &resp.refs {
        outln!("{} {}", &name[TAGS_PREFIX.len()..], short(id));
    }
    Ok(0)
}

/// The revision to point a new ref at: `rev`, else the working copy base.
fn target_rev(ctx: &Ctx, rev: Option<&str>) -> Result<ChangesetId> {
    match rev {
        Some(r) => ctx.remote().resolve(r),
        None => ctx.wc.state.base.ok_or_else(|| {
            CliError::user("working copy has no base yet; commit first or give a REV")
        }),
    }
}

/// One compare-and-swap; `lost` explains a lost race: the ref already exists
/// (create) or moved (delete).
fn swap(
    ctx: &Ctx,
    name: &str,
    expected: Option<ChangesetId>,
    new: Option<ChangesetId>,
    lost: impl FnOnce() -> String,
) -> Result<()> {
    let resp = ctx.client.cas_ref(
        name,
        &CasRefRequest {
            expected,
            new,
            ..CasRefRequest::default()
        },
    )?;
    if resp.ok {
        Ok(())
    } else if expected.is_none() {
        // A create loses only to a ref that exists: a state, not a race.
        Err(CliError::user(lost()))
    } else {
        Err(CliError::lost_race(lost()))
    }
}

/// Whether `a` is in the history of `b` (inclusive), by the merge base.
fn contained_in(ctx: &Ctx, a: ChangesetId, b: ChangesetId) -> Result<bool> {
    Ok(ctx.client.merge_base(&a, &b)?.base == Some(a))
}

#[cfg(test)]
mod tests {
    use leep_core::Hash;

    use super::*;

    #[test]
    fn current_branch_is_starred() {
        let id = ChangesetId(Hash::of_blob(b"x"));
        let refs = BTreeMap::from([("dev".to_string(), id), ("main".to_string(), id)]);
        let s = short(id);
        assert_eq!(
            branch_lines(&refs, "main"),
            [format!("  dev {s}"), format!("* main {s}")]
        );
        assert_eq!(
            branch_lines(&BTreeMap::new(), "main"),
            ["* main (not created yet)"]
        );
    }
}
