//! `leep checkout` and `leep sync`.

use clap::Args;

use super::{print_json, Ctx};
use crate::error::{CliError, Result};
use crate::fmt::{plural, short};
use crate::materialize::materialize;
use crate::overlay::{self, rebase_stack};
use crate::snapshot;
use crate::state::normalize_prefix;
use crate::status::status;

#[derive(Args, Debug)]
pub struct CheckoutArgs {
    /// Ref name, 64-hex changeset id, or `@`.
    pub rev: String,
    /// Replace the sparse prefixes (repeatable).
    #[arg(long = "prefix")]
    pub prefixes: Vec<String>,
    /// Discard local changes and overwrite untracked files.
    #[arg(long)]
    pub force: bool,
}

pub fn checkout(ctx: &mut Ctx, args: &CheckoutArgs) -> Result<i32> {
    if !args.force {
        ctx.require_clean()?;
    }
    let id = ctx.remote().resolve(&args.rev)?;
    if !args.prefixes.is_empty() {
        ctx.wc.config.prefixes = args
            .prefixes
            .iter()
            .map(|p| normalize_prefix(p))
            .collect::<Result<_>>()?;
        ctx.wc.save_config()?;
    }
    let stats = materialize(ctx, id, args.force)?;
    report(ctx, "Checked out", id, &stats)?;
    Ok(0)
}

#[derive(Args, Debug, Default)]
pub struct SyncArgs {
    /// Refuse a working copy with uncommitted changes instead of carrying
    /// them over, and do not move local commits.
    #[arg(long)]
    pub no_rebase: bool,
}

/// Move to the current head of the target ref, carrying uncommitted changes
/// and unlanded local commits along (see ADR 0017).
pub fn sync(ctx: &mut Ctx, args: &SyncArgs) -> Result<i32> {
    let name = ctx.wc.config.target_ref.clone();
    if args.no_rebase {
        ctx.require_clean()?;
    }
    let head = ctx
        .client
        .get_ref(&name)?
        .target
        .ok_or_else(|| CliError::user(format!("ref {name} does not exist")))?;
    let st = status(&ctx.wc)?;
    st.refuse_conflicts()?;
    let base = match ctx.wc.state.base {
        Some(b) if !args.no_rebase => b,
        _ => return plain_sync(ctx, head),
    };
    if base == head {
        let kept = st.entries.len();
        if ctx.json {
            print_json(&serde_json::json!({
                "base": head, "written": 0, "removed": 0, "kept": kept
            }))?;
        } else {
            println!(
                "Already at {} ({} kept)",
                short(head),
                plural(kept, "uncommitted change")
            );
        }
        return Ok(0);
    }

    // Unlanded commits move first, so the snapshot is rebased onto them.
    let moved = rebase_stack(ctx, base, head)?;
    let new_base = moved.map_or(head, |(tip, _)| tip);
    if st.is_clean() {
        if moved.is_none() {
            return plain_sync(ctx, head);
        }
        let stats = materialize(ctx, new_base, false)?;
        return finish(ctx, new_base, &stats, moved, None, &[], &[]);
    }

    let snapshot = snapshot::create(ctx, &st, "leep: sync snapshot".into(), "sync")?;
    let kept_msg = |e: CliError| {
        let note = format!(
            "{e}\nnothing changed locally; your uncommitted changes are also saved on the \
             server as snapshot {snapshot} (`leep stash apply {snapshot}` re-applies them)"
        );
        match e {
            CliError::User(_) => CliError::user(note),
            CliError::Server(_) => CliError::server(note),
        }
    };
    let plan = overlay::plan(ctx, snapshot, new_base).map_err(kept_msg)?;
    let stats = overlay::apply(ctx, &plan).map_err(kept_msg)?;
    finish(
        ctx,
        new_base,
        &stats,
        moved,
        Some(snapshot),
        &plan.conflicts,
        &plan.unmerged,
    )
}

fn plain_sync(ctx: &mut Ctx, head: leep_core::ChangesetId) -> Result<i32> {
    ctx.require_clean()?;
    let stats = materialize(ctx, head, false)?;
    report(ctx, "Synced to", head, &stats)?;
    Ok(0)
}

fn finish(
    ctx: &Ctx,
    new_base: leep_core::ChangesetId,
    stats: &crate::materialize::Stats,
    moved: Option<(leep_core::ChangesetId, usize)>,
    snapshot: Option<leep_core::ChangesetId>,
    conflicts: &[String],
    unmerged: &[String],
) -> Result<i32> {
    let code = i32::from(!conflicts.is_empty() || !unmerged.is_empty());
    if ctx.json {
        print_json(&serde_json::json!({
            "base": new_base, "written": stats.written, "removed": stats.removed,
            "rebased_commits": moved.map_or(0, |(_, n)| n),
            "snapshot": snapshot, "conflicts": conflicts, "unmerged": unmerged,
        }))?;
        return Ok(code);
    }
    if let Some((_, n)) = moved {
        println!(
            "Moved {} onto {}",
            plural(n, "local commit"),
            ctx.wc.config.target_ref
        );
    }
    println!(
        "Synced to {} ({} written, {} removed)",
        short(new_base),
        stats.written,
        stats.removed
    );
    if snapshot.is_some() {
        println!("Your uncommitted changes were carried over.");
    }
    for p in conflicts {
        println!("  C {p} (markers written)");
    }
    for p in unmerged {
        println!(
            "  C {p} (no text merge: kept your version; `leep cat {} {p}` shows the other side)",
            short(new_base)
        );
    }
    if code == 1 {
        println!(
            "Resolve the conflicts (`leep resolve PATH` when you have chosen, or \
             `leep restore PATH`), then `leep commit`."
        );
    }
    Ok(code)
}

fn report(
    ctx: &Ctx,
    verb: &str,
    id: leep_core::ChangesetId,
    stats: &crate::materialize::Stats,
) -> Result<()> {
    if ctx.json {
        print_json(&serde_json::json!({
            "base": id, "written": stats.written, "removed": stats.removed
        }))
    } else {
        println!(
            "{verb} {} ({} written, {} removed)",
            short(id),
            stats.written,
            stats.removed
        );
        Ok(())
    }
}
