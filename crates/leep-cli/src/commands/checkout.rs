//! `leep checkout` and `leep sync`.

use clap::Args;

use super::{print_json, Ctx};
use crate::error::{exit, CliError, Result};
use crate::fmt::{plural, short};
use crate::materialize::materialize;
use crate::overlay::{self, rebase_stack};
use crate::snapshot;
use crate::state::normalize_prefix;
use crate::status::status;
use leep_core::outln;

#[derive(Args, Debug)]
pub struct CheckoutArgs {
    /// Ref or tag name, changeset id (full, or 4+ leading hex digits), or `@`.
    pub rev: String,
    /// Replace the sparse prefixes with these (repeatable). The whole set
    /// must be given; see --add-prefix to widen.
    #[arg(long = "prefix", conflicts_with = "add_prefixes")]
    pub prefixes: Vec<String>,
    /// Add sparse prefixes to the current set (repeatable).
    #[arg(long = "add-prefix", value_name = "PREFIX")]
    pub add_prefixes: Vec<String>,
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
    } else if !args.add_prefixes.is_empty() {
        // A working copy with no prefixes holds everything already; adding
        // one would narrow it, so there is nothing to widen.
        if ctx.wc.config.prefixes.is_empty() {
            return Err(CliError::user(
                "this working copy is not sparse: it already holds the whole repository",
            ));
        }
        for p in &args.add_prefixes {
            let p = normalize_prefix(p)?;
            if !ctx.wc.config.prefixes.contains(&p) {
                ctx.wc.config.prefixes.push(p);
            }
        }
        ctx.wc.save_config()?;
    }
    let stats = materialize(ctx, id, args.force)?;
    report(ctx, "Checked out", id, &stats)?;
    Ok(0)
}

#[derive(Args, Debug, Default)]
pub struct SyncArgs {
    /// Refuse a working copy with uncommitted changes instead of carrying
    /// them over, and do not move local commits: a base that is not on the
    /// target is refused.
    #[arg(long)]
    pub no_rebase: bool,
    /// With --no-rebase, sync even when local commits are not on the target,
    /// leaving them behind (their id is printed on stderr).
    #[arg(long, requires = "no_rebase")]
    pub discard: bool,
}

/// Move to the current head of the target ref, carrying uncommitted changes
/// and unlanded local commits along (see ADR 0017).
pub fn sync(ctx: &mut Ctx, args: &SyncArgs) -> Result<i32> {
    ctx.refuse_in_progress()?;
    let name = ctx.wc.config.target_ref.clone();
    if args.no_rebase {
        ctx.require_clean()?;
    }
    let head = ctx
        .client
        .get_ref(&name)?
        .target
        .ok_or_else(|| CliError::not_found(format!("ref {name} does not exist")))?;
    let st = status(&ctx.wc)?;
    st.refuse_conflicts()?;
    let base = match ctx.wc.state.base {
        Some(b) if !args.no_rebase => b,
        Some(b) => return strict_sync(ctx, b, head, args.discard),
        None => return plain_sync(ctx, head),
    };
    if base == head {
        let kept = st.entries.len();
        if ctx.json {
            print_json(&serde_json::json!({
                "base": head, "written": 0, "removed": 0, "kept": kept
            }))?;
        } else {
            outln!(
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
        e.map_message(|_| note)
    };
    let labels = crate::markers::Labels::landed(&ctx.wc.config.target_ref);
    let plan = overlay::plan(ctx, snapshot, new_base, &labels).map_err(kept_msg)?;
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

/// `sync --no-rebase`: move to `head` only when that leaves no local commit
/// behind, unless `discard` says to. A changeset no ref points at is easily
/// lost and may be collected (ADR 0017).
fn strict_sync(
    ctx: &mut Ctx,
    base: leep_core::ChangesetId,
    head: leep_core::ChangesetId,
    discard: bool,
) -> Result<i32> {
    let target = ctx.wc.config.target_ref.clone();
    let fork = ctx.client.merge_base(&base, &head)?.base;
    if fork == Some(base) {
        return plain_sync(ctx, head);
    }
    if !discard {
        let what = match fork {
            Some(f) => {
                let gen = |id| ctx.client.get_changeset(&id).map(|c| c.generation);
                let n = gen(base)?.saturating_sub(gen(f)?) as usize;
                let verb = if n == 1 { "is" } else { "are" };
                format!(
                    "{} ({}..) {verb} not on {target}",
                    plural(n, "local commit"),
                    short(base)
                )
            }
            None => format!("base {} shares no history with {target}", short(base)),
        };
        return Err(CliError::user(format!(
            "{what}; `leep sync` rebases them onto {target}, \
             `leep sync --no-rebase --discard` abandons them"
        ))
        .with_hint(format!(
            "`leep branch <name> {base}` keeps them under a name"
        )));
    }
    let code = plain_sync(ctx, head)?;
    eprintln!(
        "note: abandoned {}, which is not on {target}; return to it with \
         `leep checkout {base}` or keep it with `leep branch <name> {base}`",
        short(base)
    );
    Ok(code)
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
    let code = if conflicts.is_empty() && unmerged.is_empty() {
        exit::OK
    } else {
        exit::CONFLICT
    };
    if ctx.json {
        print_json(&serde_json::json!({
            "base": new_base, "written": stats.written, "removed": stats.removed,
            "rebased_commits": moved.map_or(0, |(_, n)| n),
            "snapshot": snapshot, "conflicts": conflicts, "unmerged": unmerged,
        }))?;
        return Ok(code);
    }
    if let Some((_, n)) = moved {
        outln!(
            "Moved {} onto {}",
            plural(n, "local commit"),
            ctx.wc.config.target_ref
        );
    }
    outln!(
        "Synced to {} ({} written, {} removed)",
        short(new_base),
        stats.written,
        stats.removed
    );
    if snapshot.is_some() {
        outln!("Your uncommitted changes were carried over.");
    }
    for p in conflicts {
        outln!("  C {p} (markers written)");
    }
    for p in unmerged {
        outln!(
            "  C {p} (no text merge: kept your version; `leep cat {} {p}` shows the other side)",
            short(new_base)
        );
    }
    if code == exit::CONFLICT {
        outln!(
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
        outln!(
            "{verb} {} ({} written, {} removed)",
            short(id),
            stats.written,
            stats.removed
        );
        Ok(())
    }
}
