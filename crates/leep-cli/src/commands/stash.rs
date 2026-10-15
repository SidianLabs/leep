//! `leep stash`: shelve uncommitted changes as a snapshot changeset.
//!
//! The content lives on the server as an ordinary changeset
//! (`leep.snapshot = "stash"`, no ref); the list of stashes is local, in
//! `.leep/state.json`. See ADR 0017.

use clap::{Args, Subcommand};

use super::restore::discard;
use super::{print_json, Ctx};
use crate::error::{CliError, Result};
use crate::fmt::{plural, short};
use crate::overlay;
use crate::snapshot;
use crate::state::Stash;
use crate::status::{status, Change};

#[derive(Args, Debug)]
#[command(args_conflicts_with_subcommands = true)]
pub struct StashArgs {
    #[command(subcommand)]
    pub cmd: Option<StashCmd>,
    #[command(flatten)]
    pub push: PushArgs,
}

#[derive(Args, Debug, Default)]
pub struct PushArgs {
    /// Message shown by `stash list`.
    #[arg(short, long)]
    pub message: Option<String>,
}

#[derive(Subcommand, Debug)]
pub enum StashCmd {
    /// Save uncommitted changes and restore the working copy (the default).
    Push(PushArgs),
    /// List stashes, newest first.
    List,
    /// Show the files a stash changes.
    Show(IndexArg),
    /// Re-apply a stash on the current base, keeping it in the list.
    Apply(ApplyArg),
    /// Re-apply a stash and drop it if that succeeds.
    Pop(IndexArg),
    /// Forget a stash (its changeset stays on the server).
    Drop(IndexArg),
}

#[derive(Args, Debug)]
pub struct IndexArg {
    /// Position in `stash list`; 0, the newest, by default.
    pub index: Option<usize>,
}

#[derive(Args, Debug)]
pub struct ApplyArg {
    /// Position in `stash list`, or the id of any snapshot changeset (what
    /// `sync` prints if it fails). Default 0, the newest.
    pub target: Option<String>,
}

pub fn run(ctx: &mut Ctx, args: &StashArgs) -> Result<i32> {
    match &args.cmd {
        None => push(ctx, &args.push),
        Some(StashCmd::Push(a)) => push(ctx, a),
        Some(StashCmd::List) => list(ctx),
        Some(StashCmd::Show(a)) => show(ctx, a.index.unwrap_or(0)),
        Some(StashCmd::Apply(a)) => apply(ctx, a.target.as_deref(), false),
        Some(StashCmd::Pop(a)) => {
            let target = a.index.map(|i| i.to_string());
            apply(ctx, target.as_deref(), true)
        }
        Some(StashCmd::Drop(a)) => drop_one(ctx, a.index.unwrap_or(0)),
    }
}

fn nth(ctx: &Ctx, index: usize) -> Result<Stash> {
    ctx.wc.state.stashes.get(index).cloned().ok_or_else(|| {
        let n = ctx.wc.state.stashes.len();
        CliError::user(if n == 0 {
            "no stashes".to_string()
        } else {
            format!(
                "no stash {index}; there {} {}",
                if n == 1 { "is" } else { "are" },
                plural(n, "stash")
            )
        })
    })
}

fn push(ctx: &mut Ctx, args: &PushArgs) -> Result<i32> {
    ctx.refuse_in_progress()?;
    let st = status(&ctx.wc)?;
    st.refuse_conflicts()?;
    if st.is_clean() {
        return Err(CliError::user("no local changes to stash"));
    }
    let Some(base) = ctx.wc.state.base else {
        return Err(CliError::user(
            "nothing to stash against: the working copy has no base yet; commit first",
        ));
    };
    let message = args
        .message
        .clone()
        .unwrap_or_else(|| format!("WIP on {}: {}", ctx.wc.config.target_ref, short(base)));
    let id = snapshot::create(ctx, &st, message.clone(), "stash")?;
    ctx.wc.state.stashes.insert(
        0,
        Stash {
            id,
            base: Some(base),
            message: message.clone(),
            timestamp: snapshot::now_secs(),
        },
    );
    ctx.wc.save_state().map_err(|e| {
        CliError::user(format!(
            "{e}; the changes are saved as snapshot {id} but nothing was removed locally"
        ))
    })?;
    let done = discard(ctx, &st.entries, true).map_err(|e| {
        CliError::user(format!(
            "{e}; your changes are safe in stash 0 ({id}), `leep stash apply` re-applies them"
        ))
    })?;
    let added = st
        .entries
        .iter()
        .filter(|e| e.change == Change::Added)
        .count();
    if ctx.json {
        print_json(&serde_json::json!({
            "stash": id, "index": 0, "base": base, "message": message,
            "restored": done.restored, "removed": done.removed,
        }))?;
    } else {
        println!(
            "Saved stash 0 ({}): {message}\nRestored the working copy to {} ({} reverted, {} removed)",
            short(id),
            short(base),
            done.restored.len(),
            plural(added, "new file")
        );
    }
    Ok(0)
}

fn list(ctx: &Ctx) -> Result<i32> {
    let stashes = &ctx.wc.state.stashes;
    if ctx.json {
        print_json(stashes)?;
        return Ok(0);
    }
    for (i, s) in stashes.iter().enumerate() {
        let base = s.base.map(short).unwrap_or_else(|| "none".into());
        println!("{i}: {} ({}, based on {base})", s.message, short(s.id));
    }
    Ok(0)
}

fn show(ctx: &Ctx, index: usize) -> Result<i32> {
    let s = nth(ctx, index)?;
    let info = ctx.client.get_changeset(&s.id)?;
    let parent = info.changeset.parents.first();
    let diff = ctx.client.diff(parent, &s.id, None)?;
    let letter = |e: &leep_core::api::DiffEntry| match (&e.old, &e.new) {
        (None, _) => 'A',
        (_, None) => 'D',
        _ => 'M',
    };
    if ctx.json {
        let files: Vec<_> = diff
            .entries
            .iter()
            .map(|e| serde_json::json!({ "path": e.path, "status": letter(e).to_string() }))
            .collect();
        return print_json(&serde_json::json!({
            "index": index, "id": s.id, "base": s.base, "message": s.message, "files": files
        }))
        .map(|()| 0);
    }
    println!("{index}: {} ({})", s.message, short(s.id));
    for e in &diff.entries {
        println!("{} {}", letter(e), e.path);
    }
    Ok(0)
}

/// `target` is a stash position or a snapshot id. Conflicts keep the stash.
fn apply(ctx: &mut Ctx, target: Option<&str>, pop: bool) -> Result<i32> {
    ctx.refuse_in_progress()?;
    let (index, id) = match target {
        None => (Some(0), nth(ctx, 0)?.id),
        Some(t) => match t.parse::<usize>() {
            Ok(i) => (Some(i), nth(ctx, i)?.id),
            Err(_) => (None, ctx.remote().resolve(t)?),
        },
    };
    let Some(base) = ctx.wc.state.base else {
        return Err(CliError::user(
            "the working copy has no base yet; cannot apply",
        ));
    };
    let st = status(&ctx.wc)?;
    if !st.is_clean() {
        return Err(CliError::user(
            "working copy has uncommitted changes; commit or stash them first",
        ));
    }
    let plan = overlay::plan(ctx, id, base)?;
    let stats = overlay::apply(ctx, &plan)?;
    let conflicted = !plan.conflicts.is_empty() || !plan.unmerged.is_empty();
    let drop_it = pop && !conflicted;
    if drop_it {
        if let Some(i) = index {
            ctx.wc.state.stashes.remove(i);
            ctx.wc.save_state()?;
        }
    }
    if ctx.json {
        print_json(&serde_json::json!({
            "stash": id, "written": stats.written, "removed": stats.removed,
            "conflicts": plan.conflicts, "unmerged": plan.unmerged, "dropped": drop_it,
        }))?;
        return Ok(i32::from(conflicted));
    }
    println!(
        "Applied {} on {} ({} written, {} removed)",
        short(id),
        short(base),
        stats.written,
        stats.removed
    );
    for p in &plan.conflicts {
        println!("  C {p} (markers written)");
    }
    for p in &plan.unmerged {
        println!(
            "  C {p} (no text merge: kept the stashed version; `leep cat @ {p}` shows the other side)"
        );
    }
    if conflicted {
        println!("Resolve the conflicts, then `leep commit`.");
        if pop {
            println!("The stash was kept; `leep stash drop` removes it once you are done.");
        }
    } else if drop_it {
        println!("Dropped stash {}", index.unwrap_or(0));
    }
    Ok(i32::from(conflicted))
}

fn drop_one(ctx: &mut Ctx, index: usize) -> Result<i32> {
    let s = nth(ctx, index)?;
    ctx.wc.state.stashes.remove(index);
    ctx.wc.save_state()?;
    if ctx.json {
        print_json(&serde_json::json!({ "dropped": s.id, "index": index }))?;
    } else {
        println!(
            "Dropped stash {index} ({}); the changeset stays on the server",
            short(s.id)
        );
    }
    Ok(0)
}
