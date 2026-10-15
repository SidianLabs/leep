//! `leep reset`: move the working copy's base. Never touches a server ref.

use std::collections::BTreeMap;

use clap::Args;
use leep_core::ChangesetId;

use super::{print_json, Ctx};
use crate::error::{CliError, Result};
use crate::fmt::{plural, short};
use crate::materialize::{list_files, materialize};
use crate::state::FileState;
use crate::status::{status, Change};

#[derive(Args, Debug)]
pub struct ResetArgs {
    /// Revision to move the base to (default: the base's first parent, which
    /// undoes the last commit and keeps its changes).
    pub rev: Option<String>,
    /// Also make the files match REV, discarding uncommitted changes.
    #[arg(long)]
    pub hard: bool,
    /// With `--hard`: allow it with uncommitted changes, and overwrite
    /// untracked files in the way.
    #[arg(long, requires = "hard")]
    pub force: bool,
}

pub fn run(ctx: &mut Ctx, args: &ResetArgs) -> Result<i32> {
    // A hard reset is the way out of an unfinished merge, so only the soft
    // one, which would keep its files, is refused.
    if !args.hard {
        ctx.refuse_in_progress()?;
    }
    let old = ctx
        .wc
        .state
        .base
        .ok_or_else(|| CliError::user("nothing to reset: no commits yet"))?;
    let target = match args.rev.as_deref() {
        Some(r) => ctx.remote().resolve(r)?,
        None => *ctx
            .client
            .get_changeset(&old)?
            .changeset
            .parents
            .first()
            .ok_or_else(|| {
                CliError::user(format!(
                    "{} has no parent to reset to; give a REV",
                    short(old)
                ))
            })?,
    };
    // Resolves the content before anything changes: an unknown or broken REV
    // fails here.
    let listing = list_files(ctx, &target)?;
    let before = status(&ctx.wc)?;
    if args.hard && !args.force && !before.is_clean() {
        return Err(CliError::user(
            "working copy has uncommitted changes; --hard would discard them (use --force)",
        ));
    }

    let (discarded, kept) = if args.hard {
        // New files not in REV stay; everything else listed is rewritten.
        let (gone, stay): (Vec<_>, Vec<_>) = before
            .entries
            .iter()
            .partition(|e| e.change != Change::Added || listing.contains_key(&e.path));
        let gone: Vec<String> = gone.into_iter().map(|e| e.path.clone()).collect();
        let kept: Vec<String> = stay.into_iter().map(|e| e.path.clone()).collect();
        materialize(ctx, target, args.force)?;
        (gone, kept)
    } else {
        // Keep the files; only what they are compared against changes. An
        // impossible mtime on changed entries makes status rehash them.
        let mut files = BTreeMap::new();
        for (p, f) in &listing {
            let state = match ctx.wc.state.files.get(p) {
                Some(o) if o.content == f.content && o.kind == f.kind => o.clone(),
                _ => FileState {
                    content: f.content,
                    kind: f.kind,
                    size: f.size,
                    mtime_ns: 0,
                },
            };
            files.insert(p.clone(), state);
        }
        ctx.wc.state.files = files;
        ctx.wc.state.base = Some(target);
        ctx.wc.save_state()?;
        (Vec::new(), Vec::new())
    };

    let after = status(&ctx.wc)?;
    report(
        ctx,
        old,
        target,
        args.hard,
        &discarded,
        &kept,
        after.entries.len(),
    )?;
    Ok(0)
}

fn report(
    ctx: &Ctx,
    old: ChangesetId,
    new: ChangesetId,
    hard: bool,
    discarded: &[String],
    kept: &[String],
    pending: usize,
) -> Result<()> {
    if ctx.json {
        return print_json(&serde_json::json!({
            "base": new, "previous": old, "hard": hard,
            "discarded": discarded, "kept": kept, "uncommitted": pending,
        }));
    }
    for p in discarded {
        println!("discarded {p}");
    }
    for p in kept {
        println!("kept new file {p}");
    }
    if hard {
        println!(
            "Reset to {} (was {}); discarded {}",
            short(new),
            short(old),
            plural(discarded.len(), "change")
        );
    } else {
        println!(
            "Reset base to {} (was {}); {} kept as uncommitted",
            short(new),
            short(old),
            plural(pending, "change")
        );
    }
    Ok(())
}
