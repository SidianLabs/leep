//! `leep checkout` and `leep sync`.

use clap::Args;

use super::{print_json, Ctx};
use crate::error::{CliError, Result};
use crate::fmt::short;
use crate::materialize::materialize;
use crate::state::normalize_prefix;

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

/// Move to the current head of the target ref.
pub fn sync(ctx: &mut Ctx) -> Result<i32> {
    ctx.require_clean()?;
    let name = ctx.wc.config.target_ref.clone();
    let head = ctx
        .client
        .get_ref(&name)?
        .target
        .ok_or_else(|| CliError::user(format!("ref {name} does not exist")))?;
    let stats = materialize(ctx, head, false)?;
    report(ctx, "Synced to", head, &stats)?;
    Ok(0)
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
