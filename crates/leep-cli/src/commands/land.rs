//! `leep land`.

use std::collections::BTreeMap;
use std::fs;

use clap::Args;
use leep_core::api::{Conflict, LandRequest, LandResponse};
use leep_core::signature::NOT_SIGNED_MARKER;
use leep_core::{ChangesetId, FileChange, FileKind, RepoPath};

use super::{print_json, Ctx};
use crate::error::{CliError, Result};
use crate::fmt::short;
use crate::materialize::{materialize, write_file};
use crate::status::status;

#[derive(Args, Debug)]
pub struct LandArgs {
    /// Ref to land on (default: the working copy's target ref).
    #[arg(long = "ref")]
    pub target_ref: Option<String>,
}

pub fn run(ctx: &mut Ctx, args: &LandArgs) -> Result<i32> {
    let base = ctx
        .wc
        .state
        .base
        .ok_or_else(|| CliError::user("nothing to land: no commits yet"))?;
    let st = status(&ctx.wc)?;
    if !st.is_clean() {
        let what = if st.has_conflicts() {
            "unresolved conflicts"
        } else {
            "uncommitted changes"
        };
        return Err(CliError::user(format!(
            "working copy has {what}; commit before landing"
        )));
    }
    let target_ref = args
        .target_ref
        .clone()
        .unwrap_or_else(|| ctx.wc.config.target_ref.clone());
    let resp = ctx
        .client
        .land(&LandRequest {
            changeset: base,
            target_ref: target_ref.clone(),
        })
        .map_err(|e| hint_unsigned(e, &target_ref))?;
    if ctx.json {
        print_json(&resp)?;
    }
    match resp {
        LandResponse::Landed {
            landed,
            rebased,
            attempts,
        } => {
            if rebased {
                materialize(ctx, landed, false)?;
            }
            if !ctx.json {
                println!(
                    "Landed {} on {target_ref}{} ({attempts} attempt{})",
                    short(landed),
                    if rebased { " (rebased)" } else { "" },
                    if attempts == 1 { "" } else { "s" },
                );
            }
            Ok(0)
        }
        LandResponse::Conflicts {
            onto,
            conflicts,
            clean,
        } => {
            let unmerged = rebase_locally(ctx, onto, &conflicts, &clean)?;
            if !ctx.json {
                println!(
                    "Cannot land cleanly: {} conflict{} against {} on {target_ref}",
                    conflicts.len(),
                    if conflicts.len() == 1 { "" } else { "s" },
                    short(onto)
                );
                for c in &conflicts {
                    let note = if unmerged.contains(&c.path.to_string()) {
                        "kept your version; no text merge available, review it"
                    } else {
                        "markers written"
                    };
                    println!("  C {} ({note})", c.path);
                }
                println!(
                    "Your working copy is now on {} with your changes applied.\n\
                     Resolve the conflicts, then run `leep commit` and `leep land`.",
                    short(onto)
                );
            }
            Ok(1)
        }
    }
}

/// The server names the unsigned changeset; add what to do about it.
fn hint_unsigned(e: CliError, target_ref: &str) -> CliError {
    match e {
        CliError::User(m) if m.contains(NOT_SIGNED_MARKER) => CliError::User(format!(
            "{m}\n{target_ref} only accepts signed changesets: every changeset being landed needs a signature. \
             Commit with `leep commit --sign KEYFILE` (or set LEEP_SIGNING_KEY); create a key with `leep keygen`."
        )),
        e => e,
    }
}

/// Move the working copy to `onto` and re-apply the local changes there:
/// `clean` as-is, conflicts as marker files (or the local version when no
/// text merge exists). The user then resolves and commits once on `onto`,
/// which lands as a fast-forward. Returns the conflicted paths without
/// markers.
fn rebase_locally(
    ctx: &mut Ctx,
    onto: ChangesetId,
    conflicts: &[Conflict],
    clean: &BTreeMap<RepoPath, FileChange>,
) -> Result<Vec<String>> {
    let outside: Vec<String> = clean
        .keys()
        .chain(conflicts.iter().map(|c| &c.path))
        .map(|p| p.to_string())
        .filter(|p| !ctx.wc.in_scope(p))
        .collect();
    if !outside.is_empty() {
        return Err(CliError::user(format!(
            "cannot resolve here: the conflicting stack touches paths outside this \
             sparse checkout ({}); widen it with `leep checkout @ --prefix ...` and land again",
            outside.join(", ")
        )));
    }

    materialize(ctx, onto, false)?;
    for (path, change) in clean {
        apply(ctx, path.as_str(), change)?;
    }
    let mut unmerged = Vec::new();
    for c in conflicts {
        let path = c.path.to_string();
        match (&c.merged_with_markers, &c.theirs) {
            (Some(text), _) => {
                write_file(&ctx.wc.abs(&path), text.as_bytes(), FileKind::Regular)?;
                if !ctx.wc.state.conflicts.contains(&path) {
                    ctx.wc.state.conflicts.push(path);
                }
            }
            (None, theirs) => {
                let change = match theirs {
                    Some(i) => FileChange::Modify {
                        content: i.content,
                        kind: i.kind,
                        size: i.size,
                    },
                    None => FileChange::Delete,
                };
                apply(ctx, &path, &change)?;
                unmerged.push(path);
            }
        }
    }
    ctx.wc.save_state()?;
    Ok(unmerged)
}

/// Write one change to disk without recording it in state, so it shows up as
/// a local modification.
fn apply(ctx: &Ctx, path: &str, change: &FileChange) -> Result<()> {
    let abs = ctx.wc.abs(path);
    match change {
        FileChange::Modify { content, kind, .. } => {
            let data = ctx.cache.fetch(&ctx.client, content)?;
            write_file(&abs, &data, *kind)
        }
        FileChange::Delete => match fs::remove_file(&abs) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        },
    }
}
