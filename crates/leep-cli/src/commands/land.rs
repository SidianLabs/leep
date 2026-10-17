//! `leep land`.

use std::collections::BTreeMap;
use std::fs;

use clap::Args;
use leep_core::api::{Conflict, LandRequest, LandResponse};
use leep_core::refname::MISSING_REF_MARKER;
use leep_core::review::NEEDS_REVIEW_MARKER;
use leep_core::signature::NOT_SIGNED_MARKER;
use leep_core::{ChangesetId, FileChange, FileKind, RepoPath};

use super::{print_json, Ctx};
use crate::error::{exit, CliError, ErrorKind, Result};
use crate::fmt::short;
use crate::markers::{relabel, Labels};
use crate::materialize::{materialize, write_file};
use crate::status::status;

#[derive(Args, Debug)]
pub struct LandArgs {
    /// Ref to land on (default: the working copy's target ref).
    #[arg(long = "ref")]
    pub target_ref: Option<String>,
    /// Create the ref if it does not exist. Without it, landing onto a
    /// missing ref fails (exit 4), so a typo cannot make a branch; only the
    /// first land onto `main` of an empty repository creates it unasked.
    #[arg(long)]
    pub create: bool,
}

pub fn run(ctx: &mut Ctx, args: &LandArgs) -> Result<i32> {
    ctx.refuse_in_progress()?;
    let base = ctx
        .wc
        .state
        .base
        .ok_or_else(|| CliError::user("nothing to land: no commits yet"))?;
    let st = status(&ctx.wc)?;
    if !st.is_clean() {
        st.refuse_conflicts()?;
        return Err(CliError::user(
            "working copy has uncommitted changes; commit before landing",
        ));
    }
    let target_ref = args
        .target_ref
        .clone()
        .unwrap_or_else(|| ctx.wc.config.target_ref.clone());
    let (resp, created) = land_creating(ctx, base, &target_ref, args.create)?;
    if created {
        eprintln!("created ref {target_ref}");
    }
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
            let unmerged = rebase_locally(ctx, &target_ref, onto, &conflicts, &clean)?;
            if !ctx.json {
                println!(
                    "Cannot land cleanly: {} conflict{} against {} on {target_ref}",
                    conflicts.len(),
                    if conflicts.len() == 1 { "" } else { "s" },
                    short(onto)
                );
                for c in &conflicts {
                    let note = if unmerged.contains(&c.path.to_string()) {
                        format!(
                            "no text merge: kept your version; `leep cat {} {}` shows the other side",
                            short(onto),
                            c.path
                        )
                    } else {
                        "markers written".to_string()
                    };
                    println!("  C {} ({note})", c.path);
                }
                println!(
                    "Your working copy is now on {} with your changes applied.\n\
                     Resolve the conflicts (`leep resolve PATH` once chosen), then run `leep commit` and `leep land`.",
                    short(onto)
                );
            }
            Ok(exit::CONFLICT)
        }
    }
}

/// Land, creating the ref only if asked to or if this is the first land onto
/// `main` of an empty repository (ADR 0035). Returns whether the ref was
/// created.
fn land_creating(
    ctx: &Ctx,
    base: ChangesetId,
    target_ref: &str,
    create: bool,
) -> Result<(LandResponse, bool)> {
    let land = |create: bool| {
        ctx.client
            .land(&LandRequest {
                changeset: base,
                target_ref: target_ref.to_string(),
                create: Some(create),
            })
            .map_err(|e| hint_policy(e, target_ref))
    };
    // Whether `--create` will create or just land: asked only when it may.
    let existed = if create {
        ctx.client.get_ref(target_ref)?.target.is_some()
    } else {
        true
    };
    match land(create) {
        Ok(resp) => {
            let created = !existed && landed(&resp);
            Ok((resp, created))
        }
        Err(e) if missing_ref(&e) => {
            if target_ref == "main" {
                let refs = ctx.client.refs()?.refs;
                if refs.is_empty() {
                    let resp = land(true)?;
                    let created = landed(&resp);
                    return Ok((resp, created));
                }
                // Another client's first land got there in between.
                if refs.contains_key("main") {
                    return Ok((land(false)?, false));
                }
            }
            Err(e.with_hint(format!(
                "`leep land --ref {target_ref} --create` creates it; `leep branch` lists branches"
            )))
        }
        Err(e) => Err(e),
    }
}

fn landed(resp: &LandResponse) -> bool {
    matches!(resp, LandResponse::Landed { .. })
}

/// A land refused because the ref does not exist and may not be created.
fn missing_ref(e: &CliError) -> bool {
    e.kind() == ErrorKind::NotFound && e.message().contains(MISSING_REF_MARKER)
}

/// The server names the changeset a policy refused; add what to do about it.
fn hint_policy(e: CliError, target_ref: &str) -> CliError {
    if e.kind() != ErrorKind::User {
        return e;
    }
    if e.message().contains(NEEDS_REVIEW_MARKER) {
        return e.map_message(|m| format!(
            "{m}\n{target_ref} needs approval from the owners of the paths a changeset changes: \
             ask one to run `leep review approve <changeset>`; `leep review status <changeset> --ref {target_ref}` shows what is missing."
        ));
    }
    if e.message().contains(NOT_SIGNED_MARKER) {
        return e.map_message(|m| format!(
            "{m}\n{target_ref} only accepts signed changesets: every changeset being landed needs a signature. \
             Commit with `leep commit --sign KEYFILE` (or set LEEP_SIGNING_KEY); create a key with `leep keygen`."
        ));
    }
    e
}

/// Move the working copy to `onto` and re-apply the local changes there:
/// `clean` as-is, conflicts as marker files (or the local version when no
/// text merge exists). The user then resolves and commits once on `onto`,
/// which lands as a fast-forward. Returns the conflicted paths without
/// markers.
fn rebase_locally(
    ctx: &mut Ctx,
    target_ref: &str,
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
                let text = relabel(text, &Labels::landed(target_ref));
                write_file(
                    &ctx.wc.abs_for_write(&path)?,
                    text.as_bytes(),
                    FileKind::Regular,
                )?;
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
                if !ctx.wc.state.unresolved.contains(&path) {
                    ctx.wc.state.unresolved.push(path.clone());
                }
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
    let abs = ctx.wc.abs_for_write(path)?;
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
