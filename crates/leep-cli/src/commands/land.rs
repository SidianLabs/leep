//! `leep land`.

use std::collections::BTreeMap;
use std::fs;

use clap::Args;
use leep_core::api::{error_code, Conflict, LandId, LandRequest, LandResponse, FEATURE_LAND_ID};
use leep_core::refname::MISSING_REF_MARKER;
use leep_core::review::NEEDS_REVIEW_MARKER;
use leep_core::signature::NOT_SIGNED_MARKER;
use leep_core::{ChangesetId, FileChange, FileKind, RepoPath};

use super::{print_json, Ctx};
use crate::client::new_land_id;
use crate::error::{exit, CliError, ErrorKind, Result};
use crate::fmt::short;
use crate::markers::{relabel, Labels};
use crate::materialize::{materialize, write_file};
use crate::state::PendingLand;
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
/// `main` of an empty repository (ADR 0036). Returns whether the ref was
/// created.
fn land_creating(
    ctx: &mut Ctx,
    base: ChangesetId,
    target_ref: &str,
    create: bool,
) -> Result<(LandResponse, bool)> {
    let land_id = pending_land_id(ctx, base, target_ref)?;
    let land = |create: bool| {
        let r = ctx.client.land_resending(&LandRequest {
            changeset: base,
            target_ref: target_ref.to_string(),
            create: Some(create),
            land_id: Some(land_id.clone()),
        });
        r.map_err(|e| hint_policy(e, target_ref))
    };
    let result = land_creating_with(ctx, target_ref, create, land);
    // An answer, or an error that sending again cannot change, ends this
    // land; otherwise the next `leep land` resends it under the same id.
    if !result.as_ref().is_err_and(|e| e.is_retryable()) {
        ctx.wc.state.pending_land = None;
        ctx.wc.save_state()?;
    }
    result
}

/// What to do about a land an earlier run sent without a final answer.
#[derive(Debug, PartialEq, Eq)]
enum Pending {
    /// None for this changeset and ref: a new land, with a new id.
    New,
    /// Send it again under its id: the server answers with the earlier
    /// result if it applied.
    Resend(LandId),
    /// The server does not record land ids, and sending again could land it
    /// twice (once its copy is past the server's landed-copy scan).
    Ask,
}

fn pending(
    p: Option<&PendingLand>,
    base: ChangesetId,
    target_ref: &str,
    server_records_ids: impl FnOnce() -> Result<bool>,
) -> Result<Pending> {
    Ok(match p {
        Some(p) if p.changeset == base && p.target_ref == target_ref => {
            if server_records_ids()? {
                Pending::Resend(p.land_id.clone())
            } else {
                Pending::Ask
            }
        }
        _ => Pending::New,
    })
}

/// The id of the land of `base` onto `target_ref`: the one a land without a
/// final answer used (ADR 0057), when the server records ids, or a new one,
/// saved before it is sent.
fn pending_land_id(ctx: &mut Ctx, base: ChangesetId, target_ref: &str) -> Result<LandId> {
    let client = &ctx.client;
    match pending(ctx.wc.state.pending_land.as_ref(), base, target_ref, || {
        Ok(client
            .server_features()?
            .iter()
            .any(|f| f == FEATURE_LAND_ID))
    })? {
        Pending::Resend(id) => return Ok(id),
        Pending::Ask => {
            ctx.wc.state.pending_land = None;
            ctx.wc.save_state()?;
            return Err(CliError::user(format!(
                "an earlier `leep land` of {} onto {target_ref} got no answer, and it may have \
                 landed; this server does not record land ids, so sending it again could land it twice",
                short(base)
            ))
            .with_hint(format!(
                "`leep reflog {target_ref}` shows `landed {base}` if it did; if not, run `leep land` again"
            )));
        }
        Pending::New => {}
    }
    let land_id = new_land_id();
    ctx.wc.state.pending_land = Some(PendingLand {
        changeset: base,
        target_ref: target_ref.to_string(),
        land_id: land_id.clone(),
    });
    ctx.wc.save_state()?;
    Ok(land_id)
}

fn land_creating_with(
    ctx: &Ctx,
    target_ref: &str,
    create: bool,
    land: impl Fn(bool) -> Result<LandResponse>,
) -> Result<(LandResponse, bool)> {
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
    e.kind() == ErrorKind::NotFound && e.is_server(error_code::REF_MISSING, MISSING_REF_MARKER)
}

/// The server names the changeset a policy refused; add what to do about it.
fn hint_policy(e: CliError, target_ref: &str) -> CliError {
    if e.kind() != ErrorKind::User {
        return e;
    }
    if e.is_server(error_code::NEEDS_REVIEW, NEEDS_REVIEW_MARKER) {
        return e.map_message(|m| format!(
            "{m}\n{target_ref} needs approval from the owners of the paths a changeset changes: \
             ask one to run `leep review approve <changeset>`; `leep review status <changeset> --ref {target_ref}` shows what is missing."
        ));
    }
    if e.is_server(error_code::NOT_SIGNED, NOT_SIGNED_MARKER) {
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
    // `materialize` recorded these files' stats moments before they were
    // overwritten. Where timestamps are coarse the second write can keep the
    // same mtime and size, and status would trust the hash of `onto`'s
    // version, so the next commit would silently drop the user's change.
    // An impossible mtime makes status rehash them.
    for path in clean
        .keys()
        .map(|p| p.as_str())
        .chain(conflicts.iter().map(|c| c.path.as_str()))
    {
        if let Some(f) = ctx.wc.state.files.get_mut(path) {
            f.mtime_ns = 0;
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

#[cfg(test)]
mod tests {
    use super::*;
    use leep_core::Hash;

    #[test]
    fn a_pending_land_is_resent_only_to_a_server_that_records_ids() {
        let (a, b) = (
            ChangesetId(Hash::of_blob(b"a")),
            ChangesetId(Hash::of_blob(b"b")),
        );
        let id = new_land_id();
        let p = PendingLand {
            changeset: a,
            target_ref: "main".into(),
            land_id: id.clone(),
        };
        let ask = |p, cs, r: &str, has: Result<bool>| pending(p, cs, r, || has).ok();
        assert_eq!(ask(None, a, "main", Ok(true)), Some(Pending::New));
        assert_eq!(
            ask(Some(&p), a, "main", Ok(true)),
            Some(Pending::Resend(id))
        );
        assert_eq!(ask(Some(&p), a, "main", Ok(false)), Some(Pending::Ask));
        // A server that cannot be asked is not taken for an old one: the
        // land stays pending.
        let down = Err(CliError::new(ErrorKind::Unreachable, "down"));
        assert_eq!(ask(Some(&p), a, "main", down), None);
        // Another changeset or ref is another land; the server is not asked.
        let never = || -> Result<bool> { unreachable!() };
        assert_eq!(pending(Some(&p), b, "main", never).ok(), Some(Pending::New));
        assert_eq!(pending(Some(&p), a, "dev", never).ok(), Some(Pending::New));
    }
}
