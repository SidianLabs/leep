//! `leep amend`: fold uncommitted changes (and a new message) into the base
//! changeset, as long as it has not landed.

use std::collections::BTreeMap;
use std::path::PathBuf;

use clap::Args;
use leep_core::{Changeset, FileChange};

use super::commit::agent_extra;
use super::{print_json, Ctx};
use crate::error::{CliError, Result};
use crate::fmt::{plural, short};
use crate::overlay::contained_in;
use crate::sign::AuthorKey;
use crate::snapshot;
use crate::status::status;

#[derive(Args, Debug, Default, Clone)]
pub struct AmendArgs {
    /// New commit message (default: keep the old one).
    #[arg(short, long)]
    pub message: Option<String>,
    /// Replace the author (default: keep it).
    #[arg(long)]
    pub author: Option<String>,
    /// Replace `agent.model`.
    #[arg(long, env = "LEEP_AGENT_MODEL")]
    pub agent_model: Option<String>,
    /// Replace `agent.session`.
    #[arg(long, env = "LEEP_AGENT_SESSION")]
    pub agent_session: Option<String>,
    /// Replace `agent.prompt`.
    #[arg(long, env = "LEEP_AGENT_PROMPT")]
    pub agent_prompt: Option<String>,
    /// Replace `agent.tests`.
    #[arg(long, env = "LEEP_AGENT_TESTS")]
    pub agent_tests: Option<String>,
    /// Sign the new changeset with this private key file. Signatures on the
    /// old changeset do not carry over.
    #[arg(long, env = "LEEP_SIGNING_KEY", value_name = "KEYFILE")]
    pub sign: Option<PathBuf>,
}

pub fn run(ctx: &mut Ctx, args: &AmendArgs) -> Result<i32> {
    let key = args
        .sign
        .as_deref()
        .filter(|p| !p.as_os_str().is_empty())
        .map(AuthorKey::load)
        .transpose()?;
    let base = ctx
        .wc
        .state
        .base
        .ok_or_else(|| CliError::user("nothing to amend: no commits yet"))?;
    let st = status(&ctx.wc)?;
    st.refuse_conflicts()?;
    let target = ctx.wc.config.target_ref.clone();
    if let Some(head) = ctx.client.get_ref(&target)?.target {
        if contained_in(ctx, base, head)? {
            return Err(CliError::user(format!(
                "{} is already landed on {target}; make a new commit, or `leep revert` it",
                short(base)
            )));
        }
    }
    let old = ctx.client.get_changeset(&base)?.changeset;
    let extra_given = agent_extra(
        &args.agent_model,
        &args.agent_session,
        &args.agent_prompt,
        &args.agent_tests,
    );
    if st.is_clean() && args.message.is_none() && extra_given.is_empty() && args.author.is_none() {
        return Err(CliError::user(
            "nothing to amend: no uncommitted changes and no new message",
        ));
    }

    let local = snapshot::upload_changes(ctx, &st)?;
    let changes = combine(ctx, &old, local)?;

    let mut extra = old.extra.clone();
    extra.extend(extra_given);
    let cs = Changeset {
        parents: old.parents.clone(),
        author: args.author.clone().unwrap_or_else(|| old.author.clone()),
        timestamp: snapshot::now_secs(),
        message: args.message.clone().unwrap_or_else(|| old.message.clone()),
        changes,
        extra,
    };
    let id = ctx.client.create_changeset(&cs)?;
    let had_signatures = ctx
        .client
        .signatures(&base)
        .is_ok_and(|s| !s.signatures.is_empty());
    let signed_by = match &key {
        Some(k) => Some(ctx.client.add_signature(&id, &k.sign(&id)).map_err(|e| {
            CliError::user(format!(
                "created {} but could not sign it: {e}; nothing was amended locally, retry",
                short(id)
            ))
        })?),
        None => None,
    };

    for e in st.entries {
        match e.now {
            Some(now) => {
                ctx.wc.state.files.insert(e.path, now);
            }
            None => {
                ctx.wc.state.files.remove(&e.path);
            }
        }
    }
    ctx.wc.state.conflicts.clear();
    ctx.wc.state.unresolved.clear();
    ctx.wc.state.base = Some(id);
    ctx.wc.save_state()?;

    if ctx.json {
        print_json(&serde_json::json!({
            "id": id, "replaced": base, "files": cs.changes.len(),
            "signed": signed_by.is_some(),
        }))?;
        return Ok(0);
    }
    println!("{id}");
    eprintln!(
        "amended {} -> {} ({})",
        short(base),
        short(id),
        plural(cs.changes.len(), "file")
    );
    if let Some(s) = signed_by {
        eprintln!("signed as {} (key {})", s.name, s.key_id);
    } else if had_signatures {
        eprintln!(
            "warning: {} was signed; the signature does not carry over. \
             Sign the new changeset with `leep amend --sign KEYFILE`",
            short(base)
        );
    }
    Ok(0)
}

/// The base's changes plus the uncommitted ones, relative to the base's first
/// parent. A path both touch is looked up in the parent so that a file the
/// base added and the user then deleted drops out, and one put back to the
/// parent's content is not recorded as a change.
fn combine(
    ctx: &Ctx,
    old: &Changeset,
    local: BTreeMap<leep_core::RepoPath, FileChange>,
) -> Result<BTreeMap<leep_core::RepoPath, FileChange>> {
    let mut out = old.changes.clone();
    for (path, change) in local {
        if !old.changes.contains_key(&path) {
            out.insert(path, change);
            continue;
        }
        let in_parent = match old.parents.first() {
            Some(p) => ctx
                .client
                .files(p, path.as_str())?
                .files
                .into_iter()
                .find(|f| f.path == path),
            None => None,
        };
        match (change, in_parent) {
            (FileChange::Delete, None) => {
                out.remove(&path);
            }
            (FileChange::Modify { content, kind, .. }, Some(p))
                if p.content == content && p.kind == kind =>
            {
                out.remove(&path);
            }
            (change, _) => {
                out.insert(path, change);
            }
        }
    }
    Ok(out)
}
