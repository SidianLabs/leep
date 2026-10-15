//! `leep commit`: upload new blobs, then create a changeset.

use std::collections::BTreeMap;
use std::path::PathBuf;

use clap::Args;
use leep_core::changeset::{
    EXTRA_AGENT_MODEL, EXTRA_AGENT_PROMPT, EXTRA_AGENT_SESSION, EXTRA_AGENT_TESTS,
};
use leep_core::Changeset;

use super::{print_json, Ctx};
use crate::error::{CliError, Result};
use crate::fmt::{plural, short};
use crate::sign::AuthorKey;
use crate::snapshot;
use crate::status::status;

#[derive(Args, Debug, Default, Clone)]
pub struct CommitArgs {
    /// Commit message.
    #[arg(short, long)]
    pub message: String,
    /// Author (default: $LEEP_AUTHOR, else $USER).
    #[arg(long)]
    pub author: Option<String>,
    /// Model that produced the change.
    #[arg(long, env = "LEEP_AGENT_MODEL")]
    pub agent_model: Option<String>,
    /// Agent session identifier.
    #[arg(long, env = "LEEP_AGENT_SESSION")]
    pub agent_session: Option<String>,
    /// Prompt that led to the change.
    #[arg(long, env = "LEEP_AGENT_PROMPT")]
    pub agent_prompt: Option<String>,
    /// Test results or commands the agent ran.
    #[arg(long, env = "LEEP_AGENT_TESTS")]
    pub agent_tests: Option<String>,
    /// Sign the changeset with this private key file (from `leep keygen`).
    #[arg(long, env = "LEEP_SIGNING_KEY", value_name = "KEYFILE")]
    pub sign: Option<PathBuf>,
}

fn author(args: &CommitArgs) -> String {
    snapshot::author(args.author.as_deref())
}

fn extra(args: &CommitArgs) -> BTreeMap<String, String> {
    agent_extra(
        &args.agent_model,
        &args.agent_session,
        &args.agent_prompt,
        &args.agent_tests,
    )
}

/// The `agent.*` keys that were given and are not empty.
pub fn agent_extra(
    model: &Option<String>,
    session: &Option<String>,
    prompt: &Option<String>,
    tests: &Option<String>,
) -> BTreeMap<String, String> {
    let mut m = BTreeMap::new();
    for (k, v) in [
        (EXTRA_AGENT_MODEL, model),
        (EXTRA_AGENT_SESSION, session),
        (EXTRA_AGENT_PROMPT, prompt),
        (EXTRA_AGENT_TESTS, tests),
    ] {
        if let Some(v) = v.as_ref().filter(|v| !v.is_empty()) {
            m.insert(k.to_string(), v.clone());
        }
    }
    m
}

pub fn run(ctx: &mut Ctx, args: &CommitArgs) -> Result<i32> {
    // Fail before uploading anything if the key cannot be used.
    let key = args
        .sign
        .as_deref()
        .filter(|p| !p.as_os_str().is_empty())
        .map(AuthorKey::load)
        .transpose()?;
    let st = status(&ctx.wc)?;
    st.refuse_conflicts()?;
    if st.is_clean() {
        return Err(CliError::user("nothing to commit, working copy clean"));
    }

    let changes = snapshot::upload_changes(ctx, &st)?;

    let cs = Changeset {
        parents: ctx.wc.state.base.into_iter().collect(),
        author: author(args),
        timestamp: snapshot::now_secs(),
        message: args.message.clone(),
        changes,
        extra: extra(args),
    };
    let id = ctx.client.create_changeset(&cs)?;
    // Before the working copy moves: if signing fails, nothing local changed
    // and a retry just makes a new changeset.
    let signed_by = match &key {
        Some(k) => Some(ctx.client.add_signature(&id, &k.sign(&id)).map_err(|e| {
            CliError::user(format!(
                "created {} but could not sign it: {e}; nothing was committed locally, retry",
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
        print_json(&leep_core::api::CreateChangesetResponse { id })?;
    } else {
        println!("{id}");
        eprintln!(
            "committed {} ({})",
            short(id),
            plural(cs.changes.len(), "file")
        );
        if let Some(s) = signed_by {
            eprintln!("signed as {} (key {})", s.name, s.key_id);
        }
    }
    Ok(0)
}
