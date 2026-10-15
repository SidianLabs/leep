//! `leep commit`: upload new blobs, then create a changeset.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use clap::Args;
use leep_core::changeset::{
    EXTRA_AGENT_MODEL, EXTRA_AGENT_PROMPT, EXTRA_AGENT_SESSION, EXTRA_AGENT_TESTS,
};
use leep_core::{Changeset, FileChange, Hash, RepoPath};

use super::{print_json, Ctx};
use crate::error::{CliError, Result};
use crate::fmt::{plural, short};
use crate::sign::AuthorKey;
use crate::status::{read_content, status, Change};

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
    let env = |k: &str| std::env::var(k).ok().filter(|v| !v.is_empty());
    args.author
        .clone()
        .or_else(|| env("LEEP_AUTHOR"))
        .or_else(|| env("USER"))
        .unwrap_or_else(|| "unknown".into())
}

fn extra(args: &CommitArgs) -> BTreeMap<String, String> {
    let mut m = BTreeMap::new();
    for (k, v) in [
        (EXTRA_AGENT_MODEL, &args.agent_model),
        (EXTRA_AGENT_SESSION, &args.agent_session),
        (EXTRA_AGENT_PROMPT, &args.agent_prompt),
        (EXTRA_AGENT_TESTS, &args.agent_tests),
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
    if st.has_conflicts() {
        return Err(CliError::user(
            "unresolved conflict markers remain; resolve them first (see `leep status`)",
        ));
    }
    if st.is_clean() {
        return Err(CliError::user("nothing to commit, working copy clean"));
    }

    let mut changes = BTreeMap::new();
    // hash -> repo path, to re-read bytes for upload.
    let mut sources: BTreeMap<Hash, String> = BTreeMap::new();
    for e in &st.entries {
        let path = RepoPath::new(e.path.as_str())?;
        match (&e.now, e.change) {
            (Some(now), Change::Added | Change::Modified) => {
                sources.entry(now.content).or_insert_with(|| e.path.clone());
                changes.insert(
                    path,
                    FileChange::Modify {
                        content: now.content,
                        kind: now.kind,
                        size: now.size,
                    },
                );
            }
            (None, Change::Deleted) => {
                changes.insert(path, FileChange::Delete);
            }
            _ => {}
        }
    }

    let hashes: Vec<Hash> = sources.keys().copied().collect();
    let missing: BTreeSet<Hash> = if hashes.is_empty() {
        BTreeSet::new()
    } else {
        ctx.client.missing_blobs(hashes)?.into_iter().collect()
    };
    for (hash, path) in &sources {
        let abs = ctx.wc.abs(path);
        let data = read_content(&abs, &fs::symlink_metadata(&abs)?)?;
        if Hash::of_blob(&data) != *hash {
            return Err(CliError::user(format!(
                "{path} changed while committing; retry"
            )));
        }
        ctx.cache.put(hash, &data)?;
        if missing.contains(hash) {
            ctx.client.put_blob(hash, data)?;
        }
    }

    let cs = Changeset {
        parents: ctx.wc.state.base.into_iter().collect(),
        author: author(args),
        timestamp: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0),
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
