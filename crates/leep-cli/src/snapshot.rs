//! Turning uncommitted changes into a changeset: shared by `commit`, `amend`,
//! `sync` and `stash`.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};

use leep_core::changeset::EXTRA_SNAPSHOT;
use leep_core::{Changeset, ChangesetId, FileChange, Hash, RepoPath};

use crate::commands::Ctx;
use crate::error::{CliError, Result};
use crate::status::{read_content, Change, Status};

/// Unix seconds now.
pub fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// `--author`, else `$LEEP_AUTHOR`, else the login name, else what the server
/// says for our token (its `author`, else its name). Containers and CI often
/// set no login name, and the token is still a real identity there.
pub fn author(ctx: &Ctx, flag: Option<&str>) -> String {
    local_author(flag)
        .or_else(|| {
            let w = ctx.client.whoami().ok()?;
            w.author.or(w.name)
        })
        .unwrap_or_else(|| "unknown".into())
}

/// The author named without asking the server. `USERNAME` is Windows' name
/// for `USER`, and `LOGNAME` is set by login(1) where `USER` may not be.
fn local_author(flag: Option<&str>) -> Option<String> {
    let env = |k: &str| {
        std::env::var(k)
            .ok()
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())
    };
    flag.map(str::to_string)
        .or_else(|| env("LEEP_AUTHOR"))
        .or_else(|| env("USER"))
        .or_else(|| env("LOGNAME"))
        .or_else(|| env("USERNAME"))
}

/// The changes `st` describes, relative to the base, with their blobs
/// uploaded. Conflicted entries are skipped: callers refuse those first.
pub fn upload_changes(ctx: &Ctx, st: &Status) -> Result<BTreeMap<RepoPath, FileChange>> {
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
    Ok(changes)
}

/// Store the working copy's uncommitted changes as a changeset on top of the
/// base, marked `leep.snapshot = kind`. No ref points at it and the working
/// copy is not touched.
pub fn create(ctx: &Ctx, st: &Status, message: String, kind: &str) -> Result<ChangesetId> {
    let changes = upload_changes(ctx, st)?;
    let cs = Changeset {
        parents: ctx.wc.state.base.into_iter().collect(),
        author: author(ctx, None),
        timestamp: now_secs(),
        message,
        changes,
        extra: BTreeMap::from([(EXTRA_SNAPSHOT.to_string(), kind.to_string())]),
    };
    ctx.client.create_changeset(&cs)
}
