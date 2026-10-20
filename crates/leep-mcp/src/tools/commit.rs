//! `leep_commit_and_land`: the agent write path, with no working copy.

use std::collections::{BTreeMap, BTreeSet};
use std::time::{SystemTime, UNIX_EPOCH};

use leep_cli::client::new_land_id;
use leep_cli::error::{CliError, ErrorKind};
use leep_core::api::{Conflict, FileInfo, LandRequest, LandResponse};
use leep_core::changeset::{
    EXTRA_AGENT_MODEL, EXTRA_AGENT_PROMPT, EXTRA_AGENT_REQUEST_ID, EXTRA_AGENT_SESSION,
    EXTRA_AGENT_TESTS,
};
use leep_core::refname::MISSING_REF_MARKER;
use leep_core::{Changeset, ChangesetId, FileChange, FileKind, Hash, RepoPath};
use serde::Deserialize;
use serde_json::{json, Value};

use super::{as_text, parse_args, truncate, ToolError, ToolOutput, ToolResult, DEFAULT_REF};
use crate::Server;

/// Per-side cap for text shown in a conflict report.
const MAX_CONFLICT_TEXT: usize = 64 * 1024;

#[derive(Deserialize)]
struct Args {
    #[serde(default)]
    message: String,
    #[serde(default)]
    changes: Vec<Edit>,
    base: Option<String>,
    target_ref: Option<String>,
    #[serde(default)]
    create_ref: bool,
    prompt: Option<String>,
    tests: Option<String>,
    /// Land this existing changeset instead of making one: the retry of a
    /// call whose answer was lost.
    changeset: Option<String>,
    /// Client-chosen id of this request. A second call with the same id in
    /// this process lands the changeset the first one made.
    request_id: Option<String>,
}

fn non_empty(s: Option<&str>) -> Option<&str> {
    s.map(str::trim).filter(|s| !s.is_empty())
}

#[derive(Deserialize)]
struct Edit {
    path: String,
    content: Option<String>,
    #[serde(default)]
    delete: bool,
    /// `None` keeps the file's existing kind (executable bit, symlink), so an
    /// agent editing a script doesn't silently strip `+x`.
    executable: Option<bool>,
}

impl Server {
    pub(super) fn commit_and_land(&self, args: Value) -> ToolResult {
        let a: Args = parse_args(args)?;
        let target_ref = a
            .target_ref
            .as_deref()
            .map(str::trim)
            .filter(|r| !r.is_empty())
            .unwrap_or("main")
            .to_string();
        if let Some(rev) = non_empty(a.changeset.as_deref()) {
            if !a.changes.is_empty() {
                return Err(ToolError::new(
                    "give either changeset (to land one that exists) or changes, not both",
                ));
            }
            let id = self.resolve_rev(Some(rev))?;
            return self.land_existing(id, &target_ref, a.create_ref, None);
        }
        let request_id = non_empty(a.request_id.as_deref()).map(str::to_string);
        if let Some(r) = &request_id {
            let known = self.requests.lock().unwrap().get(r).cloned();
            if let Some((id, onto)) = known {
                if onto != target_ref {
                    return Err(ToolError(format!(
                        "request_id `{r}` was used for changeset {id} on `{onto}`; use a new \
                         request_id for a different request"
                    )));
                }
                return self.land_existing(id, &target_ref, a.create_ref, Some(r.as_str()));
            }
        }
        if a.message.trim().is_empty() {
            return Err(ToolError::new("message must not be empty"));
        }
        if a.changes.is_empty() {
            return Err(ToolError::new("changes must contain at least one entry"));
        }

        // Validate everything before touching the server.
        let mut inherit_kind: Vec<RepoPath> = Vec::new();
        let mut changes: BTreeMap<RepoPath, FileChange> = BTreeMap::new();
        let mut blobs: BTreeMap<Hash, Vec<u8>> = BTreeMap::new();
        for e in &a.changes {
            let path = RepoPath::new(e.path.as_str())
                .map_err(|err| ToolError(format!("bad path `{}`: {err}", e.path)))?;
            let change = match (&e.content, e.delete) {
                (Some(_), true) => {
                    return Err(ToolError(format!(
                        "{}: give either content or delete:true, not both",
                        e.path
                    )))
                }
                (None, false) => {
                    return Err(ToolError(format!(
                        "{}: give content (new file text) or delete:true",
                        e.path
                    )))
                }
                (None, true) => FileChange::Delete,
                (Some(text), false) => {
                    let data = text.as_bytes().to_vec();
                    let hash = Hash::of_blob(&data);
                    let size = data.len() as u64;
                    blobs.insert(hash, data);
                    let kind = match e.executable {
                        Some(true) => FileKind::Executable,
                        Some(false) => FileKind::Regular,
                        None => {
                            inherit_kind.push(path.clone());
                            FileKind::Regular
                        }
                    };
                    FileChange::Modify {
                        content: hash,
                        kind,
                        size,
                    }
                }
            };
            if changes.insert(path, change).is_some() {
                return Err(ToolError(format!("{}: listed more than once", e.path)));
            }
        }

        let parent: Option<ChangesetId> = match a.base.as_deref().filter(|b| !b.trim().is_empty()) {
            Some(b) => Some(self.resolve_rev(Some(b))?),
            // An absent target ref is an empty repository: this becomes a root.
            None => self.client.get_ref(&target_ref)?.target,
        };
        for path in inherit_kind {
            if let Some(kind) = self.existing_kind(parent, &path)? {
                if let Some(FileChange::Modify { kind: k, .. }) = changes.get_mut(&path) {
                    *k = kind;
                }
            }
        }
        let hashes: Vec<Hash> = blobs.keys().copied().collect();
        let missing: BTreeSet<Hash> = if hashes.is_empty() {
            BTreeSet::new()
        } else {
            self.client.missing_blobs(hashes)?.into_iter().collect()
        };
        for (hash, data) in blobs {
            if missing.contains(&hash) {
                self.client.put_blob(&hash, data)?;
            }
        }

        let mut extra = BTreeMap::new();
        for (k, arg, default) in [
            (EXTRA_AGENT_MODEL, None, &self.cfg.agent_model),
            (EXTRA_AGENT_SESSION, None, &self.cfg.agent_session),
            (
                EXTRA_AGENT_PROMPT,
                a.prompt.as_ref(),
                &self.cfg.agent_prompt,
            ),
            (EXTRA_AGENT_TESTS, a.tests.as_ref(), &self.cfg.agent_tests),
        ] {
            if let Some(v) = arg.or(default.as_ref()).filter(|v| !v.is_empty()) {
                extra.insert(k.to_string(), v.clone());
            }
        }
        if let Some(r) = &request_id {
            extra.insert(EXTRA_AGENT_REQUEST_ID.to_string(), r.clone());
        }
        let cs = Changeset {
            parents: parent.into_iter().collect(),
            author: self.cfg.author.clone(),
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0),
            message: a.message,
            changes,
            extra,
        };
        let created = self.client.create_changeset(&cs)?;
        if let Some(r) = &request_id {
            self.requests
                .lock()
                .unwrap()
                .insert(r.clone(), (created, target_ref.clone()));
        }
        let files = cs.changes.len();
        self.land_and_report(
            created,
            &target_ref,
            a.create_ref,
            files,
            request_id.as_deref(),
        )
    }

    /// Land a changeset that already exists. Safe to repeat: one already on
    /// the ref, or landed there as a rebased copy, is reported as landed
    /// rather than added again.
    fn land_existing(
        &self,
        id: ChangesetId,
        target_ref: &str,
        create: bool,
        request_id: Option<&str>,
    ) -> ToolResult {
        let files = self.client.get_changeset(&id)?.changeset.changes.len();
        self.land_and_report(id, target_ref, create, files, request_id)
    }

    /// Land `created` and report the outcome. A conflict forgets
    /// `request_id`, since the retry that resolves it carries new content.
    fn land_and_report(
        &self,
        created: ChangesetId,
        target_ref: &str,
        create: bool,
        files_changed: usize,
        request_id: Option<&str>,
    ) -> ToolResult {
        let resp = self.land_creating(created, target_ref, create)?;
        match resp {
            LandResponse::Landed {
                landed,
                rebased,
                attempts,
            } => Ok(ToolOutput::json(json!({
                "status": "landed",
                "landed": landed.to_string(),
                "changeset": created.to_string(),
                "target_ref": target_ref,
                "rebased": rebased,
                "attempts": attempts,
                "files_changed": files_changed,
            }))),
            LandResponse::Conflicts {
                onto,
                conflicts,
                clean,
            } => {
                if let Some(r) = request_id {
                    self.requests.lock().unwrap().remove(r);
                }
                self.conflict_report(target_ref, created, onto, &conflicts, &clean)
            }
        }
    }

    /// Kind of `path` at `parent`, if the file exists there.
    fn existing_kind(
        &self,
        parent: Option<ChangesetId>,
        path: &RepoPath,
    ) -> Result<Option<FileKind>, ToolError> {
        let Some(parent) = parent else {
            return Ok(None);
        };
        Ok(self
            .client
            .files(&parent, path.as_str())?
            .files
            .into_iter()
            .find(|f| &f.path == path)
            .map(|f| f.kind))
    }

    /// Land, creating the ref only when asked to or on the first land onto
    /// `main` of an empty repository, like `leep land` (ADR 0036).
    fn land_creating(
        &self,
        changeset: ChangesetId,
        target_ref: &str,
        create: bool,
    ) -> Result<LandResponse, ToolError> {
        let key = (changeset, target_ref.to_string());
        let land_id = self
            .pending_lands
            .lock()
            .unwrap()
            .entry(key.clone())
            .or_insert_with(new_land_id)
            .clone();
        let land = |create: bool| {
            let r = self.client.land_resending(&LandRequest {
                changeset,
                target_ref: target_ref.to_string(),
                create: Some(create),
                land_id: Some(land_id.clone()),
            });
            // Only an error that sending again could change keeps the id.
            if !r.as_ref().is_err_and(|e| e.is_retryable()) {
                self.pending_lands.lock().unwrap().remove(&key);
            }
            r
        };
        match land(create) {
            Err(e) if missing_ref(&e) => {
                if target_ref == DEFAULT_REF {
                    let refs = self.client.refs()?.refs;
                    if refs.is_empty() {
                        return land(true).map_err(|e| land_error(e, changeset));
                    }
                    // Another client's first land got there in between.
                    if refs.contains_key(DEFAULT_REF) {
                        return land(false).map_err(|e| land_error(e, changeset));
                    }
                }
                Err(ToolError(format!(
                    "ref `{target_ref}` does not exist and nothing was landed. Check the name \
                     with leep_refs, or call again with create_ref:true to create it."
                )))
            }
            other => other.map_err(|e| land_error(e, changeset)),
        }
    }

    fn conflict_report(
        &self,
        target_ref: &str,
        created: ChangesetId,
        onto: ChangesetId,
        conflicts: &[Conflict],
        clean: &BTreeMap<RepoPath, FileChange>,
    ) -> ToolResult {
        let mut items = Vec::new();
        for c in conflicts {
            items.push(json!({
                "path": c.path.to_string(),
                "base": self.side_view(c.base.as_ref())?,
                "ours": self.side_view(c.ours.as_ref())?,
                "theirs": self.side_view(c.theirs.as_ref())?,
                "merged_with_markers": c.merged_with_markers,
            }));
        }
        let clean_paths: Vec<Value> = clean
            .iter()
            .map(|(p, ch)| {
                json!({
                    "path": p.to_string(),
                    "change": if matches!(ch, FileChange::Delete) { "delete" } else { "modify" },
                })
            })
            .collect();
        let guidance = format!(
            "Nothing was landed: {target_ref} is unchanged. `ours` is what {target_ref} now has \
             (at {onto}), `theirs` is your change, `base` is the common ancestor. Decide the \
             correct final content for each conflicting path, then call leep_commit_and_land again \
             with base set to {onto} and include ALL files you meant to change: the resolved \
             content for the conflicting paths, and your original content (or delete:true) for the \
             clean paths listed in `clean_paths`."
        );
        let v = json!({
            "status": "conflict",
            "target_ref": target_ref,
            "onto": onto.to_string(),
            "changeset": created.to_string(),
            "conflicts": items,
            "clean_paths": clean_paths,
            "guidance": guidance,
        });
        let mut out = ToolOutput::json(v);
        out.is_error = true;
        out.text = format!("NOT LANDED: conflicts with {target_ref}.\n{}", out.text);
        Ok(out)
    }

    /// One side of a conflict as text (capped) or a binary marker.
    fn side_view(&self, f: Option<&FileInfo>) -> Result<Value, ToolError> {
        let Some(f) = f else {
            return Ok(Value::Null);
        };
        let bytes = self.blob(&f.content)?;
        Ok(match as_text(&bytes) {
            None => json!({ "binary": true, "size": f.size }),
            Some(t) => {
                let (shown, cut) = truncate(t, MAX_CONFLICT_TEXT);
                json!({ "text": shown, "size": f.size, "truncated": cut })
            }
        })
    }
}

/// A failed land, with how to finish it safely when it may have applied: the
/// server's timeout can cancel a land after it moved the ref.
fn land_error(e: CliError, changeset: ChangesetId) -> ToolError {
    match e.kind() {
        ErrorKind::Timeout | ErrorKind::Unreachable | ErrorKind::Server => ToolError(format!(
            "{e}. The land of changeset {changeset} may have applied. Call \
             leep_commit_and_land again with changeset: \"{changeset}\" (or the same \
             request_id) to finish it; a changeset that already landed is not landed twice."
        )),
        _ => e.into(),
    }
}

/// A land refused because the ref does not exist and may not be created.
fn missing_ref(e: &CliError) -> bool {
    e.kind() == ErrorKind::NotFound && e.message().contains(MISSING_REF_MARKER)
}
