//! `leep_commit_and_land`: the agent write path, with no working copy.

use std::collections::{BTreeMap, BTreeSet};
use std::time::{SystemTime, UNIX_EPOCH};

use leep_cli::client::new_land_id;
use leep_cli::error::{CliError, ErrorKind};
use leep_core::api::{error_code, Conflict, FileInfo, LandRequest, LandResponse};
use leep_core::changeset::{
    EXTRA_AGENT_MODEL, EXTRA_AGENT_PROMPT, EXTRA_AGENT_REQUEST_ID, EXTRA_AGENT_SESSION,
    EXTRA_AGENT_TESTS,
};
use leep_core::refname::MISSING_REF_MARKER;
use leep_core::{Changeset, ChangesetId, FileChange, FileKind, Hash, LandId, RepoPath};
use serde::Deserialize;
use serde_json::{json, Value};

use super::resolve::conflict_request;
use super::{as_text, parse_args, truncate, ToolError, ToolOutput, ToolResult, DEFAULT_REF};
use crate::Server;

/// Per-side cap for text shown in a conflict report.
const MAX_CONFLICT_TEXT: usize = 64 * 1024;
/// Ref log pages read, at most, to find which changeset a land id landed
/// when the server's refusal does not name it.
const RECORD_SCAN_PAGES: usize = 10;
const RECORD_SCAN_PAGE: usize = 1000;
/// Text of a `land_id_reused` refusal from servers that send no code.
const LAND_ID_REUSED_MARKER: &str = "; use a new land id";

/// The land id of `request_id` on `target_ref` (ADR 0057), namespaced by
/// the author and, when set, the agent session. Derived, not random, so
/// every MCP process of the same agent sends the same one: a retry after a
/// restart, at any age, is answered from the server's record instead of
/// landing again, while two agents that pick the same `request_id` land
/// independently.
///
/// The NUL separators cannot be forged: `author` and `session` come from
/// environment variables, which cannot hold NUL, `refname::validate` refuses
/// control characters in `target_ref`, and `request_id` goes last.
pub fn request_land_id(
    author: &str,
    session: Option<&str>,
    target_ref: &str,
    request_id: &str,
) -> LandId {
    let mut h = blake3::Hasher::new();
    h.update(b"leep-mcp-land-id\0");
    for part in [author, session.unwrap_or(""), target_ref] {
        h.update(part.as_bytes());
        h.update(b"\0");
    }
    h.update(request_id.as_bytes());
    LandId::from_bytes(&h.finalize().as_bytes()[..LandId::MIN_LEN])
        .expect("16 bytes is a valid land id")
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
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
    /// Client-chosen id of this request. A second call with the same id
    /// lands at most once: in this process it lands the changeset the first
    /// call made, and in any process the server answers the land id derived
    /// from it with the first land's result.
    request_id: Option<String>,
}

pub(super) fn non_empty(s: Option<&str>) -> Option<&str> {
    s.map(str::trim).filter(|s| !s.is_empty())
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Edit {
    pub path: String,
    content: Option<String>,
    #[serde(default)]
    delete: bool,
    /// `None` keeps the file's existing kind (executable bit, symlink), so an
    /// agent editing a script doesn't silently strip `+x`.
    pub executable: Option<bool>,
}

impl Edit {
    /// The change this edit makes, its content added to `blobs`. Without
    /// `executable` the kind is `Regular`, for the caller to replace with the
    /// file's existing kind.
    pub(super) fn change(
        &self,
        blobs: &mut BTreeMap<Hash, Vec<u8>>,
    ) -> Result<FileChange, ToolError> {
        match (&self.content, self.delete) {
            (Some(_), true) => Err(ToolError(format!(
                "{}: give either content or delete:true, not both",
                self.path
            ))),
            (None, false) => Err(ToolError(format!(
                "{}: give content (new file text) or delete:true",
                self.path
            ))),
            (None, true) => Ok(FileChange::Delete),
            (Some(text), false) => {
                let data = text.as_bytes().to_vec();
                let hash = Hash::of_blob(&data);
                let size = data.len() as u64;
                blobs.insert(hash, data);
                let kind = match self.executable {
                    Some(true) => FileKind::Executable,
                    _ => FileKind::Regular,
                };
                Ok(FileChange::Modify {
                    content: hash,
                    kind,
                    size,
                })
            }
        }
    }
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
                if a.changes.is_empty() || self.same_changes(id, &a.changes)? {
                    return self.land_existing(id, &target_ref, a.create_ref, Some(r.as_str()));
                }
                // Other changes under a known id: the remembered changeset may
                // never have landed (its land failed), so forget it and let
                // the land id decide. If it did land, the server's record
                // refuses this call as `already_landed`.
                self.requests.lock().unwrap().remove(r);
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
            let change = e.change(&mut blobs)?;
            if e.executable.is_none() && matches!(change, FileChange::Modify { .. }) {
                inherit_kind.push(path.clone());
            }
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
        self.upload(blobs)?;

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

    /// Whether `edits` make the same changes as the stored changeset `id`.
    /// One that is no longer stored counts as different.
    fn same_changes(&self, id: ChangesetId, edits: &[Edit]) -> Result<bool, ToolError> {
        let mut now = BTreeMap::new();
        let mut blobs = BTreeMap::new();
        for e in edits {
            let path = RepoPath::new(e.path.as_str())
                .map_err(|err| ToolError(format!("bad path `{}`: {err}", e.path)))?;
            now.insert(path, e.change(&mut blobs)?);
        }
        match self.client.get_changeset(&id) {
            Ok(info) => Ok(differing_paths(&info.changeset.changes, &now).is_empty()),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    /// Upload the blobs the server lacks.
    pub(super) fn upload(&self, blobs: BTreeMap<Hash, Vec<u8>>) -> Result<(), ToolError> {
        let hashes: Vec<Hash> = blobs.keys().copied().collect();
        if hashes.is_empty() {
            return Ok(());
        }
        let missing: BTreeSet<Hash> = self.client.missing_blobs(hashes)?.into_iter().collect();
        for (hash, data) in blobs {
            if missing.contains(&hash) {
                self.client.put_blob(&hash, data)?;
            }
        }
        Ok(())
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

    /// Land `created` and report the outcome. A conflict forgets the
    /// changeset made for `request_id`, since the retry that resolves it
    /// carries new content, and is remembered for `leep_resolve_and_land`.
    pub(super) fn land_and_report(
        &self,
        created: ChangesetId,
        target_ref: &str,
        create: bool,
        files_changed: usize,
        request_id: Option<&str>,
    ) -> ToolResult {
        let fixed = request_id.map(|r| (r, self.request_land_id(target_ref, r)));
        let (landed_cs, resp) = self.land_creating(created, target_ref, create, fixed)?;
        match resp {
            LandResponse::Landed {
                landed,
                rebased,
                attempts,
            } => {
                let mut v = json!({
                    "status": "landed",
                    "landed": landed.to_string(),
                    "changeset": landed_cs.to_string(),
                    "target_ref": target_ref,
                    "rebased": rebased,
                    "attempts": attempts,
                    "files_changed": files_changed,
                });
                if landed_cs == created {
                    return Ok(ToolOutput::json(v));
                }
                // The request landed before, from another call or process.
                let r = request_id.unwrap_or_default();
                self.requests
                    .lock()
                    .unwrap()
                    .insert(r.to_string(), (landed_cs, target_ref.to_string()));
                v["from_record"] = json!(true);
                let earlier = match self.client.get_changeset(&landed_cs) {
                    Ok(info) => info.changeset.changes,
                    // Reclaimed since it landed (ADR 0059): the record still
                    // stands, there is just nothing left to compare.
                    Err(e) if e.kind() == ErrorKind::NotFound => {
                        v["note"] = json!(format!(
                            "request_id `{r}` had already landed changeset {landed_cs} on \
                             {target_ref}; it was not landed again. That changeset is no longer \
                             stored, so this call's changes were not compared with it."
                        ));
                        return Ok(ToolOutput::json(v));
                    }
                    Err(e) => return Err(e.into()),
                };
                let now = self.client.get_changeset(&created)?.changeset.changes;
                v["files_changed"] = json!(earlier.len());
                let differ = differing_paths(&earlier, &now);
                if differ.is_empty() {
                    v["note"] = json!(format!(
                        "request_id `{r}` had already landed changeset {landed_cs} on \
                         {target_ref}; it was not landed again."
                    ));
                    return Ok(ToolOutput::json(v));
                }
                v["status"] = json!("already_landed");
                v["differing_paths"] = json!(differ);
                v["note"] = json!(format!(
                    "request_id `{r}` already landed changeset {landed_cs} on {target_ref}, \
                     whose changes differ from this call's at `differing_paths`. Nothing new \
                     landed. If this is a different change, call again with a new request_id."
                ));
                let mut out = ToolOutput::json(v);
                out.is_error = true;
                Ok(out)
            }
            LandResponse::Conflicts {
                onto,
                conflicts,
                clean,
            } => {
                if let Some(r) = request_id {
                    self.requests.lock().unwrap().remove(r);
                }
                let request = request_id
                    .map(str::to_string)
                    .unwrap_or_else(|| conflict_request(&created, &onto));
                let id = self.remember_conflict(
                    target_ref, created, onto, &conflicts, &clean, &request, create,
                );
                self.conflict_report(target_ref, created, onto, &id, &conflicts, &clean)
            }
        }
    }

    /// [`request_land_id`] for this server's author and agent session.
    pub(super) fn request_land_id(&self, target_ref: &str, request_id: &str) -> LandId {
        request_land_id(
            &self.cfg.author,
            self.cfg.agent_session.as_deref().filter(|s| !s.is_empty()),
            target_ref,
            request_id,
        )
    }

    /// Kind of `path` at `parent`, if the file exists there.
    pub(super) fn existing_kind(
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
    /// `main` of an empty repository, like `leep land` (ADR 0036). `fixed`
    /// is a land id derived from a request id; without one, the land gets a
    /// random id kept until it is answered. Answers the changeset that
    /// landed: a land id the ref already recorded for another changeset
    /// (the same request, landed by an earlier call) is answered for that
    /// one.
    fn land_creating(
        &self,
        changeset: ChangesetId,
        target_ref: &str,
        create: bool,
        fixed: Option<(&str, LandId)>,
    ) -> Result<(ChangesetId, LandResponse), ToolError> {
        let key = (changeset, target_ref.to_string());
        let land_id = match &fixed {
            Some((_, id)) => id.clone(),
            None => self
                .pending_lands
                .lock()
                .unwrap()
                .entry(key.clone())
                .or_insert_with(new_land_id)
                .clone(),
        };
        let send = |cs: ChangesetId, create: bool| {
            self.client.land_resending(&LandRequest {
                changeset: cs,
                target_ref: target_ref.to_string(),
                create: Some(create),
                land_id: Some(land_id.clone()),
            })
        };
        // An error names the changeset last sent, the one a retry must land.
        let land = |create: bool| {
            let mut sent = changeset;
            let mut r = send(changeset, create);
            if let Err(e) = &r {
                let request = fixed.as_ref().map(|(r, _)| *r);
                if let (Some(request), true) = (
                    request,
                    e.is_server(error_code::LAND_ID_REUSED, LAND_ID_REUSED_MARKER),
                ) {
                    match self.recorded_changeset(e, target_ref, &land_id) {
                        Ok(Some(earlier)) => {
                            sent = earlier;
                            r = send(earlier, create);
                        }
                        Ok(None) => {
                            r = Err(CliError::user(format!(
                                "request_id `{request}` already landed something on \
                                 `{target_ref}` (land id {land_id}), and it was not landed \
                                 again; see leep_reflog for what. Use a new request_id for a \
                                 new change."
                            )))
                        }
                        Err(e) => r = Err(e),
                    }
                }
            }
            // Only an error that sending again could change keeps the id.
            if !r.as_ref().is_err_and(|e| e.is_retryable()) {
                self.pending_lands.lock().unwrap().remove(&key);
            }
            r.map(|resp| (sent, resp)).map_err(|e| (sent, Box::new(e)))
        };
        let fail = |(sent, e): (ChangesetId, Box<CliError>)| land_error(*e, sent);
        match land(create) {
            Err((_, e)) if missing_ref(&e) => {
                if target_ref == DEFAULT_REF {
                    let refs = self.client.refs()?.refs;
                    if refs.is_empty() {
                        return land(true).map_err(fail);
                    }
                    // Another client's first land got there in between.
                    if refs.contains_key(DEFAULT_REF) {
                        return land(false).map_err(fail);
                    }
                }
                Err(ToolError(format!(
                    "ref `{target_ref}` does not exist and nothing was landed. Check the name \
                     with leep_refs, or call again with create_ref:true to create it."
                )))
            }
            other => other.map_err(fail),
        }
    }

    /// The changeset `target_ref` recorded for `land_id`: named by the
    /// server's refusal, or else found in the ref's log.
    fn recorded_changeset(
        &self,
        refusal: &CliError,
        target_ref: &str,
        land_id: &LandId,
    ) -> Result<Option<ChangesetId>, CliError> {
        let named = refused_changeset(refusal, land_id);
        if named.is_some() {
            return Ok(named);
        }
        let mut after: Option<String> = None;
        for _ in 0..RECORD_SCAN_PAGES {
            let page = self.client.ref_log_page(
                Some(target_ref),
                None,
                after.as_deref(),
                RECORD_SCAN_PAGE,
            )?;
            if let Some(e) = page
                .entries
                .iter()
                .find(|e| e.land_id.as_ref() == Some(land_id))
            {
                return Ok(e.land_changeset);
            }
            match page.next {
                Some(n) => after = Some(n),
                None => break,
            }
        }
        Ok(None)
    }

    fn conflict_report(
        &self,
        target_ref: &str,
        created: ChangesetId,
        onto: ChangesetId,
        conflict_id: &str,
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
             correct final content for each conflicting path, then call leep_resolve_and_land \
             with conflict_id \"{conflict_id}\" and `resolutions` holding ONLY the conflicting \
             paths (full resolved content, or delete:true). Your changes to the paths in \
             `clean_paths` are kept for you; do not resend them. If leep_resolve_and_land says \
             the conflict is not remembered (the MCP server restarted), call \
             leep_commit_and_land again with base set to {onto} and ALL files you meant to \
             change, keeping {target_ref}'s edits to any of them."
        );
        let v = json!({
            "status": "conflict",
            "conflict_id": conflict_id,
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

/// The changeset a `land_id_reused` refusal of `land_id` names: its `land`
/// record, or, from servers older than that field, its text.
fn refused_changeset(refusal: &CliError, land_id: &LandId) -> Option<ChangesetId> {
    if let Some(land) = refusal.land_refusal().filter(|l| &l.land_id == land_id) {
        return Some(land.changeset);
    }
    refusal
        .message()
        .split_once("already landed ")
        .and_then(|(_, rest)| rest.get(..64))
        .and_then(|hex| hex.parse::<ChangesetId>().ok())
}

/// Paths whose change differs between two change maps (content or delete;
/// the kind is ignored, since a retry may infer it differently).
fn differing_paths(
    a: &BTreeMap<RepoPath, FileChange>,
    b: &BTreeMap<RepoPath, FileChange>,
) -> Vec<String> {
    let content = |c: Option<&FileChange>| match c {
        None => None,
        Some(FileChange::Delete) => Some(None),
        Some(FileChange::Modify { content, .. }) => Some(Some(*content)),
    };
    let paths: BTreeSet<&RepoPath> = a.keys().chain(b.keys()).collect();
    paths
        .into_iter()
        .filter(|p| content(a.get(*p)) != content(b.get(*p)))
        .map(|p| p.to_string())
        .collect()
}

/// A failed land, with how to finish it safely when it may have applied: the
/// server's timeout can cancel a land after it moved the ref.
fn land_error(e: CliError, changeset: ChangesetId) -> ToolError {
    // Not admitted (ADR 0060): nothing was queued, so the land certainly
    // did not apply; the agent calls again after `retry_after`.
    if e.refused().is_some() {
        return e.into();
    }
    match e.kind() {
        ErrorKind::Timeout | ErrorKind::Unreachable | ErrorKind::Server => ToolError(format!(
            "{}. The land of changeset {changeset} may have applied. Call \
             leep_commit_and_land again with changeset: \"{changeset}\" (or the same \
             request_id) to finish it; a changeset that already landed is not landed twice.",
            super::describe(&e)
        )),
        _ => e.into(),
    }
}

/// A land refused because the ref does not exist and may not be created.
fn missing_ref(e: &CliError) -> bool {
    e.kind() == ErrorKind::NotFound && e.is_server(error_code::REF_MISSING, MISSING_REF_MARKER)
}

#[cfg(test)]
mod tests {
    use leep_core::api::LandRefusal;

    use super::*;

    #[test]
    fn unknown_arguments_are_refused() {
        let e = parse_args::<Args>(json!({"message":"m","changes":[],"target-ref":"x"}))
            .err()
            .expect("unknown field");
        assert!(
            e.0.starts_with("invalid arguments: unknown field `target-ref`"),
            "{}",
            e.0
        );
        let e = parse_args::<Args>(json!({
            "message": "m",
            "changes": [{ "path": "a", "content": "x", "exec": true }],
        }))
        .err()
        .expect("unknown field in an edit");
        assert!(e.0.contains("unknown field `exec`"), "{}", e.0);
        parse_args::<Args>(json!({"message":"m","changes":[{"path":"a","delete":true}]}))
            .unwrap_or_else(|e| panic!("{}", e.0));
    }

    fn cs(c: char) -> ChangesetId {
        c.to_string().repeat(64).parse().unwrap()
    }

    fn reused_text(id: &LandId, earlier: ChangesetId, requested: ChangesetId) -> String {
        format!(
            "invalid input: land id {id} already landed {earlier}; use a new land id to land \
             {requested}"
        )
    }

    #[test]
    fn a_reused_land_id_is_read_from_the_record() {
        let id = request_land_id("bob", None, "main", "r1");
        // The record wins over text that names another changeset.
        let e = CliError::user(reused_text(&id, cs('c'), cs('d')))
            .with_server_code(Some(error_code::LAND_ID_REUSED.into()))
            .with_land_refusal(Some(Box::new(LandRefusal {
                land_id: id.clone(),
                changeset: cs('a'),
                landed: Some(cs('b')),
            })));
        assert_eq!(refused_changeset(&e, &id), Some(cs('a')));
        // A reworded message does not matter when the record is there.
        let e = CliError::user("this land id was used before").with_land_refusal(Some(Box::new(
            LandRefusal {
                land_id: id.clone(),
                changeset: cs('a'),
                landed: None,
            },
        )));
        assert_eq!(refused_changeset(&e, &id), Some(cs('a')));
    }

    #[test]
    fn a_reused_land_id_falls_back_to_the_text_of_an_older_server() {
        let id = request_land_id("bob", None, "main", "r1");
        let e = CliError::user(reused_text(&id, cs('a'), cs('d')));
        assert!(e.land_refusal().is_none());
        assert_eq!(refused_changeset(&e, &id), Some(cs('a')));
        // A record for another land id is not this land's: use the text.
        let other = request_land_id("bob", None, "main", "r2");
        let e = e.with_land_refusal(Some(Box::new(LandRefusal {
            land_id: other,
            changeset: cs('b'),
            landed: None,
        })));
        assert_eq!(refused_changeset(&e, &id), Some(cs('a')));
        // Neither record nor text: the caller scans the ref log.
        let e = CliError::user("this land id was used before; use a new land id");
        assert_eq!(refused_changeset(&e, &id), None);
    }
}
