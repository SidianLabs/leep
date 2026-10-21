//! `leep_resolve_and_land`: finish a conflicted `leep_commit_and_land` by
//! sending only the conflicting files. The rest of the change is the `clean`
//! map the server returned with the conflict, whose blobs it already holds.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::time::{SystemTime, UNIX_EPOCH};

use leep_core::api::{Authorship, Conflict, MergeRequest, MAX_MISSING_BLOBS};
use leep_core::changeset::EXTRA_AGENT_REQUEST_ID;
use leep_core::{Changeset, ChangesetId, FileChange, FileKind, Hash, RepoPath};
use serde::Deserialize;
use serde_json::Value;

use super::commit::{non_empty, Edit};
use super::{parse_args, ToolError, ToolResult};
use crate::Server;

use leep_core::api::FEATURE_LAND_CLEAN_BLOBS;

/// Conflicts remembered per MCP process; the oldest is forgotten first.
const MAX_REMEMBERED: usize = 64;

/// What a conflict report left to finish: everything but the conflicting
/// paths' content.
#[derive(Clone)]
pub(crate) struct PendingConflict {
    pub id: String,
    pub target_ref: String,
    pub onto: ChangesetId,
    /// The changeset that conflicted: its message and `extra` carry over.
    pub changeset: ChangesetId,
    /// Changes relative to `onto` for every path that merged cleanly.
    pub clean: BTreeMap<RepoPath, FileChange>,
    /// Each conflicting path, with the kind the agent's side had (else the
    /// ref's), kept when a resolution does not set `executable`.
    pub conflicts: BTreeMap<RepoPath, Option<FileKind>>,
    /// The request the land belongs to; it keeps naming the land, so a
    /// resolution lands at most once (ADR 0057).
    pub request_id: String,
    pub create: bool,
}

#[derive(Default)]
pub(crate) struct Conflicts(VecDeque<PendingConflict>);

impl Conflicts {
    /// Remember `c`, replacing an earlier conflict of the same request.
    fn remember(&mut self, c: PendingConflict) {
        self.0
            .retain(|p| p.id != c.id && p.request_id != c.request_id);
        self.0.push_back(c);
        while self.0.len() > MAX_REMEMBERED {
            self.0.pop_front();
        }
    }

    fn forget(&mut self, id: &str) {
        self.0.retain(|p| p.id != id);
    }
}

/// A stable name for one conflict: the same changeset conflicting at the
/// same head gets the same id, in any process.
pub(crate) fn conflict_id(changeset: &ChangesetId, onto: &ChangesetId) -> String {
    let mut h = blake3::Hasher::new();
    h.update(b"leep-mcp-conflict\0");
    h.update(changeset.to_string().as_bytes());
    h.update(b"\0");
    h.update(onto.to_string().as_bytes());
    hex(&h.finalize().as_bytes()[..8])
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

/// The request key a land without a `request_id` is named by once it
/// conflicted, so resolving it twice still lands once.
pub(crate) fn conflict_request(changeset: &ChangesetId, onto: &ChangesetId) -> String {
    format!("conflict:{}", conflict_id(changeset, onto))
}

#[derive(Deserialize)]
struct Args {
    conflict_id: Option<String>,
    onto: Option<String>,
    #[serde(default)]
    resolutions: Vec<Edit>,
    message: Option<String>,
}

impl Server {
    /// Remember a conflict report so `leep_resolve_and_land` can finish it.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn remember_conflict(
        &self,
        target_ref: &str,
        changeset: ChangesetId,
        onto: ChangesetId,
        conflicts: &[Conflict],
        clean: &BTreeMap<RepoPath, FileChange>,
        request_id: &str,
        create: bool,
    ) -> String {
        let id = conflict_id(&changeset, &onto);
        self.conflicts.lock().unwrap().remember(PendingConflict {
            id: id.clone(),
            target_ref: target_ref.to_string(),
            onto,
            changeset,
            clean: clean.clone(),
            conflicts: conflicts
                .iter()
                .map(|c| {
                    let kind = c.theirs.as_ref().or(c.ours.as_ref()).map(|f| f.kind);
                    (c.path.clone(), kind)
                })
                .collect(),
            request_id: request_id.to_string(),
            create,
        });
        id
    }

    fn find_conflict(&self, a: &Args) -> Result<PendingConflict, ToolError> {
        let gone = |what: String| {
            ToolError(format!(
                "{what} is not remembered by this MCP server: it may have restarted, or the \
                 conflict was resolved or replaced by a newer one. Nothing was landed. Call \
                 leep_commit_and_land again with base set to the conflict's `onto` id and ALL \
                 files you meant to change: the resolved content for the conflicting paths, and \
                 for the other paths the content they should end up with (where `onto` changed \
                 one too, read it there and keep its edits). Keep the same request_id if you had \
                 one."
            ))
        };
        let remembered = self.conflicts.lock().unwrap();
        if let Some(id) = non_empty(a.conflict_id.as_deref()) {
            return remembered
                .0
                .iter()
                .find(|p| p.id == id)
                .cloned()
                .ok_or_else(|| gone(format!("conflict `{id}`")));
        }
        let Some(onto) = non_empty(a.onto.as_deref()) else {
            return Err(ToolError::new(
                "give conflict_id (from the conflict report) or onto",
            ));
        };
        let onto = onto.to_ascii_lowercase();
        let found: Vec<&PendingConflict> = remembered
            .0
            .iter()
            .filter(|p| onto.len() >= 4 && p.onto.to_string().starts_with(&onto))
            .collect();
        match found.as_slice() {
            [] => Err(gone(format!("a conflict onto `{onto}`"))),
            [one] => Ok((*one).clone()),
            many => Err(ToolError(format!(
                "several remembered conflicts are onto `{onto}`; give conflict_id, one of: {}",
                many.iter()
                    .map(|p| p.id.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            ))),
        }
    }

    /// Make sure the server holds every blob the clean changes name. A land
    /// computes its conflict report in a staged replay that stores what it
    /// wrote only when the ref moves, so the merged text of a path both
    /// sides changed is not stored when the land conflicts. A merge of the
    /// same two changesets is deterministic and stores those blobs. A server
    /// that advertises [`FEATURE_LAND_CLEAN_BLOBS`] stores them itself, and
    /// its `clean` is trusted as it is.
    fn ensure_clean_blobs(&self, p: &PendingConflict) -> Result<(), ToolError> {
        // A server that cannot say is treated as one without the feature:
        // the check below is only extra requests.
        let features = self.client.server_features().unwrap_or_default();
        if features.iter().any(|f| f == FEATURE_LAND_CLEAN_BLOBS) {
            return Ok(());
        }
        let hashes: Vec<Hash> = p
            .clean
            .values()
            .filter_map(|c| match c {
                FileChange::Modify { content, .. } => Some(*content),
                FileChange::Delete => None,
            })
            .collect();
        let missing = self.missing(hashes)?;
        if missing.is_empty() {
            return Ok(());
        }
        // Its answer is not used: only the blobs it stores on the way. Should
        // it merge cleanly after all, it stores a merge changeset that no
        // ref names: an orphan that changeset GC (ADR 0059) may reclaim.
        self.client.merge(&MergeRequest {
            parents: [p.onto, p.changeset],
            message: "leep-mcp: store merged files of a conflict".into(),
            authorship: Authorship {
                author: self.cfg.author.clone(),
                timestamp: None,
            },
            extra: BTreeMap::new(),
        })?;
        let still: BTreeSet<Hash> = self.missing(missing)?.into_iter().collect();
        if still.is_empty() {
            return Ok(());
        }
        let paths: Vec<String> = p
            .clean
            .iter()
            .filter(
                |(_, c)| matches!(c, FileChange::Modify { content, .. } if still.contains(content)),
            )
            .map(|(path, _)| path.to_string())
            .collect();
        Err(ToolError(format!(
            "nothing was landed: the server does not hold the merged content of {}. Call \
             leep_commit_and_land again with base set to {} and ALL files you meant to change.",
            paths.join(", "),
            p.onto
        )))
    }

    fn missing(&self, hashes: Vec<Hash>) -> Result<Vec<Hash>, ToolError> {
        let mut out = Vec::new();
        for chunk in hashes.chunks(MAX_MISSING_BLOBS) {
            out.extend(self.client.missing_blobs(chunk.to_vec())?);
        }
        Ok(out)
    }

    pub(super) fn resolve_and_land(&self, args: Value) -> ToolResult {
        let a: Args = parse_args(args)?;
        let pending = self.find_conflict(&a)?;
        if a.resolutions.is_empty() {
            return Err(ToolError::new(
                "resolutions must give the content (or delete:true) of each conflicting path",
            ));
        }
        let mut changes = pending.clean.clone();
        let mut blobs: BTreeMap<Hash, Vec<u8>> = BTreeMap::new();
        let mut seen = BTreeSet::new();
        for e in &a.resolutions {
            let path = RepoPath::new(e.path.as_str())
                .map_err(|err| ToolError(format!("bad path `{}`: {err}", e.path)))?;
            if !seen.insert(path.clone()) {
                return Err(ToolError(format!("{}: listed more than once", e.path)));
            }
            let change = match e.change(&mut blobs)? {
                FileChange::Modify {
                    content,
                    size,
                    kind,
                } if e.executable.is_none() => {
                    // Keep the kind the agent's side, the clean change or
                    // the ref had.
                    let kind = match (pending.conflicts.get(&path), changes.get(&path)) {
                        (Some(Some(k)), _) => *k,
                        (_, Some(FileChange::Modify { kind, .. })) => *kind,
                        _ => self
                            .existing_kind(Some(pending.onto), &path)?
                            .unwrap_or(kind),
                    };
                    FileChange::Modify {
                        content,
                        kind,
                        size,
                    }
                }
                other => other,
            };
            changes.insert(path, change);
        }
        let missing: Vec<String> = pending
            .conflicts
            .keys()
            .filter(|p| !seen.contains(*p))
            .map(|p| p.to_string())
            .collect();
        if !missing.is_empty() {
            return Err(ToolError(format!(
                "nothing was landed: resolutions must cover every conflicting path; missing: {}",
                missing.join(", ")
            )));
        }
        self.upload(blobs)?;
        self.ensure_clean_blobs(&pending)?;

        let original = self.client.get_changeset(&pending.changeset)?.changeset;
        let message = non_empty(a.message.as_deref())
            .map(str::to_string)
            .unwrap_or(original.message);
        let mut extra: BTreeMap<String, String> = original
            .extra
            .into_iter()
            .filter(|(k, _)| !k.starts_with("leep."))
            .collect();
        // The land id derives from this key, so the changeset says what it
        // was (`conflict:<id>` for a land that named no request).
        extra.insert(
            EXTRA_AGENT_REQUEST_ID.to_string(),
            pending.request_id.clone(),
        );
        let cs = Changeset {
            parents: vec![pending.onto],
            author: self.cfg.author.clone(),
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0),
            message,
            changes,
            extra,
        };
        let created = self.client.create_changeset(&cs)?;
        let request = pending.request_id.as_str();
        if !request.starts_with("conflict:") {
            self.requests
                .lock()
                .unwrap()
                .insert(request.to_string(), (created, pending.target_ref.clone()));
        }
        let out = self.land_and_report(
            created,
            &pending.target_ref,
            pending.create,
            cs.changes.len(),
            Some(request),
        )?;
        // Landed (now or before): nothing is left to resolve. A new
        // conflict replaced this one when it was reported.
        let landed = out
            .structured
            .as_ref()
            .and_then(|v| v["status"].as_str())
            .is_some_and(|s| s == "landed" || s == "already_landed");
        if landed {
            self.conflicts.lock().unwrap().forget(&pending.id);
        }
        Ok(out)
    }
}
