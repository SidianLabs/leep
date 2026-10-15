//! The Leep HTTP API (v1): request and response types shared by
//! `leep-server` and `leep-cli`. Bodies are JSON except raw blob bytes.
//!
//! | Method | Path | Request | Response |
//! |---|---|---|---|
//! | GET  | `/v1/health` | – | `"ok"` |
//! | PUT  | `/v1/blobs/{hash}` | raw bytes | 204; 400 if `Hash::of_blob(body) != hash` |
//! | GET  | `/v1/blobs/{hash}` | – | raw bytes, or 404 |
//! | POST | `/v1/blobs/missing` | [`MissingBlobsRequest`] | [`MissingBlobsResponse`] |
//! | POST | `/v1/changesets` | [`Changeset`] | [`CreateChangesetResponse`] |
//! | GET  | `/v1/changesets/{id}` | – | [`ChangesetInfo`] |
//! | GET  | `/v1/refs` | – | [`RefsResponse`] |
//! | GET  | `/v1/refs/{name}` | – | [`RefResponse`] |
//! | POST | `/v1/refs/{name}` | [`CasRefRequest`] | [`CasRefResponse`] (409 if it lost) |
//! | GET  | `/v1/resolve/{rev}` | – | [`ResolveResponse`] (`rev` = ref name or full 64-hex id) |
//! | GET  | `/v1/tree/{id}?path=dir` | – | [`TreeResponse`] (one directory level) |
//! | GET  | `/v1/files/{id}?prefix=dir` | – | [`FilesResponse`] (recursive listing) |
//! | GET  | `/v1/file/{id}?path=p` | – | raw bytes + `x-leep-kind` header, or 404 |
//! | GET  | `/v1/log/{id}?path=p&limit=n` | – | [`LogResponse`] (newest first; `limit` defaults to 100, capped at 10000) |
//! | GET  | `/v1/diff?from=a&to=b&prefix=dir` | – | [`DiffResponse`] (`from` omitted = empty tree) |
//! | GET  | `/v1/blame/{id}?path=p` | – | [`BlameResponse`] |
//! | GET  | `/v1/merge-base?a=x&b=y` | – | [`MergeBaseResponse`] |
//! | POST | `/v1/merge` | [`MergeRequest`] | [`MergeResponse`] |
//! | POST | `/v1/rebase` | [`RebaseRequest`] | [`RebaseResponse`] |
//! | POST | `/v1/land` | [`LandRequest`] | [`LandResponse`] |
//!
//! Ref names may contain `/` (e.g. `release/1.0`); `{name}` and `{rev}` match
//! the rest of the path.
//!
//! Errors are `{"error": "..."}` ([`ErrorResponse`]): `Error::Invalid` → 400,
//! `Error::NotFound` → 404, CAS loss → 409, anything else → 500.
//! Hashes and ids are lowercase hex in JSON.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::{Changeset, ChangesetId, FileKind, Hash, RepoPath, TreeEntry};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorResponse {
    pub error: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MissingBlobsRequest {
    pub hashes: Vec<Hash>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MissingBlobsResponse {
    /// The subset of requested hashes the server does not have.
    pub missing: Vec<Hash>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateChangesetResponse {
    pub id: ChangesetId,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChangesetInfo {
    pub id: ChangesetId,
    pub changeset: Changeset,
    /// Length of the longest parent chain to a root; roots are 1.
    pub generation: u64,
    pub root_tree: Hash,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RefsResponse {
    pub refs: BTreeMap<String, ChangesetId>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RefResponse {
    pub name: String,
    pub target: Option<ChangesetId>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CasRefRequest {
    pub expected: Option<ChangesetId>,
    pub new: Option<ChangesetId>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CasRefResponse {
    pub ok: bool,
    /// Value of the ref after the call.
    pub current: Option<ChangesetId>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolveResponse {
    pub id: ChangesetId,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TreeResponse {
    pub path: String,
    pub tree: Hash,
    pub entries: BTreeMap<String, TreeEntry>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileInfo {
    pub path: RepoPath,
    pub content: Hash,
    pub kind: FileKind,
    pub size: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FilesResponse {
    pub files: Vec<FileInfo>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogResponse {
    pub entries: Vec<ChangesetInfo>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiffEntry {
    pub path: RepoPath,
    pub old: Option<FileInfo>,
    pub new: Option<FileInfo>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiffResponse {
    pub entries: Vec<DiffEntry>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlameLine {
    /// Changeset that last changed this line.
    pub changeset: ChangesetId,
    /// 1-based line number in that changeset's version of the file.
    pub origin_line: u32,
    pub text: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlameResponse {
    pub path: RepoPath,
    pub lines: Vec<BlameLine>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MergeBaseResponse {
    pub base: Option<ChangesetId>,
}

/// Fields the server needs to author a changeset it creates (merge/rebase).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Authorship {
    pub author: String,
    /// Unix seconds; server uses its clock if absent.
    #[serde(default)]
    pub timestamp: Option<i64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MergeRequest {
    /// First is the "ours" side; changes are recorded relative to it.
    pub parents: [ChangesetId; 2],
    pub message: String,
    pub authorship: Authorship,
    #[serde(default)]
    pub extra: BTreeMap<String, String>,
}

/// A path both sides changed incompatibly. For text files `merged` holds the
/// content with `<<<<<<<`/`=======`/`>>>>>>>` markers.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Conflict {
    pub path: RepoPath,
    pub base: Option<FileInfo>,
    pub ours: Option<FileInfo>,
    pub theirs: Option<FileInfo>,
    #[serde(default)]
    pub merged_with_markers: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum MergeResponse {
    Merged { id: ChangesetId },
    Conflicts { conflicts: Vec<Conflict> },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RebaseRequest {
    /// Rebases this single changeset (its first-parent changes) onto `onto`.
    pub changeset: ChangesetId,
    pub onto: ChangesetId,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum RebaseResponse {
    /// New changeset with parent `onto`, same author/message/extra, plus
    /// `extra["leep.rebased_from"]`. If already based on `onto`, returns it.
    Rebased {
        id: ChangesetId,
    },
    Conflicts {
        conflicts: Vec<Conflict>,
    },
}

/// Land a changeset on a ref with no merge queue: if the ref moved since the
/// changeset's parent, the server rebases it onto the current head and retries
/// the compare-and-swap until it wins or hits a conflict.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LandRequest {
    pub changeset: ChangesetId,
    #[serde(default = "default_ref")]
    pub target_ref: String,
}

fn default_ref() -> String {
    "main".into()
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum LandResponse {
    /// `landed` is the changeset now at the ref head (a rebased copy if the
    /// ref had moved).
    Landed {
        landed: ChangesetId,
        rebased: bool,
        attempts: u32,
    },
    Conflicts {
        onto: ChangesetId,
        conflicts: Vec<Conflict>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tagged_enums_serialize_with_status() {
        let id = ChangesetId(Hash::of_blob(b"x"));
        let v = serde_json::to_value(LandResponse::Landed {
            landed: id,
            rebased: false,
            attempts: 1,
        })
        .unwrap();
        assert_eq!(v["status"], "landed");
        let req: LandRequest =
            serde_json::from_value(serde_json::json!({ "changeset": id.to_string() })).unwrap();
        assert_eq!(req.target_ref, "main");
    }
}
