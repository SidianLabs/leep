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
//! | GET  | `/v1/resolve/{rev}` | – | [`ResolveResponse`] (`rev` = ref name, full 64-hex id or short tag name) |
//! | GET  | `/v1/tree/{id}?path=dir` | – | [`TreeResponse`] (one directory level) |
//! | GET  | `/v1/files/{id}?prefix=dir` | – | [`FilesResponse`] (recursive listing) |
//! | GET  | `/v1/file/{id}?path=p` | – | raw bytes + `x-leep-kind` header, or 404 |
//! | GET  | `/v1/log/{id}?path=p&limit=n&author=a&extra=k:v` | – | [`LogResponse`] (newest first; `limit` defaults to 100, capped at 10000). `author` is a case-insensitive substring; `extra` repeats, is ANDed, and is `key:value` (exact) or `key` (present). `limit` counts matches. With `path` the walk follows the path-history index; without it, or for `author` and `extra`, a selective filter scans changesets linearly. `follow=true` (needs a file `path`; 400 otherwise) continues the history of a renamed file under its old name and marks entries with `path_at` and `renamed` (ADR 0016) |
//! | GET  | `/v1/last-change/{id}?path=p` | – | [`LastChangeResponse`] (the newest changeset reachable from `id` that changed file or directory `p`, answered from the path-history index; a deleted path answers with the deleting changeset; 404 if `p` never existed) |
//! | GET  | `/v1/diff?from=a&to=b&prefix=dir&renames=bool&rename_threshold=n` | – | [`DiffResponse`] (`from` omitted = empty tree; `renames=true` adds detected renames, `rename_threshold` is 0 to 100 and defaults to 50, anything else is 400; ADR 0016) |
//! | GET  | `/v1/blame/{id}?path=p` | – | [`BlameResponse`] (with per-changeset [`ChangesetSummary`]) |
//! | GET  | `/v1/provenance/{id}?path=prefix&limit=n` | – | [`ProvenanceResponse`] (who and what made the changesets reachable from `id`; `limit` defaults to 10000, capped at 10000) |
//! | GET  | `/v1/search/{rev}?q=text&regex=bool&i=bool&path=dir&glob=g&limit=n` | – | [`SearchResponse`] (`rev` = ref name or full id; `limit` defaults to 100, capped at 1000; booleans are `true` or `false`; a bad regex is 400, an unknown `rev` 404) |
//! | GET  | `/v1/merge-base?a=x&b=y` | – | [`MergeBaseResponse`] |
//! | POST | `/v1/changesets/{id}/signatures` | [`AddSignatureRequest`] | [`AddSignatureResponse`] (400 if the signature is bad or the key is not trusted; 404 if the changeset is unknown) |
//! | GET  | `/v1/changesets/{id}/signatures` | – | [`SignaturesResponse`] (trusted signatures, re-verified on read) |
//! | POST | `/v1/changesets/{id}/reviews` | [`AddReviewRequest`] | [`AddReviewResponse`] (the reviewer is the token's name; 400 without authentication; 404 if the changeset is unknown) |
//! | GET  | `/v1/changesets/{id}/reviews` | – | [`ReviewsResponse`] |
//! | GET  | `/v1/changesets/{id}/review-status?ref=main` | – | [`ReviewStatusResponse`] (`ref` defaults to `main`) |
//! | POST | `/v1/merge` | [`MergeRequest`] | [`MergeResponse`] |
//! | POST | `/v1/rebase` | [`RebaseRequest`] | [`RebaseResponse`] |
//! | POST | `/v1/land` | [`LandRequest`] | [`LandResponse`] |
//!
//! Ref names may contain `/` (e.g. `release/1.0`); `{name}` and `{rev}` match
//! the rest of the path. Every ref write (`POST /v1/refs/{name}` and the
//! `target_ref` of `land`) checks the name with [`crate::refname::validate`]
//! and answers 400 if it is invalid. Refs under `tags/` are tags: they can be
//! created (`expected: null`) or deleted (`new: null`) but a move from one
//! changeset to another is 400, and `land` onto one is 400. Deleting a ref
//! that a land policy protects is 400. `{rev}` tries a ref, then a full id,
//! then `tags/{rev}`.
//!
//! Errors are `{"error": "..."}` ([`ErrorResponse`]): `Error::Invalid` → 400,
//! `Error::NotFound` → 404, CAS loss and `Error::Conflict` (e.g. a land that
//! kept losing to concurrent lands) → 409, anything else → 500.
//! Hashes and ids are lowercase hex in JSON.
//!
//! # Authentication
//!
//! A server started with a tokens file requires `Authorization: Bearer
//! <token>` on every endpoint except `GET /v1/health`, which stays public for
//! load balancers. A missing or unknown token is 401 with
//! `WWW-Authenticate: Bearer`. A `read` token may call every `GET` endpoint and
//! `POST /v1/blobs/missing` (a query); any other request with it is 403. A
//! `write` token may call everything. A server without a tokens file accepts
//! all requests and ignores the header. Both failures use [`ErrorResponse`].
//! This adds behavior only; no type or path changed (FORMATS.md, rule 4).
//! Adding a signature is a `POST`, so it needs a `write` token; listing is a
//! `GET`.
//!
//! # Reviews
//!
//! Reviews are attestations stored beside changesets (ADR 0011). They need
//! authentication: the reviewer is the token's name, and `GET` lists reviews
//! by looking up each known principal, so a server without a tokens file
//! answers all three review endpoints with 400. Servers started with
//! `--require-review REF` or a review policy file answer `POST /v1/land`
//! onto a protected ref with 400 (`changeset X needs review: ...`) unless
//! every changeset the land would add is approved.
//!
//! # Signatures
//!
//! A signature attests to a changeset id and is stored beside the changeset
//! (ADR 0010). The signed bytes are [`crate::signature::signing_message`]. The
//! server accepts only keys listed in its signing keys file. Servers started
//! with `--require-signatures REF` answer `POST /v1/land` onto `REF` with 400
//! (`changeset X is not signed by a trusted key`) unless every changeset the
//! land would add to the ref is signed.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::review::{Review, Verdict};
use crate::{Changeset, ChangesetId, FileChange, FileKind, Hash, RepoPath, TreeEntry};

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
    /// Name of the followed file in this changeset. Only set by a `follow`
    /// log (ADR 0016), where it differs from the requested path before a
    /// rename.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path_at: Option<RepoPath>,
    /// Set on the entry of a `follow` log where the file got its name by
    /// renaming `from`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub renamed: Option<RenameEntry>,
}

/// Attest to a changeset. Both fields are text so the body is easy to build
/// with any tool: the key as `ed25519:<64 hex>`, the signature as 128 hex.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AddSignatureRequest {
    pub public_key: String,
    pub signature: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AddSignatureResponse {
    pub key_id: String,
    /// The name the key has in the server's signing keys file.
    pub name: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignatureInfo {
    pub key_id: String,
    pub name: String,
    /// `ed25519:<64 hex>`.
    pub public_key: String,
}

/// Trusted signatures of one changeset.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignaturesResponse {
    pub signatures: Vec<SignatureInfo>,
    /// True when every stored signature verified on this read. One that does
    /// not (a corrupted row) is left out of `signatures` and clears this.
    pub verified: bool,
    /// Set when the changeset has no signature of its own but is a server-made
    /// rebased copy of this one, whose `signatures` are reported instead.
    /// Informational: `land` policy never accepts a copy on this basis.
    #[serde(default)]
    pub via: Option<ChangesetId>,
}

/// Record the caller's verdict on a changeset. The reviewer is the
/// authenticated token's name; the body cannot name one.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AddReviewRequest {
    pub verdict: Verdict,
    #[serde(default)]
    pub comment: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AddReviewResponse {
    pub reviewer: String,
    pub verdict: Verdict,
}

/// Reviews of one changeset by the server's known principals.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReviewsResponse {
    pub reviews: Vec<Review>,
    /// Set when the changeset has no review of its own but is a server-made
    /// rebased copy of this one, whose `reviews` are reported instead.
    /// Informational: the land policy never accepts a copy on this basis.
    #[serde(default)]
    pub via: Option<ChangesetId>,
}

/// Review state of one changed path. `rule` is `auto:<rule>` (for example
/// `auto:comment_only`), `owner`, `merge_parent`, `pending` or `rejected`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PathReview {
    pub path: RepoPath,
    pub rule: String,
    /// Owners from the target ref's head; empty when none cover the path.
    pub owners: Vec<String>,
    pub approved_by: Vec<String>,
    #[serde(default)]
    pub rejected_by: Vec<String>,
}

/// Whether a changeset could land on `target_ref` now, per path.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReviewStatusResponse {
    pub changeset: ChangesetId,
    pub target_ref: String,
    pub approved: bool,
    pub paths: Vec<PathReview>,
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

/// Newest change to a path, from the path-history index.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LastChangeResponse {
    pub path: String,
    pub changeset: ChangesetInfo,
    /// Other newest changes that are not ancestors of `changeset`: two merged
    /// branches that both changed the path and kept it as it was in the first
    /// parent. Usually empty.
    #[serde(default)]
    pub concurrent: Vec<ChangesetId>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiffEntry {
    pub path: RepoPath,
    pub old: Option<FileInfo>,
    pub new: Option<FileInfo>,
}

/// A deleted and an added file that are probably the same file moved.
/// Changesets store renames as delete plus add, so these are detected when
/// displaying (ADR 0016).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RenameEntry {
    pub from: RepoPath,
    pub to: RepoPath,
    /// 0 to 100; see `leep_core::rename` for the definition.
    pub similarity: u8,
    /// Same content and same kind, so `similarity` is 100 and nothing else
    /// changed.
    pub exact: bool,
}

/// `entries` is unchanged by rename detection: a rename still appears as a
/// deleted and an added entry, so clients that ignore `renames` see what they
/// always saw.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiffResponse {
    pub entries: Vec<DiffEntry>,
    /// Only with `renames=true`; sorted by `to`. Each path of `entries`
    /// appears in at most one rename.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub renames: Vec<RenameEntry>,
    /// Similarity matching was skipped because there were too many candidate
    /// pairs; exact renames are still reported.
    #[serde(default, skip_serializing_if = "is_false")]
    pub renames_truncated: bool,
}

fn is_false(b: &bool) -> bool {
    !*b
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlameLine {
    /// Changeset that last changed this line.
    pub changeset: ChangesetId,
    /// 1-based line number in that changeset's version of the file.
    pub origin_line: u32,
    pub text: String,
}

/// Who made a changeset, kept small enough to ship once per changeset.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChangesetSummary {
    pub author: String,
    pub timestamp: i64,
    pub message_first_line: String,
    /// Only the `agent.*` keys of the changeset's `extra`.
    #[serde(default)]
    pub agent: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlameResponse {
    pub path: RepoPath,
    pub lines: Vec<BlameLine>,
    /// One entry per distinct changeset in `lines`, so a line stays small
    /// and a changeset that owns many lines is described once. Absent from
    /// responses of servers older than provenance support.
    #[serde(default)]
    pub changesets: BTreeMap<ChangesetId, ChangesetSummary>,
}

/// Changesets made by one agent model, optionally narrowed to one session.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentStat {
    pub model: String,
    #[serde(default)]
    pub session: Option<String>,
    pub changesets: u64,
    /// Distinct paths changed (within the requested prefix, if any).
    pub files_touched: u64,
    /// Oldest and newest changeset timestamps (unix seconds).
    pub first: i64,
    pub last: i64,
}

/// Changesets by one author that carry no `agent.*` provenance.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HumanStat {
    pub author: String,
    pub changesets: u64,
    pub files_touched: u64,
    pub first: i64,
    pub last: i64,
}

/// A changeset counts as an agent's if its `extra` has `agent.model` or
/// `agent.session` (a missing model is reported as `unknown`). Counts are over
/// the first `limit` changesets of the walk from the starting changeset, newest
/// first; `truncated` says more history was left unscanned.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProvenanceResponse {
    pub agents: Vec<AgentStat>,
    pub humans: Vec<HumanStat>,
    /// Changesets scanned.
    pub scanned: u64,
    pub truncated: bool,
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
    Rebased { id: ChangesetId },
    Conflicts {
        conflicts: Vec<Conflict>,
        /// Changes relative to `onto` for every path that merged cleanly.
        /// Applying these plus a resolution of each conflict on top of `onto`
        /// gives the rebased result.
        #[serde(default)]
        clean: BTreeMap<RepoPath, FileChange>,
    },
}

/// Land a changeset on a ref with no merge queue. If the ref head is an
/// ancestor, the ref fast-forwards. Otherwise the server replays the stack of
/// unlanded changesets (the first-parent chain from `changeset` down to where
/// it joins the head's history) onto the current head, one by one, and
/// retries the compare-and-swap until it wins or hits a conflict.
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
    /// The changeset (and any unlanded changesets below it) could not be
    /// replayed onto `onto`. `conflicts` and `clean` describe the whole stack
    /// squashed into one change relative to `onto`: a client resolves the
    /// conflicts on top of `onto`, commits once, and lands again.
    Conflicts {
        onto: ChangesetId,
        conflicts: Vec<Conflict>,
        /// Changes relative to `onto` for every path that merged cleanly.
        #[serde(default)]
        clean: BTreeMap<RepoPath, FileChange>,
    },
}

/// A byte range within [`SearchMatch::line`] (`end` is exclusive).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SearchRange {
    pub start: u32,
    pub end: u32,
}

/// One matching line. Files are searched line by line, so a match never spans
/// lines.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SearchMatch {
    pub path: RepoPath,
    /// 1-based.
    pub line_number: u64,
    /// The line without its terminator, cut to 1024 bytes on a character
    /// boundary.
    pub line: String,
    /// Non-empty matches within `line`, in order.
    #[serde(default)]
    pub ranges: Vec<SearchRange>,
}

/// What a search looked at. Every count is a number of files unless named
/// otherwise; `files_considered` counts files in directories the index did not
/// rule out, so a pruned directory contributes only to `dirs_pruned`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SearchStats {
    #[serde(default)]
    pub files_considered: u64,
    /// Directories skipped because their summary lacks a required trigram.
    #[serde(default)]
    pub dirs_pruned: u64,
    /// Files skipped because their trigram set lacks a required trigram.
    #[serde(default)]
    pub files_pruned: u64,
    /// Files fetched and matched.
    #[serde(default)]
    pub files_scanned: u64,
    /// Files that are not searched: binary, not UTF-8, over 1 MiB, or symlinks.
    #[serde(default)]
    pub files_skipped: u64,
    #[serde(default)]
    pub bytes_scanned: u64,
    /// Index rows this request had to compute (a cold revision is slow once).
    #[serde(default)]
    pub blob_rows_derived: u64,
    #[serde(default)]
    pub tree_rows_derived: u64,
    /// False when the query yields no required trigram, so the files were
    /// scanned without the index.
    #[serde(default)]
    pub indexed: bool,
}

/// Matches ordered by path, then line. `truncated` is set when more matches
/// exist than were returned, or when the server's scan budget ran out first
/// (then `stats.bytes_scanned` is near the cap and the list is a prefix).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SearchResponse {
    #[serde(default)]
    pub matches: Vec<SearchMatch>,
    #[serde(default)]
    pub truncated: bool,
    #[serde(default)]
    pub stats: SearchStats,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn search_response_tolerates_missing_fields() {
        let r: SearchResponse = serde_json::from_value(serde_json::json!({})).unwrap();
        assert!(r.matches.is_empty() && !r.truncated);
        assert_eq!(r.stats, SearchStats::default());
        let m: SearchMatch = serde_json::from_value(serde_json::json!({
            "path": "a/b", "line_number": 3, "line": "x"
        }))
        .unwrap();
        assert!(m.ranges.is_empty());
        let v = serde_json::to_value(SearchRange { start: 1, end: 2 }).unwrap();
        assert_eq!(v, serde_json::json!({ "start": 1, "end": 2 }));
    }

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

    #[test]
    fn review_shapes() {
        let req: AddReviewRequest =
            serde_json::from_value(serde_json::json!({ "verdict": "approve" })).unwrap();
        assert_eq!(req.verdict, Verdict::Approve);
        assert_eq!(req.comment, "");
        assert!(serde_json::from_value::<AddReviewRequest>(
            serde_json::json!({ "verdict": "shrug" })
        )
        .is_err());
        let v: ReviewsResponse =
            serde_json::from_value(serde_json::json!({ "reviews": [] })).unwrap();
        assert_eq!(v.via, None);
        let p: PathReview = serde_json::from_value(serde_json::json!({
            "path": "a", "rule": "owner", "owners": [], "approved_by": []
        }))
        .unwrap();
        assert!(p.rejected_by.is_empty());
    }

    #[test]
    fn signatures_response_without_via_parses() {
        let v: SignaturesResponse =
            serde_json::from_value(serde_json::json!({ "signatures": [], "verified": true }))
                .unwrap();
        assert_eq!(v.via, None);
    }

    #[test]
    fn last_change_response_without_concurrent_parses() {
        let id = ChangesetId(Hash::of_blob(b"z"));
        let cs = Changeset {
            parents: vec![],
            author: "a".into(),
            timestamp: 1,
            message: "m".into(),
            changes: BTreeMap::new(),
            extra: BTreeMap::new(),
        };
        let info = ChangesetInfo {
            id,
            changeset: cs,
            generation: 1,
            root_tree: Hash::of_blob(b"t"),
            path_at: None,
            renamed: None,
        };
        let mut v = serde_json::json!({ "path": "src", "changeset": info });
        let r: LastChangeResponse = serde_json::from_value(v.clone()).unwrap();
        assert!(r.concurrent.is_empty());
        assert_eq!(r.changeset.id, id);
        v["concurrent"] = serde_json::json!([id]);
        let r: LastChangeResponse = serde_json::from_value(v).unwrap();
        assert_eq!(r.concurrent, [id]);
    }

    #[test]
    fn blame_response_without_changesets_still_parses() {
        let v: BlameResponse =
            serde_json::from_value(serde_json::json!({ "path": "a", "lines": [] })).unwrap();
        assert!(v.changesets.is_empty());
    }

    #[test]
    fn blame_summaries_round_trip_keyed_by_id() {
        let id = ChangesetId(Hash::of_blob(b"y"));
        let r = BlameResponse {
            path: RepoPath::new("a").unwrap(),
            lines: vec![],
            changesets: BTreeMap::from([(
                id,
                ChangesetSummary {
                    author: "x".into(),
                    timestamp: 3,
                    message_first_line: "m".into(),
                    agent: BTreeMap::from([("agent.model".into(), "m1".into())]),
                },
            )]),
        };
        let v = serde_json::to_value(&r).unwrap();
        assert_eq!(
            v["changesets"][id.to_string()]["agent"]["agent.model"],
            "m1"
        );
        assert_eq!(serde_json::from_value::<BlameResponse>(v).unwrap(), r);
    }

    #[test]
    fn diff_response_without_renames_keeps_its_old_shape() {
        let old: DiffResponse =
            serde_json::from_value(serde_json::json!({ "entries": [] })).unwrap();
        assert!(old.renames.is_empty() && !old.renames_truncated);
        assert_eq!(
            serde_json::to_value(&old).unwrap(),
            serde_json::json!({ "entries": [] })
        );
        let r = RenameEntry {
            from: RepoPath::new("a").unwrap(),
            to: RepoPath::new("b").unwrap(),
            similarity: 90,
            exact: false,
        };
        let new = DiffResponse {
            renames: vec![r],
            renames_truncated: true,
            ..old
        };
        let v = serde_json::to_value(&new).unwrap();
        assert_eq!(v["renames"][0]["to"], "b");
        assert_eq!(v["renames_truncated"], true);
        assert_eq!(serde_json::from_value::<DiffResponse>(v).unwrap(), new);
    }
}
