//! The Leep HTTP API (v1): request and response types shared by
//! `leep-server` and `leep-cli`. Bodies are JSON except raw blob bytes.
//!
//! | Method | Path | Request | Response |
//! |---|---|---|---|
//! | GET  | `/v1/health` | – | `"ok"` (liveness: the process is up and serving HTTP; checks nothing else) |
//! | GET  | `/v1/whoami` | – | [`WhoAmIResponse`] (the calling token's name, role and optional `author`; null when authentication is off) |
//! | GET  | `/v1/ready` | – | [`ReadyResponse`] (readiness: 200 `{"status":"ready","features":[...]}` when the metadata and blob stores each answered a cheap read within the server's ready timeout, else 503 with `failed` naming the stores; `features` lists optional behaviours such as `land-id`, ADR 0057) |
//! | PUT  | `/v1/blobs/{hash}` | raw bytes | 204; 400 if `Hash::of_blob(body) != hash` |
//! | GET  | `/v1/blobs/{hash}` | – | raw bytes, or 404 |
//! | POST | `/v1/blobs/missing` | [`MissingBlobsRequest`] | [`MissingBlobsResponse`]; 400 if over [`MAX_MISSING_BLOBS`] hashes |
//! | POST | `/v1/changesets` | [`Changeset`] | [`CreateChangesetResponse`] |
//! | GET  | `/v1/changesets/{id}` | – | [`ChangesetInfo`] |
//! | GET  | `/v1/refs` | – | [`RefsResponse`] |
//! | GET  | `/v1/refs/{name}` | – | [`RefResponse`] |
//! | POST | `/v1/refs/{name}` | [`CasRefRequest`] | [`CasRefResponse`] (409 if it lost) |
//! | GET  | `/v1/reflog?before=seq&limit=n&after=cursor` | – | [`RefLogResponse`] (every ref move, newest first; `limit` defaults to 50, capped at 1000; `before` returns entries with a smaller `seq`; `after` is a page cursor, and with `before` is 400) |
//! | GET  | `/v1/reflog/{name}?before=seq&limit=n&after=cursor` | – | [`RefLogResponse`] (the moves of one ref; an unknown or never-moved ref is an empty list) |
//! | GET  | `/v1/resolve/{rev}` | – | [`ResolveResponse`] (`rev` = ref name, full 64-hex id or short tag name) |
//! | GET  | `/v1/resolve-prefix/{prefix}` | – | [`ResolvePrefixResponse`] (the one changeset whose id starts with `prefix`, 4 to 64 hex digits in any case; 404 if none; 400 [`AmbiguousPrefixResponse`] listing up to 10 `candidates` if several; 400 if the prefix is shorter than 4 digits, longer than 64 or not hex; ADR 0035) |
//! | GET  | `/v1/tree/{id}?path=dir` | – | [`TreeResponse`] (one directory level) |
//! | GET  | `/v1/files/{id}?prefix=dir` | – | [`FilesResponse`] (recursive listing) |
//! | GET  | `/v1/file/{id}?path=p` | – | raw bytes + `x-leep-kind` header, or 404 |
//! | GET  | `/v1/log/{id}?path=p&limit=n&author=a&extra=k:v` | – | [`LogResponse`] (newest first; `limit` defaults to 100, capped at 10000). `author` is a case-insensitive substring; `extra` repeats, is ANDed, and is `key:value` (exact) or `key` (present). `limit` counts matches. With `path` the walk follows the path-history index; without it, or for `author` and `extra`, a selective filter scans changesets linearly. `follow=true` (needs a file `path`; 400 otherwise) continues the history of a renamed file under its old name and marks entries with `path_at` and `renamed` (ADR 0016). `after` is a page cursor. `fields=summary` answers [`LogSummaryResponse`] instead: the same entries, cursors and fields, each entry a [`LogSummaryEntry`] (the message's first line, `agent.*` keys, the number of changed paths, no paths); `fields=full` is the default; anything else is 400 |
//! | GET  | `/v1/last-change/{id}?path=p` | – | [`LastChangeResponse`] (the newest changeset reachable from `id` that changed file or directory `p`, answered from the path-history index; a deleted path answers with the deleting changeset; 404 if `p` never existed) |
//! | GET  | `/v1/diff?from=a&to=b&prefix=dir&renames=bool&rename_threshold=n` | – | [`DiffResponse`] (`from` omitted = empty tree; `renames=true` adds detected renames, `rename_threshold` is 0 to 100 and defaults to 50, anything else is 400; ADR 0016) |
//! | GET  | `/v1/blame/{id}?path=p` | – | [`BlameResponse`] (with per-changeset [`ChangesetSummary`]) |
//! | GET  | `/v1/provenance/{id}?path=prefix&limit=n` | – | [`ProvenanceResponse`] (who and what made the changesets reachable from `id`; `limit` defaults to 10000, capped at 10000) |
//! | GET  | `/v1/search/{rev}?q=text&regex=bool&i=bool&path=dir&glob=g&limit=n&after=cursor` | – | [`SearchResponse`] (`rev` = ref name or full id; `limit` defaults to 100, capped at 1000; booleans are `true` or `false`; a bad regex is 400, an unknown `rev` 404; `after` is a page cursor) |
//! | GET  | `/v1/merge-base?a=x&b=y` | – | [`MergeBaseResponse`] |
//! | POST | `/v1/changesets/{id}/signatures` | [`AddSignatureRequest`] | [`AddSignatureResponse`] (400 if the signature is bad or the key is not trusted; 404 if the changeset is unknown) |
//! | GET  | `/v1/changesets/{id}/signatures` | – | [`SignaturesResponse`] (trusted signatures, re-verified on read) |
//! | POST | `/v1/changesets/{id}/reviews` | [`AddReviewRequest`] | [`AddReviewResponse`] (the reviewer is the token's name; 400 without authentication; 404 if the changeset is unknown) |
//! | GET  | `/v1/changesets/{id}/reviews` | – | [`ReviewsResponse`] |
//! | GET  | `/v1/changesets/{id}/review-status?ref=main` | – | [`ReviewStatusResponse`] (`ref` defaults to `main`) |
//! | POST | `/v1/merge` | [`MergeRequest`] | [`MergeResponse`] |
//! | POST | `/v1/rebase` | [`RebaseRequest`] | [`RebaseResponse`] |
//! | POST | `/v1/land` | [`LandRequest`] | [`LandResponse`] (`create: false` refuses to create a missing `target_ref` with 404; absent creates it, as before; ADR 0036. `land_id` makes retries idempotent at any age; ADR 0057) |
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
//! Errors are `{"error": "...", "code": "..."}` ([`ErrorResponse`]):
//! `Error::Invalid` → 400, `Error::NotFound` → 404, CAS loss and
//! `Error::Conflict` (e.g. a land that kept losing to concurrent lands) →
//! 409, `Error::Indexing` → 503 with `Retry-After` and [`IndexingInfo`]
//! (ADR 0058), anything else → 500.
//!
//! # Error codes
//!
//! `code` names the condition so clients need not match `error` text. The
//! list is a stable contract: a code keeps its meaning, new codes may be
//! added, and a client treats an unknown or absent code (older servers send
//! none) by its HTTP status. `error` stays human-readable and may change.
//!
//! | Status | `code` | Condition |
//! |---|---|---|
//! | 400 | `needs_review` | a land onto a ref whose review policy some changeset does not meet |
//! | 400 | `not_signed` | a land onto a ref that needs every changeset signed by a trusted key |
//! | 400 | `not_forward` | a move of a land-policy-protected ref, or of a protected ref (`--protect-ref`), that is not a fast-forward |
//! | 400 | `tag_immutable` | a move of a tag, or a land onto one |
//! | 400 | `land_id_reused` | a land id the ref already recorded for another changeset (ADR 0057) |
//! | 400 | `invalid_name` | a ref name [`crate::refname::validate`] refuses |
//! | 400 | `ambiguous_prefix` | a short id matching several changesets ([`AmbiguousPrefixResponse`]) |
//! | 400 | `invalid` | any other bad request |
//! | 401 | `unauthorized` | no token, or an unknown one |
//! | 401 | `token_expired` | a known token past its `expires` (ADR 0007): fetch a new one; retrying will not help |
//! | 403 | `forbidden` | a `read` token on a write, or a `"review": false` token posting a review |
//! | 404 | `ref_missing` | a land onto a missing ref with `create: false` (ADR 0036) |
//! | 404 | `not_found` | anything else that does not exist |
//! | 408 | `timeout` | the request timeout ran out; the request may or may not have applied |
//! | 409 | `cas_lost` | a concurrent update won repeatedly; read again and retry |
//! | 409 | `forward_failed` | the land could not reach the server coordinating its ref; retry (ADR 0056) |
//! | 413 | `too_large` | the body is over the server's or the token's limit |
//! | 429 | `rate_limited` | a request limit (rate or concurrency); see `Retry-After` |
//! | 500 | `internal` | the server failed |
//! | 503 | `indexing` | the index is too far behind; see `indexing` and `Retry-After` (ADR 0058) |
//! | 503 | `not_leader` | the server no longer coordinates lands on that ref; send again (ADR 0056) |
//! | 503 | `unavailable` | overloaded or busy (with `Retry-After` when known): back off and retry |
//!
//! A lost `POST /v1/refs/{name}` swap is not an error body: it is 409 with
//! [`CasRefResponse`] (`ok: false`). The constants are in [`error_code`].
//!
//! # Request ids
//!
//! A client may send [`REQUEST_ID_HEADER`] (`X-Request-Id`). The server keeps
//! a value that passes [`valid_request_id`] (1 to 128 characters of
//! `A-Z a-z 0-9 . _ : / -`) and otherwise picks 32 random hex digits, logs the request
//! under it, answers with it in the same header on every response, and adds
//! it to JSON error bodies as `request_id`.
//!
//! # Index lag (ADR 0058)
//!
//! Log with a path or filters, last-change, blame and search use indexes that
//! one server per group may build behind the head. Such a server never
//! derives them in a request: it scans what is missing up to a budget. Over
//! the budget the request header [`INDEXING_HEADER`] decides: `retry` gets
//! 503, `stale-ok` an answer at the newest indexed first-parent ancestor
//! (`answered_at`, `behind`), and no header a wait, a larger scan, then 503.
//! Successful indexed reads carry [`INDEX_SCANNED_HEADER`].
//! Hashes and ids are lowercase hex in JSON.
//!
//! # Pagination
//!
//! `GET /v1/log`, `/v1/search` and `/v1/reflog` answer one page of at most
//! `limit` items. A page that stopped before the end carries `next`, an
//! opaque cursor of at most 4096 URL-safe characters; repeating the request
//! with the same parameters plus `after=<next>` returns the following page
//! (`limit` may change between pages). Concatenated pages equal what one
//! request without a cap would return. A cursor is valid only for the query
//! that produced it: a malformed one, one from another endpoint or query, or
//! one from a server that changed the cursor layout is 400, and the client
//! starts over without `after`. A search cursor pins the revision the first
//! page resolved, so later pages read the same tree if the ref moves. A log
//! page with filters can be empty and still carry `next`; only a missing
//! `next` means the end. Clients that never send `after` see no change except
//! the extra field.
//!
//! # Ref log
//!
//! Every successful ref change (create, move, delete) appends a
//! [`RefLogEntry`] in the same atomic step as the change (ADR 0018). `land`
//! records `land`, `POST /v1/refs/{name}` records the request's `operation`
//! (default `cas`), and the entry's `actor` is the token's name, never a name
//! from the body, or null when authentication is off. A client may label its
//! own moves with `operation` and `message` (`leep undo` sends `undo`); the
//! server-owned labels `land` and `import` are 400. Refused and lost moves
//! write nothing. Entries are never removed. An entry of a land that carried
//! a `land_id` (ADR 0057) has `land_id` and `land_changeset` (the changeset
//! the land asked for); both are omitted otherwise and by older servers.
//!
//! # Authentication
//!
//! A server started with a tokens file requires `Authorization: Bearer
//! <token>` on every endpoint except `GET /v1/health` and `GET /v1/ready`,
//! which stay public for load balancers and orchestrator probes. A missing or unknown token is 401 with
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
pub use crate::store::{LandId, RefLogEntry};
use crate::{Changeset, ChangesetId, FileChange, FileKind, Hash, RepoPath, TreeEntry};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorResponse {
    pub error: String,
    /// Set on a 503 from a query whose index is too far behind (ADR 0058):
    /// what is behind and when to retry. Absent otherwise and from older
    /// servers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub indexing: Option<IndexingInfo>,
    /// What went wrong, as one of the stable [`error_code`] strings, so
    /// clients need not match `error` text. Absent from older servers;
    /// clients may meet codes they do not know and must then go by status.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
    /// The request's id ([`REQUEST_ID_HEADER`]), which the server's log line
    /// for it carries too. Absent from older servers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
}

impl ErrorResponse {
    pub fn new(error: impl Into<String>) -> Self {
        ErrorResponse {
            error: error.into(),
            indexing: None,
            code: None,
            request_id: None,
        }
    }

    /// The same error with `code` set (one of [`error_code`]).
    pub fn with_code(mut self, code: &str) -> Self {
        self.code = Some(code.to_string());
        self
    }
}

/// The `code` of an error body ([`ErrorResponse`],
/// [`AmbiguousPrefixResponse`]). A stable contract: a code keeps its meaning
/// forever and new ones may be added, so clients treat an unknown code like
/// an absent one and go by the HTTP status. Older servers send no code;
/// clients then fall back to the status and the `*_MARKER` texts.
pub mod error_code {
    /// 400: a land onto a ref whose policy needs review that some changeset
    /// lacks ([`crate::review::NEEDS_REVIEW_MARKER`]).
    pub const NEEDS_REVIEW: &str = "needs_review";
    /// 400: a land onto a ref that only accepts changesets signed by a
    /// trusted key ([`crate::signature::NOT_SIGNED_MARKER`]).
    pub const NOT_SIGNED: &str = "not_signed";
    /// 400: a move of a land-policy-protected ref, or of a protected ref
    /// (`--protect-ref`), that is not a fast-forward
    /// ([`crate::refname::NOT_FORWARD_MARKER`]).
    pub const NOT_FORWARD: &str = "not_forward";
    /// 400: a tag move, or a land onto a tag. Tags are created and deleted,
    /// never moved.
    pub const TAG_IMMUTABLE: &str = "tag_immutable";
    /// 400: a land id the ref already recorded for another changeset
    /// (ADR 0057).
    pub const LAND_ID_REUSED: &str = "land_id_reused";
    /// 400: a ref name that [`crate::refname::validate`] refuses.
    pub const INVALID_NAME: &str = "invalid_name";
    /// 400: a short id that matches several changesets
    /// ([`super::AmbiguousPrefixResponse`]).
    pub const AMBIGUOUS_PREFIX: &str = "ambiguous_prefix";
    /// 400: any other bad request.
    pub const INVALID: &str = "invalid";
    /// 401: no token, or an unknown one.
    pub const UNAUTHORIZED: &str = "unauthorized";
    /// 401: the token is known but past its `expires` (ADR 0007). Fetch a
    /// new token; retrying with this one will not help.
    pub const TOKEN_EXPIRED: &str = "token_expired";
    /// 403: a `read` token on a write, or a `"review": false` token posting
    /// a review (ADR 0011).
    pub const FORBIDDEN: &str = "forbidden";
    /// 404: a land onto a missing ref with `create: false`
    /// ([`crate::refname::MISSING_REF_MARKER`]).
    pub const REF_MISSING: &str = "ref_missing";
    /// 404: anything else that does not exist.
    pub const NOT_FOUND: &str = "not_found";
    /// 408: the server's request timeout ran out; the request may or may not
    /// have taken effect.
    pub const TIMEOUT: &str = "timeout";
    /// 409: a concurrent update won (a land that kept losing its ref swap);
    /// reading again and retrying can succeed.
    pub const CAS_LOST: &str = "cas_lost";
    /// 409: the land could not reach the server coordinating its ref
    /// ([`super::FORWARD_FAILED_MARKER`], ADR 0056). Nothing landed here;
    /// retrying can succeed once that server answers or another takes over.
    pub const FORWARD_FAILED: &str = "forward_failed";
    /// 413: the request body is over the server's or the token's limit.
    pub const TOO_LARGE: &str = "too_large";
    /// 429: a request limit (rate or concurrency); see `Retry-After`.
    pub const RATE_LIMITED: &str = "rate_limited";
    /// 503 with [`super::IndexingInfo`]: the index is too far behind
    /// (ADR 0058).
    pub const INDEXING: &str = "indexing";
    /// 503: the server no longer coordinates lands on the ref; send the land
    /// again (ADR 0056).
    pub const NOT_LEADER: &str = "not_leader";
    /// 503 (with `Retry-After` when the server knows): overloaded or busy.
    /// Back off and retry.
    pub const UNAVAILABLE: &str = "unavailable";
    /// 500: the server failed.
    pub const INTERNAL: &str = "internal";

    /// Every code, for documentation tests.
    pub const ALL: &[&str] = &[
        NEEDS_REVIEW,
        NOT_SIGNED,
        NOT_FORWARD,
        TAG_IMMUTABLE,
        LAND_ID_REUSED,
        INVALID_NAME,
        AMBIGUOUS_PREFIX,
        INVALID,
        UNAUTHORIZED,
        TOKEN_EXPIRED,
        FORBIDDEN,
        REF_MISSING,
        NOT_FOUND,
        TIMEOUT,
        CAS_LOST,
        FORWARD_FAILED,
        TOO_LARGE,
        RATE_LIMITED,
        INDEXING,
        NOT_LEADER,
        UNAVAILABLE,
        INTERNAL,
    ];
}

/// Stable text in the 409 of a land that could not reach the server
/// coordinating its ref (ADR 0056); the server maps it to
/// [`error_code::FORWARD_FAILED`].
pub const FORWARD_FAILED_MARKER: &str = "could not reach the server coordinating that ref";

#[cfg(test)]
mod error_code_tests {
    #[test]
    fn codes_are_unique_snake_case() {
        let mut seen = std::collections::BTreeSet::new();
        for c in super::error_code::ALL {
            assert!(
                c.bytes().all(|b| b.is_ascii_lowercase() || b == b'_'),
                "{c}"
            );
            assert!(seen.insert(c), "{c} twice");
        }
    }
}

/// Why a query was answered `503` with `Retry-After` (ADR 0058): its index
/// is `behind` changesets behind and the server would not scan that far
/// without it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexingInfo {
    /// The index group, by its lease name (`history-tree-v4`,
    /// `log-filter-v1`, `tree-trigrams-v1`).
    pub kind: String,
    pub behind: u64,
    pub retry_after_ms: u64,
}

/// Request and response header naming one request, to find it in the
/// server's logs. A client may send one; the server keeps it if it passes
/// [`valid_request_id`], else makes its own, and answers with the id it used.
pub const REQUEST_ID_HEADER: &str = "x-request-id";

/// Longest request id a server keeps, in bytes.
pub const MAX_REQUEST_ID_LEN: usize = 128;

/// Whether a client's request id is kept as sent: 1 to
/// [`MAX_REQUEST_ID_LEN`] characters of `A-Z a-z 0-9 . _ : / -`. No spaces,
/// `=` or quotes, so an id cannot pose as another field of a log line.
pub fn valid_request_id(id: &str) -> bool {
    (1..=MAX_REQUEST_ID_LEN).contains(&id.len())
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b".-_:/".contains(&b))
}

/// Request header choosing what a query does when its index is behind
/// (ADR 0058): [`INDEXING_RETRY`] or [`INDEXING_STALE_OK`]. Servers that
/// list [`FEATURE_INDEXING_RETRY`] honour it; others ignore it.
pub const INDEXING_HEADER: &str = "leep-indexing";
/// [`INDEXING_HEADER`] value: answer `503` with `Retry-After` instead of
/// waiting.
pub const INDEXING_RETRY: &str = "retry";
/// [`INDEXING_HEADER`] value: answer at the newest indexed first-parent
/// ancestor, naming it in `answered_at` and the lag in `behind`.
pub const INDEXING_STALE_OK: &str = "stale-ok";
/// Response header of an indexed read: changesets (or, for search, files)
/// without index rows that the answer examined directly.
pub const INDEX_SCANNED_HEADER: &str = "leep-index-scanned";

/// Body of `GET /v1/whoami`. Lets a client name the author of its changesets
/// after the token when nothing local names one.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WhoAmIResponse {
    /// The token's name, or null when authentication is off.
    pub name: Option<String>,
    /// `read` or `write`, or null when authentication is off.
    pub role: Option<String>,
    /// The token entry's `author` (e.g. `Name <email>`), if the tokens file
    /// sets one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub author: Option<String>,
}

/// Body of `GET /v1/ready`. `status` is `ready` (HTTP 200) or `not ready`
/// (HTTP 503, with `failed` non-empty).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadyResponse {
    pub status: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub failed: Vec<ReadyFailure>,
    /// Optional behaviours this server has, for clients that must know
    /// before relying on one (absent from older servers): see
    /// [`FEATURE_LAND_ID`].
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub features: Vec<String>,
}

/// In [`ReadyResponse::features`]: the server records [`LandRequest::land_id`]
/// and answers a repeat of a recorded land with its result (ADR 0057).
pub const FEATURE_LAND_ID: &str = "land-id";

/// In [`ReadyResponse::features`]: the server honours [`INDEXING_HEADER`]
/// (ADR 0058).
pub const FEATURE_INDEXING_RETRY: &str = "indexing-retry";

/// In [`ReadyResponse::features`]: the server rescues condemned changesets
/// before it writes anything that names them, so a changeset garbage
/// collection may run beside it (ADR 0059).
pub const FEATURE_CHANGESET_RESCUE: &str = "changeset-rescue";

/// In [`ReadyResponse::features`]: every blob named in the `clean` changes of
/// a [`LandResponse::Conflicts`] is stored before the answer, so a client may
/// apply them as they are. Older servers could answer with merged text they
/// never stored.
pub const FEATURE_LAND_CLEAN_BLOBS: &str = "land-clean-blobs";

/// One store that failed its readiness check.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadyFailure {
    /// `meta` or `blobs`.
    pub store: String,
    /// `timed out after <n>ms` or `unavailable`. Never an error message, which
    /// could carry a connection string; the server logs the detail.
    pub reason: String,
}

/// Most hashes one `POST /v1/blobs/missing` may ask about. Each costs a
/// metadata read and a claim write, so an unbounded list is an unbounded
/// request. Well above what clients send at once ([`MISSING_BLOBS_CHUNK`]),
/// so a client that does not split its list (older CLIs send a whole tree,
/// about 90k hashes for Linux) still works.
pub const MAX_MISSING_BLOBS: usize = 100_000;

/// Hashes per `POST /v1/blobs/missing` that the CLI sends: about 670 KB of
/// JSON, under the 1 MiB default body cap of `read` tokens.
pub const MISSING_BLOBS_CHUNK: usize = 10_000;

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

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CasRefRequest {
    pub expected: Option<ChangesetId>,
    pub new: Option<ChangesetId>,
    /// Short label stored in the ref log entry: lowercase letters, digits and
    /// `-_.`, up to 32 bytes. Defaults to `cas`; `land` and `import` are
    /// reserved for the server.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation: Option<String>,
    /// Free text stored in the ref log entry, up to 256 bytes, no control
    /// characters.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CasRefResponse {
    pub ok: bool,
    /// Value of the ref after the call.
    pub current: Option<ChangesetId>,
}

/// Ref log entries, newest first.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RefLogResponse {
    pub entries: Vec<RefLogEntry>,
    /// Cursor for the next page (see "Pagination"); absent on the last page
    /// and from older servers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolveResponse {
    pub id: ChangesetId,
}

/// Body of `GET /v1/resolve-prefix/{prefix}` when exactly one changeset
/// matches.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvePrefixResponse {
    pub id: ChangesetId,
}

/// The 400 body of `GET /v1/resolve-prefix/{prefix}` when several changesets
/// match. A superset of [`ErrorResponse`], so clients that only read `error`
/// still get the message.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AmbiguousPrefixResponse {
    pub error: String,
    /// Matching ids, ascending, at most 10.
    pub candidates: Vec<ChangesetId>,
    /// More changesets match than `candidates` lists.
    #[serde(default)]
    pub truncated: bool,
    /// [`error_code::AMBIGUOUS_PREFIX`]; absent from older servers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
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
    /// Cursor for the next page (see "Pagination"); absent when the walk
    /// ended and from older servers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next: Option<String>,
    /// With `Leep-Indexing: stale-ok`, the changeset the answer is for when
    /// it is not the requested one (ADR 0058). Absent otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub answered_at: Option<ChangesetId>,
    /// Generations between `answered_at` and the requested changeset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub behind: Option<u64>,
}

/// What each log entry carries: `fields` of `GET /v1/log/{id}`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum LogFields {
    /// [`ChangesetInfo`], the whole changeset: the default.
    #[default]
    Full,
    /// [`LogSummaryEntry`]: no changed paths, the message's first line.
    Summary,
}

impl LogFields {
    pub fn parse(v: &str) -> crate::Result<Self> {
        match v {
            "full" => Ok(LogFields::Full),
            "summary" => Ok(LogFields::Summary),
            _ => Err(crate::Error::Invalid(format!(
                "bad fields {v:?}: expected full or summary"
            ))),
        }
    }
}

/// A log entry for listings: what `git log --oneline` shows and a little
/// more. Its fields match the blame [`ChangesetSummary`] (first line of the
/// message, `agent.*` keys only) plus ancestry and the number of changed
/// paths. On git/git a full entry is about 1.9 KB of JSON, a third each the
/// changed paths and the message, and the rest mostly `git.*` metadata; this
/// is about 440 bytes. The id still names the changeset for the rest.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogSummaryEntry {
    pub id: ChangesetId,
    pub parents: Vec<ChangesetId>,
    pub author: String,
    /// Unix seconds.
    pub timestamp: i64,
    pub message_first_line: String,
    /// Only the `agent.*` keys of the changeset's `extra`.
    #[serde(default)]
    pub agent: BTreeMap<String, String>,
    /// Number of paths in the changeset's `changes`.
    pub files_changed: u64,
    /// See [`ChangesetInfo::generation`].
    pub generation: u64,
    pub root_tree: Hash,
    /// See [`ChangesetInfo::path_at`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path_at: Option<RepoPath>,
    /// See [`ChangesetInfo::renamed`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub renamed: Option<RenameEntry>,
}

impl From<&ChangesetInfo> for LogSummaryEntry {
    fn from(info: &ChangesetInfo) -> Self {
        let cs = &info.changeset;
        LogSummaryEntry {
            id: info.id,
            parents: cs.parents.clone(),
            author: cs.author.clone(),
            timestamp: cs.timestamp,
            message_first_line: cs.message.lines().next().unwrap_or("").to_string(),
            agent: cs
                .extra
                .iter()
                .filter(|(k, _)| k.starts_with("agent."))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            files_changed: cs.changes.len() as u64,
            generation: info.generation,
            root_tree: info.root_tree,
            path_at: info.path_at.clone(),
            renamed: info.renamed.clone(),
        }
    }
}

impl From<ChangesetInfo> for LogSummaryEntry {
    fn from(info: ChangesetInfo) -> Self {
        (&info).into()
    }
}

/// [`LogResponse`] with `fields=summary`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogSummaryResponse {
    pub entries: Vec<LogSummaryEntry>,
    /// See [`LogResponse::next`]; a cursor continues either shape.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next: Option<String>,
    /// See [`LogResponse::answered_at`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub answered_at: Option<ChangesetId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub behind: Option<u64>,
}

impl From<LogResponse> for LogSummaryResponse {
    fn from(log: LogResponse) -> Self {
        LogSummaryResponse {
            entries: log.entries.into_iter().map(Into::into).collect(),
            next: log.next,
            answered_at: log.answered_at,
            behind: log.behind,
        }
    }
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
    /// See [`LogResponse::answered_at`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub answered_at: Option<ChangesetId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub behind: Option<u64>,
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
    /// See [`LogResponse::answered_at`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub answered_at: Option<ChangesetId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub behind: Option<u64>,
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
    Merged {
        id: ChangesetId,
    },
    Conflicts {
        conflicts: Vec<Conflict>,
        /// Changes relative to `parents[0]` for every path that merged
        /// cleanly. Applying these plus a resolution of each conflict on top
        /// of `parents[0]` gives the merge result. Older servers omit it.
        #[serde(default)]
        clean: BTreeMap<RepoPath, FileChange>,
    },
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
    /// May the land create `target_ref` if it does not exist? Absent keeps
    /// the original behaviour for old clients: it creates. `false` makes a
    /// land onto a missing ref a 404 whose message carries
    /// [`crate::refname::MISSING_REF_MARKER`] (ADR 0036).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub create: Option<bool>,
    /// The client's id for this logical land, the same on every retry of it
    /// (ADR 0057). A land whose id `target_ref` already recorded answers
    /// with that land's result, however long ago it landed, and moves
    /// nothing; reusing an id for a different changeset is 400. Absent, a
    /// retry is recognised only while its rebased copy is near the head.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub land_id: Option<LandId>,
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
    /// Files without an index row that were fetched and matched directly
    /// because the index was behind (ADR 0058); omitted when 0.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub files_unindexed: u64,
}

fn is_zero(n: &u64) -> bool {
    *n == 0
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
    /// Cursor for the next page (see "Pagination"); absent when the search
    /// ended and from older servers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next: Option<String>,
    /// See [`LogResponse::answered_at`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub answered_at: Option<ChangesetId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub behind: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn whoami_shape_is_stable() {
        let v = serde_json::to_value(WhoAmIResponse {
            name: Some("ci".into()),
            role: Some("write".into()),
            author: None,
        })
        .unwrap();
        assert_eq!(v, serde_json::json!({ "name": "ci", "role": "write" }));
        let v = serde_json::to_value(WhoAmIResponse {
            name: Some("bo".into()),
            role: Some("write".into()),
            author: Some("Bo <bo@example.com>".into()),
        })
        .unwrap();
        assert_eq!(v["author"], "Bo <bo@example.com>");
        let open: WhoAmIResponse =
            serde_json::from_value(serde_json::json!({ "name": null, "role": null })).unwrap();
        assert_eq!(open, WhoAmIResponse::default());
    }

    #[test]
    fn page_cursors_are_optional_and_omitted_when_absent() {
        let log: LogResponse =
            serde_json::from_value(serde_json::json!({ "entries": [] })).unwrap();
        assert_eq!(log.next, None);
        assert_eq!(
            serde_json::to_value(&log).unwrap(),
            serde_json::json!({ "entries": [] })
        );
        let log = LogResponse {
            entries: vec![],
            next: Some("abc".into()),
            answered_at: None,
            behind: None,
        };
        assert_eq!(
            serde_json::to_value(&log).unwrap(),
            serde_json::json!({ "entries": [], "next": "abc" })
        );
        let reflog: RefLogResponse =
            serde_json::from_value(serde_json::json!({ "entries": [], "next": "x" })).unwrap();
        assert_eq!(reflog.next.as_deref(), Some("x"));
        let search = SearchResponse::default();
        assert!(serde_json::to_value(&search).unwrap().get("next").is_none());
    }

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
    fn ref_log_shapes_are_stable_and_old_cas_bodies_parse() {
        let id = ChangesetId(Hash::of_blob(b"x"));
        let r: CasRefRequest = serde_json::from_value(serde_json::json!({
            "expected": null, "new": id.to_string()
        }))
        .unwrap();
        assert_eq!((r.operation, r.message), (None, None));
        let v = serde_json::to_value(CasRefRequest {
            expected: None,
            new: Some(id),
            ..Default::default()
        })
        .unwrap();
        assert_eq!(v.as_object().unwrap().len(), 2, "unset fields are omitted");

        let e = RefLogEntry {
            seq: 4,
            name: "main".into(),
            old: Some(id),
            new: None,
            time: 1_760_000_000,
            actor: None,
            operation: "undo".into(),
            message: Some("undo of 3".into()),
            land_id: None,
            land_changeset: None,
        };
        let v = serde_json::to_value(RefLogResponse {
            entries: vec![e.clone()],
            next: None,
        })
        .unwrap();
        assert_eq!(
            v,
            serde_json::json!({ "entries": [{
                "seq": 4, "name": "main", "old": id.to_string(), "new": null,
                "time": 1_760_000_000, "actor": null, "operation": "undo",
                "message": "undo of 3"
            }]})
        );
        let back: RefLogResponse = serde_json::from_value(v).unwrap();
        assert_eq!(back.entries, std::slice::from_ref(&e));

        // A land that named itself carries its id and changeset.
        let land = RefLogEntry {
            operation: "land".into(),
            message: None,
            new: Some(id),
            land_id: Some("00112233445566778899aabbccddeeff".parse().unwrap()),
            land_changeset: Some(id),
            ..e
        };
        let v = serde_json::to_value(&land).unwrap();
        assert_eq!(v["land_id"], "00112233445566778899aabbccddeeff");
        assert_eq!(v["land_changeset"], id.to_string());
        assert_eq!(serde_json::from_value::<RefLogEntry>(v).unwrap(), land);
    }

    #[test]
    fn request_ids_are_a_safe_charset_up_to_128_bytes() {
        for ok in ["a", "0123-abc_DEF.x", "run:7/step.2", &"x".repeat(128)] {
            assert!(valid_request_id(ok), "{ok:?}");
        }
        for bad in [
            "",
            " ",
            "with space",
            "x status=200 route=/v1/land",
            "q\"uote",
            "a=b",
            &"x".repeat(129),
            "tab\there",
            "new\nline",
            "caf\u{e9}",
            "\u{7f}",
        ] {
            assert!(!valid_request_id(bad), "{bad:?}");
        }
    }

    #[test]
    fn prefix_shapes_are_stable() {
        let id = ChangesetId(Hash::of_blob(b"p"));
        let v = serde_json::to_value(ResolvePrefixResponse { id }).unwrap();
        assert_eq!(v, serde_json::json!({ "id": id.to_string() }));
        let amb = AmbiguousPrefixResponse {
            error: "ambiguous".into(),
            candidates: vec![id],
            truncated: false,
            code: None,
        };
        let v = serde_json::to_value(&amb).unwrap();
        assert_eq!(
            v,
            serde_json::json!({ "error": "ambiguous", "candidates": [id.to_string()], "truncated": false })
        );
        // An old client reads it as a plain error.
        let e: ErrorResponse = serde_json::from_value(v).unwrap();
        assert_eq!(e.error, "ambiguous");
        let back: AmbiguousPrefixResponse =
            serde_json::from_value(serde_json::json!({ "error": "x", "candidates": [] })).unwrap();
        assert!(!back.truncated);
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
        assert_eq!(req.create, None, "old clients send no create");
        let v = serde_json::to_value(&req).unwrap();
        assert_eq!(
            v.as_object().unwrap().len(),
            2,
            "an unset create is omitted"
        );
        assert_eq!(req.land_id, None, "old clients send no land id");
        let req = LandRequest {
            create: Some(false),
            ..req
        };
        assert_eq!(serde_json::to_value(&req).unwrap()["create"], false);
        let hex = "00112233445566778899aabbccddeeff";
        let req: LandRequest = serde_json::from_value(
            serde_json::json!({ "changeset": id.to_string(), "land_id": hex }),
        )
        .unwrap();
        assert_eq!(req.land_id.as_ref().unwrap().to_string(), hex);
        assert_eq!(serde_json::to_value(&req).unwrap()["land_id"], hex);
        for bad in [
            "00",
            "zz112233445566778899aabbccddeeff",
            &hex.to_uppercase(),
            &"ab".repeat(33),
        ] {
            assert!(
                serde_json::from_value::<LandRequest>(
                    serde_json::json!({ "changeset": id.to_string(), "land_id": bad }),
                )
                .is_err(),
                "{bad}"
            );
        }
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
    fn log_summary_is_a_projection_of_the_full_entry() {
        let mut changes = BTreeMap::new();
        for p in ["a/b.rs", "c"] {
            changes.insert(RepoPath::new(p).unwrap(), FileChange::Delete);
        }
        let info = ChangesetInfo {
            id: ChangesetId(Hash::of_blob(b"id")),
            changeset: Changeset {
                parents: vec![ChangesetId(Hash::of_blob(b"p"))],
                author: "Ann".into(),
                timestamp: 7,
                message: "subject\n\nbody\n".into(),
                changes,
                extra: BTreeMap::from([
                    ("agent.model".into(), "m".into()),
                    ("git.commit".into(), "c".into()),
                ]),
            },
            generation: 3,
            root_tree: Hash::of_blob(b"t"),
            path_at: Some(RepoPath::new("a/b.rs").unwrap()),
            renamed: None,
        };
        let s = LogSummaryEntry::from(&info);
        assert_eq!(s, LogSummaryEntry::from(info.clone()));
        let full = serde_json::to_value(&info).unwrap();
        assert_eq!(
            serde_json::to_value(&s).unwrap(),
            serde_json::json!({
                "id": full["id"],
                "parents": full["changeset"]["parents"],
                "author": "Ann",
                "timestamp": 7,
                "message_first_line": "subject",
                "agent": { "agent.model": "m" },
                "files_changed": 2,
                "generation": 3,
                "root_tree": full["root_tree"],
                "path_at": "a/b.rs",
            })
        );
        let mut info2 = info.clone();
        info2.changeset.message = String::new();
        assert_eq!(LogSummaryEntry::from(&info2).message_first_line, "");
        // `agent` may be left out.
        let mut v = serde_json::to_value(&s).unwrap();
        v.as_object_mut().unwrap().remove("agent");
        let back: LogSummaryEntry = serde_json::from_value(v).unwrap();
        assert!(back.agent.is_empty());

        let log = LogResponse {
            entries: vec![info],
            next: Some("c".into()),
            answered_at: None,
            behind: Some(2),
        };
        let sum = LogSummaryResponse::from(log);
        assert_eq!(sum.entries, [s]);
        assert_eq!((sum.next.as_deref(), sum.behind), (Some("c"), Some(2)));

        assert_eq!(LogFields::parse("full").unwrap(), LogFields::Full);
        assert_eq!(LogFields::parse("summary").unwrap(), LogFields::Summary);
        assert!(LogFields::parse("Summary").is_err());
        assert!(LogFields::parse("").is_err());
    }

    #[test]
    fn indexing_fields_are_additive() {
        let id = ChangesetId(Hash::of_blob(b"z"));
        // Older bodies decode, and the new fields are omitted when unset.
        let log: LogResponse =
            serde_json::from_value(serde_json::json!({ "entries": [] })).unwrap();
        assert_eq!((log.answered_at, log.behind), (None, None));
        let log = LogResponse {
            entries: vec![],
            next: None,
            answered_at: Some(id),
            behind: Some(7),
        };
        assert_eq!(
            serde_json::to_value(&log).unwrap(),
            serde_json::json!({ "entries": [], "answered_at": id.to_string(), "behind": 7 })
        );
        let err = ErrorResponse::new("x");
        assert_eq!(
            serde_json::to_value(&err).unwrap(),
            serde_json::json!({ "error": "x" })
        );
        let err = ErrorResponse {
            error: "indexing: ...".into(),
            indexing: Some(IndexingInfo {
                kind: "history-tree-v4".into(),
                behind: 5210,
                retry_after_ms: 4000,
            }),
            code: Some(error_code::INDEXING.into()),
            request_id: None,
        };
        assert_eq!(
            serde_json::to_value(&err).unwrap(),
            serde_json::json!({
                "error": "indexing: ...",
                "indexing": { "kind": "history-tree-v4", "behind": 5210, "retry_after_ms": 4000 },
                "code": "indexing"
            })
        );
        let mut with_id = ErrorResponse::new("x").with_code(error_code::NOT_FOUND);
        with_id.request_id = Some("r-1".into());
        assert_eq!(
            serde_json::to_value(&with_id).unwrap(),
            serde_json::json!({ "error": "x", "code": "not_found", "request_id": "r-1" })
        );
        let old: ErrorResponse =
            serde_json::from_value(serde_json::json!({ "error": "e" })).unwrap();
        assert_eq!(old.indexing, None);
        assert_eq!(old.code, None);
        assert_eq!(
            serde_json::to_value(ErrorResponse::new("x").with_code(error_code::CAS_LOST)).unwrap(),
            serde_json::json!({ "error": "x", "code": "cas_lost" })
        );
        let stats = SearchStats::default();
        assert!(serde_json::to_value(&stats)
            .unwrap()
            .get("files_unindexed")
            .is_none());
        let stats = SearchStats {
            files_unindexed: 3,
            ..SearchStats::default()
        };
        assert_eq!(serde_json::to_value(&stats).unwrap()["files_unindexed"], 3);
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
            answered_at: None,
            behind: None,
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
