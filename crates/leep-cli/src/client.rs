//! Blocking HTTP client wrapping every endpoint in `leep_core::api`.
//!
//! Every failed request becomes a [`CliError`] whose kind follows the HTTP
//! status (ADR 0034): no connection is `Unreachable`, 5xx `Server`, 408
//! `Timeout`, 429 `RateLimited`, 404 `NotFound`, 409 `Conflict` (retryable),
//! 401 and 403 `Auth`, any other 4xx `User`. `ErrorResponse` bodies become the
//! error message. 401 and 403 get fixed, token-free messages instead.
//!
//! A 429 from the server's request limits (ADR 0020) is retried for GET
//! requests only, honoring `Retry-After`; anything else fails at once, since a
//! blind retry of a write could repeat it.
//!
//! Index-backed queries (log, last-change, blame, search) send
//! `Leep-Indexing: retry` (ADR 0058): a server whose index lags answers 503
//! with `Retry-After`, which is retried, saying so on stderr, for up to
//! `LEEP_INDEX_WAIT` seconds (60) in all, then reported as `indexing`.
//! With `--stale-ok` they send `stale-ok` and take an answer at an older
//! changeset instead.

use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use leep_core::api::*;
use leep_core::{Changeset, ChangesetId, FileKind, Hash};
use reqwest::blocking::{Client as Http, RequestBuilder, Response};
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, RETRY_AFTER};
use reqwest::StatusCode;
use serde::de::DeserializeOwned;

use crate::error::{CliError, ErrorKind, Result};

/// Tries per GET before a 429 is reported.
const MAX_ATTEMPTS: u32 = 4;
/// Times a land with a land id is sent again after an answer that may not
/// have been the server's last word (ADR 0057).
const LAND_RESENDS: u32 = 3;
/// Longest pause between tries. A longer `Retry-After` means the server wants
/// the caller gone for a while, so fail with that advice instead of hanging.
const MAX_RETRY_WAIT: Duration = Duration::from_secs(10);

#[derive(Clone)]
pub struct Client {
    base: String,
    http: Http,
    has_token: bool,
    /// Sent as `X-Request-Id` on every request; shared by clones, so
    /// [`Client::new_request_id`] renames all of them.
    request_id: Arc<Mutex<String>>,
    /// Index-backed queries accept an answer at an older changeset.
    stale_ok: bool,
    /// How long, and how many times, index-backed queries retry an
    /// indexing 503: `LEEP_INDEX_WAIT` and unlimited unless set.
    index_retry: Option<(Duration, u32)>,
}

/// How long index-backed queries keep retrying an indexing 503, unless
/// `LEEP_INDEX_WAIT` (seconds) says otherwise.
const INDEX_WAIT: Duration = Duration::from_secs(60);

fn index_wait() -> Duration {
    std::env::var("LEEP_INDEX_WAIT")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .map_or(INDEX_WAIT, Duration::from_secs)
}

/// Parameters of `GET /v1/log/{id}`; `extra` entries are `key:value` or `key`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LogQuery {
    pub path: Option<String>,
    pub limit: Option<u32>,
    pub author: Option<String>,
    pub extra: Vec<String>,
    /// Continue a file's history across renames (needs a file `path`).
    pub follow: bool,
    /// The previous page's `next` cursor.
    pub after: Option<String>,
}

impl LogQuery {
    /// Query-string pairs in request order; `extra` repeats.
    pub fn params(&self) -> Vec<(&'static str, String)> {
        let mut out = Vec::new();
        if let Some(p) = &self.path {
            out.push(("path", p.clone()));
        }
        if let Some(n) = self.limit {
            out.push(("limit", n.to_string()));
        }
        if let Some(a) = &self.author {
            out.push(("author", a.clone()));
        }
        out.extend(self.extra.iter().map(|e| ("extra", e.clone())));
        if self.follow {
            out.push(("follow", "true".into()));
        }
        if let Some(a) = &self.after {
            out.push(("after", a.clone()));
        }
        out
    }

    /// [`LogQuery::params`] plus the entry shape: only [`Client::log`] and
    /// [`Client::log_summary`] choose it, so each always gets the shape it
    /// decodes.
    fn params_for(&self, summary: bool) -> Vec<(&'static str, String)> {
        let mut out = self.params();
        if summary {
            out.push(("fields", "summary".into()));
        }
        out
    }
}

/// A summary log as either shape decodes: servers from before
/// `fields=summary` ignore it and send full entries, which are projected.
#[derive(serde::Deserialize)]
struct AnyLog {
    entries: Vec<AnyEntry>,
    #[serde(default)]
    next: Option<String>,
    #[serde(default)]
    answered_at: Option<ChangesetId>,
    #[serde(default)]
    behind: Option<u64>,
}

#[derive(serde::Deserialize)]
#[serde(untagged)]
enum AnyEntry {
    Summary(LogSummaryEntry),
    Full(ChangesetInfo),
}

impl From<AnyLog> for LogSummaryResponse {
    fn from(log: AnyLog) -> Self {
        LogSummaryResponse {
            entries: log
                .entries
                .into_iter()
                .map(|e| match e {
                    AnyEntry::Summary(s) => s,
                    AnyEntry::Full(f) => f.into(),
                })
                .collect(),
            next: log.next,
            answered_at: log.answered_at,
            behind: log.behind,
        }
    }
}

/// One log page: entries, `next`, `answered_at`, `behind`.
type LogPageParts<E> = (Vec<E>, Option<String>, Option<ChangesetId>, Option<u64>);

/// Up to `want` entries of the log `q` asks for, from pages that `fetch`
/// reads, each continuing at the previous one's cursor.
fn pages<E>(
    q: &LogQuery,
    want: usize,
    fetch: impl Fn(&LogQuery) -> Result<LogPageParts<E>>,
) -> Result<LogPageParts<E>> {
    let mut q = q.clone();
    let mut out: LogPageParts<E> = (Vec::new(), None, None, None);
    loop {
        let left = want - out.0.len();
        q.limit = Some(left.min(LOG_PAGE as usize) as u32);
        let (entries, next, answered_at, behind) = fetch(&q)?;
        out.0.extend(entries);
        out.1 = next;
        // Later pages continue the walk the first one pinned.
        if out.2.is_none() {
            out.2 = answered_at;
            out.3 = behind;
        }
        if out.0.len() >= want || out.1.is_none() {
            return Ok(out);
        }
        q.after = out.1.clone();
    }
}

/// Parameters of `GET /v1/search/{rev}`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SearchQuery {
    pub query: String,
    pub regex: bool,
    pub ignore_case: bool,
    pub path: Option<String>,
    pub glob: Option<String>,
    pub limit: Option<u32>,
    /// The previous page's `next` cursor.
    pub after: Option<String>,
}

impl SearchQuery {
    /// Query-string pairs; flags are sent only when set.
    pub fn params(&self) -> Vec<(&'static str, String)> {
        let mut out = vec![("q", self.query.clone())];
        if self.regex {
            out.push(("regex", "true".into()));
        }
        if self.ignore_case {
            out.push(("i", "true".into()));
        }
        if let Some(p) = &self.path {
            out.push(("path", p.clone()));
        }
        if let Some(g) = &self.glob {
            out.push(("glob", g.clone()));
        }
        if let Some(n) = self.limit {
            out.push(("limit", n.to_string()));
        }
        if let Some(a) = &self.after {
            out.push(("after", a.clone()));
        }
        out
    }
}

/// Most log entries the server returns per request.
pub const LOG_PAGE: u32 = 10_000;

/// Most search matches the server returns per request.
pub const SEARCH_PAGE: u32 = 1000;

fn add_stats(total: &mut SearchStats, page: &SearchStats) {
    total.files_considered += page.files_considered;
    total.dirs_pruned += page.dirs_pruned;
    total.files_pruned += page.files_pruned;
    total.files_scanned += page.files_scanned;
    total.files_skipped += page.files_skipped;
    total.bytes_scanned += page.bytes_scanned;
    total.blob_rows_derived += page.blob_rows_derived;
    total.tree_rows_derived += page.tree_rows_derived;
    total.indexed = page.indexed;
}

/// Percent-encode a URL path, keeping `/` so ref names like `team/x` work.
fn enc(s: &str) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' | b'/' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

impl Client {
    pub fn new(server: &str) -> Self {
        Self::with_token(server, None)
    }

    /// Send `Authorization: Bearer <token>` on every request. The header is
    /// marked sensitive so it is redacted from debug output.
    pub fn with_token(server: &str, token: Option<&str>) -> Self {
        let mut builder = Http::builder().timeout(Duration::from_secs(300));
        if let Some(t) = token {
            if let Ok(mut v) = HeaderValue::from_str(&format!("Bearer {t}")) {
                v.set_sensitive(true);
                let mut h = HeaderMap::new();
                h.insert(AUTHORIZATION, v);
                builder = builder.default_headers(h);
            }
        }
        Client {
            base: server.trim_end_matches('/').to_string(),
            http: builder.build().expect("http client builds"),
            has_token: token.is_some(),
            request_id: Arc::new(Mutex::new(invocation_request_id().to_string())),
            stale_ok: false,
            index_retry: None,
        }
    }

    /// Retry an indexing 503 at most `retries` times within `within`.
    pub fn with_index_retries(mut self, within: Duration, retries: u32) -> Self {
        self.index_retry = Some((within, retries));
        self
    }

    /// Ask index-backed queries for an answer at the newest indexed
    /// changeset when the index lags (`Leep-Indexing: stale-ok`), instead
    /// of waiting for it.
    pub fn with_stale_ok(mut self, yes: bool) -> Self {
        self.stale_ok = yes;
        self
    }

    /// An index-backed query: sent with `Leep-Indexing`, and retried while
    /// the server answers that its index is behind, for at most
    /// `LEEP_INDEX_WAIT` in all.
    fn indexed<T: DeserializeOwned>(&self, make: impl Fn() -> RequestBuilder) -> Result<T> {
        let value = if self.stale_ok {
            INDEXING_STALE_OK
        } else {
            INDEXING_RETRY
        };
        let started = std::time::Instant::now();
        let (limit, mut retries) = self.index_retry.unwrap_or((index_wait(), u32::MAX));
        loop {
            match self.json(make().header(INDEXING_HEADER, value)) {
                Err(e) if e.kind() == ErrorKind::Indexing => {
                    let wait = Duration::from_millis(e.retry_after_ms().unwrap_or(1000))
                        .clamp(Duration::from_millis(100), MAX_RETRY_WAIT)
                        .min(limit.saturating_sub(started.elapsed()));
                    if retries == 0 || wait.is_zero() {
                        return Err(e);
                    }
                    retries -= 1;
                    eprintln!(
                        "{}, retrying in {}s",
                        e.message(),
                        wait.as_secs_f64().ceil()
                    );
                    std::thread::sleep(wait);
                }
                r => return r,
            }
        }
    }

    pub fn server(&self) -> &str {
        &self.base
    }

    /// The `X-Request-Id` this client sends: one per process (a CLI
    /// invocation) unless [`Client::new_request_id`] changed it.
    pub fn request_id(&self) -> String {
        self.request_id.lock().unwrap().clone()
    }

    /// Send a fresh request id from now on, in this client and its clones:
    /// for a long-running process that makes one call after another.
    pub fn new_request_id(&self) {
        *self.request_id.lock().unwrap() = random_request_id();
    }

    fn named(&self, req: RequestBuilder) -> RequestBuilder {
        req.header(REQUEST_ID_HEADER, self.request_id())
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.base, path)
    }

    fn send(&self, req: RequestBuilder) -> Result<Response> {
        let (http, built) = self.named(req).build_split();
        let built = built.map_err(|e| CliError::user(format!("bad request: {e}")))?;
        let idempotent = built.method() == reqwest::Method::GET;
        let mut current = built;
        let mut attempt = 1;
        loop {
            let spare = if idempotent {
                current.try_clone()
            } else {
                None
            };
            let resp = http.execute(current).map_err(|e| self.unreachable(e))?;
            let status = resp.status();
            if status.is_success() {
                return Ok(resp);
            }
            if let (StatusCode::TOO_MANY_REQUESTS, Some(next), true) =
                (status, spare, attempt < MAX_ATTEMPTS)
            {
                if let Some(wait) = retry_after(&resp).filter(|w| *w <= MAX_RETRY_WAIT) {
                    std::thread::sleep(wait);
                    attempt += 1;
                    current = next;
                    continue;
                }
            }
            return Err(self.error_from(resp));
        }
    }

    fn error_from(&self, resp: Response) -> CliError {
        let sent = self.request_id();
        error_from(resp, self.has_token, &sent)
    }

    /// A request that never got an answer. A timeout may have taken effect;
    /// a refused connection did not.
    fn unreachable(&self, e: reqwest::Error) -> CliError {
        let kind = if e.is_timeout() {
            ErrorKind::Timeout
        } else {
            ErrorKind::Unreachable
        };
        CliError::new(kind, format!("cannot reach {}: {e}", self.base))
            .with_hint(
                "check that leep-server runs at that URL; it comes from --server, \
                 LEEP_SERVER or .leep/config.json",
            )
            .with_request_id(Some(self.request_id()))
    }

    fn json<T: DeserializeOwned>(&self, req: RequestBuilder) -> Result<T> {
        let resp = self.send(req)?;
        resp.json::<T>().map_err(bad_response)
    }

    /// The features `GET /v1/ready` advertises, read from a 200 or a 503
    /// (stores down); none for a server too old to name any (no field, or no
    /// `/v1/ready`). A server that cannot be asked is an error, so a client
    /// never mistakes an outage for an old server.
    pub fn server_features(&self) -> Result<Vec<String>> {
        let resp = self
            .named(self.http.get(self.url("/v1/ready")))
            .send()
            .map_err(|e| self.unreachable(e))?;
        match resp.status() {
            StatusCode::OK | StatusCode::SERVICE_UNAVAILABLE => Ok(resp
                .json::<ReadyResponse>()
                .map_or_else(|_| Vec::new(), |r| r.features)),
            StatusCode::NOT_FOUND => Ok(Vec::new()),
            _ => Err(self.error_from(resp)),
        }
    }

    pub fn health(&self) -> Result<()> {
        self.send(self.http.get(self.url("/v1/health"))).map(|_| ())
    }

    pub fn put_blob(&self, hash: &Hash, data: Vec<u8>) -> Result<()> {
        self.send(
            self.http
                .put(self.url(&format!("/v1/blobs/{hash}")))
                .body(data),
        )
        .map(|_| ())
    }

    pub fn get_blob(&self, hash: &Hash) -> Result<Vec<u8>> {
        let resp = self.send(self.http.get(self.url(&format!("/v1/blobs/{hash}"))))?;
        resp.bytes()
            .map(|b| b.to_vec())
            .map_err(|e| CliError::server(format!("download of blob {hash} failed: {e}")))
    }

    /// Asks in requests of [`MISSING_BLOBS_CHUNK`] hashes, well under the
    /// server's cap and a read token's body limit.
    pub fn missing_blobs(&self, hashes: Vec<Hash>) -> Result<Vec<Hash>> {
        let mut missing = Vec::new();
        for chunk in hashes.chunks(MISSING_BLOBS_CHUNK) {
            let r: MissingBlobsResponse =
                self.json(self.http.post(self.url("/v1/blobs/missing")).json(
                    &MissingBlobsRequest {
                        hashes: chunk.to_vec(),
                    },
                ))?;
            missing.extend(r.missing);
        }
        Ok(missing)
    }

    pub fn create_changeset(&self, cs: &Changeset) -> Result<ChangesetId> {
        let r: CreateChangesetResponse =
            self.json(self.http.post(self.url("/v1/changesets")).json(cs))?;
        Ok(r.id)
    }

    pub fn get_changeset(&self, id: &ChangesetId) -> Result<ChangesetInfo> {
        self.json(self.http.get(self.url(&format!("/v1/changesets/{id}"))))
    }

    /// Attest to a changeset with an already-signed request.
    pub fn add_signature(
        &self,
        id: &ChangesetId,
        req: &AddSignatureRequest,
    ) -> Result<AddSignatureResponse> {
        self.json(
            self.http
                .post(self.url(&format!("/v1/changesets/{id}/signatures")))
                .json(req),
        )
    }

    pub fn signatures(&self, id: &ChangesetId) -> Result<SignaturesResponse> {
        self.json(
            self.http
                .get(self.url(&format!("/v1/changesets/{id}/signatures"))),
        )
    }

    /// Record the caller's verdict; the server names the reviewer from the token.
    pub fn add_review(
        &self,
        id: &ChangesetId,
        req: &AddReviewRequest,
    ) -> Result<AddReviewResponse> {
        self.json(
            self.http
                .post(self.url(&format!("/v1/changesets/{id}/reviews")))
                .json(req),
        )
    }

    pub fn reviews(&self, id: &ChangesetId) -> Result<ReviewsResponse> {
        self.json(
            self.http
                .get(self.url(&format!("/v1/changesets/{id}/reviews"))),
        )
    }

    pub fn review_status(
        &self,
        id: &ChangesetId,
        target_ref: &str,
    ) -> Result<ReviewStatusResponse> {
        self.json(
            self.http
                .get(self.url(&format!("/v1/changesets/{id}/review-status")))
                .query(&[("ref", target_ref)]),
        )
    }

    pub fn whoami(&self) -> Result<WhoAmIResponse> {
        self.json(self.http.get(self.url("/v1/whoami")))
    }

    pub fn refs(&self) -> Result<RefsResponse> {
        self.json(self.http.get(self.url("/v1/refs")))
    }

    pub fn get_ref(&self, name: &str) -> Result<RefResponse> {
        self.json(self.http.get(self.url(&format!("/v1/refs/{}", enc(name)))))
    }

    /// Ref log entries, newest first; `name = None` covers every ref and
    /// `before` returns entries with a smaller `seq`.
    pub fn ref_log(
        &self,
        name: Option<&str>,
        before: Option<u64>,
        limit: usize,
    ) -> Result<RefLogResponse> {
        self.ref_log_page(name, before, None, limit)
    }

    /// [`Client::ref_log`] that can also resume from a page's `next` cursor
    /// (`after`); the server refuses `before` and `after` together.
    pub fn ref_log_page(
        &self,
        name: Option<&str>,
        before: Option<u64>,
        after: Option<&str>,
        limit: usize,
    ) -> Result<RefLogResponse> {
        let url = match name {
            Some(n) => self.url(&format!("/v1/reflog/{}", enc(n))),
            None => self.url("/v1/reflog"),
        };
        let mut query = vec![("limit", limit.to_string())];
        if let Some(b) = before {
            query.push(("before", b.to_string()));
        }
        if let Some(a) = after {
            query.push(("after", a.to_string()));
        }
        self.json(self.http.get(url).query(&query))
    }

    /// Compare-and-swap a ref. A lost race (409) is `ok: false`, not an error.
    pub fn cas_ref(&self, name: &str, req: &CasRefRequest) -> Result<CasRefResponse> {
        let resp = self
            .named(
                self.http
                    .post(self.url(&format!("/v1/refs/{}", enc(name))))
                    .json(req),
            )
            .send()
            .map_err(|e| self.unreachable(e))?;
        if resp.status().is_success() || resp.status() == StatusCode::CONFLICT {
            return resp.json().map_err(bad_response);
        }
        Err(self.error_from(resp))
    }

    pub fn resolve(&self, rev: &str) -> Result<ChangesetId> {
        let r: ResolveResponse = self.json(
            self.http
                .get(self.url(&format!("/v1/resolve/{}", enc(rev)))),
        )?;
        Ok(r.id)
    }

    /// The one changeset whose id starts with `prefix`. Several matches are
    /// an `Ambiguous` error carrying the candidates (ADR 0035).
    pub fn resolve_prefix(&self, prefix: &str) -> Result<ChangesetId> {
        let r: ResolvePrefixResponse = self.json(
            self.http
                .get(self.url(&format!("/v1/resolve-prefix/{}", enc(prefix)))),
        )?;
        Ok(r.id)
    }

    pub fn tree(&self, id: &ChangesetId, path: &str) -> Result<TreeResponse> {
        self.json(
            self.http
                .get(self.url(&format!("/v1/tree/{id}")))
                .query(&[("path", path)]),
        )
    }

    /// Recursive listing; an empty `prefix` lists the whole repository.
    pub fn files(&self, id: &ChangesetId, prefix: &str) -> Result<FilesResponse> {
        let mut req = self.http.get(self.url(&format!("/v1/files/{id}")));
        if !prefix.is_empty() {
            req = req.query(&[("prefix", prefix)]);
        }
        self.json(req)
    }

    /// File bytes plus the `x-leep-kind` header, if the server sent one.
    pub fn file(&self, id: &ChangesetId, path: &str) -> Result<(Vec<u8>, Option<FileKind>)> {
        let resp = self.send(
            self.http
                .get(self.url(&format!("/v1/file/{id}")))
                .query(&[("path", path)]),
        )?;
        let kind = resp
            .headers()
            .get("x-leep-kind")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| serde_json::from_value(serde_json::Value::String(s.to_string())).ok());
        let bytes = resp
            .bytes()
            .map_err(|e| CliError::server(format!("download failed: {e}")))?;
        Ok((bytes.to_vec(), kind))
    }

    pub fn log(&self, id: &ChangesetId, q: &LogQuery) -> Result<LogResponse> {
        let params = q.params_for(false);
        self.indexed(|| {
            self.http
                .get(self.url(&format!("/v1/log/{id}")))
                .query(&params)
        })
    }

    /// [`Client::log`] without each changeset's changed paths, which a
    /// listing does not show and which make most of a full log's bytes.
    pub fn log_summary(&self, id: &ChangesetId, q: &LogQuery) -> Result<LogSummaryResponse> {
        let params = q.params_for(true);
        let any: AnyLog = self.indexed(|| {
            self.http
                .get(self.url(&format!("/v1/log/{id}")))
                .query(&params)
        })?;
        Ok(any.into())
    }

    /// Up to `want` log entries, following page cursors past the server's
    /// per-request cap. `next` is set if the log goes on. A server without
    /// cursors answers one page.
    pub fn log_all(&self, id: &ChangesetId, q: &LogQuery, want: usize) -> Result<LogResponse> {
        let (entries, next, answered_at, behind) = pages(q, want, |q| {
            let p = self.log(id, q)?;
            Ok((p.entries, p.next, p.answered_at, p.behind))
        })?;
        Ok(LogResponse {
            entries,
            next,
            answered_at,
            behind,
        })
    }

    /// [`Client::log_all`] with summary entries.
    pub fn log_summary_all(
        &self,
        id: &ChangesetId,
        q: &LogQuery,
        want: usize,
    ) -> Result<LogSummaryResponse> {
        let (entries, next, answered_at, behind) = pages(q, want, |q| {
            let p = self.log_summary(id, q)?;
            Ok((p.entries, p.next, p.answered_at, p.behind))
        })?;
        Ok(LogSummaryResponse {
            entries,
            next,
            answered_at,
            behind,
        })
    }

    /// Newest change to a file or directory, from the server's path index.
    pub fn last_change(&self, id: &ChangesetId, path: &str) -> Result<LastChangeResponse> {
        self.indexed(|| {
            self.http
                .get(self.url(&format!("/v1/last-change/{id}")))
                .query(&[("path", path)])
        })
    }

    pub fn provenance(
        &self,
        id: &ChangesetId,
        path: Option<&str>,
        limit: Option<u32>,
    ) -> Result<ProvenanceResponse> {
        let mut req = self.http.get(self.url(&format!("/v1/provenance/{id}")));
        if let Some(p) = path {
            req = req.query(&[("path", p)]);
        }
        if let Some(n) = limit {
            req = req.query(&[("limit", n.to_string())]);
        }
        self.json(req)
    }

    pub fn search(&self, id: &ChangesetId, q: &SearchQuery) -> Result<SearchResponse> {
        self.indexed(|| {
            self.http
                .get(self.url(&format!("/v1/search/{id}")))
                .query(&q.params())
        })
    }

    /// Up to `want` matches, following page cursors past the server's
    /// per-request cap. Stats add up over the pages; `truncated` and `next`
    /// are the last page's.
    pub fn search_all(
        &self,
        id: &ChangesetId,
        q: &SearchQuery,
        want: usize,
    ) -> Result<SearchResponse> {
        let mut q = q.clone();
        let mut out = SearchResponse::default();
        loop {
            let left = want - out.matches.len();
            q.limit = Some(left.min(SEARCH_PAGE as usize) as u32);
            let page = self.search(id, &q)?;
            out.matches.extend(page.matches);
            add_stats(&mut out.stats, &page.stats);
            out.truncated = page.truncated;
            out.next = page.next;
            if out.answered_at.is_none() {
                out.answered_at = page.answered_at;
                out.behind = page.behind;
            }
            if out.matches.len() >= want || out.next.is_none() {
                return Ok(out);
            }
            q.after = out.next.clone();
        }
    }

    pub fn diff(
        &self,
        from: Option<&ChangesetId>,
        to: &ChangesetId,
        prefix: Option<&str>,
    ) -> Result<DiffResponse> {
        let mut req = self
            .http
            .get(self.url("/v1/diff"))
            .query(&[("to", to.to_string())]);
        if let Some(f) = from {
            req = req.query(&[("from", f.to_string())]);
        }
        if let Some(p) = prefix {
            req = req.query(&[("prefix", p)]);
        }
        self.json(req)
    }

    /// [`Client::diff`] asking the server to detect renames. `threshold` is
    /// 0 to 100; `None` leaves the server's default of 50.
    pub fn diff_renames(
        &self,
        from: Option<&ChangesetId>,
        to: &ChangesetId,
        prefix: Option<&str>,
        threshold: Option<u8>,
    ) -> Result<DiffResponse> {
        let mut req = self
            .http
            .get(self.url("/v1/diff"))
            .query(&[("to", to.to_string()), ("renames", "true".into())]);
        if let Some(f) = from {
            req = req.query(&[("from", f.to_string())]);
        }
        if let Some(p) = prefix {
            req = req.query(&[("prefix", p)]);
        }
        if let Some(t) = threshold {
            req = req.query(&[("rename_threshold", t.to_string())]);
        }
        self.json(req)
    }

    pub fn blame(&self, id: &ChangesetId, path: &str) -> Result<BlameResponse> {
        self.indexed(|| {
            self.http
                .get(self.url(&format!("/v1/blame/{id}")))
                .query(&[("path", path)])
        })
    }

    pub fn merge_base(&self, a: &ChangesetId, b: &ChangesetId) -> Result<MergeBaseResponse> {
        self.json(
            self.http
                .get(self.url("/v1/merge-base"))
                .query(&[("a", a.to_string()), ("b", b.to_string())]),
        )
    }

    pub fn merge(&self, req: &MergeRequest) -> Result<MergeResponse> {
        self.json(self.http.post(self.url("/v1/merge")).json(req))
    }

    pub fn rebase(&self, req: &RebaseRequest) -> Result<RebaseResponse> {
        self.json(self.http.post(self.url("/v1/rebase")).json(req))
    }

    pub fn land(&self, req: &LandRequest) -> Result<LandResponse> {
        self.json(self.http.post(self.url("/v1/land")).json(req))
    }

    /// [`land`](Self::land), sent again with the same land id after a
    /// timeout, a lost connection, a 5xx or a lost race. The id makes a
    /// repeat answer with the first one's result if that one landed, so a
    /// resend never lands twice. Without an id it sends once.
    pub fn land_resending(&self, req: &LandRequest) -> Result<LandResponse> {
        let mut resends = 0;
        loop {
            match self.land(req) {
                Err(e) if req.land_id.is_some() && resends < LAND_RESENDS && resend_land(&e) => {
                    std::thread::sleep(Duration::from_millis(500 << resends));
                    resends += 1;
                }
                r => return r,
            }
        }
    }
}

/// An error after which the land may or may not have happened, or may
/// succeed if sent again. `Conflict` here is only the 409 of a land that lost
/// the race too often; a land with conflicts is a 200 answer, never resent.
fn resend_land(e: &CliError) -> bool {
    e.is_retryable()
        && matches!(
            e.kind(),
            ErrorKind::Timeout | ErrorKind::Server | ErrorKind::Unreachable | ErrorKind::Conflict
        )
}

/// A new random land id (ADR 0057): one per logical land, reused on every
/// retry of it.
/// A new random request id: 32 lowercase hex digits.
fn random_request_id() -> String {
    let mut b = [0u8; 16];
    getrandom::fill(&mut b).expect("the OS random source is available");
    hex::encode(b)
}

/// The request id of this process, made on first use: every request of one
/// CLI invocation carries it, so the server's log lines for the invocation
/// share it.
pub fn invocation_request_id() -> &'static str {
    static ID: OnceLock<String> = OnceLock::new();
    ID.get_or_init(random_request_id)
}

pub fn new_land_id() -> LandId {
    let mut b = [0u8; 16];
    getrandom::fill(&mut b).expect("the OS random source is available");
    LandId::from_bytes(&b).expect("16 bytes is a valid land id")
}

/// The pause the server asked for. A missing or unparsable header counts as
/// one second, so a bare 429 is still retried gently.
fn retry_after(resp: &Response) -> Option<Duration> {
    let secs = match resp.headers().get(RETRY_AFTER) {
        Some(v) => v.to_str().ok()?.trim().parse::<u64>().ok()?,
        None => 1,
    };
    Some(Duration::from_secs(secs))
}

/// An answer that does not decode: a version mismatch or a broken proxy, so
/// running the command again will not help.
fn bad_response(e: reqwest::Error) -> CliError {
    CliError::server(format!("bad response from server: {e}")).retryable(false)
}

/// The error a non-success answer means, naming the request: by the id
/// the server echoed, else the one `sent`.
fn error_from(resp: Response, has_token: bool, sent: &str) -> CliError {
    let echoed = resp
        .headers()
        .get(REQUEST_ID_HEADER)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let id = Some(echoed.unwrap_or_else(|| sent.to_string()));
    classify(resp, has_token).with_request_id(id)
}

/// 401 and 403. The server explains a 401 only for a known token that has
/// expired: by `code` `token_expired`, or by message text on servers older
/// than error codes.
fn classify_auth(status: StatusCode, resp: Response, has_token: bool) -> CliError {
    let parsed = resp
        .text()
        .ok()
        .and_then(|b| serde_json::from_str::<ErrorResponse>(&b).ok());
    let code = parsed.as_ref().and_then(|e| e.code.clone());
    let said = parsed.map(|e| e.error).filter(|e| !e.is_empty());
    if status == StatusCode::FORBIDDEN {
        // The server's reason (no write access, or not a reviewer).
        return CliError::new(
            ErrorKind::Auth,
            said.unwrap_or_else(|| "token lacks write access".into()),
        )
        .with_server_code(code);
    }
    if !has_token {
        return CliError::new(
            ErrorKind::Auth,
            "authentication required: set LEEP_TOKEN (or pass --token)",
        )
        .with_server_code(code);
    }
    let expired = match code.as_deref() {
        Some(c) => c == error_code::TOKEN_EXPIRED,
        None => said.as_deref().is_some_and(|e| e.contains("token expired")),
    };
    let e = if expired {
        CliError::new(
            ErrorKind::Auth,
            said.unwrap_or_else(|| "authentication required: token expired".into()),
        )
        .with_hint(
            "ask the server's operator for a new token, then set LEEP_TOKEN \
             (or `leep init --token`)",
        )
    } else {
        CliError::new(
            ErrorKind::Auth,
            "authentication required: the server rejected the token; check LEEP_TOKEN",
        )
    };
    e.with_server_code(code)
}

fn classify(resp: Response, has_token: bool) -> CliError {
    let status = resp.status();
    if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN {
        return classify_auth(status, resp, has_token);
    }
    let body = resp.text().unwrap_or_default();
    if status == StatusCode::BAD_REQUEST {
        if let Ok(amb) = serde_json::from_str::<AmbiguousPrefixResponse>(&body) {
            let mut candidates: Vec<String> =
                amb.candidates.iter().map(ToString::to_string).collect();
            if amb.truncated {
                candidates.push("...".into());
            }
            return CliError::new(ErrorKind::Ambiguous, format!("server: {}", amb.error))
                .with_server_code(amb.code)
                .with_candidates(candidates)
                .with_hint("give more digits of the id; the candidates are listed above");
        }
    }
    let parsed = serde_json::from_str::<ErrorResponse>(&body).ok();
    let code = parsed.as_ref().and_then(|e| e.code.clone());
    if let Some(info) = parsed.as_ref().and_then(|e| e.indexing.clone()) {
        if status == StatusCode::SERVICE_UNAVAILABLE {
            return CliError::new(
                ErrorKind::Indexing,
                format!(
                    "indexing: {} changesets behind ({})",
                    info.behind, info.kind
                ),
            )
            .with_retry_after_ms(info.retry_after_ms)
            .with_server_code(code)
            .with_hint("the server's index is catching up; run the command again later, or pass --stale-ok");
        }
    }
    let msg = parsed
        .map(|e| e.error)
        .unwrap_or_else(|| body.trim().to_string());
    let msg = if msg.is_empty() {
        format!("server returned {status}")
    } else {
        format!("server: {msg} ({status})")
    };
    let e = match status {
        // The server's request timeout cancels a request at any point,
        // possibly after it took effect: like a lost connection, the outcome
        // is unknown and running the command again is the answer.
        StatusCode::REQUEST_TIMEOUT => CliError::new(
            ErrorKind::Timeout,
            format!(
                "{msg}; the request may or may not have taken effect, so run the command again"
            ),
        ),
        StatusCode::TOO_MANY_REQUESTS => CliError::new(ErrorKind::RateLimited, msg),
        StatusCode::NOT_FOUND => CliError::not_found(msg),
        // Only concurrent updates answer 409 (a land that kept losing, a ref
        // that moved); content conflicts are a normal land answer.
        StatusCode::CONFLICT => CliError::lost_race(msg),
        s if s.is_server_error() => CliError::server(msg),
        _ => CliError::user(msg),
    };
    e.with_server_code(code)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    /// Answers each connection with the next canned response (the last one
    /// repeats) and counts the requests it saw.
    fn stub(responses: &[&'static str]) -> (String, Arc<AtomicUsize>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let seen = Arc::new(AtomicUsize::new(0));
        let count = seen.clone();
        let responses = responses.to_vec();
        std::thread::spawn(move || {
            for conn in listener.incoming() {
                let mut conn = conn.unwrap();
                let mut buf = [0u8; 4096];
                let mut got = Vec::new();
                while !got.windows(4).any(|w| w == b"\r\n\r\n") {
                    let n = conn.read(&mut buf).unwrap();
                    if n == 0 {
                        break;
                    }
                    got.extend_from_slice(&buf[..n]);
                }
                let i = count
                    .fetch_add(1, Ordering::SeqCst)
                    .min(responses.len() - 1);
                conn.write_all(responses[i].as_bytes()).unwrap();
            }
        });
        (url, seen)
    }

    /// [`stub`], also keeping each request's head.
    fn recording_stub(responses: Vec<String>) -> (String, Arc<std::sync::Mutex<Vec<String>>>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let heads = seen.clone();
        std::thread::spawn(move || {
            for conn in listener.incoming() {
                let mut conn = conn.unwrap();
                let mut buf = [0u8; 4096];
                let mut got = Vec::new();
                while !got.windows(4).any(|w| w == b"\r\n\r\n") {
                    let n = conn.read(&mut buf).unwrap();
                    if n == 0 {
                        break;
                    }
                    got.extend_from_slice(&buf[..n]);
                }
                let i = {
                    let mut h = heads.lock().unwrap();
                    h.push(String::from_utf8_lossy(&got).to_lowercase());
                    (h.len() - 1).min(responses.len() - 1)
                };
                conn.write_all(responses[i].as_bytes()).unwrap();
            }
        });
        (url, seen)
    }

    fn reply(status: &str, extra: &str, body: &str) -> String {
        format!(
            "HTTP/1.1 {status}\r\n{extra}content-type: application/json\r\ncontent-length: {}\r\n\
             connection: close\r\n\r\n{body}",
            body.len()
        )
    }

    fn indexing_503() -> String {
        reply(
            "503 Service Unavailable",
            "retry-after: 1\r\nleep-indexing: kind=history-tree-v4; behind=5210\r\n",
            r#"{"error":"indexing: history-tree-v4 is 5210 changesets behind; retry in 1s","indexing":{"kind":"history-tree-v4","behind":5210,"retry_after_ms":100}}"#,
        )
    }

    #[test]
    fn indexed_queries_send_the_header_and_retry_an_indexing_503() {
        let ok = reply("200 OK", "", r#"{"entries":[]}"#);
        let (url, heads) = recording_stub(vec![indexing_503(), ok]);
        let id = ChangesetId(Hash::of_blob(b"x"));
        let log = Client::new(&url).log(&id, &LogQuery::default()).unwrap();
        assert!(log.entries.is_empty());
        let heads = heads.lock().unwrap().clone();
        assert_eq!(heads.len(), 2, "one retry");
        assert!(
            heads.iter().all(|h| h.contains("leep-indexing: retry")),
            "{heads:?}"
        );

        let (url, heads) = recording_stub(vec![reply("200 OK", "", r#"{"entries":[]}"#)]);
        Client::new(&url)
            .with_stale_ok(true)
            .log(&id, &LogQuery::default())
            .unwrap();
        assert!(heads.lock().unwrap()[0].contains("leep-indexing: stale-ok"));
    }

    #[test]
    fn an_indexing_503_past_the_wait_is_an_indexing_error() {
        let (url, heads) = recording_stub(vec![indexing_503()]);
        let id = ChangesetId(Hash::of_blob(b"x"));
        let e = Client::new(&url)
            .with_index_retries(Duration::from_secs(5), 1)
            .blame(&id, "f")
            .unwrap_err();
        assert_eq!(heads.lock().unwrap().len(), 2, "one retry within the wait");
        assert_eq!(e.kind(), ErrorKind::Indexing);
        assert!(e.is_retryable());
        assert_eq!(e.exit_code(), crate::error::exit::SERVER);
        assert_eq!(e.retry_after_ms(), Some(100));
        assert_eq!(e.to_json()["error"]["retry_after_ms"], 100);
        assert!(
            e.message().contains("5210 changesets behind"),
            "{}",
            e.message()
        );
    }

    const LIMITED_NOW: &str = "HTTP/1.1 429 Too Many Requests\r\nretry-after: 0\r\n\
        content-type: application/json\r\ncontent-length: 52\r\nconnection: close\r\n\r\n\
        {\"error\":\"rate limit exceeded; retry after 1s     \"}";
    const LIMITED_LONG: &str = "HTTP/1.1 429 Too Many Requests\r\nretry-after: 60\r\n\
        content-type: application/json\r\ncontent-length: 52\r\nconnection: close\r\n\r\n\
        {\"error\":\"rate limit exceeded; retry after 60s    \"}";
    const OK: &str = "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\n\
        content-length: 17\r\nconnection: close\r\n\r\n{\"refs\":{}}      ";

    #[test]
    fn every_request_names_the_invocation_and_errors_carry_the_id() {
        // A server from before codes and request ids: no echo, no code.
        let old = reply("404 Not Found", "", r#"{"error":"not found: x"}"#);
        let (url, heads) = recording_stub(vec![old]);
        let c = Client::new(&url);
        assert_eq!(c.request_id(), invocation_request_id());
        let e = c.resolve("x").unwrap_err();
        let sent = c.request_id();
        assert!(
            heads.lock().unwrap()[0].contains(&format!("x-request-id: {sent}")),
            "{:?}",
            heads.lock().unwrap()
        );
        assert_eq!(e.request_id(), Some(sent.as_str()));
        assert_eq!(e.server_code(), None);
        assert_eq!(e.kind(), ErrorKind::NotFound);

        // A newer server echoes the id it used and names the error.
        let new = reply(
            "404 Not Found",
            "x-request-id: srv-1\r\n",
            r#"{"error":"not found: x","code":"not_found","request_id":"srv-1"}"#,
        );
        let (url, heads) = recording_stub(vec![new]);
        let c = Client::new(&url);
        c.new_request_id();
        assert_ne!(c.request_id(), invocation_request_id());
        let clone = c.clone();
        let e = clone.resolve("x").unwrap_err();
        assert!(heads.lock().unwrap()[0].contains(&format!("x-request-id: {}", c.request_id())));
        assert_eq!(e.request_id(), Some("srv-1"));
        assert_eq!(e.server_code(), Some("not_found"));
        assert_eq!(e.to_json()["error"]["request_id"], "srv-1");
    }

    #[test]
    fn a_limited_get_is_retried_after_the_pause_the_server_asks_for() {
        let (url, seen) = stub(&[LIMITED_NOW, LIMITED_NOW, OK]);
        let refs = Client::new(&url).refs().unwrap();
        assert!(refs.refs.is_empty());
        assert_eq!(seen.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn a_get_gives_up_after_a_bounded_number_of_tries() {
        let (url, seen) = stub(&[LIMITED_NOW]);
        let e = Client::new(&url).refs().unwrap_err();
        assert!(e.to_string().contains("rate limit exceeded"), "{e}");
        assert_eq!(seen.load(Ordering::SeqCst), MAX_ATTEMPTS as usize);
        assert_eq!(e.kind(), ErrorKind::RateLimited);
        assert_eq!(e.exit_code(), 3);
    }

    #[test]
    fn a_long_retry_after_is_reported_instead_of_waited_out() {
        let (url, seen) = stub(&[LIMITED_LONG, OK]);
        let e = Client::new(&url).refs().unwrap_err();
        assert!(e.to_string().contains("retry after 60s"), "{e}");
        assert_eq!(seen.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn an_expired_token_is_named_as_such() {
        const EXPIRED: &str = "HTTP/1.1 401 Unauthorized\r\nwww-authenticate: Bearer\r\n\
            content-type: application/json\r\ncontent-length: 93\r\nconnection: close\r\n\r\n\
            {\"error\":\"authentication required: token expired at 2020-01-01T00:00:00Z; ask for a new one\"}";
        let (url, _) = stub(&[EXPIRED]);
        let e = Client::with_token(&url, Some("t")).refs().unwrap_err();
        assert_eq!(e.kind(), ErrorKind::Auth);
        assert!(e.to_string().contains("expired at 2020-01-01"), "{e}");
        assert!(e.hint().unwrap().contains("new token"), "{e:?}");
        // Any other 401 keeps the fixed text.
        let (url, _) = stub(&[EXPIRED.replace("token expired", "token unknown").leak()]);
        let e = Client::with_token(&url, Some("t")).refs().unwrap_err();
        assert!(e.to_string().contains("rejected the token"), "{e}");
    }

    #[test]
    fn an_expired_token_is_known_by_its_code() {
        const CODED: &str = "HTTP/1.1 401 Unauthorized\r\nwww-authenticate: Bearer\r\n\
            content-type: application/json\r\ncontent-length: 59\r\nconnection: close\r\n\r\n\
            {\"error\":\"token is past its expiry\",\"code\":\"token_expired\"}";
        let (url, _) = stub(&[CODED]);
        let e = Client::with_token(&url, Some("t")).refs().unwrap_err();
        assert_eq!(e.kind(), ErrorKind::Auth);
        assert_eq!(e.server_code(), Some("token_expired"));
        assert!(e.to_string().contains("past its expiry"), "{e}");
        assert!(e.hint().unwrap().contains("new token"), "{e:?}");
        // A code other than token_expired wins over the message text.
        const UNKNOWN: &str = "HTTP/1.1 401 Unauthorized\r\nwww-authenticate: Bearer\r\n\
            content-type: application/json\r\ncontent-length: 52\r\nconnection: close\r\n\r\n\
            {\"error\":\"token expired at x\",\"code\":\"unauthorized\"}";
        let (url, _) = stub(&[UNKNOWN]);
        let e = Client::with_token(&url, Some("t")).refs().unwrap_err();
        assert!(e.to_string().contains("rejected the token"), "{e}");
        assert!(e.hint().is_none(), "{e:?}");
    }

    #[test]
    fn writes_are_never_retried() {
        let (url, seen) = stub(&[LIMITED_NOW, OK]);
        let h = Hash::of_blob(b"x");
        let e = Client::new(&url).put_blob(&h, b"x".to_vec()).unwrap_err();
        assert!(e.to_string().contains("rate limit exceeded"), "{e}");
        assert_eq!(seen.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn a_request_timeout_is_a_server_error_and_is_not_retried() {
        const TIMED_OUT: &str = "HTTP/1.1 408 Request Timeout\r\ncontent-length: 0\r\n\
            connection: close\r\n\r\n";
        let (url, seen) = stub(&[TIMED_OUT, OK]);
        let e = Client::new(&url)
            .land(&LandRequest {
                changeset: ChangesetId(Hash::of_blob(b"x")),
                target_ref: "main".into(),
                create: None,
                land_id: None,
            })
            .unwrap_err();
        assert_eq!(e.kind(), ErrorKind::Timeout, "{e}");
        assert_eq!(e.exit_code(), 3, "{e}");
        assert!(e.to_string().contains("run the command again"), "{e}");
        assert_eq!(seen.load(Ordering::SeqCst), 1, "a write is never resent");
    }

    #[test]
    fn a_land_with_a_land_id_is_resent_after_a_timeout() {
        const TIMED_OUT: &str = "HTTP/1.1 408 Request Timeout\r\ncontent-length: 0\r\n\
            connection: close\r\n\r\n";
        let id = ChangesetId(Hash::of_blob(b"x"));
        let body = format!(
            "{{\"status\":\"landed\",\"landed\":\"{id}\",\"rebased\":false,\"attempts\":1}}"
        );
        let landed: &'static str = Box::leak(
            format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\n\
                 content-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            )
            .into_boxed_str(),
        );
        let req = LandRequest {
            changeset: id,
            target_ref: "main".into(),
            create: None,
            land_id: Some(new_land_id()),
        };
        let (url, seen) = stub(&[TIMED_OUT, TIMED_OUT, landed]);
        let resp = Client::new(&url).land_resending(&req).unwrap();
        assert!(matches!(resp, LandResponse::Landed { landed, .. } if landed == id));
        assert_eq!(seen.load(Ordering::SeqCst), 3);

        // Without an id a resend could land twice: sent once.
        let (url, seen) = stub(&[TIMED_OUT, landed]);
        let req = LandRequest {
            land_id: None,
            ..req
        };
        let e = Client::new(&url).land_resending(&req).unwrap_err();
        assert_eq!(e.kind(), ErrorKind::Timeout);
        assert_eq!(seen.load(Ordering::SeqCst), 1);
        assert_ne!(new_land_id(), new_land_id());

        // A land with conflicts is an answer: sent once.
        let body = format!("{{\"status\":\"conflicts\",\"onto\":\"{id}\",\"conflicts\":[]}}");
        let conflicts: &'static str = Box::leak(
            format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\n\
                 content-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            )
            .into_boxed_str(),
        );
        let (url, seen) = stub(&[conflicts, landed]);
        let req = LandRequest {
            land_id: Some(new_land_id()),
            ..req
        };
        let resp = Client::new(&url).land_resending(&req).unwrap();
        assert!(matches!(resp, LandResponse::Conflicts { .. }));
        assert_eq!(seen.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn search_flags_become_params() {
        let q = SearchQuery {
            query: "a b".into(),
            ..Default::default()
        };
        assert_eq!(q.params(), [("q", "a b".to_string())]);
        let q = SearchQuery {
            query: "x".into(),
            regex: true,
            ignore_case: true,
            path: Some("src".into()),
            glob: Some("**/*.rs".into()),
            limit: Some(7),
            after: Some("c1".into()),
        };
        assert_eq!(
            q.params(),
            [
                ("q", "x".to_string()),
                ("regex", "true".to_string()),
                ("i", "true".to_string()),
                ("path", "src".to_string()),
                ("glob", "**/*.rs".to_string()),
                ("limit", "7".to_string()),
                ("after", "c1".to_string()),
            ]
        );
    }

    #[test]
    fn encodes_paths() {
        assert_eq!(enc("team/x y"), "team/x%20y");
        assert_eq!(enc("main"), "main");
    }
}
