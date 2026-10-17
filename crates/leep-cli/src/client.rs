//! Blocking HTTP client wrapping every endpoint in `leep_core::api`.
//!
//! Every failed request becomes a [`CliError`] whose kind follows the HTTP
//! status (ADR 0033): no connection is `Unreachable`, 5xx `Server`, 408
//! `Timeout`, 429 `RateLimited`, 404 `NotFound`, 409 `Conflict` (retryable),
//! 401 and 403 `Auth`, any other 4xx `User`. `ErrorResponse` bodies become the
//! error message. 401 and 403 get fixed, token-free messages instead.
//!
//! A 429 from the server's request limits (ADR 0020) is retried for GET
//! requests only, honoring `Retry-After`; anything else fails at once, since a
//! blind retry of a write could repeat it.

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
/// Longest pause between tries. A longer `Retry-After` means the server wants
/// the caller gone for a while, so fail with that advice instead of hanging.
const MAX_RETRY_WAIT: Duration = Duration::from_secs(10);

#[derive(Clone)]
pub struct Client {
    base: String,
    http: Http,
    has_token: bool,
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
        out
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
        out
    }
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
        }
    }

    pub fn server(&self) -> &str {
        &self.base
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.base, path)
    }

    fn send(&self, req: RequestBuilder) -> Result<Response> {
        let (http, built) = req.build_split();
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
            return Err(error_from(resp, self.has_token));
        }
    }

    /// A request that never got an answer. A timeout may have taken effect;
    /// a refused connection did not.
    fn unreachable(&self, e: reqwest::Error) -> CliError {
        let kind = if e.is_timeout() {
            ErrorKind::Timeout
        } else {
            ErrorKind::Unreachable
        };
        CliError::new(kind, format!("cannot reach {}: {e}", self.base)).with_hint(
            "check that leep-server runs at that URL; it comes from --server, \
             LEEP_SERVER or .leep/config.json",
        )
    }

    fn json<T: DeserializeOwned>(&self, req: RequestBuilder) -> Result<T> {
        let resp = self.send(req)?;
        resp.json::<T>().map_err(bad_response)
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

    pub fn missing_blobs(&self, hashes: Vec<Hash>) -> Result<Vec<Hash>> {
        let r: MissingBlobsResponse = self.json(
            self.http
                .post(self.url("/v1/blobs/missing"))
                .json(&MissingBlobsRequest { hashes }),
        )?;
        Ok(r.missing)
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
        let url = match name {
            Some(n) => self.url(&format!("/v1/reflog/{}", enc(n))),
            None => self.url("/v1/reflog"),
        };
        let mut query = vec![("limit", limit.to_string())];
        if let Some(b) = before {
            query.push(("before", b.to_string()));
        }
        self.json(self.http.get(url).query(&query))
    }

    /// Compare-and-swap a ref. A lost race (409) is `ok: false`, not an error.
    pub fn cas_ref(&self, name: &str, req: &CasRefRequest) -> Result<CasRefResponse> {
        let resp = self
            .http
            .post(self.url(&format!("/v1/refs/{}", enc(name))))
            .json(req)
            .send()
            .map_err(|e| self.unreachable(e))?;
        if resp.status().is_success() || resp.status() == StatusCode::CONFLICT {
            return resp.json().map_err(bad_response);
        }
        Err(error_from(resp, self.has_token))
    }

    pub fn resolve(&self, rev: &str) -> Result<ChangesetId> {
        let r: ResolveResponse = self.json(
            self.http
                .get(self.url(&format!("/v1/resolve/{}", enc(rev)))),
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
        self.json(
            self.http
                .get(self.url(&format!("/v1/log/{id}")))
                .query(&q.params()),
        )
    }

    /// Newest change to a file or directory, from the server's path index.
    pub fn last_change(&self, id: &ChangesetId, path: &str) -> Result<LastChangeResponse> {
        self.json(
            self.http
                .get(self.url(&format!("/v1/last-change/{id}")))
                .query(&[("path", path)]),
        )
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
        self.json(
            self.http
                .get(self.url(&format!("/v1/search/{id}")))
                .query(&q.params()),
        )
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
        self.json(
            self.http
                .get(self.url(&format!("/v1/blame/{id}")))
                .query(&[("path", path)]),
        )
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

fn error_from(resp: Response, has_token: bool) -> CliError {
    let status = resp.status();
    match status {
        StatusCode::UNAUTHORIZED if has_token => {
            return CliError::new(
                ErrorKind::Auth,
                "authentication required: the server rejected the token; check LEEP_TOKEN",
            )
        }
        StatusCode::UNAUTHORIZED => {
            return CliError::new(
                ErrorKind::Auth,
                "authentication required: set LEEP_TOKEN (or pass --token)",
            )
        }
        StatusCode::FORBIDDEN => return CliError::new(ErrorKind::Auth, "token lacks write access"),
        _ => {}
    }
    let body = resp.text().unwrap_or_default();
    let msg = serde_json::from_str::<ErrorResponse>(&body)
        .map(|e| e.error)
        .unwrap_or_else(|_| body.trim().to_string());
    let msg = if msg.is_empty() {
        format!("server returned {status}")
    } else {
        format!("server: {msg} ({status})")
    };
    match status {
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
    }
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

    const LIMITED_NOW: &str = "HTTP/1.1 429 Too Many Requests\r\nretry-after: 0\r\n\
        content-type: application/json\r\ncontent-length: 52\r\nconnection: close\r\n\r\n\
        {\"error\":\"rate limit exceeded; retry after 1s     \"}";
    const LIMITED_LONG: &str = "HTTP/1.1 429 Too Many Requests\r\nretry-after: 60\r\n\
        content-type: application/json\r\ncontent-length: 52\r\nconnection: close\r\n\r\n\
        {\"error\":\"rate limit exceeded; retry after 60s    \"}";
    const OK: &str = "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\n\
        content-length: 17\r\nconnection: close\r\n\r\n{\"refs\":{}}      ";

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
            })
            .unwrap_err();
        assert_eq!(e.kind(), ErrorKind::Timeout, "{e}");
        assert_eq!(e.exit_code(), 3, "{e}");
        assert!(e.to_string().contains("run the command again"), "{e}");
        assert_eq!(seen.load(Ordering::SeqCst), 1, "a write is never resent");
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
            ]
        );
    }

    #[test]
    fn encodes_paths() {
        assert_eq!(enc("team/x y"), "team/x%20y");
        assert_eq!(enc("main"), "main");
    }
}
