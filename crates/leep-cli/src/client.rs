//! Blocking HTTP client wrapping every endpoint in `leep_core::api`.
//!
//! Network failures and 5xx answers are [`CliError::Server`] (exit 2); 4xx
//! answers are [`CliError::User`] (exit 1). `ErrorResponse` bodies become the
//! error message. 401 and 403 get fixed, token-free messages instead.

use std::time::Duration;

use leep_core::api::*;
use leep_core::{Changeset, ChangesetId, FileKind, Hash};
use reqwest::blocking::{Client as Http, RequestBuilder, Response};
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION};
use reqwest::StatusCode;
use serde::de::DeserializeOwned;

use crate::error::{CliError, Result};

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
        let resp = req
            .send()
            .map_err(|e| CliError::server(format!("cannot reach {}: {e}", self.base)))?;
        let status = resp.status();
        if status.is_success() {
            return Ok(resp);
        }
        Err(error_from(resp, self.has_token))
    }

    fn json<T: DeserializeOwned>(&self, req: RequestBuilder) -> Result<T> {
        let resp = self.send(req)?;
        resp.json::<T>()
            .map_err(|e| CliError::server(format!("bad response from server: {e}")))
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

    pub fn refs(&self) -> Result<RefsResponse> {
        self.json(self.http.get(self.url("/v1/refs")))
    }

    pub fn get_ref(&self, name: &str) -> Result<RefResponse> {
        self.json(self.http.get(self.url(&format!("/v1/refs/{}", enc(name)))))
    }

    /// Compare-and-swap a ref. A lost race (409) is `ok: false`, not an error.
    pub fn cas_ref(&self, name: &str, req: &CasRefRequest) -> Result<CasRefResponse> {
        let resp = self
            .http
            .post(self.url(&format!("/v1/refs/{}", enc(name))))
            .json(req)
            .send()
            .map_err(|e| CliError::server(format!("cannot reach {}: {e}", self.base)))?;
        if resp.status().is_success() || resp.status() == StatusCode::CONFLICT {
            return resp
                .json()
                .map_err(|e| CliError::server(format!("bad response from server: {e}")));
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

fn error_from(resp: Response, has_token: bool) -> CliError {
    let status = resp.status();
    match status {
        StatusCode::UNAUTHORIZED if has_token => {
            return CliError::user(
                "authentication required: the server rejected the token; check LEEP_TOKEN",
            )
        }
        StatusCode::UNAUTHORIZED => {
            return CliError::user("authentication required: set LEEP_TOKEN (or pass --token)")
        }
        StatusCode::FORBIDDEN => return CliError::user("token lacks write access"),
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
    if status.is_server_error() {
        CliError::Server(msg)
    } else {
        CliError::User(msg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_paths() {
        assert_eq!(enc("team/x y"), "team/x%20y");
        assert_eq!(enc("main"), "main");
    }
}
