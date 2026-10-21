//! Static bearer-token authentication (ADR 0007).
//!
//! Tokens live in a JSON file as `blake3:<hex>` digests, never plaintext, so a
//! leaked file does not leak credentials. The file is read at startup and
//! again on SIGHUP (ADR 0024).
//!
//! ```json
//! { "tokens": [
//!   { "name": "ci",    "role": "write", "hash": "blake3:<64 hex>" },
//!   { "name": "viewer", "role": "read",  "hash": "blake3:<64 hex>" }
//! ] }
//! ```
//!
//! An entry may also carry request limits (`rate`, `burst`, `concurrency`,
//! `max_body`; ADR 0020), see [`crate::limits`].

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;

use axum::extract::{FromRequestParts, Request};
use axum::http::request::Parts;
use axum::http::{header, HeaderValue, Method, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::Json;
use leep_core::api::{error_code, ErrorResponse};
use serde::{Deserialize, Serialize};
use subtle::ConstantTimeEq;
use tracing::Instrument;

use crate::limits::{Quota, TokenLimits};
use crate::reload::RuntimeConfig;

const DIGEST_PREFIX: &str = "blake3:";
const TOKEN_BYTES: usize = 32;

/// What a token may do.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    /// GET endpoints, plus `POST /v1/blobs/missing` (a query).
    Read,
    /// Everything.
    Write,
}

impl std::str::FromStr for Role {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, String> {
        match s {
            "read" => Ok(Role::Read),
            "write" => Ok(Role::Write),
            _ => Err(format!("unknown role {s:?} (expected read or write)")),
        }
    }
}

impl Role {
    pub fn as_str(self) -> &'static str {
        match self {
            Role::Read => "read",
            Role::Write => "write",
        }
    }
}

/// Who made a request, set by [`require_token`] for the handlers behind it.
/// Handlers that record or filter by identity read it; the name is always the
/// token's, never anything the request says.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Principal {
    pub name: String,
    pub role: Role,
    /// The token entry's `author`, for clients to stamp on changesets.
    pub author: Option<String>,
}

/// The request's [`Principal`], or `None` when authentication is off.
pub struct MaybePrincipal(pub Option<Principal>);

impl<S: Send + Sync> FromRequestParts<S> for MaybePrincipal {
    type Rejection = std::convert::Infallible;

    async fn from_request_parts(parts: &mut Parts, _: &S) -> Result<Self, Self::Rejection> {
        Ok(MaybePrincipal(parts.extensions.get::<Principal>().cloned()))
    }
}

/// One line of the tokens file.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TokenEntry {
    pub name: String,
    pub role: Role,
    pub hash: String,
    /// Who changesets made with this token are by, e.g. `Name <email>`.
    /// Clients use it when nothing local names an author.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub author: Option<String>,
    /// Requests per second, for reads and writes separately.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rate: Option<Quota>,
    /// Requests allowed at once before the rate applies.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub burst: Option<f64>,
    /// Requests in flight at once.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub concurrency: Option<Quota>,
    /// Largest request body, in bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_body: Option<Quota>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TokenFile {
    tokens: Vec<TokenEntry>,
}

struct Known {
    name: String,
    role: Role,
    author: Option<String>,
    digest: [u8; 32],
    limits: TokenLimits,
}

/// What a reload changed in the token table, by token name. Never holds
/// digests, so it is safe to log.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct AuthChange {
    pub added: Vec<String>,
    pub removed: Vec<String>,
    /// Same name, new digest.
    pub rotated: Vec<String>,
    pub role_changed: Vec<String>,
    pub limits_changed: Vec<String>,
}

impl AuthChange {
    pub fn is_empty(&self) -> bool {
        *self == AuthChange::default()
    }
}

/// The loaded token table.
pub struct Auth {
    tokens: Vec<Known>,
}

/// `blake3:<hex>` of a token. Surrounding whitespace is not part of the token.
pub fn hash_token(token: &str) -> String {
    format!(
        "{DIGEST_PREFIX}{}",
        blake3::hash(token.trim().as_bytes()).to_hex()
    )
}

/// A fresh random token (hex) and its tokens-file entry. The token is shown
/// once; only the digest is meant to be stored.
pub fn generate(name: &str, role: Role) -> Result<(String, TokenEntry), String> {
    let mut raw = [0u8; TOKEN_BYTES];
    getrandom::fill(&mut raw).map_err(|e| format!("no system randomness: {e}"))?;
    let token = hex::encode(raw);
    let entry = TokenEntry {
        name: name.to_string(),
        role,
        hash: hash_token(&token),
        author: None,
        rate: None,
        burst: None,
        concurrency: None,
        max_body: None,
    };
    Ok((token, entry))
}

fn parse_digest(s: &str) -> Result<[u8; 32], String> {
    let hex_part = s.strip_prefix(DIGEST_PREFIX).ok_or_else(|| {
        "hash must be `blake3:<64 hex>`; the file must hold digests, not plaintext tokens \
         (see `leep-server hash-token`)"
            .to_string()
    })?;
    let mut out = [0u8; 32];
    if hex_part.len() != 64 || hex::decode_to_slice(hex_part, &mut out).is_err() {
        return Err("hash must be `blake3:` followed by 64 hex digits".into());
    }
    Ok(out)
}

impl Auth {
    /// Parse the JSON tokens file contents.
    pub fn parse(text: &str) -> Result<Auth, String> {
        let file: TokenFile = serde_json::from_str(text).map_err(|e| format!("bad JSON: {e}"))?;
        if file.tokens.is_empty() {
            return Err("no tokens listed; remove the file to disable authentication".into());
        }
        let mut names = HashSet::new();
        let mut tokens = Vec::new();
        for (i, t) in file.tokens.into_iter().enumerate() {
            if t.name.trim().is_empty() {
                return Err(format!("token #{i}: empty name"));
            }
            if !names.insert(t.name.clone()) {
                return Err(format!("duplicate token name {:?}", t.name));
            }
            let digest = parse_digest(&t.hash).map_err(|e| format!("token {:?}: {e}", t.name))?;
            if let Some(a) = &t.author {
                if a.trim().is_empty() || a.contains(['\n', '\r']) {
                    return Err(format!(
                        "token {:?}: author must be one non-empty line",
                        t.name
                    ));
                }
            }
            let limits = TokenLimits {
                rate: t.rate,
                burst: t.burst,
                concurrency: t.concurrency,
                max_body: t.max_body,
            };
            limits
                .validate()
                .map_err(|e| format!("token {:?}: {e}", t.name))?;
            tokens.push(Known {
                name: t.name,
                role: t.role,
                author: t.author,
                digest,
                limits,
            });
        }
        Ok(Auth { tokens })
    }

    pub fn from_file(path: &Path) -> Result<Auth, String> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| format!("cannot read tokens file {}: {e}", path.display()))?;
        Auth::parse(&text).map_err(|e| format!("tokens file {}: {e}", path.display()))
    }

    pub fn len(&self) -> usize {
        self.tokens.len()
    }

    pub fn is_empty(&self) -> bool {
        self.tokens.is_empty()
    }

    /// Names of the tokens that may write, in file order: the principals whose
    /// reviews count.
    pub fn reviewers(&self) -> Vec<String> {
        self.tokens
            .iter()
            .filter(|k| k.role == Role::Write)
            .map(|k| k.name.clone())
            .collect()
    }

    /// Each token's own limit fields, in file order; unset ones fall back to
    /// the server-wide defaults.
    pub fn token_limits(&self) -> impl Iterator<Item = (&str, Role, &TokenLimits)> {
        self.tokens
            .iter()
            .map(|k| (k.name.as_str(), k.role, &k.limits))
    }

    /// Feed the table into a config fingerprint, sorted by name. Holds the
    /// digests the file already holds, never a token.
    pub(crate) fn fingerprint_into(&self, c: &mut crate::fingerprint::Canon) {
        let mut all: Vec<&Known> = self.tokens.iter().collect();
        all.sort_by(|a, b| a.name.cmp(&b.name));
        c.section("tokens", all.len());
        for k in all {
            c.str(&k.name)
                .str(k.role.as_str())
                .bytes(&k.digest)
                .opt_str(k.author.as_deref())
                .opt_quota(k.limits.rate)
                .opt_f64(k.limits.burst)
                .opt_quota(k.limits.concurrency)
                .opt_quota(k.limits.max_body);
        }
    }

    /// What `newer` changes relative to `self`, each list sorted by name.
    pub fn diff(&self, newer: &Auth) -> AuthChange {
        let old: HashMap<&str, &Known> = self.tokens.iter().map(|k| (k.name.as_str(), k)).collect();
        let new: HashMap<&str, &Known> =
            newer.tokens.iter().map(|k| (k.name.as_str(), k)).collect();
        let mut c = AuthChange::default();
        for (name, k) in &new {
            let Some(o) = old.get(name) else {
                c.added.push(name.to_string());
                continue;
            };
            if o.digest != k.digest {
                c.rotated.push(name.to_string());
            }
            if o.role != k.role {
                c.role_changed.push(name.to_string());
            }
            if o.limits != k.limits {
                c.limits_changed.push(name.to_string());
            }
        }
        c.removed = old
            .keys()
            .filter(|n| !new.contains_key(*n))
            .map(|n| n.to_string())
            .collect();
        for list in [
            &mut c.added,
            &mut c.removed,
            &mut c.rotated,
            &mut c.role_changed,
            &mut c.limits_changed,
        ] {
            list.sort();
        }
        c
    }

    /// Match a presented token. Compares against every entry without early
    /// exit so timing does not reveal which digest matched.
    fn lookup(&self, token: &str) -> Option<&Known> {
        let presented = *blake3::hash(token.as_bytes()).as_bytes();
        let mut found = None;
        for k in &self.tokens {
            if bool::from(k.digest.ct_eq(&presented)) {
                found = Some(k);
            }
        }
        found
    }
}

/// Mutating requests need `write`. `POST /v1/blobs/missing` is a query, so a
/// read token may call it.
pub(crate) fn needs_write(method: &Method, path: &str) -> bool {
    match *method {
        Method::GET | Method::HEAD => false,
        Method::POST => path != "/v1/blobs/missing",
        _ => true,
    }
}

/// An [`ErrorResponse`] refusal; `code` is one of `error_code`, named by
/// the caller so no refusal is left to a status default.
pub(crate) fn reject(status: StatusCode, code: &str, msg: &str) -> Response {
    let body = ErrorResponse::new(msg.to_string()).with_code(code);
    let mut resp = (status, Json(body)).into_response();
    if status == StatusCode::UNAUTHORIZED {
        resp.headers_mut()
            .insert(header::WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
    }
    resp
}

fn bearer(req: &Request) -> Option<&str> {
    let v = req.headers().get(header::AUTHORIZATION)?.to_str().ok()?;
    let (scheme, rest) = v.split_once(' ')?;
    scheme
        .eq_ignore_ascii_case("bearer")
        .then(|| rest.trim())
        .filter(|t| !t.is_empty())
}

/// Axum middleware enforcing [`Auth`]; `GET /v1/health` and `GET /v1/ready` stay public. Uses the
/// config snapshot the request started with, so a token revoked mid-request
/// does not change that request's answer, and the next request sees it.
pub async fn require_token(mut req: Request, next: Next) -> Response {
    if crate::is_probe(req.method(), req.uri().path()) {
        return next.run(req).await;
    }
    let Some(config) = req.extensions().get::<Arc<RuntimeConfig>>().cloned() else {
        // Layered without the snapshot middleware: refuse rather than serve open.
        return reject(
            StatusCode::INTERNAL_SERVER_ERROR,
            error_code::INTERNAL,
            "server misconfigured",
        );
    };
    let Some(auth) = &config.auth else {
        return next.run(req).await;
    };
    let Some(Known {
        name, role, author, ..
    }) = bearer(&req).and_then(|t| auth.lookup(t))
    else {
        return reject(
            StatusCode::UNAUTHORIZED,
            error_code::UNAUTHORIZED,
            "authentication required: missing or invalid bearer token",
        );
    };
    let role = *role;
    if role == Role::Read && needs_write(req.method(), req.uri().path()) {
        tracing::debug!(token = name, "read token refused on a mutating request");
        return reject(
            StatusCode::FORBIDDEN,
            error_code::FORBIDDEN,
            "token lacks write access",
        );
    }
    let span = tracing::debug_span!("auth", token = name, role = role.as_str());
    req.extensions_mut().insert(Principal {
        name: name.to_string(),
        role,
        author: author.clone(),
    });
    next.run(req).instrument(span).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn digest_round_trip() {
        let (token, entry) = generate("ci", Role::Write).unwrap();
        assert_eq!(token.len(), 64);
        let text = serde_json::json!({ "tokens": [entry] }).to_string();
        let auth = Auth::parse(&text).unwrap();
        let k = auth.lookup(&token).unwrap();
        assert_eq!((k.name.as_str(), k.role), ("ci", Role::Write));
        assert!(auth.lookup("nope").is_none());
    }

    #[test]
    fn reviewers_are_the_write_tokens() {
        let text = serde_json::json!({ "tokens": [
            { "name": "ci", "role": "write", "hash": hash_token("a") },
            { "name": "viewer", "role": "read", "hash": hash_token("b") },
            { "name": "alice", "role": "write", "hash": hash_token("c") },
        ]})
        .to_string();
        assert_eq!(Auth::parse(&text).unwrap().reviewers(), ["ci", "alice"]);
    }

    #[test]
    fn write_classification() {
        assert!(!needs_write(&Method::GET, "/v1/refs"));
        assert!(!needs_write(&Method::GET, "/v1/search/main"));
        assert!(!needs_write(&Method::POST, "/v1/blobs/missing"));
        assert!(needs_write(&Method::POST, "/v1/land"));
        assert!(needs_write(&Method::POST, "/v1/changesets/abc/signatures"));
        assert!(needs_write(&Method::POST, "/v1/changesets/abc/reviews"));
        assert!(!needs_write(&Method::GET, "/v1/changesets/abc/reviews"));
        assert!(!needs_write(
            &Method::GET,
            "/v1/changesets/abc/review-status"
        ));
        assert!(!needs_write(&Method::GET, "/v1/changesets/abc/signatures"));
        assert!(needs_write(&Method::PUT, "/v1/blobs/x"));
        assert!(needs_write(&Method::DELETE, "/v1/refs/main"));
    }

    #[test]
    fn diff_names_what_changed_and_nothing_else() {
        let parse = |entries: serde_json::Value| {
            Auth::parse(&serde_json::json!({ "tokens": entries }).to_string()).unwrap()
        };
        let old = parse(serde_json::json!([
            { "name": "same", "role": "write", "hash": hash_token("1") },
            { "name": "rotated", "role": "write", "hash": hash_token("2") },
            { "name": "promoted", "role": "read", "hash": hash_token("3") },
            { "name": "throttled", "role": "write", "hash": hash_token("4") },
            { "name": "gone", "role": "write", "hash": hash_token("5") },
        ]));
        let new = parse(serde_json::json!([
            { "name": "same", "role": "write", "hash": hash_token("1") },
            { "name": "rotated", "role": "write", "hash": hash_token("2b") },
            { "name": "promoted", "role": "write", "hash": hash_token("3") },
            { "name": "throttled", "role": "write", "hash": hash_token("4"), "rate": 2 },
            { "name": "fresh", "role": "read", "hash": hash_token("6") },
        ]));
        assert!(old.diff(&old).is_empty());
        let c = old.diff(&new);
        assert_eq!(c.added, ["fresh"]);
        assert_eq!(c.removed, ["gone"]);
        assert_eq!(c.rotated, ["rotated"]);
        assert_eq!(c.role_changed, ["promoted"]);
        assert_eq!(c.limits_changed, ["throttled"]);
    }
}
