//! Static bearer-token authentication (ADR 0007).
//!
//! Tokens live in a JSON file as `blake3:<hex>` digests, never plaintext, so a
//! leaked file does not leak credentials. The file is read once at startup.
//!
//! ```json
//! { "tokens": [
//!   { "name": "ci",    "role": "write", "hash": "blake3:<64 hex>" },
//!   { "name": "viewer", "role": "read",  "hash": "blake3:<64 hex>" }
//! ] }
//! ```

use std::collections::HashSet;
use std::path::Path;

use axum::extract::{FromRequestParts, Request, State};
use axum::http::request::Parts;
use axum::http::{header, HeaderValue, Method, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::Json;
use leep_core::api::ErrorResponse;
use serde::{Deserialize, Serialize};
use subtle::ConstantTimeEq;
use tracing::Instrument;

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
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TokenEntry {
    pub name: String,
    pub role: Role,
    pub hash: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TokenFile {
    tokens: Vec<TokenEntry>,
}

struct Known {
    name: String,
    role: Role,
    digest: [u8; 32],
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
            tokens.push(Known {
                name: t.name,
                role: t.role,
                digest,
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

    /// Match a presented token. Compares against every entry without early
    /// exit so timing does not reveal which digest matched.
    fn lookup(&self, token: &str) -> Option<(&str, Role)> {
        let presented = *blake3::hash(token.as_bytes()).as_bytes();
        let mut found = None;
        for k in &self.tokens {
            if bool::from(k.digest.ct_eq(&presented)) {
                found = Some((k.name.as_str(), k.role));
            }
        }
        found
    }
}

/// Mutating requests need `write`. `POST /v1/blobs/missing` is a query, so a
/// read token may call it.
fn needs_write(method: &Method, path: &str) -> bool {
    match *method {
        Method::GET | Method::HEAD => false,
        Method::POST => path != "/v1/blobs/missing",
        _ => true,
    }
}

fn reject(status: StatusCode, msg: &str) -> Response {
    let mut resp = (
        status,
        Json(ErrorResponse {
            error: msg.to_string(),
        }),
    )
        .into_response();
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

/// Axum middleware enforcing [`Auth`]; `GET /v1/health` stays public.
pub async fn require_token(
    State(auth): State<std::sync::Arc<Auth>>,
    mut req: Request,
    next: Next,
) -> Response {
    if req.method() == Method::GET && req.uri().path() == "/v1/health" {
        return next.run(req).await;
    }
    let Some((name, role)) = bearer(&req).and_then(|t| auth.lookup(t)) else {
        return reject(
            StatusCode::UNAUTHORIZED,
            "authentication required: missing or invalid bearer token",
        );
    };
    if role == Role::Read && needs_write(req.method(), req.uri().path()) {
        tracing::debug!(token = name, "read token refused on a mutating request");
        return reject(StatusCode::FORBIDDEN, "token lacks write access");
    }
    let span = tracing::debug_span!("auth", token = name, role = role.as_str());
    req.extensions_mut().insert(Principal {
        name: name.to_string(),
        role,
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
        assert_eq!(auth.lookup(&token), Some(("ci", Role::Write)));
        assert_eq!(auth.lookup("nope"), None);
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
}
