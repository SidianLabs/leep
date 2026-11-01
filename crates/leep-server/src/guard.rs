//! Checks on where a request comes from and what its body claims to be,
//! made before any body is read.
//!
//! - A body must declare its type: `application/json` on `POST`,
//!   `application/octet-stream` on `PUT` (blob uploads), else 415. A request
//!   with no `Content-Type` at all is accepted when it carries an
//!   `Authorization` header, so clients built before this check, which sent
//!   blob uploads without one, keep working against servers with tokens.
//! - Without authentication (no `--tokens-file`), a request with an `Origin`
//!   header, or whose host is not `localhost`, an IP address or a name given
//!   with `--allow-host`, is refused with 403. Leep's clients send neither a
//!   foreign host nor `Origin`; only a web page would. The probes are exempt.

use std::net::IpAddr;
use std::sync::Arc;

use axum::extract::{MatchedPath, Request};
use axum::http::{header, Method, StatusCode};
use axum::middleware::Next;
use axum::response::Response;
use leep_core::api::error_code;

use crate::reload::RuntimeConfig;

/// Host names accepted without authentication besides `localhost` and IP
/// addresses (`--allow-host`), lowercase without a trailing dot. An
/// `Extension` on the router; absent means none.
#[derive(Clone, Debug, Default)]
pub struct AllowedHosts(Arc<Vec<String>>);

impl AllowedHosts {
    pub fn new<S: AsRef<str>>(names: &[S]) -> Self {
        AllowedHosts(Arc::new(
            names
                .iter()
                .map(|n| normalize(n.as_ref()))
                .filter(|n| !n.is_empty())
                .collect(),
        ))
    }

    fn allows(&self, host: &str) -> bool {
        self.0.iter().any(|h| h == host)
    }
}

fn normalize(host: &str) -> String {
    host.trim().trim_end_matches('.').to_ascii_lowercase()
}

/// The type a body of `method` must declare, if the method takes one.
fn wanted_type(method: &Method) -> Option<&'static str> {
    match *method {
        Method::POST => Some("application/json"),
        Method::PUT => Some("application/octet-stream"),
        _ => None,
    }
}

/// The media type of a `Content-Type` value, without its parameters.
fn essence(v: &str) -> &str {
    v.split(';').next().unwrap_or("").trim()
}

/// The host a request names: the URI's authority (HTTP/2, absolute form)
/// or the `Host` header, without the port. `None` when it names none.
fn host_of(req: &Request) -> Option<String> {
    let raw = match req.uri().authority() {
        Some(a) => a.as_str().to_string(),
        None => req.headers().get(header::HOST)?.to_str().ok()?.to_string(),
    };
    let authority: axum::http::uri::Authority = raw.parse().ok()?;
    Some(normalize(authority.host()))
}

fn local_host(host: &str, extra: Option<&AllowedHosts>) -> bool {
    host == "localhost"
        || host
            .trim_start_matches('[')
            .trim_end_matches(']')
            .parse::<IpAddr>()
            .is_ok()
        || extra.is_some_and(|e| e.allows(host))
}

/// Axum middleware applying the checks above. Layer it inside
/// authentication and before anything reads the body.
pub async fn check(req: Request, next: Next) -> Response {
    if crate::is_probe(req.method(), req.uri().path()) {
        return next.run(req).await;
    }
    let open = req
        .extensions()
        .get::<Arc<RuntimeConfig>>()
        .is_some_and(|c| c.auth.is_none());
    if open {
        if req.headers().contains_key(header::ORIGIN) {
            return crate::auth::reject(
                StatusCode::FORBIDDEN,
                error_code::FORBIDDEN,
                "requests with an Origin header are refused by a server without \
                 authentication; give the server a --tokens-file",
            );
        }
        if let Some(host) = host_of(&req) {
            if !local_host(&host, req.extensions().get::<AllowedHosts>()) {
                return crate::auth::reject(
                    StatusCode::FORBIDDEN,
                    error_code::FORBIDDEN,
                    &format!(
                        "host {host:?} is refused by a server without authentication: \
                         use localhost or an IP address, list the name with \
                         --allow-host, or give the server a --tokens-file"
                    ),
                );
            }
        }
    }
    let routed = req.extensions().get::<MatchedPath>().is_some();
    if let (true, Some(want)) = (routed, wanted_type(req.method())) {
        let declared = req.headers().get(header::CONTENT_TYPE);
        let ok = match declared {
            Some(v) => v
                .to_str()
                .is_ok_and(|v| essence(v).eq_ignore_ascii_case(want)),
            None => req.headers().contains_key(header::AUTHORIZATION),
        };
        if !ok {
            return crate::auth::reject(
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                error_code::INVALID,
                &format!("this request's body must be sent with Content-Type: {want}"),
            );
        }
    }
    next.run(req).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn content_types_and_hosts() {
        assert_eq!(
            essence("application/json; charset=utf-8"),
            "application/json"
        );
        assert_eq!(essence(" text/plain "), "text/plain");
        let extra = AllowedHosts::new(&["Leep.Internal.", " "]);
        for ok in [
            "localhost",
            "127.0.0.1",
            "[::1]",
            "10.1.2.3",
            "leep.internal",
        ] {
            assert!(local_host(ok, Some(&extra)), "{ok}");
        }
        for no in ["example.com", "localhost.example.com", "127.0.0.1.nip.io"] {
            assert!(!local_host(no, Some(&extra)), "{no}");
        }
    }
}
