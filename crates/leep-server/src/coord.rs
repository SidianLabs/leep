//! Forwarding lands to the server that coordinates their ref (ADR 0056).
//!
//! The lease holder is found through `leep_derive::land_coord`; this module
//! is the HTTP side: which peer addresses a land may be sent to, sending
//! it, and recognising a land another server forwarded here.
//!
//! A forward carries the client's own body and token: the holder
//! authenticates the client and applies its own land policy, so forwarding
//! grants no identity. The forward headers are unauthenticated hints, and
//! count only on requests from an allowed peer address.

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use axum::body::Body;
use axum::extract::{ConnectInfo, Request};
use axum::http::{header, HeaderMap, HeaderName, HeaderValue, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::Json;
use bytes::Bytes;
use leep_core::api::ErrorResponse;
use leep_core::Lease;
use leep_derive::land_coord::{Forwarded, LandForward, LAND_FORWARD_PROTOCOL};

/// Marks a land forwarded by another server: `v=1; epoch=N; from=HOLDER`.
pub const FORWARD_HEADER: &str = "leep-land-forward";
/// On a 503: this server does not coordinate the ref; route the land again.
pub const NOT_LEADER_HEADER: &str = "leep-land-not-leader";
/// Names the server process (its lease holder id), on `GET /v1/health`, so
/// a server can check that its `--advertise-url` reaches itself.
pub const INSTANCE_HEADER: &str = "leep-instance";

/// Which addresses lands may be forwarded to, and which callers are peers.
#[derive(Clone, Debug, Default)]
pub struct PeerPolicy {
    /// `--peer-url-prefix`: an address must be on one of these origins
    /// (see [`PeerPrefix`])...
    pub prefixes: Vec<String>,
    /// `--peer-cidr`: ...or name an IP address inside one of these.
    pub cidrs: Vec<Cidr>,
    /// `--peer-allow-plain-http`: allow `http://` to hosts that are not a
    /// loopback or private address.
    pub allow_plain_http: bool,
}

/// An IP network, `10.0.0.0/8` or `fd00::/8`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Cidr {
    net: IpAddr,
    bits: u8,
}

impl std::str::FromStr for Cidr {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, String> {
        let (ip, bits) = match s.split_once('/') {
            Some((ip, bits)) => (ip, Some(bits)),
            None => (s, None),
        };
        let net: IpAddr = ip
            .parse()
            .map_err(|_| format!("{s:?} is not an IP address or network"))?;
        let max = if net.is_ipv4() { 32 } else { 128 };
        let bits = match bits {
            Some(b) => b
                .parse::<u8>()
                .ok()
                .filter(|b| *b <= max)
                .ok_or_else(|| format!("{s:?}: prefix length must be 0 to {max}"))?,
            None => max,
        };
        Ok(Cidr { net, bits })
    }
}

impl Cidr {
    pub fn contains(&self, ip: IpAddr) -> bool {
        let ip = canonical(ip);
        match (self.net, ip) {
            (IpAddr::V4(n), IpAddr::V4(a)) => {
                let mask = u32::MAX.checked_shl(32 - u32::from(self.bits)).unwrap_or(0);
                u32::from(n) & mask == u32::from(a) & mask
            }
            (IpAddr::V6(n), IpAddr::V6(a)) => {
                let mask = u128::MAX
                    .checked_shl(128 - u32::from(self.bits))
                    .unwrap_or(0);
                u128::from(n) & mask == u128::from(a) & mask
            }
            _ => false,
        }
    }
}

/// An IPv4 address seen through an IPv6 socket, as itself.
fn canonical(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V6(v6) => v6.to_ipv4_mapped().map_or(ip, IpAddr::V4),
        v4 => v4,
    }
}

/// A `--peer-url-prefix`, matched on the parsed address rather than as
/// text: scheme, host and port must equal the address's, and its path must
/// start with the prefix's path. `http://10.0.3.7:7070/` names one server;
/// `http://10.0.3.7:` (a colon and no port) names every port of that host.
/// As text, `https://leep.example` would also have matched
/// `https://leep.example.evil.net`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PeerPrefix {
    scheme: String,
    host: String,
    /// `None`: any port.
    port: Option<u16>,
    path: String,
}

impl std::str::FromStr for PeerPrefix {
    type Err = String;

    fn from_str(p: &str) -> Result<Self, String> {
        let bad = |why: &str| {
            format!(
                "--peer-url-prefix {p:?} {why}; write an origin such as \
                 https://leep-1.internal:7070/"
            )
        };
        let (scheme, rest) = p.split_once("://").ok_or_else(|| bad("is not a URL"))?;
        let authority = rest.split('/').next().unwrap_or("");
        let any_port = authority.ends_with(':');
        let host_port = authority.trim_end_matches(':');
        let text = format!("{scheme}://{host_port}{}", &rest[authority.len()..]);
        let url = reqwest::Url::parse(&text).map_err(|e| bad(&format!("is not a URL ({e})")))?;
        if !matches!(url.scheme(), "http" | "https") {
            return Err(bad("is not http or https"));
        }
        if url.username() != "" || url.password().is_some() || url.query().is_some() {
            return Err(bad("must not carry credentials or a query"));
        }
        // The host as written must be the host parsed, so a partial address
        // such as `10.0.3.` (which a URL parser reads as 10.0.0.3) is
        // refused instead of reinterpreted.
        let written = match host_port.find(']') {
            Some(end) if host_port.starts_with('[') => &host_port[..=end],
            _ => match host_port.rsplit_once(':') {
                Some((h, port)) if !port.is_empty() && port.bytes().all(|b| b.is_ascii_digit()) => {
                    h
                }
                _ => host_port,
            },
        };
        let host = url.host_str().unwrap_or("").to_string();
        if host.is_empty() || !host.eq_ignore_ascii_case(written) || !whole_host(&host) {
            return Err(bad("does not name a whole host"));
        }
        Ok(PeerPrefix {
            scheme: url.scheme().to_string(),
            host,
            port: if any_port {
                None
            } else {
                url.port_or_known_default()
            },
            path: url.path().to_string(),
        })
    }
}

/// Whether `host` is an IP address or a complete DNS name: labels of
/// letters, digits, `-` and `_`, none empty and none starting or ending
/// with `-`. A text prefix such as `https://leep-` used to match
/// `https://leep-1.internal`; parsed, it would match nothing, so it is
/// refused rather than left to stop every forward without a word.
fn whole_host(host: &str) -> bool {
    if host.starts_with('[') || host.parse::<IpAddr>().is_ok() {
        return true;
    }
    host.split('.').all(|label| {
        !label.is_empty()
            && !label.starts_with('-')
            && !label.ends_with('-')
            && label
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    })
}

impl PeerPrefix {
    fn matches(&self, url: &reqwest::Url) -> bool {
        url.scheme() == self.scheme
            && url.host_str() == Some(self.host.as_str())
            && self
                .port
                .is_none_or(|p| url.port_or_known_default() == Some(p))
            && url.path().starts_with(&self.path)
    }

    /// The host when it is an IP address.
    fn ip(&self) -> Option<IpAddr> {
        self.host
            .trim_start_matches('[')
            .trim_end_matches(']')
            .parse()
            .ok()
    }
}

/// The URL's host when it is an IP address; names are never resolved.
fn host_ip(url: &reqwest::Url) -> Option<IpAddr> {
    url.host_str()?
        .trim_start_matches('[')
        .trim_end_matches(']')
        .parse()
        .ok()
}

fn is_private(ip: IpAddr) -> bool {
    match canonical(ip) {
        IpAddr::V4(a) => a.is_loopback() || a.is_private() || a.is_link_local(),
        IpAddr::V6(a) => {
            a.is_loopback()
                || (a.segments()[0] & 0xfe00) == 0xfc00
                || (a.segments()[0] & 0xffc0) == 0xfe80
        }
    }
}

impl PeerPolicy {
    /// Whether a land, with its client's token, may be sent to `address`.
    pub fn check_url(&self, address: &str) -> Result<reqwest::Url, String> {
        let url = reqwest::Url::parse(address)
            .map_err(|e| format!("peer address {address:?} is not a URL: {e}"))?;
        if !matches!(url.scheme(), "http" | "https") {
            return Err(format!("peer address {address:?} is not http or https"));
        }
        if url.username() != "" || url.password().is_some() || url.query().is_some() {
            return Err(format!(
                "peer address {address:?} must not carry credentials or a query"
            ));
        }
        if url.host_str().is_none_or(str::is_empty) {
            return Err(format!("peer address {address:?} has no host"));
        }
        let ip = host_ip(&url);
        let listed = self.parsed_prefixes().any(|p| p.matches(&url))
            || ip.is_some_and(|ip| self.cidrs.iter().any(|c| c.contains(ip)));
        if !listed {
            return Err(format!(
                "peer address {address:?} is not allowed by --peer-url-prefix or --peer-cidr"
            ));
        }
        let local = ip.is_some_and(is_private) || url.host_str() == Some("localhost");
        if url.scheme() == "http" && !local && !self.allow_plain_http {
            return Err(format!(
                "peer address {address:?} is plain http to a host that is not loopback or \
                 private; use https, or --peer-allow-plain-http"
            ));
        }
        Ok(url)
    }

    /// Whether any request could be recognised as a peer's: a CIDR, or a
    /// prefix whose host is an IP address. Host names are never resolved, so
    /// an allow-list of names alone recognises no peer.
    pub fn recognises_peers(&self) -> bool {
        !self.cidrs.is_empty() || self.parsed_prefixes().any(|p| p.ip().is_some())
    }

    /// Whether a request from `ip` comes from a peer server.
    pub fn is_peer(&self, ip: IpAddr) -> bool {
        let ip = canonical(ip);
        self.cidrs.iter().any(|c| c.contains(ip))
            || self
                .parsed_prefixes()
                .any(|p| p.ip().is_some_and(|h| canonical(h) == ip))
    }

    /// Check every `--peer-url-prefix`, so a malformed one stops the server
    /// at startup instead of allowing nothing, or something else.
    pub fn validate(&self) -> Result<(), String> {
        for p in &self.prefixes {
            p.parse::<PeerPrefix>()?;
        }
        Ok(())
    }

    fn parsed_prefixes(&self) -> impl Iterator<Item = PeerPrefix> + '_ {
        self.prefixes.iter().filter_map(|p| p.parse().ok())
    }
}

/// What the HTTP layer needs to coordinate lands; an `Extension` on the
/// router when `--land-coordination on`.
pub struct Coordination {
    pub policy: PeerPolicy,
    pub holder: String,
    client: reqwest::Client,
}

impl Coordination {
    pub fn new(policy: PeerPolicy, holder: String) -> anyhow::Result<Arc<Self>> {
        policy.validate().map_err(anyhow::Error::msg)?;
        let client = reqwest::Client::builder()
            // A redirect could send the client's token anywhere.
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(1))
            .tcp_keepalive(Duration::from_secs(5))
            .pool_idle_timeout(Duration::from_secs(30))
            .build()?;
        Ok(Arc::new(Coordination {
            policy,
            holder,
            client,
        }))
    }

    /// Check that `address` reaches this very process, so a typo in
    /// `--advertise-url` stops the server at startup instead of stranding
    /// every land forwarded to it.
    pub async fn check_self(&self, address: &str) -> anyhow::Result<()> {
        let url = self
            .policy
            .check_url(address)
            .map_err(anyhow::Error::msg)?
            .join("v1/health")?;
        let mut last = String::new();
        for _ in 0..50 {
            match self.client.get(url.clone()).send().await {
                Ok(r) => {
                    let id = r
                        .headers()
                        .get(INSTANCE_HEADER)
                        .and_then(|v| v.to_str().ok())
                        .unwrap_or("");
                    if id == self.holder {
                        return Ok(());
                    }
                    last = format!(
                        "{url} answered {} from another process ({})",
                        r.status(),
                        if id.is_empty() { "no instance id" } else { id }
                    );
                }
                Err(e) => last = format!("{url}: {e}"),
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        anyhow::bail!("--advertise-url {address} does not reach this server: {last}")
    }
}

/// A land forwarded by an allowed peer: it skips the per-token concurrency
/// permit (the sending server holds one for it) and is never forwarded on.
#[derive(Clone, Copy, Debug)]
pub struct FromPeer;

/// Marks forwarded lands from allowed peers and names this process on
/// `GET /v1/health`. A forward header from anywhere else is ignored: the
/// request is an ordinary client land.
pub async fn mark_peer(mut req: Request, next: Next) -> Response {
    let coord = req.extensions().get::<Arc<Coordination>>().cloned();
    let Some(coord) = coord else {
        return next.run(req).await;
    };
    let health = req.uri().path() == "/v1/health";
    if req.headers().contains_key(FORWARD_HEADER) {
        let from = req
            .extensions()
            .get::<ConnectInfo<SocketAddr>>()
            .map(|c| c.0.ip());
        if from.is_some_and(|ip| coord.policy.is_peer(ip)) {
            req.extensions_mut().insert(FromPeer);
            metrics::counter!("leep_land_received_forwarded_total").increment(1);
        }
    }
    let mut resp = next.run(req).await;
    if health {
        if let Ok(v) = HeaderValue::from_str(&coord.holder) {
            resp.headers_mut().insert(INSTANCE_HEADER, v);
        }
    }
    resp
}

/// `app` with land coordination: forwarded lands from peers are recognised
/// before the per-token limits run, and the handler finds `coordination`.
pub fn layer(app: axum::Router, coordination: Arc<Coordination>) -> axum::Router {
    app.layer(axum::middleware::from_fn(mark_peer))
        .layer(axum::Extension(coordination))
}

/// The answer to a land this server does not coordinate any more.
pub fn not_leader() -> Response {
    let mut resp = (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(
            ErrorResponse::new(
                "this server no longer coordinates lands on that ref; send it again",
            )
            .with_code(leep_core::api::error_code::NOT_LEADER),
        ),
    )
        .into_response();
    resp.headers_mut()
        .insert(NOT_LEADER_HEADER, HeaderValue::from_static("1"));
    resp.headers_mut()
        .insert(header::RETRY_AFTER, HeaderValue::from_static("1"));
    resp
}

/// The leader's answer, relayed to the client as it came.
pub struct Relayed {
    status: StatusCode,
    headers: Vec<(HeaderName, HeaderValue)>,
    body: Bytes,
}

impl IntoResponse for Relayed {
    fn into_response(self) -> Response {
        let mut resp = Response::new(Body::from(self.body));
        *resp.status_mut() = self.status;
        for (k, v) in self.headers {
            resp.headers_mut().insert(k, v);
        }
        resp
    }
}

/// One land's forward: the client's body and token, sent to the holder.
pub struct HttpForward {
    pub coord: Arc<Coordination>,
    pub body: Bytes,
    pub authorization: Option<HeaderValue>,
    /// The client request's id, so the holder logs the land under it too.
    pub request_id: Option<HeaderValue>,
    /// Where on the holder: `v1/land`, or a ref's `rebase-onto`.
    pub path: String,
    /// `Leep-Session` and `Leep-Notices`, so the holder answers the land's
    /// notices for the caller's session (ADR 0096).
    pub session: Vec<(axum::http::HeaderName, HeaderValue)>,
}

impl HttpForward {
    pub fn new(coord: Arc<Coordination>, headers: &HeaderMap, body: Bytes) -> Self {
        HttpForward {
            coord,
            body,
            authorization: headers.get(header::AUTHORIZATION).cloned(),
            request_id: headers.get(leep_core::api::REQUEST_ID_HEADER).cloned(),
            path: "v1/land".into(),
            session: [
                leep_core::api::SESSION_HEADER,
                leep_core::api::NOTICES_HEADER,
            ]
            .into_iter()
            .filter_map(|h| {
                headers
                    .get(h)
                    .map(|v| (axum::http::HeaderName::from_static(h), v.clone()))
            })
            .collect(),
        }
    }

    /// The same forward to `path` (relative, as `v1/land`).
    pub fn to(mut self, path: impl Into<String>) -> Self {
        self.path = path.into();
        self
    }
}

#[async_trait]
impl LandForward<Relayed> for HttpForward {
    async fn forward(&self, lease: &Lease) -> Forwarded<Relayed> {
        let url = match self.coord.policy.check_url(&lease.address) {
            Ok(u) => u,
            Err(why) => {
                tracing::warn!(r#ref = %lease.key.name, holder = %lease.holder, "{why}");
                return Forwarded::failed("not_allowed", why);
            }
        };
        let url = match url.join(&self.path) {
            Ok(u) => u,
            Err(e) => return Forwarded::failed("not_allowed", e.to_string()),
        };
        let mut req = self
            .coord
            .client
            .post(url)
            .header(header::CONTENT_TYPE, "application/json")
            .header(
                FORWARD_HEADER,
                format!(
                    "v={LAND_FORWARD_PROTOCOL}; epoch={}; from={}",
                    lease.epoch, self.coord.holder
                ),
            )
            .body(self.body.clone());
        if let Some(a) = &self.authorization {
            req = req.header(header::AUTHORIZATION, a.clone());
        }
        if let Some(id) = &self.request_id {
            req = req.header(leep_core::api::REQUEST_ID_HEADER, id.clone());
        }
        for (name, value) in &self.session {
            req = req.header(name.clone(), value.clone());
        }
        let resp = match req.send().await {
            Ok(r) => r,
            Err(e) => return Forwarded::failed(send_failure(&e), e.to_string()),
        };
        let status = resp.status();
        if status == StatusCode::SERVICE_UNAVAILABLE
            && resp.headers().contains_key(NOT_LEADER_HEADER)
        {
            return Forwarded::NotLeader;
        }
        // `Leep-Refused` tells the client nothing was done (ADR 0060), which
        // it needs to resend an unkeyed land the leader refused.
        let headers: Vec<_> = [
            header::CONTENT_TYPE,
            header::RETRY_AFTER,
            HeaderName::from_static(leep_core::api::REFUSED_HEADER),
        ]
        .into_iter()
        .filter_map(|k| resp.headers().get(&k).cloned().map(|v| (k, v)))
        .collect();
        match resp.bytes().await {
            Ok(body) => Forwarded::Answered(Relayed {
                status,
                headers,
                body,
            }),
            Err(e) => Forwarded::failed("reset", e.to_string()),
        }
    }
}

/// Metric label for a forward that got no answer.
fn send_failure(e: &reqwest::Error) -> &'static str {
    if e.is_timeout() {
        "timeout"
    } else if e.is_connect() {
        "connect"
    } else if e.is_redirect() {
        "redirect"
    } else {
        "reset"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A land forwarded to the lease holder keeps the client's request id:
    /// the holder answers (and logs) under it, so both servers' lines match.
    #[tokio::test]
    async fn a_forwarded_land_keeps_the_request_id() {
        use leep_core::memory::{MemoryBlobStore, MemoryMetaStore};
        use leep_core::{ChangesetId, Hash, LeaseKey};

        let holder_repo = leep_derive::Repo::new(
            Arc::new(MemoryBlobStore::new()),
            Arc::new(MemoryMetaStore::new()),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(crate::serve(
            listener,
            crate::router(holder_repo),
            std::future::pending(),
        ));

        let coord = Coordination::new(
            PeerPolicy {
                prefixes: vec!["http://127.0.0.1:".into()],
                ..PeerPolicy::default()
            },
            "sender".into(),
        )
        .unwrap();
        let mut headers = HeaderMap::new();
        headers.insert(
            leep_core::api::REQUEST_ID_HEADER,
            HeaderValue::from_static("client-run:42"),
        );
        // A land the holder refuses (unknown changeset), so it answers with
        // an error body naming the request.
        let body = serde_json::to_vec(&leep_core::api::LandRequest {
            changeset: ChangesetId(Hash::of_blob(b"never stored")),
            target_ref: "main".into(),
            create: None,
            land_id: None,
        })
        .unwrap();
        let forward = HttpForward::new(coord, &headers, Bytes::from(body));
        let lease = Lease {
            key: LeaseKey::new("land", "main"),
            holder: "holder".into(),
            address: format!("http://{addr}/"),
            protocol: LAND_FORWARD_PROTOCOL,
            meta: String::new(),
            epoch: 1,
            remaining: Duration::from_secs(2),
        };
        let Forwarded::Answered(relayed) = forward.forward(&lease).await else {
            panic!("the holder did not answer");
        };
        assert_eq!(relayed.status, StatusCode::NOT_FOUND);
        let e: ErrorResponse = serde_json::from_slice(&relayed.body).unwrap();
        assert_eq!(e.request_id.as_deref(), Some("client-run:42"));
        assert_eq!(e.code.as_deref(), Some("not_found"));
    }

    /// A refusal from the holder reaches the client with the headers that
    /// say what to do about it: `Retry-After`, and `Leep-Refused`, which
    /// tells it nothing was done (ADR 0060). Other headers stay behind.
    #[tokio::test]
    async fn a_relayed_refusal_keeps_retry_after_and_leep_refused() {
        use leep_core::LeaseKey;

        let holder = axum::Router::new().route(
            "/v1/land",
            axum::routing::post(|| async {
                let mut resp = crate::auth::reject(
                    StatusCode::SERVICE_UNAVAILABLE,
                    leep_core::api::error_code::UNAVAILABLE,
                    "server busy",
                );
                let h = resp.headers_mut();
                h.insert(header::RETRY_AFTER, HeaderValue::from_static("7"));
                h.insert(
                    leep_core::api::REFUSED_HEADER,
                    HeaderValue::from_static("admission"),
                );
                h.insert("x-holder-only", HeaderValue::from_static("1"));
                resp
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(crate::serve(listener, holder, std::future::pending()));
        let coord = Coordination::new(
            PeerPolicy {
                prefixes: vec!["http://127.0.0.1:".into()],
                ..PeerPolicy::default()
            },
            "sender".into(),
        )
        .unwrap();
        let forward = HttpForward::new(coord, &HeaderMap::new(), Bytes::from_static(b"{}"));
        let lease = Lease {
            key: LeaseKey::new("land", "main"),
            holder: "holder".into(),
            address: format!("http://{addr}/"),
            protocol: LAND_FORWARD_PROTOCOL,
            meta: String::new(),
            epoch: 1,
            remaining: Duration::from_secs(2),
        };
        let Forwarded::Answered(relayed) = forward.forward(&lease).await else {
            panic!("the holder did not answer");
        };
        let resp = relayed.into_response();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
        let h = resp.headers();
        assert_eq!(h[header::RETRY_AFTER], "7");
        assert_eq!(h[leep_core::api::REFUSED_HEADER], "admission");
        assert_eq!(h[header::CONTENT_TYPE], "application/json");
        assert!(h.get("x-holder-only").is_none());
    }

    fn policy(prefixes: &[&str], cidrs: &[&str], plain: bool) -> PeerPolicy {
        PeerPolicy {
            prefixes: prefixes.iter().map(|s| s.to_string()).collect(),
            cidrs: cidrs.iter().map(|s| s.parse().unwrap()).collect(),
            allow_plain_http: plain,
        }
    }

    #[test]
    fn cidrs_parse_and_match() {
        let c: Cidr = "10.1.0.0/16".parse().unwrap();
        assert!(c.contains("10.1.200.3".parse().unwrap()));
        assert!(!c.contains("10.2.0.1".parse().unwrap()));
        assert!(c.contains("::ffff:10.1.0.9".parse().unwrap()), "mapped v4");
        let one: Cidr = "192.168.1.5".parse().unwrap();
        assert!(one.contains("192.168.1.5".parse().unwrap()));
        assert!(!one.contains("192.168.1.6".parse().unwrap()));
        let all: Cidr = "0.0.0.0/0".parse().unwrap();
        assert!(all.contains("8.8.8.8".parse().unwrap()));
        let v6: Cidr = "fd00::/8".parse().unwrap();
        assert!(v6.contains("fd12::1".parse().unwrap()));
        assert!("10.0.0.0/33".parse::<Cidr>().is_err());
        assert!("leep.internal".parse::<Cidr>().is_err());
    }

    #[test]
    fn only_listed_addresses_get_a_token() {
        let p = policy(
            &["https://leep-2.internal:7070/", "http://127.0.0.1:"],
            &["10.0.0.0/8"],
            false,
        );
        assert!(p.check_url("https://leep-2.internal:7070").is_ok());
        assert!(p.check_url("https://leep-2.internal:7070/").is_ok());
        assert!(p.check_url("https://leep-3.internal:7070/").is_err());
        assert!(p.check_url("http://127.0.0.1:7391").is_ok());
        assert!(p.check_url("http://127.0.0.10:7391").is_err());
        assert!(p.check_url("http://10.3.4.5:7070").is_ok());
        assert!(p.check_url("https://evil.example").is_err());
        assert!(p.check_url("http://11.0.0.1:7070").is_err());
        assert!(p.check_url("ftp://10.0.0.1").is_err());
        assert!(p.check_url("http://user:pw@10.0.0.1:7070").is_err());
        assert!(p.check_url("not a url").is_err());
        // Plain http only to loopback or private hosts, unless allowed.
        let p = policy(
            &["http://leep-1.internal:7070/", "http://8.8.8.8:"],
            &[],
            false,
        );
        assert!(p.check_url("http://leep-1.internal:7070").is_err());
        assert!(p.check_url("http://8.8.8.8:7070").is_err());
        let p = policy(&["http://leep-1.internal:7070/"], &[], true);
        assert!(p.check_url("http://leep-1.internal:7070").is_ok());
    }

    /// A prefix is an origin, compared parsed: a longer host name, another
    /// port, scheme or path, or credentials do not pass for it.
    #[test]
    fn prefixes_match_whole_origins() {
        let p = policy(&["https://leep.example.com"], &[], false);
        assert!(p.check_url("https://leep.example.com/").is_ok());
        assert!(p.check_url("https://LEEP.example.com:443/").is_ok());
        assert!(p.check_url("https://leep.example.com.evil.net/").is_err());
        assert!(p.check_url("https://leep.example.community/").is_err());
        assert!(p.check_url("https://leep.example.com:8443/").is_err());
        assert!(p.check_url("http://leep.example.com/").is_err());
        assert!(p.check_url("https://leep.example.com@evil.net/").is_err());
        let p = policy(&["https://gw.example.com/leep/"], &[], false);
        assert!(p.check_url("https://gw.example.com/leep/").is_ok());
        assert!(p.check_url("https://gw.example.com/other/").is_err());
        let p = policy(&["http://[fd00::7]:7070/"], &[], false);
        assert!(p.check_url("http://[fd00::7]:7070").is_ok());
        assert!(p.is_peer("fd00::7".parse().unwrap()));
        // Partial hosts and other malformed prefixes are refused at startup.
        for bad in [
            "http://10.0.3.",
            "10.0.3.7:7070",
            "ftp://10.0.0.1/",
            "https://u:p@leep/",
            "https://leep-",
            "https://leep-:7070/",
            "https://leep.internal.",
            "https://leep..internal/",
        ] {
            assert!(policy(&[bad], &[], false).validate().is_err(), "{bad}");
        }
        for good in [
            "http://10.0.3.7:7070/",
            "http://127.0.0.1:",
            "https://leep-1.internal:7070/",
            "http://leep_server:7070/",
            "http://[::1]:",
        ] {
            assert!(policy(&[good], &[], false).validate().is_ok(), "{good}");
        }
    }

    #[test]
    fn peers_are_the_listed_networks_and_prefix_hosts() {
        let p = policy(&["http://127.0.0.1:"], &["10.0.0.0/8"], false);
        assert!(p.is_peer("127.0.0.1".parse().unwrap()));
        assert!(p.is_peer("10.9.9.9".parse().unwrap()));
        assert!(!p.is_peer("192.168.0.1".parse().unwrap()));
        let p = policy(&["https://leep-1.internal"], &[], false);
        assert!(
            !p.is_peer("10.0.0.1".parse().unwrap()),
            "names are not resolved"
        );
        assert!(!p.recognises_peers(), "names alone recognise no peer");
        assert!(policy(&["http://127.0.0.1:7070/"], &[], false).recognises_peers());
        assert!(policy(&[], &["10.0.0.0/8"], false).recognises_peers());
    }
}
