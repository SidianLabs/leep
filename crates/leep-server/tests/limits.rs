//! Per-principal request limits (ADR 0020), through the real router.
//! Rate tests drive a manual clock, so none of them sleeps.

use std::convert::Infallible;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::body::{Body, Bytes};
use axum::extract::ConnectInfo;
use axum::http::{header, Method, Request, StatusCode};
use axum::Router;
use http_body_util::BodyExt;
use leep_core::api::ErrorResponse;
use leep_core::memory::{MemoryBlobStore, MemoryMetaStore};
use leep_core::Hash;
use leep_derive::Repo;
use leep_server::auth::{generate, hash_token, Auth, Role};
use leep_server::limits::{Defaults, Limiter, ManualClock};
use metrics_exporter_prometheus::PrometheusBuilder;
use tokio::sync::mpsc;
use tower::ServiceExt;

const A: &str = "token-a-for-tests";
const B: &str = "token-b-for-tests";
const V: &str = "viewer-token-for-tests";

fn repo() -> Repo {
    Repo::new(
        Arc::new(MemoryBlobStore::new()),
        Arc::new(MemoryMetaStore::new()),
    )
}

/// Tokens `a` and `b` (write) and `viewer` (read), each entry extended with
/// the extra fields given for its name.
fn auth(extra: &[(&str, serde_json::Value)]) -> Auth {
    let mut tokens = vec![
        serde_json::json!({ "name": "a", "role": "write", "hash": hash_token(A) }),
        serde_json::json!({ "name": "b", "role": "write", "hash": hash_token(B) }),
        serde_json::json!({ "name": "viewer", "role": "read", "hash": hash_token(V) }),
    ];
    for t in &mut tokens {
        for (name, fields) in extra {
            if t["name"] == *name {
                t.as_object_mut()
                    .unwrap()
                    .extend(fields.as_object().unwrap().clone());
            }
        }
    }
    Auth::parse(&serde_json::json!({ "tokens": tokens }).to_string()).unwrap()
}

struct Rig {
    app: Router,
    clock: Arc<ManualClock>,
}

fn rig(extra: &[(&str, serde_json::Value)], defaults: Defaults, timeout: Duration) -> Rig {
    let clock = Arc::new(ManualClock::new());
    let a = auth(extra);
    let limiter = Limiter::with_clock(Some(&a), &defaults, clock.clone())
        .unwrap()
        .map(Arc::new);
    let app =
        leep_server::router_with_limits(repo(), 1 << 20, Some(a), None, None, timeout, limiter);
    Rig { app, clock }
}

fn rig_with(extra: &[(&str, serde_json::Value)]) -> Rig {
    rig(extra, Defaults::default(), Duration::from_secs(60))
}

#[derive(Debug)]
struct Reply {
    status: StatusCode,
    retry_after: Option<String>,
    body: Vec<u8>,
}

impl Reply {
    fn error(&self) -> String {
        serde_json::from_slice::<ErrorResponse>(&self.body)
            .unwrap_or_else(|e| panic!("not a JSON error ({e}): {:?}", self.body))
            .error
    }
}

async fn send(app: &Router, req: Request<Body>) -> Reply {
    let resp = app.clone().oneshot(req).await.unwrap();
    let (parts, body) = resp.into_parts();
    Reply {
        status: parts.status,
        retry_after: parts
            .headers
            .get(header::RETRY_AFTER)
            .map(|v| v.to_str().unwrap().to_string()),
        body: body.collect().await.unwrap().to_bytes().to_vec(),
    }
}

fn request(method: Method, uri: &str, token: &str, body: Body) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(uri)
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .body(body)
        .unwrap()
}

async fn get(app: &Router, token: &str) -> Reply {
    send(app, request(Method::GET, "/v1/refs", token, Body::empty())).await
}

fn blob_uri(data: &[u8]) -> String {
    format!("/v1/blobs/{}", Hash::of_blob(data))
}

async fn put(app: &Router, token: &str, data: &[u8]) -> Reply {
    let req = request(
        Method::PUT,
        &blob_uri(data),
        token,
        Body::from(data.to_vec()),
    );
    send(app, req).await
}

async fn statuses(app: &Router, token: &str, n: usize) -> Vec<u16> {
    let mut out = Vec::new();
    for _ in 0..n {
        out.push(get(app, token).await.status.as_u16());
    }
    out
}

#[tokio::test]
async fn rate_limit_answers_429_with_retry_after_and_a_json_body() {
    let r = rig_with(&[("a", serde_json::json!({ "rate": 1, "burst": 2 }))]);
    assert_eq!(statuses(&r.app, A, 2).await, [200, 200]);
    let limited = get(&r.app, A).await;
    assert_eq!(limited.status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(limited.retry_after.as_deref(), Some("1"));
    assert!(
        limited.error().contains("rate limit"),
        "{}",
        limited.error()
    );

    r.clock.advance(Duration::from_secs(1));
    assert_eq!(statuses(&r.app, A, 2).await, [200, 429]);
}

#[tokio::test]
async fn retry_after_rounds_up_to_whole_seconds() {
    let r = rig_with(&[("a", serde_json::json!({ "rate": 0.4, "burst": 1 }))]);
    assert_eq!(get(&r.app, A).await.status, StatusCode::OK);
    // One token takes 2.5s to come back.
    assert_eq!(get(&r.app, A).await.retry_after.as_deref(), Some("3"));
    r.clock.advance(Duration::from_secs(2));
    assert_eq!(get(&r.app, A).await.retry_after.as_deref(), Some("1"));
    r.clock.advance(Duration::from_millis(500));
    assert_eq!(get(&r.app, A).await.status, StatusCode::OK);
}

#[tokio::test]
async fn limiting_one_token_leaves_the_others_alone() {
    let r = rig_with(&[("a", serde_json::json!({ "rate": 1, "burst": 1 }))]);
    assert_eq!(statuses(&r.app, A, 3).await, [200, 429, 429]);
    assert_eq!(statuses(&r.app, B, 10).await, [200; 10]);
    assert_eq!(statuses(&r.app, V, 10).await, [200; 10]);
}

#[tokio::test]
async fn reads_and_writes_have_separate_buckets() {
    let r = rig_with(&[("a", serde_json::json!({ "rate": 1, "burst": 1 }))]);
    assert_eq!(get(&r.app, A).await.status, StatusCode::OK);
    assert_eq!(get(&r.app, A).await.status, StatusCode::TOO_MANY_REQUESTS);
    // The read bucket is empty, the write bucket untouched.
    assert_eq!(put(&r.app, A, b"x").await.status, StatusCode::NO_CONTENT);
    assert_eq!(
        put(&r.app, A, b"y").await.status,
        StatusCode::TOO_MANY_REQUESTS
    );
    // `POST /v1/blobs/missing` is a query, so it draws on the read bucket.
    let req = request(
        Method::POST,
        "/v1/blobs/missing",
        A,
        Body::from("{\"hashes\":[]}"),
    );
    assert_eq!(
        send(&r.app, req).await.status,
        StatusCode::TOO_MANY_REQUESTS
    );
}

#[tokio::test]
async fn read_token_is_limited_independently_of_write_tokens() {
    let r = rig_with(&[("viewer", serde_json::json!({ "rate": 1, "burst": 1 }))]);
    assert_eq!(statuses(&r.app, V, 2).await, [200, 429]);
    assert_eq!(statuses(&r.app, A, 5).await, [200; 5]);
}

#[tokio::test]
async fn server_wide_defaults_apply_to_every_token_and_entries_override_them() {
    let defaults = Defaults {
        rate: Some(1.0),
        burst: Some(1),
        ..Default::default()
    };
    let r = rig(
        &[
            ("b", serde_json::json!({ "rate": 3, "burst": 3 })),
            ("viewer", serde_json::json!({ "rate": "unlimited" })),
        ],
        defaults,
        Duration::from_secs(60),
    );
    assert_eq!(statuses(&r.app, A, 2).await, [200, 429]);
    assert_eq!(statuses(&r.app, B, 4).await, [200, 200, 200, 429]);
    assert_eq!(statuses(&r.app, V, 5).await, [200; 5]);
}

#[tokio::test]
async fn unauthenticated_requests_never_spend_a_tokens_budget() {
    let r = rig_with(&[("a", serde_json::json!({ "rate": 1, "burst": 1 }))]);
    for _ in 0..3 {
        let bad = get(&r.app, "wrong").await;
        assert_eq!(bad.status, StatusCode::UNAUTHORIZED);
    }
    assert_eq!(get(&r.app, A).await.status, StatusCode::OK);
}

#[tokio::test]
async fn health_is_never_limited() {
    let defaults = Defaults {
        rate: Some(1.0),
        burst: Some(1),
        concurrency: Some(1),
        ..Default::default()
    };
    let clock = Arc::new(ManualClock::new());
    let limiter = Limiter::with_clock(None, &defaults, clock)
        .unwrap()
        .map(Arc::new);
    let app = leep_server::router_with_limits(
        repo(),
        1 << 20,
        None,
        None,
        None,
        Duration::from_secs(60),
        limiter,
    );
    for _ in 0..5 {
        let req = Request::builder()
            .uri("/v1/health")
            .body(Body::empty())
            .unwrap();
        assert_eq!(send(&app, req).await.status, StatusCode::OK);
    }
}

#[tokio::test]
async fn no_limits_configured_means_no_limiter_and_nothing_changes() {
    let a = auth(&[]);
    assert!(Limiter::build(Some(&a), &Defaults::default())
        .unwrap()
        .is_none());
    let app = leep_server::router_with_limits(
        repo(),
        1 << 20,
        Some(a),
        None,
        None,
        Duration::from_secs(60),
        None,
    );
    assert_eq!(statuses(&app, A, 50).await, [200; 50]);
    assert_eq!(put(&app, A, b"x").await.status, StatusCode::NO_CONTENT);
}

fn anonymous_app(defaults: Defaults) -> Router {
    let limiter = Limiter::with_clock(None, &defaults, Arc::new(ManualClock::new()))
        .unwrap()
        .map(Arc::new);
    leep_server::router_with_limits(
        repo(),
        1 << 20,
        None,
        None,
        None,
        Duration::from_secs(60),
        limiter,
    )
}

async fn get_from(app: &Router, addr: Option<&str>) -> StatusCode {
    let mut req = Request::builder()
        .uri("/v1/refs")
        .body(Body::empty())
        .unwrap();
    if let Some(a) = addr {
        let a: SocketAddr = a.parse().unwrap();
        req.extensions_mut().insert(ConnectInfo(a));
    }
    send(app, req).await.status
}

#[tokio::test]
async fn anonymous_mode_limits_each_client_address() {
    let app = anonymous_app(Defaults {
        rate: Some(1.0),
        burst: Some(2),
        ..Default::default()
    });
    let (x, y) = ("10.0.0.1:5000", "10.0.0.2:5000");
    assert_eq!(get_from(&app, Some(x)).await, StatusCode::OK);
    assert_eq!(get_from(&app, Some(x)).await, StatusCode::OK);
    assert_eq!(get_from(&app, Some(x)).await, StatusCode::TOO_MANY_REQUESTS);
    // The port is not part of the key; another address is.
    assert_eq!(
        get_from(&app, Some("10.0.0.1:6000")).await,
        StatusCode::TOO_MANY_REQUESTS
    );
    assert_eq!(get_from(&app, Some(y)).await, StatusCode::OK);
}

#[tokio::test]
async fn anonymous_requests_without_a_peer_address_share_one_bucket() {
    let app = anonymous_app(Defaults {
        rate: Some(1.0),
        burst: Some(1),
        ..Default::default()
    });
    assert_eq!(get_from(&app, None).await, StatusCode::OK);
    assert_eq!(get_from(&app, None).await, StatusCode::TOO_MANY_REQUESTS);
}

#[tokio::test]
async fn per_token_body_limit_is_a_413_with_a_json_body() {
    let r = rig_with(&[("a", serde_json::json!({ "max_body": 10 }))]);
    assert_eq!(
        put(&r.app, A, b"0123456789").await.status,
        StatusCode::NO_CONTENT
    );
    let big = put(&r.app, A, b"0123456789a").await;
    assert_eq!(big.status, StatusCode::PAYLOAD_TOO_LARGE);
    assert!(big.error().contains("10 bytes"), "{}", big.error());
    // Another token keeps the global limit.
    assert_eq!(
        put(&r.app, B, b"0123456789a").await.status,
        StatusCode::NO_CONTENT
    );
}

#[tokio::test]
async fn per_token_body_limit_covers_bodies_with_no_declared_length() {
    let r = rig_with(&[("a", serde_json::json!({ "max_body": 10 }))]);
    let chunks = futures_util::stream::iter(
        ["0123456", "789abcd"].map(|c| Ok::<_, Infallible>(Bytes::from(c))),
    );
    let req = request(
        Method::PUT,
        &blob_uri(b"0123456789abcd"),
        A,
        Body::from_stream(chunks),
    );
    let reply = send(&r.app, req).await;
    assert_eq!(reply.status, StatusCode::PAYLOAD_TOO_LARGE);
    assert!(reply.error().contains("10 bytes"), "{}", reply.error());
}

#[tokio::test]
async fn read_tokens_get_a_small_body_limit_by_default() {
    let defaults = Defaults {
        read_max_body: Some(64),
        ..Default::default()
    };
    let r = rig(&[], defaults, Duration::from_secs(60));
    let missing = |n: usize| {
        let hashes = vec![Hash::of_blob(b"x").to_string(); n];
        let body = serde_json::json!({ "hashes": hashes }).to_string();
        request(Method::POST, "/v1/blobs/missing", V, Body::from(body))
    };
    assert_eq!(send(&r.app, missing(0)).await.status, StatusCode::OK);
    assert_eq!(
        send(&r.app, missing(5)).await.status,
        StatusCode::PAYLOAD_TOO_LARGE
    );
    // Write tokens are not affected by the read default.
    let body = serde_json::json!({ "hashes": vec![Hash::of_blob(b"x").to_string(); 5] });
    let req = request(
        Method::POST,
        "/v1/blobs/missing",
        A,
        Body::from(body.to_string()),
    );
    assert_eq!(send(&r.app, req).await.status, StatusCode::OK);
}

#[tokio::test]
async fn a_token_can_lift_the_read_body_default() {
    let defaults = Defaults {
        read_max_body: Some(8),
        ..Default::default()
    };
    let r = rig(
        &[("viewer", serde_json::json!({ "max_body": "unlimited" }))],
        defaults,
        Duration::from_secs(60),
    );
    let body = serde_json::json!({ "hashes": [Hash::of_blob(b"x").to_string()] });
    let req = request(
        Method::POST,
        "/v1/blobs/missing",
        V,
        Body::from(body.to_string()),
    );
    assert_eq!(send(&r.app, req).await.status, StatusCode::OK);
}

/// A PUT whose body the test controls: it stays in flight until `tx` sends
/// the blob and is dropped, which completes it.
fn held_put(token: &'static str) -> (mpsc::Sender<Bytes>, Request<Body>) {
    let (tx, rx) = mpsc::channel::<Bytes>(4);
    let stream = futures_util::stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|b| (Ok::<_, Infallible>(b), rx))
    });
    let req = request(
        Method::PUT,
        &blob_uri(b"x"),
        token,
        Body::from_stream(stream),
    );
    (tx, req)
}

/// Yield until a request by `token` is refused, which means the permit held by
/// the request started earlier is really taken.
async fn until_busy(app: &Router, token: &str) {
    for _ in 0..200 {
        if get(app, token).await.status == StatusCode::TOO_MANY_REQUESTS {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("the held request never took its permit");
}

fn one_at_a_time() -> [(&'static str, serde_json::Value); 1] {
    [("a", serde_json::json!({ "concurrency": 1 }))]
}

fn spawn_request(app: &Router, req: Request<Body>) -> tokio::task::JoinHandle<Reply> {
    let app = app.clone();
    tokio::spawn(async move { send(&app, req).await })
}

#[tokio::test]
async fn concurrency_limit_answers_429_while_a_request_is_in_flight() {
    let r = rig_with(&one_at_a_time());
    let (tx, req) = held_put(A);
    let first = spawn_request(&r.app, req);
    until_busy(&r.app, A).await;

    let busy = get(&r.app, A).await;
    assert_eq!(busy.status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(busy.retry_after.as_deref(), Some("1"));
    assert!(busy.error().contains("concurrent"), "{}", busy.error());
    // The limit is per token, and covers reads and writes together.
    assert_eq!(get(&r.app, B).await.status, StatusCode::OK);
    assert_eq!(
        put(&r.app, A, b"y").await.status,
        StatusCode::TOO_MANY_REQUESTS
    );

    // Finishing the request frees the permit.
    tx.send(Bytes::from_static(b"x")).await.unwrap();
    drop(tx);
    assert_eq!(first.await.unwrap().status, StatusCode::NO_CONTENT);
    assert_eq!(get(&r.app, A).await.status, StatusCode::OK);
}

#[tokio::test]
async fn permit_is_released_when_the_client_cancels() {
    let r = rig_with(&one_at_a_time());
    let (_tx, req) = held_put(A);
    let first = spawn_request(&r.app, req);
    until_busy(&r.app, A).await;

    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    assert_eq!(statuses(&r.app, A, 3).await, [200; 3]);
}

#[tokio::test]
async fn permit_is_released_when_the_request_times_out() {
    let r = rig(
        &one_at_a_time(),
        Defaults::default(),
        Duration::from_millis(100),
    );
    let (_tx, req) = held_put(A);
    let first = spawn_request(&r.app, req);
    until_busy(&r.app, A).await;

    assert_eq!(first.await.unwrap().status, StatusCode::REQUEST_TIMEOUT);
    assert_eq!(statuses(&r.app, A, 3).await, [200; 3]);
}

#[tokio::test]
async fn permit_is_held_until_the_response_body_is_finished() {
    let r = rig_with(&one_at_a_time());
    let resp = r
        .app
        .clone()
        .oneshot(request(Method::GET, "/v1/refs", A, Body::empty()))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    // The head has been returned but the body not read: still in flight.
    assert_eq!(get(&r.app, A).await.status, StatusCode::TOO_MANY_REQUESTS);
    resp.into_body().collect().await.unwrap();
    assert_eq!(get(&r.app, A).await.status, StatusCode::OK);
}

#[tokio::test]
async fn permit_is_released_when_a_response_body_is_dropped_unread() {
    let r = rig_with(&one_at_a_time());
    let resp = r
        .app
        .clone()
        .oneshot(request(Method::GET, "/v1/refs", A, Body::empty()))
        .await
        .unwrap();
    assert_eq!(get(&r.app, A).await.status, StatusCode::TOO_MANY_REQUESTS);
    drop(resp);
    assert_eq!(get(&r.app, A).await.status, StatusCode::OK);
}

#[tokio::test]
async fn rejected_requests_do_not_leak_permits() {
    let r = rig_with(&[(
        "a",
        serde_json::json!({ "concurrency": 2, "rate": 1, "burst": 1 }),
    )]);
    assert_eq!(statuses(&r.app, A, 20).await[0], 200);
    r.clock.advance(Duration::from_secs(1));
    assert_eq!(get(&r.app, A).await.status, StatusCode::OK);
}

fn with_metrics<F: Future<Output = ()>>(fut: F) -> String {
    let recorder = PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    metrics::with_local_recorder(&recorder, || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(fut)
    });
    handle.render()
}

fn series<'a>(text: &'a str, name: &str, parts: &[&str]) -> &'a str {
    text.lines()
        .find(|l| l.starts_with(name) && parts.iter().all(|p| l.contains(p)))
        .unwrap_or_else(|| panic!("no {name} line with {parts:?} in:\n{text}"))
}

fn value(line: &str) -> f64 {
    line.rsplit(' ').next().unwrap().parse().unwrap()
}

#[test]
fn limited_requests_are_counted_by_reason_and_token_name() {
    let text = with_metrics(async {
        let r = rig_with(&[(
            "a",
            serde_json::json!({ "rate": 1, "burst": 1, "concurrency": 1, "max_body": 4 }),
        )]);
        get(&r.app, A).await;
        get(&r.app, A).await; // rate
        get(&r.app, A).await; // rate
        r.clock.advance(Duration::from_secs(1));
        let held = r
            .app
            .clone()
            .oneshot(request(Method::GET, "/v1/refs", A, Body::empty()))
            .await
            .unwrap();
        r.clock.advance(Duration::from_secs(1));
        get(&r.app, A).await; // concurrency
        drop(held);
        r.clock.advance(Duration::from_secs(1));
        put(&r.app, A, b"too big").await; // size
    });
    let limited = |reason: &str| {
        value(series(
            &text,
            "leep_requests_limited_total",
            &[&format!("reason=\"{reason}\""), "token=\"a\""],
        ))
    };
    assert_eq!(limited("rate"), 2.0);
    assert_eq!(limited("concurrency"), 1.0);
    assert_eq!(limited("size"), 1.0);
    assert!(!text.contains(A) && !text.contains(&hash_token(A)));
    // Everything finished, so nothing is left in flight.
    let g = series(&text, "leep_requests_in_flight", &["token=\"a\""]);
    assert_eq!(value(g), 0.0, "{text}");
}

#[test]
fn tokens_file_without_limit_fields_still_parses() {
    let text = serde_json::json!({ "tokens": [
        { "name": "ci", "role": "write", "hash": hash_token("a") },
    ]})
    .to_string();
    let a = Auth::parse(&text).unwrap();
    assert!(a.token_limits().all(|(_, _, l)| l.rate.is_none()));
    assert!(Limiter::build(Some(&a), &Defaults::default())
        .unwrap()
        .is_none());
}

#[test]
fn generated_entries_carry_no_limit_fields() {
    let (_, entry) = generate("ci", Role::Write).unwrap();
    let v = serde_json::to_value(&entry).unwrap();
    assert_eq!(v.as_object().unwrap().len(), 3, "{v}");
}

#[test]
fn tokens_file_rejects_bad_limits_and_unknown_fields() {
    let parse = |extra: serde_json::Value| {
        let mut t = serde_json::json!({ "name": "ci", "role": "write", "hash": hash_token("a") });
        t.as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        Auth::parse(&serde_json::json!({ "tokens": [t] }).to_string()).map(|_| ())
    };
    assert!(parse(
        serde_json::json!({ "rate": 5, "burst": 10, "concurrency": 2, "max_body": 100 })
    )
    .is_ok());
    assert!(parse(serde_json::json!({ "rate": "unlimited", "concurrency": "unlimited", "max_body": "unlimited" })).is_ok());
    for (extra, want) in [
        (serde_json::json!({ "rate": 0 }), "rate must be"),
        (serde_json::json!({ "rate": -1 }), "rate must be"),
        (serde_json::json!({ "burst": 0 }), "burst must be"),
        (serde_json::json!({ "burst": 1.5 }), "burst must be"),
        (
            serde_json::json!({ "concurrency": 0 }),
            "concurrency must be",
        ),
        (serde_json::json!({ "max_body": 0 }), "max_body must be"),
        (serde_json::json!({ "rate": "fast" }), "unlimited"),
        (serde_json::json!({ "rate": true }), "unlimited"),
        (serde_json::json!({ "rps": 5 }), "unknown field"),
    ] {
        let e = parse(extra.clone()).unwrap_err();
        assert!(e.contains(want), "{extra}: {e}");
    }
}

#[test]
fn burst_without_a_rate_fails_at_startup_with_the_token_name() {
    let a = auth(&[("b", serde_json::json!({ "burst": 3 }))]);
    let e = Limiter::build(Some(&a), &Defaults::default())
        .err()
        .expect("must fail");
    assert!(
        e.contains("token \"b\"") && e.contains("burst needs a rate"),
        "{e}"
    );
}

fn start(args: &[&str], tokens: Option<&str>) -> std::process::Output {
    let dir = tempfile::tempdir().unwrap();
    let mut cmd = std::process::Command::new(env!("CARGO_BIN_EXE_leep-server"));
    cmd.args([
        "--listen",
        "127.0.0.1:0",
        "--meta",
        "mem://",
        "--blobs",
        "mem://",
    ])
    .args(args);
    if let Some(t) = tokens {
        let path = dir.path().join("tokens.json");
        std::fs::write(&path, t).unwrap();
        cmd.arg("--tokens-file").arg(path);
    }
    cmd.output().unwrap()
}

#[test]
fn invalid_limit_settings_fail_startup_with_a_clear_error() {
    let entry = |extra: &str| {
        format!(
            r#"{{"tokens":[{{"name":"ci","role":"write","hash":"{}"{extra}}}]}}"#,
            hash_token("x")
        )
    };
    let cases: [(&[&str], Option<String>, &str); 6] = [
        (&["--rate-limit", "0"], None, "--rate-limit"),
        (&["--rate-limit=-3"], None, "--rate-limit"),
        (
            &["--rate-burst", "5"],
            None,
            "--rate-burst needs --rate-limit",
        ),
        (
            &["--max-concurrent-per-token", "0"],
            None,
            "--max-concurrent-per-token",
        ),
        (&["--read-max-body-bytes", "10"], None, "--tokens-file"),
        (&[], Some(entry(r#","burst":3"#)), "burst needs a rate"),
    ];
    for (args, tokens, want) in cases {
        let out = start(args, tokens.as_deref());
        assert!(!out.status.success(), "{args:?}");
        let err = String::from_utf8_lossy(&out.stderr);
        assert!(err.contains(want), "{args:?}: {err}");
    }
}
