//! Connection bounds of `leep_server::conn::serve_with`, over real sockets:
//! the header-read timeout, and the body idle timeout that frees what a
//! response holds when its client stops reading. Timeouts are short here.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::body::{Body, Bytes};
use axum::http::{header, Method, Request, StatusCode};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;
use http_body_util::BodyExt;
use leep_core::api::{CasRefRequest, CreateChangesetResponse};
use leep_core::memory::{MemoryBlobStore, MemoryMetaStore};
use leep_core::{Changeset, ChangesetId};
use leep_derive::Repo;
use leep_server::auth::{hash_token, Auth};
use leep_server::conn::ServeOptions;
use leep_server::limits::{Defaults, Limiter};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpSocket, TcpStream};
use tokio::sync::Semaphore;
use tower::ServiceExt;

const TOKEN: &str = "connections-test-token";

fn repo() -> Repo {
    Repo::new(
        Arc::new(MemoryBlobStore::new()),
        Arc::new(MemoryMetaStore::new()),
    )
}

fn opts(header: Duration, body_idle: Duration) -> ServeOptions {
    ServeOptions {
        header_read_timeout: Some(header),
        idle_timeout: Some(Duration::from_secs(60)),
        body_idle_timeout: Some(body_idle),
        max_connections: Some(64),
    }
}

async fn start(app: Router, opts: ServeOptions) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(leep_server::conn::serve_with(
        listener,
        app,
        std::future::pending(),
        opts,
    ));
    addr
}

/// Reads until the server closes the connection; how long that took.
async fn until_closed(mut s: TcpStream, limit: Duration) -> Duration {
    let started = Instant::now();
    let mut buf = [0u8; 4096];
    let read = async {
        loop {
            match s.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(_) => {}
            }
        }
    };
    tokio::time::timeout(limit, read)
        .await
        .expect("the server kept the connection open");
    started.elapsed()
}

#[tokio::test]
async fn a_partial_request_head_is_closed_after_the_header_timeout() {
    let header = Duration::from_millis(300);
    let addr = start(
        leep_server::router(repo()),
        opts(header, Duration::from_secs(30)),
    )
    .await;

    // Half a head, then nothing.
    let mut s = TcpStream::connect(addr).await.unwrap();
    s.write_all(b"GET /v1/health HTTP/1.1\r\nHost: 127.0.0.1\r\n")
        .await
        .unwrap();
    let took = until_closed(s, Duration::from_secs(10)).await;
    assert!(took >= header - Duration::from_millis(50), "{took:?}");

    // Not a byte: the protocol is not even known yet.
    let s = TcpStream::connect(addr).await.unwrap();
    let took = until_closed(s, Duration::from_secs(10)).await;
    assert!(took >= header - Duration::from_millis(50), "{took:?}");

    // A complete request is answered as before.
    let mut s = TcpStream::connect(addr).await.unwrap();
    s.write_all(b"GET /v1/health HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    let mut out = Vec::new();
    s.read_to_end(&mut out).await.unwrap();
    assert!(out.starts_with(b"HTTP/1.1 200"), "{out:?}");
}

#[tokio::test]
async fn a_request_body_that_stops_is_cut_off_after_the_body_idle_timeout() {
    let idle = Duration::from_millis(300);
    let addr = start(
        leep_server::router(repo()),
        opts(Duration::from_secs(30), idle),
    )
    .await;
    let mut s = TcpStream::connect(addr).await.unwrap();
    s.write_all(
        b"POST /v1/changesets HTTP/1.1\r\nHost: 127.0.0.1\r\n\
          Content-Type: application/json\r\nContent-Length: 1000\r\n\r\n{\"parents\"",
    )
    .await
    .unwrap();
    let took = until_closed(s, Duration::from_secs(10)).await;
    assert!(took >= idle - Duration::from_millis(50), "{took:?}");
}

/// A client with a tiny receive window, so a large response cannot be
/// buffered away between the server and it.
async fn slow_reader(addr: SocketAddr) -> TcpStream {
    let sock = TcpSocket::new_v4().unwrap();
    sock.set_recv_buffer_size(4096).unwrap();
    sock.connect(addr).await.unwrap()
}

/// Sends `GET path` and returns the status of the answer, leaving the body
/// unread on the socket.
async fn get_status(s: &mut TcpStream, path: &str, token: Option<&str>) -> u16 {
    let auth = token.map_or(String::new(), |t| format!("Authorization: Bearer {t}\r\n"));
    let req = format!("GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\n{auth}\r\n");
    s.write_all(req.as_bytes()).await.unwrap();
    let mut head = Vec::new();
    let mut b = [0u8; 1];
    while !head.ends_with(b"\r\n") {
        s.read_exact(&mut b).await.unwrap();
        head.push(b[0]);
    }
    let line = String::from_utf8(head).unwrap();
    line.split(' ').nth(1).unwrap().parse().unwrap()
}

/// Polls `GET path` on fresh connections until one answers 200; how long
/// that took.
async fn until_served(addr: SocketAddr, path: &str, token: Option<&str>) -> Duration {
    let started = Instant::now();
    loop {
        let mut s = TcpStream::connect(addr).await.unwrap();
        if get_status(&mut s, path, token).await == 200 {
            return started.elapsed();
        }
        assert!(
            started.elapsed() < Duration::from_secs(20),
            "never served again"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// A response that holds a permit in its body, as the read classes do,
/// and streams without end: the permit comes back only when the body is
/// dropped.
#[tokio::test]
async fn a_response_left_unread_releases_its_permit_after_the_body_idle_timeout() {
    let permits = Arc::new(Semaphore::new(1));
    let app = Router::new().route(
        "/hold",
        get(move || {
            let permits = permits.clone();
            async move {
                let Ok(permit) = permits.try_acquire_owned() else {
                    return StatusCode::SERVICE_UNAVAILABLE.into_response();
                };
                let chunk = Bytes::from(vec![b'x'; 64 * 1024]);
                let stream = futures_util::stream::repeat_with(move || {
                    let _held = &permit;
                    Ok::<_, std::io::Error>(chunk.clone())
                });
                axum::response::Response::new(Body::from_stream(stream))
            }
        }),
    );
    let idle = Duration::from_millis(500);
    let addr = start(app, opts(Duration::from_secs(30), idle)).await;

    let mut holder = slow_reader(addr).await;
    assert_eq!(get_status(&mut holder, "/hold", None).await, 200);
    let mut s = TcpStream::connect(addr).await.unwrap();
    assert_eq!(get_status(&mut s, "/hold", None).await, 503, "permit held");
    let took = until_served(addr, "/hold", None).await;
    assert!(took >= idle / 2, "served after {took:?}");
    drop(holder);
}

async fn call(app: &Router, method: Method, uri: &str, body: Vec<u8>) -> (StatusCode, Bytes) {
    let req = Request::builder()
        .method(method)
        .uri(uri)
        .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(body))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    (status, resp.into_body().collect().await.unwrap().to_bytes())
}

/// The same through the real router: a token limited to one request at a
/// time streams a long log to a client that stops reading. Its concurrency
/// slot is held by the body until the body idle timeout drops it.
#[tokio::test]
async fn a_streamed_log_left_unread_frees_the_tokens_slot() {
    let tokens = serde_json::json!({ "tokens": [
        { "name": "a", "role": "write", "hash": hash_token(TOKEN), "concurrency": 1 },
    ]});
    let auth = Auth::parse(&tokens.to_string()).unwrap();
    let limiter = Limiter::build(Some(&auth), &Defaults::default())
        .unwrap()
        .map(Arc::new);
    let app = leep_server::router_with_limits(
        repo(),
        1 << 20,
        Some(auth),
        None,
        None,
        Duration::from_secs(60),
        limiter,
    );
    // More entries than one streamed chunk, with long messages, so the log
    // is megabytes of JSON.
    let message = "m".repeat(8 * 1024);
    let mut head: Option<ChangesetId> = None;
    for _ in 0..600 {
        let cs = Changeset {
            parents: head.into_iter().collect(),
            author: "t".into(),
            timestamp: 1,
            message: message.clone(),
            changes: BTreeMap::new(),
            extra: BTreeMap::new(),
        };
        let (status, body) = call(
            &app,
            Method::POST,
            "/v1/changesets",
            serde_json::to_vec(&cs).unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body:?}");
        head = Some(
            serde_json::from_slice::<CreateChangesetResponse>(&body)
                .unwrap()
                .id,
        );
    }
    let head = head.unwrap();
    let cas = CasRefRequest {
        expected: None,
        new: Some(head),
        ..Default::default()
    };
    let (status, body) = call(
        &app,
        Method::POST,
        "/v1/refs/main",
        serde_json::to_vec(&cas).unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body:?}");

    let idle = Duration::from_millis(700);
    let addr = start(app, opts(Duration::from_secs(30), idle)).await;
    let path = format!("/v1/log/{head}?limit=1000");
    let mut holder = slow_reader(addr).await;
    assert_eq!(get_status(&mut holder, &path, Some(TOKEN)).await, 200);
    let mut s = TcpStream::connect(addr).await.unwrap();
    assert_eq!(
        get_status(&mut s, "/v1/refs", Some(TOKEN)).await,
        429,
        "the unread log holds the token's slot"
    );
    let took = until_served(addr, "/v1/refs", Some(TOKEN)).await;
    assert!(took >= idle / 2, "served after {took:?}");
    drop(holder);
}
