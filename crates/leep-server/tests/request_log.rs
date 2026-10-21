//! The per-request log line. Alone in its test binary: `tracing` caches
//! callsite interest per process, so a test running beside others without a
//! subscriber could see the line disabled.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use leep_core::memory::{MemoryBlobStore, MemoryMetaStore};
use leep_derive::Repo;
use tower::ServiceExt;

/// Collects what a `tracing` subscriber writes.
#[derive(Clone, Default)]
struct Captured(Arc<std::sync::Mutex<Vec<u8>>>);

impl std::io::Write for Captured {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// The text log lines of one request per id in `ids`, in order.
fn request_lines(ids: &[&str]) -> Vec<String> {
    let out = Captured::default();
    let writer = out.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_ansi(false)
        .with_writer(move || writer.clone())
        .finish();
    tracing::subscriber::with_default(subscriber, || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let repo = Repo::new(
                    Arc::new(MemoryBlobStore::new()),
                    Arc::new(MemoryMetaStore::new()),
                );
                let app = leep_server::router(repo);
                for id in ids {
                    let req = Request::builder()
                        .uri("/v1/resolve/nope")
                        .header(leep_core::api::REQUEST_ID_HEADER, *id)
                        .body(Body::empty())
                        .unwrap();
                    let resp = app.clone().oneshot(req).await.unwrap();
                    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
                    let _ = resp.into_body().collect().await;
                }
            })
    });
    let text = String::from_utf8(out.0.lock().unwrap().clone()).unwrap();
    let lines: Vec<String> = text
        .lines()
        .filter(|l| l.contains("request completed"))
        .map(str::to_string)
        .collect();
    assert_eq!(lines.len(), ids.len(), "{text}");
    lines
}

#[test]
fn the_request_log_line_names_the_request_id_and_cannot_be_spoofed() {
    let spoof = "x status=200 route=/v1/land";
    let lines = request_lines(&["bug-1234", spoof]);
    // Quoted, so the line shows where the id ends.
    assert!(lines[0].contains("request_id=\"bug-1234\""), "{}", lines[0]);
    // An id that could pose as other fields is replaced, not logged.
    assert!(!lines[1].contains("status=200"), "{}", lines[1]);
    assert!(!lines[1].contains("/v1/land"), "{}", lines[1]);
    let (_, rest) = lines[1].split_once("request_id=\"").expect("an id");
    let made = &rest[..32];
    assert!(made.bytes().all(|b| b.is_ascii_hexdigit()), "{}", lines[1]);
    assert_eq!(rest.as_bytes()[32], b'"', "{}", lines[1]);
}
