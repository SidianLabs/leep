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

#[test]
fn the_request_log_line_names_the_request_id() {
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
                let req = Request::builder()
                    .uri("/v1/resolve/nope")
                    .header(leep_core::api::REQUEST_ID_HEADER, "bug-1234")
                    .body(Body::empty())
                    .unwrap();
                let resp = leep_server::router(repo).oneshot(req).await.unwrap();
                assert_eq!(resp.status(), StatusCode::NOT_FOUND);
                let _ = resp.into_body().collect().await;
            })
    });
    let text = String::from_utf8(out.0.lock().unwrap().clone()).unwrap();
    let line = text
        .lines()
        .find(|l| l.contains("request completed"))
        .unwrap_or_else(|| panic!("no request line in:\n{text}"));
    assert!(line.contains("request_id=bug-1234"), "{line}");
}
