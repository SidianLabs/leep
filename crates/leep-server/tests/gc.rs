//! How garbage collection's condemnation shows through the HTTP API, and the
//! request timeout GC's fence relies on (ADR 0012).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use axum::Router;
use bytes::Bytes;
use http_body_util::BodyExt;
use leep_core::api::*;
use leep_core::memory::{MemoryBlobStore, MemoryMetaStore};
use leep_core::{BlobMeta, BlobStore, Changeset, FileChange, FileKind, Hash, Result};
use leep_derive::gc::{self, Clock, GcOptions};
use leep_derive::Repo;
use tower::ServiceExt;

/// A clock a minute ahead, so blobs written just now are older than the
/// start of the run.
struct Ahead;

#[async_trait]
impl Clock for Ahead {
    fn now(&self) -> i64 {
        gc::SystemClock.now() + 60
    }
    async fn sleep(&self, _: Duration) {}
}

struct Stack {
    repo: Repo,
    blobs: Arc<MemoryBlobStore>,
    app: Router,
}

impl Stack {
    fn new() -> Self {
        let blobs = Arc::new(MemoryBlobStore::new());
        let repo = Repo::new(blobs.clone(), Arc::new(MemoryMetaStore::new()));
        let app = leep_server::router(repo.clone());
        Stack { repo, blobs, app }
    }

    async fn send(&self, method: Method, uri: &str, body: Vec<u8>) -> (StatusCode, Vec<u8>) {
        let req = Request::builder()
            .method(method)
            .uri(uri)
            .body(Body::from(body))
            .unwrap();
        let resp = self.app.clone().oneshot(req).await.unwrap();
        let (parts, body) = resp.into_parts();
        (
            parts.status,
            body.collect().await.unwrap().to_bytes().to_vec(),
        )
    }

    async fn put(&self, data: &[u8]) -> Hash {
        let h = Hash::of_blob(data);
        let (s, _) = self
            .send(Method::PUT, &format!("/v1/blobs/{h}"), data.to_vec())
            .await;
        assert_eq!(s, StatusCode::NO_CONTENT);
        h
    }

    async fn missing(&self, hashes: &[Hash]) -> Vec<Hash> {
        let body = serde_json::to_vec(&MissingBlobsRequest {
            hashes: hashes.to_vec(),
        })
        .unwrap();
        let (s, body) = self.send(Method::POST, "/v1/blobs/missing", body).await;
        assert_eq!(s, StatusCode::OK);
        serde_json::from_slice::<MissingBlobsResponse>(&body)
            .unwrap()
            .missing
    }

    async fn commit(&self, content: Hash, size: u64, path: &str) -> (StatusCode, String) {
        let cs = Changeset {
            parents: vec![],
            author: "t".into(),
            timestamp: 1,
            message: "m".into(),
            changes: BTreeMap::from([(
                leep_core::RepoPath::new(path).unwrap(),
                FileChange::Modify {
                    content,
                    kind: FileKind::Regular,
                    size,
                },
            )]),
            extra: BTreeMap::new(),
        };
        let (s, body) = self
            .send(
                Method::POST,
                "/v1/changesets",
                serde_json::to_vec(&cs).unwrap(),
            )
            .await;
        (s, String::from_utf8_lossy(&body).into_owned())
    }

    /// Condemn every unreferenced blob (grace 0, so nothing needs aging).
    async fn condemn(&self) -> u64 {
        let opts = GcOptions {
            grace: Duration::ZERO,
            ..GcOptions::default()
        };
        let r = gc::condemn(&self.repo, &opts, &Ahead).await.unwrap();
        r.condemned
    }
}

#[tokio::test]
async fn condemned_blob_is_reported_missing_and_rejected_until_uploaded_again() {
    let s = Stack::new();
    let h = s.put(b"orphan bytes").await;
    assert!(s.missing(&[h]).await.is_empty());
    assert_eq!(s.condemn().await, 1);

    // Dedup no longer trusts it, and a commit that skipped the upload fails
    // the same way a missing blob does.
    assert_eq!(s.missing(&[h]).await, [h]);
    let (status, body) = s.commit(h, 12, "a.txt").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body.contains("missing blob"), "{body}");

    // The retry the client performs: upload, then commit.
    assert_eq!(s.put(b"orphan bytes").await, h);
    assert!(s.missing(&[h]).await.is_empty());
    let (status, body) = s.commit(h, 12, "a.txt").await;
    assert_eq!(status, StatusCode::OK, "{body}");
}

#[tokio::test]
async fn referenced_blobs_are_never_condemned() {
    let s = Stack::new();
    let h = s.put(b"live").await;
    let (status, _) = s.commit(h, 4, "live.txt").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(s.condemn().await, 0);
    assert!(s.missing(&[h]).await.is_empty());
    assert!(s.blobs.contains(&h).await.unwrap());
}

/// A blob store whose writes take a while.
struct SlowBlobs(MemoryBlobStore, Duration);

#[async_trait]
impl BlobStore for SlowBlobs {
    async fn put_with_hash(&self, hash: Hash, data: Bytes) -> Result<()> {
        tokio::time::sleep(self.1).await;
        self.0.put_with_hash(hash, data).await
    }
    async fn get(&self, hash: &Hash) -> Result<Option<Bytes>> {
        self.0.get(hash).await
    }
    async fn list_page(&self, after: Option<Hash>, limit: usize) -> Result<Vec<BlobMeta>> {
        self.0.list_page(after, limit).await
    }
    async fn delete(&self, hash: &Hash) -> Result<()> {
        self.0.delete(hash).await
    }
}

#[tokio::test]
async fn requests_over_the_timeout_are_aborted() {
    let repo = Repo::new(
        Arc::new(SlowBlobs(
            MemoryBlobStore::new(),
            Duration::from_millis(400),
        )),
        Arc::new(MemoryMetaStore::new()),
    );
    let app = leep_server::router_with_timeout(
        repo.clone(),
        1 << 20,
        None,
        None,
        None,
        Duration::from_millis(50),
    );
    let h = Hash::of_blob(b"slow");
    let put = |app: Router| async move {
        let req = Request::builder()
            .method(Method::PUT)
            .uri(format!("/v1/blobs/{h}"))
            .body(Body::from("slow"))
            .unwrap();
        app.oneshot(req).await.unwrap().status()
    };
    assert_eq!(put(app).await, StatusCode::REQUEST_TIMEOUT);

    // Zero disables the bound.
    let app = leep_server::router_with_timeout(repo, 1 << 20, None, None, None, Duration::ZERO);
    assert_eq!(put(app).await, StatusCode::NO_CONTENT);

    // The default router has the bound too.
    assert_eq!(
        leep_server::DEFAULT_REQUEST_TIMEOUT,
        Duration::from_secs(300)
    );
}
