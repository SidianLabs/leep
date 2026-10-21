//! `--protect-ref`: a ref that only moves forward and cannot be deleted,
//! without a signature or review requirement (ADR 0015).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use axum::Router;
use http_body_util::BodyExt;
use leep_core::api::*;
use leep_core::memory::{MemoryBlobStore, MemoryMetaStore};
use leep_core::refname::NOT_FORWARD_MARKER;
use leep_core::{Changeset, ChangesetId, FileChange, FileKind, Hash, RepoPath};
use leep_derive::Repo;
use leep_server::reload::{ConfigHandle, ReloadSources};
use tower::ServiceExt;

struct Api(Router);

struct Reply {
    status: StatusCode,
    body: serde_json::Value,
}

impl Api {
    /// An open server (no tokens) started with `--protect-ref main`.
    fn new() -> Api {
        let config = ConfigHandle::load(ReloadSources {
            protect_refs: vec!["main".into()],
            ..Default::default()
        })
        .unwrap();
        let repo = Repo::new(
            Arc::new(MemoryBlobStore::new()),
            Arc::new(MemoryMetaStore::new()),
        );
        Api(leep_server::router_with_config(
            repo,
            1 << 20,
            Duration::from_secs(60),
            config,
        ))
    }

    async fn call(&self, method: Method, uri: &str, body: Vec<u8>) -> Reply {
        let req = Request::builder()
            .method(method)
            .uri(uri)
            .body(Body::from(body))
            .unwrap();
        let resp = self.0.clone().oneshot(req).await.unwrap();
        let (parts, body) = resp.into_parts();
        let bytes = body.collect().await.unwrap().to_bytes();
        Reply {
            status: parts.status,
            body: serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null),
        }
    }

    async fn post<T: serde::Serialize>(&self, uri: &str, body: &T) -> Reply {
        self.call(Method::POST, uri, serde_json::to_vec(body).unwrap())
            .await
    }

    async fn commit(&self, parents: &[ChangesetId], path: &str, text: &str) -> ChangesetId {
        let h = Hash::of_blob(text.as_bytes());
        let r = self
            .call(
                Method::PUT,
                &format!("/v1/blobs/{h}"),
                text.as_bytes().to_vec(),
            )
            .await;
        assert_eq!(r.status, StatusCode::NO_CONTENT);
        let cs = Changeset {
            parents: parents.to_vec(),
            author: "t".into(),
            timestamp: 1,
            message: text.into(),
            changes: BTreeMap::from([(
                RepoPath::new(path).unwrap(),
                FileChange::Modify {
                    content: h,
                    kind: FileKind::Regular,
                    size: text.len() as u64,
                },
            )]),
            extra: BTreeMap::new(),
        };
        let r = self.post("/v1/changesets", &cs).await;
        assert_eq!(r.status, StatusCode::OK, "{}", r.body);
        serde_json::from_value::<CreateChangesetResponse>(r.body)
            .unwrap()
            .id
    }

    async fn cas(
        &self,
        name: &str,
        expected: Option<ChangesetId>,
        new: Option<ChangesetId>,
    ) -> Reply {
        self.post(
            &format!("/v1/refs/{name}"),
            &CasRefRequest {
                expected,
                new,
                ..Default::default()
            },
        )
        .await
    }

    async fn head(&self, name: &str) -> Option<ChangesetId> {
        let r = self
            .call(Method::GET, &format!("/v1/refs/{name}"), vec![])
            .await;
        serde_json::from_value::<RefResponse>(r.body)
            .unwrap()
            .target
    }

    async fn land(&self, cs: ChangesetId, target: &str) -> Reply {
        self.post(
            "/v1/land",
            &LandRequest {
                changeset: cs,
                target_ref: target.into(),
                create: None,
                land_id: None,
            },
        )
        .await
    }
}

fn error(r: &Reply) -> &str {
    r.body["error"].as_str().unwrap_or_default()
}

#[tokio::test]
async fn a_protected_ref_only_moves_forward_and_cannot_be_deleted() {
    let api = Api::new();
    let a = api.commit(&[], "f", "1\n").await;
    let b = api.commit(&[a], "f", "2\n").await;
    let side = api.commit(&[a], "g", "side\n").await;

    // Creating it and fast-forwarding it through the raw endpoint work.
    assert_eq!(api.cas("main", None, Some(a)).await.status, StatusCode::OK);
    assert_eq!(
        api.cas("main", Some(a), Some(b)).await.status,
        StatusCode::OK
    );

    // A rewind and a sideways move are refused.
    for to in [a, side] {
        let r = api.cas("main", Some(b), Some(to)).await;
        assert_eq!(r.status, StatusCode::BAD_REQUEST, "{}", r.body);
        assert!(error(&r).contains(NOT_FORWARD_MARKER), "{}", r.body);
    }
    // So is a delete.
    let r = api.cas("main", Some(b), None).await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST, "{}", r.body);
    assert!(error(&r).contains("cannot delete"), "{}", r.body);
    assert_eq!(api.head("main").await, Some(b));
}

#[tokio::test]
async fn land_still_moves_a_protected_ref() {
    let api = Api::new();
    let a = api.commit(&[], "f", "1\n").await;
    assert_eq!(api.land(a, "main").await.body["status"], "landed");
    let b = api.commit(&[a], "f", "2\n").await;
    let r = api.land(b, "main").await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.body);
    assert_eq!(r.body["status"], "landed", "{}", r.body);
    assert_eq!(api.head("main").await, Some(b));
    // A change made on an older head is rebased, which is still forward.
    let late = api.commit(&[a], "g", "late\n").await;
    let r = api.land(late, "main").await;
    assert_eq!(r.body["status"], "landed", "{}", r.body);
    assert_eq!(r.body["rebased"], true, "{}", r.body);
}

#[tokio::test]
async fn unprotected_refs_are_unaffected() {
    let api = Api::new();
    let a = api.commit(&[], "f", "1\n").await;
    let b = api.commit(&[a], "f", "2\n").await;
    assert_eq!(api.cas("dev", None, Some(b)).await.status, StatusCode::OK);
    assert_eq!(
        api.cas("dev", Some(b), Some(a)).await.status,
        StatusCode::OK
    );
    assert_eq!(api.cas("dev", Some(a), None).await.status, StatusCode::OK);
    assert_eq!(api.head("dev").await, None);
    // `main` is protected by exact name only.
    assert_eq!(api.cas("main2", None, Some(b)).await.status, StatusCode::OK);
    assert_eq!(api.cas("main2", Some(b), None).await.status, StatusCode::OK);
}
