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

#[tokio::test]
async fn a_protected_tag_cannot_be_deleted() {
    let config = ConfigHandle::load(ReloadSources {
        protect_refs: vec!["tags/v1".into()],
        ..Default::default()
    })
    .unwrap();
    let repo = Repo::new(
        Arc::new(MemoryBlobStore::new()),
        Arc::new(MemoryMetaStore::new()),
    );
    let api = Api(leep_server::router_with_config(
        repo,
        1 << 20,
        Duration::from_secs(60),
        config,
    ));
    let a = api.commit(&[], "f", "1\n").await;
    let b = api.commit(&[a], "f", "2\n").await;
    // Creating it is allowed; tags never move, protected or not.
    assert_eq!(
        api.cas("tags/v1", None, Some(a)).await.status,
        StatusCode::OK
    );
    let r = api.cas("tags/v1", Some(a), Some(b)).await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST, "{}", r.body);
    // Unlike an unprotected tag, it cannot be deleted and recreated elsewhere.
    let r = api.cas("tags/v1", Some(a), None).await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST, "{}", r.body);
    assert!(error(&r).contains("cannot delete"), "{}", r.body);
    assert_eq!(api.head("tags/v1").await, Some(a));
    // Other tags keep the old rule.
    assert_eq!(
        api.cas("tags/v2", None, Some(a)).await.status,
        StatusCode::OK
    );
    assert_eq!(
        api.cas("tags/v2", Some(a), None).await.status,
        StatusCode::OK
    );
}

/// `--protect-ref main` beside `--require-review main`: protection adds no
/// way around review.
#[tokio::test]
async fn protecting_a_reviewed_ref_still_requires_review() {
    let dir = tempfile::tempdir().unwrap();
    let tokens = dir.path().join("tokens.json");
    std::fs::write(
        &tokens,
        serde_json::json!({ "tokens": [
            { "name": "alice", "role": "write", "hash": leep_server::auth::hash_token("tok-alice") },
            { "name": "bob", "role": "write", "hash": leep_server::auth::hash_token("tok-bob") },
        ]})
        .to_string(),
    )
    .unwrap();
    let config = ConfigHandle::load(ReloadSources {
        tokens_file: Some(tokens),
        require_review: vec!["main".into()],
        protect_refs: vec!["main".into()],
        ..Default::default()
    })
    .unwrap();
    let repo = Repo::new(
        Arc::new(MemoryBlobStore::new()),
        Arc::new(MemoryMetaStore::new()),
    );
    let app = leep_server::router_with_config(repo, 1 << 20, Duration::from_secs(60), config);
    let call = |method: Method, uri: String, tok: &'static str, body: Vec<u8>| {
        let app = app.clone();
        async move {
            let req = Request::builder()
                .method(method)
                .uri(uri)
                .header("authorization", format!("Bearer {tok}"))
                .body(Body::from(body))
                .unwrap();
            let resp = app.oneshot(req).await.unwrap();
            let status = resp.status();
            let bytes = resp.into_body().collect().await.unwrap().to_bytes();
            (
                status,
                serde_json::from_slice::<serde_json::Value>(&bytes).unwrap_or_default(),
            )
        }
    };
    let text = "1\n";
    let h = Hash::of_blob(text.as_bytes());
    let (s, _) = call(
        Method::PUT,
        format!("/v1/blobs/{h}"),
        "tok-bob",
        text.into(),
    )
    .await;
    assert_eq!(s, StatusCode::NO_CONTENT);
    let cs = Changeset {
        parents: vec![],
        author: "t".into(),
        timestamp: 1,
        message: "m".into(),
        changes: BTreeMap::from([(
            RepoPath::new("f").unwrap(),
            FileChange::Modify {
                content: h,
                kind: FileKind::Regular,
                size: 2,
            },
        )]),
        extra: BTreeMap::new(),
    };
    let (s, v) = call(
        Method::POST,
        "/v1/changesets".into(),
        "tok-bob",
        serde_json::to_vec(&cs).unwrap(),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{v}");
    let id = serde_json::from_value::<CreateChangesetResponse>(v)
        .unwrap()
        .id;
    let land = serde_json::to_vec(&LandRequest {
        changeset: id,
        target_ref: "main".into(),
        create: None,
        land_id: None,
    })
    .unwrap();
    let cas = serde_json::to_vec(&CasRefRequest {
        expected: None,
        new: Some(id),
        ..Default::default()
    })
    .unwrap();
    for (uri, body) in [("/v1/land", land.clone()), ("/v1/refs/main", cas)] {
        let (s, v) = call(Method::POST, uri.into(), "tok-bob", body).await;
        assert_eq!(s, StatusCode::BAD_REQUEST, "{uri}: {v}");
        assert!(
            v["error"]
                .as_str()
                .unwrap()
                .contains(leep_core::review::NEEDS_REVIEW_MARKER),
            "{uri}: {v}"
        );
    }
    let approve = serde_json::json!({ "verdict": "approve" }).to_string();
    let (s, v) = call(
        Method::POST,
        format!("/v1/changesets/{id}/reviews"),
        "tok-alice",
        approve.into(),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{v}");
    let (s, v) = call(Method::POST, "/v1/land".into(), "tok-bob", land).await;
    assert_eq!(s, StatusCode::OK, "{v}");
    assert_eq!(v["status"], "landed", "{v}");
}
