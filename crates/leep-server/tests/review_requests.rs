//! Review requests and the recorded creator of a changeset over HTTP
//! (ADR 0086).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{header, Method, Request, StatusCode};
use axum::Router;
use bytes::Bytes;
use http_body_util::BodyExt;
use leep_core::api::*;
use leep_core::memory::{MemoryBlobStore, MemoryMetaStore};
use leep_core::review::Verdict;
use leep_core::{Changeset, ChangesetId, FileChange, FileKind, RepoPath};
use leep_derive::Repo;
use leep_server::auth::{hash_token, Auth};
use leep_server::review::{RequireReview, ReviewPolicy, ReviewService};
use tower::ServiceExt;

const VIEWER: &str = "tok-viewer";

fn token(name: &str) -> String {
    format!("tok-{name}")
}

fn auth() -> Auth {
    let mut tokens: Vec<_> = ["alice", "bob", "carol"]
        .iter()
        .map(|n| serde_json::json!({ "name": n, "role": "write", "hash": hash_token(&token(n)) }))
        .collect();
    tokens
        .push(serde_json::json!({ "name": "viewer", "role": "read", "hash": hash_token(VIEWER) }));
    tokens.push(serde_json::json!({
        "name": "agent", "role": "write", "review": false, "hash": hash_token(&token("agent"))
    }));
    Auth::parse(&serde_json::json!({ "tokens": tokens }).to_string()).unwrap()
}

struct Api {
    app: Router,
    repo: Repo,
}

struct Reply {
    status: StatusCode,
    body: Vec<u8>,
}

impl Reply {
    fn json<T: serde::de::DeserializeOwned>(&self) -> T {
        serde_json::from_slice(&self.body)
            .unwrap_or_else(|e| panic!("{e}: {}", String::from_utf8_lossy(&self.body)))
    }

    fn error(&self) -> String {
        self.json::<ErrorResponse>().error
    }

    #[track_caller]
    fn ok<T: serde::de::DeserializeOwned>(&self) -> T {
        assert_eq!(
            self.status,
            StatusCode::OK,
            "{}",
            String::from_utf8_lossy(&self.body)
        );
        self.json()
    }
}

impl Api {
    /// With authentication, a review policy on `main`; without, nothing.
    fn new(with_auth: bool) -> Api {
        let mut repo = Repo::new(
            Arc::new(MemoryBlobStore::new()),
            Arc::new(MemoryMetaStore::new()),
        );
        let mut service = None;
        if with_auth {
            let policy = ReviewPolicy::parse(r#"{ "refs": ["main"] }"#).unwrap();
            let svc = Arc::new(ReviewService::new(auth().reviewers(), policy.rules));
            repo = repo.with_land_policy(Arc::new(RequireReview::new(svc.clone(), policy.refs)));
            service = Some(svc);
        }
        Api {
            app: leep_server::router_with_review(
                repo.clone(),
                1 << 20,
                with_auth.then(auth),
                None,
                service,
            ),
            repo,
        }
    }

    async fn call(&self, method: Method, uri: &str, tok: Option<&str>, body: Vec<u8>) -> Reply {
        let mut b = Request::builder()
            .method(method)
            .uri(uri)
            .header(header::HOST, "localhost")
            .header("content-type", "application/json");
        if let Some(t) = tok {
            b = b.header(header::AUTHORIZATION, format!("Bearer {t}"));
        }
        let resp = self
            .app
            .clone()
            .oneshot(b.body(Body::from(body)).unwrap())
            .await
            .unwrap();
        let (parts, body) = resp.into_parts();
        Reply {
            status: parts.status,
            body: body.collect().await.unwrap().to_bytes().to_vec(),
        }
    }

    async fn post<T: serde::Serialize>(&self, uri: &str, who: Option<&str>, body: &T) -> Reply {
        let tok = who.map(token);
        self.call(
            Method::POST,
            uri,
            tok.as_deref(),
            serde_json::to_vec(body).unwrap(),
        )
        .await
    }

    async fn get(&self, uri: &str) -> Reply {
        self.call(Method::GET, uri, Some(VIEWER), vec![]).await
    }

    /// Create a changeset through the API as `who` (no token for `None`).
    async fn commit(
        &self,
        who: Option<&str>,
        parents: &[ChangesetId],
        files: &[(&str, &str)],
        message: &str,
    ) -> ChangesetId {
        let mut changes = BTreeMap::new();
        for (path, text) in files {
            let content = self
                .repo
                .blobs()
                .put(Bytes::copy_from_slice(text.as_bytes()))
                .await
                .unwrap();
            changes.insert(
                RepoPath::new(*path).unwrap(),
                FileChange::Modify {
                    content,
                    kind: FileKind::Regular,
                    size: text.len() as u64,
                },
            );
        }
        let cs = Changeset {
            parents: parents.to_vec(),
            author: "Claude <noreply@anthropic.com>".into(),
            timestamp: 1,
            message: message.into(),
            changes,
            extra: BTreeMap::new(),
        };
        let r: CreateChangesetResponse = self.post("/v1/changesets", who, &cs).await.ok();
        r.id
    }

    async fn seed_main(&self, files: &[(&str, &str)]) -> ChangesetId {
        let head = self.commit(Some("alice"), &[], files, "seed").await;
        assert!(self
            .repo
            .meta()
            .cas_ref("main", None, Some(head))
            .await
            .unwrap());
        head
    }

    async fn request(&self, who: &str, c: ChangesetId, reviewers: &[&str], msg: &str) -> Reply {
        self.post(
            "/v1/review-requests",
            Some(who),
            &CreateReviewRequest {
                changeset: c,
                target_ref: "main".into(),
                reviewers: reviewers.iter().map(|s| s.to_string()).collect(),
                message: msg.into(),
            },
        )
        .await
    }

    async fn list(&self, query: &str) -> ReviewRequestsResponse {
        self.get(&format!("/v1/review-requests?{query}")).await.ok()
    }

    async fn ids(&self, query: &str) -> Vec<u64> {
        self.list(query)
            .await
            .requests
            .iter()
            .map(|e| e.request.id)
            .collect()
    }
}

#[tokio::test]
async fn the_creating_token_is_recorded_and_shown() {
    let api = Api::new(true);
    let c = api.commit(Some("alice"), &[], &[("a", "1\n")], "one").await;
    let info: ChangesetInfo = api.get(&format!("/v1/changesets/{c}")).await.ok();
    let by = info.created_by.expect("recorded");
    assert_eq!(by.principal, "alice");
    assert!(by.timestamp > 1_700_000_000);
    // Posting the same changeset again, as anyone, keeps the first creator.
    let again = api.commit(Some("bob"), &[], &[("a", "1\n")], "one").await;
    assert_eq!(again, c);
    let info: ChangesetInfo = api.get(&format!("/v1/changesets/{c}")).await.ok();
    assert_eq!(info.created_by, Some(by));
    // By name too (ADR 0082).
    api.repo
        .meta()
        .cas_ref("main", None, Some(c))
        .await
        .unwrap();
    let raw = api.get("/v1/changesets/main").await;
    assert_eq!(
        raw.json::<serde_json::Value>()["created_by"]["principal"],
        "alice"
    );
}

#[tokio::test]
async fn without_authentication_nothing_is_recorded_and_requests_refuse() {
    let api = Api::new(false);
    let c = api.commit(None, &[], &[("a", "1\n")], "anon").await;
    let raw = api
        .call(Method::GET, &format!("/v1/changesets/{c}"), None, vec![])
        .await;
    let v: serde_json::Value = raw.json();
    assert!(v.get("created_by").is_none(), "{v}");
    // The old shape: a `ChangesetInfo` without the field decodes.
    let info: ChangesetInfo = serde_json::from_value(v).unwrap();
    assert_eq!(info.created_by, None);
    let r = api
        .post(
            "/v1/review-requests",
            None,
            &CreateReviewRequest {
                changeset: c,
                target_ref: "main".into(),
                reviewers: vec![],
                message: String::new(),
            },
        )
        .await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);
    assert!(r.error().contains("authentication"), "{}", r.error());
}

#[tokio::test]
async fn requests_go_to_pending_owners_and_names_are_checked() {
    let api = Api::new(true);
    let head = api
        .seed_main(&[
            ("OWNERS", "bob\ncarol\nagent\nghost\n"),
            ("src/a.rs", "1\n"),
        ])
        .await;
    let c = api
        .commit(
            Some("alice"),
            &[head],
            &[("src/a.rs", "2\n")],
            "fix a\n\nbody",
        )
        .await;

    // Default: the owners of the pending paths who may review.
    let r: CreateReviewRequestResponse = api.request("alice", c, &[], "please").await.ok();
    assert!(!r.existing);
    let req = &r.request.request;
    assert_eq!(
        (
            req.requester.as_str(),
            req.reviewers.clone(),
            req.message.as_str()
        ),
        (
            "alice",
            vec!["bob".to_string(), "carol".to_string()],
            "please"
        )
    );
    assert_eq!(req.state, ReviewRequestState::Open);
    assert_eq!(r.request.title.as_deref(), Some("fix a"));
    assert_eq!(r.request.created_by.as_ref().unwrap().principal, "alice");
    // The same request again is the same request.
    let again: CreateReviewRequestResponse = api.request("alice", c, &[], "please").await.ok();
    assert!(again.existing);
    assert_eq!(again.request.request.id, req.id);

    // Names must be reviewers, and not the requester.
    for (name, why) in [
        ("zed", "not a reviewer"),
        ("viewer", "not a reviewer"),
        ("agent", "not a reviewer"),
        ("alice", "its own request"),
    ] {
        let r = api.request("alice", c, &[name], "").await;
        assert_eq!(r.status, StatusCode::BAD_REQUEST, "{name}");
        assert!(r.error().contains(why), "{name}: {}", r.error());
    }
    // A read token may list but not ask.
    let r = api
        .call(
            Method::POST,
            "/v1/review-requests",
            Some(VIEWER),
            serde_json::to_vec(&CreateReviewRequest {
                changeset: c,
                target_ref: "main".into(),
                reviewers: vec![],
                message: String::new(),
            })
            .unwrap(),
        )
        .await;
    assert_eq!(r.status, StatusCode::FORBIDDEN);
    // Unknown changeset, bad ref.
    let ghost = ChangesetId(leep_core::Hash::of_blob(b"ghost"));
    assert_eq!(
        api.request("alice", ghost, &[], "").await.status,
        StatusCode::NOT_FOUND
    );
    let r = api
        .post(
            "/v1/review-requests",
            Some("alice"),
            &serde_json::json!({ "changeset": c, "target_ref": "a..b" }),
        )
        .await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);
    // Already on the ref.
    assert_eq!(
        api.request("alice", head, &["bob"], "").await.status,
        StatusCode::BAD_REQUEST
    );

    // The inbox of each owner, and of nobody else.
    assert_eq!(api.ids("reviewer=bob&open=true").await, [req.id]);
    assert_eq!(api.ids("reviewer=carol").await, [req.id]);
    assert!(api.ids("reviewer=alice").await.is_empty());
    assert_eq!(api.ids("requester=alice").await, [req.id]);
}

#[tokio::test]
async fn listing_filters_pages_and_withdraws() {
    let api = Api::new(true);
    let head = api.seed_main(&[("a", "1\n")]).await;
    let mut ids = Vec::new();
    for (i, (who, to)) in [
        ("alice", vec!["bob"]),
        ("alice", vec![]),
        ("bob", vec!["carol"]),
    ]
    .into_iter()
    .enumerate()
    {
        let c = api
            .commit(
                Some(who),
                &[head],
                &[("a", &format!("{i}\n"))],
                &format!("c{i}"),
            )
            .await;
        let r: CreateReviewRequestResponse = api.request(who, c, &to, "").await.ok();
        ids.push(r.request.request.id);
    }
    // No owners anywhere: the default is any reviewer.
    let any: ReviewRequestEntry = api
        .get(&format!("/v1/review-requests/{}", ids[1]))
        .await
        .ok();
    assert!(any.request.reviewers.is_empty());
    assert_eq!(api.ids("reviewer=bob").await, [ids[1], ids[0]]);
    assert_eq!(api.ids("reviewer=carol").await, [ids[2], ids[1]]);
    assert!(api.ids("reviewer=alice").await.is_empty(), "named nowhere");
    assert_eq!(api.ids("requester=alice").await, [ids[1], ids[0]]);
    assert_eq!(api.ids("ref=main").await, [ids[2], ids[1], ids[0]]);
    assert!(api.ids("ref=dev").await.is_empty());

    // Pages, newest first.
    let page = api.list("limit=2").await;
    assert_eq!(page.requests.len(), 2);
    let next = page.next.expect("a next page");
    assert_eq!(api.ids(&format!("limit=2&before={next}")).await, [ids[0]]);

    // Only the requester withdraws.
    let uri = format!("/v1/review-requests/{}/withdraw", ids[0]);
    let r = api.post(&uri, Some("bob"), &()).await;
    assert_eq!(r.status, StatusCode::FORBIDDEN);
    assert!(r.error().contains("only alice"), "{}", r.error());
    let w: ReviewRequestEntry = api.post(&uri, Some("alice"), &()).await.ok();
    assert_eq!(w.request.state, ReviewRequestState::Withdrawn);
    assert!(w.request.closed.is_some());
    assert_eq!(api.ids("reviewer=bob&open=true").await, [ids[1]]);
    assert_eq!(api.ids("open=false").await, [ids[0]]);
    assert_eq!(api.ids("state=withdrawn").await, [ids[0]]);

    // Bad queries.
    for q in [
        "state=closed",
        "state=open&open=true",
        "limit=0",
        "changeset=xyz",
        "reviewer=",
    ] {
        let r = api.get(&format!("/v1/review-requests?{q}")).await;
        assert_eq!(r.status, StatusCode::BAD_REQUEST, "{q}");
    }
    assert_eq!(
        api.get("/v1/review-requests/999").await.status,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        api.get("/v1/review-requests/x").await.status,
        StatusCode::BAD_REQUEST
    );
}

/// A land closes the request of what it landed, even through a rebase,
/// and never fails or waits for it.
#[tokio::test]
async fn landing_closes_the_request() {
    let api = Api::new(true);
    let head = api
        .seed_main(&[("OWNERS", "bob\n"), ("a", "1\n"), ("b", "1\n")])
        .await;
    let c = api
        .commit(Some("alice"), &[head], &[("a", "2\n")], "change a")
        .await;
    let r: CreateReviewRequestResponse = api.request("alice", c, &[], "").await.ok();
    let id = r.request.request.id;
    assert_eq!(r.request.request.reviewers, ["bob"]);
    // Someone lands first, so this land replays `c`.
    let other = api
        .commit(Some("carol"), &[head], &[("b", "2\n")], "change b")
        .await;
    for x in [other, c] {
        let r = api
            .post(
                &format!("/v1/changesets/{x}/reviews"),
                Some("bob"),
                &AddReviewRequest {
                    verdict: Verdict::Approve,
                    comment: String::new(),
                    reviewer: None,
                    source: None,
                },
            )
            .await;
        assert_eq!(r.status, StatusCode::OK);
    }
    let land = |x: ChangesetId| LandRequest {
        changeset: x,
        target_ref: "main".into(),
        create: None,
        land_id: None,
    };
    let _: LandResponse = api.post("/v1/land", Some("carol"), &land(other)).await.ok();
    let landed = match api.post("/v1/land", Some("alice"), &land(c)).await.ok() {
        LandResponse::Landed {
            landed, rebased, ..
        } => {
            assert!(rebased);
            landed
        }
        other => panic!("{other:?}"),
    };
    // Closing runs beside the land's answer.
    let mut entry = None;
    for _ in 0..200 {
        let e: ReviewRequestEntry = api.get(&format!("/v1/review-requests/{id}")).await.ok();
        if e.request.state != ReviewRequestState::Open {
            entry = Some(e);
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let entry = entry.expect("closed by the land");
    assert_eq!(entry.request.state, ReviewRequestState::Landed);
    assert_eq!(entry.request.landed_as, Some(landed));
    assert!(api.ids("reviewer=bob&open=true").await.is_empty());
    assert_eq!(api.ids("state=landed").await, [id]);
}
