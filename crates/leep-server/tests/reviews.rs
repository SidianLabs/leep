//! The review service over HTTP: verdicts, owners, semantic auto-approval and
//! the `--require-review` land policy (ADR 0011).

use std::collections::BTreeMap;
use std::sync::Arc;

use axum::body::Body;
use axum::http::{header, Method, Request, StatusCode};
use axum::Router;
use bytes::Bytes;
use ed25519_dalek::{Signer, SigningKey};
use http_body_util::BodyExt;
use leep_core::api::*;
use leep_core::changeset::EXTRA_REBASED_FROM;
use leep_core::memory::{MemoryBlobStore, MemoryMetaStore};
use leep_core::review::{Verdict, NEEDS_REVIEW_MARKER};
use leep_core::signature::{format_public_key, signing_message, NOT_SIGNED_MARKER};
use leep_core::{Changeset, ChangesetId, FileChange, FileKind, Hash, RepoPath};
use leep_derive::Repo;
use leep_server::auth::{hash_token, Auth};
use leep_server::review::{RequireReview, ReviewPolicy, ReviewService};
use leep_server::signing::{RequireSignatures, SigningKeys};
use tower::ServiceExt;

const NAMES: [&str; 4] = ["alice", "bob", "carol", "mallory"];
const VIEWER: &str = "tok-viewer";

fn token(name: &str) -> String {
    format!("tok-{name}")
}

const POLICY: &str = r#"{ "refs": ["main"] }"#;
const POLICY_AUTO: &str = r#"{ "refs": ["main"],
    "auto_approve": ["comment_only", "whitespace_only", "docs_only"],
    "docs_globs": ["**/*.md", "docs/**"] }"#;

fn signer() -> SigningKey {
    SigningKey::from_bytes(&[1; 32])
}

struct Opts {
    auth: bool,
    policy: Option<&'static str>,
    signatures: bool,
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
}

fn auth() -> Auth {
    let mut tokens: Vec<_> = NAMES
        .iter()
        .map(|n| serde_json::json!({ "name": n, "role": "write", "hash": hash_token(&token(n)) }))
        .collect();
    tokens
        .push(serde_json::json!({ "name": "viewer", "role": "read", "hash": hash_token(VIEWER) }));
    tokens.push(serde_json::json!({
        "name": "mirror", "role": "write", "review": false, "review_for": true,
        "hash": hash_token(&token("mirror"))
    }));
    Auth::parse(&serde_json::json!({ "tokens": tokens }).to_string()).unwrap()
}

impl Api {
    fn new(o: Opts) -> Api {
        let mut repo = Repo::new(
            Arc::new(MemoryBlobStore::new()),
            Arc::new(MemoryMetaStore::new()),
        );
        let keys = o.signatures.then(|| {
            let text = serde_json::json!({ "keys": [
                { "name": "alice", "public_key": format_public_key(signer().verifying_key().as_bytes()) }
            ]})
            .to_string();
            Arc::new(SigningKeys::parse(&text).unwrap())
        });
        if let Some(k) = keys.clone() {
            repo = repo.with_land_policy(Arc::new(RequireSignatures::new(k, ["main".to_string()])));
        }
        let mut service = None;
        if let (Some(text), true) = (o.policy, o.auth) {
            let policy = ReviewPolicy::parse(text).unwrap();
            let svc = Arc::new(ReviewService::new(auth().reviewers(), policy.rules));
            repo = repo.with_land_policy(Arc::new(RequireReview::new(svc.clone(), policy.refs)));
            service = Some(svc);
        }
        let a = o.auth.then(auth);
        Api {
            app: leep_server::router_with_review(repo.clone(), 1 << 20, a, keys, service),
            repo,
        }
    }

    fn with(policy: &'static str) -> Api {
        Api::new(Opts {
            auth: true,
            policy: Some(policy),
            signatures: false,
        })
    }

    async fn call(&self, method: Method, uri: &str, tok: Option<&str>, body: Vec<u8>) -> Reply {
        let ty = if method == Method::PUT {
            "application/octet-stream"
        } else {
            "application/json"
        };
        let mut b = Request::builder()
            .method(method)
            .uri(uri)
            .header("content-type", ty);
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

    async fn post<T: serde::Serialize>(&self, uri: &str, who: &str, body: &T) -> Reply {
        self.call(
            Method::POST,
            uri,
            Some(&token(who)),
            serde_json::to_vec(body).unwrap(),
        )
        .await
    }

    async fn get(&self, uri: &str) -> Reply {
        self.call(Method::GET, uri, Some(VIEWER), vec![]).await
    }

    /// Store a changeset directly: `Some` writes a file, `None` deletes it.
    async fn commit(&self, parents: &[ChangesetId], files: &[(&str, Option<&str>)]) -> ChangesetId {
        self.commit_from(parents, files, None).await
    }

    /// [`Self::commit`] with `leep.rebased_from` set to `from`, as a client
    /// stamps a commit that resolves a land's conflicts.
    async fn commit_from(
        &self,
        parents: &[ChangesetId],
        files: &[(&str, Option<&str>)],
        from: Option<&str>,
    ) -> ChangesetId {
        let mut changes = BTreeMap::new();
        for (path, text) in files {
            let change = match text {
                Some(t) => FileChange::Modify {
                    content: self
                        .repo
                        .blobs()
                        .put(Bytes::copy_from_slice(t.as_bytes()))
                        .await
                        .unwrap(),
                    kind: FileKind::Regular,
                    size: t.len() as u64,
                },
                None => FileChange::Delete,
            };
            changes.insert(RepoPath::new(*path).unwrap(), change);
        }
        self.repo
            .create_changeset(Changeset {
                parents: parents.to_vec(),
                author: "t".into(),
                timestamp: 1,
                message: "m".into(),
                changes,
                extra: from
                    .map(|f| BTreeMap::from([(EXTRA_REBASED_FROM.to_string(), f.to_string())]))
                    .unwrap_or_default(),
            })
            .await
            .unwrap()
    }

    /// Make `files` the head of `main` without going through any policy.
    async fn seed_main(&self, files: &[(&str, Option<&str>)]) -> ChangesetId {
        let head = self.commit(&[], files).await;
        assert!(self
            .repo
            .meta()
            .cas_ref("main", None, Some(head))
            .await
            .unwrap());
        head
    }

    async fn review(&self, id: ChangesetId, who: &str, v: Verdict) -> Reply {
        self.post(
            &format!("/v1/changesets/{id}/reviews"),
            who,
            &AddReviewRequest {
                verdict: v,
                comment: String::new(),
                reviewer: None,
                source: None,
            },
        )
        .await
    }

    async fn approve(&self, id: ChangesetId, who: &str) {
        let r = self.review(id, who, Verdict::Approve).await;
        assert_eq!(
            r.status,
            StatusCode::OK,
            "{}",
            String::from_utf8_lossy(&r.body)
        );
    }

    async fn reviews(&self, id: ChangesetId) -> ReviewsResponse {
        let r = self.get(&format!("/v1/changesets/{id}/reviews")).await;
        assert_eq!(
            r.status,
            StatusCode::OK,
            "{}",
            String::from_utf8_lossy(&r.body)
        );
        r.json()
    }

    async fn status(&self, id: ChangesetId) -> ReviewStatusResponse {
        let r = self
            .get(&format!("/v1/changesets/{id}/review-status"))
            .await;
        assert_eq!(
            r.status,
            StatusCode::OK,
            "{}",
            String::from_utf8_lossy(&r.body)
        );
        r.json()
    }

    async fn land(&self, id: ChangesetId, who: &str) -> Reply {
        self.post(
            "/v1/land",
            who,
            &LandRequest {
                changeset: id,
                target_ref: "main".into(),
                create: None,
                land_id: None,
            },
        )
        .await
    }

    async fn main_ref(&self) -> Option<ChangesetId> {
        self.repo.meta().get_ref("main").await.unwrap()
    }
}

fn needs_review(id: ChangesetId, path: &str, why: &str) -> String {
    format!("changeset {id} {NEEDS_REVIEW_MARKER}: path {path} {why}")
}

fn rules(s: &ReviewStatusResponse) -> Vec<(&str, &str)> {
    s.paths
        .iter()
        .map(|p| (p.path.as_str(), p.rule.as_str()))
        .collect()
}

#[tokio::test]
async fn approve_reject_flow_latest_verdict_wins() {
    let api = Api::with_policy_none();
    let c = api.commit(&[], &[("a", Some("1\n"))]).await;
    assert!(api.reviews(c).await.reviews.is_empty());

    let r = api
        .post(
            &format!("/v1/changesets/{c}/reviews"),
            "alice",
            &AddReviewRequest {
                verdict: Verdict::Approve,
                comment: "looks right".into(),
                reviewer: None,
                source: None,
            },
        )
        .await;
    assert_eq!(r.status, StatusCode::OK);
    let ok: AddReviewResponse = r.json();
    assert_eq!(
        (ok.reviewer.as_str(), ok.verdict),
        ("alice", Verdict::Approve)
    );
    api.approve(c, "bob").await;

    let list = api.reviews(c).await;
    assert_eq!(list.via, None);
    let names: Vec<_> = list.reviews.iter().map(|r| r.reviewer.as_str()).collect();
    assert_eq!(names, ["alice", "bob"]);
    assert_eq!(list.reviews[0].comment, "looks right");
    assert!(list.reviews[0].timestamp > 1_600_000_000);

    // Idempotent, and a reviewer can change their mind.
    api.approve(c, "bob").await;
    assert_eq!(api.reviews(c).await.reviews.len(), 2);
    let r = api
        .post(
            &format!("/v1/changesets/{c}/reviews"),
            "alice",
            &AddReviewRequest {
                verdict: Verdict::Reject,
                comment: "wait".into(),
                reviewer: None,
                source: None,
            },
        )
        .await;
    assert_eq!(r.status, StatusCode::OK);
    let list = api.reviews(c).await;
    assert_eq!(list.reviews.len(), 2);
    assert_eq!(list.reviews[0].verdict, Verdict::Reject);
    assert_eq!(list.reviews[0].comment, "wait");
    assert_eq!(list.reviews[1].verdict, Verdict::Approve);

    // Another changeset is untouched.
    let d = api.commit(&[c], &[("b", Some("2\n"))]).await;
    assert!(api.reviews(d).await.reviews.is_empty());
}

impl Api {
    /// Auth on, no review policy: reviews can be recorded but nothing lands
    /// differently.
    fn with_policy_none() -> Api {
        Api::new(Opts {
            auth: true,
            policy: None,
            signatures: false,
        })
    }
}

#[tokio::test]
async fn bad_requests_and_unknown_changesets() {
    let api = Api::with_policy_none();
    let c = api.commit(&[], &[("a", Some("1\n"))]).await;
    let missing = ChangesetId(Hash::of_blob(b"nope"));
    assert_eq!(
        api.review(missing, "alice", Verdict::Approve).await.status,
        StatusCode::NOT_FOUND
    );
    for uri in [
        format!("/v1/changesets/{missing}/reviews"),
        format!("/v1/changesets/{missing}/review-status"),
    ] {
        assert_eq!(api.get(&uri).await.status, StatusCode::NOT_FOUND);
    }
    assert_eq!(
        api.get("/v1/changesets/zz/reviews").await.status,
        StatusCode::BAD_REQUEST
    );

    let uri = format!("/v1/changesets/{c}/reviews");
    let alice = token("alice");
    let post = |body: &str| api.call(Method::POST, &uri, Some(&alice), body.as_bytes().to_vec());
    assert_eq!(
        post(r#"{"verdict":"maybe"}"#).await.status,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(post("{}").await.status, StatusCode::BAD_REQUEST);
    assert_eq!(post("{not json").await.status, StatusCode::BAD_REQUEST);
    // The comment is optional.
    assert_eq!(
        post(r#"{"verdict":"approve"}"#).await.status,
        StatusCode::OK
    );
    let big = serde_json::json!({ "verdict": "approve", "comment": "x".repeat(70_000) });
    let r = post(&big.to_string()).await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);
    assert!(r.error().contains("comment"), "{}", r.error());
}

#[tokio::test]
async fn reviews_need_authentication() {
    let api = Api::new(Opts {
        auth: false,
        policy: None,
        signatures: false,
    });
    let c = api.commit(&[], &[("a", Some("1\n"))]).await;
    let uri = format!("/v1/changesets/{c}/reviews");
    let body = serde_json::to_vec(&AddReviewRequest {
        verdict: Verdict::Approve,
        comment: String::new(),
        reviewer: None,
        source: None,
    })
    .unwrap();
    let r = api.call(Method::POST, &uri, None, body).await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);
    assert!(
        r.error().contains("reviews need authentication"),
        "{}",
        r.error()
    );
    for uri in [uri, format!("/v1/changesets/{c}/review-status")] {
        let r = api.call(Method::GET, &uri, None, vec![]).await;
        assert_eq!(r.status, StatusCode::BAD_REQUEST);
        assert!(r.error().contains("reviews need authentication"));
    }
    assert!(api.repo.get_review(c, "anonymous").await.unwrap().is_none());
}

#[tokio::test]
async fn auth_is_enforced_and_read_tokens_cannot_review() {
    let api = Api::with_policy_none();
    let c = api.commit(&[], &[("a", Some("1\n"))]).await;
    let uri = format!("/v1/changesets/{c}/reviews");
    let body = serde_json::to_vec(&AddReviewRequest {
        verdict: Verdict::Approve,
        comment: String::new(),
        reviewer: None,
        source: None,
    })
    .unwrap();

    let r = api.call(Method::POST, &uri, None, body.clone()).await;
    assert_eq!(r.status, StatusCode::UNAUTHORIZED);
    let r = api
        .call(Method::POST, &uri, Some(VIEWER), body.clone())
        .await;
    assert_eq!(r.status, StatusCode::FORBIDDEN);
    assert!(api.reviews(c).await.reviews.is_empty());
    // Reading is allowed with a read token.
    assert_eq!(api.get(&uri).await.status, StatusCode::OK);
    assert_eq!(
        api.get(&format!("/v1/changesets/{c}/review-status"))
            .await
            .status,
        StatusCode::OK
    );
    let r = api
        .call(Method::POST, &uri, Some(&token("alice")), body)
        .await;
    assert_eq!(r.status, StatusCode::OK);
}

#[tokio::test]
async fn the_reviewer_is_the_token_not_the_body() {
    let api = Api::with_policy_none();
    let c = api.commit(&[], &[("a", Some("1\n"))]).await;
    let body = serde_json::json!({ "verdict": "approve", "reviewer": "alice", "comment": "hi" });
    let r = api
        .call(
            Method::POST,
            &format!("/v1/changesets/{c}/reviews"),
            Some(&token("mallory")),
            body.to_string().into_bytes(),
        )
        .await;
    // ADR 0063: naming a reviewer needs `review_for`; it is refused, not
    // taken as mallory's own review.
    assert_eq!(r.status, StatusCode::FORBIDDEN);
    assert!(api.reviews(c).await.reviews.is_empty());
    for who in ["alice", "mallory"] {
        assert!(api.repo.get_review(c, who).await.unwrap().is_none());
    }
}

#[tokio::test]
async fn owners_are_read_from_the_head_not_the_change() {
    let api = Api::with(POLICY);
    let head = api
        .seed_main(&[
            ("a/OWNERS", Some("alice\n")),
            ("a/x.rs", Some("fn x() {}\n")),
        ])
        .await;
    // A change that tries to make mallory the sole owner of a/sub.
    let c = api
        .commit(
            &[head],
            &[
                ("a/sub/OWNERS", Some("mallory\nnoparent\n")),
                ("a/sub/f.rs", Some("fn f() {}\n")),
            ],
        )
        .await;
    api.approve(c, "mallory").await;
    let r = api.land(c, "mallory").await;
    assert_eq!(r.status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(
        r.error(),
        needs_review(c, "a/sub/OWNERS", "needs approval from one of [alice]")
    );
    assert_eq!(api.main_ref().await, Some(head));

    let s = api.status(c).await;
    assert!(!s.approved);
    assert_eq!(s.paths[0].owners, ["alice"]);
    // The file that named them, from the head: not the change's own.
    assert_eq!(s.paths[0].owners_files, ["a/OWNERS"]);
    assert!(s.paths[0].approved_by.is_empty());

    api.approve(c, "alice").await;
    assert_eq!(api.land(c, "mallory").await.status, StatusCode::OK);
    // Now it is in the head, so it takes effect for later changes.
    let d = api
        .commit(&[c], &[("a/sub/f.rs", Some("fn f2() {}\n"))])
        .await;
    api.approve(d, "alice").await;
    let r = api.land(d, "alice").await;
    assert_eq!(
        r.error(),
        needs_review(d, "a/sub/f.rs", "needs approval from one of [mallory]")
    );
    api.approve(d, "mallory").await;
    assert_eq!(api.land(d, "alice").await.status, StatusCode::OK);
}

#[tokio::test]
async fn editing_an_owners_file_needs_the_existing_owners() {
    let api = Api::with(POLICY);
    let head = api
        .seed_main(&[
            ("OWNERS", Some("alice\n")),
            ("a/OWNERS", Some("bob\n")),
            ("a/f.rs", Some("fn f() {}\n")),
        ])
        .await;
    // carol writes herself into a/OWNERS.
    let c = api
        .commit(&[head], &[("a/OWNERS", Some("bob\ncarol\n"))])
        .await;
    api.approve(c, "carol").await;
    let r = api.land(c, "carol").await;
    assert_eq!(
        r.error(),
        needs_review(c, "a/OWNERS", "needs approval from one of [alice, bob]")
    );
    api.approve(c, "bob").await;
    assert_eq!(api.land(c, "carol").await.status, StatusCode::OK);

    // Deleting an OWNERS file needs the same approval, now from carol too.
    let d = api.commit(&[c], &[("a/OWNERS", None)]).await;
    api.approve(d, "mallory").await;
    let r = api.land(d, "mallory").await;
    assert_eq!(
        r.error(),
        needs_review(
            d,
            "a/OWNERS",
            "needs approval from one of [alice, bob, carol]"
        )
    );
    api.approve(d, "carol").await;
    assert_eq!(api.land(d, "mallory").await.status, StatusCode::OK);
}

#[tokio::test]
async fn noparent_stops_inheritance() {
    let api = Api::with(POLICY);
    let head = api
        .seed_main(&[
            ("OWNERS", Some("alice\n")),
            ("x/OWNERS", Some("bob\nnoparent\n")),
            ("x/f.rs", Some("fn f() {}\n")),
            ("y/f.rs", Some("fn f() {}\n")),
        ])
        .await;
    let c = api
        .commit(&[head], &[("x/f.rs", Some("fn g() {}\n"))])
        .await;
    api.approve(c, "alice").await;
    let r = api.land(c, "alice").await;
    assert_eq!(
        r.error(),
        needs_review(c, "x/f.rs", "needs approval from one of [bob]")
    );
    api.approve(c, "bob").await;
    assert_eq!(api.land(c, "alice").await.status, StatusCode::OK);

    // Outside the noparent directory the root owner still applies.
    let d = api.commit(&[c], &[("y/f.rs", Some("fn g() {}\n"))]).await;
    api.approve(d, "bob").await;
    let r = api.land(d, "bob").await;
    assert_eq!(
        r.error(),
        needs_review(d, "y/f.rs", "needs approval from one of [alice]")
    );
}

#[tokio::test]
async fn paths_without_owners_take_any_single_approval() {
    let api = Api::with(POLICY);
    // No head at all: the first changeset needs one approval from anyone.
    let c = api.commit(&[], &[("a", Some("1\n"))]).await;
    let r = api.land(c, "alice").await;
    assert_eq!(
        r.error(),
        needs_review(c, "a", "needs approval from any reviewer")
    );
    api.approve(c, "mallory").await;
    assert_eq!(api.land(c, "alice").await.status, StatusCode::OK);
    // Still no OWNERS file anywhere.
    let d = api.commit(&[c], &[("a", Some("2\n"))]).await;
    assert_eq!(
        api.land(d, "alice").await.status,
        StatusCode::UNPROCESSABLE_ENTITY
    );
    api.approve(d, "carol").await;
    assert_eq!(api.land(d, "alice").await.status, StatusCode::OK);
}

#[tokio::test]
async fn reject_blocks_even_beside_approvals_until_the_reviewer_changes_their_mind() {
    let api = Api::with(POLICY_AUTO);
    let head = api
        .seed_main(&[
            ("OWNERS", Some("alice\nbob\n")),
            ("a.rs", Some("fn a() {}\n")),
        ])
        .await;
    let c = api
        .commit(&[head], &[("a.rs", Some("fn a() { 1 }\n"))])
        .await;
    api.approve(c, "alice").await;
    assert_eq!(
        api.review(c, "bob", Verdict::Reject).await.status,
        StatusCode::OK
    );
    let r = api.land(c, "carol").await;
    assert_eq!(r.error(), needs_review(c, "a.rs", "rejected by bob"));
    assert_eq!(rules(&api.status(c).await), [("a.rs", "rejected")]);
    assert_eq!(api.main_ref().await, Some(head));

    api.approve(c, "bob").await;
    assert_eq!(api.land(c, "carol").await.status, StatusCode::OK);
}

#[tokio::test]
async fn a_reject_from_an_owner_beats_auto_approval() {
    let api = Api::with(POLICY_AUTO);
    let head = api
        .seed_main(&[
            ("OWNERS", Some("alice\n")),
            ("a.rs", Some("fn a() {}\n")),
            ("docs/x.md", Some("x\n")),
        ])
        .await;
    let c = api
        .commit(
            &[head],
            &[
                ("a.rs", Some("// why\nfn a() {}\n")),
                ("docs/x.md", Some("y\n")),
            ],
        )
        .await;
    assert_eq!(
        rules(&api.status(c).await),
        [
            ("a.rs", "auto:comment_only"),
            ("docs/x.md", "auto:docs_only")
        ]
    );
    // Both are auto-approved, so it lands with no review at all.
    assert!(api.status(c).await.approved);
    assert_eq!(
        api.review(c, "alice", Verdict::Reject).await.status,
        StatusCode::OK
    );
    let s = api.status(c).await;
    assert!(!s.approved);
    assert_eq!(s.paths[0].rejected_by, ["alice"]);
    let r = api.land(c, "bob").await;
    assert_eq!(r.error(), needs_review(c, "a.rs", "rejected by alice"));
    assert_eq!(api.main_ref().await, Some(head));
}

#[tokio::test]
async fn comment_only_and_docs_changes_land_without_a_review() {
    let api = Api::with(POLICY_AUTO);
    let head = api
        .seed_main(&[
            ("OWNERS", Some("alice\n")),
            ("src/lib.rs", Some("fn a() {}\nfn b() {}\n")),
            ("src/util.py", Some("def f():\n    return 1\n")),
        ])
        .await;
    let c = api
        .commit(
            &[head],
            &[
                (
                    "src/lib.rs",
                    Some("// adds b\nfn a() {}\nfn b() {} // trailing\n"),
                ),
                (
                    "src/util.py",
                    Some("# helper\ndef f():\n    return 1  # one\n"),
                ),
                ("README.md", Some("# hello\n")),
                ("docs/guide/a.txt", Some("text\n")),
            ],
        )
        .await;
    let s = api.status(c).await;
    assert!(s.approved, "{s:?}");
    assert_eq!(
        rules(&s),
        [
            ("README.md", "auto:docs_only"),
            ("docs/guide/a.txt", "auto:docs_only"),
            ("src/lib.rs", "auto:comment_only"),
            ("src/util.py", "auto:comment_only"),
        ]
    );
    let r = api.land(c, "mallory").await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.error());

    // A change that also touches code is held back, naming that path.
    let d = api
        .commit(
            &[c],
            &[
                (
                    "src/lib.rs",
                    Some("// adds b\nfn a() {}\nfn b() { 1 } // trailing\n"),
                ),
                ("README.md", Some("# bye\n")),
            ],
        )
        .await;
    let r = api.land(d, "mallory").await;
    assert_eq!(
        r.error(),
        needs_review(d, "src/lib.rs", "needs approval from one of [alice]")
    );
    // Turning a comment into code is not a comment change.
    let e = api
        .commit(
            &[c],
            &[(
                "src/lib.rs",
                Some("fn a() {}\nfn b() {}\n// trailing\nfn c() {}\n"),
            )],
        )
        .await;
    assert_eq!(
        api.land(e, "mallory").await.status,
        StatusCode::UNPROCESSABLE_ENTITY
    );
    let e2 = api
        .commit(
            &[c],
            &[(
                "src/lib.rs",
                Some("// adds b\nfn a() {}\nfn b() {} trailing\n"),
            )],
        )
        .await;
    assert_eq!(
        api.land(e2, "mallory").await.status,
        StatusCode::UNPROCESSABLE_ENTITY
    );
}

#[tokio::test]
async fn a_multi_line_status_lists_each_rule() {
    let api = Api::with(POLICY_AUTO);
    let head = api
        .seed_main(&[
            ("OWNERS", Some("alice\n")),
            ("a.rs", Some("fn a() {}\n")),
            ("b.rs", Some("fn b() {}\n")),
            ("c.rs", Some("fn c() {}\n")),
            ("d.rs", Some("fn d() {}\n")),
        ])
        .await;
    let c = api
        .commit(
            &[head],
            &[
                ("a.rs", Some("// n\nfn a() {}\n")),
                ("b.rs", Some("fn b() {}  \r\n")),
                ("c.rs", Some("fn c() { 1 }\n")),
                ("d.rs", None),
                ("e.rs", Some("fn e() {}\n")),
            ],
        )
        .await;
    api.approve(c, "alice").await;
    api.approve(c, "bob").await;
    let s = api.status(c).await;
    assert_eq!(s.target_ref, "main");
    assert_eq!(s.changeset, c);
    assert!(s.approved);
    assert_eq!(
        rules(&s),
        [
            ("a.rs", "auto:comment_only"),
            ("b.rs", "auto:whitespace_only"),
            ("c.rs", "owner"),
            ("d.rs", "owner"),
            ("e.rs", "owner"),
        ]
    );
    assert_eq!(s.paths[2].owners, ["alice"]);
    assert_eq!(s.paths[2].approved_by, ["alice"]);
    assert!(s.paths[2].rejected_by.is_empty());

    // Another ref has no head, hence no owners: a bob approval would do.
    let r = api
        .get(&format!("/v1/changesets/{c}/review-status?ref=release"))
        .await;
    let other: ReviewStatusResponse = r.json();
    assert_eq!(other.target_ref, "release");
    assert!(other.paths.iter().all(|p| p.owners.is_empty()));
    assert!(other.approved);
}

#[tokio::test]
async fn stacks_need_every_entry_approved_and_rebased_copies_report_via() {
    let api = Api::with(POLICY);
    let base = api
        .seed_main(&[("OWNERS", Some("alice\n")), ("a", Some("1\n"))])
        .await;
    // The head moves on, so landing the stack below has to replay it.
    let head = api.commit(&[base], &[("h", Some("h\n"))]).await;
    api.approve(head, "alice").await;
    assert_eq!(api.land(head, "bob").await.status, StatusCode::OK);

    let s1 = api.commit(&[base], &[("b", Some("2\n"))]).await;
    let s2 = api.commit(&[s1], &[("c", Some("3\n"))]).await;
    api.approve(s2, "alice").await;
    let r = api.land(s2, "bob").await;
    assert_eq!(
        r.error(),
        needs_review(s1, "b", "needs approval from one of [alice]")
    );
    assert_eq!(api.main_ref().await, Some(head));

    api.approve(s1, "alice").await;
    let LandResponse::Landed {
        landed, rebased, ..
    } = api.land(s2, "bob").await.json::<LandResponse>()
    else {
        panic!("expected a land");
    };
    assert!(rebased);

    // The rebased copy has no review of its own; the listing shows the one
    // that carries to it. Its status counts it because it makes exactly
    // `s2`'s change (ADR 0085), not because the server says it is a copy.
    let list = api.reviews(landed).await;
    assert_eq!(list.via, None);
    assert_eq!(list.reviews.len(), 1);
    assert_eq!(list.reviews[0].reviewer, "alice");
    assert_eq!(list.reviews[0].carried_from, Some(s2));
    assert_eq!(api.reviews(s2).await.via, None);
    assert_eq!(api.reviews(s2).await.reviews[0].carried_from, None);
    let copy = api.status(landed).await;
    assert!(copy.approved);
    assert_eq!(copy.paths[0].rule, "owner");
    assert_eq!(copy.paths[0].carried_from, Some(s2));
}

#[tokio::test]
async fn merges_are_covered_by_their_side_parents() {
    let api = Api::with(POLICY);
    let base = api
        .seed_main(&[
            ("OWNERS", Some("alice\n")),
            ("a", Some("1\n")),
            ("b", Some("1\n")),
        ])
        .await;
    let ours = api.commit(&[base], &[("a", Some("ours\n"))]).await;
    let theirs = api.commit(&[base], &[("b", Some("theirs\n"))]).await;
    // The merge takes their b unchanged and adds nothing else.
    let m = api
        .commit(&[ours, theirs], &[("b", Some("theirs\n"))])
        .await;
    assert_eq!(rules(&api.status(m).await), [("b", "merge_parent")]);

    // Each side branch is judged on its own, the merge's first parent first.
    let r = api.land(m, "bob").await;
    assert_eq!(
        r.error(),
        needs_review(ours, "a", "needs approval from one of [alice]")
    );
    api.approve(ours, "alice").await;
    let r = api.land(m, "bob").await;
    assert_eq!(
        r.error(),
        needs_review(theirs, "b", "needs approval from one of [alice]")
    );
    api.approve(theirs, "alice").await;
    assert_eq!(api.land(m, "bob").await.status, StatusCode::OK);
    assert_eq!(api.main_ref().await, Some(m));

    // A merge that makes its own change still needs approval.
    let n = api.commit(&[m, theirs], &[("c", Some("new\n"))]).await;
    assert_eq!(rules(&api.status(n).await), [("c", "pending")]);
}

/// Security review 2026-10, S-01: a hand-made merge whose side parent is an
/// old commit could put back that commit's version of an owned file (say,
/// undo a fix) as `merge_parent`, with no owner approval.
#[tokio::test]
async fn a_merge_cannot_launder_an_old_version_back_without_review() {
    let api = Api::with(POLICY);
    let old = api
        .seed_main(&[
            ("OWNERS", Some("alice\n")),
            ("auth.rs", Some("fn check() -> bool { true }\n")),
        ])
        .await;
    let fix = api
        .commit(
            &[old],
            &[("auth.rs", Some("fn check() -> bool { verify() }\n"))],
        )
        .await;
    api.approve(fix, "alice").await;
    assert_eq!(api.land(fix, "bob").await.status, StatusCode::OK);

    // Side parent already in main's history.
    let undo = api
        .commit(
            &[fix, old],
            &[("auth.rs", Some("fn check() -> bool { true }\n"))],
        )
        .await;
    assert_eq!(rules(&api.status(undo).await), [("auth.rs", "pending")]);
    let r = api.land(undo, "mallory").await;
    assert_eq!(r.status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(
        r.error(),
        needs_review(undo, "auth.rs", "needs approval from one of [alice]")
    );

    // Side parent built on the old commit, touching nothing owned by others.
    let side = api.commit(&[old], &[("notes.txt", Some("hi\n"))]).await;
    api.approve(side, "alice").await;
    let undo = api
        .commit(
            &[fix, side],
            &[
                ("auth.rs", Some("fn check() -> bool { true }\n")),
                ("notes.txt", Some("hi\n")),
            ],
        )
        .await;
    assert_eq!(
        rules(&api.status(undo).await),
        [("auth.rs", "pending"), ("notes.txt", "merge_parent")]
    );
    assert_eq!(
        api.land(undo, "mallory").await.status,
        StatusCode::UNPROCESSABLE_ENTITY
    );
    assert_eq!(api.main_ref().await, Some(fix));
}

/// An empty merge whose first parent is the old commit and whose side parent
/// is main's head is a fast-forward of main, yet its tree is the old commit's:
/// landing it would undo the reviewed fix with no review.
#[tokio::test]
async fn a_merge_with_an_old_first_parent_cannot_rewind_main_without_review() {
    let api = Api::with(POLICY);
    let old = api
        .seed_main(&[("OWNERS", Some("alice\n")), ("auth.rs", Some("weak\n"))])
        .await;
    let fix = api.commit(&[old], &[("auth.rs", Some("strong\n"))]).await;
    api.approve(fix, "alice").await;
    assert_eq!(api.land(fix, "bob").await.status, StatusCode::OK);

    let rewind = api.commit(&[old, fix], &[]).await;
    assert_eq!(rules(&api.status(rewind).await), [("auth.rs", "pending")]);
    let r = api.land(rewind, "mallory").await;
    assert_eq!(r.status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(
        r.error(),
        needs_review(rewind, "auth.rs", "needs approval from one of [alice]")
    );
    let r = api
        .post(
            "/v1/refs/main",
            "mallory",
            &CasRefRequest {
                expected: Some(fix),
                new: Some(rewind),
                ..Default::default()
            },
        )
        .await;
    assert_eq!(r.status, StatusCode::UNPROCESSABLE_ENTITY, "{}", r.error());
    assert_eq!(api.main_ref().await, Some(fix));

    api.approve(rewind, "alice").await;
    assert_eq!(api.land(rewind, "bob").await.status, StatusCode::OK);
}

/// Security review 2026-10, S-09: rewinding a protected ref would drop a
/// reviewed fix without review, so the server refuses any move that is not
/// a fast-forward.
#[tokio::test]
async fn raw_ref_cas_cannot_rewind_a_protected_ref() {
    let api = Api::with(POLICY);
    let old = api
        .seed_main(&[("OWNERS", Some("alice\n")), ("auth.rs", Some("weak\n"))])
        .await;
    let fix = api.commit(&[old], &[("auth.rs", Some("strong\n"))]).await;
    api.approve(fix, "alice").await;
    let cas = |expected, new| CasRefRequest {
        expected: Some(expected),
        new: Some(new),
        operation: Some("undo".into()),
        ..Default::default()
    };
    // A fast-forward through the raw endpoint is allowed.
    let r = api.post("/v1/refs/main", "bob", &cas(old, fix)).await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.error());
    // Moving back is refused, by anyone.
    for who in ["mallory", "alice"] {
        let r = api.post("/v1/refs/main", who, &cas(fix, old)).await;
        assert_eq!(r.status, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(
            r.json::<ErrorResponse>().code.as_deref(),
            Some("not_forward")
        );
        assert!(
            r.error().contains(leep_core::refname::NOT_FORWARD_MARKER),
            "{}",
            r.error()
        );
    }
    assert_eq!(api.main_ref().await, Some(fix));
    // Unprotected refs can still be rewound.
    let r = api
        .post(
            "/v1/refs/scratch",
            "bob",
            &CasRefRequest {
                expected: None,
                new: Some(fix),
                ..Default::default()
            },
        )
        .await;
    assert_eq!(r.status, StatusCode::OK);
    let r = api.post("/v1/refs/scratch", "bob", &cas(fix, old)).await;
    assert_eq!(r.status, StatusCode::OK);
}

#[tokio::test]
async fn raw_ref_cas_cannot_skip_review() {
    let api = Api::with(POLICY);
    let c = api.commit(&[], &[("a", Some("1\n"))]).await;
    let cas = |new| CasRefRequest {
        expected: None,
        new: Some(new),
        ..Default::default()
    };
    let r = api.post("/v1/refs/main", "bob", &cas(c)).await;
    assert_eq!(r.status, StatusCode::UNPROCESSABLE_ENTITY);
    assert!(r.error().contains(NEEDS_REVIEW_MARKER), "{}", r.error());
    assert_eq!(
        r.json::<ErrorResponse>().code.as_deref(),
        Some("needs_review")
    );
    assert_eq!(api.main_ref().await, None);
    // Unprotected refs are unaffected.
    assert_eq!(
        api.post("/v1/refs/scratch", "bob", &cas(c)).await.status,
        StatusCode::OK
    );
    assert_eq!(
        api.land(c, "bob").await.status,
        StatusCode::UNPROCESSABLE_ENTITY,
        "land is gated too"
    );

    api.approve(c, "carol").await;
    assert_eq!(
        api.post("/v1/refs/main", "bob", &cas(c)).await.status,
        StatusCode::OK
    );
    assert_eq!(api.main_ref().await, Some(c));
    // Deleting a protected ref is refused, whatever the review state.
    let r = api
        .post(
            "/v1/refs/main",
            "bob",
            &CasRefRequest {
                expected: Some(c),
                new: None,
                ..Default::default()
            },
        )
        .await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);
    assert!(r.error().contains("protected"), "{}", r.error());
    assert_eq!(api.main_ref().await, Some(c));
}

#[tokio::test]
async fn review_and_signature_policies_both_apply() {
    let api = Api::new(Opts {
        auth: true,
        policy: Some(POLICY),
        signatures: true,
    });
    let c = api.commit(&[], &[("a", Some("1\n"))]).await;

    // Neither: the signature policy was set first and speaks first.
    let r = api.land(c, "bob").await;
    assert_eq!(r.error(), format!("changeset {c} {NOT_SIGNED_MARKER}"));

    // Signed but not reviewed.
    let sig = AddSignatureRequest {
        public_key: format_public_key(signer().verifying_key().as_bytes()),
        signature: hex::encode(signer().sign(&signing_message(&c)).to_bytes()),
    };
    let r = api
        .post(&format!("/v1/changesets/{c}/signatures"), "bob", &sig)
        .await;
    assert_eq!(
        r.status,
        StatusCode::OK,
        "{}",
        String::from_utf8_lossy(&r.body)
    );
    let r = api.land(c, "bob").await;
    assert_eq!(
        r.error(),
        needs_review(c, "a", "needs approval from any reviewer")
    );

    // Reviewed but not signed.
    let d = api.commit(&[], &[("b", Some("2\n"))]).await;
    api.approve(d, "alice").await;
    let r = api.land(d, "bob").await;
    assert_eq!(r.error(), format!("changeset {d} {NOT_SIGNED_MARKER}"));
    assert_eq!(api.main_ref().await, None);

    api.approve(c, "alice").await;
    assert_eq!(api.land(c, "bob").await.status, StatusCode::OK);
}

#[tokio::test]
async fn approving_after_the_head_moves_uses_the_new_owners() {
    let api = Api::with(POLICY);
    let head = api
        .seed_main(&[("OWNERS", Some("alice\n")), ("a", Some("1\n"))])
        .await;
    let c = api.commit(&[head], &[("a", Some("2\n"))]).await;
    api.approve(c, "alice").await;
    // Ownership moves to bob before c lands.
    let handover = api.commit(&[head], &[("OWNERS", Some("bob\n"))]).await;
    api.approve(handover, "alice").await;
    assert_eq!(api.land(handover, "carol").await.status, StatusCode::OK);
    let r = api.land(c, "carol").await;
    assert_eq!(
        r.error(),
        needs_review(c, "a", "needs approval from one of [bob]")
    );
}

#[test]
fn server_refuses_to_start_with_bad_review_config() {
    let dir = tempfile::tempdir().unwrap();
    let run = |args: &[&str]| {
        std::process::Command::new(env!("CARGO_BIN_EXE_leep-server"))
            .args([
                "--meta",
                "mem://",
                "--blobs",
                "mem://",
                "--listen",
                "127.0.0.1:0",
            ])
            .args(args)
            .env_remove("LEEP_TOKENS_FILE")
            .env_remove("LEEP_REVIEW_POLICY_FILE")
            .env_remove("LEEP_REQUIRE_REVIEW")
            .output()
            .unwrap()
    };
    let msg = |o: &std::process::Output| String::from_utf8_lossy(&o.stderr).into_owned();

    let out = run(&["--require-review", "main"]);
    assert!(!out.status.success());
    assert!(msg(&out).contains("--tokens-file"), "{}", msg(&out));

    let tokens = dir.path().join("tokens.json");
    std::fs::write(
        &tokens,
        serde_json::json!({ "tokens": [{ "name": "a", "role": "write", "hash": hash_token("t") }] })
            .to_string(),
    )
    .unwrap();
    let bad = dir.path().join("policy.json");
    std::fs::write(&bad, r#"{"auto_approve":["docs_only"]}"#).unwrap();
    let out = run(&[
        "--tokens-file",
        tokens.to_str().unwrap(),
        "--review-policy-file",
        bad.to_str().unwrap(),
    ]);
    assert!(!out.status.success());
    assert!(
        msg(&out).contains("policy.json") && msg(&out).contains("docs_globs"),
        "{}",
        msg(&out)
    );

    let out = run(&["--review-policy-file", "/nonexistent/policy.json"]);
    assert!(!out.status.success());

    // Security review 2026-10, S-06: a protected name no ref can have would
    // protect nothing, so it stops startup.
    let spaced = dir.path().join("spaced.json");
    std::fs::write(&spaced, r#"{"refs":["main "]}"#).unwrap();
    let out = run(&[
        "--tokens-file",
        tokens.to_str().unwrap(),
        "--review-policy-file",
        spaced.to_str().unwrap(),
    ]);
    assert!(!out.status.success());
    assert!(msg(&out).contains("invalid ref name"), "{}", msg(&out));
    let out = run(&[
        "--tokens-file",
        tokens.to_str().unwrap(),
        "--require-review",
        "refs//main",
    ]);
    assert!(!out.status.success());
    assert!(msg(&out).contains("--require-review"), "{}", msg(&out));
}

/// A token with `"review": false` (an agent's) cannot post reviews, and
/// reviews it posted before it lost the right stop counting.
#[tokio::test]
async fn tokens_without_review_cannot_review_and_their_old_reviews_stop_counting() {
    let tokens = |agent_reviews: bool| {
        Auth::parse(
            &serde_json::json!({ "tokens": [
                { "name": "alice", "role": "write", "hash": hash_token(&token("alice")) },
                { "name": "agent", "role": "write", "hash": hash_token(&token("agent")),
                  "review": agent_reviews },
                { "name": "viewer", "role": "read", "hash": hash_token(VIEWER) },
            ]})
            .to_string(),
        )
        .unwrap()
    };
    let repo = Repo::new(
        Arc::new(MemoryBlobStore::new()),
        Arc::new(MemoryMetaStore::new()),
    );
    let api = |auth: Auth| {
        let svc = Arc::new(ReviewService::new(
            auth.reviewers(),
            ReviewPolicy::parse(POLICY).unwrap().rules,
        ));
        let repo = repo
            .clone()
            .with_land_policy(Arc::new(RequireReview::new(svc.clone(), ["main".into()])));
        Api {
            app: leep_server::router_with_review(
                repo.clone(),
                1 << 20,
                Some(auth),
                None,
                Some(svc),
            ),
            repo,
        }
    };
    let before = api(tokens(true));
    let c = before.commit(&[], &[("a", Some("1\n"))]).await;
    before.approve(c, "agent").await;
    assert!(before.status(c).await.approved);

    let after = api(tokens(false));
    let r = after.review(c, "agent", Verdict::Approve).await;
    assert_eq!(r.status, StatusCode::FORBIDDEN);
    assert!(r.error().contains("\"review\": false"), "{}", r.error());
    assert_eq!(
        r.json::<ErrorResponse>().code.as_deref(),
        Some(leep_core::api::error_code::FORBIDDEN)
    );
    // Its earlier approval is no longer listed and no longer counts.
    let listed = after.reviews(c).await;
    assert!(listed.reviews.is_empty(), "{:?}", listed.reviews);
    assert!(!after.status(c).await.approved);
    let r = after.land(c, "agent").await;
    assert_eq!(r.status, StatusCode::UNPROCESSABLE_ENTITY);
    assert!(r.error().contains(NEEDS_REVIEW_MARKER), "{}", r.error());
    // It can still do everything else a write token does, and a reviewer's
    // approval lets its change land.
    after.approve(c, "alice").await;
    let r = after.land(c, "agent").await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.error());
}

fn review_for(who: &str, v: Verdict, source: Option<&str>) -> AddReviewRequest {
    AddReviewRequest {
        verdict: v,
        comment: String::new(),
        reviewer: Some(who.into()),
        source: source.map(Into::into),
    }
}

#[tokio::test]
async fn review_for_stores_under_the_named_reviewer_with_a_source_row() {
    let api = Api::with(POLICY);
    let head = api
        .seed_main(&[
            (
                "OWNERS",
                Some(
                    "alice
",
                ),
            ),
            (
                "a",
                Some(
                    "1
",
                ),
            ),
        ])
        .await;
    let c = api
        .commit(
            &[head],
            &[(
                "a",
                Some(
                    "2
",
                ),
            )],
        )
        .await;
    let uri = format!("/v1/changesets/{c}/reviews");
    let r = api
        .post(
            &uri,
            "mirror",
            &review_for("alice", Verdict::Approve, Some("github:o/r#1/reviews/2")),
        )
        .await;
    assert_eq!(
        r.status,
        StatusCode::OK,
        "{}",
        String::from_utf8_lossy(&r.body)
    );
    assert_eq!(r.json::<AddReviewResponse>().reviewer, "alice");
    assert!(api.repo.get_review(c, "mirror").await.unwrap().is_none());
    let row = api.repo.get_review(c, "alice").await.unwrap().unwrap();
    assert_eq!(row.verdict, Verdict::Approve);
    let src = api
        .repo
        .get_review_source(c, "alice")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (src.via.as_str(), src.source.as_str()),
        ("mirror", "github:o/r#1/reviews/2")
    );

    // The owner approval counts, so the change is approved.
    assert!(api.status(c).await.approved);
    let list = api.reviews(c).await;
    assert_eq!(list.reviews.len(), 1);
    assert_eq!(list.reviews[0].reviewer, "alice");
    assert_eq!(
        list.reviews[0].source.as_deref(),
        Some("github:o/r#1/reviews/2")
    );
    assert_eq!(list.reviews[0].via.as_deref(), Some("mirror"));
    // Old clients' view: the review fields stay at the top level.
    let raw: serde_json::Value = api.get(&uri).await.json();
    assert_eq!(raw["reviews"][0]["reviewer"], "alice");

    // A direct post by alice replaces the row and no longer shows the source.
    api.approve(c, "alice").await;
    let list = api.reviews(c).await;
    assert_eq!(list.reviews[0].source, None);
    assert_eq!(list.reviews[0].via.as_deref(), Some("alice"));
}

#[tokio::test]
async fn naming_a_reviewer_needs_the_review_for_capability() {
    let api = Api::with(POLICY);
    let c = api
        .commit(
            &[],
            &[(
                "a",
                Some(
                    "1
",
                ),
            )],
        )
        .await;
    let uri = format!("/v1/changesets/{c}/reviews");
    for who in ["bob", "mallory"] {
        let r = api
            .post(&uri, who, &review_for("alice", Verdict::Approve, None))
            .await;
        assert_eq!(r.status, StatusCode::FORBIDDEN, "{who}");
        assert!(r.error().contains("review_for"), "{}", r.error());
    }
    // Not taken as the token's own review either.
    for name in ["alice", "bob", "mallory"] {
        assert!(api.repo.get_review(c, name).await.unwrap().is_none());
    }
    assert!(api.reviews(c).await.reviews.is_empty());
    // Without a reviewer the token still reviews as itself.
    api.approve(c, "bob").await;
}

#[tokio::test]
async fn withdrawn_counts_as_no_review_even_after_an_approve() {
    let api = Api::with(POLICY);
    let head = api
        .seed_main(&[
            (
                "OWNERS",
                Some(
                    "alice
",
                ),
            ),
            (
                "a",
                Some(
                    "1
",
                ),
            ),
        ])
        .await;
    let c = api
        .commit(
            &[head],
            &[(
                "a",
                Some(
                    "2
",
                ),
            )],
        )
        .await;
    api.approve(c, "alice").await;
    assert!(api.status(c).await.approved);
    assert_eq!(
        api.review(c, "alice", Verdict::Withdrawn).await.status,
        StatusCode::OK
    );
    let s = api.status(c).await;
    assert!(!s.approved);
    assert_eq!(rules(&s), [("a", "pending")]);
    assert!(s.paths[0].approved_by.is_empty() && s.paths[0].rejected_by.is_empty());
    let r = api.land(c, "bob").await;
    assert_eq!(
        r.error(),
        needs_review(c, "a", "needs approval from one of [alice]")
    );
    // Listed, as what it is.
    assert_eq!(api.reviews(c).await.reviews[0].verdict, Verdict::Withdrawn);

    // It also lifts a veto, and an approval can follow.
    api.review(c, "alice", Verdict::Reject).await;
    assert_eq!(rules(&api.status(c).await), [("a", "rejected")]);
    api.review(c, "alice", Verdict::Withdrawn).await;
    assert_eq!(rules(&api.status(c).await), [("a", "pending")]);
    // On behalf of someone, too.
    let r = api
        .post(
            &format!("/v1/changesets/{c}/reviews"),
            "mirror",
            &review_for("alice", Verdict::Approve, Some("x")),
        )
        .await;
    assert_eq!(r.status, StatusCode::OK);
    assert!(api.status(c).await.approved);
    assert_eq!(api.land(c, "bob").await.status, StatusCode::OK);
}

#[tokio::test]
async fn rows_written_by_the_old_encoder_still_decode_and_count() {
    let api = Api::with(POLICY);
    let c = api.commit(&[], &[("a", Some("1\n"))]).await;
    // postcard of Review { "bob", Approve, "", 7 } as written before
    // `Withdrawn` existed, put straight into the store under the row's key.
    let old = [3, b'b', b'o', b'b', 0, 0, 14];
    let key = {
        let mut bytes = c.0.as_bytes().to_vec();
        bytes.extend_from_slice(b"bob");
        Hash::of_object("review", &bytes)
    };
    api.repo
        .meta()
        .put_derived("review-v1", &key, Bytes::copy_from_slice(&old))
        .await
        .unwrap();
    let row = api.repo.get_review(c, "bob").await.unwrap().unwrap();
    assert_eq!((row.verdict, row.timestamp), (Verdict::Approve, 7));
    let list = api.reviews(c).await;
    assert_eq!(list.reviews.len(), 1);
    assert_eq!(
        (list.reviews[0].source.clone(), list.reviews[0].via.clone()),
        (None, None)
    );
    assert!(api.status(c).await.approved);
}

/// The setting of ADR 0085: `src/a.rs` (owned by alice) and the changelog
/// change together; main's head then changes only the changelog, so the
/// server's replay of `x` conflicts there and the client commits a
/// resolution `y` on the new head, naming `x` as where it came from.
struct Carry {
    api: Api,
    base: ChangesetId,
    head: ChangesetId,
    x: ChangesetId,
}

const A0: &str = "fn a() {}\n";
const A1: &str = "fn a() { b() }\n";

async fn carry_setup() -> Carry {
    let api = Api::with(POLICY_AUTO);
    let base = api
        .seed_main(&[
            ("OWNERS", Some("alice\n")),
            ("src/a.rs", Some(A0)),
            ("CHANGELOG.md", Some("# log\n")),
        ])
        .await;
    let x = api
        .commit(
            &[base],
            &[
                ("src/a.rs", Some(A1)),
                ("CHANGELOG.md", Some("# log\n- x\n")),
            ],
        )
        .await;
    let other = api
        .commit(&[base], &[("CHANGELOG.md", Some("# log\n- other\n"))])
        .await;
    assert_eq!(api.land(other, "bob").await.status, StatusCode::OK);
    Carry {
        head: api.main_ref().await.unwrap(),
        api,
        base,
        x,
    }
}

impl Carry {
    /// `x`'s change of `src/a.rs` made again on the head, with `a` as its
    /// content and the changelog merged.
    async fn resolve(&self, a: &str, from: Option<ChangesetId>) -> ChangesetId {
        let from = from.map(|f| f.to_string());
        self.api
            .commit_from(
                &[self.head],
                &[
                    ("src/a.rs", Some(a)),
                    ("CHANGELOG.md", Some("# log\n- other\n- x\n")),
                ],
                from.as_deref(),
            )
            .await
    }
}

#[tokio::test]
async fn an_approval_carries_to_a_changelog_only_conflict_resolution() {
    let c = carry_setup().await;
    let api = &c.api;
    api.approve(c.x, "alice").await;
    let r = api.land(c.x, "bob").await;
    assert!(
        matches!(r.json::<LandResponse>(), LandResponse::Conflicts { .. }),
        "{}",
        String::from_utf8_lossy(&r.body)
    );

    // Without the hint nothing names `x`: a fresh changeset, judged alone.
    let bare = c.resolve(A1, None).await;
    assert!(!api.status(bare).await.approved);

    let y = c.resolve(A1, Some(c.x)).await;
    let s = api.status(y).await;
    assert!(s.approved, "{s:?}");
    assert_eq!(
        rules(&s),
        [("CHANGELOG.md", "auto:docs_only"), ("src/a.rs", "owner")]
    );
    assert_eq!(s.paths[1].approved_by, ["alice"]);
    assert_eq!(s.paths[1].carried_from, Some(c.x));
    assert_eq!(s.paths[0].carried_from, None);
    // The JSON names it; a path without one leaves the field out.
    let raw: serde_json::Value = api
        .get(&format!("/v1/changesets/{y}/review-status"))
        .await
        .json();
    assert_eq!(raw["paths"][1]["carried_from"], c.x.to_string());
    assert!(raw["paths"][0].get("carried_from").is_none());
    // `review list` shows the carried approval, saying whose it is.
    let list = api.reviews(y).await;
    assert_eq!(list.via, None);
    assert_eq!(list.reviews.len(), 1);
    assert_eq!(
        (
            list.reviews[0].reviewer.as_str(),
            list.reviews[0].carried_from
        ),
        ("alice", Some(c.x))
    );
    assert!(s.on_refs.is_empty());
    assert_eq!(api.land(y, "bob").await.status, StatusCode::OK);
    assert_eq!(api.main_ref().await, Some(y));
    // Landed: status says so, and a verdict on it is kept but answered
    // with the refs that already hold it (ADR 0087).
    assert_eq!(api.status(y).await.on_refs, ["main"]);
    let r: AddReviewResponse = api.review(y, "carol", Verdict::Approve).await.json();
    assert_eq!(r.on_refs, ["main"]);
    assert!(r.taken_back.is_empty());
    let r: AddReviewResponse = api.review(c.x, "carol", Verdict::Approve).await.json();
    assert!(r.on_refs.is_empty(), "x itself never landed");
}

#[tokio::test]
async fn a_hint_through_an_unreviewed_resolution_still_reaches_the_approval() {
    // Resolved twice: `y2` names `y1`, which names the approved `x`.
    let c = carry_setup().await;
    c.api.approve(c.x, "alice").await;
    let y1 = c.resolve(A1, Some(c.x)).await;
    let y2 = c
        .api
        .commit_from(&[c.head], &[("src/a.rs", Some(A1))], Some(&y1.to_string()))
        .await;
    let s = c.api.status(y2).await;
    assert!(s.approved, "{s:?}");
    assert_eq!(s.paths[0].carried_from, Some(c.x));
}

#[tokio::test]
async fn one_changed_byte_in_an_owned_file_stays_pending() {
    let c = carry_setup().await;
    c.api.approve(c.x, "alice").await;
    let y = c.resolve("fn a() { c() }\n", Some(c.x)).await;
    let s = c.api.status(y).await;
    assert!(!s.approved);
    assert_eq!(rules(&s)[1], ("src/a.rs", "pending"));
    assert_eq!(s.paths[1].carried_from, None);
    let r = c.api.land(y, "bob").await;
    assert_eq!(r.status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(
        r.error(),
        needs_review(y, "src/a.rs", "needs approval from one of [alice]")
    );
}

#[tokio::test]
async fn a_forged_hint_to_an_approved_changeset_gains_nothing() {
    // `y` names the approved `x` but makes another change, on `x`'s own base:
    // nothing about the claim is checked, only the content.
    let c = carry_setup().await;
    c.api.approve(c.x, "alice").await;
    let evil = c
        .api
        .commit_from(
            &[c.base],
            &[
                ("src/a.rs", Some("fn a() { evil() }\n")),
                ("CHANGELOG.md", Some("# log\n- x\n")),
            ],
            Some(&c.x.to_string()),
        )
        .await;
    let s = c.api.status(evil).await;
    assert!(!s.approved);
    assert_eq!(rules(&s)[1], ("src/a.rs", "pending"));
    // Nor does naming it among junk, itself, or ids that do not exist.
    let junk = format!(
        "nope,{},{evil},{}",
        ChangesetId(Hash::of_blob(b"missing")),
        c.x
    );
    let again = c
        .api
        .commit_from(
            &[c.base],
            &[("src/a.rs", Some("fn a() { evil() }\n"))],
            Some(&junk),
        )
        .await;
    assert!(!c.api.status(again).await.approved);
    let r = c.api.land(again, "bob").await;
    assert_eq!(r.status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(c.api.main_ref().await, Some(c.head));
}

#[tokio::test]
async fn dropping_a_reviewed_path_carries_nothing() {
    let api = Api::with(POLICY);
    let base = api
        .seed_main(&[
            ("OWNERS", Some("alice\n")),
            ("a", Some("1\n")),
            ("b", Some("1\n")),
        ])
        .await;
    // The fix to `a` was approved together with the check in `b`.
    let x = api
        .commit(&[base], &[("a", Some("2\n")), ("b", Some("2\n"))])
        .await;
    api.approve(x, "alice").await;
    let y = api
        .commit_from(&[base], &[("a", Some("2\n"))], Some(&x.to_string()))
        .await;
    let s = api.status(y).await;
    assert!(!s.approved);
    assert_eq!(rules(&s), [("a", "pending")]);
    // Adding a path is fine: it is judged on its own.
    let z = api
        .commit_from(
            &[base],
            &[("a", Some("2\n")), ("b", Some("2\n")), ("c", Some("new\n"))],
            Some(&x.to_string()),
        )
        .await;
    let s = api.status(z).await;
    assert_eq!(
        rules(&s),
        [("a", "owner"), ("b", "owner"), ("c", "pending")]
    );
    assert_eq!(s.paths[2].approved_by, Vec::<String>::new());
}

#[tokio::test]
async fn a_base_that_moved_under_a_reviewed_path_stays_pending() {
    // `x` changed `a` from 1 to 2; main then changed it from 1 to 3. A `y`
    // ending at 2 on the new head has the same new content, but its change
    // is 3 -> 2 (it undoes main's): not what alice approved.
    let api = Api::with(POLICY);
    let base = api
        .seed_main(&[("OWNERS", Some("alice\n")), ("a", Some("1\n"))])
        .await;
    let x = api.commit(&[base], &[("a", Some("2\n"))]).await;
    api.approve(x, "alice").await;
    let m = api.commit(&[base], &[("a", Some("3\n"))]).await;
    api.approve(m, "alice").await;
    assert_eq!(api.land(m, "bob").await.status, StatusCode::OK);
    let y = api
        .commit_from(&[m], &[("a", Some("2\n"))], Some(&x.to_string()))
        .await;
    let s = api.status(y).await;
    assert!(!s.approved);
    assert_eq!(rules(&s), [("a", "pending")]);
}

#[tokio::test]
async fn a_reject_carries_and_the_reviewers_own_verdict_wins() {
    let c = carry_setup().await;
    assert_eq!(
        c.api.review(c.x, "alice", Verdict::Reject).await.status,
        StatusCode::OK
    );
    let y = c.resolve(A1, Some(c.x)).await;
    let s = c.api.status(y).await;
    assert_eq!(rules(&s)[1], ("src/a.rs", "rejected"));
    assert_eq!(s.paths[1].rejected_by, ["alice"]);
    assert_eq!(s.paths[1].carried_from, Some(c.x));
    let r = c.api.land(y, "bob").await;
    assert_eq!(r.status, StatusCode::UNPROCESSABLE_ENTITY);
    assert!(r.error().ends_with("rejected by alice"), "{}", r.error());
    // Alice looks at `y` itself: her verdict on it replaces the carried one.
    c.api.approve(y, "alice").await;
    let s = c.api.status(y).await;
    assert!(s.approved);
    assert_eq!(s.paths[1].carried_from, None);
}

#[tokio::test]
async fn a_withdrawal_carries_nothing_on_either_side() {
    let c = carry_setup().await;
    c.api.approve(c.x, "alice").await;
    c.api.review(c.x, "alice", Verdict::Withdrawn).await;
    let y = c.resolve(A1, Some(c.x)).await;
    assert!(!c.api.status(y).await.approved);

    // An approval of `x` does not outvote alice's withdrawal on `y` itself.
    c.api.approve(c.x, "alice").await;
    assert!(c.api.status(y).await.approved);
    c.api.review(y, "alice", Verdict::Withdrawn).await;
    let s = c.api.status(y).await;
    assert!(!s.approved);
    assert_eq!(rules(&s)[1], ("src/a.rs", "pending"));
}

#[tokio::test]
async fn a_policy_refusal_is_422_with_its_code_and_no_invalid_input_prefix() {
    let api = Api::with(POLICY);
    let c = api.commit(&[], &[("a", Some("1\n"))]).await;
    let r = api.land(c, "bob").await;
    assert_eq!(r.status, StatusCode::UNPROCESSABLE_ENTITY);
    let e: ErrorResponse = r.json();
    assert_eq!(e.code.as_deref(), Some("needs_review"));
    assert_eq!(
        e.error,
        needs_review(c, "a", "needs approval from any reviewer")
    );
    // Bad input is still 400, whatever is wrong with it.
    let bob = Some(token("bob"));
    for body in [&b"nope"[..], b"{}"] {
        let r = api
            .call(Method::POST, "/v1/land", bob.as_deref(), body.to_vec())
            .await;
        assert_eq!(r.status, StatusCode::BAD_REQUEST);
        assert_eq!(r.json::<ErrorResponse>().code.as_deref(), Some("invalid"));
    }
}

#[tokio::test]
async fn a_resolved_stack_carries_each_entry_it_kept_exactly() {
    // The CLI names the stack newest first. `x1` and `x2` were approved one
    // by one; `y` makes both changes at once on a moved head.
    let api = Api::with(POLICY);
    let base = api
        .seed_main(&[
            ("OWNERS", Some("alice\n")),
            ("a", Some("1\n")),
            ("b", Some("1\n")),
        ])
        .await;
    let x1 = api.commit(&[base], &[("a", Some("2\n"))]).await;
    let x2 = api.commit(&[x1], &[("b", Some("2\n"))]).await;
    api.approve(x1, "alice").await;
    api.approve(x2, "alice").await;
    let m = api.commit(&[base], &[("c", Some("m\n"))]).await;
    api.approve(m, "alice").await;
    assert_eq!(api.land(m, "bob").await.status, StatusCode::OK);
    let hint = format!("{x2},{x1}");
    let y = api
        .commit_from(&[m], &[("a", Some("2\n")), ("b", Some("2\n"))], Some(&hint))
        .await;
    let s = api.status(y).await;
    assert!(s.approved, "{s:?}");
    assert_eq!(s.paths[0].carried_from, Some(x1));
    assert_eq!(s.paths[1].carried_from, Some(x2));
    // An entry whose change the resolution altered carries nothing; the
    // other still does.
    let z = api
        .commit_from(&[m], &[("a", Some("2\n")), ("b", Some("3\n"))], Some(&hint))
        .await;
    let s = api.status(z).await;
    assert_eq!(rules(&s), [("a", "owner"), ("b", "pending")]);
    assert_eq!(
        api.land(z, "bob").await.status,
        StatusCode::UNPROCESSABLE_ENTITY
    );
    assert_eq!(api.land(y, "bob").await.status, StatusCode::OK);
}

/// A review stored with a chosen time, past the endpoint (and so past its
/// take-back), to order verdicts within one second.
async fn put_at(api: &Api, id: ChangesetId, who: &str, v: Verdict, at: i64) {
    api.repo
        .put_review(
            id,
            &leep_core::review::Review {
                reviewer: who.into(),
                verdict: v,
                comment: String::new(),
                timestamp: at,
            },
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn the_newest_verdict_wins_whatever_order_the_hint_names_them_in() {
    // Alice approved `x`, then rejected (or withdrew on) `x2`, an identical
    // re-commit. A third copy names both, in either order: her newest word
    // decides, not the first id in the hint.
    for later in [Verdict::Reject, Verdict::Withdrawn] {
        let c = carry_setup().await;
        put_at(&c.api, c.x, "alice", Verdict::Approve, 100).await;
        let x2 = c.resolve(A1, Some(c.x)).await;
        assert!(c.api.status(x2).await.approved);
        put_at(&c.api, x2, "alice", later, 200).await;
        for hint in [format!("{},{x2}", c.x), format!("{x2},{}", c.x)] {
            let x3 = c
                .api
                .commit_from(&[c.head], &[("src/a.rs", Some(A1))], Some(&hint))
                .await;
            let s = c.api.status(x3).await;
            assert!(!s.approved, "{later:?} {hint}: {s:?}");
            let want = if later == Verdict::Reject {
                "rejected"
            } else {
                "pending"
            };
            assert_eq!(rules(&s), [("src/a.rs", want)], "{hint}");
            assert_eq!(
                c.api.land(x3, "bob").await.status,
                StatusCode::UNPROCESSABLE_ENTITY
            );
        }
    }
}

#[tokio::test]
async fn a_newer_approval_wins_only_where_its_changeset_reproduces_the_path() {
    let c = carry_setup().await;
    put_at(&c.api, c.x, "alice", Verdict::Reject, 100).await;
    // `same` reproduces `x`'s change; `other` does not.
    let same = c.resolve(A1, Some(c.x)).await;
    put_at(&c.api, same, "alice", Verdict::Approve, 200).await;
    let other = c.resolve("fn a() { other() }\n", Some(c.x)).await;
    put_at(&c.api, other, "alice", Verdict::Approve, 300).await;
    for hint in [format!("{},{same}", c.x), format!("{same},{}", c.x)] {
        let y = c
            .api
            .commit_from(&[c.head], &[("src/a.rs", Some(A1))], Some(&hint))
            .await;
        let s = c.api.status(y).await;
        assert!(s.approved, "{hint}: {s:?}");
        assert_eq!(s.paths[0].carried_from, Some(same));
    }
    for hint in [format!("{},{other}", c.x), format!("{other},{}", c.x)] {
        let y = c
            .api
            .commit_from(&[c.head], &[("src/a.rs", Some(A1))], Some(&hint))
            .await;
        let s = c.api.status(y).await;
        assert_eq!(rules(&s), [("src/a.rs", "rejected")], "{hint}");
        assert_eq!(s.paths[0].carried_from, Some(c.x));
    }
}

#[tokio::test]
async fn a_reject_on_a_copy_takes_back_the_approval_it_carried() {
    // The copy that names only `x` would otherwise never see the reject on
    // `x2`: the reject is also recorded on `x`, as alice's, with its source.
    for later in [Verdict::Reject, Verdict::Withdrawn] {
        let c = carry_setup().await;
        c.api.approve(c.x, "alice").await;
        let x2 = c.resolve(A1, Some(c.x)).await;
        assert!(c.api.status(x2).await.approved);
        let r = c.api.review(x2, "alice", later).await;
        assert_eq!(r.status, StatusCode::OK);
        // The answer says where else the verdict was written.
        assert_eq!(r.json::<AddReviewResponse>().taken_back, [c.x]);
        let on_x = c.api.reviews(c.x).await.reviews;
        assert_eq!(on_x[0].verdict, later);
        assert_eq!(
            on_x[0].source.as_deref(),
            Some(&*format!("leep:taken-back-on:{x2}"))
        );
        let x3 = c.resolve(A1, Some(c.x)).await;
        assert!(!c.api.status(x3).await.approved, "{later:?}");
        assert_eq!(
            c.api.land(x3, "bob").await.status,
            StatusCode::UNPROCESSABLE_ENTITY
        );
        // A copy that did not reproduce `x` takes nothing back.
        c.api.approve(c.x, "alice").await;
        let other = c.resolve("fn a() { other() }\n", Some(c.x)).await;
        let r = c.api.review(other, "alice", Verdict::Reject).await;
        assert!(r.json::<AddReviewResponse>().taken_back.is_empty());
        assert_eq!(
            c.api.reviews(c.x).await.reviews[0].verdict,
            Verdict::Approve
        );
    }
}

#[tokio::test]
async fn too_many_reviewed_candidates_carry_nothing_and_the_work_is_bounded() {
    use leep_testkit::budget::Budget;
    // Behind a counting store: the work of one status request on a
    // changeset whose hint names 64 approved candidates, none reproducing.
    let budget = Budget::new(
        Arc::new(MemoryBlobStore::new()),
        Arc::new(MemoryMetaStore::new()),
    );
    let mut repo = Repo::new(budget.clone(), budget.clone());
    let policy = ReviewPolicy::parse(POLICY).unwrap();
    let svc = Arc::new(ReviewService::new(auth().reviewers(), policy.rules));
    repo = repo.with_land_policy(Arc::new(RequireReview::new(svc.clone(), policy.refs)));
    let api = Api {
        app: leep_server::router_with_review(repo.clone(), 1 << 20, Some(auth()), None, Some(svc)),
        repo,
    };
    let base = api
        .seed_main(&[("OWNERS", Some("alice\n")), ("a", Some("1\n"))])
        .await;
    let mut ids = Vec::new();
    for i in 0..64 {
        let x = api
            .commit(&[base], &[("a", Some(&format!("x{i}\n")))])
            .await;
        put_at(&api, x, "alice", Verdict::Approve, 100).await;
        ids.push(x.to_string());
    }
    let y = api
        .commit_from(&[base], &[("a", Some("y\n"))], Some(&ids.join(",")))
        .await;
    budget.start();
    let s = api.status(y).await;
    let used = budget.stop();
    assert_eq!(rules(&s), [("a", "pending")]);
    // The walk reads the 64 changesets (and the one rebase row of the
    // hinted changeset); reviews are read per candidate (4 principals here)
    // until the 17th reviewed one, when nothing carries and the rest are
    // not compared. Measured: 176 calls (100 changesets, 72 review rows,
    // and the ref list and ref read that `on_refs` makes, ADR 0087).
    let meta = used.meta_calls();
    eprintln!(
        "carry status over 64 candidates: {meta} meta calls, {} blob calls\n{used}",
        used.blob_calls()
    );
    assert!(meta <= 180, "{meta} meta calls\n{used}");
    // With one more reviewed candidate than the cap, an approval that
    // would carry does not: no partial answer.
    let good = api.commit(&[base], &[("a", Some("y\n"))]).await;
    put_at(&api, good, "alice", Verdict::Approve, 100).await;
    let mut hint = vec![good.to_string()];
    hint.extend(ids.iter().take(16).cloned());
    let z = api
        .commit_from(&[base], &[("a", Some("y\n"))], Some(&hint.join(",")))
        .await;
    assert!(!api.status(z).await.approved);
    let ok = api
        .commit_from(&[base], &[("a", Some("y\n"))], Some(&hint[..16].join(",")))
        .await;
    assert!(api.status(ok).await.approved);
}

#[tokio::test]
async fn a_reject_takes_back_an_approval_stamped_later_by_a_skewed_clock() {
    // The approval on `x` carries a later time than the reject posted on
    // `x2` (another server's clock ahead, or this one stepped back). The
    // reject is still alice's newest word: it is taken back, and stamped so
    // a copy naming both does not rank the approval above it.
    let c = carry_setup().await;
    let ahead = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
        + 1_000;
    put_at(&c.api, c.x, "alice", Verdict::Approve, ahead).await;
    let x2 = c.resolve(A1, Some(c.x)).await;
    assert!(c.api.status(x2).await.approved);
    assert_eq!(
        c.api.review(x2, "alice", Verdict::Reject).await.status,
        StatusCode::OK
    );
    let on_x = c.api.repo.get_review(c.x, "alice").await.unwrap().unwrap();
    assert_eq!(on_x.verdict, Verdict::Reject);
    assert!(on_x.timestamp > ahead, "{on_x:?}");
    for hint in [
        c.x.to_string(),
        format!("{},{x2}", c.x),
        format!("{x2},{}", c.x),
    ] {
        let z = c
            .api
            .commit_from(&[c.head], &[("src/a.rs", Some(A1))], Some(&hint))
            .await;
        let s = c.api.status(z).await;
        assert_eq!(rules(&s), [("src/a.rs", "rejected")], "{hint}: {s:?}");
        assert_eq!(
            c.api.land(z, "bob").await.status,
            StatusCode::UNPROCESSABLE_ENTITY
        );
    }
}

#[tokio::test]
async fn a_reject_takes_back_every_approved_copy_it_names() {
    // More approved copies than the carry comparison cap: taking back is
    // not capped, so none of them is left to carry the approval.
    let c = carry_setup().await;
    let mut copies = Vec::new();
    for i in 0..17 {
        // A junk id in each hint only makes the copies distinct.
        let x = c
            .api
            .commit_from(
                &[c.head],
                &[
                    ("src/a.rs", Some(A1)),
                    ("CHANGELOG.md", Some("# log\n- other\n- x\n")),
                ],
                Some(&format!("{},junk{i}", c.x)),
            )
            .await;
        c.api.approve(x, "alice").await;
        copies.push(x);
    }
    let hint: Vec<String> = copies.iter().map(ToString::to_string).collect();
    let y = c
        .api
        .commit_from(&[c.head], &[("src/a.rs", Some(A1))], Some(&hint.join(",")))
        .await;
    assert_eq!(
        c.api.review(y, "alice", Verdict::Reject).await.status,
        StatusCode::OK
    );
    for x in &copies {
        let r = c.api.repo.get_review(*x, "alice").await.unwrap().unwrap();
        assert_eq!(r.verdict, Verdict::Reject, "{x}");
    }
    let z = c
        .api
        .commit_from(&[c.head], &[("src/a.rs", Some(A1))], Some(&hint[16]))
        .await;
    assert_eq!(rules(&c.api.status(z).await), [("src/a.rs", "rejected")]);
}

/// ADR 0089: `main` holds `src/a.rs` (owned by alice) and a changelog. `x`
/// changes `a.rs` (`x_a`) and appends to the changelog; alice approves it.
/// Then `head_a`, approved too, lands first, with its own changelog line,
/// so `x` conflicts and the client resolves it on the new head.
struct Moved {
    api: Api,
    head: ChangesetId,
    x: ChangesetId,
}

async fn moved_setup(policy: &'static str, a0: &str, x_a: &str, head_a: &str) -> Moved {
    moved_on(Api::with(policy), a0, x_a, head_a).await
}

async fn moved_on(api: Api, a0: &str, x_a: &str, head_a: &str) -> Moved {
    moved_owned(api, "alice\n", a0, x_a, head_a).await
}

/// [`moved_on`] with `owners` as the root `OWNERS` file.
async fn moved_owned(api: Api, owners: &str, a0: &str, x_a: &str, head_a: &str) -> Moved {
    let base = api
        .seed_main(&[
            ("OWNERS", Some(owners)),
            ("src/a.rs", Some(a0)),
            ("CHANGELOG.md", Some("# log\n")),
        ])
        .await;
    let x = api
        .commit(
            &[base],
            &[
                ("src/a.rs", Some(x_a)),
                ("CHANGELOG.md", Some("# log\n- x\n")),
            ],
        )
        .await;
    api.approve(x, "alice").await;
    let other = api
        .commit(
            &[base],
            &[
                ("src/a.rs", Some(head_a)),
                ("CHANGELOG.md", Some("# log\n- other\n")),
            ],
        )
        .await;
    api.approve(other, "alice").await;
    assert_eq!(api.land(other, "bob").await.status, StatusCode::OK);
    Moved {
        head: api.main_ref().await.unwrap(),
        api,
        x,
    }
}

impl Moved {
    /// The client's resolution on the head, naming `x`, with `a` as
    /// `src/a.rs` and both changelog lines.
    async fn resolve(&self, a: &str) -> ChangesetId {
        self.api
            .commit_from(
                &[self.head],
                &[
                    ("src/a.rs", Some(a)),
                    ("CHANGELOG.md", Some("# log\n- other\n- x\n")),
                ],
                Some(&self.x.to_string()),
            )
            .await
    }
}

const F0: &str = "fn a() {\n    one();\n}\n\nfn b() {\n    two();\n}\n";
/// `x`: a check added at the end of `b`.
const FX: &str = "fn a() {\n    one();\n}\n\nfn b() {\n    two();\n    check();\n}\n";
/// The head: `a` changed.
const FH: &str = "fn a() {\n    uno();\n}\n\nfn b() {\n    two();\n}\n";
/// The server's rebase of `x`'s change onto the head.
const FR: &str = "fn a() {\n    uno();\n}\n\nfn b() {\n    two();\n    check();\n}\n";

#[tokio::test]
async fn an_approval_carries_to_a_resolution_equal_to_the_servers_rebase() {
    let m = moved_setup(POLICY_AUTO, F0, FX, FH).await;
    let api = &m.api;
    // The server itself would make FR, but the changelog conflicts.
    let r = api.land(m.x, "bob").await;
    assert!(matches!(
        r.json::<LandResponse>(),
        LandResponse::Conflicts { .. }
    ));
    let y = m.resolve(FR).await;
    let s = api.status(y).await;
    assert!(s.approved, "{s:?}");
    assert_eq!(
        rules(&s),
        [("CHANGELOG.md", "auto:docs_only"), ("src/a.rs", "owner")]
    );
    assert_eq!(
        (s.paths[1].carried_from, s.paths[1].carried_rule.as_deref()),
        (Some(m.x), Some("rebased"))
    );
    assert_eq!(s.paths[0].carried_rule, None);
    assert_eq!(s.paths[1].carry.as_deref(), Some("merged"));
    assert_eq!(s.origins.len(), 1);
    assert!(s.origins[0].carried && s.origins[0].differs.is_empty());
    let raw: serde_json::Value = api
        .get(&format!("/v1/changesets/{y}/review-status"))
        .await
        .json();
    assert_eq!(raw["paths"][1]["carried_rule"], "rebased");
    assert!(raw["paths"][0].get("carried_rule").is_none());
    assert_eq!(raw["origins"][0]["reviews"][0]["reviewer"], "alice");
    // `review list` shows it as carried, and it lands.
    assert_eq!(api.reviews(y).await.reviews[0].carried_from, Some(m.x));
    assert_eq!(api.land(y, "bob").await.status, StatusCode::OK);
    assert_eq!(api.main_ref().await, Some(y));
}

/// The motivating case: two tests appended at the end of one module.
const T0: &str = "mod tests {\n    fn t0() {}\n}\n";
const TX: &str = "mod tests {\n    fn t0() {}\n    fn dee() {}\n}\n";
const TH: &str = "mod tests {\n    fn t0() {}\n    fn cy() {}\n}\n";

#[tokio::test]
async fn an_approval_carries_to_a_union_of_two_appends_in_either_order() {
    // No auto rule: the changelog's two lines are a union too.
    let m = moved_setup(POLICY, T0, TX, TH).await;
    for a in [
        "mod tests {\n    fn t0() {}\n    fn cy() {}\n    fn dee() {}\n}\n",
        "mod tests {\n    fn t0() {}\n    fn dee() {}\n    fn cy() {}\n}\n",
    ] {
        let y = m.resolve(a).await;
        let s = m.api.status(y).await;
        assert!(s.approved, "{s:?}");
        assert_eq!(
            rules(&s),
            [("CHANGELOG.md", "owner"), ("src/a.rs", "owner")]
        );
        for p in &s.paths {
            assert_eq!(p.carried_rule.as_deref(), Some("union"), "{p:?}");
        }
    }
    // The changelog's lines in the other order than the code's is fine
    // too: each region picks its own side, here two separate files.
    let y = m
        .api
        .commit_from(
            &[m.head],
            &[
                (
                    "src/a.rs",
                    Some("mod tests {\n    fn t0() {}\n    fn dee() {}\n    fn cy() {}\n}\n"),
                ),
                ("CHANGELOG.md", Some("# log\n- x\n- other\n")),
            ],
            Some(&m.x.to_string()),
        )
        .await;
    assert!(m.api.status(y).await.approved);
    assert_eq!(m.api.land(y, "bob").await.status, StatusCode::OK);
}

#[tokio::test]
async fn a_union_that_edits_a_base_line_stays_pending() {
    // Both sides edit `t0` and append: the conflict has a base line.
    let m = moved_setup(
        POLICY_AUTO,
        T0,
        "mod tests {\n    fn t0() { x() }\n    fn dee() {}\n}\n",
        "mod tests {\n    fn t0() { h() }\n    fn cy() {}\n}\n",
    )
    .await;
    for a in [
        "mod tests {\n    fn t0() { h() }\n    fn cy() {}\n    fn t0() { x() }\n    fn dee() {}\n}\n",
        "mod tests {\n    fn t0() { x() }\n    fn dee() {}\n    fn cy() {}\n}\n",
    ] {
        let y = m.resolve(a).await;
        let s = m.api.status(y).await;
        assert!(!s.approved);
        assert_eq!(rules(&s)[1], ("src/a.rs", "pending"));
    }
}

#[tokio::test]
async fn a_resolution_with_one_extra_line_stays_pending() {
    let m = moved_setup(POLICY_AUTO, T0, TX, TH).await;
    let y = m
        .resolve(
            "mod tests {\n    fn t0() {}\n    fn cy() {}\n    fn dee() {}\n    fn evil() {}\n}\n",
        )
        .await;
    assert!(!m.api.status(y).await.approved);
    // Nor one line dropped from either side.
    let y = m.resolve(TX).await;
    assert!(!m.api.status(y).await.approved);
    // The rebased case with one extra line.
    let m = moved_setup(POLICY_AUTO, F0, FX, FH).await;
    let y = m.resolve(&format!("{FR}fn evil() {{}}\n")).await;
    let s = m.api.status(y).await;
    assert!(!s.approved);
    assert_eq!(rules(&s)[1], ("src/a.rs", "pending"));
    assert_eq!(
        m.api.land(y, "bob").await.status,
        StatusCode::UNPROCESSABLE_ENTITY
    );
}

#[tokio::test]
async fn reviewed_lines_moved_to_another_function_stay_pending() {
    // Alice approved an `unsafe` call in `b`, after its check. The
    // resolution puts the same line, with the same hunk, into `a`.
    let m = moved_setup(
        POLICY_AUTO,
        F0,
        "fn a() {\n    one();\n}\n\nfn b() {\n    two();\n    unsafe { poke() }\n}\n",
        FH,
    )
    .await;
    let y = m
        .resolve("fn a() {\n    uno();\n    unsafe { poke() }\n}\n\nfn b() {\n    two();\n}\n")
        .await;
    let s = m.api.status(y).await;
    assert!(!s.approved);
    assert_eq!(rules(&s)[1], ("src/a.rs", "pending"));
}

#[tokio::test]
async fn a_rebased_kind_change_stays_pending() {
    // `x` makes the file executable as well; a reviewed kind change carries
    // only identically, never through a merge.
    let api = Api::with(POLICY_AUTO);
    let base = api
        .seed_main(&[("OWNERS", Some("alice\n")), ("run.sh", Some(F0))])
        .await;
    let put = |text: &'static str, kind: FileKind| {
        let api = &api;
        async move {
            let content = api
                .repo
                .blobs()
                .put(Bytes::from_static(text.as_bytes()))
                .await
                .unwrap();
            FileChange::Modify {
                content,
                kind,
                size: text.len() as u64,
            }
        }
    };
    let make = |parent: ChangesetId, change: FileChange, from: Option<ChangesetId>| {
        let api = &api;
        async move {
            api.repo
                .create_changeset(Changeset {
                    parents: vec![parent],
                    author: "t".into(),
                    timestamp: 1,
                    message: "m".into(),
                    changes: BTreeMap::from([(RepoPath::new("run.sh").unwrap(), change)]),
                    extra: from
                        .map(|f| BTreeMap::from([(EXTRA_REBASED_FROM.into(), f.to_string())]))
                        .unwrap_or_default(),
                })
                .await
                .unwrap()
        }
    };
    let x = make(base, put(FX, FileKind::Executable).await, None).await;
    api.approve(x, "alice").await;
    let head = api.commit(&[base], &[("run.sh", Some(FH))]).await;
    api.approve(head, "alice").await;
    assert_eq!(api.land(head, "bob").await.status, StatusCode::OK);
    let y = make(head, put(FR, FileKind::Executable).await, Some(x)).await;
    let s = api.status(y).await;
    assert!(!s.approved, "{s:?}");
    assert_eq!(s.origins[0].differs, [RepoPath::new("run.sh").unwrap()]);
}

#[tokio::test]
async fn a_reject_takes_back_through_a_rebased_carry() {
    let m = moved_setup(POLICY_AUTO, F0, FX, FH).await;
    let y = m.resolve(FR).await;
    assert!(m.api.status(y).await.approved);
    let r = m.api.review(y, "alice", Verdict::Reject).await;
    assert_eq!(r.json::<AddReviewResponse>().taken_back, [m.x]);
    // Another copy of the same resolution naming only `x` carries the
    // reject now, not the approval.
    let z = m
        .api
        .commit_from(
            &[m.head],
            &[
                ("src/a.rs", Some(FR)),
                ("CHANGELOG.md", Some("# log\n- other\n- x\n\n")),
            ],
            Some(&m.x.to_string()),
        )
        .await;
    let s = m.api.status(z).await;
    assert_eq!(rules(&s)[1], ("src/a.rs", "rejected"));
    assert_eq!(s.paths[1].carried_rule.as_deref(), Some("rebased"));
}

#[tokio::test]
async fn too_many_merged_paths_carry_nothing() {
    let api = Api::with(POLICY);
    let n = leep_derive::review::MAX_CARRY_MERGED_PATHS + 1;
    let names: Vec<String> = (0..n).map(|i| format!("f{i:02}.rs")).collect();
    let mut seed = vec![("OWNERS", Some("alice\n"))];
    seed.extend(names.iter().map(|p| (p.as_str(), Some(F0))));
    let base = api.seed_main(&seed).await;
    let all = |text: &'static str, count: usize| -> Vec<(&str, Option<&str>)> {
        names[..count]
            .iter()
            .map(|p| (p.as_str(), Some(text)))
            .collect()
    };
    // `x` and the head each change every file, in different functions.
    for count in [n, n - 1] {
        let x = api.commit(&[base], &all(FX, count)).await;
        api.approve(x, "alice").await;
        let head = api.commit(&[base], &all(FH, n)).await;
        let y = api
            .commit_from(&[head], &all(FR, count), Some(&x.to_string()))
            .await;
        let s = api.status(y).await;
        assert_eq!(s.approved, count < n, "{count}: {s:?}");
        if count == n {
            assert_eq!(
                s.origins[0].reason.as_deref(),
                Some("more than 32 of its paths moved under it")
            );
        } else {
            assert!(s
                .paths
                .iter()
                .all(|p| p.carried_rule.as_deref() == Some("rebased")));
        }
    }
}

#[tokio::test]
async fn an_oversized_blob_is_not_merged() {
    // Each side just past the content rules' 2 MiB: a human looks again.
    let filler = "// padding line\n".repeat(2 * 1024 * 1024 / 16 + 1);
    let big = |s: &str| -> &'static str { Box::leak(format!("{s}{filler}").into_boxed_str()) };
    let m = moved_setup(POLICY_AUTO, big(F0), big(FX), big(FH)).await;
    let y = m.resolve(big(FR)).await;
    let s = m.api.status(y).await;
    assert!(!s.approved);
    assert_eq!(rules(&s)[1], ("src/a.rs", "pending"));
}

#[tokio::test]
async fn status_names_the_origin_that_did_not_carry_and_why() {
    let m = moved_setup(POLICY_AUTO, F0, FX, FH).await;
    let y = m.resolve(&FR.replace("check", "chek")).await;
    let s = m.api.status(y).await;
    assert!(!s.approved);
    assert_eq!(
        s.origins,
        [OriginStatus {
            from: m.x,
            reviews: vec![OriginReview {
                reviewer: "alice".into(),
                verdict: Verdict::Approve,
            }],
            carried: false,
            differs: vec![RepoPath::new("src/a.rs").unwrap()],
            reason: None,
        }]
    );
    // A reviewer with a review of `y` itself is not listed for `x`; with
    // no one left, `x` is not an origin worth naming.
    m.api.review(y, "alice", Verdict::Withdrawn).await;
    assert!(m.api.status(y).await.origins.is_empty());
    // A changeset with no hint has no origins, and the field is left out.
    let raw: serde_json::Value = m
        .api
        .get(&format!("/v1/changesets/{}/review-status", m.head))
        .await
        .json();
    assert!(raw.get("origins").is_none());
}

#[tokio::test]
async fn a_strict_ref_carries_only_identical_changes() {
    // Lands on a strict ref never merge file contents, so a carry does not
    // rest on a merge there either.
    let mut repo = Repo::new(
        Arc::new(MemoryBlobStore::new()),
        Arc::new(MemoryMetaStore::new()),
    )
    .with_land_conflicts(leep_derive::LandConflicts::new(
        leep_derive::ConflictPolicy::Merge,
        ["main".to_string()],
    ));
    let policy = ReviewPolicy::parse(POLICY_AUTO).unwrap();
    let svc = Arc::new(ReviewService::new(auth().reviewers(), policy.rules));
    repo = repo.with_land_policy(Arc::new(RequireReview::new(svc.clone(), policy.refs)));
    let api = Api {
        app: leep_server::router_with_review(repo.clone(), 1 << 20, Some(auth()), None, Some(svc)),
        repo,
    };
    let m = moved_on(api, F0, FX, FH).await;
    let y = m.resolve(FR).await;
    let s = m.api.status(y).await;
    assert!(!s.approved, "{s:?}");
    assert_eq!(rules(&s)[1], ("src/a.rs", "pending"));
    assert_eq!(s.origins[0].differs, [RepoPath::new("src/a.rs").unwrap()]);
    assert_eq!(
        s.origins[0].reason.as_deref(),
        Some("src/a.rs is identical-only (strict land ref)")
    );
    assert_eq!(s.paths[1].carry.as_deref(), Some("identical"));
    assert_eq!(s.paths[1].carry_source.as_deref(), Some("strict land ref"));
}

#[tokio::test]
async fn a_reviewed_origin_that_is_gone_carries_nothing_and_fails_nothing() {
    // A review row outlives its changeset (collected, or never stored):
    // naming it is not an error, status says why, and the land goes on.
    let m = moved_setup(POLICY_AUTO, F0, FX, FH).await;
    let gone = m
        .api
        .commit(&[m.head], &[("src/a.rs", Some("gone\n"))])
        .await;
    put_at(&m.api, gone, "alice", Verdict::Reject, 100).await;
    // Collected, as the changeset sweep does; its review row stays.
    m.api
        .repo
        .meta()
        .delete_changesets(&[gone], &[])
        .await
        .unwrap();
    assert!(m
        .api
        .repo
        .get_review(gone, "alice")
        .await
        .unwrap()
        .is_some());
    let y = m
        .api
        .commit_from(
            &[m.head],
            &[
                ("src/a.rs", Some(FR)),
                ("CHANGELOG.md", Some("# log\n- other\n- x\n")),
            ],
            Some(&format!("{gone},{}", m.x)),
        )
        .await;
    let s = m.api.status(y).await;
    assert!(s.approved, "{s:?}");
    let o = s.origins.iter().find(|o| o.from == gone).unwrap();
    assert!(!o.carried);
    assert_eq!(o.reason.as_deref(), Some("it is no longer stored"));
    assert_eq!(m.api.land(y, "bob").await.status, StatusCode::OK);
}

/// ADR 0092: the root `OWNERS` says `carry identical`, so a resolution
/// equal to the server's rebase keeps alice's approval no longer, and
/// status says which file declared it.
#[tokio::test]
async fn an_identical_only_path_keeps_a_rebased_resolution_pending_and_says_why() {
    let m = moved_owned(
        Api::with(POLICY_AUTO),
        "alice\ncarry identical\n",
        F0,
        FX,
        FH,
    )
    .await;
    let y = m.resolve(FR).await;
    let s = m.api.status(y).await;
    assert!(!s.approved, "{s:?}");
    assert_eq!(rules(&s)[1], ("src/a.rs", "pending"));
    assert_eq!(s.paths[1].carry.as_deref(), Some("identical"));
    assert_eq!(s.paths[1].carry_source.as_deref(), Some("OWNERS"));
    assert_eq!(s.paths[1].owners_files, ["OWNERS"]);
    assert_eq!(s.origins[0].differs, [RepoPath::new("src/a.rs").unwrap()]);
    assert_eq!(
        s.origins[0].reason.as_deref(),
        Some("src/a.rs is identical-only (OWNERS)")
    );
    let r = m.api.land(y, "bob").await;
    assert_eq!(r.status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(
        r.error(),
        needs_review(y, "src/a.rs", "needs approval from one of [alice]")
    );
    // Taking back still reaches through the merge: a reject on `y`
    // rejects `x`, whose approval a merging ref would carry.
    let r = m.api.review(y, "alice", Verdict::Reject).await;
    assert_eq!(r.json::<AddReviewResponse>().taken_back, [m.x]);
}

/// A union resolution of two appends stays pending too, and an identical
/// change still carries under `carry identical`.
#[tokio::test]
async fn an_identical_only_path_refuses_a_union_but_carries_an_identical_change() {
    let m = moved_owned(Api::with(POLICY), "alice\ncarry identical\n", T0, TX, TH).await;
    let y = m
        .resolve("mod tests {\n    fn t0() {}\n    fn cy() {}\n    fn dee() {}\n}\n")
        .await;
    let s = m.api.status(y).await;
    assert!(!s.approved);
    assert_eq!(
        s.origins[0].reason.as_deref(),
        Some("CHANGELOG.md is identical-only (OWNERS)")
    );
    // `x2` changes only `src/a.rs` and the head did not move under it.
    let x2 = m
        .api
        .commit(&[m.head], &[("src/a.rs", Some("changed\n"))])
        .await;
    m.api.approve(x2, "alice").await;
    let other = m.api.commit(&[m.head], &[("b.rs", Some("b\n"))]).await;
    m.api.approve(other, "alice").await;
    assert_eq!(m.api.land(other, "bob").await.status, StatusCode::OK);
    let head = m.api.main_ref().await.unwrap();
    let y2 = m
        .api
        .commit_from(
            &[head],
            &[("src/a.rs", Some("changed\n"))],
            Some(&x2.to_string()),
        )
        .await;
    let s = m.api.status(y2).await;
    assert!(s.approved, "{s:?}");
    assert_eq!(s.paths[0].carried_rule.as_deref(), Some("identical"));
    assert_eq!(m.api.land(y2, "bob").await.status, StatusCode::OK);
}

/// A policy glob makes a path identical-only whatever `OWNERS` says.
#[tokio::test]
async fn a_policy_glob_outranks_carry_merged() {
    const GLOBBED: &str = r#"{ "refs": ["main"],
        "auto_approve": ["comment_only", "whitespace_only", "docs_only"],
        "docs_globs": ["**/*.md", "docs/**"],
        "identical_only_globs": ["src/**"] }"#;
    let m = moved_owned(Api::with(GLOBBED), "alice\ncarry merged\n", F0, FX, FH).await;
    let y = m.resolve(FR).await;
    let s = m.api.status(y).await;
    assert!(!s.approved, "{s:?}");
    assert_eq!(
        s.origins[0].reason.as_deref(),
        Some("src/a.rs is identical-only (policy glob src/**)")
    );
    assert_eq!(
        (s.paths[0].carry.as_deref(), s.paths[1].carry.as_deref()),
        (Some("merged"), Some("identical"))
    );
}

/// Security (ADR 0092): a change that adds `carry merged` is judged under
/// the head's `carry identical`, alone or below the resolution in a stack,
/// so it cannot loosen the rule for its own land.
#[tokio::test]
async fn a_change_adding_carry_merged_is_judged_under_the_heads_identical() {
    let m = moved_owned(
        Api::with(POLICY_AUTO),
        "alice\ncarry identical\n",
        F0,
        FX,
        FH,
    )
    .await;
    let api = &m.api;
    // In the resolution itself.
    let y = api
        .commit_from(
            &[m.head],
            &[
                ("OWNERS", Some("alice\ncarry merged\n")),
                ("src/a.rs", Some(FR)),
                ("CHANGELOG.md", Some("# log\n- other\n- x\n")),
            ],
            Some(&m.x.to_string()),
        )
        .await;
    let s = api.status(y).await;
    assert_eq!(
        rules(&s),
        [
            ("CHANGELOG.md", "auto:docs_only"),
            ("OWNERS", "pending"),
            ("src/a.rs", "pending"),
        ]
    );
    assert_eq!(
        s.origins[0].reason.as_deref(),
        Some("src/a.rs is identical-only (OWNERS)")
    );
    // Below it, approved by the owner: the land is still judged under the
    // head's rule, so the resolution above it needs its own approval.
    let loosen = api
        .commit(&[m.head], &[("OWNERS", Some("alice\ncarry merged\n"))])
        .await;
    api.approve(loosen, "alice").await;
    let y = api
        .commit_from(
            &[loosen],
            &[
                ("src/a.rs", Some(FR)),
                ("CHANGELOG.md", Some("# log\n- other\n- x\n")),
            ],
            Some(&m.x.to_string()),
        )
        .await;
    let r = api.land(y, "bob").await;
    assert_eq!(r.status, StatusCode::UNPROCESSABLE_ENTITY, "{}", r.error());
    assert_eq!(
        r.error(),
        needs_review(y, "src/a.rs", "needs approval from one of [alice]")
    );
    assert_eq!(api.main_ref().await, Some(m.head));
    // Once the looser rule is on the head, it applies.
    assert_eq!(api.land(loosen, "bob").await.status, StatusCode::OK);
    let s = api.status(y).await;
    assert!(s.approved, "{s:?}");
    assert_eq!(s.paths[1].carried_rule.as_deref(), Some("rebased"));
}

/// ADR 0092: an `OWNERS` line that starts with `carry` but is no directive
/// is neither an owner name nor ignored: a land writing it is refused with
/// 422 naming the line, even when approved, and status says the same.
#[tokio::test]
async fn a_misspelled_carry_line_is_refused_and_status_says_why() {
    let api = Api::with(POLICY);
    let base = api
        .seed_main(&[("OWNERS", Some("alice\n")), ("src/a.rs", Some("a\n"))])
        .await;
    let typo = api
        .commit(
            &[base],
            &[(
                "src/OWNERS",
                Some("bob\ncarry identically  # meant identical\nCarry identical\n"),
            )],
        )
        .await;
    api.approve(typo, "alice").await;
    let s = api.status(typo).await;
    assert!(!s.approved, "{s:?}");
    let why = s.refused.expect("refused");
    assert!(why.contains(r#"src/OWNERS: "carry identically""#), "{why}");
    let r = api.land(typo, "bob").await;
    assert_eq!(r.status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(r.error(), why);
    assert_eq!(api.main_ref().await, Some(base));
    // A directive in another case is no directive either.
    let cased = api
        .commit(&[base], &[("src/OWNERS", Some("bob\nCarry identical\n"))])
        .await;
    api.approve(cased, "alice").await;
    let r = api.land(cased, "bob").await;
    assert_eq!(r.status, StatusCode::UNPROCESSABLE_ENTITY);
    assert!(
        r.error().contains(r#"src/OWNERS: "Carry identical""#),
        "{}",
        r.error()
    );
    // Fixed, it lands, and the directory is identical-only.
    let fixed = api
        .commit(&[base], &[("src/OWNERS", Some("bob\ncarry identical\n"))])
        .await;
    api.approve(fixed, "alice").await;
    let s = api.status(fixed).await;
    assert!(s.approved && s.refused.is_none(), "{s:?}");
    assert_eq!(api.land(fixed, "bob").await.status, StatusCode::OK);
    // A changeset already on the ref (here put there past the policy) has
    // nothing left to refuse: status reports it landed, not refused.
    let head = api.main_ref().await.unwrap();
    let late = api
        .commit(&[head], &[("lib/OWNERS", Some("carry identically\n"))])
        .await;
    assert!(api
        .repo
        .meta()
        .cas_ref("main", Some(head), Some(late))
        .await
        .unwrap());
    let s = api.status(late).await;
    assert_eq!(s.on_refs, ["main"]);
    assert_eq!(s.refused, None, "{s:?}");
}

/// An oversized `OWNERS` file in a submitted changeset is a policy refusal
/// (422) like the rest of the land check, not a malformed request (400).
#[tokio::test]
async fn an_oversized_owners_file_is_refused_with_422() {
    let api = Api::with(POLICY);
    let base = api.seed_main(&[("OWNERS", Some("alice\n"))]).await;
    let big = "a\n".repeat(600 * 1024);
    let id = api
        .commit(&[base], &[("src/OWNERS", Some(big.as_str()))])
        .await;
    api.approve(id, "alice").await;
    let r = api.land(id, "bob").await;
    assert_eq!(r.status, StatusCode::UNPROCESSABLE_ENTITY, "{}", r.error());
    assert!(r.error().contains("the limit is"), "{}", r.error());
}
