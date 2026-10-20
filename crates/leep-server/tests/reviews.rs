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
        let mut b = Request::builder().method(method).uri(uri);
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
                extra: BTreeMap::new(),
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
    format!("invalid input: changeset {id} {NEEDS_REVIEW_MARKER}: path {path} {why}")
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
    assert_eq!(r.status, StatusCode::OK);
    assert_eq!(r.json::<AddReviewResponse>().reviewer, "mallory");
    let list = api.reviews(c).await;
    assert_eq!(list.reviews.len(), 1);
    assert_eq!(list.reviews[0].reviewer, "mallory");
    assert!(api.repo.get_review(c, "alice").await.unwrap().is_none());
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
    assert_eq!(r.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        r.error(),
        needs_review(c, "a/sub/OWNERS", "needs approval from one of [alice]")
    );
    assert_eq!(api.main_ref().await, Some(head));

    let s = api.status(c).await;
    assert!(!s.approved);
    assert_eq!(s.paths[0].owners, ["alice"]);
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
    assert_eq!(api.land(d, "alice").await.status, StatusCode::BAD_REQUEST);
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
    assert_eq!(api.land(e, "mallory").await.status, StatusCode::BAD_REQUEST);
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
        StatusCode::BAD_REQUEST
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

    // The rebased copy has no review of its own; the listing says whose it
    // borrows. The land policy never accepts a copy on that basis.
    let list = api.reviews(landed).await;
    assert_eq!(list.via, Some(s2));
    assert_eq!(list.reviews.len(), 1);
    assert_eq!(list.reviews[0].reviewer, "alice");
    assert_eq!(api.reviews(s2).await.via, None);
    let copy = api.status(landed).await;
    assert!(!copy.approved);
    assert_eq!(copy.paths[0].rule, "pending");
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
    assert_eq!(r.status, StatusCode::BAD_REQUEST);
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
        StatusCode::BAD_REQUEST
    );
    assert_eq!(api.main_ref().await, Some(fix));
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
        assert_eq!(r.status, StatusCode::BAD_REQUEST);
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
    assert_eq!(r.status, StatusCode::BAD_REQUEST);
    assert!(r.error().contains(NEEDS_REVIEW_MARKER), "{}", r.error());
    assert_eq!(api.main_ref().await, None);
    // Unprotected refs are unaffected.
    assert_eq!(
        api.post("/v1/refs/scratch", "bob", &cas(c)).await.status,
        StatusCode::OK
    );
    assert_eq!(
        api.land(c, "bob").await.status,
        StatusCode::BAD_REQUEST,
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
    assert_eq!(
        r.error(),
        format!("invalid input: changeset {c} {NOT_SIGNED_MARKER}")
    );

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
    assert_eq!(
        r.error(),
        format!("invalid input: changeset {d} {NOT_SIGNED_MARKER}")
    );
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
