//! Check results, known-good refs and suspects over HTTP (ADR 0091).

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
use leep_core::{Changeset, ChangesetId, FileChange, FileKind, RepoPath};
use leep_derive::Repo;
use leep_server::auth::hash_token;
use leep_server::reload::{ConfigHandle, ReloadSources};
use tower::ServiceExt;

fn token(name: &str) -> String {
    format!("tok-{name}")
}

fn tokens_json() -> String {
    let t = |name: &str, role: &str, extra: serde_json::Value| {
        let mut v =
            serde_json::json!({ "name": name, "role": role, "hash": hash_token(&token(name)) });
        if let serde_json::Value::Object(m) = extra {
            v.as_object_mut().unwrap().extend(m);
        }
        v
    };
    serde_json::json!({ "tokens": [
        t("alice", "write", serde_json::json!({})),
        t("agent", "write", serde_json::json!({ "review": false })),
        t("viewer", "read", serde_json::json!({})),
        // One token per check name: a promotion needs both.
        t("ci-test", "write", serde_json::json!({ "checks": ["test"] })),
        t("ci-lint", "write", serde_json::json!({ "checks": ["lint"] })),
    ]})
    .to_string()
}

const POLICY: &str = r#"{ "refs": { "main": { "required": ["test", "lint"] } } }"#;

struct Api {
    app: Router,
    repo: Repo,
    _dir: tempfile::TempDir,
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

    #[track_caller]
    fn refused(&self, status: StatusCode, code: &str) -> String {
        assert_eq!(
            self.status,
            status,
            "{}",
            String::from_utf8_lossy(&self.body)
        );
        let e: ErrorResponse = self.json();
        assert_eq!(e.code.as_deref(), Some(code), "{}", e.error);
        e.error
    }
}

impl Api {
    /// With a tokens file and the checks policy; `auth: false` is an open
    /// server with neither.
    fn new(auth: bool) -> Api {
        let dir = tempfile::tempdir().unwrap();
        let repo = Repo::new(
            Arc::new(MemoryBlobStore::new()),
            Arc::new(MemoryMetaStore::new()),
        );
        let sources = if auth {
            let tokens = dir.path().join("tokens.json");
            std::fs::write(&tokens, tokens_json()).unwrap();
            let checks = dir.path().join("checks.json");
            std::fs::write(&checks, POLICY).unwrap();
            ReloadSources {
                tokens_file: Some(tokens),
                checks_policy_file: Some(checks),
                ..Default::default()
            }
        } else {
            ReloadSources::default()
        };
        let handle = ConfigHandle::load(sources).unwrap();
        Api {
            app: leep_server::router_with_config(
                repo.clone(),
                1 << 20,
                Duration::from_secs(30),
                handle,
            ),
            repo,
            _dir: dir,
        }
    }

    async fn call(&self, method: Method, uri: &str, who: Option<&str>, body: Vec<u8>) -> Reply {
        let mut b = Request::builder()
            .method(method)
            .uri(uri)
            .header(header::HOST, "localhost")
            .header("content-type", "application/json");
        if let Some(w) = who {
            b = b.header(header::AUTHORIZATION, format!("Bearer {}", token(w)));
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
        self.call(Method::POST, uri, who, serde_json::to_vec(body).unwrap())
            .await
    }

    async fn get(&self, uri: &str) -> Reply {
        self.call(Method::GET, uri, Some("viewer"), vec![]).await
    }

    async fn commit(
        &self,
        who: &str,
        parents: &[ChangesetId],
        path: &str,
        msg: &str,
    ) -> ChangesetId {
        let content = self
            .repo
            .blobs()
            .put(Bytes::copy_from_slice(msg.as_bytes()))
            .await
            .unwrap();
        let mut changes = BTreeMap::new();
        changes.insert(
            RepoPath::new(path).unwrap(),
            FileChange::Modify {
                content,
                kind: FileKind::Regular,
                size: msg.len() as u64,
            },
        );
        let cs = Changeset {
            parents: parents.to_vec(),
            author: "A <a@example.com>".into(),
            timestamp: 1,
            message: msg.into(),
            changes,
            extra: BTreeMap::new(),
        };
        let r: CreateChangesetResponse = self.post("/v1/changesets", Some(who), &cs).await.ok();
        r.id
    }

    /// Land `n` changesets on `main` as `alice`; the heads, base first.
    async fn lands(&self, n: usize) -> Vec<ChangesetId> {
        let base = self.commit("alice", &[], "base", "base").await;
        self.land(base).await;
        let mut heads = vec![base];
        for i in 0..n {
            let c = self
                .commit("alice", &[base], &format!("d{i}/f"), &format!("land {i}"))
                .await;
            heads.push(self.land(c).await);
        }
        heads
    }

    async fn land(&self, c: ChangesetId) -> ChangesetId {
        let body = serde_json::json!({ "changeset": c, "target_ref": "main" });
        let r = self.post("/v1/land", Some("alice"), &body).await;
        assert_eq!(
            r.status,
            StatusCode::OK,
            "{}",
            String::from_utf8_lossy(&r.body)
        );
        self.repo.meta().get_ref("main").await.unwrap().unwrap()
    }

    async fn check(&self, who: &str, id: ChangesetId, check: &str, outcome: Outcome) -> Reply {
        self.post(
            &format!("/v1/changesets/{id}/checks"),
            Some(who),
            &PostCheckRequest {
                check: check.into(),
                outcome,
                url: format!("https://ci.example/{check}"),
                summary: String::new(),
            },
        )
        .await
    }

    async fn green(&self) -> Option<ChangesetId> {
        self.repo.meta().get_ref("green/main").await.unwrap()
    }
}

#[tokio::test]
async fn only_allow_listed_tokens_post_and_never_on_their_own_changes() {
    let api = Api::new(true);
    let h = api.lands(1).await;
    // A token without `checks` (every agent token) attests nothing.
    let e = api.check("alice", h[1], "test", Outcome::Pass).await;
    e.refused(StatusCode::FORBIDDEN, error_code::FORBIDDEN);
    api.check("agent", h[1], "test", Outcome::Pass)
        .await
        .refused(StatusCode::FORBIDDEN, error_code::FORBIDDEN);
    // A CI token posts only the names it is allowed.
    api.check("ci-test", h[1], "lint", Outcome::Pass)
        .await
        .refused(StatusCode::FORBIDDEN, error_code::FORBIDDEN);
    api.check("viewer", h[1], "test", Outcome::Pass)
        .await
        .refused(StatusCode::FORBIDDEN, error_code::FORBIDDEN);
    // A refused post stored nothing.
    let listed: ChecksResponse = api
        .get(&format!("/v1/changesets/{}/checks", h[1]))
        .await
        .ok();
    assert!(listed.checks.is_empty());
    // The creator of a changeset cannot attest it, even with the right name.
    let own = api.commit("ci-test", &[h[1]], "x", "made by ci").await;
    let why = api
        .check("ci-test", own, "test", Outcome::Pass)
        .await
        .refused(StatusCode::FORBIDDEN, error_code::FORBIDDEN);
    assert!(why.contains("created"), "{why}");
    // Nor the copy the server made of it when it landed on a moved head.
    let theirs = api.commit("ci-test", &[h[0]], "y", "also by ci").await;
    let copy = api.land(theirs).await;
    assert_ne!(copy, theirs, "rebased");
    api.check("ci-test", copy, "test", Outcome::Pass)
        .await
        .refused(StatusCode::FORBIDDEN, error_code::FORBIDDEN);
    // Unknown changeset, bad name.
    let missing = ChangesetId(leep_core::Hash::of_object("x", b"y"));
    assert_eq!(
        api.check("ci-test", missing, "test", Outcome::Pass)
            .await
            .status,
        StatusCode::NOT_FOUND
    );
    let ok: PostCheckResponse = api.check("ci-test", h[1], "test", Outcome::Fail).await.ok();
    assert_eq!((ok.outcome, ok.promoted.len()), (Outcome::Fail, 0));
    let listed: ChecksResponse = api
        .get(&format!("/v1/changesets/{}/checks", h[1]))
        .await
        .ok();
    assert_eq!(listed.checks.len(), 1);
    assert_eq!(listed.checks[0].by, "ci-test", "the poster is the token");
    assert_eq!(listed.checks[0].url, "https://ci.example/test");
}

#[tokio::test]
async fn without_a_tokens_file_checks_are_a_400() {
    let api = Api::new(false);
    let c = api.commit("alice", &[], "f", "one").await;
    let r = api.check("alice", c, "test", Outcome::Pass).await;
    r.refused(StatusCode::BAD_REQUEST, error_code::INVALID);
}

#[tokio::test]
async fn every_required_pass_promotes_green_forward_only() {
    let api = Api::new(true);
    let h = api.lands(4).await;
    let r: PostCheckResponse = api.check("ci-test", h[3], "test", Outcome::Pass).await.ok();
    assert!(r.promoted.is_empty(), "lint has not passed");
    let r: PostCheckResponse = api.check("ci-lint", h[3], "lint", Outcome::Pass).await.ok();
    assert_eq!(r.promoted.len(), 1);
    let p = &r.promoted[0];
    assert_eq!(
        (p.target_ref.as_str(), p.green_ref.as_str()),
        ("main", "green/main")
    );
    assert_eq!((p.from, p.to), (None, h[3]));
    assert_eq!(api.green().await, Some(h[3]));
    // An older changeset passing later never rewinds green.
    api.check("ci-lint", h[1], "lint", Outcome::Pass)
        .await
        .ok::<PostCheckResponse>();
    let r: PostCheckResponse = api.check("ci-test", h[1], "test", Outcome::Pass).await.ok();
    assert!(r.promoted.is_empty());
    assert_eq!(api.green().await, Some(h[3]));
    // The promotion is in green's log, by the token that completed it.
    let log: RefLogResponse = api.get("/v1/reflog/green/main").await.ok();
    assert_eq!(log.entries.len(), 1);
    let e = &log.entries[0];
    assert_eq!(e.operation, "promote");
    assert_eq!(e.actor.as_deref(), Some("ci-lint"));
    // Status: green, how far behind, which refs hold it.
    let s: RefChecksResponse = api.get("/v1/refs/main/checks").await.ok();
    assert_eq!((s.head, s.green, s.behind), (h[4], Some(h[3]), Some(1)));
    assert_eq!(s.required, ["lint", "test"]);
    assert_eq!(s.red, None);
    let listed: ChecksResponse = api
        .get(&format!("/v1/changesets/{}/checks", h[2]))
        .await
        .ok();
    assert!(listed.checks.is_empty());
    assert_eq!(listed.green_on, ["main"]);
    let listed: ChecksResponse = api
        .get(&format!("/v1/changesets/{}/checks", h[4]))
        .await
        .ok();
    assert!(listed.green_on.is_empty());
    // A failure makes the head red, from the failing changeset.
    api.check("ci-test", h[4], "test", Outcome::Fail)
        .await
        .ok::<PostCheckResponse>();
    let s: RefChecksResponse = api.get("/v1/refs/main/checks").await.ok();
    let red = s.red.expect("red");
    assert_eq!((red.first_failed, red.check.as_str()), (h[4], "test"));
}

#[tokio::test]
async fn an_unlanded_changeset_never_promotes() {
    let api = Api::new(true);
    let h = api.lands(1).await;
    let wip = api.commit("alice", &[h[1]], "w", "wip").await;
    // A changeset no ref holds takes no results: they would only pin it.
    let why = api
        .check("ci-test", wip, "test", Outcome::Pass)
        .await
        .refused(StatusCode::BAD_REQUEST, error_code::INVALID);
    assert!(why.contains("no ref"), "{why}");
    // On another ref (PR CI on a review ref) it does, and promotes nothing.
    api.repo
        .meta()
        .cas_ref("review/x", None, Some(wip))
        .await
        .unwrap();
    api.check("ci-test", wip, "test", Outcome::Pass)
        .await
        .ok::<PostCheckResponse>();
    let r: PostCheckResponse = api.check("ci-lint", wip, "lint", Outcome::Pass).await.ok();
    assert!(r.promoted.is_empty());
    assert_eq!(api.green().await, None);
}

#[tokio::test]
async fn green_refuses_land_cas_undo_and_delete_and_main_is_protected() {
    let api = Api::new(true);
    let h = api.lands(3).await;
    api.check("ci-test", h[2], "test", Outcome::Pass)
        .await
        .ok::<PostCheckResponse>();
    api.check("ci-lint", h[2], "lint", Outcome::Pass)
        .await
        .ok::<PostCheckResponse>();
    assert_eq!(api.green().await, Some(h[2]));
    let unprocessable = StatusCode::UNPROCESSABLE_ENTITY;
    let land = serde_json::json!({ "changeset": h[3], "target_ref": "green/main" });
    api.post("/v1/land", Some("alice"), &land)
        .await
        .refused(unprocessable, error_code::PROMOTION_ONLY);
    // Forward, back (what `leep undo` sends) and delete.
    for (new, op) in [(Some(h[3]), "cas"), (Some(h[1]), "undo"), (None, "cas")] {
        let body = CasRefRequest {
            expected: Some(h[2]),
            new,
            operation: Some(op.into()),
            message: None,
        };
        api.post("/v1/refs/green/main", Some("alice"), &body)
            .await
            .refused(unprocessable, error_code::PROMOTION_ONLY);
    }
    assert_eq!(api.green().await, Some(h[2]));
    // No write token plants a green ref outside the policy either.
    let land = serde_json::json!({ "changeset": h[3], "target_ref": "green/dev" });
    api.post("/v1/land", Some("alice"), &land)
        .await
        .refused(unprocessable, error_code::PROMOTION_ONLY);
    let plant = CasRefRequest {
        expected: None,
        new: Some(h[3]),
        operation: None,
        message: None,
    };
    api.post("/v1/refs/green/dev", Some("alice"), &plant)
        .await
        .refused(unprocessable, error_code::PROMOTION_ONLY);
    // A stale one, outside the policy, may be deleted.
    api.repo
        .meta()
        .cas_ref("green/old", None, Some(h[1]))
        .await
        .unwrap();
    let delete = CasRefRequest {
        expected: Some(h[1]),
        new: None,
        operation: None,
        message: None,
    };
    let r: CasRefResponse = api
        .post("/v1/refs/green/old", Some("alice"), &delete)
        .await
        .ok();
    assert!(r.ok);
    // Nobody records a promotion by hand, on any ref.
    let body = CasRefRequest {
        expected: None,
        new: Some(h[1]),
        operation: Some("promote".into()),
        message: None,
    };
    api.post("/v1/refs/scratch", Some("alice"), &body)
        .await
        .refused(StatusCode::BAD_REQUEST, error_code::INVALID);
    // `main` is forward-only: a write token rewinding it below green would
    // leave green ahead of it for good.
    let back = CasRefRequest {
        expected: Some(h[3]),
        new: Some(h[1]),
        operation: None,
        message: None,
    };
    api.post("/v1/refs/main", Some("alice"), &back)
        .await
        .refused(unprocessable, error_code::NOT_FORWARD);
    let delete = CasRefRequest {
        expected: Some(h[3]),
        new: None,
        operation: None,
        message: None,
    };
    let r = api.post("/v1/refs/main", Some("alice"), &delete).await;
    assert_eq!(
        r.status,
        StatusCode::BAD_REQUEST,
        "{}",
        String::from_utf8_lossy(&r.body)
    );
    assert_eq!(api.repo.meta().get_ref("main").await.unwrap(), Some(h[3]));
}

#[tokio::test]
async fn suspects_are_the_lands_between_good_and_bad() {
    let api = Api::new(true);
    let h = api.lands(6).await;
    api.check("ci-test", h[2], "test", Outcome::Pass)
        .await
        .ok::<PostCheckResponse>();
    api.check("ci-lint", h[2], "lint", Outcome::Pass)
        .await
        .ok::<PostCheckResponse>();
    let c: CulpritsResponse = api.get("/v1/refs/main/culprits").await.ok();
    let got: Vec<ChangesetId> = c.suspects.iter().map(|s| s.changeset).collect();
    assert_eq!(got, [h[6], h[5], h[4], h[3]]);
    assert_eq!((c.good, c.bad, c.truncated), (Some(h[2]), h[6], false));
    let s = &c.suspects[0];
    assert_eq!(
        (s.operation.as_str(), s.actor.as_deref()),
        ("land", Some("alice"))
    );
    assert_eq!(s.created_by.as_deref(), Some("alice"));
    assert_eq!(s.title, "land 5");
    assert_eq!(s.paths, [RepoPath::new("d5/f").unwrap()]);
    // The suspects equal the ref log entries in the range.
    let log: RefLogResponse = api.get("/v1/reflog/main?limit=100").await.ok();
    let seqs: Vec<u64> = log.entries[..4].iter().map(|e| e.seq).collect();
    assert_eq!(c.suspects.iter().map(|s| s.seq).collect::<Vec<_>>(), seqs);
    // bad and good by revision.
    let c: CulpritsResponse = api
        .get(&format!(
            "/v1/refs/main/culprits?bad={}&good={}",
            h[4], h[1]
        ))
        .await
        .ok();
    let got: Vec<ChangesetId> = c.suspects.iter().map(|s| s.changeset).collect();
    assert_eq!((c.good, got), (Some(h[1]), vec![h[4], h[3], h[2]]));
    // Paged: two at a time.
    let p: CulpritsResponse = api.get("/v1/refs/main/culprits?limit=2").await.ok();
    assert_eq!(p.suspects.len(), 2);
    let n = p.next.expect("more");
    let p: CulpritsResponse = api
        .get(&format!("/v1/refs/main/culprits?limit=2&before={n}"))
        .await
        .ok();
    let got: Vec<ChangesetId> = p.suspects.iter().map(|s| s.changeset).collect();
    assert_eq!((got, p.next), (vec![h[4], h[3]], None));
    let r = api
        .get(&format!("/v1/refs/main/culprits?bad={}", h[1]))
        .await;
    r.refused(StatusCode::BAD_REQUEST, error_code::INVALID);
    assert_eq!(
        api.get("/v1/refs/nope/culprits").await.status,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        api.get("/v1/refs/nope/checks").await.status,
        StatusCode::NOT_FOUND
    );
}

#[tokio::test]
async fn a_ref_named_like_a_suffix_is_still_itself() {
    let api = Api::new(true);
    let h = api.lands(1).await;
    api.repo
        .meta()
        .cas_ref("main/checks", None, Some(h[0]))
        .await
        .unwrap();
    let r: RefResponse = api.get("/v1/refs/main/checks").await.ok();
    assert_eq!((r.name.as_str(), r.target), ("main/checks", Some(h[0])));
}

#[test]
fn wire_shapes_are_additive() {
    // Clients may leave out the optional texts.
    let req: PostCheckRequest =
        serde_json::from_str(r#"{"check":"test","outcome":"pass"}"#).unwrap();
    assert_eq!((req.url.as_str(), req.summary.as_str()), ("", ""));
    assert!(
        serde_json::from_str::<PostCheckRequest>(r#"{"check":"t","outcome":"maybe"}"#).is_err()
    );
    // A response without the newer optional fields still reads.
    let r: PostCheckResponse = serde_json::from_str(r#"{"check":"t","outcome":"fail"}"#).unwrap();
    assert!(r.promoted.is_empty());
    let id = ChangesetId(leep_core::Hash::of_object("x", b"y"));
    let r: RefChecksResponse = serde_json::from_value(serde_json::json!({
        "name": "main", "head": id, "green_ref": "green/main", "green": null
    }))
    .unwrap();
    assert!(r.required.is_empty() && r.behind.is_none() && r.red.is_none());
    let r: ChecksResponse = serde_json::from_str(r#"{"checks":[]}"#).unwrap();
    assert!(r.green_on.is_empty());
    let r: CulpritsResponse = serde_json::from_value(serde_json::json!({
        "name": "main", "good": null, "bad": id, "suspects": [{
            "changeset": id, "seq": 1, "operation": "land", "time": 0,
            "author": "a", "title": "t"
        }], "truncated": false
    }))
    .unwrap();
    assert!(r.suspects[0].paths.is_empty() && r.suspects[0].actor.is_none());
    assert_eq!(r.next, None);
    // Field names on the wire.
    let v = serde_json::to_value(Promotion {
        target_ref: "main".into(),
        green_ref: "green/main".into(),
        from: None,
        to: id,
    })
    .unwrap();
    assert_eq!(v["ref"], "main");
    assert_eq!(serde_json::to_value(Outcome::Pass).unwrap(), "pass");
}

#[tokio::test]
async fn a_reload_that_drops_a_required_check_promotes_at_once() {
    let api = Api::new(true);
    let h = api.lands(3).await;
    for id in &h[1..] {
        api.check("ci-test", *id, "test", Outcome::Pass)
            .await
            .ok::<PostCheckResponse>();
    }
    // lint never passes (flaky), so green never moves.
    api.check("ci-lint", h[3], "lint", Outcome::Fail)
        .await
        .ok::<PostCheckResponse>();
    assert_eq!(api.green().await, None);
    let dir = tempfile::tempdir().unwrap();
    let tokens = dir.path().join("tokens.json");
    std::fs::write(&tokens, tokens_json()).unwrap();
    let policy = dir.path().join("checks.json");
    std::fs::write(&policy, POLICY).unwrap();
    let handle = ConfigHandle::load(ReloadSources {
        tokens_file: Some(tokens),
        checks_policy_file: Some(policy.clone()),
        ..Default::default()
    })
    .unwrap();
    // At startup nothing has every required pass.
    leep_server::checks::catch_up(&api.repo, &handle.current()).await;
    assert_eq!(api.green().await, None);
    std::fs::write(
        &policy,
        r#"{ "refs": { "main": { "required": ["test"] } } }"#,
    )
    .unwrap();
    let report = handle.reload().unwrap();
    assert!(report.checks_policy_changed);
    leep_server::checks::catch_up(&api.repo, &handle.current()).await;
    assert_eq!(api.green().await, Some(h[3]));
    let log = api
        .repo
        .meta()
        .ref_log(Some("green/main"), None, 10)
        .await
        .unwrap();
    assert_eq!(log[0].operation, "promote");
}

#[tokio::test]
async fn a_merge_made_by_a_ci_token_is_its_own_and_cannot_be_attested() {
    // S-25: the creator rule must hold for changesets made by POST /v1/merge
    // too, or a CI token that commits makes X, merges it and attests that.
    let api = Api::new(true);
    let h = api.lands(1).await;
    let x = api.commit("ci-test", &[h[0]], "x", "by ci").await;
    let req = MergeRequest {
        parents: [h[1], x],
        message: "merge".into(),
        authorship: Authorship {
            author: "ci".into(),
            timestamp: Some(1),
        },
        extra: BTreeMap::new(),
    };
    let m = match api.post("/v1/merge", Some("ci-test"), &req).await.ok() {
        MergeResponse::Merged { id } => id,
        other => panic!("{other:?}"),
    };
    let info: ChangesetInfo = api.get(&format!("/v1/changesets/{m}")).await.ok();
    assert_eq!(
        info.created_by.map(|c| c.principal).as_deref(),
        Some("ci-test")
    );
    let landed = api.land(m).await;
    assert_eq!(landed, m, "a fast-forward lands the merge itself");
    api.check("ci-test", landed, "test", Outcome::Pass)
        .await
        .refused(StatusCode::FORBIDDEN, error_code::FORBIDDEN);
    // Another token's merge of the same parents is not ci-test's to claim:
    // the first creator stays.
    let again = api
        .post("/v1/merge", Some("alice"), &req)
        .await
        .ok::<MergeResponse>();
    assert_eq!(again, MergeResponse::Merged { id: m });
    let info: ChangesetInfo = api.get(&format!("/v1/changesets/{m}")).await.ok();
    assert_eq!(
        info.created_by.map(|c| c.principal).as_deref(),
        Some("ci-test")
    );
}
