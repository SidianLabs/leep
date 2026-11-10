//! `POST /v1/context` over HTTP (ADR 0097): a read token gets a pack, the
//! caller's session comes from `Leep-Session`, a pack marks no notice
//! said, and bad requests are refused with the usual codes.

use std::collections::BTreeMap;
use std::sync::Arc;

use axum::body::Body;
use axum::http::{header, Method, Request, StatusCode};
use axum::Router;
use bytes::Bytes;
use http_body_util::BodyExt;
use leep_core::api::*;
use leep_core::changeset::{EXTRA_AGENT_MODEL, EXTRA_AGENT_SESSION};
use leep_core::memory::{MemoryBlobStore, MemoryMetaStore};
use leep_core::{Changeset, ChangesetId, FileChange, FileKind, RepoPath};
use leep_derive::Repo;
use leep_server::auth::{hash_token, Auth};
use serde_json::json;
use tower::ServiceExt;

fn auth() -> Auth {
    let mut tokens: Vec<_> = ["alice", "bob", "carol"]
        .iter()
        .map(|n| json!({ "name": n, "role": "write", "hash": hash_token(&format!("tok-{n}")) }))
        .collect();
    tokens.push(json!({ "name": "reader", "role": "read", "hash": hash_token("tok-reader") }));
    Auth::parse(&json!({ "tokens": tokens }).to_string()).unwrap()
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
    #[track_caller]
    fn ok<T: serde::de::DeserializeOwned>(&self) -> T {
        assert_eq!(
            self.status,
            StatusCode::OK,
            "{}",
            String::from_utf8_lossy(&self.body)
        );
        serde_json::from_slice(&self.body).unwrap()
    }

    fn code(&self) -> String {
        let v: serde_json::Value = serde_json::from_slice(&self.body).unwrap();
        v["code"].as_str().unwrap_or_default().to_string()
    }
}

impl Api {
    fn new() -> Api {
        let repo = Repo::new(
            Arc::new(MemoryBlobStore::new()),
            Arc::new(MemoryMetaStore::new()),
        );
        Api {
            app: leep_server::router_with_auth(repo.clone(), 1 << 20, Some(auth())),
            repo,
        }
    }

    async fn call(
        &self,
        method: Method,
        uri: &str,
        who: Option<&str>,
        headers: &[(&str, &str)],
        body: Vec<u8>,
    ) -> Reply {
        let mut b = Request::builder()
            .method(method)
            .uri(uri)
            .header(header::HOST, "localhost")
            .header("content-type", "application/json");
        if let Some(who) = who {
            b = b.header(header::AUTHORIZATION, format!("Bearer tok-{who}"));
        }
        for (k, v) in headers {
            b = b.header(*k, *v);
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

    /// Create a changeset of `paths` as `who` under session `key`, on
    /// `parent`.
    async fn commit(
        &self,
        who: &str,
        key: &str,
        parent: Option<ChangesetId>,
        paths: &[&str],
    ) -> ChangesetId {
        let body = format!("{who}: fn parse_token() {{}}\n");
        let content = self
            .repo
            .put_blob(Bytes::from(body.clone().into_bytes()))
            .await
            .unwrap();
        let changes: BTreeMap<RepoPath, FileChange> = paths
            .iter()
            .map(|p| {
                (
                    RepoPath::new(*p).unwrap(),
                    FileChange::Modify {
                        content,
                        kind: FileKind::Regular,
                        size: body.len() as u64,
                    },
                )
            })
            .collect();
        let cs = Changeset {
            parents: parent.into_iter().collect(),
            author: who.into(),
            timestamp: 1_700_000_000,
            message: format!("{who} edits {}", paths.join(", ")),
            changes,
            extra: [
                (EXTRA_AGENT_SESSION.to_string(), key.to_string()),
                (EXTRA_AGENT_MODEL.to_string(), format!("{who}-model")),
            ]
            .into(),
        };
        let r: CreateChangesetResponse = self
            .call(
                Method::POST,
                "/v1/changesets",
                Some(who),
                &[(SESSION_HEADER, key)],
                serde_json::to_vec(&cs).unwrap(),
            )
            .await
            .ok();
        r.id
    }

    async fn land(&self, who: &str, id: ChangesetId) -> ChangesetId {
        let req = LandRequest {
            changeset: id,
            target_ref: "main".into(),
            create: None,
            land_id: None,
        };
        let a: LandAnswer = self
            .call(
                Method::POST,
                "/v1/land",
                Some(who),
                &[],
                serde_json::to_vec(&req).unwrap(),
            )
            .await
            .ok();
        match a.answer {
            LandResponse::Landed { landed, .. } => landed,
            other => panic!("{other:?}"),
        }
    }

    async fn context(&self, who: &str, headers: &[(&str, &str)], body: serde_json::Value) -> Reply {
        self.call(
            Method::POST,
            "/v1/context",
            Some(who),
            headers,
            body.to_string().into_bytes(),
        )
        .await
    }
}

/// alice landed the file, bob is editing it now: a reader asking about it
/// sees both, and the owners.
async fn busy() -> Api {
    let api = Api::new();
    let seed = api
        .commit("alice", "a-1", None, &["src/auth/token.rs", "OWNERS"])
        .await;
    let main = api.land("alice", seed).await;
    api.commit("bob", "b-1", Some(main), &["src/auth/token.rs"])
        .await;
    api
}

#[tokio::test]
async fn a_read_token_gets_live_work_and_history() {
    let api = busy().await;
    let p: ContextPack = api
        .context(
            "reader",
            &[],
            json!({ "task": "Speed up parse_token", "paths": ["src/auth/token.rs"] }),
        )
        .await
        .ok();
    assert_eq!(p.target_ref, "main");
    assert_eq!(
        p.files[0].untrusted.path.as_deref(),
        Some("src/auth/token.rs")
    );
    assert_eq!(p.files[0].why, ["seed", "search"]);
    let w = &p.in_flight[0];
    assert_eq!(
        (w.notice.level.as_str(), w.notice.principal.as_deref()),
        ("same_file", Some("bob"))
    );
    let h = &p.history[0];
    assert_eq!(h.created_by.as_deref(), Some("alice"));
    assert_eq!(h.untrusted.agent_model.as_deref(), Some("alice-model"));
    // The OWNERS file names whoever wrote it: "alice: fn ..." is one line.
    assert_eq!(p.owners[0].untrusted.owners, ["alice: fn parse_token() {}"]);
    assert!(p.ref_health.is_some() && p.omitted.is_empty());
}

/// `Leep-Session` names the caller's session: its paths are probed, it
/// is never its own overlap, and the pack marks nothing as said.
#[tokio::test]
async fn the_callers_session_comes_from_the_header_and_nothing_is_said() {
    let api = busy().await;
    let main = api.repo.meta().get_ref("main").await.unwrap();
    api.commit("carol", "c-1", main, &["src/auth/token.rs"])
        .await;
    let ask = || {
        api.context(
            "bob",
            &[(SESSION_HEADER, "b-1")],
            json!({ "task": "parse_token speedup" }),
        )
    };
    let p: ContextPack = ask().await.ok();
    assert!(p.session.is_some());
    let who: Vec<_> = p
        .in_flight
        .iter()
        .filter_map(|w| w.notice.principal.as_deref())
        .collect();
    assert_eq!(who, ["carol"], "found by bob's own touched path");
    let again: ContextPack = ask().await.ok();
    assert_eq!(again, p);
    // Notices are still bob's to hear, once.
    let n: NoticesResponse = api
        .call(
            Method::GET,
            "/v1/notices",
            Some("bob"),
            &[(SESSION_HEADER, "b-1")],
            vec![],
        )
        .await
        .ok();
    assert_eq!(n.notices.len(), 1, "{:?}", n.notices);
    assert_eq!(n.notices[0].principal.as_deref(), Some("carol"));
}

/// `/v1` only grows: the smallest body works, a field from a later client
/// is ignored, and the answer has the documented names.
#[tokio::test]
async fn the_route_is_additive() {
    let api = busy().await;
    let r = api
        .context(
            "reader",
            &[],
            json!({ "task": "x", "from_the_future": true }),
        )
        .await;
    let v: serde_json::Value = r.ok();
    for k in [
        "ref",
        "head",
        "budget_bytes",
        "bytes",
        "files",
        "owners",
        "history",
        "in_flight",
        "duplicates",
        "ref_health",
    ] {
        assert!(v.get(k).is_some(), "{k} in {v}");
    }
    assert_eq!(v["budget_bytes"], CONTEXT_DEFAULT_BUDGET);
    // Older routes answer as before beside it.
    let s: SessionsResponse = api
        .call(Method::GET, "/v1/sessions", Some("reader"), &[], vec![])
        .await
        .ok();
    assert!(s.sessions.iter().any(|x| x.principal == "bob"));
}

#[tokio::test]
async fn bad_requests_are_refused_with_their_codes() {
    let api = busy().await;
    let r = api
        .context("reader", &[], json!({ "task": "x", "ref": "nope" }))
        .await;
    assert_eq!(
        (r.status, r.code().as_str()),
        (StatusCode::NOT_FOUND, "not_found")
    );
    let r = api.context("reader", &[], json!({ "paths": ["a"] })).await;
    assert_eq!(
        (r.status, r.code().as_str()),
        (StatusCode::BAD_REQUEST, "invalid")
    );
    let many: Vec<String> = (0..65).map(|i| format!("p{i}")).collect();
    let r = api
        .context("reader", &[], json!({ "task": "x", "paths": many }))
        .await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);
    let r = api.context("reader", &[], json!({ "task": 7 })).await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);
    let r = api
        .call(
            Method::POST,
            "/v1/context",
            None,
            &[],
            b"{\"task\":\"x\"}".to_vec(),
        )
        .await;
    assert_eq!(r.status, StatusCode::UNAUTHORIZED);
}
