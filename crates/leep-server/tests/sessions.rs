//! Sessions over HTTP (ADR 0095): declare, list who works on or near a
//! path, end, heartbeats, and sessions inferred from changesets and ended
//! by lands.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

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
use tower::ServiceExt;

const VIEWER: &str = "tok-viewer";

fn token(name: &str) -> String {
    format!("tok-{name}")
}

fn auth() -> Auth {
    let mut tokens: Vec<_> = ["alice", "bob"]
        .iter()
        .map(|n| serde_json::json!({ "name": n, "role": "write", "hash": hash_token(&token(n)) }))
        .collect();
    tokens
        .push(serde_json::json!({ "name": "viewer", "role": "read", "hash": hash_token(VIEWER) }));
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

    fn code(&self) -> Option<String> {
        self.json::<ErrorResponse>().code
    }
}

impl Api {
    fn new(with_auth: bool) -> Api {
        Self::over(
            Repo::new(
                Arc::new(MemoryBlobStore::new()),
                Arc::new(MemoryMetaStore::new()),
            ),
            with_auth,
        )
    }

    fn over(repo: Repo, with_auth: bool) -> Api {
        Api {
            app: leep_server::router_with_auth(repo.clone(), 1 << 20, with_auth.then(auth)),
            repo,
        }
    }

    async fn call(
        &self,
        method: Method,
        uri: &str,
        tok: Option<&str>,
        session: Option<&str>,
        body: Vec<u8>,
    ) -> Reply {
        let mut b = Request::builder()
            .method(method)
            .uri(uri)
            .header(header::HOST, "localhost")
            .header("content-type", "application/json");
        if let Some(t) = tok {
            b = b.header(header::AUTHORIZATION, format!("Bearer {t}"));
        }
        if let Some(s) = session {
            b = b.header(SESSION_HEADER, s);
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

    async fn declare(&self, who: &str, req: serde_json::Value) -> Reply {
        let t = token(who);
        self.call(
            Method::POST,
            "/v1/sessions",
            Some(&t),
            None,
            req.to_string().into(),
        )
        .await
    }

    async fn who(&self, query: &str) -> SessionsResponse {
        self.call(
            Method::GET,
            &format!("/v1/sessions{query}"),
            Some(VIEWER),
            None,
            vec![],
        )
        .await
        .ok()
    }

    /// Store a changeset changing `paths` as `who`, carrying `session`.
    async fn commit(
        &self,
        who: &str,
        session: &str,
        parents: &[ChangesetId],
        paths: &[&str],
    ) -> ChangesetId {
        let content = self
            .repo
            .put_blob(Bytes::from_static(b"x\n"))
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
                        size: 2,
                    },
                )
            })
            .collect();
        let cs = Changeset {
            parents: parents.to_vec(),
            author: who.into(),
            timestamp: 1_700_000_000,
            message: format!("{who} changes {}\n\nmore", paths.join(", ")),
            changes,
            extra: [
                (EXTRA_AGENT_SESSION.to_string(), session.to_string()),
                (EXTRA_AGENT_MODEL.to_string(), "model-x".to_string()),
            ]
            .into(),
        };
        let t = token(who);
        let r: CreateChangesetResponse = self
            .call(
                Method::POST,
                "/v1/changesets",
                Some(&t),
                None,
                serde_json::to_vec(&cs).unwrap(),
            )
            .await
            .ok();
        r.id
    }

    /// Wait for the hooks the server runs after an answer.
    async fn settle(&self, cond: impl AsyncFn() -> bool) {
        for _ in 0..200 {
            if cond().await {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("condition never held");
    }
}

#[tokio::test]
async fn declare_list_and_end_sessions() {
    let api = Api::new(true);
    let r = api
        .declare(
            "alice",
            serde_json::json!({
                "session": "a-1",
                "title": "Refactor auth",
                "approach": "split the module",
                "task_key": "L-12",
                "scope": ["src/auth/", "./docs"],
                "claims": ["src/auth/token.rs"],
            }),
        )
        .await;
    let made: DeclareSessionResponse = r.ok();
    assert!(made.created);
    let a = made.session;
    assert_eq!((a.principal.as_str(), a.session.as_str()), ("alice", "a-1"));
    assert_eq!(a.untrusted.title, "Refactor auth");
    assert_eq!(a.scope, ["docs", "src/auth"]);
    assert_eq!(
        (a.origin.as_str(), a.state.as_str()),
        ("declared", "active")
    );
    assert!(a.expires.is_some());
    // Declaring again updates the same session.
    let again: DeclareSessionResponse = api
        .declare(
            "alice",
            serde_json::json!({ "session": "a-1", "approach": "rewrite it" }),
        )
        .await
        .ok();
    assert!(!again.created);
    assert_eq!(again.session.id, a.id);
    assert_eq!(again.session.untrusted.title, "Refactor auth");
    assert_eq!(again.session.untrusted.approach, "rewrite it");
    let b: DeclareSessionResponse = api
        .declare(
            "bob",
            serde_json::json!({ "session": "b-1", "task_key": "L-12", "scope": ["src/auth/token.rs"] }),
        )
        .await
        .ok();

    // Who works on or near a path: the path, a directory above, or below.
    let ids = |r: SessionsResponse| r.sessions.iter().map(|s| s.id).collect::<Vec<_>>();
    let both = vec![b.session.id, a.id];
    assert_eq!(ids(api.who("?path=src/auth/token.rs").await), both);
    assert_eq!(ids(api.who("?path=src").await), both);
    assert_eq!(ids(api.who("?path=docs/x.md").await), [a.id]);
    assert!(ids(api.who("?path=web").await).is_empty());
    assert_eq!(ids(api.who("?task=L-12").await), both);
    assert_eq!(ids(api.who("?principal=bob").await), [b.session.id]);
    assert_eq!(ids(api.who("?session=a-1").await), [a.id]);
    let page = api.who("?limit=1").await;
    assert_eq!(
        (ids(page.clone()), page.next),
        (vec![b.session.id], Some(b.session.id))
    );
    assert_eq!(
        ids(api.who(&format!("?limit=1&before={}", b.session.id)).await),
        [a.id]
    );
    let t = token("bob");
    let mine: SessionsResponse = api
        .call(
            Method::GET,
            "/v1/sessions?mine=true",
            Some(&t),
            None,
            vec![],
        )
        .await
        .ok();
    assert_eq!(ids(mine), [b.session.id]);
    let one: SessionEntry = api
        .call(
            Method::GET,
            &format!("/v1/sessions/{}", a.id),
            Some(VIEWER),
            None,
            vec![],
        )
        .await
        .ok();
    assert_eq!(one.claims, ["src/auth/token.rs"]);

    // Bad input is 400; an unknown session 404.
    for (q, what) in [
        ("?state=open", "state"),
        ("?limit=0", "limit"),
        ("?all=yes", "all"),
        ("?path=/abs", "path"),
    ] {
        let r = api
            .call(
                Method::GET,
                &format!("/v1/sessions{q}"),
                Some(VIEWER),
                None,
                vec![],
            )
            .await;
        assert_eq!(r.status, StatusCode::BAD_REQUEST, "{what}");
    }
    let r = api
        .call(Method::GET, "/v1/sessions/999", Some(VIEWER), None, vec![])
        .await;
    assert_eq!(r.status, StatusCode::NOT_FOUND);
    let r = api
        .declare("alice", serde_json::json!({ "session": "has space" }))
        .await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);
    let r = api
        .declare(
            "alice",
            serde_json::json!({ "session": "a-2", "title": "two\nlines" }),
        )
        .await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);
    let r = api
        .declare(
            "alice",
            serde_json::json!({ "session": "a-2", "base": ChangesetId(leep_core::Hash::of_blob(b"nope")) }),
        )
        .await;
    assert_eq!(r.status, StatusCode::NOT_FOUND);

    // A read token lists but cannot declare or end.
    let r = api
        .call(
            Method::POST,
            "/v1/sessions",
            Some(VIEWER),
            None,
            br#"{"session":"v"}"#.to_vec(),
        )
        .await;
    assert_eq!(r.status, StatusCode::FORBIDDEN);
    let r = api
        .call(
            Method::DELETE,
            &format!("/v1/sessions/{}", a.id),
            Some(VIEWER),
            None,
            vec![],
        )
        .await;
    assert_eq!(r.status, StatusCode::FORBIDDEN);

    // Only its principal ends a session; ending is answered as it is after.
    let t = token("bob");
    let r = api
        .call(
            Method::DELETE,
            &format!("/v1/sessions/{}", a.id),
            Some(&t),
            None,
            vec![],
        )
        .await;
    assert_eq!(r.status, StatusCode::FORBIDDEN);
    assert_eq!(r.code().as_deref(), Some(error_code::FORBIDDEN));
    let t = token("alice");
    for _ in 0..2 {
        let ended: SessionEntry = api
            .call(
                Method::DELETE,
                &format!("/v1/sessions/{}", a.id),
                Some(&t),
                None,
                vec![],
            )
            .await
            .ok();
        assert_eq!(ended.state, "abandoned");
        assert!(ended.ended.is_some() && ended.expires.is_none());
    }
    assert_eq!(ids(api.who("?path=src").await), [b.session.id]);
    assert_eq!(ids(api.who("?path=src&all=true").await), both);
    assert_eq!(ids(api.who("?state=abandoned").await), [a.id]);
}

#[tokio::test]
async fn changesets_and_lands_drive_inferred_sessions() {
    let api = Api::new(true);
    let first = api.commit("alice", "a-1", &[], &["src/a.rs"]).await;
    api.settle(async || !api.who("?path=src").await.sessions.is_empty())
        .await;
    let s = api.who("?path=src/a.rs").await.sessions.remove(0);
    assert_eq!((s.principal.as_str(), s.session.as_str()), ("alice", "a-1"));
    assert_eq!(s.origin, "inferred");
    assert_eq!(s.untrusted.title, "alice changes src/a.rs");
    assert_eq!(s.head, Some(first));
    assert_eq!(s.agent_model.as_deref(), Some("model-x"));
    // Bob's work elsewhere is his own session.
    let bobs = api.commit("bob", "b-1", &[], &["web/b.ts"]).await;
    api.settle(async || api.who("?principal=bob").await.sessions.len() == 1)
        .await;
    // A second commit of alice adds paths and moves the head.
    let second = api.commit("alice", "a-1", &[first], &["src/c.rs"]).await;
    api.settle(async || api.who("?path=src/c.rs").await.sessions.len() == 1)
        .await;
    let s = api.who("?session=a-1").await.sessions.remove(0);
    assert_eq!(s.touched, ["src/a.rs", "src/c.rs"]);
    assert_eq!(s.head, Some(second));

    // Landing the first commit does not end the session: its head moved on.
    let t = token("alice");
    let land = |cs: ChangesetId| {
        serde_json::to_vec(&LandRequest {
            changeset: cs,
            target_ref: "main".into(),
            create: Some(true),
            land_id: None,
        })
        .unwrap()
    };
    let r: LandResponse = api
        .call(Method::POST, "/v1/land", Some(&t), None, land(first))
        .await
        .ok();
    assert!(matches!(r, LandResponse::Landed { .. }));
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(api.who("?session=a-1").await.sessions.len(), 1);
    // Landing its head ends it as landed.
    let r: LandResponse = api
        .call(Method::POST, "/v1/land", Some(&t), None, land(second))
        .await
        .ok();
    let LandResponse::Landed { landed, .. } = r else {
        panic!("{r:?}")
    };
    api.settle(async || api.who("?session=a-1").await.sessions.is_empty())
        .await;
    let done = api
        .who("?session=a-1&state=landed")
        .await
        .sessions
        .remove(0);
    assert_eq!(done.landed_as, Some(landed));
    // Bob's land of his own work ends only his.
    let t = token("bob");
    let r: LandResponse = api
        .call(Method::POST, "/v1/land", Some(&t), None, land(bobs))
        .await
        .ok();
    assert!(matches!(r, LandResponse::Landed { .. }), "{r:?}");
    api.settle(async || api.who("").await.sessions.is_empty())
        .await;
}

#[tokio::test]
async fn a_session_header_is_a_heartbeat() {
    let api = Api::new(true);
    let made: DeclareSessionResponse = api
        .declare("alice", serde_json::json!({ "session": "a-1" }))
        .await
        .ok();
    let t = token("alice");
    // Any request with the header, a read included, and a bad value is
    // ignored rather than refused.
    let r = api
        .call(Method::GET, "/v1/refs", Some(&t), Some("a-1"), vec![])
        .await;
    assert_eq!(r.status, StatusCode::OK);
    let r = api
        .call(Method::GET, "/v1/refs", Some(&t), Some("not ok"), vec![])
        .await;
    assert_eq!(r.status, StatusCode::OK);
    // A beat of someone else's key changes nothing.
    let r = api
        .call(Method::GET, "/v1/refs", Some(VIEWER), Some("a-1"), vec![])
        .await;
    assert_eq!(r.status, StatusCode::OK);
    assert_eq!(api.repo.flush_beats().await.unwrap(), 1);
    let s: SessionEntry = api
        .call(
            Method::GET,
            &format!("/v1/sessions/{}", made.session.id),
            Some(VIEWER),
            None,
            vec![],
        )
        .await
        .ok();
    assert!(s.last_seen >= made.session.last_seen);
}

#[tokio::test]
async fn without_authentication_sessions_are_anonymous() {
    let api = Api::new(false);
    let r: DeclareSessionResponse = api
        .call(
            Method::POST,
            "/v1/sessions",
            None,
            None,
            br#"{"session":"k","scope":["src"]}"#.to_vec(),
        )
        .await
        .ok();
    assert_eq!(r.session.principal, "anonymous");
    let mine: SessionsResponse = api
        .call(Method::GET, "/v1/sessions?mine=true", None, None, vec![])
        .await
        .ok();
    assert_eq!(mine.sessions.len(), 1);
}

#[tokio::test]
async fn a_server_without_sessions_answers_404_and_lands_as_ever() {
    let repo = Repo::new(
        Arc::new(MemoryBlobStore::new()),
        Arc::new(MemoryMetaStore::new()),
    )
    .with_coord(None);
    let api = Api::over(repo, true);
    let r = api
        .declare("alice", serde_json::json!({ "session": "k" }))
        .await;
    assert_eq!(r.status, StatusCode::NOT_FOUND);
    let r = api
        .call(Method::GET, "/v1/sessions", Some(VIEWER), None, vec![])
        .await;
    assert_eq!(r.status, StatusCode::NOT_FOUND);
    let cs = api.commit("alice", "a-1", &[], &["src/a.rs"]).await;
    let t = token("alice");
    let body = serde_json::to_vec(&LandRequest {
        changeset: cs,
        target_ref: "main".into(),
        create: Some(true),
        land_id: None,
    })
    .unwrap();
    let r: LandResponse = api
        .call(Method::POST, "/v1/land", Some(&t), Some("a-1"), body)
        .await
        .ok();
    assert!(matches!(r, LandResponse::Landed { .. }));
}

#[tokio::test]
async fn too_many_sessions_has_its_own_code() {
    use leep_core::coord::{SessionOrigin, SessionUpdate, MAX_ACTIVE_PER_PRINCIPAL};
    let api = Api::new(true);
    let coord = api.repo.coord().unwrap().clone();
    for n in 0..MAX_ACTIVE_PER_PRINCIPAL {
        let u = SessionUpdate::new("alice", &format!("k{n}"), SessionOrigin::Declared, 1);
        coord.upsert_session(&u).await.unwrap();
    }
    let r = api
        .declare("alice", serde_json::json!({ "session": "one-more" }))
        .await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);
    assert_eq!(r.code().as_deref(), Some(error_code::TOO_MANY_SESSIONS));
    // Updating one that exists is fine, and so is another principal.
    let r = api
        .declare("alice", serde_json::json!({ "session": "k1" }))
        .await;
    assert_eq!(r.status, StatusCode::OK);
    let r = api
        .declare("bob", serde_json::json!({ "session": "k1" }))
        .await;
    assert_eq!(r.status, StatusCode::OK);
}

/// A coordination store that never answers.
struct Hanging;

#[async_trait::async_trait]
impl leep_core::coord::CoordStore for Hanging {
    async fn upsert_session(
        &self,
        _: &leep_core::coord::SessionUpdate,
    ) -> leep_core::Result<(leep_core::coord::Session, bool)> {
        std::future::pending().await
    }
    async fn get_session(&self, _: u64) -> leep_core::Result<Option<leep_core::coord::Session>> {
        std::future::pending().await
    }
    async fn list_sessions(
        &self,
        _: &leep_core::coord::SessionFilter,
    ) -> leep_core::Result<Vec<leep_core::coord::Session>> {
        std::future::pending().await
    }
    async fn end_session(
        &self,
        _: u64,
        _: &leep_core::coord::SessionEnd,
    ) -> leep_core::Result<bool> {
        std::future::pending().await
    }
    async fn beat_sessions(&self, _: &[leep_core::coord::Beat]) -> leep_core::Result<u64> {
        std::future::pending().await
    }
    async fn sweep_sessions(
        &self,
        _: i64,
        _: usize,
    ) -> leep_core::Result<leep_core::coord::SweepReport> {
        std::future::pending().await
    }
    async fn count_sessions(
        &self,
        _: leep_core::coord::SessionState,
        _: usize,
    ) -> leep_core::Result<u64> {
        std::future::pending().await
    }
    async fn near_paths(
        &self,
        _: &leep_core::coord::NearQuery,
    ) -> leep_core::Result<Vec<leep_core::coord::NearRow>> {
        std::future::pending().await
    }
}

/// A coordination store that hangs costs a create at most its 250 ms
/// bound, and changes neither the land's answer nor the ref log (ADR
/// 0095): the same commits and lands on a server without sessions answer
/// the same.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_hanging_coordination_store_changes_no_answer() {
    let fresh = || {
        Repo::new(
            Arc::new(MemoryBlobStore::new()),
            Arc::new(MemoryMetaStore::new()),
        )
    };
    let hung = Api::over(fresh().with_coord(Some(Arc::new(Hanging))), true);
    let none = Api::over(fresh().with_coord(None), true);
    let mut answers = Vec::new();
    let mut logs = Vec::new();
    for api in [&hung, &none] {
        let started = std::time::Instant::now();
        let first = api.commit("alice", "a-1", &[], &["src/a.rs"]).await;
        let took = started.elapsed();
        assert!(took < Duration::from_millis(300), "create took {took:?}");
        let second = api.commit("bob", "b-1", &[], &["web/b.ts"]).await;
        let t = token("alice");
        let mut got = Vec::new();
        for cs in [first, second] {
            let body = serde_json::to_vec(&LandRequest {
                changeset: cs,
                target_ref: "main".into(),
                create: Some(true),
                land_id: None,
            })
            .unwrap();
            let r = api
                .call(Method::POST, "/v1/land", Some(&t), Some("a-1"), body)
                .await;
            got.push((r.status, String::from_utf8(r.body).unwrap()));
        }
        answers.push(got);
        let log: RefLogResponse = api
            .call(Method::GET, "/v1/reflog", Some(VIEWER), None, vec![])
            .await
            .ok();
        logs.push(
            log.entries
                .into_iter()
                .map(|e| (e.name, e.old, e.new, e.actor, e.operation))
                .collect::<Vec<_>>(),
        );
    }
    assert_eq!(answers[0], answers[1]);
    assert_eq!(logs[0], logs[1]);
    assert_eq!(logs[0].len(), 2);
}
