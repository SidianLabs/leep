//! `GET /v1/events` over HTTP (ADR 0099): the tail, pages, filters,
//! `session=KEY` within the caller's principal, 410 for an expired cursor,
//! the long-poll bound on a paused clock, waiter admission, and streams
//! with resume and a reset for a reader that falls behind.

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{header, Method, Request, StatusCode};
use axum::Router;
use http_body_util::BodyExt;
use leep_core::api::*;
use leep_core::coord::NewEvent;
use leep_core::memory::{MemoryBlobStore, MemoryMetaStore};
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

fn repo() -> Repo {
    Repo::new(
        Arc::new(MemoryBlobStore::new()),
        Arc::new(MemoryMetaStore::new()),
    )
}

struct Api {
    app: Router,
    repo: Repo,
}

struct Reply {
    status: StatusCode,
    refused: Option<String>,
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
}

impl Api {
    fn new() -> Api {
        Self::over(repo())
    }

    fn over(repo: Repo) -> Api {
        Api {
            app: leep_server::router_with_auth(repo.clone(), 1 << 20, Some(auth())),
            repo,
        }
    }

    fn request(&self, method: Method, uri: &str, tok: &str, body: Vec<u8>) -> Request<Body> {
        Request::builder()
            .method(method)
            .uri(uri)
            .header(header::HOST, "localhost")
            .header("content-type", "application/json")
            .header(header::AUTHORIZATION, format!("Bearer {tok}"))
            .body(Body::from(body))
            .unwrap()
    }

    async fn send(&self, req: Request<Body>) -> Reply {
        let resp = self.app.clone().oneshot(req).await.unwrap();
        let (parts, body) = resp.into_parts();
        Reply {
            status: parts.status,
            refused: parts
                .headers
                .get(REFUSED_HEADER)
                .map(|v| v.to_str().unwrap().to_string()),
            body: body.collect().await.unwrap().to_bytes().to_vec(),
        }
    }

    async fn get(&self, uri: &str, tok: &str) -> Reply {
        self.send(self.request(Method::GET, uri, tok, vec![])).await
    }

    async fn declare(&self, who: &str, req: serde_json::Value) {
        let r = self
            .send(self.request(
                Method::POST,
                "/v1/sessions",
                &token(who),
                req.to_string().into(),
            ))
            .await;
        let _: DeclareSessionResponse = r.ok();
    }

    async fn events(&self, query: &str, tok: &str) -> EventsResponse {
        self.repo.flush_events().await.unwrap();
        self.get(&format!("/v1/events{query}"), tok).await.ok()
    }
}

fn seq(cursor: &str) -> u64 {
    parse_event_cursor(cursor).unwrap().seq
}

async fn epoch(api: &Api) -> String {
    api.repo.events_tail().await.unwrap().epoch.unwrap()
}

fn ev(paths: &[&str]) -> NewEvent {
    NewEvent {
        kind: "note.added".into(),
        session_id: None,
        principal: "alice".into(),
        subject: "note:1".into(),
        paths: paths.iter().map(|p| p.to_string()).collect(),
        payload: r#"{"note":1}"#.into(),
        created: leep_core::store::unix_now(),
    }
}

/// No cursor answers the tail and no events; a read token reads on from
/// it, by kind and path, and the cursor moves past events it filtered out.
#[tokio::test]
async fn a_reader_starts_at_the_tail_and_filters() {
    let api = Api::new();
    api.declare(
        "alice",
        serde_json::json!({ "session": "s-1", "scope": ["src/a"] }),
    )
    .await;
    let tail = api.events("", VIEWER).await;
    assert!(tail.events.is_empty());
    assert_eq!(
        seq(&tail.next),
        1,
        "the declaration before it is behind the tail"
    );
    api.declare(
        "bob",
        serde_json::json!({ "session": "s-2", "scope": ["docs"], "task_key": "T-9" }),
    )
    .await;
    api.declare(
        "alice",
        serde_json::json!({ "session": "s-1", "scope": ["src/a", "src/b"] }),
    )
    .await;
    let all = api.events(&format!("?after={}", tail.next), VIEWER).await;
    let kinds: Vec<_> = all.events.iter().map(|e| e.kind.as_str()).collect();
    assert_eq!(kinds, ["session.started", "session.paths"]);
    assert_eq!(all.events[0].principal, "bob");
    assert_eq!(all.events[0].untrusted.paths, ["docs"]);
    assert_eq!(all.next, all.events[1].cursor);
    assert!(!all.more);
    let src = api
        .events(&format!("?after={}&paths=src/b/x.rs", tail.next), VIEWER)
        .await;
    assert_eq!(src.events.len(), 1);
    assert_eq!(src.events[0].kind, "session.paths");
    let none = api
        .events(&format!("?after={}&kinds=session.ended", tail.next), VIEWER)
        .await;
    assert!(none.events.is_empty());
    assert_eq!(none.next, all.next, "filtered events still move the cursor");
    // Bad asks are 400, never quoted back past their caps.
    for q in ["?after=12", "?kinds=everything", "?wait=later"] {
        let r = api.get(&format!("/v1/events{q}"), VIEWER).await;
        assert_eq!(r.status, StatusCode::BAD_REQUEST, "{q}");
    }
}

/// `session=KEY` resolves only within the caller's principal: bob naming
/// alice's key sees nothing, and his own session of that key sees other
/// sessions' events on its paths, never its own.
#[tokio::test]
async fn session_keys_resolve_within_the_callers_principal() {
    let api = Api::new();
    let tail = api.events("", VIEWER).await.next;
    api.declare(
        "alice",
        serde_json::json!({ "session": "k", "scope": ["src"] }),
    )
    .await;
    api.declare(
        "alice",
        serde_json::json!({ "session": "other", "scope": ["src/x"] }),
    )
    .await;
    let q = format!("?after={tail}&session=k");
    let bobs = api.events(&q, &token("bob")).await;
    assert!(bobs.events.is_empty(), "bob has no session k");
    assert!(api.events(&q, VIEWER).await.events.is_empty());
    let alices = api.events(&q, &token("alice")).await;
    assert_eq!(alices.events.len(), 1, "her other session, on src/x");
    assert_eq!(alices.events[0].untrusted.paths, ["src/x"]);
    // bob's own session k, on docs: alice's src work is not near it.
    api.declare(
        "bob",
        serde_json::json!({ "session": "k", "scope": ["docs"] }),
    )
    .await;
    assert!(api.events(&q, &token("bob")).await.events.is_empty());
    api.declare(
        "alice",
        serde_json::json!({ "session": "third", "scope": ["docs/guide.md"] }),
    )
    .await;
    let near = api.events(&q, &token("bob")).await;
    assert_eq!(near.events.len(), 1);
    assert_eq!(near.events[0].principal, "alice");
}

/// A cursor older than the events kept is 410 `cursor_expired` naming
/// where to read on; so is one from another store.
#[tokio::test]
async fn an_expired_cursor_is_410() {
    let api = Api::new();
    let coord = api.repo.coord().unwrap().clone();
    coord
        .append_events(&[ev(&["a"]), ev(&["b"])])
        .await
        .unwrap();
    while coord
        .sweep_events(leep_core::store::unix_now() + 10, 1000)
        .await
        .unwrap()
        > 0
    {}
    coord.append_events(&[ev(&["c"])]).await.unwrap();
    let epoch = epoch(&api).await;
    let mine = format!("?after=e{epoch}.0");
    for q in [
        "?after=e0",
        "?after=e1",
        "?after=e99",
        "?after=e0123456789abcdef.2",
        &mine,
    ] {
        let r = api.get(&format!("/v1/events{q}"), VIEWER).await;
        assert_eq!(r.status, StatusCode::GONE, "{q}");
        let e: CursorExpiredResponse = serde_json::from_slice(&r.body).unwrap();
        assert_eq!(e.error.code.as_deref(), Some(error_code::CURSOR_EXPIRED));
        assert_eq!(e.oldest, format!("e{epoch}.2"), "{q}");
    }
    let on = api.events(&format!("?after=e{epoch}.2"), VIEWER).await;
    assert_eq!(on.events.len(), 1);
    assert_eq!(on.next, format!("e{epoch}.3"));
}

/// A long-poll with nothing new answers after its wait, clamped to 30 s,
/// on a paused clock; an event wakes it early.
#[tokio::test(start_paused = true)]
async fn a_long_poll_is_bounded() {
    let api = Api::new();
    let tail = api.events("", VIEWER).await.next;
    for (wait, took) in [("2", 2), ("99", 30)] {
        let t0 = tokio::time::Instant::now();
        let r: EventsResponse = api
            .get(&format!("/v1/events?after={tail}&wait={wait}"), VIEWER)
            .await
            .ok();
        assert_eq!(t0.elapsed(), Duration::from_secs(took));
        assert!(r.events.is_empty());
        assert_eq!(r.next, tail);
    }
    let coord = api.repo.coord().unwrap().clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(5)).await;
        coord.append_events(&[ev(&["a"])]).await.unwrap();
    });
    let t0 = tokio::time::Instant::now();
    let r: EventsResponse = api
        .get(&format!("/v1/events?after={tail}&wait=30"), VIEWER)
        .await
        .ok();
    assert_eq!(r.events.len(), 1);
    assert!(t0.elapsed() < Duration::from_secs(6), "{:?}", t0.elapsed());
}

/// Waiters are admitted per principal and per server: past the cap a
/// waiting read is 503 `Leep-Refused: events`; a read that does not wait
/// is never counted.
#[tokio::test(start_paused = true)]
async fn waiters_past_the_caps_are_refused() {
    let api = Api::over(repo().with_event_waiter_caps(3, 2));
    let tail = api.events("", VIEWER).await.next;
    let long = |tok: String| {
        let app = api.app.clone();
        let req = api.request(
            Method::GET,
            &format!("/v1/events?after={tail}&wait=30"),
            &tok,
            vec![],
        );
        tokio::spawn(async move { app.oneshot(req).await.unwrap().status() })
    };
    let a1 = long(token("alice"));
    let a2 = long(token("alice"));
    tokio::time::sleep(Duration::from_secs(1)).await;
    let wait = format!("/v1/events?after={tail}&wait=30");
    let third = api.get(&wait, &token("alice")).await;
    assert_eq!(third.status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(third.refused.as_deref(), Some(REFUSED_EVENTS));
    let now = api
        .get(&format!("/v1/events?after={tail}"), &token("alice"))
        .await;
    assert_eq!(now.status, StatusCode::OK, "wait=0 takes no place");
    let b1 = long(token("bob"));
    tokio::time::sleep(Duration::from_secs(1)).await;
    let full = api.get(&wait, VIEWER).await;
    assert_eq!(full.status, StatusCode::SERVICE_UNAVAILABLE, "server cap");
    for h in [a1, a2, b1] {
        assert_eq!(h.await.unwrap(), StatusCode::OK);
    }
    assert_eq!(api.get(&wait, VIEWER).await.status, StatusCode::OK);
}

fn stream_req(api: &Api, query: &str, last: Option<&str>) -> Request<Body> {
    let mut req = api.request(Method::GET, &format!("/v1/events{query}"), VIEWER, vec![]);
    req.headers_mut()
        .insert(header::ACCEPT, "text/event-stream".parse().unwrap());
    if let Some(l) = last {
        req.headers_mut()
            .insert("last-event-id", l.parse().unwrap());
    }
    req
}

/// Read a stream's frames until `done` says so or the stream ends.
async fn frames(body: Body, done: impl Fn(&str) -> bool) -> String {
    let mut body = body;
    let mut text = String::new();
    loop {
        let next = tokio::time::timeout(Duration::from_secs(20), body.frame())
            .await
            .expect("the stream stalled");
        match next {
            Some(Ok(f)) => {
                if let Ok(d) = f.into_data() {
                    text.push_str(&String::from_utf8_lossy(&d));
                }
                if done(&text) {
                    return text;
                }
            }
            _ => return text,
        }
    }
}

/// A stream sends each event with its cursor as `id:`, and a reconnect
/// with `Last-Event-ID` resumes after it.
#[tokio::test]
async fn a_stream_sends_events_and_resumes() {
    let api = Api::new();
    let coord = api.repo.coord().unwrap().clone();
    coord
        .append_events(&[ev(&["a"]), ev(&["b"])])
        .await
        .unwrap();
    let resp = api
        .app
        .clone()
        .oneshot(stream_req(&api, "?after=e0", None))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(resp.headers()[header::CONTENT_TYPE], "text/event-stream");
    let ep = epoch(&api).await;
    let id = |n: u64| format!("id: e{ep}.{n}");
    let text = frames(resp.into_body(), |t| t.contains(&id(2))).await;
    assert!(text.contains("event: note.added"), "{text}");
    assert!(text.contains(&id(1)), "{text}");
    coord.append_events(&[ev(&["c"])]).await.unwrap();
    let resp = api
        .app
        .clone()
        .oneshot(stream_req(&api, "", Some(&format!("e{ep}.2"))))
        .await
        .unwrap();
    let text = frames(resp.into_body(), |t| t.contains(&id(3))).await;
    assert!(!text.contains(&id(1)) && !text.contains(&id(2)), "{text}");
    assert!(text.contains("\"untrusted\":{\"paths\":[\"c\"]}"), "{text}");
}

/// A reader that takes nothing while events pour in gets at most what the
/// stream holds for it, then `event: reset`, and the stream ends: the
/// server never buffers without bound. On a paused clock, so the producer
/// has filled the channel before the reader starts, whatever the machine.
#[tokio::test(start_paused = true)]
async fn a_slow_stream_reader_is_reset() {
    let api = Api::new();
    let coord = api.repo.coord().unwrap().clone();
    let resp = api
        .app
        .clone()
        .oneshot(stream_req(&api, "?after=e0", None))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let many: Vec<NewEvent> = (0..400).map(|_| ev(&["a"])).collect();
    coord.append_events(&many).await.unwrap();
    // Nothing is read until the producer has had time to fill the channel.
    let text = {
        tokio::time::sleep(Duration::from_secs(5)).await;
        frames(resp.into_body(), |_| false).await
    };
    let sent = text.matches("event: note.added").count();
    assert!(sent <= leep_server::events::STREAM_BUFFER, "{sent}");
    assert!(
        text.trim_end().ends_with("data: {\"reason\":\"lagged\"}"),
        "{text}"
    );
    assert!(text.contains("event: reset"));
}

/// Without a coordination store the route is 404.
#[tokio::test]
async fn events_are_404_without_sessions() {
    let api = Api::over(repo().with_coord(None));
    assert_eq!(
        api.get("/v1/events", VIEWER).await.status,
        StatusCode::NOT_FOUND
    );
}

/// A waiting read takes its waiter place before any store work: past the
/// cap it is refused at once, though the store would never answer the
/// tail read or the session lookup it would otherwise make first, and a
/// `wait` without `after` is a waiter like any other.
#[tokio::test(start_paused = true)]
async fn admission_comes_before_any_store_work() {
    let faults = Arc::new(leep_core::memory::CoordFaults::new(Arc::new(
        leep_core::memory::MemoryCoordStore::new(),
    )));
    let api = Api::over(
        repo()
            .with_coord(Some(faults.clone()))
            .with_event_waiter_caps(1, 1),
    );
    faults
        .hang_reads
        .store(true, std::sync::atomic::Ordering::SeqCst);
    // One waiter holds the only place (its read hangs on the store).
    let app = api.app.clone();
    let holder = api.request(Method::GET, "/v1/events?after=e0&wait=30", VIEWER, vec![]);
    let held = tokio::spawn(async move { app.oneshot(holder).await });
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(!held.is_finished());
    for q in [
        "?wait=5",
        "?after=e0&wait=5&session=k",
        "?wait=0&wait=5&after=e0",
    ] {
        let r = tokio::time::timeout(
            Duration::from_secs(5),
            api.get(&format!("/v1/events{q}"), &token("bob")),
        )
        .await
        .unwrap_or_else(|_| panic!("{q}: store work before admission"));
        assert_eq!(r.status, StatusCode::SERVICE_UNAVAILABLE, "{q}");
        assert_eq!(r.refused.as_deref(), Some(REFUSED_EVENTS), "{q}");
    }
    held.abort();
}

/// Every land is a `ref.landed` event, a human's without `agent.session`
/// too, with the paths it changed; an agent's session on them matches it.
#[tokio::test]
async fn every_land_is_an_event_over_http() {
    let api = Api::new();
    api.declare(
        "alice",
        serde_json::json!({ "session": "k", "scope": ["src/a.rs"] }),
    )
    .await;
    let tail = api.events("", VIEWER).await.next;
    let content = api
        .repo
        .put_blob(bytes::Bytes::from_static(b"x\n"))
        .await
        .unwrap();
    let cs = leep_core::Changeset {
        parents: vec![],
        author: "bob".into(),
        timestamp: 1,
        message: "by hand".into(),
        changes: [(
            leep_core::RepoPath::new("src/a.rs").unwrap(),
            leep_core::FileChange::Modify {
                content,
                kind: leep_core::FileKind::Regular,
                size: 2,
            },
        )]
        .into(),
        extra: Default::default(),
    };
    let r = api
        .send(api.request(
            Method::POST,
            "/v1/changesets",
            &token("bob"),
            serde_json::to_vec(&cs).unwrap(),
        ))
        .await;
    let made: CreateChangesetResponse = r.ok();
    let land = LandRequest {
        changeset: made.id,
        target_ref: "main".into(),
        create: None,
        land_id: None,
    };
    let r = api
        .send(api.request(
            Method::POST,
            "/v1/land",
            &token("bob"),
            serde_json::to_vec(&land).unwrap(),
        ))
        .await;
    assert_eq!(r.status, StatusCode::OK);
    let q = format!("?after={tail}&session=k&kinds=ref.landed");
    let mut seen = Vec::new();
    for _ in 0..200 {
        seen = api.events(&q, &token("alice")).await.events;
        if !seen.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(seen.len(), 1, "the land reached alice's session");
    assert_eq!(seen[0].principal, "bob");
    assert_eq!(seen[0].untrusted.paths, ["src/a.rs"]);
    assert_eq!(seen[0].untrusted.ref_name.as_deref(), Some("main"));
}
