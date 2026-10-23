//! `POST /v1/refs/{name}/rebase-onto` (ADR 0062): the git mirror's replay of
//! a git-primary ref onto commits git gained.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{header, Method, Request, StatusCode};
use axum::Router;
use bytes::Bytes;
use http_body_util::BodyExt;
use leep_core::api::*;
use leep_core::memory::{MemoryBlobStore, MemoryMetaStore};
use leep_core::{Changeset, ChangesetId, FileChange, FileKind, RepoPath};
use leep_derive::{LandPolicy, Repo};
use leep_server::auth::{hash_token, Auth};
use leep_server::reload::{ConfigHandle, ReloadSources};
use tower::ServiceExt;

const READ: &str = "read-token-for-tests";
const WRITE: &str = "write-token-for-tests";

fn repo() -> Repo {
    Repo::new(
        Arc::new(MemoryBlobStore::new()),
        Arc::new(MemoryMetaStore::new()),
    )
}

struct Reply {
    status: StatusCode,
    body: serde_json::Value,
}

async fn post(app: &Router, uri: &str, token: Option<&str>, body: serde_json::Value) -> Reply {
    let mut b = Request::builder()
        .method(Method::POST)
        .uri(uri)
        .header(header::CONTENT_TYPE, "application/json");
    if let Some(t) = token {
        b = b.header(header::AUTHORIZATION, format!("Bearer {t}"));
    }
    let resp = app
        .clone()
        .oneshot(b.body(Body::from(body.to_string())).unwrap())
        .await
        .unwrap();
    let status = resp.status();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    Reply {
        status,
        body: serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null),
    }
}

async fn rebase_onto(
    app: &Router,
    name: &str,
    onto: ChangesetId,
    base: ChangesetId,
    land_id: Option<&str>,
) -> Reply {
    let mut body = serde_json::json!({ "onto": onto, "expected_base": base });
    if let Some(id) = land_id {
        body["land_id"] = id.into();
    }
    post(app, &format!("/v1/refs/{name}/rebase-onto"), None, body).await
}

async fn commit(r: &Repo, parents: &[ChangesetId], path: &str, text: &str) -> ChangesetId {
    let content = r
        .blobs()
        .put(Bytes::copy_from_slice(text.as_bytes()))
        .await
        .unwrap();
    r.create_changeset(Changeset {
        parents: parents.to_vec(),
        author: "t".into(),
        timestamp: 1,
        message: path.into(),
        changes: BTreeMap::from([(
            RepoPath::new(path).unwrap(),
            FileChange::Modify {
                content,
                kind: FileKind::Regular,
                size: text.len() as u64,
            },
        )]),
        extra: BTreeMap::new(),
    })
    .await
    .unwrap()
}

/// `name` at `b` over `a` over `base`; git's `t` over `t1` over `base`.
struct Split {
    base: ChangesetId,
    a: ChangesetId,
    b: ChangesetId,
    t1: ChangesetId,
    t: ChangesetId,
}

async fn split(r: &Repo, name: &str) -> Split {
    let base = commit(r, &[], "base", "0\n").await;
    r.land(base, name).await.unwrap();
    let a = commit(r, &[base], "a", "a\n").await;
    let b = commit(r, &[a], "b", "b\n").await;
    r.land(b, name).await.unwrap();
    let t1 = commit(r, &[base], "t1", "t1\n").await;
    let t = commit(r, &[t1], "t", "t\n").await;
    Split { base, a, b, t1, t }
}

fn moved(r: &Reply) -> (ChangesetId, Vec<(ChangesetId, ChangesetId)>) {
    assert_eq!(r.status, StatusCode::OK, "{}", r.body);
    match serde_json::from_value(r.body.clone()).unwrap() {
        RebaseOntoResponse::Moved { head, rebased } => (head, rebased),
        other => panic!("expected a move, got {other:?}"),
    }
}

#[tokio::test]
async fn replays_a_ref_with_a_slash_and_logs_one_git_mirror_move() {
    let r = repo();
    let app = leep_server::router(r.clone());
    let s = split(&r, "review/x").await;
    let reply = rebase_onto(&app, "review/x", s.t, s.base, None).await;
    assert_eq!(reply.body["status"], "moved");
    let (head, rebased) = moved(&reply);
    assert_eq!(rebased.iter().map(|p| p.0).collect::<Vec<_>>(), [s.a, s.b]);
    assert_eq!(rebased[1].1, head);
    assert_eq!(r.meta().get_ref("review/x").await.unwrap(), Some(head));
    let parents = r.get_changeset(rebased[0].1).await.unwrap().parents;
    assert_eq!(parents, [s.t]);
    let log = r.meta().ref_log(Some("review/x"), None, 10).await.unwrap();
    assert_eq!(
        (log[0].operation.as_str(), log[0].old, log[0].new),
        ("git-mirror", Some(s.b), Some(head))
    );
    // A plain move of a ref is still `POST /v1/refs/{name}`.
    let cas = post(
        &app,
        "/v1/refs/review/y",
        None,
        serde_json::json!({ "expected": null, "new": head }),
    )
    .await;
    assert_eq!(cas.status, StatusCode::OK, "{}", cas.body);
}

#[tokio::test]
async fn a_stale_expected_base_is_409_and_moves_nothing() {
    let r = repo();
    let app = leep_server::router(r.clone());
    let s = split(&r, "main").await;
    let reply = rebase_onto(&app, "main", s.t, s.a, None).await;
    assert_eq!(reply.status, StatusCode::CONFLICT, "{}", reply.body);
    assert_eq!(reply.body["code"], "cas_lost");
    assert_eq!(r.meta().get_ref("main").await.unwrap(), Some(s.b));
    let missing = rebase_onto(&app, "nope", s.t, s.base, None).await;
    assert_eq!(missing.status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn a_protected_ref_is_refused_with_400() {
    let config = ConfigHandle::load(ReloadSources {
        protect_refs: vec!["main".into()],
        ..Default::default()
    })
    .unwrap();
    let r = repo();
    let app = leep_server::router_with_config(r.clone(), 1 << 20, Duration::from_secs(60), config);
    let s = split(&r, "main").await;
    let reply = rebase_onto(&app, "main", s.t, s.base, None).await;
    assert_eq!(reply.status, StatusCode::BAD_REQUEST, "{}", reply.body);
    assert_eq!(reply.body["code"], "not_forward");
    assert!(
        reply.body["error"].as_str().unwrap().contains("GitHub"),
        "{}",
        reply.body
    );
    assert_eq!(r.meta().get_ref("main").await.unwrap(), Some(s.b));
    // An unprotected ref of the same server replays.
    let s = split(&r, "other").await;
    moved(&rebase_onto(&app, "other", s.t, s.base, None).await);
}

#[tokio::test]
async fn a_land_id_makes_a_retry_answer_with_the_first_result() {
    let r = repo();
    let app = leep_server::router(r.clone());
    let s = split(&r, "main").await;
    let id = "0123456789abcdef0123456789abcdef";
    let first = rebase_onto(&app, "main", s.t, s.base, Some(id)).await;
    let (head, _) = moved(&first);
    let c = commit(&r, &[head], "c", "c\n").await;
    r.land(c, "main").await.unwrap();
    let again = rebase_onto(&app, "main", s.t, s.base, Some(id)).await;
    assert_eq!(again.status, StatusCode::OK);
    assert_eq!(again.body, first.body);
    assert_eq!(r.meta().get_ref("main").await.unwrap(), Some(c));
    let reused = rebase_onto(&app, "main", s.t1, s.base, Some(id)).await;
    assert_eq!(reused.status, StatusCode::BAD_REQUEST);
    assert_eq!(reused.body["code"], "land_id_reused");
}

#[tokio::test]
async fn conflicts_answer_200_and_move_nothing() {
    let r = repo();
    let app = leep_server::router(r.clone());
    let base = commit(&r, &[], "x", "0\n").await;
    r.land(base, "main").await.unwrap();
    let b = commit(&r, &[base], "x", "leep\n").await;
    r.land(b, "main").await.unwrap();
    let t = commit(&r, &[base], "x", "git\n").await;
    let reply = rebase_onto(&app, "main", t, base, None).await;
    assert_eq!(reply.status, StatusCode::OK);
    let RebaseOntoResponse::Conflicts {
        onto, conflicts, ..
    } = serde_json::from_value(reply.body).unwrap()
    else {
        panic!("expected conflicts")
    };
    assert_eq!((onto, conflicts[0].path.as_str()), (t, "x"));
    assert_eq!(r.meta().get_ref("main").await.unwrap(), Some(b));
}

#[tokio::test]
async fn needs_a_write_token() {
    let text = serde_json::json!({ "tokens": [
        { "name": "viewer", "role": "read", "hash": hash_token(READ) },
        { "name": "mirror", "role": "write", "hash": hash_token(WRITE) },
    ]})
    .to_string();
    let r = repo();
    let app = leep_server::router_with_auth(r.clone(), 1 << 20, Some(Auth::parse(&text).unwrap()));
    let s = split(&r, "main").await;
    let body = serde_json::json!({ "onto": s.t, "expected_base": s.base });
    let uri = "/v1/refs/main/rebase-onto";
    assert_eq!(
        post(&app, uri, None, body.clone()).await.status,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        post(&app, uri, Some(READ), body.clone()).await.status,
        StatusCode::FORBIDDEN
    );
    let reply = post(&app, uri, Some(WRITE), body).await;
    let (head, _) = moved(&reply);
    let log = r.meta().ref_log(Some("main"), None, 1).await.unwrap();
    assert_eq!(
        (log[0].new, log[0].actor.as_deref()),
        (Some(head), Some("mirror"))
    );
}

/// Records the histories the policy was asked about.
struct Recorder(Mutex<Vec<Vec<ChangesetId>>>);

#[async_trait]
impl LandPolicy for Recorder {
    fn applies_to(&self, target_ref: &str) -> bool {
        target_ref == "main"
    }

    async fn check_land(
        &self,
        _repo: &Repo,
        _target_ref: &str,
        _head: Option<ChangesetId>,
        new_history: &[ChangesetId],
    ) -> leep_core::Result<()> {
        self.0.lock().unwrap().push(new_history.to_vec());
        Ok(())
    }
}

#[tokio::test]
async fn the_policy_sees_the_imported_changesets_only() {
    let plain = repo();
    let s = split(&plain, "main").await;
    let policy = Arc::new(Recorder(Mutex::new(Vec::new())));
    let app = leep_server::router(plain.clone().with_land_policy(policy.clone()));
    moved(&rebase_onto(&app, "main", s.t, s.base, None).await);
    let seen = policy.0.lock().unwrap().clone();
    assert_eq!(seen.len(), 1);
    let mut checked = seen[0].clone();
    checked.sort();
    let mut imported = vec![s.t, s.t1];
    imported.sort();
    assert_eq!(checked, imported);
}

/// Lands sent with a replay are each in the replayed stack or above its
/// result, and the head keeps all of them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lands_racing_a_replay_are_not_lost() {
    let r = repo();
    let app = leep_server::router(r.clone());
    let s = split(&r, "main").await;
    let mut tasks = Vec::new();
    for i in 0..16 {
        let id = commit(&r, &[s.b], &format!("land/{i}"), "x\n").await;
        let app = app.clone();
        tasks.push(tokio::spawn(async move {
            let body = serde_json::json!({ "changeset": id, "target_ref": "main" });
            post(&app, "/v1/land", None, body).await
        }));
    }
    let replay = {
        let app = app.clone();
        tokio::spawn(async move { rebase_onto(&app, "main", s.t, s.base, None).await })
    };
    let mut landed = Vec::new();
    for t in tasks {
        let reply = t.await.unwrap();
        assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
        match serde_json::from_value(reply.body).unwrap() {
            LandResponse::Landed { landed: l, .. } => landed.push(l),
            other => panic!("{other:?}"),
        }
    }
    let (head, rebased) = moved(&replay.await.unwrap());
    let originals: BTreeMap<_, _> = rebased.into_iter().collect();
    for l in landed {
        assert!(
            originals.contains_key(&l) || r.is_ancestor(head, l).await.unwrap(),
            "{l} is neither replayed nor above the replay"
        );
    }
    let tip = r.meta().get_ref("main").await.unwrap().unwrap();
    let files = r.list_files(tip, "").await.unwrap();
    assert_eq!(files.len(), 16 + 5, "every land, a, b, base, t1 and t");
}
