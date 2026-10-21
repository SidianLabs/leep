//! Config reload without restart (ADR 0024), through the real router. Each
//! test edits files in a temp directory and calls `ConfigHandle::reload`,
//! which is what SIGHUP does; the signal itself is covered by the e2e tests.

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use axum::body::{Body, Bytes};
use axum::http::{header, Method, Request, StatusCode};
use axum::Router;
use ed25519_dalek::{Signer, SigningKey};
use http_body_util::BodyExt;
use leep_core::api::*;
use leep_core::memory::{MemoryBlobStore, MemoryMetaStore};
use leep_core::signature::{format_public_key, signing_message};
use leep_core::{Changeset, ChangesetId, FileChange, FileKind, Hash, RepoPath};
use leep_derive::Repo;
use leep_server::auth::hash_token;
use leep_server::reload::{ConfigHandle, ReloadSources};
use metrics_exporter_prometheus::PrometheusBuilder;
use tokio::sync::mpsc;
use tower::ServiceExt;

const ADMIN: &str = "admin-token-for-tests";
const VIEWER: &str = "viewer-token-for-tests";

fn signer(seed: u8) -> SigningKey {
    SigningKey::from_bytes(&[seed; 32])
}

fn pk(k: &SigningKey) -> String {
    format_public_key(k.verifying_key().as_bytes())
}

/// A tokens file body. Each entry is `(name, role, token)` plus optional extra
/// fields (limits).
fn tokens(entries: &[(&str, &str, &str, serde_json::Value)]) -> String {
    let list: Vec<_> = entries
        .iter()
        .map(|(name, role, token, extra)| {
            let mut t =
                serde_json::json!({ "name": name, "role": role, "hash": hash_token(token) });
            if let Some(extra) = extra.as_object() {
                t.as_object_mut().unwrap().extend(extra.clone());
            }
            t
        })
        .collect();
    serde_json::json!({ "tokens": list }).to_string()
}

fn base_tokens() -> String {
    tokens(&[
        ("admin", "write", ADMIN, serde_json::Value::Null),
        ("viewer", "read", VIEWER, serde_json::Value::Null),
    ])
}

fn keys(seeds: &[(&str, u8)]) -> String {
    let list: Vec<_> = seeds
        .iter()
        .map(|(n, s)| serde_json::json!({ "name": n, "public_key": pk(&signer(*s)) }))
        .collect();
    serde_json::json!({ "keys": list }).to_string()
}

struct Reply {
    status: StatusCode,
    body: Vec<u8>,
}

impl Reply {
    fn error(&self) -> String {
        serde_json::from_slice::<ErrorResponse>(&self.body)
            .unwrap_or_else(|e| panic!("not a JSON error ({e}): {:?}", self.body))
            .error
    }
    fn json<T: serde::de::DeserializeOwned>(&self) -> T {
        serde_json::from_slice(&self.body).unwrap()
    }
}

struct Rig {
    app: Router,
    repo: Repo,
    handle: Arc<ConfigHandle>,
    dir: tempfile::TempDir,
}

#[derive(Default)]
struct Setup {
    tokens: Option<String>,
    keys: Option<String>,
    policy: Option<&'static str>,
    require_signatures: Vec<String>,
    defaults: leep_server::limits::Defaults,
}

impl Rig {
    fn new(s: Setup) -> Rig {
        let dir = tempfile::tempdir().unwrap();
        let write = |name: &str, text: &str| {
            let p = dir.path().join(name);
            std::fs::write(&p, text).unwrap();
            p
        };
        let handle = ConfigHandle::load(ReloadSources {
            tokens_file: Some(write("tokens.json", &s.tokens.unwrap_or_else(base_tokens))),
            signing_keys_file: s.keys.map(|k| write("keys.json", &k)),
            review_policy_file: s.policy.map(|p| write("policy.json", p)),
            limit_defaults: s.defaults,
            require_signatures: s.require_signatures,
            require_review: vec![],
            protect_refs: vec![],
        })
        .unwrap();
        let repo = Repo::new(
            Arc::new(MemoryBlobStore::new()),
            Arc::new(MemoryMetaStore::new()),
        );
        let app = leep_server::router_with_config(
            repo.clone(),
            1 << 20,
            Duration::from_secs(60),
            handle.clone(),
        );
        Rig {
            app,
            repo,
            handle,
            dir,
        }
    }

    fn edit(&self, name: &str, text: &str) {
        std::fs::write(self.dir.path().join(name), text).unwrap();
    }

    fn reload(&self) {
        self.handle.reload().unwrap();
    }

    async fn call(&self, method: Method, uri: &str, token: &str, body: Vec<u8>) -> Reply {
        send(&self.app, request(method, uri, token, Body::from(body))).await
    }

    async fn get(&self, token: &str) -> StatusCode {
        self.call(Method::GET, "/v1/refs", token, vec![])
            .await
            .status
    }

    async fn put(&self, token: &str, data: &[u8]) -> StatusCode {
        self.call(Method::PUT, &blob_uri(data), token, data.to_vec())
            .await
            .status
    }

    async fn post<T: serde::Serialize>(&self, uri: &str, token: &str, body: &T) -> Reply {
        self.call(Method::POST, uri, token, serde_json::to_vec(body).unwrap())
            .await
    }

    async fn land(&self, id: ChangesetId, target: &str) -> Reply {
        self.post(
            "/v1/land",
            ADMIN,
            &LandRequest {
                changeset: id,
                target_ref: target.into(),
                create: None,
                land_id: None,
            },
        )
        .await
    }

    async fn commit(&self, parents: &[ChangesetId], path: &str, text: &str) -> ChangesetId {
        let content = self
            .repo
            .blobs()
            .put(Bytes::copy_from_slice(text.as_bytes()))
            .await
            .unwrap();
        self.repo
            .create_changeset(Changeset {
                parents: parents.to_vec(),
                author: "t".into(),
                timestamp: 1,
                message: format!("write {path}"),
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

    async fn seed(&self, name: &str, head: ChangesetId) {
        assert!(self
            .repo
            .meta()
            .cas_ref(name, None, Some(head))
            .await
            .unwrap());
    }

    async fn sign(&self, id: ChangesetId, k: &SigningKey) -> Reply {
        self.post(
            &format!("/v1/changesets/{id}/signatures"),
            ADMIN,
            &AddSignatureRequest {
                public_key: pk(k),
                signature: hex::encode(k.sign(&signing_message(&id)).to_bytes()),
            },
        )
        .await
    }

    async fn move_ref(
        &self,
        name: &str,
        expected: Option<ChangesetId>,
        new: Option<ChangesetId>,
    ) -> Reply {
        self.post(
            &format!("/v1/refs/{name}"),
            ADMIN,
            &CasRefRequest {
                expected,
                new,
                operation: None,
                message: None,
            },
        )
        .await
    }
}

async fn send(app: &Router, req: Request<Body>) -> Reply {
    let resp = app.clone().oneshot(req).await.unwrap();
    let (parts, body) = resp.into_parts();
    Reply {
        status: parts.status,
        body: body.collect().await.unwrap().to_bytes().to_vec(),
    }
}

fn request(method: Method, uri: &str, token: &str, body: Body) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(uri)
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .body(body)
        .unwrap()
}

fn blob_uri(data: &[u8]) -> String {
    format!("/v1/blobs/{}", Hash::of_blob(data))
}

fn unauthorized() -> StatusCode {
    StatusCode::UNAUTHORIZED
}

#[tokio::test]
async fn a_rotated_token_works_and_the_old_one_stops_without_a_restart() {
    let r = Rig::new(Setup::default());
    assert_eq!(r.get(ADMIN).await, StatusCode::OK);
    assert_eq!(r.get("rotated-token").await, unauthorized());

    r.edit(
        "tokens.json",
        &tokens(&[
            ("admin", "write", "rotated-token", serde_json::Value::Null),
            ("viewer", "read", VIEWER, serde_json::Value::Null),
        ]),
    );
    r.reload();
    assert_eq!(r.get(ADMIN).await, unauthorized());
    assert_eq!(r.get("rotated-token").await, StatusCode::OK);
    assert_eq!(r.get(VIEWER).await, StatusCode::OK);
}

#[tokio::test]
async fn a_revoked_token_stops_and_a_new_one_starts() {
    let r = Rig::new(Setup::default());
    r.edit(
        "tokens.json",
        &tokens(&[
            ("admin", "write", ADMIN, serde_json::Value::Null),
            (
                "newcomer",
                "read",
                "newcomer-token",
                serde_json::Value::Null,
            ),
        ]),
    );
    assert_eq!(r.get("newcomer-token").await, unauthorized());
    r.reload();
    assert_eq!(r.get(VIEWER).await, unauthorized());
    assert_eq!(r.get("newcomer-token").await, StatusCode::OK);
}

#[tokio::test]
async fn a_role_change_applies_to_the_next_request() {
    let r = Rig::new(Setup::default());
    assert_eq!(r.put(VIEWER, b"x").await, StatusCode::FORBIDDEN);
    r.edit(
        "tokens.json",
        &tokens(&[
            ("admin", "read", ADMIN, serde_json::Value::Null),
            ("viewer", "write", VIEWER, serde_json::Value::Null),
        ]),
    );
    r.reload();
    assert_eq!(r.put(VIEWER, b"x").await, StatusCode::NO_CONTENT);
    assert_eq!(r.put(ADMIN, b"y").await, StatusCode::FORBIDDEN);
}

fn limited(rate: f64, burst: u32) -> serde_json::Value {
    serde_json::json!({ "rate": rate, "burst": burst })
}

#[tokio::test]
async fn limits_reset_only_for_tokens_whose_limits_changed() {
    // A tiny refill rate makes the wall clock irrelevant.
    let r = Rig::new(Setup {
        tokens: Some(tokens(&[
            ("admin", "write", ADMIN, limited(0.0001, 2)),
            ("viewer", "read", VIEWER, limited(0.0001, 2)),
        ])),
        ..Setup::default()
    });
    assert_eq!(r.get(ADMIN).await, StatusCode::OK);
    assert_eq!(r.get(VIEWER).await, StatusCode::OK);
    assert_eq!(r.get(VIEWER).await, StatusCode::OK);
    assert_eq!(r.get(VIEWER).await, StatusCode::TOO_MANY_REQUESTS);

    // The viewer's entry is unchanged but for its digest; the admin's burst
    // changes. The viewer keeps its spent bucket, the admin starts full.
    r.edit(
        "tokens.json",
        &tokens(&[
            ("admin", "write", ADMIN, limited(0.0001, 4)),
            ("viewer", "read", "viewer-rotated", limited(0.0001, 2)),
        ]),
    );
    r.reload();
    assert_eq!(r.get("viewer-rotated").await, StatusCode::TOO_MANY_REQUESTS);
    for _ in 0..4 {
        assert_eq!(r.get(ADMIN).await, StatusCode::OK);
    }
    assert_eq!(r.get(ADMIN).await, StatusCode::TOO_MANY_REQUESTS);

    // Lifting a limit takes effect at once, and removing a token drops it.
    r.edit(
        "tokens.json",
        &tokens(&[
            (
                "admin",
                "write",
                ADMIN,
                serde_json::json!({ "rate": "unlimited" }),
            ),
            ("viewer", "read", "viewer-rotated", limited(0.0001, 2)),
        ]),
    );
    r.reload();
    for _ in 0..10 {
        assert_eq!(r.get(ADMIN).await, StatusCode::OK);
    }
    assert_eq!(r.get("viewer-rotated").await, StatusCode::TOO_MANY_REQUESTS);
}

#[tokio::test]
async fn a_token_that_gains_limits_is_limited_and_one_that_loses_them_is_not() {
    let r = Rig::new(Setup::default());
    for _ in 0..5 {
        assert_eq!(r.get(VIEWER).await, StatusCode::OK);
    }
    r.edit(
        "tokens.json",
        &tokens(&[
            ("admin", "write", ADMIN, serde_json::Value::Null),
            (
                "viewer",
                "read",
                VIEWER,
                serde_json::json!({ "max_body": 4 }),
            ),
        ]),
    );
    r.reload();
    let big = r
        .call(Method::POST, "/v1/blobs/missing", VIEWER, vec![b' '; 64])
        .await;
    assert_eq!(big.status, StatusCode::PAYLOAD_TOO_LARGE);
    r.edit("tokens.json", &base_tokens());
    r.reload();
    let ok = r
        .call(
            Method::POST,
            "/v1/blobs/missing",
            VIEWER,
            br#"{"hashes":[]}"#.to_vec(),
        )
        .await;
    assert_eq!(ok.status, StatusCode::OK);
}

#[tokio::test]
async fn adding_and_removing_a_trusted_key_changes_what_land_accepts() {
    let r = Rig::new(Setup {
        keys: Some(keys(&[("alice", 1)])),
        require_signatures: vec!["main".into()],
        ..Setup::default()
    });
    let by_bob = r.commit(&[], "a", "1\n").await;

    // Bob is not trusted: his signature is refused and the land is too.
    let refused = r.sign(by_bob, &signer(2)).await;
    assert_eq!(refused.status, StatusCode::BAD_REQUEST);
    assert!(
        refused.error().contains("not trusted"),
        "{}",
        refused.error()
    );
    let land = r.land(by_bob, "main").await;
    assert_eq!(land.status, StatusCode::BAD_REQUEST);

    r.edit("keys.json", &keys(&[("alice", 1), ("bob", 2)]));
    r.reload();
    assert_eq!(r.sign(by_bob, &signer(2)).await.status, StatusCode::OK);
    assert_eq!(r.land(by_bob, "main").await.status, StatusCode::OK);

    // Alice signs the next one, then loses trust: it can no longer land, and
    // her signature stops being listed as verified.
    let by_alice = r.commit(&[by_bob], "b", "2\n").await;
    assert_eq!(r.sign(by_alice, &signer(1)).await.status, StatusCode::OK);
    r.edit("keys.json", &keys(&[("bob", 2)]));
    r.reload();
    let land = r.land(by_alice, "main").await;
    assert_eq!(land.status, StatusCode::BAD_REQUEST, "{}", land.error());
    let listed = r
        .call(
            Method::GET,
            &format!("/v1/changesets/{by_alice}/signatures"),
            VIEWER,
            vec![],
        )
        .await
        .json::<SignaturesResponse>();
    assert!(listed.signatures.is_empty());
}

#[tokio::test]
async fn review_policy_refs_and_rules_change_at_runtime() {
    let r = Rig::new(Setup {
        policy: Some("{}"),
        ..Setup::default()
    });
    let head = r.commit(&[], "a.txt", "x = 1\n").await;
    r.seed("main", head).await;

    // `main` is not protected yet: a plain land is fine, and so is moving it.
    let other = r.commit(&[head], "b.txt", "other\n").await;
    assert_eq!(r.land(other, "main").await.status, StatusCode::OK);

    // Protect it: now the whitespace change needs a review.
    r.edit("policy.json", r#"{ "refs": ["main"] }"#);
    r.reload();
    let whitespace = r.commit(&[other], "a.txt", "x = 1 \n").await;
    let needs = r.land(whitespace, "main").await;
    assert_eq!(needs.status, StatusCode::BAD_REQUEST);
    assert!(needs.error().contains("review"), "{}", needs.error());

    // Teach the policy that whitespace-only edits approve themselves.
    r.edit(
        "policy.json",
        r#"{ "refs": ["main"], "auto_approve": ["whitespace_only"] }"#,
    );
    r.reload();
    assert_eq!(r.land(whitespace, "main").await.status, StatusCode::OK);

    // Unprotect it again.
    r.edit("policy.json", "{}");
    r.reload();
    let more = r.commit(&[whitespace], "c.txt", "more\n").await;
    assert_eq!(r.land(more, "main").await.status, StatusCode::OK);
}

#[tokio::test]
async fn protected_delete_and_forward_only_rules_follow_the_reloaded_refs() {
    let r = Rig::new(Setup {
        policy: Some("{}"),
        ..Setup::default()
    });
    let first = r.commit(&[], "a", "1\n").await;
    let second = r.commit(&[first], "a", "2\n").await;
    r.seed("stable", second).await;

    r.edit("policy.json", r#"{ "refs": ["stable"] }"#);
    r.reload();
    let rewind = r.move_ref("stable", Some(second), Some(first)).await;
    assert_eq!(rewind.status, StatusCode::BAD_REQUEST);
    assert!(rewind.error().contains("forward"), "{}", rewind.error());
    let delete = r.move_ref("stable", Some(second), None).await;
    assert_eq!(delete.status, StatusCode::BAD_REQUEST);
    assert!(delete.error().contains("protected"), "{}", delete.error());

    r.edit("policy.json", "{}");
    r.reload();
    let rewind = r.move_ref("stable", Some(second), Some(first)).await;
    assert_eq!(rewind.status, StatusCode::OK, "{}", rewind.error());
    let delete = r.move_ref("stable", Some(first), None).await;
    assert_eq!(delete.status, StatusCode::OK);
}

#[tokio::test]
async fn a_reload_that_adds_a_bad_ref_name_changes_nothing() {
    let r = Rig::new(Setup {
        policy: Some("{}"),
        ..Setup::default()
    });
    let before = r.handle.current();
    r.edit(
        "tokens.json",
        &tokens(&[("admin", "write", "different", serde_json::Value::Null)]),
    );
    r.edit("policy.json", r#"{ "refs": ["main "] }"#);
    let e = r.handle.reload().unwrap_err();
    assert!(e.contains("review"), "{e}");
    assert!(Arc::ptr_eq(&before, &r.handle.current()));
    // The tokens edit did not slip through on its own.
    assert_eq!(r.get(ADMIN).await, StatusCode::OK);
    assert_eq!(r.get("different").await, unauthorized());
}

fn with_metrics<F: Future<Output = ()>>(fut: F) -> String {
    let recorder = PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    metrics::with_local_recorder(&recorder, || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(fut)
    });
    handle.render()
}

fn series<'a>(text: &'a str, name: &str, parts: &[&str]) -> &'a str {
    text.lines()
        .find(|l| l.starts_with(name) && parts.iter().all(|p| l.contains(p)))
        .unwrap_or_else(|| panic!("no {name} line with {parts:?} in:\n{text}"))
}

fn value(line: &str) -> f64 {
    line.rsplit(' ').next().unwrap().parse().unwrap()
}

#[test]
fn reloads_are_counted_and_the_generation_and_time_are_exported() {
    let text = with_metrics(async {
        let r = Rig::new(Setup::default());
        r.edit("tokens.json", "{ broken");
        assert!(r.handle.reload().is_err());
        assert!(r.handle.reload().is_err());
        r.edit("tokens.json", &base_tokens());
        r.reload();
        assert_eq!(r.get(ADMIN).await, StatusCode::OK);
    });
    let n = |result: &str| {
        value(series(
            &text,
            "leep_config_reload_total",
            &[&format!("result=\"{result}\"")],
        ))
    };
    assert_eq!(n("error"), 2.0);
    assert_eq!(n("ok"), 1.0);
    assert_eq!(
        value(series(&text, "leep_config_generation", &[])),
        2.0,
        "failed reloads do not advance the generation"
    );
    let at = value(series(
        &text,
        "leep_config_last_reload_timestamp_seconds",
        &[],
    ));
    assert!(at > 1.7e9, "{at}");
}

#[test]
fn counters_exist_at_zero_before_any_reload() {
    let text = with_metrics(async {
        let _r = Rig::new(Setup::default());
    });
    for result in ["ok", "error"] {
        let line = series(
            &text,
            "leep_config_reload_total",
            &[&format!("result=\"{result}\"")],
        );
        assert_eq!(value(line), 0.0);
    }
    assert_eq!(value(series(&text, "leep_config_generation", &[])), 1.0);
}

/// A request whose body the test controls: it stays in flight until the
/// sender is dropped.
fn held(method: Method, uri: &str, token: &str) -> (mpsc::Sender<Bytes>, Request<Body>) {
    let (tx, rx) = mpsc::channel::<Bytes>(4);
    let stream = futures_util::stream::unfold(rx, |mut rx| async move {
        rx.recv().await.map(|b| (Ok::<_, Infallible>(b), rx))
    });
    (tx, request(method, uri, token, Body::from_stream(stream)))
}

/// Yield until `token` is refused for concurrency, i.e. its held request has
/// passed authentication and the limiter and is stalled in the handler.
async fn until_busy(r: &Rig, token: &str) {
    for _ in 0..500 {
        if r.get(token).await == StatusCode::TOO_MANY_REQUESTS {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("the held request never became busy");
}

#[tokio::test]
async fn an_in_flight_request_finishes_under_the_config_it_started_with() {
    let one = serde_json::json!({ "concurrency": 1 });
    let r = Rig::new(Setup {
        tokens: Some(tokens(&[
            ("admin", "write", ADMIN, serde_json::Value::Null),
            ("uploader", "write", "uploader-token", one.clone()),
            ("lander", "write", "lander-token", one.clone()),
        ])),
        policy: Some("{}"),
        ..Setup::default()
    });
    let head = r.commit(&[], "a", "1\n").await;
    r.seed("main", head).await;
    let next = r.commit(&[head], "a", "2\n").await;
    let after = r.commit(&[next], "a", "3\n").await;

    let data: &'static [u8] = b"uploaded under the old config";
    let (up_tx, up_req) = held(Method::PUT, &blob_uri(data), "uploader-token");
    let (land_tx, land_req) = held(Method::POST, "/v1/land", "lander-token");
    let up = tokio::spawn({
        let app = r.app.clone();
        async move { send(&app, up_req).await }
    });
    let land = tokio::spawn({
        let app = r.app.clone();
        async move { send(&app, land_req).await }
    });
    until_busy(&r, "uploader-token").await;
    until_busy(&r, "lander-token").await;

    // Revoke the uploader and the lander, and protect `main`, while both
    // requests are stalled reading their bodies.
    r.edit(
        "tokens.json",
        &tokens(&[
            ("admin", "write", ADMIN, serde_json::Value::Null),
            ("uploader", "write", "uploader-rotated", one.clone()),
            ("lander", "write", "lander-token", one),
        ]),
    );
    r.edit("policy.json", r#"{ "refs": ["main"] }"#);
    r.reload();

    // New requests see the new config: the old token is gone, `main` needs
    // review, and the lander's in-flight count survived the reload.
    assert_eq!(r.get("uploader-token").await, unauthorized());
    assert_eq!(r.get("lander-token").await, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(r.land(after, "main").await.status, StatusCode::BAD_REQUEST);

    // The stalled requests finish with the config they started with.
    up_tx.send(Bytes::from_static(data)).await.unwrap();
    drop(up_tx);
    assert_eq!(up.await.unwrap().status, StatusCode::NO_CONTENT);
    let body = serde_json::to_vec(&LandRequest {
        changeset: next,
        target_ref: "main".into(),
        create: None,
        land_id: None,
    })
    .unwrap();
    land_tx.send(Bytes::from(body)).await.unwrap();
    drop(land_tx);
    let done = land.await.unwrap();
    assert_eq!(
        done.status,
        StatusCode::OK,
        "{}",
        String::from_utf8_lossy(&done.body)
    );
    assert!(matches!(
        done.json::<LandResponse>(),
        LandResponse::Landed { landed, .. } if landed == next
    ));
    assert_eq!(r.repo.meta().get_ref("main").await.unwrap(), Some(next));
}

#[tokio::test]
async fn requests_stay_well_formed_while_reloads_churn() {
    // Token `flip` is a read token under one config and a write token under
    // another, while reloads swap them back and forth. Every request must get
    // a clean answer from exactly one of the two, never a 5xx.
    let r = Rig::new(Setup {
        policy: Some("{}"),
        ..Setup::default()
    });
    let head = r.commit(&[], "a", "1\n").await;
    r.seed("main", head).await;
    let a_tokens = tokens(&[
        ("admin", "write", ADMIN, serde_json::Value::Null),
        ("flip", "read", "flip-token", serde_json::Value::Null),
    ]);
    let b_tokens = tokens(&[
        ("admin", "write", ADMIN, serde_json::Value::Null),
        ("flip", "write", "flip-token", serde_json::Value::Null),
    ]);
    r.edit("tokens.json", &a_tokens);
    r.reload();
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    // Stops the flipper even if an assertion below panics.
    struct Stop(Arc<std::sync::atomic::AtomicBool>);
    impl Drop for Stop {
        fn drop(&mut self) {
            self.0.store(true, std::sync::atomic::Ordering::Relaxed);
        }
    }
    let _stop = Stop(stop.clone());
    let flipper = {
        let (h, dir, stop) = (r.handle.clone(), r.dir.path().to_path_buf(), stop.clone());
        tokio::task::spawn_blocking(move || {
            let mut b = false;
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                b = !b;
                let (t, p) = if b {
                    (&b_tokens, r#"{ "refs": ["main"] }"#)
                } else {
                    (&a_tokens, "{}")
                };
                std::fs::write(dir.join("tokens.json"), t).unwrap();
                std::fs::write(dir.join("policy.json"), p).unwrap();
                let _ = h.reload();
            }
        })
    };
    for i in 0..200 {
        let next = r.commit(&[head], "a", &format!("v{i}\n")).await;
        let resp = r
            .post(
                "/v1/land",
                "flip-token",
                &LandRequest {
                    changeset: next,
                    target_ref: "scratch".into(),
                    create: None,
                    land_id: None,
                },
            )
            .await;
        // Either refused as a read token (403) or accepted as a writer (200);
        // never a 5xx and never a panic from a half-built config.
        assert!(
            resp.status == StatusCode::FORBIDDEN || resp.status == StatusCode::OK,
            "{} {}",
            resp.status,
            String::from_utf8_lossy(&resp.body)
        );
        tokio::task::yield_now().await;
    }
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    flipper.await.unwrap();
}
