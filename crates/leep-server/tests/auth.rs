use std::io::Write;
use std::process::{Command, Stdio};
use std::sync::Arc;

use axum::body::Body;
use axum::http::{header, Method, Request, StatusCode};
use axum::Router;
use http_body_util::BodyExt;
use leep_core::api::*;
use leep_core::memory::{MemoryBlobStore, MemoryMetaStore};
use leep_core::{Changeset, FileChange, FileKind, Hash, RepoPath};
use leep_derive::Repo;
use leep_server::auth::{generate, hash_token, Auth, Role};
use tower::ServiceExt;

const READ: &str = "read-token-for-tests";
const WRITE: &str = "write-token-for-tests";

fn auth() -> Auth {
    let text = serde_json::json!({ "tokens": [
        { "name": "viewer", "role": "read", "hash": hash_token(READ) },
        { "name": "ci", "role": "write", "hash": hash_token(WRITE),
          "author": "CI Bot <ci@example.com>" },
    ]})
    .to_string();
    Auth::parse(&text).unwrap()
}

fn repo() -> Repo {
    Repo::new(
        Arc::new(MemoryBlobStore::new()),
        Arc::new(MemoryMetaStore::new()),
    )
}

fn app(with_auth: bool) -> Router {
    leep_server::router_with_auth(repo(), 1 << 20, with_auth.then(auth))
}

struct Reply {
    status: StatusCode,
    www: Option<String>,
    body: Vec<u8>,
}

async fn call(
    app: &Router,
    method: Method,
    uri: &str,
    token: Option<&str>,
    body: Vec<u8>,
) -> Reply {
    let mut b = Request::builder().method(method).uri(uri);
    if let Some(t) = token {
        b = b.header(header::AUTHORIZATION, format!("Bearer {t}"));
    }
    let resp = app
        .clone()
        .oneshot(b.body(Body::from(body)).unwrap())
        .await
        .unwrap();
    let (parts, body) = resp.into_parts();
    Reply {
        status: parts.status,
        www: parts
            .headers
            .get(header::WWW_AUTHENTICATE)
            .map(|v| v.to_str().unwrap().to_string()),
        body: body.collect().await.unwrap().to_bytes().to_vec(),
    }
}

fn json<T: serde::Serialize>(v: &T) -> Vec<u8> {
    serde_json::to_vec(v).unwrap()
}

fn changeset() -> Changeset {
    Changeset {
        parents: vec![],
        author: "t".into(),
        timestamp: 1,
        message: "m".into(),
        changes: [(
            RepoPath::new("a").unwrap(),
            FileChange::Modify {
                content: Hash::of_blob(b"x"),
                kind: FileKind::Regular,
                size: 1,
            },
        )]
        .into(),
        extra: Default::default(),
    }
}

#[tokio::test]
async fn no_auth_mode_is_unchanged() {
    let app = app(false);
    let r = call(&app, Method::GET, "/v1/refs", None, vec![]).await;
    assert_eq!(r.status, StatusCode::OK);
    let h = Hash::of_blob(b"x");
    let r = call(
        &app,
        Method::PUT,
        &format!("/v1/blobs/{h}"),
        None,
        b"x".to_vec(),
    )
    .await;
    assert_eq!(r.status, StatusCode::NO_CONTENT);
    // Credentials sent to an open server are ignored.
    let r = call(&app, Method::GET, "/v1/refs", Some("whatever"), vec![]).await;
    assert_eq!(r.status, StatusCode::OK);
}

#[tokio::test]
async fn health_is_public() {
    let r = call(&app(true), Method::GET, "/v1/health", None, vec![]).await;
    assert_eq!(r.status, StatusCode::OK);
}

#[tokio::test]
async fn missing_token_is_401_with_challenge() {
    let r = call(&app(true), Method::GET, "/v1/refs", None, vec![]).await;
    assert_eq!(r.status, StatusCode::UNAUTHORIZED);
    assert_eq!(r.www.as_deref(), Some("Bearer"));
    let e: ErrorResponse = serde_json::from_slice(&r.body).unwrap();
    assert!(e.error.contains("authentication required"), "{}", e.error);
}

#[tokio::test]
async fn bad_token_is_401() {
    let app = app(true);
    for t in ["nope", "", &hash_token(WRITE)] {
        let r = call(&app, Method::GET, "/v1/refs", Some(t), vec![]).await;
        assert_eq!(r.status, StatusCode::UNAUTHORIZED, "token {t:?}");
        assert_eq!(r.www.as_deref(), Some("Bearer"));
    }
    // Wrong scheme.
    let req = Request::builder()
        .uri("/v1/refs")
        .header(header::AUTHORIZATION, format!("Basic {WRITE}"))
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    // Unknown routes do not leak existence without a token either.
    let r = call(&app, Method::GET, "/v1/nope", None, vec![]).await;
    assert_eq!(r.status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn read_token_reads_but_cannot_mutate() {
    let app = app(true);
    let t = Some(READ);
    assert_eq!(
        call(&app, Method::GET, "/v1/refs", t, vec![]).await.status,
        StatusCode::OK
    );
    assert_eq!(
        call(&app, Method::GET, "/v1/refs/main", t, vec![])
            .await
            .status,
        StatusCode::OK
    );
    let h = Hash::of_blob(b"x");
    let q = MissingBlobsRequest { hashes: vec![h] };
    let r = call(&app, Method::POST, "/v1/blobs/missing", t, json(&q)).await;
    assert_eq!(r.status, StatusCode::OK, "blobs/missing is a query");
    let resp: MissingBlobsResponse = serde_json::from_slice(&r.body).unwrap();
    assert_eq!(resp.missing, vec![h]);

    let cas = CasRefRequest {
        expected: None,
        new: None,
        ..Default::default()
    };
    let mutating = [
        (Method::PUT, format!("/v1/blobs/{h}"), b"x".to_vec()),
        (Method::POST, "/v1/changesets".into(), json(&changeset())),
        (Method::POST, "/v1/refs/main".into(), json(&cas)),
        (Method::POST, "/v1/land".into(), b"{}".to_vec()),
        (Method::POST, "/v1/merge".into(), b"{}".to_vec()),
        (Method::POST, "/v1/rebase".into(), b"{}".to_vec()),
    ];
    for (m, uri, body) in mutating {
        let r = call(&app, m.clone(), &uri, t, body).await;
        assert_eq!(r.status, StatusCode::FORBIDDEN, "{m} {uri}");
        assert!(r.www.is_none());
        let e: ErrorResponse = serde_json::from_slice(&r.body).unwrap();
        assert!(e.error.contains("write access"), "{}", e.error);
    }
}

#[tokio::test]
async fn write_token_does_everything() {
    let app = app(true);
    let t = Some(WRITE);
    let h = Hash::of_blob(b"x");
    let r = call(
        &app,
        Method::PUT,
        &format!("/v1/blobs/{h}"),
        t,
        b"x".to_vec(),
    )
    .await;
    assert_eq!(r.status, StatusCode::NO_CONTENT);
    let r = call(&app, Method::POST, "/v1/changesets", t, json(&changeset())).await;
    assert_eq!(r.status, StatusCode::OK);
    let CreateChangesetResponse { id } = serde_json::from_slice(&r.body).unwrap();
    let r = call(
        &app,
        Method::POST,
        "/v1/land",
        t,
        json(&LandRequest {
            changeset: id,
            target_ref: "main".into(),
            create: None,
        }),
    )
    .await;
    assert_eq!(
        r.status,
        StatusCode::OK,
        "{}",
        String::from_utf8_lossy(&r.body)
    );
    let r = call(&app, Method::GET, "/v1/refs/main", t, vec![]).await;
    let rr: RefResponse = serde_json::from_slice(&r.body).unwrap();
    assert_eq!(rr.target, Some(id));
    let cas = CasRefRequest {
        expected: Some(id),
        new: None,
        ..Default::default()
    };
    let r = call(&app, Method::POST, "/v1/refs/main", t, json(&cas)).await;
    assert_eq!(r.status, StatusCode::OK);
    // The read token sees the same state.
    let r = call(&app, Method::GET, "/v1/refs/main", Some(READ), vec![]).await;
    assert_eq!(r.status, StatusCode::OK);
}

#[tokio::test]
async fn search_needs_a_token_but_only_a_read_one() {
    let app = app(true);
    let uri = "/v1/search/main?q=x";
    let r = call(&app, Method::GET, uri, None, vec![]).await;
    assert_eq!(r.status, StatusCode::UNAUTHORIZED);
    // Authorized; the ref does not exist, which is a 404 and not a 403.
    let r = call(&app, Method::GET, uri, Some(READ), vec![]).await;
    assert_eq!(r.status, StatusCode::NOT_FOUND);
}

fn digest(c: char) -> String {
    format!("blake3:{}", c.to_string().repeat(64))
}

#[test]
fn parses_valid_file() {
    let text = format!(
        r#"{{"tokens":[{{"name":"a","role":"read","hash":"{}"}},{{"name":"b","role":"write","hash":"{}"}}]}}"#,
        digest('a'),
        digest('b')
    );
    assert_eq!(Auth::parse(&text).unwrap().len(), 2);
}

#[test]
fn rejects_bad_files() {
    let entry = |name: &str, role: &str, hash: &str| {
        format!(r#"{{"tokens":[{{"name":"{name}","role":"{role}","hash":"{hash}"}}]}}"#)
    };
    // Malformed.
    assert!(Auth::parse("{ nope").is_err());
    assert!(Auth::parse("{}").is_err());
    assert!(Auth::parse(r#"{"tokens":[]}"#).is_err());
    assert!(Auth::parse(r#"{"tokens":[{"name":"a"}]}"#).is_err());
    // Plaintext token where a digest belongs.
    let e = Auth::parse(&entry("a", "write", "s3cret-plaintext-token"))
        .err()
        .unwrap();
    assert!(e.contains("plaintext"), "{e}");
    // A bare hex digest without the algorithm prefix is also refused.
    assert!(Auth::parse(&entry("a", "write", &"a".repeat(64))).is_err());
    // Bad digest length / chars / role.
    assert!(Auth::parse(&entry("a", "write", "blake3:abc")).is_err());
    assert!(Auth::parse(&entry("a", "write", &format!("blake3:{}", "z".repeat(64)))).is_err());
    assert!(Auth::parse(&entry("a", "admin", &digest('a'))).is_err());
    // Duplicate names.
    let dup = format!(
        r#"{{"tokens":[{{"name":"a","role":"read","hash":"{}"}},{{"name":"a","role":"read","hash":"{}"}}]}}"#,
        digest('a'),
        digest('b')
    );
    assert!(Auth::parse(&dup).is_err());
}

#[test]
fn generated_entry_matches_its_token() {
    let (token, entry) = generate("agent-1", Role::Read).unwrap();
    assert_eq!(token.len(), 64);
    assert!(token.chars().all(|c| c.is_ascii_hexdigit()));
    assert_eq!(entry.hash, hash_token(&token));
    let (other, _) = generate("agent-1", Role::Read).unwrap();
    assert_ne!(token, other);
}

fn server_bin() -> Command {
    Command::new(env!("CARGO_BIN_EXE_leep-server"))
}

fn hash_via_cli(stdin: &str) -> std::process::Output {
    let mut child = server_bin()
        .arg("hash-token")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(stdin.as_bytes())
        .unwrap();
    child.wait_with_output().unwrap()
}

#[test]
fn hash_token_subcommand() {
    let out = hash_via_cli("my-token\n");
    assert!(out.status.success());
    let line = String::from_utf8(out.stdout).unwrap();
    assert_eq!(line, format!("{}\n", hash_token("my-token")));
    assert!(line.starts_with("blake3:") && line.trim_end().len() == 7 + 64);
    assert!(!hash_via_cli("  \n").status.success());
}

#[test]
fn gen_token_subcommand() {
    let out = server_bin()
        .args(["gen-token", "--name", "ci", "--role", "write"])
        .output()
        .unwrap();
    assert!(out.status.success());
    let text = String::from_utf8(out.stdout).unwrap();
    let mut lines = text.lines();
    let token = lines.next().unwrap().strip_prefix("token: ").unwrap();
    let entry: serde_json::Value =
        serde_json::from_str(lines.next().unwrap().strip_prefix("entry: ").unwrap()).unwrap();
    assert!(lines.next().is_none());
    assert_eq!(token.len(), 64);
    assert_eq!(entry["name"], "ci");
    assert_eq!(entry["role"], "write");
    assert_eq!(entry["hash"], hash_token(token));
    // The token never lands in the entry line.
    assert!(!entry.to_string().contains(token));

    let bad = server_bin()
        .args(["gen-token", "--name", "x", "--role", "root"])
        .output()
        .unwrap();
    assert!(!bad.status.success());
}

/// Without tokens, a server reachable from other hosts would let anyone
/// rewrite every ref, so it needs an explicit opt-in.
#[test]
fn startup_refuses_a_public_listen_without_tokens() {
    for addr in ["0.0.0.0:0", "[::]:0"] {
        let out = server_bin()
            .args(["--listen", addr, "--meta", "mem://", "--blobs", "mem://"])
            .env_remove("LEEP_TOKENS_FILE")
            .env_remove("LEEP_INSECURE_NO_AUTH")
            .output()
            .unwrap();
        assert!(!out.status.success(), "{addr}");
        let err = String::from_utf8_lossy(&out.stderr);
        assert!(err.contains("--insecure-no-auth"), "{addr}: {err}");
    }
}

#[test]
fn startup_fails_on_bad_tokens_file() {
    let dir = tempfile::tempdir().unwrap();
    let run = |content: Option<&str>| {
        let path = dir.path().join("tokens.json");
        if let Some(c) = content {
            std::fs::write(&path, c).unwrap();
        }
        server_bin()
            .args([
                "--listen",
                "127.0.0.1:0",
                "--meta",
                "mem://",
                "--blobs",
                "mem://",
            ])
            .arg("--tokens-file")
            .arg(&path)
            .output()
            .unwrap()
    };
    for content in [
        Some("not json"),
        Some(r#"{"tokens":[{"name":"a","role":"write","hash":"plaintext-secret"}]}"#),
        None,
    ] {
        let out = run(content);
        assert!(!out.status.success(), "{content:?}");
        let err = String::from_utf8_lossy(&out.stderr);
        assert!(err.contains("tokens file"), "{err}");
        assert!(
            !err.contains("plaintext-secret"),
            "must not echo file contents"
        );
    }
}

#[tokio::test]
async fn read_token_reads_the_reflog_and_the_actor_is_the_token_name() {
    let app = app(true);
    let h = Hash::of_blob(b"x");
    call(
        &app,
        Method::PUT,
        &format!("/v1/blobs/{h}"),
        Some(WRITE),
        b"x".to_vec(),
    )
    .await;
    let r = call(
        &app,
        Method::POST,
        "/v1/changesets",
        Some(WRITE),
        json(&changeset()),
    )
    .await;
    let CreateChangesetResponse { id } = serde_json::from_slice(&r.body).unwrap();

    for uri in ["/v1/reflog", "/v1/reflog/main"] {
        let r = call(&app, Method::GET, uri, None, vec![]).await;
        assert_eq!(r.status, StatusCode::UNAUTHORIZED, "{uri}");
        let r = call(&app, Method::GET, uri, Some(READ), vec![]).await;
        assert_eq!(r.status, StatusCode::OK, "{uri}");
    }

    let land = LandRequest {
        changeset: id,
        target_ref: "main".into(),
        create: None,
    };
    let r = call(&app, Method::POST, "/v1/land", Some(WRITE), json(&land)).await;
    assert_eq!(r.status, StatusCode::OK);
    let cas = CasRefRequest {
        expected: None,
        new: Some(id),
        operation: Some("cas".into()),
        message: None,
    };
    let r = call(&app, Method::POST, "/v1/refs/dev", Some(WRITE), json(&cas)).await;
    assert_eq!(r.status, StatusCode::OK);
    // A read token cannot write, so it leaves no entry either.
    let r = call(&app, Method::POST, "/v1/refs/dev2", Some(READ), json(&cas)).await;
    assert_eq!(r.status, StatusCode::FORBIDDEN);

    let r = call(&app, Method::GET, "/v1/reflog", Some(READ), vec![]).await;
    let log: RefLogResponse = serde_json::from_slice(&r.body).unwrap();
    let seen: Vec<_> = log
        .entries
        .iter()
        .map(|e| (e.name.as_str(), e.operation.as_str(), e.actor.as_deref()))
        .collect();
    assert_eq!(
        seen,
        [("dev", "cas", Some("ci")), ("main", "land", Some("ci"))]
    );
}

#[tokio::test]
async fn whoami_names_the_token_and_is_null_without_auth() {
    let app = app(true);
    let r = call(&app, Method::GET, "/v1/whoami", Some(WRITE), vec![]).await;
    assert_eq!(r.status, StatusCode::OK);
    let w: WhoAmIResponse = serde_json::from_slice(&r.body).unwrap();
    assert_eq!(
        (w.name.as_deref(), w.role.as_deref()),
        (Some("ci"), Some("write"))
    );
    assert_eq!(w.author.as_deref(), Some("CI Bot <ci@example.com>"));
    let r = call(&app, Method::GET, "/v1/whoami", Some(READ), vec![]).await;
    let w: WhoAmIResponse = serde_json::from_slice(&r.body).unwrap();
    assert_eq!(
        (w.name.as_deref(), w.role.as_deref()),
        (Some("viewer"), Some("read"))
    );
    assert_eq!(w.author, None);
    let r = call(&app, Method::GET, "/v1/whoami", None, vec![]).await;
    assert_eq!(r.status, StatusCode::UNAUTHORIZED);

    let r = call(&self::app(false), Method::GET, "/v1/whoami", None, vec![]).await;
    assert_eq!(r.status, StatusCode::OK);
    let w: WhoAmIResponse = serde_json::from_slice(&r.body).unwrap();
    assert_eq!(w, WhoAmIResponse::default());
}

#[test]
fn token_author_must_be_one_non_empty_line() {
    let entry = |author: &str| {
        serde_json::json!({ "tokens": [
            { "name": "a", "role": "write", "hash": hash_token(WRITE), "author": author },
        ]})
        .to_string()
    };
    assert!(Auth::parse(&entry("A <a@example.com>")).is_ok());
    assert!(Auth::parse(&entry(" ")).is_err());
    assert!(Auth::parse(&entry("A\nB")).is_err());
}
