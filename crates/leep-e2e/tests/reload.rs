//! Config reload with the real binary (ADR 0024): edit the tokens file, send
//! SIGHUP, and watch the server change its answers without restarting.

mod common;

use std::fs;
use std::path::Path;
use std::time::{Duration, Instant};

use common::*;

/// A token and its tokens-file entry from `leep-server gen-token`.
fn gen_token(name: &str, role: &str) -> (String, serde_json::Value) {
    let out = std::process::Command::new(bin("leep-server"))
        .args(["gen-token", "--name", name, "--role", role])
        .output()
        .unwrap();
    assert_ok(&out, "leep-server gen-token");
    let text = stdout(&out);
    let field = |key: &str| {
        text.lines()
            .find_map(|l| l.strip_prefix(key))
            .unwrap_or_else(|| panic!("no {key} in {text}"))
            .trim()
            .to_string()
    };
    (
        field("token:"),
        serde_json::from_str(&field("entry:")).unwrap(),
    )
}

/// Replace `path` the way an operator should: write beside it, then rename, so
/// a reload never reads half a file.
fn replace_file(path: &Path, text: &str) {
    let tmp = path.with_extension("new");
    fs::write(&tmp, text).unwrap();
    fs::rename(&tmp, path).unwrap();
}

fn tokens_file(entries: &[&serde_json::Value]) -> String {
    serde_json::json!({ "tokens": entries }).to_string()
}

fn status(server: &Server, token: &str) -> u16 {
    http_get(server.addr, "/v1/refs", Some(token)).0
}

/// Poll until `check` holds; a reload runs on the server's own schedule after
/// the signal, so there is no instant to assert at.
fn eventually(what: &str, server: &Server, mut check: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(20);
    while !check() {
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {what}; log:\n{}",
            server.log_text()
        );
        std::thread::sleep(Duration::from_millis(25));
    }
}

fn metric(addr: std::net::SocketAddr, line_start: &str) -> Option<f64> {
    let (_, _, body) = http_get(addr, "/metrics", None);
    body.lines()
        .find(|l| l.starts_with(line_start))
        .and_then(|l| l.rsplit(' ').next()?.parse().ok())
}

#[test]
fn sighup_applies_token_edits_and_rejects_a_bad_file_without_stopping() {
    let _g = serial();
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    let (old_t, old_e) = gen_token("agent", "write");
    let (viewer_t, viewer_e) = gen_token("viewer", "read");
    let tokens = root.join("tokens.json");
    fs::write(&tokens, tokens_file(&[&old_e, &viewer_e])).unwrap();

    let metrics_addr: std::net::SocketAddr = format!("127.0.0.1:{}", free_port()).parse().unwrap();
    let stores = Stores::sqlite(&root.join("data"));
    let server = Server::start_with(
        free_port(),
        &stores,
        root,
        &[
            "--tokens-file",
            tokens.to_str().unwrap(),
            "--metrics-listen",
            &metrics_addr.to_string(),
        ],
    );
    assert_eq!(status(&server, &old_t), 200);
    assert_eq!(metric(metrics_addr, "leep_config_generation"), Some(1.0));

    // Rotate the agent's token and drop the viewer.
    let (new_t, new_e) = gen_token("agent", "write");
    replace_file(&tokens, &tokens_file(&[&new_e]));
    server.sighup();
    eventually("the new token to work", &server, || {
        status(&server, &new_t) == 200
    });
    assert_eq!(status(&server, &old_t), 401, "the old token must stop");
    assert_eq!(status(&server, &viewer_t), 401, "a removed token must stop");
    assert_eq!(metric(metrics_addr, "leep_config_generation"), Some(2.0));
    assert_eq!(
        metric(metrics_addr, "leep_config_reload_total{result=\"ok\"}"),
        Some(1.0)
    );

    // A broken edit is refused: the old config keeps serving, the error is
    // logged and counted, and the generation does not move.
    replace_file(&tokens, "{ this is not json");
    server.sighup();
    eventually("the failed reload to be counted", &server, || {
        metric(metrics_addr, "leep_config_reload_total{result=\"error\"}") == Some(1.0)
    });
    assert_eq!(status(&server, &new_t), 200);
    assert_eq!(status(&server, &old_t), 401);
    assert_eq!(metric(metrics_addr, "leep_config_generation"), Some(2.0));
    eventually("the error to be logged", &server, || {
        server.log_text().contains("config reload failed")
    });
    let log = server.log_text();
    assert!(
        !log.contains(&new_t) && !log.contains(&old_t),
        "tokens leaked into the log"
    );

    // A valid edit afterwards works again.
    let (third_t, third_e) = gen_token("agent", "read");
    replace_file(&tokens, &tokens_file(&[&third_e]));
    server.sighup();
    eventually("recovery after the bad file", &server, || {
        status(&server, &third_t) == 200
    });
    assert_eq!(status(&server, &new_t), 401);
    assert_eq!(http_get(server.addr, "/v1/refs", None).0, 401);
}

#[test]
fn sighup_without_any_config_file_is_harmless() {
    let _g = serial();
    let tmp = tempfile::tempdir().unwrap();
    let stores = Stores::sqlite(&tmp.path().join("data"));
    let server = Server::start(&stores, tmp.path());
    server.sighup();
    server.sighup();
    // Give the handler a moment, then check the server is still the same one.
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(http_get(server.addr, "/v1/health", None).0, 200);
    assert_eq!(http_get(server.addr, "/v1/refs", None).0, 200);
}
