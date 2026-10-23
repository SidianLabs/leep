//! `GET /v1/ready` and the config fingerprint with the real `leep-server`.
#![cfg_attr(not(unix), allow(dead_code))]

mod common;

use std::fs;

use common::*;

fn check_ready(server: &Server) {
    let (status, _, body) = http_get(server.addr, "/v1/ready", None);
    assert_eq!(status, 200, "{body}\nlog:\n{}", server.log_text());
    assert_eq!(
        body.trim(),
        r#"{"status":"ready","features":["land-id","indexing-retry","changeset-rescue","land-clean-blobs"]}"#
    );
    // Liveness is unchanged.
    let (status, _, body) = http_get(server.addr, "/v1/health", None);
    assert_eq!((status, body.trim()), (200, r#""ok""#));
}

#[test]
fn ready_on_sqlite_without_a_token_even_when_tokens_are_required() {
    let _g = serial();
    let tmp = tempfile::tempdir().unwrap();
    let stores = Stores::sqlite(&tmp.path().join("data"));
    check_ready(&Server::start(&stores, tmp.path()));

    let tokens = tmp.path().join("tokens.json");
    let out = std::process::Command::new(bin("leep-server"))
        .args(["gen-token", "--name", "ci", "--role", "write"])
        .output()
        .unwrap();
    assert_ok(&out, "leep-server gen-token");
    let entry = stdout(&out)
        .lines()
        .find_map(|l| l.strip_prefix("entry:").map(|e| e.trim().to_string()))
        .unwrap();
    fs::write(&tokens, format!(r#"{{ "tokens": [{entry}] }}"#)).unwrap();
    let server = Server::start_with(
        free_port(),
        &stores,
        tmp.path(),
        &[
            "--tokens-file",
            tokens.to_str().unwrap(),
            "--ready-timeout",
            "500ms",
        ],
    );
    check_ready(&server);
    assert_eq!(http_get(server.addr, "/v1/refs", None).0, 401);
}

#[test]
fn ready_on_postgres() {
    let _g = serial();
    let Some(db) = PgDb::create() else {
        return;
    };
    let tmp = tempfile::tempdir().unwrap();
    let stores = Stores::with_meta(&tmp.path().join("data"), db.url.clone());
    check_ready(&Server::start(&stores, tmp.path()));
}

#[test]
fn a_zero_ready_timeout_is_refused_at_startup() {
    let _g = serial();
    let out = std::process::Command::new(bin("leep-server"))
        .args(["--ready-timeout", "0"])
        .output()
        .unwrap();
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("greater than zero"));
}

fn fingerprints(addr: std::net::SocketAddr) -> Vec<(String, f64)> {
    let (_, _, body) = http_get(addr, "/metrics", None);
    body.lines()
        .filter(|l| l.starts_with("leep_config_info{"))
        .map(|l| {
            let fp = l
                .split("fingerprint=\"")
                .nth(1)
                .unwrap()
                .split('"')
                .next()
                .unwrap();
            (
                fp.to_string(),
                l.rsplit(' ').next().unwrap().parse().unwrap(),
            )
        })
        .collect()
}

fn the_one(addr: std::net::SocketAddr) -> String {
    let all = fingerprints(addr);
    let ones: Vec<_> = all.iter().filter(|(_, v)| *v == 1.0).collect();
    assert_eq!(ones.len(), 1, "{all:?}");
    ones[0].0.clone()
}

/// A restart resets the generation but not the fingerprint; a real change
/// moves the fingerprint and leaves one series at 1.
#[cfg(unix)] // needs SIGHUP
#[test]
fn the_fingerprint_survives_a_restart_and_follows_the_content() {
    let _g = serial();
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    let entry = |name: &str| {
        let out = std::process::Command::new(bin("leep-server"))
            .args(["gen-token", "--name", name, "--role", "write"])
            .output()
            .unwrap();
        assert_ok(&out, "leep-server gen-token");
        let e = stdout(&out)
            .lines()
            .find_map(|l| l.strip_prefix("entry:").map(|e| e.trim().to_string()))
            .unwrap();
        serde_json::from_str::<serde_json::Value>(&e).unwrap()
    };
    let (a, b) = (entry("a"), entry("b"));
    let tokens = root.join("tokens.json");
    fs::write(&tokens, serde_json::json!({ "tokens": [a, b] }).to_string()).unwrap();
    let stores = Stores::sqlite(&root.join("data"));
    let metrics_addr: std::net::SocketAddr = format!("127.0.0.1:{}", free_port()).parse().unwrap();
    let args = [
        "--tokens-file",
        tokens.to_str().unwrap(),
        "--metrics-listen",
        &metrics_addr.to_string(),
    ];

    let mut first = Server::start_with(free_port(), &stores, root, &args);
    let before = the_one(metrics_addr);
    assert_eq!(before.len(), 16);
    first.kill();

    // Same files, a new process, the same fingerprint (reordered and
    // reformatted on the way).
    fs::write(
        &tokens,
        format!(
            "{}\n",
            serde_json::to_string_pretty(&serde_json::json!({ "tokens": [b, a] })).unwrap()
        ),
    )
    .unwrap();
    let second = Server::start_with(free_port(), &stores, root, &args);
    assert_eq!(the_one(metrics_addr), before);

    // A reload that changes nothing keeps it.
    second.sighup();
    std::thread::sleep(std::time::Duration::from_millis(500));
    assert_eq!(the_one(metrics_addr), before);

    // A removed token changes it.
    fs::write(&tokens, serde_json::json!({ "tokens": [a] }).to_string()).unwrap();
    second.sighup();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
    loop {
        let all = fingerprints(metrics_addr);
        if all.contains(&(before.clone(), 0.0)) {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "{all:?}\n{}",
            second.log_text()
        );
        std::thread::sleep(std::time::Duration::from_millis(25));
    }
    let after = the_one(metrics_addr);
    assert_ne!(after, before);
}
