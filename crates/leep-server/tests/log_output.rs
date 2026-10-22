//! Logs written to a file (systemd, `> server.log`, a log shipper) must not
//! carry ANSI colour escapes, and JSON logs stay one object per line.

use std::io::Read;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

/// Start a server with its stdout (the log) going to a file, wait for the
/// line that says it listens, stop it and return the log.
fn server_log(extra: &[&str]) -> String {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("server.log");
    let file = std::fs::File::create(&path).unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_leep-server"))
        .args([
            "--listen",
            "127.0.0.1:0",
            "--meta",
            "mem://",
            "--blobs",
            "mem://",
        ])
        .args(extra)
        .env_remove("NO_COLOR")
        .env_remove("RUST_LOG")
        .stdout(Stdio::from(file))
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut log = String::new();
    while Instant::now() < deadline {
        log.clear();
        std::fs::File::open(&path)
            .unwrap()
            .read_to_string(&mut log)
            .unwrap();
        if log.contains("leep-server listening") {
            break;
        }
        if let Some(status) = child.try_wait().unwrap() {
            panic!("server exited with {status}: {log}");
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    let _ = child.kill();
    let _ = child.wait();
    assert!(
        log.contains("leep-server listening"),
        "no listen line: {log}"
    );
    log
}

#[test]
fn text_logs_to_a_file_have_no_ansi_escapes() {
    let log = server_log(&[]);
    assert!(!log.contains('\x1b'), "escape in file log: {log:?}");
    assert!(log.contains(" INFO "), "{log}");
}

#[test]
fn json_logs_to_a_file_are_plain_json_lines() {
    let log = server_log(&["--log-format", "json"]);
    assert!(!log.contains('\x1b'), "escape in file log: {log:?}");
    for line in log.lines().filter(|l| !l.is_empty()) {
        let v: serde_json::Value =
            serde_json::from_str(line).unwrap_or_else(|e| panic!("{e}: {line}"));
        assert!(v.get("level").is_some(), "{line}");
    }
}
