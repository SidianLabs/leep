//! A reader that closes stdout early (`leep-server gen-token | head -1`,
//! `gc --dry-run | head`) ends a one-shot command quietly with 141, the
//! status a shell reports for SIGPIPE, never a panic.

use std::io::{BufRead, BufReader};
use std::process::{Command, ExitStatus, Stdio};

fn server(args: &[&str]) -> Command {
    let mut c = Command::new(env!("CARGO_BIN_EXE_leep-server"));
    c.env_remove("RUST_LOG").args(args);
    for (k, _) in std::env::vars() {
        if k.starts_with("LEEP_") {
            c.env_remove(k);
        }
    }
    c
}

/// Run `cmd` with stdout on a pipe whose read end is already closed, so its
/// first write fails.
fn run_with_closed_stdout(mut cmd: Command) -> (ExitStatus, String) {
    let (reader, writer) = std::io::pipe().unwrap();
    drop(reader);
    let out = cmd.stdout(writer).stderr(Stdio::piped()).output().unwrap();
    (
        out.status,
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

/// Run `cmd`, read one line of its stdout and close the pipe, like `head -1`.
/// The command may have written everything by then, so 0 is fine too.
fn run_closing_after_first_line(mut cmd: Command) -> (ExitStatus, String, String) {
    let mut child = cmd
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut first = String::new();
    BufReader::new(child.stdout.take().unwrap())
        .read_line(&mut first)
        .unwrap();
    let out = child.wait_with_output().unwrap();
    let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
    (out.status, first, stderr)
}

fn assert_quiet(status: ExitStatus, stderr: &str, allow_success: bool) {
    assert!(
        !stderr.contains("panicked") && !stderr.contains("Broken pipe"),
        "{stderr}"
    );
    let code = status.code();
    assert!(
        code == Some(141) || (allow_success && code == Some(0)),
        "status {status}: {stderr}"
    );
}

#[test]
fn closed_stdout_ends_one_shot_commands_quietly() {
    let dir = tempfile::tempdir().unwrap();
    let meta = format!("sqlite://{}/meta.db?mode=rwc", dir.path().display());
    let blobs = format!("file://{}/blobs", dir.path().display());
    // gc logs to stdout as well as printing its report there.
    let commands: Vec<Vec<&str>> = vec![
        vec!["gen-token", "--name", "ci", "--role", "write"],
        vec!["--meta", &meta, "--blobs", &blobs, "gc", "--dry-run"],
    ];
    for args in &commands {
        let (status, stderr) = run_with_closed_stdout(server(args));
        assert_quiet(status, &stderr, false);
        let (status, first, stderr) = run_closing_after_first_line(server(args));
        assert!(!first.is_empty(), "{args:?}: no output: {stderr}");
        assert_quiet(status, &stderr, true);
    }
}

/// The server logs to stdout too, but must keep serving when nothing reads
/// it any more.
#[test]
fn server_outlives_a_closed_stdout() {
    use std::io::{Read, Write};
    use std::time::{Duration, Instant};

    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let listen = format!("127.0.0.1:{port}");
    let (reader, writer) = std::io::pipe().unwrap();
    drop(reader);
    let mut child = server(&["--listen", &listen, "--meta", "mem://", "--blobs", "mem://"])
        .stdout(writer)
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let get_health = || -> Option<String> {
        let mut s = std::net::TcpStream::connect(&listen).ok()?;
        s.write_all(b"GET /v1/health HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
            .ok()?;
        let mut body = String::new();
        s.read_to_string(&mut body).ok()?;
        Some(body)
    };
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut answers = 0;
    while Instant::now() < deadline && answers < 3 {
        if let Some(status) = child.try_wait().unwrap() {
            panic!("server exited with {status}");
        }
        // Each request is logged, so later ones run after failed log writes.
        if get_health().is_some_and(|r| r.starts_with("HTTP/1.1 200")) {
            answers += 1;
        } else {
            std::thread::sleep(Duration::from_millis(50));
        }
    }
    let alive = child.try_wait().unwrap().is_none();
    let _ = child.kill();
    let _ = child.wait();
    assert_eq!(answers, 3, "server stopped answering");
    assert!(alive, "server exited");
}
