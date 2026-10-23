//! A reader that closes stdout early (`leep log | head -1`) ends the command
//! quietly with 141, the status a shell reports for SIGPIPE, never a panic.

mod common;

use std::io::{BufRead, BufReader};
use std::path::Path;
use std::process::{Command, ExitStatus, Stdio};

use common::Mock;
use leep_core::FileKind;

fn leep(cache: &Path, args: &[&str]) -> Command {
    let mut c = Command::new(env!("CARGO_BIN_EXE_leep"));
    for (k, _) in std::env::vars() {
        if k.starts_with("LEEP_") || k == "PAGER" {
            c.env_remove(k);
        }
    }
    c.current_dir(cache).env("LEEP_CACHE_DIR", cache).args(args);
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
fn closed_stdout_exits_quietly() {
    let mock = Mock::start();
    let mut parent = None;
    for i in 0..50 {
        let body = format!("{i}\n");
        parent = Some(mock.seed(parent, &[("a.txt", &body, FileKind::Regular)], &[]));
        let id = parent.unwrap();
        mock.seed_on(&format!("b{i}"), Some(id), &[], &[]);
    }
    let dir = tempfile::tempdir().unwrap();
    let server = mock.url.as_str();
    // `log` goes through the pager path, `refs` and `--json` print directly.
    let commands: &[&[&str]] = &[
        &["--server", server, "log"],
        &["--server", server, "refs"],
        &["--server", server, "--json", "refs"],
    ];
    for args in commands {
        let (status, stderr) = run_with_closed_stdout(leep(dir.path(), args));
        assert_quiet(status, &stderr, false);
        let (status, first, stderr) = run_closing_after_first_line(leep(dir.path(), args));
        assert!(!first.is_empty(), "{args:?}: no output: {stderr}");
        assert_quiet(status, &stderr, true);
    }
}
