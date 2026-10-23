//! A reader that closes stdout early (`leep-git import ... | head -1`) ends
//! the command quietly with 141, the status a shell reports for SIGPIPE,
//! never a panic.

use std::io::{BufRead, BufReader};
use std::path::Path;
use std::process::{Command, ExitStatus, Stdio};

fn git(dir: &Path, args: &[&str]) {
    let out = Command::new("/usr/bin/git")
        .args(args)
        .current_dir(dir)
        .env("GIT_AUTHOR_NAME", "Ada Lovelace")
        .env("GIT_AUTHOR_EMAIL", "ada@example.com")
        .env("GIT_COMMITTER_NAME", "Grace Hopper")
        .env("GIT_COMMITTER_EMAIL", "grace@example.com")
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_SYSTEM", "/dev/null")
        .output()
        .expect("run git");
    assert!(
        out.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

/// `leep-git import` of a one-commit repository: it prints two lines.
fn import(git_dir: &Path) -> Command {
    let mut c = Command::new(env!("CARGO_BIN_EXE_leep-git"));
    for (k, _) in std::env::vars() {
        if k.starts_with("LEEP_") {
            c.env_remove(k);
        }
    }
    c.args([
        "import",
        "--meta",
        "mem://",
        "--blobs",
        "mem://",
        "--git-dir",
    ])
    .arg(git_dir);
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
    let repo = tempfile::tempdir().unwrap();
    git(repo.path(), &["init", "-q", "-b", "main"]);
    std::fs::write(repo.path().join("a.txt"), "a\n").unwrap();
    git(repo.path(), &["add", "-A"]);
    git(repo.path(), &["commit", "-q", "-m", "one"]);

    let (status, stderr) = run_with_closed_stdout(import(repo.path()));
    assert_quiet(status, &stderr, false);
    let (status, first, stderr) = run_closing_after_first_line(import(repo.path()));
    assert!(first.starts_with("imported 1 commits"), "{first}: {stderr}");
    assert_quiet(status, &stderr, true);
}
