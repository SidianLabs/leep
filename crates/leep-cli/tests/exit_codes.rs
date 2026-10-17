//! The documented exit codes (ADR 0033), checked by running the `leep`
//! binary against the in-memory mock server.

mod common;

use std::path::Path;
use std::process::{Command, Output};

use common::{Mock, Tokens};
use leep_core::FileKind;

/// Runs `leep` in `dir` with a private cache and none of the caller's
/// `LEEP_*` settings.
struct Leep {
    cache: tempfile::TempDir,
}

impl Leep {
    fn new() -> Leep {
        Leep {
            cache: tempfile::tempdir().unwrap(),
        }
    }

    fn run(&self, dir: &Path, args: &[&str]) -> Output {
        let mut c = Command::new(env!("CARGO_BIN_EXE_leep"));
        for (k, _) in std::env::vars() {
            if k.starts_with("LEEP_") {
                c.env_remove(k);
            }
        }
        c.current_dir(dir)
            .env("LEEP_CACHE_DIR", self.cache.path())
            .env("LEEP_AUTHOR", "tester")
            .env("NO_COLOR", "1")
            .args(args)
            .output()
            .unwrap()
    }

    fn ok(&self, dir: &Path, args: &[&str]) -> String {
        let out = self.run(dir, args);
        assert!(out.status.success(), "{args:?}: {}", text(&out.stderr));
        text(&out.stdout)
    }
}

fn text(b: &[u8]) -> String {
    String::from_utf8_lossy(b).into_owned()
}

fn init(leep: &Leep, mock: &Mock) -> tempfile::TempDir {
    let wc = tempfile::tempdir().unwrap();
    leep.ok(wc.path(), &["--server", &mock.url, "init", "."]);
    wc
}

/// Two working copies that edit the same line: the second land conflicts.
fn conflicted_land(leep: &Leep, mock: &Mock) -> Output {
    mock.seed(None, &[("a.txt", "base\n", FileKind::Regular)], &[]);
    let (a, b) = (init(leep, mock), init(leep, mock));
    std::fs::write(a.path().join("a.txt"), "from a\n").unwrap();
    std::fs::write(b.path().join("a.txt"), "from b\n").unwrap();
    leep.ok(a.path(), &["commit", "-m", "a"]);
    leep.ok(b.path(), &["commit", "-m", "b"]);
    leep.ok(a.path(), &["land"]);
    leep.run(b.path(), &["land"])
}

#[test]
fn each_error_class_has_its_exit_code() {
    let leep = Leep::new();
    let mock = Mock::start();
    let outside = tempfile::tempdir().unwrap();
    let dir = outside.path();
    let missing = "ab".repeat(32);
    let cases: Vec<(&str, Output, i32)> = vec![
        ("usage: missing argument", leep.run(dir, &["cat"]), 2),
        (
            "usage: unknown subcommand",
            leep.run(dir, &["--server", &mock.url, "frobnicate"]),
            2,
        ),
        (
            "not found",
            leep.run(dir, &["--server", &mock.url, "show", &missing]),
            4,
        ),
        (
            "unknown ref",
            leep.run(dir, &["--server", &mock.url, "log", "nosuch"]),
            4,
        ),
        (
            "server unreachable",
            leep.run(dir, &["--server", "http://127.0.0.1:1", "refs"]),
            3,
        ),
        ("generic: no server configured", leep.run(dir, &["refs"]), 1),
        ("help", leep.run(dir, &["--help"]), 0),
    ];
    for (what, out, want) in cases {
        assert_eq!(
            out.status.code(),
            Some(want),
            "{what}: stdout {} stderr {}",
            text(&out.stdout),
            text(&out.stderr)
        );
    }
}

#[test]
fn a_land_conflict_exits_5() {
    let leep = Leep::new();
    let mock = Mock::start();
    let out = conflicted_land(&leep, &mock);
    assert_eq!(out.status.code(), Some(5), "{}", text(&out.stderr));
    assert!(text(&out.stdout).contains("Cannot land cleanly"));
}

#[test]
fn nothing_to_commit_exits_6_and_a_server_failure_exits_3() {
    let leep = Leep::new();
    let mock = Mock::start();
    let first = mock.seed(None, &[("a.txt", "a\n", FileKind::Regular)], &[]);
    let wc = init(&leep, &mock);
    let out = leep.run(wc.path(), &["commit", "-m", "nothing"]);
    assert_eq!(out.status.code(), Some(6), "{}", text(&out.stderr));
    assert!(text(&out.stderr).contains("nothing to commit"));

    // A 500 from the server: sync with local changes needs a rebase.
    std::fs::write(wc.path().join("a.txt"), "edit\n").unwrap();
    mock.seed(Some(first), &[("b.txt", "b\n", FileKind::Regular)], &[]);
    mock.data.lock().unwrap().fail_rebase = true;
    let out = leep.run(wc.path(), &["sync"]);
    assert_eq!(out.status.code(), Some(3), "{}", text(&out.stderr));
}

#[test]
fn authentication_failures_exit_1() {
    let leep = Leep::new();
    let mock = Mock::start_with_auth(Tokens {
        write: "w-token",
        read: "r-token",
    });
    let dir = tempfile::tempdir().unwrap();
    let out = leep.run(dir.path(), &["--server", &mock.url, "refs"]);
    assert_eq!(out.status.code(), Some(1), "{}", text(&out.stderr));
    assert!(text(&out.stderr).contains("authentication required"));
}

#[test]
fn an_unreachable_server_names_where_the_url_comes_from() {
    let leep = Leep::new();
    let dir = tempfile::tempdir().unwrap();
    let out = leep.run(dir.path(), &["--server", "http://127.0.0.1:1", "refs"]);
    let err = text(&out.stderr);
    assert!(
        err.starts_with("leep: cannot reach http://127.0.0.1:1"),
        "{err}"
    );
    assert!(
        err.contains("hint: ") && err.contains("LEEP_SERVER"),
        "{err}"
    );
}

fn json_error(out: &Output) -> serde_json::Value {
    let v: serde_json::Value = serde_json::from_slice(&out.stdout)
        .unwrap_or_else(|e| panic!("stdout is not JSON ({e}): {}", text(&out.stdout)));
    let err = &v["error"];
    let keys: Vec<&str> = err
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    assert_eq!(keys, ["code", "hint", "message", "retryable"], "{v}");
    assert!(err["message"].as_str().is_some_and(|m| !m.is_empty()));
    err.clone()
}

#[test]
fn json_errors_go_to_stdout_with_a_stable_code() {
    let leep = Leep::new();
    let mock = Mock::start();
    let dir = tempfile::tempdir().unwrap();
    let missing = "ab".repeat(32);
    let cases: Vec<(Vec<&str>, &str, i32, bool)> = vec![
        (
            vec!["--json", "--server", &mock.url, "show", &missing],
            "not_found",
            4,
            false,
        ),
        (vec!["--json", "cat"], "usage", 2, false),
        (
            vec!["cat", "--json", "nosuch-flag", "x", "y"],
            "usage",
            2,
            false,
        ),
        (
            vec!["--json", "--server", "http://127.0.0.1:1", "refs"],
            "unreachable",
            3,
            true,
        ),
        (vec!["--json", "refs"], "user", 1, false),
    ];
    for (args, code, exit, retryable) in cases {
        let out = leep.run(dir.path(), &args);
        assert_eq!(out.status.code(), Some(exit), "{args:?}");
        let err = json_error(&out);
        assert_eq!(err["code"], code, "{args:?}: {err}");
        assert_eq!(err["retryable"], retryable, "{args:?}: {err}");
        // The human line still goes to stderr.
        assert!(text(&out.stderr).starts_with("leep: "), "{args:?}");
    }

    // Without --json, stdout stays empty.
    let out = leep.run(dir.path(), &["--server", &mock.url, "show", &missing]);
    assert!(out.stdout.is_empty());
}

#[test]
fn json_nothing_to_commit_and_conflict_outcomes() {
    let leep = Leep::new();
    let mock = Mock::start();
    mock.seed(None, &[("a.txt", "a\n", FileKind::Regular)], &[]);
    let wc = init(&leep, &mock);
    let out = leep.run(wc.path(), &["--json", "commit", "-m", "nothing"]);
    assert_eq!(out.status.code(), Some(6));
    assert_eq!(json_error(&out)["code"], "nothing_to_do");

    // A land conflict is a result, not an error: the land response, exit 5.
    let mock = Mock::start();
    mock.seed(None, &[("a.txt", "base\n", FileKind::Regular)], &[]);
    let (a, b) = (init(&leep, &mock), init(&leep, &mock));
    std::fs::write(a.path().join("a.txt"), "from a\n").unwrap();
    std::fs::write(b.path().join("a.txt"), "from b\n").unwrap();
    leep.ok(a.path(), &["commit", "-m", "a"]);
    leep.ok(b.path(), &["commit", "-m", "b"]);
    leep.ok(a.path(), &["land"]);
    let out = leep.run(b.path(), &["--json", "land"]);
    assert_eq!(out.status.code(), Some(5));
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(v["status"], "conflicts");
}

#[test]
fn land_onto_a_missing_ref_needs_create() {
    let leep = Leep::new();
    let mock = Mock::start();
    let wc = init(&leep, &mock);
    std::fs::write(wc.path().join("a.txt"), "a\n").unwrap();
    leep.ok(wc.path(), &["commit", "-m", "first"]);

    // The first land onto `main` of an empty repository creates it.
    let out = leep.run(wc.path(), &["land"]);
    assert_eq!(out.status.code(), Some(0), "{}", text(&out.stderr));
    assert!(text(&out.stderr).contains("created ref main"));
    assert!(mock.data.lock().unwrap().refs.contains_key("main"));

    // A typo does not make a branch.
    std::fs::write(wc.path().join("a.txt"), "b\n").unwrap();
    leep.ok(wc.path(), &["commit", "-m", "second"]);
    let out = leep.run(wc.path(), &["land", "--ref", "mian"]);
    assert_eq!(out.status.code(), Some(4), "{}", text(&out.stderr));
    let err = text(&out.stderr);
    assert!(err.contains("--create"), "{err}");
    assert!(!mock.data.lock().unwrap().refs.contains_key("mian"));
    let out = leep.run(wc.path(), &["--json", "land", "--ref", "mian"]);
    assert_eq!(out.status.code(), Some(4));
    assert_eq!(json_error(&out)["code"], "not_found");

    // `--create` makes it and says so; landing there again does not.
    let out = leep.run(wc.path(), &["land", "--ref", "feature", "--create"]);
    assert_eq!(out.status.code(), Some(0), "{}", text(&out.stderr));
    assert!(text(&out.stderr).contains("created ref feature"));
    assert!(mock.data.lock().unwrap().refs.contains_key("feature"));
    let out = leep.run(wc.path(), &["land", "--ref", "feature", "--create"]);
    assert_eq!(out.status.code(), Some(0));
    assert!(!text(&out.stderr).contains("created ref"));
}

#[test]
fn main_is_created_unasked_only_in_an_empty_repository() {
    let leep = Leep::new();
    let mock = Mock::start();
    mock.seed_on("other", None, &[("a.txt", "a\n", FileKind::Regular)], &[]);
    let wc = tempfile::tempdir().unwrap();
    leep.ok(
        wc.path(),
        &["--server", &mock.url, "init", "--ref", "other", "."],
    );
    std::fs::write(wc.path().join("a.txt"), "b\n").unwrap();
    leep.ok(wc.path(), &["commit", "-m", "x"]);
    let out = leep.run(wc.path(), &["land", "--ref", "main"]);
    assert_eq!(out.status.code(), Some(4), "{}", text(&out.stderr));
    assert!(!mock.data.lock().unwrap().refs.contains_key("main"));
}
