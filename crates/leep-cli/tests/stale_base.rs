//! A base whose target ref moved on: what `status` reports and what
//! `sync --no-rebase` does, checked by running the `leep` binary against the
//! in-memory mock server.

mod common;

use std::path::Path;
use std::process::{Command, Output};

use common::Mock;
use leep_core::{ChangesetId, FileKind};

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

    fn json(&self, dir: &Path, args: &[&str]) -> serde_json::Value {
        let mut all = vec!["--json"];
        all.extend_from_slice(args);
        serde_json::from_str(&self.ok(dir, &all)).unwrap()
    }
}

fn text(b: &[u8]) -> String {
    String::from_utf8_lossy(b).into_owned()
}

fn file(body: &str) -> [(&str, &str, FileKind); 1] {
    [("a.txt", body, FileKind::Regular)]
}

/// A working copy whose base is one local commit on top of main's first
/// changeset, after main moved on by two more. Returns the working copy,
/// the local commit and main's head.
fn forked(leep: &Leep, mock: &Mock) -> (tempfile::TempDir, String, ChangesetId) {
    let m1 = mock.seed(None, &file("1\n"), &[]);
    let wc = tempfile::tempdir().unwrap();
    leep.ok(wc.path(), &["--server", &mock.url, "init", "."]);
    std::fs::write(wc.path().join("local.txt"), "mine\n").unwrap();
    let local = leep.ok(wc.path(), &["commit", "-m", "local"]);
    let m2 = mock.seed(Some(m1), &file("2\n"), &[]);
    let m3 = mock.seed(Some(m2), &file("3\n"), &[]);
    (wc, local.trim().to_string(), m3)
}

#[test]
fn status_reports_a_diverged_base_as_ahead_and_behind() {
    let leep = Leep::new();
    let mock = Mock::start();
    let (wc, local, _) = forked(&leep, &mock);
    let st = leep.json(wc.path(), &["status"]);
    assert_eq!(st["base"], local.as_str());
    assert_eq!(st["on_target"], false, "{st}");
    assert_eq!(st["ahead"], 1, "{st}");
    assert_eq!(st["behind"], 2, "{st}");
    let out = leep.ok(wc.path(), &["status"]);
    assert!(
        out.contains("base is 1 commit ahead, 2 commits behind main"),
        "{out}"
    );
}

#[test]
fn status_on_a_base_ahead_of_its_target_has_no_behind() {
    let leep = Leep::new();
    let mock = Mock::start();
    mock.seed(None, &file("1\n"), &[]);
    let wc = tempfile::tempdir().unwrap();
    leep.ok(wc.path(), &["--server", &mock.url, "init", "."]);
    std::fs::write(wc.path().join("local.txt"), "mine\n").unwrap();
    leep.ok(wc.path(), &["commit", "-m", "local"]);
    let st = leep.json(wc.path(), &["status"]);
    assert_eq!(st["on_target"], false, "{st}");
    assert_eq!(st["ahead"], 1, "{st}");
    assert!(st.get("behind").is_none(), "{st}");
    let out = leep.ok(wc.path(), &["status"]);
    assert!(out.contains("base is 1 commit ahead of main\n"), "{out}");
}

#[test]
fn status_on_a_base_unrelated_to_its_target_says_so() {
    let leep = Leep::new();
    let mock = Mock::start();
    let (wc, _, _) = forked(&leep, &mock);
    // main now points at a root changeset that shares nothing with the base.
    mock.seed(None, &file("other\n"), &[]);
    let st = leep.json(wc.path(), &["status"]);
    assert_eq!(st["on_target"], false, "{st}");
    assert!(st["ahead"].is_null(), "{st}");
    assert!(st.get("behind").is_none(), "{st}");
    let out = leep.ok(wc.path(), &["status"]);
    assert!(out.contains("base shares no history with main"), "{out}");
}

fn base_of(leep: &Leep, dir: &Path) -> String {
    leep.json(dir, &["status"])["base"]
        .as_str()
        .unwrap()
        .to_string()
}

#[test]
fn sync_no_rebase_refuses_to_drop_a_local_commit() {
    let leep = Leep::new();
    let mock = Mock::start();
    let (wc, local, _) = forked(&leep, &mock);
    let out = leep.run(wc.path(), &["sync", "--no-rebase"]);
    let err = text(&out.stderr);
    assert_eq!(out.status.code(), Some(1), "{err}");
    assert!(err.contains(&local[..12]), "{err}");
    assert!(err.contains("not on main"), "{err}");
    assert!(err.contains("`leep sync`"), "{err}");
    assert!(err.contains("--discard"), "{err}");
    assert_eq!(base_of(&leep, wc.path()), local);
    assert_eq!(
        std::fs::read_to_string(wc.path().join("local.txt")).unwrap(),
        "mine\n"
    );
}

#[test]
fn sync_no_rebase_discard_abandons_the_local_commit_and_names_it() {
    let leep = Leep::new();
    let mock = Mock::start();
    let (wc, local, head) = forked(&leep, &mock);
    let out = leep.run(wc.path(), &["sync", "--no-rebase", "--discard"]);
    let err = text(&out.stderr);
    assert_eq!(out.status.code(), Some(0), "{err}");
    assert!(err.contains(&local[..12]), "{err}");
    assert_eq!(base_of(&leep, wc.path()), head.to_string());
    assert!(!wc.path().join("local.txt").exists());
}

#[test]
fn sync_no_rebase_still_moves_a_base_that_is_on_the_target() {
    let leep = Leep::new();
    let mock = Mock::start();
    let m1 = mock.seed(None, &file("1\n"), &[]);
    let wc = tempfile::tempdir().unwrap();
    leep.ok(wc.path(), &["--server", &mock.url, "init", "."]);
    let m2 = mock.seed(Some(m1), &file("2\n"), &[]);
    let out = leep.run(wc.path(), &["sync", "--no-rebase"]);
    assert_eq!(out.status.code(), Some(0), "{}", text(&out.stderr));
    assert_eq!(base_of(&leep, wc.path()), m2.to_string());
    assert_eq!(
        std::fs::read_to_string(wc.path().join("a.txt")).unwrap(),
        "2\n"
    );
}

#[test]
fn sync_discard_needs_no_rebase() {
    let leep = Leep::new();
    let mock = Mock::start();
    let (wc, local, _) = forked(&leep, &mock);
    let out = leep.run(wc.path(), &["sync", "--discard"]);
    assert_eq!(out.status.code(), Some(2), "{}", text(&out.stderr));
    assert_eq!(base_of(&leep, wc.path()), local);
}
