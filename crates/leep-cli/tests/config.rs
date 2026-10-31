//! `leep config` and where the author of a new changeset comes from, checked
//! by running the binary with a private home, user config and git config,
//! so the real user's settings never leak in.

mod common;

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use common::Mock;
use leep_core::FileKind;

struct Home {
    dir: tempfile::TempDir,
}

impl Home {
    fn new() -> Home {
        let h = Home {
            dir: tempfile::tempdir().unwrap(),
        };
        for d in ["home", "xdg", "appdata", "cache"] {
            std::fs::create_dir_all(h.dir.path().join(d)).unwrap();
        }
        h
    }

    fn path(&self, p: &str) -> PathBuf {
        self.dir.path().join(p)
    }

    fn user_config(&self) -> PathBuf {
        self.path("xdg").join("leep").join("config.json")
    }

    /// Point a command at this home and nothing of the caller's: no `LEEP_*`
    /// settings, no global or system git config, login name `login`.
    fn isolate(&self, c: &mut Command) {
        for (k, _) in std::env::vars() {
            if k.starts_with("LEEP_") || k.starts_with("GIT_") {
                c.env_remove(k);
            }
        }
        c.env("HOME", self.path("home"))
            .env("XDG_CONFIG_HOME", self.path("xdg"))
            .env("APPDATA", self.path("appdata"))
            .env("GIT_CONFIG_GLOBAL", self.path("home").join("no-gitconfig"))
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("LEEP_CACHE_DIR", self.path("cache"))
            .env("USER", "login")
            .env("LOGNAME", "login")
            .env("USERNAME", "login")
            .env("NO_COLOR", "1");
    }

    fn run_env(&self, dir: &Path, env: &[(&str, &str)], args: &[&str]) -> Output {
        let mut c = Command::new(env!("CARGO_BIN_EXE_leep"));
        self.isolate(&mut c);
        c.current_dir(dir).envs(env.iter().copied()).args(args);
        c.output().unwrap()
    }

    fn ok_env(&self, dir: &Path, env: &[(&str, &str)], args: &[&str]) -> String {
        let out = self.run_env(dir, env, args);
        assert!(
            out.status.success(),
            "{args:?}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8_lossy(&out.stdout).into_owned()
    }

    fn ok(&self, dir: &Path, args: &[&str]) -> String {
        self.ok_env(dir, &[], args)
    }

    /// `leep --json config author` as (author, source).
    fn effective(&self, dir: &Path, env: &[(&str, &str)]) -> (String, String) {
        let v: serde_json::Value =
            serde_json::from_str(&self.ok_env(dir, env, &["--json", "config", "author"])).unwrap();
        (
            v["author"].as_str().unwrap_or_default().to_string(),
            v["source"].as_str().unwrap().to_string(),
        )
    }

    fn git(&self, dir: &Path, args: &[&str]) -> bool {
        let mut c = Command::new("git");
        self.isolate(&mut c);
        c.current_dir(dir)
            .args(args)
            .output()
            .is_ok_and(|o| o.status.success())
    }

    /// A working copy with one file, against `mock`.
    fn init(&self, mock: &Mock) -> tempfile::TempDir {
        let wc = tempfile::tempdir().unwrap();
        self.ok(wc.path(), &["--server", &mock.url, "init", "."]);
        wc
    }

    /// Commit a change and return the author it recorded.
    fn commit_author(&self, wc: &Path, n: usize) -> String {
        self.commit_author_env(wc, n, &[])
    }

    fn commit_author_env(&self, wc: &Path, n: usize, env: &[(&str, &str)]) -> String {
        std::fs::write(wc.join("a.txt"), format!("change {n}\n")).unwrap();
        self.ok_env(wc, env, &["commit", "-m", &format!("change {n}")]);
        let v: serde_json::Value =
            serde_json::from_str(&self.ok(wc, &["--json", "show", "@"])).unwrap();
        v["changeset"]["author"].as_str().unwrap().to_string()
    }
}

fn pair(a: &str, s: &str) -> (String, String) {
    (a.to_string(), s.to_string())
}

#[test]
fn config_set_get_unset_round_trip_and_precedence() {
    let home = Home::new();
    let mock = Mock::start();
    mock.seed(None, &[("a.txt", "base\n", FileKind::Regular)], &[]);
    let wc = home.init(&mock);
    let wc = wc.path();

    assert_eq!(home.effective(wc, &[]), pair("login", "login"));

    home.ok(
        wc,
        &["config", "--global", "author", "User <u@example.com>"],
    );
    assert_eq!(
        home.effective(wc, &[]),
        pair("User <u@example.com>", "user")
    );
    let file: serde_json::Value =
        serde_json::from_slice(&std::fs::read(home.user_config()).unwrap()).unwrap();
    assert_eq!(file["author"], "User <u@example.com>");

    home.ok(wc, &["config", "author", "  Repo <r@example.com> "]);
    assert_eq!(
        home.effective(wc, &[]),
        pair("Repo <r@example.com>", "repo")
    );
    let cfg: serde_json::Value =
        serde_json::from_slice(&std::fs::read(wc.join(".leep/config.json")).unwrap()).unwrap();
    assert_eq!(cfg["author"], "Repo <r@example.com>");
    assert_eq!(
        home.ok(wc, &["config", "author"]).trim(),
        "Repo <r@example.com>"
    );
    assert_eq!(
        home.ok(wc, &["config", "--global", "author"]).trim(),
        "User <u@example.com>"
    );
    assert_eq!(home.commit_author(wc, 1), "Repo <r@example.com>");

    let env = [("LEEP_AUTHOR", "Env")];
    assert_eq!(home.effective(wc, &env), pair("Env", "env"));
    assert_eq!(home.commit_author_env(wc, 2, &env), "Env");
    std::fs::write(wc.join("a.txt"), "flag\n").unwrap();
    home.ok_env(wc, &env, &["commit", "-m", "flag", "--author", "Flag"]);
    let v: serde_json::Value =
        serde_json::from_str(&home.ok(wc, &["--json", "show", "@"])).unwrap();
    assert_eq!(v["changeset"]["author"], "Flag");

    home.ok(wc, &["config", "--unset", "author"]);
    assert_eq!(
        home.effective(wc, &[]),
        pair("User <u@example.com>", "user")
    );
    let cfg = std::fs::read_to_string(wc.join(".leep/config.json")).unwrap();
    assert!(!cfg.contains("author"), "{cfg}");
    home.ok(wc, &["config", "--unset", "--global", "author"]);
    assert_eq!(home.effective(wc, &[]), pair("login", "login"));
    // Unsetting again, or with no user config file at all, is fine.
    std::fs::remove_file(home.user_config()).unwrap();
    home.ok(wc, &["config", "--unset", "--global", "author"]);
    assert!(!home.user_config().exists());

    let v: serde_json::Value =
        serde_json::from_str(&home.ok(wc, &["--json", "config", "--global", "author"])).unwrap();
    assert_eq!(v, serde_json::json!({"author": null, "source": null}));
}

#[test]
fn config_rejects_unknown_keys_and_bad_values() {
    let home = Home::new();
    let dir = home.path("home");
    let out = home.run_env(&dir, &[], &["config", "editor", "vi"]);
    assert_eq!(out.status.code(), Some(2));
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("editor") && err.contains("author"), "{err}");

    for bad in ["", "  ", "two\nlines"] {
        let out = home.run_env(&dir, &[], &["config", "--global", "author", bad]);
        assert_eq!(out.status.code(), Some(1), "{bad:?}");
    }
    let out = home.run_env(&dir, &[], &["config", "--unset", "author", "x"]);
    assert_eq!(out.status.code(), Some(2));
    // Setting the working copy's author needs a working copy.
    let out = home.run_env(&dir, &[], &["config", "author", "x"]);
    assert_eq!(out.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&out.stderr).contains("not a leep working copy"));
    assert!(!home.user_config().exists());
}

#[test]
fn author_falls_back_to_git_below_the_leep_settings() {
    let home = Home::new();
    let mock = Mock::start();
    mock.seed(None, &[("a.txt", "base\n", FileKind::Regular)], &[]);
    let wc = home.init(&mock);
    let wc = wc.path();
    if !home.git(wc, &["init", "-q"]) {
        eprintln!("skipping: git is not installed");
        return;
    }
    assert!(home.git(wc, &["config", "user.name", "Git Name"]));
    assert!(home.git(wc, &["config", "user.email", "git@example.com"]));

    assert_eq!(
        home.effective(wc, &[]),
        pair("Git Name <git@example.com>", "git")
    );
    assert_eq!(home.commit_author(wc, 1), "Git Name <git@example.com>");
    // The `.git` directory itself is never recorded.
    let files = home.ok(wc, &["--json", "status"]);
    assert!(!files.contains(".git/"), "{files}");

    assert!(home.git(wc, &["config", "--unset", "user.email"]));
    assert_eq!(home.effective(wc, &[]), pair("Git Name", "git"));

    // The user config wins over git, the working copy's over both.
    home.ok(wc, &["config", "--global", "author", "User"]);
    assert_eq!(home.effective(wc, &[]), pair("User", "user"));
    home.ok(wc, &["config", "author", "Repo"]);
    assert_eq!(home.effective(wc, &[]), pair("Repo", "repo"));
    assert_eq!(
        home.effective(wc, &[("LEEP_AUTHOR", "Env")]),
        pair("Env", "env")
    );

    // No name in git: the login name, as before.
    home.ok(wc, &["config", "--unset", "author"]);
    home.ok(wc, &["config", "--global", "--unset", "author"]);
    assert!(home.git(wc, &["config", "--unset", "user.name"]));
    assert_eq!(home.effective(wc, &[]), pair("login", "login"));
}
