//! Colour and paging (`--color`, `NO_COLOR`, `CLICOLOR_FORCE`, pagers):
//! output to a pipe, as in these tests, is plain unless colour is forced, and
//! is never paged.

mod common;

use std::path::Path;
use std::process::{Command, Output};

use common::Mock;
use leep_core::FileKind;

fn leep(cache: &Path, env: &[(&str, &str)], args: &[&str]) -> Output {
    let mut c = Command::new(env!("CARGO_BIN_EXE_leep"));
    for (k, _) in std::env::vars() {
        if k.starts_with("LEEP_") || k == "NO_COLOR" || k.starts_with("CLICOLOR") {
            c.env_remove(k);
        }
    }
    c.current_dir(cache)
        .env("LEEP_CACHE_DIR", cache)
        .envs(env.iter().copied())
        .args(args)
        .output()
        .unwrap()
}

fn has_escapes(out: &Output) -> bool {
    out.stdout.contains(&0x1b)
}

#[test]
fn colour_only_when_forced_and_never_in_json() {
    let mock = Mock::start();
    let first = mock.seed(None, &[("a.txt", "one\n", FileKind::Regular)], &[]);
    mock.seed(Some(first), &[("a.txt", "two\n", FileKind::Regular)], &[]);
    let dir = tempfile::tempdir().unwrap();
    let base = ["--server", mock.url.as_str()];
    let run = |env: &[(&str, &str)], extra: &[&str]| {
        let args: Vec<&str> = base.iter().chain(extra).copied().collect();
        let out = leep(dir.path(), env, &args);
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
        out
    };
    let log = ["log"];
    let show = ["show", "main"];
    type Vars = &'static [(&'static str, &'static str)];
    let cases: &[(Vars, &[&str], bool)] = &[
        (&[], &[], false),
        (&[("CLICOLOR_FORCE", "1")], &[], true),
        (&[("CLICOLOR_FORCE", "1"), ("NO_COLOR", "1")], &[], false),
        (&[("CLICOLOR_FORCE", "0")], &[], false),
        (&[], &["--color", "always"], true),
        (&[("NO_COLOR", "1")], &["--color", "always"], true),
        (&[("CLICOLOR_FORCE", "1")], &["--color", "never"], false),
        (&[("CLICOLOR_FORCE", "1")], &["--color", "auto"], true),
    ];
    for cmd in [&log[..], &show[..]] {
        for (env, flags, want) in cases {
            let args: Vec<&str> = cmd.iter().chain(flags.iter()).copied().collect();
            let out = run(env, &args);
            assert_eq!(has_escapes(&out), *want, "{cmd:?} {env:?} {flags:?}");
        }
    }
    let out = run(&[("CLICOLOR_FORCE", "1")], &["log"]);
    let text = String::from_utf8_lossy(&out.stdout).into_owned();
    assert!(text.starts_with("\x1b[33mchangeset "), "{text:?}");

    // JSON is never coloured, whatever is asked.
    let out = run(
        &[("CLICOLOR_FORCE", "1")],
        &["--color", "always", "--json", "log"],
    );
    assert!(!has_escapes(&out));
    serde_json::from_slice::<serde_json::Value>(&out.stdout).unwrap();
}

#[test]
fn output_to_a_pipe_is_never_paged() {
    let mock = Mock::start();
    mock.seed(None, &[("a.txt", "one\n", FileKind::Regular)], &[]);
    let dir = tempfile::tempdir().unwrap();
    let marker = dir.path().join("pager-ran");
    let pager = format!("touch {}; cat", marker.display());
    for pager_var in ["LEEP_PAGER", "PAGER"] {
        let out = leep(
            dir.path(),
            &[(pager_var, pager.as_str())],
            &["--server", &mock.url, "log"],
        );
        assert!(out.status.success());
        assert!(String::from_utf8_lossy(&out.stdout).starts_with("changeset "));
        assert!(!marker.exists(), "{pager_var} ran for a pipe");
    }
}
