//! Git-style revision suffixes (ADR 0064): the server walks `~N` and `^N`;
//! the CLI swaps `@` and short ids for full ids before sending the suffix.

mod common;

use std::path::Path;
use std::process::Command;

use common::Mock;
use leep_cli::cache::Cache;
use leep_cli::client::{Client, RevVia};
use leep_cli::commands::Remote;
use leep_cli::error::ErrorKind;
use leep_core::{ChangesetId, FileKind};

/// `main` = c3 -> c2 -> c1, each rewriting `a.txt`.
fn seed_chain(mock: &Mock) -> [ChangesetId; 3] {
    let c1 = mock.seed(None, &[("a.txt", "1\n", FileKind::Regular)], &[]);
    let c2 = mock.seed(Some(c1), &[("a.txt", "2\n", FileKind::Regular)], &[]);
    let c3 = mock.seed(Some(c2), &[("a.txt", "3\n", FileKind::Regular)], &[]);
    [c1, c2, c3]
}

#[test]
fn remote_resolve_takes_suffixes_on_refs_ids_short_ids_and_at() {
    let mock = Mock::start();
    let [c1, c2, c3] = seed_chain(&mock);
    let client = Client::new(&mock.url);
    let dir = tempfile::tempdir().unwrap();
    let cache = Cache::new(dir.path());
    let r = Remote {
        client: &client,
        cache: &cache,
        base: Some(c2),
        target_ref: "main",
        json: false,
    };

    assert_eq!(r.resolve("main~1").unwrap(), c2);
    assert_eq!(r.resolve("main~2").unwrap(), c1);
    assert_eq!(r.resolve("main^").unwrap(), c2);
    assert_eq!(r.resolve("main^0").unwrap(), c3);
    assert_eq!(r.resolve("main~^").unwrap(), c1);
    assert_eq!(r.resolve(&format!("{c3}~2")).unwrap(), c1);
    let short = &c3.to_string()[..12];
    assert_eq!(r.resolve(&format!("{short}~1")).unwrap(), c2);
    assert_eq!(
        client.resolve_rev(&format!("{short}^")).unwrap(),
        (c2, RevVia::Suffix)
    );
    assert_eq!(r.resolve("@").unwrap(), c2);
    assert_eq!(r.resolve("@~1").unwrap(), c1);
    assert_eq!(r.resolve("@^0").unwrap(), c2);

    for past_root in ["main~3", "@~2", "@^2"] {
        let e = r.resolve(past_root).unwrap_err();
        assert_eq!(e.kind(), ErrorKind::NotFound, "{past_root}: {e}");
    }
    // A short id whose walk fails is reported as such, not retried.
    let e = r.resolve(&format!("{short}~9")).unwrap_err();
    assert_eq!(e.kind(), ErrorKind::NotFound, "{e}");
    for bad in ["main~x", "@~x", "main@{1}"] {
        let e = r.resolve(bad).unwrap_err();
        assert_eq!(e.kind(), ErrorKind::User, "{bad}: {e}");
    }
}

fn leep(cache: &Path, dir: &Path, server: &str, args: &[&str]) -> std::process::Output {
    let mut c = Command::new(env!("CARGO_BIN_EXE_leep"));
    for (k, _) in std::env::vars() {
        if k.starts_with("LEEP_") {
            c.env_remove(k);
        }
    }
    c.current_dir(dir)
        .env("LEEP_CACHE_DIR", cache)
        .arg("--server")
        .arg(server)
        .args(args)
        .output()
        .unwrap()
}

fn ok(out: &std::process::Output) -> String {
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).into_owned()
}

#[test]
fn the_cli_takes_suffixes_in_diff_show_and_checkout() {
    let mock = Mock::start();
    let [c1, c2, _] = seed_chain(&mock);
    let (cache, dir) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let run = |args: &[&str]| leep(cache.path(), dir.path(), &mock.url, args);

    ok(&run(&["init"]));
    let diff = ok(&run(&["diff", "main~1", "main"]));
    assert!(diff.contains("-2") && diff.contains("+3"), "{diff}");

    let v: serde_json::Value =
        serde_json::from_str(&ok(&run(&["--json", "show", "main~2"]))).expect("show --json");
    assert_eq!(v["id"], c1.to_string());

    ok(&run(&["checkout", "main~1"]));
    assert_eq!(
        std::fs::read_to_string(dir.path().join("a.txt")).unwrap(),
        "2\n"
    );
    let v: serde_json::Value =
        serde_json::from_str(&ok(&run(&["--json", "show", "@~1"]))).expect("show --json");
    assert_eq!(v["id"], c1.to_string());
    let v: serde_json::Value =
        serde_json::from_str(&ok(&run(&["--json", "show", "@"]))).expect("show --json");
    assert_eq!(v["id"], c2.to_string());

    let out = run(&["show", "@~5"]);
    assert_eq!(out.status.code(), Some(4));
    let out = run(&["show", "main~x"]);
    assert_eq!(out.status.code(), Some(1));
}
