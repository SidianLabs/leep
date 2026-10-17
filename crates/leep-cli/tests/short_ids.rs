//! Short changeset ids (ADR 0034): accepted wherever a revision is, printed
//! by `log`, resolved by the server's prefix lookup.

mod common;

use std::collections::BTreeMap;
use std::path::Path;
use std::process::Command;

use common::Mock;
use leep_cli::cache::Cache;
use leep_cli::client::Client;
use leep_cli::commands::Remote;
use leep_cli::error::ErrorKind;
use leep_core::{ChangesetId, FileKind};

/// Seed changesets on `main` until two ids share their first four hex
/// digits; returns them in ascending order.
fn seed_shared_prefix(mock: &Mock) -> (ChangesetId, ChangesetId) {
    let mut seen: BTreeMap<String, ChangesetId> = BTreeMap::new();
    let mut parent = None;
    loop {
        let n = seen.len();
        let id = mock.seed_as(
            "main",
            &format!("change {n}"),
            parent,
            &[("a.txt", &format!("{n}\n"), FileKind::Regular)],
            &[],
        );
        parent = Some(id);
        if let Some(other) = seen.insert(id.to_string()[..4].to_string(), id) {
            return (other.min(id), other.max(id));
        }
    }
}

fn remote<'a>(client: &'a Client, cache: &'a Cache) -> Remote<'a> {
    Remote {
        client,
        cache,
        base: None,
        target_ref: "main",
        json: false,
    }
}

#[test]
fn resolve_accepts_unique_prefixes_and_explains_the_rest() {
    let mock = Mock::start();
    let (a, b) = seed_shared_prefix(&mock);
    mock.data.lock().unwrap().refs.insert("cafe".into(), b);
    let client = Client::new(&mock.url);
    let dir = tempfile::tempdir().unwrap();
    let cache = Cache::new(dir.path());
    let r = remote(&client, &cache);

    let hex = a.to_string();
    assert_eq!(r.resolve(&hex[..12]).unwrap(), a, "unique");
    assert_eq!(r.resolve(&hex[..12].to_uppercase()).unwrap(), a, "any case");
    assert_eq!(r.resolve(&hex).unwrap(), a, "full id");
    assert_eq!(r.resolve("cafe").unwrap(), b, "a ref wins over a short id");

    let e = r.resolve(&hex[..4]).unwrap_err();
    assert_eq!(e.kind(), ErrorKind::Ambiguous, "{e}");
    assert_eq!(e.exit_code(), 1);
    assert_eq!(e.candidates(), [a.to_string(), b.to_string()]);
    assert!(e.message().contains("ambiguous"), "{e}");

    let e = r.resolve("0123456789ab").unwrap_err();
    let unknown_is_missing = !hex.starts_with("0123") && !b.to_string().starts_with("0123");
    if unknown_is_missing {
        assert_eq!(e.kind(), ErrorKind::NotFound, "{e}");
        assert!(e.hint().unwrap().contains("4 hex digits"), "{e:?}");
    }
    for short_or_not_hex in [&hex[..3], "zzzz"] {
        let e = r.resolve(short_or_not_hex).unwrap_err();
        assert_eq!(e.kind(), ErrorKind::NotFound, "{short_or_not_hex}: {e}");
        assert!(e.hint().unwrap().contains("leep refs"), "{e:?}");
    }
}

fn leep(cache: &Path, dir: &Path, args: &[&str]) -> std::process::Output {
    let mut c = Command::new(env!("CARGO_BIN_EXE_leep"));
    for (k, _) in std::env::vars() {
        if k.starts_with("LEEP_") {
            c.env_remove(k);
        }
    }
    c.current_dir(dir)
        .env("LEEP_CACHE_DIR", cache)
        .args(args)
        .output()
        .unwrap()
}

#[test]
fn the_cli_prints_short_ids_in_log_and_takes_them_back() {
    let mock = Mock::start();
    let (a, b) = seed_shared_prefix(&mock);
    let (cache, dir) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let run = |args: &[&str]| {
        let mut all = vec!["--server", mock.url.as_str()];
        all.extend_from_slice(args);
        leep(cache.path(), dir.path(), &all)
    };

    let out = run(&["log", "-n", "1"]);
    let text = String::from_utf8_lossy(&out.stdout).into_owned();
    let first = text.lines().next().unwrap();
    let short = first.strip_prefix("changeset ").unwrap();
    assert_eq!(short.len(), 12, "{first}");
    let head = mock.data.lock().unwrap().refs["main"];
    assert_eq!(short, &head.to_string()[..12]);
    // `--json` keeps full ids.
    let out = run(&["--json", "log", "-n", "1"]);
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(v["entries"][0]["id"], head.to_string());

    // What log printed works as a revision.
    let out = run(&["--json", "show", short]);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(v["id"], head.to_string());
    let out = run(&["blame", &a.to_string()[..10], "a.txt"]);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );

    // Ambiguous: exit 1, the candidates on stderr and in the JSON error.
    let shared = &a.to_string()[..4];
    let out = run(&["show", shared]);
    assert_eq!(out.status.code(), Some(1));
    let err = String::from_utf8_lossy(&out.stderr).into_owned();
    assert!(
        err.contains(&a.to_string()) && err.contains(&b.to_string()),
        "{err}"
    );
    let out = run(&["--json", "show", shared]);
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(v["error"]["code"], "ambiguous");
    assert_eq!(
        v["error"]["candidates"],
        serde_json::json!([a.to_string(), b.to_string()])
    );

    // Too short and not hex are unknown revisions.
    for rev in [&a.to_string()[..3], "zzzz"] {
        let out = run(&["show", rev]);
        assert_eq!(out.status.code(), Some(4), "{rev}");
    }
}
