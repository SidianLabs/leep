//! Reading changes and reviews: `diff REV`, `show -p`, the quiet default
//! `log`, review verdicts that say where they leave a changeset, conflict
//! markers that explain themselves, and the provenance a land's conflict
//! resolution records. Run against the in-memory mock server.

mod common;

use std::path::Path;
use std::process::{Command, Output};

use common::Mock;
use leep_core::changeset::EXTRA_REBASED_FROM;
use leep_core::{ChangesetId, FileKind};

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

fn head(mock: &Mock) -> ChangesetId {
    mock.data.lock().unwrap().refs["main"]
}

#[test]
fn diff_of_one_revision_is_its_own_change() {
    let leep = Leep::new();
    let mock = Mock::start();
    let root = mock.seed(None, &[("a.txt", "one\n", FileKind::Regular)], &[]);
    let second = mock.seed(Some(root), &[("a.txt", "two\n", FileKind::Regular)], &[]);
    let wc = init(&leep, &mock);

    let one = leep.ok(wc.path(), &["diff", &second.to_string()[..12]]);
    let two = leep.ok(wc.path(), &["diff", &root.to_string(), &second.to_string()]);
    assert_eq!(one, two);
    assert!(one.contains("-one\n+two\n"), "{one}");
    // `main` resolves like any revision, and a root diffs against nothing.
    assert_eq!(leep.ok(wc.path(), &["diff", "main"]), one);
    let first = leep.ok(wc.path(), &["diff", &root.to_string()]);
    assert!(
        first.contains("--- /dev/null") && first.contains("+one"),
        "{first}"
    );
    let out = leep.run(wc.path(), &["diff", "nope"]);
    assert_eq!(out.status.code(), Some(4), "{}", text(&out.stderr));
}

#[test]
fn show_puts_the_parent_in_the_header_and_p_adds_the_patch() {
    let leep = Leep::new();
    let mock = Mock::start();
    let root = mock.seed(None, &[("a.txt", "one\n", FileKind::Regular)], &[]);
    let second = mock.seed(Some(root), &[("a.txt", "two\n", FileKind::Regular)], &[]);
    let wc = init(&leep, &mock);

    let shown = leep.ok(wc.path(), &["show", "main"]);
    assert!(
        shown.starts_with(&format!("changeset {second}\nParent: {root}\nAuthor: ")),
        "{shown}"
    );
    assert!(
        shown.contains("M a.txt") && !shown.contains("+two"),
        "{shown}"
    );
    let patched = leep.ok(wc.path(), &["show", "-p", "main"]);
    assert!(patched.starts_with(&shown), "{patched}");
    assert!(patched.contains("-one\n+two\n"), "{patched}");

    let json: serde_json::Value =
        serde_json::from_str(&leep.ok(wc.path(), &["--json", "show", "-p", "main"])).unwrap();
    assert_eq!(json["id"], second.to_string());
    assert!(json["patch"].as_str().unwrap().contains("+two"), "{json}");
    let plain: serde_json::Value =
        serde_json::from_str(&leep.ok(wc.path(), &["--json", "show", "main"])).unwrap();
    assert!(plain.get("patch").is_none());
}

#[test]
fn log_hides_git_headers_and_message_bodies_unless_verbose() {
    let leep = Leep::new();
    let mock = Mock::start();
    let root = mock.seed(None, &[("a.txt", "one\n", FileKind::Regular)], &[]);
    let signature = "-----BEGIN PGP SIGNATURE-----\n".to_string() + &"x".repeat(4000);
    let imported = {
        let mut d = mock.data.lock().unwrap();
        let mut cs = d.changesets[&root].clone();
        cs.parents = vec![root];
        cs.changes.clear();
        cs.message = "subject line\n\nA long body\nthat goes on.\n".into();
        cs.extra
            .insert("git.extra_headers".into(), signature.clone());
        cs.extra.insert("agent.model".into(), "m".into());
        let id = d.insert(cs);
        d.set_ref("main", Some(id), "cas", None);
        id
    };
    let wc = init(&leep, &mock);

    let quiet = leep.ok(wc.path(), &["log", "-n", "1"]);
    assert!(quiet.contains("agent.model: m\n"), "{quiet}");
    assert!(
        !quiet.contains("git.extra_headers") && !quiet.contains("PGP"),
        "{quiet}"
    );
    assert!(quiet.contains("    subject line\n    [...]\n"), "{quiet}");
    assert!(!quiet.contains("A long body"), "{quiet}");

    let full = leep.ok(wc.path(), &["log", "-n", "1", "-v"]);
    assert!(full.contains("git.extra_headers: -----BEGIN PGP"), "{full}");
    assert!(
        full.contains("    A long body\n") && !full.contains("[...]"),
        "{full}"
    );
    assert!(full.len() > quiet.len() + 4000);

    // `--json` is the raw response either way.
    let json: serde_json::Value =
        serde_json::from_str(&leep.ok(wc.path(), &["--json", "log", "-n", "1"])).unwrap();
    assert_eq!(json["entries"][0]["id"], imported.to_string());
    assert_eq!(
        json["entries"][0]["changeset"]["extra"]["git.extra_headers"],
        signature
    );
    // `show` prints everything.
    assert!(leep
        .ok(wc.path(), &["show", "main"])
        .contains("git.extra_headers"));
}

#[test]
fn a_verdict_says_where_it_leaves_the_changeset() {
    let leep = Leep::new();
    let mock = Mock::start();
    let root = mock.seed(None, &[("a.txt", "one\n", FileKind::Regular)], &[]);
    let wc = init(&leep, &mock);
    let id = root.to_string();

    let out = leep.run(wc.path(), &["review", "status", &id]);
    assert_eq!(out.status.code(), Some(1));
    let status = text(&out.stdout);
    assert!(
        status.contains("PENDING  a.txt  needs an approval from any reviewer"),
        "{status}"
    );
    assert!(
        status.ends_with(&format!("diff: leep diff {}\n", &id[..12])),
        "{status}"
    );

    let rejected = leep.ok(wc.path(), &["review", "reject", &id, "-m", "no"]);
    assert!(rejected.starts_with("Rejected "), "{rejected}");
    assert!(
        rejected.contains("not yet approved for main:\n  REJECTED a.txt  rejected by mock"),
        "{rejected}"
    );
    let approved = leep.ok(wc.path(), &["review", "approve", &id]);
    assert!(
        approved.ends_with("as mock\nnow approved for main\n"),
        "{approved}"
    );
    let status = leep.ok(wc.path(), &["review", "status", &id]);
    assert!(
        status.contains("ok       a.txt  approved by mock (any reviewer)"),
        "{status}"
    );
    // `--json` stays the server's answer.
    let json: serde_json::Value =
        serde_json::from_str(&leep.ok(wc.path(), &["--json", "review", "approve", &id])).unwrap();
    assert_eq!(
        json,
        serde_json::json!({"reviewer": "mock", "verdict": "approve"})
    );
}

/// A verdict on a changeset main already holds still counts for copies,
/// and says so; its status says where it landed instead of "not approved".
#[test]
fn a_verdict_on_a_landed_changeset_says_it_landed() {
    let leep = Leep::new();
    let mock = Mock::start();
    let root = mock.seed(None, &[("a.txt", "one\n", FileKind::Regular)], &[]);
    let original = mock.seed(None, &[("a.txt", "zero\n", FileKind::Regular)], &[]);
    let wc = init(&leep, &mock);
    let id = root.to_string();
    {
        let mut d = mock.data.lock().unwrap();
        d.report_on_refs = true;
        d.taken_back = vec![original];
        d.refs.insert("main".into(), root);
    }
    // Landed: nothing left to wait for, so exit 0 and `--wait` returns at once.
    let status = leep.ok(wc.path(), &["review", "status", &id]);
    assert!(
        status.starts_with(&format!(
            "changeset {} on main: landed on main\n",
            &id[..12]
        )),
        "{status}"
    );
    leep.ok(wc.path(), &["review", "status", &id, "--wait=5"]);

    let out = leep.ok(wc.path(), &["review", "reject", &id, "-m", "oops"]);
    assert_eq!(
        out,
        format!(
            "Rejected {} as mock\n\
             already on main: verdicts now only affect copies (cherry-picks, rebases)\n\
             also rejected on {} (your approval of it carried here)\n",
            &id[..12],
            &original.to_string()[..12]
        )
    );
    let out = leep.ok(wc.path(), &["review", "withdraw", &id]);
    assert!(out.contains("also withdrawn on"), "{out}");
}

/// `review status --wait` asks until a verdict comes: approved is exit 0,
/// a rejection stops it at once, and running out of time is exit 1.
#[test]
fn review_status_waits_for_the_verdict() {
    use leep_core::review::Verdict;
    let leep = Leep::new();
    let mock = Mock::start();
    let root = mock.seed(None, &[("a.txt", "one\n", FileKind::Regular)], &[]);
    let wc = init(&leep, &mock);
    let id = root.to_string();

    // Out of time: exit 1, after printing where it stands.
    let out = leep.run(wc.path(), &["review", "status", &id, "--wait=1"]);
    assert_eq!(out.status.code(), Some(1));
    assert!(
        text(&out.stdout).contains("not approved"),
        "{}",
        text(&out.stdout)
    );
    let err = text(&out.stderr);
    assert!(
        err.contains("waiting for approval of") && err.contains("timed out after"),
        "{err}"
    );

    // An approval arriving while it waits ends it with exit 0.
    let data = mock.data.clone();
    let approver = std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_millis(500));
        data.lock().unwrap().reviews.push((root, Verdict::Approve));
    });
    let out = leep.run(wc.path(), &["review", "status", &id, "--wait=60"]);
    approver.join().unwrap();
    assert_eq!(out.status.code(), Some(0), "{}", text(&out.stderr));
    assert!(
        text(&out.stdout).contains(": approved"),
        "{}",
        text(&out.stdout)
    );

    // A rejection is an answer too: no point waiting for the clock.
    leep.ok(wc.path(), &["review", "reject", &id, "-m", "no"]);
    let started = std::time::Instant::now();
    let out = leep.run(wc.path(), &["review", "status", &id, "--wait"]);
    assert_eq!(out.status.code(), Some(1));
    assert!(started.elapsed() < std::time::Duration::from_secs(30));
    assert!(
        text(&out.stderr).contains("stopped waiting"),
        "{}",
        text(&out.stderr)
    );
}

/// Two working copies change the same line from `base`; the second land
/// conflicts. Returns the losing working copy.
fn conflict(leep: &Leep, mock: &Mock, base: &str) -> tempfile::TempDir {
    conflict_after(leep, mock, base, false)
}

/// [`conflict`]; with `stack`, the loser commits another file first.
fn conflict_after(leep: &Leep, mock: &Mock, base: &str, stack: bool) -> tempfile::TempDir {
    mock.seed(None, &[("a.txt", base, FileKind::Regular)], &[]);
    let (a, b) = (init(leep, mock), init(leep, mock));
    if stack {
        std::fs::write(b.path().join("b.txt"), "more\n").unwrap();
        leep.ok(b.path(), &["commit", "-m", "b1"]);
    }
    std::fs::write(a.path().join("a.txt"), "from a\n").unwrap();
    std::fs::write(b.path().join("a.txt"), "from b\n").unwrap();
    leep.ok(a.path(), &["commit", "-m", "a"]);
    leep.ok(b.path(), &["commit", "-m", "b"]);
    leep.ok(a.path(), &["land"]);
    b
}

#[test]
fn conflict_markers_drop_an_empty_base_and_explain_the_sections() {
    let leep = Leep::new();
    let mock = Mock::start();
    mock.data.lock().unwrap().diff3 = true;
    let b = conflict(&leep, &mock, "base\n");
    let out = leep.run(b.path(), &["land"]);
    assert_eq!(out.status.code(), Some(5));
    let report = text(&out.stdout);
    assert!(
        report.contains("from `<<<<<<< main (landed)` to `=======` is main (landed)"),
        "{report}"
    );
    assert!(report.contains("`||||||| base` section"), "{report}");
    assert_eq!(
        std::fs::read_to_string(b.path().join("a.txt")).unwrap(),
        "<<<<<<< main (landed)\nfrom a\n||||||| base\nbase\n=======\nfrom b\n>>>>>>> yours\n"
    );

    // Deleting only the first marker line still counts as unresolved.
    std::fs::write(
        b.path().join("a.txt"),
        "from a\n||||||| base\nbase\n=======\nfrom b\n",
    )
    .unwrap();
    assert!(leep.ok(b.path(), &["status"]).contains("C a.txt"));
    assert_eq!(
        leep.run(b.path(), &["commit", "-m", "x"]).status.code(),
        Some(5)
    );
    std::fs::write(b.path().join("a.txt"), "from b\n>>>>>>> yours\n").unwrap();
    assert_eq!(
        leep.run(b.path(), &["commit", "-m", "x"]).status.code(),
        Some(5)
    );
    // A heading underline is not a marker.
    std::fs::write(b.path().join("a.txt"), "Title\n=======\nfrom b\n").unwrap();
    leep.ok(b.path(), &["commit", "-m", "x"]);

    // Both sides added to an empty file: no base section at all.
    let mock = Mock::start();
    mock.data.lock().unwrap().diff3 = true;
    let b = conflict(&leep, &mock, "");
    assert_eq!(leep.run(b.path(), &["land"]).status.code(), Some(5));
    assert_eq!(
        std::fs::read_to_string(b.path().join("a.txt")).unwrap(),
        "<<<<<<< main (landed)\nfrom a\n=======\nfrom b\n>>>>>>> yours\n"
    );
}

#[test]
fn the_resolution_of_a_land_names_what_it_replaces() {
    let leep = Leep::new();
    let mock = Mock::start();
    // A stack of two: both are what the land tried to add.
    let b = conflict_after(&leep, &mock, "base\n", true);
    let tip: ChangesetId = text(&leep.run(b.path(), &["--json", "status"]).stdout)
        .parse::<serde_json::Value>()
        .unwrap()["base"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    let first = mock.data.lock().unwrap().changesets[&tip].parents[0];

    let out = leep.run(b.path(), &["land"]);
    assert_eq!(out.status.code(), Some(5));
    assert!(
        text(&out.stdout).contains("replaces 2 changesets"),
        "{}",
        text(&out.stdout)
    );
    // Two messages, so none to reuse.
    assert!(
        text(&out.stdout).contains("then run `leep commit -m MESSAGE`"),
        "{}",
        text(&out.stdout)
    );
    let onto = head(&mock);
    let state: serde_json::Value =
        serde_json::from_slice(&std::fs::read(b.path().join(".leep/state.json")).unwrap()).unwrap();
    assert_eq!(
        state["resolving"],
        serde_json::json!({
            "from": [tip.to_string(), first.to_string()],
            "target_ref": "main",
            "onto": onto.to_string(),
        })
    );
    let status: serde_json::Value =
        serde_json::from_str(&leep.ok(b.path(), &["--json", "status"])).unwrap();
    assert_eq!(status["resolving"], state["resolving"]);
    assert!(leep
        .ok(b.path(), &["status"])
        .contains("resolving the land of"));

    std::fs::write(b.path().join("a.txt"), "both\n").unwrap();
    leep.ok(b.path(), &["resolve", "a.txt"]);
    let id: ChangesetId = leep
        .ok(b.path(), &["commit", "-m", "b, resolved"])
        .trim()
        .parse()
        .unwrap();
    let cs = mock.data.lock().unwrap().changesets[&id].clone();
    assert_eq!(cs.parents, [onto]);
    assert_eq!(cs.extra[EXTRA_REBASED_FROM], format!("{tip},{first}"));

    // A second commit on top is not a resolution.
    std::fs::write(b.path().join("c.txt"), "c\n").unwrap();
    let next: ChangesetId = leep
        .ok(b.path(), &["commit", "-m", "after"])
        .trim()
        .parse()
        .unwrap();
    assert!(!mock.data.lock().unwrap().changesets[&next]
        .extra
        .contains_key(EXTRA_REBASED_FROM));

    assert!(leep.ok(b.path(), &["land"]).contains("Landed"));
    let state: serde_json::Value =
        serde_json::from_slice(&std::fs::read(b.path().join(".leep/state.json")).unwrap()).unwrap();
    assert!(state.get("resolving").is_none(), "{state}");
}

/// One changeset replaced: its resolution commit reuses its message.
#[test]
fn the_resolution_of_one_changeset_defaults_to_its_message() {
    let leep = Leep::new();
    let mock = Mock::start();
    let b = conflict(&leep, &mock, "base\n");
    let out = leep.run(b.path(), &["land"]);
    assert_eq!(out.status.code(), Some(5));
    assert!(
        text(&out.stdout).contains("then run `leep commit` (its message by default)"),
        "{}",
        text(&out.stdout)
    );
    let state: serde_json::Value =
        serde_json::from_slice(&std::fs::read(b.path().join(".leep/state.json")).unwrap()).unwrap();
    assert_eq!(state["resolving"]["message"], "b");

    std::fs::write(b.path().join("a.txt"), "both\n").unwrap();
    leep.ok(b.path(), &["resolve", "a.txt"]);
    let id: ChangesetId = leep.ok(b.path(), &["commit"]).trim().parse().unwrap();
    let cs = mock.data.lock().unwrap().changesets[&id].clone();
    assert_eq!(cs.message, "b");
    assert!(cs.extra.contains_key(EXTRA_REBASED_FROM));
    // Past the resolution, a commit needs its own message again.
    std::fs::write(b.path().join("c.txt"), "c\n").unwrap();
    let out = leep.run(b.path(), &["commit"]);
    assert_eq!(out.status.code(), Some(1));
    assert!(
        text(&out.stderr).contains("message is required"),
        "{}",
        text(&out.stderr)
    );
}

#[test]
fn a_checkout_forgets_the_conflicted_land() {
    let leep = Leep::new();
    let mock = Mock::start();
    let b = conflict(&leep, &mock, "base\n");
    assert_eq!(leep.run(b.path(), &["land"]).status.code(), Some(5));
    leep.ok(b.path(), &["reset", "--hard", "--force", "main"]);
    let state: serde_json::Value =
        serde_json::from_slice(&std::fs::read(b.path().join(".leep/state.json")).unwrap()).unwrap();
    assert!(state.get("resolving").is_none(), "{state}");
}
