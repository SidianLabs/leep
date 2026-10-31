//! `leep bisect` against the mock server: manual marks, `bisect run`, merge
//! history, skipped changesets, and getting back with `bisect reset`.

mod common;

use std::collections::BTreeMap;
use std::path::Path;
use std::process::{Command, Output};

use common::Mock;
use leep_core::{Changeset, ChangesetId, FileChange, FileKind, Hash, RepoPath};
use serde_json::Value;

/// Not a test: `bisect run` starts this test binary with `BISECT_HELPER`
/// set, as a portable script. Exits 1 (bad) when `a.txt` or `s.txt` in the
/// current directory says `bug`, 125 (skip) when one says `skip`, else 0;
/// `BISECT_HELPER_EXIT` forces a code.
#[test]
fn bisect_run_helper() {
    if std::env::var_os("BISECT_HELPER").is_none() {
        return;
    }
    if let Ok(code) = std::env::var("BISECT_HELPER_EXIT") {
        std::process::exit(code.parse().unwrap());
    }
    let text = ["a.txt", "s.txt"]
        .iter()
        .map(|p| std::fs::read_to_string(p).unwrap_or_default())
        .collect::<String>();
    let code = if text.contains("bug") {
        1
    } else if text.contains("skip") {
        125
    } else {
        0
    };
    std::process::exit(code);
}

/// The arguments that make `bisect run` start [`bisect_run_helper`].
fn helper_cmd() -> Vec<String> {
    vec![
        std::env::current_exe().unwrap().display().to_string(),
        "--exact".into(),
        "bisect_run_helper".into(),
        "--quiet".into(),
    ]
}

struct Wc {
    mock: Mock,
    cache: tempfile::TempDir,
    dir: tempfile::TempDir,
}

impl Wc {
    fn new(mock: Mock) -> Wc {
        let wc = Wc {
            mock,
            cache: tempfile::tempdir().unwrap(),
            dir: tempfile::tempdir().unwrap(),
        };
        ok(&wc.run(&["init"]));
        wc
    }

    fn run_env(&self, env: &[(&str, &str)], args: &[&str]) -> Output {
        let mut c = Command::new(env!("CARGO_BIN_EXE_leep"));
        for (k, _) in std::env::vars() {
            if k.starts_with("LEEP_") || k.starts_with("BISECT_HELPER") {
                c.env_remove(k);
            }
        }
        c.current_dir(self.dir.path())
            .env("LEEP_CACHE_DIR", self.cache.path())
            .envs(env.iter().copied())
            .arg("--server")
            .arg(&self.mock.url)
            .args(args)
            .output()
            .unwrap()
    }

    fn run(&self, args: &[&str]) -> Output {
        self.run_env(&[], args)
    }

    fn json(&self, args: &[&str]) -> Value {
        let mut all = vec!["--json"];
        all.extend_from_slice(args);
        serde_json::from_str(&ok(&self.run(&all))).expect("json output")
    }

    fn read(&self, path: &str) -> String {
        std::fs::read_to_string(self.dir.path().join(path)).unwrap_or_default()
    }

    fn path(&self) -> &Path {
        self.dir.path()
    }
}

fn ok(out: &Output) -> String {
    assert!(
        out.status.success(),
        "exit {:?}\nstdout: {}\nstderr: {}",
        out.status.code(),
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).into_owned()
}

fn stdout(out: &Output) -> String {
    String::from_utf8_lossy(&out.stdout).into_owned()
}

/// Store a changeset with `parents` setting `files`, without moving refs.
fn commit(mock: &Mock, parents: &[ChangesetId], files: &[(&str, &str)], msg: &str) -> ChangesetId {
    let mut d = mock.data.lock().unwrap();
    let mut changes = BTreeMap::new();
    for (p, body) in files {
        let h = Hash::of_blob(body.as_bytes());
        d.blobs.insert(h, body.as_bytes().to_vec());
        changes.insert(
            RepoPath::new(*p).unwrap(),
            FileChange::Modify {
                content: h,
                kind: FileKind::Regular,
                size: body.len() as u64,
            },
        );
    }
    d.insert(Changeset {
        parents: parents.to_vec(),
        author: "seed".into(),
        timestamp: 1,
        message: msg.into(),
        changes,
        extra: BTreeMap::new(),
    })
}

/// `main` = c[1] .. c[n] (index 0 unused); `a.txt` says `bug` from
/// `culprit` on, and `skip` in the changesets listed in `skips`.
fn linear(mock: &Mock, n: usize, culprit: usize, skips: &[usize]) -> Vec<ChangesetId> {
    let mut ids = vec![ChangesetId(Hash::of_blob(b"unused"))];
    for i in 1..=n {
        let body = if i >= culprit {
            format!("bug {i}\n")
        } else if skips.contains(&i) {
            format!("skip {i}\n")
        } else {
            format!("ok {i}\n")
        };
        let parents: Vec<ChangesetId> = if i == 1 { vec![] } else { vec![ids[i - 1]] };
        ids.push(commit(
            mock,
            &parents,
            &[("a.txt", &body)],
            &format!("change {i}"),
        ));
    }
    mock.data.lock().unwrap().refs.insert("main".into(), ids[n]);
    ids
}

fn id_of(v: &Value) -> ChangesetId {
    v.as_str().unwrap().parse().unwrap()
}

/// Mark the checked-out changeset with `judge` until the bisect ends;
/// returns the last answer and how many marks it took.
fn drive(wc: &Wc, first: Value, judge: impl Fn(&Wc) -> &'static str) -> (Value, usize) {
    let mut v = first;
    let mut marks = 0;
    while v["status"] == "testing" {
        assert_eq!(id_of(&v["current"]), id_of(&wc.json(&["status"])["base"]));
        marks += 1;
        let out = wc.run(&["--json", "bisect", judge(wc)]);
        v = serde_json::from_str(&stdout(&out)).expect("json");
    }
    (v, marks)
}

fn judge_bug(wc: &Wc) -> &'static str {
    if wc.read("a.txt").contains("bug") || wc.read("s.txt").contains("bug") {
        "bad"
    } else {
        "good"
    }
}

#[test]
fn manual_marks_converge_on_a_linear_history() {
    let mock = Mock::start();
    let c = linear(&mock, 20, 13, &[]);
    let wc = Wc::new(mock);
    let start = wc.run(&["bisect", "start", "main", &c[1].to_string()]);
    let text = ok(&start);
    assert!(
        text.starts_with("Bisecting: ")
            && text.contains("revisions left to test after this (roughly "),
        "{text}"
    );
    let v = wc.json(&["bisect", "log"]);
    assert_eq!(id_of(&v["bad"]), c[20]);

    // `start` checked out the first candidate.
    let first = serde_json::json!({"status": "testing", "current": wc.json(&["status"])["base"]});
    let (v, marks) = drive(&wc, first, judge_bug);
    assert_eq!(v["status"], "found");
    assert_eq!(id_of(&v["first_bad"]["id"]), c[13]);
    assert!(marks <= 5, "{marks} marks");

    // Asking again prints the answer the human way, with the show summary.
    let out = ok(&wc.run(&["bisect", "bad", &c[13].to_string()]));
    assert!(
        out.starts_with(&format!("{} is the first bad changeset\n", c[13])),
        "{out}"
    );
    assert!(out.contains("change 13") && out.contains("a.txt"), "{out}");
    let log = ok(&wc.run(&["bisect", "log"]));
    assert!(log.contains(&format!("leep bisect bad {}", c[20])), "{log}");
    assert!(
        log.contains(&format!("# first bad changeset: [{}] change 13", c[13])),
        "{log}"
    );
}

#[test]
fn bisect_run_finds_the_culprit_and_reads_skip_and_abort_codes() {
    let mock = Mock::start();
    let c = linear(&mock, 20, 13, &[10, 11]);
    let wc = Wc::new(mock);
    ok(&wc.run(&["bisect", "start", "main", &c[1].to_string()]));
    let mut args = vec!["--json", "bisect", "run"];
    let cmd = helper_cmd();
    args.extend(cmd.iter().map(String::as_str));
    let out = wc.run_env(&[("BISECT_HELPER", "1")], &args);
    let v: Value = serde_json::from_str(&ok(&out)).expect("json");
    assert_eq!(v["status"], "found", "{v}");
    assert_eq!(id_of(&v["first_bad"]["id"]), c[13]);
    let runs = v["runs"].as_array().unwrap();
    assert!(!runs.is_empty() && runs.len() <= 6, "{v}");
    for r in runs {
        let id = id_of(&r["id"]);
        let i = c.iter().position(|x| *x == id).unwrap();
        let want = match i {
            10 | 11 => "skip",
            i if i >= 13 => "bad",
            _ => "good",
        };
        assert_eq!(r["result"], want, "{r}");
    }

    // 128 and above stops the bisect, like git, and keeps the marks.
    ok(&wc.run(&["bisect", "reset"]));
    ok(&wc.run(&["bisect", "start", "main", &c[1].to_string()]));
    let mut args = vec!["bisect", "run"];
    args.extend(cmd.iter().map(String::as_str));
    let out = wc.run_env(
        &[("BISECT_HELPER", "1"), ("BISECT_HELPER_EXIT", "200")],
        &args,
    );
    assert_eq!(out.status.code(), Some(1));
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(
        err.contains("exit code 200") && err.contains(">= 128"),
        "{err}"
    );
    assert!(wc.path().join(".leep/bisect.json").exists());
}

#[test]
fn a_culprit_on_a_merged_branch_is_found_not_the_merge() {
    //  c1 - c2 - c3 - c4 ------ m - c5 - c6      (main)
    //         \                /
    //          s1 - s2 - s3 --'                   s2 brings the bug
    let mock = Mock::start();
    let c1 = commit(&mock, &[], &[("m.txt", "m1\n"), ("s.txt", "s0\n")], "c1");
    let c2 = commit(&mock, &[c1], &[("m.txt", "m2\n")], "c2");
    let c3 = commit(&mock, &[c2], &[("m.txt", "m3\n")], "c3");
    let c4 = commit(&mock, &[c3], &[("m.txt", "m4\n")], "c4");
    let s1 = commit(&mock, &[c2], &[("s.txt", "s1\n")], "s1");
    let s2 = commit(&mock, &[s1], &[("s.txt", "bug s2\n")], "s2");
    let s3 = commit(&mock, &[s2], &[("s.txt", "bug s3\n")], "s3");
    let m = commit(&mock, &[c4, s3], &[("s.txt", "bug s3\n")], "merge side");
    let c5 = commit(&mock, &[m], &[("m.txt", "m5\n")], "c5");
    let c6 = commit(&mock, &[c5], &[("m.txt", "m6\n")], "c6");
    mock.data.lock().unwrap().refs.insert("main".into(), c6);
    let wc = Wc::new(mock);

    let first = wc.json(&["bisect", "start", "main", &c1.to_string()]);
    let (v, marks) = drive(&wc, first, judge_bug);
    assert_eq!(v["status"], "found", "{v}");
    assert_eq!(id_of(&v["first_bad"]["id"]), s2, "found {v}");
    assert!(marks <= 4, "{marks}");

    // Revisions take suffixes: `main~3` is c4, so only the side is left.
    ok(&wc.run(&["bisect", "reset"]));
    let first = wc.json(&["bisect", "start", "main", "main~3", &s1.to_string()]);
    assert_eq!(first["status"], "testing");
    let (v, _) = drive(&wc, first, judge_bug);
    assert_eq!(id_of(&v["first_bad"]["id"]), s2);
}

#[test]
fn skipping_next_to_the_culprit_leaves_a_choice() {
    let mock = Mock::start();
    let c = linear(&mock, 20, 13, &[]);
    let wc = Wc::new(mock);
    let skipped = c[12];
    let mut out = wc.run(&["bisect", "start", "main", &c[1].to_string()]);
    let mut marks = 0;
    while stdout(&out).starts_with("Bisecting:") {
        marks += 1;
        assert!(marks < 10, "does not end");
        let base = id_of(&wc.json(&["status"])["base"]);
        let verb = if base == skipped {
            "skip"
        } else if wc.read("a.txt").contains("bug") {
            "bad"
        } else {
            "good"
        };
        out = wc.run(&["bisect", verb]);
    }
    let text = stdout(&out);
    assert_eq!(out.status.code(), Some(1), "{text}");
    assert!(
        text.contains("There are only 'skip'ped changesets left to test.")
            && text.contains("The first bad changeset could be any of:")
            && text.contains("We cannot bisect more!"),
        "{text}"
    );
    assert!(
        text.contains(&c[12].to_string()) && text.contains(&c[13].to_string()),
        "{text}"
    );
    let v: Value = serde_json::from_str(&stdout(&wc.run(&[
        "--json",
        "bisect",
        "skip",
        &c[12].to_string(),
    ])))
    .unwrap();
    assert_eq!(v["status"], "only_skipped");
    assert_eq!(v["candidates"].as_array().unwrap().len(), 2);
}

#[test]
fn reset_restores_the_start_and_commit_refuses_meanwhile() {
    let mock = Mock::start();
    let c = linear(&mock, 8, 5, &[]);
    mock.data.lock().unwrap().refs.insert("dev".into(), c[8]);
    let wc = Wc::new(mock);
    ok(&wc.run(&["switch", "dev"]));
    std::fs::write(wc.path().join("untracked.txt"), "mine\n").unwrap();
    // A clean working copy is needed to start.
    let out = wc.run(&["bisect", "start", "main", &c[1].to_string()]);
    assert_eq!(out.status.code(), Some(1));
    std::fs::remove_file(wc.path().join("untracked.txt")).unwrap();

    let v = wc.json(&["bisect", "start"]);
    assert_eq!(v["status"], "waiting");
    let v = wc.json(&["bisect", "bad"]);
    assert_eq!(v["status"], "waiting");
    let v = wc.json(&["bisect", "good", &c[1].to_string()]);
    assert_eq!(v["status"], "testing");
    assert_ne!(wc.read("a.txt"), "bug 8\n");
    // Someone switches the target ref meanwhile; reset puts it back.
    ok(&wc.run(&["switch", "main"]));

    let st = ok(&wc.run(&["status"]));
    assert!(
        st.contains("bisecting: bad ") && st.contains("leep bisect reset"),
        "{st}"
    );
    let v = wc.json(&["status"]);
    assert_eq!(id_of(&v["bisecting"]["bad"]), c[8]);
    assert_eq!(v["bisecting"]["original_ref"], "dev");

    std::fs::write(wc.path().join("new.txt"), "x\n").unwrap();
    for args in [&["commit", "-m", "x"][..], &["land"][..], &["sync"][..]] {
        let out = wc.run(args);
        assert_eq!(out.status.code(), Some(1), "{args:?}");
        let err = String::from_utf8_lossy(&out.stderr);
        assert!(err.contains("leep bisect reset"), "{args:?}: {err}");
    }
    // Edits to tracked files would be lost by the next checkout.
    std::fs::write(wc.path().join("a.txt"), "edited\n").unwrap();
    let out = wc.run(&["bisect", "good"]);
    assert_eq!(out.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&out.stderr).contains("a.txt"));
    ok(&wc.run(&["restore", "a.txt"]));

    let v = wc.json(&["bisect", "reset"]);
    assert_eq!(id_of(&v["base"]), c[8]);
    assert_eq!(v["target_ref"], "dev");
    assert_eq!(wc.read("a.txt"), "bug 8\n");
    assert_eq!(wc.read("new.txt"), "x\n", "untracked files stay");
    let v = wc.json(&["status"]);
    assert_eq!(id_of(&v["base"]), c[8]);
    assert_eq!(v["target_ref"], "dev");
    assert!(v.get("bisecting").is_none());
    assert!(!wc.path().join(".leep/bisect.json").exists());
    let out = wc.run(&["bisect", "reset"]);
    assert_eq!(out.status.code(), Some(6));
    std::fs::remove_file(wc.path().join("new.txt")).unwrap();
    // Committing works again.
    std::fs::write(wc.path().join("b.txt"), "b\n").unwrap();
    ok(&wc.run(&["commit", "-m", "after bisect"]));
}

#[test]
fn marks_in_the_wrong_order_are_refused_without_saving() {
    let mock = Mock::start();
    let c = linear(&mock, 6, 4, &[]);
    let wc = Wc::new(mock);
    let out = wc.run(&["bisect", "start", &c[2].to_string(), "main"]);
    assert_eq!(out.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&out.stderr).contains("wrong way round"));
    assert!(!wc.path().join(".leep/bisect.json").exists());
}
