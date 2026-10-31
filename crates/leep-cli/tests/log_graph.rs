//! `leep log --graph`: golden output against the mock server, whose log is
//! newest first by generation then id, like the real one.

mod common;

use std::collections::BTreeMap;
use std::process::{Command, Output};

use common::{Data, Mock};
use leep_core::{Changeset, ChangesetId, FileChange, FileKind, Hash, RepoPath};

/// A changeset with message `msg` named `name` in the output. With `above`,
/// its unprinted content is varied until its id sorts above that one, so
/// changesets of one generation come in the order a test expects.
fn add(
    d: &mut Data,
    names: &mut Vec<(String, ChangesetId)>,
    name: &str,
    parents: &[ChangesetId],
    above: Option<ChangesetId>,
) -> ChangesetId {
    for n in 0u32.. {
        let body = format!("{name}{n}");
        let cs = Changeset {
            parents: parents.to_vec(),
            author: "t".into(),
            timestamp: 1_700_000_000,
            message: "msg".into(),
            changes: BTreeMap::from([(
                RepoPath::new("f").unwrap(),
                FileChange::Modify {
                    content: Hash::of_blob(body.as_bytes()),
                    kind: FileKind::Regular,
                    size: body.len() as u64,
                },
            )]),
            extra: BTreeMap::new(),
        };
        if above.is_none_or(|a| cs.id() > a) {
            let id = d.insert(cs);
            names.push((name.into(), id));
            return id;
        }
    }
    unreachable!()
}

struct Repo {
    mock: Mock,
    names: Vec<(String, ChangesetId)>,
    dir: tempfile::TempDir,
}

impl Repo {
    fn new(build: impl FnOnce(&mut Data, &mut Vec<(String, ChangesetId)>) -> ChangesetId) -> Repo {
        let mock = Mock::start();
        let mut names = Vec::new();
        {
            let mut d = mock.data.lock().unwrap();
            let tip = build(&mut d, &mut names);
            d.refs.insert("main".into(), tip);
        }
        Repo {
            mock,
            names,
            dir: tempfile::tempdir().unwrap(),
        }
    }

    fn run(&self, args: &[&str]) -> Output {
        let mut c = Command::new(env!("CARGO_BIN_EXE_leep"));
        for (k, _) in std::env::vars() {
            if k.starts_with("LEEP_") || k == "NO_COLOR" || k.starts_with("CLICOLOR") {
                c.env_remove(k);
            }
        }
        c.current_dir(self.dir.path())
            .env("LEEP_CACHE_DIR", self.dir.path())
            .args(["--server", self.mock.url.as_str()])
            .args(args)
            .output()
            .unwrap()
    }

    /// stdout of a successful run, short ids replaced by names.
    fn out(&self, args: &[&str]) -> String {
        let out = self.run(args);
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
        let mut text = String::from_utf8(out.stdout).unwrap();
        for (name, id) in &self.names {
            text = text.replace(&id.to_string()[..12], name);
        }
        text
    }
}

fn linear() -> Repo {
    Repo::new(|d, n| {
        let a = add(d, n, "A", &[], None);
        let b = add(d, n, "B", &[a], None);
        add(d, n, "C", &[b], None)
    })
}

fn branch_and_merge() -> Repo {
    Repo::new(|d, n| {
        let a = add(d, n, "A", &[], None);
        let b = add(d, n, "B", &[a], None);
        let c = add(d, n, "C", &[a], Some(b));
        add(d, n, "M", &[b, c], None)
    })
}

/// main A - B - M1 - M2; side X off A merged by M1, side Y off B by M2.
fn merged_in_sequence() -> Repo {
    Repo::new(|d, n| {
        let a = add(d, n, "A", &[], None);
        let b = add(d, n, "B", &[a], None);
        let x = add(d, n, "X", &[a], Some(b));
        let m1 = add(d, n, "M1", &[b, x], None);
        let y = add(d, n, "Y", &[b], Some(m1));
        add(d, n, "M2", &[m1, y], None)
    })
}

fn octopus() -> Repo {
    Repo::new(|d, n| {
        let r = add(d, n, "R", &[], None);
        let a = add(d, n, "A", &[r], None);
        let b = add(d, n, "B", &[r], Some(a));
        let c = add(d, n, "C", &[r], Some(b));
        let dd = add(d, n, "D", &[r], Some(c));
        add(d, n, "O", &[a, b, c, dd], None)
    })
}

#[test]
fn linear_history() {
    let want = "\
* changeset C
| Author: t
| Date:   2023-11-14 22:13:20
|
|     msg
|
* changeset B
| Author: t
| Date:   2023-11-14 22:13:20
|
|     msg
|
* changeset A
  Author: t
  Date:   2023-11-14 22:13:20

      msg

";
    assert_eq!(linear().out(&["log", "--graph", "main"]), want);
    assert_eq!(
        linear().out(&["log", "--oneline", "--graph", "main"]),
        "* C msg\n* B msg\n* A msg\n"
    );
}

#[test]
fn branch_and_merge_full_entries() {
    let want = "\
*   changeset M
|\\  Author: t
| | Date:   2023-11-14 22:13:20
| |
| |     msg
| |
| * changeset C
| | Author: t
| | Date:   2023-11-14 22:13:20
| |
| |     msg
| |
* | changeset B
|/  Author: t
|   Date:   2023-11-14 22:13:20
|
|       msg
|
* changeset A
  Author: t
  Date:   2023-11-14 22:13:20

      msg

";
    let got = branch_and_merge().out(&["log", "--graph", "main"]);
    assert_eq!(got, want, "\n{got}");
}

#[test]
fn oneline_graphs() {
    let got = branch_and_merge().out(&["log", "--oneline", "--graph", "main"]);
    assert_eq!(got, "*   M msg\n|\\\n| * C msg\n* | B msg\n|/\n* A msg\n");

    let want = "\
*   M2 msg
|\\
| * Y msg
* |  M1 msg
|\\ \\
|_|/
| * X msg
* | B msg
|/
* A msg
";
    let got = merged_in_sequence().out(&["log", "--oneline", "--graph", "main"]);
    assert_eq!(got, want, "\n{got}");

    let want = "\
*---.   O msg
|\\ \\ \\
| | | * D msg
| | * | C msg
| | |/
| * | B msg
| |/
* | A msg
|/
* R msg
";
    let got = octopus().out(&["log", "--oneline", "--graph", "main"]);
    assert_eq!(got, want, "\n{got}");
}

#[test]
fn the_graph_carries_across_pages() {
    for repo in [merged_in_sequence(), octopus()] {
        let whole = repo.out(&["log", "--oneline", "--graph", "main"]);
        let full = repo.out(&["log", "--graph", "main"]);
        // Two entries a page, so every merge's lanes span a page boundary.
        repo.mock.data.lock().unwrap().page_cap = Some(2);
        assert_eq!(repo.out(&["log", "--oneline", "--graph", "main"]), whole);
        assert_eq!(repo.out(&["log", "--graph", "main"]), full);
        let paged = repo
            .mock
            .data
            .lock()
            .unwrap()
            .queries
            .iter()
            .filter(|(k, q)| k == "log" && q.contains("after="))
            .count();
        assert!(paged >= 4, "{paged} continued pages");

        // `-n` stopping mid-merge leaves the open lanes drawn.
        let cut = repo.out(&["log", "--oneline", "--graph", "-n", "3", "main"]);
        let lines: Vec<&str> = whole.lines().collect();
        let starred = lines
            .iter()
            .enumerate()
            .filter(|(_, l)| l.contains('*'))
            .nth(3)
            .map(|(i, _)| i)
            .unwrap();
        assert_eq!(cut, lines[..starred].join("\n") + "\n", "\n{cut}");
    }
}

#[test]
fn graph_refuses_filters_and_leaves_json_alone() {
    let repo = branch_and_merge();
    for filter in [
        &["--path", "f"][..],
        &["--follow", "f"],
        &["--author", "t"],
        &["--agent"],
        &["--agent-model", "m"],
        &["--agent-session", "s"],
        &["--extra", "k"],
    ] {
        let mut args = vec!["log", "--graph", "main"];
        args.extend(filter);
        let out = repo.run(&args);
        assert_eq!(out.status.code(), Some(2), "{filter:?}");
        assert!(String::from_utf8_lossy(&out.stderr).contains("--graph"));
    }
    for extra in [&[][..], &["--oneline"]] {
        let mut plain = vec!["--json", "log", "main"];
        plain.extend(extra);
        let mut graph = plain.clone();
        graph.push("--graph");
        assert_eq!(repo.out(&graph), repo.out(&plain));
    }
}
