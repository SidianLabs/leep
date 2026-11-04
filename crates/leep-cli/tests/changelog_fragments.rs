//! Changelog fragments (ADR 0088): every `changelog.d/<slug>.md` in the
//! repository is well formed, and `scripts/changelog-assemble.sh` moves them
//! into CHANGELOG.md. Not about the CLI: this crate's tests are simply where
//! `cargo test --workspace` (and so CI, on every platform) picks the check up.

use std::fs;
use std::path::{Path, PathBuf};

fn repo() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// The rules `changelog-assemble.sh --check` applies, for platforms
/// without bash.
fn problems(name: &str, text: &str) -> Vec<&'static str> {
    let mut out = Vec::new();
    let stem = name.strip_suffix(".md").unwrap_or(name);
    let ok_name = stem
        .chars()
        .next()
        .is_some_and(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
        && stem
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '.');
    if !ok_name {
        out.push("bad name");
    }
    if text.is_empty() {
        out.push("empty");
        return out;
    }
    if !text.ends_with('\n') {
        out.push("no final newline");
    }
    let mut lines = text.lines();
    if !lines
        .next()
        .is_some_and(|l| l.starts_with("### ") && !l[4..].starts_with(' ') && l.len() > 4)
    {
        out.push("first line is not '### Title'");
    }
    let heading = |l: &&str| {
        let hashes = l.chars().take_while(|c| *c == '#').count();
        (1..=3).contains(&hashes) && matches!(l[hashes..].chars().next(), None | Some(' '))
    };
    if text.lines().filter(heading).count() != 1 {
        out.push("not exactly one heading of level 1 to 3");
    }
    if !lines.any(|l| !l.trim().is_empty()) {
        out.push("no entry");
    }
    if text.lines().any(|l| {
        ["<<<<<<<", "|||||||", ">>>>>>>"]
            .iter()
            .any(|m| l == *m || l.starts_with(&format!("{m} ")))
    }) {
        out.push("conflict markers");
    }
    out
}

#[test]
fn the_repositorys_fragments_are_well_formed() {
    let dir = repo().join("changelog.d");
    let Ok(entries) = fs::read_dir(&dir) else {
        return;
    };
    for e in entries {
        let e = e.unwrap();
        let name = e.file_name().to_string_lossy().into_owned();
        if name == "README.md" {
            continue;
        }
        assert!(
            name.ends_with(".md"),
            "changelog.d/{name}: fragments are .md files (ADR 0088)"
        );
        let text = fs::read_to_string(e.path()).unwrap();
        let p = problems(&name, &text);
        assert!(
            p.is_empty(),
            "changelog.d/{name}: {p:?}; see changelog.d/README.md"
        );
    }
}

#[test]
fn the_rules_catch_what_the_script_catches() {
    assert!(problems("cli-review-wait.md", "### A title\n- **Added.** x\n").is_empty());
    assert!(problems("cli-review-wait.md", "### A\n- x\n\n#### Detail\n- y\n").is_empty());
    for (name, text) in [
        ("Bad_Name.md", "### A\n- x\n"),
        ("a.md", ""),
        ("a.md", "### A\n- x"),
        ("a.md", "## A\n- x\n"),
        ("a.md", "- x\n"),
        ("a.md", "### A\n- x\n### B\n- y\n"),
        ("a.md", "### A\n\n"),
        ("a.md", "### A\n<<<<<<< main\n- x\n"),
    ] {
        assert!(!problems(name, text).is_empty(), "{name}: {text:?}");
    }
}

#[cfg(unix)]
mod script {
    use std::process::{Command, Output};

    use super::*;

    /// A copy of the script in a scratch repository: CHANGELOG.md with an
    /// entry under [Unreleased] and a released section below.
    fn scratch() -> tempfile::TempDir {
        let t = tempfile::tempdir().unwrap();
        fs::create_dir_all(t.path().join("scripts")).unwrap();
        fs::create_dir_all(t.path().join("changelog.d")).unwrap();
        fs::copy(
            repo().join("scripts/changelog-assemble.sh"),
            t.path().join("scripts/changelog-assemble.sh"),
        )
        .unwrap();
        fs::write(
            t.path().join("CHANGELOG.md"),
            "# Changelog\n\nIntro.\n\n## [Unreleased]\n\n### Older\n- **Fixed.** old\n\n\
             ## [0.0.1]\n\n### Released\n- r\n",
        )
        .unwrap();
        t
    }

    fn run(t: &Path, args: &[&str]) -> Output {
        Command::new("bash")
            .arg(t.join("scripts/changelog-assemble.sh"))
            .args(args)
            .output()
            .unwrap()
    }

    fn stdout(o: &Output) -> String {
        String::from_utf8_lossy(&o.stdout).into_owned()
    }

    #[test]
    fn assembling_moves_fragments_under_unreleased_in_name_order() {
        let t = scratch();
        let d = t.path().join("changelog.d");
        fs::write(d.join("server-b.md"), "### B\n- **Added.** b\n\n\n").unwrap();
        fs::write(d.join("cli-a.md"), "### A\n- **Fixed.** a\n").unwrap();
        fs::write(d.join("README.md"), "# How to\n\nnot a fragment\n").unwrap();

        let o = run(t.path(), &["--check"]);
        assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
        let before = fs::read_to_string(t.path().join("CHANGELOG.md")).unwrap();
        let o = run(t.path(), &["--preview"]);
        assert!(o.status.success());
        assert_eq!(
            stdout(&o),
            "## [Unreleased]\n\n### A\n- **Fixed.** a\n\n### B\n- **Added.** b\n\n\
             ### Older\n- **Fixed.** old\n\n"
        );
        // A preview changes nothing.
        assert_eq!(
            fs::read_to_string(t.path().join("CHANGELOG.md")).unwrap(),
            before
        );
        assert!(d.join("cli-a.md").exists());

        let o = run(t.path(), &[]);
        assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
        assert_eq!(
            fs::read_to_string(t.path().join("CHANGELOG.md")).unwrap(),
            "# Changelog\n\nIntro.\n\n## [Unreleased]\n\n### A\n- **Fixed.** a\n\n\
             ### B\n- **Added.** b\n\n### Older\n- **Fixed.** old\n\n\
             ## [0.0.1]\n\n### Released\n- r\n"
        );
        assert!(!d.join("cli-a.md").exists() && !d.join("server-b.md").exists());
        assert!(d.join("README.md").exists());
        // Nothing left: a second run changes nothing.
        assert!(run(t.path(), &[]).status.success());
        assert!(fs::read_to_string(t.path().join("CHANGELOG.md"))
            .unwrap()
            .contains("### A\n- **Fixed.** a\n\n### B"));
    }

    #[test]
    fn a_malformed_fragment_stops_everything() {
        let t = scratch();
        let d = t.path().join("changelog.d");
        fs::write(d.join("good.md"), "### Good\n- x\n").unwrap();
        fs::write(d.join("bad.md"), "## Wrong level\n- x\n").unwrap();
        let before = fs::read_to_string(t.path().join("CHANGELOG.md")).unwrap();
        for args in [&["--check"][..], &[]] {
            let o = run(t.path(), args);
            assert_eq!(o.status.code(), Some(1), "{args:?}");
            let err = String::from_utf8_lossy(&o.stderr);
            assert!(
                err.contains("changelog.d/bad.md: first line must be '### Title'"),
                "{err}"
            );
        }
        assert_eq!(
            fs::read_to_string(t.path().join("CHANGELOG.md")).unwrap(),
            before
        );
        assert!(d.join("good.md").exists() && d.join("bad.md").exists());
    }

    /// The repository's own fragments pass the script too.
    #[test]
    fn the_script_accepts_the_repositorys_fragments() {
        let o = Command::new("bash")
            .arg(repo().join("scripts/changelog-assemble.sh"))
            .arg("--check")
            .output()
            .unwrap();
        assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
    }
}
