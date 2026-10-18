use std::collections::BTreeMap;
use std::path::Path;
use std::process::Command;
use std::sync::Arc;

use bytes::Bytes;
use leep_core::memory::{MemoryBlobStore, MemoryMetaStore};
use leep_core::{Changeset, ChangesetId, FileChange, FileKind, RepoPath};
use leep_derive::Repo;
use leep_git::{export, import, ExportOptions, ImportOptions};

fn git(dir: &Path, args: &[&str]) -> String {
    git_env(dir, args, 1_700_000_000, "+0000")
}

fn git_env(dir: &Path, args: &[&str], time: i64, tz: &str) -> String {
    let date = format!("{time} {tz}");
    let out = Command::new("/usr/bin/git")
        .args(args)
        .current_dir(dir)
        .env("GIT_AUTHOR_NAME", "Ada Lovelace")
        .env("GIT_AUTHOR_EMAIL", "ada@example.com")
        .env("GIT_AUTHOR_DATE", &date)
        .env("GIT_COMMITTER_NAME", "Grace Hopper")
        .env("GIT_COMMITTER_EMAIL", "grace@example.com")
        .env("GIT_COMMITTER_DATE", &date)
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_SYSTEM", "/dev/null")
        .output()
        .expect("run git");
    assert!(
        out.status.success(),
        "git {args:?} failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout)
        .unwrap()
        .trim_end()
        .to_string()
}

fn write(dir: &Path, path: &str, content: &str) {
    let p = dir.join(path);
    std::fs::create_dir_all(p.parent().unwrap()).unwrap();
    std::fs::write(p, content).unwrap();
}

fn init() -> tempfile::TempDir {
    let d = tempfile::tempdir().unwrap();
    git(d.path(), &["init", "-q", "-b", "main"]);
    d
}

fn commit(dir: &Path, msg: &str) {
    git(dir, &["add", "-A"]);
    git(dir, &["commit", "-q", "-m", msg]);
}

fn new_repo() -> Repo {
    Repo::new(
        Arc::new(MemoryBlobStore::new()),
        Arc::new(MemoryMetaStore::new()),
    )
}

/// Compare the Leep snapshot of `head` with `git ls-tree -r HEAD`.
async fn assert_matches_git(repo: &Repo, head: ChangesetId, dir: &Path, rev: &str) {
    let files = repo.list_files(head, "").await.unwrap();
    let ls = git(dir, &["ls-tree", "-r", rev]);
    let expected: Vec<(String, String)> = ls
        .lines()
        .filter(|l| !l.starts_with("160000"))
        .map(|l| {
            let (meta, path) = l.split_once('\t').unwrap();
            (
                meta.split(' ').next().unwrap().to_string(),
                path.to_string(),
            )
        })
        .collect();
    assert_eq!(files.len(), expected.len());
    for (f, (mode, path)) in files.iter().zip(&expected) {
        assert_eq!(f.path.as_str(), path);
        let want = match mode.as_str() {
            "100644" => FileKind::Regular,
            "100755" => FileKind::Executable,
            "120000" => FileKind::Symlink,
            m => panic!("unexpected mode {m}"),
        };
        assert_eq!(f.kind, want, "{path}");
        let (_, data) = repo.read_file(head, &f.path).await.unwrap().unwrap();
        let out = Command::new("/usr/bin/git")
            .args(["show", &format!("{rev}:{path}")])
            .current_dir(dir)
            .output()
            .unwrap();
        assert_eq!(data.as_ref(), out.stdout.as_slice(), "{path}");
    }
}

fn linear_fixture() -> tempfile::TempDir {
    let d = init();
    let p = d.path();
    write(p, "a.txt", "alpha\n");
    write(p, "dir/sub/b.txt", "bravo\n");
    write(p, "dir/c.txt", "charlie\n");
    write(p, "run.sh", "#!/bin/sh\n");
    std::fs::set_permissions(
        p.join("run.sh"),
        std::os::unix::fs::PermissionsExt::from_mode(0o755),
    )
    .unwrap();
    std::os::unix::fs::symlink("a.txt", p.join("link")).unwrap();
    commit(p, "first\n\nbody line");
    write(p, "a.txt", "alpha 2\n");
    git(p, &["rm", "-q", "dir/c.txt"]);
    commit(p, "modify and delete");
    git(p, &["mv", "dir/sub/b.txt", "dir/renamed.txt"]);
    write(p, "new/deep/er/f.txt", "deep\n");
    commit(p, "rename and add");
    // dir -> file and file -> dir type changes
    git(p, &["rm", "-rq", "new"]);
    write(p, "new", "now a file\n");
    git(p, &["rm", "-q", "a.txt"]);
    write(p, "a.txt/inner", "dir now\n");
    commit(p, "type changes");
    d
}

#[tokio::test]
async fn linear_history_matches_git() {
    let d = linear_fixture();
    let repo = new_repo();
    let r = import(
        &repo,
        d.path().join(".git").as_path(),
        "HEAD",
        &ImportOptions::default(),
    )
    .await
    .unwrap();
    assert_eq!(r.commits_imported, 4);
    assert_eq!(r.commits_skipped, 0);
    let head = r.head.unwrap();
    assert_eq!(repo.generation(head).await.unwrap(), 4);
    assert_matches_git(&repo, head, d.path(), "HEAD").await;
    for n in 1..4 {
        let rev = format!("HEAD~{n}");
        // Walk first-parent chain in Leep.
        let mut id = head;
        for _ in 0..n {
            id = repo.get_changeset(id).await.unwrap().parents[0];
        }
        assert_matches_git(&repo, id, d.path(), &rev).await;
    }
    let cs = repo.get_changeset(head).await.unwrap();
    assert_eq!(cs.author, "Ada Lovelace <ada@example.com>");
    assert_eq!(cs.timestamp, 1_700_000_000);
    assert_eq!(cs.message, "type changes\n");
    assert_eq!(
        cs.extra["git.commit"],
        git(d.path(), &["rev-parse", "HEAD"])
    );
    assert_eq!(
        cs.extra["git.committer"],
        "Grace Hopper <grace@example.com> 1700000000"
    );
}

#[tokio::test]
async fn merge_commit() {
    let d = init();
    let p = d.path();
    write(p, "base.txt", "base\n");
    commit(p, "base");
    git(p, &["checkout", "-q", "-b", "side"]);
    write(p, "side.txt", "side\n");
    commit(p, "side");
    git(p, &["checkout", "-q", "main"]);
    write(p, "main.txt", "main\n");
    commit(p, "main");
    git(p, &["merge", "-q", "--no-ff", "-m", "merge side", "side"]);
    let repo = new_repo();
    let r = import(&repo, &p.join(".git"), "main", &ImportOptions::default())
        .await
        .unwrap();
    assert_eq!(r.commits_imported, 4);
    let head = r.head.unwrap();
    let cs = repo.get_changeset(head).await.unwrap();
    assert_eq!(cs.parents.len(), 2);
    assert_eq!(repo.generation(head).await.unwrap(), 3);
    // First-parent diff: the merge adds side.txt only.
    assert_eq!(cs.changes.len(), 1);
    assert_matches_git(&repo, head, p, "main").await;
}

#[tokio::test]
async fn incremental_import_and_set_ref() {
    let d = init();
    let p = d.path();
    write(p, "a", "1\n");
    commit(p, "one");
    write(p, "a", "2\n");
    commit(p, "two");
    let repo = new_repo();
    let opts = ImportOptions {
        set_ref: Some("main".into()),
    };
    let r1 = import(&repo, &p.join(".git"), "HEAD", &opts).await.unwrap();
    assert_eq!((r1.commits_imported, r1.commits_skipped), (2, 0));
    assert_eq!(repo.resolve("main").await.unwrap(), r1.head.unwrap());

    let r2 = import(&repo, &p.join(".git"), "HEAD", &opts).await.unwrap();
    assert_eq!((r2.commits_imported, r2.blobs_uploaded), (0, 0));
    assert_eq!(r2.head, r1.head);

    write(p, "b", "3\n");
    commit(p, "three");
    write(p, "b", "4\n");
    commit(p, "four");
    let r3 = import(&repo, &p.join(".git"), "HEAD", &opts).await.unwrap();
    assert_eq!((r3.commits_imported, r3.commits_skipped), (2, 1));
    assert_eq!(repo.resolve("main").await.unwrap(), r3.head.unwrap());
    assert_eq!(repo.generation(r3.head.unwrap()).await.unwrap(), 4);
    assert_matches_git(&repo, r3.head.unwrap(), p, "HEAD").await;
    // Only the two moves are logged; the no-op re-import is not.
    let log = repo.meta().ref_log(Some("main"), None, 10).await.unwrap();
    assert_eq!(log.len(), 2);
    assert!(log.iter().all(|e| e.operation == "import"));
    assert_eq!(log[0].new, r3.head);
    assert_eq!(log[1].old, None);

    // A divergent history must not move the ref.
    git(p, &["checkout", "-q", "-b", "other", "HEAD~3"]);
    write(p, "z", "z\n");
    commit(p, "diverge");
    let err = import(&repo, &p.join(".git"), "other", &opts)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("fast-forward"), "{err}");
    assert_eq!(repo.resolve("main").await.unwrap(), r3.head.unwrap());
}

/// A file whose bytes are an object's hash preimage cannot be stored as a
/// blob (ADR 0041). Import stops at that commit with an error naming the
/// file, after importing the commits before it; a text file that only starts
/// with `leep:` imports normally.
#[tokio::test]
async fn reserved_blob_prefix_stops_import_clearly() {
    let d = init();
    let p = d.path();
    write(p, "config.yml", "leep: true\n");
    commit(p, "ordinary");
    write(p, "evil.bin", "leep:tree-v1\0\0");
    commit(p, "reserved");
    let repo = new_repo();
    let err = import(&repo, &p.join(".git"), "HEAD", &ImportOptions::default())
        .await
        .unwrap_err();
    assert!(matches!(err, leep_core::Error::Invalid(_)), "{err}");
    assert!(err.to_string().contains("evil.bin"), "{err}");

    let r = import(&repo, &p.join(".git"), "HEAD~1", &ImportOptions::default())
        .await
        .unwrap();
    assert_eq!(r.commits_imported + r.commits_skipped, 1);
    assert_matches_git(&repo, r.head.unwrap(), p, "HEAD~1").await;
}

#[tokio::test]
async fn submodules_are_skipped() {
    let d = init();
    let p = d.path();
    write(p, "a", "1\n");
    git(p, &["add", "a"]);
    git(
        p,
        &[
            "update-index",
            "--add",
            "--cacheinfo",
            "160000,1111111111111111111111111111111111111111,vendor/sub",
        ],
    );
    git(p, &["commit", "-q", "-m", "with gitlink"]);
    let repo = new_repo();
    let r = import(&repo, &p.join(".git"), "HEAD", &ImportOptions::default())
        .await
        .unwrap();
    assert_eq!(r.submodules_skipped, 1);
    assert_eq!(repo.list_files(r.head.unwrap(), "").await.unwrap().len(), 1);
}

#[tokio::test]
async fn round_trip_is_byte_exact() {
    let d = linear_fixture();
    let p = d.path();
    // Merge plus a non-UTC timezone, to exercise header fidelity.
    git(p, &["checkout", "-q", "-b", "side", "HEAD~1"]);
    write(p, "side.txt", "side\n");
    git(p, &["add", "-A"]);
    git_env(p, &["commit", "-q", "-m", "side"], 1_700_001_000, "+0530");
    git(p, &["checkout", "-q", "main"]);
    git_env(
        p,
        &["merge", "-q", "--no-ff", "-m", "merge", "side"],
        1_700_002_000,
        "-0800",
    );

    let repo = new_repo();
    let r = import(&repo, &p.join(".git"), "main", &ImportOptions::default())
        .await
        .unwrap();
    let out = tempfile::tempdir().unwrap();
    let bare = out.path().join("out.git");
    let e = export(
        &repo,
        r.head.unwrap(),
        &bare,
        "main",
        &ExportOptions::default(),
    )
    .await
    .unwrap();
    assert_eq!(e.commits_written, 6);

    let want = git(p, &["rev-list", "--topo-order", "main"]);
    let got = git(&bare, &["rev-list", "--topo-order", "main"]);
    assert_eq!(want, got);
    assert_eq!(
        git(p, &["rev-parse", "main"]),
        git(&bare, &["rev-parse", "main"])
    );
    git(&bare, &["fsck", "--strict"]);

    // Exporting again is a no-op.
    let e2 = export(
        &repo,
        r.head.unwrap(),
        &bare,
        "main",
        &ExportOptions::default(),
    )
    .await
    .unwrap();
    assert_eq!(e2.commits_written, 0);
}

fn native(
    parents: Vec<ChangesetId>,
    msg: &str,
    changes: BTreeMap<RepoPath, FileChange>,
    extra: BTreeMap<String, String>,
) -> Changeset {
    Changeset {
        parents,
        author: "Bot <bot@example.com>".into(),
        timestamp: 1_750_000_000,
        message: msg.into(),
        changes,
        extra,
    }
}

async fn put_file(repo: &Repo, path: &str, data: &str, kind: FileKind) -> (RepoPath, FileChange) {
    let content = repo
        .blobs()
        .put(Bytes::from(data.to_string()))
        .await
        .unwrap();
    (
        RepoPath::new(path).unwrap(),
        FileChange::Modify {
            content,
            kind,
            size: data.len() as u64,
        },
    )
}

#[tokio::test]
async fn export_native_changesets() {
    let repo = new_repo();
    let c1 = repo
        .create_changeset(native(
            vec![],
            "initial\n",
            BTreeMap::from([
                put_file(&repo, "README.md", "hi\n", FileKind::Regular).await,
                put_file(
                    &repo,
                    "src/deep/main.rs",
                    "fn main() {}\n",
                    FileKind::Regular,
                )
                .await,
                put_file(&repo, "bin/tool", "#!/bin/sh\n", FileKind::Executable).await,
                put_file(&repo, "link", "README.md", FileKind::Symlink).await,
                // Names that stress git's tree ordering.
                put_file(&repo, "a.b", "x", FileKind::Regular).await,
                put_file(&repo, "a/c", "y", FileKind::Regular).await,
            ]),
            BTreeMap::new(),
        ))
        .await
        .unwrap();
    let c2 = repo
        .create_changeset(native(
            vec![c1],
            "second",
            BTreeMap::from([
                (RepoPath::new("README.md").unwrap(), FileChange::Delete),
                put_file(&repo, "src/lib.rs", "pub fn f() {}\n", FileKind::Regular).await,
            ]),
            BTreeMap::from([
                ("agent.model".to_string(), "test-model".to_string()),
                ("agent.session_id".to_string(), "s-1".to_string()),
                ("review.by".to_string(), "someone".to_string()),
            ]),
        ))
        .await
        .unwrap();

    for trailers in [false, true] {
        let out = tempfile::tempdir().unwrap();
        let bare = out.path().join("n.git");
        let e = export(
            &repo,
            c2,
            &bare,
            "trunk",
            &ExportOptions {
                provenance_trailers: trailers,
            },
        )
        .await
        .unwrap();
        assert!(e.commits_written >= 1);
        git(&bare, &["fsck", "--strict"]);
        assert_eq!(git(&bare, &["rev-list", "--count", "trunk"]), "2");
        assert_eq!(
            git(&bare, &["log", "-1", "--format=%an <%ae>", "trunk"]),
            "Bot <bot@example.com>"
        );
        let files = git(&bare, &["ls-tree", "-r", "--name-only", "trunk"]);
        assert_eq!(
            files.lines().collect::<Vec<_>>(),
            [
                "a.b",
                "a/c",
                "bin/tool",
                "link",
                "src/deep/main.rs",
                "src/lib.rs"
            ]
        );
        let ls = git(&bare, &["ls-tree", "trunk", "bin/tool", "link"]);
        assert!(ls.contains("100755") && ls.contains("120000"), "{ls}");
        let body = git(&bare, &["log", "-1", "--format=%B", "trunk"]);
        if trailers {
            assert_eq!(
                body,
                "second\n\nLeep-Agent-Model: test-model\nLeep-Agent-Session-Id: s-1"
            );
        } else {
            assert_eq!(body, "second");
        }
        assert!(!body.contains("review"));
        let first = git(&bare, &["log", "--format=%B", "trunk~1"]);
        assert_eq!(first, "initial");
    }
}

#[tokio::test]
async fn export_into_existing_repo_after_import_is_incremental() {
    let d = init();
    let p = d.path();
    write(p, "a", "1\n");
    commit(p, "one");
    let repo = new_repo();
    let r = import(&repo, &p.join(".git"), "HEAD", &ImportOptions::default())
        .await
        .unwrap();
    // Add a Leep-native child of the imported head, then export to a clone.
    let c = repo
        .create_changeset(native(
            vec![r.head.unwrap()],
            "native child\n",
            BTreeMap::from([put_file(&repo, "b", "2\n", FileKind::Regular).await]),
            BTreeMap::new(),
        ))
        .await
        .unwrap();
    git(p, &["branch", "-q", "leep"]);
    let e = export(&repo, c, &p.join(".git"), "leep", &ExportOptions::default())
        .await
        .unwrap();
    assert_eq!((e.commits_written, e.commits_skipped), (1, 1));
    git(p, &["fsck", "--strict"]);
    assert_eq!(git(p, &["rev-list", "--count", "leep"]), "2");
    assert_eq!(
        git(p, &["rev-parse", "leep~1"]),
        git(p, &["rev-parse", "main"])
    );
}

/// Commits made on github.com carry a `gpgsig` header; dropping it (or other
/// extra headers like `encoding`) would change every exported sha.
#[tokio::test]
async fn signed_commits_round_trip() {
    let d = init();
    write(d.path(), "a.txt", "one\n");
    commit(d.path(), "first");
    let tree = git(d.path(), &["rev-parse", "HEAD^{tree}"]);
    let parent = git(d.path(), &["rev-parse", "HEAD"]);
    let raw = format!(
        "tree {tree}\nparent {parent}\n\
         author Ada Lovelace <ada@example.com> 1700000100 +0530\n\
         committer GitHub <noreply@github.com> 1700000100 +0000\n\
         encoding UTF-8\n\
         gpgsig -----BEGIN PGP SIGNATURE-----\n \n wsFcBAABCAAQ\n =abcd\n -----END PGP SIGNATURE-----\n\
         \nsigned commit\n"
    );
    let path = d.path().join("raw-commit");
    std::fs::write(&path, raw).unwrap();
    let signed = git(
        d.path(),
        &["hash-object", "-t", "commit", "-w", path.to_str().unwrap()],
    );
    git(d.path(), &["update-ref", "refs/heads/main", &signed]);

    let repo = new_repo();
    let head = import(
        &repo,
        &d.path().join(".git"),
        "main",
        &ImportOptions::default(),
    )
    .await
    .unwrap()
    .head
    .unwrap();
    let out = tempfile::tempdir().unwrap();
    let bare = out.path().join("out.git");
    export(&repo, head, &bare, "main", &ExportOptions::default())
        .await
        .unwrap();
    assert_eq!(git(&bare, &["rev-parse", "main"]), signed);
    git(&bare, &["fsck", "--strict"]);
}

/// More commits than the importer decodes ahead, with contents that come
/// back again and again, so in-flight commits share blobs; plus merges.
#[tokio::test]
async fn long_history_with_repeated_blobs_matches_git() {
    let d = init();
    let p = d.path();
    for i in 0..60 {
        write(p, "flip.txt", if i % 2 == 0 { "even\n" } else { "odd\n" });
        write(p, &format!("dir{}/f.txt", i % 5), &format!("v{}\n", i % 3));
        commit(p, &format!("c{i}"));
        if i % 20 == 19 {
            git(p, &["checkout", "-q", "-b", &format!("side{i}"), "HEAD~3"]);
            write(p, &format!("side{i}.txt"), "side\n");
            commit(p, &format!("side {i}"));
            git(p, &["checkout", "-q", "-"]);
            git(p, &["merge", "-q", "--no-edit", &format!("side{i}")]);
        }
    }
    let repo = new_repo();
    let r = import(
        &repo,
        p.join(".git").as_path(),
        "HEAD",
        &ImportOptions::default(),
    )
    .await
    .unwrap();
    let expected: usize = git(p, &["rev-list", "--count", "HEAD"]).parse().unwrap();
    assert_eq!(r.commits_imported, expected);
    let head = r.head.unwrap();
    assert_matches_git(&repo, head, p, "HEAD").await;
    let mut id = head;
    for n in 1..=40 {
        id = repo.get_changeset(id).await.unwrap().parents[0];
        assert_matches_git(&repo, id, p, &format!("HEAD~{n}")).await;
    }
    // A rerun finds every commit mapped.
    let again = import(
        &repo,
        p.join(".git").as_path(),
        "HEAD",
        &ImportOptions::default(),
    )
    .await
    .unwrap();
    assert_eq!((again.commits_imported, again.head), (0, Some(head)));
}
