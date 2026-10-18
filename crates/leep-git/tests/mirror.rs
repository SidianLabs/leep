//! The live mirror (ADR 0046) against in-memory Leep stores and local bare
//! git repositories: incremental exports match full exports bit for bit, and
//! each relation between the two sides gets the action the ADR names.

use std::collections::BTreeMap;
use std::path::Path;
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use bytes::Bytes;
use leep_core::memory::{MemoryBlobStore, MemoryMetaStore};
use leep_core::{Changeset, ChangesetId, FileChange, FileKind, Hash, RefUpdateContext, RepoPath};
use leep_derive::Repo;
use leep_git::mirror::export::{CommitStyle, Exporter};
use leep_git::mirror::map::Map;
use leep_git::mirror::{self, Cas, Config, Leep, RefSpec, RefState};
use leep_git::{export, import, ExportOptions, ImportOptions};

fn git(dir: &Path, args: &[&str]) -> String {
    let out = Command::new("git")
        .args(args)
        .current_dir(dir)
        .env("GIT_AUTHOR_NAME", "Ada Lovelace")
        .env("GIT_AUTHOR_EMAIL", "ada@example.com")
        .env("GIT_AUTHOR_DATE", "1700000000 +0100")
        .env("GIT_COMMITTER_NAME", "Grace Hopper")
        .env("GIT_COMMITTER_EMAIL", "grace@example.com")
        .env("GIT_COMMITTER_DATE", "1700000000 +0100")
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

fn commit(dir: &Path, msg: &str) {
    git(dir, &["add", "-A"]);
    git(dir, &["commit", "-q", "-m", msg]);
}

/// A Leep server stand-in: the mirror's [`Leep`] over an in-memory `Repo`.
struct Local {
    repo: Repo,
    rt: tokio::runtime::Runtime,
}

impl Local {
    fn new() -> Local {
        Local {
            repo: Repo::new(
                Arc::new(MemoryBlobStore::new()),
                Arc::new(MemoryMetaStore::new()),
            ),
            rt: tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap(),
        }
    }

    fn on<F: std::future::Future>(&self, f: F) -> F::Output {
        self.rt.block_on(f)
    }

    fn file(&self, path: &str, data: &str, kind: FileKind) -> (RepoPath, FileChange) {
        let content = self
            .on(self.repo.blobs().put(Bytes::from(data.to_string())))
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

    fn native(
        &self,
        parents: Vec<ChangesetId>,
        msg: &str,
        changes: Vec<(RepoPath, FileChange)>,
    ) -> ChangesetId {
        let cs = Changeset {
            parents,
            author: "Bot <bot@example.com>".into(),
            timestamp: 1_750_000_000,
            message: msg.into(),
            changes: changes.into_iter().collect(),
            extra: BTreeMap::from([("agent.model".to_string(), "m".to_string())]),
        };
        self.on(self.repo.create_changeset(cs)).unwrap()
    }

    fn set_ref(&self, name: &str, to: ChangesetId) {
        let meta = self.repo.meta();
        let cur = self.on(meta.get_ref(name)).unwrap();
        assert!(self
            .on(meta.cas_ref_logged(name, cur, Some(to), &RefUpdateContext::new("land")))
            .unwrap());
    }

    fn get_ref(&self, name: &str) -> Option<ChangesetId> {
        self.on(self.repo.meta().get_ref(name)).unwrap()
    }
}

impl Leep for Local {
    fn changeset(&self, id: &ChangesetId) -> Result<Changeset> {
        Ok(self.on(self.repo.get_changeset(*id))?)
    }

    fn has_changeset(&self, id: &ChangesetId) -> Result<bool> {
        Ok(self.on(self.repo.meta().get_changeset(id))?.is_some())
    }

    fn blob(&self, hash: &Hash) -> Result<Vec<u8>> {
        Ok(self
            .on(self.repo.blobs().get(hash))?
            .ok_or_else(|| anyhow::anyhow!("no blob {hash}"))?
            .to_vec())
    }

    fn missing_blobs(&self, hashes: Vec<Hash>) -> Result<Vec<Hash>> {
        let have = self.on(self.repo.blobs().contains_many(&hashes))?;
        Ok(hashes
            .into_iter()
            .zip(have)
            .filter(|(_, h)| !h)
            .map(|(x, _)| x)
            .collect())
    }

    fn put_blob(&self, hash: &Hash, data: Vec<u8>) -> Result<()> {
        let got = self.on(self.repo.blobs().put(Bytes::from(data)))?;
        assert_eq!(&got, hash);
        Ok(())
    }

    fn create_changeset(&self, cs: &Changeset) -> Result<ChangesetId> {
        Ok(self.on(self.repo.create_changeset(cs.clone()))?)
    }

    fn refs(&self) -> Result<BTreeMap<String, ChangesetId>> {
        Ok(self.on(self.repo.meta().list_refs())?)
    }

    fn ref_log_head(&self) -> Result<Option<u64>> {
        Ok(self
            .on(self.repo.meta().ref_log(None, None, 1))?
            .first()
            .map(|e| e.seq))
    }

    fn ref_had(&self, name: &str, id: &ChangesetId) -> Result<bool> {
        let all = self.on(self.repo.meta().ref_log(Some(name), None, 100_000))?;
        Ok(all.iter().any(|e| e.new == Some(*id)))
    }

    fn cas_ref(
        &self,
        name: &str,
        expected: Option<ChangesetId>,
        new: ChangesetId,
        message: &str,
    ) -> Result<Cas> {
        let ctx = RefUpdateContext::new(mirror::CAS_OPERATION).message(message);
        let won = self.on(self.repo.cas_ref_checked(name, expected, Some(new), &ctx))?;
        Ok(if won {
            Cas::Won
        } else {
            Cas::Lost(self.get_ref(name))
        })
    }
}

fn exporter(dir: &Path, style: CommitStyle) -> Exporter {
    let repo = gix::init_bare(dir.join("git")).unwrap();
    let map = Map::open(&dir.join("map")).unwrap();
    Exporter::new(repo, map, style)
}

/// History with every shape the tree editor must handle: imported commits
/// with type changes and a merge, then native changesets that empty a
/// directory, turn a file into a directory and merge two native branches.
fn history(leep: &Local) -> ChangesetId {
    let d = tempfile::tempdir().unwrap();
    let p = d.path();
    git(p, &["init", "-q", "-b", "main"]);
    write(p, "a.txt", "alpha\n");
    write(p, "dir/sub/b.txt", "bravo\n");
    write(p, "run.sh", "#!/bin/sh\n");
    std::fs::set_permissions(
        p.join("run.sh"),
        std::os::unix::fs::PermissionsExt::from_mode(0o755),
    )
    .unwrap();
    std::os::unix::fs::symlink("a.txt", p.join("link")).unwrap();
    commit(p, "first\n\nbody");
    git(p, &["checkout", "-q", "-b", "side"]);
    write(p, "side.txt", "side\n");
    commit(p, "side");
    git(p, &["checkout", "-q", "main"]);
    git(p, &["rm", "-rq", "dir"]);
    write(p, "dir", "dir is a file now\n");
    commit(p, "type change");
    git(p, &["merge", "-q", "--no-ff", "-m", "merge side", "side"]);
    let r = leep
        .on(import(
            &leep.repo,
            &p.join(".git"),
            "main",
            &ImportOptions::default(),
        ))
        .unwrap();
    let base = r.head.unwrap();

    let a = leep.native(
        vec![base],
        "native one",
        vec![
            leep.file("deep/x/y/z.txt", "z\n", FileKind::Regular),
            leep.file("a.b", "ab\n", FileKind::Regular),
            leep.file("a/c", "ac\n", FileKind::Regular),
        ],
    );
    let b = leep.native(
        vec![a],
        "empty a directory\n\nLeep-Agent-Model: m",
        vec![
            (RepoPath::new("deep/x/y/z.txt").unwrap(), FileChange::Delete),
            (RepoPath::new("dir").unwrap(), FileChange::Delete),
            leep.file("dir/inner.txt", "inner\n", FileKind::Executable),
        ],
    );
    let c = leep.native(
        vec![a],
        "other branch",
        vec![leep.file("other.txt", "o\n", FileKind::Regular)],
    );
    leep.native(
        vec![b, c],
        "merge native",
        vec![leep.file("other.txt", "o\n", FileKind::Regular)],
    )
}

#[test]
fn incremental_export_matches_the_full_export() {
    let leep = Local::new();
    let head = history(&leep);
    for provenance in [false, true] {
        let out = tempfile::tempdir().unwrap();
        let bare = out.path().join("full.git");
        let full = leep
            .on(export(
                &leep.repo,
                head,
                &bare,
                "main",
                &ExportOptions {
                    provenance_trailers: provenance,
                },
            ))
            .unwrap()
            .head
            .unwrap();
        let mut ex = exporter(
            out.path(),
            CommitStyle {
                provenance_trailers: provenance,
                changeset_trailer: false,
            },
        );
        let inc = ex.export(&leep, head).unwrap();
        assert_eq!(inc, full, "provenance trailers {provenance}");
        // A second exporter over the same directory finds everything mapped.
        drop(ex);
        let repo = gix::open(out.path().join("git")).unwrap();
        let mut again = Exporter::new(
            repo,
            Map::open(&out.path().join("map")).unwrap(),
            CommitStyle {
                provenance_trailers: provenance,
                changeset_trailer: false,
            },
        );
        assert_eq!(again.export(&leep, head).unwrap(), full);
        assert_eq!(again.commits_written, 0);
    }
}

#[test]
fn only_native_changesets_get_a_changeset_trailer() {
    let leep = Local::new();
    let head = history(&leep);
    let out = tempfile::tempdir().unwrap();
    let mut ex = exporter(
        out.path(),
        CommitStyle {
            provenance_trailers: false,
            changeset_trailer: true,
        },
    );
    let oid = ex.export(&leep, head).unwrap();
    let gd = out.path().join("git");
    let gd = gd.to_str().unwrap();
    let msg = git(
        Path::new(gd),
        &["log", "-1", "--format=%B", &oid.to_string()],
    );
    assert_eq!(msg, format!("merge native\n\nLeep-Changeset: {head}"));
    // An existing trailer paragraph is extended, not duplicated.
    let b_msg = git(
        Path::new(gd),
        &["log", "-1", "--format=%B", &format!("{oid}^1")],
    );
    let lines: Vec<&str> = b_msg.lines().collect();
    assert_eq!(lines[..3], ["empty a directory", "", "Leep-Agent-Model: m"]);
    assert!(lines[3].starts_with("Leep-Changeset: "), "{b_msg}");
    // Imported commits keep their original bytes: no trailer.
    let imported = git(
        Path::new(gd),
        &["log", "-1", "--format=%B", &format!("{oid}^1^1^1")],
    );
    assert_eq!(imported, "merge side");
}

struct Fixture {
    _dir: tempfile::TempDir,
    remote: std::path::PathBuf,
    work: std::path::PathBuf,
    human: std::path::PathBuf,
}

fn fixture() -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let remote = dir.path().join("remote.git");
    std::fs::create_dir_all(&remote).unwrap();
    git(&remote, &["init", "-q", "--bare", "-b", "main"]);
    Fixture {
        remote,
        work: dir.path().join("work"),
        human: dir.path().join("human"),
        _dir: dir,
    }
}

fn config(f: &Fixture, import: bool) -> Config {
    Config {
        server: "memory".into(),
        token: None,
        remote: f.remote.to_str().unwrap().to_string(),
        work_dir: f.work.clone(),
        refs: vec![RefSpec::parse("main").unwrap()],
        tags: true,
        import,
        allow_rewind: vec![],
        diverged_prefix: "leep-diverged/".into(),
        style: CommitStyle {
            provenance_trailers: false,
            changeset_trailer: true,
        },
        interval: Duration::from_millis(10),
        git_poll: Duration::from_secs(60),
        reconcile: Duration::from_secs(60),
        git_timeout: Duration::from_secs(60),
        max_backoff: Duration::from_secs(1),
        listen: None,
    }
}

fn remote_head(f: &Fixture, r: &str) -> Option<String> {
    let out = git(&f.remote, &["for-each-ref", "--format=%(objectname)", r]);
    (!out.is_empty()).then_some(out)
}

fn state(m: &mirror::Mirror, leep_ref: &str) -> (RefState, String) {
    let s = m.status.lock().unwrap();
    let r = s.refs.iter().find(|r| r.leep_ref == leep_ref).unwrap();
    (r.state, r.detail.clone())
}

#[test]
fn pushes_fast_forwards_and_tags_and_resumes_without_duplicates() {
    let f = fixture();
    let leep = Local::new();
    let c1 = leep.native(
        vec![],
        "one",
        vec![leep.file("a", "1\n", FileKind::Regular)],
    );
    leep.set_ref("main", c1);
    leep.set_ref("tags/v1", c1);
    {
        let mut m = mirror::Mirror::open(config(&f, false), &leep).unwrap();
        // One mirror per work directory.
        assert!(mirror::Mirror::open(config(&f, false), &leep).is_err());
        assert!(m.run_once());
        assert_eq!(state(&m, "main").0, RefState::InSync);
        assert_eq!(state(&m, "tags/v1").0, RefState::InSync);
    }
    let first = remote_head(&f, "refs/heads/main").unwrap();
    assert_eq!(remote_head(&f, "refs/tags/v1").unwrap(), first);
    let msg = git(&f.remote, &["log", "-1", "--format=%B", "main"]);
    assert_eq!(msg, format!("one\n\nLeep-Changeset: {c1}"));

    // Two more lands; a fresh mirror process pushes both in one fast-forward.
    let c2 = leep.native(
        vec![c1],
        "two",
        vec![leep.file("b", "2\n", FileKind::Regular)],
    );
    let c3 = leep.native(
        vec![c2],
        "three",
        vec![leep.file("a", "3\n", FileKind::Regular)],
    );
    leep.set_ref("main", c3);
    let mut m = mirror::Mirror::open(config(&f, false), &leep).unwrap();
    assert!(m.run_once());
    let log = git(&f.remote, &["log", "--format=%s", "main"]);
    assert_eq!(log, "three\ntwo\none");
    assert_eq!(git(&f.remote, &["rev-parse", "main~2"]), first);
    // Nothing new: nothing written, nothing pushed.
    let pushes = m.status.lock().unwrap().refs[0].pushes.ok;
    assert!(m.run_once());
    assert_eq!(m.status.lock().unwrap().refs[0].pushes.ok, pushes);

    // A tag that differs on the remote is reported, never moved.
    drop(m);
    git(&f.remote, &["update-ref", "refs/tags/v2", &first]);
    leep.set_ref("tags/v2", c3);
    let mut m = mirror::Mirror::open(config(&f, false), &leep).unwrap();
    assert!(!m.run_once());
    assert_eq!(state(&m, "tags/v2").0, RefState::Diverged);
    assert_eq!(remote_head(&f, "refs/tags/v2").unwrap(), first);
}

#[test]
fn imports_fast_forward_pushes_and_reports_divergence_until_merged() {
    let f = fixture();
    let leep = Local::new();
    let c1 = leep.native(
        vec![],
        "one",
        vec![leep.file("a", "1\n", FileKind::Regular)],
    );
    leep.set_ref("main", c1);
    let mut m = mirror::Mirror::open(config(&f, true), &leep).unwrap();
    assert!(m.run_once());

    // A person pushes to the remote; the mirror lands it in Leep.
    git(
        f.human.parent().unwrap(),
        &["clone", "-q", f.remote.to_str().unwrap(), "human"],
    );
    write(&f.human, "human.txt", "from git\n");
    commit(&f.human, "human change");
    git(&f.human, &["push", "-q", "origin", "main"]);
    let pushed = git(&f.human, &["rev-parse", "HEAD"]);
    assert!(m.run_once());
    assert_eq!(state(&m, "main").0, RefState::InSync);
    let landed = leep.get_ref("main").unwrap();
    let cs = leep.on(leep.repo.get_changeset(landed)).unwrap();
    assert_eq!(cs.extra["git.commit"], pushed);
    assert_eq!(cs.parents, vec![c1]);
    let log = leep
        .on(leep.repo.meta().ref_log(Some("main"), None, 1))
        .unwrap();
    assert_eq!(log[0].operation, mirror::CAS_OPERATION);
    // No loop: the remote still has exactly the person's commit.
    assert_eq!(remote_head(&f, "refs/heads/main").unwrap(), pushed);

    // Leep lands while someone pushes to git: diverged, nothing moves.
    let agent = leep.native(
        vec![landed],
        "agent change",
        vec![leep.file("agent.txt", "agent\n", FileKind::Regular)],
    );
    leep.set_ref("main", agent);
    write(&f.human, "human2.txt", "more\n");
    commit(&f.human, "second human change");
    git(&f.human, &["push", "-q", "origin", "main"]);
    let human2 = git(&f.human, &["rev-parse", "HEAD"]);
    assert!(!m.run_once());
    let (st, detail) = state(&m, "main");
    assert_eq!(st, RefState::Diverged, "{detail}");
    assert!(detail.contains("refs/heads/leep-diverged/main"), "{detail}");
    assert_eq!(remote_head(&f, "refs/heads/main").unwrap(), human2);
    assert_eq!(leep.get_ref("main"), Some(agent));
    let side = remote_head(&f, "refs/heads/leep-diverged/main").unwrap();

    // The person merges Leep's side and pushes; the mirror lands the merge
    // and retires the diverged branch.
    git(&f.human, &["fetch", "-q", "origin"]);
    git(
        &f.human,
        &["merge", "-q", "--no-edit", "origin/leep-diverged/main"],
    );
    git(&f.human, &["push", "-q", "origin", "main"]);
    let merge = git(&f.human, &["rev-parse", "HEAD"]);
    assert!(m.run_once(), "{:?}", state(&m, "main"));
    assert_eq!(remote_head(&f, "refs/heads/main").unwrap(), merge);
    assert_eq!(remote_head(&f, "refs/heads/leep-diverged/main"), None);
    let head = leep.get_ref("main").unwrap();
    let cs = leep.on(leep.repo.get_changeset(head)).unwrap();
    assert_eq!(cs.extra["git.commit"], merge);
    assert_eq!(cs.parents.len(), 2);
    assert!(leep.on(leep.repo.is_ancestor(agent, head)).unwrap());
    let side_cs = mirror::import::leep_origin(&leep, &mut exporter_for(&f), side.parse().unwrap());
    assert!(
        side_cs.unwrap().is_some(),
        "the diverged branch held Leep's export"
    );

    // An undo in Leep is reported and the remote is not forced.
    leep.set_ref("main", agent);
    assert!(!m.run_once());
    assert_eq!(state(&m, "main").0, RefState::Rewound);
    assert_eq!(remote_head(&f, "refs/heads/main").unwrap(), merge);
    assert_eq!(leep.get_ref("main"), Some(agent));
}

/// A second exporter over a copy of the mirror's work directory.
fn exporter_for(f: &Fixture) -> Exporter {
    let copy = f.work.with_file_name("copy");
    let _ = std::fs::remove_dir_all(&copy);
    std::fs::create_dir_all(&copy).unwrap();
    let status = Command::new("cp")
        .args(["-r"])
        .arg(f.work.join("git"))
        .arg(f.work.join("map"))
        .arg(&copy)
        .status()
        .unwrap();
    assert!(status.success());
    Exporter::new(
        gix::open(copy.join("git")).unwrap(),
        Map::open(&copy.join("map")).unwrap(),
        CommitStyle {
            provenance_trailers: false,
            changeset_trailer: true,
        },
    )
}

#[test]
fn without_import_a_remote_ahead_is_reported_and_left_alone() {
    let f = fixture();
    let leep = Local::new();
    let c1 = leep.native(
        vec![],
        "one",
        vec![leep.file("a", "1\n", FileKind::Regular)],
    );
    leep.set_ref("main", c1);
    let mut m = mirror::Mirror::open(config(&f, false), &leep).unwrap();
    assert!(m.run_once());
    git(
        f.human.parent().unwrap(),
        &["clone", "-q", f.remote.to_str().unwrap(), "human"],
    );
    write(&f.human, "h.txt", "h\n");
    commit(&f.human, "human");
    git(&f.human, &["push", "-q", "origin", "main"]);
    assert!(!m.run_once());
    assert_eq!(state(&m, "main").0, RefState::RemoteAhead);
    assert_eq!(leep.get_ref("main"), Some(c1));
}
