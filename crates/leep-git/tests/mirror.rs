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
use leep_core::api::{LandResponse, MergeRequest, MergeResponse, RebaseOntoResponse, ReviewEntry};
use leep_core::memory::{MemoryBlobStore, MemoryMetaStore};
use leep_core::review::{Review, ReviewSource, Verdict};
use leep_core::{
    Changeset, ChangesetId, FileChange, FileKind, Hash, LandId, RefUpdateContext, RepoPath,
};
use leep_derive::Repo;
use leep_git::mirror::export::{CommitStyle, Exporter};
use leep_git::mirror::github::{GhReview, GitHubApi, GitHubConfig, Pull};
use leep_git::mirror::map::Map;
use leep_git::mirror::{
    self, Cas, Config, LandOutcome, LandState, Leep, MergeOutcome, OnDivergence, Primary, RefSpec,
    RefState, ReplayOutcome,
};
use leep_git::{export, import, ExportOptions, ImportOptions};

/// Run git with a fixed identity and no user configuration; the caller
/// decides what a non-zero status means.
fn git_output(dir: &Path, args: &[&str]) -> std::process::Output {
    Command::new("git")
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
        .expect("run git")
}

fn git(dir: &Path, args: &[&str]) -> String {
    let out = git_output(dir, args);
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
    /// Merges asked for.
    merges: std::sync::Mutex<u32>,
    /// Replays (`rebase-onto`) asked for.
    replays: std::sync::Mutex<u32>,
    /// Ref lists `refs` answers before the real one, oldest first: a list
    /// read just before a ref was recreated.
    stale_refs: std::sync::Mutex<Vec<BTreeMap<String, ChangesetId>>>,
    /// The server is unreachable: every ref read fails.
    down: std::sync::Mutex<bool>,
    /// Principals whose reviews are listed (the server reads its tokens
    /// file for this).
    reviewers: std::sync::Mutex<Vec<String>>,
    /// Reviews posted through `post_review`, oldest first.
    posted: std::sync::Mutex<Vec<(ChangesetId, String, Verdict)>>,
    /// Fail every `post_review` once this many have been posted.
    fail_posts_after: std::sync::Mutex<Option<usize>>,
}

impl Local {
    fn new() -> Local {
        Local::with_repo(Repo::new(
            Arc::new(MemoryBlobStore::new()),
            Arc::new(MemoryMetaStore::new()),
        ))
    }

    fn with_repo(repo: Repo) -> Local {
        Local {
            repo,
            rt: tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap(),
            merges: std::sync::Mutex::new(0),
            replays: std::sync::Mutex::new(0),
            stale_refs: std::sync::Mutex::new(Vec::new()),
            down: std::sync::Mutex::new(false),
            reviewers: std::sync::Mutex::new(vec!["alice".into(), "bob".into()]),
            posted: std::sync::Mutex::new(Vec::new()),
            fail_posts_after: std::sync::Mutex::new(None),
        }
    }

    fn review(&self, id: ChangesetId, reviewer: &str) -> Option<Review> {
        self.on(self.repo.get_review(id, reviewer)).unwrap()
    }

    fn check_up(&self) -> Result<()> {
        if *self.down.lock().unwrap() {
            anyhow::bail!("connection refused");
        }
        Ok(())
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

    fn delete_ref(&self, name: &str) {
        let meta = self.repo.meta();
        let cur = self.on(meta.get_ref(name)).unwrap();
        assert!(self
            .on(meta.cas_ref_logged(name, cur, None, &RefUpdateContext::new("delete")))
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
        self.check_up()?;
        let mut stale = self.stale_refs.lock().unwrap();
        if !stale.is_empty() {
            return Ok(stale.remove(0));
        }
        Ok(self.on(self.repo.meta().list_refs())?)
    }

    fn ref_log_head(&self) -> Result<Option<u64>> {
        self.check_up()?;
        Ok(self
            .on(self.repo.meta().ref_log(None, None, 1))?
            .first()
            .map(|e| e.seq))
    }

    fn ref_had(&self, name: &str, id: &ChangesetId) -> Result<bool> {
        self.check_up()?;
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
        match self.on(self.repo.cas_ref_checked(name, expected, Some(new), &ctx)) {
            Ok(true) => Ok(Cas::Won),
            Ok(false) => Ok(Cas::Lost(self.get_ref(name))),
            Err(leep_core::Error::Invalid(m)) => Ok(Cas::Refused(m)),
            Err(e) => Err(e.into()),
        }
    }

    fn merge(&self, req: &MergeRequest) -> Result<MergeOutcome> {
        *self.merges.lock().unwrap() += 1;
        Ok(match self.on(self.repo.merge(req.clone()))? {
            MergeResponse::Merged { id } => MergeOutcome::Clean(id),
            MergeResponse::Conflicts { conflicts, .. } => MergeOutcome::Conflicts(
                conflicts
                    .into_iter()
                    .map(|c| c.path.as_str().to_string())
                    .collect(),
            ),
        })
    }

    fn land(
        &self,
        changeset: ChangesetId,
        target_ref: &str,
        land_id: Option<LandId>,
    ) -> Result<LandOutcome> {
        match self.on(self.repo.land_keyed(
            changeset,
            target_ref,
            Some("git-mirror"),
            false,
            land_id,
        )) {
            Ok(LandResponse::Landed { landed, .. }) => Ok(LandOutcome::Landed(landed)),
            Ok(LandResponse::Conflicts { conflicts, .. }) => Ok(LandOutcome::Conflicts(
                conflicts
                    .into_iter()
                    .map(|c| c.path.as_str().to_string())
                    .collect(),
            )),
            Err(leep_core::Error::Invalid(m) | leep_core::Error::NotFound(m)) => {
                Ok(LandOutcome::Refused(m))
            }
            Err(e) => Err(e.into()),
        }
    }

    fn merge_base(&self, a: ChangesetId, b: ChangesetId) -> Result<Option<ChangesetId>> {
        Ok(self.on(self.repo.merge_base(a, b))?)
    }

    fn delete_ref(&self, name: &str, expected: ChangesetId, message: &str) -> Result<Cas> {
        let ctx = RefUpdateContext::new(mirror::CAS_OPERATION).message(message);
        match self.on(self.repo.cas_ref_checked(name, Some(expected), None, &ctx)) {
            Ok(true) => Ok(Cas::Won),
            Ok(false) => Ok(Cas::Lost(self.get_ref(name))),
            Err(leep_core::Error::Invalid(m)) => Ok(Cas::Refused(m)),
            Err(e) => Err(e.into()),
        }
    }

    fn rebase_onto(
        &self,
        name: &str,
        onto: ChangesetId,
        expected_base: ChangesetId,
        land_id: LandId,
    ) -> Result<ReplayOutcome> {
        *self.replays.lock().unwrap() += 1;
        match self.on(self.repo.rebase_onto(
            name,
            onto,
            expected_base,
            Some("git-mirror"),
            Some(land_id),
        )) {
            Ok(RebaseOntoResponse::Moved { head, rebased }) => {
                Ok(ReplayOutcome::Moved { head, rebased })
            }
            Ok(RebaseOntoResponse::Conflicts { conflicts, .. }) => Ok(ReplayOutcome::Conflicts(
                conflicts
                    .into_iter()
                    .map(|c| c.path.as_str().to_string())
                    .collect(),
            )),
            Err(leep_core::Error::Conflict(_)) => Ok(ReplayOutcome::StaleBase),
            Err(leep_core::Error::Invalid(m) | leep_core::Error::NotFound(m)) => {
                Ok(ReplayOutcome::Refused(m))
            }
            Err(e) => Err(e.into()),
        }
    }

    fn reviews(&self, id: ChangesetId) -> Result<Vec<ReviewEntry>> {
        let mut out = Vec::new();
        for name in self.reviewers.lock().unwrap().iter() {
            let Some(review) = self.on(self.repo.get_review(id, name))? else {
                continue;
            };
            let row = self.on(self.repo.get_review_source(id, name))?;
            out.push(ReviewEntry {
                source: row.as_ref().map(|s| s.source.clone()),
                via: row.map(|s| s.via),
                review,
            });
        }
        Ok(out)
    }

    fn post_review(
        &self,
        id: ChangesetId,
        reviewer: &str,
        verdict: Verdict,
        source: &str,
    ) -> Result<()> {
        if let Some(n) = *self.fail_posts_after.lock().unwrap() {
            if self.posted.lock().unwrap().len() >= n {
                anyhow::bail!("connection reset while posting a review");
            }
        }
        // As the server does: the source row first, then the review.
        self.on(self.repo.put_review_source(
            id,
            reviewer,
            &ReviewSource {
                via: "git-mirror".into(),
                source: source.into(),
                timestamp: 1,
            },
        ))?;
        self.on(self.repo.put_review(
            id,
            &Review {
                reviewer: reviewer.into(),
                verdict,
                comment: String::new(),
                timestamp: 1,
            },
        ))?;
        self.posted
            .lock()
            .unwrap()
            .push((id, reviewer.into(), verdict));
        Ok(())
    }
}

/// GitHub as a map: pull requests by branch and reviews by number.
#[derive(Default)]
struct FakeGitHub {
    pulls: std::sync::Mutex<BTreeMap<String, Pull>>,
    reviews: std::sync::Mutex<BTreeMap<u64, Vec<GhReview>>>,
    /// Fail every call while set.
    down: std::sync::Mutex<bool>,
}

impl FakeGitHub {
    fn open(&self, branch: &str, number: u64, head_sha: &str, base: &str) {
        self.pulls.lock().unwrap().insert(
            branch.into(),
            Pull {
                number,
                head_sha: head_sha.into(),
                base_branch: base.into(),
            },
        );
    }

    fn review(&self, number: u64, id: u64, login: &str, state: &str, sha: &str) {
        self.reviews
            .lock()
            .unwrap()
            .entry(number)
            .or_default()
            .push(GhReview {
                id,
                login: login.into(),
                state: state.into(),
                commit_id: sha.into(),
            });
    }
}

/// The mirror's handle on a [`FakeGitHub`] the test keeps editing.
struct SharedGitHub(Arc<FakeGitHub>);

impl GitHubApi for SharedGitHub {
    fn open_pull(&self, _: &str, _: &str, branch: &str) -> Result<Option<Pull>> {
        if *self.0.down.lock().unwrap() {
            anyhow::bail!("GitHub answered 503");
        }
        Ok(self.0.pulls.lock().unwrap().get(branch).cloned())
    }

    fn reviews(&self, _: &str, _: &str, number: u64) -> Result<Vec<GhReview>> {
        if *self.0.down.lock().unwrap() {
            anyhow::bail!("GitHub answered 503");
        }
        Ok(self
            .0
            .reviews
            .lock()
            .unwrap()
            .get(&number)
            .cloned()
            .unwrap_or_default())
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
    // Exec bits and symlinks are Unix shapes; the rest of the history is
    // exercised everywhere.
    #[cfg(unix)]
    {
        std::fs::set_permissions(
            p.join("run.sh"),
            std::os::unix::fs::PermissionsExt::from_mode(0o755),
        )
        .unwrap();
        std::os::unix::fs::symlink("a.txt", p.join("link")).unwrap();
    }
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
        primary: Primary::Leep,
        tags: true,
        ref_prefixes: vec![],
        import,
        allow_rewind: vec![],
        diverged_prefix: "leep-diverged/".into(),
        on_divergence: OnDivergence::Report,
        merge_author: mirror::DEFAULT_MERGE_AUTHOR.into(),
        land_prefix: None,
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
        github: None,
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

fn merge_config(f: &Fixture) -> Config {
    Config {
        on_divergence: OnDivergence::Merge,
        land_prefix: Some("leep-land/".into()),
        ..config(f, true)
    }
}

fn clone_human(f: &Fixture) {
    git(
        f.human.parent().unwrap(),
        &["clone", "-q", f.remote.to_str().unwrap(), "human"],
    );
}

fn is_ancestor(dir: &Path, a: &str, b: &str) -> bool {
    Command::new("git")
        .args(["merge-base", "--is-ancestor", a, b])
        .current_dir(dir)
        .status()
        .unwrap()
        .success()
}

fn merges(m: &mirror::Mirror) -> mirror::status::MergeCounts {
    m.status.lock().unwrap().refs[0].merges.clone()
}

#[test]
fn merge_mode_lands_a_clean_merge_and_pushes_it_as_a_fast_forward() {
    let f = fixture();
    let leep = Local::new();
    let c1 = leep.native(
        vec![],
        "one",
        vec![leep.file("a", "1\n", FileKind::Regular)],
    );
    leep.set_ref("main", c1);
    let mut m = mirror::Mirror::open(merge_config(&f), &leep).unwrap();
    assert!(m.run_once());

    // Both sides move, on different files.
    clone_human(&f);
    write(&f.human, "human.txt", "from git\n");
    commit(&f.human, "human change");
    git(&f.human, &["push", "-q", "origin", "main"]);
    let human = git(&f.human, &["rev-parse", "HEAD"]);
    let agent = leep.native(
        vec![c1],
        "agent change",
        vec![leep.file("agent.txt", "agent\n", FileKind::Regular)],
    );
    leep.set_ref("main", agent);

    assert!(m.run_once(), "{:?}", state(&m, "main"));
    assert_eq!(state(&m, "main").0, RefState::InSync);
    let head = leep.get_ref("main").unwrap();
    let merge = leep.on(leep.repo.get_changeset(head)).unwrap();
    assert_eq!(merge.parents.len(), 2);
    assert_eq!(merge.parents[0], agent, "Leep's side is the first parent");
    let theirs = leep.on(leep.repo.get_changeset(merge.parents[1])).unwrap();
    assert_eq!(theirs.extra["git.commit"], human);
    assert_eq!(merge.author, mirror::DEFAULT_MERGE_AUTHOR);
    // Deterministic: the newer parent's time, not the clock.
    assert_eq!(merge.timestamp, theirs.timestamp.max(1_750_000_000));
    assert_eq!(merge.extra[mirror::MERGE_EXTRA], human);
    let log = leep
        .on(leep.repo.meta().ref_log(Some("main"), None, 1))
        .unwrap();
    assert_eq!(log[0].operation, mirror::CAS_OPERATION);
    assert_eq!(log[0].new, Some(head));

    // The remote fast-forwarded from the person's commit to the merge, and
    // never saw a diverged branch.
    let tip = remote_head(&f, "refs/heads/main").unwrap();
    assert!(is_ancestor(&f.remote, &human, &tip));
    assert_eq!(remote_head(&f, "refs/heads/leep-diverged/main"), None);
    let body = git(&f.remote, &["log", "-1", "--format=%B", "main"]);
    assert!(body.starts_with("Merge main from git into main"), "{body}");
    assert!(body.ends_with(&format!("Leep-Changeset: {head}")), "{body}");
    assert_eq!(git(&f.remote, &["show", "main:agent.txt"]), "agent");
    assert_eq!(git(&f.remote, &["show", "main:human.txt"]), "from git");
    let counts = merges(&m);
    assert_eq!((counts.landed, counts.conflicts), (1, 0));
    assert_eq!(m.status.lock().unwrap().refs[0].divergences, 0);

    // Nothing left to do: no second merge, no second push.
    let pushes = m.status.lock().unwrap().refs[0].pushes.ok;
    assert!(m.run_once());
    assert_eq!(merges(&m).landed, 1);
    assert_eq!(m.status.lock().unwrap().refs[0].pushes.ok, pushes);
    assert_eq!(leep.get_ref("main"), Some(head));

    // The same two sides always give the same merge: a restarted mirror
    // that asks again (after a crash before the CAS) gets this changeset.
    let again = leep
        .on(leep.repo.merge(MergeRequest {
            parents: [agent, merge.parents[1]],
            message: merge.message.clone(),
            authorship: leep_core::api::Authorship {
                author: merge.author.clone(),
                timestamp: Some(merge.timestamp),
            },
            extra: merge.extra.clone(),
        }))
        .unwrap();
    assert_eq!(again, MergeResponse::Merged { id: head });
}

#[test]
fn merge_mode_reports_a_conflicting_divergence_and_never_resolves_it() {
    let f = fixture();
    let leep = Local::new();
    let c1 = leep.native(
        vec![],
        "one",
        vec![leep.file("a", "1\n", FileKind::Regular)],
    );
    leep.set_ref("main", c1);
    let mut m = mirror::Mirror::open(merge_config(&f), &leep).unwrap();
    assert!(m.run_once());
    clone_human(&f);
    write(&f.human, "a", "git\n");
    commit(&f.human, "human edits a");
    git(&f.human, &["push", "-q", "origin", "main"]);
    let human = git(&f.human, &["rev-parse", "HEAD"]);
    let agent = leep.native(
        vec![c1],
        "agent edits a",
        vec![leep.file("a", "leep\n", FileKind::Regular)],
    );
    leep.set_ref("main", agent);

    assert!(!m.run_once());
    let (st, detail) = state(&m, "main");
    assert_eq!(st, RefState::Diverged, "{detail}");
    assert!(
        detail.contains("not merged automatically: 1 conflicting path: a"),
        "{detail}"
    );
    assert!(detail.contains("refs/heads/leep-diverged/main"), "{detail}");
    assert_eq!(leep.get_ref("main"), Some(agent));
    assert_eq!(remote_head(&f, "refs/heads/main").unwrap(), human);
    assert!(remote_head(&f, "refs/heads/leep-diverged/main").is_some());
    assert_eq!(merges(&m).conflicts, 1);
    // The same pair is not merged again every cycle.
    let asked = *leep.merges.lock().unwrap();
    assert!(!m.run_once());
    assert_eq!(*leep.merges.lock().unwrap(), asked);
    assert_eq!(merges(&m).conflicts, 1);

    // A person resolves it in git; the merge imports as a fast-forward.
    git(&f.human, &["fetch", "-q", "origin"]);
    let ok = Command::new("git")
        .args(["merge", "-q", "--no-edit", "origin/leep-diverged/main"])
        .current_dir(&f.human)
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_AUTHOR_NAME", "A")
        .env("GIT_AUTHOR_EMAIL", "a@example.com")
        .env("GIT_COMMITTER_NAME", "A")
        .env("GIT_COMMITTER_EMAIL", "a@example.com")
        .output()
        .unwrap()
        .status
        .success();
    assert!(!ok, "the merge conflicts in git too");
    write(&f.human, "a", "both\n");
    commit(&f.human, "resolve");
    git(&f.human, &["push", "-q", "origin", "main"]);
    let resolved = git(&f.human, &["rev-parse", "HEAD"]);
    assert!(m.run_once(), "{:?}", state(&m, "main"));
    assert_eq!(remote_head(&f, "refs/heads/main").unwrap(), resolved);
    assert_eq!(remote_head(&f, "refs/heads/leep-diverged/main"), None);
    let head = leep.get_ref("main").unwrap();
    let cs = leep.on(leep.repo.get_changeset(head)).unwrap();
    assert_eq!(cs.extra["git.commit"], resolved);
    assert_eq!(merges(&m).landed, 0);
}

#[test]
fn merge_mode_does_not_merge_back_what_leep_moved_back_from() {
    let f = fixture();
    let leep = Local::new();
    let c1 = leep.native(
        vec![],
        "one",
        vec![leep.file("a", "1\n", FileKind::Regular)],
    );
    let c2 = leep.native(
        vec![c1],
        "two",
        vec![leep.file("b", "2\n", FileKind::Regular)],
    );
    leep.set_ref("main", c2);
    let mut m = mirror::Mirror::open(merge_config(&f), &leep).unwrap();
    assert!(m.run_once());
    let pushed = remote_head(&f, "refs/heads/main").unwrap();
    // Undo two, then land something else: the remote's tip is Leep's own.
    leep.set_ref("main", c1);
    let c3 = leep.native(
        vec![c1],
        "three",
        vec![leep.file("c", "3\n", FileKind::Regular)],
    );
    leep.set_ref("main", c3);
    assert!(!m.run_once());
    let (st, detail) = state(&m, "main");
    assert_eq!(st, RefState::Diverged, "{detail}");
    assert!(detail.contains("came from Leep"), "{detail}");
    assert_eq!(leep.get_ref("main"), Some(c3));
    assert_eq!(remote_head(&f, "refs/heads/main").unwrap(), pushed);
    assert_eq!(*leep.merges.lock().unwrap(), 0);
}

fn land_branch(m: &mirror::Mirror, branch: &str) -> Option<mirror::LandBranchStatus> {
    m.status
        .lock()
        .unwrap()
        .land_branches
        .iter()
        .find(|b| b.branch == format!("refs/heads/{branch}"))
        .cloned()
}

#[test]
fn land_branches_land_through_the_server_and_are_deleted() {
    let f = fixture();
    let leep = Local::new();
    let c1 = leep.native(
        vec![],
        "one",
        vec![leep.file("a", "1\n", FileKind::Regular)],
    );
    leep.set_ref("main", c1);
    let mut m = mirror::Mirror::open(merge_config(&f), &leep).unwrap();
    assert!(m.run_once());
    clone_human(&f);

    // A land branch on top of the current main fast-forwards: the person's
    // commit itself lands and becomes the remote's main.
    write(&f.human, "ff.txt", "ff\n");
    commit(&f.human, "fast-forward land");
    git(
        &f.human,
        &["push", "-q", "origin", "HEAD:refs/heads/leep-land/main/ff"],
    );
    let ff = git(&f.human, &["rev-parse", "HEAD"]);
    assert!(m.run_once(), "{:?}", state(&m, "main"));
    assert_eq!(remote_head(&f, "refs/heads/main").unwrap(), ff);
    assert_eq!(remote_head(&f, "refs/heads/leep-land/main/ff"), None);
    assert!(land_branch(&m, "leep-land/main/ff").is_none());
    let head = leep.get_ref("main").unwrap();
    let cs = leep.on(leep.repo.get_changeset(head)).unwrap();
    assert_eq!(cs.extra["git.commit"], ff);

    // An agent lands meanwhile: the next land branch is rebased onto it by
    // the server, like an agent's land, and its copy gets a trailer.
    let agent = leep.native(
        vec![head],
        "agent",
        vec![leep.file("agent.txt", "agent\n", FileKind::Regular)],
    );
    leep.set_ref("main", agent);
    write(&f.human, "feature.txt", "feature\n");
    commit(&f.human, "feature work");
    write(&f.human, "feature2.txt", "more\n");
    commit(&f.human, "more feature work");
    git(
        &f.human,
        &[
            "push",
            "-q",
            "origin",
            "HEAD:refs/heads/leep-land/main/people/feature",
        ],
    );
    let feature = git(&f.human, &["rev-parse", "HEAD"]);
    assert!(m.run_once(), "{:?}", state(&m, "main"));
    assert_eq!(state(&m, "main").0, RefState::InSync);
    assert_eq!(
        remote_head(&f, "refs/heads/leep-land/main/people/feature"),
        None
    );
    let head = leep.get_ref("main").unwrap();
    let copy = leep.on(leep.repo.get_changeset(head)).unwrap();
    assert_eq!(copy.message, "more feature work\n");
    assert!(copy.extra.contains_key("leep.rebased_from"));
    let parent = leep.on(leep.repo.get_changeset(copy.parents[0])).unwrap();
    assert_eq!(parent.parents, vec![agent]);
    let tip = remote_head(&f, "refs/heads/main").unwrap();
    assert_ne!(tip, feature, "rebased, so a new commit");
    let body = git(&f.remote, &["log", "-1", "--format=%B", "main"]);
    assert_eq!(body, format!("more feature work\n\nLeep-Changeset: {head}"));
    assert_eq!(git(&f.remote, &["show", "main:feature2.txt"]), "more");
    assert_eq!(git(&f.remote, &["show", "main:agent.txt"]), "agent");
    let log = git(&f.remote, &["log", "--format=%s", "main"]);
    assert_eq!(
        log,
        "more feature work\nfeature work\nagent\nfast-forward land\none"
    );
    let counts = m.status.lock().unwrap().refs[0].land_branches.clone();
    assert_eq!((counts.landed, counts.conflicts), (2, 0));
    // A restart lands nothing twice.
    drop(m);
    let mut m = mirror::Mirror::open(merge_config(&f), &leep).unwrap();
    assert!(m.run_once());
    assert_eq!(leep.get_ref("main"), Some(head));
    assert_eq!(remote_head(&f, "refs/heads/main").unwrap(), tip);
}

#[test]
fn failing_land_branches_are_reported_and_left_in_place() {
    let f = fixture();
    let leep = Local::new();
    let c1 = leep.native(
        vec![],
        "one",
        vec![leep.file("a", "1\n", FileKind::Regular)],
    );
    leep.set_ref("main", c1);
    let mut m = mirror::Mirror::open(merge_config(&f), &leep).unwrap();
    assert!(m.run_once());
    clone_human(&f);
    let base = git(&f.human, &["rev-parse", "HEAD"]);

    // Conflicts with what an agent landed.
    let agent = leep.native(
        vec![c1],
        "agent edits a",
        vec![leep.file("a", "leep\n", FileKind::Regular)],
    );
    leep.set_ref("main", agent);
    write(&f.human, "a", "git\n");
    commit(&f.human, "human edits a");
    git(
        &f.human,
        &[
            "push",
            "-q",
            "origin",
            "HEAD:refs/heads/leep-land/main/clash",
        ],
    );
    let clash = git(&f.human, &["rev-parse", "HEAD"]);

    // Names no mirrored ref.
    git(
        &f.human,
        &["push", "-q", "origin", "HEAD:refs/heads/leep-land/nope/x"],
    );

    // Holds a merge, which land does not rebase.
    git(&f.human, &["checkout", "-q", "-b", "side", &base]);
    write(&f.human, "s1", "s\n");
    commit(&f.human, "side");
    git(&f.human, &["checkout", "-q", "-b", "merged", &base]);
    write(&f.human, "s2", "s\n");
    commit(&f.human, "other");
    git(&f.human, &["merge", "-q", "--no-edit", "side"]);
    git(
        &f.human,
        &[
            "push",
            "-q",
            "origin",
            "HEAD:refs/heads/leep-land/main/merge",
        ],
    );

    // The ref itself stays healthy: land branches are the pusher's to fix.
    assert!(m.run_once(), "{:?}", state(&m, "main"));
    let b = land_branch(&m, "leep-land/main/clash").unwrap();
    assert_eq!(b.state, LandState::Conflict, "{}", b.detail);
    assert!(b.detail.contains("path: a"), "{}", b.detail);
    assert_eq!(b.commit, clash);
    assert_eq!(b.leep_ref.as_deref(), Some("main"));
    let b = land_branch(&m, "leep-land/nope/x").unwrap();
    assert_eq!(b.state, LandState::Refused);
    assert!(b.detail.contains("names no mirrored ref"), "{}", b.detail);
    assert_eq!(b.leep_ref, None);
    let b = land_branch(&m, "leep-land/main/merge").unwrap();
    assert_eq!(b.state, LandState::Refused, "{}", b.detail);
    assert!(b.detail.contains("is a merge"), "{}", b.detail);
    for name in ["clash", "merge"] {
        assert!(remote_head(&f, &format!("refs/heads/leep-land/main/{name}")).is_some());
    }
    assert_eq!(leep.get_ref("main"), Some(agent));
    let counts = m.status.lock().unwrap().refs[0].land_branches.clone();
    assert_eq!((counts.landed, counts.conflicts, counts.refused), (0, 1, 1));
    let metrics = mirror::status::render_metrics(&m.status.lock().unwrap());
    assert!(
        metrics.contains("leep_mirror_land_branches_failing{ref=\"main\"} 2"),
        "{metrics}"
    );
    assert!(
        metrics.contains("leep_mirror_land_branches_total{ref=\"main\",result=\"conflicts\"} 1")
    );

    // Retries count once; a branch deleted by its owner leaves the status.
    std::thread::sleep(Duration::from_millis(1100));
    assert!(m.run_once());
    let counts = m.status.lock().unwrap().refs[0].land_branches.clone();
    assert_eq!((counts.conflicts, counts.refused), (1, 1));
    git(
        &f.human,
        &["push", "-q", "origin", ":refs/heads/leep-land/main/merge"],
    );
    assert!(m.run_once());
    assert!(land_branch(&m, "leep-land/main/merge").is_none());

    // Fixed and pushed again, the branch lands.
    git(&f.human, &["checkout", "-q", "main"]);
    git(&f.human, &["fetch", "-q", "origin"]);
    git(&f.human, &["reset", "-q", "--hard", "origin/main"]);
    write(&f.human, "a", "both\n");
    commit(&f.human, "human edits a again");
    git(
        &f.human,
        &[
            "push",
            "-q",
            "-f",
            "origin",
            "HEAD:refs/heads/leep-land/main/clash",
        ],
    );
    assert!(m.run_once());
    assert!(land_branch(&m, "leep-land/main/clash").is_none());
    assert_eq!(git(&f.remote, &["show", "main:a"]), "both");
}

fn prefix_config(f: &Fixture, import: bool) -> Config {
    Config {
        ref_prefixes: vec!["review/".into()],
        ..config(f, import)
    }
}

fn tracked(m: &mirror::Mirror) -> Vec<String> {
    m.status
        .lock()
        .unwrap()
        .refs
        .iter()
        .map(|r| r.leep_ref.clone())
        .collect()
}

/// Leep's `main` at one changeset, and a review ref on top of it.
fn main_and_review(leep: &Local, review: &str) -> (ChangesetId, ChangesetId) {
    let c1 = leep.native(
        vec![],
        "one",
        vec![leep.file("a", "1\n", FileKind::Regular)],
    );
    leep.set_ref("main", c1);
    let r1 = leep.native(
        vec![c1],
        "review work",
        vec![leep.file("r", "r1\n", FileKind::Regular)],
    );
    leep.set_ref(review, r1);
    (c1, r1)
}

/// A person's commit on top of the remote's `branch`, pushed back to it.
fn push_on_top(f: &Fixture, branch: &str, file: &str) -> String {
    if !f.human.exists() {
        clone_human(f);
    }
    git(&f.human, &["fetch", "-q", "origin"]);
    git(
        &f.human,
        &["checkout", "-q", "-B", "work", &format!("origin/{branch}")],
    );
    write(&f.human, file, "pushed to git\n");
    commit(&f.human, "pushed straight to git");
    git(
        &f.human,
        &["push", "-q", "origin", &format!("HEAD:refs/heads/{branch}")],
    );
    git(&f.human, &["rev-parse", "HEAD"])
}

#[test]
fn prefix_refs_are_discovered_and_pushed_and_rewrites_go_with_a_lease() {
    let f = fixture();
    let leep = Local::new();
    let (c1, r1) = main_and_review(&leep, "review/git-a");
    leep.set_ref("scratch", r1);
    let mut m = mirror::Mirror::open(prefix_config(&f, false), &leep).unwrap();
    assert!(m.run_once());
    let main = remote_head(&f, "refs/heads/main").unwrap();
    let a1 = remote_head(&f, "refs/heads/review/git-a").unwrap();
    assert_eq!(git(&f.remote, &["rev-parse", "review/git-a~1"]), main);
    assert_eq!(state(&m, "review/git-a").0, RefState::InSync);
    {
        let s = m.status.lock().unwrap();
        let r = s
            .refs
            .iter()
            .find(|r| r.leep_ref == "review/git-a")
            .unwrap();
        assert_eq!(r.ref_prefix.as_deref(), Some("review/"));
        assert_eq!(r.git_ref, "refs/heads/review/git-a");
        let main = s.refs.iter().find(|r| r.leep_ref == "main").unwrap();
        assert_eq!(main.ref_prefix, None);
    }
    // Only refs under the prefix are found.
    assert_eq!(remote_head(&f, "refs/heads/scratch"), None);
    assert!(!tracked(&m).contains(&"scratch".to_string()));

    // More work on the review ref is a fast-forward.
    let r2 = leep.native(
        vec![r1],
        "more review work",
        vec![leep.file("r", "r2\n", FileKind::Regular)],
    );
    leep.set_ref("review/git-a", r2);
    assert!(m.run_once());
    let a2 = remote_head(&f, "refs/heads/review/git-a").unwrap();
    assert!(is_ancestor(&f.remote, &a1, &a2));

    // The agent squashes: the branch is rewritten with a lease on what the
    // mirror pushed, never forced blind.
    let squashed = leep.native(
        vec![c1],
        "review work, squashed",
        vec![leep.file("r", "r2\n", FileKind::Regular)],
    );
    leep.set_ref("review/git-a", squashed);
    assert!(m.run_once(), "{:?}", state(&m, "review/git-a"));
    let a3 = remote_head(&f, "refs/heads/review/git-a").unwrap();
    assert!(!is_ancestor(&f.remote, &a2, &a3));
    assert_eq!(
        git(&f.remote, &["log", "-1", "--format=%s", "review/git-a"]),
        "review work, squashed"
    );

    // A ref that appears later is found on the next cycle.
    leep.set_ref("review/cli-b", squashed);
    assert!(m.run_once());
    assert_eq!(remote_head(&f, "refs/heads/review/cli-b").unwrap(), a3);
    assert_eq!(state(&m, "review/cli-b").0, RefState::InSync);
    assert_eq!(remote_head(&f, "refs/heads/main").unwrap(), main);
}

#[test]
fn prefix_refs_deleted_in_leep_are_deleted_on_the_remote_with_a_lease() {
    let f = fixture();
    let leep = Local::new();
    let (_, r1) = main_and_review(&leep, "review/git-a");
    leep.set_ref("review/git-b", r1);
    let mut m = mirror::Mirror::open(prefix_config(&f, false), &leep).unwrap();
    assert!(m.run_once());
    let main = remote_head(&f, "refs/heads/main").unwrap();
    let a = remote_head(&f, "refs/heads/review/git-a").unwrap();
    assert!(remote_head(&f, "refs/heads/review/git-b").is_some());

    // Deleted in Leep: the branch goes too, and the mirror stays healthy.
    leep.delete_ref("review/git-a");
    assert!(m.run_once());
    assert_eq!(remote_head(&f, "refs/heads/review/git-a"), None);
    assert!(!tracked(&m).contains(&"review/git-a".to_string()));
    {
        let s = m.status.lock().unwrap();
        assert_eq!(s.branches_deleted, 1);
        assert_eq!(s.deleted_branches.len(), 1);
        let d = &s.deleted_branches[0];
        assert_eq!(
            (d.leep_ref.as_str(), d.git_ref.as_str(), d.commit.as_str()),
            ("review/git-a", "refs/heads/review/git-a", a.as_str())
        );
        let metrics = mirror::status::render_metrics(&s);
        assert!(
            metrics.contains("leep_mirror_branches_deleted_total 1"),
            "{metrics}"
        );
        let json = serde_json::to_string(&*s).unwrap();
        let back: mirror::Status = serde_json::from_str(&json).unwrap();
        assert_eq!(back, *s);
    }

    // Deleted while the mirror was down: a new mirror finds the branch
    // under the prefix, sees in the ref log that the Leep ref held it, and
    // deletes it with the same lease.
    drop(m);
    leep.delete_ref("review/git-b");
    let mut m = mirror::Mirror::open(prefix_config(&f, false), &leep).unwrap();
    assert!(m.run_once());
    assert_eq!(remote_head(&f, "refs/heads/review/git-b"), None);
    assert_eq!(m.status.lock().unwrap().branches_deleted, 1);
    assert_eq!(tracked(&m), vec!["main".to_string()]);
    assert_eq!(remote_head(&f, "refs/heads/main").unwrap(), main);
}

#[test]
fn a_prefix_branch_the_remote_moved_is_reported_and_never_deleted() {
    let f = fixture();
    let leep = Local::new();
    main_and_review(&leep, "review/git-a");
    let mut m = mirror::Mirror::open(prefix_config(&f, false), &leep).unwrap();
    assert!(m.run_once());

    // Someone pushes to the branch after the mirror last polled; the Leep
    // ref is deleted. The delete's lease names what the mirror pushed, so
    // the remote refuses it.
    let pushed = push_on_top(&f, "review/git-a", "late.txt");
    leep.delete_ref("review/git-a");
    m.cycle();
    assert_eq!(
        remote_head(&f, "refs/heads/review/git-a").unwrap(),
        pushed,
        "a lease that no longer matches deletes nothing"
    );
    {
        let s = m.status.lock().unwrap();
        let r = s
            .refs
            .iter()
            .find(|r| r.leep_ref == "review/git-a")
            .unwrap();
        assert_eq!(r.pushes.rejected, 1, "{}", r.detail);
        assert_eq!(s.branches_deleted, 0);
    }

    // With the new value read, it is reported and left alone.
    assert!(!m.run_once());
    let (st, detail) = state(&m, "review/git-a");
    assert_eq!(st, RefState::RemoteAhead, "{detail}");
    assert!(detail.contains("never held in Leep"), "{detail}");
    assert_eq!(remote_head(&f, "refs/heads/review/git-a").unwrap(), pushed);

    // A branch under the prefix that Leep never had is reported too.
    git(
        &f.human,
        &["push", "-q", "origin", "HEAD:refs/heads/review/human-only"],
    );
    drop(m);
    let mut m = mirror::Mirror::open(prefix_config(&f, false), &leep).unwrap();
    assert!(!m.run_once());
    assert_eq!(state(&m, "review/human-only").0, RefState::RemoteAhead);
    assert_eq!(
        remote_head(&f, "refs/heads/review/human-only").unwrap(),
        pushed
    );
    assert_eq!(m.status.lock().unwrap().branches_deleted, 0);

    // Once a person deletes it, the mirror forgets it.
    git(
        &f.human,
        &[
            "push",
            "-q",
            "origin",
            ":refs/heads/review/human-only",
            ":refs/heads/review/git-a",
        ],
    );
    assert!(m.run_once());
    assert_eq!(tracked(&m), vec!["main".to_string()]);
}

#[test]
fn prefix_refs_are_push_only_without_import() {
    let f = fixture();
    let leep = Local::new();
    let (_, r1) = main_and_review(&leep, "review/git-a");
    let mut m = mirror::Mirror::open(prefix_config(&f, false), &leep).unwrap();
    assert!(m.run_once());
    let exported = remote_head(&f, "refs/heads/review/git-a").unwrap();

    // A push to the review branch: reported, not landed.
    let pushed = push_on_top(&f, "review/git-a", "pushed.txt");
    assert!(!m.run_once());
    let (st, detail) = state(&m, "review/git-a");
    assert_eq!(st, RefState::RemoteAhead, "{detail}");
    assert!(detail.contains("push-only"), "{detail}");
    assert_eq!(leep.get_ref("review/git-a"), Some(r1));
    assert_eq!(remote_head(&f, "refs/heads/review/git-a").unwrap(), pushed);
    assert_eq!(m.status.lock().unwrap().commits_imported, 0);

    // Leep moves too: diverged, but no merge, no diverged branch, no import.
    let r2 = leep.native(
        vec![r1],
        "agent keeps working",
        vec![leep.file("r", "r2\n", FileKind::Regular)],
    );
    leep.set_ref("review/git-a", r2);
    assert!(!m.run_once());
    let (st, detail) = state(&m, "review/git-a");
    assert_eq!(st, RefState::Diverged, "{detail}");
    assert_eq!(leep.get_ref("review/git-a"), Some(r2));
    assert_eq!(remote_head(&f, "refs/heads/review/git-a").unwrap(), pushed);
    assert_eq!(
        remote_head(&f, "refs/heads/leep-diverged/review/git-a"),
        None
    );
    assert_eq!(*leep.merges.lock().unwrap(), 0);

    // Deleting the pushed branch lets the mirror push Leep's again.
    git(
        &f.human,
        &["push", "-q", "origin", ":refs/heads/review/git-a"],
    );
    assert!(m.run_once());
    assert_eq!(state(&m, "review/git-a").0, RefState::InSync);
    let now = remote_head(&f, "refs/heads/review/git-a").unwrap();
    assert!(is_ancestor(&f.remote, &exported, &now));
}

/// Two-way prefix refs (ADR 0062): a person's commits on a review branch
/// are imported, and land on the Leep ref when an agent rewrote it.
fn two_way_config(f: &Fixture) -> Config {
    Config {
        land_prefix: Some("leep-land/".into()),
        ..prefix_config(f, true)
    }
}

fn changeset(leep: &Local, id: ChangesetId) -> Changeset {
    leep.on(leep.repo.get_changeset(id)).unwrap()
}

#[test]
fn two_way_prefix_refs_import_a_persons_commits_and_land_them_on_a_rewrite() {
    let f = fixture();
    let leep = Local::new();
    let (c1, r1) = main_and_review(&leep, "review/git-a");
    let mut m = mirror::Mirror::open(two_way_config(&f), &leep).unwrap();
    assert!(m.run_once());

    // On top of what the mirror pushed: a fast-forward import, nothing
    // pushed back.
    let p1 = push_on_top(&f, "review/git-a", "late.txt");
    assert!(m.run_once(), "{:?}", state(&m, "review/git-a"));
    assert_eq!(state(&m, "review/git-a").0, RefState::InSync);
    let head = leep.get_ref("review/git-a").unwrap();
    let cs = changeset(&leep, head);
    assert_eq!(cs.extra["git.commit"], p1);
    assert_eq!(cs.parents, vec![r1]);
    assert_eq!(remote_head(&f, "refs/heads/review/git-a").unwrap(), p1);
    let log = leep
        .on(leep.repo.meta().ref_log(Some("review/git-a"), None, 1))
        .unwrap();
    assert_eq!(log[0].operation, mirror::CAS_OPERATION);

    // The agent squashes the stack while the person pushes again: the
    // person's new commit is landed onto the squash and the branch is
    // rewritten with a lease on the exact tip imported.
    let squashed = leep.native(
        vec![c1],
        "squashed",
        vec![
            leep.file("r", "r1\n", FileKind::Regular),
            leep.file("late.txt", "pushed to git\n", FileKind::Regular),
        ],
    );
    leep.set_ref("review/git-a", squashed);
    let p2 = push_on_top(&f, "review/git-a", "later.txt");
    assert!(m.run_once(), "{:?}", state(&m, "review/git-a"));
    assert_eq!(state(&m, "review/git-a").0, RefState::InSync);
    let head = leep.get_ref("review/git-a").unwrap();
    let cs = changeset(&leep, head);
    assert_eq!(cs.message.trim_end(), "pushed straight to git");
    // The person's whole unlanded stack (both pushes) is replayed onto the
    // squash, as a land branch would be.
    assert!(leep.on(leep.repo.is_ancestor(squashed, head)).unwrap());
    assert!(cs
        .changes
        .contains_key(&RepoPath::new("later.txt").unwrap()));
    let tip = remote_head(&f, "refs/heads/review/git-a").unwrap();
    assert_ne!(tip, p2, "the person's commit got a new id on the branch");
    let log = git(&f.remote, &["log", "--format=%s", "review/git-a"]);
    assert!(
        log.starts_with("pushed straight to git\n") && log.ends_with("\nsquashed\none"),
        "{log}"
    );
    // The person's original commit is left dangling by the rewrite, which
    // is what a rebased pull request leaves too; nothing is broken.
    let fsck = git(&f.remote, &["fsck", "--strict"]);
    assert!(fsck.lines().all(|l| l.starts_with("dangling ")), "{fsck}");
    {
        let s = m.status.lock().unwrap();
        let r = s
            .refs
            .iter()
            .find(|r| r.leep_ref == "review/git-a")
            .unwrap();
        assert_eq!(r.imports.landed, 2, "{}", r.detail);
    }

    // A land branch still cannot target a prefix ref.
    git(
        &f.human,
        &[
            "push",
            "-q",
            "origin",
            "HEAD:refs/heads/leep-land/review/git-a/x",
        ],
    );
    m.run_once();
    let b = land_branch(&m, "leep-land/review/git-a/x").unwrap();
    assert_eq!(b.state, LandState::Refused, "{}", b.detail);
    assert_eq!(b.leep_ref, None);
    assert_eq!(leep.get_ref("review/git-a"), Some(head));
    // Main was never touched.
    assert_eq!(leep.get_ref("main"), Some(c1));
}

#[test]
fn a_two_way_prefix_conflict_is_reported_and_nothing_moves() {
    let f = fixture();
    let leep = Local::new();
    let (c1, _) = main_and_review(&leep, "review/git-a");
    let mut m = mirror::Mirror::open(two_way_config(&f), &leep).unwrap();
    assert!(m.run_once());
    let exported = remote_head(&f, "refs/heads/review/git-a").unwrap();

    // Both sides change `r`: the person on the branch, the agent in a squash.
    let p = push_on_top(&f, "review/git-a", "r");
    let squashed = leep.native(
        vec![c1],
        "squashed",
        vec![leep.file("r", "agent\n", FileKind::Regular)],
    );
    leep.set_ref("review/git-a", squashed);
    assert!(!m.run_once());
    let (st, detail) = state(&m, "review/git-a");
    assert_eq!(st, RefState::Diverged, "{detail}");
    assert!(detail.contains("path: r"), "{detail}");
    assert_eq!(leep.get_ref("review/git-a"), Some(squashed));
    assert_eq!(remote_head(&f, "refs/heads/review/git-a").unwrap(), p);
    assert_eq!(
        remote_head(&f, "refs/heads/leep-diverged/review/git-a"),
        None,
        "prefix refs get no diverged branch"
    );

    // The agent resolves it in Leep; the person's commit still conflicts
    // with the new tip, so the ref stays reported until the person resets
    // the branch to what the mirror last pushed. Then the rewrite goes out
    // with a lease.
    let fixed = leep.native(
        vec![squashed],
        "take both",
        vec![leep.file("r", "agent and git\n", FileKind::Regular)],
    );
    leep.set_ref("review/git-a", fixed);
    assert!(!m.run_once(), "the person's commit is still on the branch");
    assert_eq!(leep.get_ref("review/git-a"), Some(fixed));
    git(
        &f.human,
        &[
            "push",
            "-q",
            "-f",
            "origin",
            &format!("{exported}:refs/heads/review/git-a"),
        ],
    );
    assert!(m.run_once(), "{:?}", state(&m, "review/git-a"));
    assert_eq!(
        git(&f.remote, &["log", "-1", "--format=%s", "review/git-a"]),
        "take both"
    );
}

#[test]
fn a_two_way_branch_deleted_on_the_remote_deletes_the_ref_only_once_landed() {
    let f = fixture();
    let leep = Local::new();
    let (c1, r1) = main_and_review(&leep, "review/git-a");
    let mut m = mirror::Mirror::open(two_way_config(&f), &leep).unwrap();
    assert!(m.run_once());

    // Deleted on the remote while the work is landed nowhere: the ref is
    // kept and reported, and the branch is not pushed again.
    clone_human(&f);
    git(
        &f.human,
        &["push", "-q", "origin", ":refs/heads/review/git-a"],
    );
    assert!(!m.run_once());
    let (st, detail) = state(&m, "review/git-a");
    assert_eq!(st, RefState::DeletedOnRemote, "{detail}");
    assert_eq!(leep.get_ref("review/git-a"), Some(r1));
    assert_eq!(remote_head(&f, "refs/heads/review/git-a"), None);
    assert!(!m.run_once());
    assert_eq!(remote_head(&f, "refs/heads/review/git-a"), None);

    // The ref moves on: the branch is pushed again.
    let r2 = leep.native(
        vec![r1],
        "more",
        vec![leep.file("r", "r2\n", FileKind::Regular)],
    );
    leep.set_ref("review/git-a", r2);
    assert!(m.run_once(), "{:?}", state(&m, "review/git-a"));
    assert!(remote_head(&f, "refs/heads/review/git-a").is_some());

    // Landed on main, then the branch is deleted (a merged pull request):
    // the Leep ref goes too.
    leep.set_ref("main", r2);
    assert!(m.run_once());
    git(
        &f.human,
        &["push", "-q", "origin", ":refs/heads/review/git-a"],
    );
    assert!(m.run_once(), "{:?}", state(&m, "review/git-a"));
    assert_eq!(leep.get_ref("review/git-a"), None);
    assert!(!tracked(&m).contains(&"review/git-a".to_string()));
    {
        let s = m.status.lock().unwrap();
        assert_eq!(s.refs_deleted, 1);
        assert_eq!(s.deleted_refs[0].leep_ref, "review/git-a");
        assert_eq!(s.branches_deleted, 0);
        let metrics = mirror::status::render_metrics(&s);
        assert!(
            metrics.contains("leep_mirror_refs_deleted_total 1"),
            "{metrics}"
        );
    }

    // A rebased copy counts as landed too: the ref's own tip is not on
    // main, but main holds the server's copy of it.
    let r3 = leep.native(
        vec![c1],
        "rebased later",
        vec![leep.file("x", "x\n", FileKind::Regular)],
    );
    leep.set_ref("review/git-b", r3);
    assert!(m.run_once());
    let landed = match leep
        .on(leep.repo.land_with(r3, "main", Some("agent"), false))
        .unwrap()
    {
        LandResponse::Landed {
            landed, rebased, ..
        } => {
            assert!(rebased);
            landed
        }
        other => panic!("{other:?}"),
    };
    assert_ne!(landed, r3);
    git(
        &f.human,
        &["push", "-q", "origin", ":refs/heads/review/git-b"],
    );
    assert!(m.run_once(), "{:?}", state(&m, "review/git-b"));
    assert_eq!(leep.get_ref("review/git-b"), None);
    assert_eq!(m.status.lock().unwrap().refs_deleted, 2);
}

/// Git-primary refs (ADR 0062): the remote wins a race.
fn git_primary_config(f: &Fixture) -> Config {
    Config {
        primary: Primary::Git,
        ..config(f, true)
    }
}

fn replays(m: &mirror::Mirror) -> mirror::status::ReplayCounts {
    let s = m.status.lock().unwrap();
    s.refs
        .iter()
        .find(|r| r.leep_ref == "main")
        .unwrap()
        .replays
        .clone()
}

#[test]
fn a_git_primary_ref_replays_leeps_lands_onto_a_remote_merge() {
    let f = fixture();
    let leep = Local::new();
    let c1 = leep.native(
        vec![],
        "one",
        vec![leep.file("a", "1\n", FileKind::Regular)],
    );
    leep.set_ref("main", c1);
    let mut m = mirror::Mirror::open(git_primary_config(&f), &leep).unwrap();
    assert!(m.run_once());
    let first = remote_head(&f, "refs/heads/main").unwrap();
    assert!(m.status.lock().unwrap().refs[0].git_primary);

    // An agent lands twice in Leep while a person merges on the remote.
    let a2 = leep.native(
        vec![c1],
        "agent two",
        vec![leep.file("agent.txt", "2\n", FileKind::Regular)],
    );
    let a3 = leep.native(
        vec![a2],
        "agent three",
        vec![leep.file("agent.txt", "3\n", FileKind::Regular)],
    );
    leep.set_ref("main", a3);
    clone_human(&f);
    write(&f.human, "human.txt", "from git\n");
    commit(&f.human, "human change");
    git(&f.human, &["push", "-q", "origin", "main"]);
    let human = git(&f.human, &["rev-parse", "HEAD"]);

    // Replayed: Leep's ref is rewritten on top of the person's commit, the
    // remote only fast-forwards, and nothing is merged.
    assert!(m.run_once(), "{:?}", state(&m, "main"));
    assert_eq!(state(&m, "main").0, RefState::InSync);
    let head = leep.get_ref("main").unwrap();
    let top = changeset(&leep, head);
    assert_eq!(top.message, "agent three");
    assert_eq!(
        top.extra.get("leep.rebased_from").map(String::as_str),
        Some(a3.to_string().as_str())
    );
    let mid = changeset(&leep, top.parents[0]);
    assert_eq!(mid.message, "agent two");
    let theirs = changeset(&leep, mid.parents[0]);
    assert_eq!(theirs.extra["git.commit"], human);
    assert_eq!(theirs.parents, vec![c1]);
    assert_eq!(*leep.merges.lock().unwrap(), 0);
    let log = git(&f.remote, &["log", "--format=%s", "main"]);
    assert_eq!(log, "agent three\nagent two\nhuman change\none");
    assert_eq!(git(&f.remote, &["rev-parse", "main~2"]), human);
    assert!(is_ancestor(
        &f.remote,
        &first,
        &remote_head(&f, "refs/heads/main").unwrap()
    ));
    assert_eq!(git(&f.remote, &["fsck", "--strict"]), "");
    let reflog = leep
        .on(leep.repo.meta().ref_log(Some("main"), None, 1))
        .unwrap();
    assert_eq!(reflog[0].operation, mirror::CAS_OPERATION);
    assert_eq!(reflog[0].new, Some(head));
    assert_eq!(replays(&m).ok, 1);
    assert_eq!(*leep.replays.lock().unwrap(), 1);
    // The originals are still stored, and the person's tree is Leep's tree.
    assert!(leep.has_changeset(&a3).unwrap());
    let metrics = mirror::status::render_metrics(&m.status.lock().unwrap());
    assert!(
        metrics.contains("leep_mirror_replays_total{ref=\"main\",result=\"ok\"} 1"),
        "{metrics}"
    );

    // A Leep undo on a git-primary ref is restored from the remote.
    leep.set_ref("main", top.parents[0]);
    m.cycle();
    assert_eq!(state(&m, "main").0, RefState::Restored);
    assert!(m.run_once(), "{:?}", state(&m, "main"));
    assert_eq!(state(&m, "main").0, RefState::InSync);
    assert_eq!(leep.get_ref("main"), Some(head));
    assert_eq!(
        git(&f.remote, &["log", "-1", "--format=%s", "main"]),
        "agent three"
    );
    let s = m.status.lock().unwrap();
    assert_eq!(s.refs[0].restores, 1);
    assert!(s.refs[0].state.healthy());
}

#[test]
fn a_git_primary_conflict_is_reported_and_moves_nothing_until_merged_in_git() {
    let f = fixture();
    let leep = Local::new();
    let c1 = leep.native(
        vec![],
        "one",
        vec![leep.file("a", "1\n", FileKind::Regular)],
    );
    leep.set_ref("main", c1);
    let mut m = mirror::Mirror::open(git_primary_config(&f), &leep).unwrap();
    assert!(m.run_once());

    // Both sides change `a`.
    let agent = leep.native(
        vec![c1],
        "agent",
        vec![leep.file("a", "agent\n", FileKind::Regular)],
    );
    leep.set_ref("main", agent);
    clone_human(&f);
    write(&f.human, "a", "human\n");
    commit(&f.human, "human");
    git(&f.human, &["push", "-q", "origin", "main"]);
    let human = git(&f.human, &["rev-parse", "HEAD"]);
    assert!(!m.run_once());
    let (st, detail) = state(&m, "main");
    assert_eq!(st, RefState::Diverged, "{detail}");
    assert!(
        detail.contains("not replayed") && detail.contains("path: a"),
        "{detail}"
    );
    assert_eq!(leep.get_ref("main"), Some(agent));
    assert_eq!(remote_head(&f, "refs/heads/main").unwrap(), human);
    assert!(remote_head(&f, "refs/heads/leep-diverged/main").is_some());
    assert_eq!(replays(&m).conflicts, 1);
    // The same pair is not asked about again.
    assert!(!m.run_once());
    assert_eq!(*leep.replays.lock().unwrap(), 1);

    // A person merges Leep's side in git and pushes: a fast-forward import.
    // The merge conflicts on `a` (exit 1) and is committed with both sides;
    // the same identity and empty configuration as every other git call,
    // so a runner without a global identity behaves like a developer's
    // machine.
    git(&f.human, &["fetch", "-q", "origin"]);
    let _ = git_output(
        &f.human,
        &[
            "merge",
            "--no-commit",
            "--no-ff",
            "origin/leep-diverged/main",
        ],
    );
    assert!(
        f.human.join(".git/MERGE_HEAD").exists(),
        "the merge of the diverged branch did not start"
    );
    write(&f.human, "a", "both\n");
    commit(&f.human, "merge, keeping both");
    assert_eq!(
        git(&f.human, &["rev-list", "--parents", "-n", "1", "HEAD"])
            .split_whitespace()
            .count(),
        3,
        "a merge commit with two parents"
    );
    git(&f.human, &["push", "-q", "origin", "main"]);
    let merge = git(&f.human, &["rev-parse", "HEAD"]);
    assert!(m.run_once(), "{:?}", state(&m, "main"));
    let head = leep.get_ref("main").unwrap();
    assert_eq!(changeset(&leep, head).extra["git.commit"], merge);
    assert!(leep.on(leep.repo.is_ancestor(agent, head)).unwrap());
    assert_eq!(remote_head(&f, "refs/heads/leep-diverged/main"), None);
    assert_eq!(git(&f.remote, &["fsck", "--strict"]), "");
}

#[test]
fn a_leep_primary_ref_keeps_adr_0046s_rules_beside_a_git_primary_one() {
    let f = fixture();
    let leep = Local::new();
    let c1 = leep.native(
        vec![],
        "one",
        vec![leep.file("a", "1\n", FileKind::Regular)],
    );
    leep.set_ref("main", c1);
    leep.set_ref("release", c1);
    let cfg = Config {
        refs: vec![
            RefSpec::parse("main@git").unwrap(),
            RefSpec::parse("release").unwrap(),
        ],
        ..config(&f, true)
    };
    let mut m = mirror::Mirror::open(cfg, &leep).unwrap();
    assert!(m.run_once());
    {
        let s = m.status.lock().unwrap();
        let by = |n: &str| s.refs.iter().find(|r| r.leep_ref == n).unwrap().git_primary;
        assert!(by("main") && !by("release"));
    }
    // An undo on each: main is restored, release is rewound.
    let a2 = leep.native(
        vec![c1],
        "two",
        vec![leep.file("b", "2\n", FileKind::Regular)],
    );
    leep.set_ref("main", a2);
    leep.set_ref("release", a2);
    assert!(m.run_once());
    leep.set_ref("main", c1);
    leep.set_ref("release", c1);
    assert!(!m.run_once());
    assert_eq!(state(&m, "main").0, RefState::InSync);
    assert_eq!(leep.get_ref("main"), Some(a2));
    assert_eq!(state(&m, "release").0, RefState::Rewound);
    assert_eq!(leep.get_ref("release"), Some(c1));
}

#[test]
fn prefix_mirroring_leaves_main_and_tags_alone() {
    let f = fixture();
    let leep = Local::new();
    let (c1, r1) = main_and_review(&leep, "review/git-a");
    leep.set_ref("tags/v1", c1);
    let mut m = mirror::Mirror::open(prefix_config(&f, false), &leep).unwrap();
    assert!(m.run_once());
    let main = remote_head(&f, "refs/heads/main").unwrap();
    let tag = remote_head(&f, "refs/tags/v1").unwrap();

    // Branches outside the prefix, including look-alikes, that Leep lacks.
    clone_human(&f);
    for b in ["reviewer", "other", "review-x"] {
        git(
            &f.human,
            &["push", "-q", "origin", &format!("HEAD:refs/heads/{b}")],
        );
    }
    git(
        &f.human,
        &["push", "-q", "origin", "HEAD:refs/tags/review/t"],
    );

    // Review refs come and go, and a Leep tag goes too.
    leep.set_ref("review/git-b", r1);
    assert!(m.run_once());
    leep.delete_ref("review/git-a");
    leep.delete_ref("review/git-b");
    leep.delete_ref("tags/v1");
    assert!(m.run_once());
    assert_eq!(remote_head(&f, "refs/heads/review/git-a"), None);
    assert_eq!(remote_head(&f, "refs/heads/review/git-b"), None);

    // Deleting main in Leep keeps the old rule for a --ref: reported, and
    // the remote's main is left alone.
    leep.delete_ref("main");
    assert!(!m.run_once());
    assert_eq!(state(&m, "main").0, RefState::DeletedInLeep);
    assert_eq!(remote_head(&f, "refs/heads/main").unwrap(), main);
    assert_eq!(remote_head(&f, "refs/tags/v1").unwrap(), tag);
    for b in ["reviewer", "other", "review-x"] {
        assert_eq!(
            remote_head(&f, &format!("refs/heads/{b}")).unwrap(),
            main,
            "{b}"
        );
    }
    assert_eq!(remote_head(&f, "refs/tags/review/t").unwrap(), main);
    assert_eq!(m.status.lock().unwrap().branches_deleted, 2);
}

#[test]
fn a_prefix_ref_recreated_after_a_stale_ref_list_is_not_deleted() {
    let f = fixture();
    let leep = Local::new();
    let (_, r1) = main_and_review(&leep, "review/git-a");
    let mut m = mirror::Mirror::open(prefix_config(&f, false), &leep).unwrap();
    assert!(m.run_once());
    let a1 = remote_head(&f, "refs/heads/review/git-a").unwrap();

    // The ref list the cycle reads is from the moment the ref was briefly
    // gone; by the time the mirror would delete the branch it is back.
    let mut gone = leep.refs().unwrap();
    gone.remove("review/git-a");
    leep.delete_ref("review/git-a");
    let r2 = leep.native(
        vec![r1],
        "recreated",
        vec![leep.file("r", "r2\n", FileKind::Regular)],
    );
    leep.set_ref("review/git-a", r2);
    leep.stale_refs.lock().unwrap().push(gone);
    m.cycle();
    assert!(
        leep.stale_refs.lock().unwrap().is_empty(),
        "the stale list was read"
    );
    assert_eq!(
        remote_head(&f, "refs/heads/review/git-a").as_deref(),
        Some(a1.as_str()),
        "the fresh read saves the branch"
    );
    assert_eq!(m.status.lock().unwrap().branches_deleted, 0);
    assert!(m.run_once());
    let a2 = remote_head(&f, "refs/heads/review/git-a").unwrap();
    assert!(is_ancestor(&f.remote, &a1, &a2));
    assert_eq!(state(&m, "review/git-a").0, RefState::InSync);
}

#[test]
fn a_prefix_branch_is_never_deleted_while_the_server_is_down() {
    let f = fixture();
    let leep = Local::new();
    main_and_review(&leep, "review/git-a");
    let mut m = mirror::Mirror::open(prefix_config(&f, false), &leep).unwrap();
    assert!(m.run_once());
    let a = remote_head(&f, "refs/heads/review/git-a").unwrap();

    // A new mirror starts while the server is unreachable: it finds the
    // branch on the remote but cannot ask Leep about it.
    drop(m);
    *leep.down.lock().unwrap() = true;
    let mut m = mirror::Mirror::open(prefix_config(&f, false), &leep).unwrap();
    assert!(!m.run_once());
    assert_eq!(state(&m, "review/git-a").0, RefState::Error);
    assert_eq!(
        remote_head(&f, "refs/heads/review/git-a").as_deref(),
        Some(a.as_str())
    );

    // Also when the ref really was deleted meanwhile.
    leep.delete_ref("review/git-a");
    std::thread::sleep(Duration::from_millis(1100));
    assert!(!m.run_once());
    assert_eq!(
        remote_head(&f, "refs/heads/review/git-a").as_deref(),
        Some(a.as_str())
    );
    assert_eq!(m.status.lock().unwrap().branches_deleted, 0);

    // Back up: now the delete goes through.
    *leep.down.lock().unwrap() = false;
    std::thread::sleep(Duration::from_millis(1100));
    assert!(m.run_once(), "{:?}", state(&m, "review/git-a"));
    assert_eq!(remote_head(&f, "refs/heads/review/git-a"), None);
    assert_eq!(m.status.lock().unwrap().branches_deleted, 1);
}

#[test]
fn a_pushed_commit_is_kept_when_leep_returns_to_a_value_it_held() {
    let f = fixture();
    let leep = Local::new();
    let (c1, x) = main_and_review(&leep, "review/git-a");
    let mut m = mirror::Mirror::open(prefix_config(&f, false), &leep).unwrap();
    assert!(m.run_once());
    let gx = remote_head(&f, "refs/heads/review/git-a").unwrap();

    // Leep moves on to Z while a person pushes Y on top of X; then Leep's
    // ref is reset back to X. The remote's Y is not something Leep held.
    let z = leep.native(vec![x], "z", vec![leep.file("z", "z\n", FileKind::Regular)]);
    leep.set_ref("review/git-a", z);
    let y = push_on_top(&f, "review/git-a", "y.txt");
    leep.set_ref("review/git-a", x);
    assert!(!m.run_once());
    let (st, detail) = state(&m, "review/git-a");
    assert_eq!(st, RefState::RemoteAhead, "{detail}");
    assert_eq!(remote_head(&f, "refs/heads/review/git-a").unwrap(), y);
    assert_eq!(leep.get_ref("review/git-a"), Some(x));

    // Deleting the Leep ref does not delete Y either.
    leep.delete_ref("review/git-a");
    assert!(!m.run_once());
    assert_eq!(state(&m, "review/git-a").0, RefState::RemoteAhead);
    assert_eq!(remote_head(&f, "refs/heads/review/git-a").unwrap(), y);
    assert_eq!(m.status.lock().unwrap().branches_deleted, 0);

    // The other way round: the person resets the branch back to X, which
    // the ref held, so a rewrite in Leep goes out with a lease on X.
    leep.set_ref("review/git-a", x);
    git(
        &f.human,
        &[
            "push",
            "-q",
            "-f",
            "origin",
            &format!("{gx}:refs/heads/review/git-a"),
        ],
    );
    let squashed = leep.native(
        vec![c1],
        "squashed",
        vec![leep.file("r", "squashed\n", FileKind::Regular)],
    );
    leep.set_ref("review/git-a", squashed);
    assert!(m.run_once(), "{:?}", state(&m, "review/git-a"));
    assert_eq!(
        git(&f.remote, &["log", "-1", "--format=%s", "review/git-a"]),
        "squashed"
    );
}

#[test]
fn a_prefix_branch_whose_name_leep_rejects_is_reported() {
    let f = fixture();
    let leep = Local::new();
    main_and_review(&leep, "review/git-a");
    let mut m = mirror::Mirror::open(prefix_config(&f, false), &leep).unwrap();
    assert!(m.run_once());
    // A commit Leep exported, so only the name stops the ref log lookup.
    let main = remote_head(&f, "refs/heads/main").unwrap();
    git(&f.remote, &["update-ref", "refs/heads/review/-x", &main]);
    assert!(!m.run_once());
    let (st, detail) = state(&m, "review/-x");
    assert_eq!(st, RefState::RemoteAhead, "{detail}");
    assert!(detail.contains("no Leep ref can have its name"), "{detail}");
    assert_eq!(m.status.lock().unwrap().errors.get("leep"), None);
    assert_eq!(remote_head(&f, "refs/heads/review/-x").unwrap(), main);
    git(&f.remote, &["update-ref", "-d", "refs/heads/review/-x"]);
    assert!(m.run_once());
    assert!(!tracked(&m).contains(&"review/-x".to_string()));
}

/// GitHub pull request reviews into the review gate (ADR 0063).
fn github_config(f: &Fixture) -> Config {
    Config {
        github: Some(GitHubConfig {
            owner: "acme".into(),
            repo: "app".into(),
            users: BTreeMap::from([
                ("ada".to_string(), "alice".to_string()),
                ("grace".to_string(), "bob".to_string()),
            ]),
            token: None,
            api_url: String::new(),
        }),
        ..two_way_config(f)
    }
}

fn github_counts(m: &mirror::Mirror, leep_ref: &str) -> mirror::status::GitHubReviewCounts {
    let s = m.status.lock().unwrap();
    s.refs
        .iter()
        .find(|r| r.leep_ref == leep_ref)
        .unwrap()
        .github_reviews
        .clone()
}

#[test]
fn github_approvals_requests_and_dismissals_become_leep_reviews() {
    let f = fixture();
    let leep = Local::new();
    let (_, r1) = main_and_review(&leep, "review/git-a");
    let gh = Arc::new(FakeGitHub::default());
    let mut m = mirror::Mirror::open(github_config(&f), &leep).unwrap();
    m.set_github(Box::new(SharedGitHub(gh.clone())));
    assert!(m.run_once());
    let sha = remote_head(&f, "refs/heads/review/git-a").unwrap();
    // No pull request yet: nothing to do.
    assert_eq!(leep.posted.lock().unwrap().len(), 0);

    gh.open("review/git-a", 7, &sha, "main");
    gh.review(7, 1, "ada", "APPROVED", &sha);
    assert!(m.run_once());
    let r = leep.review(r1, "alice").unwrap();
    assert_eq!(r.verdict, Verdict::Approve);
    let src = leep
        .on(leep.repo.get_review_source(r1, "alice"))
        .unwrap()
        .unwrap();
    assert_eq!(src.source, "github:acme/app#7/reviews/1");
    {
        let s = m.status.lock().unwrap();
        let st = s
            .refs
            .iter()
            .find(|r| r.leep_ref == "review/git-a")
            .unwrap();
        assert_eq!(st.pull_request, Some(7));
        assert_eq!(st.github_reviews.approved, 1);
        let metrics = mirror::status::render_metrics(&s);
        assert!(
            metrics.contains(
                "leep_mirror_github_reviews_total{ref=\"review/git-a\",result=\"approved\"} 1"
            ),
            "{metrics}"
        );
    }
    // The same answer again posts nothing.
    assert!(m.run_once());
    assert_eq!(leep.posted.lock().unwrap().len(), 1);

    // Changes requested by a mapped login, a review by an unmapped one.
    gh.review(7, 2, "grace", "CHANGES_REQUESTED", &sha);
    gh.review(7, 3, "linus", "APPROVED", &sha);
    assert!(m.run_once());
    assert_eq!(leep.review(r1, "bob").unwrap().verdict, Verdict::Reject);
    assert_eq!(leep.posted.lock().unwrap().len(), 2);
    // Counted on every poll (`run_once` polls until nothing changes).
    assert!(github_counts(&m, "review/git-a").unmapped >= 1);

    // A dismissal withdraws; an approval of an older commit is stale and
    // withdraws too.
    gh.review(7, 4, "ada", "DISMISSED", &sha);
    gh.review(
        7,
        5,
        "grace",
        "APPROVED",
        "0000000000000000000000000000000000000000",
    );
    assert!(m.run_once());
    assert_eq!(
        leep.review(r1, "alice").unwrap().verdict,
        Verdict::Withdrawn
    );
    assert_eq!(leep.review(r1, "bob").unwrap().verdict, Verdict::Withdrawn);
    assert_eq!(github_counts(&m, "review/git-a").withdrawn, 2);

    // The agent pushes more; a fresh approval covers the whole stack.
    let r2 = leep.native(
        vec![r1],
        "more",
        vec![leep.file("r", "r2\n", FileKind::Regular)],
    );
    leep.set_ref("review/git-a", r2);
    assert!(m.run_once());
    let sha2 = remote_head(&f, "refs/heads/review/git-a").unwrap();
    assert_ne!(sha2, sha);
    gh.open("review/git-a", 7, &sha2, "main");
    gh.review(7, 6, "grace", "APPROVED", &sha2);
    assert!(m.run_once());
    assert_eq!(leep.review(r2, "bob").unwrap().verdict, Verdict::Approve);
    assert_eq!(leep.review(r1, "bob").unwrap().verdict, Verdict::Approve);

    // A review a person posted in Leep themselves is never touched.
    leep.on(leep.repo.put_review(
        r2,
        &Review {
            reviewer: "alice".into(),
            verdict: Verdict::Reject,
            comment: "not like this".into(),
            timestamp: 5,
        },
    ))
    .unwrap();
    gh.review(7, 8, "ada", "APPROVED", &sha2);
    assert!(m.run_once());
    assert_eq!(leep.review(r2, "alice").unwrap().verdict, Verdict::Reject);
    assert_eq!(leep.review(r1, "alice").unwrap().verdict, Verdict::Approve);
    // Main was never reviewed: it is not in the stack.
    assert_eq!(leep.review(leep.get_ref("main").unwrap(), "bob"), None);
}

#[test]
fn github_reviews_converge_after_a_failure_between_reading_and_posting() {
    let f = fixture();
    let leep = Local::new();
    let (_, r1) = main_and_review(&leep, "review/git-a");
    let r2 = leep.native(
        vec![r1],
        "two",
        vec![leep.file("r", "r2\n", FileKind::Regular)],
    );
    leep.set_ref("review/git-a", r2);
    let gh = Arc::new(FakeGitHub::default());
    let mut m = mirror::Mirror::open(github_config(&f), &leep).unwrap();
    m.set_github(Box::new(SharedGitHub(gh.clone())));
    assert!(m.run_once());
    let sha = remote_head(&f, "refs/heads/review/git-a").unwrap();
    gh.open("review/git-a", 9, &sha, "main");
    gh.review(9, 1, "ada", "APPROVED", &sha);

    // GitHub is down: counted, nothing posted, the ref stays healthy.
    *gh.down.lock().unwrap() = true;
    assert!(m.run_once());
    assert_eq!(leep.posted.lock().unwrap().len(), 0);
    assert!(m
        .status
        .lock()
        .unwrap()
        .errors
        .get("github")
        .is_some_and(|n| *n >= 1));

    // The first post of the stack lands and the second fails (the mirror
    // dies, the server hiccups): the next poll posts only what is missing.
    *gh.down.lock().unwrap() = false;
    *leep.fail_posts_after.lock().unwrap() = Some(1);
    // `run_once` polls the remote (and so GitHub) on every cycle; a bare
    // cycle waits for --git-poll.
    m.run_once();
    assert_eq!(leep.posted.lock().unwrap().len(), 1);
    *leep.fail_posts_after.lock().unwrap() = None;
    drop(m);
    let mut m = mirror::Mirror::open(github_config(&f), &leep).unwrap();
    m.set_github(Box::new(SharedGitHub(gh.clone())));
    assert!(m.run_once());
    assert_eq!(leep.posted.lock().unwrap().len(), 2);
    assert_eq!(leep.review(r1, "alice").unwrap().verdict, Verdict::Approve);
    assert_eq!(leep.review(r2, "alice").unwrap().verdict, Verdict::Approve);
    assert!(m.run_once());
    assert_eq!(leep.posted.lock().unwrap().len(), 2, "nothing posted twice");
}

#[test]
fn a_mapped_owners_github_approval_lets_a_require_review_land_through() {
    use leep_server::review::{RequireReview, ReviewPolicy, ReviewService};
    let f = fixture();
    let policy = ReviewPolicy::parse(r#"{"refs": ["main"]}"#).unwrap();
    let service = Arc::new(ReviewService::new(
        vec!["alice".into(), "bob".into()],
        policy.rules,
    ));
    let leep = Local::with_repo(
        Repo::new(
            Arc::new(MemoryBlobStore::new()),
            Arc::new(MemoryMetaStore::new()),
        )
        .with_land_policy(Arc::new(RequireReview::new(service.clone(), policy.refs))),
    );
    let (_, r1) = main_and_review(&leep, "review/git-a");
    let gh = Arc::new(FakeGitHub::default());
    let mut m = mirror::Mirror::open(github_config(&f), &leep).unwrap();
    m.set_github(Box::new(SharedGitHub(gh.clone())));
    assert!(m.run_once());

    // Unreviewed: the land is refused.
    match leep.land(r1, "main", None).unwrap() {
        LandOutcome::Refused(why) => assert!(why.contains("review"), "{why}"),
        other => panic!("{other:?}"),
    }
    assert!(
        !leep
            .on(service.status(&leep.repo, r1, "main"))
            .unwrap()
            .approved
    );

    // The owner approves on GitHub; the mirror posts it; the land goes.
    let sha = remote_head(&f, "refs/heads/review/git-a").unwrap();
    gh.open("review/git-a", 3, &sha, "main");
    gh.review(3, 1, "ada", "APPROVED", &sha);
    assert!(m.run_once());
    assert!(
        leep.on(service.status(&leep.repo, r1, "main"))
            .unwrap()
            .approved
    );
    assert!(matches!(
        leep.land(r1, "main", None).unwrap(),
        LandOutcome::Landed(_)
    ));
    assert_eq!(leep.get_ref("main"), Some(r1));
}
