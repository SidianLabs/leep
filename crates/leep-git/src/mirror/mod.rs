//! Live mirror between a Leep server and a git remote (ADR 0046).
//!
//! A daemon that talks to the server over its HTTP API and to the remote
//! through the system `git`. Every cycle it compares, per mirrored ref, the
//! export of the Leep ref (`E`) with the remote ref (`G`) and acts on how
//! they relate:
//!
//! | relation | action |
//! |---|---|
//! | `G == E` | in sync |
//! | `G` missing, or an ancestor of `E` | push `E` (the remote accepts only a fast-forward) |
//! | `E` an ancestor of `G`, tip from git | import the new commits and move the Leep ref by compare-and-swap, through the land policy |
//! | `E` an ancestor of `G`, tip from Leep | Leep moved back: report, never force (unless allowed for that ref) |
//! | neither | diverged: report, push Leep's side to a separate branch, move nothing; or, with `--on-divergence merge`, import the remote's side, merge on the server and land the merge if it is clean |
//!
//! Land branches (`leep-land/<ref>/<name>` on the remote) are imported and
//! landed onto `<ref>` through `POST /v1/land`, which rebases them like an
//! agent's land, and then deleted with an exact lease (ADR 0049).
//!
//! The decision depends only on the two current values and the map, never
//! on remembered progress, so a crash at any point is recovered by running
//! the cycle again: exports are deterministic, the mirror's merges are
//! deterministic, lands are idempotent and a push of a value the remote
//! already has is a no-op.

pub mod export;
mod git;
pub mod import;
mod land;
pub mod map;
pub mod status;

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use gix::ObjectId;
use leep_cli::client::Client;
use leep_cli::error::ErrorKind;
use leep_core::api::{
    Authorship, CasRefRequest, LandRequest, LandResponse, MergeRequest, MergeResponse,
};
use leep_core::{Changeset, ChangesetId, Hash};

pub use export::CommitStyle;
pub use git::redact;
use git::{PushOutcome, Remote};
use map::{Map, Origin};
pub use status::{read_status, LandBranchStatus, LandState, RefState, RefStatus, Status};

/// Ref log label of the mirror's moves of Leep refs.
pub const CAS_OPERATION: &str = "git-mirror";

/// Test hook: the mirror aborts, as a SIGKILL would stop it, when it reaches
/// the step this names (`export-commit`, `exported`, `pushed`, `imported`,
/// `landed`, `diverged-pushed`, `merge-imported`, `merged`, `merge-landed`,
/// `land-imported`, `land-landed`, `land-deleted`). Lets the crash tests stop
/// it exactly between two steps rather than at a random moment.
pub const CRASH_ENV: &str = "LEEP_MIRROR_CRASH_AT";

pub(crate) fn crash_point(name: &str) {
    if std::env::var_os(CRASH_ENV).is_some_and(|v| v == name) {
        eprintln!("leep-git mirror: {CRASH_ENV}={name}: aborting");
        std::process::abort();
    }
}

/// What the mirror needs from a Leep server. Implemented over HTTP by
/// [`Client`]; tests may implement it over anything.
pub trait Leep {
    fn changeset(&self, id: &ChangesetId) -> Result<Changeset>;
    fn has_changeset(&self, id: &ChangesetId) -> Result<bool>;
    fn blob(&self, hash: &Hash) -> Result<Vec<u8>>;
    fn missing_blobs(&self, hashes: Vec<Hash>) -> Result<Vec<Hash>>;
    fn put_blob(&self, hash: &Hash, data: Vec<u8>) -> Result<()>;
    fn create_changeset(&self, cs: &Changeset) -> Result<ChangesetId>;
    fn refs(&self) -> Result<BTreeMap<String, ChangesetId>>;
    /// The newest ref log `seq`, or None for an empty log.
    fn ref_log_head(&self) -> Result<Option<u64>>;
    /// Has ref `name` ever pointed at `id`, by its ref log?
    fn ref_had(&self, name: &str, id: &ChangesetId) -> Result<bool>;
    fn cas_ref(
        &self,
        name: &str,
        expected: Option<ChangesetId>,
        new: ChangesetId,
        message: &str,
    ) -> Result<Cas>;
    /// `POST /v1/merge`: a three-way merge stored as a new changeset, or the
    /// paths that conflict. Never resolves a conflict.
    fn merge(&self, req: &MergeRequest) -> Result<MergeOutcome>;
    /// `POST /v1/land` onto an existing ref (`create: false`): the server
    /// rebases the unlanded stack onto the head, through the land policy.
    fn land(&self, changeset: ChangesetId, target_ref: &str) -> Result<LandOutcome>;
}

/// How a server-side merge ended.
#[derive(Debug, PartialEq, Eq)]
pub enum MergeOutcome {
    Clean(ChangesetId),
    /// The conflicting paths.
    Conflicts(Vec<String>),
}

/// How a land ended.
#[derive(Debug, PartialEq, Eq)]
pub enum LandOutcome {
    /// The changeset now at the ref head for this land (a rebased copy if the
    /// ref had moved).
    Landed(ChangesetId),
    /// The conflicting paths against the head.
    Conflicts(Vec<String>),
    /// The server refused (400 or 404): land policy, a merge in the stack, or
    /// a missing ref.
    Refused(String),
}

/// How a compare-and-swap of a Leep ref ended.
#[derive(Debug, PartialEq, Eq)]
pub enum Cas {
    Won,
    /// Someone else moved the ref first; its value now.
    Lost(Option<ChangesetId>),
    /// The server refused the move (400), for example by its land policy.
    Refused(String),
}

impl Leep for Client {
    fn changeset(&self, id: &ChangesetId) -> Result<Changeset> {
        Ok(self.get_changeset(id)?.changeset)
    }

    fn has_changeset(&self, id: &ChangesetId) -> Result<bool> {
        match self.get_changeset(id) {
            Ok(_) => Ok(true),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    fn blob(&self, hash: &Hash) -> Result<Vec<u8>> {
        Ok(self.get_blob(hash)?)
    }

    fn missing_blobs(&self, hashes: Vec<Hash>) -> Result<Vec<Hash>> {
        Ok(Client::missing_blobs(self, hashes)?)
    }

    fn put_blob(&self, hash: &Hash, data: Vec<u8>) -> Result<()> {
        Ok(Client::put_blob(self, hash, data)?)
    }

    fn create_changeset(&self, cs: &Changeset) -> Result<ChangesetId> {
        Ok(Client::create_changeset(self, cs)?)
    }

    fn refs(&self) -> Result<BTreeMap<String, ChangesetId>> {
        Ok(Client::refs(self)?.refs)
    }

    fn ref_log_head(&self) -> Result<Option<u64>> {
        Ok(self.ref_log(None, None, 1)?.entries.first().map(|e| e.seq))
    }

    fn ref_had(&self, name: &str, id: &ChangesetId) -> Result<bool> {
        let mut before = None;
        loop {
            let page = self.ref_log(Some(name), before, 1000)?.entries;
            if page.iter().any(|e| e.new == Some(*id)) {
                return Ok(true);
            }
            match page.last() {
                Some(e) => before = Some(e.seq),
                None => return Ok(false),
            }
        }
    }

    fn cas_ref(
        &self,
        name: &str,
        expected: Option<ChangesetId>,
        new: ChangesetId,
        message: &str,
    ) -> Result<Cas> {
        let req = CasRefRequest {
            expected,
            new: Some(new),
            operation: Some(CAS_OPERATION.into()),
            message: Some(message.chars().take(256).collect()),
        };
        match Client::cas_ref(self, name, &req) {
            Ok(r) if r.ok => Ok(Cas::Won),
            Ok(r) => Ok(Cas::Lost(r.current)),
            Err(e) if e.kind() == ErrorKind::User => Ok(Cas::Refused(e.message().to_string())),
            Err(e) => Err(e.into()),
        }
    }

    fn merge(&self, req: &MergeRequest) -> Result<MergeOutcome> {
        Ok(match Client::merge(self, req)? {
            MergeResponse::Merged { id } => MergeOutcome::Clean(id),
            MergeResponse::Conflicts { conflicts, .. } => MergeOutcome::Conflicts(
                conflicts
                    .into_iter()
                    .map(|c| c.path.as_str().to_string())
                    .collect(),
            ),
        })
    }

    fn land(&self, changeset: ChangesetId, target_ref: &str) -> Result<LandOutcome> {
        let req = LandRequest {
            changeset,
            target_ref: target_ref.to_string(),
            create: Some(false),
            land_id: None,
        };
        match Client::land(self, &req) {
            Ok(LandResponse::Landed { landed, .. }) => Ok(LandOutcome::Landed(landed)),
            Ok(LandResponse::Conflicts { conflicts, .. }) => Ok(LandOutcome::Conflicts(
                conflicts
                    .into_iter()
                    .map(|c| c.path.as_str().to_string())
                    .collect(),
            )),
            Err(e) if matches!(e.kind(), ErrorKind::User | ErrorKind::NotFound) => {
                Ok(LandOutcome::Refused(e.message().to_string()))
            }
            Err(e) => Err(e.into()),
        }
    }
}

/// What the mirror does when both sides of a ref moved.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum OnDivergence {
    /// Report it and push Leep's side to the diverged branch for a person
    /// to merge in git.
    #[default]
    Report,
    /// Ask the server for a merge and land it when it is clean and the land
    /// policy allows it; report as above otherwise.
    Merge,
}

impl std::str::FromStr for OnDivergence {
    type Err = String;
    fn from_str(s: &str) -> std::result::Result<Self, String> {
        match s {
            "report" => Ok(OnDivergence::Report),
            "merge" => Ok(OnDivergence::Merge),
            _ => Err(format!("{s:?}: expected report or merge")),
        }
    }
}

/// Author of the merges the mirror asks the server for.
pub const DEFAULT_MERGE_AUTHOR: &str = "Leep git mirror <git-mirror@leep.invalid>";

/// One Leep ref and the remote ref it is mirrored to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RefSpec {
    pub leep: String,
    pub git: String,
}

impl RefSpec {
    /// `NAME` or `NAME:GIT_REF`. Without a git ref, `tags/T` maps to
    /// `refs/tags/T` and anything else to `refs/heads/NAME`.
    pub fn parse(s: &str) -> Result<RefSpec> {
        let (leep, git) = match s.split_once(':') {
            Some((l, g)) => (l.to_string(), g.to_string()),
            None => (s.to_string(), default_git_ref(s)),
        };
        if leep.is_empty() {
            bail!("empty Leep ref name in {s:?}");
        }
        leep_core::refname::validate(&leep).map_err(|e| anyhow!("{s:?}: {e}"))?;
        if !git.starts_with("refs/heads/") && !git.starts_with("refs/tags/") {
            bail!("{s:?}: the git ref must start with refs/heads/ or refs/tags/");
        }
        if git.starts_with("refs/tags/") != leep.starts_with("tags/") {
            bail!("{s:?}: a Leep tag must map to a git tag and a branch to a branch");
        }
        Ok(RefSpec { leep, git })
    }

    fn is_tag(&self) -> bool {
        self.git.starts_with("refs/tags/")
    }
}

fn default_git_ref(leep: &str) -> String {
    match leep.strip_prefix("tags/") {
        Some(t) => format!("refs/tags/{t}"),
        None => format!("refs/heads/{leep}"),
    }
}

#[derive(Clone, Debug)]
pub struct Config {
    pub server: String,
    pub token: Option<String>,
    /// Git remote URL (https, ssh, file or a path). May carry credentials;
    /// it is redacted everywhere it is shown.
    pub remote: String,
    pub work_dir: PathBuf,
    pub refs: Vec<RefSpec>,
    /// Also mirror every Leep tag (`tags/*`) to `refs/tags/*`.
    pub tags: bool,
    /// Import commits pushed to the remote into Leep (fast-forward only).
    pub import: bool,
    /// Leep refs whose backward moves may be mirrored with a lease.
    pub allow_rewind: Vec<String>,
    /// Prefix of the mirror-owned branch Leep's side goes to on divergence.
    pub diverged_prefix: String,
    pub on_divergence: OnDivergence,
    /// Author of the mirror's merges. Part of the merge's id, so keep it
    /// fixed for the life of a mirror.
    pub merge_author: String,
    /// Land branches pushed under this prefix (`<prefix><ref>/<name>`); None
    /// turns them off.
    pub land_prefix: Option<String>,
    pub style: CommitStyle,
    /// How often to ask the server whether any ref moved.
    pub interval: Duration,
    /// How often to ask the remote for its refs.
    pub git_poll: Duration,
    /// How often to re-read every ref even when the ref log is quiet.
    pub reconcile: Duration,
    pub git_timeout: Duration,
    pub max_backoff: Duration,
    pub listen: Option<std::net::SocketAddr>,
}

impl Config {
    pub fn validate(&self) -> Result<()> {
        for name in &self.allow_rewind {
            if name == "main" || name.starts_with("tags/") {
                bail!("--allow-rewind {name}: main and tags are never rewound on the remote");
            }
        }
        if self.refs.is_empty() && !self.tags {
            bail!("nothing to mirror: pass --ref (repeatable) or --tags");
        }
        let mut owned = vec![("--diverged-prefix", &self.diverged_prefix)];
        if let Some(p) = &self.land_prefix {
            owned.push(("--land-prefix", p));
        }
        for (flag, prefix) in &owned {
            if !prefix.ends_with('/') || prefix.starts_with('/') {
                bail!("{flag} must be a relative branch prefix ending in '/'");
            }
            for r in &self.refs {
                if r.git
                    .strip_prefix("refs/heads/")
                    .is_some_and(|b| b.starts_with(prefix.as_str()))
                {
                    bail!("{} maps into {prefix}, which the mirror owns", r.leep);
                }
            }
        }
        if let Some(p) = &self.land_prefix {
            let d = &self.diverged_prefix;
            if p.starts_with(d.as_str()) || d.starts_with(p.as_str()) {
                bail!("--land-prefix {p} and --diverged-prefix {d} overlap");
            }
            if !self.import {
                bail!("land branches (--land-branches) need --import");
            }
        }
        if self.on_divergence == OnDivergence::Merge && !self.import {
            bail!("--on-divergence merge needs --import");
        }
        if self.merge_author.trim().is_empty() {
            bail!("--merge-author must not be empty");
        }
        Ok(())
    }
}

struct Tracked {
    spec: RefSpec,
    leep: Option<ChangesetId>,
    remote: Option<ObjectId>,
    /// The remote value of the diverged branch, as last seen.
    side: Option<ObjectId>,
    /// Last export written to the local `refs/leep-mirror/leep/` ref.
    local: Option<ObjectId>,
    st: RefStatus,
    next_try: Instant,
    /// Set from configuration, or discovered (`--tags`).
    configured: bool,
    /// The last automatic merge that fell back to a report, so the same
    /// pair is not merged again every cycle.
    merge_memo: Option<MergeMemo>,
}

struct MergeMemo {
    leep: ChangesetId,
    remote: ObjectId,
    until: Option<Instant>,
    reason: String,
}

impl Tracked {
    fn new(spec: RefSpec, configured: bool) -> Tracked {
        Tracked {
            st: RefStatus {
                leep_ref: spec.leep.clone(),
                git_ref: spec.git.clone(),
                ..Default::default()
            },
            spec,
            leep: None,
            remote: None,
            side: None,
            local: None,
            next_try: Instant::now(),
            configured,
            merge_memo: None,
        }
    }

    fn side_ref(&self, prefix: &str) -> Option<String> {
        self.spec
            .git
            .strip_prefix("refs/heads/")
            .map(|b| format!("refs/heads/{prefix}{b}"))
    }
}

pub struct Mirror<'a> {
    cfg: Config,
    leep: &'a dyn Leep,
    remote: Remote,
    ex: export::Exporter,
    refs: Vec<Tracked>,
    seq: Option<Option<u64>>,
    next_reconcile: Instant,
    next_git_poll: Instant,
    remote_stale: bool,
    pub status: Arc<Mutex<Status>>,
    status_path: PathBuf,
    pushes_since_gc: u32,
    /// Land branches on the remote, by git ref.
    lands: BTreeMap<String, land::LandBranch>,
    _lock: std::fs::File,
}

fn log(msg: impl AsRef<str>) {
    eprintln!(
        "{} leep-git mirror: {}",
        status::now(),
        redact(msg.as_ref())
    );
}

fn hex(o: Option<impl std::fmt::Display>) -> Option<String> {
    o.map(|v| v.to_string())
}

impl<'a> Mirror<'a> {
    /// Open the work directory (creating it and its bare repository), take
    /// its lock and load the map.
    pub fn open(cfg: Config, leep: &'a dyn Leep) -> Result<Mirror<'a>> {
        cfg.validate()?;
        std::fs::create_dir_all(&cfg.work_dir)
            .with_context(|| format!("creating {}", cfg.work_dir.display()))?;
        let lock = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(cfg.work_dir.join("lock"))?;
        // A child forked elsewhere in this process keeps a copy of a lock
        // just released here until it execs, so a restart waits briefly.
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            match lock.try_lock() {
                Ok(()) => break,
                Err(std::fs::TryLockError::WouldBlock) if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(10))
                }
                Err(_) => bail!(
                    "another mirror holds {}; run one mirror per work directory",
                    cfg.work_dir.join("lock").display()
                ),
            }
        }
        let git_dir = cfg.work_dir.join("git");
        let repo = if git_dir.exists() {
            crate::open_git(&git_dir)?
        } else {
            gix::init_bare(&git_dir).context("creating the mirror's git repository")?
        };
        let map = Map::open(&cfg.work_dir.join("map")).context("opening the mirror map")?;
        let ex = export::Exporter::new(repo, map, cfg.style);
        let remote = Remote::new(&git_dir, &cfg.remote, cfg.git_timeout);
        let now = status::now();
        let status = Status {
            version: status::STATUS_VERSION,
            pid: std::process::id(),
            server: redact(&cfg.server),
            remote: remote.display_url(),
            started: now,
            updated: now,
            interval_secs: cfg.interval.as_secs().max(1),
            ..Default::default()
        };
        let refs = cfg
            .refs
            .iter()
            .map(|s| Tracked::new(s.clone(), true))
            .collect();
        Ok(Mirror {
            status_path: cfg.work_dir.join("status.json"),
            cfg,
            leep,
            remote,
            ex,
            refs,
            seq: None,
            next_reconcile: Instant::now(),
            next_git_poll: Instant::now(),
            remote_stale: true,
            status: Arc::new(Mutex::new(status)),
            pushes_since_gc: 0,
            lands: BTreeMap::new(),
            _lock: lock,
        })
    }

    /// Run until the process is killed. `wake` cuts a sleep short and makes
    /// the next cycle poll the remote.
    pub fn run(&mut self, wake: &status::Wake) -> ! {
        log(format!(
            "mirroring {} to {} ({} refs{})",
            redact(&self.cfg.server),
            self.remote.display_url(),
            self.refs.len(),
            if self.cfg.tags { " and tags" } else { "" }
        ));
        loop {
            self.cycle();
            if wake.wait(self.cfg.interval) {
                self.remote_stale = true;
            }
        }
    }

    /// Cycle until a cycle changes nothing (at most a few), for `--once`.
    /// Returns whether every ref ended in a healthy state.
    pub fn run_once(&mut self) -> bool {
        let mut last = None;
        for _ in 0..6 {
            self.remote_stale = true;
            self.cycle();
            let now: (Vec<_>, Vec<_>) = (
                self.refs
                    .iter()
                    .map(|r| (r.st.state, r.leep, r.remote))
                    .collect(),
                self.lands
                    .iter()
                    .map(|(n, b)| (n.clone(), b.commit, b.st.state))
                    .collect(),
            );
            if last.as_ref() == Some(&now) {
                break;
            }
            last = Some(now);
        }
        self.refs.iter().all(|r| r.st.state.healthy())
    }

    fn count_error(&self, stage: &str) {
        count_error(&self.status, stage);
    }

    /// One pass: read Leep's refs if anything moved, the remote's refs if
    /// due, then bring each ref as far as it can go.
    pub fn cycle(&mut self) {
        if let Err(e) = self.poll_leep() {
            self.count_error("leep");
            log(format!("reading refs from the server failed: {e:#}"));
        }
        if self.remote_stale || Instant::now() >= self.next_git_poll {
            match self.poll_remote() {
                Ok(()) => {
                    self.remote_stale = false;
                    self.next_git_poll = Instant::now() + self.cfg.git_poll;
                }
                Err(e) => {
                    self.count_error("git");
                    log(format!("reading the remote failed: {e:#}"));
                    for r in &mut self.refs {
                        set_state(r, RefState::Error, format!("reading the remote: {e:#}"));
                    }
                    self.next_git_poll =
                        Instant::now() + self.cfg.interval.max(Duration::from_secs(5));
                    self.publish();
                    return;
                }
            }
        }
        for i in 0..self.refs.len() {
            if Instant::now() < self.refs[i].next_try {
                continue;
            }
            let result = self.sync_ref(i);
            let max = self.cfg.max_backoff;
            let r = &mut self.refs[i];
            // States only a person can clear are looked at again when either
            // side moves (which resets `next_try`), or now and then.
            if result.is_ok() && !r.st.state.healthy() && r.st.state != RefState::Error {
                r.next_try = r
                    .next_try
                    .max(Instant::now() + max.min(Duration::from_secs(30)));
            }
            if let Err(e) = result {
                let text = format!("{e:#}");
                let stage = if text.contains("import") || text.contains("merge") {
                    "import"
                } else {
                    "export"
                };
                self.count_error(stage);
                let r = &mut self.refs[i];
                r.st.failures += 1;
                set_state(r, RefState::Error, format!("{e:#}"));
                backoff(r, self.cfg.max_backoff);
            }
        }
        self.sync_lands();
        if self.pushes_since_gc >= 200 {
            self.remote.gc_auto();
            self.pushes_since_gc = 0;
        }
        self.publish();
    }

    fn publish(&mut self) {
        let snapshot = {
            let mut s = self.status.lock().unwrap();
            s.updated = status::now();
            s.cycles += 1;
            s.refs = self.refs.iter().map(|r| r.st.clone()).collect();
            s.land_branches = self.lands.values().map(|b| b.st.clone()).collect();
            s.commits_exported = self.ex.commits_written;
            s.ref_log_seq = self.seq.flatten();
            s.clone()
        };
        if let Err(e) = status::write_status(&self.status_path, &snapshot) {
            log(format!("writing {}: {e}", self.status_path.display()));
        }
    }

    fn poll_leep(&mut self) -> Result<()> {
        let head = self.leep.ref_log_head()?;
        let due = Instant::now() >= self.next_reconcile;
        if self.seq == Some(head) && !due {
            return Ok(());
        }
        let refs = self.leep.refs()?;
        if self.cfg.tags {
            for name in refs.keys().filter(|n| n.starts_with("tags/")) {
                if !self.refs.iter().any(|r| &r.spec.leep == name) {
                    self.refs.push(Tracked::new(
                        RefSpec {
                            leep: name.clone(),
                            git: default_git_ref(name),
                        },
                        false,
                    ));
                }
            }
            // A deleted tag that was only discovered is forgotten.
            self.refs
                .retain(|r| r.configured || refs.contains_key(&r.spec.leep));
        }
        for r in &mut self.refs {
            let new = refs.get(&r.spec.leep).copied();
            if new != r.leep {
                r.leep = new;
                r.st.leep = hex(new);
                // A new value deserves an attempt now, whatever the backoff.
                r.next_try = Instant::now();
            }
        }
        self.seq = Some(head);
        if due {
            self.next_reconcile = Instant::now() + self.cfg.reconcile;
        }
        Ok(())
    }

    fn poll_remote(&mut self) -> Result<()> {
        let mut names: Vec<String> = Vec::new();
        for r in &self.refs {
            names.push(r.spec.git.clone());
            if let Some(s) = r.side_ref(&self.cfg.diverged_prefix) {
                names.push(s);
            }
        }
        let land_prefix = self
            .cfg
            .land_prefix
            .as_ref()
            .map(|p| format!("refs/heads/{p}"));
        let prefixes: Vec<String> = land_prefix.iter().cloned().collect();
        let found = self
            .remote
            .ls_remote(&names, &prefixes)
            .map_err(|e| anyhow!(e))?;
        let fetch: Vec<String> = found
            .iter()
            .filter(|(_, oid)| !self.ex.repo.has_object(*oid))
            .map(|(n, _)| n.clone())
            .collect();
        self.remote.fetch(&fetch).map_err(|e| anyhow!(e))?;
        for r in &mut self.refs {
            let new = found.get(&r.spec.git).copied();
            if new != r.remote {
                r.next_try = Instant::now();
            }
            r.remote = new;
            r.st.remote = hex(new);
            r.side = r
                .side_ref(&self.cfg.diverged_prefix)
                .and_then(|s| found.get(&s).copied());
            r.st.diverged_branch = r.side.and(r.side_ref(&self.cfg.diverged_prefix));
        }
        if let Some(prefix) = land_prefix {
            let lands: BTreeMap<String, ObjectId> = found
                .into_iter()
                .filter(|(n, _)| n.starts_with(&prefix))
                .collect();
            self.update_lands(lands);
        }
        Ok(())
    }

    fn sync_ref(&mut self, i: usize) -> Result<()> {
        let Some(l) = self.refs[i].leep else {
            let r = &mut self.refs[i];
            match r.remote {
                None => set_state(r, RefState::InSync, "absent on both sides".into()),
                Some(_) => set_state(
                    r,
                    RefState::DeletedInLeep,
                    "deleted in Leep; the remote ref is left alone".into(),
                ),
            }
            return Ok(());
        };
        let e = self
            .ex
            .export(self.leep, l)
            .with_context(|| format!("export of {l}"))?;
        {
            let r = &mut self.refs[i];
            r.st.exported = Some(e.to_string());
            if r.local != Some(e) {
                let local = format!("refs/leep-mirror/leep/{}", r.spec.leep);
                self.remote
                    .update_local_ref(&local, e)
                    .map_err(|err| anyhow!(err))?;
                r.local = Some(e);
            }
        }
        crash_point("exported");
        let g = self.refs[i].remote;
        match g {
            Some(g) if g == e => {
                self.in_sync(i)?;
                Ok(())
            }
            None => self.push(i, e, None),
            Some(g) if !self.ex.repo.has_object(g) => {
                // Fetched by the next remote poll.
                self.remote_stale = true;
                let r = &mut self.refs[i];
                set_state(r, RefState::Pending, format!("fetching remote commit {g}"));
                Ok(())
            }
            Some(_) if self.refs[i].spec.is_tag() => {
                let r = &mut self.refs[i];
                set_state(
                    r,
                    RefState::Diverged,
                    format!(
                        "the remote tag points at {} but Leep's exports to {e}; tags are never moved",
                        r.remote.map(|o| o.to_string()).unwrap_or_default()
                    ),
                );
                Ok(())
            }
            Some(g) if self.ancestor(g, e)? => self.push(i, e, None),
            Some(g) if self.ancestor(e, g)? => {
                // The remote is ahead. If its tip came from Leep, Leep moved
                // back (an undo) and must not be pushed forward again; a
                // commit the mirror imported counts as Leep's once this ref
                // has held it.
                let origin = import::leep_origin(self.leep, &mut self.ex, g)?
                    .and_then(|_| self.ex.map.leep(&g));
                let rewound = match origin {
                    Some((_, Origin::Exported)) => true,
                    Some((cg, Origin::Imported)) => {
                        self.leep.ref_had(&self.refs[i].spec.leep, &cg)?
                    }
                    None => false,
                };
                if rewound {
                    self.rewound(i, e, g)
                } else if self.cfg.import {
                    self.import(i, l, g)
                } else {
                    let r = &mut self.refs[i];
                    set_state(
                        r,
                        RefState::RemoteAhead,
                        format!(
                            "the remote has commits Leep lacks (up to {g}) and importing is off \
                             (--import)"
                        ),
                    );
                    Ok(())
                }
            }
            Some(g) => self.diverged(i, l, e, g),
        }
    }

    fn ancestor(&self, a: ObjectId, b: ObjectId) -> Result<bool> {
        self.remote.is_ancestor(a, b).map_err(|e| anyhow!(e))
    }

    fn in_sync(&mut self, i: usize) -> Result<()> {
        let prefix = self.cfg.diverged_prefix.clone();
        let r = &mut self.refs[i];
        if r.st.state != RefState::InSync {
            log(format!(
                "{}: in sync at {}",
                r.spec.leep,
                r.st.exported.as_deref().unwrap_or("?")
            ));
        }
        set_state(r, RefState::InSync, String::new());
        r.st.failures = 0;
        r.st.last_sync = Some(status::now());
        // The divergence is resolved: retire the branch Leep's side went to.
        if let (Some(side), Some(name)) = (r.side, r.side_ref(&prefix)) {
            match self.remote.push(&name, None, Some(Some(side))) {
                Ok(PushOutcome::Ok) => {
                    r.side = None;
                    r.st.diverged_branch = None;
                }
                Ok(other) => log(format!("deleting {name}: {other:?}")),
                Err(e) => log(format!("deleting {name}: {e}")),
            }
        }
        Ok(())
    }

    fn push(&mut self, i: usize, e: ObjectId, lease: Option<Option<ObjectId>>) -> Result<()> {
        let git_ref = self.refs[i].spec.git.clone();
        let status = self.status.clone();
        let outcome = self.remote.push(&git_ref, Some(e), lease);
        let r = &mut self.refs[i];
        mark_pending(r);
        match outcome {
            Ok(PushOutcome::Ok) => {
                crash_point("pushed");
                r.st.pushes.ok += 1;
                r.remote = Some(e);
                r.st.remote = Some(e.to_string());
                self.pushes_since_gc += 1;
                log(format!("{}: pushed {e} to {git_ref}", r.spec.leep));
                self.in_sync(i)
            }
            Ok(PushOutcome::Rejected(why)) => {
                // Usually the remote moved under us: classify again with
                // fresh values. A remote that keeps refusing (branch
                // protection, a hook) is retried with backoff, not hammered.
                r.st.pushes.rejected += 1;
                r.st.failures += 1;
                self.remote_stale = true;
                let state = if r.st.failures >= 3 {
                    RefState::Error
                } else {
                    RefState::Pending
                };
                set_state(r, state, format!("the remote rejected the push: {why}"));
                if r.st.failures > 1 {
                    backoff(r, self.cfg.max_backoff);
                }
                Ok(())
            }
            Ok(PushOutcome::Failed(why)) | Err(why) => {
                r.st.pushes.failed += 1;
                r.st.failures += 1;
                count_error(&status, "git");
                set_state(r, RefState::Error, why.clone());
                backoff(r, self.cfg.max_backoff);
                Ok(())
            }
        }
    }

    fn rewound(&mut self, i: usize, e: ObjectId, g: ObjectId) -> Result<()> {
        if self.cfg.allow_rewind.contains(&self.refs[i].spec.leep) {
            log(format!(
                "{}: Leep moved back; updating the remote from {g} to {e} with a lease",
                self.refs[i].spec.leep
            ));
            return self.push(i, e, Some(Some(g)));
        }
        let r = &mut self.refs[i];
        mark_pending(r);
        set_state(
            r,
            RefState::Rewound,
            format!(
                "Leep moved {} back to {e}; the remote keeps {g}. The mirror never forces it: \
                 land a revert in Leep, or allow it with --allow-rewind",
                r.spec.leep
            ),
        );
        Ok(())
    }

    fn import(&mut self, i: usize, l: ChangesetId, g: ObjectId) -> Result<()> {
        let report = import::import(self.leep, &mut self.ex, g).context("import")?;
        self.status.lock().unwrap().commits_imported += report.commits;
        let head = report
            .head
            .ok_or_else(|| anyhow!("import: {g} has no changeset after import"))?;
        crash_point("imported");
        let name = self.refs[i].spec.leep.clone();
        let outcome = self
            .leep
            .cas_ref(&name, Some(l), head, &format!("git {g}"))
            .context("import: moving the Leep ref")?;
        let r = &mut self.refs[i];
        match outcome {
            Cas::Won => {
                crash_point("landed");
                r.st.imports.landed += 1;
                r.leep = Some(head);
                r.st.leep = Some(head.to_string());
                log(format!(
                    "{name}: landed {g} from the remote ({} new changesets) as {head}",
                    report.commits
                ));
                set_state(r, RefState::Pending, format!("landed {g} from the remote"));
                // Confirm in the same breath rather than a cycle later.
                r.next_try = Instant::now();
                Ok(())
            }
            Cas::Lost(now) => {
                r.st.imports.lost_race += 1;
                r.leep = now;
                r.st.leep = hex(now);
                mark_pending(r);
                set_state(
                    r,
                    RefState::Pending,
                    "Leep moved while the remote's commits were imported; checking again".into(),
                );
                Ok(())
            }
            Cas::Refused(why) => {
                r.st.imports.refused += 1;
                mark_pending(r);
                set_state(
                    r,
                    RefState::Refused,
                    format!("Leep refused to land {g} from the remote: {why}"),
                );
                // Policy answers change slowly (a review, a new key); ask again later.
                r.next_try = Instant::now() + self.cfg.max_backoff.min(Duration::from_secs(60));
                Ok(())
            }
        }
    }

    /// Both sides moved. With `--on-divergence merge`, try a clean merge on
    /// the server first; anything short of a landed merge is reported.
    fn diverged(&mut self, i: usize, l: ChangesetId, e: ObjectId, g: ObjectId) -> Result<()> {
        let mut merge_note = None;
        if self.cfg.on_divergence == OnDivergence::Merge {
            match self.merge(i, l, g)? {
                None => return Ok(()),
                Some(why) => merge_note = Some(why),
            }
        }
        let prefix = self.cfg.diverged_prefix.clone();
        let side_name = self.refs[i].side_ref(&prefix);
        if let Some(name) = &side_name {
            let side = self.refs[i].side;
            if side != Some(e) {
                match self.remote.push(name, Some(e), Some(side)) {
                    Ok(PushOutcome::Ok) => {
                        crash_point("diverged-pushed");
                        let r = &mut self.refs[i];
                        r.side = Some(e);
                        r.st.diverged_branch = Some(name.clone());
                    }
                    Ok(other) => {
                        self.remote_stale = true;
                        log(format!("pushing {name}: {other:?}"));
                    }
                    Err(err) => log(format!("pushing {name}: {err}")),
                }
            }
        }
        let r = &mut self.refs[i];
        mark_pending(r);
        if r.st.state != RefState::Diverged {
            r.st.divergences += 1;
        }
        let hint = match &side_name {
            Some(n) => format!(
                "; Leep's side is on {n}: merge it in git and push, and the mirror lands the merge"
            ),
            None => String::new(),
        };
        let note = merge_note
            .map(|n| format!("; not merged automatically: {n}"))
            .unwrap_or_default();
        set_state(
            r,
            RefState::Diverged,
            format!(
                "Leep exports to {e} and the remote has {g}, and neither contains the other\
                 {note}{hint}"
            ),
        );
        Ok(())
    }

    /// Import the remote's side `g`, merge it into `l` on the server and
    /// land the merge by CAS from `l`. `Ok(None)` when the ref moved (the
    /// merge landed, or Leep moved first); `Ok(Some(why))` when the
    /// divergence must be reported instead.
    ///
    /// The merge is a function of `l` and `g` alone (fixed author, message
    /// and the newer parent's timestamp), so a rerun after a crash at any
    /// step asks for, and gets, the same changeset, and so the same commit.
    fn merge(&mut self, i: usize, l: ChangesetId, g: ObjectId) -> Result<Option<String>> {
        if let Some(m) = &self.refs[i].merge_memo {
            if m.leep == l && m.remote == g && m.until.is_none_or(|t| Instant::now() < t) {
                return Ok(Some(m.reason.clone()));
            }
        }
        let name = self.refs[i].spec.leep.clone();
        // A remote tip Leep moved back from is an undo the merge would
        // revert without anyone asking: the same rule as for fast-forwards.
        let origin =
            import::leep_origin(self.leep, &mut self.ex, g)?.and_then(|_| self.ex.map.leep(&g));
        let undone = match origin {
            Some((_, Origin::Exported)) => true,
            Some((cg, Origin::Imported)) => self.leep.ref_had(&name, &cg)?,
            None => false,
        };
        if undone {
            return Ok(Some(self.memo_merge(
                i,
                l,
                g,
                None,
                format!("the remote's tip {g} came from Leep, which moved {name} back from it"),
            )));
        }
        let report = import::import(self.leep, &mut self.ex, g).context("import")?;
        self.status.lock().unwrap().commits_imported += report.commits;
        let theirs = report
            .head
            .ok_or_else(|| anyhow!("import: {g} has no changeset after import"))?;
        crash_point("merge-imported");
        let timestamp = self
            .leep
            .changeset(&l)
            .context("merge: reading Leep's side")?
            .timestamp
            .max(
                self.leep
                    .changeset(&theirs)
                    .context("merge: reading the remote's side")?
                    .timestamp,
            );
        let git_ref = self.refs[i].spec.git.clone();
        let req = MergeRequest {
            parents: [l, theirs],
            message: format!(
                "Merge {} from git into {name}\n\nThe git mirror merged the remote's {g} into \
                 Leep's {l}; no path conflicted.\n",
                git_ref.strip_prefix("refs/heads/").unwrap_or(&git_ref)
            ),
            authorship: Authorship {
                author: self.cfg.merge_author.clone(),
                timestamp: Some(timestamp),
            },
            extra: BTreeMap::from([(MERGE_EXTRA.to_string(), g.to_string())]),
        };
        let merged = match self.leep.merge(&req).context("merge")? {
            MergeOutcome::Clean(m) => m,
            MergeOutcome::Conflicts(paths) => {
                self.refs[i].st.merges.conflicts += 1;
                let why = format!("{} conflicting {}", paths.len(), list_paths(&paths));
                return Ok(Some(self.memo_merge(i, l, g, None, why)));
            }
        };
        crash_point("merged");
        let outcome = self
            .leep
            .cas_ref(
                &name,
                Some(l),
                merged,
                &format!("git {g} merged as {merged}"),
            )
            .context("merge: moving the Leep ref")?;
        let r = &mut self.refs[i];
        match outcome {
            Cas::Won => {
                crash_point("merge-landed");
                r.st.merges.landed += 1;
                r.leep = Some(merged);
                r.st.leep = Some(merged.to_string());
                r.merge_memo = None;
                log(format!(
                    "{name}: merged {g} from the remote into {l} as {merged} ({} new changesets)",
                    report.commits
                ));
                mark_pending(r);
                set_state(
                    r,
                    RefState::Pending,
                    format!("merged {g} from the remote as {merged}"),
                );
                r.next_try = Instant::now();
                Ok(None)
            }
            Cas::Lost(now) => {
                r.st.merges.lost_race += 1;
                r.leep = now;
                r.st.leep = hex(now);
                mark_pending(r);
                set_state(
                    r,
                    RefState::Pending,
                    "Leep moved while the remote's side was merged; checking again".into(),
                );
                Ok(None)
            }
            Cas::Refused(why) => {
                r.st.merges.refused += 1;
                // A policy answer can change (a review, a new key); ask again later.
                let until = Instant::now() + self.cfg.max_backoff.min(Duration::from_secs(60));
                let why = format!("Leep refused to land the merge {merged}: {why}");
                Ok(Some(self.memo_merge(i, l, g, Some(until), why)))
            }
        }
    }

    fn memo_merge(
        &mut self,
        i: usize,
        l: ChangesetId,
        g: ObjectId,
        until: Option<Instant>,
        reason: String,
    ) -> String {
        self.refs[i].merge_memo = Some(MergeMemo {
            leep: l,
            remote: g,
            until,
            reason: reason.clone(),
        });
        reason
    }
}

/// Extra key on the mirror's merges: the remote commit merged.
pub const MERGE_EXTRA: &str = "git-mirror.merged";

/// `path` or `paths: a, b, c and N more`, for a detail line.
fn list_paths(paths: &[String]) -> String {
    const SHOWN: usize = 5;
    let mut s = if paths.len() == 1 {
        "path: ".to_string()
    } else {
        "paths: ".to_string()
    };
    s.push_str(
        &paths
            .iter()
            .take(SHOWN)
            .cloned()
            .collect::<Vec<_>>()
            .join(", "),
    );
    if paths.len() > SHOWN {
        s.push_str(&format!(" and {} more", paths.len() - SHOWN));
    }
    s
}

fn count_error(status: &Mutex<Status>, stage: &str) {
    *status
        .lock()
        .unwrap()
        .errors
        .entry(stage.to_string())
        .or_default() += 1;
}

fn set_state(r: &mut Tracked, state: RefState, detail: String) {
    let detail = redact(&detail);
    if !state.healthy() && (state != r.st.state || detail != r.st.detail) {
        log(format!("{}: {}: {detail}", r.spec.leep, state.label()));
    }
    if state == RefState::InSync {
        r.st.pending_since = None;
    } else if r.st.pending_since.is_none() {
        r.st.pending_since = Some(status::now());
    }
    r.st.state = state;
    r.st.detail = detail;
}

fn mark_pending(r: &mut Tracked) {
    if r.st.pending_since.is_none() {
        r.st.pending_since = Some(status::now());
    }
}

fn backoff(r: &mut Tracked, max: Duration) {
    let secs = 1u64 << r.st.failures.min(16);
    r.next_try = Instant::now() + Duration::from_secs(secs).min(max);
}

/// Report whether a mirror is healthy from its status file, for
/// `leep-git mirror status`: `Ok(true)` when every ref is in sync or catching
/// up and the file is fresh.
pub fn status_healthy(s: &Status, now: u64) -> (bool, Vec<String>) {
    let mut problems = Vec::new();
    let stale_after = s.interval_secs * 3 + 30;
    if now.saturating_sub(s.updated) > stale_after {
        problems.push(format!(
            "status is {}s old; is the mirror (pid {}) running?",
            now.saturating_sub(s.updated),
            s.pid
        ));
    }
    for r in &s.refs {
        if !r.state.healthy() {
            problems.push(format!("{}: {}: {}", r.leep_ref, r.state.label(), r.detail));
        }
    }
    (problems.is_empty(), problems)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ref_specs_default_branches_and_tags() {
        assert_eq!(
            RefSpec::parse("main").unwrap(),
            RefSpec {
                leep: "main".into(),
                git: "refs/heads/main".into()
            }
        );
        assert_eq!(
            RefSpec::parse("tags/v1").unwrap().git,
            "refs/tags/v1".to_string()
        );
        assert_eq!(
            RefSpec::parse("release/1.0:refs/heads/rel-1.0")
                .unwrap()
                .git,
            "refs/heads/rel-1.0"
        );
        assert!(RefSpec::parse("main:heads/main").is_err());
        assert!(RefSpec::parse("main:refs/tags/main").is_err());
        assert!(RefSpec::parse(":refs/heads/x").is_err());
    }

    fn config() -> Config {
        Config {
            server: "http://x".into(),
            token: None,
            remote: "file:///r".into(),
            work_dir: "/tmp/x".into(),
            refs: vec![RefSpec::parse("main").unwrap()],
            tags: false,
            import: false,
            allow_rewind: vec![],
            diverged_prefix: "leep-diverged/".into(),
            on_divergence: OnDivergence::Report,
            merge_author: DEFAULT_MERGE_AUTHOR.into(),
            land_prefix: None,
            style: CommitStyle {
                provenance_trailers: false,
                changeset_trailer: true,
            },
            interval: Duration::from_secs(1),
            git_poll: Duration::from_secs(15),
            reconcile: Duration::from_secs(60),
            git_timeout: Duration::from_secs(600),
            max_backoff: Duration::from_secs(300),
            listen: None,
        }
    }

    #[test]
    fn main_and_tags_are_never_rewound_and_the_diverged_prefix_is_reserved() {
        assert!(config().validate().is_ok());
        let mut c = config();
        c.allow_rewind = vec!["main".into()];
        assert!(c.validate().is_err());
        c.allow_rewind = vec!["tags/v1".into()];
        assert!(c.validate().is_err());
        c.allow_rewind = vec!["scratch".into()];
        assert!(c.validate().is_ok());
        let mut c = config();
        c.refs
            .push(RefSpec::parse("x:refs/heads/leep-diverged/x").unwrap());
        assert!(c.validate().is_err());
    }

    #[test]
    fn merging_and_land_branches_need_import_and_their_own_prefix() {
        let mut c = config();
        c.on_divergence = OnDivergence::Merge;
        assert!(c.validate().is_err(), "merge without --import");
        c.import = true;
        assert!(c.validate().is_ok());
        c.land_prefix = Some("leep-land/".into());
        assert!(c.validate().is_ok());
        c.land_prefix = Some("leep-land".into());
        assert!(c.validate().is_err(), "no trailing slash");
        c.land_prefix = Some("leep-diverged/land/".into());
        assert!(c.validate().is_err(), "overlaps the diverged prefix");
        c.land_prefix = Some("leep-land/".into());
        c.refs
            .push(RefSpec::parse("x:refs/heads/leep-land/x").unwrap());
        assert!(c.validate().is_err(), "a ref maps into the land prefix");
        let mut c = config();
        c.land_prefix = Some("leep-land/".into());
        assert!(c.validate().is_err(), "land branches without --import");
        assert_eq!("merge".parse(), Ok(OnDivergence::Merge));
        assert!("rebase".parse::<OnDivergence>().is_err());
    }

    #[test]
    fn path_lists_are_cut_short() {
        assert_eq!(list_paths(&["a".into()]), "path: a");
        let many: Vec<String> = (0..7).map(|i| format!("p{i}")).collect();
        assert_eq!(list_paths(&many), "paths: p0, p1, p2, p3, p4 and 2 more");
    }

    #[test]
    fn stale_or_stuck_mirrors_are_unhealthy() {
        let now = 1_000_000;
        let mut s = Status {
            interval_secs: 1,
            updated: now - 5,
            refs: vec![RefStatus {
                leep_ref: "main".into(),
                state: RefState::Pending,
                ..Default::default()
            }],
            ..Default::default()
        };
        assert!(status_healthy(&s, now).0);
        s.refs[0].state = RefState::Diverged;
        assert!(!status_healthy(&s, now).0);
        s.refs[0].state = RefState::InSync;
        s.updated = now - 100;
        assert!(!status_healthy(&s, now).0);
    }
}
