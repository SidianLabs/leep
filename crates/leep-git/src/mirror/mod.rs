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
//! A git-primary ref (`--ref main@git`, or `--primary git`; ADR 0062) makes
//! the remote the source of truth: a Leep undo is moved forward again to the
//! remote's tip (`restored`), and when both sides moved the remote's commits
//! are imported and Leep's unpushed lands are replayed onto them by
//! `POST /v1/refs/{name}/rebase-onto`, inside the ref's land coordinator, so
//! the remote only ever fast-forwards and Leep's ref is the one rewritten.
//!
//! Refs under a `--ref-prefix` (such as `review/`) are found as they appear
//! in Leep (ADR 0061): a Leep rewrite of one is pushed with a lease when the
//! remote holds only what that ref held in Leep, and a Leep delete deletes
//! the branch with the same lease. Without `--import` they are push-only:
//! anything pushed to the branch on the remote is reported and never
//! imported or overwritten. With `--import` they are two-way (ADR 0062): a
//! person's commits on the branch are imported and landed on the Leep ref,
//! and the branch is pushed with a lease on the exact tip imported.
//!
//! The decision depends only on the two current values and the map, never
//! on remembered progress, so a crash at any point is recovered by running
//! the cycle again: exports are deterministic, the mirror's merges are
//! deterministic, lands and replays are idempotent and a push of a value the
//! remote already has is a no-op.

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
    RebaseOntoRequest, RebaseOntoResponse,
};
use leep_core::changeset::EXTRA_REBASED_FROM;
use leep_core::{Changeset, ChangesetId, Hash, LandId};

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
/// `land-imported`, `land-landed`, `land-deleted`, `prefix-deleted`,
/// `replay-imported`, `replayed`, `restored`, `prefix-imported`,
/// `prefix-landed`, `ref-deleted`). Lets the crash tests stop it exactly
/// between two steps rather than at a random moment.
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
    /// `land_id` makes a retry answer with the first result (ADR 0057).
    fn land(
        &self,
        changeset: ChangesetId,
        target_ref: &str,
        land_id: Option<LandId>,
    ) -> Result<LandOutcome>;
    /// `GET /v1/merge-base`.
    fn merge_base(&self, a: ChangesetId, b: ChangesetId) -> Result<Option<ChangesetId>>;
    /// Delete ref `name` if it still holds `expected`.
    fn delete_ref(&self, name: &str, expected: ChangesetId, message: &str) -> Result<Cas>;
    /// `POST /v1/refs/{name}/rebase-onto` (ADR 0062): replay the ref's
    /// first-parent stack above `expected_base` onto `onto` and move the ref,
    /// ordered with the ref's lands on the server.
    fn rebase_onto(
        &self,
        name: &str,
        onto: ChangesetId,
        expected_base: ChangesetId,
        land_id: LandId,
    ) -> Result<ReplayOutcome>;
}

/// How a replay (`rebase-onto`) ended.
#[derive(Debug, PartialEq, Eq)]
pub enum ReplayOutcome {
    /// The ref is at `head`; `rebased` pairs each replayed changeset with
    /// its copy, oldest first.
    Moved {
        head: ChangesetId,
        rebased: Vec<(ChangesetId, ChangesetId)>,
    },
    /// The conflicting paths, the stack squashed against the new base.
    /// Nothing moved.
    Conflicts(Vec<String>),
    /// The server refused (400 or 404): a protected ref, the land policy, a
    /// missing ref.
    Refused(String),
    /// The merge base moved between the mirror's read and the call (409).
    StaleBase,
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

    fn land(
        &self,
        changeset: ChangesetId,
        target_ref: &str,
        land_id: Option<LandId>,
    ) -> Result<LandOutcome> {
        let req = LandRequest {
            changeset,
            target_ref: target_ref.to_string(),
            create: Some(false),
            land_id,
        };
        match Client::land_resending(self, &req) {
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

    fn merge_base(&self, a: ChangesetId, b: ChangesetId) -> Result<Option<ChangesetId>> {
        Ok(Client::merge_base(self, &a, &b)?.base)
    }

    fn delete_ref(&self, name: &str, expected: ChangesetId, message: &str) -> Result<Cas> {
        let req = CasRefRequest {
            expected: Some(expected),
            new: None,
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

    fn rebase_onto(
        &self,
        name: &str,
        onto: ChangesetId,
        expected_base: ChangesetId,
        land_id: LandId,
    ) -> Result<ReplayOutcome> {
        let req = RebaseOntoRequest {
            onto,
            expected_base,
            land_id: Some(land_id),
        };
        match Client::rebase_onto(self, name, &req) {
            Ok(RebaseOntoResponse::Moved { head, rebased }) => {
                Ok(ReplayOutcome::Moved { head, rebased })
            }
            Ok(RebaseOntoResponse::Conflicts { conflicts, .. }) => Ok(ReplayOutcome::Conflicts(
                conflicts
                    .into_iter()
                    .map(|c| c.path.as_str().to_string())
                    .collect(),
            )),
            Err(e) if e.kind() == ErrorKind::Conflict => Ok(ReplayOutcome::StaleBase),
            Err(e) if matches!(e.kind(), ErrorKind::User | ErrorKind::NotFound) => {
                Ok(ReplayOutcome::Refused(e.message().to_string()))
            }
            Err(e) => Err(e.into()),
        }
    }
}

/// Which side of a `--ref` is the source of truth (ADR 0062).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Primary {
    /// Both sides moved: report, and push Leep's side to the diverged
    /// branch. A Leep undo is `rewound`. ADR 0046's rules.
    #[default]
    Leep,
    /// The remote wins: Leep's unpushed lands are replayed onto its commits,
    /// and a Leep undo is moved forward again to its tip.
    Git,
}

impl std::str::FromStr for Primary {
    type Err = String;
    fn from_str(s: &str) -> std::result::Result<Self, String> {
        match s {
            "leep" => Ok(Primary::Leep),
            "git" => Ok(Primary::Git),
            _ => Err(format!("{s:?}: expected leep or git")),
        }
    }
}

/// A land id that is a function of what is being landed, so a retry after a
/// crash names the same land and the server answers with the first result.
fn land_id_for(parts: &[&str]) -> LandId {
    let h = Hash::of_blob(format!("leep-git mirror\0{}", parts.join("\0")).as_bytes());
    LandId::from_bytes(h.as_bytes()).expect("a hash is a valid land id")
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
    /// Which side is this ref's source of truth; None follows
    /// `--primary`.
    pub primary: Option<Primary>,
}

impl RefSpec {
    /// `NAME` or `NAME:GIT_REF`, with `@git` or `@leep` appended to name the
    /// source of truth. Without a git ref, `tags/T` maps to `refs/tags/T`
    /// and anything else to `refs/heads/NAME`.
    pub fn parse(s: &str) -> Result<RefSpec> {
        let (rest, primary) = match s.rsplit_once('@') {
            Some((r, p)) => (
                r,
                Some(p.parse::<Primary>().map_err(|e| anyhow!("{s:?}: {e}"))?),
            ),
            None => (s, None),
        };
        let (leep, git) = match rest.split_once(':') {
            Some((l, g)) => (l.to_string(), g.to_string()),
            None => (rest.to_string(), default_git_ref(rest)),
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
        if primary == Some(Primary::Git) && leep.starts_with("tags/") {
            bail!("{s:?}: tags are never moved on either side, so none is git-primary");
        }
        Ok(RefSpec { leep, git, primary })
    }

    /// A branch mapping with the default source of truth.
    pub fn branch(leep: &str, git: &str) -> RefSpec {
        RefSpec {
            leep: leep.to_string(),
            git: git.to_string(),
            primary: None,
        }
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
    /// The source of truth for a `--ref` that does not name one.
    pub primary: Primary,
    /// Also mirror every Leep tag (`tags/*`) to `refs/tags/*`.
    pub tags: bool,
    /// Also mirror every Leep ref under one of these prefixes (each ending
    /// in `/`) to the branch of the same name: push-only, or two-way with
    /// `import`.
    pub ref_prefixes: Vec<String>,
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
    /// Is the remote the source of truth for this ref? Never for a tag or a
    /// prefix ref, which have their own rules.
    fn git_primary(&self, spec: &RefSpec) -> bool {
        !spec.is_tag() && spec.primary.unwrap_or(self.primary) == Primary::Git
    }

    pub fn validate(&self) -> Result<()> {
        for name in &self.allow_rewind {
            if name == "main" || name.starts_with("tags/") {
                bail!("--allow-rewind {name}: main and tags are never rewound on the remote");
            }
        }
        let git_primary: Vec<&str> = self
            .refs
            .iter()
            .filter(|r| self.git_primary(r))
            .map(|r| r.leep.as_str())
            .collect();
        if !git_primary.is_empty() && !self.import {
            bail!(
                "a git-primary ref ({}) needs --import: the remote's commits are what Leep's \
                 lands are replayed onto",
                git_primary.join(", ")
            );
        }
        for name in &self.allow_rewind {
            if git_primary.contains(&name.as_str()) {
                bail!(
                    "--allow-rewind {name}: a git-primary ref restores a Leep undo from the \
                     remote; it cannot also push it back"
                );
            }
        }
        if self.refs.is_empty() && !self.tags && self.ref_prefixes.is_empty() {
            bail!("nothing to mirror: pass --ref (repeatable), --ref-prefix or --tags");
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
        self.validate_ref_prefixes()?;
        if self.on_divergence == OnDivergence::Merge && !self.import {
            bail!("--on-divergence merge needs --import");
        }
        if self.merge_author.trim().is_empty() {
            bail!("--merge-author must not be empty");
        }
        Ok(())
    }

    /// A ref prefix gives the mirror push, rewind and delete rights over a
    /// set of branches, so it must name exactly the branches it means.
    fn validate_ref_prefixes(&self) -> Result<()> {
        for p in &self.ref_prefixes {
            if p.is_empty() || p == "/" {
                bail!("--ref-prefix must not be empty or '/': it would cover every branch");
            }
            if !p.ends_with('/') || p.starts_with('/') {
                bail!("--ref-prefix {p}: must be a relative prefix ending in '/', such as review/");
            }
            // Only the one trailing '/' is the prefix's own: `review//`
            // leaves `review/`, which is no ref name.
            let stem = p.strip_suffix('/').unwrap_or(p);
            leep_core::refname::validate(stem).map_err(|e| anyhow!("--ref-prefix {p}: {e}"))?;
            if p.starts_with("tags/") {
                bail!("--ref-prefix {p}: tags are never mirrored under a prefix");
            }
            let mut owned = vec![("--diverged-prefix", self.diverged_prefix.as_str())];
            if let Some(l) = &self.land_prefix {
                owned.push(("--land-prefix", l));
            }
            for (flag, o) in owned {
                if p.starts_with(o) || o.starts_with(p.as_str()) {
                    bail!("--ref-prefix {p} and {flag} {o} overlap");
                }
            }
            for q in &self.ref_prefixes {
                if q != p && q.starts_with(p.as_str()) {
                    bail!("--ref-prefix {p} and --ref-prefix {q} overlap");
                }
            }
            for r in &self.refs {
                let branch = r.git.strip_prefix("refs/heads/");
                if r.leep.starts_with(p.as_str())
                    || branch.is_some_and(|b| b.starts_with(p.as_str()))
                {
                    bail!(
                        "--ref {} falls under --ref-prefix {p}: a ref is mirrored one way or the other",
                        r.leep
                    );
                }
            }
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
    /// Set from configuration, or discovered (`--tags`, `--ref-prefix`).
    configured: bool,
    /// The `--ref-prefix` it was discovered under: push-only, and deleted on
    /// the remote when it is deleted in Leep.
    prefix: Option<String>,
    /// The remote value last seen equal to this ref's export.
    pushed: Option<ObjectId>,
    /// Dropped from tracking at the end of the cycle.
    forget: bool,
    /// Why the remote branch's name is no Leep ref name, for a branch under
    /// a prefix that no Leep ref can own.
    bad_name: Option<String>,
    /// The last automatic merge or replay that fell back to a report, so
    /// the same pair is not tried again every cycle.
    merge_memo: Option<MergeMemo>,
    /// For a two-way prefix ref: the Leep value held when its branch was
    /// found deleted on the remote with the work landed nowhere. The branch
    /// is not pushed again until the ref moves on from it.
    gone_at: Option<ChangesetId>,
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
            prefix: None,
            pushed: None,
            forget: false,
            bad_name: None,
            merge_memo: None,
            gone_at: None,
        }
    }

    fn discovered(name: &str, prefix: &str) -> Tracked {
        let mut t = Tracked::new(RefSpec::branch(name, &format!("refs/heads/{name}")), false);
        t.prefix = Some(prefix.to_string());
        t.st.ref_prefix = Some(prefix.to_string());
        t
    }

    fn side_ref(&self, prefix: &str) -> Option<String> {
        if self.prefix.is_some() {
            return None;
        }
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
    /// Prefix branches deleted on the remote, newest last.
    deleted: Vec<status::DeletedBranch>,
    branches_deleted: u64,
    /// Two-way prefix refs deleted in Leep after their branch went, newest
    /// last.
    deleted_refs: Vec<status::DeletedBranch>,
    refs_deleted: u64,
    _lock: std::fs::File,
}

/// How many deletions the status keeps.
const DELETED_KEPT: usize = 50;

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
            .map(|s| {
                let mut t = Tracked::new(s.clone(), true);
                t.st.git_primary = cfg.git_primary(s);
                t
            })
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
            deleted: Vec::new(),
            branches_deleted: 0,
            deleted_refs: Vec::new(),
            refs_deleted: 0,
            _lock: lock,
        })
    }

    /// Run until the process is killed. `wake` cuts a sleep short and makes
    /// the next cycle poll the remote.
    pub fn run(&mut self, wake: &status::Wake) -> ! {
        log(format!(
            "mirroring {} to {} ({} refs{}{})",
            redact(&self.cfg.server),
            self.remote.display_url(),
            self.refs.len(),
            if self.cfg.tags { " and tags" } else { "" },
            self.cfg
                .ref_prefixes
                .iter()
                .map(|p| format!(", refs under {p}"))
                .collect::<String>()
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
        self.refs.retain(|r| !r.forget);
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
            s.branches_deleted = self.branches_deleted;
            s.deleted_branches = self.deleted.clone();
            s.refs_deleted = self.refs_deleted;
            s.deleted_refs = self.deleted_refs.clone();
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
                        RefSpec::branch(name, &default_git_ref(name)),
                        false,
                    ));
                }
            }
            // A deleted tag that was only discovered is forgotten.
            self.refs
                .retain(|r| r.configured || r.prefix.is_some() || refs.contains_key(&r.spec.leep));
        }
        // A prefix ref that is deleted stays tracked until its branch is
        // deleted on the remote, or reported.
        for p in &self.cfg.ref_prefixes {
            for name in refs.keys().filter(|n| n.starts_with(p.as_str())) {
                if !self.refs.iter().any(|r| &r.spec.leep == name) {
                    log(format!("{name}: found under {p}; mirroring it"));
                    self.refs.push(Tracked::discovered(name, p));
                }
            }
        }
        for r in &mut self.refs {
            let new = refs.get(&r.spec.leep).copied();
            if new != r.leep {
                r.leep = new;
                r.st.leep = hex(new);
                // A new value deserves an attempt now, whatever the backoff.
                r.next_try = Instant::now();
                if r.gone_at.is_some_and(|g| Some(g) != new) {
                    r.gone_at = None;
                }
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
        let ref_prefixes: Vec<(String, String)> = self
            .cfg
            .ref_prefixes
            .iter()
            .map(|p| (p.clone(), format!("refs/heads/{p}")))
            .collect();
        let mut prefixes: Vec<String> = land_prefix.iter().cloned().collect();
        prefixes.extend(ref_prefixes.iter().map(|(_, g)| g.clone()));
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
        // A branch under a ref prefix whose Leep ref is not known here: left
        // over from a ref deleted while the mirror was down, or not yet seen
        // in Leep. `retire` decides which, from a fresh read of Leep.
        for name in found.keys() {
            let Some((p, _)) = ref_prefixes.iter().find(|(_, g)| name.starts_with(g)) else {
                continue;
            };
            if !self.refs.iter().any(|r| &r.spec.git == name) {
                let leep = name.trim_start_matches("refs/heads/");
                let mut t = Tracked::discovered(leep, p);
                t.bad_name = leep_core::refname::validate(leep)
                    .err()
                    .map(|e| e.to_string());
                self.refs.push(t);
            }
        }
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
        if let Some(why) = self.refs[i].bad_name.clone() {
            return self.bad_name(i, &why);
        }
        if self.refs[i].leep.is_none() && self.refs[i].prefix.is_some() {
            return self.retire(i);
        }
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
        let git_primary = self.cfg.git_primary(&self.refs[i].spec);
        let two_way = self.cfg.import && self.refs[i].prefix.is_some();
        match g {
            Some(g) if g == e => {
                self.in_sync(i)?;
                Ok(())
            }
            // A branch the mirror pushed and the remote no longer has; or
            // one already found gone, looked at again in case the work
            // landed meanwhile. A ref that moved on is pushed again.
            None if two_way
                && (self.refs[i].pushed.is_some() || self.refs[i].gone_at == Some(l)) =>
            {
                self.branch_gone(i, l)
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
            Some(g) if self.refs[i].prefix.is_some() => self.prefix_moved(i, e, g),
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
                if rewound && git_primary {
                    self.restore(i, l, g)
                } else if rewound {
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
        r.pushed = r.remote;
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

    /// Did this ref hold, in Leep, the commit `g` the remote has for it?
    /// Then the remote branch holds nothing Leep lacks, and the mirror may
    /// replace or delete it with a lease on `g`. Decided from the ref log so
    /// a restarted mirror answers the same.
    fn held(&mut self, i: usize, g: ObjectId) -> Result<bool> {
        if self.refs[i].pushed == Some(g) {
            return Ok(true);
        }
        let Some(cg) = import::leep_origin(self.leep, &mut self.ex, g)? else {
            return Ok(false);
        };
        self.leep.ref_had(&self.refs[i].spec.leep, &cg)
    }

    /// A prefix ref whose remote branch is not behind Leep's export. If the
    /// branch holds only what the ref held in Leep (an agent rebased,
    /// squashed or undid it), push the export with a lease. If someone
    /// pushed to the branch: with `--import` the branch is two-way, so the
    /// person's commits are imported and landed on the Leep ref (ADR 0062);
    /// without it, report it, since push-only pushes are never overwritten.
    fn prefix_moved(&mut self, i: usize, e: ObjectId, g: ObjectId) -> Result<()> {
        if self.held(i, g)? {
            log(format!(
                "{}: rewritten in Leep; updating {} from {g} to {e} with a lease",
                self.refs[i].spec.leep, self.refs[i].spec.git
            ));
            return self.push(i, e, Some(Some(g)));
        }
        let ahead = self.ancestor(e, g)?;
        if self.cfg.import {
            let l = self.refs[i].leep.expect("a prefix ref with a Leep value");
            return if ahead {
                self.import(i, l, g)
            } else {
                self.prefix_replay(i, l, g)
            };
        }
        let r = &mut self.refs[i];
        mark_pending(r);
        let prefix = r.prefix.clone().unwrap_or_default();
        set_state(
            r,
            if ahead {
                RefState::RemoteAhead
            } else {
                RefState::Diverged
            },
            format!(
                "{} has commits pushed to the remote (up to {g}) that Leep's {} lacks; refs \
                 under --ref-prefix {prefix} are push-only, so the mirror neither imports nor \
                 overwrites them. Land that work in Leep, then delete the remote branch and \
                 the mirror pushes Leep's again",
                r.spec.git, r.spec.leep
            ),
        );
        Ok(())
    }

    /// A prefix ref that Leep no longer has. Its remote branch is deleted
    /// with a lease when it holds only what the ref held in Leep; a branch
    /// someone pushed to is reported and left alone.
    fn retire(&mut self, i: usize) -> Result<()> {
        let (name, git_ref) = (
            self.refs[i].spec.leep.clone(),
            self.refs[i].spec.git.clone(),
        );
        let Some(g) = self.refs[i].remote else {
            self.forget(i);
            return Ok(());
        };
        if !self.ex.repo.has_object(g) {
            self.remote_stale = true;
            let r = &mut self.refs[i];
            set_state(r, RefState::Pending, format!("fetching remote commit {g}"));
            return Ok(());
        }
        // The ref list read earlier may predate the ref: never delete a
        // branch on a stale answer.
        if let Some(now) = self.leep.refs()?.get(&name).copied() {
            let r = &mut self.refs[i];
            r.leep = Some(now);
            r.st.leep = Some(now.to_string());
            r.next_try = Instant::now();
            set_state(r, RefState::Pending, "found in Leep".into());
            return Ok(());
        }
        if !self.held(i, g)? {
            let r = &mut self.refs[i];
            mark_pending(r);
            set_state(
                r,
                RefState::RemoteAhead,
                format!(
                    "{name} is not in Leep, but {git_ref} on the remote is at {g}, which \
                     {name} never held in Leep; the mirror leaves it alone. Delete it by hand \
                     once its work is landed or not needed"
                ),
            );
            return Ok(());
        }
        let max = self.cfg.max_backoff;
        match self.remote.push(&git_ref, None, Some(Some(g))) {
            Ok(PushOutcome::Ok) => {
                crash_point("prefix-deleted");
                log(format!(
                    "{name}: deleted in Leep; deleted {git_ref} at {g} with a lease"
                ));
                self.branches_deleted += 1;
                self.deleted.push(status::DeletedBranch {
                    leep_ref: name,
                    git_ref,
                    commit: g.to_string(),
                    at: status::now(),
                });
                if self.deleted.len() > DELETED_KEPT {
                    self.deleted.remove(0);
                }
                self.forget(i);
            }
            Ok(PushOutcome::Rejected(why)) => {
                // Usually the branch moved since the last poll: the next
                // poll sees its new value, which `held` then judges.
                self.remote_stale = true;
                let r = &mut self.refs[i];
                r.st.pushes.rejected += 1;
                r.st.failures += 1;
                let state = if r.st.failures >= 3 {
                    RefState::Error
                } else {
                    RefState::Pending
                };
                set_state(
                    r,
                    state,
                    format!("the remote refused to delete {git_ref} at {g}: {why}"),
                );
                if r.st.failures > 1 {
                    backoff(r, max);
                }
            }
            Ok(PushOutcome::Failed(why)) | Err(why) => {
                count_error(&self.status, "git");
                let r = &mut self.refs[i];
                r.st.pushes.failed += 1;
                r.st.failures += 1;
                set_state(r, RefState::Error, why);
                backoff(r, max);
            }
        }
        Ok(())
    }

    /// A branch under a prefix whose name Leep rejects: no Leep ref can hold
    /// it, so asking the server about it would fail every cycle. Report it
    /// once and leave it to a person.
    fn bad_name(&mut self, i: usize, why: &str) -> Result<()> {
        if self.refs[i].remote.is_none() {
            self.forget(i);
            return Ok(());
        }
        let r = &mut self.refs[i];
        mark_pending(r);
        let prefix = r.prefix.clone().unwrap_or_default();
        set_state(
            r,
            RefState::RemoteAhead,
            format!(
                "{} is under --ref-prefix {prefix} but no Leep ref can have its name ({why}); \
                 the mirror never pushes, imports or deletes it. Rename or delete it by hand",
                r.spec.git
            ),
        );
        Ok(())
    }

    /// Stop tracking a prefix ref and drop the mirror's local refs for it.
    fn forget(&mut self, i: usize) {
        let r = &mut self.refs[i];
        r.forget = true;
        let local = format!("refs/leep-mirror/leep/{}", r.spec.leep);
        let _ = self.remote.delete_local_ref(&local);
        let _ = self
            .remote
            .delete_local_ref(&git::tracking_ref(&r.spec.git));
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

    /// Both sides moved. On a git-primary ref, replay Leep's unpushed lands
    /// onto the remote's commits; otherwise, with `--on-divergence merge`,
    /// try a clean merge on the server first. Anything short of a moved ref
    /// is reported.
    fn diverged(&mut self, i: usize, l: ChangesetId, e: ObjectId, g: ObjectId) -> Result<()> {
        let mut merge_note = None;
        let git_primary = self.cfg.git_primary(&self.refs[i].spec);
        if git_primary {
            match self.replay(i, l, g)? {
                None => return Ok(()),
                Some(why) => merge_note = Some(why),
            }
        } else if self.cfg.on_divergence == OnDivergence::Merge {
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
            .map(|n| {
                if git_primary {
                    format!("; not replayed onto the remote's commits: {n}")
                } else {
                    format!("; not merged automatically: {n}")
                }
            })
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

    /// A git-primary ref Leep moved back from the remote's tip `g`: the
    /// remote is the truth, so the ref is moved forward to `g`'s changeset
    /// again (ADR 0062). A fast-forward, so forward-only protection allows
    /// it. An operator who meant it reverts on the remote.
    fn restore(&mut self, i: usize, l: ChangesetId, g: ObjectId) -> Result<()> {
        let name = self.refs[i].spec.leep.clone();
        let Some((cg, _)) = self.ex.map.leep(&g) else {
            bail!("restore: the remote's {g} came from Leep but the map does not list it");
        };
        let outcome = self
            .leep
            .cas_ref(&name, Some(l), cg, &format!("restored from git {g}"))
            .context("restore: moving the Leep ref")?;
        let r = &mut self.refs[i];
        match outcome {
            Cas::Won => {
                crash_point("restored");
                r.st.restores += 1;
                r.leep = Some(cg);
                r.st.leep = Some(cg.to_string());
                log(format!(
                    "{name}: moved back in Leep to {l}; restored to the remote's {g} ({cg}), \
                     which is the truth for a git-primary ref"
                ));
                set_state(
                    r,
                    RefState::Restored,
                    format!("Leep moved back to {l}; restored to the remote's {g} as {cg}"),
                );
                r.next_try = Instant::now();
            }
            Cas::Lost(now) => {
                r.leep = now;
                r.st.leep = hex(now);
                mark_pending(r);
                set_state(
                    r,
                    RefState::Pending,
                    "Leep moved while its undo was being restored; checking again".into(),
                );
            }
            Cas::Refused(why) => {
                mark_pending(r);
                set_state(
                    r,
                    RefState::Refused,
                    format!("Leep refused to restore {name} to the remote's {g}: {why}"),
                );
                r.next_try = Instant::now() + self.cfg.max_backoff.min(Duration::from_secs(60));
            }
        }
        Ok(())
    }

    /// Both sides of a git-primary ref moved: import the remote's side `g`
    /// and ask the server to replay Leep's unpushed lands onto it, moving the
    /// ref in one step ordered with its lands (ADR 0062). `Ok(None)` when
    /// the ref moved or must be classified again; `Ok(Some(why))` when the
    /// divergence must be reported instead. Nothing is resolved here.
    fn replay(&mut self, i: usize, l: ChangesetId, g: ObjectId) -> Result<Option<String>> {
        if let Some(m) = &self.refs[i].merge_memo {
            if m.leep == l && m.remote == g && m.until.is_none_or(|t| Instant::now() < t) {
                return Ok(Some(m.reason.clone()));
            }
        }
        let name = self.refs[i].spec.leep.clone();
        let report = import::import(self.leep, &mut self.ex, g).context("import")?;
        self.status.lock().unwrap().commits_imported += report.commits;
        let theirs = report
            .head
            .ok_or_else(|| anyhow!("import: {g} has no changeset after import"))?;
        crash_point("replay-imported");
        let Some(base) = self
            .leep
            .merge_base(l, theirs)
            .context("replay: merge base")?
        else {
            return Ok(Some(self.memo_merge(
                i,
                l,
                g,
                None,
                format!("Leep's {l} and the remote's {g} share no history"),
            )));
        };
        let id = land_id_for(&["replay", &name, &l.to_string(), &g.to_string()]);
        let outcome = self
            .leep
            .rebase_onto(&name, theirs, base, id)
            .context("replay: rebase-onto")?;
        let max = self.cfg.max_backoff;
        let r = &mut self.refs[i];
        match outcome {
            ReplayOutcome::Moved { head, rebased } => {
                crash_point("replayed");
                r.st.replays.ok += 1;
                r.leep = Some(head);
                r.st.leep = Some(head.to_string());
                r.merge_memo = None;
                log(format!(
                    "{name}: replayed {} unpushed lands onto the remote's {g} ({} new \
                     changesets); the ref is at {head}",
                    rebased.len(),
                    report.commits
                ));
                mark_pending(r);
                set_state(
                    r,
                    RefState::Pending,
                    format!(
                        "replayed {} lands onto {g} from the remote; pushing",
                        rebased.len()
                    ),
                );
                r.next_try = Instant::now();
                Ok(None)
            }
            ReplayOutcome::StaleBase => {
                r.st.replays.stale_base += 1;
                mark_pending(r);
                set_state(
                    r,
                    RefState::Pending,
                    "the base moved while the remote's commits were imported; checking again"
                        .into(),
                );
                // Either side moved: read both again.
                self.seq = None;
                self.remote_stale = true;
                Ok(None)
            }
            ReplayOutcome::Conflicts(paths) => {
                r.st.replays.conflicts += 1;
                let why = format!(
                    "{} conflicting {}; merge the diverged branch on the remote and push",
                    paths.len(),
                    list_paths(&paths)
                );
                Ok(Some(self.memo_merge(i, l, g, None, why)))
            }
            ReplayOutcome::Refused(why) => {
                r.st.replays.refused += 1;
                let until = Instant::now() + max.min(Duration::from_secs(60));
                Ok(Some(self.memo_merge(
                    i,
                    l,
                    g,
                    Some(until),
                    format!("Leep refused the replay: {why}"),
                )))
            }
        }
    }

    /// A two-way prefix ref where an agent rewrote the stack while a person
    /// pushed: import the person's commits, land them on the Leep ref (the
    /// server replays them onto Leep's new tip, as it does a land branch),
    /// and push the result with a lease on the exact tip imported (ADR
    /// 0062). Conflicts are reported and nothing moves.
    fn prefix_replay(&mut self, i: usize, l: ChangesetId, g: ObjectId) -> Result<()> {
        if let Some(m) = &self.refs[i].merge_memo {
            if m.leep == l && m.remote == g && m.until.is_none_or(|t| Instant::now() < t) {
                let why = m.reason.clone();
                return self.report_prefix_conflict(i, &why);
            }
        }
        let name = self.refs[i].spec.leep.clone();
        let report = import::import(self.leep, &mut self.ex, g).context("import")?;
        self.status.lock().unwrap().commits_imported += report.commits;
        let theirs = report
            .head
            .ok_or_else(|| anyhow!("import: {g} has no changeset after import"))?;
        crash_point("prefix-imported");
        let id = land_id_for(&["prefix", &name, &g.to_string()]);
        let outcome = self
            .leep
            .land(theirs, &name, Some(id))
            .with_context(|| format!("import: landing {g} onto {name}"))?;
        let max = self.cfg.max_backoff;
        match outcome {
            LandOutcome::Landed(landed) => {
                crash_point("prefix-landed");
                let r = &mut self.refs[i];
                r.st.imports.landed += 1;
                r.merge_memo = None;
                r.leep = Some(landed);
                r.st.leep = Some(landed.to_string());
                log(format!(
                    "{name}: landed {g} from the remote onto the rewritten ref as {landed} ({} \
                     new changesets); pushing with a lease on {g}",
                    report.commits
                ));
                let e = self
                    .ex
                    .export(self.leep, landed)
                    .with_context(|| format!("export of {landed}"))?;
                let r = &mut self.refs[i];
                r.st.exported = Some(e.to_string());
                let local = format!("refs/leep-mirror/leep/{name}");
                self.remote
                    .update_local_ref(&local, e)
                    .map_err(|err| anyhow!(err))?;
                r.local = Some(e);
                self.push(i, e, Some(Some(g)))
            }
            LandOutcome::Conflicts(paths) => {
                self.refs[i].st.imports.refused += 1;
                let why = format!(
                    "{} conflicting {} between the commits pushed to {} (up to {g}) and Leep's \
                     rewritten {name}; merge one side into the other and push or land it",
                    paths.len(),
                    list_paths(&paths),
                    self.refs[i].spec.git
                );
                self.memo_merge(i, l, g, None, why.clone());
                self.report_prefix_conflict(i, &why)
            }
            LandOutcome::Refused(why) => {
                let r = &mut self.refs[i];
                r.st.imports.refused += 1;
                mark_pending(r);
                set_state(
                    r,
                    RefState::Refused,
                    format!("Leep refused to land {g} from the remote onto {name}: {why}"),
                );
                r.next_try = Instant::now() + max.min(Duration::from_secs(60));
                Ok(())
            }
        }
    }

    fn report_prefix_conflict(&mut self, i: usize, why: &str) -> Result<()> {
        let r = &mut self.refs[i];
        mark_pending(r);
        if r.st.state != RefState::Diverged {
            r.st.divergences += 1;
        }
        set_state(r, RefState::Diverged, why.to_string());
        Ok(())
    }

    /// The branch of a two-way prefix ref is gone from the remote after the
    /// mirror pushed it. When the ref's work is landed (its tip, or a
    /// rebased copy of it, is reachable from a mirrored `--ref`) the Leep
    /// ref is deleted too; otherwise the ref is kept and reported, and the
    /// branch is not pushed again until the ref moves on (ADR 0062).
    fn branch_gone(&mut self, i: usize, l: ChangesetId) -> Result<()> {
        let (name, git_ref) = (
            self.refs[i].spec.leep.clone(),
            self.refs[i].spec.git.clone(),
        );
        // Never act on a stale ref list: the ref may have moved on already.
        let now = self.leep.refs()?.get(&name).copied();
        if now != Some(l) {
            let r = &mut self.refs[i];
            r.leep = now;
            r.st.leep = hex(now);
            r.next_try = Instant::now();
            return Ok(());
        }
        if self.landed_elsewhere(l)? {
            match self
                .leep
                .delete_ref(
                    &name,
                    l,
                    &format!("{git_ref} deleted on the remote; landed"),
                )
                .context("deleting the Leep ref")?
            {
                Cas::Won => {
                    crash_point("ref-deleted");
                    log(format!(
                        "{name}: {git_ref} was deleted on the remote and {l} is landed; \
                         deleted the Leep ref"
                    ));
                    self.refs_deleted += 1;
                    self.deleted_refs.push(status::DeletedBranch {
                        leep_ref: name,
                        git_ref,
                        commit: l.to_string(),
                        at: status::now(),
                    });
                    if self.deleted_refs.len() > DELETED_KEPT {
                        self.deleted_refs.remove(0);
                    }
                    self.forget(i);
                }
                Cas::Lost(now) => {
                    let r = &mut self.refs[i];
                    r.leep = now;
                    r.st.leep = hex(now);
                    r.next_try = Instant::now();
                }
                Cas::Refused(why) => {
                    let r = &mut self.refs[i];
                    mark_pending(r);
                    set_state(
                        r,
                        RefState::Refused,
                        format!("Leep refused to delete {name}: {why}"),
                    );
                    r.next_try = Instant::now() + self.cfg.max_backoff.min(Duration::from_secs(60));
                }
            }
            return Ok(());
        }
        let r = &mut self.refs[i];
        r.gone_at = Some(l);
        // The branch is gone: once the ref moves on, it is pushed anew.
        r.pushed = None;
        mark_pending(r);
        set_state(
            r,
            RefState::DeletedOnRemote,
            format!(
                "{git_ref} was deleted on the remote, but {name} at {l} is landed on no mirrored \
                 ref; the Leep ref is kept and the branch is not pushed again until the ref \
                 moves. Land the work, or delete the ref in Leep"
            ),
        );
        Ok(())
    }

    /// Is `l`, or the server's rebased copy of it, in the history of a
    /// mirrored `--ref`? The copy is looked for along the first-parent chain
    /// of each ref's head, within the same bound a land uses to recognise
    /// its own retry.
    fn landed_elsewhere(&self, l: ChangesetId) -> Result<bool> {
        const SCAN: usize = 1024;
        let heads: Vec<ChangesetId> = self
            .refs
            .iter()
            .filter(|r| r.configured && !r.spec.is_tag())
            .filter_map(|r| r.leep)
            .collect();
        for h in heads {
            if self.leep.merge_base(l, h)? == Some(l) {
                return Ok(true);
            }
            let mut cur = h;
            for _ in 0..SCAN {
                let cs = self.leep.changeset(&cur)?;
                if cs.extra.get(EXTRA_REBASED_FROM) == Some(&l.to_string()) {
                    return Ok(true);
                }
                let Some(p) = cs.parents.first() else { break };
                cur = *p;
            }
        }
        Ok(false)
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
                git: "refs/heads/main".into(),
                primary: None,
            }
        );
        assert_eq!(
            RefSpec::parse("main@git").unwrap(),
            RefSpec {
                leep: "main".into(),
                git: "refs/heads/main".into(),
                primary: Some(Primary::Git),
            }
        );
        assert_eq!(
            RefSpec::parse("main:refs/heads/trunk@leep")
                .unwrap()
                .primary,
            Some(Primary::Leep)
        );
        assert!(RefSpec::parse("main@upstream").is_err());
        assert!(
            RefSpec::parse("tags/v1@git").is_err(),
            "tags are never git-primary"
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
            primary: Primary::Leep,
            tags: false,
            ref_prefixes: vec![],
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
    fn ref_prefixes_must_be_narrow_and_not_overlap_owned_prefixes_or_refs() {
        let with = |prefixes: &[&str]| {
            let mut c = config();
            c.import = true;
            c.land_prefix = Some("leep-land/".into());
            c.ref_prefixes = prefixes.iter().map(|p| p.to_string()).collect();
            c.validate()
        };
        assert!(with(&["review/"]).is_ok());
        assert!(with(&["review/", "release/"]).is_ok());
        assert!(with(&[""]).is_err(), "empty");
        assert!(with(&["/"]).is_err(), "slash");
        assert!(with(&["review//"]).is_err(), "empty component");
        assert!(with(&["review//x/"]).is_err(), "empty component inside");
        assert!(with(&["review"]).is_err(), "no trailing slash");
        assert!(with(&["/review/"]).is_err(), "absolute");
        assert!(with(&["re view/"]).is_err(), "not a ref name");
        assert!(with(&["tags/"]).is_err(), "tags");
        assert!(with(&["leep-diverged/"]).is_err(), "the diverged prefix");
        assert!(
            with(&["leep-diverged/x/"]).is_err(),
            "inside the diverged prefix"
        );
        assert!(with(&["leep-land/"]).is_err(), "the land prefix");
        assert!(
            with(&["leep-land/main/"]).is_err(),
            "inside the land prefix"
        );
        assert!(with(&["leep-"]).is_err(), "no trailing slash");
        assert!(with(&["review/", "review/a/"]).is_err(), "two that overlap");
        // The diverged prefix covers a ref prefix the other way round.
        let mut c = config();
        c.diverged_prefix = "review/diverged/".into();
        c.ref_prefixes = vec!["review/".into()];
        assert!(c.validate().is_err());
        // A --ref under the prefix, by its Leep name or by its branch.
        let mut c = config();
        c.ref_prefixes = vec!["review/".into()];
        c.refs.push(RefSpec::parse("review/x").unwrap());
        assert!(c.validate().is_err());
        let mut c = config();
        c.ref_prefixes = vec!["review/".into()];
        c.refs
            .push(RefSpec::parse("x:refs/heads/review/x").unwrap());
        assert!(c.validate().is_err());
        // A prefix alone is something to mirror.
        let mut c = config();
        c.refs.clear();
        assert!(c.validate().is_err());
        c.ref_prefixes = vec!["review/".into()];
        assert!(c.validate().is_ok());
    }

    #[test]
    fn git_primary_refs_need_import_and_cannot_rewind() {
        let mut c = config();
        c.primary = Primary::Git;
        assert!(c.validate().is_err(), "git-primary without --import");
        c.import = true;
        assert!(c.validate().is_ok());
        assert!(c.git_primary(&c.refs[0]));
        c.refs.push(RefSpec::parse("scratch@leep").unwrap());
        assert!(!c.git_primary(&c.refs[1]), "@leep overrides --primary git");
        c.refs.push(RefSpec::parse("tags/v1").unwrap());
        assert!(!c.git_primary(&c.refs[2]), "tags are never git-primary");
        // A single ref can be git-primary under the default.
        let mut c = config();
        c.import = true;
        c.refs = vec![
            RefSpec::parse("main@git").unwrap(),
            RefSpec::parse("scratch").unwrap(),
        ];
        assert!(c.validate().is_ok());
        assert!(c.git_primary(&c.refs[0]) && !c.git_primary(&c.refs[1]));
        c.allow_rewind = vec!["scratch".into()];
        assert!(c.validate().is_ok());
        c.refs[1] = RefSpec::parse("scratch@git").unwrap();
        assert!(c.validate().is_err(), "allow-rewind on a git-primary ref");
        assert_eq!("git".parse(), Ok(Primary::Git));
        assert!("remote".parse::<Primary>().is_err());
    }

    #[test]
    fn land_ids_are_functions_of_their_parts() {
        let a = land_id_for(&["replay", "main", "x", "y"]);
        assert_eq!(a, land_id_for(&["replay", "main", "x", "y"]));
        assert_ne!(a, land_id_for(&["replay", "main", "x", "z"]));
        assert_ne!(a, land_id_for(&["prefix", "main", "x", "y"]));
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
