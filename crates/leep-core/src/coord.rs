//! Coordination state (ADR 0095): advisory, expiring rows about work in
//! flight, beside the immutable history and its refs.
//!
//! A session is one agent run on one task in one repository, keyed by
//! (repository, principal, agent session). It says what the agent means to
//! do (title, approach, task key), where (scope prefixes and claims) and
//! where it has already written (touched paths). Nothing here is hashed or
//! encoded with postcard: sessions are JSON on the wire and columns at rest,
//! and may grow optional fields. Nothing here may change a land's answer:
//! dropping every row loses no history and moves no ref.
//!
//! The store contract is [`CoordStore`]. It is a trait of its own, not part
//! of [`crate::MetaStore`], because nothing transactional joins it to refs;
//! a SQL meta store hands one out over its own pool
//! ([`crate::MetaStore::coord_store`]).

use std::collections::BTreeSet;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::{ChangesetId, Error, RepoPath, Result};

/// Longest session title, in bytes; one line of plain text.
pub const MAX_TITLE_BYTES: usize = 200;
/// Longest approach, in bytes; plain text, newlines and tabs allowed.
pub const MAX_APPROACH_BYTES: usize = 2048;
/// Longest agent session key (`agent.session`), task key, agent model or
/// principal, in bytes.
pub const MAX_KEY_BYTES: usize = 128;
/// Most scope prefixes, and separately most claims, one session declares.
pub const MAX_SCOPE_PATHS: usize = 64;
/// Most touched paths kept per session. More are folded into their
/// directories (see [`bound_paths`]), which still overlap everything below
/// them, so a large change stays visible without growing the table.
pub const MAX_TOUCHED_PATHS: usize = 256;
/// Longest path kept, in bytes; a longer touched path is kept as the
/// nearest ancestor that fits (index entries have a size limit).
pub const MAX_PATH_BYTES: usize = 1024;
/// Most sessions one principal may have active at once. One more is
/// refused with [`TOO_MANY_SESSIONS_MARKER`] (an inferred one is skipped),
/// so a runaway token cannot fill the table.
pub const MAX_ACTIVE_PER_PRINCIPAL: usize = 1000;
/// Stable text in the 400 for a principal at its active session limit.
pub const TOO_MANY_SESSIONS_MARKER: &str = "active sessions, the most one principal may have";

/// A session not heard from for this long is shown as `idle`.
pub const IDLE_AFTER_SECS: i64 = 10 * 60;
/// A session not heard from for this long is ended as `expired`.
pub const EXPIRE_AFTER_SECS: i64 = 24 * 60 * 60;
/// Paths of an ended session are deleted this long after it ended.
pub const PATHS_RETENTION_SECS: i64 = 24 * 60 * 60;
/// An ended session is deleted this long after it ended. The durable
/// record of the work is the changesets' own `agent.*` extras.
pub const SESSION_RETENTION_SECS: i64 = 30 * 24 * 60 * 60;

/// Where a session is in its life. Stored as its [`as_str`] text, except
/// [`SessionState::Idle`], which is never stored: it is an active session
/// not heard from for [`IDLE_AFTER_SECS`].
///
/// [`as_str`]: SessionState::as_str
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionState {
    Active,
    Idle,
    /// Its head changeset landed.
    Landed,
    /// Its principal ended it.
    Abandoned,
    /// Nothing was heard from it for [`EXPIRE_AFTER_SECS`].
    Expired,
}

impl SessionState {
    pub const ALL: [SessionState; 5] = [
        SessionState::Active,
        SessionState::Idle,
        SessionState::Landed,
        SessionState::Abandoned,
        SessionState::Expired,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            SessionState::Active => "active",
            SessionState::Idle => "idle",
            SessionState::Landed => "landed",
            SessionState::Abandoned => "abandoned",
            SessionState::Expired => "expired",
        }
    }

    pub fn parse(s: &str) -> Result<Self> {
        Self::ALL
            .into_iter()
            .find(|v| v.as_str() == s)
            .ok_or_else(|| {
                Error::Invalid(format!(
                    "unknown session state {s:?} (expected active, idle, landed, abandoned or \
                     expired)"
                ))
            })
    }

    /// Whether the session has ended (landed, abandoned or expired).
    pub fn is_ended(self) -> bool {
        !matches!(self, SessionState::Active | SessionState::Idle)
    }
}

/// How the server learned of a session.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionOrigin {
    /// An agent declared it (`POST /v1/sessions`).
    Declared,
    /// The server made it from a changeset carrying `agent.session`.
    Inferred,
}

impl SessionOrigin {
    pub fn as_str(self) -> &'static str {
        match self {
            SessionOrigin::Declared => "declared",
            SessionOrigin::Inferred => "inferred",
        }
    }

    pub fn parse(s: &str) -> Result<Self> {
        match s {
            "declared" => Ok(SessionOrigin::Declared),
            "inferred" => Ok(SessionOrigin::Inferred),
            _ => Err(Error::Invalid(format!("unknown session origin {s:?}"))),
        }
    }
}

/// What a session path row says about its path. Stored as [`as_str`].
///
/// [`as_str`]: PathKind::as_str
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum PathKind {
    /// Declared: the agent means to work here.
    Scope,
    /// Declared: the agent means to own this, and asks to be told before
    /// anyone starts here. A stronger warning, never a lock.
    Claim,
    /// Inferred: a changeset of the session changed this path.
    Touched,
}

impl PathKind {
    pub const ALL: [PathKind; 3] = [PathKind::Scope, PathKind::Claim, PathKind::Touched];

    pub fn as_str(self) -> &'static str {
        match self {
            PathKind::Scope => "scope",
            PathKind::Claim => "claim",
            PathKind::Touched => "touched",
        }
    }

    pub fn parse(s: &str) -> Result<Self> {
        Self::ALL
            .into_iter()
            .find(|v| v.as_str() == s)
            .ok_or_else(|| Error::Invalid(format!("unknown session path kind {s:?}")))
    }
}

/// One session as stored. Never hashed; columns at rest.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Session {
    /// Assigned by the store, increasing.
    pub id: u64,
    /// Token name of the principal; never taken from a request body.
    pub principal: String,
    /// The `agent.session` value the agent's changesets carry.
    pub agent_session: String,
    pub agent_model: Option<String>,
    pub title: String,
    pub approach: String,
    pub task_key: Option<String>,
    pub target_ref: Option<String>,
    /// The changeset the work started from.
    pub base: Option<ChangesetId>,
    /// The newest changeset the session created.
    pub head: Option<ChangesetId>,
    pub origin: SessionOrigin,
    /// Never [`SessionState::Idle`] in a store.
    pub state: SessionState,
    /// Unix seconds by the server.
    pub created: i64,
    /// Unix seconds of the last request or write heard from it.
    pub last_seen: i64,
    pub ended: Option<i64>,
    /// For a landed session, the changeset its ref got.
    pub landed_as: Option<ChangesetId>,
    /// Sorted, deduplicated.
    pub scope: Vec<String>,
    pub claims: Vec<String>,
    pub touched: Vec<String>,
}

impl Session {
    /// Every path of the session, of every kind.
    pub fn paths(&self) -> impl Iterator<Item = (PathKind, &str)> {
        let scope = self.scope.iter().map(|p| (PathKind::Scope, p.as_str()));
        let claims = self.claims.iter().map(|p| (PathKind::Claim, p.as_str()));
        let touched = self.touched.iter().map(|p| (PathKind::Touched, p.as_str()));
        scope.chain(claims).chain(touched)
    }

    /// The state to show at `now`: an active session not heard from for
    /// [`IDLE_AFTER_SECS`] is idle, and one past [`EXPIRE_AFTER_SECS`] is
    /// expired even before the sweep ends it.
    pub fn state_at(&self, now: i64) -> SessionState {
        match self.state {
            SessionState::Active if now - self.last_seen >= EXPIRE_AFTER_SECS => {
                SessionState::Expired
            }
            SessionState::Active if now - self.last_seen >= IDLE_AFTER_SECS => SessionState::Idle,
            s => s,
        }
    }
}

/// What [`CoordStore::upsert_session`] writes for (principal,
/// agent_session). `None` fields keep what is stored.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionUpdate {
    pub principal: String,
    pub agent_session: String,
    /// `Inferred` only fills what is empty and never turns a declared
    /// session into an inferred one; `Declared` sets every given field.
    pub origin: SessionOrigin,
    pub agent_model: Option<String>,
    pub title: Option<String>,
    pub approach: Option<String>,
    pub task_key: Option<String>,
    pub target_ref: Option<String>,
    pub base: Option<ChangesetId>,
    pub head: Option<ChangesetId>,
    /// Replaces the scope when set.
    pub scope: Option<Vec<String>>,
    /// Replaces the claims when set.
    pub claims: Option<Vec<String>>,
    /// Added to the touched paths, bounded by [`bound_paths`].
    pub touch: Vec<String>,
    /// Unix seconds; also the session's `last_seen`.
    pub at: i64,
    /// Refuse to create or revive a session when the principal already has
    /// this many active ones ([`MAX_ACTIVE_PER_PRINCIPAL`] in a server).
    pub max_active: usize,
}

impl SessionUpdate {
    /// An update that only says the session is alive.
    pub fn new(principal: &str, agent_session: &str, origin: SessionOrigin, at: i64) -> Self {
        SessionUpdate {
            principal: principal.to_string(),
            agent_session: agent_session.to_string(),
            origin,
            agent_model: None,
            title: None,
            approach: None,
            task_key: None,
            target_ref: None,
            base: None,
            head: None,
            scope: None,
            claims: None,
            touch: Vec::new(),
            at,
            max_active: MAX_ACTIVE_PER_PRINCIPAL,
        }
    }

    /// Check the free-form parts before anything is stored.
    pub fn check(&self) -> Result<()> {
        check_key("principal", &self.principal)?;
        check_key("agent session", &self.agent_session)?;
        if let Some(m) = &self.agent_model {
            check_line("agent model", m, MAX_KEY_BYTES)?;
        }
        if let Some(t) = &self.title {
            check_line("title", t, MAX_TITLE_BYTES)?;
        }
        if let Some(a) = &self.approach {
            check_text("approach", a, MAX_APPROACH_BYTES)?;
        }
        if let Some(k) = &self.task_key {
            check_key("task key", k)?;
        }
        if let Some(r) = &self.target_ref {
            crate::refname::validate(r)?;
        }
        for (what, paths) in [("scope", &self.scope), ("claims", &self.claims)] {
            if let Some(p) = paths {
                if p.len() > MAX_SCOPE_PATHS {
                    return Err(Error::Invalid(format!(
                        "a session's {what} names at most {MAX_SCOPE_PATHS} paths, got {}",
                        p.len()
                    )));
                }
                for path in p {
                    check_path(path)?;
                }
            }
        }
        Ok(())
    }
}

/// An agent session key, task key or principal: 1 to [`MAX_KEY_BYTES`]
/// bytes of printable ASCII without spaces, so it fits an HTTP header and
/// carries no prose.
pub fn check_key(what: &str, k: &str) -> Result<()> {
    if k.is_empty() || k.len() > MAX_KEY_BYTES {
        return Err(Error::Invalid(format!(
            "{what} must be 1 to {MAX_KEY_BYTES} bytes, got {}",
            k.len()
        )));
    }
    if !k.bytes().all(|b| b.is_ascii_graphic()) {
        return Err(Error::Invalid(format!(
            "{what} {k:?} may only hold printable ASCII without spaces"
        )));
    }
    Ok(())
}

fn check_line(what: &str, s: &str, max: usize) -> Result<()> {
    if s.len() > max {
        return Err(Error::Invalid(format!(
            "session {what} is {} bytes; the limit is {max}",
            s.len()
        )));
    }
    if s.chars().any(char::is_control) {
        return Err(Error::Invalid(format!(
            "session {what} is one line without control characters"
        )));
    }
    Ok(())
}

fn check_text(what: &str, s: &str, max: usize) -> Result<()> {
    if s.len() > max {
        return Err(Error::Invalid(format!(
            "session {what} is {} bytes; the limit is {max}",
            s.len()
        )));
    }
    if s.chars().any(|c| c.is_control() && c != '\n' && c != '\t') {
        return Err(Error::Invalid(format!(
            "session {what} may not contain control characters"
        )));
    }
    Ok(())
}

/// A declared scope or claim path: a repository path (a file or a
/// directory), at most [`MAX_PATH_BYTES`], without control characters.
pub fn check_path(p: &str) -> Result<()> {
    RepoPath::new(p)?;
    if p.len() > MAX_PATH_BYTES {
        return Err(Error::Invalid(format!(
            "session path is {} bytes; the limit is {MAX_PATH_BYTES}",
            p.len()
        )));
    }
    if p.chars().any(char::is_control) {
        return Err(Error::Invalid(format!(
            "session path {p:?} contains control characters"
        )));
    }
    Ok(())
}

/// A declared path as a client may type it: surrounding spaces, `./` and a
/// trailing `/` dropped.
pub fn normalize_path(p: &str) -> String {
    let p = p.trim();
    let p = p.strip_prefix("./").unwrap_or(p);
    p.trim_end_matches('/').to_string()
}

/// Whether two session paths overlap: equal, or one is a directory above
/// the other. `src/a` and `src/ab` do not.
pub fn paths_overlap(a: &str, b: &str) -> bool {
    let within = |x: &str, dir: &str| {
        x.len() > dir.len() && x.starts_with(dir) && x.as_bytes()[dir.len()] == b'/'
    };
    a == b || within(a, b) || within(b, a)
}

/// The directories above `p`, nearest last: `a/b/c` gives `a`, `a/b`.
pub fn ancestors(p: &str) -> Vec<&str> {
    p.match_indices('/').map(|(i, _)| &p[..i]).collect()
}

fn parent(p: &str) -> Option<&str> {
    p.rsplit_once('/').map(|(d, _)| d)
}

fn depth(p: &str) -> usize {
    p.bytes().filter(|&b| b == b'/').count()
}

/// At most `max` paths covering every path given: paths under another
/// given path are dropped, paths longer than [`MAX_PATH_BYTES`] become the
/// nearest ancestor that fits, and while too many remain the deepest are
/// replaced by their directories. A directory overlaps everything below
/// it, so the result never misses an overlap the input had; it may report
/// more.
pub fn bound_paths<I, S>(paths: I, max: usize) -> Vec<String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut set: BTreeSet<String> = BTreeSet::new();
    for p in paths {
        let mut p = p.as_ref();
        while p.len() > MAX_PATH_BYTES {
            match parent(p) {
                Some(d) => p = d,
                None => break,
            }
        }
        if !p.is_empty() && p.len() <= MAX_PATH_BYTES {
            set.insert(p.to_string());
        }
    }
    let uncovered = |set: BTreeSet<String>| -> BTreeSet<String> {
        set.iter()
            .filter(|p| !ancestors(p).iter().any(|a| set.contains(*a)))
            .cloned()
            .collect()
    };
    set = uncovered(set);
    while set.len() > max {
        let deepest = set.iter().map(|p| depth(p)).max().unwrap_or(0);
        if deepest == 0 {
            break;
        }
        set = uncovered(
            set.into_iter()
                .map(|p| {
                    if depth(&p) == deepest {
                        parent(&p).unwrap_or(&p).to_string()
                    } else {
                        p
                    }
                })
                .collect(),
        );
    }
    set.into_iter().take(max).collect()
}

fn sorted(v: &[String]) -> Vec<String> {
    let s: BTreeSet<&String> = v.iter().collect();
    s.into_iter().cloned().collect()
}

/// The session `u` makes of `old` (absent: a new one with id `id`). A
/// session that had ended is revived: active again, with no head, landed
/// copy or touched paths, and an inferred one also loses its title. Stores
/// call this under their row lock, so concurrent upserts never lose each
/// other's paths.
pub fn apply_update(old: Option<&Session>, u: &SessionUpdate, id: u64) -> Session {
    let mut s = match old {
        Some(s) => s.clone(),
        None => Session {
            id,
            principal: u.principal.clone(),
            agent_session: u.agent_session.clone(),
            agent_model: None,
            title: String::new(),
            approach: String::new(),
            task_key: None,
            target_ref: None,
            base: None,
            head: None,
            origin: u.origin,
            state: SessionState::Active,
            created: u.at,
            last_seen: u.at,
            ended: None,
            landed_as: None,
            scope: Vec::new(),
            claims: Vec::new(),
            touched: Vec::new(),
        },
    };
    if s.state.is_ended() {
        s.state = SessionState::Active;
        s.ended = None;
        s.landed_as = None;
        s.head = None;
        s.base = None;
        s.touched.clear();
        if s.origin == SessionOrigin::Inferred {
            s.title.clear();
        }
    }
    let declared = u.origin == SessionOrigin::Declared;
    if declared {
        s.origin = SessionOrigin::Declared;
    }
    // A declared value replaces; an inferred one only fills a gap.
    fn set<T: Clone>(slot: &mut T, v: &Option<T>, declared: bool, empty: bool) {
        if let Some(v) = v {
            if declared || empty {
                *slot = v.clone();
            }
        }
    }
    let e = s.agent_model.is_none();
    set(
        &mut s.agent_model,
        &u.agent_model.clone().map(Some),
        declared,
        e,
    );
    let e = s.title.is_empty();
    set(&mut s.title, &u.title, declared, e);
    let e = s.approach.is_empty();
    set(&mut s.approach, &u.approach, declared, e);
    let e = s.task_key.is_none();
    set(&mut s.task_key, &u.task_key.clone().map(Some), declared, e);
    let e = s.target_ref.is_none();
    set(
        &mut s.target_ref,
        &u.target_ref.clone().map(Some),
        declared,
        e,
    );
    let e = s.base.is_none();
    set(&mut s.base, &u.base.map(Some), declared, e);
    if u.head.is_some() {
        s.head = u.head;
    }
    if let Some(p) = &u.scope {
        s.scope = sorted(p);
    }
    if let Some(p) = &u.claims {
        s.claims = sorted(p);
    }
    if !u.touch.is_empty() {
        s.touched = bound_paths(s.touched.iter().chain(&u.touch), MAX_TOUCHED_PATHS);
    }
    s.last_seen = s.last_seen.max(u.at);
    s
}

/// How [`CoordStore::end_session`] ends a session. It applies only to an
/// active session and only when every condition given holds.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionEnd {
    /// [`SessionState::Landed`], [`SessionState::Abandoned`] or
    /// [`SessionState::Expired`].
    pub state: SessionState,
    pub at: i64,
    pub landed_as: Option<ChangesetId>,
    /// Only the session of this principal.
    pub principal: Option<String>,
    /// Only while the session's head is this changeset.
    pub if_head: Option<ChangesetId>,
}

/// Check an end before it is stored: never back to active or idle.
pub fn check_end(end: &SessionEnd) -> Result<()> {
    if !end.state.is_ended() {
        return Err(Error::Invalid(format!(
            "a session cannot be ended as {}",
            end.state.as_str()
        )));
    }
    Ok(())
}

/// Which sessions [`CoordStore::list_sessions`] returns, newest (highest id)
/// first. Every set field must match.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SessionFilter {
    /// Sessions with a scope, claim or touched path equal to this path,
    /// a directory above it, or (for a directory) a path below it.
    pub path: Option<String>,
    pub task_key: Option<String>,
    pub principal: Option<String>,
    pub agent_session: Option<String>,
    /// Any of these stored states; empty means every state. `Idle` is not
    /// a stored state: ask for `Active` and look at `last_seen`.
    pub states: Vec<SessionState>,
    /// Only ids below this, for paging.
    pub before: Option<u64>,
    /// At most this many; 0 means none.
    pub limit: usize,
}

impl SessionFilter {
    /// Whether `s` passes, for stores that filter in memory.
    pub fn matches(&self, s: &Session) -> bool {
        self.path
            .as_ref()
            .is_none_or(|x| s.paths().any(|(_, p)| paths_overlap(p, x)))
            && self
                .task_key
                .as_ref()
                .is_none_or(|k| s.task_key.as_ref() == Some(k))
            && self.principal.as_ref().is_none_or(|p| &s.principal == p)
            && self
                .agent_session
                .as_ref()
                .is_none_or(|k| &s.agent_session == k)
            && (self.states.is_empty() || self.states.contains(&s.state))
            && self.before.is_none_or(|b| s.id < b)
    }
}

/// A heartbeat: the session of (principal, agent_session) was heard from
/// at `at`. Beats never create or revive a session.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Beat {
    pub principal: String,
    pub agent_session: String,
    pub at: i64,
}

/// What one [`CoordStore::sweep_sessions`] call did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SweepReport {
    /// Active sessions ended as expired.
    pub expired: u64,
    /// Path rows of long-ended sessions deleted.
    pub paths_deleted: u64,
    /// Long-ended sessions deleted.
    pub sessions_deleted: u64,
}

impl SweepReport {
    /// Whether the sweep hit its limit somewhere and may have more to do.
    pub fn full(&self, limit: usize) -> bool {
        let l = limit as u64;
        self.expired >= l || self.paths_deleted >= l || self.sessions_deleted >= l
    }
}

/// Storage for coordination state (ADR 0095). Every mutation is one
/// conditional statement or one transaction holding the row's lock; nothing
/// reads a row and writes it back unguarded. Each store keeps the default
/// repository's rows (`repo_id` 1, ADR 0054).
#[async_trait]
pub trait CoordStore: Send + Sync {
    /// Create the session of (`u.principal`, `u.agent_session`) or update
    /// it ([`apply_update`]), in one transaction. Answers the session and
    /// whether it was created or revived. Creating or reviving one when the
    /// principal has `u.max_active` active is `Error::Invalid` with
    /// [`TOO_MANY_SESSIONS_MARKER`].
    async fn upsert_session(&self, u: &SessionUpdate) -> Result<(Session, bool)>;

    async fn get_session(&self, id: u64) -> Result<Option<Session>>;

    async fn list_sessions(&self, filter: &SessionFilter) -> Result<Vec<Session>>;

    /// End an active session if `end`'s conditions hold, as one conditional
    /// update. Answers whether it did.
    async fn end_session(&self, id: u64, end: &SessionEnd) -> Result<bool>;

    /// Record beats of active sessions, raising `last_seen` (never lowering
    /// it). Beats of unknown or ended sessions are ignored. Answers how
    /// many sessions were updated.
    async fn beat_sessions(&self, beats: &[Beat]) -> Result<u64>;

    /// One bounded step of the expiry and retention sweep at `now`: end as
    /// expired up to `limit` active sessions not heard from for
    /// [`EXPIRE_AFTER_SECS`], delete up to `limit` path rows of sessions
    /// ended [`PATHS_RETENTION_SECS`] ago, and up to `limit` sessions ended
    /// [`SESSION_RETENTION_SECS`] ago. Each part commits on its own.
    async fn sweep_sessions(&self, now: i64, limit: usize) -> Result<SweepReport>;

    /// How many sessions are in stored `state`, counting at most `cap`, so
    /// the answer costs a bounded index scan.
    async fn count_sessions(&self, state: SessionState, cap: usize) -> Result<u64>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stored_names_are_stable() {
        let names: Vec<_> = SessionState::ALL.iter().map(|s| s.as_str()).collect();
        assert_eq!(names, ["active", "idle", "landed", "abandoned", "expired"]);
        for s in SessionState::ALL {
            assert_eq!(SessionState::parse(s.as_str()).unwrap(), s);
            assert_eq!(
                serde_json::to_value(s).unwrap(),
                serde_json::json!(s.as_str())
            );
        }
        for k in PathKind::ALL {
            assert_eq!(PathKind::parse(k.as_str()).unwrap(), k);
        }
        let kinds: Vec<_> = PathKind::ALL.iter().map(|k| k.as_str()).collect();
        assert_eq!(kinds, ["scope", "claim", "touched"]);
        for o in [SessionOrigin::Declared, SessionOrigin::Inferred] {
            assert_eq!(SessionOrigin::parse(o.as_str()).unwrap(), o);
        }
        assert!(SessionState::parse("open").is_err());
    }

    #[test]
    fn overlap_is_by_whole_components() {
        assert!(paths_overlap("src/a", "src/a"));
        assert!(paths_overlap("src", "src/a/b.rs"));
        assert!(paths_overlap("src/a/b.rs", "src"));
        assert!(!paths_overlap("src/a", "src/ab"));
        assert!(!paths_overlap("src/ab", "src/a"));
        assert!(!paths_overlap("docs", "src"));
        assert_eq!(ancestors("a/b/c"), ["a", "a/b"]);
        assert!(ancestors("a").is_empty());
    }

    #[test]
    fn bounding_folds_the_deepest_paths_into_directories() {
        let many: Vec<String> = (0..10).map(|i| format!("src/x/f{i}.rs")).collect();
        assert_eq!(bound_paths(&many, 20).len(), 10);
        let b = bound_paths(many.iter().chain(&["docs/a.md".to_string()]), 3);
        assert_eq!(b, ["docs/a.md", "src/x"]);
        // A path under a kept directory is dropped.
        assert_eq!(
            bound_paths(["src", "src/a.rs", "srcx"], 10),
            ["src", "srcx"]
        );
        // Nothing overlapped before is missed after.
        let input: Vec<String> = (0..50).map(|i| format!("d{}/e{i}/f.rs", i % 7)).collect();
        let out = bound_paths(&input, 8);
        assert!(out.len() <= 8);
        for p in &input {
            assert!(
                out.iter().any(|o| paths_overlap(o, p)),
                "{p} lost in {out:?}"
            );
        }
        let long = format!("a/{}", "x".repeat(MAX_PATH_BYTES + 5));
        assert_eq!(bound_paths([long], 4), ["a"]);
    }

    fn update(at: i64) -> SessionUpdate {
        SessionUpdate::new("ada", "s-1", SessionOrigin::Inferred, at)
    }

    #[test]
    fn inference_fills_gaps_and_declarations_replace() {
        let mut u = update(10);
        u.title = Some("from the commit".into());
        u.touch = vec!["src/a.rs".into()];
        let s = apply_update(None, &u, 7);
        assert_eq!(
            (s.id, s.origin, s.created),
            (7, SessionOrigin::Inferred, 10)
        );
        assert_eq!(s.title, "from the commit");
        let mut d = SessionUpdate::new("ada", "s-1", SessionOrigin::Declared, 20);
        d.title = Some("declared".into());
        d.scope = Some(vec!["src".into(), "docs".into(), "src".into()]);
        let s = apply_update(Some(&s), &d, 0);
        assert_eq!(
            (s.id, s.origin, s.title.as_str()),
            (7, SessionOrigin::Declared, "declared")
        );
        assert_eq!(s.scope, ["docs", "src"]);
        let mut i = update(15);
        i.title = Some("later commit".into());
        i.touch = vec!["src/b.rs".into()];
        let s = apply_update(Some(&s), &i, 0);
        assert_eq!(s.title, "declared", "inference never replaces");
        assert_eq!(s.origin, SessionOrigin::Declared);
        assert_eq!(s.touched, ["src/a.rs", "src/b.rs"]);
        assert_eq!(s.last_seen, 20, "never lowered");
    }

    #[test]
    fn an_ended_session_is_revived_without_its_old_work() {
        let mut u = update(10);
        u.title = Some("t".into());
        u.touch = vec!["a".into()];
        u.head = Some(ChangesetId(crate::Hash::of_blob(b"h")));
        let mut s = apply_update(None, &u, 1);
        s.state = SessionState::Landed;
        s.ended = Some(11);
        let s = apply_update(Some(&s), &update(12), 0);
        assert_eq!(s.state, SessionState::Active);
        assert!(s.touched.is_empty() && s.head.is_none() && s.ended.is_none());
        assert!(s.title.is_empty(), "an inferred title went with the work");
    }

    #[test]
    fn idle_and_expired_are_shown_by_age() {
        let s = apply_update(None, &update(1000), 1);
        assert_eq!(s.state_at(1000), SessionState::Active);
        assert_eq!(s.state_at(1000 + IDLE_AFTER_SECS), SessionState::Idle);
        assert_eq!(s.state_at(1000 + EXPIRE_AFTER_SECS), SessionState::Expired);
    }

    #[test]
    fn updates_are_checked() {
        let ok = {
            let mut u = SessionUpdate::new("ada", "sess-1", SessionOrigin::Declared, 1);
            u.title = Some("Refactor auth".into());
            u.approach = Some("split the module\n\tthen move tests".into());
            u.task_key = Some("L-12".into());
            u.scope = Some(vec!["src/auth".into()]);
            u.target_ref = Some("main".into());
            u
        };
        ok.check().unwrap();
        type Change = Box<dyn Fn(&mut SessionUpdate)>;
        let bad: Vec<Change> = vec![
            Box::new(|u| u.agent_session = String::new()),
            Box::new(|u| u.agent_session = "has space".into()),
            Box::new(|u| u.agent_session = "x".repeat(MAX_KEY_BYTES + 1)),
            Box::new(|u| u.title = Some("two\nlines".into())),
            Box::new(|u| u.title = Some("x".repeat(MAX_TITLE_BYTES + 1))),
            Box::new(|u| u.approach = Some("bell\u{7}".into())),
            Box::new(|u| u.task_key = Some("a b".into())),
            Box::new(|u| u.scope = Some(vec!["/abs".into()])),
            Box::new(|u| u.scope = Some(vec!["a/../b".into()])),
            Box::new(|u| u.claims = Some(vec![String::new()])),
            Box::new(|u| u.scope = Some((0..=MAX_SCOPE_PATHS).map(|i| format!("p{i}")).collect())),
            Box::new(|u| u.target_ref = Some("bad ref".into())),
        ];
        for (i, f) in bad.iter().enumerate() {
            let mut u = ok.clone();
            f(&mut u);
            assert!(u.check().is_err(), "case {i}: {u:?}");
        }
        assert_eq!(normalize_path(" ./src/auth/ "), "src/auth");
    }
}
