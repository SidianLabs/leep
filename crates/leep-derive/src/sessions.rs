//! Sessions (ADR 0095): who is working on what, before it lands.
//!
//! Everything here is advisory. Nothing a land decides reads a session, and
//! every write here happens after the answer it rides on is decided, so a
//! failure of the coordination store costs a stale picture, never a wrong
//! land. The server calls [`Repo::note_created`] after storing a changeset
//! and [`Repo::note_landed`] after a land answered `landed`, each in a task
//! of its own; [`Repo::heartbeat`] only touches memory, and
//! [`Repo::flush_beats`] writes what it gathered in one batch.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use leep_core::api::{
    DeclareSessionRequest, DeclareSessionResponse, SessionEntry, SessionText, SessionsResponse,
};
use leep_core::changeset::{EXTRA_AGENT_MODEL, EXTRA_AGENT_SESSION};
use leep_core::coord::{
    bound_paths, check_key, normalize_path, Beat, CoordStore, Session, SessionEnd, SessionFilter,
    SessionOrigin, SessionState, SessionUpdate, SweepReport, EXPIRE_AFTER_SECS, IDLE_AFTER_SECS,
    MAX_KEY_BYTES, MAX_TITLE_BYTES, MAX_TOUCHED_PATHS, TOO_MANY_SESSIONS_MARKER,
};
use leep_core::store::unix_now;
use leep_core::{Changeset, ChangesetId, Error, Result};

use crate::Repo;

/// The principal of every request when the server has no tokens file.
pub const ANONYMOUS: &str = "anonymous";

/// A session's beat is written at most this often, however often it is
/// heard from: the bound on heartbeat writes per session.
pub const BEAT_WRITE_EVERY_SECS: i64 = 60;

/// Most sessions whose beats are held between flushes. Past it new keys
/// are not held (counted in `leep_session_beats_dropped_total`); their
/// sessions only look idle sooner.
pub const MAX_HELD_BEATS: usize = 100_000;

/// Rows one sweep step touches per part.
pub const SWEEP_STEP: usize = 1000;

/// Default and largest page of a listing.
pub const DEFAULT_LIST_LIMIT: usize = 50;
pub const MAX_LIST_LIMIT: usize = 500;

#[derive(Clone, Copy)]
struct Held {
    /// Newest time heard from.
    seen: i64,
    /// `seen` as of the last write; 0 before the first.
    written: i64,
}

/// Heartbeats gathered in memory, by (principal, agent session), so a
/// request carrying `Leep-Session` costs no database write.
#[derive(Default)]
pub struct BeatBuffer {
    held: Mutex<HashMap<(String, String), Held>>,
}

impl BeatBuffer {
    /// Note that the session was heard from at `now`. False when the
    /// buffer is full and the key is new.
    pub fn note(&self, principal: &str, key: &str, now: i64) -> bool {
        let mut held = self.held.lock().unwrap();
        if let Some(h) = held.get_mut(&(principal.to_string(), key.to_string())) {
            h.seen = h.seen.max(now);
            return true;
        }
        if held.len() >= MAX_HELD_BEATS {
            return false;
        }
        held.insert(
            (principal.to_string(), key.to_string()),
            Held {
                seen: now,
                written: 0,
            },
        );
        true
    }

    /// The beats to write at `now`: each session's newest, at most once per
    /// [`BEAT_WRITE_EVERY_SECS`], marked written. Keys not heard from for
    /// [`IDLE_AFTER_SECS`] are forgotten.
    pub fn due(&self, now: i64) -> Vec<Beat> {
        let mut held = self.held.lock().unwrap();
        held.retain(|_, h| now - h.seen < IDLE_AFTER_SECS);
        let mut out = Vec::new();
        for ((principal, key), h) in held.iter_mut() {
            if h.seen > h.written && h.seen - h.written >= BEAT_WRITE_EVERY_SECS {
                out.push(Beat {
                    principal: principal.clone(),
                    agent_session: key.clone(),
                    at: h.seen,
                });
                h.written = h.seen;
            }
        }
        out
    }

    pub fn len(&self) -> usize {
        self.held.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// What `GET /v1/sessions` asks for.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SessionQuery {
    pub path: Option<String>,
    pub task_key: Option<String>,
    pub principal: Option<String>,
    pub session: Option<String>,
    /// One state; `None` with `all` false means live (active and idle).
    pub state: Option<SessionState>,
    pub all: bool,
    pub before: Option<u64>,
    pub limit: Option<usize>,
}

/// The wire form of `s` as seen at `now`.
pub fn entry(s: Session, now: i64) -> SessionEntry {
    let state = s.state_at(now);
    SessionEntry {
        id: s.id,
        principal: s.principal,
        session: s.agent_session,
        agent_model: s.agent_model,
        untrusted: SessionText {
            title: s.title,
            approach: s.approach,
        },
        task_key: s.task_key,
        target_ref: s.target_ref,
        base: s.base,
        head: s.head,
        origin: s.origin.as_str().to_string(),
        state: state.as_str().to_string(),
        created: s.created,
        last_seen: s.last_seen,
        expires: (!s.state.is_ended()).then_some(s.last_seen + EXPIRE_AFTER_SECS),
        ended: s.ended,
        landed_as: s.landed_as,
        scope: s.scope,
        claims: s.claims,
        touched: s.touched,
    }
}

/// The first line of a commit message as a session title: control
/// characters dropped, cut to [`MAX_TITLE_BYTES`] on a character boundary.
pub fn title_of(message: &str) -> String {
    let line: String = message
        .lines()
        .next()
        .unwrap_or_default()
        .chars()
        .filter(|c| !c.is_control())
        .collect();
    let line = line.trim();
    let mut end = line.len().min(MAX_TITLE_BYTES);
    while !line.is_char_boundary(end) {
        end -= 1;
    }
    line[..end].to_string()
}

fn too_many(e: &Error) -> bool {
    matches!(e, Error::Invalid(m) if m.contains(TOO_MANY_SESSIONS_MARKER))
}

impl Repo {
    /// Keep sessions in `coord`, or none. [`Repo::new`] takes the meta
    /// store's own ([`leep_core::MetaStore::coord_store`]).
    pub fn with_coord(mut self, coord: Option<Arc<dyn CoordStore>>) -> Self {
        self.coord = coord;
        self
    }

    pub fn coord(&self) -> Option<&Arc<dyn CoordStore>> {
        self.coord.as_ref()
    }

    fn coord_on(&self) -> Result<&Arc<dyn CoordStore>> {
        self.coord
            .as_ref()
            .ok_or_else(|| Error::NotFound("sessions: this server keeps none".into()))
    }

    /// Declare or update `principal`'s session of `req.session`.
    pub async fn declare_session(
        &self,
        principal: &str,
        req: DeclareSessionRequest,
    ) -> Result<DeclareSessionResponse> {
        let coord = self.coord_on()?;
        let now = unix_now();
        let mut u = SessionUpdate::new(principal, &req.session, SessionOrigin::Declared, now);
        u.title = req.title;
        u.approach = req.approach;
        u.task_key = req.task_key;
        u.target_ref = req.target_ref;
        u.agent_model = req.agent_model;
        u.base = req.base;
        let paths = |v: Option<Vec<String>>| {
            v.map(|v| {
                let mut v: Vec<String> = v.iter().map(|p| normalize_path(p)).collect();
                v.sort();
                v.dedup();
                v
            })
        };
        u.scope = paths(req.scope);
        u.claims = paths(req.claims);
        u.check()?;
        if let Some(b) = u.base {
            // An id nobody can read is a typo; say so now.
            self.get_changeset(b).await?;
        }
        let (s, created) = coord.upsert_session(&u).await?;
        if created {
            metrics::counter!("leep_sessions_started_total", "origin" => "declared").increment(1);
        }
        Ok(DeclareSessionResponse {
            session: entry(s, now),
            created,
        })
    }

    /// The sessions `q` asks for, newest first.
    pub async fn sessions(&self, q: &SessionQuery) -> Result<SessionsResponse> {
        let coord = self.coord_on()?;
        let limit = q
            .limit
            .unwrap_or(DEFAULT_LIST_LIMIT)
            .clamp(1, MAX_LIST_LIMIT);
        let path = match &q.path {
            Some(p) => {
                let p = normalize_path(p);
                if p.is_empty() {
                    None
                } else {
                    leep_core::RepoPath::new(p.clone())?;
                    Some(p)
                }
            }
            None => None,
        };
        // `idle` and a not yet swept `expired` are views of stored `active`.
        let stored = match (q.all, q.state) {
            (true, _) => vec![],
            (false, None | Some(SessionState::Active | SessionState::Idle)) => {
                vec![SessionState::Active]
            }
            (false, Some(SessionState::Expired)) => {
                vec![SessionState::Expired, SessionState::Active]
            }
            (false, Some(s)) => vec![s],
        };
        let filter = SessionFilter {
            path,
            task_key: q.task_key.clone(),
            principal: q.principal.clone(),
            agent_session: q.session.clone(),
            states: stored,
            before: q.before,
            limit,
        };
        let rows = coord.list_sessions(&filter).await?;
        let next = (rows.len() == limit)
            .then(|| rows.last().map(|s| s.id))
            .flatten();
        let now = unix_now();
        let shown = |s: SessionState| match (q.all, q.state) {
            (true, _) => true,
            (false, None | Some(SessionState::Active)) => !s.is_ended(),
            (false, Some(want)) => s == want,
        };
        let sessions = rows
            .into_iter()
            .map(|s| entry(s, now))
            .filter(|e| SessionState::parse(&e.state).is_ok_and(shown))
            .collect();
        Ok(SessionsResponse { sessions, next })
    }

    /// One session; 404 if unknown.
    pub async fn session(&self, id: u64) -> Result<SessionEntry> {
        let coord = self.coord_on()?;
        let s = coord
            .get_session(id)
            .await?
            .ok_or_else(|| Error::NotFound(format!("session {id}")))?;
        Ok(entry(s, unix_now()))
    }

    /// End `principal`'s session `id` as abandoned. A session of another
    /// principal is refused with `Error::Policy` (the server answers 403);
    /// an ended one is answered as it is.
    pub async fn abandon_session(&self, id: u64, principal: &str) -> Result<SessionEntry> {
        let coord = self.coord_on()?;
        let s = coord
            .get_session(id)
            .await?
            .ok_or_else(|| Error::NotFound(format!("session {id}")))?;
        if s.principal != principal {
            return Err(Error::Policy(format!(
                "session {id} is {}'s; only its principal may end it",
                s.principal
            )));
        }
        let end = SessionEnd {
            state: SessionState::Abandoned,
            at: unix_now(),
            landed_as: None,
            principal: Some(principal.to_string()),
            if_head: None,
        };
        coord.end_session(id, &end).await?;
        self.session(id).await
    }

    /// Infer `principal`'s session from a changeset it stored that carries
    /// `agent.session` (design D.2): create or revive it, record the head,
    /// and add the paths the changeset changed. A merge adds no paths: its
    /// changes against the first parent are mostly the other side's work.
    /// Answers whether a session was written; false when the changeset
    /// carries no usable key, when sessions are off, or when the principal
    /// is at its active session limit.
    pub async fn note_created(
        &self,
        cs: &Changeset,
        id: ChangesetId,
        principal: &str,
    ) -> Result<bool> {
        let Some(coord) = self.coord.as_ref() else {
            return Ok(false);
        };
        let Some(key) = cs.extra.get(EXTRA_AGENT_SESSION) else {
            return Ok(false);
        };
        if check_key("agent session", key).is_err() || check_key("principal", principal).is_err() {
            return Ok(false);
        }
        let mut u = SessionUpdate::new(principal, key, SessionOrigin::Inferred, unix_now());
        u.title = Some(title_of(&cs.message)).filter(|t| !t.is_empty());
        u.base = cs.parents.first().copied();
        u.head = Some(id);
        u.agent_model = cs
            .extra
            .get(EXTRA_AGENT_MODEL)
            .filter(|m| m.len() <= MAX_KEY_BYTES && !m.chars().any(char::is_control))
            .cloned();
        if cs.parents.len() < 2 {
            u.touch = bound_paths(cs.changes.keys().map(|p| p.as_str()), MAX_TOUCHED_PATHS);
        }
        match coord.upsert_session(&u).await {
            Ok((_, created)) => {
                if created {
                    metrics::counter!("leep_sessions_started_total", "origin" => "inferred")
                        .increment(1);
                }
                Ok(true)
            }
            Err(e) if too_many(&e) => {
                metrics::counter!("leep_sessions_refused_total").increment(1);
                Ok(false)
            }
            Err(e) => Err(e),
        }
    }

    /// After a land of `submitted` answered `landed` with the ref at
    /// `landed`, end its session as landed: the session of the
    /// changeset's creator (else `actor`) and `agent.session`, if its head
    /// is still `submitted`. A session that made more changesets since
    /// stays active. Answers whether one ended.
    pub async fn note_landed(
        &self,
        submitted: ChangesetId,
        landed: ChangesetId,
        actor: Option<&str>,
    ) -> Result<bool> {
        let Some(coord) = self.coord.as_ref() else {
            return Ok(false);
        };
        let cs = self.get_changeset(submitted).await?;
        let Some(key) = cs.extra.get(EXTRA_AGENT_SESSION) else {
            return Ok(false);
        };
        let principal = match self.created_by(submitted).await? {
            Some(c) => c.principal,
            None => actor.unwrap_or(ANONYMOUS).to_string(),
        };
        let filter = SessionFilter {
            principal: Some(principal.clone()),
            agent_session: Some(key.clone()),
            states: vec![SessionState::Active],
            limit: 1,
            ..Default::default()
        };
        let Some(s) = coord.list_sessions(&filter).await?.pop() else {
            return Ok(false);
        };
        let end = SessionEnd {
            state: SessionState::Landed,
            at: unix_now(),
            landed_as: Some(landed),
            principal: Some(principal),
            if_head: Some(submitted),
        };
        let ended = coord.end_session(s.id, &end).await?;
        if ended {
            metrics::counter!("leep_sessions_ended_total", "state" => "landed").increment(1);
        }
        Ok(ended)
    }

    /// `principal`'s session of `key` was heard from now. Memory only; a
    /// bad key is ignored.
    pub fn heartbeat(&self, principal: &str, key: &str) {
        if self.coord.is_none() || check_key("agent session", key).is_err() {
            return;
        }
        if !self.beats.note(principal, key, unix_now()) {
            metrics::counter!("leep_session_beats_dropped_total").increment(1);
        }
    }

    /// Write the beats due now in one batch. Answers how many sessions
    /// were updated.
    pub async fn flush_beats(&self) -> Result<u64> {
        self.flush_beats_at(unix_now()).await
    }

    pub(crate) async fn flush_beats_at(&self, now: i64) -> Result<u64> {
        let Some(coord) = self.coord.as_ref() else {
            return Ok(0);
        };
        let due = self.beats.due(now);
        if due.is_empty() {
            return Ok(0);
        }
        coord.beat_sessions(&due).await
    }

    /// Expire silent sessions and forget long-ended ones, in bounded steps,
    /// at most `steps` of them.
    pub async fn sweep_sessions(&self, steps: usize) -> Result<SweepReport> {
        let Some(coord) = self.coord.as_ref() else {
            return Ok(SweepReport::default());
        };
        let now = unix_now();
        let mut total = SweepReport::default();
        for _ in 0..steps {
            let r = coord.sweep_sessions(now, SWEEP_STEP).await?;
            total.expired += r.expired;
            total.paths_deleted += r.paths_deleted;
            total.sessions_deleted += r.sessions_deleted;
            if !r.full(SWEEP_STEP) {
                break;
            }
        }
        if total.expired > 0 {
            metrics::counter!("leep_sessions_ended_total", "state" => "expired")
                .increment(total.expired);
        }
        Ok(total)
    }

    /// Active sessions, for the `leep_sessions_active` gauge: at most
    /// `cap`, so the count costs a bounded read.
    pub async fn count_active_sessions(&self, cap: usize) -> Result<usize> {
        let Some(coord) = self.coord.as_ref() else {
            return Ok(0);
        };
        let f = SessionFilter {
            states: vec![SessionState::Active],
            limit: cap,
            ..Default::default()
        };
        Ok(coord.list_sessions(&f).await?.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use leep_core::memory::{MemoryBlobStore, MemoryMetaStore};

    fn repo() -> Repo {
        Repo::new(
            Arc::new(MemoryBlobStore::new()),
            Arc::new(MemoryMetaStore::new()),
        )
    }

    #[test]
    fn beats_are_written_at_most_once_a_minute() {
        let b = BeatBuffer::default();
        assert!(b.note("ada", "s", 100));
        assert!(b.note("ada", "s", 130));
        let due = b.due(130);
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].at, 130);
        assert!(b.note("ada", "s", 150));
        assert!(b.due(150).is_empty(), "written 20 s ago");
        assert!(b.note("ada", "s", 191));
        assert_eq!(b.due(191)[0].at, 191);
        // Forgotten once idle.
        assert!(b.due(191 + IDLE_AFTER_SECS).is_empty());
        assert!(b.is_empty());
    }

    #[test]
    fn titles_come_from_the_first_line() {
        assert_eq!(title_of("fix: a thing\n\nbody"), "fix: a thing");
        assert_eq!(title_of("tab\there"), "tabhere");
        assert_eq!(title_of(""), "");
        let long = "é".repeat(MAX_TITLE_BYTES);
        assert!(title_of(&long).len() <= MAX_TITLE_BYTES);
    }

    #[tokio::test]
    async fn without_a_coord_store_nothing_is_kept_and_nothing_fails() {
        let r = repo().with_coord(None);
        let cs = Changeset {
            parents: vec![],
            author: "a".into(),
            timestamp: 1,
            message: "m".into(),
            changes: Default::default(),
            extra: [(EXTRA_AGENT_SESSION.to_string(), "s".to_string())].into(),
        };
        assert!(!r.note_created(&cs, cs.id(), "ada").await.unwrap());
        r.heartbeat("ada", "s");
        assert_eq!(r.flush_beats().await.unwrap(), 0);
        assert!(matches!(
            r.sessions(&SessionQuery::default()).await,
            Err(Error::NotFound(_))
        ));
    }

    #[tokio::test]
    async fn heartbeats_reach_the_store_in_batches() {
        let r = repo();
        let req = DeclareSessionRequest {
            session: "s-1".into(),
            title: Some("t".into()),
            ..Default::default()
        };
        let made = r.declare_session("ada", req).await.unwrap();
        assert!(made.created);
        let now = unix_now();
        r.heartbeat("ada", "s-1");
        r.heartbeat("ada", "not a key");
        r.heartbeat("bo", "s-1");
        assert_eq!(r.beats.len(), 2);
        // `bo` has no such session: its beat changes nothing.
        assert_eq!(r.flush_beats_at(now + 1).await.unwrap(), 1);
        assert_eq!(r.flush_beats_at(now + 2).await.unwrap(), 0, "debounced");
    }
}
