//! The in-memory [`CoordStore`] and the contract every backend must pass
//! (re-exported from [`crate::memory`]).

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;

use async_trait::async_trait;

use crate::coord::{
    apply_update, check_end, check_supersede, closed_expiry, notes_per_hour, reports_per_hour,
    too_many_notes, Beat, CoordStore, Event, EventBounds, NearQuery, NearRow, NewEvent, NewNote,
    Note, NoteChange, NoteFilter, NoteState, NoteSweep, NoteTrust, PathKind, Session, SessionEnd,
    SessionFilter, SessionOrigin, SessionState, SessionUpdate, SweepReport,
    CLOSED_NOTE_RETENTION_SECS, EXPIRE_AFTER_SECS, MAX_TOUCHED_PATHS, NOTE_RATE_WINDOW_SECS,
    PATHS_RETENTION_SECS, SESSION_RETENTION_SECS, TOO_MANY_SESSIONS_MARKER,
};
use crate::{ChangesetId, Error, Hash, Result};

#[derive(Default)]
struct Rows {
    next: u64,
    sessions: BTreeMap<u64, Session>,
    next_note: u64,
    notes: BTreeMap<u64, Note>,
    /// The last event `seq` handed out.
    last_event: u64,
    events: BTreeMap<u64, Event>,
}

/// Sessions in memory, for tests and `mem://` servers. One lock over every
/// row, so each call is atomic.
#[derive(Default)]
pub struct MemoryCoordStore {
    rows: Mutex<Rows>,
}

impl MemoryCoordStore {
    pub fn new() -> Self {
        Self::default()
    }
}

/// `n` after `change` at `at`, if it applies.
pub(crate) fn changed_note(n: &Note, change: &NoteChange, at: i64) -> Option<Note> {
    if n.state != NoteState::Open {
        return None;
    }
    let mut n = n.clone();
    let close = |n: &mut Note, state| {
        n.state = state;
        n.expires = closed_expiry(n.expires, at);
    };
    match change {
        NoteChange::Retract { by } if *by == n.principal => close(&mut n, NoteState::Retracted),
        NoteChange::Report { by, .. } if n.trust == NoteTrust::Agent && n.endorsed_by.is_none() => {
            close(&mut n, NoteState::Reported);
            n.reported_by = Some(by.clone());
            n.reported_at = Some(at);
        }
        NoteChange::Revoke => close(&mut n, NoteState::Retracted),
        NoteChange::Endorse { by } if n.endorsed_by.is_none() => {
            n.endorsed_by = Some(by.clone());
            n.endorsed_at = Some(at);
            n.expires = None;
        }
        _ => return None,
    }
    Some(n)
}

/// The refusal for a principal at its active session limit.
pub(crate) fn too_many(principal: &str, max: usize) -> Error {
    Error::Invalid(format!(
        "{principal} has {max} {TOO_MANY_SESSIONS_MARKER} ({max}); end some (`leep session end`) \
         or let them expire"
    ))
}

#[async_trait]
impl CoordStore for MemoryCoordStore {
    async fn upsert_session(&self, u: &SessionUpdate) -> Result<(Session, bool)> {
        u.check()?;
        let mut rows = self.rows.lock().unwrap();
        let old = rows
            .sessions
            .values()
            .find(|s| s.principal == u.principal && s.agent_session == u.agent_session)
            .cloned();
        let starting = old.as_ref().is_none_or(|s| s.state.is_ended());
        if starting {
            let active = rows
                .sessions
                .values()
                .filter(|s| s.principal == u.principal && s.state == SessionState::Active)
                .count();
            if active >= u.max_active {
                return Err(too_many(&u.principal, u.max_active));
            }
        }
        let id = match &old {
            Some(s) => s.id,
            None => {
                rows.next += 1;
                rows.next
            }
        };
        let s = apply_update(old.as_ref(), u, id);
        rows.sessions.insert(id, s.clone());
        Ok((s, starting))
    }

    async fn get_session(&self, id: u64) -> Result<Option<Session>> {
        Ok(self.rows.lock().unwrap().sessions.get(&id).cloned())
    }

    async fn list_sessions(&self, f: &SessionFilter) -> Result<Vec<Session>> {
        Ok(self
            .rows
            .lock()
            .unwrap()
            .sessions
            .values()
            .rev()
            .filter(|s| f.matches(s))
            .take(f.limit)
            .cloned()
            .map(|mut s| {
                if f.skip_paths {
                    s.scope.clear();
                    s.claims.clear();
                    s.touched.clear();
                }
                s
            })
            .collect())
    }

    async fn end_session(&self, id: u64, end: &SessionEnd) -> Result<bool> {
        check_end(end)?;
        let mut rows = self.rows.lock().unwrap();
        let Some(s) = rows.sessions.get_mut(&id) else {
            return Ok(false);
        };
        if s.state != SessionState::Active
            || end.principal.as_ref().is_some_and(|p| p != &s.principal)
            || end.if_head.is_some_and(|h| s.head != Some(h))
        {
            return Ok(false);
        }
        s.state = end.state;
        s.ended = Some(end.at);
        s.landed_as = end.landed_as;
        Ok(true)
    }

    async fn beat_sessions(&self, beats: &[Beat]) -> Result<u64> {
        let mut rows = self.rows.lock().unwrap();
        let mut n = 0;
        for s in rows.sessions.values_mut() {
            if s.state != SessionState::Active {
                continue;
            }
            let newest = beats
                .iter()
                .filter(|b| b.principal == s.principal && b.agent_session == s.agent_session)
                .map(|b| b.at)
                .max();
            if let Some(at) = newest {
                s.last_seen = s.last_seen.max(at);
                n += 1;
            }
        }
        Ok(n)
    }

    async fn sweep_sessions(&self, now: i64, limit: usize) -> Result<SweepReport> {
        let mut rows = self.rows.lock().unwrap();
        let mut r = SweepReport::default();
        for s in rows.sessions.values_mut() {
            if r.expired as usize >= limit {
                break;
            }
            if s.state == SessionState::Active && s.last_seen < now - EXPIRE_AFTER_SECS {
                s.state = SessionState::Expired;
                s.ended = Some(now);
                r.expired += 1;
            }
        }
        for s in rows.sessions.values_mut() {
            let old = s.ended.is_some_and(|e| e < now - PATHS_RETENTION_SECS);
            if !old {
                continue;
            }
            for v in [&mut s.scope, &mut s.claims, &mut s.touched] {
                let room = limit.saturating_sub(r.paths_deleted as usize);
                let n = v.len().min(room);
                v.drain(..n);
                r.paths_deleted += n as u64;
            }
        }
        let gone: Vec<u64> = rows
            .sessions
            .values()
            .filter(|s| s.ended.is_some_and(|e| e < now - SESSION_RETENTION_SECS))
            .map(|s| s.id)
            .take(limit)
            .collect();
        for id in gone {
            rows.sessions.remove(&id);
            r.sessions_deleted += 1;
        }
        Ok(r)
    }

    async fn count_sessions(&self, state: SessionState, cap: usize) -> Result<u64> {
        let rows = self.rows.lock().unwrap();
        Ok(rows
            .sessions
            .values()
            .filter(|s| s.state == state)
            .take(cap)
            .count() as u64)
    }

    async fn near_paths(&self, q: &NearQuery) -> Result<Vec<NearRow>> {
        if q.limit == 0 {
            return Ok(Vec::new());
        }
        let (exact, ranges) = q.probes();
        let rows = self.rows.lock().unwrap();
        let mut all: Vec<(&str, u64, PathKind)> = Vec::new();
        for s in rows.sessions.values() {
            if s.state != SessionState::Active || q.exclude == Some(s.id) {
                continue;
            }
            for (kind, path) in s.paths() {
                all.push((path, s.id, kind));
            }
        }
        all.sort();
        let mut picked: BTreeSet<(&str, u64, PathKind)> = BTreeSet::new();
        for e in &exact {
            let mut on: Vec<_> = all.iter().filter(|(p, _, _)| *p == e.as_str()).collect();
            on.sort_by_key(|(_, id, k)| (*k != PathKind::Claim, *id));
            picked.extend(on.into_iter().take(q.per_range).copied());
        }
        for (lo, hi) in &ranges {
            picked.extend(
                all.iter()
                    .filter(|(p, _, _)| *p > lo.as_str() && *p < hi.as_str())
                    .take(q.per_range)
                    .copied(),
            );
        }
        let mut out: Vec<NearRow> = picked
            .into_iter()
            .map(|(path, id, kind)| {
                let mut session = rows.sessions[&id].clone();
                session.scope.clear();
                session.claims.clear();
                session.touched.clear();
                NearRow {
                    session,
                    kind,
                    path: path.to_string(),
                }
            })
            .collect();
        NearQuery::order(&exact, &mut out);
        out.truncate(q.limit);
        Ok(out)
    }

    async fn add_note(&self, n: &NewNote) -> Result<Note> {
        n.check()?;
        let mut rows = self.rows.lock().unwrap();
        if let Some(old) = n.supersedes {
            let found = rows
                .notes
                .get(&old)
                .map(|o| (o.principal.as_str(), o.state));
            check_supersede(old, found, &n.principal)?;
        }
        let mine = || rows.notes.values().filter(|o| o.principal == n.principal);
        let open = mine()
            .filter(|o| o.state == NoteState::Open)
            .count()
            .saturating_sub(usize::from(n.supersedes.is_some()));
        if open >= n.max_open {
            return Err(too_many_notes(&n.principal, n.max_open));
        }
        let recent = mine()
            .filter(|o| o.created > n.created - NOTE_RATE_WINDOW_SECS)
            .count();
        if recent >= n.max_per_hour {
            return Err(notes_per_hour(&n.principal, n.max_per_hour));
        }
        let today = mine()
            .filter(|o| o.created > n.created - crate::coord::NOTE_DAY_WINDOW_SECS)
            .count();
        if today >= n.max_per_day {
            return Err(crate::coord::notes_per_day(&n.principal, n.max_per_day));
        }
        rows.next_note += 1;
        let id = rows.next_note;
        let note = Note {
            id,
            kind: n.kind,
            anchor_type: n.anchor_type,
            anchor: n.anchor.clone(),
            at_changeset: n.at_changeset,
            at_blob: n.at_blob,
            lines: n.lines,
            title: n.title.clone(),
            body: n.body.clone(),
            principal: n.principal.clone(),
            agent_model: n.agent_model.clone(),
            session_id: n.session_id,
            trust: n.trust,
            endorsed_by: None,
            endorsed_at: None,
            state: NoteState::Open,
            superseded_by: None,
            reported_by: None,
            reported_at: None,
            created: n.created,
            expires: n.expires,
        };
        if let Some(old) = n.supersedes.and_then(|o| rows.notes.get_mut(&o)) {
            old.state = NoteState::Superseded;
            old.superseded_by = Some(id);
            old.expires = closed_expiry(old.expires, n.created);
        }
        rows.notes.insert(id, note.clone());
        Ok(note)
    }

    async fn get_note(&self, id: u64) -> Result<Option<Note>> {
        Ok(self.rows.lock().unwrap().notes.get(&id).cloned())
    }

    async fn list_notes(&self, f: &NoteFilter) -> Result<Vec<Note>> {
        f.check()?;
        Ok(self
            .rows
            .lock()
            .unwrap()
            .notes
            .values()
            .rev()
            .filter(|n| f.matches(n))
            .take(f.limit)
            .cloned()
            .collect())
    }

    async fn notes_at(&self, anchors: &[String], cap: usize) -> Result<Vec<Note>> {
        crate::coord::check_anchor_probe(anchors)?;
        Ok(self
            .rows
            .lock()
            .unwrap()
            .notes
            .values()
            .rev()
            .filter(|n| {
                n.state == NoteState::Open
                    && n.anchor_type == crate::coord::AnchorType::Path
                    && anchors.contains(&n.anchor)
            })
            .take(cap)
            .cloned()
            .collect())
    }

    async fn change_note(&self, id: u64, change: &NoteChange, at: i64) -> Result<bool> {
        let mut rows = self.rows.lock().unwrap();
        if let NoteChange::Report { by, max_per_hour } = change {
            let hid = rows
                .notes
                .values()
                .filter(|n| {
                    n.reported_by.as_ref() == Some(by)
                        && n.reported_at
                            .is_some_and(|t| t > at - NOTE_RATE_WINDOW_SECS)
                })
                .count();
            if hid >= *max_per_hour {
                return Err(reports_per_hour(by, *max_per_hour));
            }
        }
        let Some(n) = rows.notes.get_mut(&id) else {
            return Ok(false);
        };
        match changed_note(n, change, at) {
            Some(new) => {
                *n = new;
                Ok(true)
            }
            None => Ok(false),
        }
    }

    async fn revoke_notes(&self, principal: &str, at: i64) -> Result<u64> {
        let mut rows = self.rows.lock().unwrap();
        let mut count = 0;
        for n in rows.notes.values_mut() {
            if n.principal == principal {
                if let Some(new) = changed_note(n, &NoteChange::Revoke, at) {
                    *n = new;
                    count += 1;
                }
            }
        }
        Ok(count)
    }

    async fn unreport_notes(&self, id: Option<u64>, reporter: Option<&str>) -> Result<u64> {
        let mut rows = self.rows.lock().unwrap();
        let mut count = 0;
        for n in rows.notes.values_mut() {
            let picked = id.is_some_and(|i| i == n.id)
                || reporter.is_some_and(|r| n.reported_by.as_deref() == Some(r));
            if picked && n.state == NoteState::Reported {
                n.state = NoteState::Open;
                n.reported_by = None;
                n.reported_at = None;
                n.expires = Some(n.created + crate::coord::AGENT_NOTE_TTL_SECS);
                count += 1;
            }
        }
        Ok(count)
    }

    async fn sweep_notes(&self, now: i64, limit: usize) -> Result<NoteSweep> {
        let mut rows = self.rows.lock().unwrap();
        let due = |open: bool| -> Vec<u64> {
            rows.notes
                .values()
                .filter(|n| (n.state == NoteState::Open) == open)
                .filter(|n| n.expires.is_some_and(|e| e < now))
                .map(|n| n.id)
                .take(limit)
                .collect()
        };
        let (open, closed) = (due(true), due(false));
        let r = NoteSweep {
            expired: open.len() as u64,
            closed: closed.len() as u64,
        };
        for id in open.into_iter().chain(closed) {
            rows.notes.remove(&id);
        }
        Ok(r)
    }

    async fn append_events(&self, events: &[NewEvent]) -> Result<u64> {
        let mut rows = self.rows.lock().unwrap();
        for e in events {
            rows.last_event += 1;
            let seq = rows.last_event;
            rows.events.insert(seq, e.stored(seq));
        }
        Ok(if events.is_empty() {
            0
        } else {
            rows.last_event
        })
    }

    async fn events_after(&self, after: u64, limit: usize) -> Result<Vec<Event>> {
        let rows = self.rows.lock().unwrap();
        Ok(rows
            .events
            .range(after.saturating_add(1)..)
            .take(limit)
            .map(|(_, e)| e.clone())
            .collect())
    }

    async fn events_bounds(&self) -> Result<EventBounds> {
        let rows = self.rows.lock().unwrap();
        Ok(EventBounds {
            oldest: rows.events.keys().next().copied(),
            newest: rows.last_event,
        })
    }

    async fn sweep_events(&self, cutoff: i64, limit: usize) -> Result<u64> {
        let mut rows = self.rows.lock().unwrap();
        let due: Vec<u64> = rows
            .events
            .values()
            .take(limit)
            .take_while(|e| e.created < cutoff)
            .map(|e| e.seq)
            .collect();
        for seq in &due {
            rows.events.remove(seq);
        }
        Ok(due.len() as u64)
    }
}

/// Assert the [`CoordStore`] contract. Run it on an empty store: the sweep
/// checks count every row.
pub async fn check_coord_contract(store: &dyn CoordStore) {
    let cs = |s: &str| ChangesetId(Hash::of_blob(s.as_bytes()));
    let t0 = 1_700_000_000;
    let declare = |p: &str, k: &str, at: i64| SessionUpdate::new(p, k, SessionOrigin::Declared, at);
    let infer = |p: &str, k: &str, at: i64| SessionUpdate::new(p, k, SessionOrigin::Inferred, at);

    // Create.
    let mut u = declare("ada", "s-1", t0);
    u.title = Some("Refactor auth".into());
    u.approach = Some("split the module\nthen move tests".into());
    u.task_key = Some("L-12".into());
    u.target_ref = Some("main".into());
    u.agent_model = Some("model-a".into());
    u.base = Some(cs("base"));
    u.scope = Some(vec!["src/auth".into(), "docs".into()]);
    u.claims = Some(vec!["src/auth/token.rs".into()]);
    let (a, created) = store.upsert_session(&u).await.unwrap();
    assert!(created);
    assert_eq!(
        (a.principal.as_str(), a.agent_session.as_str()),
        ("ada", "s-1")
    );
    assert_eq!(
        (a.origin, a.state),
        (SessionOrigin::Declared, SessionState::Active)
    );
    assert_eq!((a.created, a.last_seen, a.ended), (t0, t0, None));
    assert_eq!(a.title, "Refactor auth");
    assert_eq!(a.approach, "split the module\nthen move tests");
    assert_eq!(a.task_key.as_deref(), Some("L-12"));
    assert_eq!(a.target_ref.as_deref(), Some("main"));
    assert_eq!(a.agent_model.as_deref(), Some("model-a"));
    assert_eq!(a.base, Some(cs("base")));
    assert_eq!(a.scope, ["docs", "src/auth"]);
    assert_eq!(a.claims, ["src/auth/token.rs"]);
    assert!(a.touched.is_empty() && a.head.is_none());
    assert_eq!(store.get_session(a.id).await.unwrap(), Some(a.clone()));
    assert_eq!(store.get_session(u64::MAX / 2).await.unwrap(), None);
    // A bad update stores nothing.
    let mut bad = declare("ada", "s-1", t0);
    bad.title = Some("two\nlines".into());
    assert!(store.upsert_session(&bad).await.is_err());

    // Update: same row, given fields replace, touched paths accumulate.
    let mut i = infer("ada", "s-1", t0 + 5);
    i.title = Some("commit subject".into());
    i.head = Some(cs("h1"));
    i.touch = vec!["src/auth/a.rs".into(), "src/auth/b.rs".into()];
    let (a2, created) = store.upsert_session(&i).await.unwrap();
    assert!(!created);
    assert_eq!(a2.id, a.id);
    assert_eq!(a2.title, "Refactor auth", "inference fills gaps only");
    assert_eq!(a2.origin, SessionOrigin::Declared);
    assert_eq!(a2.head, Some(cs("h1")));
    assert_eq!(a2.touched, ["src/auth/a.rs", "src/auth/b.rs"]);
    assert_eq!(a2.last_seen, t0 + 5);
    let mut d = declare("ada", "s-1", t0 + 6);
    d.scope = Some(vec!["src/auth".into()]);
    d.claims = Some(vec![]);
    d.touch = vec!["src/auth/c.rs".into()];
    let (a3, _) = store.upsert_session(&d).await.unwrap();
    assert_eq!(a3.scope, ["src/auth"]);
    assert!(a3.claims.is_empty());
    assert_eq!(a3.touched.len(), 3);
    assert_eq!(a3.title, "Refactor auth", "absent fields keep their value");
    assert_eq!(store.get_session(a.id).await.unwrap(), Some(a3.clone()));
    // Touched paths are bounded.
    let mut many = infer("ada", "s-1", t0 + 7);
    many.touch = (0..MAX_TOUCHED_PATHS + 40)
        .map(|n| format!("gen/d{}/f{n}.rs", n % 3))
        .collect();
    let (a4, _) = store.upsert_session(&many).await.unwrap();
    assert!(
        a4.touched.len() <= MAX_TOUCHED_PATHS,
        "{}",
        a4.touched.len()
    );
    assert!(a4.touched.iter().any(|p| p == "gen/d1"), "{:?}", a4.touched);
    // Folded into its directory, which still overlaps it.
    assert!(a4
        .touched
        .iter()
        .any(|p| crate::coord::paths_overlap(p, "src/auth/a.rs")));

    // Another principal's session of the same key is another row.
    let mut b = infer("bo", "s-1", t0 + 10);
    b.touch = vec!["src/authx/z.rs".into(), "web/app.ts".into()];
    b.head = Some(cs("bh"));
    let (b, created) = store.upsert_session(&b).await.unwrap();
    assert!(created && b.id != a.id && b.id > a.id);
    assert_eq!(b.origin, SessionOrigin::Inferred);
    let mut c = declare("cy", "s-9", t0 + 11);
    c.task_key = Some("L-12".into());
    c.scope = Some(vec!["src".into()]);
    let (c, _) = store.upsert_session(&c).await.unwrap();

    // Listing: newest first, every filter, paging.
    let all = SessionFilter {
        limit: 100,
        ..Default::default()
    };
    let ids = |v: Vec<Session>| v.into_iter().map(|s| s.id).collect::<Vec<_>>();
    let list = |f: SessionFilter| async move { ids(store.list_sessions(&f).await.unwrap()) };
    assert_eq!(list(all.clone()).await, [c.id, b.id, a.id]);
    let at = |p: &str| SessionFilter {
        path: Some(p.into()),
        ..all.clone()
    };
    // The path itself, a directory above a session path, one below.
    assert_eq!(list(at("src/auth/token.rs")).await, [c.id, a.id]);
    assert_eq!(list(at("src/auth")).await, [c.id, a.id]);
    assert_eq!(list(at("src")).await, [c.id, b.id, a.id]);
    assert_eq!(
        list(at("src/authx")).await,
        [c.id, b.id],
        "not a prefix of a name"
    );
    assert_eq!(list(at("web")).await, [b.id]);
    assert_eq!(list(at("gen/d2/f2.rs")).await, [a.id]);
    assert!(list(at("nowhere/x")).await.is_empty());
    let task = SessionFilter {
        task_key: Some("L-12".into()),
        ..all.clone()
    };
    assert_eq!(list(task).await, [c.id, a.id]);
    let by = SessionFilter {
        principal: Some("bo".into()),
        ..all.clone()
    };
    assert_eq!(list(by).await, [b.id]);
    let declared = SessionFilter {
        origin: Some(SessionOrigin::Declared),
        ..all.clone()
    };
    assert_eq!(list(declared).await, [c.id, a.id]);
    let inferred = SessionFilter {
        origin: Some(SessionOrigin::Inferred),
        ..all.clone()
    };
    assert_eq!(list(inferred).await, [b.id]);
    let key = SessionFilter {
        agent_session: Some("s-1".into()),
        principal: Some("ada".into()),
        ..all.clone()
    };
    assert_eq!(
        store.list_sessions(&key).await.unwrap(),
        std::slice::from_ref(&a4)
    );
    let page = SessionFilter {
        limit: 1,
        before: Some(c.id),
        ..all.clone()
    };
    assert_eq!(list(page).await, [b.id]);
    assert!(list(SessionFilter {
        limit: 0,
        ..all.clone()
    })
    .await
    .is_empty());

    // Beats raise last_seen, never lower it, and skip unknown keys.
    let beat = |p: &str, k: &str, at: i64| Beat {
        principal: p.into(),
        agent_session: k.into(),
        at,
    };
    let n = store
        .beat_sessions(&[
            beat("ada", "s-1", t0 + 100),
            beat("ada", "s-1", t0 + 90),
            beat("bo", "s-1", t0 + 1),
            beat("ada", "nope", t0 + 100),
        ])
        .await
        .unwrap();
    assert_eq!(n, 2);
    assert_eq!(
        store.get_session(a.id).await.unwrap().unwrap().last_seen,
        t0 + 100
    );
    assert_eq!(
        store.get_session(b.id).await.unwrap().unwrap().last_seen,
        t0 + 10
    );
    assert_eq!(store.beat_sessions(&[]).await.unwrap(), 0);

    // Ending is conditional and once only.
    let end = |state, principal: Option<&str>, if_head| SessionEnd {
        state,
        at: t0 + 200,
        landed_as: Some(cs("landed")),
        principal: principal.map(str::to_string),
        if_head,
    };
    assert!(store
        .end_session(b.id, &end(SessionState::Active, None, None))
        .await
        .is_err());
    assert!(!store
        .end_session(b.id, &end(SessionState::Landed, Some("ada"), None))
        .await
        .unwrap());
    assert!(!store
        .end_session(b.id, &end(SessionState::Landed, None, Some(cs("other"))))
        .await
        .unwrap());
    assert!(store
        .end_session(b.id, &end(SessionState::Landed, Some("bo"), Some(cs("bh"))))
        .await
        .unwrap());
    assert!(!store
        .end_session(b.id, &end(SessionState::Abandoned, None, None))
        .await
        .unwrap());
    assert!(!store
        .end_session(u64::MAX / 2, &end(SessionState::Abandoned, None, None))
        .await
        .unwrap());
    let ended = store.get_session(b.id).await.unwrap().unwrap();
    assert_eq!(ended.state, SessionState::Landed);
    assert_eq!(
        (ended.ended, ended.landed_as),
        (Some(t0 + 200), Some(cs("landed")))
    );
    assert_eq!(
        store
            .beat_sessions(&[beat("bo", "s-1", t0 + 300)])
            .await
            .unwrap(),
        0
    );
    let active = SessionFilter {
        states: vec![SessionState::Active],
        ..all.clone()
    };
    assert_eq!(list(active.clone()).await, [c.id, a.id]);
    assert_eq!(
        store
            .count_sessions(SessionState::Active, 100)
            .await
            .unwrap(),
        2
    );
    assert_eq!(
        store.count_sessions(SessionState::Active, 1).await.unwrap(),
        1
    );
    assert_eq!(
        store
            .count_sessions(SessionState::Landed, 100)
            .await
            .unwrap(),
        1
    );
    let landed = SessionFilter {
        states: vec![SessionState::Landed, SessionState::Expired],
        ..all.clone()
    };
    assert_eq!(list(landed).await, [b.id]);

    // An ended session is revived by its next update, without its old work.
    let mut again = infer("bo", "s-1", t0 + 400);
    again.touch = vec!["lib/new.rs".into()];
    let (b2, revived) = store.upsert_session(&again).await.unwrap();
    assert!(revived);
    assert_eq!(b2.id, b.id);
    assert_eq!(
        (b2.state, b2.ended, b2.landed_as, b2.head),
        (SessionState::Active, None, None, None)
    );
    assert_eq!(b2.touched, ["lib/new.rs"]);
    assert_eq!(list(at("web")).await, Vec::<u64>::new());

    // At most `max_active` active sessions per principal.
    let mut capped = declare("dee", "k1", t0);
    capped.max_active = 2;
    store.upsert_session(&capped).await.unwrap();
    capped.agent_session = "k2".into();
    store.upsert_session(&capped).await.unwrap();
    capped.agent_session = "k3".into();
    match store.upsert_session(&capped).await {
        Err(Error::Invalid(m)) => assert!(m.contains(TOO_MANY_SESSIONS_MARKER), "{m}"),
        other => panic!("expected the limit, got {other:?}"),
    }
    // Updating one of them is not starting one.
    capped.agent_session = "k2".into();
    capped.title = Some("still fine".into());
    assert!(!store.upsert_session(&capped).await.unwrap().1);

    // Concurrent upserts of one key make one session and lose no path.
    let tasks = (0..16).map(|n| {
        let mut u = infer("eve", "race", t0 + n);
        u.touch = vec![format!("race/f{n}.rs")];
        async move { store.upsert_session(&u).await }
    });
    let results = futures_util::future::join_all(tasks).await;
    let ids: std::collections::BTreeSet<u64> =
        results.iter().map(|r| r.as_ref().unwrap().0.id).collect();
    assert_eq!(ids.len(), 1, "one row per key");
    assert_eq!(results.iter().filter(|r| r.as_ref().unwrap().1).count(), 1);
    let race = store
        .get_session(*ids.iter().next().unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(race.touched.len(), 16, "{:?}", race.touched);

    // The sweep expires the silent, then forgets the long ended.
    let swept = store
        .sweep_sessions(t0 + 100 + EXPIRE_AFTER_SECS - 1, 1000)
        .await
        .unwrap();
    // Only `c`, `dee`'s and `eve`'s sessions were last heard before `a`'s beat.
    let still: Vec<u64> = list(active.clone()).await;
    assert!(still.contains(&a.id) && still.contains(&b.id), "{swept:?}");
    assert!(!still.contains(&c.id));
    let later = t0 + 400 + EXPIRE_AFTER_SECS + 1;
    let r = store.sweep_sessions(later, 1000).await.unwrap();
    assert!(r.expired >= 2, "{r:?}");
    assert!(list(active).await.is_empty());
    let gone = store.get_session(c.id).await.unwrap().unwrap();
    assert_eq!(gone.state, SessionState::Expired);
    assert!(!gone.scope.is_empty(), "paths stay for a while");
    let r = store
        .sweep_sessions(later + PATHS_RETENTION_SECS + 1, 1000)
        .await
        .unwrap();
    assert!(r.paths_deleted > 0 && r.sessions_deleted == 0, "{r:?}");
    let gone = store.get_session(c.id).await.unwrap().unwrap();
    assert!(gone.scope.is_empty() && gone.touched.is_empty());
    assert!(list(at("src")).await.is_empty());
    // Bounded steps: one row at a time still gets there.
    let end_of_days = later + SESSION_RETENTION_SECS + 1;
    let mut deleted = 0;
    loop {
        let r = store.sweep_sessions(end_of_days, 1).await.unwrap();
        assert!(r.sessions_deleted <= 1 && r.paths_deleted <= 1 && r.expired <= 1);
        deleted += r.sessions_deleted;
        if r == SweepReport::default() {
            break;
        }
    }
    assert!(deleted >= 5, "{deleted}");
    assert!(list(all).await.is_empty());
    assert_eq!(store.get_session(a.id).await.unwrap(), None);

    check_near_contract(store, end_of_days).await;
    check_notes_contract(store).await;
    check_events_contract(store).await;
}

/// [`CoordStore::near_paths`] (ADR 0096): the rows of other active
/// sessions on, above, below and (with siblings) beside the paths asked
/// about, with their sessions, bounded per range and in all.
async fn check_near_contract(store: &dyn CoordStore, t0: i64) {
    let declare = |p: &str, k: &str| SessionUpdate::new(p, k, SessionOrigin::Declared, t0);
    let mut me = declare("me", "near-0");
    me.title = Some("mine".into());
    me.touch = vec!["nr/a/x.rs".into()];
    let (me, _) = store.upsert_session(&me).await.unwrap();
    let mut same = declare("p1", "near-1");
    same.title = Some("same file".into());
    same.touch = vec!["nr/a/x.rs".into(), "elsewhere/q".into()];
    let (same, _) = store.upsert_session(&same).await.unwrap();
    let mut above = declare("p2", "near-2");
    above.claims = Some(vec!["nr/a".into()]);
    let (above, _) = store.upsert_session(&above).await.unwrap();
    let mut beside = declare("p3", "near-3");
    beside.scope = Some(vec!["nr/a/y.rs".into(), "nr/ab".into()]);
    let (beside, _) = store.upsert_session(&beside).await.unwrap();
    let mut ended = declare("p4", "near-4");
    ended.touch = vec!["nr/a/x.rs".into()];
    let (ended, _) = store.upsert_session(&ended).await.unwrap();
    let end = SessionEnd {
        state: SessionState::Abandoned,
        at: t0,
        landed_as: None,
        principal: None,
        if_head: None,
    };
    assert!(store.end_session(ended.id, &end).await.unwrap());

    let q = NearQuery {
        paths: vec!["nr/a/x.rs".into()],
        siblings: false,
        exclude: Some(me.id),
        per_range: 10,
        limit: 100,
    };
    let rows = |v: Vec<NearRow>| {
        let mut v: Vec<(u64, PathKind, String)> = v
            .into_iter()
            .map(|r| {
                assert!(r.session.scope.is_empty() && r.session.touched.is_empty());
                assert_eq!(r.session.state, SessionState::Active);
                (r.session.id, r.kind, r.path)
            })
            .collect();
        v.sort();
        v
    };
    let got = rows(store.near_paths(&q).await.unwrap());
    assert_eq!(
        got,
        [
            (same.id, PathKind::Touched, "nr/a/x.rs".to_string()),
            (above.id, PathKind::Claim, "nr/a".to_string()),
        ]
        .into_iter()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>(),
        "the ended session and the excluded one are left out"
    );
    let found = store.near_paths(&q).await.unwrap();
    let s = found.iter().find(|r| r.session.id == same.id).unwrap();
    assert_eq!(
        (s.session.title.as_str(), s.session.principal.as_str()),
        ("same file", "p1")
    );
    // Siblings: the same directory, never a name that only starts alike.
    let sib = NearQuery {
        siblings: true,
        ..q.clone()
    };
    let got = rows(store.near_paths(&sib).await.unwrap());
    assert!(got.contains(&(beside.id, PathKind::Scope, "nr/a/y.rs".into())));
    assert!(!got.iter().any(|r| r.2 == "nr/ab"), "{got:?}");
    assert_eq!(got.len(), 3, "{got:?}");
    // Below a directory asked about.
    let dir = NearQuery {
        paths: vec!["nr".into()],
        ..q.clone()
    };
    assert_eq!(store.near_paths(&dir).await.unwrap().len(), 4);
    // Bounds: per range and in all.
    let mut crowd = Vec::new();
    for n in 0..12 {
        let mut u = declare("crowd", &format!("c-{n}"));
        u.touch = vec![format!("hot/f{n}.rs")];
        crowd.push(store.upsert_session(&u).await.unwrap().0);
    }
    let hot = NearQuery {
        paths: vec!["hot".into()],
        per_range: 5,
        ..q.clone()
    };
    assert_eq!(store.near_paths(&hot).await.unwrap().len(), 5);
    let capped = NearQuery {
        paths: vec!["hot".into(), "nr/a/x.rs".into()],
        per_range: 50,
        limit: 7,
        ..q.clone()
    };
    assert_eq!(store.near_paths(&capped).await.unwrap().len(), 7);
    assert!(store
        .near_paths(&NearQuery {
            limit: 0,
            ..q.clone()
        })
        .await
        .unwrap()
        .is_empty());
    // Paths without overlap find nothing.
    let none = NearQuery {
        paths: vec!["zz/top".into()],
        siblings: true,
        ..q.clone()
    };
    assert!(store.near_paths(&none).await.unwrap().is_empty());
    // Skipping paths in a listing.
    let bare = store
        .list_sessions(&SessionFilter {
            principal: Some("p1".into()),
            skip_paths: true,
            limit: 10,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(bare.len(), 1);
    assert!(bare[0].touched.is_empty() && bare[0].title == "same file");
    // Claims and exact matches first: a crowd scoped to a directory above
    // never crowds out a claim on the very file, and each exact path is
    // bounded on its own.
    for n in 0..12 {
        let mut u = declare("wide", &format!("w-{n}"));
        u.scope = Some(vec!["cl".into()]);
        crowd.push(store.upsert_session(&u).await.unwrap().0);
    }
    let mut claim = declare("owner", "cl-claim");
    claim.claims = Some(vec!["cl/a/x.rs".into()]);
    let (claim, _) = store.upsert_session(&claim).await.unwrap();
    crowd.push(claim.clone());
    let narrow = NearQuery {
        paths: vec!["cl/a/x.rs".into()],
        per_range: 4,
        limit: 3,
        ..q.clone()
    };
    let got = rows(store.near_paths(&narrow).await.unwrap());
    assert!(
        got.contains(&(claim.id, PathKind::Claim, "cl/a/x.rs".into())),
        "{got:?}"
    );
    assert_eq!(got.len(), 3, "{got:?}");
    let per_path = NearQuery {
        limit: 100,
        ..narrow.clone()
    };
    let got = rows(store.near_paths(&per_path).await.unwrap());
    assert_eq!(got.iter().filter(|r| r.2 == "cl").count(), 4, "{got:?}");
    for s in [me, same, above, beside].into_iter().chain(crowd) {
        assert!(store.end_session(s.id, &end).await.unwrap());
    }
    // Leave the store as the rest of the contract does: empty, so callers
    // that check a fresh store after it (the migration tests) see none.
    let gone = t0 + SESSION_RETENTION_SECS + PATHS_RETENTION_SECS + 1;
    while store.sweep_sessions(gone, 1000).await.unwrap() != SweepReport::default() {}
    let every = SessionFilter {
        limit: 100,
        ..Default::default()
    };
    assert!(store.list_sessions(&every).await.unwrap().is_empty());
}

/// The notes part of the contract (ADR 0098): writes checked and capped in
/// one transaction, one winner among concurrent supersessions, listings by
/// anchor, conditional state changes and a bounded sweep. Leaves no note.
pub async fn check_notes_contract(store: &dyn CoordStore) {
    use crate::coord::{
        AnchorType, NoteKind, AGENT_NOTE_TTL_SECS, MAX_NOTES_PER_HOUR,
        MAX_OPEN_NOTES_PER_PRINCIPAL, NOTES_PER_DAY_MARKER, NOTES_PER_HOUR_MARKER,
        REPORTS_PER_HOUR_MARKER, TOO_MANY_NOTES_MARKER,
    };
    let t0: i64 = 1_700_000_000;
    let cs = |s: &str| ChangesetId(Hash::of_blob(s.as_bytes()));
    let note = |who: &str, anchor: &str, title: &str| NewNote {
        kind: NoteKind::Finding,
        anchor_type: AnchorType::Path,
        anchor: anchor.into(),
        at_changeset: Some(cs("at")),
        at_blob: Some(Hash::of_blob(b"blob")),
        lines: None,
        title: title.into(),
        body: String::new(),
        principal: who.into(),
        agent_model: None,
        session_id: None,
        trust: NoteTrust::Agent,
        supersedes: None,
        created: t0,
        expires: Some(t0 + AGENT_NOTE_TTL_SECS),
        max_open: MAX_OPEN_NOTES_PER_PRINCIPAL,
        max_per_hour: 1000,
        max_per_day: 1000,
    };

    // Write and read back every column.
    let mut n = note("ada", "src/auth/token.rs", "Refresh races the cache");
    n.kind = NoteKind::Gotcha;
    n.lines = Some((10, 20));
    n.body = "Take the lock first.\nThen refresh.".into();
    n.agent_model = Some("model-a".into());
    n.session_id = Some(42);
    let a = store.add_note(&n).await.unwrap();
    assert_eq!(
        (a.kind, a.anchor_type, a.anchor.as_str()),
        (NoteKind::Gotcha, AnchorType::Path, "src/auth/token.rs")
    );
    assert_eq!((a.at_changeset, a.at_blob), (n.at_changeset, n.at_blob));
    assert_eq!(a.lines, Some((10, 20)));
    assert_eq!(a.title, "Refresh races the cache");
    assert_eq!(a.body, "Take the lock first.\nThen refresh.");
    assert_eq!(
        (a.principal.as_str(), a.agent_model.as_deref(), a.session_id),
        ("ada", Some("model-a"), Some(42))
    );
    assert_eq!((a.trust, a.state), (NoteTrust::Agent, NoteState::Open));
    assert_eq!((a.endorsed_by.as_deref(), a.endorsed_at), (None, None));
    assert_eq!((a.superseded_by, a.created), (None, t0));
    assert_eq!(a.expires, Some(t0 + AGENT_NOTE_TTL_SECS));
    assert_eq!(store.get_note(a.id).await.unwrap(), Some(a.clone()));
    assert_eq!(store.get_note(u64::MAX / 2).await.unwrap(), None);
    // A bad note stores nothing.
    let mut bad = note("ada", "src/x", "two\nlines");
    assert!(store.add_note(&bad).await.is_err());
    bad.title = "ok".into();
    bad.anchor = "/abs".into();
    assert!(store.add_note(&bad).await.is_err());

    let mut h = note("hugo", "src/auth", "Auth is owned by the platform team");
    h.trust = NoteTrust::Human;
    h.expires = None;
    let dir = store.add_note(&h).await.unwrap();
    assert_eq!((dir.trust, dir.expires), (NoteTrust::Human, None));
    let mut c = note("bo", "", "This change broke the nightly");
    c.anchor_type = AnchorType::Changeset;
    c.anchor = cs("broke").to_string();
    c.at_changeset = Some(cs("broke"));
    c.at_blob = None;
    let on_cs = store.add_note(&c).await.unwrap();
    let sib = store
        .add_note(&note("bo", "src/authx/a.rs", "not under src/auth"))
        .await
        .unwrap();
    let below = store
        .add_note(&note("bo", "src/auth/token.rs/x", "below a file"))
        .await
        .unwrap();
    assert!(a.id < dir.id && dir.id < on_cs.id && on_cs.id < sib.id);

    // Listing: newest first, by path (the path and directories above it),
    // changeset, principal, state, and paged.
    let all = NoteFilter {
        limit: 100,
        ..Default::default()
    };
    let ids = |v: Vec<Note>| v.into_iter().map(|n| n.id).collect::<Vec<_>>();
    let list = |f: NoteFilter| async move { ids(store.list_notes(&f).await.unwrap()) };
    assert_eq!(
        list(all.clone()).await,
        [below.id, sib.id, on_cs.id, dir.id, a.id]
    );
    let at = |p: &str| NoteFilter {
        path: Some(p.into()),
        ..all.clone()
    };
    assert_eq!(list(at("src/auth/token.rs")).await, [dir.id, a.id]);
    assert_eq!(list(at("src/auth")).await, [dir.id]);
    assert_eq!(list(at("src/auth/other.rs")).await, [dir.id]);
    assert_eq!(list(at("src/authx/a.rs")).await, [sib.id]);
    assert!(list(at("src")).await.is_empty(), "nothing below is listed");
    let on = NoteFilter {
        changeset: Some(cs("broke").to_string()),
        ..all.clone()
    };
    assert_eq!(list(on).await, [on_cs.id]);
    let by = NoteFilter {
        principal: Some("bo".into()),
        ..all.clone()
    };
    assert_eq!(list(by).await, [below.id, sib.id, on_cs.id]);
    let page = NoteFilter {
        before: Some(on_cs.id),
        limit: 1,
        ..all.clone()
    };
    assert_eq!(list(page).await, [dir.id]);
    assert!(list(NoteFilter {
        limit: 0,
        ..all.clone()
    })
    .await
    .is_empty());
    // Probes are bounded: a path past MAX_PATH_BYTES, or too many anchors.
    let long = NoteFilter {
        path: Some("a/".repeat(200)),
        ..all.clone()
    };
    assert!(store.list_notes(&long).await.is_err());
    let many: Vec<String> = (0..=crate::coord::MAX_NOTE_ANCHORS)
        .map(|i| format!("p{i}"))
        .collect();
    assert!(store.notes_at(&many, 1).await.is_err());
    // Exact anchors, open only, capped.
    let anchors = ["src/auth/token.rs".to_string(), "src/auth".to_string()];
    assert_eq!(
        ids(store.notes_at(&anchors, 10).await.unwrap()),
        [dir.id, a.id]
    );
    assert_eq!(ids(store.notes_at(&anchors, 1).await.unwrap()), [dir.id]);
    assert!(store.notes_at(&[], 10).await.unwrap().is_empty());

    // Supersede: only the principal's own open note, once.
    let mut s = note("bo", "src/auth/token.rs", "edit");
    s.supersedes = Some(a.id);
    assert!(matches!(store.add_note(&s).await, Err(Error::Policy(_))));
    s.principal = "ada".into();
    s.supersedes = Some(u64::MAX / 2);
    assert!(matches!(store.add_note(&s).await, Err(Error::NotFound(_))));
    s.supersedes = Some(a.id);
    let a2 = store.add_note(&s).await.unwrap();
    let old = store.get_note(a.id).await.unwrap().unwrap();
    assert_eq!(
        (old.state, old.superseded_by),
        (NoteState::Superseded, Some(a2.id))
    );
    assert_eq!(old.expires, Some(t0 + CLOSED_NOTE_RETENTION_SECS));
    let before = list(all.clone()).await.len();
    assert!(matches!(store.add_note(&s).await, Err(Error::Conflict(_))));
    assert_eq!(list(all.clone()).await.len(), before, "nothing stored");
    assert_eq!(
        ids(store.notes_at(&anchors, 10).await.unwrap()),
        [a2.id, dir.id]
    );
    let open = NoteFilter {
        states: vec![NoteState::Open],
        ..at("src/auth/token.rs")
    };
    assert_eq!(list(open).await, [a2.id, dir.id]);
    let closed = NoteFilter {
        states: vec![NoteState::Superseded, NoteState::Retracted],
        ..all.clone()
    };
    assert_eq!(list(closed).await, [a.id]);

    // Concurrent supersessions of one note: one winner, the rest refused
    // and stored nothing.
    let target = store
        .add_note(&note("eve", "race/a.rs", "v0"))
        .await
        .unwrap();
    let tasks = (0..16).map(|i| {
        let mut n = note("eve", "race/a.rs", &format!("v{}", i + 1));
        n.supersedes = Some(target.id);
        async move { store.add_note(&n).await }
    });
    let results = futures_util::future::join_all(tasks).await;
    let won: Vec<&Note> = results.iter().filter_map(|r| r.as_ref().ok()).collect();
    assert_eq!(won.len(), 1, "one winner");
    for r in &results {
        if let Err(e) = r {
            assert!(matches!(e, Error::Conflict(_)), "{e}");
        }
    }
    let eve = NoteFilter {
        principal: Some("eve".into()),
        ..all.clone()
    };
    assert_eq!(list(eve.clone()).await, [won[0].id, target.id]);

    // Caps: open notes per principal, and notes per hour.
    let mut capped = note("cap", "cap/a", "one");
    capped.max_open = 2;
    let c1 = store.add_note(&capped).await.unwrap();
    store.add_note(&capped).await.unwrap();
    match store.add_note(&capped).await {
        Err(Error::Invalid(m)) => assert!(m.contains(TOO_MANY_NOTES_MARKER), "{m}"),
        other => panic!("expected the open cap, got {other:?}"),
    }
    // Superseding at the cap replaces one open note with another.
    capped.supersedes = Some(c1.id);
    store.add_note(&capped).await.unwrap();
    let mut rated = note("rate", "rate/a", "one");
    rated.max_per_hour = 2;
    store.add_note(&rated).await.unwrap();
    rated.created = t0 + 10;
    let r2 = store.add_note(&rated).await.unwrap();
    // Closing notes does not give back the hour's writes.
    assert!(store
        .change_note(r2.id, &NoteChange::Retract { by: "rate".into() }, t0 + 11)
        .await
        .unwrap());
    rated.created = t0 + 20;
    match store.add_note(&rated).await {
        Err(Error::Invalid(m)) => assert!(m.contains(NOTES_PER_HOUR_MARKER), "{m}"),
        other => panic!("expected the hourly cap, got {other:?}"),
    }
    rated.created = t0 + NOTE_RATE_WINDOW_SECS + 11;
    store.add_note(&rated).await.unwrap();
    assert_eq!(MAX_NOTES_PER_HOUR, 30);
    // And per day: hours apart, the day's cap still holds.
    let mut daily = note("day", "day/a", "one");
    daily.max_per_day = 2;
    store.add_note(&daily).await.unwrap();
    daily.created = t0 + 2 * NOTE_RATE_WINDOW_SECS;
    store.add_note(&daily).await.unwrap();
    daily.created = t0 + 4 * NOTE_RATE_WINDOW_SECS;
    match store.add_note(&daily).await {
        Err(Error::Invalid(m)) => assert!(m.contains(NOTES_PER_DAY_MARKER), "{m}"),
        other => panic!("expected the daily cap, got {other:?}"),
    }
    daily.created = t0 + crate::coord::NOTE_DAY_WINDOW_SECS + 1;
    store.add_note(&daily).await.unwrap();

    // Changes are conditional, and close a note for 30 days.
    let at1 = t0 + 100;
    let retract = |by: &str| NoteChange::Retract { by: by.into() };
    assert!(!store.change_note(a2.id, &retract("bo"), at1).await.unwrap());
    assert!(store
        .change_note(a2.id, &retract("ada"), at1)
        .await
        .unwrap());
    assert!(!store
        .change_note(a2.id, &retract("ada"), at1)
        .await
        .unwrap());
    let gone = store.get_note(a2.id).await.unwrap().unwrap();
    assert_eq!(
        (gone.state, gone.expires),
        (NoteState::Retracted, Some(at1 + CLOSED_NOTE_RETENTION_SECS))
    );
    assert!(!store
        .change_note(u64::MAX / 2, &NoteChange::Revoke, at1)
        .await
        .unwrap());
    // A report hides an agent note; a human one stays.
    let report = NoteChange::Report {
        by: "rex".into(),
        max_per_hour: 2,
    };
    assert!(!store.change_note(dir.id, &report, at1).await.unwrap());
    assert_eq!(
        store.get_note(dir.id).await.unwrap().unwrap().state,
        NoteState::Open
    );
    assert!(store.change_note(sib.id, &report, at1).await.unwrap());
    let hid = store.get_note(sib.id).await.unwrap().unwrap();
    assert_eq!(
        (hid.state, hid.reported_by.as_deref(), hid.reported_at),
        (NoteState::Reported, Some("rex"), Some(at1))
    );
    // A reporter may hide at most `max_per_hour` notes an hour; past it
    // every report of theirs is refused, and nothing changes.
    let spare = store.add_note(&note("bo", "rep/a", "spare")).await.unwrap();
    let spare2 = store
        .add_note(&note("bo", "rep/b", "spare 2"))
        .await
        .unwrap();
    assert!(store.change_note(spare.id, &report, at1 + 1).await.unwrap());
    match store.change_note(spare2.id, &report, at1 + 2).await {
        Err(Error::Invalid(m)) => assert!(m.contains(REPORTS_PER_HOUR_MARKER), "{m}"),
        other => panic!("expected the report cap, got {other:?}"),
    }
    assert_eq!(
        store.get_note(spare2.id).await.unwrap().unwrap().state,
        NoteState::Open
    );
    // An operator undoes reports: one note, or every note of a reporter.
    // The note is open again with an agent note's life.
    assert_eq!(store.unreport_notes(Some(spare.id), None).await.unwrap(), 1);
    let back = store.get_note(spare.id).await.unwrap().unwrap();
    assert_eq!(
        (back.state, back.reported_by, back.expires),
        (NoteState::Open, None, Some(t0 + AGENT_NOTE_TTL_SECS))
    );
    assert_eq!(store.unreport_notes(None, Some("rex")).await.unwrap(), 1);
    assert_eq!(
        store.get_note(sib.id).await.unwrap().unwrap().state,
        NoteState::Open
    );
    assert_eq!(store.unreport_notes(None, Some("rex")).await.unwrap(), 0);
    // Closing never lengthens a life: a note past its end, not yet swept,
    // keeps that end when reported.
    let mut dying = note("bo", "rep/c", "short");
    dying.expires = Some(at1 - 1);
    let dying = store.add_note(&dying).await.unwrap();
    let other = NoteChange::Report {
        by: "rae".into(),
        max_per_hour: 5,
    };
    assert!(store.change_note(dying.id, &other, at1).await.unwrap());
    assert_eq!(
        store.get_note(dying.id).await.unwrap().unwrap().expires,
        Some(at1 - 1)
    );
    // Endorsement: once, and the note no longer expires; a report then
    // only counts.
    let endorse = NoteChange::Endorse { by: "hugo".into() };
    assert!(store.change_note(below.id, &endorse, at1).await.unwrap());
    assert!(!store.change_note(below.id, &endorse, at1).await.unwrap());
    let e = store.get_note(below.id).await.unwrap().unwrap();
    assert_eq!(
        (e.endorsed_by.as_deref(), e.endorsed_at, e.expires),
        (Some("hugo"), Some(at1), None)
    );
    assert!(!store.change_note(below.id, &other, at1).await.unwrap());
    // An operator's mass revoke: every open note of one principal.
    assert_eq!(store.revoke_notes("eve", at1).await.unwrap(), 1);
    assert_eq!(store.revoke_notes("eve", at1).await.unwrap(), 0);
    assert!(list(NoteFilter {
        states: vec![NoteState::Open],
        ..eve
    })
    .await
    .is_empty());

    // The sweep: open notes past their life, closed ones 30 days on, in
    // bounded steps.
    // Only the reported note whose life had already ended is due.
    let r = store.sweep_notes(at1, 1000).await.unwrap();
    assert_eq!((r.expired, r.closed), (0, 1));
    assert_eq!(store.get_note(dying.id).await.unwrap(), None);
    let mut short = note("ada", "sweep/a", "short-lived");
    short.expires = Some(at1 + 5);
    let short = store.add_note(&short).await.unwrap();
    let r = store.sweep_notes(at1 + 6, 1000).await.unwrap();
    assert_eq!((r.expired, r.closed), (1, 0));
    assert_eq!(store.get_note(short.id).await.unwrap(), None);
    assert!(store.get_note(a2.id).await.unwrap().is_some());
    let end = t0 + NOTE_RATE_WINDOW_SECS + AGENT_NOTE_TTL_SECS + 1000;
    let mut swept = NoteSweep::default();
    loop {
        let r = store.sweep_notes(end, 1).await.unwrap();
        assert!(r.expired <= 1 && r.closed <= 1, "{r:?}");
        swept.expired += r.expired;
        swept.closed += r.closed;
        if r == NoteSweep::default() {
            break;
        }
    }
    assert!(swept.closed >= 4 && swept.expired >= 4, "{swept:?}");
    // Left: the human note and the endorsed one, which never expire.
    assert_eq!(list(all.clone()).await, [below.id, dir.id]);
    for id in [below.id, dir.id] {
        assert!(store
            .change_note(id, &NoteChange::Revoke, end)
            .await
            .unwrap());
    }
    let after = end + CLOSED_NOTE_RETENTION_SECS + 1;
    while store.sweep_notes(after, 1000).await.unwrap() != NoteSweep::default() {}
    assert!(list(all).await.is_empty());
}

/// Assert the events part of the [`CoordStore`] contract (ADR 0099) on an
/// empty store: `seq` ascends in append order, pages are bounded and
/// ordered, the bounds follow appends and sweeps, and the sweep deletes a
/// bounded prefix in `seq` order, stopping at the first young event.
pub async fn check_events_contract(store: &dyn CoordStore) {
    let t0 = 1_700_000_000;
    let ev = |n: usize, at: i64| NewEvent {
        kind: "session.started".into(),
        session_id: Some(n as u64),
        principal: format!("p{}", n % 3),
        subject: format!("session:{n}"),
        paths: vec![format!("src/m{n}"), "docs".into()],
        payload: format!("{{\"origin\":\"declared\",\"n\":{n}}}"),
        created: at,
    };
    let fresh = store.events_bounds().await.unwrap();
    assert_eq!(fresh, EventBounds::default());
    assert!(!fresh.expired(0));
    assert_eq!(store.append_events(&[]).await.unwrap(), 0);
    assert!(store.events_after(0, 10).await.unwrap().is_empty());

    // Appends keep their order, within a call and across calls; a call
    // larger than any statement's bind limit is still one ordered append.
    let first: Vec<NewEvent> = (0..5).map(|n| ev(n, t0)).collect();
    let top = store.append_events(&first).await.unwrap();
    let big: Vec<NewEvent> = (5..2505).map(|n| ev(n, t0 + 10)).collect();
    let top2 = store.append_events(&big).await.unwrap();
    assert!(top2 > top);
    let mut all = Vec::new();
    let mut after = 0;
    loop {
        let page = store.events_after(after, 700).await.unwrap();
        assert!(page.len() <= 700);
        let Some(last) = page.last() else { break };
        after = last.seq;
        all.extend(page);
    }
    assert_eq!(all.len(), 2505);
    assert!(all.windows(2).all(|w| w[0].seq < w[1].seq), "seq ascends");
    assert_eq!(all.last().unwrap().seq, top2);
    for (n, e) in all.iter().enumerate() {
        assert_eq!(e, &ev(n, e.created).stored(e.seq), "event {n} reads back");
    }
    assert_eq!(all[4].seq, top);
    let b = store.events_bounds().await.unwrap();
    assert_eq!(b.oldest, Some(all[0].seq));
    assert_eq!(b.newest, top2);
    assert!(!b.expired(all[0].seq - 1));
    assert!(!b.expired(top2));
    assert!(
        b.expired(top2 + 1),
        "a cursor past the newest is from another store"
    );
    let mid = all[1000].seq;
    let page = store.events_after(mid, 3).await.unwrap();
    assert_eq!(
        page.iter().map(|e| e.seq).collect::<Vec<_>>(),
        all[1001..1004].iter().map(|e| e.seq).collect::<Vec<_>>()
    );

    // The sweep is bounded, takes the oldest first, and stops at the first
    // event created at or after the cutoff, even when older ones follow.
    let late = store.append_events(&[ev(9000, t0 + 500)]).await.unwrap();
    let old_again = store.append_events(&[ev(9001, t0)]).await.unwrap();
    assert_eq!(store.sweep_events(t0 + 5, 3).await.unwrap(), 3);
    assert_eq!(
        store.events_bounds().await.unwrap().oldest,
        Some(all[3].seq)
    );
    assert!(store.events_bounds().await.unwrap().expired(all[1].seq));
    assert!(!store.events_bounds().await.unwrap().expired(all[2].seq));
    assert_eq!(store.sweep_events(t0 + 5, 100).await.unwrap(), 2);
    assert_eq!(
        store.events_bounds().await.unwrap().oldest,
        Some(all[5].seq)
    );
    let mut swept = 0;
    loop {
        let n = store.sweep_events(t0 + 100, 1000).await.unwrap();
        assert!(n <= 1000);
        if n == 0 {
            break;
        }
        swept += n;
    }
    assert_eq!(swept, 2500);
    let b = store.events_bounds().await.unwrap();
    assert_eq!(b.oldest, Some(late), "stopped at the young event");
    assert_eq!(b.newest, old_again);
    assert_eq!(
        store
            .events_after(0, 10)
            .await
            .unwrap()
            .iter()
            .map(|e| e.seq)
            .collect::<Vec<_>>(),
        [late, old_again]
    );
    while store.sweep_events(t0 + 1000, 1000).await.unwrap() > 0 {}
    // Empty again: the sequence goes on, so old cursors are expired and
    // the newest is not.
    let b = store.events_bounds().await.unwrap();
    assert_eq!(b.oldest, None);
    assert_eq!(b.newest, old_again);
    assert!(b.expired(late));
    assert!(!b.expired(old_again));
    let next = store.append_events(&[ev(1, t0 + 2000)]).await.unwrap();
    assert!(next > old_again, "seq never goes back after a sweep");
    while store.sweep_events(t0 + 3000, 1000).await.unwrap() > 0 {}
    check_event_order(store).await;
}

/// Let the other futures of a `join` run.
async fn yield_once() {
    let mut yielded = false;
    std::future::poll_fn(|cx| {
        if yielded {
            std::task::Poll::Ready(())
        } else {
            yielded = true;
            cx.waker().wake_by_ref();
            std::task::Poll::Pending
        }
    })
    .await
}

/// Concurrent appends seen by a reader that pages by cursor while they
/// run: it ends with every event exactly once. A store that let a reader
/// see `seq` 11 before 10 committed would lose 10 here (ADR 0099).
async fn check_event_order(store: &dyn CoordStore) {
    use std::sync::atomic::{AtomicUsize, Ordering};
    const WRITERS: usize = 6;
    const BATCHES: usize = 40;
    let start = store.events_bounds().await.unwrap().newest;
    let done = AtomicUsize::new(0);
    let write = |w: usize| {
        let done = &done;
        async move {
            for b in 0..BATCHES {
                let batch: Vec<NewEvent> = (0..1 + (w + b) % 4)
                    .map(|i| NewEvent {
                        kind: "note.added".into(),
                        session_id: None,
                        principal: format!("w{w}"),
                        subject: format!("note:{}", w * 10_000 + b * 10 + i),
                        paths: Vec::new(),
                        payload: "{}".into(),
                        created: 1,
                    })
                    .collect();
                store.append_events(&batch).await.unwrap();
                yield_once().await;
            }
            done.fetch_add(1, Ordering::SeqCst);
        }
    };
    let read = async {
        let mut seen = Vec::new();
        let mut after = start;
        loop {
            let finished = done.load(Ordering::SeqCst) == WRITERS;
            let page = store.events_after(after, 37).await.unwrap();
            if let Some(last) = page.last() {
                after = last.seq;
            }
            let empty = page.is_empty();
            seen.extend(page.into_iter().map(|e| e.subject));
            if finished && empty {
                return seen;
            }
            yield_once().await;
        }
    };
    let writers = futures_util::future::join_all((0..WRITERS).map(write));
    let (_, seen) = futures_util::future::join(writers, read).await;
    let mut want: Vec<String> = (0..WRITERS)
        .flat_map(|w| {
            (0..BATCHES).flat_map(move |b| {
                (0..1 + (w + b) % 4).map(move |i| format!("note:{}", w * 10_000 + b * 10 + i))
            })
        })
        .collect();
    let mut got = seen.clone();
    got.sort();
    got.dedup();
    assert_eq!(got.len(), seen.len(), "no event read twice");
    want.sort();
    assert_eq!(got, want, "no event missed");
    while store.sweep_events(2, 1000).await.unwrap() > 0 {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn memory_coord_store_passes_the_contract() {
        check_coord_contract(&MemoryCoordStore::new()).await;
    }

    #[tokio::test]
    async fn the_memory_meta_store_hands_out_one_coord_store() {
        use crate::MetaStore;
        let meta = crate::memory::MemoryMetaStore::new();
        let a = meta.coord_store().unwrap();
        let b = meta.coord_store().unwrap();
        let u = SessionUpdate::new("ada", "k", SessionOrigin::Declared, 1);
        let (s, _) = a.upsert_session(&u).await.unwrap();
        assert_eq!(b.get_session(s.id).await.unwrap(), Some(s));
    }
}
