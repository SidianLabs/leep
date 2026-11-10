//! The in-memory [`CoordStore`] and the contract every backend must pass
//! (re-exported from [`crate::memory`]).

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;

use async_trait::async_trait;

use crate::coord::{
    apply_update, check_end, Beat, CoordStore, NearQuery, NearRow, PathKind, Session, SessionEnd,
    SessionFilter, SessionOrigin, SessionState, SessionUpdate, SweepReport, EXPIRE_AFTER_SECS,
    MAX_TOUCHED_PATHS, PATHS_RETENTION_SECS, SESSION_RETENTION_SECS, TOO_MANY_SESSIONS_MARKER,
};
use crate::{ChangesetId, Error, Hash, Result};

#[derive(Default)]
struct Rows {
    next: u64,
    sessions: BTreeMap<u64, Session>,
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
        let mut picked: BTreeSet<(&str, u64, PathKind)> = all
            .iter()
            .filter(|(p, _, _)| exact.binary_search_by(|e| e.as_str().cmp(p)).is_ok())
            .copied()
            .collect();
        for (lo, hi) in &ranges {
            picked.extend(
                all.iter()
                    .filter(|(p, _, _)| *p > lo.as_str() && *p < hi.as_str())
                    .take(q.per_range)
                    .copied(),
            );
        }
        Ok(picked
            .into_iter()
            .take(q.limit)
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
            .collect())
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
    for s in [me, same, above, beside].into_iter().chain(crowd) {
        assert!(store.end_session(s.id, &end).await.unwrap());
    }
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
