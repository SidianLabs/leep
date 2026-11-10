//! Events (ADR 0099): an ordered log of coordination facts that any
//! server can read from any cursor, for orchestrators and for an MCP
//! server's cheap "anything new?" check.
//!
//! Writes are cheap and lossy: a hook pushes an event into a bounded
//! buffer ([`EVENT_BUFFER`], overflow counted) after the answer it follows
//! is decided, and a task flushes the buffer every [`FLUSH_EVERY`] in one
//! append. A failed append is counted and logged, never returned: like
//! every coordination write, no answer depends on it.
//!
//! Reads fan out in process. While a server has waiters (long-polls and
//! streams), one tailer task reads new events every [`FLUSH_EVERY`] into a
//! ring of the last [`RING`] and bumps a `watch`; waiters filter from the
//! ring. A cursor behind the ring, or a read with no waiter, reads one
//! bounded page from the store. An event carries ids, states and paths,
//! never text an agent wrote.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock, Weak};
use std::time::Duration;

use leep_core::api::{EventEntry, EventFacts, EventText, NoteEntry};
use leep_core::coord::{
    bound_paths, paths_overlap, CoordStore, Event, EventKind, NewEvent, Session,
    EVENT_RETENTION_SECS, MAX_EVENT_PATHS,
};
use leep_core::store::unix_now;
use leep_core::{Error, Hash, Result};
use tokio::sync::watch;

use crate::Repo;

/// Events held between flushes; past it new ones are dropped and counted
/// (`leep_events_dropped_total{reason="full"}`).
pub const EVENT_BUFFER: usize = 10_000;
/// How often held events are appended, and how often the tailer reads.
pub const FLUSH_EVERY: Duration = Duration::from_millis(250);
/// Events the tailer keeps in memory for its waiters.
pub const RING: usize = 10_000;
/// Events one store read returns.
pub const READ_PAGE: usize = 1000;
/// Most events, and bytes of them, in one answer.
pub const MAX_PAGE_EVENTS: usize = 500;
pub const MAX_PAGE_BYTES: usize = 256 * 1024;
/// The longest a long-poll waits.
pub const MAX_WAIT: Duration = Duration::from_secs(30);
/// Waiters (long-polls with a wait, streams) one server admits, and one
/// principal on one server.
pub const MAX_WAITERS: usize = 10_000;
pub const MAX_WAITERS_PER_PRINCIPAL: usize = 64;
/// Most `paths` filters one read takes.
pub const MAX_FILTER_PATHS: usize = 16;
/// Events one sweep statement deletes.
pub const SWEEP_STEP: usize = 1000;
/// Sessions whose last sent path set a server remembers, so
/// `session.paths` is sent only when the set changed. Past it the memory is
/// cleared; a session it forgot sends its paths once more.
const PATHS_SEEN: usize = 100_000;

/// The events state of a repository handle, shared by its clones.
pub struct Events {
    buf: Mutex<Vec<NewEvent>>,
    /// Whether a flush task runs.
    flusher: AtomicBool,
    /// Set when the store keeps no events: nothing is buffered.
    off: AtomicBool,
    /// One flush at a time, so batches append in the order they were taken.
    flushing: tokio::sync::Mutex<()>,
    /// A digest of the paths last sent per session.
    paths_seen: Mutex<HashMap<u64, u64>>,
    retention_secs: AtomicI64,
    hub: Hub,
}

impl Default for Events {
    fn default() -> Self {
        Events {
            buf: Mutex::default(),
            flusher: AtomicBool::new(false),
            off: AtomicBool::new(false),
            flushing: tokio::sync::Mutex::new(()),
            paths_seen: Mutex::default(),
            retention_secs: AtomicI64::new(EVENT_RETENTION_SECS),
            hub: Hub::default(),
        }
    }
}

/// The tailer, its ring and its waiters.
struct Hub {
    state: Mutex<HubState>,
    ring: RwLock<Ring>,
    /// The newest `seq` in the ring, bumped after each read that added any.
    tail: watch::Sender<u64>,
    shutdown: watch::Sender<bool>,
    max_waiters: AtomicUsize,
    max_per_principal: AtomicUsize,
}

impl Default for Hub {
    fn default() -> Self {
        Hub {
            state: Mutex::default(),
            ring: RwLock::default(),
            tail: watch::Sender::new(0),
            shutdown: watch::Sender::new(false),
            max_waiters: AtomicUsize::new(MAX_WAITERS),
            max_per_principal: AtomicUsize::new(MAX_WAITERS_PER_PRINCIPAL),
        }
    }
}

#[derive(Default)]
struct HubState {
    waiters: usize,
    per: HashMap<String, usize>,
    tailing: bool,
}

/// The ring holds every event with `floor < seq <= last` while `live`.
#[derive(Default)]
struct Ring {
    live: bool,
    floor: u64,
    last: u64,
    events: VecDeque<Arc<Event>>,
}

impl Ring {
    /// Events above `after`, at most [`READ_PAGE`]; `None` when the ring
    /// does not hold everything above it.
    fn after(&self, after: u64) -> Option<Vec<Arc<Event>>> {
        if !self.live || after < self.floor {
            return None;
        }
        let from = self.events.partition_point(|e| e.seq <= after);
        Some(self.events.range(from..).take(READ_PAGE).cloned().collect())
    }

    fn push(&mut self, page: Vec<Event>) {
        for e in page {
            self.last = self.last.max(e.seq);
            self.events.push_back(Arc::new(e));
            if self.events.len() > RING {
                if let Some(old) = self.events.pop_front() {
                    self.floor = old.seq;
                }
            }
        }
    }
}

/// Why a waiter was refused (`leep_events_refused_total{reason}`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Refused {
    /// The server has [`MAX_WAITERS`].
    Server,
    /// The principal has [`MAX_WAITERS_PER_PRINCIPAL`] on this server.
    Principal,
}

impl Refused {
    pub fn as_str(self) -> &'static str {
        match self {
            Refused::Server => "server",
            Refused::Principal => "principal",
        }
    }
}

/// An admitted waiter: holds its place among the server's waiters and
/// keeps the tailer running until dropped.
pub struct Waiter {
    events: Arc<Events>,
    principal: String,
    tail: watch::Receiver<u64>,
    shutdown: watch::Receiver<bool>,
}

impl Drop for Waiter {
    fn drop(&mut self) {
        let mut st = self.events.hub.state.lock().unwrap();
        st.waiters -= 1;
        if let Some(n) = st.per.get_mut(&self.principal) {
            *n -= 1;
            if *n == 0 {
                st.per.remove(&self.principal);
            }
        }
        metrics::gauge!("leep_event_waiters").set(st.waiters as f64);
    }
}

impl Waiter {
    /// Whether the server is shutting down.
    pub fn stopping(&self) -> bool {
        *self.shutdown.borrow()
    }
}

/// Which events a reader wants.
#[derive(Clone, Debug, Default)]
pub struct EventFilter {
    /// Kinds, exactly; empty is every kind.
    pub kinds: Vec<String>,
    /// Events on or around these paths; empty is every path.
    pub paths: Vec<String>,
    pub session: SessionScope,
}

/// What `session=KEY` resolved to.
#[derive(Clone, Debug, Default)]
pub enum SessionScope {
    /// No `session`.
    #[default]
    Any,
    /// A key that names no session of the caller's: nothing matches.
    Nothing,
    /// The caller's session: events of other sessions on its paths, or of
    /// sessions on its task.
    Of {
        id: u64,
        paths: Vec<String>,
        task: Option<String>,
    },
}

impl EventFilter {
    pub fn matches(&self, e: &Event) -> bool {
        if !self.kinds.is_empty() && !self.kinds.contains(&e.kind) {
            return false;
        }
        let on = |want: &[String]| {
            e.paths
                .iter()
                .any(|p| want.iter().any(|q| paths_overlap(p, q)))
        };
        if !self.paths.is_empty() && !on(&self.paths) {
            return false;
        }
        match &self.session {
            SessionScope::Any => true,
            SessionScope::Nothing => false,
            SessionScope::Of { id, paths, task } => {
                if e.session_id == Some(*id) {
                    return false;
                }
                on(paths)
                    || task.as_ref().is_some_and(|t| {
                        e.kind.starts_with("session.")
                            && facts(e).task.as_deref() == Some(t.as_str())
                    })
            }
        }
    }
}

fn facts(e: &Event) -> EventFacts {
    serde_json::from_str(&e.payload).unwrap_or_default()
}

/// The wire form of `e`.
pub fn entry(e: &Event) -> EventEntry {
    EventEntry {
        cursor: leep_core::api::event_cursor(e.seq),
        kind: e.kind.clone(),
        subject: e.subject.clone(),
        principal: e.principal.clone(),
        session: e.session_id,
        created: e.created,
        facts: facts(e),
        untrusted: EventText {
            paths: e.paths.clone(),
        },
    }
}

/// About how many bytes `e` takes in an answer, at least what it does.
fn size(e: &Event) -> usize {
    256 + e.kind.len()
        + e.subject.len()
        + e.principal.len()
        + e.payload.len()
        + e.paths.iter().map(|p| p.len() + 4).sum::<usize>()
}

/// One answer's worth of events.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct EventsPage {
    pub events: Vec<Event>,
    /// The last event looked at, matched or not: where to read on from.
    pub next: u64,
    /// Whether more were there when the answer was cut.
    pub more: bool,
}

/// The answer to a read.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EventsAnswer {
    Page(EventsPage),
    /// The cursor is older than the oldest event kept, or from another
    /// store: resync, then read on from `oldest`.
    Expired {
        oldest: u64,
    },
}

/// A digest of a session's task key, sent instead of the key.
pub fn task_digest(task: &str) -> String {
    Hash::of_blob(task.as_bytes()).to_string()[..16].to_string()
}

fn digest(paths: &[String]) -> u64 {
    use std::hash::{Hash as _, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    paths.hash(&mut h);
    h.finish()
}

/// A session's paths as events carry them: scope, claims and touched
/// paths, folded to [`MAX_EVENT_PATHS`].
pub fn session_paths(s: &Session) -> Vec<String> {
    bound_paths(
        s.scope.iter().chain(&s.claims).chain(&s.touched),
        MAX_EVENT_PATHS,
    )
}

fn session_facts(s: &Session) -> EventFacts {
    EventFacts {
        task: s.task_key.as_deref().map(task_digest),
        ..Default::default()
    }
}

impl Repo {
    /// Keep events `secs` before the sweep deletes them
    /// (`--events-retention`).
    pub fn with_events_retention(self, secs: i64) -> Self {
        self.events
            .retention_secs
            .store(secs.max(1), Ordering::Relaxed);
        self
    }

    /// Admit at most `server` waiters, `per_principal` of one principal.
    pub fn with_event_waiter_caps(self, server: usize, per_principal: usize) -> Self {
        let hub = &self.events.hub;
        hub.max_waiters.store(server, Ordering::Relaxed);
        hub.max_per_principal
            .store(per_principal, Ordering::Relaxed);
        self
    }

    /// Hold `e` for the next flush. Never fails: past [`EVENT_BUFFER`] it
    /// is dropped and counted.
    pub(crate) fn emit(
        &self,
        kind: EventKind,
        session_id: Option<u64>,
        principal: &str,
        subject: String,
        paths: Vec<String>,
        facts: &EventFacts,
    ) {
        if self.coord.is_none() || self.events.off.load(Ordering::Relaxed) {
            return;
        }
        let e = NewEvent {
            kind: kind.as_str().to_string(),
            session_id,
            principal: principal.to_string(),
            subject,
            paths: bound_paths(&paths, MAX_EVENT_PATHS),
            payload: serde_json::to_string(facts).unwrap_or_else(|_| "{}".into()),
            created: unix_now(),
        };
        {
            let mut buf = self.events.buf.lock().unwrap();
            if buf.len() >= EVENT_BUFFER {
                metrics::counter!("leep_events_dropped_total", "reason" => "full").increment(1);
                return;
            }
            buf.push(e);
        }
        self.start_flusher();
    }

    /// Start the flush task, once, if there is a runtime to run it. It
    /// holds the handle weakly and ends when the handle is gone.
    fn start_flusher(&self) {
        if self.events.flusher.swap(true, Ordering::AcqRel) {
            return;
        }
        let (Ok(rt), Some(coord)) = (tokio::runtime::Handle::try_current(), self.coord.as_ref())
        else {
            self.events.flusher.store(false, Ordering::Release);
            return;
        };
        let events = Arc::downgrade(&self.events);
        let coord: Weak<dyn CoordStore> = Arc::downgrade(coord);
        rt.spawn(async move {
            let mut tick = tokio::time::interval(FLUSH_EVERY);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                let (Some(events), Some(coord)) = (events.upgrade(), coord.upgrade()) else {
                    return;
                };
                let _ = flush(&events, coord.as_ref()).await;
            }
        });
    }

    /// Append the held events now, in one call. A failure is counted and
    /// logged here, and answered only to the caller (a test, the
    /// shutdown); no request waits on it.
    pub async fn flush_events(&self) -> Result<u64> {
        match self.coord.as_ref() {
            Some(coord) => flush(&self.events, coord.as_ref()).await,
            None => Ok(0),
        }
    }

    /// Delete events older than the retention, in at most `steps` bounded
    /// steps.
    pub async fn sweep_events(&self, steps: usize) -> Result<u64> {
        let Some(coord) = self.coord.as_ref() else {
            return Ok(0);
        };
        let cutoff = unix_now() - self.events.retention_secs.load(Ordering::Relaxed);
        let mut total = 0;
        for _ in 0..steps {
            let n = coord.sweep_events(cutoff, SWEEP_STEP).await?;
            total += n;
            if n < SWEEP_STEP as u64 {
                break;
            }
        }
        Ok(total)
    }

    /// Wake every waiter and stop the tailer: the server is shutting down.
    pub fn events_shutdown(&self) {
        self.events.hub.shutdown.send_replace(true);
    }

    /// The cursor of the newest event: where a reader with no cursor
    /// starts. 404 when the store keeps no events.
    pub async fn events_tail(&self) -> Result<u64> {
        let coord = self.events_on()?;
        Ok(coord.events_bounds().await?.newest)
    }

    fn events_on(&self) -> Result<&Arc<dyn CoordStore>> {
        self.coord.as_ref().ok_or_else(leep_core::coord::no_events)
    }

    /// A place among this server's waiters for `principal`, or why not.
    /// The tailer runs while any is held.
    pub fn event_waiter(&self, principal: &str) -> std::result::Result<Waiter, Refused> {
        let hub = &self.events.hub;
        let mut st = hub.state.lock().unwrap();
        let refused = if st.waiters >= hub.max_waiters.load(Ordering::Relaxed) {
            Some(Refused::Server)
        } else if st.per.get(principal).copied().unwrap_or(0)
            >= hub.max_per_principal.load(Ordering::Relaxed)
        {
            Some(Refused::Principal)
        } else {
            None
        };
        if let Some(why) = refused {
            metrics::counter!("leep_events_refused_total", "reason" => why.as_str()).increment(1);
            return Err(why);
        }
        st.waiters += 1;
        *st.per.entry(principal.to_string()).or_default() += 1;
        metrics::gauge!("leep_event_waiters").set(st.waiters as f64);
        if !st.tailing {
            if let (Ok(rt), Some(coord)) =
                (tokio::runtime::Handle::try_current(), self.coord.clone())
            {
                st.tailing = true;
                rt.spawn(tail(self.events.clone(), coord));
            }
        }
        drop(st);
        Ok(Waiter {
            events: self.events.clone(),
            principal: principal.to_string(),
            tail: hub.tail.subscribe(),
            shutdown: hub.shutdown.subscribe(),
        })
    }

    /// The events after `after` that `filter` keeps, one bounded page,
    /// from the ring when it holds them, else from the store.
    pub async fn events_read(&self, after: u64, filter: &EventFilter) -> Result<EventsAnswer> {
        let coord = self.events_on()?;
        let ring = self.events.hub.ring.read().unwrap().after(after);
        let scanned: Vec<Arc<Event>> = match ring {
            Some(v) => v,
            None => {
                let page = coord.events_after(after, READ_PAGE).await?;
                if page.first().is_none_or(|e| e.seq > after.saturating_add(1)) {
                    let b = coord.events_bounds().await?;
                    if b.expired(after) {
                        metrics::counter!("leep_events_cursor_expired_total").increment(1);
                        let oldest = b.oldest.map_or(b.newest, |o| o - 1);
                        return Ok(EventsAnswer::Expired { oldest });
                    }
                }
                page.into_iter().map(Arc::new).collect()
            }
        };
        let full = scanned.len() >= READ_PAGE;
        let mut out = EventsPage {
            events: Vec::new(),
            next: after,
            more: false,
        };
        let mut bytes = 0;
        for e in &scanned {
            if filter.matches(e) {
                let n = size(e);
                if out.events.len() >= MAX_PAGE_EVENTS
                    || (!out.events.is_empty() && bytes + n > MAX_PAGE_BYTES)
                {
                    out.more = true;
                    return Ok(EventsAnswer::Page(out));
                }
                bytes += n;
                out.events.push(Event::clone(e));
            }
            out.next = e.seq;
        }
        out.more = full;
        Ok(EventsAnswer::Page(out))
    }

    /// [`Repo::events_read`], waiting up to `wait` (at most [`MAX_WAIT`])
    /// for something to match when `waiter` is given. Answers early on
    /// shutdown.
    pub async fn events_wait(
        &self,
        after: u64,
        filter: &EventFilter,
        wait: Duration,
        waiter: Option<&mut Waiter>,
    ) -> Result<EventsAnswer> {
        let deadline = tokio::time::Instant::now() + wait.min(MAX_WAIT);
        let mut waiter = waiter;
        if let Some(w) = waiter.as_deref_mut() {
            w.tail.borrow_and_update();
        }
        let mut after = after;
        loop {
            let page = match self.events_read(after, filter).await? {
                EventsAnswer::Page(p) => p,
                expired => return Ok(expired),
            };
            if !page.events.is_empty() {
                return Ok(EventsAnswer::Page(page));
            }
            after = page.next;
            let Some(w) = waiter.as_deref_mut() else {
                return Ok(EventsAnswer::Page(page));
            };
            if w.stopping() || tokio::time::Instant::now() >= deadline {
                return Ok(EventsAnswer::Page(page));
            }
            if page.more {
                // Nothing matched in a full page: read on at once.
                continue;
            }
            tokio::select! {
                changed = w.tail.changed() => {
                    if changed.is_err() {
                        return Ok(EventsAnswer::Page(page));
                    }
                }
                _ = w.shutdown.changed() => {}
                _ = tokio::time::sleep_until(deadline) => {}
            }
        }
    }

    /// A session was written (declared, inferred from a create): send
    /// `session.started` when `created`, else `session.paths` when its
    /// paths are not what this server last sent.
    pub(crate) fn session_written(&self, s: &Session, created: bool) {
        let paths = session_paths(s);
        let d = digest(&paths);
        let changed = {
            let mut seen = self.events.paths_seen.lock().unwrap();
            if seen.len() >= PATHS_SEEN && !seen.contains_key(&s.id) {
                seen.clear();
            }
            seen.insert(s.id, d) != Some(d)
        };
        let mut facts = session_facts(s);
        let kind = if created {
            facts.origin = Some(s.origin.as_str().to_string());
            EventKind::SessionStarted
        } else if changed {
            EventKind::SessionPaths
        } else {
            return;
        };
        self.emit(
            kind,
            Some(s.id),
            &s.principal,
            format!("session:{}", s.id),
            paths,
            &facts,
        );
    }

    /// A session ended, as `s` was when it did, with its paths.
    pub(crate) fn session_ended(
        &self,
        s: &Session,
        state: leep_core::coord::SessionState,
        landed_as: Option<leep_core::ChangesetId>,
    ) {
        self.events.paths_seen.lock().unwrap().remove(&s.id);
        let facts = EventFacts {
            state: Some(state.as_str().to_string()),
            landed_as,
            ..session_facts(s)
        };
        self.emit(
            EventKind::SessionEnded,
            Some(s.id),
            &s.principal,
            format!("session:{}", s.id),
            session_paths(s),
            &facts,
        );
    }

    /// A note was added: `note.added`, and `note.superseded` for the one
    /// it replaced.
    pub(crate) fn note_added_events(&self, n: &NoteEntry, supersedes: Option<u64>) {
        let paths = if n.anchor_type == "path" {
            vec![n.untrusted.anchor.clone()]
        } else {
            Vec::new()
        };
        let facts = EventFacts {
            note: Some(n.id),
            note_kind: Some(n.kind.clone()),
            trust: Some(n.trust.clone()),
            anchor_type: Some(n.anchor_type.clone()),
            ..Default::default()
        };
        self.emit(
            EventKind::NoteAdded,
            n.session,
            &n.principal,
            format!("note:{}", n.id),
            paths.clone(),
            &facts,
        );
        if let Some(old) = supersedes {
            let facts = EventFacts {
                note: Some(old),
                superseded_by: Some(n.id),
                ..Default::default()
            };
            self.emit(
                EventKind::NoteSuperseded,
                n.session,
                &n.principal,
                format!("note:{old}"),
                paths,
                &facts,
            );
        }
    }

    /// `by` endorsed note `n` (ADR 0098): `note.endorsed`. For the
    /// endorsement route to call once its update succeeded.
    pub fn note_endorsed_event(&self, n: &NoteEntry, by: &str) {
        let paths = if n.anchor_type == "path" {
            vec![n.untrusted.anchor.clone()]
        } else {
            Vec::new()
        };
        let facts = EventFacts {
            note: Some(n.id),
            ..Default::default()
        };
        self.emit(
            EventKind::NoteEndorsed,
            None,
            by,
            format!("note:{}", n.id),
            paths,
            &facts,
        );
    }
}

/// Append what `events` holds, one batch at a time.
async fn flush(events: &Events, coord: &dyn CoordStore) -> Result<u64> {
    let _one = events.flushing.lock().await;
    let batch = std::mem::take(&mut *events.buf.lock().unwrap());
    if batch.is_empty() {
        return Ok(0);
    }
    match coord.append_events(&batch).await {
        Ok(_) => {
            let mut by: HashMap<&str, u64> = HashMap::new();
            for e in &batch {
                *by.entry(e.kind.as_str()).or_default() += 1;
            }
            for (kind, n) in by {
                metrics::counter!("leep_events_appended_total", "kind" => kind.to_string())
                    .increment(n);
            }
            Ok(batch.len() as u64)
        }
        Err(e) => {
            let reason = if matches!(e, Error::NotFound(_)) {
                // An older schema: keep nothing from now on.
                events.off.store(true, Ordering::Relaxed);
                "off"
            } else {
                tracing::warn!(error = %e, events = batch.len(), "appending events failed");
                "error"
            };
            metrics::counter!("leep_events_dropped_total", "reason" => reason)
                .increment(batch.len() as u64);
            Err(e)
        }
    }
}

/// The tailer: reads new events into the ring every [`FLUSH_EVERY`] while
/// the server has waiters, from the newest committed when it starts.
async fn tail(events: Arc<Events>, coord: Arc<dyn CoordStore>) {
    let hub = &events.hub;
    let mut shutdown = hub.shutdown.subscribe();
    // Stop when nobody waits; under the state lock, so a new waiter
    // either sees this tailer running or starts the next.
    let idle = || {
        let mut st = hub.state.lock().unwrap();
        if st.waiters == 0 || *hub.shutdown.borrow() {
            st.tailing = false;
            hub.ring.write().unwrap().live = false;
            return true;
        }
        false
    };
    let mut last = loop {
        if idle() {
            return;
        }
        match coord.events_bounds().await {
            Ok(b) => break b.newest,
            Err(e) => {
                tracing::warn!(error = %e, "reading the events tail failed");
                metrics::counter!("leep_session_hook_errors_total", "hook" => "events_tail", "reason" => "error")
                    .increment(1);
            }
        }
        tokio::select! {
            _ = tokio::time::sleep(FLUSH_EVERY) => {}
            _ = shutdown.changed() => {}
        }
    };
    {
        let mut ring = hub.ring.write().unwrap();
        *ring = Ring {
            live: true,
            floor: last,
            last,
            events: VecDeque::new(),
        };
    }
    hub.tail.send_replace(last);
    loop {
        if idle() {
            return;
        }
        let mut full = false;
        match coord.events_after(last, READ_PAGE).await {
            Ok(page) => {
                full = page.len() >= READ_PAGE;
                if let Some(e) = page.last() {
                    last = e.seq;
                    hub.ring.write().unwrap().push(page);
                    hub.tail.send_replace(last);
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "reading events failed");
                metrics::counter!("leep_session_hook_errors_total", "hook" => "events_tail", "reason" => "error")
                    .increment(1);
            }
        }
        if full {
            continue;
        }
        tokio::select! {
            _ = tokio::time::sleep(FLUSH_EVERY) => {}
            _ = shutdown.changed() => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ev(seq: u64, kind: &str, session: Option<u64>, paths: &[&str], payload: &str) -> Event {
        Event {
            seq,
            kind: kind.into(),
            session_id: session,
            principal: "ada".into(),
            subject: format!("session:{}", session.unwrap_or(0)),
            paths: paths.iter().map(|p| p.to_string()).collect(),
            payload: payload.into(),
            created: 1,
        }
    }

    #[test]
    fn filters_by_kind_path_and_session() {
        let a = ev(1, "session.paths", Some(2), &["src/x"], "{}");
        let all = EventFilter::default();
        assert!(all.matches(&a));
        let kinds = EventFilter {
            kinds: vec!["note.added".into()],
            ..Default::default()
        };
        assert!(!kinds.matches(&a));
        let under = EventFilter {
            paths: vec!["src/x/y.rs".into()],
            ..Default::default()
        };
        assert!(under.matches(&a), "a directory event covers files below");
        let beside = EventFilter {
            paths: vec!["src/xy".into()],
            ..Default::default()
        };
        assert!(!beside.matches(&a));
        let nothing = EventFilter {
            session: SessionScope::Nothing,
            ..Default::default()
        };
        assert!(!nothing.matches(&a));
        let mine = |id: u64, task: Option<&str>| EventFilter {
            session: SessionScope::Of {
                id,
                paths: vec!["src/x/a.rs".into()],
                task: task.map(str::to_string),
            },
            ..Default::default()
        };
        assert!(mine(9, None).matches(&a), "another session on my paths");
        assert!(!mine(2, None).matches(&a), "my own session's events");
        let t = task_digest("T-1");
        let elsewhere = ev(
            3,
            "session.started",
            Some(4),
            &["docs"],
            &format!("{{\"task\":\"{t}\"}}"),
        );
        assert!(!mine(9, None).matches(&elsewhere));
        assert!(mine(9, Some(&t)).matches(&elsewhere), "same task");
        assert!(!mine(9, Some(&task_digest("T-2"))).matches(&elsewhere));
    }

    #[test]
    fn the_ring_answers_only_what_it_holds() {
        let mut r = Ring {
            live: true,
            floor: 10,
            last: 10,
            events: VecDeque::new(),
        };
        r.push(
            (11..=10 + RING as u64 + 5)
                .map(|s| ev(s, "k", None, &[], "{}"))
                .collect(),
        );
        assert_eq!(r.floor, 15);
        assert!(r.after(14).is_none(), "evicted");
        let v = r.after(15).unwrap();
        assert_eq!(v.len(), READ_PAGE);
        assert_eq!(v[0].seq, 16);
        assert!(r.after(r.last).unwrap().is_empty());
        r.live = false;
        assert!(r.after(20).is_none());
    }
}
