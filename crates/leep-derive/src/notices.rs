//! Overlap notices and duplicate flags (ADR 0096): what an agent should know
//! about other live work before it lands.
//!
//! Levels, lowest first: `near` (same directory), `same_file` (the same
//! path, or a directory holding it), `claimed` (another session's claim
//! covers my path), `possible_duplicate` (same task key, or a title and
//! approach much like mine) and `landed` (a change touching my paths landed
//! on my target ref since my base). Path levels come from one
//! [`CoordStore::near_paths`] call on the `(repo_id, path)` index; `landed`
//! walks at most [`LANDED_WINDOW`] changesets of the target ref's first-parent
//! chain; duplicates read at most [`DUPLICATE_CANDIDATES`] recent sessions.
//!
//! Everything here is advisory and bounded. Nothing a land decides reads
//! it; in-band notices are computed within [`NOTICE_BOUND`] and dropped (and
//! counted) past it, so a slow coordination store costs an answer its
//! notices, never its time. Each notice is said once per session and
//! [`SEEN_TTL_SECS`] (per server), at most [`MAX_NOTICES`] per answer.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ops::Bound;
use std::sync::Mutex;
use std::time::Duration;

use leep_core::api::{Notice, NoticeText, OverlapsResponse};
use leep_core::changeset::EXTRA_AGENT_SESSION;
use leep_core::coord::{
    ancestors, parent_dir, paths_overlap, NearQuery, NoticeLevel, PathKind, Session, SessionFilter,
    SessionOrigin, SessionState,
};
use leep_core::store::unix_now;
use leep_core::{Changeset, ChangesetId, Error, RepoPath, Result};

use crate::Repo;

/// Most notices one answer carries.
pub const MAX_NOTICES: usize = 3;
/// Most overlaps `GET /v1/sessions/{id}/overlaps` lists.
pub const MAX_OVERLAPS: usize = 50;
/// How long an answer waits for its notices; past it they are left out.
pub const NOTICE_BOUND: Duration = Duration::from_millis(150);
/// How long each part of `GET /v1/sessions/{id}/overlaps` may take: a
/// question asked on purpose, so looser than [`NOTICE_BOUND`].
pub const OVERLAPS_BOUND: Duration = Duration::from_secs(2);
/// A notice said to a session is not said again for this long.
pub const SEEN_TTL_SECS: i64 = 30 * 60;
/// A scope change this soon after a notice counts as following it.
pub const FOLLOW_WINDOW_SECS: i64 = 10 * 60;
/// Most of a session's paths probed per answer: the paths of the request
/// first, then claims, scope and touched paths.
pub const MAX_PROBED_PATHS: usize = 128;
/// Most rows one range scan of `near_paths` returns, and in all.
pub const PER_RANGE: usize = 64;
pub const NEAR_LIMIT: usize = 512;
/// More sessions than this on one of my paths make it crowded: one notice
/// with a count instead of one each (design D.3, the hot-path rule).
pub const HOT_SESSIONS: usize = 50;
/// The target ref of a session that names none, for `landed`: the ref
/// clients land on by default.
pub const DEFAULT_TARGET_REF: &str = "main";
/// Changesets of the target ref walked for `landed`.
pub const LANDED_WINDOW: usize = 64;
/// The session's own changesets walked to find what it is based on.
pub const OWN_WALK: usize = 32;
/// Most recent active declared sessions compared for a similar intent.
pub const DUPLICATE_CANDIDATES: usize = 200;
/// Sessions of one task key read per answer, live and landed.
pub const KEY_SESSIONS: usize = 64;
/// Most sessions of one task key flagged.
pub const DUPLICATE_BY_KEY: usize = 20;
/// Trigram similarity (percent) of two intents from which they are flagged.
pub const SIMILAR_PERCENT: u32 = 60;
/// Intents shorter than this (normalized, in bytes) are never compared:
/// two three-word titles agree too easily.
pub const MIN_INTENT_BYTES: usize = 16;
/// Longest title repeated in a notice.
pub const NOTICE_TITLE_BYTES: usize = 80;
/// Sessions whose said notices are remembered, and notices per session.
const MAX_SEEN_SESSIONS: usize = 100_000;
const MAX_SEEN_PER_SESSION: usize = 64;
/// A full memory is purged of expired entries at most this often.
const PURGE_EVERY_SECS: i64 = 60;
/// Sessions whose `landed` answer is kept until the ref or head moves.
const MAX_LANDED_CACHE: usize = 10_000;

/// Which notices an answer carries: the lowest level wanted, the request's
/// own paths (probed first), and whether to compare intents for
/// duplicates by wording. Duplicates by task key are looked for on every
/// answer (one indexed listing), so the first of a pair hears of the
/// second on its next create, land or notice read.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NoticeAsk {
    pub min: NoticeLevel,
    pub focus: Vec<String>,
    pub duplicates: bool,
}

impl Default for NoticeAsk {
    /// `same_file` and up, the design's default minimum (F.3).
    fn default() -> Self {
        NoticeAsk {
            min: NoticeLevel::SameFile,
            focus: Vec::new(),
            duplicates: false,
        }
    }
}

/// The `Leep-Notices` header: `None` for `off`; a value not known is the
/// default.
pub fn ask_from_header(v: Option<&str>) -> Option<NoticeAsk> {
    match v.map(str::trim) {
        Some("off") => None,
        Some(l) => Some(NoticeAsk {
            min: NoticeLevel::parse(l).unwrap_or(NoticeLevel::SameFile),
            ..Default::default()
        }),
        None => Some(NoticeAsk::default()),
    }
}

/// One overlap found, before ranking and deduplication.
#[derive(Clone, Debug)]
pub struct Found {
    pub level: NoticeLevel,
    /// Stable within a session: the other session (`session:ID`), the
    /// landed changeset or the crowded path.
    pub key: String,
    pub notice: Notice,
    /// My path it is about, and whether I declared it (scope or claim),
    /// so a later scope change can be counted as following it.
    pub mine: Option<(String, bool)>,
    /// `landed`: the landed changeset's `agent.session` and the principal
    /// that created it (through server rebases), when known, to tell a
    /// duplicate's land.
    pub landed_by: Option<(String, Option<String>)>,
}

/// A session's `landed` walk, as far as it got: of the target ref at
/// `tip`, for the session's `head`.
#[derive(Clone)]
struct LandedWalk {
    tip: ChangesetId,
    head: Option<ChangesetId>,
    bases: BTreeSet<ChangesetId>,
    own: BTreeSet<ChangesetId>,
    /// The next changeset to read; `None` once done.
    at: Option<ChangesetId>,
    walked: usize,
    found: Vec<Found>,
}

impl LandedWalk {
    /// `w`'s base, walked again from the ref's new `tip`.
    fn restart(w: LandedWalk, tip: ChangesetId) -> Self {
        LandedWalk {
            tip,
            at: Some(tip),
            walked: 0,
            found: Vec::new(),
            ..w
        }
    }
}

/// Reads a first-parent chain newest first, a batch at a time: each read
/// also asks for the ancestors the store's skiplists name (ADR 0030).
struct ChainReader {
    ahead: crate::prefetch::ReadAhead,
}

impl ChainReader {
    fn new() -> Self {
        ChainReader {
            ahead: crate::prefetch::ReadAhead::new(false),
        }
    }

    /// Changeset `id`, reading ahead up to `want` more of the chain.
    async fn read(&mut self, repo: &Repo, id: ChangesetId, want: usize) -> Result<Changeset> {
        let at = self.ahead.known_generation(repo, id).unwrap_or(u64::MAX);
        self.ahead.ensure(repo, &[id], at, want.max(1)).await;
        match self.ahead.take(id).0 {
            Some(cs) => Ok(cs),
            None => repo.get_changeset(id).await,
        }
    }
}

struct Seen {
    key: String,
    level: NoticeLevel,
    at: i64,
    mine: Option<(String, bool)>,
    followed: bool,
}

/// What a server remembers about notices, shared by a repository's clones.
#[derive(Default)]
pub struct NoticeState {
    seen: Mutex<HashMap<u64, Vec<Seen>>>,
    /// When the full memory was last purged of expired entries.
    purged: Mutex<i64>,
    /// Session id to (target head, session head, `landed` found).
    landed: Mutex<HashMap<u64, LandedWalk>>,
}

impl NoticeState {
    /// The first `max` of `found` (ranked) not said to `session` lately,
    /// marked said at `now`.
    fn pick(&self, session: u64, found: Vec<Found>, max: usize, now: i64) -> Vec<Found> {
        let mut all = self.seen.lock().unwrap();
        let fresh = |s: &Seen| now - s.at < SEEN_TTL_SECS;
        let mut seen = all.remove(&session).unwrap_or_default();
        seen.retain(fresh);
        let mut out = Vec::new();
        // Only the best-ranked overlaps are ever considered, and an
        // unexpired entry is never dropped: a session with more overlaps
        // than its memory holds hears the first ones once, not in a loop.
        for f in found.into_iter().take(MAX_SEEN_PER_SESSION) {
            if out.len() >= max || seen.len() >= MAX_SEEN_PER_SESSION {
                break;
            }
            // Another session is said once at a level; again only if it
            // comes back higher (near, then a claim on my file).
            match seen.iter().position(|s| s.key == f.key) {
                Some(i) if seen[i].level >= f.level => continue,
                Some(i) => {
                    seen.remove(i);
                }
                None => {}
            }
            seen.push(Seen {
                key: f.key.clone(),
                level: f.level,
                at: now,
                mine: f.mine.clone(),
                followed: false,
            });
            out.push(f);
        }
        if seen.is_empty() {
            // Nothing to remember: no entry, so quiet sessions cost nothing.
            return out;
        }
        if all.len() >= MAX_SEEN_SESSIONS {
            // Full: forget what expired, at most once a minute (a purge
            // walks every entry under the lock); if still full, say
            // nothing new rather than repeat on every call.
            let mut last = self.purged.lock().unwrap();
            if now - *last >= PURGE_EVERY_SECS {
                *last = now;
                all.retain(|_, v| {
                    v.retain(fresh);
                    !v.is_empty()
                });
            }
            if all.len() >= MAX_SEEN_SESSIONS {
                metrics::counter!("leep_notices_omitted_total", "reason" => "full").increment(1);
                return Vec::new();
            }
        }
        all.insert(session, seen);
        out
    }

    /// Count the notices `session` followed: a declared path a notice was
    /// about is no longer declared (`declared` is the new scope and
    /// claims), or the session ended (`declared` is `None`), within
    /// [`FOLLOW_WINDOW_SECS`] of the notice. Answers how many.
    fn followed(&self, session: u64, declared: Option<&[String]>, now: i64) -> usize {
        let mut all = self.seen.lock().unwrap();
        let Some(seen) = all.get_mut(&session) else {
            return 0;
        };
        let mut n = 0;
        for s in seen.iter_mut() {
            if s.followed || now - s.at > FOLLOW_WINDOW_SECS {
                continue;
            }
            let gone = match (declared, &s.mine) {
                (None, _) => true,
                (Some(d), Some((path, true))) => !d.iter().any(|p| paths_overlap(p, path)),
                _ => false,
            };
            if gone {
                s.followed = true;
                n += 1;
                metrics::counter!("leep_notices_followed_total", "level" => s.level.as_str())
                    .increment(1);
            }
        }
        n
    }
}

/// Live sessions of `keyed` (sessions with `s`'s task key) as duplicates,
/// at most [`DUPLICATE_BY_KEY`]. The same principal under the same key is
/// `s`'s own earlier session, never flagged.
pub(crate) fn key_duplicates(s: &Session, keyed: &[Session]) -> Vec<Found> {
    let Some(k) = &s.task_key else {
        return Vec::new();
    };
    let now = unix_now();
    keyed
        .iter()
        .filter(|o| {
            o.id != s.id
                && !o.state.is_ended()
                && o.state_at(now) != SessionState::Expired
                && o.task_key.as_ref() == Some(k)
                && !same_agent_and_task(s, o)
        })
        .take(DUPLICATE_BY_KEY)
        .map(|o| {
            let mut n = notice(NoticeLevel::PossibleDuplicate, Some(o));
            n.by = Some("task_key".into());
            n.untrusted.task_key = Some(k.clone());
            Found {
                level: NoticeLevel::PossibleDuplicate,
                key: format!("session:{}", o.id),
                notice: n,
                mine: None,
                landed_by: None,
            }
        })
        .collect()
}

/// Mark each `landed` overlap that a session with `s`'s task key landed
/// (of `keyed`): `by: task_key`, naming that session and the key, so the
/// agent reads it as its duplicate's land of the task. Matched by the
/// changeset's `agent.session` and, when the server recorded one, its
/// creator; never `s`'s own principal (its own restart).
pub(crate) fn tag_duplicate_lands(s: &Session, landed: &mut [Found], keyed: &[Session]) {
    let Some(k) = &s.task_key else {
        return;
    };
    for f in landed.iter_mut() {
        let Some((key, creator)) = &f.landed_by else {
            continue;
        };
        let twin = keyed.iter().find(|o| {
            o.id != s.id
                && &o.agent_session == key
                && o.task_key.as_ref() == Some(k)
                && o.principal != s.principal
                && creator.as_ref().is_none_or(|c| c == &o.principal)
        });
        if let Some(o) = twin {
            f.notice.by = Some("task_key".into());
            f.notice.session = Some(o.id);
            f.notice.principal = Some(o.principal.clone());
            f.notice.untrusted.task_key = Some(k.clone());
        }
    }
}

/// Each other session once, at its highest level (`found` is ranked),
/// taking the paths a lower level of it named when the highest has none:
/// a duplicate that also claims my file is one notice, not two. A `landed`
/// notice is about its changeset, not the session that landed it: each
/// one passes through, and none stands for that session's live work.
pub(crate) fn one_per_session(found: Vec<Found>) -> Vec<Found> {
    let mut at: HashMap<u64, usize> = HashMap::new();
    let mut out: Vec<Found> = Vec::with_capacity(found.len());
    for f in found {
        if f.notice.changeset.is_some() {
            out.push(f);
            continue;
        }
        let Some(id) = f.notice.session else {
            out.push(f);
            continue;
        };
        match at.get(&id) {
            Some(&i) => {
                let kept = &mut out[i];
                if kept.notice.untrusted.path.is_none() {
                    kept.notice.untrusted.path = f.notice.untrusted.path;
                    kept.notice.untrusted.other_path = f.notice.untrusted.other_path;
                    kept.mine = kept.mine.take().or(f.mine);
                }
            }
            None => {
                at.insert(id, out.len());
                out.push(f);
            }
        }
    }
    out
}

/// One part of the overlaps, within `bound`; nothing (counted) if it
/// fails or runs out of time.
async fn part<T: Default>(bound: Duration, f: impl std::future::Future<Output = Result<T>>) -> T {
    match tokio::time::timeout(bound, f).await {
        Ok(Ok(found)) => found,
        Ok(Err(e)) => {
            metrics::counter!("leep_notices_omitted_total", "reason" => "error").increment(1);
            tracing::debug!(error = %e, "computing notices failed");
            T::default()
        }
        Err(_) => {
            metrics::counter!("leep_notices_omitted_total", "reason" => "timeout").increment(1);
            T::default()
        }
    }
}

pub(crate) fn cut(s: &str, max: usize) -> String {
    let mut end = s.len().min(max);
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    s[..end].to_string()
}

fn notice(level: NoticeLevel, other: Option<&Session>) -> Notice {
    Notice {
        level: level.as_str().to_string(),
        session: other.map(|o| o.id),
        principal: other.map(|o| o.principal.clone()),
        changeset: None,
        count: None,
        by: None,
        similarity: None,
        via: None,
        untrusted: NoticeText {
            title: other.map_or_else(String::new, |o| cut(&o.title, NOTICE_TITLE_BYTES)),
            ..Default::default()
        },
    }
}

/// One path probed for overlaps.
#[derive(Clone, Debug)]
struct Probe {
    path: String,
    /// The session declared it (scope or claim).
    declared: bool,
    /// Only related to the task (a context pack's co-change or search
    /// file), not the session's or the request's own.
    related: bool,
}

/// The session's paths to probe: `focus` (the request's own paths) first,
/// then claims, scope and touched paths, then `related` paths, each once,
/// at most [`MAX_PROBED_PATHS`].
fn probed(s: &Session, focus: &[String], related: &[String]) -> Vec<Probe> {
    let declared: BTreeSet<&str> = s
        .scope
        .iter()
        .chain(&s.claims)
        .map(String::as_str)
        .collect();
    let mut seen = BTreeSet::new();
    let mut out = Vec::new();
    let own = focus
        .iter()
        .chain(&s.claims)
        .chain(&s.scope)
        .chain(&s.touched)
        .map(|p| (p, false));
    for (p, related) in own.chain(related.iter().map(|p| (p, true))) {
        if out.len() >= MAX_PROBED_PATHS {
            break;
        }
        if seen.insert(p.as_str()) {
            out.push(Probe {
                path: p.clone(),
                declared: declared.contains(p.as_str()),
                related,
            });
        }
    }
    out
}

/// How a found overlap ranks: on my own paths above one found only through
/// related files, then by level. Higher first.
pub(crate) fn rank(f: &Found) -> (bool, NoticeLevel) {
    (f.notice.via.is_none(), f.level)
}

/// The level of another session's path `theirs` (of `kind`) against mine.
pub fn path_level(mine: &str, theirs: &str, kind: PathKind) -> Option<NoticeLevel> {
    if paths_overlap(mine, theirs) {
        return Some(if kind == PathKind::Claim {
            NoticeLevel::Claimed
        } else {
            NoticeLevel::SameFile
        });
    }
    match (parent_dir(mine), parent_dir(theirs)) {
        (Some(a), Some(b)) if a == b => Some(NoticeLevel::Near),
        _ => None,
    }
}

/// An intent's trigram set: lowercased, every run of other characters one
/// space. Empty when shorter than [`MIN_INTENT_BYTES`].
pub fn intent_trigrams(title: &str, approach: &str) -> Vec<u32> {
    let mut text = String::from(" ");
    for c in title.chars().chain([' ']).chain(approach.chars()) {
        if c.is_alphanumeric() {
            text.extend(c.to_lowercase());
        } else if !text.ends_with(' ') {
            text.push(' ');
        }
    }
    if !text.ends_with(' ') {
        text.push(' ');
    }
    if text.trim().len() < MIN_INTENT_BYTES {
        return Vec::new();
    }
    crate::search::trigram::extract(text.as_bytes())
}

/// What a session says it is doing, as compared for duplicates: its
/// trigrams and the paths it names (words with a `/`, or a name and an
/// extension).
#[derive(Clone, Debug, Default)]
pub struct Intent {
    grams: Vec<u32>,
    paths: BTreeSet<String>,
}

pub fn intent(title: &str, approach: &str) -> Intent {
    let paths = title
        .split_whitespace()
        .chain(approach.split_whitespace())
        .map(|w| w.trim_matches(|c: char| !c.is_alphanumeric() && c != '_'))
        .filter(|w| {
            w.contains('/')
                || w.split_once('.').is_some_and(|(a, b)| {
                    !a.is_empty() && !b.is_empty() && b.chars().all(char::is_alphanumeric)
                })
        })
        .map(str::to_lowercase)
        .collect();
    Intent {
        grams: intent_trigrams(title, approach),
        paths,
    }
}

/// How alike two intents are, in percent: trigram similarity, but none
/// when both name paths and share none. A template filled in with other
/// files ("Fix flaky test in <path>") is other work, however long the
/// shared words.
pub fn alike(a: &Intent, b: &Intent) -> u32 {
    if !a.paths.is_empty() && !b.paths.is_empty() && a.paths.is_disjoint(&b.paths) {
        return 0;
    }
    similarity(&a.grams, &b.grams)
}

/// The same agent on the same task under another session key (a
/// restart): the same principal and task key. It is not told about
/// itself.
fn same_agent_and_task(s: &Session, o: &Session) -> bool {
    s.task_key.is_some() && o.task_key == s.task_key && o.principal == s.principal
        || same_session(s, o)
}

/// `o` is `s` itself, by key rather than id: a context pack whose lookup of
/// the caller's session gave up stands one in (id 0) under the same
/// principal and key, and must not find the caller as its own overlap.
fn same_session(s: &Session, o: &Session) -> bool {
    !s.agent_session.is_empty() && o.agent_session == s.agent_session && o.principal == s.principal
}

/// Jaccard similarity of two sorted trigram sets, in percent.
pub fn similarity(a: &[u32], b: &[u32]) -> u32 {
    if a.is_empty() || b.is_empty() {
        return 0;
    }
    let (mut i, mut j, mut both) = (0, 0, 0usize);
    while i < a.len() && j < b.len() {
        match a[i].cmp(&b[j]) {
            std::cmp::Ordering::Less => i += 1,
            std::cmp::Ordering::Greater => j += 1,
            std::cmp::Ordering::Equal => {
                both += 1;
                i += 1;
                j += 1;
            }
        }
    }
    let either = a.len() + b.len() - both;
    (both * 100 / either) as u32
}

/// The first changed path of `cs` overlapping `p`: `p` itself, a directory
/// above it, or a path below it.
fn changed_near<'a>(cs: &'a Changeset, p: &str) -> Option<&'a str> {
    let at = |q: &str| {
        RepoPath::new(q)
            .ok()
            .and_then(|k| cs.changes.get_key_value(&k).map(|(k, _)| k.as_str()))
    };
    if let Some(hit) = at(p).or_else(|| ancestors(p).into_iter().find_map(at)) {
        return Some(hit);
    }
    let start = RepoPath::new(p).ok()?;
    cs.changes
        .range((Bound::Excluded(start), Bound::Unbounded))
        .map(|(k, _)| k.as_str())
        .take_while(|k| k.starts_with(p))
        .find(|k| k.as_bytes().get(p.len()) == Some(&b'/'))
}

impl Repo {
    /// Answers carry notices (`true`, the default) or not (`--notices
    /// off`); explicit overlap queries answer either way.
    pub fn with_notices(mut self, on: bool) -> Self {
        self.notices_on = on;
        self
    }

    pub fn notices_on(&self) -> bool {
        self.notices_on && self.coord.is_some()
    }

    /// Everything overlapping `s` now, highest level first. Path levels
    /// need `near_paths`; `landed` walks the target ref; duplicates by
    /// task key read one indexed listing, and by wording (when
    /// `duplicates`) recent sessions. The parts run together, each within
    /// `bound`: one that fails or runs out of time is left out (and
    /// counted), never the others.
    pub async fn find_overlaps(
        &self,
        s: &Session,
        focus: &[String],
        duplicates: bool,
        bound: Duration,
    ) -> Vec<Found> {
        let similar = async {
            if duplicates {
                part(bound, self.similar_duplicates(s)).await
            } else {
                Vec::new()
            }
        };
        let (paths, mut landed, keyed, similar) = tokio::join!(
            part(bound, self.path_overlaps(s, focus, &[])),
            part(bound, self.landed_overlaps(s)),
            part(bound, self.key_sessions(s)),
            similar,
        );
        tag_duplicate_lands(s, &mut landed, &keyed);
        let mut found = paths;
        found.extend(landed);
        // By key before by wording: one session flagged both ways is one
        // notice, `by: task_key`.
        found.extend(key_duplicates(s, &keyed));
        found.extend(similar);
        // Stable: within a level, the order each part found them in.
        found.sort_by_key(|f| std::cmp::Reverse(rank(f)));
        one_per_session(found)
    }

    /// Path levels of other live sessions against `s`'s paths, `focus`
    /// (the request's own) first. A session met only on a `related` path
    /// is marked `via: related` and ranks below every one met on mine:
    /// its level says how it meets a file related to the task, never mine.
    pub(crate) async fn path_overlaps(
        &self,
        s: &Session,
        focus: &[String],
        related: &[String],
    ) -> Result<Vec<Found>> {
        let Some(coord) = self.coord.as_ref() else {
            return Ok(Vec::new());
        };
        let mine = probed(s, focus, related);
        if mine.is_empty() {
            return Ok(Vec::new());
        }
        let q = NearQuery {
            paths: mine.iter().map(|p| p.path.clone()).collect(),
            siblings: true,
            exclude: Some(s.id),
            per_range: PER_RANGE,
            limit: NEAR_LIMIT,
        };
        let rows = coord.near_paths(&q).await?;
        let now = unix_now();
        // Each row with my first path it meets and how.
        let mut met: Vec<(&leep_core::coord::NearRow, NoticeLevel, usize)> = Vec::new();
        // Sessions at `same_file` or above per path of theirs: a crowded
        // path is one notice, whichever and however many of mine it holds.
        let mut crowd: BTreeMap<&str, (usize, BTreeSet<u64>)> = BTreeMap::new();
        for r in &rows {
            if r.session.state_at(now) == SessionState::Expired
                || same_agent_and_task(s, &r.session)
            {
                continue;
            }
            // My own paths first, whatever a related file's level.
            let best = mine
                .iter()
                .enumerate()
                .filter_map(|(i, p)| path_level(&p.path, &r.path, r.kind).map(|l| (l, i)))
                .max_by_key(|(l, i)| (!mine[*i].related, *l, std::cmp::Reverse(*i)));
            let Some((level, i)) = best else { continue };
            if level >= NoticeLevel::SameFile && !mine[i].related {
                let e = crowd.entry(r.path.as_str()).or_insert((i, BTreeSet::new()));
                e.1.insert(r.session.id);
            }
            met.push((r, level, i));
        }
        let mut out = Vec::new();
        let mut hot: BTreeSet<&str> = BTreeSet::new();
        for (theirs, (i, ids)) in &crowd {
            if ids.len() > HOT_SESSIONS {
                let p = &mine[*i];
                let mut n = notice(NoticeLevel::SameFile, None);
                n.count = Some(ids.len() as u64);
                n.untrusted.path = Some(p.path.clone());
                n.untrusted.other_path = Some(theirs.to_string());
                out.push(Found {
                    level: NoticeLevel::SameFile,
                    key: format!("hot:{theirs}"),
                    notice: n,
                    mine: Some((p.path.clone(), p.declared)),
                    landed_by: None,
                });
                hot.insert(theirs);
            }
        }
        // The best (level, my path, their path) per other session, leaving
        // out its rows on crowded paths (their count) unless they claim:
        // a session that also meets me on an uncrowded path is named there.
        // A row on one of my paths beats any on a related file.
        type Best = ((bool, NoticeLevel), usize, String);
        let mut best: BTreeMap<u64, (&Session, Best)> = BTreeMap::new();
        for (r, level, i) in met {
            if hot.contains(r.path.as_str()) && level < NoticeLevel::Claimed {
                continue;
            }
            let at = (!mine[i].related, level);
            let e = best
                .entry(r.session.id)
                .or_insert_with(|| (&r.session, (at, i, r.path.clone())));
            if at > e.1 .0 {
                e.1 = (at, i, r.path.clone());
            }
        }
        for (id, (o, ((_, level), i, theirs))) in best.into_iter().rev() {
            let p = &mine[i];
            let mut n = notice(level, Some(o));
            n.untrusted.path = Some(p.path.clone());
            n.untrusted.other_path = Some(theirs);
            if p.related {
                n.via = Some("related".into());
            }
            out.push(Found {
                level,
                key: format!("session:{id}"),
                notice: n,
                mine: Some((p.path.clone(), p.declared)),
                landed_by: None,
            });
        }
        Ok(out)
    }

    /// Changesets on `s`'s target ref since its base that touch its paths.
    /// The base is every parent outside the session of the session's own
    /// changesets from its head (at most [`OWN_WALK`]), and its declared
    /// base; the walk down the ref's first parents stops at one of them or
    /// after [`LANDED_WINDOW`] changesets. Both walks read ahead in batches
    /// (ADR 0030). Progress is kept per session as it goes, so a walk cut
    /// short by the notice bound resumes on the next answer; it is redone
    /// when the ref or the session's head moves.
    pub(crate) async fn landed_overlaps(&self, s: &Session) -> Result<Vec<Found>> {
        // A session that never said where it lands (inferred from an older
        // client's commit) is taken to land on the default ref.
        let target = s.target_ref.as_deref().unwrap_or(DEFAULT_TARGET_REF);
        if s.head.is_none() && s.base.is_none() {
            return Ok(Vec::new());
        }
        let Some(tip) = self.meta().get_ref(target).await? else {
            return Ok(Vec::new());
        };
        let kept = self.notices.landed.lock().unwrap().get(&s.id).cloned();
        let mut w = match kept {
            Some(w) if w.head == s.head && w.tip == tip => w,
            // The same work on a moved ref: its base stands, the walk does not.
            Some(w) if w.head == s.head => LandedWalk::restart(w, tip),
            _ => {
                let Some((bases, own)) = self.landed_bases(s).await? else {
                    return Ok(Vec::new());
                };
                LandedWalk {
                    tip,
                    head: s.head,
                    bases,
                    own,
                    at: Some(tip),
                    walked: 0,
                    found: Vec::new(),
                }
            }
        };
        let mine = probed(s, &[], &[]);
        let mut reader = ChainReader::new();
        while let Some(id) = w.at {
            if w.walked >= LANDED_WINDOW || w.bases.contains(&id) || w.own.contains(&id) {
                w.at = None;
                break;
            }
            let cs = reader.read(self, id, LANDED_WINDOW - w.walked).await?;
            w.walked += 1;
            w.at = cs.parents.first().copied();
            if !self.is_own(s, id, &cs).await? {
                let hit = mine.iter().find_map(|p| {
                    changed_near(&cs, &p.path).map(|c| (&p.path, p.declared, c.to_string()))
                });
                if let Some((p, declared, theirs)) = hit {
                    // Who landed it, to tell a duplicate's land (read only
                    // for a hit; kept with the walk).
                    let landed_by = match cs.extra.get(EXTRA_AGENT_SESSION) {
                        Some(k) => Some((
                            k.clone(),
                            self.created_by_through_rebases(id)
                                .await
                                .ok()
                                .flatten()
                                .map(|c| c.principal),
                        )),
                        None => None,
                    };
                    let mut n = notice(NoticeLevel::Landed, None);
                    n.changeset = Some(id);
                    n.untrusted.title =
                        cut(&crate::sessions::title_of(&cs.message), NOTICE_TITLE_BYTES);
                    n.untrusted.path = Some(p.clone());
                    n.untrusted.other_path = Some(theirs);
                    w.found.push(Found {
                        level: NoticeLevel::Landed,
                        key: format!("landed:{id}"),
                        notice: n,
                        mine: Some((p.clone(), declared)),
                        landed_by,
                    });
                }
            }
            // Kept as it goes: a walk the bound cuts short resumes here.
            self.keep_landed(s.id, &w);
        }
        self.keep_landed(s.id, &w);
        Ok(w.found)
    }

    fn keep_landed(&self, session: u64, w: &LandedWalk) {
        let mut cache = self.notices.landed.lock().unwrap();
        if cache.len() >= MAX_LANDED_CACHE && !cache.contains_key(&session) {
            cache.clear();
        }
        cache.insert(session, w.clone());
    }

    /// Whether `cs` (`id`) is the session's own work: it carries the
    /// session's `agent.session` and its creator is the session's
    /// principal (when the server recorded one), so another principal's
    /// work under the same key is not hidden.
    async fn is_own(&self, s: &Session, id: ChangesetId, cs: &Changeset) -> Result<bool> {
        if cs.extra.get(EXTRA_AGENT_SESSION) != Some(&s.agent_session) {
            return Ok(false);
        }
        Ok(self
            .created_by(id)
            .await?
            .is_none_or(|c| c.principal == s.principal))
    }

    /// What the session's work is based on: its declared base and the
    /// parents outside the session of its own changesets from its head, at
    /// most [`OWN_WALK`]; with those own changesets. A changeset that is
    /// gone (collected) ends the walk there; `None` when nothing is known
    /// to stop the `landed` walk at.
    async fn landed_bases(
        &self,
        s: &Session,
    ) -> Result<Option<(BTreeSet<ChangesetId>, BTreeSet<ChangesetId>)>> {
        let mut bases: BTreeSet<ChangesetId> = s.base.into_iter().collect();
        let mut own: BTreeSet<ChangesetId> = BTreeSet::new();
        let mut reader = ChainReader::new();
        let mut next = s.head;
        let mut first = true;
        while let Some(id) = next.take() {
            if own.len() >= OWN_WALK || !own.insert(id) {
                break;
            }
            let cs = match reader.read(self, id, OWN_WALK).await {
                Ok(cs) => cs,
                Err(Error::NotFound(_)) => {
                    own.remove(&id);
                    break;
                }
                Err(e) => return Err(e),
            };
            if !first && cs.extra.get(EXTRA_AGENT_SESSION) != Some(&s.agent_session) {
                // Not the session's: its base.
                own.remove(&id);
                bases.insert(id);
                break;
            }
            first = false;
            for (i, p) in cs.parents.iter().enumerate() {
                if i == 0 {
                    next = Some(*p);
                } else {
                    bases.insert(*p);
                }
            }
        }
        if let Some(p) = next {
            bases.insert(p);
        }
        Ok((!bases.is_empty()).then_some((bases, own)))
    }

    /// Live sessions that look like `s`'s task: the same task key, or an
    /// intent (title and approach) at least [`SIMILAR_PERCENT`] alike by
    /// trigrams among the [`DUPLICATE_CANDIDATES`] newest active sessions
    /// (declared ones only, on both sides).
    pub(crate) async fn duplicates(&self, s: &Session) -> Result<Vec<Found>> {
        let mut out = key_duplicates(s, &self.key_sessions(s).await?);
        let flagged: BTreeSet<u64> = out.iter().filter_map(|f| f.notice.session).collect();
        out.extend(
            self.similar_duplicates(s)
                .await?
                .into_iter()
                .filter(|f| f.notice.session.is_none_or(|id| !flagged.contains(&id))),
        );
        Ok(out)
    }

    /// Sessions with `s`'s task key, live or landed, newest first, at
    /// most [`KEY_SESSIONS`]: one indexed listing, none without a key.
    /// Live ones are duplicates; landed ones tell a duplicate's land.
    pub(crate) async fn key_sessions(&self, s: &Session) -> Result<Vec<Session>> {
        let (Some(coord), Some(k)) = (self.coord.as_ref(), &s.task_key) else {
            return Ok(Vec::new());
        };
        coord
            .list_sessions(&SessionFilter {
                task_key: Some(k.clone()),
                states: vec![SessionState::Active, SessionState::Landed],
                limit: KEY_SESSIONS,
                skip_paths: true,
                ..Default::default()
            })
            .await
    }

    /// Live declared sessions whose title and approach are at least
    /// [`SIMILAR_PERCENT`] alike to `s`'s, among the
    /// [`DUPLICATE_CANDIDATES`] newest.
    pub(crate) async fn similar_duplicates(&self, s: &Session) -> Result<Vec<Found>> {
        let Some(coord) = self.coord.as_ref() else {
            return Ok(Vec::new());
        };
        let now = unix_now();
        let live = |o: &Session| {
            o.id != s.id && o.state_at(now) != SessionState::Expired && !same_agent_and_task(s, o)
        };
        let mut out = Vec::new();
        // Only declared intents are compared: an inferred title is a commit
        // subject, and two commits to one file often read alike.
        let me = intent(&s.title, &s.approach);
        if me.grams.is_empty() || s.origin != SessionOrigin::Declared {
            return Ok(out);
        }
        let recent = coord
            .list_sessions(&SessionFilter {
                states: vec![SessionState::Active],
                origin: Some(SessionOrigin::Declared),
                limit: DUPLICATE_CANDIDATES,
                skip_paths: true,
                ..Default::default()
            })
            .await?;
        let mut similar: Vec<(u32, &Session)> = recent
            .iter()
            .filter(|o| live(o))
            .map(|o| (alike(&me, &intent(&o.title, &o.approach)), o))
            .filter(|(p, _)| *p >= SIMILAR_PERCENT)
            .collect();
        similar.sort_by_key(|s| std::cmp::Reverse(s.0));
        for (pct, o) in similar {
            let mut n = notice(NoticeLevel::PossibleDuplicate, Some(o));
            n.by = Some("similar".into());
            n.similarity = Some(pct);
            out.push(Found {
                level: NoticeLevel::PossibleDuplicate,
                key: format!("session:{}", o.id),
                notice: n,
                mine: None,
                landed_by: None,
            });
        }
        Ok(out)
    }

    /// `GET /v1/sessions/{id}/overlaps`: everything overlapping session
    /// `id` now, every level and duplicates, highest first, none held back.
    pub async fn session_overlaps(&self, id: u64) -> Result<OverlapsResponse> {
        let coord = self
            .coord
            .as_ref()
            .ok_or_else(|| Error::NotFound("sessions: this server keeps none".into()))?;
        let s = tokio::time::timeout(OVERLAPS_BOUND, coord.get_session(id))
            .await
            .map_err(|_| Error::Other("sessions: the coordination store did not answer".into()))??
            .ok_or_else(|| Error::NotFound(format!("session {id}")))?;
        let overlaps = if s.state.is_ended() {
            Vec::new()
        } else {
            self.find_overlaps(&s, &[], true, OVERLAPS_BOUND)
                .await
                .into_iter()
                .take(MAX_OVERLAPS)
                .map(|f| f.notice)
                .collect()
        };
        Ok(OverlapsResponse {
            session: id,
            overlaps,
        })
    }

    /// The notices an answer carries for session `s`: at least `ask.min`,
    /// not said to it lately, at most [`MAX_NOTICES`], highest first.
    /// Never fails and never waits past [`NOTICE_BOUND`]: an error or a
    /// slow store leaves the notices out, counted in
    /// `leep_notices_omitted_total`.
    pub async fn notices_for_session(&self, s: &Session, ask: &NoticeAsk) -> Vec<Notice> {
        if !self.notices_on() || s.state.is_ended() {
            return Vec::new();
        }
        let found = self
            .find_overlaps(s, &ask.focus, ask.duplicates, NOTICE_BOUND)
            .await;
        let found: Vec<Found> = found.into_iter().filter(|f| f.level >= ask.min).collect();
        let picked = self.notices.pick(s.id, found, MAX_NOTICES, unix_now());
        picked
            .into_iter()
            .map(|f| {
                metrics::counter!("leep_overlap_notices_total", "level" => f.level.as_str())
                    .increment(1);
                if f.level == NoticeLevel::PossibleDuplicate {
                    let by = if f.notice.by.as_deref() == Some("task_key") {
                        "task_key"
                    } else {
                        "similar"
                    };
                    metrics::counter!("leep_duplicate_sessions_flagged_total", "by" => by)
                        .increment(1);
                }
                f.notice
            })
            .collect()
    }

    /// [`Repo::notices_for_session`] for `principal`'s live session of
    /// `key`, looked up within the same bound; nothing when there is none.
    pub async fn notices_for(&self, principal: &str, key: &str, ask: &NoticeAsk) -> Vec<Notice> {
        if !self.notices_on() {
            return Vec::new();
        }
        let Some(coord) = self.coord.as_ref() else {
            return Vec::new();
        };
        let filter = SessionFilter {
            principal: Some(principal.to_string()),
            agent_session: Some(key.to_string()),
            states: vec![SessionState::Active],
            limit: 1,
            ..Default::default()
        };
        let started = tokio::time::Instant::now();
        let s = match tokio::time::timeout(NOTICE_BOUND, coord.list_sessions(&filter)).await {
            Ok(Ok(mut v)) => match v.pop() {
                Some(s) => s,
                None => return Vec::new(),
            },
            Ok(Err(_)) => {
                metrics::counter!("leep_notices_omitted_total", "reason" => "error").increment(1);
                return Vec::new();
            }
            Err(_) => {
                metrics::counter!("leep_notices_omitted_total", "reason" => "timeout").increment(1);
                return Vec::new();
            }
        };
        let left = NOTICE_BOUND.saturating_sub(started.elapsed());
        match tokio::time::timeout(left, self.notices_for_session(&s, ask)).await {
            Ok(n) => n,
            Err(_) => {
                metrics::counter!("leep_notices_omitted_total", "reason" => "timeout").increment(1);
                Vec::new()
            }
        }
    }

    /// A session changed its declared paths (`Some`) or ended (`None`):
    /// count the recent notices it followed (`leep_notices_followed_total`).
    pub fn note_scope_change(&self, session: u64, declared: Option<&[String]>) -> usize {
        self.notices.followed(session, declared, unix_now())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn levels_by_path() {
        use NoticeLevel::*;
        let l = |a, b, k| path_level(a, b, k);
        assert_eq!(l("src/a.rs", "src/a.rs", PathKind::Touched), Some(SameFile));
        assert_eq!(l("src/a.rs", "src", PathKind::Scope), Some(SameFile));
        assert_eq!(l("src", "src/x/y.rs", PathKind::Touched), Some(SameFile));
        assert_eq!(l("src/a.rs", "src", PathKind::Claim), Some(Claimed));
        assert_eq!(l("src/a.rs", "src/b.rs", PathKind::Touched), Some(Near));
        assert_eq!(l("src/a.rs", "src/b.rs", PathKind::Claim), Some(Near));
        assert_eq!(l("src/a.rs", "src/x/b.rs", PathKind::Touched), None);
        assert_eq!(l("a.rs", "b.rs", PathKind::Touched), None, "top level");
        assert_eq!(l("src/a", "src/ab", PathKind::Touched), Some(Near));
        assert!(Landed > PossibleDuplicate && PossibleDuplicate > Claimed);
        assert!(Claimed > SameFile && SameFile > Near);
    }

    #[test]
    fn the_header_picks_the_minimum() {
        assert_eq!(ask_from_header(None).unwrap().min, NoticeLevel::SameFile);
        assert!(ask_from_header(Some("off")).is_none());
        assert_eq!(
            ask_from_header(Some("near")).unwrap().min,
            NoticeLevel::Near
        );
        assert_eq!(
            ask_from_header(Some("bogus")).unwrap().min,
            NoticeLevel::SameFile
        );
    }

    fn found(level: NoticeLevel, key: &str) -> Found {
        Found {
            level,
            key: key.into(),
            notice: notice(level, None),
            mine: Some(("src/a.rs".into(), true)),
            landed_by: None,
        }
    }

    #[test]
    fn notices_are_said_once_and_capped() {
        let st = NoticeState::default();
        let all = || {
            vec![
                found(NoticeLevel::Landed, "l"),
                found(NoticeLevel::Claimed, "c"),
                found(NoticeLevel::SameFile, "s1"),
                found(NoticeLevel::SameFile, "s2"),
            ]
        };
        let keys = |v: Vec<Found>| v.into_iter().map(|f| f.key).collect::<Vec<_>>();
        assert_eq!(keys(st.pick(1, all(), 3, 1000)), ["l", "c", "s1"]);
        assert_eq!(keys(st.pick(1, all(), 3, 1001)), ["s2"], "the rest, once");
        assert!(st.pick(1, all(), 3, 1002).is_empty());
        assert_eq!(keys(st.pick(2, all(), 3, 1002)).len(), 3, "per session");
        // Said again once the memory expires.
        assert_eq!(keys(st.pick(1, all(), 3, 1001 + SEEN_TTL_SECS)).len(), 3);
    }

    /// More overlaps than a session's memory holds: each is said once,
    /// never again while unexpired, however many answers follow.
    #[test]
    fn a_full_memory_never_repeats_a_notice() {
        let st = NoticeState::default();
        let all = || {
            (0..70)
                .map(|i| found(NoticeLevel::SameFile, &format!("s{i}")))
                .collect::<Vec<_>>()
        };
        let mut said = std::collections::BTreeMap::new();
        for t in 0..60 {
            for f in st.pick(1, all(), 3, 1000 + t) {
                *said.entry(f.key).or_insert(0) += 1;
            }
        }
        assert!(said.values().all(|n| *n == 1), "{said:?}");
        assert!(said.contains_key("s0") && said.len() <= MAX_SEEN_PER_SESSION);
    }

    /// A session said at one level is not said again at that level or
    /// below, through any answer; it is when it comes back higher.
    #[test]
    fn a_session_is_said_again_only_higher() {
        let st = NoticeState::default();
        let keys = |v: Vec<Found>| v.into_iter().map(|f| f.level).collect::<Vec<_>>();
        assert_eq!(
            keys(st.pick(
                1,
                vec![found(NoticeLevel::PossibleDuplicate, "session:9")],
                3,
                1000
            ))
            .len(),
            1
        );
        assert!(st
            .pick(1, vec![found(NoticeLevel::SameFile, "session:9")], 3, 1001)
            .is_empty());
        let st = NoticeState::default();
        st.pick(1, vec![found(NoticeLevel::Near, "session:9")], 3, 1000);
        assert_eq!(
            keys(st.pick(1, vec![found(NoticeLevel::Claimed, "session:9")], 3, 1001)),
            [NoticeLevel::Claimed]
        );
        assert!(st
            .pick(1, vec![found(NoticeLevel::Claimed, "session:9")], 3, 1002)
            .is_empty());
    }

    /// Answers that say nothing leave nothing behind, so sessions without
    /// overlaps never fill the memory.
    #[test]
    fn saying_nothing_keeps_nothing() {
        let st = NoticeState::default();
        for id in 0..100 {
            assert!(st.pick(id, Vec::new(), 3, 1000).is_empty());
        }
        st.pick(7, vec![found(NoticeLevel::SameFile, "s")], 3, 1000);
        assert!(st
            .pick(7, vec![found(NoticeLevel::SameFile, "s")], 3, 1001)
            .is_empty());
        assert_eq!(st.seen.lock().unwrap().len(), 1);
        // Once its notices expire, the session's entry goes too.
        st.pick(7, Vec::new(), 3, 1000 + SEEN_TTL_SECS);
        assert!(st.seen.lock().unwrap().is_empty());
    }

    #[test]
    fn a_narrowed_scope_follows_a_notice() {
        let st = NoticeState::default();
        st.pick(1, vec![found(NoticeLevel::SameFile, "s")], 3, 1000);
        assert_eq!(
            st.followed(1, Some(&["src".into()]), 1100),
            0,
            "still there"
        );
        assert_eq!(st.followed(1, Some(&["docs".into()]), 1100), 1);
        assert_eq!(st.followed(1, None, 1100), 0, "counted once");
        st.pick(1, vec![found(NoticeLevel::Claimed, "c")], 3, 2000);
        assert_eq!(
            st.followed(1, None, 2000 + FOLLOW_WINDOW_SECS + 1),
            0,
            "too late"
        );
        st.pick(1, vec![found(NoticeLevel::Near, "n")], 3, 5000);
        assert_eq!(st.followed(1, None, 5001), 1, "ending follows");
    }

    #[test]
    fn similar_intents_and_near_misses() {
        let sim = |a: &str, b: &str| alike(&intent(a, ""), &intent(b, ""));
        let same = sim(
            "Fix the flaky login test in the auth module",
            "fix flaky login test in auth module",
        );
        assert!(same >= SIMILAR_PERCENT, "{same}");
        let reworded = sim(
            "Retry transient blob store errors on upload",
            "Retry transient blob-store errors during upload",
        );
        assert!(reworded >= SIMILAR_PERCENT, "{reworded}");
        let miss = sim(
            "Fix the flaky login test in the auth module",
            "Fix the flaky logout test in the billing module",
        );
        assert!(miss < SIMILAR_PERCENT, "{miss}");
        let other = sim("Add a cache for blame results", "Fix the flaky login test");
        assert!(other < 20, "{other}");
        assert_eq!(sim("fix bug", "fix bug"), 0, "too short to compare");
        // A shared path or template is not a shared task.
        for (a, b) in [
            (
                "Fix flaky test in crates/leep-server/tests/notices.rs",
                "Fix flaky test in crates/leep-server/tests/sessions.rs",
            ),
            (
                "Fix flaky test in crates/leep-server/tests/land_forwarding.rs",
                "Fix flaky test in crates/leep-server/tests/land_retries.rs",
            ),
            (
                "Bump the version of crates/leep-meta/Cargo.toml",
                "Bump the version of crates/leep-store/Cargo.toml",
            ),
        ] {
            let p = sim(a, b);
            assert!(p < SIMILAR_PERCENT, "{p}: {a:?} {b:?}");
        }
        // The same path and goal still are.
        let p = sim(
            "Fix the flaky test in crates/leep-server/tests/notices.rs",
            "fix flaky test crates/leep-server/tests/notices.rs",
        );
        assert!(p >= SIMILAR_PERCENT, "{p}");
    }

    #[test]
    fn landed_paths_match_by_component() {
        let content = leep_core::Hash::of_blob(b"x");
        let cs = Changeset {
            parents: vec![],
            author: "a".into(),
            timestamp: 1,
            message: "m".into(),
            changes: ["src/auth/token.rs", "src/authx/a.rs", "src-b/c.rs"]
                .into_iter()
                .map(|p| {
                    (
                        RepoPath::new(p).unwrap(),
                        leep_core::FileChange::Modify {
                            content,
                            kind: leep_core::FileKind::Regular,
                            size: 1,
                        },
                    )
                })
                .collect(),
            extra: Default::default(),
        };
        assert_eq!(changed_near(&cs, "src/auth"), Some("src/auth/token.rs"));
        assert_eq!(
            changed_near(&cs, "src/auth/token.rs"),
            Some("src/auth/token.rs")
        );
        assert_eq!(changed_near(&cs, "src"), Some("src/auth/token.rs"));
        assert_eq!(changed_near(&cs, "src/aut"), None);
        assert_eq!(changed_near(&cs, "docs"), None);
        assert_eq!(
            changed_near(&cs, "src/authx/a.rs/x"),
            Some("src/authx/a.rs")
        );
    }
}
