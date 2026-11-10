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
/// Changesets of the target ref walked for `landed`.
pub const LANDED_WINDOW: usize = 64;
/// The session's own changesets walked to find what it is based on.
pub const OWN_WALK: usize = 32;
/// Most recent active sessions compared for a similar intent.
pub const DUPLICATE_CANDIDATES: usize = 200;
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
/// Sessions whose `landed` answer is kept until the ref or head moves.
const MAX_LANDED_CACHE: usize = 10_000;

/// Which notices an answer carries: the lowest level wanted, the request's
/// own paths (probed first), and whether to look for duplicates.
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
    /// Stable within a session: (level, other session or changeset).
    pub key: String,
    pub notice: Notice,
    /// My path it is about, and whether I declared it (scope or claim),
    /// so a later scope change can be counted as following it.
    pub mine: Option<(String, bool)>,
}

/// A session's `landed` overlaps as of (target head, session head).
type LandedAt = (ChangesetId, Option<ChangesetId>, Vec<Found>);

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
    /// Session id to (target head, session head, `landed` found).
    landed: Mutex<HashMap<u64, LandedAt>>,
}

impl NoticeState {
    /// The first `max` of `found` (ranked) not said to `session` lately,
    /// marked said at `now`.
    fn pick(&self, session: u64, found: Vec<Found>, max: usize, now: i64) -> Vec<Found> {
        let mut all = self.seen.lock().unwrap();
        if !all.contains_key(&session) && all.len() >= MAX_SEEN_SESSIONS {
            // Full: forget what expired; if still full, say nothing rather
            // than repeat on every call.
            all.retain(|_, v| {
                v.retain(|s| now - s.at < SEEN_TTL_SECS);
                !v.is_empty()
            });
            if all.len() >= MAX_SEEN_SESSIONS {
                metrics::counter!("leep_notices_omitted_total", "reason" => "full").increment(1);
                return Vec::new();
            }
        }
        let seen = all.entry(session).or_default();
        seen.retain(|s| now - s.at < SEEN_TTL_SECS);
        let mut out = Vec::new();
        // Only the best-ranked overlaps are ever considered, and an
        // unexpired entry is never dropped: a session with more overlaps
        // than its memory holds hears the first ones once, not in a loop.
        for f in found.into_iter().take(MAX_SEEN_PER_SESSION) {
            if out.len() >= max || seen.len() >= MAX_SEEN_PER_SESSION {
                break;
            }
            if seen.iter().any(|s| s.key == f.key) {
                continue;
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

fn cut(s: &str, max: usize) -> String {
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
        untrusted: NoticeText {
            title: other.map_or_else(String::new, |o| cut(&o.title, NOTICE_TITLE_BYTES)),
            ..Default::default()
        },
    }
}

/// The session's paths to probe: `focus` first, then claims, scope and
/// touched paths, each once, at most [`MAX_PROBED_PATHS`]. Each with
/// whether the session declared it.
fn probed(s: &Session, focus: &[String]) -> Vec<(String, bool)> {
    let declared: BTreeSet<&str> = s
        .scope
        .iter()
        .chain(&s.claims)
        .map(String::as_str)
        .collect();
    let mut seen = BTreeSet::new();
    let mut out = Vec::new();
    let all = focus
        .iter()
        .chain(&s.claims)
        .chain(&s.scope)
        .chain(&s.touched);
    for p in all {
        if out.len() >= MAX_PROBED_PATHS {
            break;
        }
        if seen.insert(p.as_str()) {
            out.push((p.clone(), declared.contains(p.as_str())));
        }
    }
    out
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
    /// need `near_paths`; `landed` walks the target ref; duplicates (when
    /// `duplicates`) read recent sessions.
    pub async fn find_overlaps(
        &self,
        s: &Session,
        focus: &[String],
        duplicates: bool,
    ) -> Result<Vec<Found>> {
        let mut found = self.path_overlaps(s, focus).await?;
        found.extend(self.landed_overlaps(s).await?);
        if duplicates {
            found.extend(self.duplicates(s).await?);
        }
        // Stable: within a level, the order each part found them in.
        found.sort_by_key(|f| std::cmp::Reverse(f.level));
        Ok(found)
    }

    async fn path_overlaps(&self, s: &Session, focus: &[String]) -> Result<Vec<Found>> {
        let Some(coord) = self.coord.as_ref() else {
            return Ok(Vec::new());
        };
        let mine = probed(s, focus);
        if mine.is_empty() {
            return Ok(Vec::new());
        }
        let q = NearQuery {
            paths: mine.iter().map(|(p, _)| p.clone()).collect(),
            siblings: true,
            exclude: Some(s.id),
            per_range: PER_RANGE,
            limit: NEAR_LIMIT,
        };
        let rows = coord.near_paths(&q).await?;
        let now = unix_now();
        // The best (level, my path, their path) per other session.
        type Best = (NoticeLevel, usize, String);
        let mut best: BTreeMap<u64, (Session, Best)> = BTreeMap::new();
        // Sessions per my path at `same_file` or above.
        let mut crowd: BTreeMap<usize, BTreeSet<u64>> = BTreeMap::new();
        for r in rows {
            if r.session.state_at(now) == SessionState::Expired {
                continue;
            }
            for (i, (p, _)) in mine.iter().enumerate() {
                let Some(level) = path_level(p, &r.path, r.kind) else {
                    continue;
                };
                if level >= NoticeLevel::SameFile {
                    crowd.entry(i).or_default().insert(r.session.id);
                }
                let e = best
                    .entry(r.session.id)
                    .or_insert_with(|| (r.session.clone(), (level, i, r.path.clone())));
                if level > e.1 .0 {
                    e.1 = (level, i, r.path.clone());
                }
            }
        }
        let mut out = Vec::new();
        let mut hot: BTreeSet<u64> = BTreeSet::new();
        for (i, ids) in &crowd {
            if ids.len() > HOT_SESSIONS {
                let (p, declared) = &mine[*i];
                let mut n = notice(NoticeLevel::SameFile, None);
                n.count = Some(ids.len() as u64);
                n.untrusted.path = Some(p.clone());
                out.push(Found {
                    level: NoticeLevel::SameFile,
                    key: format!("hot:{p}"),
                    notice: n,
                    mine: Some((p.clone(), *declared)),
                });
                hot.extend(ids);
            }
        }
        for (id, (o, (level, i, theirs))) in best.into_iter().rev() {
            // A crowded path's sessions are its count, unless they claimed.
            if hot.contains(&id) && level < NoticeLevel::Claimed {
                continue;
            }
            let (p, declared) = &mine[i];
            let mut n = notice(level, Some(&o));
            n.untrusted.path = Some(p.clone());
            n.untrusted.other_path = Some(theirs);
            out.push(Found {
                level,
                key: format!("{}:{id}", level.as_str()),
                notice: n,
                mine: Some((p.clone(), *declared)),
            });
        }
        Ok(out)
    }

    /// Changesets on `s`'s target ref since its base that touch its paths.
    /// The base is every parent outside the session of the session's own
    /// changesets from its head (at most [`OWN_WALK`]), and its declared
    /// base; the walk down the ref's first parents stops at one of them or
    /// after [`LANDED_WINDOW`] changesets. Kept per session until the ref
    /// or the session's head moves.
    async fn landed_overlaps(&self, s: &Session) -> Result<Vec<Found>> {
        let Some(target) = s.target_ref.as_deref() else {
            return Ok(Vec::new());
        };
        if s.head.is_none() && s.base.is_none() {
            return Ok(Vec::new());
        }
        let Some(tip) = self.meta().get_ref(target).await? else {
            return Ok(Vec::new());
        };
        if let Some((t, h, found)) = self.notices.landed.lock().unwrap().get(&s.id) {
            if *t == tip && *h == s.head {
                return Ok(found.clone());
            }
        }
        let mut bases: BTreeSet<ChangesetId> = s.base.into_iter().collect();
        let mut own: BTreeSet<ChangesetId> = BTreeSet::new();
        let mut next = s.head;
        for _ in 0..OWN_WALK {
            let Some(id) = next.take() else { break };
            if !own.insert(id) {
                break;
            }
            let cs = self.get_changeset(id).await?;
            for (i, p) in cs.parents.iter().enumerate() {
                if i == 0 {
                    next = Some(*p);
                } else {
                    bases.insert(*p);
                }
            }
            if let Some(p) = next {
                let parent = self.get_changeset(p).await?;
                if parent.extra.get(EXTRA_AGENT_SESSION) != Some(&s.agent_session) {
                    bases.insert(p);
                    break;
                }
            }
        }
        let mine = probed(s, &[]);
        let mut out = Vec::new();
        let mut at = Some(tip);
        for _ in 0..LANDED_WINDOW {
            let Some(id) = at else { break };
            if bases.contains(&id) || own.contains(&id) {
                break;
            }
            let cs = self.get_changeset(id).await?;
            at = cs.parents.first().copied();
            if cs.extra.get(EXTRA_AGENT_SESSION) == Some(&s.agent_session) {
                continue;
            }
            let hit = mine
                .iter()
                .find_map(|(p, d)| changed_near(&cs, p).map(|c| (p, *d, c.to_string())));
            if let Some((p, declared, theirs)) = hit {
                let mut n = notice(NoticeLevel::Landed, None);
                n.changeset = Some(id);
                n.untrusted.title =
                    cut(&crate::sessions::title_of(&cs.message), NOTICE_TITLE_BYTES);
                n.untrusted.path = Some(p.clone());
                n.untrusted.other_path = Some(theirs);
                out.push(Found {
                    level: NoticeLevel::Landed,
                    key: format!("landed:{id}"),
                    notice: n,
                    mine: Some((p.clone(), declared)),
                });
            }
        }
        let mut cache = self.notices.landed.lock().unwrap();
        if cache.len() >= MAX_LANDED_CACHE {
            cache.clear();
        }
        cache.insert(s.id, (tip, s.head, out.clone()));
        Ok(out)
    }

    /// Live sessions that look like `s`'s task: the same task key, or an
    /// intent (title and approach) at least [`SIMILAR_PERCENT`] alike by
    /// trigrams among the [`DUPLICATE_CANDIDATES`] newest active sessions
    /// (declared ones only, on both sides).
    async fn duplicates(&self, s: &Session) -> Result<Vec<Found>> {
        let Some(coord) = self.coord.as_ref() else {
            return Ok(Vec::new());
        };
        let now = unix_now();
        let live = |o: &Session| o.id != s.id && o.state_at(now) != SessionState::Expired;
        let mut out = Vec::new();
        let mut flagged = BTreeSet::new();
        if let Some(k) = &s.task_key {
            let same = coord
                .list_sessions(&SessionFilter {
                    task_key: Some(k.clone()),
                    states: vec![SessionState::Active],
                    limit: DUPLICATE_BY_KEY + 1,
                    skip_paths: true,
                    ..Default::default()
                })
                .await?;
            for o in same.iter().filter(|o| live(o)) {
                let mut n = notice(NoticeLevel::PossibleDuplicate, Some(o));
                n.by = Some("task_key".into());
                n.untrusted.task_key = Some(k.clone());
                flagged.insert(o.id);
                out.push(Found {
                    level: NoticeLevel::PossibleDuplicate,
                    key: format!("possible_duplicate:{}", o.id),
                    notice: n,
                    mine: None,
                });
            }
        }
        // Only declared intents are compared: an inferred title is a commit
        // subject, and two commits to one file often read alike.
        let me = intent_trigrams(&s.title, &s.approach);
        if me.is_empty() || s.origin != SessionOrigin::Declared {
            return Ok(out);
        }
        let recent = coord
            .list_sessions(&SessionFilter {
                states: vec![SessionState::Active],
                limit: DUPLICATE_CANDIDATES,
                skip_paths: true,
                ..Default::default()
            })
            .await?;
        let mut similar: Vec<(u32, &Session)> = recent
            .iter()
            .filter(|o| live(o) && !flagged.contains(&o.id) && o.origin == SessionOrigin::Declared)
            .map(|o| (similarity(&me, &intent_trigrams(&o.title, &o.approach)), o))
            .filter(|(p, _)| *p >= SIMILAR_PERCENT)
            .collect();
        similar.sort_by_key(|s| std::cmp::Reverse(s.0));
        for (pct, o) in similar {
            let mut n = notice(NoticeLevel::PossibleDuplicate, Some(o));
            n.by = Some("similar".into());
            n.similarity = Some(pct);
            out.push(Found {
                level: NoticeLevel::PossibleDuplicate,
                key: format!("possible_duplicate:{}", o.id),
                notice: n,
                mine: None,
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
        let s = coord
            .get_session(id)
            .await?
            .ok_or_else(|| Error::NotFound(format!("session {id}")))?;
        let overlaps = if s.state.is_ended() {
            Vec::new()
        } else {
            self.find_overlaps(&s, &[], true)
                .await?
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
        let work = self.find_overlaps(s, &ask.focus, ask.duplicates);
        let found = match tokio::time::timeout(NOTICE_BOUND, work).await {
            Ok(Ok(f)) => f,
            Ok(Err(e)) => {
                metrics::counter!("leep_notices_omitted_total", "reason" => "error").increment(1);
                tracing::debug!(error = %e, "computing notices failed");
                return Vec::new();
            }
            Err(_) => {
                metrics::counter!("leep_notices_omitted_total", "reason" => "timeout").increment(1);
                return Vec::new();
            }
        };
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
        let sim = |a: &str, b: &str| similarity(&intent_trigrams(a, ""), &intent_trigrams(b, ""));
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
