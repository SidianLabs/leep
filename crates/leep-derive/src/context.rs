//! Context packs (ADR 0097): what the server already knows that bears on a
//! task, assembled without a model and cut to a budget.
//!
//! Sections, in the order the pack lists them: files (the request's paths,
//! files where identifiers from the task occur, files that changed with
//! the paths), owners and `carry` directives, the newest change to each
//! file, live work overlapping the files, the ref's health and possible
//! duplicates of the task. Every part reuses a query the server already
//! answers (search, path history, `OWNERS`, last change, overlaps, checks),
//! each within its own time bound: a part that fails or runs out of time is
//! listed as omitted and the rest still comes. The whole call is bounded by
//! [`CONTEXT_BOUND`].
//!
//! A pack is a read. It writes nothing, marks no notice as said and never
//! touches the land path. Given the same request and the same state it is
//! the same bytes: every list is ranked by a total order and nothing in it
//! depends on the time of the call.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::time::Duration;

use futures_util::StreamExt;
use leep_core::api::{
    ContextChange, ContextCut, ContextFile, ContextOmitted, ContextOwners, ContextPack,
    ContextRefHealth, ContextRequest, ContextSuspect, ContextText, ContextWork, Notice,
    CONTEXT_DEFAULT_BUDGET, CONTEXT_MAX_BUDGET, CONTEXT_MAX_PATHS, CONTEXT_MAX_TASK_BYTES,
    CONTEXT_MIN_BUDGET,
};
use leep_core::changeset::{EXTRA_AGENT_MODEL, EXTRA_AGENT_SESSION};
use leep_core::coord::{
    check_path, normalize_path, paths_overlap, Session, SessionFilter, SessionOrigin, SessionState,
    MAX_APPROACH_BYTES, MAX_TITLE_BYTES,
};
use leep_core::store::unix_now;
use leep_core::{ChangesetId, Error, Hash, RepoPath, Result};
use serde::Serialize;
use tokio::time::Instant;

use crate::indexing::Freshness;
use crate::notices::{cut, one_per_session, Found, MAX_OVERLAPS, NOTICE_TITLE_BYTES};
use crate::owners::{CarryDirective, OwnersResolver};
use crate::{LogParams, Repo, SearchQuery};

/// The longest a context pack takes, whatever its parts do.
pub const CONTEXT_BOUND: Duration = Duration::from_secs(2);
/// The longest one section takes.
pub const SECTION_BOUND: Duration = Duration::from_secs(1);
/// Looking up the caller's session.
pub const SESSION_LOOKUP_BOUND: Duration = Duration::from_millis(250);
/// Identifiers from the task searched for, and matching lines kept of each.
pub const MAX_IDENTIFIERS: usize = 4;
pub const SEARCH_MATCHES: usize = 50;
/// Blob bytes one identifier's search may read before it stops.
pub const SEARCH_SCAN_BYTES: u64 = 32 << 20;
/// Files a search, or the co-change count, may add.
pub const SEARCH_FILES: usize = 32;
pub const CO_CHANGE_FILES: usize = 32;
/// Paths whose history is read for co-changes, and changesets read in all.
pub const CO_CHANGE_SEEDS: usize = 8;
pub const CO_CHANGE_WINDOW: usize = 200;
/// A changeset changing more paths than this (an import, a rename sweep)
/// says nothing about which files go together, and is not counted.
pub const CO_CHANGE_MAX_PATHS: usize = 1000;
/// Files in a pack, and files whose owners and last change are read.
pub const MAX_FILES: usize = 64;
pub const DETAIL_FILES: usize = 32;
/// Possible duplicates and suspects listed.
pub const MAX_DUPLICATES: usize = 20;
pub const MAX_SUSPECTS: usize = 5;
/// A suspect's paths repeated: the ones meeting the pack's files.
pub const SUSPECT_PATHS: usize = 8;
/// Last-change reads in flight at once.
const HISTORY_CONCURRENCY: usize = 8;

/// The sections, in the order the pack lists them, each with its share of
/// the budget in percent. A section that needs less leaves the rest to the
/// others, in [`SPARE_ORDER`].
pub const SECTIONS: [(&str, u64); 6] = [
    ("files", 25),
    ("owners", 10),
    ("history", 20),
    ("in_flight", 20),
    ("ref_health", 5),
    ("duplicates", 20),
];
/// Which sections take unused budget first: what only the server knows
/// (ref health, a single small item that says whether the ref is red;
/// live work; duplicates) before what a client can find itself.
const SPARE_ORDER: [usize; 6] = [4, 3, 5, 0, 2, 1];
/// Most owner names, and `OWNERS` files, an owners item lists.
pub const MAX_OWNERS: usize = 16;
/// Longest agent-written name (model, session key, owner) an item repeats.
pub const NAME_BYTES: usize = 80;

/// A context request as the server hands it over: the caller, and the
/// checks policy that names required checks (server configuration).
#[derive(Clone, Debug, Default)]
pub struct ContextAsk {
    pub principal: String,
    pub req: ContextRequest,
    /// The checks `ref` requires before its green moves.
    pub required_checks: Vec<String>,
    /// Every check name the server knows, for red when none is required.
    pub check_names: Vec<String>,
}

/// Why a part is missing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Omit {
    Timeout,
    Error,
    Off,
}

impl Omit {
    fn as_str(self) -> &'static str {
        match self {
            Omit::Timeout => "timeout",
            Omit::Error => "error",
            Omit::Off => "off",
        }
    }
}

type Part<T> = std::result::Result<T, Omit>;

/// `f` until `until`: its answer, or why there is none (counted).
async fn bounded<T>(
    name: &'static str,
    until: Instant,
    f: impl Future<Output = Result<T>>,
) -> Part<T> {
    match tokio::time::timeout_at(until, f).await {
        Ok(Ok(v)) => Ok(v),
        Ok(Err(e)) => {
            tracing::debug!(section = name, error = %e, "context section failed");
            Err(Omit::Error)
        }
        Err(_) => Err(Omit::Timeout),
    }
}

/// A section's items, or none with the reason noted in `omitted`.
fn take<T>(omitted: &mut Vec<(&'static str, Omit)>, name: &'static str, p: Part<Vec<T>>) -> Vec<T> {
    p.unwrap_or_else(|o| {
        omitted.push((name, o));
        Vec::new()
    })
}

/// `file:` (or another kind) and 12 hex digits naming `key`: stable,
/// and safe to print whatever `key` holds.
fn item_id(kind: &str, key: &str) -> String {
    let h = Hash::of_object("context-item", key.as_bytes()).to_string();
    format!("{kind}:{}", &h[..12])
}

/// The size of `item`'s JSON once `set` has written that size into it.
fn sized<T: Serialize>(mut item: T, set: impl Fn(&mut T, u64)) -> T {
    set(&mut item, 0);
    // The JSON with a one-digit size, less that digit.
    let base = serde_json::to_vec(&item)
        .map_or(0, |v| v.len() as u64)
        .saturating_sub(1);
    let digits = |n: u64| n.to_string().len() as u64;
    let mut n = base + 1;
    // A fixed point: the size counts its own digits.
    for _ in 0..3 {
        n = base + digits(n);
    }
    set(&mut item, n);
    item
}

/// Identifiers in a task worth a code search: words in backticks, and
/// words that look like code (`snake_case`, `camelCase`, `HTTPServer`),
/// 3 to 64 bytes, starting with a letter or `_`,
/// at most [`MAX_IDENTIFIERS`], in the order they first appear.
pub fn identifiers(task: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let mut push = |w: &str| {
        let ok = (3..=64).contains(&w.len())
            && w.starts_with(|c: char| c.is_ascii_alphabetic() || c == '_')
            && !out.iter().any(|o| o == w);
        if ok && out.len() < MAX_IDENTIFIERS {
            out.push(w.to_string());
        }
    };
    let words = |s: &str| -> Vec<String> {
        s.split(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
            .filter(|w| !w.is_empty())
            .map(str::to_string)
            .collect()
    };
    // Backticked spans first: the task's author marked them as code.
    let mut quoted = BTreeSet::new();
    for (i, span) in task.split('`').enumerate() {
        if i % 2 == 1 {
            for w in words(span) {
                quoted.insert(w.clone());
                push(&w);
            }
        }
    }
    for w in words(task) {
        if quoted.contains(&w) {
            continue;
        }
        let snake = w.trim_matches('_').contains('_');
        let b = w.as_bytes();
        // `camelCase`, or an acronym running into a word (`HTTPServer`).
        let camel = b
            .windows(2)
            .any(|p| p[0].is_ascii_lowercase() && p[1].is_ascii_uppercase())
            || b.windows(3).any(|p| {
                p[0].is_ascii_uppercase() && p[1].is_ascii_uppercase() && p[2].is_ascii_lowercase()
            });
        if snake || camel {
            push(&w);
        }
    }
    out
}

/// A request checked and filled in: paths normalized, ref and budget
/// defaulted.
struct Checked {
    task: String,
    paths: Vec<String>,
    target_ref: String,
    budget: u64,
}

fn check(req: &ContextRequest) -> Result<Checked> {
    if req.task.len() > CONTEXT_MAX_TASK_BYTES {
        return Err(Error::Invalid(format!(
            "context task is {} bytes; the limit is {CONTEXT_MAX_TASK_BYTES}",
            req.task.len()
        )));
    }
    if req.paths.len() > CONTEXT_MAX_PATHS {
        return Err(Error::Invalid(format!(
            "context names {} paths; the limit is {CONTEXT_MAX_PATHS}",
            req.paths.len()
        )));
    }
    let mut paths: Vec<String> = Vec::new();
    for p in &req.paths {
        let p = normalize_path(p);
        if p.is_empty() {
            continue;
        }
        check_path(&p)?;
        if !paths.contains(&p) {
            paths.push(p);
        }
    }
    let target_ref = req
        .target_ref
        .as_deref()
        .map(str::trim)
        .filter(|r| !r.is_empty())
        .unwrap_or("main")
        .to_string();
    leep_core::refname::validate(&target_ref)?;
    let budget = req
        .budget_bytes
        .unwrap_or(CONTEXT_DEFAULT_BUDGET)
        .clamp(CONTEXT_MIN_BUDGET, CONTEXT_MAX_BUDGET);
    Ok(Checked {
        task: req.task.trim().to_string(),
        paths,
        target_ref,
        budget,
    })
}

/// The task as a session's intent: its first line the title, the rest the
/// approach, each cut to the session limits.
fn task_intent(task: &str) -> (String, String) {
    let (title, rest) = task.split_once('\n').unwrap_or((task, ""));
    (
        cut(title.trim(), MAX_TITLE_BYTES),
        cut(rest.trim(), MAX_APPROACH_BYTES),
    )
}

/// One file of the pack before it gets an id and a size.
#[derive(Default)]
struct FileRank {
    seed: Option<usize>,
    matches: u64,
    co_changes: u64,
}

/// What the files parts found.
#[derive(Default)]
struct Files {
    /// Ranked: seeds in request order, then by matches and co-changes,
    /// then by path.
    ranked: Vec<(String, FileRank)>,
}

impl Files {
    fn rank(seeds: &[String], search: &BTreeMap<String, u64>, co: &BTreeMap<String, u64>) -> Files {
        let mut all: BTreeMap<String, FileRank> = BTreeMap::new();
        for (i, s) in seeds.iter().enumerate() {
            all.entry(s.clone()).or_default().seed = Some(i);
        }
        let top = |m: &BTreeMap<String, u64>, n: usize| {
            let mut v: Vec<(&String, u64)> = m.iter().map(|(p, c)| (p, *c)).collect();
            v.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(b.0)));
            v.truncate(n);
            v.into_iter()
                .map(|(p, c)| (p.clone(), c))
                .collect::<Vec<_>>()
        };
        for (p, n) in top(search, SEARCH_FILES) {
            all.entry(p).or_default().matches = n;
        }
        for (p, n) in top(co, CO_CHANGE_FILES) {
            all.entry(p).or_default().co_changes = n;
        }
        let mut ranked: Vec<(String, FileRank)> = all.into_iter().collect();
        ranked.sort_by(|(pa, a), (pb, b)| {
            let key = |r: &FileRank| {
                (
                    r.seed.is_none(),
                    r.seed.unwrap_or(0),
                    std::cmp::Reverse(r.matches + r.co_changes),
                )
            };
            key(a).cmp(&key(b)).then_with(|| pa.cmp(pb))
        });
        ranked.truncate(MAX_FILES);
        Files { ranked }
    }

    fn paths(&self) -> Vec<String> {
        self.ranked.iter().map(|(p, _)| p.clone()).collect()
    }
}

/// Each section's items kept, in rank order, so their bytes total at most
/// `budget`: first each takes a prefix within its quota, then the budget
/// left is offered, in [`SPARE_ORDER`], to the next items of each.
/// Answers how many of each to keep.
pub fn keep_within(sizes: &[Vec<u64>; 6], budget: u64) -> [usize; 6] {
    let mut kept = [0usize; 6];
    let mut used = [0u64; 6];
    for (i, (_, pct)) in SECTIONS.iter().enumerate() {
        let quota = budget * pct / 100;
        for b in &sizes[i] {
            if used[i] + b > quota {
                break;
            }
            used[i] += b;
            kept[i] += 1;
        }
    }
    let mut left = budget - used.iter().sum::<u64>();
    for i in SPARE_ORDER {
        for b in &sizes[i][kept[i]..] {
            if *b > left {
                break;
            }
            left -= b;
            kept[i] += 1;
        }
    }
    kept
}

impl Repo {
    /// `POST /v1/context` (ADR 0097): the pack for `ask`. `Invalid` for a
    /// bad request, `NotFound` when the ref does not exist; anything a
    /// section meets after that is reported in `omitted`, never returned.
    pub async fn context_pack(&self, ask: &ContextAsk) -> Result<ContextPack> {
        let started = Instant::now();
        let deadline = started + CONTEXT_BOUND;
        let c = check(&ask.req)?;
        let session_key = ask
            .req
            .session
            .as_deref()
            .map(str::trim)
            .filter(|k| !k.is_empty());
        if c.task.is_empty() && session_key.is_none() {
            return Err(Error::Invalid(
                "a context pack needs a task, or a session whose title says it".into(),
            ));
        }
        let head = self
            .meta()
            .get_ref(&c.target_ref)
            .await?
            .ok_or_else(|| Error::NotFound(format!("ref {:?}", c.target_ref)))?;
        let mut omitted: Vec<(&'static str, Omit)> = Vec::new();

        // The caller's session: first when the task comes from it, else
        // beside the files parts.
        let lookup = self.caller_session(&ask.principal, session_key, deadline);
        let (session, files) = if c.task.is_empty() {
            let session = lookup.await;
            let task = session
                .as_ref()
                .map(|s| format!("{}\n{}", s.title, s.approach))
                .unwrap_or_default();
            if task.trim().is_empty() {
                return Err(Error::Invalid(
                    "a context pack needs a task, or a session whose title says it".into(),
                ));
            }
            let files = self.files_parts(&task, &c.paths, head, deadline).await;
            (session, files)
        } else {
            tokio::join!(lookup, self.files_parts(&c.task, &c.paths, head, deadline))
        };
        let (search, co) = files;
        let search = search.unwrap_or_else(|o| {
            omitted.push(("search", o));
            BTreeMap::new()
        });
        let co = co.unwrap_or_else(|o| {
            omitted.push(("co_change", o));
            BTreeMap::new()
        });
        let files = Files::rank(&c.paths, &search, &co);
        let paths = files.paths();
        let detail: Vec<String> = paths.iter().take(DETAIL_FILES).cloned().collect();

        let task_text = if c.task.is_empty() {
            session
                .as_ref()
                .map(|s| format!("{}\n{}", s.title, s.approach))
                .unwrap_or_default()
        } else {
            c.task.clone()
        };
        let me = self.context_session(ask, &c, session.clone(), session_key, &task_text);
        let section_until = || deadline.min(Instant::now() + SECTION_BOUND);
        let (owners, history, in_flight, health, dups) = tokio::join!(
            bounded(
                "owners",
                section_until(),
                self.context_owners(head, &detail)
            ),
            bounded(
                "history",
                section_until(),
                self.context_history(head, &detail)
            ),
            self.context_in_flight(&me, &paths, section_until()),
            bounded(
                "ref_health",
                section_until(),
                self.context_ref_health(ask, &c.target_ref, &paths)
            ),
            self.context_duplicates(&me, section_until()),
        );
        let owners = take(&mut omitted, "owners", owners);
        let history = take(&mut omitted, "history", history);
        let in_flight = take(&mut omitted, "in_flight", in_flight);
        let duplicates = take(&mut omitted, "duplicates", dups);
        let (in_flight, duplicates) = merge_duplicates(in_flight, duplicates);
        let ref_health: Vec<ContextRefHealth> = match health {
            Ok(h) => vec![h],
            Err(o) => {
                omitted.push(("ref_health", o));
                Vec::new()
            }
        };
        let files: Vec<ContextFile> = files
            .ranked
            .into_iter()
            .map(|(path, r)| {
                let mut why = Vec::new();
                if r.seed.is_some() {
                    why.push("seed".to_string());
                }
                if r.matches > 0 {
                    why.push("search".to_string());
                }
                if r.co_changes > 0 {
                    why.push("co_change".to_string());
                }
                sized(
                    ContextFile {
                        id: item_id("file", &path),
                        bytes: 0,
                        why,
                        matches: r.matches,
                        co_changes: r.co_changes,
                        untrusted: ContextText {
                            path: Some(path),
                            ..Default::default()
                        },
                    },
                    |i, n| i.bytes = n,
                )
            })
            .collect();
        let mut pack = ContextPack {
            target_ref: c.target_ref.clone(),
            head,
            session: session.as_ref().map(|s| s.id),
            budget_bytes: c.budget,
            bytes: 0,
            files,
            owners,
            history,
            in_flight,
            ref_health: None,
            duplicates,
            cut: Vec::new(),
            omitted: Vec::new(),
        };
        let mut ref_health = ref_health;
        // Suspects dropped, newest kept, so that the ref's state (red or
        // not) fits rather than the whole item going.
        let (mut suspects_cut, mut suspect_bytes) = (0u64, 0u64);
        let (sizes, kept) = loop {
            let sizes: [Vec<u64>; 6] = [
                pack.files.iter().map(|i| i.bytes).collect(),
                pack.owners.iter().map(|i| i.bytes).collect(),
                pack.history.iter().map(|i| i.bytes).collect(),
                pack.in_flight.iter().map(|i| i.bytes).collect(),
                ref_health.iter().map(|i| i.bytes).collect(),
                pack.duplicates.iter().map(|i| i.bytes).collect(),
            ];
            let kept = keep_within(&sizes, c.budget);
            match ref_health.first_mut() {
                Some(h) if kept[4] == 0 && !h.suspects.is_empty() => {
                    let before = h.bytes;
                    h.suspects.pop();
                    *h = sized(h.clone(), |i, n| i.bytes = n);
                    suspects_cut += 1;
                    suspect_bytes += before.saturating_sub(h.bytes);
                }
                _ => break (sizes, kept),
            }
        };
        if suspects_cut > 0 {
            metrics::counter!("leep_context_items_cut_total", "section" => "suspects")
                .increment(suspects_cut);
            pack.cut.push(ContextCut {
                section: "suspects".to_string(),
                items: suspects_cut,
                bytes: suspect_bytes,
            });
        }
        for (i, (name, _)) in SECTIONS.iter().enumerate() {
            let dropped = &sizes[i][kept[i]..];
            if !dropped.is_empty() {
                metrics::counter!("leep_context_items_cut_total", "section" => *name)
                    .increment(dropped.len() as u64);
                pack.cut.push(ContextCut {
                    section: name.to_string(),
                    items: dropped.len() as u64,
                    bytes: dropped.iter().sum(),
                });
            }
            pack.bytes += sizes[i][..kept[i]].iter().sum::<u64>();
        }
        pack.files.truncate(kept[0]);
        pack.owners.truncate(kept[1]);
        pack.history.truncate(kept[2]);
        pack.in_flight.truncate(kept[3]);
        ref_health.truncate(kept[4]);
        pack.ref_health = ref_health.pop();
        pack.duplicates.truncate(kept[5]);
        // In section order, so the same failures read the same.
        omitted.sort_by_key(|(name, _)| {
            ["search", "co_change"]
                .iter()
                .chain(SECTIONS.iter().map(|(n, _)| n))
                .position(|n| n == name)
        });
        for (name, why) in omitted {
            metrics::counter!(
                "leep_context_sections_omitted_total",
                "section" => name,
                "reason" => why.as_str()
            )
            .increment(1);
            pack.omitted.push(ContextOmitted {
                section: name.to_string(),
                reason: why.as_str().to_string(),
            });
        }
        metrics::counter!("leep_context_packs_total").increment(1);
        metrics::counter!("leep_context_bytes_total").increment(pack.bytes);
        Ok(pack)
    }

    /// `principal`'s live session of `key`, within its bound; `None` when
    /// there is none, no key, or the store does not answer.
    async fn caller_session(
        &self,
        principal: &str,
        key: Option<&str>,
        deadline: Instant,
    ) -> Option<Session> {
        let (coord, key) = (self.coord.as_ref()?, key?);
        let filter = SessionFilter {
            principal: Some(principal.to_string()),
            agent_session: Some(key.to_string()),
            states: vec![SessionState::Active],
            limit: 1,
            ..Default::default()
        };
        let until = deadline.min(Instant::now() + SESSION_LOOKUP_BOUND);
        bounded("session", until, coord.list_sessions(&filter))
            .await
            .ok()?
            .pop()
    }

    /// The session overlaps and duplicates are found for: the caller's,
    /// else one standing for the request (id 0, never stored). The task
    /// text is its intent either way.
    fn context_session(
        &self,
        ask: &ContextAsk,
        c: &Checked,
        session: Option<Session>,
        key: Option<&str>,
        task: &str,
    ) -> Session {
        let now = unix_now();
        let (title, approach) = task_intent(task);
        // The stand-in carries the caller's key, so a session of the caller
        // the lookup missed (it gave up, or the store is slow) is still
        // never its own overlap or duplicate.
        let mut s = session.unwrap_or_else(|| Session {
            id: 0,
            principal: ask.principal.clone(),
            agent_session: key.unwrap_or_default().to_string(),
            agent_model: None,
            title: String::new(),
            approach: String::new(),
            task_key: None,
            target_ref: Some(c.target_ref.clone()),
            base: None,
            head: None,
            origin: SessionOrigin::Declared,
            state: SessionState::Active,
            created: now,
            last_seen: now,
            ended: None,
            landed_as: None,
            scope: Vec::new(),
            claims: Vec::new(),
            touched: Vec::new(),
        });
        if !title.is_empty() {
            s.title = title;
            s.approach = approach;
            s.origin = SessionOrigin::Declared;
        }
        // The request's task key wins, as its task text does.
        let asked = ask
            .req
            .task_key
            .as_deref()
            .map(str::trim)
            .filter(|k| !k.is_empty());
        if let Some(k) = asked {
            s.task_key = Some(k.to_string());
        }
        s
    }

    /// Search and co-change, side by side, each within the section bound.
    async fn files_parts(
        &self,
        task: &str,
        seeds: &[String],
        head: ChangesetId,
        deadline: Instant,
    ) -> (Part<BTreeMap<String, u64>>, Part<BTreeMap<String, u64>>) {
        let until = deadline.min(Instant::now() + SECTION_BOUND);
        tokio::join!(
            bounded("search", until, self.context_search(task, head)),
            bounded("co_change", until, self.co_changes(seeds, head)),
        )
    }

    /// Matching lines per path for each identifier of the task, searched
    /// as a literal at `head` through the index (stale answers allowed:
    /// a pack never waits for the indexer).
    async fn context_search(&self, task: &str, head: ChangesetId) -> Result<BTreeMap<String, u64>> {
        let rev = head.to_string();
        let searches = identifiers(task).into_iter().map(|ident| {
            let q = SearchQuery {
                limit: SEARCH_MATCHES,
                max_scan_bytes: SEARCH_SCAN_BYTES,
                ..SearchQuery::new(ident)
            };
            let rev = rev.clone();
            async move {
                self.search_page_indexed(&rev, &q, None, Freshness::StaleOk)
                    .await
            }
        });
        let mut hits: BTreeMap<String, u64> = BTreeMap::new();
        for r in futures_util::future::join_all(searches).await {
            for m in r?.value.matches {
                *hits.entry(m.path.as_str().to_string()).or_default() += 1;
            }
        }
        Ok(hits)
    }

    /// Files changed together with the seeds: how many of each seed's
    /// newest changesets (path history, [`CO_CHANGE_WINDOW`] in all, each
    /// counted once) changed them, seeds and paths below them left out.
    async fn co_changes(
        &self,
        seeds: &[String],
        head: ChangesetId,
    ) -> Result<BTreeMap<String, u64>> {
        let seeds: Vec<&String> = seeds.iter().take(CO_CHANGE_SEEDS).collect();
        let mut out: BTreeMap<String, u64> = BTreeMap::new();
        if seeds.is_empty() {
            return Ok(out);
        }
        let per = (CO_CHANGE_WINDOW / seeds.len()).max(1);
        let mut counted: BTreeSet<ChangesetId> = BTreeSet::new();
        for seed in &seeds {
            let params = LogParams {
                path: Some(seed.to_string()),
                ..Default::default()
            };
            let page = match self
                .log_page_indexed(head, &params, per, None, Freshness::StaleOk)
                .await
            {
                Ok(p) => p.value,
                // A seed that does not exist yet has no history.
                Err(Error::NotFound(_)) | Err(Error::Invalid(_)) => continue,
                Err(e) => return Err(e),
            };
            for info in page.entries {
                if !counted.insert(info.id) || info.changeset.changes.len() > CO_CHANGE_MAX_PATHS {
                    continue;
                }
                for p in info.changeset.changes.keys() {
                    let p = p.as_str();
                    if seeds.iter().any(|s| paths_overlap(s, p)) {
                        continue;
                    }
                    *out.entry(p.to_string()).or_default() += 1;
                }
            }
        }
        Ok(out)
    }

    /// Owners and `carry` directives of `paths` as of `head`, for the paths
    /// that have either.
    async fn context_owners(
        &self,
        head: ChangesetId,
        paths: &[String],
    ) -> Result<Vec<ContextOwners>> {
        let mut r = OwnersResolver::new(self, Some(head)).await?;
        let mut out = Vec::new();
        for p in paths {
            let Ok(path) = RepoPath::new(p.as_str()) else {
                continue;
            };
            let owners = r.owners_of(&path).await?;
            let carry = r.carry_of(&path).await?;
            if owners.is_empty() && carry.is_none() {
                continue;
            }
            let files = r.owners_files_of(&path).await?;
            let (carry, carry_file) = match carry {
                Some((d, f)) => (
                    Some(
                        match d {
                            CarryDirective::Identical => "identical",
                            CarryDirective::Merged => "merged",
                        }
                        .to_string(),
                    ),
                    Some(f),
                ),
                None => (None, None),
            };
            out.push(sized(
                ContextOwners {
                    id: item_id("owners", p),
                    bytes: 0,
                    carry,
                    untrusted: ContextText {
                        path: Some(p.clone()),
                        owners: owners
                            .iter()
                            .take(MAX_OWNERS)
                            .map(|o| cut(o, NAME_BYTES))
                            .collect(),
                        owners_files: files.iter().take(MAX_OWNERS).cloned().collect(),
                        carry_file,
                        ..Default::default()
                    },
                },
                |i, n| i.bytes = n,
            ));
        }
        Ok(out)
    }

    /// The newest change to each of `paths` at `head`, and who made it.
    /// Paths that never existed are left out.
    async fn context_history(
        &self,
        head: ChangesetId,
        paths: &[String],
    ) -> Result<Vec<ContextChange>> {
        let reads = futures_util::stream::iter(paths.iter().cloned())
            .map(|p| async move {
                match self.last_change_indexed(head, &p, Freshness::StaleOk).await {
                    Ok(a) => {
                        let (info, concurrent) = a.value;
                        let by = self.created_by_through_rebases(info.id).await?;
                        Ok(Some((p, info, concurrent.len() as u64, by)))
                    }
                    Err(Error::NotFound(_)) | Err(Error::Invalid(_)) => Ok(None),
                    Err(e) => Err(e),
                }
            })
            .buffered(HISTORY_CONCURRENCY);
        let found: Vec<Result<_>> = reads.collect().await;
        let mut out = Vec::new();
        for f in found {
            let Some((p, info, concurrent, by)) = f? else {
                continue;
            };
            let cs = &info.changeset;
            let title = crate::sessions::title_of(&cs.message);
            out.push(sized(
                ContextChange {
                    id: item_id("change", &p),
                    bytes: 0,
                    changeset: info.id,
                    time: cs.timestamp,
                    created_by: by.map(|b| b.principal),
                    concurrent,
                    untrusted: ContextText {
                        path: Some(p),
                        title: Some(cut(&title, NOTICE_TITLE_BYTES)),
                        author: Some(cut(&cs.author, NOTICE_TITLE_BYTES)),
                        agent_model: cs.extra.get(EXTRA_AGENT_MODEL).map(|m| cut(m, NAME_BYTES)),
                        agent_session: cs
                            .extra
                            .get(EXTRA_AGENT_SESSION)
                            .map(|k| cut(k, NAME_BYTES)),
                        ..Default::default()
                    },
                },
                |i, n| i.bytes = n,
            ));
        }
        Ok(out)
    }

    /// Live work overlapping the pack's files (and the session's own
    /// paths), each other session once at its highest level, plus lands
    /// on the target ref since the session's base. Nothing is marked
    /// said. The path and `landed` parts are bounded together.
    async fn context_in_flight(
        &self,
        s: &Session,
        paths: &[String],
        until: Instant,
    ) -> Part<Vec<ContextWork>> {
        if self.coord.is_none() {
            return Err(Omit::Off);
        }
        let work = async {
            let (p, l) = tokio::join!(self.path_overlaps(s, paths), self.landed_overlaps(s));
            let mut found = p?;
            found.extend(l?);
            found.sort_by_key(|f| std::cmp::Reverse(f.level));
            Ok(one_per_session(found))
        };
        let found = bounded("in_flight", until, work).await?;
        Ok(works(found, MAX_OVERLAPS))
    }

    /// Live sessions that may be doing this task: the same task key, or a
    /// title and approach much like the task (ADR 0096).
    async fn context_duplicates(&self, s: &Session, until: Instant) -> Part<Vec<ContextWork>> {
        if self.coord.is_none() {
            return Err(Omit::Off);
        }
        let found = bounded("duplicates", until, self.duplicates(s)).await?;
        Ok(works(found, MAX_DUPLICATES))
    }

    /// The ref's known-good state and red, and the suspects of a red head
    /// that touch `paths`.
    async fn context_ref_health(
        &self,
        ask: &ContextAsk,
        name: &str,
        paths: &[String],
    ) -> Result<ContextRefHealth> {
        let names = if ask.required_checks.is_empty() {
            &ask.check_names
        } else {
            &ask.required_checks
        };
        let r = self.ref_checks(name, &ask.required_checks, names).await?;
        let mut suspects = Vec::new();
        if r.red.is_some() && !paths.is_empty() {
            let c = self
                .culprits(name, None, None, None, leep_core::api::MAX_SUSPECTS)
                .await?;
            for s in c.suspects {
                let hit: Vec<String> = s
                    .paths
                    .iter()
                    .map(|p| p.as_str())
                    .filter(|p| paths.iter().any(|q| paths_overlap(p, q)))
                    .take(SUSPECT_PATHS)
                    .map(str::to_string)
                    .collect();
                if hit.is_empty() {
                    continue;
                }
                suspects.push(ContextSuspect {
                    changeset: s.changeset,
                    seq: s.seq,
                    actor: s.actor,
                    time: s.time,
                    created_by: s.created_by,
                    untrusted: ContextText {
                        title: Some(cut(&s.title, NOTICE_TITLE_BYTES)),
                        author: Some(cut(&s.author, NOTICE_TITLE_BYTES)),
                        agent_model: s.agent_model.map(|m| cut(&m, NAME_BYTES)),
                        paths: hit,
                        ..Default::default()
                    },
                });
                if suspects.len() >= MAX_SUSPECTS {
                    break;
                }
            }
        }
        Ok(sized(
            ContextRefHealth {
                id: format!(
                    "ref:{}",
                    &Hash::of_object("context-item", name.as_bytes()).to_string()[..12]
                ),
                bytes: 0,
                head: r.head,
                green_ref: r.green_ref,
                green: r.green,
                behind: r.behind,
                red: r.red,
                required_checks: r.required,
                suspects,
            },
            |i, n| i.bytes = n,
        ))
    }
}

/// Each other session once: a session that is a possible duplicate and
/// also works on the pack's files is listed under `duplicates` only,
/// taking the paths its in-flight item named when it has none (as one
/// notice is, ADR 0096), so it is never charged to the budget twice.
fn merge_duplicates(
    in_flight: Vec<ContextWork>,
    duplicates: Vec<ContextWork>,
) -> (Vec<ContextWork>, Vec<ContextWork>) {
    let mut paths: BTreeMap<u64, (Option<String>, Option<String>)> = BTreeMap::new();
    let dup_ids: BTreeSet<u64> = duplicates.iter().filter_map(|d| d.notice.session).collect();
    let in_flight: Vec<ContextWork> = in_flight
        .into_iter()
        .filter(|w| match w.notice.session {
            Some(id) if dup_ids.contains(&id) => {
                let t = &w.notice.untrusted;
                paths.insert(id, (t.path.clone(), t.other_path.clone()));
                false
            }
            _ => true,
        })
        .collect();
    let duplicates = duplicates
        .into_iter()
        .map(|d| {
            let Some((path, other)) = d.notice.session.and_then(|id| paths.remove(&id)) else {
                return d;
            };
            if d.notice.untrusted.path.is_some() {
                return d;
            }
            let mut notice = d.notice;
            notice.untrusted.path = path;
            notice.untrusted.other_path = other;
            work(d.id, notice)
        })
        .collect();
    (in_flight, duplicates)
}

/// Found overlaps as pack items, at most `max`, with ids that name what
/// they are about.
fn works(found: Vec<Found>, max: usize) -> Vec<ContextWork> {
    found
        .into_iter()
        .take(max)
        .map(|f| {
            let id = match (&f.notice.session, &f.notice.changeset) {
                (Some(s), _) => format!("session:{s}"),
                (None, Some(c)) => format!("landed:{}", &c.to_string()[..12]),
                _ => item_id("crowd", &f.key),
            };
            work(id, f.notice)
        })
        .collect()
}

fn work(id: String, notice: Notice) -> ContextWork {
    sized(
        ContextWork {
            id,
            bytes: 0,
            notice,
        },
        |i, n| i.bytes = n,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identifiers_are_code_looking_words() {
        assert_eq!(
            identifiers("Fix parse_token in the `LandCoordinator`, and retryAfter too"),
            ["LandCoordinator", "parse_token", "retryAfter"]
        );
        assert_eq!(
            identifiers("Make the flaky login test pass"),
            Vec::<String>::new()
        );
        assert_eq!(
            identifiers("`land` `land` a_b c_d e_f g_h i_j"),
            ["land", "a_b", "c_d", "e_f"],
            "deduplicated, at most four"
        );
        assert_eq!(identifiers("__ _x 9_lives"), Vec::<String>::new());
        assert_eq!(
            identifiers("HTTPServer and IOError"),
            ["HTTPServer", "IOError"]
        );
    }

    #[test]
    fn sizes_count_their_own_digits() {
        let w = work(
            "session:1".into(),
            Notice {
                level: "near".into(),
                session: Some(1),
                principal: None,
                changeset: None,
                count: None,
                by: None,
                similarity: None,
                untrusted: Default::default(),
            },
        );
        assert_eq!(w.bytes, serde_json::to_vec(&w).unwrap().len() as u64);
        let long = sized(
            ContextFile {
                id: "file:x".into(),
                bytes: 0,
                why: vec!["seed".into()],
                matches: 0,
                co_changes: 0,
                untrusted: ContextText {
                    path: Some("p".repeat(990)),
                    ..Default::default()
                },
            },
            |i, n| i.bytes = n,
        );
        assert_eq!(long.bytes, serde_json::to_vec(&long).unwrap().len() as u64);
    }

    #[test]
    fn quotas_then_spare_budget_in_rank_order() {
        let sizes = [
            vec![100; 10], // files: 25% of 1000 = 250 → 2
            vec![50; 1],   // owners: fits its 100
            vec![300],     // history: over its 200
            vec![100; 5],  // in_flight: 200 → 2
            vec![],
            vec![100; 1], // duplicates: fits its 200
        ];
        let kept = keep_within(&sizes, 1000);
        // Quotas: 2 + 1 + 0 + 2 + 0 + 1 = 550 bytes; 450 spare goes to
        // in_flight first (3 more), then files (1 more); history's 300
        // never fits what is left.
        assert_eq!(kept, [3, 1, 0, 5, 0, 1]);
        let total: u64 = (0..6)
            .map(|i| sizes[i][..kept[i]].iter().sum::<u64>())
            .sum();
        assert!(total <= 1000);
        // An item larger than the whole budget is cut, and nothing past it
        // in its section is kept, so a section stays a ranked prefix.
        assert_eq!(
            keep_within(
                &[vec![5000, 1], vec![], vec![], vec![], vec![], vec![]],
                1024
            )[0],
            0
        );
    }
}
