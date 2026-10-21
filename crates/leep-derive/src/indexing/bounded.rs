//! Queries that never derive an index range (ADR 0058 section 3).
//!
//! Index rows are parent-closed: a changeset has its row only once its
//! parents have theirs. So the changesets reachable from a query's start
//! that lack a row are exactly those above a set of indexed ancestors, and
//! a query can answer from the rows that exist plus a direct look at those
//! few changesets, the way the index-free definition of the answer does,
//! storing nothing. Rows decide speed, never answers: the entries, their
//! order, `truncated` and whether a page has a `next` cursor are what a
//! fully indexed store gives.
//!
//! Each query first finds its un-indexed part, stopping at a budget. Within
//! the budget it runs on a handle that knows the part ([`Bounded`]). Over
//! it, the request's `Leep-Indexing` header decides: `retry` gets
//! [`Error::Indexing`] (503), `stale-ok` an answer at the newest indexed
//! first-parent ancestor, and a request without the header waits for the
//! indexer, then scans with larger budgets, then gets the 503. Only when no
//! live index lease exists does a query derive inline, as in `local` mode.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use leep_core::api::{BlameResponse, ChangesetInfo, SearchResponse};
use leep_core::{ChangesetId, Error, Hash, Lease, RepoPath, Result, TreeEntry, TreeNode};
use tokio::time::Instant;

use super::{parse_holder_meta, Answered, Budgets, IndexGroup, IndexMode, IndexPolicy};
use crate::derived::{self, DerivedKind, HistoryTree, LogFilterIndex, SkipEntry};
use crate::history::{LogPage, LogParams};
use crate::search::SearchQuery;
use crate::Repo;

/// What a request asked to happen when its index is behind (the
/// `Leep-Indexing` request header).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Freshness {
    /// `retry`: answer 503 with `Retry-After`.
    Retry,
    /// `stale-ok`: answer at the newest indexed first-parent ancestor.
    StaleOk,
    /// No header (clients from before ADR 0058): wait for the indexer,
    /// then scan more, then 503.
    Legacy,
}

impl Freshness {
    /// The header's value; anything unknown counts as no header.
    pub fn from_header(v: Option<&str>) -> Self {
        match v.map(str::trim) {
            Some(v) if v.eq_ignore_ascii_case(leep_core::api::INDEXING_RETRY) => Freshness::Retry,
            Some(v) if v.eq_ignore_ascii_case(leep_core::api::INDEXING_STALE_OK) => {
                Freshness::StaleOk
            }
            _ => Freshness::Legacy,
        }
    }
}

/// The un-indexed part of one bounded query, carried by the handle it runs
/// on ([`Repo::bounded`]).
#[derive(Default)]
pub(crate) struct Bounded {
    /// Changesets reachable from the query's start without a row: examined
    /// directly, never looked up in the index.
    pub(crate) unindexed: HashSet<ChangesetId>,
    /// Their parents.
    pub(crate) parents: HashMap<ChangesetId, Vec<ChangesetId>>,
    /// The indexed changesets right below them, and the indexed starts.
    pub(crate) frontier: Vec<ChangesetId>,
    /// Blame: first-parent ancestors of the head above this generation have
    /// no row.
    pub(crate) floor: Option<u64>,
    /// Search: rows computed in memory instead of stored.
    pub(crate) search: Option<Arc<SearchScan>>,
    /// Changesets (or files) examined without a row.
    pub(crate) examined: AtomicU64,
}

impl Bounded {
    pub(crate) fn examined(&self, n: u64) {
        self.examined.fetch_add(n, Ordering::Relaxed);
    }
}

/// Code search rows computed for one query and kept in memory (ADR 0058:
/// "a file without a row is fetched and matched"), and the budget for them.
/// A directory's filter is computed from its children's rows the way it
/// would be stored, so pruning, and every count but the bytes fetched to
/// compute them, is what the stored rows would give.
pub(crate) struct SearchScan {
    rows: Mutex<HashMap<(bool, Hash), Bytes>>,
    pub(crate) files: AtomicU64,
    pub(crate) bytes: AtomicU64,
    max_files: u64,
    max_bytes: u64,
    pub(crate) over: AtomicBool,
}

impl SearchScan {
    fn new(budgets: &Budgets) -> Self {
        SearchScan {
            rows: Mutex::default(),
            files: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
            max_files: budgets.search_files as u64,
            max_bytes: budgets.search_bytes,
            over: AtomicBool::new(false),
        }
    }

    /// A row computed earlier; `tree` tells tree filters from blob rows.
    pub(crate) fn get(&self, tree: bool, h: &Hash) -> Option<Bytes> {
        self.rows.lock().unwrap().get(&(tree, *h)).cloned()
    }

    pub(crate) fn put(&self, tree: bool, h: Hash, row: Bytes) {
        self.rows.lock().unwrap().insert((tree, h), row);
    }

    /// Count a file fetched to compute its row; over the budget the search
    /// stops with [`Error::Indexing`].
    pub(crate) fn fetched(&self, bytes: u64) -> Result<()> {
        let files = self.files.fetch_add(1, Ordering::Relaxed) + 1;
        let total = self.bytes.fetch_add(bytes, Ordering::Relaxed) + bytes;
        if files > self.max_files || total > self.max_bytes {
            self.over.store(true, Ordering::Relaxed);
            return Err(Error::Indexing {
                kind: IndexGroup::Search.lease_name().into(),
                behind: 0,
                retry_after_ms: 1000,
            });
        }
        Ok(())
    }
}

/// The changesets reachable from some starts that lack a row of one kind.
#[derive(Default)]
pub(crate) struct Missing {
    pub(crate) set: HashSet<ChangesetId>,
    pub(crate) parents: HashMap<ChangesetId, Vec<ChangesetId>>,
    pub(crate) frontier: Vec<ChangesetId>,
    /// More than the cap lack one; `set` is partial.
    pub(crate) over: bool,
}

/// The changesets reachable from `starts` without a stored row of `kind`,
/// found the way a derivation finds its missing range (a batch of rows and
/// changesets per round trip), stopping once more than `cap` are found.
/// Rows only staged by a derivation in this process count as missing: they
/// can still be dropped.
pub(crate) async fn find_missing(
    repo: &Repo,
    kind: &str,
    starts: &[ChangesetId],
    cap: usize,
) -> Result<Missing> {
    let meta = repo.meta();
    let mut m = Missing::default();
    let mut seen: HashSet<ChangesetId> = HashSet::new();
    let mut frontier: Vec<ChangesetId> =
        starts.iter().copied().filter(|s| seen.insert(*s)).collect();
    while !frontier.is_empty() {
        let mut missing = Vec::new();
        for chunk in frontier.chunks(derived::BATCH) {
            let have = derived::get_derived_many(meta.as_ref(), kind, chunk).await?;
            for (id, v) in chunk.iter().zip(have) {
                match v {
                    Some(_) => m.frontier.push(*id),
                    None => missing.push(*id),
                }
            }
        }
        if m.set.len() + missing.len() > cap {
            m.over = true;
            m.set.extend(missing);
            return Ok(m);
        }
        frontier.clear();
        for chunk in missing.chunks(derived::BATCH) {
            for (cid, cs) in chunk
                .iter()
                .zip(derived::get_changesets(repo, chunk).await?)
            {
                m.set.insert(*cid);
                repo.remember_parents(*cid, cs.parents.as_slice().into());
                frontier.extend(cs.parents.iter().filter(|p| seen.insert(**p)));
                m.parents.insert(*cid, cs.parents);
            }
        }
    }
    Ok(m)
}

/// The newest first-parent ancestor of `head` (itself included) with a
/// stored row of `kind`, and how many first-parent ancestors above it lack
/// one; `None` when none has one. Rows are parent-closed, so down a
/// first-parent chain every changeset below one with a row has its own: the
/// skiplists find the boundary in a few batched reads.
pub(crate) async fn newest_indexed(
    repo: &Repo,
    kind: &str,
    head: ChangesetId,
) -> Result<(Option<SkipEntry>, u64)> {
    let meta = repo.meta();
    if meta.get_derived(kind, &head.0).await?.is_some() {
        let generation = repo.generation(head).await?;
        return Ok((
            Some(SkipEntry {
                id: head,
                generation,
            }),
            0,
        ));
    }
    let (mut x, mut steps) = (head, 0u64);
    loop {
        let list = repo.skiplist(x).await?;
        if list.is_empty() {
            return Ok((None, steps + 1));
        }
        let ids: Vec<ChangesetId> = list.iter().map(|e| e.id).collect();
        let rows = derived::get_derived_many(meta.as_ref(), kind, &ids).await?;
        match rows.iter().position(Option::is_some) {
            Some(0) => return Ok((Some(list[0]), steps + 1)),
            Some(k) => {
                x = list[k - 1].id;
                steps += 1 << (k - 1);
            }
            None => {
                x = list[list.len() - 1].id;
                steps += 1 << (list.len() - 1);
            }
        }
    }
}

/// The endpoint a bounded query serves, for metrics.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Route {
    Log,
    LogFilter,
    LastChange,
    Blame,
    Search,
}

impl Route {
    fn label(self) -> &'static str {
        match self {
            Route::Log => "log",
            Route::LogFilter => "log_filter",
            Route::LastChange => "last_change",
            Route::Blame => "blame",
            Route::Search => "search",
        }
    }

    fn group(self) -> IndexGroup {
        match self {
            Route::Log | Route::LastChange | Route::Blame => IndexGroup::History,
            Route::LogFilter => IndexGroup::LogFilter,
            Route::Search => IndexGroup::Search,
        }
    }
}

/// What a bounded query needs to look at to tell whether it fits.
#[derive(Clone)]
enum Need {
    /// Every changeset reachable from these (path logs, last-change,
    /// filtered logs).
    Reachable(Vec<ChangesetId>),
    /// The first-parent chain of this head (blame).
    Chain(ChangesetId),
    /// The search index of the directory or file `path` of a changeset's
    /// tree: its rows, or what computing them costs.
    Search {
        path: String,
        start: Option<SearchStart>,
    },
}

/// What a search starts from.
#[derive(Clone, Copy)]
pub(crate) enum SearchStart {
    Dir(Hash),
    File { content: Hash, size: u64 },
}

/// First-parent ancestors a `stale-ok` search looks at for one whose index
/// is complete; past them it answers 503.
const STALE_SEARCH_STEPS: usize = 64;

/// How a query proceeds.
enum Plan {
    /// No policy, `local` mode, or no live lease: derive inline as before.
    Inline,
    /// Answer at `at` with the un-indexed part `bounded`.
    At {
        at: ChangesetId,
        bounded: Arc<Bounded>,
        behind: Option<u64>,
    },
}

impl Repo {
    /// This handle, bounded to `b`.
    fn with_bounded(&self, b: Arc<Bounded>) -> Repo {
        let mut r = self.clone();
        r.bounded = Some(b);
        r
    }

    /// Whether `id` is in the un-indexed part of the bounded query this
    /// handle serves.
    pub(crate) fn unindexed(&self, id: ChangesetId) -> bool {
        self.bounded
            .as_ref()
            .is_some_and(|b| b.unindexed.contains(&id))
    }

    /// Whether the policy wants queries for `group` bounded now: the mode
    /// says so and a live lease exists.
    async fn bounding(&self, route: Route) -> Option<(Arc<IndexPolicy>, Lease)> {
        let policy = self.index_policy.clone()?;
        if policy.mode == IndexMode::Local {
            return None;
        }
        match policy.leases.live(route.group()).await {
            Some(lease) => Some((policy, lease)),
            None => {
                metrics::counter!("leep_query_inline_derive_total", "route" => route.label())
                    .increment(1);
                None
            }
        }
    }

    /// Whether `need` fits `budgets`: the un-indexed part, or `None` over
    /// them.
    async fn fits(
        &self,
        group: IndexGroup,
        need: &Need,
        budgets: &Budgets,
    ) -> Result<Option<Bounded>> {
        match need {
            Need::Reachable(starts) => {
                let kind = match group {
                    IndexGroup::LogFilter => LogFilterIndex::NAME,
                    _ => HistoryTree::NAME,
                };
                let m = find_missing(self, kind, starts, budgets.scan).await?;
                if m.over {
                    return Ok(None);
                }
                Ok(Some(Bounded {
                    unindexed: m.set,
                    parents: m.parents,
                    frontier: m.frontier,
                    ..Default::default()
                }))
            }
            Need::Chain(head) => {
                let (found, steps) = newest_indexed(self, HistoryTree::NAME, *head).await?;
                if steps > budgets.blame as u64 {
                    return Ok(None);
                }
                Ok(Some(Bounded {
                    floor: Some(found.map_or(0, |e| e.generation)),
                    ..Default::default()
                }))
            }
            Need::Search { start, .. } => {
                let scan = Arc::new(SearchScan::new(budgets));
                let counters = crate::search::index::Counters::scanning(scan.clone());
                let r = match start {
                    None => Ok(()),
                    Some(SearchStart::Dir(tree)) => {
                        self.tree_row(*tree, &counters).await.map(|_| ())
                    }
                    Some(SearchStart::File { content, size }) => self
                        .blob_row_bytes(*content, *size, &counters)
                        .await
                        .map(|_| ()),
                };
                match r {
                    Ok(()) => Ok(Some(Bounded {
                        search: Some(scan),
                        ..Default::default()
                    })),
                    Err(Error::Indexing { .. }) if scan.over.load(Ordering::Relaxed) => Ok(None),
                    Err(e) => Err(e),
                }
            }
        }
    }

    /// Decide how a query at `at` proceeds (see the module docs). `stale`
    /// is the changeset to fall back to under `stale-ok` (`None` when the
    /// answer cannot move, as for a later page), found lazily.
    async fn plan(
        &self,
        route: Route,
        freshness: Freshness,
        at: ChangesetId,
        need: Need,
        stale_allowed: bool,
        started: Instant,
    ) -> Result<Plan> {
        let Some((policy, lease)) = self.bounding(route).await else {
            return Ok(Plan::Inline);
        };
        let group = route.group();
        if let Some(b) = self.fits(group, &need, &policy.budgets).await? {
            return Ok(Plan::At {
                at,
                bounded: Arc::new(b),
                behind: None,
            });
        }
        match freshness {
            Freshness::Retry => {}
            Freshness::StaleOk if stale_allowed => {
                if let Some((stale_at, behind)) = self.stale_target(route, at, &need).await? {
                    let need = self.need_at(stale_at, &need).await?;
                    if let Some(b) = self.fits(group, &need, &policy.budgets).await? {
                        metrics::counter!("leep_query_stale_answers_total", "route" => route.label())
                            .increment(1);
                        return Ok(Plan::At {
                            at: stale_at,
                            bounded: Arc::new(b),
                            behind: Some(behind),
                        });
                    }
                }
            }
            Freshness::StaleOk => {}
            Freshness::Legacy => {
                if let Some(b) = self.wait_for_index(&policy, group, &need, started).await? {
                    return Ok(Plan::At {
                        at,
                        bounded: Arc::new(b),
                        behind: None,
                    });
                }
                let larger = policy.budgets.times(policy.budgets.legacy_factor);
                if let Some(b) = self.fits(group, &need, &larger).await? {
                    return Ok(Plan::At {
                        at,
                        bounded: Arc::new(b),
                        behind: None,
                    });
                }
            }
        }
        let mode = if freshness == Freshness::Legacy {
            "legacy"
        } else {
            "retry"
        };
        metrics::counter!("leep_query_indexing_answers_total", "route" => route.label(), "mode" => mode)
            .increment(1);
        Err(self.indexing_error(route, at, &need, &lease).await)
    }

    /// Wait for the indexer, re-checking once a second, for at most
    /// `--index-wait` and the request's remaining time less a second, if a
    /// waiter's place is free.
    async fn wait_for_index(
        &self,
        policy: &IndexPolicy,
        group: IndexGroup,
        need: &Need,
        started: Instant,
    ) -> Result<Option<Bounded>> {
        let mut limit = policy.index_wait;
        if !policy.request_timeout.is_zero() {
            let left = policy
                .request_timeout
                .saturating_sub(started.elapsed())
                .saturating_sub(Duration::from_secs(1));
            limit = limit.min(left);
        }
        if limit.is_zero() {
            return Ok(None);
        }
        let Ok(_permit) = policy.waiting().clone().try_acquire_owned() else {
            metrics::counter!("leep_query_index_wait_refused_total").increment(1);
            return Ok(None);
        };
        let _waiting = Waiting::new();
        let waited = Instant::now();
        let deadline = waited + limit;
        let mut found = None;
        // What the last full check found missing: while enough of it still
        // lacks rows, the need is still over budget, which one batched read
        // tells without walking history again.
        let mut last: Option<Vec<ChangesetId>> = None;
        loop {
            let next = (Instant::now() + Duration::from_secs(1)).min(deadline);
            tokio::time::sleep_until(next).await;
            if self.still_over(group, need, &policy.budgets, &last).await? {
                if Instant::now() >= deadline {
                    break;
                }
                continue;
            }
            match need {
                Need::Reachable(starts) => {
                    let m =
                        find_missing(self, group_kind(group), starts, policy.budgets.scan).await?;
                    if !m.over {
                        found = Some(Bounded {
                            unindexed: m.set,
                            parents: m.parents,
                            frontier: m.frontier,
                            ..Default::default()
                        });
                        break;
                    }
                    last = Some(m.set.into_iter().collect());
                }
                _ => {
                    if let Some(b) = self.fits(group, need, &policy.budgets).await? {
                        found = Some(b);
                        break;
                    }
                }
            }
            if Instant::now() >= deadline {
                break;
            }
        }
        metrics::histogram!("leep_query_index_wait_seconds").record(waited.elapsed().as_secs_f64());
        Ok(found)
    }

    /// A cheap test that `need` is still over `budgets` while waiting: a
    /// search whose starting directory has no stored row yet (computing it
    /// fetches up to the budget from the blob store, so it is done only once
    /// the row exists or the wait is over), or a walk of which more than the
    /// budget of what the last check found missing still lacks rows. Rows
    /// are never removed by an indexer, so those are still missing now.
    async fn still_over(
        &self,
        group: IndexGroup,
        need: &Need,
        budgets: &Budgets,
        last: &Option<Vec<ChangesetId>>,
    ) -> Result<bool> {
        match need {
            Need::Search { start, .. } => Ok(!self.search_start_stored(start).await?),
            Need::Reachable(_) => {
                let Some(ids) = last else {
                    return Ok(false);
                };
                let mut lacking = 0;
                for chunk in ids.chunks(derived::BATCH) {
                    let rows =
                        derived::get_derived_many(self.meta().as_ref(), group_kind(group), chunk)
                            .await?;
                    lacking += rows.iter().filter(|r| r.is_none()).count();
                }
                Ok(lacking > budgets.scan)
            }
            Need::Chain(_) => Ok(false),
        }
    }

    /// Whether the rows of a search's start are stored.
    async fn search_start_stored(&self, start: &Option<SearchStart>) -> Result<bool> {
        let meta = self.meta();
        Ok(match start {
            None => true,
            Some(SearchStart::Dir(tree)) => meta
                .get_derived(crate::search::TREE_KIND, tree)
                .await?
                .is_some(),
            Some(SearchStart::File { content, size }) => {
                *size > crate::search::MAX_INDEXED_BYTES
                    || meta
                        .get_derived(crate::search::BLOB_KIND, content)
                        .await?
                        .is_some()
            }
        })
    }

    /// The newest first-parent ancestor of `at` the index covers, and the
    /// generations between them, for `stale-ok`.
    async fn stale_target(
        &self,
        route: Route,
        at: ChangesetId,
        need: &Need,
    ) -> Result<Option<(ChangesetId, u64)>> {
        let here = self.generation(at).await?;
        if let Need::Search { path, .. } = need {
            // Search rows are kept per tree, not per changeset, and only
            // warmed for heads: look down the chain for a tree whose rows
            // are all stored.
            let mut x = at;
            for _ in 0..STALE_SEARCH_STEPS {
                let Some(p) = self.skiplist(x).await?.first().copied() else {
                    return Ok(None);
                };
                x = p.id;
                if self.search_indexed(x, path).await? {
                    return Ok(Some((x, here - p.generation)));
                }
            }
            return Ok(None);
        }
        let kind = match route.group() {
            IndexGroup::LogFilter => LogFilterIndex::NAME,
            _ => HistoryTree::NAME,
        };
        let (found, _) = newest_indexed(self, kind, at).await?;
        Ok(found.map(|e| (e.id, here - e.generation)))
    }

    /// `need` moved to the changeset `at`.
    async fn need_at(&self, at: ChangesetId, need: &Need) -> Result<Need> {
        Ok(match need {
            Need::Reachable(_) => Need::Reachable(vec![at]),
            Need::Chain(_) => Need::Chain(at),
            Need::Search { path, .. } => Need::Search {
                path: path.clone(),
                start: self.search_start(at, path).await?,
            },
        })
    }

    /// What a search of `path` at `at` starts from; `None` when there is
    /// nothing there the index covers.
    async fn search_start(&self, at: ChangesetId, path: &str) -> Result<Option<SearchStart>> {
        let root = self.root_tree(at).await?;
        Ok(match self.entry_at(root, path).await? {
            Some(TreeEntry::Dir { tree }) => Some(SearchStart::Dir(tree)),
            Some(TreeEntry::File {
                content,
                kind,
                size,
            }) if kind != leep_core::FileKind::Symlink => Some(SearchStart::File { content, size }),
            _ => None,
        })
    }

    /// Whether the search index of `path` at `at` is stored whole.
    async fn search_indexed(&self, at: ChangesetId, path: &str) -> Result<bool> {
        let start = self.search_start(at, path).await?;
        self.search_start_stored(&start).await
    }

    /// The 503 for a query over every budget: how far behind its index is,
    /// and when to retry by the holder's published rate.
    async fn indexing_error(
        &self,
        route: Route,
        at: ChangesetId,
        need: &Need,
        lease: &Lease,
    ) -> Error {
        let group = route.group();
        let behind = match need {
            Need::Search { .. } => match self.stale_target(route, at, need).await {
                Ok(Some((_, behind))) => behind,
                _ => STALE_SEARCH_STEPS as u64,
            },
            _ => {
                let kind = match group {
                    IndexGroup::LogFilter => LogFilterIndex::NAME,
                    _ => HistoryTree::NAME,
                };
                match (
                    newest_indexed(self, kind, at).await,
                    self.generation(at).await,
                ) {
                    (Ok((Some(e), _)), Ok(g)) => g.saturating_sub(e.generation),
                    (Ok((None, _)), Ok(g)) => g,
                    _ => 0,
                }
            }
        };
        Error::Indexing {
            kind: group.lease_name().into(),
            behind,
            retry_after_ms: retry_after(behind, &parse_holder_meta(&lease.meta)).as_millis() as u64,
        }
    }
}

/// The kind whose rows say whether a changeset is indexed for `g`'s walks.
fn group_kind(g: IndexGroup) -> &'static str {
    match g {
        IndexGroup::LogFilter => LogFilterIndex::NAME,
        _ => HistoryTree::NAME,
    }
}

/// What a bounded query answers when index rows it relied on were deleted
/// under it (a `repair drop` racing it): the index is behind; retry.
pub(crate) fn rows_vanished(g: IndexGroup) -> Error {
    metrics::counter!("leep_query_rows_vanished_total", "kind" => g.lease_name()).increment(1);
    Error::Indexing {
        kind: g.lease_name().into(),
        behind: 0,
        retry_after_ms: 1000,
    }
}

/// Keeps `leep_query_index_waiters` right however a wait ends.
struct Waiting(metrics::Gauge);

impl Waiting {
    fn new() -> Self {
        let g = metrics::gauge!("leep_query_index_waiters");
        g.increment(1.0);
        Waiting(g)
    }
}

impl Drop for Waiting {
    fn drop(&mut self) {
        self.0.decrement(1.0);
    }
}

/// `Retry-After` for an index `behind` changesets behind, by the holder's
/// rate and backlog, clamped to 1 to 30 s. Without a rate yet, 100
/// changesets a second is assumed.
pub fn retry_after(behind: u64, holder: &super::HolderMeta) -> Duration {
    let work = behind.max(holder.backlog) as f64;
    let rate = if holder.rate > 0.0 {
        holder.rate
    } else {
        100.0
    };
    Duration::from_secs_f64((work / rate).ceil().clamp(1.0, 30.0))
}

impl Repo {
    /// Whether `id` (`cs`) changed `path` relative to its first parent, as
    /// `log_scan` decides it, for a changeset without a row. A changeset's
    /// tree is its first parent's with `changes` applied, merges included,
    /// so one whose change keys neither lie at or below `path` nor above it
    /// leaves the path alone, and only the others read trees.
    pub(crate) async fn examine_path(
        &self,
        id: ChangesetId,
        cs: &leep_core::Changeset,
        path: &str,
    ) -> Result<bool> {
        if let Some(b) = &self.bounded {
            b.examined(1);
        }
        let below = |a: &str, b: &str| a.strip_prefix(b).is_some_and(|r| r.starts_with('/'));
        let touches = cs.changes.keys().any(|k| {
            let k = k.as_str();
            k == path || below(k, path) || below(path, k)
        });
        if !touches {
            return Ok(false);
        }
        let root = self.root_tree(id).await?;
        let parent = match cs.parents.first() {
            Some(p) => self.root_tree(*p).await?,
            None => TreeNode::empty_hash(),
        };
        Ok(root != parent
            && self.entry_at(root, path).await? != self.entry_at(parent, path).await?)
    }

    /// [`Repo::last_change`] in a bounded query: the newest changes of
    /// `path` at `id`, computed for the changesets without a row from their
    /// parents' the way the index computes them (a changeset that changed
    /// the path is its own newest change, others take the maximal changes
    /// behind their parents), and read from the index below them.
    async fn last_change_bounded(
        &self,
        id: ChangesetId,
        path: &str,
    ) -> Result<(ChangesetInfo, Vec<ChangesetId>)> {
        let path = path.trim_matches('/');
        let missing = || Error::NotFound(format!("path {path:?} at {id}"));
        let b = self.bounded.clone().expect("a bounded handle");
        let changed: Option<Vec<ChangesetId>> = if !b.unindexed.contains(&id) {
            self.history_entry(id, path)
                .await?
                .map(|e| e.changed().to_vec())
        } else {
            // Parents first, iteratively, over the un-indexed part below `id`.
            let mut order = Vec::new();
            let mut done: HashSet<ChangesetId> = HashSet::new();
            let mut stack = vec![(id, 0usize)];
            done.insert(id);
            while let Some((top, next)) = stack.last_mut() {
                let ps = &b.parents[top];
                if let Some(&p) = ps.get(*next) {
                    *next += 1;
                    if b.unindexed.contains(&p) && done.insert(p) {
                        stack.push((p, 0));
                    }
                } else {
                    order.push(*top);
                    stack.pop();
                }
            }
            let mut sets: HashMap<ChangesetId, Option<Vec<ChangesetId>>> = HashMap::new();
            for u in order {
                let cs = self.get_changeset(u).await?;
                let set = if self.examine_path(u, &cs, path).await? {
                    Some(vec![u])
                } else {
                    let mut all: Option<Vec<ChangesetId>> = None;
                    for p in &cs.parents {
                        let s = match sets.get(p) {
                            Some(s) => s.clone(),
                            None => self
                                .history_entry(*p, path)
                                .await?
                                .map(|e| e.changed().to_vec()),
                        };
                        if let Some(s) = s {
                            all.get_or_insert_with(Vec::new).extend(s);
                        }
                    }
                    match all {
                        Some(all) => Some(self.maximal_ids(all).await?),
                        None => None,
                    }
                };
                sets.insert(u, set);
            }
            sets.remove(&id).flatten()
        };
        let set = changed.ok_or_else(missing)?;
        let mut newest = Vec::new();
        for c in set {
            newest.push((self.generation(c).await?, c));
        }
        newest.sort_by(|a, b| b.cmp(a));
        let mut ids = newest.into_iter().map(|(_, c)| c);
        let first = ids.next().ok_or_else(missing)?;
        Ok((self.info(first).await?, ids.collect()))
    }

    /// `ids` without those that are ancestors of another, sorted by id.
    async fn maximal_ids(&self, mut ids: Vec<ChangesetId>) -> Result<Vec<ChangesetId>> {
        ids.sort();
        ids.dedup();
        if ids.len() < 2 {
            return Ok(ids);
        }
        let mut keep = Vec::new();
        'next: for &a in &ids {
            for &b in &ids {
                if a != b && self.is_ancestor(a, b).await? {
                    continue 'next;
                }
            }
            keep.push(a);
        }
        Ok(keep)
    }

    /// Generations between `at` and `id`, when they differ.
    async fn behind_of(&self, id: ChangesetId, at: ChangesetId) -> Result<Option<u64>> {
        if at == id {
            return Ok(None);
        }
        let (a, b) = (self.generation(id).await?, self.generation(at).await?);
        Ok(Some(a.saturating_sub(b)))
    }

    fn count_scanned(route: Route, b: &Bounded) -> u64 {
        let n = b.examined.load(Ordering::Relaxed);
        metrics::counter!("leep_query_unindexed_scanned_total", "route" => route.label())
            .increment(n);
        n
    }

    /// [`Repo::log_page`] under the server's index policy (ADR 0058): path
    /// logs and filtered logs never derive an index range while a live index
    /// lease exists. `freshness` is the request's `Leep-Indexing`.
    pub async fn log_page_indexed(
        &self,
        id: ChangesetId,
        params: &LogParams,
        limit: usize,
        after: Option<&str>,
        freshness: Freshness,
    ) -> Result<Answered<LogPage>> {
        let started = Instant::now();
        let route = match (
            crate::history::norm(params.path.as_deref()),
            params.filter.is_empty(),
        ) {
            (Some(_), _) => Route::Log,
            (None, false) => Route::LogFilter,
            (None, true) => {
                return Ok(Answered::fresh(
                    self.log_page(id, params, limit, after).await?,
                ))
            }
        };
        let (at, from) = self.log_resume(id, params, after).await?;
        let starts: Vec<ChangesetId> = match &from.seeds {
            Some(seeds) => seeds.iter().map(|e| e.id).collect(),
            None => vec![at],
        };
        let scan_first = self
            .index_policy
            .as_ref()
            .map_or(crate::filter_log::SCAN_FIRST, |p| p.scan_first);
        let max_frontier = crate::cursor::MAX_FRONTIER;
        let plan = self
            .plan(
                route,
                freshness,
                at,
                Need::Reachable(starts),
                after.is_none(),
                started,
            )
            .await?;
        let (answer_at, page, scanned) = match plan {
            Plan::Inline => (
                at,
                self.log_page_at(id, at, params, limit, &from, max_frontier, scan_first)
                    .await?,
                0,
            ),
            Plan::At {
                at: answer_at,
                bounded,
                ..
            } => {
                let from = if answer_at == at {
                    from
                } else {
                    crate::cursor::LogFrom::tip()
                };
                let page = self
                    .with_bounded(bounded.clone())
                    .log_page_at(
                        id,
                        answer_at,
                        params,
                        limit,
                        &from,
                        max_frontier,
                        scan_first,
                    )
                    .await?;
                (answer_at, page, Self::count_scanned(route, &bounded))
            }
        };
        Ok(Answered {
            value: page,
            answered_at: (answer_at != id).then_some(answer_at),
            behind: self.behind_of(id, answer_at).await?,
            scanned,
        })
    }

    /// [`Repo::last_change`] under the server's index policy (ADR 0058).
    pub async fn last_change_indexed(
        &self,
        id: ChangesetId,
        path: &str,
        freshness: Freshness,
    ) -> Result<Answered<(ChangesetInfo, Vec<ChangesetId>)>> {
        let started = Instant::now();
        let route = Route::LastChange;
        match self
            .plan(
                route,
                freshness,
                id,
                Need::Reachable(vec![id]),
                true,
                started,
            )
            .await?
        {
            Plan::Inline => Ok(Answered::fresh(self.last_change(id, path).await?)),
            Plan::At {
                at,
                bounded,
                behind,
            } => {
                let value = self
                    .with_bounded(bounded.clone())
                    .last_change_bounded(at, path)
                    .await?;
                Ok(Answered {
                    value,
                    answered_at: (at != id).then_some(at),
                    behind,
                    scanned: Self::count_scanned(route, &bounded),
                })
            }
        }
    }

    /// [`Repo::blame`] under the server's index policy (ADR 0058). A blame
    /// still stores its result cache.
    pub async fn blame_indexed(
        &self,
        id: ChangesetId,
        path: &RepoPath,
        freshness: Freshness,
    ) -> Result<Answered<BlameResponse>> {
        let started = Instant::now();
        let route = Route::Blame;
        match self
            .plan(route, freshness, id, Need::Chain(id), true, started)
            .await?
        {
            Plan::Inline => Ok(Answered::fresh(self.blame(id, path).await?)),
            Plan::At {
                at,
                bounded,
                behind,
            } => {
                let value = self.with_bounded(bounded.clone()).blame(at, path).await?;
                Ok(Answered {
                    value,
                    answered_at: (at != id).then_some(at),
                    behind,
                    scanned: Self::count_scanned(route, &bounded),
                })
            }
        }
    }

    /// [`Repo::search_page`] under the server's index policy (ADR 0058).
    /// Missing trigram rows are computed in memory, never stored.
    pub async fn search_page_indexed(
        &self,
        rev: &str,
        q: &SearchQuery,
        after: Option<&str>,
        freshness: Freshness,
    ) -> Result<Answered<SearchResponse>> {
        let started = Instant::now();
        let route = Route::Search;
        let (id, pos) = self.search_resume(rev, q, after).await?;
        if !Self::search_uses_index(q)? {
            return Ok(Answered::fresh(
                self.search_page_at(rev, id, q, pos.as_ref()).await?,
            ));
        }
        let path = q.path.trim_matches('/').to_string();
        let need = Need::Search {
            start: self.search_start(id, &path).await?,
            path,
        };
        match self
            .plan(route, freshness, id, need, after.is_none(), started)
            .await?
        {
            Plan::Inline => Ok(Answered::fresh(
                self.search_page_at(rev, id, q, pos.as_ref()).await?,
            )),
            Plan::At {
                at,
                bounded,
                behind,
            } => {
                let pos = if at == id { pos } else { None };
                let mut value = self
                    .with_bounded(bounded.clone())
                    .search_page_at(rev, at, q, pos.as_ref())
                    .await?;
                let scanned = bounded
                    .search
                    .as_ref()
                    .map_or(0, |s| s.files.load(Ordering::Relaxed));
                metrics::counter!("leep_query_unindexed_scanned_total", "route" => route.label())
                    .increment(scanned);
                value.answered_at = (at != id).then_some(at);
                value.behind = behind;
                Ok(Answered {
                    value,
                    answered_at: (at != id).then_some(at),
                    behind,
                    scanned,
                })
            }
        }
    }
}
