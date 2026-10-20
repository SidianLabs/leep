//! Background index warming (ADR 0028). Derived kinds are computed on first
//! use, so the first path query on a freshly imported history used to derive
//! the whole path-history index inside one request. The warmer derives the
//! indexes for every ref head as refs move, so queries find them ready.
//!
//! It learns of moves by tailing the ref log (ADR 0058): a pass reads the
//! entries newer than the last it saw, which is one indexed query when
//! nothing moved, so passes can run every 250 ms. A ref log `seq` is
//! assigned in commit order (Postgres takes a global lock before inserting
//! the row; SQLite and memory have one writer), so an entry committed after
//! a read never carries a `seq` below what that read saw. A full listing of
//! the refs every few seconds backs the tail up anyway.
//!
//! In `lease` mode (ADR 0058) a pass derives only the index groups whose
//! lease this process holds, checks the lease before each write of a
//! derivation, and stops a derivation at the next write once the lease is
//! gone.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use leep_core::{ChangesetId, Result};

use crate::derived::{self, HistoryTree, LogFilterIndex};
use crate::indexing::{IndexGroup, IndexLeases};
use crate::Repo;

/// Ref log entries read per round trip of the tail.
pub const TAIL_PAGE: usize = 256;
/// Pages the tail reads before it gives up and lists every ref instead.
const TAIL_PAGES: usize = 16;
/// How often a pass lists every ref, whatever the tail found.
pub const FULL_LISTING_EVERY: Duration = Duration::from_secs(5);

/// What one [`Warmer::pass`] did.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WarmReport {
    /// Refs whose head the pass looked at: every ref on a full listing, the
    /// refs the ref log says moved otherwise.
    pub refs: usize,
    /// Refs whose head was new and got its indexes derived.
    pub warmed: usize,
    /// Refs whose warming failed; the next full listing retries them.
    pub failed: usize,
}

/// Derives the indexes of ref heads that changed since the last pass.
pub struct Warmer {
    repo: Repo,
    search: bool,
    /// `lease` mode: warm only the groups held here.
    leases: Option<Arc<IndexLeases>>,
    /// Per group, the head each ref was last warmed at.
    warmed: HashMap<IndexGroup, HashMap<String, ChangesetId>>,
    /// Groups the last pass warmed.
    active: Vec<IndexGroup>,
    /// Newest ref log entry handled; `None` until a full listing.
    seq: Option<u64>,
    last_full: Option<Instant>,
    full_every: Duration,
}

/// A ref's head as a pass found it, and when it moved there if known.
struct Moved {
    head: ChangesetId,
    at: Option<i64>,
}

impl Warmer {
    /// `search` also derives the code-search index of each head.
    pub fn new(repo: Repo, search: bool) -> Self {
        Warmer {
            repo,
            search,
            leases: None,
            warmed: HashMap::new(),
            active: Vec::new(),
            seq: None,
            last_full: None,
            full_every: FULL_LISTING_EVERY,
        }
    }

    /// `lease` mode: warm a group only while `leases` holds its lease.
    pub fn with_leases(mut self, leases: Arc<IndexLeases>) -> Self {
        self.leases = Some(leases);
        self
    }

    /// List every ref this often instead of [`FULL_LISTING_EVERY`] (tests).
    pub fn with_full_listing_every(mut self, every: Duration) -> Self {
        self.full_every = every;
        self
    }

    /// The groups this process warms now: all of them in `local` mode, the
    /// held ones in `lease` mode.
    async fn groups(&self) -> Vec<IndexGroup> {
        let mut out = vec![IndexGroup::History, IndexGroup::LogFilter];
        if self.search {
            out.push(IndexGroup::Search);
        }
        let Some(leases) = &self.leases else {
            return out;
        };
        let mut held = Vec::new();
        for g in out {
            if leases.check(g).await {
                held.push(g);
            }
        }
        held
    }

    /// Warm every ref whose head moved since the last pass. All moved heads
    /// derive as one range per kind ([`derived::derive_heads`]), so history
    /// they share is derived once and different refs' new changesets derive
    /// side by side.
    pub async fn pass(&mut self) -> Result<WarmReport> {
        let groups = self.groups().await;
        if groups.iter().any(|g| !self.active.contains(g)) {
            // A group taken over (or the first pass): what the previous
            // holder left undone is found only by looking at every ref.
            self.last_full = None;
            for g in &groups {
                if !self.active.contains(g) {
                    self.warmed.remove(g);
                }
            }
        }
        self.active = groups.clone();
        if groups.is_empty() {
            return Ok(WarmReport::default());
        }
        let moved = self.moved().await?;
        let mut report = WarmReport {
            refs: moved.len(),
            ..Default::default()
        };
        let todo: Vec<(String, Moved)> = moved
            .into_iter()
            .filter(|(name, m)| {
                groups
                    .iter()
                    .any(|g| self.warmed.get(g).and_then(|w| w.get(name)) != Some(&m.head))
            })
            .collect();
        if todo.is_empty() {
            for g in &groups {
                metrics::gauge!("leep_index_lag_seconds", "kind" => g.lease_name()).set(0.0);
            }
            return Ok(report);
        }
        let started = Instant::now();
        let now = unix_now();
        let oldest = todo.iter().filter_map(|(_, m)| m.at).min();
        for g in &groups {
            let lag = oldest.map_or(0, |at| (now - at).max(0));
            metrics::gauge!("leep_index_lag_seconds", "kind" => g.lease_name()).set(lag as f64);
        }
        // Each group's heads, then each group derived as one range, the
        // groups side by side.
        let per_group: Vec<(IndexGroup, Vec<ChangesetId>)> = groups
            .iter()
            .map(|g| {
                let w = self.warmed.get(g);
                let heads = todo
                    .iter()
                    .filter(|(name, m)| w.and_then(|w| w.get(name)) != Some(&m.head))
                    .map(|(_, m)| m.head)
                    .collect();
                (*g, heads)
            })
            .collect();
        let results = futures_util::future::join_all(
            per_group
                .iter()
                .map(|(g, heads)| self.warm_group(*g, heads)),
        )
        .await;
        let mut failed_groups: HashMap<IndexGroup, ()> = HashMap::new();
        for ((g, _), r) in per_group.iter().zip(&results) {
            match r {
                Ok(n) => {
                    metrics::counter!("leep_index_derived_total", "kind" => g.lease_name())
                        .increment(*n);
                    if let Some(l) = &self.leases {
                        l.record(*g, *n, *n);
                    }
                }
                Err(_) => {
                    failed_groups.insert(*g, ());
                }
            }
        }
        for (name, m) in todo {
            let mut ok = true;
            for g in &groups {
                if self.warmed.get(g).and_then(|w| w.get(&name)) == Some(&m.head) {
                    continue;
                }
                // After a failure together, each ref alone, so one bad ref
                // does not hold the others back.
                let result = if failed_groups.contains_key(g) {
                    self.warm_group(*g, &[m.head]).await.map(|_| ())
                } else {
                    Ok(())
                };
                match result {
                    Ok(()) => {
                        self.warmed
                            .entry(*g)
                            .or_default()
                            .insert(name.clone(), m.head);
                    }
                    Err(e) => {
                        ok = false;
                        tracing::warn!(r#ref = %name, head = %m.head, kind = g.lease_name(), error = %e, "warming indexes failed; retrying on the next full listing");
                    }
                }
            }
            if ok {
                metrics::counter!("leep_warm_refs_total", "result" => "ok").increment(1);
                if let Some(at) = m.at {
                    // 1-second resolution: ref log times are whole
                    // seconds, so 0 means "within the same second".
                    let delay = (unix_now() - at).max(0) as f64;
                    metrics::histogram!("leep_warm_delay_seconds").record(delay);
                }
                report.warmed += 1;
            } else {
                metrics::counter!("leep_warm_refs_total", "result" => "error").increment(1);
                report.failed += 1;
            }
        }
        if report.failed == 0 {
            for g in &groups {
                metrics::gauge!("leep_index_lag_seconds", "kind" => g.lease_name()).set(0.0);
            }
        }
        let took = started.elapsed();
        metrics::histogram!("leep_warm_duration_seconds").record(took.as_secs_f64());
        if took > Duration::from_secs(1) {
            tracing::info!(
                refs = report.warmed,
                seconds = took.as_secs_f64(),
                "indexes warmed"
            );
        }
        Ok(report)
    }

    /// Derive group `g` for `heads`: the changesets derived (heads, for
    /// search). In `lease` mode the derivation writes only while the lease
    /// is held.
    async fn warm_group(&self, g: IndexGroup, heads: &[ChangesetId]) -> Result<u64> {
        let repo = match &self.leases {
            Some(l) => self.repo.with_write_guard(Arc::new(LeaseGuard {
                leases: l.clone(),
                group: g,
            })),
            None => self.repo.clone(),
        };
        match g {
            IndexGroup::History => {
                Ok(derived::derive_heads_counted::<HistoryTree>(&repo, heads).await? as u64)
            }
            IndexGroup::LogFilter => {
                Ok(derived::derive_heads_counted::<LogFilterIndex>(&repo, heads).await? as u64)
            }
            IndexGroup::Search => {
                for &id in heads {
                    if !repo.write_allowed().await {
                        return Err(lease_lost());
                    }
                    repo.derive_search(id).await?;
                }
                Ok(heads.len() as u64)
            }
        }
    }

    /// The refs to look at: every ref on a full listing, else those the ref
    /// log says moved. A ref whose warming failed is retried by the next
    /// full listing, a few seconds later, rather than on every pass.
    async fn moved(&mut self) -> Result<BTreeMap<String, Moved>> {
        let due = self
            .last_full
            .is_none_or(|t| t.elapsed() >= self.full_every);
        let tailed = if due { None } else { self.tail().await? };
        match tailed {
            Some(found) => Ok(found),
            None => self.full().await,
        }
    }

    /// Every ref, read after the newest ref log entry, so the tail resumes
    /// from a point the listing already covers.
    async fn full(&mut self) -> Result<BTreeMap<String, Moved>> {
        let newest = self.repo.meta().ref_log(None, None, 1).await?;
        let refs = self.repo.meta().list_refs().await?;
        self.seq = Some(newest.first().map_or(0, |e| e.seq));
        self.last_full = Some(Instant::now());
        for w in self.warmed.values_mut() {
            w.retain(|name, _| refs.contains_key(name));
        }
        Ok(refs
            .into_iter()
            .map(|(name, head)| (name, Moved { head, at: None }))
            .collect())
    }

    /// Refs moved since the last entry handled, at their newest heads; a
    /// deleted ref is forgotten. `None` when the tail cannot tell (too many
    /// entries, or no full listing yet), and the caller lists every ref.
    async fn tail(&mut self) -> Result<Option<BTreeMap<String, Moved>>> {
        let Some(last) = self.seq else {
            return Ok(None);
        };
        let meta = self.repo.meta();
        let mut entries = Vec::new();
        let mut before = None;
        let mut reached = false;
        for _ in 0..TAIL_PAGES {
            let page = meta.ref_log(None, before, TAIL_PAGE).await?;
            let full = page.len() == TAIL_PAGE;
            for e in page {
                if e.seq <= last {
                    reached = true;
                    break;
                }
                before = Some(e.seq);
                entries.push(e);
            }
            if reached || !full {
                reached = true;
                break;
            }
        }
        if !reached {
            return Ok(None);
        }
        let mut moved = BTreeMap::new();
        if let Some(newest) = entries.first() {
            self.seq = Some(newest.seq);
        }
        // Newest first: the first entry of a ref is its head now.
        for e in entries {
            if moved.contains_key(&e.name) {
                continue;
            }
            match e.new {
                Some(head) => {
                    moved.insert(
                        e.name,
                        Some(Moved {
                            head,
                            at: Some(e.time),
                        }),
                    );
                }
                None => {
                    for w in self.warmed.values_mut() {
                        w.remove(&e.name);
                    }
                    moved.insert(e.name, None);
                }
            }
        }
        Ok(Some(
            moved
                .into_iter()
                .filter_map(|(name, m)| m.map(|m| (name, m)))
                .collect(),
        ))
    }

    /// Run passes forever, `interval` apart, starting now.
    pub fn spawn(mut self, interval: Duration) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                if let Err(e) = self.pass().await {
                    tracing::warn!(error = %e, "looking for moved refs to warm failed");
                }
                tokio::time::sleep(interval).await;
            }
        })
    }
}

fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs() as i64)
}

fn lease_lost() -> leep_core::Error {
    leep_core::Error::Other("stopped deriving: the index lease was lost".into())
}

/// Lets a derivation write while this process holds a group's lease.
struct LeaseGuard {
    leases: Arc<IndexLeases>,
    group: IndexGroup,
}

#[async_trait]
impl crate::repo::WriteGuard for LeaseGuard {
    async fn allowed(&self) -> bool {
        self.leases.check(self.group).await
    }
}
