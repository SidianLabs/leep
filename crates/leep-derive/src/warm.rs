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

use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, Instant};

use leep_core::{ChangesetId, Result};

use crate::derived::{self, HistoryTree, LogFilterIndex};
use crate::Repo;

/// Ref log entries read per round trip of the tail.
const TAIL_PAGE: usize = 256;
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
    warmed: HashMap<String, ChangesetId>,
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
            warmed: HashMap::new(),
            seq: None,
            last_full: None,
            full_every: FULL_LISTING_EVERY,
        }
    }

    /// List every ref this often instead of [`FULL_LISTING_EVERY`] (tests).
    pub fn with_full_listing_every(mut self, every: Duration) -> Self {
        self.full_every = every;
        self
    }

    /// Warm every ref whose head moved since the last pass. All moved heads
    /// derive as one range per kind ([`derived::derive_heads`]), so history
    /// they share is derived once and different refs' new changesets derive
    /// side by side.
    pub async fn pass(&mut self) -> Result<WarmReport> {
        let moved = self.moved().await?;
        let mut report = WarmReport {
            refs: moved.len(),
            ..Default::default()
        };
        let todo: Vec<(String, Moved)> = moved
            .into_iter()
            .filter(|(name, m)| self.warmed.get(name) != Some(&m.head))
            .collect();
        if todo.is_empty() {
            return Ok(report);
        }
        let started = Instant::now();
        let heads: Vec<ChangesetId> = todo.iter().map(|(_, m)| m.head).collect();
        let together = warm_all(&self.repo, &heads, self.search).await;
        for (name, m) in todo {
            // After a failure together, each ref alone, so one bad ref does
            // not hold the others back.
            let result = match &together {
                Ok(()) => Ok(()),
                Err(_) => warm_all(&self.repo, &[m.head], self.search).await,
            };
            match result {
                Ok(()) => {
                    metrics::counter!("leep_warm_refs_total", "result" => "ok").increment(1);
                    if let Some(at) = m.at {
                        let delay = (unix_now() - at).max(0) as f64;
                        metrics::histogram!("leep_warm_delay_seconds").record(delay);
                    }
                    self.warmed.insert(name, m.head);
                    report.warmed += 1;
                }
                Err(e) => {
                    metrics::counter!("leep_warm_refs_total", "result" => "error").increment(1);
                    tracing::warn!(r#ref = %name, head = %m.head, error = %e, "warming indexes failed; retrying on the next full listing");
                    report.failed += 1;
                }
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
        self.warmed.retain(|name, _| refs.contains_key(name));
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
                    self.warmed.remove(&e.name);
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

/// Derive the indexes queries need for `heads`: the path-history index
/// (which brings generation, skiplists and last-merge with it), the
/// log-filter index and, with `search`, the code-search index of each tree.
/// The first two need only each other's inputs, so they derive side by
/// side, each on the repo's derive jobs (ADR 0050).
async fn warm_all(repo: &Repo, heads: &[ChangesetId], search: bool) -> Result<()> {
    futures_util::try_join!(
        derived::derive_heads::<HistoryTree>(repo, heads),
        derived::derive_heads::<LogFilterIndex>(repo, heads)
    )?;
    if search {
        for &id in heads {
            repo.derive_search(id).await?;
        }
    }
    Ok(())
}
