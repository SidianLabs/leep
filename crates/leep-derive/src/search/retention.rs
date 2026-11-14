//! Retention of blob trigram rows (ADR 0102).
//!
//! A blob's row is a pure function of the blob, so a row no search needs
//! soon is only bytes. A pass deletes the `blob-trigrams-v2` rows (and the
//! retired `blob-trigrams-v1` rows) of blobs that no ref head has held for
//! `keep`: the blobs below every head current now or at any time in that
//! window, which the ref log names (a move's old and new heads), are live,
//! and every other row is deleted. A search that needs a deleted row
//! derives it again on its first probe.
//!
//! Safe beside lands, the warmer and searches without coordinating with
//! them:
//!
//! - **A head that appears during the pass.** The refs are read again
//!   before each delete, and the blobs of any new head join the live set
//!   first. They are read once more after the delete, and a deleted row a
//!   head that appeared meanwhile holds is derived again at once. A head
//!   that appears after that read is derived by the warmer after the delete
//!   took effect, and a derivation computes every missing row of the
//!   directories it derives.
//! - **A head that takes over a directory from a revision older than
//!   `keep`** (a revert to old content, a branch from an old commit) reuses
//!   that directory's stored filter, so the warmer derives nothing below it
//!   and the rows retention removed there stay missing until a search
//!   probes them, as for any revision older than `keep`. A bounded search
//!   stores what it computes, so each is computed once.
//! - **Keyframes** (`blob-trigrams-keyframe-v2`) are never deleted here: a
//!   live patch may stand on the keyframe of a blob no head holds.
//!
//! Nothing here is on the land path: the server runs passes on a thread of
//! its own (`leep-server`, `search_retention`).

use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::{Duration, Instant};

use leep_core::{ChangesetId, Error, Hash, LeaseKey, Result, TreeEntry};

use super::index::{Counters, BLOB_KIND, RETIRED_BLOB_KIND};
use crate::gc_lease::{self, GcLease};
use crate::Repo;

/// The store-wide lease a pass holds, so one server at a time runs it.
pub fn lease_key() -> LeaseKey {
    LeaseKey {
        scope: "maintenance".into(),
        repo_id: 0,
        name: "search-retention".into(),
        shard: 0,
    }
}

/// One pass under [`lease_key`]'s lease, at `now` (Unix seconds); `None`
/// when another server holds it.
pub async fn retention_pass(
    repo: &Repo,
    opts: &RetentionOptions,
    now: i64,
) -> Result<Option<RetentionReport>> {
    let lease = match GcLease::acquire_key(repo.meta(), lease_key(), "search-retention").await {
        Ok(lease) => lease,
        Err(Error::Conflict(_)) => return Ok(None),
        Err(e) => return Err(e),
    };
    let result = repo
        .retire_search_rows(opts, now, &|| gc_lease::check(&lease))
        .await;
    gc_lease::release(lease, result).await.map(Some)
}

/// How long a blob's row outlives the last head that held it by default:
/// two weeks, so revisions of the last two weekly cycles (open reviews,
/// the bases agents branch from, last week's release) search as fast as
/// heads.
pub const DEFAULT_KEEP: Duration = Duration::from_secs(14 * 24 * 3600);

/// Ref log entries one read asks for.
const REF_LOG_PAGE: usize = 256;
/// Ref log entries are read this far past the window's start: servers
/// stamp them by their own clocks. Only the window's edge depends on it;
/// current heads come from the refs themselves.
const CLOCK_SKEW_SECS: i64 = 300;
/// Tree nodes one batched read asks for.
const TREE_BATCH: usize = 1024;

/// How a pass runs.
#[derive(Clone, Debug)]
pub struct RetentionOptions {
    /// Rows of blobs no head has held for this long are deleted.
    pub keep: Duration,
    /// Keys listed per read, and the most deleted per statement.
    pub batch: usize,
    /// Rest after each delete, as a multiple of how long its batch took
    /// (listing, checking and deleting), so the pass keeps the database
    /// busy a bounded share of the time.
    pub rest_factor: u32,
}

impl Default for RetentionOptions {
    fn default() -> Self {
        RetentionOptions {
            keep: DEFAULT_KEEP,
            batch: 500,
            rest_factor: 3,
        }
    }
}

/// What one pass did.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize)]
pub struct RetentionReport {
    /// Heads whose blobs are live: current, or current within the window.
    pub heads: usize,
    /// Distinct tree nodes and blobs below them.
    pub trees: usize,
    pub live_blobs: usize,
    /// Rows listed, of both kinds.
    pub examined: u64,
    /// Rows deleted, of both kinds.
    pub deleted: u64,
    /// Deleted rows derived again because a head that appeared during the
    /// pass holds their blobs.
    pub restored: u64,
}

/// The live set: the blobs below the heads, and what was walked to find
/// them.
#[derive(Default)]
struct Live {
    refs: BTreeMap<String, ChangesetId>,
    heads: HashSet<ChangesetId>,
    trees: HashSet<Hash>,
    /// Blob and its size, for deriving a row again without a stat.
    blobs: HashMap<Hash, u64>,
}

impl Repo {
    /// One retention pass over the blob trigram rows (see the module docs),
    /// at `now` (Unix seconds). `check` is asked before every delete, and an
    /// error from it stops the pass: the server passes its lease check.
    pub async fn retire_search_rows(
        &self,
        opts: &RetentionOptions,
        now: i64,
        check: &(dyn Fn() -> Result<()> + Send + Sync),
    ) -> Result<RetentionReport> {
        let cutoff = now.saturating_sub(opts.keep.as_secs().min(i64::MAX as u64) as i64);
        let mut live = Live {
            refs: self.meta().list_refs().await?,
            ..Live::default()
        };
        let mut heads: Vec<ChangesetId> = live.refs.values().copied().collect();
        heads.extend(self.heads_since(cutoff).await?);
        self.add_heads(&mut live, heads).await?;
        let mut report = RetentionReport::default();
        let batch = opts.batch.max(1);
        for kind in [BLOB_KIND, RETIRED_BLOB_KIND] {
            let mut after = None;
            loop {
                let started = Instant::now();
                let keys = self.meta().list_derived_keys(kind, after, batch).await?;
                let Some(&last) = keys.last() else {
                    break;
                };
                after = Some(last);
                report.examined += keys.len() as u64;
                let mut doomed: Vec<Hash> = keys
                    .into_iter()
                    .filter(|k| !live.blobs.contains_key(k))
                    .collect();
                if doomed.is_empty() {
                    continue;
                }
                // Heads that appeared since the walk join the live set
                // before anything is deleted.
                self.add_moved_refs(&mut live).await?;
                doomed.retain(|k| !live.blobs.contains_key(k));
                if doomed.is_empty() {
                    continue;
                }
                check()?;
                let n = self.meta().delete_derived_keys(&[kind], &doomed).await?;
                report.deleted += n;
                metrics::counter!("leep_search_retention_rows_total", "kind" => kind, "outcome" => "deleted")
                    .increment(n);
                // A head that appeared between that read and the delete may
                // hold a blob whose row is gone: derive it again now.
                let fresh = self.add_moved_refs(&mut live).await?;
                let back: Vec<(Hash, u64)> = doomed
                    .iter()
                    .filter_map(|k| fresh.get(k).map(|size| (*k, *size)))
                    .collect();
                if !back.is_empty() {
                    let c = Counters::default();
                    for (blob, size) in &back {
                        self.blob_row_stored(*blob, *size, &c).await?;
                    }
                    report.restored += back.len() as u64;
                    metrics::counter!("leep_search_retention_rows_total", "kind" => BLOB_KIND, "outcome" => "restored")
                        .increment(back.len() as u64);
                }
                let rest = started.elapsed() * opts.rest_factor;
                if !rest.is_zero() {
                    tokio::time::sleep(rest.min(Duration::from_secs(10))).await;
                }
            }
        }
        report.heads = live.heads.len();
        report.trees = live.trees.len();
        report.live_blobs = live.blobs.len();
        metrics::gauge!("leep_search_retention_live_blobs").set(live.blobs.len() as f64);
        Ok(report)
    }

    /// Every head a ref held at some time since `cutoff`: the old and new
    /// heads of the ref log's moves since then, newest first, read a little
    /// past it for clock skew between servers.
    async fn heads_since(&self, cutoff: i64) -> Result<Vec<ChangesetId>> {
        let mut out = Vec::new();
        let mut before = None;
        loop {
            let page = self.meta().ref_log(None, before, REF_LOG_PAGE).await?;
            let full = page.len() == REF_LOG_PAGE;
            let mut past = false;
            for e in page {
                before = Some(e.seq);
                if e.time < cutoff.saturating_sub(CLOCK_SKEW_SECS) {
                    past = true;
                    break;
                }
                out.extend(e.old);
                out.extend(e.new);
            }
            if past || !full {
                return Ok(out);
            }
        }
    }

    /// Read the refs again and add the blobs of heads not yet live; returns
    /// the blobs that became live.
    async fn add_moved_refs(&self, live: &mut Live) -> Result<HashMap<Hash, u64>> {
        let refs = self.meta().list_refs().await?;
        let new: Vec<ChangesetId> = refs
            .values()
            .filter(|h| !live.heads.contains(h))
            .copied()
            .collect();
        live.refs = refs;
        self.add_heads(live, new).await
    }

    /// Walk the trees of `heads` not walked yet, iteratively, and add their
    /// blobs to `live`; returns the blobs that were not live before. A head
    /// whose changeset is gone (collected) holds nothing a search can
    /// reach.
    async fn add_heads(
        &self,
        live: &mut Live,
        heads: Vec<ChangesetId>,
    ) -> Result<HashMap<Hash, u64>> {
        let mut todo: Vec<ChangesetId> = Vec::new();
        for h in heads {
            if live.heads.insert(h) {
                todo.push(h);
            }
        }
        let mut level: Vec<Hash> = Vec::new();
        for chunk in todo.chunks(TREE_BATCH) {
            let roots = match self.root_trees(chunk).await {
                Ok(roots) => roots,
                Err(_) => {
                    let mut roots = Vec::new();
                    for id in chunk {
                        match self.root_tree(*id).await {
                            Ok(t) => roots.push(t),
                            Err(Error::NotFound(_)) => {}
                            Err(e) => return Err(e),
                        }
                    }
                    roots
                }
            };
            for t in roots {
                if live.trees.insert(t) {
                    level.push(t);
                }
            }
        }
        let mut added = HashMap::new();
        while !level.is_empty() {
            let mut next = Vec::new();
            for chunk in level.chunks(TREE_BATCH) {
                for node in self.read_trees_shared(chunk).await? {
                    for entry in node.entries.values() {
                        match *entry {
                            TreeEntry::Dir { tree } => {
                                if live.trees.insert(tree) {
                                    next.push(tree);
                                }
                            }
                            TreeEntry::File { content, size, .. } => {
                                if live.blobs.insert(content, size).is_none() {
                                    added.insert(content, size);
                                }
                            }
                        }
                    }
                }
            }
            level = next;
        }
        Ok(added)
    }
}
