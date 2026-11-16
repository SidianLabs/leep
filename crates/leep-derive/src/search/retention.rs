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
//! - **A head that appears during the pass.** Before each delete the pass
//!   checks whether the ref log moved, and if so the blobs of any new head
//!   join the live set first. It checks again after the delete, and a
//!   deleted row a head that appeared meanwhile holds is derived again at
//!   once; a restore that fails is retried by the next pass before anything
//!   else. A head that appears after that check is derived by the warmer
//!   after the delete took effect, and a derivation computes every missing
//!   row of the directories it derives.
//! - **A head whose directory filters are stored but whose blob rows are
//!   not** (a branch or revert to content older than `keep`, a rollback, a
//!   restarted process that lost a pending restore) is found by the
//!   backfill: each pass walks the current heads' tree nodes it has not
//!   checked yet, asks which of their blobs lack a row (keys only) and
//!   derives a bounded number of them.
//! - **Keyframes** (`blob-trigrams-keyframe-v2`) are never deleted here: a
//!   live patch may stand on the keyframe of a blob no head holds.
//!
//! Memory: liveness is a Bloom filter over the window's blobs (a false
//! positive only keeps a row), about 1.2 to 2.5 bytes a blob; the tree
//! nodes walked are held exactly, since skipping a node would miss its
//! blobs. The walks never fill the shared node cache.
//!
//! Nothing here is on the land path: the server runs passes in the index
//! lane (`leep-server`, `search_retention`).

use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::{Duration, Instant};

use leep_core::{ChangesetId, Error, FileKind, Hash, LeaseKey, Result, TreeEntry};

use super::index::{Counters, BLOB_KIND, RETIRED_BLOB_KIND};
use super::trigram::BlobRow;
use crate::gc_lease::{self, GcLease};
use crate::Repo;

/// How long a blob's row outlives the last head that held it by default:
/// two weeks, so revisions of the last two weekly cycles (open reviews,
/// the bases agents branch from, last week's release) search as fast as
/// heads.
pub const DEFAULT_KEEP: Duration = Duration::from_secs(14 * 24 * 3600);

/// Ref log entries one read asks for.
const REF_LOG_PAGE: usize = 256;
/// Tree nodes one batched read asks for.
const TREE_BATCH: usize = 1024;
/// A pass runs at least this often even when nothing moved, so heads that
/// left the window have their rows deleted.
const FORCE_EVERY: Duration = Duration::from_secs(24 * 3600);
/// Tree nodes the backfill remembers as checked; past it, it forgets them
/// all and checks again (a full walk of the current heads).
const VERIFIED_CAP: usize = 1 << 20;
/// Attempts at deriving a deleted row again before leaving it to the next
/// pass.
const RESTORE_TRIES: usize = 3;

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
    state: &mut RetentionState,
) -> Result<Option<RetentionReport>> {
    let lease = match GcLease::acquire_key(repo.meta(), lease_key(), "search-retention").await {
        Ok(lease) => lease,
        Err(Error::Conflict(_)) => return Ok(None),
        Err(e) => return Err(e),
    };
    let result = repo
        .retire_search_rows(opts, now, state, &|| gc_lease::check(&lease))
        .await;
    gc_lease::release(lease, result).await.map(Some)
}

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
    /// Ref log entries are read this far past the window's start: servers
    /// stamp them by their own clocks, so an entry stamped this much early
    /// still counts. Only the window's edge depends on it; current heads
    /// come from the refs themselves.
    pub skew: Duration,
    /// Most missing blob rows the backfill derives in one pass.
    pub backfill_rows: usize,
}

impl Default for RetentionOptions {
    fn default() -> Self {
        RetentionOptions {
            keep: DEFAULT_KEEP,
            batch: 500,
            rest_factor: 3,
            skew: Duration::from_secs(300),
            backfill_rows: 4096,
        }
    }
}

/// What a server keeps from one pass to the next.
#[derive(Default)]
pub struct RetentionState {
    /// The newest ref log entry when the last complete pass started.
    last_seq: Option<u64>,
    /// Rows the last complete pass deleted.
    last_deleted: u64,
    last_full: Option<Instant>,
    /// Deleted rows a head holds whose restore failed: derived again first.
    pending: Vec<Hash>,
    /// Tree nodes whose files all had rows when the backfill looked.
    verified: HashSet<Hash>,
    /// Whether the last backfill checked every current head to the end.
    backfilled: bool,
}

/// What one pass did.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize)]
pub struct RetentionReport {
    /// Nothing moved and the last pass deleted nothing: skipped.
    pub skipped: bool,
    /// Heads whose blobs are live: current, or current within the window.
    pub heads: usize,
    /// Distinct tree nodes walked, and blob entries added to the live
    /// filter.
    pub trees: usize,
    pub live_blobs: usize,
    /// Rows listed, of both kinds.
    pub examined: u64,
    /// Rows deleted, of both kinds.
    pub deleted: u64,
    /// Deleted rows derived again because a head that appeared during the
    /// pass holds their blobs, or left pending by an earlier pass.
    pub restored: u64,
    /// Missing rows of current heads' blobs the backfill derived.
    pub backfilled: u64,
}

/// A Bloom filter over blob hashes: no false negatives, so a blob it does
/// not hold is certainly not live.
struct Bloom {
    bits: Vec<u64>,
    mask: u64,
}

/// Probes per hash: about 1% false positives at 10 bits a blob.
const BLOOM_PROBES: u64 = 7;

impl Bloom {
    /// A filter for about `n` blobs, 10 bits each, rounded up to a power of
    /// two.
    fn with_capacity(n: usize) -> Bloom {
        let bits = (n.max(1 << 12) * 10).next_power_of_two();
        Bloom {
            bits: vec![0; bits / 64],
            mask: bits as u64 - 1,
        }
    }

    /// Bit indices of `h`: double hashing over two halves of the (already
    /// uniform) content hash.
    fn indices(&self, h: &Hash) -> impl Iterator<Item = u64> + '_ {
        let b = h.as_bytes();
        let h1 = u64::from_le_bytes(b[0..8].try_into().expect("8 bytes"));
        let h2 = u64::from_le_bytes(b[8..16].try_into().expect("8 bytes")) | 1;
        (0..BLOOM_PROBES).map(move |i| h1.wrapping_add(i.wrapping_mul(h2)) & self.mask)
    }

    fn insert(&mut self, h: &Hash) {
        let idx: Vec<u64> = self.indices(h).collect();
        for i in idx {
            self.bits[(i / 64) as usize] |= 1 << (i % 64);
        }
    }

    fn contains(&self, h: &Hash) -> bool {
        self.indices(h)
            .all(|i| self.bits[(i / 64) as usize] >> (i % 64) & 1 == 1)
    }
}

/// The live set: the blobs below the heads, and what was walked to find
/// them.
struct Live {
    /// The newest ref log entry read before the refs were last listed.
    seq: Option<u64>,
    heads: HashSet<ChangesetId>,
    trees: HashSet<Hash>,
    blobs: Bloom,
    added: usize,
}

/// A tree node as the live walk keeps it: the files first found in it and
/// its subdirectories.
type WalkedNode = (Box<[Hash]>, Box<[Hash]>);

/// What retention keeps live, as a derived segment pass of family
/// `content-v1` needs it (ADR 0106): every blob and tree node below a head
/// current now or within the window, each numbered by its first place in a
/// depth-first walk of those heads' trees (a directory, then its files, then
/// its subdirectories), so a directory's filter and its files' rows sit in
/// the same blocks of a segment.
#[derive(Debug)]
pub(crate) struct ContentLive {
    /// The newest ref log entry read before the refs were listed: a pass
    /// trusts this set only while it is still the newest.
    pub seq: Option<u64>,
    /// Retention is off: every row is kept, so every row is live (`order`
    /// still numbers the current heads' content).
    pub all: bool,
    order: HashMap<Hash, u32>,
}

impl ContentLive {
    /// Whether retention keeps `key`'s row.
    pub fn live(&self, key: &Hash) -> bool {
        self.all || self.order.contains_key(key)
    }

    /// `key`'s place in the walk, if a head holds it.
    pub fn order(&self, key: &Hash) -> Option<u32> {
        self.order.get(key).copied()
    }
}

impl Repo {
    /// [`ContentLive`] at `now` (Unix seconds): the heads of every ref, then
    /// with `keep` (retention on) every head the ref log names since `keep`
    /// (and `skew`) before `now`, newest first, as a retention pass finds
    /// them. Trees are read a level at a time, without filling the shared
    /// node cache, each distinct node once.
    pub(crate) async fn content_live(
        &self,
        keep: Option<Duration>,
        skew: Duration,
        now: i64,
    ) -> Result<ContentLive> {
        let walker = self.without_node_caching();
        let seq = self.newest_seq().await?;
        let refs = self.meta().list_refs().await?;
        let mut heads: Vec<ChangesetId> = refs.values().copied().collect();
        if let Some(keep) = keep {
            let cutoff = now
                .saturating_sub(keep.as_secs().min(i64::MAX as u64) as i64)
                .saturating_sub(skew.as_secs().min(i64::MAX as u64) as i64);
            heads.extend(self.heads_since(cutoff).await?);
        }
        let mut seen = HashSet::new();
        heads.retain(|h| seen.insert(*h));
        let roots = walker.roots_of(&heads).await?;
        // Each node's subdirectories, and the files first found in it (a
        // file under many directories, or many versions of one, is kept
        // once: the order is a locality key, so where a shared file lands
        // does not matter). Every list sized exactly; the files seen are
        // dropped before the order is built, so the peak is one of them.
        let mut nodes: HashMap<Hash, WalkedNode> = HashMap::new();
        let mut queued: HashSet<Hash> = HashSet::new();
        let mut seen_files: HashSet<Hash> = HashSet::new();
        let mut level: Vec<Hash> = roots
            .iter()
            .filter(|r| queued.insert(**r))
            .copied()
            .collect();
        while !level.is_empty() {
            let mut next = Vec::new();
            for chunk in level.chunks(TREE_BATCH) {
                for (tree, node) in chunk.iter().zip(walker.read_trees_shared(chunk).await?) {
                    let (mut files, mut dirs) = (Vec::new(), Vec::new());
                    for entry in node.entries.values() {
                        match *entry {
                            TreeEntry::Dir { tree } => dirs.push(tree),
                            TreeEntry::File { content, .. } => {
                                if seen_files.insert(content) {
                                    files.push(content);
                                }
                            }
                        }
                    }
                    next.extend(dirs.iter().filter(|d| queued.insert(**d)).copied());
                    nodes.insert(*tree, (files.into_boxed_slice(), dirs.into_boxed_slice()));
                }
            }
            level = next;
        }
        let total = nodes.len() + seen_files.len();
        drop(seen_files);
        drop(queued);
        let mut order: HashMap<Hash, u32> = HashMap::with_capacity(total);
        let mut stack: Vec<Hash> = Vec::new();
        for root in roots {
            stack.push(root);
            while let Some(tree) = stack.pop() {
                if order.contains_key(&tree) {
                    continue;
                }
                let at = order.len() as u32;
                order.insert(tree, at);
                let Some((files, dirs)) = nodes.remove(&tree) else {
                    continue;
                };
                for f in files.iter() {
                    let at = order.len() as u32;
                    order.entry(*f).or_insert(at);
                }
                stack.extend(dirs.iter().rev().filter(|d| !order.contains_key(d)));
            }
        }
        drop(nodes);
        order.shrink_to_fit();
        Ok(ContentLive {
            seq,
            all: keep.is_none(),
            order,
        })
    }

    /// The newest ref log entry's `seq` (`None` for an empty log): a
    /// [`ContentLive`] is current while this has not moved.
    pub(crate) async fn newest_ref_seq(&self) -> Result<Option<u64>> {
        self.newest_seq().await
    }

    /// One retention pass over the blob trigram rows (see the module docs),
    /// at `now` (Unix seconds). `check` is asked before every delete, and an
    /// error from it stops the pass: the server passes its lease check.
    pub async fn retire_search_rows(
        &self,
        opts: &RetentionOptions,
        now: i64,
        state: &mut RetentionState,
        check: &(dyn Fn() -> Result<()> + Send + Sync),
    ) -> Result<RetentionReport> {
        let walker = self.without_node_caching();
        let mut report = RetentionReport::default();
        // Restores an earlier pass could not finish come first.
        if !state.pending.is_empty() {
            let pending = std::mem::take(&mut state.pending);
            let (done, left) = self.restore(&pending).await;
            report.restored += done;
            if let Some((rest, e)) = left {
                state.pending = rest;
                return Err(e);
            }
        }
        let seq = self.newest_seq().await?;
        let quiet = seq == state.last_seq
            && state.last_deleted == 0
            && state.backfilled
            && state.last_full.is_some_and(|t| t.elapsed() < FORCE_EVERY);
        if quiet {
            report.skipped = true;
            return Ok(report);
        }
        let refs = self.meta().list_refs().await?;
        report.backfilled = walker.backfill(&refs, opts, state).await?;

        let cutoff = now
            .saturating_sub(opts.keep.as_secs().min(i64::MAX as u64) as i64)
            .saturating_sub(opts.skew.as_secs().min(i64::MAX as u64) as i64);
        let rows = self.meta().estimate_derived(BLOB_KIND).await?.rows
            + self.meta().estimate_derived(RETIRED_BLOB_KIND).await?.rows;
        let mut live = Live {
            seq,
            heads: HashSet::new(),
            trees: HashSet::new(),
            blobs: Bloom::with_capacity((rows as usize).saturating_mul(2)),
            added: 0,
        };
        let mut heads: Vec<ChangesetId> = refs.values().copied().collect();
        heads.extend(self.heads_since(cutoff).await?);
        walker.add_heads(&mut live, heads).await?;
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
                    .filter(|k| !live.blobs.contains(k))
                    .collect();
                if doomed.is_empty() {
                    continue;
                }
                // Heads that appeared since the walk join the live set
                // before anything is deleted.
                walker.add_moved_refs(&mut live).await?;
                doomed.retain(|k| !live.blobs.contains(k));
                if doomed.is_empty() {
                    continue;
                }
                check()?;
                let n = self.meta().delete_derived_keys(&[kind], &doomed).await?;
                report.deleted += n;
                metrics::counter!("leep_search_retention_rows_total", "kind" => kind, "outcome" => "deleted")
                    .increment(n);
                // A head that appeared between that check and the delete
                // may hold a blob whose row is gone: derive it again now.
                let fresh = walker.add_moved_refs(&mut live).await?;
                let back: Vec<Hash> = doomed
                    .iter()
                    .filter(|k| fresh.contains(*k))
                    .copied()
                    .collect();
                if !back.is_empty() {
                    let (done, left) = self.restore(&back).await;
                    report.restored += done;
                    if let Some((rest, e)) = left {
                        state.pending = rest;
                        return Err(e);
                    }
                }
                let rest = started.elapsed() * opts.rest_factor;
                if !rest.is_zero() {
                    tokio::time::sleep(rest.min(Duration::from_secs(10))).await;
                }
            }
        }
        state.last_seq = seq;
        state.last_deleted = report.deleted;
        state.last_full = Some(Instant::now());
        report.heads = live.heads.len();
        report.trees = live.trees.len();
        report.live_blobs = live.added;
        metrics::gauge!("leep_search_retention_live_blobs").set(live.added as f64);
        Ok(report)
    }

    /// The newest ref log entry's `seq`, `None` for an empty log.
    async fn newest_seq(&self) -> Result<Option<u64>> {
        Ok(self
            .meta()
            .ref_log(None, None, 1)
            .await?
            .first()
            .map(|e| e.seq))
    }

    /// Derive the whole rows of deleted `blobs` again, each tried
    /// [`RESTORE_TRIES`] times. Returns how many were stored and, if one
    /// could not be, those left (it included) with the error.
    async fn restore(&self, blobs: &[Hash]) -> (u64, Option<(Vec<Hash>, Error)>) {
        let mut done = 0;
        for (i, blob) in blobs.iter().enumerate() {
            let mut last = None;
            for _ in 0..RESTORE_TRIES {
                match self.restore_one(*blob).await {
                    Ok(()) => {
                        last = None;
                        break;
                    }
                    Err(e) => last = Some(e),
                }
            }
            if let Some(e) = last {
                return (done, Some((blobs[i..].to_vec(), e)));
            }
            done += 1;
            metrics::counter!("leep_search_retention_rows_total", "kind" => BLOB_KIND, "outcome" => "restored")
                .increment(1);
        }
        (done, None)
    }

    /// The whole row of `blob`, computed from its bytes and stored unless a
    /// row is there by now. A blob no longer stored (collected) needs none.
    async fn restore_one(&self, blob: Hash) -> Result<()> {
        let Some(data) = self.read_content(&blob).await? else {
            return Ok(());
        };
        let row = bytes::Bytes::from(BlobRow::of(&data).encode());
        self.meta()
            .put_derived_if_absent(BLOB_KIND, &blob, row)
            .await?;
        Ok(())
    }

    /// Every head a ref held at some time since `cutoff`: the old and new
    /// heads of the ref log's moves since then, newest first. Entries are
    /// stamped by the clock of the server that applied them, so `cutoff`
    /// already includes the allowance for skew.
    async fn heads_since(&self, cutoff: i64) -> Result<Vec<ChangesetId>> {
        let mut out = Vec::new();
        let mut before = None;
        loop {
            let page = self.meta().ref_log(None, before, REF_LOG_PAGE).await?;
            let full = page.len() == REF_LOG_PAGE;
            let mut past = false;
            for e in page {
                before = Some(e.seq);
                if e.time < cutoff {
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

    /// If the ref log moved since the refs were last listed, list them
    /// again and add the blobs of heads not yet live; returns the blobs
    /// found below trees not walked before (exactly, not through the
    /// filter). The newest `seq` is read before the refs, so a move after
    /// the read shows in the listing or in a later `seq`.
    async fn add_moved_refs(&self, live: &mut Live) -> Result<HashSet<Hash>> {
        let seq = self.newest_seq().await?;
        if seq == live.seq {
            return Ok(HashSet::new());
        }
        live.seq = seq;
        let refs = self.meta().list_refs().await?;
        let new: Vec<ChangesetId> = refs
            .values()
            .filter(|h| !live.heads.contains(h))
            .copied()
            .collect();
        self.add_heads(live, new).await
    }

    /// The root trees of `ids`, those whose changeset is gone (collected,
    /// holding nothing a search can reach) left out.
    async fn roots_of(&self, ids: &[ChangesetId]) -> Result<Vec<Hash>> {
        let mut out = Vec::new();
        for chunk in ids.chunks(TREE_BATCH) {
            match self.root_trees(chunk).await {
                Ok(roots) => out.extend(roots),
                Err(_) => {
                    for id in chunk {
                        match self.root_tree(*id).await {
                            Ok(t) => out.push(t),
                            Err(Error::NotFound(_)) => {}
                            Err(e) => return Err(e),
                        }
                    }
                }
            }
        }
        Ok(out)
    }

    /// Walk the trees of `heads` not walked yet, iteratively, and add their
    /// blobs to `live`; returns the blobs found below trees not walked
    /// before.
    async fn add_heads(&self, live: &mut Live, heads: Vec<ChangesetId>) -> Result<HashSet<Hash>> {
        let todo: Vec<ChangesetId> = heads
            .into_iter()
            .filter(|h| live.heads.insert(*h))
            .collect();
        let mut level: Vec<Hash> = self
            .roots_of(&todo)
            .await?
            .into_iter()
            .filter(|t| live.trees.insert(*t))
            .collect();
        let mut added = HashSet::new();
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
                            TreeEntry::File { content, .. } => {
                                live.blobs.insert(&content);
                                live.added += 1;
                                added.insert(content);
                            }
                        }
                    }
                }
            }
            level = next;
        }
        Ok(added)
    }

    /// Derive the missing rows of the current heads' blobs, at most
    /// `opts.backfill_rows` per pass, walking only the tree nodes not
    /// checked before. A node is remembered once every searchable file
    /// below it has a row. Returns the rows derived.
    async fn backfill(
        &self,
        refs: &BTreeMap<String, ChangesetId>,
        opts: &RetentionOptions,
        state: &mut RetentionState,
    ) -> Result<u64> {
        if state.verified.len() > VERIFIED_CAP {
            state.verified.clear();
        }
        let heads: Vec<ChangesetId> = refs
            .values()
            .copied()
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        let mut seen: HashSet<Hash> = HashSet::new();
        let mut level: Vec<Hash> = self
            .roots_of(&heads)
            .await?
            .into_iter()
            .filter(|t| !state.verified.contains(t) && seen.insert(*t))
            .collect();
        let c = Counters::default();
        let mut derived = 0u64;
        let mut complete = true;
        // Per level, each node walked: whether its own files all have rows,
        // and its subdirectories. A node is remembered only once its whole
        // subtree is, so the levels are judged bottom up afterwards.
        let mut walked: Vec<Vec<(Hash, bool, Vec<Hash>)>> = Vec::new();
        while !level.is_empty() {
            let mut this_level = Vec::new();
            let mut next = Vec::new();
            for chunk in level.chunks(TREE_BATCH) {
                let nodes = self.read_trees_shared(chunk).await?;
                let mut files: Vec<(Hash, u64)> = Vec::new();
                for node in &nodes {
                    for entry in node.entries.values() {
                        match *entry {
                            TreeEntry::Dir { tree } => {
                                if !state.verified.contains(&tree) && seen.insert(tree) {
                                    next.push(tree);
                                }
                            }
                            TreeEntry::File {
                                content,
                                kind,
                                size,
                            } if kind != FileKind::Symlink => files.push((content, size)),
                            TreeEntry::File { .. } => {}
                        }
                    }
                }
                files.sort_unstable();
                files.dedup();
                let mut short: HashSet<Hash> = HashSet::new();
                for (blob, size) in self.blobs_without_rows(&files).await? {
                    if derived as usize >= opts.backfill_rows {
                        short.insert(blob);
                        complete = false;
                        continue;
                    }
                    let row =
                        bytes::Bytes::from(self.fresh_blob_row(blob, size, &c).await?.encode());
                    self.meta()
                        .put_derived_if_absent(BLOB_KIND, &blob, row)
                        .await?;
                    derived += 1;
                }
                for (tree, node) in chunk.iter().zip(&nodes) {
                    let own = node.entries.values().all(|e| match e {
                        TreeEntry::File { content, .. } => !short.contains(content),
                        TreeEntry::Dir { .. } => true,
                    });
                    let dirs = node
                        .entries
                        .values()
                        .filter_map(|e| match e {
                            TreeEntry::Dir { tree } => Some(*tree),
                            TreeEntry::File { .. } => None,
                        })
                        .collect();
                    this_level.push((*tree, own, dirs));
                }
            }
            walked.push(this_level);
            level = next;
        }
        for nodes in walked.into_iter().rev() {
            for (tree, own, dirs) in nodes {
                if own && dirs.iter().all(|d| state.verified.contains(d)) {
                    state.verified.insert(tree);
                }
            }
        }
        state.backfilled = complete;
        if derived > 0 {
            metrics::counter!("leep_search_retention_rows_total", "kind" => BLOB_KIND, "outcome" => "backfilled")
                .increment(derived);
        }
        Ok(derived)
    }

    /// Those of `files` with neither a `blob-trigrams-v2` nor a
    /// `blob-trigrams-v1` row, asked for keys only.
    async fn blobs_without_rows(&self, files: &[(Hash, u64)]) -> Result<Vec<(Hash, u64)>> {
        if files.is_empty() {
            return Ok(Vec::new());
        }
        let keys: Vec<Hash> = files.iter().map(|f| f.0).collect();
        let v2 = self.meta().derived_present(BLOB_KIND, &keys).await?;
        let lacking: Vec<(Hash, u64)> = files
            .iter()
            .zip(v2)
            .filter(|(_, p)| !p)
            .map(|(f, _)| *f)
            .collect();
        if lacking.is_empty() {
            return Ok(lacking);
        }
        let keys: Vec<Hash> = lacking.iter().map(|f| f.0).collect();
        let v1 = self
            .meta()
            .derived_present(RETIRED_BLOB_KIND, &keys)
            .await?;
        Ok(lacking
            .into_iter()
            .zip(v1)
            .filter(|(_, p)| !p)
            .map(|(f, _)| f)
            .collect())
    }
}
