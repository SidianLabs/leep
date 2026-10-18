//! Derived data framework. Each kind computes `value(changeset)` from the
//! changeset and its parents' values; [`derive`] walks missing ancestors with
//! an explicit stack (histories are too deep for recursion), derives them
//! oldest-first and caches each under `(kind, changeset id)` in the `MetaStore`.

mod generation;
pub mod history_pack;
mod history_tree;
mod last_merge;
mod log_filter;
mod root_tree;
mod skiplist;

use std::collections::{HashMap, HashSet};
use std::ops::Range;

use async_trait::async_trait;
use bytes::Bytes;
use leep_core::{Changeset, ChangesetId, DerivedRow, Error, Hash, MetaStore, Result};

use crate::Repo;

pub use generation::Generation;
pub use history_tree::{
    chunk_ranges, flat_row, split_row, worth_patching, ChunkRef, HistoryEntry, HistoryNode,
    HistoryTree, NodeRef, ResolvedRow, Row, SplitChange, StoredHistoryNode,
    NODE_KIND as HISTORY_NODE_KIND,
};
pub(crate) use history_tree::{ends_chunk, run_ranges, FLAT_MAX};
pub use last_merge::LastMerge;
pub(crate) use log_filter::LogFilterCaches;
pub use log_filter::{
    author_fingerprint, class_of, key_fingerprint, level_count, name_key, own_fingerprints,
    value_fingerprint, FilterRow, Level, LogFilterIndex, CLASS_AUTHOR, CLASS_KEY, CLASS_VALUE,
    INDEXED_VALUES, LEVEL_CAP, NAME_KIND as LOG_FILTER_NAME_KIND,
};
pub use root_tree::RootTree;
pub use skiplist::{SkipEntry, Skiplist};

/// A kind of derived data keyed by changeset id.
#[async_trait]
pub trait DerivedKind: Send + Sync + 'static {
    /// Key namespace in the `MetaStore`.
    const NAME: &'static str;

    /// Compute the value for `id`. `parents` holds the already-derived value
    /// of every parent, in order.
    async fn derive(
        repo: &Repo,
        id: ChangesetId,
        cs: &Changeset,
        parents: &[Bytes],
    ) -> Result<Bytes>;

    /// Whether [`derive`] may hold this kind's rows back and store a window
    /// of changesets' rows in one write, through [`DerivedKind::derive_staged`].
    /// Values held back are found by [`derive`] (a merge asks for its merge
    /// base's), and other rows through [`DerivedKind::derive_staged`]'s own
    /// staging, until they are stored.
    const STAGED: bool = false;

    /// [`DerivedKind::derive`], adding any other rows it stores to `rows`
    /// instead of writing them, for a kind that is [`DerivedKind::STAGED`].
    /// Reads of staged rows must still find them until
    /// [`DerivedKind::unstage`].
    async fn derive_staged(
        repo: &Repo,
        id: ChangesetId,
        cs: &Changeset,
        parents: &[Bytes],
        rows: &mut Vec<DerivedRow>,
    ) -> Result<Bytes> {
        let _ = rows;
        Self::derive(repo, id, cs, parents).await
    }

    /// Called with staged rows once they are stored, or dropped after an
    /// error.
    fn unstage(_repo: &Repo, _rows: &[DerivedRow]) {}

    /// Called before each window of changesets is derived, oldest first, to
    /// read what their derivations will need in a few round trips.
    async fn prepare(_repo: &Repo, _window: &[(ChangesetId, &Changeset)]) {}

    /// Whether a derivation of this kind keeps the skiplist rows its
    /// ancestry queries read until it ends (see `ancestry::SKIPLIST_READS`).
    const KEEP_SKIPLIST_READS: bool = false;
}

tokio::task_local! {
    /// Kinds whose derive lock this task holds, so a derivation that asks for
    /// its own kind again (a merge asking for its merge base) does not wait on
    /// itself.
    static HELD: std::cell::RefCell<Vec<&'static str>>;
}

/// Get (deriving and caching if needed) kind `K` for `id`.
///
/// Deriving a kind takes that kind's lock, so concurrent requests (and the
/// background warmer) never derive the same range twice: the second waits,
/// then finds the rows the first stored. Kinds only depend on other kinds
/// without cycles, so the locks are always taken in one order.
pub async fn derive<K: DerivedKind>(repo: &Repo, id: ChangesetId) -> Result<Bytes> {
    if let Some(v) = staged_value(repo, K::NAME, id) {
        return Ok(v);
    }
    if let Some(v) = repo.meta().get_derived(K::NAME, &id.0).await? {
        return Ok(v);
    }
    let held = HELD
        .try_with(|h| h.borrow().contains(&K::NAME))
        .unwrap_or(false);
    if held {
        return derive_missing::<K>(repo, id).await;
    }
    let lock = repo.derive_lock(K::NAME);
    let _guard = lock.lock().await;
    if let Some(v) = staged_value(repo, K::NAME, id) {
        return Ok(v);
    }
    if let Some(v) = repo.meta().get_derived(K::NAME, &id.0).await? {
        return Ok(v);
    }
    let mut kinds = HELD.try_with(|h| h.borrow().clone()).unwrap_or_default();
    kinds.push(K::NAME);
    let reads = crate::ancestry::skiplist_reads_for(K::KEEP_SKIPLIST_READS);
    HELD.scope(
        std::cell::RefCell::new(kinds),
        crate::ancestry::SKIPLIST_READS.scope(reads, derive_missing::<K>(repo, id)),
    )
    .await
}

/// Changesets read or checked per round trip while finding and deriving a
/// missing range.
const BATCH: usize = 256;

async fn derive_missing<K: DerivedKind>(repo: &Repo, id: ChangesetId) -> Result<Bytes> {
    let meta = repo.meta();
    // Find the changesets lacking a value, a batch of them per round trip,
    // keeping only their parents: a whole range of changesets (Linux has 1.5
    // million) does not fit in memory, and each is read again when derived.
    let mut parents: HashMap<ChangesetId, Range<usize>> = HashMap::new();
    let mut arena: Vec<ChangesetId> = Vec::new();
    let mut seen: HashSet<ChangesetId> = HashSet::from([id]);
    let mut frontier = vec![id];
    let mut first = true;
    while !frontier.is_empty() {
        let mut missing = Vec::with_capacity(frontier.len());
        for chunk in frontier.chunks(BATCH) {
            // The head was checked by the caller.
            let have = if first {
                vec![None]
            } else {
                get_derived_many(meta.as_ref(), K::NAME, chunk).await?
            };
            first = false;
            missing.extend(
                chunk
                    .iter()
                    .zip(have)
                    .filter(|(_, v)| v.is_none())
                    .map(|(id, _)| *id),
            );
        }
        frontier.clear();
        for chunk in missing.chunks(BATCH) {
            for (cid, cs) in chunk.iter().zip(get_changesets(repo, chunk).await?) {
                let start = arena.len();
                arena.extend_from_slice(&cs.parents);
                parents.insert(*cid, start..arena.len());
                frontier.extend(cs.parents.iter().filter(|p| seen.insert(**p)));
            }
        }
    }
    drop(seen);

    // Parents before children: iterative post-order over the parents found.
    let mut order: Vec<ChangesetId> = Vec::with_capacity(parents.len());
    let mut done: HashSet<ChangesetId> = HashSet::with_capacity(parents.len());
    let mut stack = vec![(id, 0usize)];
    done.insert(id);
    while let Some((top, next)) = stack.last_mut() {
        let ps = &arena[parents[top].clone()];
        if let Some(&p) = ps.get(*next) {
            *next += 1;
            if parents.contains_key(&p) && done.insert(p) {
                stack.push((p, 0));
            }
        } else {
            order.push(*top);
            stack.pop();
        }
    }
    drop(done);

    let mut staged: Vec<DerivedRow> = Vec::new();
    let result = derive_order::<K>(repo, &order, &arena, &parents, &mut staged).await;
    let result = match result {
        Ok(v) => write_staged::<K>(repo, &mut staged).await.map(|()| v),
        Err(e) => Err(e),
    };
    K::unstage(repo, &staged);
    forget_staged_values(repo, K::NAME, &staged);
    result
}

/// A value of `kind` computed by a running derivation and not stored yet.
fn staged_value(repo: &Repo, kind: &'static str, id: ChangesetId) -> Option<Bytes> {
    repo.staged_values
        .lock()
        .unwrap()
        .get(&(kind, id.0))
        .cloned()
}

/// Forget the staged values among `rows`, once stored or dropped.
fn forget_staged_values(repo: &Repo, kind: &'static str, rows: &[DerivedRow]) {
    let mut values = repo.staged_values.lock().unwrap();
    for row in rows.iter().filter(|r| r.kind == kind) {
        values.remove(&(kind, row.key));
    }
}

/// Most staged rows held back before a write.
const STAGED_ROWS: usize = 4096;

/// Derive `order` (parents first), holding back a [`DerivedKind::STAGED`]
/// kind's rows in `staged` and writing them a window at a time.
async fn derive_order<K: DerivedKind>(
    repo: &Repo,
    order: &[ChangesetId],
    arena: &[ChangesetId],
    parents: &HashMap<ChangesetId, Range<usize>>,
    staged: &mut Vec<DerivedRow>,
) -> Result<Bytes> {
    let meta = repo.meta();
    // A computed value is kept until its last child in the range has used
    // it, so memory follows the width of history, not its length.
    let mut children: HashMap<ChangesetId, u32> = HashMap::new();
    for cid in order {
        for p in &arena[parents[cid].clone()] {
            if parents.contains_key(p) {
                *children.entry(*p).or_default() += 1;
            }
        }
    }
    let mut computed: HashMap<ChangesetId, Bytes> = HashMap::new();
    let mut last = None;
    for window in order.chunks(BATCH) {
        let changesets = get_changesets(repo, window).await?;
        let pairs: Vec<(ChangesetId, &Changeset)> =
            window.iter().copied().zip(changesets.iter()).collect();
        K::prepare(repo, &pairs).await;
        for (cid, cs) in pairs {
            let mut pv = Vec::with_capacity(cs.parents.len());
            for p in &cs.parents {
                let v = match computed.get(p) {
                    Some(v) => {
                        let v = v.clone();
                        let left = children.get_mut(p).expect("counted above");
                        *left -= 1;
                        if *left == 0 {
                            computed.remove(p);
                        }
                        v
                    }
                    None => meta
                        .get_derived(K::NAME, &p.0)
                        .await?
                        .ok_or_else(|| Error::Storage(format!("{} missing for {p}", K::NAME)))?,
                };
                pv.push(v);
            }
            let started = std::time::Instant::now();
            #[cfg(test)]
            repo.derive_calls
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let v = if K::STAGED {
                let v = K::derive_staged(repo, cid, cs, &pv, staged).await?;
                repo.staged_values
                    .lock()
                    .unwrap()
                    .insert((K::NAME, cid.0), v.clone());
                staged.push(DerivedRow::new(K::NAME, cid.0, v.clone()));
                if staged.len() >= STAGED_ROWS {
                    write_staged::<K>(repo, staged).await?;
                }
                v
            } else {
                let v = K::derive(repo, cid, cs, &pv).await?;
                meta.put_derived(K::NAME, &cid.0, v.clone()).await?;
                v
            };
            record_derivation(K::NAME, started);
            if children.contains_key(&cid) {
                computed.insert(cid, v.clone());
            }
            last = Some(v);
        }
        write_staged::<K>(repo, staged).await?;
    }
    last.ok_or_else(|| Error::Storage("derivation produced no value".into()))
}

/// Store staged rows in order, a bounded batch per write. Each changeset's
/// rows end with its value, after the rows it names, so any prefix a failed
/// write leaves behind holds no dangling reference.
async fn write_staged<K: DerivedKind>(repo: &Repo, staged: &mut Vec<DerivedRow>) -> Result<()> {
    if staged.is_empty() {
        return Ok(());
    }
    for batch in staged.chunks(STAGED_WRITE) {
        repo.meta().put_derived_many(batch).await?;
    }
    K::unstage(repo, staged);
    forget_staged_values(repo, K::NAME, staged);
    staged.clear();
    Ok(())
}

/// Most rows one staged write carries.
const STAGED_WRITE: usize = 1024;

/// `ids`' values of `kind`, in one read if the store can, else one by one.
async fn get_derived_many(
    meta: &dyn MetaStore,
    kind: &str,
    ids: &[ChangesetId],
) -> Result<Vec<Option<Bytes>>> {
    let keys: Vec<Hash> = ids.iter().map(|id| id.0).collect();
    if let Ok(v) = meta.get_derived_many(kind, &keys).await {
        if v.len() == keys.len() {
            return Ok(v);
        }
    }
    let mut out = Vec::with_capacity(keys.len());
    for k in &keys {
        out.push(meta.get_derived(kind, k).await?);
    }
    Ok(out)
}

/// The changesets `ids`, in one read if the store can, else one by one;
/// `NotFound` if any is absent.
async fn get_changesets(repo: &Repo, ids: &[ChangesetId]) -> Result<Vec<Changeset>> {
    if let Ok(v) = repo.meta().get_changesets(ids).await {
        if v.len() == ids.len() {
            if let Some(found) = v.into_iter().collect::<Option<Vec<_>>>() {
                return Ok(found);
            }
        }
    }
    let mut out = Vec::with_capacity(ids.len());
    for &id in ids {
        out.push(repo.get_changeset(id).await?);
    }
    Ok(out)
}

/// Count one computed value of `kind` and its time. Also used where a value
/// is computed outside [`derive`], as changeset creation does for the rows it
/// writes with the changeset, so the metrics keep counting every derivation.
pub(crate) fn record_derivation(kind: &'static str, started: std::time::Instant) {
    metrics::counter!("leep_derive_total", "kind" => kind).increment(1);
    metrics::histogram!("leep_derive_duration_seconds", "kind" => kind)
        .record(started.elapsed().as_secs_f64());
}

pub(crate) fn decode_hash(b: &[u8]) -> Result<Hash> {
    let arr: [u8; 32] = b
        .try_into()
        .map_err(|_| Error::Encoding("bad hash value".into()))?;
    Ok(Hash(arr))
}

pub(crate) fn decode_u64(b: &[u8]) -> Result<u64> {
    let arr: [u8; 8] = b
        .try_into()
        .map_err(|_| Error::Encoding("bad u64 value".into()))?;
    Ok(u64::from_be_bytes(arr))
}
