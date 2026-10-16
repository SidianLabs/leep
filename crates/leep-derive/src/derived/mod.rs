//! Derived data framework. Each kind computes `value(changeset)` from the
//! changeset and its parents' values; [`derive`] walks missing ancestors with
//! an explicit stack (histories are too deep for recursion), derives them
//! oldest-first and caches each under `(kind, changeset id)` in the `MetaStore`.

mod generation;
mod history_tree;
mod last_merge;
mod root_tree;
mod skiplist;

use std::collections::{HashMap, HashSet};

use async_trait::async_trait;
use bytes::Bytes;
use leep_core::{Changeset, ChangesetId, Error, Hash, Result};

use crate::Repo;

pub use generation::Generation;
pub use history_tree::{
    ChunkRef, HistoryEntry, HistoryNode, HistoryRow, HistoryTree, StoredHistoryNode,
    NODE_KIND as HISTORY_NODE_KIND,
};
pub use last_merge::LastMerge;
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
}

struct Frame {
    id: ChangesetId,
    cs: Changeset,
    next: usize,
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
    if let Some(v) = repo.meta().get_derived(K::NAME, &id.0).await? {
        return Ok(v);
    }
    let mut kinds = HELD.try_with(|h| h.borrow().clone()).unwrap_or_default();
    kinds.push(K::NAME);
    HELD.scope(
        std::cell::RefCell::new(kinds),
        derive_missing::<K>(repo, id),
    )
    .await
}

async fn derive_missing<K: DerivedKind>(repo: &Repo, id: ChangesetId) -> Result<Bytes> {
    let meta = repo.meta();
    // Iterative post-order DFS over parents that lack a value.
    let mut order: Vec<(ChangesetId, Changeset)> = Vec::new();
    let mut visited: HashSet<ChangesetId> = HashSet::from([id]);
    let mut stack = vec![Frame {
        id,
        cs: repo.get_changeset(id).await?,
        next: 0,
    }];
    while let Some(top) = stack.last_mut() {
        if let Some(&p) = top.cs.parents.get(top.next) {
            top.next += 1;
            if !visited.insert(p) || meta.get_derived(K::NAME, &p.0).await?.is_some() {
                continue;
            }
            let cs = repo.get_changeset(p).await?;
            stack.push(Frame { id: p, cs, next: 0 });
        } else {
            let f = stack.pop().expect("non-empty");
            order.push((f.id, f.cs));
        }
    }
    let mut computed: HashMap<ChangesetId, Bytes> = HashMap::new();
    let mut last = None;
    for (cid, cs) in order {
        let mut pv = Vec::with_capacity(cs.parents.len());
        for p in &cs.parents {
            let v = match computed.get(p) {
                Some(v) => v.clone(),
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
        let v = K::derive(repo, cid, &cs, &pv).await?;
        metrics::counter!("leep_derive_total", "kind" => K::NAME).increment(1);
        metrics::histogram!("leep_derive_duration_seconds", "kind" => K::NAME)
            .record(started.elapsed().as_secs_f64());
        meta.put_derived(K::NAME, &cid.0, v.clone()).await?;
        computed.insert(cid, v.clone());
        last = Some(v);
    }
    last.ok_or_else(|| Error::Storage("derivation produced no value".into()))
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
