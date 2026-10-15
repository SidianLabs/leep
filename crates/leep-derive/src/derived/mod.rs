//! Derived data framework. Each kind computes `value(changeset)` from the
//! changeset and its parents' values; [`derive`] walks missing ancestors with
//! an explicit stack (histories are too deep for recursion), derives them
//! oldest-first and caches each under `(kind, changeset id)` in the `MetaStore`.

mod generation;
mod root_tree;
mod skiplist;

use std::collections::{HashMap, HashSet};

use async_trait::async_trait;
use bytes::Bytes;
use leep_core::{Changeset, ChangesetId, Error, Hash, Result};

use crate::Repo;

pub use generation::Generation;
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

/// Get (deriving and caching if needed) kind `K` for `id`.
pub async fn derive<K: DerivedKind>(repo: &Repo, id: ChangesetId) -> Result<Bytes> {
    let meta = repo.meta();
    if let Some(v) = meta.get_derived(K::NAME, &id.0).await? {
        return Ok(v);
    }
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
