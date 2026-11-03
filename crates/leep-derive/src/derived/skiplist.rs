use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::future::{BoxFuture, FutureExt, Shared};
use leep_core::{Changeset, ChangesetId, Error, Hash, Result};

use super::{derive, DerivedKind};
use crate::Repo;

/// A first-parent ancestor and its generation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SkipEntry {
    pub id: ChangesetId,
    pub generation: u64,
}

/// First-parent ancestors at distances 1, 2, 4, ... (entry `k` is 2^k steps
/// up, while the chain is long enough). Value: 40 bytes per entry.
pub struct Skiplist;

impl Skiplist {
    pub fn decode(b: &[u8]) -> Result<Vec<SkipEntry>> {
        let (chunks, rest) = b.as_chunks::<40>();
        if !rest.is_empty() {
            return Err(Error::Encoding("bad skiplist value".into()));
        }
        let entries: Vec<SkipEntry> = chunks
            .iter()
            .map(|c| SkipEntry {
                id: ChangesetId(Hash(c[..32].try_into().expect("32 bytes"))),
                generation: u64::from_be_bytes(c[32..].try_into().expect("8 bytes")),
            })
            .collect();
        // Each entry is farther up the first-parent chain than the last, so
        // a walk riding the list always moves down and ends.
        if !entries
            .windows(2)
            .all(|w| w[0].generation > w[1].generation)
        {
            return Err(Error::Encoding(
                "bad skiplist value: generations do not descend".into(),
            ));
        }
        Ok(entries)
    }

    /// [`Skiplist::decode`] for the row of `id`, which must not name `id`
    /// itself: a walk riding it would go round forever. Every reader that
    /// knows whose row it holds uses this, cached or not.
    pub fn decode_of(id: ChangesetId, b: &[u8]) -> Result<Vec<SkipEntry>> {
        let list = Self::decode(b)?;
        if list.iter().any(|e| e.id == id) {
            return Err(Error::Storage(format!("skiplist of {id} names itself")));
        }
        Ok(list)
    }

    pub(crate) fn encode(entries: &[SkipEntry]) -> Bytes {
        let mut out = Vec::with_capacity(entries.len() * 40);
        for e in entries {
            out.extend_from_slice(e.id.0.as_bytes());
            out.extend_from_slice(&e.generation.to_be_bytes());
        }
        Bytes::from(out)
    }
}

impl Skiplist {
    /// The skiplist of a changeset whose first parent is `parent`. Entry `k`
    /// comes from the skiplist of entry `k - 1`, so the chain is read one
    /// list at a time; `parent_list` is the parent's own list if the caller
    /// has it already. With `remember`, lists read on the way are cached too
    /// (see `SkiplistCache` for who may do that).
    pub(crate) async fn compute(
        repo: &Repo,
        parent: SkipEntry,
        parent_list: Option<Arc<[SkipEntry]>>,
        remember: bool,
    ) -> Result<Arc<[SkipEntry]>> {
        let mut list = vec![parent];
        let mut known = parent_list;
        loop {
            let last = list[list.len() - 1];
            let k = list.len() - 1;
            let anc = match known.take() {
                Some(l) => l,
                None => {
                    let l = repo.skiplist(last.id).await?;
                    if remember {
                        repo.skiplists.insert(last.id, l.clone());
                    }
                    l
                }
            };
            match anc.get(k) {
                Some(e) => list.push(*e),
                None => break,
            }
        }
        Ok(list.into())
    }
}

/// Where an entry of a planned copy's skiplist lies: a stored changeset on
/// the head's first-parent chain, or the `n`th copy stacked on the head.
#[derive(Clone, Copy)]
enum Planned {
    Stored(ChangesetId),
    Copy(usize),
}

/// A land batch attempt's read-ahead of skiplist rows (ADR 0079), running
/// beside its replays; it never fails (a row it misses is read later).
pub(crate) type SkipPlan = Shared<BoxFuture<'static, ()>>;

/// Skiplist plans that may run at once in a process. A plan makes one
/// metadata call at a time, in the land lane, beside its leader's calls;
/// past this many (several refs landing at once), a batch's copies read
/// their rows as they did before plans.
pub(crate) const MAX_RUNNING_SKIP_PLANS: usize = 2;

impl Repo {
    /// Start [`Repo::prefetch_copy_skiplists`] on a task of its own, in
    /// the caller's lane, so it runs while the batch replays; `None` when
    /// [`MAX_RUNNING_SKIP_PLANS`] are running already.
    pub(crate) fn start_copy_skiplists(
        &self,
        head: ChangesetId,
        copies: usize,
    ) -> Option<SkipPlan> {
        let Ok(permit) = Arc::clone(&self.skip_plans).try_acquire_owned() else {
            metrics::counter!("leep_land_skip_plans_total", "outcome" => "skipped").increment(1);
            return None;
        };
        metrics::counter!("leep_land_skip_plans_total", "outcome" => "started").increment(1);
        let repo = self.clone();
        let task = tokio::spawn(leep_core::lane::inherit(async move {
            let started = std::time::Instant::now();
            repo.prefetch_copy_skiplists(head, copies).await;
            metrics::histogram!("leep_land_skip_plan_seconds")
                .record(started.elapsed().as_secs_f64());
            drop(permit);
        }));
        Some(
            async move {
                let _ = task.await;
            }
            .boxed()
            .shared(),
        )
    }

    /// Read the stored skiplists that up to `copies` changesets stacked one
    /// on another on `head` will be built from, into `skiplists`, one
    /// batched read per level (ADR 0079). Copy `i`'s entry `k` comes from
    /// the list of its entry `k - 1`, which is either an earlier copy (whose
    /// entries the plan already has) or a stored changeset on the head's
    /// chain, so level `k` needs only the rows of the stored entries `k - 1`,
    /// all known once level `k - 1` is. A land batch calls this before it
    /// replays, so each copy's [`Skiplist::compute`] finds every list in
    /// memory instead of reading about log2(history) rows one after another.
    /// The lists are the same; only where their inputs are read from
    /// changes. A row that cannot be read or decoded is left to `compute`,
    /// which reads or derives it as before.
    pub(crate) async fn prefetch_copy_skiplists(&self, head: ChangesetId, copies: usize) {
        // Each copy's entry `k - 1`, copies `1..=copies` (index 0 unused).
        let mut level: Vec<Option<Planned>> = (0..=copies)
            .map(|i| match i {
                0 => None,
                1 => Some(Planned::Stored(head)),
                _ => Some(Planned::Copy(i - 1)),
            })
            .collect();
        // Ends when the lists do; the bound only stops a damaged row from
        // naming entries forever (`decode` keeps generations descending).
        for k in 1..=64 {
            let mut seen = HashSet::new();
            let stored: Vec<ChangesetId> = level
                .iter()
                .filter_map(|e| match e {
                    Some(Planned::Stored(id)) => Some(*id),
                    _ => None,
                })
                .filter(|id| seen.insert(*id))
                .collect();
            if stored.is_empty() {
                break;
            }
            let mut lists: HashMap<ChangesetId, Arc<[SkipEntry]>> =
                HashMap::with_capacity(stored.len());
            let mut missing = Vec::new();
            for id in stored {
                match self.skiplists.get(&id) {
                    Some(l) => {
                        lists.insert(id, l);
                    }
                    None => missing.push(id),
                }
            }
            if !missing.is_empty() {
                let keys: Vec<Hash> = missing.iter().map(|id| id.0).collect();
                if let Ok(rows) = self.meta().get_derived_many(Skiplist::NAME, &keys).await {
                    if rows.len() == keys.len() {
                        for (id, row) in missing.into_iter().zip(rows) {
                            let Some(list) = row.and_then(|b| Skiplist::decode_of(id, &b).ok())
                            else {
                                continue;
                            };
                            let list: Arc<[SkipEntry]> = list.into();
                            self.skiplists.insert(id, list.clone());
                            lists.insert(id, list);
                        }
                    }
                }
            }
            level = level
                .iter()
                .map(|e| match *e {
                    None => None,
                    // That copy's own entry `k - 1`, from this level.
                    Some(Planned::Copy(j)) => level[j],
                    Some(Planned::Stored(id)) => lists
                        .get(&id)
                        .and_then(|l| l.get(k - 1))
                        .map(|e| Planned::Stored(e.id)),
                })
                .collect();
        }
    }
}

#[async_trait]
impl DerivedKind for Skiplist {
    const NAME: &'static str = "skiplist";

    async fn derive(repo: &Repo, id: ChangesetId, cs: &Changeset, _: &[Bytes]) -> Result<Bytes> {
        let Some(&p) = cs.parents.first() else {
            return Ok(Bytes::new());
        };
        let parent = SkipEntry {
            id: p,
            generation: repo.generation(p).await?,
        };
        let list = Skiplist::compute(repo, parent, None, false).await?;
        repo.skiplists.insert(id, list.clone());
        Ok(Skiplist::encode(&list))
    }
}

impl Repo {
    /// The skiplist of a stored changeset: from the cache (see
    /// `SkiplistCache`), or derived (read, or computed and stored).
    pub(crate) async fn skiplist(&self, id: ChangesetId) -> Result<Arc<[SkipEntry]>> {
        if let Some(l) = self.skiplists.get(&id) {
            return Ok(l);
        }
        Ok(Skiplist::decode_of(id, &derive::<Skiplist>(self, id).await?)?.into())
    }

    /// A list kept by a query (see `SkipReadCache`), without a read.
    pub(crate) fn cached_skips(&self, id: ChangesetId) -> Option<Arc<[SkipEntry]>> {
        self.skip_reads.get(&id.0).map(|s| (*s).clone())
    }

    /// Keep `list`, the skiplist of `id` as read or derived, for later
    /// queries.
    pub(crate) fn cache_skips(&self, id: ChangesetId, list: Arc<[SkipEntry]>) {
        let n = 40 * list.len();
        self.skip_reads.insert(id.0, Arc::new(list), n);
    }

    /// [`Repo::skiplist`] for a query, kept for the next ones.
    pub(crate) async fn query_skiplist(&self, id: ChangesetId) -> Result<Arc<[SkipEntry]>> {
        if let Some(l) = self.cached_skips(id) {
            return Ok(l);
        }
        let l = self.skiplist(id).await?;
        self.cache_skips(id, l.clone());
        Ok(l)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(b: u8, generation: u64) -> SkipEntry {
        SkipEntry {
            id: ChangesetId(Hash([b; 32])),
            generation,
        }
    }

    /// A damaged row whose entries do not move down the chain (so a walk
    /// riding it could go round) is an error, not a list.
    #[test]
    fn generations_must_descend() {
        let good = [entry(1, 9), entry(2, 8), entry(3, 6)];
        assert_eq!(Skiplist::decode(&Skiplist::encode(&good)).unwrap(), good);
        for bad in [
            vec![entry(1, 9), entry(2, 9)],
            vec![entry(1, 6), entry(2, 8)],
            vec![entry(1, 5), entry(1, 5)],
        ] {
            assert!(Skiplist::decode(&Skiplist::encode(&bad)).is_err());
        }
    }

    /// A stored list that names its own changeset is reported, not walked.
    #[tokio::test]
    async fn a_list_naming_itself_is_corrupt() {
        let r = crate::tests::repo();
        let id = crate::tests::commit(&r, &[], &[("a", Some("1"))], "a").await;
        let row = Skiplist::encode(&[entry(0, 3)]);
        let mut looped = row.to_vec();
        looped[..32].copy_from_slice(id.0.as_bytes());
        r.meta()
            .put_derived(Skiplist::NAME, &id.0, looped.into())
            .await
            .unwrap();
        let fresh = Repo::new(r.blobs().clone(), r.meta().clone());
        let err = fresh.skiplist(id).await.expect_err("names itself");
        assert!(matches!(err, Error::Storage(_)), "{err}");
    }

    /// The same row read in a batch into a derivation's skiplist cache
    /// (`prefetch_skiplists`) is refused too, so an ancestry walk served
    /// from that cache errs instead of going round forever.
    #[tokio::test]
    async fn a_prefetched_list_naming_itself_is_not_walked() {
        use crate::ancestry::SKIPLIST_READS;
        let r = crate::tests::repo();
        let root = crate::tests::commit(&r, &[], &[("a", Some("1"))], "a").await;
        let id = crate::tests::commit(&r, &[root], &[("a", Some("2"))], "b").await;
        let looped = Skiplist::encode(&[SkipEntry { id, generation: 5 }]);
        r.meta()
            .put_derived(Skiplist::NAME, &id.0, looped)
            .await
            .unwrap();
        let fresh = Repo::new(r.blobs().clone(), r.meta().clone());
        let cache = std::sync::Arc::new(crate::repo::SkiplistCache::default());
        let walk = SKIPLIST_READS.scope(Some(cache.clone()), async {
            fresh.prefetch_skiplists(&[id]).await;
            fresh.is_ancestor(root, id).await
        });
        let got = tokio::time::timeout(std::time::Duration::from_secs(10), walk)
            .await
            .expect("the walk ends");
        let err = got.expect_err("names itself");
        assert!(matches!(err, Error::Storage(_)), "{err}");
        assert!(cache.get(&id).is_none(), "the row is not cached");
    }
}
