use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
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
