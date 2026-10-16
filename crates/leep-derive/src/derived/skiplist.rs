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
        Ok(chunks
            .iter()
            .map(|c| SkipEntry {
                id: ChangesetId(Hash(c[..32].try_into().expect("32 bytes"))),
                generation: u64::from_be_bytes(c[32..].try_into().expect("8 bytes")),
            })
            .collect())
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
    /// has it already.
    pub(crate) async fn compute(
        repo: &Repo,
        parent: SkipEntry,
        parent_list: Option<Arc<[SkipEntry]>>,
    ) -> Result<Arc<[SkipEntry]>> {
        let mut list = vec![parent];
        let mut known = parent_list;
        loop {
            let last = list[list.len() - 1];
            let k = list.len() - 1;
            let anc = match known.take() {
                Some(l) => l,
                None => repo.skiplist(last.id).await?,
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
        let list = Skiplist::compute(repo, parent, None).await?;
        repo.skiplists.insert(id, list.clone());
        Ok(Skiplist::encode(&list))
    }
}

impl Repo {
    /// The skiplist of a stored changeset: from the cache of lists this
    /// process computed, or derived (read, or computed and stored).
    pub(crate) async fn skiplist(&self, id: ChangesetId) -> Result<Arc<[SkipEntry]>> {
        if let Some(l) = self.skiplists.get(&id) {
            return Ok(l);
        }
        Ok(Skiplist::decode(&derive::<Skiplist>(self, id).await?)?.into())
    }
}
