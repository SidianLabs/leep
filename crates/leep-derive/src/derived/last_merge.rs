use std::collections::HashMap;

use async_trait::async_trait;
use bytes::Bytes;
use leep_core::{Changeset, ChangesetId, DerivedRow, Hash, Result};

use super::{decode_u64, derive, DerivedKind};
use crate::Repo;

/// Generation of the nearest merge changeset on the first-parent chain,
/// including the changeset itself; 0 if the chain has none. Every ancestor
/// with a higher generation than this lies on the first-parent chain, which
/// is what lets `merge_base` answer from skiplists. Value: 8 big-endian bytes.
pub struct LastMerge;

#[async_trait]
impl DerivedKind for LastMerge {
    const NAME: &'static str = "last_merge-v1";

    /// Reads no value of its own kind but its parents', and is derived for
    /// long ranges at once (path history asks for it at every merge), so
    /// its rows are written a window at a time.
    const STAGED: bool = true;

    async fn derive(
        repo: &Repo,
        id: ChangesetId,
        cs: &Changeset,
        parents: &[Bytes],
    ) -> Result<Bytes> {
        let v = match cs.parents.len() {
            0 => 0,
            1 => decode_u64(&parents[0])?,
            _ => repo.generation(id).await?,
        };
        Ok(Bytes::copy_from_slice(&v.to_be_bytes()))
    }
}

impl LastMerge {
    /// Store the values of `window`, parents first, that are missing, from
    /// the changesets in hand: path history derives a window at a time and
    /// asks for these at each merge, and deriving them through [`derive`]
    /// from each merge read the changesets of every range again. Values are
    /// those [`derive`] computes, so a derivation racing this one writes the
    /// same rows.
    pub(crate) async fn derive_window(
        repo: &Repo,
        window: &[(ChangesetId, &Changeset)],
    ) -> Result<()> {
        let meta = repo.meta();
        let keys: Vec<Hash> = window.iter().map(|(id, _)| id.0).collect();
        let have = meta.get_derived_many(Self::NAME, &keys).await?;
        if have.len() != keys.len() || have.iter().all(Option::is_some) {
            return Ok(());
        }
        let mut values: HashMap<ChangesetId, u64> = HashMap::new();
        for ((id, _), v) in window.iter().zip(&have) {
            if let Some(v) = v {
                values.insert(*id, decode_u64(v)?);
            }
        }
        let outside: Vec<ChangesetId> = {
            let mut o: Vec<ChangesetId> = window
                .iter()
                .flat_map(|(_, cs)| cs.parents.first().copied())
                .filter(|p| !keys.contains(&p.0))
                .collect();
            o.sort();
            o.dedup();
            o
        };
        let found = meta
            .get_derived_many(Self::NAME, &outside.iter().map(|p| p.0).collect::<Vec<_>>())
            .await?;
        for (p, v) in outside.iter().zip(found) {
            let v = match v {
                Some(v) => v,
                None => derive::<LastMerge>(repo, *p).await?,
            };
            values.insert(*p, decode_u64(&v)?);
        }
        let mut rows = Vec::new();
        for (id, cs) in window {
            if values.contains_key(id) {
                continue;
            }
            let v = match cs.parents.as_slice() {
                [] => 0,
                [p] => values[p],
                _ => repo.generation(*id).await?,
            };
            values.insert(*id, v);
            rows.push(DerivedRow::new(
                Self::NAME,
                id.0,
                Bytes::copy_from_slice(&v.to_be_bytes()),
            ));
        }
        meta.put_derived_many(&rows).await
    }
}
