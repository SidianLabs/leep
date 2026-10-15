use async_trait::async_trait;
use bytes::Bytes;
use leep_core::{Changeset, ChangesetId, Result};

use super::{decode_u64, DerivedKind};
use crate::Repo;

/// Generation of the nearest merge changeset on the first-parent chain,
/// including the changeset itself; 0 if the chain has none. Every ancestor
/// with a higher generation than this lies on the first-parent chain, which
/// is what lets `merge_base` answer from skiplists. Value: 8 big-endian bytes.
pub struct LastMerge;

#[async_trait]
impl DerivedKind for LastMerge {
    const NAME: &'static str = "last_merge-v1";

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
