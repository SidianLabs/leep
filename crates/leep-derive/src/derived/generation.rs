use async_trait::async_trait;
use bytes::Bytes;
use leep_core::{Changeset, ChangesetId, Result};

use super::{decode_u64, DerivedKind};
use crate::Repo;

/// `1 + max(parent generations)`; roots are 1. Value: 8 big-endian bytes.
pub struct Generation;

#[async_trait]
impl DerivedKind for Generation {
    const NAME: &'static str = "generation";

    async fn derive(_: &Repo, _: ChangesetId, _: &Changeset, parents: &[Bytes]) -> Result<Bytes> {
        let mut max = 0;
        for p in parents {
            max = max.max(decode_u64(p)?);
        }
        Ok(Bytes::copy_from_slice(&(max + 1).to_be_bytes()))
    }
}
