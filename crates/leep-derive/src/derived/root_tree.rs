use async_trait::async_trait;
use bytes::Bytes;
use leep_core::{Changeset, ChangesetId, Hash, Result, TreeNode};

use super::{decode_hash, DerivedKind};
use crate::tree_ops::apply_changes;
use crate::Repo;

/// Root tree hash: `changes` applied to `parents[0]`'s tree. Value: 32 bytes.
pub struct RootTree;

impl RootTree {
    pub(crate) const NAME_STR: &'static str = "root_tree";
}

#[async_trait]
impl DerivedKind for RootTree {
    const NAME: &'static str = Self::NAME_STR;

    async fn derive(
        repo: &Repo,
        _: ChangesetId,
        cs: &Changeset,
        parents: &[Bytes],
    ) -> Result<Bytes> {
        let base: Hash = match parents.first() {
            Some(p) => decode_hash(p)?,
            None => TreeNode::empty_hash(),
        };
        let tree = apply_changes(repo, base, &cs.changes).await?;
        Ok(Bytes::copy_from_slice(tree.as_bytes()))
    }
}
