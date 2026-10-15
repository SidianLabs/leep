//! Log with optional path filtering.

use std::collections::{BinaryHeap, HashSet};

use leep_core::api::ChangesetInfo;
use leep_core::{ChangesetId, Result, TreeNode};

use crate::Repo;

impl Repo {
    /// Ancestors of `id` (inclusive, all parents), newest first by generation
    /// then id. With `path`, only changesets that changed that file or
    /// directory relative to their first parent.
    pub async fn log(
        &self,
        id: ChangesetId,
        path: Option<&str>,
        limit: usize,
    ) -> Result<Vec<ChangesetInfo>> {
        let path = path.map(|p| p.trim_matches('/')).filter(|p| !p.is_empty());
        let mut out = Vec::new();
        let mut seen = HashSet::from([id]);
        let mut heap = BinaryHeap::from([(self.generation(id).await?, id)]);
        while out.len() < limit {
            let Some((generation, cid)) = heap.pop() else {
                break;
            };
            let cs = self.get_changeset(cid).await?;
            for &p in &cs.parents {
                if seen.insert(p) {
                    heap.push((self.generation(p).await?, p));
                }
            }
            let root_tree = self.root_tree(cid).await?;
            if let Some(path) = path {
                let parent_tree = match cs.parents.first() {
                    Some(p) => self.root_tree(*p).await?,
                    None => TreeNode::empty_hash(),
                };
                if root_tree == parent_tree
                    || self.entry_at(root_tree, path).await?
                        == self.entry_at(parent_tree, path).await?
                {
                    continue;
                }
            }
            out.push(ChangesetInfo {
                id: cid,
                changeset: cs,
                generation,
                root_tree,
            });
        }
        Ok(out)
    }
}
