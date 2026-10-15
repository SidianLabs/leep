//! Path log and last-change queries answered from the `history-tree-v1`
//! index instead of a scan of all ancestors (ADR 0013).

use std::collections::{BinaryHeap, HashSet};

use bytes::Bytes;
use leep_core::api::ChangesetInfo;
use leep_core::{Changeset, ChangesetId, Error, Hash, Result};

use crate::derived::{
    self, HistoryEntry, HistoryNode, HistoryTree, HISTORY_NODE_KIND as NODE_KIND,
};
use crate::history::LogFilter;
use crate::Repo;

/// Newest-first walk over the changesets that changed one path. Each yielded
/// changeset's parents are looked up in the index to find what to yield next,
/// so only changesets that changed the path are ever read.
struct PathWalk {
    path: String,
    seen: HashSet<ChangesetId>,
    heap: BinaryHeap<(u64, ChangesetId)>,
}

impl PathWalk {
    async fn start(repo: &Repo, tip: ChangesetId, path: &str) -> Result<Self> {
        let mut w = PathWalk {
            path: path.to_string(),
            seen: HashSet::new(),
            heap: BinaryHeap::new(),
        };
        w.push_frontier(repo, tip).await?;
        Ok(w)
    }

    async fn push_frontier(&mut self, repo: &Repo, at: ChangesetId) -> Result<()> {
        let Some(entry) = repo.history_entry(at, &self.path).await? else {
            return Ok(());
        };
        for &c in entry.changed() {
            if self.seen.insert(c) {
                self.heap.push((repo.generation(c).await?, c));
            }
        }
        Ok(())
    }

    async fn next(&mut self, repo: &Repo) -> Result<Option<(u64, ChangesetId, Changeset)>> {
        let Some((generation, cid)) = self.heap.pop() else {
            return Ok(None);
        };
        let cs = repo.get_changeset(cid).await?;
        for &p in &cs.parents {
            self.push_frontier(repo, p).await?;
        }
        Ok(Some((generation, cid, cs)))
    }
}

/// Split into components, ignoring empty ones the way tree lookups do.
fn components(path: &str) -> impl Iterator<Item = &str> {
    path.split('/').filter(|c| !c.is_empty())
}

impl Repo {
    /// Root history node of `id`, deriving the index for its ancestors first.
    pub(crate) async fn history_root(&self, id: ChangesetId) -> Result<Hash> {
        derived::decode_hash(&derived::derive::<HistoryTree>(self, id).await?)
    }

    /// Read one history node. The empty node always exists.
    pub(crate) async fn read_history_node(&self, node: Hash) -> Result<HistoryNode> {
        if node == HistoryNode::empty_hash() {
            return Ok(HistoryNode::default());
        }
        match self.meta().get_derived(NODE_KIND, &node).await? {
            Some(b) => HistoryNode::decode(&b),
            None => Err(Error::Storage(format!("history node {node} missing"))),
        }
    }

    /// Content-addressed and idempotent, so racing derivations are harmless.
    pub(crate) async fn put_history_node(&self, node: &HistoryNode) -> Result<Hash> {
        let hash = node.hash();
        self.meta()
            .put_derived(NODE_KIND, &hash, Bytes::from(node.encode()))
            .await?;
        Ok(hash)
    }

    /// Index entry for `path` at `id`: `None` if the path never existed in
    /// the history of `id`.
    pub(crate) async fn history_entry(
        &self,
        id: ChangesetId,
        path: &str,
    ) -> Result<Option<HistoryEntry>> {
        let mut node = self.history_root(id).await?;
        let mut comps = components(path).peekable();
        while let Some(c) = comps.next() {
            let Some(entry) = self.read_history_node(node).await?.entries.remove(c) else {
                return Ok(None);
            };
            if comps.peek().is_none() {
                return Ok(Some(entry));
            }
            match entry.node() {
                Some(child) => node = child,
                None => return Ok(None),
            }
        }
        Ok(None)
    }

    /// Path log through the index: the same changesets, in the same order, as
    /// scanning every ancestor (see `log_scan`), but cost follows the number
    /// of changes to `path`, not the length of history. `filter` is applied
    /// to those changesets only.
    pub(crate) async fn log_indexed(
        &self,
        id: ChangesetId,
        path: &str,
        limit: usize,
        filter: &LogFilter,
    ) -> Result<Vec<ChangesetInfo>> {
        let mut out = Vec::new();
        let mut walk = PathWalk::start(self, id, path).await?;
        while out.len() < limit {
            let Some((generation, cid, cs)) = walk.next(self).await? else {
                break;
            };
            if !filter.matches(&cs) {
                continue;
            }
            out.push(ChangesetInfo {
                id: cid,
                changeset: cs,
                generation,
                root_tree: self.root_tree(cid).await?,
            });
        }
        Ok(out)
    }

    /// The newest changeset reachable from `id` that changed the file or
    /// directory `path`, plus any other newest changes that are concurrent
    /// with it (see `LastChangeResponse`). A deleted path reports the
    /// changeset that deleted it. `NotFound` if the path never existed.
    pub async fn last_change(
        &self,
        id: ChangesetId,
        path: &str,
    ) -> Result<(ChangesetInfo, Vec<ChangesetId>)> {
        let path = path.trim_matches('/');
        let missing = || Error::NotFound(format!("path {path:?} at {id}"));
        let entry = self.history_entry(id, path).await?.ok_or_else(missing)?;
        let mut newest = Vec::new();
        for &c in entry.changed() {
            newest.push((self.generation(c).await?, c));
        }
        newest.sort_by(|a, b| b.cmp(a));
        let mut ids = newest.into_iter().map(|(_, c)| c);
        let first = ids.next().ok_or_else(missing)?;
        Ok((self.info(first).await?, ids.collect()))
    }
}
