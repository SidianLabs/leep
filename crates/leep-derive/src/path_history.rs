//! Path log and last-change queries answered from the `history-tree-v1`
//! index instead of a scan of all ancestors (ADR 0013).

use std::collections::{BinaryHeap, HashSet};
use std::sync::Arc;

use bytes::Bytes;
use leep_core::api::{ChangesetInfo, RenameEntry};
use leep_core::rename::RenameOptions;
use leep_core::{Changeset, ChangesetId, Error, FileChange, Hash, RepoPath, Result, TreeEntry};

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

/// Like [`PathWalk`], but each pending changeset carries the name its file had
/// there, so a rename can switch one line of history to the old name while
/// concurrent branches keep following the new one. Items are keyed by
/// `(changeset, name)`; every item of a changeset is queued before it pops,
/// because queuing happens only from its descendants.
struct FollowWalk {
    seen: HashSet<(ChangesetId, Arc<str>)>,
    heap: BinaryHeap<(u64, ChangesetId, Arc<str>)>,
}

impl FollowWalk {
    async fn push_frontier(&mut self, repo: &Repo, at: ChangesetId, path: &Arc<str>) -> Result<()> {
        let Some(entry) = repo.history_entry(at, path).await? else {
            return Ok(());
        };
        for &c in entry.changed() {
            if self.seen.insert((c, path.clone())) {
                self.heap.push((repo.generation(c).await?, c, path.clone()));
            }
        }
        Ok(())
    }

    /// Pop every name the newest changeset is queued under.
    fn pop_group(&mut self) -> Option<(u64, ChangesetId, Vec<Arc<str>>)> {
        let (generation, cid, first) = self.heap.pop()?;
        let mut names = vec![first];
        while self
            .heap
            .peek()
            .is_some_and(|(g, c, _)| (*g, *c) == (generation, cid))
        {
            names.push(self.heap.pop().expect("peeked").2);
        }
        names.sort();
        Some((generation, cid, names))
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
            Some(b) if Hash::of_object(NODE_KIND, &b) != node => {
                Err(Error::Storage(format!("corrupt history node {node}")))
            }
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
                path_at: None,
                renamed: None,
            });
        }
        Ok(out)
    }

    /// The rename that gave `path` its name in changeset `cid`, if `cid`
    /// created `path` (it is a new file relative to the first parent) and the
    /// diff against the first parent pairs it with a deleted file.
    async fn rename_into(
        &self,
        cid: ChangesetId,
        cs: &Changeset,
        path: &str,
        renames: &mut Option<Vec<RenameEntry>>,
    ) -> Result<Option<RenameEntry>> {
        let (Some(&parent), Ok(rp)) = (cs.parents.first(), RepoPath::new(path)) else {
            return Ok(None);
        };
        if !matches!(cs.changes.get(&rp), Some(FileChange::Modify { .. })) {
            return Ok(None);
        }
        let parent_tree = self.root_tree(parent).await?;
        if matches!(
            self.entry_at(parent_tree, path).await?,
            Some(TreeEntry::File { .. })
        ) {
            return Ok(None);
        }
        if renames.is_none() {
            let entries = self.diff(Some(parent), cid, "").await?;
            let found = self
                .detect_renames(&entries, &RenameOptions::default())
                .await?;
            *renames = Some(found.renames);
        }
        Ok(renames
            .as_ref()
            .and_then(|r| r.iter().find(|r| r.to == rp))
            .cloned())
    }

    /// Log of the file `path` that continues under the old name across
    /// renames (`log --follow`, ADR 0016). Same order and filters as
    /// [`Repo::log_filtered`] with a path; each entry carries the name the
    /// file had in it, and the entry that renamed it says from what.
    ///
    /// Renames are detected when the walk reaches the changeset that created
    /// the file, by diffing it against its first parent with the default
    /// threshold. Then the walk continues with the old name from that
    /// changeset's parents, and keeps the new name only on merged-in parents,
    /// which may hold the file's earlier history under that name.
    /// `Invalid` if `path` is a directory at `id`.
    pub async fn log_follow(
        &self,
        id: ChangesetId,
        path: &str,
        limit: usize,
        filter: &LogFilter,
    ) -> Result<Vec<ChangesetInfo>> {
        let path = path.trim_matches('/');
        RepoPath::new(path)?;
        let tip_tree = self.root_tree(id).await?;
        if matches!(
            self.entry_at(tip_tree, path).await?,
            Some(TreeEntry::Dir { .. })
        ) {
            return Err(Error::Invalid(format!(
                "cannot follow {path:?}: it is a directory, and renames are tracked per file"
            )));
        }
        let start: Arc<str> = path.into();
        let mut walk = FollowWalk {
            seen: HashSet::new(),
            heap: BinaryHeap::new(),
        };
        walk.push_frontier(self, id, &start).await?;
        let mut out = Vec::new();
        while out.len() < limit {
            let Some((generation, cid, names)) = walk.pop_group() else {
                break;
            };
            let cs = self.get_changeset(cid).await?;
            let mut renames = None;
            let mut shown: Option<(Arc<str>, Option<RenameEntry>)> = None;
            for name in names {
                let renamed = self.rename_into(cid, &cs, &name, &mut renames).await?;
                for (n, &p) in cs.parents.iter().enumerate() {
                    match &renamed {
                        Some(r) => {
                            walk.push_frontier(self, p, &Arc::from(r.from.as_str()))
                                .await?;
                            if n > 0 {
                                walk.push_frontier(self, p, &name).await?;
                            }
                        }
                        None => walk.push_frontier(self, p, &name).await?,
                    }
                }
                // Prefer the name that was created by a rename here.
                let better = match &shown {
                    None => true,
                    Some((_, current)) => current.is_none() && renamed.is_some(),
                };
                if better {
                    shown = Some((name, renamed));
                }
            }
            if !filter.matches(&cs) {
                continue;
            }
            let (name, renamed) = shown.expect("a group has at least one name");
            out.push(ChangesetInfo {
                id: cid,
                changeset: cs,
                generation,
                root_tree: self.root_tree(cid).await?,
                path_at: Some(RepoPath::new(&*name)?),
                renamed,
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
