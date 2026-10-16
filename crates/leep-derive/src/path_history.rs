//! Path log and last-change queries answered from the `history-tree-v2`
//! index instead of a scan of all ancestors (ADR 0013).

use std::collections::{BinaryHeap, HashSet};
use std::sync::Arc;

use bytes::Bytes;
use leep_core::api::{ChangesetInfo, RenameEntry};
use leep_core::rename::RenameOptions;
use leep_core::{
    Changeset, ChangesetId, Error, FileChange, Hash, MetaStore, RepoPath, Result, TreeEntry,
};

use crate::derived::{
    self, ChunkRef, HistoryEntry, HistoryNode, HistoryTree, StoredHistoryNode,
    HISTORY_NODE_KIND as NODE_KIND,
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

/// Read and check one stored history row without any cache, and its encoded
/// length. The empty node always exists.
pub(crate) async fn load_history_row(
    meta: &dyn MetaStore,
    hash: Hash,
) -> Result<(StoredHistoryNode, usize)> {
    if hash == HistoryNode::empty_hash() {
        return Ok((StoredHistoryNode::Flat(HistoryNode::default()), 2));
    }
    match meta.get_derived(NODE_KIND, &hash).await? {
        Some(b) if Hash::of_object(NODE_KIND, &b) != hash => {
            Err(Error::Storage(format!("corrupt history node {hash}")))
        }
        Some(b) => Ok((StoredHistoryNode::decode(&b)?, b.len())),
        None => Err(Error::Storage(format!("history node {hash} missing"))),
    }
}

/// The entries of `row` if it can be chunk `i` of a node split as `refs`: a
/// whole node starting with the chunk's first name and ending before the
/// next chunk's, so a lookup by name finds what a full read would.
pub(crate) fn chunk_fits<'a>(
    refs: &[ChunkRef],
    i: usize,
    row: &'a StoredHistoryNode,
) -> Option<&'a HistoryNode> {
    let StoredHistoryNode::Flat(chunk) = row else {
        return None;
    };
    let first = chunk.entries.keys().next()?;
    let last = chunk.entries.keys().next_back()?;
    let below_next = refs.get(i + 1).is_none_or(|n| *last < n.first);
    (*first == refs[i].first && below_next).then_some(chunk)
}

pub(crate) fn bad_chunk(node: Hash, i: usize) -> Error {
    Error::Storage(format!(
        "corrupt history node {node}: chunk {i} does not fit"
    ))
}

impl Repo {
    /// Root history node of `id`, deriving the index for its ancestors first.
    pub(crate) async fn history_root(&self, id: ChangesetId) -> Result<Hash> {
        derived::decode_hash(&derived::derive::<HistoryTree>(self, id).await?)
    }

    /// Read one stored `history-node-v2` row: a whole node or a chunk, or the
    /// chunk list of a split node. The empty node always exists.
    pub(crate) async fn read_history_row(&self, hash: Hash) -> Result<Arc<StoredHistoryNode>> {
        if let Some(n) = self.history_rows.get(&hash) {
            return Ok(n);
        }
        let (node, len) = load_history_row(&**self.meta(), hash).await?;
        let node = Arc::new(node);
        self.history_rows.insert(hash, node.clone(), len);
        Ok(node)
    }

    /// Read one history node, joining its chunks if it is split.
    pub(crate) async fn read_history_node(&self, node: Hash) -> Result<HistoryNode> {
        let refs = match &*self.read_history_row(node).await? {
            StoredHistoryNode::Flat(n) => return Ok(n.clone()),
            StoredHistoryNode::Split(refs) => refs.clone(),
        };
        let mut out = HistoryNode::default();
        for i in 0..refs.len() {
            let row = self.read_history_chunk(node, &refs, i).await?;
            let chunk = chunk_fits(&refs, i, &row).expect("checked");
            out.entries
                .extend(chunk.entries.iter().map(|(k, v)| (k.clone(), v.clone())));
        }
        Ok(out)
    }

    /// Chunk `i` of the split node `node`.
    async fn read_history_chunk(
        &self,
        node: Hash,
        refs: &[ChunkRef],
        i: usize,
    ) -> Result<Arc<StoredHistoryNode>> {
        let row = self.read_history_row(refs[i].node).await?;
        if chunk_fits(refs, i, &row).is_none() {
            return Err(bad_chunk(node, i));
        }
        Ok(row)
    }

    /// The entry `name` of the node `node`, reading only the chunk that would
    /// hold it.
    pub(crate) async fn history_child(
        &self,
        node: Hash,
        name: &str,
    ) -> Result<Option<HistoryEntry>> {
        let row = self.read_history_row(node).await?;
        let refs = match &*row {
            StoredHistoryNode::Flat(n) => return Ok(n.entries.get(name).cloned()),
            StoredHistoryNode::Split(refs) => refs,
        };
        let i = refs.partition_point(|r| r.first.as_str() <= name);
        if i == 0 {
            return Ok(None);
        }
        let row = self.read_history_chunk(node, refs, i - 1).await?;
        let chunk = chunk_fits(refs, i - 1, &row).expect("checked");
        Ok(chunk.entries.get(name).cloned())
    }

    /// Store `node` and return its hash. `base` is the stored node it was
    /// edited from; rows shared with it are not written again, so an edit to
    /// a split node writes one chunk and the chunk list. Skipping by `base`,
    /// not by the cache, stays right after `repair drop`: `base` was stored by
    /// the derivation that made it, as with untouched subdirectories, but a
    /// cached row may since have been deleted. Content addressed and
    /// idempotent, so racing derivations are harmless.
    pub(crate) async fn put_history_node(
        &self,
        node: &HistoryNode,
        base: Option<Hash>,
    ) -> Result<Hash> {
        let mut stored: HashSet<Hash> = HashSet::from([HistoryNode::empty_hash()]);
        if let Some(base) = base {
            stored.insert(base);
            if let StoredHistoryNode::Split(refs) = &*self.read_history_row(base).await? {
                stored.extend(refs.iter().map(|r| r.node));
            }
        }
        let mut top = HistoryNode::empty_hash();
        for row in node.rows() {
            top = row.hash;
            if stored.contains(&row.hash) {
                continue;
            }
            let len = row.bytes.len();
            self.meta()
                .put_derived(NODE_KIND, &row.hash, Bytes::from(row.bytes))
                .await?;
            self.history_rows.insert(row.hash, Arc::new(row.node), len);
        }
        Ok(top)
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
            let Some(entry) = self.history_child(node, c).await? else {
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
