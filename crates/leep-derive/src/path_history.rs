//! Path log and last-change queries answered from the `history-tree-v3`
//! index instead of a scan of all ancestors (ADR 0013).

use std::collections::{BinaryHeap, HashSet};
use std::ops::Bound;
use std::sync::Arc;

use bytes::Bytes;
use leep_core::api::{ChangesetInfo, RenameEntry};
use leep_core::rename::RenameOptions;
use leep_core::{
    Changeset, ChangesetId, DerivedRow, Error, FileChange, Hash, MetaStore, RepoPath, Result,
    TreeEntry,
};

use crate::derived::{
    self, ChunkRef, HistoryEntry, HistoryNode, HistoryRow, HistoryTree, ResolvedRow,
    StoredHistoryNode, HISTORY_NODE_KIND as NODE_KIND,
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

/// Read and check one stored history row without any cache, as stored (a
/// patch is not applied), and its encoded length. The empty node always
/// exists.
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

/// `row`, stored at `hash`, with its patch applied to `key` (the row at its
/// base, as stored). A patch must point at a keyframe of its own shape, so a
/// damaged or foreign row is an error, not a wrong answer.
pub(crate) fn resolve_row(
    hash: Hash,
    row: StoredHistoryNode,
    key: Option<&StoredHistoryNode>,
) -> Result<ResolvedRow> {
    let keyframe = row.base().unwrap_or(hash);
    match row.resolve(key) {
        Ok(node) => Ok(ResolvedRow { node, keyframe }),
        Err(why) => Err(Error::Storage(format!(
            "corrupt history node {hash}: {why}"
        ))),
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

fn misfit(node: Hash, first: &str) -> Error {
    Error::Storage(format!(
        "corrupt history node {node}: the chunk at {first:?} does not fit"
    ))
}

/// Where a lookup goes in a split node: the chunk whose range holds the name,
/// that range's first name, and the next chunk's first name.
struct ChunkAt {
    first: String,
    node: Hash,
    next: Option<String>,
}

/// Whether the names `lo..=hi` lie in the range `first..next`.
fn within(lo: &str, hi: &str, first: &str, next: Option<&str>) -> bool {
    lo >= first && next.is_none_or(|n| hi < n)
}

impl Repo {
    /// Root history node of `id`, deriving the index for its ancestors first.
    pub(crate) async fn history_root(&self, id: ChangesetId) -> Result<Hash> {
        derived::decode_hash(&derived::derive::<HistoryTree>(self, id).await?)
    }

    /// Read one stored `history-node-v3` row as stored: a whole node or a
    /// chunk, a split node's chunk list, or a patch of either. The empty node
    /// always exists.
    pub(crate) async fn read_history_row(&self, hash: Hash) -> Result<Arc<StoredHistoryNode>> {
        if let Some(n) = self.history_rows.get(&hash) {
            return Ok(n);
        }
        let (node, len) = load_history_row(&**self.meta(), hash).await?;
        let node = Arc::new(node);
        self.history_rows.insert(hash, node.clone(), len);
        Ok(node)
    }

    /// The keyframe the patch `row` (at `hash`) is stored against, checked to
    /// be a keyframe of its shape; `None` if `row` is a keyframe.
    async fn history_key(
        &self,
        hash: Hash,
        row: &StoredHistoryNode,
    ) -> Result<Option<Arc<StoredHistoryNode>>> {
        let Some(base) = row.base() else {
            return Ok(None);
        };
        let key = self.read_history_row(base).await?;
        match (row, &*key) {
            (StoredHistoryNode::Patch { .. }, StoredHistoryNode::Flat(_))
            | (StoredHistoryNode::SplitPatch { .. }, StoredHistoryNode::Split(_)) => Ok(Some(key)),
            _ => Err(Error::Storage(format!(
                "corrupt history node {hash}: its base is not a keyframe of its shape"
            ))),
        }
    }

    /// A row as a keyframe, with any patch applied, and its keyframe.
    async fn read_resolved(&self, hash: Hash) -> Result<ResolvedRow> {
        let row = self.read_history_row(hash).await?;
        let key = self.history_key(hash, &row).await?;
        resolve_row(hash, (*row).clone(), key.as_deref())
    }

    /// Read one history node, joining its chunks if it is split.
    pub(crate) async fn read_history_node(&self, node: Hash) -> Result<HistoryNode> {
        let refs = match self.read_resolved(node).await?.node {
            StoredHistoryNode::Split(refs) => refs,
            StoredHistoryNode::Flat(n) => return Ok(n),
            _ => unreachable!("resolved rows are Flat or Split"),
        };
        let mut out = HistoryNode::default();
        for i in 0..refs.len() {
            let chunk = self.read_history_chunk(node, &refs, i).await?;
            out.entries.extend(chunk.entries);
        }
        Ok(out)
    }

    /// Chunk `i` of the split node `node`, resolved and checked to fit.
    async fn read_history_chunk(
        &self,
        node: Hash,
        refs: &[ChunkRef],
        i: usize,
    ) -> Result<HistoryNode> {
        let row = self.read_resolved(refs[i].node).await?;
        match chunk_fits(refs, i, &row.node) {
            Some(_) => match row.node {
                StoredHistoryNode::Flat(n) => Ok(n),
                _ => unreachable!("chunk_fits accepts only Flat"),
            },
            None => Err(bad_chunk(node, i)),
        }
    }

    /// The entry `name` of the node `node`, reading only the chunk that would
    /// hold it, and a patch's keyframe only when the patch does not settle the
    /// question: a recently changed name is in the patch.
    pub(crate) async fn history_child(
        &self,
        node: Hash,
        name: &str,
    ) -> Result<Option<HistoryEntry>> {
        let row = self.read_history_row(node).await?;
        let at = match &*row {
            StoredHistoryNode::Split(refs) => {
                let i = refs.partition_point(|r| r.first.as_str() <= name);
                if i == 0 {
                    return Ok(None);
                }
                ChunkAt {
                    first: refs[i - 1].first.clone(),
                    node: refs[i - 1].node,
                    next: refs.get(i).map(|r| r.first.clone()),
                }
            }
            StoredHistoryNode::SplitPatch { refs: changes, .. } => {
                match changes
                    .range::<str, _>((Bound::Unbounded, Bound::Included(name)))
                    .next_back()
                {
                    // In the range of a chunk the patch sets, which ends where
                    // the patch says.
                    Some((first, Some(c))) if c.next.as_deref().is_none_or(|n| name < n) => {
                        ChunkAt {
                            first: first.clone(),
                            node: c.node,
                            next: c.next.clone(),
                        }
                    }
                    _ => {
                        let key = self.history_key(node, &row).await?;
                        let StoredHistoryNode::Split(refs) =
                            resolve_row(node, (*row).clone(), key.as_deref())?.node
                        else {
                            unreachable!("a resolved list patch is Split")
                        };
                        let i = refs.partition_point(|r| r.first.as_str() <= name);
                        if i == 0 {
                            return Ok(None);
                        }
                        ChunkAt {
                            first: refs[i - 1].first.clone(),
                            node: refs[i - 1].node,
                            next: refs.get(i).map(|r| r.first.clone()),
                        }
                    }
                }
            }
            StoredHistoryNode::Flat(n) => return Ok(n.entries.get(name).cloned()),
            StoredHistoryNode::Patch { over, .. } => {
                if let Some(e) = over.entries.get(name) {
                    return Ok(Some(e.clone()));
                }
                let key = self.history_key(node, &row).await?;
                let Some(StoredHistoryNode::Flat(key)) = key.as_deref() else {
                    unreachable!("history_key checked the shape")
                };
                return Ok(key.entries.get(name).cloned());
            }
        };
        self.chunk_child(node, &at, name).await
    }

    /// The entry `name` of the chunk `at` of the split node `node`. A chunk
    /// must lie in its range, so a lookup finds what a full read would; a
    /// patch is checked against its own entries before answering from them,
    /// and against its keyframe's when that is read.
    async fn chunk_child(
        &self,
        node: Hash,
        at: &ChunkAt,
        name: &str,
    ) -> Result<Option<HistoryEntry>> {
        let next = at.next.as_deref();
        let row = self.read_history_row(at.node).await?;
        match &*row {
            StoredHistoryNode::Flat(n) => {
                match (n.entries.keys().next(), n.entries.keys().next_back()) {
                    (Some(lo), Some(hi)) if *lo == at.first && within(lo, hi, &at.first, next) => {
                        Ok(n.entries.get(name).cloned())
                    }
                    _ => Err(misfit(node, &at.first)),
                }
            }
            StoredHistoryNode::Patch { over, .. } => {
                match (over.entries.keys().next(), over.entries.keys().next_back()) {
                    (Some(lo), Some(hi)) if within(lo, hi, &at.first, next) => {}
                    _ => return Err(misfit(node, &at.first)),
                }
                if let Some(e) = over.entries.get(name) {
                    return Ok(Some(e.clone()));
                }
                let key = self
                    .history_key(at.node, &row)
                    .await?
                    .expect("a patch has a keyframe");
                self.patched_chunk_child(node, at, &row, &key, name)
            }
            _ => Err(misfit(node, &at.first)),
        }
    }

    /// [`Repo::chunk_child`] for a patched chunk whose patch lacks `name`.
    fn patched_chunk_child(
        &self,
        node: Hash,
        at: &ChunkAt,
        row: &StoredHistoryNode,
        key: &StoredHistoryNode,
        name: &str,
    ) -> Result<Option<HistoryEntry>> {
        let (StoredHistoryNode::Patch { over, .. }, StoredHistoryNode::Flat(key)) = (row, key)
        else {
            unreachable!("checked by the caller")
        };
        let first = over.first_name().min(key.first_name());
        let last = over
            .entries
            .keys()
            .next_back()
            .max(key.entries.keys().next_back())
            .map_or("", |s| s.as_str());
        if first != at.first || !within(first, last, &at.first, at.next.as_deref()) {
            return Err(misfit(node, &at.first));
        }
        Ok(key.entries.get(name).cloned())
    }

    /// Store `node` and return its hash. `base` is the stored node it was
    /// edited from: rows shared with it are not written again, so an edit to
    /// a split node writes one chunk and the chunk list, and each changed row
    /// is a patch on the keyframe of its predecessor in `base` while that is
    /// small (see `flat_row`). Skipping by `base`, not by the cache, stays
    /// right after `repair drop`: `base` was stored by the derivation that
    /// made it, as with untouched subdirectories, but a cached row may since
    /// have been deleted. The rows depend only on `node` and the stored
    /// `base`, so derivation stays deterministic; they are content addressed
    /// and idempotent, so racing derivations are harmless.
    pub(crate) async fn put_history_node(
        &self,
        node: &HistoryNode,
        base: Option<Hash>,
    ) -> Result<Hash> {
        let base_row = match base {
            Some(b) => Some(self.read_resolved(b).await?),
            None => None,
        };
        let mut known: HashSet<Hash> = HashSet::from([HistoryNode::empty_hash()]);
        known.extend(base);
        let mut writes: Vec<HistoryRow> = Vec::new();
        let top = match node.chunks() {
            None => {
                let key = match &base_row {
                    Some(b) if matches!(b.node, StoredHistoryNode::Flat(_)) => {
                        Some((b.keyframe, self.read_history_row(b.keyframe).await?))
                    }
                    _ => None,
                };
                let key = key.as_ref().and_then(|(h, k)| match &**k {
                    StoredHistoryNode::Flat(k) => Some((*h, k)),
                    _ => None,
                });
                derived::flat_row(node, key)
            }
            Some(chunks) => {
                let base_refs = match base_row.as_ref().map(|b| &b.node) {
                    Some(StoredHistoryNode::Split(refs)) => Some(refs.as_slice()),
                    _ => None,
                };
                let mut refs = Vec::with_capacity(chunks.len());
                for chunk in chunks {
                    let first = chunk.first_name().to_string();
                    // The predecessor is the base chunk whose range holds
                    // this chunk's first name.
                    let mut key = None;
                    if let (Some(brefs), Some(b)) = (base_refs, base) {
                        let i = brefs.partition_point(|r| r.first <= first);
                        if i > 0 {
                            let at = brefs[i - 1].node;
                            known.insert(at);
                            let prev = self.read_history_chunk(b, brefs, i - 1).await?;
                            if prev == chunk {
                                refs.push(ChunkRef { first, node: at });
                                continue;
                            }
                            let keyframe = self.read_history_row(at).await?.base().unwrap_or(at);
                            key = Some((keyframe, self.read_history_row(keyframe).await?));
                        }
                    }
                    let key = key.as_ref().and_then(|(h, k)| match &**k {
                        StoredHistoryNode::Flat(k) => Some((*h, k)),
                        _ => None,
                    });
                    let row = derived::flat_row(&chunk, key);
                    refs.push(ChunkRef {
                        first,
                        node: row.hash,
                    });
                    writes.push(row);
                }
                let key = match &base_row {
                    Some(b) if matches!(b.node, StoredHistoryNode::Split(_)) => {
                        Some((b.keyframe, self.read_history_row(b.keyframe).await?))
                    }
                    _ => None,
                };
                let key = key.as_ref().and_then(|(h, k)| match &**k {
                    StoredHistoryNode::Split(r) => Some((*h, r.as_slice())),
                    _ => None,
                });
                derived::split_row(refs, key)
            }
        };
        let hash = top.hash;
        writes.push(top);
        writes.retain(|row| known.insert(row.hash));
        if writes.is_empty() {
            return Ok(hash);
        }
        // Chunks come before the list that names them, so any prefix a failed
        // batch leaves behind holds no dangling reference.
        let rows: Vec<DerivedRow> = writes
            .iter()
            .map(|r| DerivedRow::new(NODE_KIND, r.hash, Bytes::copy_from_slice(&r.bytes)))
            .collect();
        self.meta().put_derived_many(&rows).await?;
        for row in writes {
            let len = row.bytes.len();
            self.history_rows.insert(row.hash, Arc::new(row.node), len);
        }
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
