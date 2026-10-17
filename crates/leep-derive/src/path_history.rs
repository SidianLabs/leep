//! Path log and last-change queries answered from the `history-tree-v3`
//! index instead of a scan of all ancestors (ADR 0013).

use std::collections::{BTreeMap, BinaryHeap, HashSet};
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

/// A history node prepared for lookups (see [`Repo::node_lookup`]).
pub(crate) enum NodeLookup {
    /// A whole or patched node, looked up in its row.
    Row(Hash),
    /// A split node and its resolved chunk list.
    Split { node: Hash, refs: Vec<ChunkRef> },
}

/// A chunk's row and, for a patch, its keyframe.
type ChunkRows = (Arc<StoredHistoryNode>, Option<Arc<StoredHistoryNode>>);

/// History rows staged for one write, each once.
#[derive(Default)]
pub(crate) struct PendingRows {
    rows: Vec<HistoryRow>,
    seen: HashSet<Hash>,
}

/// Most history rows one write carries.
const WRITE_BATCH: usize = 1024;

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

/// The entries of chunk `i` of the split node `node`, stored as `row` (a
/// patch on `key`, if one), in name order, without copying them: a patch's
/// entries over its keyframe's. A chunk that does not fit its range is an
/// error, as for [`chunk_fits`] on the joined chunk.
fn chunk_view<'r>(
    node: Hash,
    refs: &[ChunkRef],
    i: usize,
    row: &'r StoredHistoryNode,
    key: Option<&'r StoredHistoryNode>,
) -> Result<Vec<(&'r String, &'r HistoryEntry)>> {
    let view: Vec<(&String, &HistoryEntry)> = match (row, key) {
        (StoredHistoryNode::Flat(n), None) => n.entries.iter().collect(),
        (StoredHistoryNode::Patch { over, .. }, Some(StoredHistoryNode::Flat(key))) => {
            let mut out = Vec::with_capacity(key.entries.len() + over.entries.len());
            let (mut k, mut o) = (
                key.entries.iter().peekable(),
                over.entries.iter().peekable(),
            );
            loop {
                match (k.peek(), o.peek()) {
                    (Some(a), Some(b)) if a.0 < b.0 => out.extend(k.next()),
                    (Some(a), Some(b)) if a.0 == b.0 => {
                        k.next();
                        out.extend(o.next());
                    }
                    (_, Some(_)) => out.extend(o.next()),
                    (Some(_), None) => out.extend(k.next()),
                    (None, None) => break,
                }
            }
            out
        }
        _ => return Err(bad_chunk(node, i)),
    };
    match (view.first(), view.last()) {
        (Some(lo), Some(hi))
            if *lo.0 == refs[i].first && refs.get(i + 1).is_none_or(|n| *hi.0 < n.first) =>
        {
            Ok(view)
        }
        _ => Err(bad_chunk(node, i)),
    }
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
        if let Some(n) = self.history_staged.lock().unwrap().get(&hash) {
            return Ok(n.clone());
        }
        let (node, len) = load_history_row(&**self.meta(), hash).await?;
        let node = Arc::new(node);
        self.history_rows.insert(hash, node.clone(), len);
        Ok(node)
    }

    /// Read the rows `hashes` that are not cached, and then the keyframes
    /// they are patches on, in one batched read each, so joining a split
    /// node costs two round trips rather than one or two per chunk. Only
    /// fills the cache: whatever this misses or cannot check is read again,
    /// singly, by the caller, with its errors.
    async fn prefetch_history_rows(&self, hashes: impl Iterator<Item = Hash>) {
        let mut want: Vec<Hash> = hashes.collect();
        for _ in 0..2 {
            let empty = HistoryNode::empty_hash();
            want.retain(|h| *h != empty && self.history_rows.get(h).is_none());
            {
                let staged = self.history_staged.lock().unwrap();
                want.retain(|h| !staged.contains_key(h));
            }
            want.sort();
            want.dedup();
            if want.len() < 2 {
                return;
            }
            let Ok(found) = self.meta().get_derived_many(NODE_KIND, &want).await else {
                return;
            };
            let mut bases = Vec::new();
            for (hash, bytes) in want.iter().zip(found) {
                let Some(b) = bytes else { continue };
                if Hash::of_object(NODE_KIND, &b) != *hash {
                    continue;
                }
                let Ok(row) = StoredHistoryNode::decode(&b) else {
                    continue;
                };
                bases.extend(row.base());
                self.history_rows.insert(*hash, Arc::new(row), b.len());
            }
            want = bases;
        }
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
        let rows = self.chunk_rows(&refs).await?;
        let mut entries = Vec::new();
        for (i, (row, key)) in rows.iter().enumerate() {
            entries.extend(chunk_view(node, &refs, i, row, key.as_deref())?);
        }
        // Names ascend across chunks, so this builds the map in one pass.
        Ok(HistoryNode {
            entries: entries
                .into_iter()
                .map(|(n, e)| (n.clone(), e.clone()))
                .collect(),
        })
    }

    /// The rows of the chunks `refs`, each with its keyframe if a patch.
    async fn chunk_rows(&self, refs: &[ChunkRef]) -> Result<Vec<ChunkRows>> {
        self.prefetch_history_rows(refs.iter().map(|r| r.node))
            .await;
        let mut rows = Vec::with_capacity(refs.len());
        for r in refs {
            let row = self.read_history_row(r.node).await?;
            let key = self.history_key(r.node, &row).await?;
            rows.push((row, key));
        }
        Ok(rows)
    }

    /// The names whose entries differ between the history nodes `a` and `b`,
    /// with both entries. Chunks the two share are skipped unread: a chunk
    /// row holds the same names and entries in both, and no other chunk of
    /// either holds those names.
    pub(crate) async fn diff_history_nodes(
        &self,
        a: Hash,
        b: Hash,
    ) -> Result<Vec<(String, Option<HistoryEntry>, Option<HistoryEntry>)>> {
        let (ra, rb) = (self.read_resolved(a).await?, self.read_resolved(b).await?);
        let shared: HashSet<Hash> = match (&ra.node, &rb.node) {
            (StoredHistoryNode::Split(x), StoredHistoryNode::Split(y)) => {
                let x: HashSet<Hash> = x.iter().map(|r| r.node).collect();
                y.iter().map(|r| r.node).filter(|h| x.contains(h)).collect()
            }
            _ => HashSet::new(),
        };
        let mut sides: Vec<BTreeMap<String, HistoryEntry>> = Vec::with_capacity(2);
        for (node, row) in [(a, ra), (b, rb)] {
            sides.push(match row.node {
                StoredHistoryNode::Flat(n) => n.entries,
                StoredHistoryNode::Split(refs) => {
                    let mut out = BTreeMap::new();
                    let keep: Vec<usize> = (0..refs.len())
                        .filter(|&i| !shared.contains(&refs[i].node))
                        .collect();
                    let wanted: Vec<ChunkRef> = keep.iter().map(|&i| refs[i].clone()).collect();
                    let rows = self.chunk_rows(&wanted).await?;
                    for (&i, (row, key)) in keep.iter().zip(&rows) {
                        for (n, e) in chunk_view(node, &refs, i, row, key.as_deref())? {
                            out.insert(n.clone(), e.clone());
                        }
                    }
                    out
                }
                _ => unreachable!("resolved rows are Flat or Split"),
            });
        }
        let nb = sides.pop().expect("two sides");
        let mut na = sides.pop().expect("two sides");
        let mut out = Vec::new();
        for (name, eb) in nb {
            let ea = na.remove(&name);
            if ea.as_ref() != Some(&eb) {
                out.push((name, ea, Some(eb)));
            }
        }
        out.extend(na.into_iter().map(|(name, ea)| (name, Some(ea), None)));
        out.sort_by(|x, y| x.0.cmp(&y.0));
        Ok(out)
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

    /// `node` ready for many [`Repo::lookup_child`] calls: a split node's
    /// chunk list is resolved once rather than at each lookup a patch of it
    /// does not settle.
    pub(crate) async fn node_lookup(&self, node: Hash) -> Result<NodeLookup> {
        let row = self.read_history_row(node).await?;
        Ok(match &*row {
            StoredHistoryNode::Flat(_) | StoredHistoryNode::Patch { .. } => NodeLookup::Row(node),
            StoredHistoryNode::Split(refs) => NodeLookup::Split {
                node,
                refs: refs.clone(),
            },
            StoredHistoryNode::SplitPatch { .. } => match self.read_resolved(node).await?.node {
                StoredHistoryNode::Split(refs) => NodeLookup::Split { node, refs },
                _ => unreachable!("a resolved list patch is Split"),
            },
        })
    }

    /// The entry `name` of a node prepared by [`Repo::node_lookup`], as
    /// [`Repo::history_child`] finds it.
    pub(crate) async fn lookup_child(
        &self,
        lookup: &NodeLookup,
        name: &str,
    ) -> Result<Option<HistoryEntry>> {
        match lookup {
            NodeLookup::Row(node) => self.history_child(*node, name).await,
            NodeLookup::Split { node, refs } => {
                let i = refs.partition_point(|r| r.first.as_str() <= name);
                if i == 0 {
                    return Ok(None);
                }
                let at = ChunkAt {
                    first: refs[i - 1].first.clone(),
                    node: refs[i - 1].node,
                    next: refs.get(i).map(|r| r.first.clone()),
                };
                self.chunk_child(*node, &at, name).await
            }
        }
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
    #[cfg(test)]
    pub(crate) async fn put_history_node(
        &self,
        node: &HistoryNode,
        base: Option<Hash>,
    ) -> Result<Hash> {
        let mut pending = PendingRows::default();
        let hash = self.stage_history_node(node, base, &mut pending).await?;
        self.write_history_rows(pending).await?;
        Ok(hash)
    }

    /// [`Repo::put_history_node`] without the write: the rows go to
    /// `pending`, so a derivation writes all the nodes of a changeset in one
    /// round trip instead of one per directory.
    pub(crate) async fn stage_history_node(
        &self,
        node: &HistoryNode,
        base: Option<Hash>,
        pending: &mut PendingRows,
    ) -> Result<Hash> {
        let base_row = match base {
            Some(b) => Some(self.read_resolved(b).await?),
            None => None,
        };
        let mut known: HashSet<Hash> = HashSet::from([HistoryNode::empty_hash()]);
        known.extend(base);
        let mut writes: Vec<HistoryRow> = Vec::new();
        let entries: Vec<(&String, &HistoryEntry)> = node.entries.iter().collect();
        let top = match derived::chunk_ranges(entries.iter().map(|(n, _)| n.as_str())) {
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
            Some(ranges) => {
                let base_refs = match base_row.as_ref().map(|b| &b.node) {
                    Some(StoredHistoryNode::Split(refs)) => Some(refs.as_slice()),
                    _ => None,
                };
                let mut refs = Vec::with_capacity(ranges.len());
                for range in ranges {
                    let want = &entries[range];
                    let first = want[0].0.clone();
                    // The predecessor is the base chunk whose range holds
                    // this chunk's first name. Only chunks that differ from
                    // it are copied out of the node.
                    let mut key = None;
                    if let (Some(brefs), Some(b)) = (base_refs, base) {
                        let i = brefs.partition_point(|r| r.first <= first);
                        if i > 0 {
                            let at = brefs[i - 1].node;
                            known.insert(at);
                            let row = self.read_history_row(at).await?;
                            let row_key = self.history_key(at, &row).await?;
                            if chunk_view(b, brefs, i - 1, &row, row_key.as_deref())? == want {
                                refs.push(ChunkRef { first, node: at });
                                continue;
                            }
                            key = Some(match row_key {
                                Some(k) => (row.base().expect("a patch has a base"), k),
                                None => (at, row),
                            });
                        }
                    }
                    let key = key.as_ref().and_then(|(h, k)| match &**k {
                        StoredHistoryNode::Flat(k) => Some((*h, k)),
                        _ => None,
                    });
                    let chunk = HistoryNode {
                        entries: want
                            .iter()
                            .map(|(n, e)| ((*n).clone(), (*e).clone()))
                            .collect(),
                    };
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
        for row in writes {
            if known.insert(row.hash) && pending.seen.insert(row.hash) {
                pending.rows.push(row);
            }
        }
        Ok(hash)
    }

    /// Hand rows staged by [`Repo::stage_history_node`] to a caller that
    /// writes them later, in order, as `rows`. Until
    /// [`Repo::unstage_history_rows`], reads find them here, since the cache
    /// may drop them before they are stored.
    pub(crate) fn stage_history_rows(&self, pending: PendingRows, rows: &mut Vec<DerivedRow>) {
        let mut staged = self.history_staged.lock().unwrap();
        for row in pending.rows {
            let node = Arc::new(row.node);
            self.history_rows
                .insert(row.hash, node.clone(), row.bytes.len());
            staged.insert(row.hash, node);
            rows.push(DerivedRow::new(NODE_KIND, row.hash, Bytes::from(row.bytes)));
        }
    }

    /// Forget staged rows among `rows`, once stored or dropped.
    pub(crate) fn unstage_history_rows(&self, rows: &[DerivedRow]) {
        let mut staged = self.history_staged.lock().unwrap();
        for row in rows.iter().filter(|r| r.kind == NODE_KIND) {
            staged.remove(&row.key);
        }
    }

    /// Store rows staged by [`Repo::stage_history_node`], in staging order.
    /// Chunks come before the list that names them and children before
    /// their parents, so any prefix a failed batch leaves behind holds no
    /// dangling reference.
    pub(crate) async fn write_history_rows(&self, pending: PendingRows) -> Result<()> {
        for batch in pending.rows.chunks(WRITE_BATCH) {
            let rows: Vec<DerivedRow> = batch
                .iter()
                .map(|r| DerivedRow::new(NODE_KIND, r.hash, Bytes::copy_from_slice(&r.bytes)))
                .collect();
            self.meta().put_derived_many(&rows).await?;
        }
        for row in pending.rows {
            let len = row.bytes.len();
            self.history_rows.insert(row.hash, Arc::new(row.node), len);
        }
        Ok(())
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
