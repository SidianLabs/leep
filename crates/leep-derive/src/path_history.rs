//! Path log and last-change queries answered from the `history-tree-v4`
//! index instead of a scan of all ancestors (ADR 0013).

use std::collections::{BTreeSet, BinaryHeap, HashSet};
use std::ops::Bound;
use std::sync::{Arc, OnceLock};

use bytes::Bytes;
use leep_core::api::{ChangesetInfo, RenameEntry};
use leep_core::rename::RenameOptions;
use leep_core::{
    Changeset, ChangesetId, DerivedRow, Error, FileChange, Hash, MetaStore, RepoPath, Result,
    TreeEntry,
};

use crate::cursor::{FrontierEntry, LogFrom, LogPageRaw};
use crate::derived::history_pack::{
    cache_size, decode_part, part_key, raw_cache_size, Pack, PackBuilder, Part, RawNode,
};
use crate::derived::{
    self, ChunkRef, DerivedKind, HistoryEntry, HistoryNode, HistoryTree, NodeRef, ResolvedRow, Row,
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
    /// Changesets without a row found to have changed the path.
    known: HashSet<ChangesetId>,
}

impl PathWalk {
    async fn start(repo: &Repo, tip: ChangesetId, path: &str, from: &LogFrom) -> Result<Self> {
        let mut w = PathWalk {
            path: path.to_string(),
            seen: HashSet::new(),
            heap: BinaryHeap::new(),
            known: HashSet::new(),
        };
        match &from.seeds {
            // A page's queue: changesets that changed the path, as queued.
            Some(seeds) => {
                for e in seeds {
                    if w.seen.insert(e.id) {
                        w.heap.push((repo.generation(e.id).await?, e.id));
                    }
                }
            }
            None => w.push_frontier(repo, tip).await?,
        }
        Ok(w)
    }

    fn rest(&self) -> Option<(Vec<FrontierEntry>, bool)> {
        (!self.heap.is_empty()).then(|| {
            let entries = self
                .heap
                .iter()
                .map(|&(_, id)| FrontierEntry::id(id))
                .collect();
            (entries, false)
        })
    }

    /// Queue what changed the path at or below `at`: its index entry's
    /// newest changes, or, in a bounded query (ADR 0058), `at` itself when
    /// it has no row, to be examined directly.
    async fn push_frontier(&mut self, repo: &Repo, at: ChangesetId) -> Result<()> {
        if repo.unindexed(at) {
            if self.seen.insert(at) {
                self.heap.push((repo.generation(at).await?, at));
            }
            return Ok(());
        }
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

    /// The next changeset that changed the path. One without a row is
    /// examined (changed relative to its first parent at the path, as in
    /// `log_scan`) and its parents queued either way, so the walk yields
    /// what the indexed walk yields, in the same order.
    async fn next(&mut self, repo: &Repo) -> Result<Option<(u64, ChangesetId, Changeset)>> {
        loop {
            let Some((generation, cid)) = self.heap.pop() else {
                return Ok(None);
            };
            let cs = repo.get_changeset(cid).await?;
            for &p in &cs.parents {
                self.push_frontier(repo, p).await?;
            }
            if repo.unindexed(cid)
                && !self.known.contains(&cid)
                && !repo.examine_path(cid, &cs, &self.path).await?
            {
                continue;
            }
            return Ok(Some((generation, cid, cs)));
        }
    }

    /// Replace every queued changeset without a row by what it stands for:
    /// itself if it changed the path, else its parents' frontier. The queue
    /// then holds only changesets that changed the path, as the indexed
    /// walk's does, so a page carries a `next` cursor exactly when the
    /// indexed walk's would, and the cursor needs no marks.
    async fn settle(&mut self, repo: &Repo) -> Result<()> {
        if repo.bounded.is_none() {
            return Ok(());
        }
        loop {
            let (todo, keep): (Vec<_>, Vec<_>) = std::mem::take(&mut self.heap)
                .into_iter()
                .partition(|(_, id)| repo.unindexed(*id) && !self.known.contains(id));
            self.heap = keep.into_iter().collect();
            if todo.is_empty() {
                return Ok(());
            }
            for (generation, cid) in todo {
                let cs = repo.get_changeset(cid).await?;
                if repo.examine_path(cid, &cs, &self.path).await? {
                    self.known.insert(cid);
                    self.heap.push((generation, cid));
                } else {
                    for &p in &cs.parents {
                        self.push_frontier(repo, p).await?;
                    }
                }
            }
        }
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
    /// Changesets without a row found to have changed the name queued.
    known: HashSet<(ChangesetId, Arc<str>)>,
}

impl FollowWalk {
    async fn push_frontier(&mut self, repo: &Repo, at: ChangesetId, path: &Arc<str>) -> Result<()> {
        if repo.unindexed(at) {
            if self.seen.insert((at, path.clone())) {
                self.heap
                    .push((repo.generation(at).await?, at, path.clone()));
            }
            return Ok(());
        }
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

    fn rest(&self) -> Option<(Vec<FrontierEntry>, bool)> {
        (!self.heap.is_empty()).then(|| {
            let entries = self
                .heap
                .iter()
                .map(|(_, id, name)| FrontierEntry {
                    id: *id,
                    name: Some(name.to_string()),
                    bound: None,
                })
                .collect();
            (entries, false)
        })
    }

    /// [`PathWalk::settle`] for the names queued.
    async fn settle(&mut self, repo: &Repo) -> Result<()> {
        if repo.bounded.is_none() {
            return Ok(());
        }
        loop {
            let (todo, keep): (Vec<_>, Vec<_>) = std::mem::take(&mut self.heap)
                .into_iter()
                .partition(|(_, id, name)| {
                    repo.unindexed(*id) && !self.known.contains(&(*id, name.clone()))
                });
            self.heap = keep.into_iter().collect();
            if todo.is_empty() {
                return Ok(());
            }
            for (generation, cid, name) in todo {
                let cs = repo.get_changeset(cid).await?;
                if repo.examine_path(cid, &cs, &name).await? {
                    self.known.insert((cid, name.clone()));
                    self.heap.push((generation, cid, name));
                } else {
                    for &p in &cs.parents {
                        self.push_frontier(repo, p, &name).await?;
                    }
                }
            }
        }
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
    Row(NodeRef),
    /// A split node and its resolved chunk list.
    Split { node: NodeRef, refs: Vec<ChunkRef> },
}

/// A chunk's row and, for a patch, its keyframe.
type ChunkRows = (Arc<StoredHistoryNode>, Option<Arc<StoredHistoryNode>>);

/// Most history rows one write carries.
const WRITE_BATCH: usize = 1024;

/// The address of `row`: an existing node, or a new one staged in `pending`.
fn stage(pending: &mut PackBuilder, row: Row) -> NodeRef {
    match row {
        Row::Keep(r) => r,
        Row::New(node, key) => pending.stage(node, key),
    }
}

/// Split into components, ignoring empty ones the way tree lookups do.
fn components(path: &str) -> impl Iterator<Item = &str> {
    path.split('/').filter(|c| !c.is_empty())
}

/// Kind and key of part `part` of `cs`'s pack: part 0 is the changeset's
/// `history-tree-v4` value.
pub(crate) fn part_row(cs: ChangesetId, part: u32) -> (&'static str, Hash) {
    if part == 0 {
        (HistoryTree::NAME, cs.0)
    } else {
        (NODE_KIND, part_key(cs, part))
    }
}

fn missing(r: NodeRef) -> Error {
    Error::Storage(format!("history node {r} missing"))
}

fn empty_node() -> Arc<StoredHistoryNode> {
    static EMPTY: OnceLock<Arc<StoredHistoryNode>> = OnceLock::new();
    EMPTY
        .get_or_init(|| Arc::new(StoredHistoryNode::Flat(HistoryNode::default())))
        .clone()
}

/// Read and check one stored history node without any cache, as stored (a
/// patch still names its keyframe's entries by position), and about the
/// bytes it takes. The empty node always exists.
pub(crate) async fn load_history_row(meta: &dyn MetaStore, r: NodeRef) -> Result<(RawNode, usize)> {
    if r == NodeRef::EMPTY {
        return Ok((
            RawNode::Ready(StoredHistoryNode::Flat(HistoryNode::default())),
            1,
        ));
    }
    let (kind, key) = part_row(r.cs, r.part);
    let Some(bytes) = meta.get_derived(kind, &key).await? else {
        return Err(missing(r));
    };
    decode_part(&bytes, r.cs, r.part)?
        .nodes
        .into_iter()
        .nth(r.idx as usize)
        .ok_or_else(|| missing(r))
}

/// [`resolve_row`] of a node as decoded, whose patch may still name its
/// keyframe's entries by position.
pub(crate) fn resolve_raw_row(
    r: NodeRef,
    row: RawNode,
    key: Option<&StoredHistoryNode>,
) -> Result<ResolvedRow> {
    let named = row
        .resolve(key)
        .map_err(|why| Error::Storage(format!("corrupt history node {r}: {why}")))?;
    resolve_row(r, named, key)
}

/// `row`, stored at `r`, with its patch applied to `key` (the row at its
/// base, as stored). A patch must point at a keyframe of its own shape, so a
/// damaged or foreign row is an error, not a wrong answer.
pub(crate) fn resolve_row(
    r: NodeRef,
    row: StoredHistoryNode,
    key: Option<&StoredHistoryNode>,
) -> Result<ResolvedRow> {
    let keyframe = row.base().unwrap_or(r);
    match row.resolve(key) {
        Ok(node) => Ok(ResolvedRow { node, keyframe }),
        Err(why) => Err(Error::Storage(format!("corrupt history node {r}: {why}"))),
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
    node: NodeRef,
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

pub(crate) fn bad_chunk(node: NodeRef, i: usize) -> Error {
    Error::Storage(format!(
        "corrupt history node {node}: chunk {i} does not fit"
    ))
}

fn misfit(node: NodeRef, first: &str) -> Error {
    Error::Storage(format!(
        "corrupt history node {node}: the chunk at {first:?} does not fit"
    ))
}

/// Where a lookup goes in a split node: the chunk whose range holds the name,
/// that range's first name, and the next chunk's first name.
struct ChunkAt {
    first: String,
    node: NodeRef,
    next: Option<String>,
}

/// Whether the names `lo..=hi` lie in the range `first..next`.
fn within(lo: &str, hi: &str, first: &str, next: Option<&str>) -> bool {
    lo >= first && next.is_none_or(|n| hi < n)
}

/// Where chunk `k` of `refs` ends: the next chunk's first name, `None` for
/// the last.
fn run_end(refs: &[ChunkRef], k: usize) -> Option<&str> {
    refs.get(k + 1).map(|r| r.first.as_str())
}

/// Append to `out` the names whose entries differ between two name-ordered
/// entry lists, with both entries.
fn diff_sorted<'a>(
    a: impl Iterator<Item = (&'a String, &'a HistoryEntry)>,
    b: impl Iterator<Item = (&'a String, &'a HistoryEntry)>,
    out: &mut Vec<(String, Option<HistoryEntry>, Option<HistoryEntry>)>,
) {
    let (mut a, mut b) = (a.peekable(), b.peekable());
    loop {
        match (a.peek(), b.peek()) {
            (Some(x), Some(y)) if x.0 == y.0 => {
                if x.1 != y.1 {
                    out.push((x.0.clone(), Some(x.1.clone()), Some(y.1.clone())));
                }
                a.next();
                b.next();
            }
            (Some(x), Some(y)) if x.0 < y.0 => {
                out.push((x.0.clone(), Some(x.1.clone()), None));
                a.next();
            }
            (Some(x), None) => {
                out.push((x.0.clone(), Some(x.1.clone()), None));
                a.next();
            }
            (_, Some(y)) => {
                out.push((y.0.clone(), None, Some(y.1.clone())));
                b.next();
            }
            (None, None) => break,
        }
    }
}

impl Repo {
    /// Root history node of `id`, deriving the index for its ancestors first.
    pub(crate) async fn history_root(&self, id: ChangesetId) -> Result<NodeRef> {
        if let Some(r) = self.history_roots.get(&id.0) {
            return Ok(*r);
        }
        let value = derived::derive::<HistoryTree>(self, id).await?;
        let part = Part::read(&value, id, 0)?;
        let root = part.root.expect("part 0 names a root");
        self.history_roots.insert(id.0, Arc::new(root), 32);
        let size = part.size();
        self.history_parts.insert(id.0, Arc::new(part), size);
        Ok(root)
    }

    /// Read one stored history node: a whole node or a chunk, a split node's
    /// chunk list, or a patch of either, with a patch's entries named (which
    /// reads its keyframe). The empty node always exists.
    pub(crate) async fn read_history_row(&self, r: NodeRef) -> Result<Arc<StoredHistoryNode>> {
        let raw = match self.fetch_history_row(r).await? {
            Ok(node) => return Ok(node),
            Err(raw) => raw,
        };
        let base = raw.base().expect("only patches wait for a keyframe");
        let key = match self.fetch_history_row(base).await? {
            Ok(key) => key,
            Err(_) => {
                return Err(Error::Storage(format!(
                    "corrupt history node {r}: its base is not a keyframe of its shape"
                )))
            }
        };
        let named = raw
            .resolve_ref(Some(&key))
            .map_err(|why| Error::Storage(format!("corrupt history node {r}: {why}")))?;
        let node = Arc::new(named);
        self.history_rows
            .insert(r.cache_key(), node.clone(), cache_size(&node));
        Ok(node)
    }

    /// A node from the caches, the staged packs or the store: `Ok` if it is
    /// ready to use, `Err` with a patch that still names its keyframe's
    /// entries by position.
    async fn fetch_history_row(
        &self,
        r: NodeRef,
    ) -> Result<std::result::Result<Arc<StoredHistoryNode>, Arc<RawNode>>> {
        if r == NodeRef::EMPTY {
            return Ok(Ok(empty_node()));
        }
        let k = r.cache_key();
        if let Some(n) = self.history_rows.get(&k) {
            return Ok(Ok(n));
        }
        if let Some(n) = self.history_staged.lock().unwrap().nodes.get(&k) {
            return Ok(Ok(n.clone()));
        }
        if let Some(raw) = self.history_raw.get(&k) {
            return Ok(Err(raw));
        }
        let (kind, key) = part_row(r.cs, r.part);
        let part = match self.history_parts.get(&key) {
            Some(p) => p,
            None => {
                let Some(bytes) = self.meta().get_derived(kind, &key).await? else {
                    // A bounded query (ADR 0058) whose rows were dropped
                    // under it answers that the index is behind, as when a
                    // row it looks up first is gone.
                    if self.bounded.is_some() {
                        return Err(crate::indexing::bounded::rows_vanished(
                            crate::indexing::IndexGroup::History,
                        ));
                    }
                    return Err(missing(r));
                };
                self.cache_part(r.cs, r.part, &bytes)?
            }
        };
        match part.node(r.idx).ok_or_else(|| missing(r))?? {
            RawNode::Ready(n) => {
                let size = cache_size(&n);
                let n = Arc::new(n);
                self.history_rows.insert(k, n.clone(), size);
                Ok(Ok(n))
            }
            raw => {
                let size = raw_cache_size(&raw);
                let raw = Arc::new(raw);
                self.history_raw.insert(k, raw.clone(), size);
                Ok(Err(raw))
            }
        }
    }

    /// Check a part and cache it; its nodes are decoded when first read.
    fn cache_part(&self, cs: ChangesetId, part: u32, bytes: &[u8]) -> Result<Arc<Part>> {
        let p = Arc::new(Part::read(bytes, cs, part)?);
        let size = p.size();
        self.history_parts
            .insert(part_row(cs, part).1, p.clone(), size);
        Ok(p)
    }

    /// Whether `r` can be read without the store.
    fn history_row_cached(&self, r: NodeRef) -> bool {
        let k = r.cache_key();
        r == NodeRef::EMPTY
            || self.history_rows.get(&k).is_some()
            || self.history_raw.get(&k).is_some()
            || self.history_parts.get(&part_row(r.cs, r.part).1).is_some()
            || self.history_staged.lock().unwrap().nodes.contains_key(&k)
    }

    /// Read the parts holding the rows `refs` that are not cached, and then
    /// those holding the keyframes they are patches on, in one batched read
    /// per kind each, so joining a split node costs two round trips rather
    /// than one or two per chunk. Only fills the cache: whatever this misses
    /// or cannot check is read again, singly, by the caller, with its errors.
    pub(crate) async fn prefetch_history_rows(&self, refs: impl Iterator<Item = NodeRef>) {
        let mut want: Vec<NodeRef> = refs.filter(|r| !self.history_row_cached(*r)).collect();
        let mut asked: Vec<NodeRef> = want.clone();
        for round in 0..2 {
            let mut parts: Vec<(ChangesetId, u32)> = want.iter().map(|r| (r.cs, r.part)).collect();
            parts.sort();
            parts.dedup();
            if round == 0 && want.len() < 2 {
                return;
            }
            for kind in [HistoryTree::NAME, NODE_KIND] {
                let these: Vec<(ChangesetId, u32)> = parts
                    .iter()
                    .copied()
                    .filter(|(cs, p)| part_row(*cs, *p).0 == kind)
                    .collect();
                if these.is_empty() {
                    continue;
                }
                let keys: Vec<Hash> = these.iter().map(|(cs, p)| part_row(*cs, *p).1).collect();
                let Ok(found) = self.meta().get_derived_many(kind, &keys).await else {
                    return;
                };
                for ((cs, p), bytes) in these.iter().zip(found) {
                    if let Some(b) = bytes {
                        let _ = self.cache_part(*cs, *p, &b);
                    }
                }
            }
            // The keyframes of the patches among them.
            want = asked
                .iter()
                .filter_map(|r| {
                    let part = self.history_parts.get(&part_row(r.cs, r.part).1)?;
                    part.node(r.idx)?.ok()?.base()
                })
                .filter(|b| !self.history_row_cached(*b))
                .collect();
            asked = Vec::new();
            if want.is_empty() {
                return;
            }
        }
    }

    /// The keyframe the patch `row` (at `r`) is stored against, checked to
    /// be a keyframe of its shape; `None` if `row` is a keyframe.
    async fn history_key(
        &self,
        r: NodeRef,
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
                "corrupt history node {r}: its base is not a keyframe of its shape"
            ))),
        }
    }

    /// A row as a keyframe, with any patch applied, and its keyframe.
    async fn read_resolved(&self, r: NodeRef) -> Result<ResolvedRow> {
        let row = self.read_history_row(r).await?;
        let key = self.history_key(r, &row).await?;
        resolve_row(r, (*row).clone(), key.as_deref())
    }

    /// Read one history node, joining its chunks if it is split. Derivation
    /// edits nodes chunk by chunk instead; tests compare against this.
    #[cfg(test)]
    pub(crate) async fn read_history_node(&self, node: NodeRef) -> Result<HistoryNode> {
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
    /// with both entries, in name order.
    ///
    /// Split nodes are compared a run of chunks at a time, between first
    /// names both lists share, so names in a run are in no other chunk of
    /// either side. A run of one chunk the two share is skipped unread. A
    /// run of one chunk a side, both patches on the same keyframe, differs
    /// at most in the names the patches set, so only those are compared: the
    /// merge base's and the second parent's versions of a directory are
    /// usually such siblings, and joining them cost as much as the rest of
    /// a merge. Other runs are joined and compared in full.
    pub(crate) async fn diff_history_nodes(
        &self,
        a: NodeRef,
        b: NodeRef,
    ) -> Result<Vec<(String, Option<HistoryEntry>, Option<HistoryEntry>)>> {
        let (ra, rb) = (self.read_resolved(a).await?, self.read_resolved(b).await?);
        let (x, y) = match (ra.node, rb.node) {
            (StoredHistoryNode::Split(x), StoredHistoryNode::Split(y)) => (x, y),
            (na, nb) => {
                let ea = self.resolved_entries(a, na).await?;
                let eb = self.resolved_entries(b, nb).await?;
                let mut out = Vec::new();
                diff_sorted(
                    ea.iter().map(|(n, e)| (n, e)),
                    eb.iter().map(|(n, e)| (n, e)),
                    &mut out,
                );
                return Ok(out);
            }
        };
        let shared: HashSet<NodeRef> = {
            let xs: HashSet<NodeRef> = x.iter().map(|r| r.node).collect();
            y.iter()
                .map(|r| r.node)
                .filter(|h| xs.contains(h))
                .collect()
        };
        self.prefetch_history_rows(
            x.iter()
                .chain(&y)
                .map(|r| r.node)
                .filter(|h| !shared.contains(h)),
        )
        .await;
        let mut out = Vec::new();
        let (mut i, mut j) = (0, 0);
        while i < x.len() || j < y.len() {
            // Grow the run on whichever side ends first until both end at
            // the same name (or both run out).
            let (i0, j0) = (i, j);
            if i >= x.len() || j >= y.len() {
                // One side is out of chunks: the rest of the other is one run.
                i = x.len().max(i);
                j = y.len().max(j);
                let ea = self.run_entries(a, &x, i0..i).await?;
                let eb = self.run_entries(b, &y, j0..j).await?;
                diff_sorted(
                    ea.iter().map(|(n, e)| (n, e)),
                    eb.iter().map(|(n, e)| (n, e)),
                    &mut out,
                );
                break;
            }
            loop {
                let (ex, ey) = (run_end(&x, i), run_end(&y, j));
                if ex == ey {
                    break;
                }
                if ey.is_none() || ex.is_some_and(|ex| ey.is_some_and(|ey| ex < ey)) {
                    i += 1;
                } else {
                    j += 1;
                }
            }
            i += 1;
            j += 1;
            if i - i0 == 1 && j - j0 == 1 {
                if x[i0].node == y[j0].node {
                    continue;
                }
                if self
                    .diff_sibling_chunks(a, &x, i0, b, &y, j0, &mut out)
                    .await?
                {
                    continue;
                }
            }
            let ea = self.run_entries(a, &x, i0..i).await?;
            let eb = self.run_entries(b, &y, j0..j).await?;
            diff_sorted(
                ea.iter().map(|(n, e)| (n, e)),
                eb.iter().map(|(n, e)| (n, e)),
                &mut out,
            );
        }
        Ok(out)
    }

    /// The entries of a resolved row: a whole node's, or all its chunks'.
    async fn resolved_entries(
        &self,
        node: NodeRef,
        row: StoredHistoryNode,
    ) -> Result<Vec<(String, HistoryEntry)>> {
        match row {
            StoredHistoryNode::Flat(n) => Ok(n.entries.into_iter().collect()),
            StoredHistoryNode::Split(refs) => self.run_entries(node, &refs, 0..refs.len()).await,
            _ => unreachable!("resolved rows are Flat or Split"),
        }
    }

    /// The entries of chunks `run` of the split node `node`, in name order.
    async fn run_entries(
        &self,
        node: NodeRef,
        refs: &[ChunkRef],
        run: std::ops::Range<usize>,
    ) -> Result<Vec<(String, HistoryEntry)>> {
        let rows = self.chunk_rows(&refs[run.clone()]).await?;
        let mut out = Vec::new();
        for (i, (row, key)) in run.zip(&rows) {
            out.extend(
                chunk_view(node, refs, i, row, key.as_deref())?
                    .into_iter()
                    .map(|(n, e)| (n.clone(), e.clone())),
            );
        }
        Ok(out)
    }

    /// Diff chunk `i` of `a` (split as `x`) against chunk `j` of `b` (split
    /// as `y`), which cover the same names, through their patches if both
    /// are patches on one keyframe; `false` if they are not.
    #[allow(clippy::too_many_arguments)]
    async fn diff_sibling_chunks(
        &self,
        a: NodeRef,
        x: &[ChunkRef],
        i: usize,
        b: NodeRef,
        y: &[ChunkRef],
        j: usize,
        out: &mut Vec<(String, Option<HistoryEntry>, Option<HistoryEntry>)>,
    ) -> Result<bool> {
        let (ra, rb) = (
            self.read_history_row(x[i].node).await?,
            self.read_history_row(y[j].node).await?,
        );
        let (
            StoredHistoryNode::Patch { base: ka, over: oa },
            StoredHistoryNode::Patch { base: kb, over: ob },
        ) = (&*ra, &*rb)
        else {
            return Ok(false);
        };
        if ka != kb {
            return Ok(false);
        }
        let key = self
            .history_key(x[i].node, &ra)
            .await?
            .expect("a patch has a keyframe");
        let at_a = ChunkAt {
            first: x[i].first.clone(),
            node: x[i].node,
            next: x.get(i + 1).map(|r| r.first.clone()),
        };
        let at_b = ChunkAt {
            first: y[j].first.clone(),
            node: y[j].node,
            next: y.get(j + 1).map(|r| r.first.clone()),
        };
        // The checks a lookup through each patch makes.
        for (node, at, row, over) in [(a, &at_a, &ra, oa), (b, &at_b, &rb, ob)] {
            match (over.entries.keys().next(), over.entries.keys().next_back()) {
                (Some(lo), Some(hi)) if within(lo, hi, &at.first, at.next.as_deref()) => {}
                _ => return Err(misfit(node, &at.first)),
            }
            self.patched_chunk_child(node, at, row, &key, "")?;
        }
        let StoredHistoryNode::Flat(key) = &*key else {
            unreachable!("history_key checked the shape")
        };
        let names: BTreeSet<&String> = oa.entries.keys().chain(ob.entries.keys()).collect();
        for name in names {
            let ea = oa.entries.get(name).or_else(|| key.entries.get(name));
            let eb = ob.entries.get(name).or_else(|| key.entries.get(name));
            if ea != eb {
                out.push((name.clone(), ea.cloned(), eb.cloned()));
            }
        }
        Ok(true)
    }

    /// The entry `name` of the node `node`, reading only the chunk that would
    /// hold it. A patch is read with its keyframe, whose entries it names by
    /// position, but once read a name it sets (a recently changed one) is
    /// answered from the patch alone.
    pub(crate) async fn history_child(
        &self,
        node: NodeRef,
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
    pub(crate) async fn node_lookup(&self, node: NodeRef) -> Result<NodeLookup> {
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

    /// The rows [`Repo::lookup_child`] would read to find `name`, as far as
    /// the cache tells: the row it lands in and, for a cached patch, its
    /// keyframe. For batched reads ahead of lookups.
    pub(crate) fn lookup_rows(&self, lookup: &NodeLookup, name: &str) -> Vec<NodeRef> {
        let row = match lookup {
            NodeLookup::Row(node) => *node,
            NodeLookup::Split { refs, .. } => {
                let i = refs.partition_point(|r| r.first.as_str() <= name);
                if i == 0 {
                    return Vec::new();
                }
                refs[i - 1].node
            }
        };
        let mut out = vec![row];
        if let Some(base) = self
            .history_rows
            .get(&row.cache_key())
            .and_then(|r| r.base())
        {
            out.push(base);
        }
        out
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
        node: NodeRef,
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
        node: NodeRef,
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

    /// Store `node`, written by `writer`, as the root of `writer`'s pack,
    /// and return its address. `base` is the stored node it was edited from:
    /// rows shared with it are not written again, so an edit to a split node
    /// writes one chunk and the chunk list, and each changed row is a patch
    /// on the keyframe of its predecessor in `base` while that is small (see
    /// `flat_row`). The rows depend only on `node`, `writer` and the stored
    /// `base`, so derivation stays deterministic.
    #[cfg(test)]
    pub(crate) async fn put_history_node(
        &self,
        writer: ChangesetId,
        node: &HistoryNode,
        base: Option<NodeRef>,
    ) -> Result<NodeRef> {
        let mut pending = PackBuilder::new(writer);
        let r = self.stage_history_node(node, base, &mut pending).await?;
        let pack = pending.finish(r);
        let root = crate::derived::history_pack::decode_root(writer, &pack.value)?;
        let value = self.write_history_pack(pack).await?;
        self.meta()
            .put_derived(HistoryTree::NAME, &writer.0, value)
            .await?;
        Ok(root)
    }

    /// [`Repo::put_history_node`] without the write: the nodes go to
    /// `pending`, the pack of the changeset being derived, and its address
    /// is returned (the base itself if nothing changed).
    pub(crate) async fn stage_history_node(
        &self,
        node: &HistoryNode,
        base: Option<NodeRef>,
        pending: &mut PackBuilder,
    ) -> Result<NodeRef> {
        if node.entries.is_empty() {
            return Ok(NodeRef::EMPTY);
        }
        let base_row = match base {
            Some(b) => Some(self.read_resolved(b).await?),
            None => None,
        };
        let entries: Vec<(&String, &HistoryEntry)> = node.entries.iter().collect();
        let top = match derived::chunk_ranges(entries.iter().map(|(n, _)| n.as_str())) {
            None => {
                if let (Some(b), Some(StoredHistoryNode::Flat(n))) =
                    (base, base_row.as_ref().map(|r| &r.node))
                {
                    if n == node {
                        return Ok(b);
                    }
                }
                let key = match &base_row {
                    Some(b) if matches!(b.node, StoredHistoryNode::Flat(_)) => {
                        Some((b.keyframe, self.read_history_row(b.keyframe).await?))
                    }
                    _ => None,
                };
                derived::flat_row(pending.cs, node, key.as_ref().map(|(h, k)| (*h, k)))
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
                    let chunk = HistoryNode {
                        entries: want
                            .iter()
                            .map(|(n, e)| ((*n).clone(), (*e).clone()))
                            .collect(),
                    };
                    let row =
                        derived::flat_row(pending.cs, &chunk, key.as_ref().map(|(h, k)| (*h, k)));
                    refs.push(ChunkRef {
                        first,
                        node: stage(pending, row),
                    });
                }
                if let (Some(b), Some(brefs)) = (base, base_refs) {
                    if brefs == refs.as_slice() {
                        return Ok(b);
                    }
                }
                let key = match &base_row {
                    Some(b) if matches!(b.node, StoredHistoryNode::Split(_)) => {
                        Some((b.keyframe, self.read_history_row(b.keyframe).await?))
                    }
                    _ => None,
                };
                derived::split_row(pending.cs, refs, key.as_ref().map(|(h, k)| (*h, k)))
            }
        };
        Ok(stage(pending, top))
    }

    /// [`Repo::stage_history_node`] of the stored node `base` (or an empty
    /// node) with the entries of `updates`, sorted by name, added or
    /// replaced, without joining `base`: the same rows and address, at a cost
    /// that follows the edit rather than the node's width.
    ///
    /// A split node is cut only after boundary names, wherever they lie, and
    /// within the 128-entry cap from the previous cut. So the runs of chunks
    /// between boundary names that hold no updated name are cut as before
    /// and are the base's chunks; only the runs that do are read, edited and
    /// cut again, each chunk against the same predecessor (the base chunk
    /// whose range holds its first name) that a full rebuild compares it
    /// with. Names are never removed, so a split base stays split. On Linux,
    /// directories of thousands of names made joining and re-cutting whole
    /// nodes most of the cost of every changeset.
    pub(crate) async fn stage_history_edit(
        &self,
        base: Option<NodeRef>,
        updates: Vec<(String, HistoryEntry)>,
        pending: &mut PackBuilder,
    ) -> Result<NodeRef> {
        let Some(b) = base else {
            let node = HistoryNode {
                entries: updates.into_iter().collect(),
            };
            return self.stage_history_node(&node, None, pending).await;
        };
        let ResolvedRow {
            node: resolved,
            keyframe,
        } = self.read_resolved(b).await?;
        let refs = match resolved {
            StoredHistoryNode::Split(refs) => refs,
            StoredHistoryNode::Flat(mut node) => {
                if updates.iter().all(|(n, e)| node.entries.get(n) == Some(e)) {
                    return Ok(b);
                }
                node.entries.extend(updates);
                if node.entries.len() > derived::FLAT_MAX {
                    return self.stage_history_node(&node, Some(b), pending).await;
                }
                // `stage_history_node` for a whole base, which is resolved
                // already.
                #[cfg(test)]
                let mut full = pending.clone();
                let key = self.read_history_row(keyframe).await?;
                let row = derived::flat_row(pending.cs, &node, Some((keyframe, &key)));
                let at = stage(pending, row);
                #[cfg(test)]
                {
                    let from = full.len();
                    let want = self.stage_history_node(&node, Some(b), &mut full).await?;
                    assert_eq!(want, at, "an edit must store what a full rebuild does");
                    assert_eq!(pending.tail(from), full.tail(from));
                }
                return Ok(at);
            }
            _ => unreachable!("resolved rows are Flat or Split"),
        };
        #[cfg(test)]
        let mut full = pending.clone();
        #[cfg(test)]
        let joined = {
            let mut node = self.read_history_node(b).await?;
            node.entries.extend(updates.iter().cloned());
            node
        };
        let chunk_of = |name: &str| {
            refs.partition_point(|r| r.first.as_str() <= name)
                .saturating_sub(1)
        };
        // The chunks edited and their neighbours, whose last names decide
        // where runs start, in one read.
        let last = refs.len() - 1;
        let mut wanted: Vec<usize> = updates
            .iter()
            .flat_map(|(n, _)| {
                let i = chunk_of(n);
                [i.saturating_sub(1), i, (i + 1).min(last)]
            })
            .collect();
        wanted.dedup();
        self.prefetch_history_rows(wanted.iter().map(|&i| refs[i].node))
            .await;

        let mut out: Vec<ChunkRef> = Vec::with_capacity(refs.len() + 1);
        // Base chunks before `next` are passed on or replaced; `next` is 0
        // or follows a boundary name.
        let mut next = 0;
        let mut updates = updates.into_iter().peekable();
        while let Some((name, _)) = updates.peek() {
            let mut start = chunk_of(name);
            while start > next {
                let prev = self.chunk_entries(b, &refs, start - 1).await?;
                if prev.last().is_some_and(|(n, _)| derived::ends_chunk(n)) {
                    break;
                }
                start -= 1;
            }
            out.extend_from_slice(&refs[next..start]);
            let mut entries: Vec<(String, HistoryEntry)> = Vec::new();
            let mut end = start;
            loop {
                let mut chunk = self
                    .chunk_entries(b, &refs, end)
                    .await?
                    .into_iter()
                    .peekable();
                let mut mine = Vec::new();
                while updates.peek().is_some_and(|(n, _)| chunk_of(n) == end) {
                    mine.push(updates.next().expect("peeked"));
                }
                let mut mine = mine.into_iter().peekable();
                loop {
                    let order = match (chunk.peek(), mine.peek()) {
                        (Some(c), Some(m)) => m.0.cmp(&c.0),
                        (None, Some(_)) => std::cmp::Ordering::Less,
                        (Some(_), None) => std::cmp::Ordering::Greater,
                        (None, None) => break,
                    };
                    if order == std::cmp::Ordering::Equal {
                        chunk.next();
                    }
                    entries.extend(if order.is_gt() {
                        chunk.next()
                    } else {
                        mine.next()
                    });
                }
                end += 1;
                if end == refs.len() || entries.last().is_some_and(|(n, _)| derived::ends_chunk(n))
                {
                    break;
                }
            }
            for range in derived::run_ranges(entries.iter().map(|(n, _)| n.as_str())) {
                let want = &entries[range];
                let first = want[0].0.clone();
                let mut key = None;
                let i = refs.partition_point(|r| r.first <= first);
                if i > 0 {
                    let at = refs[i - 1].node;
                    let row = self.read_history_row(at).await?;
                    let row_key = self.history_key(at, &row).await?;
                    let view = chunk_view(b, &refs, i - 1, &row, row_key.as_deref())?;
                    if view.len() == want.len()
                        && view
                            .iter()
                            .zip(want)
                            .all(|(v, w)| *v.0 == w.0 && *v.1 == w.1)
                    {
                        out.push(ChunkRef { first, node: at });
                        continue;
                    }
                    key = Some(match row_key {
                        Some(k) => (row.base().expect("a patch has a base"), k),
                        None => (at, row),
                    });
                }
                let chunk = HistoryNode {
                    entries: want.iter().cloned().collect(),
                };
                let row = derived::flat_row(pending.cs, &chunk, key.as_ref().map(|(h, k)| (*h, k)));
                out.push(ChunkRef {
                    first,
                    node: stage(pending, row),
                });
            }
            next = end;
        }
        out.extend_from_slice(&refs[next..]);
        let at = if out == refs {
            b
        } else {
            let key = self.read_history_row(keyframe).await?;
            stage(
                pending,
                derived::split_row(pending.cs, out, Some((keyframe, &key))),
            )
        };
        #[cfg(test)]
        {
            let from = full.len();
            let want = self.stage_history_node(&joined, Some(b), &mut full).await?;
            assert_eq!(want, at, "an edit must store what a full rebuild does");
            assert_eq!(
                pending.tail(from),
                full.tail(from),
                "an edit must write the rows a full rebuild does"
            );
        }
        Ok(at)
    }

    /// The entries of chunk `i` of the split node `node`, checked to fit it.
    async fn chunk_entries(
        &self,
        node: NodeRef,
        refs: &[ChunkRef],
        i: usize,
    ) -> Result<Vec<(String, HistoryEntry)>> {
        let row = self.read_history_row(refs[i].node).await?;
        let key = self.history_key(refs[i].node, &row).await?;
        Ok(chunk_view(node, refs, i, &row, key.as_deref())?
            .into_iter()
            .map(|(n, e)| (n.clone(), e.clone()))
            .collect())
    }

    /// Hand a finished pack to a caller that writes it later: its parts
    /// after the first go to `rows`, in order, and part 0 is returned, the
    /// changeset's value. Until [`Repo::unstage_history_rows`], reads find
    /// its nodes here, since the cache may drop them before they are stored.
    pub(crate) fn stage_history_pack(&self, pack: Pack, rows: &mut Vec<DerivedRow>) -> Bytes {
        let mut staged = self.history_staged.lock().unwrap();
        for (r, node, size) in pack.nodes {
            let k = r.cache_key();
            self.history_rows.insert(k, node.clone(), size);
            staged.nodes.insert(k, node);
            staged
                .by_row
                .entry(part_row(r.cs, r.part).1)
                .or_default()
                .push(k);
        }
        for (key, bytes) in pack.rows {
            rows.push(DerivedRow::new(NODE_KIND, key, bytes));
        }
        pack.value
    }

    /// Forget the staged nodes of the parts among `rows`, once stored or
    /// dropped.
    pub(crate) fn unstage_history_rows(&self, rows: &[DerivedRow]) {
        let mut staged = self.history_staged.lock().unwrap();
        for row in rows
            .iter()
            .filter(|r| r.kind == NODE_KIND || r.kind == HistoryTree::NAME)
        {
            if let Some(keys) = staged.by_row.remove(&row.key) {
                for k in keys {
                    staged.nodes.remove(&k);
                }
            }
        }
    }

    /// Store a finished pack's parts after the first, in order (children's
    /// parts before their parents'), and return part 0, the changeset's
    /// value, for the caller to store last.
    pub(crate) async fn write_history_pack(&self, pack: Pack) -> Result<Bytes> {
        for batch in pack.rows.chunks(WRITE_BATCH) {
            let rows: Vec<DerivedRow> = batch
                .iter()
                .map(|(k, v)| DerivedRow::new(NODE_KIND, *k, v.clone()))
                .collect();
            self.meta().put_derived_many(&rows).await?;
        }
        for (r, node, size) in pack.nodes {
            self.history_rows.insert(r.cache_key(), node, size);
        }
        Ok(pack.value)
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
        Ok(self
            .log_indexed_from(id, path, limit, filter, &LogFrom::tip())
            .await?
            .entries)
    }

    /// [`Repo::log_indexed`] from `from`, returning what is still queued.
    pub(crate) async fn log_indexed_from(
        &self,
        id: ChangesetId,
        path: &str,
        limit: usize,
        filter: &LogFilter,
        from: &LogFrom,
    ) -> Result<LogPageRaw> {
        let mut out = Vec::new();
        let mut walk = PathWalk::start(self, id, path, from).await?;
        while out.len() < limit {
            let Some((generation, cid, cs)) = walk.next(self).await? else {
                break;
            };
            if !from.admits(generation, cid) || !filter.matches(&cs) {
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
        walk.settle(self).await?;
        Ok(LogPageRaw {
            entries: out,
            rest: walk.rest(),
        })
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
        Ok(self
            .log_follow_from(id, path, limit, filter, &LogFrom::tip())
            .await?
            .entries)
    }

    /// [`Repo::log_follow`] from `from`, returning what is still queued.
    pub(crate) async fn log_follow_from(
        &self,
        id: ChangesetId,
        path: &str,
        limit: usize,
        filter: &LogFilter,
        from: &LogFrom,
    ) -> Result<LogPageRaw> {
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
            known: HashSet::new(),
        };
        match &from.seeds {
            Some(seeds) => {
                for e in seeds {
                    let name: Arc<str> = e.name.as_deref().unwrap_or(path).into();
                    if walk.seen.insert((e.id, name.clone())) {
                        walk.heap.push((self.generation(e.id).await?, e.id, name));
                    }
                }
            }
            None => walk.push_frontier(self, id, &start).await?,
        }
        let mut out = Vec::new();
        while out.len() < limit {
            let Some((generation, cid, mut names)) = walk.pop_group() else {
                break;
            };
            let cs = self.get_changeset(cid).await?;
            if self.unindexed(cid) {
                // Without a row: keep the names it changed, and follow the
                // others into its parents unchanged.
                let mut changed = Vec::with_capacity(names.len());
                for name in names {
                    if walk.known.contains(&(cid, name.clone()))
                        || self.examine_path(cid, &cs, &name).await?
                    {
                        changed.push(name);
                    } else {
                        for &p in &cs.parents {
                            walk.push_frontier(self, p, &name).await?;
                        }
                    }
                }
                if changed.is_empty() {
                    continue;
                }
                names = changed;
            }
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
            if !from.admits(generation, cid) || !filter.matches(&cs) {
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
        walk.settle(self).await?;
        Ok(LogPageRaw {
            entries: out,
            rest: walk.rest(),
        })
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
