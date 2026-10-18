//! Path-history index (ADR 0013): a tree parallel to `root_tree` that records,
//! for every path that ever existed, which changesets last changed it.
//!
//! A changeset's derivation writes the history nodes it changes into one
//! pack (ADR 0045, see `history_pack`): its `history-tree-v4` value is the
//! pack's first part and names the root, and every node is addressed by a
//! [`NodeRef`] (writer, part, index), so a changeset shares every node it
//! did not touch with its first parent. Large nodes are stored in chunks
//! (ADR 0027), so a change to one entry of a wide directory rewrites one
//! chunk, not the whole node, and a changed node or chunk is stored as a
//! patch on an earlier version (ADR 0037), so it costs the entries that
//! changed, not all of them. Derivation reads only the spines of changed
//! paths, like `root_tree`.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use leep_core::{Changeset, ChangesetId, DerivedRow, Hash, Result, TreeEntry, TreeNode};

use super::history_pack::{cost, decode_root, PackBuilder};
use super::DerivedKind;
use crate::ancestry::Ancestors;
use crate::path_history::NodeLookup;
use crate::tree_ops::{join, TreeView};
use crate::Repo;

/// Meta-store kind holding the parts of packs after the first, keyed by
/// `history_pack::part_key`. The labels are the only versioned names: `-v1`
/// rows held whole nodes, `-v2` rows had no patches and `-v3` rows were one
/// content-addressed row per node, and an older server may still read them.
pub const NODE_KIND: &str = "history-node-v4";

/// Nodes with at most this many entries are stored whole.
pub const FLAT_MAX: usize = 64;
/// Most entries in one chunk, so a run of names without a boundary cannot
/// make a chunk as large as the node it splits.
pub const CHUNK_MAX: usize = 128;
/// A changed node is stored as a patch on its predecessor's keyframe while
/// `patch² <= PATCH_BUDGET * whole` (bytes); past that a new keyframe starts.
/// See [`worth_patching`].
pub const PATCH_BUDGET: usize = 96;
/// Domain of the hash that picks chunk boundaries from entry names.
const BOUNDARY_DOMAIN: &str = "history-chunk-boundary";

/// The part of a [`NodeRef`] to a node of a pack still being built.
pub const PENDING: u32 = u32::MAX;

/// Where a history node is stored: node `idx` of part `part` of the pack
/// `cs`'s derivation wrote. Packs are a function of their changeset, so a
/// reference always names the same content.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NodeRef {
    pub cs: ChangesetId,
    pub part: u32,
    pub idx: u32,
}

impl NodeRef {
    /// The empty node, which is never stored.
    pub const EMPTY: NodeRef = NodeRef {
        cs: ChangesetId(Hash([0; 32])),
        part: 0,
        idx: 0,
    };

    /// A key for in-memory maps: the writer with the position mixed into its
    /// first bytes (positions are unique per writer, and writers differ in
    /// their other 24 bytes).
    pub(crate) fn cache_key(&self) -> Hash {
        let mut k = self.cs.0;
        let pos = ((self.part as u64) << 32 | self.idx as u64).to_le_bytes();
        for (b, p) in k.0.iter_mut().zip(pos) {
            *b ^= p;
        }
        k
    }
}

impl fmt::Display for NodeRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}/{}", self.cs, self.part, self.idx)
    }
}

impl fmt::Debug for NodeRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}/{}", &self.cs.0.to_hex()[..8], self.part, self.idx)
    }
}

/// One directory of the history tree. Unlike a `TreeNode` it keeps entries for
/// deleted paths, because the log of a deleted path must still find the
/// deleting changeset.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct HistoryNode {
    pub entries: BTreeMap<String, HistoryEntry>,
}

/// What is known about one path component. `changed` is the set of newest
/// changesets that changed the path: the maximal elements, under ancestry, of
/// all changesets whose entry for the path differs from their first parent's.
/// It is one id unless merged branches changed the path concurrently, sorted
/// by id so equal sets encode equally. For a directory this covers any change
/// beneath it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HistoryEntry {
    /// Only ever a file, or a directory with no history below it.
    Leaf { changed: Vec<ChangesetId> },
    /// Has been a directory: `node` holds the history of its children,
    /// including deleted ones. A path that changed kind keeps both lineages in
    /// `changed`.
    Tree {
        changed: Vec<ChangesetId>,
        node: NodeRef,
    },
}

impl HistoryEntry {
    pub fn changed(&self) -> &[ChangesetId] {
        match self {
            HistoryEntry::Leaf { changed } | HistoryEntry::Tree { changed, .. } => changed,
        }
    }

    pub fn node(&self) -> Option<NodeRef> {
        match self {
            HistoryEntry::Leaf { .. } => None,
            HistoryEntry::Tree { node, .. } => Some(*node),
        }
    }
}

impl HistoryNode {
    /// The node cut into chunks, or `None` if it is stored whole. Chunk
    /// boundaries depend only on entry names (see [`ends_chunk`]), so an edit
    /// changes the chunk holding the entry and every other chunk is shared
    /// with the previous version.
    pub fn chunks(&self) -> Option<Vec<HistoryNode>> {
        let entries: Vec<_> = self.entries.iter().collect();
        let ranges = chunk_ranges(entries.iter().map(|(n, _)| n.as_str()))?;
        Some(
            ranges
                .into_iter()
                .map(|r| HistoryNode {
                    entries: entries[r]
                        .iter()
                        .map(|(n, e)| ((*n).clone(), (*e).clone()))
                        .collect(),
                })
                .collect(),
        )
    }

    /// Name of the first entry; empty for the empty node.
    pub fn first_name(&self) -> &str {
        self.entries.keys().next().map_or("", |n| n.as_str())
    }
}

/// Where [`HistoryNode::chunks`] cuts a node whose entries have these names,
/// in order, as index ranges; `None` if it is stored whole. Lets a writer
/// compare and copy only the chunks that changed.
pub fn chunk_ranges<'a>(
    names: impl ExactSizeIterator<Item = &'a str>,
) -> Option<Vec<std::ops::Range<usize>>> {
    if names.len() <= FLAT_MAX {
        return None;
    }
    Some(run_ranges(names))
}

/// [`chunk_ranges`] of a run of a split node's names that starts right after
/// a chunk boundary name (or at the first name) and ends at one (or at the
/// last name). Cuts at a boundary name do not depend on position, so such a
/// run is cut the same way inside the node as on its own, and an edit of a
/// wide node re-cuts only the runs it changes (see
/// `Repo::stage_history_edit`).
pub(crate) fn run_ranges<'a>(
    names: impl ExactSizeIterator<Item = &'a str>,
) -> Vec<std::ops::Range<usize>> {
    let len = names.len();
    let mut ranges = Vec::new();
    let mut start = 0;
    for (i, name) in names.enumerate() {
        if i == len - 1 || i + 1 - start == CHUNK_MAX || ends_chunk(name) {
            ranges.push(start..i + 1);
            start = i + 1;
        }
    }
    ranges
}

/// Whether a chunk ends after `name`: about one name in 32.
pub(crate) fn ends_chunk(name: &str) -> bool {
    Hash::of_object(BOUNDARY_DOMAIN, name.as_bytes()).0[0] & 0x1f == 0
}

/// Whether a patch costing `patch` bytes is stored instead of a keyframe
/// costing `whole` (see `history_pack::cost`).
///
/// A patch holds every change since its keyframe, so it grows by about `d`
/// bytes per version until a new keyframe of `T` bytes starts the next run.
/// Ending runs at a patch of `L` bytes costs about `L / 2 + d * T / L` bytes
/// per version, least at `L = sqrt(2 * d * T)` (ADR 0037).
pub fn worth_patching(patch: usize, whole: usize) -> bool {
    patch * patch <= PATCH_BUDGET * whole
}

/// What to store for a changed node or chunk list: an existing row (the
/// keyframe, when the content came back to it), or a new one, with the
/// keyframe a patch is on.
pub enum Row {
    Keep(NodeRef),
    New(StoredHistoryNode, Option<Arc<StoredHistoryNode>>),
}

/// The row storing the whole node or chunk `node`, written by `writer`,
/// given `key`, the keyframe (address and row) its predecessor was stored
/// against, if any.
///
/// Successive versions of a node differ in a few entries, but a keyframe
/// repeats all of them. So while every name of `key` is still in `node`
/// (names are never removed by derivation) and the entries that differ from
/// it are small enough ([`worth_patching`]), the row is a `Patch` holding
/// only those entries. Patches always point at a keyframe, never at another
/// patch, so a read costs at most two rows, and the keyframe is shared by
/// every patch on it. The row is a function of `(writer, key, node)` only,
/// so derivation stays deterministic, and `node` equal to the keyframe gives
/// the keyframe itself.
pub fn flat_row(
    writer: ChangesetId,
    node: &HistoryNode,
    key: Option<(NodeRef, &Arc<StoredHistoryNode>)>,
) -> Row {
    let whole = || Row::New(StoredHistoryNode::Flat(node.clone()), None);
    let Some((base, key_row)) = key else {
        return whole();
    };
    let StoredHistoryNode::Flat(key) = &**key_row else {
        return whole();
    };
    if key == node {
        return Row::Keep(base);
    }
    if !key.entries.keys().all(|n| node.entries.contains_key(n)) {
        return whole();
    }
    let over = HistoryNode {
        entries: node
            .entries
            .iter()
            .filter(|(n, e)| key.entries.get(*n) != Some(e))
            .map(|(n, e)| (n.clone(), e.clone()))
            .collect(),
    };
    let patch = StoredHistoryNode::Patch { base, over };
    let p = cost(writer, &patch, Some(key_row));
    // Every entry costs at least three bytes (a name of two and a header),
    // so a small patch is chosen without costing the whole node.
    if worth_patching(p, 2 + 3 * node.entries.len())
        || worth_patching(
            p,
            cost(writer, &StoredHistoryNode::Flat(node.clone()), None),
        )
    {
        Row::New(patch, Some(key_row.clone()))
    } else {
        Row::New(StoredHistoryNode::Flat(node.clone()), None)
    }
}

/// The row storing the chunk list `refs`, given the keyframe list its
/// predecessor was stored against: a `SplitPatch` of the refs that changed or
/// went away while that is small, as for [`flat_row`].
pub fn split_row(
    writer: ChangesetId,
    refs: Vec<ChunkRef>,
    key: Option<(NodeRef, &Arc<StoredHistoryNode>)>,
) -> Row {
    let Some((base, key_row)) = key else {
        return Row::New(StoredHistoryNode::Split(refs), None);
    };
    let StoredHistoryNode::Split(key) = &**key_row else {
        return Row::New(StoredHistoryNode::Split(refs), None);
    };
    if *key == refs {
        return Row::Keep(base);
    }
    let new: BTreeSet<&str> = refs.iter().map(|r| r.first.as_str()).collect();
    let old: BTreeMap<&str, NodeRef> = key.iter().map(|r| (r.first.as_str(), r.node)).collect();
    let mut changes: BTreeMap<String, Option<SplitChange>> = key
        .iter()
        .filter(|r| !new.contains(r.first.as_str()))
        .map(|r| (r.first.clone(), None))
        .collect();
    for (i, r) in refs.iter().enumerate() {
        if old.get(r.first.as_str()) != Some(&r.node) {
            let next = refs.get(i + 1).map(|n| n.first.clone());
            changes.insert(r.first.clone(), Some(SplitChange { node: r.node, next }));
        }
    }
    let n = refs.len();
    let whole = StoredHistoryNode::Split(refs);
    let patch = StoredHistoryNode::SplitPatch {
        base,
        refs: changes,
    };
    let p = cost(writer, &patch, Some(key_row));
    // Every chunk costs at least five bytes: a name of two and a reference.
    if worth_patching(p, 2 + 5 * n) || worth_patching(p, cost(writer, &whole, None)) {
        Row::New(patch, Some(key_row.clone()))
    } else {
        Row::New(whole, None)
    }
}

/// One stored node. A node with more than [`FLAT_MAX`] entries is split: a
/// list of chunks, each a whole or patched node holding a run of its entries
/// in name order. One level is enough: chunks average 32 entries, so the
/// list stays near 1/32 of the node. `Flat` and `Split` are keyframes;
/// `Patch` and `SplitPatch` are edits of a keyframe of the matching shape
/// (see [`flat_row`]). Bytes are in `history_pack` and FORMATS.md.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StoredHistoryNode {
    Flat(HistoryNode),
    Split(Vec<ChunkRef>),
    /// The `Flat` row `base` with the entries of `over` added or replaced.
    Patch {
        base: NodeRef,
        over: HistoryNode,
    },
    /// The `Split` row `base` with the refs named in `refs` set (`Some`) or
    /// removed (`None`), keyed by their first name.
    SplitPatch {
        base: NodeRef,
        refs: BTreeMap<String, Option<SplitChange>>,
    },
}

/// A chunk a [`StoredHistoryNode::SplitPatch`] sets: its row, and the first
/// name of the chunk after it in the patched list (`None` if it is last).
/// The successor is not stored; it is filled in when the patch is read.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SplitChange {
    pub node: NodeRef,
    pub next: Option<String>,
}

/// A chunk of a split node: the name of its first entry and its row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChunkRef {
    pub first: String,
    pub node: NodeRef,
}

impl StoredHistoryNode {
    /// The keyframe a patch is stored against.
    pub fn base(&self) -> Option<NodeRef> {
        match self {
            StoredHistoryNode::Flat(_) | StoredHistoryNode::Split(_) => None,
            StoredHistoryNode::Patch { base, .. } | StoredHistoryNode::SplitPatch { base, .. } => {
                Some(*base)
            }
        }
    }

    /// This row as a keyframe: a patch applied to `key`, the row at its
    /// `base`. `Err` says why the two do not fit together.
    pub fn resolve(self, key: Option<&StoredHistoryNode>) -> std::result::Result<Self, String> {
        match (self, key) {
            (row @ (StoredHistoryNode::Flat(_) | StoredHistoryNode::Split(_)), None) => Ok(row),
            (StoredHistoryNode::Patch { over, .. }, Some(StoredHistoryNode::Flat(key))) => {
                let mut node = key.clone();
                node.entries.extend(over.entries);
                Ok(StoredHistoryNode::Flat(node))
            }
            (StoredHistoryNode::SplitPatch { refs, .. }, Some(StoredHistoryNode::Split(key)))
                if key.windows(2).all(|w| w[0].first < w[1].first) =>
            {
                // Both in name order: merge them instead of building a map.
                let mut all: Vec<ChunkRef> = Vec::with_capacity(key.len() + refs.len());
                let mut set = Vec::new();
                let mut k = key.iter().peekable();
                for (first, change) in &refs {
                    while let Some(r) = k.next_if(|r| r.first < *first) {
                        all.push(r.clone());
                    }
                    let present = k.next_if(|r| r.first == *first).is_some();
                    match change {
                        Some(c) => {
                            set.push((all.len(), c.next.as_deref()));
                            all.push(ChunkRef {
                                first: first.clone(),
                                node: c.node,
                            });
                        }
                        None if !present => {
                            return Err(format!("removes {first:?}, which its base lacks"))
                        }
                        None => {}
                    }
                }
                all.extend(k.cloned());
                if all.is_empty() {
                    return Err("leaves no chunks".into());
                }
                for (i, next) in set {
                    if next != all.get(i + 1).map(|n| n.first.as_str()) {
                        return Err(format!("names the wrong chunk after {:?}", all[i].first));
                    }
                }
                Ok(StoredHistoryNode::Split(all))
            }
            (StoredHistoryNode::SplitPatch { .. }, Some(StoredHistoryNode::Split(_))) => {
                Err("is on a list whose names are out of order".into())
            }
            (_, Some(_)) => Err("is a patch on a row of another shape".into()),
            (_, None) => Err("is a patch without its base".into()),
        }
    }
}

/// A row as readers use it: always `Flat` or `Split`, with any patch applied,
/// and the address of the keyframe it was stored against (its own if it is
/// one), which the next version is patched against.
#[derive(Debug)]
pub struct ResolvedRow {
    pub node: StoredHistoryNode,
    pub keyframe: NodeRef,
}

/// History tree root of a changeset. Value: part 0 of the changeset's pack
/// (`history_pack`), which names the root node.
pub struct HistoryTree;

#[async_trait]
impl DerivedKind for HistoryTree {
    const NAME: &'static str = "history-tree-v4";

    async fn derive(
        repo: &Repo,
        id: ChangesetId,
        cs: &Changeset,
        parents: &[Bytes],
    ) -> Result<Bytes> {
        let mut pack = PackBuilder::new(id);
        let root = Self::derive_into(repo, id, cs, parents, &mut pack).await?;
        repo.write_history_pack(pack.finish(root)).await
    }

    const STAGED: bool = true;

    const KEEP_SKIPLIST_READS: bool = true;

    async fn derive_staged(
        repo: &Repo,
        id: ChangesetId,
        cs: &Changeset,
        parents: &[Bytes],
        rows: &mut Vec<DerivedRow>,
    ) -> Result<Bytes> {
        let mut pack = PackBuilder::new(id);
        let root = Self::derive_into(repo, id, cs, parents, &mut pack).await?;
        Ok(repo.stage_history_pack(pack.finish(root), rows))
    }

    fn unstage(repo: &Repo, rows: &[DerivedRow]) {
        repo.unstage_history_rows(rows);
    }

    /// Root trees of the window and of its first parents, which each
    /// derivation compares, and the parents and generations merges walk
    /// (`merge_base`, ancestry checks), which would otherwise be read a
    /// batch per walk.
    async fn prepare(repo: &Repo, window: &[(ChangesetId, &Changeset)]) {
        let mut ids: Vec<ChangesetId> = window.iter().map(|(id, _)| *id).collect();
        for (id, cs) in window {
            repo.remember_parents(*id, cs.parents.as_slice().into());
            ids.extend(&cs.parents);
        }
        ids.sort();
        ids.dedup();
        repo.prefetch_generations(&ids).await;
        // `merge_base` at each merge reads its parents' last merges; the
        // window's are derived from the changesets in hand. Errors are left
        // for `merge_base` to meet and report.
        if window.iter().any(|(_, cs)| cs.parents.len() > 1) {
            let _ = super::LastMerge::derive_window(repo, window).await;
        }
        ids.retain(|id| {
            window
                .iter()
                .any(|(w, cs)| w == id || cs.parents.first() == Some(id))
        });
        repo.prefetch_root_trees(&ids).await;
    }
}

impl HistoryTree {
    /// The history root of `id`, adding the nodes it stores to `pending`.
    async fn derive_into(
        repo: &Repo,
        id: ChangesetId,
        cs: &Changeset,
        parents: &[Bytes],
        pending: &mut PackBuilder,
    ) -> Result<NodeRef> {
        let heads = cs
            .parents
            .iter()
            .zip(parents)
            .map(|(p, v)| decode_root(*p, v))
            .collect::<Result<Vec<_>>>()?;
        let base = heads.first().copied().unwrap_or(NodeRef::EMPTY);

        // At a two-parent merge, the history at the merge base settles most
        // paths without ancestry checks; see `inherit`. Any common ancestor
        // would be correct, but only the best one is recent enough to match
        // the parents' entries for paths one side never touched.
        let common = match cs.parents.as_slice() {
            [a, b] => match repo.merge_base(*a, *b).await? {
                Some(c) => Some((
                    c,
                    decode_root(c, &super::derive::<HistoryTree>(repo, c).await?)?,
                )),
                None => None,
            },
            _ => None,
        };

        // Paths whose entry may differ from the first parent's: what this
        // changeset changed, plus, at a merge, wherever another parent's
        // history differs. With a merge base, only paths the second parent
        // changed since it can: where its entry equals the base's, `inherit`
        // gives the first parent's set, and an equal directory entry means an
        // equal subtree. On Linux, mainline moves thousands of paths while a
        // branch is out, and comparing against the first parent visited them
        // all at every merge.
        let mut touched: BTreeSet<String> =
            cs.changes.keys().map(|p| p.as_str().to_string()).collect();
        match common {
            Some((_, c)) => diff_history(repo, c, heads[1], &mut touched).await?,
            None => {
                for &other in heads.iter().skip(1) {
                    diff_history(repo, base, other, &mut touched).await?;
                }
            }
        }
        if touched.is_empty() {
            return Ok(base);
        }
        let mut paths = BTreeSet::new();
        for p in &touched {
            for (i, _) in p.match_indices('/') {
                paths.insert(p[..i].to_string());
            }
            paths.insert(p.clone());
        }

        let old_tree = match cs.parents.first() {
            Some(p) => repo.root_tree_prefetched(*p).await?,
            None => TreeNode::empty_hash(),
        };
        let mut old = Cursor::new(repo, old_tree);
        let mut new = Cursor::new(repo, repo.root_tree_prefetched(id).await?);
        let dirs: BTreeSet<&str> = paths
            .iter()
            .map(|p| p.rsplit_once('/').map_or("", |(d, _)| d))
            .collect();
        futures_util::try_join!(old.preload(&dirs), new.preload(&dirs))?;
        // Every parent's directories are only looked up in: a rebuilt
        // directory is edited chunk by chunk (`Repo::stage_history_edit`).
        let mut first_hist = heads.first().map(|h| HistCursor::new(repo, *h));
        let mut other_hist: Vec<HistCursor> = heads
            .iter()
            .skip(1)
            .map(|h| HistCursor::new(repo, *h))
            .collect();
        if let Some(h) = &mut first_hist {
            h.preload(&paths).await;
        }
        for h in &mut other_hist {
            h.preload(&paths).await;
        }
        let mut merge = MergeAncestry::new(repo, &cs.parents, common).await?;
        if let Some(Some(c)) = merge.bases.get_mut(&(0, 1)) {
            c.history.preload(&paths).await;
        }
        let mut kids: HashMap<&str, Vec<&str>> = HashMap::new();
        for p in &paths {
            kids.entry(p.rsplit_once('/').map_or("", |(d, _)| d))
                .or_default()
                .push(p);
        }
        let mut deepest_first: Vec<&String> = paths.iter().collect();
        deepest_first.sort_by_key(|p| std::cmp::Reverse(p.matches('/').count()));

        let mut built: HashMap<&str, HistoryEntry> = HashMap::new();
        for p in deepest_first {
            let changed = if old.entry(p).await? != new.entry(p).await? {
                vec![id]
            } else {
                // Not changed here, so everything that last changed it lies
                // behind some parent.
                let mut sets = Vec::with_capacity(heads.len());
                if let Some(h) = &mut first_hist {
                    sets.push(changed_of(h.entry(p).await?));
                }
                for h in &mut other_hist {
                    sets.push(changed_of(h.entry(p).await?));
                }
                let at_common = match sets.len() {
                    2 => match merge.base(repo, 0, 1).await? {
                        Some(c) => Some(changed_of(c.history.entry(p).await?)),
                        None => None,
                    },
                    _ => None,
                };
                let inherited = at_common.and_then(|c| inherit(&sets[0], &sets[1], &c));
                let set = match inherited {
                    Some(set) => set,
                    None => merge.maximal(repo, p, &sets).await?,
                };
                #[cfg(test)]
                assert_eq!(set, maximal(repo, sets.concat()).await?, "{p}");
                set
            };
            let first = match first_hist.as_mut() {
                Some(h) => h.entry(p).await?.and_then(|e| e.node()),
                None => None,
            };
            let node = match kids.get(p.as_str()) {
                None => first,
                Some(names) => Some(rebuild(repo, first, names, &mut built, pending).await?),
            };
            built.insert(
                p.as_str(),
                match node {
                    Some(node) => HistoryEntry::Tree { changed, node },
                    None => HistoryEntry::Leaf { changed },
                },
            );
        }
        rebuild(repo, Some(base), &kids[""], &mut built, pending).await
    }
}

/// Store `first` (or an empty node) with the entries of `names` replaced by
/// their rebuilt versions, and return its hash.
async fn rebuild<'a>(
    repo: &Repo,
    first: Option<NodeRef>,
    names: &[&'a str],
    built: &mut HashMap<&'a str, HistoryEntry>,
    pending: &mut PackBuilder,
) -> Result<NodeRef> {
    let mut updates: Vec<(String, HistoryEntry)> = names
        .iter()
        .map(|path| {
            let entry = built.remove(path).expect("children are built first");
            let name = path.rsplit('/').next().expect("non-empty");
            (name.to_string(), entry)
        })
        .collect();
    updates.sort_by(|a, b| a.0.cmp(&b.0));
    repo.stage_history_edit(first, updates, pending).await
}

fn changed_of(e: Option<HistoryEntry>) -> Vec<ChangesetId> {
    e.map(|e| e.changed().to_vec()).unwrap_or_default()
}

/// The newest changes of a path at a merge of `first` and `second`, given its
/// set at `common`, a common ancestor of both parents, when that settles it.
///
/// Each set holds the maximal changes among a changeset's ancestors. If the
/// first parent's set equals the common ancestor's, every change behind the
/// first parent is behind `common` and so behind the second parent, where its
/// set already covers it: the answer is the second parent's set. The same
/// holds the other way round. Only paths changed on both sides since `common`
/// need [`maximal`], whose ancestry checks walk history on merge-heavy graphs.
fn inherit(
    first: &[ChangesetId],
    second: &[ChangesetId],
    common: &[ChangesetId],
) -> Option<Vec<ChangesetId>> {
    if first == common {
        Some(second.to_vec())
    } else if second == common {
        Some(first.to_vec())
    } else {
        None
    }
}

/// `maximal` of the parents' sets at a path, when [`inherit`] does not
/// settle it. Most ancestry questions are answered from the history at the
/// parents' merge bases and their generations; the rest share one walk per
/// parent instead of walking history once per pair of ids and path, which
/// made a Linux merge take seconds and its twelve-parent merge minutes.
struct MergeAncestry<'a> {
    parents: Vec<ChangesetId>,
    /// Ancestors of each parent, walked as deep as questions needed.
    walks: Vec<Option<Ancestors>>,
    /// The merge base of each pair of parents, `None` inside for unrelated
    /// ones.
    bases: HashMap<(usize, usize), Option<MergeBase<'a>>>,
}

/// The merge base of two parents: its rank and its history.
struct MergeBase<'a> {
    key: (u64, ChangesetId),
    history: HistCursor<'a>,
    /// Whether the two parents provably have no common ancestor outside the
    /// merge base's ancestors, once asked (see [`MergeAncestry::behind`]).
    sole: Option<bool>,
}

/// Changesets [`Repo::side_is_clear`] may walk per side before giving up.
const SIDE_BUDGET: usize = 10_000;

impl<'a> MergeAncestry<'a> {
    /// `first_two`: at a two-parent merge, the parents' merge base and the
    /// history root there, already found (`None` for unrelated parents).
    async fn new(
        repo: &'a Repo,
        parents: &[ChangesetId],
        first_two: Option<(ChangesetId, NodeRef)>,
    ) -> Result<Self> {
        let mut bases = HashMap::new();
        if parents.len() == 2 {
            let base = match first_two {
                Some((m, root)) => Some(MergeBase::new(repo, m, root).await?),
                None => None,
            };
            bases.insert((0, 1), base);
        }
        Ok(MergeAncestry {
            parents: parents.to_vec(),
            walks: parents.iter().map(|_| None).collect(),
            bases,
        })
    }

    /// An id of a parent's set is dropped if it lies strictly behind an id of
    /// another parent's set; `sets[j]` holds the maximal changes behind
    /// parent `j`, so that is exactly when it lies behind parent `j` and is
    /// not in `sets[j]` (see [`MergeAncestry::behind`]).
    async fn maximal(
        &mut self,
        repo: &'a Repo,
        path: &str,
        sets: &[Vec<ChangesetId>],
    ) -> Result<Vec<ChangesetId>> {
        let mut all: Vec<ChangesetId> = sets.concat();
        all.sort();
        all.dedup();
        let mut keep = Vec::with_capacity(all.len());
        'next: for x in all {
            let i = sets
                .iter()
                .position(|s| s.contains(&x))
                .expect("x comes from a set");
            for (j, set) in sets.iter().enumerate() {
                if !set.is_empty() && !set.contains(&x) && self.behind(repo, path, x, i, j).await? {
                    continue 'next;
                }
            }
            keep.push(x);
        }
        Ok(keep)
    }

    /// Whether `x`, a change in parent `i`'s set, lies behind parent `j`.
    ///
    /// `sets[j]` covers every change behind parent `j`, so if `x` lies
    /// behind it and is not in `sets[j]`, it lies strictly behind an id of
    /// `sets[j]`: no check against that set's ids is needed. With `m` the
    /// merge base of the two parents, whose set covers every change behind
    /// it and whose changes behind parent `i` are covered by `sets[i]`:
    ///
    /// - `x` in `m`'s set lies behind `m`, so behind parent `j`.
    /// - Otherwise `x` is not behind `m` (it would be in `m`'s set, being
    ///   maximal behind parent `i`), so it is behind parent `j` only as a
    ///   common ancestor outside `m`'s ancestors (criss-cross merges). Such
    ///   a common ancestor ranks below `m`, the best one, by (generation,
    ///   id); and there is none if one parent's ancestors outside `m`'s
    ///   that rank below `m` are all behind `m` (a branch forked from `m`,
    ///   however old). Only then is parent `j`'s walk asked, which is the
    ///   walk an old-based merge of a small branch used to pay for: from
    ///   the tip down to the oldest such `x`.
    async fn behind(
        &mut self,
        repo: &'a Repo,
        path: &str,
        x: ChangesetId,
        i: usize,
        j: usize,
    ) -> Result<bool> {
        let (lo, hi) = (i.min(j), i.max(j));
        let parents = (self.parents[lo], self.parents[hi]);
        let Some(base) = self.base(repo, lo, hi).await? else {
            // Unrelated parents share no ancestor.
            return Ok(false);
        };
        if changed_of(base.history.entry(path).await?).contains(&x) {
            return Ok(true);
        }
        let gx = repo.generation(x).await?;
        if (gx, x) > base.key {
            return Ok(false);
        }
        let sole = match base.sole {
            Some(s) => s,
            None => {
                let (gm, m) = base.key;
                let (ga, gb) = (
                    repo.generation(parents.0).await?,
                    repo.generation(parents.1).await?,
                );
                // The side closer to the merge base first: usually the
                // branch, a few changesets.
                let sides = if gb - gm <= ga - gm {
                    [parents.1, parents.0]
                } else {
                    [parents.0, parents.1]
                };
                let mut s = false;
                for tip in sides {
                    if repo.side_is_clear(tip, (gm, m), SIDE_BUDGET).await? {
                        s = true;
                        break;
                    }
                }
                base.sole = Some(s);
                s
            }
        };
        if sole {
            return Ok(false);
        }
        let walk = match &mut self.walks[j] {
            Some(w) => w,
            slot => {
                let tip = self.parents[j];
                slot.insert(Ancestors::new(tip, repo.generation(tip).await?))
            }
        };
        walk.contains(repo, x, gx).await
    }

    /// The merge base of parents `a` and `b`.
    async fn base(
        &mut self,
        repo: &'a Repo,
        a: usize,
        b: usize,
    ) -> Result<Option<&mut MergeBase<'a>>> {
        if !self.bases.contains_key(&(a, b)) {
            let base = match repo.merge_base(self.parents[a], self.parents[b]).await? {
                Some(m) => {
                    let root = decode_root(m, &super::derive::<HistoryTree>(repo, m).await?)?;
                    Some(MergeBase::new(repo, m, root).await?)
                }
                None => None,
            };
            self.bases.insert((a, b), base);
        }
        Ok(self.bases.get_mut(&(a, b)).and_then(|c| c.as_mut()))
    }
}

impl<'a> MergeBase<'a> {
    async fn new(repo: &'a Repo, m: ChangesetId, root: NodeRef) -> Result<Self> {
        Ok(MergeBase {
            key: (repo.generation(m).await?, m),
            history: HistCursor::new(repo, root),
            sole: None,
        })
    }
}

/// Drop ids that are ancestors of other ids in the set. Sorted by id. The
/// reference `MergeAncestry` is checked against in tests.
#[cfg(test)]
async fn maximal(repo: &Repo, mut ids: Vec<ChangesetId>) -> Result<Vec<ChangesetId>> {
    ids.sort();
    ids.dedup();
    if ids.len() < 2 {
        return Ok(ids);
    }
    let mut keep = Vec::new();
    'next: for &a in &ids {
        for &b in &ids {
            if a != b && repo.is_ancestor(a, b).await? {
                continue 'next;
            }
        }
        keep.push(a);
    }
    Ok(keep)
}

/// Add to `out` every path whose history entry differs between two history
/// trees, skipping equal subtrees by hash.
///
/// A level of the trees at a time, with the rows of a level's directories
/// read in one batch: the merge base's rows are old and mostly not cached.
async fn diff_history(
    repo: &Repo,
    a: NodeRef,
    b: NodeRef,
    out: &mut BTreeSet<String>,
) -> Result<()> {
    let mut level = vec![(String::new(), a, b)];
    while !level.is_empty() {
        level.retain(|(_, a, b)| a != b);
        repo.prefetch_history_rows(level.iter().flat_map(|(_, a, b)| [*a, *b]))
            .await;
        let mut next = Vec::new();
        for (dir, a, b) in level {
            for (name, ea, eb) in repo.diff_history_nodes(a, b).await? {
                let path = join(&dir, &name);
                out.insert(path.clone());
                let (ca, cb) = (ea.and_then(|e| e.node()), eb.and_then(|e| e.node()));
                if ca != cb {
                    let empty = NodeRef::EMPTY;
                    next.push((path, ca.unwrap_or(empty), cb.unwrap_or(empty)));
                }
            }
        }
        level = next;
    }
    Ok(())
}

/// Tree node reads [`Cursor::preload`] keeps in flight.
const PRELOAD_READS: usize = 16;

/// Path lookups in one tree that view each directory node once, so many
/// lookups along shared spines cost one read per directory.
struct Cursor<'a> {
    repo: &'a Repo,
    root: Hash,
    dirs: HashMap<String, Option<Arc<TreeView>>>,
}

impl<'a> Cursor<'a> {
    fn new(repo: &'a Repo, root: Hash) -> Self {
        Cursor {
            repo,
            root,
            dirs: HashMap::new(),
        }
    }

    /// Entry at the non-empty `path`.
    async fn entry(&mut self, path: &str) -> Result<Option<TreeEntry>> {
        let (dir, name) = path.rsplit_once('/').unwrap_or(("", path));
        self.load(dir).await?;
        match &self.dirs[dir] {
            Some(n) => n.get(name),
            None => Ok(None),
        }
    }

    /// Load the directories `dirs`, whose ancestors are all among them, a
    /// level at a time with the reads of a level in flight together: one
    /// store round trip per level of the tree instead of one per directory.
    async fn preload(&mut self, dirs: &BTreeSet<&str>) -> Result<()> {
        self.load("").await?;
        let mut levels: BTreeMap<usize, Vec<&str>> = BTreeMap::new();
        for d in dirs.iter().filter(|d| !d.is_empty()) {
            levels.entry(d.matches('/').count()).or_default().push(d);
        }
        for level in levels.into_values() {
            let mut want = Vec::new();
            for dir in level {
                if self.dirs.contains_key(dir) {
                    continue;
                }
                let (parent, name) = dir.rsplit_once('/').unwrap_or(("", dir));
                if !self.dirs.contains_key(parent) {
                    // Not closed under ancestors: leave it to `load`.
                    continue;
                }
                let entry = match &self.dirs[parent] {
                    Some(n) => n.get(name)?,
                    None => None,
                };
                match entry {
                    Some(TreeEntry::Dir { tree }) => want.push((dir, tree)),
                    _ => {
                        self.dirs.insert(dir.to_string(), None);
                    }
                }
            }
            for group in want.chunks(PRELOAD_READS) {
                let mut reads = Vec::with_capacity(group.len());
                for (_, tree) in group {
                    reads.push(self.repo.read_tree_view(*tree));
                }
                let views = futures_util::future::join_all(reads).await;
                for ((dir, _), view) in group.iter().zip(views) {
                    self.dirs.insert(dir.to_string(), Some(view?));
                }
            }
        }
        Ok(())
    }

    async fn load(&mut self, dir: &str) -> Result<()> {
        if self.dirs.contains_key(dir) {
            return Ok(());
        }
        if !self.dirs.contains_key("") {
            let root = self.repo.read_tree_view(self.root).await?;
            self.dirs.insert(String::new(), Some(root));
        }
        let mut cur = String::new();
        for comp in dir.split('/').filter(|c| !c.is_empty()) {
            let next = join(&cur, comp);
            if !self.dirs.contains_key(&next) {
                let entry = match &self.dirs[&cur] {
                    Some(n) => n.get(comp)?,
                    None => None,
                };
                let child = match entry {
                    Some(TreeEntry::Dir { tree }) => Some(self.repo.read_tree_view(tree).await?),
                    _ => None,
                };
                self.dirs.insert(next.clone(), child);
            }
            cur = next;
        }
        Ok(())
    }
}

/// Path lookups in one history tree, like [`Cursor`], that read only the
/// chunk holding each name instead of joining every chunk of a directory:
/// at a merge, the other parents' and the merge bases' trees are only
/// looked up in, never rebuilt.
struct HistCursor<'a> {
    repo: &'a Repo,
    root: NodeRef,
    dirs: HashMap<String, Option<NodeLookup>>,
}

impl<'a> HistCursor<'a> {
    fn new(repo: &'a Repo, root: NodeRef) -> Self {
        HistCursor {
            repo,
            root,
            dirs: HashMap::new(),
        }
    }

    /// Entry at the non-empty `path`.
    async fn entry(&mut self, path: &str) -> Result<Option<HistoryEntry>> {
        let (dir, name) = path.rsplit_once('/').unwrap_or(("", path));
        self.load(dir).await?;
        match &self.dirs[dir] {
            Some(l) => self.repo.lookup_child(l, name).await,
            None => Ok(None),
        }
    }

    /// Read the rows that looking up `paths` (closed under ancestors) will
    /// need, a level of the tree at a time and a batch per step, instead of
    /// a row at a time: an older tree's rows (another parent's, the merge
    /// base's) are mostly not cached. Errors are left for the lookups to
    /// meet.
    async fn preload(&mut self, paths: &BTreeSet<String>) {
        if self.load("").await.is_err() {
            return;
        }
        let mut levels: BTreeMap<usize, Vec<&str>> = BTreeMap::new();
        for p in paths {
            levels.entry(p.matches('/').count()).or_default().push(p);
        }
        for level in levels.into_values() {
            // The chunks holding each name, then the nodes of the
            // directories among them.
            let mut want = Vec::new();
            for path in &level {
                let (dir, name) = path.rsplit_once('/').unwrap_or(("", path));
                if let Some(Some(l)) = self.dirs.get(dir) {
                    want.extend(self.repo.lookup_rows(l, name));
                }
            }
            self.repo.prefetch_history_rows(want.into_iter()).await;
            let mut kids = Vec::new();
            for path in &level {
                if self.dirs.contains_key(*path) {
                    continue;
                }
                let (dir, name) = path.rsplit_once('/').unwrap_or(("", path));
                let Some(Some(l)) = self.dirs.get(dir) else {
                    continue;
                };
                if let Ok(Some(HistoryEntry::Tree { node, .. })) =
                    self.repo.lookup_child(l, name).await
                {
                    kids.push((*path, node));
                }
            }
            self.repo
                .prefetch_history_rows(kids.iter().map(|(_, h)| *h))
                .await;
            for (path, node) in kids {
                if let Ok(l) = self.repo.node_lookup(node).await {
                    self.dirs.insert(path.to_string(), Some(l));
                }
            }
        }
    }

    async fn load(&mut self, dir: &str) -> Result<()> {
        if self.dirs.contains_key(dir) {
            return Ok(());
        }
        if !self.dirs.contains_key("") {
            let root = self.repo.node_lookup(self.root).await?;
            self.dirs.insert(String::new(), Some(root));
        }
        let mut cur = String::new();
        for comp in dir.split('/').filter(|c| !c.is_empty()) {
            let next = join(&cur, comp);
            if !self.dirs.contains_key(&next) {
                let child = match &self.dirs[&cur] {
                    Some(l) => self
                        .repo
                        .lookup_child(l, comp)
                        .await?
                        .and_then(|e| e.node()),
                    None => None,
                };
                let node = match child {
                    Some(h) => Some(self.repo.node_lookup(h).await?),
                    None => None,
                };
                self.dirs.insert(next.clone(), node);
            }
            cur = next;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
