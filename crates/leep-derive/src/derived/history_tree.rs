//! Path-history index (ADR 0013): a tree parallel to `root_tree` that records,
//! for every path that ever existed, which changesets last changed it.
//!
//! The value per changeset is the hash of the root [`HistoryNode`], stored
//! content-addressed under [`NODE_KIND`], so a changeset shares every node it
//! did not touch with its first parent. Large nodes are stored in chunks
//! (ADR 0027), so a change to one entry of a wide directory rewrites one chunk,
//! not the whole node, and a changed node or chunk is stored as a patch on an
//! earlier version (ADR 0037), so it costs the entries that changed, not all
//! of them. Derivation reads only the spines of changed paths, like
//! `root_tree`.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::OnceLock;

use async_trait::async_trait;
use bytes::Bytes;
use leep_core::{Changeset, ChangesetId, Hash, Result, TreeEntry, TreeNode};
use serde::{Deserialize, Serialize};

use super::{decode_hash, DerivedKind};
use crate::tree_ops::join;
use crate::Repo;

/// Meta-store kind holding stored history nodes and chunks, keyed by the hash
/// of their [`StoredHistoryNode`] bytes. The label is the only versioned name:
/// `-v1` rows held whole nodes and `-v2` rows had no patches, and an older
/// server may still read them.
pub const NODE_KIND: &str = "history-node-v3";

/// Nodes with at most this many entries are stored whole.
pub const FLAT_MAX: usize = 64;
/// Most entries in one chunk, so a run of names without a boundary cannot
/// make a chunk as large as the node it splits.
pub const CHUNK_MAX: usize = 128;
/// A changed row is stored as a patch on its predecessor's keyframe while
/// `patch² <= PATCH_BUDGET * whole` (bytes); past that a new keyframe starts.
/// See [`worth_patching`].
pub const PATCH_BUDGET: usize = 128;
/// Domain of the hash that picks chunk boundaries from entry names.
const BOUNDARY_DOMAIN: &str = "history-chunk-boundary";

/// One directory of the history tree. Unlike a `TreeNode` it keeps entries for
/// deleted paths, because the log of a deleted path must still find the
/// deleting changeset.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoryNode {
    pub entries: BTreeMap<String, HistoryEntry>,
}

/// What is known about one path component. `changed` is the set of newest
/// changesets that changed the path: the maximal elements, under ancestry, of
/// all changesets whose entry for the path differs from their first parent's.
/// It is one id unless merged branches changed the path concurrently, sorted
/// by id so equal sets encode equally. For a directory this covers any change
/// beneath it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum HistoryEntry {
    /// Only ever a file, or a directory with no history below it.
    Leaf { changed: Vec<ChangesetId> },
    /// Has been a directory: `node` holds the history of its children,
    /// including deleted ones. A path that changed kind keeps both lineages in
    /// `changed`.
    Tree {
        changed: Vec<ChangesetId>,
        node: Hash,
    },
}

impl HistoryEntry {
    pub fn changed(&self) -> &[ChangesetId] {
        match self {
            HistoryEntry::Leaf { changed } | HistoryEntry::Tree { changed, .. } => changed,
        }
    }

    pub fn node(&self) -> Option<Hash> {
        match self {
            HistoryEntry::Leaf { .. } => None,
            HistoryEntry::Tree { node, .. } => Some(*node),
        }
    }
}

impl HistoryNode {
    /// Postcard of the logical node; the payload of [`StoredHistoryNode::Flat`].
    pub fn encode(&self) -> Vec<u8> {
        postcard::to_stdvec(self).expect("history node encoding cannot fail")
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        Ok(postcard::from_bytes(bytes)?)
    }

    /// Address of the node stored without a predecessor: the hash of its top
    /// keyframe row. A node stored as an edit of another may have a patch as
    /// its top row and so another address (see [`flat_row`]).
    pub fn hash(&self) -> Hash {
        self.rows().pop().expect("a node has a top row").hash
    }

    pub fn empty_hash() -> Hash {
        static EMPTY: OnceLock<Hash> = OnceLock::new();
        *EMPTY.get_or_init(|| HistoryNode::default().hash())
    }

    /// The node cut into chunks, or `None` if it is stored whole. Chunk
    /// boundaries depend only on entry names (see [`ends_chunk`]), so an edit
    /// changes the chunk holding the entry and every other chunk is shared
    /// with the previous version.
    pub fn chunks(&self) -> Option<Vec<HistoryNode>> {
        if self.entries.len() <= FLAT_MAX {
            return None;
        }
        let mut chunks = Vec::new();
        let mut chunk = HistoryNode::default();
        let last = self.entries.len() - 1;
        for (i, (name, entry)) in self.entries.iter().enumerate() {
            chunk.entries.insert(name.clone(), entry.clone());
            if i == last || chunk.entries.len() == CHUNK_MAX || ends_chunk(name) {
                chunks.push(std::mem::take(&mut chunk));
            }
        }
        Some(chunks)
    }

    /// The rows that store this node with no predecessor, all keyframes: its
    /// chunks, if split, then the top row.
    pub fn rows(&self) -> Vec<HistoryRow> {
        let Some(chunks) = self.chunks() else {
            return vec![HistoryRow::new(StoredHistoryNode::Flat(self.clone()))];
        };
        let mut rows = Vec::new();
        let mut refs = Vec::new();
        for chunk in chunks {
            let first = chunk.first_name().to_string();
            let row = HistoryRow::new(StoredHistoryNode::Flat(chunk));
            refs.push(ChunkRef {
                first,
                node: row.hash,
            });
            rows.push(row);
        }
        rows.push(HistoryRow::new(StoredHistoryNode::Split(refs)));
        rows
    }

    /// Name of the first entry; empty for the empty node.
    pub fn first_name(&self) -> &str {
        self.entries.keys().next().map_or("", |n| n.as_str())
    }
}

/// Whether a chunk ends after `name`: about one name in 32.
fn ends_chunk(name: &str) -> bool {
    Hash::of_object(BOUNDARY_DOMAIN, name.as_bytes()).0[0] & 0x1f == 0
}

/// Whether a patch of `patch` bytes is stored instead of a keyframe of
/// `whole` bytes.
///
/// A patch holds every change since its keyframe, so it grows by about `d`
/// bytes (one entry, about 64) per version until a new keyframe of `T` bytes
/// starts the next run. Ending runs at a patch of `L` bytes costs about
/// `L / 2 + d * T / L` bytes per version, least at `L = sqrt(2 * d * T)`: a
/// quarter of a 2 KB chunk, but half of a 500-byte chunk list, which a fixed
/// fraction would keyframe after one or two changes. On git/git's first
/// 3,795 commits this stores 7% fewer bytes than a fixed quarter.
pub fn worth_patching(patch: usize, whole: usize) -> bool {
    patch * patch <= PATCH_BUDGET * whole
}

/// The row storing the whole node or chunk `node`, given `key`, the keyframe
/// (address and entries) its predecessor was stored against, if any.
///
/// Successive versions of a node differ in a few entries, but a keyframe
/// repeats all of them, changeset ids included (over half of v2's bytes). So
/// while every name of `key` is still in `node` (names are never removed by
/// derivation) and the entries that differ from it are small enough
/// ([`worth_patching`]), the row is a `Patch` holding only those entries.
/// Patches always point at a keyframe, never at another patch, so a read
/// costs at most two rows, and the keyframe is shared by every patch on it.
/// The row is a function of `(key, node)` only, so derivation stays
/// deterministic, and `node` equal to the keyframe gives the keyframe itself.
pub fn flat_row(node: &HistoryNode, key: Option<(Hash, &HistoryNode)>) -> HistoryRow {
    let whole = HistoryRow::new(StoredHistoryNode::Flat(node.clone()));
    let Some((base, key)) = key else {
        return whole;
    };
    if key == node {
        return whole;
    }
    if !key.entries.keys().all(|n| node.entries.contains_key(n)) {
        return whole;
    }
    let over = HistoryNode {
        entries: node
            .entries
            .iter()
            .filter(|(n, e)| key.entries.get(*n) != Some(e))
            .map(|(n, e)| (n.clone(), e.clone()))
            .collect(),
    };
    let patch = HistoryRow::new(StoredHistoryNode::Patch { base, over });
    if worth_patching(patch.bytes.len(), whole.bytes.len()) {
        patch
    } else {
        whole
    }
}

/// The row storing the chunk list `refs`, given the keyframe list its
/// predecessor was stored against: a `SplitPatch` of the refs that changed or
/// went away while that is small, as for [`flat_row`]. Each changed ref also
/// names the first name of the chunk after it, so a lookup that lands in a
/// changed chunk's range needs no read of the keyframe list.
pub fn split_row(refs: Vec<ChunkRef>, key: Option<(Hash, &[ChunkRef])>) -> HistoryRow {
    let Some((base, key)) = key else {
        return HistoryRow::new(StoredHistoryNode::Split(refs));
    };
    if key == refs.as_slice() {
        return HistoryRow::new(StoredHistoryNode::Split(refs));
    }
    let new: BTreeSet<&str> = refs.iter().map(|r| r.first.as_str()).collect();
    let old: BTreeMap<&str, Hash> = key.iter().map(|r| (r.first.as_str(), r.node)).collect();
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
    let whole = HistoryRow::new(StoredHistoryNode::Split(refs));
    let patch = HistoryRow::new(StoredHistoryNode::SplitPatch {
        base,
        refs: changes,
    });
    if worth_patching(patch.bytes.len(), whole.bytes.len()) {
        patch
    } else {
        whole
    }
}

/// One `history-node-v3` row. A node with more than [`FLAT_MAX`] entries is
/// split: a list of chunks, each a whole or patched row holding a run of its
/// entries in name order. One level is enough: chunks average 32 entries, so
/// the list stays near 1/32 of the node (about 1.3 KB per 1,000 entries).
/// `Flat` and `Split` are keyframes; `Patch` and `SplitPatch` are edits of a
/// keyframe of the matching shape (see [`flat_row`]).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum StoredHistoryNode {
    Flat(HistoryNode),
    Split(Vec<ChunkRef>),
    /// The `Flat` row `base` with the entries of `over` added or replaced.
    Patch {
        base: Hash,
        over: HistoryNode,
    },
    /// The `Split` row `base` with the refs named in `refs` set (`Some`) or
    /// removed (`None`), keyed by their first name.
    SplitPatch {
        base: Hash,
        refs: BTreeMap<String, Option<SplitChange>>,
    },
}

/// A chunk a [`StoredHistoryNode::SplitPatch`] sets: its row, and the first
/// name of the chunk after it in the patched list (`None` if it is last).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SplitChange {
    pub node: Hash,
    pub next: Option<String>,
}

/// A chunk of a split node: the name of its first entry and its row.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkRef {
    pub first: String,
    pub node: Hash,
}

impl StoredHistoryNode {
    pub fn encode(&self) -> Vec<u8> {
        postcard::to_stdvec(self).expect("history node encoding cannot fail")
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        Ok(postcard::from_bytes(bytes)?)
    }

    /// The keyframe a patch is stored against.
    pub fn base(&self) -> Option<Hash> {
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
            (StoredHistoryNode::SplitPatch { refs, .. }, Some(StoredHistoryNode::Split(key))) => {
                let mut all: BTreeMap<String, Hash> =
                    key.iter().map(|r| (r.first.clone(), r.node)).collect();
                for (first, change) in &refs {
                    match change {
                        Some(c) => {
                            all.insert(first.clone(), c.node);
                        }
                        None => {
                            if all.remove(first).is_none() {
                                return Err(format!("removes {first:?}, which its base lacks"));
                            }
                        }
                    }
                }
                if all.is_empty() {
                    return Err("leaves no chunks".into());
                }
                let all: Vec<ChunkRef> = all
                    .into_iter()
                    .map(|(first, node)| ChunkRef { first, node })
                    .collect();
                // Lookups trust `next` without reading the base, so it must
                // be the patched list's own successor.
                for (i, r) in all.iter().enumerate() {
                    if let Some(Some(c)) = refs.get(&r.first) {
                        if c.next.as_deref() != all.get(i + 1).map(|n| n.first.as_str()) {
                            return Err(format!("names the wrong chunk after {:?}", r.first));
                        }
                    }
                }
                Ok(StoredHistoryNode::Split(all))
            }
            (_, Some(_)) => Err("is a patch on a row of another shape".into()),
            (_, None) => Err("is a patch without its base".into()),
        }
    }
}

/// A stored row with its address and bytes.
pub struct HistoryRow {
    pub hash: Hash,
    pub bytes: Vec<u8>,
    pub node: StoredHistoryNode,
}

impl HistoryRow {
    fn new(node: StoredHistoryNode) -> Self {
        let bytes = node.encode();
        HistoryRow {
            hash: Hash::of_object(NODE_KIND, &bytes),
            bytes,
            node,
        }
    }
}

/// A row as readers use it: always `Flat` or `Split`, with any patch applied,
/// and the address of the keyframe it was stored against (its own if it is
/// one), which the next version is patched against.
#[derive(Debug)]
pub struct ResolvedRow {
    pub node: StoredHistoryNode,
    pub keyframe: Hash,
}

/// History tree root of a changeset. Value: 32 bytes, the root node's hash.
pub struct HistoryTree;

#[async_trait]
impl DerivedKind for HistoryTree {
    const NAME: &'static str = "history-tree-v3";

    async fn derive(
        repo: &Repo,
        id: ChangesetId,
        cs: &Changeset,
        parents: &[Bytes],
    ) -> Result<Bytes> {
        let heads = parents
            .iter()
            .map(|p| decode_hash(p))
            .collect::<Result<Vec<_>>>()?;
        let base = heads
            .first()
            .copied()
            .unwrap_or_else(HistoryNode::empty_hash);

        // Paths whose entry may differ from the first parent's: what this
        // changeset changed, plus, at a merge, wherever another parent's
        // history differs from the first parent's.
        let mut touched: BTreeSet<String> =
            cs.changes.keys().map(|p| p.as_str().to_string()).collect();
        for &other in heads.iter().skip(1) {
            diff_history(repo, base, other, &mut touched).await?;
        }
        if touched.is_empty() {
            return Ok(Bytes::copy_from_slice(base.as_bytes()));
        }
        let mut paths = BTreeSet::new();
        for p in &touched {
            for (i, _) in p.match_indices('/') {
                paths.insert(p[..i].to_string());
            }
            paths.insert(p.clone());
        }

        let old_tree = match cs.parents.first() {
            Some(p) => repo.root_tree(*p).await?,
            None => TreeNode::empty_hash(),
        };
        let mut old = Cursor::<TreeNode>::new(repo, old_tree);
        let mut new = Cursor::<TreeNode>::new(repo, repo.root_tree(id).await?);
        let mut hist: Vec<Cursor<HistoryNode>> =
            heads.iter().map(|h| Cursor::new(repo, *h)).collect();
        // At a two-parent merge, the history at the merge base settles most
        // paths without ancestry checks; see `inherit`. Any common ancestor
        // would be correct, but only the best one is recent enough to match
        // the parents' entries for paths one side never touched.
        let mut common = match cs.parents.as_slice() {
            [a, b] => match repo.merge_base(*a, *b).await? {
                Some(c) => Some(Cursor::<HistoryNode>::new(
                    repo,
                    decode_hash(&super::derive::<HistoryTree>(repo, c).await?)?,
                )),
                None => None,
            },
            _ => None,
        };

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
                let mut sets = Vec::with_capacity(hist.len());
                for h in &mut hist {
                    sets.push(changed_of(h.entry(p).await?));
                }
                match &mut common {
                    Some(c) => {
                        let at_common = changed_of(c.entry(p).await?);
                        match inherit(&sets[0], &sets[1], &at_common) {
                            Some(set) => {
                                #[cfg(test)]
                                assert_eq!(set, maximal(repo, sets.concat()).await?, "{p}");
                                set
                            }
                            None => maximal(repo, sets.concat()).await?,
                        }
                    }
                    None => maximal(repo, sets.concat()).await?,
                }
            };
            let first = match hist.first_mut() {
                Some(h) => h.entry(p).await?.and_then(|e| e.node()),
                None => None,
            };
            let node = match kids.get(p.as_str()) {
                None => first,
                Some(names) => Some(rebuild(repo, first, names, &mut built).await?),
            };
            built.insert(
                p.as_str(),
                match node {
                    Some(node) => HistoryEntry::Tree { changed, node },
                    None => HistoryEntry::Leaf { changed },
                },
            );
        }
        let root = rebuild(repo, Some(base), &kids[""], &mut built).await?;
        Ok(Bytes::copy_from_slice(root.as_bytes()))
    }
}

/// Store `first` (or an empty node) with the entries of `names` replaced by
/// their rebuilt versions, and return its hash.
async fn rebuild<'a>(
    repo: &Repo,
    first: Option<Hash>,
    names: &[&'a str],
    built: &mut HashMap<&'a str, HistoryEntry>,
) -> Result<Hash> {
    let mut node = match first {
        Some(h) => repo.read_history_node(h).await?,
        None => HistoryNode::default(),
    };
    for path in names {
        let entry = built.remove(path).expect("children are built first");
        let name = path.rsplit('/').next().expect("non-empty");
        node.entries.insert(name.to_string(), entry);
    }
    repo.put_history_node(&node, first).await
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

/// Drop ids that are ancestors of other ids in the set. Sorted by id.
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
async fn diff_history(repo: &Repo, a: Hash, b: Hash, out: &mut BTreeSet<String>) -> Result<()> {
    let mut stack = vec![(String::new(), a, b)];
    while let Some((dir, a, b)) = stack.pop() {
        if a == b {
            continue;
        }
        let (na, nb) = (
            repo.read_history_node(a).await?,
            repo.read_history_node(b).await?,
        );
        let names: BTreeSet<&String> = na.entries.keys().chain(nb.entries.keys()).collect();
        for name in names {
            let (ea, eb) = (na.entries.get(name), nb.entries.get(name));
            if ea == eb {
                continue;
            }
            let path = join(&dir, name);
            out.insert(path.clone());
            let (ca, cb) = (ea.and_then(|e| e.node()), eb.and_then(|e| e.node()));
            if ca != cb {
                let empty = HistoryNode::empty_hash();
                stack.push((path, ca.unwrap_or(empty), cb.unwrap_or(empty)));
            }
        }
    }
    Ok(())
}

/// A directory-shaped node that [`Cursor`] can walk.
#[async_trait]
trait Layer: Sized + Send + Sync {
    type Entry: Clone + Send + Sync;

    async fn load(repo: &Repo, node: Hash) -> Result<Self>;
    fn get(&self, name: &str) -> Option<&Self::Entry>;
    fn child(entry: &Self::Entry) -> Option<Hash>;
}

#[async_trait]
impl Layer for TreeNode {
    type Entry = TreeEntry;

    async fn load(repo: &Repo, node: Hash) -> Result<Self> {
        repo.read_tree(node).await
    }

    fn get(&self, name: &str) -> Option<&TreeEntry> {
        self.entries.get(name)
    }

    fn child(entry: &TreeEntry) -> Option<Hash> {
        match entry {
            TreeEntry::Dir { tree } => Some(*tree),
            TreeEntry::File { .. } => None,
        }
    }
}

#[async_trait]
impl Layer for HistoryNode {
    type Entry = HistoryEntry;

    async fn load(repo: &Repo, node: Hash) -> Result<Self> {
        repo.read_history_node(node).await
    }

    fn get(&self, name: &str) -> Option<&HistoryEntry> {
        self.entries.get(name)
    }

    fn child(entry: &HistoryEntry) -> Option<Hash> {
        entry.node()
    }
}

/// Path lookups in one tree that load each directory node once, so many
/// lookups along shared spines cost one read per directory.
struct Cursor<'a, N: Layer> {
    repo: &'a Repo,
    root: Hash,
    dirs: HashMap<String, Option<N>>,
}

impl<'a, N: Layer> Cursor<'a, N> {
    fn new(repo: &'a Repo, root: Hash) -> Self {
        Cursor {
            repo,
            root,
            dirs: HashMap::new(),
        }
    }

    /// Entry at the non-empty `path`.
    async fn entry(&mut self, path: &str) -> Result<Option<N::Entry>> {
        let (dir, name) = path.rsplit_once('/').unwrap_or(("", path));
        self.load(dir).await?;
        Ok(self.dirs[dir].as_ref().and_then(|n| n.get(name)).cloned())
    }

    async fn load(&mut self, dir: &str) -> Result<()> {
        if self.dirs.contains_key(dir) {
            return Ok(());
        }
        if !self.dirs.contains_key("") {
            let root = N::load(self.repo, self.root).await?;
            self.dirs.insert(String::new(), Some(root));
        }
        let mut cur = String::new();
        for comp in dir.split('/').filter(|c| !c.is_empty()) {
            let next = join(&cur, comp);
            if !self.dirs.contains_key(&next) {
                let child = self.dirs[&cur]
                    .as_ref()
                    .and_then(|n| n.get(comp))
                    .and_then(N::child);
                let node = match child {
                    Some(h) => Some(N::load(self.repo, h).await?),
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
mod tests {
    use super::*;

    fn id(b: u8) -> ChangesetId {
        ChangesetId(Hash([b; 32]))
    }

    fn leaf(b: u8) -> HistoryEntry {
        HistoryEntry::Leaf {
            changed: vec![id(b)],
        }
    }

    fn wide(n: usize) -> HistoryNode {
        HistoryNode {
            entries: (0..n).map(|i| (format!("f{i:04}"), leaf(1))).collect(),
        }
    }

    /// Golden bytes: changing them orphans every stored history node. A new
    /// encoding needs a new kind name (`history-node-v4`).
    #[test]
    fn node_encoding_is_stable() {
        let node = HistoryNode {
            entries: BTreeMap::from([
                ("a".to_string(), leaf(1)),
                (
                    "d".to_string(),
                    HistoryEntry::Tree {
                        changed: vec![id(1), id(2)],
                        node: Hash([3; 32]),
                    },
                ),
            ]),
        };
        let mut logical = vec![2]; // two entries
        logical.extend([1, b'a', 0, 1]); // name, Leaf (variant 0), one id
        logical.extend([1; 32]);
        logical.extend([1, b'd', 1, 2]); // name, Tree (variant 1), two ids
        logical.extend([1; 32]);
        logical.extend([2; 32]);
        logical.extend([3; 32]);
        assert_eq!(node.encode(), logical);
        assert_eq!(HistoryNode::decode(&logical).unwrap(), node);

        // A small node is one row: Flat (variant 0), then the node.
        let mut want = vec![0];
        want.extend(&logical);
        let rows = node.rows();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].bytes, want);
        assert_eq!(StoredHistoryNode::decode(&want).unwrap(), rows[0].node);
        assert_eq!(node.hash(), Hash::of_object("history-node-v3", &want));
        assert_eq!(
            HistoryNode::empty_hash(),
            Hash::of_object("history-node-v3", &[0, 0])
        );

        // Split (variant 1): the chunk count, then each chunk's first name
        // and address.
        let split = StoredHistoryNode::Split(vec![
            ChunkRef {
                first: "a".into(),
                node: Hash([4; 32]),
            },
            ChunkRef {
                first: "m".into(),
                node: Hash([5; 32]),
            },
        ]);
        let mut want = vec![1, 2, 1, b'a'];
        want.extend([4; 32]);
        want.extend([1, b'm']);
        want.extend([5; 32]);
        assert_eq!(split.encode(), want);
        assert_eq!(StoredHistoryNode::decode(&want).unwrap(), split);

        // Patch (variant 2): the keyframe's address, then the entries that
        // differ from it, encoded as a node.
        let patch = StoredHistoryNode::Patch {
            base: Hash([6; 32]),
            over: HistoryNode {
                entries: BTreeMap::from([("a".to_string(), leaf(1))]),
            },
        };
        let mut want = vec![2];
        want.extend([6; 32]);
        want.extend([1, 1, b'a', 0, 1]);
        want.extend([1; 32]);
        assert_eq!(patch.encode(), want);
        assert_eq!(StoredHistoryNode::decode(&want).unwrap(), patch);

        // SplitPatch (variant 3): the keyframe list's address, then changes
        // by first name, each an option: 0 removed, or 1 then the chunk's
        // address and the next chunk's first name (an option too).
        let patch = StoredHistoryNode::SplitPatch {
            base: Hash([7; 32]),
            refs: BTreeMap::from([
                (
                    "a".to_string(),
                    Some(SplitChange {
                        node: Hash([8; 32]),
                        next: Some("q".into()),
                    }),
                ),
                ("m".to_string(), None),
            ]),
        };
        let mut want = vec![3];
        want.extend([7; 32]);
        want.extend([2, 1, b'a', 1]);
        want.extend([8; 32]);
        want.extend([1, 1, b'q']);
        want.extend([1, b'm', 0]);
        assert_eq!(patch.encode(), want);
        assert_eq!(StoredHistoryNode::decode(&want).unwrap(), patch);

        assert_eq!(HistoryTree::NAME, "history-tree-v3");
        assert_eq!(NODE_KIND, "history-node-v3");
        assert_eq!(PATCH_BUDGET, 128);
        assert!(worth_patching(400, 1250) && !worth_patching(401, 1250));
    }

    /// When a patch is chosen over a keyframe is part of the format: it
    /// decides which rows, and so which addresses, an edit produces.
    #[test]
    fn patches_are_chosen_while_small_and_name_their_keyframe() {
        let key = wide(40);
        let key_hash = key.hash();
        let mut node = key.clone();
        for i in 0..5 {
            node.entries.insert(format!("f{i:04}"), leaf(2));
        }
        let row = flat_row(&node, Some((key_hash, &key)));
        let StoredHistoryNode::Patch { base, over } = &row.node else {
            panic!("five of forty entries changed: a patch");
        };
        assert_eq!(*base, key_hash);
        assert_eq!(over.entries.len(), 5);
        assert_eq!(row.hash, Hash::of_object(NODE_KIND, &row.bytes));
        let resolved = row
            .node
            .clone()
            .resolve(Some(&StoredHistoryNode::Flat(key.clone())));
        assert_eq!(resolved, Ok(StoredHistoryNode::Flat(node.clone())));

        // Past the budget a new keyframe starts.
        for i in 5..15 {
            node.entries.insert(format!("f{i:04}"), leaf(2));
        }
        let row = flat_row(&node, Some((key_hash, &key)));
        assert_eq!(row.node, StoredHistoryNode::Flat(node.clone()));
        assert_eq!(row.hash, node.hash());

        // Unchanged: the keyframe itself. A name of the keyframe missing from
        // the node cannot be expressed as a patch.
        assert_eq!(flat_row(&key, Some((key_hash, &key))).hash, key_hash);
        let mut fewer = key.clone();
        fewer.entries.remove("f0001");
        assert!(matches!(
            flat_row(&fewer, Some((key_hash, &key))).node,
            StoredHistoryNode::Flat(_)
        ));

        // Lists: a changed ref and a removed one.
        let refs: Vec<ChunkRef> = (0..40)
            .map(|i| ChunkRef {
                first: format!("n{i:02}"),
                node: Hash([i; 32]),
            })
            .collect();
        let list = Hash([9; 32]);
        let mut edited = refs.clone();
        edited[3].node = Hash([99; 32]);
        edited.remove(7);
        let row = split_row(edited.clone(), Some((list, &refs)));
        let change = |node: u8, next: &str| {
            Some(SplitChange {
                node: Hash([node; 32]),
                next: Some(next.to_string()),
            })
        };
        let want = StoredHistoryNode::SplitPatch {
            base: list,
            refs: BTreeMap::from([
                ("n03".to_string(), change(99, "n04")),
                ("n07".to_string(), None),
            ]),
        };
        assert_eq!(row.node, want);
        assert_eq!(
            want.resolve(Some(&StoredHistoryNode::Split(refs.clone()))),
            Ok(StoredHistoryNode::Split(edited))
        );
        // A change must name its true successor: lookups rely on it.
        let lying = StoredHistoryNode::SplitPatch {
            base: list,
            refs: BTreeMap::from([("n03".to_string(), change(99, "n05"))]),
        };
        assert!(lying
            .resolve(Some(&StoredHistoryNode::Split(refs.clone())))
            .is_err());
        assert_eq!(
            split_row(refs.clone(), Some((list, &refs))).node,
            StoredHistoryNode::Split(refs)
        );
    }

    #[test]
    fn patches_only_resolve_against_a_keyframe_of_their_shape() {
        let flat = StoredHistoryNode::Flat(wide(3));
        let split = StoredHistoryNode::Split(vec![ChunkRef {
            first: "a".into(),
            node: Hash([1; 32]),
        }]);
        let patch = StoredHistoryNode::Patch {
            base: Hash([2; 32]),
            over: wide(1),
        };
        let split_patch = StoredHistoryNode::SplitPatch {
            base: Hash([3; 32]),
            refs: BTreeMap::from([("a".to_string(), None)]),
        };
        assert!(patch.clone().resolve(None).is_err());
        assert!(patch.clone().resolve(Some(&split)).is_err());
        assert!(patch.clone().resolve(Some(&patch)).is_err());
        assert!(split_patch.clone().resolve(Some(&flat)).is_err());
        assert!(
            split_patch.clone().resolve(Some(&split)).is_err(),
            "removing the only chunk leaves none"
        );
        assert!(flat.clone().resolve(Some(&flat)).is_err());
        assert_eq!(flat.clone().resolve(None), Ok(flat));
    }

    /// Chunk boundaries are part of the format: a different choice gives the
    /// same logical node another hash.
    #[test]
    fn chunk_boundaries_are_stable() {
        let ends: Vec<usize> = (0..1000)
            .filter(|i| ends_chunk(&format!("f{i:04}")))
            .take(6)
            .collect();
        assert_eq!(ends, [29, 40, 76, 142, 152, 159]);
        let rows = wide(1000).rows();
        let StoredHistoryNode::Split(refs) = &rows.last().unwrap().node else {
            panic!("a 1,000-entry node is split");
        };
        assert_eq!(refs.len(), rows.len() - 1);
        assert_eq!(refs.len(), 33);
    }

    #[test]
    fn small_nodes_are_flat_and_large_ones_split_within_bounds() {
        assert_eq!(wide(FLAT_MAX).rows().len(), 1);
        for n in [FLAT_MAX + 1, 300, 1000, 5000] {
            let node = wide(n);
            let rows = node.rows();
            let StoredHistoryNode::Split(refs) = &rows.last().unwrap().node else {
                panic!("{n} entries must split");
            };
            let mut joined = HistoryNode::default();
            for (r, row) in refs.iter().zip(&rows) {
                let StoredHistoryNode::Flat(chunk) = &row.node else {
                    panic!("chunks are flat");
                };
                assert!(!chunk.entries.is_empty() && chunk.entries.len() <= CHUNK_MAX);
                assert_eq!(chunk.entries.keys().next(), Some(&r.first));
                assert_eq!(r.node, row.hash);
                joined.entries.extend(chunk.entries.clone());
            }
            assert_eq!(joined, node);
        }
        // A long run of names without a boundary is cut at the cap.
        let names: Vec<String> = (0..)
            .map(|i| format!("n{i}"))
            .filter(|n| !ends_chunk(n))
            .take(3 * CHUNK_MAX)
            .collect();
        let node = HistoryNode {
            entries: names.into_iter().map(|n| (n, leaf(1))).collect(),
        };
        assert_eq!(node.rows().len(), 3 + 1);
    }

    /// Inserting a name replaces at most two chunks (the one it lands in, cut
    /// in two if the name is a boundary); every other chunk is shared.
    #[test]
    fn an_insert_changes_at_most_two_chunks() {
        let base = wide(1000);
        let before: BTreeSet<Hash> = base.rows().iter().map(|r| r.hash).collect();
        for extra in ["f0000a", "f0500x", "f0999z", "a", "zzz", "f0123_"] {
            let mut node = base.clone();
            node.entries.insert(extra.to_string(), leaf(2));
            let rows = node.rows();
            let (_, chunks) = rows.split_last().unwrap();
            let new = chunks.iter().filter(|r| !before.contains(&r.hash)).count();
            assert!((1..=2).contains(&new), "{extra}: {new} new chunks");
        }
    }

    async fn rows_of(meta: &dyn leep_core::MetaStore) -> usize {
        meta.list_derived(NODE_KIND, None, usize::MAX)
            .await
            .unwrap()
            .len()
    }

    #[tokio::test]
    async fn a_split_node_round_trips_and_an_edit_writes_one_chunk() {
        use leep_core::memory::{MemoryBlobStore, MemoryMetaStore};
        use leep_core::MetaStore;
        use std::sync::Arc;
        let meta = Arc::new(MemoryMetaStore::new());
        let repo = Repo::new(Arc::new(MemoryBlobStore::new()), meta.clone());
        let node = wide(1000);
        let h = repo.put_history_node(&node, None).await.unwrap();
        assert_eq!(h, node.hash());
        let stored = rows_of(&*meta).await;
        assert_eq!(stored, node.rows().len());
        assert_eq!(repo.read_history_node(h).await.unwrap(), node);

        let mut edited = node.clone();
        edited.entries.insert("f0500".into(), leaf(9));
        let h2 = repo.put_history_node(&edited, Some(h)).await.unwrap();
        assert_eq!(rows_of(&*meta).await, stored + 2, "one chunk and the list");
        // Read back without the cache, as another server would.
        let cold = Repo::new(Arc::new(MemoryBlobStore::new()), meta.clone());
        assert_eq!(cold.read_history_node(h2).await.unwrap(), edited);
        assert_eq!(cold.read_history_node(h).await.unwrap(), node);
        for name in ["f0000", "f0500", "f0999", "f0501"] {
            assert_eq!(
                cold.history_child(h2, name).await.unwrap(),
                edited.entries.get(name).cloned()
            );
        }
        for name in ["", "a", "f05", "zzz"] {
            assert_eq!(cold.history_child(h2, name).await.unwrap(), None);
        }
        // Both new rows are patches on the keyframes they replace.
        let StoredHistoryNode::SplitPatch { base, refs } =
            StoredHistoryNode::decode(&meta.get_derived(NODE_KIND, &h2).await.unwrap().unwrap())
                .unwrap()
        else {
            panic!("an edit of one chunk is a list patch");
        };
        assert_eq!(base, h);
        assert_eq!(refs.len(), 1);
        let chunk = refs.values().next().unwrap().as_ref().unwrap().node;
        let row =
            StoredHistoryNode::decode(&meta.get_derived(NODE_KIND, &chunk).await.unwrap().unwrap())
                .unwrap();
        assert!(
            matches!(row, StoredHistoryNode::Patch { ref over, .. } if over.entries.len() == 1)
        );

        // A lookup of the changed name reads the list patch and the chunk
        // patch only: with both keyframes gone it still answers, and any other
        // name reports the damage instead of a wrong answer.
        let StoredHistoryNode::Patch {
            base: chunk_key, ..
        } = row
        else {
            unreachable!()
        };
        let mut rows = meta
            .list_derived(NODE_KIND, None, usize::MAX)
            .await
            .unwrap();
        rows.retain(|(k, _)| *k != h && *k != chunk_key);
        meta.delete_derived_kind(NODE_KIND).await.unwrap();
        for (k, v) in rows {
            meta.put_derived(NODE_KIND, &k, v).await.unwrap();
        }
        let cold = Repo::new(Arc::new(MemoryBlobStore::new()), meta.clone());
        assert_eq!(
            cold.history_child(h2, "f0500").await.unwrap(),
            Some(leaf(9))
        );
        for name in ["f0501", "f0000"] {
            let e = cold.history_child(h2, name).await.unwrap_err().to_string();
            assert!(e.contains("missing"), "{name}: {e}");
        }
    }

    /// A long run of edits, each stored against the previous version: every
    /// version reads back from a cold cache, patches always point at a
    /// keyframe, keyframes restart as patches grow, and the bytes are a small
    /// part of what whole rows would take.
    #[tokio::test]
    async fn a_chain_of_edits_stays_two_reads_deep_and_small() {
        use leep_core::memory::{MemoryBlobStore, MemoryMetaStore};
        use leep_core::MetaStore;
        use std::sync::Arc;
        let meta = Arc::new(MemoryMetaStore::new());
        let repo = Repo::new(Arc::new(MemoryBlobStore::new()), meta.clone());
        let mut node = wide(300);
        let mut h = repo.put_history_node(&node, None).await.unwrap();
        let mut versions = vec![(h, node.clone())];
        let mut whole = 0;
        for step in 0..200u32 {
            let name = match step % 3 {
                0 => format!("f{:04}", (step * 37) % 300),
                1 => format!("g{step:04}"),
                _ => "f0150".to_string(),
            };
            node.entries.insert(name, leaf((step % 250) as u8));
            whole += node.rows().iter().map(|r| r.bytes.len()).sum::<usize>();
            h = repo.put_history_node(&node, Some(h)).await.unwrap();
            versions.push((h, node.clone()));
        }
        let rows = meta
            .list_derived(NODE_KIND, None, usize::MAX)
            .await
            .unwrap();
        let mut patches = 0;
        let mut stored = 0;
        for (key, value) in &rows {
            stored += value.len();
            let row = StoredHistoryNode::decode(value).unwrap();
            if let Some(base) = row.base() {
                patches += 1;
                let key_row = rows
                    .iter()
                    .find(|(k, _)| *k == base)
                    .expect("keyframe stored");
                let key_row = StoredHistoryNode::decode(&key_row.1).unwrap();
                assert!(key_row.base().is_none(), "{key}: patch on a patch");
            }
        }
        assert!(patches > rows.len() / 2, "{patches} of {}", rows.len());
        assert!(
            stored * 3 < whole,
            "stored {stored} bytes against {whole} for whole rows"
        );
        let cold = Repo::new(Arc::new(MemoryBlobStore::new()), meta.clone());
        for (h, node) in &versions {
            assert_eq!(&cold.read_history_node(*h).await.unwrap(), node);
        }
    }

    #[tokio::test]
    async fn a_patch_on_a_patch_is_corrupt() {
        use leep_core::memory::{MemoryBlobStore, MemoryMetaStore};
        use leep_core::MetaStore;
        use std::sync::Arc;
        let meta = Arc::new(MemoryMetaStore::new());
        let repo = Repo::new(Arc::new(MemoryBlobStore::new()), meta.clone());
        let key = repo.put_history_node(&wide(10), None).await.unwrap();
        let put = |row: StoredHistoryNode| {
            let bytes = row.encode();
            let h = Hash::of_object(NODE_KIND, &bytes);
            let meta = meta.clone();
            async move {
                meta.put_derived(NODE_KIND, &h, Bytes::from(bytes))
                    .await
                    .unwrap();
                h
            }
        };
        let first = put(StoredHistoryNode::Patch {
            base: key,
            over: HistoryNode {
                entries: BTreeMap::from([("x".to_string(), leaf(2))]),
            },
        })
        .await;
        let second = put(StoredHistoryNode::Patch {
            base: first,
            over: HistoryNode {
                entries: BTreeMap::from([("y".to_string(), leaf(3))]),
            },
        })
        .await;
        assert_eq!(
            repo.read_history_node(first).await.unwrap().entries.len(),
            11
        );
        for repo in [
            repo.clone(),
            Repo::new(Arc::new(MemoryBlobStore::new()), meta.clone()),
        ] {
            let e = repo
                .read_history_node(second)
                .await
                .unwrap_err()
                .to_string();
            assert!(e.contains("corrupt history node"), "{e}");
        }
    }

    #[tokio::test]
    async fn a_chunk_that_does_not_fit_its_list_is_corrupt() {
        use leep_core::memory::{MemoryBlobStore, MemoryMetaStore};
        use leep_core::MetaStore;
        use std::sync::Arc;
        let meta = Arc::new(MemoryMetaStore::new());
        let repo = Repo::new(Arc::new(MemoryBlobStore::new()), meta.clone());
        let a = repo.put_history_node(&wide(10), None).await.unwrap();
        // The list names "f0005" first, but the chunk starts at "f0000".
        let bad = StoredHistoryNode::Split(vec![ChunkRef {
            first: "f0005".into(),
            node: a,
        }]);
        let bytes = bad.encode();
        let h = Hash::of_object(NODE_KIND, &bytes);
        meta.put_derived(NODE_KIND, &h, Bytes::from(bytes))
            .await
            .unwrap();
        let e = repo.read_history_node(h).await.unwrap_err().to_string();
        assert!(e.contains("corrupt history node"), "{e}");
    }
}
