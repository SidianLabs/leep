//! Path-history index (ADR 0013): a tree parallel to `root_tree` that records,
//! for every path that ever existed, which changesets last changed it.
//!
//! The value per changeset is the hash of the root [`HistoryNode`], stored
//! content-addressed under [`NODE_KIND`], so a changeset shares every node it
//! did not touch with its first parent. Derivation reads only the spines of
//! changed paths, like `root_tree`.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use async_trait::async_trait;
use bytes::Bytes;
use leep_core::{Changeset, ChangesetId, Hash, Result, TreeEntry, TreeNode};
use serde::{Deserialize, Serialize};

use super::{decode_hash, DerivedKind};
use crate::tree_ops::join;
use crate::Repo;

/// Meta-store kind holding history nodes, keyed by [`HistoryNode::hash`].
pub const NODE_KIND: &str = "history-node-v1";

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
    pub fn encode(&self) -> Vec<u8> {
        postcard::to_stdvec(self).expect("history node encoding cannot fail")
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        Ok(postcard::from_bytes(bytes)?)
    }

    pub fn hash(&self) -> Hash {
        Hash::of_object(NODE_KIND, &self.encode())
    }

    pub fn empty_hash() -> Hash {
        HistoryNode::default().hash()
    }
}

/// History tree root of a changeset. Value: 32 bytes, the root node's hash.
pub struct HistoryTree;

#[async_trait]
impl DerivedKind for HistoryTree {
    const NAME: &'static str = "history-tree-v1";

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
                let mut all = Vec::new();
                for h in &mut hist {
                    if let Some(e) = h.entry(p).await? {
                        all.extend_from_slice(e.changed());
                    }
                }
                maximal(repo, all).await?
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
    repo.put_history_node(&node).await
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

    /// Golden bytes: changing them orphans every stored history node. A new
    /// encoding needs a new kind name (`history-node-v2`).
    #[test]
    fn node_encoding_is_stable() {
        let node = HistoryNode {
            entries: BTreeMap::from([
                (
                    "a".to_string(),
                    HistoryEntry::Leaf {
                        changed: vec![id(1)],
                    },
                ),
                (
                    "d".to_string(),
                    HistoryEntry::Tree {
                        changed: vec![id(1), id(2)],
                        node: Hash([3; 32]),
                    },
                ),
            ]),
        };
        let mut want = vec![2]; // two entries
        want.extend([1, b'a', 0, 1]); // name, Leaf (variant 0), one id
        want.extend([1; 32]);
        want.extend([1, b'd', 1, 2]); // name, Tree (variant 1), two ids
        want.extend([1; 32]);
        want.extend([2; 32]);
        want.extend([3; 32]);
        assert_eq!(node.encode(), want);
        assert_eq!(HistoryNode::decode(&want).unwrap(), node);
        assert_eq!(node.hash(), Hash::of_object("history-node-v1", &want));
        assert_eq!(HistoryNode::default().encode(), [0]);
        assert_eq!(HistoryTree::NAME, "history-tree-v1");
        assert_eq!(NODE_KIND, "history-node-v1");
    }
}
