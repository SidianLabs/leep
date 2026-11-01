//! Trees of created changesets that are not stored (ADR 0069).
//!
//! With `--lazy-create-trees on` a client's create builds its tree in
//! memory, as it always did, to check it and to compute the root hash its
//! `root_tree` row holds, but stores none of the nodes it rewrote: most
//! creates are landed as a rebased copy with a different tree, and their
//! own nodes were garbage written with an fsync each on the request path.
//!
//! The rule that replaces "every stored changeset's tree is stored": a
//! changeset's tree is either stored whole, or its root node is absent and
//! the tree is its first parent's tree with its `changes` applied, the
//! parent's tree being stored or itself derivable this way, at most
//! [`MAX_LAZY_CHAIN`] steps down. A root node present means every node
//! below it is present: eager writes store a tree's nodes before the
//! changeset, and [`Repo::materialize_tree`] stores the root only after the
//! rest is durable.
//!
//! Readers find a missing node here: [`Repo::read_tree`] and its siblings
//! ask [`Repo::lazy_node`] when the store lacks one. Nodes are derived on
//! demand from the changeset that holds them, found through the roots that
//! [`Repo::note_root`] recorded when a changeset's root was looked up, and
//! kept in memory, never stored by a read: an unlanded changeset is read
//! rarely and a few directory rebuilds are cheap, while a store write would
//! turn reads into fsyncs and write garbage again. A derivation is checked
//! against every `root_tree` row it passes, so a wrong tree is never served.

use std::collections::HashMap;
use std::hash::Hash as StdHash;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use leep_core::{Changeset, ChangesetId, Error, Hash, Result, TreeNode};

use crate::repo::{CacheScale, SizedCache};
use crate::tree_ops::{build_tree, Reads};
use crate::Repo;

/// The longest run of changesets with unstored trees a create starts: a
/// create whose parent's tree is this many derivations from a stored one
/// stores its own tree. Bounds what a read of any changeset derives.
pub const MAX_LAZY_DEPTH: u32 = 8;

/// The longest run a derivation follows before it gives up with
/// `Error::Storage`. Larger than [`MAX_LAZY_DEPTH`] so a history written
/// under another bound still reads; a run this long means damage.
pub const MAX_LAZY_CHAIN: usize = 64;

/// Bytes of derived nodes kept, counted like the decoded node cache (three
/// times their encoding).
pub(crate) const LAZY_NODE_CACHE_BYTES: usize = 16 << 20;

/// Entries per generation of the origin and depth maps: about 100 bytes
/// each, so a few MiB.
const LAZY_MAP_ENTRIES: usize = 1 << 15;

/// What a server knows of unstored trees, shared by a [`Repo`]'s clones.
pub(crate) struct LazyTrees {
    /// Nodes derived (or built by a create) and not stored, by hash.
    pub(crate) nodes: SizedCache<TreeNode, LAZY_NODE_CACHE_BYTES>,
    /// Tree node hash -> the changeset whose tree holds it: every root
    /// looked up, and every node a derivation built. A missing node with no
    /// origin is missing.
    origins: Mutex<TwoGen<Hash, ChangesetId>>,
    /// Changesets whose trees were derived or built unstored -> derivations
    /// from their tree down to a stored one (1 for a changeset on a stored
    /// parent's tree).
    depths: Mutex<TwoGen<ChangesetId, u32>>,
    /// Derivations made, for tests and the metric.
    pub(crate) derivations: std::sync::atomic::AtomicU64,
}

impl Default for LazyTrees {
    fn default() -> Self {
        Self::scaled(CacheScale::default())
    }
}

impl LazyTrees {
    pub(crate) fn scaled(s: CacheScale) -> Self {
        let entries = s.of(LAZY_MAP_ENTRIES).max(1024);
        LazyTrees {
            nodes: SizedCache::scaled(s),
            origins: Mutex::new(TwoGen::new(entries)),
            depths: Mutex::new(TwoGen::new(entries)),
            derivations: Default::default(),
        }
    }

    fn origin(&self, tree: &Hash) -> Option<ChangesetId> {
        self.origins.lock().unwrap().get(tree)
    }

    pub(crate) fn depth(&self, id: &ChangesetId) -> Option<u32> {
        self.depths.lock().unwrap().get(id)
    }

    /// Keep the nodes of `id`'s unstored tree, `depth` derivations from a
    /// stored one.
    pub(crate) fn remember(&self, id: ChangesetId, depth: u32, nodes: &[(Hash, Bytes, TreeNode)]) {
        {
            let mut o = self.origins.lock().unwrap();
            for (h, _, _) in nodes {
                o.insert(*h, id);
            }
        }
        self.depths.lock().unwrap().insert(id, depth);
        for (h, bytes, node) in nodes {
            self.nodes
                .insert_sized(*h, Arc::new(node.clone()), 3 * bytes.len());
        }
    }
}

/// A map bounded like the ancestry cache: two generations, the older
/// dropped when the newer fills, so recently used entries survive.
struct TwoGen<K, V> {
    new: HashMap<K, V>,
    old: HashMap<K, V>,
    limit: usize,
}

impl<K: StdHash + Eq + Copy, V: Copy> TwoGen<K, V> {
    fn new(limit: usize) -> Self {
        TwoGen {
            new: HashMap::new(),
            old: HashMap::new(),
            limit,
        }
    }

    fn get(&mut self, k: &K) -> Option<V> {
        if let Some(v) = self.new.get(k) {
            return Some(*v);
        }
        let v = self.old.remove(k)?;
        self.insert(*k, v);
        Some(v)
    }

    fn insert(&mut self, k: K, v: V) {
        if self.new.len() >= self.limit && !self.new.contains_key(&k) {
            self.old = std::mem::take(&mut self.new);
        }
        self.new.insert(k, v);
    }
}

/// A changeset's tree as a derivation rebuilt it: how many derivations it
/// is from a stored tree, and every node the derivation built, down the
/// chain.
pub(crate) struct DerivedTree {
    pub(crate) depth: u32,
    pub(crate) chain: HashMap<Hash, TreeNode>,
}

impl Repo {
    /// Record that `root` is `id`'s root tree, so a read that finds it
    /// missing knows which changeset to derive it from. Every path that
    /// reads a `root_tree` row and then reads the tree calls this (or
    /// [`Repo::root_tree`], which does).
    pub(crate) fn note_root(&self, id: ChangesetId, root: Hash) {
        if root != TreeNode::empty_hash() {
            self.lazy.origins.lock().unwrap().insert(root, id);
        }
    }

    /// [`Repo::note_root`] for many.
    pub(crate) fn note_roots<'a>(&self, roots: impl IntoIterator<Item = (ChangesetId, &'a Hash)>) {
        let empty = TreeNode::empty_hash();
        let mut o = self.lazy.origins.lock().unwrap();
        for (id, root) in roots {
            if *root != empty {
                o.insert(*root, id);
            }
        }
    }

    /// Whether creates leave their tree nodes out (ADR 0069).
    pub fn lazy_create_trees(&self) -> bool {
        self.lazy_create
    }

    /// Derivations of unstored trees this handle's process made.
    pub fn lazy_tree_derivations(&self) -> u64 {
        self.lazy
            .derivations
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// The node `tree` of an unstored tree: derived earlier, or derived now
    /// from the changeset that holds it. `None` when no changeset known to
    /// hold it was looked up: the node is missing.
    pub(crate) async fn lazy_node(&self, tree: Hash) -> Result<Option<TreeNode>> {
        if let Some(n) = self.lazy.nodes.get(&tree) {
            return Ok(Some((*n).clone()));
        }
        let Some(id) = self.lazy.origin(&tree) else {
            return Ok(None);
        };
        // Boxed: a derivation reads through `Reads::Derive`, which never
        // comes back here, but the types do not show that.
        // The node may be one of an ancestor the derivation passed (a
        // changeset that changed nothing has its parent's tree).
        let mut derived = Box::pin(self.derive_tree(id)).await?;
        Ok(derived.chain.remove(&tree))
    }

    /// Rebuild `id`'s tree from the nearest stored tree on its first-parent
    /// chain, checking each tree built against its `root_tree` row. Walks
    /// the chain iteratively, at most [`MAX_LAZY_CHAIN`] changesets.
    pub(crate) async fn derive_tree(&self, id: ChangesetId) -> Result<DerivedTree> {
        match self.derive_tree_with(id, true).await {
            // A node derived earlier, which the chain started from, left the
            // cache: derive the whole chain from stored trees instead.
            Err(Error::NotFound(_)) => self.derive_tree_with(id, false).await,
            r => r,
        }
    }

    async fn derive_tree_with(&self, id: ChangesetId, cached: bool) -> Result<DerivedTree> {
        let empty = TreeNode::empty_hash();
        // Newest first: the changesets whose trees must be built.
        let mut chain: Vec<(ChangesetId, Changeset, Hash)> = Vec::new();
        let mut cur = id;
        let mut root = self.root_tree(id).await?;
        let (mut base, mut depth) = loop {
            if chain.len() >= MAX_LAZY_CHAIN {
                return Err(Error::Storage(format!(
                    "tree of changeset {id} is missing: not stored, and no stored tree is \
                     within {MAX_LAZY_CHAIN} first parents"
                )));
            }
            let cs = self.get_changeset(cur).await?;
            let parent = cs.parents.first().copied();
            chain.push((cur, cs, root));
            let Some(p) = parent else {
                break (empty, 0);
            };
            let parent_root = self.root_tree(p).await?;
            root = parent_root;
            if parent_root == empty {
                break (empty, 0);
            }
            if cached && self.lazy.nodes.get(&parent_root).is_some() {
                if let Some(d) = self.lazy.depth(&p) {
                    break (parent_root, d);
                }
            }
            if self.read_tree_stored_opt(parent_root).await?.is_some() {
                break (parent_root, 0);
            }
            cur = p;
        };
        let started = std::time::Instant::now();
        let mut local: HashMap<Hash, TreeNode> = HashMap::new();
        let mut last = None;
        for (cid, cs, want) in chain.into_iter().rev() {
            let reads = Reads::Derive {
                local: &local,
                cached,
            };
            let built = build_tree(self, reads, base, &cs.changes).await?;
            if built.root != want {
                return Err(Error::Storage(format!(
                    "corrupt tree of changeset {cid}: it derives to {}, but its root_tree row \
                     is {want}",
                    built.root
                )));
            }
            depth += 1;
            self.lazy.remember(cid, depth, &built.nodes);
            self.lazy
                .derivations
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            metrics::counter!("leep_lazy_tree_derivations_total").increment(1);
            for (h, _, n) in &built.nodes {
                local.insert(*h, n.clone());
            }
            base = built.root;
            last = Some(depth);
        }
        metrics::histogram!("leep_lazy_tree_derive_seconds")
            .record(started.elapsed().as_secs_f64());
        Ok(DerivedTree {
            depth: last.expect("the chain holds `id`"),
            chain: local,
        })
    }

    /// How many derivations `parent`'s tree (root `base`) is from a stored
    /// tree: 0 when stored. Reads the root node, which the create that asks
    /// reads next anyway.
    pub(crate) async fn lazy_depth(&self, parent: ChangesetId, base: Hash) -> Result<u32> {
        if base == TreeNode::empty_hash() || self.read_tree_stored_opt(base).await?.is_some() {
            return Ok(0);
        }
        if self.lazy.nodes.get(&base).is_some() {
            if let Some(d) = self.lazy.depth(&parent) {
                return Ok(d);
            }
        }
        Ok(self.derive_tree(parent).await?.depth)
    }

    /// `id`'s tree nodes the store lacks, children before parents (the root
    /// last). Empty when its tree is stored (its root node present).
    pub(crate) async fn unstored_tree(
        &self,
        id: ChangesetId,
    ) -> Result<Vec<(Hash, Bytes, TreeNode)>> {
        let root = self.root_tree(id).await?;
        self.unstored_closure(vec![root], &HashMap::new()).await
    }

    /// The nodes of the unstored tree `base` that `built` (built on it)
    /// keeps: what storing `built`'s own nodes would leave out. Empty, after
    /// one read of `base`'s root, when `base` is stored, which is the usual
    /// case: a stored root has a whole tree below it.
    pub(crate) async fn unstored_below(
        &self,
        base: Hash,
        built: &crate::tree_ops::Built,
    ) -> Result<Vec<(Hash, Bytes, TreeNode)>> {
        if base == TreeNode::empty_hash() || self.read_tree_stored_opt(base).await?.is_some() {
            return Ok(Vec::new());
        }
        let own: std::collections::HashSet<Hash> = built.nodes.iter().map(|(h, _, _)| *h).collect();
        let kept: Vec<Hash> = built
            .nodes
            .iter()
            .flat_map(|(_, _, n)| n.entries.values())
            .filter_map(|e| match e {
                leep_core::TreeEntry::Dir { tree } if !own.contains(tree) => Some(*tree),
                _ => None,
            })
            .collect();
        self.unstored_closure(kept, &HashMap::new()).await
    }

    /// Every node under `roots` the store lacks (derived as reads do, or
    /// from `local`), children before parents. Stops at stored nodes: a
    /// stored node has a whole tree below it. Iterative.
    pub(crate) async fn unstored_closure(
        &self,
        roots: Vec<Hash>,
        local: &HashMap<Hash, TreeNode>,
    ) -> Result<Vec<(Hash, Bytes, TreeNode)>> {
        let empty = TreeNode::empty_hash();
        let mut out = Vec::new();
        let mut seen = std::collections::HashSet::new();
        let mut stack: Vec<(Hash, Option<(Bytes, TreeNode)>)> =
            roots.into_iter().map(|h| (h, None)).collect();
        while let Some((h, done)) = stack.pop() {
            if let Some((bytes, node)) = done {
                out.push((h, bytes, node));
                continue;
            }
            if h == empty || !seen.insert(h) || self.read_tree_stored_opt(h).await?.is_some() {
                continue;
            }
            let node = match local.get(&h) {
                Some(n) => n.clone(),
                None => self.read_tree(h).await?,
            };
            let bytes = Bytes::from(node.encode());
            let dirs: Vec<Hash> = node
                .entries
                .values()
                .filter_map(|e| match e {
                    leep_core::TreeEntry::Dir { tree } => Some(*tree),
                    _ => None,
                })
                .collect();
            stack.push((h, Some((bytes, node))));
            stack.extend(dirs.into_iter().map(|t| (t, None)));
        }
        Ok(out)
    }

    /// Store `id`'s tree if it is not stored: every node but the root, made
    /// durable, then the root, so a root present always means a whole tree.
    /// Returns whether it wrote anything.
    pub async fn materialize_tree(&self, id: ChangesetId) -> Result<bool> {
        let mut nodes: Vec<(Hash, Bytes)> = self
            .unstored_tree(id)
            .await?
            .into_iter()
            .map(|(h, b, _)| (h, b))
            .collect();
        let Some(root) = nodes.pop() else {
            return Ok(false);
        };
        if !nodes.is_empty() {
            self.put_rescuing_many(nodes, &[]).await?;
        }
        self.put_rescuing_many(vec![root], &[]).await?;
        metrics::counter!("leep_lazy_tree_materialized_total").increment(1);
        Ok(true)
    }
}
