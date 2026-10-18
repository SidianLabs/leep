//! The order to pack blobs in, and the base to compress each against
//! (ADR 0029). A file version's base is the previous content at the same
//! path; a tree node's base is the node of the same directory in the first
//! parent. Both come from one walk over every changeset, oldest generation
//! first, so a base is always listed before the objects based on it. An
//! incremental repack walks only the changesets whose objects are loose.

use std::collections::{HashMap, HashSet};

use leep_core::intern::{HashIds, PackHints, NO_ID};
use leep_core::{ChangesetId, Hash, Result, TreeEntry, TreeNode};

use crate::derived::{decode_hash, DerivedKind, RootTree};
use crate::Repo;

const PAGE: usize = 1000;

#[derive(Debug, Default)]
pub struct PackOrder {
    /// Objects in write order, each with the object to delta it against.
    pub objects: PackHints,
    pub changesets: u64,
}

/// Every stored changeset (not only those on refs), parents before children,
/// numbered: ids in `ids`, and the order and each one's first parent (stored
/// or not) as numbers. Numbers instead of maps of `ChangesetId`: Linux has
/// 1.5 million changesets.
struct Changesets {
    ids: HashIds,
    order: Vec<u32>,
    first_parent: Vec<u32>,
}

async fn oldest_first(repo: &Repo) -> Result<Changesets> {
    let mut ids = HashIds::new();
    let mut stored: Vec<bool> = Vec::new();
    let mut first_parent: Vec<u32> = Vec::new();
    // Distinct stored-or-not parents of each listed changeset, as
    // (child, parent) edges.
    let mut edges: Vec<(u32, u32)> = Vec::new();
    let mut after = None;
    let number =
        |ids: &mut HashIds, stored: &mut Vec<bool>, fp: &mut Vec<u32>, id: &ChangesetId| {
            let (n, new) = ids.insert(&id.0);
            if new {
                stored.push(false);
                fp.push(NO_ID);
            }
            n
        };
    loop {
        let page = repo.meta().list_changesets(after, PAGE).await?;
        let Some(last) = page.last().copied() else {
            break;
        };
        after = Some(last);
        let changesets = repo.meta().get_changesets(&page).await?;
        for (id, cs) in page.iter().zip(changesets) {
            let cs = match cs {
                Some(cs) => cs,
                None => repo.get_changeset(*id).await?,
            };
            let n = number(&mut ids, &mut stored, &mut first_parent, id);
            stored[n as usize] = true;
            let mut ps: Vec<u32> = cs
                .parents
                .iter()
                .map(|p| number(&mut ids, &mut stored, &mut first_parent, p))
                .collect();
            first_parent[n as usize] = ps.first().copied().unwrap_or(NO_ID);
            ps.sort_unstable();
            ps.dedup();
            edges.extend(ps.into_iter().map(|p| (n, p)));
        }
    }
    // Parents that are not stored are ignored.
    edges.retain(|&(_, p)| stored[p as usize]);
    let n = ids.len();
    let mut waiting = vec![0u32; n];
    // Children of each changeset, as a compressed adjacency list.
    let mut child_at = vec![0u32; n + 1];
    for &(c, p) in &edges {
        waiting[c as usize] += 1;
        child_at[p as usize + 1] += 1;
    }
    for i in 0..n {
        child_at[i + 1] += child_at[i];
    }
    let mut children = vec![0u32; edges.len()];
    let mut fill = child_at.clone();
    for &(c, p) in &edges {
        children[fill[p as usize] as usize] = c;
        fill[p as usize] += 1;
    }
    drop(fill);
    // Kahn's algorithm, iteratively. A child's generation is one more than
    // its highest parent's, which is final by the time the child is ready.
    let mut generation = vec![0u64; n];
    let mut ready: Vec<u32> = (0..n as u32)
        .filter(|&i| stored[i as usize] && waiting[i as usize] == 0)
        .collect();
    for &i in &ready {
        generation[i as usize] = 1;
    }
    let mut order: Vec<u32> = Vec::new();
    while let Some(i) = ready.pop() {
        order.push(i);
        let g = generation[i as usize];
        for &c in &children[child_at[i as usize] as usize..child_at[i as usize + 1] as usize] {
            let c = c as usize;
            generation[c] = generation[c].max(g + 1);
            waiting[c] -= 1;
            if waiting[c] == 0 {
                ready.push(c as u32);
            }
        }
    }
    order.sort_unstable_by_key(|&i| (generation[i as usize], *ids.hash(i)));
    Ok(Changesets {
        ids,
        order,
        first_parent,
    })
}

/// Root trees of `ids`, read in one round trip where stored.
async fn root_trees(repo: &Repo, ids: &[ChangesetId]) -> Result<Vec<Hash>> {
    let keys: Vec<Hash> = ids.iter().map(|id| id.0).collect();
    let values = repo.meta().get_derived_many(RootTree::NAME, &keys).await?;
    let mut out = Vec::with_capacity(ids.len());
    for (id, v) in ids.iter().zip(values) {
        out.push(match v.map(|v| decode_hash(&v)) {
            Some(Ok(h)) => h,
            _ => repo.root_tree(*id).await?,
        });
    }
    Ok(out)
}

/// Walk every changeset's root tree against its first parent's, skipping
/// equal subtrees, and list each new tree node and file version once.
pub async fn pack_order(repo: &Repo) -> Result<PackOrder> {
    let cs = oldest_first(repo).await?;
    let mut walk = Walk::default();
    // Root tree of each changeset walked, as a walk id.
    let mut roots = vec![NO_ID; cs.ids.len()];
    for chunk in cs.order.chunks(PAGE) {
        let ids: Vec<ChangesetId> = chunk
            .iter()
            .map(|&i| ChangesetId(*cs.ids.hash(i)))
            .collect();
        for (&i, root) in chunk.iter().zip(root_trees(repo, &ids).await?) {
            let parent_root = match cs.first_parent[i as usize] {
                NO_ID => None,
                p => match roots[p as usize] {
                    NO_ID => Some(repo.root_tree(ChangesetId(*cs.ids.hash(p))).await?),
                    r => Some(*walk.ids.hash(r)),
                },
            };
            roots[i as usize] = walk.changeset(repo, root, parent_root, &|_| true).await?;
        }
    }
    Ok(PackOrder {
        objects: walk.into_hints(),
        changesets: cs.order.len() as u64,
    })
}

/// Like [`pack_order`], for an incremental repack: only the changesets whose
/// objects are still loose, and only their loose objects. Walks back from
/// every ref tip and stops at a changeset whose root tree is not loose: a
/// repack packed everything stored before it, and a tree node is stored
/// after the nodes and blobs it names. So the cost is the new history, not
/// all of it. An object the walk misses (an upload no ref reaches, a node a
/// concurrent repack listed out of order) is still packed, whole.
pub async fn pack_order_loose(
    repo: &Repo,
    is_loose: &(dyn Fn(&Hash) -> bool + Sync),
) -> Result<PackOrder> {
    let empty = TreeNode::empty_hash();
    let mut seen: HashSet<ChangesetId> = HashSet::new();
    let mut stack: Vec<ChangesetId> = repo.meta().list_refs().await?.into_values().collect();
    let mut found: Vec<(u64, ChangesetId, Option<ChangesetId>)> = Vec::new();
    let mut roots: HashMap<ChangesetId, Hash> = HashMap::new();
    while let Some(id) = stack.pop() {
        if !seen.insert(id) {
            continue;
        }
        let root = repo.root_tree(id).await?;
        if root != empty && !is_loose(&root) {
            continue;
        }
        roots.insert(id, root);
        let parents = repo.get_changeset(id).await?.parents;
        found.push((repo.generation(id).await?, id, parents.first().copied()));
        stack.extend(parents);
    }
    found.sort();
    let mut walk = Walk::default();
    for &(_, id, first_parent) in &found {
        let parent_root = match first_parent {
            Some(p) => match roots.get(&p) {
                Some(r) => Some(*r),
                None => Some(repo.root_tree(p).await?),
            },
            None => None,
        };
        walk.changeset(repo, roots[&id], parent_root, is_loose)
            .await?;
    }
    Ok(PackOrder {
        objects: walk.into_hints(),
        changesets: found.len() as u64,
    })
}

/// Walk state, with every hash it meets numbered once in `ids`.
#[derive(Default)]
struct Walk {
    ids: HashIds,
    /// Objects in write order, with their bases, as ids.
    objects: Vec<(u32, u32)>,
    /// One bit per id: listed (or refused) already.
    seen: Vec<u64>,
    /// Last object listed at each path: the base when the first parent has
    /// nothing there (a file deleted and re-added, or added on a side branch).
    last_at: HashMap<String, u32>,
}

impl Walk {
    fn id(&mut self, hash: &Hash) -> u32 {
        let (id, new) = self.ids.insert(hash);
        if new && self.seen.len() * 64 <= id as usize {
            self.seen.push(0);
        }
        id
    }

    fn is_seen(&self, id: u32) -> bool {
        self.seen[id as usize / 64] & (1 << (id % 64)) != 0
    }

    /// Mark `id` seen; `false` if it was already.
    fn see(&mut self, id: u32) -> bool {
        let was = self.is_seen(id);
        self.seen[id as usize / 64] |= 1 << (id % 64);
        !was
    }

    fn into_hints(self) -> PackHints {
        PackHints {
            hashes: self.ids.into_hashes(),
            objects: self.objects,
        }
    }

    /// List the objects of the changeset with root tree `root` that its
    /// first parent (root tree `parent_root`) lacks, oldest generation first
    /// across calls. Objects `wanted` refuses are not listed, and a refused
    /// tree node's subtree is not entered; both still serve as bases.
    /// Returns the id of `root`.
    async fn changeset(
        &mut self,
        repo: &Repo,
        root: Hash,
        parent_root: Option<Hash>,
        wanted: &(dyn Fn(&Hash) -> bool + Sync),
    ) -> Result<u32> {
        let empty = TreeNode::empty_hash();
        let root_id = self.id(&root);
        let parent_id = parent_root.map_or(NO_ID, |r| self.id(&r));
        let mut stack: Vec<(u32, u32, String)> = vec![(root_id, parent_id, String::new())];
        while let Some((tree_id, old_id, path)) = stack.pop() {
            if !self.see(tree_id) {
                continue;
            }
            let base = match old_id {
                o if o != NO_ID && o != tree_id => o,
                _ => self.last_at.get(&path).copied().unwrap_or(NO_ID),
            };
            let tree = *self.ids.hash(tree_id);
            self.last_at.insert(path.clone(), tree_id);
            if tree == empty || !wanted(&tree) {
                // The empty node is never stored; read_tree answers it.
                continue;
            }
            self.objects.push((tree_id, base));
            // Shared: the parent's node read here is usually the node the
            // parent's own walk just read.
            let node = repo.read_tree_shared(tree).await?;
            let old_node = match old_id {
                NO_ID => None,
                o if *self.ids.hash(o) == empty => None,
                o => Some(repo.read_tree_shared(*self.ids.hash(o)).await?),
            };
            for (name, entry) in &node.entries {
                let child_path = if path.is_empty() {
                    name.clone()
                } else {
                    format!("{path}/{name}")
                };
                let before = old_node.as_ref().and_then(|n| n.entries.get(name));
                match entry {
                    TreeEntry::File { content, .. } => {
                        let content_id = self.id(content);
                        if !self.see(content_id) {
                            continue;
                        }
                        let base = match before {
                            Some(TreeEntry::File { content: c, .. }) if c != content => self.id(c),
                            _ => self.last_at.get(&child_path).copied().unwrap_or(NO_ID),
                        };
                        self.last_at.insert(child_path, content_id);
                        if wanted(content) {
                            let base = if base == content_id { NO_ID } else { base };
                            self.objects.push((content_id, base));
                        }
                    }
                    TreeEntry::Dir { tree: child } => {
                        let child_id = self.id(child);
                        let old_child = match before {
                            Some(TreeEntry::Dir { tree: t }) => self.id(t),
                            _ => NO_ID,
                        };
                        if !self.is_seen(child_id) {
                            stack.push((child_id, old_child, child_path));
                        }
                    }
                }
            }
        }
        Ok(root_id)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use bytes::Bytes;
    use leep_core::memory::{MemoryBlobStore, MemoryMetaStore};
    use leep_core::{Changeset, FileChange, FileKind, RepoPath};

    use super::*;

    async fn commit(r: &Repo, parents: &[ChangesetId], files: &[(&str, &str)]) -> ChangesetId {
        let mut changes = BTreeMap::new();
        for (p, t) in files {
            let content = r.blobs().put(Bytes::from(t.to_string())).await.unwrap();
            changes.insert(
                RepoPath::new(*p).unwrap(),
                FileChange::Modify {
                    content,
                    kind: FileKind::Regular,
                    size: t.len() as u64,
                },
            );
        }
        let cs = Changeset {
            parents: parents.to_vec(),
            author: "t".into(),
            timestamp: 1,
            message: format!("{files:?}"),
            changes,
            extra: BTreeMap::new(),
        };
        let id = r.meta().put_changeset(&cs).await.unwrap();
        r.root_tree(id).await.unwrap();
        id
    }

    #[tokio::test]
    async fn bases_are_previous_versions_and_come_first() {
        let r = Repo::new(
            Arc::new(MemoryBlobStore::new()),
            Arc::new(MemoryMetaStore::new()),
        );
        let a = commit(&r, &[], &[("src/a.rs", "v1"), ("README", "r1")]).await;
        let b = commit(&r, &[a], &[("src/a.rs", "v2")]).await;
        let c = commit(&r, &[b], &[("src/a.rs", "v3"), ("src/new.rs", "n")]).await;
        let order = pack_order(&r).await.unwrap();
        assert_eq!(order.changesets, 3);
        let pos: HashMap<Hash, usize> = order
            .objects
            .iter()
            .enumerate()
            .map(|(i, (h, _))| (h, i))
            .collect();
        assert_eq!(pos.len(), order.objects.len(), "each object once");
        let base_of: HashMap<Hash, Option<Hash>> = order.objects.iter().collect();
        let h = |t: &str| Hash::of_blob(t.as_bytes());
        assert_eq!(base_of[&h("v2")], Some(h("v1")));
        assert_eq!(base_of[&h("v3")], Some(h("v2")));
        assert_eq!(base_of[&h("v1")], None);
        assert_eq!(base_of[&h("n")], None);
        assert_eq!(base_of[&h("r1")], None);
        // Root trees and the src/ nodes are each based on the parent's.
        let (ra, rb, rc) = (
            r.root_tree(a).await.unwrap(),
            r.root_tree(b).await.unwrap(),
            r.root_tree(c).await.unwrap(),
        );
        assert_eq!(base_of[&rb], Some(ra));
        assert_eq!(base_of[&rc], Some(rb));
        let src = |root: Hash| {
            let r = r.clone();
            async move {
                match r.read_tree(root).await.unwrap().entries["src"] {
                    TreeEntry::Dir { tree } => tree,
                    _ => unreachable!(),
                }
            }
        };
        assert_eq!(base_of[&src(rc).await], Some(src(rb).await));
        for (h, b) in order.objects.iter() {
            if let Some(b) = b {
                assert!(pos[&b] < pos[&h], "a base comes before its delta");
            }
            assert!(r.blobs().contains(&h).await.unwrap());
        }
    }

    /// Only the changesets after the packed ones are walked, and only their
    /// loose objects listed; a base in a pack is still named.
    #[tokio::test]
    async fn the_loose_walk_stops_at_packed_history() {
        let r = Repo::new(
            Arc::new(MemoryBlobStore::new()),
            Arc::new(MemoryMetaStore::new()),
        );
        let mut tip = commit(&r, &[], &[("src/a.rs", "v0"), ("README", "r1")]).await;
        for i in 1..50 {
            tip = commit(&r, &[tip], &[("src/a.rs", &format!("v{i}"))]).await;
        }
        let rf = |r: &Repo, to| {
            let r = r.clone();
            async move {
                assert!(r
                    .meta()
                    .cas_ref("main", r.meta().get_ref("main").await.unwrap(), Some(to))
                    .await
                    .unwrap());
            }
        };
        rf(&r, tip).await;
        // Everything so far is "packed".
        let full = pack_order(&r).await.unwrap();
        let packed: HashSet<Hash> = full.objects.iter().map(|(h, _)| h).collect();
        let b = commit(&r, &[tip], &[("src/a.rs", "v50"), ("src/new.rs", "n")]).await;
        let side = commit(&r, &[tip], &[("side.txt", "s")]).await;
        let m = commit(&r, &[b, side], &[("src/a.rs", "v51")]).await;
        rf(&r, m).await;
        let is_loose = |h: &Hash| !packed.contains(h);
        let order = pack_order_loose(&r, &is_loose).await.unwrap();
        assert_eq!(order.changesets, 3, "only the new changesets");
        let base_of: HashMap<Hash, Option<Hash>> = order.objects.iter().collect();
        let h = |t: &str| Hash::of_blob(t.as_bytes());
        assert_eq!(base_of[&h("v50")], Some(h("v49")), "a packed base is named");
        assert_eq!(base_of[&h("v51")], Some(h("v50")));
        assert!(base_of.contains_key(&h("s")) && base_of.contains_key(&h("n")));
        assert!(order.objects.iter().all(|(h, _)| is_loose(&h)));
        // Every loose object a ref reaches is listed: the new versions, and
        // the root and src/ nodes of the three new changesets (one shared).
        let full = pack_order(&r).await.unwrap();
        let loose: HashSet<Hash> = full
            .objects
            .iter()
            .map(|(h, _)| h)
            .filter(|h| is_loose(h))
            .collect();
        let listed: HashSet<Hash> = order.objects.iter().map(|(h, _)| h).collect();
        assert_eq!(listed, loose);
    }
}
