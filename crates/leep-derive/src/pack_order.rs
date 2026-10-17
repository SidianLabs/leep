//! The order to pack blobs in, and the base to compress each against
//! (ADR 0029). A file version's base is the previous content at the same
//! path; a tree node's base is the node of the same directory in the first
//! parent. Both come from one walk over every changeset, oldest generation
//! first, so a base is always listed before the objects based on it. An
//! incremental repack walks only the changesets whose objects are loose.

use std::collections::{HashMap, HashSet};

use leep_core::{ChangesetId, Hash, Result, TreeEntry, TreeNode};

use crate::Repo;

const PAGE: usize = 1000;

#[derive(Debug, Default)]
pub struct PackOrder {
    /// Objects in write order, each with the object to delta it against.
    pub objects: Vec<(Hash, Option<Hash>)>,
    pub changesets: u64,
}

/// Every stored changeset (not only those on refs), parents before children.
async fn oldest_first(repo: &Repo) -> Result<Vec<ChangesetId>> {
    let mut parents: HashMap<ChangesetId, Vec<ChangesetId>> = HashMap::new();
    let mut after = None;
    loop {
        let page = repo.meta().list_changesets(after, PAGE).await?;
        let Some(last) = page.last().copied() else {
            break;
        };
        after = Some(last);
        for id in page {
            parents.insert(id, repo.get_changeset(id).await?.parents);
        }
    }
    // Kahn's algorithm, iteratively; parents that are not stored are ignored.
    let mut children: HashMap<ChangesetId, Vec<ChangesetId>> = HashMap::new();
    let mut waiting: HashMap<ChangesetId, usize> = HashMap::new();
    for (id, ps) in &parents {
        let known: HashSet<&ChangesetId> = ps.iter().filter(|p| parents.contains_key(p)).collect();
        waiting.insert(*id, known.len());
        for p in known {
            children.entry(*p).or_default().push(*id);
        }
    }
    let mut generation: HashMap<ChangesetId, u64> = HashMap::new();
    let mut ready: Vec<ChangesetId> = waiting
        .iter()
        .filter(|(_, n)| **n == 0)
        .map(|(id, _)| *id)
        .collect();
    while let Some(id) = ready.pop() {
        let g = parents[&id]
            .iter()
            .filter_map(|p| generation.get(p))
            .max()
            .map_or(1, |g| g + 1);
        generation.insert(id, g);
        for c in children.get(&id).into_iter().flatten() {
            let n = waiting.get_mut(c).unwrap();
            *n -= 1;
            if *n == 0 {
                ready.push(*c);
            }
        }
    }
    let mut order: Vec<ChangesetId> = generation.keys().copied().collect();
    order.sort_by_key(|id| (generation[id], *id));
    Ok(order)
}

/// Walk every changeset's root tree against its first parent's, skipping
/// equal subtrees, and list each new tree node and file version once.
pub async fn pack_order(repo: &Repo) -> Result<PackOrder> {
    let order = oldest_first(repo).await?;
    let mut walk = Walk::default();
    for &id in &order {
        walk.changeset(repo, id, &|_| true).await?;
    }
    Ok(PackOrder {
        objects: walk.objects,
        changesets: order.len() as u64,
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
    let mut found: Vec<(u64, ChangesetId)> = Vec::new();
    while let Some(id) = stack.pop() {
        if !seen.insert(id) {
            continue;
        }
        let root = repo.root_tree(id).await?;
        if root != empty && !is_loose(&root) {
            continue;
        }
        found.push((repo.generation(id).await?, id));
        stack.extend(repo.get_changeset(id).await?.parents);
    }
    found.sort();
    let mut walk = Walk::default();
    for &(_, id) in &found {
        walk.changeset(repo, id, is_loose).await?;
    }
    Ok(PackOrder {
        objects: walk.objects,
        changesets: found.len() as u64,
    })
}

#[derive(Default)]
struct Walk {
    objects: Vec<(Hash, Option<Hash>)>,
    seen: HashSet<Hash>,
    roots: HashMap<ChangesetId, Hash>,
    /// Last object listed at each path: the base when the first parent has
    /// nothing there (a file deleted and re-added, or added on a side branch).
    last_at: HashMap<String, Hash>,
}

impl Walk {
    /// List the objects `id` adds over its first parent, oldest generation
    /// first across calls. Objects `wanted` refuses are not listed, and a
    /// refused tree node's subtree is not entered; both still serve as bases.
    async fn changeset(
        &mut self,
        repo: &Repo,
        id: ChangesetId,
        wanted: &(dyn Fn(&Hash) -> bool + Sync),
    ) -> Result<()> {
        let empty = TreeNode::empty_hash();
        let root = repo.root_tree(id).await?;
        self.roots.insert(id, root);
        let first_parent = repo.get_changeset(id).await?.parents.first().copied();
        let parent_root = match first_parent {
            Some(p) => match self.roots.get(&p) {
                Some(r) => Some(*r),
                None => Some(repo.root_tree(p).await?),
            },
            None => None,
        };
        let mut stack: Vec<(Hash, Option<Hash>, String)> = vec![(root, parent_root, String::new())];
        while let Some((tree, old, path)) = stack.pop() {
            if !self.seen.insert(tree) {
                continue;
            }
            let base = old
                .filter(|o| *o != tree)
                .or_else(|| self.last_at.get(&path).copied());
            self.last_at.insert(path.clone(), tree);
            if tree == empty || !wanted(&tree) {
                // The empty node is never stored; read_tree answers it.
                continue;
            }
            self.objects.push((tree, base));
            let node = repo.read_tree(tree).await?;
            let old_node = match old {
                Some(o) if o != empty => Some(repo.read_tree(o).await?),
                _ => None,
            };
            for (name, entry) in node.entries {
                let child_path = if path.is_empty() {
                    name.clone()
                } else {
                    format!("{path}/{name}")
                };
                let before = old_node.as_ref().and_then(|n| n.entries.get(&name));
                match entry {
                    TreeEntry::File { content, .. } => {
                        if !self.seen.insert(content) {
                            continue;
                        }
                        let base = match before {
                            Some(TreeEntry::File { content: c, .. }) if *c != content => Some(*c),
                            _ => self.last_at.get(&child_path).copied(),
                        };
                        self.last_at.insert(child_path, content);
                        if wanted(&content) {
                            self.objects.push((content, base.filter(|b| *b != content)));
                        }
                    }
                    TreeEntry::Dir { tree: child } => {
                        let old_child = match before {
                            Some(TreeEntry::Dir { tree: t }) => Some(*t),
                            _ => None,
                        };
                        if !self.seen.contains(&child) {
                            stack.push((child, old_child, child_path));
                        }
                    }
                }
            }
        }
        Ok(())
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
            .map(|(i, (h, _))| (*h, i))
            .collect();
        assert_eq!(pos.len(), order.objects.len(), "each object once");
        let base_of: HashMap<Hash, Option<Hash>> = order.objects.iter().copied().collect();
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
        for (h, b) in &order.objects {
            if let Some(b) = b {
                assert!(pos[b] < pos[h], "a base comes before its delta");
            }
            assert!(r.blobs().contains(h).await.unwrap());
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
        let packed: HashSet<Hash> = full.objects.iter().map(|(h, _)| *h).collect();
        let b = commit(&r, &[tip], &[("src/a.rs", "v50"), ("src/new.rs", "n")]).await;
        let side = commit(&r, &[tip], &[("side.txt", "s")]).await;
        let m = commit(&r, &[b, side], &[("src/a.rs", "v51")]).await;
        rf(&r, m).await;
        let is_loose = |h: &Hash| !packed.contains(h);
        let order = pack_order_loose(&r, &is_loose).await.unwrap();
        assert_eq!(order.changesets, 3, "only the new changesets");
        let base_of: HashMap<Hash, Option<Hash>> = order.objects.iter().copied().collect();
        let h = |t: &str| Hash::of_blob(t.as_bytes());
        assert_eq!(base_of[&h("v50")], Some(h("v49")), "a packed base is named");
        assert_eq!(base_of[&h("v51")], Some(h("v50")));
        assert!(base_of.contains_key(&h("s")) && base_of.contains_key(&h("n")));
        assert!(order.objects.iter().all(|(h, _)| is_loose(h)));
        // Every loose object a ref reaches is listed: the new versions, and
        // the root and src/ nodes of the three new changesets (one shared).
        let full = pack_order(&r).await.unwrap();
        let loose: HashSet<Hash> = full
            .objects
            .iter()
            .map(|(h, _)| *h)
            .filter(|h| is_loose(h))
            .collect();
        let listed: HashSet<Hash> = order.objects.iter().map(|(h, _)| *h).collect();
        assert_eq!(listed, loose);
    }
}
