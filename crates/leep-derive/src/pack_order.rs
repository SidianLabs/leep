//! The order to pack blobs in, and the base to compress each against
//! (ADR 0029). A file version's base is the previous content at the same
//! path; a tree node's base is the node of the same directory in the first
//! parent. Both come from one walk over every changeset, oldest generation
//! first, so a base is always listed before the objects based on it.

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
    let mut out = PackOrder {
        changesets: order.len() as u64,
        ..PackOrder::default()
    };
    let mut seen: HashSet<Hash> = HashSet::new();
    let mut roots: HashMap<ChangesetId, Hash> = HashMap::new();
    // Last object listed at each path: the base when the first parent has
    // nothing there (a file deleted and re-added, or added on a side branch).
    let mut last_at: HashMap<String, Hash> = HashMap::new();
    let empty = TreeNode::empty_hash();
    for id in order {
        let root = repo.root_tree(id).await?;
        roots.insert(id, root);
        let first_parent = repo.get_changeset(id).await?.parents.first().copied();
        let parent_root = match first_parent {
            Some(p) => match roots.get(&p) {
                Some(r) => Some(*r),
                None => Some(repo.root_tree(p).await?),
            },
            None => None,
        };
        let mut stack: Vec<(Hash, Option<Hash>, String)> = vec![(root, parent_root, String::new())];
        while let Some((tree, old, path)) = stack.pop() {
            if !seen.insert(tree) {
                continue;
            }
            let base = old
                .filter(|o| *o != tree)
                .or_else(|| last_at.get(&path).copied());
            last_at.insert(path.clone(), tree);
            if tree == empty {
                // Never stored; read_tree answers it without the store.
                continue;
            }
            out.objects.push((tree, base));
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
                        if !seen.insert(content) {
                            continue;
                        }
                        let base = match before {
                            Some(TreeEntry::File { content: c, .. }) if *c != content => Some(*c),
                            _ => last_at.get(&child_path).copied(),
                        };
                        last_at.insert(child_path, content);
                        out.objects.push((content, base.filter(|b| *b != content)));
                    }
                    TreeEntry::Dir { tree: child } => {
                        let old_child = match before {
                            Some(TreeEntry::Dir { tree: t }) => Some(*t),
                            _ => None,
                        };
                        if !seen.contains(&child) {
                            stack.push((child, old_child, child_path));
                        }
                    }
                }
            }
        }
    }
    Ok(out)
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
}
