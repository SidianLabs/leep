//! Tree operations: applying changes, lookups, listings and tree diffs.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use bytes::Bytes;
use leep_core::api::{DiffEntry, FileInfo, TreeResponse};
use leep_core::{ChangesetId, Error, FileChange, Hash, RepoPath, Result, TreeEntry, TreeNode};

use crate::Repo;

pub(crate) fn join(dir: &str, name: &str) -> String {
    if dir.is_empty() {
        name.to_string()
    } else {
        format!("{dir}/{name}")
    }
}

fn parent_dir(path: &str) -> &str {
    path.rsplit_once('/').map_or("", |(d, _)| d)
}

fn depth(dir: &str) -> usize {
    if dir.is_empty() {
        0
    } else {
        dir.matches('/').count() + 1
    }
}

pub(crate) fn file_info(path: RepoPath, e: &TreeEntry) -> Option<FileInfo> {
    match e {
        TreeEntry::File {
            content,
            kind,
            size,
        } => Some(FileInfo {
            path,
            content: *content,
            kind: *kind,
            size: *size,
        }),
        TreeEntry::Dir { .. } => None,
    }
}

/// Load the directory nodes along `dir` into `nodes` (which must contain the
/// root). Returns false if a component is absent and `create` is false.
async fn ensure_dir(
    repo: &Repo,
    nodes: &mut HashMap<String, TreeNode>,
    dir: &str,
    create: bool,
) -> Result<bool> {
    let mut cur = String::new();
    for comp in dir.split('/').filter(|c| !c.is_empty()) {
        let next = join(&cur, comp);
        if !nodes.contains_key(&next) {
            let node = match nodes[&cur].entries.get(comp) {
                Some(TreeEntry::Dir { tree }) => repo.read_tree(*tree).await?,
                None if create => TreeNode::default(),
                None => return Ok(false),
                Some(TreeEntry::File { .. }) => {
                    return Err(Error::Invalid(format!("{next} is a file, not a directory")))
                }
            };
            nodes.insert(next.clone(), node);
        }
        cur = next;
    }
    Ok(true)
}

/// Apply `changes` to the tree `base`, rewriting only directories on changed
/// paths and pruning empty ones. Returns the new root hash. Errors with
/// `Invalid` for deletes of absent files and file/directory clashes.
///
/// The nodes written and the contents `changes` names are flushed
/// (`BlobStore::flush`) before it returns, so the changeset built on the
/// result can be stored at once (ADR 0044). Callers check the contents exist
/// first, so the flush follows whatever write made them visible.
pub(crate) async fn apply_changes(
    repo: &Repo,
    base: Hash,
    changes: &BTreeMap<RepoPath, FileChange>,
) -> Result<Hash> {
    if changes.is_empty() {
        if base == TreeNode::empty_hash() {
            let empty = TreeNode::default();
            repo.put_rescuing_many(vec![(base, Bytes::from(empty.encode()))], &[])
                .await?;
        }
        return Ok(base);
    }
    let mut nodes: HashMap<String, TreeNode> = HashMap::new();
    nodes.insert(String::new(), repo.read_tree(base).await?);

    // Deletes first, so a changeset may replace a file with a directory.
    let mut deleted = false;
    for (path, change) in changes {
        if !matches!(change, FileChange::Delete) {
            continue;
        }
        let absent = || Error::Invalid(format!("delete of absent path {path}"));
        let dir = parent_dir(path.as_str());
        if !ensure_dir(repo, &mut nodes, dir, false).await? {
            return Err(absent());
        }
        let node = nodes.get_mut(dir).expect("loaded");
        match node.entries.get(path.file_name()) {
            Some(TreeEntry::File { .. }) => node.entries.remove(path.file_name()),
            _ => return Err(absent()),
        };
        deleted = true;
    }
    if deleted {
        prune(&mut nodes);
    }
    for (path, change) in changes {
        let FileChange::Modify {
            content,
            kind,
            size,
        } = change
        else {
            continue;
        };
        let dir = parent_dir(path.as_str());
        ensure_dir(repo, &mut nodes, dir, true).await?;
        let node = nodes.get_mut(dir).expect("loaded");
        if let Some(TreeEntry::Dir { .. }) = node.entries.get(path.file_name()) {
            return Err(Error::Invalid(format!("{path} is a directory")));
        }
        node.entries.insert(
            path.file_name().to_string(),
            TreeEntry::File {
                content: *content,
                kind: *kind,
                size: *size,
            },
        );
    }

    // Rebuild bottom-up, then store every rewritten node in one batch (the
    // changeset that will reference them is written after all of them).
    let mut dirs: Vec<String> = nodes.keys().filter(|d| !d.is_empty()).cloned().collect();
    dirs.sort_by_key(|d| std::cmp::Reverse(depth(d)));
    let mut rewritten = Vec::with_capacity(dirs.len() + 1);
    for d in dirs {
        let node = nodes.remove(&d).expect("present");
        let parent = nodes.get_mut(parent_dir(&d)).expect("parent loaded");
        let name = d.rsplit('/').next().expect("non-empty");
        if node.entries.is_empty() {
            parent.entries.remove(name);
        } else {
            let h = node.hash();
            rewritten.push((h, Bytes::from(node.encode())));
            parent
                .entries
                .insert(name.to_string(), TreeEntry::Dir { tree: h });
        }
    }
    let root = nodes.remove("").expect("root");
    let h = root.hash();
    rewritten.push((h, Bytes::from(root.encode())));
    let mut named: Vec<Hash> = changes
        .values()
        .filter_map(|c| match c {
            FileChange::Modify { content, .. } => Some(*content),
            FileChange::Delete => None,
        })
        .collect();
    named.sort_unstable();
    named.dedup();
    repo.put_rescuing_many(rewritten, &named).await?;
    Ok(h)
}

/// Drop loaded directories that became empty (deepest first).
fn prune(nodes: &mut HashMap<String, TreeNode>) {
    let mut dirs: Vec<String> = nodes.keys().filter(|d| !d.is_empty()).cloned().collect();
    dirs.sort_by_key(|d| std::cmp::Reverse(depth(d)));
    for d in dirs {
        if nodes[&d].entries.is_empty() {
            nodes.remove(&d);
            let name = d.rsplit('/').next().expect("non-empty");
            if let Some(p) = nodes.get_mut(parent_dir(&d)) {
                p.entries.remove(name);
            }
        }
    }
}

impl Repo {
    /// Entry at `path` under `tree`; `""` is the root directory itself.
    pub(crate) async fn entry_at(&self, tree: Hash, path: &str) -> Result<Option<TreeEntry>> {
        let mut cur = TreeEntry::Dir { tree };
        for comp in path.split('/').filter(|c| !c.is_empty()) {
            let TreeEntry::Dir { tree } = cur else {
                return Ok(None);
            };
            match self.read_tree(tree).await?.entries.remove(comp) {
                Some(e) => cur = e,
                None => return Ok(None),
            }
        }
        Ok(Some(cur))
    }

    pub(crate) async fn file_at(&self, tree: Hash, path: &RepoPath) -> Result<Option<FileInfo>> {
        Ok(self
            .entry_at(tree, path.as_str())
            .await?
            .and_then(|e| file_info(path.clone(), &e)))
    }

    /// One directory level; `NotFound` if `dir` is not a directory.
    pub async fn list_dir(&self, id: ChangesetId, dir: &str) -> Result<TreeResponse> {
        let dir = dir.trim_matches('/');
        let root = self.root_tree(id).await?;
        match self.entry_at(root, dir).await? {
            Some(TreeEntry::Dir { tree }) => Ok(TreeResponse {
                path: dir.to_string(),
                tree,
                entries: self.read_tree(tree).await?.entries,
            }),
            _ => Err(Error::NotFound(format!("directory {dir:?}"))),
        }
    }

    /// Recursive file listing under `prefix`, sorted by path.
    pub async fn list_files(&self, id: ChangesetId, prefix: &str) -> Result<Vec<FileInfo>> {
        let prefix = prefix.trim_matches('/');
        let root = self.root_tree(id).await?;
        let mut out = Vec::new();
        self.collect_files(prefix, self.entry_at(root, prefix).await?, &mut out)
            .await?;
        out.sort_by(|a, b| a.path.cmp(&b.path));
        Ok(out)
    }

    async fn collect_files(
        &self,
        path: &str,
        entry: Option<TreeEntry>,
        out: &mut Vec<FileInfo>,
    ) -> Result<()> {
        let mut stack = vec![(path.to_string(), entry)];
        while let Some((p, e)) = stack.pop() {
            match e {
                None => {}
                Some(TreeEntry::Dir { tree }) => {
                    for (name, child) in self.read_tree(tree).await?.entries {
                        stack.push((join(&p, &name), Some(child)));
                    }
                }
                Some(f) => out.extend(file_info(RepoPath::new(p)?, &f)),
            }
        }
        Ok(())
    }

    pub async fn lookup_file(&self, id: ChangesetId, path: &RepoPath) -> Result<Option<FileInfo>> {
        let root = self.root_tree(id).await?;
        self.file_at(root, path).await
    }

    pub async fn read_file(
        &self,
        id: ChangesetId,
        path: &RepoPath,
    ) -> Result<Option<(FileInfo, Bytes)>> {
        let Some(info) = self.lookup_file(id, path).await? else {
            return Ok(None);
        };
        let data =
            self.blobs().get(&info.content).await?.ok_or_else(|| {
                Error::Storage(format!("blob {} missing for {path}", info.content))
            })?;
        Ok(Some((info, data)))
    }

    /// File-level diff of two trees under `prefix`, skipping equal subtrees.
    /// Sorted by path.
    pub(crate) async fn diff_trees(
        &self,
        from: Hash,
        to: Hash,
        prefix: &str,
    ) -> Result<Vec<DiffEntry>> {
        let prefix = prefix.trim_matches('/');
        let mut stack = vec![(
            prefix.to_string(),
            self.entry_at(from, prefix).await?,
            self.entry_at(to, prefix).await?,
        )];
        let mut out = Vec::new();
        while let Some((path, old, new)) = stack.pop() {
            if old == new {
                continue;
            }
            let dir_of = |e: &Option<TreeEntry>| match e {
                Some(TreeEntry::Dir { tree }) => Some(*tree),
                _ => None,
            };
            let (old_dir, new_dir) = (dir_of(&old), dir_of(&new));
            if old_dir.is_none() && new_dir.is_none() {
                let rp = RepoPath::new(path)?;
                out.push(DiffEntry {
                    old: old.as_ref().and_then(|e| file_info(rp.clone(), e)),
                    new: new.as_ref().and_then(|e| file_info(rp.clone(), e)),
                    path: rp,
                });
                continue;
            }
            // A file opposite a directory is a one-sided file change.
            let is_file = |e: &Option<TreeEntry>| matches!(e, Some(TreeEntry::File { .. }));
            if is_file(&old) || is_file(&new) {
                let rp = RepoPath::new(path.clone())?;
                out.push(DiffEntry {
                    old: old.as_ref().and_then(|e| file_info(rp.clone(), e)),
                    new: new.as_ref().and_then(|e| file_info(rp.clone(), e)),
                    path: rp,
                });
            }
            let load = |t: Option<Hash>| async move {
                match t {
                    Some(t) => self.read_tree(t).await,
                    None => Ok(TreeNode::default()),
                }
            };
            let (a, b) = (load(old_dir).await?, load(new_dir).await?);
            let names: BTreeSet<&String> = a.entries.keys().chain(b.entries.keys()).collect();
            for name in names {
                stack.push((
                    join(&path, name),
                    a.entries.get(name).cloned(),
                    b.entries.get(name).cloned(),
                ));
            }
        }
        out.sort_by(|a, b| a.path.cmp(&b.path));
        Ok(out)
    }

    /// Diff `from` (`None` = empty tree) to `to`, restricted to `prefix`.
    pub async fn diff(
        &self,
        from: Option<ChangesetId>,
        to: ChangesetId,
        prefix: &str,
    ) -> Result<Vec<DiffEntry>> {
        let from_tree = match from {
            Some(f) => self.root_tree(f).await?,
            None => TreeNode::empty_hash(),
        };
        let to_tree = self.root_tree(to).await?;
        self.diff_trees(from_tree, to_tree, prefix).await
    }
}

/// A tree node read for lookups only: its encoded bytes and where each entry
/// starts. Path-history derivation looks up a few names in each directory on
/// a changed path, twice per changeset (old and new tree); decoding a
/// directory of thousands of names into a map of owned names, and dropping
/// it when the cache rotates, cost more than the rest of the derivation.
/// A view costs one pass over the bytes and no allocation per entry.
pub(crate) struct TreeView {
    bytes: Bytes,
    /// Per entry, by name: where its name starts, the name's length, and
    /// where its `TreeEntry` starts.
    index: Vec<(u32, u32, u32)>,
}

impl TreeView {
    /// The view of an encoded tree node, which answers lookups exactly as
    /// the decoded node would. Bytes that are not a canonical encoding
    /// (names out of order, trailing bytes) are decoded as before, with the
    /// same errors, and viewed through their canonical encoding.
    pub(crate) fn parse(bytes: Bytes) -> Result<Self> {
        if let Some(index) = Self::scan(&bytes) {
            return Ok(TreeView { bytes, index });
        }
        let bytes = Bytes::from(TreeNode::decode(&bytes)?.encode());
        let index = Self::scan(&bytes)
            .ok_or_else(|| Error::Encoding("tree node does not re-encode canonically".into()))?;
        Ok(TreeView { bytes, index })
    }

    pub(crate) fn empty() -> Self {
        TreeView::parse(Bytes::from(TreeNode::default().encode())).expect("the empty node parses")
    }

    fn scan(b: &[u8]) -> Option<Vec<(u32, u32, u32)>> {
        u32::try_from(b.len()).ok()?;
        let mut pos = 0;
        let n = varint(b, &mut pos)?;
        let mut index = Vec::with_capacity(n.min(b.len()));
        let mut prev: Option<&[u8]> = None;
        for _ in 0..n {
            let len = varint(b, &mut pos)?;
            let name = b.get(pos..pos.checked_add(len)?)?;
            std::str::from_utf8(name).ok()?;
            if prev.is_some_and(|p| p >= name) {
                return None;
            }
            let name_at = pos;
            pos += len;
            let entry_at = pos;
            // A `TreeEntry`: variant 0 (`File`) with a hash, a kind and a
            // size, or 1 (`Dir`) with a hash. Skipped without decoding: the
            // hash alone is 32 fields to postcard.
            let tag = varint(b, &mut pos)?;
            pos = pos.checked_add(32).filter(|&p| p <= b.len())?;
            match tag {
                0 => {
                    if varint(b, &mut pos)? > 2 {
                        return None;
                    }
                    varint(b, &mut pos)?;
                }
                1 => {}
                _ => return None,
            }
            let pos_after = pos;
            pos = entry_at;
            index.push((
                u32::try_from(name_at).ok()?,
                u32::try_from(len).ok()?,
                u32::try_from(pos).ok()?,
            ));
            pos = pos_after;
            prev = Some(name);
        }
        (pos == b.len()).then_some(index)
    }

    pub(crate) fn get(&self, name: &str) -> Result<Option<TreeEntry>> {
        let Ok(i) = self.index.binary_search_by(|&(at, len, _)| {
            self.bytes[at as usize..(at + len) as usize].cmp(name.as_bytes())
        }) else {
            return Ok(None);
        };
        let at = self.index[i].2 as usize;
        Ok(Some(
            postcard::take_from_bytes::<TreeEntry>(&self.bytes[at..])?.0,
        ))
    }

    /// About the memory it holds, for cache accounting.
    pub(crate) fn size(&self) -> usize {
        self.bytes.len() + 12 * self.index.len()
    }
}

/// A postcard varint (LEB128, at most 10 bytes) at `pos`, advancing it.
fn varint(b: &[u8], pos: &mut usize) -> Option<usize> {
    let mut v: u64 = 0;
    for i in 0..10 {
        let byte = *b.get(*pos)?;
        *pos += 1;
        if i == 9 && byte > 1 {
            return None;
        }
        v |= u64::from(byte & 0x7f) << (7 * i);
        if byte & 0x80 == 0 {
            return usize::try_from(v).ok();
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use leep_core::FileKind;

    fn node(n: usize) -> TreeNode {
        TreeNode {
            entries: (0..n)
                .map(|i| {
                    let e = if i % 3 == 0 {
                        TreeEntry::Dir {
                            tree: Hash([i as u8; 32]),
                        }
                    } else {
                        TreeEntry::File {
                            content: Hash([7; 32]),
                            kind: [FileKind::Regular, FileKind::Executable, FileKind::Symlink]
                                [i % 3],
                            size: (i as u64) << (i % 50),
                        }
                    };
                    (format!("n{i:05}é"), e)
                })
                .collect(),
        }
    }

    /// A view answers every lookup as the decoded node does, including for
    /// bytes that are not a canonical encoding.
    #[test]
    fn tree_views_answer_as_decoded_nodes() {
        for n in [0, 1, 2, 300, 3000] {
            let t = node(n);
            let view = TreeView::parse(Bytes::from(t.encode())).unwrap();
            for (name, e) in &t.entries {
                assert_eq!(view.get(name).unwrap().as_ref(), Some(e));
            }
            for name in ["", "a", "n00001", "zzz"] {
                assert_eq!(view.get(name).unwrap(), None);
            }
        }
        // Names out of order and a duplicate: postcard decodes them into the
        // map, the last duplicate winning.
        let a = TreeEntry::Dir {
            tree: Hash([1; 32]),
        };
        let b = TreeEntry::Dir {
            tree: Hash([2; 32]),
        };
        let pairs = vec![
            ("y".to_string(), a.clone()),
            ("x".to_string(), a.clone()),
            ("y".to_string(), b.clone()),
        ];
        let bytes = postcard::to_stdvec(&pairs).unwrap();
        let decoded = TreeNode::decode(&bytes).unwrap();
        let view = TreeView::parse(Bytes::from(bytes)).unwrap();
        for name in ["x", "y", "z"] {
            assert_eq!(view.get(name).unwrap().as_ref(), decoded.entries.get(name));
        }
        // Damage is the decoder's error.
        let mut bytes = node(5).encode();
        bytes.truncate(bytes.len() - 3);
        assert!(TreeView::parse(Bytes::from(bytes)).is_err());
    }
}
