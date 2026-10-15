//! Tree operations: applying changes, lookups, listings and tree diffs.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use bytes::Bytes;
use leep_core::api::{DiffEntry, FileInfo, TreeResponse};
use leep_core::{ChangesetId, Error, FileChange, Hash, RepoPath, Result, TreeEntry, TreeNode};

use crate::Repo;

fn join(dir: &str, name: &str) -> String {
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
pub(crate) async fn apply_changes(
    repo: &Repo,
    base: Hash,
    changes: &BTreeMap<RepoPath, FileChange>,
) -> Result<Hash> {
    if changes.is_empty() {
        if base == TreeNode::empty_hash() {
            let empty = TreeNode::default();
            repo.blobs()
                .put_with_hash(base, Bytes::from(empty.encode()))
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

    // Rebuild bottom-up, storing each rewritten node.
    let mut dirs: Vec<String> = nodes.keys().filter(|d| !d.is_empty()).cloned().collect();
    dirs.sort_by_key(|d| std::cmp::Reverse(depth(d)));
    for d in dirs {
        let node = nodes.remove(&d).expect("present");
        let parent = nodes.get_mut(parent_dir(&d)).expect("parent loaded");
        let name = d.rsplit('/').next().expect("non-empty");
        if node.entries.is_empty() {
            parent.entries.remove(name);
        } else {
            let h = node.hash();
            repo.blobs()
                .put_with_hash(h, Bytes::from(node.encode()))
                .await?;
            parent
                .entries
                .insert(name.to_string(), TreeEntry::Dir { tree: h });
        }
    }
    let root = nodes.remove("").expect("root");
    let h = root.hash();
    repo.blobs()
        .put_with_hash(h, Bytes::from(root.encode()))
        .await?;
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
