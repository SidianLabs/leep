//! Tree operations: applying changes, lookups, listings and tree diffs.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use bytes::Bytes;
use leep_core::api::{DiffEntry, DiffResponse, FileInfo, FilesResponse, TreeResponse};
use leep_core::rename::{Detected, RenameOptions};
use leep_core::{ChangesetId, Error, FileChange, Hash, RepoPath, Result, TreeEntry, TreeNode};

use crate::cursor::{self, path_cursor, CursorKind, Fingerprint, PathPos};
use crate::Repo;

/// Most tree nodes one [`Repo::read_trees`] of a listing or diff walk asks
/// for, bounding the bytes held at once.
const TREE_BATCH: usize = 1024;

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
/// (`BlobStore::flush_blobs`) before it returns, so the changeset built on the
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
        self.files_after(id, prefix, None, usize::MAX).await
    }

    /// One page of [`Repo::list_files`]: at most `limit` files after the
    /// `after` cursor, with `next` when more remain. `limit: None` is the
    /// whole rest of the listing; `Some(0)` is `Invalid`.
    pub async fn list_files_page(
        &self,
        id: ChangesetId,
        prefix: &str,
        limit: Option<usize>,
        after: Option<&str>,
    ) -> Result<FilesResponse> {
        let fp = Fingerprint::of(CursorKind::Files, &(id, prefix.trim_matches('/')));
        let after = page_start(limit, after, CursorKind::Files, fp)?;
        // One more than the page, to tell a full last page from a middle one.
        let want = limit.map_or(usize::MAX, |l| l.saturating_add(1));
        let mut files = self.files_after(id, prefix, after.as_deref(), want).await?;
        let next = match limit {
            Some(l) if files.len() > l => {
                files.truncate(l);
                Some(path_cursor(
                    CursorKind::Files,
                    fp,
                    files[l - 1].path.as_str(),
                )?)
            }
            _ => None,
        };
        Ok(FilesResponse { files, next })
    }

    /// Up to `want` files under `prefix` whose paths sort after `after`, in
    /// path order. The walk keeps the pending files and unread directories
    /// ordered by [`dir_key`], so it yields in order without collecting the
    /// whole tree and stops once it has `want`. It reads up to
    /// [`TREE_BATCH`] directories per [`Repo::read_trees`] (fewer when close
    /// to `want`), so a cold listing waits on the store about once per level
    /// of depth, as a breadth-first walk would. Directories that hold only
    /// paths at or before `after` are never read.
    async fn files_after(
        &self,
        id: ChangesetId,
        prefix: &str,
        after: Option<&str>,
        want: usize,
    ) -> Result<Vec<FileInfo>> {
        let prefix = prefix.trim_matches('/');
        let root = self.root_tree(id).await?;
        let mut q: PathQueue<FileInfo, Hash> = PathQueue::new(after);
        let add = |q: &mut PathQueue<FileInfo, Hash>, path: String, e: TreeEntry| -> Result<()> {
            match e {
                TreeEntry::Dir { tree } => q.dir(path, tree),
                f => {
                    if q.wants(&path) {
                        let info = file_info(RepoPath::new(path.clone())?, &f);
                        q.ready.extend(info.map(|i| (path, i)));
                    }
                }
            }
            Ok(())
        };
        if let Some(e) = self.entry_at(root, prefix).await? {
            add(&mut q, prefix.to_string(), e)?;
        }
        let mut out = Vec::new();
        while out.len() < want {
            match q.next() {
                Next::Done => break,
                Next::Ready(f) => out.push(f),
                Next::Read => {
                    let batch = q.take_dirs((want - out.len()).min(TREE_BATCH));
                    let hashes: Vec<Hash> = batch.iter().map(|(_, t)| *t).collect();
                    let nodes = self.read_trees(&hashes).await?;
                    for ((p, _), node) in batch.iter().zip(nodes) {
                        for (name, child) in node.entries {
                            add(&mut q, join(p, &name), child)?;
                        }
                    }
                }
            }
        }
        Ok(out)
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
        self.diff_trees_after(from, to, prefix, None, usize::MAX)
            .await
    }

    /// Up to `want` entries of [`Repo::diff_trees`] whose paths sort after
    /// `after`, walked in path order like [`Repo::files_after`] so it stops
    /// once it has them. Each read takes up to `TREE_BATCH / 2` directory
    /// pairs, so a cold diff waits on the store about once per level of
    /// depth rather than once per changed directory.
    async fn diff_trees_after(
        &self,
        from: Hash,
        to: Hash,
        prefix: &str,
        after: Option<&str>,
        want: usize,
    ) -> Result<Vec<DiffEntry>> {
        type Pair = (Option<Hash>, Option<Hash>);
        let prefix = prefix.trim_matches('/');
        let mut q: PathQueue<DiffEntry, Pair> = PathQueue::new(after);
        let dir_of = |e: &Option<TreeEntry>| match e {
            Some(TreeEntry::Dir { tree }) => Some(*tree),
            _ => None,
        };
        let is_file = |e: &Option<TreeEntry>| matches!(e, Some(TreeEntry::File { .. }));
        let add = |q: &mut PathQueue<DiffEntry, Pair>,
                   path: String,
                   old: Option<TreeEntry>,
                   new: Option<TreeEntry>|
         -> Result<()> {
            if old == new {
                return Ok(());
            }
            let (old_dir, new_dir) = (dir_of(&old), dir_of(&new));
            // A file opposite a directory (or nothing) is a one-sided file
            // change, and the directory is walked too.
            if (is_file(&old) || is_file(&new) || (old_dir.is_none() && new_dir.is_none()))
                && q.wants(&path)
            {
                let rp = RepoPath::new(path.clone())?;
                let entry = DiffEntry {
                    old: old.as_ref().and_then(|e| file_info(rp.clone(), e)),
                    new: new.as_ref().and_then(|e| file_info(rp.clone(), e)),
                    path: rp,
                };
                q.ready.insert(path.clone(), entry);
            }
            if old_dir.is_some() || new_dir.is_some() {
                q.dir(path, (old_dir, new_dir));
            }
            Ok(())
        };
        add(
            &mut q,
            prefix.to_string(),
            self.entry_at(from, prefix).await?,
            self.entry_at(to, prefix).await?,
        )?;
        let mut out = Vec::new();
        while out.len() < want {
            match q.next() {
                Next::Done => break,
                Next::Ready(e) => out.push(e),
                Next::Read => {
                    let batch = q.take_dirs((want - out.len()).min(TREE_BATCH / 2));
                    let hashes: Vec<Hash> = batch
                        .iter()
                        .flat_map(|(_, (a, b))| [*a, *b])
                        .flatten()
                        .collect();
                    // The same node can be on both sides of different pairs.
                    let nodes: HashMap<Hash, Arc<TreeNode>> = hashes
                        .iter()
                        .copied()
                        .zip(self.read_trees(&hashes).await?.into_iter().map(Arc::new))
                        .collect();
                    let empty = Arc::new(TreeNode::default());
                    let take = |t: Option<Hash>| match t.and_then(|t| nodes.get(&t)) {
                        Some(n) => n.clone(),
                        None => empty.clone(),
                    };
                    for (path, (old_dir, new_dir)) in batch {
                        let (a, b) = (take(old_dir), take(new_dir));
                        let names: BTreeSet<&String> =
                            a.entries.keys().chain(b.entries.keys()).collect();
                        for name in names {
                            add(
                                &mut q,
                                join(&path, name),
                                a.entries.get(name).cloned(),
                                b.entries.get(name).cloned(),
                            )?;
                        }
                    }
                }
            }
        }
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

    /// One page of [`Repo::diff`], with renames detected when `renames` is
    /// set: at most `limit` entries after the `after` cursor, with `next`
    /// when more remain. `limit: None` is the whole rest of the diff;
    /// `Some(0)` is `Invalid`.
    ///
    /// Without renames the walk stops after the page. With them, pairing
    /// needs every deleted and added file, so each page walks the whole diff
    /// and detects over all of it (pairs never depend on page size), then
    /// keeps the renames whose `to` is on the page.
    pub async fn diff_page(
        &self,
        from: Option<ChangesetId>,
        to: ChangesetId,
        prefix: &str,
        renames: Option<&RenameOptions>,
        limit: Option<usize>,
        after: Option<&str>,
    ) -> Result<DiffResponse> {
        let prefix = prefix.trim_matches('/');
        let fp = Fingerprint::of(
            CursorKind::Diff,
            &(from, to, prefix, renames.map(|o| o.threshold)),
        );
        let after = page_start(limit, after, CursorKind::Diff, fp)?;
        let from_tree = match from {
            Some(f) => self.root_tree(f).await?,
            None => TreeNode::empty_hash(),
        };
        let to_tree = self.root_tree(to).await?;
        let want = limit.map_or(usize::MAX, |l| l.saturating_add(1));
        let (mut entries, found) = match renames {
            None => {
                let page = self
                    .diff_trees_after(from_tree, to_tree, prefix, after.as_deref(), want)
                    .await?;
                (page, Detected::default())
            }
            Some(opts) => {
                let all = self.diff_trees(from_tree, to_tree, prefix).await?;
                let page: Vec<DiffEntry> = all
                    .iter()
                    .filter(|e| after.as_deref().is_none_or(|a| e.path.as_str() > a))
                    .take(want)
                    .cloned()
                    .collect();
                // A page past the end has no renames to keep.
                let found = if page.is_empty() {
                    Detected::default()
                } else {
                    self.detect_renames(&all, opts).await?
                };
                (page, found)
            }
        };
        let next = match limit {
            Some(l) if entries.len() > l => {
                entries.truncate(l);
                Some(path_cursor(
                    CursorKind::Diff,
                    fp,
                    entries[l - 1].path.as_str(),
                )?)
            }
            _ => None,
        };
        let on_page: BTreeSet<&RepoPath> = entries.iter().map(|e| &e.path).collect();
        let renames = found
            .renames
            .into_iter()
            .filter(|r| on_page.contains(&r.to))
            .collect();
        Ok(DiffResponse {
            renames,
            renames_truncated: found.truncated,
            entries,
            next,
        })
    }
}

/// Where a page of a path-ordered listing starts: `after`'s path, or the
/// start. A `limit` of 0 would page forever, so it is `Invalid`.
fn page_start(
    limit: Option<usize>,
    after: Option<&str>,
    kind: CursorKind,
    fp: Fingerprint,
) -> Result<Option<String>> {
    if limit == Some(0) {
        return Err(Error::Invalid("limit must be at least 1".into()));
    }
    Ok(after
        .map(|t| cursor::decode::<PathPos>(t, kind, fp))
        .transpose()?
        .map(|p| p.path))
}

/// Where a directory sorts among paths: its path and `/`, so it sorts with
/// its descendants. A name never contains `/`, so between two children of
/// one directory, everything under the smaller key sorts before everything
/// under the larger (`a.txt` < `a/b`, as `.` < `/`). The walk's root is `""`
/// and sorts first.
fn dir_key(path: &str) -> String {
    if path.is_empty() {
        String::new()
    } else {
        format!("{path}/")
    }
}

/// Whether the directory with key `key` holds any path after `after`: its
/// paths all start with `key`, so only when `after` sorts before `key` or
/// lies inside it.
fn dir_follows(key: &str, after: Option<&str>) -> bool {
    after.is_none_or(|a| a < key || a.starts_with(key))
}

/// The pending work of a walk that yields in path order: items ready to
/// yield, keyed by path, and directories not yet read, keyed by
/// [`dir_key`]. Whatever is smallest across both comes next, so the walk
/// never sorts at the end and can stop as soon as it has enough. Items and
/// directories that hold nothing after `after` are dropped on insert.
struct PathQueue<T, D> {
    ready: BTreeMap<String, T>,
    dirs: BTreeMap<String, (String, D)>,
    after: Option<String>,
}

enum Next<T> {
    Ready(T),
    /// The next item is under an unread directory.
    Read,
    Done,
}

impl<T, D> PathQueue<T, D> {
    fn new(after: Option<&str>) -> Self {
        PathQueue {
            ready: BTreeMap::new(),
            dirs: BTreeMap::new(),
            after: after.map(str::to_string),
        }
    }

    /// Whether an item at `path` belongs in the walk.
    fn wants(&self, path: &str) -> bool {
        self.after.as_deref().is_none_or(|a| path > a)
    }

    fn dir(&mut self, path: String, d: D) {
        let key = dir_key(&path);
        if dir_follows(&key, self.after.as_deref()) {
            self.dirs.insert(key, (path, d));
        }
    }

    fn next(&mut self) -> Next<T> {
        let read = match (self.ready.first_key_value(), self.dirs.first_key_value()) {
            (None, None) => return Next::Done,
            (Some((r, _)), Some((d, _))) => d < r,
            (r, _) => r.is_none(),
        };
        if read {
            return Next::Read;
        }
        self.ready
            .pop_first()
            .map_or(Next::Done, |(_, item)| Next::Ready(item))
    }

    /// The `n` smallest unread directories, as `(path, payload)`. Reading
    /// them in one batch is always safe: their children sort after them.
    fn take_dirs(&mut self, n: usize) -> Vec<(String, D)> {
        std::iter::from_fn(|| self.dirs.pop_first())
            .take(n.max(1))
            .map(|(_, d)| d)
            .collect()
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

    /// Every entry in name order.
    pub(crate) fn entries(&self) -> impl Iterator<Item = Result<(&str, TreeEntry)>> + '_ {
        self.index.iter().map(|&(at, len, entry)| {
            // `scan` checked the name is UTF-8.
            let name = std::str::from_utf8(&self.bytes[at as usize..(at + len) as usize])
                .map_err(|e| Error::Encoding(e.to_string()))?;
            let entry = postcard::take_from_bytes::<TreeEntry>(&self.bytes[entry as usize..])?.0;
            Ok((name, entry))
        })
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
