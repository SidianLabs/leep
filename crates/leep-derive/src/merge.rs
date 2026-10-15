//! Three-way tree merge, plus `merge` and `rebase` built on it.

use std::collections::{BTreeMap, HashMap};
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use leep_core::api::{Conflict, FileInfo, MergeRequest, MergeResponse, RebaseResponse};
use leep_core::changeset::EXTRA_REBASED_FROM;
use leep_core::{
    Changeset, ChangesetId, Error, FileChange, FileKind, Hash, RepoPath, Result, TreeEntry,
    TreeNode,
};

use crate::Repo;

/// Result of merging `theirs` into `ours`: `changes` are relative to `ours`.
pub(crate) struct TreeMerge {
    pub changes: BTreeMap<RepoPath, FileChange>,
    pub conflicts: Vec<Conflict>,
}

enum Resolved {
    Keep,
    Set(FileChange),
    Conflict(Option<String>),
}

fn same(a: &Option<FileInfo>, b: &Option<FileInfo>) -> bool {
    match (a, b) {
        (None, None) => true,
        (Some(a), Some(b)) => a.content == b.content && a.kind == b.kind,
        _ => false,
    }
}

fn as_change(i: &Option<FileInfo>) -> FileChange {
    match i {
        Some(i) => FileChange::Modify {
            content: i.content,
            kind: i.kind,
            size: i.size,
        },
        None => FileChange::Delete,
    }
}

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs() as i64)
}

impl Repo {
    async fn text(&self, info: Option<&FileInfo>) -> Result<Option<String>> {
        let Some(i) = info else {
            return Ok(Some(String::new()));
        };
        let data = self
            .blobs()
            .get(&i.content)
            .await?
            .ok_or_else(|| Error::Storage(format!("blob {} missing", i.content)))?;
        Ok(String::from_utf8(data.to_vec()).ok())
    }

    async fn resolve3(
        &self,
        base: &Option<FileInfo>,
        ours: &Option<FileInfo>,
        theirs: &Option<FileInfo>,
    ) -> Result<Resolved> {
        if same(ours, theirs) || same(theirs, base) {
            return Ok(Resolved::Keep);
        }
        if same(ours, base) {
            return Ok(Resolved::Set(as_change(theirs)));
        }
        let (Some(o), Some(t)) = (ours, theirs) else {
            return Ok(Resolved::Conflict(None));
        };
        // Both sides changed differently: resolve kind and content separately.
        let base_kind = base.as_ref().map(|b| b.kind);
        let kind = if o.kind == t.kind {
            o.kind
        } else if base_kind == Some(o.kind) {
            t.kind
        } else if base_kind == Some(t.kind) {
            o.kind
        } else {
            return Ok(Resolved::Conflict(None));
        };
        let base_content = base.as_ref().map(|b| b.content);
        let content = if o.content == t.content || base_content == Some(t.content) {
            o.content
        } else if base_content == Some(o.content) {
            t.content
        } else {
            if o.kind == FileKind::Symlink || t.kind == FileKind::Symlink {
                return Ok(Resolved::Conflict(None));
            }
            let (b, ot, th) = (
                self.text(base.as_ref()).await?,
                self.text(Some(o)).await?,
                self.text(Some(t)).await?,
            );
            let (Some(b), Some(ot), Some(th)) = (b, ot, th) else {
                return Ok(Resolved::Conflict(None));
            };
            match diffy::merge(&b, &ot, &th) {
                Ok(merged) => {
                    let bytes = Bytes::from(merged.into_bytes());
                    let size = bytes.len() as u64;
                    let content = self.blobs().put(bytes).await?;
                    return Ok(Resolved::Set(FileChange::Modify {
                        content,
                        kind,
                        size,
                    }));
                }
                Err(marked) => return Ok(Resolved::Conflict(Some(marked))),
            }
        };
        let size = if content == o.content { o.size } else { t.size };
        let new = FileChange::Modify {
            content,
            kind,
            size,
        };
        if new == as_change(ours) {
            Ok(Resolved::Keep)
        } else {
            Ok(Resolved::Set(new))
        }
    }

    /// Would setting `p` clash with a file/directory already in `ours`?
    async fn collides(
        &self,
        ours: Hash,
        p: &RepoPath,
        changes: &BTreeMap<RepoPath, FileChange>,
    ) -> Result<bool> {
        let mut prefix = String::new();
        for comp in p.components() {
            if !prefix.is_empty() {
                prefix.push('/');
            }
            prefix.push_str(comp);
            let entry = self.entry_at(ours, &prefix).await?;
            if prefix == p.as_str() {
                return Ok(matches!(entry, Some(TreeEntry::Dir { .. }))
                    && !changes
                        .iter()
                        .any(|(k, c)| k.is_within(p.as_str()) && matches!(c, FileChange::Delete)));
            }
            match entry {
                Some(TreeEntry::File { .. }) => {
                    let deleted = RepoPath::new(prefix.clone())
                        .ok()
                        .is_some_and(|q| changes.get(&q) == Some(&FileChange::Delete));
                    if !deleted {
                        return Ok(true);
                    }
                }
                Some(TreeEntry::Dir { .. }) => {}
                None => return Ok(false),
            }
        }
        Ok(false)
    }

    /// Three-way merge of trees; `changes` apply on top of `ours`.
    pub(crate) async fn merge_trees(
        &self,
        base: Hash,
        ours: Hash,
        theirs: Hash,
    ) -> Result<TreeMerge> {
        let ours_diff: HashMap<RepoPath, Option<FileInfo>> = self
            .diff_trees(base, ours, "")
            .await?
            .into_iter()
            .map(|e| (e.path, e.new))
            .collect();
        let mut changes = BTreeMap::new();
        let mut conflicts = Vec::new();
        let mut taken = Vec::new();
        for e in self.diff_trees(base, theirs, "").await? {
            let our_info = match ours_diff.get(&e.path) {
                Some(n) => n.clone(),
                None => e.old.clone(),
            };
            match self.resolve3(&e.old, &our_info, &e.new).await? {
                Resolved::Keep => {}
                Resolved::Set(c) => {
                    changes.insert(e.path.clone(), c);
                    taken.push((e.path, e.old, our_info, e.new));
                }
                Resolved::Conflict(m) => conflicts.push(Conflict {
                    path: e.path,
                    base: e.old,
                    ours: our_info,
                    theirs: e.new,
                    merged_with_markers: m,
                }),
            }
        }
        for (path, old, our_info, new) in taken {
            if matches!(changes.get(&path), Some(FileChange::Modify { .. }))
                && self.collides(ours, &path, &changes).await?
            {
                changes.remove(&path);
                conflicts.push(Conflict {
                    path,
                    base: old,
                    ours: our_info,
                    theirs: new,
                    merged_with_markers: None,
                });
            }
        }
        conflicts.sort_by(|a, b| a.path.cmp(&b.path));
        Ok(TreeMerge { changes, conflicts })
    }

    pub(crate) async fn tree_or_empty(&self, id: Option<ChangesetId>) -> Result<Hash> {
        match id {
            Some(id) => self.root_tree(id).await,
            None => Ok(TreeNode::empty_hash()),
        }
    }

    /// Three-way merge of two changesets into a new merge changeset with
    /// parents `[ours, theirs]`, or the list of conflicts.
    pub async fn merge(&self, req: MergeRequest) -> Result<MergeResponse> {
        let [ours, theirs] = req.parents;
        let base = self.merge_base(ours, theirs).await?;
        let m = self
            .merge_trees(
                self.tree_or_empty(base).await?,
                self.root_tree(ours).await?,
                self.root_tree(theirs).await?,
            )
            .await?;
        if !m.conflicts.is_empty() {
            return Ok(MergeResponse::Conflicts {
                conflicts: m.conflicts,
            });
        }
        let id = self
            .create_changeset(Changeset {
                parents: vec![ours, theirs],
                author: req.authorship.author,
                timestamp: req.authorship.timestamp.unwrap_or_else(now),
                message: req.message,
                changes: m.changes,
                extra: req.extra,
            })
            .await?;
        Ok(MergeResponse::Merged { id })
    }

    /// Replay `cs`'s changes (relative to its first parent) onto `onto`.
    pub async fn rebase(&self, cs: ChangesetId, onto: ChangesetId) -> Result<RebaseResponse> {
        let orig = self.get_changeset(cs).await?;
        if orig.parents.len() > 1 {
            return Err(Error::Invalid("cannot rebase a merge changeset".into()));
        }
        if orig.parents.first() == Some(&onto) {
            return Ok(RebaseResponse::Rebased { id: cs });
        }
        self.get_changeset(onto).await?;
        let m = self
            .merge_trees(
                self.tree_or_empty(orig.parents.first().copied()).await?,
                self.root_tree(onto).await?,
                self.root_tree(cs).await?,
            )
            .await?;
        if !m.conflicts.is_empty() {
            return Ok(RebaseResponse::Conflicts {
                conflicts: m.conflicts,
                clean: m.changes,
            });
        }
        let mut extra = orig.extra;
        extra.insert(EXTRA_REBASED_FROM.into(), cs.to_string());
        let id = self
            .create_changeset(Changeset {
                parents: vec![onto],
                author: orig.author,
                timestamp: orig.timestamp,
                message: orig.message,
                changes: m.changes,
                extra,
            })
            .await?;
        self.record_rebase(id, cs).await?;
        Ok(RebaseResponse::Rebased { id })
    }
}
