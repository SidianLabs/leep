//! Three-way tree merge, plus `merge` and `rebase` built on it.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use leep_core::api::{Conflict, DiffEntry, FileInfo, MergeRequest, MergeResponse, RebaseResponse};
use leep_core::changeset::EXTRA_REBASED_FROM;
use leep_core::{
    Changeset, ChangesetId, Error, FileChange, FileKind, Hash, RepoPath, Result, TreeEntry,
    TreeNode,
};

use crate::changeset_gc::RescueBy;
use crate::{ConflictPolicy, Repo};

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
            .read_content(&i.content)
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
                // A merge result the blob store would refuse (ADR 0041) is
                // left to the caller to resolve.
                Ok(merged) if leep_core::hash::is_object_preimage(merged.as_bytes()) => {
                    return Ok(Resolved::Conflict(None));
                }
                Ok(merged) => {
                    let bytes = Bytes::from(merged.into_bytes());
                    let size = bytes.len() as u64;
                    let content = self.put_blob(bytes).await?;
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
    /// `at` holds `ours`' entry at `p` and at each of its directories.
    async fn collides(
        &self,
        at: &HashMap<String, Option<TreeEntry>>,
        p: &RepoPath,
        changes: &BTreeMap<RepoPath, FileChange>,
    ) -> Result<bool> {
        let mut prefix = String::new();
        for comp in p.components() {
            if !prefix.is_empty() {
                prefix.push('/');
            }
            prefix.push_str(comp);
            let entry = at.get(&prefix).cloned().flatten();
            if prefix == p.as_str() {
                let Some(TreeEntry::Dir { tree }) = entry else {
                    return Ok(false);
                };
                // The file fits only if every file `ours` has below `p` goes:
                // `ours` may have added some there since the base.
                let below = self.diff_trees(TreeNode::empty_hash(), tree, "").await?;
                return Ok(below.iter().any(|e| {
                    let path = format!("{}/{}", p.as_str(), e.path.as_str());
                    !RepoPath::new(path)
                        .ok()
                        .is_some_and(|q| changes.get(&q) == Some(&FileChange::Delete))
                }));
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

    /// `ours`' entries at `p` and its directories, one `entry_at` each: the
    /// oracle for the batched read.
    #[cfg(test)]
    async fn walk_entries(
        &self,
        ours: Hash,
        p: &RepoPath,
    ) -> Result<HashMap<String, Option<TreeEntry>>> {
        let mut out = HashMap::new();
        let s = p.as_str();
        for k in s.match_indices('/').map(|(i, _)| &s[..i]).chain([s]) {
            out.insert(k.to_string(), self.entry_at(ours, k).await?);
        }
        Ok(out)
    }

    /// Three-way merge of trees; `changes` apply on top of `ours`.
    ///
    /// Only `theirs` is diffed against `base`. `ours` is read at the paths
    /// that diff names and nowhere else, so a merge costs the size of
    /// `theirs`' change, not of everything `ours` did since `base` (a land
    /// onto a busy ref). The file `ours` has at a path is what a full diff
    /// from `base` would give there: the diff's new side where it lists
    /// the path, and otherwise `base`'s file, which `ours` still has.
    pub(crate) async fn merge_trees(
        &self,
        base: Hash,
        ours: Hash,
        theirs: Hash,
    ) -> Result<TreeMerge> {
        self.merge_trees_as(base, ours, theirs, ConflictPolicy::Merge)
            .await
    }

    /// [`Repo::merge_trees`] under `policy`: [`ConflictPolicy::Strict`]
    /// answers every path both sides changed since `base` as a conflict,
    /// even one a three-way merge would resolve (ADR 0066).
    pub(crate) async fn merge_trees_as(
        &self,
        base: Hash,
        ours: Hash,
        theirs: Hash,
        policy: ConflictPolicy,
    ) -> Result<TreeMerge> {
        let theirs_diff = self.diff_trees(base, theirs, "").await?;
        let paths: Vec<&RepoPath> = theirs_diff.iter().map(|e| &e.path).collect();
        let ours_at: HashMap<RepoPath, Option<FileInfo>> = if ours == base {
            HashMap::new()
        } else {
            self.files_at(ours, &paths).await?
        };
        self.resolve_merge(ours, theirs_diff, ours_at, policy).await
    }

    /// [`Repo::merge_trees`] the way it was before it read `ours` only at
    /// `theirs`' paths: from a diff of the whole of `ours` against `base`.
    /// The oracle the restricted read is checked against.
    #[cfg(test)]
    pub(crate) async fn merge_trees_full(
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
        let theirs_diff = self.diff_trees(base, theirs, "").await?;
        let ours_at = theirs_diff
            .iter()
            .filter_map(|e| Some((e.path.clone(), ours_diff.get(&e.path)?.clone())))
            .collect();
        self.resolve_merge(ours, theirs_diff, ours_at, ConflictPolicy::Merge)
            .await
    }

    /// Resolve each entry of `theirs_diff` against `ours`' file at its path
    /// (`ours_at`; a path missing there is unchanged from `base`).
    async fn resolve_merge(
        &self,
        ours: Hash,
        theirs_diff: Vec<DiffEntry>,
        mut ours_at: HashMap<RepoPath, Option<FileInfo>>,
        policy: ConflictPolicy,
    ) -> Result<TreeMerge> {
        let mut changes = BTreeMap::new();
        let mut conflicts = Vec::new();
        let mut taken = Vec::new();
        for e in theirs_diff {
            let our_info = match ours_at.remove(&e.path) {
                Some(n) => n,
                None => e.old.clone(),
            };
            if policy == ConflictPolicy::Strict && our_info != e.old {
                conflicts.push(Conflict {
                    path: e.path,
                    base: e.old,
                    ours: our_info,
                    theirs: e.new,
                    merged_with_markers: None,
                });
                continue;
            }
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
        // `ours`' entries at every modified path and its directories, in
        // one batched read per level, for the collision checks.
        let mut wanted: BTreeSet<&str> = BTreeSet::new();
        for (path, ..) in &taken {
            if matches!(changes.get(path), Some(FileChange::Modify { .. })) {
                let s = path.as_str();
                wanted.extend(s.match_indices('/').map(|(i, _)| &s[..i]));
                wanted.insert(s);
            }
        }
        let wanted: Vec<&str> = wanted.into_iter().collect();
        let at = if wanted.is_empty() {
            HashMap::new()
        } else {
            self.entries_at(ours, &wanted).await?
        };
        for (path, old, our_info, new) in taken {
            let modify = matches!(changes.get(&path), Some(FileChange::Modify { .. }));
            // Under test, every check is made both ways: from the batched
            // entries and by walking `ours` from its root.
            #[cfg(test)]
            if modify {
                let walked = self.walk_entries(ours, &path).await?;
                assert_eq!(
                    self.collides(&at, &path, &changes).await?,
                    self.collides(&walked, &path, &changes).await?,
                    "{path}"
                );
            }
            if modify && self.collides(&at, &path, &changes).await? {
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

    /// Three-way merge of changesets `ours` and `theirs` against their merge
    /// bases. With one base this is [`Repo::merge_trees`]. In a criss-cross
    /// there are several, and the merge is made against the first, then
    /// every path the bases disagree on is resolved against each of them:
    /// a path whose answers differ is a conflict, though any one base would
    /// have taken a side's version without one (ADR 0071).
    pub(crate) async fn merge_changesets(
        &self,
        ours: ChangesetId,
        theirs: ChangesetId,
    ) -> Result<TreeMerge> {
        let bases = self.merge_bases(ours, theirs).await?;
        let (ours, theirs) = (self.root_tree(ours).await?, self.root_tree(theirs).await?);
        let first = self.tree_or_empty(bases.first().copied()).await?;
        let mut m = self.merge_trees(first, ours, theirs).await?;
        if bases.len() > 1 {
            let mut trees = vec![first];
            for &b in &bases[1..] {
                trees.push(self.root_tree(b).await?);
            }
            self.reconcile_bases(&trees, ours, theirs, &mut m).await?;
        }
        Ok(m)
    }

    /// Make `m`, merged against `bases[0]`, a conflict at every path where
    /// the merge against another of `bases` would end differently. A path
    /// all bases agree on resolves the same against each, so only paths some
    /// base changes relative to `bases[0]` are looked at.
    async fn reconcile_bases(
        &self,
        bases: &[Hash],
        ours: Hash,
        theirs: Hash,
        m: &mut TreeMerge,
    ) -> Result<()> {
        let mut disputed: BTreeSet<RepoPath> = BTreeSet::new();
        for &b in &bases[1..] {
            disputed.extend(
                self.diff_trees(bases[0], b, "")
                    .await?
                    .into_iter()
                    .map(|e| e.path),
            );
        }
        if disputed.is_empty() {
            return Ok(());
        }
        let paths: Vec<&RepoPath> = disputed.iter().collect();
        let ours_at = self.files_at(ours, &paths).await?;
        let theirs_at = self.files_at(theirs, &paths).await?;
        let mut bases_at = Vec::with_capacity(bases.len());
        for &b in bases {
            bases_at.push(self.files_at(b, &paths).await?);
        }
        let key = |f: &Option<FileInfo>| f.as_ref().map(|f| (f.content, f.kind));
        for path in disputed {
            let (o, t) = (&ours_at[&path], &theirs_at[&path]);
            // What the path ends as against each base; `None` for a conflict.
            let mut outcome = None;
            let mut agree = true;
            for at in &bases_at {
                let end = match self.resolve3(&at[&path], o, t).await? {
                    Resolved::Keep => Some(key(o)),
                    Resolved::Set(FileChange::Modify { content, kind, .. }) => {
                        Some(Some((content, kind)))
                    }
                    Resolved::Set(FileChange::Delete) => Some(None),
                    Resolved::Conflict(_) => None,
                };
                match (&outcome, end) {
                    (_, None) => {
                        agree = false;
                        break;
                    }
                    (None, Some(e)) => outcome = Some(e),
                    (Some(prev), Some(e)) if *prev != e => {
                        agree = false;
                        break;
                    }
                    _ => {}
                }
            }
            if agree {
                continue;
            }
            m.changes.remove(&path);
            m.conflicts.retain(|c| c.path != path);
            m.conflicts.push(Conflict {
                path: path.clone(),
                base: bases_at[0][&path].clone(),
                ours: o.clone(),
                theirs: t.clone(),
                merged_with_markers: None,
            });
        }
        m.conflicts.sort_by(|a, b| a.path.cmp(&b.path));
        Ok(())
    }

    /// Three-way merge of two changesets into a new merge changeset with
    /// parents `[ours, theirs]`, or the list of conflicts.
    pub async fn merge(&self, req: MergeRequest) -> Result<MergeResponse> {
        self.merge_by(req, None).await
    }

    /// [`Repo::merge`] for an authenticated caller: the merge is stored with
    /// a `created-by-v1` row naming `principal`, written only if the merge
    /// is new, as `POST /v1/changesets` does (ADR 0086). Without it a token
    /// could make a changeset through a merge and then attest it (ADR 0091).
    pub async fn merge_by(
        &self,
        req: MergeRequest,
        principal: Option<&str>,
    ) -> Result<MergeResponse> {
        let [ours, theirs] = req.parents;
        self.rescue_changesets(&[ours, theirs], RescueBy::Create)
            .await?;
        let m = self.merge_changesets(ours, theirs).await?;
        if !m.conflicts.is_empty() {
            return Ok(MergeResponse::Conflicts {
                conflicts: m.conflicts,
                clean: m.changes,
            });
        }
        let cs = Changeset {
            parents: vec![ours, theirs],
            author: req.authorship.author,
            timestamp: req.authorship.timestamp.unwrap_or_else(now),
            message: req.message,
            changes: m.changes,
            extra: req.extra,
        };
        let id = match principal {
            Some(p) => {
                let if_new = vec![crate::review_requests::created_by_row(cs.id(), p)];
                self.store_changeset(cs, false, Vec::new(), if_new).await?
            }
            None => self.store_server_changeset(cs).await?,
        };
        Ok(MergeResponse::Merged { id })
    }

    /// Replay `cs`'s changes (relative to its first parent) onto `onto`.
    pub async fn rebase(&self, cs: ChangesetId, onto: ChangesetId) -> Result<RebaseResponse> {
        self.rebase_as(cs, onto, ConflictPolicy::Merge).await
    }

    /// [`Repo::rebase`] with `policy` deciding same-path overlaps.
    pub(crate) async fn rebase_as(
        &self,
        cs: ChangesetId,
        onto: ChangesetId,
        policy: ConflictPolicy,
    ) -> Result<RebaseResponse> {
        // The copy's link names `cs`, and its parent is `onto`.
        self.rescue_changesets(&[cs, onto], RescueBy::Create)
            .await?;
        let orig = self.get_changeset(cs).await?;
        if orig.parents.len() > 1 {
            return Err(Error::Invalid("cannot rebase a merge changeset".into()));
        }
        if orig.parents.first() == Some(&onto) {
            return Ok(RebaseResponse::Rebased { id: cs });
        }
        self.get_changeset(onto).await?;
        let m = self
            .merge_trees_as(
                self.tree_or_empty(orig.parents.first().copied()).await?,
                self.root_tree(onto).await?,
                self.root_tree(cs).await?,
                policy,
            )
            .await?;
        if !m.conflicts.is_empty() {
            return Ok(RebaseResponse::Conflicts {
                conflicts: m.conflicts,
                clean: m.changes,
            });
        }
        let id = self.rebased_copy(cs, onto, m.changes).await?;
        Ok(RebaseResponse::Rebased { id })
    }

    /// Store `cs`'s copy on `onto` with `changes`, linked to `cs`. The link is
    /// written with the copy, so a copy never exists without it (on stores
    /// with atomic writes) and is never linked before it exists (on any
    /// store). `changes` may be empty: `onto` already has what `cs` does.
    pub(crate) async fn rebased_copy(
        &self,
        cs: ChangesetId,
        onto: ChangesetId,
        changes: BTreeMap<RepoPath, FileChange>,
    ) -> Result<ChangesetId> {
        let orig = self.get_changeset(cs).await?;
        let mut extra = orig.extra;
        extra.insert(EXTRA_REBASED_FROM.into(), cs.to_string());
        let copy = Changeset {
            parents: vec![onto],
            author: orig.author,
            timestamp: orig.timestamp,
            message: orig.message,
            changes,
            extra,
        };
        let link = Self::rebase_link(copy.id(), cs);
        self.store_server_changeset_with_rows(copy, vec![link])
            .await
    }
}
