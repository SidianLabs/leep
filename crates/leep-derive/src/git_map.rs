//! Git ids of Leep objects (ADR 0090): what a git client would call each
//! blob, tree and changeset, and the way back from a git id.
//!
//! Every row is a pure function of content and the store's commit style, so
//! a cold store derives the same ids an import or the mirror recorded, with
//! no backfill. `git-commit-v1` is a per-changeset kind; blob and tree ids
//! are keyed by the Leep hash they describe; `git-oid-v1` is the reverse
//! index, written beside each forward row.
//!
//! These are not the bridge's `leep-to-git-v1`/`git-to-leep-v1` records:
//! those name the commit an import or a mirror actually used, keep their
//! changesets alive through garbage collection (ADR 0059), and cannot be
//! recomputed for a commit the export does not reproduce (one with
//! submodules, say).

use std::collections::{HashMap, HashSet};

use async_trait::async_trait;
use bytes::Bytes;
use leep_core::{Changeset, ChangesetId, DerivedRow, Error, Hash, Result, TreeEntry, TreeNode};
use leep_gitobj::{CommitStyle, Entry, EntryKind, ObjectId};

use crate::derived::{self, DerivedKind};
use crate::Repo;

/// Blob hash -> 20-byte git blob id.
pub const GIT_BLOB_KIND: &str = "git-blob-v1";
/// Tree node hash -> 20-byte git tree id.
pub const GIT_TREE_KIND: &str = "git-tree-v1";
/// Changeset id -> 20-byte git commit id, under the store's commit style.
pub const GIT_COMMIT_KIND: &str = "git-commit-v1";
/// `Hash::of_object("git-oid", sha)` -> a kind byte (see [`GitObject`]) and
/// the 32-byte Leep hash of the object with that git id.
pub const GIT_OID_KIND: &str = "git-oid-v1";
/// One row at [`style_key`]: the store's [`CommitStyle`], encoded as text.
/// Written once; the commit ids above depend on it.
pub const GIT_STYLE_KIND: &str = "git-style-v1";

/// Blobs read per batch while their git ids are computed, and rows per
/// batched derived read (as the derive framework's walks use).
const BATCH: usize = 256;
/// Blob bytes one batch holds at most; a blob larger than this is read on
/// its own.
const BATCH_BYTES: u64 = 64 << 20;

/// What a git id names in Leep.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GitObject {
    Blob(Hash),
    Tree(Hash),
    Commit(ChangesetId),
}

impl GitObject {
    fn encode(self) -> Bytes {
        let (tag, h) = match self {
            GitObject::Blob(h) => (1u8, h),
            GitObject::Tree(h) => (2, h),
            GitObject::Commit(id) => (3, id.0),
        };
        let mut v = Vec::with_capacity(33);
        v.push(tag);
        v.extend_from_slice(h.as_bytes());
        Bytes::from(v)
    }

    fn decode(b: &[u8]) -> Result<GitObject> {
        let bad = || Error::Encoding(format!("bad {GIT_OID_KIND} value"));
        if b.len() != 33 {
            return Err(bad());
        }
        let h = Hash(b[1..].try_into().map_err(|_| bad())?);
        match b[0] {
            1 => Ok(GitObject::Blob(h)),
            2 => Ok(GitObject::Tree(h)),
            3 => Ok(GitObject::Commit(ChangesetId(h))),
            _ => Err(bad()),
        }
    }

    fn forward(self) -> (&'static str, Hash) {
        match self {
            GitObject::Blob(h) => (GIT_BLOB_KIND, h),
            GitObject::Tree(h) => (GIT_TREE_KIND, h),
            GitObject::Commit(id) => (GIT_COMMIT_KIND, id.0),
        }
    }
}

/// Key of the reverse row for git id `oid`.
pub fn oid_key(oid: &ObjectId) -> Hash {
    Hash::of_object("git-oid", oid.as_bytes())
}

/// Key of the store's one [`GIT_STYLE_KIND`] row.
pub fn style_key() -> Hash {
    Hash::of_object("git-style", b"")
}

pub(crate) fn oid_of(b: &[u8], kind: &str) -> Result<ObjectId> {
    ObjectId::try_from(b).map_err(|_| Error::Encoding(format!("bad {kind} value")))
}

fn rows_for(obj: GitObject, oid: &ObjectId) -> [DerivedRow; 2] {
    let (kind, key) = obj.forward();
    [
        DerivedRow::new(kind, key, Bytes::copy_from_slice(oid.as_bytes())),
        DerivedRow::new(GIT_OID_KIND, oid_key(oid), obj.encode()),
    ]
}

/// `git-commit-v1`: a changeset's git commit id, from its root tree's git
/// id, its parents' commit ids and the store's commit style.
pub struct GitCommit;

#[async_trait]
impl DerivedKind for GitCommit {
    const NAME: &'static str = GIT_COMMIT_KIND;

    async fn derive(
        repo: &Repo,
        id: ChangesetId,
        cs: &Changeset,
        parents: &[Bytes],
    ) -> Result<Bytes> {
        let style = repo.git_style_or_default().await?;
        let root = repo.root_tree(id).await?;
        let tree = repo.git_tree_id(root).await?;
        let parents = parents
            .iter()
            .map(|p| oid_of(p, GIT_COMMIT_KIND))
            .collect::<Result<Vec<_>>>()?;
        let commit = leep_gitobj::commit_for(cs, id, tree, parents, style)?;
        let oid = leep_gitobj::commit_id(&commit)?;
        // Two changesets can share a commit (under `plain`, ones that differ
        // only in extras no commit shows), so the reverse row is written
        // once: it names the first changeset derived to that commit.
        let [_, reverse] = rows_for(GitObject::Commit(id), &oid);
        repo.meta()
            .put_derived_if_absent(&reverse.kind, &reverse.key, reverse.value)
            .await?;
        Ok(Bytes::copy_from_slice(oid.as_bytes()))
    }
}

impl Repo {
    /// The store's commit style, if one was recorded.
    pub async fn git_style(&self) -> Result<Option<CommitStyle>> {
        if let Some(s) = self.git_style.get() {
            return Ok(Some(*s));
        }
        let Some(v) = self
            .meta()
            .get_derived(GIT_STYLE_KIND, &style_key())
            .await?
        else {
            return Ok(None);
        };
        let style = decode_style(&v)?;
        // Written once, so it can be kept for the life of the handle.
        let _ = self.git_style.set(style);
        Ok(Some(style))
    }

    /// Record `style` as the store's commit style, or check it is the one
    /// recorded. The style is chosen once: a store whose changesets already
    /// have commit ids under one style would give them second ids under
    /// another, so a different `style` is a conflict. Of concurrent first
    /// writers exactly one style is stored, and every other one is refused.
    pub async fn set_git_style(&self, style: CommitStyle) -> Result<CommitStyle> {
        let conflict = |have: CommitStyle| {
            Error::Conflict(format!(
                "this store's git commit style is {:?}, not {:?}; it is chosen once \
                 (ADR 0090): change the flags to match it, or drop the recorded style \
                 with `leep-server repair drop --kind {GIT_STYLE_KIND}`, which drops \
                 every git id derived under it",
                have.encode(),
                style.encode()
            ))
        };
        if let Some(have) = self.git_style().await? {
            return if have == style {
                Ok(have)
            } else {
                Err(conflict(have))
            };
        }
        let won = self
            .meta()
            .put_derived_if_absent(
                GIT_STYLE_KIND,
                &style_key(),
                Bytes::from(style.encode().into_bytes()),
            )
            .await?;
        let have = decode_style(&won)?;
        if have != style {
            return Err(conflict(have));
        }
        let _ = self.git_style.set(have);
        Ok(have)
    }

    /// The recorded style, recording the default (the mirror's) first when
    /// there is none: the first commit id derived fixes it.
    pub async fn git_style_or_default(&self) -> Result<CommitStyle> {
        match self.git_style().await? {
            Some(s) => Ok(s),
            None => self.set_git_style(CommitStyle::default()).await,
        }
    }

    /// Git blob ids of `blobs`, in order, computing and storing the missing
    /// ones.
    pub async fn git_blob_ids(&self, blobs: &[Hash]) -> Result<Vec<ObjectId>> {
        let sized: Vec<(Hash, u64)> = blobs.iter().map(|h| (*h, 0)).collect();
        self.git_blob_ids_sized(&sized).await
    }

    /// [`Repo::git_blob_ids`] of blobs whose sizes are known (0 when not), so
    /// a batch of reads stays under [`BATCH_BYTES`].
    async fn git_blob_ids_sized(&self, blobs: &[(Hash, u64)]) -> Result<Vec<ObjectId>> {
        let keys: Vec<Hash> = blobs.iter().map(|(h, _)| *h).collect();
        let mut out: Vec<Option<ObjectId>> = Vec::with_capacity(blobs.len());
        for chunk in keys.chunks(BATCH) {
            for r in self.meta().get_derived_many(GIT_BLOB_KIND, chunk).await? {
                out.push(r.map(|v| oid_of(&v, GIT_BLOB_KIND)).transpose()?);
            }
        }
        let mut missing: Vec<(Hash, u64)> = blobs
            .iter()
            .zip(&out)
            .filter(|(_, o)| o.is_none())
            .map(|(b, _)| *b)
            .collect();
        if missing.is_empty() {
            return Ok(out.into_iter().flatten().collect());
        }
        missing.sort();
        missing.dedup_by_key(|(h, _)| *h);
        let mut found: HashMap<Hash, ObjectId> = HashMap::new();
        let mut at = 0;
        while at < missing.len() {
            let (mut end, mut bytes) = (at, 0u64);
            while end < missing.len() && end - at < BATCH && (end == at || bytes < BATCH_BYTES) {
                bytes += missing[end].1;
                end += 1;
            }
            let chunk: Vec<Hash> = missing[at..end].iter().map(|(h, _)| *h).collect();
            at = end;
            let data = self.blobs().get_many(&chunk).await?;
            let mut rows = Vec::with_capacity(chunk.len() * 2);
            for (h, d) in chunk.iter().zip(data) {
                let d = d.ok_or_else(|| Error::NotFound(format!("blob {h}")))?;
                let oid = leep_gitobj::blob_id(&d)?;
                // A blob's reverse row is a function of its key (one content,
                // one sha1), so racing writers write the same bytes.
                rows.extend(rows_for(GitObject::Blob(*h), &oid));
                found.insert(*h, oid);
            }
            self.meta().put_derived_many(&rows).await?;
        }
        Ok(keys
            .iter()
            .zip(out)
            .map(|(h, o)| o.unwrap_or_else(|| found[h]))
            .collect())
    }

    /// The git id of tree node `tree`, computing and storing the ids of it
    /// and of every subtree and blob below it that lacks one. Walks with an
    /// explicit stack, children before parents. A subtree with no files
    /// (which Leep does not normally keep) is left out, as git would.
    pub async fn git_tree_id(&self, tree: Hash) -> Result<ObjectId> {
        if let Some(v) = self.meta().get_derived(GIT_TREE_KIND, &tree).await? {
            return oid_of(&v, GIT_TREE_KIND);
        }
        let empty = leep_gitobj::empty_tree_id();
        let mut done: HashMap<Hash, ObjectId> = HashMap::new();
        let mut nodes: HashMap<Hash, std::sync::Arc<TreeNode>> = HashMap::new();
        let mut queued: HashSet<Hash> = HashSet::new();
        let mut stack: Vec<(Hash, bool)> = vec![(tree, false)];
        while let Some((h, expanded)) = stack.pop() {
            if done.contains_key(&h) {
                continue;
            }
            if !expanded {
                if !queued.insert(h) {
                    continue;
                }
                let node = self.read_tree_shared(h).await?;
                let dirs: Vec<Hash> = node
                    .entries
                    .values()
                    .filter_map(|e| match e {
                        TreeEntry::Dir { tree } => Some(*tree),
                        TreeEntry::File { .. } => None,
                    })
                    .filter(|d| !done.contains_key(d))
                    .collect();
                stack.push((h, true));
                for chunk in dirs.chunks(BATCH) {
                    let known = self.meta().get_derived_many(GIT_TREE_KIND, chunk).await?;
                    for (d, k) in chunk.iter().zip(known) {
                        match k {
                            Some(v) => {
                                done.insert(*d, oid_of(&v, GIT_TREE_KIND)?);
                            }
                            None => stack.push((*d, false)),
                        }
                    }
                }
                nodes.insert(h, node);
                continue;
            }
            let node = nodes.remove(&h).expect("expanded nodes were read");
            let files: Vec<(Hash, u64)> = node
                .entries
                .values()
                .filter_map(|e| match e {
                    TreeEntry::File { content, size, .. } => Some((*content, *size)),
                    TreeEntry::Dir { .. } => None,
                })
                .collect();
            let blob_ids = self.git_blob_ids_sized(&files).await?;
            let mut blob_ids = blob_ids.into_iter();
            let mut entries = Vec::with_capacity(node.entries.len());
            for (name, e) in &node.entries {
                let (mode, oid) = match e {
                    TreeEntry::File { kind, .. } => (
                        leep_gitobj::entry_kind(*kind),
                        blob_ids.next().expect("one id per file"),
                    ),
                    TreeEntry::Dir { tree } => {
                        let oid = done[tree];
                        if oid == empty {
                            continue;
                        }
                        (EntryKind::Tree, oid)
                    }
                };
                entries.push(Entry {
                    mode: mode.into(),
                    filename: name.as_str().into(),
                    oid,
                });
            }
            let oid = leep_gitobj::tree_id(&leep_gitobj::tree_object(entries)?)?;
            self.meta()
                .put_derived_many(&rows_for(GitObject::Tree(h), &oid))
                .await?;
            done.insert(h, oid);
        }
        Ok(done[&tree])
    }

    /// The git commit id of changeset `id` under the store's commit style,
    /// deriving it (and its ancestors') if needed.
    pub async fn git_commit_id(&self, id: ChangesetId) -> Result<ObjectId> {
        let v = derived::derive::<GitCommit>(self, id).await?;
        oid_of(&v, GIT_COMMIT_KIND)
    }

    /// What git id `oid` names here, if a forward row for it has been
    /// derived. A reverse row whose forward row is gone (a changeset sweep
    /// deleted the changeset, ADR 0059) or names another id answers None.
    pub async fn git_object(&self, oid: &ObjectId) -> Result<Option<GitObject>> {
        let Some(v) = self.meta().get_derived(GIT_OID_KIND, &oid_key(oid)).await? else {
            return Ok(None);
        };
        let obj = GitObject::decode(&v)?;
        let (kind, key) = obj.forward();
        let Some(f) = self.meta().get_derived(kind, &key).await? else {
            return Ok(None);
        };
        Ok((oid_of(&f, kind)? == *oid).then_some(obj))
    }
}

impl Repo {
    /// The git id tree node `tree` should have, from the stored ids of the
    /// trees and blobs it names, without deriving anything; None when one of
    /// those is not stored. What `verify` compares a stored row with.
    pub(crate) async fn expected_git_tree_id(&self, tree: Hash) -> Result<Option<ObjectId>> {
        let node = self.read_tree_shared(tree).await?;
        let mut rows: HashMap<(&'static str, Hash), Option<ObjectId>> = HashMap::new();
        for kind in [GIT_BLOB_KIND, GIT_TREE_KIND] {
            let keys: Vec<Hash> = node
                .entries
                .values()
                .filter_map(|e| match (e, kind) {
                    (TreeEntry::File { content, .. }, GIT_BLOB_KIND) => Some(*content),
                    (TreeEntry::Dir { tree }, GIT_TREE_KIND) => Some(*tree),
                    _ => None,
                })
                .collect();
            for chunk in keys.chunks(BATCH) {
                let got = self.meta().get_derived_many(kind, chunk).await?;
                for (k, v) in chunk.iter().zip(got) {
                    rows.insert((kind, *k), v.map(|v| oid_of(&v, kind)).transpose()?);
                }
            }
        }
        let empty = leep_gitobj::empty_tree_id();
        let mut entries = Vec::with_capacity(node.entries.len());
        for (name, e) in &node.entries {
            let (kind, key) = match e {
                TreeEntry::File { content, .. } => (GIT_BLOB_KIND, *content),
                TreeEntry::Dir { tree } => (GIT_TREE_KIND, *tree),
            };
            let Some(oid) = rows[&(kind, key)] else {
                return Ok(None);
            };
            let mode = match e {
                TreeEntry::File { kind, .. } => leep_gitobj::entry_kind(*kind),
                TreeEntry::Dir { .. } if oid == empty => continue,
                TreeEntry::Dir { .. } => EntryKind::Tree,
            };
            entries.push(Entry {
                mode: mode.into(),
                filename: name.as_str().into(),
                oid,
            });
        }
        Ok(Some(leep_gitobj::tree_id(&leep_gitobj::tree_object(
            entries,
        )?)?))
    }
}

/// Why a reverse row is wrong, or None when it is right or stale (its
/// forward row is gone, which a changeset sweep leaves behind).
pub(crate) async fn check_oid_row(repo: &Repo, key: &Hash, value: &[u8]) -> Result<Option<String>> {
    let obj = match GitObject::decode(value) {
        Ok(o) => o,
        Err(e) => return Ok(Some(e.to_string())),
    };
    let (kind, fkey) = obj.forward();
    let Some(f) = repo.meta().get_derived(kind, &fkey).await? else {
        return Ok(None);
    };
    let oid = match oid_of(&f, kind) {
        Ok(o) => o,
        Err(e) => return Ok(Some(e.to_string())),
    };
    Ok((oid_key(&oid) != *key).then(|| {
        format!("names {obj:?}, whose {kind} row is {oid}, which is not this row's git id")
    }))
}

/// Why the stored commit style is unreadable, if it is.
pub(crate) fn check_style_row(value: &[u8]) -> Option<String> {
    match std::str::from_utf8(value).map(CommitStyle::decode) {
        Ok(Ok(_)) => None,
        Ok(Err(e)) => Some(e.to_string()),
        Err(_) => Some("is not UTF-8".into()),
    }
}

fn decode_style(v: &[u8]) -> Result<CommitStyle> {
    let text = std::str::from_utf8(v)
        .map_err(|_| Error::Encoding(format!("bad {GIT_STYLE_KIND} value")))?;
    CommitStyle::decode(text)
}
