//! The [`Repo`] handle and changeset creation.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use leep_core::api::ChangesetInfo;
use leep_core::refname;
use leep_core::{
    BlobStore, Changeset, ChangesetId, Error, FileChange, Hash, MetaStore, RepoPath, Result,
    TreeNode,
};

use crate::derived::{self, Generation, RootTree, Skiplist};
use crate::signatures::{LandPolicy, PolicySet};
use crate::tree_ops::apply_changes;

/// Longest path, in bytes, a client changeset may name (Linux's `PATH_MAX`).
pub const MAX_PATH_BYTES: usize = 4096;
/// Most distinct directories a client changeset's paths may name, and their
/// total length. Building the new tree keeps every changed directory and its
/// full path in memory, so unbounded paths cost quadratic memory: one request
/// with a few megabytes of `a/a/a/...` could exhaust the server.
const MAX_CHANGED_DIRS: usize = 1_000_000;
const MAX_CHANGED_DIR_BYTES: usize = 256 << 20;

/// Refuse paths that would make tree building blow up (see the constants).
fn check_path_bounds<'a>(paths: impl Iterator<Item = &'a RepoPath>) -> Result<()> {
    let mut dirs = HashSet::new();
    let mut bytes = 0usize;
    for path in paths {
        let s = path.as_str();
        if s.len() > MAX_PATH_BYTES {
            return Err(Error::Invalid(format!(
                "path of {} bytes; the limit is {MAX_PATH_BYTES}",
                s.len()
            )));
        }
        for (i, _) in s.match_indices('/') {
            if dirs.insert(&s[..i]) {
                bytes += i;
                if dirs.len() > MAX_CHANGED_DIRS || bytes > MAX_CHANGED_DIR_BYTES {
                    return Err(Error::Invalid(format!(
                        "changeset touches too many directories (limit {MAX_CHANGED_DIRS} \
                         directories or {MAX_CHANGED_DIR_BYTES} bytes of directory names)"
                    )));
                }
            }
        }
    }
    Ok(())
}

/// A repository: content-addressed blobs plus changeset metadata.
#[derive(Clone)]
pub struct Repo {
    blobs: Arc<dyn BlobStore>,
    meta: Arc<dyn MetaStore>,
    /// Per-ref locks serializing lands within this process (see `land`).
    pub(crate) land_locks: Arc<Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>>,
    pub(crate) land_policy: Option<Arc<dyn LandPolicy>>,
}

impl Repo {
    pub fn new(blobs: Arc<dyn BlobStore>, meta: Arc<dyn MetaStore>) -> Self {
        Repo {
            blobs,
            meta,
            land_locks: Arc::default(),
            land_policy: None,
        }
    }

    /// Consult `policy` before every `land`. Called again, it adds to the
    /// policies already set; all must pass.
    pub fn with_land_policy(mut self, policy: Arc<dyn LandPolicy>) -> Self {
        self.land_policy = Some(match self.land_policy.take() {
            None => policy,
            Some(prev) => Arc::new(PolicySet::new(vec![prev, policy])),
        });
        self
    }

    /// The land policy in force, if any.
    pub fn land_policy(&self) -> Option<&Arc<dyn LandPolicy>> {
        self.land_policy.as_ref()
    }

    /// A handle on the same stores with `policy` instead of whatever was set.
    /// A server that reloads its policy gives each request a handle with the
    /// snapshot the request started under, so one `land` or ref move never
    /// sees two policies. Land locks stay shared with `self`.
    pub fn with_land_policy_replaced(&self, policy: Option<Arc<dyn LandPolicy>>) -> Self {
        Repo {
            land_policy: policy,
            ..self.clone()
        }
    }

    pub fn blobs(&self) -> &Arc<dyn BlobStore> {
        &self.blobs
    }

    pub fn meta(&self) -> &Arc<dyn MetaStore> {
        &self.meta
    }

    /// Validate and store a changeset, deriving its tree and generation.
    /// Idempotent. A blob that garbage collection has condemned counts as
    /// missing, so the client uploads it again (ADR 0012).
    pub async fn create_changeset(&self, cs: Changeset) -> Result<ChangesetId> {
        self.store_changeset(cs, true).await
    }

    /// Like [`Repo::create_changeset`] for changesets the server builds from
    /// stored changesets (merge, rebase): their blobs are referenced by a
    /// stored changeset, so GC keeps them, or were just written through
    /// [`Repo::put_blob`], so a condemnation is already answered.
    pub(crate) async fn store_server_changeset(&self, cs: Changeset) -> Result<ChangesetId> {
        self.store_changeset(cs, false).await
    }

    async fn store_changeset(&self, cs: Changeset, client_blobs: bool) -> Result<ChangesetId> {
        let id = cs.id();
        if client_blobs {
            check_path_bounds(cs.changes.keys())?;
        }
        for p in &cs.parents {
            if self.meta.get_changeset(p).await?.is_none() {
                return Err(Error::Invalid(format!("unknown parent {p}")));
            }
        }
        let mut seen = HashSet::new();
        for (path, change) in &cs.changes {
            let FileChange::Modify { content, size, .. } = change else {
                continue;
            };
            if !seen.insert(*content) {
                continue;
            }
            if client_blobs {
                self.check_reusable(content, path.as_str()).await?;
            }
            match self.blobs.get(content).await? {
                None => return Err(Error::Invalid(format!("missing blob {content} for {path}"))),
                Some(b) if b.len() as u64 != *size => {
                    return Err(Error::Invalid(format!(
                        "size mismatch for {path}: declared {size}, blob is {}",
                        b.len()
                    )))
                }
                Some(_) => {}
            }
            // `get` may be served from a cache tier; existence must come from
            // durable storage, which GC can change from another process.
            if !self.blobs.contains(content).await? {
                return Err(Error::Invalid(format!("missing blob {content} for {path}")));
            }
        }
        let base = match cs.parents.first() {
            Some(p) => self.root_tree(*p).await?,
            None => TreeNode::empty_hash(),
        };
        let tree = apply_changes(self, base, &cs.changes).await?;
        self.meta.put_changeset(&cs).await?;
        self.meta
            .put_derived(
                RootTree::NAME_STR,
                &id.0,
                Bytes::copy_from_slice(tree.as_bytes()),
            )
            .await?;
        derived::derive::<Generation>(self, id).await?;
        derived::derive::<Skiplist>(self, id).await?;
        Ok(id)
    }

    /// Fetch a changeset; `NotFound` if absent.
    pub async fn get_changeset(&self, id: ChangesetId) -> Result<Changeset> {
        self.meta
            .get_changeset(&id)
            .await?
            .ok_or_else(|| Error::NotFound(format!("changeset {id}")))
    }

    pub async fn info(&self, id: ChangesetId) -> Result<ChangesetInfo> {
        Ok(ChangesetInfo {
            id,
            changeset: self.get_changeset(id).await?,
            generation: self.generation(id).await?,
            root_tree: self.root_tree(id).await?,
            path_at: None,
            renamed: None,
        })
    }

    /// Hash of the changeset's root tree node.
    pub async fn root_tree(&self, id: ChangesetId) -> Result<Hash> {
        let v = derived::derive::<RootTree>(self, id).await?;
        derived::decode_hash(&v)
    }

    /// Length of the longest parent chain to a root; roots are 1.
    pub async fn generation(&self, id: ChangesetId) -> Result<u64> {
        let v = derived::derive::<Generation>(self, id).await?;
        derived::decode_u64(&v)
    }

    /// Read one tree node. The empty tree always exists.
    pub async fn read_tree(&self, tree: Hash) -> Result<TreeNode> {
        match self.blobs.get(&tree).await? {
            // A node's address is the hash of its bytes, so damage is
            // detectable. GC marks from these nodes: reading a damaged one as
            // valid would leave live blobs unmarked.
            Some(b) if Hash::of_object(TreeNode::KIND, &b) != tree => {
                Err(Error::Storage(format!("corrupt tree node {tree}")))
            }
            Some(b) => TreeNode::decode(&b),
            None if tree == TreeNode::empty_hash() => Ok(TreeNode::default()),
            None => Err(Error::NotFound(format!("tree {tree}"))),
        }
    }

    /// Resolve a ref name or full 64-hex changeset id. As a last resort a
    /// short name is looked up as a tag (`v1` for `tags/v1`), so a branch or
    /// id of the same name always wins.
    pub async fn resolve(&self, rev: &str) -> Result<ChangesetId> {
        if let Some(id) = self.meta.get_ref(rev).await? {
            return Ok(id);
        }
        if let Ok(id) = rev.parse::<ChangesetId>() {
            if rev.len() == 64 && self.meta.get_changeset(&id).await?.is_some() {
                return Ok(id);
            }
        }
        if !refname::is_tag(rev) && refname::validate(rev).is_ok() {
            if let Some(id) = self.meta.get_ref(&refname::tag_ref(rev)).await? {
                return Ok(id);
            }
        }
        Err(Error::NotFound(format!("revision {rev:?}")))
    }
}
