//! Storage contracts. Implementations live in `leep-store` (blobs) and
//! `leep-meta` (changesets, refs, derived data); in-memory versions for tests
//! live in [`crate::memory`].

use std::collections::BTreeMap;

use async_trait::async_trait;
use bytes::Bytes;

use crate::{Changeset, ChangesetId, Hash, Result};

/// What a blob listing reports about one stored blob.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BlobMeta {
    pub hash: Hash,
    pub size: u64,
    /// Last write time (unix seconds), refreshed when the same blob is put
    /// again. `None` if the backend cannot tell; garbage collection treats
    /// those blobs as too young to delete.
    pub modified: Option<i64>,
}

/// Immutable content-addressed bytes: file contents and tree nodes.
#[async_trait]
pub trait BlobStore: Send + Sync + 'static {
    /// Store file contents; returns `Hash::of_blob(data)`. Idempotent.
    async fn put(&self, data: Bytes) -> Result<Hash> {
        let h = Hash::of_blob(&data);
        self.put_with_hash(h, data).await?;
        Ok(h)
    }

    /// Store bytes under a caller-computed address (used for tree nodes and
    /// other derived objects). Callers guarantee `hash` is correct for `data`.
    async fn put_with_hash(&self, hash: Hash, data: Bytes) -> Result<()>;

    async fn get(&self, hash: &Hash) -> Result<Option<Bytes>>;

    async fn contains(&self, hash: &Hash) -> Result<bool> {
        Ok(self.get(hash).await?.is_some())
    }

    /// Up to `limit` blobs with hash strictly greater than `after`, in
    /// ascending hash order. Paged so garbage collection can scan stores too
    /// large to list at once.
    async fn list_page(&self, after: Option<Hash>, limit: usize) -> Result<Vec<BlobMeta>>;

    /// Remove a blob. Idempotent: deleting an absent blob succeeds. Only
    /// garbage collection should call this.
    async fn delete(&self, hash: &Hash) -> Result<()>;
}

/// Changesets, refs and derived data. Changesets and derived data are
/// immutable once written; refs are the only mutable state.
#[async_trait]
pub trait MetaStore: Send + Sync + 'static {
    /// Store a changeset. Idempotent; returns its id.
    async fn put_changeset(&self, cs: &Changeset) -> Result<ChangesetId>;

    async fn get_changeset(&self, id: &ChangesetId) -> Result<Option<Changeset>>;

    /// Up to `limit` changeset ids strictly greater than `after`, in
    /// ascending byte order. Paged for garbage collection and other full
    /// scans.
    async fn list_changesets(
        &self,
        after: Option<ChangesetId>,
        limit: usize,
    ) -> Result<Vec<ChangesetId>>;

    async fn get_ref(&self, name: &str) -> Result<Option<ChangesetId>>;

    /// Atomically set `name` to `new` iff it currently equals `expected`
    /// (`None` = absent). `new = None` deletes the ref. Returns whether the
    /// swap happened.
    async fn cas_ref(
        &self,
        name: &str,
        expected: Option<ChangesetId>,
        new: Option<ChangesetId>,
    ) -> Result<bool>;

    async fn list_refs(&self) -> Result<BTreeMap<String, ChangesetId>>;

    /// Derived data is keyed by (kind, key), e.g. ("root_tree", changeset id).
    /// Values are opaque bytes owned by `leep-derive`. Writing the same key
    /// twice must store the same value, so last-writer-wins is fine.
    async fn get_derived(&self, kind: &str, key: &Hash) -> Result<Option<Bytes>>;

    async fn put_derived(&self, kind: &str, key: &Hash, value: Bytes) -> Result<()>;
}
