//! Storage contracts. Implementations live in `leep-store` (blobs) and
//! `leep-meta` (changesets, refs, derived data); in-memory versions for tests
//! live in [`crate::memory`].

use std::collections::BTreeMap;

use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::{Changeset, ChangesetId, Hash, Result};

/// Unix seconds now, the server clock every ref log entry is stamped with.
pub fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs() as i64)
}

/// Who moved a ref and why, recorded in the ref log with the move itself.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RefUpdateContext {
    /// Token name of the caller, or `None` when authentication is off.
    pub actor: Option<String>,
    /// Short free-form label: `land`, `cas`, `undo`, `import`, ...
    pub operation: String,
    pub message: Option<String>,
}

impl RefUpdateContext {
    pub fn new(operation: impl Into<String>) -> Self {
        Self {
            actor: None,
            operation: operation.into(),
            message: None,
        }
    }

    pub fn actor(mut self, actor: Option<impl Into<String>>) -> Self {
        self.actor = actor.map(Into::into);
        self
    }

    pub fn message(mut self, message: impl Into<String>) -> Self {
        self.message = Some(message.into());
        self
    }
}

/// One recorded ref move. `old = None` is a create, `new = None` a delete.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RefLogEntry {
    /// Increases with every entry in the store, so it orders moves across refs.
    pub seq: u64,
    pub name: String,
    pub old: Option<ChangesetId>,
    pub new: Option<ChangesetId>,
    /// Unix seconds by the server that applied the move.
    pub time: i64,
    pub actor: Option<String>,
    pub operation: String,
    #[serde(default)]
    pub message: Option<String>,
}

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

    /// [`cas_ref_logged`](Self::cas_ref_logged) with no actor and the
    /// operation `cas`. For callers that have no better context.
    async fn cas_ref(
        &self,
        name: &str,
        expected: Option<ChangesetId>,
        new: Option<ChangesetId>,
    ) -> Result<bool> {
        self.cas_ref_logged(name, expected, new, &RefUpdateContext::new("cas"))
            .await
    }

    /// Atomically set `name` to `new` iff it currently equals `expected`
    /// (`None` = absent). `new = None` deletes the ref. Returns whether the
    /// swap happened.
    ///
    /// A swap that happens appends one [`RefLogEntry`] in the same atomic
    /// step, so a ref never moves unrecorded. A lost swap, and a swap that
    /// changes nothing (`expected == new`), write no entry.
    async fn cas_ref_logged(
        &self,
        name: &str,
        expected: Option<ChangesetId>,
        new: Option<ChangesetId>,
        ctx: &RefUpdateContext,
    ) -> Result<bool>;

    /// Ref log entries, newest first: those of `name` or of every ref, with
    /// `seq` below `before_seq` when given, at most `limit`.
    async fn ref_log(
        &self,
        name: Option<&str>,
        before_seq: Option<u64>,
        limit: usize,
    ) -> Result<Vec<RefLogEntry>>;

    async fn list_refs(&self) -> Result<BTreeMap<String, ChangesetId>>;

    /// Derived data is keyed by (kind, key), e.g. ("root_tree", changeset id).
    /// Values are opaque bytes owned by `leep-derive`. Writing the same key
    /// twice must store the same value, so last-writer-wins is fine.
    async fn get_derived(&self, kind: &str, key: &Hash) -> Result<Option<Bytes>>;

    async fn put_derived(&self, kind: &str, key: &Hash, value: Bytes) -> Result<()>;

    /// Version of the newest schema migration applied (ADR 0023), for
    /// diagnostics. `None` for a store without a versioned schema.
    async fn schema_version(&self) -> Result<Option<u64>> {
        Ok(None)
    }
}
