//! Read-only views of the stores, so verification can run the real derivation
//! code without writing anything.
//!
//! Derivation writes as it goes (tree nodes, history nodes, missing rows). A
//! shadow lets those writes land in memory instead, and lets the verifier
//! substitute a recomputed row for a damaged one so that later checks are made
//! against the truth, not against the damage they would otherwise inherit.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use leep_core::{
    BlobMeta, BlobStore, Changeset, ChangesetId, DeleteStep, Error, Hash, MetaStore, RefLogEntry,
    RefUpdateContext, Result,
};

fn read_only() -> Error {
    Error::Other("read-only store".into())
}

/// A meta store whose derived rows can be overridden and whose derived writes
/// stay in memory. Changesets and refs cannot be written at all.
pub(crate) struct ShadowMeta {
    inner: Arc<dyn MetaStore>,
    over: Mutex<HashMap<(String, Hash), Bytes>>,
}

impl ShadowMeta {
    pub fn new(inner: Arc<dyn MetaStore>) -> Arc<Self> {
        Arc::new(ShadowMeta {
            inner,
            over: Mutex::default(),
        })
    }

    /// Make reads of `(kind, key)` return `value` from now on.
    pub fn set(&self, kind: &str, key: &Hash, value: Bytes) {
        self.over
            .lock()
            .unwrap()
            .insert((kind.to_string(), *key), value);
    }
}

#[async_trait]
impl MetaStore for ShadowMeta {
    async fn put_changeset(&self, _: &Changeset) -> Result<ChangesetId> {
        Err(read_only())
    }

    async fn get_changeset(&self, id: &ChangesetId) -> Result<Option<Changeset>> {
        self.inner.get_changeset(id).await
    }

    async fn list_changesets(
        &self,
        after: Option<ChangesetId>,
        limit: usize,
    ) -> Result<Vec<ChangesetId>> {
        self.inner.list_changesets(after, limit).await
    }

    async fn get_ref(&self, name: &str) -> Result<Option<ChangesetId>> {
        self.inner.get_ref(name).await
    }

    async fn cas_ref_logged(
        &self,
        _: &str,
        _: Option<ChangesetId>,
        _: Option<ChangesetId>,
        _: &RefUpdateContext,
    ) -> Result<bool> {
        Err(read_only())
    }

    async fn ref_log(
        &self,
        name: Option<&str>,
        before_seq: Option<u64>,
        limit: usize,
    ) -> Result<Vec<RefLogEntry>> {
        self.inner.ref_log(name, before_seq, limit).await
    }

    async fn list_refs(&self) -> Result<BTreeMap<String, ChangesetId>> {
        self.inner.list_refs().await
    }

    async fn list_derived(
        &self,
        kind: &str,
        after: Option<Hash>,
        limit: usize,
    ) -> Result<Vec<(Hash, Bytes)>> {
        self.inner.list_derived(kind, after, limit).await
    }

    async fn get_derived(&self, kind: &str, key: &Hash) -> Result<Option<Bytes>> {
        if let Some(v) = self.over.lock().unwrap().get(&(kind.to_string(), *key)) {
            return Ok(Some(v.clone()));
        }
        self.inner.get_derived(kind, key).await
    }

    async fn put_derived(&self, kind: &str, key: &Hash, value: Bytes) -> Result<()> {
        self.set(kind, key, value);
        Ok(())
    }

    async fn delete_derived_step(&self, _: &[&str], _: Option<&[u8]>) -> Result<DeleteStep> {
        Err(read_only())
    }
}

/// A blob store whose writes stay in memory until [`ShadowBlobs::clear`].
pub(crate) struct ShadowBlobs {
    inner: Arc<dyn BlobStore>,
    writes: Mutex<HashMap<Hash, Bytes>>,
}

impl ShadowBlobs {
    pub fn new(inner: Arc<dyn BlobStore>) -> Arc<Self> {
        Arc::new(ShadowBlobs {
            inner,
            writes: Mutex::default(),
        })
    }

    /// Forget what was "written", which only recomputation produced.
    pub fn clear(&self) {
        self.writes.lock().unwrap().clear();
    }
}

#[async_trait]
impl BlobStore for ShadowBlobs {
    async fn put_with_hash(&self, hash: Hash, data: Bytes) -> Result<()> {
        self.writes.lock().unwrap().insert(hash, data);
        Ok(())
    }

    async fn get(&self, hash: &Hash) -> Result<Option<Bytes>> {
        if let Some(b) = self.writes.lock().unwrap().get(hash) {
            return Ok(Some(b.clone()));
        }
        self.inner.get(hash).await
    }

    async fn contains(&self, hash: &Hash) -> Result<bool> {
        if self.writes.lock().unwrap().contains_key(hash) {
            return Ok(true);
        }
        self.inner.contains(hash).await
    }

    async fn list_page(&self, after: Option<Hash>, limit: usize) -> Result<Vec<BlobMeta>> {
        self.inner.list_page(after, limit).await
    }

    async fn delete(&self, _: &Hash) -> Result<()> {
        Err(read_only())
    }
}
