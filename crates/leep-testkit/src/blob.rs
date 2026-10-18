use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use leep_core::{BlobMeta, BlobStore, Hash, Result};

use crate::plan::{FaultPlan, Op};

/// A [`BlobStore`] that injects the faults of a [`FaultPlan`] around calls to
/// `inner`. A put that fails after applying leaves the blob stored, which is
/// the only torn write a blob store can show: every backend publishes a blob
/// whole (rename, object PUT), never a prefix.
pub struct FaultyBlobStore {
    inner: Arc<dyn BlobStore>,
    plan: Arc<FaultPlan>,
}

impl FaultyBlobStore {
    pub fn new(inner: Arc<dyn BlobStore>, plan: Arc<FaultPlan>) -> Self {
        Self { inner, plan }
    }

    pub fn plan(&self) -> &Arc<FaultPlan> {
        &self.plan
    }
}

#[async_trait]
impl BlobStore for FaultyBlobStore {
    async fn put_with_hash(&self, hash: Hash, data: Bytes) -> Result<()> {
        self.plan
            .run(Op::Put, self.inner.put_with_hash(hash, data), |_, v| v)
            .await
    }

    async fn get(&self, hash: &Hash) -> Result<Option<Bytes>> {
        self.plan
            .run(Op::Get, self.inner.get(hash), |plan, v| {
                v.map(|b| Bytes::from(plan.corrupt(&b)))
            })
            .await
    }

    async fn contains(&self, hash: &Hash) -> Result<bool> {
        self.plan
            .run(Op::Contains, self.inner.contains(hash), |_, v| v)
            .await
    }

    async fn contains_many(&self, hashes: &[Hash]) -> Result<Vec<bool>> {
        self.plan
            .run(Op::Contains, self.inner.contains_many(hashes), |_, v| v)
            .await
    }

    async fn flush(&self, named: &[Hash]) -> Result<()> {
        self.plan
            .run(Op::Flush, self.inner.flush(named), |_, v| v)
            .await
    }

    async fn flush_blobs(&self, hashes: &[Hash]) -> Result<()> {
        self.plan
            .run(Op::Flush, self.inner.flush_blobs(hashes), |_, v| v)
            .await
    }

    async fn list_page(&self, after: Option<Hash>, limit: usize) -> Result<Vec<BlobMeta>> {
        self.plan
            .run(Op::ListPage, self.inner.list_page(after, limit), |_, v| v)
            .await
    }

    async fn delete(&self, hash: &Hash) -> Result<()> {
        self.plan
            .run(Op::Delete, self.inner.delete(hash), |_, v| v)
            .await
    }
}
