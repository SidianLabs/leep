use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use leep_core::{BlobStore, Hash, Result};
use object_store::aws::AmazonS3Builder;
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, PutPayload};

use crate::storage_err;

/// Settings for an S3-compatible bucket. Credentials come from the standard
/// AWS environment variables.
#[derive(Debug, Clone, Default)]
pub struct S3Config {
    pub bucket: String,
    pub endpoint: Option<String>,
    pub region: Option<String>,
    /// Permit plain-HTTP endpoints (MinIO in development).
    pub allow_http: bool,
}

/// Blobs in any `object_store` backend under `blobs/<2 hex>/<62 hex>`.
#[derive(Clone)]
pub struct ObjectBlobStore {
    store: Arc<dyn ObjectStore>,
}

impl ObjectBlobStore {
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self { store }
    }

    pub fn s3(cfg: S3Config) -> Result<Self> {
        let mut b = AmazonS3Builder::from_env()
            .with_bucket_name(cfg.bucket)
            .with_allow_http(cfg.allow_http);
        if let Some(e) = cfg.endpoint {
            b = b.with_endpoint(e);
        }
        if let Some(r) = cfg.region {
            b = b.with_region(r);
        }
        Ok(Self::new(Arc::new(b.build().map_err(storage_err)?)))
    }

    pub fn in_memory() -> Self {
        Self::new(Arc::new(InMemory::new()))
    }

    pub fn local(root: impl AsRef<std::path::Path>) -> Result<Self> {
        std::fs::create_dir_all(root.as_ref()).map_err(storage_err)?;
        let fs = LocalFileSystem::new_with_prefix(root).map_err(storage_err)?;
        Ok(Self::new(Arc::new(fs)))
    }

    fn key(hash: &Hash) -> Path {
        let hex = hash.to_hex();
        Path::from(format!("blobs/{}/{}", &hex[..2], &hex[2..]))
    }
}

#[async_trait]
impl BlobStore for ObjectBlobStore {
    async fn put_with_hash(&self, hash: Hash, data: Bytes) -> Result<()> {
        self.store
            .put(&Self::key(&hash), PutPayload::from_bytes(data))
            .await
            .map_err(storage_err)?;
        Ok(())
    }

    async fn get(&self, hash: &Hash) -> Result<Option<Bytes>> {
        match self.store.get(&Self::key(hash)).await {
            Ok(r) => Ok(Some(r.bytes().await.map_err(storage_err)?)),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(storage_err(e)),
        }
    }

    async fn contains(&self, hash: &Hash) -> Result<bool> {
        match self.store.head(&Self::key(hash)).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(storage_err(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use leep_core::memory::check_blob_store_contract;

    #[tokio::test]
    async fn in_memory_passes_contract() {
        check_blob_store_contract(&ObjectBlobStore::in_memory()).await;
    }

    #[tokio::test]
    async fn local_passes_contract() {
        let dir = tempfile::tempdir().unwrap();
        check_blob_store_contract(&ObjectBlobStore::local(dir.path()).unwrap()).await;
    }

    #[tokio::test]
    async fn key_layout_and_concurrency() {
        let mem = Arc::new(InMemory::new());
        let store = Arc::new(ObjectBlobStore::new(mem.clone()));
        let tasks: Vec<_> = (0..32u32)
            .map(|i| {
                let s = store.clone();
                tokio::spawn(async move {
                    s.put(Bytes::from_static(b"shared")).await.unwrap();
                    s.put(Bytes::from(format!("own {}", i % 4))).await.unwrap()
                })
            })
            .collect();
        for t in tasks {
            let h = t.await.unwrap();
            assert!(store.contains(&h).await.unwrap());
        }
        let h = Hash::of_blob(b"shared");
        let hex = h.to_hex();
        let key = Path::from(format!("blobs/{}/{}", &hex[..2], &hex[2..]));
        assert_eq!(
            mem.get(&key).await.unwrap().bytes().await.unwrap(),
            &b"shared"[..]
        );
    }
}
