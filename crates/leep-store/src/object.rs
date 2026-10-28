use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::TryStreamExt;
use leep_core::{BlobMeta, BlobStore, Hash, Result};
use object_store::aws::AmazonS3Builder;
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};

use crate::objpacks::{RemotePackSet, DEFAULT_REFRESH_INTERVAL};
use crate::packset::DEFAULT_DECODED_CACHE;
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

/// Blobs in any `object_store` backend: loose under `blobs/<2 hex>/<62 hex>`,
/// and packed under `packs/` once `repack` has run (ADR 0040). New writes are
/// always loose; reads try the loose object first, then the packs.
#[derive(Clone)]
pub struct ObjectBlobStore {
    pub(crate) store: Arc<dyn ObjectStore>,
    pub(crate) packs: Arc<RemotePackSet>,
}

impl std::fmt::Debug for ObjectBlobStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObjectBlobStore")
            .field("store", &self.store.to_string())
            .finish_non_exhaustive()
    }
}

impl ObjectBlobStore {
    /// Packs are listed on first use, not here, so opening never fails.
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self::with_options(store, DEFAULT_DECODED_CACHE, DEFAULT_REFRESH_INTERVAL)
    }

    /// Like [`new`](Self::new), with the byte budget for decoded packed
    /// objects (0 disables it) and the shortest time between two pack
    /// listings a miss may cause. The interval must stay well below the
    /// repack fence, or a reader could miss an object whose loose copy a
    /// repack deleted after the fence.
    pub fn with_options(
        store: Arc<dyn ObjectStore>,
        decoded_cache: usize,
        refresh_interval: std::time::Duration,
    ) -> Self {
        let packs = Arc::new(RemotePackSet::new(
            store.clone(),
            decoded_cache,
            refresh_interval,
        ));
        Self { store, packs }
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

    pub(crate) fn key(hash: &Hash) -> Path {
        let hex = hash.to_hex();
        Path::from(format!("blobs/{}/{}", &hex[..2], &hex[2..]))
    }
}

#[async_trait]
impl BlobStore for ObjectBlobStore {
    /// Always writes, so a re-upload refreshes `modified` (GC's grace window,
    /// ADR 0012) and a loose copy outlives a tombstone on a packed one.
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
            Err(object_store::Error::NotFound { .. }) => self.packs.get(hash).await,
            Err(e) => Err(storage_err(e)),
        }
    }

    async fn contains(&self, hash: &Hash) -> Result<bool> {
        match self.store.head(&Self::key(hash)).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => self.packs.contains(hash).await,
            Err(e) => Err(storage_err(e)),
        }
    }

    /// Loose and packed blobs merged in hash order, with logical sizes and
    /// each packed blob's recorded `modified`, as the filesystem store lists.
    /// The loose page is listed before the packs, so a loose copy a repack
    /// deletes in between is found in the pack that replaced it.
    async fn list_page(&self, after: Option<Hash>, limit: usize) -> Result<Vec<BlobMeta>> {
        let loose = self.list_loose(after, limit).await?;
        self.packs.merge_page(loose, after, limit).await
    }

    /// Tombstones packed copies first, then deletes the loose object, so a
    /// repack that finds the tombstone keeps the loose copy.
    async fn delete(&self, hash: &Hash) -> Result<()> {
        self.packs.tombstone(hash).await?;
        match self.store.delete(&Self::key(hash)).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(storage_err(e)),
        }
    }
}

impl ObjectBlobStore {
    /// Loose blobs above `after`, at most `limit`. Lists one shard prefix at
    /// a time and sorts it, because `object_store` does not promise listing
    /// order across backends.
    async fn list_loose(&self, after: Option<Hash>, limit: usize) -> Result<Vec<BlobMeta>> {
        let mut out = Vec::new();
        let first_shard = after.map_or(0, |h| h.0[0]);
        for shard in first_shard..=255u8 {
            if out.len() >= limit {
                break;
            }
            let prefix = format!("{shard:02x}");
            let dir = Path::from(format!("blobs/{prefix}"));
            let objects: Vec<_> = self
                .store
                .list(Some(&dir))
                .try_collect()
                .await
                .map_err(storage_err)?;
            let mut found = Vec::new();
            for o in objects {
                let Some(name) = o.location.filename() else {
                    continue;
                };
                let Ok(hash) = format!("{prefix}{name}").parse::<Hash>() else {
                    continue;
                };
                if after.is_some_and(|a| hash <= a) {
                    continue;
                }
                found.push(BlobMeta {
                    hash,
                    size: o.size,
                    modified: Some(o.last_modified.timestamp()),
                });
            }
            found.sort_by_key(|m| m.hash);
            out.extend(found);
        }
        out.truncate(limit);
        Ok(out)
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
