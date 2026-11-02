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

use crate::objpacks::{
    bump_gen, idx_key, pack_key, RemotePack, RemotePackSet, DEFAULT_REFRESH_INTERVAL,
};
use crate::packput::{self, PackWriteStep, PackWrites};
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
/// and packed under `packs/` (ADR 0040). A put writes a loose object; a
/// `put_many` of at least [`PackWrites::min_objects`] objects writes them as
/// one pack (ADR 0068). Reads try the loose object first, then the packs.
#[derive(Clone)]
pub struct ObjectBlobStore {
    pub(crate) store: Arc<dyn ObjectStore>,
    pub(crate) packs: Arc<RemotePackSet>,
    writes: PackWrites,
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
        Self {
            store,
            packs,
            writes: PackWrites::default(),
        }
    }

    /// How `put_many` writes packs (ADR 0068); [`PackWrites::off`] keeps
    /// every write loose.
    pub fn with_pack_writes(mut self, writes: PackWrites) -> Self {
        self.writes = writes;
        self
    }

    /// Packs reads search now: published, and not merged by a repack of
    /// this process. Lists `packs/` first.
    pub async fn pack_count(&self) -> Result<usize> {
        Ok(self.packs.refresh().await?.searchable().len())
    }

    /// `put_with_hash` for each item, [`leep_core::store::PUT_IN_FLIGHT`]
    /// at a time.
    async fn put_loose_many(&self, items: Vec<(Hash, Bytes)>) -> Result<()> {
        for chunk in items.chunks(leep_core::store::PUT_IN_FLIGHT) {
            let puts: Vec<_> = chunk
                .iter()
                .map(|(h, d)| self.put_with_hash(*h, d.clone()))
                .collect();
            futures::future::try_join_all(puts).await?;
        }
        Ok(())
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

    /// At least [`PackWrites::min_objects`] objects become one pack: the
    /// `.pack` uploaded, then the `.idx`, which publishes it, then
    /// `packs/.gen` rewritten to announce it (ADR 0070), then added to this
    /// process's view; their hashes are returned. Other servers find it on
    /// their next miss. Unlike `put_with_hash`, an object that also has a
    /// loose copy keeps that copy's `modified` in listings, which GC's grace
    /// window reads; the rescue rows of ADR 0012, which the caller writes
    /// after this, are what protect it.
    async fn put_many(&self, mut items: Vec<(Hash, Bytes)>) -> Result<Vec<Hash>> {
        if !self.writes.packs(items.len()) {
            self.put_loose_many(items).await?;
            return Ok(Vec::new());
        }
        let started = std::time::Instant::now();
        packput::sort_distinct(&mut items);
        let mut tree_first = false;
        let (new, loose): (Vec<_>, Vec<_>) = items
            .into_iter()
            .partition(|(h, d)| packput::packable(h, d, &mut tree_first));
        if !self.writes.packs(new.len()) {
            self.put_loose_many(new.into_iter().chain(loose).collect())
                .await?;
            return Ok(Vec::new());
        }
        let level = self.writes.level;
        let now = crate::fs::unix_secs(std::time::SystemTime::now()).unwrap_or(0);
        let (new, encoded) = tokio::task::spawn_blocking(move || {
            let encoded = packput::encode(&new, level, now);
            (new, encoded)
        })
        .await
        .map_err(|e| leep_core::Error::Storage(format!("pack task failed: {e}")))?;
        let mut encoded = encoded?;
        let view = self.packs.view().await?;
        while view.pack(&encoded.id).is_some() {
            encoded.next_id()?;
        }
        let bytes = encoded.pack.len() as u64;
        self.writes.step(PackWriteStep::PackSynced)?;
        self.store
            .put(&pack_key(&encoded.id), PutPayload::from(encoded.pack))
            .await
            .map_err(storage_err)?;
        self.writes.step(PackWriteStep::PackRenamed)?;
        self.writes.step(PackWriteStep::IndexSynced)?;
        self.store
            .put(&idx_key(&encoded.id), PutPayload::from(encoded.idx))
            .await
            .map_err(storage_err)?;
        self.writes.step(PackWriteStep::Published)?;
        // Before returning: a changeset naming these objects is stored only
        // after, and a reader that misses one then sees the stamp changed.
        bump_gen(&*self.store, &encoded.id).await?;
        self.packs
            .insert_published(RemotePack {
                id: encoded.id,
                index: encoded.index,
            })
            .await;
        self.put_loose_many(loose).await?;
        packput::record(new.len(), bytes, started.elapsed());
        Ok(new.into_iter().map(|(h, _)| h).collect())
    }

    async fn get(&self, hash: &Hash) -> Result<Option<Bytes>> {
        match self.store.get(&Self::key(hash)).await {
            Ok(r) => Ok(Some(r.bytes().await.map_err(storage_err)?)),
            Err(object_store::Error::NotFound { .. }) => self.packs.get(hash).await,
            Err(e) => Err(storage_err(e)),
        }
    }

    /// [`get_stored`](Self::get_stored), whose packed lookup lists
    /// `packs/` once more after a miss the stamp confirmed (ADR 0070).
    async fn get_exact(&self, hash: &Hash) -> Result<Option<Bytes>> {
        let called = std::time::Instant::now();
        match self.store.get(&Self::key(hash)).await {
            Ok(r) => Ok(Some(r.bytes().await.map_err(storage_err)?)),
            Err(object_store::Error::NotFound { .. }) => {
                match self.packs.get_exact(hash, called).await? {
                    Some(b) if self.contains(hash).await? => Ok(Some(b)),
                    _ => Ok(None),
                }
            }
            Err(e) => Err(storage_err(e)),
        }
    }

    /// A loose object read is the durable copy; a packed one is checked as
    /// `contains` checks.
    async fn get_stored(&self, hash: &Hash) -> Result<Option<Bytes>> {
        match self.store.get(&Self::key(hash)).await {
            Ok(r) => Ok(Some(r.bytes().await.map_err(storage_err)?)),
            Err(object_store::Error::NotFound { .. }) => match self.packs.get(hash).await? {
                Some(b) if self.contains(hash).await? => Ok(Some(b)),
                _ => Ok(None),
            },
            Err(e) => Err(storage_err(e)),
        }
    }

    async fn warm(&self) -> Result<()> {
        self.packs.view().await.map(|_| ())
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
