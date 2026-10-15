use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use async_trait::async_trait;
use bytes::Bytes;
use leep_core::{BlobMeta, BlobStore, Hash, Result};

/// Cache counters since construction.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CacheStats {
    pub hits: u64,
    pub misses: u64,
    pub entries: usize,
    pub bytes: usize,
}

#[derive(Default)]
struct Lru {
    map: HashMap<Hash, (Bytes, u64)>,
    order: BTreeMap<u64, Hash>,
    tick: u64,
    bytes: usize,
}

impl Lru {
    fn get(&mut self, hash: &Hash) -> Option<Bytes> {
        let (data, old) = self.map.get_mut(hash)?;
        self.order.remove(old);
        self.tick += 1;
        *old = self.tick;
        self.order.insert(self.tick, *hash);
        Some(data.clone())
    }

    fn insert(&mut self, hash: Hash, data: Bytes, capacity: usize) {
        if data.len() > capacity {
            return;
        }
        if self.map.contains_key(&hash) {
            self.get(&hash);
            return;
        }
        self.tick += 1;
        self.bytes += data.len();
        self.order.insert(self.tick, hash);
        self.map.insert(hash, (data, self.tick));
        self.evict_over(capacity);
    }

    fn remove(&mut self, hash: &Hash) {
        if let Some((d, tick)) = self.map.remove(hash) {
            self.order.remove(&tick);
            self.bytes -= d.len();
        }
    }

    fn evict_over(&mut self, capacity: usize) {
        while self.bytes > capacity {
            let Some((_, victim)) = self.order.pop_first() else {
                break;
            };
            if let Some((d, _)) = self.map.remove(&victim) {
                self.bytes -= d.len();
            }
        }
    }
}

/// Bounded LRU cache (by total bytes) in front of any `BlobStore`. Safe
/// without invalidation because blobs are immutable. Blobs larger than the
/// capacity are never cached.
pub struct CachedBlobStore<S> {
    inner: S,
    capacity: usize,
    lru: Mutex<Lru>,
    hits: AtomicU64,
    misses: AtomicU64,
}

impl<S: BlobStore> CachedBlobStore<S> {
    pub fn new(inner: S, capacity_bytes: usize) -> Self {
        Self {
            inner,
            capacity: capacity_bytes,
            lru: Mutex::default(),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    pub fn inner(&self) -> &S {
        &self.inner
    }

    pub fn stats(&self) -> CacheStats {
        let lru = self.lru.lock().unwrap();
        CacheStats {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            entries: lru.map.len(),
            bytes: lru.bytes,
        }
    }
}

#[async_trait]
impl<S: BlobStore> BlobStore for CachedBlobStore<S> {
    async fn put_with_hash(&self, hash: Hash, data: Bytes) -> Result<()> {
        self.inner.put_with_hash(hash, data.clone()).await?;
        self.lru.lock().unwrap().insert(hash, data, self.capacity);
        Ok(())
    }

    async fn get(&self, hash: &Hash) -> Result<Option<Bytes>> {
        if let Some(d) = self.lru.lock().unwrap().get(hash) {
            self.hits.fetch_add(1, Ordering::Relaxed);
            return Ok(Some(d));
        }
        self.misses.fetch_add(1, Ordering::Relaxed);
        let found = self.inner.get(hash).await?;
        if let Some(d) = &found {
            self.lru
                .lock()
                .unwrap()
                .insert(*hash, d.clone(), self.capacity);
        }
        Ok(found)
    }

    async fn list_page(&self, after: Option<Hash>, limit: usize) -> Result<Vec<BlobMeta>> {
        self.inner.list_page(after, limit).await
    }

    /// Evicts after the inner delete. A `get` racing with a delete can
    /// re-cache the blob; only garbage collection deletes, and it never reads
    /// the blobs it deletes.
    async fn delete(&self, hash: &Hash) -> Result<()> {
        self.inner.delete(hash).await?;
        self.lru.lock().unwrap().remove(hash);
        Ok(())
    }

    /// Always asks the inner store. GC may delete a blob from another process
    /// while it is still cached here; answering from the cache would let a
    /// client skip re-uploading a blob that is gone from durable storage.
    async fn contains(&self, hash: &Hash) -> Result<bool> {
        self.inner.contains(hash).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use leep_core::memory::{check_blob_store_contract, MemoryBlobStore};

    #[tokio::test]
    async fn passes_contract() {
        check_blob_store_contract(&CachedBlobStore::new(MemoryBlobStore::new(), 1024)).await;
        check_blob_store_contract(&CachedBlobStore::new(MemoryBlobStore::new(), 0)).await;
    }

    #[tokio::test]
    async fn hits_misses_and_eviction() {
        let inner = MemoryBlobStore::new();
        let a = inner.put(Bytes::from(vec![1u8; 40])).await.unwrap();
        let b = inner.put(Bytes::from(vec![2u8; 40])).await.unwrap();
        let c = inner.put(Bytes::from(vec![3u8; 40])).await.unwrap();
        let big = inner.put(Bytes::from(vec![4u8; 500])).await.unwrap();
        let cache = CachedBlobStore::new(inner, 100);

        cache.get(&a).await.unwrap();
        cache.get(&a).await.unwrap();
        assert_eq!((cache.stats().hits, cache.stats().misses), (1, 1));
        cache.get(&b).await.unwrap();
        cache.get(&a).await.unwrap(); // a is now most recent
        cache.get(&c).await.unwrap(); // evicts b
        assert_eq!(cache.stats().bytes, 80);
        let before = cache.stats();
        cache.get(&a).await.unwrap();
        assert_eq!(cache.stats().hits, before.hits + 1);
        cache.get(&b).await.unwrap();
        assert_eq!(cache.stats().misses, before.misses + 1);

        cache.get(&big).await.unwrap();
        assert!(cache.stats().bytes <= 100);
        assert!(cache.get(&Hash::of_blob(b"nope")).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn delete_evicts_from_cache() {
        let cache = CachedBlobStore::new(MemoryBlobStore::new(), 1024);
        let h = cache.put(Bytes::from_static(b"gone")).await.unwrap();
        let keep = cache.put(Bytes::from_static(b"kept")).await.unwrap();
        cache.delete(&h).await.unwrap();
        assert!(cache.get(&h).await.unwrap().is_none());
        assert!(!cache.contains(&h).await.unwrap());
        assert_eq!(cache.stats().entries, 1);
        assert_eq!(cache.stats().bytes, 4);
        assert!(cache.contains(&keep).await.unwrap());
    }

    /// Another process (GC) deleting from the durable store must be visible
    /// to `contains` even while the blob is still cached.
    #[tokio::test]
    async fn contains_reflects_external_deletes() {
        let cache = CachedBlobStore::new(MemoryBlobStore::new(), 1024);
        let h = cache.put(Bytes::from_static(b"cached")).await.unwrap();
        cache.inner().delete(&h).await.unwrap();
        assert!(!cache.contains(&h).await.unwrap());
    }

    #[tokio::test]
    async fn put_writes_through_and_populates() {
        let cache = CachedBlobStore::new(MemoryBlobStore::new(), 1024);
        let h = cache.put(Bytes::from_static(b"data")).await.unwrap();
        assert!(cache.inner().contains(&h).await.unwrap());
        cache.get(&h).await.unwrap();
        assert_eq!((cache.stats().hits, cache.stats().misses), (1, 0));
    }
}
