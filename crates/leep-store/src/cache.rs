use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use async_trait::async_trait;
use bytes::Bytes;
use leep_core::{BlobStore, Hash, Result};

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

    /// Mirror occupancy into the gauges. Called with the lock held so the
    /// two values are consistent. Hits and misses are counted at the source.
    fn publish(lru: &Lru) {
        metrics::gauge!("leep_blob_cache_entries").set(lru.map.len() as f64);
        metrics::gauge!("leep_blob_cache_bytes").set(lru.bytes as f64);
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
        let mut lru = self.lru.lock().unwrap();
        lru.insert(hash, data, self.capacity);
        Self::publish(&lru);
        Ok(())
    }

    async fn get(&self, hash: &Hash) -> Result<Option<Bytes>> {
        if let Some(d) = self.lru.lock().unwrap().get(hash) {
            self.hits.fetch_add(1, Ordering::Relaxed);
            metrics::counter!("leep_blob_cache_hits_total").increment(1);
            return Ok(Some(d));
        }
        self.misses.fetch_add(1, Ordering::Relaxed);
        metrics::counter!("leep_blob_cache_misses_total").increment(1);
        let found = self.inner.get(hash).await?;
        if let Some(d) = &found {
            let mut lru = self.lru.lock().unwrap();
            lru.insert(*hash, d.clone(), self.capacity);
            Self::publish(&lru);
        }
        Ok(found)
    }

    async fn contains(&self, hash: &Hash) -> Result<bool> {
        if self.lru.lock().unwrap().map.contains_key(hash) {
            return Ok(true);
        }
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
    async fn put_writes_through_and_populates() {
        let cache = CachedBlobStore::new(MemoryBlobStore::new(), 1024);
        let h = cache.put(Bytes::from_static(b"data")).await.unwrap();
        assert!(cache.inner().contains(&h).await.unwrap());
        cache.get(&h).await.unwrap();
        assert_eq!((cache.stats().hits, cache.stats().misses), (1, 0));
    }
}
