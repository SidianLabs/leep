use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use leep_core::{BlobMeta, BlobStore, Hash, Result};
use tokio::io::AsyncWriteExt;

use crate::storage_err;

static TMP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Blobs on the local filesystem at `<root>/<2 hex>/<62 hex>`.
#[derive(Debug, Clone)]
pub struct FsBlobStore {
    root: PathBuf,
}

impl FsBlobStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    fn path_for(&self, hash: &Hash) -> PathBuf {
        let hex = hash.to_hex();
        self.root.join(&hex[..2]).join(&hex[2..])
    }
}

/// Refresh the mtime of an existing blob so garbage collection's grace window
/// counts from the latest upload. Returns false if the blob is absent.
fn touch(path: &Path) -> std::io::Result<bool> {
    match std::fs::OpenOptions::new().write(true).open(path) {
        Ok(f) => {
            f.set_modified(std::time::SystemTime::now())?;
            Ok(true)
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(e),
    }
}

fn unix_secs(t: std::time::SystemTime) -> Option<i64> {
    match t.duration_since(std::time::UNIX_EPOCH) {
        Ok(d) => Some(d.as_secs() as i64),
        Err(_) => None,
    }
}

#[async_trait]
impl BlobStore for FsBlobStore {
    async fn put_with_hash(&self, hash: Hash, data: Bytes) -> Result<()> {
        let path = self.path_for(&hash);
        if touch(&path).map_err(storage_err)? {
            return Ok(());
        }
        let dir = path.parent().expect("blob path has a parent");
        tokio::fs::create_dir_all(dir).await.map_err(storage_err)?;
        let tmp = dir.join(format!(
            ".tmp-{}-{}",
            std::process::id(),
            TMP_COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        let res = async {
            let mut f = tokio::fs::File::create(&tmp).await?;
            f.write_all(&data).await?;
            f.sync_all().await?;
            drop(f);
            tokio::fs::rename(&tmp, &path).await
        }
        .await;
        if let Err(e) = res {
            let _ = tokio::fs::remove_file(&tmp).await;
            return Err(storage_err(e));
        }
        Ok(())
    }

    async fn get(&self, hash: &Hash) -> Result<Option<Bytes>> {
        match tokio::fs::read(self.path_for(hash)).await {
            Ok(v) => Ok(Some(v.into())),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(storage_err(e)),
        }
    }

    async fn contains(&self, hash: &Hash) -> Result<bool> {
        tokio::fs::try_exists(self.path_for(hash))
            .await
            .map_err(storage_err)
    }

    /// Walks shard directories in order, so a page reads at most the shards
    /// it needs (each holds about 1/256 of the blobs).
    async fn list_page(&self, after: Option<Hash>, limit: usize) -> Result<Vec<BlobMeta>> {
        let mut out = Vec::new();
        let first_shard = after.map_or(0, |h| h.0[0]);
        for shard in first_shard..=255u8 {
            if out.len() >= limit {
                break;
            }
            let prefix = format!("{shard:02x}");
            let mut dir = match tokio::fs::read_dir(self.root.join(&prefix)).await {
                Ok(d) => d,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(storage_err(e)),
            };
            let mut found = Vec::new();
            while let Some(entry) = dir.next_entry().await.map_err(storage_err)? {
                // Skips in-flight `.tmp-*` files and anything foreign.
                let name = entry.file_name();
                let Ok(hash) = format!("{prefix}{}", name.to_string_lossy()).parse::<Hash>() else {
                    continue;
                };
                if after.is_some_and(|a| hash <= a) {
                    continue;
                }
                let meta = match entry.metadata().await {
                    Ok(m) => m,
                    // Deleted between readdir and stat.
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                    Err(e) => return Err(storage_err(e)),
                };
                found.push(BlobMeta {
                    hash,
                    size: meta.len(),
                    modified: meta.modified().ok().and_then(unix_secs),
                });
            }
            found.sort_by_key(|m| m.hash);
            out.extend(found);
        }
        out.truncate(limit);
        Ok(out)
    }

    async fn delete(&self, hash: &Hash) -> Result<()> {
        match tokio::fs::remove_file(self.path_for(hash)).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(storage_err(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use leep_core::memory::check_blob_store_contract;

    #[tokio::test]
    async fn passes_contract_and_layout() {
        let dir = tempfile::tempdir().unwrap();
        let store = FsBlobStore::new(dir.path());
        check_blob_store_contract(&store).await;
        let h = store.put(Bytes::from_static(b"layout")).await.unwrap();
        let hex = h.to_hex();
        assert!(dir.path().join(&hex[..2]).join(&hex[2..]).is_file());
    }

    #[tokio::test]
    async fn reput_refreshes_mtime_and_listing_skips_foreign_files() {
        let dir = tempfile::tempdir().unwrap();
        let store = FsBlobStore::new(dir.path());
        let h = store.put(Bytes::from_static(b"old")).await.unwrap();
        let hex = h.to_hex();
        let path = dir.path().join(&hex[..2]).join(&hex[2..]);
        let old = std::time::SystemTime::now() - std::time::Duration::from_secs(48 * 3600);
        std::fs::File::options()
            .write(true)
            .open(&path)
            .unwrap()
            .set_modified(old)
            .unwrap();
        let listed = store.list_page(None, 10).await.unwrap();
        let t = listed[0].modified.unwrap();
        assert!(t < unix_secs(std::time::SystemTime::now()).unwrap() - 47 * 3600);
        store.put(Bytes::from_static(b"old")).await.unwrap();
        let t2 = store.list_page(None, 10).await.unwrap()[0]
            .modified
            .unwrap();
        assert!(t2 > t + 47 * 3600, "re-put refreshes the mtime");
        std::fs::write(dir.path().join(&hex[..2]).join(".tmp-1-1"), b"x").unwrap();
        std::fs::write(dir.path().join(&hex[..2]).join("junk"), b"x").unwrap();
        assert_eq!(store.list_page(None, 10).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn concurrent_puts() {
        let dir = tempfile::tempdir().unwrap();
        let store = std::sync::Arc::new(FsBlobStore::new(dir.path()));
        let mut tasks = Vec::new();
        for i in 0..64u32 {
            let s = store.clone();
            tasks.push(tokio::spawn(async move {
                let same = s.put(Bytes::from(vec![7u8; 100_000])).await.unwrap();
                let own = s.put(Bytes::from(format!("blob {}", i % 8))).await.unwrap();
                (same, own, i)
            }));
        }
        for t in tasks {
            let (same, own, i) = t.await.unwrap();
            assert_eq!(store.get(&same).await.unwrap().unwrap().len(), 100_000);
            assert_eq!(
                store.get(&own).await.unwrap().unwrap(),
                format!("blob {}", i % 8).as_bytes()
            );
        }
        for shard in std::fs::read_dir(dir.path()).unwrap() {
            for f in std::fs::read_dir(shard.unwrap().path()).unwrap() {
                let name = f.unwrap().file_name();
                assert!(
                    !name.to_string_lossy().starts_with(".tmp"),
                    "leaked temp file"
                );
            }
        }
    }
}
