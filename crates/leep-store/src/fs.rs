use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use leep_core::{BlobStore, Hash, Result};
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

#[async_trait]
impl BlobStore for FsBlobStore {
    async fn put_with_hash(&self, hash: Hash, data: Bytes) -> Result<()> {
        let path = self.path_for(&hash);
        if tokio::fs::try_exists(&path).await.map_err(storage_err)? {
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
