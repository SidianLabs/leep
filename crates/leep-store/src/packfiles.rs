//! A blob store's `packs/` as plain files (ADR 0039), so a backup or restore
//! copies a pack whole instead of object by object. A pack and its index are
//! immutable and named by content, so the same name means the same bytes on
//! both sides; tombstones only ever grow.

use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::{self, BoxStream, StreamExt, TryStreamExt};
use leep_core::{Error, Hash, Result};
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::objpacks;
use crate::packset::{
    self, parse_tombstones, sync_dir, tombstone_line, Tombstones, PACKS_DIR, TOMBSTONES,
};
use crate::storage_err;
use crate::tmpfile::create_exclusive;

pub type ByteStream = BoxStream<'static, Result<Bytes>>;

const CHUNK: usize = 1 << 20;

/// The files under a blob store's `packs/`: `<id>.pack`, `<id>.idx` and the
/// tombstones (one file on a filesystem, one empty object each in a bucket).
#[async_trait]
pub trait PackFiles: Send + Sync {
    /// Ids of the published packs (those with an index), sorted.
    async fn pack_ids(&self) -> Result<Vec<String>>;
    /// Length of a file, `None` if it is absent.
    async fn len(&self, name: &str) -> Result<Option<u64>>;
    /// A file's bytes in chunks, `None` if it is absent.
    async fn read(&self, name: &str) -> Result<Option<ByteStream>>;
    /// Write a file whole: a reader sees all of it under `name` or nothing.
    /// Durable when it returns. An error from `data` abandons the write.
    async fn write(&self, name: &str, data: ByteStream) -> Result<u64>;
    async fn delete(&self, name: &str) -> Result<()>;
    /// The tombstones, by pack id.
    async fn tombstones(&self) -> Result<Tombstones>;
    /// Add tombstones, durably.
    async fn add_tombstones(&self, add: &[(String, Hash)]) -> Result<()>;
}

/// Read a whole (small) file.
pub(crate) async fn read_all(files: &dyn PackFiles, name: &str) -> Result<Option<Vec<u8>>> {
    let Some(s) = files.read(name).await? else {
        return Ok(None);
    };
    let chunks: Vec<Bytes> = s.try_collect().await?;
    Ok(Some(chunks.concat()))
}

/// `packs/` of a filesystem blob store.
pub struct FsPackFiles {
    dir: PathBuf,
}

impl FsPackFiles {
    /// `root` is the blob store's directory, as given to `FsBlobStore::new`.
    pub fn new(root: impl AsRef<Path>) -> Self {
        FsPackFiles {
            dir: root.as_ref().join(PACKS_DIR),
        }
    }
}

fn not_found_none<T>(r: std::io::Result<T>) -> Result<Option<T>> {
    match r {
        Ok(v) => Ok(Some(v)),
        Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
        Err(e) => Err(storage_err(e)),
    }
}

async fn blocking<T: Send + 'static>(f: impl FnOnce() -> Result<T> + Send + 'static) -> Result<T> {
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| Error::Storage(format!("pack file task failed: {e}")))?
}

#[async_trait]
impl PackFiles for FsPackFiles {
    async fn pack_ids(&self) -> Result<Vec<String>> {
        let dir = self.dir.clone();
        blocking(move || packset::published_ids(&dir)).await
    }

    async fn len(&self, name: &str) -> Result<Option<u64>> {
        Ok(not_found_none(tokio::fs::metadata(self.dir.join(name)).await)?.map(|m| m.len()))
    }

    async fn read(&self, name: &str) -> Result<Option<ByteStream>> {
        let Some(file) = not_found_none(tokio::fs::File::open(self.dir.join(name)).await)? else {
            return Ok(None);
        };
        let s = stream::try_unfold(file, |mut f| async move {
            let mut buf = vec![0u8; CHUNK];
            let n = f.read(&mut buf).await.map_err(storage_err)?;
            if n == 0 {
                return Ok(None);
            }
            buf.truncate(n);
            Ok(Some((Bytes::from(buf), f)))
        });
        Ok(Some(s.boxed()))
    }

    /// A temp file in `packs/`, fsynced and renamed into place. A leftover
    /// temp file is garbage the next repack of this store deletes.
    async fn write(&self, name: &str, mut data: ByteStream) -> Result<u64> {
        tokio::fs::create_dir_all(&self.dir)
            .await
            .map_err(storage_err)?;
        let (tmp, file) = {
            let dir = self.dir.clone();
            blocking(move || create_exclusive(&dir, ".copy").map_err(storage_err)).await?
        };
        let res = async {
            let mut f = tokio::fs::File::from_std(file);
            let mut n = 0u64;
            while let Some(chunk) = data.next().await {
                let chunk = chunk?;
                f.write_all(&chunk).await.map_err(storage_err)?;
                n += chunk.len() as u64;
            }
            f.sync_all().await.map_err(storage_err)?;
            drop(f);
            tokio::fs::rename(&tmp, self.dir.join(name))
                .await
                .map_err(storage_err)?;
            Ok::<u64, Error>(n)
        }
        .await;
        if res.is_err() {
            let _ = tokio::fs::remove_file(&tmp).await;
        }
        let n = res?;
        let dir = self.dir.clone();
        blocking(move || sync_dir(&dir)).await?;
        Ok(n)
    }

    async fn delete(&self, name: &str) -> Result<()> {
        not_found_none(tokio::fs::remove_file(self.dir.join(name)).await)?;
        let dir = self.dir.clone();
        blocking(move || sync_dir(&dir)).await
    }

    async fn tombstones(&self) -> Result<Tombstones> {
        let read = tokio::fs::read(self.dir.join(TOMBSTONES)).await;
        Ok(not_found_none(read)?.map_or_else(Tombstones::new, |t| parse_tombstones(&t)))
    }

    async fn add_tombstones(&self, add: &[(String, Hash)]) -> Result<()> {
        let dir = self.dir.clone();
        let lines: String = add.iter().map(|(id, h)| tombstone_line(id, h)).collect();
        blocking(move || packset::append_tombstones(&dir, &lines)).await
    }
}

/// `packs/` keys of an `object_store` bucket, in the layout
/// `ObjectBlobStore` reads packs from (ADR 0040).
pub struct ObjectPackFiles {
    store: Arc<dyn ObjectStore>,
}

impl ObjectPackFiles {
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        ObjectPackFiles { store }
    }

    fn key(name: &str) -> ObjectPath {
        ObjectPath::from(format!("{PACKS_DIR}/{name}"))
    }
}

fn object_none<T>(r: object_store::Result<T>) -> Result<Option<T>> {
    match r {
        Ok(v) => Ok(Some(v)),
        Err(object_store::Error::NotFound { .. }) => Ok(None),
        Err(e) => Err(storage_err(e)),
    }
}

#[async_trait]
impl PackFiles for ObjectPackFiles {
    async fn pack_ids(&self) -> Result<Vec<String>> {
        Ok(objpacks::list_packs_dir(&*self.store).await?.0)
    }

    async fn len(&self, name: &str) -> Result<Option<u64>> {
        Ok(object_none(self.store.head(&Self::key(name)).await)?.map(|m| m.size))
    }

    async fn read(&self, name: &str) -> Result<Option<ByteStream>> {
        let Some(r) = object_none(self.store.get(&Self::key(name)).await)? else {
            return Ok(None);
        };
        Ok(Some(r.into_stream().map_err(storage_err).boxed()))
    }

    /// One put, or a multipart upload for a large file; either becomes
    /// visible only when complete.
    async fn write(&self, name: &str, mut data: ByteStream) -> Result<u64> {
        let mut w = object_store::buffered::BufWriter::new(self.store.clone(), Self::key(name));
        let mut n = 0u64;
        while let Some(chunk) = data.next().await {
            let chunk = match chunk {
                Ok(c) => c,
                Err(e) => {
                    let _ = w.abort().await;
                    return Err(e);
                }
            };
            n += chunk.len() as u64;
            if let Err(e) = w.put(chunk).await {
                let _ = w.abort().await;
                return Err(storage_err(e));
            }
        }
        w.shutdown().await.map_err(storage_err)?;
        Ok(n)
    }

    async fn delete(&self, name: &str) -> Result<()> {
        object_none(self.store.delete(&Self::key(name)).await)?;
        Ok(())
    }

    async fn tombstones(&self) -> Result<Tombstones> {
        objpacks::list_tombstones(&*self.store).await
    }

    async fn add_tombstones(&self, add: &[(String, Hash)]) -> Result<()> {
        for (id, h) in add {
            self.store
                .put(&objpacks::tombstone_key(id, h), Bytes::new().into())
                .await
                .map_err(storage_err)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A copy never writes through a temp file another writer already made.
    #[tokio::test]
    async fn write_does_not_reuse_an_existing_temp_file() {
        let root = tempfile::tempdir().unwrap();
        let files = FsPackFiles::new(root.path());
        let dir = root.path().join(PACKS_DIR);
        std::fs::create_dir_all(&dir).unwrap();
        let next = crate::tmpfile::TMP_COUNTER.load(std::sync::atomic::Ordering::Relaxed);
        let theirs: Vec<_> = (next..next + 16)
            .map(|n| {
                let nonce = crate::tmpfile::nonce();
                dir.join(format!(".tmp-{}-{nonce}-{n}.copy", std::process::id()))
            })
            .collect();
        for p in &theirs {
            std::fs::write(p, b"theirs").unwrap();
        }
        let body = stream::iter([Ok(Bytes::from_static(b"the pack"))]).boxed();
        files.write("a.pack", body).await.unwrap();
        assert_eq!(std::fs::read(dir.join("a.pack")).unwrap(), b"the pack");
        for p in &theirs {
            assert_eq!(std::fs::read(p).unwrap(), b"theirs");
        }
    }
}
