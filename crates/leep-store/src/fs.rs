use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use leep_core::{BlobMeta, BlobStore, Error, Hash, Result};
use std::io::Write;

use crate::dirsync::DirSyncer;
use crate::packset::{merge_listing, PackSet, DEFAULT_DECODED_CACHE};
use crate::storage_err;

static TMP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Most blocking tasks one `get_many` uses: enough to keep a disk's queue
/// and the cores busy, few enough to leave the blocking pool to others.
const READ_TASKS: usize = 16;
/// Fewest blobs given to one of those tasks, so a small batch is not spread
/// over more task hops than it saves.
const MIN_PER_READ_TASK: usize = 8;

/// Blobs on the local filesystem: loose at `<root>/<2 hex>/<62 hex>`, and
/// packed in `<root>/packs/` once `repack` has run (ADR 0029). New writes are
/// always loose; reads try the loose file first, then the packs.
///
/// A put syncs its file but not its directory; [`BlobStore::flush`] syncs
/// the directories of every put since the last flush (ADR 0044). Clones share
/// that record, so a flush on one covers puts through another.
#[derive(Clone)]
pub struct FsBlobStore {
    root: PathBuf,
    pub(crate) packs: Arc<PackSet>,
    dirs: Arc<DirSyncer>,
}

impl std::fmt::Debug for FsBlobStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FsBlobStore")
            .field("root", &self.root)
            .finish_non_exhaustive()
    }
}

impl FsBlobStore {
    /// Packs are found on first use, not here, so opening never fails.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self::with_decoded_cache(root, DEFAULT_DECODED_CACHE)
    }

    /// Like [`new`](Self::new), with the byte budget for decoded packed
    /// objects kept to speed up delta chains (0 disables it).
    pub fn with_decoded_cache(root: impl Into<PathBuf>, bytes: usize) -> Self {
        let root = root.into();
        let packs = Arc::new(PackSet::new(&root, bytes));
        let dirs = Arc::new(DirSyncer::new(&root));
        Self { root, packs, dirs }
    }

    /// Directory fsyncs issued by flushes so far, for tests.
    pub fn dir_syncs(&self) -> u64 {
        self.dirs.syncs()
    }

    /// Directories with renames a flush has not synced yet, for tests.
    pub fn unsynced_dirs(&self) -> usize {
        self.dirs.pending()
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Whether `hash` has a loose copy: what an incremental repack packs.
    /// One `stat`, for the history walk that picks delta bases.
    pub fn is_loose(&self, hash: &Hash) -> bool {
        self.path_for(hash).is_file()
    }

    pub(crate) fn path_for(&self, hash: &Hash) -> PathBuf {
        let hex = hash.to_hex();
        self.root.join(&hex[..2]).join(&hex[2..])
    }

    async fn blocking<T: Send + 'static>(
        &self,
        f: impl FnOnce(&PackSet) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let packs = self.packs.clone();
        tokio::task::spawn_blocking(move || f(&packs))
            .await
            .map_err(|e| Error::Storage(format!("pack task failed: {e}")))?
    }

    /// Loose blobs with hash strictly above `after`, at most `limit`.
    async fn list_loose(&self, after: Option<Hash>, limit: usize) -> Result<Vec<BlobMeta>> {
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

pub(crate) fn unix_secs(t: std::time::SystemTime) -> Option<i64> {
    match t.duration_since(std::time::UNIX_EPOCH) {
        Ok(d) => Some(d.as_secs() as i64),
        Err(_) => None,
    }
}

#[async_trait]
impl BlobStore for FsBlobStore {
    /// A packed copy does not count: the loose write refreshes `modified`,
    /// which GC's grace window needs (ADR 0012), and a loose copy also
    /// outlives a tombstone on the packed one. One blocking task does the
    /// whole write. The directory is synced by the next [`flush`](Self::flush),
    /// which is also told about a blob found already stored, since it may be
    /// another writer's rename that is not synced yet.
    async fn put_with_hash(&self, hash: Hash, data: Bytes) -> Result<()> {
        let path = self.path_for(&hash);
        let dirs = self.dirs.clone();
        let shard = hash.0[0];
        self.blocking(move |_| {
            if touch(&path).map_err(storage_err)? {
                dirs.mark(shard);
                return Ok(());
            }
            let dir = path.parent().expect("blob path has a parent");
            std::fs::create_dir_all(dir).map_err(storage_err)?;
            let tmp = dir.join(format!(
                ".tmp-{}-{}",
                std::process::id(),
                TMP_COUNTER.fetch_add(1, Ordering::Relaxed)
            ));
            let res = (|| {
                let mut f = std::fs::File::create(&tmp)?;
                f.write_all(&data)?;
                f.sync_all()?;
                drop(f);
                std::fs::rename(&tmp, &path)
            })();
            if let Err(e) = res {
                let _ = std::fs::remove_file(&tmp);
                return Err(storage_err(e));
            }
            dirs.mark(shard);
            Ok(())
        })
        .await
    }

    /// Syncs the shard directories (and the root for shards it has not
    /// synced it for) of every put since the last flush and of `named`, all
    /// at once. Concurrent flushes share waves.
    async fn flush(&self, named: &[Hash]) -> Result<()> {
        self.dirs.flush_all(named.iter().map(|h| h.0[0])).await
    }

    /// Syncs the shard directories of `hashes` (and the root for shards it
    /// has not synced it for) and nothing else, sharing syncs already running
    /// that started after this call.
    async fn flush_blobs(&self, hashes: &[Hash]) -> Result<()> {
        self.dirs.flush_shards(hashes.iter().map(|h| h.0[0])).await
    }

    /// Stats the loose files in a few blocking tasks, then looks up the
    /// misses in the packs after one rescan, instead of a task and a rescan
    /// per hash. Every miss is looked up in packs scanned after its stat, as
    /// in `contains`.
    async fn contains_many(&self, hashes: &[Hash]) -> Result<Vec<bool>> {
        const CHUNK: usize = 4096;
        if hashes.len() <= 1 {
            let mut out = Vec::with_capacity(hashes.len());
            for h in hashes {
                out.push(self.contains(h).await?);
            }
            return Ok(out);
        }
        let mut tasks = Vec::new();
        for chunk in hashes.chunks(CHUNK) {
            let paths: Vec<PathBuf> = chunk.iter().map(|h| self.path_for(h)).collect();
            tasks.push(tokio::task::spawn_blocking(move || {
                paths
                    .iter()
                    .map(|p| p.try_exists().map_err(storage_err))
                    .collect::<Result<Vec<bool>>>()
            }));
        }
        let mut found = Vec::with_capacity(hashes.len());
        for t in tasks {
            found.extend(
                t.await
                    .map_err(|e| Error::Storage(format!("stat task failed: {e}")))??,
            );
        }
        let misses: Vec<(usize, Hash)> = found
            .iter()
            .enumerate()
            .filter(|(_, f)| !**f)
            .map(|(i, _)| (i, hashes[i]))
            .collect();
        if misses.is_empty() {
            return Ok(found);
        }
        let packed = self
            .blocking(move |p| {
                let v = p.refresh()?;
                Ok(misses
                    .into_iter()
                    .filter(|(_, h)| v.locate(h).is_some())
                    .map(|(i, _)| i)
                    .collect::<Vec<usize>>())
            })
            .await?;
        for i in packed {
            found[i] = true;
        }
        Ok(found)
    }

    /// The loose file and the packs are read in one blocking task, so a
    /// packed read costs one hop to the blocking pool like a loose one.
    async fn get(&self, hash: &Hash) -> Result<Option<Bytes>> {
        let path = self.path_for(hash);
        let hash = *hash;
        self.blocking(move |p| match std::fs::read(&path) {
            Ok(v) => Ok(Some(v.into())),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => p.get(&hash),
            Err(e) => Err(storage_err(e)),
        })
        .await
    }

    /// Split over up to [`READ_TASKS`] blocking tasks, each of which first
    /// asks the kernel for every pack frame its blobs need (see
    /// [`PackSet::prefetch`]), so a cold read of many packed objects waits on
    /// the disk for all of them at once instead of one frame at a time, and
    /// decodes on several cores.
    async fn get_many(&self, hashes: &[Hash]) -> Result<Vec<Option<Bytes>>> {
        if hashes.len() <= 1 {
            let mut out = Vec::with_capacity(hashes.len());
            for h in hashes {
                out.push(self.get(h).await?);
            }
            return Ok(out);
        }
        let per_task = hashes.len().div_ceil(READ_TASKS).max(MIN_PER_READ_TASK);
        let mut tasks = Vec::new();
        for chunk in hashes.chunks(per_task) {
            let jobs: Vec<(Hash, PathBuf)> = chunk.iter().map(|h| (*h, self.path_for(h))).collect();
            let packs = self.packs.clone();
            tasks.push(tokio::task::spawn_blocking(move || {
                let mut out: Vec<Option<Bytes>> = Vec::with_capacity(jobs.len());
                let mut packed = Vec::new();
                for (i, (hash, path)) in jobs.iter().enumerate() {
                    match std::fs::read(path) {
                        Ok(v) => out.push(Some(v.into())),
                        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                            out.push(None);
                            packed.push((i, *hash));
                        }
                        Err(e) => return Err(storage_err(e)),
                    }
                }
                let wanted: Vec<Hash> = packed.iter().map(|(_, h)| *h).collect();
                packs.prefetch(&wanted)?;
                for (i, hash) in packed {
                    out[i] = packs.get(&hash)?;
                }
                Ok(out)
            }));
        }
        let mut out = Vec::with_capacity(hashes.len());
        for t in tasks {
            out.extend(
                t.await
                    .map_err(|e| Error::Storage(format!("pack task failed: {e}")))??,
            );
        }
        Ok(out)
    }

    /// Rereads the tombstones if they changed, so a blob GC deleted from a
    /// pack in another process is not reported present.
    async fn contains(&self, hash: &Hash) -> Result<bool> {
        let path = self.path_for(hash);
        let hash = *hash;
        self.blocking(move |p| match path.try_exists() {
            Ok(true) => Ok(true),
            Ok(false) => p.contains(&hash),
            Err(e) => Err(storage_err(e)),
        })
        .await
    }

    /// Loose and packed blobs merged in hash order. Sizes are logical, so a
    /// packed store lists like an unpacked one (the backup copy compares
    /// sizes, ADR 0026); `modified` of a packed blob is the time it was last
    /// written before it was packed.
    async fn list_page(&self, after: Option<Hash>, limit: usize) -> Result<Vec<BlobMeta>> {
        let view = self.blocking(|p| p.refresh()).await?;
        let loose = self.list_loose(after, limit).await?;
        if view.is_empty() {
            return Ok(loose);
        }
        let packs = view.packs.iter().map(|(p, dead)| (&p.index, dead));
        Ok(merge_listing(packs, loose, after, limit))
    }

    /// Tombstones packed copies first, then removes the loose file, so a
    /// repack that finds the tombstone keeps the loose copy (see `repack`).
    async fn delete(&self, hash: &Hash) -> Result<()> {
        let h = *hash;
        self.blocking(move |p| p.tombstone(&h)).await?;
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

    /// A put syncs its file but leaves its directory (and a new shard's
    /// entry in the root) to the next flush, which syncs each once (ADR
    /// 0044). Finding a blob already stored also counts, since that may be
    /// another writer's unsynced rename, and so does naming it in a flush.
    #[tokio::test]
    async fn puts_leave_their_directories_to_the_next_flush() {
        let dir = tempfile::tempdir().unwrap();
        let store = FsBlobStore::new(dir.path());
        let mut shards = std::collections::HashSet::new();
        for i in 0..20u32 {
            let h = store.put(Bytes::from(format!("blob {i}"))).await.unwrap();
            shards.insert(h.0[0]);
        }
        assert_eq!(store.dir_syncs(), 0, "no put synced a directory");
        assert_eq!(store.unsynced_dirs(), shards.len() + 1, "shards and root");
        store.flush(&[]).await.unwrap();
        assert_eq!(store.dir_syncs(), shards.len() as u64 + 1);
        assert_eq!(store.unsynced_dirs(), 0);
        store.flush(&[]).await.unwrap();
        assert_eq!(store.dir_syncs(), shards.len() as u64 + 1, "nothing new");

        // Another handle on the same directory: a process that did not write
        // the blob, or one restarted since.
        let other = FsBlobStore::new(dir.path());
        let h = store.put(Bytes::from_static(b"blob 0")).await.unwrap();
        assert_eq!(store.unsynced_dirs(), 1, "found present: its shard");
        other.flush(&[h]).await.unwrap();
        assert_eq!(other.dir_syncs(), 2, "the named shard and, once, the root");
        let packed_only = Hash::of_blob(b"in no shard directory");
        other.flush(&[packed_only]).await.unwrap();
    }

    /// `flush_blobs` syncs the shards of the blobs it names and nothing else,
    /// so a create does not wait for other requests' uploads (ADR 0048).
    #[tokio::test]
    async fn flush_blobs_syncs_only_their_shards() {
        let dir = tempfile::tempdir().unwrap();
        let store = FsBlobStore::new(dir.path());
        let mut by_shard = std::collections::BTreeMap::new();
        let mut i = 0u32;
        while by_shard.len() < 3 {
            let h = store.put(Bytes::from(format!("blob {i}"))).await.unwrap();
            by_shard.entry(h.0[0]).or_insert(h);
            i += 1;
        }
        let pending = store.unsynced_dirs();
        let mine = *by_shard.values().next().unwrap();
        store.flush_blobs(&[mine]).await.unwrap();
        assert_eq!(store.dir_syncs(), 2, "its shard and the root");
        assert_eq!(store.unsynced_dirs(), pending - 2, "the other shards");
        store.flush(&[]).await.unwrap();
        assert_eq!(store.unsynced_dirs(), 0);
    }

    #[tokio::test]
    async fn contains_many_matches_contains() {
        let dir = tempfile::tempdir().unwrap();
        let store = FsBlobStore::new(dir.path());
        let mut asked = Vec::new();
        for i in 0..5000u32 {
            let data = Bytes::from(format!("blob {i}"));
            if i % 3 == 0 {
                asked.push(store.put(data).await.unwrap());
            } else {
                asked.push(Hash::of_blob(&data));
            }
        }
        let many = store.contains_many(&asked).await.unwrap();
        for (h, found) in asked.iter().zip(&many) {
            assert_eq!(*found, store.contains(h).await.unwrap());
        }
        assert_eq!(many.iter().filter(|f| **f).count(), 1667);
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

    /// Every file under a shard directory, as (name, bytes).
    fn files(root: &Path) -> Vec<(String, Vec<u8>)> {
        let mut out = Vec::new();
        for shard in std::fs::read_dir(root).into_iter().flatten().flatten() {
            for f in std::fs::read_dir(shard.path())
                .into_iter()
                .flatten()
                .flatten()
            {
                out.push((
                    format!(
                        "{}{}",
                        shard.file_name().to_string_lossy(),
                        f.file_name().to_string_lossy()
                    ),
                    std::fs::read(f.path()).unwrap(),
                ));
            }
        }
        out
    }

    /// A blob is published whole or not at all: a put abandoned at any
    /// suspension point (a timeout, a killed request) leaves either no file at
    /// the blob's address or the complete bytes, and never a blob that does
    /// not hash to its name. Leftover temp files are not blobs.
    #[tokio::test]
    async fn abandoned_puts_never_publish_a_partial_blob() {
        use leep_testkit::cancel_after_polls;
        let dir = tempfile::tempdir().unwrap();
        let store = FsBlobStore::new(dir.path());
        let data = Bytes::from(vec![0x5a; 3 << 20]);
        let hash = Hash::of_blob(&data);
        let mut finished = 0;
        for polls in 0..40 {
            if cancel_after_polls(store.put(data.clone()), polls)
                .await
                .is_some()
            {
                finished += 1;
            }
            // Let in-flight blocking file operations land before looking.
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            match store.get(&hash).await.unwrap() {
                None => assert!(!store.contains(&hash).await.unwrap()),
                Some(b) => assert_eq!(b, data, "published bytes are whole"),
            }
            for (name, bytes) in files(dir.path()) {
                if !name.contains(".tmp-") {
                    assert_eq!(Hash::of_blob(&bytes).to_hex(), name, "a file at an address");
                }
            }
            for m in store.list_page(None, 100).await.unwrap() {
                assert_eq!(m.hash, hash, "only real blobs are listed");
            }
        }
        assert!(finished > 0, "the later attempts ran to the end");
        // Whatever was left behind, a full put works and reads back.
        store.put(data.clone()).await.unwrap();
        assert_eq!(store.get(&hash).await.unwrap().unwrap(), data);
    }

    /// What a crash between writing the temp file and renaming it leaves.
    #[tokio::test]
    async fn crash_leftovers_are_not_blobs() {
        let dir = tempfile::tempdir().unwrap();
        let store = FsBlobStore::new(dir.path());
        let data = Bytes::from("whole blob");
        let hash = Hash::of_blob(&data);
        let hex = hash.to_hex();
        let shard = dir.path().join(&hex[..2]);
        std::fs::create_dir_all(&shard).unwrap();
        // Full and torn temp files, one of them named like another process's.
        std::fs::write(shard.join(".tmp-4242-0"), &data).unwrap();
        std::fs::write(shard.join(".tmp-4242-1"), &data[..4]).unwrap();
        assert!(store.get(&hash).await.unwrap().is_none());
        assert!(!store.contains(&hash).await.unwrap());
        assert!(store.list_page(None, 10).await.unwrap().is_empty());
        // A put does not mistake them for the blob, and a delete leaves them.
        store.put(data.clone()).await.unwrap();
        assert_eq!(store.get(&hash).await.unwrap().unwrap(), data);
        assert_eq!(store.list_page(None, 10).await.unwrap().len(), 1);
        store.delete(&hash).await.unwrap();
        assert!(store.get(&hash).await.unwrap().is_none());
        assert!(shard.join(".tmp-4242-1").exists());
    }

    /// Errors injected around a real filesystem store: a put that fails after
    /// applying leaves a complete blob, and retrying is harmless.
    #[tokio::test]
    async fn put_failing_after_it_applied_leaves_a_whole_blob() {
        use leep_testkit::{is_injected, Fault, FaultPlan, FaultyBlobStore, Op};
        let dir = tempfile::tempdir().unwrap();
        let plan = FaultPlan::new(3);
        plan.script(Op::Put, 1, Fault::FailAfter)
            .script(Op::Put, 2, Fault::Fail);
        let inner = std::sync::Arc::new(FsBlobStore::new(dir.path()));
        let store = FaultyBlobStore::new(inner.clone(), plan);
        let data = Bytes::from("ambiguous");
        let hash = Hash::of_blob(&data);
        assert!(is_injected(&store.put(data.clone()).await.unwrap_err()));
        assert_eq!(
            inner.get(&hash).await.unwrap().unwrap(),
            data,
            "it was applied"
        );
        assert!(is_injected(&store.put(data.clone()).await.unwrap_err()));
        assert_eq!(store.put(data.clone()).await.unwrap(), hash);
        assert_eq!(files(dir.path()).len(), 1, "no duplicate and no temp file");
    }
}
