//! Incremental blob copy between two stores, for backup and restore
//! (ADR 0026), and the copy of whole packs that goes first when both sides
//! keep packs (ADR 0039).
//!
//! Blobs are immutable and named by their content, so "copy what the other
//! side lacks" is always safe: a blob present under a hash holds the bytes of
//! that hash, whenever and by whomever it was written. The two ascending
//! listings are merged, so an unchanged store costs two listings and no reads.

use std::collections::{HashSet, VecDeque};
use std::sync::{Arc, Mutex};

use futures::stream::{self, StreamExt};
use leep_core::{BlobMeta, BlobStore, Error, Hash, Result, TreeNode};

use crate::pack::{self, PackIndex};
use crate::packfiles::{read_all, PackFiles};

/// Listing page size.
const PAGE: usize = 1000;

/// Told about progress about every five seconds.
pub type SyncProgress = Arc<dyn Fn(&SyncReport) + Send + Sync>;

#[derive(Clone)]
pub struct SyncOptions {
    /// Blobs copied at once. Each is held in memory while it is copied, so
    /// this times the largest blob bounds memory.
    pub concurrency: usize,
    /// Called with the totals so far.
    pub progress: Option<SyncProgress>,
}

impl Default for SyncOptions {
    fn default() -> Self {
        SyncOptions {
            concurrency: 8,
            progress: None,
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct SyncReport {
    /// Blobs the source listed.
    pub listed: u64,
    /// Already in the destination with the same size.
    pub present: u64,
    /// Copied (includes `replaced`).
    pub copied: u64,
    pub bytes_copied: u64,
    /// Present in the destination with a different size, so deleted and
    /// copied again (a torn or foreign object).
    pub replaced: u64,
    /// Listed by the source and gone when read: garbage collection deleted an
    /// unreferenced blob in between. Nothing to copy.
    pub vanished: u64,
    /// Bytes that do not hash to their name; not copied. A snapshot that
    /// references one fails its closure check.
    pub corrupt: Vec<Hash>,
    /// In the destination only: blobs the source has since deleted, or that an
    /// earlier snapshot needs. Kept.
    pub only_in_destination: u64,
}

/// Do `data` hash to `hash`, as a file blob or as a tree node?
pub fn blob_intact(hash: &Hash, data: &[u8]) -> bool {
    Hash::of_blob(data) == *hash || TreeNode::decode(data).is_ok_and(|n| n.hash() == *hash)
}

/// A listing read a page at a time.
struct Cursor<'a> {
    store: &'a dyn BlobStore,
    buf: VecDeque<BlobMeta>,
    after: Option<Hash>,
    done: bool,
}

impl<'a> Cursor<'a> {
    fn new(store: &'a dyn BlobStore) -> Self {
        Cursor {
            store,
            buf: VecDeque::new(),
            after: None,
            done: false,
        }
    }

    async fn fill(&mut self) -> Result<()> {
        if self.buf.is_empty() && !self.done {
            let page = self.store.list_page(self.after, PAGE).await?;
            match page.last() {
                Some(last) => self.after = Some(last.hash),
                None => self.done = true,
            }
            self.buf.extend(page);
        }
        Ok(())
    }

    /// Drop everything below `hash`, returning how many were dropped, then
    /// the entry at `hash` if there is one.
    async fn seek(&mut self, hash: &Hash, skipped: &mut u64) -> Result<Option<BlobMeta>> {
        loop {
            self.fill().await?;
            match self.buf.front() {
                None => return Ok(None),
                Some(m) if m.hash < *hash => {
                    *skipped += 1;
                    self.buf.pop_front();
                }
                Some(m) if m.hash == *hash => return Ok(Some(*m)),
                Some(_) => return Ok(None),
            }
        }
    }

    async fn count_rest(&mut self) -> Result<u64> {
        let mut n = 0;
        loop {
            self.fill().await?;
            if self.buf.is_empty() {
                return Ok(n);
            }
            n += self.buf.len() as u64;
            self.buf.clear();
        }
    }
}

enum Outcome {
    Copied(u64),
    Vanished,
    Corrupt(Hash),
}

async fn copy_one(
    src: &dyn BlobStore,
    dst: &dyn BlobStore,
    m: BlobMeta,
    replace: bool,
) -> Result<Outcome> {
    let Some(data) = src.get(&m.hash).await? else {
        return Ok(Outcome::Vanished);
    };
    if !blob_intact(&m.hash, &data) {
        return Ok(Outcome::Corrupt(m.hash));
    }
    if replace {
        dst.delete(&m.hash).await?;
    }
    let n = data.len() as u64;
    dst.put_with_hash(m.hash, data).await?;
    Ok(Outcome::Copied(n))
}

/// Copy every blob of `src` that `dst` lacks. Blobs only in `dst` stay.
/// Safe to repeat and to interrupt: a half-finished run leaves whole blobs
/// only (the stores write atomically) and the next run continues from there.
/// Everything copied is flushed (`BlobStore::flush`) before it returns.
pub async fn sync_blobs(
    src: &dyn BlobStore,
    dst: &dyn BlobStore,
    opts: &SyncOptions,
) -> Result<SyncReport> {
    if opts.concurrency == 0 {
        return Err(Error::Invalid("concurrency must be at least 1".into()));
    }
    let mut report = SyncReport::default();
    let mut dst_cursor = Cursor::new(dst);
    let mut after: Option<Hash> = None;
    let mut last_progress = std::time::Instant::now();
    loop {
        let page = src.list_page(after, PAGE).await?;
        let Some(last) = page.last() else { break };
        after = Some(last.hash);
        let mut todo: Vec<(BlobMeta, bool)> = Vec::new();
        for m in &page {
            report.listed += 1;
            match dst_cursor
                .seek(&m.hash, &mut report.only_in_destination)
                .await?
            {
                Some(d) if d.size == m.size => {
                    report.present += 1;
                    dst_cursor.buf.pop_front();
                }
                Some(_) => {
                    dst_cursor.buf.pop_front();
                    report.replaced += 1;
                    todo.push((*m, true));
                }
                None => todo.push((*m, false)),
            }
        }
        let mut results = stream::iter(todo.into_iter().map(|(m, replace)| async move {
            copy_one(src, dst, m, replace)
                .await
                .map(|o| (m, replace, o))
        }))
        .buffer_unordered(opts.concurrency);
        while let Some(r) = results.next().await {
            match r? {
                (_, _, Outcome::Copied(n)) => {
                    report.copied += 1;
                    report.bytes_copied += n;
                }
                (_, replace, Outcome::Vanished) => {
                    report.vanished += 1;
                    if replace {
                        report.replaced -= 1;
                    }
                }
                (_, replace, Outcome::Corrupt(h)) => {
                    if replace {
                        report.replaced -= 1;
                    }
                    report.corrupt.push(h);
                }
            }
        }
        drop(results);
        if let Some(p) = &opts.progress {
            if last_progress.elapsed() >= std::time::Duration::from_secs(5) {
                last_progress = std::time::Instant::now();
                p(&report);
            }
        }
    }
    report.only_in_destination += dst_cursor.count_rest().await?;
    // A backup or restore is sealed after this returns, so every copy must
    // survive a power loss by then (ADR 0044).
    dst.flush(&[]).await?;
    Ok(report)
}

/// What a pack copy is for, which decides what happens to tombstones.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PackSyncMode {
    /// Into a backup pool, the union of every sealed snapshot. The pool only
    /// grows: the source's tombstones are not copied, since an older
    /// snapshot may still name an object the source has since deleted (the
    /// changeset sweep, ADR 0059, issue #155). Pool packs a source repack
    /// merged away are pruned when other pool packs hold every object they
    /// hold.
    Pool,
    /// Into a store being restored. The source's tombstones of the packs
    /// the destination holds are copied, so the target reads exactly as the
    /// source (the pool) does. Nothing is pruned.
    Restore,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct PackSyncReport {
    /// Packs the source has published.
    pub listed: u64,
    /// Already published in the destination.
    pub present: u64,
    /// Copied, and their pack plus index bytes.
    pub copied: u64,
    pub bytes_copied: u64,
    /// Replaced in the source (by a repack) before they could be read. Their
    /// objects are in the pack that replaced them, or copied loose.
    pub vanished: u64,
    /// Source packs whose bytes do not match their index; not copied.
    pub corrupt: Vec<String>,
    /// Tombstone lines copied for packs the destination holds; always zero
    /// into a pool.
    pub tombstones_added: u64,
    /// Destination packs the source no longer has, deleted because other
    /// destination packs hold every object they do.
    pub pruned: u64,
}

/// Copy the packs `src` has published that `dst` lacks, each as its two
/// files, then (on a restore) the tombstones of the packs `dst` holds. Run
/// it before [`sync_blobs`]: a destination that reads packs then lists
/// their objects as present, so only loose objects are copied one by one,
/// and one that does not still gets every object loose.
///
/// The pack file goes first and the index publishes it, as in a repack, so
/// an interrupted copy leaves an unpublished pack that the next run writes
/// again. Each pack is checked against the length and BLAKE3 its index
/// records while it streams, and the index against its name, so a damaged
/// source pack is reported and never published in the destination.
///
/// Into a pool, a destination pack the source no longer has (a repack
/// merged it into a bigger one) is deleted when the destination's other
/// packs hold every object it holds, so a backup pool does not keep every
/// intermediate pack. Objects are never lost that way; a pack that holds
/// anything no other pack does stays.
pub async fn sync_packs(
    src: &dyn PackFiles,
    dst: &dyn PackFiles,
    mode: PackSyncMode,
) -> Result<PackSyncReport> {
    let mut report = PackSyncReport::default();
    let src_ids = src.pack_ids().await?;
    let dst_ids: HashSet<String> = dst.pack_ids().await?.into_iter().collect();
    report.listed = src_ids.len() as u64;
    for id in &src_ids {
        if dst_ids.contains(id) {
            report.present += 1;
            continue;
        }
        let idx_name = format!("{id}.idx");
        let pack_name = format!("{id}.pack");
        let Some(idx) = read_all(src, &idx_name).await? else {
            report.vanished += 1;
            continue;
        };
        let index = match (pack::pack_id(&idx) == *id).then(|| PackIndex::decode(idx.clone())) {
            Some(Ok(i)) => i,
            _ => {
                report.corrupt.push(id.clone());
                continue;
            }
        };
        let Some(data) = src.read(&pack_name).await? else {
            report.vanished += 1;
            continue;
        };
        let hasher = Arc::new(Mutex::new((blake3::Hasher::new(), 0u64)));
        let h = hasher.clone();
        let want = index.pack_len();
        let data = data
            .map(move |chunk| {
                let chunk = chunk?;
                let mut g = h.lock().unwrap();
                g.0.update(&chunk);
                g.1 += chunk.len() as u64;
                if g.1 > want {
                    return Err(Error::Storage("pack longer than its index says".into()));
                }
                Ok(chunk)
            })
            .boxed();
        let n = match dst.write(&pack_name, data).await {
            Ok(n) => n,
            Err(_) if hasher.lock().unwrap().1 > want => {
                report.corrupt.push(id.clone());
                continue;
            }
            Err(e) => return Err(e),
        };
        let digest = *hasher.lock().unwrap().0.finalize().as_bytes();
        if n != want || &digest != index.pack_hash() {
            dst.delete(&pack_name).await?;
            report.corrupt.push(id.clone());
            continue;
        }
        let idx_len = idx.len() as u64;
        dst.write(&idx_name, stream::once(async { Ok(idx.into()) }).boxed())
            .await?;
        report.copied += 1;
        report.bytes_copied += n + idx_len;
    }

    match mode {
        PackSyncMode::Restore => report.tombstones_added = copy_tombstones(src, dst).await?,
        PackSyncMode::Pool => {
            let source: HashSet<&String> = src_ids.iter().collect();
            report.pruned = prune_superseded(dst, &source).await?;
        }
    }
    Ok(report)
}

/// Add the source's tombstones of the packs `dst` holds that `dst` lacks.
async fn copy_tombstones(src: &dyn PackFiles, dst: &dyn PackFiles) -> Result<u64> {
    let held: HashSet<String> = dst.pack_ids().await?.into_iter().collect();
    let theirs = src.tombstones().await?;
    let ours = dst.tombstones().await?;
    let mut add = Vec::new();
    for (id, hashes) in &theirs {
        if !held.contains(id) {
            continue;
        }
        let mut new: Vec<&Hash> = hashes
            .iter()
            .filter(|h| !ours.get(id).is_some_and(|o| o.contains(h)))
            .collect();
        new.sort();
        add.extend(new.into_iter().map(|h| (id.clone(), *h)));
    }
    add.sort();
    if !add.is_empty() {
        dst.add_tombstones(&add).await?;
    }
    Ok(add.len() as u64)
}

/// Delete destination packs the source no longer has when every object they
/// hold is in another destination pack, and readable there wherever it is
/// readable in them. Index first, as a repack does. Tombstones never make
/// an object prunable: a pool's tombstones (only written before issue #155
/// was fixed) may hide what an older snapshot needs, and its bytes must stay.
async fn prune_superseded(dst: &dyn PackFiles, source: &HashSet<&String>) -> Result<u64> {
    let ids = dst.pack_ids().await?;
    if ids.iter().all(|id| source.contains(id)) {
        return Ok(0);
    }
    let dead = dst.tombstones().await?;
    let mut packs: Vec<(String, PackIndex)> = Vec::new();
    for id in ids {
        if let Some(bytes) = read_all(dst, &format!("{id}.idx")).await? {
            if pack::pack_id(&bytes) != id {
                continue;
            }
            packs.push((id, PackIndex::decode(bytes)?));
        }
    }
    let no_dead = HashSet::new();
    let live = |id: &str, h: &Hash| !dead.get(id).unwrap_or(&no_dead).contains(h);
    // Another pack keeps `h`'s bytes, and keeps it readable if `id` does.
    let kept = |i: usize, id: &str, h: &Hash, packs: &[(String, PackIndex)]| {
        packs
            .iter()
            .enumerate()
            .any(|(j, (oid, o))| j != i && o.find(h).is_some() && (live(oid, h) || !live(id, h)))
    };
    let mut pruned = 0;
    let mut i = 0;
    while i < packs.len() {
        let (id, index) = &packs[i];
        let covered = !source.contains(id) && index.hashes().all(|h| kept(i, id, &h, &packs));
        if covered {
            dst.delete(&format!("{id}.idx")).await?;
            dst.delete(&format!("{id}.pack")).await?;
            packs.remove(i);
            pruned += 1;
        } else {
            i += 1;
        }
    }
    Ok(pruned)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use leep_core::memory::MemoryBlobStore;
    use leep_core::{FileKind, TreeEntry};

    async fn fill(store: &MemoryBlobStore, n: usize, tag: &str) -> Vec<Hash> {
        let mut v = Vec::new();
        for i in 0..n {
            v.push(store.put(Bytes::from(format!("{tag}-{i}"))).await.unwrap());
        }
        v
    }

    fn opts() -> SyncOptions {
        SyncOptions {
            concurrency: 4,
            progress: None,
        }
    }

    #[tokio::test]
    async fn copies_what_is_missing_and_only_that() {
        let (src, dst) = (MemoryBlobStore::new(), MemoryBlobStore::new());
        // More than one listing page, so the merge crosses page boundaries.
        let shared = fill(&src, 1500, "shared").await;
        for h in shared.iter().step_by(2) {
            dst.put_with_hash(*h, src.get(h).await.unwrap().unwrap())
                .await
                .unwrap();
        }
        fill(&src, 300, "only-src").await;
        let extra = fill(&dst, 40, "only-dst").await;

        let r = sync_blobs(&src, &dst, &opts()).await.unwrap();
        assert_eq!(r.listed, 1800);
        assert_eq!(r.present, 750);
        assert_eq!(r.copied, 1050);
        assert_eq!(r.only_in_destination, 40);
        assert_eq!((r.vanished, r.replaced, r.corrupt.len()), (0, 0, 0));
        assert_eq!(dst.len(), 1840);
        for h in &extra {
            assert!(
                dst.contains(h).await.unwrap(),
                "destination-only blobs stay"
            );
        }

        // Incremental: nothing left to copy, (the second run lists both sides and reads no blob).
        let r = sync_blobs(&src, &dst, &opts()).await.unwrap();
        assert_eq!((r.copied, r.present, r.only_in_destination), (0, 1800, 40));
    }

    #[tokio::test]
    async fn tree_nodes_copy_under_their_own_address() {
        let (src, dst) = (MemoryBlobStore::new(), MemoryBlobStore::new());
        let mut node = TreeNode::default();
        node.entries.insert(
            "a".into(),
            TreeEntry::File {
                content: Hash::of_blob(b"a"),
                kind: FileKind::Regular,
                size: 1,
            },
        );
        src.put_with_hash(node.hash(), Bytes::from(node.encode()))
            .await
            .unwrap();
        let r = sync_blobs(&src, &dst, &opts()).await.unwrap();
        assert_eq!((r.copied, r.corrupt.len()), (1, 0));
        assert!(dst.contains(&node.hash()).await.unwrap());
    }

    #[tokio::test]
    async fn corrupt_source_blobs_are_reported_not_copied() {
        let (src, dst) = (MemoryBlobStore::new(), MemoryBlobStore::new());
        let good = fill(&src, 3, "good").await;
        let bad = Hash::of_blob(b"what it should hold");
        src.put_with_hash(bad, Bytes::from_static(b"what it holds"))
            .await
            .unwrap();
        let r = sync_blobs(&src, &dst, &opts()).await.unwrap();
        assert_eq!((r.copied, r.corrupt.clone()), (3, vec![bad]));
        assert!(!dst.contains(&bad).await.unwrap());
        assert!(dst.contains(&good[0]).await.unwrap());
    }

    #[tokio::test]
    async fn a_blob_of_the_wrong_size_in_the_destination_is_replaced() {
        let (src, dst) = (MemoryBlobStore::new(), MemoryBlobStore::new());
        let h = src
            .put(Bytes::from_static(b"the whole blob"))
            .await
            .unwrap();
        dst.put_with_hash(h, Bytes::from_static(b"torn"))
            .await
            .unwrap();
        let r = sync_blobs(&src, &dst, &opts()).await.unwrap();
        assert_eq!((r.replaced, r.copied), (1, 1));
        assert_eq!(dst.get(&h).await.unwrap().unwrap(), "the whole blob");
    }

    /// A source that loses a blob between listing it and reading it, as when
    /// garbage collection deletes an unreferenced one during the copy.
    struct Losing {
        inner: MemoryBlobStore,
        victim: Hash,
    }

    #[async_trait::async_trait]
    impl BlobStore for Losing {
        async fn put_with_hash(&self, h: Hash, d: Bytes) -> Result<()> {
            self.inner.put_with_hash(h, d).await
        }
        async fn get(&self, h: &Hash) -> Result<Option<Bytes>> {
            self.inner.delete(&self.victim).await?;
            self.inner.get(h).await
        }
        async fn list_page(&self, a: Option<Hash>, l: usize) -> Result<Vec<BlobMeta>> {
            self.inner.list_page(a, l).await
        }
        async fn delete(&self, h: &Hash) -> Result<()> {
            self.inner.delete(h).await
        }
    }

    #[tokio::test]
    async fn a_blob_deleted_during_the_copy_is_skipped_not_an_error() {
        let inner = MemoryBlobStore::new();
        let hashes = fill(&inner, 10, "x").await;
        let victim = *hashes.iter().max().unwrap();
        let src = Losing { inner, victim };
        let dst = MemoryBlobStore::new();
        let r = sync_blobs(
            &src,
            &dst,
            &SyncOptions {
                concurrency: 1,
                progress: None,
            },
        )
        .await
        .unwrap();
        assert_eq!(r.listed, 10);
        assert_eq!(r.vanished, 1);
        assert_eq!(r.copied, 9);
        assert!(!dst.contains(&victim).await.unwrap());
    }

    #[tokio::test]
    async fn zero_concurrency_is_refused() {
        let s = MemoryBlobStore::new();
        let o = SyncOptions {
            concurrency: 0,
            progress: None,
        };
        assert!(sync_blobs(&s, &s, &o).await.is_err());
    }

    mod packs {
        use super::*;
        use crate::{
            FsBlobStore, FsPackFiles, ObjectBlobStore, ObjectPackFiles, RepackMode, RepackOptions,
        };
        use object_store::memory::InMemory;
        use object_store::ObjectStoreExt;

        async fn repack(store: &FsBlobStore, mode: RepackMode) {
            let opts = RepackOptions {
                mode,
                ..RepackOptions::default()
            };
            store
                .repack_publish(Vec::new(), opts)
                .await
                .unwrap()
                .finish()
                .await
                .unwrap();
        }

        const INCREMENTAL: RepackMode = RepackMode::Incremental {
            factor: 2,
            max_packs: 8,
        };

        async fn all(store: &dyn BlobStore) -> Vec<Hash> {
            let mut out = Vec::new();
            let mut after = None;
            loop {
                let page = store.list_page(after, 100).await.unwrap();
                let Some(last) = page.last() else { return out };
                after = Some(last.hash);
                out.extend(page.iter().map(|m| m.hash));
            }
        }

        /// A packed source with loose objects beside its packs and a
        /// deleted packed object.
        async fn source(dir: &std::path::Path) -> (FsBlobStore, Vec<Hash>) {
            let src = FsBlobStore::new(dir);
            let mut hashes = Vec::new();
            for i in 0..40 {
                hashes.push(src.put(Bytes::from(format!("object {i}"))).await.unwrap());
            }
            repack(&src, RepackMode::Full).await;
            for i in 40..45 {
                hashes.push(src.put(Bytes::from(format!("object {i}"))).await.unwrap());
            }
            repack(&src, INCREMENTAL).await;
            src.delete(&hashes[3]).await.unwrap();
            for i in 45..48 {
                hashes.push(src.put(Bytes::from(format!("object {i}"))).await.unwrap());
            }
            (src, hashes)
        }

        #[tokio::test]
        async fn packs_copy_whole_to_a_filesystem_store_and_objects_follow() {
            let (a, b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
            let (src, hashes) = source(a.path()).await;
            let dst = FsBlobStore::new(b.path());
            let (sp, dp) = (FsPackFiles::new(a.path()), FsPackFiles::new(b.path()));
            let r = sync_packs(&sp, &dp, PackSyncMode::Pool).await.unwrap();
            assert_eq!((r.listed, r.copied, r.present), (2, 2, 0), "{r:?}");
            assert_eq!(r.tombstones_added, 0, "{r:?}");
            assert!(dp.tombstones().await.unwrap().is_empty());
            assert!(r.corrupt.is_empty());
            let b2 = sync_blobs(&src, &dst, &SyncOptions::default())
                .await
                .unwrap();
            assert_eq!(b2.copied, 3, "only the loose objects: {b2:?}");
            assert_eq!(b2.only_in_destination, 1, "{b2:?}");
            // The pool keeps what the source deleted: an older snapshot may
            // need it (issue #155).
            for h in &hashes {
                assert!(dst.get(h).await.unwrap().is_some());
            }
            // Again: nothing to do.
            let r = sync_packs(&sp, &dp, PackSyncMode::Pool).await.unwrap();
            assert_eq!(
                (r.copied, r.present, r.tombstones_added),
                (0, 2, 0),
                "{r:?}"
            );

            // The source merges its packs and the full repack drops the
            // deleted object. The pool gets the merged pack and drops the
            // small pack it covers; the big one still holds the deleted
            // object, so it stays.
            repack(&src, RepackMode::Full).await;
            let r = sync_packs(&sp, &dp, PackSyncMode::Pool).await.unwrap();
            assert_eq!((r.copied, r.pruned), (1, 1), "{r:?}");
            assert_eq!(dp.pack_ids().await.unwrap().len(), 2);
            let fresh = FsBlobStore::new(b.path());
            for h in &hashes {
                assert!(fresh.get(h).await.unwrap().is_some());
            }
        }

        #[tokio::test]
        async fn a_pool_pack_holding_something_no_other_pack_holds_is_kept() {
            let (a, b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
            let (src, hashes) = source(a.path()).await;
            let (sp, dp) = (FsPackFiles::new(a.path()), FsPackFiles::new(b.path()));
            sync_packs(&sp, &dp, PackSyncMode::Pool).await.unwrap();
            // GC deletes another packed object; the full repack drops it, so
            // only the pool's old pack still has it.
            src.delete(&hashes[5]).await.unwrap();
            repack(&src, RepackMode::Full).await;
            let r = sync_packs(&sp, &dp, PackSyncMode::Pool).await.unwrap();
            assert_eq!((r.copied, r.pruned), (1, 1), "{r:?}");
            let pool = FsBlobStore::new(b.path());
            assert!(
                pool.get(&hashes[5]).await.unwrap().is_some(),
                "the pool keeps it"
            );
        }

        /// A pool written before issue #155 was fixed carries tombstones. A
        /// tombstoned object never makes a pool pack prunable: its bytes may
        /// be what an older snapshot needs.
        #[tokio::test]
        async fn pool_tombstones_never_make_a_pack_prunable() {
            let (a, b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
            let (src, _) = source(a.path()).await;
            let (sp, dp) = (FsPackFiles::new(a.path()), FsPackFiles::new(b.path()));
            sync_packs(&sp, &dp, PackSyncMode::Pool).await.unwrap();
            // What an old backup did: copy the source's tombstones.
            let old: Vec<(String, Hash)> = sp
                .tombstones()
                .await
                .unwrap()
                .into_iter()
                .flat_map(|(id, hs)| hs.into_iter().map(move |h| (id.clone(), h)))
                .collect();
            assert_eq!(old.len(), 1);
            dp.add_tombstones(&old).await.unwrap();
            repack(&src, RepackMode::Full).await;
            let r = sync_packs(&sp, &dp, PackSyncMode::Pool).await.unwrap();
            assert_eq!((r.copied, r.pruned), (1, 1), "{r:?}");
            let big = &old[0].0;
            assert!(
                dp.pack_ids().await.unwrap().contains(big),
                "the pack holding the tombstoned object stays"
            );
        }

        #[tokio::test]
        async fn packs_copy_to_an_object_store_under_packs_keys() {
            let a = tempfile::tempdir().unwrap();
            let (src, hashes) = source(a.path()).await;
            let bucket: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
            let dp = ObjectPackFiles::new(bucket.clone());
            let sp = FsPackFiles::new(a.path());
            let r = sync_packs(&sp, &dp, PackSyncMode::Pool).await.unwrap();
            assert_eq!(r.copied, 2, "{r:?}");
            let ids = sp.pack_ids().await.unwrap();
            assert_eq!(dp.pack_ids().await.unwrap(), ids);
            for id in &ids {
                for ext in ["pack", "idx"] {
                    let key = object_store::path::Path::from(format!("packs/{id}.{ext}"));
                    let got = bucket.get(&key).await.unwrap().bytes().await.unwrap();
                    let want = std::fs::read(a.path().join(format!("packs/{id}.{ext}"))).unwrap();
                    assert_eq!(got, want);
                }
            }
            assert!(dp.tombstones().await.unwrap().is_empty());
            // The object store reads the packs, so only loose objects follow
            // one by one, and it lists what the source does plus the object
            // the source deleted.
            let dst = ObjectBlobStore::new(bucket);
            let r = sync_blobs(&src, &dst, &SyncOptions::default())
                .await
                .unwrap();
            assert_eq!(r.copied, 3, "{r:?}");
            let mut want = all(&src).await;
            want.push(hashes[3]);
            want.sort();
            assert_eq!(all(&dst).await, want);
            // And back: packs restore from the bucket into a fresh store.
            let c = tempfile::tempdir().unwrap();
            let r = sync_packs(&dp, &FsPackFiles::new(c.path()), PackSyncMode::Restore)
                .await
                .unwrap();
            assert_eq!((r.copied, r.tombstones_added), (2, 0), "{r:?}");
            assert_eq!(all(&FsBlobStore::new(c.path())).await.len(), 45);
        }

        /// A restore copies the tombstones its source holds, so the target
        /// reads as the source does.
        #[tokio::test]
        async fn a_restore_copies_the_source_tombstones() {
            let (a, c) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
            let (_, hashes) = source(a.path()).await;
            let (sp, cp) = (FsPackFiles::new(a.path()), FsPackFiles::new(c.path()));
            let r = sync_packs(&sp, &cp, PackSyncMode::Restore).await.unwrap();
            assert_eq!((r.copied, r.tombstones_added, r.pruned), (2, 1, 0), "{r:?}");
            assert_eq!(
                cp.tombstones().await.unwrap(),
                sp.tombstones().await.unwrap()
            );
            assert!(FsBlobStore::new(c.path())
                .get(&hashes[3])
                .await
                .unwrap()
                .is_none());
        }

        #[tokio::test]
        async fn a_damaged_source_pack_is_reported_and_not_published() {
            let (a, b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
            source(a.path()).await;
            let sp = FsPackFiles::new(a.path());
            let ids = sp.pack_ids().await.unwrap();
            let path = a.path().join(format!("packs/{}.pack", ids[0]));
            let mut bytes = std::fs::read(&path).unwrap();
            let last = bytes.len() - 1;
            bytes[last] ^= 1;
            std::fs::write(&path, bytes).unwrap();
            let dp = FsPackFiles::new(b.path());
            let r = sync_packs(&sp, &dp, PackSyncMode::Restore).await.unwrap();
            assert_eq!(r.corrupt, vec![ids[0].clone()], "{r:?}");
            assert_eq!(r.copied, 1);
            assert_eq!(dp.pack_ids().await.unwrap(), vec![ids[1].clone()]);
            assert!(dp.len(&format!("{}.pack", ids[0])).await.unwrap().is_none());
        }
    }
}
