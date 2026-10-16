//! Incremental blob copy between two stores, for backup and restore
//! (ADR 0026).
//!
//! Blobs are immutable and named by their content, so "copy what the other
//! side lacks" is always safe: a blob present under a hash holds the bytes of
//! that hash, whenever and by whomever it was written. The two ascending
//! listings are merged, so an unchanged store costs two listings and no reads.

use std::collections::VecDeque;
use std::sync::Arc;

use futures::stream::{self, StreamExt};
use leep_core::{BlobMeta, BlobStore, Error, Hash, Result, TreeNode};

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
    Ok(report)
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
}
