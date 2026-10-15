//! Deriving the two index kinds. Both are pure functions of content-addressed
//! inputs (a blob; a tree node and the rows below it), so rows are written
//! idempotently and shared by every revision that contains the same bytes.

use std::collections::btree_map;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use futures_util::{stream, StreamExt};
use leep_core::{Error, FileKind, Hash, Result, TreeEntry};

use super::trigram::{BlobRow, Bloom, Skip, MAX_INDEXED_BYTES};
use crate::Repo;

/// Per-blob trigram set, keyed by blob hash.
pub const BLOB_KIND: &str = "blob-trigrams-v1";
/// Per-directory filter over every trigram below it, keyed by tree node hash.
pub const TREE_KIND: &str = "tree-trigrams-v1";

/// Row lookups and blob fetches in flight at once.
pub(super) const CONCURRENCY: usize = 16;

/// Rows computed (not read) during one operation.
#[derive(Default)]
pub(super) struct Counters {
    pub blob_rows: AtomicU64,
    pub tree_rows: AtomicU64,
}

impl Counters {
    pub fn blob_rows(&self) -> u64 {
        self.blob_rows.load(Ordering::Relaxed)
    }

    pub fn tree_rows(&self) -> u64 {
        self.tree_rows.load(Ordering::Relaxed)
    }
}

enum RawRow {
    Stored(Bytes),
    Fresh(BlobRow),
}

struct Frame {
    hash: Hash,
    entries: btree_map::IntoIter<String, TreeEntry>,
    files: Vec<(Hash, u64)>,
    bloom: Bloom,
}

impl Repo {
    /// The trigram row of a blob of `size` bytes, computed if missing. An
    /// oversize blob is recorded without being fetched.
    async fn blob_row_raw(&self, hash: Hash, size: u64, c: &Counters) -> Result<RawRow> {
        if let Some(v) = self.meta().get_derived(BLOB_KIND, &hash).await? {
            return Ok(RawRow::Stored(v));
        }
        let row = if size > MAX_INDEXED_BYTES {
            BlobRow::NotIndexed(Skip::TooLarge)
        } else {
            let data = self
                .blobs()
                .get(&hash)
                .await?
                .ok_or_else(|| Error::Storage(format!("blob {hash} missing while indexing")))?;
            BlobRow::of(&data)
        };
        self.meta()
            .put_derived(BLOB_KIND, &hash, Bytes::from(row.encode()))
            .await?;
        c.blob_rows.fetch_add(1, Ordering::Relaxed);
        metrics::counter!("leep_derive_total", "kind" => BLOB_KIND).increment(1);
        Ok(RawRow::Fresh(row))
    }

    pub(super) async fn blob_row(&self, hash: Hash, size: u64, c: &Counters) -> Result<BlobRow> {
        match self.blob_row_raw(hash, size, c).await? {
            RawRow::Stored(b) => BlobRow::decode(&b),
            RawRow::Fresh(r) => Ok(r),
        }
    }

    /// The encoded row, for probing without decoding it.
    pub(super) async fn blob_row_bytes(
        &self,
        hash: Hash,
        size: u64,
        c: &Counters,
    ) -> Result<Bytes> {
        match self.blob_row_raw(hash, size, c).await? {
            RawRow::Stored(b) => Ok(b),
            RawRow::Fresh(r) => Ok(Bytes::from(r.encode())),
        }
    }

    async fn frame(&self, hash: Hash) -> Result<Frame> {
        Ok(Frame {
            hash,
            entries: self.read_tree(hash).await?.entries.into_iter(),
            files: Vec::new(),
            bloom: Bloom::default(),
        })
    }

    /// The encoded filter of the tree node `root`. Missing rows below it are
    /// derived children first with an explicit stack, so a deep tree cannot
    /// overflow the call stack, and existing rows stop the descent: a
    /// revision that changed one file derives one row per directory on its
    /// path.
    pub(super) async fn tree_row(&self, root: Hash, c: &Counters) -> Result<Bytes> {
        let meta = self.meta();
        if let Some(v) = meta.get_derived(TREE_KIND, &root).await? {
            return Ok(v);
        }
        let mut stack = vec![self.frame(root).await?];
        loop {
            let top = stack.last_mut().expect("stack is non-empty");
            let mut missing = None;
            for (_, entry) in top.entries.by_ref() {
                match entry {
                    TreeEntry::Dir { tree } => match meta.get_derived(TREE_KIND, &tree).await? {
                        Some(row) => top.bloom.or_encoded(&row)?,
                        None => {
                            missing = Some(tree);
                            break;
                        }
                    },
                    TreeEntry::File {
                        content,
                        kind,
                        size,
                    } if kind != FileKind::Symlink => top.files.push((content, size)),
                    TreeEntry::File { .. } => {}
                }
            }
            if let Some(tree) = missing {
                stack.push(self.frame(tree).await?);
                continue;
            }
            let mut done = stack.pop().expect("stack is non-empty");
            let files = std::mem::take(&mut done.files);
            let mut rows = stream::iter(files)
                .map(|(h, size)| self.blob_row(h, size, c))
                .buffered(CONCURRENCY);
            while let Some(row) = rows.next().await {
                if let BlobRow::Trigrams(set) = row? {
                    for t in set {
                        done.bloom.insert(t);
                    }
                }
            }
            drop(rows);
            let row = Bytes::from(done.bloom.encode());
            meta.put_derived(TREE_KIND, &done.hash, row.clone()).await?;
            c.tree_rows.fetch_add(1, Ordering::Relaxed);
            metrics::counter!("leep_derive_total", "kind" => TREE_KIND).increment(1);
            match stack.last_mut() {
                Some(parent) => parent.bloom.or_encoded(&row)?,
                None => return Ok(row),
            }
        }
    }
}
