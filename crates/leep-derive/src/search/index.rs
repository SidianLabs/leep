//! Deriving the two index kinds. Both are pure functions of content-addressed
//! inputs (a blob; a tree node and the rows below it), so rows are written
//! idempotently and shared by every revision that contains the same bytes.

use std::collections::btree_map;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use futures_util::{stream, StreamExt};
use leep_core::{Error, FileKind, Hash, Result, TreeEntry};

use super::trigram::{BlobRow, Bloom, Skip, MAX_INDEXED_BYTES};
use crate::indexing::bounded::SearchScan;
use crate::Repo;

/// Per-blob trigram set, keyed by blob hash.
pub const BLOB_KIND: &str = "blob-trigrams-v1";
/// Per-directory filter over every trigram below it, keyed by tree node hash.
pub const TREE_KIND: &str = "tree-trigrams-v1";

/// Row lookups and blob fetches in flight at once.
pub(super) const CONCURRENCY: usize = 16;

/// Rows computed (not read) during one operation.
#[derive(Default)]
pub(crate) struct Counters {
    pub blob_rows: AtomicU64,
    pub tree_rows: AtomicU64,
    /// A bounded query (ADR 0058): rows are computed into this, never
    /// stored, within its budget.
    pub(crate) scan: Option<Arc<SearchScan>>,
}

impl Counters {
    /// Counters for a bounded query that computes missing rows into `scan`.
    pub(crate) fn scanning(scan: Arc<SearchScan>) -> Self {
        Counters {
            scan: Some(scan),
            ..Counters::default()
        }
    }

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
    /// Stored filters of the subdirectories, read in one batch.
    rows: std::collections::HashMap<Hash, Bytes>,
}

impl Repo {
    /// The trigram row of a blob of `size` bytes, computed if missing. An
    /// oversize blob is recorded without being fetched.
    async fn blob_row_raw(&self, hash: Hash, size: u64, c: &Counters) -> Result<RawRow> {
        if let Some(v) = self.meta().get_derived(BLOB_KIND, &hash).await? {
            return Ok(RawRow::Stored(v));
        }
        if let Some(v) = c.scan.as_ref().and_then(|s| s.get(false, &hash)) {
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
            if let Some(scan) = &c.scan {
                scan.fetched(data.len() as u64)?;
            }
            BlobRow::of(&data)
        };
        if let Some(scan) = &c.scan {
            let bytes = Bytes::from(row.encode());
            scan.put(false, hash, bytes.clone());
            return Ok(RawRow::Stored(bytes));
        }
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
    pub(crate) async fn blob_row_bytes(
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
            rows: Default::default(),
        })
    }

    /// [`Repo::frame`] with its subdirectories' stored filters read in one
    /// batch: a directory of hundreds of subdirectories (one per agent)
    /// otherwise costs a round trip each.
    async fn frame_with_rows(&self, hash: Hash) -> Result<Frame> {
        let node = self.read_tree(hash).await?;
        let dirs: Vec<Hash> = node
            .entries
            .values()
            .filter_map(|e| match e {
                TreeEntry::Dir { tree } => Some(*tree),
                TreeEntry::File { .. } => None,
            })
            .collect();
        let mut rows = std::collections::HashMap::new();
        if !dirs.is_empty() {
            let found = self.meta().get_derived_many(TREE_KIND, &dirs).await?;
            for (h, row) in dirs.into_iter().zip(found) {
                if let Some(row) = row {
                    rows.insert(h, row);
                }
            }
        }
        Ok(Frame {
            hash,
            entries: node.entries.into_iter(),
            files: Vec::new(),
            bloom: Bloom::default(),
            rows,
        })
    }

    /// Blob rows of `files` in one batched read, computing the missing ones.
    async fn blob_rows_of(&self, files: Vec<(Hash, u64)>, c: &Counters) -> Result<Vec<BlobRow>> {
        let keys: Vec<Hash> = files.iter().map(|f| f.0).collect();
        let stored = if keys.is_empty() {
            Vec::new()
        } else {
            self.meta().get_derived_many(BLOB_KIND, &keys).await?
        };
        let stored = if stored.len() == keys.len() {
            stored
        } else {
            vec![None; keys.len()]
        };
        let mut rows = stream::iter(files.into_iter().zip(stored))
            .map(|((h, size), row)| async move {
                match row {
                    Some(b) => BlobRow::decode(&b),
                    None => self.blob_row(h, size, c).await,
                }
            })
            .buffered(CONCURRENCY);
        let mut out = Vec::new();
        while let Some(row) = rows.next().await {
            out.push(row?);
        }
        Ok(out)
    }

    /// The encoded filter of the tree node `root`. Missing rows below it are
    /// derived children first with an explicit stack, so a deep tree cannot
    /// overflow the call stack, and existing rows stop the descent: a
    /// revision that changed one file derives one row per directory on its
    /// path.
    pub(crate) async fn tree_row(&self, root: Hash, c: &Counters) -> Result<Bytes> {
        let meta = self.meta();
        if let Some(v) = meta.get_derived(TREE_KIND, &root).await? {
            return Ok(v);
        }
        let memo = |h: &Hash| c.scan.as_ref().and_then(|s| s.get(true, h));
        if let Some(v) = memo(&root) {
            return Ok(v);
        }
        let mut stack = vec![self.frame_with_rows(root).await?];
        loop {
            let top = stack.last_mut().expect("stack is non-empty");
            let mut missing = None;
            for (_, entry) in top.entries.by_ref() {
                match entry {
                    TreeEntry::Dir { tree } => {
                        match top.rows.get(&tree).cloned().or_else(|| memo(&tree)) {
                            Some(row) => top.bloom.or_encoded(&row)?,
                            None => {
                                missing = Some(tree);
                                break;
                            }
                        }
                    }
                    TreeEntry::File {
                        content,
                        kind,
                        size,
                    } if kind != FileKind::Symlink => top.files.push((content, size)),
                    TreeEntry::File { .. } => {}
                }
            }
            if let Some(tree) = missing {
                stack.push(self.frame_with_rows(tree).await?);
                continue;
            }
            let mut done = stack.pop().expect("stack is non-empty");
            let files = std::mem::take(&mut done.files);
            for row in self.blob_rows_of(files, c).await? {
                if let BlobRow::Trigrams(set) = row {
                    for t in set {
                        done.bloom.insert(t);
                    }
                }
            }
            let row = Bytes::from(done.bloom.encode());
            match &c.scan {
                Some(scan) => scan.put(true, done.hash, row.clone()),
                None => {
                    meta.put_derived(TREE_KIND, &done.hash, row.clone()).await?;
                    c.tree_rows.fetch_add(1, Ordering::Relaxed);
                    metrics::counter!("leep_derive_total", "kind" => TREE_KIND).increment(1);
                }
            }
            match stack.last_mut() {
                Some(parent) => parent.bloom.or_encoded(&row)?,
                None => return Ok(row),
            }
        }
    }
}

/// Rows `recompute_tree_row` has computed, so a scan over many directories
/// computes each one once. Cleared when it outgrows its budget; a row is then
/// computed again, which costs time and not correctness.
pub(crate) struct TreeRowMemo {
    rows: std::collections::HashMap<Hash, Bytes>,
    bytes: usize,
}

/// Bytes of memoized filters to hold: a few hundred dense 64 KiB filters.
const MEMO_BUDGET: usize = 32 << 20;

impl TreeRowMemo {
    pub fn new() -> Self {
        TreeRowMemo {
            rows: Default::default(),
            bytes: 0,
        }
    }

    fn insert(&mut self, tree: Hash, row: Bytes) {
        if self.bytes + row.len() > MEMO_BUDGET {
            self.rows.clear();
            self.bytes = 0;
        }
        self.bytes += row.len();
        self.rows.insert(tree, row);
    }
}

impl Repo {
    /// The row `tree_row` stores for `root`, recomputed from the tree itself:
    /// the filter over every searchable blob below it, taken from the blobs'
    /// own rows and the filters recomputed for the directories below, never
    /// from a stored directory filter, so damage in one cannot make another
    /// look wrong. For verification and repair; like derivation it writes only
    /// the blob rows it finds missing.
    pub(crate) async fn recompute_tree_row(
        &self,
        root: Hash,
        memo: &mut TreeRowMemo,
    ) -> Result<Bytes> {
        if let Some(row) = memo.rows.get(&root) {
            return Ok(row.clone());
        }
        let c = Counters::default();
        let mut stack = vec![self.frame(root).await?];
        loop {
            let top = stack.last_mut().expect("stack is non-empty");
            let mut missing = None;
            for (_, entry) in top.entries.by_ref() {
                match entry {
                    TreeEntry::Dir { tree } => match memo.rows.get(&tree) {
                        Some(row) => top.bloom.or_encoded(row)?,
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
                .map(|(h, size)| self.blob_row(h, size, &c))
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
            memo.insert(done.hash, row.clone());
            match stack.last_mut() {
                Some(parent) => parent.bloom.or_encoded(&row)?,
                None => return Ok(row),
            }
        }
    }
}
