//! Deriving the two index kinds. Both are pure functions of content-addressed
//! inputs (a blob; a tree node and the rows below it), so rows are written
//! idempotently and shared by every revision that contains the same bytes.

use std::collections::{btree_map, HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use futures_util::{stream, StreamExt};
use leep_core::{DerivedRow, Error, FileKind, Hash, Result, TreeEntry};

use super::trigram::{BlobRow, Bloom, Skip, MAX_INDEXED_BYTES};
use crate::indexing::bounded::SearchScan;
use crate::Repo;

/// Per-blob trigram set, keyed by blob hash.
pub const BLOB_KIND: &str = "blob-trigrams-v1";
/// Per-directory filter over every trigram below it, keyed by tree node hash.
pub const TREE_KIND: &str = "tree-trigrams-v1";

/// Bytes a cached filter costs beyond its own: the map entry (key, `Arc`,
/// size; about 48) plus the `Arc`'s counts and the `Bytes` handle (about 48).
pub(super) const CACHE_ENTRY_OVERHEAD: usize = 96;

/// Row lookups and blob fetches in flight at once.
pub(crate) const CONCURRENCY: usize = 16;

/// Most rows (each directory's filter and its files' rows) one batch of a
/// level derives before it writes them, as a staged derivation holds back
/// (ADR 0058 section 6).
pub(super) const BATCH_ROWS: usize = 4096;
/// Most bytes of rows a batch keeps from its reads, and holds for one write:
/// dense filters are 64 KiB, so 4,096 of them would be 256 MiB.
pub(super) const BATCH_BYTES: usize = 16 << 20;
/// Rows one write carries, as a staged derivation's writes do.
pub(super) const WRITE_ROWS: usize = 1024;
/// Keys one batched read asks for.
const READ_KEYS: usize = 1024;
/// Bytes of filters a derivation keeps for the parents still to derive;
/// past it, a parent reads its children's filters again.
const KNOWN_BYTES: usize = 32 << 20;

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
        let row = self.fresh_blob_row(hash, size, c).await?;
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

    /// The row of a blob that has none, computed. An oversize blob is
    /// recorded without being fetched; a bounded query counts the bytes it
    /// fetches against its budget and stops past it.
    async fn fresh_blob_row(&self, hash: Hash, size: u64, c: &Counters) -> Result<BlobRow> {
        if size > MAX_INDEXED_BYTES {
            return Ok(BlobRow::NotIndexed(Skip::TooLarge));
        }
        let data = self
            .blobs()
            .get(&hash)
            .await?
            .ok_or_else(|| Error::Storage(format!("blob {hash} missing while indexing")))?;
        if let Some(scan) = &c.scan {
            scan.fetched(data.len() as u64)?;
        }
        Ok(BlobRow::of(&data))
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
        })
    }

    /// The encoded filter of the tree node `root`. Missing rows below it are
    /// derived a level at a time ([`Repo::tree_row_unstored`]), and existing
    /// rows stop the descent: a revision that changed one file derives one
    /// row per directory on its path.
    pub(crate) async fn tree_row(&self, root: Hash, c: &Counters) -> Result<Bytes> {
        if let Some(v) = self.meta().get_derived(TREE_KIND, &root).await? {
            return Ok(v);
        }
        self.tree_row_unstored(root, c).await
    }

    /// The filter of `tree` for a search: from the shared cache, else from
    /// the store (and then cached), else computed by [`Repo::tree_row`]'s
    /// rules. A computed row is not cached: under a bounded query it lives
    /// only in that query's [`SearchScan`].
    pub(crate) async fn search_tree_row(&self, tree: Hash, c: &Counters) -> Result<Bytes> {
        match self.search_tree_rows(&[tree]).await?.pop().flatten() {
            Some(row) => Ok(row),
            None => self.tree_row_unstored(tree, c).await,
        }
    }

    /// The stored filters of `trees`, in order, reading from the store only
    /// those the shared cache does not hold, in one batch, and caching what
    /// it returns. `None` for a row not stored.
    pub(crate) async fn search_tree_rows(&self, trees: &[Hash]) -> Result<Vec<Option<Bytes>>> {
        let mut rows: Vec<Option<Bytes>> = trees
            .iter()
            .map(|h| self.search_trees.get(h).map(|b| Bytes::clone(&b)))
            .collect();
        let missing: Vec<Hash> = trees
            .iter()
            .zip(&rows)
            .filter(|(_, r)| r.is_none())
            .map(|(h, _)| *h)
            .collect();
        if missing.is_empty() {
            return Ok(rows);
        }
        let read = self.meta().get_derived_many(TREE_KIND, &missing).await?;
        if read.len() != missing.len() {
            return Err(Error::Storage(format!(
                "get_derived_many answered {} of {} {TREE_KIND} rows",
                read.len(),
                missing.len()
            )));
        }
        let mut read = read.into_iter();
        for (h, slot) in trees.iter().zip(&mut rows) {
            if slot.is_some() {
                continue;
            }
            *slot = read.next().expect("one row per missing key");
            if let Some(row) = slot {
                // A copy, so the entry holds exactly the bytes it counts
                // even if the store answered with a slice of a larger
                // buffer.
                let owned = Bytes::copy_from_slice(row);
                let size = owned.len() + CACHE_ENTRY_OVERHEAD;
                self.search_trees.insert_sized(*h, Arc::new(owned), size);
            }
        }
        Ok(rows)
    }

    /// [`Repo::tree_row`] for a row the store does not hold.
    ///
    /// The missing directories below `root` are found a level at a time, a
    /// level costing one batch of tree reads and one batched read of its
    /// subdirectories' filters. They are then derived by height, lowest
    /// first, in batches of at most [`BATCH_ROWS`] rows: a batch's missing
    /// blob rows are computed side by side and written, then its filters,
    /// and a height is written whole before the next starts. So a stored
    /// filter always means its whole subtree is stored, which `stale_target`
    /// and the bounded queries' checks rely on, and a gap of many lands
    /// costs a few round trips per level instead of a few per directory.
    /// Memory follows the missing directories' entries, plus one batch.
    async fn tree_row_unstored(&self, root: Hash, c: &Counters) -> Result<Bytes> {
        if let Some(v) = c.scan.as_ref().and_then(|s| s.get(true, &root)) {
            return Ok(v);
        }
        let mut known = Held::new(KNOWN_BYTES);
        let dirs = match self.missing_dirs(root, c, &mut known).await? {
            Missing::Stored(row) => return Ok(row),
            Missing::Dirs(dirs) => dirs,
        };
        let mut root_row = None;
        for level in by_height(&dirs) {
            let mut rest = level.as_slice();
            while !rest.is_empty() {
                let (mut n, mut rows) = (0, 0);
                while n < rest.len() {
                    let more = dirs[rest[n]].files.len() + 1;
                    if n > 0 && rows + more > BATCH_ROWS {
                        break;
                    }
                    rows += more;
                    n += 1;
                }
                let (batch, next) = rest.split_at(n);
                let batch: Vec<&Dir> = batch.iter().map(|&i| &dirs[i]).collect();
                if let Some(row) = self.derive_batch(root, &batch, c, &mut known).await? {
                    root_row = Some(row);
                }
                rest = next;
            }
        }
        root_row.ok_or_else(|| Error::Storage(format!("no {TREE_KIND} row derived for {root}")))
    }

    /// The directories at and below `root` without a filter, parents before
    /// their children, or `root`'s own filter if it is stored after all
    /// (another derivation finished it). Stored filters read on the way are
    /// kept in `known` for the parents.
    async fn missing_dirs(&self, root: Hash, c: &Counters, known: &mut Held) -> Result<Missing> {
        let mut out: Vec<Dir> = Vec::new();
        let mut seen: HashSet<Hash> = HashSet::from([root]);
        let mut level = vec![root];
        let mut lookup = vec![root];
        while !level.is_empty() {
            let mut nodes = Vec::with_capacity(level.len());
            for chunk in level.chunks(READ_KEYS) {
                nodes.extend(self.read_trees(chunk).await?);
            }
            for (&hash, node) in level.iter().zip(nodes) {
                let mut dir = Dir {
                    hash,
                    dirs: Vec::new(),
                    files: Vec::new(),
                };
                for entry in node.entries.into_values() {
                    match entry {
                        TreeEntry::Dir { tree } => {
                            dir.dirs.push(tree);
                            if seen.insert(tree) {
                                lookup.push(tree);
                            }
                        }
                        TreeEntry::File {
                            content,
                            kind,
                            size,
                        } if kind != FileKind::Symlink => dir.files.push((content, size)),
                        TreeEntry::File { .. } => {}
                    }
                }
                out.push(dir);
            }
            let rows = self.rows_of(true, &lookup, c).await?;
            level = Vec::new();
            for (h, row) in lookup.drain(..).zip(rows) {
                match row {
                    Some(row) if h == root => return Ok(Missing::Stored(row)),
                    Some(row) => known.insert(h, row),
                    None if h == root => {}
                    None => level.push(h),
                }
            }
        }
        Ok(Missing::Dirs(out))
    }

    /// Derive one batch of directories of one height: their missing blob
    /// rows, all written first, then their filters. Returns `root`'s filter
    /// if it is in the batch.
    async fn derive_batch(
        &self,
        root: Hash,
        batch: &[&Dir],
        c: &Counters,
        known: &mut Held,
    ) -> Result<Option<Bytes>> {
        let mut distinct = HashSet::new();
        let files: Vec<(Hash, u64)> = batch
            .iter()
            .flat_map(|d| d.files.iter().copied())
            .filter(|f| distinct.insert(f.0))
            .collect();
        let mut blobs = Held::new(BATCH_BYTES);
        let mut writes = Writes::new(self, c, BLOB_KIND);
        for chunk in files.chunks(READ_KEYS) {
            let keys: Vec<Hash> = chunk.iter().map(|f| f.0).collect();
            let mut missing = Vec::new();
            for (f, row) in chunk.iter().zip(self.rows_of(false, &keys, c).await?) {
                match row {
                    Some(row) => blobs.insert(f.0, row),
                    None => missing.push(*f),
                }
            }
            let mut computed = stream::iter(missing)
                .map(|(h, size)| async move {
                    let row = self.fresh_blob_row(h, size, c).await?;
                    Ok::<_, Error>((h, Bytes::from(row.encode())))
                })
                .buffered(CONCURRENCY);
            while let Some(r) = computed.next().await {
                let (h, row) = r?;
                blobs.insert(h, row.clone());
                writes.push(h, row).await?;
            }
        }
        writes.flush().await?;

        let mut writes = Writes::new(self, c, TREE_KIND);
        let mut root_row = None;
        for dir in batch {
            let mut bloom = Bloom::default();
            let mut absent = Vec::new();
            for t in &dir.dirs {
                match known.get(t) {
                    Some(row) => bloom.or_encoded(row)?,
                    None => absent.push(*t),
                }
            }
            for row in self.rows_present(true, &absent, c).await? {
                bloom.or_encoded(&row)?;
            }
            let mut absent = Vec::new();
            for (h, _) in &dir.files {
                match blobs.get(h) {
                    Some(row) => add_blob_row(&mut bloom, row)?,
                    None => absent.push(*h),
                }
            }
            for row in self.rows_present(false, &absent, c).await? {
                add_blob_row(&mut bloom, &row)?;
            }
            let row = Bytes::from(bloom.encode());
            known.insert(dir.hash, row.clone());
            if dir.hash == root {
                root_row = Some(row.clone());
            }
            writes.push(dir.hash, row).await?;
        }
        writes.flush().await?;
        Ok(root_row)
    }

    /// Rows of `keys`, filters if `tree` else blob rows, in order: from a
    /// bounded query's memory, else from the store, a batch per round trip.
    async fn rows_of(&self, tree: bool, keys: &[Hash], c: &Counters) -> Result<Vec<Option<Bytes>>> {
        let kind = if tree { TREE_KIND } else { BLOB_KIND };
        let mut out: Vec<Option<Bytes>> = keys
            .iter()
            .map(|h| c.scan.as_ref().and_then(|s| s.get(tree, h)))
            .collect();
        let ask: Vec<usize> = (0..keys.len()).filter(|&i| out[i].is_none()).collect();
        for chunk in ask.chunks(READ_KEYS) {
            let batch: Vec<Hash> = chunk.iter().map(|&i| keys[i]).collect();
            let rows = self.meta().get_derived_many(kind, &batch).await?;
            if rows.len() != batch.len() {
                return Err(Error::Storage(format!(
                    "get_derived_many answered {} of {} {kind} rows",
                    rows.len(),
                    batch.len()
                )));
            }
            for (&i, row) in chunk.iter().zip(rows) {
                out[i] = row;
            }
        }
        Ok(out)
    }

    /// [`Repo::rows_of`] for rows this derivation stored or found stored,
    /// read again because they did not fit in memory.
    async fn rows_present(&self, tree: bool, keys: &[Hash], c: &Counters) -> Result<Vec<Bytes>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let rows = self.rows_of(tree, keys, c).await?;
        keys.iter()
            .zip(rows)
            .map(|(h, row)| {
                row.ok_or_else(|| {
                    let kind = if tree { TREE_KIND } else { BLOB_KIND };
                    Error::Storage(format!("{kind} row of {h} vanished while deriving"))
                })
            })
            .collect()
    }
}

/// A directory without a filter, as [`Repo::missing_dirs`] found it.
struct Dir {
    hash: Hash,
    /// Every subdirectory, with a filter or not.
    dirs: Vec<Hash>,
    /// Searchable files: content and size.
    files: Vec<(Hash, u64)>,
}

enum Missing {
    Stored(Bytes),
    Dirs(Vec<Dir>),
}

/// Indices of `dirs` by height, lowest first: 0 for a directory without a
/// missing subdirectory, else one more than its highest missing one. A
/// missing subdirectory is always lower than its parent, wherever else in
/// the tree the same subtree appears.
fn by_height(dirs: &[Dir]) -> Vec<Vec<usize>> {
    let index: HashMap<Hash, usize> = dirs.iter().enumerate().map(|(i, d)| (d.hash, i)).collect();
    let kids: Vec<Vec<usize>> = dirs
        .iter()
        .map(|d| {
            d.dirs
                .iter()
                .filter_map(|t| index.get(t).copied())
                .collect()
        })
        .collect();
    let mut height: Vec<Option<usize>> = vec![None; dirs.len()];
    for start in 0..dirs.len() {
        if height[start].is_some() {
            continue;
        }
        let mut stack = vec![(start, 0usize)];
        while let Some((i, next)) = stack.last_mut() {
            let i = *i;
            match kids[i].get(*next) {
                Some(&k) => {
                    *next += 1;
                    if height[k].is_none() {
                        stack.push((k, 0));
                    }
                }
                None => {
                    let below = kids[i].iter().map(|&k| height[k].expect("derived lower"));
                    height[i] = Some(below.map(|h| h + 1).max().unwrap_or(0));
                    stack.pop();
                }
            }
        }
    }
    let mut levels: Vec<Vec<usize>> = Vec::new();
    for (i, h) in height.into_iter().enumerate() {
        let h = h.expect("every directory has a height");
        if levels.len() <= h {
            levels.resize_with(h + 1, Vec::new);
        }
        levels[h].push(i);
    }
    levels
}

fn add_blob_row(bloom: &mut Bloom, row: &[u8]) -> Result<()> {
    if let BlobRow::Trigrams(set) = BlobRow::decode(row)? {
        for t in set {
            bloom.insert(t);
        }
    }
    Ok(())
}

/// Rows kept in memory up to a byte budget; one that does not fit is read
/// again when it is needed.
struct Held {
    rows: HashMap<Hash, Bytes>,
    bytes: usize,
    budget: usize,
}

impl Held {
    fn new(budget: usize) -> Self {
        Held {
            rows: HashMap::new(),
            bytes: 0,
            budget,
        }
    }

    fn get(&self, h: &Hash) -> Option<&Bytes> {
        self.rows.get(h)
    }

    fn insert(&mut self, h: Hash, row: Bytes) {
        if self.bytes + row.len() <= self.budget && !self.rows.contains_key(&h) {
            self.bytes += row.len();
            self.rows.insert(h, row);
        }
    }
}

/// Rows of one kind a batch computed and has not written: put in a bounded
/// query's memory at once, else written [`WRITE_ROWS`] or [`BATCH_BYTES`] at
/// a time, each write after the index lease check.
struct Writes<'a> {
    repo: &'a Repo,
    c: &'a Counters,
    kind: &'static str,
    rows: Vec<DerivedRow>,
    bytes: usize,
}

impl<'a> Writes<'a> {
    fn new(repo: &'a Repo, c: &'a Counters, kind: &'static str) -> Self {
        Writes {
            repo,
            c,
            kind,
            rows: Vec::new(),
            bytes: 0,
        }
    }

    async fn push(&mut self, key: Hash, row: Bytes) -> Result<()> {
        if let Some(scan) = &self.c.scan {
            scan.put(self.kind == TREE_KIND, key, row);
            return Ok(());
        }
        self.bytes += row.len();
        self.rows.push(DerivedRow::new(self.kind, key, row));
        if self.rows.len() >= WRITE_ROWS || self.bytes >= BATCH_BYTES {
            self.flush().await?;
        }
        Ok(())
    }

    async fn flush(&mut self) -> Result<()> {
        if self.rows.is_empty() {
            return Ok(());
        }
        // The warmer's index lease (ADR 0058), asked before each write: a
        // holder that lost it stops before writing a filter whose blob rows
        // a drop may remove.
        if !self.repo.write_allowed().await {
            return Err(Error::Other(
                "stopped deriving: the index lease was lost".into(),
            ));
        }
        self.repo.meta().put_derived_many(&self.rows).await?;
        let n = self.rows.len() as u64;
        let counter = if self.kind == TREE_KIND {
            &self.c.tree_rows
        } else {
            &self.c.blob_rows
        };
        counter.fetch_add(n, Ordering::Relaxed);
        metrics::counter!("leep_derive_total", "kind" => self.kind).increment(n);
        self.rows.clear();
        self.bytes = 0;
        Ok(())
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
