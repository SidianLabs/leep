//! Deriving the index kinds. A blob's row and a keyframe are pure functions
//! of content-addressed inputs (a blob; a tree node and the rows below it),
//! so they are written idempotently and shared by every revision that
//! contains the same bytes. A directory's row may instead be a patch on the
//! keyframe its predecessor's row is on (ADR 0094): the same filter, written
//! in fewer bytes, and also shared by content.

use std::collections::{btree_map, HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use futures_util::{stream, StreamExt};
use leep_core::{ChangesetId, DerivedRow, Error, FileKind, Hash, Result, TreeEntry, TreeNode};

use super::trigram::{row_of, BlobRow, Bloom, Skip, TreeRow, Union, MAX_INDEXED_BYTES};
use crate::indexing::bounded::SearchScan;
use crate::Repo;

/// Per-blob trigram set, keyed by blob hash.
pub const BLOB_KIND: &str = "blob-trigrams-v1";
/// Per-directory filter over every trigram below it, keyed by tree node hash:
/// whole, saturated, or a patch on a keyframe (ADR 0094).
pub const TREE_KIND: &str = "tree-trigrams-v2";
/// The keyframes patches are written on, keyed by the tree node whose whole
/// filter they hold. A keyframe is written only with that filter's one
/// encoding, so a row never changes once written, which is what a patch
/// written on it relies on; a `tree-trigrams-v2` row itself may be
/// rewritten by a concurrent derivation from another predecessor.
pub const KEYFRAME_KIND: &str = "tree-trigrams-keyframe-v2";
/// Directory filters before ADR 0094, always whole.
pub const RETIRED_TREE_KIND: &str = "tree-trigrams-v1";

/// First-parent steps a derivation looks back for a predecessor whose tree
/// has a row, when the first parent's has none: the warmer skips heads
/// when lands outrun it.
const PRED_STEPS: usize = 8;

/// Where keyframes sit in the search filter cache, apart from the row of
/// the same tree.
pub(crate) fn keyframe_cache_key(tree: &Hash) -> Hash {
    Hash::of_object(KEYFRAME_KIND, tree.as_bytes())
}

/// A directory's stored filter row and, for a patch, its keyframe row.
#[derive(Clone, Debug)]
pub(crate) struct StoredFilter {
    pub row: Bytes,
    pub keyframe: Option<Bytes>,
}

impl StoredFilter {
    fn whole(row: Bytes) -> Self {
        StoredFilter {
            row,
            keyframe: None,
        }
    }
}

/// The tree a derivation diffs a directory's filter against: the same path
/// in the first parent.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Pred {
    pub tree: Hash,
    /// The first parent, whose ancestors stand in when its tree has no
    /// row; `None` to not look for them.
    pub parent: Option<ChangesetId>,
}

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
    union: Union,
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
            .read_content(&hash)
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
            union: Union::default(),
        })
    }

    /// The filter row of the tree node `root` as stored, or as derived
    /// (whole) if it was missing. Missing rows below it are derived a level
    /// at a time ([`Repo::tree_row_unstored`]), and existing rows stop the
    /// descent: a revision that changed one file derives one row per
    /// directory on its path. Without a predecessor every row derived is
    /// whole.
    pub(crate) async fn tree_row(&self, root: Hash, c: &Counters) -> Result<Bytes> {
        self.tree_row_after(root, None, c).await
    }

    /// [`Repo::tree_row`] for a tree whose predecessor is `pred`: each
    /// directory derived is patched on the keyframe of the row of the same
    /// path in `pred`'s tree, where the keyframe rule allows.
    pub(crate) async fn tree_row_after(
        &self,
        root: Hash,
        pred: Option<Pred>,
        c: &Counters,
    ) -> Result<Bytes> {
        // The predecessor's row is read with the root's: without one, a
        // nearer indexed ancestor stands in.
        let keys = match pred {
            Some(p) if p.tree != root => vec![root, p.tree],
            _ => vec![root],
        };
        let rows = self.meta().get_derived_many(TREE_KIND, &keys).await?;
        if let Some(Some(v)) = rows.first() {
            return Ok(v.clone());
        }
        let pred = match (pred, rows.get(1)) {
            (Some(p), Some(None)) => self.indexed_ancestor(root, p).await?,
            _ => pred,
        };
        self.tree_row_unstored(root, pred, c, &mut PoolSlot::none())
            .await
    }

    /// When the first parent's tree has no filter (the warmer skipped it
    /// because lands outran it), the tree of the nearest of the next
    /// [`PRED_STEPS`] first-parent ancestors that has one, if any.
    async fn indexed_ancestor(&self, root: Hash, pred: Pred) -> Result<Option<Pred>> {
        let Some(mut at) = pred.parent else {
            return Ok(None);
        };
        let mut ids = Vec::with_capacity(PRED_STEPS);
        for _ in 0..PRED_STEPS {
            let Some(&p) = self.parents(at).await?.first() else {
                break;
            };
            ids.push(p);
            at = p;
        }
        if ids.is_empty() {
            return Ok(None);
        }
        let trees = self.root_trees(&ids).await?;
        let rows = self.meta().get_derived_many(TREE_KIND, &trees).await?;
        Ok(trees
            .into_iter()
            .zip(rows)
            .find(|(_, r)| r.is_some())
            .filter(|(t, _)| *t != root)
            .map(|(tree, _)| Pred { tree, parent: None }))
    }

    /// The predecessor of the tree of `id` for deriving its filters: its
    /// first parent's tree. `None` for a root changeset.
    pub(crate) async fn search_pred(&self, id: ChangesetId) -> Result<Option<Pred>> {
        let Some(&parent) = self.parents(id).await?.first() else {
            return Ok(None);
        };
        Ok(Some(Pred {
            tree: self.root_tree(parent).await?,
            parent: Some(parent),
        }))
    }

    /// [`Repo::search_pred`] if this process holds the first parent and its
    /// tree, else `None`: for a search, which must not pay reads for it.
    pub(crate) fn search_pred_cached(&self, id: ChangesetId) -> Option<Pred> {
        let parent = *self.cached_parents(id)?.first()?;
        Some(Pred {
            tree: self.cached_root(parent)?,
            parent: None,
        })
    }

    /// The filter of `tree` for a search that found it missing from the
    /// cache and the store ([`Repo::search_tree_rows`]): computed by
    /// [`Repo::tree_row`]'s rules after `pred`, whole. It is not cached:
    /// under a bounded query it lives only in that query's [`SearchScan`].
    /// `pool` is the search's pool permit, given back while it waits for
    /// another derivation of `tree`.
    pub(crate) async fn search_tree_row_missing(
        &self,
        tree: Hash,
        pred: Option<Pred>,
        c: &Counters,
        pool: &mut PoolSlot<'_>,
    ) -> Result<StoredFilter> {
        self.tree_row_unstored(tree, pred, c, pool)
            .await
            .map(StoredFilter::whole)
    }

    /// The stored filters of `trees`, in order, with the keyframes their
    /// patches need, reading from the store only what the shared cache does
    /// not hold (a batch for the rows, and one for their keyframes if any is
    /// missing) and caching what it returns. `None` for a row not stored.
    pub(crate) async fn search_tree_rows(
        &self,
        trees: &[Hash],
    ) -> Result<Vec<Option<StoredFilter>>> {
        let rows = self.cached_rows(TREE_KIND, trees, |h| *h).await?;
        let mut bases: Vec<Hash> = Vec::new();
        for row in rows.iter().flatten() {
            if let Ok(TreeRow::Patch(p)) = TreeRow::parse(row) {
                if !bases.contains(&p.base) {
                    bases.push(p.base);
                }
            }
        }
        let keyframes = self.keyframe_rows(&bases).await?;
        rows.into_iter()
            .zip(trees)
            .map(|(row, tree)| {
                let Some(row) = row else {
                    return Ok(None);
                };
                let keyframe = match TreeRow::parse(&row) {
                    Ok(TreeRow::Patch(p)) => {
                        Some(keyframes.get(&p.base).cloned().ok_or_else(|| {
                            Error::Storage(format!(
                                "{KEYFRAME_KIND} row of {} missing for the patch of {tree}",
                                p.base
                            ))
                        })?)
                    }
                    _ => None,
                };
                Ok(Some(StoredFilter { row, keyframe }))
            })
            .collect()
    }

    /// The keyframe rows of `trees` that are stored, through the shared
    /// cache: most patches of a busy directory share one keyframe.
    pub(crate) async fn keyframe_rows(&self, trees: &[Hash]) -> Result<HashMap<Hash, Bytes>> {
        let rows = self
            .cached_rows(KEYFRAME_KIND, trees, keyframe_cache_key)
            .await?;
        Ok(trees
            .iter()
            .zip(rows)
            .filter_map(|(h, r)| Some((*h, r?)))
            .collect())
    }

    /// Rows of `kind` for `keys`, in order, from the shared filter cache
    /// (under `cache_key`) or, for those it lacks, from one batched read,
    /// whose rows are then cached.
    async fn cached_rows(
        &self,
        kind: &str,
        keys: &[Hash],
        cache_key: impl Fn(&Hash) -> Hash,
    ) -> Result<Vec<Option<Bytes>>> {
        let mut rows: Vec<Option<Bytes>> = keys
            .iter()
            .map(|h| {
                self.search_trees
                    .get(&cache_key(h))
                    .map(|b| Bytes::clone(&b))
            })
            .collect();
        let missing: Vec<Hash> = keys
            .iter()
            .zip(&rows)
            .filter(|(_, r)| r.is_none())
            .map(|(h, _)| *h)
            .collect();
        if missing.is_empty() {
            return Ok(rows);
        }
        let read = self.meta().get_derived_many(kind, &missing).await?;
        if read.len() != missing.len() {
            return Err(Error::Storage(format!(
                "get_derived_many answered {} of {} {kind} rows",
                read.len(),
                missing.len()
            )));
        }
        let mut read = read.into_iter();
        for (h, slot) in keys.iter().zip(&mut rows) {
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
                self.search_trees
                    .insert_sized(cache_key(h), Arc::new(owned), size);
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
    ///
    /// With `pred`, each missing directory's predecessor (the directory at
    /// the same path in `pred`'s tree) is read alongside it, its node with
    /// the level's nodes and its row with the level's rows, and the filter
    /// is written as a patch on the keyframe that row is on, or starts a new
    /// keyframe (ADR 0094). The row returned is always whole.
    ///
    /// One derivation of a root runs at a time in this process: a search
    /// arriving while the warmer (or another search) derives the same root
    /// waits for it, without its pool permit, then finds the row stored. If
    /// that derivation stopped (cancelled, or failed), the next in line
    /// derives instead; it never sees the other's error. A bounded scan
    /// stores nothing, so it neither waits nor makes anyone wait.
    async fn tree_row_unstored(
        &self,
        root: Hash,
        pred: Option<Pred>,
        c: &Counters,
        pool: &mut PoolSlot<'_>,
    ) -> Result<Bytes> {
        if let Some(v) = c.scan.as_ref().and_then(|s| s.get(true, &root)) {
            return Ok(v);
        }
        let _flight = match c.scan {
            Some(_) => None,
            None => {
                let mut flight = Flight::join(&self.search_flights, root);
                if !flight.try_lead() {
                    // Waiters hold no pool permit, or searches waiting on
                    // one derivation would starve every other search.
                    pool.release();
                    flight.wait().await;
                    pool.reacquire().await?;
                }
                Some(flight)
            }
        };
        // Finds the root's row stored if a derivation just finished it.
        let mut known = Held::new(KNOWN_BYTES);
        let mut preds = Preds::default();
        // A bounded query writes nothing, so nothing to patch.
        let pred = pred.filter(|_| c.scan.is_none());
        let dirs = match self
            .missing_dirs(root, pred, c, &mut known, &mut preds)
            .await?
        {
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
                if let Some(row) = self
                    .derive_batch(root, &batch, c, &mut known, &preds)
                    .await?
                {
                    root_row = Some(row);
                }
                rest = next;
            }
        }
        root_row.ok_or_else(|| Error::Storage(format!("no {TREE_KIND} row derived for {root}")))
    }

    /// The directories at and below `root` without a filter, parents before
    /// their children, or `root`'s own filter (whole) if it is stored after
    /// all (another derivation finished it). Stored filters read on the way
    /// are kept whole in `known` for the parents; the predecessors of the
    /// missing directories, their rows and their keyframes go in `preds`.
    async fn missing_dirs(
        &self,
        root: Hash,
        pred: Option<Pred>,
        c: &Counters,
        known: &mut Held,
        preds: &mut Preds,
    ) -> Result<Missing> {
        let mut out: Vec<Dir> = Vec::new();
        let mut seen: HashSet<Hash> = HashSet::from([root]);
        let mut level = vec![root];
        let mut lookup = vec![root];
        if let Some(p) = pred.filter(|p| p.tree != root) {
            preds.of.insert(root, p.tree);
        }
        // A bounded scan's searchable files found so far, and their bytes.
        let (mut files, mut bytes) = (0u64, 0u64);
        while !level.is_empty() {
            for chunk in level.chunks(READ_KEYS) {
                // The level's nodes and their predecessors' in one read.
                let mut keys = chunk.to_vec();
                let mut asked: HashSet<Hash> = chunk.iter().copied().collect();
                for h in chunk {
                    if let Some(&p) = preds.of.get(h) {
                        if asked.insert(p) {
                            keys.push(p);
                        }
                    }
                }
                let mut nodes = self.read_trees_shared(&keys).await?;
                let pred_nodes: HashMap<Hash, Arc<TreeNode>> = keys[chunk.len()..]
                    .iter()
                    .copied()
                    .zip(nodes.split_off(chunk.len()))
                    .collect();
                for (&hash, node) in chunk.iter().zip(nodes) {
                    let before = preds.of.get(&hash).and_then(|p| pred_nodes.get(p));
                    let mut dir = Dir {
                        hash,
                        dirs: Vec::new(),
                        files: Vec::new(),
                    };
                    for (name, entry) in &node.entries {
                        match *entry {
                            TreeEntry::Dir { tree } => {
                                dir.dirs.push(tree);
                                if seen.insert(tree) {
                                    lookup.push(tree);
                                    if let Some(TreeEntry::Dir { tree: was }) =
                                        before.and_then(|n| n.entries.get(name))
                                    {
                                        if *was != tree {
                                            preds.of.insert(tree, *was);
                                        }
                                    }
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
                    for &(_, size) in &dir.files {
                        if size <= MAX_INDEXED_BYTES {
                            files += 1;
                            bytes += size;
                        }
                    }
                    out.push(dir);
                }
                // A bounded scan gives up here, before reading the rest of
                // a cold tree, once it has found far more than it may fetch.
                if let Some(scan) = &c.scan {
                    scan.discovered(files, bytes)?;
                }
            }
            // The rows of the next level and of their predecessors in one
            // read.
            let mut ask = lookup.clone();
            let mut asked: HashSet<Hash> = lookup.iter().copied().collect();
            for h in &lookup {
                if let Some(&p) = preds.of.get(h) {
                    if asked.insert(p) {
                        ask.push(p);
                    }
                }
            }
            let rows: HashMap<Hash, Option<Bytes>> = ask
                .iter()
                .copied()
                .zip(self.rows_of(true, &ask, c).await?)
                .collect();
            for h in &lookup {
                if let Some(p) = preds.of.get(h) {
                    if let Some(row) = &rows[p] {
                        preds.rows.insert(*p, row.clone());
                    }
                }
            }
            // Keyframes of the patches read: those of the next level, to
            // know their filters, and those of their predecessors, to patch.
            let mut bases: Vec<Hash> = Vec::new();
            let pred_rows = lookup
                .iter()
                .filter_map(|h| preds.of.get(h))
                .filter_map(|p| preds.rows.get(p));
            for row in lookup
                .iter()
                .filter_map(|h| rows[h].as_ref())
                .chain(pred_rows)
            {
                if let Ok(TreeRow::Patch(p)) = TreeRow::parse(row) {
                    if !bases.contains(&p.base) {
                        bases.push(p.base);
                    }
                }
            }
            let keyframes = self.keyframe_rows(&bases).await?;
            for (h, row) in &keyframes {
                preds.keyframes.insert(*h, row.clone());
            }
            level = Vec::new();
            for h in lookup.drain(..) {
                match &rows[&h] {
                    Some(row) => {
                        let row = whole(row, &keyframes)?;
                        if h == root {
                            return Ok(Missing::Stored(row));
                        }
                        known.insert(h, row);
                    }
                    None if h == root => {}
                    None => level.push(h),
                }
            }
        }
        Ok(Missing::Dirs(out))
    }

    /// Derive one batch of directories of one height: their missing blob
    /// rows, all written first, then their filters, each keyframe before
    /// the patch on it. Returns `root`'s filter, whole, if it is in the
    /// batch.
    async fn derive_batch(
        &self,
        root: Hash,
        batch: &[&Dir],
        c: &Counters,
        known: &mut Held,
        preds: &Preds,
    ) -> Result<Option<Bytes>> {
        let mut distinct = HashSet::new();
        let files: Vec<(Hash, u64)> = batch
            .iter()
            .flat_map(|d| d.files.iter().copied())
            .filter(|f| distinct.insert(f.0))
            .collect();
        let mut blobs = Held::new(BATCH_BYTES);
        let mut writes = Writes::new(self, c);
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
                writes.push(BLOB_KIND, h, row).await?;
            }
        }
        writes.flush().await?;

        let mut root_row = None;
        for dir in batch {
            // A saturated child makes the directory saturated, so once one
            // is found nothing more is read.
            let mut union = Union::default();
            let mut absent = Vec::new();
            for t in &dir.dirs {
                match known.get(t) {
                    Some(row) => union.or_row(row)?,
                    None => absent.push(*t),
                }
            }
            if !union.is_saturated() {
                for row in self.filters_present(&absent, c).await? {
                    union.or_row(&row)?;
                }
            }
            if !union.is_saturated() {
                let mut absent = Vec::new();
                for (h, _) in &dir.files {
                    match blobs.get(h) {
                        Some(row) => add_blob_row(&mut union, row)?,
                        None => absent.push(*h),
                    }
                }
                for row in self.rows_present(false, &absent, c).await? {
                    add_blob_row(&mut union, &row)?;
                }
            }
            let filter = union.finish();
            let whole_row = Bytes::from(row_of(filter.as_ref()));
            let row = match &filter {
                Some(f) => match preds.chain(dir.hash) {
                    Chain::None => whole_row.clone(),
                    Chain::On(key, base) => match f.encode_after(Some((&key, &base))) {
                        Some(patch) => Bytes::from(patch),
                        None => {
                            writes
                                .push(KEYFRAME_KIND, dir.hash, whole_row.clone())
                                .await?;
                            Bytes::from(f.patch_on(&dir.hash, f))
                        }
                    },
                    Chain::Start => {
                        writes
                            .push(KEYFRAME_KIND, dir.hash, whole_row.clone())
                            .await?;
                        Bytes::from(f.patch_on(&dir.hash, f))
                    }
                },
                None => whole_row.clone(),
            };
            known.insert(dir.hash, whole_row.clone());
            if dir.hash == root {
                root_row = Some(whole_row);
            }
            writes.push(TREE_KIND, dir.hash, row).await?;
        }
        writes.flush().await?;
        Ok(root_row)
    }

    /// Whole filters of stored directories, patches applied to their
    /// keyframes.
    async fn filters_present(&self, keys: &[Hash], c: &Counters) -> Result<Vec<Bytes>> {
        let rows = self.rows_present(true, keys, c).await?;
        let mut bases = Vec::new();
        for row in &rows {
            if let Ok(TreeRow::Patch(p)) = TreeRow::parse(row) {
                if !bases.contains(&p.base) {
                    bases.push(p.base);
                }
            }
        }
        let keyframes = self.keyframe_rows(&bases).await?;
        rows.iter().map(|row| whole(row, &keyframes)).collect()
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

/// A search's pool permit ([`Repo::with_search_pool_permits`]), which it
/// gives back while it waits for another derivation of the same tree.
pub(crate) struct PoolSlot<'a> {
    pool: Option<&'a tokio::sync::Semaphore>,
    permit: Option<tokio::sync::SemaphorePermit<'a>>,
}

impl<'a> PoolSlot<'a> {
    /// No pool: the warmer, `derive-search` and bounded scans.
    pub(crate) fn none() -> Self {
        PoolSlot {
            pool: None,
            permit: None,
        }
    }

    /// A permit of `pool`, when there is a pool.
    pub(crate) async fn acquire(pool: Option<&'a tokio::sync::Semaphore>) -> Result<Self> {
        let mut slot = PoolSlot { pool, permit: None };
        slot.reacquire().await?;
        Ok(slot)
    }

    fn release(&mut self) {
        self.permit = None;
    }

    async fn reacquire(&mut self) -> Result<()> {
        if let (Some(pool), None) = (self.pool, &self.permit) {
            let permit = pool
                .acquire()
                .await
                .map_err(|e| Error::Other(format!("search pool: {e}")))?;
            self.permit = Some(permit);
        }
        Ok(())
    }
}

/// Search derivations in flight in this process, by tree root (see
/// [`Repo::tree_row_unstored`]).
pub(crate) type Flights = std::sync::Mutex<HashMap<Hash, Arc<tokio::sync::Mutex<()>>>>;

/// A place in line to derive one root's rows.
struct Flight<'a> {
    flights: &'a Flights,
    key: Hash,
    lock: Option<Arc<tokio::sync::Mutex<()>>>,
    guard: Option<tokio::sync::OwnedMutexGuard<()>>,
}

impl<'a> Flight<'a> {
    fn join(flights: &'a Flights, key: Hash) -> Self {
        let lock = flights
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entry(key)
            .or_default()
            .clone();
        Flight {
            flights,
            key,
            lock: Some(lock),
            guard: None,
        }
    }

    /// Lead, if no one else derives this root now.
    fn try_lead(&mut self) -> bool {
        let lock = self.lock.clone().expect("joined");
        match lock.try_lock_owned() {
            Ok(guard) => {
                self.guard = Some(guard);
                true
            }
            Err(_) => false,
        }
    }

    /// Wait until the derivation ahead ends, finished or not, then lead.
    async fn wait(&mut self) {
        let lock = self.lock.clone().expect("joined");
        self.guard = Some(lock.lock_owned().await);
    }
}

/// The last one out removes the root's entry, so the map holds only roots
/// being derived or waited for. Done under the map's lock, which every
/// newcomer takes to join.
impl Drop for Flight<'_> {
    fn drop(&mut self) {
        let mut flights = self.flights.lock().unwrap_or_else(|e| e.into_inner());
        self.guard = None;
        self.lock = None;
        if flights
            .get(&self.key)
            .is_some_and(|l| Arc::strong_count(l) == 1)
        {
            flights.remove(&self.key);
        }
    }
}

/// A stored row as the whole filter it stands for: a patch applied to its
/// keyframe, the others as they are.
fn whole(row: &Bytes, keyframes: &HashMap<Hash, Bytes>) -> Result<Bytes> {
    let TreeRow::Patch(p) = TreeRow::parse(row)? else {
        return Ok(row.clone());
    };
    let f = Bloom::of_row(row, keyframes.get(&p.base).map(|k| &k[..]))?;
    Ok(Bytes::from(row_of(f.as_ref())))
}

/// What a derivation knows of the predecessors of the directories it
/// derives (ADR 0094).
struct Preds {
    /// A missing directory's predecessor, where it has one that differs.
    of: HashMap<Hash, Hash>,
    /// The predecessors' rows, as stored.
    rows: Held,
    /// Keyframe rows, by tree.
    keyframes: Held,
}

impl Default for Preds {
    fn default() -> Self {
        Preds {
            of: HashMap::new(),
            rows: Held::new(KNOWN_BYTES),
            keyframes: Held::new(KNOWN_BYTES),
        }
    }
}

/// How a directory's filter is written given its predecessor's row.
enum Chain {
    /// Whole: no predecessor, or a saturated one, or one whose row or
    /// keyframe was not kept.
    None,
    /// A patch on this keyframe if the keyframe rule allows, else a new
    /// keyframe.
    On(Hash, Bloom),
    /// A new keyframe: the predecessor's row is whole, not on a keyframe.
    Start,
}

impl Preds {
    fn chain(&self, dir: Hash) -> Chain {
        let Some(row) = self.of.get(&dir).and_then(|p| self.rows.get(p)) else {
            return Chain::None;
        };
        match TreeRow::parse(row) {
            Ok(TreeRow::Keyframe(_)) => Chain::Start,
            Ok(TreeRow::Patch(p)) => {
                match self.keyframes.get(&p.base).map(|k| Bloom::decode(k)) {
                    Some(Ok(bits)) => Chain::On(p.base, bits),
                    // A damaged keyframe is for verify to find; this
                    // directory starts a new one.
                    Some(Err(_)) => Chain::Start,
                    None => Chain::None,
                }
            }
            Ok(TreeRow::Saturated) | Err(_) => Chain::None,
        }
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

fn add_blob_row(union: &mut Union, row: &[u8]) -> Result<()> {
    if let BlobRow::Trigrams(set) = BlobRow::decode(row)? {
        union.insert_all(set);
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

/// Rows a batch computed and has not written: put in a bounded query's
/// memory at once, else written in order [`WRITE_ROWS`] or [`BATCH_BYTES`]
/// at a time, each write after the index lease check. Order matters: a
/// write that fails part way may leave any prefix of its rows, and a
/// keyframe goes before the patch on it.
struct Writes<'a> {
    repo: &'a Repo,
    c: &'a Counters,
    rows: Vec<DerivedRow>,
    bytes: usize,
}

impl<'a> Writes<'a> {
    fn new(repo: &'a Repo, c: &'a Counters) -> Self {
        Writes {
            repo,
            c,
            rows: Vec::new(),
            bytes: 0,
        }
    }

    async fn push(&mut self, kind: &'static str, key: Hash, row: Bytes) -> Result<()> {
        if let Some(scan) = &self.c.scan {
            // A bounded query writes whole rows only, so no keyframes.
            scan.put(kind == TREE_KIND, key, row);
            return Ok(());
        }
        self.bytes += row.len();
        self.rows.push(DerivedRow::new(kind, key, row));
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
        for kind in [BLOB_KIND, TREE_KIND, KEYFRAME_KIND] {
            let n = self.rows.iter().filter(|r| r.kind == kind).count() as u64;
            if n == 0 {
                continue;
            }
            match kind {
                BLOB_KIND => self.c.blob_rows.fetch_add(n, Ordering::Relaxed),
                TREE_KIND => self.c.tree_rows.fetch_add(n, Ordering::Relaxed),
                _ => 0,
            };
            metrics::counter!("leep_derive_total", "kind" => kind).increment(n);
        }
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
    /// The whole row `tree_row` stores for `root` without a predecessor,
    /// recomputed from the tree itself: the filter over every searchable
    /// blob below it, taken from the blobs' own rows and the filters
    /// recomputed for the directories below, never from a stored directory
    /// filter, so damage in one cannot make another look wrong. For
    /// verification and repair, which compare the filters rows stand for
    /// ([`Repo::stored_filter`]), not their bytes; like derivation it writes
    /// only the blob rows it finds missing.
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
                        Some(row) => top.union.or_row(row)?,
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
                    done.union.insert_all(set);
                }
            }
            drop(rows);
            let row = Bytes::from(row_of(done.union.finish().as_ref()));
            memo.insert(done.hash, row.clone());
            match stack.last_mut() {
                Some(parent) => parent.union.or_row(&row)?,
                None => return Ok(row),
            }
        }
    }
}

impl Repo {
    /// Whether the stored filter row `row` stands for the same filter as
    /// `expected`, a whole row: a patch is applied to its keyframe, read
    /// from the store. A row that cannot be read that way does not match.
    pub(crate) async fn same_filter(&self, row: &[u8], expected: &[u8]) -> Result<bool> {
        let keyframe = match TreeRow::parse(row) {
            Ok(TreeRow::Patch(p)) => self.meta().get_derived(KEYFRAME_KIND, &p.base).await?,
            Ok(_) => None,
            Err(_) => return Ok(false),
        };
        let (Ok(got), Ok(want)) = (
            Bloom::of_row(row, keyframe.as_deref()),
            Bloom::of_row(expected, None),
        ) else {
            return Ok(false);
        };
        Ok(got == want)
    }
}

/// Whether a stored keyframe row is a whole (dense or sparse) filter equal
/// to `expected`'s, a whole row that is not saturated.
pub(crate) fn same_keyframe(row: &[u8], expected: &[u8]) -> bool {
    matches!(TreeRow::parse(row), Ok(TreeRow::Keyframe(_)))
        && matches!(
            (Bloom::of_row(row, None), Bloom::of_row(expected, None)),
            (Ok(Some(a)), Ok(Some(b))) if a == b
        )
}

#[cfg(test)]
mod height_tests {
    use super::*;

    fn h(n: u8) -> Hash {
        Hash::of_blob(&[n])
    }

    fn dir(hash: Hash, dirs: &[Hash]) -> Dir {
        Dir {
            hash,
            dirs: dirs.to_vec(),
            files: Vec::new(),
        }
    }

    /// A diamond: the root holds `a` and `s`, and `a` holds `s` again, so
    /// the same missing subtree sits at depths 1 and 2, under parents of
    /// different heights. In any order the walk could list them, every
    /// missing child is lower than its parent, and a stored child (not
    /// listed) is ignored.
    #[test]
    fn a_shared_subtree_is_lower_than_every_parent() {
        let (root, a, s, leaf, stored) = (h(0), h(1), h(2), h(3), h(4));
        let all = [
            dir(root, &[a, s, stored]),
            dir(a, &[s, stored]),
            dir(s, &[leaf]),
            dir(leaf, &[stored]),
        ];
        for order in [[0, 1, 2, 3], [0, 2, 1, 3], [3, 2, 1, 0], [2, 0, 3, 1]] {
            let dirs: Vec<Dir> = order
                .iter()
                .map(|&i| dir(all[i].hash, &all[i].dirs))
                .collect();
            let levels = by_height(&dirs);
            let height = |x: Hash| {
                levels
                    .iter()
                    .position(|l| l.iter().any(|&i| dirs[i].hash == x))
                    .expect("listed once")
            };
            assert_eq!(levels.iter().map(Vec::len).sum::<usize>(), 4);
            for d in &dirs {
                for &child in d.dirs.iter().filter(|&&c| c != stored) {
                    assert!(height(child) < height(d.hash), "{order:?}");
                }
            }
            assert_eq!(
                [height(leaf), height(s), height(a), height(root)],
                [0, 1, 2, 3],
                "{order:?}"
            );
        }
    }
}
