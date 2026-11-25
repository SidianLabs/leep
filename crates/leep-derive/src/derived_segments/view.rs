//! The read path of derived segments (ADR 0104, section "Read path"; ADR
//! 0105, 0106): a [`MetaStore`] that answers reads of the kinds a segment
//! family holds (`skiplist`, `history-tree-v4`, `generation`,
//! `last_merge-v1`, `root_tree`, `log-filter-v1`, and the code search rows
//! and keyframes) that the database misses from the live
//! segments, installed by `Repo::new` so every read site gets it unchanged.
//!
//! Order of a read: the database, exactly the statement issued before;
//! then, for misses, the in-memory key indexes of the family's live
//! segments, newest first. Before a miss is declared, manifests older than
//! [`REFRESH`] are read again, every family's together: in one batched read
//! for a batched read's miss, one single-row read per family for a
//! single-row read's miss, at most once a minute per process, and only when
//! a row is missing (in mode `build` only rows not derived yet); a
//! `get_derived_kinds` of these kinds carries them as one more read of the
//! same call. Statement counts per operation do not change (ADR 0081). Rows are
//! deleted at least [`RETIRE_AFTER`](super::RETIRE_AFTER) after their
//! segment was published, so a miss judged against a manifest at most
//! [`REFRESH`] old is a true miss. DB rows win over segments, so `repair
//! rederive` fixes a wrong value by writing a row.

use std::collections::{BTreeMap, HashSet};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use leep_core::prefix::IdPrefix;
use leep_core::{
    BlobStore, Changeset, ChangesetId, DeleteStep, DerivedRow, Error, Hash, MetaStore, RefLogEntry,
    RefStep, RefUpdateContext, Result,
};

use super::codec::{self, Block, Loc, Manifest, SegmentData, SegmentIndex, SegmentRef, FAMILIES};
use super::MANIFEST_KIND;
use crate::repo::{CacheScale, SizedCache};

/// How old a manifest may be when a miss is decided against it.
pub const REFRESH: Duration = Duration::from_secs(60);

/// Bytes of decompressed blocks kept per handle (a block is about 64 KiB).
pub(crate) const BLOCK_CACHE_BYTES: usize = 16 << 20;
/// Bytes of data blobs kept per handle (each at most 4 MiB): a block miss
/// in a kept segment costs no blob read.
pub(crate) const DATA_CACHE_BYTES: usize = 32 << 20;

type BlockCache = SizedCache<Block, BLOCK_CACHE_BYTES>;
type DataCache = SizedCache<SegmentData, DATA_CACHE_BYTES>;

/// A live segment whose index is in memory.
#[derive(Debug)]
pub struct Live {
    pub r: SegmentRef,
    pub idx: Arc<SegmentIndex>,
}

/// What a read of the manifest found.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ManifestRead {
    Absent,
    Read(Manifest),
    /// The row does not decode: no segment can be found through it.
    Damaged,
    /// A newer binary wrote it (checksum good, version, flags or family
    /// unknown): this one reads no segment through it, and writes nothing.
    Newer,
}

impl ManifestRead {
    /// What `raw`, read from `family`'s manifest key, says. A manifest of
    /// another family under that key is damage.
    pub fn of(family: u8, raw: Option<&[u8]>) -> ManifestRead {
        match raw {
            None => ManifestRead::Absent,
            Some(v) => match Manifest::parse(v) {
                Ok(m) if m.family == family => ManifestRead::Read(m),
                Ok(_) => ManifestRead::Damaged,
                Err(codec::ManifestFault::Newer(_)) => ManifestRead::Newer,
                Err(codec::ManifestFault::Damaged(_)) => ManifestRead::Damaged,
            },
        }
    }
}

#[derive(Default)]
struct State {
    /// The manifest row as last read; `None`: no row.
    raw: Option<Bytes>,
    /// When it was read; `None`: never.
    loaded: Option<tokio::time::Instant>,
    damaged: bool,
    /// The `seq` of the manifest in use: an older one read late is ignored.
    seq: Option<u64>,
    /// Live segments whose index could be read, newest first.
    segs: Vec<Arc<Live>>,
    /// Live segments named by `raw` whose index or data could not be read,
    /// and when: left out for [`REFRESH`], then tried again (a blob a repack
    /// was moving reads again), or until the manifest changes.
    broken: BTreeMap<Hash, tokio::time::Instant>,
}

struct Caches {
    blocks: BlockCache,
    datas: DataCache,
}

/// The block and data caches every family's view shares (keyed by data
/// blob, so families never collide), under one budget.
type SharedCaches = Arc<RwLock<Arc<Caches>>>;

/// One handle's view of the live segments of every family. Shared by a
/// `Repo` and its clones.
pub struct Segments {
    /// One per entry of [`FAMILIES`], in its order.
    views: Vec<Arc<SegmentView>>,
    caches: SharedCaches,
}

impl Segments {
    pub fn new(blobs: Arc<dyn BlobStore>) -> Self {
        let caches: SharedCaches = Arc::new(RwLock::new(Arc::new(Caches {
            blocks: SizedCache::default(),
            datas: SizedCache::default(),
        })));
        let views = FAMILIES
            .iter()
            .map(|f| Arc::new(SegmentView::new(blobs.clone(), f.id, caches.clone())))
            .collect();
        Segments { views, caches }
    }

    /// Caches scaled like the rest of a `Repo`'s (see `with_cache_budget`).
    pub(crate) fn scale(&self, s: CacheScale) {
        *self.caches.write().unwrap() = Arc::new(Caches {
            blocks: SizedCache::scaled(s),
            datas: SizedCache::scaled(s),
        });
    }

    /// The view of `family`. Every family this binary knows has one.
    pub fn family(&self, family: u8) -> &Arc<SegmentView> {
        self.views
            .iter()
            .find(|v| v.family == family)
            .expect("a family this binary knows")
    }

    pub fn views(&self) -> impl Iterator<Item = &Arc<SegmentView>> {
        self.views.iter()
    }

    /// Whether a manifest is older than [`REFRESH`]: the next read of a
    /// segmented kind should bring every family's.
    pub fn stale(&self) -> bool {
        self.views.iter().any(|v| v.stale())
    }

    /// Bytes the live segments' indexes hold in memory, every family.
    pub fn index_memory(&self) -> usize {
        self.views.iter().map(|v| v.index_memory()).sum()
    }

    /// The manifest keys of every family, in [`FAMILIES`] order.
    pub(crate) fn manifest_keys() -> Vec<Hash> {
        FAMILIES.iter().map(|f| f.manifest_key()).collect()
    }

    /// Use `raws` (every family's manifest row, in [`FAMILIES`] order, as
    /// just read). Every family is applied, whatever another's error (a
    /// transient read of one family's index blob must not freeze the other
    /// family's view); the first error is returned.
    pub async fn apply_all(&self, raws: Vec<Option<Bytes>>) -> Result<()> {
        first_error(self.apply_each(raws).await?)
    }

    /// [`Segments::apply_all`], each family's outcome apart (in
    /// [`FAMILIES`] order), so a reader of one family is not failed by
    /// another's transient error.
    pub async fn apply_each(&self, raws: Vec<Option<Bytes>>) -> Result<Vec<Result<()>>> {
        if raws.len() != self.views.len() {
            return Err(Error::Storage(format!(
                "read {} {MANIFEST_KIND} rows for {} families",
                raws.len(),
                self.views.len()
            )));
        }
        let mut out = Vec::with_capacity(raws.len());
        for (v, raw) in self.views.iter().zip(raws) {
            out.push(v.apply(raw).await);
        }
        Ok(out)
    }

    /// Read every family's manifest from `rows` in one statement and use
    /// them.
    pub async fn refresh(&self, rows: &dyn MetaStore) -> Result<()> {
        first_error(self.refresh_each(rows).await?)
    }

    /// [`Segments::refresh`], each family's outcome apart.
    pub async fn refresh_each(&self, rows: &dyn MetaStore) -> Result<Vec<Result<()>>> {
        let raws = rows
            .get_derived_many(MANIFEST_KIND, &Self::manifest_keys())
            .await?;
        self.apply_each(raws).await
    }

    /// [`Segments::refresh`] with one single-row read per family, for a miss
    /// of a single-row read: such a read never turns into a batched one
    /// (whose failure behaviour differs), at the cost of one more statement
    /// a minute per family.
    /// Each family is read and applied on its own; the first error is
    /// returned once all were tried.
    pub async fn refresh_singly(&self, rows: &dyn MetaStore) -> Result<()> {
        first_error(self.refresh_singly_each(rows).await)
    }

    /// [`Segments::refresh_singly`], each family's outcome apart.
    pub async fn refresh_singly_each(&self, rows: &dyn MetaStore) -> Vec<Result<()>> {
        let mut out = Vec::with_capacity(self.views.len());
        for (v, k) in self.views.iter().zip(Self::manifest_keys()) {
            out.push(match rows.get_derived(MANIFEST_KIND, &k).await {
                Ok(raw) => v.apply(raw).await,
                Err(e) => Err(e),
            });
        }
        out
    }

    /// Of `each` (every family's outcome, in [`FAMILIES`] order), the first
    /// error of a family in `wanted`; the others' errors are logged, so a
    /// read of one family never fails for another family's segments.
    pub(crate) fn errors_of(each: Vec<Result<()>>, wanted: &[u8]) -> Result<()> {
        let mut first = None;
        for (fam, r) in FAMILIES.iter().zip(each) {
            if let Err(e) = r {
                if wanted.contains(&fam.id) {
                    first.get_or_insert(e);
                } else {
                    tracing::warn!(
                        family = fam.name,
                        error = %e,
                        "derived segment manifest not applied; this family's reads try again"
                    );
                }
            }
        }
        first.map_or(Ok(()), Err)
    }
}

fn first_error(each: Vec<Result<()>>) -> Result<()> {
    each.into_iter().find_map(|r| r.err()).map_or(Ok(()), Err)
}

/// One family's view of its live segments: their indexes in memory, and
/// what was read from them in the shared caches.
pub struct SegmentView {
    family: u8,
    blobs: Arc<dyn BlobStore>,
    state: RwLock<State>,
    /// Held while a manifest is applied, so two readers do not load the
    /// same index blobs at once.
    apply: tokio::sync::Mutex<()>,
    caches: SharedCaches,
    /// Data blob loads in flight, by hash, so concurrent misses read one.
    loading: Mutex<BTreeMap<Hash, Arc<tokio::sync::Mutex<()>>>>,
    refresh: Duration,
}

impl SegmentView {
    fn new(blobs: Arc<dyn BlobStore>, family: u8, caches: SharedCaches) -> Self {
        SegmentView {
            family,
            blobs,
            state: RwLock::default(),
            apply: tokio::sync::Mutex::new(()),
            caches,
            loading: Mutex::default(),
            refresh: REFRESH,
        }
    }

    pub fn family(&self) -> u8 {
        self.family
    }

    fn family_name(&self) -> &'static str {
        codec::family(self.family).map_or("unknown", |f| f.name)
    }

    fn caches(&self) -> Arc<Caches> {
        self.caches.read().unwrap().clone()
    }

    /// Whether the next read of this family's kinds should bring the
    /// manifest.
    pub fn stale(&self) -> bool {
        self.state
            .read()
            .unwrap()
            .loaded
            .is_none_or(|t| t.elapsed() >= self.refresh)
    }

    /// Live segments in use, newest first.
    pub fn segments(&self) -> Vec<Arc<Live>> {
        self.state.read().unwrap().segs.clone()
    }

    /// The manifest bytes this view last applied.
    pub fn raw(&self) -> Option<Bytes> {
        self.state.read().unwrap().raw.clone()
    }

    pub fn damaged(&self) -> bool {
        self.state.read().unwrap().damaged
    }

    /// Use the manifest `raw` (as just read): load the index of every live
    /// segment not loaded yet. A segment whose index is not stored, is
    /// damaged or cannot be read now is left out (its keys read as rows or
    /// derive again), the last for [`REFRESH`], then tried again: one
    /// segment whose blob fails to read must not fail every read of the
    /// family, which would also keep the view stale and refetch every
    /// index on each miss.
    pub async fn apply(&self, raw: Option<Bytes>) -> Result<()> {
        let _held = self.apply.lock().await;
        {
            let mut s = self.state.write().unwrap();
            let retry = s.broken.values().any(|t| t.elapsed() >= self.refresh);
            if s.loaded.is_some() && s.raw == raw && !retry {
                s.loaded = Some(tokio::time::Instant::now());
                return Ok(());
            }
        }
        let read = ManifestRead::of(self.family, raw.as_deref());
        let family = self.family_name();
        let live = match &read {
            ManifestRead::Absent => Vec::new(),
            ManifestRead::Read(m) => {
                // Monotonic: a manifest older than the one in use (a read
                // that raced a newer write) is not applied.
                let mut s = self.state.write().unwrap();
                if s.seq.is_some_and(|cur| m.seq < cur) {
                    s.loaded = Some(tokio::time::Instant::now());
                    return Ok(());
                }
                drop(s);
                m.live.clone()
            }
            ManifestRead::Damaged | ManifestRead::Newer => {
                let newer = read == ManifestRead::Newer;
                metrics::counter!(
                    "leep_derived_segment_errors_total",
                    "what" => "manifest", "family" => family
                )
                .increment(1);
                if newer {
                    tracing::warn!(
                        family,
                        "the {MANIFEST_KIND} row was written by a newer leep-server: this one \
                         reads no derived segment through it (rows still are)"
                    );
                } else {
                    tracing::error!(
                        family,
                        "the {MANIFEST_KIND} row does not decode: derived segments are not \
                         read (rows still are); see LeepDerivedSegmentPassesFailing in \
                         RUNBOOKS.md"
                    );
                }
                // The segments in use were read whole and checked; they
                // still give right values, so keep them rather than read
                // everything as missing.
                let mut s = self.state.write().unwrap();
                s.raw = raw;
                s.loaded = Some(tokio::time::Instant::now());
                s.damaged = true;
                return Ok(());
            }
        };
        let (have, mut broken) = {
            let s = self.state.read().unwrap();
            let have: BTreeMap<Hash, Arc<Live>> =
                s.segs.iter().map(|l| (l.r.index, l.clone())).collect();
            (have, s.broken.clone())
        };
        broken.retain(|h, t| live.iter().any(|s| s.index == *h) && t.elapsed() < self.refresh);
        // Indexes not in memory yet, fetched a few at a time.
        let wanted: Vec<SegmentRef> = live
            .iter()
            .filter(|r| !have.contains_key(&r.index) && !broken.contains_key(&r.index))
            .copied()
            .collect();
        let loaded = load_indexes(&*self.blobs, self.family, &wanted).await;
        let mut segs = Vec::with_capacity(live.len());
        for r in live.iter().rev() {
            if let Some(l) = have.get(&r.index) {
                if l.r == *r {
                    segs.push(l.clone());
                } else {
                    // Same segment, other columns (repair drop).
                    segs.push(Arc::new(Live {
                        r: *r,
                        idx: l.idx.clone(),
                    }));
                }
                continue;
            }
            match loaded.get(&r.index) {
                Some(Ok(Some(idx))) => segs.push(Arc::new(Live {
                    r: *r,
                    idx: idx.clone(),
                })),
                Some(Ok(None)) => {
                    broken.insert(r.index, tokio::time::Instant::now());
                }
                Some(Err(e)) => {
                    // Left out like a lost one, but only for `REFRESH`: its
                    // keys read as rows or derive again meanwhile, and every
                    // other segment of the family still answers.
                    metrics::counter!(
                        "leep_derived_segment_errors_total",
                        "what" => "index-read", "family" => family
                    )
                    .increment(1);
                    tracing::warn!(
                        segment = %r.index,
                        family,
                        error = %e,
                        "derived segment index cannot be read now; its keys read as rows or \
                         derive again, and it is tried again in a minute"
                    );
                    broken.insert(r.index, tokio::time::Instant::now());
                }
                None => {}
            }
        }
        let memory: usize = segs.iter().map(|l| l.idx.memory()).sum();
        metrics::gauge!("leep_derived_segment_index_bytes", "family" => family).set(memory as f64);
        let mut s = self.state.write().unwrap();
        s.seq = match &read {
            ManifestRead::Read(m) => Some(m.seq),
            _ => s.seq,
        };
        s.raw = raw;
        s.loaded = Some(tokio::time::Instant::now());
        s.damaged = false;
        s.segs = segs;
        s.broken = broken;
        Ok(())
    }

    /// Bytes the live segments' indexes hold in memory.
    pub fn index_memory(&self) -> usize {
        self.segments().iter().map(|l| l.idx.memory()).sum()
    }

    /// Stop using a live segment whose data cannot be read, until the
    /// manifest changes.
    fn mark_broken(&self, index: Hash) {
        let mut s = self.state.write().unwrap();
        s.segs.retain(|l| l.r.index != index);
        s.broken.insert(index, tokio::time::Instant::now());
    }

    /// The newest live segment holding `key`'s value of `col`.
    pub fn find(&self, key: &Hash, col: u8) -> Option<(Arc<Live>, Loc)> {
        let s = self.state.read().unwrap();
        s.segs
            .iter()
            .filter(|l| l.r.has(col))
            .find_map(|l| l.idx.locate(key, col).map(|loc| (l.clone(), loc)))
    }

    /// Whether a live segment holds `key`'s value of `col`, from the
    /// indexes alone.
    pub fn holds(&self, key: &Hash, col: u8) -> bool {
        self.find(key, col).is_some()
    }

    /// `key`'s value of `col` from the newest live segment holding it.
    pub async fn lookup(&self, key: &Hash, col: u8) -> Result<Found> {
        let Some((live, loc)) = self.find(key, col) else {
            return Ok(Found::No);
        };
        self.read_at(&live, loc, col).await
    }

    /// The value at `loc` of segment `live`.
    pub async fn read_at(&self, live: &Live, loc: Loc, col: u8) -> Result<Found> {
        let Some(data) = self.data(live).await? else {
            return Ok(Found::Gone(live.r.index));
        };
        let caches = self.caches();
        let bkey = block_key(&live.r.data, loc.col, loc.block);
        let block = match caches.blocks.get(&bkey) {
            Some(b) => b,
            None => match codec::read_block(&live.idx, &data, loc.col, loc.block) {
                Ok(b) => {
                    let b = Arc::new(b);
                    caches.blocks.insert_sized(bkey, b.clone(), b.memory());
                    b
                }
                Err(e) => {
                    self.damaged_segment(live, &e);
                    return Ok(Found::Gone(live.r.index));
                }
            },
        };
        let value = block
            .row(loc.rank)
            .ok_or_else(|| Error::Encoding("derived segment: block short".into()))
            .and_then(|row| codec::row_value(&live.idx, &data, col, loc.ord, row));
        match value {
            Ok(v) => {
                metrics::counter!(
                    "leep_derived_segment_reads_total",
                    "outcome" => "hit", "family" => self.family_name()
                )
                .increment(1);
                Ok(Found::Yes(v))
            }
            Err(e) => {
                self.damaged_segment(live, &e);
                Ok(Found::Gone(live.r.index))
            }
        }
    }

    fn damaged_segment(&self, live: &Live, e: &Error) {
        metrics::counter!(
            "leep_derived_segment_errors_total",
            "what" => "segment", "family" => self.family_name()
        )
        .increment(1);
        tracing::warn!(
            segment = %live.r.index,
            family = self.family_name(),
            "derived segment does not decode: {e}"
        );
        self.mark_broken(live.r.index);
    }

    /// The data blob of `live`, read once and kept; `None` when it is not
    /// stored or reads damaged.
    pub async fn data(&self, live: &Live) -> Result<Option<Arc<SegmentData>>> {
        let caches = self.caches();
        if let Some(d) = caches.datas.get(&live.r.data) {
            return Ok(Some(d));
        }
        let gate = self
            .loading
            .lock()
            .unwrap()
            .entry(live.r.data)
            .or_default()
            .clone();
        // Removed however this future ends, cancelled included.
        let _entry = Loading {
            map: &self.loading,
            key: live.r.data,
        };
        let _one = gate.lock().await;
        async {
            if let Some(d) = caches.datas.get(&live.r.data) {
                return Ok(Some(d));
            }
            let bytes = match get_segment_blob(&*self.blobs, &live.r.data).await {
                Ok(Some(b)) => b,
                Ok(None) => {
                    metrics::counter!(
                        "leep_derived_segment_errors_total",
                        "what" => "missing", "family" => self.family_name()
                    )
                    .increment(1);
                    return Ok(None);
                }
                Err(e) if crate::gc_segments::segment_reads_corrupt(&e) => {
                    self.damaged_segment(live, &e);
                    return Ok(None);
                }
                Err(e) => return Err(e),
            };
            match codec::decode_data(&live.idx, &live.r.data, bytes) {
                Ok(d) => {
                    let d = Arc::new(d);
                    caches
                        .datas
                        .insert_sized(live.r.data, d.clone(), d.len() + 64);
                    Ok(Some(d))
                }
                Err(e) => {
                    self.damaged_segment(live, &e);
                    Ok(None)
                }
            }
        }
        .await
    }

    /// Values of `col` the live segments hold, a key in several counted in
    /// each: an upper bound, for estimates, without building a set.
    pub fn sealed_upper(&self, col: u8) -> u64 {
        self.segments()
            .iter()
            .filter(|l| l.r.has(col))
            .map(|l| l.idx.keys_with(col).count() as u64)
            .sum()
    }

    /// Keys a live segment holds a value of `col` for, each once (counted
    /// in the newest segment holding it), without building a set.
    pub fn sealed_distinct(&self, col: u8) -> u64 {
        let segs: Vec<Arc<Live>> = self
            .segments()
            .into_iter()
            .filter(|l| l.r.has(col))
            .collect();
        let mut n = 0;
        for (i, l) in segs.iter().enumerate() {
            n += l
                .idx
                .keys_with(col)
                .filter(|k| !segs[..i].iter().any(|newer| newer.idx.has(k, col)))
                .count() as u64;
        }
        n
    }

    /// Keys a live segment holds a value of `col` for, deduplicated.
    pub fn keys_with(&self, col: u8) -> HashSet<Hash> {
        let mut out = HashSet::new();
        for l in self.segments() {
            if !l.r.has(col) {
                continue;
            }
            for k in l.idx.keys() {
                if l.idx.has(k, col) {
                    out.insert(*k);
                }
            }
        }
        out
    }
}

/// A data blob load in flight, forgotten when the load ends or is dropped.
struct Loading<'a> {
    map: &'a Mutex<BTreeMap<Hash, Arc<tokio::sync::Mutex<()>>>>,
    key: Hash,
}

impl Drop for Loading<'_> {
    fn drop(&mut self) {
        self.map
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.key);
    }
}

/// What a segment lookup found.
#[derive(Debug)]
pub enum Found {
    Yes(Bytes),
    No,
    /// The segment holding it cannot be read now: not stored (merged away
    /// and collected) or damaged. The reader refreshes the manifest once.
    Gone(Hash),
}

fn block_key(data: &Hash, col: usize, block: usize) -> Hash {
    let mut k = *data;
    let pos = ((col as u64) << 40 | block as u64).to_le_bytes();
    for (b, p) in k.0.iter_mut().zip(pos) {
        *b ^= p;
    }
    k
}

/// Index blobs read at once by a manifest change.
const INDEX_FETCHES: usize = 8;

/// What loading one segment's index found.
pub(crate) type IndexLoad = Result<Option<Arc<SegmentIndex>>>;

/// [`load_index`] of each of `wanted`, [`INDEX_FETCHES`] at a time, each
/// outcome apart: one index that cannot be read now must not keep the
/// others from being used.
pub(crate) async fn load_indexes(
    blobs: &dyn BlobStore,
    family: u8,
    wanted: &[SegmentRef],
) -> BTreeMap<Hash, IndexLoad> {
    use futures_util::stream::{self, StreamExt};
    stream::iter(wanted.iter().copied())
        .map(|r| async move {
            (
                r.index,
                load_index(blobs, family, &r).await.map(|i| i.map(Arc::new)),
            )
        })
        .buffer_unordered(INDEX_FETCHES)
        .collect()
        .await
}

/// A segment blob, with a miss that is exact: `get`, and after a miss
/// `get_exact`, so a blob another process packed a moment ago (segments are
/// written loose and packed minutes later) is not taken for lost while a
/// cached pack view hides it (ADR 0070). Costs nothing more on a hit.
pub(crate) async fn get_segment_blob(blobs: &dyn BlobStore, hash: &Hash) -> Result<Option<Bytes>> {
    match blobs.get(hash).await? {
        Some(b) => Ok(Some(b)),
        None => blobs.get_exact(hash).await,
    }
}

/// Whether a segment blob is stored, with an exact "no" (see
/// [`get_segment_blob`]).
pub(crate) async fn segment_blob_stored(blobs: &dyn BlobStore, hash: &Hash) -> Result<bool> {
    if blobs.contains(hash).await? {
        return Ok(true);
    }
    Ok(blobs.get_exact(hash).await?.is_some())
}

/// Read and decode `r`'s index blob, named by `family`'s manifest. `None`
/// when it is not stored, is damaged, or names another family, data blob
/// or row count.
pub(crate) async fn load_index(
    blobs: &dyn BlobStore,
    family: u8,
    r: &SegmentRef,
) -> Result<Option<SegmentIndex>> {
    let what = match get_segment_blob(blobs, &r.index).await {
        Ok(Some(bytes)) => match codec::decode_index(&r.index, &bytes) {
            Ok(idx)
                if idx.family == family && idx.data == r.data && idx.len() == r.rows as usize =>
            {
                return Ok(Some(idx))
            }
            Ok(_) => {
                "names another family, data blob or row count than its manifest entry".to_string()
            }
            Err(e) => e.to_string(),
        },
        Ok(None) => "is not stored".to_string(),
        Err(e) if crate::gc_segments::segment_reads_corrupt(&e) => e.to_string(),
        Err(e) => return Err(e),
    };
    let name = codec::family(family).map_or("unknown", |f| f.name);
    metrics::counter!(
        "leep_derived_segment_errors_total",
        "what" => "index", "family" => name
    )
    .increment(1);
    tracing::warn!(
        segment = %r.index,
        family = name,
        "derived segment index {what}; its keys read as rows or derive again"
    );
    Ok(None)
}

/// The `MetaStore` a `Repo` reads through. See the module comment.
pub(crate) struct SegmentedMeta {
    inner: Arc<dyn MetaStore>,
    view: Arc<Segments>,
}

impl SegmentedMeta {
    pub(crate) fn new(inner: Arc<dyn MetaStore>, view: Arc<Segments>) -> Self {
        SegmentedMeta { inner, view }
    }

    /// Read every family's manifest again now and use them: in one batched
    /// read, or for a single-row read's miss in single reads. Only an error
    /// of `family`'s (a failed read of the manifests, or its own segments)
    /// is returned; another family's is logged.
    async fn refresh(&self, single: bool, family: u8) -> Result<()> {
        let each = if single {
            self.view.refresh_singly_each(&*self.inner).await
        } else {
            self.view.refresh_each(&*self.inner).await?
        };
        Segments::errors_of(each, &[family])
    }

    /// Fill the misses in `rows` (values of `keys` of column `col` of
    /// `family`) from the live segments. A segment that cannot be read makes
    /// the view read the manifests again, once, and look again. `single`:
    /// the miss is a single-row read's (`get_derived`), which refreshes with
    /// single-row reads; a batched read's (even of one key) refreshes with
    /// one batched read of every family's manifest.
    async fn fill(
        &self,
        family: u8,
        col: u8,
        keys: &[Hash],
        rows: &mut [Option<Bytes>],
        single: bool,
    ) -> Result<()> {
        if rows.iter().all(Option::is_some) {
            return Ok(());
        }
        let view = self.view.family(family);
        // A miss is judged against a manifest at most `REFRESH` old.
        if view.stale() {
            self.refresh(single, family).await?;
        }
        if view.state.read().unwrap().segs.is_empty() {
            return Ok(());
        }
        let mut retried = false;
        loop {
            let mut gone = false;
            for (k, row) in keys.iter().zip(rows.iter_mut()) {
                if row.is_some() {
                    continue;
                }
                match view.lookup(k, col).await? {
                    Found::Yes(v) => *row = Some(v),
                    Found::No => {}
                    Found::Gone(index) => {
                        gone = true;
                        if retried {
                            view.mark_broken(index);
                        }
                    }
                }
            }
            if !gone || retried {
                return Ok(());
            }
            retried = true;
            self.refresh(single, family).await?;
        }
    }

    /// The reads with every family's manifest added when they are due, as
    /// one call.
    async fn read_kinds(&self, reads: &[(&str, &[Hash])]) -> Result<Vec<Vec<Option<Bytes>>>> {
        let wants = reads
            .iter()
            .any(|(k, keys)| codec::kind_column(k).is_some() && !keys.is_empty());
        if !wants || !self.view.stale() {
            return self.inner.get_derived_kinds(reads).await;
        }
        let mk = Segments::manifest_keys();
        let mut with: Vec<(&str, &[Hash])> = reads.to_vec();
        with.push((MANIFEST_KIND, &mk));
        let mut got = self.inner.get_derived_kinds(&with).await?;
        let manifests = got
            .pop()
            .filter(|v| v.len() == mk.len())
            .ok_or_else(|| Error::Storage("get_derived_kinds answered short".into()))?;
        // Only the families this read asks for may fail it.
        let wanted: Vec<u8> = reads
            .iter()
            .filter(|(_, keys)| !keys.is_empty())
            .filter_map(|(k, _)| codec::kind_column(k).map(|(f, _)| f))
            .collect();
        Segments::errors_of(self.view.apply_each(manifests).await?, &wanted)?;
        Ok(got)
    }
}

#[async_trait]
impl MetaStore for SegmentedMeta {
    fn view_base(&self) -> Option<Arc<dyn MetaStore>> {
        Some(self.inner.clone())
    }

    async fn get_derived(&self, kind: &str, key: &Hash) -> Result<Option<Bytes>> {
        let Some((family, col)) = codec::kind_column(kind) else {
            return self.inner.get_derived(kind, key).await;
        };
        let mut row = self.inner.get_derived(kind, key).await?;
        if row.is_none() {
            let mut rows = [None];
            self.fill(family, col, &[*key], &mut rows, true).await?;
            row = rows[0].take();
        }
        Ok(row)
    }

    async fn get_derived_many(&self, kind: &str, keys: &[Hash]) -> Result<Vec<Option<Bytes>>> {
        let Some((family, col)) = codec::kind_column(kind) else {
            return self.inner.get_derived_many(kind, keys).await;
        };
        let mut rows = self.inner.get_derived_many(kind, keys).await?;
        if rows.len() != keys.len() {
            return Err(Error::Storage(format!(
                "get_derived_many answered {} of {} {kind} keys",
                rows.len(),
                keys.len()
            )));
        }
        self.fill(family, col, keys, &mut rows, false).await?;
        Ok(rows)
    }

    async fn get_derived_kinds(
        &self,
        reads: &[(&str, &[Hash])],
    ) -> Result<Vec<Vec<Option<Bytes>>>> {
        let mut got = self.read_kinds(reads).await?;
        if got.len() != reads.len() {
            return Err(Error::Storage("get_derived_kinds answered short".into()));
        }
        for ((kind, keys), rows) in reads.iter().zip(got.iter_mut()) {
            if let Some((family, col)) = codec::kind_column(kind) {
                if rows.len() != keys.len() {
                    return Err(Error::Storage("get_derived_kinds answered short".into()));
                }
                self.fill(family, col, keys, rows, false).await?;
            }
        }
        Ok(got)
    }

    async fn derived_present(&self, kind: &str, keys: &[Hash]) -> Result<Vec<bool>> {
        let mut have = self.inner.derived_present(kind, keys).await?;
        if let Some((family, col)) = codec::kind_column(kind) {
            if have.iter().any(|h| !h) {
                let view = self.view.family(family);
                if view.stale() {
                    self.refresh(false, family).await?;
                }
                for (k, h) in keys.iter().zip(have.iter_mut()) {
                    *h = *h || view.holds(k, col);
                }
            }
        }
        Ok(have)
    }

    /// Rows and segment values together, each key once.
    async fn count_derived(&self, kind: &str) -> Result<u64> {
        let Some((family, col)) = codec::kind_column(kind) else {
            return self.inner.count_derived(kind).await;
        };
        self.refresh(false, family).await?;
        let view = self.view.family(family);
        let mut n = view.sealed_distinct(col);
        if n == 0 {
            return self.inner.count_derived(kind).await;
        }
        let mut after = None;
        loop {
            let page = self.inner.list_derived_keys(kind, after, 10_000).await?;
            let Some(last) = page.last().copied() else {
                return Ok(n);
            };
            after = Some(last);
            n += page.iter().filter(|k| !view.holds(k, col)).count() as u64;
        }
    }

    /// The larger of the row estimate and the keys segments hold: rows and
    /// segments overlap until `retire` deletes the rows.
    async fn estimate_derived(&self, kind: &str) -> Result<leep_core::RowCount> {
        let mut est = self.inner.estimate_derived(kind).await?;
        if let Some((family, col)) = codec::kind_column(kind) {
            let view = self.view.family(family);
            if view.stale() {
                self.refresh(false, family).await?;
            }
            let sealed = view.sealed_upper(col);
            if sealed > est.rows {
                est.rows = sealed;
                est.exact = false;
            }
        }
        Ok(est)
    }

    async fn put_changeset(&self, cs: &Changeset) -> Result<ChangesetId> {
        self.inner.put_changeset(cs).await
    }

    async fn get_changeset(&self, id: &ChangesetId) -> Result<Option<Changeset>> {
        self.inner.get_changeset(id).await
    }

    async fn get_changesets(&self, ids: &[ChangesetId]) -> Result<Vec<Option<Changeset>>> {
        self.inner.get_changesets(ids).await
    }

    async fn list_changesets(
        &self,
        after: Option<ChangesetId>,
        limit: usize,
    ) -> Result<Vec<ChangesetId>> {
        self.inner.list_changesets(after, limit).await
    }

    async fn changesets_with_prefix(
        &self,
        prefix: &IdPrefix,
        limit: usize,
    ) -> Result<Vec<ChangesetId>> {
        self.inner.changesets_with_prefix(prefix, limit).await
    }

    async fn changesets_stored_at(&self, ids: &[ChangesetId]) -> Result<Vec<Option<u64>>> {
        self.inner.changesets_stored_at(ids).await
    }

    async fn get_ref(&self, name: &str) -> Result<Option<ChangesetId>> {
        self.inner.get_ref(name).await
    }

    async fn cas_ref(
        &self,
        name: &str,
        expected: Option<ChangesetId>,
        new: Option<ChangesetId>,
    ) -> Result<bool> {
        self.inner.cas_ref(name, expected, new).await
    }

    async fn cas_ref_logged(
        &self,
        name: &str,
        expected: Option<ChangesetId>,
        new: Option<ChangesetId>,
        ctx: &RefUpdateContext,
    ) -> Result<bool> {
        self.inner.cas_ref_logged(name, expected, new, ctx).await
    }

    async fn cas_ref_logged_chain(
        &self,
        name: &str,
        expected: Option<ChangesetId>,
        steps: &[RefStep],
    ) -> Result<usize> {
        self.inner.cas_ref_logged_chain(name, expected, steps).await
    }

    async fn find_lands(
        &self,
        name: &str,
        ids: &[leep_core::LandId],
    ) -> Result<Vec<Option<leep_core::LandRecord>>> {
        self.inner.find_lands(name, ids).await
    }

    async fn delete_changesets(&self, ids: &[ChangesetId], kinds: &[&str]) -> Result<u64> {
        self.inner.delete_changesets(ids, kinds).await
    }

    async fn delete_derived_keys(&self, kinds: &[&str], keys: &[Hash]) -> Result<u64> {
        self.inner.delete_derived_keys(kinds, keys).await
    }

    async fn delete_derived_values(&self, kind: &str, rows: &[(Hash, Bytes)]) -> Result<u64> {
        self.inner.delete_derived_values(kind, rows).await
    }

    async fn ref_log(
        &self,
        name: Option<&str>,
        before_seq: Option<u64>,
        limit: usize,
    ) -> Result<Vec<RefLogEntry>> {
        self.inner.ref_log(name, before_seq, limit).await
    }

    async fn list_refs(&self) -> Result<BTreeMap<String, ChangesetId>> {
        self.inner.list_refs().await
    }

    async fn list_derived(
        &self,
        kind: &str,
        after: Option<Hash>,
        limit: usize,
    ) -> Result<Vec<(Hash, Bytes)>> {
        self.inner.list_derived(kind, after, limit).await
    }

    async fn list_derived_keys(
        &self,
        kind: &str,
        after: Option<Hash>,
        limit: usize,
    ) -> Result<Vec<Hash>> {
        self.inner.list_derived_keys(kind, after, limit).await
    }

    async fn delete_derived_step(
        &self,
        kinds: &[&str],
        cursor: Option<&[u8]>,
    ) -> Result<DeleteStep> {
        self.inner.delete_derived_step(kinds, cursor).await
    }

    async fn delete_derived_kind(&self, kind: &str) -> Result<u64> {
        self.inner.delete_derived_kind(kind).await
    }

    async fn put_derived(&self, kind: &str, key: &Hash, value: Bytes) -> Result<()> {
        self.inner.put_derived(kind, key, value).await
    }

    async fn put_derived_if_absent(&self, kind: &str, key: &Hash, value: Bytes) -> Result<Bytes> {
        self.inner.put_derived_if_absent(kind, key, value).await
    }

    async fn cas_derived(
        &self,
        kind: &str,
        key: &Hash,
        expected: Option<&[u8]>,
        new: Bytes,
    ) -> Result<bool> {
        self.inner.cas_derived(kind, key, expected, new).await
    }

    async fn put_derived_many(&self, rows: &[DerivedRow]) -> Result<()> {
        self.inner.put_derived_many(rows).await
    }

    async fn put_changeset_with_derived(
        &self,
        cs: &Changeset,
        rows: &[DerivedRow],
    ) -> Result<ChangesetId> {
        self.inner.put_changeset_with_derived(cs, rows).await
    }

    async fn put_changeset_with_derived_if_new(
        &self,
        cs: &Changeset,
        rows: &[DerivedRow],
        if_new: &[DerivedRow],
    ) -> Result<bool> {
        self.inner
            .put_changeset_with_derived_if_new(cs, rows, if_new)
            .await
    }

    async fn put_changesets_with_derived(
        &self,
        changesets: &[Changeset],
        rows: &[DerivedRow],
    ) -> Result<()> {
        self.inner
            .put_changesets_with_derived(changesets, rows)
            .await
    }

    fn supports_leases(&self) -> bool {
        self.inner.supports_leases()
    }

    async fn lease(
        &self,
        key: &leep_core::LeaseKey,
        timing: leep_core::LeaseTiming,
    ) -> Result<Option<leep_core::Lease>> {
        self.inner.lease(key, timing).await
    }

    async fn acquire_lease(
        &self,
        claim: &leep_core::LeaseClaim<'_>,
        timing: leep_core::LeaseTiming,
    ) -> Result<leep_core::Lease> {
        self.inner.acquire_lease(claim, timing).await
    }

    async fn renew_leases(
        &self,
        holder: &str,
        held: &[(leep_core::LeaseKey, u64)],
        timing: leep_core::LeaseTiming,
    ) -> Result<Vec<Option<Duration>>> {
        self.inner.renew_leases(holder, held, timing).await
    }

    async fn release_lease(
        &self,
        key: &leep_core::LeaseKey,
        holder: &str,
        epoch: u64,
    ) -> Result<bool> {
        self.inner.release_lease(key, holder, epoch).await
    }

    async fn schema_version(&self) -> Result<Option<u64>> {
        self.inner.schema_version().await
    }

    fn coord_store(&self) -> Option<Arc<dyn leep_core::coord::CoordStore>> {
        self.inner.coord_store()
    }

    fn supports_review_requests(&self) -> bool {
        self.inner.supports_review_requests()
    }

    async fn put_review_request(
        &self,
        req: &leep_core::review_request::NewReviewRequest,
    ) -> Result<leep_core::review_request::ReviewRequest> {
        self.inner.put_review_request(req).await
    }

    async fn get_review_request(
        &self,
        id: u64,
    ) -> Result<Option<leep_core::review_request::ReviewRequest>> {
        self.inner.get_review_request(id).await
    }

    async fn list_review_requests(
        &self,
        filter: &leep_core::review_request::ReviewRequestFilter,
    ) -> Result<Vec<leep_core::review_request::ReviewRequest>> {
        self.inner.list_review_requests(filter).await
    }

    async fn close_review_request(
        &self,
        id: u64,
        state: leep_core::review_request::ReviewRequestState,
        landed_as: Option<ChangesetId>,
        closed: i64,
    ) -> Result<bool> {
        self.inner
            .close_review_request(id, state, landed_as, closed)
            .await
    }
}
