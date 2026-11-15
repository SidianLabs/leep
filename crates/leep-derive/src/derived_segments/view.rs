//! The read path of derived segments (ADR 0104, section "Read path"): a
//! [`MetaStore`] that answers `skiplist` and `history-tree-v4` reads the
//! database misses from the live segments, installed by `Repo::new` so
//! every read site gets it unchanged.
//!
//! Order of a read: the database, exactly the statement issued before;
//! then, for misses, the in-memory key indexes of the live segments, newest
//! first. Before a miss is declared, a manifest older than [`REFRESH`] is
//! read again: one statement a minute per process at most, and only when a
//! row is missing (in mode `build` rows are all there, so never); a
//! `get_derived_kinds` of the two kinds carries it as one more read of the
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

use super::codec::{
    self, Block, Loc, Manifest, SegmentData, SegmentIndex, SegmentRef, FAMILY_CHANGESET,
};
use super::{manifest_key, MANIFEST_KIND};
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
}

impl ManifestRead {
    pub fn of(raw: Option<&[u8]>) -> ManifestRead {
        match raw {
            None => ManifestRead::Absent,
            Some(v) => match Manifest::decode(v) {
                Ok(m) if m.family == FAMILY_CHANGESET => ManifestRead::Read(m),
                _ => ManifestRead::Damaged,
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
    /// Live segments whose index could be read, newest first.
    segs: Vec<Arc<Live>>,
    /// Live segments named by `raw` whose index or data cannot be read:
    /// left out until the manifest changes.
    broken: HashSet<Hash>,
}

struct Caches {
    blocks: BlockCache,
    datas: DataCache,
}

/// One handle's view of the live segments: their indexes in memory, and
/// caches of what was read from them. Shared by a `Repo` and its clones.
pub struct SegmentView {
    blobs: Arc<dyn BlobStore>,
    state: RwLock<State>,
    /// Held while a manifest is applied, so two readers do not load the
    /// same index blobs at once.
    apply: tokio::sync::Mutex<()>,
    caches: RwLock<Arc<Caches>>,
    /// Data blob loads in flight, by hash, so concurrent misses read one.
    loading: Mutex<BTreeMap<Hash, Arc<tokio::sync::Mutex<()>>>>,
    refresh: Duration,
}

impl SegmentView {
    pub fn new(blobs: Arc<dyn BlobStore>) -> Self {
        SegmentView {
            blobs,
            state: RwLock::default(),
            apply: tokio::sync::Mutex::new(()),
            caches: RwLock::new(Arc::new(Caches {
                blocks: SizedCache::default(),
                datas: SizedCache::default(),
            })),
            loading: Mutex::default(),
            refresh: REFRESH,
        }
    }

    /// Caches scaled like the rest of a `Repo`'s (see `with_cache_budget`).
    pub(crate) fn scale(&self, s: CacheScale) {
        *self.caches.write().unwrap() = Arc::new(Caches {
            blocks: SizedCache::scaled(s),
            datas: SizedCache::scaled(s),
        });
    }

    fn caches(&self) -> Arc<Caches> {
        self.caches.read().unwrap().clone()
    }

    /// Whether the next read of the two kinds should bring the manifest.
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
    /// segment not loaded yet. A segment whose index is not stored or is
    /// damaged is left out (its keys read as missing, so they derive
    /// again); any other read error is returned and the view keeps what it
    /// had, so a transient error never turns into a mass of derivations.
    pub async fn apply(&self, raw: Option<Bytes>) -> Result<()> {
        let _held = self.apply.lock().await;
        {
            let mut s = self.state.write().unwrap();
            if s.loaded.is_some() && s.raw == raw {
                s.loaded = Some(tokio::time::Instant::now());
                return Ok(());
            }
        }
        let read = ManifestRead::of(raw.as_deref());
        let (live, damaged) = match &read {
            ManifestRead::Absent => (Vec::new(), false),
            ManifestRead::Read(m) => (m.live.clone(), false),
            ManifestRead::Damaged => {
                metrics::counter!("leep_derived_segment_errors_total", "what" => "manifest")
                    .increment(1);
                tracing::error!(
                    "the {MANIFEST_KIND} row does not decode: derived segments are not read \
                     (rows still are); see \"Derived segments\" in RUNBOOKS.md"
                );
                (Vec::new(), true)
            }
        };
        let (have, mut broken) = {
            let s = self.state.read().unwrap();
            let have: BTreeMap<Hash, Arc<Live>> =
                s.segs.iter().map(|l| (l.r.index, l.clone())).collect();
            (have, s.broken.clone())
        };
        broken.retain(|h| live.iter().any(|s| s.index == *h));
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
            if broken.contains(&r.index) {
                continue;
            }
            match load_index(&*self.blobs, r).await? {
                Some(idx) => segs.push(Arc::new(Live {
                    r: *r,
                    idx: Arc::new(idx),
                })),
                None => {
                    broken.insert(r.index);
                }
            }
        }
        let mut s = self.state.write().unwrap();
        s.raw = raw;
        s.loaded = Some(tokio::time::Instant::now());
        s.damaged = damaged;
        s.segs = segs;
        s.broken = broken;
        Ok(())
    }

    /// Stop using a live segment whose data cannot be read, until the
    /// manifest changes.
    fn mark_broken(&self, index: Hash) {
        let mut s = self.state.write().unwrap();
        s.segs.retain(|l| l.r.index != index);
        s.broken.insert(index);
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
                metrics::counter!("leep_derived_segment_reads_total", "outcome" => "hit")
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
        metrics::counter!("leep_derived_segment_errors_total", "what" => "segment").increment(1);
        tracing::warn!(segment = %live.r.index, "derived segment does not decode: {e}");
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
        let _one = gate.lock().await;
        let out = async {
            if let Some(d) = caches.datas.get(&live.r.data) {
                return Ok(Some(d));
            }
            let bytes = match self.blobs.get(&live.r.data).await {
                Ok(Some(b)) => b,
                Ok(None) => {
                    metrics::counter!("leep_derived_segment_errors_total", "what" => "missing")
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
        .await;
        self.loading.lock().unwrap().remove(&live.r.data);
        out
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

/// Read and decode `r`'s index blob. `None` when it is not stored, is
/// damaged, or names another data blob.
pub(crate) async fn load_index(
    blobs: &dyn BlobStore,
    r: &SegmentRef,
) -> Result<Option<SegmentIndex>> {
    let what = match blobs.get(&r.index).await {
        Ok(Some(bytes)) => match codec::decode_index(&r.index, &bytes) {
            Ok(idx) if idx.data == r.data && idx.len() == r.rows as usize => return Ok(Some(idx)),
            Ok(_) => "names another data blob or row count than its manifest entry".to_string(),
            Err(e) => e.to_string(),
        },
        Ok(None) => "is not stored".to_string(),
        Err(e) if crate::gc_segments::segment_reads_corrupt(&e) => e.to_string(),
        Err(e) => return Err(e),
    };
    metrics::counter!("leep_derived_segment_errors_total", "what" => "index").increment(1);
    tracing::warn!(
        segment = %r.index,
        "derived segment index {what}; its keys read as rows or derive again"
    );
    Ok(None)
}

/// The `MetaStore` a `Repo` reads through. See the module comment.
pub(crate) struct SegmentedMeta {
    inner: Arc<dyn MetaStore>,
    view: Arc<SegmentView>,
}

impl SegmentedMeta {
    pub(crate) fn new(inner: Arc<dyn MetaStore>, view: Arc<SegmentView>) -> Self {
        SegmentedMeta { inner, view }
    }

    /// Read the manifest again now and use it.
    async fn refresh(&self) -> Result<()> {
        let raw = self
            .inner
            .get_derived(MANIFEST_KIND, &manifest_key())
            .await?;
        self.view.apply(raw).await
    }

    /// Fill the misses in `rows` (values of `keys` of column `col`) from the
    /// live segments. A segment that cannot be read makes the view read the
    /// manifest again, once, and look again.
    async fn fill(&self, col: u8, keys: &[Hash], rows: &mut [Option<Bytes>]) -> Result<()> {
        if rows.iter().all(Option::is_some) {
            return Ok(());
        }
        // A miss is judged against a manifest at most `REFRESH` old.
        if self.view.stale() {
            self.refresh().await?;
        }
        if self.view.state.read().unwrap().segs.is_empty() {
            return Ok(());
        }
        let mut retried = false;
        loop {
            let mut gone = false;
            for (k, row) in keys.iter().zip(rows.iter_mut()) {
                if row.is_some() {
                    continue;
                }
                match self.view.lookup(k, col).await? {
                    Found::Yes(v) => *row = Some(v),
                    Found::No => {}
                    Found::Gone(index) => {
                        gone = true;
                        if retried {
                            self.view.mark_broken(index);
                        }
                    }
                }
            }
            if !gone || retried {
                return Ok(());
            }
            retried = true;
            self.refresh().await?;
        }
    }

    /// The reads with the manifest added when it is due, as one call.
    async fn read_kinds(&self, reads: &[(&str, &[Hash])]) -> Result<Vec<Vec<Option<Bytes>>>> {
        let wants = reads
            .iter()
            .any(|(k, keys)| codec::kind_column(k).is_some() && !keys.is_empty());
        if !wants || !self.view.stale() {
            return self.inner.get_derived_kinds(reads).await;
        }
        let mk = [manifest_key()];
        let mut with: Vec<(&str, &[Hash])> = reads.to_vec();
        with.push((MANIFEST_KIND, &mk));
        let mut got = self.inner.get_derived_kinds(&with).await?;
        let manifest = got
            .pop()
            .and_then(|mut v| v.pop())
            .ok_or_else(|| Error::Storage("get_derived_kinds answered short".into()))?;
        self.view.apply(manifest).await?;
        Ok(got)
    }
}

#[async_trait]
impl MetaStore for SegmentedMeta {
    fn view_base(&self) -> Option<Arc<dyn MetaStore>> {
        Some(self.inner.clone())
    }

    async fn get_derived(&self, kind: &str, key: &Hash) -> Result<Option<Bytes>> {
        let Some(col) = codec::kind_column(kind) else {
            return self.inner.get_derived(kind, key).await;
        };
        let mut row = self.inner.get_derived(kind, key).await?;
        if row.is_none() {
            let mut rows = [None];
            self.fill(col, &[*key], &mut rows).await?;
            row = rows[0].take();
        }
        Ok(row)
    }

    async fn get_derived_many(&self, kind: &str, keys: &[Hash]) -> Result<Vec<Option<Bytes>>> {
        let Some(col) = codec::kind_column(kind) else {
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
        self.fill(col, keys, &mut rows).await?;
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
            if let Some(col) = codec::kind_column(kind) {
                if rows.len() != keys.len() {
                    return Err(Error::Storage("get_derived_kinds answered short".into()));
                }
                self.fill(col, keys, rows).await?;
            }
        }
        Ok(got)
    }

    async fn derived_present(&self, kind: &str, keys: &[Hash]) -> Result<Vec<bool>> {
        let mut have = self.inner.derived_present(kind, keys).await?;
        if let Some(col) = codec::kind_column(kind) {
            if have.iter().any(|h| !h) {
                if self.view.stale() {
                    self.refresh().await?;
                }
                for (k, h) in keys.iter().zip(have.iter_mut()) {
                    *h = *h || self.view.holds(k, col);
                }
            }
        }
        Ok(have)
    }

    /// Rows and segment values together, each key once.
    async fn count_derived(&self, kind: &str) -> Result<u64> {
        let Some(col) = codec::kind_column(kind) else {
            return self.inner.count_derived(kind).await;
        };
        self.refresh().await?;
        let sealed = self.view.keys_with(col);
        if sealed.is_empty() {
            return self.inner.count_derived(kind).await;
        }
        let mut n = sealed.len() as u64;
        let mut after = None;
        loop {
            let page = self.inner.list_derived_keys(kind, after, 10_000).await?;
            let Some(last) = page.last().copied() else {
                return Ok(n);
            };
            after = Some(last);
            n += page.iter().filter(|k| !sealed.contains(k)).count() as u64;
        }
    }

    /// The larger of the row estimate and the keys segments hold: rows and
    /// segments overlap until `retire` deletes the rows.
    async fn estimate_derived(&self, kind: &str) -> Result<leep_core::RowCount> {
        let mut est = self.inner.estimate_derived(kind).await?;
        if let Some(col) = codec::kind_column(kind) {
            if self.view.stale() {
                self.refresh().await?;
            }
            let sealed = self.view.keys_with(col).len() as u64;
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
