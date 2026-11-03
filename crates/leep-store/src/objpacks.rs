//! The packs an object store blob store reads from (ADR 0040): indexes
//! fetched from `packs/<id>.idx` and kept in memory, tombstones stored as one
//! empty object each at `packs/tombstones/<pack id>/<hash>`, and entries
//! read with ranged GETs.
//!
//! There is no directory to watch. Every publisher writes `packs/.gen` after
//! the index it publishes, so a miss reads that one small object (at most
//! once per [`GEN_WINDOW`] in a process) and lists `packs/` only when its
//! contents changed (ADR 0070). A bucket without it (written only by older servers)
//! is listed on every miss, as ADR 0068 did. The full listing, tombstones
//! included, still runs on a miss at most once per refresh interval (ADR
//! 0040).

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use leep_core::{BlobMeta, Error, Hash, Result};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};

use crate::pack::{self, IdxEntry, PackIndex};
use crate::packset::{
    hidden, merge_listing, order_packs, record_miss, DecodedCache, PackLike, ScanReason, Tombstones,
};
use crate::retire::{encode_retired, parse_retired, Retired};
use crate::storage_err;

pub(crate) const PACKS_PREFIX: &str = "packs";
pub(crate) const TOMBSTONES_PREFIX: &str = "packs/tombstones";
/// Shortest time between two refreshes a miss may trigger. Must stay well
/// under the repack fence (minutes); see the module comment.
pub(crate) const DEFAULT_REFRESH_INTERVAL: Duration = Duration::from_secs(5);
/// Indexes fetched at once when a refresh finds several new packs.
const LOAD_CONCURRENCY: usize = 8;
/// The pack-set generation stamp (ADR 0070): rewritten, last writer wins,
/// by every writer after it publishes or deletes a pack.
pub(crate) const GEN_KEY: &str = "packs/.gen";
/// How old a read of [`GEN_KEY`] a miss may reuse: concurrent misses in one
/// process share one request.
pub(crate) const GEN_WINDOW: Duration = Duration::from_millis(50);

pub(crate) fn gen_key() -> Path {
    Path::from(GEN_KEY)
}

/// The retired packs record of a bucket (ADR 0080): the filesystem's
/// `packs/retired` lines, rewritten whole (buckets have no append) only by
/// a lease holder, and followed by a write of [`GEN_KEY`].
pub(crate) const RETIRED_KEY: &str = "packs/.retired";

/// The bucket's retired record; empty if it has none.
pub(crate) async fn read_retired(store: &dyn ObjectStore) -> Result<Retired> {
    match store.get(&Path::from(RETIRED_KEY)).await {
        Ok(r) => match r.bytes().await {
            Ok(b) => Ok(parse_retired(&b)),
            Err(e) if not_found(&e) => Ok(Retired::new()),
            Err(e) => Err(storage_err(e)),
        },
        Err(e) if not_found(&e) => Ok(Retired::new()),
        Err(e) => Err(storage_err(e)),
    }
}

/// Replace the bucket's retired record with `r` (deleting it when empty).
/// Only under the repack lease; the caller announces it with [`bump_gen`].
pub(crate) async fn write_retired(store: &dyn ObjectStore, r: &Retired) -> Result<()> {
    let key = Path::from(RETIRED_KEY);
    if r.is_empty() {
        return match store.delete(&key).await {
            Ok(()) => Ok(()),
            Err(e) if not_found(&e) => Ok(()),
            Err(e) => Err(storage_err(e)),
        };
    }
    store
        .put(&key, PutPayload::from(encode_retired(r).into_bytes()))
        .await
        .map_err(storage_err)?;
    Ok(())
}

/// What a read of [`GEN_KEY`] saw.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Gen {
    /// No stamp: only older servers wrote this bucket.
    Absent,
    /// The stamp's contents, which every write makes unique. Compared
    /// instead of an etag: some backends derive the etag from metadata
    /// (the local filesystem's is inode, mtime and size), which two writes
    /// can share.
    Body(Bytes),
    /// An empty stamp, which no writer leaves, so changes cannot be seen.
    Unknown,
}

/// Announce a change of the published packs: write [`GEN_KEY`] with
/// contents no other write has (the pack id, this process, a counter and
/// the time). Called after the index PUT or the
/// deletes it announces, before the write returns: a reader that reads the
/// stamp after that sees new contents and lists `packs/`.
pub(crate) async fn bump_gen(store: &dyn ObjectStore, pack_id: &str) -> Result<()> {
    static WRITES: AtomicU64 = AtomicU64::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let body = format!(
        "{pack_id} {} {} {nanos}\n",
        crate::tmpfile::nonce(),
        WRITES.fetch_add(1, Ordering::Relaxed)
    );
    store
        .put(&gen_key(), PutPayload::from(body.into_bytes()))
        .await
        .map_err(storage_err)?;
    Ok(())
}

/// One GET of the stamp: about 60 bytes, so it costs what a HEAD does.
pub(crate) async fn read_gen(store: &dyn ObjectStore) -> Result<Gen> {
    match store.get(&gen_key()).await {
        Ok(r) => {
            let body = match r.bytes().await {
                Ok(b) => b,
                // Replaced or deleted between the response and its body.
                Err(e) if not_found(&e) => return Ok(Gen::Absent),
                Err(e) => return Err(storage_err(e)),
            };
            Ok(if body.is_empty() {
                Gen::Unknown
            } else {
                Gen::Body(body)
            })
        }
        Err(e) if not_found(&e) => Ok(Gen::Absent),
        Err(e) => Err(storage_err(e)),
    }
}

pub(crate) fn idx_key(id: &str) -> Path {
    Path::from(format!("{PACKS_PREFIX}/{id}.idx"))
}

pub(crate) fn pack_key(id: &str) -> Path {
    Path::from(format!("{PACKS_PREFIX}/{id}.pack"))
}

pub(crate) fn tombstone_key(id: &str, hash: &Hash) -> Path {
    Path::from(format!("{TOMBSTONES_PREFIX}/{id}/{hash}"))
}

pub(crate) fn is_pack_id(s: &str) -> bool {
    s.len() == 64 && s.bytes().all(|c| c.is_ascii_hexdigit())
}

/// One published pack: its index, in memory. The bytes stay in the bucket.
pub(crate) struct RemotePack {
    pub id: String,
    pub index: PackIndex,
}

impl PackLike for RemotePack {
    fn id(&self) -> &str {
        &self.id
    }
    fn index(&self) -> &PackIndex {
        &self.index
    }
}

/// The packs and tombstones as of one listing.
pub(crate) struct View {
    /// Searchable packs first, smallest first, then the ones a repack of
    /// this process superseded (see `packset::View`).
    pub packs: Vec<(Arc<RemotePack>, HashSet<Hash>)>,
    searchable: usize,
    /// The packs `packs/.retired` named when this view's listing read it
    /// (ADR 0080), ordered after the searchable ones.
    retired: Arc<HashSet<String>>,
    /// When the listing behind this view began.
    started: Instant,
    /// When the latest listing of `packs/` began, this one's or a later
    /// one that only looked for new packs.
    packs_started: Instant,
    /// The stamp a read that ended before that listing began saw: while a
    /// later read sees the same, no pack was published since.
    gen: Gen,
}

impl View {
    /// The first live packed copy of `hash`, in the packs reads search.
    pub fn locate(&self, hash: &Hash) -> Option<(&Arc<RemotePack>, IdxEntry)> {
        self.packs[..self.searchable].iter().find_map(|(p, dead)| {
            let e = p.index.find(hash)?;
            (!dead.contains(hash)).then_some((p, e))
        })
    }

    /// The packs reads search: not superseded here, not retired.
    pub fn searchable(&self) -> &[(Arc<RemotePack>, HashSet<Hash>)] {
        &self.packs[..self.searchable]
    }

    pub fn pack(&self, id: &str) -> Option<&Arc<RemotePack>> {
        self.packs.iter().map(|(p, _)| p).find(|p| p.id == id)
    }

    pub fn is_empty(&self) -> bool {
        self.packs.is_empty()
    }
}

fn not_found(e: &object_store::Error) -> bool {
    matches!(e, object_store::Error::NotFound { .. })
}

pub(crate) async fn exists(store: &dyn ObjectStore, key: &Path) -> Result<bool> {
    match store.head(key).await {
        Ok(_) => Ok(true),
        Err(e) if not_found(&e) => Ok(false),
        Err(e) => Err(storage_err(e)),
    }
}

/// Every tombstone in the bucket, by pack id.
pub(crate) async fn list_tombstones(store: &dyn ObjectStore) -> Result<Tombstones> {
    let prefix = Path::from(TOMBSTONES_PREFIX);
    let mut out = Tombstones::new();
    let mut objects = store.list(Some(&prefix));
    while let Some(o) = objects.try_next().await.map_err(storage_err)? {
        let parts: Vec<_> = o.location.parts().collect();
        let [_, _, id, hash] = parts.as_slice() else {
            continue;
        };
        let Ok(hash) = hash.as_ref().parse::<Hash>() else {
            continue;
        };
        if is_pack_id(id.as_ref()) {
            out.entry(id.as_ref().to_string()).or_default().insert(hash);
        }
    }
    Ok(out)
}

/// The tombstones naming pack `id`.
pub(crate) async fn list_tombstones_of(store: &dyn ObjectStore, id: &str) -> Result<HashSet<Hash>> {
    let prefix = Path::from(format!("{TOMBSTONES_PREFIX}/{id}"));
    let listed: Vec<_> = store
        .list(Some(&prefix))
        .try_collect()
        .await
        .map_err(storage_err)?;
    Ok(listed
        .iter()
        .filter_map(|o| o.location.filename()?.parse().ok())
        .collect())
}

/// What sits directly under `packs/`: published ids (those with an index,
/// sorted), and every `.pack` with its last-modified time.
pub(crate) async fn list_packs_dir(
    store: &dyn ObjectStore,
) -> Result<(Vec<String>, Vec<(String, i64)>)> {
    let prefix = Path::from(PACKS_PREFIX);
    let listed = match store.list_with_delimiter(Some(&prefix)).await {
        Ok(l) => l,
        Err(e) if not_found(&e) => return Ok(Default::default()),
        Err(e) => return Err(storage_err(e)),
    };
    let mut ids = Vec::new();
    let mut packs = Vec::new();
    for o in listed.objects {
        let Some(name) = o.location.filename() else {
            continue;
        };
        if let Some(id) = name.strip_suffix(".idx").filter(|id| is_pack_id(id)) {
            ids.push(id.to_string());
        } else if let Some(id) = name.strip_suffix(".pack").filter(|id| is_pack_id(id)) {
            packs.push((id.to_string(), o.last_modified.timestamp()));
        }
    }
    ids.sort();
    Ok((ids, packs))
}

/// Fetch and check a published pack's index. `None` if the pack vanished (a
/// repack replaced it) while loading.
async fn load_pack(store: &dyn ObjectStore, id: &str) -> Result<Option<RemotePack>> {
    let idx_bytes = match store.get(&idx_key(id)).await {
        Ok(r) => r.bytes().await.map_err(storage_err)?,
        Err(e) if not_found(&e) => return Ok(None),
        Err(e) => return Err(storage_err(e)),
    };
    if pack::pack_id(&idx_bytes) != id {
        return Err(Error::Storage(format!(
            "corrupt pack index {id}.idx: its hash is not its name"
        )));
    }
    let index = PackIndex::decode(idx_bytes.to_vec())
        .map_err(|e| Error::Storage(format!("pack {id}: {e}")))?;
    let size = match store.head(&pack_key(id)).await {
        Ok(m) => m.size,
        Err(e) if not_found(&e) => return Ok(None),
        Err(e) => return Err(storage_err(e)),
    };
    if size != index.pack_len() {
        return Err(Error::Storage(format!(
            "corrupt pack {id}.pack: {size} bytes, index says {}",
            index.pack_len()
        )));
    }
    Ok(Some(RemotePack {
        id: id.to_string(),
        index,
    }))
}

/// A read of the stamp, kept for misses within [`GEN_WINDOW`] of it.
struct GenSeen {
    started: Instant,
    gen: Gen,
}

pub(crate) struct RemotePackSet {
    store: Arc<dyn ObjectStore>,
    view: RwLock<Option<Arc<View>>>,
    refreshing: tokio::sync::Mutex<()>,
    /// Held while reading the stamp; taken after `refreshing`, never before.
    gen_check: tokio::sync::Mutex<()>,
    last_gen: std::sync::Mutex<Option<GenSeen>>,
    /// Listings of `packs/` so far, for tests.
    lists: AtomicU64,
    decoded: DecodedCache,
    interval: Duration,
    /// Packs a repack of this process merged into a published pack, until
    /// they are deleted (see `packset::PackSet`).
    superseded: std::sync::Mutex<HashSet<String>>,
}

impl RemotePackSet {
    pub fn new(store: Arc<dyn ObjectStore>, decoded_capacity: usize, interval: Duration) -> Self {
        RemotePackSet {
            store,
            view: RwLock::new(None),
            refreshing: tokio::sync::Mutex::new(()),
            gen_check: tokio::sync::Mutex::new(()),
            last_gen: std::sync::Mutex::new(None),
            lists: AtomicU64::new(0),
            decoded: DecodedCache::new(decoded_capacity),
            interval,
            superseded: std::sync::Mutex::default(),
        }
    }

    /// Listings of `packs/` so far, for tests.
    #[cfg(test)]
    pub fn lists(&self) -> u64 {
        self.lists.load(Ordering::Relaxed)
    }

    /// One LIST of `packs/`, counted.
    async fn list_packs(&self, reason: ScanReason) -> Result<Vec<String>> {
        self.lists.fetch_add(1, Ordering::Relaxed);
        reason.record();
        Ok(list_packs_dir(&*self.store).await?.0)
    }

    /// The stamp as seen by a read that began at or after `since`: a recent
    /// one if there is one, else a new one, shared by concurrent callers.
    async fn gen_since(&self, since: Instant) -> Result<Gen> {
        let recent = |seen: &Option<GenSeen>| {
            seen.as_ref()
                .filter(|g| g.started >= since)
                .map(|g| g.gen.clone())
        };
        if let Some(g) = recent(&self.last_gen.lock().unwrap()) {
            return Ok(g);
        }
        let _one = self.gen_check.lock().await;
        if let Some(g) = recent(&self.last_gen.lock().unwrap()) {
            return Ok(g);
        }
        let started = Instant::now();
        let gen = read_gen(&*self.store).await?;
        *self.last_gen.lock().unwrap() = Some(GenSeen {
            started,
            gen: gen.clone(),
        });
        Ok(gen)
    }

    /// A new view of `packs`, ordered for reads, and stored as current.
    fn install(
        &self,
        mut packs: Vec<(Arc<RemotePack>, HashSet<Hash>)>,
        started: Instant,
        packs_started: Instant,
        gen: Gen,
        retired: Arc<HashSet<String>>,
    ) -> Arc<View> {
        let superseded = {
            let mut s = self.superseded.lock().unwrap();
            s.retain(|id| packs.iter().any(|(p, _)| p.id == *id));
            s.clone()
        };
        let searchable = order_packs(&mut packs, &hidden(&superseded, &retired));
        let view = Arc::new(View {
            packs,
            searchable,
            retired,
            started,
            packs_started,
            gen,
        });
        *self.view.write().unwrap() = Some(view.clone());
        metrics::gauge!("leep_blob_packs").set(searchable as f64);
        view
    }

    /// Add a pack this process just published, without listing.
    pub async fn insert_published(&self, pack: RemotePack) {
        let _one = self.refreshing.lock().await;
        let Some(current) = self.current() else {
            return;
        };
        if current.pack(&pack.id).is_some() {
            return;
        }
        let mut packs = current.packs.clone();
        packs.push((Arc::new(pack), HashSet::new()));
        self.install(
            packs,
            current.started,
            current.packs_started,
            current.gen.clone(),
            current.retired.clone(),
        );
    }

    /// Stop searching `ids` (see `packset::PackSet::supersede`).
    pub async fn supersede(&self, ids: &[String]) {
        let _one = self.refreshing.lock().await;
        self.superseded.lock().unwrap().extend(ids.iter().cloned());
        if let Some(current) = self.current() {
            self.install(
                current.packs.clone(),
                current.started,
                current.packs_started,
                current.gen.clone(),
                current.retired.clone(),
            );
        }
    }

    /// A view that holds every pack published before `since`: one LIST
    /// of `packs/` (shared by concurrent callers), loading only the packs
    /// not loaded yet, with their tombstones. What a miss needs now that
    /// any server's `put_many` publishes packs: a changeset naming their
    /// objects can be stored right after, and read through another
    /// server at once (ADR 0068).
    async fn refresh_new_packs(&self, since: Instant, reason: ScanReason) -> Result<Arc<View>> {
        let _one = self.refreshing.lock().await;
        let current = self.current();
        if let Some(v) = current.as_ref().filter(|v| v.packs_started >= since) {
            return Ok(v.clone());
        }
        let Some(current) = current else {
            drop(_one);
            return self.refresh_after(Instant::now(), ScanReason::First).await;
        };
        let gen = self.gen_since(Instant::now()).await?;
        // After the stamp: a retirement is written before the stamp that
        // announces it, so a stamp this view records was not followed by a
        // retirement this read missed.
        let retired = self.retired_now().await?;
        let packs_started = Instant::now();
        let ids = self.list_packs(reason).await?;
        let new: Vec<String> = ids
            .iter()
            .filter(|id| current.pack(id).is_none())
            .cloned()
            .collect();
        let mut packs: Vec<(Arc<RemotePack>, HashSet<Hash>)> = current
            .packs
            .iter()
            .filter(|(p, _)| ids.binary_search(&p.id).is_ok())
            .cloned()
            .collect();
        let loaded = self.open_packs(None, &new).await?;
        for p in loaded {
            let dead = list_tombstones_of(&*self.store, &p.id).await?;
            packs.push((p, dead));
        }
        Ok(self.install(packs, current.started, packs_started, gen, retired))
    }

    /// The view a miss of the current one should search: the periodic full
    /// listing if one is due, then, unless that holds `hash`, a listing of
    /// new packs only if the stamp changed since the view's listing (or
    /// the bucket has no usable stamp).
    async fn view_after_miss(&self, hash: &Hash) -> Result<Arc<View>> {
        let view = self.refresh_for_miss().await?;
        if view.locate(hash).is_some() {
            return Ok(view);
        }
        let asked = Instant::now();
        let gen = self
            .gen_since(asked.checked_sub(GEN_WINDOW).unwrap_or(asked))
            .await?;
        match (&gen, &view.gen) {
            (Gen::Body(now), Gen::Body(then)) if now == then => Ok(view),
            (Gen::Body(_), _) => self.refresh_new_packs(asked, ScanReason::MissChanged).await,
            _ => {
                self.refresh_new_packs(asked, ScanReason::MissUnstamped)
                    .await
            }
        }
    }

    fn current(&self) -> Option<Arc<View>> {
        self.view.read().unwrap().clone()
    }

    /// The current view, listing on first use.
    pub async fn view(&self) -> Result<Arc<View>> {
        match self.current() {
            Some(v) => Ok(v),
            None => {
                let since = Instant::now();
                let _one = self.refreshing.lock().await;
                if let Some(v) = self.current() {
                    return Ok(v);
                }
                drop(_one);
                self.refresh_after(since, ScanReason::First).await
            }
        }
    }

    /// A view listed now: one that began listing after this call.
    pub async fn refresh(&self) -> Result<Arc<View>> {
        self.rescan(ScanReason::Explicit).await
    }

    /// [`refresh`](Self::refresh), counted under `reason`.
    pub async fn rescan(&self, reason: ScanReason) -> Result<Arc<View>> {
        self.refresh_after(Instant::now(), reason).await
    }

    /// A view no older than the refresh interval, for a miss.
    async fn refresh_for_miss(&self) -> Result<Arc<View>> {
        let now = Instant::now();
        self.refresh_after(
            now.checked_sub(self.interval).unwrap_or(now),
            ScanReason::Interval,
        )
        .await
    }

    /// A view whose listing began at or after `since`: the current one if it
    /// qualifies (a concurrent caller may just have listed), else a new one.
    /// Packs already loaded are reused.
    async fn refresh_after(&self, since: Instant, reason: ScanReason) -> Result<Arc<View>> {
        let current = self.current();
        if let Some(v) = current.as_ref().filter(|v| v.started >= since) {
            return Ok(v.clone());
        }
        let _one = self.refreshing.lock().await;
        let current = self.current();
        if let Some(v) = current.as_ref().filter(|v| v.started >= since) {
            return Ok(v.clone());
        }
        let started = Instant::now();
        // Tombstones before the indexes: a repack deletes a replaced index
        // before its tombstones, so this order never pairs a listed pack with
        // a tombstone listing that has forgotten it. The stamp before the
        // listing of `packs/`, which then holds every pack it announces.
        let mut tombstones = list_tombstones(&*self.store).await?;
        let gen = self.gen_since(Instant::now()).await?;
        let retired = self.retired_now().await?;
        let ids = self.list_packs(reason).await?;
        let packs = self.open_packs(current.as_deref(), &ids).await?;
        let packs = packs
            .into_iter()
            .map(|p| {
                let dead = tombstones.remove(&p.id).unwrap_or_default();
                (p, dead)
            })
            .collect();
        Ok(self.install(packs, started, started, gen, retired))
    }

    /// The retired record as one GET reads it now.
    async fn retired_now(&self) -> Result<Arc<HashSet<String>>> {
        Ok(Arc::new(
            read_retired(&*self.store).await?.into_keys().collect(),
        ))
    }

    /// The packs published now, without listing tombstones: what a delete
    /// needs, at one LIST instead of a listing that grows with every
    /// tombstone GC has written.
    async fn packs_now(&self) -> Result<Vec<Arc<RemotePack>>> {
        let (ids, _) = list_packs_dir(&*self.store).await?;
        let current = self.current();
        self.open_packs(current.as_deref(), &ids).await
    }

    /// The packs `ids` names, in that order, reusing indexes already loaded.
    /// Packs that vanished while loading are left out.
    async fn open_packs(
        &self,
        current: Option<&View>,
        ids: &[String],
    ) -> Result<Vec<Arc<RemotePack>>> {
        let mut open: HashMap<String, Arc<RemotePack>> = HashMap::new();
        if let Some(v) = current {
            for (p, _) in &v.packs {
                open.insert(p.id.clone(), p.clone());
            }
        }
        let missing: Vec<String> = ids
            .iter()
            .filter(|id| !open.contains_key(*id))
            .cloned()
            .collect();
        // Owned inputs: borrowing ones trip the `Send` check of the
        // `async_trait` futures this runs in.
        let loaded: Vec<Option<RemotePack>> = futures::stream::iter(missing)
            .map(|id| {
                let store = self.store.clone();
                async move { load_pack(&*store, &id).await }
            })
            .buffered(LOAD_CONCURRENCY)
            .try_collect()
            .await?;
        for p in loaded.into_iter().flatten() {
            open.insert(p.id.clone(), Arc::new(p));
        }
        Ok(ids.iter().filter_map(|id| open.get(id)).cloned().collect())
    }

    /// Bytes of a packed object, `None` if no live packed copy exists in
    /// the view a miss gets ([`view_after_miss`](Self::view_after_miss)).
    pub async fn get(&self, hash: &Hash) -> Result<Option<Bytes>> {
        let mut view = self.view().await?;
        if view.locate(hash).is_none() {
            let started = Instant::now();
            view = self.view_after_miss(hash).await?;
            let found = view.locate(hash).is_some() as usize;
            record_miss(started, found, 1 - found, view.packs_started);
        }
        self.read_located(view, hash).await
    }

    /// [`get`](Self::get), with a `None` that is exact: after a miss of the
    /// view a miss gets, one listing of `packs/` that began after `called`,
    /// which holds every pack published before then.
    pub async fn get_exact(&self, hash: &Hash, called: Instant) -> Result<Option<Bytes>> {
        let mut view = self.view().await?;
        if view.locate(hash).is_none() {
            let started = Instant::now();
            view = self.view_after_miss(hash).await?;
            if view.locate(hash).is_none() {
                view = self
                    .refresh_new_packs(called, ScanReason::MissForced)
                    .await?;
            }
            let found = view.locate(hash).is_some() as usize;
            record_miss(started, found, 1 - found, view.packs_started);
        }
        self.read_located(view, hash).await
    }

    /// Read `hash` from the pack `view` locates it in, listing again if
    /// that pack was deleted.
    async fn read_located(&self, mut view: Arc<View>, hash: &Hash) -> Result<Option<Bytes>> {
        // A pack deleted after its fence (this view predates the repack that
        // replaced it) answers 404; the refresh finds the replacement.
        for _ in 0..3 {
            let Some((p, e)) = view.locate(hash) else {
                return Ok(None);
            };
            if let Some(b) = self.decode(p, &e).await? {
                return Ok(Some(b));
            }
            view = self.refresh().await?;
        }
        Err(Error::Storage(format!(
            "pack holding {hash} is listed but its .pack is missing"
        )))
    }

    /// Exact even across processes: a tombstone or a replaced pack written
    /// since this view was listed is seen, because GC's existence checks
    /// rely on it (ADR 0012).
    pub async fn contains(&self, hash: &Hash) -> Result<bool> {
        let mut view = self.view().await?;
        if view.locate(hash).is_none() {
            let started = Instant::now();
            view = self.view_after_miss(hash).await?;
            let found = view.locate(hash).is_some() as usize;
            record_miss(started, found, 1 - found, view.packs_started);
        }
        for _ in 0..3 {
            let Some((p, _)) = view.locate(hash) else {
                return Ok(false);
            };
            // The tombstone first, then the index. A delete tombstones every
            // pack it lists, so if the pack still exists after its tombstone
            // was found absent, no delete had finished before that look.
            // The other order could miss a delete that tombstoned only the
            // replacement after this pack was removed.
            let dead = exists(&*self.store, &tombstone_key(&p.id, hash)).await?;
            if !dead && exists(&*self.store, &idx_key(&p.id)).await? {
                return Ok(true);
            }
            view = self.refresh().await?;
        }
        Err(Error::Storage(format!(
            "packs holding {hash} keep changing; try again"
        )))
    }

    /// Decode an entry with one batched ranged read of its chain's frames.
    /// `None` if the pack is gone.
    pub async fn decode(&self, p: &RemotePack, e: &IdxEntry) -> Result<Option<Bytes>> {
        let chain = self.decoded.plan(&p.id, &p.index, e)?;
        let ranges: Vec<_> = chain
            .entries
            .iter()
            .map(|e| e.offset..e.offset + e.stored_len)
            .collect();
        let frames = if ranges.is_empty() {
            Vec::new()
        } else {
            // Coalesces nearby ranges into one request and fetches the rest
            // in parallel.
            match self.store.get_ranges(&pack_key(&p.id), &ranges).await {
                Ok(f) => f,
                Err(err) if not_found(&err) => return Ok(None),
                Err(err) => return Err(storage_err(err)),
            }
        };
        let mut by_offset: HashMap<u64, Bytes> =
            chain.entries.iter().map(|e| e.offset).zip(frames).collect();
        self.decoded
            .finish(&p.id, e, chain, |entry| {
                match by_offset.remove(&entry.offset) {
                    Some(f) if f.len() as u64 == entry.stored_len => Ok(f),
                    _ => Err(Error::Storage(format!(
                        "short read of pack {} at {}",
                        p.id, entry.offset
                    ))),
                }
            })
            .map(Some)
    }

    /// The bytes of `hash` in pack `id`, tombstoned or not. Repack uses it to
    /// restore a loose copy it should not have deleted.
    pub async fn read_from(&self, id: &str, hash: &Hash) -> Result<Option<Bytes>> {
        let view = self.view().await?;
        let view = match view.pack(id) {
            Some(_) => view,
            None => self.refresh().await?,
        };
        let Some(p) = view.pack(id) else {
            return Ok(None);
        };
        match p.index.find(hash) {
            Some(e) => self.decode(p, &e).await,
            None => Ok(None),
        }
    }

    /// Hide every packed copy of `hash`. Lists the packs first, so one
    /// another process published is not missed; a copy already tombstoned
    /// is tombstoned again, which is idempotent.
    pub async fn tombstone(&self, hash: &Hash) -> Result<()> {
        let ids: Vec<String> = self
            .packs_now()
            .await?
            .iter()
            .filter(|p| p.index.find(hash).is_some())
            .map(|p| p.id.clone())
            .collect();
        if ids.is_empty() {
            return Ok(());
        }
        self.write_tombstones(ids.iter().map(|id| (id.as_str(), *hash)))
            .await
    }

    /// Write tombstones, then hide them in the cached view too.
    pub async fn write_tombstones<'a>(
        &self,
        items: impl IntoIterator<Item = (&'a str, Hash)>,
    ) -> Result<()> {
        let items: Vec<(String, Hash)> = items
            .into_iter()
            .map(|(id, h)| (id.to_string(), h))
            .collect();
        let keys: Vec<Path> = items.iter().map(|(id, h)| tombstone_key(id, h)).collect();
        futures::stream::iter(keys)
            .map(|key| {
                let store = self.store.clone();
                async move {
                    store
                        .put(&key, PutPayload::default())
                        .await
                        .map_err(storage_err)
                }
            })
            .buffer_unordered(16)
            .try_collect::<Vec<_>>()
            .await?;
        let mut slot = self.view.write().unwrap();
        if let Some(v) = slot.as_ref() {
            let mut packs = v.packs.clone();
            for (p, dead) in &mut packs {
                dead.extend(items.iter().filter(|(id, _)| *id == p.id).map(|(_, h)| *h));
            }
            *slot = Some(Arc::new(View {
                packs,
                searchable: v.searchable,
                retired: v.retired.clone(),
                started: v.started,
                packs_started: v.packs_started,
                gen: v.gen.clone(),
            }));
        }
        Ok(())
    }

    /// Merge a page of loose objects with the packed ones. The view may be
    /// up to a refresh interval old: a repack deletes loose copies only a
    /// fence after it publishes, so any pack replacing a copy missing from
    /// the loose page is in it. GC lists page by page, and listing every
    /// tombstone per page would make a full listing quadratic.
    pub async fn merge_page(
        &self,
        loose: Vec<BlobMeta>,
        after: Option<Hash>,
        limit: usize,
    ) -> Result<Vec<BlobMeta>> {
        let view = self.refresh_for_miss().await?;
        if view.is_empty() {
            return Ok(loose);
        }
        let packs = view.packs.iter().map(|(p, dead)| (&p.index, dead));
        Ok(merge_listing(packs, loose, after, limit))
    }
}
