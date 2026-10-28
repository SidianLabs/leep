//! The packs an object store blob store reads from (ADR 0040): indexes
//! fetched from `packs/<id>.idx` and kept in memory, tombstones stored as one
//! empty object each at `packs/tombstones/<pack id>/<hash>`, and entries
//! read with ranged GETs.
//!
//! There is no directory to watch, so the cached view is refreshed on a miss
//! at most once per refresh interval. That is enough because a repack keeps
//! the loose copies and replaced packs for a fence after it publishes, and
//! the interval is far shorter than any fence: a miss after the fence comes
//! more than an interval after the publish, so the refresh it allows (or one
//! that started later) lists the new pack.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use leep_core::{BlobMeta, Error, Hash, Result};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};

use crate::pack::{self, IdxEntry, PackIndex};
use crate::packset::{merge_listing, DecodedCache, Tombstones};
use crate::storage_err;

pub(crate) const PACKS_PREFIX: &str = "packs";
pub(crate) const TOMBSTONES_PREFIX: &str = "packs/tombstones";
/// Shortest time between two refreshes a miss may trigger. Must stay well
/// under the repack fence (minutes); see the module comment.
pub(crate) const DEFAULT_REFRESH_INTERVAL: Duration = Duration::from_secs(5);
/// Indexes fetched at once when a refresh finds several new packs.
const LOAD_CONCURRENCY: usize = 8;

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

/// The packs and tombstones as of one listing.
pub(crate) struct View {
    pub packs: Vec<(Arc<RemotePack>, HashSet<Hash>)>,
    /// When the listing behind this view began.
    started: Instant,
}

impl View {
    /// The first live packed copy of `hash`.
    pub fn locate(&self, hash: &Hash) -> Option<(&Arc<RemotePack>, IdxEntry)> {
        self.packs.iter().find_map(|(p, dead)| {
            let e = p.index.find(hash)?;
            (!dead.contains(hash)).then_some((p, e))
        })
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

pub(crate) struct RemotePackSet {
    store: Arc<dyn ObjectStore>,
    view: RwLock<Option<Arc<View>>>,
    refreshing: tokio::sync::Mutex<()>,
    decoded: DecodedCache,
    interval: Duration,
}

impl RemotePackSet {
    pub fn new(store: Arc<dyn ObjectStore>, decoded_capacity: usize, interval: Duration) -> Self {
        RemotePackSet {
            store,
            view: RwLock::new(None),
            refreshing: tokio::sync::Mutex::new(()),
            decoded: DecodedCache::new(decoded_capacity),
            interval,
        }
    }

    fn current(&self) -> Option<Arc<View>> {
        self.view.read().unwrap().clone()
    }

    /// The current view, listing on first use.
    pub async fn view(&self) -> Result<Arc<View>> {
        match self.current() {
            Some(v) => Ok(v),
            None => self.refresh().await,
        }
    }

    /// A view listed now: one that began listing after this call.
    pub async fn refresh(&self) -> Result<Arc<View>> {
        self.refresh_after(Instant::now()).await
    }

    /// A view no older than the refresh interval, for a miss.
    async fn refresh_for_miss(&self) -> Result<Arc<View>> {
        let now = Instant::now();
        self.refresh_after(now.checked_sub(self.interval).unwrap_or(now))
            .await
    }

    /// A view whose listing began at or after `since`: the current one if it
    /// qualifies (a concurrent caller may just have listed), else a new one.
    /// Packs already loaded are reused.
    async fn refresh_after(&self, since: Instant) -> Result<Arc<View>> {
        let _one = self.refreshing.lock().await;
        let current = self.current();
        if let Some(v) = current.as_ref().filter(|v| v.started >= since) {
            return Ok(v.clone());
        }
        let started = Instant::now();
        // Tombstones before the indexes: a repack deletes a replaced index
        // before its tombstones, so this order never pairs a listed pack with
        // a tombstone listing that has forgotten it.
        let mut tombstones = list_tombstones(&*self.store).await?;
        let (ids, _) = list_packs_dir(&*self.store).await?;
        let packs = self.open_packs(current.as_deref(), &ids).await?;
        let packs = packs
            .into_iter()
            .map(|p| {
                let dead = tombstones.remove(&p.id).unwrap_or_default();
                (p, dead)
            })
            .collect();
        let view = Arc::new(View { packs, started });
        *self.view.write().unwrap() = Some(view.clone());
        Ok(view)
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

    /// Bytes of a packed object, `None` if no live packed copy exists.
    pub async fn get(&self, hash: &Hash) -> Result<Option<Bytes>> {
        let mut view = self.view().await?;
        if view.locate(hash).is_none() {
            view = self.refresh_for_miss().await?;
        }
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
            view = self.refresh_for_miss().await?;
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
                started: v.started,
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
