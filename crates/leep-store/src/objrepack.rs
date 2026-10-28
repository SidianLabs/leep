//! Full repack of an object store blob store (ADR 0040): the filesystem
//! store's repack (ADR 0029) with the pack written to a local temp file and
//! uploaded, the index uploaded after it to publish, and a lease object in
//! place of the repack lock.
//!
//! The hook steps mean here: `PackSynced` the local pack file is written,
//! `PackRenamed` the pack is uploaded (no index names it yet), `IndexSynced`
//! the index is encoded and about to be uploaded, `Published` it is uploaded,
//! `RemovingLoose(n)` the n-th loose copy is about to be checked,
//! `LooseMovedAside(n)` a batch of loose copies ending at the n-th was
//! deleted and the tombstones are about to be checked again, and
//! `TombstonesRewritten` tombstones of deleted packs are gone.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::BufWriter;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use leep_core::{Error, Hash, Result};
use object_store::path::Path;
use object_store::{
    ObjectStore, ObjectStoreExt, PutMode, PutPayload, PutResult, UpdateVersion, WriteMultipart,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::task::JoinHandle;

use crate::fs::unix_secs;
use crate::objpacks::{
    idx_key, list_packs_dir, list_tombstones, pack_key, tombstone_key, TOMBSTONES_PREFIX,
};
use crate::pack::{self, IdxEntry, PackIndex};
use crate::packset::{self, PackSet, DEFAULT_DECODED_CACHE};
use crate::packwrite::{hint_ids, write_order, write_pack, ObjectTable, PackSource, SourceRead};
use crate::repack::step;
use crate::{storage_err, ObjectBlobStore, RepackOptions, RepackReport, RepackStep};
use leep_core::intern::PackHints;

const LEASE_KEY: &str = "packs/repack.lease";
/// How long a lease outlives its holder's last renewal. Only matters when a
/// repack dies: the next one waits this long, or an operator deletes the
/// lease object.
const LEASE_TTL: Duration = Duration::from_secs(600);
const LEASE_RENEW_EVERY: Duration = Duration::from_secs(120);
/// A `.pack` with no index younger than this may be an upload of a repack
/// that lost its lease and has not noticed yet, so it is left for later.
const GARBAGE_AGE: Duration = if cfg!(test) {
    Duration::ZERO
} else {
    LEASE_TTL
};
/// Loose copies deleted per request (S3's bulk delete takes up to 1000).
/// Small in tests so the fault sweep can stop between batches.
const DELETE_BATCH: usize = if cfg!(test) { 4 } else { 1000 };
/// Loose objects fetched ahead of the writer.
const PREFETCH: usize = 32;
const UPLOAD_CHUNK: usize = 16 << 20;
const UPLOAD_CONCURRENCY: usize = 4;

static WORK_COUNTER: AtomicU64 = AtomicU64::new(0);

/// What identifies one loose copy: a re-upload changes `modified` (and on
/// most backends the etag), so cleanup deletes only the copy it packed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct LooseStamp {
    size: u64,
    modified_ms: i64,
    e_tag: Option<String>,
}

impl LooseStamp {
    fn secs(&self) -> i64 {
        self.modified_ms.div_euclid(1000)
    }
}

/// Every loose object, in one paginated listing of `blobs/`.
pub(crate) async fn list_all_loose(store: &dyn ObjectStore) -> Result<BTreeMap<Hash, LooseStamp>> {
    let prefix = Path::from("blobs");
    let mut out = BTreeMap::new();
    let mut objects = store.list(Some(&prefix));
    while let Some(o) = objects.try_next().await.map_err(storage_err)? {
        let parts: Vec<_> = o.location.parts().collect();
        let [_, shard, rest] = parts.as_slice() else {
            continue;
        };
        if shard.as_ref().len() != 2 {
            continue;
        }
        let Ok(hash) = format!("{}{}", shard.as_ref(), rest.as_ref()).parse::<Hash>() else {
            continue;
        };
        out.insert(
            hash,
            LooseStamp {
                size: o.size,
                modified_ms: o.last_modified.timestamp_millis(),
                e_tag: o.e_tag,
            },
        );
    }
    Ok(out)
}

/// Delete many keys with the backend's bulk delete. Absent keys are fine.
async fn delete_all(store: &dyn ObjectStore, keys: Vec<Path>) -> Result<()> {
    if keys.is_empty() {
        return Ok(());
    }
    let input = futures::stream::iter(keys.into_iter().map(Ok)).boxed();
    let mut results = store.delete_stream(input);
    while let Some(r) = results.next().await {
        match r {
            Ok(_) | Err(object_store::Error::NotFound { .. }) => {}
            Err(e) => return Err(storage_err(e)),
        }
    }
    Ok(())
}

fn now_secs() -> i64 {
    unix_secs(SystemTime::now()).unwrap_or(0)
}

// ---- The lease ----

fn lease_token() -> String {
    let mut h = blake3::Hasher::new();
    h.update(&std::process::id().to_be_bytes());
    let nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    h.update(&nanos.to_be_bytes());
    h.update(&WORK_COUNTER.fetch_add(1, Ordering::Relaxed).to_be_bytes());
    let host = std::env::var("HOSTNAME").unwrap_or_default();
    format!(
        "{host}/{}/{}",
        std::process::id(),
        &h.finalize().to_hex()[..16]
    )
}

fn lease_body(token: &str) -> PutPayload {
    let expires = now_secs() + LEASE_TTL.as_secs() as i64;
    PutPayload::from(format!(
        "leep repack lease\nholder {token}\nexpires {expires}\n"
    ))
}

/// Holder and expiry (unix seconds) of a lease object's body.
fn parse_lease(body: &[u8]) -> (String, Option<i64>) {
    let text = String::from_utf8_lossy(body);
    let field = |name: &str| {
        text.lines()
            .find_map(|l| l.strip_prefix(name).map(|v| v.trim().to_string()))
    };
    let holder = field("holder ").unwrap_or_else(|| "?".into());
    (holder, field("expires ").and_then(|t| t.parse().ok()))
}

fn version_of(r: PutResult) -> UpdateVersion {
    UpdateVersion {
        e_tag: r.e_tag,
        version: r.version,
    }
}

fn lost_lease() -> Error {
    Error::Conflict(
        "this repack lost its lease (packs/repack.lease) to another; \
         stopping before it deletes anything"
            .into(),
    )
}

struct LeaseInner {
    store: Arc<dyn ObjectStore>,
    token: String,
    version: Mutex<UpdateVersion>,
    lost: AtomicBool,
    released: AtomicBool,
}

/// `packs/repack.lease`: created only if absent, renewed only if unchanged
/// since this holder last wrote it, so two repacks never both hold it. A
/// repack checks it before each step that deletes.
struct Lease {
    inner: Arc<LeaseInner>,
    renewer: JoinHandle<()>,
}

impl Lease {
    async fn acquire(store: Arc<dyn ObjectStore>) -> Result<Lease> {
        let token = lease_token();
        let key = Path::from(LEASE_KEY);
        for _ in 0..3 {
            match store
                .put_opts(&key, lease_body(&token), PutMode::Create.into())
                .await
            {
                Ok(r) => return Ok(Lease::start(store, token, version_of(r))),
                Err(object_store::Error::AlreadyExists { .. }) => {}
                Err(object_store::Error::NotImplemented { .. }) => {
                    return Err(Error::Storage(
                        "this object store does not support conditional writes \
                         (If-None-Match), which the repack lease needs"
                            .into(),
                    ))
                }
                Err(e) => return Err(storage_err(e)),
            }
            let held = match store.get(&key).await {
                Ok(r) => r,
                Err(object_store::Error::NotFound { .. }) => continue,
                Err(e) => return Err(storage_err(e)),
            };
            let seen = UpdateVersion {
                e_tag: held.meta.e_tag.clone(),
                version: held.meta.version.clone(),
            };
            let (holder, expires) = parse_lease(&held.bytes().await.map_err(storage_err)?);
            if let Some(t) = expires.filter(|t| *t > now_secs()) {
                return Err(Error::Conflict(format!(
                    "another repack is running on this blob store (lease {LEASE_KEY} \
                     held by {holder} until unix time {t})"
                )));
            }
            tracing::warn!(%holder, "taking over an expired repack lease");
            match store
                .put_opts(&key, lease_body(&token), PutMode::Update(seen).into())
                .await
            {
                Ok(r) => return Ok(Lease::start(store, token, version_of(r))),
                Err(object_store::Error::Precondition { .. })
                | Err(object_store::Error::NotFound { .. }) => {
                    return Err(Error::Conflict(
                        "another repack took over the expired lease first".into(),
                    ))
                }
                // Backends without conditional updates (the local filesystem
                // one): delete the expired lease and race to create it again.
                Err(object_store::Error::NotImplemented { .. }) => {
                    delete_all(&*store, vec![key.clone()]).await?;
                }
                Err(e) => return Err(storage_err(e)),
            }
        }
        Err(Error::Conflict(
            "could not take the repack lease; try again".into(),
        ))
    }

    fn start(store: Arc<dyn ObjectStore>, token: String, version: UpdateVersion) -> Lease {
        let inner = Arc::new(LeaseInner {
            store,
            token,
            version: Mutex::new(version),
            lost: AtomicBool::new(false),
            released: AtomicBool::new(false),
        });
        let bg = inner.clone();
        let renewer = tokio::spawn(async move {
            loop {
                tokio::time::sleep(LEASE_RENEW_EVERY).await;
                match bg.renew().await {
                    Ok(()) => {}
                    Err(Error::Conflict(_)) => return,
                    Err(e) => tracing::warn!(error = %e, "renewing the repack lease failed"),
                }
            }
        });
        Lease { inner, renewer }
    }

    /// Renew now; an error if another repack holds the lease.
    async fn check(&self) -> Result<()> {
        self.inner.renew().await
    }

    async fn release(&self) {
        self.renewer.abort();
        if let Err(e) = self.inner.release().await {
            tracing::warn!(error = %e, "releasing the repack lease failed; it expires on its own");
        }
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        self.renewer.abort();
        if !self.inner.released.load(Ordering::SeqCst) {
            if let Ok(rt) = tokio::runtime::Handle::try_current() {
                let inner = self.inner.clone();
                rt.spawn(async move { inner.release().await });
            }
        }
    }
}

impl LeaseInner {
    async fn renew(&self) -> Result<()> {
        if self.lost.load(Ordering::SeqCst) {
            return Err(lost_lease());
        }
        let key = Path::from(LEASE_KEY);
        let seen = self.version.lock().unwrap().clone();
        let res = self
            .store
            .put_opts(&key, lease_body(&self.token), PutMode::Update(seen).into())
            .await;
        let r = match res {
            Ok(r) => r,
            Err(object_store::Error::Precondition { .. })
            | Err(object_store::Error::NotFound { .. }) => {
                self.lost.store(true, Ordering::SeqCst);
                return Err(lost_lease());
            }
            // Without conditional updates: overwrite only if still ours.
            Err(object_store::Error::NotImplemented { .. }) => {
                if !self.holds().await? {
                    self.lost.store(true, Ordering::SeqCst);
                    return Err(lost_lease());
                }
                self.store
                    .put(&key, lease_body(&self.token))
                    .await
                    .map_err(storage_err)?
            }
            Err(e) => return Err(storage_err(e)),
        };
        *self.version.lock().unwrap() = version_of(r);
        Ok(())
    }

    async fn holds(&self) -> Result<bool> {
        match self.store.get(&Path::from(LEASE_KEY)).await {
            Ok(r) => {
                let body = r.bytes().await.map_err(storage_err)?;
                Ok(parse_lease(&body).0 == self.token)
            }
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(storage_err(e)),
        }
    }

    async fn release(&self) -> Result<()> {
        if self.released.swap(true, Ordering::SeqCst) || self.lost.load(Ordering::SeqCst) {
            return Ok(());
        }
        if self.holds().await? {
            delete_all(&*self.store, vec![Path::from(LEASE_KEY)]).await?;
        }
        Ok(())
    }
}

// ---- Reading sources ----

/// A local directory holding copies of the packs being replaced (laid out
/// like a filesystem store, so `PackSet` reads them) and the new pack file.
struct WorkDir(PathBuf);

impl WorkDir {
    fn new() -> Result<WorkDir> {
        let dir = std::env::temp_dir().join(format!(
            "leep-repack-{}-{}-{}",
            std::process::id(),
            WORK_COUNTER.fetch_add(1, Ordering::Relaxed),
            now_secs()
        ));
        std::fs::create_dir_all(dir.join(packset::PACKS_DIR)).map_err(storage_err)?;
        Ok(WorkDir(dir))
    }
}

impl Drop for WorkDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Where a repack reads an object from. A loose copy is preferred: it is
/// live even where the packed copy is tombstoned.
#[derive(Default)]
struct Source {
    loose: Option<LooseStamp>,
    packed: Option<(String, IdxEntry)>,
}

type Fetch = JoinHandle<Result<Option<Bytes>>>;

/// Reads for the pack writer, which runs on a blocking thread: loose objects
/// by GET, fetched ahead in write order, and packed ones from local copies.
struct ObjectSource<'a> {
    rt: tokio::runtime::Handle,
    store: Arc<dyn ObjectStore>,
    /// By id in the repack's [`ObjectTable`].
    sources: &'a [Source],
    local: PackSet,
    local_view: Arc<packset::View>,
    /// Objects with a loose copy, in write order.
    ahead: Vec<Hash>,
    cursor: usize,
    inflight: HashMap<Hash, Fetch>,
}

impl ObjectSource<'_> {
    fn spawn_get(&self, hash: Hash) -> Fetch {
        let store = self.store.clone();
        self.rt.spawn(async move {
            match store.get(&ObjectBlobStore::key(&hash)).await {
                Ok(r) => r.bytes().await.map(Some).map_err(storage_err),
                Err(object_store::Error::NotFound { .. }) => Ok(None),
                Err(e) => Err(storage_err(e)),
            }
        })
    }

    fn fetch(&mut self, hash: &Hash) -> Result<Option<Bytes>> {
        while self.inflight.len() < PREFETCH && self.cursor < self.ahead.len() {
            let h = self.ahead[self.cursor];
            self.cursor += 1;
            if !self.inflight.contains_key(&h) {
                let t = self.spawn_get(h);
                self.inflight.insert(h, t);
            }
        }
        let task = match self.inflight.remove(hash) {
            Some(t) => t,
            None => self.spawn_get(*hash),
        };
        self.rt
            .block_on(task)
            .map_err(|e| Error::Storage(format!("repack fetch failed: {e}")))?
    }
}

impl Drop for ObjectSource<'_> {
    fn drop(&mut self) {
        for t in self.inflight.values() {
            t.abort();
        }
    }
}

impl PackSource for ObjectSource<'_> {
    type Stamp = LooseStamp;

    fn read(&mut self, id: u32, hash: &Hash) -> Result<SourceRead<LooseStamp>> {
        let src = &self.sources[id as usize];
        let mut corrupt = false;
        if let Some(stamp) = &src.loose {
            match self.fetch(hash)? {
                Some(data) if pack::object_intact(hash, &data) => {
                    return Ok(SourceRead::Ok {
                        data,
                        modified: stamp.secs(),
                        from_loose: Some(stamp.clone()),
                    })
                }
                Some(_) => corrupt = true,
                None => {}
            }
        }
        if let Some((id, e)) = &src.packed {
            let Some((p, _)) = self.local_view.packs.iter().find(|(p, _)| p.id == *id) else {
                return Err(Error::Storage(format!("pack {id} was not downloaded")));
            };
            let data = self.local.decode(p, e)?;
            return Ok(SourceRead::Ok {
                data,
                modified: e.modified,
                from_loose: None,
            });
        }
        Ok(if corrupt {
            SourceRead::Corrupt
        } else {
            SourceRead::Vanished
        })
    }
}

async fn download_pack(
    store: &dyn ObjectStore,
    id: &str,
    index: &PackIndex,
    dir: &std::path::Path,
) -> Result<()> {
    tokio::fs::write(dir.join(format!("{id}.idx")), index.as_bytes())
        .await
        .map_err(storage_err)?;
    let mut f = tokio::fs::File::create(dir.join(format!("{id}.pack")))
        .await
        .map_err(storage_err)?;
    let mut stream = store
        .get(&pack_key(id))
        .await
        .map_err(storage_err)?
        .into_stream();
    while let Some(chunk) = stream.try_next().await.map_err(storage_err)? {
        f.write_all(&chunk).await.map_err(storage_err)?;
    }
    f.flush().await.map_err(storage_err)
}

/// Stream a local file to `key` as a multipart upload, which becomes
/// visible only when complete.
async fn upload_file(store: &dyn ObjectStore, key: &Path, path: &std::path::Path) -> Result<()> {
    let upload = store.put_multipart(key).await.map_err(storage_err)?;
    let mut w = WriteMultipart::new_with_chunk_size(upload, UPLOAD_CHUNK);
    let res: Result<()> = async {
        let mut f = tokio::fs::File::open(path).await.map_err(storage_err)?;
        let mut buf = vec![0u8; 1 << 20];
        loop {
            let n = f.read(&mut buf).await.map_err(storage_err)?;
            if n == 0 {
                return Ok(());
            }
            w.wait_for_capacity(UPLOAD_CONCURRENCY)
                .await
                .map_err(storage_err)?;
            w.write(&buf[..n]);
        }
    }
    .await;
    match res {
        Ok(()) => w.finish().await.map(|_| ()).map_err(storage_err),
        Err(e) => {
            let _ = w.abort().await;
            Err(e)
        }
    }
}

// ---- Publish and finish ----

/// A published pack whose loose copies and replaced packs are still in the
/// bucket. Call [`finish`](Self::finish) after the fence. Dropping it instead
/// is safe, leaves both copies, and releases the lease.
pub struct PublishedObjectPack {
    store: ObjectBlobStore,
    report: RepackReport,
    from_loose: Vec<(Hash, LooseStamp)>,
    replaced: Vec<String>,
    opts: RepackOptions,
    lease: Lease,
}

struct Published {
    report: RepackReport,
    from_loose: Vec<(Hash, LooseStamp)>,
    replaced: Vec<String>,
}

impl ObjectBlobStore {
    /// Write every live object into one new pack and publish it, as
    /// [`FsBlobStore::repack_publish`](crate::FsBlobStore::repack_publish)
    /// does. The pack is written to a local temp file (under `TMPDIR`), so
    /// that needs room for it and for the packs it replaces.
    ///
    /// Refuses with `Conflict` while another repack holds the lease.
    pub async fn repack_publish(
        &self,
        hints: impl Into<PackHints>,
        opts: RepackOptions,
    ) -> Result<PublishedObjectPack> {
        let hints = hints.into();
        if !(1..=22).contains(&opts.level) {
            return Err(Error::Invalid(format!(
                "zstd level {} is outside 1..=22",
                opts.level
            )));
        }
        if opts.mode != crate::RepackMode::Full {
            return Err(Error::Invalid(
                "object stores support only a full repack for now".into(),
            ));
        }
        let lease = Lease::acquire(self.store.clone()).await?;
        match self.publish_leased(hints, &opts, &lease).await {
            Ok(p) => Ok(PublishedObjectPack {
                store: self.clone(),
                report: p.report,
                from_loose: p.from_loose,
                replaced: p.replaced,
                opts,
                lease,
            }),
            Err(e) => {
                lease.release().await;
                Err(e)
            }
        }
    }

    async fn publish_leased(
        &self,
        hints: PackHints,
        opts: &RepackOptions,
        lease: &Lease,
    ) -> Result<Published> {
        let mut report = RepackReport {
            garbage_removed: self.remove_unpublished().await?,
            ..RepackReport::default()
        };
        step(opts, RepackStep::Started)?;

        let view = self.packs.refresh().await?;
        let loose = list_all_loose(&*self.store).await?;
        let mut sources: HashMap<Hash, Source> = HashMap::new();
        report.packs_before = view.packs.len() as u64;
        for (p, dead) in &view.packs {
            report.pack_bytes_before += p.index.pack_len();
            for e in p.index.iter() {
                if dead.contains(&e.hash) {
                    continue;
                }
                let s = sources.entry(e.hash).or_default();
                if s.packed.is_none() {
                    s.packed = Some((p.id.clone(), e));
                    report.packed_before += 1;
                }
            }
        }
        let tombstoned: HashSet<Hash> = view
            .packs
            .iter()
            .flat_map(|(_, dead)| dead.iter().copied())
            .collect();
        for (hash, stamp) in loose {
            report.loose_before += 1;
            report.loose_bytes_before += stamp.size;
            sources.entry(hash).or_default().loose = Some(stamp);
        }
        report.dropped_tombstoned = tombstoned
            .iter()
            .filter(|h| !sources.contains_key(h))
            .count() as u64;

        let work = WorkDir::new()?;
        let local_packs = work.0.join(packset::PACKS_DIR);
        let needed: HashSet<&str> = sources
            .values()
            .filter_map(|s| s.packed.as_ref().map(|(id, _)| id.as_str()))
            .collect();
        for (p, _) in view
            .packs
            .iter()
            .filter(|(p, _)| needed.contains(p.id.as_str()))
        {
            download_pack(&*self.store, &p.id, &p.index, &local_packs).await?;
        }

        let mut sources: Vec<(Hash, Source)> = sources.into_iter().collect();
        sources.sort_unstable_by_key(|(h, _)| *h);
        let (hashes, sources): (Vec<Hash>, Vec<Source>) = sources.into_iter().unzip();
        let table = ObjectTable::from_sorted(hashes)?;
        let order = write_order(&table, hint_ids(&table, hints), Vec::new());
        let ahead: Vec<Hash> = order
            .iter()
            .filter(|(id, _)| sources[*id as usize].loose.is_some())
            .map(|(id, _)| *table.hash(*id))
            .collect();
        let new_pack = work.0.join("new.pack");
        let (rt, store, root, path, wopts) = (
            tokio::runtime::Handle::current(),
            self.store.clone(),
            work.0.clone(),
            new_pack.clone(),
            opts.clone(),
        );
        let (written, report, sources, table) = tokio::task::spawn_blocking(move || {
            let local = PackSet::new(&root, DEFAULT_DECODED_CACHE);
            let local_view = local.refresh()?;
            let mut src = ObjectSource {
                rt,
                store,
                sources: &sources,
                local,
                local_view,
                ahead,
                cursor: 0,
                inflight: HashMap::new(),
            };
            let file = std::fs::File::create(&path).map_err(storage_err)?;
            let mut out = BufWriter::with_capacity(1 << 20, file);
            let written = write_pack(&table, order, &mut src, &wopts, &mut out, &mut report)?;
            drop(src);
            out.into_inner().map_err(|e| storage_err(e.error()))?;
            Ok::<_, Error>((written, report, sources, table))
        })
        .await
        .map_err(|e| Error::Storage(format!("repack task failed: {e}")))??;
        let mut report = report;
        step(opts, RepackStep::PackSynced)?;
        let replaced: Vec<String> = view.packs.iter().map(|(p, _)| p.id.clone()).collect();
        if written.records.is_empty() {
            return Ok(Published {
                report,
                from_loose: Vec::new(),
                replaced,
            });
        }

        let idx_bytes = written.encode_index(&table)?;
        let id = pack::pack_id(&idx_bytes);
        // One id means the same bytes, so a repack that changed nothing finds
        // its pack published already and uploads nothing.
        let upload = view.pack(&id).is_none();
        if upload {
            upload_file(&*self.store, &pack_key(&id), &new_pack).await?;
            let size = self
                .store
                .head(&pack_key(&id))
                .await
                .map_err(storage_err)?
                .size;
            if size != written.len {
                return Err(Error::Storage(format!(
                    "uploaded pack {id} has {size} bytes, wrote {}",
                    written.len
                )));
            }
        }
        step(opts, RepackStep::PackRenamed)?;
        step(opts, RepackStep::IndexSynced)?;
        // A repack that lost its lease must not publish beside the holder.
        lease.check().await?;
        let index_len = idx_bytes.len() as u64;
        if upload {
            self.store
                .put(&idx_key(&id), PutPayload::from(idx_bytes))
                .await
                .map_err(storage_err)?;
        }
        step(opts, RepackStep::Published)?;
        report.pack_bytes = written.len;
        report.index_bytes = index_len;
        report.pack_id = Some(id.clone());

        // GC may have deleted objects after this run read them. Hide them in
        // the new pack too; a delete that starts from now on sees the pack.
        let late = self.late_deletes(&table, written.ids(), &sources).await?;
        if !late.is_empty() {
            self.packs
                .write_tombstones(late.iter().map(|h| (id.as_str(), *h)))
                .await?;
            report.late_tombstones = late.len() as u64;
        }
        self.packs.refresh().await?;
        Ok(Published {
            report,
            from_loose: written
                .from_loose
                .into_iter()
                .map(|(id, stamp)| (*table.hash(id), stamp))
                .collect(),
            replaced: replaced.into_iter().filter(|r| *r != id).collect(),
        })
    }

    /// Objects of the new pack whose source GC deleted while it was written:
    /// a loose copy that is gone, or a packed copy tombstoned since.
    async fn late_deletes(
        &self,
        table: &ObjectTable,
        packed: impl Iterator<Item = u32>,
        sources: &[Source],
    ) -> Result<Vec<Hash>> {
        let loose = list_all_loose(&*self.store).await?;
        let tombstones = list_tombstones(&*self.store).await?;
        Ok(packed
            .filter(|&i| {
                let h = table.hash(i);
                let s = &sources[i as usize];
                match (&s.loose, &s.packed) {
                    (Some(_), _) => !loose.contains_key(h),
                    (None, Some((id, _))) => tombstones.get(id).is_some_and(|d| d.contains(h)),
                    (None, None) => false,
                }
            })
            .map(|i| *table.hash(i))
            .collect())
    }

    /// Delete `.pack` objects no index publishes, left by a killed repack.
    /// Only under the lease.
    async fn remove_unpublished(&self) -> Result<u64> {
        let (ids, packs) = list_packs_dir(&*self.store).await?;
        let cutoff = now_secs() - GARBAGE_AGE.as_secs() as i64;
        let garbage: Vec<Path> = packs
            .iter()
            .filter(|(id, modified)| ids.binary_search(id).is_err() && *modified <= cutoff)
            .map(|(id, _)| pack_key(id))
            .collect();
        let n = garbage.len() as u64;
        delete_all(&*self.store, garbage).await?;
        Ok(n)
    }

    /// Tombstones naming pack `id`.
    async fn tombstones_of(&self, id: &str) -> Result<HashSet<Hash>> {
        let prefix = Path::from(format!("{TOMBSTONES_PREFIX}/{id}"));
        let listed: Vec<_> = self
            .store
            .list(Some(&prefix))
            .try_collect()
            .await
            .map_err(storage_err)?;
        Ok(listed
            .iter()
            .filter_map(|o| o.location.filename()?.parse().ok())
            .collect())
    }
}

impl PublishedObjectPack {
    pub fn report(&self) -> &RepackReport {
        &self.report
    }

    /// Delete the loose copies of everything now packed and the replaced
    /// packs, then the tombstones of packs that are gone.
    pub async fn finish(mut self) -> Result<RepackReport> {
        let res = self.finish_leased().await;
        self.lease.release().await;
        res?;
        Ok(std::mem::take(&mut self.report))
    }

    async fn finish_leased(&mut self) -> Result<()> {
        let objects = self.store.store.clone();
        self.lease.check().await?;
        if let Some(id) = self.report.pack_id.clone() {
            self.remove_loose(&id).await?;
        }
        self.lease.check().await?;
        for id in &self.replaced {
            // The index first: without it the pack is unpublished garbage.
            delete_all(&*objects, vec![idx_key(id)]).await?;
            delete_all(&*objects, vec![pack_key(id)]).await?;
            self.report.old_packs_removed += 1;
        }
        step(&self.opts, RepackStep::OldPacksRemoved)?;
        // Tombstones listed before packs, so a pack published in between
        // (none can be, under the lease) would keep its tombstones.
        let tombstones = list_tombstones(&*objects).await?;
        let (ids, _) = list_packs_dir(&*objects).await?;
        let stale: Vec<Path> = tombstones
            .into_iter()
            .filter(|(id, _)| ids.binary_search(id).is_err())
            .flat_map(|(id, hashes)| hashes.into_iter().map(move |h| tombstone_key(&id, &h)))
            .collect();
        delete_all(&*objects, stale).await?;
        step(&self.opts, RepackStep::TombstonesRewritten)?;
        self.store.packs.refresh().await?;
        let loose = list_all_loose(&*objects).await?;
        self.report.loose_after = loose.len() as u64;
        self.report.loose_bytes_after = loose.values().map(|s| s.size).sum();
        Ok(())
    }

    /// Delete each loose copy the new pack holds, if it is still the copy
    /// that was packed and the packed copy is not tombstoned. There is no
    /// conditional delete, so the tombstones are listed again after each
    /// batch: GC tombstones before it deletes, and a blob it deleted and an
    /// upload rescued in between gets its loose copy back from the pack.
    async fn remove_loose(&mut self, id: &str) -> Result<()> {
        let objects = self.store.store.clone();
        let mut dead = self.store.tombstones_of(id).await?;
        let current = list_all_loose(&*objects).await?;
        let from_loose = std::mem::take(&mut self.from_loose);
        let total = from_loose.len();
        let mut batch: Vec<Hash> = Vec::new();
        for (n, (hash, stamp)) in from_loose.iter().enumerate() {
            step(&self.opts, RepackStep::RemovingLoose(n as u64 + 1))?;
            match current.get(hash) {
                None => {}
                Some(now) if now != stamp || dead.contains(hash) => self.report.loose_kept += 1,
                Some(_) => batch.push(*hash),
            }
            if batch.len() >= DELETE_BATCH || (n + 1 == total && !batch.is_empty()) {
                self.lease.check().await?;
                let keys = batch.iter().map(ObjectBlobStore::key).collect();
                delete_all(&*objects, keys).await?;
                step(&self.opts, RepackStep::LooseMovedAside(n as u64 + 1))?;
                let now_dead = self.store.tombstones_of(id).await?;
                for h in batch.drain(..) {
                    if now_dead.contains(&h) && !dead.contains(&h) {
                        let Some(bytes) = self.store.packs.read_from(id, &h).await? else {
                            return Err(Error::Storage(format!("{h} vanished from pack {id}")));
                        };
                        objects
                            .put(&ObjectBlobStore::key(&h), PutPayload::from(bytes))
                            .await
                            .map_err(storage_err)?;
                        self.report.loose_kept += 1;
                    } else {
                        self.report.loose_removed += 1;
                    }
                }
                dead = now_dead;
            }
        }
        Ok(())
    }
}
