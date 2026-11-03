//! The packs a filesystem store reads from: their indexes, the tombstones
//! that hide deleted objects in them, and a cache of decoded objects.
//!
//! Everything here is synchronous file I/O; the async store calls it from
//! blocking tasks.

use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant, SystemTime};

use bytes::Bytes;
use leep_core::{BlobMeta, Error, Hash, Result};

use crate::cache::Lru;
use crate::pack::{self, IdxEntry, PackIndex, PACK_MAGIC, READ_CHAIN_LIMIT};
use crate::retire::{read_retired, RETIRED};
use crate::storage_err;

pub(crate) const PACKS_DIR: &str = "packs";
pub(crate) const TOMBSTONES: &str = "tombstones";
/// Held while appending to or rewriting `tombstones`. A separate file, because
/// a rewrite renames a new `tombstones` into place and an append that locked
/// the old one would land in an unlinked file.
pub(crate) const TOMBSTONES_LOCK: &str = "tombstones.lock";
/// Decoded objects kept for delta chains: a read of a recent version usually
/// decodes the older versions it is based on, which the next read needs again.
pub(crate) const DEFAULT_DECODED_CACHE: usize = 64 << 20;
/// A view whose `packs/` modification time was this close to the clock when
/// it was scanned is unconfirmed: a change in the same timestamp tick as the
/// last one before the scan leaves the time as it was (ADR 0070). Covers the
/// coarsest timestamps of a local filesystem (2 s on FAT).
pub(crate) const MTIME_GRANULARITY: Duration = Duration::from_secs(2);
/// Shortest time between two rescans that misses on an unconfirmed view
/// trigger while its stamps look unchanged.
pub(crate) const UNCONFIRMED_RESCAN: Duration = Duration::from_millis(100);

/// Why a store scanned its packs again, the `reason` label of
/// `leep_blob_pack_scans_total` (ADR 0070).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ScanReason {
    /// No view yet.
    First,
    /// A miss found a stamp changed: `packs/` or the tombstones on a
    /// filesystem, `packs/.gen` in a bucket.
    MissChanged,
    /// A miss on a filesystem view whose stamp may hide a change (see
    /// [`MTIME_GRANULARITY`]), at most every [`UNCONFIRMED_RESCAN`].
    MissUnconfirmed,
    /// A bucket without `packs/.gen`, or whose stamp has no etag: every
    /// miss lists `packs/`, as before ADR 0070.
    MissUnstamped,
    /// [`BlobStore::get_exact`](leep_core::BlobStore::get_exact) missed
    /// after the stamp check and looks once more.
    MissForced,
    /// `contains` found the tombstones changed.
    Tombstones,
    /// A bucket's periodic full listing on a miss (ADR 0040 section 5).
    Interval,
    /// A repack step, which must see every pack.
    Repack,
    /// Anything else that must see every pack: a delete, a pack count, a
    /// deleted pack found under a bucket view.
    Explicit,
}

impl ScanReason {
    fn label(self) -> &'static str {
        match self {
            ScanReason::First => "first",
            ScanReason::MissChanged => "miss_changed",
            ScanReason::MissUnconfirmed => "miss_unconfirmed",
            ScanReason::MissUnstamped => "miss_unstamped",
            ScanReason::MissForced => "miss_forced",
            ScanReason::Tombstones => "tombstones",
            ScanReason::Interval => "interval",
            ScanReason::Repack => "repack",
            ScanReason::Explicit => "explicit",
        }
    }

    pub fn record(self) {
        metrics::counter!("leep_blob_pack_scans_total", "reason" => self.label()).increment(1);
    }
}

/// Record a miss of the cached pack view: how it ended, what resolving it
/// took, and how old the view that answered was.
pub(crate) fn record_miss(started: Instant, found: usize, absent: usize, view_scanned: Instant) {
    if found > 0 {
        metrics::counter!("leep_blob_miss_total", "outcome" => "found_after_scan")
            .increment(found as u64);
    }
    if absent > 0 {
        metrics::counter!("leep_blob_miss_total", "outcome" => "absent").increment(absent as u64);
    }
    metrics::histogram!("leep_blob_miss_seconds").record(started.elapsed().as_secs_f64());
    metrics::gauge!("leep_blob_pack_view_age_seconds").set(view_scanned.elapsed().as_secs_f64());
}

/// One published pack, open for reading. The open handle keeps the bytes
/// readable after a repack deletes the file.
pub(crate) struct Pack {
    pub id: String,
    pub index: PackIndex,
    file: File,
}

impl Pack {
    /// A pack this process just wrote and published, from its decoded
    /// index and the open file, so publishing need not read either back.
    pub fn from_parts(id: String, index: PackIndex, file: File) -> Pack {
        Pack { id, index, file }
    }

    /// `None` if the pack vanished (a repack replaced it) while loading.
    fn open(dir: &Path, id: &str) -> Result<Option<Pack>> {
        let idx_bytes = match std::fs::read(dir.join(format!("{id}.idx"))) {
            Ok(b) => b,
            Err(e) if e.kind() == ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(storage_err(e)),
        };
        if pack::pack_id(&idx_bytes) != id {
            return Err(Error::Storage(format!(
                "corrupt pack index {id}.idx: its hash is not its name"
            )));
        }
        let index =
            PackIndex::decode(idx_bytes).map_err(|e| Error::Storage(format!("pack {id}: {e}")))?;
        let file = match File::open(dir.join(format!("{id}.pack"))) {
            Ok(f) => f,
            Err(e) if e.kind() == ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(storage_err(e)),
        };
        let len = file.metadata().map_err(storage_err)?.len();
        let mut magic = [0u8; 8];
        if len != index.pack_len() || read_at(&file, &mut magic, 0).is_err() || magic != PACK_MAGIC
        {
            return Err(Error::Storage(format!(
                "corrupt pack {id}.pack: {len} bytes or bad magic, index says {}",
                index.pack_len()
            )));
        }
        Ok(Some(Pack {
            id: id.to_string(),
            index,
            file,
        }))
    }

    /// Ask the kernel to start reading `entries`' frames, without waiting.
    /// A delta chain's frames lie far apart in the pack (each version is
    /// written with the changeset that made it), so reading them one by one
    /// from a cold disk waits once per frame; asked for together, the disk
    /// serves them at once. Advice only: errors are ignored, and other
    /// systems read as before.
    pub fn prefetch(&self, entries: &[IdxEntry]) {
        #[cfg(target_os = "linux")]
        for e in entries {
            let Some(len) = std::num::NonZeroU64::new(e.stored_len) else {
                continue;
            };
            let _ = rustix::fs::fadvise(
                &self.file,
                e.offset,
                Some(len),
                rustix::fs::Advice::WillNeed,
            );
        }
        #[cfg(not(target_os = "linux"))]
        let _ = entries;
    }

    pub fn frame(&self, e: &IdxEntry) -> Result<Vec<u8>> {
        let len = usize::try_from(e.stored_len)
            .map_err(|_| Error::Storage("pack entry too large".into()))?;
        let mut buf = vec![0u8; len];
        read_at(&self.file, &mut buf, e.offset).map_err(storage_err)?;
        Ok(buf)
    }
}

fn read_at(f: &File, buf: &mut [u8], offset: u64) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        std::os::unix::fs::FileExt::read_exact_at(f, buf, offset)
    }
    #[cfg(windows)]
    {
        let mut done = 0;
        while done < buf.len() {
            let n = std::os::windows::fs::FileExt::seek_read(
                f,
                &mut buf[done..],
                offset + done as u64,
            )?;
            if n == 0 {
                return Err(ErrorKind::UnexpectedEof.into());
            }
            done += n;
        }
        Ok(())
    }
}

/// Enough of a file's identity to notice that it changed or was replaced.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct FileSig {
    len: u64,
    modified: Option<std::time::SystemTime>,
    ino: u64,
}

/// A directory's stamp: inode and modification time. Its length is left
/// out: some filesystems change it with every entry and others never.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct DirSig {
    modified: Option<SystemTime>,
    ino: u64,
}

fn dir_sig(path: &Path) -> Result<Option<DirSig>> {
    Ok(file_sig(path)?.map(|s| DirSig {
        modified: s.modified,
        ino: s.ino,
    }))
}

/// Whether a directory stamped `sig` when the clock read `now` shows every
/// later change: its time is older than any timestamp tick, so the next
/// change in it sets another. An absent directory shows its creation.
fn stamp_confirmed(sig: Option<DirSig>, now: SystemTime) -> bool {
    match sig {
        None => true,
        Some(DirSig {
            modified: Some(m), ..
        }) => now
            .duration_since(m)
            .is_ok_and(|age| age > MTIME_GRANULARITY),
        Some(_) => false,
    }
}

pub(crate) fn file_sig(path: &Path) -> Result<Option<FileSig>> {
    match std::fs::metadata(path) {
        Ok(m) => Ok(Some(FileSig {
            len: m.len(),
            modified: m.modified().ok(),
            #[cfg(unix)]
            ino: std::os::unix::fs::MetadataExt::ino(&m),
            #[cfg(not(unix))]
            ino: 0,
        })),
        Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
        Err(e) => Err(storage_err(e)),
    }
}

/// Pack id → hashes deleted from that pack.
pub(crate) type Tombstones = HashMap<String, HashSet<Hash>>;

/// One line per deleted object: `<pack id hex> <hash hex>\n`. A last line
/// without its newline is a torn append and is ignored.
pub(crate) fn parse_tombstones(text: &[u8]) -> Tombstones {
    let mut out = Tombstones::new();
    let complete = match text.iter().rposition(|&b| b == b'\n') {
        Some(i) => &text[..=i],
        None => return out,
    };
    for line in complete.split(|&b| b == b'\n') {
        if line.is_empty() {
            continue;
        }
        let parsed = std::str::from_utf8(line).ok().and_then(|l| {
            let (id, hash) = l.split_once(' ')?;
            let ok = id.len() == 64 && id.bytes().all(|c| c.is_ascii_hexdigit());
            Some((ok.then_some(id)?, hash.parse::<Hash>().ok()?))
        });
        match parsed {
            Some((id, hash)) => {
                out.entry(id.to_string()).or_default().insert(hash);
            }
            None => tracing::warn!("ignoring a malformed line in packs/tombstones"),
        }
    }
    out
}

pub(crate) fn tombstone_line(pack_id: &str, hash: &Hash) -> String {
    format!("{pack_id} {hash}\n")
}

/// The tombstones file's contents. Stat it first: a change after the stat
/// is noticed next time.
fn read_tombstones(path: &Path) -> Result<Tombstones> {
    match std::fs::read(path) {
        Ok(b) => Ok(parse_tombstones(&b)),
        Err(e) if e.kind() == ErrorKind::NotFound => Ok(Tombstones::new()),
        Err(e) => Err(storage_err(e)),
    }
}

/// Ids of the published packs (those with an index), sorted.
pub(crate) fn published_ids(dir: &Path) -> Result<Vec<String>> {
    let rd = match std::fs::read_dir(dir) {
        Ok(rd) => rd,
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(storage_err(e)),
    };
    let mut ids = Vec::new();
    for entry in rd {
        let name = entry.map_err(storage_err)?.file_name();
        let name = name.to_string_lossy();
        if let Some(id) = name.strip_suffix(".idx") {
            if id.len() == 64 && id.bytes().all(|c| c.is_ascii_hexdigit()) {
                ids.push(id.to_string());
            }
        }
    }
    ids.sort();
    Ok(ids)
}

/// The packs and tombstones as of one scan.
pub(crate) struct View {
    /// Each pack with the hashes deleted from it: the packs reads search
    /// first, smallest first (recent writes are read most), then the packs
    /// a repack of this process merged into a published pack and has not
    /// deleted yet ([`PackSet::supersede`]), which reads skip.
    pub packs: Vec<(Arc<Pack>, HashSet<Hash>)>,
    /// How many of `packs` reads search.
    searchable: usize,
    ids: Vec<String>,
    /// The packs `packs/retired` named when the scan read it (ADR 0080):
    /// ordered after the searchable ones, like superseded packs.
    retired: Arc<HashSet<String>>,
    /// Stamps taken before the scan read the retired record, the
    /// tombstones and the directory: while all are unchanged (and the view
    /// is confirmed), so are the packs, which of them reads search, and
    /// their tombstones (ADR 0070).
    retired_sig: Option<FileSig>,
    tombstones: Option<FileSig>,
    dir: Option<DirSig>,
    /// When the scan behind this view began.
    scanned: Instant,
    /// Whether an unchanged `dir` stamp proves `packs/` unchanged: false
    /// while its time is within [`MTIME_GRANULARITY`] of the scan, or when a
    /// listed pack failed to load.
    confirmed: bool,
    /// Whether every listed pack loaded. If one did not, its tombstones
    /// were dropped, so the next scan reads the file again.
    complete: bool,
}

impl View {
    /// The first live packed copy of `hash`, in the packs reads search.
    pub fn locate(&self, hash: &Hash) -> Option<(&Arc<Pack>, IdxEntry)> {
        self.packs[..self.searchable].iter().find_map(|(p, dead)| {
            let e = p.index.find(hash)?;
            (!dead.contains(hash)).then_some((p, e))
        })
    }

    /// The packs reads search: every published pack but the superseded and
    /// the retired.
    pub fn searchable(&self) -> &[(Arc<Pack>, HashSet<Hash>)] {
        &self.packs[..self.searchable]
    }

    pub fn is_empty(&self) -> bool {
        self.packs.is_empty()
    }

    /// When the scan behind this view began.
    pub fn scanned(&self) -> Instant {
        self.scanned
    }
}

/// Order `packs` for [`View`]: searchable ones smallest first, superseded
/// ones after them. Returns how many are searchable.
pub(crate) fn order_packs<P: PackLike>(
    packs: &mut [(Arc<P>, HashSet<Hash>)],
    superseded: &HashSet<String>,
) -> usize {
    packs.sort_by(|(a, _), (b, _)| {
        superseded
            .contains(a.id())
            .cmp(&superseded.contains(b.id()))
            .then(a.index().len().cmp(&b.index().len()))
            .then_with(|| a.id().cmp(b.id()))
    });
    packs
        .iter()
        .take_while(|(p, _)| !superseded.contains(p.id()))
        .count()
}

/// The packs reads skip: superseded in this process, or retired.
pub(crate) fn hidden(superseded: &HashSet<String>, retired: &HashSet<String>) -> HashSet<String> {
    if retired.is_empty() {
        return superseded.clone();
    }
    superseded.union(retired).cloned().collect()
}

/// A loaded pack of either kind of store, for [`order_packs`].
pub(crate) trait PackLike {
    fn id(&self) -> &str;
    fn index(&self) -> &PackIndex;
}

impl PackLike for Pack {
    fn id(&self) -> &str {
        &self.id
    }
    fn index(&self) -> &PackIndex {
        &self.index
    }
}

pub(crate) struct PackSet {
    dir: PathBuf,
    view: RwLock<Option<Arc<View>>>,
    refreshing: Mutex<()>,
    /// Directory scans so far, for tests.
    scans: AtomicU64,
    decoded: DecodedCache,
    /// Packs a repack of this process merged into a published pack, until
    /// they are deleted: every live object of each is in that pack (packed
    /// copies are always copied; GC's deletes are tombstoned there too), so
    /// reads here skip them. Other processes read them until the deletion.
    superseded: Mutex<HashSet<String>>,
}

impl PackSet {
    pub fn new(root: &Path, decoded_capacity: usize) -> Self {
        PackSet {
            dir: root.join(PACKS_DIR),
            view: RwLock::new(None),
            refreshing: Mutex::new(()),
            scans: AtomicU64::new(0),
            decoded: DecodedCache::new(decoded_capacity),
            superseded: Mutex::default(),
        }
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Directory scans so far, for tests.
    #[cfg(test)]
    pub fn scans(&self) -> u64 {
        self.scans.load(Ordering::Relaxed)
    }

    /// The current view, scanning on first use.
    pub fn view(&self) -> Result<Arc<View>> {
        if let Some(v) = self.view.read().unwrap().as_ref() {
            return Ok(v.clone());
        }
        let _one = self.refreshing.lock().unwrap();
        if let Some(v) = self.view.read().unwrap().as_ref() {
            return Ok(v.clone());
        }
        self.scan(ScanReason::First)
    }

    /// Scan `packs/` again now, reusing packs already open: for callers
    /// that must see every pack, whatever the stamps say.
    pub fn refresh(&self) -> Result<Arc<View>> {
        self.rescan(ScanReason::Explicit)
    }

    /// [`refresh`](Self::refresh), counted under `reason`.
    pub fn rescan(&self, reason: ScanReason) -> Result<Arc<View>> {
        self.rescan_after(Instant::now(), reason)
    }

    /// A view whose scan began at or after `since`: the current one if it
    /// qualifies (a concurrent caller may just have scanned), else a new
    /// one. Concurrent misses share scans this way instead of queueing one
    /// each.
    fn rescan_after(&self, since: Instant, reason: ScanReason) -> Result<Arc<View>> {
        let _one = self.refreshing.lock().unwrap();
        if let Some(v) = self.view.read().unwrap().as_ref() {
            if v.scanned >= since {
                return Ok(v.clone());
            }
        }
        self.scan(reason)
    }

    /// The view a miss of the current one should search: the current view
    /// while the stamps of the retired record, the tombstones and `packs/`
    /// are as it recorded them (three `stat`s, no lock), else a scan. An
    /// unconfirmed view is rescanned at most every [`UNCONFIRMED_RESCAN`].
    pub fn view_after_miss(&self) -> Result<Arc<View>> {
        let v = self.view()?;
        let started = Instant::now();
        let retired = file_sig(&self.dir.join(RETIRED))?;
        let tombstones = file_sig(&self.dir.join(TOMBSTONES))?;
        let dir = dir_sig(&self.dir)?;
        if retired != v.retired_sig || tombstones != v.tombstones || dir != v.dir {
            return self.rescan_after(started, ScanReason::MissChanged);
        }
        if !v.confirmed && started.duration_since(v.scanned) >= UNCONFIRMED_RESCAN {
            let since = started.checked_sub(UNCONFIRMED_RESCAN).unwrap_or(started);
            return self.rescan_after(since, ScanReason::MissUnconfirmed);
        }
        Ok(v)
    }

    /// Scan `packs/`, reusing packs already open. Another process may have
    /// published a pack (and deleted the loose copies it holds), deleted
    /// replaced packs, or written tombstones. Call with `refreshing` held.
    fn scan(&self, reason: ScanReason) -> Result<Arc<View>> {
        let began = Instant::now();
        let current = self.view.read().unwrap().clone();
        // The retired record first, then the tombstones, then the
        // directory (ADR 0080): a line is dropped only after the pack it
        // names is gone, so a pack this scan lists that a record read
        // earlier forgot was never retired when that record was read. Read
        // only when its stamp changed, as the tombstones are.
        let retired_path = self.dir.join(RETIRED);
        let retired_sig = file_sig(&retired_path)?;
        let retired = match &current {
            Some(v) if v.retired_sig == retired_sig => None,
            _ => Some(Arc::new(
                read_retired(&self.dir)?.into_keys().collect::<HashSet<_>>(),
            )),
        };
        // Tombstones before the directory: a repack deletes replaced indexes
        // before it drops their tombstones, so this order never pairs an old
        // pack with a tombstone file that has forgotten it. The file is read
        // only when its stamp changed: tombstones are only appended, or
        // rewritten into a new file, and one naming a pack this scan lists
        // for the first time was written after that pack was published, so
        // after the stamp the current view recorded.
        let tomb_path = self.dir.join(TOMBSTONES);
        let tomb_sig = file_sig(&tomb_path)?;
        let mut tombstones = match &current {
            Some(v) if v.complete && v.tombstones == tomb_sig => None,
            _ => Some(read_tombstones(&tomb_path)?),
        };
        let now = SystemTime::now();
        let dir = dir_sig(&self.dir)?;
        let ids = published_ids(&self.dir)?;
        self.scans.fetch_add(1, Ordering::Relaxed);
        reason.record();
        let confirmed = stamp_confirmed(dir, now);
        if let Some(v) = current
            .as_ref()
            .filter(|v| v.ids == ids && tombstones.is_none() && retired.is_none())
        {
            let view = Arc::new(View {
                packs: v.packs.clone(),
                searchable: v.searchable,
                ids,
                retired: v.retired.clone(),
                retired_sig,
                tombstones: tomb_sig,
                dir,
                scanned: began,
                confirmed,
                complete: true,
            });
            *self.view.write().unwrap() = Some(view.clone());
            return Ok(view);
        }
        let retired = match retired {
            Some(r) => r,
            None => current
                .as_ref()
                .map_or_else(Default::default, |v| v.retired.clone()),
        };
        let superseded = {
            let mut s = self.superseded.lock().unwrap();
            // Deleted: nothing left to skip.
            s.retain(|id| ids.binary_search(id).is_ok());
            s.clone()
        };
        let hidden = hidden(&superseded, &retired);
        let mut open: HashMap<&str, &(Arc<Pack>, HashSet<Hash>)> = HashMap::new();
        if let Some(v) = &current {
            for p in &v.packs {
                open.insert(&p.0.id, p);
            }
        }
        let mut packs = Vec::new();
        let mut loaded = Vec::new();
        for id in &ids {
            let known = open.get(id.as_str());
            let pack = match known {
                Some((p, _)) => p.clone(),
                None => match Pack::open(&self.dir, id)? {
                    Some(p) => Arc::new(p),
                    None => continue,
                },
            };
            loaded.push(id.clone());
            let dead = match &mut tombstones {
                Some(t) => t.remove(id).unwrap_or_default(),
                None => known.map(|(_, d)| d.clone()).unwrap_or_default(),
            };
            packs.push((pack, dead));
        }
        let searchable = order_packs(&mut packs, &hidden);
        let complete = loaded == ids;
        let view = Arc::new(View {
            packs,
            searchable,
            // Only what loaded, so a pack seen mid-delete is retried next
            // time, and the view stays unconfirmed until then.
            ids: if complete { ids } else { loaded },
            retired,
            retired_sig,
            tombstones: tomb_sig,
            dir,
            scanned: began,
            confirmed: confirmed && complete,
            complete,
        });
        *self.view.write().unwrap() = Some(view.clone());
        metrics::gauge!("leep_blob_packs").set(searchable as f64);
        Ok(view)
    }

    /// Add a pack this process just published to the view, without
    /// scanning `packs/`: its writer reads its objects next. Tombstones
    /// cannot name a pack no view has listed, so it has none yet.
    pub fn insert_published(&self, pack: Pack) -> Result<()> {
        let _one = self.refreshing.lock().unwrap();
        let Some(current) = self.view.read().unwrap().clone() else {
            // No view yet: the first use scans and finds it.
            return Ok(());
        };
        if current.ids.binary_search(&pack.id).is_ok() {
            return Ok(());
        }
        let mut ids = current.ids.clone();
        let at = ids.binary_search(&pack.id).unwrap_err();
        ids.insert(at, pack.id.clone());
        let mut packs = current.packs.clone();
        packs.push((Arc::new(pack), HashSet::new()));
        let superseded = self.superseded.lock().unwrap().clone();
        let searchable = order_packs(&mut packs, &hidden(&superseded, &current.retired));
        *self.view.write().unwrap() = Some(Arc::new(View {
            packs,
            searchable,
            ids,
            retired: current.retired.clone(),
            ..*current
        }));
        metrics::gauge!("leep_blob_packs").set(searchable as f64);
        Ok(())
    }

    /// Stop searching `ids`: a repack of this process merged them into a
    /// published pack, which holds every live object of each. They stay in
    /// the view, so a delete still tombstones them for other processes,
    /// until the repack deletes them.
    pub fn supersede(&self, ids: &[String]) {
        let _one = self.refreshing.lock().unwrap();
        let superseded = {
            let mut s = self.superseded.lock().unwrap();
            s.extend(ids.iter().cloned());
            s.clone()
        };
        let Some(current) = self.view.read().unwrap().clone() else {
            return;
        };
        let mut packs = current.packs.clone();
        let searchable = order_packs(&mut packs, &hidden(&superseded, &current.retired));
        *self.view.write().unwrap() = Some(Arc::new(View {
            packs,
            searchable,
            ids: current.ids.clone(),
            retired: current.retired.clone(),
            ..*current
        }));
        metrics::gauge!("leep_blob_packs").set(searchable as f64);
    }

    /// The view, rescanned first if the tombstones file changed since. One
    /// `stat`, so a caller that must not report a deleted object can afford it.
    pub fn fresh_tombstones(&self) -> Result<Arc<View>> {
        let v = self.view()?;
        let started = Instant::now();
        if file_sig(&self.dir.join(TOMBSTONES))? == v.tombstones {
            return Ok(v);
        }
        self.rescan_after(started, ScanReason::Tombstones)
    }

    /// Bytes of a packed object, `None` if no live packed copy exists in
    /// the view a miss gets ([`view_after_miss`](Self::view_after_miss)).
    pub fn get(&self, hash: &Hash) -> Result<Option<Bytes>> {
        let v = self.view()?;
        if let Some((p, e)) = v.locate(hash) {
            return self.decode(p, &e).map(Some);
        }
        let started = Instant::now();
        let v = self.view_after_miss()?;
        let found = v.locate(hash);
        record_miss(
            started,
            found.is_some() as usize,
            found.is_none() as usize,
            v.scanned,
        );
        match found {
            Some((p, e)) => self.decode(p, &e).map(Some),
            None => Ok(None),
        }
    }

    /// [`get`](Self::get), with a `None` that is exact: after a miss of
    /// the view a miss gets, one scan that began after `called`, which
    /// holds every pack published before then. Callers pass when their call
    /// began, so lookups started together share one scan.
    pub fn get_exact(&self, hash: &Hash, called: Instant) -> Result<Option<Bytes>> {
        let v = self.view()?;
        if let Some((p, e)) = v.locate(hash) {
            return self.decode(p, &e).map(Some);
        }
        let started = Instant::now();
        let mut v = self.view_after_miss()?;
        if v.locate(hash).is_none() {
            v = self.rescan_after(called, ScanReason::MissForced)?;
        }
        let found = v.locate(hash);
        record_miss(
            started,
            found.is_some() as usize,
            found.is_none() as usize,
            v.scanned,
        );
        match found {
            Some((p, e)) => self.decode(p, &e).map(Some),
            None => Ok(None),
        }
    }

    /// Whether a live packed copy exists: exact for tombstones, which GC
    /// relies on (ADR 0012); a pack another process published is found as
    /// [`get`](Self::get) finds it.
    pub fn contains(&self, hash: &Hash) -> Result<bool> {
        if self.fresh_tombstones()?.locate(hash).is_some() {
            return Ok(true);
        }
        let started = Instant::now();
        let v = self.view_after_miss()?;
        let found = v.locate(hash).is_some();
        record_miss(started, found as usize, !found as usize, v.scanned);
        Ok(found)
    }

    /// Start reading every frame that decoding `hashes` will need (see
    /// [`Pack::prefetch`]); a later [`get`](Self::get) of each finds its
    /// frames in the page cache or on their way. Hashes not packed are
    /// skipped.
    pub fn prefetch(&self, hashes: &[Hash]) -> Result<()> {
        if hashes.is_empty() {
            return Ok(());
        }
        let v = self.view()?;
        for h in hashes {
            if let Some((p, e)) = v.locate(h) {
                // A chain that does not plan fails its own read instead.
                if let Ok(chain) = self.decoded.plan(&p.id, &p.index, &e) {
                    p.prefetch(&chain.entries);
                }
            }
        }
        Ok(())
    }

    /// Decode an entry and check it against its hash.
    pub fn decode(&self, pack: &Pack, entry: &IdxEntry) -> Result<Bytes> {
        let chain = self.decoded.plan(&pack.id, &pack.index, entry)?;
        if chain.entries.len() > 1 {
            pack.prefetch(&chain.entries);
        }
        self.decoded
            .finish(&pack.id, entry, chain, |e| pack.frame(e))
    }

    /// Hide packed copies of `hash` from every pack that has one. Durable
    /// before it returns.
    pub fn tombstone(&self, hash: &Hash) -> Result<()> {
        let v = self.refresh()?;
        let lines: String = v
            .packs
            .iter()
            .filter(|(p, dead)| p.index.find(hash).is_some() && !dead.contains(hash))
            .map(|(p, _)| tombstone_line(&p.id, hash))
            .collect();
        if lines.is_empty() {
            return Ok(());
        }
        self.append_tombstones(&lines)?;
        self.refresh()?;
        Ok(())
    }

    pub fn append_tombstones(&self, lines: &str) -> Result<()> {
        append_tombstones(&self.dir, lines)
    }
}

/// Decoded packed objects, shared by every pack of a store. A read of a
/// recent version usually decodes the older versions it is based on, which
/// the next read needs again.
pub(crate) struct DecodedCache {
    lru: Mutex<Lru>,
    capacity: usize,
    /// Whether the last object decoded was a tree node: reads come in runs
    /// of one kind (a tree walk, a file's versions), so its kind is checked
    /// first. Only a hint; every object is still checked.
    last_tree: std::sync::atomic::AtomicBool,
}

/// What a delta chain needs: entries to decode, the wanted one first and the
/// deepest base last, and the cached object below them, if any.
pub(crate) struct Chain {
    pub entries: Vec<IdxEntry>,
    start: Option<Bytes>,
}

fn corrupt(pack_id: &str, hash: &Hash, why: String) -> Error {
    Error::Storage(format!(
        "corrupt packed object {hash} in pack {pack_id}: {why}"
    ))
}

impl DecodedCache {
    pub fn new(capacity: usize) -> Self {
        DecodedCache {
            lru: Mutex::default(),
            capacity,
            last_tree: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// Follow `entry`'s chain iteratively down to a whole entry or a cached
    /// object. Separate from [`finish`](Self::finish) so a remote reader can
    /// fetch every frame of the chain in one batch.
    pub fn plan(&self, pack_id: &str, index: &PackIndex, entry: &IdxEntry) -> Result<Chain> {
        let mut entries = vec![*entry];
        loop {
            let cur = *entries.last().unwrap();
            if let Some(b) = self.lru.lock().unwrap().get(&cur.hash) {
                entries.pop();
                return Ok(Chain {
                    entries,
                    start: Some(b),
                });
            }
            // In range: decoding the index checked every base.
            let Some(base) = cur.base else {
                return Ok(Chain {
                    entries,
                    start: None,
                });
            };
            if entries.len() >= READ_CHAIN_LIMIT {
                return Err(corrupt(pack_id, &entry.hash, "delta chain too long".into()));
            }
            entries.push(index.entry(base));
        }
    }

    /// Decode a planned chain back up, given each entry's frame. Every decoded
    /// object is checked against its hash, so damage is an error and never
    /// wrong bytes.
    pub fn finish<F, B>(
        &self,
        pack_id: &str,
        entry: &IdxEntry,
        chain: Chain,
        mut frame: F,
    ) -> Result<Bytes>
    where
        F: FnMut(&IdxEntry) -> Result<B>,
        B: AsRef<[u8]>,
    {
        let Chain {
            mut entries,
            start: mut bytes,
        } = chain;
        let bad = |why: String| corrupt(pack_id, &entry.hash, why);
        let mut tree_first = self.last_tree.load(std::sync::atomic::Ordering::Relaxed);
        while let Some(e) = entries.pop() {
            let frame = frame(&e)?;
            let data = pack::decompress(frame.as_ref(), bytes.as_deref(), e.len)
                .map_err(|err| bad(err.to_string()))?;
            match pack::object_kind(&e.hash, &data, tree_first) {
                Some(tree) => tree_first = tree,
                None => return Err(bad(format!("{} decodes to other bytes", e.hash))),
            }
            let data = Bytes::from(data);
            self.lru
                .lock()
                .unwrap()
                .insert(e.hash, data.clone(), self.capacity);
            bytes = Some(data);
        }
        self.last_tree
            .store(tree_first, std::sync::atomic::Ordering::Relaxed);
        Ok(bytes.expect("the chain decodes at least one entry"))
    }
}

/// Packed objects above `after`, minus each pack's deleted ones, merged with
/// `loose` (which wins) in hash order, at most `limit`. A copy in several
/// packs is listed once, with its latest modification time.
pub(crate) fn merge_listing<'a>(
    packs: impl IntoIterator<Item = (&'a PackIndex, &'a HashSet<Hash>)>,
    loose: Vec<BlobMeta>,
    after: Option<Hash>,
    limit: usize,
) -> Vec<BlobMeta> {
    let mut merged = std::collections::BTreeMap::new();
    for (index, dead) in packs {
        let from = after.map_or(0, |a| match index.rank_of(&a) {
            Ok(r) => r + 1,
            Err(r) => r,
        });
        for e in (from..index.len())
            .map(|r| index.entry(r as u32))
            .filter(|e| !dead.contains(&e.hash))
            .take(limit)
        {
            let m = BlobMeta {
                hash: e.hash,
                size: e.len,
                modified: Some(e.modified),
            };
            merged
                .entry(e.hash)
                .and_modify(|old: &mut BlobMeta| old.modified = old.modified.max(m.modified))
                .or_insert(m);
        }
    }
    for m in loose {
        merged.insert(m.hash, m);
    }
    merged.into_values().take(limit).collect()
}

/// Append lines to `packs/tombstones` under its lock, durably.
pub(crate) fn append_tombstones(dir: &Path, lines: &str) -> Result<()> {
    let _lock = lock_tombstones(dir)?;
    cut_torn_tombstone(&dir.join(TOMBSTONES))?;
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(dir.join(TOMBSTONES))
        .map_err(storage_err)?;
    f.write_all(lines.as_bytes()).map_err(storage_err)?;
    f.sync_all().map_err(storage_err)?;
    sync_dir(dir)
}

/// Cut off a last line without its newline, left by an append that died part
/// way. Appending after it would join it to the new first line, and readers
/// drop the joined line, losing a tombstone that was reported written. Call
/// with the tombstone lock held. The torn line's delete never returned, so
/// dropping it is what readers did already.
fn cut_torn_tombstone(path: &Path) -> Result<()> {
    let mut f = match std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
    {
        Ok(f) => f,
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(storage_err(e)),
    };
    let len = f.metadata().map_err(storage_err)?.len();
    if len == 0 {
        return Ok(());
    }
    let mut last = [0u8; 1];
    f.seek(SeekFrom::Start(len - 1)).map_err(storage_err)?;
    f.read_exact(&mut last).map_err(storage_err)?;
    if last[0] == b'\n' {
        return Ok(());
    }
    let mut text = Vec::new();
    f.seek(SeekFrom::Start(0)).map_err(storage_err)?;
    f.read_to_end(&mut text).map_err(storage_err)?;
    let keep = text.iter().rposition(|&b| b == b'\n').map_or(0, |i| i + 1);
    tracing::warn!(
        bytes = text.len() - keep,
        "cutting a torn last line off packs/tombstones"
    );
    f.set_len(keep as u64).map_err(storage_err)?;
    f.sync_all().map_err(storage_err)
}

/// Exclusive lock on `packs/tombstones.lock`, released on drop.
pub(crate) fn lock_tombstones(dir: &Path) -> Result<File> {
    std::fs::create_dir_all(dir).map_err(storage_err)?;
    let f = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(dir.join(TOMBSTONES_LOCK))
        .map_err(storage_err)?;
    f.lock().map_err(storage_err)?;
    Ok(f)
}

/// Make renames and creations in `dir` durable.
pub(crate) fn sync_dir(dir: &Path) -> Result<()> {
    leep_core::fsync::sync_dir(dir).map_err(storage_err)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tombstone_lines_parse_and_torn_tails_are_ignored() {
        let id = "ab".repeat(32);
        let h1 = Hash::of_blob(b"one");
        let h2 = Hash::of_blob(b"two");
        let mut text = tombstone_line(&id, &h1);
        text.push_str("garbage line\n");
        text.push_str(&tombstone_line(&id, &h2)[..40]);
        let t = parse_tombstones(text.as_bytes());
        assert_eq!(t.len(), 1);
        assert_eq!(t[&id], HashSet::from([h1]));
        assert!(parse_tombstones(b"").is_empty());
    }
}
