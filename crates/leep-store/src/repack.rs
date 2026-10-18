//! Repack of a filesystem store (ADR 0029, ADR 0039): live objects go into
//! one new pack, which is made durable and published; after a fence the loose
//! copies and the replaced packs are deleted. A full repack packs every loose
//! and packed object; an incremental one packs the loose objects and folds in
//! only the packs its merge rule picks.
//!
//! The two halves are separate calls ([`FsBlobStore::repack_publish`] and
//! [`PublishedPack::finish`]) so the caller waits out the fence in between.
//! A run killed anywhere leaves a readable store: before the index rename
//! nothing is published (the next repack deletes the temp files), and after
//! it every object is in the new pack, perhaps also loose or in an old pack.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashSet};
use std::fs::File;
use std::io::{BufWriter, ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use leep_core::intern::{PackHints, NO_ID};
use leep_core::{Error, Hash, Result};

use crate::fs::unix_secs;
use crate::pack::{self, MAX_CHAIN};
use crate::packset::{
    lock_tombstones, parse_tombstones, published_ids, sync_dir, tombstone_line, Pack, TOMBSTONES,
};
use crate::packwrite::{
    hint_ids, plan_merge, write_order, write_pack, ObjectTable, PackSource, SourceRead,
};
use crate::{storage_err, FsBlobStore};

/// Held for a whole repack, fence included, so two never overlap. A lock,
/// not a pid file: the kernel releases it when the process dies.
const REPACK_LOCK: &str = "repack.lock";
/// Prefix of a loose copy moved aside while a repack decides whether to
/// delete it. A leftover is a whole object and the next repack puts it back.
const ASIDE_PREFIX: &str = ".repack-";
static TMP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Where a repack is. The hook sees each step before it runs or after it is
/// durable, so returning an error simulates a kill there.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RepackStep {
    /// Locked, garbage from an earlier run cleaned, nothing written yet.
    Started,
    /// This many objects are in the temporary pack file.
    Wrote(u64),
    /// The temporary pack file is complete and fsynced.
    PackSynced,
    /// The pack has its final name; no index refers to it yet.
    PackRenamed,
    /// The temporary index file is complete and fsynced.
    IndexSynced,
    /// The index has its final name: the pack is published.
    Published,
    /// About to move the n-th (from 1) loose copy aside.
    RemovingLoose(u64),
    /// The n-th loose copy is moved aside and about to be deleted.
    LooseMovedAside(u64),
    /// Replaced packs are deleted.
    OldPacksRemoved,
    /// The tombstones file keeps only lines for remaining packs.
    TombstonesRewritten,
}

pub type RepackHook = Arc<dyn Fn(RepackStep) -> Result<()> + Send + Sync>;

/// Which packs a repack replaces.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RepackMode {
    /// Every live object, loose or packed, into one pack; every pack replaced.
    Full,
    /// The loose objects into a new pack, together with the packs
    /// [`plan_merge`](crate::plan_merge) picks, so pack sizes stay a geometric series and the
    /// count stays at most `max_packs`. The other packs are left alone.
    Incremental { factor: u64, max_packs: usize },
}

#[derive(Clone)]
pub struct RepackOptions {
    /// zstd level, 1 to 22.
    pub level: i32,
    /// Longest delta chain written.
    pub max_chain: usize,
    pub mode: RepackMode,
    /// Threads compressing frames; 0 for one per CPU. The pack's bytes do
    /// not depend on it.
    pub threads: usize,
    /// Called at each step; an error aborts the repack where it stands.
    pub hook: Option<RepackHook>,
}

impl Default for RepackOptions {
    fn default() -> Self {
        RepackOptions {
            level: 9,
            max_chain: MAX_CHAIN,
            mode: RepackMode::Full,
            threads: 0,
            hook: None,
        }
    }
}

impl std::fmt::Debug for RepackOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RepackOptions")
            .field("level", &self.level)
            .field("max_chain", &self.max_chain)
            .field("mode", &self.mode)
            .field("threads", &self.threads)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct RepackReport {
    pub loose_before: u64,
    pub loose_bytes_before: u64,
    pub packs_before: u64,
    /// Live objects in those packs, and the packs' size on disk.
    pub packed_before: u64,
    pub pack_bytes_before: u64,
    /// Objects in the new pack, and their total logical size.
    pub objects: u64,
    pub logical_bytes: u64,
    /// Objects stored as a delta against a base, and stored whole.
    pub deltas: u64,
    pub whole: u64,
    /// Packs folded into the new one (every pack, for a full repack).
    pub packs_merged: u64,
    /// Packed objects copied frame for frame: their base did not change.
    pub frames_reused: u64,
    /// `None` when there was nothing to pack.
    pub pack_id: Option<String>,
    pub pack_bytes: u64,
    pub index_bytes: u64,
    /// Packed objects left out because GC had deleted them.
    pub dropped_tombstoned: u64,
    /// Loose files whose bytes do not match their name; left in place.
    pub corrupt: Vec<Hash>,
    /// Listed, then deleted (by GC) before they were read.
    pub vanished: u64,
    /// Deleted by GC while this repack ran, and tombstoned in the new pack.
    pub late_tombstones: u64,
    /// Temp files and unpublished packs of earlier runs deleted, and loose
    /// copies they had moved aside put back.
    pub garbage_removed: u64,
    pub leftovers_restored: u64,
    /// Loose copies deleted because the new pack holds them.
    pub loose_removed: u64,
    /// Loose copies kept: rewritten during the repack, or deleted from the
    /// new pack by GC and uploaded again.
    pub loose_kept: u64,
    pub old_packs_removed: u64,
    pub packs_after: u64,
    pub loose_after: u64,
    pub loose_bytes_after: u64,
}

/// A published pack whose loose copies and replaced packs are still on disk.
/// Call [`finish`](Self::finish) after the fence. Dropping it instead is safe
/// and leaves both copies.
pub struct PublishedPack {
    store: FsBlobStore,
    report: RepackReport,
    /// Objects packed from loose files, with the file mtime that was packed
    /// (see [`stamp_of`]).
    from_loose: Vec<(Hash, i64)>,
    replaced: Vec<String>,
    opts: RepackOptions,
    _lock: File,
}

impl PublishedPack {
    pub fn report(&self) -> &RepackReport {
        &self.report
    }

    /// Delete the loose copies of everything now packed and the replaced
    /// packs, then drop tombstones of packs that are gone.
    pub async fn finish(self) -> Result<RepackReport> {
        tokio::task::spawn_blocking(move || self.finish_blocking())
            .await
            .map_err(|e| Error::Storage(format!("repack task failed: {e}")))?
    }

    fn finish_blocking(mut self) -> Result<RepackReport> {
        let store = self.store.clone();
        let new_id = self.report.pack_id.clone();
        for (n, (hash, packed_mtime)) in std::mem::take(&mut self.from_loose).iter().enumerate() {
            let n = n as u64 + 1;
            step(&self.opts, RepackStep::RemovingLoose(n))?;
            let path = store.path_for(hash);
            let aside = aside_path(&path);
            // Moved aside first, so the checks below look at the file that is
            // then deleted, not one a later upload put at the address.
            match std::fs::rename(&path, &aside) {
                Ok(()) => {}
                Err(e) if e.kind() == ErrorKind::NotFound => continue,
                Err(e) => return Err(storage_err(e)),
            }
            step(&self.opts, RepackStep::LooseMovedAside(n))?;
            let rewritten = stamp_of(mtime(&aside)?) != *packed_mtime;
            // Collect writes its tombstone before it unlinks the loose file,
            // and the upload that rescues the blob comes after the unlink.
            let view = store.packs.fresh_tombstones()?;
            let dead = new_id.as_ref().is_none_or(|id| {
                !view
                    .packs
                    .iter()
                    .any(|(p, dead)| &p.id == id && !dead.contains(hash))
            });
            if rewritten || dead {
                put_back(&aside, &path)?;
                self.report.loose_kept += 1;
            } else {
                remove_if_present(&aside)?;
                self.report.loose_removed += 1;
            }
        }
        let dir = store.packs.dir().to_path_buf();
        for id in &self.replaced {
            // The index first: without it the pack is unpublished garbage.
            remove_if_present(&dir.join(format!("{id}.idx")))?;
            sync_dir(&dir)?;
            remove_if_present(&dir.join(format!("{id}.pack")))?;
            self.report.old_packs_removed += 1;
        }
        sync_dir(&dir)?;
        step(&self.opts, RepackStep::OldPacksRemoved)?;
        rewrite_tombstones(&dir)?;
        step(&self.opts, RepackStep::TombstonesRewritten)?;
        self.report.packs_after = store.packs.refresh()?.packs.len() as u64;
        let (n, bytes) = count_loose(store.root())?;
        self.report.loose_after = n;
        self.report.loose_bytes_after = bytes;
        Ok(self.report)
    }
}

pub(crate) fn step(opts: &RepackOptions, s: RepackStep) -> Result<()> {
    match &opts.hook {
        Some(h) => h(s),
        None => Ok(()),
    }
}

fn mtime(path: &Path) -> Result<Option<SystemTime>> {
    std::fs::metadata(path)
        .map(|m| m.modified().ok())
        .map_err(storage_err)
}

/// A loose copy's mtime as nanoseconds from the epoch, in 8 bytes instead
/// of 16; [`NO_MTIME`] when there is none (or it is out of range).
fn stamp_of(t: Option<SystemTime>) -> i64 {
    let nanos = t.and_then(|t| match t.duration_since(UNIX_EPOCH) {
        Ok(d) => i64::try_from(d.as_nanos()).ok(),
        Err(e) => i64::try_from(e.duration().as_nanos()).ok().map(|n| -n),
    });
    nanos.unwrap_or(NO_MTIME)
}

/// The stamp of an object with no loose copy.
const NOT_LOOSE: i64 = i64::MIN;
const NO_MTIME: i64 = i64::MIN + 1;

/// What the index records for a loose copy with this stamp: its mtime in
/// whole seconds, or now.
fn stamp_secs(stamp: i64) -> i64 {
    if stamp >= 0 {
        stamp / 1_000_000_000
    } else {
        now_secs()
    }
}

fn aside_path(path: &Path) -> PathBuf {
    let name = path.file_name().unwrap().to_string_lossy();
    path.with_file_name(format!("{ASIDE_PREFIX}{name}"))
}

fn remove_if_present(path: &Path) -> Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
        Err(e) => Err(storage_err(e)),
    }
}

/// Return a moved-aside copy to its address, unless an upload already put
/// one there (same bytes, newer mtime: keep that one).
fn put_back(aside: &Path, path: &Path) -> Result<()> {
    if path.exists() {
        return remove_if_present(aside);
    }
    std::fs::rename(aside, path).map_err(storage_err)
}

/// Keep only tombstones of packs that still exist. Under the tombstone lock,
/// so no concurrent delete's line is lost.
fn rewrite_tombstones(dir: &Path) -> Result<()> {
    let _lock = lock_tombstones(dir)?;
    let path = dir.join(TOMBSTONES);
    let text = match std::fs::read(&path) {
        Ok(t) => t,
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(storage_err(e)),
    };
    let live: HashSet<String> = published_ids(dir)?.into_iter().collect();
    let mut keep: Vec<(String, Hash)> = parse_tombstones(&text)
        .into_iter()
        .filter(|(id, _)| live.contains(id))
        .flat_map(|(id, hashes)| hashes.into_iter().map(move |h| (id.clone(), h)))
        .collect();
    keep.sort();
    if keep.is_empty() {
        remove_if_present(&path)?;
        return sync_dir(dir);
    }
    let body: String = keep.iter().map(|(id, h)| tombstone_line(id, h)).collect();
    let tmp = temp_name(dir, "tombstones");
    write_durably(&tmp, body.as_bytes())?;
    std::fs::rename(&tmp, &path).map_err(storage_err)?;
    sync_dir(dir)
}

fn temp_name(dir: &Path, ext: &str) -> PathBuf {
    dir.join(format!(
        ".tmp-{}-{}.{ext}",
        std::process::id(),
        TMP_COUNTER.fetch_add(1, Ordering::Relaxed)
    ))
}

fn write_durably(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut f = File::create(path).map_err(storage_err)?;
    f.write_all(bytes).map_err(storage_err)?;
    f.sync_all().map_err(storage_err)
}

/// Loose objects and their total size.
fn count_loose(root: &Path) -> Result<(u64, u64)> {
    let mut n = 0;
    let mut bytes = 0;
    for o in loose_objects(root, &mut LeftoverCounts::default())? {
        n += 1;
        bytes += o.len;
    }
    Ok((n, bytes))
}

#[derive(Default)]
struct LeftoverCounts {
    restored: u64,
    removed: u64,
    /// Put moved-aside copies back (only under the repack lock).
    restore: bool,
}

struct LooseObject {
    hash: Hash,
    len: u64,
    mtime: Option<SystemTime>,
}

/// Every loose object, in hash order.
fn loose_objects(root: &Path, leftovers: &mut LeftoverCounts) -> Result<Vec<LooseObject>> {
    let mut out = Vec::new();
    for shard in 0..=255u8 {
        let prefix = format!("{shard:02x}");
        let dir = root.join(&prefix);
        let rd = match std::fs::read_dir(&dir) {
            Ok(rd) => rd,
            Err(e) if e.kind() == ErrorKind::NotFound => continue,
            Err(e) => return Err(storage_err(e)),
        };
        for entry in rd {
            let entry = entry.map_err(storage_err)?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if let Some(rest) = name.strip_prefix(ASIDE_PREFIX) {
                if leftovers.restore && format!("{prefix}{rest}").parse::<Hash>().is_ok() {
                    let path = dir.join(rest);
                    if path.exists() {
                        leftovers.removed += 1;
                    } else {
                        leftovers.restored += 1;
                    }
                    put_back(&entry.path(), &path)?;
                    if let Ok(m) = std::fs::metadata(&path) {
                        let hash = format!("{prefix}{rest}").parse().unwrap();
                        out.push(LooseObject {
                            hash,
                            len: m.len(),
                            mtime: m.modified().ok(),
                        });
                    }
                }
                continue;
            }
            let Ok(hash) = format!("{prefix}{name}").parse::<Hash>() else {
                continue;
            };
            let m = match entry.metadata() {
                Ok(m) => m,
                Err(e) if e.kind() == ErrorKind::NotFound => continue,
                Err(e) => return Err(storage_err(e)),
            };
            out.push(LooseObject {
                hash,
                len: m.len(),
                mtime: m.modified().ok(),
            });
        }
    }
    out.sort_by_key(|o| o.hash);
    out.dedup_by_key(|o| o.hash);
    Ok(out)
}

/// Where each object of a repack is read from, by id in its
/// [`ObjectTable`]. A loose copy is preferred: it is live even where the
/// packed copy is tombstoned. 16 bytes per object.
struct Sources {
    /// The loose copy's stamp ([`stamp_of`]), or [`NOT_LOOSE`].
    loose: Vec<i64>,
    /// `pack << 32 | rank` of the packed copy in `packs`, or `u64::MAX`.
    packed: Vec<u64>,
    packs: Vec<Arc<Pack>>,
}

impl Sources {
    fn packed(&self, id: u32) -> Option<(&Arc<Pack>, u32)> {
        match self.packed[id as usize] {
            u64::MAX => None,
            v => Some((&self.packs[(v >> 32) as usize], v as u32)),
        }
    }
}

type Read = SourceRead<i64>;

struct FsSource<'a> {
    store: &'a FsBlobStore,
    sources: &'a Sources,
}

impl PackSource for FsSource<'_> {
    type Stamp = i64;
    fn read(&mut self, id: u32, hash: &Hash) -> Result<Read> {
        read_source(self.store, self.sources, id, hash)
    }

    /// Only for an object read from its pack: a loose copy is what the new
    /// pack records then.
    fn stored_frame(&mut self, id: u32, _hash: &Hash) -> Result<Option<(Vec<u8>, Option<Hash>)>> {
        if self.sources.loose[id as usize] != NOT_LOOSE {
            return Ok(None);
        }
        let Some((p, rank)) = self.sources.packed(id) else {
            return Ok(None);
        };
        let e = p.index.entry(rank);
        let base = e.base.map(|r| p.index.hash_at(r as usize));
        Ok(Some((p.frame(&e)?, base)))
    }
}

fn read_source(store: &FsBlobStore, src: &Sources, id: u32, hash: &Hash) -> Result<Read> {
    let mut corrupt = false;
    let stamp = src.loose[id as usize];
    if stamp != NOT_LOOSE {
        match std::fs::read(store.path_for(hash)) {
            Ok(data) if pack::object_intact(hash, &data) => {
                return Ok(Read::Ok {
                    data: data.into(),
                    modified: stamp_secs(stamp),
                    from_loose: Some(stamp),
                });
            }
            Ok(_) => corrupt = true,
            Err(e) if e.kind() == ErrorKind::NotFound => {}
            Err(e) => return Err(storage_err(e)),
        }
    }
    if let Some((p, rank)) = src.packed(id) {
        let e = p.index.entry(rank);
        let data = store.packs.decode(p, &e)?;
        return Ok(Read::Ok {
            data,
            modified: e.modified,
            from_loose: None,
        });
    }
    Ok(if corrupt {
        Read::Corrupt
    } else {
        Read::Vanished
    })
}

/// Live hashes of `packs` in ascending order, each once, with the first
/// pack (in the order given) that has it and its rank there. A heap merge
/// of the sorted indexes, so it holds nothing per object.
fn merge_packs<'a>(
    packs: &'a [(usize, &'a Pack, &'a HashSet<Hash>)],
) -> impl Iterator<Item = (Hash, usize, u32)> + 'a {
    let mut heap: BinaryHeap<Reverse<(Hash, usize, u32)>> = BinaryHeap::new();
    let next_live = |k: usize, mut rank: u32| -> Option<(Hash, usize, u32)> {
        let (_, p, dead) = packs[k];
        while (rank as usize) < p.index.len() {
            let h = p.index.hash_at(rank as usize);
            if !dead.contains(&h) {
                return Some((h, k, rank));
            }
            rank += 1;
        }
        None
    };
    for k in 0..packs.len() {
        heap.extend(next_live(k, 0).map(Reverse));
    }
    let mut last: Option<Hash> = None;
    std::iter::from_fn(move || loop {
        let Reverse((h, k, rank)) = heap.pop()?;
        heap.extend(next_live(k, rank + 1).map(Reverse));
        if last == Some(h) {
            continue;
        }
        last = Some(h);
        return Some((h, packs[k].0, rank));
    })
}

fn now_secs() -> i64 {
    unix_secs(SystemTime::now()).unwrap_or(0)
}

impl FsBlobStore {
    /// Write every live object into one new pack and publish it. `hints`
    /// lists objects in the order to write them, each with the object to
    /// compress it against; objects the hints miss are written whole after
    /// them. A base is used only if it is already in the new pack, within the
    /// chain cap, and the delta is smaller.
    ///
    /// Refuses with `Conflict` while another repack holds the store.
    pub async fn repack_publish(
        &self,
        hints: impl Into<PackHints>,
        opts: RepackOptions,
    ) -> Result<PublishedPack> {
        let store = self.clone();
        let hints = hints.into();
        tokio::task::spawn_blocking(move || store.publish_blocking(hints, opts))
            .await
            .map_err(|e| Error::Storage(format!("repack task failed: {e}")))?
    }

    fn publish_blocking(&self, hints: PackHints, opts: RepackOptions) -> Result<PublishedPack> {
        if !(1..=22).contains(&opts.level) {
            return Err(Error::Invalid(format!(
                "zstd level {} is outside 1..=22",
                opts.level
            )));
        }
        let dir = self.packs.dir().to_path_buf();
        std::fs::create_dir_all(&dir).map_err(storage_err)?;
        let lock = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(dir.join(REPACK_LOCK))
            .map_err(storage_err)?;
        match lock.try_lock() {
            Ok(()) => {}
            Err(std::fs::TryLockError::WouldBlock) => {
                return Err(Error::Conflict(
                    "another repack is running on this blob store".into(),
                ))
            }
            Err(std::fs::TryLockError::Error(e)) => return Err(storage_err(e)),
        }
        let mut report = RepackReport {
            garbage_removed: remove_unpublished(&dir)?,
            ..RepackReport::default()
        };
        step(&opts, RepackStep::Started)?;

        let view = self.packs.refresh()?;
        let mut leftovers = LeftoverCounts {
            restore: true,
            ..LeftoverCounts::default()
        };
        let loose = loose_objects(self.root(), &mut leftovers)?;
        report.leftovers_restored = leftovers.restored;
        report.garbage_removed += leftovers.removed;
        report.packs_before = view.packs.len() as u64;
        let live = |dead: &HashSet<Hash>, p: &Pack| p.index.len().saturating_sub(dead.len()) as u64;
        let merged: HashSet<String> = match opts.mode {
            RepackMode::Full => view.packs.iter().map(|(p, _)| p.id.clone()).collect(),
            RepackMode::Incremental { factor, max_packs } => {
                if factor < 2 || max_packs < 1 {
                    return Err(Error::Invalid(format!(
                        "geometric factor {factor} must be at least 2 and max packs {max_packs} at least 1"
                    )));
                }
                let sizes: Vec<(String, u64)> = view
                    .packs
                    .iter()
                    .map(|(p, dead)| (p.id.clone(), live(dead, p)))
                    .collect();
                plan_merge(&sizes, loose.len() as u64, factor, max_packs)
                    .into_iter()
                    .collect()
            }
        };
        report.packs_merged = merged.len() as u64;
        let all: Vec<(usize, &Pack, &HashSet<Hash>)> = view
            .packs
            .iter()
            .enumerate()
            .map(|(i, (p, dead))| (i, &**p, dead))
            .collect();
        report.pack_bytes_before = all.iter().map(|(_, p, _)| p.index.pack_len()).sum();
        report.packed_before = merge_packs(&all).count() as u64;
        let merging: Vec<(usize, &Pack, &HashSet<Hash>)> = all
            .iter()
            .copied()
            .filter(|(_, p, _)| merged.contains(&p.id))
            .collect();

        // Every object to pack, in hash order, from the merged packs and the
        // loose listing at once; an object's id is its position.
        let most = loose.len() + merging.iter().map(|(_, p, _)| p.index.len()).sum::<usize>();
        let mut hashes: Vec<Hash> = Vec::with_capacity(most);
        let mut sources = Sources {
            loose: Vec::with_capacity(most),
            packed: Vec::with_capacity(most),
            packs: view.packs.iter().map(|(p, _)| p.clone()).collect(),
        };
        {
            let mut packed = merge_packs(&merging).peekable();
            let mut loose = loose.into_iter().peekable();
            loop {
                let take_packed = match (packed.peek(), loose.peek()) {
                    (None, None) => break,
                    (Some(_), None) => true,
                    (None, Some(_)) => false,
                    (Some(p), Some(l)) => p.0 <= l.hash,
                };
                let take_loose = match (packed.peek(), loose.peek()) {
                    (_, None) => false,
                    (None, Some(_)) => true,
                    (Some(p), Some(l)) => l.hash <= p.0,
                };
                let mut hash = None;
                let mut src_packed = u64::MAX;
                let mut src_loose = NOT_LOOSE;
                if take_packed {
                    let (h, k, rank) = packed.next().unwrap();
                    hash = Some(h);
                    src_packed = (k as u64) << 32 | u64::from(rank);
                }
                if take_loose {
                    let o = loose.next().unwrap();
                    report.loose_before += 1;
                    report.loose_bytes_before += o.len;
                    hash = Some(o.hash);
                    src_loose = stamp_of(o.mtime);
                }
                hashes.push(hash.unwrap());
                sources.packed.push(src_packed);
                sources.loose.push(src_loose);
            }
        }
        hashes.shrink_to_fit();
        sources.loose.shrink_to_fit();
        sources.packed.shrink_to_fit();
        let table = ObjectTable::from_sorted(hashes)?;
        let mut tombstoned: Vec<Hash> = merging
            .iter()
            .flat_map(|(_, _, dead)| dead.iter().copied())
            .collect();
        tombstoned.sort_unstable();
        tombstoned.dedup();
        report.dropped_tombstoned =
            tombstoned.iter().filter(|h| table.id(h).is_none()).count() as u64;
        drop(tombstoned);

        // Objects of the merged packs in each pack's write order, with the
        // base each had there, so a merge keeps the deltas it had.
        let mut old_bases: Vec<(u32, u32)> = Vec::new();
        for &(k, p, dead) in &merging {
            let mut in_order: Vec<(u64, u32, u32)> = Vec::new();
            for rank in 0..p.index.len() as u32 {
                let h = p.index.hash_at(rank as usize);
                if dead.contains(&h) {
                    continue;
                }
                let Some(id) = table.id(&h) else { continue };
                if sources.packed[id as usize] != (k as u64) << 32 | u64::from(rank) {
                    continue;
                }
                let e = p.index.entry(rank);
                let base = e.base.map_or(NO_ID, |r| {
                    table.id(&p.index.hash_at(r as usize)).unwrap_or(NO_ID)
                });
                in_order.push((e.offset, id, base));
            }
            in_order.sort_unstable();
            old_bases.extend(in_order.into_iter().map(|(_, id, base)| (id, base)));
        }
        drop(merging);
        drop(all);

        // A full repack trusts the walk first; an incremental one writes the
        // merged packs first, so loose objects can use them as bases.
        let hints = hint_ids(&table, hints);
        let order = match opts.mode {
            RepackMode::Full => write_order(&table, hints, old_bases),
            RepackMode::Incremental { .. } => write_order(&table, old_bases, hints),
        };

        let tmp_pack = temp_name(&dir, "pack");
        let mut out =
            BufWriter::with_capacity(1 << 20, File::create(&tmp_pack).map_err(storage_err)?);
        let mut src = FsSource {
            store: self,
            sources: &sources,
        };
        let written = write_pack(&table, order, &mut src, &opts, &mut out, &mut report)?;
        let from_loose: Vec<(Hash, i64)> = written
            .from_loose
            .iter()
            .map(|&(id, stamp)| (*table.hash(id), stamp))
            .collect();
        let file = out.into_inner().map_err(|e| storage_err(e.error()))?;
        file.sync_all().map_err(storage_err)?;
        drop(file);
        step(&opts, RepackStep::PackSynced)?;
        let replaced: Vec<String> = view
            .packs
            .iter()
            .map(|(p, _)| p.id.clone())
            .filter(|id| merged.contains(id))
            .collect();

        if written.records.is_empty() {
            remove_if_present(&tmp_pack)?;
            return Ok(PublishedPack {
                store: self.clone(),
                report,
                from_loose,
                replaced,
                opts,
                _lock: lock,
            });
        }
        // The index is encoded twice, once for its id and once into its
        // file, so it is never all in memory.
        let mut id_hasher = blake3::Hasher::new();
        let mut index_len = 0u64;
        written.write_index(&table, &mut |b: &[u8]| {
            id_hasher.update(b);
            index_len += b.len() as u64;
            Ok(())
        })?;
        let id = hex::encode(id_hasher.finalize().as_bytes());
        std::fs::rename(&tmp_pack, dir.join(format!("{id}.pack"))).map_err(storage_err)?;
        sync_dir(&dir)?;
        step(&opts, RepackStep::PackRenamed)?;
        let tmp_idx = temp_name(&dir, "idx");
        {
            let mut f =
                BufWriter::with_capacity(1 << 20, File::create(&tmp_idx).map_err(storage_err)?);
            let mut check = blake3::Hasher::new();
            written.write_index(&table, &mut |b: &[u8]| {
                check.update(b);
                f.write_all(b).map_err(storage_err)
            })?;
            if hex::encode(check.finalize().as_bytes()) != id {
                return Err(Error::Storage(
                    "pack index encoded differently twice".into(),
                ));
            }
            let f = f.into_inner().map_err(|e| storage_err(e.error()))?;
            f.sync_all().map_err(storage_err)?;
        }
        step(&opts, RepackStep::IndexSynced)?;
        std::fs::rename(&tmp_idx, dir.join(format!("{id}.idx"))).map_err(storage_err)?;
        sync_dir(&dir)?;
        step(&opts, RepackStep::Published)?;
        report.pack_bytes = written.len;
        report.index_bytes = index_len;
        report.pack_id = Some(id.clone());

        // GC may have deleted objects after this run read them. Hide them in
        // the new pack too; a delete that starts from now on sees the pack.
        let late = self.late_deletes(&table, written.ids(), &sources)?;
        if !late.is_empty() {
            let lines: String = late.iter().map(|h| tombstone_line(&id, h)).collect();
            self.packs.append_tombstones(&lines)?;
            report.late_tombstones = late.len() as u64;
        }
        // Loading the new index holds it beside the old ones; the per-object
        // arrays of this run go first.
        drop((written, sources, table, view));
        self.packs.refresh()?;
        Ok(PublishedPack {
            store: self.clone(),
            report,
            from_loose,
            replaced: replaced.into_iter().filter(|r| *r != id).collect(),
            opts,
            _lock: lock,
        })
    }

    /// Objects of the new pack whose source GC deleted while it was written:
    /// a loose file that is gone, or a packed copy tombstoned since.
    fn late_deletes(
        &self,
        table: &ObjectTable,
        packed: impl Iterator<Item = u32>,
        sources: &Sources,
    ) -> Result<Vec<Hash>> {
        let tombstones = match std::fs::read(self.packs.dir().join(TOMBSTONES)) {
            Ok(t) => parse_tombstones(&t),
            Err(e) if e.kind() == ErrorKind::NotFound => Default::default(),
            Err(e) => return Err(storage_err(e)),
        };
        let mut late = Vec::new();
        for id in packed {
            let hash = table.hash(id);
            let gone = if sources.loose[id as usize] != NOT_LOOSE {
                !self.path_for(hash).exists()
            } else if let Some((p, _)) = sources.packed(id) {
                tombstones.get(&p.id).is_some_and(|d| d.contains(hash))
            } else {
                false
            };
            if gone {
                late.push(*hash);
            }
        }
        Ok(late)
    }
}

/// Delete what a killed repack left in `packs/`: temp files, and packs whose
/// index was never published. Only under the repack lock.
fn remove_unpublished(dir: &Path) -> Result<u64> {
    let ids: HashSet<String> = published_ids(dir)?.into_iter().collect();
    let mut removed = 0;
    for entry in std::fs::read_dir(dir).map_err(storage_err)? {
        let entry = entry.map_err(storage_err)?;
        let name = entry.file_name().to_string_lossy().into_owned();
        let garbage = name.starts_with(".tmp-")
            || name
                .strip_suffix(".pack")
                .is_some_and(|id| !ids.contains(id));
        if garbage {
            remove_if_present(&entry.path())?;
            removed += 1;
        }
    }
    if removed > 0 {
        sync_dir(dir)?;
    }
    Ok(removed)
}
