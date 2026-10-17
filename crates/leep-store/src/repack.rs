//! Full repack of a filesystem store (ADR 0029): every live loose and packed
//! object goes into one new pack, which is made durable and published; after
//! a fence the loose copies and the replaced packs are deleted.
//!
//! The two halves are separate calls ([`FsBlobStore::repack_publish`] and
//! [`PublishedPack::finish`]) so the caller waits out the fence in between.
//! A run killed anywhere leaves a readable store: before the index rename
//! nothing is published (the next repack deletes the temp files), and after
//! it every object is in the new pack, perhaps also loose or in an old pack.

use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufWriter, ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::SystemTime;

use bytes::Bytes;
use leep_core::{Error, Hash, Result};

use crate::cache::Lru;
use crate::fs::unix_secs;
use crate::pack::{self, IdxEntry, PackIndex, MAX_CHAIN, PACK_MAGIC};
use crate::packset::{
    lock_tombstones, parse_tombstones, published_ids, sync_dir, tombstone_line, Pack, TOMBSTONES,
};
use crate::{storage_err, FsBlobStore};

/// Held for a whole repack, fence included, so two never overlap. A lock,
/// not a pid file: the kernel releases it when the process dies.
const REPACK_LOCK: &str = "repack.lock";
/// Prefix of a loose copy moved aside while a repack decides whether to
/// delete it. A leftover is a whole object and the next repack puts it back.
const ASIDE_PREFIX: &str = ".repack-";
/// Recently written objects kept as delta bases while writing a pack.
const BASE_CACHE: usize = 256 << 20;

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

#[derive(Clone)]
pub struct RepackOptions {
    /// zstd level, 1 to 22.
    pub level: i32,
    /// Longest delta chain written.
    pub max_chain: usize,
    /// Called at each step; an error aborts the repack where it stands.
    pub hook: Option<RepackHook>,
}

impl Default for RepackOptions {
    fn default() -> Self {
        RepackOptions {
            level: 9,
            max_chain: MAX_CHAIN,
            hook: None,
        }
    }
}

impl std::fmt::Debug for RepackOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RepackOptions")
            .field("level", &self.level)
            .field("max_chain", &self.max_chain)
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
    pub loose_after: u64,
    pub loose_bytes_after: u64,
}

/// A published pack whose loose copies and replaced packs are still on disk.
/// Call [`finish`](Self::finish) after the fence. Dropping it instead is safe
/// and leaves both copies.
pub struct PublishedPack {
    store: FsBlobStore,
    report: RepackReport,
    /// Objects packed from loose files, with the file mtime that was packed.
    from_loose: Vec<(Hash, Option<SystemTime>)>,
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
            let rewritten = mtime(&aside)? != *packed_mtime;
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
        store.packs.refresh()?;
        let (n, bytes) = count_loose(store.root())?;
        self.report.loose_after = n;
        self.report.loose_bytes_after = bytes;
        Ok(self.report)
    }
}

fn step(opts: &RepackOptions, s: RepackStep) -> Result<()> {
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
    path: PathBuf,
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
                            path,
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
                path: entry.path(),
                len: m.len(),
                mtime: m.modified().ok(),
            });
        }
    }
    out.sort_by_key(|o| o.hash);
    out.dedup_by_key(|o| o.hash);
    Ok(out)
}

/// Where a repack reads an object from. A loose copy is preferred: it is
/// live even where the packed copy is tombstoned.
#[derive(Default)]
struct Source {
    loose: Option<(PathBuf, Option<SystemTime>)>,
    packed: Option<(Arc<Pack>, IdxEntry)>,
}

enum Read {
    Ok {
        data: Bytes,
        modified: i64,
        from_loose: Option<Option<SystemTime>>,
    },
    Vanished,
    Corrupt,
}

fn read_source(store: &FsBlobStore, src: &Source, hash: &Hash) -> Result<Read> {
    let mut corrupt = false;
    if let Some((path, mtime)) = &src.loose {
        match std::fs::read(path) {
            Ok(data) if pack::object_intact(hash, &data) => {
                let modified = mtime.and_then(unix_secs).unwrap_or_else(now_secs);
                return Ok(Read::Ok {
                    data: data.into(),
                    modified,
                    from_loose: Some(*mtime),
                });
            }
            Ok(_) => corrupt = true,
            Err(e) if e.kind() == ErrorKind::NotFound => {}
            Err(e) => return Err(storage_err(e)),
        }
    }
    if let Some((p, e)) = &src.packed {
        let data = store.packs.decode(p, e)?;
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
        hints: Vec<(Hash, Option<Hash>)>,
        opts: RepackOptions,
    ) -> Result<PublishedPack> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || store.publish_blocking(hints, opts))
            .await
            .map_err(|e| Error::Storage(format!("repack task failed: {e}")))?
    }

    fn publish_blocking(
        &self,
        hints: Vec<(Hash, Option<Hash>)>,
        opts: RepackOptions,
    ) -> Result<PublishedPack> {
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
                    s.packed = Some((p.clone(), e));
                    report.packed_before += 1;
                }
            }
        }
        let tombstoned: HashSet<Hash> = view
            .packs
            .iter()
            .flat_map(|(_, dead)| dead.iter().copied())
            .collect();
        for o in loose {
            report.loose_before += 1;
            report.loose_bytes_before += o.len;
            sources.entry(o.hash).or_default().loose = Some((o.path, o.mtime));
        }
        report.dropped_tombstoned = tombstoned
            .iter()
            .filter(|h| !sources.contains_key(h))
            .count() as u64;

        let mut order: Vec<(Hash, Option<Hash>)> = Vec::with_capacity(sources.len());
        let mut placed: HashSet<Hash> = HashSet::with_capacity(sources.len());
        for (h, base) in hints {
            if sources.contains_key(&h) && placed.insert(h) {
                order.push((h, base));
            }
        }
        let mut rest: Vec<Hash> = sources
            .keys()
            .filter(|h| !placed.contains(*h))
            .copied()
            .collect();
        rest.sort();
        order.extend(rest.into_iter().map(|h| (h, None)));

        let tmp_pack = temp_name(&dir, "pack");
        let mut out =
            BufWriter::with_capacity(1 << 20, File::create(&tmp_pack).map_err(storage_err)?);
        let mut hasher = blake3::Hasher::new();
        let mut offset = 0u64;
        let mut emit = |bytes: &[u8], out: &mut BufWriter<File>| -> Result<u64> {
            out.write_all(bytes).map_err(storage_err)?;
            hasher.update(bytes);
            let at = offset;
            offset += bytes.len() as u64;
            Ok(at)
        };
        emit(&PACK_MAGIC, &mut out)?;
        // In pack order; a base is a position in this list.
        let mut entries: Vec<IdxEntry> = Vec::with_capacity(order.len());
        // Position in `entries` and chain depth of each object written.
        let mut written: HashMap<Hash, (u32, usize)> = HashMap::with_capacity(order.len());
        let mut bases = Lru::default();
        let mut from_loose = Vec::new();
        for (hash, base) in order {
            let src = &sources[&hash];
            let (data, modified, loose_mtime) = match read_source(self, src, &hash)? {
                Read::Ok {
                    data,
                    modified,
                    from_loose,
                } => (data, modified, from_loose),
                Read::Vanished => {
                    report.vanished += 1;
                    continue;
                }
                Read::Corrupt => {
                    report.corrupt.push(hash);
                    continue;
                }
            };
            let mut frame = pack::compress_whole(&data, opts.level)?;
            let mut used = None;
            let mut d = 0;
            if let Some(b) = base.filter(|b| *b != hash) {
                if let Some(&(bp, bd)) = written.get(&b).filter(|(_, bd)| *bd < opts.max_chain) {
                    let base_bytes = match bases.get(&b) {
                        Some(x) => Some(x),
                        None => match read_source(self, &sources[&b], &b)? {
                            Read::Ok { data, .. } => Some(data),
                            _ => None,
                        },
                    };
                    if let Some(bb) = base_bytes {
                        if let Some(delta) = pack::compress_delta(&bb, &data, opts.level)? {
                            if delta.len() < frame.len() {
                                frame = delta;
                                used = Some(bp);
                                d = bd + 1;
                            }
                        }
                    }
                }
            }
            let at = emit(&frame, &mut out)?;
            entries.push(IdxEntry {
                hash,
                offset: at,
                stored_len: frame.len() as u64,
                len: data.len() as u64,
                base: used,
                modified,
            });
            if used.is_some() {
                report.deltas += 1;
            } else {
                report.whole += 1;
            }
            report.logical_bytes += data.len() as u64;
            written.insert(hash, (entries.len() as u32 - 1, d));
            bases.insert(hash, data, BASE_CACHE);
            if let Some(m) = loose_mtime {
                from_loose.push((hash, m));
            }
            step(&opts, RepackStep::Wrote(entries.len() as u64))?;
        }
        let file = out.into_inner().map_err(|e| storage_err(e.error()))?;
        file.sync_all().map_err(storage_err)?;
        drop(file);
        step(&opts, RepackStep::PackSynced)?;
        report.objects = entries.len() as u64;
        let replaced: Vec<String> = view.packs.iter().map(|(p, _)| p.id.clone()).collect();

        if entries.is_empty() {
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
        let idx_bytes = PackIndex::encode(offset, *hasher.finalize().as_bytes(), &entries)?;
        let id = pack::pack_id(&idx_bytes);
        std::fs::rename(&tmp_pack, dir.join(format!("{id}.pack"))).map_err(storage_err)?;
        sync_dir(&dir)?;
        step(&opts, RepackStep::PackRenamed)?;
        let tmp_idx = temp_name(&dir, "idx");
        write_durably(&tmp_idx, &idx_bytes)?;
        step(&opts, RepackStep::IndexSynced)?;
        std::fs::rename(&tmp_idx, dir.join(format!("{id}.idx"))).map_err(storage_err)?;
        sync_dir(&dir)?;
        step(&opts, RepackStep::Published)?;
        report.pack_bytes = offset;
        report.index_bytes = idx_bytes.len() as u64;
        report.pack_id = Some(id.clone());

        // GC may have deleted objects after this run read them. Hide them in
        // the new pack too; a delete that starts from now on sees the pack.
        let late = self.late_deletes(entries.iter().map(|e| e.hash), &sources)?;
        if !late.is_empty() {
            let lines: String = late.iter().map(|h| tombstone_line(&id, h)).collect();
            self.packs.append_tombstones(&lines)?;
            report.late_tombstones = late.len() as u64;
        }
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
        packed: impl Iterator<Item = Hash>,
        sources: &HashMap<Hash, Source>,
    ) -> Result<Vec<Hash>> {
        let tombstones = match std::fs::read(self.packs.dir().join(TOMBSTONES)) {
            Ok(t) => parse_tombstones(&t),
            Err(e) if e.kind() == ErrorKind::NotFound => Default::default(),
            Err(e) => return Err(storage_err(e)),
        };
        let mut late = Vec::new();
        for hash in packed {
            let src = &sources[&hash];
            let gone = match (&src.loose, &src.packed) {
                (Some((path, _)), _) => !path.exists(),
                (None, Some((p, _))) => tombstones.get(&p.id).is_some_and(|d| d.contains(&hash)),
                (None, None) => false,
            };
            if gone {
                late.push(hash);
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
