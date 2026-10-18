//! The format-writing core of a repack, shared by the filesystem and object
//! store backends: choose the write order, then stream every object into a
//! `.pack` as a whole or delta frame and collect its index records. Where the
//! bytes come from and where the pack goes are the backend's business.

use std::collections::BTreeMap;
use std::io::Write;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{mpsc, Condvar, Mutex};

use bytes::Bytes;
use leep_core::intern::{PackHints, NO_ID};
use leep_core::{Error, Hash, Result};

use crate::pack::{self, IdxEntry, PackIndex, PACK_MAGIC};
use crate::repack::step;
use crate::{storage_err, RepackOptions, RepackReport, RepackStep};

/// Recently written objects kept as delta bases while writing a pack.
const BASE_CACHE: usize = 256 << 20;
/// Bytes (objects and bases) read ahead of the object being written, at
/// most, unless one batch alone is larger.
const AHEAD_BYTES: u64 = 64 << 20;

/// One object as a repack source read it.
pub(crate) enum SourceRead<S> {
    Ok {
        data: Bytes,
        /// Unix seconds, for the index record.
        modified: i64,
        /// Set when read from a loose copy: what identifies that copy, so
        /// cleanup deletes it only if nothing rewrote it since.
        from_loose: Option<S>,
    },
    /// Deleted (by GC) after it was listed.
    Vanished,
    /// A loose copy whose bytes do not match its name, and no packed copy.
    Corrupt,
}

/// Where a repack reads objects from. Objects are named by their id in the
/// repack's [`ObjectTable`], and by hash.
pub(crate) trait PackSource {
    type Stamp;
    fn read(&mut self, id: u32, hash: &Hash) -> Result<SourceRead<Self::Stamp>>;
    /// The frame the object already has in an old pack and that frame's
    /// base, when [`read`](Self::read) reads it from there. A delta frame
    /// decodes the same against the same base in any pack, so a writer that
    /// gives it the same base copies it instead of compressing again.
    fn stored_frame(&mut self, _id: u32, _hash: &Hash) -> Result<Option<(Vec<u8>, Option<Hash>)>> {
        Ok(None)
    }
}

/// The live objects of one repack, sorted by hash: an object's id is its
/// rank. Every per-object structure of a repack is an array indexed by id
/// or holds ids, so each hash is held once (ADR 0029, "Implementation").
pub(crate) struct ObjectTable {
    hashes: Vec<Hash>,
    /// Index of the first hash of each two-byte prefix, and the length.
    fanout: Vec<u32>,
}

impl ObjectTable {
    /// `hashes` must be sorted and distinct.
    pub fn from_sorted(hashes: Vec<Hash>) -> Result<ObjectTable> {
        if hashes.len() >= NO_ID as usize {
            return Err(Error::Storage("too many objects for one pack".into()));
        }
        debug_assert!(hashes.windows(2).all(|w| w[0] < w[1]));
        let mut fanout = vec![0u32; (1 << 16) + 1];
        for h in &hashes {
            fanout[Self::prefix(h) + 1] += 1;
        }
        for i in 0..1 << 16 {
            fanout[i + 1] += fanout[i];
        }
        Ok(ObjectTable { hashes, fanout })
    }

    fn prefix(h: &Hash) -> usize {
        usize::from(h.0[0]) << 8 | usize::from(h.0[1])
    }

    pub fn len(&self) -> usize {
        self.hashes.len()
    }

    pub fn hash(&self, id: u32) -> &Hash {
        &self.hashes[id as usize]
    }

    pub fn id(&self, hash: &Hash) -> Option<u32> {
        let p = Self::prefix(hash);
        let (lo, hi) = (self.fanout[p] as usize, self.fanout[p + 1] as usize);
        self.hashes[lo..hi]
            .binary_search(hash)
            .ok()
            .map(|i| (lo + i) as u32)
    }
}

/// `hints` with each hash replaced by its id in `table`; objects not in the
/// table are left out, and bases not in it become [`NO_ID`]. Reuses the
/// hints' own buffer.
pub(crate) fn hint_ids(table: &ObjectTable, hints: PackHints) -> Vec<(u32, u32)> {
    let map: Vec<u32> = hints
        .hashes
        .iter()
        .map(|h| table.id(h).unwrap_or(NO_ID))
        .collect();
    drop(hints.hashes);
    let mut objects = hints.objects;
    objects.retain_mut(|(o, b)| {
        *o = map[*o as usize];
        if *b != NO_ID {
            *b = map[*b as usize];
        }
        *o != NO_ID
    });
    objects.shrink_to_fit();
    objects
}

/// Every object of `table` once, in write order: as `first` and then
/// `second` list them (each with its suggested base; the first listing of
/// an object wins), then the rest whole, in hash order.
pub(crate) fn write_order(
    table: &ObjectTable,
    first: Vec<(u32, u32)>,
    second: Vec<(u32, u32)>,
) -> Vec<(u32, u32)> {
    let n = table.len();
    let mut placed = vec![0u64; n.div_ceil(64)];
    let mut order = Vec::with_capacity(n);
    for list in [first, second] {
        for (id, base) in list {
            let (w, bit) = (id as usize / 64, 1u64 << (id % 64));
            if placed[w] & bit == 0 {
                placed[w] |= bit;
                order.push((id, base));
            }
        }
    }
    for id in 0..n as u32 {
        if placed[id as usize / 64] & (1 << (id % 64)) == 0 {
            order.push((id, NO_ID));
        }
    }
    order
}

/// The entries as written, in pack order, one array per field (28 bytes
/// an entry). A frame's length is the distance to the next entry (or the
/// pack's end).
#[derive(Default)]
pub(crate) struct Records {
    offset: Vec<u64>,
    len: Vec<u64>,
    modified: Vec<i64>,
    /// The base's id, or [`NO_ID`].
    base: Vec<u32>,
}

impl Records {
    pub fn len(&self) -> usize {
        self.offset.len()
    }

    pub fn is_empty(&self) -> bool {
        self.offset.is_empty()
    }

    fn push(&mut self, base: u32, offset: u64, len: u64, modified: i64) {
        self.offset.push(offset);
        self.len.push(len);
        self.modified.push(modified);
        self.base.push(base);
    }
}

/// A pack as written, before its index is encoded.
pub(crate) struct WrittenPack<S> {
    pub records: Records,
    /// Position in `records` of each id, or [`NO_ID`] if not written.
    pub pos: Vec<u32>,
    pub len: u64,
    pub hash: [u8; 32],
    /// Ids packed from loose copies, with each copy's stamp.
    pub from_loose: Vec<(u32, S)>,
}

impl<S> WrittenPack<S> {
    /// Ids written, in hash order.
    pub fn ids(&self) -> impl Iterator<Item = u32> + '_ {
        (0..self.pos.len() as u32).filter(|&id| self.pos[id as usize] != NO_ID)
    }

    /// The `LEEPIDX2` bytes for this pack: the same bytes
    /// [`PackIndex::encode`] gives for its entries.
    pub fn encode_index(&self, table: &ObjectTable) -> Result<Vec<u8>> {
        let mut out = Vec::new();
        self.write_index(table, &mut |b: &[u8]| {
            out.extend_from_slice(b);
            Ok(())
        })?;
        Ok(out)
    }

    /// [`encode_index`](Self::encode_index), in pieces to `sink`.
    pub fn write_index(
        &self,
        table: &ObjectTable,
        sink: &mut dyn FnMut(&[u8]) -> Result<()>,
    ) -> Result<()> {
        // An entry's rank is the number of written ids below it: one count
        // per 64 ids, then the bits below it in its word.
        let words = self.pos.len().div_ceil(64);
        let mut bits = vec![0u64; words];
        for id in self.ids() {
            bits[id as usize / 64] |= 1 << (id % 64);
        }
        let mut below = Vec::with_capacity(words);
        let mut total = 0u32;
        for w in &bits {
            below.push(total);
            total += w.count_ones();
        }
        let rank_of = |id: u32| {
            let (w, b) = (id as usize / 64, id % 64);
            below[w] + (bits[w] & ((1u64 << b) - 1)).count_ones()
        };
        let ranked: Vec<u32> = self.ids().collect();
        let entry = |r: usize| {
            let id = ranked[r];
            let p = self.pos[id as usize] as usize;
            let end = self.records.offset.get(p + 1).copied().unwrap_or(self.len);
            let base = self.records.base[p];
            IdxEntry {
                hash: *table.hash(id),
                offset: self.records.offset[p],
                stored_len: end - self.records.offset[p],
                len: self.records.len[p],
                base: (base != NO_ID).then(|| rank_of(base)),
                modified: self.records.modified[p],
            }
        };
        PackIndex::encode_sorted_to(self.len, self.hash, ranked.len(), entry, sink)
    }
}

/// Chain depth of an object not decided yet: in flight or not reached.
const UNDECIDED: u8 = u8::MAX;
/// Of an object that was not written (vanished or corrupt).
const SKIPPED: u8 = u8::MAX - 1;

/// One object the reader hands the compression threads.
struct Job {
    id: u32,
    data: Bytes,
    modified: i64,
    kind: JobKind,
}

enum JobKind {
    /// Copy an old frame: its base (or [`NO_ID`]) and chain depth are known.
    Reuse {
        frame: Vec<u8>,
        base: u32,
        depth: u8,
    },
    /// Compress whole, and against `base` if its bytes were read; the
    /// writer picks.
    Compress {
        base: u32,
        base_bytes: Option<Bytes>,
    },
}

/// Consecutive jobs handed over together: a thread wakeup per object cost
/// a third of a repack of small objects.
struct Batch<T> {
    seq: u64,
    /// Bytes held for these jobs, released when they are written.
    held: u64,
    items: Vec<T>,
}

const BATCH_JOBS: usize = 64;
const BATCH_BYTES: u64 = 4 << 20;

struct Done {
    id: u32,
    len: u64,
    modified: i64,
    out: Frames,
}

enum Frames {
    Reused {
        frame: Vec<u8>,
        base: u32,
        depth: u8,
    },
    Compressed {
        whole: Vec<u8>,
        base: u32,
        delta: Option<Vec<u8>>,
    },
}

/// What the writer has finished, for the reader's flow control.
#[derive(Default)]
struct Progress {
    /// Batches written.
    written: u64,
    held: u64,
}

struct Shared {
    depth: Vec<AtomicU8>,
    progress: Mutex<Progress>,
    changed: Condvar,
    stop: AtomicBool,
}

impl Shared {
    fn stop(&self) {
        self.stop.store(true, Ordering::SeqCst);
        let _guard = self.progress.lock().unwrap();
        self.changed.notify_all();
    }

    fn stopped(&self) -> bool {
        self.stop.load(Ordering::SeqCst)
    }

    /// Wait until `id`'s chain depth is decided; `None` if stopped.
    fn wait_depth(&self, id: u32) -> Option<u8> {
        let mut p = self.progress.lock().unwrap();
        loop {
            let d = self.depth[id as usize].load(Ordering::SeqCst);
            if d != UNDECIDED {
                return Some(d);
            }
            if self.stopped() {
                return None;
            }
            p = self.changed.wait(p).unwrap();
        }
    }

    /// Wait for room to hand out batch `seq` holding `held` bytes; `false`
    /// if stopped.
    fn wait_room(&self, seq: u64, held: u64, ahead_batches: u64) -> bool {
        let mut p = self.progress.lock().unwrap();
        loop {
            if self.stopped() {
                return false;
            }
            let ahead = seq - p.written;
            if ahead == 0 || (ahead < ahead_batches && p.held + held <= AHEAD_BYTES) {
                p.held += held;
                return true;
            }
            p = self.changed.wait(p).unwrap();
        }
    }
}

/// Counts the writer thread keeps, added to the report at the end.
#[derive(Default)]
struct Counts {
    deltas: u64,
    whole: u64,
    frames_reused: u64,
    logical_bytes: u64,
}

/// Write `order` to `out` as a `.pack`, counting what was written in
/// `report`. A base is used only if it is already in this pack, within the
/// chain cap, and the delta is smaller than the whole frame. The caller
/// flushes and makes `out` durable.
///
/// Reads run in order on this thread; compression runs on
/// `opts.threads` threads, both candidate frames of an object at once, and
/// a writer thread decides between them in order. Every decision depends
/// only on the objects before it, so the bytes are those of a sequential
/// writer, however many threads run.
pub(crate) fn write_pack<W: Write + Send, S: PackSource>(
    table: &ObjectTable,
    order: Vec<(u32, u32)>,
    src: &mut S,
    opts: &RepackOptions,
    out: &mut W,
    report: &mut RepackReport,
) -> Result<WrittenPack<S::Stamp>> {
    let threads = match opts.threads {
        0 => std::thread::available_parallelism().map_or(1, |n| n.get()),
        n => n,
    }
    .max(1);
    let shared = Shared {
        depth: (0..table.len()).map(|_| AtomicU8::new(UNDECIDED)).collect(),
        progress: Mutex::new(Progress::default()),
        changed: Condvar::new(),
        stop: AtomicBool::new(false),
    };
    let max_chain = opts.max_chain.min(usize::from(SKIPPED) - 1) as u8;
    let (job_tx, job_rx) = mpsc::sync_channel::<Batch<Job>>(threads * 2);
    let job_rx = Mutex::new(job_rx);
    let (done_tx, done_rx) = mpsc::channel::<Result<Batch<Done>>>();
    let mut from_loose = Vec::new();
    let expected = order.len();

    let (read_res, write_res) = std::thread::scope(|scope| {
        for _ in 0..threads {
            let (job_rx, done_tx, shared) = (&job_rx, done_tx.clone(), &shared);
            scope.spawn(move || {
                let mut zstd = pack::Compressor::default();
                loop {
                    let batch = match job_rx.lock().unwrap().recv() {
                        Ok(b) => b,
                        Err(_) => return,
                    };
                    if shared.stopped() {
                        continue;
                    }
                    let items: Result<Vec<Done>> = batch
                        .items
                        .into_iter()
                        .map(|j| compress_job(&mut zstd, j, opts.level))
                        .collect();
                    let done = items.map(|items| Batch {
                        seq: batch.seq,
                        held: batch.held,
                        items,
                    });
                    if done_tx.send(done).is_err() {
                        return;
                    }
                }
            });
        }
        drop(done_tx);
        let writer = {
            let shared = &shared;
            let out = &mut *out;
            scope.spawn(move || {
                let res =
                    write_frames(table.len(), expected, done_rx, shared, opts, max_chain, out);
                shared.stop();
                res
            })
        };
        let read_res = read_all(
            table,
            order,
            src,
            &shared,
            job_tx,
            threads as u64 + 2,
            max_chain,
            report,
            &mut from_loose,
        );
        if read_res.is_err() {
            shared.stop();
        }
        let write_res = writer
            .join()
            .unwrap_or_else(|_| Err(Error::Storage("repack writer thread panicked".into())));
        (read_res, write_res)
    });
    let (written, counts) = write_res?;
    read_res?;
    report.deltas += counts.deltas;
    report.whole += counts.whole;
    report.frames_reused += counts.frames_reused;
    report.logical_bytes += counts.logical_bytes;
    report.objects = written.records.len() as u64;
    Ok(WrittenPack {
        records: written.records,
        pos: written.pos,
        len: written.len,
        hash: written.hash,
        from_loose,
    })
}

/// What the writer thread wrote.
struct Output {
    records: Records,
    pos: Vec<u32>,
    len: u64,
    hash: [u8; 32],
}

fn compress_job(zstd: &mut pack::Compressor, job: Job, level: i32) -> Result<Done> {
    let out = match job.kind {
        JobKind::Reuse { frame, base, depth } => Frames::Reused { frame, base, depth },
        JobKind::Compress { base, base_bytes } => {
            let whole = zstd.whole(&job.data, level)?;
            let delta = match &base_bytes {
                Some(b) => zstd.delta(b, &job.data, level)?,
                None => None,
            };
            Frames::Compressed { whole, base, delta }
        }
    };
    Ok(Done {
        id: job.id,
        len: job.data.len() as u64,
        modified: job.modified,
        out,
    })
}

/// Recently read objects by id, as delta bases: first in, first out, within
/// a byte budget that charges each entry's bookkeeping too, since most
/// objects are small. Which bases it holds changes only what is read
/// again, never a frame.
#[derive(Default)]
struct BaseCache {
    map: std::collections::HashMap<u32, Bytes>,
    order: std::collections::VecDeque<u32>,
    bytes: usize,
}

impl BaseCache {
    /// Bookkeeping per entry: the map slot, the `Bytes` and its header.
    const ENTRY: usize = 128;
    /// Bases are recent objects; more entries would only hold small ones.
    const MAX_ENTRIES: usize = 1 << 17;

    fn get(&self, id: u32) -> Option<Bytes> {
        self.map.get(&id).cloned()
    }

    fn insert(&mut self, id: u32, data: Bytes) {
        if data.len() > BASE_CACHE {
            return;
        }
        self.bytes += data.len() + Self::ENTRY;
        if let Some(old) = self.map.insert(id, data) {
            self.bytes -= old.len() + Self::ENTRY;
        } else {
            self.order.push_back(id);
        }
        while self.bytes > BASE_CACHE || self.order.len() > Self::MAX_ENTRIES {
            let Some(victim) = self.order.pop_front() else {
                break;
            };
            if let Some(d) = self.map.remove(&victim) {
                self.bytes -= d.len() + Self::ENTRY;
            }
        }
    }
}

/// The reader's batch being filled.
struct Pending {
    seq: u64,
    held: u64,
    jobs: Vec<Job>,
}

impl Pending {
    /// Hand the batch over; `false` if stopped.
    fn flush(&mut self, shared: &Shared, jobs: &mpsc::SyncSender<Batch<Job>>, ahead: u64) -> bool {
        if self.jobs.is_empty() {
            return true;
        }
        if !shared.wait_room(self.seq, self.held, ahead) {
            return false;
        }
        let batch = Batch {
            seq: self.seq,
            held: self.held,
            items: std::mem::take(&mut self.jobs),
        };
        self.seq += 1;
        self.held = 0;
        jobs.send(batch).is_ok()
    }
}

/// The reader: read each object in order, decide what can be decided
/// without the frames before it, and hand it to the compression threads.
#[allow(clippy::too_many_arguments)]
fn read_all<S: PackSource>(
    table: &ObjectTable,
    order: Vec<(u32, u32)>,
    src: &mut S,
    shared: &Shared,
    jobs: mpsc::SyncSender<Batch<Job>>,
    ahead: u64,
    max_chain: u8,
    report: &mut RepackReport,
    from_loose: &mut Vec<(u32, S::Stamp)>,
) -> Result<()> {
    let mut bases = BaseCache::default();
    // Read and handed out: such a base will be in the pack, before.
    let mut sent = vec![0u64; table.len().div_ceil(64)];
    let is_sent = |sent: &[u64], id: u32| sent[id as usize / 64] & (1 << (id % 64)) != 0;
    let mut pending = Pending {
        seq: 0,
        held: 0,
        jobs: Vec::with_capacity(BATCH_JOBS),
    };
    for (id, base) in order {
        if shared.stopped() {
            return Ok(());
        }
        let hash = *table.hash(id);
        let (data, modified, stamp) = match src.read(id, &hash)? {
            SourceRead::Ok {
                data,
                modified,
                from_loose,
            } => (data, modified, from_loose),
            SourceRead::Vanished => {
                report.vanished += 1;
                shared.depth[id as usize].store(SKIPPED, Ordering::SeqCst);
                continue;
            }
            SourceRead::Corrupt => {
                report.corrupt.push(hash);
                shared.depth[id as usize].store(SKIPPED, Ordering::SeqCst);
                continue;
            }
        };
        let base = if base == id { NO_ID } else { base };
        let base_hash = (base != NO_ID).then(|| *table.hash(base));
        // The bytes were decoded and checked above, so a copied frame is
        // known good.
        let mut reuse = None;
        if let Some((frame, old)) = src.stored_frame(id, &hash)? {
            if old == base_hash {
                if base == NO_ID {
                    reuse = Some((frame, NO_ID, 0));
                } else if is_sent(&sent, base) {
                    let mut bd = shared.depth[base as usize].load(Ordering::SeqCst);
                    if bd == UNDECIDED {
                        // Its frame is in a batch not written yet.
                        if !pending.flush(shared, &jobs, ahead) {
                            return Ok(());
                        }
                        match shared.wait_depth(base) {
                            Some(d) => bd = d,
                            None => return Ok(()),
                        }
                    }
                    if bd < max_chain {
                        reuse = Some((frame, base, bd + 1));
                    }
                }
            }
        }
        let (kind, held) = match reuse {
            Some((frame, base, depth)) => {
                shared.depth[id as usize].store(depth, Ordering::SeqCst);
                let held = data.len() as u64;
                (JobKind::Reuse { frame, base, depth }, held)
            }
            None => {
                let base_bytes = match base {
                    b if b != NO_ID && is_sent(&sent, b) => match bases.get(b) {
                        Some(x) => Some(x),
                        None => match src.read(b, table.hash(b))? {
                            SourceRead::Ok { data, .. } => Some(data),
                            _ => None,
                        },
                    },
                    _ => None,
                };
                let held = data.len() as u64 + base_bytes.as_ref().map_or(0, |b| b.len() as u64);
                (JobKind::Compress { base, base_bytes }, held)
            }
        };
        bases.insert(id, data.clone());
        sent[id as usize / 64] |= 1 << (id % 64);
        if let Some(s) = stamp {
            from_loose.push((id, s));
        }
        pending.held += held;
        pending.jobs.push(Job {
            id,
            data,
            modified,
            kind,
        });
        if (pending.jobs.len() >= BATCH_JOBS || pending.held >= BATCH_BYTES)
            && !pending.flush(shared, &jobs, ahead)
        {
            return Ok(());
        }
    }
    pending.flush(shared, &jobs, ahead);
    Ok(())
}

/// The writer: take finished batches in order, pick each object's frame,
/// and append it to the pack.
fn write_frames<W: Write>(
    objects: usize,
    expected: usize,
    done: mpsc::Receiver<Result<Batch<Done>>>,
    shared: &Shared,
    opts: &RepackOptions,
    max_chain: u8,
    out: &mut W,
) -> Result<(Output, Counts)> {
    let mut hasher = blake3::Hasher::new();
    let mut offset = 0u64;
    let mut emit = |bytes: &[u8], out: &mut W| -> Result<u64> {
        out.write_all(bytes).map_err(storage_err)?;
        hasher.update(bytes);
        let at = offset;
        offset += bytes.len() as u64;
        Ok(at)
    };
    emit(&PACK_MAGIC, out)?;
    let mut records = Records {
        offset: Vec::with_capacity(expected),
        len: Vec::with_capacity(expected),
        modified: Vec::with_capacity(expected),
        base: Vec::with_capacity(expected),
    };
    let mut pos = vec![NO_ID; objects];
    let mut counts = Counts::default();
    let mut waiting: BTreeMap<u64, Batch<Done>> = BTreeMap::new();
    let mut next = 0u64;
    while let Ok(b) = done.recv() {
        let b = b?;
        waiting.insert(b.seq, b);
        while let Some(batch) = waiting.remove(&next) {
            next += 1;
            for d in batch.items {
                let (frame, base, depth) = match d.out {
                    Frames::Reused { frame, base, depth } => {
                        counts.frames_reused += 1;
                        (frame, base, depth)
                    }
                    Frames::Compressed { whole, base, delta } => {
                        let bd = match base {
                            NO_ID => SKIPPED,
                            b => shared.depth[b as usize].load(Ordering::SeqCst),
                        };
                        match delta {
                            Some(delta) if bd < max_chain && delta.len() < whole.len() => {
                                (delta, base, bd + 1)
                            }
                            _ => (whole, NO_ID, 0),
                        }
                    }
                };
                let at = emit(&frame, out)?;
                if base == NO_ID {
                    counts.whole += 1;
                } else {
                    counts.deltas += 1;
                }
                counts.logical_bytes += d.len;
                pos[d.id as usize] = records.len() as u32;
                records.push(base, at, d.len, d.modified);
                shared.depth[d.id as usize].store(depth, Ordering::SeqCst);
                step(opts, RepackStep::Wrote(records.len() as u64))?;
            }
            let mut p = shared.progress.lock().unwrap();
            p.written = next;
            p.held -= batch.held;
            shared.changed.notify_all();
        }
    }
    if !waiting.is_empty() && !shared.stopped() {
        return Err(Error::Storage("repack writer lost a frame".into()));
    }
    Ok((
        Output {
            records,
            pos,
            len: offset,
            hash: *hasher.finalize().as_bytes(),
        },
        counts,
    ))
}

/// The packs an incremental repack folds into its new pack, given each
/// pack's live object count and the number of loose objects.
///
/// Like git's `repack --geometric`: the largest packs that already grow by
/// `factor` each stay; everything below the first break merges with the
/// loose objects, and the merge keeps absorbing the next pack while that pack
/// is less than `factor` times the merged total. Then the smallest remaining
/// packs join until at most `max_packs` remain. Sizes are object counts, so
/// loose and packed objects compare, and the count bounds how many indexes a
/// read that misses searches.
pub fn plan_merge(
    packs: &[(String, u64)],
    loose: u64,
    factor: u64,
    max_packs: usize,
) -> Vec<String> {
    let mut sorted: Vec<&(String, u64)> = packs.iter().collect();
    sorted.sort_by(|a, b| a.1.cmp(&b.1).then_with(|| a.0.cmp(&b.0)));
    let n = sorted.len();
    let mut split = 0;
    for i in (0..n.saturating_sub(1)).rev() {
        if sorted[i + 1].1 < sorted[i].1.saturating_mul(factor) {
            split = i + 1;
            break;
        }
    }
    let mut total = loose + sorted[..split].iter().map(|p| p.1).sum::<u64>();
    while split < n && sorted[split].1 < total.saturating_mul(factor) {
        total += sorted[split].1;
        split += 1;
    }
    let after = |split: usize| n - split + usize::from(loose > 0 || split > 0);
    while split < n && after(split) > max_packs.max(1) {
        split += 1;
    }
    // Rewriting one pack alone gains nothing.
    if loose == 0 && split < 2 {
        return Vec::new();
    }
    sorted[..split].iter().map(|p| p.0.clone()).collect()
}
