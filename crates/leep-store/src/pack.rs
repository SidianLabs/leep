//! The pack file format (ADR 0029): many objects in one immutable file,
//! each a zstd frame, stored whole or compressed against a base object in the
//! same pack. The layout is in docs/FORMATS.md, "Packs".

use leep_core::{Error, Hash, Result, TreeNode};
use zstd::zstd_safe::{self, CParameter, DParameter};

/// First bytes of a `.pack` file.
pub const PACK_MAGIC: [u8; 8] = *b"LEEPPAK1";
/// First bytes of an `.idx` written today (ADR 0038): field widths chosen
/// per pack, about 43 to 48 bytes per object.
pub const IDX_MAGIC: [u8; 8] = *b"LEEPIDX2";
/// First bytes of the original `.idx` layout: fixed 96-byte records. Still
/// read, never written.
pub const IDX1_MAGIC: [u8; 8] = *b"LEEPIDX1";
/// `LEEPIDX1`: magic, record count, pack length, BLAKE3 of the pack.
pub const IDX1_HEADER_LEN: usize = 8 + 8 + 8 + 32;
/// `LEEPIDX1`: hash, offset, stored length, logical length, base, modified.
pub const IDX1_RECORD_LEN: usize = 32 + 8 + 8 + 8 + 32 + 8;
/// `LEEPIDX2`: magic, record count, pack length, BLAKE3 of the pack, base
/// modification time, five column widths, three zero bytes.
pub const IDX_HEADER_LEN: usize = 8 + 8 + 8 + 32 + 8 + 8;
/// `LEEPIDX2`: 256 cumulative counts by the hash's first byte.
pub const IDX_FANOUT_LEN: usize = 256 * 4;
/// BLAKE3 of everything before it.
pub const IDX_TRAILER_LEN: usize = 32;
/// Longest delta chain the writer produces. Bounds the entries one read
/// decodes.
pub const MAX_CHAIN: usize = 32;
/// Longest chain a reader follows before calling the pack corrupt. Above
/// [`MAX_CHAIN`] so packs from a writer with a higher cap still read.
pub const READ_CHAIN_LIMIT: usize = 64;
/// zstd's default decoder window limit is 2^27; deltas need the base and the
/// object inside one window, so larger pairs are stored whole.
const MAX_DELTA_WINDOW_LOG: u32 = 27;

/// One object's index record.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IdxEntry {
    pub hash: Hash,
    /// Byte offset of the entry's zstd frame in the `.pack`.
    pub offset: u64,
    /// Length of the frame.
    pub stored_len: u64,
    /// Length of the object once decoded.
    pub len: u64,
    /// The entry the frame was compressed against, in the same pack: its
    /// rank in hash order, for [`PackIndex::entry`]. When writing, its
    /// position in the slice given to [`PackIndex::encode`].
    pub base: Option<u32>,
    /// Unix seconds the object was last written before it was packed. Kept
    /// per object so a repack does not make old garbage look new to GC.
    pub modified: i64,
}

/// A loaded `.idx`, kept as its file bytes and searched in place, so memory
/// per object is its size on disk (plus 4 bytes for a `LEEPIDX1` index).
#[derive(Debug, Clone)]
pub struct PackIndex {
    bytes: Vec<u8>,
    count: usize,
    pack_len: u64,
    pack_hash: [u8; 32],
    layout: Layout,
}

#[derive(Debug, Clone)]
enum Layout {
    /// `LEEPIDX1`: 96-byte records with bases as hashes, and each base's
    /// rank (`u32::MAX` for none), found once at load.
    Wide(Vec<u32>),
    /// `LEEPIDX2`: a hash column, then a record per hash of these widths.
    Columns(Widths),
}

/// Byte widths of a `LEEPIDX2` record's fields, chosen per pack as the
/// fewest that hold its largest value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Widths {
    offset: usize,
    stored: usize,
    base: usize,
    len: usize,
    modified: usize,
    /// Subtracted from every modification time before it is stored.
    modified_base: i64,
}

impl Widths {
    fn record(&self) -> usize {
        self.offset + self.stored + self.base + self.len + self.modified
    }
}

/// The pack id: the hex BLAKE3 of the whole index file.
pub fn pack_id(idx_bytes: &[u8]) -> String {
    hex::encode(blake3::hash(idx_bytes).as_bytes())
}

/// Big-endian unsigned integer of 0 to 8 bytes.
fn be_uint(b: &[u8]) -> u64 {
    b.iter().fold(0, |acc, &x| acc << 8 | u64::from(x))
}

fn put_uint(out: &mut Vec<u8>, v: u64, width: usize) {
    out.extend_from_slice(&v.to_be_bytes()[8 - width..]);
}

/// Bytes needed for `v`; at least `min`.
fn width_of(v: u64, min: usize) -> usize {
    (8 - v.leading_zeros() as usize / 8).max(min)
}

fn u64_at(b: &[u8], at: usize) -> u64 {
    u64::from_be_bytes(b[at..at + 8].try_into().unwrap())
}

const HASHES_AT: usize = IDX_HEADER_LEN + IDX_FANOUT_LEN;

impl PackIndex {
    /// Objects in the pack.
    pub fn len(&self) -> usize {
        self.count
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    pub fn pack_len(&self) -> u64 {
        self.pack_len
    }

    pub fn pack_hash(&self) -> &[u8; 32] {
        &self.pack_hash
    }

    /// The index file's bytes, exactly as read.
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// The magic this index was read with.
    pub fn magic(&self) -> [u8; 8] {
        match self.layout {
            Layout::Wide(_) => IDX1_MAGIC,
            Layout::Columns(_) => IDX_MAGIC,
        }
    }

    /// Bytes held in memory for this index.
    pub fn heap_bytes(&self) -> usize {
        let bases = match &self.layout {
            Layout::Wide(b) => b.capacity() * 4,
            Layout::Columns(_) => 0,
        };
        self.bytes.capacity() + bases
    }

    fn hash_bytes(&self, rank: usize) -> &[u8] {
        let at = match self.layout {
            Layout::Wide(_) => IDX1_HEADER_LEN + rank * IDX1_RECORD_LEN,
            Layout::Columns(_) => HASHES_AT + rank * 32,
        };
        &self.bytes[at..at + 32]
    }

    /// The hash at `rank` in ascending order.
    pub fn hash_at(&self, rank: usize) -> Hash {
        Hash(self.hash_bytes(rank).try_into().unwrap())
    }

    /// `Ok(rank)` of `hash`, or `Err` with where it would go.
    pub fn rank_of(&self, hash: &Hash) -> std::result::Result<usize, usize> {
        let key = hash.as_bytes().as_slice();
        let (mut lo, mut hi) = match self.layout {
            Layout::Wide(_) => (0, self.count),
            Layout::Columns(_) => {
                let b = usize::from(key[0]);
                let lo = if b == 0 { 0 } else { self.fanout(b - 1) };
                (lo, self.fanout(b))
            }
        };
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            match self.hash_bytes(mid).cmp(key) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Equal => return Ok(mid),
            }
        }
        Err(lo)
    }

    fn fanout(&self, b: usize) -> usize {
        let at = IDX_HEADER_LEN + b * 4;
        u32::from_be_bytes(self.bytes[at..at + 4].try_into().unwrap()) as usize
    }

    pub fn find(&self, hash: &Hash) -> Option<IdxEntry> {
        self.rank_of(hash).ok().map(|r| self.entry(r as u32))
    }

    /// The entry at `rank` in hash order (also what [`IdxEntry::base`]
    /// names). Panics past the end.
    pub fn entry(&self, rank: u32) -> IdxEntry {
        let rank = rank as usize;
        assert!(rank < self.count, "pack index rank out of range");
        match &self.layout {
            Layout::Wide(bases) => {
                let r = &self.bytes[IDX1_HEADER_LEN + rank * IDX1_RECORD_LEN..][..IDX1_RECORD_LEN];
                IdxEntry {
                    hash: Hash(r[..32].try_into().unwrap()),
                    offset: u64_at(r, 32),
                    stored_len: u64_at(r, 40),
                    len: u64_at(r, 48),
                    base: Some(bases[rank]).filter(|&b| b != u32::MAX),
                    modified: i64::from_be_bytes(r[88..96].try_into().unwrap()),
                }
            }
            Layout::Columns(w) => {
                let rec = w.record();
                let at = HASHES_AT + self.count * 32 + rank * rec;
                let r = &self.bytes[at..at + rec];
                let (offset, r) = r.split_at(w.offset);
                let (stored, r) = r.split_at(w.stored);
                let (base, r) = r.split_at(w.base);
                let (len, modified) = r.split_at(w.len);
                IdxEntry {
                    hash: self.hash_at(rank),
                    offset: be_uint(offset),
                    stored_len: be_uint(stored),
                    len: be_uint(len),
                    base: be_uint(base).checked_sub(1).map(|b| b as u32),
                    // Decode checked that this does not overflow.
                    modified: w.modified_base.wrapping_add_unsigned(be_uint(modified)),
                }
            }
        }
    }

    /// Entries in hash order.
    pub fn iter(&self) -> impl Iterator<Item = IdxEntry> + '_ {
        (0..self.count).map(|r| self.entry(r as u32))
    }

    /// Hashes in ascending order.
    pub fn hashes(&self) -> impl Iterator<Item = Hash> + '_ {
        (0..self.count).map(|r| self.hash_at(r))
    }

    /// Serialize as `LEEPIDX2`. `entries` may be in any order (a writer
    /// passes pack order); each `base` is a position in `entries`, of an
    /// entry at a lower offset. Hashes must be distinct.
    pub fn encode(pack_len: u64, pack_hash: [u8; 32], entries: &[IdxEntry]) -> Result<Vec<u8>> {
        let count = entries.len();
        if count >= u32::MAX as usize {
            return Err(encode_err("too many objects"));
        }
        let mut order: Vec<u32> = (0..count as u32).collect();
        order.sort_unstable_by_key(|&i| entries[i as usize].hash);
        let mut rank = vec![0u32; count];
        for (r, &i) in order.iter().enumerate() {
            rank[i as usize] = r as u32;
        }
        for (i, e) in entries.iter().enumerate() {
            if let Some(b) = e.base {
                if b as usize == i || b as usize >= count {
                    return Err(encode_err("base missing or not earlier in the pack"));
                }
            }
        }
        Self::encode_sorted(pack_len, pack_hash, count, |r| {
            let e = entries[order[r] as usize];
            IdxEntry {
                base: e.base.map(|b| rank[b as usize]),
                ..e
            }
        })
    }

    /// Serialize as `LEEPIDX2` from `count` entries given in hash order by
    /// `entry(rank)`, each `base` a rank. Called a few times per entry, so
    /// a writer can keep its records in whatever compact form it likes.
    pub fn encode_sorted(
        pack_len: u64,
        pack_hash: [u8; 32],
        count: usize,
        entry: impl Fn(usize) -> IdxEntry,
    ) -> Result<Vec<u8>> {
        let mut out = Vec::new();
        Self::encode_sorted_to(pack_len, pack_hash, count, entry, &mut |b: &[u8]| {
            out.extend_from_slice(b);
            Ok(())
        })?;
        Ok(out)
    }

    /// [`encode_sorted`](Self::encode_sorted), handing the bytes to `sink`
    /// in pieces instead of holding the whole index: at 45 bytes per object
    /// it would be a third of a large repack's memory.
    pub fn encode_sorted_to(
        pack_len: u64,
        pack_hash: [u8; 32],
        count: usize,
        entry: impl Fn(usize) -> IdxEntry,
        sink: &mut dyn FnMut(&[u8]) -> Result<()>,
    ) -> Result<()> {
        if count >= u32::MAX as usize {
            return Err(encode_err("too many objects"));
        }
        let mut w = Widths {
            offset: 1,
            stored: 1,
            base: width_of(count as u64, 1),
            len: 0,
            modified: 0,
            modified_base: 0,
        };
        let mut fanout = [0u32; 256];
        let mut prev: Option<Hash> = None;
        let mut modified_base = i64::MAX;
        for r in 0..count {
            let e = entry(r);
            if prev.is_some_and(|p| p >= e.hash) {
                return Err(encode_err("duplicate hash"));
            }
            prev = Some(e.hash);
            if let Some(b) = e.base {
                let ok =
                    (b as usize) < count && b as usize != r && entry(b as usize).offset < e.offset;
                if !ok {
                    return Err(encode_err("base missing or not earlier in the pack"));
                }
            }
            if e.offset
                .checked_add(e.stored_len)
                .is_none_or(|end| end > pack_len)
            {
                return Err(encode_err("entry outside the pack"));
            }
            w.offset = w.offset.max(width_of(e.offset, 1));
            w.stored = w.stored.max(width_of(e.stored_len, 1));
            w.len = w.len.max(width_of(e.len, 0));
            modified_base = modified_base.min(e.modified);
            fanout[usize::from(e.hash.as_bytes()[0])] += 1;
        }
        if count == 0 {
            modified_base = 0;
        }
        w.modified_base = modified_base;
        for r in 0..count {
            w.modified = w
                .modified
                .max(width_of(entry(r).modified.abs_diff(modified_base), 0));
        }
        let mut sum = blake3::Hasher::new();
        let mut buf: Vec<u8> = Vec::with_capacity(HASHES_AT.max(1 << 16) + 64);
        let mut flush = |buf: &mut Vec<u8>, force: bool| -> Result<()> {
            if force || buf.len() >= 1 << 16 {
                sum.update(buf);
                sink(buf)?;
                buf.clear();
            }
            Ok(())
        };
        buf.extend_from_slice(&IDX_MAGIC);
        buf.extend_from_slice(&(count as u64).to_be_bytes());
        buf.extend_from_slice(&pack_len.to_be_bytes());
        buf.extend_from_slice(&pack_hash);
        buf.extend_from_slice(&modified_base.to_be_bytes());
        for width in [w.offset, w.stored, w.base, w.len, w.modified] {
            buf.push(width as u8);
        }
        buf.extend_from_slice(&[0; 3]);
        let mut total = 0;
        for f in fanout {
            total += f;
            buf.extend_from_slice(&total.to_be_bytes());
        }
        for r in 0..count {
            buf.extend_from_slice(entry(r).hash.as_bytes());
            flush(&mut buf, false)?;
        }
        for r in 0..count {
            let e = entry(r);
            put_uint(&mut buf, e.offset, w.offset);
            put_uint(&mut buf, e.stored_len, w.stored);
            put_uint(&mut buf, e.base.map_or(0, |b| u64::from(b) + 1), w.base);
            put_uint(&mut buf, e.len, w.len);
            put_uint(&mut buf, e.modified.abs_diff(modified_base), w.modified);
            flush(&mut buf, false)?;
        }
        flush(&mut buf, true)?;
        sink(sum.finalize().as_bytes())
    }

    /// Parse and check an index of either layout: checksum, order, bounds,
    /// and that every base is another entry of the same pack at a lower
    /// offset (so chains end).
    pub fn decode(bytes: Vec<u8>) -> Result<PackIndex> {
        let bad = |why: &str| Error::Storage(format!("corrupt pack index: {why}"));
        if bytes.len() < 8 + IDX_TRAILER_LEN {
            return Err(bad("too short"));
        }
        let magic: [u8; 8] = bytes[..8].try_into().unwrap();
        let header_len = match magic {
            IDX_MAGIC => IDX_HEADER_LEN + IDX_FANOUT_LEN,
            IDX1_MAGIC => IDX1_HEADER_LEN,
            _ => return Err(bad("bad magic")),
        };
        if bytes.len() < header_len + IDX_TRAILER_LEN {
            return Err(bad("too short"));
        }
        let (body, sum) = bytes.split_at(bytes.len() - IDX_TRAILER_LEN);
        if blake3::hash(body).as_bytes() != sum {
            return Err(bad("checksum mismatch"));
        }
        let count = u64_at(body, 8);
        let pack_len = u64_at(body, 16);
        let pack_hash: [u8; 32] = body[24..56].try_into().unwrap();
        let (layout, record) = if magic == IDX1_MAGIC {
            (Layout::Wide(Vec::new()), IDX1_RECORD_LEN)
        } else {
            let wb = &body[64..72];
            let w = Widths {
                offset: usize::from(wb[0]),
                stored: usize::from(wb[1]),
                base: usize::from(wb[2]),
                len: usize::from(wb[3]),
                modified: usize::from(wb[4]),
                modified_base: u64_at(body, 56) as i64,
            };
            let widths_ok = (1..=8).contains(&w.offset)
                && (1..=8).contains(&w.stored)
                && (1..=4).contains(&w.base)
                && w.len <= 8
                && w.modified <= 8
                && wb[5..] == [0, 0, 0];
            if !widths_ok {
                return Err(bad("bad column widths"));
            }
            if count >= 1u64 << (8 * w.base) {
                return Err(bad("more records than the base width can name"));
            }
            (Layout::Columns(w), 32 + w.record())
        };
        let records = body.len() - header_len;
        if count.checked_mul(record as u64) != Some(records as u64) {
            return Err(bad("record count does not match length"));
        }
        let mut idx = PackIndex {
            count: count as usize,
            pack_len,
            pack_hash,
            layout,
            bytes,
        };
        idx.check(&bad)?;
        Ok(idx)
    }

    fn check(&mut self, bad: &dyn Fn(&str) -> Error) -> Result<()> {
        if let Layout::Columns(_) = self.layout {
            let mut counts = [0usize; 256];
            for r in 0..self.count {
                counts[usize::from(self.hash_bytes(r)[0])] += 1;
            }
            let mut total = 0;
            for (b, c) in counts.iter().enumerate() {
                total += c;
                if self.fanout(b) != total {
                    return Err(bad("fanout does not match the hashes"));
                }
            }
        }
        for r in 1..self.count {
            if self.hash_bytes(r - 1) >= self.hash_bytes(r) {
                return Err(bad("records out of order"));
            }
        }
        if let Layout::Wide(_) = self.layout {
            let mut bases = Vec::with_capacity(self.count);
            for r in 0..self.count {
                let at = IDX1_HEADER_LEN + r * IDX1_RECORD_LEN;
                let base: [u8; 32] = self.bytes[at + 56..at + 88].try_into().unwrap();
                bases.push(if base == [0; 32] {
                    u32::MAX
                } else {
                    match self.rank_of(&Hash(base)) {
                        Ok(b) => b as u32,
                        Err(_) => return Err(bad("delta base missing or not earlier in the pack")),
                    }
                });
            }
            self.layout = Layout::Wide(bases);
        }
        for r in 0..self.count {
            if let Layout::Columns(w) = self.layout {
                let rec = w.record();
                let at = HASHES_AT + self.count * 32 + r * rec;
                let field = &self.bytes[at + w.offset + w.stored..][..w.base];
                let base = be_uint(field);
                if base > self.count as u64 || base == r as u64 + 1 {
                    return Err(bad("delta base out of range"));
                }
                let delta = be_uint(&self.bytes[at + rec - w.modified..at + rec]);
                if w.modified_base.checked_add_unsigned(delta).is_none() {
                    return Err(bad("modification time out of range"));
                }
            }
            let e = self.entry(r as u32);
            let end = e.offset.checked_add(e.stored_len);
            if e.offset < PACK_MAGIC.len() as u64 || end.is_none_or(|end| end > self.pack_len) {
                return Err(bad("entry outside the pack"));
            }
            if let Some(b) = e.base {
                if self.entry(b).offset >= e.offset {
                    return Err(bad("delta base missing or not earlier in the pack"));
                }
            }
        }
        Ok(())
    }
}

fn encode_err(why: &str) -> Error {
    Error::Storage(format!("cannot write pack index: {why}"))
}

fn zstd_err(what: &str, code: usize) -> Error {
    Error::Storage(format!("{what}: {}", zstd_safe::get_error_name(code)))
}

/// Compress `data` alone.
pub fn compress_whole(data: &[u8], level: i32) -> Result<Vec<u8>> {
    Compressor::default().whole(data, level)
}

/// Compress `data` with `base` as a reference prefix (zstd's `--patch-from`
/// mode). A prefix, unlike a loaded dictionary, is always taken as raw
/// content, even if the base happens to start with the dictionary magic.
/// `None` when the pair is too large for one decoder window.
pub fn compress_delta(base: &[u8], data: &[u8], level: i32) -> Result<Option<Vec<u8>>> {
    Compressor::default().delta(base, data, level)
}

/// A zstd context kept across whole frames: creating one per frame cost
/// several percent of a repack. A reused context writes the same bytes,
/// because every parameter is reset and set again per frame. A delta's
/// context borrows its base, so it is made per frame.
#[derive(Default)]
pub struct Compressor {
    cctx: Option<zstd_safe::CCtx<'static>>,
}

impl Compressor {
    /// As [`compress_whole`].
    pub fn whole(&mut self, data: &[u8], level: i32) -> Result<Vec<u8>> {
        let cctx = self.cctx.get_or_insert_with(zstd_safe::CCtx::create);
        cctx.reset(zstd_safe::ResetDirective::SessionAndParameters)
            .map_err(|c| zstd_err("zstd reset", c))?;
        cctx.set_parameter(CParameter::CompressionLevel(level))
            .map_err(|c| zstd_err("zstd parameter", c))?;
        let mut out = Vec::with_capacity(zstd_safe::compress_bound(data.len()));
        cctx.compress2(&mut out, data)
            .map_err(|c| zstd_err("zstd compress", c))?;
        Ok(out)
    }

    /// As [`compress_delta`].
    pub fn delta(&self, base: &[u8], data: &[u8], level: i32) -> Result<Option<Vec<u8>>> {
        let total = (base.len() + data.len()).max(1);
        let window_log = usize::BITS - (total - 1).leading_zeros();
        if window_log > MAX_DELTA_WINDOW_LOG {
            return Ok(None);
        }
        let mut cctx = zstd_safe::CCtx::create();
        let set = |cctx: &mut zstd_safe::CCtx, p| {
            cctx.set_parameter(p)
                .map_err(|c| zstd_err("zstd parameter", c))
        };
        set(&mut cctx, CParameter::CompressionLevel(level))?;
        // The level's default window can be smaller than base plus object,
        // which would hide the start of the base from the matcher.
        set(&mut cctx, CParameter::WindowLog(window_log.max(10)))?;
        cctx.ref_prefix(base)
            .map_err(|c| zstd_err("zstd prefix", c))?;
        let mut out = Vec::with_capacity(zstd_safe::compress_bound(data.len()));
        cctx.compress2(&mut out, data)
            .map_err(|c| zstd_err("zstd compress", c))?;
        Ok(Some(out))
    }
}

/// zstd's dictionary magic (`0xEC30A437`, little-endian). A base starting
/// with it would be parsed as a dictionary rather than taken as raw content.
const DICT_MAGIC: [u8; 4] = [0x37, 0xa4, 0x30, 0xec];

thread_local! {
    /// A decoder context is costly to create and a read decodes a chain of
    /// frames, so each thread keeps one.
    static DCTX: std::cell::RefCell<zstd_safe::DCtx<'static>> =
        std::cell::RefCell::new(zstd_safe::DCtx::create());
}

/// Decode one frame of logical length `len`, against `base` for a delta.
pub fn decompress(frame: &[u8], base: Option<&[u8]>, len: u64) -> Result<Vec<u8>> {
    let len = usize::try_from(len).map_err(|_| Error::Storage("pack entry too large".into()))?;
    let mut out = Vec::with_capacity(len);
    let n = match base {
        // As a reference prefix, which is always raw content.
        Some(b) if b.starts_with(&DICT_MAGIC) => {
            let mut dctx = zstd_safe::DCtx::create();
            dctx.set_parameter(DParameter::WindowLogMax(MAX_DELTA_WINDOW_LOG))
                .map_err(|c| zstd_err("zstd parameter", c))?;
            dctx.ref_prefix(b).map_err(|c| zstd_err("zstd prefix", c))?;
            dctx.decompress(&mut out, frame)
        }
        // Without the magic, a dictionary is raw content: the same history
        // as the prefix the frame was compressed with.
        Some(b) => DCTX.with_borrow_mut(|d| d.decompress_using_dict(&mut out, frame, b)),
        None => DCTX.with_borrow_mut(|d| d.decompress(&mut out, frame)),
    }
    .map_err(|c| zstd_err("corrupt pack entry", c))?;
    if n != len {
        return Err(Error::Storage(format!(
            "corrupt pack entry: decoded {n} bytes, index says {len}"
        )));
    }
    Ok(out)
}

/// Do `data` hash to `hash`, as file contents or as a tree node? Blobs and
/// tree nodes are the only objects in a blob store.
pub fn object_intact(hash: &Hash, data: &[u8]) -> bool {
    Hash::of_blob(data) == *hash || Hash::of_object(TreeNode::KIND, data) == *hash
}

/// Bytes of a `LEEPIDX1` index, which this writer no longer produces, for
/// tests of reading stores written before `LEEPIDX2`. `entries` as for
/// [`PackIndex::encode`].
#[cfg(test)]
pub(crate) fn encode_idx1(pack_len: u64, pack_hash: [u8; 32], entries: &[IdxEntry]) -> Vec<u8> {
    let mut sorted: Vec<&IdxEntry> = entries.iter().collect();
    sorted.sort_by_key(|e| e.hash);
    let mut out = Vec::new();
    out.extend_from_slice(&IDX1_MAGIC);
    out.extend_from_slice(&(entries.len() as u64).to_be_bytes());
    out.extend_from_slice(&pack_len.to_be_bytes());
    out.extend_from_slice(&pack_hash);
    for e in sorted {
        out.extend_from_slice(e.hash.as_bytes());
        out.extend_from_slice(&e.offset.to_be_bytes());
        out.extend_from_slice(&e.stored_len.to_be_bytes());
        out.extend_from_slice(&e.len.to_be_bytes());
        let base = e.base.map_or([0; 32], |b| entries[b as usize].hash.0);
        out.extend_from_slice(&base);
        out.extend_from_slice(&e.modified.to_be_bytes());
    }
    let sum = blake3::hash(&out);
    out.extend_from_slice(sum.as_bytes());
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Two entries in pack order; the second is a delta against the first.
    fn sample() -> Vec<IdxEntry> {
        vec![
            IdxEntry {
                hash: Hash([0x22; 32]),
                offset: 8,
                stored_len: 300,
                len: 70_000,
                base: None,
                modified: 1_700_000_000,
            },
            IdxEntry {
                hash: Hash([0x11; 32]),
                offset: 308,
                stored_len: 5,
                len: 101,
                base: Some(0),
                modified: 1_700_070_000,
            },
        ]
    }

    /// Replace the trailing checksum after editing an index.
    fn reseal(mut bytes: Vec<u8>) -> Vec<u8> {
        let body = bytes.len() - IDX_TRAILER_LEN;
        let sum = *blake3::hash(&bytes[..body]).as_bytes();
        bytes[body..].copy_from_slice(&sum);
        bytes
    }

    /// The index layout is persisted: these bytes must never change. A new
    /// layout needs a new magic (see docs/FORMATS.md).
    #[test]
    fn index_golden_bytes() {
        let bytes = PackIndex::encode(313, [0xab; 32], &sample()).unwrap();
        let mut want = Vec::new();
        want.extend_from_slice(b"LEEPIDX2");
        want.extend_from_slice(&2u64.to_be_bytes());
        want.extend_from_slice(&313u64.to_be_bytes());
        want.extend_from_slice(&[0xab; 32]);
        want.extend_from_slice(&1_700_000_000i64.to_be_bytes());
        // Widths: offset 2, stored length 2, base 1, length 3, modified 3.
        want.extend_from_slice(&[2, 2, 1, 3, 3, 0, 0, 0]);
        for b in 0..=255u8 {
            let n: u32 = match b {
                0..0x11 => 0,
                0x11..0x22 => 1,
                _ => 2,
            };
            want.extend_from_slice(&n.to_be_bytes());
        }
        want.extend_from_slice(&[0x11; 32]);
        want.extend_from_slice(&[0x22; 32]);
        // 0x11..: offset 308, 5 bytes, base rank 1 (+1), 101 bytes, +70,000 s.
        want.extend_from_slice(&[0x01, 0x34, 0x00, 0x05, 0x02, 0x00, 0x00, 0x65]);
        want.extend_from_slice(&[0x01, 0x11, 0x70]);
        // 0x22..: offset 8, 300 bytes, whole, 70,000 bytes, +0 s.
        want.extend_from_slice(&[0x00, 0x08, 0x01, 0x2c, 0x00, 0x01, 0x11, 0x70]);
        want.extend_from_slice(&[0x00, 0x00, 0x00]);
        let sum = *blake3::hash(&want).as_bytes();
        want.extend_from_slice(&sum);
        assert_eq!(bytes, want);
        assert_eq!(
            bytes.len(),
            IDX_HEADER_LEN + IDX_FANOUT_LEN + 2 * (32 + 11) + 32
        );
        assert_eq!(
            pack_id(&bytes),
            "c28fae7eeea6d9a8774a9df31d34b632363d93d77a481e7fb7ad58234c510b77"
        );
        let back = PackIndex::decode(bytes).unwrap();
        assert_eq!(back.magic(), IDX_MAGIC);
        assert_eq!((back.pack_len(), *back.pack_hash()), (313, [0xab; 32]));
        let mut want = sample();
        want.swap(0, 1);
        want[0].base = Some(1);
        assert_eq!(back.iter().collect::<Vec<_>>(), want);
        assert_eq!(back.find(&Hash([0x22; 32])), Some(want[1]));
        assert_eq!(back.find(&Hash([0x12; 32])), None);
        assert_eq!(back.rank_of(&Hash([0x12; 32])), Err(1));
    }

    /// `LEEPIDX1` indexes are on disk in deployed stores and must keep
    /// reading: these bytes and their id are fixed.
    #[test]
    fn idx1_golden_bytes_decode() {
        let mut want = Vec::new();
        want.extend_from_slice(b"LEEPIDX1");
        want.extend_from_slice(&2u64.to_be_bytes());
        want.extend_from_slice(&33u64.to_be_bytes());
        want.extend_from_slice(&[0xab; 32]);
        want.extend_from_slice(&[0x11; 32]);
        want.extend_from_slice(&8u64.to_be_bytes());
        want.extend_from_slice(&20u64.to_be_bytes());
        want.extend_from_slice(&100u64.to_be_bytes());
        want.extend_from_slice(&[0; 32]);
        want.extend_from_slice(&1_700_000_000i64.to_be_bytes());
        want.extend_from_slice(&[0x22; 32]);
        want.extend_from_slice(&28u64.to_be_bytes());
        want.extend_from_slice(&5u64.to_be_bytes());
        want.extend_from_slice(&101u64.to_be_bytes());
        want.extend_from_slice(&[0x11; 32]);
        want.extend_from_slice(&1_700_000_001i64.to_be_bytes());
        let sum = *blake3::hash(&want).as_bytes();
        want.extend_from_slice(&sum);
        assert_eq!(want.len(), IDX1_HEADER_LEN + 2 * IDX1_RECORD_LEN + 32);
        assert_eq!(
            pack_id(&want),
            "25b80a7692278d93d0b96200de40de81063b86f202bec22907075c65cec94ea5"
        );
        let entries = [
            IdxEntry {
                hash: Hash([0x11; 32]),
                offset: 8,
                stored_len: 20,
                len: 100,
                base: None,
                modified: 1_700_000_000,
            },
            IdxEntry {
                hash: Hash([0x22; 32]),
                offset: 28,
                stored_len: 5,
                len: 101,
                base: Some(0),
                modified: 1_700_000_001,
            },
        ];
        assert_eq!(encode_idx1(33, [0xab; 32], &entries), want);
        let back = PackIndex::decode(want).unwrap();
        assert_eq!(back.magic(), IDX1_MAGIC);
        assert_eq!((back.pack_len(), *back.pack_hash()), (33, [0xab; 32]));
        assert_eq!(back.iter().collect::<Vec<_>>(), entries);
        assert_eq!(back.find(&Hash([0x22; 32])), Some(entries[1]));
        assert_eq!(back.find(&Hash([0x33; 32])), None);
    }

    /// Where the record of rank `r` starts in an index of [`sample`].
    fn record_at(r: usize) -> usize {
        IDX_HEADER_LEN + IDX_FANOUT_LEN + 2 * 32 + r * 11
    }

    #[test]
    fn damaged_indexes_are_refused() {
        let good = PackIndex::encode(313, [0xab; 32], &sample()).unwrap();
        let refused = |bytes: Vec<u8>, why: &str| {
            let err = PackIndex::decode(bytes).expect_err(why);
            assert!(
                err.to_string().contains("corrupt pack index"),
                "{why}: {err}"
            );
        };
        let mut flipped = good.clone();
        flipped[70] ^= 1;
        refused(flipped, "checksum");
        refused(good[..good.len() - 1].to_vec(), "torn");
        let mut magic = good.clone();
        magic[7] = b'9';
        refused(reseal(magic), "unknown magic");
        let mut extra = good.clone();
        extra.insert(record_at(0), 0);
        refused(reseal(extra), "length does not match the count");
        let mut width = good.clone();
        width[64] = 0;
        refused(reseal(width), "zero offset width");
        let mut width = good.clone();
        width[66] = 5;
        refused(reseal(width), "base wider than four bytes");
        let mut reserved = good.clone();
        reserved[71] = 1;
        refused(reseal(reserved), "reserved byte");
        let mut fanout = good.clone();
        fanout[IDX_HEADER_LEN + 0x11 * 4 + 3] = 0;
        refused(reseal(fanout), "fanout");
        let mut order = good.clone();
        order[IDX_HEADER_LEN + IDX_FANOUT_LEN] = 0x33;
        refused(reseal(order), "hashes out of order");
        // Base fields: rank + 1, one byte at offset 4 of each record.
        let mut range = good.clone();
        range[record_at(0) + 4] = 3;
        refused(reseal(range), "base rank past the end");
        let mut own = good.clone();
        own[record_at(0) + 4] = 1;
        refused(reseal(own), "base is the entry itself");
        // A base later in the pack could form a cycle.
        let mut later = good.clone();
        later[record_at(0) + 4] = 0;
        later[record_at(1) + 4] = 1;
        refused(reseal(later), "later base");
        let mut past = good.clone();
        past[record_at(0) + 3] = 6;
        refused(reseal(past), "frame past the end of the pack");
        let mut early = good.clone();
        early[record_at(1) + 1] = 7;
        refused(reseal(early), "frame inside the pack magic");
        let mut time = good.clone();
        time[56..64].copy_from_slice(&i64::MAX.to_be_bytes());
        refused(reseal(time), "modification time overflows");

        // And the original layout.
        let mut v1 = sample();
        let good = encode_idx1(313, [0xab; 32], &v1);
        assert!(PackIndex::decode(good.clone()).is_ok());
        let mut flipped = good.clone();
        flipped[70] ^= 1;
        refused(flipped, "idx1 checksum");
        refused(good[..good.len() - 1].to_vec(), "idx1 torn");
        let mut missing = good.clone();
        missing[IDX1_HEADER_LEN + 56] = 0x44;
        refused(reseal(missing), "idx1 base not in the index");
        v1[0].base = Some(1);
        v1[1].base = None;
        refused(encode_idx1(313, [0xab; 32], &v1), "idx1 later base");
        v1[0].base = None;
        v1[1].stored_len = 6;
        refused(encode_idx1(313, [0xab; 32], &v1), "idx1 past the end");
    }

    #[test]
    fn the_writer_refuses_what_readers_would() {
        let mut later = sample();
        later[0].base = Some(1);
        later[1].base = None;
        assert!(PackIndex::encode(313, [0; 32], &later).is_err());
        let mut range = sample();
        range[1].base = Some(2);
        assert!(PackIndex::encode(313, [0; 32], &range).is_err());
        let mut dup = sample();
        dup[1].hash = dup[0].hash;
        dup[1].base = None;
        assert!(PackIndex::encode(313, [0; 32], &dup).is_err());
        assert!(PackIndex::encode(312, [0; 32], &sample()).is_err());
    }

    /// Widths follow the largest value of each field, up to eight bytes, and
    /// modification times may span the whole i64 range.
    #[test]
    fn extreme_values_round_trip() {
        let entries = vec![
            IdxEntry {
                hash: Hash([0x00; 32]),
                offset: 8,
                stored_len: 1,
                len: 0,
                base: None,
                modified: i64::MIN,
            },
            IdxEntry {
                hash: Hash([0xff; 32]),
                offset: 9,
                stored_len: u64::MAX - 9,
                len: u64::MAX,
                base: Some(0),
                modified: i64::MAX,
            },
        ];
        let bytes = PackIndex::encode(u64::MAX, [0; 32], &entries).unwrap();
        assert_eq!(bytes[64..72], [1, 8, 1, 8, 8, 0, 0, 0]);
        let back = PackIndex::decode(bytes).unwrap();
        assert_eq!(back.iter().collect::<Vec<_>>(), entries);
        let empty = PackIndex::decode(PackIndex::encode(8, [0; 32], &[]).unwrap()).unwrap();
        assert!(empty.is_empty());
        assert_eq!(empty.find(&Hash([0; 32])), None);
    }

    /// Lookups agree with a sorted list on a few thousand random hashes,
    /// across every fanout bucket.
    #[test]
    fn lookups_match_a_sorted_list() {
        let mut seed = 1u64;
        let mut entries = Vec::new();
        for i in 0..3000u64 {
            seed = seed
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let mut h = [0u8; 32];
            h[..8].copy_from_slice(&seed.to_be_bytes());
            entries.push(IdxEntry {
                hash: Hash(h),
                offset: 8 + i * 10,
                stored_len: 10,
                len: i,
                base: i.checked_sub(1 + i % 7).map(|b| b as u32),
                modified: i as i64,
            });
        }
        let idx = PackIndex::decode(PackIndex::encode(30_008, [0; 32], &entries).unwrap()).unwrap();
        let mut sorted = entries.clone();
        sorted.sort_by_key(|e| e.hash);
        for e in &entries {
            let got = idx.find(&e.hash).unwrap();
            assert_eq!(
                (got.offset, got.len, got.modified),
                (e.offset, e.len, e.modified)
            );
            let base = got.base.map(|b| idx.entry(b).hash);
            assert_eq!(base, e.base.map(|b| entries[b as usize].hash));
            let mut miss = e.hash;
            miss.0[31] = 1;
            assert_eq!(idx.find(&miss), None);
        }
        assert!(idx.hashes().eq(sorted.iter().map(|e| e.hash)));
    }

    /// The frames are plain zstd: whole objects decode with any zstd, deltas
    /// with the base as a prefix. Frame bytes depend on the zstd version, so
    /// the golden check is on decoding fixed frames, not on encoding.
    #[test]
    fn frame_golden_bytes_decode() {
        // A single raw block: any zstd decodes it.
        let whole: &[u8] = &[
            0x28, 0xb5, 0x2f, 0xfd, 0x20, 0x11, 0x89, 0x00, 0x00, b'h', b'e', b'l', b'l', b'o',
            b' ', b'h', b'e', b'l', b'l', b'o', b' ', b'h', b'e', b'l', b'l', b'o',
        ];
        assert_eq!(decompress(whole, None, 17).unwrap(), b"hello hello hello");
        assert!(decompress(whole, None, 16).is_err(), "length is checked");
        let base = b"the quick brown fox jumps over the lazy dog".repeat(4);
        let mut data = base.clone();
        data.extend_from_slice(b" and again");
        // What this writer produced for the pair below; readers must keep
        // decoding it whatever a later zstd writes.
        let golden_delta: &[u8] = &[
            0x28, 0xb5, 0x2f, 0xfd, 0x20, 0xb6, 0x8d, 0x00, 0x00, 0x50, 0x20, 0x61, 0x6e, 0x64,
            0x20, 0x61, 0x67, 0x61, 0x69, 0x6e, 0x01, 0x00, 0x29, 0x47, 0x29, 0x20,
        ];
        assert_eq!(
            decompress(golden_delta, Some(&base), data.len() as u64).unwrap(),
            data
        );
        assert!(decompress(golden_delta, None, data.len() as u64).is_err());
        let delta = compress_delta(&base, &data, 9).unwrap().unwrap();
        assert!(delta.len() < compress_whole(&data, 9).unwrap().len());
        assert_eq!(
            decompress(&delta, Some(&base), data.len() as u64).unwrap(),
            data
        );
    }

    /// A base that starts like a zstd dictionary is still raw content.
    #[test]
    fn a_base_with_the_dictionary_magic_is_raw_content() {
        let mut base = DICT_MAGIC.to_vec();
        base.extend_from_slice(&b"some file that happens to start like a dictionary ".repeat(8));
        let mut data = base.clone();
        data.extend_from_slice(b"plus an edit");
        let delta = compress_delta(&base, &data, 9).unwrap().unwrap();
        assert_eq!(
            decompress(&delta, Some(&base), data.len() as u64).unwrap(),
            data
        );
        // And the shared decoder is not left holding it.
        let other = compress_whole(b"plain", 9).unwrap();
        assert_eq!(decompress(&other, None, 5).unwrap(), b"plain");
    }

    #[test]
    fn object_intact_knows_blobs_and_tree_nodes() {
        assert!(object_intact(&Hash::of_blob(b"x"), b"x"));
        assert!(object_intact(&Hash::of_object("tree-v1", b"node"), b"node"));
        assert!(!object_intact(&Hash::of_blob(b"x"), b"y"));
    }
}
