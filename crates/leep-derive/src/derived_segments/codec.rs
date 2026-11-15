//! Bytes of derived segments (ADR 0104): the index blob, the data blob and
//! the manifest row. FORMATS.md has the same layouts.
//!
//! A segment is a lossless container of derived rows that already exist:
//! a read gives back the bytes a row read would (`skiplist` byte for byte,
//! `history-tree-v4` the same part by ADR 0045's definition). Inside it,
//! changeset ids the rows name are positions: the keys are numbered by
//! `(generation, id)`, so an ancestor in the same segment is a small
//! backwards distance, and anything else an index into one table of
//! external ids per segment.

use bytes::Bytes;
use leep_core::{ChangesetId, Error, Hash, Result};

use crate::derived::history_pack;
use crate::derived::{DerivedKind, HistoryTree, Skiplist};

/// Object kind whose preimage prefix starts every index blob, so no upload
/// can take its address (ADR 0041).
pub const INDEX_KIND: &str = "derived-segment-index-v1";
/// Object kind whose preimage prefix starts every data blob.
pub const DATA_KIND: &str = "derived-segment-data-v1";

/// Family `changeset-v1`: keys are changeset ids.
pub const FAMILY_CHANGESET: u8 = 1;
/// Name of [`FAMILY_CHANGESET`], hashed into its manifest key.
pub const FAMILY_CHANGESET_NAME: &str = "changeset-v1";

/// Column numbers of `changeset-v1`, fixed forever: a later kind takes a
/// new number and every stored segment stays valid.
pub const COL_SKIPLIST: u8 = 1;
pub const COL_HISTORY_TREE: u8 = 2;

/// Columns this binary can decode, per family.
fn known_column(family: u8, col: u8) -> bool {
    family == FAMILY_CHANGESET && matches!(col, COL_SKIPLIST | COL_HISTORY_TREE)
}

const VERSION: u8 = 1;
const MANIFEST_VERSION: u8 = 1;

/// Rows a block holds before it is closed, by uncompressed bytes: a hit
/// decompresses one block. Tests use small blocks so every test has many.
pub(crate) const BLOCK_BYTES: usize = if cfg!(test) { 300 } else { 64 << 10 };
/// zstd level of blocks: segments are written once in the background and
/// read many times.
const ZSTD_LEVEL: i32 = 9;
/// Largest block a reader decompresses, so a damaged length cannot make it
/// allocate without bound.
const BLOCK_MAX: u64 = 16 << 20;
/// Most keys a reader accepts in one segment.
const ROWS_MAX: u64 = 1 << 22;

/// Retired blobs the manifest remembers, for reporting (1,024 segments);
/// beyond this the oldest are forgotten and collected as plain orphans.
pub const MAX_RETIRED: usize = 2048;

fn bad(what: &str) -> Error {
    Error::Encoding(format!("derived segment: {what}"))
}

pub(crate) fn put_varint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push(v as u8 | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

pub(crate) struct Reader<'a> {
    pub(crate) b: &'a [u8],
    pub(crate) pos: usize,
}

impl<'a> Reader<'a> {
    pub(crate) fn new(b: &'a [u8]) -> Self {
        Reader { b, pos: 0 }
    }

    fn left(&self) -> usize {
        self.b.len() - self.pos
    }

    pub(crate) fn byte(&mut self) -> Result<u8> {
        let b = *self.b.get(self.pos).ok_or_else(|| bad("truncated"))?;
        self.pos += 1;
        Ok(b)
    }

    pub(crate) fn varint(&mut self) -> Result<u64> {
        let mut v = 0u64;
        for shift in (0..64).step_by(7) {
            let b = self.byte()?;
            v |= ((b & 0x7f) as u64) << shift;
            if b < 0x80 {
                // One encoding per value: no trailing zero groups.
                if b == 0 && shift > 0 {
                    return Err(bad("varint not minimal"));
                }
                return Ok(v);
            }
        }
        Err(bad("varint too long"))
    }

    /// A count of items each at least `each` bytes long.
    fn count(&mut self, each: usize) -> Result<usize> {
        let n = self.varint()?;
        if n.saturating_mul(each as u64) > self.left() as u64 {
            return Err(bad("count past the end"));
        }
        Ok(n as usize)
    }

    pub(crate) fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        if n > self.left() {
            return Err(bad("truncated"));
        }
        let s = &self.b[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }

    fn hash(&mut self) -> Result<Hash> {
        Ok(Hash(self.take(32)?.try_into().expect("32 bytes")))
    }

    fn u32(&mut self) -> Result<u32> {
        u32::try_from(self.varint()?).map_err(|_| bad("number out of range"))
    }
}

/// `leep:<kind>\0`.
fn prefix(kind: &str) -> Vec<u8> {
    let mut p = leep_core::hash::OBJECT_DOMAIN.to_vec();
    p.extend_from_slice(kind.as_bytes());
    p.push(0);
    p
}

/// Whether `data` stored at `hash` is an index or data blob of a segment.
pub fn is_segment_blob(hash: &Hash, data: &[u8]) -> bool {
    (data.starts_with(&prefix(INDEX_KIND)) || data.starts_with(&prefix(DATA_KIND)))
        && Hash::of_blob(data) == *hash
}

/// A fresh nonce: process-random keys mixed with the time.
pub(crate) fn nonce() -> u64 {
    use std::hash::{BuildHasher, Hasher};
    let mut h = std::collections::hash_map::RandomState::new().build_hasher();
    h.write_u128(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0),
    );
    h.finish()
}

// --- the manifest ----------------------------------------------------------

/// One live segment as the manifest names it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SegmentRef {
    pub index: Hash,
    pub data: Hash,
    /// Keys in the segment.
    pub rows: u32,
    /// Columns readers may use: bit `c` for column `c`. `repair drop`
    /// clears a column's bit without rewriting the segment.
    pub columns: u64,
    /// Bytes of the data blob, for the merge caps.
    pub data_len: u32,
}

impl SegmentRef {
    pub fn has(&self, col: u8) -> bool {
        col < 64 && self.columns & (1 << col) != 0
    }
}

/// The manifest row of one family.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Manifest {
    pub family: u8,
    /// Grows with every write (the previous value plus one, or the writer's
    /// clock in microseconds if that is more), so a reader never goes back to
    /// an older manifest it reads late, and one written over a damaged row
    /// still sorts after what readers held.
    pub seq: u64,
    /// Live segments, oldest first. A key in several answers from the
    /// newest that has the column.
    pub live: Vec<SegmentRef>,
    /// Blobs of segments no longer live and perhaps still stored, oldest
    /// first. Blob GC collects them like orphans; this only reports them
    /// apart.
    pub retired: Vec<Hash>,
}

/// Why a manifest row is not a [`Manifest`] this binary can use.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ManifestFault {
    /// The checksum or the structure is wrong: damage.
    Damaged(String),
    /// The checksum holds but the version, flags or family are unknown: a
    /// newer binary wrote it. Never treated as damage, never replaced.
    Newer(String),
}

impl Manifest {
    pub fn new(family: u8) -> Self {
        Manifest {
            family,
            seq: 0,
            live: Vec::new(),
            retired: Vec::new(),
        }
    }

    /// `version, flags, family, u64 seq, u32 live count, (index, data, u32
    /// rows, u64 columns, u32 data bytes)*, u32 retired count, hash*, BLAKE3
    /// of everything before`. Integers big-endian.
    pub fn encode(&self) -> Bytes {
        let mut out =
            Vec::with_capacity(3 + 8 + 4 + self.live.len() * 80 + 4 + self.retired.len() * 32 + 32);
        out.push(MANIFEST_VERSION);
        out.push(0);
        out.push(self.family);
        out.extend_from_slice(&self.seq.to_be_bytes());
        out.extend_from_slice(&(self.live.len() as u32).to_be_bytes());
        for s in &self.live {
            out.extend_from_slice(&s.index.0);
            out.extend_from_slice(&s.data.0);
            out.extend_from_slice(&s.rows.to_be_bytes());
            out.extend_from_slice(&s.columns.to_be_bytes());
            out.extend_from_slice(&s.data_len.to_be_bytes());
        }
        out.extend_from_slice(&(self.retired.len() as u32).to_be_bytes());
        for h in &self.retired {
            out.extend_from_slice(&h.0);
        }
        let sum = Hash::of_blob(&out);
        out.extend_from_slice(&sum.0);
        Bytes::from(out)
    }

    pub fn decode(v: &[u8]) -> Result<Manifest> {
        Self::parse(v).map_err(|f| match f {
            ManifestFault::Damaged(m) | ManifestFault::Newer(m) => {
                Error::Encoding(format!("derived segment manifest: {m}"))
            }
        })
    }

    /// [`Manifest::decode`], telling damage from a newer format.
    pub fn parse(v: &[u8]) -> std::result::Result<Manifest, ManifestFault> {
        let bad = |what: &str| ManifestFault::Damaged(what.to_string());
        if v.len() < 32 + 3 {
            return Err(bad("short"));
        }
        let (body, sum) = v.split_at(v.len() - 32);
        if Hash::of_blob(body).0 != sum {
            return Err(bad("checksum"));
        }
        let newer = |what: &str| ManifestFault::Newer(what.to_string());
        if body[0] != MANIFEST_VERSION {
            return Err(newer("unknown version"));
        }
        if body[1] != 0 {
            return Err(newer("unknown flags"));
        }
        let family = body[2];
        if family != FAMILY_CHANGESET {
            return Err(newer("unknown family"));
        }
        let mut at = 3;
        let mut take = |n: usize| -> std::result::Result<&[u8], ManifestFault> {
            let s = body.get(at..at + n).ok_or_else(|| bad("truncated"))?;
            at += n;
            Ok(s)
        };
        let seq = u64::from_be_bytes(take(8)?.try_into().unwrap());
        let n = u32::from_be_bytes(take(4)?.try_into().unwrap()) as usize;
        let mut live = Vec::with_capacity(n.min(4096));
        for _ in 0..n {
            let index = Hash(take(32)?.try_into().unwrap());
            let data = Hash(take(32)?.try_into().unwrap());
            let rows = u32::from_be_bytes(take(4)?.try_into().unwrap());
            let columns = u64::from_be_bytes(take(8)?.try_into().unwrap());
            let data_len = u32::from_be_bytes(take(4)?.try_into().unwrap());
            live.push(SegmentRef {
                index,
                data,
                rows,
                columns,
                data_len,
            });
        }
        let r = u32::from_be_bytes(take(4)?.try_into().unwrap()) as usize;
        let mut retired = Vec::with_capacity(r.min(4096));
        for _ in 0..r {
            retired.push(Hash(take(32)?.try_into().unwrap()));
        }
        if at != body.len() {
            return Err(bad("trailing bytes"));
        }
        Ok(Manifest {
            family,
            seq,
            live,
            retired,
        })
    }

    /// Every blob a live segment is made of: blob GC never condemns these.
    pub fn live_blobs(&self) -> impl Iterator<Item = Hash> + '_ {
        self.live.iter().flat_map(|s| [s.index, s.data])
    }

    /// Remember `segments`' blobs as retired, unless a live segment still
    /// uses them; the list keeps the newest [`MAX_RETIRED`].
    pub fn retire(&mut self, segments: impl IntoIterator<Item = SegmentRef>) {
        for s in segments {
            for h in [s.index, s.data] {
                if !self.retired.contains(&h) {
                    self.retired.push(h);
                }
            }
        }
        let live: std::collections::HashSet<Hash> = self.live_blobs().collect();
        self.retired.retain(|h| !live.contains(h));
        if self.retired.len() > MAX_RETIRED {
            let extra = self.retired.len() - MAX_RETIRED;
            self.retired.drain(..extra);
        }
    }
}

// --- encoding a segment ----------------------------------------------------

/// One key to seal: its generation (which orders the segment) and the
/// values of the columns it has, as read from rows.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SealRow {
    pub key: Hash,
    pub generation: u64,
    pub values: Vec<(u8, Bytes)>,
}

/// A segment ready to store: both blobs and the manifest entry naming it.
pub struct Encoded {
    pub index: (Hash, Bytes),
    pub data: (Hash, Bytes),
    pub seg: SegmentRef,
}

/// A row value that cannot go into a segment: it does not decode, or its
/// checksum or structure is wrong. The caller leaves the row as it is.
#[derive(Debug)]
pub struct BadValue {
    pub key: Hash,
    pub col: u8,
    pub why: String,
}

/// Encode `rows` (distinct keys) as one segment of `family`. Values that do
/// not decode as their column's kind are left out and returned, so a
/// damaged row is never copied into a segment.
pub fn encode(family: u8, rows: &[SealRow], nonce: u64) -> Result<(Encoded, Vec<BadValue>)> {
    if family != FAMILY_CHANGESET {
        return Err(Error::Invalid(format!(
            "unknown derived segment family {family}"
        )));
    }
    // Ordinals: `(generation, key)` order, so ancestors come first.
    let mut by_ord: Vec<&SealRow> = rows.iter().collect();
    by_ord.sort_by_key(|r| (r.generation, r.key));
    if by_ord.windows(2).any(|w| w[0].key == w[1].key) {
        return Err(Error::Invalid(
            "a key is sealed twice in one segment".into(),
        ));
    }
    let n = by_ord.len();
    let ord_of: std::collections::HashMap<Hash, u32> = by_ord
        .iter()
        .enumerate()
        .map(|(o, r)| (r.key, o as u32))
        .collect();
    let mut ext = Externals::default();
    let mut bad_values = Vec::new();
    // Per column (ascending), each present row's encoding by ordinal.
    let mut cols: std::collections::BTreeMap<u8, Vec<Option<Vec<u8>>>> = Default::default();
    for (o, r) in by_ord.iter().enumerate() {
        let mut seen = 0u64;
        for (col, value) in &r.values {
            if !known_column(family, *col) || seen & (1 << col) != 0 {
                return Err(Error::Invalid(format!("bad column {col} for {}", r.key)));
            }
            seen |= 1 << col;
            let mut refer = |id: &Hash| -> u64 {
                match ord_of.get(id) {
                    Some(&p) if (p as usize) < o => 2 * (o as u64 - 1 - p as u64),
                    _ => 2 * ext.index(*id) + 1,
                }
            };
            let enc = match *col {
                COL_SKIPLIST => encode_skiplist(r.key, value, &mut refer),
                _ => encode_history(r.key, value, &mut refer),
            };
            match enc {
                Ok(e) => cols.entry(*col).or_insert_with(|| vec![None; n])[o] = Some(e),
                Err(why) => bad_values.push(BadValue {
                    key: r.key,
                    col: *col,
                    why,
                }),
            }
        }
    }
    let nonce_bytes = nonce.to_be_bytes();
    let mut data = prefix(DATA_KIND);
    data.push(VERSION);
    data.extend_from_slice(&nonce_bytes);
    put_varint(&mut data, ext.ids.len() as u64);
    for id in &ext.ids {
        data.extend_from_slice(&id.0);
    }
    // Column directories, filled as blocks are written.
    let mut dirs: Vec<(u8, Vec<u8>, Vec<BlockRef>)> = Vec::new();
    let mut z =
        zstd::bulk::Compressor::new(ZSTD_LEVEL).map_err(|e| Error::Other(format!("zstd: {e}")))?;
    for (col, rows) in &cols {
        let mut present = vec![0u8; n.div_ceil(8)];
        let mut blocks = Vec::new();
        let mut buf: Vec<u8> = Vec::new();
        let mut first = 0u32;
        let mut flush = |buf: &mut Vec<u8>, first: u32, data: &mut Vec<u8>| -> Result<()> {
            let packed = z
                .compress(buf)
                .map_err(|e| Error::Other(format!("zstd: {e}")))?;
            blocks.push(BlockRef {
                first,
                offset: data.len() as u64,
                len: packed.len() as u32,
                raw: buf.len() as u32,
            });
            data.extend_from_slice(&packed);
            buf.clear();
            Ok(())
        };
        for (o, row) in rows.iter().enumerate() {
            let Some(row) = row else { continue };
            if buf.is_empty() {
                first = o as u32;
            }
            present[o / 8] |= 1 << (o % 8);
            put_varint(&mut buf, row.len() as u64);
            buf.extend_from_slice(row);
            if buf.len() >= BLOCK_BYTES {
                flush(&mut buf, first, &mut data)?;
            }
        }
        if !buf.is_empty() {
            flush(&mut buf, first, &mut data)?;
        }
        dirs.push((*col, present, blocks));
    }
    let data_hash = Hash::of_blob(&data);
    let mut index = prefix(INDEX_KIND);
    index.push(VERSION);
    index.extend_from_slice(&nonce_bytes);
    put_varint(&mut index, family as u64);
    put_varint(&mut index, n as u64);
    let mut keys: Vec<(Hash, u32)> = ord_of.iter().map(|(k, o)| (*k, *o)).collect();
    keys.sort_unstable();
    for (k, _) in &keys {
        index.extend_from_slice(&k.0);
    }
    for (_, o) in &keys {
        put_varint(&mut index, *o as u64);
    }
    put_varint(&mut index, dirs.len() as u64);
    let mut columns = 0u64;
    for (col, present, blocks) in &dirs {
        columns |= 1 << col;
        put_varint(&mut index, *col as u64);
        index.extend_from_slice(present);
        put_varint(&mut index, blocks.len() as u64);
        for b in blocks {
            put_varint(&mut index, b.first as u64);
            put_varint(&mut index, b.offset);
            put_varint(&mut index, b.len as u64);
            put_varint(&mut index, b.raw as u64);
        }
    }
    index.extend_from_slice(&data_hash.0);
    put_varint(&mut index, data.len() as u64);
    let index_hash = Hash::of_blob(&index);
    let seg = SegmentRef {
        index: index_hash,
        data: data_hash,
        rows: n as u32,
        columns,
        data_len: data.len() as u32,
    };
    Ok((
        Encoded {
            index: (index_hash, Bytes::from(index)),
            data: (data_hash, Bytes::from(data)),
            seg,
        },
        bad_values,
    ))
}

/// Ids outside the segment (or not before the row naming them), numbered
/// by first use.
#[derive(Default)]
struct Externals {
    ids: Vec<Hash>,
    at: std::collections::HashMap<Hash, u64>,
}

impl Externals {
    fn index(&mut self, id: Hash) -> u64 {
        *self.at.entry(id).or_insert_with(|| {
            self.ids.push(id);
            self.ids.len() as u64 - 1
        })
    }
}

/// `count, id reference*, first generation, (previous - this)*`.
fn encode_skiplist(
    key: Hash,
    value: &[u8],
    refer: &mut dyn FnMut(&Hash) -> u64,
) -> std::result::Result<Vec<u8>, String> {
    let list = Skiplist::decode_of(ChangesetId(key), value).map_err(|e| e.to_string())?;
    let mut out = Vec::with_capacity(4 + 3 * list.len());
    put_varint(&mut out, list.len() as u64);
    for e in &list {
        put_varint(&mut out, refer(&e.id.0));
    }
    let mut prev = None;
    for e in &list {
        match prev {
            None => put_varint(&mut out, e.generation),
            Some(p) => put_varint(&mut out, p - e.generation),
        }
        prev = Some(e.generation);
    }
    Ok(out)
}

/// `table length, id reference*, then part 0's bytes after its table`. The
/// checksum is left out: it is a function of the key and those bytes.
fn encode_history(
    key: Hash,
    value: &[u8],
    refer: &mut dyn FnMut(&Hash) -> u64,
) -> std::result::Result<Vec<u8>, String> {
    let cs = ChangesetId(key);
    history_pack::decode_root(cs, value).map_err(|e| e.to_string())?;
    let rest = history_pack::canonical(value).map_err(|e| e.to_string())?;
    let mut r = Reader::new(&rest);
    let t = r.count(32).map_err(|e| e.to_string())?;
    let mut out = Vec::with_capacity(rest.len());
    put_varint(&mut out, t as u64);
    for _ in 0..t {
        let id = r.hash().map_err(|e| e.to_string())?;
        put_varint(&mut out, refer(&id));
    }
    out.extend_from_slice(&rest[r.pos..]);
    Ok(out)
}

// --- decoding --------------------------------------------------------------

/// Where a column's block lies in the data blob.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BlockRef {
    /// The first ordinal it holds; it runs to the next block's first.
    pub first: u32,
    pub offset: u64,
    pub len: u32,
    /// Bytes once decompressed.
    pub raw: u32,
}

#[derive(Clone, Debug)]
pub struct ColumnIndex {
    pub id: u8,
    /// Bit `o` set: ordinal `o` has a value.
    present: Vec<u8>,
    pub blocks: Vec<BlockRef>,
}

impl ColumnIndex {
    fn has(&self, o: u32) -> bool {
        self.present[o as usize / 8] & (1 << (o % 8)) != 0
    }

    /// Present ordinals in `from..to`.
    fn count(&self, from: u32, to: u32) -> usize {
        (from..to).filter(|&o| self.has(o)).count()
    }
}

/// An index blob, decoded: what servers keep in memory, about 40 bytes a
/// key.
#[derive(Clone, Debug)]
pub struct SegmentIndex {
    pub hash: Hash,
    pub nonce: u64,
    pub family: u8,
    keys: Vec<Hash>,
    /// The ordinal of `keys[i]`.
    ord_of: Vec<u32>,
    /// The position in `keys` of ordinal `o`.
    key_at: Vec<u32>,
    pub columns: Vec<ColumnIndex>,
    pub data: Hash,
    pub data_len: u64,
}

/// Where a key's value of one column is: its ordinal, its block and its
/// place among the block's rows.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Loc {
    pub ord: u32,
    pub col: usize,
    pub block: usize,
    pub rank: usize,
}

pub fn decode_index(hash: &Hash, bytes: &[u8]) -> Result<SegmentIndex> {
    if Hash::of_blob(bytes) != *hash {
        return Err(bad("index address mismatch"));
    }
    parse_index(hash, bytes)
}

/// Decode an index blob and its data blob without checking addresses or
/// that the index names this data blob, then every value: for fuzzing the
/// parsers past the checksums. Never used to read a store.
#[doc(hidden)]
pub fn read_unchecked(index: &[u8], data: &[u8]) -> Result<Vec<(Hash, u8, Bytes)>> {
    let mut idx = parse_index(&Hash::of_blob(index), index)?;
    idx.data = Hash::of_blob(data);
    idx.data_len = data.len() as u64;
    for c in &idx.columns {
        if c.blocks.iter().any(|b| {
            b.offset
                .checked_add(b.len as u64)
                .is_none_or(|e| e > idx.data_len)
        }) {
            return Err(bad("block past the data blob"));
        }
    }
    let d = decode_data(&idx, &idx.data.clone(), Bytes::copy_from_slice(data))?;
    read_all(&idx, &d)
}

fn parse_index(hash: &Hash, bytes: &[u8]) -> Result<SegmentIndex> {
    let p = prefix(INDEX_KIND);
    if !bytes.starts_with(&p) {
        return Err(bad("not an index blob"));
    }
    let mut r = Reader::new(&bytes[p.len()..]);
    if r.byte()? != VERSION {
        return Err(bad("unknown index version"));
    }
    let nonce = u64::from_be_bytes(r.take(8)?.try_into().unwrap());
    let family = u8::try_from(r.varint()?).map_err(|_| bad("unknown family"))?;
    if family != FAMILY_CHANGESET {
        return Err(bad("unknown family"));
    }
    let n = r.count(33)?;
    if n as u64 > ROWS_MAX {
        return Err(bad("too many keys"));
    }
    let mut keys = Vec::with_capacity(n);
    for _ in 0..n {
        keys.push(r.hash()?);
    }
    if !keys.windows(2).all(|w| w[0] < w[1]) {
        return Err(bad("keys not sorted"));
    }
    let mut ord_of = Vec::with_capacity(n);
    let mut key_at = vec![u32::MAX; n];
    for i in 0..n {
        let o = r.u32()?;
        if o as usize >= n || key_at[o as usize] != u32::MAX {
            return Err(bad("ordinals are not a permutation"));
        }
        key_at[o as usize] = i as u32;
        ord_of.push(o);
    }
    let c = r.count(2)?;
    let mut columns: Vec<ColumnIndex> = Vec::with_capacity(c);
    for _ in 0..c {
        let id = u8::try_from(r.varint()?).map_err(|_| bad("column out of range"))?;
        if !known_column(family, id) {
            return Err(bad("unknown column"));
        }
        if columns.last().is_some_and(|l| l.id >= id) {
            return Err(bad("columns not ascending"));
        }
        let present = r.take(n.div_ceil(8))?.to_vec();
        if n % 8 != 0 && present.last().is_some_and(|b| b >> (n % 8) != 0) {
            return Err(bad("presence bits past the last key"));
        }
        let b = r.count(4)?;
        let mut blocks: Vec<BlockRef> = Vec::with_capacity(b);
        for _ in 0..b {
            let first = r.u32()?;
            let offset = r.varint()?;
            let len = r.u32()?;
            let raw = r.u32()?;
            if blocks.last().is_some_and(|l| l.first >= first) || first as usize >= n {
                return Err(bad("blocks not ascending"));
            }
            if raw as u64 > BLOCK_MAX {
                return Err(bad("block too long"));
            }
            blocks.push(BlockRef {
                first,
                offset,
                len,
                raw,
            });
        }
        let col = ColumnIndex {
            id,
            present,
            blocks,
        };
        // Every block starts at a present ordinal, the first at the first,
        // and no column is listed without values.
        let first_present = (0..n as u32).find(|&o| col.has(o));
        if first_present.is_none()
            || col.blocks.first().map(|b| b.first) != first_present
            || col.blocks.iter().any(|b| !col.has(b.first))
        {
            return Err(bad("block directory does not fit the presence bits"));
        }
        columns.push(col);
    }
    let data = r.hash()?;
    let data_len = r.varint()?;
    if r.pos != r.b.len() {
        return Err(bad("trailing bytes in index"));
    }
    for col in &columns {
        if col.blocks.iter().any(|b| {
            b.offset
                .checked_add(b.len as u64)
                .is_none_or(|e| e > data_len)
        }) {
            return Err(bad("block past the data blob"));
        }
    }
    Ok(SegmentIndex {
        hash: *hash,
        nonce,
        family,
        keys,
        ord_of,
        key_at,
        columns,
        data,
        data_len,
    })
}

impl SegmentIndex {
    pub fn len(&self) -> usize {
        self.keys.len()
    }

    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    /// The keys, ascending.
    pub fn keys(&self) -> &[Hash] {
        &self.keys
    }

    /// The keys with a value of `col`, ascending.
    pub fn keys_with(&self, col: u8) -> impl Iterator<Item = &Hash> + '_ {
        let c = self.columns.iter().find(|c| c.id == col);
        self.keys
            .iter()
            .zip(&self.ord_of)
            .filter(move |(_, o)| c.is_some_and(|c| c.has(**o)))
            .map(|(k, _)| k)
    }

    /// Bit `c` for each column `c` it holds.
    pub fn column_mask(&self) -> u64 {
        self.columns.iter().fold(0, |m, c| m | 1 << c.id)
    }

    /// Where `key`'s value of `col` is, if the segment has one.
    pub fn locate(&self, key: &Hash, col: u8) -> Option<Loc> {
        let i = self.keys.binary_search(key).ok()?;
        let ci = self.columns.iter().position(|c| c.id == col)?;
        self.locate_ord(self.ord_of[i], ci)
    }

    fn locate_ord(&self, o: u32, ci: usize) -> Option<Loc> {
        let c = &self.columns[ci];
        if !c.has(o) {
            return None;
        }
        let block = c.blocks.partition_point(|b| b.first <= o) - 1;
        Some(Loc {
            ord: o,
            col: ci,
            block,
            rank: c.count(c.blocks[block].first, o),
        })
    }

    /// Whether `key` has a value of `col` here.
    pub fn has(&self, key: &Hash, col: u8) -> bool {
        self.locate(key, col).is_some()
    }

    /// The key at ordinal `o`.
    fn key_of(&self, o: u32) -> Hash {
        self.keys[self.key_at[o as usize] as usize]
    }

    /// Rows block `block` of column `ci` holds.
    fn block_rows(&self, ci: usize, block: usize) -> usize {
        let c = &self.columns[ci];
        let end = c
            .blocks
            .get(block + 1)
            .map_or(self.keys.len() as u32, |b| b.first);
        c.count(c.blocks[block].first, end)
    }

    /// Bytes held in memory, about.
    pub fn memory(&self) -> usize {
        self.keys.len() * 40
            + self
                .columns
                .iter()
                .map(|c| c.present.len() + c.blocks.len() * 24)
                .sum::<usize>()
    }
}

/// A data blob, checked against its index.
#[derive(Clone, Debug)]
pub struct SegmentData {
    pub hash: Hash,
    ext: Vec<Hash>,
    bytes: Bytes,
}

impl SegmentData {
    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }
}

pub fn decode_data(idx: &SegmentIndex, hash: &Hash, bytes: Bytes) -> Result<SegmentData> {
    if *hash != idx.data || Hash::of_blob(&bytes) != *hash {
        return Err(bad("data address mismatch"));
    }
    if bytes.len() as u64 != idx.data_len {
        return Err(bad("data length mismatch"));
    }
    let p = prefix(DATA_KIND);
    if !bytes.starts_with(&p) {
        return Err(bad("not a data blob"));
    }
    let mut r = Reader::new(&bytes[p.len()..]);
    if r.byte()? != VERSION {
        return Err(bad("unknown data version"));
    }
    if u64::from_be_bytes(r.take(8)?.try_into().unwrap()) != idx.nonce {
        return Err(bad("data blob of another index"));
    }
    let m = r.count(32)?;
    let mut ext = Vec::with_capacity(m);
    for _ in 0..m {
        let id = r.hash()?;
        // An id in the segment before every row would be a reference,
        // never external; one that is a key here is still allowed (a
        // later ancestor), but never the row's own key (checked on read).
        ext.push(id);
    }
    let header = (p.len() + r.pos) as u64;
    for c in &idx.columns {
        if c.blocks.iter().any(|b| b.offset < header) {
            return Err(bad("block inside the data header"));
        }
    }
    Ok(SegmentData {
        hash: *hash,
        ext,
        bytes,
    })
}

/// A decompressed block: its bytes and where each row lies.
#[derive(Debug)]
pub struct Block {
    raw: Vec<u8>,
    rows: Vec<(u32, u32)>,
}

impl Block {
    pub fn row(&self, rank: usize) -> Option<&[u8]> {
        let (s, e) = *self.rows.get(rank)?;
        Some(&self.raw[s as usize..e as usize])
    }

    /// Bytes held in memory, about.
    pub fn memory(&self) -> usize {
        self.raw.len() + self.rows.len() * 8 + 64
    }
}

/// Decompress block `block` of column `ci` and split it into its rows.
pub fn read_block(
    idx: &SegmentIndex,
    data: &SegmentData,
    ci: usize,
    block: usize,
) -> Result<Block> {
    let b = idx.columns[ci].blocks[block];
    let start = b.offset as usize;
    let packed = data
        .bytes
        .get(start..start + b.len as usize)
        .ok_or_else(|| bad("block past the data blob"))?;
    let raw =
        zstd::bulk::decompress(packed, b.raw as usize).map_err(|e| bad(&format!("zstd: {e}")))?;
    if raw.len() != b.raw as usize {
        return Err(bad("block length mismatch"));
    }
    let want = idx.block_rows(ci, block);
    let mut rows = Vec::with_capacity(want);
    let mut r = Reader::new(&raw);
    for _ in 0..want {
        let len = r.count(1)?;
        let s = r.pos;
        r.take(len)?;
        rows.push((s as u32, r.pos as u32));
    }
    if r.pos != raw.len() {
        return Err(bad("trailing bytes in block"));
    }
    Ok(Block { raw, rows })
}

/// The value the row of the key at `ord` had, from its encoding `row` in
/// column `col`.
pub fn row_value(
    idx: &SegmentIndex,
    data: &SegmentData,
    col: u8,
    ord: u32,
    row: &[u8],
) -> Result<Bytes> {
    let key = idx.key_of(ord);
    let mut r = Reader::new(row);
    let id = |r: &mut Reader| -> Result<Hash> {
        let v = r.varint()?;
        let id = if v & 1 == 0 {
            let back = v >> 1;
            if back >= ord as u64 {
                return Err(bad("reference before the first key"));
            }
            idx.key_of(ord - 1 - back as u32)
        } else {
            *data
                .ext
                .get((v >> 1) as usize)
                .ok_or_else(|| bad("external id out of range"))?
        };
        if id == key {
            return Err(bad("a row names its own key"));
        }
        Ok(id)
    };
    match col {
        COL_SKIPLIST => {
            let k = r.count(2)?;
            let mut ids = Vec::with_capacity(k);
            for _ in 0..k {
                ids.push(id(&mut r)?);
            }
            let mut out = Vec::with_capacity(40 * k);
            let mut prev: Option<u64> = None;
            for i in ids {
                let g = match prev {
                    None => r.varint()?,
                    Some(p) => {
                        let d = r.varint()?;
                        if d == 0 || d > p {
                            return Err(bad("skiplist generations do not descend"));
                        }
                        p - d
                    }
                };
                prev = Some(g);
                out.extend_from_slice(&i.0);
                out.extend_from_slice(&g.to_be_bytes());
            }
            if r.pos != row.len() {
                return Err(bad("trailing bytes in a skiplist row"));
            }
            Ok(Bytes::from(out))
        }
        COL_HISTORY_TREE => {
            let t = r.count(1)?;
            let mut rest = Vec::with_capacity(row.len() + 32 * t);
            put_varint(&mut rest, t as u64);
            for _ in 0..t {
                rest.extend_from_slice(&id(&mut r)?.0);
            }
            rest.extend_from_slice(&row[r.pos..]);
            Ok(history_pack::part0_from_rest(ChangesetId(key), &rest))
        }
        _ => Err(bad("unknown column")),
    }
}

/// Every value in the segment: `(key, column, value)`, by column then
/// ordinal. For merges, `verify` and tests.
pub fn read_all(idx: &SegmentIndex, data: &SegmentData) -> Result<Vec<(Hash, u8, Bytes)>> {
    let mut out = Vec::with_capacity(idx.len() * idx.columns.len());
    for (ci, c) in idx.columns.iter().enumerate() {
        for bi in 0..c.blocks.len() {
            let block = read_block(idx, data, ci, bi)?;
            let end = c.blocks.get(bi + 1).map_or(idx.len() as u32, |b| b.first);
            let mut rank = 0;
            for o in c.blocks[bi].first..end {
                if !c.has(o) {
                    continue;
                }
                let row = block.row(rank).ok_or_else(|| bad("block short"))?;
                rank += 1;
                out.push((idx.key_of(o), c.id, row_value(idx, data, c.id, o, row)?));
            }
        }
    }
    Ok(out)
}

/// The kind each column of `changeset-v1` holds.
pub fn column_kind(col: u8) -> Option<&'static str> {
    match col {
        COL_SKIPLIST => Some(Skiplist::NAME),
        COL_HISTORY_TREE => Some(HistoryTree::NAME),
        _ => None,
    }
}

/// The column of `changeset-v1` holding `kind`.
pub fn kind_column(kind: &str) -> Option<u8> {
    match kind {
        k if k == Skiplist::NAME => Some(COL_SKIPLIST),
        k if k == HistoryTree::NAME => Some(COL_HISTORY_TREE),
        _ => None,
    }
}
