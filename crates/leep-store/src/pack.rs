//! The pack file format (ADR 0029): many objects in one immutable file,
//! each a zstd frame, stored whole or compressed against a base object in the
//! same pack. The layout is in docs/FORMATS.md, "Packs".

use leep_core::{Error, Hash, Result, TreeNode};
use zstd::zstd_safe::{self, CParameter, DParameter};

/// First bytes of a `.pack` file.
pub const PACK_MAGIC: [u8; 8] = *b"LEEPPAK1";
/// First bytes of a `.idx` file.
pub const IDX_MAGIC: [u8; 8] = *b"LEEPIDX1";
/// Magic, record count, pack length, BLAKE3 of the pack.
pub const IDX_HEADER_LEN: usize = 8 + 8 + 8 + 32;
/// Hash, offset, stored length, logical length, base, modified.
pub const IDX_RECORD_LEN: usize = 32 + 8 + 8 + 8 + 32 + 8;
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
    /// The object the frame was compressed against, in the same pack.
    pub base: Option<Hash>,
    /// Unix seconds the object was last written before it was packed. Kept
    /// per object so a repack does not make old garbage look new to GC.
    pub modified: i64,
}

/// A parsed `.idx`.
#[derive(Debug, Clone)]
pub struct PackIndex {
    pub pack_len: u64,
    pub pack_hash: [u8; 32],
    /// Strictly ascending by hash.
    pub entries: Vec<IdxEntry>,
}

/// The pack id: the hex BLAKE3 of the whole index file.
pub fn pack_id(idx_bytes: &[u8]) -> String {
    hex::encode(blake3::hash(idx_bytes).as_bytes())
}

impl PackIndex {
    pub fn find(&self, hash: &Hash) -> Option<&IdxEntry> {
        self.entries
            .binary_search_by(|e| e.hash.cmp(hash))
            .ok()
            .map(|i| &self.entries[i])
    }

    /// Serialize. `entries` must be strictly ascending by hash.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(
            IDX_HEADER_LEN + self.entries.len() * IDX_RECORD_LEN + IDX_TRAILER_LEN,
        );
        out.extend_from_slice(&IDX_MAGIC);
        out.extend_from_slice(&(self.entries.len() as u64).to_be_bytes());
        out.extend_from_slice(&self.pack_len.to_be_bytes());
        out.extend_from_slice(&self.pack_hash);
        for e in &self.entries {
            out.extend_from_slice(e.hash.as_bytes());
            out.extend_from_slice(&e.offset.to_be_bytes());
            out.extend_from_slice(&e.stored_len.to_be_bytes());
            out.extend_from_slice(&e.len.to_be_bytes());
            out.extend_from_slice(e.base.as_ref().map_or(&[0u8; 32], |b| b.as_bytes()));
            out.extend_from_slice(&e.modified.to_be_bytes());
        }
        let sum = blake3::hash(&out);
        out.extend_from_slice(sum.as_bytes());
        out
    }

    /// Parse and check an index: checksum, order, bounds, and that every
    /// base is an earlier entry of the same pack (so chains end).
    pub fn decode(bytes: &[u8]) -> Result<PackIndex> {
        let bad = |why: &str| Error::Storage(format!("corrupt pack index: {why}"));
        if bytes.len() < IDX_HEADER_LEN + IDX_TRAILER_LEN || bytes[..8] != IDX_MAGIC {
            return Err(bad("bad magic or too short"));
        }
        let (body, sum) = bytes.split_at(bytes.len() - IDX_TRAILER_LEN);
        if blake3::hash(body).as_bytes() != sum {
            return Err(bad("checksum mismatch"));
        }
        let u64_at = |b: &[u8], at: usize| u64::from_be_bytes(b[at..at + 8].try_into().unwrap());
        let count = u64_at(body, 8);
        let pack_len = u64_at(body, 16);
        let pack_hash: [u8; 32] = body[24..56].try_into().unwrap();
        let records = &body[IDX_HEADER_LEN..];
        if count.checked_mul(IDX_RECORD_LEN as u64) != Some(records.len() as u64) {
            return Err(bad("record count does not match length"));
        }
        let mut entries = Vec::with_capacity(count as usize);
        for r in records.chunks_exact(IDX_RECORD_LEN) {
            let base: [u8; 32] = r[56..88].try_into().unwrap();
            let e = IdxEntry {
                hash: Hash(r[..32].try_into().unwrap()),
                offset: u64_at(r, 32),
                stored_len: u64_at(r, 40),
                len: u64_at(r, 48),
                base: (base != [0; 32]).then_some(Hash(base)),
                modified: i64::from_be_bytes(r[88..96].try_into().unwrap()),
            };
            if entries.last().is_some_and(|p: &IdxEntry| p.hash >= e.hash) {
                return Err(bad("records out of order"));
            }
            let end = e.offset.checked_add(e.stored_len);
            if e.offset < PACK_MAGIC.len() as u64 || end.is_none_or(|end| end > pack_len) {
                return Err(bad("entry outside the pack"));
            }
            entries.push(e);
        }
        let idx = PackIndex {
            pack_len,
            pack_hash,
            entries,
        };
        for e in &idx.entries {
            if let Some(b) = e.base {
                match idx.find(&b) {
                    Some(base) if base.offset < e.offset => {}
                    _ => return Err(bad("delta base missing or not earlier in the pack")),
                }
            }
        }
        Ok(idx)
    }
}

fn zstd_err(what: &str, code: usize) -> Error {
    Error::Storage(format!("{what}: {}", zstd_safe::get_error_name(code)))
}

/// Compress `data` alone.
pub fn compress_whole(data: &[u8], level: i32) -> Result<Vec<u8>> {
    zstd::bulk::compress(data, level).map_err(|e| Error::Storage(format!("zstd: {e}")))
}

/// Compress `data` with `base` as a reference prefix (zstd's `--patch-from`
/// mode). A prefix, unlike a loaded dictionary, is always taken as raw
/// content, even if the base happens to start with the dictionary magic.
/// `None` when the pair is too large for one decoder window.
pub fn compress_delta(base: &[u8], data: &[u8], level: i32) -> Result<Option<Vec<u8>>> {
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
    // The level's default window can be smaller than base plus object, which
    // would hide the start of the base from the matcher.
    set(&mut cctx, CParameter::WindowLog(window_log.max(10)))?;
    cctx.ref_prefix(base)
        .map_err(|c| zstd_err("zstd prefix", c))?;
    let mut out = Vec::with_capacity(zstd_safe::compress_bound(data.len()));
    cctx.compress2(&mut out, data)
        .map_err(|c| zstd_err("zstd compress", c))?;
    Ok(Some(out))
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

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> PackIndex {
        let mut entries = vec![
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
                base: Some(Hash([0x11; 32])),
                modified: 1_700_000_001,
            },
        ];
        entries.sort_by_key(|e| e.hash);
        PackIndex {
            pack_len: 33,
            pack_hash: [0xab; 32],
            entries,
        }
    }

    /// The index layout is persisted: these bytes must never change. A new
    /// layout needs a new magic (see docs/FORMATS.md).
    #[test]
    fn index_golden_bytes() {
        let bytes = sample().encode();
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
        assert_eq!(bytes, want);
        assert_eq!(bytes.len(), IDX_HEADER_LEN + 2 * IDX_RECORD_LEN + 32);
        assert_eq!(
            pack_id(&bytes),
            "25b80a7692278d93d0b96200de40de81063b86f202bec22907075c65cec94ea5"
        );
        let back = PackIndex::decode(&bytes).unwrap();
        assert_eq!(back.entries, sample().entries);
        assert_eq!((back.pack_len, back.pack_hash), (33, [0xab; 32]));
    }

    #[test]
    fn damaged_indexes_are_refused() {
        let good = sample().encode();
        let mut flipped = good.clone();
        flipped[70] ^= 1;
        assert!(PackIndex::decode(&flipped).is_err(), "checksum");
        assert!(PackIndex::decode(&good[..good.len() - 1]).is_err(), "torn");
        let mut magic = good.clone();
        magic[0] = b'X';
        assert!(PackIndex::decode(&magic).is_err(), "magic");
        // A base that is not earlier in the pack could form a cycle.
        let mut idx = sample();
        idx.entries[0].base = Some(Hash([0x22; 32]));
        idx.entries[1].base = None;
        assert!(PackIndex::decode(&idx.encode()).is_err(), "later base");
        let mut idx = sample();
        idx.entries[1].stored_len = 6;
        assert!(PackIndex::decode(&idx.encode()).is_err(), "past the end");
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
