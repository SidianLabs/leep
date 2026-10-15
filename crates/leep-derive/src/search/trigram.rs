//! Trigram sets and the directory summary filter. Both encodings are
//! persisted, so changing one means a new kind name (FORMATS.md).

use leep_core::{Error, Result};

/// Files above this size are not indexed and not searched. Bounds the cost of
/// one index row and of one blob fetch while matching.
pub const MAX_INDEXED_BYTES: u64 = 1 << 20;

/// Three bytes as `b0 << 16 | b1 << 8 | b2`.
pub fn code(a: u8, b: u8, c: u8) -> u32 {
    (a as u32) << 16 | (b as u32) << 8 | c as u32
}

/// Whether a blob is searched: UTF-8 without NUL bytes.
pub fn is_text(data: &[u8]) -> bool {
    !data.contains(&0) && std::str::from_utf8(data).is_ok()
}

/// Sorted, deduplicated trigram codes of `data`.
pub fn extract(data: &[u8]) -> Vec<u32> {
    let mut v: Vec<u32> = data.windows(3).map(|w| code(w[0], w[1], w[2])).collect();
    v.sort_unstable();
    v.dedup();
    v
}

/// Why a blob has no trigram set.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Skip {
    /// Contains NUL or is not valid UTF-8.
    Binary,
    TooLarge,
}

/// Value of `blob-trigrams-v1`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BlobRow {
    NotIndexed(Skip),
    Trigrams(Vec<u32>),
}

const NOT_INDEXED: u8 = 0;
const INDEXED: u8 = 1;

fn put_varint(out: &mut Vec<u8>, mut v: u32) {
    while v >= 0x80 {
        out.push(v as u8 | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

/// Reads one LEB128 `u32`; rejects overlong and out-of-range encodings so a
/// value has one encoding.
fn get_varint(b: &[u8], at: &mut usize) -> Result<u32> {
    if let Some(&byte) = b.get(*at) {
        if byte < 0x80 {
            *at += 1;
            return Ok(byte as u32);
        }
    }
    let mut v = 0u32;
    for shift in (0..35).step_by(7) {
        let byte = *b
            .get(*at)
            .ok_or_else(|| Error::Encoding("truncated varint".into()))?;
        *at += 1;
        let bits = (byte & 0x7f) as u32;
        if shift == 28 && bits > 0x0f {
            return Err(Error::Encoding("varint overflows u32".into()));
        }
        v |= bits << shift;
        if byte & 0x80 == 0 {
            if byte == 0 && shift > 0 {
                return Err(Error::Encoding("overlong varint".into()));
            }
            return Ok(v);
        }
    }
    Err(Error::Encoding("varint too long".into()))
}

/// Strictly increasing values as deltas; the first is relative to zero.
fn put_deltas(out: &mut Vec<u8>, sorted: impl Iterator<Item = u32>) {
    let mut prev = 0u32;
    for (i, v) in sorted.enumerate() {
        put_varint(out, if i == 0 { v } else { v - prev });
        prev = v;
    }
}

fn get_deltas(b: &[u8], limit: u32) -> Result<Vec<u32>> {
    let mut out = Vec::with_capacity(b.len());
    let (mut at, mut prev) = (0usize, 0u32);
    while at < b.len() {
        let d = get_varint(b, &mut at)?;
        let v = if out.is_empty() {
            d
        } else if d == 0 {
            return Err(Error::Encoding("values not strictly increasing".into()));
        } else {
            prev.checked_add(d)
                .ok_or_else(|| Error::Encoding("delta overflows".into()))?
        };
        if v >= limit {
            return Err(Error::Encoding("value out of range".into()));
        }
        out.push(v);
        prev = v;
    }
    Ok(out)
}

impl BlobRow {
    pub fn encode(&self) -> Vec<u8> {
        match self {
            BlobRow::NotIndexed(why) => vec![
                NOT_INDEXED,
                match why {
                    Skip::Binary => 1,
                    Skip::TooLarge => 2,
                },
            ],
            BlobRow::Trigrams(set) => {
                let mut out = vec![INDEXED];
                put_deltas(&mut out, set.iter().copied());
                out
            }
        }
    }

    pub fn decode(b: &[u8]) -> Result<Self> {
        match b {
            [NOT_INDEXED, 1] => Ok(BlobRow::NotIndexed(Skip::Binary)),
            [NOT_INDEXED, 2] => Ok(BlobRow::NotIndexed(Skip::TooLarge)),
            [INDEXED, rest @ ..] => Ok(BlobRow::Trigrams(get_deltas(rest, 1 << 24)?)),
            _ => Err(Error::Encoding("bad blob-trigrams row".into())),
        }
    }

    /// Compute the row for blob bytes.
    pub fn of(data: &[u8]) -> Self {
        if data.len() as u64 > MAX_INDEXED_BYTES {
            BlobRow::NotIndexed(Skip::TooLarge)
        } else if !is_text(data) {
            BlobRow::NotIndexed(Skip::Binary)
        } else {
            BlobRow::Trigrams(extract(data))
        }
    }
}

/// Filter size in bits. Fixed so any two filters merge with a bitwise OR,
/// which lets a directory be summarized from its children's rows alone.
/// 64 KiB dense; about 0.1% false positives for a directory holding 20k
/// distinct trigrams and 8% for 100k.
pub const BLOOM_BITS: usize = 1 << 19;
const WORDS: usize = BLOOM_BITS / 64;
const HASHES: u32 = 3;
const DENSE: u8 = 1;
const SPARSE: u8 = 2;

/// Bit positions of a trigram: splitmix64 of the code, then double hashing
/// over its two halves.
fn positions(code: u32) -> [usize; HASHES as usize] {
    let mut z = (code as u64).wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^= z >> 31;
    let (h1, h2) = (z as u32, (z >> 32) as u32 | 1);
    let mut out = [0; HASHES as usize];
    for (i, p) in out.iter_mut().enumerate() {
        *p = (h1.wrapping_add((i as u32).wrapping_mul(h2)) as usize) & (BLOOM_BITS - 1);
    }
    out
}

/// Probabilistic set of trigrams: no false negatives.
#[derive(Clone, PartialEq, Eq)]
pub struct Bloom {
    words: Vec<u64>,
}

impl std::fmt::Debug for Bloom {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Bloom({} bits set)", self.set_bits())
    }
}

impl Default for Bloom {
    fn default() -> Self {
        Bloom {
            words: vec![0; WORDS],
        }
    }
}

impl Bloom {
    pub fn insert(&mut self, code: u32) {
        for p in positions(code) {
            self.words[p / 64] |= 1 << (p % 64);
        }
    }

    pub fn contains(&self, code: u32) -> bool {
        positions(code)
            .iter()
            .all(|p| self.words[p / 64] >> (p % 64) & 1 == 1)
    }

    pub fn set_bits(&self) -> usize {
        self.words.iter().map(|w| w.count_ones() as usize).sum()
    }

    fn set_positions(&self) -> impl Iterator<Item = u32> + '_ {
        self.words.iter().enumerate().flat_map(|(i, &w)| {
            let mut w = w;
            std::iter::from_fn(move || {
                if w == 0 {
                    return None;
                }
                let bit = w.trailing_zeros();
                w &= w - 1;
                Some(i as u32 * 64 + bit)
            })
        })
    }

    /// Dense (tag 1, then 64 KiB, bit `p` at byte `p / 8` bit `p % 8`) or, when
    /// shorter, sparse (tag 2, then the set positions as delta varints).
    pub fn encode(&self) -> Vec<u8> {
        // A sparse position costs at least one byte, so skip the attempt when
        // it cannot win.
        if self.set_bits() < WORDS * 8 {
            let mut out = vec![SPARSE];
            put_deltas(&mut out, self.set_positions());
            if out.len() < 1 + WORDS * 8 {
                return out;
            }
        }
        let mut out = Vec::with_capacity(1 + WORDS * 8);
        out.push(DENSE);
        for w in &self.words {
            out.extend_from_slice(&w.to_le_bytes());
        }
        out
    }

    /// Merge an encoded filter into this one.
    pub fn or_encoded(&mut self, b: &[u8]) -> Result<()> {
        match b {
            [DENSE, rest @ ..] => {
                if rest.len() != WORDS * 8 {
                    return Err(Error::Encoding("bad dense filter length".into()));
                }
                for (w, c) in self.words.iter_mut().zip(rest.chunks_exact(8)) {
                    *w |= u64::from_le_bytes(c.try_into().expect("8 bytes"));
                }
                Ok(())
            }
            [SPARSE, rest @ ..] => {
                for p in get_deltas(rest, BLOOM_BITS as u32)? {
                    self.words[p as usize / 64] |= 1 << (p % 64);
                }
                Ok(())
            }
            _ => Err(Error::Encoding("bad tree-trigrams row".into())),
        }
    }

    pub fn decode(b: &[u8]) -> Result<Self> {
        let mut f = Bloom::default();
        f.or_encoded(b)?;
        Ok(f)
    }
}

/// Answers "does this row contain trigram t" for a fixed set of trigrams by
/// reading the encoded row once, without decoding it into memory. A query asks
/// the same few dozen questions of thousands of rows, so this is the hot loop
/// of an indexed search.
#[derive(Clone, Debug, Default)]
pub struct Probe {
    codes: Vec<u32>,
    /// Sorted bloom bit positions of all `codes`.
    positions: Vec<u32>,
    /// For each code, the indexes of its bits in `positions`.
    code_bits: Vec<[u32; HASHES as usize]>,
}

/// What a row holds of the probed trigrams.
pub struct Present<'a> {
    probe: &'a Probe,
    /// Per code for a blob row, per bit position for a tree row.
    flags: Vec<bool>,
    tree: bool,
}

/// Merge the strictly increasing delta-varint values in `b` against sorted
/// `needed`, setting `found[i]` for each present `needed[i]`.
fn find_sorted(b: &[u8], needed: &[u32], found: &mut [bool]) -> Result<()> {
    let (mut at, mut j, mut prev) = (0usize, 0usize, 0u32);
    let mut first = true;
    while at < b.len() && j < needed.len() {
        let d = get_varint(b, &mut at)?;
        let v = if first {
            d
        } else if d == 0 {
            return Err(Error::Encoding("values not strictly increasing".into()));
        } else {
            prev.checked_add(d)
                .ok_or_else(|| Error::Encoding("delta overflows".into()))?
        };
        (first, prev) = (false, v);
        while j < needed.len() && needed[j] < v {
            j += 1;
        }
        if j < needed.len() && needed[j] == v {
            found[j] = true;
            j += 1;
        }
    }
    Ok(())
}

impl Probe {
    pub fn new(mut codes: Vec<u32>) -> Probe {
        codes.sort_unstable();
        codes.dedup();
        let mut bits: Vec<u32> = codes
            .iter()
            .flat_map(|&c| positions(c))
            .map(|p| p as u32)
            .collect();
        bits.sort_unstable();
        bits.dedup();
        let code_bits = codes
            .iter()
            .map(|&c| positions(c).map(|p| positions_index(&bits, p as u32)))
            .collect();
        Probe {
            codes,
            positions: bits,
            code_bits,
        }
    }

    /// `None` for a row that records "not indexed".
    pub fn blob(&self, row: &[u8]) -> Result<Option<Present<'_>>> {
        match row {
            [NOT_INDEXED, 1 | 2] => Ok(None),
            [INDEXED, rest @ ..] => {
                let mut flags = vec![false; self.codes.len()];
                find_sorted(rest, &self.codes, &mut flags)?;
                Ok(Some(Present {
                    probe: self,
                    flags,
                    tree: false,
                }))
            }
            _ => Err(Error::Encoding("bad blob-trigrams row".into())),
        }
    }

    pub fn tree(&self, row: &[u8]) -> Result<Present<'_>> {
        let mut flags = vec![false; self.positions.len()];
        match row {
            [DENSE, rest @ ..] => {
                if rest.len() != WORDS * 8 {
                    return Err(Error::Encoding("bad dense filter length".into()));
                }
                for (f, &p) in flags.iter_mut().zip(&self.positions) {
                    *f = rest[p as usize / 8] >> (p % 8) & 1 == 1;
                }
            }
            [SPARSE, rest @ ..] => find_sorted(rest, &self.positions, &mut flags)?,
            _ => return Err(Error::Encoding("bad tree-trigrams row".into())),
        }
        Ok(Present {
            probe: self,
            flags,
            tree: true,
        })
    }
}

fn positions_index(sorted: &[u32], p: u32) -> u32 {
    sorted.binary_search(&p).expect("position was collected") as u32
}

impl Present<'_> {
    /// Whether the row may contain `code`. A code that was not probed is
    /// reported present, which can only keep a file or directory in play.
    pub fn has(&self, code: u32) -> bool {
        let Ok(i) = self.probe.codes.binary_search(&code) else {
            return true;
        };
        if self.tree {
            self.probe.code_bits[i]
                .iter()
                .all(|&b| self.flags[b as usize])
        } else {
            self.flags[i]
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_sorted_distinct_trigrams() {
        assert!(extract(b"").is_empty() && extract(b"ab").is_empty());
        let t = extract(b"abcabc\n");
        assert_eq!(
            t,
            [
                code(b'a', b'b', b'c'),
                code(b'b', b'c', b'\n'),
                code(b'b', b'c', b'a'),
                code(b'c', b'a', b'b'),
            ]
        );
        assert!(t.windows(2).all(|w| w[0] < w[1]));
    }

    #[test]
    fn blob_row_golden_bytes() {
        // Tag 1, then deltas from zero: 0x616263, 0x100a7, 0x57, 0xfe01.
        let row = BlobRow::of(b"abcabc\n");
        assert_eq!(
            row.encode(),
            [0x01, 0xe3, 0xc4, 0x85, 0x03, 0xa7, 0x81, 0x04, 0x57, 0x81, 0xfc, 0x03]
        );
        assert_eq!(BlobRow::decode(&row.encode()).unwrap(), row);
        assert_eq!(BlobRow::of(b"").encode(), [0x01]);
        assert_eq!(BlobRow::of(&[b'a', 0, b'b']).encode(), [0x00, 0x01]);
        assert_eq!(BlobRow::of(&[0xff, 0xfe, b'a']).encode(), [0x00, 0x01]);
        let big = vec![b'a'; MAX_INDEXED_BYTES as usize + 1];
        assert_eq!(BlobRow::of(&big).encode(), [0x00, 0x02]);
        assert_eq!(
            BlobRow::of(&big[1..]).encode(),
            [0x01, 0xe1, 0xc2, 0x85, 0x03]
        );
    }

    #[test]
    fn blob_row_rejects_malformed_bytes() {
        for bad in [
            &[][..],
            &[0x00],
            &[0x00, 0x03],
            &[0x00, 0x01, 0x00],
            &[0x02],
            &[0x01, 0x80],                         // truncated varint
            &[0x01, 0x80, 0x00],                   // overlong
            &[0x01, 0x01, 0x00],                   // zero delta after the first
            &[0xff],                               // unknown tag
            &[0x01, 0x80, 0x80, 0x80, 0x80, 0x10], // overflows u32
            &[0x01, 0x80, 0x80, 0x80, 0x08],       // 2^24, out of range
        ] {
            assert!(BlobRow::decode(bad).is_err(), "{bad:?}");
        }
        assert_eq!(
            BlobRow::decode(&[0x01, 0x00, 0x01]).unwrap(),
            BlobRow::Trigrams(vec![0, 1])
        );
    }

    #[test]
    fn bloom_has_no_false_negatives_and_few_false_positives() {
        let mut f = Bloom::default();
        let present: Vec<u32> = (0..20_000u32)
            .map(|i| i.wrapping_mul(2_654_435_761) >> 8)
            .collect();
        for &t in &present {
            f.insert(t);
        }
        assert!(present.iter().all(|&t| f.contains(t)));
        let set: std::collections::HashSet<u32> = present.iter().copied().collect();
        let (mut fp, mut n) = (0, 0);
        for i in 0..200_000u32 {
            let t = i.wrapping_mul(40_503) & 0xff_ffff;
            if set.contains(&t) {
                continue;
            }
            n += 1;
            fp += f.contains(t) as u32;
        }
        let rate = fp as f64 / n as f64;
        assert!(rate < 0.003, "false positive rate {rate}");
    }

    #[test]
    fn bloom_positions_are_frozen() {
        // These positions are part of the persisted format.
        // Checked against an independent implementation of the documented hash.
        assert_eq!(
            positions(code(b'a', b'b', b'c')),
            [433_570, 251_069, 68_568]
        );
        assert_eq!(positions(0), [380_335, 423_400, 466_465]);
    }

    #[test]
    fn bloom_encoding_round_trips_sparse_and_dense() {
        let mut f = Bloom::default();
        assert_eq!(f.encode(), [SPARSE]);
        f.insert(1);
        let sparse = f.encode();
        assert_eq!(sparse[0], SPARSE);
        assert!(sparse.len() <= 1 + 3 * 3);
        assert_eq!(Bloom::decode(&sparse).unwrap(), f);
        for t in 0..60_000u32 {
            f.insert(t * 7);
        }
        let dense = f.encode();
        assert_eq!(dense[0], DENSE);
        assert_eq!(dense.len(), 1 + BLOOM_BITS / 8);
        assert_eq!(Bloom::decode(&dense).unwrap(), f);
    }

    #[test]
    fn bloom_union_is_bitwise_or() {
        let (mut a, mut b) = (Bloom::default(), Bloom::default());
        a.insert(10);
        b.insert(20);
        let mut u = Bloom::default();
        u.or_encoded(&a.encode()).unwrap();
        u.or_encoded(&b.encode()).unwrap();
        assert!(u.contains(10) && u.contains(20) && !u.contains(30));
        assert!(Bloom::decode(&[DENSE, 0]).is_err());
        assert!(Bloom::decode(&[SPARSE, 0x80, 0x80, 0x20]).is_err()); // position >= 2^19
        assert!(Bloom::decode(&[9]).is_err());
    }

    #[test]
    fn probe_agrees_with_decoding() {
        let text = b"the quick brown fox\njumps over the lazy dog\n";
        let set = extract(text);
        let row = BlobRow::Trigrams(set.clone()).encode();
        let mut bloom = Bloom::default();
        for &t in &set {
            bloom.insert(t);
        }
        let sparse = bloom.encode();
        assert_eq!(sparse[0], SPARSE);
        for t in 0..60_000u32 {
            bloom.insert(t * 7);
        }
        let dense = bloom.encode();
        assert_eq!(dense[0], DENSE);

        let asked: Vec<u32> = [&b"the"[..], b"fox", b"zzz", b"qqq", b"azy", b"dog", b"\n"]
            .iter()
            .filter(|s| s.len() == 3)
            .map(|s| code(s[0], s[1], s[2]))
            .chain([0, 0xff_ffff, 7, 14])
            .collect();
        let probe = Probe::new(asked.clone());
        let by_blob = probe.blob(&row).unwrap().unwrap();
        let by_sparse = probe.tree(&sparse).unwrap();
        let by_dense = probe.tree(&dense).unwrap();
        let sparse_bloom = Bloom::decode(&sparse).unwrap();
        let dense_bloom = Bloom::decode(&dense).unwrap();
        for &c in &asked {
            assert_eq!(by_blob.has(c), set.binary_search(&c).is_ok(), "{c:x}");
            assert_eq!(by_sparse.has(c), sparse_bloom.contains(c), "{c:x}");
            assert_eq!(by_dense.has(c), dense_bloom.contains(c), "{c:x}");
        }
        // Codes nobody asked about stay in play.
        assert!(by_blob.has(code(b'x', b'y', b'z')));
        assert!(probe
            .blob(&BlobRow::NotIndexed(Skip::Binary).encode())
            .unwrap()
            .is_none());
        assert!(probe.blob(&[INDEXED, 0x01, 0x00]).is_err());
        assert!(probe.tree(&[DENSE]).is_err());
        assert!(probe.blob(&[9]).is_err());
    }
}
