//! How a changeset row's `data` is stored (ADR 0053). A row holds either the
//! changeset's canonical postcard bytes, as every row did before, or a zstd
//! frame of exactly those bytes compressed with one of the database's own
//! dictionaries (`changeset_dicts`), named by the frame's dictionary id.
//! Either way the reader rebuilds the canonical bytes and checks they hash to
//! the row's id, so the encoding can never change an id or let a wrong
//! changeset through.

use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use leep_core::{Changeset, ChangesetId, Error, Result};
use zstd::dict::{DecoderDictionary, EncoderDictionary};
use zstd::zstd_safe::{CCtx, DCtx};

/// First bytes of every zstd frame.
pub(crate) const ZSTD_MAGIC: [u8; 4] = [0x28, 0xB5, 0x2F, 0xFD];

/// First bytes of a zstd dictionary; its id follows, little-endian.
const DICT_MAGIC: [u8; 4] = [0x37, 0xA4, 0x30, 0xEC];

/// zstd level for new rows. On git/git (PERFORMANCE.md, "Changeset
/// compression") it is 6% smaller than level 3 and costs about 40 µs a row
/// to write instead of 6, next to the milliseconds a create takes; reading
/// costs about 2 µs at any level.
pub const LEVEL: i32 = 9;

/// Largest changeset a compressed row may decode to; larger changesets are
/// stored plain. Part of the row format (FORMATS.md): a reader with a lower
/// bound could not read rows a writer with a higher one compressed. It is
/// below the largest changeset a default server accepts (a JSON body of at
/// most `DEFAULT_MAX_BODY_BYTES`, 512 MiB, is always longer than the
/// changeset's canonical bytes). A frame header's claimed size is never
/// trusted for more than [`ONE_SHOT_MAX`] of memory up front.
pub const MAX_DECODED: usize = 256 << 20;

/// How often a writer looks for a dictionary added since it last looked.
pub(crate) const REFRESH: Duration = Duration::from_secs(60);

/// One stored dictionary, prepared for both directions.
struct Dict {
    id: u32,
    enc: EncoderDictionary<'static>,
    dec: DecoderDictionary<'static>,
}

#[derive(Default)]
struct Dicts {
    by_id: HashMap<u32, Arc<Dict>>,
    /// The one new rows are written with: the highest id.
    newest: Option<Arc<Dict>>,
}

/// The dictionaries a store knows, shared by its connections.
pub(crate) struct Codec {
    dicts: RwLock<Dicts>,
    checked: RwLock<Instant>,
}

impl Default for Codec {
    fn default() -> Self {
        Codec {
            dicts: RwLock::default(),
            checked: RwLock::new(Instant::now()),
        }
    }
}

impl Codec {
    /// Learn `rows` (`(id, dictionary bytes)`), keeping those already known.
    pub(crate) fn add(&self, rows: Vec<(i64, Vec<u8>)>) -> Result<()> {
        let mut prepared = Vec::new();
        for (id, data) in rows {
            let id = u32::try_from(id).map_err(|_| {
                Error::Storage(format!("changeset dictionary id {id} out of range"))
            })?;
            if self.dicts.read().unwrap().by_id.contains_key(&id) {
                continue;
            }
            check_dictionary(id, &data)?;
            prepared.push(Arc::new(Dict {
                id,
                enc: EncoderDictionary::copy(&data, LEVEL),
                dec: DecoderDictionary::copy(&data),
            }));
        }
        let mut d = self.dicts.write().unwrap();
        for dict in prepared {
            if d.newest.as_ref().is_none_or(|n| n.id < dict.id) {
                d.newest = Some(dict.clone());
            }
            d.by_id.insert(dict.id, dict);
        }
        *self.checked.write().unwrap() = Instant::now();
        Ok(())
    }

    pub(crate) fn knows(&self, id: u32) -> bool {
        self.dicts.read().unwrap().by_id.contains_key(&id)
    }

    /// Highest dictionary id known, 0 for none.
    pub(crate) fn newest_id(&self) -> u32 {
        self.dicts
            .read()
            .unwrap()
            .newest
            .as_ref()
            .map_or(0, |d| d.id)
    }

    /// Whether a writer should look for a newer dictionary before writing.
    pub(crate) fn due(&self) -> bool {
        self.checked.read().unwrap().elapsed() >= REFRESH
    }

    pub(crate) fn mark_checked(&self) {
        *self.checked.write().unwrap() = Instant::now();
    }

    /// A dictionary that one of `rows` needs and this codec lacks, if any.
    pub(crate) fn missing<'a>(&self, rows: impl IntoIterator<Item = &'a [u8]>) -> Option<u32> {
        let d = self.dicts.read().unwrap();
        rows.into_iter()
            .filter_map(frame_dict_id)
            .find(|id| *id != 0 && !d.by_id.contains_key(id))
    }

    /// The row bytes to store for `cs`: compressed with the newest dictionary
    /// when that is smaller, else plain. Plain bytes that would read as a
    /// zstd frame are always compressed, so a row starting with the magic is
    /// a frame whenever this code wrote it.
    pub(crate) fn encode(&self, cs: &Changeset) -> Vec<u8> {
        let plain = cs.encode();
        let newest = self.dicts.read().unwrap().newest.clone();
        let must = plain.starts_with(&ZSTD_MAGIC);
        if (newest.is_none() && !must) || plain.len() > MAX_DECODED {
            return plain;
        }
        let packed = compress(&plain, newest.as_deref());
        if packed.len() < plain.len() || must {
            packed
        } else {
            plain
        }
    }

    /// Decode a stored row and check it hashes to `id`.
    pub(crate) fn decode_checked(&self, id: &ChangesetId, data: &[u8]) -> Result<Changeset> {
        if data.starts_with(&ZSTD_MAGIC) {
            let dict_id = frame_dict_id(data).unwrap_or(0);
            let dict = if dict_id == 0 {
                None
            } else {
                self.dicts.read().unwrap().by_id.get(&dict_id).cloned()
            };
            if dict_id == 0 || dict.is_some() {
                if let Some(cs) = decompress(data, dict.as_deref())
                    .and_then(|bytes| Changeset::decode(&bytes).ok())
                    .filter(|cs| cs.id() == *id)
                {
                    return Ok(cs);
                }
            }
            // A plain row from before ADR 0053 can start with the magic (a
            // changeset of 40 parents whose first id begins b5 2f fd).
            if let Some(cs) = Changeset::decode(data).ok().filter(|cs| cs.id() == *id) {
                return Ok(cs);
            }
            if dict_id != 0 && dict.is_none() {
                return Err(Error::Storage(format!(
                    "changeset row {} needs dictionary {dict_id}, which changeset_dicts lacks",
                    id.0.to_hex()
                )));
            }
            return Err(corrupt(id));
        }
        let cs = Changeset::decode(data)?;
        if cs.id() != *id {
            return Err(corrupt(id));
        }
        Ok(cs)
    }

    /// Whether `data` is already compressed with dictionary `dict_id`.
    pub(crate) fn uses(data: &[u8], dict_id: u32) -> bool {
        data.starts_with(&ZSTD_MAGIC) && frame_dict_id(data) == Some(dict_id)
    }
}

fn corrupt(id: &ChangesetId) -> Error {
    Error::Storage(format!("corrupt changeset row {}", id.0.to_hex()))
}

/// The dictionary id a frame names (0: none), or `None` for bytes that are
/// not a frame.
pub(crate) fn frame_dict_id(data: &[u8]) -> Option<u32> {
    if !data.starts_with(&ZSTD_MAGIC) {
        return None;
    }
    Some(zstd::zstd_safe::get_dict_id_from_frame(data).map_or(0, |n| n.get()))
}

thread_local! {
    // A context is a few hundred KB to set up; a row takes microseconds to
    // code, so each thread keeps one of each.
    static CCTX: RefCell<CCtx<'static>> = RefCell::new(CCtx::create());
    static DCTX: RefCell<DCtx<'static>> = RefCell::new(DCtx::create());
}

fn compress(plain: &[u8], dict: Option<&Dict>) -> Vec<u8> {
    let mut out = Vec::with_capacity(zstd::zstd_safe::compress_bound(plain.len()));
    CCTX.with(|c| {
        let mut c = c.borrow_mut();
        // Both record the content size and the dictionary id, and leave out
        // the checksum, which the id check makes redundant.
        match dict {
            Some(d) => c.compress_using_cdict(&mut out, plain, d.enc.as_cdict()),
            None => c.compress(&mut out, plain, LEVEL),
        }
    })
    .expect("zstd compression into a buffer of the bound's size");
    out
}

fn decompress(data: &[u8], dict: Option<&Dict>) -> Option<Vec<u8>> {
    let size = zstd::zstd_safe::get_frame_content_size(data).ok()??;
    let size = usize::try_from(size).ok().filter(|s| *s <= MAX_DECODED)?;
    if size > ONE_SHOT_MAX {
        return decompress_streaming(data, dict, size);
    }
    let mut out = Vec::with_capacity(size);
    DCTX.with(|d| {
        let mut d = d.borrow_mut();
        match dict {
            Some(dict) => d.decompress_using_ddict(&mut out, data, dict.dec.as_ddict()),
            None => d.decompress(&mut out, data),
        }
    })
    .ok()?;
    (out.len() == size).then_some(out)
}

/// Largest frame decoded into a buffer reserved up front from the size its
/// header claims. The header is unchecked until the bytes hash to the id, so
/// a damaged one must not reserve more than this; real rows are a few KB.
const ONE_SHOT_MAX: usize = 1 << 20;

/// Largest window a streamed frame may use, as a power of two. A streaming
/// decoder allocates its window up front, and a single-segment frame's
/// window is its claimed size, so this bounds that allocation too. Level 9
/// uses a 4 MiB window (2^22), with or without a dictionary, and a frame
/// is single-segment only when its content fits that window.
const WINDOW_LOG_MAX: u32 = 23;

/// Decode a frame claiming more than [`ONE_SHOT_MAX`], growing the buffer
/// only as bytes actually come out, so a false header costs nothing.
fn decompress_streaming(data: &[u8], dict: Option<&Dict>, size: usize) -> Option<Vec<u8>> {
    use std::io::Read;
    let decoder = match dict {
        Some(dict) => zstd::stream::read::Decoder::with_prepared_dictionary(data, &dict.dec),
        None => zstd::stream::read::Decoder::with_buffer(data),
    };
    let mut decoder = decoder.ok()?.single_frame();
    decoder.window_log_max(WINDOW_LOG_MAX).ok()?;
    let mut out = Vec::with_capacity(ONE_SHOT_MAX);
    decoder.take(size as u64 + 1).read_to_end(&mut out).ok()?;
    (out.len() == size).then_some(out)
}

/// A dictionary row must be a zstd dictionary carrying its own row id, so a
/// frame's id always names the dictionary it was made with.
fn check_dictionary(id: u32, data: &[u8]) -> Result<()> {
    if id == 0 || data.len() < 8 || data[..4] != DICT_MAGIC || data[4..8] != id.to_le_bytes() {
        return Err(Error::Storage(format!(
            "changeset dictionary {id} is not a zstd dictionary with that id"
        )));
    }
    Ok(())
}

/// Train a dictionary of at most `size` bytes on canonical changeset bytes
/// and give it id `id`. zstd stores the id in the dictionary's header and
/// copies it into every frame made with it.
pub fn train(samples: &[Vec<u8>], size: usize, id: u32) -> Result<Vec<u8>> {
    if id == 0 {
        return Err(Error::Invalid("dictionary id 0 means no dictionary".into()));
    }
    let mut dict = zstd::dict::from_samples(samples, size)
        .map_err(|e| Error::Other(format!("training a changeset dictionary: {e}")))?;
    if dict.len() < 8 || dict[..4] != DICT_MAGIC {
        return Err(Error::Other(
            "zstd returned a dictionary without its header".into(),
        ));
    }
    dict[4..8].copy_from_slice(&id.to_le_bytes());
    Ok(dict)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use leep_core::{FileChange, FileKind, Hash, RepoPath};

    use super::*;

    fn cs(i: u32, parents: Vec<ChangesetId>) -> Changeset {
        let mut changes = BTreeMap::new();
        for f in 0..(i % 5 + 1) {
            changes.insert(
                RepoPath::new(format!("src/module{}/file{f}.rs", i % 7)).unwrap(),
                FileChange::Modify {
                    content: Hash::of_blob(&i.to_le_bytes()),
                    kind: FileKind::Regular,
                    size: u64::from(i) * 13,
                },
            );
        }
        Changeset {
            parents,
            author: format!("Dev {} <dev{}@example.com>", i % 3, i % 3),
            timestamp: 1_700_000_000 + i64::from(i),
            message: format!(
                "module{}: fix the thing number {i}\n\nSigned-off-by: Dev {} <dev{}@example.com>\n",
                i % 7,
                i % 3,
                i % 3
            ),
            changes,
            extra: BTreeMap::from([("git.commit".into(), format!("{i:040x}"))]),
        }
    }

    fn history(n: u32) -> Vec<Changeset> {
        let mut out: Vec<Changeset> = Vec::new();
        for i in 0..n {
            let parents = out.last().map(|p| vec![p.id()]).unwrap_or_default();
            out.push(cs(i, parents));
        }
        out
    }

    fn trained(id: u32) -> Vec<u8> {
        let samples: Vec<Vec<u8>> = history(2000).iter().map(Changeset::encode).collect();
        train(&samples, 16 << 10, id).unwrap()
    }

    #[test]
    fn plain_without_a_dictionary() {
        let codec = Codec::default();
        let c = cs(1, vec![]);
        let row = codec.encode(&c);
        assert_eq!(row, c.encode());
        assert_eq!(codec.decode_checked(&c.id(), &row).unwrap(), c);
    }

    #[test]
    fn compressed_rows_round_trip_and_shrink() {
        let codec = Codec::default();
        codec.add(vec![(7, trained(7))]).unwrap();
        assert_eq!(codec.newest_id(), 7);
        let (mut plain, mut packed) = (0, 0);
        for c in history(300) {
            let row = codec.encode(&c);
            assert!(Codec::uses(&row, 7));
            assert_eq!(frame_dict_id(&row), Some(7));
            plain += c.encode().len();
            packed += row.len();
            assert_eq!(codec.decode_checked(&c.id(), &row).unwrap(), c);
        }
        assert!(packed * 2 < plain, "{packed} of {plain}");
    }

    #[test]
    fn rows_need_their_dictionary() {
        let writer = Codec::default();
        writer.add(vec![(3, trained(3))]).unwrap();
        let c = cs(5, vec![]);
        let row = writer.encode(&c);
        let reader = Codec::default();
        assert_eq!(reader.missing([row.as_slice()]), Some(3));
        let err = reader.decode_checked(&c.id(), &row).unwrap_err();
        assert!(err.to_string().contains("needs dictionary 3"), "{err}");
        reader.add(vec![(3, trained(3))]).unwrap();
        assert_eq!(reader.missing([row.as_slice()]), None);
        assert_eq!(reader.decode_checked(&c.id(), &row).unwrap(), c);
    }

    #[test]
    fn newer_dictionaries_take_over_and_old_rows_still_read() {
        let codec = Codec::default();
        codec.add(vec![(1, trained(1))]).unwrap();
        let c = cs(9, vec![]);
        let old = codec.encode(&c);
        codec.add(vec![(2, trained(2)), (1, trained(1))]).unwrap();
        assert_eq!(codec.newest_id(), 2);
        let new = codec.encode(&c);
        assert!(Codec::uses(&old, 1) && Codec::uses(&new, 2));
        assert_eq!(codec.decode_checked(&c.id(), &old).unwrap(), c);
        assert_eq!(codec.decode_checked(&c.id(), &new).unwrap(), c);
    }

    #[test]
    fn damage_is_reported_not_returned() {
        let codec = Codec::default();
        codec.add(vec![(1, trained(1))]).unwrap();
        let c = cs(4, vec![]);
        let other = cs(5, vec![]);
        let mut row = codec.encode(&c);
        assert!(codec.decode_checked(&other.id(), &row).is_err());
        let last = row.len() - 1;
        row[last] ^= 0x40;
        assert!(codec.decode_checked(&c.id(), &row).is_err());
        // A frame that claims an absurd size is refused before allocating.
        let mut huge = zstd::bulk::compress(&c.encode(), 3).unwrap();
        huge.truncate(6);
        assert!(codec.decode_checked(&c.id(), &huge).is_err());
    }

    /// A one-block frame whose header claims `size` bytes of content.
    fn frame_claiming(size: u64) -> Vec<u8> {
        let mut f = ZSTD_MAGIC.to_vec();
        // 8-byte content size, single segment, no checksum, no dictionary.
        f.push(0xE0);
        f.extend_from_slice(&size.to_le_bytes());
        // The last block: raw, 4 bytes.
        f.extend_from_slice(&[(4 << 3) | 1, 0, 0]);
        f.extend_from_slice(b"leep");
        f
    }

    /// A damaged header can claim up to the format's bound, or far past
    /// it; neither may reserve a buffer of that size, and both are refused.
    #[test]
    fn false_content_sizes_reserve_nothing() {
        let honest = frame_claiming(4);
        assert_eq!(decompress(&honest, None).as_deref(), Some(&b"leep"[..]));
        for size in [
            ONE_SHOT_MAX as u64 + 1,
            MAX_DECODED as u64,
            MAX_DECODED as u64 + 1,
            1 << 40,
            u64::MAX >> 1,
        ] {
            let f = frame_claiming(size);
            assert_eq!(
                zstd::zstd_safe::get_frame_content_size(&f).unwrap(),
                Some(size)
            );
            assert!(decompress(&f, None).is_none(), "{size}");
            let id = cs(1, vec![]).id();
            assert!(Codec::default().decode_checked(&id, &f).is_err());
        }
        // Frames past the one-shot bound still decode, through the stream,
        // including ones larger than the window, with or without a
        // dictionary.
        let codec = Codec::default();
        codec.add(vec![(1, trained(1))]).unwrap();
        let dict = codec.dicts.read().unwrap().newest.clone().unwrap();
        for len in [ONE_SHOT_MAX + 1, 9 << 20] {
            let mut x = 1u64;
            let big: Vec<u8> = (0..len)
                .map(|i| {
                    x = x.wrapping_mul(6364136223846793005).wrapping_add(1);
                    if i % 3 == 0 {
                        (x >> 59) as u8
                    } else {
                        b'a'
                    }
                })
                .collect();
            let packed = compress(&big, None);
            assert!(packed.len() < big.len());
            assert_eq!(decompress(&packed, None).as_ref(), Some(&big));
            let packed = compress(&big, Some(&dict));
            assert_eq!(decompress(&packed, Some(&dict)).as_ref(), Some(&big));
        }
    }

    /// Plain bytes that begin like a frame: 40 parents, the first of which
    /// starts b5 2f fd. Old servers stored such a row plain; this code reads
    /// it and stores it compressed.
    #[test]
    fn plain_rows_that_look_like_frames() {
        let mut first = [0u8; 32];
        first[..3].copy_from_slice(&[0xB5, 0x2F, 0xFD]);
        let mut parents = vec![ChangesetId(Hash(first))];
        parents.extend((1..40u8).map(|i| ChangesetId(Hash([i; 32]))));
        let c = cs(1, parents);
        let plain = c.encode();
        assert!(plain.starts_with(&ZSTD_MAGIC));
        let codec = Codec::default();
        assert_eq!(codec.decode_checked(&c.id(), &plain).unwrap(), c);
        let row = codec.encode(&c);
        assert_ne!(row, plain);
        assert_eq!(frame_dict_id(&row), Some(0));
        assert_eq!(codec.decode_checked(&c.id(), &row).unwrap(), c);
        codec.add(vec![(1, trained(1))]).unwrap();
        let row = codec.encode(&c);
        assert_eq!(codec.decode_checked(&c.id(), &row).unwrap(), c);
    }

    #[test]
    fn dictionaries_must_carry_their_id() {
        let codec = Codec::default();
        assert!(codec.add(vec![(2, trained(1))]).is_err());
        assert!(codec.add(vec![(0, trained(1))]).is_err());
        assert!(codec.add(vec![(1, b"not a dictionary".to_vec())]).is_err());
        assert!(train(&[], 1024, 0).is_err());
    }
}
