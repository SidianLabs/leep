//! The format-writing core of a repack, shared by the filesystem and object
//! store backends: choose the write order, then stream every object into a
//! `.pack` as a whole or delta frame and collect its index records. Where the
//! bytes come from and where the pack goes are the backend's business.

use std::collections::{HashMap, HashSet};
use std::io::Write;

use bytes::Bytes;
use leep_core::{Hash, Result};

use crate::cache::Lru;
use crate::pack::{self, IdxEntry, PACK_MAGIC};
use crate::repack::step;
use crate::{storage_err, RepackOptions, RepackReport, RepackStep};

/// Recently written objects kept as delta bases while writing a pack.
const BASE_CACHE: usize = 256 << 20;

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

/// Where a repack reads objects from.
pub(crate) trait PackSource {
    type Stamp;
    fn read(&mut self, hash: &Hash) -> Result<SourceRead<Self::Stamp>>;
}

/// A pack's entries as written, before its index is encoded.
pub(crate) struct WrittenPack<S> {
    /// In pack order; a base is a position in this list.
    pub entries: Vec<IdxEntry>,
    pub len: u64,
    pub hash: [u8; 32],
    /// Objects packed from loose copies, with each copy's stamp.
    pub from_loose: Vec<(Hash, S)>,
}

/// The objects of `live` in write order: as `hints` lists them (each with its
/// suggested base), then the rest whole, in hash order.
pub(crate) fn write_order(
    hints: Vec<(Hash, Option<Hash>)>,
    live: &HashSet<Hash>,
) -> Vec<(Hash, Option<Hash>)> {
    let mut order = Vec::with_capacity(live.len());
    let mut placed: HashSet<Hash> = HashSet::with_capacity(live.len());
    for (h, base) in hints {
        if live.contains(&h) && placed.insert(h) {
            order.push((h, base));
        }
    }
    let mut rest: Vec<Hash> = live
        .iter()
        .filter(|h| !placed.contains(*h))
        .copied()
        .collect();
    rest.sort();
    order.extend(rest.into_iter().map(|h| (h, None)));
    order
}

/// Write `order` to `out` as a `.pack`, counting what was written in
/// `report`. A base is used only if it is already in this pack, within the
/// chain cap, and the delta is smaller than the whole frame. The caller
/// flushes and makes `out` durable.
pub(crate) fn write_pack<W: Write, S: PackSource>(
    order: Vec<(Hash, Option<Hash>)>,
    src: &mut S,
    opts: &RepackOptions,
    out: &mut W,
    report: &mut RepackReport,
) -> Result<WrittenPack<S::Stamp>> {
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
    let mut entries: Vec<IdxEntry> = Vec::with_capacity(order.len());
    // Position in `entries` and chain depth of each object written.
    let mut written: HashMap<Hash, (u32, usize)> = HashMap::with_capacity(order.len());
    let mut bases = Lru::default();
    let mut from_loose = Vec::new();
    for (hash, base) in order {
        let (data, modified, stamp) = match src.read(&hash)? {
            SourceRead::Ok {
                data,
                modified,
                from_loose,
            } => (data, modified, from_loose),
            SourceRead::Vanished => {
                report.vanished += 1;
                continue;
            }
            SourceRead::Corrupt => {
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
                    None => match src.read(&b)? {
                        SourceRead::Ok { data, .. } => Some(data),
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
        let at = emit(&frame, out)?;
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
        if let Some(s) = stamp {
            from_loose.push((hash, s));
        }
        step(opts, RepackStep::Wrote(entries.len() as u64))?;
    }
    report.objects = entries.len() as u64;
    Ok(WrittenPack {
        entries,
        len: offset,
        hash: *hasher.finalize().as_bytes(),
        from_loose,
    })
}
