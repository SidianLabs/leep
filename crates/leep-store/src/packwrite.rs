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
    /// The frame `hash` already has in an old pack and that frame's base,
    /// when [`read`](Self::read) reads it from there. A delta frame decodes
    /// the same against the same base in any pack, so a writer that gives it
    /// the same base copies it instead of compressing again.
    fn stored_frame(&mut self, _hash: &Hash) -> Result<Option<(Vec<u8>, Option<Hash>)>> {
        Ok(None)
    }
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
        let base = base.filter(|b| *b != hash);
        // The bytes were decoded and checked above, so a copied frame is
        // known good.
        let reuse = match src.stored_frame(&hash)? {
            Some((frame, old)) if old == base => match old {
                None => Some((frame, None, 0)),
                Some(b) => written
                    .get(&b)
                    .filter(|(_, bd)| *bd < opts.max_chain)
                    .map(|&(bp, bd)| (frame, Some(bp), bd + 1)),
            },
            _ => None,
        };
        let (frame, used, d) = match reuse {
            Some(r) => {
                report.frames_reused += 1;
                r
            }
            None => compress(src, &hash, &data, base, &written, &mut bases, opts)?,
        };
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

/// A new frame for `data`: a delta against `base` if that is already in this
/// pack within the chain cap and smaller, else whole. Returns the frame, the
/// base's position and the chain depth.
fn compress<S: PackSource>(
    src: &mut S,
    hash: &Hash,
    data: &[u8],
    base: Option<Hash>,
    written: &HashMap<Hash, (u32, usize)>,
    bases: &mut Lru,
    opts: &RepackOptions,
) -> Result<(Vec<u8>, Option<u32>, usize)> {
    let whole = pack::compress_whole(data, opts.level)?;
    let Some(b) = base.filter(|b| b != hash) else {
        return Ok((whole, None, 0));
    };
    let Some(&(bp, bd)) = written.get(&b).filter(|(_, bd)| *bd < opts.max_chain) else {
        return Ok((whole, None, 0));
    };
    let base_bytes = match bases.get(&b) {
        Some(x) => x,
        None => match src.read(&b)? {
            SourceRead::Ok { data, .. } => data,
            _ => return Ok((whole, None, 0)),
        },
    };
    match pack::compress_delta(&base_bytes, data, opts.level)? {
        Some(delta) if delta.len() < whole.len() => Ok((delta, Some(bp), bd + 1)),
        _ => Ok((whole, None, 0)),
    }
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
