//! Packs written by [`BlobStore::put_many`](leep_core::BlobStore::put_many)
//! outside a repack (ADR 0068): a land batch's new objects become one pack
//! instead of a loose file each. The pack is built in memory, every object a
//! whole zstd frame in hash order, in the format a repack writes (`LEEPPAK1`
//! and a `LEEPIDX2` index), so readers, repacks, GC and backups treat it like
//! any other pack. A later repack merges it with deltas.

use std::sync::Arc;

use bytes::Bytes;
use leep_core::{Error, Hash, Result};

use crate::pack::{self, IdxEntry, PackIndex, PACK_MAGIC};

/// Fewest new objects a `put_many` packs: below it, a pack and its index
/// (four fsyncs and two files) cost about what loose files do, and a lone
/// create's three or four tree nodes would each make a pack.
pub const DEFAULT_PACK_MIN_OBJECTS: usize = 16;

/// zstd level of the frames: fast, since the land leader waits for them; a
/// repack compresses them again with deltas.
pub const DEFAULT_PACK_LEVEL: i32 = 1;

/// Where a pack write is. The hook sees each step after it is durable, so
/// returning an error there simulates a kill at that point.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PackWriteStep {
    /// The temporary pack file is complete and fsynced.
    PackSynced,
    /// The pack has its final name; no index names it yet.
    PackRenamed,
    /// The temporary index file is complete and fsynced.
    IndexSynced,
    /// The index has its final name: the pack is published.
    Published,
}

pub type PackWriteHook = Arc<dyn Fn(PackWriteStep) -> Result<()> + Send + Sync>;

/// How a store writes packs from `put_many`.
#[derive(Clone)]
pub struct PackWrites {
    /// Fewest objects to pack; batches with fewer new objects are written
    /// loose. 0 turns pack writes off.
    pub min_objects: usize,
    /// zstd level, 1 to 22.
    pub level: i32,
    /// Called at each step; an error stops the write where it stands.
    pub hook: Option<PackWriteHook>,
}

impl Default for PackWrites {
    fn default() -> Self {
        PackWrites {
            min_objects: DEFAULT_PACK_MIN_OBJECTS,
            level: DEFAULT_PACK_LEVEL,
            hook: None,
        }
    }
}

impl PackWrites {
    /// Pack writes off: every `put_many` writes loose objects.
    pub fn off() -> Self {
        PackWrites {
            min_objects: 0,
            ..Self::default()
        }
    }

    pub(crate) fn packs(&self, objects: usize) -> bool {
        self.min_objects > 0 && objects >= self.min_objects
    }

    pub(crate) fn step(&self, s: PackWriteStep) -> Result<()> {
        match &self.hook {
            Some(h) => h(s),
            None => Ok(()),
        }
    }
}

impl std::fmt::Debug for PackWrites {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PackWrites")
            .field("min_objects", &self.min_objects)
            .field("level", &self.level)
            .finish_non_exhaustive()
    }
}

/// A pack built in memory, ready to write.
pub(crate) struct EncodedPack {
    pub id: String,
    pub pack: Vec<u8>,
    pub idx: Vec<u8>,
    pub index: PackIndex,
    entries: Vec<IdxEntry>,
}

impl EncodedPack {
    /// Index the same frames again with `modified` one second later, for a
    /// new pack id: a pack with this id is published already, and may have
    /// tombstones that would hide the objects written again (the same
    /// objects packed within one second make the same pack).
    pub fn next_id(&mut self) -> Result<()> {
        for e in &mut self.entries {
            e.modified += 1;
        }
        self.reindex()
    }

    fn reindex(&mut self) -> Result<()> {
        let pack_hash = *self.index.pack_hash();
        let entries = &self.entries;
        self.idx =
            PackIndex::encode_sorted(self.pack.len() as u64, pack_hash, entries.len(), |r| {
                entries[r]
            })?;
        self.id = pack::pack_id(&self.idx);
        self.index = PackIndex::decode(self.idx.clone())
            .map_err(|e| Error::Storage(format!("pack {} as written: {e}", self.id)))?;
        Ok(())
    }
}

/// Build a pack of `items` (sorted by hash, distinct), each a whole frame
/// recording `modified` as its write time. Every item must be a file blob
/// or a tree node under its address, as readers check.
pub(crate) fn encode(items: &[(Hash, Bytes)], level: i32, modified: i64) -> Result<EncodedPack> {
    debug_assert!(items.windows(2).all(|w| w[0].0 < w[1].0));
    let mut zstd = pack::Compressor::default();
    let logical: usize = items.iter().map(|(_, d)| d.len()).sum();
    let mut out = Vec::with_capacity(PACK_MAGIC.len() + logical / 2 + 64 * items.len());
    out.extend_from_slice(&PACK_MAGIC);
    let mut entries = Vec::with_capacity(items.len());
    for (hash, data) in items {
        let frame = zstd.whole(data, level)?;
        entries.push(IdxEntry {
            hash: *hash,
            offset: out.len() as u64,
            stored_len: frame.len() as u64,
            len: data.len() as u64,
            base: None,
            modified,
        });
        out.extend_from_slice(&frame);
    }
    let pack_hash = *blake3::hash(&out).as_bytes();
    let idx = PackIndex::encode_sorted(out.len() as u64, pack_hash, entries.len(), |r| entries[r])?;
    let id = pack::pack_id(&idx);
    let index = PackIndex::decode(idx.clone())
        .map_err(|e| Error::Storage(format!("pack {id} as written: {e}")))?;
    Ok(EncodedPack {
        id,
        pack: out,
        idx,
        index,
        entries,
    })
}

/// Sort `items` by hash and drop repeats, keeping the first.
pub(crate) fn sort_distinct(items: &mut Vec<(Hash, Bytes)>) {
    items.sort_by_key(|(h, _)| *h);
    items.dedup_by_key(|(h, _)| *h);
}

/// Whether `data` may go in a pack under `hash`: readers check every
/// decoded object as a file blob or a tree node, so anything else is
/// written loose.
pub(crate) fn packable(hash: &Hash, data: &[u8], tree_first: &mut bool) -> bool {
    match pack::object_kind(hash, data, *tree_first) {
        Some(tree) => {
            *tree_first = tree;
            true
        }
        None => false,
    }
}

pub(crate) fn record(objects: usize, bytes: u64, took: std::time::Duration) {
    metrics::counter!("leep_pack_writes_total").increment(1);
    metrics::histogram!("leep_pack_write_objects").record(objects as f64);
    metrics::histogram!("leep_pack_write_bytes").record(bytes as f64);
    metrics::histogram!("leep_pack_write_seconds").record(took.as_secs_f64());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encoded_packs_decode_to_their_objects() {
        let mut items: Vec<(Hash, Bytes)> = (0..50)
            .map(|i| {
                let d = Bytes::from(format!("object {i}").repeat(i + 1));
                (Hash::of_blob(&d), d)
            })
            .collect();
        items.push(items[3].clone());
        sort_distinct(&mut items);
        assert_eq!(items.len(), 50);
        let p = encode(&items, 1, 1234).unwrap();
        assert_eq!(p.id, pack::pack_id(&p.idx));
        assert_eq!(p.index.len(), 50);
        assert_eq!(p.index.pack_len(), p.pack.len() as u64);
        for (h, d) in &items {
            let e = p.index.find(h).unwrap();
            assert_eq!(e.modified, 1234);
            assert_eq!(e.base, None);
            let frame = &p.pack[e.offset as usize..(e.offset + e.stored_len) as usize];
            assert_eq!(pack::decompress(frame, None, e.len).unwrap(), &d[..]);
        }
        // The same objects at the same time make the same pack; a second
        // later, another.
        let mut again = encode(&items, 1, 1234).unwrap();
        assert_eq!(again.id, p.id);
        again.next_id().unwrap();
        assert_ne!(again.id, p.id);
        assert_eq!(again.pack, p.pack, "the same frames");
        assert_eq!(again.index.find(&items[0].0).unwrap().modified, 1235);
    }

    #[test]
    fn only_blobs_and_tree_nodes_are_packable() {
        let mut tree_first = false;
        let d = b"some bytes";
        assert!(packable(&Hash::of_blob(d), d, &mut tree_first));
        assert!(packable(
            &Hash::of_object(leep_core::TreeNode::KIND, d),
            d,
            &mut tree_first
        ));
        assert!(tree_first);
        assert!(!packable(
            &Hash::of_object("other-v1", d),
            d,
            &mut tree_first
        ));
    }
}
