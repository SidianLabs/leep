//! Hashes numbered by first sight, for passes over millions of objects (the
//! repack walk and writer): each 32-byte hash is held once and everything
//! else refers to it by a 4-byte id. A `HashMap<Hash, _>` per structure
//! costs 40 to 80 bytes per object per map; at 10 million objects that is
//! gigabytes.

use std::hash::BuildHasher;

use crate::Hash;

/// The id that names no object.
pub const NO_ID: u32 = u32::MAX;

/// An append-only set of hashes, each numbered by when it was first added:
/// the hashes in a `Vec`, and an open-addressing table of ids over it.
/// About 37 to 42 bytes per hash.
#[derive(Clone, Default)]
pub struct HashIds {
    hashes: Vec<Hash>,
    /// `id + 1` per slot, 0 for empty; the length is a power of two.
    slots: Vec<u32>,
    /// Seeded per table, so crafted hashes cannot pile into one run.
    seed: std::collections::hash_map::RandomState,
}

impl HashIds {
    pub fn new() -> HashIds {
        HashIds::default()
    }

    pub fn len(&self) -> usize {
        self.hashes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.hashes.is_empty()
    }

    /// The hash numbered `id`. Panics for an id this table did not give.
    pub fn hash(&self, id: u32) -> &Hash {
        &self.hashes[id as usize]
    }

    /// Every hash, by id.
    pub fn hashes(&self) -> &[Hash] {
        &self.hashes
    }

    /// Only the hashes, by id, without the lookup table.
    pub fn into_hashes(self) -> Vec<Hash> {
        self.hashes
    }

    pub fn get(&self, hash: &Hash) -> Option<u32> {
        if self.slots.is_empty() {
            return None;
        }
        let mask = self.slots.len() - 1;
        let mut i = self.slot_of(hash) & mask;
        loop {
            match self.slots[i] {
                0 => return None,
                s if self.hashes[s as usize - 1] == *hash => return Some(s - 1),
                _ => i = (i + 1) & mask,
            }
        }
    }

    /// The id of `hash`, adding it if new; `true` when it was added.
    pub fn insert(&mut self, hash: &Hash) -> (u32, bool) {
        if let Some(id) = self.get(hash) {
            return (id, false);
        }
        let id = self.hashes.len();
        assert!(id < NO_ID as usize - 1, "too many hashes for u32 ids");
        // At most 3/4 full, so probe runs stay short.
        if (id + 1) * 4 > self.slots.len() * 3 {
            self.grow();
        }
        self.hashes.push(*hash);
        self.place(id as u32);
        (id as u32, true)
    }

    fn slot_of(&self, hash: &Hash) -> usize {
        self.seed.hash_one(hash.0) as usize
    }

    fn place(&mut self, id: u32) {
        let mask = self.slots.len() - 1;
        let mut i = self.slot_of(&self.hashes[id as usize]) & mask;
        while self.slots[i] != 0 {
            i = (i + 1) & mask;
        }
        self.slots[i] = id + 1;
    }

    fn grow(&mut self) {
        let n = (self.slots.len() * 2).max(64);
        self.slots = vec![0; n];
        for id in 0..self.hashes.len() as u32 {
            self.place(id);
        }
    }
}

/// The order to write a repack's objects in, each with the object to
/// compress it against (a hint the writer may ignore): what the history
/// walk (`leep_derive::pack_order`) gives the pack writer. Hashes are held
/// once, in `hashes`; `objects` names them by index, 8 bytes per object
/// instead of 65.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PackHints {
    /// Every object `objects` names, as object or base. May hold more.
    pub hashes: Vec<Hash>,
    /// Indexes into `hashes`: the object, and its base or [`NO_ID`].
    pub objects: Vec<(u32, u32)>,
}

impl PackHints {
    pub fn len(&self) -> usize {
        self.objects.len()
    }

    pub fn is_empty(&self) -> bool {
        self.objects.is_empty()
    }

    /// The hints as hashes, in write order.
    pub fn iter(&self) -> impl Iterator<Item = (Hash, Option<Hash>)> + '_ {
        self.objects.iter().map(|&(o, b)| {
            (
                self.hashes[o as usize],
                (b != NO_ID).then(|| self.hashes[b as usize]),
            )
        })
    }
}

impl IntoIterator for PackHints {
    type Item = (Hash, Option<Hash>);
    type IntoIter = std::vec::IntoIter<(Hash, Option<Hash>)>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter().collect::<Vec<_>>().into_iter()
    }
}

impl From<Vec<(Hash, Option<Hash>)>> for PackHints {
    fn from(pairs: Vec<(Hash, Option<Hash>)>) -> PackHints {
        let mut ids = HashIds::new();
        let objects = pairs
            .iter()
            .map(|(h, b)| (ids.insert(h).0, b.map_or(NO_ID, |b| ids.insert(&b).0)))
            .collect();
        PackHints {
            hashes: ids.into_hashes(),
            objects,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_are_dense_and_stable() {
        let mut ids = HashIds::new();
        let hs: Vec<Hash> = (0..10_000u32)
            .map(|i| Hash::of_blob(&i.to_be_bytes()))
            .collect();
        for (i, h) in hs.iter().enumerate() {
            assert_eq!(ids.insert(h), (i as u32, true));
        }
        for (i, h) in hs.iter().enumerate() {
            assert_eq!(ids.insert(h), (i as u32, false));
            assert_eq!(ids.get(h), Some(i as u32));
            assert_eq!(ids.hash(i as u32), h);
        }
        assert_eq!(ids.get(&Hash::of_blob(b"absent")), None);
        assert_eq!(ids.len(), hs.len());
    }

    #[test]
    fn hints_round_trip() {
        let h = |s: &str| Hash::of_blob(s.as_bytes());
        let pairs = vec![
            (h("a"), None),
            (h("b"), Some(h("a"))),
            (h("c"), Some(h("x"))),
            (h("a"), Some(h("c"))),
        ];
        let hints = PackHints::from(pairs.clone());
        assert_eq!(hints.iter().collect::<Vec<_>>(), pairs);
        assert_eq!(hints.hashes.len(), 4);
    }
}
