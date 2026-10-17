//! Packed filesystem stores: reads, listings, deletes and repacks, including
//! repacks killed at every step.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use leep_core::memory::check_blob_store_contract;
use leep_core::{BlobStore, Error, Hash, TreeNode};

use crate::pack::{self, IdxEntry, PackIndex};
use crate::{plan_merge, FsBlobStore, RepackMode, RepackOptions, RepackReport, RepackStep};

/// Versions of a growing file: each a small edit of the one before.
fn versions(n: usize) -> Vec<Bytes> {
    let mut text = String::new();
    (0..n)
        .map(|i| {
            text.push_str(&format!(
                "line {i}: the quick brown fox jumps over the lazy dog\n"
            ));
            Bytes::from(text.clone())
        })
        .collect()
}

/// Each version based on the one before it, as the history walk would say.
fn chain_hints(hashes: &[Hash]) -> Vec<(Hash, Option<Hash>)> {
    hashes
        .iter()
        .enumerate()
        .map(|(i, h)| (*h, i.checked_sub(1).map(|p| hashes[p])))
        .collect()
}

async fn repack(store: &FsBlobStore, hints: Vec<(Hash, Option<Hash>)>) -> RepackReport {
    store
        .repack_publish(hints, RepackOptions::default())
        .await
        .unwrap()
        .finish()
        .await
        .unwrap()
}

const INCREMENTAL: RepackMode = RepackMode::Incremental {
    factor: 2,
    max_packs: 8,
};

async fn repack_incremental(store: &FsBlobStore, hints: Vec<(Hash, Option<Hash>)>) -> RepackReport {
    let opts = RepackOptions {
        mode: INCREMENTAL,
        ..RepackOptions::default()
    };
    store
        .repack_publish(hints, opts)
        .await
        .unwrap()
        .finish()
        .await
        .unwrap()
}

fn files_named(root: &Path, pred: impl Fn(&str) -> bool) -> Vec<String> {
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(d) = stack.pop() {
        for e in std::fs::read_dir(&d).into_iter().flatten().flatten() {
            let p = e.path();
            if p.is_dir() {
                stack.push(p);
            } else if pred(&e.file_name().to_string_lossy()) {
                out.push(p.display().to_string());
            }
        }
    }
    out.sort();
    out
}

fn loose_count(root: &Path) -> usize {
    files_named(root, |n| {
        n.len() == 62 && n.bytes().all(|c| c.is_ascii_hexdigit())
    })
    .len()
}

fn pack_ids(root: &Path) -> Vec<String> {
    crate::packset::published_ids(&root.join("packs")).unwrap()
}

fn index_of(root: &Path) -> PackIndex {
    let ids = pack_ids(root);
    assert_eq!(ids.len(), 1, "one pack");
    let bytes = std::fs::read(root.join("packs").join(format!("{}.idx", ids[0]))).unwrap();
    PackIndex::decode(bytes).unwrap()
}

async fn list_all(store: &dyn BlobStore, page: usize) -> Vec<leep_core::BlobMeta> {
    let mut out = Vec::new();
    let mut after = None;
    loop {
        let batch = store.list_page(after, page).await.unwrap();
        let Some(last) = batch.last() else {
            return out;
        };
        after = Some(last.hash);
        out.extend(batch);
    }
}

#[tokio::test]
async fn packed_store_passes_the_contract() {
    let dir = tempfile::tempdir().unwrap();
    let store = FsBlobStore::new(dir.path());
    check_blob_store_contract(&store).await;
    let report = repack(&store, Vec::new()).await;
    assert!(report.objects > 10, "{report:?}");
    assert_eq!(loose_count(dir.path()), 0, "every loose copy went");
    // Again on the packed store (its objects are now packed, deletes become
    // tombstones), and from a second handle as another process would see it.
    check_blob_store_contract(&store).await;
    check_blob_store_contract(&FsBlobStore::new(dir.path())).await;
    repack(&store, Vec::new()).await;
    check_blob_store_contract(&FsBlobStore::with_decoded_cache(dir.path(), 0)).await;
}

#[tokio::test]
async fn quiet_fault_wrapper_passes_the_contract_on_a_packed_store() {
    use leep_testkit::{FaultPlan, FaultyBlobStore};
    let dir = tempfile::tempdir().unwrap();
    let store = FsBlobStore::new(dir.path());
    check_blob_store_contract(&store).await;
    repack(&store, Vec::new()).await;
    let faulty = FaultyBlobStore::new(Arc::new(store), FaultPlan::new(1));
    check_blob_store_contract(&faulty).await;
}

#[tokio::test]
async fn reads_after_repack_equal_the_originals_and_deltas_are_used() {
    let dir = tempfile::tempdir().unwrap();
    let store = FsBlobStore::new(dir.path());
    let mut originals: BTreeMap<Hash, Bytes> = BTreeMap::new();
    let vs = versions(80);
    let mut chain = Vec::new();
    for v in &vs {
        let h = store.put(v.clone()).await.unwrap();
        originals.insert(h, v.clone());
        chain.push(h);
    }
    // A tree node, an empty blob, a large incompressible-ish blob.
    let node = TreeNode::default();
    let node_bytes = Bytes::from(node.encode());
    store
        .put_with_hash(node.hash(), node_bytes.clone())
        .await
        .unwrap();
    originals.insert(node.hash(), node_bytes);
    let empty = store.put(Bytes::new()).await.unwrap();
    originals.insert(empty, Bytes::new());
    let big: Vec<u8> = (0..3_000_000u32)
        .map(|i| (i.wrapping_mul(2_654_435_761) >> 13) as u8)
        .collect();
    let big = Bytes::from(big);
    originals.insert(store.put(big.clone()).await.unwrap(), big);

    let loose_bytes: u64 = originals.values().map(|b| b.len() as u64).sum();
    let report = repack(&store, chain_hints(&chain)).await;
    assert_eq!(report.objects, originals.len() as u64);
    assert_eq!(report.loose_before, originals.len() as u64);
    assert_eq!(report.loose_removed, originals.len() as u64);
    assert_eq!((report.loose_after, report.loose_kept), (0, 0));
    assert_eq!(report.logical_bytes, loose_bytes);
    assert!(
        report.deltas >= 70,
        "similar versions are deltas: {report:?}"
    );
    let versions_bytes: u64 = vs.iter().map(|v| v.len() as u64).sum();
    assert!(
        report.pack_bytes < 3_100_000 + versions_bytes / 20,
        "the versions shrink: {report:?}"
    );
    assert_eq!(loose_count(dir.path()), 0);

    // Fresh handle (nothing cached), byte for byte.
    for s in [
        FsBlobStore::new(dir.path()),
        FsBlobStore::with_decoded_cache(dir.path(), 0),
    ] {
        for (h, b) in &originals {
            assert_eq!(s.get(h).await.unwrap().as_ref(), Some(b), "{h}");
            assert!(s.contains(h).await.unwrap());
        }
    }
    // Chains are capped, and every base precedes its delta.
    let idx = index_of(dir.path());
    assert_eq!(
        idx.magic(),
        pack::IDX_MAGIC,
        "new packs use the compact index"
    );
    for e in idx.iter() {
        let mut depth = 0;
        let mut cur = e;
        while let Some(b) = cur.base {
            let base = idx.entry(b);
            assert!(base.offset < cur.offset);
            cur = base;
            depth += 1;
        }
        assert!(depth <= pack::MAX_CHAIN, "depth {depth}");
    }
    assert!(idx.iter().any(|e| e.base.is_some()));
}

#[tokio::test]
async fn listing_merges_loose_and_packed_with_logical_sizes() {
    let dir = tempfile::tempdir().unwrap();
    let store = FsBlobStore::new(dir.path());
    let mut sizes = BTreeMap::new();
    for i in 0..30u32 {
        let b = Bytes::from(format!("packed {i} ").repeat(50 + i as usize));
        sizes.insert(store.put(b.clone()).await.unwrap(), b.len() as u64);
    }
    repack(&store, Vec::new()).await;
    for i in 0..30u32 {
        let b = Bytes::from(format!("loose {i}"));
        sizes.insert(store.put(b.clone()).await.unwrap(), b.len() as u64);
    }
    // Packed and loose at once: listed once.
    store
        .put(Bytes::from("packed 3 ".repeat(53)))
        .await
        .unwrap();
    let all = list_all(&store, 1000).await;
    assert!(
        all.windows(2).all(|w| w[0].hash < w[1].hash),
        "ascending, no repeats"
    );
    let listed: BTreeMap<Hash, u64> = all.iter().map(|m| (m.hash, m.size)).collect();
    assert_eq!(listed, sizes, "complete, with logical sizes");
    for page in [1, 3, 7] {
        assert_eq!(list_all(&store, page).await, all, "page {page}");
    }
    let mid = all[29].hash;
    assert_eq!(store.list_page(Some(mid), 5).await.unwrap(), all[30..35]);
}

#[tokio::test]
async fn packed_objects_keep_their_age_and_a_reput_refreshes_it() {
    let dir = tempfile::tempdir().unwrap();
    let store = FsBlobStore::new(dir.path());
    let h = store.put(Bytes::from("old orphan")).await.unwrap();
    let old = std::time::SystemTime::now() - std::time::Duration::from_secs(72 * 3600);
    std::fs::File::options()
        .write(true)
        .open(store.path_for(&h))
        .unwrap()
        .set_modified(old)
        .unwrap();
    let want = crate::fs::unix_secs(old);
    repack(&store, Vec::new()).await;
    assert_eq!(store.list_page(None, 10).await.unwrap()[0].modified, want);
    repack(&store, Vec::new()).await;
    assert_eq!(
        store.list_page(None, 10).await.unwrap()[0].modified,
        want,
        "a second repack does not make it young"
    );
    store.put(Bytes::from("old orphan")).await.unwrap();
    let now = crate::fs::unix_secs(std::time::SystemTime::now()).unwrap();
    assert!(
        store.list_page(None, 10).await.unwrap()[0]
            .modified
            .unwrap()
            >= now - 5
    );
}

#[tokio::test]
async fn tombstones_hide_packed_objects_and_a_full_repack_drops_them() {
    let dir = tempfile::tempdir().unwrap();
    let store = FsBlobStore::new(dir.path());
    let vs = versions(10);
    let mut hashes = Vec::new();
    for v in &vs {
        hashes.push(store.put(v.clone()).await.unwrap());
    }
    repack(&store, chain_hints(&hashes)).await;
    // Delete bases of later deltas: the deltas still read.
    let doomed = [hashes[0], hashes[4], hashes[5]];
    let other = FsBlobStore::new(dir.path());
    assert!(other.contains(&hashes[4]).await.unwrap());
    for h in &doomed {
        store.delete(h).await.unwrap();
        store.delete(h).await.unwrap();
    }
    for s in [&store, &other] {
        for (i, h) in hashes.iter().enumerate() {
            let gone = doomed.contains(h);
            assert_eq!(s.contains(h).await.unwrap(), !gone, "contains {i}");
            assert_eq!(s.get(h).await.unwrap().is_none(), gone, "get {i}");
            if !gone {
                assert_eq!(s.get(h).await.unwrap().unwrap(), vs[i]);
            }
        }
        let listed: Vec<Hash> = list_all(s, 3).await.iter().map(|m| m.hash).collect();
        assert_eq!(listed.len(), hashes.len() - doomed.len());
        assert!(doomed.iter().all(|h| !listed.contains(h)));
    }
    // An upload rescuing a deleted blob wins over the tombstone.
    store.put(vs[5].clone()).await.unwrap();
    assert_eq!(other.get(&hashes[5]).await.unwrap().unwrap(), vs[5]);
    assert!(other.contains(&hashes[5]).await.unwrap());

    let report = repack(&store, chain_hints(&hashes)).await;
    assert_eq!(report.dropped_tombstoned, 2, "{report:?}");
    assert_eq!(report.objects, 8);
    assert!(
        !dir.path().join("packs/tombstones").exists(),
        "no tombstones refer to the new pack"
    );
    for s in [&store, &other, &FsBlobStore::new(dir.path())] {
        for (i, h) in hashes.iter().enumerate() {
            let gone = doomed.contains(h) && *h != hashes[5];
            assert_eq!(s.get(h).await.unwrap().is_none(), gone, "after repack {i}");
        }
    }
    assert_eq!(index_of(dir.path()).len(), 8);
}

#[tokio::test]
async fn a_corrupt_pack_entry_is_a_storage_error_not_wrong_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let store = FsBlobStore::new(dir.path());
    let vs = versions(4);
    let mut hashes = Vec::new();
    for v in &vs {
        hashes.push(store.put(v.clone()).await.unwrap());
    }
    repack(&store, chain_hints(&hashes)).await;
    let idx = index_of(dir.path());
    let id = &pack_ids(dir.path())[0];
    let path = dir.path().join("packs").join(format!("{id}.pack"));
    let mut bytes = std::fs::read(&path).unwrap();
    // Damage the payload of the first version, the base of all others.
    let e = idx.find(&hashes[0]).unwrap();
    let at = (e.offset + e.stored_len - 2) as usize;
    bytes[at] ^= 0x40;
    std::fs::write(&path, &bytes).unwrap();
    let fresh = FsBlobStore::with_decoded_cache(dir.path(), 0);
    for h in &hashes {
        match fresh.get(h).await {
            Err(Error::Storage(msg)) => assert!(msg.contains("corrupt"), "{msg}"),
            other => panic!("expected a storage error, got {other:?}"),
        }
    }
    // A damaged index is refused too.
    let idx_path = dir.path().join("packs").join(format!("{id}.idx"));
    let mut ib = std::fs::read(&idx_path).unwrap();
    ib[60] ^= 1;
    std::fs::write(&idx_path, &ib).unwrap();
    assert!(matches!(
        FsBlobStore::new(dir.path()).get(&hashes[1]).await,
        Err(Error::Storage(_))
    ));
}

/// A pack written by hand from the layout in docs/FORMATS.md: a raw-block
/// zstd frame and a delta frame against it, with an index in each layout.
/// Readers must keep reading both.
#[tokio::test]
async fn hand_written_pack_reads() {
    let base = b"the quick brown fox jumps over the lazy dog".repeat(4);
    let mut data = base.clone();
    data.extend_from_slice(b" and again");
    let mut whole = vec![0x28, 0xb5, 0x2f, 0xfd, 0x20, 0xac, 0x61, 0x05, 0x00];
    whole.extend_from_slice(&base);
    let delta: &[u8] = &[
        0x28, 0xb5, 0x2f, 0xfd, 0x20, 0xb6, 0x8d, 0x00, 0x00, 0x50, 0x20, 0x61, 0x6e, 0x64, 0x20,
        0x61, 0x67, 0x61, 0x69, 0x6e, 0x01, 0x00, 0x29, 0x47, 0x29, 0x20,
    ];
    let mut packfile = b"LEEPPAK1".to_vec();
    packfile.extend_from_slice(&whole);
    packfile.extend_from_slice(delta);
    let (hb, hd) = (Hash::of_blob(&base), Hash::of_blob(&data));
    let entries = vec![
        IdxEntry {
            hash: hb,
            offset: 8,
            stored_len: whole.len() as u64,
            len: base.len() as u64,
            base: None,
            modified: 1_000,
        },
        IdxEntry {
            hash: hd,
            offset: 8 + whole.len() as u64,
            stored_len: delta.len() as u64,
            len: data.len() as u64,
            base: Some(0),
            modified: 2_000,
        },
    ];
    let pack_len = packfile.len() as u64;
    let pack_hash = *blake3::hash(&packfile).as_bytes();
    for idx in [
        pack::encode_idx1(pack_len, pack_hash, &entries),
        PackIndex::encode(pack_len, pack_hash, &entries).unwrap(),
    ] {
        let id = pack::pack_id(&idx);
        let dir = tempfile::tempdir().unwrap();
        let packs = dir.path().join("packs");
        std::fs::create_dir_all(&packs).unwrap();
        std::fs::write(packs.join(format!("{id}.pack")), &packfile).unwrap();
        std::fs::write(packs.join(format!("{id}.idx")), &idx).unwrap();
        let store = FsBlobStore::new(dir.path());
        assert_eq!(store.get(&hd).await.unwrap().unwrap(), data);
        assert_eq!(store.get(&hb).await.unwrap().unwrap(), base);
        let listed = store.list_page(None, 10).await.unwrap();
        assert_eq!(listed.len(), 2);
        assert!(listed.iter().all(|m| m.modified.is_some()));
        // The writer produces the same pack layout.
        let report = repack(&store, vec![(hb, None), (hd, Some(hb))]).await;
        assert_eq!((report.objects, report.deltas), (2, 1));
        let written =
            std::fs::read(packs.join(format!("{}.pack", report.pack_id.as_ref().unwrap())))
                .unwrap();
        assert_eq!(&written[..8], b"LEEPPAK1");
        let idx = index_of(dir.path());
        assert_eq!(idx.magic(), pack::IDX_MAGIC);
        assert_eq!(idx.find(&hd).unwrap().modified, 2_000);
    }
}

/// Write a pack whose index has the original `LEEPIDX1` layout, as stores
/// packed before `LEEPIDX2` hold. Each object is a delta against the one
/// before it. Returns the pack id.
fn write_idx1_pack(root: &Path, objects: &[(Bytes, i64)]) -> String {
    let mut packfile = b"LEEPPAK1".to_vec();
    let mut entries = Vec::new();
    for (i, (data, modified)) in objects.iter().enumerate() {
        let frame = match i.checked_sub(1) {
            Some(b) => pack::compress_delta(&objects[b].0, data, 9)
                .unwrap()
                .unwrap(),
            None => pack::compress_whole(data, 9).unwrap(),
        };
        entries.push(IdxEntry {
            hash: Hash::of_blob(data),
            offset: packfile.len() as u64,
            stored_len: frame.len() as u64,
            len: data.len() as u64,
            base: i.checked_sub(1).map(|b| b as u32),
            modified: *modified,
        });
        packfile.extend_from_slice(&frame);
    }
    let idx = pack::encode_idx1(
        packfile.len() as u64,
        *blake3::hash(&packfile).as_bytes(),
        &entries,
    );
    let id = pack::pack_id(&idx);
    let packs = root.join("packs");
    std::fs::create_dir_all(&packs).unwrap();
    std::fs::write(packs.join(format!("{id}.pack")), &packfile).unwrap();
    std::fs::write(packs.join(format!("{id}.idx")), &idx).unwrap();
    id
}

fn idx_magic(root: &Path, id: &str) -> Vec<u8> {
    std::fs::read(root.join("packs").join(format!("{id}.idx"))).unwrap()[..8].to_vec()
}

/// A deployed store holds `LEEPIDX1` packs; after this change it also gets
/// `LEEPIDX2` ones. Both read, list and take tombstones side by side, and a
/// full repack folds them into one new-format pack.
#[tokio::test]
async fn a_store_with_old_and_new_indexes_reads_lists_deletes_and_repacks() {
    let dir = tempfile::tempdir().unwrap();
    let vs = versions(30);
    // Versions 0..12 in an old pack, with recorded ages; 10 and 11 are also
    // in the new pack below.
    let old: Vec<(Bytes, i64)> = vs[..12]
        .iter()
        .enumerate()
        .map(|(i, v)| (v.clone(), 1_600_000_000 + i as i64))
        .collect();
    let old_id = write_idx1_pack(dir.path(), &old);
    let store = FsBlobStore::new(dir.path());
    let hashes: Vec<Hash> = vs.iter().map(|v| Hash::of_blob(v)).collect();
    for v in &vs[10..24] {
        store.put(v.clone()).await.unwrap();
    }
    // A repack packs everything it sees, so hide the old pack while writing
    // a new one that holds only 10..24.
    let packs = dir.path().join("packs");
    let aside = dir.path().join("old-pack-aside");
    std::fs::create_dir_all(&aside).unwrap();
    for ext in ["idx", "pack"] {
        let name = format!("{old_id}.{ext}");
        std::fs::rename(packs.join(&name), aside.join(&name)).unwrap();
    }
    let store = FsBlobStore::new(dir.path());
    let first = repack(&store, chain_hints(&hashes[10..24])).await;
    let new_id = first.pack_id.unwrap();
    for ext in ["idx", "pack"] {
        let name = format!("{old_id}.{ext}");
        std::fs::rename(aside.join(&name), packs.join(&name)).unwrap();
    }
    for v in &vs[24..] {
        store.put(v.clone()).await.unwrap();
    }
    assert_eq!(idx_magic(dir.path(), &old_id), b"LEEPIDX1");
    assert_eq!(idx_magic(dir.path(), &new_id), b"LEEPIDX2");
    assert_eq!(pack_ids(dir.path()).len(), 2);

    let store = FsBlobStore::new(dir.path());
    let check = |store: FsBlobStore, gone: Vec<Hash>| {
        let vs = vs.clone();
        let hashes = hashes.clone();
        async move {
            for (i, h) in hashes.iter().enumerate() {
                let got = store.get(h).await.unwrap();
                if gone.contains(h) {
                    assert!(got.is_none(), "{i} gone");
                    assert!(!store.contains(h).await.unwrap());
                } else {
                    assert_eq!(got.as_ref(), Some(&vs[i]), "{i}");
                    assert!(store.contains(h).await.unwrap());
                }
            }
            let want: BTreeMap<Hash, u64> = hashes
                .iter()
                .zip(&vs)
                .filter(|(h, _)| !gone.contains(h))
                .map(|(h, v)| (*h, v.len() as u64))
                .collect();
            for page in [1, 4, 1000] {
                let all = list_all(&store, page).await;
                let listed: BTreeMap<Hash, u64> = all.iter().map(|m| (m.hash, m.size)).collect();
                assert_eq!(listed, want, "page {page}");
                assert_eq!(all.len(), want.len(), "listed once each");
            }
            list_all(&store, 1000).await
        }
    };
    let listed = check(FsBlobStore::with_decoded_cache(dir.path(), 0), vec![]).await;
    // Ages of objects only in the old pack come from its records.
    let age = |h: &Hash| listed.iter().find(|m| m.hash == *h).unwrap().modified;
    assert_eq!(age(&hashes[3]), Some(1_600_000_003));

    // Delete the base of the old pack's chain, an object in both packs and
    // one only in the new pack.
    let doomed = vec![hashes[0], hashes[11], hashes[15]];
    for h in &doomed {
        store.delete(h).await.unwrap();
    }
    let tombstones = std::fs::read_to_string(packs.join("tombstones")).unwrap();
    let mut lines: Vec<&str> = tombstones.lines().collect();
    lines.sort();
    let mut want = vec![
        format!("{old_id} {}", hashes[0]),
        format!("{old_id} {}", hashes[11]),
        format!("{new_id} {}", hashes[11]),
        format!("{new_id} {}", hashes[15]),
    ];
    want.sort();
    assert_eq!(lines, want);
    check(FsBlobStore::new(dir.path()), doomed.clone()).await;
    check(store.clone(), doomed.clone()).await;

    // A full repack converts everything to one new-format pack.
    let report = repack(&store, chain_hints(&hashes)).await;
    assert_eq!(report.packs_before, 2);
    assert_eq!(report.objects, 27, "{report:?}");
    assert_eq!(report.dropped_tombstoned, 3, "{report:?}");
    let ids = pack_ids(dir.path());
    assert_eq!(ids.len(), 1);
    assert_eq!(idx_magic(dir.path(), &ids[0]), b"LEEPIDX2");
    assert!(!packs.join("tombstones").exists());
    assert_eq!(loose_count(dir.path()), 0);
    let listed = check(FsBlobStore::with_decoded_cache(dir.path(), 0), doomed).await;
    let age = |h: &Hash| listed.iter().find(|m| m.hash == *h).unwrap().modified;
    assert_eq!(age(&hashes[3]), Some(1_600_000_003), "old ages survive");
}

/// A new-format index that is damaged but still named by its own hash (so
/// only the decoder can catch it) is refused, whatever the damage.
#[tokio::test]
async fn corrupt_new_indexes_are_refused_by_the_store() {
    let dir = tempfile::tempdir().unwrap();
    let store = FsBlobStore::new(dir.path());
    let vs = versions(4);
    let mut hashes = Vec::new();
    for v in &vs {
        hashes.push(store.put(v.clone()).await.unwrap());
    }
    repack(&store, chain_hints(&hashes)).await;
    let packs = dir.path().join("packs");
    let id = pack_ids(dir.path()).remove(0);
    let good = std::fs::read(packs.join(format!("{id}.idx"))).unwrap();
    assert_eq!(&good[..8], b"LEEPIDX2");
    let widths: [u8; 5] = good[64..69].try_into().unwrap();
    let rec: usize = widths.iter().map(|&w| w as usize).sum();
    let records = pack::IDX_HEADER_LEN + pack::IDX_FANOUT_LEN + 4 * 32;
    let base_at = (widths[0] + widths[1]) as usize;
    type Damage = Box<dyn Fn(&mut Vec<u8>)>;
    let damages: Vec<(&str, Damage)> = vec![
        (
            "base ordinal past the end",
            Box::new(move |b| b[records + base_at + widths[2] as usize - 1] = 5),
        ),
        (
            "base is itself",
            Box::new(move |b| b[records + base_at + widths[2] as usize - 1] = 1),
        ),
        ("bad widths", Box::new(|b| b[66] = 9)),
        (
            "frame outside the pack",
            Box::new(move |b| {
                for x in &mut b[records + rec..records + rec + widths[0] as usize] {
                    *x = 0xff;
                }
            }),
        ),
        ("pack length", Box::new(|b| b[23] ^= 1)),
    ];
    for (what, damage) in damages {
        let mut bytes = good.clone();
        damage(&mut bytes);
        let body = bytes.len() - 32;
        let sum = *blake3::hash(&bytes[..body]).as_bytes();
        bytes[body..].copy_from_slice(&sum);
        let bad_id = pack::pack_id(&bytes);
        std::fs::remove_file(packs.join(format!("{id}.idx"))).unwrap();
        std::fs::rename(
            packs.join(format!("{id}.pack")),
            packs.join(format!("{bad_id}.pack")),
        )
        .unwrap();
        std::fs::write(packs.join(format!("{bad_id}.idx")), &bytes).unwrap();
        match FsBlobStore::new(dir.path()).get(&hashes[1]).await {
            Err(Error::Storage(msg)) => assert!(msg.contains("corrupt"), "{what}: {msg}"),
            other => panic!("{what}: expected a storage error, got {other:?}"),
        }
        std::fs::remove_file(packs.join(format!("{bad_id}.idx"))).unwrap();
        std::fs::rename(
            packs.join(format!("{bad_id}.pack")),
            packs.join(format!("{id}.pack")),
        )
        .unwrap();
        std::fs::write(packs.join(format!("{id}.idx")), &good).unwrap();
    }
    let fresh = FsBlobStore::new(dir.path());
    for (h, v) in hashes.iter().zip(&vs) {
        assert_eq!(&fresh.get(h).await.unwrap().unwrap(), v);
    }
}

#[tokio::test]
async fn a_second_repack_waits_for_the_first() {
    let dir = tempfile::tempdir().unwrap();
    let store = FsBlobStore::new(dir.path());
    store.put(Bytes::from("x")).await.unwrap();
    let first = store
        .repack_publish(Vec::new(), RepackOptions::default())
        .await
        .unwrap();
    let second = store
        .repack_publish(Vec::new(), RepackOptions::default())
        .await;
    assert!(
        matches!(second, Err(Error::Conflict(_))),
        "{:?}",
        second.err()
    );
    first.finish().await.unwrap();
    repack(&store, Vec::new()).await;
}

/// A world with a pack, newer loose objects, a deleted packed object and one
/// rescued by a later upload, so every kind of source is in play. With
/// `two_packs`, a big pack and a small one an incremental repack wrote, each
/// with a deleted and a rescued object, so an incremental repack of it merges
/// the small pack and keeps the big one.
struct World {
    live: BTreeMap<Hash, Bytes>,
    gone: Vec<Hash>,
    hints: Vec<(Hash, Option<Hash>)>,
}

async fn build_world(store: &FsBlobStore, two_packs: bool) -> World {
    let vs = versions(if two_packs { 70 } else { 24 });
    let (packed, small) = if two_packs { (60, 63) } else { (16, 16) };
    let mut hashes = Vec::new();
    let mut live = BTreeMap::new();
    for (i, v) in vs.iter().enumerate() {
        if i == packed {
            repack(store, chain_hints(&hashes)).await;
        }
        if i == small && small > packed {
            let r = repack_incremental(store, chain_hints(&hashes)).await;
            assert_eq!((r.packs_merged, r.packs_after), (0, 2), "{r:?}");
        }
        let h = store.put(v.clone()).await.unwrap();
        hashes.push(h);
        live.insert(h, v.clone());
    }
    let mut gone = vec![hashes[2], hashes[3]];
    let mut rescued = vec![hashes[7]];
    if two_packs {
        gone.push(hashes[61]);
        rescued.push(hashes[62]);
    }
    for h in &gone {
        store.delete(h).await.unwrap();
        live.remove(h);
    }
    for h in &rescued {
        store.delete(h).await.unwrap();
        store.put(live[h].clone()).await.unwrap();
    }
    World {
        live,
        gone,
        hints: chain_hints(&hashes),
    }
}

async fn check_world(root: &Path, w: &World, what: &str) {
    let store = FsBlobStore::with_decoded_cache(root, 0);
    for (h, b) in &w.live {
        assert_eq!(store.get(h).await.unwrap().as_ref(), Some(b), "{what}: {h}");
        assert!(store.contains(h).await.unwrap(), "{what}");
    }
    for h in &w.gone {
        assert!(
            store.get(h).await.unwrap().is_none(),
            "{what}: {h} stays gone"
        );
    }
    let listed: Vec<Hash> = list_all(&store, 5).await.iter().map(|m| m.hash).collect();
    assert_eq!(listed, w.live.keys().copied().collect::<Vec<_>>(), "{what}");
}

/// Abort a repack at each step, as a kill there would leave it, then check
/// every object reads, and that the next repack cleans up and succeeds.
async fn kill_sweep(mode: RepackMode) {
    let incremental = mode != RepackMode::Full;
    let steps = [
        RepackStep::Started,
        RepackStep::Wrote(1),
        RepackStep::Wrote(5),
        RepackStep::PackSynced,
        RepackStep::PackRenamed,
        RepackStep::IndexSynced,
        RepackStep::Published,
        RepackStep::RemovingLoose(1),
        RepackStep::LooseMovedAside(1),
        RepackStep::LooseMovedAside(5),
        RepackStep::OldPacksRemoved,
        RepackStep::TombstonesRewritten,
    ];
    for kill_at in steps {
        let dir = tempfile::tempdir().unwrap();
        let store = FsBlobStore::new(dir.path());
        let w = build_world(&store, incremental).await;
        check_world(dir.path(), &w, "before").await;
        let big = pack_ids(dir.path());
        let seen = Arc::new(Mutex::new(Vec::new()));
        let log = seen.clone();
        let opts = RepackOptions {
            mode,
            hook: Some(Arc::new(move |s| {
                log.lock().unwrap().push(s);
                if s == kill_at {
                    Err(Error::Storage(format!("killed at {s:?}")))
                } else {
                    Ok(())
                }
            })),
            ..RepackOptions::default()
        };
        let res = match store.repack_publish(w.hints.clone(), opts).await {
            Ok(p) => p.finish().await.map(|_| ()),
            Err(e) => Err(e),
        };
        assert!(res.is_err(), "{kill_at:?} was reached");
        assert!(seen.lock().unwrap().contains(&kill_at));
        let what = format!("{mode:?} killed at {kill_at:?}");
        check_world(dir.path(), &w, &what).await;

        let report = if incremental {
            repack_incremental(&store, w.hints.clone()).await
        } else {
            repack(&store, w.hints.clone()).await
        };
        check_world(dir.path(), &w, &format!("{what}, repacked")).await;
        let after = pack_ids(dir.path());
        let junk = files_named(dir.path(), |n| {
            n.starts_with(".tmp-") || n.starts_with(".repack-")
        });
        assert!(junk.is_empty(), "{what}: {junk:?}");
        assert_eq!(loose_count(dir.path()), 0, "{what}");
        if incremental {
            // The big pack stays, with its tombstones; the small one and
            // everything loose end up in one new pack.
            assert_eq!(after.len(), 2, "{what}: {report:?}");
            assert!(after.contains(&big[0]) || after.contains(&big[1]), "{what}");
            let tomb = std::fs::read_to_string(dir.path().join("packs/tombstones")).unwrap();
            for line in tomb.lines() {
                assert!(
                    after.iter().any(|id| line.starts_with(id.as_str())),
                    "{what}: {line}"
                );
            }
        } else {
            assert_eq!(after.len(), 1, "{what}");
            assert_eq!(report.objects, w.live.len() as u64, "{what}");
            assert!(!dir.path().join("packs/tombstones").exists(), "{what}");
        }
    }
}

#[tokio::test]
async fn a_repack_killed_at_any_step_leaves_a_readable_store() {
    kill_sweep(RepackMode::Full).await;
}

#[tokio::test]
async fn an_incremental_repack_killed_at_any_step_leaves_a_readable_store() {
    kill_sweep(INCREMENTAL).await;
}

/// GC deleting objects while a repack runs: what it deleted stays deleted,
/// and an upload that rescues a deleted blob keeps its loose copy.
#[tokio::test]
async fn deletes_during_a_repack_stay_deleted() {
    let dir = tempfile::tempdir().unwrap();
    let store = FsBlobStore::new(dir.path());
    let w = build_world(&store, false).await;
    let victims: Vec<Hash> = w.live.keys().take(3).copied().collect();
    let gc = FsBlobStore::new(dir.path());
    let published = store
        .repack_publish(w.hints.clone(), RepackOptions::default())
        .await
        .unwrap();
    // After publish, before the fence ends: GC deletes, and one victim is
    // uploaded again.
    for h in &victims {
        gc.delete(h).await.unwrap();
    }
    gc.put(w.live[&victims[0]].clone()).await.unwrap();
    published.finish().await.unwrap();
    let fresh = FsBlobStore::new(dir.path());
    assert_eq!(
        fresh.get(&victims[0]).await.unwrap().unwrap(),
        w.live[&victims[0]]
    );
    for h in &victims[1..] {
        assert!(fresh.get(h).await.unwrap().is_none());
        assert!(!fresh.contains(h).await.unwrap());
    }
    // A delete while the pack was being written (simulated by deleting from
    // the hook just before publish).
    let gc2 = gc.clone();
    let victim = *w.live.keys().nth(5).unwrap();
    let opts = RepackOptions {
        hook: Some(Arc::new(move |s| {
            if s == RepackStep::IndexSynced {
                let gc = gc2.clone();
                std::thread::spawn(move || {
                    tokio::runtime::Builder::new_current_thread()
                        .build()
                        .unwrap()
                        .block_on(gc.delete(&victim))
                })
                .join()
                .unwrap()?;
            }
            Ok(())
        })),
        ..RepackOptions::default()
    };
    let r = store
        .repack_publish(w.hints.clone(), opts)
        .await
        .unwrap()
        .finish()
        .await
        .unwrap();
    assert_eq!(r.late_tombstones, 1, "{r:?}");
    assert!(FsBlobStore::new(dir.path())
        .get(&victim)
        .await
        .unwrap()
        .is_none());
}

#[test]
fn the_merge_rule_keeps_pack_sizes_geometric_and_their_count_bounded() {
    let packs = |sizes: &[u64]| -> Vec<(String, u64)> {
        sizes
            .iter()
            .enumerate()
            .map(|(i, n)| (format!("p{i}"), *n))
            .collect()
    };
    let plan = |sizes: &[u64], loose: u64, max: usize| {
        let mut v = plan_merge(&packs(sizes), loose, 2, max);
        v.sort();
        v
    };
    // A big pack and a few loose objects: the big pack stays.
    assert!(plan(&[1000], 10, 8).is_empty());
    // Small packs roll up into the new one while they are not half its size.
    assert_eq!(plan(&[1000, 4, 3], 2, 8), ["p1", "p2"]);
    assert_eq!(plan(&[1000, 40, 3], 2, 8), ["p2"]);
    // A break in the series high up merges everything below it.
    assert_eq!(plan(&[1000, 900, 3], 1, 8), ["p0", "p1", "p2"]);
    // Nothing loose and already geometric: nothing to do; one pack alone is
    // never rewritten.
    assert!(plan(&[1000, 100, 10], 0, 8).is_empty());
    assert!(plan(&[5], 0, 8).is_empty());
    // max_packs forces the smallest packs together.
    let series = [1 << 20, 1 << 15, 1 << 10, 1 << 5];
    assert_eq!(plan(&series, 0, 2), ["p1", "p2", "p3"]);
    assert_eq!(plan(&series, 1, 3), ["p2", "p3"]);

    // Simulated daily runs: the count stays logarithmic in the history.
    let mut sizes: Vec<u64> = vec![100_000];
    let mut most = 0;
    for day in 0..2000u64 {
        let loose = 20 + day % 50;
        let merged = plan_merge(&packs(&sizes), loose, 2, 8);
        let idx: Vec<usize> = merged.iter().map(|m| m[1..].parse().unwrap()).collect();
        let total = loose + idx.iter().map(|&i| sizes[i]).sum::<u64>();
        sizes = sizes
            .iter()
            .enumerate()
            .filter(|(i, _)| !idx.contains(i))
            .map(|(_, n)| *n)
            .collect();
        sizes.push(total);
        most = most.max(sizes.len());
    }
    assert!(most <= 8, "{most}");
    assert_eq!(
        sizes.iter().sum::<u64>(),
        100_000 + (0..2000u64).map(|d| 20 + d % 50).sum::<u64>()
    );
}

/// An incremental repack packs only loose objects, leaves the big pack as it
/// is, and merging a small pack keeps its deltas without recompressing them.
#[tokio::test]
async fn incremental_repacks_pack_loose_objects_and_merge_small_packs() {
    let dir = tempfile::tempdir().unwrap();
    let store = FsBlobStore::new(dir.path());
    let vs = versions(60);
    let mut hashes = Vec::new();
    for v in &vs[..50] {
        hashes.push(store.put(v.clone()).await.unwrap());
    }
    let first = repack_incremental(&store, chain_hints(&hashes)).await;
    assert_eq!((first.objects, first.packs_after), (50, 1), "{first:?}");
    let big = pack_ids(dir.path());
    let big_bytes = std::fs::read(dir.path().join(format!("packs/{}.pack", big[0]))).unwrap();

    for v in &vs[50..54] {
        hashes.push(store.put(v.clone()).await.unwrap());
    }
    let second = repack_incremental(&store, chain_hints(&hashes)).await;
    assert_eq!(
        (second.objects, second.packs_merged, second.packs_after),
        (4, 0, 2),
        "{second:?}"
    );
    // The first of the four has its base in the big pack, so it is whole;
    // the rest are deltas inside the new pack.
    assert_eq!((second.whole, second.deltas), (1, 3), "{second:?}");

    // Three loose objects are worth merging the four-object pack for
    // (4 < 2 * 3); the big pack is not (50 >= 2 * 7).
    for v in &vs[54..57] {
        hashes.push(store.put(v.clone()).await.unwrap());
    }
    let third = repack_incremental(&store, chain_hints(&hashes)).await;
    assert_eq!(
        (third.objects, third.packs_merged, third.packs_after),
        (7, 1, 2),
        "{third:?}"
    );
    assert_eq!(
        third.frames_reused, 4,
        "the merged pack's frames: {third:?}"
    );
    assert_eq!((third.whole, third.deltas), (1, 6), "{third:?}");
    assert!(pack_ids(dir.path()).contains(&big[0]), "the big pack stays");
    assert_eq!(
        std::fs::read(dir.path().join(format!("packs/{}.pack", big[0]))).unwrap(),
        big_bytes
    );
    let fresh = FsBlobStore::with_decoded_cache(dir.path(), 0);
    for (h, v) in hashes.iter().zip(&vs) {
        assert_eq!(&fresh.get(h).await.unwrap().unwrap(), v);
    }
    assert_eq!(loose_count(dir.path()), 0);

    // A full repack folds both into one, reusing the frames it can.
    let full = repack(&store, chain_hints(&hashes)).await;
    assert_eq!((full.objects, full.packs_after), (57, 1), "{full:?}");
    assert!(full.frames_reused >= 50, "{full:?}");
    for (h, v) in hashes.iter().zip(&vs) {
        assert_eq!(
            &FsBlobStore::new(dir.path()).get(h).await.unwrap().unwrap(),
            v
        );
    }
}
