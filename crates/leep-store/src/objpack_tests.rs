//! Packed object stores (ADR 0040), over the in-memory and local-filesystem
//! `object_store` backends: reads, listings, deletes, refreshes and repacks,
//! including repacks killed at every step and readers holding old indexes.

use std::collections::{BTreeMap, HashMap};
use std::ops::Range;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use futures::TryStreamExt;
use leep_core::memory::check_blob_store_contract;
use leep_core::{BlobStore, Error, Hash, TreeNode};
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};

use crate::objrepack::list_all_loose;
use crate::pack::{self, PackIndex};
use crate::{FsBlobStore, ObjectBlobStore, RepackOptions, RepackReport, RepackStep};

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

fn chain_hints(hashes: &[Hash]) -> Vec<(Hash, Option<Hash>)> {
    hashes
        .iter()
        .enumerate()
        .map(|(i, h)| (*h, i.checked_sub(1).map(|p| hashes[p])))
        .collect()
}

/// Each backend, with the temp dir that must outlive it.
fn backends() -> Vec<(
    &'static str,
    Arc<dyn ObjectStore>,
    Option<tempfile::TempDir>,
)> {
    let dir = tempfile::tempdir().unwrap();
    let local = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
    vec![
        ("memory", Arc::new(InMemory::new()), None),
        ("local", Arc::new(local), Some(dir)),
    ]
}

/// Another handle on the same bucket, as another process would open it.
fn handle(store: &Arc<dyn ObjectStore>) -> ObjectBlobStore {
    ObjectBlobStore::new(store.clone())
}

/// A handle that lists packs on every miss and caches nothing decoded.
fn fresh(store: &Arc<dyn ObjectStore>) -> ObjectBlobStore {
    ObjectBlobStore::with_options(store.clone(), 0, Duration::ZERO)
}

async fn repack(store: &ObjectBlobStore, hints: Vec<(Hash, Option<Hash>)>) -> RepackReport {
    store
        .repack_publish(hints, RepackOptions::default())
        .await
        .unwrap()
        .finish()
        .await
        .unwrap()
}

async fn keys(store: &dyn ObjectStore, prefix: &str) -> Vec<String> {
    let p = Path::from(prefix);
    let mut out: Vec<String> = store
        .list(Some(&p))
        .map_ok(|o| o.location.to_string())
        .try_collect()
        .await
        .unwrap();
    out.sort();
    out
}

async fn loose_count(store: &dyn ObjectStore) -> usize {
    list_all_loose(store).await.unwrap().len()
}

async fn pack_ids(store: &dyn ObjectStore) -> Vec<String> {
    crate::objpacks::list_packs_dir(store).await.unwrap().0
}

async fn index_of(store: &dyn ObjectStore) -> (String, PackIndex) {
    let ids = pack_ids(store).await;
    assert_eq!(ids.len(), 1, "one pack");
    let bytes = store
        .get(&crate::objpacks::idx_key(&ids[0]))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    (ids[0].clone(), PackIndex::decode(bytes.to_vec()).unwrap())
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
async fn packed_object_store_passes_the_contract() {
    for (name, objects, _dir) in backends() {
        let store = handle(&objects);
        check_blob_store_contract(&store).await;
        let report = repack(&store, Vec::new()).await;
        assert!(report.objects > 10, "{name}: {report:?}");
        assert_eq!(
            loose_count(&*objects).await,
            0,
            "{name}: every loose copy went"
        );
        assert_eq!(report.loose_removed, report.loose_before, "{name}");
        check_blob_store_contract(&store).await;
        check_blob_store_contract(&handle(&objects)).await;
        repack(&store, Vec::new()).await;
        check_blob_store_contract(&fresh(&objects)).await;
        assert!(
            keys(&*objects, "packs")
                .await
                .iter()
                .all(|k| !k.ends_with(".lease")),
            "{name}: the lease is released"
        );
    }
}

#[tokio::test]
async fn quiet_fault_wrapper_passes_the_contract_on_a_packed_object_store() {
    use leep_testkit::{FaultPlan, FaultyBlobStore};
    let store = ObjectBlobStore::in_memory();
    check_blob_store_contract(&store).await;
    repack(&store, Vec::new()).await;
    let faulty = FaultyBlobStore::new(Arc::new(store), FaultPlan::new(1));
    check_blob_store_contract(&faulty).await;
}

/// The bucket holds `packs/<id>.pack` and `packs/<id>.idx` in the filesystem
/// store's format: copied into a filesystem store, they read there too.
#[tokio::test]
async fn reads_after_repack_equal_the_originals_in_the_shared_format() {
    for (name, objects, _dir) in backends() {
        let store = handle(&objects);
        let mut originals: BTreeMap<Hash, Bytes> = BTreeMap::new();
        let vs = versions(80);
        let mut chain = Vec::new();
        for v in &vs {
            let h = store.put(v.clone()).await.unwrap();
            originals.insert(h, v.clone());
            chain.push(h);
        }
        let node = TreeNode::default();
        let node_bytes = Bytes::from(node.encode());
        store
            .put_with_hash(node.hash(), node_bytes.clone())
            .await
            .unwrap();
        originals.insert(node.hash(), node_bytes);
        originals.insert(store.put(Bytes::new()).await.unwrap(), Bytes::new());
        let big: Vec<u8> = (0..3_000_000u32)
            .map(|i| (i.wrapping_mul(2_654_435_761) >> 13) as u8)
            .collect();
        let big = Bytes::from(big);
        originals.insert(store.put(big.clone()).await.unwrap(), big);

        let report = repack(&store, chain_hints(&chain)).await;
        assert_eq!(report.objects, originals.len() as u64, "{name}");
        assert_eq!(report.loose_removed, originals.len() as u64, "{name}");
        assert_eq!((report.loose_after, report.loose_kept), (0, 0), "{name}");
        assert!(report.deltas >= 70, "{name}: {report:?}");
        assert_eq!(loose_count(&*objects).await, 0);

        let (id, idx) = index_of(&*objects).await;
        assert_eq!(
            keys(&*objects, "packs").await,
            vec![format!("packs/{id}.idx"), format!("packs/{id}.pack")],
            "{name}: only the pack, its index and nothing else"
        );
        assert_eq!(idx.magic(), pack::IDX_MAGIC);
        for s in [handle(&objects), fresh(&objects)] {
            for (h, b) in &originals {
                assert_eq!(s.get(h).await.unwrap().as_ref(), Some(b), "{name}: {h}");
                assert!(s.contains(h).await.unwrap());
            }
        }

        let dir = tempfile::tempdir().unwrap();
        let packs = dir.path().join("packs");
        std::fs::create_dir_all(&packs).unwrap();
        for ext in ["pack", "idx"] {
            let key = Path::from(format!("packs/{id}.{ext}"));
            let bytes = objects.get(&key).await.unwrap().bytes().await.unwrap();
            std::fs::write(packs.join(format!("{id}.{ext}")), bytes).unwrap();
        }
        let fs = FsBlobStore::new(dir.path());
        for (h, b) in &originals {
            assert_eq!(
                fs.get(h).await.unwrap().as_ref(),
                Some(b),
                "{name}: fs reads {h}"
            );
        }
    }
}

#[tokio::test]
async fn listing_merges_loose_and_packed_and_keeps_ages() {
    for (name, objects, _dir) in backends() {
        let store = handle(&objects);
        let mut sizes = BTreeMap::new();
        for i in 0..30u32 {
            let b = Bytes::from(format!("packed {i} ").repeat(50 + i as usize));
            sizes.insert(store.put(b.clone()).await.unwrap(), b.len() as u64);
        }
        let ages: BTreeMap<Hash, Option<i64>> = list_all(&store, 1000)
            .await
            .into_iter()
            .map(|m| (m.hash, m.modified))
            .collect();
        repack(&store, Vec::new()).await;
        let after: BTreeMap<Hash, Option<i64>> = list_all(&store, 1000)
            .await
            .into_iter()
            .map(|m| (m.hash, m.modified))
            .collect();
        assert_eq!(after, ages, "{name}: packed objects keep their loose ages");
        for i in 0..30u32 {
            let b = Bytes::from(format!("loose {i}"));
            sizes.insert(store.put(b.clone()).await.unwrap(), b.len() as u64);
        }
        store
            .put(Bytes::from("packed 3 ".repeat(53)))
            .await
            .unwrap();
        let all = list_all(&store, 1000).await;
        assert!(all.windows(2).all(|w| w[0].hash < w[1].hash), "{name}");
        let listed: BTreeMap<Hash, u64> = all.iter().map(|m| (m.hash, m.size)).collect();
        assert_eq!(listed, sizes, "{name}: complete, with logical sizes");
        for page in [1, 3, 7] {
            assert_eq!(list_all(&store, page).await, all, "{name}: page {page}");
        }
        assert_eq!(
            store.list_page(Some(all[29].hash), 5).await.unwrap(),
            all[30..35]
        );
    }
}

#[tokio::test]
async fn tombstones_hide_packed_objects_an_upload_rescues_and_a_repack_drops_them() {
    for (name, objects, _dir) in backends() {
        let store = handle(&objects);
        let vs = versions(10);
        let mut hashes = Vec::new();
        for v in &vs {
            hashes.push(store.put(v.clone()).await.unwrap());
        }
        repack(&store, chain_hints(&hashes)).await;
        let (id, _) = index_of(&*objects).await;
        let doomed = [hashes[0], hashes[4], hashes[5]];
        let other = handle(&objects);
        assert!(other.contains(&hashes[4]).await.unwrap());
        assert!(other.get(&hashes[4]).await.unwrap().is_some());
        for h in &doomed {
            store.delete(h).await.unwrap();
            store.delete(h).await.unwrap();
        }
        let mut want: Vec<String> = doomed
            .iter()
            .map(|h| format!("packs/tombstones/{id}/{h}"))
            .collect();
        want.sort();
        assert_eq!(keys(&*objects, "packs/tombstones").await, want, "{name}");
        for s in [&store, &other] {
            for (i, h) in hashes.iter().enumerate() {
                let gone = doomed.contains(h);
                // `contains` is exact even from the handle that cached the
                // index before the deletes; `get` follows it.
                assert_eq!(s.contains(h).await.unwrap(), !gone, "{name}: contains {i}");
                assert_eq!(s.get(h).await.unwrap().is_none(), gone, "{name}: get {i}");
            }
            let listed: Vec<Hash> = list_all(s, 3).await.iter().map(|m| m.hash).collect();
            assert_eq!(listed.len(), hashes.len() - doomed.len(), "{name}");
        }
        // An upload rescuing a deleted blob wins over the tombstone.
        store.put(vs[5].clone()).await.unwrap();
        assert_eq!(other.get(&hashes[5]).await.unwrap().unwrap(), vs[5]);
        assert!(other.contains(&hashes[5]).await.unwrap());

        let report = repack(&store, chain_hints(&hashes)).await;
        assert_eq!(report.dropped_tombstoned, 2, "{name}: {report:?}");
        assert_eq!(report.objects, 8);
        assert!(
            keys(&*objects, "packs/tombstones").await.is_empty(),
            "{name}: tombstones of the replaced pack are gone"
        );
        for s in [&store, &other, &fresh(&objects)] {
            for (i, h) in hashes.iter().enumerate() {
                let gone = doomed.contains(h) && *h != hashes[5];
                assert_eq!(s.get(h).await.unwrap().is_none(), gone, "{name}: after {i}");
                assert_eq!(s.contains(h).await.unwrap(), !gone, "{name}: after {i}");
            }
        }
    }
}

#[tokio::test]
async fn a_corrupt_pack_entry_is_a_storage_error_not_wrong_bytes() {
    let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let store = handle(&objects);
    let vs = versions(4);
    let mut hashes = Vec::new();
    for v in &vs {
        hashes.push(store.put(v.clone()).await.unwrap());
    }
    repack(&store, chain_hints(&hashes)).await;
    let (id, idx) = index_of(&*objects).await;
    let key = crate::objpacks::pack_key(&id);
    let mut bytes = objects
        .get(&key)
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap()
        .to_vec();
    let e = idx.find(&hashes[0]).unwrap();
    bytes[(e.offset + e.stored_len - 2) as usize] ^= 0x40;
    objects.put(&key, bytes.into()).await.unwrap();
    for h in &hashes {
        match fresh(&objects).get(h).await {
            Err(Error::Storage(msg)) => assert!(msg.contains("corrupt"), "{msg}"),
            other => panic!("expected a storage error, got {other:?}"),
        }
    }
    // A pack whose length is not the index's is refused.
    objects
        .put(&key, Bytes::from_static(b"LEEPPAK1").into())
        .await
        .unwrap();
    assert!(matches!(
        fresh(&objects).get(&hashes[1]).await,
        Err(Error::Storage(_))
    ));
}

#[tokio::test]
async fn one_repack_at_a_time_and_an_expired_lease_is_taken_over() {
    for (name, objects, _dir) in backends() {
        let store = handle(&objects);
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
            "{name}: {:?}",
            second.err()
        );
        first.finish().await.unwrap();
        repack(&store, Vec::new()).await;

        // A repack that died an hour ago left its lease behind.
        let lease = Path::from("packs/repack.lease");
        let stale = "leep repack lease\nholder dead/1/x\nexpires 1000\n";
        objects.put(&lease, stale.into()).await.unwrap();
        repack(&store, Vec::new()).await;
        assert!(objects.head(&lease).await.is_err(), "{name}: released");
        // A live one is respected.
        let live = format!(
            "leep repack lease\nholder other/2/y\nexpires {}\n",
            crate::fs::unix_secs(std::time::SystemTime::now()).unwrap() + 600
        );
        objects.put(&lease, live.into()).await.unwrap();
        let refused = store
            .repack_publish(Vec::new(), RepackOptions::default())
            .await;
        assert!(matches!(refused, Err(Error::Conflict(_))), "{name}");
    }
}

/// A world with a pack, newer loose objects, deleted packed objects and one
/// rescued by a later upload, so every kind of source is in play.
struct World {
    live: BTreeMap<Hash, Bytes>,
    gone: Vec<Hash>,
    hints: Vec<(Hash, Option<Hash>)>,
}

async fn build_world(store: &ObjectBlobStore) -> World {
    let vs = versions(24);
    let mut hashes = Vec::new();
    let mut live = BTreeMap::new();
    for v in &vs[..16] {
        let h = store.put(v.clone()).await.unwrap();
        hashes.push(h);
        live.insert(h, v.clone());
    }
    repack(store, chain_hints(&hashes)).await;
    for v in &vs[16..] {
        let h = store.put(v.clone()).await.unwrap();
        hashes.push(h);
        live.insert(h, v.clone());
    }
    let gone = vec![hashes[2], hashes[3]];
    for h in &gone {
        store.delete(h).await.unwrap();
        live.remove(h);
    }
    store.delete(&hashes[7]).await.unwrap();
    store.put(vs[7].clone()).await.unwrap();
    World {
        live,
        gone,
        hints: chain_hints(&hashes),
    }
}

async fn check_world(objects: &Arc<dyn ObjectStore>, w: &World, what: &str) {
    let store = fresh(objects);
    for (h, b) in &w.live {
        assert_eq!(store.get(h).await.unwrap().as_ref(), Some(b), "{what}: {h}");
        assert!(store.contains(h).await.unwrap(), "{what}");
    }
    for h in &w.gone {
        assert!(
            store.get(h).await.unwrap().is_none(),
            "{what}: {h} stays gone"
        );
        assert!(!store.contains(h).await.unwrap(), "{what}: {h} stays gone");
    }
    let listed: Vec<Hash> = list_all(&store, 5).await.iter().map(|m| m.hash).collect();
    assert_eq!(listed, w.live.keys().copied().collect::<Vec<_>>(), "{what}");
}

/// Abort a repack at each step, as a kill there would leave it, then check
/// every object reads, and that the next repack cleans up and succeeds.
#[tokio::test]
async fn a_repack_killed_at_any_step_leaves_a_readable_store() {
    let steps = [
        RepackStep::Started,
        RepackStep::Wrote(1),
        RepackStep::Wrote(10),
        RepackStep::PackSynced,
        RepackStep::PackRenamed,
        RepackStep::IndexSynced,
        RepackStep::Published,
        RepackStep::RemovingLoose(1),
        RepackStep::RemovingLoose(6),
        RepackStep::LooseMovedAside(4),
        RepackStep::LooseMovedAside(8),
        RepackStep::OldPacksRemoved,
        RepackStep::TombstonesRewritten,
    ];
    for (name, _, _) in backends() {
        for kill_at in steps {
            let mut b = backends();
            let (_, objects, _dir) = b.remove(if name == "memory" { 0 } else { 1 });
            let store = handle(&objects);
            let w = build_world(&store).await;
            check_world(&objects, &w, "before").await;
            let seen = Arc::new(Mutex::new(Vec::new()));
            let log = seen.clone();
            let opts = RepackOptions {
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
            let what = format!("{name}: killed at {kill_at:?}");
            assert!(res.is_err(), "{what} was reached");
            assert!(seen.lock().unwrap().contains(&kill_at), "{what}");
            check_world(&objects, &w, &what).await;

            let report = repack(&store, w.hints.clone()).await;
            check_world(&objects, &w, &format!("{what}, repacked")).await;
            assert_eq!(pack_ids(&*objects).await.len(), 1, "{what}");
            assert_eq!(report.objects, w.live.len() as u64, "{what}");
            assert_eq!(loose_count(&*objects).await, 0, "{what}");
            let (id, _) = index_of(&*objects).await;
            assert_eq!(
                keys(&*objects, "packs").await,
                vec![format!("packs/{id}.idx"), format!("packs/{id}.pack")],
                "{what}: no garbage, tombstones or lease left"
            );
        }
    }
}

/// Run `f` to completion from a hook, which may be on the runtime's thread.
fn block_on_side<F: std::future::Future<Output = leep_core::Result<()>> + Send + 'static>(
    f: F,
) -> leep_core::Result<()> {
    std::thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(f)
    })
    .join()
    .unwrap()
}

/// GC deleting objects while a repack runs: what it deleted stays deleted,
/// and an upload that rescues a deleted blob keeps its loose copy.
#[tokio::test]
async fn deletes_during_a_repack_stay_deleted() {
    for (name, objects, _dir) in backends() {
        let store = handle(&objects);
        let w = build_world(&store).await;
        let victims: Vec<Hash> = w.live.keys().take(3).copied().collect();
        let gc = handle(&objects);
        let published = store
            .repack_publish(w.hints.clone(), RepackOptions::default())
            .await
            .unwrap();
        for h in &victims {
            gc.delete(h).await.unwrap();
        }
        gc.put(w.live[&victims[0]].clone()).await.unwrap();
        published.finish().await.unwrap();
        let reader = fresh(&objects);
        assert_eq!(
            reader.get(&victims[0]).await.unwrap().unwrap(),
            w.live[&victims[0]],
            "{name}"
        );
        for h in &victims[1..] {
            assert!(reader.get(h).await.unwrap().is_none(), "{name}");
            assert!(!reader.contains(h).await.unwrap(), "{name}");
        }

        // A delete while the pack was being written.
        let gc2 = gc.clone();
        let victim = *w.live.keys().nth(5).unwrap();
        let opts = RepackOptions {
            hook: Some(Arc::new(move |s| {
                if s == RepackStep::IndexSynced {
                    let gc = gc2.clone();
                    block_on_side(async move { gc.delete(&victim).await })?;
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
        assert_eq!(r.late_tombstones, 1, "{name}: {r:?}");
        assert!(
            fresh(&objects).get(&victim).await.unwrap().is_none(),
            "{name}"
        );
    }
}

/// GC deletes a packed blob and an upload rescues it in the moment between
/// the repack's last look at the tombstones and its delete of the loose
/// copies: the repack notices and puts the loose copy back.
#[tokio::test]
async fn a_rescue_racing_the_loose_delete_keeps_its_copy() {
    let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let store = handle(&objects);
    let vs = versions(3);
    for v in &vs {
        store.put(v.clone()).await.unwrap();
    }
    let victim = Hash::of_blob(&vs[1]);
    let gc = handle(&objects);
    let (gc2, data) = (gc.clone(), vs[1].clone());
    let opts = RepackOptions {
        hook: Some(Arc::new(move |s| {
            if s == RepackStep::RemovingLoose(3) {
                // GC's delete: tombstone the new pack's copy, delete the
                // loose one; then the upload writes it again, unchanged.
                let (gc, data) = (gc2.clone(), data.clone());
                block_on_side(async move {
                    gc.delete(&victim).await?;
                    gc.put(data).await.map(|_| ())
                })?;
            }
            Ok(())
        })),
        ..RepackOptions::default()
    };
    let published = store.repack_publish(Vec::new(), opts).await.unwrap();
    let r = published.finish().await.unwrap();
    assert_eq!(r.loose_kept, 1, "{r:?}");
    assert_eq!(fresh(&objects).get(&victim).await.unwrap().unwrap(), vs[1]);
    assert!(fresh(&objects).contains(&victim).await.unwrap());
}

/// A pack another process publishes becomes visible on a miss, at most one
/// refresh interval after the previous listing.
#[tokio::test]
async fn a_miss_refreshes_and_finds_a_new_pack() {
    let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = handle(&objects);
    let h = writer.put(Bytes::from("soon packed")).await.unwrap();
    let eager = fresh(&objects);
    let lazy = ObjectBlobStore::with_options(objects.clone(), 0, Duration::from_secs(3600));
    for r in [&eager, &lazy] {
        assert!(r.get(&Hash::of_blob(b"absent")).await.unwrap().is_none());
    }
    repack(&writer, Vec::new()).await;
    assert_eq!(loose_count(&*objects).await, 0);
    assert_eq!(&eager.get(&h).await.unwrap().unwrap()[..], b"soon packed");
    assert!(eager.contains(&h).await.unwrap());
    // Within the interval a miss does not list again: why the interval must
    // stay below the repack fence.
    assert!(lazy.get(&h).await.unwrap().is_none());
    lazy.packs.refresh().await.unwrap();
    assert_eq!(&lazy.get(&h).await.unwrap().unwrap()[..], b"soon packed");
}

/// A reader holding the index of a pack that a later repack replaced and
/// deleted after its fence: its reads hit a missing pack, refresh and find
/// the object in the new pack, and `contains` sees a delete made in the new
/// pack only.
#[tokio::test]
async fn a_reader_with_an_old_index_finds_objects_in_the_new_pack() {
    for (name, objects, _dir) in backends() {
        let writer = handle(&objects);
        let vs = versions(6);
        let mut hashes = Vec::new();
        for v in &vs {
            hashes.push(writer.put(v.clone()).await.unwrap());
        }
        repack(&writer, chain_hints(&hashes)).await;
        let old = pack_ids(&*objects).await;
        let reader = ObjectBlobStore::with_options(objects.clone(), 0, Duration::from_secs(3600));
        assert_eq!(reader.get(&hashes[0]).await.unwrap().unwrap(), vs[0]);
        writer.put(Bytes::from("one more")).await.unwrap();
        repack(&writer, chain_hints(&hashes)).await;
        let new = pack_ids(&*objects).await;
        assert_ne!(old, new, "{name}: the old pack was replaced and deleted");
        for (h, v) in hashes.iter().zip(&vs) {
            assert_eq!(&reader.get(h).await.unwrap().unwrap(), v, "{name}");
        }

        let stale = ObjectBlobStore::with_options(objects.clone(), 0, Duration::from_secs(3600));
        assert!(stale.contains(&hashes[1]).await.unwrap());
        writer.put(Bytes::from("and another")).await.unwrap();
        repack(&writer, Vec::new()).await;
        writer.delete(&hashes[1]).await.unwrap();
        assert!(!stale.contains(&hashes[1]).await.unwrap(), "{name}");
        assert!(stale.contains(&hashes[2]).await.unwrap(), "{name}");
    }
}

/// Requests by kind, to measure what reads and repacks cost.
#[derive(Debug)]
struct Counting {
    inner: InMemory,
    counts: Mutex<HashMap<&'static str, u64>>,
}

impl Counting {
    fn bump(&self, what: &'static str) {
        *self.counts.lock().unwrap().entry(what).or_default() += 1;
    }

    fn take(&self) -> BTreeMap<&'static str, u64> {
        std::mem::take(&mut *self.counts.lock().unwrap())
            .into_iter()
            .collect()
    }
}

impl std::fmt::Display for Counting {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Counting({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for Counting {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.bump("PUT");
        self.inner.put_opts(location, payload, opts).await
    }
    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.bump("multipart upload");
        self.inner.put_multipart_opts(location, opts).await
    }
    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.bump(if options.head {
            "HEAD"
        } else if options.range.is_some() {
            "GET range"
        } else {
            "GET"
        });
        self.inner.get_opts(location, options).await
    }
    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> object_store::Result<Vec<Bytes>> {
        // The default coalesces and calls `get_range` (counted) per run.
        object_store::coalesce_ranges(
            ranges,
            |r| self.get_range(location, r),
            object_store::OBJECT_STORE_COALESCE_DEFAULT,
        )
        .await
    }
    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.bump("DELETE");
        self.inner.delete_stream(locations)
    }
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.bump("LIST");
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.bump("LIST");
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

/// What a read and a repack cost in requests, for ADR 0040. Printed with
/// `--nocapture`; the asserts pin the per-read costs.
#[tokio::test]
async fn request_counts() {
    let counting = Arc::new(Counting {
        inner: InMemory::new(),
        counts: Mutex::default(),
    });
    let objects: Arc<dyn ObjectStore> = counting.clone();
    let store = handle(&objects);
    let vs = versions(200);
    let mut hashes = Vec::new();
    for v in &vs {
        hashes.push(store.put(v.clone()).await.unwrap());
    }
    counting.take();
    let report = repack(&store, chain_hints(&hashes)).await;
    let repack_counts = counting.take();
    println!(
        "repack of {} loose objects: {repack_counts:?}",
        report.objects
    );
    let again = repack(&store, chain_hints(&hashes)).await;
    let again_counts = counting.take();
    println!("the same repack again: {again_counts:?}");
    assert_eq!(again.pack_id, report.pack_id, "nothing changed");
    assert!(
        !again_counts.contains_key("multipart upload"),
        "{again_counts:?}"
    );

    let reader = ObjectBlobStore::with_options(objects.clone(), 0, Duration::from_secs(3600));
    reader.get(&hashes[0]).await.unwrap().unwrap();
    println!("first read (loads the index): {:?}", counting.take());
    reader.get(&hashes[199]).await.unwrap().unwrap();
    let cold = counting.take();
    println!("packed read, depth-32 chain, no decoded cache: {cold:?}");
    assert_eq!(cold.get("GET"), Some(&1), "one loose miss");
    assert!(cold.get("GET range").is_some_and(|n| *n <= 2), "{cold:?}");
    let cached =
        ObjectBlobStore::with_options(objects.clone(), 64 << 20, Duration::from_secs(3600));
    cached.get(&hashes[198]).await.unwrap();
    counting.take();
    cached.get(&hashes[199]).await.unwrap();
    let warm = counting.take();
    println!("packed read, base in the decoded cache: {warm:?}");
    assert_eq!(
        (warm.get("GET"), warm.get("GET range")),
        (Some(&1), Some(&1))
    );
    reader.contains(&hashes[5]).await.unwrap();
    let contains = counting.take();
    println!("packed contains: {contains:?}");
    assert_eq!(contains.get("HEAD"), Some(&3), "loose, tombstone, index");
    assert!(reader
        .get(&Hash::of_blob(b"absent"))
        .await
        .unwrap()
        .is_none());
    let miss = counting.take();
    println!("miss within the refresh interval: {miss:?}");
    assert_eq!(miss.len(), 1, "{miss:?}");
}
