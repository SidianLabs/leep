//! Pack-set stamps (ADR 0070): a miss checks a cheap stamp and scans the
//! packs only when it changed, and `get_exact` looks once more.

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use bytes::Bytes;
use leep_core::{BlobStore, Hash};
use object_store::memory::InMemory;
use object_store::{ObjectStore, ObjectStoreExt};

use crate::objpacks::gen_key;
use crate::packset::PACKS_DIR;
use crate::{FsBlobStore, ObjectBlobStore, RepackOptions};

/// A batch large enough to be written as one pack.
fn batch(tag: &str) -> Vec<(Hash, Bytes)> {
    (0..24)
        .map(|i| {
            let data = Bytes::from(format!("{tag} object {i} ").repeat(i % 5 + 1));
            (Hash::of_blob(&data), data)
        })
        .collect()
}

async fn publish(store: &dyn BlobStore, tag: &str) -> Vec<(Hash, Bytes)> {
    let items = batch(tag);
    let packed = store.put_many(items.clone()).await.unwrap();
    assert_eq!(packed.len(), items.len(), "{tag}: written as one pack");
    items
}

fn packs_mtime(root: &Path) -> SystemTime {
    std::fs::metadata(root.join(PACKS_DIR))
        .unwrap()
        .modified()
        .unwrap()
}

/// Set the modification time of `packs/`, as a change within one
/// timestamp tick (or a filesystem that hides it) leaves it.
fn set_packs_mtime(root: &Path, t: SystemTime) {
    std::fs::File::open(root.join(PACKS_DIR))
        .unwrap()
        .set_modified(t)
        .unwrap();
}

fn absent() -> Hash {
    Hash::of_blob(b"never stored")
}

#[tokio::test]
async fn a_second_store_sees_a_pack_the_first_published() {
    let dir = tempfile::tempdir().unwrap();
    let a = FsBlobStore::new(dir.path());
    let b = FsBlobStore::new(dir.path());
    let early = publish(&a, "early").await;
    // B's view, listing the early pack; B publishes nothing itself.
    assert_eq!(b.get(&early[0].0).await.unwrap(), Some(early[0].1.clone()));
    let late = publish(&a, "late").await;
    for (h, data) in &late {
        assert_eq!(b.get(h).await.unwrap().as_ref(), Some(data), "{h}");
        assert!(b.contains(h).await.unwrap());
    }
    let hashes: Vec<Hash> = late.iter().map(|(h, _)| *h).collect();
    assert!(b.contains_many(&hashes).await.unwrap().iter().all(|c| *c));
}

/// With `packs/` unchanged since a confirmed scan, a miss costs two stats
/// and no directory scan. Prints what a miss costs (`--nocapture`).
#[tokio::test]
async fn a_miss_on_an_unchanged_directory_does_not_scan() {
    let dir = tempfile::tempdir().unwrap();
    let writer = FsBlobStore::new(dir.path());
    publish(&writer, "one").await;
    publish(&writer, "two").await;
    set_packs_mtime(dir.path(), SystemTime::now() - Duration::from_secs(10));
    let store = FsBlobStore::new(dir.path());
    assert!(store.get(&absent()).await.unwrap().is_none());
    let scans = store.packs.scans();
    for _ in 0..100 {
        assert!(store.get(&absent()).await.unwrap().is_none());
        assert!(!store.contains(&absent()).await.unwrap());
    }
    assert!(store
        .contains_many(&[absent(), Hash::of_blob(b"also absent")])
        .await
        .unwrap()
        .iter()
        .all(|c| !c));
    assert_eq!(store.packs.scans(), scans, "no directory scan");

    const N: u32 = 20_000;
    let started = Instant::now();
    for _ in 0..N {
        assert!(store.packs.get(&absent()).unwrap().is_none());
    }
    let pack_miss = started.elapsed() / N;
    let started = Instant::now();
    for _ in 0..N / 10 {
        assert!(store.get(&absent()).await.unwrap().is_none());
    }
    let store_miss = started.elapsed() / (N / 10);
    assert_eq!(store.packs.scans(), scans, "no directory scan");
    println!(
        "miss on an unchanged directory: {pack_miss:?} in the pack set, \
         {store_miss:?} through FsBlobStore::get (loose read and blocking task included)"
    );
}

/// Two publishes in the tick of the last change before a scan leave the
/// time of `packs/` as the scan recorded it. The view is unconfirmed, so a
/// miss rescans once the rate limit allows, and finds both.
#[tokio::test]
async fn two_publishes_within_one_mtime_tick_are_both_found() {
    let dir = tempfile::tempdir().unwrap();
    let writer = FsBlobStore::new(dir.path());
    publish(&writer, "before").await;
    let reader = FsBlobStore::new(dir.path());
    assert!(reader.get(&absent()).await.unwrap().is_none());
    let tick = packs_mtime(dir.path());
    let first = publish(&writer, "first").await;
    let second = publish(&writer, "second").await;
    set_packs_mtime(dir.path(), tick);
    tokio::time::sleep(crate::packset::UNCONFIRMED_RESCAN + Duration::from_millis(20)).await;
    assert_eq!(
        reader.get(&first[3].0).await.unwrap(),
        Some(first[3].1.clone())
    );
    assert_eq!(
        reader.get(&second[5].0).await.unwrap(),
        Some(second[5].1.clone())
    );
}

/// A stamp that hides a pack (here set back by hand on a confirmed view)
/// fools `get`, never `get_exact`, which scans once more after the miss.
#[tokio::test]
async fn get_exact_finds_a_pack_the_stamp_hides() {
    let dir = tempfile::tempdir().unwrap();
    let writer = FsBlobStore::new(dir.path());
    publish(&writer, "before").await;
    let old = SystemTime::now() - Duration::from_secs(10);
    set_packs_mtime(dir.path(), old);
    let reader = FsBlobStore::new(dir.path());
    assert!(reader.get(&absent()).await.unwrap().is_none());
    let hidden = publish(&writer, "hidden").await;
    set_packs_mtime(dir.path(), old);
    let scans = reader.packs.scans();
    assert_eq!(reader.get(&hidden[0].0).await.unwrap(), None, "stale view");
    assert_eq!(reader.packs.scans(), scans);
    assert_eq!(
        reader.get_exact(&hidden[0].0).await.unwrap(),
        Some(hidden[0].1.clone())
    );
    assert_eq!(reader.packs.scans(), scans + 1, "one scan");
    // Lookups started together share that one scan.
    let hidden2 = publish(&writer, "hidden two").await;
    set_packs_mtime(dir.path(), old);
    let scans = reader.packs.scans();
    let asks = hidden2.iter().map(|(h, _)| reader.get_exact(h));
    let got = futures::future::try_join_all(asks).await.unwrap();
    for ((_, data), got) in hidden2.iter().zip(got) {
        assert_eq!(got.as_ref(), Some(data));
    }
    assert!(
        reader.packs.scans() - scans <= 2,
        "{} scans",
        reader.packs.scans() - scans
    );
    assert_eq!(reader.get_exact(&absent()).await.unwrap(), None);
}

/// A repack in another process deletes a pack this one has in its view:
/// the open handle keeps reading it, and the next scan drops it.
#[tokio::test]
async fn a_repack_deletes_a_pack_between_view_and_read() {
    let dir = tempfile::tempdir().unwrap();
    let writer = FsBlobStore::new(dir.path());
    let a = publish(&writer, "a").await;
    let b = publish(&writer, "b").await;
    let reader = FsBlobStore::new(dir.path());
    assert_eq!(reader.get(&a[0].0).await.unwrap(), Some(a[0].1.clone()));
    let before = reader.packs.view().unwrap().searchable().len();
    assert_eq!(before, 2);
    writer
        .repack_publish(Vec::new(), RepackOptions::default())
        .await
        .unwrap()
        .finish()
        .await
        .unwrap();
    assert_eq!(writer.pack_count().unwrap(), 1);
    for (h, data) in a.iter().chain(&b) {
        assert_eq!(reader.get(h).await.unwrap().as_ref(), Some(data), "{h}");
        assert_eq!(reader.get_exact(h).await.unwrap().as_ref(), Some(data));
    }
    // A miss notices the changed directory and drops the deleted packs.
    assert!(reader.get(&absent()).await.unwrap().is_none());
    assert_eq!(reader.packs.view().unwrap().searchable().len(), 1);
    for (h, data) in a.iter().chain(&b) {
        assert_eq!(reader.get(h).await.unwrap().as_ref(), Some(data), "{h}");
    }
}

/// A bucket that older servers wrote has no stamp: every miss lists
/// `packs/`, as before.
#[tokio::test]
async fn a_bucket_without_the_stamp_still_finds_foreign_packs() {
    let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = ObjectBlobStore::new(objects.clone());
    let reader = ObjectBlobStore::new(objects.clone());
    publish(&writer, "old").await;
    objects.delete(&gen_key()).await.unwrap();
    assert!(reader.get(&absent()).await.unwrap().is_none());
    let late = publish(&writer, "late").await;
    objects.delete(&gen_key()).await.unwrap();
    for (h, data) in &late {
        assert_eq!(reader.get(h).await.unwrap().as_ref(), Some(data), "{h}");
    }
    let lists = reader.packs.lists();
    assert!(reader.get(&absent()).await.unwrap().is_none());
    assert_eq!(reader.packs.lists(), lists + 1, "a listing per miss");
}

/// With the stamp, a miss lists `packs/` only after a publish: a foreign
/// pack is found once the stamp HEAD a miss may reuse is newer than the
/// publish, and at once by `get_exact`.
#[tokio::test]
async fn a_stamped_bucket_lists_only_after_a_publish() {
    let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let writer = ObjectBlobStore::new(objects.clone());
    let reader = ObjectBlobStore::new(objects.clone());
    publish(&writer, "first").await;
    assert!(reader.get(&absent()).await.unwrap().is_none());
    let lists = reader.packs.lists();
    for _ in 0..10 {
        assert!(reader.get(&absent()).await.unwrap().is_none());
        assert!(!reader.contains(&absent()).await.unwrap());
    }
    assert_eq!(reader.packs.lists(), lists, "no listing while unchanged");

    let late = publish(&writer, "late").await;
    tokio::time::sleep(crate::objpacks::GEN_WINDOW + Duration::from_millis(10)).await;
    for (h, data) in &late {
        assert_eq!(reader.get(h).await.unwrap().as_ref(), Some(data), "{h}");
    }
    assert_eq!(
        reader.packs.lists(),
        lists + 1,
        "one listing for the publish"
    );

    let exact = publish(&writer, "exact").await;
    assert_eq!(
        reader.get_exact(&exact[0].0).await.unwrap(),
        Some(exact[0].1.clone())
    );
    // The last pack write left its id in the stamp.
    let body = objects
        .get(&gen_key())
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    let view = writer.packs.view().await.unwrap();
    let (newest, _) = view
        .searchable()
        .iter()
        .find(|(p, _)| p.index.find(&exact[0].0).is_some())
        .unwrap();
    assert!(body.starts_with(newest.id.as_bytes()));
}
