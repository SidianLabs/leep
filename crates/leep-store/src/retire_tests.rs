//! Durable retirement of replaced packs (ADR 0080): a merge's packs are
//! never searched or merged again once it is published, by any process or
//! after a restart, readers with older views read through the fence, and
//! a sweep deletes them after it.

use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use leep_core::memory::check_blob_store_contract;
use leep_core::{BlobStore, Error, Hash};
use object_store::memory::InMemory;
use object_store::{ObjectStore, ObjectStoreExt};

use crate::objpacks::RETIRED_KEY;
use crate::pack::PackIndex;
use crate::packset::PACKS_DIR;
use crate::retire::RETIRED;
use crate::{FsBlobStore, ObjectBlobStore, RepackMode, RepackOptions, RepackStep};

const LONG: Duration = Duration::from_secs(3600);

/// A batch large enough to be written as one pack.
fn batch(tag: &str) -> Vec<(Hash, Bytes)> {
    (0..24)
        .map(|i| {
            let data = Bytes::from(format!("{tag} object {i} ").repeat(i % 5 + 1));
            (Hash::of_blob(&data), data)
        })
        .collect()
}

/// Land packs `tags`, one each, through `store`; every object written.
async fn land_packs(store: &dyn BlobStore, tags: &[String]) -> BTreeMap<Hash, Bytes> {
    let mut all = BTreeMap::new();
    for tag in tags {
        let items = batch(tag);
        let packed = store.put_many(items.clone()).await.unwrap();
        assert_eq!(packed.len(), items.len(), "{tag}: written as one pack");
        all.extend(items);
    }
    all
}

fn tags(round: usize, n: usize) -> Vec<String> {
    (0..n).map(|i| format!("round {round} pack {i}")).collect()
}

fn planned(packs: Vec<String>) -> RepackOptions {
    RepackOptions {
        level: 3,
        mode: RepackMode::Planned { loose: false },
        packs,
        threads: 1,
        ..RepackOptions::default()
    }
}

/// Merge every searchable pack of `store` (if two or more), then let the
/// cleanup go, as a process stopping inside the fence does.
async fn merge_and_stop(store: &FsBlobStore) -> Vec<String> {
    let ids: Vec<String> = store
        .pack_sizes()
        .unwrap()
        .into_iter()
        .map(|p| p.id)
        .collect();
    if ids.len() < 2 {
        return Vec::new();
    }
    let mut p = store
        .repack_publish(Vec::new(), planned(ids))
        .await
        .unwrap();
    p.unlock();
    let replaced = p.replaced().to_vec();
    drop(p);
    replaced
}

/// Each published pack's index, read from disk.
fn indexes(root: &Path) -> BTreeMap<String, PackIndex> {
    let dir = root.join(PACKS_DIR);
    let mut out = BTreeMap::new();
    for e in std::fs::read_dir(&dir).unwrap() {
        let name = e.unwrap().file_name().to_string_lossy().into_owned();
        if let Some(id) = name.strip_suffix(".idx") {
            let bytes = std::fs::read(dir.join(&name)).unwrap();
            out.insert(id.to_string(), PackIndex::decode(bytes).unwrap());
        }
    }
    out
}

/// Copies of each object across `packs` (all published ones if `None`).
fn copies(root: &Path, packs: Option<&[String]>) -> HashMap<Hash, usize> {
    let mut out = HashMap::new();
    for (id, idx) in indexes(root) {
        if packs.is_some_and(|p| !p.contains(&id)) {
            continue;
        }
        for r in 0..idx.len() {
            *out.entry(idx.hash_at(r)).or_default() += 1;
        }
    }
    out
}

/// Bytes of every file in `packs/`.
fn pack_dir_bytes(root: &Path) -> u64 {
    std::fs::read_dir(root.join(PACKS_DIR))
        .unwrap()
        .map(|e| e.unwrap().metadata().unwrap().len())
        .sum()
}

fn junk(root: &Path) -> Vec<String> {
    let dir = root.join(PACKS_DIR);
    let ids: Vec<String> = indexes(root).into_keys().collect();
    std::fs::read_dir(&dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|n| {
            n.starts_with(".tmp-")
                || n.strip_suffix(".pack")
                    .is_some_and(|id| !ids.iter().any(|i| i == id))
        })
        .collect()
}

async fn assert_reads(store: &dyn BlobStore, objects: &BTreeMap<Hash, Bytes>, what: &str) {
    for (h, b) in objects {
        assert_eq!(store.get(h).await.unwrap().as_ref(), Some(b), "{what}: {h}");
        assert!(store.contains(h).await.unwrap(), "{what}: {h}");
    }
}

fn searchable(store: &FsBlobStore) -> Vec<String> {
    store
        .pack_sizes()
        .unwrap()
        .into_iter()
        .map(|p| p.id)
        .collect()
}

#[tokio::test]
async fn a_merge_retires_its_packs_for_every_process_and_restart() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let a = FsBlobStore::new(root);
    let objects = land_packs(&a, &tags(0, 4)).await;
    let land: Vec<String> = indexes(root).into_keys().collect();
    assert_eq!(land.len(), 4);

    // Another process reads through a view that lists the land packs.
    let reader = FsBlobStore::with_decoded_cache(root, 0);
    assert_reads(&reader, &objects, "before").await;
    let old_view = reader.packs.view().unwrap();
    assert_eq!(old_view.searchable().len(), 4);

    let replaced = merge_and_stop(&a).await;
    assert_eq!(replaced.len(), 4);
    let retired: Vec<String> = a
        .retired_packs()
        .unwrap()
        .into_iter()
        .map(|(id, _)| id)
        .collect();
    assert_eq!(retired, land, "every merged pack is retired, durably");
    let merged: Vec<String> = searchable(&a);
    assert_eq!(merged.len(), 1);

    // The reader still reads every object through its old view, and its
    // next miss sees the record changed and stops searching the land packs.
    assert_reads(&reader, &objects, "old view, inside the fence").await;
    assert!(reader
        .get(&Hash::of_blob(b"absent"))
        .await
        .unwrap()
        .is_none());
    let v = reader.packs.view().unwrap();
    assert_eq!(
        v.searchable()
            .iter()
            .map(|(p, _)| p.id.clone())
            .collect::<Vec<_>>(),
        merged,
        "a miss rescans when the retired record changes"
    );
    // A restarted process plans from the merged pack only.
    let restarted = FsBlobStore::new(root);
    assert_eq!(searchable(&restarted), merged);
    assert!(
        merge_and_stop(&restarted).await.is_empty(),
        "nothing to merge again"
    );

    // Inside the fence the sweep deletes nothing.
    let r = restarted.retire_sweep(LONG).await.unwrap().unwrap();
    assert_eq!((r.retired_removed, r.retired_waiting), (0, 4), "{r:?}");
    assert_eq!(indexes(root).len(), 5);
    // Past it, it deletes them; readers holding them open read on.
    let r = restarted
        .retire_sweep(Duration::ZERO)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(r.retired_removed, 4, "{r:?}");
    assert!(r.retired_bytes_removed > 0, "{r:?}");
    assert_eq!(indexes(root).into_keys().collect::<Vec<_>>(), merged);
    assert!(
        !root.join(PACKS_DIR).join(RETIRED).exists(),
        "lines of gone packs are dropped"
    );
    for (h, b) in &objects {
        let (p, e) = old_view.locate(h).unwrap();
        assert_eq!(
            &reader.packs.decode(p, &e).unwrap(),
            b,
            "an open pack reads on"
        );
    }
    assert_reads(&reader, &objects, "after the sweep").await;
    assert_reads(&FsBlobStore::new(root), &objects, "a new process").await;
    assert!(copies(root, None).values().all(|&n| n == 1));
    check_blob_store_contract(&restarted).await;
}

/// Kill a background-style merge at each step from its publish on, or
/// stop the process inside the fence, then restart, merge again and let
/// the fence pass: one copy of each object remains, and nothing else.
#[tokio::test]
async fn a_merge_stopped_anywhere_after_publishing_leaves_one_copy_after_a_restart() {
    let kills = [
        Some(RepackStep::Published),
        Some(RepackStep::Retired),
        Some(RepackStep::OldPacksRemoved),
        Some(RepackStep::TombstonesRewritten),
        None,
    ];
    for kill_at in kills {
        let what = format!("killed at {kill_at:?}");
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let store = FsBlobStore::new(root);
        let mut objects = land_packs(&store, &tags(0, 4)).await;
        let ids = searchable(&store);
        let mut opts = planned(ids);
        opts.hook = Some(Arc::new(move |s| match kill_at {
            Some(k) if k == s => Err(Error::Storage(format!("killed at {s:?}"))),
            _ => Ok(()),
        }));
        match store.repack_publish(Vec::new(), opts).await {
            Ok(mut p) if kill_at.is_some() => {
                // Killed in the cleanup, a fence later.
                p.unlock();
                p.relock().unwrap();
                assert!(p.finish().await.is_err(), "{what}");
            }
            // Stopped inside the fence: the cleanup never runs.
            Ok(p) => drop(p),
            Err(e) => assert!(kill_at.is_some(), "{what}: {e}"),
        }
        assert_eq!(junk(root), Vec::<String>::new(), "{what}");

        // Three restarts inside the fence, each landing more and merging.
        for round in 1..=3 {
            let store = FsBlobStore::new(root);
            assert_reads(&store, &objects, &what).await;
            let found = searchable(&store);
            let dup = copies(root, Some(&found));
            assert!(
                dup.values().all(|&n| n == 1) || kill_at == Some(RepackStep::Published),
                "{what}, round {round}: searched packs hold an object twice"
            );
            objects.extend(land_packs(&store, &tags(round, 2)).await);
            merge_and_stop(&store).await;
        }
        // The fence passes.
        let store = FsBlobStore::new(root);
        let r = store.retire_sweep(Duration::ZERO).await.unwrap().unwrap();
        assert_reads(&store, &objects, &format!("{what}, swept")).await;
        let packs = indexes(root);
        assert_eq!(packs.len(), 1, "{what}: {r:?}");
        let c = copies(root, None);
        assert_eq!(c.len(), objects.len(), "{what}");
        assert!(c.values().all(|&n| n == 1), "{what}: one copy of each");
        assert_eq!(junk(root), Vec::<String>::new(), "{what}");
        let pack_bytes: u64 = packs.values().map(|i| i.pack_len()).sum();
        let idx_bytes: u64 = indexes(root)
            .keys()
            .map(|id| {
                std::fs::metadata(root.join(PACKS_DIR).join(format!("{id}.idx")))
                    .unwrap()
                    .len()
            })
            .sum();
        // Bounded: the pack, its index and the empty lock files.
        assert_eq!(pack_dir_bytes(root), pack_bytes + idx_bytes, "{what}");
        assert!(!root.join(PACKS_DIR).join(RETIRED).exists(), "{what}");
    }
}

/// Restarts inside the fence used to make every replaced pack searched and
/// merged again, so each restart copied the merged objects once more. Now
/// a run with restarts stores exactly what the same run without them does.
#[tokio::test]
async fn restarts_inside_the_fence_do_not_grow_the_pack_dir() {
    async fn run(restarts: bool) -> (usize, usize) {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let mut store = FsBlobStore::new(root);
        let mut pending = Vec::new();
        let mut objects = 0;
        for round in 0..6 {
            if restarts {
                // The cleanups waiting for their fence are lost.
                pending.clear();
                store = FsBlobStore::new(root);
            }
            objects += land_packs(&store, &tags(round, 3)).await.len();
            // As the background repack's tiers do: merge the small packs
            // (this round's land packs), leave the merged ones.
            let ids: Vec<String> = store
                .pack_sizes()
                .unwrap()
                .into_iter()
                .filter(|p| p.objects <= 24)
                .map(|p| p.id)
                .collect();
            let mut p = store
                .repack_publish(Vec::new(), planned(ids))
                .await
                .unwrap();
            p.unlock();
            pending.push(p);
        }
        let entries: usize = copies(root, None).values().sum();
        (entries, objects)
    }
    let (with, objects) = run(true).await;
    let (without, _) = run(false).await;
    assert_eq!(with, without, "restarts add no copies");
    // The land packs waiting out their fence and the merged packs: each
    // object twice, whatever the restarts.
    assert_eq!(with, 2 * objects);
}

#[tokio::test]
async fn a_sweep_removes_temp_files_and_packs_without_an_index_and_waits_for_the_lock() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let store = FsBlobStore::new(root);
    let objects = land_packs(&store, &tags(0, 2)).await;
    let packs = root.join(PACKS_DIR);
    // What an ENOSPC or a crash between the two renames leaves.
    std::fs::write(packs.join(".tmp-1-abc-0.pack"), b"partial").unwrap();
    std::fs::write(packs.join(format!("{}.pack", "ab".repeat(32))), b"LEEPPAK1").unwrap();
    // Another repack holds the store: the sweep leaves everything.
    let lock = crate::repack::take_repack_lock(&packs).unwrap();
    assert!(store.retire_sweep(Duration::ZERO).await.unwrap().is_none());
    assert_eq!(junk(root).len(), 2);
    drop(lock);
    let r = store.retire_sweep(Duration::ZERO).await.unwrap().unwrap();
    assert_eq!(r.garbage_removed, 2, "{r:?}");
    assert!(junk(root).is_empty());
    assert_reads(&store, &objects, "after the sweep").await;
    // A store without packs is left without a `packs/`.
    let empty = tempfile::tempdir().unwrap();
    let s = FsBlobStore::new(empty.path());
    assert_eq!(
        s.retire_sweep(LONG).await.unwrap(),
        Some(Default::default())
    );
    assert!(!empty.path().join(PACKS_DIR).exists());
}

#[tokio::test]
async fn an_object_store_merge_retires_its_packs_and_the_sweep_deletes_them_later() {
    let bucket: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let a = ObjectBlobStore::new(bucket.clone());
    let objects = land_packs(&a, &tags(0, 4)).await;
    let reader = ObjectBlobStore::with_options(bucket.clone(), 0, LONG);
    assert_reads(&reader, &objects, "before").await;
    let land: Vec<String> = a
        .pack_sizes()
        .await
        .unwrap()
        .into_iter()
        .map(|p| p.id)
        .collect();
    assert_eq!(land.len(), 4);

    let mut p = a
        .repack_publish(Vec::new(), planned(land.clone()))
        .await
        .unwrap();
    p.unlock().await;
    drop(p);
    let retired: Vec<String> = a
        .retired_packs()
        .await
        .unwrap()
        .into_iter()
        .map(|(id, _)| id)
        .collect();
    let mut sorted = land.clone();
    sorted.sort();
    assert_eq!(retired, sorted);
    assert!(bucket.head(&RETIRED_KEY.into()).await.is_ok());

    // A restarted server plans from the merged pack only, once it lists.
    let restarted = ObjectBlobStore::new(bucket.clone());
    let sizes = restarted.pack_sizes().await.unwrap();
    assert_eq!(sizes.len(), 1, "{sizes:?}");
    // The old reader reads on, and its next miss (the stamp changed)
    // stops searching the retired packs, once its last read of the stamp
    // is too old to share (ADR 0070).
    assert_reads(&reader, &objects, "old view").await;
    tokio::time::sleep(crate::objpacks::GEN_WINDOW * 2).await;
    assert!(reader
        .get(&Hash::of_blob(b"absent"))
        .await
        .unwrap()
        .is_none());
    assert_eq!(reader.pack_sizes().await.unwrap().len(), 1);

    assert!(!restarted.retired_due(LONG).await.unwrap());
    let r = restarted.retire_sweep(LONG).await.unwrap().unwrap();
    assert_eq!((r.retired_removed, r.retired_waiting), (0, 4), "{r:?}");
    assert!(restarted.retired_due(Duration::ZERO).await.unwrap());
    let r = restarted
        .retire_sweep(Duration::ZERO)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(r.retired_removed, 4, "{r:?}");
    assert!(
        restarted.retired_packs().await.unwrap().is_empty(),
        "record emptied"
    );
    let (ids, files) = crate::objpacks::list_packs_dir(&*bucket).await.unwrap();
    assert_eq!(ids.len(), 1);
    assert_eq!(files.len(), 1);
    assert_reads(&reader, &objects, "old reader after the sweep").await;
    assert_reads(&ObjectBlobStore::new(bucket.clone()), &objects, "new").await;
    check_blob_store_contract(&restarted).await;
}

fn loose_batch(tag: &str, n: usize) -> Vec<Bytes> {
    (0..n)
        .map(|i| Bytes::from(format!("{tag} loose object {i}\n").repeat(i % 3 + 1)))
        .collect()
}

fn full() -> RepackOptions {
    RepackOptions {
        threads: 1,
        ..RepackOptions::default()
    }
}

/// A merge can reproduce a retired pack byte for byte: A is published,
/// A plus newer objects become N (A retired), GC deletes the newer
/// objects, and a full repack inside A's fence writes exactly A again.
/// The republished A must not stay retired, or it would hide the only
/// copy of its objects (N is retired now) and a sweep would delete it.
#[tokio::test]
async fn a_merge_that_republishes_a_retired_pack_unretires_it() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let store = FsBlobStore::new(root);
    let mut kept = BTreeMap::new();
    for b in loose_batch("first", 20) {
        kept.insert(store.put(b.clone()).await.unwrap(), b);
    }
    let mut p = store.repack_publish(Vec::new(), full()).await.unwrap();
    let a = p.report().pack_id.clone().unwrap();
    p.unlock();
    p.finish().await.unwrap();
    let mut newer = Vec::new();
    for b in loose_batch("second", 20) {
        newer.push(store.put(b).await.unwrap());
    }
    // A and the new loose objects into N; A retired, inside its fence.
    let mut p = store.repack_publish(Vec::new(), full()).await.unwrap();
    let n = p.report().pack_id.clone().unwrap();
    p.unlock();
    drop(p);
    assert_ne!(n, a);
    assert!(store
        .retired_packs()
        .unwrap()
        .iter()
        .any(|(id, _)| *id == a));
    for h in &newer {
        store.delete(h).await.unwrap();
    }
    // Only A's objects are live: a full repack writes A's bytes again.
    let mut p = store.repack_publish(Vec::new(), full()).await.unwrap();
    assert_eq!(
        p.report().pack_id.as_deref(),
        Some(a.as_str()),
        "{:?}",
        p.report()
    );
    p.unlock();
    drop(p);
    let retired: Vec<String> = store
        .retired_packs()
        .unwrap()
        .into_iter()
        .map(|(id, _)| id)
        .collect();
    assert_eq!(
        retired,
        vec![n.clone()],
        "the republished pack is not retired"
    );
    assert_reads(&FsBlobStore::new(root), &kept, "a new process").await;
    assert_reads(&store, &kept, "this process").await;
    let r = store.retire_sweep(Duration::ZERO).await.unwrap().unwrap();
    assert_eq!(r.retired_removed, 1, "{r:?}");
    assert_eq!(indexes(root).into_keys().collect::<Vec<_>>(), vec![a]);
    assert_reads(&FsBlobStore::new(root), &kept, "after the sweep").await;
}

/// The same on an object store, where the republished pack's upload is
/// skipped (its id is listed already).
#[tokio::test]
async fn a_bucket_merge_that_republishes_a_retired_pack_unretires_it() {
    let bucket: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let store = ObjectBlobStore::new(bucket.clone());
    let mut kept = BTreeMap::new();
    for b in loose_batch("first", 20) {
        kept.insert(store.put(b.clone()).await.unwrap(), b);
    }
    let p = store.repack_publish(Vec::new(), full()).await.unwrap();
    let a = p.report().pack_id.clone().unwrap();
    p.finish().await.unwrap();
    let mut newer = Vec::new();
    for b in loose_batch("second", 20) {
        newer.push(store.put(b).await.unwrap());
    }
    let mut p = store.repack_publish(Vec::new(), full()).await.unwrap();
    let n = p.report().pack_id.clone().unwrap();
    p.unlock().await;
    drop(p);
    assert_ne!(n, a);
    assert!(store
        .retired_packs()
        .await
        .unwrap()
        .iter()
        .any(|(id, _)| *id == a));
    for h in &newer {
        store.delete(h).await.unwrap();
    }
    let mut p = store.repack_publish(Vec::new(), full()).await.unwrap();
    assert_eq!(
        p.report().pack_id.as_deref(),
        Some(a.as_str()),
        "{:?}",
        p.report()
    );
    p.unlock().await;
    drop(p);
    let retired: Vec<String> = store
        .retired_packs()
        .await
        .unwrap()
        .into_iter()
        .map(|(id, _)| id)
        .collect();
    assert_eq!(
        retired,
        vec![n.clone()],
        "the republished pack is not retired"
    );
    assert_reads(
        &ObjectBlobStore::new(bucket.clone()),
        &kept,
        "a new process",
    )
    .await;
    let r = store.retire_sweep(Duration::ZERO).await.unwrap().unwrap();
    assert_eq!(r.retired_removed, 1, "{r:?}");
    let (ids, _) = crate::objpacks::list_packs_dir(&*bucket).await.unwrap();
    assert_eq!(ids, vec![a]);
    assert_reads(
        &ObjectBlobStore::new(bucket.clone()),
        &kept,
        "after the sweep",
    )
    .await;
}

/// A bucket whose `packs/.retired` another writer rewrites between a
/// repack's read of it and its PUT (one that lost the lease and has not
/// noticed): the conditional PUT fails, the repack applies its change
/// again, and neither writer's lines are lost.
#[derive(Debug)]
struct Interleaved {
    inner: InMemory,
    foreign: std::sync::Mutex<Option<String>>,
}

impl std::fmt::Display for Interleaved {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Interleaved({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for Interleaved {
    async fn put_opts(
        &self,
        location: &object_store::path::Path,
        payload: object_store::PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        if location.as_ref() == RETIRED_KEY {
            let line = self.foreign.lock().unwrap().take();
            if let Some(line) = line {
                // The other writer gets in first, unconditionally.
                let mut body = match self.inner.get(location).await {
                    Ok(r) => r.bytes().await?.to_vec(),
                    Err(_) => Vec::new(),
                };
                body.extend_from_slice(line.as_bytes());
                self.inner.put(location, body.into()).await?;
            }
        }
        self.inner.put_opts(location, payload, opts).await
    }
    async fn put_multipart_opts(
        &self,
        location: &object_store::path::Path,
        opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }
    async fn get_opts(
        &self,
        location: &object_store::path::Path,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        self.inner.get_opts(location, options).await
    }
    fn delete_stream(
        &self,
        locations: futures::stream::BoxStream<
            'static,
            object_store::Result<object_store::path::Path>,
        >,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::path::Path>> {
        self.inner.delete_stream(locations)
    }
    fn list(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        self.inner.list(prefix)
    }
    async fn list_with_delimiter(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
    async fn copy_opts(
        &self,
        from: &object_store::path::Path,
        to: &object_store::path::Path,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

#[tokio::test]
async fn a_rewrite_of_the_bucket_record_keeps_an_interleaved_writers_lines() {
    let foreign_id = "ef".repeat(32);
    let wrapped = Arc::new(Interleaved {
        inner: InMemory::new(),
        foreign: std::sync::Mutex::new(None),
    });
    let bucket: Arc<dyn ObjectStore> = wrapped.clone();
    let store = ObjectBlobStore::new(bucket.clone());
    land_packs(&store, &tags(0, 4)).await;
    let land: Vec<String> = store
        .pack_sizes()
        .await
        .unwrap()
        .into_iter()
        .map(|p| p.id)
        .collect();
    *wrapped.foreign.lock().unwrap() = Some(crate::retire::retired_line(&foreign_id, 1));
    let mut p = store
        .repack_publish(Vec::new(), planned(land.clone()))
        .await
        .unwrap();
    p.unlock().await;
    drop(p);
    assert!(
        wrapped.foreign.lock().unwrap().is_none(),
        "the other writer ran"
    );
    let retired: Vec<String> = store
        .retired_packs()
        .await
        .unwrap()
        .into_iter()
        .map(|(id, _)| id)
        .collect();
    assert!(retired.contains(&foreign_id), "{retired:?}");
    for id in &land {
        assert!(retired.contains(id), "{id} retired: {retired:?}");
    }
}
