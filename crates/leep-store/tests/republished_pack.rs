//! A pending cleanup (`PublishedPack::finish`) deletes only the packs it
//! replaced that are still retired (ADR 0107). A later merge can publish a
//! pack with the same id again (the id is the index's hash, and merges copy
//! frames in the old order), and `retire_for` then takes it out of
//! `packs/retired`: it is the live copy, and the earlier cleanup leaves it.

use bytes::Bytes;
use leep_core::{BlobStore, Hash};
use leep_store::{FsBlobStore, PackWrites, RepackMode, RepackOptions};

fn body(tag: &str, i: usize) -> Bytes {
    Bytes::from(format!("{tag} object {i}\n").repeat(40 + i))
}

fn ids(root: &std::path::Path) -> Vec<String> {
    let mut v: Vec<String> = std::fs::read_dir(root.join("packs"))
        .unwrap()
        .filter_map(|e| {
            let n = e.unwrap().file_name().to_string_lossy().into_owned();
            n.strip_suffix(".idx").map(str::to_string)
        })
        .collect();
    v.sort();
    v
}

/// A published, R1 folds it in with loose objects GC then deletes, and R2
/// (a consolidation of R1's pack) publishes A again; both cleanups wait.
struct Republished {
    dir: tempfile::TempDir,
    store: FsBlobStore,
    history: Vec<Hash>,
    a: String,
    p1: String,
    r1: leep_store::PublishedPack,
    r2: leep_store::PublishedPack,
}

async fn republished() -> Republished {
    let dir = tempfile::tempdir().unwrap();
    let store = FsBlobStore::new(dir.path()).with_pack_writes(PackWrites::off());

    // Pack A: the history.
    let mut history = Vec::new();
    for i in 0..40 {
        history.push(store.put(body("history", i)).await.unwrap());
    }
    let p = store
        .repack_publish(Vec::<(Hash, Option<Hash>)>::new(), RepackOptions::default())
        .await
        .unwrap();
    let a = p.report().pack_id.clone().unwrap();
    p.finish().await.unwrap();
    assert_eq!(ids(dir.path()), vec![a.clone()]);

    // R1: an incremental repack (as a delta run with a geometric fold)
    // packs loose objects and folds A in: P1 = A's frames, then the new.
    let mut junk = Vec::new();
    for i in 0..60 {
        junk.push(store.put(body("abandoned upload", i)).await.unwrap());
    }
    let incremental = RepackOptions {
        mode: RepackMode::Incremental {
            factor: 2,
            max_packs: 8,
        },
        ..RepackOptions::default()
    };
    let mut r1 = store
        .repack_publish(Vec::<(Hash, Option<Hash>)>::new(), incremental.clone())
        .await
        .unwrap();
    let p1 = r1.report().pack_id.clone().unwrap();
    assert_eq!(r1.replaced(), std::slice::from_ref(&a), "R1 replaced A");
    r1.unlock(); // the background task: cleanup waits for the fence

    // Inside R1's fence GC deletes every object R1 added.
    for h in &junk {
        store.delete(h).await.unwrap();
    }

    // R2: a consolidation of the delta packs (here: P1 alone, its gain the
    // tombstoned bytes). It writes A's objects, in A's order, with A's
    // frames: A's index, so A's id.
    let consolidate = RepackOptions {
        packs: vec![p1.clone()],
        loose_subset: Some(std::sync::Arc::new(Default::default())),
        mark: true,
        ..incremental
    };
    let mut r2 = store
        .repack_publish(Vec::<(Hash, Option<Hash>)>::new(), consolidate)
        .await
        .unwrap();
    let p2 = r2.report().pack_id.clone().unwrap();
    assert_eq!(p2, a, "the consolidation published A again");
    assert_eq!(r2.replaced(), std::slice::from_ref(&p1));
    r2.unlock();
    for h in &history {
        assert!(store.get(h).await.unwrap().is_some());
    }
    Republished {
        dir,
        store,
        history,
        a,
        p1,
        r1,
        r2,
    }
}

#[tokio::test]
async fn an_earlier_cleanup_keeps_a_pack_published_again() {
    let Republished {
        dir,
        store,
        history,
        a,
        p1,
        r1,
        r2,
    } = republished().await;
    // R1's fence passes: A, which it replaced, is published again, so its
    // cleanup leaves it.
    let r = r1.finish().await.unwrap();
    assert_eq!((r.old_packs_removed, r.old_packs_kept), (0, 1), "{r:?}");
    let mut both = vec![a.clone(), p1.clone()];
    both.sort();
    assert_eq!(ids(dir.path()), both);
    // R2's fence passes: its cleanup deletes P1.
    let r = r2.finish().await.unwrap();
    assert_eq!(r.old_packs_removed, 1, "{r:?}");
    assert_eq!(ids(dir.path()), vec![a.clone()]);
    for h in &history {
        assert!(
            store.get(h).await.unwrap().is_some(),
            "this process reads it"
        );
    }

    let fresh = FsBlobStore::new(dir.path());
    let mut lost = 0;
    for h in &history {
        if fresh.get(h).await.unwrap().is_none() {
            lost += 1;
        }
    }
    assert_eq!(
        lost,
        0,
        "{lost} of {} live objects were deleted",
        history.len()
    );
}

/// A pack published again and then retired again by a later repack waits
/// for that later retirement's fence: the earlier cleanup, whose own
/// retirement of it came first, leaves it to the later one.
#[tokio::test]
async fn an_earlier_cleanup_keeps_a_pack_retired_again_since() {
    let Republished {
        dir,
        store,
        history,
        a,
        r1,
        r2,
        ..
    } = republished().await;
    // A second later, R3 folds A into a new pack: A is retired again.
    std::thread::sleep(std::time::Duration::from_millis(1100));
    store.put(body("later upload", 0)).await.unwrap();
    let fold = RepackOptions {
        mode: RepackMode::Incremental {
            factor: 2,
            max_packs: 1,
        },
        fold_only: Some(vec![a.clone()]),
        ..RepackOptions::default()
    };
    let mut r3 = store
        .repack_publish(Vec::<(Hash, Option<Hash>)>::new(), fold)
        .await
        .unwrap();
    assert_eq!(r3.replaced(), std::slice::from_ref(&a));
    r3.unlock();
    let r = r1.finish().await.unwrap();
    assert_eq!((r.old_packs_removed, r.old_packs_kept), (0, 1), "{r:?}");
    assert!(ids(dir.path()).contains(&a), "A waits for R3's fence");
    r2.finish().await.unwrap();
    let r = r3.finish().await.unwrap();
    assert_eq!(r.old_packs_removed, 1, "{r:?}");
    let fresh = FsBlobStore::new(dir.path());
    for h in &history {
        assert!(fresh.get(h).await.unwrap().is_some(), "{h}");
    }
}

/// Tombstones written for a pack before it was published again belong to
/// its earlier copy: an object GC deleted then and a client uploaded again
/// is live in the new copy. Left in place, the old line would hide it there,
/// and once the pack that held the upload is swept it would not read at all.
#[tokio::test]
async fn a_republished_pack_drops_its_earlier_tombstones() {
    let dir = tempfile::tempdir().unwrap();
    let store = FsBlobStore::new(dir.path()).with_pack_writes(PackWrites::off());
    let plain = RepackOptions {
        mark: true,
        ..RepackOptions::default()
    };
    // Pack A. `y` is its first object in write order (hash order here),
    // as it is in the consolidation, which writes the smaller pack first.
    let mut objects = Vec::new();
    for i in 0..20 {
        let b = body("history", i);
        objects.push((store.put(b.clone()).await.unwrap(), b));
    }
    objects.sort_by_key(|o| o.0);
    let (y, y_body) = objects.first().cloned().unwrap();
    let p = store
        .repack_publish(Vec::<(Hash, Option<Hash>)>::new(), plain.clone())
        .await
        .unwrap();
    let a = p.report().pack_id.clone().unwrap();
    p.finish().await.unwrap();
    let a_idx = std::fs::read(dir.path().join("packs").join(format!("{a}.idx"))).unwrap();
    let y_written = leep_store::pack::PackIndex::decode(a_idx)
        .unwrap()
        .find(&y)
        .unwrap()
        .modified;

    // B = A + j; A retired, its cleanup pending.
    let j = store.put(body("junk", 0)).await.unwrap();
    let fold = RepackOptions {
        mode: RepackMode::Incremental {
            factor: 2,
            max_packs: 1,
        },
        mark: true,
        ..RepackOptions::default()
    };
    let mut rb = store
        .repack_publish(Vec::<(Hash, Option<Hash>)>::new(), fold.clone())
        .await
        .unwrap();
    let b = rb.report().pack_id.clone().unwrap();
    assert_eq!(rb.replaced(), std::slice::from_ref(&a));
    rb.unlock();

    // GC deletes y (tombstones on A and B); a client uploads it again (at
    // the same write time, so the bytes match) and a delta run packs it: D.
    store.delete(&y).await.unwrap();
    store.put(y_body.clone()).await.unwrap();
    let loose = dir.path().join(&y.to_hex()[..2]).join(&y.to_hex()[2..]);
    std::fs::File::options()
        .write(true)
        .open(&loose)
        .unwrap()
        .set_modified(std::time::UNIX_EPOCH + std::time::Duration::from_secs(y_written as u64))
        .unwrap();
    let only_loose = RepackOptions {
        fold_only: Some(Vec::new()),
        ..fold.clone()
    };
    let rd = store
        .repack_publish(Vec::<(Hash, Option<Hash>)>::new(), only_loose)
        .await
        .unwrap();
    let d = rd.report().pack_id.clone().unwrap();
    rd.finish().await.unwrap();

    // GC deletes j; a consolidation of B and D writes A again.
    store.delete(&j).await.unwrap();
    let consolidate = RepackOptions {
        packs: vec![b.clone(), d.clone()],
        loose_subset: Some(std::sync::Arc::new(Default::default())),
        fold_only: Some(Vec::new()),
        ..fold
    };
    let rc = store
        .repack_publish(Vec::<(Hash, Option<Hash>)>::new(), consolidate)
        .await
        .unwrap();
    assert_eq!(rc.report().pack_id.as_deref(), Some(a.as_str()), "A again");
    rc.finish().await.unwrap();
    rb.finish().await.unwrap();
    store
        .retire_sweep(std::time::Duration::ZERO)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ids(dir.path()), vec![a.clone()]);
    let fresh = FsBlobStore::new(dir.path());
    for (h, want) in &objects {
        assert_eq!(fresh.get(h).await.unwrap().as_ref(), Some(want), "{h}");
    }
    assert!(!fresh.contains(&j).await.unwrap());
    assert_eq!(store.get(&y).await.unwrap(), Some(y_body), "this process");
}
