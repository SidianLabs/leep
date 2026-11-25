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

#[tokio::test]
async fn an_earlier_cleanup_keeps_a_pack_published_again() {
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
