//! A backup pool stays bounded when the source's repacks drop objects GC
//! deleted (ADR 0107): a consolidation rewrites the delta packs without
//! them, and the pool pack it supersedes held objects no other pool pack
//! does. Those objects are copied into a pool pack of their own, so the old
//! pack is pruned instead of kept whole, and no object is lost: an older
//! snapshot may still need what the source deleted (ADR 0026).

use bytes::Bytes;
use leep_core::{BlobStore, Hash};
use leep_store::sync::{sync_packs, PackSyncMode};
use leep_store::{FsBlobStore, FsPackFiles, PackFiles, RepackMode, RepackOptions};

fn noise(seed: u64, n: usize) -> Bytes {
    let mut x = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    let mut v = Vec::with_capacity(n);
    for _ in 0..n {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        v.push(x as u8);
    }
    Bytes::from(v)
}

/// A full repack stands in for a consolidation: both name every pack and
/// drop tombstoned objects.
async fn repack(store: &FsBlobStore) {
    let opts = RepackOptions {
        mode: RepackMode::Full,
        ..RepackOptions::default()
    };
    store
        .repack_publish(Vec::<(Hash, Option<Hash>)>::new(), opts)
        .await
        .unwrap()
        .finish()
        .await
        .unwrap();
}

fn dir_bytes(p: &std::path::Path) -> u64 {
    std::fs::read_dir(p)
        .unwrap()
        .flatten()
        .map(|e| e.metadata().unwrap().len())
        .sum()
}

/// Each "day" a segment-sized blob is written, the previous one is
/// deleted, the store repacks (dropping it) and a backup copies the packs.
/// The pool stays near the source's size, its pack count stays small, and
/// every object it ever held still reads from it.
#[tokio::test]
async fn the_pool_stays_bounded_when_repacks_drop_deleted_objects() {
    let (a, b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let src = FsBlobStore::new(a.path());
    let (sp, dp) = (FsPackFiles::new(a.path()), FsPackFiles::new(b.path()));
    let mut all: Vec<(Hash, Bytes)> = Vec::new();
    for i in 0..200 {
        let d = noise(i, 16 << 10);
        all.push((src.put(d.clone()).await.unwrap(), d));
    }
    repack(&src).await;
    sync_packs(&sp, &dp, PackSyncMode::Pool).await.unwrap();
    let mut seg: Option<Hash> = None;
    let (mut rescued, mut pruned) = (0, 0);
    for day in 1..=12u64 {
        let d = noise(10_000 + day, 4 << 10);
        let s = src.put(d.clone()).await.unwrap();
        all.push((s, d));
        if let Some(old) = seg.replace(s) {
            src.delete(&old).await.unwrap();
        }
        repack(&src).await;
        let r = sync_packs(&sp, &dp, PackSyncMode::Pool).await.unwrap();
        rescued += r.rescued_objects;
        pruned += r.pruned;
        let src_b = dir_bytes(&a.path().join("packs"));
        let pool_b = dir_bytes(&b.path().join("packs"));
        let n = dp.pack_ids().await.unwrap().len();
        assert!(
            pool_b <= src_b + 4 * (4 << 10) * day + (64 << 10),
            "day {day}: the pool holds {pool_b} B for a {src_b} B source ({n} packs)"
        );
        assert!(n <= 6, "day {day}: {n} pool packs");
    }
    assert!(
        rescued >= 11 && pruned >= 12,
        "{rescued} rescued, {pruned} pruned"
    );
    // Nothing the pool ever held is lost, the deleted segments included.
    let pool = FsBlobStore::new(b.path());
    for (h, d) in &all {
        assert_eq!(pool.get(h).await.unwrap().as_ref(), Some(d), "{h}");
    }
}

/// A rescue is crash safe: a pool left with both the rescue pack and the
/// pack it came from (killed before the delete) loses nothing and is back
/// to two packs after the next run.
#[tokio::test]
async fn a_rescue_interrupted_before_the_delete_is_finished_later() {
    let (a, b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let src = FsBlobStore::new(a.path());
    let (sp, dp) = (FsPackFiles::new(a.path()), FsPackFiles::new(b.path()));
    let mut all = Vec::new();
    for i in 0..20 {
        let d = noise(i, 8 << 10);
        all.push((src.put(d.clone()).await.unwrap(), d));
    }
    repack(&src).await;
    sync_packs(&sp, &dp, PackSyncMode::Pool).await.unwrap();
    let before = dp.pack_ids().await.unwrap();
    let copy = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(copy.path().join("packs")).unwrap();
    for id in &before {
        for ext in ["pack", "idx"] {
            let name = format!("{id}.{ext}");
            std::fs::copy(
                b.path().join("packs").join(&name),
                copy.path().join("packs").join(&name),
            )
            .unwrap();
        }
    }
    src.delete(&all[0].0).await.unwrap();
    repack(&src).await;
    let r = sync_packs(&sp, &dp, PackSyncMode::Pool).await.unwrap();
    assert_eq!(
        (r.rescued_objects, r.rescue_packs, r.pruned),
        (1, 1, 1),
        "{r:?}"
    );
    // As if killed before the delete: the old pack is back beside it.
    for id in &before {
        for ext in ["pack", "idx"] {
            let name = format!("{id}.{ext}");
            std::fs::copy(
                copy.path().join("packs").join(&name),
                b.path().join("packs").join(&name),
            )
            .unwrap();
        }
    }
    // Either the old pack goes (the rescue pack holds its object), or, in
    // the other order, the rescue pack goes first and the old one is
    // rescued again into the same pack.
    let r = sync_packs(&sp, &dp, PackSyncMode::Pool).await.unwrap();
    assert!(r.rescued_objects <= 1 && r.pruned >= 1, "{r:?}");
    assert_eq!(dp.pack_ids().await.unwrap().len(), 2);
    let pool = FsBlobStore::new(b.path());
    for (h, d) in &all {
        assert_eq!(pool.get(h).await.unwrap().as_ref(), Some(d), "{h}");
    }
}

/// An object only the superseded pack holds may be a delta deep in a chain
/// there: the rescue decodes the chain from the pack's stream, checks the
/// object against its hash and keeps it whole.
#[tokio::test]
async fn a_rescued_delta_is_decoded_through_its_chain() {
    let (a, b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let src = FsBlobStore::new(a.path());
    let (sp, dp) = (FsPackFiles::new(a.path()), FsPackFiles::new(b.path()));
    let mut text = String::new();
    let mut all: Vec<(Hash, Bytes)> = Vec::new();
    for v in 0..10 {
        for l in 0..200 {
            text.push_str(&format!("version {v} line {l}: {}\n", l * 31 + v));
        }
        let d = Bytes::from(text.clone());
        all.push((src.put(d.clone()).await.unwrap(), d));
    }
    let hints: Vec<(Hash, Option<Hash>)> = all
        .iter()
        .enumerate()
        .map(|(i, (h, _))| (*h, i.checked_sub(1).map(|j| all[j].0)))
        .collect();
    let full = |hints: Vec<(Hash, Option<Hash>)>| {
        let src = src.clone();
        async move {
            src.repack_publish(hints, RepackOptions::default())
                .await
                .unwrap()
                .finish()
                .await
                .unwrap()
        }
    };
    let r = full(hints.clone()).await;
    assert!(r.deltas >= 8, "{r:?}");
    sync_packs(&sp, &dp, PackSyncMode::Pool).await.unwrap();
    src.delete(&all[6].0).await.unwrap();
    full(hints).await;
    let r = sync_packs(&sp, &dp, PackSyncMode::Pool).await.unwrap();
    assert_eq!((r.rescued_objects, r.pruned), (1, 1), "{r:?}");
    let pool = FsBlobStore::new(b.path());
    for (h, d) in &all {
        assert_eq!(pool.get(h).await.unwrap().as_ref(), Some(d), "{h}");
    }
}
