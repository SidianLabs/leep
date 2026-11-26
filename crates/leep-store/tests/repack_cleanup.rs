//! Repack cleanups and the loose copies they move aside, and the write
//! times merges keep.

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use bytes::Bytes;
use leep_core::{BlobStore, Error, Hash};
use leep_store::{FsBlobStore, PackWrites, RepackMode, RepackOptions, RepackStep};

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

fn no_hints() -> Vec<(Hash, Option<Hash>)> {
    Vec::new()
}

fn incremental() -> RepackOptions {
    RepackOptions {
        mode: RepackMode::Incremental {
            factor: 2,
            max_packs: 8,
        },
        ..RepackOptions::default()
    }
}

/// A cleanup killed between moving a loose copy aside and putting it back
/// leaves the only live copy of a rescued object (tombstoned in the pack,
/// uploaded again) at `.repack-<name>`, where nothing reads it. A cleanup
/// that fails puts it back itself; after a kill, the restarted server's
/// first sweep does, before any background merge.
#[tokio::test]
async fn a_cleanup_killed_with_a_copy_aside_keeps_the_object_readable() {
    let dir = tempfile::tempdir().unwrap();
    let store = FsBlobStore::new(dir.path()).with_pack_writes(PackWrites::off());
    let x = store.put(body("rescued", 7)).await.unwrap();
    let opts = RepackOptions {
        hook: Some(Arc::new(|s| match s {
            RepackStep::LooseMovedAside(_) => Err(Error::Storage("killed".into())),
            _ => Ok(()),
        })),
        ..incremental()
    };
    let mut p = store.repack_publish(no_hints(), opts).await.unwrap();
    p.unlock();
    // GC deletes X; a client uploads it again (the loose copy is the only
    // live one: the packed copy is tombstoned).
    store.delete(&x).await.unwrap();
    assert!(store.get(&x).await.unwrap().is_none());
    store.put(body("rescued", 7)).await.unwrap();
    assert!(store.get(&x).await.unwrap().is_some());
    // The fence passes; the cleanup fails with X moved aside, and puts it
    // back.
    assert!(p.finish().await.is_err());
    assert!(store.get(&x).await.unwrap().is_some());
    // A kill leaves it aside.
    let hex = x.to_hex();
    let shard = dir.path().join(&hex[..2]);
    std::fs::rename(
        shard.join(&hex[2..]),
        shard.join(format!(".repack-{}", &hex[2..])),
    )
    .unwrap();

    // The server restarts: its sweep runs, then background merges.
    let restarted = FsBlobStore::new(dir.path());
    let swept = restarted
        .retire_sweep(Duration::ZERO)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(swept.asides_restored, 1);
    let merge = RepackOptions {
        mode: RepackMode::Planned { loose: false },
        packs: ids(dir.path()),
        ..RepackOptions::default()
    };
    restarted
        .repack_publish(no_hints(), merge)
        .await
        .unwrap()
        .finish()
        .await
        .unwrap();
    let readable = restarted.get(&x).await.unwrap().is_some();
    let listed = restarted
        .loose_listing()
        .unwrap()
        .iter()
        .any(|(h, _)| *h == x);
    assert!(readable, "a stored object is readable after the restart");
    assert!(listed, "and counted as loose");
}

async fn modified_of(store: &FsBlobStore, h: &Hash) -> i64 {
    let page = store.list_page(None, 10_000).await.unwrap();
    page.iter()
        .find(|m| m.hash == *h)
        .unwrap()
        .modified
        .unwrap()
}

/// A merge of two packs that both hold an object keeps the latest write
/// time of its copies: an upload's refresh of `modified` (GC's grace
/// window, ADR 0012) survives when the fresh copy is in the larger pack.
#[tokio::test]
async fn a_merge_keeps_the_latest_write_time_of_a_duplicate() {
    let dir = tempfile::tempdir().unwrap();
    let store = FsBlobStore::new(dir.path());
    let x = store.put(body("x", 0)).await.unwrap();
    store.put(body("old", 1)).await.unwrap();
    let hex = x.to_hex();
    let old = SystemTime::now() - Duration::from_secs(3 * 24 * 3600);
    std::fs::File::options()
        .write(true)
        .open(dir.path().join(&hex[..2]).join(&hex[2..]))
        .unwrap()
        .set_modified(old)
        .unwrap();
    let p = store
        .repack_publish(no_hints(), RepackOptions::default())
        .await
        .unwrap();
    let small = p.report().pack_id.clone().unwrap();
    p.finish().await.unwrap();
    let before = modified_of(&store, &x).await;

    // Uploaded again today, in a land batch: a pack of whole frames.
    let mut batch: Vec<(Hash, Bytes)> = (0..20)
        .map(|i| body("batch", i))
        .map(|b| (Hash::of_blob(&b), b))
        .collect();
    batch.push((x, body("x", 0)));
    let packed = store.put_many(batch).await.unwrap();
    assert!(packed.contains(&x), "X is in the new land pack");
    let fresh = modified_of(&store, &x).await;
    assert!(fresh > before + 2 * 24 * 3600, "the upload refreshed it");

    let land: Vec<String> = ids(dir.path())
        .into_iter()
        .filter(|i| *i != small)
        .collect();
    let merge = RepackOptions {
        mode: RepackMode::Planned { loose: false },
        packs: vec![small.clone(), land[0].clone()],
        ..RepackOptions::default()
    };
    store
        .repack_publish(no_hints(), merge)
        .await
        .unwrap()
        .finish()
        .await
        .unwrap();
    let after = modified_of(&FsBlobStore::new(dir.path()), &x).await;
    assert_eq!(after, fresh, "the merge keeps the fresh upload's time");
}
