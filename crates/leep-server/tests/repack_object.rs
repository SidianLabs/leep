//! Garbage collection over a packed object store blob store (ADR 0012,
//! ADR 0040), as `tests/repack.rs` checks it for the filesystem store: GC
//! deletes packed orphans through tombstone objects, an upload rescues a
//! condemned packed blob, and a repack between condemn and collect does not
//! rescue garbage. Uses the local-filesystem `object_store` backend.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use leep_core::memory::MemoryMetaStore;
use leep_core::{BlobStore, Changeset, ChangesetId, FileChange, FileKind, Hash, RepoPath};
use leep_derive::gc::{self, Clock, GcOptions};
use leep_derive::Repo;
use leep_store::{ObjectBlobStore, RepackOptions, RepackReport};

/// Starts a minute ahead, so blobs written just now are older than the run,
/// and moves only when GC sleeps through its fence.
struct Stepping(Mutex<i64>);

impl Stepping {
    fn new() -> Self {
        Stepping(Mutex::new(gc::SystemClock.now() + 60))
    }
}

#[async_trait]
impl Clock for Stepping {
    fn now(&self) -> i64 {
        *self.0.lock().unwrap()
    }
    async fn sleep(&self, d: Duration) {
        *self.0.lock().unwrap() += d.as_secs() as i64;
    }
}

fn opts() -> GcOptions {
    GcOptions {
        grace: Duration::ZERO,
        fence: Duration::from_secs(600),
        unsafe_no_fence: false,
        dry_run: false,
        backup_fence: None,
    }
}

async fn commit(r: &Repo, parents: &[ChangesetId], files: &[(&str, &str)]) -> ChangesetId {
    let mut changes = BTreeMap::new();
    for (p, t) in files {
        let content = r.put_blob(Bytes::from(t.to_string())).await.unwrap();
        changes.insert(
            RepoPath::new(*p).unwrap(),
            FileChange::Modify {
                content,
                kind: FileKind::Regular,
                size: t.len() as u64,
            },
        );
    }
    let cs = Changeset {
        parents: parents.to_vec(),
        author: "t".into(),
        timestamp: 1,
        message: format!("{files:?}"),
        changes,
        extra: BTreeMap::new(),
    };
    let id = r.meta().put_changeset(&cs).await.unwrap();
    r.root_tree(id).await.unwrap();
    id
}

async fn repack(store: &ObjectBlobStore, repo: &Repo) -> RepackReport {
    let order = leep_derive::pack_order::pack_order(repo).await.unwrap();
    store
        .repack_publish(order.objects, RepackOptions::default())
        .await
        .unwrap()
        .finish()
        .await
        .unwrap()
}

/// Another process's view of the same bucket.
fn reopen(dir: &Path) -> ObjectBlobStore {
    ObjectBlobStore::local(dir).unwrap()
}

/// Object keys under `prefix`, as paths relative to the bucket.
fn keys(dir: &Path, prefix: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut stack = vec![dir.join(prefix)];
    while let Some(d) = stack.pop() {
        for e in std::fs::read_dir(&d).into_iter().flatten().flatten() {
            if e.path().is_dir() {
                stack.push(e.path());
            } else {
                let rel = e.path().strip_prefix(dir).unwrap().display().to_string();
                out.push(rel);
            }
        }
    }
    out.sort();
    out
}

/// A history of a few versions of two files, packed, plus `orphans`
/// uploaded and never committed.
async fn packed_site(dir: &Path, orphans: &[&str]) -> (ObjectBlobStore, Repo, Vec<Hash>) {
    let store = ObjectBlobStore::local(dir).unwrap();
    let repo = Repo::new(Arc::new(store.clone()), Arc::new(MemoryMetaStore::new()));
    let mut parent = commit(&repo, &[], &[("a.txt", "a0\n"), ("dir/b.txt", "b0\n")]).await;
    for i in 1..6 {
        let a = format!("a0\n{}", "more a\n".repeat(i));
        parent = commit(&repo, &[parent], &[("a.txt", &a)]).await;
    }
    let mut hashes = Vec::new();
    for o in orphans {
        hashes.push(repo.put_blob(Bytes::from(o.to_string())).await.unwrap());
    }
    let r = repack(&store, &repo).await;
    assert!(r.deltas > 0, "{r:?}");
    assert!(keys(dir, "blobs").is_empty(), "every loose copy went");
    (store, repo, hashes)
}

async fn all_referenced_read(store: &ObjectBlobStore, repo: &Repo) {
    let order = leep_derive::pack_order::pack_order(repo).await.unwrap();
    assert!(!order.objects.is_empty());
    for (h, _) in order.objects {
        assert!(store.get(&h).await.unwrap().is_some(), "{h} lost");
    }
}

#[tokio::test]
async fn gc_collects_a_packed_orphan_through_a_tombstone_object() {
    let dir = tempfile::tempdir().unwrap();
    let (store, repo, orphans) = packed_site(dir.path(), &["orphan one", "orphan two"]).await;
    let report = gc::run(&repo, &opts(), &Stepping::new()).await.unwrap();
    assert_eq!(report.condemn.condemned, 2, "{report:?}");
    assert_eq!(report.collect.deleted, 2, "{report:?}");
    for h in &orphans {
        assert!(store.get(h).await.unwrap().is_none());
        assert!(!store.contains(h).await.unwrap());
        assert!(!reopen(dir.path()).contains(h).await.unwrap());
    }
    assert_eq!(keys(dir.path(), "packs/tombstones").len(), 2);
    all_referenced_read(&store, &repo).await;
    let again = gc::run(&repo, &opts(), &Stepping::new()).await.unwrap();
    assert_eq!((again.condemn.condemned, again.collect.deleted), (0, 0));

    let r = repack(&store, &repo).await;
    assert_eq!(r.dropped_tombstoned, 2, "{r:?}");
    assert!(keys(dir.path(), "packs/tombstones").is_empty());
    all_referenced_read(&reopen(dir.path()), &repo).await;
    for h in &orphans {
        assert!(store.get(h).await.unwrap().is_none());
    }
}

#[tokio::test]
async fn an_upload_rescues_a_condemned_packed_blob() {
    let dir = tempfile::tempdir().unwrap();
    let (store, repo, orphans) = packed_site(dir.path(), &["wanted after all"]).await;
    let clock = Stepping::new();
    assert_eq!(
        gc::condemn(&repo, &opts(), &clock).await.unwrap().condemned,
        1
    );
    assert_eq!(repo.missing_blobs(&orphans).await.unwrap(), orphans);
    repo.put_blob(Bytes::from("wanted after all"))
        .await
        .unwrap();
    clock.sleep(Duration::from_secs(601)).await;
    let col = gc::collect(&repo, &opts(), &clock).await.unwrap();
    assert_eq!(col.deleted, 0, "{col:?}");
    assert!(store.get(&orphans[0]).await.unwrap().is_some());
    store.delete(&orphans[0]).await.unwrap();
    assert!(store.get(&orphans[0]).await.unwrap().is_none());
    repo.put_blob(Bytes::from("wanted after all"))
        .await
        .unwrap();
    assert!(store.get(&orphans[0]).await.unwrap().is_some());
    let r = repack(&store, &repo).await;
    assert_eq!(
        r.dropped_tombstoned, 0,
        "the live loose copy is packed: {r:?}"
    );
    assert!(reopen(dir.path()).get(&orphans[0]).await.unwrap().is_some());
}

#[tokio::test]
async fn a_repack_between_condemn_and_collect_does_not_rescue_garbage() {
    let dir = tempfile::tempdir().unwrap();
    let (store, repo, orphans) = packed_site(dir.path(), &["garbage"]).await;
    let clock = Stepping::new();
    assert_eq!(
        gc::condemn(&repo, &opts(), &clock).await.unwrap().condemned,
        1
    );
    repack(&store, &repo).await;
    clock.sleep(Duration::from_secs(601)).await;
    let col = gc::collect(&repo, &opts(), &clock).await.unwrap();
    assert_eq!((col.deleted, col.rescued_by_upload), (1, 0), "{col:?}");
    assert!(store.get(&orphans[0]).await.unwrap().is_none());
    all_referenced_read(&store, &repo).await;
}
