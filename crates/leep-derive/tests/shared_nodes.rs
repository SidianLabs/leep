//! A lookup in a wide directory allocates about what its answer needs, not
//! a copy of the directory: cached tree nodes are shared, not cloned per
//! read. Its own test binary, for the counting allocator.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::collections::BTreeMap;
use std::sync::Arc;

use bytes::Bytes;
use leep_core::memory::{MemoryBlobStore, MemoryMetaStore};
use leep_core::{Changeset, FileChange, FileKind, RepoPath};
use leep_derive::Repo;

struct Counting;

thread_local! {
    static BYTES: Cell<usize> = const { Cell::new(0) };
}

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let _ = BYTES.try_with(|b| b.set(b.get() + layout.size()));
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOC: Counting = Counting;

/// Bytes this thread allocates while `f` runs.
async fn allocated<T>(f: impl std::future::Future<Output = T>) -> usize {
    let start = BYTES.with(Cell::get);
    f.await;
    BYTES.with(Cell::get) - start
}

#[tokio::test(flavor = "current_thread")]
async fn a_lookup_in_a_wide_directory_does_not_copy_it() {
    let repo = Repo::new(
        Arc::new(MemoryBlobStore::new()),
        Arc::new(MemoryMetaStore::new()),
    );
    let data = Bytes::from_static(b"state\n");
    let content = repo.blobs().put(data.clone()).await.unwrap();
    let change = FileChange::Modify {
        content,
        kind: FileKind::Regular,
        size: data.len() as u64,
    };
    let changes = (0..1000)
        .map(|i| {
            (
                RepoPath::new(format!("agents/a{i}")).unwrap(),
                change.clone(),
            )
        })
        .collect();
    let id = repo
        .create_changeset(Changeset {
            parents: Vec::new(),
            author: "t".into(),
            timestamp: 1,
            message: "agents".into(),
            changes,
            extra: BTreeMap::new(),
        })
        .await
        .unwrap();
    let path = RepoPath::new("agents/a500").unwrap();
    // Warm: the root and the directory node are cached after this.
    assert!(repo.lookup_file(id, &path).await.unwrap().is_some());

    let root = repo.root_tree(id).await.unwrap();
    let dir = repo.read_tree(root).await.unwrap().entries["agents"].clone();
    let leep_core::TreeEntry::Dir { tree } = dir else {
        panic!("agents is a directory");
    };
    // What a copy of the directory costs, for scale.
    let copy = allocated(async { repo.read_tree(tree).await.unwrap() }).await;
    assert!(
        copy > 50_000,
        "a copy of 1,000 entries allocates {copy} bytes"
    );

    let lookups = allocated(async {
        for _ in 0..100 {
            assert!(repo.lookup_file(id, &path).await.unwrap().is_some());
        }
    })
    .await;
    assert!(
        lookups / 100 < copy / 20,
        "a lookup allocates {} bytes, a copy of the directory {copy}",
        lookups / 100
    );
}
