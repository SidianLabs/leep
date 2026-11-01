//! Two servers on one filesystem store (ADR 0070): a content or tree node
//! another server just packed is read through a stale pack view without a
//! false "missing", because reads of what a stored changeset names retry
//! exactly after a miss.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use bytes::Bytes;
use leep_core::memory::MemoryMetaStore;
use leep_core::{Changeset, ChangesetId, FileChange, FileKind, Hash, RepoPath};
use leep_derive::Repo;
use leep_store::FsBlobStore;

/// Set the time of `packs/` back, as a change in the same timestamp tick
/// (or a filesystem that caches directory attributes) leaves it: the
/// other server's stamp then vouches for a view without the new packs.
fn hide_packs(root: &Path, t: SystemTime) {
    std::fs::File::open(root.join("packs"))
        .unwrap()
        .set_modified(t)
        .unwrap();
}

async fn commit(
    repo: &Repo,
    parent: Option<ChangesetId>,
    files: &[(String, Bytes)],
) -> ChangesetId {
    // Packed by one put_many, as a land batch or an import writes them.
    let items: Vec<(Hash, Bytes)> = files
        .iter()
        .map(|(_, b)| (Hash::of_blob(b), b.clone()))
        .collect();
    repo.blobs().put_many(items).await.unwrap();
    let changes = files
        .iter()
        .map(|(p, b)| {
            (
                RepoPath::new(p).unwrap(),
                FileChange::Modify {
                    content: Hash::of_blob(b),
                    kind: FileKind::Regular,
                    size: b.len() as u64,
                },
            )
        })
        .collect();
    repo.create_changeset(Changeset {
        parents: parent.into_iter().collect(),
        author: "agent".into(),
        timestamp: 1,
        message: "m".into(),
        changes,
        extra: BTreeMap::new(),
    })
    .await
    .unwrap()
}

fn files(tag: &str) -> Vec<(String, Bytes)> {
    (0..24)
        .map(|i| {
            (
                format!("src/d{}/{tag}{i}.txt", i % 4),
                Bytes::from(format!("{tag} file {i}\n")),
            )
        })
        .collect()
}

#[tokio::test]
async fn a_second_server_reads_what_another_just_landed() {
    let dir = tempfile::tempdir().unwrap();
    let meta = Arc::new(MemoryMetaStore::new());
    let a = Repo::new(Arc::new(FsBlobStore::new(dir.path())), meta.clone());
    let b = Repo::new(Arc::new(FsBlobStore::new(dir.path())), meta.clone());
    let old = SystemTime::now() - Duration::from_secs(10);

    let base_files = files("base");
    let base = commit(&a, None, &base_files).await;
    a.land(base, "main").await.unwrap();
    hide_packs(dir.path(), old);
    let path = RepoPath::new(&base_files[0].0).unwrap();
    let (_, got) = b.read_file(base, &path).await.unwrap().unwrap();
    assert_eq!(got, base_files[0].1);

    let new_files = files("new");
    let next = commit(&a, Some(base), &new_files).await;
    a.land(next, "main").await.unwrap();
    hide_packs(dir.path(), old);

    let content = Hash::of_blob(&new_files[5].1);
    assert_eq!(
        b.blobs().get(&content).await.unwrap(),
        None,
        "the stale view misses it"
    );
    // Before anything else rescans B's packs: the content read alone.
    assert_eq!(
        b.read_content(&content).await.unwrap(),
        Some(new_files[5].1.clone())
    );
    let hashes: Vec<Hash> = new_files.iter().map(|(_, c)| Hash::of_blob(c)).collect();
    let all = b.read_contents(&hashes).await.unwrap();
    for ((_, want), got) in new_files.iter().zip(all) {
        assert_eq!(got.as_ref(), Some(want));
    }

    // And a file read, whose tree nodes the land packed, after another
    // hidden land.
    let later_files = files("later");
    let later = commit(&a, Some(next), &later_files).await;
    a.land(later, "main").await.unwrap();
    hide_packs(dir.path(), old);
    let path = RepoPath::new(&later_files[7].0).unwrap();
    assert_eq!(
        b.blobs()
            .get(&Hash::of_blob(&later_files[7].1))
            .await
            .unwrap(),
        None
    );
    let (_, got) = b.read_file(later, &path).await.unwrap().unwrap();
    assert_eq!(got, later_files[7].1);
    assert_eq!(
        b.read_content(&Hash::of_blob(b"never")).await.unwrap(),
        None
    );
}
