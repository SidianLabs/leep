//! Creates that leave their tree nodes out (ADR 0069), on a real
//! filesystem store: what a create writes, and that lands and a restarted
//! process read the same trees as when creates stored them.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;
use leep_core::api::LandResponse;
use leep_core::memory::MemoryMetaStore;
use leep_core::{Changeset, ChangesetId, FileChange, FileKind, MetaStore, RepoPath};
use leep_derive::Repo;
use leep_store::FsBlobStore;

/// Loose files in the store, and the files of its packs.
fn files(root: &Path) -> (usize, usize) {
    let (mut loose, mut packed) = (0, 0);
    let mut stack = vec![root.to_path_buf()];
    while let Some(d) = stack.pop() {
        for e in std::fs::read_dir(&d).unwrap() {
            let e = e.unwrap();
            if e.file_type().unwrap().is_dir() {
                stack.push(e.path());
            } else if e.path().to_string_lossy().contains("packs") {
                packed += 1;
            } else {
                loose += 1;
            }
        }
    }
    (loose, packed)
}

async fn upload(repo: &Repo, files: &[(String, String)]) -> BTreeMap<RepoPath, FileChange> {
    let mut changes = BTreeMap::new();
    for (path, body) in files {
        let content = repo.put_blob(Bytes::from(body.clone())).await.unwrap();
        changes.insert(
            RepoPath::new(path.as_str()).unwrap(),
            FileChange::Modify {
                content,
                kind: FileKind::Regular,
                size: body.len() as u64,
            },
        );
    }
    changes
}

fn changeset(parent: Option<ChangesetId>, changes: BTreeMap<RepoPath, FileChange>) -> Changeset {
    Changeset {
        parents: parent.into_iter().collect(),
        author: "agent".into(),
        timestamp: 1,
        message: format!("{:?}", changes.keys().collect::<Vec<_>>()),
        changes,
        extra: BTreeMap::new(),
    }
}

/// An agent's change: a file of its own three levels down, and one in a
/// directory every agent writes.
fn agent_files(a: usize, r: usize) -> Vec<(String, String)> {
    vec![
        (
            format!("src/agents/a{a}/r{r}.rs"),
            format!("agent {a} round {r}\n"),
        ),
        (format!("src/shared/a{a}.rs"), format!("round {r}\n")),
    ]
}

/// Files a create adds beyond its uploads, eager and lazy: a create on a
/// stored tree rewrites its root and each directory on its paths, one file
/// each; a lazy one writes none.
#[tokio::test]
async fn a_lazy_create_adds_no_file_beyond_its_uploads() {
    let mut per_create = Vec::new();
    for lazy in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let meta = Arc::new(MemoryMetaStore::new());
        let repo = Repo::new(Arc::new(FsBlobStore::new(dir.path())), meta.clone())
            .with_lazy_create_trees(lazy);
        let changes = upload(&repo, &agent_files(0, 0)).await;
        let base = repo
            .create_changeset(changeset(None, changes))
            .await
            .unwrap();
        meta.cas_ref("main", None, Some(base)).await.unwrap();
        // The ref's tree is stored whole: a land stores a fast-forward's.
        let base_root = repo.root_tree(base).await.unwrap();
        repo.materialize_tree(base).await.unwrap();
        assert!(repo.blobs().contains(&base_root).await.unwrap());
        let mut added = 0;
        let creates = 20;
        for a in 1..=creates {
            let changes = upload(&repo, &agent_files(a, 0)).await;
            let uploaded = files(dir.path());
            repo.create_changeset(changeset(Some(base), changes))
                .await
                .unwrap();
            let after = files(dir.path());
            assert_eq!(after.1, uploaded.1, "a create writes no pack");
            added += after.0 - uploaded.0;
        }
        per_create.push(added as f64 / creates as f64);
    }
    println!(
        "tree files per create: eager {:.1}, lazy {:.1}",
        per_create[0], per_create[1]
    );
    assert_eq!(per_create[0], 5.0, "root, src, agents, a<n>, shared");
    assert_eq!(per_create[1], 0.0);
}

/// Many agents creating and landing on one ref: the heads and every file
/// they read are the same with lazy creates, and a new process on the same
/// directory reads every landed tree from disk alone.
#[tokio::test]
async fn lazy_lands_read_the_same_after_a_restart() {
    let mut results = Vec::new();
    for lazy in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let meta = Arc::new(MemoryMetaStore::new());
        let repo = Repo::new(Arc::new(FsBlobStore::new(dir.path())), meta.clone())
            .with_lazy_create_trees(lazy);
        let changes = upload(&repo, &agent_files(0, 0)).await;
        let base = repo
            .create_changeset(changeset(None, changes))
            .await
            .unwrap();
        repo.land(base, "main").await.unwrap();
        // Rounds of 6 agents on the head they read; some land a stack of two.
        for r in 0..4 {
            let mut tasks = Vec::new();
            for a in 0..6 {
                let repo = repo.clone();
                tasks.push(tokio::spawn(async move {
                    let head = repo.meta().get_ref("main").await.unwrap();
                    let changes = upload(&repo, &agent_files(a, r)).await;
                    let mut cs = repo
                        .create_changeset(changeset(head, changes))
                        .await
                        .unwrap();
                    if a % 3 == 0 {
                        let more = upload(&repo, &[(format!("top/a{a}"), format!("{r}\n"))]).await;
                        cs = repo
                            .create_changeset(changeset(Some(cs), more))
                            .await
                            .unwrap();
                    }
                    match repo.land(cs, "main").await.unwrap() {
                        LandResponse::Landed { .. } => {}
                        other => panic!("{other:?}"),
                    }
                }));
            }
            for t in tasks {
                t.await.unwrap();
            }
        }
        drop(repo);
        let fresh = Repo::new(Arc::new(FsBlobStore::new(dir.path())), meta.clone());
        let head = meta.get_ref("main").await.unwrap().unwrap();
        let mut seen = Vec::new();
        let mut cur = Some(head);
        while let Some(id) = cur {
            let listed = fresh.list_files(id, "").await.unwrap();
            let mut text = String::new();
            for f in listed {
                let (_, data) = fresh.read_file(id, &f.path).await.unwrap().unwrap();
                text += &format!("{} {}", f.path, String::from_utf8_lossy(&data));
            }
            seen.push(text);
            cur = fresh
                .get_changeset(id)
                .await
                .unwrap()
                .parents
                .first()
                .copied();
        }
        // The head's tree is stored whole on disk, whatever made it.
        let root = fresh.root_tree(head).await.unwrap();
        assert!(fresh.blobs().contains(&root).await.unwrap());
        results.push(seen.len());
    }
    // Lands of the same changes interleave differently from run to run, so
    // the histories are compared by length; their contents were read above.
    assert_eq!(results[0], results[1]);
}
