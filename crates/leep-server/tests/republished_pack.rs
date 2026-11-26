//! A pack published again under the same id is not deleted by the cleanup
//! of the repack that replaced it earlier (ADR 0107), end to end through
//! `BackgroundRepack::tick` with the default fence, ratio, factor and caps
//! (only the check interval and the age trigger are shortened):
//!
//! 1. a delta run packs the history: pack A;
//! 2. loose objects no ref reaches arrive (an abandoned upload, retired
//!    segment blobs); the next delta run folds A in by the geometric rule:
//!    P1 = A's frames + the new objects. Its cleanup ("delete A") waits for
//!    the fence;
//! 3. inside the fence GC deletes those objects (tombstones on P1);
//! 4. the next check consolidates the delta packs for the tombstoned bytes:
//!    the result is byte for byte A, so pack id A is published again and
//!    taken out of `packs/retired`;
//! 5. both fences pass: the cleanup of step 2 leaves A (no longer retired),
//!    that of step 4 deletes P1, and every object reads.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use leep_core::memory::MemoryMetaStore;
use leep_core::RepoPath;
use leep_core::{BlobStore, Changeset, ChangesetId, FileChange, FileKind, Hash, MetaStore};
use leep_derive::Repo;
use leep_server::background_repack::{
    AutoOutcome, AutoRepackOptions, BackgroundRepack, BackgroundRepackOptions, ConsolidateOutcome,
    PackStore,
};
use leep_store::FsBlobStore;

const FILES: [&str; 3] = ["src/lib.rs", "src/deep/mod.rs", "README.md"];

fn version(file: &str, v: usize) -> String {
    let mut s = format!("// {file}\n");
    for i in 0..=v {
        s.push_str(&format!(
            "fn item_{i}() {{ let value = {i} * 7; println!(\"{file} {{value}}\"); }}\n"
        ));
    }
    s
}

async fn land_one(repo: &Repo, meta: &Arc<MemoryMetaStore>, v: usize) -> Vec<Hash> {
    let head: Option<ChangesetId> = meta.get_ref("main").await.unwrap();
    let mut changes = BTreeMap::new();
    let mut contents = Vec::new();
    for f in FILES {
        let body = version(f, v);
        contents.push(repo.blobs().put(Bytes::from(body.clone())).await.unwrap());
        changes.insert(
            RepoPath::new(f).unwrap(),
            FileChange::Modify {
                content: Hash::of_blob(body.as_bytes()),
                kind: FileKind::Regular,
                size: body.len() as u64,
            },
        );
    }
    let tip = repo
        .create_changeset(Changeset {
            parents: head.into_iter().collect(),
            author: "agent".into(),
            timestamp: 1,
            message: format!("v{v}"),
            changes,
            extra: BTreeMap::new(),
        })
        .await
        .unwrap();
    assert!(meta.cas_ref("main", head, Some(tip)).await.unwrap());
    contents
}

fn pack_ids(root: &std::path::Path) -> Vec<String> {
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
async fn a_pack_published_again_survives_the_earlier_cleanup() {
    let dir = tempfile::tempdir().unwrap();
    let store = FsBlobStore::new(dir.path());
    let meta = Arc::new(MemoryMetaStore::new());
    let repo = Repo::new(Arc::new(store.clone()), meta.clone());

    let opts = BackgroundRepackOptions {
        auto: Some(AutoRepackOptions {
            check_interval: Duration::ZERO,
            max_age: Some(Duration::ZERO),
            ..AutoRepackOptions::default()
        }),
        ..BackgroundRepackOptions::default()
    };
    assert_eq!(opts.fence, Duration::from_secs(360));
    let bg = BackgroundRepack::new(PackStore::Fs(store.clone()), opts);
    let walk = bg.walk_repo(meta.clone());
    let mut bg = bg.with_repo(walk);

    // 1. History, compressed by a delta run: pack A. Its fence passes.
    let mut history = Vec::new();
    for v in 0..8 {
        history.extend(land_one(&repo, &meta, v).await);
    }
    let t = bg.tick().await.unwrap();
    let AutoOutcome::Ran(r) = t.auto else {
        panic!("{:?}", t.auto)
    };
    let a = r.pack_id.clone().unwrap();
    bg.finish_all().await.unwrap();
    assert_eq!(pack_ids(dir.path()), vec![a.clone()]);

    // 2. Unreferenced loose objects; the delta run folds A in.
    let mut junk = Vec::new();
    for i in 0..64u32 {
        let mut b = Vec::new();
        for k in 0..64u32 {
            b.extend_from_slice(blake3::hash(&(i * 1000 + k).to_be_bytes()).as_bytes());
        }
        junk.push(store.put(Bytes::from(b)).await.unwrap());
    }
    let t = bg.tick().await.unwrap();
    let AutoOutcome::Ran(r) = t.auto else {
        panic!("{:?}", t.auto)
    };
    let p1 = r.pack_id.clone().unwrap();
    assert_ne!(p1, a);
    assert_eq!(r.packs_merged, 1, "A was folded in: {r:?}");
    assert_eq!(bg.pending(), 1, "its cleanup waits for the fence");

    // 3. GC deletes the unreferenced objects, inside the fence.
    for h in &junk {
        store.delete(h).await.unwrap();
    }

    // 4. The next check consolidates for the tombstoned bytes.
    let t = bg.tick().await.unwrap();
    let ConsolidateOutcome::Ran { report, gain, .. } = t.consolidate else {
        panic!("{:?} / {:?}", t.auto, t.consolidate)
    };
    let p2 = report.pack_id.clone().unwrap();
    assert!(gain > 0);
    assert_eq!(p2, a, "the consolidation published A again");
    assert_eq!(bg.pending(), 2);
    let fresh = FsBlobStore::new(dir.path());
    for h in &history {
        assert!(fresh.get(h).await.unwrap().is_some(), "still readable");
    }

    // 5. Both fences pass.
    bg.finish_all().await.unwrap();
    assert_eq!(pack_ids(dir.path()), vec![a.clone()]);
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
    assert_eq!(lost, 0, "{lost} of {} file contents lost", history.len());
}
