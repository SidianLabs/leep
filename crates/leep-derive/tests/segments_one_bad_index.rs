//! One segment index blob that reads with an I/O error (not "missing", not
//! "corrupt": EIO, EACCES, a throttled or failing object store key) is
//! isolated: the family's other segments still answer, its own keys read as
//! rows or derive again, lands go on, and the compactor drops just that
//! segment once it failed several passes in a row.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use leep_core::memory::{MemoryBlobStore, MemoryMetaStore};
use leep_core::{
    BlobMeta, BlobStore, Changeset, ChangesetId, Error, FileChange, FileKind, Hash, RepoPath,
    Result,
};
use leep_derive::derived_segments::{
    compact_pass, read_manifest, CompactOptions, CompactState, Features, ManifestRead, Mode,
    FAMILY_CHANGESET_V2,
};
use leep_derive::Repo;

/// A blob store one of whose objects reads with an I/O error.
struct OneBad {
    inner: Arc<dyn BlobStore>,
    bad: Mutex<Option<Hash>>,
}

#[async_trait]
impl BlobStore for OneBad {
    async fn put_with_hash(&self, hash: Hash, data: Bytes) -> Result<()> {
        self.inner.put_with_hash(hash, data).await
    }
    async fn get(&self, hash: &Hash) -> Result<Option<Bytes>> {
        if *self.bad.lock().unwrap() == Some(*hash) {
            return Err(Error::Storage("Input/output error (os error 5)".into()));
        }
        self.inner.get(hash).await
    }
    async fn list_page(&self, after: Option<Hash>, limit: usize) -> Result<Vec<BlobMeta>> {
        self.inner.list_page(after, limit).await
    }
    async fn delete(&self, hash: &Hash) -> Result<()> {
        self.inner.delete(hash).await
    }
}

fn features() -> Features {
    let log: Arc<Mutex<Vec<&'static str>>> = Arc::default();
    let seen = log.clone();
    Features {
        record: Arc::new(move |name| {
            let log = log.clone();
            Box::pin(async move {
                log.lock().unwrap().push(name);
                Ok(())
            })
        }),
        recorded: Arc::new(move |name| {
            let seen = seen.clone();
            Box::pin(async move { Ok(seen.lock().unwrap().contains(&name)) })
        }),
    }
}

async fn build(r: &Repo, from: Option<ChangesetId>, n: usize, tag: &str) -> Vec<ChangesetId> {
    let mut ids = Vec::new();
    let mut parents: Vec<ChangesetId> = from.into_iter().collect();
    for i in 0..n {
        let data = Bytes::from(format!("{tag}{i}\n"));
        let content = r.blobs().put(data.clone()).await.unwrap();
        let mut changes = BTreeMap::new();
        changes.insert(
            RepoPath::new(format!("f{}", i % 3)).unwrap(),
            FileChange::Modify {
                content,
                kind: FileKind::Regular,
                size: data.len() as u64,
            },
        );
        let id = r
            .create_changeset(Changeset {
                parents: parents.clone(),
                author: "t".into(),
                timestamp: 1,
                message: format!("{tag}{i}"),
                changes,
                extra: BTreeMap::new(),
            })
            .await
            .unwrap();
        parents = vec![id];
        ids.push(id);
    }
    ids
}

#[tokio::test]
async fn one_unreadable_index_is_skipped_and_dropped_alone() {
    let store = Arc::new(OneBad {
        inner: Arc::new(MemoryBlobStore::new()),
        bad: Mutex::new(None),
    });
    let meta = Arc::new(MemoryMetaStore::new());
    let r = Repo::new(store.clone(), meta.clone());
    let f = features();
    // No merges, so the two seals stay two segments.
    let opts = CompactOptions {
        fanout: 64,
        ..CompactOptions::small(Mode::Retire)
    };
    let mut st = CompactState::default();
    let old = build(&r, None, 12, "a").await;
    for _ in 0..6 {
        compact_pass(&r, &opts, &mut st, Some(&f)).await.unwrap();
    }
    let ManifestRead::Read(m1) = read_manifest(&r, FAMILY_CHANGESET_V2).await.unwrap().1 else {
        panic!("no manifest");
    };
    let new = build(&r, old.last().copied(), 12, "b").await;
    for _ in 0..6 {
        compact_pass(&r, &opts, &mut st, Some(&f)).await.unwrap();
    }
    let ManifestRead::Read(m2) = read_manifest(&r, FAMILY_CHANGESET_V2).await.unwrap().1 else {
        panic!("no manifest");
    };
    assert_eq!(r.row_meta().count_derived("generation").await.unwrap(), 0);
    let newest = *m2
        .live
        .iter()
        .find(|s| !m1.live.iter().any(|o| o.index == s.index))
        .expect("a second segment");
    assert!(m2.live.len() >= 2);

    let mut gens = std::collections::HashMap::new();
    for id in old.iter().chain(&new) {
        gens.insert(*id, r.generation(*id).await.unwrap());
    }

    // The newest segment's index blob now reads with an I/O error.
    *store.bad.lock().unwrap() = Some(newest.index);
    let s = Repo::new(store.clone(), meta.clone());
    // Keys a readable segment holds still read from it.
    for id in [old[0], old[5]] {
        assert!(
            s.meta()
                .get_derived("generation", &id.0)
                .await
                .unwrap()
                .is_some(),
            "{id}"
        );
    }
    // A key only the unreadable segment held reads as missing (it derives
    // again), not as an error.
    s.meta().get_derived("generation", &new[0].0).await.unwrap();
    s.generation(new[3]).await.unwrap();
    // A land on a sealed parent goes on.
    let data = Bytes::from("x\n");
    let content = s.blobs().put(data.clone()).await.unwrap();
    let mut changes = BTreeMap::new();
    changes.insert(
        RepoPath::new("f0").unwrap(),
        FileChange::Modify {
            content,
            kind: FileKind::Regular,
            size: 2,
        },
    );
    s.create_changeset(Changeset {
        parents: vec![old[3]],
        author: "t".into(),
        timestamp: 1,
        message: "child of an old changeset".into(),
        changes,
        extra: BTreeMap::new(),
    })
    .await
    .unwrap();

    // Passes go on, and drop just that segment once it failed in enough
    // passes in a row.
    let mut st2 = CompactState::default();
    let mut dropped = 0;
    for _ in 0..4 {
        let p = compact_pass(&s, &opts, &mut st2, Some(&f))
            .await
            .unwrap()
            .unwrap();
        dropped += p.family("changeset-v2").unwrap().dropped;
    }
    assert_eq!(dropped, 1);
    let ManifestRead::Read(m3) = read_manifest(&s, FAMILY_CHANGESET_V2).await.unwrap().1 else {
        panic!("no manifest");
    };
    assert!(!m3.live.iter().any(|x| x.index == newest.index));
    for o in &m1.live {
        assert!(
            m3.live.iter().any(|x| x.index == o.index),
            "kept {}",
            o.index
        );
    }
    // Every value reads, from a segment or derived again, and is right.
    for id in old.iter().chain(&new) {
        assert_eq!(s.generation(*id).await.unwrap(), gens[id]);
    }
}
