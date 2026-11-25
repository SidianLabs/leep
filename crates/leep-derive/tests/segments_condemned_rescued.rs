//! A live derived segment that an older `gc` condemned and a new `gc`
//! collect then rescued (a rescue row written, the condemnation row left in
//! place, as collect always does) has its rows retired: the compactor reads
//! the condemnation with its rescue, as every other reader of the fence does
//! (ADR 0105 section 4: the rows "go after the next collect").

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use leep_core::memory::{MemoryBlobStore, MemoryMetaStore};
use leep_core::{Changeset, ChangesetId, FileChange, FileKind, RepoPath};
use leep_derive::derived_segments::{
    compact_pass, read_manifest, CompactOptions, CompactState, Features, ManifestRead, Mode,
    FAMILY_CHANGESET,
};
use leep_derive::gc::{self, GcOptions, SystemClock};
use leep_derive::Repo;

async fn build(r: &Repo, n: usize) -> Vec<ChangesetId> {
    let mut ids = Vec::new();
    let mut parents: Vec<ChangesetId> = Vec::new();
    for i in 0..n {
        let data = Bytes::from(format!("v{i}\n"));
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
                message: format!("c{i}"),
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

async fn settle(r: &Repo, opts: &CompactOptions, st: &mut CompactState, f: Option<&Features>) {
    for _ in 0..20 {
        compact_pass(r, opts, st, f).await.unwrap();
    }
}

async fn setup() -> (Repo, Vec<ChangesetId>) {
    let r = Repo::new(
        Arc::new(MemoryBlobStore::new()),
        Arc::new(MemoryMetaStore::new()),
    );
    let ids = build(&r, 24).await;
    settle(
        &r,
        &CompactOptions::small(Mode::Build),
        &mut CompactState::default(),
        None,
    )
    .await;
    (r, ids)
}

async fn skiplist_rows(r: &Repo) -> u64 {
    r.row_meta().count_derived("skiplist").await.unwrap()
}

#[tokio::test]
async fn a_rescued_segment_has_its_rows_retired() {
    // Control: without any condemnation, `retire` deletes every skiplist row.
    let (c, cids) = setup().await;
    assert_eq!(skiplist_rows(&c).await, cids.len() as u64);
    let f = features();
    settle(
        &c,
        &CompactOptions::small(Mode::Retire),
        &mut CompactState::default(),
        Some(&f),
    )
    .await;
    assert_eq!(skiplist_rows(&c).await, 0, "control: rows retire");

    // A condemned, then rescued, segment.
    let (r, ids) = setup().await;
    let ManifestRead::Read(m) = read_manifest(&r, FAMILY_CHANGESET).await.unwrap().1 else {
        panic!("no manifest");
    };
    assert!(!m.live.is_empty());
    // An older `gc` (one that does not know derived segments) condemns the
    // live segment blobs as orphans, at t = 1.
    for s in &m.live {
        for h in [s.index, s.data] {
            r.row_meta()
                .put_derived(
                    gc::BLOB_CONDEMNED,
                    &h,
                    Bytes::copy_from_slice(&1i64.to_be_bytes()),
                )
                .await
                .unwrap();
        }
    }
    // A new `gc collect` runs after the fence: it sees the segments live and
    // rescues them.
    let opts = GcOptions {
        grace: Duration::ZERO,
        fence: Duration::ZERO,
        unsafe_no_fence: true,
        ..GcOptions::default()
    };
    gc::collect(&r, &opts, &SystemClock).await.unwrap();
    for s in &m.live {
        assert!(
            r.blobs().contains(&s.index).await.unwrap(),
            "rescued, not deleted"
        );
        assert!(
            r.blobs().contains(&s.data).await.unwrap(),
            "rescued, not deleted"
        );
        let resc = r
            .row_meta()
            .get_derived(gc::BLOB_RESCUE, &s.data)
            .await
            .unwrap();
        let cond = r
            .row_meta()
            .get_derived(gc::BLOB_CONDEMNED, &s.data)
            .await
            .unwrap();
        assert!(resc.is_some() && cond.is_some());
    }
    // Retire passes now delete the rows.
    assert_eq!(skiplist_rows(&r).await, ids.len() as u64);
    let f = features();
    let mut st = CompactState::default();
    settle(&r, &CompactOptions::small(Mode::Retire), &mut st, Some(&f)).await;
    assert_eq!(
        skiplist_rows(&r).await,
        0,
        "a rescued segment's rows retire"
    );
}
