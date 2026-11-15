//! GC mark segments (ADR 0103) against the real metadata backends, SQLite
//! and (when `LEEP_TEST_POSTGRES_URL` is set) Postgres, over a filesystem
//! blob store: checkpoints and merges every few marks, the segment cap, the
//! import and deletion of `gc-mark-v1` rows, and a full mark replacing the
//! record. The unit tests run all of this over the memory stores; here the
//! manifest's compare-and-swap and the segment writes go through SQL and
//! files. In this crate because `leep-derive` may not depend on a concrete
//! store.

use std::collections::{BTreeMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use leep_core::{
    BlobStore, Changeset, ChangesetId, DerivedRow, FileChange, FileKind, Hash, MetaStore, RepoPath,
};
use leep_derive::gc::{self, Clock, GcOptions, Mark};
use leep_derive::gc_segments::{self, Manifest, ManifestRead};
use leep_derive::Repo;
use leep_store::FsBlobStore;

/// Every run in this test binary checkpoints every few marks and caps
/// merges low, so a small history publishes and merges many segments.
const CHECKPOINT: usize = 5;
const CAP: u64 = 24;

struct Stepping(Mutex<i64>);

#[async_trait]
impl Clock for Stepping {
    fn now(&self) -> i64 {
        *self.0.lock().unwrap()
    }
    async fn sleep(&self, d: Duration) {
        *self.0.lock().unwrap() += d.as_secs() as i64;
    }
}

impl Stepping {
    fn advance(&self, secs: i64) {
        *self.0.lock().unwrap() += secs;
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

async fn commit(r: &Repo, parents: &[ChangesetId], files: &[(String, String)]) -> ChangesetId {
    let mut changes = BTreeMap::new();
    for (p, t) in files {
        let content = r.put_blob(Bytes::from(t.clone())).await.unwrap();
        changes.insert(
            RepoPath::new(p).unwrap(),
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

async fn manifest(r: &Repo) -> Manifest {
    match gc_segments::read_manifest(r).await.unwrap() {
        ManifestRead::Read(m) => m,
        other => panic!("manifest: {other:?}"),
    }
}

/// Every referenced object reads back, and a condemn finds no garbage.
async fn all_kept(r: &Repo, clock: &Stepping, what: &str) {
    let order = leep_derive::pack_order::pack_order(r).await.unwrap();
    for (h, _) in order.objects.iter() {
        assert!(
            r.blobs().get(&h).await.unwrap().is_some(),
            "{what}: {h} lost"
        );
    }
    clock.advance(10);
    let c = gc::condemn(r, &opts(), clock).await.unwrap();
    assert_eq!(c.condemned, 0, "{what}: {c:?}");
}

fn assert_capped(m: &Manifest, what: &str) {
    for w in m.live.windows(2) {
        assert!(
            w[1].marks * 2 < w[0].marks || w[0].marks + w[1].marks > CAP,
            "{what}: {:?}",
            m.live
        );
    }
}

async fn exercise(meta: Arc<dyn MetaStore>, dir: &std::path::Path) {
    gc_segments::set_limits_for_tests(CHECKPOINT, CAP);
    let blobs: Arc<dyn BlobStore> = Arc::new(FsBlobStore::new(dir));
    let r = Repo::new(blobs, meta);
    let clock = Stepping(Mutex::new(gc::SystemClock.now() + 60));

    // A store GC'd before ADR 0103: a row for every referenced object and
    // stored changeset.
    let mut tip = commit(&r, &[], &[("a.txt".into(), "a\n".into())]).await;
    for i in 0..4 {
        let f = vec![(format!("d{i}/e/f.txt"), format!("f{i}\n"))];
        tip = commit(&r, &[tip], &f).await;
    }
    let order = leep_derive::pack_order::pack_order(&r).await.unwrap();
    let mut keys: Vec<Hash> = order.objects.iter().map(|(h, _)| h).collect();
    keys.extend(
        r.meta()
            .list_changesets(None, usize::MAX)
            .await
            .unwrap()
            .into_iter()
            .map(|id| id.0),
    );
    let rows: Vec<DerivedRow> = keys
        .iter()
        .map(|k| DerivedRow::new(gc::GC_MARK, *k, Bytes::from_static(&[1])))
        .collect();
    r.meta().put_derived_many(&rows).await.unwrap();
    let c = gc::condemn(&r, &opts(), &clock).await.unwrap();
    assert_eq!((c.changesets_walked, c.condemned), (0, 0), "{c:?}");

    // New history: segments resting on the rows, published every few
    // marks and merged, never past the cap.
    let mut published = HashSet::new();
    for round in 0..10 {
        let f = vec![
            (format!("r{round}/x/y.txt"), format!("y{round}\n")),
            (format!("r{round}/z.txt"), format!("z{round}\n")),
        ];
        tip = commit(&r, &[tip], &f).await;
        clock.advance(1);
        let c = gc::condemn(&r, &opts(), &clock).await.unwrap();
        assert_eq!((c.changesets_walked, c.condemned), (1, 0), "{c:?}");
        let m = manifest(&r).await;
        assert!(!m.rows_retired);
        assert_capped(&m, &format!("round {round}"));
        published.extend(m.live.iter().map(|s| s.hash));
    }
    let m = manifest(&r).await;
    assert!(m.live.len() >= 2, "{:?}", m.live);
    assert!(!m.retired.is_empty(), "no merge ran: {m:?}");
    assert!(published.len() > m.live.len());
    all_kept(&r, &clock, "rows and segments").await;

    // Retire the rows: one imported segment first, then the delete.
    let rep = gc_segments::import_mark_rows(&r).await.unwrap();
    assert_eq!(rep.imported, keys.len() as u64);
    let imported = manifest(&r).await;
    assert!(imported.rows_retired);
    assert_eq!(imported.live[0].marks, keys.len() as u64);
    assert_eq!(&imported.live[1..], &m.live[..]);
    let deleted = gc_segments::delete_mark_rows(&r, &mut |_| {})
        .await
        .unwrap();
    assert_eq!(deleted, keys.len() as u64);
    assert!(r
        .meta()
        .list_derived(gc::GC_MARK, None, 1)
        .await
        .unwrap()
        .is_empty());
    clock.advance(1);
    let c = gc::condemn(&r, &opts(), &clock).await.unwrap();
    assert_eq!((c.changesets_walked, c.condemned), (0, 0), "{c:?}");

    // A full mark replaces the record with capped segments of its own,
    // named only at its end.
    clock.advance(1);
    let f = gc::condemn_with(&r, &opts(), Mark::Full, &clock)
        .await
        .unwrap();
    assert_eq!(f.condemned, 0, "{f:?}");
    let full = manifest(&r).await;
    assert!(full.rows_retired);
    assert!(full.live.len() >= 2, "{:?}", full.live);
    for w in full.live.windows(2) {
        assert!(w[0].marks + w[1].marks > CAP, "{:?}", full.live);
    }
    for s in &imported.live {
        assert!(full.retired.contains(&s.hash), "{s:?} not retired");
    }

    // The retired segments are collected like orphans a fence later; the
    // history stays.
    clock.advance(3600);
    let c = gc::condemn(&r, &opts(), &clock).await.unwrap();
    assert_eq!(c.condemned, 0, "{c:?}");
    // With no grace, the full run's own condemn already condemned what it
    // retired.
    let condemned = f.segments_condemned + c.segments_condemned;
    assert!(condemned >= imported.live.len() as u64, "{f:?} {c:?}");
    clock.advance(601);
    let k = gc::collect(&r, &opts(), &clock).await.unwrap();
    assert_eq!(k.deleted, 0, "{k:?}");
    assert!(k.segments_deleted >= condemned, "{k:?}");
    all_kept(&r, &clock, "after the full mark").await;
    let last = manifest(&r).await;
    assert!(last.retired.is_empty(), "{last:?}");
    for s in &last.live {
        assert!(r.blobs().get(&s.hash).await.unwrap().is_some());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn segments_on_sqlite() {
    let dir = tempfile::tempdir().unwrap();
    let url = format!("sqlite://{}?mode=rwc", dir.path().join("meta.db").display());
    let meta = Arc::new(leep_meta::SqliteMetaStore::connect(&url).await.unwrap());
    exercise(meta, &dir.path().join("blobs")).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn segments_on_postgres() {
    let Some(schema) = leep_meta::pg_test::TestSchema::create("leep_test_gc_segments") else {
        return;
    };
    let meta = Arc::new(
        leep_meta::PostgresMetaStore::connect_with_schema(&schema.url, &schema.name)
            .await
            .unwrap(),
    );
    let dir = tempfile::tempdir().unwrap();
    exercise(meta, &dir.path().join("blobs")).await;
}
