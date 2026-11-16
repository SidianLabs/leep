//! Derived segments (ADR 0104 to 0106) against the real metadata backends, SQLite
//! and (when `LEEP_TEST_POSTGRES_URL` is set) Postgres, over a filesystem
//! blob store: the `MetaStore` contracts through the segment view, and a
//! store sealed, merged and retired by the compactor reading every value
//! as before, through SQL and files. In this crate because `leep-derive`
//! may not depend on a concrete store.

use std::collections::BTreeMap;
use std::sync::Arc;

use bytes::Bytes;
use leep_core::{
    BlobStore, Changeset, ChangesetId, FileChange, FileKind, Hash, MetaStore, RepoPath,
};
use leep_derive::derived::{derive, HistoryTree};
use leep_derive::derived_segments::{
    compact_pass, CompactOptions, CompactState, Features, Mode, PassReport,
};
use leep_derive::search::SearchQuery;
use leep_derive::Repo;
use leep_store::FsBlobStore;

async fn sqlite(dir: &std::path::Path, name: &str) -> Arc<dyn MetaStore> {
    let url = format!("sqlite://{}?mode=rwc", dir.join(name).display());
    Arc::new(leep_meta::SqliteMetaStore::connect(&url).await.unwrap())
}

async fn postgres(name: &str) -> Option<(Arc<dyn MetaStore>, leep_meta::pg_test::TestSchema)> {
    let schema = leep_meta::pg_test::TestSchema::create(name)?;
    let meta = leep_meta::PostgresMetaStore::connect_with_schema(&schema.url, &schema.name)
        .await
        .unwrap();
    Some((Arc::new(meta), schema))
}

/// The view a `Repo` reads through, over `meta`.
fn view(meta: Arc<dyn MetaStore>, dir: &std::path::Path) -> Arc<dyn MetaStore> {
    let blobs: Arc<dyn BlobStore> = Arc::new(FsBlobStore::new(dir.join("blobs")));
    Repo::new(blobs, meta).meta().clone()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn contracts_hold_through_the_view_on_sqlite() {
    let dir = tempfile::tempdir().unwrap();
    let d = dir.path();
    leep_core::memory::check_meta_store_contract(&*view(sqlite(d, "a.db").await, d)).await;
    leep_core::memory::check_cas_derived_contract(&*view(sqlite(d, "b.db").await, d)).await;
    leep_core::memory::check_insert_once_contract(&*view(sqlite(d, "c.db").await, d)).await;
    leep_core::memory::check_lease_contract(&*view(sqlite(d, "d.db").await, d)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn contracts_hold_through_the_view_on_postgres() {
    let dir = tempfile::tempdir().unwrap();
    let d = dir.path();
    let Some((a, _sa)) = postgres("leep_test_ds_contract_a").await else {
        return;
    };
    leep_core::memory::check_meta_store_contract(&*view(a, d)).await;
    let (b, _sb) = postgres("leep_test_ds_contract_b").await.unwrap();
    leep_core::memory::check_cas_derived_contract(&*view(b, d)).await;
    let (c, _sc) = postgres("leep_test_ds_contract_c").await.unwrap();
    leep_core::memory::check_insert_once_contract(&*view(c, d)).await;
    let (e, _se) = postgres("leep_test_ds_contract_d").await.unwrap();
    leep_core::memory::check_lease_contract(&*view(e, d)).await;
}

async fn commit(r: &Repo, parents: &[ChangesetId], path: &str, body: &str) -> ChangesetId {
    let content = r.put_blob(Bytes::from(body.to_string())).await.unwrap();
    r.create_changeset(Changeset {
        parents: parents.to_vec(),
        author: "t".into(),
        timestamp: 1,
        message: format!("{path} {body}"),
        changes: BTreeMap::from([(
            RepoPath::new(path).unwrap(),
            FileChange::Modify {
                content,
                kind: FileKind::Regular,
                size: body.len() as u64,
            },
        )]),
        extra: BTreeMap::new(),
    })
    .await
    .unwrap()
}

/// Both values of `ids` through a new handle; history parts by content.
async fn values(r: &Repo, ids: &[ChangesetId]) -> Vec<Option<Vec<u8>>> {
    let fresh = Repo::new(r.blobs().clone(), r.meta().clone());
    let keys: Vec<Hash> = ids.iter().map(|i| i.0).collect();
    let mut out: Vec<Option<Vec<u8>>> = fresh
        .meta()
        .get_derived_many("skiplist", &keys)
        .await
        .unwrap()
        .into_iter()
        .map(|v| v.map(|b| b.to_vec()))
        .collect();
    for v in fresh
        .meta()
        .get_derived_many("history-tree-v4", &keys)
        .await
        .unwrap()
    {
        out.push(v.map(|b| {
            leep_derive::derived::history_pack::canonical(&b)
                .unwrap()
                .into_owned()
        }));
    }
    out
}

fn body(i: u32) -> String {
    format!("fn value_{i}() -> u32 {{ needle_{} }}\n", i % 5)
}

/// What searches of `id` find, through a new handle.
async fn answers(r: &Repo, id: ChangesetId) -> Vec<String> {
    let fresh = Repo::new(r.blobs().clone(), r.meta().clone());
    let mut out = Vec::new();
    for q in ["needle_3", "value_4", "fn value", "nowhere_xyz"] {
        for m in fresh
            .search(id, &SearchQuery::new(q))
            .await
            .unwrap()
            .matches
        {
            out.push(format!("{q} {} {}", m.path, m.line));
        }
    }
    out
}

async fn pass(r: &Repo, mode: Mode, state: &mut CompactState) -> PassReport {
    let hook = Features {
        record: Arc::new(|_| Box::pin(async { Ok(()) })),
        recorded: Arc::new(|_| Box::pin(async { Ok(true) })),
    };
    compact_pass(r, &CompactOptions::small(mode), state, Some(&hook))
        .await
        .unwrap()
        .expect("lease")
}

async fn exercise(meta: Arc<dyn MetaStore>, dir: &std::path::Path) {
    let blobs: Arc<dyn BlobStore> = Arc::new(FsBlobStore::new(dir));
    let r = Repo::new(blobs, meta);
    let mut state = CompactState::default();
    let mut ids = Vec::new();
    let mut tip = commit(&r, &[], "a.txt", "a").await;
    ids.push(tip);
    let mut sealed = 0;
    let mut merges = 0;
    for i in 0..48 {
        let next = if i % 11 == 5 {
            let side = commit(&r, &[tip], &format!("s/{i}"), "s").await;
            ids.push(side);
            commit(&r, &[tip, side], &format!("d{}/f", i % 3), &body(i)).await
        } else {
            commit(&r, &[tip], &format!("d{}/f", i % 3), &body(i)).await
        };
        tip = next;
        ids.push(tip);
        if i % 3 == 2 {
            derive::<HistoryTree>(&r, tip).await.unwrap();
            // A head, and its search rows (family content-v1, ADR 0106).
            let was = r.meta().get_ref("main").await.unwrap();
            assert!(r.meta().cas_ref("main", was, Some(tip)).await.unwrap());
            r.derive_search(tip).await.unwrap();
            let p = pass(&r, Mode::Build, &mut state).await;
            sealed += p.sealed_segments;
            merges += p.merges;
        }
    }
    derive::<HistoryTree>(&r, tip).await.unwrap();
    assert!(sealed > 4 && merges > 0, "{sealed} sealed, {merges} merges");
    let want = values(&r, &ids).await;
    assert!(want.iter().all(Option::is_some));
    let found = answers(&r, tip).await;
    assert!(!found.is_empty());
    for _ in 0..10 {
        let p = pass(&r, Mode::Retire, &mut state).await;
        if p.sealed_segments + p.merges + p.retired_rows == 0 {
            break;
        }
    }
    assert_eq!(r.row_meta().count_derived("skiplist").await.unwrap(), 0);
    assert_eq!(
        r.row_meta().count_derived("history-tree-v4").await.unwrap(),
        0
    );
    assert_eq!(values(&r, &ids).await, want);
    // Every head is inside the retention window, so every search row was
    // live, sealed and retired.
    for kind in [
        "blob-trigrams-v2",
        "blob-trigrams-keyframe-v2",
        "tree-trigrams-v2",
        "tree-trigrams-keyframe-v2",
    ] {
        assert_eq!(r.row_meta().count_derived(kind).await.unwrap(), 0, "{kind}");
    }
    assert_eq!(answers(&r, tip).await, found);
    let opts = leep_derive::integrity::VerifyOptions {
        full: true,
        ..Default::default()
    };
    let rep = leep_derive::integrity::verify(&r, &opts, &leep_derive::integrity::Quiet)
        .await
        .unwrap();
    assert!(rep.problems.is_empty(), "{:?}", rep.problems);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn seal_merge_retire_on_sqlite() {
    let dir = tempfile::tempdir().unwrap();
    let meta = sqlite(dir.path(), "meta.db").await;
    exercise(meta, &dir.path().join("blobs")).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn seal_merge_retire_on_postgres() {
    let Some((meta, _schema)) = postgres("leep_test_ds_exercise").await else {
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    exercise(meta, &dir.path().join("blobs")).await;
}
