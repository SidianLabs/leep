//! How fast the warmer's history index catches up a long linear gap on
//! Postgres, and the metadata reads it makes (ADR 0077). A measurement,
//! not a check:
//!
//! ```sh
//! LEEP_TEST_POSTGRES_URL=... LEEP_CATCH_UP_GAP=4000 \
//!   cargo test --release -p leep-server --test warm_catch_up -- --ignored --nocapture
//! ```

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use leep_core::lane::{scope, Lane};
use leep_core::memory::MemoryBlobStore;
use leep_core::{
    BlobStore, Changeset, ChangesetId, DerivedRow, FileChange, FileKind, Hash, MetaStore,
    RefLogEntry, RefUpdateContext, RepoPath, Result,
};
use leep_derive::derived::{self, HistoryTree};
use leep_derive::Repo;
use leep_meta::PostgresMetaStore;

/// Counts read statements, passing everything a derivation uses through.
struct Counter {
    inner: Arc<dyn MetaStore>,
    reads: AtomicUsize,
    /// Reads, and microseconds since `started`, at the first write: the
    /// cost of finding the range before anything derives.
    first_write: std::sync::OnceLock<(usize, u128)>,
    started: Instant,
}

impl Counter {
    fn note(&self) {
        self.reads.fetch_add(1, Ordering::Relaxed);
    }

    fn wrote(&self) {
        self.first_write.get_or_init(|| {
            (
                self.reads.load(Ordering::Relaxed),
                self.started.elapsed().as_micros(),
            )
        });
    }
}

#[async_trait::async_trait]
impl MetaStore for Counter {
    async fn put_changeset(&self, cs: &Changeset) -> Result<ChangesetId> {
        self.inner.put_changeset(cs).await
    }
    async fn get_changeset(&self, id: &ChangesetId) -> Result<Option<Changeset>> {
        self.note();
        self.inner.get_changeset(id).await
    }
    async fn get_changesets(&self, ids: &[ChangesetId]) -> Result<Vec<Option<Changeset>>> {
        self.note();
        self.inner.get_changesets(ids).await
    }
    async fn list_changesets(
        &self,
        after: Option<ChangesetId>,
        limit: usize,
    ) -> Result<Vec<ChangesetId>> {
        self.inner.list_changesets(after, limit).await
    }
    async fn get_ref(&self, name: &str) -> Result<Option<ChangesetId>> {
        self.inner.get_ref(name).await
    }
    async fn cas_ref_logged(
        &self,
        name: &str,
        expected: Option<ChangesetId>,
        new: Option<ChangesetId>,
        ctx: &RefUpdateContext,
    ) -> Result<bool> {
        self.inner.cas_ref_logged(name, expected, new, ctx).await
    }
    async fn ref_log(
        &self,
        name: Option<&str>,
        before_seq: Option<u64>,
        limit: usize,
    ) -> Result<Vec<RefLogEntry>> {
        self.inner.ref_log(name, before_seq, limit).await
    }
    async fn list_refs(&self) -> Result<BTreeMap<String, ChangesetId>> {
        self.inner.list_refs().await
    }
    async fn get_derived(&self, kind: &str, key: &Hash) -> Result<Option<Bytes>> {
        self.note();
        self.inner.get_derived(kind, key).await
    }
    async fn get_derived_many(&self, kind: &str, keys: &[Hash]) -> Result<Vec<Option<Bytes>>> {
        self.note();
        self.inner.get_derived_many(kind, keys).await
    }
    async fn get_derived_kinds(
        &self,
        reads: &[(&str, &[Hash])],
    ) -> Result<Vec<Vec<Option<Bytes>>>> {
        self.note();
        self.inner.get_derived_kinds(reads).await
    }
    async fn put_derived(&self, kind: &str, key: &Hash, value: Bytes) -> Result<()> {
        self.wrote();
        self.inner.put_derived(kind, key, value).await
    }
    async fn put_derived_many(&self, rows: &[DerivedRow]) -> Result<()> {
        self.wrote();
        self.inner.put_derived_many(rows).await
    }
}

async fn commit(r: &Repo, parent: Option<ChangesetId>, n: usize) -> ChangesetId {
    let text = format!("change {n}\n");
    let content = r.blobs().put(Bytes::from(text.clone())).await.unwrap();
    r.create_changeset(Changeset {
        parents: parent.into_iter().collect(),
        author: "agent".into(),
        timestamp: n as i64,
        message: format!("c{n}"),
        changes: BTreeMap::from([(
            RepoPath::new(format!("src/m{}/f{}.rs", n % 16, n % 97)).unwrap(),
            FileChange::Modify {
                content,
                kind: FileKind::Regular,
                size: text.len() as u64,
            },
        )]),
        extra: BTreeMap::new(),
    })
    .await
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn warmer_catches_up_a_linear_gap() {
    let Ok(url) = std::env::var("LEEP_TEST_POSTGRES_URL") else {
        eprintln!("skipping: LEEP_TEST_POSTGRES_URL not set");
        return;
    };
    let gap: usize = std::env::var("LEEP_CATCH_UP_GAP")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(4000);
    let schema = format!("leep_test_warm_catch_up_{}", std::process::id());
    let pg: Arc<dyn MetaStore> = Arc::new(
        PostgresMetaStore::connect_with_schema(&url, &schema)
            .await
            .unwrap(),
    );
    let blobs: Arc<dyn BlobStore> = Arc::new(MemoryBlobStore::new());
    let writer = Repo::new(blobs.clone(), pg.clone());
    // An indexed base, then the gap on top of it.
    let mut head = commit(&writer, None, 0).await;
    for n in 1..500 {
        head = commit(&writer, Some(head), n).await;
    }
    derived::derive_heads_counted::<HistoryTree>(&writer, &[head])
        .await
        .unwrap();
    for n in 500..500 + gap {
        head = commit(&writer, Some(head), n).await;
    }

    // A cold handle in the index lane, as the warmer derives.
    let counter = Arc::new(Counter {
        inner: pg,
        reads: AtomicUsize::new(0),
        first_write: std::sync::OnceLock::new(),
        started: Instant::now(),
    });
    let cold = Repo::new(blobs, counter.clone());
    let started = counter.started;
    let n = scope(
        Lane::Index,
        derived::derive_heads_counted::<HistoryTree>(&cold, &[head]),
    )
    .await
    .unwrap();
    let took = started.elapsed();
    let reads = counter.reads.load(Ordering::Relaxed);
    let (found_reads, found_us) = counter.first_write.get().copied().unwrap_or_default();
    println!(
        "gap {gap}: derived {n} in {:.2}s, {:.0} changesets/s, {reads} metadata reads ({:.2} per changeset); first write after {found_reads} reads, {:.2}s",
        took.as_secs_f64(),
        n as f64 / took.as_secs_f64(),
        reads as f64 / n as f64,
        found_us as f64 / 1e6,
    );
    assert_eq!(n, gap);
}
