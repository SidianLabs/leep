//! Time `gc condemn` and `gc collect` against real stores and count the store
//! calls each phase makes, as measured in docs/PERFORMANCE.md ("Garbage
//! collection").
//!
//! ```sh
//! cargo run --release -p leep-server --example gc_profile -- <meta url> <blob url> [condemn|collect|both] [--full] [--commits N]
//! ```
//! `--commits N` first stores N new changesets, each changing one file
//! two directories deep on top of the previous one, starting from the newest
//! ref (the "few new commits since the last run" case). Runs with a zero
//! fence, so only point it at stores no server is using.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use leep_core::{
    BlobMeta, BlobStore, Changeset, ChangesetId, DerivedRow, Hash, MetaStore, RefLogEntry, RefStep,
    RefUpdateContext, Result,
};
use leep_derive::gc::{self, GcOptions, Mark, SystemClock};
use leep_derive::Repo;

/// Calls and time per store method.
#[derive(Default)]
struct Tally(Mutex<BTreeMap<&'static str, (u64, u64, Duration)>>);

impl Tally {
    /// `items` counts keys or rows for batched calls (1 otherwise).
    fn add(&self, name: &'static str, items: usize, d: Duration) {
        let mut m = self.0.lock().unwrap();
        let e = m.entry(name).or_default();
        e.0 += 1;
        e.1 += items as u64;
        e.2 += d;
    }

    fn take(&self) -> BTreeMap<&'static str, (u64, u64, Duration)> {
        std::mem::take(&mut *self.0.lock().unwrap())
    }
}

macro_rules! timed {
    ($tally:expr, $name:literal, $items:expr, $e:expr) => {{
        let t = Instant::now();
        let r = $e;
        $tally.add($name, $items, t.elapsed());
        r
    }};
}

struct Blobs(Arc<dyn BlobStore>, Arc<Tally>);

#[async_trait]
impl BlobStore for Blobs {
    async fn put_with_hash(&self, hash: Hash, data: Bytes) -> Result<()> {
        timed!(
            self.1,
            "blob put",
            1,
            self.0.put_with_hash(hash, data).await
        )
    }
    async fn get(&self, hash: &Hash) -> Result<Option<Bytes>> {
        timed!(self.1, "blob get", 1, self.0.get(hash).await)
    }
    async fn contains(&self, hash: &Hash) -> Result<bool> {
        timed!(self.1, "blob contains", 1, self.0.contains(hash).await)
    }
    async fn list_page(&self, after: Option<Hash>, limit: usize) -> Result<Vec<BlobMeta>> {
        timed!(
            self.1,
            "blob list_page",
            limit,
            self.0.list_page(after, limit).await
        )
    }
    async fn delete(&self, hash: &Hash) -> Result<()> {
        timed!(self.1, "blob delete", 1, self.0.delete(hash).await)
    }
}

struct Meta(Arc<dyn MetaStore>, Arc<Tally>);

#[async_trait]
impl MetaStore for Meta {
    async fn put_changeset(&self, cs: &Changeset) -> Result<ChangesetId> {
        timed!(self.1, "put_changeset", 1, self.0.put_changeset(cs).await)
    }
    async fn get_changeset(&self, id: &ChangesetId) -> Result<Option<Changeset>> {
        timed!(self.1, "get_changeset", 1, self.0.get_changeset(id).await)
    }
    async fn get_changesets(&self, ids: &[ChangesetId]) -> Result<Vec<Option<Changeset>>> {
        timed!(
            self.1,
            "get_changesets",
            ids.len(),
            self.0.get_changesets(ids).await
        )
    }
    async fn list_changesets(
        &self,
        after: Option<ChangesetId>,
        limit: usize,
    ) -> Result<Vec<ChangesetId>> {
        timed!(
            self.1,
            "list_changesets",
            limit,
            self.0.list_changesets(after, limit).await
        )
    }
    async fn get_ref(&self, name: &str) -> Result<Option<ChangesetId>> {
        self.0.get_ref(name).await
    }
    async fn cas_ref_logged(
        &self,
        name: &str,
        expected: Option<ChangesetId>,
        new: Option<ChangesetId>,
        ctx: &RefUpdateContext,
    ) -> Result<bool> {
        self.0.cas_ref_logged(name, expected, new, ctx).await
    }
    async fn cas_ref_logged_chain(
        &self,
        name: &str,
        expected: Option<ChangesetId>,
        steps: &[RefStep],
    ) -> Result<usize> {
        self.0.cas_ref_logged_chain(name, expected, steps).await
    }
    async fn ref_log(
        &self,
        name: Option<&str>,
        before_seq: Option<u64>,
        limit: usize,
    ) -> Result<Vec<RefLogEntry>> {
        self.0.ref_log(name, before_seq, limit).await
    }
    async fn list_refs(&self) -> Result<BTreeMap<String, ChangesetId>> {
        self.0.list_refs().await
    }
    async fn list_derived(
        &self,
        kind: &str,
        after: Option<Hash>,
        limit: usize,
    ) -> Result<Vec<(Hash, Bytes)>> {
        timed!(
            self.1,
            "list_derived",
            limit,
            self.0.list_derived(kind, after, limit).await
        )
    }
    async fn delete_derived_kind(&self, kind: &str) -> Result<u64> {
        self.0.delete_derived_kind(kind).await
    }
    async fn get_derived(&self, kind: &str, key: &Hash) -> Result<Option<Bytes>> {
        timed!(
            self.1,
            "get_derived",
            1,
            self.0.get_derived(kind, key).await
        )
    }
    async fn get_derived_many(&self, kind: &str, keys: &[Hash]) -> Result<Vec<Option<Bytes>>> {
        timed!(
            self.1,
            "get_derived_many",
            keys.len(),
            self.0.get_derived_many(kind, keys).await
        )
    }
    async fn put_derived(&self, kind: &str, key: &Hash, value: Bytes) -> Result<()> {
        timed!(
            self.1,
            "put_derived",
            1,
            self.0.put_derived(kind, key, value).await
        )
    }
    async fn put_derived_many(&self, rows: &[DerivedRow]) -> Result<()> {
        timed!(
            self.1,
            "put_derived_many",
            rows.len(),
            self.0.put_derived_many(rows).await
        )
    }
    async fn put_changeset_with_derived(
        &self,
        cs: &Changeset,
        rows: &[DerivedRow],
    ) -> Result<ChangesetId> {
        self.0.put_changeset_with_derived(cs, rows).await
    }
    async fn schema_version(&self) -> Result<Option<u64>> {
        self.0.schema_version().await
    }
}

fn print_tally(phase: &str, elapsed: Duration, tally: &Tally) {
    println!("{phase}: {:.2} s", elapsed.as_secs_f64());
    let calls = tally.take();
    let total: u64 = calls.values().map(|c| c.0).sum();
    for (name, (n, items, d)) in calls {
        println!(
            "  {name:<18} {n:>8} calls {items:>9} items {:>8.2} s",
            d.as_secs_f64()
        );
    }
    println!("  {:<18} {total:>8} calls", "total");
}

async fn add_commits(repo: &Repo, n: usize) {
    let refs = repo.meta().list_refs().await.unwrap();
    let mut tip = *refs.values().next().expect("a ref");
    let path = repo
        .list_files(tip, "")
        .await
        .unwrap()
        .into_iter()
        .map(|f| f.path)
        .find(|p| p.as_str().matches('/').count() >= 2)
        .expect("a file two directories deep");
    for i in 0..n {
        let data = Bytes::from(format!(
            "gc_profile commit {i} at {}\n",
            leep_core::store::unix_now()
        ));
        let content = repo.put_blob(data.clone()).await.unwrap();
        let cs = Changeset {
            parents: vec![tip],
            author: "gc_profile".into(),
            timestamp: 1,
            message: format!("gc_profile {i}"),
            changes: BTreeMap::from([(
                path.clone(),
                leep_core::FileChange::Modify {
                    content,
                    kind: leep_core::FileKind::Regular,
                    size: data.len() as u64,
                },
            )]),
            extra: BTreeMap::new(),
        };
        tip = repo.create_changeset(cs).await.unwrap();
    }
    println!("stored {n} changesets changing {path}");
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let usage =
        "usage: gc_profile <meta url> <blob url> [condemn|collect|both] [--full] [--commits N]";
    let meta_url = args.first().expect(usage);
    let blob_url = args.get(1).expect(usage);
    let phase = args.get(2).map_or("both", String::as_str);
    let mark = if args.iter().any(|a| a == "--full") {
        Mark::Full
    } else {
        Mark::Incremental
    };
    let tally = Arc::new(Tally::default());
    let blobs: Arc<dyn BlobStore> = Arc::new(Blobs(
        leep_store::open_blob_store(blob_url).unwrap(),
        tally.clone(),
    ));
    let meta: Arc<dyn MetaStore> = Arc::new(Meta(
        leep_meta::open_meta_store(meta_url).await.unwrap(),
        tally.clone(),
    ));
    let repo = Repo::new(blobs, meta);
    if let Some(n) = args
        .iter()
        .position(|a| a == "--commits")
        .map(|i| args[i + 1].parse::<usize>().unwrap())
    {
        add_commits(&repo, n).await;
    }
    tally.take();
    let opts = GcOptions {
        fence: Duration::ZERO,
        unsafe_no_fence: true,
        ..GcOptions::default()
    };
    if phase == "condemn" || phase == "both" {
        let t = Instant::now();
        let r = gc::condemn_with(&repo, &opts, mark, &SystemClock)
            .await
            .unwrap();
        print_tally("condemn", t.elapsed(), &tally);
        println!("  {r:?}");
    }
    if phase == "collect" || phase == "both" {
        let t = Instant::now();
        let r = gc::collect_with(&repo, &opts, mark, &SystemClock)
            .await
            .unwrap();
        print_tally("collect", t.elapsed(), &tally);
        println!("  {r:?}");
    }
}
