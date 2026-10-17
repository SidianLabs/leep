//! Time path-history derivation (`history-tree-v3`) against real stores and
//! count the store calls it makes, as measured in docs/PERFORMANCE.md
//! ("Path history on Linux").
//!
//! ```sh
//! cargo run --release -p leep-server --example history_profile -- <meta url> <blob url> <rev> [--reset]
//! ```
//! `--reset` first deletes every `history-tree-v3` and `history-node-v3` row,
//! so only point it at stores no server is using. Prints progress every 10 s
//! and, at the end, changesets per second, peak RSS and calls per store method.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use leep_core::{
    BlobMeta, BlobStore, Changeset, ChangesetId, DerivedRow, Hash, MetaStore, RefLogEntry, RefStep,
    RefUpdateContext, Result,
};
use leep_derive::derived::{self, DerivedKind, HistoryTree, HISTORY_NODE_KIND};
use leep_derive::Repo;

/// Calls and time per store method.
#[derive(Default)]
struct Tally(
    Mutex<BTreeMap<&'static str, (u64, u64, Duration)>>,
    AtomicU64,
);

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
        let t = Instant::now();
        let r = self.0.get_derived(kind, key).await;
        self.1
            .add(intern(format!("get_derived {kind}")), 1, t.elapsed());
        r
    }
    async fn get_derived_many(&self, kind: &str, keys: &[Hash]) -> Result<Vec<Option<Bytes>>> {
        let t = Instant::now();
        let r = self.0.get_derived_many(kind, keys).await;
        self.1.add(
            intern(format!("get_derived_many {kind}")),
            keys.len(),
            t.elapsed(),
        );
        r
    }
    async fn put_derived(&self, kind: &str, key: &Hash, value: Bytes) -> Result<()> {
        if kind == HistoryTree::NAME {
            self.1 .1.fetch_add(1, Ordering::Relaxed);
        }
        timed!(
            self.1,
            "put_derived",
            1,
            self.0.put_derived(kind, key, value).await
        )
    }
    async fn put_derived_many(&self, rows: &[DerivedRow]) -> Result<()> {
        let trees = rows.iter().filter(|r| r.kind == HistoryTree::NAME).count();
        self.1 .1.fetch_add(trees as u64, Ordering::Relaxed);
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

/// A static name for a tally line, made once per distinct name.
fn intern(name: String) -> &'static str {
    static NAMES: Mutex<BTreeMap<String, &'static str>> = Mutex::new(BTreeMap::new());
    let mut m = NAMES.lock().unwrap();
    if let Some(n) = m.get(&name) {
        return n;
    }
    let n: &'static str = Box::leak(name.clone().into_boxed_str());
    m.insert(name, n);
    n
}

fn print_tally(tally: &Tally) {
    let calls = tally.take();
    for (name, (n, items, d)) in calls {
        println!(
            "  {name:<34} {n:>8} calls {items:>9} items {:>8.2} s",
            d.as_secs_f64()
        );
    }
}

/// A field of /proc/self/status in KiB (`VmRSS`, `VmHWM`).
fn status_kib(field: &str) -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .unwrap_or_default()
        .lines()
        .find_map(|l| l.strip_prefix(field)?.strip_prefix(':'))
        .and_then(|v| v.trim().trim_end_matches(" kB").parse().ok())
        .unwrap_or(0)
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let usage = "usage: history_profile <meta url> <blob url> <rev> [--reset]";
    let meta_url = args.first().expect(usage);
    let blob_url = args.get(1).expect(usage);
    let rev = args.get(2).expect(usage);
    let tally = Arc::new(Tally::default());
    let blobs: Arc<dyn BlobStore> = Arc::new(Blobs(
        leep_store::open_blob_store(blob_url).unwrap(),
        tally.clone(),
    ));
    let meta: Arc<dyn MetaStore> = Arc::new(Meta(
        leep_meta::open_meta_store(meta_url).await.unwrap(),
        tally.clone(),
    ));
    let repo = Repo::new(blobs, meta.clone());
    let head = repo.resolve(rev).await.unwrap();
    if args.iter().any(|a| a == "--reset") {
        let t = meta.delete_derived_kind(HistoryTree::NAME).await.unwrap();
        let n = meta.delete_derived_kind(HISTORY_NODE_KIND).await.unwrap();
        println!("deleted {t} tree rows, {n} node rows");
    }
    tally.take();
    let started = Instant::now();
    let progress = {
        let tally = tally.clone();
        tokio::spawn(async move {
            let mut last = (0, Instant::now());
            loop {
                tokio::time::sleep(Duration::from_secs(10)).await;
                let n = tally.1.load(Ordering::Relaxed);
                let rate = (n - last.0) as f64 / last.1.elapsed().as_secs_f64();
                println!(
                    "{:>7.1} s  {n:>8} changesets  {rate:>7.1}/s  rss {} MiB",
                    started.elapsed().as_secs_f64(),
                    status_kib("VmRSS") / 1024
                );
                last = (n, Instant::now());
            }
        })
    };
    derived::derive::<HistoryTree>(&repo, head).await.unwrap();
    progress.abort();
    let took = started.elapsed().as_secs_f64();
    let n = tally.1.load(Ordering::Relaxed);
    println!(
        "derived {n} changesets in {took:.1} s: {:.1}/s, peak rss {} MiB",
        n as f64 / took,
        status_kib("VmHWM") / 1024
    );
    print_tally(&tally);
}
