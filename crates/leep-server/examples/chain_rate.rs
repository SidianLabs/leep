//! Time how fast `history-tree-v4` follows one hot chain: the rate a single
//! ref built by lands can be indexed at, whatever the number of indexers
//! (ADR 0058, section 8), as measured in docs/PERFORMANCE.md ("Hot chain
//! rate").
//!
//! ```sh
//! cargo run --release -p leep-server --example chain_rate -- <meta url> <blob url> \
//!     [--shape git|linux] [--base N] [--chain N] [--jobs N]
//! ```
//! Builds a synthetic history (`leep-derive/benches/support/wide_history.rs`)
//! shaped like git/git's tree (about 4,300 files) or late Linux's (about
//! 80,000, directories of thousands), `--base` changesets after its import
//! (default 1,000), and derives its index, which warms the process's caches
//! as a running indexer's are. Then it grows two chains of `--chain`
//! changesets (default 1,000) off the head, each changeset editing 1 to 3
//! files as an agent's land does, and times:
//!
//! - *follow*: the first chain's changesets indexed one at a time, oldest
//!   first, each as soon as it exists: an indexer keeping up with lands;
//! - *catch-up*: the second chain indexed in one derivation of its tip, on
//!   `--jobs` jobs: an indexer that fell behind and catches up;
//! - *log filter, follow*: `log-filter-v1` over the first chain, one at a
//!   time.
//!
//! For each it prints changesets per second, wall and CPU milliseconds per
//! changeset (CPU of the whole process, so it includes the store's work on
//! `mem://`), and store round trips per changeset. Point it at stores no
//! server is using: it writes a history and its rows.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use bytes::Bytes;
use leep_core::{
    Changeset, ChangesetId, DerivedRow, Hash, MetaStore, RefLogEntry, RefStep, RefUpdateContext,
    Result,
};
use leep_derive::derived::{self, DerivedKind, HistoryTree, LogFilterIndex};
use leep_derive::Repo;

#[path = "../../leep-derive/benches/support/wide_history.rs"]
mod wide_history;

/// Counts metadata round trips.
struct Counted(Arc<dyn MetaStore>, Arc<AtomicU64>);

impl Counted {
    fn tick(&self) {
        self.1.fetch_add(1, Ordering::Relaxed);
    }
}

#[async_trait]
impl MetaStore for Counted {
    async fn put_changeset(&self, cs: &Changeset) -> Result<ChangesetId> {
        self.tick();
        self.0.put_changeset(cs).await
    }
    async fn get_changeset(&self, id: &ChangesetId) -> Result<Option<Changeset>> {
        self.tick();
        self.0.get_changeset(id).await
    }
    async fn get_changesets(&self, ids: &[ChangesetId]) -> Result<Vec<Option<Changeset>>> {
        self.tick();
        self.0.get_changesets(ids).await
    }
    async fn list_changesets(
        &self,
        after: Option<ChangesetId>,
        limit: usize,
    ) -> Result<Vec<ChangesetId>> {
        self.0.list_changesets(after, limit).await
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
    async fn find_lands(
        &self,
        name: &str,
        ids: &[leep_core::LandId],
    ) -> Result<Vec<Option<leep_core::LandRecord>>> {
        self.0.find_lands(name, ids).await
    }
    async fn ref_log(
        &self,
        name: Option<&str>,
        before_seq: Option<u64>,
        limit: usize,
    ) -> Result<Vec<RefLogEntry>> {
        self.0.ref_log(name, before_seq, limit).await
    }
    async fn list_refs(&self) -> Result<std::collections::BTreeMap<String, ChangesetId>> {
        self.0.list_refs().await
    }
    async fn list_derived(
        &self,
        kind: &str,
        after: Option<Hash>,
        limit: usize,
    ) -> Result<Vec<(Hash, Bytes)>> {
        self.0.list_derived(kind, after, limit).await
    }
    async fn get_derived(&self, kind: &str, key: &Hash) -> Result<Option<Bytes>> {
        self.tick();
        self.0.get_derived(kind, key).await
    }
    async fn get_derived_many(&self, kind: &str, keys: &[Hash]) -> Result<Vec<Option<Bytes>>> {
        self.tick();
        self.0.get_derived_many(kind, keys).await
    }
    async fn put_derived(&self, kind: &str, key: &Hash, value: Bytes) -> Result<()> {
        self.tick();
        self.0.put_derived(kind, key, value).await
    }
    async fn put_derived_many(&self, rows: &[DerivedRow]) -> Result<()> {
        self.tick();
        self.0.put_derived_many(rows).await
    }
    async fn put_changeset_with_derived(
        &self,
        cs: &Changeset,
        rows: &[DerivedRow],
    ) -> Result<ChangesetId> {
        self.tick();
        self.0.put_changeset_with_derived(cs, rows).await
    }
    async fn put_changesets_with_derived(
        &self,
        changesets: &[Changeset],
        rows: &[DerivedRow],
    ) -> Result<()> {
        self.tick();
        self.0.put_changesets_with_derived(changesets, rows).await
    }
}

/// CPU seconds of this process so far (user and system), from
/// /proc/self/stat; 0 where it cannot be read.
fn cpu_seconds() -> f64 {
    let stat = std::fs::read_to_string("/proc/self/stat").unwrap_or_default();
    // Fields after the command name, which is in parentheses.
    let rest = stat.rsplit_once(')').map_or("", |(_, r)| r);
    let f: Vec<&str> = rest.split_whitespace().collect();
    let ticks = |i: usize| f.get(i).and_then(|v| v.parse::<f64>().ok()).unwrap_or(0.0);
    // utime and stime are fields 14 and 15; `rest` starts at field 3.
    (ticks(11) + ticks(12)) / 100.0
}

/// Times `work` over `n` changesets and prints one line.
async fn timed<F: std::future::Future<Output = ()>>(
    label: &str,
    n: usize,
    trips: &AtomicU64,
    work: F,
) {
    let (t, cpu, rt) = (Instant::now(), cpu_seconds(), trips.load(Ordering::Relaxed));
    work.await;
    let wall = t.elapsed().as_secs_f64();
    let cpu = cpu_seconds() - cpu;
    let rt = trips.load(Ordering::Relaxed) - rt;
    println!(
        "{label:<22} {n:>6} changesets  {:>7.1}/s  wall {:>6.2} ms  cpu {:>6.2} ms  {:>5.1} round trips per changeset",
        n as f64 / wall,
        wall * 1000.0 / n as f64,
        cpu * 1000.0 / n as f64,
        rt as f64 / n as f64
    );
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let usage = "usage: chain_rate <meta url> <blob url> [--shape git|linux] [--base N] [--chain N] [--jobs N]";
    let meta_url = args.first().expect(usage);
    let blob_url = args.get(1).expect(usage);
    let flag = |name: &str| {
        args.iter()
            .position(|a| a == name)
            .map(|i| args.get(i + 1).expect(usage).clone())
    };
    let base: usize = flag("--base").map_or(1000, |v| v.parse().expect(usage));
    let chain: usize = flag("--chain").map_or(1000, |v| v.parse().expect(usage));
    let spec = match flag("--shape").as_deref().unwrap_or("git") {
        "git" => wide_history::Spec::git_like(base),
        "linux" => wide_history::Spec::linux_like(base),
        _ => panic!("{usage}"),
    };
    let trips = Arc::new(AtomicU64::new(0));
    let blobs = leep_store::open_blob_store(blob_url).unwrap();
    let raw = leep_meta::open_meta_store(meta_url).await.unwrap();
    let meta: Arc<dyn MetaStore> = Arc::new(Counted(raw, trips.clone()));
    let mut repo = Repo::new(blobs, meta);
    if let Some(jobs) = flag("--jobs") {
        repo = repo.with_derive_jobs(jobs.parse().expect(usage));
    }
    let files = spec.wide_dirs * spec.wide_min
        + spec.wide_step * spec.wide_dirs * spec.wide_dirs.saturating_sub(1) / 2
        + spec.deep.0 * spec.deep.1 * spec.deep.2;
    println!(
        "shape {:?}: {files} files, {base} changesets after the import, chains of {chain}, {} jobs",
        flag("--shape").unwrap_or_else(|| "git".into()),
        repo.derive_jobs()
    );

    let t = Instant::now();
    let head = wide_history::build(&repo, &spec).await;
    println!("built the base in {:.1} s", t.elapsed().as_secs_f64());
    let t = Instant::now();
    derived::derive::<HistoryTree>(&repo, head).await.unwrap();
    derived::derive::<LogFilterIndex>(&repo, head)
        .await
        .unwrap();
    println!(
        "indexed the base in {:.1} s (warms the caches)",
        t.elapsed().as_secs_f64()
    );

    let a = wide_history::agent_chain(&repo, &spec, head, chain, 1).await;
    let b = wide_history::agent_chain(&repo, &spec, head, chain, 2).await;
    timed("history, follow", chain, &trips, async {
        for &id in &a {
            derived::derive::<HistoryTree>(&repo, id).await.unwrap();
        }
    })
    .await;
    timed("history, catch-up", chain, &trips, async {
        derived::derive::<HistoryTree>(&repo, *b.last().unwrap())
            .await
            .unwrap();
    })
    .await;
    timed("log filter, follow", chain, &trips, async {
        for &id in &a {
            derived::derive::<LogFilterIndex>(&repo, id).await.unwrap();
        }
    })
    .await;
    for (name, id) in [("a", a.last()), ("b", b.last())] {
        let id = *id.unwrap();
        assert!(repo
            .meta()
            .get_derived(HistoryTree::NAME, &id.0)
            .await
            .unwrap()
            .is_some());
        println!("chain {name} tip {id}");
    }
}
