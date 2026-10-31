//! Background merges of packs in the server (ADR 0068). Land batches write
//! a pack each, a few a second at full load, and a read that misses its
//! loose copy searches the packs one by one, so the server merges them as
//! they come: when more than `trigger_packs` packs are searched, the
//! geometric rule of `leep-server repack` ([`plan_merge`]) picks the small
//! ones, a walk of the recent history they hold picks delta bases (a
//! directory node against its previous version), and a planned repack of
//! exactly those packs, without loose objects, publishes the merged pack.
//!
//! One merge at a time, on a thread of its own at the lowest CPU priority
//! with one compression thread, so it takes what the land leader leaves.
//! Reads in this process stop searching the merged packs at once; they are
//! deleted a fence later, as `leep-server repack` deletes what it replaced,
//! so other processes reading them meanwhile (backups, a second server)
//! keep finding every object. Each step holds the store's repack lock or
//! lease, so a `leep-server repack` run beside it is refused or refuses
//! this, and neither deletes what the other still needs.

use std::collections::{HashSet, VecDeque};
use std::time::{Duration, Instant};

use leep_core::{Error, Hash, Result};
use leep_derive::Repo;
use leep_store::{
    plan_merge, FsBlobStore, ObjectBlobStore, PublishedObjectPack, PublishedPack, RepackMode,
    RepackOptions, RepackReport,
};

/// What the background repack does, and how often.
#[derive(Clone, Debug)]
pub struct BackgroundRepackOptions {
    /// How often it looks at the pack count and runs what is due.
    pub interval: Duration,
    /// It merges when more packs than this are searched.
    pub trigger_packs: usize,
    /// The geometric factor of [`plan_merge`].
    pub factor: u64,
    /// The pack count [`plan_merge`] merges down to.
    pub max_packs: usize,
    /// How long merged packs stay on disk after the merged pack is
    /// published, for readers in other processes. Must exceed the servers'
    /// request timeout, like `leep-server repack --fence`.
    pub fence: Duration,
    /// zstd level of the merged packs.
    pub level: i32,
    /// Changesets the walk for delta bases visits at most.
    pub walk_limit: usize,
    /// Merges whose cleanup may wait for its fence at once; past it no
    /// merge starts, which bounds the disk held by merged packs.
    pub max_pending: usize,
}

impl Default for BackgroundRepackOptions {
    fn default() -> Self {
        BackgroundRepackOptions {
            interval: Duration::from_secs(5),
            trigger_packs: 32,
            factor: 4,
            max_packs: 16,
            fence: Duration::from_secs(600),
            level: 3,
            walk_limit: 4096,
            max_pending: 256,
        }
    }
}

/// The stores that hold packs.
#[derive(Clone)]
pub enum PackStore {
    Fs(FsBlobStore),
    Object(ObjectBlobStore),
}

enum Pending {
    Fs(PublishedPack),
    Object(PublishedObjectPack),
}

impl PackStore {
    async fn pack_sizes(&self) -> Result<Vec<(String, u64)>> {
        match self {
            PackStore::Fs(s) => {
                let s = s.clone();
                tokio::task::spawn_blocking(move || s.pack_sizes())
                    .await
                    .map_err(|e| Error::Storage(format!("pack task failed: {e}")))?
            }
            PackStore::Object(s) => s.pack_sizes().await,
        }
    }

    /// The hints for a merge of `plan`: a walk of recent history listing
    /// the objects of those packs, each with its previous version.
    async fn hints(
        &self,
        repo: &Repo,
        plan: &HashSet<String>,
        limit: usize,
    ) -> Result<leep_core::intern::PackHints> {
        let order = match self {
            PackStore::Fs(s) => {
                let wanted = s.packed_in(plan)?;
                let fs = s.clone();
                let loose = move |h: &Hash| fs.is_loose(h);
                leep_derive::pack_order::pack_order_where(repo, &wanted, &loose, limit).await?
            }
            PackStore::Object(s) => {
                let wanted = s.packed_in(plan).await?;
                // A loose object costs a request to see: walk packed
                // history only.
                let loose = |_: &Hash| false;
                leep_derive::pack_order::pack_order_where(repo, &wanted, &loose, limit).await?
            }
        };
        Ok(order.objects)
    }

    async fn publish(
        &self,
        hints: leep_core::intern::PackHints,
        opts: RepackOptions,
    ) -> Result<Pending> {
        match self {
            PackStore::Fs(s) => {
                let s = s.clone();
                let mut p = on_low_thread(move || s.repack_publish_blocking(hints, opts)).await?;
                p.unlock();
                Ok(Pending::Fs(p))
            }
            PackStore::Object(s) => {
                let mut p = s.repack_publish(hints, opts).await?;
                p.unlock().await;
                Ok(Pending::Object(p))
            }
        }
    }
}

impl Pending {
    fn report(&self) -> &RepackReport {
        match self {
            Pending::Fs(p) => p.report(),
            Pending::Object(p) => p.report(),
        }
    }

    /// Take the store's repack lock or lease again for the cleanup.
    async fn relock(&mut self) -> Result<()> {
        match self {
            Pending::Fs(p) => p.relock(),
            Pending::Object(p) => p.relock().await,
        }
    }

    async fn finish(self) -> Result<RepackReport> {
        match self {
            Pending::Fs(p) => on_low_thread(move || p.finish_blocking()).await,
            Pending::Object(p) => p.finish().await,
        }
    }
}

/// Run `f` on a new thread at the lowest CPU priority.
async fn on_low_thread<T: Send + 'static>(
    f: impl FnOnce() -> Result<T> + Send + 'static,
) -> Result<T> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    std::thread::Builder::new()
        .name("leep-repack".into())
        .spawn(move || {
            leep_store::lower_thread_priority();
            let _ = tx.send(f());
        })
        .map_err(|e| Error::Storage(format!("starting the repack thread: {e}")))?;
    rx.await
        .map_err(|_| Error::Storage("the repack thread stopped".into()))?
}

/// The background repack of one store: [`BackgroundRepack::spawn`] runs it
/// for the life of the process; tests call [`BackgroundRepack::tick`].
pub struct BackgroundRepack {
    store: PackStore,
    repo: Repo,
    opts: BackgroundRepackOptions,
    pending: VecDeque<(Instant, Pending)>,
}

/// What one [`BackgroundRepack::tick`] did.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct TickReport {
    /// The merge published, if one ran.
    pub merged: Option<RepackReport>,
    /// Cleanups of earlier merges finished.
    pub finished: usize,
}

impl BackgroundRepack {
    pub fn new(store: PackStore, repo: Repo, opts: BackgroundRepackOptions) -> Self {
        metrics::counter!("leep_repack_background_runs_total", "outcome" => "merged").increment(0);
        metrics::counter!("leep_repack_background_runs_total", "outcome" => "error").increment(0);
        metrics::counter!("leep_repack_background_packs_merged_total").increment(0);
        BackgroundRepack {
            store,
            repo,
            opts,
            pending: VecDeque::new(),
        }
    }

    /// Merges whose cleanup waits for its fence.
    pub fn pending(&self) -> usize {
        self.pending.len()
    }

    /// Run for the life of the process, one tick per interval.
    pub fn spawn(mut self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(self.opts.interval).await;
                if let Err(e) = self.tick().await {
                    tracing::warn!(error = %e, "background repack failed; trying again later");
                }
            }
        })
    }

    /// Finish the cleanups whose fence has passed, then merge if more than
    /// `trigger_packs` packs are searched.
    pub async fn tick(&mut self) -> Result<TickReport> {
        let mut report = TickReport {
            finished: self.finish_due(false).await?,
            ..TickReport::default()
        };
        report.merged = self.merge().await?;
        Ok(report)
    }

    /// Finish every pending cleanup now, fence or not: for tests, and for a
    /// caller that knows no other process reads the store.
    pub async fn finish_all(&mut self) -> Result<usize> {
        self.finish_due(true).await
    }

    async fn finish_due(&mut self, all: bool) -> Result<usize> {
        let mut n = 0;
        while let Some((due, _)) = self.pending.front() {
            if !all && *due > Instant::now() {
                break;
            }
            let (due, mut p) = self.pending.pop_front().expect("peeked");
            // A `leep-server repack` holds the store: try again next time.
            // Its own cleanup may delete the same packs, which is fine.
            match p.relock().await {
                Ok(()) => {}
                Err(Error::Conflict(m)) => {
                    tracing::debug!(reason = %m, "background repack cleanup deferred");
                    self.pending.push_front((due, p));
                    break;
                }
                Err(e) => {
                    self.pending.push_front((due, p));
                    return Err(e);
                }
            }
            let started = Instant::now();
            match p.finish().await {
                Ok(r) => {
                    n += 1;
                    metrics::histogram!("leep_repack_background_cleanup_seconds")
                        .record(started.elapsed().as_secs_f64());
                    tracing::debug!(
                        old_packs_removed = r.old_packs_removed,
                        "background repack cleaned up"
                    );
                }
                Err(e) => {
                    // The merged packs stay on disk, unsearched here; the
                    // next `leep-server repack` merges them away.
                    tracing::warn!(error = %e, "background repack cleanup failed");
                }
            }
        }
        Ok(n)
    }

    async fn merge(&mut self) -> Result<Option<RepackReport>> {
        if self.pending.len() >= self.opts.max_pending {
            return Ok(None);
        }
        let sizes = self.store.pack_sizes().await?;
        metrics::gauge!("leep_blob_packs").set(sizes.len() as f64);
        if sizes.len() <= self.opts.trigger_packs {
            return Ok(None);
        }
        let plan: Vec<String> = plan_merge(&sizes, 0, self.opts.factor, self.opts.max_packs);
        if plan.len() < 2 {
            return Ok(None);
        }
        let started = Instant::now();
        let planned: HashSet<String> = plan.iter().cloned().collect();
        let objects: u64 = sizes
            .iter()
            .filter(|(id, _)| planned.contains(id))
            .map(|(_, n)| *n)
            .sum();
        let limit = self.opts.walk_limit.min(objects as usize).max(1);
        let result = async {
            let hints = self.store.hints(&self.repo, &planned, limit).await?;
            let opts = RepackOptions {
                level: self.opts.level,
                mode: RepackMode::Planned { loose: false },
                packs: plan,
                threads: 1,
                ..RepackOptions::default()
            };
            self.store.publish(hints, opts).await
        }
        .await;
        let pending = match result {
            Ok(p) => p,
            // Another repack holds the store; nothing was done.
            Err(Error::Conflict(m)) => {
                tracing::debug!(reason = %m, "background repack skipped");
                return Ok(None);
            }
            Err(e) => {
                metrics::counter!("leep_repack_background_runs_total", "outcome" => "error")
                    .increment(1);
                return Err(e);
            }
        };
        let took = started.elapsed();
        let report = pending.report().clone();
        metrics::counter!("leep_repack_background_runs_total", "outcome" => "merged").increment(1);
        metrics::counter!("leep_repack_background_packs_merged_total")
            .increment(report.packs_merged);
        metrics::histogram!("leep_repack_background_seconds").record(took.as_secs_f64());
        tracing::info!(
            packs_merged = report.packs_merged,
            objects = report.objects,
            deltas = report.deltas,
            pack_bytes = report.pack_bytes,
            seconds = took.as_secs_f64(),
            "background repack merged packs"
        );
        self.pending
            .push_back((Instant::now() + self.opts.fence, pending));
        Ok(Some(report))
    }
}
