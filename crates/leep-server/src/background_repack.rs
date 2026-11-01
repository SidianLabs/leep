//! Background merges of packs in the server (ADR 0068). Land batches write
//! a pack each, a few a second at full load, and a read that misses its
//! loose copy searches the packs one by one, so the server merges them as
//! they come.
//!
//! Merges are tiered, so the work stays linear in what is written: a pack's
//! class is `floor(log_fanout(live objects))`, and while more than
//! `trigger_packs` packs are searched, the packs of the lowest class that
//! holds at least `fanout` of them are merged (the smallest first, at most
//! `max_merge_packs` and `max_merge_bytes`). A merge of `fanout` packs of a
//! class makes a pack of a higher class, so an object is rewritten at most
//! once per class, and after the merges every class holds fewer than
//! `fanout` packs: at most `trigger_packs`, or `fanout - 1` per class in
//! use, are searched. Packs over `max_pack_bytes` are left to
//! `leep-server repack`, which also finds delta bases by walking history;
//! the background merge does not walk (whole frames, recompressed), so it
//! costs only reading and compressing the packs it merges. The cumulative
//! rule of `plan_merge` would instead fold the largest pack into every few
//! merges, rewriting it again and again.
//!
//! One merge at a time, on a thread of its own at the lowest CPU priority
//! with one compression thread, so it takes what the land leader leaves.
//! Reads in this process stop searching the merged packs at once; they are
//! deleted a fence later, as `leep-server repack` deletes what it replaced,
//! so other processes reading them meanwhile (backups, a second server)
//! keep finding every object. Each step holds the store's repack lock or
//! lease, so a `leep-server repack` run beside it is refused or refuses
//! this, and neither deletes what the other still needs.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use leep_core::{Error, Result};
use leep_store::{
    FsBlobStore, ObjectBlobStore, PackSize, PublishedObjectPack, PublishedPack, RepackMode,
    RepackOptions, RepackReport,
};

/// What the background repack does, and how often.
#[derive(Clone, Debug)]
pub struct BackgroundRepackOptions {
    /// How often it looks at the pack count and runs what is due.
    pub interval: Duration,
    /// It merges while more packs than this are searched.
    pub trigger_packs: usize,
    /// Packs of one class merged together, at least; the base of the
    /// classes.
    pub fanout: u64,
    /// Packs one merge takes at most.
    pub max_merge_packs: usize,
    /// Bytes of packs one merge takes at most (past its first two packs).
    pub max_merge_bytes: u64,
    /// Packs larger than this are not merged in the background.
    pub max_pack_bytes: u64,
    /// How long merged packs stay on disk after the merged pack is
    /// published, for readers in other processes. Must exceed the servers'
    /// request timeout, like `leep-server repack --fence`.
    pub fence: Duration,
    /// zstd level of the merged packs.
    pub level: i32,
    /// Merges whose cleanup may wait for its fence at once, and the bytes
    /// of the packs they will delete; past either no merge starts, which
    /// bounds the disk held twice.
    pub max_pending: usize,
    pub max_pending_bytes: u64,
    /// Merges one tick runs at most, back to back.
    pub max_merges_per_tick: usize,
}

impl Default for BackgroundRepackOptions {
    fn default() -> Self {
        BackgroundRepackOptions {
            interval: Duration::from_secs(5),
            trigger_packs: 32,
            fanout: 4,
            max_merge_packs: 64,
            max_merge_bytes: 1 << 30,
            max_pack_bytes: 256 << 20,
            fence: Duration::from_secs(360),
            level: 3,
            max_pending: 256,
            max_pending_bytes: 4 << 30,
            max_merges_per_tick: 32,
        }
    }
}

/// The class of a pack of `objects` live objects: `floor(log_fanout)`.
fn class(objects: u64, fanout: u64) -> u32 {
    objects.max(1).ilog(fanout.max(2))
}

/// The packs to merge next, or none: the lowest class with at least
/// `fanout` packs no larger than `max_pack_bytes`, smallest first, within
/// `max_merge_packs` and `max_merge_bytes`. See the module comment for why
/// this bounds both the work and the pack count.
pub fn plan_tiered(sizes: &[PackSize], opts: &BackgroundRepackOptions) -> Vec<String> {
    let fanout = opts.fanout.max(2);
    let mut small: Vec<&PackSize> = sizes
        .iter()
        .filter(|p| p.bytes <= opts.max_pack_bytes)
        .collect();
    small.sort_by(|a, b| a.objects.cmp(&b.objects).then_with(|| a.id.cmp(&b.id)));
    let mut start = 0;
    while start < small.len() {
        let c = class(small[start].objects, fanout);
        let end = start
            + small[start..]
                .iter()
                .take_while(|p| class(p.objects, fanout) == c)
                .count();
        if (end - start) as u64 >= fanout {
            let mut plan = Vec::new();
            let mut bytes = 0u64;
            for p in &small[start..end] {
                if plan.len() >= opts.max_merge_packs.max(2)
                    || (plan.len() >= 2 && bytes + p.bytes > opts.max_merge_bytes)
                {
                    break;
                }
                bytes += p.bytes;
                plan.push(p.id.clone());
            }
            return plan;
        }
        start = end;
    }
    Vec::new()
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
    async fn pack_sizes(&self) -> Result<Vec<PackSize>> {
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

    async fn publish(&self, opts: RepackOptions) -> Result<Pending> {
        let hints = leep_core::intern::PackHints::default();
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

/// A merge whose cleanup waits for its fence.
struct Waiting {
    due: Instant,
    /// Bytes of the packs the cleanup deletes.
    bytes: u64,
    pending: Pending,
}

/// The background repack of one store: [`BackgroundRepack::spawn`] runs it
/// for the life of the process; tests call [`BackgroundRepack::tick`].
pub struct BackgroundRepack {
    store: PackStore,
    opts: BackgroundRepackOptions,
    pending: VecDeque<Waiting>,
}

/// What one [`BackgroundRepack::tick`] did.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct TickReport {
    /// The merges published, in order.
    pub merged: Vec<RepackReport>,
    /// Cleanups of earlier merges finished.
    pub finished: usize,
    /// Packs reads search after the tick.
    pub packs: usize,
}

impl BackgroundRepack {
    pub fn new(store: PackStore, opts: BackgroundRepackOptions) -> Self {
        metrics::counter!("leep_repack_background_runs_total", "outcome" => "merged").increment(0);
        metrics::counter!("leep_repack_background_runs_total", "outcome" => "error").increment(0);
        metrics::counter!("leep_repack_background_packs_merged_total").increment(0);
        metrics::counter!("leep_repack_background_objects_written_total").increment(0);
        BackgroundRepack {
            store,
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

    /// Finish the cleanups whose fence has passed, then merge while more
    /// than `trigger_packs` packs are searched and a class has `fanout`.
    pub async fn tick(&mut self) -> Result<TickReport> {
        let mut report = TickReport {
            finished: self.finish_due(false).await?,
            ..TickReport::default()
        };
        loop {
            let sizes = self.store.pack_sizes().await?;
            report.packs = sizes.len();
            metrics::gauge!("leep_blob_packs").set(sizes.len() as f64);
            if report.merged.len() >= self.opts.max_merges_per_tick
                || sizes.len() <= self.opts.trigger_packs
            {
                break;
            }
            match self.merge(&sizes).await? {
                Some(r) => report.merged.push(r),
                None => break,
            }
        }
        Ok(report)
    }

    /// Finish every pending cleanup now, fence or not: for tests, and for a
    /// caller that knows no other process reads the store.
    pub async fn finish_all(&mut self) -> Result<usize> {
        self.finish_due(true).await
    }

    async fn finish_due(&mut self, all: bool) -> Result<usize> {
        let mut n = 0;
        while let Some(w) = self.pending.front() {
            if !all && w.due > Instant::now() {
                break;
            }
            let mut w = self.pending.pop_front().expect("peeked");
            // A `leep-server repack` holds the store: try again next time.
            // Its own cleanup may delete the same packs, which is fine.
            match w.pending.relock().await {
                Ok(()) => {}
                Err(Error::Conflict(m)) => {
                    tracing::debug!(reason = %m, "background repack cleanup deferred");
                    self.pending.push_front(w);
                    break;
                }
                Err(e) => {
                    self.pending.push_front(w);
                    return Err(e);
                }
            }
            let started = Instant::now();
            match w.pending.finish().await {
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

    async fn merge(&mut self, sizes: &[PackSize]) -> Result<Option<RepackReport>> {
        let pending_bytes: u64 = self.pending.iter().map(|w| w.bytes).sum();
        if self.pending.len() >= self.opts.max_pending
            || pending_bytes >= self.opts.max_pending_bytes
        {
            return Ok(None);
        }
        let plan = plan_tiered(sizes, &self.opts);
        if plan.len() < 2 {
            return Ok(None);
        }
        let bytes: u64 = sizes
            .iter()
            .filter(|p| plan.contains(&p.id))
            .map(|p| p.bytes)
            .sum();
        let started = Instant::now();
        let opts = RepackOptions {
            level: self.opts.level,
            mode: RepackMode::Planned { loose: false },
            packs: plan,
            threads: 1,
            ..RepackOptions::default()
        };
        let pending = match self.store.publish(opts).await {
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
        metrics::counter!("leep_repack_background_objects_written_total").increment(report.objects);
        metrics::histogram!("leep_repack_background_seconds").record(took.as_secs_f64());
        tracing::info!(
            packs_merged = report.packs_merged,
            objects = report.objects,
            pack_bytes = report.pack_bytes,
            seconds = took.as_secs_f64(),
            "background repack merged packs"
        );
        self.pending.push_back(Waiting {
            due: Instant::now() + self.opts.fence,
            bytes,
            pending,
        });
        Ok(Some(report))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn packs(objects: &[u64]) -> Vec<PackSize> {
        objects
            .iter()
            .enumerate()
            .map(|(i, n)| PackSize {
                id: format!("{i:064x}"),
                objects: *n,
                bytes: n * 100,
            })
            .collect()
    }

    /// Simulated sustained arrival of land packs: the searched count stays
    /// bounded and each object is rewritten at most about once per class.
    #[test]
    fn tiered_merges_keep_the_count_bounded_and_the_work_linear() {
        let opts = BackgroundRepackOptions::default();
        let mut sizes: Vec<PackSize> = Vec::new();
        let (mut written, mut rewritten, mut most) = (0u64, 0u64, 0usize);
        let mut next = 0u64;
        for step in 0..20_000u64 {
            // A land pack of 100 to 1,500 objects, 2 KB each.
            let n = 100 + (step * 7919) % 1_400;
            sizes.push(PackSize {
                id: format!("{next:064x}"),
                objects: n,
                bytes: n * 2_000,
            });
            next += 1;
            written += n;
            while sizes.len() > opts.trigger_packs {
                let plan = plan_tiered(&sizes, &opts);
                if plan.len() < 2 {
                    break;
                }
                let (merged, kept): (Vec<_>, Vec<_>) =
                    sizes.drain(..).partition(|p| plan.contains(&p.id));
                sizes = kept;
                let objects: u64 = merged.iter().map(|p| p.objects).sum();
                rewritten += objects;
                sizes.push(PackSize {
                    id: format!("{next:064x}"),
                    objects,
                    bytes: merged.iter().map(|p| p.bytes).sum(),
                });
                next += 1;
            }
            let small = sizes
                .iter()
                .filter(|p| p.bytes <= opts.max_pack_bytes)
                .count();
            most = most.max(small);
        }
        let ratio = rewritten as f64 / written as f64;
        eprintln!("most searched {most}, rewritten per written {ratio:.2}");
        // Packs over max_pack_bytes are left to `leep-server repack`; the
        // ones the background merges stay few.
        assert!(
            most <= opts.trigger_packs + 1,
            "{most} packs searched at most"
        );
        assert!(
            ratio <= 6.0,
            "{ratio:.2} objects rewritten per object written"
        );
    }

    #[test]
    fn a_plan_takes_the_lowest_full_class_smallest_first() {
        let opts = BackgroundRepackOptions::default();
        // Classes (base 4): 16..63 is 2, 64..255 is 3, 1,000 is 4.
        let sizes = packs(&[1000, 1000, 1000, 70, 80, 90, 30, 40]);
        assert!(plan_tiered(&sizes, &opts).is_empty(), "no class has 4");
        let sizes = packs(&[1000, 1000, 1000, 1000, 70, 80, 90, 100, 30]);
        let plan = plan_tiered(&sizes, &opts);
        let want: Vec<String> = sizes[4..8].iter().map(|p| p.id.clone()).collect();
        assert_eq!(plan, want);
        let capped = BackgroundRepackOptions {
            max_merge_packs: 2,
            ..BackgroundRepackOptions::default()
        };
        assert_eq!(plan_tiered(&sizes, &capped).len(), 2);
        let big = BackgroundRepackOptions {
            max_pack_bytes: 50 * 100,
            ..BackgroundRepackOptions::default()
        };
        assert!(
            plan_tiered(&sizes, &big).is_empty(),
            "large packs are left alone"
        );
    }
}
