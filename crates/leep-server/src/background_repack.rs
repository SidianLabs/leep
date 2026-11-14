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
//! A merge retires the packs it merged durably (ADR 0080): no process
//! searches or plans them again, this one after a restart included. They
//! are deleted a fence later, as `leep-server repack` deletes what it
//! replaced, so other processes reading them meanwhile (backups, a second
//! server) keep finding every object: by the merge's cleanup while this
//! process lives, and otherwise by the sweep that runs at start and at the
//! start of every tick. Each step holds the store's repack lock or lease,
//! so a `leep-server repack` run beside it is refused or refuses this, and
//! neither deletes what the other still needs.
//!
//! The merges only copy frames. Given a [`Repo`] to walk
//! ([`BackgroundRepack::with_repo`]), the same task also compresses new
//! file versions against their previous ones (ADR 0100), as `leep-server
//! repack` does: when loose objects, or packs written whole by land batches
//! and imports, pass a count or a size (or have waited `max_age`), it walks
//! the history they belong to and runs an incremental repack that writes
//! them as deltas, the whole frames included. On an object store it runs a
//! full repack instead, at most once per `object_store_interval`. Whole
//! packs and delta packs are merged apart, so a whole frame is never hidden
//! in a delta pack before it is compressed. Lands never wait for any of it.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use leep_core::intern::PackHints;
use leep_core::{BlobStore, Error, Hash, Result};
use leep_derive::Repo;
use leep_store::{
    FsBlobStore, ObjectBlobStore, PackDetail, PackSize, PublishedObjectPack, PublishedPack,
    RepackMode, RepackOptions, RepackReport, SweepReport,
};
use tokio::time::Instant;

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
    /// The cap on bytes waiting for cleanup is also this share of the live
    /// store (pack and loose bytes), but at least `min_pending_bytes`: a
    /// small store is not held at twice its size for a fence after every
    /// burst of merges, and `max_pending_bytes` still bounds a large one.
    pub pending_share: f64,
    pub min_pending_bytes: u64,
    /// The automatic delta repack (ADR 0100); `None` turns it off. Runs
    /// only with a [`Repo`] to walk.
    pub auto: Option<AutoRepackOptions>,
}

/// When the background task compresses new objects against their previous
/// versions (ADR 0100).
#[derive(Clone, Debug)]
pub struct AutoRepackOptions {
    /// It runs once loose objects plus the live objects of whole packs reach
    /// this many,
    pub objects: u64,
    /// or their bytes (loose files plus whole `.pack` files) reach this,
    pub bytes: u64,
    /// or some have waited this long since it last found none.
    pub max_age: Option<Duration>,
    /// How often it counts them: a listing of every loose shard directory
    /// (one bucket listing on an object store).
    pub check_interval: Duration,
    /// zstd level of the packs it writes, 1 to 22.
    pub level: i32,
    /// Older packs folded into the new one by `leep-server repack`'s
    /// geometric rule ([`leep_store::plan_merge`]).
    pub factor: u64,
    pub max_packs: usize,
    /// On an object store, which has no incremental repack, the shortest
    /// time between two full repacks.
    pub object_store_interval: Duration,
}

impl Default for AutoRepackOptions {
    fn default() -> Self {
        AutoRepackOptions {
            objects: 1024,
            bytes: 32 << 20,
            max_age: Some(Duration::from_secs(30 * 60)),
            check_interval: Duration::from_secs(60),
            level: 9,
            factor: 2,
            max_packs: 8,
            object_store_interval: Duration::from_secs(24 * 3600),
        }
    }
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
            pending_share: 0.5,
            min_pending_bytes: 64 << 20,
            auto: Some(AutoRepackOptions::default()),
        }
    }
}

impl BackgroundRepackOptions {
    /// The cap on bytes waiting for cleanup for a store of `live` bytes.
    pub fn pending_cap(&self, live: u64) -> u64 {
        let scaled = (live as f64 * self.pending_share.max(0.0)) as u64;
        scaled
            .max(self.min_pending_bytes)
            .min(self.max_pending_bytes)
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

    /// The store's [`retire_sweep`](FsBlobStore::retire_sweep): garbage
    /// and the retired packs past `fence`. `None` while another repack
    /// holds the store. On an object store, unless `full`, the lease is
    /// taken only when a retired pack is due (one GET otherwise).
    pub async fn sweep(&self, fence: Duration, full: bool) -> Result<Option<SweepReport>> {
        match self {
            PackStore::Fs(s) => {
                let s = s.clone();
                on_low_thread(move || s.retire_sweep_blocking(fence)).await
            }
            PackStore::Object(s) => {
                if !full && !s.retired_due(fence).await? {
                    return Ok(Some(SweepReport::default()));
                }
                s.retire_sweep(fence).await
            }
        }
    }

    fn blob_store(&self) -> Arc<dyn BlobStore> {
        match self {
            PackStore::Fs(s) => Arc::new(s.clone()),
            PackStore::Object(s) => Arc::new(s.clone()),
        }
    }

    async fn loose_stats(&self) -> Result<(u64, u64)> {
        match self {
            PackStore::Fs(s) => {
                let s = s.clone();
                on_low_thread(move || s.loose_stats()).await
            }
            PackStore::Object(s) => s.loose_stats().await,
        }
    }

    async fn pack_details(&self, ids: HashSet<String>) -> Result<Vec<PackDetail>> {
        match self {
            PackStore::Fs(s) => {
                let s = s.clone();
                tokio::task::spawn_blocking(move || s.pack_details(&ids))
                    .await
                    .map_err(|e| Error::Storage(format!("pack task failed: {e}")))?
            }
            PackStore::Object(s) => s.pack_details(&ids).await,
        }
    }

    async fn publish(&self, opts: RepackOptions) -> Result<Pending> {
        self.publish_with(PackHints::default(), opts).await
    }

    async fn publish_with(&self, hints: PackHints, opts: RepackOptions) -> Result<Pending> {
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

/// The sweep a server runs when it starts (ADR 0080), logged; failures
/// are logged too, as the background repack retries at every tick.
pub async fn sweep_at_start(store: &PackStore, fence: Duration) {
    match store.sweep(fence, true).await {
        Ok(Some(r)) => {
            metrics::counter!("leep_repack_retired_removed_total").increment(r.retired_removed);
            if r.garbage_removed + r.retired_removed + r.retired_waiting > 0 {
                tracing::info!(
                    garbage_removed = r.garbage_removed,
                    retired_removed = r.retired_removed,
                    retired_bytes_removed = r.retired_bytes_removed,
                    retired_waiting = r.retired_waiting,
                    "swept packs left by earlier repacks"
                );
            }
        }
        Ok(None) => tracing::debug!("another repack holds the store; sweep skipped"),
        Err(e) => tracing::warn!(error = %e, "sweep of retired packs failed"),
    }
}

/// A merge whose cleanup waits for its fence.
struct Waiting {
    due: Instant,
    /// Bytes the cleanup deletes: the replaced packs, and the loose copies
    /// a delta repack packed.
    bytes: u64,
    /// Loose copies (count and bytes) a delta repack packed, still on disk
    /// until this cleanup; zero for a merge.
    loose: (u64, u64),
    pending: Pending,
}

/// The background repack of one store: [`BackgroundRepack::spawn`] runs it
/// for the life of the process; tests call [`BackgroundRepack::tick`].
pub struct BackgroundRepack {
    store: PackStore,
    opts: BackgroundRepackOptions,
    pending: VecDeque<Waiting>,
    /// What the automatic delta repack walks; `None` leaves it off.
    repo: Option<Repo>,
    auto: AutoState,
}

/// What the automatic delta repack remembers between ticks.
#[derive(Default)]
struct AutoState {
    last_check: Option<Instant>,
    /// When it first found objects to compress since it last found none.
    waiting_since: Option<Instant>,
    /// When it last ran a full repack of an object store.
    last_full: Option<Instant>,
    /// Packs never change, so each one's index is read once.
    details: HashMap<String, PackDetail>,
    /// Packs it wrote, never compressed again even when they hold no delta
    /// (a history with nothing to delta against). Forgotten at a restart,
    /// which costs one more pass over such a pack.
    written: HashSet<String>,
    /// Loose bytes at the last check, for the pending cap.
    loose_bytes: u64,
}

/// What the automatic delta repack did in one tick.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub enum AutoOutcome {
    /// Off, or not due for a check yet.
    #[default]
    NotChecked,
    /// Nothing loose and no whole pack.
    Nothing,
    /// Below every threshold: this many objects and bytes wait.
    BelowThreshold { objects: u64, bytes: u64 },
    /// Due, but held back: cleanups wait on too many bytes (or an earlier
    /// run's cleanup has not deleted the loose copies it packed), an object
    /// store's full repack ran too recently, or another repack holds the
    /// store.
    Held(&'static str),
    /// Published this repack; its cleanup waits for the fence.
    Ran(Box<RepackReport>),
}

/// What one [`BackgroundRepack::tick`] did.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct TickReport {
    /// The merges published, in order.
    pub merged: Vec<RepackReport>,
    /// Cleanups of earlier merges finished.
    pub finished: usize,
    /// What the sweep the tick began with did; `None` if it was skipped
    /// (another repack held the store) or failed.
    pub swept: Option<SweepReport>,
    /// Packs reads search after the tick.
    pub packs: usize,
    /// What the automatic delta repack did.
    pub auto: AutoOutcome,
}

impl BackgroundRepack {
    pub fn new(store: PackStore, opts: BackgroundRepackOptions) -> Self {
        metrics::counter!("leep_repack_background_runs_total", "outcome" => "merged").increment(0);
        metrics::counter!("leep_repack_background_runs_total", "outcome" => "error").increment(0);
        metrics::counter!("leep_repack_background_packs_merged_total").increment(0);
        metrics::counter!("leep_repack_background_objects_written_total").increment(0);
        metrics::counter!("leep_repack_retired_removed_total").increment(0);
        BackgroundRepack {
            store,
            opts,
            pending: VecDeque::new(),
            repo: None,
            auto: AutoState::default(),
        }
    }

    /// Compress new objects against their previous versions too (ADR
    /// 0100), walking `repo`'s history for the bases. `repo` should read
    /// this store directly, not through a cache the walk would flush.
    pub fn with_repo(mut self, repo: Repo) -> Self {
        if self.opts.auto.is_some() {
            for outcome in ["ran", "error"] {
                metrics::counter!("leep_repack_auto_runs_total", "outcome" => outcome).increment(0);
            }
            for reason in ["threshold", "pending", "interval", "busy"] {
                metrics::counter!("leep_repack_auto_skipped_total", "reason" => reason)
                    .increment(0);
            }
            metrics::counter!("leep_repack_auto_input_bytes_total").increment(0);
            metrics::counter!("leep_repack_auto_delta_bytes_total").increment(0);
            metrics::counter!("leep_repack_auto_objects_total").increment(0);
        }
        self.repo = Some(repo);
        self
    }

    /// A repo over this store's own blobs and `meta`, for
    /// [`with_repo`](Self::with_repo).
    pub fn walk_repo(&self, meta: Arc<dyn leep_core::MetaStore>) -> Repo {
        Repo::new(self.store.blob_store(), meta)
    }

    /// Merges whose cleanup waits for its fence.
    pub fn pending(&self) -> usize {
        self.pending.len()
    }

    /// Bytes those cleanups will delete.
    pub fn pending_bytes(&self) -> u64 {
        self.pending.iter().map(|w| w.bytes).sum()
    }

    /// Run for the life of the process: a full sweep at once (what a
    /// process that stopped inside a fence, or a killed pack write, left),
    /// then one tick per interval.
    pub fn spawn(mut self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            sweep_at_start(&self.store, self.opts.fence).await;
            loop {
                tokio::time::sleep(self.opts.interval).await;
                if let Err(e) = self.tick().await {
                    tracing::warn!(error = %e, "background repack failed; trying again later");
                }
            }
        })
    }

    /// Finish the cleanups whose fence has passed, run the delta repack if
    /// it is due, then merge while more than `trigger_packs` packs are
    /// searched and a class has `fanout`.
    pub async fn tick(&mut self) -> Result<TickReport> {
        let swept = match self.store.sweep(self.opts.fence, false).await {
            Ok(s) => s,
            // The merges need no sweep; the next tick tries again.
            Err(e) => {
                tracing::warn!(error = %e, "sweep of retired packs failed");
                None
            }
        };
        if let Some(s) = swept.as_ref().filter(|s| s.retired_removed > 0) {
            metrics::counter!("leep_repack_retired_removed_total").increment(s.retired_removed);
        }
        let mut report = TickReport {
            finished: self.finish_due(false).await?,
            swept,
            ..TickReport::default()
        };
        report.auto = match self.auto_repack().await {
            Ok(o) => o,
            Err(e) => {
                metrics::counter!("leep_repack_auto_runs_total", "outcome" => "error").increment(1);
                // The merges below do not depend on it.
                tracing::warn!(error = %e, "automatic delta repack failed; trying again later");
                AutoOutcome::NotChecked
            }
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
                        loose_removed = r.loose_removed,
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

    /// Whether cleanups already wait on as many bytes as a store of
    /// `live` bytes allows.
    fn pending_full(&self, live: u64) -> bool {
        self.pending.len() >= self.opts.max_pending
            || self.pending_bytes() >= self.opts.pending_cap(live)
    }

    /// Read the index of each pack not seen before, and forget packs gone.
    async fn refresh_details(&mut self, sizes: &[PackSize]) -> Result<()> {
        let live: HashSet<&str> = sizes.iter().map(|p| p.id.as_str()).collect();
        self.auto.details.retain(|id, _| live.contains(id.as_str()));
        let new: HashSet<String> = sizes
            .iter()
            .filter(|p| !self.auto.details.contains_key(&p.id))
            .map(|p| p.id.clone())
            .collect();
        if !new.is_empty() {
            for d in self.store.pack_details(new).await? {
                self.auto.details.insert(d.id.clone(), d);
            }
        }
        Ok(())
    }

    /// Whether `id` is a pack written whole that the delta repack has not
    /// compressed yet.
    fn is_whole(&self, id: &str) -> bool {
        !self.auto.written.contains(id) && self.auto.details.get(id).is_some_and(|d| d.whole())
    }

    /// The automatic delta repack (ADR 0100): count what waits to be
    /// compressed, and if a threshold is passed, walk its history and
    /// publish it as deltas. Its cleanup waits for the fence like a merge's.
    async fn auto_repack(&mut self) -> Result<AutoOutcome> {
        let (Some(auto), Some(repo)) = (self.opts.auto.clone(), self.repo.clone()) else {
            return Ok(AutoOutcome::NotChecked);
        };
        let now = Instant::now();
        if self
            .auto
            .last_check
            .is_some_and(|t| now < t + auto.check_interval)
        {
            return Ok(AutoOutcome::NotChecked);
        }
        self.auto.last_check = Some(now);
        let sizes = self.store.pack_sizes().await?;
        self.refresh_details(&sizes).await?;
        let (loose_all, loose_bytes_all) = self.store.loose_stats().await?;
        self.auto.loose_bytes = loose_bytes_all;
        // Loose copies an earlier run packed stay on disk until its
        // cleanup; they do not wait, and a run now would pack them again.
        let (packed, packed_bytes) = self
            .pending
            .iter()
            .fold((0, 0), |(n, b), w| (n + w.loose.0, b + w.loose.1));
        let (loose, loose_bytes) = (
            loose_all.saturating_sub(packed),
            loose_bytes_all.saturating_sub(packed_bytes),
        );
        let whole: Vec<&PackSize> = sizes.iter().filter(|p| self.is_whole(&p.id)).collect();
        let objects = loose + whole.iter().map(|p| p.objects).sum::<u64>();
        let bytes = loose_bytes + whole.iter().map(|p| p.bytes).sum::<u64>();
        let pack_bytes: u64 = sizes.iter().map(|p| p.bytes).sum();
        let logical: u64 = sizes
            .iter()
            .filter_map(|p| self.auto.details.get(&p.id))
            .map(|d| d.logical_bytes)
            .sum::<u64>()
            + loose_bytes_all;
        let live = pack_bytes + loose_bytes_all;
        metrics::gauge!("leep_repack_auto_waiting_objects").set(objects as f64);
        metrics::gauge!("leep_repack_auto_waiting_bytes").set(bytes as f64);
        metrics::gauge!("leep_blob_store_bytes").set(live as f64);
        metrics::gauge!("leep_blob_store_logical_bytes").set(logical as f64);
        if logical > 0 {
            metrics::gauge!("leep_blob_store_size_ratio").set(live as f64 / logical as f64);
        }
        if objects == 0 {
            self.auto.waiting_since = None;
            return Ok(AutoOutcome::Nothing);
        }
        let since = *self.auto.waiting_since.get_or_insert(now);
        let due = objects >= auto.objects
            || bytes >= auto.bytes
            || auto.max_age.is_some_and(|a| now >= since + a);
        if !due {
            metrics::counter!("leep_repack_auto_skipped_total", "reason" => "threshold")
                .increment(1);
            return Ok(AutoOutcome::BelowThreshold { objects, bytes });
        }
        if self.pending_full(live) || packed > 0 {
            metrics::counter!("leep_repack_auto_skipped_total", "reason" => "pending").increment(1);
            return Ok(AutoOutcome::Held("pending"));
        }
        let object_store = matches!(self.store, PackStore::Object(_));
        if object_store
            && self
                .auto
                .last_full
                .is_some_and(|t| now < t + auto.object_store_interval)
        {
            metrics::counter!("leep_repack_auto_skipped_total", "reason" => "interval")
                .increment(1);
            return Ok(AutoOutcome::Held("interval"));
        }

        let started = Instant::now();
        let whole_ids: Vec<String> = whole.iter().map(|p| p.id.clone()).collect();
        let replaced_bytes = |ids: &[String]| -> u64 {
            sizes
                .iter()
                .filter(|p| ids.contains(&p.id))
                .map(|p| p.bytes)
                .sum()
        };
        let (order, opts) = match &self.store {
            PackStore::Fs(s) => {
                let in_whole = s.packed_in(&whole_ids.iter().cloned().collect())?;
                let s = s.clone();
                let wanted = move |h: &Hash| s.is_loose(h) || in_whole(h);
                let order =
                    leep_derive::pack_order::pack_order_where(&repo, &wanted, &wanted, usize::MAX)
                        .await?;
                let opts = RepackOptions {
                    level: auto.level,
                    mode: RepackMode::Incremental {
                        factor: auto.factor.max(2),
                        max_packs: auto.max_packs.max(1),
                    },
                    packs: whole_ids,
                    threads: 1,
                    ..RepackOptions::default()
                };
                (order, opts)
            }
            // No incremental repack on an object store: a full one, which
            // walks all of history, so it runs at most once per interval.
            PackStore::Object(_) => {
                let order = leep_derive::pack_order::pack_order(&repo).await?;
                let opts = RepackOptions {
                    level: auto.level,
                    mode: RepackMode::Full,
                    threads: 1,
                    ..RepackOptions::default()
                };
                (order, opts)
            }
        };
        let walked = started.elapsed();
        let pending = match self.store.publish_with(order.objects, opts).await {
            Ok(p) => p,
            Err(Error::Conflict(m)) => {
                tracing::debug!(reason = %m, "automatic delta repack skipped");
                metrics::counter!("leep_repack_auto_skipped_total", "reason" => "busy")
                    .increment(1);
                return Ok(AutoOutcome::Held("busy"));
            }
            Err(e) => return Err(e),
        };
        if object_store {
            self.auto.last_full = Some(Instant::now());
        }
        let took = started.elapsed();
        let report = pending.report().clone();
        if let Some(id) = &report.pack_id {
            self.auto.written.insert(id.clone());
        }
        self.auto.waiting_since = None;
        let replaced = match &pending {
            Pending::Fs(p) => replaced_bytes(p.replaced()),
            Pending::Object(p) => replaced_bytes(p.replaced()),
        };
        metrics::counter!("leep_repack_auto_runs_total", "outcome" => "ran").increment(1);
        metrics::counter!("leep_repack_auto_input_bytes_total").increment(bytes);
        metrics::counter!("leep_repack_auto_objects_total").increment(report.objects);
        metrics::counter!("leep_repack_auto_delta_bytes_total").increment(report.pack_bytes);
        metrics::histogram!("leep_repack_auto_seconds").record(took.as_secs_f64());
        tracing::info!(
            changesets = order.changesets,
            objects = report.objects,
            deltas = report.deltas,
            whole = report.whole,
            loose = report.loose_before,
            packs_merged = report.packs_merged,
            waiting_bytes = bytes,
            pack_bytes = report.pack_bytes,
            walk_seconds = walked.as_secs_f64(),
            seconds = took.as_secs_f64(),
            "automatic delta repack published a pack"
        );
        self.pending.push_back(Waiting {
            due: Instant::now() + self.opts.fence,
            bytes: replaced + report.loose_bytes_before,
            loose: (report.loose_before, report.loose_bytes_before),
            pending,
        });
        Ok(AutoOutcome::Ran(Box::new(report)))
    }

    async fn merge(&mut self, sizes: &[PackSize]) -> Result<Option<RepackReport>> {
        let live = sizes.iter().map(|p| p.bytes).sum::<u64>() + self.auto.loose_bytes;
        if self.pending_full(live) {
            return Ok(None);
        }
        let plan = if self.repo.is_some() && self.opts.auto.is_some() {
            // Whole packs merge only with whole packs, so the delta repack
            // still finds their frames; delta packs only with delta packs.
            self.refresh_details(sizes).await?;
            let (whole, deltas): (Vec<PackSize>, Vec<PackSize>) =
                sizes.iter().cloned().partition(|p| self.is_whole(&p.id));
            match plan_tiered(&whole, &self.opts) {
                p if p.len() >= 2 => p,
                _ => plan_tiered(&deltas, &self.opts),
            }
        } else {
            plan_tiered(sizes, &self.opts)
        };
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
            loose: (0, 0),
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
