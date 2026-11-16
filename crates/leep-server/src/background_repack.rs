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
//! in a delta pack before it is compressed. A delta run can only use a base
//! in the pack it writes, so on a filesystem store the task also
//! consolidates its delta packs when that saves enough (ADR 0107): it
//! merges them and compresses the frames the runs stored whole against
//! their previous versions, now in the same pack. Lands never wait for any
//! of it.

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
    /// Merges of at most this many bytes start past that cap (not past
    /// `max_pending`): they are what keeps the land packs few, and they
    /// hold little disk twice.
    pub small_merge_bytes: u64,
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
    /// One run (a batch) takes at most this many bytes, objects and whole
    /// packs, oldest packs first, and walks at most `walk_limit`
    /// changesets; what is left runs as the next batch, a tick later, after
    /// the merges, cleanups and sweep of that tick. Each batch publishes
    /// on its own, so a restart keeps the batches done.
    pub max_batch_bytes: u64,
    pub max_batch_objects: u64,
    pub max_batch_packs: usize,
    pub walk_limit: usize,
    /// On an object store, which has no incremental repack, the shortest
    /// time between two full repacks, kept in the bucket, so restarts and
    /// replicas share it.
    pub object_store_interval: Duration,
    /// An object store's full repack runs only once the bytes waiting
    /// reach this share of the store, and at least `object_store_min_bytes`
    /// (not on age: a full rewrite for a few objects is not worth it).
    pub object_store_share: f64,
    pub object_store_min_bytes: u64,
    /// Consolidation of the delta packs (ADR 0107), on filesystem stores;
    /// `None` turns it off.
    pub consolidate: Option<ConsolidateOptions>,
}

/// When the background task consolidates its delta packs (ADR 0107): it
/// merges them into one and compresses each frame they hold whole against
/// the previous version the history walk names, now in the same pack. A
/// delta run stores the first new version at each path whole, because its
/// base is in an older pack (packs are self-contained, ADR 0039); this is
/// what compresses those frames later, as `git gc` does.
#[derive(Clone, Debug)]
pub struct ConsolidateOptions {
    /// It runs once the bytes it would save reach `1 / ratio` of the packs
    /// it rewrites,
    pub ratio: u64,
    /// or reach `1/64` of them (at least `min_gain`) and have done so for
    /// this long; `None` leaves only the ratio.
    pub age: Option<Duration>,
    pub min_gain: u64,
    /// Delta packs larger than this are never rewritten in the background:
    /// they are left to `leep-server repack --full`, and logged once.
    pub max_bytes: u64,
}

impl Default for ConsolidateOptions {
    fn default() -> Self {
        ConsolidateOptions {
            ratio: 8,
            age: Some(Duration::from_secs(24 * 3600)),
            min_gain: 64 << 10,
            max_bytes: 2 << 30,
        }
    }
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
            max_batch_bytes: 1 << 30,
            max_batch_objects: 500_000,
            max_batch_packs: 256,
            walk_limit: 2_000_000,
            object_store_interval: Duration::from_secs(24 * 3600),
            object_store_share: 0.25,
            object_store_min_bytes: 64 << 20,
            consolidate: Some(ConsolidateOptions::default()),
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
            small_merge_bytes: 64 << 20,
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

/// Bytes of the packs in `sizes` that `ids` names.
fn replaced_bytes(sizes: &[PackSize], ids: &[String]) -> u64 {
    let ids: HashSet<&String> = ids.iter().collect();
    sizes
        .iter()
        .filter(|p| ids.contains(&p.id))
        .map(|p| p.bytes)
        .sum()
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

    /// Every loose object with its size: one listing, off the async
    /// workers.
    async fn loose_listing(&self) -> Result<Vec<(Hash, u64)>> {
        match self {
            PackStore::Fs(s) => {
                let s = s.clone();
                on_low_thread(move || s.loose_listing()).await
            }
            PackStore::Object(s) => s.loose_listing().await,
        }
    }

    async fn considered(&self) -> Result<HashSet<String>> {
        match self {
            PackStore::Fs(s) => {
                let s = s.clone();
                tokio::task::spawn_blocking(move || s.considered_packs())
                    .await
                    .map_err(|e| Error::Storage(format!("pack task failed: {e}")))?
            }
            PackStore::Object(s) => s.considered_packs().await,
        }
    }

    /// Whether `hash` has a live copy in one of the packs `ids` names; an
    /// in-memory lookup.
    async fn packed_in(
        &self,
        ids: &HashSet<String>,
    ) -> Result<Box<dyn Fn(&Hash) -> bool + Send + Sync>> {
        Ok(match self {
            PackStore::Fs(s) => Box::new(s.packed_in(ids)?),
            PackStore::Object(s) => Box::new(s.packed_in(ids).await?),
        })
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
    /// Loose copies a delta repack packed, still on disk until this
    /// cleanup; none for a merge.
    loose: Vec<Hash>,
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
    /// No count before this: the check interval, or longer while a run is
    /// held (a cleanup's fence, an object store's interval).
    next_check: Option<Instant>,
    /// When it first found objects to compress since it last found none.
    waiting_since: Option<Instant>,
    /// The last batch left work: the next runs without a threshold.
    more: bool,
    /// Held because cleanups wait on too many bytes: the next cleanup
    /// that finishes ends the back-off.
    held: bool,
    /// Packs never change, so each one's index is read once.
    details: HashMap<String, PackDetail>,
    /// Packs a delta repack already compressed (`packs/considered`), read
    /// at each check.
    considered: HashSet<String>,
    /// Loose objects a run found corrupt: left in place for an operator
    /// (`verify --rehash`), never counted as waiting again.
    corrupt: HashSet<Hash>,
    /// Loose bytes at the last check, for the pending cap.
    loose_bytes: u64,
    /// What the last check's consolidation did, for the tick's report.
    consolidated: ConsolidateOutcome,
    /// For each largest delta pack a walk went through (all of history up
    /// to the walk limit, once per pack and process): the bytes its own
    /// whole frames would save. Later walks stop at that pack's history.
    inside: HashMap<String, u64>,
    /// The delta packs last judged and the bytes the walk found outside
    /// the largest: no walk again until the set changes.
    judged: Option<(Vec<String>, u64)>,
    /// When the gain first reached the age rule's floor.
    gain_since: Option<Instant>,
    /// Delta packs over `max_bytes`, logged once each.
    too_big: HashSet<String>,
    /// A consolidation is due but held by the cleanups waiting: delta runs
    /// wait for it (at most a fence), so a busy store still gets it.
    consolidation_held: bool,
}

/// What the consolidation of delta packs (ADR 0107) did at a check.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub enum ConsolidateOutcome {
    /// Off, not due for a check, or a delta run went first.
    #[default]
    NotChecked,
    /// No delta pack the server may rewrite, or too few whole frames in
    /// them to be worth a walk.
    Nothing,
    /// Walked (or remembered the last walk of the same packs): rewriting
    /// them would save this many bytes, not enough yet. `walked` counts the
    /// changesets the walks of this check went through (0 if remembered).
    Below { gain: u64, walked: u64 },
    /// Due, but cleanups wait on too many bytes, or another repack holds
    /// the store.
    Held(&'static str),
    /// Published this pack; `gain` is what the walk expected to save.
    Ran {
        report: Box<RepackReport>,
        gain: u64,
        walked: u64,
    },
    /// Failed; logged, and tried again at the next check.
    Failed(String),
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
    /// Failed; logged, and tried again at the next check.
    Failed(String),
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
    /// What the consolidation of delta packs did.
    pub consolidate: ConsolidateOutcome,
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
            for reason in ["threshold", "pending", "interval", "busy", "consolidation"] {
                metrics::counter!("leep_repack_auto_skipped_total", "reason" => reason)
                    .increment(0);
            }
            metrics::counter!("leep_repack_auto_input_bytes_total").increment(0);
            metrics::counter!("leep_repack_auto_delta_bytes_total").increment(0);
            metrics::counter!("leep_repack_auto_objects_total").increment(0);
            if self
                .opts
                .auto
                .as_ref()
                .is_some_and(|a| a.consolidate.is_some())
            {
                for outcome in ["ran", "error", "held"] {
                    metrics::counter!("leep_repack_consolidate_runs_total", "outcome" => outcome)
                        .increment(0);
                }
                metrics::counter!("leep_repack_consolidate_gain_bytes_total").increment(0);
            }
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
                AutoOutcome::Failed(e.to_string())
            }
        };
        report.consolidate = std::mem::take(&mut self.auto.consolidated);
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
                    if std::mem::take(&mut self.auto.held) {
                        self.auto.next_check = None;
                    }
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
        !self.auto.considered.contains(id) && self.auto.details.get(id).is_some_and(|d| d.whole())
    }

    /// The automatic delta repack (ADR 0100): count what waits to be
    /// compressed, and if a threshold is passed, walk its history and
    /// publish one batch of it as deltas. Its cleanup waits for the fence
    /// like a merge's; the next batch runs at the next tick.
    async fn auto_repack(&mut self) -> Result<AutoOutcome> {
        let (Some(auto), Some(repo)) = (self.opts.auto.clone(), self.repo.clone()) else {
            return Ok(AutoOutcome::NotChecked);
        };
        let now = Instant::now();
        if self.auto.next_check.is_some_and(|t| now < t) {
            return Ok(AutoOutcome::NotChecked);
        }
        self.auto.next_check = Some(now + auto.check_interval);
        let sizes = self.store.pack_sizes().await?;
        self.refresh_details(&sizes).await?;
        self.auto.considered = self.store.considered().await?;
        let listing = self.store.loose_listing().await?;
        let loose_bytes_all: u64 = listing.iter().map(|(_, n)| n).sum();
        self.auto.loose_bytes = loose_bytes_all;
        // Loose copies an earlier run packed stay on disk until its
        // cleanup, and corrupt ones until an operator acts: neither waits.
        let held: HashSet<&Hash> = self.pending.iter().flat_map(|w| &w.loose).collect();
        let loose: Vec<(Hash, u64)> = listing
            .iter()
            .filter(|(h, _)| !held.contains(h) && !self.auto.corrupt.contains(h))
            .copied()
            .collect();
        drop(held);
        // The walk of a consolidation passes through loose objects.
        let consolidating = matches!(self.store, PackStore::Fs(_)) && auto.consolidate.is_some();
        let loose_all: HashSet<Hash> = if consolidating {
            listing.iter().map(|(h, _)| *h).collect()
        } else {
            HashSet::new()
        };
        drop(listing);
        let mut whole: Vec<&PackSize> = sizes.iter().filter(|p| self.is_whole(&p.id)).collect();
        let loose_bytes: u64 = loose.iter().map(|(_, n)| n).sum();
        let objects = loose.len() as u64 + whole.iter().map(|p| p.objects).sum::<u64>();
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
        metrics::gauge!("leep_blob_store_size_ratio").set(if logical > 0 {
            live as f64 / logical as f64
        } else {
            0.0
        });
        if objects == 0 {
            self.auto.waiting_since = None;
            self.auto.more = false;
            if consolidating {
                self.consolidate_now(&auto, &repo, &sizes, &loose_all, live)
                    .await;
            }
            return Ok(AutoOutcome::Nothing);
        }
        let since = *self.auto.waiting_since.get_or_insert(now);
        let object_store = matches!(self.store, PackStore::Object(_));
        let due = if object_store {
            let floor = ((live as f64 * auto.object_store_share.max(0.0)) as u64)
                .max(auto.object_store_min_bytes);
            bytes >= floor
        } else {
            self.auto.more
                || objects >= auto.objects
                || bytes >= auto.bytes
                || auto.max_age.is_some_and(|a| now >= since + a)
        };
        if !due {
            metrics::counter!("leep_repack_auto_skipped_total", "reason" => "threshold")
                .increment(1);
            if consolidating {
                self.consolidate_now(&auto, &repo, &sizes, &loose_all, live)
                    .await;
            }
            return Ok(AutoOutcome::BelowThreshold { objects, bytes });
        }
        if consolidating && self.auto.consolidation_held && !self.auto.more {
            // A consolidation due first: no new cleanup while it waits.
            self.consolidate_now(&auto, &repo, &sizes, &loose_all, live)
                .await;
            if self.auto.consolidation_held {
                metrics::counter!("leep_repack_auto_skipped_total", "reason" => "consolidation")
                    .increment(1);
                return Ok(AutoOutcome::Held("consolidation"));
            }
        }
        if self.pending_full(live) {
            metrics::counter!("leep_repack_auto_skipped_total", "reason" => "pending").increment(1);
            // Nothing changes before a cleanup is due: no listing until then.
            if let Some(due) = self.pending.iter().map(|w| w.due).min() {
                self.auto.next_check = Some(due.max(now + auto.check_interval));
            }
            self.auto.held = true;
            return Ok(AutoOutcome::Held("pending"));
        }
        if let PackStore::Object(s) = &self.store {
            if let Some(last) = s.last_full_repack().await? {
                let next = last.saturating_add(auto.object_store_interval.as_secs() as i64);
                let wall = jiff::Timestamp::now().as_second();
                if wall < next {
                    metrics::counter!("leep_repack_auto_skipped_total", "reason" => "interval")
                        .increment(1);
                    let wait = Duration::from_secs((next - wall) as u64);
                    self.auto.next_check = Some(now + wait.max(auto.check_interval));
                    return Ok(AutoOutcome::Held("interval"));
                }
            }
        }

        let started = Instant::now();
        let (order, opts, more) = match &self.store {
            PackStore::Fs(_) => {
                // One batch: the oldest whole packs within the caps (at
                // least one), then loose objects within what is left.
                whole.sort_by(|a, b| {
                    let age = |p: &PackSize| self.auto.details.get(&p.id).map_or(0, |d| d.oldest);
                    age(a).cmp(&age(b)).then_with(|| a.id.cmp(&b.id))
                });
                let (mut batch, mut b_bytes, mut b_objects) = (Vec::new(), 0u64, 0u64);
                for p in &whole {
                    if !batch.is_empty()
                        && (batch.len() >= auto.max_batch_packs
                            || b_bytes + p.bytes > auto.max_batch_bytes
                            || b_objects + p.objects > auto.max_batch_objects)
                    {
                        break;
                    }
                    b_bytes += p.bytes;
                    b_objects += p.objects;
                    batch.push(p.id.clone());
                }
                let mut more = batch.len() < whole.len();
                let batch_set: HashSet<String> = batch.iter().cloned().collect();
                let all_whole: HashSet<String> = whole.iter().map(|p| p.id.clone()).collect();
                let in_batch = self.store.packed_in(&batch_set).await?;
                let in_whole = self.store.packed_in(&all_whole).await?;
                let loose_set: HashSet<Hash> = loose.iter().map(|(h, _)| *h).collect();
                let wanted = |h: &Hash| loose_set.contains(h) || in_batch(h);
                let through = |h: &Hash| loose_set.contains(h) || in_whole(h);
                let order = leep_derive::pack_order::pack_order_where(
                    &repo,
                    &wanted,
                    &through,
                    auto.walk_limit,
                )
                .await?;
                let room_objects = auto.max_batch_objects.saturating_sub(b_objects);
                let room_bytes = auto.max_batch_bytes.saturating_sub(b_bytes);
                let subset: HashSet<Hash> =
                    if loose.len() as u64 <= room_objects && loose_bytes <= room_bytes {
                        loose_set.clone()
                    } else {
                        // The oldest loose objects the walk lists first, then
                        // those it does not list (uploads no ref reaches,
                        // history past the walk limit) in hash order. The
                        // first always fits, so every batch packs something
                        // and a loose object larger than the room still goes.
                        more = true;
                        let size: HashMap<Hash, u64> = loose.iter().copied().collect();
                        let (mut n, mut b) = (0u64, 0u64);
                        let mut out = HashSet::new();
                        let listed = order.objects.iter().map(|(h, _)| h);
                        let rest = loose.iter().map(|(h, _)| *h);
                        for h in listed.chain(rest) {
                            if n >= room_objects.max(1) {
                                break;
                            }
                            let Some(&len) = size.get(&h) else { continue };
                            if out.contains(&h) || (n > 0 && b + len > room_bytes) {
                                continue;
                            }
                            n += 1;
                            b += len;
                            out.insert(h);
                        }
                        out
                    };
                let opts = RepackOptions {
                    level: auto.level,
                    mode: RepackMode::Incremental {
                        factor: auto.factor.max(2),
                        max_packs: auto.max_packs.max(1),
                    },
                    leave: all_whole.difference(&batch_set).cloned().collect(),
                    packs: batch,
                    loose_subset: Some(Arc::new(subset)),
                    mark: true,
                    threads: 1,
                    ..RepackOptions::default()
                };
                (order, opts, more)
            }
            // No incremental repack on an object store: a full one, which
            // walks all of history, so it runs at most once per interval.
            PackStore::Object(_) => {
                let order = leep_derive::pack_order::pack_order(&repo).await?;
                let opts = RepackOptions {
                    level: auto.level,
                    mode: RepackMode::Full,
                    mark: true,
                    // Checked again, and recorded, under the lease: a
                    // replica may have run one since the check above.
                    full_interval: Some(auto.object_store_interval),
                    threads: 1,
                    ..RepackOptions::default()
                };
                (order, opts, false)
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
        let took = started.elapsed();
        let report = pending.report().clone();
        self.auto.corrupt.extend(report.corrupt.iter().copied());
        self.auto.waiting_since = None;
        // A batch that packed nothing (everything vanished meanwhile) does
        // not run again at once: the next check comes after the interval.
        let more = more && report.objects > 0;
        self.auto.more = more;
        if more {
            // The next batch at the next tick, after its merges and cleanups.
            self.auto.next_check = None;
        }
        let (replaced, packed_loose): (u64, Vec<Hash>) = match &pending {
            Pending::Fs(p) => (
                replaced_bytes(&sizes, p.replaced()),
                p.packed_loose().copied().collect(),
            ),
            Pending::Object(p) => (
                replaced_bytes(&sizes, p.replaced()),
                p.packed_loose().copied().collect(),
            ),
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
            more,
            walk_seconds = walked.as_secs_f64(),
            seconds = took.as_secs_f64(),
            "automatic delta repack published a pack"
        );
        self.pending.push_back(Waiting {
            due: Instant::now() + self.opts.fence,
            bytes: replaced + report.loose_bytes_before,
            loose: packed_loose,
            pending,
        });
        // A busy store may never be below every threshold: judge the delta
        // packs after the run too, unless more batches follow.
        if consolidating && !more {
            let sizes = self.store.pack_sizes().await?;
            self.refresh_details(&sizes).await?;
            self.auto.considered = self.store.considered().await?;
            let live = sizes.iter().map(|p| p.bytes).sum::<u64>() + self.auto.loose_bytes;
            self.consolidate_now(&auto, &repo, &sizes, &loose_all, live)
                .await;
        }
        Ok(AutoOutcome::Ran(Box::new(report)))
    }

    /// [`consolidate`](Self::consolidate), its error logged and reported.
    async fn consolidate_now(
        &mut self,
        auto: &AutoRepackOptions,
        repo: &Repo,
        sizes: &[PackSize],
        loose: &HashSet<Hash>,
        live: u64,
    ) {
        let outcome = match self.consolidate(auto, repo, sizes, loose, live).await {
            Ok(o) => o,
            Err(e) => {
                metrics::counter!("leep_repack_consolidate_runs_total", "outcome" => "error")
                    .increment(1);
                tracing::warn!(error = %e, "consolidation of delta packs failed; trying again later");
                ConsolidateOutcome::Failed(e.to_string())
            }
        };
        self.auto.consolidation_held = outcome == ConsolidateOutcome::Held("pending");
        self.auto.consolidated = outcome;
    }

    /// The consolidation of delta packs (ADR 0107), when nothing waits for
    /// a delta run: if compressing the frames they hold whole against the
    /// previous versions they also hold would save enough, merge them all
    /// into one pack, giving each such frame the walk's base.
    ///
    /// The walk goes through the history written since the largest delta
    /// pack and stops at changesets whose root tree is in it, so its cost is
    /// that of the recent history; that pack is merged, so the frames the
    /// walk lists find their bases in it. The largest pack's own whole
    /// frames are judged once per pack and process, by a walk through its
    /// history too: that finds the frames that older merges (or older
    /// servers) left whole next to their bases, so an upgraded store is
    /// compressed with no manual step. Packs over `max_bytes` are left out
    /// (and the walks stop at them).
    async fn consolidate(
        &mut self,
        auto: &AutoRepackOptions,
        repo: &Repo,
        sizes: &[PackSize],
        loose: &HashSet<Hash>,
        live: u64,
    ) -> Result<ConsolidateOutcome> {
        let (Some(c), PackStore::Fs(fs)) = (auto.consolidate.clone(), self.store.clone()) else {
            return Ok(ConsolidateOutcome::NotChecked);
        };
        let present: HashSet<&str> = sizes.iter().map(|p| p.id.as_str()).collect();
        self.auto
            .inside
            .retain(|id, _| present.contains(id.as_str()));
        self.auto.too_big.retain(|id| present.contains(id.as_str()));
        drop(present);
        let delta: Vec<&PackSize> = sizes.iter().filter(|p| !self.is_whole(&p.id)).collect();
        let mut cand: Vec<&PackSize> = Vec::new();
        for p in delta {
            if p.bytes <= c.max_bytes {
                cand.push(p);
            } else if self.auto.too_big.insert(p.id.clone()) {
                tracing::info!(
                    pack = %p.id,
                    bytes = p.bytes,
                    max_bytes = c.max_bytes,
                    "a delta pack is larger than --auto-repack-consolidate-max-bytes: the \
                     background repack leaves it to `leep-server repack --full`"
                );
            }
        }
        let whole_of = |id: &str| self.auto.details.get(id).map_or(0, |d| d.whole_bytes);
        let Some(largest) = cand
            .iter()
            .max_by(|a, b| a.bytes.cmp(&b.bytes).then_with(|| a.id.cmp(&b.id)))
            .copied()
        else {
            metrics::gauge!("leep_repack_whole_bytes_outside_largest").set(0.0);
            return Ok(ConsolidateOutcome::Nothing);
        };
        let w_rest: u64 = cand
            .iter()
            .filter(|p| p.id != largest.id)
            .map(|p| whole_of(&p.id))
            .sum();
        metrics::gauge!("leep_repack_whole_bytes_outside_largest").set(w_rest as f64);
        let total: u64 = cand.iter().map(|p| p.bytes).sum();
        let ratio_floor = (total / c.ratio.max(1)).max(1);
        let age_floor = (total / 64).max(c.min_gain).max(1);
        let floor = match c.age {
            Some(_) => ratio_floor.min(age_floor),
            None => ratio_floor,
        };
        let known_inside = self.auto.inside.get(&largest.id).copied();
        // Objects GC deleted are dropped by the merge.
        let dead = {
            let fs = fs.clone();
            let ids: HashSet<String> = cand.iter().map(|p| p.id.clone()).collect();
            tokio::task::spawn_blocking(move || fs.tombstoned_bytes(&ids))
                .await
                .map_err(|e| Error::Storage(format!("pack task failed: {e}")))??
        };
        // The walk only finds gain in whole frames; too few, and it is not
        // worth one.
        let possible = dead + w_rest + known_inside.unwrap_or_else(|| whole_of(&largest.id));
        if possible < floor {
            self.auto.inside.entry(largest.id.clone()).or_insert(0);
            self.auto.gain_since = None;
            return Ok(ConsolidateOutcome::Nothing);
        }
        let mut ids: Vec<String> = cand.iter().map(|p| p.id.clone()).collect();
        ids.sort();
        let all: HashSet<String> = ids.iter().cloned().collect();
        let whole_packs: HashSet<String> = sizes
            .iter()
            .filter(|p| self.is_whole(&p.id))
            .map(|p| p.id.clone())
            .collect();
        let largest_id = largest.id.clone();
        let cached = match (&self.auto.judged, known_inside) {
            (Some((k, g)), Some(_)) if *k == ids => Some(*g),
            _ => None,
        };
        let mut walked = None;
        let mut walked_cs = 0u64;
        let outside = match cached {
            Some(g) => g,
            None => {
                let deep = known_inside.is_none();
                let w = consolidation_walk(
                    &fs,
                    repo,
                    auto.walk_limit,
                    auto.level,
                    &all,
                    &largest_id,
                    deep,
                    loose,
                    &whole_packs,
                )
                .await?;
                if deep {
                    self.auto.inside.insert(largest_id.clone(), w.inside);
                }
                self.auto.judged = Some((ids.clone(), w.outside));
                walked_cs += w.order.changesets;
                let g = w.outside;
                walked = Some((w, deep));
                g
            }
        };
        let inside = self.auto.inside.get(&largest_id).copied().unwrap_or(0);
        let gain = dead + inside + outside;
        let now = Instant::now();
        if gain >= age_floor {
            self.auto.gain_since.get_or_insert(now);
        } else {
            self.auto.gain_since = None;
        }
        let aged = match (c.age, self.auto.gain_since) {
            (Some(a), Some(since)) => now >= since + a,
            _ => false,
        };
        if gain < ratio_floor && !aged {
            return Ok(ConsolidateOutcome::Below {
                gain,
                walked: walked_cs,
            });
        }
        // Past the cap when no cleanup but those of small merges waits: a
        // store that is mostly one pack could never consolidate otherwise,
        // and land packs keep merging under load.
        let only_small = self
            .pending
            .iter()
            .all(|w| w.loose.is_empty() && w.bytes <= self.opts.small_merge_bytes);
        if !only_small && self.pending_bytes() + total > self.opts.pending_cap(live) {
            metrics::counter!("leep_repack_consolidate_runs_total", "outcome" => "held")
                .increment(1);
            return Ok(ConsolidateOutcome::Held("pending"));
        }
        let started = Instant::now();
        // The largest pack's own frames need the walk through its history.
        let order = match walked {
            Some((w, deep)) if deep || inside == 0 => w.order,
            _ => {
                let w = consolidation_walk(
                    &fs,
                    repo,
                    auto.walk_limit,
                    auto.level,
                    &all,
                    &largest_id,
                    inside > 0,
                    loose,
                    &whole_packs,
                )
                .await?;
                walked_cs += w.order.changesets;
                w.order
            }
        };
        let walked_s = started.elapsed();
        let opts = RepackOptions {
            level: auto.level,
            mode: RepackMode::Incremental {
                factor: auto.factor.max(2),
                max_packs: auto.max_packs.max(1),
            },
            // Exactly these packs: no loose object, no other pack.
            leave: sizes
                .iter()
                .filter(|p| !all.contains(&p.id))
                .map(|p| p.id.clone())
                .collect(),
            packs: ids,
            loose_subset: Some(Arc::new(HashSet::new())),
            mark: true,
            threads: 1,
            ..RepackOptions::default()
        };
        let pending = match self.store.publish_with(order.objects, opts).await {
            Ok(p) => p,
            Err(Error::Conflict(m)) => {
                tracing::debug!(reason = %m, "consolidation of delta packs skipped");
                metrics::counter!("leep_repack_consolidate_runs_total", "outcome" => "held")
                    .increment(1);
                return Ok(ConsolidateOutcome::Held("busy"));
            }
            Err(e) => return Err(e),
        };
        let took = started.elapsed();
        let report = pending.report().clone();
        if let Some(id) = &report.pack_id {
            // Every frame it holds whole was listed by a walk or judged.
            self.auto.inside.insert(id.clone(), 0);
        }
        self.auto.judged = None;
        self.auto.gain_since = None;
        let replaced = match &pending {
            Pending::Fs(p) => replaced_bytes(sizes, p.replaced()),
            Pending::Object(p) => replaced_bytes(sizes, p.replaced()),
        };
        metrics::counter!("leep_repack_consolidate_runs_total", "outcome" => "ran").increment(1);
        metrics::counter!("leep_repack_consolidate_gain_bytes_total").increment(gain);
        metrics::histogram!("leep_repack_consolidate_seconds").record(took.as_secs_f64());
        tracing::info!(
            changesets = order.changesets,
            packs_merged = report.packs_merged,
            objects = report.objects,
            deltas = report.deltas,
            whole = report.whole,
            frames_reused = report.frames_reused,
            pack_bytes_before = replaced,
            pack_bytes = report.pack_bytes,
            expected_gain = gain,
            walk_seconds = walked_s.as_secs_f64(),
            seconds = took.as_secs_f64(),
            "consolidated delta packs"
        );
        self.pending.push_back(Waiting {
            due: Instant::now() + self.opts.fence,
            bytes: replaced,
            loose: Vec::new(),
            pending,
        });
        Ok(ConsolidateOutcome::Ran {
            report: Box::new(report),
            gain,
            walked: walked_cs,
        })
    }

    async fn merge(&mut self, sizes: &[PackSize]) -> Result<Option<RepackReport>> {
        if self.pending.len() >= self.opts.max_pending {
            return Ok(None);
        }
        let auto = self.repo.is_some() && self.opts.auto.is_some();
        let (plan, mark) = if auto {
            // Whole packs merge only with whole packs, so the delta repack
            // still finds their frames; delta (or already considered) packs
            // only with each other, and their merge stays considered.
            self.refresh_details(sizes).await?;
            self.auto.considered = self.store.considered().await?;
            let (whole, deltas): (Vec<PackSize>, Vec<PackSize>) =
                sizes.iter().cloned().partition(|p| self.is_whole(&p.id));
            match plan_tiered(&whole, &self.opts) {
                p if p.len() >= 2 => (p, false),
                _ => {
                    let p = plan_tiered(&deltas, &self.opts);
                    let any_zero = p
                        .iter()
                        .any(|id| self.auto.details.get(id).is_some_and(|d| d.deltas == 0));
                    (p, any_zero)
                }
            }
        } else {
            (plan_tiered(sizes, &self.opts), false)
        };
        if plan.len() < 2 {
            return Ok(None);
        }
        let bytes = replaced_bytes(sizes, &plan);
        let live = sizes.iter().map(|p| p.bytes).sum::<u64>() + self.auto.loose_bytes;
        if bytes > self.opts.small_merge_bytes && self.pending_full(live) {
            return Ok(None);
        }
        let started = Instant::now();
        let opts = RepackOptions {
            level: self.opts.level,
            mode: RepackMode::Planned { loose: false },
            packs: plan,
            mark,
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
            loose: Vec::new(),
            pending,
        });
        Ok(Some(report))
    }
}

/// A consolidation's walk and what it found.
struct ConsolidationWalk {
    order: leep_derive::pack_order::PackOrder,
    /// What compressing the frames stored whole in the largest pack, and
    /// in the other packs, against the walk's bases in the packs would
    /// save: measured on a sample.
    inside: u64,
    outside: u64,
}

/// Frames compressed again to measure what a consolidation would save, at
/// most: an evenly spread sample of them, by hash.
const GAIN_SAMPLE: usize = 256;
const GAIN_SAMPLE_BYTES: usize = 64 << 20;

/// A frame stored whole, the base the walk names for it, and its size.
struct Candidate {
    hash: Hash,
    base: Hash,
    stored: u64,
}

/// What compressing `cands` against their bases would save, from a sample
/// compressed at `level`. Not the stored size of the frames: a writer that
/// had the base at hand already kept whole every frame its delta would
/// not shrink (small tree nodes, rewritten files), and counting those
/// would rewrite a pack for nothing.
async fn measured_gain(fs: &FsBlobStore, mut cands: Vec<Candidate>, level: i32) -> Result<u64> {
    if cands.is_empty() {
        return Ok(0);
    }
    let total: u64 = cands.iter().map(|c| c.stored).sum();
    cands.sort_unstable_by_key(|c| c.hash);
    let step = cands.len().div_ceil(GAIN_SAMPLE);
    let (mut sample, mut bytes) = (Vec::new(), 0usize);
    for c in cands.iter().step_by(step) {
        if bytes >= GAIN_SAMPLE_BYTES {
            break;
        }
        // Deleted since the walk: it gains nothing.
        let (Some(data), Some(base)) = (fs.get(&c.hash).await?, fs.get(&c.base).await?) else {
            continue;
        };
        bytes += data.len() + base.len();
        sample.push((data, base, c.stored));
    }
    let (saved, stored) = on_low_thread(move || {
        let (mut saved, mut stored) = (0u64, 0u64);
        for (data, base, n) in sample {
            stored += n;
            if let Some(d) = leep_store::pack::compress_delta(&base, &data, level)? {
                saved += n.saturating_sub(d.len() as u64);
            }
        }
        Ok((saved, stored))
    })
    .await?;
    Ok(match stored {
        0 => 0,
        s => (u128::from(total) * u128::from(saved) / u128::from(s)) as u64,
    })
}

/// Walk the history of the delta packs `all` but `largest` (and of
/// `largest` too if `deep`), listing the objects they hold whole with the
/// base each should be compressed against. The walk passes through loose
/// objects and whole packs (history no delta run has taken yet) and stops
/// at changesets whose root tree is elsewhere.
#[allow(clippy::too_many_arguments)]
async fn consolidation_walk(
    fs: &FsBlobStore,
    repo: &Repo,
    limit: usize,
    level: i32,
    all: &HashSet<String>,
    largest: &str,
    deep: bool,
    loose: &HashSet<Hash>,
    whole_packs: &HashSet<String>,
) -> Result<ConsolidationWalk> {
    let walked: HashSet<String> = all
        .iter()
        .filter(|id| deep || *id != largest)
        .cloned()
        .collect();
    let in_largest = fs.packed_in(&HashSet::from([largest.to_string()]))?;
    let depth = fs.depth_in(all)?;
    let in_walked = fs.packed_in(&walked)?;
    let whole_walked = fs.whole_in(&walked)?;
    let in_whole = fs.packed_in(whole_packs)?;
    let wanted = |h: &Hash| whole_walked(h).is_some();
    let through = |h: &Hash| loose.contains(h) || in_whole(h) || in_walked(h);
    let order = leep_derive::pack_order::pack_order_where(repo, &wanted, &through, limit).await?;
    let (mut inside, mut outside) = (Vec::new(), Vec::new());
    for (hash, base) in order.objects.iter() {
        let (Some(base), Some(stored)) = (base, whole_walked(&hash)) else {
            continue;
        };
        // Not in the packs, or already as deep as a chain may go.
        if !depth(&base).is_some_and(|d| d < leep_store::pack::MAX_CHAIN) {
            continue;
        }
        let c = Candidate { hash, base, stored };
        if in_largest(&hash) {
            inside.push(c);
        } else {
            outside.push(c);
        }
    }
    drop((in_largest, depth, in_walked, whole_walked, in_whole));
    Ok(ConsolidationWalk {
        inside: measured_gain(fs, inside, level).await?,
        outside: measured_gain(fs, outside, level).await?,
        order,
    })
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
