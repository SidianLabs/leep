//! Index leases (ADR 0058 section 2): rows of ADR 0056's `leases` table in
//! the `index` scope, one per index group, so one process per fleet builds
//! each group. A lease decides only who derives: rows are pure functions of
//! history, so two processes that both believe they hold one write the same
//! bytes, and a stale holder's late write is wasted work, never damage
//! (tools that delete rows wait out a fence instead, [`ToolLease`]).

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use leep_core::{Error, Lease, LeaseClaim, LeaseKey, LeaseTiming, MetaStore, Result};
use tokio::sync::watch;
use tokio::time::Instant;

use super::IndexGroup;

/// The protocol an index holder speaks. Nothing is forwarded to index
/// holders yet; a later change can refuse to share a lease with an
/// incompatible holder by it.
pub const INDEX_LEASE_PROTOCOL: u32 = 1;

/// How far back the published rate looks.
const RATE_WINDOW: Duration = Duration::from_secs(60);

/// How long a read of another holder's lease is trusted by query servers.
const READ_CACHE: Duration = Duration::from_secs(1);

/// Timing and identity of one process's index leases.
#[derive(Clone, Debug)]
pub struct IndexLeaseConfig {
    /// Random per process start.
    pub holder: String,
    /// `--advertise-url`, for operators only.
    pub address: String,
    /// A lease stays live this long after a renewal (15 s).
    pub ttl: Duration,
    /// Held leases are renewed this often (3 s).
    pub renew: Duration,
    /// Another process may take a lease not renewed for this long (6 s).
    pub stale_after: Duration,
    /// Groups this process competes for (`--index-kinds`); none for a
    /// process that only reads leases (`off` mode).
    pub candidate: Vec<IndexGroup>,
}

impl IndexLeaseConfig {
    pub fn new(holder: impl Into<String>, address: impl Into<String>) -> Self {
        IndexLeaseConfig {
            holder: holder.into(),
            address: address.into(),
            ttl: Duration::from_secs(15),
            renew: Duration::from_secs(3),
            stale_after: Duration::from_secs(6),
            candidate: Vec::new(),
        }
    }

    pub fn with_candidates(mut self, groups: Vec<IndexGroup>) -> Self {
        self.candidate = groups;
        self
    }

    pub fn timing(&self) -> LeaseTiming {
        LeaseTiming {
            ttl: self.ttl,
            stale_after: self.stale_after,
        }
    }
}

/// What a holder publishes in its lease's `meta` on every renewal: its
/// rate over the last minute and its backlog, which query servers turn
/// into `Retry-After`.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct HolderMeta {
    /// Changesets indexed per second.
    pub rate: f64,
    /// Changesets the last pass found missing.
    pub backlog: u64,
}

/// Read a holder's `meta`; anything unreadable counts as unknown (zeros).
pub fn parse_holder_meta(meta: &str) -> HolderMeta {
    let Ok(v) = serde_json::from_str::<serde_json::Value>(meta) else {
        return HolderMeta::default();
    };
    HolderMeta {
        rate: v.get("rate").and_then(|r| r.as_f64()).unwrap_or(0.0),
        backlog: v.get("backlog").and_then(|r| r.as_u64()).unwrap_or(0),
    }
}

struct Held {
    epoch: u64,
    /// The view ends here: the store-reported time left at the last
    /// successful claim, from when it was sent, less half a renew interval.
    valid_until: Instant,
}

struct Seen {
    lease: Option<Lease>,
    read_at: Instant,
}

#[derive(Default)]
struct Progress {
    done: VecDeque<(Instant, u64)>,
    backlog: u64,
}

#[derive(Default)]
struct State {
    held: HashMap<IndexGroup, Held>,
    seen: HashMap<IndexGroup, Seen>,
    progress: HashMap<IndexGroup, Progress>,
    stopped: bool,
}

/// Takes, renews and releases this process's index leases, and reads other
/// processes' for queries.
pub struct IndexLeases {
    meta: Arc<dyn MetaStore>,
    cfg: IndexLeaseConfig,
    state: Mutex<State>,
    stop: watch::Sender<bool>,
}

impl IndexLeases {
    pub fn new(meta: Arc<dyn MetaStore>, cfg: IndexLeaseConfig) -> Arc<Self> {
        Arc::new(IndexLeases {
            meta,
            cfg,
            state: Mutex::default(),
            stop: watch::channel(false).0,
        })
    }

    pub fn config(&self) -> &IndexLeaseConfig {
        &self.cfg
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Claim free leases and renew held ones now, then every `renew`, until
    /// [`shutdown`](Self::shutdown) or [`halt`](Self::halt).
    pub fn start(self: &Arc<Self>) {
        if self.cfg.candidate.is_empty() {
            return;
        }
        let me = Arc::clone(self);
        let mut stop = self.stop.subscribe();
        tokio::spawn(async move {
            loop {
                if *stop.borrow() {
                    return;
                }
                me.tick().await;
                tokio::select! {
                    _ = tokio::time::sleep(me.cfg.renew) => {}
                    _ = stop.changed() => return,
                }
            }
        });
    }

    /// One renewal round: every candidate group is claimed, which renews a
    /// held lease (publishing this process's rate and backlog), takes a free
    /// one, and leaves a live lease of another process alone.
    pub async fn tick(&self) {
        for g in self.cfg.candidate.clone() {
            self.claim(g).await;
        }
    }

    /// Claim `g` once. Returns whether this process holds it afterwards.
    async fn claim(&self, g: IndexGroup) -> bool {
        if self.lock().stopped {
            return false;
        }
        let key = LeaseKey::index(g.lease_name());
        let meta = self.meta_json(g);
        let claim = LeaseClaim {
            key: &key,
            holder: &self.cfg.holder,
            address: &self.cfg.address,
            protocol: INDEX_LEASE_PROTOCOL,
            meta: &meta,
        };
        let sent = Instant::now();
        let result = self.meta.acquire_lease(&claim, self.cfg.timing()).await;
        let kind = g.lease_name();
        let mut s = self.lock();
        if s.stopped {
            return false;
        }
        let holds = match result {
            Ok(l) if l.holder == self.cfg.holder && l.is_live() => {
                let valid_until = sent + l.remaining.saturating_sub(self.cfg.renew / 2);
                let before = s.held.insert(
                    g,
                    Held {
                        epoch: l.epoch,
                        valid_until,
                    },
                );
                if before.is_none_or(|h| h.epoch != l.epoch) {
                    metrics::counter!("leep_index_lease_acquired_total", "kind" => kind)
                        .increment(1);
                    tracing::info!(kind, epoch = l.epoch, "took the index lease");
                }
                s.seen.insert(
                    g,
                    Seen {
                        lease: Some(l),
                        read_at: sent,
                    },
                );
                true
            }
            Ok(l) => {
                if s.held.remove(&g).is_some() {
                    metrics::counter!("leep_index_lease_lost_total", "kind" => kind, "reason" => "taken")
                        .increment(1);
                    tracing::warn!(kind, holder = %l.holder, "lost the index lease");
                }
                s.seen.insert(
                    g,
                    Seen {
                        lease: Some(l),
                        read_at: sent,
                    },
                );
                false
            }
            Err(e) => {
                let expired = s
                    .held
                    .get(&g)
                    .is_some_and(|h| Instant::now() >= h.valid_until);
                if expired {
                    s.held.remove(&g);
                    metrics::counter!("leep_index_lease_lost_total", "kind" => kind, "reason" => "store")
                        .increment(1);
                    tracing::warn!(kind, error = %e, "lost the index lease: renewals failed");
                }
                s.held.contains_key(&g)
            }
        };
        let held = s.held.contains_key(&g);
        drop(s);
        metrics::gauge!("leep_index_lease_held", "kind" => kind).set(if held { 1.0 } else { 0.0 });
        holds
    }

    /// Whether this process holds `g`'s lease by its current view.
    pub fn holds(&self, g: IndexGroup) -> bool {
        let s = self.lock();
        !s.stopped
            && s.held
                .get(&g)
                .is_some_and(|h| Instant::now() < h.valid_until)
    }

    /// [`holds`](Self::holds), renewing on the spot when the view ran out
    /// before the periodic renewal came round. The warmer asks before each
    /// write of a derivation and between passes.
    pub async fn check(&self, g: IndexGroup) -> bool {
        if self.holds(g) {
            return true;
        }
        let thinks = {
            let s = self.lock();
            !s.stopped && s.held.contains_key(&g)
        };
        thinks && self.claim(g).await
    }

    /// The groups this process holds now.
    pub fn held(&self) -> Vec<IndexGroup> {
        IndexGroup::ALL
            .into_iter()
            .filter(|g| self.holds(*g))
            .collect()
    }

    /// `g`'s lease if any process holds it live, this one included, from a
    /// read at most a second old. When the store cannot be read, the last
    /// lease read stands while its own reported time lasts: queries must
    /// not start deriving inline because a read failed.
    pub async fn live(&self, g: IndexGroup) -> Option<Lease> {
        if self.holds(g) {
            return self.lock().seen.get(&g).and_then(|s| s.lease.clone());
        }
        let cached = {
            let s = self.lock();
            s.seen.get(&g).map(|seen| {
                let age = Instant::now().duration_since(seen.read_at);
                (
                    age,
                    seen.lease.clone().map(|mut l| {
                        l.remaining = l.remaining.saturating_sub(age);
                        l
                    }),
                )
            })
        };
        if let Some((age, lease)) = &cached {
            if *age < READ_CACHE {
                return lease.clone().filter(Lease::is_live);
            }
        }
        let key = LeaseKey::index(g.lease_name());
        let read_at = Instant::now();
        match self.meta.lease(&key, self.cfg.timing()).await {
            Ok(lease) => {
                self.lock().seen.insert(
                    g,
                    Seen {
                        lease: lease.clone(),
                        read_at,
                    },
                );
                lease.filter(Lease::is_live)
            }
            Err(e) => {
                tracing::debug!(kind = g.lease_name(), error = %e, "reading the index lease failed");
                cached.and_then(|(_, l)| l).filter(Lease::is_live)
            }
        }
    }

    /// Record a pass of the holder: `derived` changesets indexed, `backlog`
    /// found missing, for the rate and backlog it publishes.
    pub fn record(&self, g: IndexGroup, derived: u64, backlog: u64) {
        let now = Instant::now();
        let mut s = self.lock();
        let p = s.progress.entry(g).or_default();
        p.done.push_back((now, derived));
        while p
            .done
            .front()
            .is_some_and(|(t, _)| now.duration_since(*t) > RATE_WINDOW)
        {
            p.done.pop_front();
        }
        p.backlog = backlog;
        metrics::gauge!("leep_index_backlog_changesets", "kind" => g.lease_name(), "lane" => "tip")
            .set(backlog as f64);
    }

    /// This process's rate and backlog for `g`.
    pub fn progress(&self, g: IndexGroup) -> HolderMeta {
        let now = Instant::now();
        let s = self.lock();
        let Some(p) = s.progress.get(&g) else {
            return HolderMeta::default();
        };
        let done: u64 = p
            .done
            .iter()
            .filter(|(t, _)| now.duration_since(*t) <= RATE_WINDOW)
            .map(|(_, n)| n)
            .sum();
        let span = p
            .done
            .front()
            .map_or(RATE_WINDOW, |(t, _)| now.duration_since(*t))
            .clamp(Duration::from_secs(1), RATE_WINDOW);
        HolderMeta {
            rate: done as f64 / span.as_secs_f64(),
            backlog: p.backlog,
        }
    }

    fn meta_json(&self, g: IndexGroup) -> String {
        let p = self.progress(g);
        // Shard count: 1 until repository shards exist (ADR 0058 section 4).
        format!(
            "{{\"rate\":{:.3},\"backlog\":{},\"shards\":1}}",
            p.rate, p.backlog
        )
    }

    /// Stop renewing and give every lease up, so another process takes them
    /// at once.
    pub async fn shutdown(&self) {
        let held: Vec<(IndexGroup, u64)> = {
            let mut s = self.lock();
            s.stopped = true;
            s.held.drain().map(|(g, h)| (g, h.epoch)).collect()
        };
        self.stop.send_replace(true);
        for (g, epoch) in held {
            let key = LeaseKey::index(g.lease_name());
            let _ = self.meta.release_lease(&key, &self.cfg.holder, epoch).await;
            metrics::gauge!("leep_index_lease_held", "kind" => g.lease_name()).set(0.0);
        }
    }

    /// Stop renewing without releasing anything, as a killed process would.
    #[doc(hidden)]
    pub fn halt(&self) {
        let mut s = self.lock();
        s.stopped = true;
        s.held.clear();
        drop(s);
        self.stop.send_replace(true);
    }

    /// Epoch of the lease this process holds on `g`, if any.
    pub fn held_epoch(&self, g: IndexGroup) -> Option<u64> {
        self.lock().held.get(&g).map(|h| h.epoch)
    }
}

/// Index leases held by a tool that deletes or rebuilds rows (`repair drop`,
/// `repair rederive --missing`, `upgrade-index --yes`; ADR 0058 section 6).
/// It takes the leases of the groups it touches, refusing when another
/// process holds one live, keeps them renewed while it runs, and only then
/// waits the fence, so a former holder's write still in flight lands before
/// the tool touches rows.
pub struct ToolLease {
    leases: Arc<IndexLeases>,
    groups: Vec<IndexGroup>,
}

impl ToolLease {
    /// Take the leases of `groups` as `holder`. A live lease of another
    /// process is refused unless `ignore_live`, which goes ahead without it
    /// (duplicate work, correct rows). Then wait `fence`, unless no group
    /// had a holder before.
    pub async fn take(
        meta: Arc<dyn MetaStore>,
        holder: &str,
        groups: &[IndexGroup],
        ignore_live: bool,
        fence: Duration,
        say: &(dyn Fn(&str) + Send + Sync),
    ) -> Result<ToolLease> {
        let mut cfg = IndexLeaseConfig::new(holder, "");
        cfg.candidate = groups.to_vec();
        let leases = IndexLeases::new(meta.clone(), cfg);
        let mut held = Vec::new();
        let mut had_holder = false;
        for &g in groups {
            let key = LeaseKey::index(g.lease_name());
            let before = meta.lease(&key, leases.cfg.timing()).await?;
            had_holder |= before.is_some();
            if leases.claim(g).await {
                held.push(g);
                continue;
            }
            let current = meta.lease(&key, leases.cfg.timing()).await?;
            match current.filter(Lease::is_live) {
                Some(l) if !ignore_live => {
                    return Err(Error::Invalid(format!(
                        "the {} index lease is held by {} ({}); stop it, switch it to \
                         `--indexing off`, or pass --ignore-lease to work beside it",
                        g.lease_name(),
                        l.holder,
                        if l.address.is_empty() {
                            "no address"
                        } else {
                            l.address.as_str()
                        }
                    )))
                }
                Some(l) => say(&format!(
                    "warning: the {} index lease is held by {}; going on without it (--ignore-lease)",
                    g.lease_name(),
                    l.holder
                )),
                None => {
                    return Err(Error::Storage(format!(
                        "could not take the {} index lease",
                        g.lease_name()
                    )))
                }
            }
        }
        leases.start();
        let tool = ToolLease {
            leases,
            groups: held,
        };
        if had_holder && !fence.is_zero() && !tool.groups.is_empty() {
            say(&format!(
                "holding the index lease of {}; waiting {}s for a former holder's writes",
                tool.groups
                    .iter()
                    .map(|g| g.lease_name())
                    .collect::<Vec<_>>()
                    .join(", "),
                fence.as_secs()
            ));
            tokio::time::sleep(fence).await;
            for &g in &tool.groups {
                if !tool.leases.check(g).await {
                    return Err(Error::Storage(format!(
                        "lost the {} index lease while waiting the fence",
                        g.lease_name()
                    )));
                }
            }
        }
        Ok(tool)
    }

    /// Whether every lease taken is still held.
    pub async fn still_held(&self) -> bool {
        for &g in &self.groups {
            if !self.leases.check(g).await {
                return false;
            }
        }
        true
    }

    /// The groups whose lease was taken.
    pub fn groups(&self) -> &[IndexGroup] {
        &self.groups
    }

    /// Give the leases up.
    pub async fn release(self) {
        self.leases.shutdown().await;
    }
}
