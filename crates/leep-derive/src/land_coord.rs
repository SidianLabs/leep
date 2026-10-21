//! One land coordinator per ref across servers (ADR 0056).
//!
//! Group commit (ADR 0032) batches the lands one process receives. With
//! several servers on one ref their batches raced through the CAS, and one
//! server won every swap while the others' agents starved. A lease row in
//! the metadata store now names, per ref, the server that batches its lands;
//! the others forward lands to it. The lease decides only where lands are
//! batched: refs still move only by `cas_ref`, so a stale or doubled leader
//! makes lands slower, never wrong.
//!
//! The transport is the caller's ([`LandForward`]): `leep-server` forwards
//! over HTTP, tests call another `Repo` directly.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use async_trait::async_trait;
use leep_core::api::{LandResponse, FORWARD_FAILED_MARKER};
use leep_core::refname;
use leep_core::{
    ChangesetId, Error, LandId, Lease, LeaseClaim, LeaseKey, LeaseTiming, MetaStore, Result,
};
use tokio::sync::watch;
use tokio::time::Instant;

use crate::Repo;

/// The forwarding protocol this build speaks. A live lease naming another
/// protocol is left alone: lands on its ref are batched where they arrive.
pub const LAND_FORWARD_PROTOCOL: u32 = 1;

/// Lease timing and identity of one server process.
#[derive(Clone, Debug)]
pub struct CoordinatorConfig {
    /// Random per process start, so a restarted server never mistakes its
    /// predecessor's lease for its own.
    pub holder: String,
    /// Where other servers send this one lands (`--advertise-url`).
    pub address: String,
    /// Another server may take a lease not renewed for this long; the
    /// holder itself stops trusting it one `renew` earlier.
    pub ttl: Duration,
    /// How often held leases are renewed.
    pub renew: Duration,
    /// A lease with no land for this long is released.
    pub idle_release: Duration,
    /// How long one land keeps trying to reach the server holding its
    /// ref's lease before it is answered 409.
    pub forward_wait: Duration,
}

impl CoordinatorConfig {
    pub fn new(holder: impl Into<String>, address: impl Into<String>) -> Self {
        CoordinatorConfig {
            holder: holder.into(),
            address: address.into(),
            ttl: Duration::from_secs(2),
            renew: Duration::from_millis(500),
            idle_release: Duration::from_secs(30),
            forward_wait: Duration::from_secs(30),
        }
    }

    pub fn timing(&self) -> LeaseTiming {
        LeaseTiming {
            ttl: self.ttl,
            // One rule: a lease is taken over `ttl` after its last renewal.
            stale_after: self.ttl,
        }
    }
}

/// Where a land on a ref goes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Route {
    /// This server holds the lease at this epoch and batches the land.
    Lead(u64),
    /// Another server holds a live lease.
    Forward(Lease),
    /// No live lease is known and none could be taken (the store failed):
    /// batch it here, racing the CAS as before.
    Uncoordinated,
    /// This server is shutting down and no other server holds a live
    /// lease: the land is answered Not-Leader (503), and the client sends
    /// it again, to a server that will take the lease.
    ShuttingDown,
}

/// What a forward to the lease holder came back with.
pub enum Forwarded<A> {
    /// Its answer, whatever it was; relayed to the caller as is.
    Answered(A),
    /// It does not hold the lease any more.
    NotLeader,
    /// No answer: refused, reset, timed out, or the address is not allowed.
    /// The land may or may not have happened there. `reason` is a short
    /// metric label (`not_allowed`, `connect`, `timeout`, `reset`, ...).
    Failed {
        reason: &'static str,
        detail: String,
    },
}

impl<A> Forwarded<A> {
    pub fn failed(reason: &'static str, detail: impl Into<String>) -> Self {
        Forwarded::Failed {
            reason,
            detail: detail.into(),
        }
    }
}

/// Sends one land to the server named by a lease.
#[async_trait]
pub trait LandForward<A>: Send + Sync {
    async fn forward(&self, lease: &Lease) -> Forwarded<A>;
}

/// The outcome of [`Repo::land_coordinated`].
pub enum Coordinated<A> {
    /// Batched by this server.
    Landed(LandResponse),
    /// Batched by the lease holder, whose answer this is.
    Forwarded(A),
    /// A land forwarded here by another server, which this server no longer
    /// coordinates: the sender routes it again.
    NotLeader,
}

/// A lease this process holds: its ref and epoch.
type HeldLease = (String, u64);

struct Held {
    epoch: u64,
    /// This process's view of its lease ends here: the store-reported time
    /// left at the last successful renewal, counted from when that renewal
    /// was sent, less one renew interval.
    valid_until: Instant,
    last_used: Instant,
}

struct Seen {
    lease: Option<Lease>,
    read_at: Instant,
    /// A forward to it failed: read again before trusting it, but keep it
    /// as the fallback while the store cannot be read.
    stale: bool,
}

#[derive(Default)]
struct State {
    held: HashMap<String, Held>,
    seen: HashMap<String, Seen>,
    stopped: bool,
}

/// Takes, renews and releases this process's land leases, and routes lands.
pub struct LandCoordinator {
    meta: Arc<dyn MetaStore>,
    cfg: CoordinatorConfig,
    state: Mutex<State>,
    stop: watch::Sender<bool>,
    /// Lands batched here without a lease (see [`Route::Uncoordinated`]).
    uncoordinated: AtomicU64,
}

impl LandCoordinator {
    pub fn new(meta: Arc<dyn MetaStore>, cfg: CoordinatorConfig) -> Arc<Self> {
        Arc::new(LandCoordinator {
            meta,
            cfg,
            state: Mutex::default(),
            stop: watch::channel(false).0,
            uncoordinated: AtomicU64::new(0),
        })
    }

    pub fn config(&self) -> &CoordinatorConfig {
        &self.cfg
    }

    /// Renew held leases every `renew` and release idle ones, until
    /// [`shutdown`](Self::shutdown) or [`halt`](Self::halt).
    pub fn start(self: &Arc<Self>) {
        let me = Arc::clone(self);
        let mut stop = self.stop.subscribe();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = tokio::time::sleep(me.cfg.renew) => {}
                    _ = stop.changed() => return,
                }
                if *stop.borrow() {
                    return;
                }
                me.renew_all().await;
            }
        });
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn valid_until(&self, sent: Instant, remaining: Duration) -> Instant {
        sent + remaining.saturating_sub(self.cfg.renew)
    }

    fn set_held_gauge(&self, n: usize) {
        metrics::gauge!("leep_land_leases_held").set(n as f64);
    }

    /// Renew the land leases of `held` refs (the `land` lease scope).
    async fn renew(&self, held: &[(String, u64)]) -> Result<Vec<Option<Duration>>> {
        let keys: Vec<(LeaseKey, u64)> = held
            .iter()
            .map(|(r, e)| (LeaseKey::land(r.as_str()), *e))
            .collect();
        self.meta
            .renew_leases(&self.cfg.holder, &keys, self.cfg.timing())
            .await
    }

    async fn release(&self, r: &str, epoch: u64) -> Result<bool> {
        self.meta
            .release_lease(&LeaseKey::land(r), &self.cfg.holder, epoch)
            .await
    }

    async fn renew_all(&self) {
        let now = Instant::now();
        let (renew, idle): (Vec<HeldLease>, Vec<HeldLease>) = {
            let s = self.lock();
            let (idle, renew): (Vec<_>, Vec<_>) = s
                .held
                .iter()
                .map(|(r, h)| ((r.clone(), h.epoch), h.last_used))
                .partition(|(_, used)| now.duration_since(*used) >= self.cfg.idle_release);
            (
                renew.into_iter().map(|(k, _)| k).collect(),
                idle.into_iter().map(|(k, _)| k).collect(),
            )
        };
        for (r, epoch) in idle {
            // Gone from `held` first, so no land is routed to it meanwhile.
            self.lock().held.remove(&r);
            let _ = self.release(&r, epoch).await;
            metrics::counter!("leep_land_lease_lost_total", "reason" => "idle").increment(1);
        }
        if !renew.is_empty() {
            let sent = Instant::now();
            if let Ok(left) = self.renew(&renew).await {
                let mut s = self.lock();
                for ((r, epoch), left) in renew.iter().zip(left) {
                    match left {
                        Some(left) => {
                            if let Some(h) = s.held.get_mut(r).filter(|h| h.epoch == *epoch) {
                                h.valid_until = self.valid_until(sent, left);
                            }
                        }
                        None => {
                            if s.held.get(r).is_some_and(|h| h.epoch == *epoch) {
                                s.held.remove(r);
                                metrics::counter!("leep_land_lease_lost_total", "reason" => "taken")
                                    .increment(1);
                            }
                        }
                    }
                }
            }
            // A failed renewal leaves each view to run out on its own.
        }
        let n = self.lock().held.len();
        self.set_held_gauge(n);
    }

    /// Renew one lease now, because its view ran out before the periodic
    /// renewal came round.
    async fn renew_one(&self, r: &str, epoch: u64) -> bool {
        let sent = Instant::now();
        let held = [(r.to_string(), epoch)];
        match self.renew(&held).await {
            Ok(left) => {
                let mut s = self.lock();
                match left.first().copied().flatten() {
                    Some(left) if !s.stopped => {
                        if let Some(h) = s.held.get_mut(r).filter(|h| h.epoch == epoch) {
                            h.valid_until = self.valid_until(sent, left);
                            h.last_used = Instant::now();
                            return true;
                        }
                        false
                    }
                    Some(_) => false,
                    None => {
                        if s.held.remove(r).is_some() {
                            metrics::counter!("leep_land_lease_lost_total", "reason" => "taken")
                                .increment(1);
                        }
                        false
                    }
                }
            }
            Err(_) => false,
        }
    }

    /// The lease row of `r`, from a read at most half a renew interval old
    /// unless `fresh`.
    async fn read(&self, r: &str, fresh: bool) -> Result<Option<Lease>> {
        if !fresh {
            let s = self.lock();
            if let Some(seen) = s.seen.get(r).filter(|seen| !seen.stale) {
                let age = Instant::now().duration_since(seen.read_at);
                if age < self.cfg.renew / 2 {
                    return Ok(seen.lease.clone().map(|mut l| {
                        l.remaining = l.remaining.saturating_sub(age);
                        l
                    }));
                }
            }
        }
        let read_at = Instant::now();
        let lease = self
            .meta
            .lease(&LeaseKey::land(r), self.cfg.timing())
            .await?;
        self.lock().seen.insert(
            r.to_string(),
            Seen {
                lease: lease.clone(),
                read_at,
                stale: false,
            },
        );
        Ok(lease)
    }

    /// The last lease of `r` read, if it named another server and has not
    /// run out by its own reported time since: what a land falls back on
    /// while the store cannot be read, so it is never batched here under a
    /// lease another server may still hold (H1).
    fn cached_live(&self, r: &str) -> Option<Lease> {
        let s = self.lock();
        let seen = s.seen.get(r)?;
        let mut l = seen.lease.clone()?;
        let age = Instant::now().duration_since(seen.read_at);
        l.remaining = l.remaining.checked_sub(age)?;
        (l.is_live() && l.holder != self.cfg.holder).then_some(l)
    }

    /// Read `r`'s lease again before the next forward, after a forward to it
    /// failed.
    pub fn forget(&self, r: &str) {
        if let Some(seen) = self.lock().seen.get_mut(r) {
            seen.stale = true;
        }
    }

    /// Where a land on `r` goes now. Takes the lease when nobody holds a
    /// live one; never takes a live lease of another server.
    pub async fn route(&self, r: &str) -> Route {
        let now = Instant::now();
        let held = {
            let mut s = self.lock();
            let stopped = s.stopped;
            s.held.get_mut(r).filter(|_| !stopped).map(|h| {
                h.last_used = now;
                (h.epoch, h.valid_until)
            })
        };
        if let Some((epoch, until)) = held {
            if now < until || self.renew_one(r, epoch).await {
                return Route::Lead(epoch);
            }
        }
        let lease = match self.read(r, false).await {
            Ok(l) => l,
            Err(_) => {
                metrics::counter!("leep_land_lease_read_failed_total").increment(1);
                return match self.cached_live(r) {
                    Some(l) => self.forward_to(l),
                    None if self.lock().stopped => Route::ShuttingDown,
                    None => Route::Uncoordinated,
                };
            }
        };
        if let Some(l) = lease.filter(|l| l.is_live() && l.holder != self.cfg.holder) {
            return self.forward_to(l);
        }
        if self.lock().stopped {
            return Route::ShuttingDown;
        }
        let key = LeaseKey::land(r);
        let claim = LeaseClaim {
            key: &key,
            holder: &self.cfg.holder,
            address: &self.cfg.address,
            protocol: LAND_FORWARD_PROTOCOL,
            meta: "",
        };
        let sent = Instant::now();
        match self.meta.acquire_lease(&claim, self.cfg.timing()).await {
            Ok(l) if l.holder == self.cfg.holder => {
                let mut s = self.lock();
                s.seen.remove(r);
                let epoch = l.epoch;
                // Lands that arrive together all claim the free lease; only
                // the first claim of an epoch counts as taking it.
                let before = s.held.insert(
                    r.to_string(),
                    Held {
                        epoch,
                        valid_until: self.valid_until(sent, l.remaining),
                        last_used: Instant::now(),
                    },
                );
                let n = s.held.len();
                drop(s);
                self.set_held_gauge(n);
                if before.is_none_or(|h| h.epoch != epoch) {
                    metrics::counter!("leep_land_lease_acquired_total").increment(1);
                    tracing::info!(r#ref = r, epoch, "took the land lease");
                }
                Route::Lead(epoch)
            }
            Ok(l) if l.is_live() => {
                self.lock().seen.insert(
                    r.to_string(),
                    Seen {
                        lease: Some(l.clone()),
                        read_at: sent,
                        stale: false,
                    },
                );
                self.forward_to(l)
            }
            Ok(_) | Err(_) => Route::Uncoordinated,
        }
    }

    fn forward_to(&self, l: Lease) -> Route {
        if l.protocol == LAND_FORWARD_PROTOCOL {
            Route::Forward(l)
        } else {
            Route::Uncoordinated
        }
    }

    /// Whether this server still holds a lease on `r`, renewing it now if
    /// its view ran out. A leader asks before each batch attempt, so one
    /// whose lease is gone stops retrying and hands its lands back.
    pub(crate) async fn still_leading(&self, r: &str) -> bool {
        let now = Instant::now();
        let held = {
            let mut s = self.lock();
            if s.stopped {
                return false;
            }
            s.held.get_mut(r).map(|h| {
                h.last_used = now;
                (h.epoch, h.valid_until)
            })
        };
        match held {
            Some((_, until)) if now < until => true,
            Some((epoch, _)) => self.renew_one(r, epoch).await,
            None => false,
        }
    }

    /// Resolves once `r`'s lease is no longer `lease` (another holder or
    /// epoch), or is no longer live. A forward waits on it, so a land never
    /// waits on a holder that has gone.
    pub async fn lease_changed(&self, r: &str, lease: &Lease) {
        loop {
            tokio::time::sleep(self.cfg.renew / 2).await;
            match self.read(r, false).await {
                Ok(Some(l))
                    if l.is_live() && l.holder == lease.holder && l.epoch == lease.epoch => {}
                Ok(_) => return,
                Err(_) => {}
            }
        }
    }

    /// Stop renewing and give up every lease, so other servers take them at
    /// once; lands this server still holds are handed back to be routed.
    pub async fn shutdown(&self) {
        let held: Vec<(String, u64)> = {
            let mut s = self.lock();
            s.stopped = true;
            s.held.drain().map(|(r, h)| (r, h.epoch)).collect()
        };
        self.stop.send_replace(true);
        for (r, epoch) in held {
            let _ = self.release(&r, epoch).await;
        }
        self.set_held_gauge(0);
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

    /// Lands this server batched without holding their ref's lease.
    pub fn uncoordinated_lands(&self) -> u64 {
        self.uncoordinated.load(Ordering::Relaxed)
    }

    /// Epoch of the lease this process holds on `r`, if any.
    pub fn held_epoch(&self, r: &str) -> Option<u64> {
        self.lock().held.get(r).map(|h| h.epoch)
    }
}

impl Repo {
    /// [`land_with`](Repo::land_with) through this server's
    /// [`LandCoordinator`], if it has one: the land is batched here when
    /// this server holds the ref's lease (taking it if nobody holds a live
    /// one), and otherwise sent with `forward` to the server that does.
    ///
    /// A land the holder does not answer is sent again, to it or to whoever
    /// holds the lease next, but never batched here while a live lease names
    /// another server: it may still be in that server's batch, and two
    /// servers batching one ref race and starve each other. It is batched
    /// here only when no live lease exists and none can be taken. Every
    /// repeat is safe because `land` is idempotent (ADR 0021).
    ///
    /// `from_peer` marks a land another server forwarded: it is never
    /// forwarded again, and is answered [`Coordinated::NotLeader`] when this
    /// server does not hold the lease.
    #[allow(clippy::too_many_arguments)]
    pub async fn land_coordinated<A: Send>(
        &self,
        cs: ChangesetId,
        target_ref: &str,
        actor: Option<&str>,
        create: bool,
        land_id: Option<LandId>,
        from_peer: bool,
        forward: &dyn LandForward<A>,
    ) -> Result<Coordinated<A>> {
        let Some(coord) = self.land_coordinator.clone() else {
            return Ok(Coordinated::Landed(
                self.land_keyed(cs, target_ref, actor, create, land_id.clone())
                    .await?,
            ));
        };
        refname::validate(target_ref)?;
        if refname::is_tag(target_ref) {
            return Err(Error::Invalid(format!(
                "cannot land onto {target_ref:?}: tags are immutable"
            )));
        }
        let started = Instant::now();
        loop {
            match coord.route(target_ref).await {
                // Only the holder takes a forwarded land; the sender routes
                // it again and may take the lease itself.
                Route::Uncoordinated if from_peer => return Ok(Coordinated::NotLeader),
                // Shutting down: never batch a land here after giving the
                // lease up. The client sends it again (with its land id).
                Route::ShuttingDown => return Ok(Coordinated::NotLeader),
                Route::Uncoordinated => {
                    coord.uncoordinated.fetch_add(1, Ordering::Relaxed);
                    metrics::counter!("leep_land_routed_total", "route" => "uncoordinated")
                        .increment(1);
                    return Ok(Coordinated::Landed(
                        self.land_keyed(cs, target_ref, actor, create, land_id.clone())
                            .await?,
                    ));
                }
                Route::Lead(_) => match self
                    .land_led(cs, target_ref, actor, create, land_id.clone())
                    .await
                {
                    Some(r) => return r.map(Coordinated::Landed),
                    None => {
                        metrics::counter!("leep_land_rerouted_total").increment(1);
                        if from_peer {
                            return Ok(Coordinated::NotLeader);
                        }
                    }
                },
                Route::Forward(lease) => {
                    if from_peer {
                        return Ok(Coordinated::NotLeader);
                    }
                    let outcome = tokio::select! {
                        o = forward.forward(&lease) => Some(o),
                        () = coord.lease_changed(target_ref, &lease) => None,
                    };
                    let label = match outcome {
                        Some(Forwarded::Answered(a)) => {
                            metrics::counter!("leep_land_forwarded_total", "outcome" => "answered")
                                .increment(1);
                            return Ok(Coordinated::Forwarded(a));
                        }
                        Some(Forwarded::NotLeader) => "not_leader",
                        Some(Forwarded::Failed { reason, detail }) => {
                            tracing::debug!(
                                r#ref = target_ref,
                                to = %lease.address,
                                reason,
                                error = %detail,
                                "forwarding a land failed"
                            );
                            metrics::counter!(
                                "leep_land_forwarded_total",
                                "outcome" => "failed",
                                "reason" => reason
                            )
                            .increment(1);
                            // Wait for the holder to come back, renew, or
                            // be replaced before asking again.
                            tokio::select! {
                                _ = tokio::time::sleep(coord.config().renew) => {}
                                () = coord.lease_changed(target_ref, &lease) => {}
                            }
                            ""
                        }
                        None => "lease_changed",
                    };
                    if !label.is_empty() {
                        metrics::counter!("leep_land_forwarded_total", "outcome" => label)
                            .increment(1);
                    }
                    coord.forget(target_ref);
                }
            }
            if started.elapsed() >= coord.config().forward_wait {
                metrics::counter!("leep_land_forward_gave_up_total").increment(1);
                return Err(Error::Conflict(format!(
                    "land onto {target_ref:?} {FORWARD_FAILED_MARKER}; retry"
                )));
            }
        }
    }
}
