//! The garbage collection lease (ADR 0059 section 6): blob GC's condemn and
//! collect and the changeset sweep's phases each hold it while they run, so
//! no two run at once. ADR 0012 and ADR 0059's safety arguments assume
//! that; this makes it hold rather than leaving it to the operator.
//!
//! The lease is renewed in the background. A run that loses it (a pause
//! longer than the TTL, a store outage) stops before its next destructive
//! step instead of deleting beside another holder.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use leep_core::{Error, LeaseClaim, LeaseKey, LeaseTiming, MetaStore, Result};

/// How long the lease stays live without a renewal.
pub const GC_LEASE_TTL: Duration = Duration::from_secs(60);
/// How often the holder renews it.
pub const GC_LEASE_RENEW: Duration = Duration::from_secs(10);
/// The protocol a GC holder publishes; nothing is ever forwarded to it.
const GC_LEASE_PROTOCOL: u32 = 1;

fn timing() -> LeaseTiming {
    LeaseTiming {
        ttl: GC_LEASE_TTL,
        stale_after: GC_LEASE_TTL,
    }
}

/// A unique holder name for this process and run.
fn holder_name(job: &str) -> String {
    static N: AtomicU64 = AtomicU64::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    format!(
        "{job}-{}-{nanos:x}-{}",
        std::process::id(),
        N.fetch_add(1, Ordering::Relaxed)
    )
}

/// The held lease. Release it with [`GcLease::release`]; dropping it stops
/// the renewals, and the lease then lapses after [`GC_LEASE_TTL`].
pub struct GcLease {
    meta: Arc<dyn MetaStore>,
    holder: String,
    epoch: u64,
    lost: Arc<AtomicBool>,
    /// When the last successful take or renewal was sent: the lease is
    /// certainly ours for a TTL after that, and no longer.
    last_ok: Arc<std::sync::Mutex<tokio::time::Instant>>,
    renewer: Option<tokio::task::JoinHandle<()>>,
}

impl GcLease {
    /// Take the lease for `job` (published as the lease's `meta`, so a
    /// refused run can say what holds it). Refuses with `Conflict` while
    /// another holder's lease is live. A store without leases (test doubles
    /// only; every backend has them) runs unguarded, with a warning.
    pub async fn acquire(meta: &Arc<dyn MetaStore>, job: &str) -> Result<Option<GcLease>> {
        if !meta.supports_leases() {
            tracing::warn!(
                job,
                "this metadata store has no leases: one GC at a time is not enforced"
            );
            return Ok(None);
        }
        let key = &LeaseKey::gc();
        let take = |holder: String| async move {
            let claim = LeaseClaim {
                key,
                holder: &holder,
                address: "",
                protocol: GC_LEASE_PROTOCOL,
                meta: job,
            };
            let lease = meta.acquire_lease(&claim, timing()).await?;
            Ok::<_, Error>((holder, lease))
        };
        // Named as abandoned until the lease is in hand, so a run stopped
        // during the call leaves a name the next run can take back.
        let sent = tokio::time::Instant::now();
        let fresh = holder_name(job);
        ABANDONED.lock().unwrap().push(fresh.clone());
        let (mut holder, mut lease) = take(fresh).await?;
        if lease.holder != holder {
            // An earlier run of this process that stopped holds it: its
            // holder name takes it back.
            let mine = ABANDONED.lock().unwrap().contains(&lease.holder);
            if mine {
                let h = lease.holder.clone();
                (holder, lease) = take(h).await?;
            }
        }
        if lease.holder == holder {
            ABANDONED.lock().unwrap().retain(|h| *h != holder);
        }
        if lease.holder != holder {
            return Err(Error::Conflict(format!(
                "another garbage collection is running ({}, holder {}, {}s left on its lease); \
                 only one GC runs at a time (ADR 0059)",
                if lease.meta.is_empty() {
                    "unknown job"
                } else {
                    &lease.meta
                },
                lease.holder,
                lease.remaining.as_secs()
            )));
        }
        let lost = Arc::new(AtomicBool::new(false));
        let last_ok = Arc::new(std::sync::Mutex::new(sent));
        let renewer = tokio::spawn(renew(
            meta.clone(),
            holder.clone(),
            lease.epoch,
            lost.clone(),
            last_ok.clone(),
        ));
        Ok(Some(GcLease {
            meta: meta.clone(),
            holder,
            epoch: lease.epoch,
            lost,
            last_ok,
            renewer: Some(renewer),
        }))
    }

    /// `Err` once the lease may have passed to another holder: call before
    /// every destructive step.
    ///
    /// Judged by when the last renewal that succeeded was *sent*, not by a
    /// flag the renewer sets: a renewer stalled in a store call sets
    /// nothing, and a renewal that succeeds late (it matches holder and
    /// epoch even after expiry) cannot vouch for the time in between. It
    /// fails a renewal period short of the TTL, before another holder can
    /// take the lease.
    pub fn check(&self) -> Result<()> {
        let since = self.last_ok.lock().unwrap().elapsed();
        if self.lost.load(Ordering::SeqCst) || since >= GC_LEASE_TTL - GC_LEASE_RENEW {
            return Err(Error::Conflict(format!(
                "lost the gc lease (not renewed for {}s, TTL {}s); stopping",
                since.as_secs(),
                GC_LEASE_TTL.as_secs()
            )));
        }
        Ok(())
    }

    /// Stop renewing and give the lease up at once.
    pub async fn release(mut self) -> Result<()> {
        if let Some(t) = self.renewer.take() {
            t.abort();
        }
        // Abandoned until the release is confirmed, as in `acquire`.
        ABANDONED.lock().unwrap().push(self.holder.clone());
        self.meta
            .release_lease(&LeaseKey::gc(), &self.holder, self.epoch)
            .await?;
        ABANDONED.lock().unwrap().retain(|h| *h != self.holder);
        Ok(())
    }
}

impl Drop for GcLease {
    /// A run stopped in-process (its future dropped) leaves its holder name
    /// for the next run of this process to take over at once; a killed
    /// process cannot, and its lease lapses after [`GC_LEASE_TTL`].
    fn drop(&mut self) {
        if let Some(t) = self.renewer.take() {
            t.abort();
            ABANDONED.lock().unwrap().push(self.holder.clone());
        }
    }
}

/// Holder names of this process's runs that stopped without releasing.
static ABANDONED: std::sync::Mutex<Vec<String>> = std::sync::Mutex::new(Vec::new());

/// [`GcLease::check`] on an optional lease.
pub fn check(lease: &Option<GcLease>) -> Result<()> {
    lease.as_ref().map_or(Ok(()), GcLease::check)
}

/// Release an optional lease, keeping `result`'s error if it has one.
pub async fn release<T>(lease: Option<GcLease>, result: Result<T>) -> Result<T> {
    let released = match lease {
        Some(l) => l.release().await,
        None => Ok(()),
    };
    let out = result?;
    released?;
    Ok(out)
}

async fn renew(
    meta: Arc<dyn MetaStore>,
    holder: String,
    epoch: u64,
    lost: Arc<AtomicBool>,
    shared_ok: Arc<std::sync::Mutex<tokio::time::Instant>>,
) {
    let key = LeaseKey::gc();
    let mut last_ok = *shared_ok.lock().unwrap();
    loop {
        tokio::time::sleep(GC_LEASE_RENEW).await;
        let sent = tokio::time::Instant::now();
        match meta
            .renew_leases(&holder, &[(key.clone(), epoch)], timing())
            .await
        {
            Ok(v) if v.first().is_some_and(|r| r.is_some()) => {
                last_ok = sent;
                *shared_ok.lock().unwrap() = sent;
            }
            Ok(_) => {
                lost.store(true, Ordering::SeqCst);
                return;
            }
            Err(e) => {
                tracing::warn!(error = %e, "renewing the gc lease failed");
                // Give up a renewal short of the TTL, so the run stops
                // before another holder could take the lease.
                if last_ok.elapsed() + GC_LEASE_RENEW >= GC_LEASE_TTL {
                    lost.store(true, Ordering::SeqCst);
                    return;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_second_gc_is_refused_until_the_first_releases() {
        let meta: Arc<dyn MetaStore> = Arc::new(leep_core::memory::MemoryMetaStore::new());
        let first = GcLease::acquire(&meta, "changeset-collect")
            .await
            .unwrap()
            .expect("memory store has leases");
        let Err(e) = GcLease::acquire(&meta, "blob-collect").await else {
            panic!("second holder got the lease");
        };
        assert!(matches!(e, Error::Conflict(_)), "{e}");
        assert!(e.to_string().contains("changeset-collect"), "{e}");
        first.check().unwrap();
        first.release().await.unwrap();
        GcLease::acquire(&meta, "blob-collect")
            .await
            .unwrap()
            .unwrap();
    }

    /// A holder whose renewals stall (a store that hangs, a paused
    /// process) must stop by itself before another holder could take the
    /// lease: renewals that succeed late cannot vouch for the time between.
    #[tokio::test(start_paused = true)]
    async fn a_holder_whose_renewals_stall_stops_before_the_ttl() {
        use leep_testkit::{Fault, FaultPlan, FaultyMetaStore, Op};
        let plan = FaultPlan::new(1);
        // The first renewal (call 2) hangs for an hour.
        plan.script(Op::Lease, 2, Fault::Delay(Duration::from_secs(3600)));
        let inner: Arc<dyn MetaStore> = Arc::new(
            leep_core::memory::MemoryMetaStore::new().with_lease_clock(Arc::new({
                let origin = tokio::time::Instant::now();
                move || origin.elapsed().as_millis() as i64
            })),
        );
        let meta: Arc<dyn MetaStore> = Arc::new(FaultyMetaStore::new(inner, plan));
        let lease = GcLease::acquire(&meta, "changeset-collect")
            .await
            .unwrap()
            .unwrap();
        tokio::time::sleep(GC_LEASE_RENEW + Duration::from_secs(1)).await;
        lease.check().unwrap();
        tokio::time::sleep(GC_LEASE_TTL - GC_LEASE_RENEW * 2).await;
        assert!(
            lease.check().is_err(),
            "still acting on a lease not renewed for {:?}",
            GC_LEASE_TTL - GC_LEASE_RENEW + Duration::from_secs(1)
        );
    }
}
