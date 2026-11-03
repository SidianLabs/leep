//! The warm floor of a server's Postgres pools (ADR 0084): connections
//! opened at startup and kept open, so a fresh or long-idle server's first
//! requests do not pay TCP, authentication and `after_connect` (6 ms of a
//! fresh server's first log, 9-14 ms of the first request after 20 idle
//! minutes; docs/PERFORMANCE.md).
//!
//! sqlx keeps `min_connections` only by reopening a connection right after
//! its reaper closed one for idleness or age; it opens nothing up front for
//! a lazy pool, and a reopen that fails is not retried. So the floor is
//! opened here at startup and topped up by a keeper task.

use std::time::{Duration, Instant};

use sqlx::PgPool;

/// How long opening the floor may hold up a server's start. Past it the
/// server starts anyway and the keeper finishes the job.
pub(crate) const OPEN_TIMEOUT: Duration = Duration::from_secs(10);

/// How often the keeper looks for a pool below its floor: well under
/// sqlx's 10 min idle timeout, so a floor lost to a failed reopen or a
/// restarted database is back before the next quiet period ends.
const KEEP_INTERVAL: Duration = Duration::from_secs(30);

/// How long one top-up may take before the keeper gives up until the next
/// interval.
const KEEP_TIMEOUT: Duration = Duration::from_secs(5);

/// One pool and the connections to keep open in it.
#[derive(Clone)]
pub(crate) struct Floor {
    pub(crate) name: &'static str,
    pub(crate) pool: PgPool,
    pub(crate) min: u32,
}

/// Bring every pool up to its floor, all at once, within `timeout`.
/// Returns whether each reached it; never fails, since a pool short of its
/// floor still serves (it opens on first use).
pub(crate) async fn fill_all(floors: &[Floor], timeout: Duration) -> bool {
    let started = Instant::now();
    let mut tasks = tokio::task::JoinSet::new();
    for f in floors.iter().filter(|f| f.min > 0) {
        let f = f.clone();
        tasks.spawn(async move {
            let r = fill(&f.pool, f.min).await;
            (f.name, r)
        });
    }
    let mut ok = true;
    let joined = tokio::time::timeout(timeout, async {
        while let Some(done) = tasks.join_next().await {
            if let Ok((name, Err(e))) = done {
                ok = false;
                tracing::warn!(pool = name, error = %e, "could not open the pool's warm floor");
            }
        }
    })
    .await;
    if joined.is_err() {
        ok = false;
        tracing::warn!(
            timeout_ms = timeout.as_millis() as u64,
            "the metadata pools' warm floor took too long; the rest opens in the background"
        );
    }
    // The lease pool has no gauge: its calls never wait behind data.
    for f in floors.iter().filter(|f| f.name != "lease") {
        crate::postgres::pool_gauges(f.name, &f.pool);
    }
    if ok {
        let total: u32 = floors.iter().map(|f| f.min).sum();
        tracing::info!(
            connections = total,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "metadata pools warm"
        );
    }
    ok
}

/// Hold `min` connections of `pool` at once, then return them idle. sqlx
/// hands out idle connections first, so this opens only the missing ones.
async fn fill(pool: &PgPool, min: u32) -> Result<(), sqlx::Error> {
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..min.min(pool.options().get_max_connections()) {
        let pool = pool.clone();
        tasks.spawn(async move { pool.acquire().await });
    }
    let mut held = Vec::new();
    while let Some(done) = tasks.join_next().await {
        match done {
            Ok(Ok(conn)) => held.push(conn),
            Ok(Err(e)) => return Err(e),
            Err(e) => return Err(sqlx::Error::Protocol(e.to_string())),
        }
    }
    Ok(())
}

/// Aborts the keeper when its store is dropped, so the pool handles it
/// holds do not keep connections open for a store nobody uses.
pub(crate) struct Keeper(tokio::task::AbortHandle);

impl Drop for Keeper {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Every [`KEEP_INTERVAL`], top up each idle pool that fell below its
/// floor. A pool with connections in use is left alone: it is warm, and
/// holding its connections to count them would stand in the requests' way.
pub(crate) fn spawn_keeper(floors: Vec<Floor>) -> Keeper {
    let task = tokio::spawn(async move {
        let mut tick = tokio::time::interval(KEEP_INTERVAL);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        tick.tick().await;
        loop {
            tick.tick().await;
            for f in &floors {
                let size = f.pool.size();
                let idle = f.pool.num_idle() as u32;
                if f.pool.is_closed() || size >= f.min || idle < size {
                    continue;
                }
                match tokio::time::timeout(KEEP_TIMEOUT, fill(&f.pool, f.min)).await {
                    Ok(Ok(())) => {
                        tracing::debug!(pool = f.name, from = size, "warm floor restored")
                    }
                    Ok(Err(e)) => {
                        tracing::debug!(pool = f.name, error = %e, "warm floor not restored")
                    }
                    Err(_) => tracing::debug!(pool = f.name, "warm floor restore timed out"),
                }
            }
        }
    });
    Keeper(task.abort_handle())
}
