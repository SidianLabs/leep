//! Metadata reads one request made: how many store calls, in how many
//! round trips, and how long it waited for them. The server reports them
//! in `Server-Timing` (ADR 0082) so a slow read can be told apart into
//! store latency and work. Counting is off unless a caller scopes a
//! [`ReadStats`] around the request with [`measure`].

use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

tokio::task_local! {
    static STATS: Arc<ReadStats>;
}

/// Counters of one scope; shared with the tasks it spawns through
/// [`inherit`].
#[derive(Debug, Default)]
pub struct ReadStats {
    calls: AtomicU64,
    rounds: AtomicU64,
    nanos: AtomicU64,
}

/// What [`measure`] counted.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ReadSummary {
    /// Store calls: one statement each on Postgres for the batched reads
    /// a walk makes.
    pub calls: u64,
    /// Waits for the store; calls issued together count once.
    pub rounds: u64,
    /// Time spent in those waits, summed over rounds.
    pub waited: Duration,
}

/// Run `f`, counting the metadata reads it makes.
pub async fn measure<F: Future>(f: F) -> (F::Output, ReadSummary) {
    let stats = Arc::new(ReadStats::default());
    let out = STATS.scope(stats.clone(), f).await;
    let summary = ReadSummary {
        calls: stats.calls.load(Ordering::Relaxed),
        rounds: stats.rounds.load(Ordering::Relaxed),
        waited: Duration::from_nanos(stats.nanos.load(Ordering::Relaxed)),
    };
    (out, summary)
}

/// `f` counted in the caller's scope, for a future about to be spawned on
/// its own task (task-locals do not cross `tokio::spawn`).
pub(crate) fn inherit<F: Future>(f: F) -> impl Future<Output = F::Output> {
    let stats = STATS.try_with(Arc::clone).ok();
    async move {
        match stats {
            Some(s) => STATS.scope(s, f).await,
            None => f.await,
        }
    }
}

/// Await `f`, one round trip of `calls` store calls.
pub(crate) async fn round<F: Future>(calls: u64, f: F) -> F::Output {
    if calls == 0 || STATS.try_with(|_| ()).is_err() {
        return f.await;
    }
    let started = Instant::now();
    let out = f.await;
    let nanos = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
    let _ = STATS.try_with(|s| {
        s.calls.fetch_add(calls, Ordering::Relaxed);
        s.rounds.fetch_add(1, Ordering::Relaxed);
        s.nanos.fetch_add(nanos, Ordering::Relaxed);
    });
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn counts_only_inside_a_scope_and_across_inherited_spawns() {
        round(1, async {}).await;
        let ((), s) = measure(async {
            round(4, async {}).await;
            round(0, async {}).await;
            tokio::spawn(inherit(round(2, async {}))).await.unwrap();
            tokio::spawn(round(9, async {})).await.unwrap();
        })
        .await;
        assert_eq!((s.calls, s.rounds), (6, 2));
    }
}
