//! Retention of blob trigram rows in the server (ADR 0102): a pass of
//! [`leep_derive::search::retention`] every `--search-retention-interval`,
//! under a store-wide lease so one server at a time runs it, in the index
//! lane (ADR 0073), so it draws on the index connections and never on the
//! land path's.

use std::time::Duration;

use leep_core::lane::{scope, Lane};
use leep_derive::search::retention::{retention_pass, RetentionOptions, RetentionReport};
use leep_derive::Repo;

/// When passes run.
#[derive(Debug, Clone)]
pub struct RetentionSchedule {
    /// Time between two passes; the first runs after `first` plus up to
    /// `jitter` (or after `interval`, if shorter), once a new server has
    /// warmed its indexes.
    pub interval: Duration,
    pub first: Duration,
    pub jitter: Duration,
    pub pass: RetentionOptions,
}

impl Default for RetentionSchedule {
    fn default() -> Self {
        RetentionSchedule {
            interval: Duration::from_secs(3600),
            first: Duration::from_secs(600),
            jitter: Duration::from_secs(300),
            pass: RetentionOptions::default(),
        }
    }
}

/// Register the task's metrics at zero, so dashboards see them at once.
fn describe() {
    for outcome in ["ok", "busy", "error"] {
        metrics::counter!("leep_search_retention_passes_total", "outcome" => outcome).increment(0);
    }
    for kind in [
        leep_derive::search::BLOB_KIND,
        leep_derive::search::RETIRED_BLOB_KIND,
    ] {
        metrics::counter!("leep_search_retention_rows_total", "kind" => kind, "outcome" => "deleted")
            .increment(0);
    }
    metrics::counter!("leep_search_retention_rows_total", "kind" => leep_derive::search::BLOB_KIND, "outcome" => "restored")
        .increment(0);
}

/// `first` plus a jitter below `jitter`, or `interval` if shorter.
fn first_delay(s: &RetentionSchedule) -> Duration {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.subsec_nanos());
    let seed = u64::from(nanos) ^ u64::from(std::process::id()).wrapping_mul(0x9E37_79B9);
    let jitter_ms = s.jitter.as_millis() as u64;
    let extra = if jitter_ms == 0 { 0 } else { seed % jitter_ms };
    (s.first + Duration::from_millis(extra)).min(s.interval)
}

fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs() as i64)
}

fn record(r: &Option<RetentionReport>, took: Duration) {
    let Some(r) = r else {
        metrics::counter!("leep_search_retention_passes_total", "outcome" => "busy").increment(1);
        return;
    };
    metrics::counter!("leep_search_retention_passes_total", "outcome" => "ok").increment(1);
    metrics::histogram!("leep_search_retention_seconds").record(took.as_secs_f64());
    if r.deleted > 0 || r.restored > 0 {
        tracing::info!(
            heads = r.heads,
            live_blobs = r.live_blobs,
            examined = r.examined,
            deleted = r.deleted,
            restored = r.restored,
            seconds = took.as_secs_f64(),
            "retired search rows of blobs no head holds (ADR 0102)"
        );
    }
}

/// Start the task. Aborting the handle stops it.
pub fn spawn(repo: Repo, schedule: RetentionSchedule) -> tokio::task::JoinHandle<()> {
    describe();
    tokio::spawn(scope(Lane::Index, async move {
        tokio::time::sleep(first_delay(&schedule)).await;
        loop {
            let started = std::time::Instant::now();
            match retention_pass(&repo, &schedule.pass, unix_now()).await {
                Ok(r) => record(&r, started.elapsed()),
                Err(e) => {
                    metrics::counter!("leep_search_retention_passes_total", "outcome" => "error")
                        .increment(1);
                    tracing::warn!(error = %e, "search row retention failed; trying again later");
                }
            }
            tokio::time::sleep(schedule.interval).await;
        }
    }))
}
