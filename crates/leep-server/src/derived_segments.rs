//! Derived segments in the server (ADR 0104): a compactor pass every
//! `--derived-segments-interval`, under a store-wide lease so one server at
//! a time runs it, in the index lane (ADR 0073), so it draws on the index
//! connections and never on the land path's.

use std::time::Duration;

use leep_core::lane::{scope, Lane};
use leep_derive::derived_segments::{
    compact_pass, CompactOptions, CompactState, Features, Mode, PassReport,
};
use leep_derive::Repo;

/// The rest after a pass that left work, instead of the interval: long
/// enough for `repair` waiting on the lease to take it.
const MORE_DELAY: Duration = Duration::from_secs(5);

/// When passes run, and what they do.
#[derive(Clone)]
pub struct Schedule {
    /// Time between two passes; the first runs after `first` plus up to
    /// `jitter`.
    pub interval: Duration,
    pub first: Duration,
    pub jitter: Duration,
    pub pass: CompactOptions,
    /// Records the store feature before mode `retire` deletes a row, and
    /// tells whether it is recorded.
    pub feature: Option<Features>,
}

impl Default for Schedule {
    fn default() -> Self {
        Schedule {
            interval: Duration::from_secs(300),
            first: Duration::from_secs(120),
            jitter: Duration::from_secs(60),
            pass: CompactOptions::default(),
            feature: None,
        }
    }
}

/// Register the task's metrics at zero, so dashboards see them at once.
fn describe() {
    for outcome in ["ok", "busy", "error"] {
        metrics::counter!("leep_derived_segment_passes_total", "outcome" => outcome).increment(0);
    }
    for kind in ["skiplist", "history-tree-v4"] {
        metrics::counter!("leep_derived_segment_mismatch_total", "kind" => kind).increment(0);
        metrics::counter!("leep_derived_segment_rows_retired_total", "kind" => kind).increment(0);
    }
    for what in ["manifest", "index", "segment", "missing"] {
        metrics::counter!("leep_derived_segment_errors_total", "what" => what).increment(0);
    }
}

fn first_delay(s: &Schedule) -> Duration {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.subsec_nanos());
    let seed = u64::from(nanos) ^ u64::from(std::process::id()).wrapping_mul(0x9E37_79B9);
    let jitter_ms = s.jitter.as_millis() as u64;
    let extra = if jitter_ms == 0 { 0 } else { seed % jitter_ms };
    s.first + Duration::from_millis(extra)
}

fn record(r: &Option<PassReport>, took: Duration) {
    let Some(r) = r else {
        metrics::counter!("leep_derived_segment_passes_total", "outcome" => "busy").increment(1);
        return;
    };
    metrics::counter!("leep_derived_segment_passes_total", "outcome" => "ok").increment(1);
    metrics::histogram!("leep_derived_segment_pass_seconds").record(took.as_secs_f64());
    metrics::gauge!("leep_derived_segments_live").set(r.live_segments as f64);
    metrics::gauge!("leep_derived_segment_bytes").set(r.live_bytes as f64);
    metrics::gauge!("leep_derived_segment_unsealed_rows").set(r.unsealed_rows as f64);
    if r.sealed_segments > 0 || r.merges > 0 || r.retired_rows > 0 || r.dropped > 0 {
        tracing::info!(
            sealed_segments = r.sealed_segments,
            sealed_changesets = r.sealed_keys,
            merges = r.merges,
            retired_rows = r.retired_rows,
            mismatched = r.mismatched,
            dropped = r.dropped,
            live_segments = r.live_segments,
            live_bytes = r.live_bytes,
            seconds = took.as_secs_f64(),
            "derived segments pass (ADR 0104)"
        );
    }
}

/// Start the task. Aborting the handle stops it. `None` for mode `off`.
pub fn spawn(repo: Repo, schedule: Schedule) -> Option<tokio::task::JoinHandle<()>> {
    if schedule.pass.mode == Mode::Off {
        return None;
    }
    describe();
    Some(tokio::spawn(scope(Lane::Index, async move {
        tokio::time::sleep(first_delay(&schedule)).await;
        let mut state = CompactState::default();
        loop {
            let started = std::time::Instant::now();
            let mut more = false;
            match compact_pass(&repo, &schedule.pass, &mut state, schedule.feature.as_ref()).await {
                Ok(r) => {
                    more = r.as_ref().is_some_and(|r| r.more);
                    record(&r, started.elapsed())
                }
                Err(e) => {
                    metrics::counter!("leep_derived_segment_passes_total", "outcome" => "error")
                        .increment(1);
                    tracing::warn!(error = %e, "derived segments pass failed; trying again later");
                }
            }
            // A pass that stopped with work left (a backfill, a long
            // retirement) is followed soon, after giving the lease up.
            let rest = if more {
                MORE_DELAY.min(schedule.interval)
            } else {
                schedule.interval
            };
            tokio::time::sleep(rest).await;
        }
    })))
}
