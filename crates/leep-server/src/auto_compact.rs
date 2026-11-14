//! Automatic changeset compaction in the server (ADR 0101): what
//! `leep-server compact-changesets` does (ADR 0053), run in the background
//! on a schedule ([`leep_meta::auto_compact`] decides what each step does).
//!
//! The task runs on a thread of its own at the lowest CPU priority, with a
//! single-threaded runtime and one database connection of its own, so
//! training and compressing take only what lands and reads leave, and never
//! a connection from the server's pools. Lands never wait for it.

use std::time::Duration;

use leep_meta::auto_compact::{AutoCompactOptions, AutoCompactReport, AutoCompactor};

/// When the task runs.
#[derive(Debug, Clone)]
pub struct AutoCompactSchedule {
    /// Time between two steps; the first runs after `first` (or this, if
    /// shorter): a server that just started is busy warming its indexes.
    pub interval: Duration,
    pub first: Duration,
    pub step: AutoCompactOptions,
}

impl Default for AutoCompactSchedule {
    fn default() -> Self {
        AutoCompactSchedule {
            interval: Duration::from_secs(600),
            first: Duration::from_secs(120),
            step: AutoCompactOptions::default(),
        }
    }
}

/// Register the task's metrics at zero, so dashboards see them at once.
fn describe() {
    for outcome in ["ok", "error"] {
        metrics::counter!("leep_changeset_compact_steps_total", "outcome" => outcome).increment(0);
    }
    metrics::counter!("leep_changeset_compact_trained_total").increment(0);
    metrics::counter!("leep_changeset_compact_rejected_total").increment(0);
    metrics::counter!("leep_changeset_compact_rows_total").increment(0);
    metrics::counter!("leep_changeset_compact_saved_bytes_total").increment(0);
}

fn record(r: &AutoCompactReport, took: Duration) {
    metrics::counter!("leep_changeset_compact_steps_total", "outcome" => "ok").increment(1);
    metrics::gauge!("leep_changeset_dictionary").set(f64::from(r.dictionary));
    if r.trained.is_some() {
        metrics::counter!("leep_changeset_compact_trained_total").increment(1);
    }
    if r.rejected {
        metrics::counter!("leep_changeset_compact_rejected_total").increment(1);
    }
    metrics::counter!("leep_changeset_compact_rows_total").increment(r.rewritten);
    metrics::counter!("leep_changeset_compact_saved_bytes_total")
        .increment(r.bytes_before.saturating_sub(r.bytes_after));
    if r.passed {
        metrics::histogram!("leep_changeset_compact_seconds").record(took.as_secs_f64());
    }
}

/// Start the task for the metadata store at `url`. Returns `None`, and
/// starts nothing, for a store it does not apply to (`mem://`, in-memory
/// SQLite).
pub fn spawn(url: &str, schedule: AutoCompactSchedule) -> Option<std::thread::JoinHandle<()>> {
    if !(url.starts_with("sqlite:") || url.starts_with("postgres")) {
        return None;
    }
    if url.contains(":memory:") || url.contains("mode=memory") {
        return None;
    }
    describe();
    let url = url.to_string();
    let spawned = std::thread::Builder::new()
        .name("leep-compact".into())
        .spawn(move || {
            leep_store::lower_thread_priority();
            let rt = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(rt) => rt,
                Err(e) => {
                    tracing::warn!(error = %e, "automatic changeset compaction did not start");
                    return;
                }
            };
            rt.block_on(run(url, schedule));
        });
    match spawned {
        Ok(h) => Some(h),
        Err(e) => {
            tracing::warn!(error = %e, "automatic changeset compaction did not start");
            None
        }
    }
}

async fn run(url: String, schedule: AutoCompactSchedule) {
    tokio::time::sleep(schedule.first.min(schedule.interval)).await;
    let mut compactor = None;
    loop {
        if compactor.is_none() {
            match AutoCompactor::connect(&url, schedule.step.clone()).await {
                Ok(Some(c)) => compactor = Some(c),
                Ok(None) => {
                    tracing::info!("automatic changeset compaction does not apply to this store");
                    return;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "automatic changeset compaction cannot open the store; trying again later");
                }
            }
        }
        if let Some(c) = compactor.as_mut() {
            let started = std::time::Instant::now();
            match c.step().await {
                Ok(r) => {
                    record(&r, started.elapsed());
                    if let Some((id, bytes, samples)) = r.trained {
                        tracing::info!(
                            dictionary = id,
                            bytes,
                            samples,
                            "trained a changeset dictionary (ADR 0101)"
                        );
                    }
                    if r.rewritten > 0 {
                        tracing::info!(
                            dictionary = r.dictionary,
                            rows = r.rewritten,
                            bytes_before = r.bytes_before,
                            bytes_after = r.bytes_after,
                            seconds = started.elapsed().as_secs_f64(),
                            "compressed changeset rows (ADR 0101)"
                        );
                    }
                }
                Err(e) => {
                    metrics::counter!("leep_changeset_compact_steps_total", "outcome" => "error")
                        .increment(1);
                    tracing::warn!(error = %e, "automatic changeset compaction failed; trying again later");
                }
            }
        }
        tokio::time::sleep(schedule.interval).await;
    }
}
