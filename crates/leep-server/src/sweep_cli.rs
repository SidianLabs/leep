//! `leep-server gc --unreachable` without `--dry-run` (ADR 0059 phase 2):
//! the changeset sweep, as one run (condemn, wait the fence, collect) or as
//! `--condemn-only` and `--collect-only` cron jobs. Also the checks that
//! guard it and blob GC: the fence against the servers' bounds, and that
//! every server reachable through a lease rescues changesets.

use std::time::Duration;

use anyhow::Context;
use leep_core::{LeaseKey, MetaStore};
use leep_derive::changeset_sweep::{self, SweepCollectReport, SweepCondemnReport, SweepOptions};
use leep_derive::gc::SystemClock;
use leep_derive::indexing::IndexGroup;

/// Clock skew the fence must also cover (ADR 0012).
pub const FENCE_SKEW_ALLOWANCE: Duration = Duration::from_secs(60);

/// What the servers are told about their bounds, for the fence checks.
#[derive(Clone, Copy, Debug)]
pub struct ServerBounds {
    pub request_timeout: Duration,
    /// `None`: no statement bound (off).
    pub statement_timeout: Option<Duration>,
    pub postgres: bool,
}

/// The statement term: Postgres only (SQLite statements end with the
/// request); `Err` when there is no bound, so no fence is safe.
fn statement_term(b: &ServerBounds) -> Result<Duration, String> {
    match (b.postgres, b.statement_timeout) {
        (false, _) => Ok(Duration::ZERO),
        (true, Some(t)) => Ok(t),
        (true, None) => Err(
            "--meta-statement-timeout is 0 (off): a statement of a timed-out request can \
             commit at any time later, so no --fence is safe"
                .into(),
        ),
    }
}

fn request_term(b: &ServerBounds) -> Result<Duration, String> {
    if b.request_timeout.is_zero() {
        return Err(
            "--request-timeout is 0 (off): a request can write at any time after it read \
             the GC state, so no --fence is safe; give gc the servers' --request-timeout"
                .into(),
        );
    }
    Ok(b.request_timeout)
}

/// Why `fence` does not cover blob GC's bound (ADR 0012: request timeout
/// plus statement timeout plus skew), or `None` when it does.
pub fn blob_fence_shortfall(fence: Duration, b: &ServerBounds) -> Option<String> {
    let needed = match (request_term(b), statement_term(b)) {
        (Ok(r), Ok(s)) => r + s + FENCE_SKEW_ALLOWANCE,
        (Err(e), _) | (_, Err(e)) => return Some(e),
    };
    (fence <= needed).then(|| {
        format!(
            "--fence {}s does not exceed --request-timeout {}s + --meta-statement-timeout {}s + \
             {}s of clock skew = {}s: a request's write can commit after the fence (ADR 0012); \
             raise --fence",
            fence.as_secs(),
            b.request_timeout.as_secs(),
            statement_term(b).unwrap_or_default().as_secs(),
            FENCE_SKEW_ALLOWANCE.as_secs(),
            needed.as_secs()
        )
    })
}

/// Why `fence` does not cover the changeset sweep's bound (ADR 0059 §3 and
/// §11): the longest of a request (timeout, statement, flag TTL), a land
/// batch (its lifetime and one swap statement) and an index write window,
/// plus skew.
pub fn changeset_fence_shortfall(
    fence: Duration,
    b: &ServerBounds,
    index_fence: Duration,
) -> Option<String> {
    let (r, s) = match (request_term(b), statement_term(b)) {
        (Ok(r), Ok(s)) => (r, s),
        (Err(e), _) | (_, Err(e)) => return Some(e),
    };
    let request = r + s + leep_derive::changeset_gc::FENCE_FLAG_TTL;
    let batch = leep_derive::MAX_BATCH_LIFETIME + s;
    let needed = request.max(batch).max(index_fence) + FENCE_SKEW_ALLOWANCE;
    (fence <= needed).then(|| {
        format!(
            "--fence {}s does not exceed the longest a writer can act on what it read: \
             max(request {}s, land batch {}s, index writes {}s) + {}s of clock skew = {}s \
             (ADR 0059); raise --fence",
            fence.as_secs(),
            request.as_secs(),
            batch.as_secs(),
            index_fence.as_secs(),
            FENCE_SKEW_ALLOWANCE.as_secs(),
            needed.as_secs()
        )
    })
}

/// Every live lease holder this store names (land coordinators per ref and
/// index holders), asked over HTTP whether it lists `changeset-rescue` in
/// `/v1/ready`. A holder that answers without it refuses the condemn; one
/// that cannot be reached is only warned about (best effort: the runbook's
/// upgrade step is the guarantee).
pub async fn check_rescue_holders(meta: &dyn MetaStore) -> anyhow::Result<()> {
    if !meta.supports_leases() {
        return Ok(());
    }
    let mut keys: Vec<(LeaseKey, leep_core::LeaseTiming)> = Vec::new();
    let land = leep_derive::land_coord::CoordinatorConfig::new("", "").timing();
    for name in meta.list_refs().await?.into_keys() {
        keys.push((LeaseKey::land(name), land));
    }
    let index = leep_derive::indexing::lease::IndexLeaseConfig::new("", "").timing();
    for g in IndexGroup::ALL {
        keys.push((LeaseKey::index(g.lease_name()), index));
    }
    let mut addresses = std::collections::BTreeSet::new();
    for (key, timing) in keys {
        if let Some(l) = meta.lease(&key, timing).await? {
            if l.is_live() && !l.address.is_empty() {
                addresses.insert(l.address);
            }
        }
    }
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()?;
    let mut lacking = Vec::new();
    for a in addresses {
        let url = format!("{}/v1/ready", a.trim_end_matches('/'));
        let body = match client.get(&url).send().await {
            Ok(r) => r.text().await.unwrap_or_default(),
            Err(e) => {
                tracing::warn!(%url, error = %e, "could not ask a lease holder whether it rescues changesets");
                continue;
            }
        };
        let features: Vec<String> = serde_json::from_str::<serde_json::Value>(&body)
            .ok()
            .and_then(|v| v.get("features").cloned())
            .and_then(|f| serde_json::from_value(f).ok())
            .unwrap_or_default();
        if !features
            .iter()
            .any(|f| f == leep_core::api::FEATURE_CHANGESET_RESCUE)
        {
            lacking.push(a);
        }
    }
    if !lacking.is_empty() {
        anyhow::bail!(
            "servers that do not rescue changesets hold leases: {}; upgrade every server \
             before the first condemnation (ADR 0059, section 9)",
            lacking.join(", ")
        );
    }
    Ok(())
}

pub enum SweepPhase {
    Both,
    Condemn,
    Collect,
}

pub async fn run_sweep(
    repo: &leep_derive::Repo,
    phase: SweepPhase,
    opts: &SweepOptions,
    metrics_file: Option<&std::path::Path>,
) -> anyhow::Result<()> {
    let clock = SystemClock;
    let mut condemned = None;
    let mut collected = None;
    if matches!(phase, SweepPhase::Both | SweepPhase::Condemn) {
        check_rescue_holders(&**repo.meta()).await?;
        condemned = Some(
            changeset_sweep::condemn(repo, opts, &clock)
                .await
                .context("condemning unreachable changesets")?,
        );
    }
    if matches!(phase, SweepPhase::Both) {
        tracing::info!(fence = ?opts.fence, "condemned; waiting out the fence");
        leep_derive::gc::Clock::sleep(&clock, opts.fence).await;
    }
    if matches!(phase, SweepPhase::Both | SweepPhase::Collect) {
        collected = Some(
            changeset_sweep::collect(repo, opts, &clock)
                .await
                .context("collecting condemned changesets")?,
        );
    }
    if let Some(r) = &condemned {
        print_condemn(r);
    }
    if let Some(r) = &collected {
        print_collect(r);
    }
    if let Some(path) = metrics_file {
        write_metrics(path, condemned.as_ref(), collected.as_ref())
            .with_context(|| format!("writing {}", path.display()))?;
    }
    Ok(())
}

fn print_condemn(r: &SweepCondemnReport) {
    println!("changesets stored: {}", r.stored);
    println!("unreachable and older than grace: {}", r.unreachable);
    println!(
        "kept only by a window, attestation, mirror or grace: {}",
        r.transient
    );
    println!("condemned: {}", r.condemned);
    println!("already condemned: {}", r.already_condemned);
    println!("reachability rows written: {}", r.reach_rows_written);
}

fn print_collect(r: &SweepCollectReport) {
    println!("collect: unreachable: {}", r.unreachable);
    println!("collect: newer than fence, left alone: {}", r.fence_pending);
    println!("collect: rescued by reference: {}", r.rescued_by_reference);
    println!(
        "collect: kept as ancestors of kept: {}",
        r.kept_by_descendant
    );
    println!(
        "collect: abandoned (rescued during the delete): {}",
        r.abandoned
    );
    println!(
        "deleted: {} changesets, {} bytes",
        r.deleted, r.deleted_bytes
    );
    println!(
        "collect: left for the next run (--max-delete): {}",
        r.left_by_cap
    );
    println!(
        "collect: deletes finished for a killed run: {}",
        r.finished_from_earlier
    );
    println!(
        "collect: tombstone pass: {} rows removed, {} tombstones dropped",
        r.tombstone_rows_deleted, r.tombstones_dropped
    );
    println!("collect: longest batch: {:.3}s", r.max_batch.as_secs_f64());
    println!("collect: still condemned: {}", r.condemned_pending);
    println!("collect: fence flag cleared: {}", r.flag_cleared);
    println!(
        "collect: blame rows of deleted changesets (estimate, leaked until \
         `repair drop --kind blame`): {}",
        r.blame_rows_of_deleted_estimate
    );
}

/// The run's numbers as a Prometheus textfile (node_exporter's textfile
/// collector), written atomically. Values are of the last run: the sweep
/// is a cron job, not a server, so it has no counters of its own.
fn write_metrics(
    path: &std::path::Path,
    condemned: Option<&SweepCondemnReport>,
    collected: Option<&SweepCollectReport>,
) -> std::io::Result<()> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    let mut out = String::new();
    let mut put = |name: &str, help: &str, v: f64| {
        out.push_str(&format!(
            "# HELP {name} {help}\n# TYPE {name} gauge\n{name} {v}\n"
        ));
    };
    put(
        "leep_gc_unreachable_last_run_timestamp_seconds",
        "When the changeset sweep last finished a phase.",
        now as f64,
    );
    if let Some(r) = condemned {
        put(
            "leep_gc_changesets_condemned",
            "Changesets the last condemn condemned.",
            r.condemned as f64,
        );
    }
    if let Some(r) = collected {
        for (name, help, v) in [
            (
                "leep_gc_changesets_deleted",
                "Changesets the last collect deleted.",
                r.deleted as f64,
            ),
            (
                "leep_gc_changeset_bytes_deleted",
                "Encoded bytes of the changesets the last collect deleted.",
                r.deleted_bytes as f64,
            ),
            (
                "leep_gc_changesets_abandoned",
                "Deletes the last collect abandoned for a rescue.",
                r.abandoned as f64,
            ),
            (
                "leep_gc_changesets_kept_by_descendant",
                "Condemned changesets the last collect kept as ancestors of kept ones.",
                r.kept_by_descendant as f64,
            ),
            (
                "leep_gc_changesets_condemned_pending",
                "Condemnations unresolved after the last collect.",
                r.condemned_pending as f64,
            ),
            (
                "leep_gc_changesets_left_by_cap",
                "Changesets the last collect left for the next run (--max-delete).",
                r.left_by_cap as f64,
            ),
            (
                "leep_gc_tombstone_rows_deleted",
                "Rows the last tombstone pass removed.",
                r.tombstone_rows_deleted as f64,
            ),
            (
                "leep_gc_changeset_collect_batch_seconds_max",
                "Longest batch of the last collect, doom deleting to deleted.",
                r.max_batch.as_secs_f64(),
            ),
            (
                "leep_gc_blame_rows_of_deleted_estimate",
                "Blame rows keyed by changesets the last collect deleted (estimate).",
                r.blame_rows_of_deleted_estimate as f64,
            ),
        ] {
            put(name, help, v);
        }
    }
    let dir = path.parent().unwrap_or(std::path::Path::new("."));
    let tmp = dir.join(format!(
        ".{}.tmp",
        path.file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("leep_gc_unreachable")
    ));
    std::fs::write(&tmp, out)?;
    std::fs::rename(&tmp, path)
}

/// Refuse a backup fence longer than the sweep keeps tombstones: the root
/// trees of changesets deleted before the retention are no longer known,
/// so no gc could keep them that long.
pub fn check_backup_fence(fence: Duration, tombstone_retention: Duration) -> anyhow::Result<()> {
    if fence > tombstone_retention {
        anyhow::bail!(
            "--backup-fence {}s exceeds --tombstone-retention {}s: tombstones of deleted \
             changesets are dropped after the retention, so their trees cannot be kept that \
             long; raise --tombstone-retention (on the sweep's collect as well) or lower the \
             backup fence",
            fence.as_secs(),
            tombstone_retention.as_secs()
        );
    }
    Ok(())
}

/// `--backup-fence` from sealed backups (ADR 0059 section 6): three times
/// the longest backup of the last 30 days (sealed minus snapshot time), at
/// least one hour. `Err` when `dir` holds no sealed backup that recent.
pub fn backup_fence_from(dir: &std::path::Path, now: i64) -> anyhow::Result<Duration> {
    let snaps = dir.join("snapshots");
    let mut longest: Option<i64> = None;
    for e in std::fs::read_dir(&snaps).with_context(|| format!("reading {}", snaps.display()))? {
        let path = e?.path().join(leep_server::backup::MANIFEST_FILE);
        let Ok(bytes) = std::fs::read(&path) else {
            continue;
        };
        let m: leep_server::backup::Manifest = match serde_json::from_slice(&bytes) {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!(path = %path.display(), error = %e, "skipping an unreadable manifest");
                continue;
            }
        };
        if m.created_at < now - 30 * 86_400 {
            continue;
        }
        let run = (m.created_at - m.snapshot_taken_at).max(0);
        longest = Some(longest.map_or(run, |l| l.max(run)));
    }
    let Some(longest) = longest else {
        anyhow::bail!(
            "no sealed backup of the last 30 days in {}; pass --backup-fence instead",
            dir.display()
        );
    };
    Ok(Duration::from_secs((3 * longest).max(3600) as u64))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pg(req: u64, st: Option<u64>) -> ServerBounds {
        ServerBounds {
            request_timeout: Duration::from_secs(req),
            statement_timeout: st.map(Duration::from_secs),
            postgres: true,
        }
    }

    #[test]
    fn the_changeset_fence_covers_requests_batches_and_index_writes() {
        let m = 60;
        let idx = Duration::from_secs(60);
        // Defaults: 15m > 5m + 5m5s + 10s + 1m.
        assert_eq!(
            changeset_fence_shortfall(
                Duration::from_secs(15 * m),
                &pg(5 * m, Some(5 * m + 5)),
                idx
            ),
            None
        );
        let e = changeset_fence_shortfall(
            Duration::from_secs(10 * m),
            &pg(5 * m, Some(5 * m + 5)),
            idx,
        )
        .unwrap();
        assert!(e.contains("raise --fence") && e.contains("675s"), "{e}");
        // With short requests the land batch bounds it: 60s + 1s + 60s.
        let short = ServerBounds {
            request_timeout: Duration::from_secs(2),
            statement_timeout: None,
            postgres: false,
        };
        assert!(changeset_fence_shortfall(Duration::from_secs(100), &short, idx).is_some());
        assert_eq!(
            changeset_fence_shortfall(Duration::from_secs(121), &short, idx),
            None
        );
        assert!(
            changeset_fence_shortfall(Duration::from_secs(15 * m), &pg(5 * m, None), idx).is_some()
        );
        assert!(blob_fence_shortfall(Duration::from_secs(15 * m), &pg(0, Some(5))).is_some());
        assert_eq!(
            blob_fence_shortfall(Duration::from_secs(15 * m), &pg(5 * m, Some(5 * m + 5))),
            None
        );
    }

    #[test]
    fn the_backup_fence_is_three_longest_runs_and_at_least_an_hour() {
        let dir = tempfile::tempdir().unwrap();
        let now = 2_000_000_000;
        assert!(backup_fence_from(dir.path(), now).is_err());
        let write = |name: &str, taken: i64, created: i64| {
            let d = dir.path().join("snapshots").join(name);
            std::fs::create_dir_all(&d).unwrap();
            let m = serde_json::json!({
                "format": 1, "leep_version": "0", "name": name, "created_at": created,
                "snapshot_taken_at": taken, "source_backend": "sqlite", "schema_version": 7,
                "meta_file": "meta.db", "meta_blake3": "", "meta_bytes": 0,
                "counts": {"changesets": 0, "refs": 0, "derived": 0, "ref_log": 0},
                "ref_log_last_seq": 0, "refs": {},
                "blobs": {"included": true},
                "check": {"mode": "closure", "ok": true}, "elapsed_secs": 0.0
            });
            std::fs::write(d.join("manifest.json"), m.to_string()).unwrap();
        };
        write("a", now - 100, now - 50);
        assert_eq!(
            backup_fence_from(dir.path(), now).unwrap(),
            Duration::from_secs(3600)
        );
        write("b", now - 5000, now - 1000);
        assert_eq!(
            backup_fence_from(dir.path(), now).unwrap(),
            Duration::from_secs(12_000)
        );
        // Older than 30 days: not counted.
        write("c", now - 40 * 86_400 - 90_000, now - 40 * 86_400);
        assert_eq!(
            backup_fence_from(dir.path(), now).unwrap(),
            Duration::from_secs(12_000)
        );
    }
}
