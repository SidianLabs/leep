//! Command line.

use std::path::PathBuf;
use std::time::Duration;

use clap::Parser;

use crate::util::parse_duration;

fn dur(s: &str) -> Result<Duration, String> {
    parse_duration(s)
}

fn seed(s: &str) -> Result<u64, String> {
    match s.strip_prefix("0x") {
        Some(h) => u64::from_str_radix(h, 16).map_err(|e| e.to_string()),
        None => s
            .parse()
            .map_err(|e: std::num::ParseIntError| e.to_string()),
    }
}

/// Soak test for leep-server: an agent swarm lands changes against a real
/// server while operators run GC and index warmups, invariants are checked
/// continuously, and performance and resource trends are recorded.
#[derive(Parser, Debug, Clone)]
#[command(name = "leep-soak", version, about)]
pub struct Args {
    /// How long the swarm runs (`90s`, `2m`, `1h`, `8h`). Setup and the final
    /// checks come on top.
    #[arg(long, default_value = "2m", value_parser = dur)]
    pub duration: Duration,
    /// Simulated agents that use the HTTP API directly.
    #[arg(long, default_value_t = 50)]
    pub agents: usize,
    /// Agents that drive real `leep` processes (init, sync, commit, land,
    /// branch, merge, revert, cherry-pick, rebase).
    #[arg(long, default_value_t = 2)]
    pub cli_agents: usize,
    /// Seed of the workload plan (decimal or 0x hex). The plan is
    /// reproducible; thread and process scheduling is not.
    #[arg(long, default_value = "1", value_parser = seed)]
    pub seed: u64,
    /// Report directory (default: soak-out/<time>).
    #[arg(long)]
    pub out: Option<PathBuf>,
    /// Keep the database, blobs and working copies after the run.
    #[arg(long)]
    pub keep_data: bool,
    /// Directory holding `leep` and `leep-server` (default: next to this
    /// binary).
    #[arg(long)]
    pub bin_dir: Option<PathBuf>,

    /// Postgres admin URL; a throwaway database is created in it and dropped
    /// at the end. Falls back to LEEP_TEST_POSTGRES_URL.
    #[arg(long, env = "LEEP_SOAK_POSTGRES_URL")]
    pub postgres_url: Option<String>,
    /// Use SQLite metadata even when a Postgres URL is available.
    #[arg(long)]
    pub sqlite: bool,

    /// Connect to a running server instead of starting one. Without
    /// --meta/--blobs there is no GC, and without --pid and --metrics-url
    /// no resource trends; the release flow and chaos need an owned server.
    #[arg(long)]
    pub server: Option<String>,
    /// With --server whose `main` already exists (say, an imported git
    /// history), land the synthetic files on top of it so the swarm runs
    /// against that history. Their paths are `src/`, `docs/` and `assets/`.
    #[arg(long, requires = "server")]
    pub seed_on_existing: bool,
    /// Bearer token for --server (write role).
    #[arg(long, env = "LEEP_TOKEN")]
    pub token: Option<String>,
    /// Prometheus URL of --server (for example http://127.0.0.1:9187/metrics).
    #[arg(long)]
    pub metrics_url: Option<String>,
    /// Process id of --server, for RSS and CPU trends.
    #[arg(long)]
    pub pid: Option<u32>,
    /// Metadata store URL of --server, for GC, derive-search and DB size.
    #[arg(long)]
    pub meta: Option<String>,
    /// Blob store URL of --server, for GC; a file:// URL also gives blob size.
    #[arg(long)]
    pub blobs: Option<String>,

    /// Files in the synthetic repository (a tenth are docs, some binary).
    #[arg(long, default_value_t = 3000)]
    pub files: usize,
    /// Mean think time between an agent's iterations. Lands to one ref are
    /// serialized, so N agents with a shorter think time than N times the
    /// cost of one land only queue; see docs/OPERATIONS.md for sizing.
    #[arg(long, default_value = "6s", value_parser = dur)]
    pub think: Duration,
    /// Server blob cache (`--cache-bytes`) in MiB. Small on purpose: a cache
    /// that is still filling looks like a leak, so it should be full early.
    #[arg(long, default_value_t = 32)]
    pub cache_mib: u64,
    /// Server --request-timeout.
    #[arg(long, default_value = "8s", value_parser = dur)]
    pub request_timeout: Duration,
    /// Interval between fenced GCs (0 disables; default scales with the
    /// duration).
    #[arg(long, value_parser = dur)]
    pub gc_interval: Option<Duration>,
    /// Interval between `derive-search main` warmups (0 disables).
    #[arg(long, value_parser = dur)]
    pub derive_interval: Option<Duration>,
    /// Interval between `leep-server backup` runs beside the swarm (0 or
    /// unset: none). With any backups, the run ends with a quiet backup that is
    /// restored into scratch SQLite and file stores and verified (ADR 0026).
    #[arg(long, value_parser = dur)]
    pub backup_interval: Option<Duration>,
    /// Restart the server during the run: graceful SIGTERM, and now and then
    /// SIGKILL. 5xx and dropped connections inside a restart are attributed.
    #[arg(long)]
    pub chaos: bool,
    /// Restarts when --chaos is set.
    #[arg(long, default_value_t = 4)]
    pub chaos_restarts: usize,
    /// Server --rate-limit (requests per second per token); pressure test.
    #[arg(long)]
    pub rate_limit: Option<f64>,
    /// Server --rate-burst.
    #[arg(long)]
    pub rate_burst: Option<f64>,
    /// Server --max-concurrent-per-token.
    #[arg(long)]
    pub max_concurrent: Option<u32>,
    /// No signing, review or release flow (an owned server then also has no
    /// token per agent, so request limits apply per address).
    #[arg(long)]
    pub no_policy: bool,

    /// Seconds between invariant checks.
    #[arg(long, default_value = "20s", value_parser = dur)]
    pub check_interval: Duration,
    /// Seconds between metric and resource samples.
    #[arg(long, default_value = "10s", value_parser = dur)]
    pub sample_interval: Duration,
    /// An agent that makes no progress for this long is a deadlock.
    #[arg(long, default_value = "120s", value_parser = dur)]
    pub stall_timeout: Duration,

    /// Fail when the client-side p99 of land requests exceeds this.
    #[arg(long, default_value = "5s", value_parser = dur)]
    pub max_land_p99: Duration,
    /// Fail when the server's RSS grows faster than this (MiB per hour,
    /// linear fit over the second half of the run, last server incarnation).
    #[arg(long, default_value_t = 64.0)]
    pub max_rss_slope_mib_h: f64,
    /// Shortest second half (seconds) over which the RSS slope is judged;
    /// shorter runs only report it.
    #[arg(long, default_value_t = 300)]
    pub rss_min_window_secs: u64,
    /// An earlier report.json to compare against.
    #[arg(long)]
    pub baseline: Option<PathBuf>,
    /// With --baseline: tolerated factor for land p99 growth and throughput
    /// loss.
    #[arg(long, default_value_t = 1.5)]
    pub max_regression: f64,
    /// Keep running after the first violation instead of stopping at once.
    #[arg(long)]
    pub no_fail_fast: bool,
}

impl Args {
    pub fn gc_every(&self) -> Option<Duration> {
        interval(self.gc_interval, self.duration, 3, 60, 1800)
    }

    /// Off unless asked for: a backup copies the whole store each time.
    pub fn backup_every(&self) -> Option<Duration> {
        self.backup_interval.filter(|d| !d.is_zero())
    }

    pub fn derive_every(&self) -> Option<Duration> {
        interval(self.derive_interval, self.duration, 8, 30, 600)
    }
}

fn interval(
    set: Option<Duration>,
    total: Duration,
    parts: u32,
    lo: u64,
    hi: u64,
) -> Option<Duration> {
    match set {
        Some(d) if d.is_zero() => None,
        Some(d) => Some(d),
        None => Some((total / parts).clamp(Duration::from_secs(lo), Duration::from_secs(hi))),
    }
}
