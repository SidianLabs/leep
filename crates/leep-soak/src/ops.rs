//! What an operator does beside the swarm: run the server (and restart it),
//! garbage-collect, warm the search index.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use serde::Serialize;
use sqlx::{AssertSqlSafe, Connection, PgConnection};
use tokio::process::{Child, Command};
use tokio::sync::Mutex;

use crate::invariants::Checker;
use crate::shared::Shared;
use crate::util::Rng;

/// Where the server keeps its state, and how to reach the same stores.
#[derive(Clone, Debug)]
pub struct Stores {
    pub meta: String,
    pub blobs: String,
    /// Directory behind a `file://` blob store, for its size.
    pub blob_dir: Option<PathBuf>,
}

/// A throwaway Postgres database inside an admin URL.
pub struct PgDb {
    admin_url: String,
    pub name: String,
    pub url: String,
}

fn replace_db(url: &str, db: &str) -> Result<String> {
    let (base, query) = match url.split_once('?') {
        Some((b, q)) => (b, format!("?{q}")),
        None => (url, String::new()),
    };
    let root = base
        .rsplit_once('/')
        .ok_or_else(|| anyhow!("postgres url has no database"))?
        .0;
    Ok(format!("{root}/{db}{query}"))
}

impl PgDb {
    pub async fn create(admin_url: &str, tag: &str) -> Result<PgDb> {
        let name = format!("leep_soak_{tag}");
        let mut c = PgConnection::connect(admin_url)
            .await
            .context("connecting to postgres")?;
        sqlx::query(AssertSqlSafe(format!(
            "DROP DATABASE IF EXISTS {name} WITH (FORCE)"
        )))
        .execute(&mut c)
        .await?;
        sqlx::query(AssertSqlSafe(format!("CREATE DATABASE {name}")))
            .execute(&mut c)
            .await?;
        Ok(PgDb {
            admin_url: admin_url.to_string(),
            url: replace_db(admin_url, &name)?,
            name,
        })
    }

    pub async fn drop_db(&self) -> Result<()> {
        let mut c = PgConnection::connect(&self.admin_url).await?;
        sqlx::query(AssertSqlSafe(format!(
            "DROP DATABASE IF EXISTS {} WITH (FORCE)",
            self.name
        )))
        .execute(&mut c)
        .await?;
        Ok(())
    }
}

/// Size and health numbers read from Postgres.
#[derive(Clone, Debug, Default, Serialize)]
pub struct PgStats {
    pub db_bytes: i64,
    pub deadlocks: i64,
    pub connections: i64,
    pub tables: BTreeMap<String, i64>,
}

pub async fn pg_stats(url: &str) -> Result<PgStats> {
    let mut c = PgConnection::connect(url).await?;
    let row: (i64, i64) = sqlx::query_as(
        "SELECT pg_database_size(current_database()), \
         COALESCE((SELECT deadlocks FROM pg_stat_database WHERE datname = current_database()), 0)::bigint",
    )
    .fetch_one(&mut c)
    .await?;
    let conns: (i64,) =
        sqlx::query_as("SELECT count(*) FROM pg_stat_activity WHERE datname = current_database()")
            .fetch_one(&mut c)
            .await?;
    let tables: Vec<(String, i64)> = sqlx::query_as(
        "SELECT relname::text, pg_total_relation_size(oid)::bigint FROM pg_class \
         WHERE relkind = 'r' AND relnamespace = 'public'::regnamespace",
    )
    .fetch_all(&mut c)
    .await?;
    Ok(PgStats {
        db_bytes: row.0,
        deadlocks: row.1,
        connections: conns.0,
        tables: tables.into_iter().collect(),
    })
}

/// Bytes under a directory (0 when it is not there).
pub fn dir_size(dir: &Path) -> u64 {
    let mut total = 0;
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        let Ok(rd) = std::fs::read_dir(&d) else {
            continue;
        };
        for e in rd.flatten() {
            match e.file_type() {
                Ok(t) if t.is_dir() => stack.push(e.path()),
                Ok(_) => total += e.metadata().map(|m| m.len()).unwrap_or(0),
                Err(_) => {}
            }
        }
    }
    total
}

pub struct ServerConfig {
    pub bin_dir: PathBuf,
    pub stores: Stores,
    pub port: u16,
    pub metrics_port: u16,
    pub log: PathBuf,
    pub extra: Vec<String>,
    pub request_timeout: Duration,
}

/// The `leep-server` this run owns.
pub struct Server {
    cfg: ServerConfig,
    child: Mutex<Option<Child>>,
    pub pid: AtomicU32,
    pub incarnation: AtomicU32,
}

impl Server {
    pub fn new(cfg: ServerConfig) -> Server {
        Server {
            cfg,
            child: Mutex::new(None),
            pid: AtomicU32::new(0),
            incarnation: AtomicU32::new(0),
        }
    }

    pub fn url(&self) -> String {
        format!("http://127.0.0.1:{}", self.cfg.port)
    }

    pub fn metrics_url(&self) -> String {
        format!("http://127.0.0.1:{}/metrics", self.cfg.metrics_port)
    }

    pub async fn start(&self) -> Result<()> {
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.cfg.log)?;
        let err = log.try_clone()?;
        let child = Command::new(self.cfg.bin_dir.join("leep-server"))
            .args(["--listen", &format!("127.0.0.1:{}", self.cfg.port)])
            .args(["--meta", &self.cfg.stores.meta])
            .args(["--blobs", &self.cfg.stores.blobs])
            .args([
                "--metrics-listen",
                &format!("127.0.0.1:{}", self.cfg.metrics_port),
            ])
            .args([
                "--request-timeout",
                &format!("{}s", self.cfg.request_timeout.as_secs()),
            ])
            .args(&self.cfg.extra)
            .env("RUST_LOG", "warn")
            .stdin(Stdio::null())
            .stdout(log)
            .stderr(err)
            .kill_on_drop(true)
            .spawn()
            .context("spawning leep-server")?;
        self.pid.store(child.id().unwrap_or(0), Ordering::Relaxed);
        self.incarnation.fetch_add(1, Ordering::Relaxed);
        *self.child.lock().await = Some(child);
        self.wait_healthy().await
    }

    async fn wait_healthy(&self) -> Result<()> {
        let client = reqwest::Client::builder().no_proxy().build()?;
        let url = format!("{}/v1/health", self.url());
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            if let Some(c) = self.child.lock().await.as_mut() {
                if let Some(st) = c.try_wait()? {
                    bail!(
                        "leep-server exited early ({st}); see {}",
                        self.cfg.log.display()
                    );
                }
            }
            if let Ok(r) = client
                .get(&url)
                .timeout(Duration::from_secs(2))
                .send()
                .await
            {
                if r.status().is_success() {
                    return Ok(());
                }
            }
            if Instant::now() > deadline {
                bail!(
                    "leep-server not healthy within 60s; see {}",
                    self.cfg.log.display()
                );
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Stop the server. `graceful` sends SIGTERM and waits (SIGKILL after
    /// 30s); otherwise SIGKILL at once. Returns what actually happened.
    pub async fn stop(&self, graceful: bool) -> &'static str {
        let mut guard = self.child.lock().await;
        let Some(mut child) = guard.take() else {
            return "not running";
        };
        if graceful {
            if let Some(pid) = child.id() {
                let _ = Command::new("kill")
                    .args(["-TERM", &pid.to_string()])
                    .status()
                    .await;
            }
            match tokio::time::timeout(Duration::from_secs(30), child.wait()).await {
                Ok(_) => return "SIGTERM",
                Err(_) => {
                    let _ = child.start_kill();
                    let _ = child.wait().await;
                    return "SIGTERM timed out, SIGKILL";
                }
            }
        }
        let _ = child.start_kill();
        let _ = child.wait().await;
        "SIGKILL"
    }
}

/// Seeded restarts spread over the run: two graceful for each SIGKILL.
pub async fn chaos_loop(server: Arc<Server>, sh: Arc<Shared>, restarts: usize) {
    let mut rng = Rng::new(sh.args.seed ^ 0xC4A05);
    let total = sh.args.duration.as_secs_f64();
    for i in 0..restarts {
        let frac = (i as f64 + 1.0) / (restarts as f64 + 1.0);
        let at = total * (frac + (rng.unit() - 0.5) * 0.4 / (restarts as f64 + 1.0));
        while sh.now_secs() < at {
            if sh.stopped() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        let graceful = i % 3 != 2;
        let idx = sh.chaos_begin(if graceful { "SIGTERM" } else { "SIGKILL" });
        let how = server.stop(graceful).await;
        eprintln!("chaos: restart {} of {restarts}: {how}", i + 1);
        tokio::time::sleep(Duration::from_millis(300 + rng.below(1200) as u64)).await;
        match server.start().await {
            Ok(()) => sh.chaos_end(idx),
            Err(e) => {
                sh.violate("server_start", format!("restart failed: {e:#}"));
                return;
            }
        }
        sh.stats.inc(if graceful {
            "restarts_sigterm"
        } else {
            "restarts_sigkill"
        });
    }
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct GcRun {
    pub at_secs: f64,
    pub took_secs: f64,
    pub numbers: BTreeMap<String, u64>,
    /// `None` when the run ended before the check could start.
    pub verified: Option<bool>,
}

/// Parse lines like `condemned: 12 blobs, 3400 bytes` and `collect: rescued
/// by reference: 3` into numbers.
pub fn parse_gc_output(text: &str) -> BTreeMap<String, u64> {
    let mut m = BTreeMap::new();
    for line in text.lines() {
        let Some((k, v)) = line.rsplit_once(": ") else {
            continue;
        };
        let nums: Vec<u64> = v
            .split(|c: char| !c.is_ascii_digit())
            .filter_map(|x| x.parse().ok())
            .collect();
        match nums.as_slice() {
            [n] => {
                m.insert(k.trim().to_string(), *n);
            }
            [n, b] => {
                m.insert(k.trim().to_string(), *n);
                m.insert(format!("{} (bytes)", k.trim()), *b);
            }
            _ => {}
        }
    }
    m
}

pub fn gc_durations(timeout: Duration) -> (Duration, Duration) {
    let grace = (timeout * 2).max(Duration::from_secs(20));
    // Above the server's request timeout, the rule of ADR 0012, with margin
    // for a loaded machine.
    let fence = timeout + timeout / 2 + Duration::from_secs(2);
    (grace, fence)
}

pub async fn gc_loop(
    bin_dir: PathBuf,
    stores: Stores,
    sh: Arc<Shared>,
    checker: Arc<Mutex<Checker>>,
    runs: Arc<std::sync::Mutex<Vec<GcRun>>>,
    every: Duration,
    log: PathBuf,
) {
    let (grace, fence) = gc_durations(sh.args.request_timeout);
    let mut next = Instant::now() + every;
    while !sh.stopped() {
        tokio::time::sleep(Duration::from_millis(250)).await;
        if Instant::now() < next {
            continue;
        }
        let t0 = Instant::now();
        let at = sh.now_secs();
        let out = Command::new(bin_dir.join("leep-server"))
            .args(["gc", "--meta", &stores.meta, "--blobs", &stores.blobs])
            .args(["--grace", &format!("{}s", grace.as_secs())])
            .args(["--fence", &format!("{}s", fence.as_secs())])
            // The soak's fence is scaled down with its servers' timeouts.
            .arg("--unsafe-fence")
            .env("RUST_LOG", "warn")
            .stdin(Stdio::null())
            .kill_on_drop(true)
            .output()
            .await;
        let out = match out {
            Ok(o) => o,
            Err(e) => {
                sh.violate("gc", format!("could not run gc: {e}"));
                return;
            }
        };
        let text = String::from_utf8_lossy(&out.stdout).into_owned();
        let _ = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log)
            .and_then(|mut f| {
                use std::io::Write;
                writeln!(
                    f,
                    "--- gc at {at:.0}s (exit {})\n{text}{}",
                    out.status,
                    String::from_utf8_lossy(&out.stderr)
                )
            });
        if !out.status.success() {
            // A GC that races a restart may lose its database connection; the
            // next run repeats it (ADR 0012: a crashed run is safe).
            if sh.in_chaos(sh.now_secs()) {
                sh.stats.inc("gc_failed_during_restart");
                next = Instant::now() + every;
                continue;
            }
            sh.violate(
                "gc",
                format!(
                    "gc failed ({}): {}",
                    out.status,
                    String::from_utf8_lossy(&out.stderr)
                        .chars()
                        .take(400)
                        .collect::<String>()
                ),
            );
            return;
        }
        let numbers = parse_gc_output(&text);
        sh.stats.inc("gc_runs");
        sh.stats.add(
            "gc_blobs_deleted",
            numbers.get("deleted").copied().unwrap_or(0),
        );
        sh.stats.add(
            "gc_blobs_condemned",
            numbers.get("condemned").copied().unwrap_or(0),
        );
        // Nothing reachable may be gone, now that the collect has finished.
        let verified = {
            let mut c = checker.lock().await;
            match c.verify_reachable().await {
                Ok(()) => Some(true),
                Err(crate::api::Fail::Stop) => None,
                Err(_) => Some(false),
            }
        };
        runs.lock().unwrap().push(GcRun {
            at_secs: at,
            took_secs: t0.elapsed().as_secs_f64(),
            numbers,
            verified,
        });
        next = Instant::now() + every;
    }
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct DeriveRun {
    pub at_secs: f64,
    pub elapsed_secs: f64,
    pub blob_rows: u64,
    pub tree_rows: u64,
}

pub async fn derive_loop(
    bin_dir: PathBuf,
    stores: Stores,
    sh: Arc<Shared>,
    runs: Arc<std::sync::Mutex<Vec<DeriveRun>>>,
    every: Duration,
) {
    let mut next = Instant::now() + every / 2;
    while !sh.stopped() {
        tokio::time::sleep(Duration::from_millis(250)).await;
        if Instant::now() < next {
            continue;
        }
        let t0 = Instant::now();
        let at = sh.now_secs();
        let out = Command::new(bin_dir.join("leep-server"))
            .args(["derive-search", "main"])
            .args(["--meta", &stores.meta, "--blobs", &stores.blobs])
            .env("RUST_LOG", "warn")
            .stdin(Stdio::null())
            .kill_on_drop(true)
            .output()
            .await;
        next = Instant::now() + every;
        let Ok(out) = out else {
            continue;
        };
        if !out.status.success() {
            if sh.in_chaos(sh.now_secs()) {
                continue;
            }
            sh.violate(
                "derive_search",
                format!(
                    "derive-search failed ({}): {}",
                    out.status,
                    String::from_utf8_lossy(&out.stderr)
                        .chars()
                        .take(400)
                        .collect::<String>()
                ),
            );
            return;
        }
        let n = parse_gc_output(&String::from_utf8_lossy(&out.stdout));
        runs.lock().unwrap().push(DeriveRun {
            at_secs: at,
            elapsed_secs: t0.elapsed().as_secs_f64(),
            blob_rows: n.get("blob rows derived").copied().unwrap_or(0),
            tree_rows: n.get("tree rows derived").copied().unwrap_or(0),
        });
        sh.stats.inc("derive_runs");
    }
}

/// One `leep-server backup` (ADR 0026) into `dir`; returns the manifest, or
/// says why not. A backup that exits 1 sealed a snapshot whose check found
/// problems, which is the violation this exists to catch.
async fn run_backup(
    bin_dir: &Path,
    stores: &Stores,
    dir: &Path,
) -> Result<(std::process::Output, Option<serde_json::Value>)> {
    let out = Command::new(bin_dir.join("leep-server"))
        .args(["--meta", &stores.meta, "--blobs", &stores.blobs])
        .args(["backup", "--out"])
        .arg(dir)
        .arg("--json")
        .env("RUST_LOG", "warn")
        .stdin(Stdio::null())
        .kill_on_drop(true)
        .output()
        .await
        .context("running leep-server backup")?;
    let manifest = serde_json::from_slice(&out.stdout).ok();
    Ok((out, manifest))
}

fn backup_problem_text(out: &std::process::Output, m: &Option<serde_json::Value>) -> String {
    let problems: Vec<String> = m
        .as_ref()
        .and_then(|r| r["problems"].as_array())
        .map(|ps| {
            ps.iter()
                .take(5)
                .map(|p| {
                    format!(
                        "[{}] {}",
                        p["code"].as_str().unwrap_or("?"),
                        p["message"].as_str().unwrap_or("?")
                    )
                })
                .collect()
        })
        .unwrap_or_default();
    format!(
        "exit {}; {}; stderr: {}",
        out.status,
        problems.join("; "),
        String::from_utf8_lossy(&out.stderr)
            .chars()
            .rev()
            .take(400)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect::<String>()
    )
}

/// Back up beside the swarm, every `every`: each snapshot must seal with a
/// clean check however much lands and however GC runs meanwhile.
pub async fn backup_loop(
    bin_dir: PathBuf,
    stores: Stores,
    sh: Arc<Shared>,
    dir: PathBuf,
    every: Duration,
) {
    let mut next = Instant::now() + every;
    while !sh.stopped() {
        tokio::time::sleep(Duration::from_millis(250)).await;
        if Instant::now() < next {
            continue;
        }
        let t0 = Instant::now();
        let (out, manifest) = match run_backup(&bin_dir, &stores, &dir).await {
            Ok(r) => r,
            Err(e) => {
                sh.violate("backup", format!("could not run backup: {e:#}"));
                return;
            }
        };
        sh.stats
            .add("backup_millis", t0.elapsed().as_millis() as u64);
        next = Instant::now() + every;
        if out.status.success() {
            sh.stats.inc("backup_runs");
            let n = |path: &[&str]| {
                path.iter()
                    .try_fold(manifest.as_ref()?.get("manifest")?, |v, k| v.get(*k))?
                    .as_u64()
            };
            sh.stats
                .add("backup_blobs_copied", n(&["blobs", "copied"]).unwrap_or(0));
            continue;
        }
        // A backup that races a restart may lose its database connection and
        // seals nothing (exit 2); the next one repeats it. Exit 1 never excuses.
        if out.status.code() == Some(2) && sh.in_chaos(sh.now_secs()) {
            sh.stats.inc("backup_failed_during_restart");
            continue;
        }
        sh.violate(
            "backup",
            format!("backup failed: {}", backup_problem_text(&out, &manifest)),
        );
        return;
    }
}

/// With the swarm stopped: take a last backup, restore it into scratch SQLite
/// and file stores (whatever the source backend is: a snapshot restores into
/// either), and verify what came back. Violations are reported under `backup`
/// and `restore`.
pub async fn final_restore_drill(bin_dir: &Path, stores: &Stores, sh: &Shared, dir: &Path) {
    let t0 = Instant::now();
    let (out, manifest) = match run_backup(bin_dir, stores, dir).await {
        Ok(r) => r,
        Err(e) => {
            sh.violate("backup", format!("could not run the final backup: {e:#}"));
            return;
        }
    };
    sh.stats
        .add("backup_millis", t0.elapsed().as_millis() as u64);
    let manifest = match manifest {
        Some(m) if out.status.success() => m,
        other => {
            sh.violate(
                "backup",
                format!(
                    "the final backup failed: {}",
                    backup_problem_text(&out, &other)
                ),
            );
            return;
        }
    };
    sh.stats.inc("backup_runs");
    let changesets = manifest["manifest"]["counts"]["changesets"]
        .as_u64()
        .unwrap_or(0);
    let scratch = dir.join("restore-drill");
    let _ = std::fs::remove_dir_all(&scratch);
    if let Err(e) = std::fs::create_dir_all(&scratch) {
        sh.violate(
            "restore",
            format!("cannot create {}: {e}", scratch.display()),
        );
        return;
    }
    let meta = format!("sqlite://{}?mode=rwc", scratch.join("meta.db").display());
    let blobs = format!("file://{}", scratch.join("blobs").display());
    let t1 = Instant::now();
    let restored = Command::new(bin_dir.join("leep-server"))
        .args(["--meta", &meta, "--blobs", &blobs])
        .args(["restore", "--from"])
        .arg(dir)
        .args(["--rehash", "--json"])
        .env("RUST_LOG", "warn")
        .stdin(Stdio::null())
        .kill_on_drop(true)
        .output()
        .await;
    let restored = match restored {
        Ok(o) => o,
        Err(e) => {
            sh.violate("restore", format!("could not run restore: {e}"));
            return;
        }
    };
    sh.stats
        .add("restore_millis", t1.elapsed().as_millis() as u64);
    let report: Option<serde_json::Value> = serde_json::from_slice(&restored.stdout).ok();
    if !restored.status.success()
        || report.as_ref().and_then(|r| r["changesets"].as_u64()) != Some(changesets)
    {
        sh.violate(
            "restore",
            format!(
                "restoring the final backup: {}",
                backup_problem_text(&restored, &report)
            ),
        );
        return;
    }
    sh.stats.add("restore_changesets", changesets);
    final_verify(
        bin_dir,
        &Stores {
            meta,
            blobs,
            blob_dir: Some(scratch.join("blobs")),
        },
        sh,
    )
    .await;
    // The scratch stores are as large as the soak's; keep them only for --keep-data.
    if !sh.args.keep_data {
        let _ = std::fs::remove_dir_all(&scratch);
    }
}

/// `leep-server verify --full --rehash` over the stores once the swarm has
/// stopped (ADR 0025): everything the run wrote must be reachable, intact and
/// derived correctly, however many restarts, collects and rebases it took.
/// Violations are reported under `verify`.
pub async fn final_verify(bin_dir: &Path, stores: &Stores, sh: &Shared) {
    let t0 = Instant::now();
    let out = Command::new(bin_dir.join("leep-server"))
        .args(["--meta", &stores.meta, "--blobs", &stores.blobs])
        .args(["verify", "--full", "--rehash", "--json"])
        .env("RUST_LOG", "warn")
        .stdin(Stdio::null())
        .kill_on_drop(true)
        .output()
        .await;
    let out = match out {
        Ok(o) => o,
        Err(e) => {
            sh.violate("verify", format!("could not run verify: {e}"));
            return;
        }
    };
    let report: Option<serde_json::Value> = serde_json::from_slice(&out.stdout).ok();
    let n = |path: &[&str]| {
        path.iter()
            .try_fold(report.as_ref()?, |v, k| v.get(*k))?
            .as_u64()
    };
    sh.stats.add(
        "verify_changesets",
        n(&["stats", "changesets_reachable"]).unwrap_or(0)
            + n(&["stats", "changesets_other"]).unwrap_or(0),
    );
    sh.stats
        .add("verify_millis", t0.elapsed().as_millis() as u64);
    if out.status.success() {
        return;
    }
    let problems: Vec<String> = report
        .as_ref()
        .and_then(|r| r["problems"].as_array())
        .map(|ps| {
            ps.iter()
                .take(5)
                .map(|p| {
                    format!(
                        "[{}] {}",
                        p["code"].as_str().unwrap_or("?"),
                        p["message"].as_str().unwrap_or("?")
                    )
                })
                .collect()
        })
        .unwrap_or_default();
    sh.violate(
        "verify",
        format!(
            "verify exited {} with {} problems; first: {}; stderr: {}",
            out.status,
            n(&["problems_total"]).unwrap_or(0),
            problems.join("; "),
            String::from_utf8_lossy(&out.stderr)
                .chars()
                .take(400)
                .collect::<String>()
        ),
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gc_output_is_parsed() {
        let m = parse_gc_output(
            "changesets scanned: 12\ncondemned: 5 blobs, 4096 bytes\ncollect: rescued by reference: 2\n",
        );
        assert_eq!(m["changesets scanned"], 12);
        assert_eq!(m["condemned"], 5);
        assert_eq!(m["condemned (bytes)"], 4096);
        assert_eq!(m["collect: rescued by reference"], 2);
    }

    #[test]
    fn the_fence_exceeds_the_request_timeout() {
        for s in [1, 2, 8, 30] {
            let t = Duration::from_secs(s);
            let (grace, fence) = gc_durations(t);
            assert!(fence > t && grace >= t * 2);
        }
    }

    #[test]
    fn database_name_is_swapped() {
        assert_eq!(
            replace_db("postgres://u:p@h:5432/postgres?sslmode=disable", "x").unwrap(),
            "postgres://u:p@h:5432/x?sslmode=disable"
        );
    }
}
