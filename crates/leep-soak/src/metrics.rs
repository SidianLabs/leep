//! Trends: the server's Prometheus metrics, process RSS and CPU, and the sizes
//! of the database and the blob directory, sampled throughout the run.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;

use crate::ops::{dir_size, pg_stats, PgStats, Server};
use crate::shared::Shared;
use crate::util::linear_fit;

const CLK_TCK: f64 = 100.0;

#[derive(Clone, Debug, Default, Serialize)]
pub struct Sample {
    pub t: f64,
    pub incarnation: u32,
    pub server_rss_mib: Option<f64>,
    pub server_cpu_cores: Option<f64>,
    pub harness_rss_mib: f64,
    pub db_mib: Option<f64>,
    pub blob_mib: Option<f64>,
    pub requests_total: u64,
    pub lands_total: u64,
    pub requests_per_sec: f64,
    pub lands_per_sec: f64,
    pub cache_hit_ratio: Option<f64>,
    pub in_flight: Option<f64>,
}

#[derive(Default)]
pub struct Trends {
    pub samples: Vec<Sample>,
    /// Last scrape of each server incarnation.
    pub scrapes: Vec<BTreeMap<String, f64>>,
    pub pg: Option<PgStats>,
    pub pg_first: Option<PgStats>,
    pub max_deadlocks: i64,
}

/// Parse Prometheus text into `series -> value` (labels kept in the key).
pub fn parse_prometheus(text: &str) -> BTreeMap<String, f64> {
    let mut m = BTreeMap::new();
    for line in text.lines() {
        if line.starts_with('#') || line.trim().is_empty() {
            continue;
        }
        let Some((k, v)) = line.rsplit_once(' ') else {
            continue;
        };
        if let Ok(v) = v.parse::<f64>() {
            m.insert(k.trim().to_string(), v);
        }
    }
    m
}

fn proc_status_kib(pid: u32, field: &str) -> Option<f64> {
    let text = std::fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
    text.lines()
        .find_map(|l| l.strip_prefix(field))
        .and_then(|r| {
            r.trim_start_matches(':')
                .split_whitespace()
                .next()?
                .parse()
                .ok()
        })
}

fn proc_cpu_ticks(pid: u32) -> Option<f64> {
    let text = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    // The command name may contain spaces and parentheses: split after it.
    let rest = text.rsplit_once(')')?.1;
    let f: Vec<&str> = rest.split_whitespace().collect();
    // After the name: state is field 0, utime 11, stime 12.
    Some(f.get(11)?.parse::<f64>().ok()? + f.get(12)?.parse::<f64>().ok()?)
}

pub fn sum_prefix(m: &BTreeMap<String, f64>, prefix: &str) -> f64 {
    m.iter()
        .filter(|(k, _)| k.starts_with(prefix))
        .map(|(_, v)| *v)
        .sum()
}

pub struct SamplerCfg {
    pub sh: Arc<Shared>,
    pub trends: Arc<std::sync::Mutex<Trends>>,
    pub server: Option<Arc<Server>>,
    pub pid: Option<u32>,
    pub metrics_url: Option<String>,
    pub pg_url: Option<String>,
    pub blob_dir: Option<PathBuf>,
    pub every: Duration,
}

pub async fn sampler(cfg: SamplerCfg) {
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("metrics client");
    let me = std::process::id();
    let mut last_ticks: Option<(f64, f64)> = None;
    let mut last_reqs = (0u64, 0u64, 0.0f64);
    let mut last_incarnation = 0u32;
    let mut round = 0u64;
    let mut blob_mib = None;
    let mut prev_requests_sum = 0.0;
    loop {
        round += 1;
        let t = cfg.sh.now_secs();
        let incarnation = cfg
            .server
            .as_ref()
            .map_or(1, |s| s.incarnation.load(Ordering::Relaxed));
        let pid = cfg
            .server
            .as_ref()
            .map(|s| s.pid.load(Ordering::Relaxed))
            .or(cfg.pid)
            .filter(|p| *p != 0);
        let rss = pid
            .and_then(|p| proc_status_kib(p, "VmRSS"))
            .map(|k| k / 1024.0);
        let ticks = pid.and_then(proc_cpu_ticks);
        let cpu = match (ticks, last_ticks, last_incarnation == incarnation) {
            (Some(now), Some((prev, pt)), true) if t > pt => {
                Some((now - prev) / CLK_TCK / (t - pt))
            }
            _ => None,
        };
        last_ticks = ticks.map(|x| (x, t));
        last_incarnation = incarnation;

        let mut hit_ratio = None;
        let mut in_flight = None;
        if let Some(url) = &cfg.metrics_url {
            if let Ok(r) = client.get(url).send().await {
                if let Ok(text) = r.text().await {
                    let m = parse_prometheus(&text);
                    let reqs = sum_prefix(&m, "leep_http_requests_total");
                    let mut tr = cfg.trends.lock().unwrap();
                    // A restart (or a counter reset) starts a new incarnation.
                    if tr.scrapes.is_empty() || reqs < prev_requests_sum {
                        tr.scrapes.push(m.clone());
                    } else {
                        *tr.scrapes.last_mut().unwrap() = m.clone();
                    }
                    prev_requests_sum = reqs;
                    let hits = m.get("leep_blob_cache_hits_total").copied().unwrap_or(0.0);
                    let miss = m
                        .get("leep_blob_cache_misses_total")
                        .copied()
                        .unwrap_or(0.0);
                    if hits + miss > 0.0 {
                        hit_ratio = Some(hits / (hits + miss));
                    }
                    in_flight = Some(sum_prefix(&m, "leep_requests_in_flight"));
                }
            }
        }
        let mut db_mib = None;
        if let Some(url) = &cfg.pg_url {
            if let Ok(s) = pg_stats(url).await {
                db_mib = Some(s.db_bytes as f64 / 1048576.0);
                let mut tr = cfg.trends.lock().unwrap();
                tr.max_deadlocks = tr.max_deadlocks.max(s.deadlocks);
                if tr.pg_first.is_none() {
                    tr.pg_first = Some(s.clone());
                }
                tr.pg = Some(s);
            }
        }
        if round % 6 == 1 {
            if let Some(d) = cfg.blob_dir.clone() {
                blob_mib = tokio::task::spawn_blocking(move || dir_size(&d))
                    .await
                    .ok()
                    .map(|b| b as f64 / 1048576.0);
            }
        }
        let (reqs, lands) = {
            let s = cfg.sh.stats.0.lock().unwrap();
            (s.status.values().sum::<u64>(), s.land_samples.len() as u64)
        };
        let dt = (t - last_reqs.2).max(0.001);
        let sample = Sample {
            t,
            incarnation,
            server_rss_mib: rss,
            server_cpu_cores: cpu,
            harness_rss_mib: proc_status_kib(me, "VmRSS").unwrap_or(0.0) / 1024.0,
            db_mib,
            blob_mib,
            requests_total: reqs,
            lands_total: lands,
            requests_per_sec: (reqs - last_reqs.0) as f64 / dt,
            lands_per_sec: (lands - last_reqs.1) as f64 / dt,
            cache_hit_ratio: hit_ratio,
            in_flight,
        };
        last_reqs = (reqs, lands, t);
        cfg.trends.lock().unwrap().samples.push(sample);
        if cfg.sh.stopped() {
            return;
        }
        let end = std::time::Instant::now() + cfg.every;
        while std::time::Instant::now() < end {
            if cfg.sh.stopped() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct RssFit {
    pub evaluated: bool,
    pub why_not: Option<String>,
    pub slope_mib_per_hour: Option<f64>,
    pub r: Option<f64>,
    pub samples: usize,
    pub window_secs: f64,
    pub start_mib: Option<f64>,
    pub end_mib: Option<f64>,
}

/// Linear fit of server RSS over the second half of the run, restricted to
/// the last server incarnation (a restart resets memory, which is not a
/// leak). `min_window` is the shortest window worth judging.
pub fn rss_slope(samples: &[Sample], run_secs: f64, min_window: f64) -> RssFit {
    let mut fit = RssFit::default();
    let Some(last_inc) = samples
        .iter()
        .rev()
        .find(|s| s.server_rss_mib.is_some())
        .map(|s| s.incarnation)
    else {
        fit.why_not = Some("no RSS samples (server pid unknown)".into());
        return fit;
    };
    let from = run_secs / 2.0;
    let pts: Vec<(f64, f64)> = samples
        .iter()
        .filter(|s| s.incarnation == last_inc && s.t >= from && s.t <= run_secs)
        .filter_map(|s| Some((s.t, s.server_rss_mib?)))
        .collect();
    fit.samples = pts.len();
    if pts.len() < 3 {
        fit.why_not = Some("fewer than three samples in the window".into());
        return fit;
    }
    fit.window_secs = pts.last().unwrap().0 - pts[0].0;
    fit.start_mib = Some(pts[0].1);
    fit.end_mib = Some(pts.last().unwrap().1);
    let xs: Vec<f64> = pts.iter().map(|p| p.0 / 3600.0).collect();
    let ys: Vec<f64> = pts.iter().map(|p| p.1).collect();
    if let Some((slope, r)) = linear_fit(&xs, &ys) {
        fit.slope_mib_per_hour = Some(slope);
        fit.r = Some(r);
    }
    if fit.window_secs < min_window {
        fit.why_not = Some(format!(
            "window of {:.0}s is shorter than {:.0}s; reported, not judged",
            fit.window_secs, min_window
        ));
    } else {
        fit.evaluated = true;
    }
    fit
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct ServerSide {
    pub incarnations: usize,
    pub requests_by_class: BTreeMap<String, f64>,
    pub land_by_outcome: BTreeMap<String, f64>,
    pub land_cas_losses: f64,
    pub land_attempts_mean: Option<f64>,
    pub land_duration_mean_ms: Option<f64>,
    pub land_duration_p99_ms: Option<f64>,
    pub land_lock_wait_mean_ms: Option<f64>,
    pub derive: BTreeMap<String, DeriveStat>,
    pub search_total: f64,
    pub search_mean_ms: Option<f64>,
    pub cache_hits: f64,
    pub cache_misses: f64,
    pub cache_hit_ratio: Option<f64>,
    pub limited: BTreeMap<String, f64>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct DeriveStat {
    pub count: f64,
    pub mean_ms: Option<f64>,
}

fn label(series: &str, name: &str) -> Option<String> {
    let start = series.find(&format!("{name}=\""))? + name.len() + 2;
    let rest = &series[start..];
    Some(rest[..rest.find('"')?].to_string())
}

/// Upper bound of the bucket holding the q-quantile of merged histograms.
fn bucket_quantile(scrapes: &[BTreeMap<String, f64>], metric: &str, q: f64) -> Option<f64> {
    let mut buckets: BTreeMap<u64, f64> = BTreeMap::new();
    let mut inf = 0.0;
    for m in scrapes {
        for (k, v) in m {
            if !k.starts_with(&format!("{metric}_bucket")) {
                continue;
            }
            match label(k, "le")?.as_str() {
                "+Inf" => inf += v,
                le => {
                    *buckets
                        .entry((le.parse::<f64>().ok()? * 1e6) as u64)
                        .or_default() += v
                }
            }
        }
    }
    if inf == 0.0 {
        return None;
    }
    let want = inf * q;
    buckets
        .iter()
        .find(|(_, c)| **c >= want)
        .map(|(le, _)| *le as f64 / 1e6)
}

pub fn server_side(scrapes: &[BTreeMap<String, f64>]) -> ServerSide {
    let mut s = ServerSide {
        incarnations: scrapes.len(),
        ..Default::default()
    };
    let total = |prefix: &str| scrapes.iter().map(|m| sum_prefix(m, prefix)).sum::<f64>();
    let exact = |name: &str| {
        scrapes
            .iter()
            .map(|m| m.get(name).copied().unwrap_or(0.0))
            .sum::<f64>()
    };
    for m in scrapes {
        for (k, v) in m {
            if k.starts_with("leep_http_requests_total") {
                if let Some(st) = label(k, "status") {
                    *s.requests_by_class
                        .entry(format!("{}xx", &st[..1.min(st.len())]))
                        .or_default() += v;
                }
            } else if k.starts_with("leep_land_total") {
                if let Some(o) = label(k, "outcome") {
                    *s.land_by_outcome.entry(o).or_default() += v;
                }
            } else if k.starts_with("leep_derive_total") {
                if let Some(kind) = label(k, "kind") {
                    s.derive.entry(kind).or_default().count += v;
                }
            } else if k.starts_with("leep_requests_limited_total") {
                if let Some(r) = label(k, "reason") {
                    *s.limited.entry(r).or_default() += v;
                }
            }
        }
    }
    s.land_cas_losses = exact("leep_land_cas_losses_total");
    let mean = |sum: &str, count: &str, scale: f64| {
        let c = exact(count);
        (c > 0.0).then(|| exact(sum) / c * scale)
    };
    s.land_attempts_mean = mean("leep_land_attempts_sum", "leep_land_attempts_count", 1.0);
    s.land_duration_mean_ms = mean(
        "leep_land_duration_seconds_sum",
        "leep_land_duration_seconds_count",
        1000.0,
    );
    s.land_lock_wait_mean_ms = mean(
        "leep_land_lock_wait_seconds_sum",
        "leep_land_lock_wait_seconds_count",
        1000.0,
    );
    s.land_duration_p99_ms =
        bucket_quantile(scrapes, "leep_land_duration_seconds", 0.99).map(|x| x * 1000.0);
    for (kind, d) in s.derive.iter_mut() {
        let sum: f64 = scrapes
            .iter()
            .map(|m| {
                m.iter()
                    .filter(|(k, _)| {
                        k.starts_with("leep_derive_duration_seconds_sum")
                            && label(k, "kind").as_deref() == Some(kind.as_str())
                    })
                    .map(|(_, v)| *v)
                    .sum::<f64>()
            })
            .sum();
        if d.count > 0.0 {
            d.mean_ms = Some(sum / d.count * 1000.0);
        }
    }
    s.search_total = total("leep_search_total");
    s.search_mean_ms = mean(
        "leep_search_duration_seconds_sum",
        "leep_search_duration_seconds_count",
        1000.0,
    );
    s.cache_hits = exact("leep_blob_cache_hits_total");
    s.cache_misses = exact("leep_blob_cache_misses_total");
    if s.cache_hits + s.cache_misses > 0.0 {
        s.cache_hit_ratio = Some(s.cache_hits / (s.cache_hits + s.cache_misses));
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prometheus_text_and_quantiles() {
        let m = parse_prometheus(
            "# TYPE x counter\nleep_land_total{outcome=\"landed\"} 7\n\
             leep_land_duration_seconds_bucket{le=\"0.1\"} 90\n\
             leep_land_duration_seconds_bucket{le=\"1\"} 99\n\
             leep_land_duration_seconds_bucket{le=\"+Inf\"} 100\n\
             leep_land_duration_seconds_sum 12\nleep_land_duration_seconds_count 100\n",
        );
        let s = server_side(std::slice::from_ref(&m));
        assert_eq!(s.land_by_outcome["landed"], 7.0);
        assert_eq!(s.land_duration_p99_ms, Some(1000.0));
        assert!((s.land_duration_mean_ms.unwrap() - 120.0).abs() < 1e-9);
        let both = server_side(&[m.clone(), m]);
        assert_eq!(both.land_by_outcome["landed"], 14.0);
    }

    #[test]
    fn rss_slope_uses_the_second_half_of_the_last_incarnation() {
        let samples: Vec<Sample> = (0..100)
            .map(|i| Sample {
                t: f64::from(i) * 10.0,
                incarnation: if i < 30 { 1 } else { 2 },
                server_rss_mib: Some(if i < 50 {
                    500.0
                } else {
                    100.0 + f64::from(i - 50) * 0.5
                }),
                ..Default::default()
            })
            .collect();
        let f = rss_slope(&samples, 1000.0, 100.0);
        assert!(f.evaluated);
        let slope = f.slope_mib_per_hour.unwrap();
        // 0.5 MiB per 10 s is 180 MiB per hour.
        assert!((slope - 180.0).abs() < 1.0, "{slope}");
        assert!(!rss_slope(&samples, 1000.0, 9000.0).evaluated);
    }
}
