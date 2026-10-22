//! The JSON report, the human summary and the pass/fail decision.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

use anyhow::Result;
use serde_json::{json, Value};

use crate::config::Args;
use crate::metrics::{rss_slope, server_side, Trends};
use crate::ops::{DeriveRun, GcRun};
use crate::repo::Layout;
use crate::shared::{Shared, Violation};
use crate::util::{percentile, round2};

pub struct Inputs<'a> {
    pub args: &'a Args,
    pub sh: &'a Arc<Shared>,
    pub out: &'a Path,
    pub trends: &'a Arc<Mutex<Trends>>,
    pub gc_runs: &'a Arc<Mutex<Vec<GcRun>>>,
    pub derive_runs: &'a Arc<Mutex<Vec<DeriveRun>>>,
    pub checker_counts: &'a BTreeMap<&'static str, u64>,
    pub nodes_known: usize,
    pub layout: Layout,
    pub base_url: &'a str,
    pub backend: &'a str,
}

fn ms(us: u32) -> f64 {
    round2(f64::from(us) / 1000.0)
}

fn dist(mut v: Vec<u32>) -> Value {
    v.sort_unstable();
    let n = v.len();
    if n == 0 {
        return json!({ "count": 0 });
    }
    let mean = v.iter().map(|x| f64::from(*x)).sum::<f64>() / n as f64 / 1000.0;
    json!({
        "count": n,
        "mean_ms": round2(mean),
        "p50_ms": ms(percentile(&v, 50.0).unwrap()),
        "p95_ms": ms(percentile(&v, 95.0).unwrap()),
        "p99_ms": ms(percentile(&v, 99.0).unwrap()),
        "max_ms": ms(*v.last().unwrap()),
    })
}

fn p99_ms(v: &mut [u32]) -> Option<f64> {
    v.sort_unstable();
    percentile(v, 99.0).map(ms)
}

fn push_violation(sh: &Shared, kind: &str, detail: String) {
    eprintln!("VIOLATION [{kind}]: {detail}");
    sh.violations.lock().unwrap().push(Violation {
        at_secs: sh.now_secs(),
        kind: kind.to_string(),
        detail,
    });
}

pub fn write(i: Inputs) -> Result<bool> {
    let sh = i.sh;
    let args = i.args;
    let run_secs = sh.stats.get("run_secs_x1000") as f64 / 1000.0;
    let run_secs = if run_secs > 0.0 {
        run_secs
    } else {
        sh.now_secs().max(1.0)
    };
    let st = sh.stats.0.lock().unwrap();
    let tr = i.trends.lock().unwrap();
    let gc_runs = i.gc_runs.lock().unwrap().clone();
    let derive_runs = i.derive_runs.lock().unwrap().clone();

    // ---- latency ----
    let mut land_us: Vec<u32> = st.land_samples.iter().map(|(_, us)| *us).collect();
    let land = dist(land_us.clone());
    let land_p99 = p99_ms(&mut land_us);
    let mut windows: BTreeMap<u32, Vec<u32>> = BTreeMap::new();
    for (t, us) in &st.land_samples {
        windows.entry(t / 60).or_default().push(*us);
    }
    let per_minute: Vec<Value> = windows
        .into_iter()
        .map(|(m, mut v)| {
            v.sort_unstable();
            json!({
                "minute": m,
                "lands": v.len(),
                "p50_ms": ms(percentile(&v, 50.0).unwrap()),
                "p99_ms": ms(percentile(&v, 99.0).unwrap()),
            })
        })
        .collect();
    let routes: BTreeMap<String, Value> = st
        .routes
        .iter()
        .map(|(k, r)| ((*k).to_string(), dist(r.micros.clone())))
        .collect();
    let land_requests = st.land_samples.len() as f64;
    let counters: BTreeMap<String, u64> = st
        .counters
        .iter()
        .map(|(k, v)| ((*k).to_string(), *v))
        .collect();
    let status: BTreeMap<String, u64> =
        st.status.iter().map(|(k, v)| (k.to_string(), *v)).collect();
    let requests: u64 = st.status.values().sum();
    let c = |name: &str| counters.get(name).copied().unwrap_or(0);
    let http_404 = st.status.get(&404).copied().unwrap_or(0);
    let e2e = dist(st.land_e2e.clone());
    drop(st);

    // ---- trends ----
    let fit = rss_slope(&tr.samples, run_secs, args.rss_min_window_secs as f64);
    let server = server_side(&tr.scrapes);
    let first = |f: fn(&crate::metrics::Sample) -> Option<f64>| tr.samples.iter().find_map(f);
    let last = |f: fn(&crate::metrics::Sample) -> Option<f64>| tr.samples.iter().rev().find_map(f);
    let cpu: Vec<f64> = tr
        .samples
        .iter()
        .filter_map(|s| s.server_cpu_cores)
        .collect();
    let cpu_mean = (!cpu.is_empty()).then(|| cpu.iter().sum::<f64>() / cpu.len() as f64);
    let rss_max = tr
        .samples
        .iter()
        .filter_map(|s| s.server_rss_mib)
        .fold(None, |m: Option<f64>, x| Some(m.map_or(x, |m| m.max(x))));
    let restarts = c("restarts_sigterm") + c("restarts_sigkill");

    // ---- thresholds ----
    let mut thresholds = Vec::new();
    let limit_ms = args.max_land_p99.as_secs_f64() * 1000.0;
    if let Some(p) = land_p99 {
        let pass = p <= limit_ms;
        thresholds.push(
            json!({ "name": "land p99 (client, ms)", "limit": limit_ms, "value": p, "pass": pass }),
        );
        if !pass {
            push_violation(
                sh,
                "threshold_land_p99",
                format!("land p99 {p} ms exceeds {limit_ms} ms"),
            );
        }
    }
    match (fit.evaluated, fit.slope_mib_per_hour) {
        (true, Some(s)) => {
            let pass = s <= args.max_rss_slope_mib_h;
            thresholds.push(json!({ "name": "server RSS slope (MiB/h)", "limit": args.max_rss_slope_mib_h, "value": round2(s), "pass": pass }));
            if !pass {
                push_violation(
                    sh,
                    "threshold_rss_slope",
                    format!(
                        "server RSS grows {s:.1} MiB/h over the second half (limit {}); {:?} to {:?} MiB",
                        args.max_rss_slope_mib_h, fit.start_mib, fit.end_mib
                    ),
                );
            }
        }
        _ => thresholds.push(json!({ "name": "server RSS slope (MiB/h)", "limit": args.max_rss_slope_mib_h, "value": fit.slope_mib_per_hour, "pass": true, "note": fit.why_not })),
    }
    if let Some(p) = &tr.pg {
        let pass = p.deadlocks == 0;
        thresholds.push(
            json!({ "name": "postgres deadlocks", "limit": 0, "value": p.deadlocks, "pass": pass }),
        );
        if !pass {
            push_violation(
                sh,
                "postgres_deadlock",
                format!("{} deadlocks reported by Postgres", p.deadlocks),
            );
        }
    }
    let acks = sh.ledger.len();
    let lands = acks as f64;
    if args.duration.as_secs() >= 10 && acks == 0 {
        push_violation(sh, "no_progress", "no land was acknowledged".into());
    }
    // Shed reads and lands not admitted are 503s the clients saw and
    // counted apart (ADR 0060).
    let server_5xx = (server.requests_by_class.get("5xx").copied().unwrap_or(0.0)
        - c("shed_503") as f64
        - c("admission_503") as f64)
        .max(0.0);
    let client_5xx = c("server_errors_5xx");
    if restarts == 0 && server_5xx > 0.0 && client_5xx == 0 && !args.chaos {
        push_violation(
            sh,
            "server_5xx_unseen",
            format!("the server counted {server_5xx} 5xx answers the clients never saw"),
        );
    }
    for g in &gc_runs {
        if g.verified == Some(false) {
            push_violation(
                sh,
                "gc",
                format!("a GC at {:.0}s was not verified", g.at_secs),
            );
        }
    }
    let mut baseline = Value::Null;
    if let Some(path) = &args.baseline {
        match std::fs::read_to_string(path)
            .ok()
            .and_then(|t| serde_json::from_str::<Value>(&t).ok())
        {
            Some(b) => {
                let bp = b["latency"]["land"]["p99_ms"].as_f64();
                let bt = b["throughput"]["lands_per_sec"].as_f64();
                let mut cmp = json!({ "file": path, "land_p99_ms": bp, "lands_per_sec": bt });
                if let (Some(bp), Some(p)) = (bp, land_p99) {
                    let pass = p <= bp * args.max_regression;
                    cmp["p99_pass"] = pass.into();
                    if !pass {
                        push_violation(
                            sh,
                            "baseline_regression",
                            format!(
                                "land p99 {p} ms vs baseline {bp} ms (factor {})",
                                args.max_regression
                            ),
                        );
                    }
                }
                if let Some(bt) = bt {
                    let now = lands / run_secs;
                    let pass = now >= bt / args.max_regression;
                    cmp["throughput_pass"] = pass.into();
                    if !pass {
                        push_violation(
                            sh,
                            "baseline_regression",
                            format!(
                                "{now:.1} lands/s vs baseline {bt:.1} (factor {})",
                                args.max_regression
                            ),
                        );
                    }
                }
                baseline = cmp;
            }
            None => push_violation(
                sh,
                "baseline_regression",
                format!("cannot read baseline {}", path.display()),
            ),
        }
    }

    // ---- warnings (never fail the run) ----
    let mut warnings: Vec<String> = Vec::new();
    if args.gc_every().is_some() && args.duration.as_secs() >= 60 && gc_runs.is_empty() {
        warnings.push("no GC completed during the run".into());
    }
    if !gc_runs.is_empty() && c("gc_blobs_deleted") == 0 {
        warnings
            .push("GC ran but deleted nothing: the garbage it exists for was not reclaimed".into());
    }
    if c("timeouts_408") > 0 {
        warnings.push(format!(
            "{} requests hit the server's request timeout (408)",
            c("timeouts_408")
        ));
    }
    if args.chaos && restarts < args.chaos_restarts as u64 {
        warnings.push(format!(
            "only {restarts} of {} restarts happened",
            args.chaos_restarts
        ));
    }
    if (args.rate_limit.is_some() || args.max_concurrent.is_some()) && c("throttled_429") == 0 {
        warnings.push("request limits are configured but no request was throttled".into());
    }
    if c("land_conflicts") == 0 && args.duration.as_secs() >= 60 {
        warnings.push("no land ever conflicted; the conflict paths were not exercised".into());
    }

    let violations: Vec<Violation> = sh.violations.lock().unwrap().clone();
    let mut kinds: BTreeMap<String, usize> = BTreeMap::new();
    for v in &violations {
        *kinds.entry(v.kind.clone()).or_default() += 1;
    }
    let passed = violations.is_empty();

    let samples_json: Vec<Value> = tr
        .samples
        .iter()
        .map(|s| serde_json::to_value(s).unwrap())
        .collect();
    let report = json!({
        "result": if passed { "pass" } else { "fail" },
        "run_id": sh.run_id,
        "seed": args.seed,
        "config": {
            "duration_secs": args.duration.as_secs(),
            "agents": args.agents,
            "cli_agents": args.cli_agents,
            "files": i.layout.total(),
            "think_ms": args.think.as_millis() as u64,
            "request_timeout_secs": args.request_timeout.as_secs(),
            "backend": i.backend,
            "server": i.base_url,
            "chaos": args.chaos,
            "policy": !args.no_policy,
            "rate_limit": args.rate_limit,
            "max_concurrent": args.max_concurrent,
            "gc_interval_secs": args.gc_every().map(|d| d.as_secs()),
            "derive_interval_secs": args.derive_every().map(|d| d.as_secs()),
            "thresholds": {
                "max_land_p99_ms": limit_ms,
                "max_rss_slope_mib_h": args.max_rss_slope_mib_h,
            },
        },
        "run_secs": round2(run_secs),
        "violations": violations,
        "violation_kinds": kinds,
        "warnings": warnings,
        "thresholds": thresholds,
        "baseline": baseline,
        "throughput": {
            "land_requests": land_requests as u64,
            "lands_acked": acks,
            "lands_per_sec": round2(lands / run_secs),
            "requests": requests,
            "requests_per_sec": round2(requests as f64 / run_secs),
            "agent_iterations": sh.iterations.load(std::sync::atomic::Ordering::Relaxed),
        },
        "latency": {
            "land": land,
            "land_end_to_end": e2e,
            "land_per_minute": per_minute,
            "routes": routes,
        },
        "status_counts": status,
        "errors": {
            "server_5xx": client_5xx,
            "attributed_to_restart": c("attributed_to_restart"),
            "transport_errors": c("transport_errors"),
            "throttled_429": c("throttled_429"),
            "shed_503": c("shed_503"),
            "admission_503": c("admission_503"),
            "timeouts_408": c("timeouts_408"),
            "land_409": c("land_409"),
            "missing_blob_retries": c("missing_blob_retries"),
            "http_404": http_404,
        },
        "workload": counters,
        "invariants": {
            "checks": i.checker_counts,
            "changesets_known": i.nodes_known,
            "acks": acks,
        },
        "restarts": sh.chaos.lock().unwrap().iter().map(|e| json!({
            "kind": e.kind, "start_secs": round2(e.start), "end_secs": e.end.map(round2),
        })).collect::<Vec<_>>(),
        "gc": gc_runs,
        "derive_search": derive_runs,
        "server_metrics": server,
        "resources": {
            "rss_fit": fit,
            "server_rss_mib": { "first": first(|s| s.server_rss_mib), "last": last(|s| s.server_rss_mib), "max": rss_max },
            "server_cpu_cores_mean": cpu_mean,
            "harness_rss_mib": { "first": first(|s| Some(s.harness_rss_mib)), "last": last(|s| Some(s.harness_rss_mib)) },
            "db_mib": { "first": first(|s| s.db_mib), "last": last(|s| s.db_mib) },
            "blob_mib": { "first": first(|s| s.blob_mib), "last": last(|s| s.blob_mib) },
            "postgres_tables_bytes": tr.pg.as_ref().map(|p| &p.tables),
            "postgres_connections": tr.pg.as_ref().map(|p| p.connections),
        },
        "samples": samples_json,
    });
    std::fs::write(
        i.out.join("report.json"),
        serde_json::to_string_pretty(&report)?,
    )?;

    // ---- summary ----
    let mut s = String::new();
    let f = |v: &Value| v.as_f64().map_or("n/a".to_string(), |x| format!("{x:.1}"));
    s.push_str(&format!(
        "leep-soak {} ({}): {} after {:.0}s, {} agents + {} cli, seed {}, backend {}\n",
        sh.run_id,
        if args.chaos { "chaos" } else { "steady" },
        if passed { "PASS" } else { "FAIL" },
        run_secs,
        args.agents,
        args.cli_agents,
        args.seed,
        i.backend
    ));
    s.push_str(&format!(
        "throughput  {:.1} lands/s ({} acknowledged, {} land requests answered), {:.0} requests/s\n",
        lands / run_secs,
        acks,
        land_requests as u64,
        requests as f64 / run_secs
    ));
    s.push_str(&format!(
        "land (client)  p50 {} ms  p95 {} ms  p99 {} ms  max {} ms; end to end p50 {} ms p99 {} ms\n",
        f(&report["latency"]["land"]["p50_ms"]),
        f(&report["latency"]["land"]["p95_ms"]),
        f(&report["latency"]["land"]["p99_ms"]),
        f(&report["latency"]["land"]["max_ms"]),
        f(&report["latency"]["land_end_to_end"]["p50_ms"]),
        f(&report["latency"]["land_end_to_end"]["p99_ms"]),
    ));
    s.push_str(&format!(
        "land (server)  attempts mean {}  CAS losses {}  p99 <= {} ms; outcomes {:?}\n",
        server
            .land_attempts_mean
            .map_or("n/a".into(), |x| format!("{x:.2}")),
        server.land_cas_losses,
        server
            .land_duration_p99_ms
            .map_or("n/a".into(), |x| format!("{x:.0}")),
        server.land_by_outcome,
    ));
    s.push_str(&format!(
        "workload  fast-forward {}  rebased {}  conflicts {}  branches {} (merged {}, retired {})  tags {}  release lands {}  policy refusals {}\n",
        c("lands_fast_forward"), c("lands_rebased"), c("land_conflicts"), c("branches_created"),
        c("branches_merged"), c("branches_retired"), c("tags_created"), c("release_lands"), c("policy_refusals"),
    ));
    s.push_str(&format!(
        "errors  5xx {}  attributed to restarts {}  transport {}  429 {}  shed 503 {}  admission 503 {}  408 {}  land 409 {}  missing-blob retries {}\n",
        client_5xx, c("attributed_to_restart"), c("transport_errors"), c("throttled_429"),
        c("shed_503"), c("admission_503"), c("timeouts_408"), c("land_409"), c("missing_blob_retries"),
    ));
    s.push_str(&format!(
        "server  RSS {} -> {} MiB (max {}), slope {} MiB/h over {:.0}s ({}), CPU {} cores, cache hit {}\n",
        f(&report["resources"]["server_rss_mib"]["first"]),
        f(&report["resources"]["server_rss_mib"]["last"]),
        f(&report["resources"]["server_rss_mib"]["max"]),
        fit.slope_mib_per_hour.map_or("n/a".into(), |x| format!("{x:.1}")),
        fit.window_secs,
        if fit.evaluated { "judged" } else { "not judged" },
        cpu_mean.map_or("n/a".into(), |x| format!("{x:.2}")),
        server.cache_hit_ratio.map_or("n/a".into(), |x| format!("{:.0}%", x * 100.0)),
    ));
    s.push_str(&format!(
        "storage  database {} -> {} MiB, blobs {} -> {} MiB; harness RSS {} -> {} MiB\n",
        f(&report["resources"]["db_mib"]["first"]),
        f(&report["resources"]["db_mib"]["last"]),
        f(&report["resources"]["blob_mib"]["first"]),
        f(&report["resources"]["blob_mib"]["last"]),
        f(&report["resources"]["harness_rss_mib"]["first"]),
        f(&report["resources"]["harness_rss_mib"]["last"]),
    ));
    s.push_str(&format!(
        "gc  {} runs, {} blobs condemned, {} deleted, all verified: {}; derive-search {} runs; restarts {} ({} SIGTERM, {} SIGKILL)\n",
        gc_runs.len(), c("gc_blobs_condemned"), c("gc_blobs_deleted"),
        gc_runs.iter().all(|g| g.verified != Some(false)), derive_runs.len(), restarts, c("restarts_sigterm"), c("restarts_sigkill"),
    ));
    s.push_str(&format!("invariant checks  {:?}\n", i.checker_counts));
    for t in &thresholds {
        s.push_str(&format!(
            "threshold  {}: {} (limit {}) {}\n",
            t["name"].as_str().unwrap_or(""),
            t["value"],
            t["limit"],
            if t["pass"].as_bool().unwrap_or(true) {
                "ok"
            } else {
                "EXCEEDED"
            }
        ));
    }
    for w in &warnings {
        s.push_str(&format!("warning  {w}\n"));
    }
    for v in violations.iter().take(15) {
        s.push_str(&format!(
            "VIOLATION [{}] at {:.1}s: {}\n",
            v.kind, v.at_secs, v.detail
        ));
    }
    if violations.len() > 15 {
        s.push_str(&format!(
            "... and {} more in report.json\n",
            violations.len() - 15
        ));
    }
    std::fs::write(i.out.join("summary.txt"), &s)?;
    println!("\n{s}");
    println!("report: {}", i.out.join("report.json").display());
    Ok(passed)
}
