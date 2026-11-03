//! The ratio bench's verdict (ADR 0081): compares `leep-land-bench` and
//! `leep-swarm` JSON from runs of a base server (A) and a changed one (B)
//! made in turn on one machine (A, B, A, B), and fails only on a
//! difference the A runs do not show between themselves.
//!
//! Two kinds of check. Counts that a healthy server keeps near zero or
//! fixed (lands not admitted, 409s, stalled agents, CAS losses, discarded
//! pipelined attempts, shed reads, pool connections in use) fail when both
//! B runs exceed what the A runs allow: `factor` times the larger A value
//! plus `slack`. Ratios of means (throughput, p99, statements and bytes per
//! land) fail outside a wide band. Either check is reported *inconclusive*,
//! not failed, when the two A runs differ from each other by as much as the
//! check allows: the machine was too noisy to tell.
//!
//! Exits 1 if any check failed, else 0; writes a Markdown table to stdout
//! and, when `GITHUB_STEP_SUMMARY` is set, to that file too.

use std::fmt::Write as _;
use std::path::PathBuf;

use anyhow::{bail, Context, Result};
use clap::Parser;
use serde_json::Value;

#[derive(Parser)]
#[command(about = "Compare A/B/A/B runs of leep-land-bench and leep-swarm")]
struct Args {
    /// Directories of the base server's runs, comma separated, each with
    /// `land.json` and `swarm.json`.
    #[arg(long, value_delimiter = ',', required = true)]
    a: Vec<PathBuf>,
    /// Directories of the changed server's runs, in the same form.
    #[arg(long, value_delimiter = ',', required = true)]
    b: Vec<PathBuf>,
}

/// Where a value comes from: a file of the run and a path into its JSON
/// (`land.json` is an array of rows; the first row is read).
#[derive(Clone, Copy)]
struct Source {
    file: &'static str,
    path: &'static [&'static str],
}

const fn land(path: &'static [&'static str]) -> Source {
    Source {
        file: "land.json",
        path,
    }
}

const fn swarm(path: &'static [&'static str]) -> Source {
    Source {
        file: "swarm.json",
        path,
    }
}

#[derive(Clone, Copy)]
enum Rule {
    /// Fails when every B value exceeds `factor * max(A) + slack`.
    Count { factor: f64, slack: f64 },
    /// Fails when mean(B) / mean(A) is above `max` (higher is worse).
    RatioAbove(f64),
    /// Fails when mean(B) / mean(A) is below `min` (lower is worse).
    RatioBelow(f64),
}

struct Check {
    name: &'static str,
    source: Source,
    rule: Rule,
}

const fn count(name: &'static str, source: Source, factor: f64, slack: f64) -> Check {
    Check {
        name,
        source,
        rule: Rule::Count { factor, slack },
    }
}

const fn above(name: &'static str, source: Source, max: f64) -> Check {
    Check {
        name,
        source,
        rule: Rule::RatioAbove(max),
    }
}

const fn below(name: &'static str, source: Source, min: f64) -> Check {
    Check {
        name,
        source,
        rule: Rule::RatioBelow(min),
    }
}

/// Every check, in the order reported.
const CHECKS: &[Check] = &[
    // Counts that the swarm and the servers report.
    count(
        "swarm lands not admitted",
        swarm(&["admission_refused"]),
        2.0,
        20.0,
    ),
    count("swarm 409s", swarm(&["status_409"]), 2.0, 20.0),
    count("swarm stalled agents", swarm(&["stalled_agents"]), 2.0, 2.0),
    count("swarm CAS losses", swarm(&["cas_losses"]), 2.0, 5.0),
    count(
        "swarm pipelined attempts discarded",
        swarm(&["pipeline_discarded"]),
        2.0,
        10.0,
    ),
    count(
        "swarm reads shed by servers",
        swarm(&["read_class_shed"]),
        2.0,
        50.0,
    ),
    count(
        "swarm max data pool in use",
        swarm(&["max_pool_in_use", "data"]),
        1.0,
        2.0,
    ),
    count(
        "swarm max land pool in use",
        swarm(&["max_pool_in_use", "land"]),
        1.0,
        1.0,
    ),
    count("land-bench CAS losses", land(&["cas_losses"]), 2.0, 5.0),
    count(
        "land-bench stalled agents",
        land(&["stalled_agents"]),
        1.0,
        0.0,
    ),
    // Work per land: the same on any machine, up to batching.
    above(
        "swarm statements per land",
        swarm(&["statements_per_land"]),
        1.3,
    ),
    above(
        "swarm land-pool statements per land",
        swarm(&["land_pool_statements_per_land"]),
        1.3,
    ),
    above(
        "land-bench statements per land",
        land(&["statements_per_land"]),
        1.3,
    ),
    above(
        "swarm write bytes per land",
        swarm(&["write_bytes_per_land"]),
        1.2,
    ),
    above(
        "swarm DB bytes per land",
        swarm(&["db_bytes_per_land"]),
        1.5,
    ),
    above("swarm batch mean", swarm(&["batch_mean"]), 2.0),
    // Wall time: wide bands, for collapses only.
    below("land-bench lands/s", land(&["lands_per_s"]), 0.7),
    above("land-bench land p99", land(&["p99_ms"]), 2.0),
    above("land-bench create p99", land(&["create_p99_ms"]), 2.0),
    below("swarm lands/s", swarm(&["lands_per_s"]), 0.7),
    above("swarm land p99", swarm(&["ops", "land", "p99_ms"]), 2.0),
    above("swarm create p99", swarm(&["ops", "create", "p99_ms"]), 2.0),
    above("swarm sync p99", swarm(&["ops", "sync", "p99_ms"]), 2.0),
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Verdict {
    Pass,
    Fail,
    /// The A runs differ from each other by as much as the check allows.
    Inconclusive,
    /// A value is missing (an older server does not report it).
    Skipped,
}

/// The verdict of one check over the A and B values, and the number shown
/// for it (the ratio, or B's smallest count against A's allowance).
fn judge(rule: Rule, a: &[f64], b: &[f64]) -> (Verdict, String) {
    if a.is_empty() || b.is_empty() {
        return (Verdict::Skipped, "-".into());
    }
    let max = |v: &[f64]| v.iter().copied().fold(f64::MIN, f64::max);
    let min = |v: &[f64]| v.iter().copied().fold(f64::MAX, f64::min);
    let mean = |v: &[f64]| v.iter().sum::<f64>() / v.len() as f64;
    match rule {
        Rule::Count { factor, slack } => {
            let allow = |base: f64| factor * base + slack;
            let shown = format!("B min {} vs allowed {}", min(b), allow(max(a)));
            if min(b) <= allow(max(a)) {
                (Verdict::Pass, shown)
            } else if max(a) > allow(min(a)) {
                (Verdict::Inconclusive, shown)
            } else {
                (Verdict::Fail, shown)
            }
        }
        Rule::RatioAbove(_) | Rule::RatioBelow(_) => {
            let ratio = |num: f64, den: f64| {
                if den == 0.0 {
                    if num == 0.0 {
                        1.0
                    } else {
                        f64::INFINITY
                    }
                } else {
                    num / den
                }
            };
            let r = ratio(mean(b), mean(a));
            // A/A in the direction that would breach: the larger A over the
            // smaller for "above", the smaller over the larger for "below".
            let (bad, aa) = match rule {
                Rule::RatioAbove(m) => (r > m, ratio(max(a), min(a)) > m),
                Rule::RatioBelow(m) => (r < m, ratio(min(a), max(a)) < m),
                Rule::Count { .. } => unreachable!(),
            };
            let shown = format!("{r:.2}x");
            match (bad, aa) {
                (false, _) => (Verdict::Pass, shown),
                (true, true) => (Verdict::Inconclusive, shown),
                (true, false) => (Verdict::Fail, shown),
            }
        }
    }
}

fn value(run: &Value, source: Source) -> Option<f64> {
    let mut v = match run {
        Value::Array(rows) => rows.first()?,
        v => v,
    };
    for key in source.path {
        v = v.get(key)?;
    }
    v.as_f64()
}

fn load(dir: &std::path::Path, file: &str) -> Result<Value> {
    let p = dir.join(file);
    let text = std::fs::read_to_string(&p).with_context(|| format!("reading {}", p.display()))?;
    serde_json::from_str(&text).with_context(|| format!("parsing {}", p.display()))
}

fn fmt_values(v: &[f64]) -> String {
    v.iter()
        .map(|x| {
            if x.fract() == 0.0 {
                format!("{x}")
            } else {
                format!("{x:.1}")
            }
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn main() -> Result<()> {
    let args = Args::parse();
    if args.a.len() < 2 {
        bail!("give at least two A runs, so their difference can be measured");
    }
    let read = |dirs: &[PathBuf], file: &str| -> Result<Vec<Value>> {
        dirs.iter().map(|d| load(d, file)).collect()
    };
    let (a_land, a_swarm) = (read(&args.a, "land.json")?, read(&args.a, "swarm.json")?);
    let (b_land, b_swarm) = (read(&args.b, "land.json")?, read(&args.b, "swarm.json")?);
    let mut out = String::new();
    let _ = writeln!(
        out,
        "| check | A | B | measure | verdict |\n|---|---|---|---|---|"
    );
    let mut failed = 0;
    let mut inconclusive = 0;
    for c in CHECKS {
        let (a_runs, b_runs) = match c.source.file {
            "land.json" => (&a_land, &b_land),
            _ => (&a_swarm, &b_swarm),
        };
        let a: Vec<f64> = a_runs.iter().filter_map(|r| value(r, c.source)).collect();
        let b: Vec<f64> = b_runs.iter().filter_map(|r| value(r, c.source)).collect();
        // A value only some runs report reads as missing everywhere.
        let (a, b) = if a.len() < a_runs.len() || b.len() < b_runs.len() {
            (Vec::new(), Vec::new())
        } else {
            (a, b)
        };
        let (verdict, shown) = judge(c.rule, &a, &b);
        match verdict {
            Verdict::Fail => failed += 1,
            Verdict::Inconclusive => inconclusive += 1,
            _ => {}
        }
        let _ = writeln!(
            out,
            "| {} | {} | {} | {shown} | {} |",
            c.name,
            fmt_values(&a),
            fmt_values(&b),
            match verdict {
                Verdict::Pass => "pass",
                Verdict::Fail => "**FAIL**",
                Verdict::Inconclusive => "inconclusive (A/A too far apart)",
                Verdict::Skipped => "skipped (not reported)",
            }
        );
    }
    let _ = writeln!(
        out,
        "\n{failed} failed, {inconclusive} inconclusive of {} checks.",
        CHECKS.len()
    );
    print!("{out}");
    if let Ok(path) = std::env::var("GITHUB_STEP_SUMMARY") {
        let mut summary = std::fs::read_to_string(&path).unwrap_or_default();
        summary.push_str("## Ratio bench\n\n");
        summary.push_str(&out);
        std::fs::write(&path, summary)?;
    }
    if failed > 0 {
        bail!("{failed} ratio-bench checks failed; see docs/PERFORMANCE.md, \"Ratio bench in CI\"");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const COUNT: Rule = Rule::Count {
        factor: 2.0,
        slack: 20.0,
    };

    #[test]
    fn counts_fail_only_when_every_b_run_exceeds_the_allowance() {
        assert_eq!(judge(COUNT, &[0.0, 3.0], &[25.0, 26.0]).0, Verdict::Pass);
        assert_eq!(judge(COUNT, &[0.0, 3.0], &[27.0, 900.0]).0, Verdict::Fail);
        assert_eq!(judge(COUNT, &[0.0, 3.0], &[0.0, 900.0]).0, Verdict::Pass);
        // The #253 swarm: no refusals before, thousands after.
        assert_eq!(
            judge(COUNT, &[0.0, 0.0], &[13_247.0, 12_000.0]).0,
            Verdict::Fail
        );
    }

    #[test]
    fn a_noisy_a_pair_makes_a_breach_inconclusive() {
        // The A runs themselves differ by more than the allowance.
        assert_eq!(
            judge(COUNT, &[0.0, 500.0], &[2000.0, 2100.0]).0,
            Verdict::Inconclusive
        );
        let r = Rule::RatioAbove(2.0);
        assert_eq!(judge(r, &[100.0, 110.0], &[300.0, 320.0]).0, Verdict::Fail);
        assert_eq!(
            judge(r, &[100.0, 250.0], &[600.0, 640.0]).0,
            Verdict::Inconclusive
        );
        let r = Rule::RatioBelow(0.7);
        assert_eq!(judge(r, &[100.0, 105.0], &[60.0, 62.0]).0, Verdict::Fail);
        assert_eq!(
            judge(r, &[100.0, 50.0], &[40.0, 42.0]).0,
            Verdict::Inconclusive
        );
        assert_eq!(judge(r, &[100.0, 105.0], &[90.0, 95.0]).0, Verdict::Pass);
    }

    #[test]
    fn missing_values_are_skipped_and_zero_bases_compare_sanely() {
        assert_eq!(
            judge(Rule::RatioAbove(1.3), &[], &[5.0, 5.0]).0,
            Verdict::Skipped
        );
        assert_eq!(
            judge(Rule::RatioAbove(1.3), &[0.0, 0.0], &[0.0, 0.0]).0,
            Verdict::Pass
        );
        assert_eq!(
            judge(Rule::RatioAbove(1.3), &[0.0, 0.0], &[1.0, 1.0]).0,
            Verdict::Fail
        );
    }

    #[test]
    fn values_are_read_from_rows_and_nested_keys() {
        let land: Value = serde_json::json!([{ "lands_per_s": 12.5 }]);
        assert_eq!(value(&land, super::land(&["lands_per_s"])), Some(12.5));
        let swarm: Value = serde_json::json!({ "ops": { "land": { "p99_ms": 40.0 } } });
        assert_eq!(
            value(&swarm, super::swarm(&["ops", "land", "p99_ms"])),
            Some(40.0)
        );
        assert_eq!(value(&swarm, super::swarm(&["missing"])), None);
    }
}
