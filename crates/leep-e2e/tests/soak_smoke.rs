//! An eight-second run of the soak harness (ADR 0022), so `leep-soak` keeps
//! working as the server changes. The real soaks run for minutes to hours
//! from `.github/workflows/soak.yml` or by hand (docs/OPERATIONS.md).
//!
//! It uses Postgres when `LEEP_TEST_POSTGRES_URL` is set and SQLite
//! otherwise: the harness has no Postgres-only logic outside the database
//! it creates, and an SQLite run still exercises the agents, the invariant
//! checks, GC and the report, so developers without a database do not lose
//! the coverage. Run `cargo build --workspace` first.

mod common;

use std::process::Command;

use common::*;

#[test]
fn soak_smoke() {
    let _guard = serial();
    let out = tempfile::tempdir().unwrap();
    let mut cmd = Command::new(bin("leep-soak"));
    cmd.args([
        "--duration",
        "8s",
        "--agents",
        "5",
        "--cli-agents",
        "1",
        "--files",
        "300",
        // A short request timeout keeps the GC fence (derived from it) short,
        // and a short interval puts a whole GC inside the run.
        "--request-timeout",
        "2s",
        "--gc-interval",
        "2s",
        // And the changeset sweep after each blob GC (ADR 0059).
        "--sweep-changesets",
        "--derive-interval",
        "4s",
        // Backups beside the swarm, and the final backup, restore and verify.
        "--backup-interval",
        "3s",
        "--check-interval",
        "3s",
        "--sample-interval",
        "2s",
        "--think",
        "100ms",
        "--chaos",
        "--chaos-restarts",
        "1",
        "--bin-dir",
    ])
    .arg(bin("leep-server").parent().unwrap())
    .arg("--out")
    .arg(out.path());
    // Resolve the database here, so the run does not depend on stray
    // variables in the developer's environment.
    cmd.env_remove("LEEP_SOAK_POSTGRES_URL");
    if let Ok(url) = std::env::var("LEEP_TEST_POSTGRES_URL") {
        cmd.arg("--postgres-url").arg(url);
    } else {
        eprintln!("soak smoke: LEEP_TEST_POSTGRES_URL not set, using SQLite");
        cmd.arg("--sqlite");
    }
    let started = std::time::Instant::now();
    let output = cmd.output().expect("spawn leep-soak");
    let report = std::fs::read_to_string(out.path().join("report.json")).unwrap_or_default();
    assert!(
        output.status.success(),
        "leep-soak failed ({}) after {:?}\n--- stdout\n{}\n--- stderr\n{}\n--- server log\n{}",
        output.status,
        started.elapsed(),
        stdout(&output),
        stderr(&output),
        std::fs::read_to_string(out.path().join("server.log")).unwrap_or_default(),
    );
    let v: serde_json::Value = serde_json::from_str(&report).expect("report.json");
    assert_eq!(v["result"], "pass", "{}", stdout(&output));
    assert!(
        v["throughput"]["lands_acked"].as_u64().unwrap_or(0) > 0,
        "no land was acknowledged"
    );
    assert!(
        v["workload"]["backup_runs"].as_u64().unwrap_or(0) >= 2,
        "backups did not run: {}",
        v["workload"]
    );
    assert!(
        v["workload"]["restore_changesets"].as_u64().unwrap_or(0) > 0,
        "the final restore drill did not run: {}",
        v["workload"]
    );
    assert!(
        v["invariants"]["checks"]["ticks"].as_u64().unwrap_or(0) >= 2,
        "the invariant checks did not run"
    );
}
