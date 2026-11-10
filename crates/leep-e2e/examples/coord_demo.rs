//! The phase 1 coordination demo with its narrated transcript: twenty
//! scripted agents on one repository through the real `leep-server`,
//! `leep` and `leep-mcp` binaries (build them first). Run it with
//! `scripts/demo-coordination.sh`; docs/DEMO-COORDINATION.md says what it
//! shows.
//!
//! `cargo run -p leep-e2e --example coord_demo -- [--out FILE] [--postgres]`
//! prints the transcript as Markdown, writes it to FILE as well, and exits
//! 1 when a claim did not hold. `--postgres` keeps metadata in a throwaway
//! database under `LEEP_TEST_POSTGRES_URL` instead of SQLite.

#[path = "../tests/common/mod.rs"]
mod common;
#[allow(dead_code)] // the test asserts; this prints
#[path = "../tests/coord_demo/scenario.rs"]
mod scenario;

fn main() {
    let mut out = None;
    let mut postgres = false;
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--out" => out = Some(args.next().expect("--out FILE")),
            "--postgres" => postgres = true,
            other => {
                eprintln!("usage: coord_demo [--out FILE] [--postgres] (unknown {other:?})");
                std::process::exit(2);
            }
        }
    }
    let tmp = tempfile::tempdir().expect("temporary directory");
    let data = tmp.path().join("data");
    let db = if postgres {
        let Some(db) = common::PgDb::create("leep_demo") else {
            eprintln!("--postgres needs LEEP_TEST_POSTGRES_URL");
            std::process::exit(2);
        };
        Some(db)
    } else {
        None
    };
    let stores = match &db {
        Some(db) => common::Stores::with_meta(&data, db.url.clone()),
        None => common::Stores::sqlite(&data),
    };
    let n = scenario::Narrator::new(true);
    let report = scenario::run(&stores, tmp.path(), &n);
    if let Some(path) = out {
        std::fs::write(&path, &report.transcript).expect("write the transcript");
        eprintln!("transcript written to {path}");
    }
    let failed = report.claims.iter().filter(|c| !c.pass).count();
    if failed > 0 {
        eprintln!("{failed} claim(s) did not hold");
        std::process::exit(1);
    }
}
