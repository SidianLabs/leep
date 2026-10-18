//! Time filtered logs (`author`, `agent.*`) with and without the
//! `log-filter-v1` index against real stores, as measured in
//! docs/PERFORMANCE.md ("Filtered log").
//!
//! ```sh
//! cargo run --release -p leep-server --example log_filter_profile -- <meta url> <rev> [--synth N] [--reset] [--scan]
//! ```
//! `--synth N` first builds a synthetic history of N changesets with
//! Zipf-distributed authors, agent models and sessions, long-lived branches
//! that merge mainline back in and are merged into it, and a few rare
//! authors only near the start; `<rev>` is pointed at its head. `--reset`
//! deletes the index rows first (only on stores no server is using).
//! `--scan` also times the plain scan each query used before the index.
//! Each query runs on a fresh `Repo` (empty caches: "first") and again
//! ("repeat"). Prints the index's derivation time and size.

use std::collections::BTreeMap;
use std::time::Instant;

use leep_core::changeset::{EXTRA_AGENT_MODEL, EXTRA_AGENT_PROMPT, EXTRA_AGENT_SESSION};
use leep_core::{Changeset, ChangesetId, MetaStore};
use leep_derive::derived::{self, DerivedKind, LogFilterIndex, LOG_FILTER_NAME_KIND};
use leep_derive::{ExtraMatch, LogFilter, Repo};

struct Lcg(u64);

impl Lcg {
    fn below(&mut self, n: usize) -> usize {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        ((self.0 >> 33) as usize) % n
    }

    /// Zipf with exponent 1 over `0..n`, by inverting the harmonic CDF.
    fn zipf(&mut self, n: usize) -> usize {
        let h = (n as f64).ln() + 0.5772;
        let u = (self.below(1 << 30) as f64 + 0.5) / (1u64 << 30) as f64;
        ((u * h - 0.5772).exp() as usize).min(n - 1)
    }
}

const BRANCHES: usize = 24;

fn author(i: usize) -> String {
    format!("Developer {i} <dev{i}@kernel.example>")
}

/// Mainline (branch 0) and long-lived subsystem branches. Most changesets
/// land on a subsystem branch; mainline merges a branch every so often, and
/// branches merge mainline back. Authors are Zipf over `authors`, a third of
/// changesets carry agent metadata (models Zipf over 8, sessions of about 20
/// changesets each), and three rare authors appear only in the first 2%.
async fn build(repo: &Repo, n: usize, authors: usize) -> ChangesetId {
    let mut rng = Lcg(0x10_6f11);
    let message = "x".repeat(400);
    let mut heads: Vec<Option<ChangesetId>> = vec![None; BRANCHES];
    let mut last = None;
    let mut session = 0usize;
    let started = Instant::now();
    for i in 0..n {
        let b = if heads[0].is_none() {
            0
        } else {
            rng.below(BRANCHES)
        };
        let mut parents: Vec<ChangesetId> = heads[b].or(heads[0]).into_iter().collect();
        let roll = rng.below(1000);
        if b == 0 && roll < 300 {
            // Mainline merges a subsystem branch.
            let o = 1 + rng.below(BRANCHES - 1);
            if let Some(h) = heads[o] {
                if !parents.contains(&h) {
                    parents.push(h);
                }
            }
        } else if b != 0 && roll < 15 {
            // A branch merges mainline back.
            if let Some(h) = heads[0] {
                if !parents.contains(&h) {
                    parents.push(h);
                }
            }
        }
        let mut extra = BTreeMap::new();
        if rng.below(3) == 0 {
            if rng.below(20) == 0 {
                session += 1;
            }
            extra.insert(
                EXTRA_AGENT_MODEL.to_string(),
                format!("model-{}", rng.zipf(8)),
            );
            extra.insert(
                EXTRA_AGENT_SESSION.to_string(),
                format!("session-{session}"),
            );
            extra.insert(EXTRA_AGENT_PROMPT.to_string(), format!("prompt {i}"));
        }
        let who = if i < n / 50 && i % (n / 200).max(1) == 7 {
            format!("Rare Person{} <rare{}@example.org>", i % 3, i % 3)
        } else {
            author(rng.zipf(authors))
        };
        let cs = Changeset {
            parents,
            author: who,
            timestamp: i as i64,
            message: message.clone(),
            changes: BTreeMap::new(),
            extra,
        };
        let id = repo.create_changeset(cs).await.unwrap();
        heads[b] = Some(id);
        last = Some(id);
        if (i + 1) % 20_000 == 0 {
            println!(
                "  built {} changesets, {:.0}/s",
                i + 1,
                (i + 1) as f64 / started.elapsed().as_secs_f64()
            );
        }
    }
    // End on a mainline merge of every branch, so the head reaches all.
    let mut parents = vec![heads[0].unwrap_or(last.unwrap())];
    for h in heads.iter().skip(1).flatten() {
        if !parents.contains(h) {
            parents.push(*h);
        }
    }
    repo.create_changeset(Changeset {
        parents,
        author: author(0),
        timestamp: n as i64,
        message: "merge all".into(),
        changes: BTreeMap::new(),
        extra: BTreeMap::new(),
    })
    .await
    .unwrap()
}

fn filter(author: Option<&str>, extra: &[&str]) -> LogFilter {
    LogFilter {
        author: author.map(str::to_string),
        extra: extra
            .iter()
            .map(|e| ExtraMatch::parse(e).unwrap())
            .collect(),
    }
}

async fn kind_bytes(meta: &dyn MetaStore, kind: &str) -> (u64, u64) {
    let (mut rows, mut bytes) = (0u64, 0u64);
    let mut after = None;
    loop {
        let page = meta.list_derived(kind, after, 10_000).await.unwrap();
        let Some(last) = page.last() else { break };
        after = Some(last.0);
        rows += page.len() as u64;
        bytes += page.iter().map(|(_, v)| v.len() as u64).sum::<u64>();
    }
    (rows, bytes)
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let usage =
        "usage: log_filter_profile <meta url> <rev> [--synth N] [--authors K] [--reset] [--scan]";
    let meta_url = args.first().expect(usage);
    let rev = args.get(1).expect(usage);
    let flag = |name: &str| {
        args.iter()
            .position(|a| a == name)
            .map(|i| args.get(i + 1).expect(usage).clone())
    };
    let blobs = leep_store::open_blob_store("mem://").unwrap();
    let meta = leep_meta::open_meta_store(meta_url).await.unwrap();
    if let Some(n) = flag("--synth") {
        let n: usize = n.parse().expect(usage);
        let authors: usize = flag("--authors").map_or(n / 50, |a| a.parse().expect(usage));
        let t = Instant::now();
        let head = build(&Repo::new(blobs.clone(), meta.clone()), n, authors).await;
        let old = meta.get_ref(rev).await.unwrap();
        assert!(meta.cas_ref(rev, old, Some(head)).await.unwrap());
        println!(
            "built {n} changesets ({authors} authors) in {:.1} s, head {head}",
            t.elapsed().as_secs_f64()
        );
    }
    if args.iter().any(|a| a == "--reset") {
        let a = meta
            .delete_derived_kind(LogFilterIndex::NAME)
            .await
            .unwrap();
        let b = meta
            .delete_derived_kind(LOG_FILTER_NAME_KIND)
            .await
            .unwrap();
        println!("deleted {a} index rows, {b} name rows");
    }
    let repo = Repo::new(blobs.clone(), meta.clone());
    let head = repo.resolve(rev).await.unwrap();
    let t = Instant::now();
    derived::derive::<LogFilterIndex>(&repo, head)
        .await
        .unwrap();
    println!("index derived in {:.1} s", t.elapsed().as_secs_f64());
    let (rows, bytes) = kind_bytes(meta.as_ref(), LogFilterIndex::NAME).await;
    let (nrows, nbytes) = kind_bytes(meta.as_ref(), LOG_FILTER_NAME_KIND).await;
    println!(
        "{} rows, {bytes} bytes ({:.1} bytes/changeset); {nrows} name rows, {nbytes} bytes",
        rows,
        bytes as f64 / rows.max(1) as f64
    );

    let queries: Vec<(&str, LogFilter, usize)> = vec![
        (
            "rare author (oldest 2%)",
            filter(Some("Rare Person1"), &[]),
            100,
        ),
        (
            "rare author, exact name",
            filter(Some("rare person2 <rare2@example.org>"), &[]),
            100,
        ),
        (
            "common author (Developer 0, ~6.5%)",
            filter(Some("Developer 0 <"), &[]),
            100,
        ),
        (
            "mid author (Developer 50, ~0.2%)",
            filter(Some("Developer 50 <"), &[]),
            100,
        ),
        (
            "tail author (Developer 2000)",
            filter(Some("Developer 2000 <"), &[]),
            100,
        ),
        ("author nobody", filter(Some("nobody"), &[]), 10),
        (
            "--agent (agent.model present)",
            filter(None, &[EXTRA_AGENT_MODEL]),
            100,
        ),
        (
            "--agent-model common",
            filter(None, &["agent.model:model-0"]),
            100,
        ),
        (
            "--agent-model rare",
            filter(None, &["agent.model:model-7"]),
            100,
        ),
        (
            "--agent-session one session",
            filter(None, &["agent.session:session-17"]),
            100,
        ),
        (
            "Developer 3 and model-1",
            filter(Some("Developer 3 <"), &["agent.model:model-1"]),
            100,
        ),
    ];
    let scan = args.iter().any(|a| a == "--scan");
    println!("| query | matches | first | repeat | scan before |");
    println!("|---|---|---|---|---|");
    for (name, f, limit) in &queries {
        let cold = Repo::new(blobs.clone(), meta.clone());
        let t = Instant::now();
        let got = cold.log_filtered(head, None, *limit, f).await.unwrap();
        let first = t.elapsed();
        let t = Instant::now();
        let again = cold.log_filtered(head, None, *limit, f).await.unwrap();
        let repeat = t.elapsed();
        assert_eq!(got, again);
        let before = if scan {
            let plain = Repo::new(blobs.clone(), meta.clone());
            let t = Instant::now();
            let want = plain.log_scan(head, None, *limit, f).await.unwrap();
            let took = t.elapsed();
            assert_eq!(got, want, "{name}");
            format!("{:.1} ms", took.as_secs_f64() * 1e3)
        } else {
            "-".into()
        };
        println!(
            "| {name}, limit {limit} | {} | {:.1} ms | {:.1} ms | {before} |",
            got.len(),
            first.as_secs_f64() * 1e3,
            repeat.as_secs_f64() * 1e3
        );
    }
}
