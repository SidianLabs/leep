//! Land throughput under contention: N agents, each in a loop, read `main`,
//! upload blobs, create a changeset on that head and land it onto `main`.
//! Reports lands per second, upload, create and land latency and the server's
//! retry counters for each N, and optionally times a missing-blobs query.
//! See "Concurrent lands on one ref" in docs/PERFORMANCE.md.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use clap::Parser;
use leep_core::api::{
    CasRefRequest, CreateChangesetResponse, LandRequest, LandResponse, MissingBlobsRequest,
    MissingBlobsResponse, RefResponse,
};
use leep_core::{Changeset, ChangesetId, FileChange, FileKind, Hash, RepoPath};
use reqwest::Client;

#[derive(Parser)]
#[command(about = "Concurrent lands onto one ref against a running leep-server")]
struct Args {
    /// Server base URL.
    #[arg(long, default_value = "http://127.0.0.1:7070")]
    server: String,
    /// The server's `--metrics-listen` base URL, to read its land counters.
    #[arg(long)]
    metrics: Option<String>,
    /// Agent counts to run, one after another.
    #[arg(long, value_delimiter = ',', default_value = "1,4,16,64")]
    agents: Vec<usize>,
    /// Seconds each agent count runs (after a one-second warm-up).
    #[arg(long, default_value_t = 20)]
    duration: u64,
    /// Ref the agents land onto; created when absent.
    #[arg(long, default_value = "main")]
    target: String,
    /// Files each changeset writes, each its own uploaded blob.
    #[arg(long, default_value_t = 1)]
    files: usize,
    /// Bytes per uploaded blob (at least the edit line).
    #[arg(long, default_value_t = 0)]
    blob_bytes: usize,
    /// Before the land rounds, ask `/v1/blobs/missing` about this many hashes
    /// (none stored) and report the time; 0 skips it.
    #[arg(long, default_value_t = 0)]
    missing: usize,
    /// Hashes per missing-blobs request; 0 sends them all in one.
    #[arg(long, default_value_t = 0)]
    missing_chunk: usize,
}

struct Http {
    client: Client,
    base: String,
}

impl Http {
    async fn json<T: serde::de::DeserializeOwned>(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<Vec<u8>>,
    ) -> Result<(u16, Option<T>)> {
        let mut rq = self.client.request(method, format!("{}{path}", self.base));
        if let Some(b) = body {
            rq = rq.header("content-type", "application/json").body(b);
        }
        let res = rq.send().await?;
        let status = res.status().as_u16();
        let bytes = res.bytes().await?;
        if status != 200 {
            return Ok((status, None));
        }
        Ok((status, Some(serde_json::from_slice(&bytes)?)))
    }

    async fn put_blob(&self, data: &[u8]) -> Result<Hash> {
        let h = Hash::of_blob(data);
        let res = self
            .client
            .put(format!("{}/v1/blobs/{h}", self.base))
            .body(data.to_vec())
            .send()
            .await?;
        if !res.status().is_success() {
            bail!("blob put: {}", res.status());
        }
        Ok(h)
    }

    async fn head(&self, target: &str) -> Result<Option<ChangesetId>> {
        let (_, r): (_, Option<RefResponse>) = self
            .json(reqwest::Method::GET, &format!("/v1/refs/{target}"), None)
            .await?;
        Ok(r.and_then(|r| r.target))
    }

    async fn create(&self, cs: &Changeset) -> Result<ChangesetId> {
        let (status, r): (_, Option<CreateChangesetResponse>) = self
            .json(
                reqwest::Method::POST,
                "/v1/changesets",
                Some(serde_json::to_vec(cs)?),
            )
            .await?;
        Ok(r.with_context(|| format!("create changeset: {status}"))?.id)
    }
}

fn modify(data: &[u8]) -> FileChange {
    FileChange::Modify {
        content: Hash::of_blob(data),
        kind: FileKind::Regular,
        size: data.len() as u64,
    }
}

#[derive(Default)]
struct Totals {
    latencies_us: Vec<u64>,
    put_us: Vec<u64>,
    create_us: Vec<u64>,
    rebased: u64,
    retried_409: u64,
    attempts_above_one: u64,
}

#[allow(clippy::too_many_arguments)]
async fn agent(
    http: Arc<Http>,
    target: String,
    files: usize,
    blob_bytes: usize,
    who: usize,
    stop: Arc<AtomicBool>,
    measuring: Arc<AtomicBool>,
    totals: Arc<Mutex<Totals>>,
) -> Result<()> {
    let mut seq = 0u64;
    while !stop.load(Ordering::Relaxed) {
        seq += 1;
        let head = http.head(&target).await?;
        let mut changes = BTreeMap::new();
        let mut puts = Vec::with_capacity(files);
        for f in 0..files {
            let mut body = format!("agent {who} edit {seq} file {f} {:?}\n", Instant::now());
            while body.len() < blob_bytes {
                body.push_str("0123456789abcdef");
            }
            let t = Instant::now();
            http.put_blob(body.as_bytes()).await?;
            puts.push(t.elapsed().as_micros() as u64);
            let name = if files == 1 {
                "f".to_string()
            } else {
                format!("f{f}")
            };
            changes.insert(
                RepoPath::new(format!("agents/a{who}/{name}").as_str())?,
                modify(body.as_bytes()),
            );
        }
        let cs = Changeset {
            parents: head.into_iter().collect(),
            author: format!("bench-agent-{who}"),
            timestamp: 1_700_000_000 + seq as i64,
            message: format!("agent {who} edit {seq}"),
            changes,
            extra: BTreeMap::new(),
        };
        let tc = Instant::now();
        let id = http.create(&cs).await?;
        let create_us = tc.elapsed().as_micros() as u64;
        let req = serde_json::to_vec(&LandRequest {
            changeset: id,
            target_ref: target.clone(),
            create: None,
        })?;
        let t0 = Instant::now();
        let mut retried = 0;
        let resp = loop {
            let (status, r): (_, Option<LandResponse>) = http
                .json(reqwest::Method::POST, "/v1/land", Some(req.clone()))
                .await?;
            match (status, r) {
                (200, Some(r)) => break r,
                (409, _) => {
                    retried += 1;
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                (s, _) => bail!("land answered {s}"),
            }
        };
        let dt = t0.elapsed();
        let LandResponse::Landed {
            rebased, attempts, ..
        } = resp
        else {
            bail!("unexpected conflicts: agents edit disjoint files");
        };
        if measuring.load(Ordering::Relaxed) && !stop.load(Ordering::Relaxed) {
            let mut t = totals.lock().unwrap();
            t.latencies_us.push(dt.as_micros() as u64);
            t.put_us.extend(puts);
            t.create_us.push(create_us);
            t.rebased += u64::from(rebased);
            t.retried_409 += retried;
            t.attempts_above_one += u64::from(attempts > 1);
        }
    }
    Ok(())
}

/// Sum of every sample of `name` (any labels) in a Prometheus text page.
fn metric(page: &str, name: &str) -> f64 {
    page.lines()
        .filter(|l| !l.starts_with('#'))
        .filter(|l| {
            l.strip_prefix(name)
                .is_some_and(|r| r.starts_with(' ') || r.starts_with('{'))
        })
        .filter_map(|l| l.rsplit(' ').next()?.parse::<f64>().ok())
        .sum()
}

async fn scrape(client: &Client, metrics: &Option<String>) -> String {
    let Some(m) = metrics else {
        return String::new();
    };
    match client.get(format!("{m}/metrics")).send().await {
        Ok(r) => r.text().await.unwrap_or_default(),
        Err(_) => String::new(),
    }
}

fn pct(sorted: &[u64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let i = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[i] as f64 / 1000.0
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let client = Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(120))
        .tcp_nodelay(true)
        .pool_max_idle_per_host(256)
        .build()?;
    let http = Arc::new(Http {
        client: client.clone(),
        base: args.server.trim_end_matches('/').to_string(),
    });

    if args.missing > 0 {
        let hashes: Vec<Hash> = (0..args.missing)
            .map(|i| Hash::of_blob(format!("never stored {i}").as_bytes()))
            .collect();
        let chunk = if args.missing_chunk == 0 {
            hashes.len()
        } else {
            args.missing_chunk
        };
        for round in 0..3 {
            let t = Instant::now();
            let mut missing = 0;
            for part in hashes.chunks(chunk) {
                let (status, r): (_, Option<MissingBlobsResponse>) = http
                    .json(
                        reqwest::Method::POST,
                        "/v1/blobs/missing",
                        Some(serde_json::to_vec(&MissingBlobsRequest {
                            hashes: part.to_vec(),
                        })?),
                    )
                    .await?;
                missing += r
                    .with_context(|| format!("missing blobs: {status}"))?
                    .missing
                    .len();
            }
            println!(
                "missing-blobs round {round}: {} hashes in {} requests of {chunk}: {:.1} ms ({missing} missing)",
                hashes.len(),
                hashes.len().div_ceil(chunk),
                t.elapsed().as_secs_f64() * 1000.0
            );
        }
    }

    if http.head(&args.target).await?.is_none() {
        let mut changes = BTreeMap::new();
        for d in 0..10 {
            for f in 0..20 {
                let data = format!("file {d}/{f}\n").into_bytes();
                http.put_blob(&data).await?;
                changes.insert(RepoPath::new(format!("d{d}/f{f}").as_str())?, modify(&data));
            }
        }
        let root = http
            .create(&Changeset {
                parents: vec![],
                author: "bench".into(),
                timestamp: 1_700_000_000,
                message: "bench root".into(),
                changes,
                extra: BTreeMap::new(),
            })
            .await?;
        let (status, _): (_, Option<serde_json::Value>) = http
            .json(
                reqwest::Method::POST,
                &format!("/v1/refs/{}", args.target),
                Some(serde_json::to_vec(&CasRefRequest {
                    expected: None,
                    new: Some(root),
                    operation: None,
                    message: None,
                })?),
            )
            .await?;
        if status != 200 {
            bail!("creating {}: {status}", args.target);
        }
    }

    println!(
        "| agents | lands | lands/s | p50 ms | p99 ms | max ms | blob puts/s | put p50/p99 ms | create p50/p99 ms | replayed | 409 retries | CAS losses | attempts>1 | lock wait mean ms | batch mean |"
    );
    println!("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|");
    for &n in &args.agents {
        let stop = Arc::new(AtomicBool::new(false));
        let measuring = Arc::new(AtomicBool::new(false));
        let totals = Arc::new(Mutex::new(Totals::default()));
        let tasks: Vec<_> = (0..n)
            .map(|i| {
                tokio::spawn(agent(
                    Arc::clone(&http),
                    args.target.clone(),
                    args.files.max(1),
                    args.blob_bytes,
                    i,
                    Arc::clone(&stop),
                    Arc::clone(&measuring),
                    Arc::clone(&totals),
                ))
            })
            .collect();
        tokio::time::sleep(Duration::from_secs(1)).await;
        let before = scrape(&client, &args.metrics).await;
        measuring.store(true, Ordering::Relaxed);
        let started = Instant::now();
        tokio::time::sleep(Duration::from_secs(args.duration)).await;
        stop.store(true, Ordering::Relaxed);
        let elapsed = started.elapsed().as_secs_f64();
        let after = scrape(&client, &args.metrics).await;
        for t in tasks {
            t.await??;
        }
        let d = |name: &str| metric(&after, name) - metric(&before, name);
        let mut t = totals.lock().unwrap();
        t.latencies_us.sort_unstable();
        t.put_us.sort_unstable();
        t.create_us.sort_unstable();
        let lands = t.latencies_us.len();
        let waits = d("leep_land_lock_wait_seconds_count");
        let batches = d("leep_land_batch_size_count");
        println!(
            "| {n} | {lands} | {:.1} | {:.1} | {:.1} | {:.1} | {:.0} | {:.1} / {:.1} | {:.1} / {:.1} | {} | {} | {} | {} | {} | {} |",
            lands as f64 / elapsed,
            pct(&t.latencies_us, 0.5),
            pct(&t.latencies_us, 0.99),
            pct(&t.latencies_us, 1.0),
            t.put_us.len() as f64 / elapsed,
            pct(&t.put_us, 0.5),
            pct(&t.put_us, 0.99),
            pct(&t.create_us, 0.5),
            pct(&t.create_us, 0.99),
            t.rebased,
            t.retried_409,
            d("leep_land_cas_losses_total"),
            t.attempts_above_one,
            if waits > 0.0 {
                format!(
                    "{:.1}",
                    1000.0 * d("leep_land_lock_wait_seconds_sum") / waits
                )
            } else {
                "-".into()
            },
            if batches > 0.0 {
                format!("{:.2}", d("leep_land_batch_size_sum") / batches)
            } else {
                "-".into()
            },
        );
    }
    Ok(())
}
