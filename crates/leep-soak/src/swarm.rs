//! A synthetic many-agent workload against one or more running servers:
//! thousands of cheap async agents that think, sync their working copy, read
//! (log, files, blame, search, diff), upload blobs, create changesets on a
//! possibly stale base and land them, a configurable share of them on a few
//! shared hot files. Reports lands per second, p50/p99 per operation, 409s,
//! conflicts, stalled agents, lands per server, the warmer's lag behind the
//! head and database bytes per land, and checks history at the end: every
//! land writes a unique marker, and a marker found twice is a double land
//! (zero is asserted when land ids are on). See "Many-agent workload" in
//! docs/PERFORMANCE.md.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use clap::Parser;
use leep_core::api::{
    CasRefRequest, ChangesetInfo, CreateChangesetResponse, LandId, LandRequest, LandResponse,
    LogResponse, RefResponse,
};
use leep_core::{Changeset, ChangesetId, FileChange, FileKind, Hash, RepoPath};
use reqwest::{Client, Method, StatusCode};
use serde::Serialize;

#[derive(Parser, Clone)]
#[command(about = "Many simulated agents against running leep-servers")]
struct Args {
    /// Server base URLs, comma separated; agent `i` talks to server
    /// `i % count`. Instances must share the metadata and blob stores.
    #[arg(long, value_delimiter = ',', default_value = "http://127.0.0.1:7070")]
    server: Vec<String>,
    /// The servers' `--metrics-listen` base URLs, to read their land counters.
    #[arg(long, value_delimiter = ',')]
    metrics: Vec<String>,
    /// Simulated agents.
    #[arg(long, default_value_t = 500)]
    agents: usize,
    /// Seconds measured, after the ramp.
    #[arg(long, default_value_t = 60)]
    duration: u64,
    /// Seconds over which agents start, evenly; measuring starts after it.
    #[arg(long, default_value_t = 10)]
    ramp: u64,
    /// Mean think time before each operation, in ms (exponentially
    /// distributed); 0 is a burst with no pauses.
    #[arg(long, default_value_t = 2000)]
    think_ms: u64,
    /// Reads per land cycle.
    #[arg(long, default_value_t = 4)]
    reads: usize,
    /// Weights of the reads: `log=3,files=3,blame=1,search=1,diff=2`.
    #[arg(long, default_value = "log=3,files=3,blame=1,search=1,diff=2")]
    read_mix: String,
    /// Sync the working copy (read the ref and list the agent's files at the
    /// head) at the start of every cycle.
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    sync: bool,
    /// Files each change writes, each an uploaded blob.
    #[arg(long, default_value_t = 2)]
    files: usize,
    /// Bytes per uploaded blob.
    #[arg(long, default_value_t = 512)]
    blob_bytes: usize,
    /// Shared hot files.
    #[arg(long, default_value_t = 8)]
    hot_files: usize,
    /// Share of written files that are hot files (0 to 1).
    #[arg(long, default_value_t = 0.02)]
    hot_fraction: f64,
    /// Largest stale-base distance: a change is made on a head up to this
    /// many observed heads behind the newest (uniform), so it is replayed.
    #[arg(long, default_value_t = 4)]
    stale: usize,
    /// Time-based stale base, in ms: a change is made on the head this run
    /// observed a uniform 0..=stale-ms ago, modelling an agent that synced,
    /// worked for minutes, then landed. Overrides `--stale` when set.
    #[arg(long)]
    stale_ms: Option<u64>,
    /// Drop search from the read mix.
    #[arg(long)]
    no_search: bool,
    /// After a land answered `conflicts`, resolve (keep this agent's
    /// version of every file) on the conflicting head and land again, up to
    /// this many times per logical land. 0 gives up at the first conflict.
    #[arg(long, default_value_t = 0)]
    reland_max: u32,
    /// Send a land id with every land (ADR 0057) and resend under it after
    /// errors; `false` resends without one, as old clients do.
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    land_id: bool,
    /// Ref to land on; created with the hot files when absent.
    #[arg(long, default_value = "main")]
    target: String,
    /// Per-request timeout in seconds. A land that times out is resent.
    #[arg(long, default_value_t = 30)]
    request_timeout: u64,
    /// Postgres URL of the servers' metadata store: enables database bytes
    /// per land and the warmer lag (head generation minus the newest one
    /// with its path-history and log-filter rows).
    #[arg(long)]
    database_url: Option<String>,
    /// Seconds between warmer-lag samples.
    #[arg(long, default_value_t = 2)]
    lag_every: u64,
    /// Skip the end-of-run history check.
    #[arg(long)]
    no_history_check: bool,
    /// Write the results as JSON here as well.
    #[arg(long)]
    json: Option<std::path::PathBuf>,
    /// Seed of the agents' choices.
    #[arg(long, default_value_t = 1)]
    seed: u64,
}

/// xorshift64*: cheap, seeded, good enough for workload choices.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Rng {
        Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n.max(1) as u64) as usize
    }
    fn unit(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64
    }
    /// Exponential with mean `mean_ms`.
    fn think(&mut self, mean_ms: u64) -> Duration {
        if mean_ms == 0 {
            return Duration::ZERO;
        }
        let x = -(1.0 - self.unit()).ln() * mean_ms as f64;
        Duration::from_micros((x * 1000.0).min(60_000_000.0) as u64)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
enum Read {
    Log,
    Files,
    Blame,
    Search,
    Diff,
}

fn parse_mix(s: &str) -> Result<Vec<(Read, u32)>> {
    let mut out = Vec::new();
    for part in s.split(',').filter(|p| !p.is_empty()) {
        let (k, v) = part
            .split_once('=')
            .context("read mix is kind=weight,...")?;
        let kind = match k.trim() {
            "log" => Read::Log,
            "files" => Read::Files,
            "blame" => Read::Blame,
            "search" => Read::Search,
            "diff" => Read::Diff,
            other => bail!("unknown read {other:?}"),
        };
        out.push((kind, v.trim().parse()?));
    }
    Ok(out)
}

/// Heads seen lately with when they were first seen, newest last; bases are
/// picked from here. Large enough to hold minutes of heads at hundreds of
/// lands a second, for `--stale-ms`.
struct Heads {
    ring: Mutex<VecDeque<(Instant, ChangesetId)>>,
}

const HEADS_KEPT: usize = 1 << 17;

impl Heads {
    fn push(&self, id: ChangesetId) {
        let mut r = self.ring.lock().unwrap();
        if r.back().map(|e| e.1) != Some(id) {
            r.push_back((Instant::now(), id));
            if r.len() > HEADS_KEPT {
                r.pop_front();
            }
        }
    }
    fn newest(&self) -> Option<ChangesetId> {
        self.ring.lock().unwrap().back().map(|e| e.1)
    }
    fn back(&self, d: usize) -> Option<ChangesetId> {
        let r = self.ring.lock().unwrap();
        let i = r.len().checked_sub(1 + d.min(r.len().saturating_sub(1)))?;
        r.get(i).map(|e| e.1)
    }
    /// The newest head first seen at least `age` ago, else the oldest kept.
    fn aged(&self, age: Duration) -> Option<ChangesetId> {
        let r = self.ring.lock().unwrap();
        let Some(cutoff) = Instant::now().checked_sub(age) else {
            return r.front().map(|e| e.1);
        };
        let i = r.partition_point(|e| e.0 <= cutoff);
        r.get(i.saturating_sub(1)).map(|e| e.1)
    }
    /// The newest `n` heads, newest first.
    fn newest_n(&self, n: usize) -> Vec<ChangesetId> {
        let r = self.ring.lock().unwrap();
        r.iter().rev().take(n).map(|e| e.1).collect()
    }
}

struct Shared {
    args: Args,
    run: String,
    servers: Vec<String>,
    client: Client,
    heads: Heads,
    mix: Vec<(Read, u32)>,
    stop: AtomicBool,
    measuring: AtomicBool,
    /// Files of the base tree, for blame.
    seeded_files: Vec<String>,
    /// Generation of the head the run started from; the history check stops
    /// there, so a run on a long history does not walk all of it.
    base_generation: u64,
    land_ids_sent: AtomicU64,
}

/// What one agent saw. Merged at the end, so agents never contend.
#[derive(Default)]
struct Tally {
    ops: BTreeMap<&'static str, Vec<u64>>,
    errors: BTreeMap<String, u64>,
    lands_in_window: u64,
    landed_after: u64,
    conflicts: u64,
    status_409: u64,
    resends: u64,
    rebased: u64,
    /// Sent a land inside the window.
    sent_in_window: bool,
    /// Had a land answered (landed or conflicts) inside the window.
    answered_in_window: bool,
    /// Markers of lands answered `Landed`.
    landed: Vec<String>,
    /// Logical lands (a change, through its re-lands after conflicts) whose
    /// first land was sent inside the window, and what they took.
    logical: LogicalTally,
}

#[derive(Default)]
struct LogicalTally {
    started: u64,
    landed: u64,
    /// Ended on a conflict after `--reland-max` re-lands, or at the stop.
    gave_up: u64,
    /// Ended without an answer (create failed, land refused or gave up).
    failed: u64,
    conflicts: u64,
    relands: u64,
    resends: u64,
    status_409: u64,
    /// Microseconds from the first land request to the `landed` answer.
    time_to_landed: Vec<u64>,
}

impl Tally {
    fn time(&mut self, measuring: bool, op: &'static str, t: Instant) {
        if measuring {
            self.ops
                .entry(op)
                .or_default()
                .push(t.elapsed().as_micros() as u64);
        }
    }
    fn error(&mut self, what: impl Into<String>) {
        *self.errors.entry(what.into()).or_default() += 1;
    }
}

enum Answer<T> {
    Ok(T),
    Status(u16),
    /// Timeout or a lost connection: the request may have applied.
    Lost(String),
}

impl Shared {
    async fn call<T: serde::de::DeserializeOwned>(
        &self,
        server: usize,
        method: Method,
        path: &str,
        body: Option<Vec<u8>>,
    ) -> Answer<T> {
        let mut rq = self
            .client
            .request(method, format!("{}{path}", self.servers[server]));
        if let Some(b) = body {
            rq = rq.header("content-type", "application/json").body(b);
        }
        let res = match rq.send().await {
            Ok(r) => r,
            Err(e) => return Answer::Lost(short_err(&e)),
        };
        let status = res.status();
        let bytes = match res.bytes().await {
            Ok(b) => b,
            Err(e) => return Answer::Lost(short_err(&e)),
        };
        if status != StatusCode::OK {
            return Answer::Status(status.as_u16());
        }
        match serde_json::from_slice(&bytes) {
            Ok(v) => Answer::Ok(v),
            Err(_) => Answer::Status(599),
        }
    }

    /// GET whose body is read and dropped.
    async fn get_raw(&self, server: usize, path: &str) -> Answer<()> {
        let url = format!("{}{path}", self.servers[server]);
        match self.client.get(url).send().await {
            Err(e) => Answer::Lost(short_err(&e)),
            Ok(r) => {
                let s = r.status();
                match r.bytes().await {
                    Err(e) => Answer::Lost(short_err(&e)),
                    Ok(_) if s == StatusCode::OK => Answer::Ok(()),
                    Ok(_) => Answer::Status(s.as_u16()),
                }
            }
        }
    }

    async fn put_blob(&self, server: usize, data: Vec<u8>) -> Answer<Hash> {
        let h = Hash::of_blob(&data);
        let url = format!("{}/v1/blobs/{h}", self.servers[server]);
        match self.client.put(url).body(data).send().await {
            Err(e) => Answer::Lost(short_err(&e)),
            Ok(r) if r.status().is_success() => Answer::Ok(h),
            Ok(r) => Answer::Status(r.status().as_u16()),
        }
    }
}

fn short_err(e: &reqwest::Error) -> String {
    if e.is_timeout() {
        "timeout".into()
    } else if e.is_connect() {
        "connect".into()
    } else {
        "transport".into()
    }
}

fn modify(data: &[u8]) -> FileChange {
    FileChange::Modify {
        content: Hash::of_blob(data),
        kind: FileKind::Regular,
        size: data.len() as u64,
    }
}

async fn sleep_unless_stopped(s: &Shared, d: Duration) {
    if !d.is_zero() && !s.stop.load(Ordering::Relaxed) {
        tokio::time::sleep(d).await;
    }
}

async fn agent(s: Arc<Shared>, who: usize, start_after: Duration) -> Tally {
    let mut t = Tally::default();
    let mut rng = Rng::new(s.args.seed ^ (who as u64 + 1).wrapping_mul(0xD1B5_4A32_D192_ED03));
    let server = who % s.servers.len();
    let target = s.args.target.clone();
    let total_weight: u32 = s.mix.iter().map(|m| m.1).sum();
    tokio::time::sleep(start_after).await;
    let mut seq = 0u64;
    let mut last_base: Option<ChangesetId> = None;
    while !s.stop.load(Ordering::Relaxed) {
        seq += 1;
        let measuring = s.measuring.load(Ordering::Relaxed);
        // Working-copy sync: the ref, then the agent's own files at it.
        let mut head = s.heads.newest();
        if s.args.sync {
            sleep_unless_stopped(&s, rng.think(s.args.think_ms)).await;
            let t0 = Instant::now();
            match s
                .call::<RefResponse>(server, Method::GET, &format!("/v1/refs/{target}"), None)
                .await
            {
                Answer::Ok(r) => {
                    if let Some(h) = r.target {
                        s.heads.push(h);
                        head = Some(h);
                        let _ = s
                            .get_raw(server, &format!("/v1/files/{h}?prefix=agents/a{who}"))
                            .await;
                    }
                }
                Answer::Status(c) => t.error(format!("sync {c}")),
                Answer::Lost(e) => t.error(format!("sync {e}")),
            }
            t.time(measuring, "sync", t0);
        }
        let Some(head) = head else {
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        };

        // Reads.
        for _ in 0..s.args.reads {
            if s.stop.load(Ordering::Relaxed) || total_weight == 0 {
                break;
            }
            sleep_unless_stopped(&s, rng.think(s.args.think_ms)).await;
            let mut pick = rng.below(total_weight as usize) as u32;
            let mut kind = s.mix[0].0;
            for &(k, w) in &s.mix {
                if pick < w {
                    kind = k;
                    break;
                }
                pick -= w;
            }
            let hot = format!("hot/h{}", rng.below(s.args.hot_files.max(1)));
            let path = match kind {
                Read::Log if rng.below(2) == 0 => format!("/v1/log/{head}?limit=20&path={hot}"),
                Read::Log => format!("/v1/log/{head}?limit=20"),
                Read::Files => format!("/v1/files/{head}?prefix=d{}", rng.below(10)),
                Read::Blame => {
                    let f = &s.seeded_files[rng.below(s.seeded_files.len())];
                    format!("/v1/blame/{head}?path={f}")
                }
                Read::Search => format!(
                    "/v1/search/{target}?q=agent+{}&limit=20",
                    rng.below(s.args.agents)
                ),
                Read::Diff => match last_base.or_else(|| s.heads.back(8)) {
                    Some(from) => format!("/v1/diff?from={from}&to={head}"),
                    None => format!("/v1/diff?to={head}&prefix=hot"),
                },
            };
            let op = match kind {
                Read::Log => "log",
                Read::Files => "files",
                Read::Blame => "blame",
                Read::Search => "search",
                Read::Diff => "diff",
            };
            let t0 = Instant::now();
            match s.get_raw(server, &path).await {
                Answer::Ok(()) => t.time(measuring, op, t0),
                Answer::Status(c) => t.error(format!("{op} {c}")),
                Answer::Lost(e) => t.error(format!("{op} {e}")),
            }
        }
        if s.stop.load(Ordering::Relaxed) {
            break;
        }

        // Write: upload, create on a possibly stale base, land.
        sleep_unless_stopped(&s, rng.think(s.args.think_ms)).await;
        let marker = format!("swarm {} a{who} s{seq}", s.run);
        let mut changes = BTreeMap::new();
        let mut upload_failed = false;
        for f in 0..s.args.files.max(1) {
            let path = if rng.unit() < s.args.hot_fraction {
                format!("hot/h{}", rng.below(s.args.hot_files.max(1)))
            } else {
                format!("agents/a{who}/f{f}")
            };
            let mut body = format!("{marker} file {f}\n");
            while body.len() < s.args.blob_bytes {
                body.push_str("0123456789abcdef0123456789abcdef\n");
            }
            let t0 = Instant::now();
            match s.put_blob(server, body.clone().into_bytes()).await {
                Answer::Ok(_) => t.time(measuring, "upload", t0),
                Answer::Status(c) => {
                    t.error(format!("upload {c}"));
                    upload_failed = true;
                }
                Answer::Lost(e) => {
                    t.error(format!("upload {e}"));
                    upload_failed = true;
                }
            }
            changes.insert(
                RepoPath::new(path.as_str()).expect("valid path"),
                modify(body.as_bytes()),
            );
        }
        if upload_failed {
            continue;
        }
        let mut base = match s.args.stale_ms {
            Some(ms) if ms > 0 => {
                let age = Duration::from_millis(rng.below(ms as usize + 1) as u64);
                s.heads.aged(age).unwrap_or(head)
            }
            _ => {
                let d = if s.args.stale == 0 {
                    0
                } else {
                    rng.below(s.args.stale + 1)
                };
                s.heads.back(d).unwrap_or(head)
            }
        };
        last_base = Some(base);
        // One logical land: create on `base`, land, and after a conflict
        // resolve onto the conflicting head and land again.
        let first_sent = Instant::now();
        let logical_in_window =
            s.measuring.load(Ordering::Relaxed) && !s.stop.load(Ordering::Relaxed);
        let (mut relands, mut conflicts, mut resends, mut s409) = (0u32, 0u64, 0u64, 0u64);
        // Some(true) landed, Some(false) gave up on a conflict, None failed.
        let outcome: Option<bool> = loop {
            let cs = Changeset {
                parents: vec![base],
                author: format!("swarm-agent-{who}"),
                timestamp: 1_700_000_000 + seq as i64 + i64::from(relands),
                message: marker.clone(),
                changes: changes.clone(),
                extra: BTreeMap::new(),
            };
            let t0 = Instant::now();
            let id = match s
                .call::<CreateChangesetResponse>(
                    server,
                    Method::POST,
                    "/v1/changesets",
                    Some(serde_json::to_vec(&cs).expect("encodes")),
                )
                .await
            {
                Answer::Ok(r) => r.id,
                Answer::Status(c) => {
                    t.error(format!("create {c}"));
                    break None;
                }
                Answer::Lost(e) => {
                    t.error(format!("create {e}"));
                    break None;
                }
            };
            t.time(measuring, "create", t0);

            let land_id = s.args.land_id.then(|| {
                let h = blake3::hash(format!("{marker} r{relands}").as_bytes());
                LandId::from_bytes(&h.as_bytes()[..16]).expect("16 bytes")
            });
            if land_id.is_some() {
                s.land_ids_sent.fetch_add(1, Ordering::Relaxed);
            }
            let req = serde_json::to_vec(&LandRequest {
                changeset: id,
                target_ref: target.clone(),
                create: None,
                land_id,
            })
            .expect("encodes");
            if s.measuring.load(Ordering::Relaxed) && !s.stop.load(Ordering::Relaxed) {
                t.sent_in_window = true;
            }
            let t0 = Instant::now();
            let mut tries = 0;
            let answer = loop {
                tries += 1;
                match s
                    .call::<LandResponse>(server, Method::POST, "/v1/land", Some(req.clone()))
                    .await
                {
                    Answer::Ok(r) => break Some(r),
                    Answer::Status(409) => {
                        t.status_409 += 1;
                        s409 += 1;
                        tokio::time::sleep(Duration::from_millis(20 + rng.below(80) as u64)).await;
                    }
                    // The land may have applied: send it again, as a client would.
                    Answer::Status(c) if c >= 500 || c == 408 => {
                        t.error(format!("land {c}"));
                        t.resends += 1;
                        resends += 1;
                        tokio::time::sleep(Duration::from_millis(200)).await;
                    }
                    Answer::Lost(e) => {
                        t.error(format!("land {e}"));
                        t.resends += 1;
                        resends += 1;
                        tokio::time::sleep(Duration::from_millis(200)).await;
                    }
                    Answer::Status(c) => {
                        t.error(format!("land {c}"));
                        break None;
                    }
                }
                if tries >= 50 {
                    t.error("land gave up");
                    break None;
                }
            };
            let in_window = s.measuring.load(Ordering::Relaxed) && !s.stop.load(Ordering::Relaxed);
            if measuring || in_window {
                t.time(true, "land", t0);
            }
            match answer {
                Some(LandResponse::Landed {
                    landed, rebased, ..
                }) => {
                    s.heads.push(landed);
                    t.landed.push(marker.clone());
                    t.rebased += u64::from(rebased);
                    if in_window {
                        t.lands_in_window += 1;
                        t.answered_in_window = true;
                    } else {
                        t.landed_after += 1;
                    }
                    break Some(true);
                }
                Some(LandResponse::Conflicts { onto, .. }) => {
                    s.heads.push(onto);
                    t.conflicts += 1;
                    conflicts += 1;
                    if in_window {
                        t.answered_in_window = true;
                    }
                    if relands >= s.args.reland_max || s.stop.load(Ordering::Relaxed) {
                        break Some(false);
                    }
                    // Resolved by keeping this agent's files, on the head
                    // the conflict was found against.
                    relands += 1;
                    base = onto;
                    last_base = Some(base);
                }
                None => break None,
            }
        };
        if logical_in_window {
            let l = &mut t.logical;
            l.started += 1;
            l.conflicts += conflicts;
            l.relands += u64::from(relands);
            l.resends += resends;
            l.status_409 += s409;
            match outcome {
                Some(true) => {
                    l.landed += 1;
                    l.time_to_landed
                        .push(first_sent.elapsed().as_micros() as u64);
                }
                Some(false) => l.gave_up += 1,
                None => l.failed += 1,
            }
        }
    }
    t
}

/// Sum of every sample of `name` in a Prometheus text page.
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

async fn scrape(client: &Client, metrics: &[String]) -> String {
    let mut all = String::new();
    for m in metrics {
        if let Ok(r) = client.get(format!("{m}/metrics")).send().await {
            all.push_str(&r.text().await.unwrap_or_default());
            all.push('\n');
        }
    }
    all
}

fn pct(sorted: &[u64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let i = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[i] as f64 / 1000.0
}

/// Generations the head is ahead of the newest head this run saw that has
/// a row of each index kind: one ref read, one indexed query per kind over
/// the heads seen (at most 4096 ids) and a changeset read per generation
/// needed. Cheap enough to run every few seconds beside the load.
async fn lag_sample(
    s: &Shared,
    pool: &sqlx::PgPool,
    generations: &mut HashMap<ChangesetId, u64>,
) -> Result<(u64, u64)> {
    let Answer::Ok(r) = s
        .call::<RefResponse>(0, Method::GET, &format!("/v1/refs/{}", s.args.target), None)
        .await
    else {
        bail!("reading the ref");
    };
    let head = r.target.context("no head")?;
    let ring = s.heads.newest_n(4096);
    let keys: Vec<Vec<u8>> = ring.iter().map(|id| id.0.as_bytes().to_vec()).collect();
    let head_gen = generation(s, head, generations).await?;
    let mut out = [0u64; 2];
    for (n, kind) in ["history-tree-v4", "log-filter-v1"].iter().enumerate() {
        let rows: Vec<(Vec<u8>,)> =
            sqlx::query_as("SELECT key FROM derived WHERE kind = $1 AND key = ANY($2)")
                .bind(kind)
                .bind(&keys)
                .fetch_all(pool)
                .await?;
        let have: HashSet<Vec<u8>> = rows.into_iter().map(|r| r.0).collect();
        // The heads seen are in about the order they were made; the oldest
        // seen bounds the lag when none of them is indexed.
        let newest = ring
            .iter()
            .find(|id| have.contains(id.0.as_bytes().as_slice()))
            .or(ring.last())
            .copied()
            .unwrap_or(head);
        out[n] = head_gen.saturating_sub(generation(s, newest, generations).await?);
    }
    Ok((out[0], out[1]))
}

async fn generation(
    s: &Shared,
    id: ChangesetId,
    cache: &mut HashMap<ChangesetId, u64>,
) -> Result<u64> {
    if let Some(g) = cache.get(&id) {
        return Ok(*g);
    }
    let Answer::Ok(info) = s
        .call::<ChangesetInfo>(0, Method::GET, &format!("/v1/changesets/{id}"), None)
        .await
    else {
        bail!("reading {id}");
    };
    cache.insert(id, info.generation);
    Ok(info.generation)
}

/// Derived kinds the write path stores (create and land); every other kind
/// is an index built by reads or the warmer.
const WRITE_KINDS: &[&str] = &["root_tree", "generation", "skiplist", "rebased-origin-v1"];

/// Logical bytes in the database: changesets, ref log and refs plus the
/// write path's derived rows, and the other derived rows.
async fn logical_bytes(pool: &sqlx::PgPool) -> Result<(i64, i64)> {
    let base: i64 = sqlx::query_scalar(
        "SELECT (SELECT COALESCE(SUM(octet_length(id) + octet_length(data)), 0) FROM changesets)::BIGINT \
         + (SELECT COALESCE(SUM(pg_column_size(ref_log.*)), 0) FROM ref_log)::BIGINT \
         + (SELECT COALESCE(SUM(pg_column_size(refs.*)), 0) FROM refs)::BIGINT",
    )
    .fetch_one(pool)
    .await?;
    let rows: Vec<(String, i64)> = sqlx::query_as(
        "SELECT kind, SUM(octet_length(key) + octet_length(value))::BIGINT FROM derived GROUP BY kind",
    )
    .fetch_all(pool)
    .await?;
    let (mut write, mut read) = (base, 0);
    for (kind, bytes) in rows {
        if WRITE_KINDS.contains(&kind.as_str()) {
            write += bytes;
        } else {
            read += bytes;
        }
    }
    Ok((write, read))
}

async fn db_bytes(pool: &sqlx::PgPool) -> Result<i64> {
    Ok(
        sqlx::query_scalar("SELECT pg_database_size(current_database())")
            .fetch_one(pool)
            .await?,
    )
}

/// Every marker of this run in `target`'s history, with its count.
async fn history_markers(s: &Shared) -> Result<HashMap<String, u32>> {
    let prefix = format!("swarm {} ", s.run);
    let mut counts = HashMap::new();
    let Answer::Ok(r) = s
        .call::<RefResponse>(0, Method::GET, &format!("/v1/refs/{}", s.args.target), None)
        .await
    else {
        bail!("reading the ref");
    };
    let head = r.target.context("no head")?;
    let mut after: Option<String> = None;
    loop {
        let mut path = format!("/v1/log/{head}?limit=1000");
        if let Some(a) = &after {
            path.push_str(&format!("&after={a}"));
        }
        let page = match s.call::<LogResponse>(0, Method::GET, &path, None).await {
            Answer::Ok(p) => p,
            Answer::Status(c) => bail!("log page: {c}"),
            Answer::Lost(e) => bail!("log page: {e}"),
        };
        let mut below_base = false;
        for e in &page.entries {
            if e.generation <= s.base_generation {
                below_base = true;
                continue;
            }
            if e.changeset.message.starts_with(&prefix) {
                *counts.entry(e.changeset.message.clone()).or_default() += 1;
            }
        }
        match page.next {
            // The log is in descending generation order, so once it reaches
            // the base no later page holds this run's lands.
            Some(n) if !below_base => after = Some(n),
            _ => return Ok(counts),
        }
    }
}

/// Seeds the target (unless it exists) and returns the base tree's files and
/// the generation of the head the run starts from.
async fn setup(s: &Shared) -> Result<(Vec<String>, u64)> {
    let mut files = Vec::new();
    let mut changes = BTreeMap::new();
    for d in 0..10 {
        for f in 0..20 {
            files.push(format!("d{d}/f{f}"));
        }
    }
    for h in 0..s.args.hot_files.max(1) {
        files.push(format!("hot/h{h}"));
    }
    let Answer::Ok(r) = s
        .call::<RefResponse>(0, Method::GET, &format!("/v1/refs/{}", s.args.target), None)
        .await
    else {
        bail!("reading {}", s.args.target);
    };
    if let Some(h) = r.target {
        let Answer::Ok(page) = s
            .call::<LogResponse>(0, Method::GET, &format!("/v1/log/{h}?limit=1"), None)
            .await
        else {
            bail!("reading the generation of {}", s.args.target);
        };
        let generation = page.entries.first().map_or(0, |e| e.generation);
        s.heads.push(h);
        return Ok((files, generation));
    }
    for f in &files {
        let data = format!("{f}\nline 2\nline 3\n").into_bytes();
        if !matches!(s.put_blob(0, data.clone()).await, Answer::Ok(_)) {
            bail!("uploading the base tree");
        }
        changes.insert(RepoPath::new(f.as_str())?, modify(&data));
    }
    let root = Changeset {
        parents: vec![],
        author: "swarm".into(),
        timestamp: 1_700_000_000,
        message: "swarm root".into(),
        changes,
        extra: BTreeMap::new(),
    };
    let Answer::Ok(created) = s
        .call::<CreateChangesetResponse>(
            0,
            Method::POST,
            "/v1/changesets",
            Some(serde_json::to_vec(&root)?),
        )
        .await
    else {
        bail!("creating the root");
    };
    let body = serde_json::to_vec(&CasRefRequest {
        expected: None,
        new: Some(created.id),
        operation: None,
        message: None,
    })?;
    match s
        .call::<serde_json::Value>(
            0,
            Method::POST,
            &format!("/v1/refs/{}", s.args.target),
            Some(body),
        )
        .await
    {
        Answer::Ok(_) => {}
        _ => bail!("creating {}", s.args.target),
    }
    s.heads.push(created.id);
    Ok((files, 0))
}

#[derive(Serialize)]
struct OpStats {
    count: usize,
    p50_ms: f64,
    p99_ms: f64,
}

#[derive(Serialize)]
struct Report {
    servers: usize,
    agents: usize,
    think_ms: u64,
    duration_s: f64,
    land_id: bool,
    lands: u64,
    lands_per_s: f64,
    landed_total: u64,
    rebased: u64,
    conflicts: u64,
    status_409: u64,
    resends: u64,
    /// Agents that sent a land inside the window and had none answered there.
    stalled_agents: usize,
    /// Agents that sent no land inside the window (still thinking, reading
    /// or building one when it closed).
    incomplete_cycles: usize,
    lands_per_server: Vec<u64>,
    double_lands: Option<usize>,
    lost_lands: Option<usize>,
    ops: BTreeMap<String, OpStats>,
    errors: BTreeMap<String, u64>,
    cas_losses: f64,
    batch_mean: Option<f64>,
    land_recorded: f64,
    warmer_lag_history: Option<(u64, u64)>,
    warmer_lag_log_filter: Option<(u64, u64)>,
    db_bytes_per_land: Option<f64>,
    /// Logical bytes per land written by the write path (changesets, ref
    /// log, refs and their derived rows), and by indexes reads and the
    /// warmer built.
    write_bytes_per_land: Option<f64>,
    index_bytes_per_land: Option<f64>,
    load_average: String,
    stale: String,
    no_search: bool,
    reland_max: u32,
    logical: LogicalReport,
}

/// Logical lands whose first land was sent inside the window; ratios are
/// per logical land that landed.
#[derive(Serialize)]
struct LogicalReport {
    started: u64,
    landed: u64,
    gave_up: u64,
    failed: u64,
    conflicts: u64,
    relands: u64,
    resends: u64,
    status_409: u64,
    conflicts_per_land: Option<f64>,
    relands_resends_per_land: Option<f64>,
    time_to_landed_p50_ms: f64,
    time_to_landed_p99_ms: f64,
}

fn lag_summary(v: &mut [u64]) -> Option<(u64, u64)> {
    if v.is_empty() {
        return None;
    }
    v.sort_unstable();
    Some((v[v.len() / 2], *v.last().unwrap()))
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let client = Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(args.request_timeout))
        .tcp_nodelay(true)
        .pool_max_idle_per_host(args.agents.max(16))
        .build()?;
    let run = {
        let t = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_nanos();
        hex::encode(&blake3::hash(format!("{t} {}", args.seed).as_bytes()).as_bytes()[..6])
    };
    let mut shared = Shared {
        servers: args
            .server
            .iter()
            .map(|s| s.trim_end_matches('/').to_string())
            .collect(),
        mix: parse_mix(&args.read_mix)?
            .into_iter()
            .filter(|m| !(args.no_search && m.0 == Read::Search))
            .collect(),
        args: args.clone(),
        run,
        client: client.clone(),
        heads: Heads {
            ring: Mutex::new(VecDeque::new()),
        },
        stop: AtomicBool::new(false),
        measuring: AtomicBool::new(false),
        seeded_files: Vec::new(),
        base_generation: 0,
        land_ids_sent: AtomicU64::new(0),
    };
    (shared.seeded_files, shared.base_generation) = setup(&shared).await?;
    let s = Arc::new(shared);
    let pool = match &args.database_url {
        Some(u) => Some(
            sqlx::postgres::PgPoolOptions::new()
                .max_connections(2)
                .connect(u)
                .await?,
        ),
        None => None,
    };

    let ramp = Duration::from_secs(args.ramp);
    let tasks: Vec<_> = (0..args.agents)
        .map(|i| {
            let start = ramp.mul_f64(i as f64 / args.agents.max(1) as f64);
            tokio::spawn(agent(Arc::clone(&s), i, start))
        })
        .collect();
    tokio::time::sleep(ramp).await;

    let before = scrape(&client, &args.metrics).await;
    let bytes_before = match &pool {
        Some(p) => Some((db_bytes(p).await?, logical_bytes(p).await?)),
        None => None,
    };
    s.measuring.store(true, Ordering::Relaxed);
    let started = Instant::now();
    let lags = Arc::new(Mutex::new((Vec::new(), Vec::new())));
    let lag_task = pool.clone().map(|p| {
        let (s, lags) = (Arc::clone(&s), Arc::clone(&lags));
        tokio::spawn(async move {
            let mut generations = HashMap::new();
            while !s.stop.load(Ordering::Relaxed) {
                if let Ok((h, l)) = lag_sample(&s, &p, &mut generations).await {
                    let mut g = lags.lock().unwrap();
                    g.0.push(h);
                    g.1.push(l);
                }
                tokio::time::sleep(Duration::from_secs(s.args.lag_every.max(1))).await;
            }
        })
    });
    tokio::time::sleep(Duration::from_secs(args.duration)).await;
    s.stop.store(true, Ordering::Relaxed);
    let elapsed = started.elapsed().as_secs_f64();
    let after = scrape(&client, &args.metrics).await;
    let mut tally = Tally::default();
    let (mut stalled, mut incomplete) = (0, 0);
    let mut per_server = vec![0u64; s.servers.len()];
    for (i, t) in tasks.into_iter().enumerate() {
        let a = t.await?;
        stalled += usize::from(a.sent_in_window && !a.answered_in_window);
        incomplete += usize::from(!a.sent_in_window);
        per_server[i % s.servers.len()] += a.lands_in_window;
        for (k, v) in a.ops {
            tally.ops.entry(k).or_default().extend(v);
        }
        for (k, v) in a.errors {
            *tally.errors.entry(k).or_default() += v;
        }
        tally.lands_in_window += a.lands_in_window;
        tally.landed_after += a.landed_after;
        tally.conflicts += a.conflicts;
        tally.status_409 += a.status_409;
        tally.resends += a.resends;
        tally.rebased += a.rebased;
        tally.landed.extend(a.landed);
        let (l, m) = (&mut tally.logical, a.logical);
        l.started += m.started;
        l.landed += m.landed;
        l.gave_up += m.gave_up;
        l.failed += m.failed;
        l.conflicts += m.conflicts;
        l.relands += m.relands;
        l.resends += m.resends;
        l.status_409 += m.status_409;
        l.time_to_landed.extend(m.time_to_landed);
    }
    tally.logical.time_to_landed.sort_unstable();
    let lg = &tally.logical;
    let per = |n: u64| (lg.landed > 0).then(|| n as f64 / lg.landed as f64);
    let logical = LogicalReport {
        started: lg.started,
        landed: lg.landed,
        gave_up: lg.gave_up,
        failed: lg.failed,
        conflicts: lg.conflicts,
        relands: lg.relands,
        resends: lg.resends,
        status_409: lg.status_409,
        conflicts_per_land: per(lg.conflicts),
        relands_resends_per_land: per(lg.relands + lg.resends),
        time_to_landed_p50_ms: pct(&lg.time_to_landed, 0.5),
        time_to_landed_p99_ms: pct(&lg.time_to_landed, 0.99),
    };
    if let Some(t) = lag_task {
        t.await?;
    }
    let bytes_after = match &pool {
        Some(p) => Some((db_bytes(p).await?, logical_bytes(p).await?)),
        None => None,
    };
    let (doubles, lost) = if args.no_history_check {
        (None, None)
    } else {
        let counts = history_markers(&s).await?;
        let doubles = counts.values().filter(|c| **c > 1).count();
        let lost = tally
            .landed
            .iter()
            .filter(|m| !counts.contains_key(*m))
            .count();
        (Some(doubles), Some(lost))
    };
    let d = |name: &str| metric(&after, name) - metric(&before, name);
    let batches = d("leep_land_batch_size_count");
    let landed_total = tally.landed.len() as u64;
    let per_land = |before: Option<i64>, after: Option<i64>| match (before, after) {
        (Some(b), Some(a)) if landed_total > 0 => Some((a - b) as f64 / landed_total as f64),
        _ => None,
    };
    let (mut lh, mut ll) = std::mem::take(&mut *lags.lock().unwrap());
    let report = Report {
        servers: s.servers.len(),
        agents: args.agents,
        think_ms: args.think_ms,
        duration_s: elapsed,
        land_id: args.land_id,
        lands: tally.lands_in_window,
        lands_per_s: tally.lands_in_window as f64 / elapsed,
        landed_total,
        rebased: tally.rebased,
        conflicts: tally.conflicts,
        status_409: tally.status_409,
        resends: tally.resends,
        stalled_agents: stalled,
        incomplete_cycles: incomplete,
        lands_per_server: per_server,
        double_lands: doubles,
        lost_lands: lost,
        ops: tally
            .ops
            .iter_mut()
            .map(|(k, v)| {
                v.sort_unstable();
                (
                    k.to_string(),
                    OpStats {
                        count: v.len(),
                        p50_ms: pct(v, 0.5),
                        p99_ms: pct(v, 0.99),
                    },
                )
            })
            .collect(),
        errors: tally.errors.clone(),
        cas_losses: d("leep_land_cas_losses_total"),
        batch_mean: (batches > 0.0).then(|| d("leep_land_batch_size_sum") / batches),
        land_recorded: d("leep_land_recorded_total"),
        warmer_lag_history: lag_summary(&mut lh),
        warmer_lag_log_filter: lag_summary(&mut ll),
        db_bytes_per_land: per_land(bytes_before.map(|b| b.0), bytes_after.map(|a| a.0)),
        write_bytes_per_land: per_land(bytes_before.map(|b| b.1 .0), bytes_after.map(|a| a.1 .0)),
        index_bytes_per_land: per_land(bytes_before.map(|b| b.1 .1), bytes_after.map(|a| a.1 .1)),
        load_average: std::fs::read_to_string("/proc/loadavg")
            .map(|l| l.split_whitespace().take(3).collect::<Vec<_>>().join(" "))
            .unwrap_or_default(),
        stale: match args.stale_ms {
            Some(ms) if ms > 0 => format!("{ms} ms"),
            _ => format!("{} heads", args.stale),
        },
        no_search: args.no_search,
        reland_max: args.reland_max,
        logical,
    };
    print_report(&report);
    if let Some(p) = &args.json {
        std::fs::write(p, serde_json::to_vec_pretty(&report)?)?;
    }
    if args.land_id && report.double_lands.unwrap_or(0) > 0 {
        bail!(
            "{} lands are in history twice with land ids on",
            report.double_lands.unwrap_or(0)
        );
    }
    if report.lost_lands.unwrap_or(0) > 0 {
        bail!(
            "{} lands answered `landed` are not in history",
            report.lost_lands.unwrap_or(0)
        );
    }
    Ok(())
}

fn print_report(r: &Report) {
    let opt = |v: Option<usize>| v.map_or("-".to_string(), |v| v.to_string());
    let lag = |v: Option<(u64, u64)>| v.map_or("-".to_string(), |(m, x)| format!("{m} / {x}"));
    println!(
        "servers {} | agents {} | think {} ms | land ids {} | {:.0} s | load {}",
        r.servers,
        r.agents,
        r.think_ms,
        if r.land_id { "on" } else { "off" },
        r.duration_s,
        r.load_average
    );
    println!(
        "| lands | lands/s | rebased | conflicts | 409s | resends | CAS losses | batch mean | recorded-id answers | stalled agents | incomplete cycles | lands per server | double lands | lost lands | warmer lag history p50/max | log-filter p50/max | DB bytes/land | write bytes/land | index bytes/land |"
    );
    println!("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|");
    println!(
        "| {} | {:.1} | {} | {} | {} | {} | {} | {} | {} | {} of {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |",
        r.lands,
        r.lands_per_s,
        r.rebased,
        r.conflicts,
        r.status_409,
        r.resends,
        r.cas_losses,
        r.batch_mean.map_or("-".into(), |b| format!("{b:.1}")),
        r.land_recorded,
        r.stalled_agents,
        r.agents,
        r.incomplete_cycles,
        r.lands_per_server
            .iter()
            .map(u64::to_string)
            .collect::<Vec<_>>()
            .join(" / "),
        opt(r.double_lands),
        opt(r.lost_lands),
        lag(r.warmer_lag_history),
        lag(r.warmer_lag_log_filter),
        r.db_bytes_per_land.map_or("-".into(), |b| format!("{b:.0}")),
        r.write_bytes_per_land.map_or("-".into(), |b| format!("{b:.0}")),
        r.index_bytes_per_land.map_or("-".into(), |b| format!("{b:.0}")),
    );
    let l = &r.logical;
    let ratio = |v: Option<f64>| v.map_or("-".into(), |v| format!("{v:.3}"));
    println!(
        "logical lands (first sent in window) | stale {} | search {} | reland max {}",
        r.stale,
        if r.no_search { "off" } else { "on" },
        r.reland_max
    );
    println!(
        "| started | landed | gave up | failed | conflicts | relands | resends | 409s | conflicts/land | (relands+resends)/land | time-to-landed p50 ms | p99 ms |"
    );
    println!("|---|---|---|---|---|---|---|---|---|---|---|---|");
    println!(
        "| {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {:.1} | {:.1} |",
        l.started,
        l.landed,
        l.gave_up,
        l.failed,
        l.conflicts,
        l.relands,
        l.resends,
        l.status_409,
        ratio(l.conflicts_per_land),
        ratio(l.relands_resends_per_land),
        l.time_to_landed_p50_ms,
        l.time_to_landed_p99_ms,
    );
    println!("| op | count | p50 ms | p99 ms |");
    println!("|---|---|---|---|");
    for (k, v) in &r.ops {
        println!("| {k} | {} | {:.1} | {:.1} |", v.count, v.p50_ms, v.p99_ms);
    }
    if !r.errors.is_empty() {
        println!("errors: {:?}", r.errors);
    }
}
