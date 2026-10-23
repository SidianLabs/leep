//! `leep-soak`: a soak test for `leep-server` (ADR 0022).
//!
//! Starts (or connects to) a server, runs an agent swarm against it for a set
//! duration while operators garbage-collect, warm indexes and optionally
//! restart the server, checks invariants throughout, records performance and
//! resource trends, and exits non-zero if anything broke.

mod agent;
mod api;
mod cli_agent;
mod config;
mod invariants;
mod metrics;
mod ops;
mod repo;
mod report;
mod shared;
mod util;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use clap::Parser;
use ed25519_dalek::SigningKey;
use leep_core::signature::format_public_key;

use crate::agent::{Agent, Env};
use crate::api::Api;
use crate::cli_agent::{CliAgent, CliEnv};
use crate::config::Args;
use crate::invariants::Checker;
use crate::metrics::{SamplerCfg, Trends};
use crate::ops::{Server, ServerConfig, Stores};
use crate::shared::Shared;
use crate::util::Rng;

/// Tokens and keys of an owned server with policy on.
struct Identity {
    ops: String,
    reviewer: String,
    agents: Vec<String>,
    clis: Vec<String>,
    signers: Vec<SigningKey>,
    key_files: Vec<PathBuf>,
}

fn token_for(seed: u64, name: &str) -> String {
    blake3::hash(format!("leep-soak:{seed}:{name}").as_bytes())
        .to_hex()
        .to_string()
}

fn digest(token: &str) -> String {
    format!("blake3:{}", blake3::hash(token.trim().as_bytes()).to_hex())
}

fn write_private(path: &Path, text: &str) -> Result<()> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)?;
    f.write_all(text.as_bytes())?;
    Ok(())
}

/// Write the tokens, trusted keys and review policy; return the server flags
/// and who holds which token.
fn write_policy(args: &Args, dir: &Path) -> Result<(Identity, Vec<String>)> {
    std::fs::create_dir_all(dir.join("keys"))?;
    let limited = args.rate_limit.is_some() || args.max_concurrent.is_some();
    let entry = |name: &str, token: &str, unlimited: bool| {
        let mut e = serde_json::json!({ "name": name, "role": "write", "hash": digest(token) });
        if unlimited && limited {
            e["rate"] = "unlimited".into();
            e["concurrency"] = "unlimited".into();
        }
        e
    };
    let ops = token_for(args.seed, "ops");
    let reviewer = token_for(args.seed, "reviewer");
    let mut tokens = vec![
        entry("soak-ops", &ops, true),
        entry("soak-reviewer", &reviewer, true),
    ];
    let agents: Vec<String> = (0..args.agents)
        .map(|i| token_for(args.seed, &format!("agent-{i}")))
        .collect();
    for (i, t) in agents.iter().enumerate() {
        tokens.push(entry(&format!("soak-agent-{i}"), t, false));
    }
    let clis: Vec<String> = (0..args.cli_agents)
        .map(|i| token_for(args.seed, &format!("cli-{i}")))
        .collect();
    // A checkout is thousands of reads (docs/OPERATIONS.md, request limits), so
    // the `leep` agents get their own unlimited entries, as a merge bot would.
    for (i, t) in clis.iter().enumerate() {
        tokens.push(entry(&format!("soak-cli-{i}"), t, true));
    }
    std::fs::write(
        dir.join("tokens.json"),
        serde_json::to_string_pretty(&serde_json::json!({ "tokens": tokens }))?,
    )?;

    let mut rng = Rng::new(args.seed ^ 0x5161);
    let keys: Vec<SigningKey> = (0..4 + args.cli_agents)
        .map(|_| {
            let mut seed = [0u8; 32];
            seed.copy_from_slice(&rng.bytes(32));
            SigningKey::from_bytes(&seed)
        })
        .collect();
    let entries: Vec<_> = keys
        .iter()
        .enumerate()
        .map(|(i, k)| {
            serde_json::json!({
                "name": format!("author-{i}"),
                "public_key": format_public_key(k.verifying_key().as_bytes())
            })
        })
        .collect();
    std::fs::write(
        dir.join("signing-keys.json"),
        serde_json::to_string_pretty(&serde_json::json!({ "keys": entries }))?,
    )?;
    let mut key_files = Vec::new();
    for (i, k) in keys.iter().enumerate().skip(4) {
        let p = dir.join("keys").join(format!("cli-{}.key", i - 4));
        write_private(
            &p,
            &format!("leep-ed25519-secret:{}\n", hex::encode(k.to_bytes())),
        )?;
        key_files.push(p);
    }
    std::fs::write(
        dir.join("review-policy.json"),
        r#"{ "refs": ["release"], "auto_approve": [], "docs_globs": [] }"#,
    )?;
    let flags = vec![
        "--tokens-file".to_string(),
        dir.join("tokens.json").display().to_string(),
        "--signing-keys-file".into(),
        dir.join("signing-keys.json").display().to_string(),
        "--require-signatures".into(),
        "release".into(),
        "--review-policy-file".into(),
        dir.join("review-policy.json").display().to_string(),
    ];
    Ok((
        Identity {
            ops,
            reviewer,
            agents,
            clis,
            signers: keys[..4].to_vec(),
            key_files,
        },
        flags,
    ))
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind")
        .local_addr()
        .unwrap()
        .port()
}

fn locate_bins(args: &Args) -> Result<PathBuf> {
    let dir = match &args.bin_dir {
        Some(d) => d.clone(),
        None => std::env::current_exe()?
            .parent()
            .context("no executable directory")?
            .to_path_buf(),
    };
    for b in ["leep", "leep-server"] {
        if !dir.join(b).is_file() {
            bail!(
                "{} not found in {}; run `cargo build --workspace` (same profile) first, or pass --bin-dir",
                b,
                dir.display()
            );
        }
    }
    Ok(dir)
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    match run(args).await {
        Ok(true) => {}
        Ok(false) => std::process::exit(1),
        Err(e) => {
            eprintln!("leep-soak: {e:#}");
            std::process::exit(2);
        }
    }
}

async fn run(args: Args) -> Result<bool> {
    let unix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs();
    let out = args
        .out
        .clone()
        .unwrap_or_else(|| PathBuf::from(format!("soak-out/{unix}")));
    let out = std::fs::canonicalize({
        std::fs::create_dir_all(&out)?;
        &out
    })?;
    // Scratch space of an earlier run in the same directory (kept after a
    // failure) would hand the `leep` agents working copies that exist already.
    let work = out.join("work");
    let _ = std::fs::remove_dir_all(&work);
    std::fs::create_dir_all(&work)?;
    let run_id = format!("{:x}{:x}", args.seed & 0xfff, unix % 0xfffff);
    let n_beats = args.agents + args.cli_agents;
    let sh = Arc::new(Shared::new(args.clone(), run_id.clone(), n_beats));

    // ---- stores and server ----
    let external = args.server.is_some();
    let mut pg: Option<ops::PgDb> = None;
    let mut server: Option<Arc<Server>> = None;
    let mut identity: Option<Identity> = None;
    let (base_url, stores, bin_dir);
    if let Some(url) = &args.server {
        base_url = url.clone();
        bin_dir = locate_bins(&args).ok();
        stores = match (&args.meta, &args.blobs) {
            (Some(m), Some(b)) => Some(Stores {
                meta: m.clone(),
                blobs: b.clone(),
                blob_dir: b.strip_prefix("file://").map(PathBuf::from),
            }),
            _ => None,
        };
    } else {
        let bins = locate_bins(&args)?;
        let admin = args
            .postgres_url
            .clone()
            .or_else(|| std::env::var("LEEP_TEST_POSTGRES_URL").ok())
            .filter(|_| !args.sqlite);
        let blob_dir = work.join("blobs");
        std::fs::create_dir_all(&blob_dir)?;
        let meta = match &admin {
            Some(u) => {
                let db = ops::PgDb::create(u, &format!("{}_{}", std::process::id(), unix)).await?;
                let url = db.url.clone();
                pg = Some(db);
                url
            }
            None => format!("sqlite://{}?mode=rwc", work.join("meta.db").display()),
        };
        let st = Stores {
            meta,
            blobs: format!("file://{}", blob_dir.display()),
            blob_dir: Some(blob_dir),
        };
        let mut extra = Vec::new();
        if !args.no_policy {
            let (id, flags) = write_policy(&args, &work)?;
            identity = Some(id);
            extra.extend(flags);
        }
        extra.extend([
            "--cache-bytes".to_string(),
            (args.cache_mib * 1024 * 1024).to_string(),
        ]);
        if let Some(r) = args.rate_limit {
            extra.extend(["--rate-limit".to_string(), r.to_string()]);
        }
        if let Some(b) = args.rate_burst {
            extra.extend(["--rate-burst".to_string(), b.to_string()]);
        }
        if let Some(c) = args.max_concurrent {
            extra.extend(["--max-concurrent-per-token".to_string(), c.to_string()]);
        }
        let s = Arc::new(Server::new(ServerConfig {
            bin_dir: bins.clone(),
            stores: st.clone(),
            port: free_port(),
            metrics_port: free_port(),
            log: out.join("server.log"),
            extra,
            request_timeout: args.request_timeout,
        }));
        s.start().await?;
        base_url = s.url();
        server = Some(s);
        stores = Some(st);
        bin_dir = Some(bins);
    }
    eprintln!(
        "soak {run_id}: {} agents + {} cli agents for {:?} against {base_url} ({}), seed {}",
        args.agents,
        args.cli_agents,
        args.duration,
        match (&pg, external) {
            (Some(_), _) => "postgres",
            (None, true) => "external server",
            _ => "sqlite",
        },
        args.seed
    );

    let result = drive(
        &args,
        &sh,
        &out,
        &work,
        &base_url,
        &stores,
        bin_dir.as_deref(),
        server.clone(),
        identity,
        pg.as_ref(),
    )
    .await;

    // ---- shutdown and cleanup ----
    let mut clean_exit = None;
    if let Some(s) = &server {
        let how = s.stop(true).await;
        clean_exit = Some(how);
    }
    let failed = result.as_ref().map_or(true, |ok| !*ok);
    if let Some(db) = &pg {
        if args.keep_data || failed {
            eprintln!("kept database {} ({})", db.name, db.url);
        } else {
            db.drop_db().await?;
        }
    }
    if !args.keep_data && !failed {
        let _ = std::fs::remove_dir_all(&work);
    } else {
        eprintln!("kept data under {}", work.display());
    }
    if let Some(how) = clean_exit {
        eprintln!("server stopped: {how}");
    }
    result
}

#[allow(clippy::too_many_arguments)]
async fn drive(
    args: &Args,
    sh: &Arc<Shared>,
    out: &Path,
    work: &Path,
    base_url: &str,
    stores: &Option<Stores>,
    bin_dir: Option<&Path>,
    server: Option<Arc<Server>>,
    identity: Option<Identity>,
    pg: Option<&ops::PgDb>,
) -> Result<bool> {
    let backend = match stores.as_ref().map(|s| s.meta.as_str()) {
        Some(m) if m.starts_with("postgres") => "postgres",
        Some(m) if m.starts_with("sqlite") => "sqlite",
        Some(_) => "other",
        None => "external server",
    };
    let ops_token = identity
        .as_ref()
        .map(|i| i.ops.clone())
        .or(args.token.clone());
    let ops_api = Api::new(sh.clone(), base_url, ops_token.clone(), "ops");
    let layout = repo::Layout::new(args.files);

    // ---- setup: the repository and the release lineage ----
    let setup_started = std::time::Instant::now();
    let existing = ops_api.get_ref("main").await;
    let Ok(existing) = existing else {
        bail!("cannot reach the server at {base_url}");
    };
    if existing.is_none() || args.seed_on_existing {
        repo::import(&ops_api, layout, args.seed, existing)
            .await
            .map_err(|e| anyhow::anyhow!("importing the repository failed: {e:?}"))?;
    }
    let mut reviewer_api = None;
    if let Some(id) = &identity {
        let rapi = Api::new(sh.clone(), base_url, Some(id.reviewer.clone()), "reviewer");
        setup_release(sh, &ops_api, &rapi, &id.signers[0])
            .await
            .map_err(|e| anyhow::anyhow!("creating the release ref failed: {e:?}"))?;
        reviewer_api = Some(rapi);
    }
    eprintln!(
        "setup: {} files imported in {:.1}s",
        layout.total(),
        setup_started.elapsed().as_secs_f64()
    );
    sh.start_run();

    let checker = Arc::new(tokio::sync::Mutex::new(Checker::new(
        sh.clone(),
        ops_api.clone(),
    )));
    let trends = Arc::new(std::sync::Mutex::new(Trends::default()));
    let gc_runs = Arc::new(std::sync::Mutex::new(Vec::new()));
    let derive_runs = Arc::new(std::sync::Mutex::new(Vec::new()));

    // First check on the fresh repository, so a broken setup fails early.
    if checker.lock().await.tick(false).await.is_err() {
        return finish(
            args,
            sh,
            out,
            &trends,
            &gc_runs,
            &derive_runs,
            &checker,
            server.as_deref(),
            layout,
            base_url,
            backend,
        )
        .await;
    }

    let env = Arc::new(Env {
        sh: sh.clone(),
        layout,
        signers: Arc::new(
            identity
                .as_ref()
                .map(|i| i.signers.clone())
                .unwrap_or_default(),
        ),
        reviewer: reviewer_api,
    });
    let mut tasks = Vec::new();
    for i in 0..args.agents {
        let token = identity
            .as_ref()
            .map(|id| id.agents[i].clone())
            .or(args.token.clone());
        let api = Api::new(sh.clone(), base_url, token, &format!("a{i:02}"));
        tasks.push(tokio::spawn(Agent::new(env.clone(), i, api).run()));
    }
    if args.cli_agents > 0 {
        let Some(bins) = bin_dir else {
            bail!("CLI agents need the `leep` binary; pass --bin-dir or --cli-agents 0");
        };
        let cenv = Arc::new(CliEnv {
            sh: sh.clone(),
            bin_dir: bins.to_path_buf(),
            url: base_url.to_string(),
            layout,
            work: work.to_path_buf(),
            key_files: identity
                .as_ref()
                .map(|i| i.key_files.clone())
                .unwrap_or_default(),
        });
        for i in 0..args.cli_agents {
            let token = identity
                .as_ref()
                .map(|id| id.clis[i].clone())
                .or(args.token.clone());
            let api = Api::new(sh.clone(), base_url, token.clone(), &format!("c{i:02}"));
            tasks.push(tokio::spawn(
                CliAgent::new(cenv.clone(), i, args.agents + i, api, token).run(),
            ));
        }
    }

    // ---- operators ----
    let mut ops_tasks = Vec::new();
    if let (Some(st), Some(bins)) = (stores.clone(), bin_dir) {
        if let Some(every) = args.gc_every() {
            ops_tasks.push(tokio::spawn(ops::gc_loop(
                bins.to_path_buf(),
                st.clone(),
                sh.clone(),
                checker.clone(),
                gc_runs.clone(),
                every,
                out.join("gc.log"),
            )));
        }
        if let Some(every) = args.backup_every() {
            ops_tasks.push(tokio::spawn(ops::backup_loop(
                bins.to_path_buf(),
                st.clone(),
                sh.clone(),
                out.join("backup"),
                every,
            )));
        }
        if let Some(every) = args.derive_every() {
            ops_tasks.push(tokio::spawn(ops::derive_loop(
                bins.to_path_buf(),
                st,
                sh.clone(),
                derive_runs.clone(),
                every,
            )));
        }
    }
    if args.chaos {
        match &server {
            Some(s) => ops_tasks.push(tokio::spawn(ops::chaos_loop(
                s.clone(),
                sh.clone(),
                args.chaos_restarts,
            ))),
            None => eprintln!("--chaos needs an owned server; ignored with --server"),
        }
    }
    let pg_url = pg.map(|d| d.url.clone()).or_else(|| {
        stores
            .as_ref()
            .map(|s| s.meta.clone())
            .filter(|m| m.starts_with("postgres"))
    });
    let metrics_url = server
        .as_ref()
        .map(|s| s.metrics_url())
        .or(args.metrics_url.clone());
    ops_tasks.push(tokio::spawn(metrics::sampler(SamplerCfg {
        sh: sh.clone(),
        trends: trends.clone(),
        server: server.clone(),
        pid: args.pid,
        metrics_url,
        pg_url,
        blob_dir: stores.as_ref().and_then(|s| s.blob_dir.clone()),
        every: args.sample_interval,
    })));
    // Invariant checks.
    {
        let (sh, checker, every) = (sh.clone(), checker.clone(), args.check_interval);
        ops_tasks.push(tokio::spawn(async move {
            let mut next = std::time::Instant::now() + every;
            while !sh.stopped() {
                tokio::time::sleep(Duration::from_millis(200)).await;
                if std::time::Instant::now() < next {
                    continue;
                }
                let mut c = checker.lock().await;
                if c.tick(false).await.is_err() && !sh.stopped() {
                    break;
                }
                eprintln!(
                    "[{:>6.0}s] {} lands acked, {} changesets walked, check took {} ms, {} violations",
                    sh.now_secs(),
                    sh.ledger.len(),
                    c.nodes_known(),
                    c.last_tick_ms,
                    sh.violation_count()
                );
                drop(c);
                next = std::time::Instant::now() + every;
            }
        }));
    }
    // Watchdog: a deadlock shows as an agent that stops making progress.
    {
        let (sh, stall) = (sh.clone(), args.stall_timeout);
        ops_tasks.push(tokio::spawn(async move {
            while !sh.stopped() {
                tokio::time::sleep(Duration::from_secs(1)).await;
                let now = sh.start.elapsed().as_millis() as u64;
                for (i, b) in sh.beats.iter().enumerate() {
                    let last = b.load(std::sync::atomic::Ordering::Relaxed);
                    if now.saturating_sub(last) > stall.as_millis() as u64 && !sh.stopped() {
                        sh.violate(
                            "deadlock",
                            format!("agent {i} made no progress for {}s", (now - last) / 1000),
                        );
                        return;
                    }
                }
            }
        }));
    }

    // ---- run ----
    let deadline = args.duration;
    while sh.now_secs() < deadline.as_secs_f64() && !sh.stopped() {
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let run_secs = sh.now_secs();
    sh.stop.store(true, std::sync::atomic::Ordering::Relaxed);
    for t in tasks {
        let _ = tokio::time::timeout(Duration::from_secs(200), t).await;
    }
    for t in ops_tasks {
        let _ = tokio::time::timeout(Duration::from_secs(120), t).await;
    }
    sh.stats
        .0
        .lock()
        .unwrap()
        .counters
        .insert("run_secs_x1000", (run_secs * 1000.0) as u64);

    // ---- final checks, with the swarm stopped ----
    sh.finishing
        .store(true, std::sync::atomic::Ordering::Relaxed);
    sh.stop.store(false, std::sync::atomic::Ordering::Relaxed);
    eprintln!("final checks after {run_secs:.0}s ...");
    let fin = {
        let mut c = checker.lock().await;
        c.tick(true).await
    };
    if let Err(e) = fin {
        eprintln!("final check did not complete: {e:?}");
    }
    if let (Some(st), Some(bins)) = (stores.as_ref(), bin_dir) {
        eprintln!("final verify ...");
        ops::final_verify(bins, st, sh).await;
        if args.backup_every().is_some() {
            eprintln!("final backup and restore drill ...");
            ops::final_restore_drill(bins, st, sh, &out.join("backup")).await;
        }
    }
    sh.stop.store(true, std::sync::atomic::Ordering::Relaxed);
    finish(
        args,
        sh,
        out,
        &trends,
        &gc_runs,
        &derive_runs,
        &checker,
        server.as_deref(),
        layout,
        base_url,
        backend,
    )
    .await
}

/// Create `release` with a root changeset that is signed and approved, which
/// is what its policy asks of every changeset it ever holds.
async fn setup_release(
    sh: &Arc<Shared>,
    ops: &Api,
    reviewer: &Api,
    key: &SigningKey,
) -> api::R<()> {
    use ed25519_dalek::Signer;
    use leep_core::api::{AddReviewRequest, AddSignatureRequest};
    use leep_core::review::Verdict;
    use leep_core::signature::signing_message;
    use leep_core::{Changeset, FileKind, RepoPath};
    use std::collections::BTreeMap;
    if ops.get_ref("release").await?.is_some() {
        return Ok(());
    }
    let body = b"# Releases\n".to_vec();
    let h = leep_core::Hash::of_blob(&body);
    ops.upload(&[(h, body.clone())]).await?;
    let cs = Changeset {
        parents: vec![],
        author: "soak-ops".into(),
        timestamp: 1_700_000_000,
        message: "release root".into(),
        changes: BTreeMap::from([(
            RepoPath::new("release/README.md").unwrap(),
            repo::change_of(&body, FileKind::Regular),
        )]),
        extra: BTreeMap::new(),
    };
    let id = ops
        .create_changeset(&cs)
        .await?
        .map_err(|_| api::Fail::Bad)?;
    let sig = key.sign(&signing_message(&id));
    let sb = serde_json::to_vec(&AddSignatureRequest {
        public_key: format_public_key(key.verifying_key().as_bytes()),
        signature: hex::encode(sig.to_bytes()),
    })
    .unwrap();
    let p = format!("/v1/changesets/{id}/signatures");
    let r = ops
        .call("sign", reqwest::Method::POST, &p, Some(&sb))
        .await?;
    ops.want("sign", &p, r, &[200])?;
    let rp = format!("/v1/changesets/{id}/reviews");
    let rb = serde_json::to_vec(&AddReviewRequest {
        verdict: Verdict::Approve,
        comment: "root".into(),
        reviewer: None,
        source: None,
    })
    .unwrap();
    let r = reviewer
        .call("review", reqwest::Method::POST, &rp, Some(&rb))
        .await?;
    reviewer.want("review", &rp, r, &[200])?;
    match ops.land(id, "release").await? {
        api::Landed::Landed { .. } => Ok(()),
        other => {
            sh.violate("setup", format!("release root did not land: {other:?}"));
            Err(api::Fail::Bad)
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn finish(
    args: &Args,
    sh: &Arc<Shared>,
    out: &Path,
    trends: &Arc<std::sync::Mutex<Trends>>,
    gc_runs: &Arc<std::sync::Mutex<Vec<ops::GcRun>>>,
    derive_runs: &Arc<std::sync::Mutex<Vec<ops::DeriveRun>>>,
    checker: &Arc<tokio::sync::Mutex<Checker>>,
    server: Option<&Server>,
    layout: repo::Layout,
    base_url: &str,
    backend: &str,
) -> Result<bool> {
    let _ = server;
    let c = checker.lock().await;
    let ok = report::write(report::Inputs {
        args,
        sh,
        out,
        trends,
        gc_runs,
        derive_runs,
        checker_counts: &c.counts,
        nodes_known: c.nodes_known(),
        layout,
        base_url,
        backend,
    })?;
    Ok(ok)
}
