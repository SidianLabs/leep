//! `leep-server` binary: wires stores from URLs and serves the HTTP API.

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Context;
use clap::{Parser, Subcommand};
use leep_core::BlobStore;
use leep_derive::Repo;
use leep_server::auth::{self, Role};
use leep_server::limits::Defaults as LimitDefaults;
use leep_server::reload::{ConfigHandle, ReloadSources, RuntimeConfig};
use leep_store::CachedBlobStore;
use tracing_subscriber::fmt::writer::BoxMakeWriter;
use tracing_subscriber::EnvFilter;

mod backup_cli;
mod repack_cli;
mod repair;

#[derive(Parser)]
#[command(version, about = "Leep HTTP server")]
struct Args {
    /// Address to listen on.
    #[arg(long, env = "LEEP_LISTEN", default_value = "127.0.0.1:7070")]
    listen: SocketAddr,
    /// Metadata store URL (sqlite:, postgres://, mem://).
    #[arg(
        long,
        global = true,
        env = "LEEP_META",
        default_value = "sqlite://./leep-data/meta.db?mode=rwc"
    )]
    meta: String,
    /// Blob store URL (file://, s3://, mem://).
    #[arg(
        long,
        global = true,
        env = "LEEP_BLOBS",
        default_value = "file://./leep-data/blobs"
    )]
    blobs: String,
    /// Refuse to start (or run gc, derive-search) when schema migrations are
    /// pending, instead of applying them; run `leep-server migrate` first. A
    /// database newer than this binary is refused either way.
    #[arg(
        long,
        global = true,
        env = "LEEP_NO_MIGRATE",
        value_parser = clap::builder::BoolishValueParser::new(),
    )]
    no_migrate: bool,
    /// In-memory blob cache size in bytes; 0 disables it.
    #[arg(long, default_value_t = 256 * 1024 * 1024)]
    cache_bytes: usize,
    /// Abort requests that run longer than this (0, 90s, 5m); 0 disables the
    /// bound. `gc --fence` must exceed it.
    #[arg(long, env = "LEEP_REQUEST_TIMEOUT", default_value = "5m", value_parser = parse_grace)]
    request_timeout: std::time::Duration,
    /// How long `GET /v1/ready` waits for each store (metadata and blobs,
    /// checked concurrently) before reporting it down (500ms, 2s).
    #[arg(long, env = "LEEP_READY_TIMEOUT", default_value = "2s", value_parser = parse_ready_timeout)]
    ready_timeout: std::time::Duration,
    /// How often to look for moved refs and derive their indexes in the
    /// background (ADR 0028), so queries find them ready; 0 turns warming off
    /// and indexes are derived by the first query that needs them.
    #[arg(long, env = "LEEP_WARM_INTERVAL", default_value = "30s", value_parser = parse_grace)]
    warm_interval: std::time::Duration,
    /// Also warm the code-search index of each ref head.
    #[arg(
        long,
        env = "LEEP_WARM_SEARCH",
        default_value_t = true,
        action = clap::ArgAction::Set,
        value_parser = clap::builder::BoolishValueParser::new(),
    )]
    warm_search: bool,
    /// Maximum request body size in bytes.
    #[arg(long, default_value_t = leep_server::DEFAULT_MAX_BODY_BYTES)]
    max_body_bytes: usize,
    /// Requests per second each token may make, counted separately for reads
    /// and writes (anonymous mode: per client address). Off when unset; a
    /// token entry's `rate` overrides it.
    #[arg(long, env = "LEEP_RATE_LIMIT")]
    rate_limit: Option<f64>,
    /// Requests a token may make at once before --rate-limit applies.
    /// Defaults to the rate rounded up. Needs --rate-limit.
    #[arg(long, env = "LEEP_RATE_BURST")]
    rate_burst: Option<u32>,
    /// Requests each token may have in flight; more get 429. Off when unset;
    /// a token entry's `concurrency` overrides it.
    #[arg(long, env = "LEEP_MAX_CONCURRENT_PER_TOKEN")]
    max_concurrent_per_token: Option<usize>,
    /// Maximum request body size in bytes for `read` tokens, which should not
    /// upload; a token entry's `max_body` overrides it. Needs --tokens-file.
    #[arg(long, env = "LEEP_READ_MAX_BODY_BYTES")]
    read_max_body_bytes: Option<usize>,
    /// JSON file of `blake3:` token digests; enables authentication. Read at
    /// startup and again on SIGHUP.
    #[arg(long, env = "LEEP_TOKENS_FILE")]
    tokens_file: Option<std::path::PathBuf>,
    /// JSON file of trusted Ed25519 author keys (`ed25519:<hex>`); enables
    /// `POST /v1/changesets/{id}/signatures`. Read at startup and on SIGHUP.
    #[arg(long, env = "LEEP_SIGNING_KEYS_FILE")]
    signing_keys_file: Option<std::path::PathBuf>,
    /// Refuse to land onto this ref unless every changeset it would add is
    /// signed by a trusted key. Repeatable; the environment variable takes a
    /// comma-separated list. Needs --signing-keys-file.
    #[arg(long, env = "LEEP_REQUIRE_SIGNATURES", value_delimiter = ',')]
    require_signatures: Vec<String>,
    /// JSON review policy: refs that require review and the auto-approval
    /// rules (`comment_only`, `whitespace_only`, `docs_only`). Its `refs` are
    /// protected as if listed in --require-review. Needs --tokens-file. Read
    /// at startup and on SIGHUP.
    #[arg(long, env = "LEEP_REVIEW_POLICY_FILE")]
    review_policy_file: Option<std::path::PathBuf>,
    /// Refuse to land onto this ref unless every changeset it would add is
    /// approved by the owners of the paths it changes. Repeatable; the
    /// environment variable takes a comma-separated list. Needs --tokens-file.
    #[arg(long, env = "LEEP_REQUIRE_REVIEW", value_delimiter = ',')]
    require_review: Vec<String>,
    /// Serve Prometheus metrics on this address (`GET /metrics`, no auth, keep
    /// it private). Off when unset.
    #[arg(long, env = "LEEP_METRICS_LISTEN")]
    metrics_listen: Option<SocketAddr>,
    /// Log output format.
    #[arg(long, env = "LEEP_LOG_FORMAT", value_enum, default_value_t = LogFormat::Text)]
    log_format: LogFormat,
    #[command(subcommand)]
    command: Option<Cmd>,
}

#[derive(Clone, Copy, clap::ValueEnum)]
enum LogFormat {
    Text,
    Json,
}

#[derive(Subcommand)]
enum Cmd {
    /// Read a token from stdin and print its `blake3:` digest.
    HashToken,
    /// Print a fresh random token (once) and its tokens-file entry.
    GenToken {
        /// Name recorded in the file and in logs.
        #[arg(long)]
        name: String,
        /// `read` or `write`.
        #[arg(long)]
        role: Role,
        /// Author for changesets made with this token when the client names
        /// none, e.g. "Name <email>".
        #[arg(long)]
        author: Option<String>,
    },
    /// Delete blobs no changeset references (uses --meta and --blobs; safe
    /// beside running servers). Without a phase: condemn, wait --fence, then
    /// collect.
    Gc {
        #[command(subcommand)]
        phase: Option<GcPhase>,
        /// Report what would be condemned or deleted without writing anything.
        #[arg(long, global = true)]
        dry_run: bool,
        /// Do not condemn unreferenced blobs written or claimed within this
        /// long (0, 30m, 24h, 7d). Must exceed the time clients take from
        /// upload to commit.
        #[arg(long, global = true, default_value = "24h", value_parser = parse_grace)]
        grace: std::time::Duration,
        /// Delete only blobs condemned at least this long ago. Must exceed
        /// the servers' --request-timeout plus clock skew.
        #[arg(long, global = true, default_value = "10m", value_parser = parse_grace)]
        fence: std::time::Duration,
        /// Allow --fence 0. Only safe while no server is running.
        #[arg(long, global = true)]
        unsafe_no_fence: bool,
    },
    /// Pack every live blob and tree node of a filesystem blob store into one
    /// delta-compressed pack (ADR 0029; uses --meta and --blobs; safe beside
    /// running servers): write and publish the pack, wait --fence, then
    /// delete the loose copies and old packs it replaces.
    Repack(repack_cli::RepackArgs),
    /// Apply pending metadata schema migrations (uses --meta). Safe beside
    /// running servers of the same version; take a backup first.
    Migrate {
        /// Print the SQL that would run, and change nothing.
        #[arg(long, conflicts_with = "status")]
        dry_run: bool,
        /// Print the schema version and migration history, and change nothing.
        #[arg(long)]
        status: bool,
    },
    /// Compute the code search index (trigram rows) for a revision, so the
    /// first `leep search` on it is fast. Uses --meta and --blobs; safe beside
    /// running servers, and cheap to repeat: only missing rows are computed.
    DeriveSearch {
        /// Ref name or full changeset id.
        rev: String,
    },
    /// Check a store without writing to it: every ref's history, tree nodes
    /// and blobs exist and match their addresses, the ref log replays to the
    /// refs, and derived rows equal a recomputation. Uses --meta and --blobs.
    /// Exit status 0 clean, 1 problems found, 2 could not run.
    Verify(repair::VerifyArgs),
    /// Rewrite or drop derived data (uses --meta and --blobs; safe beside
    /// running servers): `rederive` recomputes rows of a kind and writes the
    /// wrong ones, `drop` deletes a kind so it derives again on demand.
    Repair(repair::RepairArgs),
    /// Back up metadata and blobs consistently while servers run: a metadata
    /// snapshot first, then the blobs the backup lacks, then a check that
    /// every blob the snapshot needs was copied (ADR 0026). Uses --meta and
    /// --blobs as the source. Exit status 0 sealed and clean, 1 sealed but
    /// the check found problems, 2 could not run.
    Backup(backup_cli::BackupArgs),
    /// Restore a backup into empty stores (--meta and --blobs); refuses a
    /// non-empty target unless --force, and a backup from a newer schema.
    /// Exit status 0 restored and clean, 1 restored but the check found
    /// problems, 2 could not run.
    Restore(backup_cli::RestoreArgs),
    /// Check a backup without restoring it: its metadata file matches the
    /// manifest and every blob it references is present. Exit status 0 clean,
    /// 1 problems, 2 could not run.
    BackupVerify(backup_cli::VerifyBackupArgs),
}

#[derive(Subcommand, Clone, Copy)]
enum GcPhase {
    /// Mark unreferenced blobs condemned; deletes nothing.
    Condemn,
    /// Delete blobs whose condemnation is at least --fence old and unrescued.
    Collect,
}

/// Set once from `--no-migrate`; read by [`open_meta`].
static NO_MIGRATE: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// Open the metadata store, applying pending migrations unless `--no-migrate`.
async fn open_meta(url: &str) -> leep_core::Result<Arc<dyn leep_core::MetaStore>> {
    let mode = if NO_MIGRATE.load(std::sync::atomic::Ordering::Relaxed) {
        leep_meta::SchemaMode::RequireCurrent
    } else {
        leep_meta::SchemaMode::Migrate
    };
    leep_meta::open_meta_store_with(url, mode).await
}

async fn run_migrate(meta: &str, dry_run: bool, status: bool) -> anyhow::Result<()> {
    if !dry_run && !status {
        ensure_local_dirs(meta, "")?;
    }
    let r = leep_meta::migrate_meta_store(meta, dry_run || status)
        .await
        .context("migrating meta store")?;
    if status {
        println!("schema version: {} (this server: {})", r.from, r.latest);
        for a in &r.applied {
            println!(
                "applied  v{:04} {} (unix {}, checksum {})",
                a.version,
                a.name,
                a.applied_at,
                &a.checksum[..12]
            );
        }
    }
    let verb = if r.performed { "applied" } else { "pending" };
    for m in &r.adopted {
        let verb = if r.performed { "adopted" } else { "to adopt" };
        println!("{verb:<8} v{:04} {} (already in place)", m.version, m.name);
    }
    for m in &r.pending {
        println!("{verb:<8} v{:04} {}", m.version, m.name);
        if dry_run {
            println!("{}", m.sql.trim_end());
        }
    }
    if r.is_current() && !r.performed {
        println!("schema is up to date at v{}", r.from);
    } else if r.performed {
        println!("schema migrated from v{} to v{}", r.from, r.to);
    } else {
        println!(
            "{} change(s) to apply to reach v{}; run `leep-server migrate`",
            r.adopted.len() + r.pending.len(),
            r.latest
        );
    }
    Ok(())
}

/// `500ms`, `2s`, `1m`; a bare number is seconds. Zero would fail every probe.
fn parse_ready_timeout(s: &str) -> Result<std::time::Duration, String> {
    let d = match s.trim().strip_suffix("ms") {
        Some(n) => std::time::Duration::from_millis(
            n.parse()
                .map_err(|_| format!("bad duration {s:?}; use e.g. 500ms, 2s"))?,
        ),
        None => parse_grace(s)?,
    };
    if d.is_zero() {
        return Err("must be greater than zero".into());
    }
    Ok(d)
}

fn parse_grace(s: &str) -> Result<std::time::Duration, String> {
    leep_derive::gc::parse_duration(s).map_err(|e| e.to_string())
}

async fn run_gc(
    meta: &str,
    blobs: &str,
    phase: Option<GcPhase>,
    opts: leep_derive::gc::GcOptions,
) -> anyhow::Result<()> {
    use leep_derive::gc;
    let blobs = leep_store::open_blob_store(blobs).context("opening blob store")?;
    let meta = open_meta(meta).await.context("opening meta store")?;
    let repo = Repo::new(blobs, meta);
    let clock = gc::SystemClock;
    let dry = opts.dry_run;
    let (condemn, collect) = match phase {
        Some(GcPhase::Condemn) => (Some(gc::condemn(&repo, &opts, &clock).await?), None),
        Some(GcPhase::Collect) => (None, Some(gc::collect(&repo, &opts, &clock).await?)),
        None => {
            if !dry {
                tracing::info!(fence = ?opts.fence, "condemning, then waiting out the fence");
            }
            let r = gc::run(&repo, &opts, &clock).await?;
            (Some(r.condemn), (!dry).then_some(r.collect))
        }
    };
    if let Some(r) = condemn {
        let verb = if dry { "would condemn" } else { "condemned" };
        println!("changesets scanned: {}", r.changesets_scanned);
        println!("referenced blobs and trees: {}", r.marked);
        println!("blobs scanned: {}", r.blobs_scanned);
        println!("{verb}: {} blobs, {} bytes", r.condemned, r.condemned_bytes);
        println!("already condemned: {}", r.already_condemned);
        println!("kept (within grace): {}", r.kept_by_grace);
        println!("kept (unknown age): {}", r.kept_unknown_mtime);
        println!("kept (recently claimed): {}", r.kept_by_claim);
    }
    if let Some(r) = collect {
        let verb = if dry { "would delete" } else { "deleted" };
        println!("collect: condemnations examined: {}", r.condemned);
        println!("collect: newer than fence, left alone: {}", r.fence_pending);
        println!("collect: rescued by upload claim: {}", r.rescued_by_claim);
        println!("collect: rescued by reference: {}", r.rescued_by_reference);
        println!("collect: rescued by newer write: {}", r.rescued_by_upload);
        println!("{verb}: {} blobs, {} bytes", r.deleted, r.deleted_bytes);
    }
    Ok(())
}

async fn run_derive_search(meta: &str, blobs: &str, rev: &str) -> anyhow::Result<()> {
    let blobs = leep_store::open_blob_store(blobs).context("opening blob store")?;
    let meta = open_meta(meta).await.context("opening meta store")?;
    let repo = Repo::new(blobs, meta);
    let id = repo.resolve(rev).await?;
    let started = std::time::Instant::now();
    let r = repo.derive_search(id).await?;
    println!("changeset: {id}");
    println!("blob rows derived: {}", r.blob_rows);
    println!("tree rows derived: {}", r.tree_rows);
    println!("elapsed: {:.1}s", started.elapsed().as_secs_f64());
    Ok(())
}

fn run_command(cmd: Cmd) -> anyhow::Result<()> {
    match cmd {
        Cmd::HashToken => {
            let mut token = String::new();
            std::io::Read::read_to_string(&mut std::io::stdin(), &mut token)?;
            if token.trim().is_empty() {
                anyhow::bail!("no token on stdin");
            }
            println!("{}", auth::hash_token(&token));
        }
        Cmd::Gc { .. }
        | Cmd::Repack(_)
        | Cmd::DeriveSearch { .. }
        | Cmd::Migrate { .. }
        | Cmd::Verify(_)
        | Cmd::Repair(_)
        | Cmd::Backup(_)
        | Cmd::Restore(_)
        | Cmd::BackupVerify(_) => {
            unreachable!("handled in main")
        }
        Cmd::GenToken { name, role, author } => {
            let (token, mut entry) = auth::generate(&name, role).map_err(anyhow::Error::msg)?;
            entry.author = author;
            println!("token: {token}");
            println!("entry: {}", serde_json::to_string(&entry)?);
            eprintln!("Store the token now; it cannot be recovered. Add the entry to the `tokens` array of your tokens file.");
        }
    }
    Ok(())
}

/// Create the parent directory a local `sqlite:` or `file://` URL points into.
fn ensure_local_dirs(meta: &str, blobs: &str) -> anyhow::Result<()> {
    if let Some(rest) = blobs.strip_prefix("file://") {
        std::fs::create_dir_all(rest).with_context(|| format!("creating {rest}"))?;
    }
    if let Some(rest) = meta.strip_prefix("sqlite:") {
        let path = rest.trim_start_matches("//");
        let path = path.split('?').next().unwrap_or(path);
        if path != ":memory:" && !path.is_empty() {
            if let Some(dir) = std::path::Path::new(path).parent() {
                std::fs::create_dir_all(dir).with_context(|| format!("creating {dir:?}"))?;
            }
        }
    }
    Ok(())
}

/// Install the global Prometheus recorder; returns the `/metrics` app.
fn install_metrics() -> anyhow::Result<axum::Router> {
    use metrics_exporter_prometheus::{Matcher, PrometheusBuilder};
    let handle = PrometheusBuilder::new()
        .set_buckets_for_metric(Matcher::Suffix("_seconds".into()), SECONDS_BUCKETS)?
        .set_buckets_for_metric(Matcher::Full("leep_land_attempts".into()), ATTEMPT_BUCKETS)?
        .install_recorder()
        .context("installing metrics recorder")?;
    leep_server::observe::record_build_info();
    let upkeep = handle.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            upkeep.run_upkeep();
        }
    });
    Ok(leep_server::observe::metrics_app(move || handle.render()))
}

const SECONDS_BUCKETS: &[f64] = &[
    0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];
const ATTEMPT_BUCKETS: &[f64] = &[1.0, 2.0, 3.0, 4.0, 6.0, 8.0, 12.0, 16.0, 24.0, 32.0];

async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let term = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut s) => {
                s.recv().await;
            }
            Err(_) => std::future::pending::<()>().await,
        }
    };
    #[cfg(not(unix))]
    let term = std::future::pending::<()>();
    tokio::select! {
        _ = ctrl_c => {},
        _ = term => {},
    }
    tracing::info!("shutting down");
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into());
    // These print their report on stdout, so logs go to stderr.
    let writer = if matches!(
        args.command,
        Some(
            Cmd::Verify(_)
                | Cmd::Repair(_)
                | Cmd::Backup(_)
                | Cmd::Restore(_)
                | Cmd::BackupVerify(_)
        )
    ) {
        BoxMakeWriter::new(std::io::stderr)
    } else {
        BoxMakeWriter::new(std::io::stdout)
    };
    match args.log_format {
        LogFormat::Text => tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_writer(writer)
            .init(),
        LogFormat::Json => tracing_subscriber::fmt()
            .json()
            .with_env_filter(filter)
            .with_writer(writer)
            .init(),
    }
    NO_MIGRATE.store(args.no_migrate, std::sync::atomic::Ordering::Relaxed);
    match args.command {
        Some(Cmd::Migrate { dry_run, status }) => {
            return run_migrate(&args.meta, dry_run, status).await
        }
        Some(Cmd::Gc {
            phase,
            dry_run,
            grace,
            fence,
            unsafe_no_fence,
        }) => {
            return run_gc(
                &args.meta,
                &args.blobs,
                phase,
                leep_derive::gc::GcOptions {
                    grace,
                    fence,
                    unsafe_no_fence,
                    dry_run,
                },
            )
            .await
        }
        Some(Cmd::Repack(r)) => return repack_cli::run_repack(&args.meta, &args.blobs, r).await,
        Some(Cmd::DeriveSearch { rev }) => {
            return run_derive_search(&args.meta, &args.blobs, &rev).await
        }
        Some(Cmd::Verify(v)) => {
            std::process::exit(repair::run_verify(&args.meta, &args.blobs, v).await)
        }
        Some(Cmd::Repair(r)) => {
            std::process::exit(repair::run_repair(&args.meta, &args.blobs, r).await)
        }
        Some(Cmd::Backup(b)) => {
            std::process::exit(backup_cli::run_backup(&args.meta, &args.blobs, b).await)
        }
        Some(Cmd::Restore(r)) => std::process::exit(
            backup_cli::run_restore(&args.meta, &args.blobs, args.no_migrate, r).await,
        ),
        Some(Cmd::BackupVerify(v)) => std::process::exit(backup_cli::run_verify_backup(v).await),
        Some(cmd) => return run_command(cmd),
        None => {}
    }
    let metrics_app = args.metrics_listen.map(|_| install_metrics()).transpose()?;
    let config = ConfigHandle::load(ReloadSources {
        tokens_file: args.tokens_file.clone(),
        signing_keys_file: args.signing_keys_file.clone(),
        review_policy_file: args.review_policy_file.clone(),
        limit_defaults: LimitDefaults {
            rate: args.rate_limit,
            burst: args.rate_burst,
            concurrency: args.max_concurrent_per_token,
            read_max_body: args.read_max_body_bytes,
        },
        require_signatures: args.require_signatures.clone(),
        require_review: args.require_review.clone(),
    })
    .map_err(anyhow::Error::msg)?;
    log_config(&config.current());
    ensure_local_dirs(&args.meta, &args.blobs)?;

    let mut blobs = leep_store::open_blob_store(&args.blobs).context("opening blob store")?;
    if args.cache_bytes > 0 {
        blobs =
            Arc::new(CachedBlobStore::new(ArcBlobs(blobs), args.cache_bytes)) as Arc<dyn BlobStore>;
    }
    let meta = open_meta(&args.meta).await.context("opening meta store")?;
    if let Some(v) = meta.schema_version().await? {
        metrics::gauge!("leep_schema_version").set(v as f64);
    }
    // The land policy comes from the reloadable config, not from the repo.
    let repo = Repo::new(blobs, meta);
    leep_server::reload::warn_missing_refs(repo.meta().as_ref(), &config.current()).await;
    if args.request_timeout.is_zero() {
        tracing::warn!("request timeout disabled: gc --fence cannot be guaranteed safe");
    }
    #[cfg(unix)]
    leep_server::reload::spawn_sighup_reload(config.clone(), repo.meta().clone())
        .context("installing the SIGHUP handler")?;
    if args.warm_interval.is_zero() {
        tracing::info!("index warming off: the first query on new history derives its indexes");
    } else {
        tracing::info!(
            interval_s = args.warm_interval.as_secs_f64(),
            search = args.warm_search,
            "warming indexes of moved refs in the background"
        );
        leep_derive::warm::Warmer::new(repo.clone(), args.warm_search).spawn(args.warm_interval);
    }
    let app = leep_server::router_with_ready_timeout(
        repo,
        args.max_body_bytes,
        args.request_timeout,
        config,
        args.ready_timeout,
    );

    let listener = tokio::net::TcpListener::bind(args.listen)
        .await
        .with_context(|| format!("binding {}", args.listen))?;
    tracing::info!(addr = %listener.local_addr()?, "leep-server listening");
    if let (Some(addr), Some(metrics_app)) = (args.metrics_listen, metrics_app) {
        let l = tokio::net::TcpListener::bind(addr)
            .await
            .with_context(|| format!("binding metrics {addr}"))?;
        tracing::info!(addr = %l.local_addr()?, "metrics listening on /metrics");
        tokio::spawn(async move {
            if let Err(e) = leep_server::serve(l, metrics_app, std::future::pending()).await {
                tracing::error!(error = %e, "metrics listener stopped");
            }
        });
    }
    leep_server::serve(listener, app, shutdown_signal()).await?;
    Ok(())
}

/// The startup summary of what the files configured. Counts only.
fn log_config(c: &RuntimeConfig) {
    tracing::info!(
        fingerprint = %c.fingerprint,
        "config loaded (servers that run the same config print the same fingerprint)"
    );
    match &c.auth {
        Some(a) => tracing::info!(tokens = a.len(), "authentication enabled"),
        None => tracing::warn!("authentication disabled: no --tokens-file; anyone who can reach this server can read and write"),
    }
    if let Some(l) = &c.limiter {
        tracing::info!(
            limited_tokens = l.limited_tokens(),
            anonymous = l.limits_anonymous(),
            "request limits enabled"
        );
    }
    if let Some(k) = &c.signing {
        tracing::info!(keys = k.len(), "signature verification enabled");
    }
    if !c.protected_refs().is_empty() {
        tracing::info!(refs = ?c.protected_refs(), "land policy protects refs");
    }
}

/// Lets the cache wrap an already-boxed store.
struct ArcBlobs(Arc<dyn BlobStore>);

#[async_trait::async_trait]
impl BlobStore for ArcBlobs {
    async fn put_with_hash(
        &self,
        hash: leep_core::Hash,
        data: bytes::Bytes,
    ) -> leep_core::Result<()> {
        self.0.put_with_hash(hash, data).await
    }
    async fn get(&self, hash: &leep_core::Hash) -> leep_core::Result<Option<bytes::Bytes>> {
        self.0.get(hash).await
    }
    async fn contains(&self, hash: &leep_core::Hash) -> leep_core::Result<bool> {
        self.0.contains(hash).await
    }
    async fn list_page(
        &self,
        after: Option<leep_core::Hash>,
        limit: usize,
    ) -> leep_core::Result<Vec<leep_core::BlobMeta>> {
        self.0.list_page(after, limit).await
    }
    async fn delete(&self, hash: &leep_core::Hash) -> leep_core::Result<()> {
        self.0.delete(hash).await
    }
}
