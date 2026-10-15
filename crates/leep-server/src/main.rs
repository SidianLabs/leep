//! `leep-server` binary: wires stores from URLs and serves the HTTP API.

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Context;
use clap::{Parser, Subcommand};
use leep_core::BlobStore;
use leep_derive::Repo;
use leep_server::auth::{self, Auth, Role};
use leep_server::limits::{Defaults as LimitDefaults, Limiter};
use leep_server::review::{RequireReview, ReviewPolicy, ReviewService};
use leep_server::signing::{RequireSignatures, SigningKeys};
use leep_store::CachedBlobStore;
use tracing_subscriber::EnvFilter;

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
    /// In-memory blob cache size in bytes; 0 disables it.
    #[arg(long, default_value_t = 256 * 1024 * 1024)]
    cache_bytes: usize,
    /// Abort requests that run longer than this (0, 90s, 5m); 0 disables the
    /// bound. `gc --fence` must exceed it.
    #[arg(long, env = "LEEP_REQUEST_TIMEOUT", default_value = "5m", value_parser = parse_grace)]
    request_timeout: std::time::Duration,
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
    /// JSON file of `blake3:` token digests; enables authentication. Read once
    /// at startup.
    #[arg(long, env = "LEEP_TOKENS_FILE")]
    tokens_file: Option<std::path::PathBuf>,
    /// JSON file of trusted Ed25519 author keys (`ed25519:<hex>`); enables
    /// `POST /v1/changesets/{id}/signatures`. Read once at startup.
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
    /// once at startup.
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
    /// Compute the code search index (trigram rows) for a revision, so the
    /// first `leep search` on it is fast. Uses --meta and --blobs; safe beside
    /// running servers, and cheap to repeat: only missing rows are computed.
    DeriveSearch {
        /// Ref name or full changeset id.
        rev: String,
    },
}

#[derive(Subcommand, Clone, Copy)]
enum GcPhase {
    /// Mark unreferenced blobs condemned; deletes nothing.
    Condemn,
    /// Delete blobs whose condemnation is at least --fence old and unrescued.
    Collect,
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
    let meta = leep_meta::open_meta_store(meta)
        .await
        .context("opening meta store")?;
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
    let meta = leep_meta::open_meta_store(meta)
        .await
        .context("opening meta store")?;
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
        Cmd::Gc { .. } | Cmd::DeriveSearch { .. } => unreachable!("handled in main"),
        Cmd::GenToken { name, role } => {
            let (token, entry) = auth::generate(&name, role).map_err(anyhow::Error::msg)?;
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
    match args.log_format {
        LogFormat::Text => tracing_subscriber::fmt().with_env_filter(filter).init(),
        LogFormat::Json => tracing_subscriber::fmt()
            .json()
            .with_env_filter(filter)
            .init(),
    }
    match args.command {
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
        Some(Cmd::DeriveSearch { rev }) => {
            return run_derive_search(&args.meta, &args.blobs, &rev).await
        }
        Some(cmd) => return run_command(cmd),
        None => {}
    }
    let metrics_app = args.metrics_listen.map(|_| install_metrics()).transpose()?;
    let auth = match &args.tokens_file {
        Some(path) => {
            let a = Auth::from_file(path).map_err(anyhow::Error::msg)?;
            tracing::info!(tokens = a.len(), "authentication enabled");
            Some(a)
        }
        None => {
            tracing::warn!("authentication disabled: no --tokens-file; anyone who can reach this server can read and write");
            None
        }
    };
    let limiter = Limiter::build(
        auth.as_ref(),
        &LimitDefaults {
            rate: args.rate_limit,
            burst: args.rate_burst,
            concurrency: args.max_concurrent_per_token,
            read_max_body: args.read_max_body_bytes,
        },
    )
    .map_err(anyhow::Error::msg)
    .context("request limits")?
    .map(Arc::new);
    if let Some(l) = &limiter {
        tracing::info!(
            limited_tokens = l.limited_tokens(),
            anonymous = l.limits_anonymous(),
            "request limits enabled"
        );
    }
    let signing = match &args.signing_keys_file {
        Some(path) => {
            let k = SigningKeys::from_file(path).map_err(anyhow::Error::msg)?;
            tracing::info!(keys = k.len(), "signature verification enabled");
            Some(Arc::new(k))
        }
        None if !args.require_signatures.is_empty() => {
            anyhow::bail!("--require-signatures needs --signing-keys-file")
        }
        None => None,
    };
    let policy = match &args.review_policy_file {
        Some(path) => ReviewPolicy::from_file(path).map_err(anyhow::Error::msg)?,
        None => ReviewPolicy::parse("{}").map_err(anyhow::Error::msg)?,
    };
    let mut review_refs = args.require_review.clone();
    for r in &policy.refs {
        if !review_refs.contains(r) {
            review_refs.push(r.clone());
        }
    }
    leep_server::check_protected_refs("--require-signatures", &args.require_signatures)
        .map_err(anyhow::Error::msg)?;
    leep_server::check_protected_refs("--require-review or the review policy", &review_refs)
        .map_err(anyhow::Error::msg)?;
    let review = match &auth {
        Some(a) => Some(Arc::new(ReviewService::new(a.reviewers(), policy.rules))),
        None if !review_refs.is_empty() || args.review_policy_file.is_some() => {
            anyhow::bail!("--require-review and --review-policy-file need --tokens-file")
        }
        None => None,
    };
    ensure_local_dirs(&args.meta, &args.blobs)?;

    let mut blobs = leep_store::open_blob_store(&args.blobs).context("opening blob store")?;
    if args.cache_bytes > 0 {
        blobs =
            Arc::new(CachedBlobStore::new(ArcBlobs(blobs), args.cache_bytes)) as Arc<dyn BlobStore>;
    }
    let meta = leep_meta::open_meta_store(&args.meta)
        .await
        .context("opening meta store")?;
    let mut repo = Repo::new(blobs, meta);
    if let (Some(keys), false) = (&signing, args.require_signatures.is_empty()) {
        tracing::info!(refs = ?args.require_signatures, "signatures required to land");
        repo = repo.with_land_policy(Arc::new(RequireSignatures::new(
            keys.clone(),
            args.require_signatures.clone(),
        )));
    }
    if let (Some(service), false) = (&review, review_refs.is_empty()) {
        tracing::info!(refs = ?review_refs, "review required to land");
        repo = repo.with_land_policy(Arc::new(RequireReview::new(
            service.clone(),
            review_refs.clone(),
        )));
    }
    for name in args.require_signatures.iter().chain(&review_refs) {
        if repo.meta().get_ref(name).await?.is_none() {
            tracing::warn!(
                name = %name,
                "protected ref does not exist yet; check the name if it should"
            );
        }
    }
    if args.request_timeout.is_zero() {
        tracing::warn!("request timeout disabled: gc --fence cannot be guaranteed safe");
    }
    let app = leep_server::router_with_limits(
        repo,
        args.max_body_bytes,
        auth,
        signing,
        review,
        args.request_timeout,
        limiter,
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
