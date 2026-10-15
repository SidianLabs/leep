//! `leep-server` binary: wires stores from URLs and serves the HTTP API.

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Context;
use clap::{Parser, Subcommand};
use leep_core::BlobStore;
use leep_derive::Repo;
use leep_server::auth::{self, Auth, Role};
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
        env = "LEEP_META",
        default_value = "sqlite://./leep-data/meta.db?mode=rwc"
    )]
    meta: String,
    /// Blob store URL (file://, s3://, mem://).
    #[arg(long, env = "LEEP_BLOBS", default_value = "file://./leep-data/blobs")]
    blobs: String,
    /// In-memory blob cache size in bytes; 0 disables it.
    #[arg(long, default_value_t = 256 * 1024 * 1024)]
    cache_bytes: usize,
    /// Maximum request body size in bytes.
    #[arg(long, default_value_t = leep_server::DEFAULT_MAX_BODY_BYTES)]
    max_body_bytes: usize,
    /// JSON file of `blake3:` token digests; enables authentication. Read once
    /// at startup.
    #[arg(long, env = "LEEP_TOKENS_FILE")]
    tokens_file: Option<std::path::PathBuf>,
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
    if let Some(cmd) = args.command {
        return run_command(cmd);
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
    ensure_local_dirs(&args.meta, &args.blobs)?;

    let mut blobs = leep_store::open_blob_store(&args.blobs).context("opening blob store")?;
    if args.cache_bytes > 0 {
        blobs =
            Arc::new(CachedBlobStore::new(ArcBlobs(blobs), args.cache_bytes)) as Arc<dyn BlobStore>;
    }
    let meta = leep_meta::open_meta_store(&args.meta)
        .await
        .context("opening meta store")?;
    let app = leep_server::router_with_auth(Repo::new(blobs, meta), args.max_body_bytes, auth);

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
}
