//! `leep-server` binary: wires stores from URLs and serves the HTTP API.

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Context;
use clap::Parser;
use leep_core::BlobStore;
use leep_derive::Repo;
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
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();
    let args = Args::parse();
    ensure_local_dirs(&args.meta, &args.blobs)?;

    let mut blobs = leep_store::open_blob_store(&args.blobs).context("opening blob store")?;
    if args.cache_bytes > 0 {
        blobs =
            Arc::new(CachedBlobStore::new(ArcBlobs(blobs), args.cache_bytes)) as Arc<dyn BlobStore>;
    }
    let meta = leep_meta::open_meta_store(&args.meta)
        .await
        .context("opening meta store")?;
    let app = leep_server::router_with_limit(Repo::new(blobs, meta), args.max_body_bytes);

    let listener = tokio::net::TcpListener::bind(args.listen)
        .await
        .with_context(|| format!("binding {}", args.listen))?;
    tracing::info!(addr = %listener.local_addr()?, "leep-server listening");
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
