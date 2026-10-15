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
    /// Maximum request body size in bytes.
    #[arg(long, default_value_t = leep_server::DEFAULT_MAX_BODY_BYTES)]
    max_body_bytes: usize,
    /// JSON file of `blake3:` token digests; enables authentication. Read once
    /// at startup.
    #[arg(long, env = "LEEP_TOKENS_FILE")]
    tokens_file: Option<std::path::PathBuf>,
    #[command(subcommand)]
    command: Option<Cmd>,
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
    /// beside running servers because of --grace).
    Gc {
        /// Report what would be deleted without deleting.
        #[arg(long)]
        dry_run: bool,
        /// Keep unreferenced blobs written within this long (0, 30m, 24h, 7d).
        /// Must exceed the time clients take from upload to commit.
        #[arg(long, default_value = "24h", value_parser = parse_grace)]
        grace: std::time::Duration,
    },
}

fn parse_grace(s: &str) -> Result<std::time::Duration, String> {
    leep_derive::gc::parse_duration(s).map_err(|e| e.to_string())
}

async fn run_gc(meta: &str, blobs: &str, opts: leep_derive::gc::GcOptions) -> anyhow::Result<()> {
    let blobs = leep_store::open_blob_store(blobs).context("opening blob store")?;
    let meta = leep_meta::open_meta_store(meta)
        .await
        .context("opening meta store")?;
    let dry = opts.dry_run;
    let r = leep_derive::gc::run(&Repo::new(blobs, meta), &opts).await?;
    let verb = if dry { "would delete" } else { "deleted" };
    println!("changesets scanned: {}", r.changesets_scanned);
    println!("referenced blobs and trees: {}", r.marked);
    println!("blobs scanned: {}", r.blobs_scanned);
    println!("{verb}: {} blobs, {} bytes", r.deleted, r.deleted_bytes);
    println!("kept (within grace): {}", r.kept_by_grace);
    println!("kept (unknown age): {}", r.kept_unknown_mtime);
    println!("kept (recently claimed): {}", r.kept_by_claim);
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
        Cmd::Gc { .. } => unreachable!("handled in main"),
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
    match args.command {
        Some(Cmd::Gc { dry_run, grace }) => {
            return run_gc(
                &args.meta,
                &args.blobs,
                leep_derive::gc::GcOptions { grace, dry_run },
            )
            .await
        }
        Some(cmd) => return run_command(cmd),
        None => {}
    }
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
