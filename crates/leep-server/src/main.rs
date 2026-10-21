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
mod upgrade_cli;

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
    /// Most connections this process opens to a Postgres metadata store
    /// (SQLite ignores it). Every server and tool has its own pool, so keep
    /// their sum under the database's max_connections.
    #[arg(
        long,
        global = true,
        env = "LEEP_META_POOL_SIZE",
        default_value_t = leep_meta::DEFAULT_POSTGRES_POOL_SIZE,
        value_parser = clap::value_parser!(u32).range(1..=10_000),
    )]
    meta_pool_size: u32,
    /// Bytes the in-memory caches of derived indexes (path history, trees,
    /// filtered log) may hold; the ancestry and skiplist caches scale with
    /// it. Separate from --cache-bytes, which caches blobs.
    #[arg(
        long,
        global = true,
        env = "LEEP_DERIVE_CACHE_BYTES",
        default_value_t = leep_derive::DEFAULT_CACHE_BYTES
    )]
    derive_cache_bytes: usize,
    /// Abort requests that run longer than this (0, 90s, 5m); 0 disables the
    /// bound. `gc --fence` must exceed it.
    #[arg(long, env = "LEEP_REQUEST_TIMEOUT", default_value = "5m", value_parser = parse_grace)]
    request_timeout: std::time::Duration,
    /// How long `GET /v1/ready` waits for each store (metadata and blobs,
    /// checked concurrently) before reporting it down (500ms, 2s).
    #[arg(long, env = "LEEP_READY_TIMEOUT", default_value = "2s", value_parser = parse_ready_timeout)]
    ready_timeout: std::time::Duration,
    /// How often to read the ref log for moved refs and derive their indexes
    /// in the background (ADR 0028, ADR 0058), so queries find them ready;
    /// every ref is also listed every 5 s. 0 turns warming off and indexes
    /// are derived by the first query that needs them.
    #[arg(long, env = "LEEP_WARM_INTERVAL", default_value = "250ms", value_parser = parse_interval)]
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
    /// Changesets a derivation of a missing range (the warmer's catch-up, or
    /// the first query over un-derived history) works on at once (ADR 0050).
    /// Default: the cores, up to 8.
    #[arg(long, env = "LEEP_DERIVE_JOBS", value_parser = clap::value_parser!(u32).range(1..=256))]
    derive_jobs: Option<u32>,
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
    /// Defaults to 1 MiB when there is one.
    #[arg(long, env = "LEEP_READ_MAX_BODY_BYTES")]
    read_max_body_bytes: Option<usize>,
    /// JSON file of `blake3:` token digests; enables authentication. Read at
    /// startup and again on SIGHUP.
    #[arg(long, env = "LEEP_TOKENS_FILE")]
    tokens_file: Option<std::path::PathBuf>,
    /// Serve without authentication on an address other hosts can reach.
    /// Without it, a non-loopback --listen needs --tokens-file.
    #[arg(
        long,
        env = "LEEP_INSECURE_NO_AUTH",
        value_parser = clap::builder::BoolishValueParser::new(),
    )]
    insecure_no_auth: bool,
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
    /// Batch the lands of each ref on one server across every server that
    /// shares this metadata store (ADR 0056): `on` takes a per-ref lease in
    /// the store and forwards lands to the ref's lease holder. Turn it on for
    /// every server of a multi-server Postgres deployment; needs
    /// --advertise-url and --peer-url-prefix or --peer-cidr.
    #[arg(long, env = "LEEP_LAND_COORDINATION", value_enum, default_value_t = Coordination::Off)]
    land_coordination: Coordination,
    /// URL other servers reach this one at directly (not through the load
    /// balancer), e.g. `http://10.0.3.7:7070`. Checked at startup.
    #[arg(long, env = "LEEP_ADVERTISE_URL")]
    advertise_url: Option<String>,
    /// Servers' URLs start with this; lands, with their client's token, are
    /// forwarded only to an address that starts with one of these or whose
    /// IP address is in a --peer-cidr. Repeatable; the environment variable
    /// takes a comma-separated list.
    #[arg(long, env = "LEEP_PEER_URL_PREFIX", value_delimiter = ',')]
    peer_url_prefix: Vec<String>,
    /// Networks the servers are in, e.g. `10.0.0.0/16`. Requests from these
    /// addresses may carry forwarded lands. Repeatable.
    #[arg(long, env = "LEEP_PEER_CIDR", value_delimiter = ',')]
    peer_cidr: Vec<leep_server::coord::Cidr>,
    /// Allow forwarding over plain http to hosts that are not loopback or
    /// private addresses (a client's token crosses that network).
    #[arg(long, env = "LEEP_PEER_ALLOW_PLAIN_HTTP")]
    peer_allow_plain_http: bool,
    /// Another server takes over a land lease not renewed for this long;
    /// the holder stops trusting it one renew interval earlier.
    #[arg(long, env = "LEEP_LAND_LEASE_TTL", default_value = "2s", value_parser = parse_ready_timeout)]
    land_lease_ttl: std::time::Duration,
    /// How often held land leases are renewed.
    #[arg(long, env = "LEEP_LAND_LEASE_RENEW", default_value = "500ms", value_parser = parse_ready_timeout)]
    land_lease_renew: std::time::Duration,
    /// Who builds the indexes (ADR 0058). `local`: this server warms every
    /// index and queries derive what they miss. `lease`: one server per
    /// index group, the holder of the group's lease in the metadata store,
    /// warms it. `off`: never warm. In `lease` and `off` modes queries never
    /// derive an index while a lease lives: they scan a bounded amount of
    /// un-indexed history and otherwise answer 503 with Retry-After.
    #[arg(long, env = "LEEP_INDEXING", value_enum, default_value_t = Indexing::Local)]
    indexing: Indexing,
    /// Index groups this server competes for in `lease` mode: `history`,
    /// `log-filter`, `search` or their lease names. Default: all.
    #[arg(long, env = "LEEP_INDEX_KINDS", value_delimiter = ',')]
    index_kinds: Vec<String>,
    /// Un-indexed changesets a path log, last-change or filtered log examines
    /// before answering that its index is behind (`lease` and `off` modes).
    #[arg(long, env = "LEEP_QUERY_SCAN_BUDGET", default_value_t = 1024)]
    query_scan_budget: usize,
    /// Un-indexed first-parent steps (file reads) a blame walks.
    #[arg(long, env = "LEEP_BLAME_SCAN_BUDGET", default_value_t = 128)]
    blame_scan_budget: usize,
    /// Files without a code-search row a search fetches.
    #[arg(long, env = "LEEP_SEARCH_SCAN_BUDGET", default_value_t = 512)]
    search_scan_budget: usize,
    /// Bytes of files without a code-search row a search fetches.
    #[arg(long, env = "LEEP_SEARCH_SCAN_BYTES", default_value_t = 16 << 20)]
    search_scan_bytes: u64,
    /// Requests without a `Leep-Indexing` header scan this many times the
    /// budgets once their wait for the indexer is over.
    #[arg(long, env = "LEEP_LEGACY_SCAN_FACTOR", default_value_t = 8, value_parser = clap::value_parser!(u32).range(1..=1024))]
    legacy_scan_factor: u32,
    /// How long a request without a `Leep-Indexing` header waits for the
    /// indexer to catch up, at most (also bounded by its remaining time).
    #[arg(long, env = "LEEP_INDEX_WAIT", default_value = "10s", value_parser = parse_grace)]
    index_wait: std::time::Duration,
    /// Requests that may wait for the indexer at once; others do not wait.
    #[arg(long, env = "LEEP_INDEX_WAITERS", default_value_t = 64)]
    index_waiters: usize,
    #[command(subcommand)]
    command: Option<Cmd>,
}

#[derive(Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
enum Coordination {
    On,
    Off,
}

#[derive(Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
enum Indexing {
    Local,
    Lease,
    Off,
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
        /// Make the token stop working this long from now (30m, 24h, 90d);
        /// the entry gets an `expires` time. Never expires when unset.
        #[arg(long, value_parser = parse_ready_timeout)]
        expires_in: Option<std::time::Duration>,
        /// Keep this token from posting reviews (`"review": false`); its
        /// reviews never count. For agent tokens.
        #[arg(long)]
        no_review: bool,
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
        /// Walk all of history and every tree instead of only what earlier
        /// runs did not mark (ADR 0033), and rewrite their record.
        #[arg(long, global = true)]
        full: bool,
    },
    /// Pack every live blob and tree node of a filesystem or s3:// blob store
    /// into one delta-compressed pack (ADR 0029, ADR 0040; uses --meta and
    /// --blobs; safe beside
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
    /// Compress stored changesets with a dictionary trained on this store's
    /// own history (ADR 0053; uses --meta). The first run trains and stores
    /// the dictionary, then rewrites every plain row; later runs compress
    /// rows written plain since. Safe beside running servers of this version
    /// or newer, never beside older ones (they cannot read compressed rows).
    CompactChangesets {
        /// Report the sizes before and after, and write nothing.
        #[arg(long)]
        dry_run: bool,
        /// Train a new dictionary even if one exists and recompress every
        /// row with it (old dictionaries stay; nothing deletes one).
        #[arg(long)]
        retrain: bool,
        /// Largest dictionary to train, in bytes.
        #[arg(long, default_value_t = leep_meta::compact::CompactOptions::default().dict_size)]
        dict_size: usize,
        /// Changesets to train on: the first ones by id, a uniform sample.
        #[arg(long, default_value_t = leep_meta::compact::CompactOptions::default().samples)]
        samples: usize,
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
    /// Bring the derived indexes up to this version in one resumable command
    /// (uses --meta and --blobs; safe beside running servers): plan from
    /// counts, derive what is missing, drop retired kinds only with
    /// --drop-retired --old-servers-stopped, and print how to reclaim space.
    /// Without --yes it only plans. Exit status 0 done or nothing to do, 1
    /// work to do, 2 could not run.
    UpgradeIndex(upgrade_cli::UpgradeIndexArgs),
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

/// Set once from `--meta-pool-size`; read by [`open_meta`].
static META_POOL_SIZE: std::sync::atomic::AtomicU32 =
    std::sync::atomic::AtomicU32::new(leep_meta::DEFAULT_POSTGRES_POOL_SIZE);

/// Set once from `--derive-cache-bytes`; read by [`new_repo`].
static DERIVE_CACHE_BYTES: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(leep_derive::DEFAULT_CACHE_BYTES);

/// A repository handle with the derive cache budget from the flags.
fn new_repo(blobs: Arc<dyn BlobStore>, meta: Arc<dyn leep_core::MetaStore>) -> Repo {
    let budget = DERIVE_CACHE_BYTES.load(std::sync::atomic::Ordering::Relaxed);
    if budget == leep_derive::DEFAULT_CACHE_BYTES {
        Repo::new(blobs, meta)
    } else {
        Repo::new(blobs, meta).with_cache_budget(budget)
    }
}

/// Open the metadata store, applying pending migrations unless `--no-migrate`.
async fn open_meta(url: &str) -> leep_core::Result<Arc<dyn leep_core::MetaStore>> {
    let mode = if NO_MIGRATE.load(std::sync::atomic::Ordering::Relaxed) {
        leep_meta::SchemaMode::RequireCurrent
    } else {
        leep_meta::SchemaMode::Migrate
    };
    let pool = META_POOL_SIZE.load(std::sync::atomic::Ordering::Relaxed);
    leep_meta::open_meta_store_with_pool(url, mode, pool).await
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

async fn run_compact(meta: &str, opts: leep_meta::compact::CompactOptions) -> anyhow::Result<()> {
    let mode = if NO_MIGRATE.load(std::sync::atomic::Ordering::Relaxed) {
        leep_meta::SchemaMode::RequireCurrent
    } else {
        leep_meta::SchemaMode::Migrate
    };
    let start = std::time::Instant::now();
    let mut last = std::time::Instant::now();
    let mut progress = |r: &leep_meta::compact::CompactReport| {
        if last.elapsed() >= std::time::Duration::from_secs(10) {
            last = std::time::Instant::now();
            println!(
                "{:>8.0} s  {} rows, {} rewritten, {} -> {} bytes",
                start.elapsed().as_secs_f64(),
                r.rows,
                r.rewritten,
                r.bytes_before,
                r.bytes_after
            );
        }
    };
    let r = leep_meta::compact::compact_changesets(meta, mode, &opts, &mut progress)
        .await
        .context("compacting changesets")?;
    if r.dictionary == 0 {
        println!(
            "fewer than {} changesets: nothing to train a dictionary on, nothing changed",
            leep_meta::compact::MIN_TRAINING_ROWS
        );
        return Ok(());
    }
    if r.trained {
        let verb = if opts.dry_run {
            "would store"
        } else {
            "stored"
        };
        println!(
            "{verb} dictionary {} ({} bytes) trained on {} changesets",
            r.dictionary, r.dictionary_bytes, r.samples
        );
    }
    let verb = if opts.dry_run {
        "would rewrite"
    } else {
        "rewrote"
    };
    println!(
        "{verb} {} of {} changeset rows in {:.1} s: {} -> {} bytes ({:.2}x)",
        r.rewritten,
        r.rows,
        start.elapsed().as_secs_f64(),
        r.bytes_before,
        r.bytes_after,
        r.bytes_before as f64 / r.bytes_after.max(1) as f64
    );
    if !opts.dry_run && r.rewritten > 0 && meta.starts_with("postgres") {
        println!(
            "the old row versions are free space now; `VACUUM FULL changesets` returns it to the \
             filesystem (it locks the table)"
        );
    }
    Ok(())
}

/// `500ms`, `2s`, `1m`; a bare number is seconds. Zero would fail every probe.
/// A duration that may be given in milliseconds (250ms) as well as the
/// units of [`parse_grace`].
fn parse_interval(s: &str) -> Result<std::time::Duration, String> {
    match s.trim().strip_suffix("ms") {
        Some(n) => Ok(std::time::Duration::from_millis(
            n.parse()
                .map_err(|_| format!("bad duration {s:?}; use e.g. 500ms, 2s"))?,
        )),
        None => parse_grace(s),
    }
}

fn parse_ready_timeout(s: &str) -> Result<std::time::Duration, String> {
    let d = parse_interval(s)?;
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
    mark: leep_derive::gc::Mark,
) -> anyhow::Result<()> {
    use leep_derive::gc;
    let blobs = leep_store::open_blob_store(blobs).context("opening blob store")?;
    let meta = open_meta(meta).await.context("opening meta store")?;
    let repo = new_repo(blobs, meta);
    let clock = gc::SystemClock;
    let dry = opts.dry_run;
    let (condemn, collect) = match phase {
        Some(GcPhase::Condemn) => (
            Some(gc::condemn_with(&repo, &opts, mark, &clock).await?),
            None,
        ),
        Some(GcPhase::Collect) => (
            None,
            Some(gc::collect_with(&repo, &opts, mark, &clock).await?),
        ),
        None => {
            if !dry {
                tracing::info!(fence = ?opts.fence, "condemning, then waiting out the fence");
            }
            let r = gc::run_with(&repo, &opts, mark, &clock).await?;
            (Some(r.condemn), (!dry).then_some(r.collect))
        }
    };
    if let Some(r) = condemn {
        let verb = if dry { "would condemn" } else { "condemned" };
        println!("changesets scanned: {}", r.changesets_scanned);
        println!("changesets walked: {}", r.changesets_walked);
        println!(
            "referenced blobs and trees marked by this run: {}",
            r.marked
        );
        println!(
            "referenced blobs marked by earlier runs: {}",
            r.marked_earlier
        );
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
    let repo = new_repo(blobs, meta);
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
        | Cmd::CompactChangesets { .. }
        | Cmd::Verify(_)
        | Cmd::Repair(_)
        | Cmd::Backup(_)
        | Cmd::Restore(_)
        | Cmd::BackupVerify(_)
        | Cmd::UpgradeIndex(_) => {
            unreachable!("handled in main")
        }
        Cmd::GenToken {
            name,
            role,
            author,
            expires_in,
            no_review,
        } => {
            let (token, mut entry) = auth::generate(&name, role).map_err(anyhow::Error::msg)?;
            entry.author = author;
            entry.review = no_review.then_some(false);
            if let Some(d) = expires_in {
                let at = jiff::Timestamp::now()
                    .checked_add(d)
                    .context("--expires-in is too far in the future")?;
                // Whole seconds read better in the file and lose nothing.
                let at = jiff::Timestamp::from_second(at.as_second())
                    .context("--expires-in is too far in the future")?;
                entry.expires = Some(at.to_string());
            }
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
        .set_buckets_for_metric(Matcher::Full("leep_land_batch_size".into()), BATCH_BUCKETS)?
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
/// Lands per group commit; a batch holds at most 64.
const BATCH_BUCKETS: &[f64] = &[1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0];

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
                | Cmd::UpgradeIndex(_)
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
    META_POOL_SIZE.store(args.meta_pool_size, std::sync::atomic::Ordering::Relaxed);
    DERIVE_CACHE_BYTES.store(
        args.derive_cache_bytes,
        std::sync::atomic::Ordering::Relaxed,
    );
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
            full,
        }) => {
            let mark = if full {
                leep_derive::gc::Mark::Full
            } else {
                leep_derive::gc::Mark::Incremental
            };
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
                mark,
            )
            .await;
        }
        Some(Cmd::Repack(r)) => return repack_cli::run_repack(&args.meta, &args.blobs, r).await,
        Some(Cmd::CompactChangesets {
            dry_run,
            retrain,
            dict_size,
            samples,
        }) => {
            let opts = leep_meta::compact::CompactOptions {
                dict_size,
                samples,
                retrain,
                dry_run,
                ..Default::default()
            };
            return run_compact(&args.meta, opts).await;
        }
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
        Some(Cmd::UpgradeIndex(u)) => {
            std::process::exit(upgrade_cli::run_upgrade_index(&args.meta, &args.blobs, u).await)
        }
        Some(cmd) => return run_command(cmd),
        None => {}
    }
    check_open_listen(
        args.listen,
        args.tokens_file.is_some(),
        args.insecure_no_auth,
    )?;
    let metrics_app = args.metrics_listen.map(|_| install_metrics()).transpose()?;
    let read_max_body = match (args.read_max_body_bytes, &args.tokens_file) {
        (Some(n), _) => Some(n),
        (None, Some(_)) => Some(leep_server::DEFAULT_READ_MAX_BODY_BYTES),
        (None, None) => None,
    };
    let config = ConfigHandle::load(ReloadSources {
        tokens_file: args.tokens_file.clone(),
        signing_keys_file: args.signing_keys_file.clone(),
        review_policy_file: args.review_policy_file.clone(),
        limit_defaults: LimitDefaults {
            rate: args.rate_limit,
            burst: args.rate_burst,
            concurrency: args.max_concurrent_per_token,
            read_max_body,
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
    let mut repo = new_repo(blobs, meta);
    if let Some(jobs) = args.derive_jobs {
        repo = repo.with_derive_jobs(jobs as usize);
    }
    leep_server::reload::warn_missing_refs(repo.meta().as_ref(), &config.current()).await;
    if args.request_timeout.is_zero() {
        tracing::warn!("request timeout disabled: gc --fence cannot be guaranteed safe");
    }
    #[cfg(unix)]
    leep_server::reload::spawn_sighup_reload(config.clone(), repo.meta().clone())
        .context("installing the SIGHUP handler")?;
    leep_server::reload::spawn_token_expiry_gauges(
        config.clone(),
        std::time::Duration::from_secs(60),
    );
    let index_leases = index_policy(&args, &mut repo)?;
    let warm = match (args.indexing, &index_leases) {
        (Indexing::Off, _) => {
            tracing::info!(
                "index warming off (--indexing off): the index lease holders build the indexes"
            );
            None
        }
        _ if args.warm_interval.is_zero() => {
            tracing::info!("index warming off: the first query on new history derives its indexes");
            None
        }
        (Indexing::Lease, Some(leases)) => {
            tracing::info!(
                interval_s = args.warm_interval.as_secs_f64(),
                kinds = ?leases.config().candidate,
                "warming the indexes whose lease this server holds"
            );
            Some(
                leep_derive::warm::Warmer::new(repo.clone(), args.warm_search)
                    .with_leases(leases.clone()),
            )
        }
        _ => {
            tracing::info!(
                interval_s = args.warm_interval.as_secs_f64(),
                search = args.warm_search,
                "warming indexes of moved refs in the background"
            );
            Some(leep_derive::warm::Warmer::new(
                repo.clone(),
                args.warm_search,
            ))
        }
    };
    if let (Indexing::Lease, Some(leases)) = (args.indexing, &index_leases) {
        leases.start();
    }
    if let Some(w) = warm {
        w.spawn(args.warm_interval);
    }
    let coordination = match args.land_coordination {
        Coordination::Off => None,
        Coordination::On => Some(land_coordination(&args, repo.meta().clone())?),
    };
    if let Some((coordinator, _)) = &coordination {
        repo = repo.with_land_coordinator(coordinator.clone());
    }
    let mut app = leep_server::router_with_ready_timeout(
        repo,
        args.max_body_bytes,
        args.request_timeout,
        config,
        args.ready_timeout,
    );
    if let Some((_, http)) = &coordination {
        app = leep_server::coord::layer(app, http.clone());
    }

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
    // Index leases are given up as soon as shutdown starts, so another
    // server takes the groups at once.
    let releasing = index_leases.clone();
    let Some((coordinator, http)) = coordination else {
        leep_server::serve(listener, app, async move {
            shutdown_signal().await;
            if let Some(l) = releasing {
                l.shutdown().await;
            }
        })
        .await?;
        return Ok(());
    };
    // Leases are given up as soon as shutdown starts, so another server
    // takes the refs at once; lands still in flight here are handed back.
    let stopping = coordinator.clone();
    let server = tokio::spawn(leep_server::serve(listener, app, async move {
        shutdown_signal().await;
        stopping.shutdown().await;
        if let Some(l) = releasing {
            l.shutdown().await;
        }
    }));
    let address = coordinator.config().address.clone();
    if let Err(e) = http.check_self(&address).await {
        coordinator.shutdown().await;
        return Err(e);
    }
    coordinator.start();
    tracing::info!(
        advertise = %address,
        holder = %coordinator.config().holder,
        "coordinating lands with other servers"
    );
    server.await.context("server task")??;
    Ok(())
}

/// The index leases and query policy of `--indexing lease|off` (ADR 0058),
/// set on `repo`; `None` in `local` mode.
fn index_policy(
    args: &Args,
    repo: &mut Repo,
) -> anyhow::Result<Option<Arc<leep_derive::indexing::IndexLeases>>> {
    use leep_derive::indexing::{
        Budgets, IndexGroup, IndexLeaseConfig, IndexLeases, IndexMode, IndexPolicy,
    };
    let mode = match args.indexing {
        Indexing::Local => {
            if !args.index_kinds.is_empty() {
                tracing::warn!("--index-kinds has no effect in --indexing local");
            }
            return Ok(None);
        }
        Indexing::Lease => IndexMode::Lease,
        Indexing::Off => IndexMode::Off,
    };
    if !repo.meta().supports_leases() {
        anyhow::bail!(
            "--indexing lease and off need a metadata store with leases (Postgres, SQLite)"
        );
    }
    let mut groups = Vec::new();
    for k in &args.index_kinds {
        let g = IndexGroup::parse(k.trim()).ok_or_else(|| {
            anyhow::anyhow!("--index-kinds: unknown index {k:?}; use history, log-filter or search")
        })?;
        if !groups.contains(&g) {
            groups.push(g);
        }
    }
    if groups.is_empty() {
        groups = IndexGroup::ALL.to_vec();
    }
    if !args.warm_search {
        groups.retain(|g| *g != IndexGroup::Search);
    }
    let mut id = [0u8; 16];
    getrandom::fill(&mut id).map_err(|e| anyhow::anyhow!("random holder id: {e}"))?;
    let cfg = IndexLeaseConfig::new(
        hex::encode(id),
        args.advertise_url.clone().unwrap_or_default(),
    )
    .with_candidates(
        if mode == IndexMode::Lease && !args.warm_interval.is_zero() {
            groups
        } else {
            Vec::new()
        },
    );
    let leases = IndexLeases::new(repo.meta().clone(), cfg);
    let budgets = Budgets {
        scan: args.query_scan_budget,
        blame: args.blame_scan_budget,
        search_files: args.search_scan_budget,
        search_bytes: args.search_scan_bytes,
        legacy_factor: args.legacy_scan_factor as usize,
    };
    let policy = IndexPolicy::new(mode, leases.clone())
        .with_budgets(budgets)
        .with_index_wait(args.index_wait, args.index_waiters)
        .with_request_timeout(args.request_timeout);
    *repo = repo.clone().with_index_policy(Arc::new(policy));
    tracing::info!(
        mode = if mode == IndexMode::Lease { "lease" } else { "off" },
        holder = %leases.config().holder,
        "queries never derive an index while its lease lives (ADR 0058)"
    );
    Ok(Some(leases))
}

/// The land coordinator and its HTTP side, from the flags (ADR 0056).
fn land_coordination(
    args: &Args,
    meta: Arc<dyn leep_core::MetaStore>,
) -> anyhow::Result<(
    Arc<leep_derive::land_coord::LandCoordinator>,
    Arc<leep_server::coord::Coordination>,
)> {
    let Some(address) = args.advertise_url.clone() else {
        anyhow::bail!("--land-coordination on needs --advertise-url");
    };
    if args.peer_url_prefix.is_empty() && args.peer_cidr.is_empty() {
        anyhow::bail!("--land-coordination on needs --peer-url-prefix or --peer-cidr");
    }
    if !meta.supports_leases() {
        anyhow::bail!("--land-coordination on needs a Postgres or SQLite metadata store");
    }
    if args.land_lease_renew.is_zero() || args.land_lease_ttl < args.land_lease_renew * 2 {
        anyhow::bail!("--land-lease-ttl must be at least twice --land-lease-renew");
    }
    let policy = leep_server::coord::PeerPolicy {
        prefixes: args.peer_url_prefix.clone(),
        cidrs: args.peer_cidr.clone(),
        allow_plain_http: args.peer_allow_plain_http,
    };
    policy
        .check_url(&address)
        .map_err(|e| anyhow::anyhow!("--advertise-url: {e}"))?;
    if !policy.recognises_peers() {
        tracing::warn!(
            "the peer allow-list names hosts only (no --peer-cidr, no IP address in \
             --peer-url-prefix): no request is recognised as a peer's, so forwarded lands \
             are charged the per-token concurrency limit again on the coordinating server, \
             and one that arrives at a server that no longer leads is forwarded again; \
             add --peer-cidr for the servers' network"
        );
    }
    let mut id = [0u8; 16];
    getrandom::fill(&mut id).map_err(|e| anyhow::anyhow!("random holder id: {e}"))?;
    let holder = hex::encode(id);
    let mut cfg = leep_derive::land_coord::CoordinatorConfig::new(holder.clone(), address);
    cfg.ttl = args.land_lease_ttl;
    cfg.renew = args.land_lease_renew;
    let coordinator = leep_derive::land_coord::LandCoordinator::new(meta, cfg);
    let http = leep_server::coord::Coordination::new(policy, holder)?;
    Ok((coordinator, http))
}

/// Without tokens anyone who reaches the server can read and rewrite every
/// ref, so only loopback may be served that way unless the operator says
/// otherwise (for example behind a proxy that authenticates).
fn check_open_listen(listen: SocketAddr, tokens: bool, insecure: bool) -> anyhow::Result<()> {
    if tokens || listen.ip().is_loopback() {
        return Ok(());
    }
    if insecure {
        tracing::warn!(%listen, "serving without authentication on a non-loopback address");
        return Ok(());
    }
    anyhow::bail!(
        "refusing to listen on {listen} without authentication: give --tokens-file \
         (LEEP_TOKENS_FILE), listen on a loopback address, or pass --insecure-no-auth \
         (LEEP_INSECURE_NO_AUTH=1) to serve anyone who can reach it"
    )
}

/// The startup summary of what the files configured. Counts only.
fn log_config(c: &RuntimeConfig) {
    tracing::info!(
        fingerprint = %c.fingerprint,
        "config loaded (servers that run the same config print the same fingerprint)"
    );
    leep_server::reload::record_token_expiry(c, true);
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
    async fn get_many(
        &self,
        hashes: &[leep_core::Hash],
    ) -> leep_core::Result<Vec<Option<bytes::Bytes>>> {
        self.0.get_many(hashes).await
    }
    async fn contains(&self, hash: &leep_core::Hash) -> leep_core::Result<bool> {
        self.0.contains(hash).await
    }
    async fn contains_many(&self, hashes: &[leep_core::Hash]) -> leep_core::Result<Vec<bool>> {
        self.0.contains_many(hashes).await
    }
    async fn flush(&self, named: &[leep_core::Hash]) -> leep_core::Result<()> {
        self.0.flush(named).await
    }
    async fn flush_blobs(&self, hashes: &[leep_core::Hash]) -> leep_core::Result<()> {
        self.0.flush_blobs(hashes).await
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
