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
mod sweep_cli;
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
    /// search's directory filters, filtered log) may hold; the ancestry and
    /// skiplist caches scale with it. Separate from --cache-bytes, which
    /// caches blobs.
    #[arg(
        long,
        global = true,
        env = "LEEP_DERIVE_CACHE_BYTES",
        default_value_t = leep_derive::DEFAULT_CACHE_BYTES
    )]
    derive_cache_bytes: usize,
    /// Abort requests that run longer than this (0, 90s, 5m); 0 disables the
    /// bound. `gc --fence` must exceed it plus --meta-statement-timeout; `gc`
    /// warns when the values it is given do not.
    #[arg(long, global = true, env = "LEEP_REQUEST_TIMEOUT", default_value = "5m", value_parser = parse_grace)]
    request_timeout: std::time::Duration,
    /// Bound on each statement the server sends to a Postgres metadata store
    /// (its `statement_timeout`), so a query whose request was cut off by
    /// --request-timeout does not run on in the database (0, 30s, 5m). Lease
    /// calls and migrations are not bounded; SQLite ignores it. Default:
    /// --request-timeout plus 5s, none when that is 0; 0 turns it off.
    #[arg(long, global = true, env = "LEEP_META_STATEMENT_TIMEOUT", value_parser = parse_grace)]
    meta_statement_timeout: Option<std::time::Duration>,
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
    /// Ceiling on every request body, in bytes; --max-blob-bytes and
    /// --max-json-body-bytes are lower and apply first.
    #[arg(long, env = "LEEP_MAX_BODY_BYTES", default_value_t = leep_server::DEFAULT_MAX_BODY_BYTES)]
    max_body_bytes: usize,
    /// Largest blob upload (`PUT /v1/blobs/{hash}`, one file's contents) in
    /// bytes; larger is 413.
    #[arg(long, env = "LEEP_MAX_BLOB_BYTES", default_value_t = leep_server::limits::DEFAULT_MAX_BLOB_BYTES)]
    max_blob_bytes: usize,
    /// Largest body of every other request (JSON: changesets, lands, ref
    /// updates) in bytes; larger is 413.
    #[arg(long, env = "LEEP_MAX_JSON_BODY_BYTES", default_value_t = leep_server::limits::DEFAULT_MAX_JSON_BODY_BYTES)]
    max_json_body_bytes: usize,
    /// Bytes of request body all requests in flight may hold together; a
    /// request that would go over is refused with 503 and Retry-After. A
    /// request counts its Content-Length, or its route's cap without one.
    #[arg(long, env = "LEEP_MAX_INFLIGHT_BYTES", default_value_t = leep_server::limits::DEFAULT_MAX_INFLIGHT_BYTES)]
    max_inflight_bytes: usize,
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
    /// Protect this ref without requiring signatures or review: it only
    /// moves forward (a land, or a fast-forward through `POST /v1/refs`)
    /// and cannot be deleted through the API. Refs listed in
    /// --require-signatures, --require-review or the review policy are
    /// already protected. Repeatable; the environment variable takes a
    /// comma-separated list.
    #[arg(long = "protect-ref", env = "LEEP_PROTECT_REFS", value_delimiter = ',')]
    protect_refs: Vec<String>,
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
    /// Most expensive reads of one class this server runs at once (ADR
    /// 0060), as CLASS=N or CLASS=off. Repeatable; the environment variable
    /// takes a comma-separated list. A request over the limit is refused at
    /// once with 503, Retry-After and `Leep-Refused: read-class`. Only
    /// `search` is limited (default: half the cores, at least 1);
    /// `land_check`, `blame`, `log_filtered` and `diff_large` take only off.
    #[arg(long, env = "LEEP_READ_CONCURRENCY", value_delimiter = ',')]
    read_concurrency: Vec<String>,
    /// Directory opens of all searches that may read the metadata store at
    /// once, or off; each holds up to two connections. Default: a quarter
    /// of --meta-pool-size, at least 1, so searches hold at most half of
    /// the pool and lands and ref reads keep the rest.
    #[arg(long, env = "LEEP_SEARCH_POOL_PERMITS", value_parser = parse_count_or_off)]
    search_pool_permits: Option<CountOrOff>,
    /// Batches a ref's land queue may hold before this server refuses new
    /// lands onto it (ADR 0060), or off. The queue counts the lands waiting
    /// in batches of 64, rounded up, plus the batch being replayed; a land
    /// waits about one batch time per batch ahead of it, so this bounds the
    /// wait. A refused land gets 503 at once with Retry-After and
    /// `Leep-Refused: admission`, before its changeset is read, and clients
    /// send it again; queued lands are never dropped. Start with N times a
    /// full batch's time under half the clients' timeout.
    #[arg(long, env = "LEEP_LAND_QUEUE_BATCHES", value_parser = parse_count_or_off, default_value = "off")]
    land_queue_batches: CountOrOff,
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
        #[arg(long, value_parser = parse_expires_in)]
        expires_in: Option<std::time::Duration>,
        /// Keep this token from posting reviews (`"review": false`); its
        /// reviews never count. For agent tokens.
        #[arg(long)]
        no_review: bool,
    },
    /// Delete blobs no changeset references (uses --meta and --blobs; safe
    /// beside running servers). Without a phase: condemn, wait --fence, then
    /// collect. With --unreachable: delete changesets nothing reaches
    /// instead (ADR 0059), or with --dry-run count them.
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
        /// Delete only what was condemned at least this long ago. Must exceed
        /// the servers' --request-timeout plus their --meta-statement-timeout
        /// (Postgres) plus clock skew, and for --unreachable also a land
        /// batch's 60 s and the index fence; give `gc` the servers' values of
        /// those flags (or their LEEP_ variables). It refuses a fence that
        /// does not, unless --unsafe-fence.
        #[arg(long, global = true, default_value = "15m", value_parser = parse_grace)]
        fence: std::time::Duration,
        /// Allow --fence 0. Only safe while no server is running.
        #[arg(long, global = true)]
        unsafe_no_fence: bool,
        /// Run with a --fence shorter than the servers' bounds require. Only
        /// safe while no server runs with those bounds (tests, a stopped
        /// fleet).
        #[arg(long, global = true)]
        unsafe_fence: bool,
        /// Walk all of history and every tree instead of only what earlier
        /// runs did not mark (ADR 0033), and rewrite their record. Needs
        /// --backups or --backup-fence (ADR 0059 section 6).
        #[arg(long, global = true)]
        full: bool,
        /// Keep the trees of changesets the changeset sweep deleted within
        /// this long, for backups still copying them (required with --full;
        /// at most --tombstone-retention).
        #[arg(long, global = true, value_parser = parse_grace)]
        backup_fence: Option<std::time::Duration>,
        /// Derive --backup-fence from the sealed backups in this directory:
        /// three times the longest of the last 30 days, at least an hour.
        #[arg(long, global = true, conflicts_with = "backup_fence")]
        backups: Option<std::path::PathBuf>,
        /// Garbage-collect changesets nothing reaches (ADR 0059): refs, every
        /// ref log entry, the landed-copy scan window of every head,
        /// attestations, mirror maps and --changeset-grace. With --dry-run,
        /// count what it would delete, strictly read-only (Postgres sessions
        /// are read-only, SQLite opens with mode=ro), with --grace as the
        /// grace. Without it: condemn, wait --fence, collect.
        #[arg(long, conflicts_with = "full")]
        unreachable: bool,
        /// With --unreachable: only condemn (a cron job).
        #[arg(long, requires = "unreachable", conflicts_with_all = ["collect_only", "dry_run"])]
        condemn_only: bool,
        /// With --unreachable: only collect what was condemned a --fence ago
        /// (a cron job).
        #[arg(long, requires = "unreachable", conflicts_with = "dry_run")]
        collect_only: bool,
        /// With --unreachable: changesets stored less than this long ago are
        /// kept, with their ancestry.
        #[arg(long, default_value = "7d", value_parser = parse_grace, requires = "unreachable")]
        changeset_grace: std::time::Duration,
        /// With --unreachable: changesets per delete transaction.
        #[arg(long, default_value_t = 500, requires = "unreachable")]
        batch: usize,
        /// With --unreachable: changesets deleted per second at most (0: no
        /// limit).
        #[arg(long, default_value_t = 500, requires = "unreachable")]
        rate: u64,
        /// With --unreachable: changesets one collect deletes at most; the
        /// rest wait for the next run.
        #[arg(long, default_value_t = 100_000, requires = "unreachable")]
        max_delete: u64,
        /// With --unreachable: how long the tombstone pass waits for index
        /// writes in flight (ADR 0058).
        #[arg(long, default_value = "60s", value_parser = parse_grace, requires = "unreachable")]
        index_fence: std::time::Duration,
        /// How long the sweep keeps tombstones of deleted changesets (with
        /// --unreachable), and the longest --backup-fence any gc accepts:
        /// past it, the root trees a backup needs are no longer known.
        #[arg(long, default_value = "7d", value_parser = parse_grace)]
        tombstone_retention: std::time::Duration,
        /// With --unreachable: walk reachability from every root instead of
        /// trusting `cs-reach-v1` rows, and write them again (ADR 0059
        /// section 4). Slower; for a store whose rows are in doubt.
        #[arg(long, requires = "unreachable")]
        reach_full: bool,
        /// With --unreachable: write the run's numbers to this Prometheus
        /// textfile (atomically).
        #[arg(long, requires = "unreachable")]
        metrics_file: Option<std::path::PathBuf>,
        /// Crash-test hook: stop a collect for good at `doom`, `batch` or
        /// `tombstone`, the Nth time it gets there (`batch:2`), and say so on
        /// stderr.
        #[arg(long, hide = true, requires = "unreachable", value_parser = parse_pause)]
        debug_pause_at: Option<(leep_derive::changeset_sweep::PausePoint, u32)>,
        /// With --unreachable: ref log retention to compare against keeping
        /// it forever (sizes the follow-up ADR on ref log retention).
        #[arg(long, default_value = "30d", value_parser = parse_grace, requires = "unreachable")]
        ref_log_retention: std::time::Duration,
        /// With --unreachable: walk every reachable tree, so tree counts are
        /// exact rather than an upper bound. Costs a full GC mark: not on a
        /// small host beside live servers.
        #[arg(long, requires = "unreachable")]
        exact_trees: bool,
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
    open_meta_bounded(url, None).await
}

/// [`open_meta`], with each Postgres data statement bounded by
/// `statement_timeout`.
async fn open_meta_bounded(
    url: &str,
    statement_timeout: Option<std::time::Duration>,
) -> leep_core::Result<Arc<dyn leep_core::MetaStore>> {
    let mode = if NO_MIGRATE.load(std::sync::atomic::Ordering::Relaxed) {
        leep_meta::SchemaMode::RequireCurrent
    } else {
        leep_meta::SchemaMode::Migrate
    };
    let pool = META_POOL_SIZE.load(std::sync::atomic::Ordering::Relaxed);
    leep_meta::open_meta_store_with_options(url, mode, pool, statement_timeout).await
}

/// Added to --request-timeout for the default statement bound, so a request
/// that runs out of time is ended by its own timeout (408), not by a
/// statement failing first.
const STATEMENT_TIMEOUT_MARGIN: std::time::Duration = std::time::Duration::from_secs(5);

/// `doom`, `batch:2`: a point of `--debug-pause-at`, and how many times to
/// pass it first.
fn parse_pause(s: &str) -> Result<(leep_derive::changeset_sweep::PausePoint, u32), String> {
    let (p, n) = s.split_once(':').unwrap_or((s, "0"));
    Ok((p.parse()?, n.parse().map_err(|e| format!("{n:?}: {e}"))?))
}

/// The `--meta-statement-timeout` in force: the flag, or the request
/// timeout plus [`STATEMENT_TIMEOUT_MARGIN`]; `None` for no bound.
fn statement_timeout(
    flag: Option<std::time::Duration>,
    request_timeout: std::time::Duration,
) -> Option<std::time::Duration> {
    match flag {
        Some(d) => (!d.is_zero()).then_some(d),
        None => (!request_timeout.is_zero()).then(|| request_timeout + STATEMENT_TIMEOUT_MARGIN),
    }
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

/// `--search-pool-permits` and `--land-queue-batches`: a number, or no
/// bound.
#[derive(Clone, Copy, Debug)]
enum CountOrOff {
    Off,
    Count(usize),
}

fn parse_count_or_off(s: &str) -> Result<CountOrOff, String> {
    match s.trim() {
        "off" => Ok(CountOrOff::Off),
        n => match n.parse::<usize>() {
            Ok(n) if n >= 1 => Ok(CountOrOff::Count(n)),
            _ => Err(format!(
                "expected a number of at least 1, or off; got {s:?}"
            )),
        },
    }
}

fn parse_ready_timeout(s: &str) -> Result<std::time::Duration, String> {
    let d = parse_interval(s)?;
    if d.is_zero() {
        return Err("must be greater than zero".into());
    }
    Ok(d)
}

/// `gen-token --expires-in`: a lifetime, so zero (expired at birth) is refused.
fn parse_expires_in(s: &str) -> Result<std::time::Duration, String> {
    let d = parse_grace(s)?;
    if d.is_zero() {
        return Err("must be greater than zero, e.g. 30d".into());
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

/// The store URL opened so that the database itself refuses writes, so the
/// dry run cannot change the store whatever it calls: Postgres sessions with
/// `default_transaction_read_only`, SQLite with `mode=ro`. A URL that
/// already sets the parameter is refused rather than given a second one.
fn read_only_meta_url(url: &str) -> anyhow::Result<String> {
    let sep = if url.contains('?') { '&' } else { '?' };
    let has = |param: &str| {
        url.split_once('?').is_some_and(|(_, q)| {
            q.split('&').any(|kv| {
                kv.split('=').next() == Some(param) || kv.starts_with(&format!("{param}["))
            })
        })
    };
    if url.starts_with("postgres://") || url.starts_with("postgresql://") {
        if has("options") {
            anyhow::bail!(
                "--unreachable opens Postgres read-only with its own options=; \
                 remove options= from the --meta URL"
            );
        }
        Ok(format!(
            "{url}{sep}options=-c%20default_transaction_read_only%3Don"
        ))
    } else if url.starts_with("sqlite:") {
        if has("mode") {
            anyhow::bail!(
                "--unreachable opens SQLite with mode=ro; remove mode= from the --meta URL"
            );
        }
        Ok(format!("{url}{sep}mode=ro"))
    } else {
        anyhow::bail!("--unreachable needs a postgres:// or sqlite: --meta URL")
    }
}

async fn run_unreachable_report(
    meta: &str,
    blobs: &str,
    opts: leep_derive::gc_unreachable::UnreachableOptions,
) -> anyhow::Result<()> {
    use leep_derive::gc_unreachable::{report, PolicyReport, AGE_BUCKETS_DAYS, WRITE_PATH_KINDS};
    let url = read_only_meta_url(meta)?;
    let blobs = leep_store::open_blob_store(blobs).context("opening blob store")?;
    let pool = META_POOL_SIZE.load(std::sync::atomic::Ordering::Relaxed);
    let meta =
        leep_meta::open_meta_store_with_pool(&url, leep_meta::SchemaMode::RequireCurrent, pool)
            .await
            .context("opening meta store")?;
    let repo = new_repo(blobs, meta);
    let started = std::time::Instant::now();
    let r = report(&repo, &opts).await?;
    let sizes = leep_meta::space::logical_sizes(&url)
        .await
        .context("reading logical sizes")?;
    let per = |b: u64| {
        if r.lands == 0 {
            "-".to_string()
        } else {
            format!("{:.0}", b as f64 / r.lands as f64)
        }
    };
    println!("changesets stored: {}", r.changesets);
    println!("parents named but not stored: {}", r.missing_parents);
    println!("refs: {}", r.refs);
    println!("ref log entries: {} ({} lands)", r.ref_log_entries, r.lands);
    println!("rebased-origin links: {}", r.rebased_links);
    println!("originals in a scan window: {}", r.window_originals);
    println!("unreachable but attested: {}", r.attested);
    println!("unreachable but mirrored: {}", r.mirrored);
    println!(
        "ref log entries older than {}s (kept: each ref's newest): {} entries, ~{} bytes, ~{} per land",
        opts.ref_log_retention.as_secs(),
        r.ref_log_prunable_entries,
        r.ref_log_prunable_bytes,
        per(r.ref_log_prunable_bytes)
    );
    let show = |name: &str, p: &PolicyReport| {
        let t = &p.reclaimable;
        println!("[{name}]");
        println!("  reachable: {}", p.reachable);
        println!("  unreachable before grace: {}", p.unreachable);
        println!(
            "  by age (<{}d, <{}d, <{}d, older): {:?}",
            AGE_BUCKETS_DAYS[0], AGE_BUCKETS_DAYS[1], AGE_BUCKETS_DAYS[2], p.by_age
        );
        println!(
            "  reclaimable changesets (older than {}s): {} (originals {}, snapshots {}, other {})",
            opts.grace.as_secs(),
            t.changesets,
            t.originals,
            t.snapshots,
            t.other
        );
        println!(
            "  changeset bytes (uncompressed encoding): {} ({} per land)",
            t.changeset_bytes,
            per(t.changeset_bytes)
        );
        for (kind, (rows, bytes)) in &t.rows {
            println!("  rows {kind}: {rows} rows, {bytes} bytes");
        }
        println!(
            "  metadata bytes (logical): {} ({} per land)",
            t.meta_bytes(),
            per(t.meta_bytes())
        );
        let bound = if p.exact_trees {
            "exact"
        } else {
            "upper bound"
        };
        println!(
            "  tree nodes ({bound}): {} nodes, {} bytes ({} per land)",
            t.tree_nodes,
            t.tree_node_bytes,
            per(t.tree_node_bytes)
        );
        println!(
            "  file contents ({bound}): {} blobs, {} bytes ({} per land)",
            t.contents,
            t.content_bytes,
            per(t.content_bytes)
        );
    };
    show("as designed: ref log kept forever", &r.log_kept);
    show(
        "ref log kept for --ref-log-retention (follow-up ADR)",
        &r.log_pruned,
    );
    let mut index_bytes = None;
    if let Some(z) = &sizes {
        println!("[store, logical: keys and values, not pages]");
        println!(
            "  changesets: {} rows, {} bytes (as stored)",
            z.changesets.0, z.changesets.1
        );
        println!("  ref log: {} rows, {} bytes", z.ref_log.0, z.ref_log.1);
        println!("  refs: {} rows, {} bytes", z.refs.0, z.refs.1);
        let mut index = 0;
        for (kind, rows, bytes) in &z.derived {
            let class = if WRITE_PATH_KINDS.contains(&kind.as_str()) {
                "write path"
            } else {
                index += bytes;
                "index"
            };
            println!("  derived {kind} ({class}): {rows} rows, {bytes} bytes");
        }
        index_bytes = Some(index);
        if let Some(p) = z.physical_bytes {
            println!(
                "  physical database size: {p} bytes (pages, not logical bytes; on \
                 Postgres pg_database_size runs 2.5 to 3.5 times the logical bytes)"
            );
        }
    }
    println!("[per land, logical bytes, side by side]");
    println!(
        "  write path (the lands' copies, originals, their {} rows and log entries): {}",
        WRITE_PATH_KINDS.join(", "),
        per(r.land_write.bytes())
    );
    println!(
        "  of which their ref log entries: {}",
        per(r.land_write.log_bytes)
    );
    println!(
        "  index rows (all non-write-path derived rows in the store / lands; \
         includes any imported history's): {}",
        index_bytes.map_or("-".to_string(), per)
    );
    println!(
        "  unreachable, reclaimable as designed: {}",
        per(r.log_kept.reclaimable.meta_bytes())
    );
    println!(
        "  unreachable, reclaimable with ref log retention (follow-up ADR): {} (+{} of log entries)",
        per(r.log_pruned.reclaimable.meta_bytes()),
        per(r.ref_log_prunable_bytes)
    );
    println!("  (changesets count by their canonical encoding; stored rows may be compressed)");
    if let Some(l) = &r.live_trees {
        let t = &r.log_kept.reclaimable;
        println!(
            "  blob store, live tree nodes and contents (all of them / lands; \
             includes any imported history's): {} nodes {}, contents {}",
            l.tree_nodes,
            per(l.tree_node_bytes),
            per(l.content_bytes)
        );
        println!(
            "  blob store, reclaimable as designed by gc --full after the sweep (exact): \
             tree nodes {}, contents {}",
            per(t.tree_node_bytes),
            per(t.content_bytes)
        );
    }
    println!(
        "reclaimable without a root_tree row (trees not counted): {}",
        r.without_root_tree
    );
    println!(
        "tree nodes missing from the blob store: {}",
        r.missing_nodes
    );
    println!("elapsed: {:.1}s", started.elapsed().as_secs_f64());
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

/// Subcommands that open no store, so the store and server flags every
/// subcommand inherits (`global = true`) mean nothing to them.
const STORELESS_SUBCOMMANDS: [&str; 3] = ["hash-token", "gen-token", "backup-verify"];

/// [`Args`]'s command, with the inherited flags left out of the help of
/// [`STORELESS_SUBCOMMANDS`]. They stay accepted there (in either position,
/// and from the environment), so no existing invocation breaks: each
/// subcommand gets its own hidden copy of every global flag, which clap
/// then treats exactly like the copy it would have propagated.
fn cli() -> clap::Command {
    let cmd = <Args as clap::CommandFactory>::command();
    let globals: Vec<clap::Arg> = cmd
        .get_arguments()
        .filter(|a| a.is_global_set())
        .cloned()
        .collect();
    STORELESS_SUBCOMMANDS.iter().fold(cmd, |cmd, name| {
        cmd.mut_subcommand(name, |sc| {
            globals
                .iter()
                .fold(sc, |sc, a| sc.arg(a.clone().hide(true)))
        })
    })
}

fn parse_args<I, T>(argv: I) -> Result<Args, clap::Error>
where
    I: IntoIterator<Item = T>,
    T: Into<std::ffi::OsString> + Clone,
{
    let mut cmd = cli();
    let matches = cmd.try_get_matches_from_mut(argv)?;
    <Args as clap::FromArgMatches>::from_arg_matches(&matches).map_err(|e| e.format(&mut cmd))
}

/// Colour text logs only on a terminal: escapes in a log file or a
/// journal are noise to every reader and grep. A non-empty `NO_COLOR`
/// (no-color.org) turns them off on a terminal too.
fn log_colours(terminal: bool, no_color: Option<std::ffi::OsString>) -> bool {
    terminal && no_color.is_none_or(|v| v.is_empty())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = parse_args(std::env::args_os()).unwrap_or_else(|e| e.exit());
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into());
    // These print their report on stdout, so logs go to stderr.
    let (writer, terminal) = if matches!(
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
        (
            BoxMakeWriter::new(std::io::stderr),
            std::io::IsTerminal::is_terminal(&std::io::stderr()),
        )
    } else {
        (
            BoxMakeWriter::new(std::io::stdout),
            std::io::IsTerminal::is_terminal(&std::io::stdout()),
        )
    };
    match args.log_format {
        LogFormat::Text => tracing_subscriber::fmt()
            .with_ansi(log_colours(terminal, std::env::var_os("NO_COLOR")))
            .with_env_filter(filter)
            .with_writer(writer)
            .init(),
        LogFormat::Json => tracing_subscriber::fmt()
            .json()
            .with_ansi(false)
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
            dry_run: false,
            fence,
            unsafe_no_fence,
            unsafe_fence,
            unreachable: true,
            condemn_only,
            collect_only,
            changeset_grace,
            batch,
            rate,
            max_delete,
            index_fence,
            tombstone_retention,
            metrics_file,
            debug_pause_at,
            reach_full,
            ..
        }) => {
            if phase.is_some() {
                anyhow::bail!("--unreachable takes no phase; use --condemn-only or --collect-only");
            }
            let bounds = sweep_cli::ServerBounds {
                request_timeout: args.request_timeout,
                statement_timeout: statement_timeout(
                    args.meta_statement_timeout,
                    args.request_timeout,
                ),
                postgres: args.meta.starts_with("postgres"),
            };
            if !(unsafe_fence || unsafe_no_fence || fence.is_zero()) {
                if let Some(why) = sweep_cli::changeset_fence_shortfall(fence, &bounds, index_fence)
                {
                    anyhow::bail!(
                        "{why}, or pass --unsafe-fence if no server runs with these bounds"
                    );
                }
            }
            let blobs = leep_store::open_blob_store(&args.blobs).context("opening blob store")?;
            let meta = open_meta(&args.meta).await.context("opening meta store")?;
            let repo = new_repo(blobs, meta);
            let opts = leep_derive::changeset_sweep::SweepOptions {
                grace: changeset_grace,
                fence,
                unsafe_no_fence,
                batch,
                rate,
                max_delete,
                index_fence,
                tombstone_retention,
                full: reach_full,
                pause_at: debug_pause_at,
            };
            let phase = match (condemn_only, collect_only) {
                (true, _) => sweep_cli::SweepPhase::Condemn,
                (_, true) => sweep_cli::SweepPhase::Collect,
                _ => sweep_cli::SweepPhase::Both,
            };
            return sweep_cli::run_sweep(&repo, phase, &opts, metrics_file.as_deref()).await;
        }
        Some(Cmd::Gc {
            phase,
            grace,
            unreachable: true,
            ref_log_retention,
            exact_trees,
            ..
        }) => {
            if phase.is_some() {
                anyhow::bail!("--unreachable takes no phase");
            }
            return run_unreachable_report(
                &args.meta,
                &args.blobs,
                leep_derive::gc_unreachable::UnreachableOptions {
                    now: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map_or(0, |d| d.as_secs() as i64),
                    grace,
                    ref_log_retention,
                    exact_trees,
                },
            )
            .await;
        }
        Some(Cmd::Gc {
            phase,
            dry_run,
            grace,
            fence,
            unsafe_no_fence,
            unsafe_fence,
            full,
            backup_fence,
            backups,
            tombstone_retention,
            ..
        }) => {
            let mark = if full {
                leep_derive::gc::Mark::Full
            } else {
                leep_derive::gc::Mark::Incremental
            };
            // A zero fence is refused below unless --unsafe-no-fence.
            if !unsafe_no_fence && !fence.is_zero() {
                let bounds = sweep_cli::ServerBounds {
                    request_timeout: args.request_timeout,
                    statement_timeout: statement_timeout(
                        args.meta_statement_timeout,
                        args.request_timeout,
                    ),
                    postgres: args.meta.starts_with("postgres"),
                };
                if let Some(why) = sweep_cli::blob_fence_shortfall(fence, &bounds) {
                    if dry_run || unsafe_fence {
                        tracing::warn!("{why}");
                    } else {
                        anyhow::bail!(
                            "{why}, or pass --unsafe-fence if no server runs with these bounds"
                        );
                    }
                }
            }
            let backup_fence = match (full, backup_fence, backups) {
                (_, Some(f), _) => Some(f),
                (_, None, Some(dir)) => {
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map_or(0, |d| d.as_secs() as i64);
                    let f = sweep_cli::backup_fence_from(&dir, now)?;
                    tracing::info!(backup_fence = ?f, "backup fence from sealed backups");
                    Some(f)
                }
                (false, None, None) => None,
                (true, None, None) if dry_run => None,
                (true, None, None) => anyhow::bail!(
                    "gc --full frees the trees of deleted changesets, which a backup in flight \
                     may still need: pass --backups DIR or --backup-fence (ADR 0059 section 6)"
                ),
            };
            if let Some(f) = backup_fence {
                sweep_cli::check_backup_fence(f, tombstone_retention)?;
            }
            return run_gc(
                &args.meta,
                &args.blobs,
                phase,
                leep_derive::gc::GcOptions {
                    grace,
                    fence,
                    unsafe_no_fence,
                    dry_run,
                    backup_fence,
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
    let read_concurrency = leep_server::read_limits::ReadConcurrency::parse(&args.read_concurrency)
        .map_err(anyhow::Error::msg)?;
    let search_pool = match args.search_pool_permits {
        Some(CountOrOff::Off) => None,
        Some(CountOrOff::Count(n)) => Some(n),
        None => Some(leep_server::read_limits::default_search_pool_permits(
            args.meta_pool_size,
        )),
    };
    let body_limits = leep_server::limits::BodyLimits {
        max_blob_bytes: args.max_blob_bytes.min(args.max_body_bytes),
        max_json_bytes: args.max_json_body_bytes.min(args.max_body_bytes),
        max_inflight_bytes: args.max_inflight_bytes,
    };
    body_limits.validate().map_err(anyhow::Error::msg)?;
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
        protect_refs: args.protect_refs.clone(),
    })
    .map_err(anyhow::Error::msg)?;
    log_config(&config.current());
    ensure_local_dirs(&args.meta, &args.blobs)?;

    let mut blobs = leep_store::open_blob_store(&args.blobs).context("opening blob store")?;
    if args.cache_bytes > 0 {
        blobs =
            Arc::new(CachedBlobStore::new(ArcBlobs(blobs), args.cache_bytes)) as Arc<dyn BlobStore>;
    }
    let bound = statement_timeout(args.meta_statement_timeout, args.request_timeout);
    if args.meta.starts_with("postgres") {
        match bound {
            Some(t) => tracing::info!(
                statement_timeout_s = t.as_secs_f64(),
                "metadata statements are bounded"
            ),
            None => tracing::warn!(
                "metadata statements are unbounded: a query whose request timed out runs on in \
                 Postgres until it ends"
            ),
        }
    }
    let meta = open_meta_bounded(&args.meta, bound)
        .await
        .context("opening meta store")?;
    if let Some(v) = meta.schema_version().await? {
        metrics::gauge!("leep_schema_version").set(v as f64);
    }
    // The land policy comes from the reloadable config, not from the repo.
    let mut repo = new_repo(blobs, meta);
    if let Some(jobs) = args.derive_jobs {
        repo = repo.with_derive_jobs(jobs as usize);
    }
    repo = repo.with_search_pool_permits(search_pool);
    repo = repo.with_land_queue_batches(match args.land_queue_batches {
        CountOrOff::Off => None,
        CountOrOff::Count(n) => Some(n),
    });
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
    tracing::info!(
        max_blob_bytes = body_limits.max_blob_bytes,
        max_json_body_bytes = body_limits.max_json_bytes,
        max_inflight_bytes = body_limits.max_inflight_bytes,
        "request body limits"
    );
    tracing::info!(
        read_concurrency = %read_concurrency.describe(),
        search_pool_permits = ?search_pool,
        land_queue_batches = ?repo.land_queue_batches(),
        "expensive read and land admission limits (ADR 0060)"
    );
    let mut app = leep_server::router_with_read_limits(
        repo,
        args.max_body_bytes,
        body_limits,
        args.request_timeout,
        config,
        leep_server::ReadyOptions {
            timeout: args.ready_timeout,
            ..Default::default()
        },
        read_concurrency,
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn cli_is_valid() {
        cli().debug_assert();
    }

    /// Every global flag, as `--flag value` (or `--flag` for switches).
    fn global_flags() -> Vec<Vec<&'static str>> {
        vec![
            vec!["--meta", "mem://m"],
            vec!["--blobs", "mem://b"],
            vec!["--no-migrate"],
            vec!["--meta-pool-size", "7"],
            vec!["--derive-cache-bytes", "1234"],
            vec!["--request-timeout", "90s"],
            vec!["--meta-statement-timeout", "30s"],
        ]
    }

    fn assert_globals_applied(a: &Args, argv: &[&str]) {
        assert_eq!(a.meta, "mem://m", "{argv:?}");
        assert_eq!(a.blobs, "mem://b", "{argv:?}");
        assert!(a.no_migrate, "{argv:?}");
        assert_eq!(a.meta_pool_size, 7, "{argv:?}");
        assert_eq!(a.derive_cache_bytes, 1234, "{argv:?}");
        assert_eq!(a.request_timeout, Duration::from_secs(90), "{argv:?}");
        assert_eq!(
            a.meta_statement_timeout,
            Some(Duration::from_secs(30)),
            "{argv:?}"
        );
    }

    #[test]
    fn global_flags_stay_accepted_before_and_after_every_subcommand() {
        let subcommands: &[&[&str]] = &[
            &["hash-token"],
            &["gen-token", "--name", "n", "--role", "read"],
            &["backup-verify", "--from", "/b"],
            &["gc", "--dry-run"],
            &["gc", "condemn"],
            &["migrate", "--status"],
            &["derive-search", "main"],
        ];
        let flags: Vec<&str> = global_flags().concat();
        for sub in subcommands {
            let before: Vec<&str> = std::iter::once("leep-server")
                .chain(flags.iter().copied())
                .chain(sub.iter().copied())
                .collect();
            let after: Vec<&str> = std::iter::once("leep-server")
                .chain(sub.iter().copied())
                .chain(flags.iter().copied())
                .collect();
            for argv in [before, after] {
                let a = parse_args(&argv).unwrap_or_else(|e| panic!("{argv:?}: {e}"));
                assert!(a.command.is_some(), "{argv:?}");
                assert_globals_applied(&a, &argv);
            }
        }
    }

    #[test]
    fn storeless_subcommands_hide_the_store_flags_from_their_help() {
        let mut cmd = cli();
        for name in STORELESS_SUBCOMMANDS {
            let help = cmd
                .find_subcommand_mut(name)
                .unwrap()
                .render_long_help()
                .to_string();
            for flag in global_flags() {
                let shown =
                    |f: &str| help.contains(&format!("{f} ")) || help.contains(&format!("{f}\n"));
                assert!(!shown(flag[0]), "{name} --help shows {}", flag[0]);
            }
        }
        let gc = cmd
            .find_subcommand_mut("gc")
            .unwrap()
            .render_long_help()
            .to_string();
        assert!(gc.contains("--meta") && gc.contains("--request-timeout"));
        let top = cmd.render_long_help().to_string();
        assert!(top.contains("--meta") && top.contains("--listen"));
    }

    /// deploy/leep-server.env.example lists exactly the variables the
    /// server reads, so it cannot fall behind a new flag or keep a dead one.
    #[test]
    fn env_example_lists_every_variable() {
        let path = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../deploy/leep-server.env.example"
        );
        let text = std::fs::read_to_string(path).unwrap();
        let listed: std::collections::BTreeSet<String> = text
            .lines()
            .map(|l| l.trim_start_matches('#'))
            .filter_map(|l| l.split_once('=').map(|(k, _)| k))
            .filter(|k| k.starts_with("LEEP_") && !k.contains(' '))
            .map(String::from)
            .collect();
        let read: std::collections::BTreeSet<String> = cli()
            .get_arguments()
            .filter_map(|a| a.get_env())
            .map(|e| e.to_string_lossy().into_owned())
            .collect();
        assert_eq!(listed, read, "{path} and the flags' variables differ");
    }

    #[test]
    fn log_colours_only_on_a_terminal_without_no_color() {
        assert!(log_colours(true, None));
        assert!(log_colours(true, Some("".into())));
        assert!(!log_colours(true, Some("1".into())));
        assert!(!log_colours(false, None));
        assert!(!log_colours(false, Some("1".into())));
    }

    #[test]
    fn the_statement_bound_follows_the_request_timeout_unless_set() {
        let m = Duration::from_secs(60);
        assert_eq!(
            statement_timeout(None, 5 * m),
            Some(5 * m + STATEMENT_TIMEOUT_MARGIN)
        );
        assert_eq!(statement_timeout(None, Duration::ZERO), None);
        assert_eq!(
            statement_timeout(Some(Duration::from_secs(30)), 5 * m),
            Some(Duration::from_secs(30))
        );
        assert_eq!(statement_timeout(Some(Duration::ZERO), 5 * m), None);
        assert_eq!(statement_timeout(Some(m), Duration::ZERO), Some(m));
    }
}
