//! The Leep HTTP server: a thin axum layer over [`leep_derive::Repo`].
//! The endpoint contract is the table in `leep_core::api`.

// Output goes through `leep_core::outln!`, which exits quietly when the
// reader of stdout has gone instead of panicking like `println!`.
#![warn(clippy::print_stdout)]

pub mod auth;
pub mod background_repack;
pub mod backup;
pub mod checks;
pub mod conn;
pub mod coord;
pub mod errors;
pub mod fingerprint;
pub mod guard;
pub mod limits;
pub mod observe;
pub mod protect;
pub mod read_limits;
pub mod reload;
pub mod review;
mod review_requests;
pub mod signing;
pub mod timing;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, Path, Query, State};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::middleware;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use futures_util::StreamExt;
use leep_core::api::*;
use leep_core::rename::RenameOptions;
use leep_core::{Changeset, ChangesetId, Error, FileKind, Hash, RefUpdateContext, RepoPath};
use leep_derive::indexing::{Answered, Freshness};
use leep_derive::read_stats;
use leep_derive::{search, ExtraMatch, LogFilter, LogParams, PrefixMatch, Repo, SearchQuery};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tower_http::limit::RequestBodyLimitLayer;
use tower_http::timeout::TimeoutLayer;

/// Default ceiling on every request body, in bytes. The per-route caps of
/// [`limits::BodyLimits`] are lower and apply first.
pub const DEFAULT_MAX_BODY_BYTES: usize = 512 * 1024 * 1024;

/// Default cap on request bodies from `read` tokens, which cannot upload: the
/// largest body they send is a `POST /v1/blobs/missing` list.
pub const DEFAULT_READ_MAX_BODY_BYTES: usize = 1024 * 1024;

/// Default bound on each store check behind `GET /v1/ready`. Short, so a
/// probe fails fast instead of piling up behind a hung database.
pub const DEFAULT_READY_TIMEOUT: Duration = Duration::from_secs(2);

/// How long one `GET /v1/ready` answer is reused. The endpoint is public, so
/// without it every caller could make the server read both stores at will.
pub const DEFAULT_READY_CACHE: Duration = Duration::from_secs(1);

/// How `GET /v1/ready` checks the stores.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReadyOptions {
    /// Bound on each store's check.
    pub timeout: Duration,
    /// How long an answer is reused; zero checks on every call.
    pub cache: Duration,
}

impl Default for ReadyOptions {
    fn default() -> Self {
        ReadyOptions {
            timeout: DEFAULT_READY_TIMEOUT,
            cache: DEFAULT_READY_CACHE,
        }
    }
}

/// A ref name no client is expected to write; `get_ref` on it is one indexed
/// read that proves the metadata store answers.
const READY_PROBE_REF: &str = "leep-ready-probe";

/// Default cap on how long one request may run. Garbage collection's fence
/// must exceed it (ADR 0012): a request that checked a blob before it was
/// condemned has stored its changeset or been aborted by then.
pub const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(5 * 60);

const DEFAULT_REFLOG_LIMIT: usize = 50;
const MAX_REFLOG_LIMIT: usize = 1000;
const DEFAULT_LOG_LIMIT: usize = 100;
/// Upper bound on `limit` so one request can't walk unbounded history.
const MAX_LOG_LIMIT: usize = 10_000;
/// Default and maximum number of changesets a provenance request scans.
const MAX_PROVENANCE_LIMIT: usize = 10_000;

/// Build the router with the default body limit.
pub fn router(repo: Repo) -> Router {
    router_with_limit(repo, DEFAULT_MAX_BODY_BYTES)
}

/// Build the router, rejecting request bodies over `max_body_bytes` with 413.
pub fn router_with_limit(repo: Repo, max_body_bytes: usize) -> Router {
    router_with_auth(repo, max_body_bytes, None)
}

/// Build the router; with `Some(auth)` every endpoint except `GET /v1/health`
/// requires a bearer token. `None` leaves the API open.
pub fn router_with_auth(repo: Repo, max_body_bytes: usize, auth: Option<auth::Auth>) -> Router {
    router_with_signing(repo, max_body_bytes, auth, None)
}

/// Like [`router_with_auth`], plus the trusted keys that
/// `POST /v1/changesets/{id}/signatures` accepts. Without keys, adding a
/// signature is a 400. The land policy is set on `repo`, not here.
pub fn router_with_signing(
    repo: Repo,
    max_body_bytes: usize,
    auth: Option<auth::Auth>,
    signing: Option<Arc<signing::SigningKeys>>,
) -> Router {
    router_with_review(repo, max_body_bytes, auth, signing, None)
}

/// Like [`router_with_signing`], plus the review service behind the review
/// endpoints. Reviews need authentication: without `auth` all three endpoints
/// are a 400. With `auth` and no service, a default one (no auto rules) is
/// built from the tokens. The land policy is set on `repo`, not here.
pub fn router_with_review(
    repo: Repo,
    max_body_bytes: usize,
    auth: Option<auth::Auth>,
    signing: Option<Arc<signing::SigningKeys>>,
    review: Option<Arc<review::ReviewService>>,
) -> Router {
    router_with_timeout(
        repo,
        max_body_bytes,
        auth,
        signing,
        review,
        DEFAULT_REQUEST_TIMEOUT,
    )
}

/// Like [`router_with_review`], aborting any request that runs longer than
/// `request_timeout` with 408. Zero disables the bound, which also disables
/// the guarantee garbage collection's fence relies on.
pub fn router_with_timeout(
    repo: Repo,
    max_body_bytes: usize,
    auth: Option<auth::Auth>,
    signing: Option<Arc<signing::SigningKeys>>,
    review: Option<Arc<review::ReviewService>>,
    request_timeout: Duration,
) -> Router {
    router_with_limits(
        repo,
        max_body_bytes,
        auth,
        signing,
        review,
        request_timeout,
        None,
    )
}

/// Like [`router_with_timeout`], plus per-principal request limits (ADR 0020).
/// Build the [`limits::Limiter`] from the same `auth` so token names match.
/// The config is fixed; for one that reloads see [`router_with_config`].
pub fn router_with_limits(
    repo: Repo,
    max_body_bytes: usize,
    auth: Option<auth::Auth>,
    signing: Option<Arc<signing::SigningKeys>>,
    review: Option<Arc<review::ReviewService>>,
    request_timeout: Duration,
    limits: Option<Arc<limits::Limiter>>,
) -> Router {
    let config =
        reload::RuntimeConfig::fixed(auth, signing, review, limits, repo.land_policy().cloned());
    router_with_config(
        repo,
        max_body_bytes,
        request_timeout,
        reload::ConfigHandle::fixed(config),
    )
}

/// Build the router over a [`reload::ConfigHandle`] (ADR 0024). Tokens,
/// limits, signing keys and the land policy come from the handle's current
/// config, taken once per request; the land policy set on `repo` is ignored.
pub fn router_with_config(
    repo: Repo,
    max_body_bytes: usize,
    request_timeout: Duration,
    config: Arc<reload::ConfigHandle>,
) -> Router {
    router_with_ready_timeout(
        repo,
        max_body_bytes,
        request_timeout,
        config,
        DEFAULT_READY_TIMEOUT,
    )
}

/// Like [`router_with_config`], with the bound on each store check behind
/// `GET /v1/ready` (default [`DEFAULT_READY_TIMEOUT`]); answers are reused for
/// [`DEFAULT_READY_CACHE`].
pub fn router_with_ready_timeout(
    repo: Repo,
    max_body_bytes: usize,
    request_timeout: Duration,
    config: Arc<reload::ConfigHandle>,
    ready_timeout: Duration,
) -> Router {
    router_with_body_limits(
        repo,
        max_body_bytes,
        limits::BodyLimits::under(max_body_bytes),
        request_timeout,
        config,
        ReadyOptions {
            timeout: ready_timeout,
            ..Default::default()
        },
    )
}

/// Like [`router_with_ready_timeout`], with the server-wide body limits:
/// per-route caps and the in-flight byte budget. `max_body_bytes` stays a
/// ceiling on every body. Read classes get their default limits.
pub fn router_with_body_limits(
    repo: Repo,
    max_body_bytes: usize,
    body_limits: limits::BodyLimits,
    request_timeout: Duration,
    config: Arc<reload::ConfigHandle>,
    ready_options: ReadyOptions,
) -> Router {
    router_with_read_limits(
        repo,
        max_body_bytes,
        body_limits,
        request_timeout,
        config,
        ready_options,
        read_limits::ReadConcurrency::default(),
    )
}

/// Like [`router_with_body_limits`], with the concurrency limits of the
/// expensive read classes (ADR 0060, `--read-concurrency`).
pub fn router_with_read_limits(
    repo: Repo,
    max_body_bytes: usize,
    body_limits: limits::BodyLimits,
    request_timeout: Duration,
    config: Arc<reload::ConfigHandle>,
    ready_options: ReadyOptions,
    read_concurrency: read_limits::ReadConcurrency,
) -> Router {
    router_with_read_classes(
        repo,
        max_body_bytes,
        body_limits,
        request_timeout,
        config,
        ready_options,
        read_limits::ReadLimits::new(&read_concurrency),
    )
}

/// Like [`router_with_read_limits`], with read classes built by the caller,
/// who shares them with the blame warmer (ADR 0083).
pub fn router_with_read_classes(
    repo: Repo,
    max_body_bytes: usize,
    body_limits: limits::BodyLimits,
    request_timeout: Duration,
    config: Arc<reload::ConfigHandle>,
    ready_options: ReadyOptions,
    read_limits: Arc<read_limits::ReadLimits>,
) -> Router {
    register_ready_metrics();
    let budget = limits::BodyBudget::new(body_limits);
    let repo = Arc::new(repo);
    let ready_state = Arc::new(ReadyProbe {
        repo: repo.clone(),
        options: ready_options,
        last: tokio::sync::Mutex::new(None),
        warming: start_warmup(&repo),
    });
    let router = Router::new()
        .route("/v1/health", get(health))
        .route("/v1/ready", get(ready).with_state(ready_state))
        .route("/v1/whoami", get(whoami))
        .route("/v1/blobs/missing", post(missing_blobs))
        .route("/v1/blobs/{hash}", get(get_blob).put(put_blob))
        .route("/v1/changesets", post(create_changeset))
        .route("/v1/changesets/{id}", get(get_changeset))
        .route(
            "/v1/changesets/{id}/signatures",
            get(get_signatures).post(add_signature),
        )
        .route(
            "/v1/changesets/{id}/reviews",
            get(get_reviews).post(add_review),
        )
        .route("/v1/changesets/{id}/review-status", get(review_status))
        .route(
            "/v1/changesets/{id}/checks",
            get(checks::list).post(checks::post),
        )
        .route(
            "/v1/review-requests",
            get(review_requests::list).post(review_requests::create),
        )
        .route("/v1/review-requests/{id}", get(review_requests::get_one))
        .route(
            "/v1/review-requests/{id}/withdraw",
            post(review_requests::withdraw),
        )
        .route("/v1/refs", get(list_refs))
        .route("/v1/refs/{*name}", get(get_ref).post(post_ref))
        .route("/v1/reflog", get(ref_log_all))
        .route("/v1/reflog/{*name}", get(ref_log_one))
        .route("/v1/resolve/{*rev}", get(resolve))
        .route("/v1/resolve-prefix/{prefix}", get(resolve_prefix))
        .route("/v1/tree/{id}", get(tree))
        .route("/v1/files/{id}", get(files))
        .route("/v1/file/{id}", get(file))
        .route("/v1/log/{id}", get(log))
        .route("/v1/last-change/{id}", get(last_change))
        .route("/v1/diff", get(diff))
        .route("/v1/blame/{id}", get(blame))
        .route("/v1/provenance/{id}", get(provenance))
        .route("/v1/search/{*rev}", get(search_rev))
        .route("/v1/merge-base", get(merge_base))
        .route("/v1/merge", post(merge))
        .route("/v1/rebase", post(rebase))
        .route("/v1/land", post(land));
    // Innermost: a request spends its token's limits before it takes a
    // read class permit (ADR 0060).
    let router = router.layer(middleware::from_fn_with_state(
        read_limits,
        read_limits::enforce,
    ));
    // Just inside authentication, which names the principal, and still before
    // any body is read.
    let router = router
        .layer(middleware::from_fn(limits::enforce))
        .layer(Extension(budget));
    // Inside authentication, so a request without a token is a 401 first,
    // and before any body is read.
    let router = router.layer(middleware::from_fn(guard::check));
    // Next in, so a rejected request never has its body read.
    let router = router.layer(middleware::from_fn(auth::require_token));
    // Outside both: the request keeps this config snapshot to its end.
    let router = router.layer(middleware::from_fn_with_state(config, reload::snapshot));
    let router = router
        .layer(DefaultBodyLimit::disable())
        .layer(RequestBodyLimitLayer::new(max_body_bytes));
    let router = if request_timeout.is_zero() {
        router
    } else {
        router.layer(TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            request_timeout,
        ))
    };
    router
        // Outside the body cap and the timeout, whose answers are plain text.
        .layer(middleware::from_fn(errors::json_errors))
        .layer(observe::trace_layer())
        .layer(middleware::from_fn(observe::track_metrics))
        // Outermost, so the log span, every handler and every answer see it.
        .layer(middleware::from_fn(observe::request_id))
        .with_state(repo)
}

/// Check the ref names a land policy protects (`--protect-ref`,
/// `--require-signatures`, `--require-review`, a review policy's `refs`). Protection matches names
/// exactly, so a name no ref can have, such as one with a stray space from a
/// comma-separated environment variable, would leave the intended ref
/// unprotected without a word.
pub fn check_protected_refs(what: &str, refs: &[String]) -> Result<(), String> {
    for r in refs {
        leep_core::refname::validate(r).map_err(|e| {
            format!(
                "{what}: {}",
                e.to_string().trim_start_matches("invalid input: ")
            )
        })?;
    }
    Ok(())
}

/// Serve `app` on `listener` until `shutdown` resolves, with the default
/// [`conn::ServeOptions`]; see [`conn::serve_with`].
pub async fn serve(
    listener: tokio::net::TcpListener,
    app: Router,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> std::io::Result<()> {
    conn::serve_with(listener, app, shutdown, conn::ServeOptions::default()).await
}

type S = State<Arc<Repo>>;
type ApiResult<T> = Result<T, ApiError>;

/// What `GET /v1/ready` advertises.
fn features() -> Vec<String> {
    vec![
        leep_core::api::FEATURE_LAND_ID.into(),
        leep_core::api::FEATURE_INDEXING_RETRY.into(),
        leep_core::api::FEATURE_CHANGESET_RESCUE.into(),
        leep_core::api::FEATURE_LAND_CLEAN_BLOBS.into(),
    ]
}

/// The request's `Leep-Indexing` (ADR 0058).
fn freshness(headers: &HeaderMap) -> Freshness {
    Freshness::from_header(headers.get(INDEXING_HEADER).and_then(|v| v.to_str().ok()))
}

/// `resp` with `Leep-Index-Scanned`: the changesets (files, for search)
/// without index rows the answer examined.
fn with_scanned(mut resp: Response, scanned: u64) -> Response {
    resp.headers_mut()
        .insert(INDEX_SCANNED_HEADER, HeaderValue::from(scanned));
    resp
}

/// The `error` of every 500 the handlers answer; `request_id` beside it
/// finds the detail in the server log.
pub const INTERNAL_ERROR_MESSAGE: &str =
    "internal server error; the server log has the details under this request_id";

/// The single place `leep_core::Error` becomes an HTTP status.
#[derive(Debug)]
pub struct ApiError(Error);

impl From<Error> for ApiError {
    fn from(e: Error) -> Self {
        ApiError(e)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = match &self.0 {
            Error::Invalid(_)
            | Error::InvalidPath(..)
            | Error::InvalidHash(_)
            | Error::LandIdReused { .. } => StatusCode::BAD_REQUEST,
            // Well-formed but refused by the ref's rules (ADR 0085). Old
            // clients treat any other 4xx as 400, by `code` and marker.
            Error::Policy(_) => StatusCode::UNPROCESSABLE_ENTITY,
            Error::NotFound(_) => StatusCode::NOT_FOUND,
            Error::Conflict(_) => StatusCode::CONFLICT,
            Error::Indexing { .. } | Error::Refused { .. } => StatusCode::SERVICE_UNAVAILABLE,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        if let Error::Refused {
            refused,
            retry_after_ms,
            message,
        } = &self.0
        {
            // Refused before any work (ADR 0060): not a failure, so not
            // logged as one, and `Leep-Refused` tells the client and any
            // balancer that sending it again is safe.
            let body = ErrorResponse::new(message.clone()).with_code(error_code::UNAVAILABLE);
            let mut resp = (status, Json(body)).into_response();
            let h = resp.headers_mut();
            h.insert(
                header::RETRY_AFTER,
                HeaderValue::from(retry_after_ms.div_ceil(1000).max(1)),
            );
            h.insert(
                leep_core::api::REFUSED_HEADER,
                HeaderValue::from_static(refused),
            );
            return resp;
        }
        if let Error::Indexing {
            kind,
            behind,
            retry_after_ms,
        } = &self.0
        {
            // An expected answer, not a failure: a client retries it later
            // (ADR 0058). Balancers should not count it against the backend.
            let body = ErrorResponse {
                error: self.0.to_string(),
                indexing: Some(IndexingInfo {
                    kind: kind.clone(),
                    behind: *behind,
                    retry_after_ms: *retry_after_ms,
                }),
                code: Some(error_code::INDEXING.into()),
                request_id: None,
                land: None,
            };
            let mut resp = (status, Json(body)).into_response();
            let h = resp.headers_mut();
            h.insert(
                header::RETRY_AFTER,
                HeaderValue::from(retry_after_ms.div_ceil(1000).max(1)),
            );
            if let Ok(v) = HeaderValue::from_str(&format!("kind={kind}; behind={behind}")) {
                h.insert(INDEXING_HEADER, v);
            }
            return resp;
        }
        if status.is_server_error() {
            // The detail (a database or object store message, a path) stays
            // in the log, where the request span names the request id; the
            // client gets that id with a generic message.
            tracing::error!(error = %self.0, "request failed");
            let body = ErrorResponse::new(INTERNAL_ERROR_MESSAGE.to_string())
                .with_code(errors::code_of(&self.0));
            return (status, Json(body)).into_response();
        }
        let mut body = ErrorResponse::new(self.0.to_string()).with_code(errors::code_of(&self.0));
        if let Error::LandIdReused { refusal, .. } = self.0 {
            body.land = Some(refusal);
        }
        (status, Json(body)).into_response()
    }
}

fn parse_json<T: DeserializeOwned>(body: &[u8]) -> ApiResult<T> {
    serde_json::from_slice(body).map_err(|e| Error::Invalid(format!("bad json: {e}")).into())
}

fn hash(s: &str) -> ApiResult<Hash> {
    Ok(s.parse()?)
}

fn cid(s: &str) -> ApiResult<ChangesetId> {
    Ok(s.parse()?)
}

#[derive(Deserialize)]
struct PathQuery {
    path: String,
}

#[derive(Deserialize)]
struct DirQuery {
    path: Option<String>,
}

#[derive(Deserialize)]
struct FilesQuery {
    prefix: Option<String>,
    /// Text, so a bad value is our 400 and not the extractor's.
    #[serde(default)]
    limit: Option<String>,
    #[serde(default)]
    after: Option<String>,
}

/// `limit` of `GET /v1/files` and `/v1/diff`: absent is the whole list (what
/// older clients expect, so it is not capped; see [`MAX_PATHS_LIMIT`]),
/// above [`MAX_PATHS_LIMIT`] is lowered to it, and 0 or not a number is 400.
fn paths_limit(limit: Option<&str>) -> ApiResult<Option<usize>> {
    let Some(v) = limit else {
        return Ok(None);
    };
    match v.parse::<usize>() {
        Ok(0) | Err(_) => Err(Error::Invalid(format!(
            "bad limit {v:?}: expected a number from 1 to {MAX_PATHS_LIMIT}"
        ))
        .into()),
        Ok(n) => Ok(Some(n.min(MAX_PATHS_LIMIT))),
    }
}

/// `extra` may repeat, which a plain struct can't express, so the log query is
/// read as raw pairs.
#[derive(Debug, Default, PartialEq, Eq)]
struct LogQuery {
    path: Option<String>,
    limit: Option<usize>,
    follow: bool,
    filter: LogFilter,
    /// The previous page's `next` cursor.
    after: Option<String>,
    fields: LogFields,
}

impl LogQuery {
    fn parse(pairs: Vec<(String, String)>) -> ApiResult<Self> {
        let mut q = LogQuery::default();
        for (k, v) in pairs {
            match k.as_str() {
                "path" => q.path = Some(v),
                "limit" => {
                    q.limit = Some(
                        v.parse()
                            .map_err(|_| Error::Invalid(format!("bad limit {v:?}")))?,
                    )
                }
                "follow" => q.follow = parse_bool("follow", &v)?,
                "after" => q.after = Some(v),
                "fields" => q.fields = LogFields::parse(&v)?,
                "author" => q.filter.author = Some(v),
                "extra" => q.filter.extra.push(ExtraMatch::parse(&v)?),
                _ => {}
            }
        }
        Ok(q)
    }
}

fn parse_bool(name: &str, v: &str) -> ApiResult<bool> {
    match v {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(Error::Invalid(format!("bad {name} {v:?}: expected true or false")).into()),
    }
}

#[derive(Deserialize)]
struct ProvenanceQuery {
    path: Option<String>,
    limit: Option<usize>,
}

#[derive(Deserialize)]
struct SearchParams {
    q: String,
    regex: Option<bool>,
    i: Option<bool>,
    path: Option<String>,
    glob: Option<String>,
    limit: Option<usize>,
    after: Option<String>,
}

#[derive(Deserialize)]
struct DiffQuery {
    from: Option<String>,
    to: String,
    prefix: Option<String>,
    renames: Option<bool>,
    /// Text, so a bad value is our 400 and not the extractor's.
    rename_threshold: Option<String>,
    #[serde(default)]
    limit: Option<String>,
    #[serde(default)]
    after: Option<String>,
}

impl DiffQuery {
    fn rename_options(&self) -> ApiResult<RenameOptions> {
        let mut opts = RenameOptions::default();
        if let Some(t) = &self.rename_threshold {
            opts.threshold = t.parse::<u8>().ok().filter(|t| *t <= 100).ok_or_else(|| {
                Error::Invalid(format!("bad rename_threshold {t:?}: expected 0 to 100"))
            })?;
        }
        Ok(opts)
    }
}

#[derive(Deserialize)]
struct MergeBaseQuery {
    a: String,
    b: String,
}

/// Liveness: the process is up and answering HTTP. Checks nothing else, so an
/// orchestrator that restarts on this does not restart a healthy server just
/// because a database is down; `/v1/ready` is for that.
async fn health() -> Json<&'static str> {
    Json("ok")
}

async fn whoami(auth::MaybePrincipal(who): auth::MaybePrincipal) -> Json<WhoAmIResponse> {
    Json(match who {
        Some(p) => WhoAmIResponse {
            role: Some(p.role.as_str().to_string()),
            name: Some(p.name),
            author: p.author,
        },
        None => WhoAmIResponse::default(),
    })
}

/// Whether a request is a public probe: no token, no per-token limits.
pub(crate) fn is_probe(method: &axum::http::Method, path: &str) -> bool {
    method == axum::http::Method::GET && matches!(path, "/v1/health" | "/v1/ready")
}

fn register_ready_metrics() {
    for store in ["meta", "blobs"] {
        for result in ["ok", "error", "timeout"] {
            metrics::counter!("leep_ready_checks_total", "store" => store, "result" => result)
                .increment(0);
        }
    }
}

/// One store's check under `limit`: `None` when it passed, else the reason to
/// report. The error itself is only logged, since it may name a host or carry
/// a connection string.
async fn check_store<F>(store: &'static str, limit: Duration, probe: F) -> Option<ReadyFailure>
where
    F: std::future::Future<Output = leep_core::Result<()>>,
{
    let (result, failure) = match tokio::time::timeout(limit, probe).await {
        Ok(Ok(())) => ("ok", None),
        Ok(Err(e)) => {
            tracing::warn!(store, error = %e, "readiness check failed");
            ("error", Some("unavailable".to_string()))
        }
        Err(_) => {
            tracing::warn!(
                store,
                timeout_ms = limit.as_millis() as u64,
                "readiness check timed out"
            );
            (
                "timeout",
                Some(format!("timed out after {}ms", limit.as_millis())),
            )
        }
    };
    metrics::counter!("leep_ready_checks_total", "store" => store, "result" => result).increment(1);
    failure.map(|reason| ReadyFailure {
        store: store.to_string(),
        reason,
    })
}

/// The state behind `GET /v1/ready`: the stores, and the last answer.
struct ReadyProbe {
    repo: Arc<Repo>,
    options: ReadyOptions,
    /// When the last check finished and what failed. Held across a check, so
    /// callers that arrive meanwhile wait for its answer instead of starting
    /// their own: at most one check per cache period, whatever the load.
    last: tokio::sync::Mutex<Option<(tokio::time::Instant, Vec<ReadyFailure>)>>,
    /// `true` while the blob store's first load (a pack index, 321 MB at 1M
    /// changesets) runs. Probing it meanwhile would only time out at the
    /// deadline, so `blobs` is reported as loading until it finishes.
    warming: tokio::sync::watch::Receiver<bool>,
}

/// Starts the blob store's warm-up in the background; the returned receiver
/// reads `true` until it ends. A failed warm-up is logged and then ends
/// too: the probe's own blob check reports the failure from then on. With
/// no runtime to run it on, the store is simply not warmed.
fn start_warmup(repo: &Arc<Repo>) -> tokio::sync::watch::Receiver<bool> {
    let Ok(rt) = tokio::runtime::Handle::try_current() else {
        return tokio::sync::watch::channel(false).1;
    };
    let (tx, rx) = tokio::sync::watch::channel(true);
    let blobs = repo.blobs().clone();
    rt.spawn(async move {
        let began = std::time::Instant::now();
        match blobs.warm().await {
            Ok(()) => tracing::info!(
                elapsed_ms = began.elapsed().as_millis() as u64,
                "blob store loaded"
            ),
            Err(e) => tracing::warn!(error = %e, "blob store warm-up failed"),
        }
        let _ = tx.send(false);
    });
    rx
}

impl ReadyProbe {
    async fn failures(&self) -> Vec<ReadyFailure> {
        if self.options.cache.is_zero() {
            return self.check().await;
        }
        let mut last = self.last.lock().await;
        if let Some((at, failed)) = &*last {
            if at.elapsed() < self.options.cache {
                return failed.clone();
            }
        }
        let failed = self.check().await;
        *last = Some((tokio::time::Instant::now(), failed.clone()));
        failed
    }

    async fn check(&self) -> Vec<ReadyFailure> {
        let limit = self.options.timeout;
        let meta = self.repo.meta().clone();
        let blobs = self.repo.blobs().clone();
        let mut warming = self.warming.clone();
        let (m, b) = tokio::join!(
            check_store("meta", limit, async move {
                meta.get_ref(READY_PROBE_REF).await.map(|_| ())
            }),
            async move {
                // Give a load that is nearly done the check's own time to
                // finish; a slower one is reported, not probed.
                let loaded = tokio::time::timeout(limit, async {
                    // Ok or Err alike: a closed channel means it ended.
                    let _ = warming.wait_for(|w| !*w).await;
                })
                .await;
                if loaded.is_err() {
                    return Some(ReadyFailure {
                        store: "blobs".to_string(),
                        reason: "loading".to_string(),
                    });
                }
                check_store("blobs", limit, async move {
                    blobs
                        .contains(&Hash::of_blob(READY_PROBE_BLOB))
                        .await
                        .map(|_| ())
                })
                .await
            },
        );
        [m, b].into_iter().flatten().collect()
    }
}

/// Readiness: both stores answer a cheap real read, concurrently, each within
/// the ready timeout. 200 `{"status":"ready","features":[...]}`, or 503 naming the failed
/// stores. An answer is reused for the cache period (1 s by default). Public
/// and unlimited like `/v1/health` (see [`is_probe`]).
async fn ready(State(probe): State<Arc<ReadyProbe>>) -> Response {
    let failed = probe.failures().await;
    let (status, body) = if failed.is_empty() {
        (
            StatusCode::OK,
            ReadyResponse {
                status: "ready".into(),
                failed,
                features: features(),
            },
        )
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            ReadyResponse {
                status: "not ready".into(),
                failed,
                features: features(),
            },
        )
    };
    (status, Json(body)).into_response()
}

/// Content whose hash the blob check asks about; whether the store holds it
/// does not matter, only that it answers.
const READY_PROBE_BLOB: &[u8] = b"leep ready probe";

async fn put_blob(State(repo): S, Path(h): Path<String>, body: Bytes) -> ApiResult<StatusCode> {
    let h = hash(&h)?;
    if Hash::of_blob(&body) != h {
        return Err(Error::Invalid(format!("body does not hash to {h}")).into());
    }
    repo.put_blob(body).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// Clients fetch hashes they read from stored trees, so a miss is asked
/// again exactly (ADR 0070): a land another server just made is not a 404.
async fn get_blob(State(repo): S, Path(h): Path<String>) -> ApiResult<Response> {
    let h = hash(&h)?;
    let data = repo
        .read_content(&h)
        .await?
        .ok_or_else(|| Error::NotFound(format!("blob {h}")))?;
    Ok(octet_stream(data, []))
}

fn octet_stream<const N: usize>(data: Bytes, extra: [(&'static str, &'static str); N]) -> Response {
    let mut resp = data.into_response();
    let h = resp.headers_mut();
    h.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    for (k, v) in extra {
        h.insert(k, HeaderValue::from_static(v));
    }
    resp
}

async fn missing_blobs(State(repo): S, body: Bytes) -> ApiResult<Json<MissingBlobsResponse>> {
    let req: MissingBlobsRequest = parse_json(&body)?;
    if req.hashes.len() > MAX_MISSING_BLOBS {
        return Err(Error::Invalid(format!(
            "at most {MAX_MISSING_BLOBS} hashes per request, got {}; split the list",
            req.hashes.len()
        ))
        .into());
    }
    let missing = repo.missing_blobs(&req.hashes).await?;
    Ok(Json(MissingBlobsResponse { missing }))
}

/// The creating token is recorded with the changeset (ADR 0086).
async fn create_changeset(
    State(repo): S,
    auth::MaybePrincipal(who): auth::MaybePrincipal,
    body: Bytes,
) -> ApiResult<Json<CreateChangesetResponse>> {
    let cs: Changeset = parse_json(&body)?;
    let id = repo
        .create_changeset_by(cs, who.as_ref().map(|p| p.name.as_str()))
        .await?;
    Ok(Json(CreateChangesetResponse { id }))
}

/// `{id}` of a read endpoint that also takes a revision (ADR 0082): a full
/// id as it is, else whatever `/v1/resolve/{rev}` resolves, so a client
/// asking for a ref saves the round trip of resolving it first. Says
/// whether it resolved; the time and reads that took go into `timing`.
async fn id_or_rev(
    repo: &Repo,
    s: &str,
    timing: &mut timing::ServerTiming,
) -> ApiResult<(ChangesetId, bool)> {
    if s.len() == 64 {
        if let Ok(id) = s.parse() {
            return Ok((id, false));
        }
    }
    let started = Instant::now();
    let (id, reads) = read_stats::measure(repo.resolve(s)).await;
    timing
        .phase("resolve", started.elapsed())
        .reads("resolve-db", reads);
    Ok((id?, true))
}

/// `resp` naming the changeset a revision resolved to ([`RESOLVED_HEADER`]).
fn with_resolved(mut resp: Response, id: ChangesetId, resolved: bool) -> Response {
    if resolved {
        if let Ok(v) = HeaderValue::from_str(&id.to_string()) {
            resp.headers_mut().insert(RESOLVED_HEADER, v);
        }
    }
    resp
}

async fn get_changeset(State(repo): S, Path(id): Path<String>) -> ApiResult<Response> {
    let mut timing = timing::ServerTiming::new();
    let (id, resolved) = id_or_rev(&repo, &id, &mut timing).await?;
    let started = Instant::now();
    let (info, reads) = read_stats::measure(repo.info(id)).await;
    let info = info?;
    timing.phase("fetch", started.elapsed()).reads("db", reads);
    let started = Instant::now();
    let resp = Json(info).into_response();
    timing.phase("encode", started.elapsed());
    let mut resp = with_resolved(resp, id, resolved);
    timing.apply(&mut resp);
    Ok(resp)
}

/// The config snapshot the request started with.
type Cfg = Extension<Arc<reload::RuntimeConfig>>;

async fn add_signature(
    State(repo): S,
    Extension(config): Cfg,
    Path(id): Path<String>,
    body: Bytes,
) -> ApiResult<Json<AddSignatureResponse>> {
    let id = cid(&id)?;
    let req: AddSignatureRequest = parse_json(&body)?;
    repo.get_changeset(id).await?;
    let keys = config.signing.as_ref().ok_or_else(|| {
        Error::Invalid("this server has no signing keys configured (--signing-keys-file)".into())
    })?;
    let public_key = leep_core::signature::parse_public_key(&req.public_key)?;
    let sig = leep_core::signature::parse_signature_hex(&req.signature)?;
    let key = keys.find(&public_key).ok_or_else(|| {
        Error::Invalid(format!(
            "key {} is not trusted by this server",
            leep_core::signature::key_id(&public_key)
        ))
    })?;
    signing::SigningKeys::verify(key, &id, &sig)?;
    repo.put_signature(id, &signing::stored(key, sig)).await?;
    Ok(Json(AddSignatureResponse {
        key_id: key.key_id.clone(),
        name: key.name.clone(),
    }))
}

async fn get_signatures(
    State(repo): S,
    Extension(config): Cfg,
    Path(id): Path<String>,
) -> ApiResult<Json<SignaturesResponse>> {
    let id = cid(&id)?;
    repo.get_changeset(id).await?;
    let resp = match &config.signing {
        Some(k) => k.describe(&repo, id).await?,
        None => SignaturesResponse {
            signatures: vec![],
            verified: true,
            via: None,
        },
    };
    Ok(Json(resp))
}

const NEEDS_AUTH: &str = "reviews need authentication (start the server with --tokens-file)";

#[derive(Deserialize)]
struct StatusQuery {
    #[serde(rename = "ref")]
    target_ref: Option<String>,
}

async fn add_review(
    State(repo): S,
    Extension(config): Cfg,
    auth::MaybePrincipal(who): auth::MaybePrincipal,
    Path(id): Path<String>,
    body: Bytes,
) -> ApiResult<Response> {
    let forbidden = |why: &str| {
        auth::reject(
            StatusCode::FORBIDDEN,
            leep_core::api::error_code::FORBIDDEN,
            why,
        )
    };
    let id = cid(&id)?;
    let req: AddReviewRequest = parse_json(&body)?;
    // A body naming a reviewer is never taken as the token's own review. With
    // the capability the token speaks for someone else, so its own `review`
    // flag (an agent-style token that approves nothing itself) does not apply.
    if let Some(p) = who.as_ref() {
        if req.reviewer.is_some() {
            if !p.review_for {
                return Ok(forbidden(
                    "token may not review for others: its tokens-file entry lacks \
                     \"review_for\": true",
                ));
            }
        } else if !p.review {
            return Ok(forbidden(
                "token may not review: its tokens-file entry has \"review\": false",
            ));
        }
    }
    repo.get_changeset(id).await?;
    let who = who.ok_or_else(|| Error::Invalid(NEEDS_AUTH.into()))?;
    let reviewer = match req.reviewer {
        Some(name) if name.trim().is_empty() => {
            return Err(Error::Invalid("review reviewer must not be empty".into()).into())
        }
        Some(name) => name,
        None => who.name.clone(),
    };
    leep_core::review::check_source(req.source.as_deref().unwrap_or_default())?;
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs() as i64);
    // Source first: a crash between the two leaves a source with no review
    // behind it, which listing ignores, never a review with a wrong origin.
    // A direct post over an earlier on-behalf one replaces its source row,
    // so the old origin is not shown for the new verdict.
    if req.source.is_some() || repo.get_review_source(id, &reviewer).await?.is_some() {
        repo.put_review_source(
            id,
            &reviewer,
            &leep_core::review::ReviewSource {
                via: who.name.clone(),
                source: req.source.unwrap_or_default(),
                timestamp,
            },
        )
        .await?;
    }
    let review = leep_core::review::Review {
        reviewer: reviewer.clone(),
        verdict: req.verdict,
        comment: req.comment,
        timestamp,
    };
    repo.put_review(id, &review).await?;
    // A reject or withdrawal also takes back the approvals by the same
    // reviewer that carried to `id` (ADR 0085).
    let taken_back = match config.review.as_ref() {
        Some(service) => service.take_back(&repo, id, &review, &who.name).await?,
        None => Vec::new(),
    };
    // A verdict on a changeset already landed is kept (it counts for its
    // copies, a cherry-pick to a release ref), but the client is told
    // (ADR 0087). Only an answer: the verdict is already stored.
    let on_refs = repo
        .refs_holding(id, &policy_refs(&repo, &config).await?)
        .await?;
    Ok(Json(AddReviewResponse {
        reviewer,
        verdict: req.verdict,
        on_refs,
        taken_back,
    })
    .into_response())
}

/// The refs a land policy covers, sorted. A reloadable config knows them
/// by name; a fixed one (handed a policy object) only answers per ref, so
/// the ref list is filtered.
async fn policy_refs(
    repo: &Repo,
    config: &reload::RuntimeConfig,
) -> leep_core::Result<Vec<String>> {
    let Some(policy) = config.land_policy.as_ref() else {
        return Ok(Vec::new());
    };
    if !config.protected_refs().is_empty() {
        return Ok(config.protected_refs().to_vec());
    }
    Ok(repo
        .meta()
        .list_refs()
        .await?
        .into_keys()
        .filter(|name| policy.applies_to(name) && !policy.promotion_only(name))
        .collect())
}

async fn get_reviews(
    State(repo): S,
    Extension(config): Cfg,
    Path(id): Path<String>,
) -> ApiResult<Json<ReviewsResponse>> {
    let id = cid(&id)?;
    repo.get_changeset(id).await?;
    let service = config
        .review
        .as_ref()
        .ok_or_else(|| Error::Invalid(NEEDS_AUTH.into()))?;
    Ok(Json(service.describe(&repo, id).await?))
}

async fn review_status(
    State(repo): S,
    Extension(config): Cfg,
    Path(id): Path<String>,
    Query(q): Query<StatusQuery>,
) -> ApiResult<Json<ReviewStatusResponse>> {
    let id = cid(&id)?;
    repo.get_changeset(id).await?;
    let service = config
        .review
        .as_ref()
        .ok_or_else(|| Error::Invalid(NEEDS_AUTH.into()))?;
    let target = q.target_ref.unwrap_or_else(|| "main".into());
    let refs = policy_refs(&repo, &config).await?;
    Ok(Json(service.status(&repo, id, &target, &refs).await?))
}

async fn list_refs(State(repo): S) -> ApiResult<Json<RefsResponse>> {
    let refs: BTreeMap<_, _> = repo.meta().list_refs().await?;
    Ok(Json(RefsResponse { refs }))
}

/// `GET /v1/refs/{name}`, and `.../checks` and `.../culprits` of a ref
/// (ADR 0091): ref names may hold `/`, so they share the route. A ref whose
/// own name ends in one of those suffixes is answered as itself only when
/// the ref before the suffix does not exist.
async fn get_ref(
    State(repo): S,
    Extension(config): Cfg,
    Path(name): Path<String>,
    q: checks::MaybeCulprits,
) -> ApiResult<Response> {
    // The suffix wins whenever the ref it names exists, so a ref called
    // `main/checks` cannot stand in for `main`'s check status.
    if let Some(base) = name.strip_suffix(CHECKS_SUFFIX) {
        if repo.meta().get_ref(base).await?.is_some() {
            return checks::for_ref(&repo, &config, base).await;
        }
    }
    if let Some(base) = name.strip_suffix(CULPRITS_SUFFIX) {
        if repo.meta().get_ref(base).await?.is_some() {
            return checks::culprits(&repo, base, q.0).await;
        }
    }
    let target = repo.meta().get_ref(&name).await?;
    if target.is_none() && (name.ends_with(CHECKS_SUFFIX) || name.ends_with(CULPRITS_SUFFIX)) {
        // Neither the ref nor the one it would be about.
        return Err(Error::NotFound(format!("ref {name:?}")).into());
    }
    Ok(Json(RefResponse { name, target }).into_response())
}

const CHECKS_SUFFIX: &str = "/checks";
const CULPRITS_SUFFIX: &str = "/culprits";

/// Operations only the server itself may record: lands, the git import and
/// promotions of known-good refs (ADR 0091).
const RESERVED_OPERATIONS: [&str; 3] = ["land", "import", leep_core::check::PROMOTE_OPERATION];

/// The ref log context of a client-driven ref move. The actor is the token's
/// name; the label and message are the client's, within limits, so entries
/// stay one readable line and a client cannot pose as a land or an import.
fn ref_context(who: Option<&auth::Principal>, req: &CasRefRequest) -> ApiResult<RefUpdateContext> {
    let op = req.operation.as_deref().unwrap_or("cas");
    let ok = !op.is_empty()
        && op.len() <= 32
        && op
            .bytes()
            .all(|b| matches!(b, b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.'));
    if !ok || RESERVED_OPERATIONS.contains(&op) {
        return Err(Error::Invalid(format!(
            "bad operation {op:?}: expected up to 32 of a-z 0-9 - _ . and not {}",
            RESERVED_OPERATIONS.join(", ")
        ))
        .into());
    }
    let mut ctx = RefUpdateContext::new(op).actor(who.map(|p| p.name.as_str()));
    if let Some(m) = &req.message {
        if m.len() > 256 || m.chars().any(char::is_control) {
            return Err(Error::Invalid(
                "bad message: expected up to 256 bytes without control characters".into(),
            )
            .into());
        }
        ctx = ctx.message(m.as_str());
    }
    Ok(ctx)
}

/// `POST /v1/refs/{name}` moves a ref, and `POST /v1/refs/{name}/rebase-onto`
/// replays it: ref names may hold `/`, so both share the route. A ref whose
/// name ends in `/rebase-onto` cannot be moved by `POST`; land onto it.
#[allow(clippy::too_many_arguments)]
async fn post_ref(
    State(repo): S,
    Extension(config): Cfg,
    who: auth::MaybePrincipal,
    coordination: Option<Extension<Arc<coord::Coordination>>>,
    from_peer: Option<Extension<coord::FromPeer>>,
    headers: HeaderMap,
    Path(name): Path<String>,
    body: Bytes,
) -> ApiResult<Response> {
    match name.strip_suffix(REBASE_ONTO_SUFFIX) {
        Some(name) => {
            rebase_onto(
                repo,
                config,
                who,
                coordination,
                from_peer,
                headers,
                name,
                body,
            )
            .await
        }
        None => cas_ref(repo, config, who, &name, body).await,
    }
}

const REBASE_ONTO_SUFFIX: &str = "/rebase-onto";

/// Replay a git-primary ref onto commits git gained (ADR 0062), queued with
/// the ref's lands on the server coordinating it.
#[allow(clippy::too_many_arguments)]
async fn rebase_onto(
    repo: Arc<Repo>,
    config: Arc<reload::RuntimeConfig>,
    auth::MaybePrincipal(who): auth::MaybePrincipal,
    coordination: Option<Extension<Arc<coord::Coordination>>>,
    from_peer: Option<Extension<coord::FromPeer>>,
    headers: HeaderMap,
    name: &str,
    body: Bytes,
) -> ApiResult<Response> {
    let req: RebaseOntoRequest = parse_json(&body)?;
    let repo = repo.with_land_policy_replaced(config.land_policy.clone());
    let actor = who.as_ref().map(|p| p.name.as_str());
    let Some(Extension(coordination)) = coordination.filter(|_| repo.land_coordinator().is_some())
    else {
        return Ok(Json(
            repo.rebase_onto(name, req.onto, req.expected_base, actor, req.land_id)
                .await?,
        )
        .into_response());
    };
    let forward = coord::HttpForward::new(coordination, &headers, body)
        .to(format!("v1/refs/{name}{REBASE_ONTO_SUFFIX}"));
    use leep_derive::land_coord::Coordinated;
    Ok(
        match repo
            .rebase_onto_coordinated(
                name,
                req.onto,
                req.expected_base,
                actor,
                req.land_id,
                from_peer.is_some(),
                &forward,
            )
            .await?
        {
            Coordinated::Landed(r) => Json(r).into_response(),
            Coordinated::Forwarded(relayed) => relayed.into_response(),
            Coordinated::NotLeader => coord::not_leader(),
        },
    )
}

async fn cas_ref(
    repo: Arc<Repo>,
    config: Arc<reload::RuntimeConfig>,
    auth::MaybePrincipal(who): auth::MaybePrincipal,
    name: &str,
    body: Bytes,
) -> ApiResult<Response> {
    let req: CasRefRequest = parse_json(&body)?;
    let ctx = ref_context(who.as_ref(), &req)?;
    // One policy snapshot for the whole move: protection, forward-only, checks.
    let repo = repo.with_land_policy_replaced(config.land_policy.clone());
    let won = repo
        .cas_ref_checked(name, req.expected, req.new, &ctx)
        .await?;
    let current = repo.meta().get_ref(name).await?;
    let status = if won {
        StatusCode::OK
    } else {
        StatusCode::CONFLICT
    };
    Ok((status, Json(CasRefResponse { ok: won, current })).into_response())
}

#[derive(Deserialize)]
struct RefLogQuery {
    /// Text, so a bad value is our 400 and not the extractor's.
    before: Option<String>,
    limit: Option<String>,
    after: Option<String>,
}

async fn ref_log(
    repo: &Repo,
    name: Option<&str>,
    q: RefLogQuery,
) -> ApiResult<Json<RefLogResponse>> {
    fn num<T: std::str::FromStr>(what: &str, v: Option<String>) -> ApiResult<Option<T>> {
        v.map(|v| {
            v.parse()
                .map_err(|_| Error::Invalid(format!("bad {what} {v:?}: expected a number")).into())
        })
        .transpose()
    }
    let before = num::<u64>("before", q.before)?;
    let limit = num::<usize>("limit", q.limit)?
        .unwrap_or(DEFAULT_REFLOG_LIMIT)
        .min(MAX_REFLOG_LIMIT);
    Ok(Json(
        repo.ref_log_page(name, before, q.after.as_deref(), limit)
            .await?,
    ))
}

async fn ref_log_all(
    State(repo): S,
    Query(q): Query<RefLogQuery>,
) -> ApiResult<Json<RefLogResponse>> {
    ref_log(&repo, None, q).await
}

async fn ref_log_one(
    State(repo): S,
    Path(name): Path<String>,
    Query(q): Query<RefLogQuery>,
) -> ApiResult<Json<RefLogResponse>> {
    ref_log(&repo, Some(&name), q).await
}

async fn resolve(State(repo): S, Path(rev): Path<String>) -> ApiResult<Response> {
    let started = Instant::now();
    let (id, reads) = read_stats::measure(repo.resolve(&rev)).await;
    let mut timing = timing::ServerTiming::new();
    timing
        .phase("resolve", started.elapsed())
        .reads("db", reads);
    let mut resp = Json(ResolveResponse { id: id? }).into_response();
    timing.apply(&mut resp);
    Ok(resp)
}

/// A short id: 200 with the one match, or 400 listing the candidates.
async fn resolve_prefix(State(repo): S, Path(prefix): Path<String>) -> ApiResult<Response> {
    Ok(match repo.resolve_prefix(&prefix).await? {
        PrefixMatch::One(id) => Json(ResolvePrefixResponse { id }).into_response(),
        PrefixMatch::Many {
            candidates,
            truncated,
        } => {
            let count = if truncated {
                format!("more than {}", candidates.len())
            } else {
                candidates.len().to_string()
            };
            let body = AmbiguousPrefixResponse {
                error: format!(
                    "invalid input: short id {} is ambiguous: it matches {count} changesets; \
                     give more digits",
                    prefix.to_ascii_lowercase()
                ),
                candidates,
                truncated,
                code: Some(error_code::AMBIGUOUS_PREFIX.into()),
            };
            (StatusCode::BAD_REQUEST, Json(body)).into_response()
        }
    })
}

async fn tree(
    State(repo): S,
    Path(id): Path<String>,
    Query(q): Query<DirQuery>,
) -> ApiResult<Json<TreeResponse>> {
    let dir = q.path.unwrap_or_default();
    Ok(Json(repo.list_dir(cid(&id)?, &dir).await?))
}

async fn files(
    State(repo): S,
    Path(id): Path<String>,
    Query(q): Query<FilesQuery>,
) -> ApiResult<Json<FilesResponse>> {
    let limit = paths_limit(q.limit.as_deref())?;
    let prefix = q.prefix.unwrap_or_default();
    Ok(Json(
        repo.list_files_page(cid(&id)?, &prefix, limit, q.after.as_deref())
            .await?,
    ))
}

async fn file(
    State(repo): S,
    Path(id): Path<String>,
    Query(q): Query<PathQuery>,
) -> ApiResult<Response> {
    let path = RepoPath::new(q.path)?;
    let (info, data) = repo
        .read_file(cid(&id)?, &path)
        .await?
        .ok_or_else(|| Error::NotFound(format!("file {path}")))?;
    let kind = match info.kind {
        FileKind::Regular => "regular",
        FileKind::Executable => "executable",
        FileKind::Symlink => "symlink",
    };
    Ok(octet_stream(data, [("x-leep-kind", kind)]))
}

async fn log(
    State(repo): S,
    Path(id): Path<String>,
    headers: HeaderMap,
    Query(pairs): Query<Vec<(String, String)>>,
) -> ApiResult<Response> {
    let q = LogQuery::parse(pairs)?;
    let mut timing = timing::ServerTiming::new();
    let (id, resolved) = id_or_rev(&repo, &id, &mut timing).await?;
    let limit = q.limit.unwrap_or(DEFAULT_LOG_LIMIT).min(MAX_LOG_LIMIT);
    let params = LogParams {
        path: q.path,
        follow: q.follow,
        filter: q.filter,
    };
    let started = Instant::now();
    let (answer, reads) = read_stats::measure(repo.log_page_indexed(
        id,
        &params,
        limit,
        q.after.as_deref(),
        freshness(&headers),
    ))
    .await;
    let Answered {
        value: page,
        answered_at,
        behind,
        scanned,
    } = answer?;
    timing.phase("walk", started.elapsed()).reads("db", reads);
    let started = Instant::now();
    let tail = LogTail {
        next: page.next,
        answered_at,
        behind,
    };
    let resp = match q.fields {
        LogFields::Full => log_json(page.entries, tail),
        LogFields::Summary => {
            let entries: Vec<LogSummaryEntry> = page.entries.into_iter().map(Into::into).collect();
            log_json(entries, tail)
        }
    };
    // A long log streams: this is only the first chunks' share.
    timing.phase("encode", started.elapsed());
    let mut resp = with_resolved(with_scanned(resp, scanned), id, resolved);
    timing.apply(&mut resp);
    Ok(resp)
}

/// Entries one task encodes of a long log.
const LOG_CHUNK_ENTRIES: usize = 512;

/// `Json` of a [`LogResponse`] (or, for summary entries, a
/// [`LogSummaryResponse`]) made of `entries` and `tail`, byte for byte. For a long log chunks of entries are
/// encoded on blocking threads, several at once, and streamed in order, so
/// the first bytes leave while later entries are encoded. 10,000 entries
/// are tens of megabytes of JSON. The walk has finished, so a failure of
/// the store still answers with an error status; encoding these types
/// cannot fail.
fn log_json<E>(entries: Vec<E>, tail: LogTail) -> Response
where
    E: Serialize + Send + 'static,
{
    if entries.len() <= LOG_CHUNK_ENTRIES {
        return Json(LogBody {
            entries,
            next: tail.next,
            answered_at: tail.answered_at,
            behind: tail.behind,
        })
        .into_response();
    }
    let mut chunks = Vec::with_capacity(entries.len().div_ceil(LOG_CHUNK_ENTRIES));
    let mut rest = entries.into_iter();
    loop {
        let chunk: Vec<E> = rest.by_ref().take(LOG_CHUNK_ENTRIES).collect();
        if chunk.is_empty() {
            break;
        }
        chunks.push(chunk);
    }
    let last = chunks.len() - 1;
    let threads = std::thread::available_parallelism().map_or(1, |n| n.get().min(4));
    let body = futures_util::stream::iter(chunks.into_iter().enumerate())
        .map(move |(i, chunk)| {
            let tail = (i == last).then(|| tail.clone());
            tokio::task::spawn_blocking(move || encode_log_chunk(&chunk, i == 0, tail))
        })
        .buffered(threads)
        .map(|r| match r {
            Ok(Ok(bytes)) => Ok(bytes),
            Ok(Err(e)) => Err(std::io::Error::other(e)),
            Err(e) => Err(std::io::Error::other(e)),
        });
    (
        [(header::CONTENT_TYPE, "application/json")],
        axum::body::Body::from_stream(body),
    )
        .into_response()
}

/// The shape of [`LogResponse`] and [`LogSummaryResponse`], whichever
/// entries it holds.
#[derive(Serialize)]
struct LogBody<E> {
    entries: Vec<E>,
    #[serde(skip_serializing_if = "Option::is_none")]
    next: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    answered_at: Option<ChangesetId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    behind: Option<u64>,
}

/// The fields of a `LogResponse` after its entries.
#[derive(Clone)]
struct LogTail {
    next: Option<String>,
    answered_at: Option<ChangesetId>,
    behind: Option<u64>,
}

/// Part of a `LogResponse` body: `chunk`'s entries, comma separated, opened
/// by the object's head if `first` or by a comma if not, closed (with the
/// fields after the entries, if any) with `tail` on the last chunk.
fn encode_log_chunk<E: Serialize>(
    chunk: &[E],
    first: bool,
    tail: Option<LogTail>,
) -> serde_json::Result<Bytes> {
    let mut out = Vec::with_capacity(chunk.len() * 1024);
    out.extend_from_slice(if first { &b"{\"entries\":["[..] } else { b"," });
    for (k, entry) in chunk.iter().enumerate() {
        if k > 0 {
            out.push(b',');
        }
        serde_json::to_writer(&mut out, entry)?;
    }
    if let Some(tail) = tail {
        out.push(b']');
        if let Some(next) = tail.next {
            out.extend_from_slice(b",\"next\":");
            serde_json::to_writer(&mut out, &next)?;
        }
        if let Some(at) = tail.answered_at {
            out.extend_from_slice(b",\"answered_at\":");
            serde_json::to_writer(&mut out, &at)?;
        }
        if let Some(behind) = tail.behind {
            out.extend_from_slice(b",\"behind\":");
            serde_json::to_writer(&mut out, &behind)?;
        }
        out.push(b'}');
    }
    Ok(Bytes::from(out))
}

async fn last_change(
    State(repo): S,
    Path(id): Path<String>,
    headers: HeaderMap,
    Query(q): Query<PathQuery>,
) -> ApiResult<Response> {
    let path = RepoPath::new(q.path.trim_matches('/'))?;
    let Answered {
        value: (changeset, concurrent),
        answered_at,
        behind,
        scanned,
    } = repo
        .last_change_indexed(cid(&id)?, path.as_str(), freshness(&headers))
        .await?;
    let resp = Json(LastChangeResponse {
        path: path.as_str().to_string(),
        changeset,
        concurrent,
        answered_at,
        behind,
    })
    .into_response();
    Ok(with_scanned(resp, scanned))
}

async fn provenance(
    State(repo): S,
    Path(id): Path<String>,
    Query(q): Query<ProvenanceQuery>,
) -> ApiResult<Json<ProvenanceResponse>> {
    let limit = q
        .limit
        .unwrap_or(MAX_PROVENANCE_LIMIT)
        .min(MAX_PROVENANCE_LIMIT);
    Ok(Json(
        repo.provenance(cid(&id)?, q.path.as_deref(), limit).await?,
    ))
}

async fn search_rev(
    State(repo): S,
    Path(rev): Path<String>,
    headers: HeaderMap,
    Query(p): Query<SearchParams>,
) -> ApiResult<Response> {
    let q = SearchQuery {
        regex: p.regex.unwrap_or(false),
        case_insensitive: p.i.unwrap_or(false),
        path: p.path.unwrap_or_default(),
        glob: p.glob.filter(|g| !g.is_empty()),
        limit: p
            .limit
            .unwrap_or(search::DEFAULT_LIMIT)
            .clamp(1, search::MAX_LIMIT),
        ..SearchQuery::new(p.q)
    };
    let answered = repo
        .search_page_indexed(&rev, &q, p.after.as_deref(), freshness(&headers))
        .await?;
    Ok(with_scanned(
        Json(answered.value).into_response(),
        answered.scanned,
    ))
}

async fn diff(State(repo): S, Query(q): Query<DiffQuery>) -> ApiResult<Json<DiffResponse>> {
    let from = q.from.as_deref().map(cid).transpose()?;
    let opts = q.rename_options()?;
    let limit = paths_limit(q.limit.as_deref())?;
    let renames = (q.renames == Some(true)).then_some(&opts);
    Ok(Json(
        repo.diff_page(
            from,
            cid(&q.to)?,
            q.prefix.as_deref().unwrap_or_default(),
            renames,
            limit,
            q.after.as_deref(),
        )
        .await?,
    ))
}

async fn blame(
    State(repo): S,
    Path(id): Path<String>,
    headers: HeaderMap,
    permit: Option<Extension<read_limits::ReadPermit>>,
    Query(q): Query<PathQuery>,
) -> ApiResult<Response> {
    let path = RepoPath::new(q.path)?;
    // The blame finishes past a timeout (ADR 0078) and keeps the class's
    // permit until it does.
    let hold = permit.map(|Extension(p)| Box::new(p) as leep_derive::BlameHold);
    let Answered {
        mut value,
        answered_at,
        behind,
        scanned,
    } = repo
        .blame_indexed_held(cid(&id)?, &path, freshness(&headers), hold)
        .await?;
    value.answered_at = answered_at;
    value.behind = behind;
    Ok(with_scanned(Json(value).into_response(), scanned))
}

async fn merge_base(
    State(repo): S,
    Query(q): Query<MergeBaseQuery>,
) -> ApiResult<Json<MergeBaseResponse>> {
    let base = repo.merge_base(cid(&q.a)?, cid(&q.b)?).await?;
    Ok(Json(MergeBaseResponse { base }))
}

/// The caller is recorded as the merge's creator (ADR 0086), so it can no
/// more attest it than a changeset it posted (ADR 0091).
async fn merge(
    State(repo): S,
    auth::MaybePrincipal(who): auth::MaybePrincipal,
    body: Bytes,
) -> ApiResult<Json<MergeResponse>> {
    let actor = who.as_ref().map(|p| p.name.as_str());
    Ok(Json(repo.merge_by(parse_json(&body)?, actor).await?))
}

async fn rebase(State(repo): S, body: Bytes) -> ApiResult<Json<RebaseResponse>> {
    let req: RebaseRequest = parse_json(&body)?;
    Ok(Json(repo.rebase(req.changeset, req.onto).await?))
}

async fn land(
    State(repo): S,
    Extension(config): Cfg,
    auth::MaybePrincipal(who): auth::MaybePrincipal,
    coordination: Option<Extension<Arc<coord::Coordination>>>,
    from_peer: Option<Extension<coord::FromPeer>>,
    headers: axum::http::HeaderMap,
    body: Bytes,
) -> ApiResult<Response> {
    let req: LandRequest = parse_json(&body)?;
    // The policy the request arrived under, even if the body took a while or
    // a reload happened since; `land` keeps it across its retries.
    let repo = repo.with_land_policy_replaced(config.land_policy.clone());
    let actor = who.as_ref().map(|p| p.name.as_str());
    // Old clients send no `create` and keep the original behaviour.
    let create = req.create.unwrap_or(true);
    let Some(Extension(coordination)) = coordination.filter(|_| repo.land_coordinator().is_some())
    else {
        let resp = repo
            .land_keyed(req.changeset, &req.target_ref, actor, create, req.land_id)
            .await?;
        review_requests::after_land(
            repo.clone(),
            &config,
            req.target_ref,
            req.changeset,
            actor,
            &resp,
        );
        return Ok(Json(resp).into_response());
    };
    // The client's body byte for byte, so fields this server does not know
    // reach the coordinating server unchanged.
    let forward = coord::HttpForward::new(coordination, &headers, body);
    use leep_derive::land_coord::Coordinated;
    Ok(
        match repo
            .land_coordinated(
                req.changeset,
                &req.target_ref,
                actor,
                create,
                req.land_id,
                from_peer.is_some(),
                &forward,
            )
            .await?
        {
            Coordinated::Landed(r) => {
                review_requests::after_land(
                    repo.clone(),
                    &config,
                    req.target_ref,
                    req.changeset,
                    actor,
                    &r,
                );
                Json(r).into_response()
            }
            Coordinated::Forwarded(relayed) => relayed.into_response(),
            Coordinated::NotLeader => coord::not_leader(),
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pairs(q: &[(&str, &str)]) -> Vec<(String, String)> {
        q.iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    /// The chunked encoding of a long log is the one-piece encoding, byte
    /// for byte, on each side of the chunk boundaries.
    #[tokio::test]
    async fn log_json_is_the_one_piece_encoding() {
        use http_body_util::BodyExt;
        let entry = |i: usize| ChangesetInfo {
            id: leep_core::ChangesetId(Hash::of_blob(format!("{i}").as_bytes())),
            changeset: Changeset {
                parents: vec![leep_core::ChangesetId(Hash::of_blob(b"p"))],
                author: format!("a\"{i}\" \u{e9}"),
                timestamp: i as i64,
                message: format!("line\n\tnext {i}"),
                changes: BTreeMap::from([
                    (
                        RepoPath::new(format!("d/{i}")).unwrap(),
                        leep_core::FileChange::Modify {
                            content: Hash::of_blob(b"x"),
                            kind: FileKind::Executable,
                            size: i as u64,
                        },
                    ),
                    (
                        RepoPath::new("gone").unwrap(),
                        leep_core::FileChange::Delete,
                    ),
                ]),
                extra: BTreeMap::from([("agent.model".to_string(), "m".to_string())]),
            },
            generation: i as u64 + 1,
            root_tree: Hash::of_blob(b"root"),
            path_at: None,
            renamed: None,
            created_by: None,
        };
        let n = LOG_CHUNK_ENTRIES;
        for len in [0, 1, n - 1, n, n + 1, 2 * n, 2 * n + 1, 5 * n + 17] {
            for (next, stale) in [
                (None, None),
                (Some("AQE-_x".to_string()), None),
                (None, Some((ChangesetId(Hash::of_blob(b"at")), 3))),
                (
                    Some("AQE-_y".to_string()),
                    Some((ChangesetId(Hash::of_blob(b"at")), 9)),
                ),
            ] {
                let entries: Vec<ChangesetInfo> = (0..len).map(entry).collect();
                let log = LogResponse {
                    entries: entries.clone(),
                    next: next.clone(),
                    answered_at: stale.map(|s| s.0),
                    behind: stale.map(|s| s.1),
                };
                let want = serde_json::to_vec(&log).unwrap();
                let tail = LogTail {
                    next: log.next.clone(),
                    answered_at: log.answered_at,
                    behind: log.behind,
                };
                let resp = log_json(log.entries.clone(), tail.clone());
                assert_eq!(resp.headers()[header::CONTENT_TYPE], "application/json");
                let got = resp.into_body().collect().await.unwrap().to_bytes();
                assert_eq!(got, want, "{len} entries, next {next:?}");
                // Summary entries: the same, for `LogSummaryResponse`.
                let summary = LogSummaryResponse::from(log);
                let want = serde_json::to_vec(&summary).unwrap();
                let resp = log_json(summary.entries, tail);
                let got = resp.into_body().collect().await.unwrap().to_bytes();
                assert_eq!(got, want, "summary: {len} entries, next {next:?}");
            }
        }
    }

    #[test]
    fn log_query_collects_repeated_extra() {
        let q = LogQuery::parse(pairs(&[
            ("extra", "agent.model:m"),
            ("limit", "5"),
            ("extra", "agent.session"),
            ("author", "Ann"),
            ("unknown", "ignored"),
        ]))
        .unwrap();
        assert_eq!(q.limit, Some(5));
        assert_eq!(q.filter.author.as_deref(), Some("Ann"));
        assert_eq!(
            q.filter.extra,
            [
                ExtraMatch {
                    key: "agent.model".into(),
                    value: Some("m".into())
                },
                ExtraMatch {
                    key: "agent.session".into(),
                    value: None
                }
            ]
        );
    }

    #[test]
    fn log_query_reads_fields() {
        assert_eq!(LogQuery::parse(pairs(&[])).unwrap().fields, LogFields::Full);
        for (v, want) in [("full", LogFields::Full), ("summary", LogFields::Summary)] {
            assert_eq!(
                LogQuery::parse(pairs(&[("fields", v)])).unwrap().fields,
                want
            );
        }
        assert!(LogQuery::parse(pairs(&[("fields", "changes")])).is_err());
    }

    #[test]
    fn log_query_rejects_malformed_values() {
        assert!(LogQuery::parse(pairs(&[("extra", ":x")])).is_err());
        assert!(LogQuery::parse(pairs(&[("extra", "")])).is_err());
        assert!(LogQuery::parse(pairs(&[("limit", "many")])).is_err());
        assert!(LogQuery::parse(pairs(&[("follow", "yes")])).is_err());
    }

    #[test]
    fn log_query_follow_defaults_to_false() {
        assert!(!LogQuery::parse(pairs(&[("path", "a")])).unwrap().follow);
        let q = LogQuery::parse(pairs(&[("path", "a"), ("follow", "true")])).unwrap();
        assert!(q.follow);
    }

    fn diff_query(threshold: Option<&str>) -> DiffQuery {
        DiffQuery {
            from: None,
            to: String::new(),
            prefix: None,
            renames: None,
            rename_threshold: threshold.map(String::from),
            limit: None,
            after: None,
        }
    }

    #[test]
    fn rename_threshold_is_validated() {
        assert_eq!(diff_query(None).rename_options().unwrap().threshold, 50);
        for (t, want) in [("0", 0), ("50", 50), ("100", 100)] {
            let o = diff_query(Some(t)).rename_options().unwrap();
            assert_eq!(o.threshold, want);
        }
        for bad in ["101", "-1", "", "50.5", "fifty", "256", "1000"] {
            assert!(diff_query(Some(bad)).rename_options().is_err(), "{bad:?}");
        }
    }
}
