//! The Leep HTTP server: a thin axum layer over [`leep_derive::Repo`].
//! The endpoint contract is the table in `leep_core::api`.

pub mod auth;
pub mod backup;
pub mod coord;
pub mod errors;
pub mod fingerprint;
pub mod limits;
pub mod observe;
pub mod protect;
pub mod reload;
pub mod review;
pub mod signing;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

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
use leep_derive::{search, ExtraMatch, LogFilter, LogParams, PrefixMatch, Repo, SearchQuery};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tower_http::limit::RequestBodyLimitLayer;
use tower_http::timeout::TimeoutLayer;

/// Default cap on request bodies (blob uploads), in bytes.
pub const DEFAULT_MAX_BODY_BYTES: usize = 512 * 1024 * 1024;

/// Default cap on request bodies from `read` tokens, which cannot upload: the
/// largest body they send is a `POST /v1/blobs/missing` list.
pub const DEFAULT_READ_MAX_BODY_BYTES: usize = 1024 * 1024;

/// Default bound on each store check behind `GET /v1/ready`. Short, so a
/// probe fails fast instead of piling up behind a hung database.
pub const DEFAULT_READY_TIMEOUT: Duration = Duration::from_secs(2);

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
/// `GET /v1/ready` (default [`DEFAULT_READY_TIMEOUT`]).
pub fn router_with_ready_timeout(
    repo: Repo,
    max_body_bytes: usize,
    request_timeout: Duration,
    config: Arc<reload::ConfigHandle>,
    ready_timeout: Duration,
) -> Router {
    register_ready_metrics();
    let repo = Arc::new(repo);
    let ready_state = (repo.clone(), ready_timeout);
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
        .route("/v1/refs", get(list_refs))
        .route("/v1/refs/{*name}", get(get_ref).post(cas_ref))
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
    // Just inside authentication, which names the principal, and still before
    // any body is read.
    let router = router.layer(middleware::from_fn(limits::enforce));
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

/// Serve `app` on `listener` until `shutdown` resolves.
pub async fn serve(
    listener: tokio::net::TcpListener,
    app: Router,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> std::io::Result<()> {
    // The peer address keys anonymous request limits (ADR 0020).
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown)
    .await
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
            Error::Invalid(_) | Error::InvalidPath(..) | Error::InvalidHash(_) => {
                StatusCode::BAD_REQUEST
            }
            Error::NotFound(_) => StatusCode::NOT_FOUND,
            Error::Conflict(_) => StatusCode::CONFLICT,
            Error::Indexing { .. } => StatusCode::SERVICE_UNAVAILABLE,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
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
            tracing::error!(error = %self.0, "request failed");
        }
        let body = ErrorResponse::new(self.0.to_string()).with_code(errors::code_of(&self.0));
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
struct PrefixQuery {
    prefix: Option<String>,
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

/// Readiness: both stores answer a cheap real read, concurrently, each within
/// the ready timeout. 200 `{"status":"ready","features":[...]}`, or 503 naming the failed
/// stores. Public and unlimited like `/v1/health` (see [`is_probe`]).
async fn ready(State((repo, limit)): State<(Arc<Repo>, Duration)>) -> Response {
    let meta = repo.meta().clone();
    let blobs = repo.blobs().clone();
    let (m, b) = tokio::join!(
        check_store("meta", limit, async move {
            meta.get_ref(READY_PROBE_REF).await.map(|_| ())
        }),
        check_store("blobs", limit, async move {
            blobs
                .contains(&Hash::of_blob(READY_PROBE_BLOB))
                .await
                .map(|_| ())
        }),
    );
    let failed: Vec<ReadyFailure> = [m, b].into_iter().flatten().collect();
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

async fn get_blob(State(repo): S, Path(h): Path<String>) -> ApiResult<Response> {
    let h = hash(&h)?;
    let data = repo
        .blobs()
        .get(&h)
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

async fn create_changeset(State(repo): S, body: Bytes) -> ApiResult<Json<CreateChangesetResponse>> {
    let cs: Changeset = parse_json(&body)?;
    let id = repo.create_changeset(cs).await?;
    Ok(Json(CreateChangesetResponse { id }))
}

async fn get_changeset(State(repo): S, Path(id): Path<String>) -> ApiResult<Json<ChangesetInfo>> {
    Ok(Json(repo.info(cid(&id)?).await?))
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
    auth::MaybePrincipal(who): auth::MaybePrincipal,
    Path(id): Path<String>,
    body: Bytes,
) -> ApiResult<Response> {
    if who.as_ref().is_some_and(|p| !p.review) {
        return Ok(auth::reject(
            StatusCode::FORBIDDEN,
            leep_core::api::error_code::FORBIDDEN,
            "token may not review: its tokens-file entry has \"review\": false",
        ));
    }
    let id = cid(&id)?;
    let req: AddReviewRequest = parse_json(&body)?;
    repo.get_changeset(id).await?;
    let who = who.ok_or_else(|| Error::Invalid(NEEDS_AUTH.into()))?;
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs() as i64);
    repo.put_review(
        id,
        &leep_core::review::Review {
            reviewer: who.name.clone(),
            verdict: req.verdict,
            comment: req.comment,
            timestamp,
        },
    )
    .await?;
    Ok(Json(AddReviewResponse {
        reviewer: who.name,
        verdict: req.verdict,
    })
    .into_response())
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
    Ok(Json(service.status(&repo, id, &target).await?))
}

async fn list_refs(State(repo): S) -> ApiResult<Json<RefsResponse>> {
    let refs: BTreeMap<_, _> = repo.meta().list_refs().await?;
    Ok(Json(RefsResponse { refs }))
}

async fn get_ref(State(repo): S, Path(name): Path<String>) -> ApiResult<Json<RefResponse>> {
    let target = repo.meta().get_ref(&name).await?;
    Ok(Json(RefResponse { name, target }))
}

/// Operations only the server itself may record.
const RESERVED_OPERATIONS: [&str; 2] = ["land", "import"];

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
            RESERVED_OPERATIONS.join(" or ")
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

async fn cas_ref(
    State(repo): S,
    Extension(config): Cfg,
    auth::MaybePrincipal(who): auth::MaybePrincipal,
    Path(name): Path<String>,
    body: Bytes,
) -> ApiResult<Response> {
    let req: CasRefRequest = parse_json(&body)?;
    let ctx = ref_context(who.as_ref(), &req)?;
    // One policy snapshot for the whole move: protection, forward-only, checks.
    let repo = repo.with_land_policy_replaced(config.land_policy.clone());
    let won = repo
        .cas_ref_checked(&name, req.expected, req.new, &ctx)
        .await?;
    let current = repo.meta().get_ref(&name).await?;
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

async fn resolve(State(repo): S, Path(rev): Path<String>) -> ApiResult<Json<ResolveResponse>> {
    Ok(Json(ResolveResponse {
        id: repo.resolve(&rev).await?,
    }))
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
    Query(q): Query<PrefixQuery>,
) -> ApiResult<Json<FilesResponse>> {
    let prefix = q.prefix.unwrap_or_default();
    let files = repo.list_files(cid(&id)?, &prefix).await?;
    Ok(Json(FilesResponse { files }))
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
    let limit = q.limit.unwrap_or(DEFAULT_LOG_LIMIT).min(MAX_LOG_LIMIT);
    let params = LogParams {
        path: q.path,
        follow: q.follow,
        filter: q.filter,
    };
    let Answered {
        value: page,
        answered_at,
        behind,
        scanned,
    } = repo
        .log_page_indexed(
            cid(&id)?,
            &params,
            limit,
            q.after.as_deref(),
            freshness(&headers),
        )
        .await?;
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
    Ok(with_scanned(resp, scanned))
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
    let entries = repo
        .diff(from, cid(&q.to)?, q.prefix.as_deref().unwrap_or_default())
        .await?;
    let found = match q.renames {
        Some(true) => repo.detect_renames(&entries, &opts).await?,
        _ => Default::default(),
    };
    Ok(Json(DiffResponse {
        entries,
        renames: found.renames,
        renames_truncated: found.truncated,
    }))
}

async fn blame(
    State(repo): S,
    Path(id): Path<String>,
    headers: HeaderMap,
    Query(q): Query<PathQuery>,
) -> ApiResult<Response> {
    let path = RepoPath::new(q.path)?;
    let Answered {
        mut value,
        answered_at,
        behind,
        scanned,
    } = repo
        .blame_indexed(cid(&id)?, &path, freshness(&headers))
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

async fn merge(State(repo): S, body: Bytes) -> ApiResult<Json<MergeResponse>> {
    Ok(Json(repo.merge(parse_json(&body)?).await?))
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
        return Ok(Json(
            repo.land_keyed(req.changeset, &req.target_ref, actor, create, req.land_id)
                .await?,
        )
        .into_response());
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
            Coordinated::Landed(r) => Json(r).into_response(),
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
