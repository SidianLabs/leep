//! The Leep HTTP server: a thin axum layer over [`leep_derive::Repo`].
//! The endpoint contract is the table in `leep_core::api`.

pub mod auth;
pub mod observe;
pub mod review;
pub mod signing;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, Path, Query, State};
use axum::http::{header, HeaderValue, StatusCode};
use axum::middleware;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Extension, Json, Router};
use leep_core::api::*;
use leep_core::rename::RenameOptions;
use leep_core::{Changeset, ChangesetId, Error, FileKind, Hash, RepoPath};
use leep_derive::{search, ExtraMatch, LogFilter, Repo, SearchQuery};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use tower_http::limit::RequestBodyLimitLayer;
use tower_http::timeout::TimeoutLayer;

/// Default cap on request bodies (blob uploads), in bytes.
pub const DEFAULT_MAX_BODY_BYTES: usize = 512 * 1024 * 1024;

/// Default cap on how long one request may run. Garbage collection's fence
/// must exceed it (ADR 0012): a request that checked a blob before it was
/// condemned has stored its changeset or been aborted by then.
pub const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(5 * 60);

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
    let review = review.or_else(|| {
        auth.as_ref().map(|a| {
            Arc::new(review::ReviewService::new(
                a.reviewers(),
                leep_derive::review::ReviewRules::default(),
            ))
        })
    });
    let router = Router::new()
        .route("/v1/health", get(health))
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
        .route("/v1/resolve/{*rev}", get(resolve))
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
        .route("/v1/land", post(land))
        .layer(Extension(signing))
        .layer(Extension(review));
    // Innermost, so a rejected request never has its body read.
    let router = match auth {
        Some(a) => router.layer(middleware::from_fn_with_state(
            Arc::new(a),
            auth::require_token,
        )),
        None => router,
    };
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
        .layer(observe::trace_layer())
        .layer(middleware::from_fn(observe::track_metrics))
        .with_state(Arc::new(repo))
}

/// Serve `app` on `listener` until `shutdown` resolves.
pub async fn serve(
    listener: tokio::net::TcpListener,
    app: Router,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> std::io::Result<()> {
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown)
        .await
}

type S = State<Arc<Repo>>;
type ApiResult<T> = Result<T, ApiError>;

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
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        if status.is_server_error() {
            tracing::error!(error = %self.0, "request failed");
        }
        let body = ErrorResponse {
            error: self.0.to_string(),
        };
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

async fn health() -> Json<&'static str> {
    Json("ok")
}

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

type Keys = Extension<Option<Arc<signing::SigningKeys>>>;

async fn add_signature(
    State(repo): S,
    Extension(keys): Keys,
    Path(id): Path<String>,
    body: Bytes,
) -> ApiResult<Json<AddSignatureResponse>> {
    let id = cid(&id)?;
    let req: AddSignatureRequest = parse_json(&body)?;
    repo.get_changeset(id).await?;
    let keys = keys.ok_or_else(|| {
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
    Extension(keys): Keys,
    Path(id): Path<String>,
) -> ApiResult<Json<SignaturesResponse>> {
    let id = cid(&id)?;
    repo.get_changeset(id).await?;
    let resp = match keys {
        Some(k) => k.describe(&repo, id).await?,
        None => SignaturesResponse {
            signatures: vec![],
            verified: true,
            via: None,
        },
    };
    Ok(Json(resp))
}

type Reviews = Extension<Option<Arc<review::ReviewService>>>;

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
) -> ApiResult<Json<AddReviewResponse>> {
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
    }))
}

async fn get_reviews(
    State(repo): S,
    Extension(service): Reviews,
    Path(id): Path<String>,
) -> ApiResult<Json<ReviewsResponse>> {
    let id = cid(&id)?;
    repo.get_changeset(id).await?;
    let service = service.ok_or_else(|| Error::Invalid(NEEDS_AUTH.into()))?;
    Ok(Json(service.describe(&repo, id).await?))
}

async fn review_status(
    State(repo): S,
    Extension(service): Reviews,
    Path(id): Path<String>,
    Query(q): Query<StatusQuery>,
) -> ApiResult<Json<ReviewStatusResponse>> {
    let id = cid(&id)?;
    repo.get_changeset(id).await?;
    let service = service.ok_or_else(|| Error::Invalid(NEEDS_AUTH.into()))?;
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

async fn cas_ref(State(repo): S, Path(name): Path<String>, body: Bytes) -> ApiResult<Response> {
    let req: CasRefRequest = parse_json(&body)?;
    let won = repo.cas_ref_checked(&name, req.expected, req.new).await?;
    let current = repo.meta().get_ref(&name).await?;
    let status = if won {
        StatusCode::OK
    } else {
        StatusCode::CONFLICT
    };
    Ok((status, Json(CasRefResponse { ok: won, current })).into_response())
}

async fn resolve(State(repo): S, Path(rev): Path<String>) -> ApiResult<Json<ResolveResponse>> {
    Ok(Json(ResolveResponse {
        id: repo.resolve(&rev).await?,
    }))
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
    Query(pairs): Query<Vec<(String, String)>>,
) -> ApiResult<Json<LogResponse>> {
    let q = LogQuery::parse(pairs)?;
    let limit = q.limit.unwrap_or(DEFAULT_LOG_LIMIT).min(MAX_LOG_LIMIT);
    let entries = if q.follow {
        let path = q
            .path
            .as_deref()
            .filter(|p| !p.trim_matches('/').is_empty());
        let path = path.ok_or_else(|| Error::Invalid("follow needs a file path".into()))?;
        repo.log_follow(cid(&id)?, path, limit, &q.filter).await?
    } else {
        repo.log_filtered(cid(&id)?, q.path.as_deref(), limit, &q.filter)
            .await?
    };
    Ok(Json(LogResponse { entries }))
}

async fn last_change(
    State(repo): S,
    Path(id): Path<String>,
    Query(q): Query<PathQuery>,
) -> ApiResult<Json<LastChangeResponse>> {
    let path = RepoPath::new(q.path.trim_matches('/'))?;
    let (changeset, concurrent) = repo.last_change(cid(&id)?, path.as_str()).await?;
    Ok(Json(LastChangeResponse {
        path: path.as_str().to_string(),
        changeset,
        concurrent,
    }))
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
    Query(p): Query<SearchParams>,
) -> ApiResult<Json<SearchResponse>> {
    let id = repo.resolve(&rev).await?;
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
    Ok(Json(repo.search(id, &q).await?))
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
    Query(q): Query<PathQuery>,
) -> ApiResult<Json<BlameResponse>> {
    let path = RepoPath::new(q.path)?;
    Ok(Json(repo.blame(cid(&id)?, &path).await?))
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

async fn land(State(repo): S, body: Bytes) -> ApiResult<Json<LandResponse>> {
    let req: LandRequest = parse_json(&body)?;
    Ok(Json(repo.land(req.changeset, &req.target_ref).await?))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pairs(q: &[(&str, &str)]) -> Vec<(String, String)> {
        q.iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
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
