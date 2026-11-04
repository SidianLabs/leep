//! `/v1/review-requests` (ADR 0086): ask for a review, list what waits for
//! a reviewer, and close requests when their changesets land.
//!
//! Identities are token names, as for reviews: the requester is the
//! authenticated principal, and reviewers must be tokens that may review.
//! The repository logic is in `leep_derive` (`review_requests.rs`).

use std::collections::{BTreeSet, HashSet};
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use leep_core::api::{
    error_code, CreateReviewRequest, CreateReviewRequestResponse, LandResponse, ReviewRequestEntry,
    ReviewRequestsResponse,
};
use leep_core::review::Verdict;
use leep_core::review_request::{
    NewReviewRequest, ReviewRequestFilter, ReviewRequestState, TOO_MANY_REQUESTS_MARKER,
};
use leep_core::store::unix_now;
use leep_core::{ChangesetId, Error};
use leep_derive::Repo;
use serde::Deserialize;

use crate::{auth, parse_json, ApiResult, Cfg, S};

const NEEDS_AUTH: &str =
    "review requests need authentication (start the server with --tokens-file)";

/// Default and largest page of a listing.
const DEFAULT_LIMIT: usize = 50;
const MAX_LIMIT: usize = 500;

/// How long closing a land's requests may take before it is left to the
/// next listing.
const CLOSE_TIMEOUT: Duration = Duration::from_secs(10);

pub(crate) async fn create(
    State(repo): S,
    Extension(config): Cfg,
    auth::MaybePrincipal(who): auth::MaybePrincipal,
    body: Bytes,
) -> ApiResult<Response> {
    let req: CreateReviewRequest = parse_json(&body)?;
    // The policy the request arrived under decides which missing refs may
    // be asked about (a protected ref's first land).
    let repo = repo.with_land_policy_replaced(config.land_policy.clone());
    let (Some(who), Some(tokens)) = (who, config.auth.as_ref()) else {
        return Err(Error::Invalid(NEEDS_AUTH.into()).into());
    };
    let may_review: HashSet<String> = tokens.reviewers().into_iter().collect();
    for name in &req.reviewers {
        if name == &who.name {
            return Err(Error::Invalid(format!(
                "{name} cannot be asked to review its own request"
            ))
            .into());
        }
        if !may_review.contains(name) {
            return Err(Error::Invalid(format!(
                "{name:?} is not a reviewer on this server: reviewers are the names of write \
                 tokens that may review"
            ))
            .into());
        }
    }
    repo.get_changeset(req.changeset).await?;
    leep_core::refname::validate(&req.target_ref)?;
    let mut reviewers = req.reviewers.clone();
    if reviewers.is_empty() {
        reviewers = default_reviewers(&repo, &config, &req, &who.name, &may_review).await?;
    }
    let rules = config.review.as_ref().map(|s| s.rules());
    let asked = repo
        .request_review(
            NewReviewRequest {
                changeset: req.changeset,
                target_ref: req.target_ref,
                requester: who.name.clone(),
                reviewers,
                message: req.message,
                created: unix_now(),
            },
            rules,
        )
        .await;
    let (request, existing) = match asked {
        Err(Error::Invalid(m)) if m.contains(TOO_MANY_REQUESTS_MARKER) => {
            return Ok(auth::reject(
                StatusCode::BAD_REQUEST,
                error_code::TOO_MANY_REVIEW_REQUESTS,
                &format!("invalid input: {m}"),
            ));
        }
        r => r?,
    };
    if !existing {
        metrics::counter!("leep_review_requests_created_total").increment(1);
    }
    Ok(Json(CreateReviewRequestResponse {
        request: entry(&repo, &config, request).await?,
        existing,
    })
    .into_response())
}

/// The owners of the paths `req.changeset` still needs approved on its ref
/// (pending or rejected), as review status judges them now, who may review
/// and are not the requester. Empty, meaning any reviewer, when no such
/// path has an owner.
async fn default_reviewers(
    repo: &Repo,
    config: &crate::reload::RuntimeConfig,
    req: &CreateReviewRequest,
    requester: &str,
    may_review: &HashSet<String>,
) -> ApiResult<Vec<String>> {
    let Some(service) = config.review.as_ref() else {
        return Ok(Vec::new());
    };
    let status = service
        .status(repo, req.changeset, &req.target_ref, &[])
        .await?;
    let owners: BTreeSet<String> = status
        .paths
        .iter()
        .filter(|p| p.rule == "pending" || p.rule == "rejected")
        .flat_map(|p| p.owners.iter())
        .filter(|o| o.as_str() != requester && may_review.contains(*o))
        .cloned()
        .collect();
    Ok(owners.into_iter().collect())
}

async fn entry(
    repo: &Repo,
    config: &crate::reload::RuntimeConfig,
    r: leep_core::review_request::ReviewRequest,
) -> ApiResult<ReviewRequestEntry> {
    let mut v = entries(repo, config, vec![r]).await?;
    Ok(v.pop().expect("one entry per request"))
}

/// `reqs` as listed: with the changeset's creator and title, and who has
/// approved or rejected the changeset itself (ADR 0087), each read in one
/// batch for the page.
async fn entries(
    repo: &Repo,
    config: &crate::reload::RuntimeConfig,
    reqs: Vec<leep_core::review_request::ReviewRequest>,
) -> ApiResult<Vec<ReviewRequestEntry>> {
    let mut out = repo.review_request_entries(reqs).await?;
    let Some(service) = config.review.as_ref() else {
        return Ok(out);
    };
    let mut ids: Vec<ChangesetId> = out.iter().map(|e| e.request.changeset).collect();
    ids.sort();
    ids.dedup();
    let verdicts = repo.reviews_by(&ids, service.reviewers()).await?;
    for e in &mut out {
        for r in verdicts.get(&e.request.changeset).into_iter().flatten() {
            match r.verdict {
                Verdict::Approve => e.approved_by.push(r.reviewer.clone()),
                Verdict::Reject => e.rejected_by.push(r.reviewer.clone()),
                Verdict::Withdrawn => {}
            }
        }
        e.approved_by.sort();
        e.rejected_by.sort();
    }
    Ok(out)
}

#[derive(Deserialize)]
pub(crate) struct ListQuery {
    reviewer: Option<String>,
    requester: Option<String>,
    changeset: Option<String>,
    #[serde(rename = "ref")]
    target_ref: Option<String>,
    state: Option<String>,
    open: Option<bool>,
    before: Option<u64>,
    limit: Option<usize>,
}

pub(crate) async fn list(
    State(repo): S,
    Extension(config): Cfg,
    Query(q): Query<ListQuery>,
) -> ApiResult<Json<ReviewRequestsResponse>> {
    let limit = q.limit.unwrap_or(DEFAULT_LIMIT);
    if limit == 0 {
        return Err(Error::Invalid("limit must be at least 1".into()).into());
    }
    let states = match (q.state.as_deref(), q.open) {
        (Some(_), Some(_)) => {
            return Err(Error::Invalid("give state or open, not both".into()).into())
        }
        (Some(s), None) => vec![ReviewRequestState::parse(s)?],
        (None, Some(true)) => vec![ReviewRequestState::Open],
        (None, Some(false)) => vec![ReviewRequestState::Landed, ReviewRequestState::Withdrawn],
        (None, None) => Vec::new(),
    };
    let nonempty = |v: Option<String>, what: &str| -> ApiResult<Option<String>> {
        match v {
            Some(s) if s.trim().is_empty() => {
                Err(Error::Invalid(format!("{what} must not be empty")).into())
            }
            v => Ok(v),
        }
    };
    let filter = ReviewRequestFilter {
        reviewer: nonempty(q.reviewer, "reviewer")?,
        requester: nonempty(q.requester, "requester")?,
        changeset: q.changeset.as_deref().map(crate::cid).transpose()?,
        target_ref: nonempty(q.target_ref, "ref")?,
        states,
        before: q.before,
        limit: limit.min(MAX_LIMIT),
    };
    let rules = config.review.as_ref().map(|s| s.rules());
    let (reqs, next) = repo.review_requests(&filter, rules).await?;
    Ok(Json(ReviewRequestsResponse {
        requests: entries(&repo, &config, reqs).await?,
        next,
    }))
}

pub(crate) async fn get_one(
    State(repo): S,
    Extension(config): Cfg,
    Path(id): Path<String>,
) -> ApiResult<Json<ReviewRequestEntry>> {
    let r = repo.review_request(request_id(&id)?).await?;
    Ok(Json(entry(&repo, &config, r).await?))
}

pub(crate) async fn withdraw(
    State(repo): S,
    Extension(config): Cfg,
    auth::MaybePrincipal(who): auth::MaybePrincipal,
    Path(id): Path<String>,
) -> ApiResult<Response> {
    let id = request_id(&id)?;
    let who = who.ok_or_else(|| Error::Invalid(NEEDS_AUTH.into()))?;
    let r = repo.review_request(id).await?;
    if r.requester != who.name {
        return Ok(auth::reject(
            StatusCode::FORBIDDEN,
            error_code::FORBIDDEN,
            &format!(
                "only {} may withdraw review request {id}, which it made",
                r.requester
            ),
        ));
    }
    let r = repo.withdraw_review_request(id).await?;
    Ok(Json(entry(&repo, &config, r).await?).into_response())
}

fn request_id(s: &str) -> ApiResult<u64> {
    s.parse()
        .map_err(|_| Error::Invalid(format!("bad review request id {s:?}")).into())
}

/// After a land answered `resp`, close the requests it satisfied, in a task
/// of its own: the land has happened, and nothing here may delay or fail
/// its answer. What this misses (an error, a timeout) a listing closes
/// later where it can see it. `actor` ran the land: its own requests close
/// when the land names their changesets (ADR 0087).
pub(crate) fn after_land(
    repo: Repo,
    config: &crate::reload::RuntimeConfig,
    target_ref: String,
    submitted: ChangesetId,
    actor: Option<&str>,
    resp: &LandResponse,
) {
    let LandResponse::Landed { landed, .. } = resp else {
        return;
    };
    if !repo.meta().supports_review_requests() {
        return;
    }
    let landed = *landed;
    let actor = actor.map(str::to_string);
    let rules = config.review.as_ref().map(|s| s.rules().clone());
    tokio::spawn(async move {
        let closing = repo.close_landed_review_requests(
            &target_ref,
            submitted,
            landed,
            actor.as_deref(),
            rules.as_ref(),
        );
        match tokio::time::timeout(CLOSE_TIMEOUT, closing).await {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => {
                metrics::counter!("leep_review_request_close_errors_total").increment(1);
                tracing::warn!(%target_ref, %landed, error = %e, "closing review requests after a land failed");
            }
            Err(_) => {
                metrics::counter!("leep_review_request_close_errors_total").increment(1);
                tracing::warn!(%target_ref, %landed, "closing review requests after a land timed out");
            }
        }
    });
}
