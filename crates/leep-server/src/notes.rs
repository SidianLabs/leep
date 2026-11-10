//! `/v1/notes` (ADR 0098): shared memory between agents, anchored to a
//! path or a changeset.
//!
//! The writer is the token's name (`anonymous` without a tokens file), never
//! a body field, and its trust tier is the token's `kind`. Writing,
//! retracting and reporting need a write token, listing a read token, as
//! for every route (ADR 0007), and the per-token limits of ADR 0020 apply
//! on top of the note caps. Title, body and anchor are agent-written text:
//! they travel only under `untrusted`, and no error message here quotes
//! them. Nothing here is read by a land. The logic is in
//! `leep_derive::notes`.

use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use leep_core::api::{error_code, AddNoteRequest, NoteEntry, NotesResponse};
use leep_core::coord::{
    NoteState, NOTES_PER_DAY_MARKER, NOTES_PER_HOUR_MARKER, REPORTS_PER_HOUR_MARKER,
};
use leep_core::Error;
use leep_derive::notes::NoteQuery;
use serde::Deserialize;

use crate::sessions::{principal, session_key};
use crate::{auth, parse_json, ApiResult, S};

fn note_id(id: &str) -> ApiResult<u64> {
    id.parse()
        .map_err(|_| Error::Invalid(format!("bad note id {id:?}: expected a number")).into())
}

/// The token's trust tier: `human` only for a `"kind": "human"` token.
/// Without a tokens file every note is an agent's.
fn trust(who: &Option<auth::Principal>) -> leep_core::coord::NoteTrust {
    who.as_ref()
        .map_or(auth::TokenKind::Agent, |p| p.kind)
        .note_trust()
}

/// `POST /v1/notes`. Past the hourly limit the answer is 429 with
/// `Retry-After`; superseding another principal's note is 403.
pub(crate) async fn add(
    State(repo): S,
    auth::MaybePrincipal(who): auth::MaybePrincipal,
    headers: HeaderMap,
    body: Bytes,
) -> ApiResult<Response> {
    let req: AddNoteRequest = parse_json(&body)?;
    let key = session_key(&headers);
    match repo
        .add_note(&principal(&who), trust(&who), key.as_deref(), req)
        .await
    {
        Ok(e) => Ok((StatusCode::OK, Json(e)).into_response()),
        Err(Error::Invalid(m))
            if m.contains(NOTES_PER_HOUR_MARKER) || m.contains(NOTES_PER_DAY_MARKER) =>
        {
            Ok(rate_limited(&m))
        }
        Err(Error::Policy(m)) => Ok(auth::reject(
            StatusCode::FORBIDDEN,
            error_code::FORBIDDEN,
            &m,
        )),
        Err(e) => Err(e.into()),
    }
}

#[derive(Deserialize)]
pub(crate) struct ListQuery {
    path: Option<String>,
    changeset: Option<String>,
    principal: Option<String>,
    mine: Option<String>,
    state: Option<String>,
    all: Option<String>,
    before: Option<String>,
    limit: Option<String>,
}

fn flag(name: &str, v: Option<&str>) -> ApiResult<bool> {
    match v {
        None | Some("false") => Ok(false),
        Some("true") => Ok(true),
        Some(v) => Err(Error::Invalid(format!("bad {name} {v:?}: expected true or false")).into()),
    }
}

fn number<T: std::str::FromStr>(name: &str, v: Option<&str>) -> ApiResult<Option<T>> {
    v.map(|v| {
        v.parse()
            .map_err(|_| Error::Invalid(format!("bad {name} {v:?}: expected a number")).into())
    })
    .transpose()
}

/// `GET /v1/notes`.
pub(crate) async fn list(
    State(repo): S,
    auth::MaybePrincipal(who): auth::MaybePrincipal,
    Query(q): Query<ListQuery>,
) -> ApiResult<Json<NotesResponse>> {
    let limit: Option<usize> = number("limit", q.limit.as_deref())?;
    if limit == Some(0) {
        return Err(Error::Invalid("bad limit 0: expected a number from 1 to 100".into()).into());
    }
    let mine = flag("mine", q.mine.as_deref())?;
    let query = NoteQuery {
        path: q.path.filter(|p| !p.is_empty()),
        changeset: q.changeset.filter(|c| !c.is_empty()),
        principal: if mine {
            Some(principal(&who))
        } else {
            q.principal.filter(|p| !p.is_empty())
        },
        state: q
            .state
            .as_deref()
            .filter(|s| !s.is_empty())
            .map(NoteState::parse)
            .transpose()?,
        all: flag("all", q.all.as_deref())?,
        before: number("before", q.before.as_deref())?,
        limit,
    };
    Ok(Json(repo.notes(&query).await?))
}

/// `GET /v1/notes/{id}`.
pub(crate) async fn get_one(State(repo): S, Path(id): Path<String>) -> ApiResult<Json<NoteEntry>> {
    Ok(Json(repo.note(note_id(&id)?).await?))
}

/// `DELETE /v1/notes/{id}`: retract it; only its principal may.
pub(crate) async fn retract(
    State(repo): S,
    auth::MaybePrincipal(who): auth::MaybePrincipal,
    Path(id): Path<String>,
) -> ApiResult<Response> {
    match repo.retract_note(note_id(&id)?, &principal(&who)).await {
        Ok(n) => Ok(Json(n).into_response()),
        Err(Error::Policy(m)) => Ok(auth::reject(
            StatusCode::FORBIDDEN,
            error_code::FORBIDDEN,
            &m,
        )),
        Err(e) => Err(e.into()),
    }
}

/// `POST /v1/notes/{id}/report`. Past the reporter's hourly cap the
/// answer is 429 with `Retry-After`.
pub(crate) async fn report(
    State(repo): S,
    auth::MaybePrincipal(who): auth::MaybePrincipal,
    Path(id): Path<String>,
) -> ApiResult<Response> {
    match repo.report_note(note_id(&id)?, &principal(&who)).await {
        Ok(r) => Ok(Json(r).into_response()),
        Err(Error::Invalid(m)) if m.contains(REPORTS_PER_HOUR_MARKER) => Ok(rate_limited(&m)),
        Err(e) => Err(e.into()),
    }
}

/// A 429 for an hourly cap: the window slides, so a minute on the oldest
/// write in it may have left.
fn rate_limited(m: &str) -> Response {
    let mut resp = auth::reject(StatusCode::TOO_MANY_REQUESTS, error_code::RATE_LIMITED, m);
    resp.headers_mut()
        .insert(header::RETRY_AFTER, HeaderValue::from_static("60"));
    resp
}
