//! `/v1/sessions` (ADR 0095): who is working on what, before it lands.
//!
//! The principal of a session is the token's name (`anonymous` without a
//! tokens file), never a body field. Declaring and ending need a write
//! token, listing a read token, like every other route (ADR 0007), and the
//! per-token limits of ADR 0020 apply as to any request. Everything here is
//! advisory: the hooks run after the answer they ride on is decided, in
//! tasks of their own with a timeout, and their failures are counted and
//! logged, never returned. The repository logic is in `leep_derive`
//! (`sessions.rs`).

use std::sync::Arc;
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::{Path, Query, Request, State};
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::Json;
use leep_core::api::{error_code, DeclareSessionRequest, LandResponse, SESSION_HEADER};
use leep_core::coord::SessionState;
use leep_core::{Changeset, ChangesetId, Error};
use leep_derive::sessions::{SessionQuery, ANONYMOUS};
use leep_derive::Repo;
use serde::Deserialize;

use crate::{auth, parse_json, ApiResult, S};

/// How long a hook may take before it is given up (and counted).
const HOOK_TIMEOUT: Duration = Duration::from_secs(10);

/// Hooks in flight at once; past it a hook is skipped and counted, so a
/// slow coordination store cannot pile up tasks behind changeset creates.
const MAX_HOOKS_IN_FLIGHT: usize = 256;

/// How often held heartbeats are written, and how often the sweep and the
/// active gauge run.
pub const FLUSH_EVERY: Duration = Duration::from_secs(5);
pub const SWEEP_EVERY: Duration = Duration::from_secs(60);

/// Most active sessions the `leep_sessions_active` gauge counts.
const GAUGE_CAP: usize = 100_000;

fn principal(who: &Option<auth::Principal>) -> String {
    who.as_ref()
        .map_or_else(|| ANONYMOUS.to_string(), |p| p.name.clone())
}

pub(crate) async fn declare(
    State(repo): S,
    auth::MaybePrincipal(who): auth::MaybePrincipal,
    body: Bytes,
) -> ApiResult<Json<leep_core::api::DeclareSessionResponse>> {
    let req: DeclareSessionRequest = parse_json(&body)?;
    Ok(Json(repo.declare_session(&principal(&who), req).await?))
}

#[derive(Deserialize)]
pub(crate) struct ListQuery {
    path: Option<String>,
    task: Option<String>,
    principal: Option<String>,
    session: Option<String>,
    state: Option<String>,
    all: Option<String>,
    mine: Option<String>,
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

pub(crate) async fn list(
    State(repo): S,
    auth::MaybePrincipal(who): auth::MaybePrincipal,
    Query(q): Query<ListQuery>,
) -> ApiResult<Json<leep_core::api::SessionsResponse>> {
    let mine = flag("mine", q.mine.as_deref())?;
    let limit: Option<usize> = number("limit", q.limit.as_deref())?;
    if limit == Some(0) {
        return Err(Error::Invalid("bad limit 0: expected a number from 1 to 500".into()).into());
    }
    let query = SessionQuery {
        path: q.path.filter(|p| !p.is_empty()),
        task_key: q.task.filter(|t| !t.is_empty()),
        principal: if mine {
            Some(principal(&who))
        } else {
            q.principal.filter(|p| !p.is_empty())
        },
        session: q.session.filter(|s| !s.is_empty()),
        state: q
            .state
            .as_deref()
            .filter(|s| !s.is_empty())
            .map(SessionState::parse)
            .transpose()?,
        all: flag("all", q.all.as_deref())?,
        before: number("before", q.before.as_deref())?,
        limit,
    };
    Ok(Json(repo.sessions(&query).await?))
}

fn session_id(id: &str) -> ApiResult<u64> {
    id.parse()
        .map_err(|_| Error::Invalid(format!("bad session id {id:?}: expected a number")).into())
}

pub(crate) async fn get_one(
    State(repo): S,
    Path(id): Path<String>,
) -> ApiResult<Json<leep_core::api::SessionEntry>> {
    Ok(Json(repo.session(session_id(&id)?).await?))
}

/// End a session as abandoned: only its principal may.
pub(crate) async fn end(
    State(repo): S,
    auth::MaybePrincipal(who): auth::MaybePrincipal,
    Path(id): Path<String>,
) -> ApiResult<Response> {
    let id = session_id(&id)?;
    let me = principal(&who);
    let s = repo.session(id).await?;
    if s.principal != me {
        return Ok(auth::reject(
            StatusCode::FORBIDDEN,
            error_code::FORBIDDEN,
            &format!(
                "session {id} is {}'s; only its principal may end it",
                s.principal
            ),
        ));
    }
    Ok(Json(repo.abandon_session(id, &me).await?).into_response())
}

/// Middleware: a request carrying `Leep-Session` is a heartbeat of the
/// caller's session of that key. Memory only; never refuses anything.
pub(crate) async fn heartbeat(State(repo): State<Arc<Repo>>, req: Request, next: Next) -> Response {
    if let Some(key) = req
        .headers()
        .get(SESSION_HEADER)
        .and_then(|v| v.to_str().ok())
    {
        let who = req
            .extensions()
            .get::<auth::Principal>()
            .map_or(ANONYMOUS, |p| p.name.as_str());
        repo.heartbeat(who, key.trim());
    }
    next.run(req).await
}

static IN_FLIGHT: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(MAX_HOOKS_IN_FLIGHT);

/// Run `hook` in a task of its own, bounded in time and in number. `what`
/// names it in metrics and logs.
fn spawn_hook<F>(what: &'static str, hook: F)
where
    F: std::future::Future<Output = leep_core::Result<bool>> + Send + 'static,
{
    let Ok(permit) = IN_FLIGHT.try_acquire() else {
        metrics::counter!("leep_session_hook_errors_total", "hook" => what, "reason" => "busy")
            .increment(1);
        return;
    };
    tokio::spawn(async move {
        let _permit = permit;
        match tokio::time::timeout(HOOK_TIMEOUT, hook).await {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => {
                metrics::counter!("leep_session_hook_errors_total", "hook" => what, "reason" => "error")
                    .increment(1);
                tracing::warn!(hook = what, error = %e, "session hook failed");
            }
            Err(_) => {
                metrics::counter!("leep_session_hook_errors_total", "hook" => what, "reason" => "timeout")
                    .increment(1);
                tracing::warn!(hook = what, "session hook timed out");
            }
        }
    });
}

/// After `POST /v1/changesets` stored `cs` as `id`: infer the creator's
/// session from it (design D.2).
pub(crate) fn after_create(
    repo: &Repo,
    cs: Changeset,
    id: ChangesetId,
    who: &Option<auth::Principal>,
) {
    if repo.coord().is_none()
        || !cs
            .extra
            .contains_key(leep_core::changeset::EXTRA_AGENT_SESSION)
    {
        return;
    }
    let repo = repo.clone();
    let principal = principal(who);
    spawn_hook("created", async move {
        repo.note_created(&cs, id, &principal).await
    });
}

/// After a land of `submitted` answered `resp`: end the session whose head
/// it was as landed.
pub(crate) fn after_land(
    repo: &Repo,
    submitted: ChangesetId,
    actor: Option<&str>,
    resp: &LandResponse,
) {
    let LandResponse::Landed { landed, .. } = resp else {
        return;
    };
    if repo.coord().is_none() {
        return;
    }
    let (repo, landed) = (repo.clone(), *landed);
    let actor = actor.map(str::to_string);
    spawn_hook("landed", async move {
        repo.note_landed(submitted, landed, actor.as_deref()).await
    });
}

/// Write held heartbeats every [`FLUSH_EVERY`], and sweep expired and
/// long-ended sessions and refresh `leep_sessions_active` every
/// [`SWEEP_EVERY`], until the process ends. Every server runs it; the
/// statements are conditional, so several servers sweeping at once agree.
pub fn spawn_session_tasks(repo: Repo) {
    if repo.coord().is_none() {
        return;
    }
    tokio::spawn(async move {
        let mut flush = tokio::time::interval(FLUSH_EVERY);
        flush.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut last_sweep: Option<tokio::time::Instant> = None;
        loop {
            flush.tick().await;
            if let Err(e) = repo.flush_beats().await {
                metrics::counter!("leep_session_hook_errors_total", "hook" => "beats", "reason" => "error")
                    .increment(1);
                tracing::warn!(error = %e, "writing session heartbeats failed");
            }
            if last_sweep.is_some_and(|t| t.elapsed() < SWEEP_EVERY) {
                continue;
            }
            last_sweep = Some(tokio::time::Instant::now());
            if let Err(e) = repo.sweep_sessions(16).await {
                metrics::counter!("leep_session_hook_errors_total", "hook" => "sweep", "reason" => "error")
                    .increment(1);
                tracing::warn!(error = %e, "sweeping sessions failed");
            }
            match repo.count_active_sessions(GAUGE_CAP).await {
                Ok(n) => metrics::gauge!("leep_sessions_active").set(n as f64),
                Err(e) => tracing::debug!(error = %e, "counting active sessions failed"),
            }
        }
    });
}
