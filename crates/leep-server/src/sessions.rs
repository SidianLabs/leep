//! `/v1/sessions` (ADR 0095): who is working on what, before it lands.
//!
//! The principal of a session is the token's name (`anonymous` without a
//! tokens file), never a body field. Declaring and ending need a write
//! token, listing a read token, like every other route (ADR 0007), and the
//! per-token limits of ADR 0020 apply as to any request. Everything here is
//! advisory: the hooks run after the answer they ride on is decided, in
//! tasks of their own with a timeout, and their failures are counted and
//! logged, never returned. The repository logic is in `leep_derive`
//! (`sessions.rs`, `notices.rs`).
//!
//! Overlap notices (ADR 0096) ride on the answers to session declarations
//! and listings, changeset creates, lands and `GET /v1/notices`, for the
//! caller's session named by `Leep-Session`; `Leep-Notices` picks the
//! lowest level wanted, or `off`. They are computed after the answer is
//! decided and within a bound, past which they are left out.

use std::sync::Arc;
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::{Path, Query, Request, State};
use axum::http::{HeaderMap, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::Json;
use leep_core::api::{
    error_code, DeclareSessionRequest, LandAnswer, LandResponse, Notice, NoticesResponse,
    OverlapsResponse, NOTICES_HEADER, SESSION_HEADER, TARGET_REF_HEADER,
};
use leep_core::coord::SessionState;
use leep_core::{ChangesetId, Error};
use leep_derive::notices::{ask_from_header, NoticeAsk};
use leep_derive::sessions::{SessionQuery, SessionSeed, ANONYMOUS};
use leep_derive::Repo;
use serde::Deserialize;

use crate::{auth, parse_json, ApiResult, S};

/// How long a hook may take before it is given up (and counted).
const HOOK_TIMEOUT: Duration = Duration::from_secs(10);

/// How long a changeset create waits for its session update. Dropping the
/// update past it rolls its transaction back.
const CREATE_HOOK_BOUND: Duration = Duration::from_millis(250);

/// Hooks in flight at once; past it a hook is skipped and counted, so a
/// slow coordination store cannot pile up tasks behind changeset creates.
const MAX_HOOKS_IN_FLIGHT: usize = 256;

/// How often held heartbeats are written, and how often the sweep and the
/// active gauge run.
pub const FLUSH_EVERY: Duration = Duration::from_secs(5);
pub const SWEEP_EVERY: Duration = Duration::from_secs(60);

/// Most active sessions the `leep_sessions_active` gauge counts.
const GAUGE_CAP: usize = 100_000;

pub(crate) fn principal(who: &Option<auth::Principal>) -> String {
    who.as_ref()
        .map_or_else(|| ANONYMOUS.to_string(), |p| p.name.clone())
}

/// The notices a request asks for (`Leep-Notices`, ADR 0096).
fn ask(headers: &HeaderMap) -> Option<NoticeAsk> {
    ask_from_header(headers.get(NOTICES_HEADER).and_then(|v| v.to_str().ok()))
}

/// The caller's agent session key (`Leep-Session`), if any.
pub(crate) fn session_key(headers: &HeaderMap) -> Option<String> {
    headers
        .get(SESSION_HEADER)
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|k| !k.is_empty())
        .map(str::to_string)
}

/// The notices for the caller's session named by `Leep-Session`.
async fn caller_notices(
    repo: &Repo,
    who: &Option<auth::Principal>,
    headers: &HeaderMap,
) -> Vec<Notice> {
    match (session_key(headers), ask(headers)) {
        (Some(key), Some(ask)) if repo.notices_on() => {
            repo.notices_for(&principal(who), &key, &ask).await
        }
        _ => Vec::new(),
    }
}

pub(crate) async fn declare(
    State(repo): S,
    auth::MaybePrincipal(who): auth::MaybePrincipal,
    headers: HeaderMap,
    body: Bytes,
) -> ApiResult<Json<leep_core::api::DeclareSessionResponse>> {
    let req: DeclareSessionRequest = parse_json(&body)?;
    let ask = ask(&headers);
    Ok(Json(
        repo.declare_session_with(&principal(&who), req, ask.as_ref())
            .await?,
    ))
}

/// `GET /v1/notices`: the caller's notices now, each said once.
pub(crate) async fn notices(
    State(repo): S,
    auth::MaybePrincipal(who): auth::MaybePrincipal,
    headers: HeaderMap,
) -> ApiResult<Json<NoticesResponse>> {
    if repo.coord().is_none() {
        return Err(Error::NotFound("sessions: this server keeps none".into()).into());
    }
    Ok(Json(NoticesResponse {
        notices: caller_notices(&repo, &who, &headers).await,
    }))
}

/// `GET /v1/sessions/{id}/overlaps`: everything overlapping it now.
pub(crate) async fn overlaps(
    State(repo): S,
    Path(id): Path<String>,
) -> ApiResult<Json<OverlapsResponse>> {
    Ok(Json(repo.session_overlaps(session_id(&id)?).await?))
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
    headers: HeaderMap,
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
    let mut resp = repo.sessions(&query).await?;
    resp.notices = caller_notices(&repo, &who, &headers).await;
    Ok(Json(resp))
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

/// After `POST /v1/changesets` stored the changeset of `seed` as `id`: infer the creator's
/// session from it (design D.2). Awaited, for at most [`CREATE_HOOK_BOUND`],
/// so the session holds the changeset before its creator can land it: the
/// land's own hook then finds it. The answer is the same either way; a slow
/// or failing store only costs the session this update. Answers the
/// session's notices (ADR 0096), within their own bound, with the
/// changeset's paths probed first.
pub(crate) async fn after_create(
    repo: &Repo,
    mut seed: SessionSeed,
    id: ChangesetId,
    who: &Option<auth::Principal>,
    headers: &HeaderMap,
) -> Vec<Notice> {
    let principal = principal(who);
    // Where the work is meant to land, when the client says: an inferred
    // session needs it to hear of lands (`Leep-Target-Ref`). A bad name is
    // ignored, as a bad `Leep-Session` is.
    seed.target_ref = headers
        .get(TARGET_REF_HEADER)
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|r| leep_core::refname::validate(r).is_ok())
        .map(str::to_string);
    let focus = seed.touch.clone();
    let noted = tokio::time::timeout(
        CREATE_HOOK_BOUND,
        repo.note_created_session(seed, id, &principal),
    )
    .await;
    let session = match noted {
        Ok(Ok(s)) => s,
        Ok(Err(e)) => {
            metrics::counter!("leep_session_hook_errors_total", "hook" => "created", "reason" => "error")
                .increment(1);
            tracing::warn!(error = %e, "inferring a session failed");
            None
        }
        Err(_) => {
            metrics::counter!("leep_session_hook_errors_total", "hook" => "created", "reason" => "timeout")
                .increment(1);
            tracing::warn!("inferring a session timed out");
            None
        }
    };
    match (session, ask(headers)) {
        (Some(s), Some(ask)) => {
            let ask = NoticeAsk { focus, ..ask };
            repo.notices_for_session(&s, &ask).await
        }
        _ => Vec::new(),
    }
}

/// A land's answer with the notices of the land's session beside it (ADR
/// 0096): the caller's `Leep-Session`, else the submitted changeset's
/// `agent.session`. Computed after the answer is decided and within the
/// notice bound; the answer itself is never touched.
pub(crate) async fn land_answer(
    repo: &Repo,
    submitted: ChangesetId,
    actor: Option<&str>,
    headers: &HeaderMap,
    answer: LandResponse,
) -> LandAnswer {
    let Some(ask) = ask(headers).filter(|_| repo.notices_on()) else {
        return LandAnswer {
            answer,
            notices: Vec::new(),
        };
    };
    let principal = actor.unwrap_or(ANONYMOUS).to_string();
    let work = async {
        let key = match session_key(headers) {
            Some(k) => Some(k),
            None => repo.get_changeset(submitted).await.ok().and_then(|cs| {
                cs.extra
                    .get(leep_core::changeset::EXTRA_AGENT_SESSION)
                    .cloned()
            }),
        };
        match key {
            Some(k) => repo.notices_for(&principal, &k, &ask).await,
            None => Vec::new(),
        }
    };
    let notices = tokio::time::timeout(leep_derive::notices::NOTICE_BOUND, work)
        .await
        .unwrap_or_else(|_| {
            metrics::counter!("leep_notices_omitted_total", "reason" => "timeout").increment(1);
            Vec::new()
        });
    LandAnswer { answer, notices }
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
/// long-ended sessions, expired and long-closed notes (ADR 0098), and
/// refresh `leep_sessions_active` every [`SWEEP_EVERY`], until the process
/// ends. Every server runs it; the
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
            if let Err(e) = repo.sweep_notes(16).await {
                metrics::counter!("leep_session_hook_errors_total", "hook" => "notes_sweep", "reason" => "error")
                    .increment(1);
                tracing::warn!(error = %e, "sweeping notes failed");
            }
            match repo.count_active_sessions(GAUGE_CAP).await {
                Ok(n) => metrics::gauge!("leep_sessions_active").set(n as f64),
                Err(e) => tracing::debug!(error = %e, "counting active sessions failed"),
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use async_trait::async_trait;
    use leep_core::coord::*;
    use leep_core::memory::{MemoryBlobStore, MemoryMetaStore};
    use leep_core::{Changeset, FileChange, FileKind, Hash, RepoPath, Result};

    use super::*;

    /// A coordination store that never answers.
    struct Hanging;

    macro_rules! hang {
        ($($name:ident($($arg:ty),*) -> $ret:ty;)*) => {
            #[async_trait]
            impl CoordStore for Hanging {
                $(async fn $name(&self, $(_: $arg),*) -> Result<$ret> {
                    std::future::pending().await
                })*
            }
        };
    }
    hang! {
        upsert_session(&SessionUpdate) -> (Session, bool);
        get_session(u64) -> Option<Session>;
        list_sessions(&SessionFilter) -> Vec<Session>;
        end_session(u64, &SessionEnd) -> bool;
        beat_sessions(&[Beat]) -> u64;
        sweep_sessions(i64, usize) -> SweepReport;
        count_sessions(SessionState, usize) -> u64;
        near_paths(&NearQuery) -> Vec<NearRow>;
        add_note(&NewNote) -> Note;
        get_note(u64) -> Option<Note>;
        list_notes(&NoteFilter) -> Vec<Note>;
        notes_at(&[String], usize) -> Vec<Note>;
        change_note(u64, &NoteChange, i64) -> bool;
        revoke_notes(&str, i64) -> u64;
        sweep_notes(i64, usize) -> NoteSweep;
    }

    /// A create waits for its session update exactly [`CREATE_HOOK_BOUND`]
    /// when the store hangs, and answers no notices: on a paused clock, so
    /// no machine's speed decides it.
    #[tokio::test(start_paused = true)]
    async fn a_hanging_store_costs_a_create_exactly_its_bound() {
        let repo = Repo::new(
            Arc::new(MemoryBlobStore::new()),
            Arc::new(MemoryMetaStore::new()),
        )
        .with_coord(Some(Arc::new(Hanging)));
        let mut changes = BTreeMap::new();
        changes.insert(
            RepoPath::new("src/a.rs").unwrap(),
            FileChange::Modify {
                content: Hash::of_blob(b"a"),
                kind: FileKind::Regular,
                size: 1,
            },
        );
        let cs = Changeset {
            parents: vec![],
            author: "ada".into(),
            timestamp: 1,
            message: "m".into(),
            changes,
            extra: [(
                leep_core::changeset::EXTRA_AGENT_SESSION.to_string(),
                "s-1".to_string(),
            )]
            .into(),
        };
        let seed = SessionSeed::of(&cs).unwrap();
        let started = tokio::time::Instant::now();
        let notices = after_create(&repo, seed, cs.id(), &None, &HeaderMap::new()).await;
        assert_eq!(started.elapsed(), CREATE_HOOK_BOUND);
        assert!(notices.is_empty());
    }
}
