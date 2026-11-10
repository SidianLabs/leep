//! `GET /v1/events` (ADR 0099): coordination events after a cursor, as one
//! long-polled answer or, with `Accept: text/event-stream`, as a stream.
//!
//! A read token may read, as for sessions. A request that waits (a
//! long-poll with `wait` above 0, or a stream) takes a place in the
//! server's waiter class instead of its token's ADR 0020 concurrency
//! permit (see [`is_waiter`]), so a long-poll never starves the token's
//! other requests; past [`leep_derive::events::MAX_WAITERS`] per server or
//! [`leep_derive::events::MAX_WAITERS_PER_PRINCIPAL`] per principal it is
//! refused with 503 `Leep-Refused: events`. `session=KEY` names a session
//! of the caller's own principal, and nobody else's. The logic is in
//! `leep_derive::events`.

use std::convert::Infallible;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::extract::{Query, Request, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::sse::{Event as SseEvent, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::Json;
use leep_core::api::{
    error_code, event_cursor, parse_event_cursor, CursorExpiredResponse, ErrorResponse,
    EventCursor, EventsResponse, REFUSED_EVENTS,
};
use leep_core::coord::{normalize_path, EventKind};
use leep_core::Error;
use leep_derive::events::{
    entry, task_digest, EventFilter, EventsAnswer, SessionScope, Waiter, MAX_FILTER_PATHS, MAX_WAIT,
};
use leep_derive::sessions::SessionQuery;
use leep_derive::Repo;

use crate::sessions::principal;
use crate::{auth, ApiResult, S};

/// The route.
pub const PATH: &str = "/v1/events";
/// A comment on an idle stream this often, so proxies and the server's
/// own body idle timeout see it alive.
pub const HEARTBEAT: Duration = Duration::from_secs(15);
/// A stream is closed after this long; the client reconnects, which
/// checks its token again.
pub const STREAM_FOR: Duration = Duration::from_secs(10 * 60);
/// Events a stream holds for a reader that has not taken them; past it the
/// reader gets `event: reset` and the stream ends.
pub const STREAM_BUFFER: usize = 64;

fn bad(msg: impl Into<String>) -> crate::ApiError {
    Error::Invalid(msg.into()).into()
}

/// Whether the request wants a stream.
fn wants_stream(headers: &HeaderMap) -> bool {
    headers
        .get(header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|a| a.contains("text/event-stream"))
}

/// Whether `req` is a waiting `GET /v1/events`: a stream, or a long-poll
/// with a `wait` above 0, decided by the one parse the handler makes too
/// ([`Ask::waits`]). It is admitted by the events waiter class, which the
/// handler takes before any other work, not by the token's concurrency
/// permit. A request that does not parse is not one: it takes a permit
/// and is answered 400.
pub fn is_waiter(req: &Request) -> bool {
    if req.method() != axum::http::Method::GET || req.uri().path() != PATH {
        return false;
    }
    let Ok(Query(q)) = Query::<Vec<(String, String)>>::try_from_uri(req.uri()) else {
        return false;
    };
    parse(&q, req.headers()).is_ok_and(|a| a.waits())
}

/// What one request asks.
struct Ask {
    after: Option<EventCursor>,
    session: Option<String>,
    filter: EventFilter,
    wait: Duration,
    stream: bool,
}

impl Ask {
    /// Whether the request waits: then it takes a waiter place, and no
    /// concurrency permit.
    fn waits(&self) -> bool {
        self.stream || !self.wait.is_zero()
    }
}

fn parse(q: &[(String, String)], headers: &HeaderMap) -> ApiResult<Ask> {
    let mut ask = Ask {
        after: None,
        session: None,
        filter: EventFilter::default(),
        wait: Duration::ZERO,
        stream: wants_stream(headers),
    };
    let cursor = |v: &str| {
        parse_event_cursor(v).ok_or_else(|| bad("bad after: expected a cursor from an answer"))
    };
    for (k, v) in q {
        match k.as_str() {
            "after" if !v.is_empty() => ask.after = Some(cursor(v)?),
            "session" if !v.trim().is_empty() => ask.session = Some(v.trim().to_string()),
            "paths" | "path" => {
                for p in v.split(',').map(normalize_path).filter(|p| !p.is_empty()) {
                    leep_core::RepoPath::new(p.clone())?;
                    ask.filter.paths.push(p);
                }
            }
            "kinds" | "kind" => {
                for kind in v.split(',').map(str::trim).filter(|k| !k.is_empty()) {
                    ask.filter
                        .kinds
                        .push(EventKind::parse(kind)?.as_str().to_string());
                }
            }
            "wait" => {
                let w: f64 = v
                    .parse()
                    .ok()
                    .filter(|w: &f64| w.is_finite() && *w >= 0.0)
                    .ok_or_else(|| bad(format!("bad wait {v:?}: expected seconds, 0 to 30")))?;
                ask.wait = Duration::from_secs_f64(w.min(MAX_WAIT.as_secs_f64()));
            }
            _ => {}
        }
    }
    if ask.filter.paths.len() > MAX_FILTER_PATHS {
        return Err(bad(format!(
            "too many paths: at most {MAX_FILTER_PATHS} per request"
        )));
    }
    // A stream resumes where the reader's last event was.
    if let Some(id) = headers
        .get("last-event-id")
        .and_then(|v| v.to_str().ok())
        .filter(|v| !v.trim().is_empty())
    {
        ask.after = Some(cursor(id)?);
    }
    Ok(ask)
}

/// `session=KEY`: the caller's own session of that key, the newest in any
/// state; another principal's sessions are never looked at.
async fn scope(repo: &Repo, me: &str, key: Option<&str>) -> ApiResult<SessionScope> {
    let Some(key) = key else {
        return Ok(SessionScope::Any);
    };
    let q = SessionQuery {
        principal: Some(me.to_string()),
        session: Some(key.to_string()),
        all: true,
        limit: Some(1),
        ..Default::default()
    };
    let Some(s) = repo.sessions(&q).await?.sessions.into_iter().next() else {
        return Ok(SessionScope::Nothing);
    };
    Ok(SessionScope::Of {
        id: s.id,
        paths: s
            .scope
            .into_iter()
            .chain(s.claims)
            .chain(s.touched)
            .collect(),
        task: s.task_key.as_deref().map(task_digest),
    })
}

fn refused(why: leep_derive::events::Refused) -> crate::ApiError {
    let message = match why {
        leep_derive::events::Refused::Server => {
            "this server has as many event readers waiting as it allows; try again".to_string()
        }
        leep_derive::events::Refused::Principal => {
            "this principal has as many event readers waiting on this server as it allows; \
             close some, or try again"
                .to_string()
        }
    };
    Error::Refused {
        refused: REFUSED_EVENTS,
        retry_after_ms: 1000,
        message,
    }
    .into()
}

fn expired(oldest: u64, epoch: &str) -> Response {
    (
        StatusCode::GONE,
        Json(CursorExpiredResponse {
            error: ErrorResponse::new(
                "this cursor is older than the events kept, or from another store; resync \
                 (GET /v1/notices, /v1/sessions/{id}/overlaps) and read on from `oldest`",
            )
            .with_code(error_code::CURSOR_EXPIRED),
            oldest: event_cursor(epoch, oldest),
        }),
    )
        .into_response()
}

/// `GET /v1/events`. A waiting request takes its waiter place first, before
/// any store read, so a request refused costs the store nothing.
pub(crate) async fn get(
    State(repo): S,
    auth::MaybePrincipal(who): auth::MaybePrincipal,
    headers: HeaderMap,
    Query(q): Query<Vec<(String, String)>>,
) -> ApiResult<Response> {
    let ask = parse(&q, &headers)?;
    let me = principal(&who);
    let mut waiter = if ask.waits() {
        Some(repo.event_waiter(&me).map_err(refused)?)
    } else {
        None
    };
    let start = match ask.after.clone() {
        Some(a) => a,
        None => {
            let tail = repo.events_tail().await?;
            if !ask.stream {
                return Ok(Json(EventsResponse {
                    events: Vec::new(),
                    next: event_cursor(tail.epoch.as_deref().unwrap_or(""), tail.seq),
                    more: false,
                })
                .into_response());
            }
            tail
        }
    };
    let mut filter = ask.filter;
    filter.session = scope(&repo, &me, ask.session.as_deref()).await?;
    if ask.stream {
        if let Some(w) = waiter.take() {
            let key = ask.session.map(|k| (me, k));
            return Ok(sse(repo, w, start, filter, key).into_response());
        }
    }
    match repo
        .events_wait(&start, &filter, ask.wait, waiter.as_mut())
        .await?
    {
        EventsAnswer::Page(p) => Ok(Json(EventsResponse {
            events: p.events.iter().map(|e| entry(e, &p.epoch)).collect(),
            next: event_cursor(&p.epoch, p.next),
            more: p.more,
        })
        .into_response()),
        EventsAnswer::Expired { oldest, epoch } => Ok(expired(oldest, &epoch)),
    }
}

/// The stream: a task reads pages as a long-poll would and hands each
/// event to a channel of [`STREAM_BUFFER`]; the response body drains it. A
/// reader that lets the channel fill gets `event: reset` after what it
/// holds, and the stream ends: nothing is buffered without bound. The
/// last event (a reset) is kept aside, so a full channel never drops it.
/// `key`, the caller's principal and session key, is resolved again every
/// [`HEARTBEAT`], so the session's new paths count.
fn sse(
    repo: Arc<Repo>,
    mut waiter: Waiter,
    start: EventCursor,
    mut filter: EventFilter,
    key: Option<(String, String)>,
) -> Sse<impl futures_util::Stream<Item = Result<SseEvent, Infallible>>> {
    let (tx, rx) = tokio::sync::mpsc::channel::<SseEvent>(STREAM_BUFFER);
    let last: Arc<Mutex<Option<SseEvent>>> = Arc::default();
    let finish = last.clone();
    tokio::spawn(async move {
        let close_at = tokio::time::Instant::now() + STREAM_FOR;
        let mut resolved = tokio::time::Instant::now();
        let mut after = start;
        let reset = |data: String| {
            *finish.lock().unwrap() = Some(SseEvent::default().event("reset").data(data));
        };
        loop {
            let left = close_at.saturating_duration_since(tokio::time::Instant::now());
            if left.is_zero() || waiter.stopping() {
                return;
            }
            if let Some((me, k)) = &key {
                if resolved.elapsed() >= HEARTBEAT {
                    resolved = tokio::time::Instant::now();
                    if let Ok(s) = scope(&repo, me, Some(k)).await {
                        filter.session = s;
                    }
                }
            }
            let wait = left.min(MAX_WAIT).min(HEARTBEAT);
            let read = tokio::select! {
                r = repo.events_wait(&after, &filter, wait, Some(&mut waiter)) => r,
                _ = tx.closed() => return,
            };
            let page = match read {
                Ok(EventsAnswer::Page(p)) => p,
                Ok(EventsAnswer::Expired { oldest, epoch }) => {
                    let data = serde_json::json!({
                        "reason": error_code::CURSOR_EXPIRED,
                        "oldest": event_cursor(&epoch, oldest),
                    });
                    reset(data.to_string());
                    return;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "reading events for a stream failed");
                    return;
                }
            };
            let lagged = || {
                metrics::counter!("leep_events_refused_total", "reason" => "lagged").increment(1);
                reset(r#"{"reason":"lagged"}"#.to_string());
            };
            for e in &page.events {
                let data = serde_json::to_string(&entry(e, &page.epoch)).unwrap_or_default();
                let ev = SseEvent::default()
                    .id(event_cursor(&page.epoch, e.seq))
                    .event(e.kind.clone())
                    .data(data);
                if let Err(err) = tx.try_send(ev) {
                    if matches!(err, tokio::sync::mpsc::error::TrySendError::Full(_)) {
                        lagged();
                    }
                    return;
                }
            }
            if page.next > after.seq && page.events.last().is_none_or(|e| e.seq < page.next) {
                // Nothing to show past here: move the reader's
                // Last-Event-ID on without an event (an `id:` alone is not
                // dispatched), so a reconnect does not read it again.
                let ev = SseEvent::default().id(event_cursor(&page.epoch, page.next));
                if tx.try_send(ev).is_err() {
                    lagged();
                    return;
                }
            }
            after = EventCursor {
                epoch: Some(page.epoch),
                seq: page.next,
            };
        }
    });
    let stream =
        futures_util::stream::unfold((rx, last, false), |(mut rx, last, done)| async move {
            if done {
                return None;
            }
            match rx.recv().await {
                Some(ev) => Some((Ok(ev), (rx, last, false))),
                None => {
                    let end = last.lock().unwrap().take();
                    end.map(|ev| (Ok(ev), (rx, last, true)))
                }
            }
        });
    Sse::new(stream).keep_alive(KeepAlive::new().interval(HEARTBEAT))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn waiters_are_streams_and_waiting_long_polls() {
        let req = |uri: &str, accept: Option<&str>| {
            let mut b = Request::builder().method("GET").uri(uri);
            if let Some(a) = accept {
                b = b.header(header::ACCEPT, a);
            }
            b.body(axum::body::Body::empty()).unwrap()
        };
        assert!(is_waiter(&req("/v1/events?after=e1&wait=5", None)));
        // One parse decides, as the handler's: the last `wait` wins, and a
        // request that does not parse takes a permit.
        assert!(!is_waiter(&req("/v1/events?after=e1&wait=1&wait=0", None)));
        assert!(is_waiter(&req("/v1/events?after=e1&wait=0&wait=1", None)));
        assert!(is_waiter(&req("/v1/events?wait=5", None)));
        assert!(!is_waiter(&req("/v1/events?wait=5&after=bogus", None)));
        assert!(is_waiter(&req("/v1/events?wait=0.5", None)));
        assert!(!is_waiter(&req("/v1/events?after=e1&wait=0", None)));
        assert!(!is_waiter(&req("/v1/events?after=e1", None)));
        assert!(is_waiter(&req("/v1/events", Some("text/event-stream"))));
        assert!(!is_waiter(&req("/v1/sessions?wait=5", None)));
    }

    #[test]
    fn asks_parse_and_bad_ones_are_refused() {
        let q = |pairs: &[(&str, &str)]| -> Vec<(String, String)> {
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect()
        };
        let h = HeaderMap::new();
        let a = parse(
            &q(&[
                ("after", "e12"),
                ("paths", "src/a,./docs/"),
                ("paths", "web"),
                ("kinds", "session.ended,note.added"),
                ("wait", "99"),
                ("session", " s-1 "),
            ]),
            &h,
        )
        .unwrap();
        assert_eq!(a.after.map(|c| c.seq), Some(12));
        assert_eq!(a.filter.paths, ["src/a", "docs", "web"]);
        assert_eq!(a.filter.kinds, ["session.ended", "note.added"]);
        assert_eq!(a.wait, MAX_WAIT);
        assert_eq!(a.session.as_deref(), Some("s-1"));
        for bad in [
            &[("after", "12")][..],
            &[("after", "e-1")],
            &[("wait", "-1")],
            &[("wait", "soon")],
            &[("kinds", "session.everything")],
            &[("paths", "/abs")],
        ] {
            assert!(parse(&q(bad), &h).is_err(), "{bad:?}");
        }
        let many: Vec<(String, String)> = (0..=MAX_FILTER_PATHS)
            .map(|i| ("paths".to_string(), format!("d{i}")))
            .collect();
        assert!(parse(&many, &h).is_err());
        let mut h = HeaderMap::new();
        h.insert("last-event-id", "e40".parse().unwrap());
        assert_eq!(
            parse(&q(&[("after", "e3")]), &h)
                .unwrap()
                .after
                .map(|c| c.seq),
            Some(40)
        );
    }
}
