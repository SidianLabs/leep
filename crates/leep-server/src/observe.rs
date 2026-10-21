//! Request logging and HTTP metrics. Both key on the matched route template
//! (`/v1/blobs/{hash}`), never the raw path, so label values stay bounded.

use std::time::{Duration, Instant};

use axum::body::Body;
use axum::extract::{MatchedPath, Request};
use axum::http::{header, HeaderValue, Method};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use futures_util::StreamExt;
use http_body_util::BodyExt;
use leep_core::api::{valid_request_id, REQUEST_ID_HEADER};
use tower_http::trace::TraceLayer;
use tracing::Span;

const UNMATCHED: &str = "unmatched";

/// Largest error body [`request_id`] adds the id to; error bodies are a few
/// hundred bytes, so a larger one is left as it is.
const MAX_ERROR_BODY: u64 = 64 * 1024;

/// The id of the request being served ([`REQUEST_ID_HEADER`]), set by
/// [`request_id`] for the log span and handlers.
#[derive(Clone, Debug)]
pub struct RequestId(pub String);

fn new_request_id() -> String {
    let mut b = [0u8; 16];
    getrandom::fill(&mut b).expect("the OS random source is available");
    hex::encode(b)
}

/// Axum middleware naming every request: the client's `X-Request-Id` when
/// [`valid_request_id`] accepts it, else a new random one. The id is put on
/// the request (header and [`RequestId`]), echoed in the response header, and
/// added as `request_id` to JSON error bodies. Layer it outermost.
pub async fn request_id(mut req: Request, next: Next) -> Response {
    let id = req
        .headers()
        .get(REQUEST_ID_HEADER)
        .and_then(|v| v.to_str().ok())
        .filter(|v| valid_request_id(v))
        .map_or_else(new_request_id, str::to_string);
    // Printable ASCII, so always a valid header value.
    let value = HeaderValue::from_str(&id).expect("request ids are header-safe ASCII");
    req.headers_mut().insert(REQUEST_ID_HEADER, value.clone());
    req.extensions_mut().insert(RequestId(id.clone()));
    let resp = next.run(req).await;
    let mut resp = with_request_id(resp, &id).await;
    resp.headers_mut().insert(REQUEST_ID_HEADER, value);
    resp
}

/// `resp` with `request_id` in its body when it is a JSON error object
/// without one (a body relayed from a peer already has it). A body over
/// [`MAX_ERROR_BODY`] is passed on unread past that point. A rewritten body
/// is re-serialized, so its keys come out in alphabetical order; JSON key
/// order carries no meaning, and no test should compare error bytes.
async fn with_request_id(resp: Response, id: &str) -> Response {
    let status = resp.status();
    if !(status.is_client_error() || status.is_server_error()) || !crate::errors::is_json(&resp) {
        return resp;
    }
    let (mut parts, mut body) = resp.into_parts();
    let mut buf = Vec::new();
    while let Some(frame) = body.frame().await {
        let Ok(frame) = frame else {
            parts.headers.remove(header::CONTENT_LENGTH);
            return Response::from_parts(parts, Body::empty());
        };
        let Ok(data) = frame.into_data() else {
            continue;
        };
        buf.extend_from_slice(&data);
        if buf.len() as u64 > MAX_ERROR_BODY {
            let head = futures_util::stream::once(async move {
                Ok::<_, axum::Error>(axum::body::Bytes::from(buf))
            });
            let rest = head.chain(body.into_data_stream());
            return Response::from_parts(parts, Body::from_stream(rest));
        }
    }
    let mut v: serde_json::Value = match serde_json::from_slice(&buf) {
        Ok(v) => v,
        Err(_) => return Response::from_parts(parts, Body::from(buf)),
    };
    match v.as_object_mut() {
        Some(o) if o.contains_key("error") && !o.contains_key("request_id") => {
            o.insert("request_id".into(), id.into());
        }
        _ => return Response::from_parts(parts, Body::from(buf)),
    }
    parts.headers.remove(header::CONTENT_LENGTH);
    let out = serde_json::to_vec(&v).unwrap_or(buf);
    Response::from_parts(parts, Body::from(out))
}

fn request_id_of(req: &Request) -> &str {
    req.extensions()
        .get::<RequestId>()
        .map_or("-", |r| r.0.as_str())
}

fn route_of(req: &Request) -> String {
    req.extensions()
        .get::<MatchedPath>()
        .map_or_else(|| UNMATCHED.to_string(), |m| m.as_str().to_string())
}

/// Clients choose the method string, so anything unknown shares one label.
fn method_label(m: &Method) -> &'static str {
    match *m {
        Method::GET => "GET",
        Method::POST => "POST",
        Method::PUT => "PUT",
        Method::DELETE => "DELETE",
        Method::HEAD => "HEAD",
        Method::OPTIONS => "OPTIONS",
        Method::PATCH => "PATCH",
        _ => "other",
    }
}

/// Count and time every request. Latency is measured until the response head
/// is ready, not until the body has been streamed.
pub async fn track_metrics(req: Request, next: Next) -> Response {
    let started = Instant::now();
    let method = method_label(req.method());
    let route = route_of(&req);
    let resp = next.run(req).await;
    let status = resp.status().as_u16().to_string();
    metrics::counter!("leep_http_requests_total", "method" => method, "route" => route.clone(), "status" => status)
        .increment(1);
    metrics::histogram!("leep_http_request_duration_seconds", "method" => method, "route" => route)
        .record(started.elapsed().as_secs_f64());
    resp
}

/// One INFO line per request. Headers are never logged, so bearer tokens
/// cannot leak through here.
#[allow(clippy::type_complexity)]
pub fn trace_layer() -> TraceLayer<
    tower_http::classify::SharedClassifier<tower_http::classify::ServerErrorsAsFailures>,
    impl Fn(&Request) -> Span + Clone,
    (),
    impl Fn(&Response, Duration, &Span) + Clone,
    (),
    (),
    (),
> {
    TraceLayer::new_for_http()
        .make_span_with(|req: &Request| {
            tracing::info_span!(
                "request",
                method = %req.method(),
                route = %route_of(req),
                // Debug quotes it: a log line shows where the id ends.
                request_id = ?request_id_of(req),
                status = tracing::field::Empty,
                latency_ms = tracing::field::Empty,
            )
        })
        .on_request(())
        .on_body_chunk(())
        .on_eos(())
        .on_failure(())
        .on_response(|resp: &Response, latency: Duration, span: &Span| {
            let status = resp.status().as_u16();
            let latency_ms = latency.as_secs_f64() * 1000.0;
            span.record("status", status);
            span.record("latency_ms", latency_ms);
            tracing::info!(status, latency_ms, "request completed");
        })
}

/// Set `leep_build_info{version}` to 1.
pub fn record_build_info() {
    metrics::gauge!("leep_build_info", "version" => env!("CARGO_PKG_VERSION")).set(1.0);
}

/// App for the separate metrics listener: `GET /metrics` serves whatever
/// `render` returns in Prometheus text format. No auth, so bind it privately.
pub fn metrics_app(render: impl Fn() -> String + Clone + Send + Sync + 'static) -> Router {
    Router::new().route(
        "/metrics",
        get(move || {
            let body = render();
            async move {
                (
                    [("content-type", "text/plain; version=0.0.4; charset=utf-8")],
                    body,
                )
                    .into_response()
            }
        }),
    )
}
