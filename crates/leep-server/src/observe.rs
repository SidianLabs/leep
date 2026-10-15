//! Request logging and HTTP metrics. Both key on the matched route template
//! (`/v1/blobs/{hash}`), never the raw path, so label values stay bounded.

use std::time::{Duration, Instant};

use axum::extract::{MatchedPath, Request};
use axum::http::Method;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use tower_http::trace::TraceLayer;
use tracing::Span;

const UNMATCHED: &str = "unmatched";

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
