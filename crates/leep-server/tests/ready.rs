//! `GET /v1/ready`: both stores answer, or the server says which one did not.

use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::body::Body;
use axum::http::{header, Method, Request, StatusCode};
use axum::Router;
use http_body_util::BodyExt;
use leep_core::api::ReadyResponse;
use leep_core::memory::{MemoryBlobStore, MemoryMetaStore};
use leep_derive::Repo;
use leep_server::auth::{hash_token, Auth};
use leep_server::limits::{Defaults, Limiter, ManualClock};
use leep_server::reload::{ConfigHandle, RuntimeConfig};
use leep_testkit::{Fault, FaultPlan, FaultyBlobStore, FaultyMetaStore, Op};
use metrics_exporter_prometheus::PrometheusBuilder;
use tower::ServiceExt;

const TOKEN: &str = "token-for-ready-tests";

struct Rig {
    app: Router,
    meta_plan: Arc<FaultPlan>,
    blob_plan: Arc<FaultPlan>,
}

fn rig_with(timeout: Duration, auth: Option<Auth>, limiter: Option<Arc<Limiter>>) -> Rig {
    let meta_plan = FaultPlan::new(1);
    let blob_plan = FaultPlan::new(2);
    let repo = Repo::new(
        Arc::new(FaultyBlobStore::new(
            Arc::new(MemoryBlobStore::new()),
            blob_plan.clone(),
        )),
        Arc::new(FaultyMetaStore::new(
            Arc::new(MemoryMetaStore::new()),
            meta_plan.clone(),
        )),
    );
    let config = ConfigHandle::fixed(RuntimeConfig::fixed(auth, None, None, limiter, None));
    let app = leep_server::router_with_ready_timeout(
        repo,
        1 << 20,
        Duration::from_secs(60),
        config,
        timeout,
    );
    Rig {
        app,
        meta_plan,
        blob_plan,
    }
}

fn rig() -> Rig {
    rig_with(Duration::from_secs(2), None, None)
}

fn tokens() -> Auth {
    Auth::parse(
        &serde_json::json!({ "tokens": [
            { "name": "a", "role": "write", "hash": hash_token(TOKEN) }
        ] })
        .to_string(),
    )
    .unwrap()
}

async fn ready(app: &Router) -> (StatusCode, Vec<u8>) {
    let req = Request::builder()
        .method(Method::GET)
        .uri("/v1/ready")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    (
        status,
        resp.into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
}

fn parse(body: &[u8]) -> ReadyResponse {
    serde_json::from_slice(body)
        .unwrap_or_else(|e| panic!("{e}: {}", String::from_utf8_lossy(body)))
}

#[tokio::test]
async fn ready_with_working_stores() {
    let r = rig();
    let (status, body) = ready(&r.app).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        String::from_utf8(body).unwrap(),
        r#"{"status":"ready","features":["land-id","indexing-retry","changeset-rescue","land-clean-blobs"]}"#
    );
    // It really asked both stores, once each.
    assert_eq!(r.meta_plan.calls(Op::GetRef), 1);
    assert_eq!(r.blob_plan.calls(Op::Contains), 1);
}

#[tokio::test]
async fn a_failing_meta_store_is_503_naming_meta() {
    let r = rig();
    r.meta_plan.rate(&[Op::GetRef], 1000, Fault::Fail);
    let (status, body) = ready(&r.app).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    let got = parse(&body);
    assert_eq!(got.status, "not ready");
    assert_eq!(got.failed.len(), 1, "{got:?}");
    assert_eq!(got.failed[0].store, "meta");
    assert_eq!(got.failed[0].reason, "unavailable");
}

#[tokio::test]
async fn a_failing_blob_store_is_503_naming_blobs() {
    let r = rig();
    r.blob_plan.rate(&[Op::Contains], 1000, Fault::Fail);
    let (status, body) = ready(&r.app).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    let got = parse(&body);
    assert_eq!(got.failed.len(), 1, "{got:?}");
    assert_eq!(got.failed[0].store, "blobs");
}

#[tokio::test]
async fn both_failing_names_both_and_leaks_no_detail() {
    let r = rig();
    r.meta_plan.rate(&[Op::GetRef], 1000, Fault::Fail);
    r.blob_plan.rate(&[Op::Contains], 1000, Fault::Fail);
    let (status, body) = ready(&r.app).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    let got = parse(&body);
    let mut names: Vec<_> = got.failed.iter().map(|f| f.store.as_str()).collect();
    names.sort();
    assert_eq!(names, ["blobs", "meta"]);
    // The injected error's text stays in the log, not the response.
    assert!(
        got.failed.iter().all(|f| f.reason == "unavailable"),
        "{got:?}"
    );
}

#[tokio::test]
async fn a_hung_store_times_out_and_the_other_store_is_still_checked() {
    let r = rig_with(Duration::from_millis(150), None, None);
    r.meta_plan
        .rate(&[Op::GetRef], 1000, Fault::Delay(Duration::from_secs(30)));
    let started = Instant::now();
    let (status, body) = ready(&r.app).await;
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "{:?}",
        started.elapsed()
    );
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    let got = parse(&body);
    assert_eq!(got.failed.len(), 1, "{got:?}");
    assert_eq!(got.failed[0].store, "meta");
    assert_eq!(got.failed[0].reason, "timed out after 150ms");
    assert_eq!(r.blob_plan.calls(Op::Contains), 1);
}

#[tokio::test]
async fn both_stores_hanging_cost_one_timeout_not_two() {
    let r = rig_with(Duration::from_millis(300), None, None);
    r.meta_plan
        .rate(&[Op::GetRef], 1000, Fault::Delay(Duration::from_secs(30)));
    r.blob_plan
        .rate(&[Op::Contains], 1000, Fault::Delay(Duration::from_secs(30)));
    let started = Instant::now();
    let (status, body) = ready(&r.app).await;
    let took = started.elapsed();
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(parse(&body).failed.len(), 2);
    assert!(
        took < Duration::from_millis(550),
        "checks ran in series: {took:?}"
    );
}

#[tokio::test]
async fn it_recovers_when_the_store_does() {
    let r = rig();
    r.meta_plan.rate(&[Op::GetRef], 1000, Fault::Fail);
    assert_eq!(ready(&r.app).await.0, StatusCode::SERVICE_UNAVAILABLE);
    r.meta_plan.clear();
    assert_eq!(ready(&r.app).await.0, StatusCode::OK);
}

#[tokio::test]
async fn no_token_is_needed_even_when_the_stores_are_down() {
    let r = rig_with(Duration::from_secs(2), Some(tokens()), None);
    assert_eq!(ready(&r.app).await.0, StatusCode::OK);
    r.meta_plan.rate(&[Op::GetRef], 1000, Fault::Fail);
    // 503 from the check itself, not 401 from authentication.
    assert_eq!(ready(&r.app).await.0, StatusCode::SERVICE_UNAVAILABLE);
    // Other endpoints still need one.
    let req = Request::builder()
        .uri("/v1/refs")
        .body(Body::empty())
        .unwrap();
    assert_eq!(
        r.app.clone().oneshot(req).await.unwrap().status(),
        StatusCode::UNAUTHORIZED
    );
}

#[tokio::test]
async fn only_get_is_a_probe() {
    let r = rig_with(Duration::from_secs(2), Some(tokens()), None);
    let req = Request::builder()
        .method(Method::POST)
        .uri("/v1/ready")
        .body(Body::empty())
        .unwrap();
    // Not a probe, so authentication runs first.
    assert_eq!(
        r.app.clone().oneshot(req).await.unwrap().status(),
        StatusCode::UNAUTHORIZED
    );
}

#[tokio::test]
async fn ready_is_never_limited_and_spends_no_token_budget() {
    let auth = tokens();
    let defaults = Defaults {
        rate: Some(1.0),
        burst: Some(1),
        concurrency: Some(1),
        ..Default::default()
    };
    let limiter = Limiter::with_clock(Some(&auth), &defaults, Arc::new(ManualClock::new()))
        .unwrap()
        .map(Arc::new);
    let r = rig_with(Duration::from_secs(2), Some(auth), limiter);
    for _ in 0..10 {
        assert_eq!(ready(&r.app).await.0, StatusCode::OK);
    }
    // The token's single-request budget is untouched.
    let req = Request::builder()
        .uri("/v1/refs")
        .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
        .body(Body::empty())
        .unwrap();
    assert_eq!(
        r.app.clone().oneshot(req).await.unwrap().status(),
        StatusCode::OK
    );
}

#[tokio::test]
async fn health_does_not_look_at_the_stores() {
    let r = rig();
    r.meta_plan.rate(&[Op::GetRef], 1000, Fault::Fail);
    r.blob_plan.rate(&[Op::Contains], 1000, Fault::Fail);
    let req = Request::builder()
        .uri("/v1/health")
        .body(Body::empty())
        .unwrap();
    let resp = r.app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(r.meta_plan.calls(Op::GetRef), 0);
    assert_eq!(r.blob_plan.calls(Op::Contains), 0);
}

fn with_metrics<F: Future<Output = ()>>(fut: F) -> String {
    let recorder = PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    metrics::with_local_recorder(&recorder, || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(fut)
    });
    handle.render()
}

#[test]
fn checks_are_counted_by_store_and_result() {
    let text = with_metrics(async {
        let r = rig_with(Duration::from_millis(100), None, None);
        ready(&r.app).await;
        r.meta_plan.rate(&[Op::GetRef], 1000, Fault::Fail);
        ready(&r.app).await;
        r.meta_plan.clear();
        r.blob_plan
            .rate(&[Op::Contains], 1000, Fault::Delay(Duration::from_secs(30)));
        ready(&r.app).await;
    });
    let n = |store: &str, result: &str| -> f64 {
        let want = format!(r#"store="{store}""#);
        let res = format!(r#"result="{result}""#);
        text.lines()
            .find(|l| {
                l.starts_with("leep_ready_checks_total") && l.contains(&want) && l.contains(&res)
            })
            .unwrap_or_else(|| panic!("no series {store} {result} in:\n{text}"))
            .rsplit(' ')
            .next()
            .unwrap()
            .parse()
            .unwrap()
    };
    assert_eq!(n("meta", "ok"), 2.0);
    assert_eq!(n("meta", "error"), 1.0);
    assert_eq!(n("meta", "timeout"), 0.0);
    assert_eq!(n("blobs", "ok"), 2.0);
    assert_eq!(n("blobs", "timeout"), 1.0);
    assert_eq!(n("blobs", "error"), 0.0);
}
