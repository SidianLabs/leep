//! Metrics and the metrics listener. Each test installs its own recorder as
//! thread-local, so parallel tests never see each other's counters.

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use axum::Router;
use http_body_util::BodyExt;
use leep_core::memory::{MemoryBlobStore, MemoryMetaStore};
use leep_core::{Changeset, FileChange, FileKind, Hash, RepoPath};
use leep_derive::Repo;
use leep_store::CachedBlobStore;
use metrics_exporter_prometheus::PrometheusBuilder;
use tower::ServiceExt;

/// Run `fut` on a current-thread runtime with a fresh recorder active, and
/// return the Prometheus text it produced.
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

async fn send(app: &Router, method: Method, uri: &str, body: Vec<u8>) -> StatusCode {
    let req = Request::builder()
        .method(method)
        .uri(uri)
        .body(Body::from(body))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let _ = resp.into_body().collect().await;
    status
}

/// First exposition line for `name` that contains every fragment in `parts`.
fn line<'a>(text: &'a str, name: &str, parts: &[&str]) -> &'a str {
    text.lines()
        .find(|l| l.starts_with(name) && parts.iter().all(|p| l.contains(p)))
        .unwrap_or_else(|| panic!("no {name} line with {parts:?} in:\n{text}"))
}

fn value(line: &str) -> f64 {
    line.rsplit(' ').next().unwrap().parse().unwrap()
}

#[test]
fn http_and_land_metrics_use_route_templates() {
    let blob = b"hello metrics";
    let hash = Hash::of_blob(blob);
    let missing = Hash::of_blob(b"never uploaded");

    let text = with_metrics(async {
        let repo = Repo::new(
            Arc::new(CachedBlobStore::new(MemoryBlobStore::new(), 1 << 20)),
            Arc::new(MemoryMetaStore::new()),
        );
        let app = leep_server::router(repo);

        assert_eq!(
            send(&app, Method::GET, "/v1/health", vec![]).await,
            StatusCode::OK
        );
        let put = format!("/v1/blobs/{hash}");
        assert_eq!(
            send(&app, Method::PUT, &put, blob.to_vec()).await,
            StatusCode::NO_CONTENT
        );
        assert_eq!(send(&app, Method::GET, &put, vec![]).await, StatusCode::OK);
        let miss = format!("/v1/blobs/{missing}");
        assert_eq!(
            send(&app, Method::GET, &miss, vec![]).await,
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            send(&app, Method::GET, "/no/such/route/abc123", vec![]).await,
            StatusCode::NOT_FOUND
        );

        let cs = Changeset {
            parents: vec![],
            author: "t".into(),
            timestamp: 1,
            message: "m".into(),
            changes: BTreeMap::from([(
                RepoPath::new("a.txt").unwrap(),
                FileChange::Modify {
                    content: hash,
                    kind: FileKind::Regular,
                    size: blob.len() as u64,
                },
            )]),
            extra: BTreeMap::new(),
        };
        let req = Request::builder()
            .method(Method::POST)
            .uri("/v1/changesets")
            .body(Body::from(serde_json::to_vec(&cs).unwrap()))
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let id = serde_json::from_slice::<serde_json::Value>(&body).unwrap()["id"].clone();
        let land = serde_json::json!({ "changeset": id, "target_ref": "main" });
        assert_eq!(
            send(
                &app,
                Method::POST,
                "/v1/land",
                serde_json::to_vec(&land).unwrap()
            )
            .await,
            StatusCode::OK
        );
    });

    let l = line(
        &text,
        "leep_http_requests_total",
        &[
            r#"route="/v1/blobs/{hash}""#,
            r#"method="GET""#,
            r#"status="404""#,
        ],
    );
    assert_eq!(value(l), 1.0);
    line(
        &text,
        "leep_http_requests_total",
        &[
            r#"route="/v1/blobs/{hash}""#,
            r#"method="PUT""#,
            r#"status="204""#,
        ],
    );
    line(
        &text,
        "leep_http_requests_total",
        &[r#"route="/v1/health""#],
    );
    line(
        &text,
        "leep_http_requests_total",
        &[r#"route="unmatched""#, r#"status="404""#],
    );
    line(
        &text,
        "leep_http_request_duration_seconds_count",
        &[r#"route="/v1/land""#],
    );
    // Raw paths and hashes must never become label values.
    for raw in [hash.to_string(), missing.to_string(), "abc123".to_string()] {
        assert!(!text.contains(&raw), "{raw} leaked into metrics:\n{text}");
    }

    assert_eq!(
        value(line(&text, "leep_land_total", &[r#"outcome="landed""#])),
        1.0
    );
    assert_eq!(value(line(&text, "leep_land_attempts_count", &[])), 1.0);
    line(&text, "leep_land_duration_seconds_count", &[]);
    line(&text, "leep_land_lock_wait_seconds_count", &[]);
    assert!(value(line(&text, "leep_derive_total", &[r#"kind="#])) >= 1.0);
    line(&text, "leep_derive_duration_seconds_count", &[r#"kind="#]);

    // The GET and the changeset's blob check both hit the cache.
    assert!(value(line(&text, "leep_blob_cache_hits_total", &[])) >= 2.0);
    assert!(value(line(&text, "leep_blob_cache_entries", &[])) >= 1.0);
    assert!(value(line(&text, "leep_blob_cache_bytes", &[])) >= blob.len() as f64);
    // The 404 lookup went through the cache and missed.
    assert!(value(line(&text, "leep_blob_cache_misses_total", &[])) >= 1.0);
}

#[tokio::test]
async fn metrics_listener_serves_text_exposition() {
    let recorder = PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    metrics::with_local_recorder(&recorder, leep_server::observe::record_build_info);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = leep_server::observe::metrics_app(move || handle.render());
    tokio::spawn(leep_server::serve(listener, app, std::future::pending()));

    let http = reqwest::Client::new();
    let resp = http
        .get(format!("http://{addr}/metrics"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let ctype = resp.headers()["content-type"].to_str().unwrap().to_string();
    assert!(ctype.starts_with("text/plain"), "{ctype}");
    let body = resp.text().await.unwrap();
    assert!(body.contains("# TYPE leep_build_info gauge"), "{body}");
    assert!(
        body.contains(&format!(
            r#"leep_build_info{{version="{}"}} 1"#,
            env!("CARGO_PKG_VERSION")
        )),
        "{body}"
    );
    // Only /metrics lives on this listener.
    let other = http
        .get(format!("http://{addr}/v1/health"))
        .send()
        .await
        .unwrap();
    assert_eq!(other.status(), 404);
}

/// The warmer counts the rows it derives by kind, beside the heads
/// `leep_index_derived_total` counts for search; rows a search derived
/// inline are in `leep_derive_total` only.
#[test]
fn warm_passes_count_rows_by_kind() {
    let text = with_metrics(async {
        let repo = Repo::new(
            Arc::new(MemoryBlobStore::new()),
            Arc::new(MemoryMetaStore::new()),
        );
        let mut changes = BTreeMap::new();
        for path in ["a/x.txt", "a/b/y.txt", "z.txt"] {
            let body = bytes::Bytes::from(format!("text of {path}\n"));
            let content = repo.blobs().put(body.clone()).await.unwrap();
            changes.insert(
                RepoPath::new(path).unwrap(),
                FileChange::Modify {
                    content,
                    kind: FileKind::Regular,
                    size: body.len() as u64,
                },
            );
        }
        let cs = |parents, changes, message: &str| Changeset {
            parents,
            author: "a".into(),
            timestamp: 1,
            message: message.into(),
            changes,
            extra: BTreeMap::new(),
        };
        let base = repo
            .create_changeset(cs(vec![], changes, "base"))
            .await
            .unwrap();
        repo.meta().cas_ref("main", None, Some(base)).await.unwrap();
        let mut warmer = leep_derive::warm::Warmer::new(repo.clone(), true);
        assert_eq!(warmer.pass().await.unwrap().warmed, 1);

        // A search derives the next head's rows inline; the warm after it
        // finds them stored.
        let body = bytes::Bytes::from_static(b"changed\n");
        let content = repo.blobs().put(body.clone()).await.unwrap();
        let change = FileChange::Modify {
            content,
            kind: FileKind::Regular,
            size: body.len() as u64,
        };
        let changes = BTreeMap::from([(RepoPath::new("a/b/y.txt").unwrap(), change)]);
        let next = repo
            .create_changeset(cs(vec![base], changes, "next"))
            .await
            .unwrap();
        let q = leep_derive::SearchQuery::new("changed");
        assert_eq!(repo.search(next, &q).await.unwrap().matches.len(), 1);
        repo.meta()
            .cas_ref("main", Some(base), Some(next))
            .await
            .unwrap();
        assert_eq!(warmer.pass().await.unwrap().warmed, 1);
    });
    let rows = |kind: &str| {
        value(line(
            &text,
            "leep_index_rows_total",
            &[&format!("kind=\"{kind}\"")],
        ))
    };
    // The base's three directories and three files; the next head's rows
    // were the search's.
    assert_eq!(rows("tree-trigrams-v1"), 3.0);
    assert_eq!(rows("blob-trigrams-v1"), 3.0);
    assert_eq!(rows("history-tree-v4"), 2.0);
    assert_eq!(rows("log-filter-v1"), 2.0);
    let derived = |kind: &str| {
        value(line(
            &text,
            "leep_derive_total",
            &[&format!("kind=\"{kind}\"")],
        ))
    };
    assert_eq!(derived("tree-trigrams-v1"), 3.0 + 3.0);
    assert_eq!(derived("blob-trigrams-v1"), 3.0 + 1.0);
    // A warmer of every group labels its passes `all`; the server's loops
    // label theirs with their group.
    line(&text, "leep_warm_duration_seconds", &["group=\"all\""]);
    // Heads, for search, as before.
    let heads = line(
        &text,
        "leep_index_derived_total",
        &["kind=\"tree-trigrams-v1\""],
    );
    assert_eq!(value(heads), 2.0);
}
