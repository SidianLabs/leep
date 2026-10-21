//! Index-backed reads behind a lagging index (ADR 0058) through the router:
//! `Leep-Indexing: retry` gets 503 with `Retry-After`, `stale-ok` an answer
//! at the newest indexed ancestor, and every indexed read says how much it
//! scanned without the index.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::Router;
use bytes::Bytes;
use http_body_util::BodyExt;
use leep_core::api::*;
use leep_core::memory::{MemoryBlobStore, MemoryMetaStore};
use leep_core::{
    BlobStore, Changeset, ChangesetId, FileChange, FileKind, LeaseClaim, LeaseKey, LeaseTiming,
    MetaStore, RepoPath,
};
use leep_derive::derived::{self, HistoryTree};
use leep_derive::indexing::{
    Budgets, IndexGroup, IndexLeaseConfig, IndexLeases, IndexMode, IndexPolicy,
};
use leep_derive::Repo;
use tower::ServiceExt;

struct Fixture {
    repo: Repo,
    ids: Vec<ChangesetId>,
}

/// A chain of `n` changesets editing `f` (and `g` every third), indexed up
/// to `indexed`, with every index lease held by a live holder.
async fn fixture(n: usize, indexed: usize) -> Fixture {
    let meta = Arc::new(MemoryMetaStore::new().with_lease_clock(Arc::new(|| 5_000)));
    let blobs = Arc::new(MemoryBlobStore::new());
    let repo = Repo::new(blobs.clone(), meta.clone());
    let mut ids = Vec::new();
    for i in 0..n {
        let mut changes = BTreeMap::new();
        for (path, every) in [("f", 1), ("g", 3)] {
            if i % every == 0 {
                let body = format!("line {i}\nneedle {i}\n");
                let content = blobs.put(Bytes::from(body.clone())).await.unwrap();
                changes.insert(
                    RepoPath::new(path).unwrap(),
                    FileChange::Modify {
                        content,
                        kind: FileKind::Regular,
                        size: body.len() as u64,
                    },
                );
            }
        }
        let id = repo
            .create_changeset(Changeset {
                parents: ids.last().copied().into_iter().collect(),
                author: format!("dev{}", i % 2),
                timestamp: i as i64,
                message: format!("c{i}"),
                changes,
                extra: BTreeMap::new(),
            })
            .await
            .unwrap();
        ids.push(id);
    }
    derived::derive::<HistoryTree>(&repo, ids[indexed])
        .await
        .unwrap();
    derived::derive::<derived::LogFilterIndex>(&repo, ids[indexed])
        .await
        .unwrap();
    repo.derive_search(ids[indexed]).await.unwrap();
    let timing = LeaseTiming {
        ttl: Duration::from_secs(15),
        stale_after: Duration::from_secs(6),
    };
    for g in IndexGroup::ALL {
        let key = LeaseKey::index(g.lease_name());
        meta.acquire_lease(
            &LeaseClaim {
                key: &key,
                holder: "indexer",
                address: "",
                protocol: 1,
                meta: "{\"rate\":10.0,\"backlog\":0}",
            },
            timing,
        )
        .await
        .unwrap();
    }
    Fixture { repo, ids }
}

fn router(repo: &Repo, budgets: Budgets) -> Router {
    let leases = IndexLeases::new(repo.meta().clone(), IndexLeaseConfig::new("query", ""));
    let policy = IndexPolicy::new(IndexMode::Off, leases)
        .with_budgets(budgets)
        .with_index_wait(Duration::ZERO, 4);
    leep_server::router(repo.clone().with_index_policy(Arc::new(policy)))
}

struct Reply {
    status: StatusCode,
    headers: axum::http::HeaderMap,
    body: Vec<u8>,
}

async fn get(app: &Router, uri: &str, indexing: Option<&str>) -> Reply {
    let mut req = Request::builder().uri(uri);
    if let Some(v) = indexing {
        req = req.header(INDEXING_HEADER, v);
    }
    let resp = app
        .clone()
        .oneshot(req.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let (parts, body) = resp.into_parts();
    Reply {
        status: parts.status,
        headers: parts.headers,
        body: body.collect().await.unwrap().to_bytes().to_vec(),
    }
}

fn json<T: serde::de::DeserializeOwned>(r: &Reply) -> T {
    serde_json::from_slice(&r.body)
        .unwrap_or_else(|e| panic!("{e}: {}", String::from_utf8_lossy(&r.body)))
}

fn scanned(r: &Reply) -> u64 {
    r.headers[INDEX_SCANNED_HEADER]
        .to_str()
        .unwrap()
        .parse()
        .unwrap()
}

#[tokio::test]
async fn retry_gets_503_with_retry_after_and_stale_ok_an_older_answer() {
    let fx = fixture(40, 9).await;
    let tip = *fx.ids.last().unwrap();
    let app = router(
        &fx.repo,
        Budgets {
            scan: 5,
            blame: 5,
            search_files: 0,
            ..Budgets::default()
        },
    );
    let plain = leep_server::router(fx.repo.clone());
    for uri in [
        format!("/v1/log/{tip}?path=g&limit=3"),
        format!("/v1/log/{tip}?author=dev1&limit=3"),
        format!("/v1/last-change/{tip}?path=g"),
        format!("/v1/blame/{tip}?path=f"),
        format!("/v1/search/{tip}?q=needle"),
    ] {
        let r = get(&app, &uri, Some("retry")).await;
        assert_eq!(r.status, StatusCode::SERVICE_UNAVAILABLE, "{uri}");
        let retry: u64 = r.headers["retry-after"].to_str().unwrap().parse().unwrap();
        assert!((1..=30).contains(&retry), "{uri}: {retry}");
        let hint = r.headers[INDEXING_HEADER].to_str().unwrap();
        assert!(
            hint.starts_with("kind=") && hint.contains("; behind="),
            "{hint}"
        );
        let e: ErrorResponse = json(&r);
        assert_eq!(e.code.as_deref(), Some("indexing"), "{uri}");
        let info = e.indexing.expect("indexing details");
        assert!(e.error.starts_with("indexing: "), "{}", e.error);
        assert_eq!(info.retry_after_ms, retry * 1000);
        if !uri.contains("/search/") {
            assert_eq!(info.behind, 30, "{uri}");
        }

        // stale-ok: 200 at the newest indexed first-parent ancestor.
        let r = get(&app, &uri, Some("stale-ok")).await;
        assert_eq!(
            r.status,
            StatusCode::OK,
            "{uri}: {}",
            String::from_utf8_lossy(&r.body)
        );
        let v: serde_json::Value = json(&r);
        assert_eq!(v["answered_at"], fx.ids[9].to_string(), "{uri}");
        assert_eq!(v["behind"], 30);
        // ... equal to the query at that changeset.
        let at_uri = uri.replace(&tip.to_string(), &fx.ids[9].to_string());
        let want: serde_json::Value = json(&get(&plain, &at_uri, None).await);
        let mut got = v.clone();
        got.as_object_mut().unwrap().remove("answered_at");
        got.as_object_mut().unwrap().remove("behind");
        if let Some(s) = got.get_mut("stats") {
            *s = want["stats"].clone();
        }
        let mut want = want;
        if want.get("next").is_some() {
            // Cursors differ in text (a stale one pins its changeset).
            want.as_object_mut().unwrap().remove("next");
            assert!(got.as_object_mut().unwrap().remove("next").is_some());
        }
        assert_eq!(got, want, "{uri}");
    }
}

#[tokio::test]
async fn within_budget_answers_equal_the_indexed_ones_and_say_what_they_scanned() {
    let fx = fixture(30, 24).await;
    let tip = *fx.ids.last().unwrap();
    let app = router(&fx.repo, Budgets::default());
    let reference = fixture(30, 29).await;
    let full = leep_server::router(reference.repo.clone());
    for uri in [
        format!("/v1/log/{tip}?path=g&limit=4"),
        format!("/v1/log/{tip}?path=f&follow=true&limit=4"),
        format!("/v1/log/{tip}?author=dev1&limit=4"),
        format!("/v1/last-change/{tip}?path=g"),
        format!("/v1/blame/{tip}?path=g"),
    ] {
        let r = get(&app, &uri, Some("retry")).await;
        assert_eq!(r.status, StatusCode::OK, "{uri}");
        let want = get(&full, &uri, None).await;
        let (a, b): (serde_json::Value, serde_json::Value) = (json(&r), json(&want));
        assert_eq!(a.get("next").is_some(), b.get("next").is_some(), "{uri}");
        let strip = |mut v: serde_json::Value| {
            v.as_object_mut().unwrap().remove("next");
            v
        };
        assert_eq!(strip(a), strip(b), "{uri}");
        if !uri.contains("author") {
            assert!(scanned(&r) > 0, "{uri}");
        }
        // Without a header too, the same answer.
        assert_eq!(get(&app, &uri, None).await.status, StatusCode::OK);
    }
    // A fully indexed read scanned nothing.
    let r = get(&full, &format!("/v1/log/{tip}?path=g"), Some("retry")).await;
    assert_eq!(scanned(&r), 0);
}

#[tokio::test]
async fn old_clients_get_larger_budgets_and_the_ready_feature_is_listed() {
    let fx = fixture(40, 9).await;
    let tip = *fx.ids.last().unwrap();
    let app = router(
        &fx.repo,
        Budgets {
            scan: 5,
            legacy_factor: 8,
            ..Budgets::default()
        },
    );
    let uri = format!("/v1/log/{tip}?path=g");
    assert_eq!(
        get(&app, &uri, Some("retry")).await.status,
        StatusCode::SERVICE_UNAVAILABLE
    );
    let r = get(&app, &uri, None).await;
    assert_eq!(r.status, StatusCode::OK);
    assert_eq!(scanned(&r), 30);
    let ready: ReadyResponse = json(&get(&app, "/v1/ready", None).await);
    assert!(ready.features.iter().any(|f| f == FEATURE_INDEXING_RETRY));
}
