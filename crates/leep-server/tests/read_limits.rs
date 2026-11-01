//! Concurrency limits per class of expensive read (ADR 0060, section 1;
//! ADR 0074), through the real router on memory stores. A read holds its
//! permit until its response body is finished or dropped, so a read whose
//! body is left unread holds the class full without any sleep; a store that
//! stalls reads on demand holds one inside its handler.

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::body::{Body, Bytes};
use axum::http::{header, Method, Request, Response, StatusCode};
use axum::Router;
use http_body_util::BodyExt;
use leep_core::api::{ErrorResponse, LandRequest, REFUSED_HEADER, REFUSED_READ_CLASS};
use leep_core::memory::{MemoryBlobStore, MemoryMetaStore};
use leep_core::{
    Changeset, ChangesetId, FileChange, FileKind, Hash, MetaStore, RefLogEntry, RefUpdateContext,
    RepoPath, Result,
};
use leep_derive::Repo;
use leep_server::read_limits::{ClassLimit, ReadConcurrency};
use metrics_exporter_prometheus::PrometheusBuilder;
use tokio::sync::{Notify, Semaphore};
use tower::ServiceExt;

/// Search reads of the trigram rows wait while `armed`, and every changeset
/// and derived read while `armed_all`, after saying so on `entered`, until
/// [`Gate::open`]. Everything else goes straight through.
struct Gate {
    armed: AtomicBool,
    armed_all: AtomicBool,
    entered: Notify,
    /// Never has permits; closing it lets every waiter through.
    release: Semaphore,
}

impl Default for Gate {
    fn default() -> Self {
        Gate {
            armed: AtomicBool::new(false),
            armed_all: AtomicBool::new(false),
            entered: Notify::new(),
            release: Semaphore::new(0),
        }
    }
}

impl Gate {
    fn open(&self) {
        self.armed.store(false, Ordering::SeqCst);
        self.armed_all.store(false, Ordering::SeqCst);
        self.release.close();
    }
}

struct GatedMeta {
    inner: MemoryMetaStore,
    gate: Arc<Gate>,
    /// Every call, to show a refused request read nothing.
    calls: AtomicUsize,
}

impl GatedMeta {
    async fn pass(&self, kind: &str) {
        let all = self.gate.armed_all.load(Ordering::SeqCst);
        if all || (kind == leep_derive::search::TREE_KIND && self.gate.armed.load(Ordering::SeqCst))
        {
            self.gate.entered.notify_one();
            let _ = self.gate.release.acquire().await;
        }
    }
    fn count(&self) {
        self.calls.fetch_add(1, Ordering::SeqCst);
    }
}

#[async_trait::async_trait]
impl MetaStore for GatedMeta {
    async fn put_changeset(&self, cs: &Changeset) -> Result<ChangesetId> {
        self.count();
        self.inner.put_changeset(cs).await
    }
    async fn get_changeset(&self, id: &ChangesetId) -> Result<Option<Changeset>> {
        self.count();
        self.pass("").await;
        self.inner.get_changeset(id).await
    }
    async fn list_changesets(
        &self,
        after: Option<ChangesetId>,
        limit: usize,
    ) -> Result<Vec<ChangesetId>> {
        self.count();
        self.inner.list_changesets(after, limit).await
    }
    async fn get_ref(&self, name: &str) -> Result<Option<ChangesetId>> {
        self.count();
        self.inner.get_ref(name).await
    }
    async fn cas_ref_logged(
        &self,
        name: &str,
        expected: Option<ChangesetId>,
        new: Option<ChangesetId>,
        ctx: &RefUpdateContext,
    ) -> Result<bool> {
        self.count();
        self.inner.cas_ref_logged(name, expected, new, ctx).await
    }
    async fn find_lands(
        &self,
        name: &str,
        ids: &[leep_core::LandId],
    ) -> Result<Vec<Option<leep_core::LandRecord>>> {
        self.count();
        self.inner.find_lands(name, ids).await
    }
    async fn ref_log(
        &self,
        name: Option<&str>,
        before_seq: Option<u64>,
        limit: usize,
    ) -> Result<Vec<RefLogEntry>> {
        self.count();
        self.inner.ref_log(name, before_seq, limit).await
    }
    async fn list_refs(&self) -> Result<BTreeMap<String, ChangesetId>> {
        self.count();
        self.inner.list_refs().await
    }
    async fn get_derived(&self, kind: &str, key: &Hash) -> Result<Option<Bytes>> {
        self.count();
        self.pass(kind).await;
        self.inner.get_derived(kind, key).await
    }
    async fn get_derived_many(&self, kind: &str, keys: &[Hash]) -> Result<Vec<Option<Bytes>>> {
        self.count();
        self.pass(kind).await;
        self.inner.get_derived_many(kind, keys).await
    }
    async fn put_derived(&self, kind: &str, key: &Hash, value: Bytes) -> Result<()> {
        self.count();
        self.inner.put_derived(kind, key, value).await
    }
}

struct Rig {
    app: Router,
    gate: Arc<Gate>,
    meta: Arc<GatedMeta>,
    /// Two changesets on `main`, and a child of the head to land.
    head: ChangesetId,
    child: ChangesetId,
}

async fn commit(repo: &Repo, parents: &[ChangesetId], path: &str, body: &str) -> ChangesetId {
    let content = repo
        .blobs()
        .put(Bytes::from(body.to_string()))
        .await
        .unwrap();
    repo.create_changeset(Changeset {
        parents: parents.to_vec(),
        author: "t".into(),
        timestamp: 1,
        message: path.into(),
        changes: BTreeMap::from([(
            RepoPath::new(path).unwrap(),
            FileChange::Modify {
                content,
                kind: FileKind::Regular,
                size: body.len() as u64,
            },
        )]),
        extra: BTreeMap::new(),
    })
    .await
    .unwrap()
}

async fn rig_with(classes: ReadConcurrency, timeout: Duration) -> Rig {
    let gate = Arc::new(Gate::default());
    let meta = Arc::new(GatedMeta {
        inner: MemoryMetaStore::new(),
        gate: gate.clone(),
        calls: AtomicUsize::new(0),
    });
    let repo = Repo::new(Arc::new(MemoryBlobStore::new()), meta.clone());
    let root = commit(&repo, &[], "a.txt", "needle one\n").await;
    let head = commit(&repo, &[root], "d/b.txt", "needle two\nhay\n").await;
    assert!(meta.cas_ref("main", None, Some(head)).await.unwrap());
    let child = commit(&repo, &[head], "c.txt", "new\n").await;
    let config = leep_server::reload::ConfigHandle::fixed(
        leep_server::reload::RuntimeConfig::fixed(None, None, None, None, None),
    );
    let app = leep_server::router_with_read_limits(
        repo,
        1 << 20,
        leep_server::limits::BodyLimits::under(1 << 20),
        timeout,
        config,
        leep_server::ReadyOptions::default(),
        classes,
    );
    Rig {
        app,
        gate,
        meta,
        head,
        child,
    }
}

/// Only the search class limited.
fn search_only(search: ClassLimit) -> ReadConcurrency {
    ReadConcurrency {
        search,
        ..ReadConcurrency::off()
    }
}

async fn rig(search: ClassLimit) -> Rig {
    rig_with(search_only(search), Duration::from_secs(60)).await
}

/// One permit each for blame and filtered log, search off.
fn one_each() -> ReadConcurrency {
    ReadConcurrency {
        blame: ClassLimit::Permits(1),
        log_filtered: ClassLimit::Permits(1),
        ..ReadConcurrency::off()
    }
}

const SEARCH: &str = "/v1/search/main?q=needle";

fn get_req(uri: &str) -> Request<Body> {
    Request::builder().uri(uri).body(Body::empty()).unwrap()
}

/// The response head only: its body, and with it any permit, stays held
/// until the response is read or dropped.
async fn start(app: &Router, uri: &str) -> Response<Body> {
    app.clone().oneshot(get_req(uri)).await.unwrap()
}

#[derive(Debug)]
struct Reply {
    status: StatusCode,
    headers: axum::http::HeaderMap,
    body: Vec<u8>,
}

impl Reply {
    fn error(&self) -> ErrorResponse {
        serde_json::from_slice(&self.body)
            .unwrap_or_else(|e| panic!("not a JSON error ({e}): {:?}", self.body))
    }
}

async fn read(resp: Response<Body>) -> Reply {
    let (parts, body) = resp.into_parts();
    Reply {
        status: parts.status,
        headers: parts.headers,
        body: body.collect().await.unwrap().to_bytes().to_vec(),
    }
}

async fn send(app: &Router, req: Request<Body>) -> Reply {
    read(app.clone().oneshot(req).await.unwrap()).await
}

async fn get(app: &Router, uri: &str) -> Reply {
    send(app, get_req(uri)).await
}

fn assert_refused(r: &Reply) {
    assert_eq!(r.status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(r.headers[REFUSED_HEADER], REFUSED_READ_CLASS);
    let secs: u64 = r.headers[header::RETRY_AFTER]
        .to_str()
        .unwrap()
        .parse()
        .unwrap();
    assert!((1..=10).contains(&secs), "Retry-After {secs}");
    let e = r.error();
    assert_eq!(e.code.as_deref(), Some("unavailable"));
    assert!(e.error.contains("--read-concurrency"), "{}", e.error);
    assert!(e.request_id.is_some());
}

/// Wait until a search is stalled inside its handler, holding its permit.
async fn until_stalled(gate: &Gate) {
    tokio::time::timeout(Duration::from_secs(10), gate.entered.notified())
        .await
        .expect("the search reached the store");
}

#[tokio::test]
async fn a_full_search_class_refuses_at_once_with_503_and_leep_refused() {
    let r = rig(ClassLimit::Permits(1)).await;
    let held = start(&r.app, SEARCH).await;
    assert_eq!(held.status(), StatusCode::OK);

    let refused = get(&r.app, "/v1/search/main?q=hay").await;
    assert_refused(&refused);

    // Finishing the held answer frees its permit.
    let done = read(held).await;
    assert_eq!(done.status, StatusCode::OK);
    assert!(done.headers.get(REFUSED_HEADER).is_none());
    assert_eq!(get(&r.app, SEARCH).await.status, StatusCode::OK);
}

#[tokio::test]
async fn the_class_admits_as_many_searches_as_it_has_permits() {
    let r = rig(ClassLimit::Permits(2)).await;
    let a = start(&r.app, SEARCH).await;
    let b = start(&r.app, SEARCH).await;
    assert_eq!((a.status(), b.status()), (StatusCode::OK, StatusCode::OK));
    assert_refused(&get(&r.app, SEARCH).await);
    drop(a);
    assert_eq!(get(&r.app, SEARCH).await.status, StatusCode::OK);
    drop(b);
}

#[tokio::test]
async fn the_permit_is_released_when_a_search_fails() {
    let r = rig(ClassLimit::Permits(1)).await;
    for uri in [
        "/v1/search/nope?q=needle",
        "/v1/search/main?q=(&regex=true",
        "/v1/search/main",
    ] {
        let failed = get(&r.app, uri).await;
        assert!(failed.status.is_client_error(), "{uri}: {}", failed.status);
        assert!(failed.headers.get(REFUSED_HEADER).is_none());
        assert_eq!(get(&r.app, SEARCH).await.status, StatusCode::OK, "{uri}");
    }
}

#[tokio::test]
async fn the_permit_is_released_when_an_answer_is_dropped_unread() {
    let r = rig(ClassLimit::Permits(1)).await;
    let held = start(&r.app, SEARCH).await;
    assert_refused(&get(&r.app, SEARCH).await);
    drop(held);
    assert_eq!(get(&r.app, SEARCH).await.status, StatusCode::OK);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_permit_is_released_when_the_client_cancels_mid_search() {
    let r = rig(ClassLimit::Permits(1)).await;
    r.gate.armed.store(true, Ordering::SeqCst);
    let app = r.app.clone();
    let stalled = tokio::spawn(async move { get(&app, SEARCH).await });
    until_stalled(&r.gate).await;
    assert_refused(&get(&r.app, SEARCH).await);

    stalled.abort();
    assert!(stalled.await.unwrap_err().is_cancelled());
    r.gate.open();
    assert_eq!(get(&r.app, SEARCH).await.status, StatusCode::OK);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_permit_is_released_when_a_search_times_out() {
    let r = rig_with(
        search_only(ClassLimit::Permits(1)),
        Duration::from_millis(300),
    )
    .await;
    r.gate.armed.store(true, Ordering::SeqCst);
    let app = r.app.clone();
    let stalled = tokio::spawn(async move { get(&app, SEARCH).await });
    until_stalled(&r.gate).await;
    assert_refused(&get(&r.app, SEARCH).await);

    assert_eq!(stalled.await.unwrap().status, StatusCode::REQUEST_TIMEOUT);
    r.gate.open();
    assert_eq!(get(&r.app, SEARCH).await.status, StatusCode::OK);
}

#[tokio::test]
async fn off_lifts_the_limit() {
    let r = rig(ClassLimit::Off).await;
    let held: Vec<_> = futures_util::future::join_all((0..8).map(|_| start(&r.app, SEARCH))).await;
    assert!(held.iter().all(|h| h.status() == StatusCode::OK));
    assert_eq!(get(&r.app, SEARCH).await.status, StatusCode::OK);
}

/// With every search permit held, lands, ref reads, file reads and the
/// other expensive reads (whose classes are not built) all go through.
#[tokio::test]
async fn lands_and_cheap_reads_go_through_while_searches_are_refused() {
    let r = rig(ClassLimit::Permits(1)).await;
    let held = start(&r.app, SEARCH).await;
    assert_refused(&get(&r.app, SEARCH).await);

    let head = r.head.to_string();
    for uri in [
        "/v1/refs".to_string(),
        "/v1/refs/main".into(),
        "/v1/resolve/main".into(),
        "/v1/reflog".into(),
        "/v1/reflog/main".into(),
        format!("/v1/tree/{head}"),
        format!("/v1/files/{head}"),
        format!("/v1/file/{head}?path=a.txt"),
        format!("/v1/changesets/{head}"),
        format!("/v1/log/{head}?path=a.txt"),
        format!("/v1/blame/{head}?path=a.txt"),
        format!("/v1/diff?to={head}"),
        "/v1/health".into(),
        "/v1/ready".into(),
    ] {
        let reply = get(&r.app, &uri).await;
        assert_eq!(reply.status, StatusCode::OK, "{uri}");
    }
    let land = LandRequest {
        changeset: r.child,
        target_ref: "main".into(),
        create: None,
        land_id: None,
    };
    let req = Request::builder()
        .method(Method::POST)
        .uri("/v1/land")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::to_vec(&land).unwrap()))
        .unwrap();
    let landed = send(&r.app, req).await;
    assert_eq!(landed.status, StatusCode::OK, "{:?}", landed.body);
    let moved = get(&r.app, "/v1/resolve/main").await;
    assert!(
        String::from_utf8_lossy(&moved.body).contains(&r.child.to_string()),
        "a fast-forward land moved main"
    );
    drop(held);
}

/// The blame and filtered-log reads of the rig, each with its class.
fn heavy(r: &Rig) -> Vec<(String, &'static str)> {
    let h = r.head;
    vec![
        (format!("/v1/blame/{h}?path=a.txt"), "blame"),
        (format!("/v1/log/{h}?path=a.txt"), "log_filtered"),
        (format!("/v1/log/{h}?author=t&limit=5"), "log_filtered"),
        (format!("/v1/last-change/{h}?path=d/b.txt"), "log_filtered"),
    ]
}

/// Over its class's permits, a blame or filtered log is refused with the
/// documented 503 before it reads anything from the metadata store.
#[tokio::test]
async fn full_blame_and_log_classes_refuse_before_any_store_read() {
    let r = rig_with(one_each(), Duration::from_secs(60)).await;
    let h = r.head;
    let blame = start(&r.app, &format!("/v1/blame/{h}?path=a.txt")).await;
    let log = start(&r.app, &format!("/v1/log/{h}?path=d/b.txt")).await;
    assert_eq!(
        (blame.status(), log.status()),
        (StatusCode::OK, StatusCode::OK)
    );
    for (uri, class) in heavy(&r) {
        let before = r.meta.calls.load(Ordering::SeqCst);
        let refused = get(&r.app, &uri).await;
        assert_refused(&refused);
        assert!(refused.error().error.contains(class), "{uri}");
        assert_eq!(
            r.meta.calls.load(Ordering::SeqCst),
            before,
            "{uri} read the store"
        );
    }
    // An unfiltered log is not in the class, and other classes are apart.
    assert_eq!(
        get(&r.app, &format!("/v1/log/{h}")).await.status,
        StatusCode::OK
    );
    drop(blame);
    let before = r.meta.calls.load(Ordering::SeqCst);
    assert_eq!(
        get(&r.app, &format!("/v1/blame/{h}?path=d/b.txt"))
            .await
            .status,
        StatusCode::OK
    );
    assert!(
        r.meta.calls.load(Ordering::SeqCst) > before,
        "an admitted blame reads"
    );
    assert_refused(&get(&r.app, &format!("/v1/last-change/{h}?path=a.txt")).await);
    drop(log);
    for (uri, _) in heavy(&r) {
        assert_eq!(get(&r.app, &uri).await.status, StatusCode::OK, "{uri}");
    }
}

#[tokio::test]
async fn blame_and_log_permits_are_released_on_error_and_dropped_answers() {
    let r = rig_with(one_each(), Duration::from_secs(60)).await;
    let h = r.head;
    for (bad, good) in [
        (
            format!("/v1/blame/{h}?path=missing.txt"),
            format!("/v1/blame/{h}?path=a.txt"),
        ),
        (
            "/v1/blame/nope?path=a.txt".to_string(),
            format!("/v1/blame/{h}?path=a.txt"),
        ),
        (
            format!("/v1/log/{h}?path=a.txt&limit=x"),
            format!("/v1/log/{h}?path=a.txt"),
        ),
        (
            "/v1/last-change/nope?path=a.txt".to_string(),
            format!("/v1/last-change/{h}?path=a.txt"),
        ),
    ] {
        let failed = get(&r.app, &bad).await;
        assert!(failed.status.is_client_error(), "{bad}: {}", failed.status);
        assert!(failed.headers.get(REFUSED_HEADER).is_none(), "{bad}");
        assert_eq!(get(&r.app, &good).await.status, StatusCode::OK, "{good}");
    }
    for (uri, _) in heavy(&r) {
        let held = start(&r.app, &uri).await;
        assert_refused(&get(&r.app, &uri).await);
        drop(held);
        assert_eq!(get(&r.app, &uri).await.status, StatusCode::OK, "{uri}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn blame_and_log_permits_are_released_when_the_client_cancels() {
    for i in 0..2 {
        let r = rig_with(one_each(), Duration::from_secs(60)).await;
        let (uri, _) = heavy(&r)[i].clone();
        r.gate.armed_all.store(true, Ordering::SeqCst);
        let (app, u) = (r.app.clone(), uri.clone());
        let stalled = tokio::spawn(async move { get(&app, &u).await });
        until_stalled(&r.gate).await;
        assert_refused(&get(&r.app, &uri).await);

        stalled.abort();
        assert!(stalled.await.unwrap_err().is_cancelled());
        r.gate.open();
        assert_eq!(get(&r.app, &uri).await.status, StatusCode::OK, "{uri}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn blame_and_log_permits_are_released_when_a_request_times_out() {
    for i in 0..2 {
        let r = rig_with(one_each(), Duration::from_millis(300)).await;
        let (uri, _) = heavy(&r)[i].clone();
        r.gate.armed_all.store(true, Ordering::SeqCst);
        let (app, u) = (r.app.clone(), uri.clone());
        let stalled = tokio::spawn(async move { get(&app, &u).await });
        until_stalled(&r.gate).await;
        assert_refused(&get(&r.app, &uri).await);

        assert_eq!(stalled.await.unwrap().status, StatusCode::REQUEST_TIMEOUT);
        r.gate.open();
        assert_eq!(get(&r.app, &uri).await.status, StatusCode::OK, "{uri}");
    }
}

#[tokio::test]
async fn off_lifts_the_blame_and_log_limits() {
    let r = rig_with(ReadConcurrency::off(), Duration::from_secs(60)).await;
    for (uri, _) in heavy(&r) {
        let held: Vec<_> =
            futures_util::future::join_all((0..8).map(|_| start(&r.app, &uri))).await;
        assert!(held.iter().all(|h| h.status() == StatusCode::OK), "{uri}");
        assert_eq!(get(&r.app, &uri).await.status, StatusCode::OK, "{uri}");
    }
}

/// The server's defaults limit blame and filtered log, at least two each.
#[tokio::test]
async fn the_default_limits_blame_and_filtered_log() {
    let r = rig_with(ReadConcurrency::default(), Duration::from_secs(60)).await;
    for (uri, class) in heavy(&r) {
        let permits = match class {
            "blame" => leep_server::read_limits::default_blame_permits(),
            _ => leep_server::read_limits::default_log_filtered_permits(),
        };
        assert!(permits >= 2, "{class}");
        let held: Vec<_> =
            futures_util::future::join_all((0..permits).map(|_| start(&r.app, &uri))).await;
        assert!(held.iter().all(|h| h.status() == StatusCode::OK), "{uri}");
        assert_refused(&get(&r.app, &uri).await);
    }
}

/// With every blame and filtered-log permit held, lands, creates and cheap
/// reads go through.
#[tokio::test]
async fn lands_and_cheap_reads_go_through_while_blame_and_log_are_refused() {
    let r = rig_with(one_each(), Duration::from_secs(60)).await;
    let h = r.head;
    let blame = start(&r.app, &format!("/v1/blame/{h}?path=a.txt")).await;
    let log = start(&r.app, &format!("/v1/log/{h}?path=a.txt")).await;
    for (uri, _) in heavy(&r) {
        assert_refused(&get(&r.app, &uri).await);
    }
    for uri in [
        "/v1/refs/main".to_string(),
        format!("/v1/files/{h}"),
        format!("/v1/file/{h}?path=a.txt"),
        format!("/v1/log/{h}?limit=5"),
        format!("/v1/diff?to={h}"),
        "/v1/search/main?q=needle".into(),
    ] {
        assert_eq!(get(&r.app, &uri).await.status, StatusCode::OK, "{uri}");
    }
    let land = LandRequest {
        changeset: r.child,
        target_ref: "main".into(),
        create: None,
        land_id: None,
    };
    let req = Request::builder()
        .method(Method::POST)
        .uri("/v1/land")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::to_vec(&land).unwrap()))
        .unwrap();
    assert_eq!(send(&r.app, req).await.status, StatusCode::OK);
    drop((blame, log));
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

fn value(text: &str, name: &str, parts: &[&str]) -> f64 {
    let line = text
        .lines()
        .find(|l| l.split(['{', ' ']).next() == Some(name) && parts.iter().all(|p| l.contains(p)))
        .unwrap_or_else(|| panic!("no {name} line with {parts:?} in:\n{text}"));
    line.rsplit(' ').next().unwrap().parse().unwrap()
}

#[test]
fn permits_in_flight_and_sheds_are_counted_by_class() {
    let text = with_metrics(async {
        let r = rig(ClassLimit::Permits(1)).await;
        let held = start(&r.app, SEARCH).await;
        assert_refused(&get(&r.app, SEARCH).await);
        assert_refused(&get(&r.app, SEARCH).await);
        drop(held);
        assert_eq!(get(&r.app, SEARCH).await.status, StatusCode::OK);
    });
    let class = r#"class="search""#;
    assert_eq!(value(&text, "leep_read_class_permits", &[class]), 1.0);
    assert_eq!(value(&text, "leep_read_class_shed_total", &[class]), 2.0);
    assert_eq!(value(&text, "leep_read_class_in_flight", &[class]), 0.0);
    // Sheds are 503s in the request counter, which LeepServerErrors
    // subtracts them from; no other label carries a ref or a query.
    assert_eq!(
        value(
            &text,
            "leep_http_requests_total",
            &[r#"status="503""#, "/v1/search"]
        ),
        2.0
    );
    for line in text.lines().filter(|l| l.starts_with("leep_read_class")) {
        assert!(!line.contains("main") && !line.contains("needle"), "{line}");
    }
}

#[test]
fn admitted_and_completed_reads_are_counted_by_class() {
    let text = with_metrics(async {
        let r = rig_with(one_each(), Duration::from_secs(60)).await;
        let h = r.head;
        let held = start(&r.app, &format!("/v1/blame/{h}?path=a.txt")).await;
        assert_refused(&get(&r.app, &format!("/v1/blame/{h}?path=a.txt")).await);
        drop(held);
        let ok = format!("/v1/blame/{h}?path=a.txt");
        assert_eq!(get(&r.app, &ok).await.status, StatusCode::OK);
        let missing = format!("/v1/blame/{h}?path=missing.txt");
        assert!(get(&r.app, &missing).await.status.is_client_error());
        let log = format!("/v1/log/{h}?path=a.txt");
        assert_eq!(get(&r.app, &log).await.status, StatusCode::OK);
    });
    let blame = r#"class="blame""#;
    let log = r#"class="log_filtered""#;
    assert_eq!(value(&text, "leep_read_class_permits", &[blame]), 1.0);
    assert_eq!(value(&text, "leep_read_class_shed_total", &[blame]), 1.0);
    // The dropped answer, the 200 and the 404.
    assert_eq!(
        value(&text, "leep_read_class_admitted_total", &[blame]),
        3.0
    );
    // A 2xx whose body was dropped is still answered; the 404 is not.
    assert_eq!(
        value(&text, "leep_read_class_completed_total", &[blame]),
        2.0
    );
    assert_eq!(value(&text, "leep_read_class_in_flight", &[blame]), 0.0);
    assert_eq!(value(&text, "leep_read_class_admitted_total", &[log]), 1.0);
    assert_eq!(value(&text, "leep_read_class_completed_total", &[log]), 1.0);
    assert_eq!(value(&text, "leep_read_class_shed_total", &[log]), 0.0);
}

fn start_server(args: &[&str]) -> std::process::Output {
    std::process::Command::new(env!("CARGO_BIN_EXE_leep-server"))
        .args([
            "--listen",
            "127.0.0.1:0",
            "--meta",
            "mem://",
            "--blobs",
            "mem://",
        ])
        .args(args)
        .output()
        .unwrap()
}

#[test]
fn bad_read_limit_settings_fail_startup_with_a_clear_error() {
    let cases: [(&[&str], &str); 7] = [
        (&["--read-concurrency", "search=0"], "whole number"),
        (&["--read-concurrency", "grep=1"], "unknown class"),
        (&["--read-concurrency", "diff_large=2"], "does not limit"),
        (&["--read-concurrency", "blame=0"], "whole number"),
        (&["--read-concurrency", "search"], "CLASS=N"),
        (&["--search-pool-permits", "0"], "at least 1"),
        (&["--search-pool-permits", "lots"], "or off"),
    ];
    for (args, want) in cases {
        let out = start_server(args);
        assert!(!out.status.success(), "{args:?}");
        let err = String::from_utf8_lossy(&out.stderr);
        assert!(err.contains(want), "{args:?}: {err}");
    }
}
