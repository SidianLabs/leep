//! Land admission per ref (ADR 0060, section 2) through the real router on
//! memory stores. A store that holds ref swaps on demand keeps a batch
//! replaying, which fills a queue whose bound is one batch.

use std::collections::BTreeMap;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::body::{Body, Bytes};
use axum::http::{header, Method, Request, StatusCode};
use axum::Router;
use http_body_util::BodyExt;
use leep_core::api::{ErrorResponse, LandRequest, LandResponse, REFUSED_ADMISSION, REFUSED_HEADER};
use leep_core::memory::{MemoryBlobStore, MemoryMetaStore};
use leep_core::{
    Changeset, ChangesetId, FileChange, FileKind, Hash, LandId, LandRecord, Lease, LeaseClaim,
    LeaseKey, LeaseTiming, MetaStore, RefLogEntry, RefStep, RefUpdateContext, RepoPath, Result,
};
use leep_derive::land_coord::{CoordinatorConfig, LandCoordinator};
use leep_derive::Repo;
use metrics_exporter_prometheus::PrometheusBuilder;
use tokio::sync::{Notify, Semaphore};
use tower::ServiceExt;

/// Ref swaps wait while `armed`, after saying so on `entered`, until
/// [`Gate::open`].
struct Gate {
    armed: AtomicBool,
    entered: Notify,
    /// Never has permits; closing it lets every waiter through.
    release: Semaphore,
}

impl Default for Gate {
    fn default() -> Self {
        Gate {
            armed: AtomicBool::new(false),
            entered: Notify::new(),
            release: Semaphore::new(0),
        }
    }
}

impl Gate {
    fn open(&self) {
        self.armed.store(false, Ordering::SeqCst);
        self.release.close();
    }

    async fn pass(&self) {
        if self.armed.load(Ordering::SeqCst) {
            self.entered.notify_one();
            let _ = self.release.acquire().await;
        }
    }

    /// Wait until a swap is held, its batch replaying.
    async fn until_held(&self) {
        tokio::time::timeout(Duration::from_secs(10), self.entered.notified())
            .await
            .expect("a land reached its swap");
    }
}

struct GatedMeta {
    inner: Arc<MemoryMetaStore>,
    gate: Arc<Gate>,
}

#[async_trait::async_trait]
impl MetaStore for GatedMeta {
    async fn put_changeset(&self, cs: &Changeset) -> Result<ChangesetId> {
        self.inner.put_changeset(cs).await
    }
    async fn get_changeset(&self, id: &ChangesetId) -> Result<Option<Changeset>> {
        self.inner.get_changeset(id).await
    }
    async fn get_changesets(&self, ids: &[ChangesetId]) -> Result<Vec<Option<Changeset>>> {
        self.inner.get_changesets(ids).await
    }
    async fn list_changesets(
        &self,
        after: Option<ChangesetId>,
        limit: usize,
    ) -> Result<Vec<ChangesetId>> {
        self.inner.list_changesets(after, limit).await
    }
    async fn get_ref(&self, name: &str) -> Result<Option<ChangesetId>> {
        self.inner.get_ref(name).await
    }
    async fn cas_ref_logged(
        &self,
        name: &str,
        expected: Option<ChangesetId>,
        new: Option<ChangesetId>,
        ctx: &RefUpdateContext,
    ) -> Result<bool> {
        self.gate.pass().await;
        self.inner.cas_ref_logged(name, expected, new, ctx).await
    }
    async fn cas_ref_logged_chain(
        &self,
        name: &str,
        expected: Option<ChangesetId>,
        steps: &[RefStep],
    ) -> Result<usize> {
        self.gate.pass().await;
        self.inner.cas_ref_logged_chain(name, expected, steps).await
    }
    async fn find_lands(&self, name: &str, ids: &[LandId]) -> Result<Vec<Option<LandRecord>>> {
        self.inner.find_lands(name, ids).await
    }
    async fn ref_log(
        &self,
        name: Option<&str>,
        before_seq: Option<u64>,
        limit: usize,
    ) -> Result<Vec<RefLogEntry>> {
        self.inner.ref_log(name, before_seq, limit).await
    }
    async fn list_refs(&self) -> Result<BTreeMap<String, ChangesetId>> {
        self.inner.list_refs().await
    }
    async fn get_derived(&self, kind: &str, key: &Hash) -> Result<Option<Bytes>> {
        self.inner.get_derived(kind, key).await
    }
    async fn get_derived_many(&self, kind: &str, keys: &[Hash]) -> Result<Vec<Option<Bytes>>> {
        self.inner.get_derived_many(kind, keys).await
    }
    async fn put_derived(&self, kind: &str, key: &Hash, value: Bytes) -> Result<()> {
        self.inner.put_derived(kind, key, value).await
    }
    async fn put_derived_many(&self, rows: &[leep_core::DerivedRow]) -> Result<()> {
        self.inner.put_derived_many(rows).await
    }
    fn supports_leases(&self) -> bool {
        self.inner.supports_leases()
    }
    async fn lease(&self, key: &LeaseKey, timing: LeaseTiming) -> Result<Option<Lease>> {
        self.inner.lease(key, timing).await
    }
    async fn acquire_lease(&self, claim: &LeaseClaim<'_>, timing: LeaseTiming) -> Result<Lease> {
        self.inner.acquire_lease(claim, timing).await
    }
    async fn renew_leases(
        &self,
        holder: &str,
        held: &[(LeaseKey, u64)],
        timing: LeaseTiming,
    ) -> Result<Vec<Option<Duration>>> {
        self.inner.renew_leases(holder, held, timing).await
    }
    async fn release_lease(&self, key: &LeaseKey, holder: &str, epoch: u64) -> Result<bool> {
        self.inner.release_lease(key, holder, epoch).await
    }
}

async fn commit(repo: &Repo, parents: &[ChangesetId], path: &str) -> ChangesetId {
    let body = format!("{path}\n");
    let content = repo.blobs().put(Bytes::from(body.clone())).await.unwrap();
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

struct Rig {
    repo: Repo,
    meta: Arc<MemoryMetaStore>,
    gate: Arc<Gate>,
    /// Changesets on `main`'s head that change disjoint files.
    lands: Vec<ChangesetId>,
}

/// Stores with `main` and `n` changesets to land, and a repo whose queues
/// hold `bound` batches.
async fn rig(bound: Option<usize>, n: usize) -> Rig {
    rig_on(MemoryMetaStore::new(), bound, n).await
}

async fn rig_on(meta: MemoryMetaStore, bound: Option<usize>, n: usize) -> Rig {
    let gate = Arc::new(Gate::default());
    let meta = Arc::new(meta);
    let gated = Arc::new(GatedMeta {
        inner: meta.clone(),
        gate: gate.clone(),
    });
    let repo =
        Repo::new(Arc::new(MemoryBlobStore::new()), gated.clone()).with_land_queue_batches(bound);
    let head = commit(&repo, &[], "base.txt").await;
    assert!(gated.cas_ref("main", None, Some(head)).await.unwrap());
    let mut lands = Vec::new();
    for i in 0..n {
        lands.push(commit(&repo, &[head], &format!("f{i}.txt")).await);
    }
    Rig {
        repo,
        meta,
        gate,
        lands,
    }
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

    fn landed(&self) -> ChangesetId {
        assert_eq!(self.status, StatusCode::OK, "{:?}", self.body);
        match serde_json::from_slice(&self.body).unwrap() {
            LandResponse::Landed { landed, .. } => landed,
            other => panic!("{other:?}"),
        }
    }
}

fn land_req(cs: ChangesetId, land_id: Option<LandId>) -> Request<Body> {
    let land = LandRequest {
        changeset: cs,
        target_ref: "main".into(),
        create: None,
        land_id,
    };
    Request::builder()
        .method(Method::POST)
        .uri("/v1/land")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::to_vec(&land).unwrap()))
        .unwrap()
}

async fn send(app: &Router, req: Request<Body>) -> Reply {
    let resp = app.clone().oneshot(req).await.unwrap();
    let (parts, body) = resp.into_parts();
    Reply {
        status: parts.status,
        headers: parts.headers,
        body: body.collect().await.unwrap().to_bytes().to_vec(),
    }
}

async fn land(app: &Router, cs: ChangesetId, land_id: Option<LandId>) -> Reply {
    send(app, land_req(cs, land_id)).await
}

/// A land sent in the background, as another client's.
fn spawn_land(
    app: &Router,
    cs: ChangesetId,
    land_id: Option<LandId>,
) -> tokio::task::JoinHandle<Reply> {
    let app = app.clone();
    tokio::spawn(async move { land(&app, cs, land_id).await })
}

/// Let spawned requests run up to where they wait: on a current-thread
/// runtime with memory stores, into the land queue.
async fn settle() {
    for _ in 0..2000 {
        tokio::task::yield_now().await;
    }
}

fn assert_not_admitted(r: &Reply) {
    assert_eq!(r.status, StatusCode::SERVICE_UNAVAILABLE, "{:?}", r.body);
    assert_eq!(r.headers[REFUSED_HEADER], REFUSED_ADMISSION);
    let secs: u64 = r.headers[header::RETRY_AFTER]
        .to_str()
        .unwrap()
        .parse()
        .unwrap();
    assert!((1..=10).contains(&secs), "Retry-After {secs}");
    let e = r.error();
    assert_eq!(e.code.as_deref(), Some("unavailable"));
    assert!(e.error.contains("--land-queue-batches"), "{}", e.error);
    assert!(e.error.contains("nothing was done"), "{}", e.error);
    assert!(e.request_id.is_some());
}

async fn land_moves(meta: &MemoryMetaStore) -> usize {
    meta.ref_log(Some("main"), None, 1000)
        .await
        .unwrap()
        .iter()
        .filter(|e| e.operation == "land")
        .count()
}

#[tokio::test]
async fn a_land_over_the_bound_is_refused_at_once_with_503_and_leep_refused() {
    let r = rig(Some(1), 2).await;
    let app = leep_server::router(r.repo.clone());
    r.gate.armed.store(true, Ordering::SeqCst);
    let held = spawn_land(&app, r.lands[0], None);
    r.gate.until_held().await;

    let started = std::time::Instant::now();
    let refused = land(&app, r.lands[1], None).await;
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "refused at once"
    );
    assert_not_admitted(&refused);
    assert_eq!(land_moves(&r.meta).await, 0, "nothing moved yet");

    r.gate.open();
    held.await.unwrap().landed();
    // Sent again once the queue has room, it lands.
    land(&app, r.lands[1], None).await.landed();
    assert_eq!(land_moves(&r.meta).await, 2);
}

/// A keyed resend of a land that already landed, sent while the queue is
/// full, gets its recorded answer instead of a refusal.
#[tokio::test]
async fn a_keyed_resend_of_a_recorded_land_gets_its_answer_while_the_queue_is_full() {
    let r = rig(Some(1), 3).await;
    let app = leep_server::router(r.repo.clone());
    let id: LandId = "000102030405060708090a0b0c0d0e0f".parse().unwrap();
    let first = land(&app, r.lands[0], Some(id.clone())).await.landed();

    r.gate.armed.store(true, Ordering::SeqCst);
    let held = spawn_land(&app, r.lands[1], None);
    r.gate.until_held().await;

    let again = land(&app, r.lands[0], Some(id.clone())).await;
    assert_eq!(again.landed(), first);
    assert!(again.headers.get(REFUSED_HEADER).is_none());
    // The same id for another changeset is still a 400.
    let reused = land(&app, r.lands[2], Some(id)).await;
    assert_eq!(reused.status, StatusCode::BAD_REQUEST);
    assert_eq!(reused.error().code.as_deref(), Some("land_id_reused"));
    // A new id is refused.
    let fresh: LandId = "0f0e0d0c0b0a09080706050403020100".parse().unwrap();
    assert_not_admitted(&land(&app, r.lands[2], Some(fresh)).await);

    r.gate.open();
    held.await.unwrap().landed();
    assert_eq!(land_moves(&r.meta).await, 2);
}

/// Lands already queued are never dropped to make room: with two batches
/// allowed, one replaying and a full one waiting, every queued land lands
/// and only the newcomer is refused. With the bound off the newcomer queues
/// too.
#[tokio::test]
async fn queued_lands_are_never_dropped_and_off_admits_every_land() {
    /// Lands in a full batch.
    const FULL: usize = 64;
    for bound in [Some(2), None] {
        let r = rig(bound, FULL + 2).await;
        let app = leep_server::router(r.repo.clone());
        r.gate.armed.store(true, Ordering::SeqCst);
        let held = spawn_land(&app, r.lands[0], None);
        r.gate.until_held().await;
        let queued: Vec<_> = r.lands[1..=FULL]
            .iter()
            .map(|cs| spawn_land(&app, *cs, None))
            .collect();
        settle().await;
        assert!(queued.iter().all(|q| !q.is_finished()), "all queued");

        let last = spawn_land(&app, r.lands[FULL + 1], None);
        settle().await;
        let refused = bound.is_some();
        if refused {
            assert!(last.is_finished(), "refused without waiting");
        }
        r.gate.open();
        held.await.unwrap().landed();
        for q in queued {
            q.await.unwrap().landed();
        }
        let last = last.await.unwrap();
        if refused {
            assert_not_admitted(&last);
        } else {
            last.landed();
        }
        assert_eq!(land_moves(&r.meta).await, 1 + FULL + usize::from(!refused));
    }
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

/// Refusals are counted by route and as 503s, never as failed lands; the
/// gauge shows the largest queue in batches.
#[test]
fn refusals_are_counted_apart_from_land_errors() {
    let text = with_metrics(async {
        let r = rig(Some(1), 3).await;
        let app = leep_server::router(r.repo.clone());
        r.gate.armed.store(true, Ordering::SeqCst);
        let held = spawn_land(&app, r.lands[0], None);
        r.gate.until_held().await;
        assert_not_admitted(&land(&app, r.lands[1], None).await);
        assert_not_admitted(&land(&app, r.lands[2], None).await);
        r.gate.open();
        held.await.unwrap().landed();
    });
    assert_eq!(
        value(
            &text,
            "leep_land_admission_refused_total",
            &["route=\"direct\""]
        ),
        2.0
    );
    assert_eq!(
        value(
            &text,
            "leep_land_admission_refused_total",
            &["route=\"forwarded\""]
        ),
        0.0
    );
    assert_eq!(
        value(
            &text,
            "leep_http_requests_total",
            &["route=\"/v1/land\"", "status=\"503\""]
        ),
        2.0
    );
    assert_eq!(
        value(&text, "leep_land_total", &["outcome=\"landed\""]),
        1.0
    );
    assert!(
        !text.contains("leep_land_total{outcome=\"error\"}"),
        "a refusal is not a failed land:\n{text}"
    );
    assert_eq!(value(&text, "leep_land_queue_batches", &[]), 0.0, "drained");
}

/// One server of a coordinated pair (ADR 0056), serving on `listener`.
async fn coordinated(
    listener: tokio::net::TcpListener,
    meta: Arc<dyn MetaStore>,
    repo: Repo,
    holder: &str,
) -> Arc<LandCoordinator> {
    let addr: SocketAddr = listener.local_addr().unwrap();
    let mut cfg = CoordinatorConfig::new(holder, format!("http://{addr}/"));
    cfg.forward_wait = Duration::from_secs(5);
    let coordinator = LandCoordinator::new(meta, cfg);
    coordinator.start();
    let repo = repo.with_land_coordinator(coordinator.clone());
    let policy = leep_server::coord::PeerPolicy {
        prefixes: vec!["http://127.0.0.1:".into()],
        ..Default::default()
    };
    let http = leep_server::coord::Coordination::new(policy, holder.into()).unwrap();
    let app = leep_server::coord::layer(leep_server::router(repo), http);
    tokio::spawn(leep_server::serve(listener, app, std::future::pending()));
    coordinator
}

/// A land forwarded to a full leader counts against the leader's bound.
/// The follower relays the 503 unchanged, `Retry-After` and `Leep-Refused`
/// included, as an answered forward, and never lands it itself.
#[test]
fn a_follower_relays_the_leaders_refusal_unchanged() {
    let text = with_metrics(async {
        let origin = std::time::Instant::now();
        let clock = Arc::new(move || origin.elapsed().as_millis() as i64);
        let r = rig_on(MemoryMetaStore::new().with_lease_clock(clock), Some(1), 3).await;
        let gated: Arc<dyn MetaStore> = Arc::new(GatedMeta {
            inner: r.meta.clone(),
            gate: r.gate.clone(),
        });
        let leader_l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let follower_l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (leader_addr, follower_addr) = (
            leader_l.local_addr().unwrap(),
            follower_l.local_addr().unwrap(),
        );
        // Both share the stores; the follower has no bound of its own, so
        // only the leader can refuse.
        let leader = coordinated(leader_l, gated.clone(), r.repo.clone(), "leader").await;
        let follower_repo = Repo::new(r.repo.blobs().clone(), gated.clone());
        let follower = coordinated(follower_l, gated, follower_repo, "follower").await;

        let client = reqwest::Client::new();
        let post = |addr: SocketAddr, cs: ChangesetId| {
            let client = client.clone();
            async move {
                let body = serde_json::to_vec(&LandRequest {
                    changeset: cs,
                    target_ref: "main".into(),
                    create: None,
                    land_id: None,
                })
                .unwrap();
                client
                    .post(format!("http://{addr}/v1/land"))
                    .header("content-type", "application/json")
                    .body(body)
                    .send()
                    .await
                    .unwrap()
            }
        };
        // The leader takes the lease with a land of its own...
        assert_eq!(post(leader_addr, r.lands[0]).await.status(), 200);
        assert_eq!(leader.held_epoch("main"), Some(1));
        // ...and then holds a batch, so its queue is at the bound.
        r.gate.armed.store(true, Ordering::SeqCst);
        let held = tokio::spawn(post(leader_addr, r.lands[1]));
        r.gate.until_held().await;

        let relayed = post(follower_addr, r.lands[2]).await;
        assert_eq!(relayed.status(), 503);
        let h = relayed.headers().clone();
        assert_eq!(h[REFUSED_HEADER], REFUSED_ADMISSION);
        let secs: u64 = h["retry-after"].to_str().unwrap().parse().unwrap();
        assert!((1..=10).contains(&secs), "Retry-After {secs}");
        assert!(h.get("leep-land-not-leader").is_none());
        let e: ErrorResponse = serde_json::from_slice(&relayed.bytes().await.unwrap()).unwrap();
        assert_eq!(e.code.as_deref(), Some("unavailable"));
        assert!(e.error.contains("--land-queue-batches"), "{}", e.error);
        assert_eq!(follower.uncoordinated_lands(), 0);
        assert_eq!(follower.held_epoch("main"), None, "never led");

        r.gate.open();
        assert_eq!(held.await.unwrap().status(), 200);
        assert_eq!(
            land_moves(&r.meta).await,
            2,
            "the refused land did not land"
        );
        // Sent again once the leader has room, it lands through the leader.
        assert_eq!(post(follower_addr, r.lands[2]).await.status(), 200);
        assert_eq!(land_moves(&r.meta).await, 3);
    });
    assert_eq!(
        value(
            &text,
            "leep_land_admission_refused_total",
            &["route=\"forwarded\""]
        ),
        1.0
    );
    assert_eq!(
        value(
            &text,
            "leep_land_forwarded_total",
            &["outcome=\"answered\""]
        ),
        2.0
    );
    assert!(
        !text.contains("leep_land_forwarded_total{outcome=\"failed\""),
        "a refusal is not a failed forward:\n{text}"
    );
    // The leader's 503 and the follower's relayed copy: the alert
    // subtracts forwarded refusals twice (OPERATIONS.md).
    assert_eq!(
        value(
            &text,
            "leep_http_requests_total",
            &["route=\"/v1/land\"", "status=\"503\""]
        ),
        2.0
    );
}
