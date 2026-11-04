//! `leep review request`, `inbox` and `cancel` against a small mock of
//! `/v1/review-requests` (ADR 0086).

use std::net::TcpListener;
use std::sync::{Arc, Mutex};

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use leep_cli::cache::Cache;
use leep_cli::client::Client;
use leep_cli::commands::review_request::{self, CancelArgs, InboxArgs, RequestArgs};
use leep_cli::commands::Remote;
use leep_cli::error::ErrorKind;
use leep_core::api::*;
use leep_core::{ChangesetId, Hash};

#[derive(Default)]
struct Data {
    requests: Vec<ReviewRequest>,
    /// Raw query strings of every listing.
    queries: Vec<Vec<(String, String)>>,
    /// Who has approved each request's changeset, by request id.
    approved: std::collections::HashMap<u64, Vec<String>>,
}

type Shared = Arc<Mutex<Data>>;

/// Plays a server where the token is `ada`, and `rhea` owns everything.
fn start(with_endpoints: bool) -> (String, Shared) {
    let data: Shared = Arc::default();
    let mut app = Router::new().route(
        "/v1/whoami",
        get(|| async {
            Json(WhoAmIResponse {
                name: Some("ada".into()),
                role: Some("write".into()),
                author: None,
            })
        }),
    );
    if with_endpoints {
        app = app
            .route("/v1/review-requests", get(list).post(create))
            .route("/v1/review-requests/{id}/withdraw", post(withdraw))
            .route("/v1/changesets/{id}/review-status", get(approved));
    }
    let app = app.with_state(data.clone());
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let addr = listener.local_addr().unwrap();
    std::thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let l = tokio::net::TcpListener::from_std(listener).unwrap();
                axum::serve(l, app).await.unwrap();
            });
    });
    (format!("http://{addr}"), data)
}

fn entry(r: &ReviewRequest) -> ReviewRequestEntry {
    ReviewRequestEntry {
        request: r.clone(),
        created_by: Some(CreatedBy {
            principal: r.requester.clone(),
            timestamp: 1,
        }),
        title: Some("a change".into()),
        approved_by: Default::default(),
        rejected_by: Default::default(),
    }
}

async fn create(
    State(d): State<Shared>,
    Json(req): Json<CreateReviewRequest>,
) -> Json<CreateReviewRequestResponse> {
    let mut d = d.lock().unwrap();
    let reviewers = if req.reviewers.is_empty() {
        vec!["rhea".to_string()]
    } else {
        req.reviewers
    };
    if let Some(r) = d
        .requests
        .iter()
        .find(|r| r.changeset == req.changeset && r.reviewers == reviewers)
    {
        return Json(CreateReviewRequestResponse {
            request: entry(r),
            existing: true,
        });
    }
    let r = ReviewRequest {
        id: d.requests.len() as u64 + 1,
        changeset: req.changeset,
        target_ref: req.target_ref,
        requester: "ada".into(),
        reviewers,
        message: req.message,
        created: 1_760_000_000,
        state: ReviewRequestState::Open,
        closed: None,
        landed_as: None,
    };
    d.requests.push(r.clone());
    Json(CreateReviewRequestResponse {
        request: entry(&r),
        existing: false,
    })
}

async fn list(
    State(d): State<Shared>,
    Query(q): Query<Vec<(String, String)>>,
) -> Json<ReviewRequestsResponse> {
    let mut d = d.lock().unwrap();
    d.queries.push(q.clone());
    let get = |k: &str| q.iter().find(|(n, _)| n == k).map(|(_, v)| v.clone());
    let requests = d
        .requests
        .iter()
        .rev()
        .filter(|r| get("reviewer").is_none_or(|x| r.reviewers.contains(&x)))
        .filter(|r| get("requester").is_none_or(|x| r.requester == x))
        .filter(|r| get("open").is_none() || r.state == ReviewRequestState::Open)
        .map(|r| ReviewRequestEntry {
            approved_by: d.approved.get(&r.id).cloned().unwrap_or_default(),
            ..entry(r)
        })
        .collect();
    Json(ReviewRequestsResponse {
        requests,
        next: None,
    })
}

/// Every changeset is approved here.
async fn approved(Path(id): Path<ChangesetId>) -> Json<ReviewStatusResponse> {
    Json(ReviewStatusResponse {
        changeset: id,
        target_ref: "main".into(),
        approved: true,
        paths: vec![],
        on_refs: vec![],
    })
}

async fn withdraw(
    State(d): State<Shared>,
    Path(id): Path<u64>,
) -> Result<Json<ReviewRequestEntry>, (StatusCode, Json<ErrorResponse>)> {
    let mut d = d.lock().unwrap();
    let Some(r) = d.requests.iter_mut().find(|r| r.id == id) else {
        return Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse::new("review request not found").with_code("not_found")),
        ));
    };
    if r.state == ReviewRequestState::Open {
        r.state = ReviewRequestState::Withdrawn;
        r.closed = Some(1_760_000_001);
    }
    Ok(Json(entry(r)))
}

fn id(n: u8) -> ChangesetId {
    ChangesetId(Hash::of_blob(&[n]))
}

struct Env {
    client: Client,
    cache: Cache,
    _dir: tempfile::TempDir,
}

impl Env {
    fn new(url: &str) -> Env {
        let dir = tempfile::tempdir().unwrap();
        Env {
            client: Client::with_token(url, Some("tok-ada")),
            cache: Cache::new(dir.path()),
            _dir: dir,
        }
    }

    fn remote(&self, json: bool) -> Remote<'_> {
        Remote {
            client: &self.client,
            cache: &self.cache,
            base: Some(id(9)),
            target_ref: "main",
            json,
        }
    }
}

fn ask(rev: &str, to: &[&str], msg: Option<&str>) -> RequestArgs {
    RequestArgs {
        rev: rev.into(),
        to: to.iter().map(|s| s.to_string()).collect(),
        message: msg.map(Into::into),
        target_ref: None,
        wait: None,
    }
}

fn inbox(mine: bool, all: bool) -> InboxArgs {
    InboxArgs {
        mine,
        all,
        limit: None,
    }
}

#[test]
fn request_inbox_and_cancel() {
    let (url, data) = start(true);
    let env = Env::new(&url);
    let r = env.remote(false);

    // `@` is the working copy's base; no `--to` asks the server's default.
    assert_eq!(
        review_request::request(&r, &ask("@", &[], Some("please"))).unwrap(),
        0
    );
    let full = id(1).to_string();
    assert_eq!(
        review_request::request(&r, &ask(&full, &["bo", "cy"], None)).unwrap(),
        0
    );
    // Asking again is answered with the same request.
    assert_eq!(
        review_request::request(&r, &ask("@", &[], None)).unwrap(),
        0
    );
    {
        let d = data.lock().unwrap();
        assert_eq!(d.requests.len(), 2);
        let first = &d.requests[0];
        assert_eq!(
            (
                first.changeset,
                first.target_ref.as_str(),
                first.message.as_str()
            ),
            (id(9), "main", "please")
        );
        assert_eq!(first.reviewers, ["rhea"]);
        assert_eq!(d.requests[1].reviewers, ["bo", "cy"]);
    }

    // The inbox asks for the caller's name; --mine for what it made.
    assert_eq!(review_request::inbox(&r, &inbox(false, false)).unwrap(), 0);
    assert_eq!(
        review_request::inbox(&env.remote(true), &inbox(true, true)).unwrap(),
        0
    );
    {
        let d = data.lock().unwrap();
        let pairs = |i: usize| -> Vec<(&str, &str)> {
            d.queries[i]
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_str()))
                .collect()
        };
        assert_eq!(pairs(0), [("reviewer", "ada"), ("open", "true")]);
        assert_eq!(pairs(1), [("requester", "ada")]);
    }

    assert_eq!(
        review_request::cancel(&r, &CancelArgs { id: 2 }).unwrap(),
        0
    );
    assert_eq!(
        data.lock().unwrap().requests[1].state,
        ReviewRequestState::Withdrawn
    );
    // Cancelling again reports what it is; an unknown number is not found.
    assert_eq!(
        review_request::cancel(&r, &CancelArgs { id: 2 }).unwrap(),
        0
    );
    let e = review_request::cancel(&r, &CancelArgs { id: 77 }).unwrap_err();
    assert_eq!(e.kind(), ErrorKind::NotFound, "{e}");
}

/// A server from before ADR 0086 has no such endpoint: the CLI says so
/// instead of a bare 404.
#[test]
fn an_older_server_is_named_as_such() {
    let (url, _) = start(false);
    let env = Env::new(&url);
    let r = env.remote(false);
    for e in [
        review_request::request(&r, &ask("@", &[], None)).unwrap_err(),
        review_request::inbox(&r, &inbox(false, false)).unwrap_err(),
    ] {
        assert!(
            e.to_string().contains("does not take review requests"),
            "{e}"
        );
        assert_eq!(e.exit_code(), 1);
    }
}

fn leep(url: &str, args: &[&str]) -> std::process::Output {
    let dir = tempfile::tempdir().unwrap();
    let mut c = std::process::Command::new(env!("CARGO_BIN_EXE_leep"));
    for (k, _) in std::env::vars() {
        if k.starts_with("LEEP_") {
            c.env_remove(k);
        }
    }
    c.current_dir(dir.path())
        .env("LEEP_CACHE_DIR", dir.path().join("cache"))
        .env("LEEP_TOKEN", "tok-ada")
        .env("NO_COLOR", "1")
        .args(["--server", url])
        .args(args)
        .output()
        .unwrap()
}

/// The inbox is what still waits for you: a request you approved leaves it
/// (until you withdraw), `--all` brings it back marked, and the requester's
/// view names who approved.
#[test]
fn the_inbox_hides_what_you_already_answered() {
    let (url, data) = start(true);
    let env = Env::new(&url);
    let r = env.remote(false);
    let one = id(1).to_string();
    let two = id(2).to_string();
    review_request::request(&r, &ask(&one, &["ada"], None)).unwrap();
    review_request::request(&r, &ask(&two, &["ada"], None)).unwrap();
    data.lock()
        .unwrap()
        .approved
        .insert(1, vec!["ada".into(), "rhea".into()]);

    let text = |o: std::process::Output| {
        assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
        String::from_utf8(o.stdout).unwrap()
    };
    let inbox = text(leep(&url, &["review", "inbox"]));
    assert!(inbox.starts_with("#2 open"), "{inbox}");
    assert!(!inbox.contains("#1 "), "{inbox}");
    assert!(
        inbox.ends_with(
            "(1 request you already answered not shown; `leep review inbox --all` shows them)\n"
        ),
        "{inbox}"
    );
    let json: ReviewRequestsResponse =
        serde_json::from_str(&text(leep(&url, &["--json", "review", "inbox"]))).unwrap();
    assert_eq!(json.requests.len(), 1);

    let all = text(leep(&url, &["review", "inbox", "--all"]));
    assert!(
        all.contains("#1 open") && all.contains("approved by you, rhea"),
        "{all}"
    );
    let mine = text(leep(&url, &["review", "inbox", "--mine"]));
    // The requester sees the same answers; here she is a reviewer too.
    assert!(mine.contains("approved by you, rhea"), "{mine}");
    assert!(mine.contains("#2 open"), "{mine}");

    // Everything answered: the empty inbox still says how to see them.
    data.lock().unwrap().approved.insert(2, vec!["ada".into()]);
    let inbox = text(leep(&url, &["review", "inbox"]));
    assert!(
        inbox.starts_with("No review requests are waiting for you.\n(2 requests you already"),
        "{inbox}"
    );
}

/// `review request --wait` asks, then waits as `review status --wait`.
#[test]
fn request_wait_delegates_to_status_wait() {
    let (url, _data) = start(true);
    let out = leep(&url, &["review", "request", &id(1).to_string(), "--wait=1"]);
    assert_eq!(out.status.code(), Some(0));
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.starts_with("Requested review of"), "{stdout}");
    assert!(!stdout.contains("waits for the verdict"), "{stdout}");
    assert!(stdout.contains(": approved\n"), "{stdout}");
}
