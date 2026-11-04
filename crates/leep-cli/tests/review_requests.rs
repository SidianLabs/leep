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
            .route("/v1/review-requests/{id}/withdraw", post(withdraw));
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
        .map(entry)
        .collect();
    Json(ReviewRequestsResponse {
        requests,
        next: None,
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
