//! API request bodies, and the real router driven in-process.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Method, Request};
use leep_core::api::*;
use leep_core::memory::{MemoryBlobStore, MemoryMetaStore};
use leep_core::{Changeset, MetaStore};
use leep_derive::Repo;
use serde::de::DeserializeOwned;
use serde::Serialize;
use tower::ServiceExt;

/// `data` as JSON of type `T`: parsing never panics, and what parses
/// serializes to JSON that parses to the same value.
fn json_roundtrip<T: Serialize + DeserializeOwned>(data: &[u8]) {
    let Ok(v) = serde_json::from_slice::<T>(data) else {
        return;
    };
    let first = serde_json::to_value(&v).expect("a parsed value serializes");
    let again: T = serde_json::from_value(first.clone()).expect("own output must parse");
    assert_eq!(serde_json::to_value(&again).unwrap(), first);
}

macro_rules! json_types {
    ($($t:ident),* $(,)?) => {
        /// The JSON types [`api_json`] parses, by name. The order is part of
        /// the input format: the first input byte indexes this list.
        pub(crate) const JSON_TYPES: &[(&str, fn(&[u8]))] =
            &[$((stringify!($t), json_roundtrip::<$t>)),*];
    };
}

json_types! {
    // Request bodies.
    MissingBlobsRequest,
    Changeset,
    AddSignatureRequest,
    AddReviewRequest,
    CasRefRequest,
    MergeRequest,
    RebaseRequest,
    LandRequest,
    // Responses, which the CLI and MCP server parse from the network.
    ErrorResponse,
    MissingBlobsResponse,
    ChangesetInfo,
    SignaturesResponse,
    ReviewsResponse,
    ReviewStatusResponse,
    RefsResponse,
    CasRefResponse,
    RefLogResponse,
    TreeResponse,
    FilesResponse,
    LogResponse,
    DiffResponse,
    BlameResponse,
    ProvenanceResponse,
    MergeResponse,
    RebaseResponse,
    LandResponse,
    SearchResponse,
}

/// The first byte picks the type; the rest is the body.
pub fn api_json(data: &[u8]) {
    if let Some((&k, body)) = data.split_first() {
        JSON_TYPES[k as usize % JSON_TYPES.len()].1(body);
    }
}

/// A small fixed history the router tests run against: two changesets on
/// `main`, one tag, a few files.
pub(crate) struct Fixture {
    pub(crate) blobs: Vec<&'static [u8]>,
    pub(crate) root: Changeset,
    pub(crate) tip: Changeset,
}

pub(crate) fn fixture() -> Fixture {
    use leep_core::{FileChange, FileKind, Hash, RepoPath};
    use std::collections::BTreeMap;
    let file = |body: &[u8]| FileChange::Modify {
        content: Hash::of_blob(body),
        kind: FileKind::Regular,
        size: body.len() as u64,
    };
    const A1: &[u8] = b"hello\nworld\n";
    const A2: &[u8] = b"hello\nbrave world\n";
    const LIB: &[u8] = b"// helper\nfn helper() -> u32 { 1 }\n";
    const OWNERS: &[u8] = b"alice\n# comment\n";
    let path = |p: &str| RepoPath::new(p).unwrap();
    let root = Changeset {
        parents: vec![],
        author: "Ada <ada@example.com>".into(),
        timestamp: 1_760_000_000,
        message: "first".into(),
        changes: BTreeMap::from([
            (path("a.txt"), file(A1)),
            (path("src/lib.rs"), file(LIB)),
            (path("OWNERS"), file(OWNERS)),
        ]),
        extra: BTreeMap::from([("agent.model".into(), "m".into())]),
    };
    let tip = Changeset {
        parents: vec![root.id()],
        author: "Bob <bob@example.com>".into(),
        timestamp: 1_760_000_100,
        message: "second".into(),
        changes: BTreeMap::from([
            (path("a.txt"), file(A2)),
            (path("src/renamed.rs"), file(LIB)),
            (path("src/lib.rs"), FileChange::Delete),
        ]),
        extra: BTreeMap::new(),
    };
    Fixture {
        blobs: vec![A1, A2, LIB, OWNERS],
        root,
        tip,
    }
}

async fn seeded_repo() -> Repo {
    let f = fixture();
    let meta = Arc::new(MemoryMetaStore::new());
    let repo = Repo::new(Arc::new(MemoryBlobStore::new()), meta.clone());
    for b in &f.blobs {
        repo.put_blob(bytes::Bytes::from_static(b)).await.unwrap();
    }
    let root = repo.create_changeset(f.root).await.unwrap();
    let tip = repo.create_changeset(f.tip).await.unwrap();
    meta.cas_ref("main", None, Some(tip)).await.unwrap();
    meta.cas_ref("tags/v1", None, Some(root)).await.unwrap();
    repo
}

/// One route the fuzzer can pick: method, a path template whose `{}` holes are
/// filled from the input, and a default for each hole.
struct Route {
    method: &'static str,
    template: &'static str,
    defaults: &'static [&'static str],
}

const fn route(
    method: &'static str,
    template: &'static str,
    defaults: &'static [&'static str],
) -> Route {
    Route {
        method,
        template,
        defaults,
    }
}

/// `$tip`, `$root` and `$blob` stand for ids that exist in the fixture.
fn routes() -> Vec<Route> {
    let (get, post, put) = ("GET", "POST", "PUT");
    vec![
        route(get, "/v1/health", &[]),
        route(post, "/v1/blobs/missing", &[]),
        route(get, "/v1/blobs/{}", &["$blob"]),
        route(put, "/v1/blobs/{}", &["$blob"]),
        route(post, "/v1/changesets", &[]),
        route(get, "/v1/changesets/{}", &["$tip"]),
        route(get, "/v1/changesets/{}/signatures", &["$tip"]),
        route(post, "/v1/changesets/{}/signatures", &["$tip"]),
        route(get, "/v1/changesets/{}/reviews", &["$tip"]),
        route(post, "/v1/changesets/{}/reviews", &["$tip"]),
        route(get, "/v1/changesets/{}/review-status", &["$tip"]),
        route(get, "/v1/refs", &[]),
        route(get, "/v1/refs/{}", &["main"]),
        route(post, "/v1/refs/{}", &["feature"]),
        route(get, "/v1/reflog?before={}&limit={}", &["10", "5"]),
        route(get, "/v1/reflog/{}", &["main"]),
        route(get, "/v1/resolve/{}", &["main"]),
        route(get, "/v1/tree/{}?path={}", &["$tip", "src"]),
        route(get, "/v1/files/{}?prefix={}", &["$tip", "src"]),
        route(get, "/v1/file/{}?path={}", &["$tip", "a.txt"]),
        route(
            get,
            "/v1/log/{}?path={}&limit={}&follow={}&author={}&extra={}",
            &[
                "$tip",
                "src/renamed.rs",
                "5",
                "true",
                "bob",
                "agent.model:m",
            ],
        ),
        route(get, "/v1/last-change/{}?path={}", &["$tip", "a.txt"]),
        route(
            get,
            "/v1/diff?from={}&to={}&prefix={}&renames={}&rename_threshold={}",
            &["$root", "$tip", "src", "true", "50"],
        ),
        route(get, "/v1/blame/{}?path={}", &["$tip", "a.txt"]),
        route(
            get,
            "/v1/provenance/{}?path={}&limit={}",
            &["$tip", "a.txt", "10"],
        ),
        route(
            get,
            "/v1/search/{}?q={}&regex={}&i={}&path={}&glob={}&limit={}",
            &["main", "world", "false", "true", "", "**/*.txt", "5"],
        ),
        route(get, "/v1/merge-base?a={}&b={}", &["$root", "$tip"]),
        route(post, "/v1/merge", &[]),
        route(post, "/v1/rebase", &[]),
        route(post, "/v1/land", &[]),
    ]
}

/// Index of the route with this method and template, for seed generation.
pub(crate) fn route_index(method: &str, template: &str) -> usize {
    routes()
        .iter()
        .position(|r| r.method == method && r.template == template)
        .unwrap_or_else(|| panic!("no route {method} {template}"))
}

fn percent_encode(piece: &[u8]) -> String {
    let mut out = String::new();
    for &b in piece {
        if b.is_ascii_alphanumeric() || b"-._~/".contains(&b) {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

/// Turn the input into a request.
///
/// - Byte 0 below 128 picks a route. Up to the first `0xFF` come the values
///   for its `{}` holes, separated by `0x00` (a missing or empty value takes
///   the default; one byte below 16 names a fixture id). After the `0xFF` is
///   the body.
/// - Byte 0 of 128 or more is raw: the method comes from its low bits, the
///   URI is the text up to the first `0xFF`, the body is the rest.
fn request(data: &[u8], ids: &[String; 3]) -> Option<Request<Body>> {
    let (&sel, rest) = data.split_first()?;
    let (head, body) = match rest.iter().position(|&b| b == 0xFF) {
        Some(i) => (&rest[..i], &rest[i + 1..]),
        None => (rest, &[][..]),
    };
    let (method, uri) = if sel >= 128 {
        let methods = [
            Method::GET,
            Method::POST,
            Method::PUT,
            Method::DELETE,
            Method::PATCH,
            Method::HEAD,
        ];
        let uri = String::from_utf8_lossy(head).into_owned();
        (methods[(sel & 0x7F) as usize % methods.len()].clone(), uri)
    } else {
        let routes = routes();
        let r = &routes[sel as usize % routes.len()];
        let mut pieces = head.split(|&b| b == 0);
        let mut uri = String::new();
        let mut holes = 0;
        let mut tpl = r.template;
        while let Some(at) = tpl.find("{}") {
            uri.push_str(&tpl[..at]);
            tpl = &tpl[at + 2..];
            let piece = pieces.next().unwrap_or(&[]);
            let default = r.defaults[holes];
            holes += 1;
            match piece {
                [] => uri.push_str(match default {
                    "$tip" => &ids[0],
                    "$root" => &ids[1],
                    "$blob" => &ids[2],
                    other => other,
                }),
                [n] if *n < 16 => uri.push_str(&ids[*n as usize % ids.len()]),
                other => uri.push_str(&percent_encode(other)),
            }
        }
        uri.push_str(tpl);
        (Method::from_bytes(r.method.as_bytes()).unwrap(), uri)
    };
    Request::builder()
        .method(method)
        .uri(uri)
        .body(Body::from(body.to_vec()))
        .ok()
}

thread_local! {
    static RUNTIME: tokio::runtime::Runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
}

/// Drive the real router with a fresh in-memory repo. Malformed input is the
/// client's fault: the answer must be below 500.
pub fn server_router(data: &[u8]) {
    let f = fixture();
    let ids = [
        f.tip.id().to_string(),
        f.root.id().to_string(),
        leep_core::Hash::of_blob(f.blobs[0]).to_string(),
    ];
    let Some(req) = request(data, &ids) else {
        return;
    };
    let uri = req.uri().to_string();
    let method = req.method().clone();
    RUNTIME.with(|rt| {
        rt.block_on(async {
            let app = leep_server::router(seeded_repo().await);
            let resp = app.oneshot(req).await.expect("the router is infallible");
            let status = resp.status();
            assert!(
                status.as_u16() < 500,
                "{method} {uri} answered {status}; malformed input must be a 4xx"
            );
        })
    });
}
