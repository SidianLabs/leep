use std::collections::BTreeMap;
use std::sync::Arc;

use axum::body::Body;
use axum::http::{HeaderMap, Method, Request, StatusCode};
use axum::Router;
use http_body_util::BodyExt;
use leep_core::api::*;
use leep_core::memory::{MemoryBlobStore, MemoryMetaStore};
use leep_core::{Changeset, ChangesetId, FileChange, FileKind, Hash};
use leep_derive::Repo;
use serde::de::DeserializeOwned;
use serde::Serialize;
use tower::ServiceExt;

struct Client(Router);

struct Reply {
    status: StatusCode,
    headers: HeaderMap,
    body: Vec<u8>,
}

impl Reply {
    fn json<T: DeserializeOwned>(&self) -> T {
        serde_json::from_slice(&self.body)
            .unwrap_or_else(|e| panic!("{e}: {}", String::from_utf8_lossy(&self.body)))
    }
}

impl Client {
    fn new() -> Self {
        let repo = Repo::new(
            Arc::new(MemoryBlobStore::new()),
            Arc::new(MemoryMetaStore::new()),
        );
        Client(leep_server::router(repo))
    }

    async fn raw(&self, method: Method, uri: &str, body: Vec<u8>) -> Reply {
        let req = Request::builder()
            .method(method)
            .uri(uri)
            .body(Body::from(body))
            .unwrap();
        let resp = self.0.clone().oneshot(req).await.unwrap();
        let (parts, body) = resp.into_parts();
        Reply {
            status: parts.status,
            headers: parts.headers,
            body: body.collect().await.unwrap().to_bytes().to_vec(),
        }
    }

    async fn get(&self, uri: &str) -> Reply {
        self.raw(Method::GET, uri, vec![]).await
    }

    async fn post<T: Serialize>(&self, uri: &str, body: &T) -> Reply {
        self.raw(Method::POST, uri, serde_json::to_vec(body).unwrap())
            .await
    }

    async fn put_blob(&self, data: &[u8]) -> Hash {
        let h = Hash::of_blob(data);
        let r = self
            .raw(Method::PUT, &format!("/v1/blobs/{h}"), data.to_vec())
            .await;
        assert_eq!(r.status, StatusCode::NO_CONTENT);
        h
    }

    /// Commit `files` (path, content; `None` deletes) on `parents`.
    async fn commit(
        &self,
        parents: &[ChangesetId],
        files: &[(&str, Option<&str>)],
        msg: &str,
    ) -> ChangesetId {
        let r = self.try_commit(parents, files, msg).await;
        assert_eq!(
            r.status,
            StatusCode::OK,
            "{}",
            String::from_utf8_lossy(&r.body)
        );
        r.json::<CreateChangesetResponse>().id
    }

    async fn try_commit(
        &self,
        parents: &[ChangesetId],
        files: &[(&str, Option<&str>)],
        msg: &str,
    ) -> Reply {
        let mut changes = BTreeMap::new();
        for (p, c) in files {
            let ch = match c {
                Some(t) => FileChange::Modify {
                    content: self.put_blob(t.as_bytes()).await,
                    kind: FileKind::Regular,
                    size: t.len() as u64,
                },
                None => FileChange::Delete,
            };
            changes.insert(leep_core::RepoPath::new(*p).unwrap(), ch);
        }
        let cs = Changeset {
            parents: parents.to_vec(),
            author: "t".into(),
            timestamp: 1,
            message: msg.into(),
            changes,
            extra: BTreeMap::new(),
        };
        self.post("/v1/changesets", &cs).await
    }

    async fn set_ref(&self, name: &str, id: ChangesetId) {
        let r = self
            .post(
                &format!("/v1/refs/{name}"),
                &CasRefRequest {
                    expected: None,
                    new: Some(id),
                },
            )
            .await;
        assert_eq!(r.status, StatusCode::OK);
    }
}

fn merge_req(a: ChangesetId, b: ChangesetId) -> MergeRequest {
    MergeRequest {
        parents: [a, b],
        message: "merge".into(),
        authorship: Authorship {
            author: "m".into(),
            timestamp: Some(5),
        },
        extra: BTreeMap::new(),
    }
}

#[tokio::test]
async fn health() {
    let c = Client::new();
    let r = c.get("/v1/health").await;
    assert_eq!(r.status, StatusCode::OK);
    assert_eq!(r.json::<String>(), "ok");
}

#[tokio::test]
async fn blobs_put_get_missing() {
    let c = Client::new();
    let h = c.put_blob(b"hello").await;
    let r = c.get(&format!("/v1/blobs/{h}")).await;
    assert_eq!(r.status, StatusCode::OK);
    assert_eq!(r.body, b"hello");

    let absent = Hash::of_blob(b"absent");
    let r = c.get(&format!("/v1/blobs/{absent}")).await;
    assert_eq!(r.status, StatusCode::NOT_FOUND);
    assert!(!r.json::<ErrorResponse>().error.is_empty());

    let r = c
        .post(
            "/v1/blobs/missing",
            &MissingBlobsRequest {
                hashes: vec![h, absent],
            },
        )
        .await;
    assert_eq!(r.json::<MissingBlobsResponse>().missing, vec![absent]);
}

#[tokio::test]
async fn blob_errors() {
    let c = Client::new();
    let wrong = Hash::of_blob(b"other");
    let r = c
        .raw(
            Method::PUT,
            &format!("/v1/blobs/{wrong}"),
            b"hello".to_vec(),
        )
        .await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);
    assert_eq!(c.get("/v1/blobs/zz").await.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        c.get(&format!("/v1/blobs/{wrong}")).await.status,
        StatusCode::NOT_FOUND
    );
}

#[tokio::test]
async fn body_limit() {
    let repo = Repo::new(
        Arc::new(MemoryBlobStore::new()),
        Arc::new(MemoryMetaStore::new()),
    );
    let c = Client(leep_server::router_with_limit(repo, 4));
    let h = Hash::of_blob(b"too big");
    let r = c
        .raw(Method::PUT, &format!("/v1/blobs/{h}"), b"too big".to_vec())
        .await;
    assert_eq!(r.status, StatusCode::PAYLOAD_TOO_LARGE);
}

#[tokio::test]
async fn changesets() {
    let c = Client::new();
    let a = c.commit(&[], &[("a.txt", Some("one\n"))], "first").await;
    let r = c.get(&format!("/v1/changesets/{a}")).await;
    assert_eq!(r.status, StatusCode::OK);
    let info: ChangesetInfo = r.json();
    assert_eq!(info.id, a);
    assert_eq!(info.generation, 1);
    assert_eq!(info.changeset.message, "first");

    // Idempotent.
    assert_eq!(c.commit(&[], &[("a.txt", Some("one\n"))], "first").await, a);

    let missing = ChangesetId(Hash::of_blob(b"nope"));
    assert_eq!(
        c.get(&format!("/v1/changesets/{missing}")).await.status,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        c.get("/v1/changesets/xyz").await.status,
        StatusCode::BAD_REQUEST
    );
}

#[tokio::test]
async fn create_changeset_invalid() {
    let c = Client::new();
    let unknown = ChangesetId(Hash::of_blob(b"nope"));
    let r = c.try_commit(&[unknown], &[("a", Some("x"))], "m").await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);

    // Blob never uploaded.
    let mut changes = BTreeMap::new();
    changes.insert(
        leep_core::RepoPath::new("a").unwrap(),
        FileChange::Modify {
            content: Hash::of_blob(b"never uploaded"),
            kind: FileKind::Regular,
            size: 14,
        },
    );
    let cs = Changeset {
        parents: vec![],
        author: "t".into(),
        timestamp: 1,
        message: "m".into(),
        changes,
        extra: BTreeMap::new(),
    };
    assert_eq!(
        c.post("/v1/changesets", &cs).await.status,
        StatusCode::BAD_REQUEST
    );

    let r = c
        .raw(Method::POST, "/v1/changesets", b"{not json".to_vec())
        .await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);
    assert!(!r.json::<ErrorResponse>().error.is_empty());
}

#[tokio::test]
async fn refs_cas_and_resolve() {
    let c = Client::new();
    let a = c.commit(&[], &[("a", Some("1"))], "a").await;
    let b = c.commit(&[a], &[("a", Some("2"))], "b").await;

    let r = c.get("/v1/refs/main").await;
    assert_eq!(r.status, StatusCode::OK);
    assert_eq!(r.json::<RefResponse>().target, None);

    let ok = c
        .post(
            "/v1/refs/main",
            &CasRefRequest {
                expected: None,
                new: Some(a),
            },
        )
        .await;
    assert_eq!(ok.status, StatusCode::OK);
    assert_eq!(
        ok.json::<CasRefResponse>(),
        CasRefResponse {
            ok: true,
            current: Some(a)
        }
    );

    let lost = c
        .post(
            "/v1/refs/main",
            &CasRefRequest {
                expected: None,
                new: Some(b),
            },
        )
        .await;
    assert_eq!(lost.status, StatusCode::CONFLICT);
    assert_eq!(
        lost.json::<CasRefResponse>(),
        CasRefResponse {
            ok: false,
            current: Some(a)
        }
    );

    c.set_ref("feature/x", b).await;
    let refs: RefsResponse = c.get("/v1/refs").await.json();
    assert_eq!(refs.refs.len(), 2);
    assert_eq!(refs.refs["main"], a);
    assert_eq!(refs.refs["feature/x"], b);
    assert_eq!(
        c.get("/v1/refs/feature/x")
            .await
            .json::<RefResponse>()
            .target,
        Some(b)
    );

    let r = c.get("/v1/resolve/main").await;
    assert_eq!(r.json::<ResolveResponse>().id, a);
    let r = c.get(&format!("/v1/resolve/{b}")).await;
    assert_eq!(r.json::<ResolveResponse>().id, b);
    assert_eq!(
        c.get("/v1/resolve/nope").await.status,
        StatusCode::NOT_FOUND
    );
}

#[tokio::test]
async fn tree_files_file() {
    let c = Client::new();
    let a = c
        .commit(
            &[],
            &[
                ("README", Some("hi")),
                ("src/a.rs", Some("fn a(){}")),
                ("src/deep/b.rs", Some("b")),
            ],
            "init",
        )
        .await;

    let t: TreeResponse = c.get(&format!("/v1/tree/{a}")).await.json();
    assert_eq!(t.entries.keys().collect::<Vec<_>>(), ["README", "src"]);
    let t: TreeResponse = c.get(&format!("/v1/tree/{a}?path=src")).await.json();
    assert_eq!(t.path, "src");
    assert_eq!(t.entries.keys().collect::<Vec<_>>(), ["a.rs", "deep"]);
    assert_eq!(
        c.get(&format!("/v1/tree/{a}?path=nope")).await.status,
        StatusCode::NOT_FOUND
    );

    let all: FilesResponse = c.get(&format!("/v1/files/{a}")).await.json();
    assert_eq!(all.files.len(), 3);
    let sub: FilesResponse = c.get(&format!("/v1/files/{a}?prefix=src")).await.json();
    let paths: Vec<_> = sub.files.iter().map(|f| f.path.as_str()).collect();
    assert_eq!(paths, ["src/a.rs", "src/deep/b.rs"]);

    let r = c.get(&format!("/v1/file/{a}?path=src/a.rs")).await;
    assert_eq!(r.status, StatusCode::OK);
    assert_eq!(r.body, b"fn a(){}");
    assert_eq!(r.headers["x-leep-kind"], "regular");
    assert_eq!(r.headers["content-type"], "application/octet-stream");
    assert_eq!(
        c.get(&format!("/v1/file/{a}?path=missing")).await.status,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        c.get(&format!("/v1/file/{a}?path=../x")).await.status,
        StatusCode::BAD_REQUEST
    );
}

#[tokio::test]
async fn file_kind_header() {
    let c = Client::new();
    let h = c.put_blob(b"#!/bin/sh\n").await;
    let mut changes = BTreeMap::new();
    changes.insert(
        leep_core::RepoPath::new("run.sh").unwrap(),
        FileChange::Modify {
            content: h,
            kind: FileKind::Executable,
            size: 10,
        },
    );
    let cs = Changeset {
        parents: vec![],
        author: "t".into(),
        timestamp: 1,
        message: "x".into(),
        changes,
        extra: BTreeMap::new(),
    };
    let id = c
        .post("/v1/changesets", &cs)
        .await
        .json::<CreateChangesetResponse>()
        .id;
    let r = c.get(&format!("/v1/file/{id}?path=run.sh")).await;
    assert_eq!(r.headers["x-leep-kind"], "executable");
}

#[tokio::test]
async fn log_diff_blame_merge_base() {
    let c = Client::new();
    let a = c
        .commit(&[], &[("f", Some("l1\n")), ("g", Some("g\n"))], "a")
        .await;
    let b = c.commit(&[a], &[("f", Some("l1\nl2\n"))], "b").await;
    let d = c.commit(&[b], &[("g", Some("g2\n"))], "d").await;

    let l: LogResponse = c.get(&format!("/v1/log/{d}")).await.json();
    let ids: Vec<_> = l.entries.iter().map(|e| e.id).collect();
    assert_eq!(ids, [d, b, a]);
    let l: LogResponse = c.get(&format!("/v1/log/{d}?limit=1")).await.json();
    assert_eq!(l.entries.len(), 1);
    let l: LogResponse = c.get(&format!("/v1/log/{d}?path=f")).await.json();
    let ids: Vec<_> = l.entries.iter().map(|e| e.id).collect();
    assert_eq!(ids, [b, a]);

    let df: DiffResponse = c.get(&format!("/v1/diff?from={a}&to={d}")).await.json();
    let paths: Vec<_> = df.entries.iter().map(|e| e.path.as_str()).collect();
    assert_eq!(paths, ["f", "g"]);
    let df: DiffResponse = c.get(&format!("/v1/diff?to={a}")).await.json();
    assert_eq!(df.entries.len(), 2);
    assert!(df
        .entries
        .iter()
        .all(|e| e.old.is_none() && e.new.is_some()));
    let df: DiffResponse = c
        .get(&format!("/v1/diff?from={a}&to={d}&prefix=g"))
        .await
        .json();
    assert_eq!(df.entries.len(), 1);
    assert_eq!(
        c.get("/v1/diff?from=zz&to=zz").await.status,
        StatusCode::BAD_REQUEST
    );

    let bl: BlameResponse = c.get(&format!("/v1/blame/{d}?path=f")).await.json();
    assert_eq!(bl.lines.len(), 2);
    assert_eq!(bl.lines[0].changeset, a);
    assert_eq!(bl.lines[1].changeset, b);
    assert_eq!(bl.lines[1].text.trim_end(), "l2");

    let side = c.commit(&[a], &[("h", Some("h"))], "side").await;
    let mb: MergeBaseResponse = c
        .get(&format!("/v1/merge-base?a={d}&b={side}"))
        .await
        .json();
    assert_eq!(mb.base, Some(a));
    assert_eq!(
        c.get(&format!("/v1/merge-base?a={d}")).await.status,
        StatusCode::BAD_REQUEST
    );
}

#[tokio::test]
async fn merge_clean_and_conflicts() {
    let c = Client::new();
    let base = c
        .commit(&[], &[("f", Some("a\nb\nc\n")), ("g", Some("g\n"))], "base")
        .await;
    let ours = c.commit(&[base], &[("g", Some("g-ours\n"))], "ours").await;
    let theirs = c.commit(&[base], &[("h", Some("h\n"))], "theirs").await;

    let r = c.post("/v1/merge", &merge_req(ours, theirs)).await;
    assert_eq!(r.status, StatusCode::OK);
    let MergeResponse::Merged { id } = r.json() else {
        panic!("expected clean merge")
    };
    let files: FilesResponse = c.get(&format!("/v1/files/{id}")).await.json();
    assert_eq!(files.files.len(), 3);

    let x = c.commit(&[base], &[("g", Some("g-x\n"))], "x").await;
    let r = c.post("/v1/merge", &merge_req(ours, x)).await;
    assert_eq!(r.status, StatusCode::OK);
    let body: serde_json::Value = r.json();
    assert_eq!(body["status"], "conflicts");
    let MergeResponse::Conflicts { conflicts } = serde_json::from_value(body).unwrap() else {
        panic!()
    };
    assert_eq!(conflicts[0].path.as_str(), "g");

    let unknown = ChangesetId(Hash::of_blob(b"nope"));
    let r = c.post("/v1/merge", &merge_req(ours, unknown)).await;
    assert_eq!(r.status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn rebase_and_land() {
    let c = Client::new();
    let base = c.commit(&[], &[("a", Some("a\n"))], "base").await;
    c.set_ref("main", base).await;
    let m1 = c.commit(&[base], &[("m", Some("m\n"))], "main1").await;
    let feat = c.commit(&[base], &[("f", Some("f\n"))], "feat").await;

    let r = c
        .post(
            "/v1/rebase",
            &RebaseRequest {
                changeset: feat,
                onto: m1,
            },
        )
        .await;
    assert_eq!(r.status, StatusCode::OK);
    let RebaseResponse::Rebased { id } = r.json() else {
        panic!()
    };
    assert_ne!(id, feat);
    let info: ChangesetInfo = c.get(&format!("/v1/changesets/{id}")).await.json();
    assert_eq!(info.changeset.parents, vec![m1]);
    assert!(info.changeset.extra.contains_key("leep.rebased_from"));

    // Rebasing onto its own parent is a no-op.
    let r = c
        .post(
            "/v1/rebase",
            &RebaseRequest {
                changeset: feat,
                onto: base,
            },
        )
        .await;
    assert!(matches!(r.json(), RebaseResponse::Rebased { id } if id == feat));

    // Conflicting rebase.
    let k1 = c.commit(&[base], &[("a", Some("one\n"))], "k1").await;
    let k2 = c.commit(&[base], &[("a", Some("two\n"))], "k2").await;
    let r = c
        .post(
            "/v1/rebase",
            &RebaseRequest {
                changeset: k1,
                onto: k2,
            },
        )
        .await;
    assert!(matches!(r.json(), RebaseResponse::Conflicts { .. }));

    // Fast-forward land; target_ref defaults to main.
    let r = c
        .post(
            "/v1/land",
            &serde_json::json!({ "changeset": m1.to_string() }),
        )
        .await;
    assert_eq!(r.status, StatusCode::OK);
    assert!(matches!(
        r.json(),
        LandResponse::Landed { landed, rebased: false, .. } if landed == m1
    ));

    // Land on a moved ref: rebased.
    let r = c
        .post(
            "/v1/land",
            &LandRequest {
                changeset: feat,
                target_ref: "main".into(),
            },
        )
        .await;
    let LandResponse::Landed {
        landed, rebased, ..
    } = r.json()
    else {
        panic!()
    };
    assert!(rebased);
    assert_ne!(landed, feat);
    let head: RefResponse = c.get("/v1/refs/main").await.json();
    assert_eq!(head.target, Some(landed));

    // Land with conflicts.
    let r = c
        .post(
            "/v1/land",
            &LandRequest {
                changeset: k1,
                target_ref: "side".into(),
            },
        )
        .await;
    assert!(matches!(r.json(), LandResponse::Landed { .. }));
    let r = c
        .post(
            "/v1/land",
            &LandRequest {
                changeset: k2,
                target_ref: "side".into(),
            },
        )
        .await;
    assert!(matches!(r.json(), LandResponse::Conflicts { .. }));

    let unknown = ChangesetId(Hash::of_blob(b"nope"));
    let r = c
        .post(
            "/v1/land",
            &LandRequest {
                changeset: unknown,
                target_ref: "main".into(),
            },
        )
        .await;
    assert_eq!(r.status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn real_tcp_listener() {
    let repo = Repo::new(
        Arc::new(MemoryBlobStore::new()),
        Arc::new(MemoryMetaStore::new()),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let server = tokio::spawn(leep_server::serve(
        listener,
        leep_server::router(repo),
        async {
            let _ = rx.await;
        },
    ));

    let http = reqwest::Client::new();
    let r = http
        .get(format!("http://{addr}/v1/health"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    assert_eq!(r.text().await.unwrap(), "\"ok\"");
    let h = Hash::of_blob(b"over tcp");
    let r = http
        .put(format!("http://{addr}/v1/blobs/{h}"))
        .body("over tcp")
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 204);
    let r = http
        .get(format!("http://{addr}/v1/blobs/{h}"))
        .send()
        .await
        .unwrap();
    assert_eq!(r.bytes().await.unwrap().as_ref(), b"over tcp");

    tx.send(()).unwrap();
    server.await.unwrap().unwrap();
}
