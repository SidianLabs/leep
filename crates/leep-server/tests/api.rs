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

    /// The `code` of an error body, which every error must have.
    fn code(&self) -> String {
        self.json::<ErrorResponse>()
            .code
            .unwrap_or_else(|| panic!("no code: {}", String::from_utf8_lossy(&self.body)))
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
                    ..Default::default()
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
async fn missing_blobs_caps_the_list() {
    let c = Client::new();
    let hashes: Vec<Hash> = (0..=MAX_MISSING_BLOBS)
        .map(|i| Hash::of_blob(&i.to_le_bytes()))
        .collect();
    let r = c
        .post(
            "/v1/blobs/missing",
            &MissingBlobsRequest {
                hashes: hashes.clone(),
            },
        )
        .await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);
    assert!(r.json::<ErrorResponse>().error.contains("at most"));
    let r = c
        .post(
            "/v1/blobs/missing",
            &MissingBlobsRequest {
                hashes: hashes[1..].to_vec(),
            },
        )
        .await;
    assert_eq!(r.status, StatusCode::OK);
    let missing = r.json::<MissingBlobsResponse>().missing;
    assert_eq!(missing.len(), MAX_MISSING_BLOBS);
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

/// A blob whose bytes are a tree's hash preimage has the tree's address. If
/// it were stored first, the tree would read back as corrupt forever
/// (ADR 0041), so the upload is refused.
#[tokio::test]
async fn blob_cannot_take_a_tree_address() {
    let c = Client::new();
    let empty = leep_core::TreeNode::default();
    let mut preimage = format!("leep:{}", leep_core::TreeNode::KIND).into_bytes();
    preimage.push(0);
    preimage.extend_from_slice(&empty.encode());
    let h = Hash::of_blob(&preimage);
    assert_eq!(h, leep_core::TreeNode::empty_hash());
    let r = c
        .raw(Method::PUT, &format!("/v1/blobs/{h}"), preimage)
        .await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);
    assert!(r.json::<ErrorResponse>().error.contains("ADR 0041"));

    // The tree itself is still stored and read normally.
    let a = c.commit(&[], &[("a.txt", Some("one\n"))], "add").await;
    let b = c.commit(&[a], &[("a.txt", None)], "remove").await;
    let r = c.get(&format!("/v1/tree/{b}")).await;
    assert_eq!(
        r.status,
        StatusCode::OK,
        "{}",
        String::from_utf8_lossy(&r.body)
    );
    assert!(r.json::<TreeResponse>().entries.is_empty());

    // A text file that merely starts with the prefix is an ordinary blob.
    c.put_blob(b"leep: true\n").await;
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
    // The body cap answers in plain text; clients still get a JSON error.
    assert_eq!(r.code(), "too_large");
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
                ..Default::default()
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
                ..Default::default()
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
async fn last_change_and_indexed_path_log() {
    let c = Client::new();
    let a = c
        .commit(&[], &[("src/f", Some("1")), ("doc/g", Some("1"))], "a")
        .await;
    let b = c.commit(&[a], &[("src/f", Some("2"))], "b").await;
    let d = c.commit(&[b], &[("doc/g", None)], "d").await;

    let get = |c: &Client, p: &str| {
        let url = format!("/v1/last-change/{d}?path={p}");
        let c = Client(c.0.clone());
        async move { c.get(&url).await }
    };
    let r: LastChangeResponse = get(&c, "src/f").await.json();
    assert_eq!((r.path.as_str(), r.changeset.id), ("src/f", b));
    assert_eq!(r.changeset.generation, 2);
    assert!(r.concurrent.is_empty());
    let r: LastChangeResponse = get(&c, "doc").await.json();
    assert_eq!(r.changeset.id, d);
    let r: LastChangeResponse = get(&c, "doc/g/").await.json();
    assert_eq!((r.path.as_str(), r.changeset.id), ("doc/g", d));
    assert_eq!(get(&c, "nope").await.status, StatusCode::NOT_FOUND);
    assert_eq!(get(&c, "").await.status, StatusCode::BAD_REQUEST);
    assert_eq!(get(&c, "a/../b").await.status, StatusCode::BAD_REQUEST);
    let missing = ChangesetId(Hash::of_blob(b"nope"));
    assert_eq!(
        c.get(&format!("/v1/last-change/{missing}?path=src"))
            .await
            .status,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        c.get("/v1/last-change/zz?path=src").await.status,
        StatusCode::BAD_REQUEST
    );

    // A deleted path still has a log, and filters apply to what it yields.
    let l: LogResponse = c.get(&format!("/v1/log/{d}?path=doc/g")).await.json();
    assert_eq!(l.entries.iter().map(|e| e.id).collect::<Vec<_>>(), [d, a]);
    let l: LogResponse = c.get(&format!("/v1/log/{d}?path=src&limit=1")).await.json();
    assert_eq!(l.entries.iter().map(|e| e.id).collect::<Vec<_>>(), [b]);
}

/// A long log is encoded in chunks and streamed; the body is the same JSON,
/// byte for byte, as encoding the response in one piece.
#[tokio::test]
async fn long_logs_are_the_same_json() {
    let c = Client::new();
    let mut chain = Vec::new();
    for i in 0..1100 {
        let parents: Vec<ChangesetId> = chain.last().copied().into_iter().collect();
        let text = format!("v{i} \"quoted\" \u{e9}\n");
        let path = format!("d{}/f", i % 7);
        let files = [(path.as_str(), Some(text.as_str()))];
        chain.push(c.commit(&parents, &files, &format!("c{i}\n\tbody")).await);
    }
    let tip = *chain.last().unwrap();
    for limit in [1, 512, 513, 1024, 1025, 1100, 10000] {
        let r = c.get(&format!("/v1/log/{tip}?limit={limit}")).await;
        assert_eq!(r.status, StatusCode::OK);
        assert_eq!(r.headers["content-type"], "application/json");
        let l: LogResponse = r.json();
        let ids: Vec<ChangesetId> = l.entries.iter().map(|e| e.id).collect();
        let want: Vec<ChangesetId> = chain.iter().rev().take(limit).copied().collect();
        assert_eq!(ids, want, "limit {limit}");
        assert_eq!(serde_json::to_vec(&l).unwrap(), r.body, "limit {limit}");
    }
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

    let x = c
        .commit(&[base], &[("g", Some("g-x\n")), ("n", Some("n\n"))], "x")
        .await;
    let r = c.post("/v1/merge", &merge_req(ours, x)).await;
    assert_eq!(r.status, StatusCode::OK);
    let body: serde_json::Value = r.json();
    assert_eq!(body["status"], "conflicts");
    let MergeResponse::Conflicts { conflicts, clean } = serde_json::from_value(body).unwrap()
    else {
        panic!()
    };
    assert_eq!(conflicts[0].path.as_str(), "g");
    assert_eq!(
        clean.keys().map(|p| p.as_str()).collect::<Vec<_>>(),
        ["n"],
        "the cleanly merged paths come back with the conflicts"
    );

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
                create: None,
                land_id: None,
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
                create: None,
                land_id: None,
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
                create: None,
                land_id: None,
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
                create: None,
                land_id: None,
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

impl Client {
    /// Commit one file with an explicit author and `extra`.
    async fn commit_as(
        &self,
        parents: &[ChangesetId],
        file: (&str, &str),
        author: &str,
        extra: &[(&str, &str)],
        ts: i64,
    ) -> ChangesetId {
        let cs = Changeset {
            parents: parents.to_vec(),
            author: author.into(),
            timestamp: ts,
            message: format!("edit {}\nbody", file.0),
            changes: BTreeMap::from([(
                leep_core::RepoPath::new(file.0).unwrap(),
                FileChange::Modify {
                    content: self.put_blob(file.1.as_bytes()).await,
                    kind: FileKind::Regular,
                    size: file.1.len() as u64,
                },
            )]),
            extra: extra
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        };
        let r = self.post("/v1/changesets", &cs).await;
        assert_eq!(r.status, StatusCode::OK);
        r.json::<CreateChangesetResponse>().id
    }
}

#[tokio::test]
async fn log_filters_provenance_and_blame_summaries() {
    let c = Client::new();
    let a = c.commit_as(&[], ("f", "l1\n"), "alice", &[], 10).await;
    let b = c
        .commit_as(
            &[a],
            ("f", "l1\nl2\n"),
            "bot",
            &[("agent.model", "m1"), ("agent.session", "7")],
            20,
        )
        .await;
    let d = c
        .commit_as(&[b], ("g", "g\n"), "bot", &[("agent.model", "m2")], 30)
        .await;

    let ids = |l: LogResponse| l.entries.iter().map(|e| e.id).collect::<Vec<_>>();
    let get = |q: String| {
        let c = &c;
        async move { c.get(&format!("/v1/log/{d}?{q}")).await }
    };
    let l: LogResponse = get("extra=agent.model:m1&extra=agent.session:7".into())
        .await
        .json();
    assert_eq!(ids(l), [b]);
    let l: LogResponse = get("extra=agent.model:m1&extra=agent.session:8".into())
        .await
        .json();
    assert!(l.entries.is_empty());
    let l: LogResponse = get("extra=agent.model".into()).await.json();
    assert_eq!(ids(l), [d, b]);
    let l: LogResponse = get("author=ALI".into()).await.json();
    assert_eq!(ids(l), [a]);
    let l: LogResponse = get("extra=agent.model&path=f".into()).await.json();
    assert_eq!(ids(l), [b]);

    for bad in ["extra=:x", "extra=", "limit=x"] {
        assert_eq!(
            get(bad.into()).await.status,
            StatusCode::BAD_REQUEST,
            "{bad}"
        );
    }

    let p: ProvenanceResponse = c.get(&format!("/v1/provenance/{d}")).await.json();
    assert_eq!(p.scanned, 3);
    assert!(!p.truncated);
    assert_eq!(p.agents.len(), 2);
    assert_eq!(p.humans.len(), 1);
    assert_eq!(p.humans[0].author, "alice");
    let p: ProvenanceResponse = c
        .get(&format!("/v1/provenance/{d}?path=g&limit=2"))
        .await
        .json();
    assert_eq!(p.agents.len(), 1);
    assert_eq!(p.agents[0].model, "m2");
    assert!(p.humans.is_empty());
    assert!(p.truncated);
    assert_eq!(
        c.get(&format!("/v1/provenance/{d}?limit=x")).await.status,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        c.get("/v1/provenance/zz").await.status,
        StatusCode::BAD_REQUEST
    );

    let bl: BlameResponse = c.get(&format!("/v1/blame/{d}?path=f")).await.json();
    assert_eq!(bl.changesets.len(), 2);
    assert_eq!(bl.changesets[&a].author, "alice");
    assert_eq!(bl.changesets[&b].agent["agent.model"], "m1");
    assert_eq!(bl.changesets[&b].message_first_line, "edit f");
}

#[tokio::test]
async fn search_endpoint() {
    let c = Client::new();
    let a = c
        .commit(
            &[],
            &[
                (
                    "src/a.rs",
                    Some("fn main() {\n    println!(\"Needle\");\n}\n"),
                ),
                ("src/b.rs", Some("// needle needle\nlet x = 1;\n")),
                ("docs/n.md", Some("a needle in docs\n")),
                ("img.bin", Some("needle\0")),
            ],
            "a",
        )
        .await;
    c.set_ref("main", a).await;
    c.set_ref("release/1.0", a).await;

    let r = c.get("/v1/search/main?q=needle").await;
    assert_eq!(r.status, StatusCode::OK);
    let s: SearchResponse = r.json();
    let hits: Vec<(String, u64)> = s
        .matches
        .iter()
        .map(|m| (m.path.to_string(), m.line_number))
        .collect();
    assert_eq!(hits, [("docs/n.md".into(), 1), ("src/b.rs".into(), 1)]);
    assert_eq!(s.matches[1].line, "// needle needle");
    assert_eq!(
        s.matches[1].ranges,
        [
            SearchRange { start: 3, end: 9 },
            SearchRange { start: 10, end: 16 }
        ]
    );
    assert!(!s.truncated && s.stats.indexed);
    assert_eq!(s.stats.files_skipped, 1);
    assert_eq!(s.stats.files_scanned, 2);

    // Revisions: a ref with a slash, and a full id.
    let by_slash: SearchResponse = c.get("/v1/search/release/1.0?q=needle").await.json();
    assert_eq!(by_slash.matches, s.matches);
    let by_id: SearchResponse = c.get(&format!("/v1/search/{a}?q=needle")).await.json();
    assert_eq!(by_id.matches, s.matches);

    // Options.
    let ci: SearchResponse = c.get("/v1/search/main?q=needle&i=true").await.json();
    assert_eq!(ci.matches.len(), 3);
    let re: SearchResponse = c
        .get("/v1/search/main?q=n%5Ba-z%5D%2Bdle%20in&regex=true")
        .await
        .json();
    assert_eq!(re.matches.len(), 1);
    assert_eq!(re.matches[0].path.as_str(), "docs/n.md");
    let scoped: SearchResponse = c.get("/v1/search/main?q=needle&path=src").await.json();
    assert_eq!(scoped.matches.len(), 1);
    let globbed: SearchResponse = c
        .get("/v1/search/main?q=needle&glob=**/*.md&i=true")
        .await
        .json();
    assert_eq!(globbed.matches.len(), 1);
    assert_eq!(globbed.matches[0].path.as_str(), "docs/n.md");
    let unindexed: SearchResponse = c
        .get("/v1/search/main?q=.*&regex=true&path=docs")
        .await
        .json();
    assert!(!unindexed.stats.indexed);
    assert_eq!(unindexed.matches.len(), 1);

    // Limits: truncated when more exist, clamped into 1..=1000.
    let one: SearchResponse = c.get("/v1/search/main?q=needle&limit=1").await.json();
    assert_eq!(one.matches.len(), 1);
    assert!(one.truncated);
    let zero: SearchResponse = c.get("/v1/search/main?q=needle&limit=0").await.json();
    assert_eq!(zero.matches.len(), 1);
    let huge: SearchResponse = c.get("/v1/search/main?q=needle&limit=999999").await.json();
    assert_eq!(huge.matches.len(), 2);
    assert!(!huge.truncated);
    let none: SearchResponse = c.get("/v1/search/main?q=absent").await.json();
    assert!(none.matches.is_empty() && !none.truncated);

    // Errors.
    for (uri, status) in [
        ("/v1/search/main?q=%28&regex=true", StatusCode::BAD_REQUEST),
        ("/v1/search/main?q=", StatusCode::BAD_REQUEST),
        ("/v1/search/main", StatusCode::BAD_REQUEST),
        ("/v1/search/main?q=x&i=maybe", StatusCode::BAD_REQUEST),
        ("/v1/search/main?q=x&limit=many", StatusCode::BAD_REQUEST),
        ("/v1/search/main?q=x&glob=%5B", StatusCode::BAD_REQUEST),
        ("/v1/search/nosuchref?q=x", StatusCode::NOT_FOUND),
        (
            "/v1/search/0000000000000000000000000000000000000000000000000000000000000000?q=x",
            StatusCode::NOT_FOUND,
        ),
    ] {
        let r = c.get(uri).await;
        assert_eq!(r.status, status, "{uri}");
    }
    let r = c.get("/v1/search/main?q=%28&regex=true").await;
    assert!(r.json::<ErrorResponse>().error.contains("bad regex"));
}

#[tokio::test]
async fn search_sees_each_revision() {
    let c = Client::new();
    let a = c.commit(&[], &[("f", Some("old token\n"))], "a").await;
    let b = c.commit(&[a], &[("f", Some("new token\n"))], "b").await;
    let old: SearchResponse = c.get(&format!("/v1/search/{a}?q=old")).await.json();
    let new: SearchResponse = c.get(&format!("/v1/search/{b}?q=old")).await.json();
    assert_eq!(old.matches.len(), 1);
    assert!(new.matches.is_empty());
}

fn lines(n: usize, tag: &str) -> String {
    (0..n).map(|i| format!("{tag} line {i}\n")).collect()
}

/// `a.txt` renamed exactly to `dir/b.txt`, and `c.txt` renamed with a small
/// edit to `d.txt`.
async fn rename_repo(c: &Client) -> (ChangesetId, ChangesetId) {
    let (a, ctext) = (lines(10, "a"), lines(10, "c"));
    let base = c
        .commit(
            &[],
            &[
                ("a.txt", Some(&a)),
                ("c.txt", Some(&ctext)),
                ("keep", Some("k\n")),
            ],
            "base",
        )
        .await;
    let edited = format!("{ctext}tail\n");
    let moved = c
        .commit(
            &[base],
            &[
                ("a.txt", None),
                ("dir/b.txt", Some(&a)),
                ("c.txt", None),
                ("d.txt", Some(&edited)),
            ],
            "moved",
        )
        .await;
    (base, moved)
}

#[tokio::test]
async fn diff_renames_are_opt_in_and_additive() {
    let c = Client::new();
    let (base, moved) = rename_repo(&c).await;
    let uri = format!("/v1/diff?from={base}&to={moved}");

    // Absent or false: the response has exactly the old shape.
    let plain = c.get(&uri).await;
    assert_eq!(plain.status, StatusCode::OK);
    let raw: serde_json::Value = plain.json();
    assert_eq!(
        raw.as_object().unwrap().keys().collect::<Vec<_>>(),
        ["entries"]
    );
    let off: serde_json::Value = c.get(&format!("{uri}&renames=false")).await.json();
    assert_eq!(off, raw);
    let df: DiffResponse = plain.json();
    assert!(df.renames.is_empty() && !df.renames_truncated);
    assert_eq!(df.entries.len(), 4);

    // Enabled: the entries are unchanged and the renames ride beside them.
    let on: DiffResponse = c.get(&format!("{uri}&renames=true")).await.json();
    assert_eq!(on.entries, df.entries);
    let got: Vec<_> = on
        .renames
        .iter()
        .map(|r| (r.from.as_str(), r.to.as_str(), r.exact))
        .collect();
    assert_eq!(
        got,
        [("c.txt", "d.txt", false), ("a.txt", "dir/b.txt", true)]
    );
    assert_eq!(on.renames[1].similarity, 100);
    assert!((50..100).contains(&on.renames[0].similarity));
    assert!(!on.renames_truncated);

    // A higher threshold drops the inexact one; exact stays at 100.
    let hi: DiffResponse = c
        .get(&format!("{uri}&renames=true&rename_threshold=100"))
        .await
        .json();
    assert_eq!(hi.renames.len(), 1);
    assert!(hi.renames[0].exact);
    let lo: DiffResponse = c
        .get(&format!("{uri}&renames=true&rename_threshold=0"))
        .await
        .json();
    assert_eq!(lo.renames.len(), 2);

    // The prefix limits what is paired: the other side is out of view.
    let part: DiffResponse = c
        .get(&format!("{uri}&renames=true&prefix=dir"))
        .await
        .json();
    assert!(part.renames.is_empty());
}

#[tokio::test]
async fn diff_rejects_a_bad_rename_threshold() {
    let c = Client::new();
    let (base, moved) = rename_repo(&c).await;
    for bad in ["101", "-1", "abc", "", "50.5", "300"] {
        let r = c
            .get(&format!(
                "/v1/diff?from={base}&to={moved}&renames=true&rename_threshold={bad}"
            ))
            .await;
        assert_eq!(r.status, StatusCode::BAD_REQUEST, "{bad:?}");
        let e: ErrorResponse = r.json();
        assert!(e.error.contains("rename_threshold"), "{}", e.error);
    }
    // It is validated even when renames are off, so a typo is not silent.
    let r = c
        .get(&format!(
            "/v1/diff?from={base}&to={moved}&rename_threshold=999"
        ))
        .await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);
    for ok in ["0", "1", "100"] {
        let r = c
            .get(&format!(
                "/v1/diff?from={base}&to={moved}&renames=true&rename_threshold={ok}"
            ))
            .await;
        assert_eq!(r.status, StatusCode::OK, "{ok}");
    }
}

/// `uri` plus one more query parameter.
fn with(uri: &str, param: &str) -> String {
    let sep = if uri.contains('?') { '&' } else { '?' };
    format!("{uri}{sep}{param}")
}

/// One page of `/v1/files` (`list = "files"`) or `/v1/diff` (`"entries"`):
/// its paths and `next`.
async fn path_page(c: &Client, uri: &str, list: &str) -> (Vec<String>, Option<String>) {
    let r = c.get(uri).await;
    assert_eq!(r.status, StatusCode::OK, "{uri}");
    let v: serde_json::Value = r.json();
    let paths = v[list]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["path"].as_str().unwrap().to_string())
        .collect();
    (
        paths,
        v.get("next").map(|n| n.as_str().unwrap().to_string()),
    )
}

/// A files or diff cursor built from the layout in docs/FORMATS.md ("Page
/// cursors"), to point one anywhere, such as past the end.
fn forged_path_cursor<Q: Serialize>(kind: u8, query: &Q, path: &str) -> String {
    use base64::Engine;
    let mut q = vec![kind];
    q.extend(postcard::to_stdvec(query).unwrap());
    let mut bytes = vec![1, kind];
    bytes.extend_from_slice(&Hash::of_blob(&q).as_bytes()[..16]);
    bytes.extend(postcard::to_stdvec(path).unwrap());
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

/// `GET /v1/files` and `/v1/diff` answer one page with `limit`, resume with
/// `after`, and answer exactly as before without either.
#[tokio::test]
async fn files_and_diff_page_with_limit_and_after() {
    let c = Client::new();
    let base = c
        .commit(
            &[],
            &[
                ("a/1", Some("1")),
                ("a/2", Some("2")),
                ("b", Some("b")),
                ("c/d/e", Some("e")),
                ("z", Some("z")),
            ],
            "base",
        )
        .await;
    let edit = c
        .commit(
            &[base],
            &[
                ("a/1", Some("1 again")),
                ("a/2", None),
                ("a.txt", Some("t")),
                ("b", None),
                ("b/x", Some("x")),
                ("z", Some("z again")),
            ],
            "edit",
        )
        .await;
    let files_uri = format!("/v1/files/{base}");
    let diff_uri = format!("/v1/diff?from={base}&to={edit}");
    let cases = [
        (
            files_uri.as_str(),
            "files",
            &["a/1", "a/2", "b", "c/d/e", "z"][..],
        ),
        (
            diff_uri.as_str(),
            "entries",
            // `a.txt` sorts before `a/1`, as `.` sorts before `/`.
            &["a.txt", "a/1", "a/2", "b", "b/x", "z"][..],
        ),
    ];
    for (uri, list, all) in cases {
        // An old client: no new parameters, the old shape, everything.
        let r = c.get(uri).await;
        assert_eq!(r.status, StatusCode::OK);
        let v: serde_json::Value = r.json();
        assert_eq!(v.as_object().unwrap().keys().collect::<Vec<_>>(), [list]);
        assert_eq!(path_page(&c, uri, list).await, (to_strings(all), None));

        // First, middle and last pages of 2.
        let (first, next) = path_page(&c, &with(uri, "limit=2"), list).await;
        assert_eq!(first, all[..2]);
        let next = next.expect("a first page has next");
        let (middle, next) =
            path_page(&c, &with(uri, &format!("limit=2&after={next}")), list).await;
        assert_eq!(middle, all[2..4]);
        let next = next.expect("a middle page has next");
        let (last, end) = path_page(&c, &with(uri, &format!("limit=2&after={next}")), list).await;
        assert_eq!(last, all[4..]);
        assert_eq!(end, None, "{uri}: the last page has no next");
        // `limit` may change between pages; without it, the rest.
        let (rest, end) = path_page(&c, &with(uri, &format!("after={next}")), list).await;
        assert_eq!((rest, end), (to_strings(&all[4..]), None));
        // A page as large as the rest is the last one.
        let n = all.len();
        assert_eq!(
            path_page(&c, &with(uri, &format!("limit={n}")), list).await,
            (to_strings(all), None)
        );

        // Zero, negative and non-numeric limits are 400 `invalid`.
        for bad in ["0", "-1", "two", ""] {
            let r = c.get(&with(uri, &format!("limit={bad}"))).await;
            assert_eq!(r.status, StatusCode::BAD_REQUEST, "{uri} limit={bad:?}");
            assert_eq!(r.code(), "invalid");
            assert!(r.json::<ErrorResponse>().error.contains("limit"));
        }
        // A cursor of another query, or garbage, is 400 naming the cursor.
        for bad in [next.as_str(), "not-a-cursor"] {
            let other = if list == "files" {
                format!("/v1/files/{edit}?after={bad}")
            } else {
                format!("/v1/diff?to={edit}&after={bad}")
            };
            let r = c.get(&other).await;
            assert_eq!(r.status, StatusCode::BAD_REQUEST, "{other}");
            assert!(r.json::<ErrorResponse>().error.contains("cursor"));
        }
    }

    // A cursor past the last path: an empty last page.
    let past = forged_path_cursor(4, &(base, ""), "zz");
    let r = c
        .get(&format!("/v1/files/{base}?limit=2&after={past}"))
        .await;
    assert_eq!(r.status, StatusCode::OK);
    assert_eq!(
        r.json::<serde_json::Value>(),
        serde_json::json!({ "files": [] })
    );
    let past = forged_path_cursor(5, &(Some(base), edit, "", None::<u8>), "zz");
    let r = c.get(&format!("{diff_uri}&limit=2&after={past}")).await;
    assert_eq!(r.status, StatusCode::OK);
    assert_eq!(
        r.json::<serde_json::Value>(),
        serde_json::json!({ "entries": [] })
    );
    // The same layout before the end resumes there, so the forging is right.
    let mid = forged_path_cursor(4, &(base, ""), "b");
    assert_eq!(
        path_page(&c, &format!("/v1/files/{base}?after={mid}"), "files").await,
        (to_strings(&["c/d/e", "z"]), None)
    );
}

/// A page that would end at a path too long for a cursor is 400
/// `cursor_unrepresentable`, and the same request without `limit` answers.
#[tokio::test]
async fn page_ending_at_a_long_path_is_cursor_unrepresentable() {
    let c = Client::new();
    let long = format!("d/{}", "n".repeat(4000));
    let tip = c
        .commit(&[], &[(long.as_str(), Some("1")), ("e", Some("2"))], "long")
        .await;
    for (uri, list) in [
        (format!("/v1/files/{tip}"), "files"),
        (format!("/v1/diff?to={tip}"), "entries"),
        (format!("/v1/diff?to={tip}&renames=true"), "entries"),
    ] {
        let r = c.get(&with(&uri, "limit=1")).await;
        assert_eq!(r.status, StatusCode::BAD_REQUEST, "{uri}");
        assert_eq!(r.code(), error_code::CURSOR_UNREPRESENTABLE, "{uri}");
        assert!(r.json::<ErrorResponse>().error.contains("without `limit`"));
        // A page that ends before it pages fine; no limit answers all.
        let (paths, _) = path_page(&c, &with(&uri, "limit=2"), list).await;
        assert_eq!(paths, [long.clone(), "e".into()]);
        let (paths, next) = path_page(&c, &uri, list).await;
        assert_eq!((paths.len(), next), (2, None));
    }
}

fn to_strings(s: &[&str]) -> Vec<String> {
    s.iter().map(|s| s.to_string()).collect()
}

/// With renames, pages of the diff concatenate to the unpaged answer, and
/// each rename comes with its `to` entry.
#[tokio::test]
async fn diff_pages_keep_renames_whole() {
    let c = Client::new();
    let (base, moved) = rename_repo(&c).await;
    let uri = format!("/v1/diff?from={base}&to={moved}&renames=true");
    let all: DiffResponse = c.get(&uri).await.json();
    assert_eq!(all.renames.len(), 2);
    for limit in 1..=5 {
        let mut got = DiffResponse::default();
        let mut after: Option<String> = None;
        loop {
            let mut u = format!("{uri}&limit={limit}");
            if let Some(a) = &after {
                u.push_str(&format!("&after={a}"));
            }
            let page: DiffResponse = c.get(&u).await.json();
            assert!(page.entries.len() <= limit);
            for r in &page.renames {
                assert!(page.entries.iter().any(|e| e.path == r.to), "{u}");
            }
            got.entries.extend(page.entries);
            got.renames.extend(page.renames);
            got.renames_truncated |= page.renames_truncated;
            match page.next {
                Some(n) => after = Some(n),
                None => break,
            }
        }
        assert_eq!(got, all, "limit {limit}");
    }
    // A cursor is bound to the rename settings too.
    let page: DiffResponse = c.get(&format!("{uri}&limit=1")).await.json();
    let next = page.next.unwrap();
    let r = c
        .get(&format!(
            "/v1/diff?from={base}&to={moved}&limit=1&after={next}"
        ))
        .await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);
}

/// A `limit` above [`MAX_PATHS_LIMIT`] answers a page of the maximum, and a
/// request without `limit` is still the whole list.
#[tokio::test]
async fn files_and_diff_clamp_limit_to_the_max() {
    let c = Client::new();
    let n = MAX_PATHS_LIMIT + 5;
    let names: Vec<String> = (0..n).map(|i| format!("d{}/f{i:05}", i % 50)).collect();
    let files: Vec<(&str, Option<&str>)> = names.iter().map(|p| (p.as_str(), Some("x"))).collect();
    let id = c.commit(&[], &files, "big").await;
    let mut sorted = names.clone();
    sorted.sort();

    for (uri, list) in [
        (format!("/v1/files/{id}"), "files"),
        (format!("/v1/diff?to={id}"), "entries"),
    ] {
        let (all, next) = path_page(&c, &uri, list).await;
        assert_eq!((all.len(), next), (n, None), "{uri}: unpaged is uncapped");
        let (page, next) = path_page(&c, &with(&uri, &format!("limit={}", n * 10)), list).await;
        assert_eq!(page.len(), MAX_PATHS_LIMIT, "{uri}");
        assert_eq!(page, sorted[..MAX_PATHS_LIMIT]);
        let next = next.expect("clamped page has next");
        let (rest, end) = path_page(
            &c,
            &with(&uri, &format!("limit={}&after={next}", usize::MAX)),
            list,
        )
        .await;
        assert_eq!((rest, end), (sorted[MAX_PATHS_LIMIT..].to_vec(), None));
    }
}

#[tokio::test]
async fn log_follow_crosses_renames() {
    let c = Client::new();
    let (base, moved) = rename_repo(&c).await;
    let tip = c
        .commit(&[moved], &[("dir/b.txt", Some("changed\n"))], "edit")
        .await;
    let ids = |l: &LogResponse| l.entries.iter().map(|e| e.id).collect::<Vec<_>>();

    // Without follow the log is as before, with no new keys in its entries.
    let plain = c.get(&format!("/v1/log/{tip}?path=d.txt")).await;
    let raw: serde_json::Value = plain.json();
    assert!(raw["entries"][0].get("path_at").is_none());
    assert!(raw["entries"][0].get("renamed").is_none());
    assert_eq!(ids(&plain.json()), [moved]);
    let off: LogResponse = c
        .get(&format!("/v1/log/{tip}?path=dir/b.txt&follow=false"))
        .await
        .json();
    assert_eq!(ids(&off), [tip, moved]);

    // With it the history continues under the old name.
    let l: LogResponse = c
        .get(&format!("/v1/log/{tip}?path=dir/b.txt&follow=true"))
        .await
        .json();
    assert_eq!(ids(&l), [tip, moved, base]);
    let at: Vec<_> = l
        .entries
        .iter()
        .map(|e| e.path_at.as_ref().unwrap().as_str())
        .collect();
    assert_eq!(at, ["dir/b.txt", "dir/b.txt", "a.txt"]);
    let r = l.entries[1].renamed.as_ref().unwrap();
    assert_eq!(
        (r.from.as_str(), r.to.as_str(), r.exact),
        ("a.txt", "dir/b.txt", true)
    );
    let limited: LogResponse = c
        .get(&format!("/v1/log/{tip}?path=dir/b.txt&follow=true&limit=2"))
        .await
        .json();
    assert_eq!(ids(&limited), [tip, moved]);
}

#[tokio::test]
async fn log_follow_needs_one_file_path() {
    let c = Client::new();
    let (_, moved) = rename_repo(&c).await;
    for q in [
        "follow=true",
        "follow=true&path=",
        "follow=true&path=dir",
        "follow=maybe&path=keep",
    ] {
        let r = c.get(&format!("/v1/log/{moved}?{q}")).await;
        assert_eq!(r.status, StatusCode::BAD_REQUEST, "{q}");
        let _: ErrorResponse = r.json();
    }
    let r = c
        .get(&format!("/v1/log/{moved}?follow=true&path=keep"))
        .await;
    assert_eq!(r.status, StatusCode::OK);
}

fn cas(expected: Option<ChangesetId>, new: Option<ChangesetId>) -> CasRefRequest {
    CasRefRequest {
        expected,
        new,
        ..Default::default()
    }
}

#[tokio::test]
async fn ref_writes_validate_names() {
    let c = Client::new();
    let a = c.commit(&[], &[("a", Some("1"))], "a").await;
    for bad in ["a..b", "x.lock", "-x", "a%20b", "a%3Ab", "a//b", "a%5Eb"] {
        let r = c
            .post(&format!("/v1/refs/{bad}"), &cas(None, Some(a)))
            .await;
        assert_eq!(r.status, StatusCode::BAD_REQUEST, "{bad}");
        assert_eq!(r.code(), "invalid_name", "{bad}");
        let r = c
            .post(
                "/v1/land",
                &LandRequest {
                    changeset: a,
                    target_ref: bad
                        .replace("%20", " ")
                        .replace("%3A", ":")
                        .replace("%5E", "^"),
                    create: None,
                    land_id: None,
                },
            )
            .await;
        assert_eq!(r.status, StatusCode::BAD_REQUEST, "land {bad}");
        assert_eq!(r.code(), "invalid_name", "land {bad}");
    }
    let r = c.post("/v1/refs/a..b", &cas(None, Some(a))).await;
    let msg = r.json::<ErrorResponse>().error;
    assert!(msg.contains("invalid ref name"), "{msg}");
    assert!(c
        .get("/v1/refs")
        .await
        .json::<RefsResponse>()
        .refs
        .is_empty());
    // Valid names, including nested ones, keep working.
    c.set_ref("release/1.0", a).await;
    c.set_ref("feature/x-1", a).await;
}

#[tokio::test]
async fn tags_are_created_and_deleted_but_never_moved() {
    let c = Client::new();
    let a = c.commit(&[], &[("a", Some("1"))], "a").await;
    let b = c.commit(&[a], &[("a", Some("2"))], "b").await;
    c.set_ref("tags/v1", a).await;

    let r = c.post("/v1/refs/tags/v1", &cas(Some(a), Some(b))).await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);
    assert!(r.json::<ErrorResponse>().error.contains("immutable"));
    assert_eq!(r.code(), "tag_immutable");
    // A lost swap is a `CasRefResponse`, not an error body.
    let r = c.post("/v1/refs/tags/v1", &cas(None, Some(b))).await;
    assert_eq!(r.status, StatusCode::CONFLICT);
    assert!(!r.json::<CasRefResponse>().ok);
    let t: RefResponse = c.get("/v1/refs/tags/v1").await.json();
    assert_eq!(t.target, Some(a));

    // Land refuses tags, even when the ref does not exist yet.
    for name in ["tags/v1", "tags/v2"] {
        let r = c
            .post(
                "/v1/land",
                &LandRequest {
                    changeset: b,
                    target_ref: name.into(),
                    create: None,
                    land_id: None,
                },
            )
            .await;
        assert_eq!(r.status, StatusCode::BAD_REQUEST, "{name}");
        assert_eq!(r.code(), "tag_immutable", "{name}");
    }

    // A tag resolves by full ref and by short name; a branch of that name wins.
    assert_eq!(
        c.get("/v1/resolve/tags/v1")
            .await
            .json::<ResolveResponse>()
            .id,
        a
    );
    assert_eq!(
        c.get("/v1/resolve/v1").await.json::<ResolveResponse>().id,
        a
    );
    c.set_ref("v1", b).await;
    assert_eq!(
        c.get("/v1/resolve/v1").await.json::<ResolveResponse>().id,
        b
    );

    // Deleting and recreating is how a tag changes.
    let r = c.post("/v1/refs/tags/v1", &cas(Some(a), None)).await;
    assert_eq!(r.status, StatusCode::OK);
    c.set_ref("tags/v1", b).await;
    let refs: RefsResponse = c.get("/v1/refs").await.json();
    assert_eq!(refs.refs["tags/v1"], b);
}

fn labeled(
    op: &str,
    message: Option<&str>,
    expected: Option<ChangesetId>,
    new: Option<ChangesetId>,
) -> CasRefRequest {
    CasRefRequest {
        expected,
        new,
        operation: Some(op.into()),
        message: message.map(Into::into),
    }
}

#[tokio::test]
async fn reflog_records_lands_and_ref_writes_newest_first() {
    let c = Client::new();
    let a = c.commit(&[], &[("a", Some("1"))], "a").await;
    let b = c.commit(&[a], &[("a", Some("2"))], "b").await;
    let land = |id| LandRequest {
        changeset: id,
        target_ref: "main".into(),
        create: None,
        land_id: None,
    };
    assert_eq!(c.post("/v1/land", &land(a)).await.status, StatusCode::OK);
    assert_eq!(c.post("/v1/land", &land(b)).await.status, StatusCode::OK);
    assert_eq!(c.post("/v1/land", &land(b)).await.status, StatusCode::OK);
    c.set_ref("dev", a).await;
    let r = c
        .post(
            "/v1/refs/main",
            &labeled("undo", Some("undo of seq 2"), Some(b), Some(a)),
        )
        .await;
    assert_eq!(r.status, StatusCode::OK);
    // A lost CAS records nothing.
    let r = c.post("/v1/refs/main", &cas(Some(b), None)).await;
    assert_eq!(r.status, StatusCode::CONFLICT);

    let all: RefLogResponse = c.get("/v1/reflog").await.json();
    let ops: Vec<_> = all.entries.iter().map(|e| e.operation.as_str()).collect();
    assert_eq!(ops, ["undo", "cas", "land", "land"]);
    assert!(all.entries.windows(2).all(|w| w[0].seq > w[1].seq));
    let undo = &all.entries[0];
    assert_eq!((undo.old, undo.new), (Some(b), Some(a)));
    assert_eq!(undo.message.as_deref(), Some("undo of seq 2"));
    assert_eq!(undo.actor, None, "auth is off");

    let main: RefLogResponse = c.get("/v1/reflog/main").await.json();
    assert_eq!(main.entries.len(), 3);
    assert!(main.entries.iter().all(|e| e.name == "main"));
    let none: RefLogResponse = c.get("/v1/reflog/nope").await.json();
    assert!(none.entries.is_empty());
    // Nested names work in the path.
    c.set_ref("release/1.0", a).await;
    let nested: RefLogResponse = c.get("/v1/reflog/release/1.0").await.json();
    assert_eq!(nested.entries.len(), 1);

    // Paging.
    let page: RefLogResponse = c.get("/v1/reflog/main?limit=2").await.json();
    assert_eq!(page.entries, main.entries[..2]);
    let before = page.entries[1].seq;
    let rest: RefLogResponse = c
        .get(&format!("/v1/reflog/main?limit=2&before={before}"))
        .await
        .json();
    assert_eq!(rest.entries, main.entries[2..]);
    let none: RefLogResponse = c.get("/v1/reflog?limit=0").await.json();
    assert!(none.entries.is_empty());
}

#[tokio::test]
async fn reflog_limits_and_bad_queries() {
    let c = Client::new();
    let a = c.commit(&[], &[("a", Some("1"))], "a").await;
    for i in 0..60 {
        c.set_ref(&format!("r{i}"), a).await;
    }
    let r: RefLogResponse = c.get("/v1/reflog").await.json();
    assert_eq!(r.entries.len(), 50, "default limit");
    let r: RefLogResponse = c.get("/v1/reflog?limit=5000").await.json();
    assert_eq!(r.entries.len(), 60, "capped at 1000, so all 60");
    for q in ["limit=x", "before=-1", "before=", "limit=1.5"] {
        let r = c.get(&format!("/v1/reflog?{q}")).await;
        assert_eq!(r.status, StatusCode::BAD_REQUEST, "{q}");
        let _: ErrorResponse = r.json();
    }
}

#[tokio::test]
async fn clients_cannot_forge_server_operations_or_unreadable_entries() {
    let c = Client::new();
    let a = c.commit(&[], &[("a", Some("1"))], "a").await;
    let long = "x".repeat(33);
    for op in ["land", "import", "", "Undo", "a b", long.as_str()] {
        let r = c
            .post("/v1/refs/x", &labeled(op, None, None, Some(a)))
            .await;
        assert_eq!(r.status, StatusCode::BAD_REQUEST, "operation {op:?}");
    }
    let too_long = "m".repeat(257);
    for m in ["two\nlines", too_long.as_str()] {
        let r = c
            .post("/v1/refs/x", &labeled("cas", Some(m), None, Some(a)))
            .await;
        assert_eq!(r.status, StatusCode::BAD_REQUEST, "message {m:?}");
    }
    let r = c
        .post(
            "/v1/refs/x",
            &labeled("my-tool.v2", Some("ok"), None, Some(a)),
        )
        .await;
    assert_eq!(r.status, StatusCode::OK);
    let log: RefLogResponse = c.get("/v1/reflog").await.json();
    assert_eq!(log.entries.len(), 1, "refused requests wrote nothing");
    assert_eq!(log.entries[0].operation, "my-tool.v2");
}

#[tokio::test]
async fn old_cas_bodies_without_log_fields_still_work() {
    let c = Client::new();
    let a = c.commit(&[], &[("a", Some("1"))], "a").await;
    let body = format!(r#"{{"expected":null,"new":"{a}"}}"#);
    let r = c.raw(Method::POST, "/v1/refs/old", body.into_bytes()).await;
    assert_eq!(r.status, StatusCode::OK);
    let log: RefLogResponse = c.get("/v1/reflog/old").await.json();
    assert_eq!(log.entries[0].operation, "cas");
}

#[tokio::test]
async fn resolve_prefix_finds_one_lists_many_and_rejects_bad_input() {
    let c = Client::new();
    // Commit until two ids share their first four hex digits.
    let mut by_prefix: BTreeMap<String, ChangesetId> = BTreeMap::new();
    let (a, b) = loop {
        let id = c
            .commit(&[], &[], &format!("prefix {}", by_prefix.len()))
            .await;
        if let Some(other) = by_prefix.insert(id.to_string()[..4].to_string(), id) {
            break (other.min(id), other.max(id));
        }
    };

    let hex = a.to_string();
    for short in [&hex[..12], &hex[..5], &hex[..64]] {
        let r = c.get(&format!("/v1/resolve-prefix/{short}")).await;
        assert_eq!(r.status, StatusCode::OK, "{short}");
        assert_eq!(r.json::<ResolvePrefixResponse>().id, a);
    }
    let upper = hex[..12].to_uppercase();
    let r = c.get(&format!("/v1/resolve-prefix/{upper}")).await;
    assert_eq!(r.json::<ResolvePrefixResponse>().id, a, "any case");

    let r = c.get(&format!("/v1/resolve-prefix/{}", &hex[..4])).await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);
    let amb: AmbiguousPrefixResponse = r.json();
    assert_eq!(amb.candidates, [a, b]);
    assert!(!amb.truncated);
    assert!(amb.error.contains("ambiguous"), "{}", amb.error);
    assert_eq!(amb.code.as_deref(), Some("ambiguous_prefix"));
    // Clients that know only ErrorResponse still read the message.
    assert!(r.json::<ErrorResponse>().error.contains("ambiguous"));

    let absent = (0u32..)
        .map(|n| format!("{n:06x}"))
        .find(|p| !by_prefix.keys().any(|k| p.starts_with(k.as_str())))
        .unwrap();
    let r = c.get(&format!("/v1/resolve-prefix/{absent}")).await;
    assert_eq!(r.status, StatusCode::NOT_FOUND);

    for bad in ["abc", "zzzz", "12g4", &"a".repeat(65)] {
        let r = c.get(&format!("/v1/resolve-prefix/{bad}")).await;
        assert_eq!(r.status, StatusCode::BAD_REQUEST, "{bad}");
        let e: ErrorResponse = r.json();
        assert!(e.error.contains("short id"), "{bad}: {}", e.error);
    }
}

#[tokio::test]
async fn land_creates_a_missing_ref_only_when_allowed() {
    let c = Client::new();
    let a = c.commit(&[], &[("a", Some("1\n"))], "a").await;
    let land = |target: &str, create: Option<bool>| LandRequest {
        changeset: a,
        target_ref: target.into(),
        create,
        land_id: None,
    };

    let r = c.post("/v1/land", &land("typo", Some(false))).await;
    assert_eq!(r.status, StatusCode::NOT_FOUND);
    assert_eq!(r.code(), "ref_missing");
    let e: ErrorResponse = r.json();
    assert!(
        e.error.contains(leep_core::refname::MISSING_REF_MARKER),
        "{}",
        e.error
    );
    let head: RefResponse = c.get("/v1/refs/typo").await.json();
    assert_eq!(head.target, None);
    let log: RefLogResponse = c.get("/v1/reflog").await.json();
    assert!(log.entries.is_empty(), "a refused land logs nothing");

    // `true`, and an old client that sends no `create`, both create.
    for (name, create) in [("asked", Some(true)), ("old-client", None)] {
        let r = c.post("/v1/land", &land(name, create)).await;
        assert_eq!(r.status, StatusCode::OK, "{name}");
        let head: RefResponse = c.get(&format!("/v1/refs/{name}")).await.json();
        assert_eq!(head.target, Some(a), "{name}");
    }
    // An existing ref lands with `create: false`.
    let b = c.commit(&[a], &[("a", Some("2\n"))], "b").await;
    let r = c
        .post(
            "/v1/land",
            &LandRequest {
                changeset: b,
                target_ref: "asked".into(),
                create: Some(false),
                land_id: None,
            },
        )
        .await;
    assert!(matches!(r.json(), LandResponse::Landed { landed, .. } if landed == b));
}

/// `fields=summary`: the same entries without their changes, paged with the
/// same cursors (either shape continues the other); without it, or with
/// `fields=full`, the body is the full log byte for byte.
#[tokio::test]
async fn log_summary_is_a_projection_of_the_full_log() {
    let c = Client::new();
    let base = c
        .commit(&[], &[("a/x", Some("0")), ("b", Some("0"))], "root")
        .await;
    let mut left = base;
    let mut right = base;
    for i in 0..12 {
        let text = format!("{i}");
        left = c
            .commit(
                &[left],
                &[("a/x", Some(&text)), ("c", Some(&text))],
                &format!("l{i}\nbody"),
            )
            .await;
        right = c
            .commit(&[right], &[("b", Some(&text))], &format!("r{i}"))
            .await;
    }
    let tip = c
        .commit(&[left, right], &[("b", Some("merged"))], "merge")
        .await;
    c.set_ref("main", tip).await;

    for query in [
        "",
        "&path=a",
        "&path=a/x",
        "&path=b",
        "&author=t",
        "&path=a/x&follow=true",
    ] {
        let uri = format!("/v1/log/{tip}?limit=100{query}");
        let full = c.get(&uri).await;
        assert_eq!(full.status, StatusCode::OK, "{uri}");
        let explicit = c.get(&format!("{uri}&fields=full")).await;
        assert_eq!(explicit.body, full.body, "{uri}");
        let parsed: LogResponse = full.json();
        assert_eq!(serde_json::to_vec(&parsed).unwrap(), full.body, "{uri}");
        assert!(!parsed.entries.is_empty(), "{uri}");

        let summary = c.get(&format!("{uri}&fields=summary")).await;
        assert_eq!(summary.status, StatusCode::OK, "{uri}");
        let got: LogSummaryResponse = summary.json();
        assert_eq!(got, LogSummaryResponse::from(parsed.clone()), "{uri}");
        assert!(summary.body.len() < full.body.len(), "{uri}");
        let v: serde_json::Value = summary.json();
        assert!(v["entries"][0].get("changes").is_none());
        assert!(v["entries"][0].get("changeset").is_none());

        // Pages of 3, alternating shapes, concatenate to the whole log.
        let mut ids = Vec::new();
        let mut after: Option<String> = None;
        for page in 0.. {
            let mut u = format!("/v1/log/{tip}?limit=3{query}");
            if page % 2 == 1 {
                u.push_str("&fields=summary");
            }
            if let Some(a) = &after {
                u.push_str(&format!("&after={a}"));
            }
            let r = c.get(&u).await;
            assert_eq!(r.status, StatusCode::OK, "{u}");
            let next = if page % 2 == 1 {
                let p: LogSummaryResponse = r.json();
                ids.extend(p.entries.iter().map(|e| e.id));
                p.next
            } else {
                let p: LogResponse = r.json();
                ids.extend(p.entries.iter().map(|e| e.id));
                p.next
            };
            match next {
                Some(n) => after = Some(n),
                None => break,
            }
        }
        let want: Vec<ChangesetId> = parsed.entries.iter().map(|e| e.id).collect();
        assert_eq!(ids, want, "{query}");
    }

    let r = c.get(&format!("/v1/log/{tip}?fields=changes")).await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);
    assert!(r.json::<ErrorResponse>().error.contains("fields"));
}

/// `?after=` pages through log, search and the ref log over HTTP; requests
/// without it answer as before, plus `next` only when they stopped early.
#[tokio::test]
async fn list_endpoints_page_with_cursors() {
    let c = Client::new();
    let mut chain: Vec<ChangesetId> = Vec::new();
    for i in 0..40 {
        let parents: Vec<ChangesetId> = chain.last().copied().into_iter().collect();
        let text = format!("needle {i}\nhay\nneedle again {i}\n");
        let path = format!("d{}/f{i}", i % 3);
        let files = [(path.as_str(), Some(text.as_str()))];
        chain.push(c.commit(&parents, &files, &format!("c{i}")).await);
        let r = c
            .post(
                "/v1/refs/main",
                &CasRefRequest {
                    expected: parents.first().copied(),
                    new: chain.last().copied(),
                    ..Default::default()
                },
            )
            .await;
        assert_eq!(r.status, StatusCode::OK);
    }
    let tip = *chain.last().unwrap();

    // An unpaged request: no `next`, and a client that knows nothing of
    // cursors decodes the body as before.
    #[derive(serde::Deserialize)]
    struct OldLog {
        entries: Vec<ChangesetInfo>,
    }
    let r = c.get(&format!("/v1/log/{tip}")).await;
    let v: serde_json::Value = r.json();
    assert!(v.get("next").is_none());
    assert_eq!(r.json::<OldLog>().entries.len(), 40);
    let r = c.get(&format!("/v1/log/{tip}?limit=7")).await;
    assert!(r.json::<LogResponse>().next.is_some());
    assert_eq!(r.json::<OldLog>().entries.len(), 7);

    for query in ["", "&path=d1", "&path=d2/f5", "&author=t"] {
        let mut got = Vec::new();
        let mut after: Option<String> = None;
        loop {
            let mut uri = format!("/v1/log/{tip}?limit=3{query}");
            if let Some(a) = &after {
                uri.push_str(&format!("&after={a}"));
            }
            let r = c.get(&uri).await;
            assert_eq!(r.status, StatusCode::OK, "{uri}");
            let page: LogResponse = r.json();
            got.extend(page.entries.into_iter().map(|e| e.id));
            match page.next {
                Some(n) => after = Some(n),
                None => break,
            }
        }
        let all: LogResponse = c
            .get(&format!("/v1/log/{tip}?limit=10000{query}"))
            .await
            .json();
        let want: Vec<ChangesetId> = all.entries.iter().map(|e| e.id).collect();
        assert_eq!(got, want, "{query}");
    }

    // A cursor is bound to its query, and garbage is a 400 with a message.
    let first: LogResponse = c.get(&format!("/v1/log/{tip}?limit=2")).await.json();
    let next = first.next.unwrap();
    for uri in [
        format!("/v1/log/{tip}?limit=2&path=d1&after={next}"),
        format!("/v1/log/{}?limit=2&after={next}", chain[3]),
        format!("/v1/log/{tip}?after=not-a-cursor"),
        format!("/v1/log/{tip}?after={}", "A".repeat(5000)),
        format!("/v1/search/main?q=needle&after={next}"),
        format!("/v1/reflog?after={next}"),
    ] {
        let r = c.get(&uri).await;
        assert_eq!(r.status, StatusCode::BAD_REQUEST, "{uri}");
        let e: ErrorResponse = r.json();
        assert!(e.error.contains("cursor"), "{uri}: {}", e.error);
    }

    // Search: pages concatenate, and `truncated` stays set on cut pages.
    let all: SearchResponse = c.get("/v1/search/main?q=needle&limit=1000").await.json();
    assert_eq!(all.matches.len(), 80);
    assert!(!all.truncated && all.next.is_none());
    let mut got = Vec::new();
    let mut after: Option<String> = None;
    loop {
        let mut uri = "/v1/search/main?q=needle&limit=7".to_string();
        if let Some(a) = &after {
            uri.push_str(&format!("&after={a}"));
        }
        let page: SearchResponse = c.get(&uri).await.json();
        got.extend(page.matches);
        match page.next {
            Some(n) => {
                assert!(page.truncated);
                after = Some(n);
            }
            None => break,
        }
    }
    assert_eq!(got, all.matches);

    // Ref log: cursors and the old `before` both page; together they are 400.
    let all: RefLogResponse = c.get("/v1/reflog/main?limit=1000").await.json();
    assert_eq!(all.entries.len(), 40);
    let mut got = Vec::new();
    let mut after: Option<String> = None;
    loop {
        let mut uri = "/v1/reflog/main?limit=6".to_string();
        if let Some(a) = &after {
            uri.push_str(&format!("&after={a}"));
        }
        let page: RefLogResponse = c.get(&uri).await.json();
        got.extend(page.entries);
        match page.next {
            Some(n) => after = Some(n),
            None => break,
        }
    }
    assert_eq!(got, all.entries);
    let page: RefLogResponse = c.get("/v1/reflog/main?limit=6").await.json();
    let r = c
        .get(&format!(
            "/v1/reflog/main?limit=6&before=9&after={}",
            page.next.unwrap()
        ))
        .await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn every_error_has_a_code() {
    let c = Client::new();
    let a = c.commit(&[], &[("a", Some("1"))], "a").await;
    let cases = [
        (
            c.get(&format!(
                "/v1/changesets/{}",
                ChangesetId(Hash::of_blob(b"none"))
            ))
            .await,
            StatusCode::NOT_FOUND,
            "not_found",
        ),
        (
            c.get("/v1/resolve/nope").await,
            StatusCode::NOT_FOUND,
            "not_found",
        ),
        (
            c.get("/v1/blobs/zz").await,
            StatusCode::BAD_REQUEST,
            "invalid",
        ),
        (
            c.get(&format!("/v1/log/{a}?limit=many")).await,
            StatusCode::BAD_REQUEST,
            "invalid",
        ),
        (
            c.raw(Method::POST, "/v1/land", b"{".to_vec()).await,
            StatusCode::BAD_REQUEST,
            "invalid",
        ),
        // Routing answers without a handler; still a JSON error.
        (
            c.get("/v1/no-such-endpoint").await,
            StatusCode::NOT_FOUND,
            "not_found",
        ),
        (
            c.raw(Method::DELETE, "/v1/land", vec![]).await,
            StatusCode::METHOD_NOT_ALLOWED,
            "invalid",
        ),
    ];
    for (r, status, code) in cases {
        assert_eq!((r.status, r.code().as_str()), (status, code));
        assert!(error_code::ALL.contains(&code));
        assert!(!r.json::<ErrorResponse>().error.is_empty());
    }

    // Refusals from the token middleware and the review endpoint.
    use leep_server::auth::{hash_token, Auth};
    let tokens = serde_json::json!({ "tokens": [
        { "name": "old", "role": "write", "hash": hash_token("old"), "expires": "2020-01-01T00:00:00Z" },
        { "name": "viewer", "role": "read", "hash": hash_token("viewer") },
        { "name": "agent", "role": "write", "hash": hash_token("agent"), "review": false },
    ]});
    let authed = leep_server::router_with_auth(
        Repo::new(
            Arc::new(MemoryBlobStore::new()),
            Arc::new(MemoryMetaStore::new()),
        ),
        1 << 20,
        Some(Auth::parse(&tokens.to_string()).unwrap()),
    );
    let call = |token: Option<&'static str>, method: Method, uri: String, body: Vec<u8>| {
        let app = authed.clone();
        async move {
            let mut req = Request::builder().method(method).uri(uri);
            if let Some(t) = token {
                req = req.header("authorization", format!("Bearer {t}"));
            }
            let resp = app
                .oneshot(req.body(Body::from(body)).unwrap())
                .await
                .unwrap();
            let (parts, body) = resp.into_parts();
            Reply {
                status: parts.status,
                headers: parts.headers,
                body: body.collect().await.unwrap().to_bytes().to_vec(),
            }
        }
    };
    let review = serde_json::to_vec(&AddReviewRequest {
        verdict: leep_core::review::Verdict::Approve,
        comment: String::new(),
    })
    .unwrap();
    let reviews = format!("/v1/changesets/{a}/reviews");
    let cases = [
        (
            call(None, Method::GET, "/v1/refs".into(), vec![]).await,
            StatusCode::UNAUTHORIZED,
            "unauthorized",
        ),
        (
            call(Some("old"), Method::GET, "/v1/refs".into(), vec![]).await,
            StatusCode::UNAUTHORIZED,
            "token_expired",
        ),
        (
            call(
                Some("viewer"),
                Method::POST,
                reviews.clone(),
                review.clone(),
            )
            .await,
            StatusCode::FORBIDDEN,
            "forbidden",
        ),
        (
            call(Some("agent"), Method::POST, reviews, review).await,
            StatusCode::FORBIDDEN,
            "forbidden",
        ),
    ];
    for (r, status, code) in cases {
        assert_eq!((r.status, r.code().as_str()), (status, code));
        assert!(!r.json::<ErrorResponse>().error.is_empty());
    }

    // The body budget's refusals: the route cap (413), a token over its share
    // (429) and the server's budget spent (503), the last two with
    // Retry-After. Blobs up to 64 KiB, 100 KiB in flight, so a share is
    // 64 KiB.
    const KIB: usize = 1024;
    let tokens = serde_json::json!({ "tokens": [
        { "name": "a", "role": "write", "hash": hash_token("a") },
        { "name": "b", "role": "write", "hash": hash_token("b") },
    ]});
    let config = leep_server::reload::RuntimeConfig::fixed(
        Some(Auth::parse(&tokens.to_string()).unwrap()),
        None,
        None,
        None,
        None,
    );
    let budget = Client(leep_server::router_with_body_limits(
        Repo::new(
            Arc::new(MemoryBlobStore::new()),
            Arc::new(MemoryMetaStore::new()),
        ),
        1 << 30,
        leep_server::limits::BodyLimits {
            max_blob_bytes: 64 * KIB,
            max_json_bytes: 8 * KIB,
            max_inflight_bytes: 100 * KIB,
        },
        std::time::Duration::from_secs(60),
        leep_server::reload::ConfigHandle::fixed(config),
        leep_server::ReadyOptions::default(),
    ));
    let upload = |token: &str, uri: String, body: Body, len: Option<usize>| {
        let mut req = Request::builder()
            .method(Method::PUT)
            .uri(uri)
            .header("authorization", format!("Bearer {token}"));
        if let Some(n) = len {
            req = req.header("content-length", n);
        }
        req.body(body).unwrap()
    };
    let put = |token: &'static str, n: usize| {
        let data = vec![b'x'; n];
        let uri = format!("/v1/blobs/{}", Hash::of_blob(&data));
        let req = upload(token, uri, Body::from(data), Some(n));
        let app = budget.0.clone();
        async move {
            let resp = app.oneshot(req).await.unwrap();
            let (parts, body) = resp.into_parts();
            Reply {
                status: parts.status,
                headers: parts.headers,
                body: body.collect().await.unwrap().to_bytes().to_vec(),
            }
        }
    };
    // `a` holds 64 KiB with an upload that declares no length and stalls.
    let (tx, rx) = tokio::sync::mpsc::channel::<bytes::Bytes>(1);
    let stalled = futures_util::stream::unfold(rx, |mut rx| async move {
        rx.recv()
            .await
            .map(|c| (Ok::<_, std::convert::Infallible>(c), rx))
    });
    let uri = format!("/v1/blobs/{}", Hash::of_blob(b"never sent"));
    let held = tokio::spawn(budget.0.clone().oneshot(upload(
        "a",
        uri,
        Body::from_stream(stalled),
        None,
    )));
    for _ in 0..20 {
        tokio::task::yield_now().await;
    }
    let cases = [
        (
            put("a", 65 * KIB).await,
            StatusCode::PAYLOAD_TOO_LARGE,
            "too_large",
            None,
        ),
        (
            put("a", KIB).await,
            StatusCode::TOO_MANY_REQUESTS,
            "rate_limited",
            Some("1"),
        ),
        (
            put("b", 40 * KIB).await,
            StatusCode::SERVICE_UNAVAILABLE,
            "unavailable",
            Some("1"),
        ),
    ];
    for (r, status, code, retry_after) in cases {
        assert_eq!((r.status, r.code().as_str()), (status, code));
        let retry = r.headers.get("retry-after").map(|v| v.to_str().unwrap());
        assert_eq!(retry, retry_after, "{code}");
        assert!(!r.json::<ErrorResponse>().error.is_empty());
    }
    drop(tx);
    held.await.unwrap().unwrap();

    // A plain-text 503 from below the handlers (a load shedder, say) is
    // `unavailable`, not `internal`, and keeps its Retry-After.
    let busy = Client(
        Router::new()
            .route(
                "/busy",
                axum::routing::get(|| async {
                    (
                        StatusCode::SERVICE_UNAVAILABLE,
                        [("retry-after", "2")],
                        "overloaded",
                    )
                }),
            )
            .layer(axum::middleware::from_fn(leep_server::errors::json_errors)),
    );
    let r = busy.get("/busy").await;
    assert_eq!(r.status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(r.code(), "unavailable");
    assert_eq!(r.headers["retry-after"], "2");
    assert_eq!(r.json::<ErrorResponse>().error, "overloaded");
}

#[tokio::test]
async fn a_reused_land_id_has_its_own_code() {
    let c = Client::new();
    let a = c.commit(&[], &[("a", Some("1"))], "a").await;
    let b = c.commit(&[a], &[("a", Some("2"))], "b").await;
    let id: LandId = "00112233445566778899aabbccddeeff".parse().unwrap();
    let land = |changeset| LandRequest {
        changeset,
        target_ref: "main".into(),
        create: None,
        land_id: Some(id.clone()),
    };
    let first = c.post("/v1/land", &land(a)).await;
    assert_eq!(first.status, StatusCode::OK);
    let LandResponse::Landed { landed, .. } = first.json::<LandResponse>() else {
        panic!("not landed");
    };
    let r = c.post("/v1/land", &land(b)).await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);
    assert_eq!(r.code(), "land_id_reused");
    let body = r.json::<ErrorResponse>();
    // The text is what it was before the structured record.
    assert_eq!(
        body.error,
        format!("invalid input: land id {id} already landed {a}; use a new land id to land {b}")
    );
    assert_eq!(
        body.land.as_deref(),
        Some(&LandRefusal {
            land_id: id.clone(),
            changeset: a,
            landed: Some(landed),
        })
    );
    // On the wire: hex strings, under `land`.
    let raw: serde_json::Value = r.json();
    assert_eq!(
        raw["land"],
        serde_json::json!({
            "land_id": id.to_string(),
            "changeset": a.to_string(),
            "landed": landed.to_string(),
        })
    );
    // Other errors carry no `land`.
    let other = c
        .post(
            "/v1/land",
            &LandRequest {
                land_id: None,
                ..land(b)
            },
        )
        .await;
    let raw: serde_json::Value = other.json();
    assert!(raw.get("land").is_none(), "{raw}");
}

async fn with_request_id(c: &Client, uri: &str, id: Option<&str>) -> Reply {
    let mut req = Request::builder().uri(uri);
    if let Some(id) = id {
        req = req.header(REQUEST_ID_HEADER, id);
    }
    let resp =
        c.0.clone()
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

fn echoed(r: &Reply) -> String {
    r.headers[REQUEST_ID_HEADER].to_str().unwrap().to_string()
}

#[tokio::test]
async fn request_ids_are_kept_or_made_and_echoed() {
    let c = Client::new();
    // A valid client id is kept, echoed and named in the error body.
    let r = with_request_id(&c, "/v1/resolve/nope", Some("agent-7:run/42")).await;
    assert_eq!(r.status, StatusCode::NOT_FOUND);
    assert_eq!(echoed(&r), "agent-7:run/42");
    let e: ErrorResponse = r.json();
    assert_eq!(e.request_id.as_deref(), Some("agent-7:run/42"));
    assert_eq!(e.code.as_deref(), Some("not_found"));

    // Without one, or with one that is too long or outside the charset
    // (spaces, `=`, non-ASCII), the server makes its own.
    let long = "x".repeat(129);
    for id in [
        None,
        Some(long.as_str()),
        Some("caf\u{e9}"),
        Some("with space"),
        Some("x status=200 route=/v1/land"),
    ] {
        let r = with_request_id(&c, "/v1/resolve/nope", id).await;
        let made = echoed(&r);
        assert_eq!(made.len(), 32, "{made}");
        assert!(made.bytes().all(|b| b.is_ascii_hexdigit()), "{made}");
        assert_eq!(r.json::<ErrorResponse>().request_id, Some(made));
    }
    let a = echoed(&with_request_id(&c, "/v1/refs", None).await);
    let b = echoed(&with_request_id(&c, "/v1/refs", None).await);
    assert_ne!(a, b, "one id per request");

    // Successes carry the header; their bodies are untouched.
    let r = with_request_id(&c, "/v1/refs", Some("ok-1")).await;
    assert_eq!(r.status, StatusCode::OK);
    assert_eq!(echoed(&r), "ok-1");
    assert!(r.json::<serde_json::Value>().get("request_id").is_none());

    // Errors that start as plain text get it too.
    let r = with_request_id(&c, "/v1/no-such-endpoint", Some("t-404")).await;
    assert_eq!(
        r.json::<ErrorResponse>().request_id.as_deref(),
        Some("t-404")
    );
}
