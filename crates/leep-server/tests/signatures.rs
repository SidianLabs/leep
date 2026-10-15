//! Signed changesets over HTTP: submission, listing, auth classification and
//! the `--require-signatures` land policy (ADR 0010).

use std::collections::BTreeMap;
use std::sync::Arc;

use axum::body::Body;
use axum::http::{header, Method, Request, StatusCode};
use axum::Router;
use bytes::Bytes;
use ed25519_dalek::{Signer, SigningKey};
use http_body_util::BodyExt;
use leep_core::api::*;
use leep_core::memory::{MemoryBlobStore, MemoryMetaStore};
use leep_core::signature::{
    format_public_key, key_id, signing_message, Signature, NOT_SIGNED_MARKER,
};
use leep_core::{Changeset, ChangesetId, FileChange, FileKind, Hash, RepoPath};
use leep_derive::Repo;
use leep_server::auth::{hash_token, Auth};
use leep_server::signing::{RequireSignatures, SigningKeys};
use tower::ServiceExt;

const READ: &str = "read-token-for-tests";
const WRITE: &str = "write-token-for-tests";

fn alice() -> SigningKey {
    SigningKey::from_bytes(&[1; 32])
}
fn bob() -> SigningKey {
    SigningKey::from_bytes(&[2; 32])
}
fn mallory() -> SigningKey {
    SigningKey::from_bytes(&[3; 32])
}

fn pk(k: &SigningKey) -> String {
    format_public_key(k.verifying_key().as_bytes())
}

fn keys() -> Arc<SigningKeys> {
    let text = serde_json::json!({ "keys": [
        { "name": "alice", "public_key": pk(&alice()) },
        { "name": "bob", "public_key": pk(&bob()) },
    ]})
    .to_string();
    Arc::new(SigningKeys::parse(&text).unwrap())
}

struct Opts {
    keys: bool,
    auth: bool,
    require: &'static [&'static str],
}

struct Api {
    app: Router,
    repo: Repo,
}

struct Reply {
    status: StatusCode,
    body: Vec<u8>,
}

impl Reply {
    fn json<T: serde::de::DeserializeOwned>(&self) -> T {
        serde_json::from_slice(&self.body)
            .unwrap_or_else(|e| panic!("{e}: {}", String::from_utf8_lossy(&self.body)))
    }
    fn error(&self) -> String {
        self.json::<ErrorResponse>().error
    }
}

impl Api {
    fn new(o: Opts) -> Api {
        let k = o.keys.then(keys);
        let mut repo = Repo::new(
            Arc::new(MemoryBlobStore::new()),
            Arc::new(MemoryMetaStore::new()),
        );
        if let Some(k) = k.clone().filter(|_| !o.require.is_empty()) {
            repo = repo.with_land_policy(Arc::new(RequireSignatures::new(
                k,
                o.require.iter().map(|s| s.to_string()),
            )));
        }
        let auth = o.auth.then(|| {
            Auth::parse(
                &serde_json::json!({ "tokens": [
                    { "name": "viewer", "role": "read", "hash": hash_token(READ) },
                    { "name": "ci", "role": "write", "hash": hash_token(WRITE) },
                ]})
                .to_string(),
            )
            .unwrap()
        });
        Api {
            app: leep_server::router_with_signing(repo.clone(), 1 << 20, auth, k),
            repo,
        }
    }

    fn open() -> Api {
        Api::new(Opts {
            keys: true,
            auth: false,
            require: &[],
        })
    }

    async fn call(&self, method: Method, uri: &str, token: Option<&str>, body: Vec<u8>) -> Reply {
        let mut b = Request::builder().method(method).uri(uri);
        if let Some(t) = token {
            b = b.header(header::AUTHORIZATION, format!("Bearer {t}"));
        }
        let resp = self
            .app
            .clone()
            .oneshot(b.body(Body::from(body)).unwrap())
            .await
            .unwrap();
        let (parts, body) = resp.into_parts();
        Reply {
            status: parts.status,
            body: body.collect().await.unwrap().to_bytes().to_vec(),
        }
    }

    async fn post<T: serde::Serialize>(&self, uri: &str, body: &T) -> Reply {
        self.call(Method::POST, uri, None, serde_json::to_vec(body).unwrap())
            .await
    }

    async fn get(&self, uri: &str) -> Reply {
        self.call(Method::GET, uri, None, vec![]).await
    }

    /// Create a changeset through the API.
    async fn commit(&self, parents: &[ChangesetId], path: &str, text: &str) -> ChangesetId {
        let content = self
            .repo
            .blobs()
            .put(Bytes::copy_from_slice(text.as_bytes()))
            .await
            .unwrap();
        let cs = Changeset {
            parents: parents.to_vec(),
            author: "t".into(),
            timestamp: 1,
            message: format!("write {path}"),
            changes: BTreeMap::from([(
                RepoPath::new(path).unwrap(),
                FileChange::Modify {
                    content,
                    kind: FileKind::Regular,
                    size: text.len() as u64,
                },
            )]),
            extra: BTreeMap::new(),
        };
        let r = self.post("/v1/changesets", &cs).await;
        assert_eq!(
            r.status,
            StatusCode::OK,
            "{}",
            String::from_utf8_lossy(&r.body)
        );
        r.json::<CreateChangesetResponse>().id
    }

    async fn sign(&self, id: ChangesetId, k: &SigningKey) -> Reply {
        self.post(&format!("/v1/changesets/{id}/signatures"), &request(id, k))
            .await
    }

    async fn signatures(&self, id: ChangesetId) -> SignaturesResponse {
        let r = self.get(&format!("/v1/changesets/{id}/signatures")).await;
        assert_eq!(r.status, StatusCode::OK);
        r.json()
    }

    async fn land(&self, id: ChangesetId, target: &str) -> Reply {
        self.post(
            "/v1/land",
            &LandRequest {
                changeset: id,
                target_ref: target.into(),
            },
        )
        .await
    }
}

fn request(id: ChangesetId, k: &SigningKey) -> AddSignatureRequest {
    AddSignatureRequest {
        public_key: pk(k),
        signature: hex::encode(k.sign(&signing_message(&id)).to_bytes()),
    }
}

#[tokio::test]
async fn valid_signature_is_accepted_listed_and_idempotent() {
    let api = Api::open();
    let c = api.commit(&[], "a", "1\n").await;
    assert!(api.signatures(c).await.signatures.is_empty());

    let r = api.sign(c, &alice()).await;
    assert_eq!(r.status, StatusCode::OK);
    let ok: AddSignatureResponse = r.json();
    assert_eq!(ok.name, "alice");
    assert_eq!(ok.key_id, key_id(alice().verifying_key().as_bytes()));
    // Same signature again, and a second signer.
    assert_eq!(api.sign(c, &alice()).await.status, StatusCode::OK);
    assert_eq!(api.sign(c, &bob()).await.status, StatusCode::OK);

    let list = api.signatures(c).await;
    assert!(list.verified);
    assert_eq!(list.via, None);
    let names: Vec<_> = list.signatures.iter().map(|s| s.name.as_str()).collect();
    assert_eq!(names, ["alice", "bob"]);
    assert_eq!(list.signatures[0].public_key, pk(&alice()));
    assert_eq!(list.signatures[0].key_id, ok.key_id);

    // Another changeset is untouched.
    let other = api.commit(&[c], "b", "2\n").await;
    assert!(api.signatures(other).await.signatures.is_empty());
}

#[tokio::test]
async fn bad_signature_is_rejected_and_not_stored() {
    let api = Api::open();
    let c = api.commit(&[], "a", "1\n").await;
    let other = api.commit(&[c], "b", "2\n").await;

    // A real signature, but over a different changeset.
    let mut req = request(other, &alice());
    let r = api
        .post(&format!("/v1/changesets/{c}/signatures"), &req)
        .await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);
    assert!(r.error().contains("does not verify"), "{}", r.error());

    // A flipped bit.
    req = request(c, &alice());
    let mut sig = hex::decode(&req.signature).unwrap();
    sig[10] ^= 1;
    req.signature = hex::encode(sig);
    let r = api
        .post(&format!("/v1/changesets/{c}/signatures"), &req)
        .await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);

    // Signed with a different key than the one claimed.
    req = request(c, &bob());
    req.public_key = pk(&alice());
    let r = api
        .post(&format!("/v1/changesets/{c}/signatures"), &req)
        .await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);

    // The signed bytes are domain separated: the bare id is not accepted.
    let mut req = request(c, &alice());
    req.signature = hex::encode(alice().sign(c.0.as_bytes()).to_bytes());
    let r = api
        .post(&format!("/v1/changesets/{c}/signatures"), &req)
        .await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);

    assert!(api.signatures(c).await.signatures.is_empty());
}

#[tokio::test]
async fn untrusted_key_is_rejected() {
    let api = Api::open();
    let c = api.commit(&[], "a", "1\n").await;
    let r = api.sign(c, &mallory()).await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);
    assert!(r.error().contains("not trusted"), "{}", r.error());
    assert!(api.signatures(c).await.signatures.is_empty());
}

#[tokio::test]
async fn malformed_requests_and_unknown_changesets() {
    let api = Api::open();
    let c = api.commit(&[], "a", "1\n").await;
    let missing = ChangesetId(Hash::of_blob(b"nope"));

    let r = api.sign(missing, &alice()).await;
    assert_eq!(r.status, StatusCode::NOT_FOUND);
    let r = api
        .get(&format!("/v1/changesets/{missing}/signatures"))
        .await;
    assert_eq!(r.status, StatusCode::NOT_FOUND);
    let r = api.get("/v1/changesets/zz/signatures").await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);

    let uri = format!("/v1/changesets/{c}/signatures");
    let good = request(c, &alice());
    for bad in [
        AddSignatureRequest {
            public_key: "nonsense".into(),
            ..good.clone()
        },
        AddSignatureRequest {
            public_key: "ed25519:00".into(),
            ..good.clone()
        },
        AddSignatureRequest {
            signature: "00".into(),
            ..good.clone()
        },
    ] {
        assert_eq!(api.post(&uri, &bad).await.status, StatusCode::BAD_REQUEST);
    }
    let r = api
        .call(Method::POST, &uri, None, b"{not json".to_vec())
        .await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn server_without_keys_refuses_signatures_but_lists_none() {
    let api = Api::new(Opts {
        keys: false,
        auth: false,
        require: &[],
    });
    let c = api.commit(&[], "a", "1\n").await;
    let r = api.sign(c, &alice()).await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);
    assert!(r.error().contains("no signing keys"), "{}", r.error());
    let list = api.signatures(c).await;
    assert!(list.signatures.is_empty() && list.verified);
}

#[tokio::test]
async fn listing_reverifies_and_drops_corrupted_rows() {
    let api = Api::open();
    let c = api.commit(&[], "a", "1\n").await;
    api.sign(c, &alice()).await;
    // Bob's row is stored directly with a signature that does not verify, as
    // if the metadata store had been tampered with.
    let bad = Signature {
        key_id: key_id(bob().verifying_key().as_bytes()),
        public_key: *bob().verifying_key().as_bytes(),
        sig: [7; 64],
    };
    api.repo.put_signature(c, &bad).await.unwrap();
    let list = api.signatures(c).await;
    assert!(!list.verified);
    let names: Vec<_> = list.signatures.iter().map(|s| s.name.as_str()).collect();
    assert_eq!(names, ["alice"]);
}

#[tokio::test]
async fn auth_read_token_lists_but_cannot_sign() {
    let api = Api::new(Opts {
        keys: true,
        auth: true,
        require: &[],
    });
    let c = {
        let content = api
            .repo
            .blobs()
            .put(Bytes::from_static(b"x"))
            .await
            .unwrap();
        api.repo
            .create_changeset(Changeset {
                parents: vec![],
                author: "t".into(),
                timestamp: 1,
                message: "m".into(),
                changes: BTreeMap::from([(
                    RepoPath::new("a").unwrap(),
                    FileChange::Modify {
                        content,
                        kind: FileKind::Regular,
                        size: 1,
                    },
                )]),
                extra: BTreeMap::new(),
            })
            .await
            .unwrap()
    };
    let uri = format!("/v1/changesets/{c}/signatures");
    let body = serde_json::to_vec(&request(c, &alice())).unwrap();

    let r = api.call(Method::GET, &uri, None, vec![]).await;
    assert_eq!(r.status, StatusCode::UNAUTHORIZED);
    let r = api.call(Method::POST, &uri, None, body.clone()).await;
    assert_eq!(r.status, StatusCode::UNAUTHORIZED);

    let r = api.call(Method::GET, &uri, Some(READ), vec![]).await;
    assert_eq!(r.status, StatusCode::OK);
    let r = api.call(Method::POST, &uri, Some(READ), body.clone()).await;
    assert_eq!(r.status, StatusCode::FORBIDDEN);
    assert!(api
        .repo
        .get_signature(c, &key_id(alice().verifying_key().as_bytes()))
        .await
        .unwrap()
        .is_none());

    let r = api.call(Method::POST, &uri, Some(WRITE), body).await;
    assert_eq!(r.status, StatusCode::OK);
    let r = api.call(Method::GET, &uri, Some(READ), vec![]).await;
    assert_eq!(r.json::<SignaturesResponse>().signatures.len(), 1);
}

fn policy_api() -> Api {
    Api::new(Opts {
        keys: true,
        auth: false,
        require: &["main"],
    })
}

#[tokio::test]
async fn require_signatures_end_to_end() {
    let api = policy_api();
    let c = api.commit(&[], "a", "1\n").await;

    let r = api.land(c, "main").await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);
    assert_eq!(
        r.error(),
        format!("invalid input: changeset {c} {NOT_SIGNED_MARKER}")
    );
    let refs: RefsResponse = api.get("/v1/refs").await.json();
    assert!(refs.refs.is_empty(), "ref must not move");

    // Unprotected refs are unaffected.
    assert_eq!(api.land(c, "scratch").await.status, StatusCode::OK);

    // Raw ref CAS cannot skip the policy.
    let cas = |new| CasRefRequest {
        expected: None,
        new: Some(new),
    };
    let r = api.post("/v1/refs/main", &cas(c)).await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);
    assert!(r.error().contains(NOT_SIGNED_MARKER));
    assert!(api.repo.meta().get_ref("main").await.unwrap().is_none());
    assert_eq!(
        api.post("/v1/refs/scratch2", &cas(c)).await.status,
        StatusCode::OK
    );

    // A signature by an untrusted key leaves it unsigned.
    assert_eq!(
        api.sign(c, &mallory()).await.status,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(api.land(c, "main").await.status, StatusCode::BAD_REQUEST);

    assert_eq!(api.sign(c, &alice()).await.status, StatusCode::OK);
    let r = api.land(c, "main").await;
    assert_eq!(r.status, StatusCode::OK);
    assert!(matches!(
        r.json::<LandResponse>(),
        LandResponse::Landed { landed, rebased: false, .. } if landed == c
    ));
}

#[tokio::test]
async fn require_signatures_checks_every_stack_entry_and_rebased_copies_report_via() {
    let api = policy_api();
    let base = api.commit(&[], "a", "1\n").await;
    api.sign(base, &alice()).await;
    assert_eq!(api.land(base, "main").await.status, StatusCode::OK);
    // The head moves on, so landing the stack below has to replay it.
    let head = api.commit(&[base], "h", "h\n").await;
    api.sign(head, &bob()).await;
    assert_eq!(api.land(head, "main").await.status, StatusCode::OK);

    let s1 = api.commit(&[base], "b", "2\n").await;
    let s2 = api.commit(&[s1], "c", "3\n").await;
    api.sign(s2, &alice()).await;
    let r = api.land(s2, "main").await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);
    assert!(r
        .error()
        .contains(&format!("changeset {s1} {NOT_SIGNED_MARKER}")));

    api.sign(s1, &bob()).await;
    let LandResponse::Landed {
        landed, rebased, ..
    } = api.land(s2, "main").await.json::<LandResponse>()
    else {
        panic!("expected land");
    };
    assert!(rebased);

    // The rebased copy has no signature of its own; the listing says whose it
    // borrows.
    let list = api.signatures(landed).await;
    assert_eq!(list.via, Some(s2));
    assert_eq!(list.signatures.len(), 1);
    assert_eq!(list.signatures[0].name, "alice");
    assert!(list.verified);
    // Signed changesets report no `via`.
    assert_eq!(api.signatures(s2).await.via, None);
}

#[tokio::test]
async fn a_forged_rebased_from_key_neither_lands_nor_reports_via() {
    let api = policy_api();
    let signed = api.commit(&[], "a", "1\n").await;
    api.sign(signed, &alice()).await;

    let content = api
        .repo
        .blobs()
        .put(Bytes::from_static(b"evil"))
        .await
        .unwrap();
    let forged = Changeset {
        parents: vec![],
        author: "t".into(),
        timestamp: 1,
        message: "forged".into(),
        changes: BTreeMap::from([(
            RepoPath::new("evil").unwrap(),
            FileChange::Modify {
                content,
                kind: FileKind::Regular,
                size: 4,
            },
        )]),
        extra: BTreeMap::from([(
            leep_core::changeset::EXTRA_REBASED_FROM.to_string(),
            signed.to_string(),
        )]),
    };
    let id = api
        .post("/v1/changesets", &forged)
        .await
        .json::<CreateChangesetResponse>()
        .id;
    assert_eq!(api.land(id, "main").await.status, StatusCode::BAD_REQUEST);
    let list = api.signatures(id).await;
    assert!(list.signatures.is_empty() && list.via.is_none());
}

#[test]
fn server_refuses_to_start_with_bad_signature_config() {
    let dir = tempfile::tempdir().unwrap();
    let run = |args: &[&str]| {
        std::process::Command::new(env!("CARGO_BIN_EXE_leep-server"))
            .args([
                "--meta",
                "mem://",
                "--blobs",
                "mem://",
                "--listen",
                "127.0.0.1:0",
            ])
            .args(args)
            .env_remove("LEEP_SIGNING_KEYS_FILE")
            .env_remove("LEEP_REQUIRE_SIGNATURES")
            .output()
            .unwrap()
    };
    let msg = |o: &std::process::Output| String::from_utf8_lossy(&o.stderr).into_owned();

    let out = run(&["--require-signatures", "main"]);
    assert!(!out.status.success());
    assert!(msg(&out).contains("--signing-keys-file"), "{}", msg(&out));

    let bad = dir.path().join("keys.json");
    std::fs::write(&bad, r#"{"keys":[{"name":"a","public_key":"ed25519:00"}]}"#).unwrap();
    let out = run(&["--signing-keys-file", bad.to_str().unwrap()]);
    assert!(!out.status.success());
    assert!(msg(&out).contains("keys.json"), "{}", msg(&out));

    let out = run(&["--signing-keys-file", "/nonexistent/keys.json"]);
    assert!(!out.status.success());
    assert!(
        msg(&out).contains("cannot read signing keys file"),
        "{}",
        msg(&out)
    );
}
