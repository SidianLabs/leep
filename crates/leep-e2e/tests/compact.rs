//! `leep-server compact-changesets` through the real binary (ADR 0052): a
//! dry run changes nothing, a run trains a dictionary and compresses every
//! row, `verify --full` stays clean, and a server serves the history and
//! keeps writing compressed rows.

mod common;

use std::collections::BTreeMap;
use std::process::Output;

use common::*;
use leep_core::{Changeset, ChangesetId, FileChange, FileKind, RepoPath};

const CHANGESETS: usize = 1100;

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

/// A first-parent chain of small changesets on `main`; returns the head.
fn populate(stores: &Stores) -> ChangesetId {
    rt().block_on(async {
        let blobs = leep_store::open_blob_store(&stores.blobs).unwrap();
        let meta = leep_meta::open_meta_store(&stores.meta).await.unwrap();
        let repo = leep_derive::Repo::new(blobs, meta.clone());
        let mut tip: Option<ChangesetId> = None;
        for i in 0..CHANGESETS {
            let body = format!("fn f{i}() {{}}\n");
            let h = repo.put_blob(body.clone().into()).await.unwrap();
            let changes = BTreeMap::from([(
                RepoPath::new(format!("src/mod{}/f{}.rs", i % 7, i % 23)).unwrap(),
                FileChange::Modify {
                    content: h,
                    kind: FileKind::Regular,
                    size: body.len() as u64,
                },
            )]);
            let id = repo
                .create_changeset(Changeset {
                    parents: tip.into_iter().collect(),
                    author: format!("Dev {} <dev{}@example.org>", i % 5, i % 5),
                    timestamp: 1_700_000_000 + i as i64,
                    message: format!(
                        "mod{}: change {i}\n\nSigned-off-by: Dev {} <dev{}@example.org>\n",
                        i % 7,
                        i % 5,
                        i % 5
                    ),
                    changes,
                    extra: BTreeMap::new(),
                })
                .await
                .unwrap();
            tip = Some(id);
        }
        assert!(meta.cas_ref("main", None, tip).await.unwrap());
        tip.unwrap()
    })
}

/// Stored bytes of every changeset row and the number of dictionaries.
fn stored(stores: &Stores) -> (Vec<Vec<u8>>, i64) {
    use sqlx::Connection;
    rt().block_on(async {
        let rows = "SELECT data FROM changesets ORDER BY id";
        let dicts = "SELECT COUNT(*) FROM changeset_dicts";
        if stores.meta.starts_with("sqlite:") {
            let mut c = sqlx::SqliteConnection::connect(&stores.meta).await.unwrap();
            (
                sqlx::query_scalar(rows).fetch_all(&mut c).await.unwrap(),
                sqlx::query_scalar(dicts).fetch_one(&mut c).await.unwrap(),
            )
        } else {
            let mut c = sqlx::PgConnection::connect(&stores.meta).await.unwrap();
            (
                sqlx::query_scalar(rows).fetch_all(&mut c).await.unwrap(),
                sqlx::query_scalar(dicts).fetch_one(&mut c).await.unwrap(),
            )
        }
    })
}

fn is_frame(d: &[u8]) -> bool {
    d.starts_with(&[0x28, 0xB5, 0x2F, 0xFD])
}

fn compact(stores: &Stores, args: &[&str]) -> Output {
    let out = server_cmd(stores, &[&["compact-changesets"][..], args].concat());
    assert_ok(&out, "compact-changesets");
    out
}

fn workflow(stores: &Stores, dir: &std::path::Path) {
    let head = populate(stores);
    let (plain, dicts) = stored(stores);
    assert_eq!((plain.len(), dicts), (CHANGESETS, 0));
    assert!(plain.iter().all(|d| !is_frame(d)));

    let out = compact(stores, &["--dry-run"]);
    assert!(
        stdout(&out).contains(&format!("would rewrite {CHANGESETS} of {CHANGESETS}")),
        "{}",
        stdout(&out)
    );
    assert_eq!(
        stored(stores),
        (plain.clone(), 0),
        "a dry run writes nothing"
    );

    let out = compact(stores, &[]);
    let text = stdout(&out);
    assert!(text.contains("stored dictionary 1"), "{text}");
    assert!(
        text.contains(&format!("rewrote {CHANGESETS} of {CHANGESETS}")),
        "{text}"
    );
    let (packed, dicts) = stored(stores);
    assert_eq!(dicts, 1);
    assert!(packed.iter().all(|d| is_frame(d)));
    let (a, b): (usize, usize) = (
        plain.iter().map(Vec::len).sum(),
        packed.iter().map(Vec::len).sum(),
    );
    assert!(b * 2 < a, "{a} -> {b}");

    // A second run has nothing to do.
    let out = compact(stores, &[]);
    assert!(stdout(&out).contains("rewrote 0 of"), "{}", stdout(&out));

    let out = server_cmd(stores, &["verify", "--full"]);
    assert_ok(&out, "verify --full");

    // A server reads the compressed history and writes compressed rows.
    let srv = Server::start(stores, dir);
    let (code, _, body) = http_get(srv.addr, &format!("/v1/log/{head}?limit=5000"), None);
    assert_eq!(code, 200, "{body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(
        v["entries"].as_array().unwrap().len(),
        CHANGESETS,
        "{body:.300}"
    );
    let req = serde_json::json!({
        "parents": [head.to_string()],
        "message": "after compaction\n\nSigned-off-by: Dev 1 <dev1@example.org>\n",
        "author": "Dev 1 <dev1@example.org>",
        "timestamp": 1_700_009_999,
        "changes": {},
        "extra": {},
    });
    let (code, body) = post_json(srv.addr, "/v1/changesets", &req.to_string());
    assert_eq!(code, 200, "{body}");
    drop(srv);
    let (after, _) = stored(stores);
    assert_eq!(after.len(), CHANGESETS + 1);
    assert!(after.iter().all(|d| is_frame(d)));
    let out = server_cmd(stores, &["verify", "--full"]);
    assert_ok(&out, "verify --full after a write");
}

#[test]
fn compact_changesets_on_sqlite() {
    let _g = serial();
    let tmp = tempfile::tempdir().unwrap();
    let stores = Stores::sqlite(&tmp.path().join("data"));
    workflow(&stores, tmp.path());
}

#[test]
fn compact_changesets_on_postgres() {
    let _g = serial();
    let Some(db) = PgDb::create() else {
        return;
    };
    let tmp = tempfile::tempdir().unwrap();
    let stores = Stores::with_meta(&tmp.path().join("data"), db.url.clone());
    workflow(&stores, tmp.path());
}
