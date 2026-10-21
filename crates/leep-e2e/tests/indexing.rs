//! One indexer per index group (ADR 0058) with the real binaries: servers
//! in `--indexing lease` mode share the work by lease, a SIGKILLed holder is
//! replaced within the stale time plus a renewal, a server in `--indexing
//! off` answers path logs correctly throughout a land loop, and the CLI
//! reports an indexing 503 it cannot wait out.
//!
//! Needs the binaries built (`cargo build --workspace`); the lease variants
//! need `LEEP_TEST_POSTGRES_URL`.

mod common;

use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use common::*;
use leep_core::{Changeset, ChangesetId, FileChange, FileKind, Hash, RepoPath};

/// Files the land loop edits in turn.
const FILES: usize = 4;

/// GET with an optional `Leep-Indexing` header: status and body.
fn get(addr: SocketAddr, path: &str, indexing: Option<&str>) -> (u16, String) {
    let mut s = TcpStream::connect(addr).expect("connect");
    s.set_read_timeout(Some(Duration::from_secs(60))).unwrap();
    let h = indexing.map_or(String::new(), |v| format!("Leep-Indexing: {v}\r\n"));
    let req = format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n{h}\r\n");
    s.write_all(req.as_bytes()).unwrap();
    let mut resp = Vec::new();
    s.read_to_end(&mut resp).unwrap();
    let text = String::from_utf8_lossy(&resp).into_owned();
    let status = text.split_whitespace().nth(1).unwrap().parse().unwrap();
    let body = text
        .split("\r\n\r\n")
        .nth(1)
        .unwrap_or_default()
        .to_string();
    (status, body)
}

/// What the land loop landed: each landed id, oldest first, with the file
/// it changed.
#[derive(Default)]
struct Landed {
    ids: Vec<(ChangesetId, usize)>,
}

impl Landed {
    /// The path log of `f{file}` at the `upto`th landed changeset.
    fn log(&self, upto: usize, file: usize) -> Vec<String> {
        self.ids[..upto]
            .iter()
            .rev()
            .filter(|(_, f)| *f == file)
            .map(|(id, _)| id.to_string())
            .collect()
    }
}

/// Commit one file edit on `parent` and land it on `main` through `addr`.
fn land_one(addr: SocketAddr, parent: Option<ChangesetId>, k: usize) -> ChangesetId {
    let body = format!("step {k}\n");
    let h = Hash::of_blob(body.as_bytes());
    let (status, _) = http(addr, "PUT", &format!("/v1/blobs/{h}"), body.as_bytes()).unwrap();
    assert_eq!(status, 204);
    let cs = Changeset {
        parents: parent.into_iter().collect(),
        author: format!("agent{}", k % 3),
        timestamp: k as i64,
        message: format!("step {k}"),
        changes: BTreeMap::from([(
            RepoPath::new(format!("dir/f{}", k % FILES)).unwrap(),
            FileChange::Modify {
                content: h,
                kind: FileKind::Regular,
                size: body.len() as u64,
            },
        )]),
        extra: BTreeMap::new(),
    };
    let (status, created) = post_json(addr, "/v1/changesets", &serde_json::to_string(&cs).unwrap());
    assert_eq!(status, 200, "{created}");
    let id: ChangesetId = serde_json::from_str::<serde_json::Value>(&created).unwrap()["id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();
    let req = serde_json::json!({ "changeset": id, "target_ref": "main" });
    let (status, landed) = post_json(addr, "/v1/land", &req.to_string());
    assert_eq!(status, 200, "{landed}");
    let v: serde_json::Value = serde_json::from_str(&landed).unwrap();
    assert_eq!(v["status"], "landed", "{landed}");
    v["landed"].as_str().unwrap().parse().unwrap()
}

/// The live holder of an index group's lease, by advertised address.
fn index_holder(meta_url: &str, kind: &str) -> Option<String> {
    use sqlx::{Connection, Row};
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let mut c = sqlx::PgConnection::connect(meta_url).await.ok()?;
        let row = sqlx::query(
            "SELECT address FROM leases WHERE scope = 'index' AND repo_id = 0 AND name = $1 \
             AND shard = 0 AND renewed_at + 6000 > (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT \
             AND expires_at > (EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT",
        )
        .bind(kind)
        .fetch_optional(&mut c)
        .await
        .ok()??;
        Some(row.get::<String, _>("address"))
    })
}

/// Check a path log answer from a query server against the land loop's.
fn check_log(tag: &str, addr: SocketAddr, landed: &Mutex<Landed>, header: Option<&str>) -> bool {
    let (upto, head) = {
        let l = landed.lock().unwrap();
        match l.ids.last() {
            Some((id, _)) => (l.ids.len(), *id),
            None => return true,
        }
    };
    let file = upto % FILES;
    let (status, body) = get(
        addr,
        &format!("/v1/log/{head}?path=dir/f{file}&limit=10000"),
        header,
    );
    let v: serde_json::Value =
        serde_json::from_str(&body).unwrap_or_else(|e| panic!("[{tag}] {e}: {body}"));
    match status {
        200 => {
            let got: Vec<String> = v["entries"]
                .as_array()
                .unwrap()
                .iter()
                .map(|e| e["id"].as_str().unwrap().to_string())
                .collect();
            assert_eq!(
                got,
                landed.lock().unwrap().log(upto, file),
                "[{tag}] log of f{file}"
            );
            true
        }
        503 if header.is_some() => {
            assert_eq!(v["indexing"]["kind"], "history-tree-v4", "[{tag}] {body}");
            false
        }
        s => panic!("[{tag}] {s}: {body}"),
    }
}

/// Two servers in `lease` mode and a query server in `off` mode on one
/// Postgres, under a steady land loop. The query server's path logs are
/// right throughout, with and without the header; each round SIGKILLs the
/// server holding the path-history lease, and the other takes it within the
/// stale time (6 s) plus a renewal (3 s), then catches the index up.
#[test]
fn killed_index_holder_is_replaced_and_queries_keep_answering_postgres() {
    let Some(db) = PgDb::create() else {
        return;
    };
    let _g = serial();
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    let stores = Stores::with_meta(&root.join("data"), db.url.clone());
    let ports: Vec<u16> = (0..2).map(|_| free_port()).collect();
    let start = |port: u16| {
        let url = format!("http://127.0.0.1:{port}");
        Server::start_with(
            port,
            &stores,
            root,
            &["--indexing", "lease", "--advertise-url", &url],
        )
    };
    let mut indexers: Vec<Server> = ports.iter().map(|p| start(*p)).collect();
    let query = Server::start_with(
        free_port(),
        &stores,
        root,
        &[
            "--indexing",
            "off",
            "--query-scan-budget",
            "64",
            "--index-wait",
            "2s",
        ],
    );
    let landed = Mutex::new(Landed::default());
    let stop = AtomicBool::new(false);
    let mut takeovers = Vec::new();
    let mut refused = 0;
    std::thread::scope(|s| {
        s.spawn(|| {
            let mut parent = None;
            let mut k = 0;
            while !stop.load(Ordering::Relaxed) {
                let id = land_one(query.addr, parent, k);
                landed.lock().unwrap().ids.push((id, k % FILES));
                parent = Some(id);
                k += 1;
                std::thread::sleep(Duration::from_millis(30));
            }
        });
        for round in 0..3 {
            let tag = format!("round {round}");
            let until = Instant::now() + Duration::from_secs(3);
            while Instant::now() < until {
                check_log(&tag, query.addr, &landed, None);
                if !check_log(&tag, query.addr, &landed, Some("retry")) {
                    refused += 1;
                }
                std::thread::sleep(Duration::from_millis(100));
            }
            let holder = index_holder(&db.url, "history-tree-v4")
                .unwrap_or_else(|| panic!("[{tag}] nobody holds the path-history lease"));
            let i = ports
                .iter()
                .position(|p| holder == format!("http://127.0.0.1:{p}"))
                .unwrap_or_else(|| panic!("[{tag}] lease names {holder}"));
            indexers[i].kill();
            let killed = Instant::now();
            let new = loop {
                // Queries go on meanwhile; without the header they wait or
                // scan, and stay right.
                check_log(&tag, query.addr, &landed, None);
                match index_holder(&db.url, "history-tree-v4") {
                    Some(h) if h != holder => break h,
                    _ => {}
                }
                assert!(
                    killed.elapsed() < Duration::from_secs(12),
                    "[{tag}] no takeover within 12 s"
                );
                std::thread::sleep(Duration::from_millis(100));
            };
            let took = killed.elapsed();
            assert!(
                took <= Duration::from_secs(10),
                "[{tag}] takeover took {took:?}"
            );
            takeovers.push(took);
            eprintln!("[{tag}] {new} took the lease {took:?} after the SIGKILL");
            indexers[i] = start(ports[i]);
        }
        // The new holder catches up: an opted-in request is answered again.
        let deadline = Instant::now() + Duration::from_secs(20);
        while !check_log("final", query.addr, &landed, Some("retry")) {
            assert!(Instant::now() < deadline, "the index never caught up");
            std::thread::sleep(Duration::from_millis(200));
        }
        stop.store(true, Ordering::Relaxed);
    });
    eprintln!(
        "takeovers after SIGKILL: {takeovers:?}; indexing answers to retry requests: {refused}; lands: {}",
        landed.lock().unwrap().ids.len()
    );
    drop(indexers);
    drop(query);
    drop(db);
}

/// The CLI against a server whose index lags under a live lease: `leep log`
/// retries the 503 for `LEEP_INDEX_WAIT` and then fails with exit code 3
/// and error code `indexing`, saying how far behind the index is; with
/// `--stale-ok` it answers at the newest indexed changeset. Once no lease
/// lives, the same query derives inline and succeeds.
#[test]
fn the_cli_reports_an_indexing_503_and_takes_stale_answers_postgres() {
    let Some(db) = PgDb::create() else {
        return;
    };
    let _g = serial();
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    let stores = Stores::with_meta(&root.join("data"), db.url.clone());
    let port = free_port();
    let url = format!("http://127.0.0.1:{port}");
    let mut indexer = Server::start_with(
        port,
        &stores,
        root,
        &["--indexing", "lease", "--advertise-url", &url],
    );
    let query = Server::start_with(
        free_port(),
        &stores,
        root,
        &[
            "--indexing",
            "off",
            "--query-scan-budget",
            "0",
            "--legacy-scan-factor",
            "1",
            "--index-wait",
            "0",
        ],
    );
    let mut parent = None;
    for k in 0..4 {
        parent = Some(land_one(query.addr, parent, k));
    }
    let deadline = Instant::now() + Duration::from_secs(10);
    while index_holder(&db.url, "history-tree-v4").is_none()
        || get(
            query.addr,
            &format!("/v1/log/{}?path=dir/f1", parent.unwrap()),
            Some("retry"),
        )
        .0 != 200
    {
        assert!(Instant::now() < deadline, "the index never caught up");
        std::thread::sleep(Duration::from_millis(100));
    }
    let indexed = parent.unwrap();
    // The indexer dies; its lease stays live for 6 s while more lands come.
    indexer.kill();
    let killed = Instant::now();
    for k in 4..8 {
        parent = Some(land_one(query.addr, parent, k));
    }
    let client = Client::new(&query.url, &root.join("cache"));
    let out = client
        .cmd(root)
        .env("LEEP_INDEX_WAIT", "1")
        .args(["--json", "log", "main", "--path", "dir/f1"])
        .output()
        .unwrap();
    assert!(
        killed.elapsed() < Duration::from_secs(5),
        "too slow to see the live lease"
    );
    assert_eq!(out.status.code(), Some(3), "{}", stderr(&out));
    let err: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(err["error"]["code"], "indexing", "{err}");
    assert_eq!(err["error"]["retryable"], true);
    assert!(err["error"]["retry_after_ms"].as_u64().unwrap() >= 1000);
    assert!(
        stderr(&out).contains("indexing: 4 changesets behind"),
        "{}",
        stderr(&out)
    );
    let stale = client.json(root, &["log", "main", "--path", "dir/f1", "--stale-ok"]);
    assert_eq!(stale["answered_at"], indexed.to_string());
    assert_eq!(stale["behind"], 4);
    // No live lease: the query derives inline and answers at the head.
    while killed.elapsed() < Duration::from_secs(7) {
        std::thread::sleep(Duration::from_millis(100));
    }
    let fresh = client.json(root, &["log", "main", "--path", "dir/f1"]);
    assert!(fresh.get("answered_at").is_none(), "{fresh}");
    assert_eq!(fresh["entries"].as_array().unwrap().len(), 2);
    drop(query);
    drop(db);
}

/// One server in `lease` mode on SQLite holds every lease, warms, and
/// answers bounded queries like `local` mode.
#[test]
fn a_single_lease_mode_server_on_sqlite_indexes_and_answers() {
    let _g = serial();
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    let stores = Stores::sqlite(&root.join("data"));
    let server = Server::start_with(free_port(), &stores, root, &["--indexing", "lease"]);
    let landed = Mutex::new(Landed::default());
    let mut parent = None;
    for k in 0..30 {
        let id = land_one(server.addr, parent, k);
        landed.lock().unwrap().ids.push((id, k % FILES));
        parent = Some(id);
        assert!(check_log("sqlite", server.addr, &landed, None));
    }
    let deadline = Instant::now() + Duration::from_secs(10);
    while !check_log("sqlite", server.addr, &landed, Some("retry")) {
        assert!(Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(100));
    }
    let (_, ready) = get(server.addr, "/v1/ready", None);
    assert!(ready.contains("indexing-retry"), "{ready}");
}
