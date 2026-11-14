//! Search row retention in the server (ADR 0102): two servers on one store,
//! with the passes set to run within seconds, retire the blob rows of
//! versions no head holds, keep every row of the head, and a search of an
//! old revision derives what it needs again. Runs on Postgres when
//! `LEEP_TEST_POSTGRES_URL` is set, on SQLite otherwise; run `cargo build
//! --workspace` first.

mod common;

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use common::*;
use leep_core::api::{CreateChangesetResponse, LandRequest, LandResponse, SearchResponse};
use leep_core::{Changeset, ChangesetId, FileChange, FileKind, Hash, RepoPath};

/// Passes every second, the first a second after start, a window of a
/// second and no allowance for clock skew.
const FLAGS: &[&str] = &[
    "--search-retention",
    "1s",
    "--search-retention-interval",
    "1s",
    "--debug-search-retention-first",
    "1s",
    "--debug-search-retention-skew",
    "0s",
];

fn land(addr: SocketAddr, parent: Option<ChangesetId>, path: &str, body: &str) -> ChangesetId {
    let h = Hash::of_blob(body.as_bytes());
    let (status, _) = http(addr, "PUT", &format!("/v1/blobs/{h}"), body.as_bytes()).unwrap();
    assert_eq!(status, 204);
    let mut changes = BTreeMap::new();
    changes.insert(
        RepoPath::new(path).unwrap(),
        FileChange::Modify {
            content: h,
            kind: FileKind::Regular,
            size: body.len() as u64,
        },
    );
    let cs = Changeset {
        parents: parent.into_iter().collect(),
        author: "Dev <dev@example.org>".into(),
        timestamp: 1_700_000_000,
        message: "edit".into(),
        changes,
        extra: BTreeMap::new(),
    };
    let (status, body) = http(
        addr,
        "POST",
        "/v1/changesets",
        &serde_json::to_vec(&cs).unwrap(),
    )
    .unwrap();
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
    let made: CreateChangesetResponse = serde_json::from_slice(&body).unwrap();
    let req = LandRequest {
        changeset: made.id,
        target_ref: "main".into(),
        create: parent.is_none().then_some(true),
        land_id: None,
    };
    let (status, body) =
        http(addr, "POST", "/v1/land", &serde_json::to_vec(&req).unwrap()).unwrap();
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
    match serde_json::from_slice(&body).unwrap() {
        LandResponse::Landed { landed, .. } => landed,
        other => panic!("{other:?}"),
    }
}

fn search(addr: SocketAddr, rev: &str, q: &str) -> SearchResponse {
    let (status, _, body) = http_get(addr, &format!("/v1/search/{rev}?q={q}"), None);
    assert_eq!(status, 200, "{body}");
    serde_json::from_str(&body).unwrap()
}

/// The sum of a metric's samples whose labels contain every one of `labels`.
fn metric(addr: SocketAddr, name: &str, labels: &[&str]) -> f64 {
    let (status, _, body) = http_get(addr, "/metrics", None);
    assert_eq!(status, 200);
    body.lines()
        .filter(|l| l.starts_with(name) && labels.iter().all(|x| l.contains(x)))
        .filter_map(|l| l.rsplit_once(' ')?.1.parse::<f64>().ok())
        .sum()
}

fn wait_for(what: &str, secs: u64, mut done: impl FnMut() -> bool, log: impl Fn() -> String) {
    let deadline = Instant::now() + Duration::from_secs(secs);
    while !done() {
        assert!(Instant::now() < deadline, "timed out: {what}\n{}", log());
        std::thread::sleep(Duration::from_millis(250));
    }
}

#[test]
fn servers_retire_rows_no_head_holds_and_keep_the_heads() {
    let _g = serial();
    let tmp = tempfile::tempdir().unwrap();
    let work = tmp.path();
    let pg = PgDb::create("leep_e2e");
    let stores = match &pg {
        Some(db) => Stores::with_meta(&work.join("data"), db.url.clone()),
        None => Stores::sqlite(&work.join("data")),
    };
    assert_ok(&server_cmd(&stores, &["migrate"]), "leep-server migrate");
    let mut servers = Vec::new();
    for _ in 0..2 {
        let metrics_port = free_port();
        let listen = format!("127.0.0.1:{metrics_port}");
        let mut flags = FLAGS.to_vec();
        flags.extend(["--metrics-listen", &listen]);
        let s = Server::start_with(free_port(), &stores, work, &flags);
        servers.push((s, listen.parse::<SocketAddr>().unwrap()));
    }
    let addr = servers[0].0.addr;
    let mut body = String::new();
    let mut tip = None;
    let mut first = None;
    for i in 0..6 {
        body.push_str(&format!("fn version_{i}() {{ marker_{i}_alpha }}\n"));
        let id = land(addr, tip, "src/lib.rs", &body);
        first.get_or_insert(id);
        tip = Some(id);
        // Each head warmed, so each version has its row.
        wait_for(
            "the head warmed",
            30,
            || {
                search(addr, "main", "marker_0_alpha")
                    .stats
                    .blob_rows_derived
                    == 0
            },
            || servers[0].0.log_text(),
        );
        std::thread::sleep(Duration::from_millis(300));
    }
    let deleted = |m: SocketAddr| {
        metric(
            m,
            "leep_search_retention_rows_total",
            &["kind=\"blob-trigrams-v2\"", "outcome=\"deleted\""],
        )
    };
    wait_for(
        "a pass deleted the old versions' rows",
        60,
        || deleted(servers[0].1) + deleted(servers[1].1) >= 5.0,
        || servers[0].0.log_text() + &servers[1].0.log_text(),
    );
    // The head's row stayed; one old version's is derived again.
    let at_head = search(addr, "main", "marker_5_alpha");
    assert_eq!(at_head.matches.len(), 1);
    assert_eq!(at_head.stats.blob_rows_derived, 0);
    let old = search(addr, &first.unwrap().to_string(), "marker_0_alpha");
    assert_eq!(old.matches.len(), 1);
    assert_eq!(old.stats.blob_rows_derived, 1, "derived again on demand");
    // Both servers ran passes, one at a time under the lease, and neither
    // failed.
    for (s, m) in &servers {
        assert_eq!(
            metric(
                *m,
                "leep_search_retention_passes_total",
                &["outcome=\"error\""]
            ),
            0.0,
            "{}",
            s.log_text()
        );
        assert!(
            metric(*m, "leep_search_retention_passes_total", &[]) >= 1.0,
            "{}",
            s.log_text()
        );
    }
    drop(servers);
    let out = server_cmd(&stores, &["verify", "--full"]);
    assert_ok(&out, "leep-server verify --full");
    drop(pg);
}
