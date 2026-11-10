//! Events across servers (ADR 0099), with the real binaries on SQLite and
//! Postgres: two servers share one store while agents declare sessions on
//! both at once, and a reader long-polls each server in turn, resuming
//! every time from a cursor the other server gave it. It must see every
//! event exactly once. `leep watch --once` reads the same events.

mod common;

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use common::*;

const WRITERS: usize = 4;
const PER_WRITER: usize = 30;

fn events(addr: std::net::SocketAddr, query: &str) -> serde_json::Value {
    let (status, _, body) = http_get(addr, &format!("/v1/events{query}"), None);
    assert_eq!(status, 200, "{body}");
    serde_json::from_str(&body).unwrap()
}

fn run_two_servers(stores: &Stores, root: &std::path::Path) {
    let a = Server::start(stores, root);
    let b = Server::start(stores, root);
    let tail = events(a.addr, "")["next"].as_str().unwrap().to_string();

    // Writers declare sessions on both servers at once; each declaration
    // is one `session.started`, appended by the server that took it.
    let done = Arc::new(AtomicBool::new(false));
    let writers: Vec<_> = (0..WRITERS)
        .map(|w| {
            let addrs = [a.addr, b.addr];
            std::thread::spawn(move || {
                for i in 0..PER_WRITER {
                    let body = serde_json::json!({
                        "session": format!("w{w}-{i}"),
                        "scope": [format!("src/w{w}/f{i}.rs")],
                    });
                    let (status, resp) =
                        post_json(addrs[(w + i) % 2], "/v1/sessions", &body.to_string());
                    assert_eq!(status, 200, "{resp}");
                }
            })
        })
        .collect();

    // The reader: each long-poll goes to the other server, from the cursor
    // the last one answered.
    let reader = {
        let (done, addrs) = (done.clone(), [a.addr, b.addr]);
        std::thread::spawn(move || {
            let mut seen: BTreeMap<String, usize> = BTreeMap::new();
            let mut cursors = Vec::new();
            let mut after = tail;
            let mut turn = 0;
            let deadline = Instant::now() + Duration::from_secs(90);
            loop {
                let page = events(
                    addrs[turn % 2],
                    &format!("?after={after}&kinds=session.started&wait=1"),
                );
                turn += 1;
                for e in page["events"].as_array().unwrap() {
                    *seen
                        .entry(e["subject"].as_str().unwrap().to_string())
                        .or_default() += 1;
                    cursors.push(e["cursor"].as_str().unwrap().to_string());
                }
                after = page["next"].as_str().unwrap().to_string();
                if seen.len() >= WRITERS * PER_WRITER && done.load(Ordering::SeqCst) {
                    // One more read on each server: nothing may follow.
                    for addr in addrs {
                        let more = events(addr, &format!("?after={after}&kinds=session.started"));
                        assert!(more["events"].as_array().unwrap().is_empty(), "{more}");
                    }
                    return (seen, cursors, turn);
                }
                assert!(Instant::now() < deadline, "saw {} events", seen.len());
            }
        })
    };
    for w in writers {
        w.join().unwrap();
    }
    done.store(true, Ordering::SeqCst);
    let (seen, cursors, turns) = reader.join().unwrap();
    assert_eq!(seen.len(), WRITERS * PER_WRITER, "no event lost");
    assert!(
        seen.values().all(|&n| n == 1),
        "no event twice: {:?}",
        seen.iter().filter(|(_, &n)| n > 1).collect::<Vec<_>>()
    );
    let seqs: Vec<u64> = cursors
        .iter()
        .map(|c| c.trim_start_matches('e').parse().unwrap())
        .collect();
    assert!(seqs.windows(2).all(|w| w[0] < w[1]), "in order");
    assert!(turns >= 2, "both servers answered");

    // The CLI reads the same events from either server.
    let client = Client::new(&b.url, &root.join("cache"));
    let out = client.ok(
        root,
        &[
            "--json",
            "watch",
            "--once",
            "--kind",
            "session.started",
            "--after",
            &format!("e{}", seqs[0] - 1),
        ],
    );
    assert_eq!(out.lines().count(), WRITERS * PER_WRITER, "{out}");
    let text = client.ok(
        root,
        &[
            "watch",
            "--once",
            "src/w0",
            "--after",
            &format!("e{}", seqs[0] - 1),
        ],
    );
    assert_eq!(text.lines().count(), PER_WRITER, "{text}");
    assert!(
        text.lines()
            .all(|l| l.contains("session.started") && l.contains("[\"src/w0/")),
        "{text}"
    );
}

#[test]
fn a_cursor_from_one_server_resumes_on_another() {
    let _g = serial();
    let tmp = tempfile::tempdir().unwrap();
    let stores = Stores::sqlite(&tmp.path().join("data"));
    run_two_servers(&stores, tmp.path());
}

#[test]
fn a_cursor_from_one_server_resumes_on_another_postgres() {
    let _g = serial();
    let Some(db) = PgDb::create("leep_e2e_events") else {
        return;
    };
    let tmp = tempfile::tempdir().unwrap();
    let stores = Stores::with_meta(&tmp.path().join("data"), db.url.clone());
    run_two_servers(&stores, tmp.path());
}
