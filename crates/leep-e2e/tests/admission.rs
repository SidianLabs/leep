//! Land admission (ADR 0060, section 2) with the real binaries: `leep land`
//! against a server started with `--land-queue-batches 1`, so every land
//! that arrives while a batch replays is refused with 503 and
//! `Leep-Refused: admission`. Run `cargo build --workspace` first.

mod common;

use std::collections::HashMap;
use std::path::PathBuf;

use common::*;

/// Working copies landing at once in a round.
const AGENTS: usize = 12;
/// Rounds tried until one of them saw `leep land` resend after a refusal.
const ROUNDS: usize = 5;

/// `GET` a JSON answer from the server.
fn get_json(addr: std::net::SocketAddr, path: &str) -> serde_json::Value {
    let (status, _, body) = http_get(addr, path, None);
    assert_eq!(status, 200, "{path}: {body}");
    serde_json::from_str(&body).unwrap()
}

/// Many `leep land`s at once against a bound of one batch: the ones
/// refused wait the `Retry-After`, send the land again under the same land
/// id and land it, each exactly once, with one ref move per land.
#[test]
fn leep_land_resends_a_land_not_admitted_and_lands_it_once() {
    let _g = serial();
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    let stores = Stores::sqlite(&root.join("data"));
    let server = Server::start_with(free_port(), &stores, root, &["--land-queue-batches", "1"]);
    let client = Client::new(&server.url, &root.join("cache"));
    let seed = root.join("seed");
    client.ok(root, &["init", seed.to_str().unwrap()]);
    write(&seed, "seed.txt", "seed\n");
    client.ok(&seed, &["commit", "-m", "seed"]);
    client.ok(&seed, &["land"]);

    let mut landed: Vec<(String, String)> = Vec::new();
    let mut resent = 0;
    for round in 0..ROUNDS {
        let wcs: Vec<PathBuf> = (0..AGENTS)
            .map(|n| {
                let wc = root.join(format!("wc-{round}-{n}"));
                client.ok(root, &["init", wc.to_str().unwrap()]);
                let path = format!("r{round}/agent-{n}.txt");
                write(&wc, &path, &format!("round {round} agent {n}\n"));
                client.ok(&wc, &["commit", "-m", &format!("r{round}a{n}")]);
                wc
            })
            .collect();
        let outs: Vec<_> = std::thread::scope(|s| {
            let handles: Vec<_> = wcs
                .iter()
                .map(|wc| s.spawn(|| client.run(wc, &["land"])))
                .collect();
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });
        for (n, out) in outs.iter().enumerate() {
            let err = stderr(out);
            if err.contains("sending the land again") {
                assert!(err.contains("land not admitted"), "{err}");
                resent += 1;
            }
            // Out of resends (five, or 60 s): the land stays pending and
            // the next `leep land` sends it again under its id.
            let mut tries = 0;
            let mut last = out.status.success();
            while !last {
                assert_eq!(out.status.code(), Some(3), "{err}");
                tries += 1;
                assert!(tries < 10, "agent {n} never landed: {err}");
                last = client.run(&wcs[n], &["land"]).status.success();
            }
            landed.push((
                format!("r{round}/agent-{n}.txt"),
                format!("round {round} agent {n}\n"),
            ));
        }
        if resent > 0 {
            break;
        }
    }
    assert!(
        resent > 0,
        "no `leep land` was refused and resent in {ROUNDS} rounds"
    );

    // Exactly once: one ref move per land, every commit once in history,
    // every file as written.
    let log = get_json(server.addr, "/v1/reflog/main?limit=1000");
    let moves = log["entries"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|e| e["operation"] == "land")
        .count();
    assert_eq!(moves, landed.len() + 1, "one ref move per land");
    let head = get_json(server.addr, "/v1/resolve/main")["id"]
        .as_str()
        .unwrap()
        .to_string();
    let history = get_json(server.addr, &format!("/v1/log/{head}?limit=1000"));
    let mut messages: HashMap<String, usize> = HashMap::new();
    for e in history["entries"].as_array().unwrap() {
        let m = e["message"]
            .as_str()
            .or(e["changeset"]["message"].as_str())
            .expect("a message")
            .to_string();
        *messages.entry(m).or_default() += 1;
    }
    assert!(
        messages.values().all(|n| *n == 1),
        "a commit landed twice: {messages:?}"
    );
    assert_eq!(messages.len(), landed.len() + 1);
    let fresh = root.join("fresh");
    client.ok(root, &["init", fresh.to_str().unwrap()]);
    for (path, body) in &landed {
        assert_eq!(
            std::fs::read_to_string(fresh.join(path)).unwrap(),
            *body,
            "{path}"
        );
    }
}
