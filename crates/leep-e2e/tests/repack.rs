//! `leep-server repack` with the real binaries (ADR 0029): a history is
//! packed while a server serves it, and everything a user can ask answers
//! the same before and after; `verify --full --rehash` is clean; a repack
//! killed with SIGKILL at several points loses nothing; and a packed store
//! backs up and restores. Run `cargo build --workspace` first.

mod common;

use std::fs;
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use common::*;
use serde_json::Value;

/// Server request timeout, and a repack fence comfortably above it.
const REQUEST_TIMEOUT: &str = "2s";
const FENCE: &str = "3s";

fn repack(stores: &Stores, extra: &[&str]) -> Value {
    let mut args = vec!["repack", "--fence", FENCE, "--json"];
    args.extend_from_slice(extra);
    let out = server_cmd(stores, &args);
    assert_ok(&out, "leep-server repack");
    serde_json::from_str(&stdout(&out)).unwrap_or_else(|e| panic!("{e}: {}", stdout(&out)))
}

fn verify_clean(stores: &Stores, what: &str) {
    let out = server_cmd(stores, &["verify", "--full", "--rehash", "--json"]);
    let v: Value = serde_json::from_str(&stdout(&out))
        .unwrap_or_else(|e| panic!("{what}: {e}\n{}\n{}", stdout(&out), stderr(&out)));
    assert_eq!(out.status.code(), Some(0), "{what}: {v}");
    assert_eq!(v["ok"], true, "{what}: {v}");
    assert!(v["stats"]["blobs_rehashed"].as_u64().unwrap() > 0, "{what}");
}

fn blobs_dir(stores: &Stores) -> std::path::PathBuf {
    stores.blobs.strip_prefix("file://").unwrap().into()
}

/// Loose objects: files named by their hash in a shard directory.
fn loose(stores: &Stores) -> usize {
    let mut n = 0;
    for shard in fs::read_dir(blobs_dir(stores)).unwrap().flatten() {
        if shard.file_name().len() == 2 {
            n += fs::read_dir(shard.path())
                .unwrap()
                .flatten()
                .filter(|f| f.file_name().len() == 62)
                .count();
        }
    }
    n
}

fn pack_files(stores: &Stores) -> Vec<String> {
    let mut v: Vec<String> = fs::read_dir(blobs_dir(stores).join("packs"))
        .map(|rd| {
            rd.flatten()
                .map(|e| e.file_name().to_string_lossy().into_owned())
                .filter(|n| n.ends_with(".pack") || n.ends_with(".idx"))
                .collect()
        })
        .unwrap_or_default();
    v.sort();
    v
}

const BODY: &str = "fn body() {\n    let needle = 1;\n}\n";

/// A history with many versions of the same files, so packing has deltas
/// to find, plus a branch, a tag and an unlanded changeset.
fn populate(client: &Client, root: &Path) -> std::path::PathBuf {
    let wc = root.join("wc");
    client.ok(root, &["init", wc.to_str().unwrap()]);
    write(&wc, "src/lib.rs", BODY);
    write(&wc, "docs/README.md", "# demo\n");
    write(&wc, "data/table.txt", "row 0\n");
    client.ok(&wc, &["commit", "-m", "seed"]);
    client.ok(&wc, &["land"]);
    let mut table = String::from("row 0\n");
    for i in 1..25 {
        table.push_str(&format!("row {i}: some values that repeat {i} {i} {i}\n"));
        write(&wc, "data/table.txt", &table);
        write(
            &wc,
            "src/lib.rs",
            &format!("{BODY}// revision {i}\nfn f{i}() {{ needle(); }}\n"),
        );
        if i % 5 == 0 {
            write(&wc, &format!("docs/note{i}.md"), &format!("note {i}\n"));
        }
        client.ok(&wc, &["commit", "-m", &format!("step {i}")]);
        client.ok(&wc, &["land"]);
    }
    client.ok(&wc, &["tag", "v1"]);
    client.ok(&wc, &["branch", "release"]);
    write(&wc, "src/lib.rs", "unlanded\n");
    client.ok(&wc, &["commit", "-m", "unlanded work"]);
    wc
}

/// What a user can ask about `main`.
fn answers(client: &Client, wc: &Path) -> Value {
    let search = |args: &[&str]| {
        let mut v = client.json(wc, args);
        if let Some(stats) = v["stats"].as_object_mut() {
            stats.remove("blob_rows_derived");
            stats.remove("tree_rows_derived");
        }
        v
    };
    serde_json::json!({
        "log": client.json(wc, &["log", "main", "-n", "1000"]),
        "log_path": client.json(wc, &["log", "main", "--path", "data", "-n", "100"]),
        "blame": client.json(wc, &["blame", "main", "data/table.txt"]),
        "blame_lib": client.json(wc, &["blame", "v1", "src/lib.rs"]),
        "search": search(&["search", "needle", "--rev", "main"]),
        "show": client.ok(wc, &["show", "main"]),
        "show_tag": client.ok(wc, &["show", "v1"]),
    })
}

/// A fresh working copy's files, as a checkout reads every blob of a tree.
fn checkout(client: &Client, root: &Path, name: &str) -> Vec<(String, String)> {
    let wc = root.join(name);
    client.ok(root, &["init", wc.to_str().unwrap()]);
    client.ok(&wc, &["sync"]);
    let mut out = Vec::new();
    let mut stack = vec![wc.clone()];
    while let Some(d) = stack.pop() {
        for e in fs::read_dir(&d).unwrap().flatten() {
            let p = e.path();
            if e.file_name().to_string_lossy().starts_with('.') {
                continue;
            }
            if p.is_dir() {
                stack.push(p);
            } else {
                out.push((
                    p.strip_prefix(&wc).unwrap().display().to_string(),
                    fs::read_to_string(&p).unwrap(),
                ));
            }
        }
    }
    out.sort();
    out
}

#[test]
fn repack_beside_a_server_changes_no_answer() {
    let _g = serial();
    let tmp = tempfile::tempdir().unwrap();
    let work = tmp.path();
    let stores = Stores::sqlite(&work.join("site"));
    let server = Server::start_with(
        free_port(),
        &stores,
        work,
        &["--request-timeout", REQUEST_TIMEOUT],
    );
    let client = Client::new(&server.url, &work.join("cache"));
    let wc = populate(&client, work);
    let before = answers(&client, &wc);
    let files = checkout(&client, work, "co-before");
    let loose_before = loose(&stores);
    assert!(loose_before > 50, "{loose_before}");

    // The server keeps running and serving through the repack.
    let r = repack(&stores, &[]);
    assert_eq!(r["loose_before"].as_u64().unwrap(), loose_before as u64);
    assert_eq!(r["objects"].as_u64().unwrap(), loose_before as u64, "{r}");
    assert!(
        r["deltas"].as_u64().unwrap() > 20,
        "versions are deltas: {r}"
    );
    assert!(
        r["pack_bytes"].as_u64().unwrap() < r["logical_bytes"].as_u64().unwrap() / 2,
        "{r}"
    );
    assert_eq!(r["loose_after"], 0, "{r}");
    assert_eq!(loose(&stores), 0);
    assert_eq!(pack_files(&stores).len(), 2);
    assert_eq!(
        answers(&client, &wc),
        before,
        "same server, after the repack"
    );
    assert_eq!(checkout(&client, work, "co-live"), files);
    verify_clean(&stores, "after the first repack");

    // New work lands loose beside the pack. The default (incremental)
    // repack walks only the new changeset and packs only its objects into a
    // second pack, leaving the first alone.
    write(&wc, "src/lib.rs", &format!("{BODY}// after the repack\n"));
    client.ok(&wc, &["commit", "-m", "after repack"]);
    client.ok(&wc, &["land"]);
    let after_work = answers(&client, &wc);
    let new_loose = loose(&stores) as u64;
    assert!(new_loose > 0);
    let first_pack = pack_files(&stores);
    let r2 = repack(&stores, &[]);
    assert_eq!(r2["mode"], "incremental", "{r2}");
    assert_eq!(r2["changesets"], 1, "{r2}");
    assert_eq!(r2["objects"].as_u64().unwrap(), new_loose, "{r2}");
    assert_eq!(
        (r2["packs_merged"].as_u64(), r2["packs_after"].as_u64()),
        (Some(0), Some(2)),
        "{r2}"
    );
    assert_eq!(loose(&stores), 0);
    assert_eq!(pack_files(&stores).len(), 4);
    assert!(first_pack.iter().all(|f| pack_files(&stores).contains(f)));
    assert_eq!(answers(&client, &wc), after_work);
    verify_clean(&stores, "after the incremental repack");

    // A full repack folds both into one new pack and deletes the old ones.
    let r3 = repack(&stores, &["--full"]);
    assert_eq!(r3["mode"], "full", "{r3}");
    assert_eq!(r3["packs_before"], 2, "{r3}");
    assert_eq!(r3["old_packs_removed"], 2, "{r3}");
    assert!(r3["frames_reused"].as_u64().unwrap() > 0, "{r3}");
    assert_eq!(pack_files(&stores).len(), 2);
    assert_eq!(answers(&client, &wc), after_work);
    drop(server);

    // A fresh server over the packed store, with nothing cached.
    let server = Server::start(&stores, work);
    let client = Client::new(&server.url, &work.join("cache2"));
    assert_eq!(answers(&client, &wc), after_work, "restarted server");
    verify_clean(&stores, "after the second repack");
    drop(server);

    // Blob stores with nowhere to keep packs are refused, clearly.
    let out = server_cmd(
        &Stores {
            meta: stores.meta.clone(),
            blobs: "mem://".into(),
        },
        &["repack"],
    );
    assert!(!out.status.success());
    assert!(stderr(&out).contains("is neither"), "{}", stderr(&out));
    let out = server_cmd(&stores, &["repack", "--fence", "0"]);
    assert!(!out.status.success());
    assert!(
        stderr(&out).contains("--unsafe-no-fence"),
        "{}",
        stderr(&out)
    );
    let out = server_cmd(&stores, &["repack", "--full", "--geometric", "3"]);
    assert!(!out.status.success(), "--full takes no merge rule");
}

/// SIGKILL `repack` at several moments: while it walks and writes, just
/// after it publishes, and during its cleanup. Every answer stays the same
/// and the next repack finishes the job.
#[test]
fn killed_repack_loses_nothing() {
    let _g = serial();
    let tmp = tempfile::tempdir().unwrap();
    let work = tmp.path();
    let stores = Stores::sqlite(&work.join("site"));
    let server = Server::start(&stores, work);
    let client = Client::new(&server.url, &work.join("cache"));
    let wc = populate(&client, work);
    let before = answers(&client, &wc);
    drop(server);

    let spawn = |fence: &str| {
        Command::new(bin("leep-server"))
            .args(["--meta", &stores.meta, "--blobs", &stores.blobs])
            .args(["repack", "--fence", fence, "--unsafe-no-fence"])
            .env("RUST_LOG", "warn")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap()
    };
    let published = || pack_files(&stores).iter().any(|f| f.ends_with(".idx"));
    // Early kills (by time), a kill right after publish (during the fence),
    // and kills during cleanup with no fence.
    let plans: [(&str, Option<Duration>); 5] = [
        ("10m", Some(Duration::from_millis(30))),
        ("10m", Some(Duration::from_millis(150))),
        ("10m", None),
        ("0", Some(Duration::from_millis(5))),
        ("0", Some(Duration::from_millis(60))),
    ];
    for (i, (fence, after)) in plans.into_iter().enumerate() {
        let mut child = spawn(fence);
        match after {
            Some(d) => std::thread::sleep(d),
            None => {
                let deadline = Instant::now() + Duration::from_secs(60);
                while !published() {
                    assert!(Instant::now() < deadline, "repack never published");
                    std::thread::sleep(Duration::from_millis(5));
                }
            }
        }
        let _ = child.kill();
        let _ = child.wait();
        let server = Server::start(&stores, work);
        let client = Client::new(&server.url, &work.join(format!("cache-k{i}")));
        assert_eq!(answers(&client, &wc), before, "after kill {i}");
        drop(server);
        verify_clean(&stores, &format!("after kill {i}"));
    }
    let r = repack(&stores, &["--unsafe-no-fence"]);
    assert_eq!(r["loose_after"], 0, "{r}");
    assert_eq!(pack_files(&stores).len(), 2, "{:?}", pack_files(&stores));
    let leftovers: Vec<_> = fs::read_dir(blobs_dir(&stores).join("packs"))
        .unwrap()
        .flatten()
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.starts_with(".tmp-"))
        .collect();
    assert!(leftovers.is_empty(), "{leftovers:?}");
    let server = Server::start(&stores, work);
    let client = Client::new(&server.url, &work.join("cache-final"));
    assert_eq!(answers(&client, &wc), before);
    verify_clean(&stores, "after the final repack");
}

/// A packed store backs up (its packs are copied as files, ADR 0039) and
/// restores into fresh stores that answer the same.
#[test]
fn packed_store_backs_up_and_restores() {
    let _g = serial();
    let tmp = tempfile::tempdir().unwrap();
    let work = tmp.path();
    let src = Stores::sqlite(&work.join("src"));
    let server = Server::start(&src, work);
    let client = Client::new(&server.url, &work.join("cache"));
    let wc = populate(&client, work);
    drop(server);
    repack(&src, &["--unsafe-no-fence"]);
    assert_eq!(loose(&src), 0);
    let server = Server::start(&src, work);
    let client = Client::new(&server.url, &work.join("cache-src"));
    let before = answers(&client, &wc);
    drop(server);

    let out_dir = work.join("backup");
    let out = server_cmd(
        &src,
        &[
            "backup",
            "--out",
            out_dir.to_str().unwrap(),
            "--rehash",
            "--json",
        ],
    );
    assert_ok(&out, "backup of a packed store");
    let b: Value = serde_json::from_str(&stdout(&out)).unwrap();
    assert_eq!(b["ok"], true, "{b}");
    // The pack crossed as files; no object went one by one.
    assert_eq!(b["manifest"]["blobs"]["packs_copied"], 1, "{b}");
    assert_eq!(b["manifest"]["blobs"]["copied"], 0, "{b}");
    let pool = out_dir.join("blobs").join("packs");
    for f in pack_files(&src) {
        assert_eq!(
            fs::read(pool.join(&f)).unwrap(),
            fs::read(blobs_dir(&src).join("packs").join(&f)).unwrap(),
            "{f}"
        );
    }
    let name = b["manifest"]["name"].as_str().unwrap().to_string();
    // An unchanged packed store has nothing new to copy: listings agree on
    // logical sizes.
    let again = server_cmd(
        &src,
        &[
            "backup",
            "--out",
            out_dir.to_str().unwrap(),
            "--name",
            "again",
            "--json",
        ],
    );
    assert_ok(&again, "second backup");
    let again: Value = serde_json::from_str(&stdout(&again)).unwrap();
    assert_eq!(again["manifest"]["blobs"]["copied"], 0, "{again}");
    assert_eq!(again["manifest"]["blobs"]["replaced"], 0, "{again}");
    assert_eq!(again["manifest"]["blobs"]["packs_copied"], 0, "{again}");
    let out = server_cmd(
        &src,
        &[
            "backup-verify",
            "--from",
            out_dir.to_str().unwrap(),
            "--rehash",
        ],
    );
    assert_ok(&out, "backup-verify --rehash of a pool holding packs");

    let dest = Stores::sqlite(&work.join("dest"));
    let out = server_cmd(
        &dest,
        &[
            "restore",
            "--from",
            out_dir.to_str().unwrap(),
            "--snapshot",
            &name,
            "--rehash",
            "--json",
        ],
    );
    assert_ok(&out, "restore");
    assert_eq!(
        pack_files(&dest),
        pack_files(&src),
        "the packs restore as files"
    );
    assert_eq!(loose(&dest), 0);
    verify_clean(&dest, "restored");
    let server = Server::start(&dest, work);
    let client = Client::new(&server.url, &work.join("cache-dest"));
    assert_eq!(answers(&client, &wc), before, "restored answers");
    drop(server);
    // The restored store packs too.
    repack(&dest, &["--unsafe-no-fence"]);
    verify_clean(&dest, "restored and repacked");
}
