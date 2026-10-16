//! The restore drill (ADR 0026), with the real binaries.
//!
//! A server takes real traffic (agents landing, a rebase, signed changesets,
//! tags, a ref log) while `leep-server gc` condemns and collects in a loop and
//! `leep-server backup` runs. The backup is restored into fresh stores, a new
//! server is started on them, and everything a user can ask is compared with
//! the source at the snapshot's point in time: `verify --full --rehash` is
//! clean, every ref tip in the manifest is present, history, blame and search
//! answer identically, the ref log is a prefix of the source's, and a fresh
//! checkout has the same files.
//!
//! SQLite always; Postgres (restoring Postgres to Postgres, and each backend
//! into the other) when `LEEP_TEST_POSTGRES_URL` is set. Run
//! `cargo build --workspace` first.

mod common;

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use common::*;
use serde_json::Value;

/// Server request timeout, and a GC fence comfortably above it (ADR 0012).
const REQUEST_TIMEOUT: &str = "3s";
const FENCE: &str = "5s";
const GRACE: &str = "1s";

const ONE: &str = "pub fn one() { println!(\"needle\"); }\n";

struct Keys {
    file: PathBuf,
    server_args: Vec<String>,
}

fn make_keys(root: &Path) -> Keys {
    let client = Client::new("http://127.0.0.1:1", &root.join("cache"));
    let file = root.join("alice.key");
    let out = client.ok(
        root,
        &["keygen", "--out", file.to_str().unwrap(), "--name", "alice"],
    );
    let entry: Value = serde_json::from_str(out.trim()).unwrap();
    let keys = root.join("keys.json");
    fs::write(&keys, serde_json::json!({ "keys": [entry] }).to_string()).unwrap();
    Keys {
        file,
        server_args: vec![
            "--signing-keys-file".into(),
            keys.to_str().unwrap().into(),
            "--request-timeout".into(),
            REQUEST_TIMEOUT.into(),
        ],
    }
}

fn start(stores: &Stores, log_dir: &Path, keys: &Keys) -> Server {
    let extra: Vec<&str> = keys.server_args.iter().map(String::as_str).collect();
    Server::start_with(free_port(), stores, log_dir, &extra)
}

fn commit_signed(client: &Client, wc: &Path, msg: &str, key: &Path) {
    let mut cmd = client.cmd(wc);
    cmd.args(["commit", "-m", msg]).env("LEEP_SIGNING_KEY", key);
    assert_ok(&cmd.output().unwrap(), &format!("commit {msg}"));
}

/// A history through the real server: signed lands, a stale working copy that
/// gets rebased, a deletion, a tag and a branch, and every kind of derived
/// data a query creates.
fn populate(client: &Client, root: &Path, keys: &Keys) -> PathBuf {
    let a = root.join("wc-a");
    let b = root.join("wc-b");
    client.ok(root, &["init", a.to_str().unwrap()]);
    write(&a, "alpha/one.rs", ONE);
    write(&a, "alpha/deep/two.txt", "two\nlines\n");
    write(&a, "README.md", "# demo\n");
    write(&a, "tool.bin", "\0\u{1}binary");
    commit_signed(client, &a, "seed", &keys.file);
    client.ok(&a, &["land"]);
    client.ok(root, &["init", b.to_str().unwrap()]);
    write(&a, "alpha/one.rs", &format!("{ONE}// more\n"));
    write(&a, "beta/three.txt", "three\n");
    commit_signed(client, &a, "second", &keys.file);
    client.ok(&a, &["land"]);
    client.ok(&a, &["tag", "v1"]);
    fs::remove_file(a.join("README.md")).unwrap();
    client.ok(&a, &["commit", "-m", "drop readme"]);
    client.ok(&a, &["land"]);
    // `b` is based on the first landed changeset, so landing it rebases it.
    write(&b, "gamma/four.txt", "four\n");
    client.ok(&b, &["commit", "-m", "stale work"]);
    client.ok(&b, &["land"]);
    client.ok(&a, &["sync"]);
    client.ok(&a, &["branch", "release"]);
    client.ok(&a, &["search", "needle"]);
    client.ok(&a, &["blame", "alpha/one.rs"]);
    client.ok(&a, &["log", "main", "--path", "alpha"]);
    a
}

/// What agents and GC do while the backup runs.
struct Load {
    stop: Arc<AtomicBool>,
    lands: Arc<AtomicU64>,
    gc_deleted: Arc<AtomicU64>,
    gc_runs: Arc<AtomicU64>,
    gc_errors: Arc<Mutex<Vec<String>>>,
    threads: Vec<thread::JoinHandle<()>>,
}

impl Load {
    fn start(server: &Server, stores: &Stores, root: &Path, agents: usize) -> Load {
        let stop = Arc::new(AtomicBool::new(false));
        let lands = Arc::new(AtomicU64::new(0));
        let gc_deleted = Arc::new(AtomicU64::new(0));
        let gc_runs = Arc::new(AtomicU64::new(0));
        let gc_errors = Arc::new(Mutex::new(Vec::new()));
        let mut threads = Vec::new();
        for n in 0..agents {
            let client = Client::new(&server.url, &root.join(format!("cache-agent{n}")));
            let wc = root.join(format!("agent{n}"));
            client.ok(root, &["init", wc.to_str().unwrap()]);
            client.ok(&wc, &["sync"]);
            let (stop, lands) = (stop.clone(), lands.clone());
            threads.push(thread::spawn(move || {
                let mut i = 0;
                while !stop.load(Ordering::Relaxed) {
                    i += 1;
                    write(
                        &wc,
                        &format!("agent{n}/f{i}.txt"),
                        &format!("agent {n} step {i}\n"),
                    );
                    if !client
                        .run(&wc, &["commit", "-m", &format!("agent {n} step {i}")])
                        .status
                        .success()
                    {
                        continue;
                    }
                    let mut landed = false;
                    for _ in 0..4 {
                        if client.run(&wc, &["land"]).status.success() {
                            landed = true;
                            break;
                        }
                        let _ = client.run(&wc, &["sync"]);
                    }
                    if landed {
                        lands.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }));
        }
        // Orphans: uploaded and never referenced, so GC has work to do.
        {
            let (stop, addr) = (stop.clone(), server.addr);
            threads.push(thread::spawn(move || {
                let mut i = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    i += 1;
                    let body = format!("orphan {i} {}", std::process::id()).into_bytes();
                    let h = leep_core::Hash::of_blob(&body);
                    let _ = http(addr, "PUT", &format!("/v1/blobs/{h}"), &body);
                    thread::sleep(Duration::from_millis(40));
                }
            }));
        }
        // GC in a loop: condemn, wait out the fence, collect, repeat.
        {
            let (stop, runs, deleted) = (stop.clone(), gc_runs.clone(), gc_deleted.clone());
            let errors = gc_errors.clone();
            let (meta, blobs) = (stores.meta.clone(), stores.blobs.clone());
            threads.push(thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    let out = Command::new(bin("leep-server"))
                        .args(["--meta", &meta, "--blobs", &blobs])
                        .args(["gc", "--grace", GRACE, "--fence", FENCE])
                        .env("RUST_LOG", "warn")
                        .output()
                        .expect("spawn gc");
                    if !out.status.success() {
                        errors.lock().unwrap().push(stderr(&out));
                        break;
                    }
                    runs.fetch_add(1, Ordering::Relaxed);
                    for line in stdout(&out).lines() {
                        if let Some(rest) = line.strip_prefix("deleted: ") {
                            let n: u64 = rest.split_whitespace().next().unwrap().parse().unwrap();
                            deleted.fetch_add(n, Ordering::Relaxed);
                        }
                    }
                }
            }));
        }
        Load {
            stop,
            lands,
            gc_deleted,
            gc_runs,
            gc_errors,
            threads,
        }
    }

    fn halt(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        for t in self.threads.drain(..) {
            let _ = t.join();
        }
    }

    fn stop(mut self) -> (u64, u64, u64) {
        self.halt();
        let errors = self.gc_errors.lock().unwrap().clone();
        assert!(
            errors.is_empty(),
            "gc failed while the backup ran: {errors:?}"
        );
        (
            self.lands.load(Ordering::Relaxed),
            self.gc_runs.load(Ordering::Relaxed),
            self.gc_deleted.load(Ordering::Relaxed),
        )
    }
}

/// A failed assertion must not leave agents and GC running over a deleted
/// directory.
impl Drop for Load {
    fn drop(&mut self) {
        self.halt();
    }
}

fn server_run(stores: &Stores, args: &[&str]) -> Output {
    server_cmd(stores, args)
}

fn json_out(out: &Output, want: i32, what: &str) -> Value {
    assert_eq!(
        out.status.code(),
        Some(want),
        "{what}\n--- stdout\n{}\n--- stderr\n{}",
        stdout(out),
        stderr(out)
    );
    serde_json::from_str(&stdout(out))
        .unwrap_or_else(|e| panic!("{what}: bad JSON: {e}\n{}", stdout(out)))
}

/// Every file below `dir` except working-copy metadata, by relative path.
fn tree(dir: &Path) -> BTreeMap<String, Vec<u8>> {
    fn walk(base: &Path, dir: &Path, out: &mut BTreeMap<String, Vec<u8>>) {
        for e in fs::read_dir(dir).unwrap().flatten() {
            let p = e.path();
            if e.file_name().to_string_lossy().starts_with('.') {
                continue;
            }
            if p.is_dir() {
                walk(base, &p, out);
            } else {
                out.insert(
                    p.strip_prefix(base).unwrap().display().to_string(),
                    fs::read(&p).unwrap(),
                );
            }
        }
    }
    let mut out = BTreeMap::new();
    walk(dir, dir, &mut out);
    out
}

/// What a user can ask about `id`, from `client`'s server.
fn answers(client: &Client, wc: &Path, id: &str) -> Value {
    serde_json::json!({
        "log": client.json(wc, &["log", id, "-n", "5000"]),
        "log_alpha": client.json(wc, &["log", id, "--path", "alpha", "-n", "100"]),
        "blame": client.json(wc, &["blame", id, "alpha/one.rs"]),
        "search": client.json(wc, &["search", "needle", "--rev", id]),
        "search_agents": client.json(wc, &["search", "step", "--rev", id, "-n", "1000"]),
        "last_change": client.json(wc, &["last-change", "alpha/deep/two.txt", "--rev", id]),
        "show": client.ok(wc, &["show", id]),
    })
}

fn reflog(client: &Client, wc: &Path) -> Vec<Value> {
    client.json(wc, &["reflog", "-n", "100000"])["entries"]
        .as_array()
        .unwrap()
        .clone()
}

struct Drill<'a> {
    root: &'a Path,
    keys: &'a Keys,
}

impl Drill<'_> {
    /// Back up `src` while it takes traffic and GC runs; returns the backup
    /// directory, the manifest, and how long the backup took.
    fn backup_under_load(
        &self,
        src: &Stores,
        server: &Server,
        agents: usize,
    ) -> (PathBuf, Value, Duration) {
        let out_dir = self.root.join("backup");
        let load = Load::start(server, src, self.root, agents);
        // Let the workload and the first GC cycles get going: orphans exist,
        // some are condemned, and landing is in full swing.
        let deadline = Instant::now() + Duration::from_secs(90);
        while load.lands.load(Ordering::Relaxed) < 6 || load.gc_runs.load(Ordering::Relaxed) < 2 {
            assert!(
                Instant::now() < deadline,
                "agents are not landing, or gc is not finishing"
            );
            thread::sleep(Duration::from_millis(200));
        }
        // Pause between the snapshot and the blob copy for longer than a whole
        // GC cycle (condemn, fence, collect), so collects certainly run in
        // that window, deleting blobs that existed when the snapshot was taken,
        // while agents keep landing work the snapshot does not contain.
        let deleted_before = load.gc_deleted.load(Ordering::Relaxed);
        let lands_before = load.lands.load(Ordering::Relaxed);
        let t = Instant::now();
        let out = server_run(
            src,
            &[
                "backup",
                "--out",
                out_dir.to_str().unwrap(),
                "--rehash",
                "--json",
                "--pause-after-snapshot",
                "13s",
            ],
        );
        let took = t.elapsed();
        let manifest = json_out(&out, 0, "backup under load");
        let deleted_during = load.gc_deleted.load(Ordering::Relaxed) - deleted_before;
        let lands_during = load.lands.load(Ordering::Relaxed) - lands_before;
        eprintln!(
            "drill: during the backup's pause gc deleted {deleted_during} blobs and agents landed {lands_during} changesets"
        );
        assert!(
            deleted_during > 0,
            "gc deleted nothing while the backup was paused between its halves"
        );
        assert!(lands_during > 0, "no land happened between the halves");
        // Another backup straight after, still under load: incremental.
        let out2 = server_run(
            src,
            &[
                "backup",
                "--out",
                out_dir.to_str().unwrap(),
                "--name",
                "second",
                "--json",
            ],
        );
        let m2 = json_out(&out2, 0, "second backup under load");
        assert!(
            m2["manifest"]["blobs"]["copied"].as_u64().unwrap()
                < manifest["manifest"]["blobs"]["listed"].as_u64().unwrap(),
            "the second backup copies only what is new: {m2}"
        );
        let (lands, gc_runs, gc_deleted) = load.stop();
        eprintln!(
            "drill: {lands} lands and {gc_runs} gc runs ({gc_deleted} blobs deleted) overlapped the backups"
        );
        assert!(lands >= 6);
        assert!(gc_runs >= 1, "gc never completed a run while backing up");
        (out_dir, manifest, took)
    }

    /// Restore into `dest` and check everything against the source.
    fn restore_and_compare(
        &self,
        dest: &Stores,
        src_client: &Client,
        src_wc: &Path,
        out_dir: &Path,
        manifest: &Value,
        tag: &str,
    ) {
        let m = &manifest["manifest"];
        let refs = m["refs"].as_object().unwrap();
        assert!(refs.contains_key("main"), "{refs:?}");

        // A refused restore leaves nothing behind: bad arguments first.
        let out = server_run(
            dest,
            &[
                "restore",
                "--from",
                self.root.join("no-such-dir").to_str().unwrap(),
            ],
        );
        assert_eq!(out.status.code(), Some(2), "{}", stderr(&out));

        let t = Instant::now();
        let out = server_run(
            dest,
            &[
                "restore",
                "--from",
                out_dir.to_str().unwrap(),
                "--snapshot",
                m["name"].as_str().unwrap(),
                "--rehash",
                "--json",
            ],
        );
        let restored = json_out(&out, 0, "restore");
        eprintln!(
            "drill[{tag}]: restore of {} changesets, {} blobs took {:.1}s",
            m["counts"]["changesets"],
            restored["blobs_listed"],
            t.elapsed().as_secs_f64()
        );
        assert_eq!(restored["changesets"], m["counts"]["changesets"]);
        assert_eq!(restored["check"]["problems_total"], 0, "{restored}");

        // Restoring again onto what is now there is refused, loudly.
        let again = server_run(
            dest,
            &[
                "restore",
                "--from",
                out_dir.to_str().unwrap(),
                "--snapshot",
                m["name"].as_str().unwrap(),
            ],
        );
        assert_eq!(again.status.code(), Some(2));
        assert!(stderr(&again).contains("not empty"), "{}", stderr(&again));

        // `verify --full --rehash` on the restored stores, with no server.
        let v = json_out(
            &server_run(dest, &["verify", "--full", "--rehash", "--json"]),
            0,
            "verify restored",
        );
        assert_eq!(v["ok"], true);
        assert!(v["stats"]["blobs_rehashed"].as_u64().unwrap() >= 6);
        assert_eq!(
            v["stats"]["changesets_reachable"].as_u64().unwrap()
                + v["stats"]["changesets_other"].as_u64().unwrap(),
            m["counts"]["changesets"].as_u64().unwrap()
        );

        let server = start(dest, self.root, self.keys);
        let client = Client::new(
            &server.url,
            &self.root.join(format!("cache-restored-{tag}")),
        );
        let wc = self.root.join(format!("restored-wc-{tag}"));
        client.ok(self.root, &["init", wc.to_str().unwrap()]);

        // Every ref tip in the manifest, exactly.
        let live = client.json(&wc, &["refs"])["refs"].clone();
        let live = live.as_object().unwrap();
        assert_eq!(live.len(), refs.len(), "{live:?} vs {refs:?}");
        for (name, tip) in refs {
            assert_eq!(live[name], *tip, "ref {name}");
        }

        // History, blame, search, last-change and show answer the same as the
        // source, for each ref's tip at the snapshot.
        for (name, tip) in refs {
            let id = tip.as_str().unwrap();
            assert_eq!(
                answers(src_client, src_wc, id),
                answers(&client, &wc, id),
                "answers about {name} at {id} differ after restore"
            );
        }
        // The signed changesets still show as signed.
        let main = refs["main"].as_str().unwrap();
        let log = client.json(&wc, &["log", main, "-n", "5000"]);
        let entries = log["entries"].as_array().unwrap();
        let signed = entries
            .iter()
            .filter(|e| {
                client
                    .ok(&wc, &["show", e["id"].as_str().unwrap()])
                    .contains("Signed-by: alice")
            })
            .count();
        assert_eq!(signed, 2, "the two signed lands survive");
        assert!(entries.len() >= 4, "{} entries", entries.len());

        // The ref log is the source's, up to the snapshot, entry for entry.
        let restored_log = reflog(&client, &wc);
        let source_log: Vec<Value> = reflog(src_client, src_wc)
            .into_iter()
            .filter(|e| e["seq"].as_u64().unwrap() <= restored_log[0]["seq"].as_u64().unwrap())
            .collect();
        assert_eq!(restored_log, source_log, "ref log differs after restore");
        // The manifest's high-water mark is the newest entry, or above it when
        // a numbered entry never committed (a Postgres sequence is not rolled
        // back with the lost compare-and-swap that took the number).
        assert!(
            restored_log[0]["seq"].as_u64().unwrap() <= m["ref_log_last_seq"].as_u64().unwrap(),
            "the manifest's last ref log seq covers the log"
        );

        // A fresh checkout matches one of the source at the same revision.
        let fresh = self.root.join(format!("fresh-{tag}"));
        client.ok(self.root, &["init", fresh.to_str().unwrap()]);
        client.ok(&fresh, &["checkout", main]);
        let reference = self.root.join(format!("reference-{tag}"));
        src_client.ok(self.root, &["init", reference.to_str().unwrap()]);
        src_client.ok(&reference, &["checkout", main]);
        let (a, b) = (tree(&fresh), tree(&reference));
        assert!(a.len() > 4, "{:?}", a.keys());
        assert_eq!(a, b, "checkouts differ after restore");

        // And it takes new work: land on the restored main, the log carries on.
        write(&fresh, "after-restore.txt", "work after the restore\n");
        client.ok(&fresh, &["commit", "-m", "after restore"]);
        client.ok(&fresh, &["land"]);
        let after = reflog(&client, &wc);
        assert!(
            after[0]["seq"].as_u64().unwrap() > m["ref_log_last_seq"].as_u64().unwrap(),
            "the next ref log entry continues the numbering past the snapshot's"
        );
        drop(server);
    }
}

fn run_drill(src: Stores, dest: Stores, work: &Path) {
    let keys = make_keys(work);
    let server = start(&src, work, &keys);
    let client = Client::new(&server.url, &work.join("cache"));
    let src_wc = populate(&client, work, &keys);
    let drill = Drill {
        root: work,
        keys: &keys,
    };

    let (out_dir, manifest, took) = drill.backup_under_load(&src, &server, 3);
    eprintln!("drill: backup under load took {:.1}s", took.as_secs_f64());
    let m = &manifest["manifest"];
    assert_eq!(manifest["ok"], true);
    assert_eq!(m["check"]["ok"], true);
    assert_eq!(m["check"]["mode"], "closure+rehash");
    assert_eq!(m["format"], 1);
    // The snapshot holds the seeded history and a good deal of what agents landed.
    assert!(m["counts"]["changesets"].as_u64().unwrap() > 30, "{m}");

    // The backup verifies on its own, with the source gone from the picture.
    let v = json_out(
        &Command::new(bin("leep-server"))
            .args([
                "backup-verify",
                "--from",
                out_dir.to_str().unwrap(),
                "--snapshot",
                m["name"].as_str().unwrap(),
                "--rehash",
                "--json",
            ])
            .output()
            .unwrap(),
        0,
        "backup-verify",
    );
    assert_eq!(v["ok"], true);

    drill.restore_and_compare(&dest, &client, &src_wc, &out_dir, &manifest, "main");

    // A backup is only as good as its blobs: lose one the snapshot needs and
    // `backup-verify` says so, naming it, without any restore.
    let hash = leep_core::Hash::of_blob(ONE.as_bytes()).to_hex();
    let blob = out_dir.join("blobs").join(&hash[..2]).join(&hash[2..]);
    let saved = fs::read(&blob).expect("the seed file's blob is in the backup");
    fs::remove_file(&blob).unwrap();
    let out = Command::new(bin("leep-server"))
        .args([
            "backup-verify",
            "--from",
            out_dir.to_str().unwrap(),
            "--snapshot",
            m["name"].as_str().unwrap(),
            "--json",
        ])
        .output()
        .unwrap();
    let v = json_out(&out, 1, "backup-verify with a lost blob");
    let problems = v["check"]["problems"].as_array().unwrap();
    assert!(
        problems
            .iter()
            .any(|p| p["code"] == "missing_blob" && p["message"].as_str().unwrap().contains(&hash)),
        "{problems:?}"
    );
    fs::write(&blob, saved).unwrap();
    drop(server);
}

#[test]
fn restore_drill_sqlite() {
    let _g = serial();
    let tmp = tempfile::tempdir().unwrap();
    let work = tmp.path();
    let src = Stores::sqlite(&work.join("src"));
    let dest = Stores::sqlite(&work.join("dest"));
    run_drill(src, dest, work);
}

#[test]
fn restore_drill_postgres() {
    let _g = serial();
    let Some(src_db) = PgDb::create() else { return };
    let dest_db = PgDb::create().unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let work = tmp.path();
    let src = Stores::with_meta(&work.join("src"), src_db.url.clone());
    let dest = Stores::with_meta(&work.join("dest"), dest_db.url.clone());
    run_drill(src, dest, work);
}

/// A backup is the same logical snapshot whichever backend wrote it, so it
/// restores into the other one (a way to move between them).
#[test]
fn backups_restore_across_backends() {
    let _g = serial();
    let Some(pg_src) = PgDb::create() else { return };
    let pg_dest = PgDb::create().unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let work = tmp.path();
    let keys = make_keys(work);

    for (label, src, dest) in [
        (
            "sqlite-to-postgres",
            Stores::sqlite(&work.join("s1")),
            Stores::with_meta(&work.join("d1"), pg_dest.url.clone()),
        ),
        (
            "postgres-to-sqlite",
            Stores::with_meta(&work.join("s2"), pg_src.url.clone()),
            Stores::sqlite(&work.join("d2")),
        ),
    ] {
        let sub = work.join(label);
        fs::create_dir_all(&sub).unwrap();
        let server = start(&src, &sub, &keys);
        let client = Client::new(&server.url, &sub.join("cache"));
        let wc = populate(&client, &sub, &keys);
        let out_dir = sub.join("backup");
        let b = json_out(
            &server_run(
                &src,
                &[
                    "backup",
                    "--out",
                    out_dir.to_str().unwrap(),
                    "--rehash",
                    "--json",
                ],
            ),
            0,
            "backup",
        );
        assert_eq!(
            b["manifest"]["source_backend"],
            label.split('-').next().unwrap()
        );
        let drill = Drill {
            root: &sub,
            keys: &keys,
        };
        drill.restore_and_compare(&dest, &client, &wc, &out_dir, &b, label);
        drop(server);
    }
}

fn dir_bytes(dir: &Path) -> u64 {
    fs::read_dir(dir)
        .map(|rd| {
            rd.flatten()
                .map(|e| {
                    let p = e.path();
                    if p.is_dir() {
                        dir_bytes(&p)
                    } else {
                        fs::metadata(&p).map_or(0, |m| m.len())
                    }
                })
                .sum()
        })
        .unwrap_or(0)
}

/// Not part of the normal run: builds a store of `LEEP_BACKUP_PERF_CHANGESETS`
/// changesets (default 5 000, three files each, derived rows warmed) and times
/// `backup`, a second backup with nothing new, `backup-verify` and `restore`.
/// The numbers go in docs/PERFORMANCE.md.
///
/// ```sh
/// cargo build --release --workspace
/// cargo test --release -p leep-e2e --test backup -- --ignored --nocapture
/// ```
#[test]
#[ignore = "builds thousands of changesets; run it in release mode"]
fn backup_and_restore_timing() {
    use leep_core::{Changeset, ChangesetId, FileChange, FileKind, RepoPath};

    let _g = serial();
    let target: usize = std::env::var("LEEP_BACKUP_PERF_CHANGESETS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(5_000);
    let tmp = tempfile::tempdir().unwrap();
    let work = tmp.path();
    let pg = PgDb::create();
    let mut variants = vec![(
        "sqlite",
        Stores::sqlite(&work.join("src-sqlite")),
        Stores::sqlite(&work.join("dest-sqlite")),
    )];
    let pg_dest = pg.as_ref().map(|_| PgDb::create().unwrap());
    if let (Some(db), Some(dest)) = (&pg, &pg_dest) {
        variants.push((
            "postgres",
            Stores::with_meta(&work.join("src-pg"), db.url.clone()),
            Stores::with_meta(&work.join("dest-pg"), dest.url.clone()),
        ));
    }
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    for (label, src, dest) in variants {
        let t0 = Instant::now();
        let total = rt.block_on(async {
            let blobs = leep_store::open_blob_store(&src.blobs).unwrap();
            let meta = leep_meta::open_meta_store(&src.meta).await.unwrap();
            let repo = leep_derive::Repo::new(blobs, meta);
            let mut tip: Option<ChangesetId> = None;
            for i in 0..target {
                let mut changes = std::collections::BTreeMap::new();
                for j in 0..3 {
                    let path = format!(
                        "pkg{}/mod{}/file{}.rs",
                        (i + j) % 40,
                        (i * 3 + j) % 25,
                        (i + j * 7) % 60
                    );
                    let body = format!(
                        "// revision {i} file {j}\nfn f{i}() {{ println!(\"{i}\"); }}\n{}\n",
                        "let x = 1;\n".repeat(20)
                    );
                    let h = repo.put_blob(body.clone().into()).await.unwrap();
                    changes.insert(
                        RepoPath::new(path).unwrap(),
                        FileChange::Modify {
                            content: h,
                            kind: FileKind::Regular,
                            size: body.len() as u64,
                        },
                    );
                }
                tip = Some(
                    repo.create_changeset(Changeset {
                        parents: tip.into_iter().collect(),
                        author: "perf".into(),
                        timestamp: 1_700_000_000 + i as i64,
                        message: format!("change {i}"),
                        changes,
                        extra: Default::default(),
                    })
                    .await
                    .unwrap(),
                );
            }
            let tip = tip.unwrap();
            repo.meta().cas_ref("main", None, Some(tip)).await.unwrap();
            repo.derive_search(tip).await.unwrap();
            target
        });
        eprintln!(
            "perf[{label}]: built {total} changesets in {:.1}s",
            t0.elapsed().as_secs_f64()
        );
        let out_dir = work.join(format!("backup-{label}"));
        let timed = |what: &str, args: &[&str], stores: &Stores| -> (Value, f64) {
            let t = Instant::now();
            let out = server_run(stores, args);
            let secs = t.elapsed().as_secs_f64();
            let v = json_out(&out, 0, what);
            eprintln!("perf[{label}]: {what}: {secs:.2}s");
            (v, secs)
        };
        let o = out_dir.to_str().unwrap();
        let (m, _) = timed(
            "backup (first, nothing in the pool)",
            &["backup", "--out", o, "--name", "a", "--json"],
            &src,
        );
        let (m2, _) = timed(
            "backup (again, nothing new)",
            &["backup", "--out", o, "--name", "b", "--json"],
            &src,
        );
        assert_eq!(m2["manifest"]["blobs"]["copied"], 0);
        timed(
            "backup-verify",
            &["backup-verify", "--from", o, "--json"],
            &src,
        );
        timed(
            "backup-verify --rehash",
            &["backup-verify", "--from", o, "--rehash", "--json"],
            &src,
        );
        let (r, _) = timed(
            "restore (with its check)",
            &["restore", "--from", o, "--snapshot", "b", "--json"],
            &dest,
        );
        assert_eq!(r["check"]["problems_total"], 0);
        timed(
            "verify --full --rehash on the restore",
            &["verify", "--full", "--rehash", "--json"],
            &dest,
        );
        eprintln!(
            "perf[{label}]: {} changesets, {} derived rows, {} blobs ({:.1} MiB), meta.db {:.1} MiB",
            m["manifest"]["counts"]["changesets"],
            m["manifest"]["counts"]["derived"],
            m["manifest"]["blobs"]["listed"],
            dir_bytes(&out_dir.join("blobs")) as f64 / 1_048_576.0,
            m["manifest"]["meta_bytes"].as_u64().unwrap() as f64 / 1_048_576.0,
        );
    }
}
