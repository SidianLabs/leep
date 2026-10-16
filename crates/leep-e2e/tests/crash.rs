//! Crash consistency with the real binaries (ADR 0021): `leep-server` is
//! killed with SIGKILL at seeded random moments while agents commit and land,
//! `leep-server gc` is killed mid-collect, and after every kill the stores
//! are checked from outside: every ref reaches only readable data, every land
//! a client was told succeeded is in history, the ref log replays to the refs,
//! and garbage collection never removed anything reachable.
//!
//! Knobs: `LEEP_CRASH_ROUNDS` (kills per backend, default 3), `LEEP_CRASH_SEED`
//! (default fixed), `LEEP_CRASH_AGENTS` (HTTP agents, default 6). The kill
//! schedule and the workload derive from the seed, which every failure
//! prints; thread scheduling stays up to the OS, so a seed replays the plan,
//! not the interleaving. Run `cargo build --workspace` first.

mod common;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::net::SocketAddr;
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use common::*;
use leep_core::api::{CreateChangesetResponse, LandRequest, LandResponse, RefResponse};
use leep_core::{Changeset, ChangesetId, FileChange, FileKind, Hash, RepoPath};
use leep_derive::Repo;
use leep_testkit::Rng;

const DEFAULT_SEED: u64 = 0x1EE9_C4A5;
const CLI_AGENTS: usize = 2;
/// Server request timeout, and a fence comfortably above it (ADR 0012).
const REQUEST_TIMEOUT: &str = "2s";
const LIVE_FENCE: &str = "4s";
/// Orphans planted for the killed-collect round: enough that a collect takes
/// well over the kill delay (each delete is several committed statements).
const PLANTED: usize = 400;

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// One file a client commits, and the message that identifies it in history.
#[derive(Clone)]
struct Step {
    path: String,
    body: String,
    msg: String,
}

/// A land a client was told succeeded.
struct Confirmed {
    steps: Vec<Step>,
    landed: ChangesetId,
}

#[derive(Default)]
struct Shared {
    stop: AtomicBool,
    confirmed: Mutex<Vec<Confirmed>>,
    /// Answers that were neither a success nor a plain "try again".
    odd: Mutex<Vec<String>>,
    retries: Mutex<u64>,
}

impl Shared {
    fn note_retry(&self, why: &str) {
        *self.retries.lock().unwrap() += 1;
        // 408 (request timeout), 409 (GC in flight) and connection failures
        // are the expected ways a retry arises; anything else is recorded.
        let expected = [
            "connect",
            "reset",
            "refused",
            "broken",
            "cut short",
            "truncated",
            "cannot reach",
            "error sending request",
        ]
        .iter()
        .any(|w| why.contains(w))
            || why.starts_with("408")
            || why.contains("408 Request Timeout")
            || why.starts_with("409")
            || why.contains("missing blob");
        if !expected {
            self.odd.lock().unwrap().push(why.to_string());
        }
    }
}

fn step_for(agent: &str, k: usize, rng: &mut Rng) -> Step {
    let pad: String = (0..rng.below(40)).map(|_| 'x').collect();
    Step {
        path: format!("{agent}/step-{k:04}.txt"),
        body: format!("{agent} step {k}\n{pad}\n"),
        msg: format!("{agent}-s{k}"),
    }
}

fn expect(what: &str, got: (u16, Vec<u8>), ok: &[u16]) -> Result<Vec<u8>, String> {
    if ok.contains(&got.0) {
        Ok(got.1)
    } else {
        Err(format!(
            "{} {what}: {}",
            got.0,
            String::from_utf8_lossy(&got.1)
        ))
    }
}

/// Upload, commit a stack on today's head and land it. Uploads and commits
/// are idempotent, so a failure before the land is answered by doing them
/// again. Once a land is sent, `pending` holds its changeset and later
/// attempts land that same changeset: a land whose answer was lost (a hang-up,
/// a restart, a 408) may have applied, and only the same changeset lets the
/// server see that it did. Rebuilding on the new head would land it twice.
/// With `hang_up` set, the numbered request is abandoned a few microseconds
/// after it is sent, like a client timing out, and the attempt reports failure.
fn http_attempt(
    addr: SocketAddr,
    steps: &[Step],
    hang_up: Option<(usize, Duration)>,
    pending: &mut Option<ChangesetId>,
) -> Result<ChangesetId, String> {
    let sent = std::cell::Cell::new(0);
    let req = |m: &str, p: &str, b: &[u8]| {
        let n = sent.get();
        sent.set(n + 1);
        if let Some((at, after)) = hang_up.filter(|(at, _)| *at == n) {
            http_abandon(addr, m, p, b, after);
            return Err(format!("{p}: abandoned request {at}: connection reset"));
        }
        http(addr, m, p, b).map_err(|e| format!("{p}: {e}"))
    };
    if let Some(tip) = *pending {
        return land_once(&req, tip, pending);
    }
    let head: RefResponse = serde_json::from_slice(&expect(
        "get ref",
        req("GET", "/v1/refs/main", b"")?,
        &[200],
    )?)
    .map_err(|e| e.to_string())?;
    let mut parent = head.target;
    for s in steps {
        let h = Hash::of_blob(s.body.as_bytes());
        expect(
            "put blob",
            req("PUT", &format!("/v1/blobs/{h}"), s.body.as_bytes())?,
            &[204],
        )?;
        let cs = Changeset {
            parents: parent.into_iter().collect(),
            author: "e2e-http".into(),
            timestamp: 1_700_000_000,
            message: s.msg.clone(),
            changes: BTreeMap::from([(
                RepoPath::new(s.path.as_str()).unwrap(),
                FileChange::Modify {
                    content: h,
                    kind: FileKind::Regular,
                    size: s.body.len() as u64,
                },
            )]),
            extra: BTreeMap::new(),
        };
        let made: CreateChangesetResponse = serde_json::from_slice(&expect(
            "create changeset",
            req("POST", "/v1/changesets", &serde_json::to_vec(&cs).unwrap())?,
            &[200],
        )?)
        .map_err(|e| e.to_string())?;
        parent = Some(made.id);
    }
    let tip = parent.expect("a stack has a step");
    *pending = Some(tip);
    land_once(&req, tip, pending)
}

fn land_once(
    req: &impl Fn(&str, &str, &[u8]) -> Result<(u16, Vec<u8>), String>,
    tip: ChangesetId,
    pending: &mut Option<ChangesetId>,
) -> Result<ChangesetId, String> {
    let land = LandRequest {
        changeset: tip,
        target_ref: "main".into(),
    };
    let got = req("POST", "/v1/land", &serde_json::to_vec(&land).unwrap())?;
    let body = expect("land", got, &[200]).inspect_err(|why| {
        // The stack never landed (a landed copy keeps its blobs alive), so
        // its blobs may be collected: upload and commit it again.
        if why.contains("missing blob") {
            *pending = None;
        }
    })?;
    let resp: LandResponse = serde_json::from_slice(&body).map_err(|e| e.to_string())?;
    match resp {
        LandResponse::Landed { landed, .. } => Ok(landed),
        LandResponse::Conflicts { .. } => panic!("agents write disjoint files, yet: {resp:?}"),
    }
}

fn http_agent(n: usize, seed: u64, addr: SocketAddr, shared: &Shared) {
    let who = format!("http-{n}");
    let mut rng = Rng::new(seed ^ ((n as u64 + 1) * 0x9E37));
    let mut k = 0;
    while !shared.stop.load(Ordering::Relaxed) {
        let steps: Vec<Step> = (0..1 + rng.below(2))
            .map(|_| {
                k += 1;
                step_for(&who, k, &mut rng)
            })
            .collect();
        let deadline = Instant::now() + Duration::from_secs(120);
        let mut pending = None;
        let landed = loop {
            // One attempt in four hangs up on a request of its own.
            let hang_up = (rng.below(4) == 0).then(|| {
                let at = 1 + rng.below(1 + 2 * steps.len() + 1);
                (at, Duration::from_micros(rng.below(8000) as u64))
            });
            match http_attempt(addr, &steps, hang_up, &mut pending) {
                Ok(id) => break id,
                Err(why) => {
                    shared.note_retry(&why);
                    assert!(Instant::now() < deadline, "{who} is stuck: {why}");
                    std::thread::sleep(Duration::from_millis(20));
                }
            }
        };
        shared
            .confirmed
            .lock()
            .unwrap()
            .push(Confirmed { steps, landed });
    }
}

/// A real `leep` client: a fresh working copy per step, `commit`, then `land`
/// until the server answers. A land whose answer was lost is asked again.
fn cli_agent(n: usize, seed: u64, client: &Client, root: &Path, shared: &Shared) {
    let who = format!("cli-{n}");
    let mut rng = Rng::new(seed ^ ((n as u64 + 77) * 0x51ED));
    let mut k = 0;
    while !shared.stop.load(Ordering::Relaxed) {
        k += 1;
        let step = step_for(&who, k, &mut rng);
        let deadline = Instant::now() + Duration::from_secs(120);
        let stuck = |what: &str| panic!("{who} is stuck at {what}");
        let mut attempt = 0;
        let wc = loop {
            attempt += 1;
            let wc = root.join(format!("{who}-{k}-{attempt}"));
            if client
                .run(root, &["init", wc.to_str().unwrap()])
                .status
                .success()
            {
                break wc;
            }
            if Instant::now() > deadline {
                stuck("init");
            }
            std::thread::sleep(Duration::from_millis(30));
        };
        write(&wc, &step.path, &step.body);
        while !client
            .run(&wc, &["commit", "-m", &step.msg])
            .status
            .success()
        {
            if Instant::now() > deadline {
                stuck("commit");
            }
            std::thread::sleep(Duration::from_millis(30));
        }
        let landed = loop {
            let out = client.run(&wc, &["--json", "land"]);
            if out.status.success() {
                let v: serde_json::Value = serde_json::from_str(&stdout(&out)).unwrap();
                break v["landed"]
                    .as_str()
                    .unwrap()
                    .parse::<ChangesetId>()
                    .unwrap();
            }
            assert_ne!(out.status.code(), Some(1), "{}", stdout(&out));
            shared.note_retry(&stderr(&out));
            if Instant::now() > deadline {
                stuck("land");
            }
            std::thread::sleep(Duration::from_millis(30));
        };
        shared.confirmed.lock().unwrap().push(Confirmed {
            steps: vec![step],
            landed,
        });
        let _ = std::fs::remove_dir_all(&wc);
    }
}

fn open_repo(stores: &Stores) -> Repo {
    let blobs = leep_store::open_blob_store(&stores.blobs).expect("blob store");
    let meta = common_block_on(leep_meta::open_meta_store(&stores.meta)).expect("meta store");
    Repo::new(blobs, meta)
}

fn runtime() -> &'static tokio::runtime::Runtime {
    static RT: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap()
    })
}

fn common_block_on<F: std::future::Future>(f: F) -> F::Output {
    runtime().block_on(f)
}

/// Everything the invariants say about a store, checked from outside the
/// server. `tag` names the backend, seed and round in every message.
fn check_store(tag: &str, stores: &Stores, shared: &Shared, live: bool) {
    let repo = open_repo(stores);
    common_block_on(async {
        let meta = repo.meta();
        let refs = meta.list_refs().await.unwrap();

        // Every changeset ever stored still has its tree and its blobs.
        let mut after = None;
        let mut all = Vec::new();
        loop {
            let page = meta.list_changesets(after, 500).await.unwrap();
            let Some(last) = page.last().copied() else {
                break;
            };
            after = Some(last);
            all.extend(page);
        }
        let mut blobs_ok: HashSet<Hash> = HashSet::new();
        for id in &all {
            let files = repo
                .list_files(*id, "")
                .await
                .unwrap_or_else(|e| panic!("[{tag}] tree of {id} is unreadable: {e}"));
            for f in files {
                if blobs_ok.insert(f.content) {
                    assert!(
                        repo.blobs().contains(&f.content).await.unwrap(),
                        "[{tag}] {id}:{} lost its blob {}",
                        f.path,
                        f.content
                    );
                }
            }
        }

        // Every ref reaches only changesets that exist.
        for (name, head) in &refs {
            let mut seen = HashSet::from([*head]);
            let mut stack = vec![*head];
            while let Some(id) = stack.pop() {
                let cs = repo
                    .get_changeset(id)
                    .await
                    .unwrap_or_else(|e| panic!("[{tag}] {name}: history lost {id}: {e}"));
                for p in cs.parents {
                    if seen.insert(p) {
                        stack.push(p);
                    }
                }
            }
        }

        // The ref log replays to the refs, entry by entry. Against a running
        // server the log is read before the refs, so it may only be behind.
        let mut log = meta.ref_log(None, None, usize::MAX >> 1).await.unwrap();
        log.reverse();
        let refs = meta.list_refs().await.unwrap();
        let mut replay: BTreeMap<String, ChangesetId> = BTreeMap::new();
        for e in &log {
            assert_eq!(
                replay.get(&e.name).copied(),
                e.old,
                "[{tag}] ref log entry {} of {} does not continue the one before",
                e.seq,
                e.name
            );
            match e.new {
                Some(n) => replay.insert(e.name.clone(), n),
                None => replay.remove(&e.name),
            };
        }
        if live {
            for (name, logged) in &replay {
                let now = refs.get(name).copied().unwrap_or(*logged);
                assert!(
                    repo.is_ancestor(*logged, now).await.unwrap(),
                    "[{tag}] ref {name} is behind its log"
                );
            }
        } else {
            assert_eq!(
                replay, refs,
                "[{tag}] the ref log does not replay to the refs"
            );
        }

        // Every land a client was told succeeded is in history.
        if let Some(head) = refs.get("main") {
            let landed: Vec<ChangesetId> = shared
                .confirmed
                .lock()
                .unwrap()
                .iter()
                .map(|c| c.landed)
                .collect();
            for id in landed {
                assert!(
                    repo.is_ancestor(id, *head).await.unwrap(),
                    "[{tag}] {id} was reported landed but is not in main's history"
                );
            }
        }
    });
}

/// `leep-server verify` finds the store sound (ADR 0025): everything
/// reachable is intact, the ref log replays to the refs and, with `--full`,
/// every derived row equals its recomputation. A kill or a collect at any
/// point must leave it so.
fn verify_clean(tag: &str, stores: &Stores, extra: &[&str]) {
    let out = Command::new(bin("leep-server"))
        .args(["--meta", &stores.meta, "--blobs", &stores.blobs, "verify"])
        .args(extra)
        .env("RUST_LOG", "warn")
        .output()
        .expect("spawn leep-server verify");
    assert!(
        out.status.success(),
        "[{tag}] verify {extra:?} exited {}\n--- stdout\n{}\n--- stderr\n{}",
        out.status,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}

fn gc_output(stores: &Stores, args: &[&str]) -> std::process::Output {
    Command::new(bin("leep-server"))
        .args(["--meta", &stores.meta, "--blobs", &stores.blobs, "gc"])
        .args(args)
        .env("RUST_LOG", "warn")
        .output()
        .expect("spawn leep-server gc")
}

fn gc_ok(tag: &str, stores: &Stores, args: &[&str]) -> String {
    let out = gc_output(stores, args);
    assert!(
        out.status.success(),
        "[{tag}] gc {args:?} failed\n{}\n{}",
        stdout(&out),
        stderr(&out)
    );
    stdout(&out)
}

/// With the server dead (so a zero fence is justified, ADR 0012): kill
/// `gc collect` partway through a large delete, check nothing reachable went,
/// and finish the job. Returns the planted blobs for a re-upload check.
fn kill_collect_midway(
    tag: &str,
    stores: &Stores,
    root: &Path,
    rng: &mut Rng,
    round: u64,
) -> Vec<Vec<u8>> {
    let blobs_dir = root.join("data/blobs");
    let planted: Vec<Vec<u8>> = (0..PLANTED)
        .map(|i| format!("planted orphan {round} {i}").into_bytes())
        .collect();
    for data in &planted {
        plant_orphan(&blobs_dir, data);
    }
    let t = Instant::now();
    gc_ok(tag, stores, &["condemn", "--grace", "0"]);
    eprintln!("[{tag}] condemn took {:.1}s", t.elapsed().as_secs_f64());
    let mut child = Command::new(bin("leep-server"))
        .args(["--meta", &stores.meta, "--blobs", &stores.blobs, "gc"])
        .args(["collect", "--fence", "0s", "--unsafe-no-fence"])
        .env("RUST_LOG", "warn")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn gc collect");
    // Kill once a seeded number of blobs are gone, so the kill lands inside
    // the delete loop however long the mark phase takes on a long history.
    let target = 1 + rng.below(PLANTED - 5);
    let gone = || {
        planted
            .iter()
            .filter(|d| !planted_path(&blobs_dir, d).exists())
            .count()
    };
    let deadline = Instant::now() + Duration::from_secs(60);
    while gone() < target && child.try_wait().unwrap().is_none() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(1));
    }
    let finished = child.try_wait().unwrap().is_some();
    let _ = child.kill();
    let _ = child.wait();
    eprintln!(
        "[{tag}] killed gc collect {} ({} of {} planted blobs left)",
        if finished {
            "after it finished"
        } else {
            "mid-run"
        },
        PLANTED - gone(),
        PLANTED
    );
    planted
}

fn planted_path(blobs_dir: &Path, data: &[u8]) -> std::path::PathBuf {
    let hex = Hash::of_blob(data).to_hex();
    blobs_dir.join(&hex[..2]).join(&hex[2..])
}

fn plant_orphan(blobs_dir: &Path, data: &[u8]) {
    let path = planted_path(blobs_dir, data);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, data).unwrap();
}

fn run_crash_rounds(backend: &str, meta_url: Option<String>) {
    let _g = serial();
    let seed = env_u64("LEEP_CRASH_SEED", DEFAULT_SEED);
    let rounds = env_u64("LEEP_CRASH_ROUNDS", 3);
    let http_agents = env_u64("LEEP_CRASH_AGENTS", 6) as usize;
    let started = Instant::now();
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    let stores = match meta_url {
        Some(url) => Stores::with_meta(&root.join("data"), url),
        None => Stores::sqlite(&root.join("data")),
    };
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        crash_rounds(backend, seed, rounds, http_agents, root, &stores)
    }));
    if let Err(e) = outcome {
        eprintln!("crash test failed: backend {backend}, LEEP_CRASH_SEED={seed}, rounds {rounds}");
        std::panic::resume_unwind(e);
    }
    eprintln!(
        "[{backend}] {rounds} kills, seed {seed:#x}, {:.1}s",
        started.elapsed().as_secs_f64()
    );
}

fn crash_rounds(
    backend: &str,
    seed: u64,
    rounds: u64,
    http_agents: usize,
    root: &Path,
    stores: &Stores,
) {
    let port = free_port();
    let server_args = ["--request-timeout", REQUEST_TIMEOUT];
    let mut server = Server::start_with(port, stores, root, &server_args);
    let client = Client::new(&server.url, &root.join("cache"));
    let addr = server.addr;
    let shared = Shared::default();
    let mut rng = Rng::new(seed);
    let mut planted: Vec<Vec<u8>> = Vec::new();

    std::thread::scope(|s| {
        for n in 0..http_agents {
            let shared = &shared;
            s.spawn(move || http_agent(n, seed, addr, shared));
        }
        for n in 0..CLI_AGENTS {
            let (shared, client) = (&shared, &client);
            let dir = root.join("wcs");
            std::fs::create_dir_all(&dir).unwrap();
            s.spawn(move || cli_agent(n, seed, client, &dir, shared));
        }

        let mut live_gc: Option<std::process::Child> = None;
        for round in 0..rounds {
            let tag = format!("{backend} seed {seed:#x} round {round}");
            let confirmed_before = shared.confirmed.lock().unwrap().len();
            // Traffic for a seeded moment, then the plug is pulled.
            std::thread::sleep(Duration::from_millis(500 + rng.below(900) as u64));
            if let Some(mut gc) = live_gc.take() {
                // Let the fenced run that overlapped this traffic finish.
                let st = gc.wait().unwrap();
                assert!(st.success(), "[{tag}] live gc failed");
            }
            server.kill();
            check_store(&format!("{tag}, after SIGKILL"), stores, &shared, false);
            verify_clean(&format!("{tag}, after SIGKILL"), stores, &[]);

            // The server is down, so a zero fence is safe here: collect
            // everything unreferenced and prove nothing else went.
            match round % 3 {
                0 => {
                    let dry = gc_ok(&tag, stores, &["--dry-run", "--grace", "0"]);
                    assert!(dry.contains("would condemn"), "[{tag}] {dry}");
                    check_store(&format!("{tag}, after dry run"), stores, &shared, false);
                    gc_ok(
                        &tag,
                        stores,
                        &["--grace", "0", "--fence", "0s", "--unsafe-no-fence"],
                    );
                    check_store(&format!("{tag}, after offline gc"), stores, &shared, false);
                }
                1 => {
                    planted = kill_collect_midway(&tag, stores, root, &mut rng, round);
                    check_store(
                        &format!("{tag}, after killed collect"),
                        stores,
                        &shared,
                        false,
                    );
                    verify_clean(
                        &format!("{tag}, after killed collect"),
                        stores,
                        &["--rehash"],
                    );
                }
                _ => {}
            }

            server = Server::start_with(port, stores, root, &server_args);
            if !planted.is_empty() {
                recover_planted(&tag, stores, addr, &planted);
                planted.clear();
                check_store(&format!("{tag}, after recovery"), stores, &shared, true);
            }
            if round % 3 == 2 {
                // A fenced run beside live traffic, as an operator would.
                live_gc = Some(
                    Command::new(bin("leep-server"))
                        .args(["--meta", &stores.meta, "--blobs", &stores.blobs, "gc"])
                        .args(["--grace", "0", "--fence", LIVE_FENCE])
                        .env("RUST_LOG", "warn")
                        .stdout(Stdio::null())
                        .spawn()
                        .expect("spawn gc"),
                );
            }
            let confirmed_now = shared.confirmed.lock().unwrap().len();
            eprintln!(
                "[{tag}] {confirmed_now} lands confirmed so far ({} during the last round)",
                confirmed_now - confirmed_before
            );
        }
        if let Some(mut gc) = live_gc.take() {
            assert!(gc.wait().unwrap().success(), "live gc failed");
        }
        // Let the agents finish what they started against a healthy server.
        std::thread::sleep(Duration::from_millis(400));
        shared.stop.store(true, Ordering::Relaxed);
    });

    let tag = format!("{backend} seed {seed:#x} final");
    check_store(&tag, stores, &shared, false);
    let odd = shared.odd.lock().unwrap();
    assert!(
        odd.is_empty(),
        "[{tag}] unexpected answers: {:?}",
        &odd[..odd.len().min(5)]
    );
    drop(odd);
    verify_exact(&tag, stores, &shared);
    verify_through_server(&tag, root, &client, &shared);
    verify_clean(&tag, stores, &["--full", "--rehash"]);
    drop(server);
}

/// After a killed collect: finish it, then every planted blob must be
/// gone, and uploading the same bytes again must work (after at most one more
/// collect for a blob caught between "deleting" and "deleted", ADR 0012).
fn recover_planted(tag: &str, stores: &Stores, addr: SocketAddr, planted: &[Vec<u8>]) {
    let t = Instant::now();
    let done = gc_ok(
        tag,
        stores,
        &["collect", "--fence", "0s", "--unsafe-no-fence"],
    );
    eprintln!(
        "[{tag}] finishing collect took {:.1}s",
        t.elapsed().as_secs_f64()
    );
    let blobs_dir = Path::new(stores.blobs.strip_prefix("file://").unwrap());
    let left: Vec<_> = planted
        .iter()
        .filter(|d| planted_path(blobs_dir, d).exists())
        .collect();
    assert!(
        left.is_empty(),
        "[{tag}] {} planted blobs survive collect:\n{done}",
        left.len()
    );
    let mut retried = false;
    for data in planted.iter().step_by(7) {
        let h = Hash::of_blob(data);
        let put = || http(addr, "PUT", &format!("/v1/blobs/{h}"), data).expect("put");
        let mut status = put().0;
        if status == 409 && !retried {
            retried = true;
            gc_ok(
                tag,
                stores,
                &["collect", "--fence", "0s", "--unsafe-no-fence"],
            );
            status = put().0;
        }
        assert!(
            status == 204 || status == 409,
            "[{tag}] upload of a collected blob answered {status}"
        );
        if status == 409 {
            // Several blobs can be in that state; one collect closes them all.
            gc_ok(
                tag,
                stores,
                &["collect", "--fence", "0s", "--unsafe-no-fence"],
            );
            assert_eq!(put().0, 204, "[{tag}] blob still blocked after a collect");
        }
    }
}

/// Every commit once, every file as written, one ref move per land.
fn verify_exact(tag: &str, stores: &Stores, shared: &Shared) {
    let repo = open_repo(stores);
    common_block_on(async {
        let confirmed: Vec<Vec<Step>> = shared
            .confirmed
            .lock()
            .unwrap()
            .iter()
            .map(|c| c.steps.clone())
            .collect();
        let head = repo
            .meta()
            .get_ref("main")
            .await
            .unwrap()
            .expect("main exists");
        let mut seen = HashSet::from([head]);
        let mut stack = vec![head];
        let mut messages: HashMap<String, u32> = HashMap::new();
        while let Some(id) = stack.pop() {
            let cs = repo.get_changeset(id).await.unwrap();
            *messages.entry(cs.message).or_default() += 1;
            for p in cs.parents {
                if seen.insert(p) {
                    stack.push(p);
                }
            }
        }
        let dups: Vec<_> = messages.iter().filter(|(_, n)| **n > 1).collect();
        assert!(dups.is_empty(), "[{tag}] landed more than once: {dups:?}");
        let steps: Vec<&Step> = confirmed.iter().flatten().collect();
        assert_eq!(
            messages.len(),
            steps.len(),
            "[{tag}] history has {} commits, clients landed {}",
            messages.len(),
            steps.len()
        );
        for s in &steps {
            assert!(
                messages.contains_key(&s.msg),
                "[{tag}] {} is not in history",
                s.msg
            );
            let (_, data) = repo
                .read_file(head, &RepoPath::new(s.path.as_str()).unwrap())
                .await
                .unwrap_or_else(|e| panic!("[{tag}] {}: {e}", s.path))
                .unwrap_or_else(|| panic!("[{tag}] {} is missing at main", s.path));
            assert_eq!(&data[..], s.body.as_bytes(), "[{tag}] {}", s.path);
        }
        let moves = repo
            .meta()
            .ref_log(Some("main"), None, usize::MAX >> 1)
            .await
            .unwrap();
        assert_eq!(
            moves.len(),
            confirmed.len(),
            "[{tag}] {} ref moves for {} lands: a retried land moved the ref again",
            moves.len(),
            confirmed.len()
        );
    });
}

/// The same data, read back the way a client would after the chaos.
fn verify_through_server(tag: &str, root: &Path, client: &Client, shared: &Shared) {
    let wc = root.join("fresh");
    client.ok(root, &["init", wc.to_str().unwrap()]);
    client.ok(&wc, &["sync"]);
    for c in shared.confirmed.lock().unwrap().iter() {
        for s in &c.steps {
            assert_eq!(
                std::fs::read_to_string(wc.join(&s.path)).unwrap_or_default(),
                s.body,
                "[{tag}] {} through the server",
                s.path
            );
        }
    }
}

#[test]
fn killed_server_and_gc_lose_nothing_sqlite() {
    run_crash_rounds("sqlite", None);
}

#[test]
fn killed_server_and_gc_lose_nothing_postgres() {
    let Some(db) = PgDb::create() else {
        return;
    };
    run_crash_rounds("postgres", Some(db.url.clone()));
    drop(db);
}
