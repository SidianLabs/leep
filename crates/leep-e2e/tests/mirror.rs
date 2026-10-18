//! The live git mirror (ADR 0046) with the real binaries: `leep-server`,
//! `leep` agents and a `leep-git mirror run` daemon pushing to a local bare
//! repository that stands in for GitHub, while a person pushes to it with
//! plain git.
//!
//! - lands appear on the remote and pushes to the remote land in Leep;
//! - SIGKILL at seeded random moments during exports, pushes and imports
//!   loses and duplicates nothing, and no update of the remote is ever
//!   anything but a fast-forward;
//! - agents landing while a person pushes end with both sides holding every
//!   change, with any divergence reported and resolved by a merge in git.
//!
//! Knobs: `LEEP_MIRROR_KILL_ROUNDS` (default 12) and `LEEP_MIRROR_SEED`. Run
//! `cargo build --workspace` first.

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use common::*;
use leep_testkit::Rng;

const DEFAULT_SEED: u64 = 0x6172_6F72;
/// The CLI's author; the mirror exports it as the commit author.
const TIMEOUT: Duration = Duration::from_secs(60);

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// A bare repository standing in for the hosted remote. Every update of
/// `main` goes into its reflog, so the test can check each was a
/// fast-forward.
fn make_remote(root: &Path) -> PathBuf {
    let remote = root.join("remote.git");
    fs::create_dir_all(&remote).unwrap();
    git(&remote, &["init", "-q", "--bare", "-b", "main"]);
    git(&remote, &["config", "core.logAllRefUpdates", "always"]);
    remote
}

/// A running `leep-git mirror run`; SIGKILLed on drop.
struct Mirror {
    child: Option<Child>,
    work: PathBuf,
    log: PathBuf,
    listen: String,
}

impl Mirror {
    fn start(
        server: &Server,
        remote: &Path,
        work: &Path,
        log_dir: &Path,
        extra: &[&str],
    ) -> Mirror {
        Self::start_env(server, remote, work, log_dir, extra, None)
    }

    /// Start with `LEEP_MIRROR_CRASH_AT` set, so it aborts at that step.
    fn start_env(
        server: &Server,
        remote: &Path,
        work: &Path,
        log_dir: &Path,
        extra: &[&str],
        crash_at: Option<&str>,
    ) -> Mirror {
        let log = log_dir.join("mirror.log");
        let out = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log)
            .unwrap();
        let err = out.try_clone().unwrap();
        let listen = format!("127.0.0.1:{}", free_port());
        let mut cmd = Command::new(bin("leep-git"));
        match crash_at {
            Some(p) => cmd.env("LEEP_MIRROR_CRASH_AT", p),
            None => cmd.env_remove("LEEP_MIRROR_CRASH_AT"),
        };
        let child = cmd
            .args(["mirror", "run", "--server", &server.url])
            .args(["--remote", remote.to_str().unwrap()])
            .args(["--work-dir", work.to_str().unwrap()])
            .args(["--ref", "main", "--import"])
            .args(["--interval", "100ms", "--git-poll", "300ms"])
            .args(["--max-backoff", "1s", "--listen", &listen])
            .args(extra)
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .stdin(Stdio::null())
            .stdout(out)
            .stderr(err)
            .spawn()
            .expect("spawn leep-git mirror");
        Mirror {
            child: Some(child),
            work: work.to_path_buf(),
            log,
            listen,
        }
    }

    /// Wait for the process to exit by itself (a crash point).
    fn wait_exit(&mut self, timeout: Duration) -> std::process::ExitStatus {
        let deadline = Instant::now() + timeout;
        let c = self.child.as_mut().expect("mirror not running");
        loop {
            if let Some(st) = c.try_wait().unwrap() {
                self.child = None;
                return st;
            }
            assert!(
                Instant::now() < deadline,
                "the mirror did not reach its crash point:\n{}",
                self.log_text()
            );
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    fn kill(&mut self) {
        if let Some(mut c) = self.child.take() {
            let _ = c.kill();
            let _ = c.wait();
        }
    }

    fn log_text(&self) -> String {
        fs::read_to_string(&self.log).unwrap_or_default()
    }

    /// `leep-git mirror status --json`: the exit code and the document.
    fn status(&self) -> (i32, serde_json::Value) {
        let out = Command::new(bin("leep-git"))
            .args(["mirror", "status", "--json", "--work-dir"])
            .arg(&self.work)
            .output()
            .unwrap();
        let v = serde_json::from_slice(&out.stdout).unwrap_or(serde_json::Value::Null);
        (out.status.code().unwrap_or(-1), v)
    }

    fn main_status(&self) -> serde_json::Value {
        let (_, v) = self.status();
        v["refs"]
            .as_array()
            .and_then(|r| r.iter().find(|r| r["leep_ref"] == "main").cloned())
            .unwrap_or(serde_json::Value::Null)
    }
}

impl Drop for Mirror {
    fn drop(&mut self) {
        self.kill();
    }
}

fn wait_until(
    what: &str,
    timeout: Duration,
    mut f: impl FnMut() -> bool,
    ctx: impl Fn() -> String,
) {
    let deadline = Instant::now() + timeout;
    while !f() {
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {what}\n{}",
            ctx()
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn leep_main(server: &Server) -> Option<String> {
    let (code, _, body) = http_get(server.addr, "/v1/refs/main", None);
    assert_eq!(code, 200, "{body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    v["target"].as_str().map(String::from)
}

fn remote_ref(remote: &Path, name: &str) -> Option<String> {
    let out = git(remote, &["for-each-ref", "--format=%(objectname)", name]);
    let out = out.trim();
    (!out.is_empty()).then(|| out.to_string())
}

/// Both sides agree: the mirror is in sync with Leep's current main and the
/// remote holds what the mirror exported.
fn converged(server: &Server, remote: &Path, m: &Mirror) -> bool {
    let st = m.main_status();
    let leep = leep_main(server);
    st["state"] == "in-sync"
        && leep.is_some()
        && st["leep"].as_str() == leep.as_deref()
        && st["remote"].as_str().is_some()
        && st["remote"].as_str() == remote_ref(remote, "refs/heads/main").as_deref()
}

fn wait_converged(server: &Server, remote: &Path, m: &Mirror) {
    wait_until(
        "the mirror to converge",
        TIMEOUT,
        || converged(server, remote, m),
        || format!("status: {}\nlog:\n{}", m.main_status(), m.log_text()),
    );
}

/// One working copy that commits a file and lands it, retrying while the
/// server answers that someone else moved the ref.
struct Agent {
    client: Client,
    wc: PathBuf,
}

impl Agent {
    fn new(client: &Client, root: &Path, name: &str) -> Agent {
        let wc = root.join(name);
        client.ok(root, &["init", wc.to_str().unwrap()]);
        Agent {
            client: client.clone(),
            wc,
        }
    }

    fn land(&self, path: &str, body: &str) {
        write(&self.wc, path, body);
        self.client
            .ok(&self.wc, &["commit", "-m", &format!("add {path}")]);
        for _ in 0..20 {
            let out = self.client.run(&self.wc, &["land"]);
            if out.status.success() {
                return;
            }
            let text = format!("{}{}", stdout(&out), stderr(&out));
            assert!(
                !text.contains("Cannot land cleanly"),
                "agents write distinct files and never conflict: {text}"
            );
            std::thread::sleep(Duration::from_millis(50));
        }
        panic!("land of {path} kept failing");
    }
}

/// A person working in a git clone of the remote.
struct Person {
    dir: PathBuf,
}

impl Person {
    fn clone(remote: &Path, root: &Path) -> Person {
        git(root, &["clone", "-q", remote.to_str().unwrap(), "person"]);
        Person {
            dir: root.join("person"),
        }
    }

    fn git(&self, args: &[&str]) -> String {
        git(&self.dir, args)
    }

    fn try_git(&self, args: &[&str]) -> bool {
        Command::new("git")
            .current_dir(&self.dir)
            .args(args)
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("GIT_AUTHOR_NAME", "Person")
            .env("GIT_AUTHOR_EMAIL", "person@example.com")
            .env("GIT_COMMITTER_NAME", "Person")
            .env("GIT_COMMITTER_EMAIL", "person@example.com")
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false)
    }

    /// Bring local main up to date with the remote, merging Leep's side of
    /// a reported divergence; true if there was one to merge.
    fn catch_up(&self) -> bool {
        self.git(&["fetch", "-q", "--prune", "origin"]);
        if self.try_git(&["rev-parse", "-q", "--verify", "refs/remotes/origin/main"]) {
            self.git(&["merge", "-q", "--no-edit", "origin/main"]);
        }
        let diverged = "refs/remotes/origin/leep-diverged/main";
        let has_side = self.try_git(&["rev-parse", "-q", "--verify", diverged]);
        if has_side && !self.try_git(&["merge-base", "--is-ancestor", diverged, "HEAD"]) {
            self.git(&["merge", "-q", "--no-edit", diverged]);
            return true;
        }
        false
    }

    /// Commit a file and push it, catching up and retrying when the remote
    /// moved first.
    fn push_file(&self, path: &str, body: &str) -> String {
        write(&self.dir, path, body);
        self.git(&["add", "-A"]);
        self.git(&["commit", "-q", "-m", &format!("person adds {path}")]);
        for _ in 0..100 {
            if self.try_git(&["push", "-q", "origin", "main"]) {
                return self.git(&["rev-parse", "HEAD"]).trim().to_string();
            }
            self.catch_up();
        }
        panic!("push of {path} kept being rejected");
    }

    /// Push a merge of Leep's side if the mirror reported a divergence.
    fn resolve(&self) -> bool {
        if self.catch_up() {
            return self.try_git(&["push", "-q", "origin", "main"]);
        }
        false
    }
}

/// Every update of the remote's main was a fast-forward: the mirror never
/// forced it, and nobody else did either.
fn assert_remote_only_fast_forwarded(remote: &Path) {
    let log = git(
        remote,
        &["reflog", "show", "--format=%H", "refs/heads/main"],
    );
    let shas: Vec<&str> = log.lines().collect();
    for pair in shas.windows(2) {
        let (new, old) = (pair[0], pair[1]);
        let ok = Command::new("git")
            .current_dir(remote)
            .args(["merge-base", "--is-ancestor", old, new])
            .status()
            .unwrap()
            .success();
        assert!(
            ok,
            "remote main moved from {old} to {new}, not a fast-forward"
        );
    }
}

/// Leep's main history and the remote's main history describe the same
/// changesets, each exactly once: native changesets by their
/// `Leep-Changeset:` trailer and imported ones by their `git.commit`.
fn assert_histories_match(client: &Client, wc: &Path, remote: &Path) {
    let log = client.json(wc, &["log", "main", "-n", "10000"]);
    let entries = log["entries"].as_array().unwrap();
    let mut leep_ids = BTreeSet::new();
    let mut from_git = BTreeMap::new();
    for e in entries {
        let id = e["id"].as_str().unwrap().to_string();
        if let Some(sha) = e["changeset"]["extra"]["git.commit"].as_str() {
            from_git.insert(sha.to_string(), id.clone());
        }
        assert!(leep_ids.insert(id), "changeset listed twice");
    }
    let commits = git(remote, &["log", "--format=%H%x00%B%x01", "main"]);
    let mut seen = BTreeSet::new();
    for rec in commits
        .split('\u{1}')
        .map(str::trim)
        .filter(|r| !r.is_empty())
    {
        let (sha, body) = rec.split_once('\0').unwrap();
        let id = match body
            .lines()
            .rev()
            .find_map(|l| l.strip_prefix("Leep-Changeset: "))
        {
            Some(id) => id.trim().to_string(),
            None => from_git
                .get(sha)
                .unwrap_or_else(|| panic!("remote commit {sha} is in no Leep changeset:\n{body}"))
                .clone(),
        };
        assert!(
            leep_ids.contains(&id),
            "commit {sha} names {id}, not on Leep main"
        );
        assert!(
            seen.insert(id.clone()),
            "changeset {id} has two commits on the remote"
        );
    }
    assert_eq!(
        seen.len(),
        leep_ids.len(),
        "every changeset on Leep main has exactly one commit on the remote"
    );
}

/// The files of Leep's main (through a fresh working copy) and of the
/// remote's main are the same.
fn assert_trees_match(
    client: &Client,
    root: &Path,
    remote: &Path,
    tag: &str,
) -> BTreeMap<String, String> {
    let wc = root.join(format!("check-{tag}"));
    client.ok(root, &["init", wc.to_str().unwrap()]);
    let checkout = root.join(format!("git-check-{tag}"));
    git(
        root,
        &[
            "clone",
            "-q",
            remote.to_str().unwrap(),
            checkout.to_str().unwrap(),
        ],
    );
    let list = |dir: &Path, skip: &str| -> BTreeMap<String, String> {
        let mut out = BTreeMap::new();
        let mut stack = vec![dir.to_path_buf()];
        while let Some(d) = stack.pop() {
            for e in fs::read_dir(&d).unwrap().flatten() {
                let p = e.path();
                let rel = p.strip_prefix(dir).unwrap().to_str().unwrap().to_string();
                if rel == skip {
                    continue;
                }
                if p.is_dir() {
                    stack.push(p);
                } else {
                    out.insert(rel, fs::read_to_string(&p).unwrap());
                }
            }
        }
        out
    };
    let leep = list(&wc, ".leep");
    let gitf = list(&checkout, ".git");
    assert_eq!(
        leep, gitf,
        "Leep main and the remote's main hold different files"
    );
    leep
}

fn run_follow_and_import(stores: &Stores, root: &Path) {
    let server = Server::start(stores, root);
    let client = Client::new(&server.url, &root.join("cache"));
    let remote = make_remote(root);
    let agent = Agent::new(&client, root, "agent");
    agent.land("a.txt", "one\n");

    let mut m = Mirror::start(&server, &remote, &root.join("work"), root, &[]);
    wait_converged(&server, &remote, &m);
    let first = leep_main(&server).unwrap();
    let body = git(&remote, &["log", "-1", "--format=%B", "main"]);
    assert!(body.contains(&format!("Leep-Changeset: {first}")), "{body}");

    // A land reaches the remote within seconds.
    let before = Instant::now();
    agent.land("b.txt", "two\n");
    wait_converged(&server, &remote, &m);
    assert!(before.elapsed() < Duration::from_secs(10));
    assert_eq!(git(&remote, &["show", "main:b.txt"]), "two\n");

    // A push to the remote lands in Leep, through the land policy, labelled
    // in the ref log, and is not exported back as a new commit.
    let person = Person::clone(&remote, root);
    let pushed = person.push_file("person.txt", "from git\n");
    wait_until(
        "the push to land in Leep",
        TIMEOUT,
        || {
            converged(&server, &remote, &m)
                && leep_main(&server).as_deref() != Some(first.as_str())
                && remote_ref(&remote, "refs/heads/main").as_deref() == Some(pushed.as_str())
        },
        || m.log_text(),
    );
    let reflog = client.json(&agent.wc, &["reflog", "main", "-n", "1"]);
    assert_eq!(reflog["entries"][0]["operation"], "git-mirror", "{reflog}");
    agent.client.ok(&agent.wc, &["sync"]);
    assert_eq!(
        fs::read_to_string(agent.wc.join("person.txt")).unwrap(),
        "from git\n"
    );

    // Status, metrics and the poke endpoint.
    let (code, st) = m.status();
    assert_eq!(code, 0, "{st}");
    let addr = m.listen.parse().unwrap();
    let (code, _, metrics) = http_get(addr, "/metrics", None);
    assert_eq!(code, 200);
    assert!(
        metrics.contains("leep_mirror_ref_state{ref=\"main\",state=\"in-sync\"} 1"),
        "{metrics}"
    );
    assert!(metrics.contains("leep_mirror_imports_total{ref=\"main\",result=\"landed\"} 1"));
    let (code, _) = post_json(addr, "/poke", "");
    assert_eq!(code, 202);

    // Leep lands while the person pushes: reported as diverged, Leep's side
    // goes to leep-diverged/main, and a merge in git resolves it.
    m.kill();
    agent.land("c.txt", "three\n");
    let theirs = person.push_file("person2.txt", "again\n");
    let m = Mirror::start(&server, &remote, &root.join("work"), root, &[]);
    wait_until(
        "the divergence to be reported",
        TIMEOUT,
        || {
            m.main_status()["state"] == "diverged"
                && remote_ref(&remote, "refs/heads/leep-diverged/main").is_some()
        },
        || format!("{}\n{}", m.main_status(), m.log_text()),
    );
    let (code, _) = m.status();
    assert_eq!(code, 1, "a diverged ref makes status exit 1");
    assert_eq!(
        remote_ref(&remote, "refs/heads/main").as_deref(),
        Some(theirs.as_str())
    );
    assert!(person.resolve(), "the person merges Leep's side and pushes");
    wait_converged(&server, &remote, &m);
    wait_until(
        "the diverged branch to be removed",
        TIMEOUT,
        || remote_ref(&remote, "refs/heads/leep-diverged/main").is_none(),
        || m.log_text(),
    );
    let files = assert_trees_match(&client, root, &remote, "final");
    for f in ["a.txt", "b.txt", "c.txt", "person.txt", "person2.txt"] {
        assert!(files.contains_key(f), "{f} missing: {files:?}");
    }
    assert_histories_match(&client, &agent.wc, &remote);
    assert_remote_only_fast_forwarded(&remote);
    git(&remote, &["fsck", "--strict", "--no-dangling"]);
    let log = m.log_text();
    assert!(!log.contains("panicked"), "{log}");
}

#[test]
fn mirror_follows_lands_and_imports_pushes() {
    let _g = serial();
    let tmp = tempfile::tempdir().unwrap();
    let stores = Stores::sqlite(&tmp.path().join("data"));
    run_follow_and_import(&stores, tmp.path());
}

#[test]
fn mirror_follows_lands_and_imports_pushes_postgres() {
    let _g = serial();
    let Some(db) = PgDb::create() else {
        return;
    };
    let tmp = tempfile::tempdir().unwrap();
    let stores = Stores::with_meta(&tmp.path().join("data"), db.url.clone());
    run_follow_and_import(&stores, tmp.path());
}

/// SIGKILL the mirror at seeded random moments while it exports a backlog,
/// pushes it, and imports pushes from the remote; then let one run finish.
#[test]
fn killed_mirror_loses_and_duplicates_nothing() {
    let _g = serial();
    let seed = env_u64("LEEP_MIRROR_SEED", DEFAULT_SEED);
    let rounds = env_u64("LEEP_MIRROR_KILL_ROUNDS", 12);
    eprintln!("mirror kill sweep: seed {seed:#x}, {rounds} rounds");
    let mut rng = Rng::new(seed);
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    let stores = Stores::sqlite(&root.join("data"));
    let server = Server::start(&stores, root);
    let client = Client::new(&server.url, &root.join("cache"));
    let remote = make_remote(root);
    let agent = Agent::new(&client, root, "agent");
    agent.land("seed.txt", "seed\n");
    let work = root.join("work");
    {
        let m = Mirror::start(&server, &remote, &work, root, &[]);
        wait_converged(&server, &remote, &m);
    }
    let person = Person::clone(&remote, root);
    let mut kills = 0;
    for round in 0..rounds {
        // 0: a backlog of lands to export and push; 1: pushes to import;
        // 2: both at once, which diverges and is merged by the person.
        let kind = rng.below(3);
        // Each round starts from both sides agreeing (the killed runs of the
        // previous round may have left work behind), so its kill lands in
        // the round's own export, push or import.
        let m = Mirror::start(&server, &remote, &work, root, &[]);
        wait_until(
            "the mirror to converge between rounds",
            TIMEOUT,
            || {
                if m.main_status()["state"] == "diverged" {
                    person.resolve();
                }
                converged(&server, &remote, &m)
            },
            || format!("seed {seed:#x} round {round}\n{}", m.log_text()),
        );
        drop(m);
        person.catch_up();
        if kind != 1 {
            for k in 0..1 + rng.below(5) {
                agent.land(
                    &format!("r{round}/agent-{k}.txt"),
                    &format!("{round} {k}\n"),
                );
            }
        }
        if kind != 0 {
            for k in 0..1 + rng.below(3) {
                person.push_file(&format!("r{round}/person-{k}.txt"), "p\n");
            }
        }
        let mut m = Mirror::start(&server, &remote, &work, root, &[]);
        let delay = rng.below(700) as u64;
        std::thread::sleep(Duration::from_millis(delay));
        m.kill();
        kills += 1;
        let log = m.log_text();
        assert!(
            !log.contains("panicked"),
            "round {round} (seed {seed:#x}):\n{log}"
        );
    }
    eprintln!("mirror kill sweep: {kills} kills");

    // A run left alone converges, with the person resolving divergences.
    let m = Mirror::start(&server, &remote, &work, root, &[]);
    wait_until(
        "the mirror to converge after the kills",
        TIMEOUT,
        || {
            if m.main_status()["state"] == "diverged" {
                person.resolve();
            }
            converged(&server, &remote, &m)
        },
        || format!("seed {seed:#x}\n{}\n{}", m.main_status(), m.log_text()),
    );
    let log = m.log_text();
    eprintln!(
        "mirror kill sweep: {} pushes, {} imports, {} divergences across all runs",
        log.matches(": pushed ").count(),
        log.matches(": landed ").count(),
        log.matches(": diverged: ").count()
    );
    assert_histories_match(&client, &agent.wc, &remote);
    assert_remote_only_fast_forwarded(&remote);
    assert_trees_match(&client, root, &remote, "kills");
    git(&remote, &["fsck", "--strict", "--no-dangling"]);
}

/// Agents land through Leep while a person pushes to the remote, all at
/// once. Every change ends up on both sides.
#[test]
fn agents_land_while_a_person_pushes_to_git() {
    let _g = serial();
    const AGENTS: usize = 3;
    const LANDS: usize = 5;
    const PUSHES: usize = 5;
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    let stores = Stores::sqlite(&root.join("data"));
    let server = Server::start(&stores, root);
    let client = Client::new(&server.url, &root.join("cache"));
    let remote = make_remote(root);
    let first = Agent::new(&client, root, "agent-first");
    first.land("start.txt", "start\n");
    let m = Mirror::start(&server, &remote, &root.join("work"), root, &[]);
    wait_converged(&server, &remote, &m);
    let person = Person::clone(&remote, root);

    let agents: Vec<Agent> = (0..AGENTS)
        .map(|n| Agent::new(&client, root, &format!("agent-{n}")))
        .collect();
    std::thread::scope(|s| {
        for (n, a) in agents.iter().enumerate() {
            s.spawn(move || {
                for k in 0..LANDS {
                    a.land(
                        &format!("agents/{n}-{k}.txt"),
                        &format!("agent {n} land {k}\n"),
                    );
                }
            });
        }
        s.spawn(|| {
            for k in 0..PUSHES {
                person.catch_up();
                person.push_file(&format!("people/{k}.txt"), &format!("push {k}\n"));
                std::thread::sleep(Duration::from_millis(100));
            }
        });
    });
    wait_until(
        "both sides to hold every change",
        TIMEOUT,
        || {
            if m.main_status()["state"] == "diverged" {
                person.resolve();
            }
            converged(&server, &remote, &m)
        },
        || format!("{}\n{}", m.main_status(), m.log_text()),
    );
    let files = assert_trees_match(&client, root, &remote, "concurrent");
    for n in 0..AGENTS {
        for k in 0..LANDS {
            assert!(
                files.contains_key(&format!("agents/{n}-{k}.txt")),
                "agent {n} land {k} lost"
            );
        }
    }
    for k in 0..PUSHES {
        assert!(
            files.contains_key(&format!("people/{k}.txt")),
            "push {k} lost"
        );
    }
    assert_histories_match(&client, &first.wc, &remote);
    assert_remote_only_fast_forwarded(&remote);
    let st = m.main_status();
    eprintln!(
        "concurrent writers: pushes {} imports {} divergences {}",
        st["pushes"], st["imports"], st["divergences"]
    );
}

/// Abort the mirror exactly between two steps (`LEEP_MIRROR_CRASH_AT`) and
/// check a restart finishes the job once: in the middle of an export, after
/// the export and before the push, after the push and before recording it
/// (an acknowledgement lost), after an import and before the Leep ref moves,
/// after it moved, and after Leep's side of a divergence was pushed.
#[test]
fn crash_points_resume_exactly_once() {
    use std::os::unix::process::ExitStatusExt;
    let _g = serial();
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    let stores = Stores::sqlite(&root.join("data"));
    let server = Server::start(&stores, root);
    let client = Client::new(&server.url, &root.join("cache"));
    let remote = make_remote(root);
    let agent = Agent::new(&client, root, "agent");
    agent.land("seed.txt", "seed\n");
    let work = root.join("work");
    let person = {
        let m = Mirror::start(&server, &remote, &work, root, &[]);
        wait_converged(&server, &remote, &m);
        Person::clone(&remote, root)
    };
    let mirror_moves = || {
        let log = client.json(&agent.wc, &["reflog", "main", "-n", "1000"]);
        log["entries"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|e| e["operation"] == "git-mirror")
            .count()
    };
    for (n, point) in [
        "export-commit",
        "exported",
        "pushed",
        "imported",
        "landed",
        "diverged-pushed",
    ]
    .into_iter()
    .enumerate()
    {
        person.catch_up();
        let moves = mirror_moves();
        let pushes = matches!(point, "imported" | "landed" | "diverged-pushed");
        let lands = matches!(
            point,
            "export-commit" | "exported" | "pushed" | "diverged-pushed"
        );
        if lands {
            for k in 0..3 {
                agent.land(&format!("{point}/agent-{k}.txt"), &format!("{n} {k}\n"));
            }
        }
        if pushes {
            for k in 0..2 {
                person.push_file(&format!("{point}/person-{k}.txt"), "p\n");
            }
        }
        let mut m = Mirror::start_env(&server, &remote, &work, root, &[], Some(point));
        let st = m.wait_exit(TIMEOUT);
        assert_eq!(st.signal(), Some(6), "{point}: expected an abort, got {st}");
        let m = Mirror::start(&server, &remote, &work, root, &[]);
        wait_until(
            &format!("convergence after a crash at {point}"),
            TIMEOUT,
            || {
                if m.main_status()["state"] == "diverged" {
                    person.resolve();
                }
                converged(&server, &remote, &m)
            },
            || format!("{}\n{}", m.main_status(), m.log_text()),
        );
        if point == "pushed" {
            // The push had landed; the restart found it there and pushed nothing.
            assert_eq!(m.main_status()["pushes"]["ok"], 0, "{point}");
        }
        if matches!(point, "imported" | "landed") {
            assert_eq!(
                mirror_moves(),
                moves + 1,
                "{point}: the push landed exactly once"
            );
        }
        drop(m);
        assert_histories_match(&client, &agent.wc, &remote);
        assert_remote_only_fast_forwarded(&remote);
    }
    assert_trees_match(&client, root, &remote, "crash-points");
    git(&remote, &["fsck", "--strict", "--no-dangling"]);
}
