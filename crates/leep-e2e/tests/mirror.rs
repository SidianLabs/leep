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
//!   change, with any divergence reported and resolved by a merge in git;
//! - with `--on-divergence merge` and `--land-branches` (ADR 0049), clean
//!   divergences are merged by the mirror, land branches land through the
//!   server, conflicts are reported and left alone, and crashes at every new
//!   step neither lose nor repeat a merge or a land;
//! - with `--primary git` (ADR 0062), the remote wins every race: agents'
//!   lands are replayed onto a person's merges with no divergence, a Leep
//!   undo is restored, and crashes and kills at every replay step neither
//!   import nor push anything twice;
//! - with `--ref-prefix review/ --import`, review branches are two-way: a
//!   person's push during an agent's squash lands on the squash, and a
//!   branch deleted on the remote deletes the ref only once its work landed.
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
/// The flags of a mirror that merges clean divergences and lands land
/// branches.
const MERGING: &[&str] = &["--on-divergence", "merge", "--land-branches"];
/// The flags of a mirror for which the remote is main's source of truth.
const GIT_PRIMARY: &[&str] = &["--primary", "git"];
/// The flags of a mirror with two-way review branches.
const TWO_WAY: &[&str] = &["--ref-prefix", "review/"];

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

    fn land_branches(&self) -> Vec<serde_json::Value> {
        let (_, v) = self.status();
        v["land_branches"].as_array().cloned().unwrap_or_default()
    }

    fn main_status(&self) -> serde_json::Value {
        self.ref_status("main")
    }

    fn ref_status(&self, name: &str) -> serde_json::Value {
        let (_, v) = self.status();
        v["refs"]
            .as_array()
            .and_then(|r| r.iter().find(|r| r["leep_ref"] == name).cloned())
            .unwrap_or(serde_json::Value::Null)
    }
}

/// The mirror stopped at its crash point by aborting, not by an error.
fn assert_aborted(st: std::process::ExitStatus, point: &str) {
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        assert_eq!(st.signal(), Some(6), "{point}: expected an abort, got {st}");
    }
    #[cfg(not(unix))]
    assert!(!st.success(), "{point}: expected an abort, got {st}");
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
    leep_ref(server, "main")
}

fn leep_ref(server: &Server, name: &str) -> Option<String> {
    let (code, _, body) = http_get(server.addr, &format!("/v1/refs/{name}"), None);
    assert_eq!(code, 200, "{body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    v["target"].as_str().map(String::from)
}

/// A changeset as the server stores it.
fn changeset(server: &Server, id: &str) -> serde_json::Value {
    let (code, _, body) = http_get(server.addr, &format!("/v1/changesets/{id}"), None);
    assert_eq!(code, 200, "{body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    v["changeset"].clone()
}

/// Move a Leep ref by compare-and-swap, as an agent rewriting its own
/// review ref does.
fn move_ref(server: &Server, name: &str, from: &str, to: &str) {
    let body = format!(
        r#"{{"expected":"{from}","new":"{to}","operation":"squash","message":"agent squash"}}"#
    );
    let (code, resp) = post_json(server.addr, &format!("/v1/refs/{name}"), &body);
    assert_eq!(code, 200, "{resp}");
    let v: serde_json::Value = serde_json::from_str(&resp).unwrap();
    assert_eq!(v["ok"], true, "{resp}");
}

/// The number of updates of a branch on the remote, from its reflog.
fn remote_updates(remote: &Path, branch: &str) -> usize {
    git(
        remote,
        &[
            "reflog",
            "show",
            "--format=%H",
            &format!("refs/heads/{branch}"),
        ],
    )
    .lines()
    .count()
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

    /// Returns the changeset committed, which the server may land as a
    /// rebased copy.
    fn land(&self, path: &str, body: &str) -> String {
        write(&self.wc, path, body);
        let id = self
            .client
            .ok(&self.wc, &["commit", "-m", &format!("add {path}")])
            .trim()
            .to_string();
        for _ in 0..20 {
            let out = self.client.run(&self.wc, &["land"]);
            if out.status.success() {
                return id;
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

    /// Commit a file on top of the remote's main and push it to the land
    /// branch `leep-land/main/<name>`; local main is left as it was.
    fn push_land(&self, name: &str, path: &str, body: &str) -> String {
        self.git(&["fetch", "-q", "origin"]);
        self.git(&["checkout", "-q", "-B", "landing", "origin/main"]);
        write(&self.dir, path, body);
        self.git(&["add", "-A"]);
        self.git(&["commit", "-q", "-m", &format!("person lands {path}")]);
        self.git(&[
            "push",
            "-q",
            "origin",
            &format!("HEAD:refs/heads/leep-land/main/{name}"),
        ]);
        let sha = self.git(&["rev-parse", "HEAD"]).trim().to_string();
        self.git(&["checkout", "-q", "main"]);
        sha
    }

    /// Merge a pull request the way the merge button does: a commit on a
    /// branch off the remote's main, merged with `--no-ff` into whatever
    /// main is when the push goes through. Returns the merge pushed.
    fn merge_pr(&self, name: &str, path: &str, body: &str) -> String {
        let pr = format!("pr/{name}");
        self.git(&["fetch", "-q", "origin"]);
        self.git(&["checkout", "-q", "-B", &pr, "origin/main"]);
        write(&self.dir, path, body);
        self.git(&["add", "-A"]);
        self.git(&["commit", "-q", "-m", &format!("person adds {path}")]);
        for _ in 0..100 {
            self.git(&["fetch", "-q", "origin"]);
            self.git(&["checkout", "-q", "-B", "main", "origin/main"]);
            self.git(&[
                "merge",
                "-q",
                "--no-ff",
                "--no-edit",
                "-m",
                &format!("Merge pull request {name}"),
                &pr,
            ]);
            if self.try_git(&["push", "-q", "origin", "main"]) {
                return self.git(&["rev-parse", "HEAD"]).trim().to_string();
            }
        }
        panic!("merge of {name} kept being rejected");
    }

    /// Commit a file on top of the remote's `branch` and push it there;
    /// local main is left as it was. Returns the commit pushed.
    fn push_on_branch(&self, branch: &str, path: &str, body: &str) -> String {
        self.git(&["fetch", "-q", "origin"]);
        self.git(&[
            "checkout",
            "-q",
            "-B",
            "on-branch",
            &format!("origin/{branch}"),
        ]);
        write(&self.dir, path, body);
        self.git(&["add", "-A"]);
        self.git(&["commit", "-q", "-m", &format!("person adds {path}")]);
        self.git(&["push", "-q", "origin", &format!("HEAD:refs/heads/{branch}")]);
        let sha = self.git(&["rev-parse", "HEAD"]).trim().to_string();
        self.git(&["checkout", "-q", "main"]);
        sha
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
    assert_ref_histories_match(client, wc, remote, "main");
}

/// [`assert_histories_match`] for the Leep ref `name` and the remote
/// branch of the same name.
fn assert_ref_histories_match(client: &Client, wc: &Path, remote: &Path, name: &str) {
    let log = client.json(wc, &["log", name, "-n", "10000"]);
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
    let commits = git(remote, &["log", "--format=%H%x00%B%x01", name]);
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
            "commit {sha} names {id}, not on Leep {name}"
        );
        assert!(
            seen.insert(id.clone()),
            "changeset {id} has two commits on the remote"
        );
    }
    assert_eq!(
        seen.len(),
        leep_ids.len(),
        "every changeset on Leep {name} has exactly one commit on the remote"
    );
}

/// Every changeset an agent landed is on Leep's main as itself or as a
/// rebased copy (`leep.rebased_from`), possibly a copy of a copy: a land the
/// server replayed onto a moved head and the mirror then replayed onto the
/// remote's commits.
fn assert_lands_on_main(server: &Server, client: &Client, wc: &Path, landed: &[String]) {
    let log = client.json(wc, &["log", "main", "-n", "10000"]);
    let mut covered = BTreeSet::new();
    for e in log["entries"].as_array().unwrap() {
        covered.insert(e["id"].as_str().unwrap().to_string());
        let mut from = e["changeset"]["extra"]["leep.rebased_from"]
            .as_str()
            .map(String::from);
        while let Some(id) = from {
            if !covered.insert(id.clone()) {
                break;
            }
            from = changeset(server, &id)["extra"]["leep.rebased_from"]
                .as_str()
                .map(String::from);
        }
    }
    for id in landed {
        assert!(
            covered.contains(id),
            "the agent's land {id} is on Leep main neither as itself nor as a rebased copy"
        );
    }
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

fn land_branches_on(remote: &Path) -> Vec<String> {
    git(
        remote,
        &[
            "for-each-ref",
            "--format=%(refname)",
            "refs/heads/leep-land/",
        ],
    )
    .lines()
    .map(String::from)
    .collect()
}

/// Leep's main holds a change whose message is `msg` exactly once: a land
/// branch or a merge was not applied twice under different ids.
fn assert_landed_once(client: &Client, wc: &Path, msg: &str) {
    assert_landed_once_on(client, wc, "main", msg);
}

fn assert_landed_once_on(client: &Client, wc: &Path, name: &str, msg: &str) {
    let log = client.json(wc, &["log", name, "-n", "10000"]);
    let n = log["entries"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|e| e["changeset"]["message"].as_str().map(str::trim) == Some(msg))
        .count();
    assert_eq!(n, 1, "{msg:?} is on Leep {name} {n} times");
}

/// SIGKILL the mirror at seeded random moments while it exports a backlog,
/// pushes it, and imports pushes from the remote; then let one run finish.
#[test]
fn killed_mirror_loses_and_duplicates_nothing() {
    kill_sweep(&[], 12);
}

/// The kill sweep with a mirror that merges divergences and lands land
/// branches: nobody resolves anything by hand.
#[test]
fn killed_merging_mirror_loses_and_duplicates_nothing() {
    kill_sweep(MERGING, 8);
}

/// The kill sweep with a git-primary main (ADR 0062): rounds where both
/// sides moved are replays, killed at random moments between the import,
/// the server's replay and the push. Nobody resolves anything, nothing is
/// imported or pushed twice, and every land is on main as itself or as a
/// rebased copy.
#[test]
fn killed_git_primary_mirror_replays_exactly_once() {
    kill_sweep(GIT_PRIMARY, 8);
}

fn kill_sweep(extra: &[&str], default_rounds: u64) {
    let _g = serial();
    let merging = extra == MERGING;
    let git_primary = extra == GIT_PRIMARY;
    let seed = env_u64("LEEP_MIRROR_SEED", DEFAULT_SEED);
    let rounds = env_u64("LEEP_MIRROR_KILL_ROUNDS", default_rounds);
    eprintln!("mirror kill sweep {extra:?}: seed {seed:#x}, {rounds} rounds");
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
        let m = Mirror::start(&server, &remote, &work, root, extra);
        wait_converged(&server, &remote, &m);
    }
    let person = Person::clone(&remote, root);
    // A merging or git-primary mirror needs nobody: the person never
    // resolves.
    let settled = |m: &Mirror| {
        if !merging && !git_primary && m.main_status()["state"] == "diverged" {
            person.resolve();
        }
        converged(&server, &remote, m) && land_branches_on(&remote).is_empty()
    };
    let mut kills = 0;
    let mut landed = Vec::new();
    let mut agent_lands = Vec::new();
    for round in 0..rounds {
        // 0: a backlog of lands to export and push; 1: pushes to import;
        // 2: both at once, which diverges and is merged by the person (or
        // by the mirror, or replayed onto the remote's side when git is
        // primary, which gets twice the rounds of it); 3 (merging only): a
        // land branch while agents land.
        let kind = match rng.below(if merging || git_primary { 4 } else { 3 }) {
            3 if git_primary => 2,
            k => k,
        };
        // Each round starts from both sides agreeing (the killed runs of the
        // previous round may have left work behind), so its kill lands in
        // the round's own export, push or import.
        let m = Mirror::start(&server, &remote, &work, root, extra);
        wait_until(
            "the mirror to converge between rounds",
            TIMEOUT,
            || settled(&m),
            || format!("seed {seed:#x} round {round}\n{}", m.log_text()),
        );
        drop(m);
        person.catch_up();
        if kind != 1 {
            for k in 0..1 + rng.below(5) {
                let path = format!("r{round}/agent-{k}.txt");
                agent_lands.push(agent.land(&path, &format!("{round} {k}\n")));
                if git_primary {
                    landed.push(format!("add {path}"));
                }
            }
        }
        if kind == 1 || kind == 2 {
            for k in 0..1 + rng.below(3) {
                let path = format!("r{round}/person-{k}.txt");
                person.push_file(&path, "p\n");
                if git_primary {
                    landed.push(format!("person adds {path}"));
                }
            }
        }
        if kind == 3 {
            let path = format!("r{round}/land.txt");
            person.push_land(&format!("r{round}"), &path, "l\n");
            landed.push(format!("person lands {path}"));
        }
        let mut m = Mirror::start(&server, &remote, &work, root, extra);
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

    // A run left alone converges.
    let m = Mirror::start(&server, &remote, &work, root, extra);
    wait_until(
        "the mirror to converge after the kills",
        TIMEOUT,
        || settled(&m),
        || format!("seed {seed:#x}\n{}\n{}", m.main_status(), m.log_text()),
    );
    let log = m.log_text();
    eprintln!(
        "mirror kill sweep: {} pushes, {} imports, {} merges, {} land branches, {} replays, \
         {} divergences across all runs",
        log.matches(": pushed ").count(),
        log.matches(": landed ").count(),
        log.matches(" from the remote into ").count(),
        log.matches(" onto main as ").count(),
        log.matches(" unpushed lands onto the remote's ").count(),
        log.matches(": diverged: ").count()
    );
    if merging || git_primary {
        assert!(
            !log.contains(": diverged: "),
            "no divergence needed a person:\n{log}"
        );
        assert_eq!(remote_ref(&remote, "refs/heads/leep-diverged/main"), None);
    }
    if git_primary {
        assert_lands_on_main(&server, &client, &agent.wc, &agent_lands);
    }
    for msg in &landed {
        assert_landed_once(&client, &agent.wc, msg);
    }
    assert_histories_match(&client, &agent.wc, &remote);
    assert_remote_only_fast_forwarded(&remote);
    assert_trees_match(&client, root, &remote, "kills");
    git(&remote, &["fsck", "--strict", "--no-dangling"]);
}

/// Agents land through Leep while a person pushes to the remote, all at
/// once. Every change ends up on both sides.
#[test]
fn agents_land_while_a_person_pushes_to_git() {
    concurrent_writers(&[]);
}

/// The same with a merging mirror: it converges with no merge in git, and
/// half the person's work goes through land branches.
#[test]
fn agents_land_while_a_person_pushes_to_git_and_the_mirror_merges() {
    concurrent_writers(MERGING);
}

fn concurrent_writers(extra: &[&str]) {
    let _g = serial();
    const AGENTS: usize = 3;
    const LANDS: usize = 5;
    const PUSHES: usize = 6;
    let merging = !extra.is_empty();
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    let stores = Stores::sqlite(&root.join("data"));
    let server = Server::start(&stores, root);
    let client = Client::new(&server.url, &root.join("cache"));
    let remote = make_remote(root);
    let first = Agent::new(&client, root, "agent-first");
    first.land("start.txt", "start\n");
    let m = Mirror::start(&server, &remote, &root.join("work"), root, extra);
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
                let path = format!("people/{k}.txt");
                let body = format!("push {k}\n");
                if merging && k % 2 == 1 {
                    person.push_land(&format!("p{k}"), &path, &body);
                } else {
                    person.catch_up();
                    person.push_file(&path, &body);
                }
                std::thread::sleep(Duration::from_millis(100));
            }
        });
    });
    wait_until(
        "both sides to hold every change",
        TIMEOUT,
        || {
            if !merging && m.main_status()["state"] == "diverged" {
                person.resolve();
            }
            converged(&server, &remote, &m) && land_branches_on(&remote).is_empty()
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
    git(&remote, &["fsck", "--strict", "--no-dangling"]);
    let st = m.main_status();
    eprintln!(
        "concurrent writers {extra:?}: pushes {} imports {} divergences {} merges {} land branches {}",
        st["pushes"], st["imports"], st["divergences"], st["merges"], st["land_branches"]
    );
    if merging {
        assert_eq!(st["divergences"], 0, "nobody had to merge in git: {st}");
        assert_eq!(remote_ref(&remote, "refs/heads/leep-diverged/main"), None);
        assert_eq!(st["land_branches"]["landed"], (PUSHES / 2) as u64, "{st}");
        for k in (1..PUSHES).step_by(2) {
            assert_landed_once(&client, &first.wc, &format!("person lands people/{k}.txt"));
        }
    }
}

/// Abort the mirror exactly between two steps (`LEEP_MIRROR_CRASH_AT`) and
/// check a restart finishes the job once: in the middle of an export, after
/// the export and before the push, after the push and before recording it
/// (an acknowledgement lost), after an import and before the Leep ref moves,
/// after it moved, and after Leep's side of a divergence was pushed.
#[test]
fn crash_points_resume_exactly_once() {
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
        assert_aborted(m.wait_exit(TIMEOUT), point);
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

/// Conflicting work from the person: resolve by taking both lines, commit
/// and push. Returns the pushed commit.
fn resolve_by_hand(person: &Person, path: &str, body: &str) -> String {
    person.git(&["fetch", "-q", "origin"]);
    let _ = person.try_git(&["merge", "-q", "--no-edit", "origin/leep-diverged/main"]);
    write(&person.dir, path, body);
    person.git(&["add", "-A"]);
    person.git(&["commit", "-q", "--no-edit", "-m", "resolve by hand"]);
    person.git(&["push", "-q", "origin", "main"]);
    person.git(&["rev-parse", "HEAD"]).trim().to_string()
}

/// `--on-divergence merge` and `--land-branches` with the real binaries: a
/// clean divergence is merged by the mirror, a conflicting one is reported
/// and left for a person, and land branches land through the server or are
/// reported.
#[test]
fn merging_mirror_merges_clean_divergence_and_lands_land_branches() {
    let _g = serial();
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    let stores = Stores::sqlite(&root.join("data"));
    let server = Server::start(&stores, root);
    let client = Client::new(&server.url, &root.join("cache"));
    let remote = make_remote(root);
    let agent = Agent::new(&client, root, "agent");
    agent.land("a.txt", "one\n");
    let work = root.join("work");
    let mut m = Mirror::start(&server, &remote, &work, root, MERGING);
    wait_converged(&server, &remote, &m);
    let person = Person::clone(&remote, root);

    // Clean divergence: merged by the mirror, pushed as a fast-forward.
    m.kill();
    agent.land("b.txt", "leep\n");
    let theirs = person.push_file("person.txt", "git\n");
    let mut m = Mirror::start(&server, &remote, &work, root, MERGING);
    wait_until(
        "the mirror to merge",
        TIMEOUT,
        || {
            converged(&server, &remote, &m)
                && remote_ref(&remote, "refs/heads/main").as_deref() != Some(theirs.as_str())
        },
        || format!("{}\n{}", m.main_status(), m.log_text()),
    );
    let st = m.main_status();
    assert_eq!(st["merges"]["landed"], 1, "{st}");
    assert_eq!(st["divergences"], 0, "{st}");
    assert_eq!(remote_ref(&remote, "refs/heads/leep-diverged/main"), None);
    let tip = remote_ref(&remote, "refs/heads/main").unwrap();
    assert!(git(&remote, &["rev-list", "--parents", "-1", &tip])
        .split_whitespace()
        .any(|p| p == theirs));
    let body = git(&remote, &["log", "-1", "--format=%an <%ae>%n%B", "main"]);
    assert!(
        body.starts_with(
            "Leep git mirror <git-mirror@leep.invalid>\nMerge main from git into main"
        ),
        "{body}"
    );
    let reflog = client.json(&agent.wc, &["reflog", "main", "-n", "1"]);
    assert_eq!(reflog["entries"][0]["operation"], "git-mirror", "{reflog}");
    let addr = m.listen.parse().unwrap();
    let (_, _, metrics) = http_get(addr, "/metrics", None);
    assert!(
        metrics.contains("leep_mirror_merges_total{ref=\"main\",result=\"landed\"} 1"),
        "{metrics}"
    );

    // Conflicting divergence: reported, nothing resolved, a person merges.
    m.kill();
    person.catch_up();
    agent.land("clash.txt", "leep\n");
    person.push_file("clash.txt", "git\n");
    let mut m = Mirror::start(&server, &remote, &work, root, MERGING);
    wait_until(
        "the conflict to be reported",
        TIMEOUT,
        || {
            let st = m.main_status();
            st["state"] == "diverged"
                && st["detail"]
                    .as_str()
                    .is_some_and(|d| d.contains("1 conflicting path: clash.txt"))
                && remote_ref(&remote, "refs/heads/leep-diverged/main").is_some()
        },
        || format!("{}\n{}", m.main_status(), m.log_text()),
    );
    assert_eq!(m.status().0, 1, "a conflict needs a person");
    assert_eq!(m.main_status()["merges"]["conflicts"], 1);
    let resolved = resolve_by_hand(&person, "clash.txt", "both\n");
    wait_until(
        "the person's resolution to land",
        TIMEOUT,
        || {
            converged(&server, &remote, &m)
                && remote_ref(&remote, "refs/heads/main").as_deref() == Some(resolved.as_str())
                && remote_ref(&remote, "refs/heads/leep-diverged/main").is_none()
        },
        || format!("{}\n{}", m.main_status(), m.log_text()),
    );

    // A land branch lands through the server, rebased onto an agent's land.
    agent.land("c.txt", "three\n");
    wait_converged(&server, &remote, &m);
    m.kill();
    agent.land("d.txt", "four\n");
    person.git(&["fetch", "-q", "origin"]);
    person.git(&["checkout", "-q", "-B", "landing", "origin/main~1"]);
    write(&person.dir, "feature.txt", "feature\n");
    person.git(&["add", "-A"]);
    person.git(&["commit", "-q", "-m", "person lands feature.txt"]);
    person.git(&[
        "push",
        "-q",
        "origin",
        "HEAD:refs/heads/leep-land/main/feature",
    ]);
    person.git(&["checkout", "-q", "main"]);
    let mut m = Mirror::start(&server, &remote, &work, root, MERGING);
    wait_until(
        "the land branch to land",
        TIMEOUT,
        || converged(&server, &remote, &m) && land_branches_on(&remote).is_empty(),
        || {
            format!(
                "{}\n{:?}\n{}",
                m.main_status(),
                m.land_branches(),
                m.log_text()
            )
        },
    );
    assert_eq!(git(&remote, &["show", "main:feature.txt"]), "feature\n");
    assert_landed_once(&client, &agent.wc, "person lands feature.txt");
    let reflog = client.json(&agent.wc, &["reflog", "main", "-n", "5"]);
    assert!(
        reflog["entries"]
            .as_array()
            .unwrap()
            .iter()
            .any(|e| e["operation"] == "land"),
        "landed through POST /v1/land: {reflog}"
    );

    // A conflicting land branch is reported and left in place.
    m.kill();
    agent.land("e.txt", "leep\n");
    person.push_land("bad", "e.txt", "git\n");
    let m = Mirror::start(&server, &remote, &work, root, MERGING);
    wait_until(
        "the land branch conflict to be reported",
        TIMEOUT,
        || {
            converged(&server, &remote, &m)
                && m.land_branches().iter().any(|b| {
                    b["branch"] == "refs/heads/leep-land/main/bad" && b["state"] == "conflict"
                })
        },
        || format!("{:?}\n{}", m.land_branches(), m.log_text()),
    );
    assert_eq!(land_branches_on(&remote), ["refs/heads/leep-land/main/bad"]);
    let (code, _) = m.status();
    assert_eq!(
        code, 0,
        "a land branch conflict is the pusher's, not the mirror's"
    );
    let (_, _, metrics) = http_get(m.listen.parse().unwrap(), "/metrics", None);
    assert!(
        metrics.contains("leep_mirror_land_branches_failing{ref=\"main\"} 1"),
        "{metrics}"
    );
    let out = Command::new(bin("leep-git"))
        .args(["mirror", "status", "--work-dir"])
        .arg(&work)
        .output()
        .unwrap();
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(
        text.contains("land branch leep-land/main/bad -> main conflict"),
        "{text}"
    );

    let files = assert_trees_match(&client, root, &remote, "merging");
    for f in [
        "a.txt",
        "b.txt",
        "person.txt",
        "clash.txt",
        "c.txt",
        "d.txt",
        "feature.txt",
    ] {
        assert!(files.contains_key(f), "{f} missing: {files:?}");
    }
    assert_histories_match(&client, &agent.wc, &remote);
    assert_remote_only_fast_forwarded(&remote);
    git(&remote, &["fsck", "--strict", "--no-dangling"]);
    assert!(!m.log_text().contains("panicked"));
}

/// Abort a merging mirror at each step it adds and check a restart finishes
/// the job exactly once: after the remote's side was imported and before the
/// merge (`merge-imported`), after the merge was created and before the ref
/// moved (`merged`), after it moved and before the push (`merge-landed`),
/// after the merge's push and before the mirror saw the answer (`pushed`);
/// for land branches, after the import (`land-imported`), after the land and
/// before the delete (`land-landed`), and after the delete (`land-deleted`).
#[test]
fn merging_crash_points_resume_exactly_once() {
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
        let m = Mirror::start(&server, &remote, &work, root, MERGING);
        wait_converged(&server, &remote, &m);
        Person::clone(&remote, root)
    };
    let moves = |op: &str| {
        let log = client.json(&agent.wc, &["reflog", "main", "-n", "1000"]);
        log["entries"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|e| e["operation"] == op)
            .count()
    };
    for (n, point) in [
        "merge-imported",
        "merged",
        "merge-landed",
        "pushed",
        "land-imported",
        "land-landed",
        "land-deleted",
    ]
    .into_iter()
    .enumerate()
    {
        person.catch_up();
        let (mirror_moves, lands) = (moves("git-mirror"), moves("land"));
        for k in 0..2 {
            agent.land(&format!("{point}/agent-{k}.txt"), &format!("{n} {k}\n"));
        }
        let land_msg = format!("person lands {point}/land.txt");
        let merging = !point.starts_with("land-");
        if merging {
            for k in 0..2 {
                person.push_file(&format!("{point}/person-{k}.txt"), "p\n");
            }
        } else {
            person.push_land(point, &format!("{point}/land.txt"), "l\n");
        }
        let mut m = Mirror::start_env(&server, &remote, &work, root, MERGING, Some(point));
        assert_aborted(m.wait_exit(TIMEOUT), point);
        let m = Mirror::start(&server, &remote, &work, root, MERGING);
        wait_until(
            &format!("convergence after a crash at {point}"),
            TIMEOUT,
            || converged(&server, &remote, &m) && land_branches_on(&remote).is_empty(),
            || {
                format!(
                    "{}\n{:?}\n{}",
                    m.main_status(),
                    m.land_branches(),
                    m.log_text()
                )
            },
        );
        let st = m.main_status();
        assert_eq!(st["divergences"], 0, "{point}: nobody merged by hand: {st}");
        if merging {
            assert_eq!(
                moves("git-mirror"),
                mirror_moves + 1,
                "{point}: the merge landed exactly once"
            );
            if point == "merge-landed" || point == "pushed" {
                // The restart found the merge landed and only pushed it, or
                // found it pushed and pushed nothing.
                assert_eq!(st["merges"]["landed"], 0, "{point}: {st}");
            }
            if point == "pushed" {
                assert_eq!(st["pushes"]["ok"], 0, "{point}: {st}");
            }
        } else {
            assert_landed_once(&client, &agent.wc, &land_msg);
            // Agents' lands plus one for the land branch, and no second land.
            assert_eq!(moves("land"), lands + 3, "{point}: one land for the branch");
            assert_eq!(moves("git-mirror"), mirror_moves, "{point}");
        }
        drop(m);
        assert_histories_match(&client, &agent.wc, &remote);
        assert_remote_only_fast_forwarded(&remote);
    }
    assert_trees_match(&client, root, &remote, "merging-crash-points");
    git(&remote, &["fsck", "--strict", "--no-dangling"]);
}

/// `--ref-prefix review/` (ADR 0061): a review ref an agent lands on
/// appears on the remote, and goes away when the Leep ref is deleted; main
/// is only ever fast-forwarded.
#[test]
fn mirror_pushes_and_deletes_review_refs_under_a_prefix() {
    let _g = serial();
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    let stores = Stores::sqlite(&root.join("data"));
    let server = Server::start(&stores, root);
    let client = Client::new(&server.url, &root.join("cache"));
    let remote = make_remote(root);
    let agent = Agent::new(&client, root, "agent");
    agent.land("a.txt", "one\n");
    let prefix = ["--ref-prefix", "review/"];
    let m = Mirror::start(&server, &remote, &root.join("work"), root, &prefix);
    wait_converged(&server, &remote, &m);
    let main = remote_ref(&remote, "refs/heads/main").unwrap();

    let review = "refs/heads/review/e2e-prefix";
    let review_status = || {
        let (_, v) = m.status();
        v["refs"]
            .as_array()
            .and_then(|r| {
                r.iter()
                    .find(|r| r["leep_ref"] == "review/e2e-prefix")
                    .cloned()
            })
            .unwrap_or(serde_json::Value::Null)
    };
    client.ok(&agent.wc, &["switch", "-c", "review/e2e-prefix"]);
    agent.land("b.txt", "review\n");
    wait_until(
        "the review ref to reach the remote",
        TIMEOUT,
        || {
            let st = review_status();
            st["state"] == "in-sync"
                && st["remote"].as_str().is_some()
                && st["remote"].as_str() == remote_ref(&remote, review).as_deref()
                // Created at main's commit by `switch -c`; wait for the land.
                && st["remote"].as_str() != Some(main.as_str())
        },
        || format!("{}\n{}", review_status(), m.log_text()),
    );
    let st = review_status();
    assert_eq!(st["ref_prefix"], "review/", "{st}");
    assert_eq!(
        git(&remote, &["show", "review/e2e-prefix:b.txt"]),
        "review\n"
    );
    let body = git(&remote, &["log", "-1", "--format=%B", "review/e2e-prefix"]);
    assert!(body.contains("Leep-Changeset: "), "{body}");
    assert_eq!(remote_ref(&remote, "refs/heads/main").unwrap(), main);

    // Deleted in Leep: deleted on the remote, and the mirror stays healthy.
    client.ok(&agent.wc, &["switch", "main"]);
    client.ok(&agent.wc, &["branch", "-D", "review/e2e-prefix"]);
    wait_until(
        "the review branch to be deleted",
        TIMEOUT,
        || {
            let (_, v) = m.status();
            remote_ref(&remote, review).is_none() && v["branches_deleted"] == 1
        },
        || format!("{}\n{}", m.status().1, m.log_text()),
    );
    let (code, v) = m.status();
    assert_eq!(code, 0, "{v}");
    assert_eq!(v["deleted_branches"][0]["git_ref"], review, "{v}");
    assert_eq!(v["refs"].as_array().unwrap().len(), 1, "{v}");
    assert_eq!(remote_ref(&remote, "refs/heads/main").unwrap(), main);
    assert_remote_only_fast_forwarded(&remote);
    let log = m.log_text();
    assert!(!log.contains("panicked"), "{log}");
}

/// `--primary git` (ADR 0062) with the real binaries: agents land through
/// Leep while a person merges pull requests on the remote, all at once. The
/// remote wins every race, so Leep's unpushed lands are replayed onto the
/// person's merges: the run converges with no divergence, every land is on
/// main as itself or a rebased copy, and the remote only fast-forwards.
#[test]
fn agents_land_while_a_person_merges_on_a_git_primary_remote() {
    let _g = serial();
    const AGENTS: usize = 3;
    const LANDS: usize = 5;
    const MERGES: usize = 6;
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    let stores = Stores::sqlite(&root.join("data"));
    let server = Server::start(&stores, root);
    let client = Client::new(&server.url, &root.join("cache"));
    let remote = make_remote(root);
    let first = Agent::new(&client, root, "agent-first");
    first.land("start.txt", "start\n");
    let m = Mirror::start(&server, &remote, &root.join("work"), root, GIT_PRIMARY);
    wait_converged(&server, &remote, &m);
    assert_eq!(m.main_status()["git_primary"], true, "{}", m.main_status());
    let person = Person::clone(&remote, root);

    let agents: Vec<Agent> = (0..AGENTS)
        .map(|n| Agent::new(&client, root, &format!("agent-{n}")))
        .collect();
    let landed: Vec<String> = std::thread::scope(|s| {
        let lands: Vec<_> = agents
            .iter()
            .enumerate()
            .map(|(n, a)| {
                s.spawn(move || {
                    (0..LANDS)
                        .map(|k| {
                            a.land(
                                &format!("agents/{n}-{k}.txt"),
                                &format!("agent {n} land {k}\n"),
                            )
                        })
                        .collect::<Vec<_>>()
                })
            })
            .collect();
        s.spawn(|| {
            for k in 0..MERGES {
                person.merge_pr(
                    &format!("p{k}"),
                    &format!("people/{k}.txt"),
                    &format!("merge {k}\n"),
                );
                std::thread::sleep(Duration::from_millis(100));
            }
        });
        lands.into_iter().flat_map(|h| h.join().unwrap()).collect()
    });
    let mut diverged = false;
    wait_until(
        "both sides to hold every change",
        TIMEOUT,
        || {
            diverged |= m.main_status()["state"] == "diverged";
            converged(&server, &remote, &m)
        },
        || format!("{}\n{}", m.main_status(), m.log_text()),
    );
    let st = m.main_status();
    assert!(!diverged, "a git-primary ref never diverges here: {st}");
    assert_eq!(st["divergences"], 0, "{st}");
    assert_eq!(remote_ref(&remote, "refs/heads/leep-diverged/main"), None);
    let (code, all) = m.status();
    assert_eq!(code, 0, "{all}");

    let files = assert_trees_match(&client, root, &remote, "git-primary");
    for n in 0..AGENTS {
        for k in 0..LANDS {
            assert!(
                files.contains_key(&format!("agents/{n}-{k}.txt")),
                "agent {n} land {k} lost"
            );
            assert_landed_once(&client, &first.wc, &format!("add agents/{n}-{k}.txt"));
        }
    }
    for k in 0..MERGES {
        assert!(
            files.contains_key(&format!("people/{k}.txt")),
            "merge {k} lost"
        );
        assert_landed_once(&client, &first.wc, &format!("person adds people/{k}.txt"));
    }
    assert_lands_on_main(&server, &client, &first.wc, &landed);
    assert_histories_match(&client, &first.wc, &remote);
    assert_remote_only_fast_forwarded(&remote);
    git(&remote, &["fsck", "--strict", "--no-dangling"]);
    let log = m.log_text();
    assert!(!log.contains("panicked"), "{log}");
    eprintln!(
        "git-primary writers: pushes {} imports {} replays {}",
        st["pushes"], st["imports"], st["replays"]
    );
}

/// Abort a git-primary mirror at each step of a replay and check a restart
/// finishes it exactly once: after the remote's side was imported and before
/// the server replayed (`replay-imported`), after the server replayed and
/// moved the ref and before the push (`replayed`), and after the replay's
/// push and before the mirror saw the answer (`pushed`). Nothing is imported
/// again, the ref moves once and the remote is updated once.
#[test]
fn git_primary_crash_points_resume_exactly_once() {
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
        let m = Mirror::start(&server, &remote, &work, root, GIT_PRIMARY);
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
    let mut lands = Vec::new();
    for (n, point) in ["replay-imported", "replayed", "pushed"]
        .into_iter()
        .enumerate()
    {
        person.catch_up();
        for k in 0..2 {
            lands.push(agent.land(&format!("{point}/agent-{k}.txt"), &format!("{n} {k}\n")));
        }
        for k in 0..2 {
            person.push_file(&format!("{point}/person-{k}.txt"), "p\n");
        }
        let (moves, updates) = (mirror_moves(), remote_updates(&remote, "main"));
        let mut m = Mirror::start_env(&server, &remote, &work, root, GIT_PRIMARY, Some(point));
        assert_aborted(m.wait_exit(TIMEOUT), point);
        let m = Mirror::start(&server, &remote, &work, root, GIT_PRIMARY);
        wait_converged(&server, &remote, &m);
        let (code, all) = m.status();
        assert_eq!(code, 0, "{point}: {all}");
        let st = m.main_status();
        assert_eq!(st["divergences"], 0, "{point}: {st}");
        assert_eq!(remote_ref(&remote, "refs/heads/leep-diverged/main"), None);
        // The import before the crash was made durable: the restart finds
        // every commit of the person's already in Leep.
        assert_eq!(all["commits_imported"], 0, "{point}: imported twice: {all}");
        assert_eq!(mirror_moves(), moves + 1, "{point}: the ref moved once");
        assert_eq!(
            remote_updates(&remote, "main"),
            updates + 1,
            "{point}: the replay was pushed once"
        );
        let (replays, pushes, exported) = match point {
            // Nothing had moved: the restart replays, exports both copies
            // and pushes.
            "replay-imported" => (1, 1, 2),
            // The ref holds the copies: the restart only exports and pushes.
            "replayed" => (0, 1, 2),
            // The push had landed: the restart finds both sides in sync.
            _ => (0, 0, 0),
        };
        assert_eq!(st["replays"]["ok"], replays, "{point}: {st}");
        assert_eq!(st["pushes"]["ok"], pushes, "{point}: {st}");
        assert_eq!(all["commits_exported"], exported, "{point}: {all}");
        drop(m);
        for k in 0..2 {
            assert_landed_once(&client, &agent.wc, &format!("add {point}/agent-{k}.txt"));
            assert_landed_once(
                &client,
                &agent.wc,
                &format!("person adds {point}/person-{k}.txt"),
            );
        }
        assert_histories_match(&client, &agent.wc, &remote);
        assert_remote_only_fast_forwarded(&remote);
    }
    assert_lands_on_main(&server, &client, &agent.wc, &lands);
    assert_trees_match(&client, root, &remote, "git-primary-crash-points");
    git(&remote, &["fsck", "--strict", "--no-dangling"]);
}

/// `leep undo main` on a git-primary ref is moved forward again to the
/// remote's tip, which is the truth (ADR 0062): once with the mirror
/// running, and once with a crash right after the restore moved the ref
/// (`restored`), after which the restart has nothing left to do. The remote
/// is never touched.
#[test]
fn a_leep_undo_on_a_git_primary_ref_is_restored_across_a_crash() {
    let _g = serial();
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    let stores = Stores::sqlite(&root.join("data"));
    let server = Server::start(&stores, root);
    let client = Client::new(&server.url, &root.join("cache"));
    let remote = make_remote(root);
    let agent = Agent::new(&client, root, "agent");
    agent.land("a.txt", "one\n");
    let work = root.join("work");
    let mut m = Mirror::start(&server, &remote, &work, root, GIT_PRIMARY);
    wait_converged(&server, &remote, &m);
    agent.land("b.txt", "two\n");
    wait_converged(&server, &remote, &m);
    let tip = leep_main(&server).unwrap();
    let remote_tip = remote_ref(&remote, "refs/heads/main").unwrap();
    let updates = remote_updates(&remote, "main");
    let mirror_moves = || {
        let log = client.json(&agent.wc, &["reflog", "main", "-n", "1000"]);
        log["entries"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|e| e["operation"] == "git-mirror")
            .count()
    };

    // With the mirror running: restored within a cycle.
    client.ok(&agent.wc, &["undo", "main"]);
    wait_until(
        "the undo to be restored",
        TIMEOUT,
        || leep_main(&server).as_deref() == Some(tip.as_str()) && converged(&server, &remote, &m),
        || format!("{}\n{}", m.main_status(), m.log_text()),
    );
    let st = m.main_status();
    assert_eq!(st["restores"], 1, "{st}");
    let reflog = client.json(&agent.wc, &["reflog", "main", "-n", "1"]);
    let e = &reflog["entries"][0];
    assert_eq!(e["operation"], "git-mirror", "{reflog}");
    assert!(
        e["message"]
            .as_str()
            .is_some_and(|msg| msg.contains("restored from git")),
        "{reflog}"
    );
    let (code, all) = m.status();
    assert_eq!(code, 0, "a restore is a healthy end: {all}");
    let (_, _, metrics) = http_get(m.listen.parse().unwrap(), "/metrics", None);
    assert!(
        metrics.contains("leep_mirror_restores_total{ref=\"main\"} 1"),
        "{metrics}"
    );

    // A crash right after the restore moved the ref.
    m.kill();
    let moves = mirror_moves();
    client.ok(&agent.wc, &["undo", "main"]);
    assert_ne!(leep_main(&server).as_deref(), Some(tip.as_str()));
    let mut m = Mirror::start_env(&server, &remote, &work, root, GIT_PRIMARY, Some("restored"));
    assert_aborted(m.wait_exit(TIMEOUT), "restored");
    assert_eq!(
        leep_main(&server).as_deref(),
        Some(tip.as_str()),
        "the restore moved the ref before the crash"
    );
    let m = Mirror::start(&server, &remote, &work, root, GIT_PRIMARY);
    wait_converged(&server, &remote, &m);
    let st = m.main_status();
    assert_eq!(st["restores"], 0, "nothing left to restore: {st}");
    assert_eq!(st["pushes"]["ok"], 0, "{st}");
    assert_eq!(mirror_moves(), moves + 1, "restored once");
    assert_eq!(leep_main(&server).as_deref(), Some(tip.as_str()));
    assert_eq!(m.status().0, 0);

    assert_eq!(
        remote_ref(&remote, "refs/heads/main").as_deref(),
        Some(remote_tip.as_str())
    );
    assert_eq!(
        remote_updates(&remote, "main"),
        updates,
        "the remote never moved"
    );
    assert_histories_match(&client, &agent.wc, &remote);
    assert_remote_only_fast_forwarded(&remote);
    assert_trees_match(&client, root, &remote, "restored");
    git(&remote, &["fsck", "--strict", "--no-dangling"]);
    assert!(!m.log_text().contains("panicked"));
}

/// Two-way review branches (`--ref-prefix review/ --import`, ADR 0062): a
/// person pushes a commit onto an agent's review branch while the agent
/// squashes its stack in Leep. The mirror imports the person's commit,
/// lands it onto the squash and pushes the result with a lease on the tip
/// it imported, across a crash after the import (`prefix-imported`) and
/// after the land (`prefix-landed`).
#[test]
fn a_persons_push_to_a_review_branch_lands_on_an_agent_squash_across_crashes() {
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
        let m = Mirror::start(&server, &remote, &work, root, TWO_WAY);
        wait_converged(&server, &remote, &m);
        Person::clone(&remote, root)
    };
    let main_tip = leep_main(&server).unwrap();
    let main_remote = remote_ref(&remote, "refs/heads/main").unwrap();

    for point in ["prefix-imported", "prefix-landed"] {
        let name = format!("review/e2e-{point}");
        let branch = format!("refs/heads/{name}");
        let in_sync = |m: &Mirror| {
            let st = m.ref_status(&name);
            st["state"] == "in-sync"
                && st["leep"].as_str() == leep_ref(&server, &name).as_deref()
                && st["remote"].as_str().is_some()
                && st["remote"].as_str() == remote_ref(&remote, &branch).as_deref()
        };

        // The agent's stack reaches the branch.
        client.ok(&agent.wc, &["switch", "main"]);
        client.ok(&agent.wc, &["switch", "-c", &name]);
        agent.land(&format!("{point}/one.txt"), "one\n");
        agent.land(&format!("{point}/two.txt"), "two\n");
        let stack = leep_ref(&server, &name).unwrap();
        {
            let m = Mirror::start(&server, &remote, &work, root, TWO_WAY);
            wait_until(
                &format!("{name} to reach the remote"),
                TIMEOUT,
                || in_sync(&m) && m.ref_status(&name)["leep"].as_str() == Some(stack.as_str()),
                || format!("{}\n{}", m.ref_status(&name), m.log_text()),
            );
        }

        // While the mirror is down, the person pushes onto the branch and
        // the agent squashes the stack into one changeset on main.
        let pushed = person.push_on_branch(&name, &format!("{point}/person.txt"), "p\n");
        client.ok(&agent.wc, &["reset", &main_tip]);
        let squash = client
            .ok(&agent.wc, &["commit", "-m", &format!("{point}: squashed")])
            .trim()
            .to_string();
        move_ref(&server, &name, &stack, &squash);

        let mut m = Mirror::start_env(&server, &remote, &work, root, TWO_WAY, Some(point));
        assert_aborted(m.wait_exit(TIMEOUT), point);
        let m = Mirror::start(&server, &remote, &work, root, TWO_WAY);
        wait_until(
            &format!("the person's commit to land on {name} after a crash at {point}"),
            TIMEOUT,
            || {
                in_sync(&m)
                    && leep_ref(&server, &name).as_deref() != Some(squash.as_str())
                    && remote_ref(&remote, &branch).as_deref() != Some(pushed.as_str())
            },
            || format!("{}\n{}", m.ref_status(&name), m.log_text()),
        );

        // The Leep ref holds the person's commit, rebased onto the squash.
        let tip = leep_ref(&server, &name).unwrap();
        let cs = changeset(&server, &tip);
        let msg = format!("person adds {point}/person.txt");
        assert_eq!(
            cs["message"].as_str().map(str::trim),
            Some(msg.as_str()),
            "{cs}"
        );
        assert_eq!(cs["parents"], serde_json::json!([squash]), "{cs}");
        assert!(cs["extra"]["leep.rebased_from"].is_string(), "{cs}");
        assert_landed_once_on(&client, &agent.wc, &name, &msg);
        // The agent's squashed commits are gone from the branch: only the
        // squash and the person's copy sit above main.
        let subjects = git(
            &remote,
            &["log", "--format=%s", &format!("{main_remote}..{name}")],
        );
        let squashed = format!("{point}: squashed");
        assert_eq!(
            subjects.lines().collect::<Vec<_>>(),
            [msg.as_str(), squashed.as_str()],
            "{subjects}"
        );
        assert_eq!(
            git(&remote, &["show", &format!("{name}:{point}/person.txt")]),
            "p\n"
        );
        assert_eq!(
            git(&remote, &["show", &format!("{name}:{point}/two.txt")]),
            "two\n"
        );

        // Rewritten with a lease on the exact tip imported, and nothing
        // imported a second time after the crash.
        let log = m.log_text();
        assert!(
            log.contains(&format!("pushing with a lease on {pushed}")),
            "{point}:\n{log}"
        );
        let (code, all) = m.status();
        assert_eq!(code, 0, "{point}: {all}");
        assert_eq!(all["commits_imported"], 0, "{point}: imported twice: {all}");
        let st = m.ref_status(&name);
        assert_eq!(st["ref_prefix"], "review/", "{st}");
        assert_eq!(st["divergences"], 0, "{st}");
        drop(m);
        // The Leep review ref and the review branch hold the same history,
        // one commit per changeset.
        assert_ref_histories_match(&client, &agent.wc, &remote, &name);
    }
    // Main was never touched.
    assert_eq!(leep_main(&server).as_deref(), Some(main_tip.as_str()));
    assert_eq!(
        remote_ref(&remote, "refs/heads/main").as_deref(),
        Some(main_remote.as_str())
    );
    assert_remote_only_fast_forwarded(&remote);
    git(&remote, &["fsck", "--strict", "--no-dangling"]);
}

/// A review branch deleted on the remote (ADR 0062): with its work landed
/// on main the Leep ref is deleted too, across a crash right after the
/// delete (`ref-deleted`), and the restart neither pushes the branch again
/// nor needs anything. With its work landed nowhere the ref is kept and
/// reported as `deleted-on-remote`, and the branch is not pushed again.
#[test]
fn a_review_branch_deleted_on_the_remote_deletes_only_a_landed_ref() {
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
    let in_sync = |m: &Mirror, name: &str| {
        let st = m.ref_status(name);
        st["state"] == "in-sync"
            && st["leep"].as_str().is_some()
            && st["leep"].as_str() == leep_ref(&server, name).as_deref()
            && st["remote"].as_str()
                == remote_ref(&remote, &format!("refs/heads/{name}")).as_deref()
    };

    // The mirror only knows a branch was deleted once it saw it in sync in
    // the same process, so this one runs from the start with the crash set.
    let mut m = Mirror::start_env(&server, &remote, &work, root, TWO_WAY, Some("ref-deleted"));
    wait_converged(&server, &remote, &m);
    let person = Person::clone(&remote, root);
    let done = "review/e2e-done";
    client.ok(&agent.wc, &["switch", "-c", done]);
    agent.land("done.txt", "done\n");
    wait_until(
        "the review branch to reach the remote",
        TIMEOUT,
        || in_sync(&m, done) && leep_ref(&server, done) != leep_main(&server),
        || format!("{}\n{}", m.ref_status(done), m.log_text()),
    );
    // The pull request is merged: the work lands on main, then the branch
    // is deleted on the remote.
    client.ok(&agent.wc, &["land", "--ref", "main"]);
    assert_eq!(leep_main(&server), leep_ref(&server, done));
    wait_converged(&server, &remote, &m);
    person.git(&["push", "-q", "origin", &format!(":refs/heads/{done}")]);
    assert_aborted(m.wait_exit(TIMEOUT), "ref-deleted");
    assert_eq!(
        leep_ref(&server, done),
        None,
        "the ref was deleted before the crash"
    );

    let m = Mirror::start(&server, &remote, &work, root, TWO_WAY);
    wait_converged(&server, &remote, &m);
    // A few cycles more: the deleted branch does not come back.
    std::thread::sleep(Duration::from_secs(1));
    assert_eq!(remote_ref(&remote, &format!("refs/heads/{done}")), None);
    assert_eq!(leep_ref(&server, done), None);
    let (code, v) = m.status();
    assert_eq!(code, 0, "{v}");
    assert_eq!(v["refs_deleted"], 0, "deleted once, before the crash: {v}");
    assert_eq!(v["branches_deleted"], 0, "{v}");
    assert_eq!(m.ref_status(done), serde_json::Value::Null, "{v}");

    // Work landed nowhere: kept and reported, never pushed again.
    let open = "review/e2e-open";
    client.ok(&agent.wc, &["switch", "main"]);
    client.ok(&agent.wc, &["switch", "-c", open]);
    agent.land("open.txt", "open\n");
    wait_until(
        "the open branch to reach the remote",
        TIMEOUT,
        || in_sync(&m, open) && leep_ref(&server, open) != leep_main(&server),
        || format!("{}\n{}", m.ref_status(open), m.log_text()),
    );
    let kept = leep_ref(&server, open).unwrap();
    person.git(&["push", "-q", "origin", &format!(":refs/heads/{open}")]);
    wait_until(
        "the deleted branch to be reported",
        TIMEOUT,
        || m.ref_status(open)["state"] == "deleted-on-remote",
        || format!("{}\n{}", m.ref_status(open), m.log_text()),
    );
    std::thread::sleep(Duration::from_secs(1));
    assert_eq!(leep_ref(&server, open).as_deref(), Some(kept.as_str()));
    assert_eq!(remote_ref(&remote, &format!("refs/heads/{open}")), None);
    let (code, v) = m.status();
    assert_eq!(code, 1, "the kept ref needs a person: {v}");
    assert_eq!(v["refs_deleted"], 0, "{v}");

    assert_histories_match(&client, &agent.wc, &remote);
    assert_remote_only_fast_forwarded(&remote);
    git(&remote, &["fsck", "--strict", "--no-dangling"]);
    assert!(!m.log_text().contains("panicked"));
}
