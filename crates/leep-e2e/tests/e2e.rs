//! End-to-end tests that drive the real `leep-server`, `leep` and `leep-git`
//! binaries as subprocesses. Run `cargo build --workspace` first.

mod common;

use std::collections::BTreeMap;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use common::*;

const AGENTS: usize = 50;
/// Agents that also edit the same line of `shared.txt`, so their lands conflict.
const CONFLICTING: [usize; 5] = [0, 10, 20, 30, 40];
const MAX_LAND_TRIES: usize = 10;

fn shared_original() -> String {
    (1..=10).map(|i| format!("line {i}\n")).collect()
}

/// What the fixture repository contains at its tip.
struct Fixture {
    dir: PathBuf,
    /// Commit shas, oldest first.
    commits: Vec<String>,
    /// Regular files expected at the tip (path -> content).
    files: BTreeMap<String, String>,
}

fn make_fixture(root: &Path) -> Fixture {
    let dir = root.join("fixture");
    fs::create_dir_all(&dir).unwrap();
    git(&dir, &["init", "-q", "-b", "main"]);
    let mut files: BTreeMap<String, String> = BTreeMap::new();
    let mut commits = Vec::new();
    let mut n = 0;
    let mut commit = |dir: &Path, msg: &str| {
        n += 1;
        let date = format!("2024-01-0{n}T12:00:00+0000");
        git_dated(dir, &["add", "-A"], &date);
        git_dated(dir, &["commit", "-q", "-m", msg], &date);
        commits.push(git(dir, &["rev-parse", "HEAD"]).trim().to_string());
    };
    let mut put = |dir: &Path, rel: &str, body: &str| {
        write(dir, rel, body);
        files.insert(rel.to_string(), body.to_string());
    };

    put(&dir, "README.md", "# fixture\n");
    put(&dir, "src/main.rs", "fn main() {}\n");
    put(&dir, "src/util/mod.rs", "pub fn util() {}\n");
    put(&dir, "old.txt", "to be removed\n");
    put(&dir, "shared.txt", &shared_original());
    commit(&dir, "initial import");

    put(&dir, "scripts/run.sh", "#!/bin/sh\necho hi\n");
    fs::set_permissions(
        dir.join("scripts/run.sh"),
        fs::Permissions::from_mode(0o755),
    )
    .unwrap();
    commit(&dir, "add script");

    put(&dir, "docs/guide/intro.md", "intro\n");
    std::os::unix::fs::symlink("../README.md", dir.join("docs/readme-link")).unwrap();
    commit(&dir, "docs and symlink");

    put(
        &dir,
        "src/main.rs",
        "fn main() {\n    println!(\"hi\");\n}\n",
    );
    commit(&dir, "edit main");

    put(&dir, "data/deep/nest/er/file.txt", "deep\n");
    fs::remove_file(dir.join("old.txt")).unwrap();
    files.remove("old.txt");
    commit(&dir, "deep file, drop old");

    Fixture {
        dir,
        commits,
        files,
    }
}

fn agent_body(n: usize) -> String {
    format!("hello from agent {n:02}\n")
}

fn shared_line(n: usize) -> String {
    format!("line 5 edited by agent-{n:02}")
}

/// Keep every side of each conflict region (a union merge), dropping markers
/// and any diff3 base section.
fn resolve_markers(text: &str) -> String {
    let mut out = String::new();
    let mut in_base = false;
    for line in text.split_inclusive('\n') {
        if line.starts_with("<<<<<<<") || line.starts_with(">>>>>>>") {
            continue;
        }
        if line.starts_with("|||||||") {
            in_base = true;
            continue;
        }
        if line.starts_with("=======") {
            in_base = false;
            continue;
        }
        if !in_base {
            out.push_str(line);
        }
    }
    out
}

fn has_markers(text: &str) -> bool {
    text.lines()
        .any(|l| l.starts_with("<<<<<<<") || l.starts_with(">>>>>>>") || l.starts_with("======="))
}

/// One agent: init, commit its file, land; on conflict resolve and land again.
/// Any other land failure fails the test: under contention the server must
/// queue lands, not give up.
fn run_agent(client: &Client, root: &Path, n: usize) -> Result<(), String> {
    let wc = root.join(format!("wc-{n:02}"));
    let wc_s = wc.to_str().unwrap();
    let session = n.to_string();
    let step =
        |args: &[&str]| -> Result<std::process::Output, String> { Ok(client.run(&wc, args)) };
    let fail = |what: &str, out: &std::process::Output| {
        format!(
            "agent {n}: {what} failed ({})\n--- stdout\n{}\n--- stderr\n{}",
            out.status,
            stdout(out),
            stderr(out)
        )
    };

    let out = client.run(root, &["init", wc_s]);
    if !out.status.success() {
        return Err(fail("init", &out));
    }
    write(&wc, &format!("agents/agent-{n:02}.txt"), &agent_body(n));
    if CONFLICTING.contains(&n) {
        let text = fs::read_to_string(wc.join("shared.txt")).map_err(|e| e.to_string())?;
        let edited: String = text
            .split_inclusive('\n')
            .enumerate()
            .map(|(i, l)| {
                if i == 4 {
                    format!("{}\n", shared_line(n))
                } else {
                    l.to_string()
                }
            })
            .collect();
        write(&wc, "shared.txt", &edited);
    }

    let commit = |msg: &str| -> Result<(), String> {
        let out = step(&[
            "commit",
            "-m",
            msg,
            "--agent-model",
            "e2e",
            "--agent-session",
            &session,
        ])?;
        if out.status.success() {
            Ok(())
        } else {
            Err(fail("commit", &out))
        }
    };
    commit(&format!("agent {n:02}: add file"))?;

    for _ in 0..MAX_LAND_TRIES {
        let out = step(&["land"])?;
        match out.status.code() {
            Some(0) => return Ok(()),
            Some(1) if stdout(&out).contains("Cannot land cleanly") => {
                let path = wc.join("shared.txt");
                let text = fs::read_to_string(&path).map_err(|e| e.to_string())?;
                if has_markers(&text) {
                    fs::write(&path, resolve_markers(&text)).map_err(|e| e.to_string())?;
                }
                commit(&format!("agent {n:02}: resolve conflicts"))?;
            }
            _ => return Err(fail("land", &out)),
        }
    }
    Err(format!(
        "agent {n}: did not land within {MAX_LAND_TRIES} tries"
    ))
}

fn swarm(client: &Client, root: &Path) {
    let errors: Vec<String> = std::thread::scope(|s| {
        let handles: Vec<_> = (0..AGENTS)
            .map(|n| s.spawn(move || run_agent(client, root, n)))
            .collect();
        handles
            .into_iter()
            .filter_map(|h| h.join().expect("agent thread panicked").err())
            .collect()
    });
    assert!(errors.is_empty(), "{}", errors.join("\n\n"));
}

fn json_str<'a>(v: &'a serde_json::Value, path: &[&str]) -> Option<&'a str> {
    path.iter().try_fold(v, |v, k| v.get(*k))?.as_str()
}

fn run_import_swarm_export(stores: &Stores, work: &Path) {
    let fx = make_fixture(work);
    let (git_dir, meta, blobs) = (fx.dir.to_str().unwrap(), &stores.meta, &stores.blobs);

    // Import before the server starts so only one process touches the stores.
    let out = leep_git(&[
        "import",
        "--meta",
        meta,
        "--blobs",
        blobs,
        "--git-dir",
        git_dir,
        "--set-ref",
        "main",
    ]);
    let imported_head = out
        .lines()
        .find_map(|l| l.strip_prefix("head "))
        .unwrap_or_else(|| panic!("no head in import output:\n{out}"))
        .trim()
        .to_string();

    let server = Server::start(stores, work);
    let cache = work.join("shared-cache");
    let client = Client::new(&server.url, &cache);
    swarm(&client, work);

    // Verify through a fresh working copy.
    let verify = work.join("verify");
    client.ok(work, &["init", verify.to_str().unwrap()]);

    for n in 0..AGENTS {
        let got = fs::read_to_string(verify.join(format!("agents/agent-{n:02}.txt")))
            .unwrap_or_else(|e| panic!("agent {n} file missing on main: {e}"));
        assert_eq!(got, agent_body(n), "agent {n} content");
    }
    let shared = fs::read_to_string(verify.join("shared.txt")).unwrap();
    assert!(!has_markers(&shared), "conflict markers on main:\n{shared}");
    for n in CONFLICTING {
        assert!(
            shared.contains(&shared_line(n)),
            "edit of agent {n} lost from shared.txt:\n{shared}"
        );
    }
    let original = shared_original();
    for (i, line) in original.split_inclusive('\n').enumerate() {
        if i != 4 {
            assert!(shared.contains(line), "shared.txt lost {line:?}");
        }
    }
    for (path, body) in &fx.files {
        if path == "shared.txt" {
            continue;
        }
        let got = fs::read_to_string(verify.join(path))
            .unwrap_or_else(|e| panic!("fixture file {path} missing: {e}"));
        assert_eq!(&got, body, "fixture file {path} changed");
    }
    assert!(!verify.join("old.txt").exists(), "deleted file reappeared");

    // History: one linear first-parent chain from main down to the git root.
    let log = client.json(&verify, &["log", "main", "-n", "200"]);
    let entries = log["entries"].as_array().expect("entries");
    assert_eq!(
        entries.len(),
        AGENTS + fx.commits.len(),
        "history length (fixture + one changeset per agent)"
    );
    for (i, e) in entries.iter().enumerate() {
        let parents = e["changeset"]["parents"].as_array().unwrap();
        match entries.get(i + 1) {
            Some(next) => {
                assert_eq!(parents.len(), 1, "entry {i} is not linear");
                assert_eq!(
                    parents[0], next["id"],
                    "entry {i} parent is not the next entry"
                );
            }
            None => assert!(parents.is_empty(), "oldest entry should be the git root"),
        }
    }
    assert_eq!(
        entries[AGENTS]["id"].as_str().unwrap(),
        imported_head,
        "imported head sits directly below the agent changesets"
    );
    for n in 0..AGENTS {
        let session = n.to_string();
        let matching: Vec<_> = entries[..AGENTS]
            .iter()
            .filter(|e| json_str(e, &["changeset", "extra", "agent.session"]) == Some(&session))
            .collect();
        assert_eq!(matching.len(), 1, "agent {n} changesets in history");
        assert_eq!(
            json_str(matching[0], &["changeset", "extra", "agent.model"]),
            Some("e2e")
        );
    }

    // Provenance queries: one agent's session, all of the model, and the totals.
    let one = client.json(&verify, &["log", "main", "--agent-session", "7"]);
    let one = one["entries"].as_array().unwrap();
    assert_eq!(one.len(), 1, "exactly agent 7's changeset");
    assert_eq!(
        json_str(&one[0], &["changeset", "extra", "agent.session"]),
        Some("7")
    );
    let by_model = client.json(
        &verify,
        &["log", "main", "--agent-model", "e2e", "-n", "200"],
    );
    assert_eq!(by_model["entries"].as_array().unwrap().len(), AGENTS);
    let other = client.json(&verify, &["log", "main", "--agent-model", "nope"]);
    assert!(other["entries"].as_array().unwrap().is_empty());
    let prov = client.json(&verify, &["provenance", "main"]);
    let agents = prov["agents"].as_array().unwrap();
    assert!(agents.iter().all(|a| a["model"] == "e2e"));
    assert_eq!(
        agents
            .iter()
            .map(|a| a["changesets"].as_u64().unwrap())
            .sum::<u64>(),
        AGENTS as u64,
        "provenance counts every agent changeset"
    );
    assert_eq!(prov["truncated"], false);
    assert_eq!(
        prov["scanned"].as_u64().unwrap() as usize,
        AGENTS + fx.commits.len()
    );
    let table = client.ok(&verify, &["provenance", "main", "--path", "agents"]);
    assert!(
        table.contains("AGENT MODEL") && table.contains("e2e"),
        "{table}"
    );
    let imported_shas: Vec<&str> = entries[AGENTS..]
        .iter()
        .rev()
        .map(|e| json_str(e, &["changeset", "extra", "git.commit"]).expect("git.commit extra"))
        .collect();
    assert_eq!(
        imported_shas,
        fx.commits.iter().map(String::as_str).collect::<Vec<_>>()
    );

    // Export back to a fresh bare repository.
    let bare = work.join("exported.git");
    fs::create_dir_all(&bare).unwrap();
    git(&bare, &["init", "-q", "--bare", "-b", "main"]);
    leep_git(&[
        "export",
        "--meta",
        meta,
        "--blobs",
        blobs,
        "--rev",
        "main",
        "--git-dir",
        bare.to_str().unwrap(),
        "--branch",
        "main",
    ]);
    git(&bare, &["fsck", "--strict"]);

    let all: Vec<String> = git(&bare, &["rev-list", "--first-parent", "--reverse", "main"])
        .lines()
        .map(String::from)
        .collect();
    assert_eq!(all.len(), AGENTS + fx.commits.len());
    assert_eq!(
        &all[..fx.commits.len()],
        &fx.commits[..],
        "imported history must round-trip with identical shas"
    );

    let tree = git(&bare, &["ls-tree", "-r", "main"]);
    let mut modes: BTreeMap<&str, &str> = BTreeMap::new();
    for line in tree.lines() {
        let (meta, path) = line.split_once('\t').unwrap();
        modes.insert(path, meta.split(' ').next().unwrap());
    }
    for n in 0..AGENTS {
        let p = format!("agents/agent-{n:02}.txt");
        assert_eq!(modes.get(p.as_str()), Some(&"100644"), "{p} in export");
        assert_eq!(git(&bare, &["show", &format!("main:{p}")]), agent_body(n));
    }
    for path in fx.files.keys().filter(|p| *p != "scripts/run.sh") {
        assert_eq!(
            modes.get(path.as_str()),
            Some(&"100644"),
            "{path} in export"
        );
    }
    assert_eq!(modes.get("scripts/run.sh"), Some(&"100755"), "exec bit");
    assert_eq!(modes.get("docs/readme-link"), Some(&"120000"), "symlink");
    assert_eq!(
        git(&bare, &["show", "main:docs/readme-link"]),
        "../README.md"
    );
    assert!(!modes.contains_key("old.txt"));
    let shared_exported = git(&bare, &["show", "main:shared.txt"]);
    assert_eq!(shared_exported, shared);

    // The checked-out working copy keeps the exec bit and the symlink too.
    let mode = fs::metadata(verify.join("scripts/run.sh"))
        .unwrap()
        .permissions()
        .mode();
    assert_ne!(mode & 0o111, 0, "script lost its exec bit");
    assert_eq!(
        fs::read_link(verify.join("docs/readme-link")).unwrap(),
        Path::new("../README.md")
    );
}

#[test]
fn git_import_swarm_and_export_round_trip() {
    let _g = serial();
    let tmp = tempfile::tempdir().unwrap();
    let stores = Stores::sqlite(&tmp.path().join("data"));
    run_import_swarm_export(&stores, tmp.path());
}

#[test]
fn git_import_swarm_and_export_round_trip_postgres() {
    let _g = serial();
    // Declared first so it drops after the server has been killed.
    let Some(db) = PgDb::create() else {
        return;
    };
    let tmp = tempfile::tempdir().unwrap();
    let stores = Stores::with_meta(&tmp.path().join("data"), db.url.clone());
    run_import_swarm_export(&stores, tmp.path());
}

#[test]
fn sparse_checkout_and_cache_sharing() {
    let _g = serial();
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    let server = Server::start(&Stores::sqlite(&root.join("data")), root);

    // Seed with its own cache so the readers below start cold.
    let seeder = Client::new(&server.url, &root.join("seed-cache"));
    let seed = root.join("seed");
    seeder.ok(root, &["init", seed.to_str().unwrap()]);
    write(&seed, "a/one.txt", "one\n");
    write(&seed, "a/same.txt", "identical content\n");
    write(&seed, "b/two.txt", "two\n");
    write(&seed, "b/same.txt", "identical content\n");
    write(&seed, "c/three.txt", "three\n");
    write(&seed, "root.txt", "root\n");
    seeder.ok(&seed, &["commit", "-m", "seed"]);
    seeder.ok(&seed, &["land"]);

    let cache = root.join("read-cache");
    let reader = Client::new(&server.url, &cache);
    let (wc1, wc2, wc3) = (root.join("wc1"), root.join("wc2"), root.join("wc3"));

    reader.ok(root, &["init", "--prefix", "a", wc1.to_str().unwrap()]);
    assert_eq!(fs::read_to_string(wc1.join("a/one.txt")).unwrap(), "one\n");
    assert!(wc1.join("a/same.txt").is_file());
    for absent in ["b", "c", "root.txt"] {
        assert!(
            !wc1.join(absent).exists(),
            "{absent} should not be materialized in wc1"
        );
    }
    assert_eq!(count_files(&cache), 2, "one.txt and the shared blob");

    reader.ok(root, &["init", "--prefix", "b", wc2.to_str().unwrap()]);
    assert_eq!(fs::read_to_string(wc2.join("b/two.txt")).unwrap(), "two\n");
    assert!(wc2.join("b/same.txt").is_file());
    for absent in ["a", "c", "root.txt"] {
        assert!(
            !wc2.join(absent).exists(),
            "{absent} should not be materialized in wc2"
        );
    }
    assert_eq!(
        count_files(&cache),
        3,
        "the identical blob came from the shared cache; only two.txt was new"
    );

    reader.ok(root, &["init", "--prefix", "a", wc3.to_str().unwrap()]);
    assert_eq!(
        count_files(&cache),
        3,
        "re-checkout of a/ downloads nothing"
    );

    // Files outside the sparse checkout are still readable on demand.
    assert_eq!(reader.ok(&wc1, &["cat", "main", "c/three.txt"]), "three\n");
    assert!(!wc1.join("c").exists(), "cat must not materialize files");
}

#[test]
fn server_restart_preserves_state() {
    let _g = serial();
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    let stores = Stores::sqlite(&root.join("data"));
    let mut server = Server::start(&stores, root);
    let port = server.addr.port();
    let client = Client::new(&server.url, &root.join("cache"));

    let (a, b) = (root.join("a"), root.join("b"));
    client.ok(root, &["init", a.to_str().unwrap()]);
    // `main` does not exist yet; this working copy only learns of it after the restart.
    client.ok(root, &["init", b.to_str().unwrap()]);

    write(&a, "dir/file.txt", "before restart\n");
    client.ok(&a, &["commit", "-m", "first"]);
    let landed = client.ok(&a, &["land"]);
    assert!(landed.contains("Landed"), "{landed}");
    let head_before = client.json(root, &["refs"]);

    server.kill();
    let server = Server::start_on(port, &stores, root);
    assert_eq!(server.url, client.server);

    assert_eq!(
        client.json(root, &["refs"]),
        head_before,
        "refs survive restart"
    );
    client.ok(&b, &["sync"]);
    assert_eq!(
        fs::read_to_string(b.join("dir/file.txt")).unwrap(),
        "before restart\n"
    );

    // The restarted server keeps accepting work.
    write(&a, "dir/second.txt", "after restart\n");
    client.ok(&a, &["commit", "-m", "second"]);
    client.ok(&a, &["land"]);
    client.ok(&b, &["sync"]);
    assert_eq!(
        fs::read_to_string(b.join("dir/second.txt")).unwrap(),
        "after restart\n"
    );
    let log = client.json(&b, &["log", "main"]);
    assert_eq!(log["entries"].as_array().unwrap().len(), 2);
    drop(server);
}

#[test]
fn signed_changesets_gate_landing_on_a_protected_ref() {
    let _g = serial();
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    let client = Client::new("http://127.0.0.1:1", &root.join("cache"));

    // `leep keygen` needs no server: it writes a 0600 key and prints the
    // entry for the server's keys file.
    let keyfile = root.join("alice.key");
    let out = client.ok(
        root,
        &[
            "keygen",
            "--out",
            keyfile.to_str().unwrap(),
            "--name",
            "alice",
        ],
    );
    assert_eq!(
        fs::metadata(&keyfile).unwrap().permissions().mode() & 0o777,
        0o600
    );
    let entry: serde_json::Value = serde_json::from_str(out.trim()).unwrap();
    assert_eq!(entry["name"], "alice");
    assert!(entry["public_key"]
        .as_str()
        .unwrap()
        .starts_with("ed25519:"));
    let keys = root.join("keys.json");
    fs::write(&keys, serde_json::json!({ "keys": [entry] }).to_string()).unwrap();

    let stores = Stores::sqlite(&root.join("data"));
    let server = Server::start_with(
        free_port(),
        &stores,
        root,
        &[
            "--signing-keys-file",
            keys.to_str().unwrap(),
            "--require-signatures",
            "main",
        ],
    );
    let client = Client::new(&server.url, &root.join("cache"));

    let wc = root.join("wc");
    client.ok(root, &["init", wc.to_str().unwrap()]);
    write(&wc, "a.txt", "unsigned\n");
    client.ok(&wc, &["commit", "-m", "unsigned"]);
    let out = client.run(&wc, &["land"]);
    assert_eq!(out.status.code(), Some(1), "{}", stderr(&out));
    let err = stderr(&out);
    assert!(err.contains("is not signed by a trusted key"), "{err}");
    assert!(err.contains("leep commit --sign"), "{err}");
    assert_eq!(client.json(&wc, &["refs"])["refs"], serde_json::json!({}));

    // A signed tip does not launder an unsigned changeset beneath it.
    write(&wc, "b.txt", "signed on top\n");
    client.ok(
        &wc,
        &[
            "commit",
            "-m",
            "signed tip",
            "--sign",
            keyfile.to_str().unwrap(),
        ],
    );
    let out = client.run(&wc, &["land"]);
    assert_eq!(out.status.code(), Some(1));
    assert!(stderr(&out).contains("is not signed by a trusted key"));

    // A fresh working copy where every changeset is signed lands.
    let wc2 = root.join("wc2");
    client.ok(root, &["init", wc2.to_str().unwrap()]);
    write(&wc2, "c.txt", "all signed\n");
    let mut cmd = client.cmd(&wc2);
    cmd.args(["commit", "-m", "signed"])
        .env("LEEP_SIGNING_KEY", &keyfile);
    let out = cmd.output().unwrap();
    assert_ok(&out, "leep commit with LEEP_SIGNING_KEY");
    assert!(stderr(&out).contains("signed as alice"), "{}", stderr(&out));
    let landed = client.ok(&wc2, &["land"]);
    assert!(landed.contains("Landed"), "{landed}");

    let shown = client.ok(&wc2, &["show", "main"]);
    assert!(shown.contains("Signed-by: alice (key "), "{shown}");
    // The unsigned history never reached main.
    let log = client.ok(&wc2, &["log", "main"]);
    assert!(!log.contains("unsigned"), "{log}");
    assert_eq!(
        client.json(&wc2, &["log", "main"])["entries"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
}

/// A user of the `leep` CLI: a token on top of the shared client settings.
struct User<'a> {
    client: &'a Client,
    token: String,
}

impl User<'_> {
    fn run(&self, cwd: &Path, args: &[&str]) -> std::process::Output {
        self.client
            .cmd(cwd)
            .env("LEEP_TOKEN", &self.token)
            .args(args)
            .output()
            .unwrap_or_else(|e| panic!("spawn leep {args:?}: {e}"))
    }

    fn ok(&self, cwd: &Path, args: &[&str]) -> String {
        let out = self.run(cwd, args);
        assert_ok(&out, &format!("leep {args:?}"));
        stdout(&out)
    }

    fn json(&self, cwd: &Path, args: &[&str]) -> serde_json::Value {
        let mut full = vec!["--json"];
        full.extend_from_slice(args);
        let out = self.ok(cwd, &full);
        serde_json::from_str(&out).unwrap_or_else(|e| panic!("bad json from {args:?}: {e}\n{out}"))
    }

    /// The changeset the working copy is on.
    fn tip(&self, wc: &Path) -> String {
        self.json(wc, &["log", "-n", "1"])["entries"][0]["id"]
            .as_str()
            .unwrap()
            .to_string()
    }
}

/// A real token and its tokens-file entry, from `leep-server gen-token`.
fn gen_token(name: &str, role: &str) -> (String, serde_json::Value) {
    let out = std::process::Command::new(bin("leep-server"))
        .args(["gen-token", "--name", name, "--role", role])
        .output()
        .unwrap();
    assert_ok(&out, "leep-server gen-token");
    let text = stdout(&out);
    let field = |key: &str| {
        text.lines()
            .find_map(|l| l.strip_prefix(key))
            .unwrap_or_else(|| panic!("no {key} in {text}"))
            .trim()
            .to_string()
    };
    (
        field("token:"),
        serde_json::from_str(&field("entry:")).unwrap(),
    )
}

#[test]
fn review_owners_and_auto_approval_gate_landing_on_a_protected_ref() {
    let _g = serial();
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();

    let mut entries = Vec::new();
    let mut token = |name: &str, role: &str| {
        let (t, e) = gen_token(name, role);
        entries.push(e);
        t
    };
    let (alice_t, bob_t, viewer_t) = (
        token("alice", "write"),
        token("bob", "write"),
        token("viewer", "read"),
    );
    let tokens = root.join("tokens.json");
    fs::write(
        &tokens,
        serde_json::json!({ "tokens": entries }).to_string(),
    )
    .unwrap();
    let policy = root.join("review.json");
    fs::write(
        &policy,
        r#"{ "refs": ["main"], "auto_approve": ["comment_only"] }"#,
    )
    .unwrap();

    let stores = Stores::sqlite(&root.join("data"));
    let server = Server::start_with(
        free_port(),
        &stores,
        root,
        &[
            "--tokens-file",
            tokens.to_str().unwrap(),
            "--review-policy-file",
            policy.to_str().unwrap(),
            "--require-review",
            "release",
        ],
    );
    let client = Client::new(&server.url, &root.join("cache"));
    let alice = User {
        client: &client,
        token: alice_t,
    };
    let bob = User {
        client: &client,
        token: bob_t,
    };
    let viewer = User {
        client: &client,
        token: viewer_t,
    };

    // Alice seeds main. Nothing is owned yet, so one approval from anyone
    // is enough, and she is told how to get it.
    let wc_a = root.join("wc-alice");
    alice.ok(root, &["init", wc_a.to_str().unwrap()]);
    write(&wc_a, "OWNERS", "# alice owns everything\nalice\n");
    write(&wc_a, "src/lib.rs", "fn a() {}\n");
    alice.ok(&wc_a, &["commit", "-m", "seed"]);
    let seed = alice.tip(&wc_a);
    let out = alice.run(&wc_a, &["land"]);
    assert_eq!(out.status.code(), Some(1), "{}", stderr(&out));
    let err = stderr(&out);
    assert!(err.contains("needs review"), "{err}");
    assert!(err.contains("leep review approve"), "{err}");
    assert_eq!(alice.json(&wc_a, &["refs"])["refs"], serde_json::json!({}));

    let out = alice.run(&wc_a, &["review", "status", "@"]);
    assert_eq!(out.status.code(), Some(1));
    let text = stdout(&out);
    assert!(
        text.contains("not approved") && text.contains("PENDING"),
        "{text}"
    );

    // A read token cannot review; reading is fine.
    let out = viewer.run(root, &["review", "approve", &seed]);
    assert_eq!(out.status.code(), Some(1));
    assert!(
        stderr(&out).contains("token lacks write access"),
        "{}",
        stderr(&out)
    );
    assert_eq!(
        viewer.run(root, &["review", "status", &seed]).status.code(),
        Some(1)
    );

    // Bob approves from outside any working copy; the reviewer is his token.
    let out = bob.ok(root, &["review", "approve", &seed, "-m", "fine"]);
    assert!(out.contains("as bob"), "{out}");
    let list = alice.ok(&wc_a, &["reviews", &seed]);
    assert!(
        list.contains("bob approve") && list.contains("\"fine\""),
        "{list}"
    );
    assert_eq!(alice.ok(&wc_a, &["review", "list", &seed]), list);
    let json = alice.json(&wc_a, &["review", "status", "@"]);
    assert_eq!(json["approved"], true);
    let landed = alice.ok(&wc_a, &["land"]);
    assert!(landed.contains("Landed"), "{landed}");

    // Now alice owns everything. A comment-only change lands without review.
    let wc_b = root.join("wc-bob");
    bob.ok(root, &["init", wc_b.to_str().unwrap()]);
    write(
        &wc_b,
        "src/lib.rs",
        "// the entry point\nfn a() {} // trailing\n",
    );
    bob.ok(&wc_b, &["commit", "-m", "comment"]);
    let status = bob.ok(&wc_b, &["review", "status", "@"]);
    assert!(status.contains("auto:comment_only"), "{status}");
    assert!(bob.ok(&wc_b, &["land"]).contains("Landed"));

    // A code change needs alice. Bob's own approval does not count.
    write(
        &wc_b,
        "src/lib.rs",
        "// the entry point\nfn a() { 1 } // trailing\n",
    );
    bob.ok(&wc_b, &["commit", "-m", "code"]);
    let code = bob.tip(&wc_b);
    bob.ok(&wc_b, &["review", "approve", "@"]);
    let out = bob.run(&wc_b, &["land"]);
    assert_eq!(out.status.code(), Some(1));
    assert!(
        stderr(&out).contains("needs approval from one of [alice]"),
        "{}",
        stderr(&out)
    );

    // An owner's rejection blocks it; a reject needs a reason.
    let out = alice.run(root, &["review", "reject", &code]);
    assert_eq!(out.status.code(), Some(2), "clap usage error");
    alice.ok(root, &["review", "reject", &code, "-m", "not yet"]);
    let out = bob.run(&wc_b, &["land"]);
    assert!(
        stderr(&out).contains("rejected by alice"),
        "{}",
        stderr(&out)
    );
    // Not approved is exit 1, so read the JSON from the raw output.
    let out = bob.run(&wc_b, &["--json", "review", "status", &code]);
    assert_eq!(out.status.code(), Some(1));
    let status: serde_json::Value = serde_json::from_str(&stdout(&out)).unwrap();
    assert_eq!(status["approved"], false);
    assert_eq!(status["paths"][0]["rule"], "rejected");
    assert_eq!(
        status["paths"][0]["rejected_by"],
        serde_json::json!(["alice"])
    );

    // She changes her mind.
    alice.ok(root, &["review", "approve", &code, "-m", "ok now"]);
    assert!(bob.ok(&wc_b, &["land"]).contains("Landed"));
    let refs = alice.json(&wc_a, &["refs"]);
    assert_eq!(refs["refs"]["main"], code);

    // Bob cannot make himself an owner: OWNERS is judged by the head's owners.
    write(&wc_b, "OWNERS", "alice\nbob\n");
    bob.ok(&wc_b, &["commit", "-m", "add bob"]);
    let own = bob.tip(&wc_b);
    bob.ok(&wc_b, &["review", "approve", "@"]);
    let out = bob.run(&wc_b, &["land"]);
    assert!(
        stderr(&out).contains("path OWNERS needs approval from one of [alice]"),
        "{}",
        stderr(&out)
    );
    alice.ok(root, &["review", "approve", &own]);
    assert!(bob.ok(&wc_b, &["land"]).contains("Landed"));

    // `release` is protected by --require-review alone. It has no head yet,
    // so nothing is owned there, and a new change needs some approval.
    write(
        &wc_b,
        "src/lib.rs",
        "// the entry point\nfn a() { 2 } // trailing\n",
    );
    bob.ok(&wc_b, &["commit", "-m", "more code"]);
    let out = bob.run(&wc_b, &["land", "--ref", "release"]);
    assert_eq!(out.status.code(), Some(1), "{}", stderr(&out));
    assert!(
        stderr(&out).contains("path src/lib.rs needs approval from any reviewer"),
        "{}",
        stderr(&out)
    );
    alice.ok(root, &["review", "approve", &bob.tip(&wc_b)]);
    assert!(bob
        .ok(&wc_b, &["land", "--ref", "release"])
        .contains("Landed"));
}
