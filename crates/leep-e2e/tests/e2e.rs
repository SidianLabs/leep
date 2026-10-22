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
    // Every command and its output, for the report if this agent's edit is
    // later missing from main.
    let transcript = root.join(format!("transcript-{n:02}.txt"));
    let step = |args: &[&str]| -> Result<std::process::Output, String> {
        let out = client.run(&wc, args);
        let shared = fs::read_to_string(wc.join("shared.txt")).unwrap_or_default();
        let entry = format!(
            "$ leep {}\n{}--- stdout\n{}--- stderr\n{}--- shared.txt after\n{shared}\n",
            args.join(" "),
            out.status,
            stdout(&out),
            stderr(&out),
        );
        fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&transcript)
            .and_then(|mut f| std::io::Write::write_all(&mut f, entry.as_bytes()))
            .map_err(|e| e.to_string())?;
        Ok(out)
    };
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
            Some(5) if stdout(&out).contains("Cannot land cleanly") => {
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
            "edit of agent {n} lost from shared.txt:\n{shared}\n--- agent {n} transcript\n{}\n--- log main\n{}",
            fs::read_to_string(work.join(format!("transcript-{n:02}.txt"))).unwrap_or_default(),
            stdout(&client.run(&verify, &["log", "main", "-n", "80"])),
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

    // Summary entries: the same history without changed paths, and
    // `--oneline` is one line per entry, the short id then the subject.
    let summary = client.json(&verify, &["log", "main", "-n", "200", "--oneline"]);
    let sums = summary["entries"].as_array().expect("summary entries");
    assert_eq!(sums.len(), entries.len());
    for (s, e) in sums.iter().zip(entries) {
        assert_eq!(s["id"], e["id"]);
        assert_eq!(s["parents"], e["changeset"]["parents"]);
        let message = e["changeset"]["message"].as_str().unwrap();
        assert_eq!(
            s["message_first_line"],
            message.lines().next().unwrap_or("")
        );
        assert_eq!(s["generation"], e["generation"]);
        assert_eq!(s["root_tree"], e["root_tree"]);
        let changed = e["changeset"]["changes"].as_object().unwrap().len();
        assert_eq!(s["files_changed"], changed);
        assert!(s.get("changeset").is_none() && s.get("changes").is_none());
        let agent: serde_json::Map<String, serde_json::Value> = e["changeset"]["extra"]
            .as_object()
            .unwrap()
            .iter()
            .filter(|(k, _)| k.starts_with("agent."))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        assert_eq!(s["agent"], serde_json::Value::Object(agent));
    }
    let text = client.ok(&verify, &["log", "main", "-n", "200", "--oneline"]);
    let lines: Vec<&str> = text.lines().collect();
    assert_eq!(lines.len(), entries.len(), "{text}");
    for (line, e) in lines.iter().zip(entries) {
        let id = e["id"].as_str().unwrap();
        let subject = e["changeset"]["message"]
            .as_str()
            .unwrap()
            .lines()
            .next()
            .unwrap_or("");
        assert_eq!(*line, format!("{} {subject}", &id[..12]));
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

    // Path history: the index answers the same as the changes it was built
    // from (history is linear, so each entry's own changes name the path).
    let shared_log = client.json(
        &verify,
        &["log", "main", "--path", "shared.txt", "-n", "200"],
    );
    let shared_log = shared_log["entries"].as_array().unwrap();
    assert!(shared_log.len() > CONFLICTING.len(), "shared.txt history");
    for e in shared_log {
        assert!(
            e["changeset"]["changes"].get("shared.txt").is_some(),
            "path log yielded a changeset that did not touch shared.txt"
        );
    }
    let touching = entries
        .iter()
        .filter(|e| e["changeset"]["changes"].get("shared.txt").is_some())
        .count();
    assert_eq!(shared_log.len(), touching, "path log versus full scan");
    let last = client.json(&verify, &["last-change", "shared.txt", "--rev", "main"]);
    assert_eq!(last["changeset"]["id"], shared_log[0]["id"]);

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

    // What the import, the swarm's rebases and the export left behind is
    // sound all the way down (ADR 0025).
    let out = server_cmd(stores, &["verify", "--full", "--rehash"]);
    assert_ok(&out, "leep-server verify --full --rehash");
    assert!(stdout(&out).contains("result: clean"), "{}", stdout(&out));
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

/// Run `leep-server gc ...` against `stores` and return its stdout.
fn server_gc(stores: &Stores, args: &[&str]) -> String {
    let out = std::process::Command::new(bin("leep-server"))
        .args(["--meta", &stores.meta, "--blobs", &stores.blobs, "gc"])
        .args(args)
        .env("RUST_LOG", "warn")
        .output()
        .expect("spawn leep-server gc");
    assert_ok(&out, &format!("leep-server gc {args:?}"));
    stdout(&out)
}

/// Write `data` straight into a filesystem blob store, as an upload whose
/// commit never came (and that nothing has claimed).
fn plant_orphan(blobs_dir: &Path, data: &[u8]) -> PathBuf {
    let hex = leep_core::Hash::of_blob(data).to_hex();
    let path = blobs_dir.join(&hex[..2]).join(&hex[2..]);
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(&path, data).unwrap();
    path
}

#[test]
fn fenced_gc_removes_orphans_and_keeps_live_data() {
    let _g = serial();
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    let stores = Stores::sqlite(&root.join("data"));
    let server = Server::start(&stores, root);
    let client = Client::new(&server.url, &root.join("cache"));

    let wc = root.join("wc");
    client.ok(root, &["init", wc.to_str().unwrap()]);
    write(&wc, "dir/live.txt", "live data\n");
    client.ok(&wc, &["commit", "-m", "live"]);
    client.ok(&wc, &["land"]);

    let blobs_dir = root.join("data/blobs");
    let live_files = count_files(&blobs_dir);
    let orphan_a = plant_orphan(&blobs_dir, b"orphan a");
    let orphan_b = plant_orphan(&blobs_dir, b"orphan b");
    assert_eq!(count_files(&blobs_dir), live_files + 2);

    // Dry run reports and writes nothing; condemn deletes nothing.
    let dry = server_gc(&stores, &["--dry-run", "--grace", "0"]);
    assert!(dry.contains("would condemn: 2 blobs"), "{dry}");
    let condemned = server_gc(&stores, &["condemn", "--grace", "0"]);
    assert!(condemned.contains("condemned: 2 blobs"), "{condemned}");
    assert!(orphan_a.exists() && orphan_b.exists());

    // Collect honors the fence: nothing is old enough yet.
    let early = server_gc(&stores, &["collect", "--fence", "10m"]);
    assert!(early.contains("deleted: 0 blobs"), "{early}");
    assert!(orphan_a.exists() && orphan_b.exists());

    // Separate processes: the condemnation time comes from the stored rows.
    let done = server_gc(&stores, &["collect", "--fence", "0s", "--unsafe-no-fence"]);
    assert!(done.contains("deleted: 2 blobs"), "{done}");
    assert!(!orphan_a.exists() && !orphan_b.exists());
    assert_eq!(count_files(&blobs_dir), live_files);

    // The combined command (condemn, wait, collect) and a refused zero fence.
    let orphan_c = plant_orphan(&blobs_dir, b"orphan c");
    let refused = std::process::Command::new(bin("leep-server"))
        .args(["--meta", &stores.meta, "--blobs", &stores.blobs])
        .args(["gc", "--grace", "0", "--fence", "0s"])
        .output()
        .unwrap();
    assert!(!refused.status.success());
    assert!(orphan_c.exists());
    let both = server_gc(&stores, &["--grace", "0", "--fence", "1s"]);
    assert!(both.contains("deleted: 1 blobs"), "{both}");
    assert!(!orphan_c.exists());

    // Live data survives, for a fresh checkout and for new work.
    let fresh = root.join("fresh");
    client.ok(root, &["init", fresh.to_str().unwrap()]);
    client.ok(&fresh, &["sync"]);
    assert_eq!(
        fs::read_to_string(fresh.join("dir/live.txt")).unwrap(),
        "live data\n"
    );
    write(&wc, "dir/more.txt", "more\n");
    client.ok(&wc, &["commit", "-m", "more"]);
    client.ok(&wc, &["land"]);
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
    // A ref that does not exist yet needs --create (ADR 0036).
    let out = bob.run(&wc_b, &["land", "--ref", "release"]);
    assert_eq!(out.status.code(), Some(4), "{}", stderr(&out));
    assert!(stderr(&out).contains("--create"), "{}", stderr(&out));
    let out = bob.run(&wc_b, &["land", "--ref", "release", "--create"]);
    assert_eq!(out.status.code(), Some(1), "{}", stderr(&out));
    assert!(
        stderr(&out).contains("path src/lib.rs needs approval from any reviewer"),
        "{}",
        stderr(&out)
    );
    alice.ok(root, &["review", "approve", &bob.tip(&wc_b)]);
    assert!(bob
        .ok(&wc_b, &["land", "--ref", "release", "--create"])
        .contains("Landed"));
}

/// Run `leep-server derive-search REV` against `stores` and return its stdout.
fn server_derive_search(stores: &Stores, rev: &str) -> String {
    let out = std::process::Command::new(bin("leep-server"))
        .args(["--meta", &stores.meta, "--blobs", &stores.blobs])
        .args(["derive-search", rev])
        .env("RUST_LOG", "warn")
        .output()
        .expect("spawn leep-server derive-search");
    assert_ok(&out, &format!("leep-server derive-search {rev}"));
    stdout(&out)
}

#[test]
fn code_search_over_the_real_binaries() {
    let _g = serial();
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    let stores = Stores::sqlite(&root.join("data"));
    // The test counts the rows `derive-search` writes, so the warmer must not
    // derive them first.
    let server = Server::start_with(free_port(), &stores, root, &["--warm-interval", "0"]);
    let client = Client::new(&server.url, &root.join("cache"));

    let wc = root.join("wc");
    client.ok(root, &["init", wc.to_str().unwrap()]);
    write(
        &wc,
        "src/lib.rs",
        "pub fn parse_config() {}\n// TODO: handle Errors\n",
    );
    write(&wc, "src/main.rs", "fn main() {\n    parse_config();\n}\n");
    write(
        &wc,
        "docs/guide.md",
        "Run parse_config first.\nthen TODO later\n",
    );
    write(&wc, "data.bin", "parse_config\0binary");
    client.ok(&wc, &["commit", "-m", "seed"]);
    client.ok(&wc, &["land"]);

    // A literal, grep-style output.
    assert_eq!(
        client.ok(&wc, &["search", "parse_config"]),
        "docs/guide.md:1:Run parse_config first.\n\
         src/lib.rs:1:pub fn parse_config() {}\n\
         src/main.rs:2:    parse_config();\n"
    );
    // Works outside a working copy too, against a named revision.
    assert_eq!(
        client.ok(
            root,
            &["search", "--rev", "main", "-n", "1", "parse_config"]
        ),
        "docs/guide.md:1:Run parse_config first.\n"
    );

    // Options.
    assert_eq!(
        client.ok(&wc, &["search", "-i", "todo", "--path", "src"]),
        "src/lib.rs:2:// TODO: handle Errors\n"
    );
    assert_eq!(
        client.ok(&wc, &["search", "-E", r"fn\s+\w+\(", "--glob", "**/*.rs"]),
        "src/lib.rs:1:pub fn parse_config() {}\nsrc/main.rs:1:fn main() {\n"
    );
    assert_eq!(
        client.ok(&wc, &["search", "TODO", "--glob", "docs/*.md"]),
        "docs/guide.md:2:then TODO later\n"
    );

    // Truncation is reported on stderr, not in the rows.
    let out = client.run(&wc, &["search", "-n", "2", "parse_config"]);
    assert_ok(&out, "truncated search");
    assert_eq!(stdout(&out).lines().count(), 2);
    assert!(stderr(&out).contains("truncated"), "{}", stderr(&out));

    // Nothing found is exit 1 with no output; a bad regex is a user error.
    let out = client.run(&wc, &["search", "no such text"]);
    assert_eq!(out.status.code(), Some(1));
    assert!(stdout(&out).is_empty() && stderr(&out).is_empty());
    let out = client.run(&wc, &["search", "-E", "("]);
    assert_eq!(out.status.code(), Some(1));
    assert!(stderr(&out).contains("bad regex"), "{}", stderr(&out));

    // JSON carries the matches, ranges and what the index did.
    let v = client.json(&wc, &["search", "TODO"]);
    assert_eq!(v["matches"].as_array().unwrap().len(), 2);
    assert_eq!(v["matches"][0]["ranges"][0]["start"], 5);
    assert_eq!(v["truncated"], false);
    assert_eq!(v["stats"]["indexed"], true);
    assert_eq!(v["stats"]["files_skipped"], 1);
    assert_eq!(v["stats"]["blob_rows_derived"], 0, "already derived above");

    // The server can precompute the index; a new revision then only derives
    // the rows on the paths it changed.
    let first = server_derive_search(&stores, "main");
    assert!(first.contains("blob rows derived: 0"), "{first}");
    write(
        &wc,
        "src/main.rs",
        "fn main() {\n    parse_config(); // run\n}\n",
    );
    client.ok(&wc, &["commit", "-m", "edit"]);
    client.ok(&wc, &["land"]);
    let second = server_derive_search(&stores, "main");
    assert!(second.contains("blob rows derived: 1"), "{second}");
    assert!(
        second.contains("tree rows derived: 2"),
        "root and src: {second}"
    );
    assert!(server_derive_search(&stores, "main").contains("tree rows derived: 0"));
    let v = client.json(&wc, &["search", "// run"]);
    assert_eq!(v["matches"].as_array().unwrap().len(), 1);
    assert_eq!(v["stats"]["tree_rows_derived"], 0);
    // The earlier revision still answers as it did.
    let log = client.json(&wc, &["log", "main"]);
    let first_id = log["entries"][1]["id"].as_str().unwrap().to_string();
    let v = client.json(&wc, &["search", "--rev", &first_id, "// run"]);
    assert!(v["matches"].as_array().unwrap().is_empty());
}

#[test]
fn renames_in_diff_status_and_log_follow() {
    let _g = serial();
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    let stores = Stores::sqlite(&root.join("data"));
    let server = Server::start(&stores, root);
    let client = Client::new(&server.url, &root.join("cache"));

    let wc = root.join("wc");
    client.ok(root, &["init", wc.to_str().unwrap()]);
    let body: String = (1..=12).map(|i| format!("fn step_{i}() {{}}\n")).collect();
    write(&wc, "src/old_name.rs", &body);
    write(&wc, "src/other.rs", "fn other() {}\n");
    client.ok(&wc, &["commit", "-m", "add old_name"]);
    client.ok(&wc, &["land"]);
    write(
        &wc,
        "src/old_name.rs",
        &format!("{body}fn step_13() {{}}\n"),
    );
    client.ok(&wc, &["commit", "-m", "extend old_name"]);
    client.ok(&wc, &["land"]);

    // Rename with a small edit, uncommitted: status and diff say renamed.
    fs::remove_file(wc.join("src/old_name.rs")).unwrap();
    let renamed = format!("{body}fn step_13() {{}}\nfn step_14() {{}}\n");
    write(&wc, "lib/new_name.rs", &renamed);
    let status = client.ok(&wc, &["status"]);
    assert!(
        status.contains("renamed: src/old_name.rs -> lib/new_name.rs ("),
        "{status}"
    );
    assert!(!status.contains("D src/old_name.rs") && !status.contains("A lib/new_name.rs"));
    let diff = client.ok(&wc, &["diff"]);
    assert!(
        diff.contains("renamed: src/old_name.rs -> lib/new_name.rs ("),
        "{diff}"
    );
    assert!(
        diff.contains("--- a/src/old_name.rs\n+++ b/lib/new_name.rs\n"),
        "{diff}"
    );
    assert!(diff.contains("+fn step_14() {}"), "{diff}");
    let off = client.ok(&wc, &["diff", "--no-renames"]);
    assert!(
        !off.contains("renamed:") && off.contains("+++ /dev/null"),
        "{off}"
    );
    let j = client.json(&wc, &["status"]);
    assert_eq!(
        j["changes"].as_array().unwrap().len(),
        2,
        "raw changes stay"
    );
    assert_eq!(j["renames"][0]["from"], "src/old_name.rs");

    client.ok(&wc, &["commit", "-m", "rename to new_name"]);
    client.ok(&wc, &["land"]);
    write(
        &wc,
        "lib/new_name.rs",
        &format!("{renamed}fn step_15() {{}}\n"),
    );
    client.ok(&wc, &["commit", "-m", "edit new_name"]);
    client.ok(&wc, &["land"]);

    // Between revisions, from the server.
    let log = client.json(&wc, &["log", "main"]);
    let rename_cs = log["entries"][1]["id"].as_str().unwrap().to_string();
    let before = log["entries"][2]["id"].as_str().unwrap().to_string();
    let diff = client.ok(&wc, &["diff", &before, &rename_cs]);
    assert!(
        diff.contains("renamed: src/old_name.rs -> lib/new_name.rs ("),
        "{diff}"
    );
    let v = client.json(&wc, &["diff", &before, &rename_cs]);
    assert_eq!(v["entries"].as_array().unwrap().len(), 2);
    assert_eq!(v["renames"][0]["to"], "lib/new_name.rs");
    assert_eq!(v["renames"][0]["exact"], false);
    let strict = client.ok(&wc, &["diff", "-M=99", &before, &rename_cs]);
    assert!(!strict.contains("renamed:"), "{strict}");

    // History: the plain path log stops at the rename, --follow does not.
    let plain = client.json(&wc, &["log", "--path", "lib/new_name.rs"]);
    assert_eq!(plain["entries"].as_array().unwrap().len(), 2);
    let followed = client.json(&wc, &["log", "--follow", "lib/new_name.rs"]);
    let msgs: Vec<_> = followed["entries"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["changeset"]["message"].as_str().unwrap())
        .collect();
    assert_eq!(
        msgs,
        [
            "edit new_name",
            "rename to new_name",
            "extend old_name",
            "add old_name"
        ]
    );
    assert_eq!(followed["entries"][3]["path_at"], "src/old_name.rs");
    assert_eq!(followed["entries"][1]["renamed"]["from"], "src/old_name.rs");
    let text = client.ok(&wc, &["log", "--follow", "lib/new_name.rs"]);
    assert!(text.contains("File:   src/old_name.rs"), "{text}");
    assert!(
        text.contains("renamed: src/old_name.rs -> lib/new_name.rs ("),
        "{text}"
    );

    // One file only.
    let out = client.run(&wc, &["log", "--follow", "lib"]);
    assert_eq!(out.status.code(), Some(1));
    assert!(stderr(&out).contains("directory"), "{}", stderr(&out));
}

fn run_branch_and_tag_workflow(stores: &Stores, work: &Path) {
    let server = Server::start(stores, work);
    let client = Client::new(&server.url, &work.join("cache"));
    let wc = work.join("wc");
    client.ok(work, &["init", wc.to_str().unwrap()]);
    let refs = |dir: &Path| client.json(dir, &["refs"])["refs"].clone();

    write(&wc, "a.txt", "one\n");
    client.ok(&wc, &["commit", "-m", "one"]);
    client.ok(&wc, &["land"]);
    let main_head = refs(&wc)["main"].as_str().unwrap().to_string();

    // Create a branch at the base and switch to it.
    client.ok(&wc, &["branch", "feature"]);
    let again = client.run(&wc, &["branch", "feature"]);
    assert_eq!(again.status.code(), Some(1));
    assert!(stderr(&again).contains("branch feature already exists"));
    let listing = client.ok(&wc, &["branch"]);
    assert!(listing.contains("* main "), "{listing}");
    assert!(listing.contains("  feature "), "{listing}");
    client.ok(&wc, &["switch", "feature"]);
    assert!(client.ok(&wc, &["status"]).contains("target feature"));
    assert!(client.ok(&wc, &["branch"]).contains("* feature "));

    // A dirty working copy cannot switch.
    write(&wc, "scratch.txt", "x\n");
    let dirty = client.run(&wc, &["switch", "main"]);
    assert_eq!(dirty.status.code(), Some(1));
    assert!(stderr(&dirty).contains("uncommitted"), "{}", stderr(&dirty));
    fs::remove_file(wc.join("scratch.txt")).unwrap();

    // Commit and land go to the branch; main does not move.
    write(&wc, "b.txt", "on feature\n");
    client.ok(&wc, &["commit", "-m", "feature work"]);
    client.ok(&wc, &["land"]);
    let now = refs(&wc);
    assert_eq!(now["main"], main_head.as_str());
    assert_ne!(now["feature"], main_head.as_str());

    // Back on main, the unmerged branch is protected from `-d`.
    client.ok(&wc, &["switch", "main"]);
    assert!(!wc.join("b.txt").exists());
    let out = client.run(&wc, &["branch", "-d", "feature"]);
    assert_eq!(out.status.code(), Some(1));
    assert!(
        stderr(&out).contains("not fully merged"),
        "{}",
        stderr(&out)
    );
    assert!(stderr(&out).contains("-D"));
    assert!(refs(&wc).get("feature").is_some());
    // A branch already in main's history deletes normally.
    client.ok(&wc, &["branch", "merged"]);
    client.ok(&wc, &["branch", "-d", "merged"]);
    assert!(refs(&wc).get("merged").is_none());
    // The target branch itself is never deleted.
    let out = client.run(&wc, &["branch", "-D", "main"]);
    assert_eq!(out.status.code(), Some(1));
    // `switch -c` creates and switches in one step.
    client.ok(&wc, &["switch", "-c", "topic"]);
    assert!(client.ok(&wc, &["status"]).contains("target topic"));
    client.ok(&wc, &["switch", "main"]);
    client.ok(&wc, &["branch", "-D", "topic"]);
    client.ok(&wc, &["branch", "-D", "feature"]);
    assert!(refs(&wc).get("feature").is_none());

    // Tags: created once, listed by short name, resolvable, never moved.
    client.ok(&wc, &["tag", "v1"]);
    assert_eq!(refs(&wc)["tags/v1"], main_head.as_str());
    assert!(client.ok(&wc, &["tag"]).starts_with("v1 "));
    assert!(!client.ok(&wc, &["branch"]).contains("v1"));
    assert!(client.ok(&wc, &["log", "tags/v1"]).contains("one"));
    assert!(client.ok(&wc, &["log", "v1"]).contains("one"));
    let out = client.run(&wc, &["tag", "v1"]);
    assert_eq!(out.status.code(), Some(1));
    assert!(stderr(&out).contains("already exists"), "{}", stderr(&out));

    // The server enforces it for raw API callers too.
    let other = "00".repeat(32);
    let body = format!(r#"{{"expected":"{main_head}","new":"{other}"}}"#);
    let (status, text) = post_json(server.addr, "/v1/refs/tags/v1", &body);
    assert_eq!(status, 400, "{text}");
    assert!(text.contains("immutable"), "{text}");
    let body = format!(r#"{{"changeset":"{main_head}","target_ref":"tags/v1"}}"#);
    assert_eq!(post_json(server.addr, "/v1/land", &body).0, 400);
    assert_eq!(refs(&wc)["tags/v1"], main_head.as_str());

    // Delete, and the name is free again.
    client.ok(&wc, &["tag", "-d", "v1"]);
    assert!(refs(&wc).get("tags/v1").is_none());
    assert_eq!(client.ok(&wc, &["tag"]), "");
    let out = client.run(&wc, &["tag", "-d", "v1"]);
    assert_eq!(out.status.code(), Some(4));
}

#[test]
fn branches_switching_and_immutable_tags() {
    let _g = serial();
    let tmp = tempfile::tempdir().unwrap();
    let stores = Stores::sqlite(&tmp.path().join("data"));
    run_branch_and_tag_workflow(&stores, tmp.path());
}

#[test]
fn branches_switching_and_immutable_tags_postgres() {
    let _g = serial();
    let Some(db) = PgDb::create() else {
        return;
    };
    let tmp = tempfile::tempdir().unwrap();
    let stores = Stores::with_meta(&tmp.path().join("data"), db.url.clone());
    run_branch_and_tag_workflow(&stores, tmp.path());
}

fn run_everyday_workflow(stores: &Stores, work: &Path) {
    let server = Server::start(stores, work);
    let client = Client::new(&server.url, &work.join("cache"));
    let (a, b) = (work.join("a"), work.join("b"));
    let main_head = |dir: &Path| {
        client.json(dir, &["refs"])["refs"]["main"]
            .as_str()
            .unwrap()
            .to_string()
    };

    client.ok(work, &["init", a.to_str().unwrap()]);
    write(&a, "doc.txt", "one\ntwo\nthree\n");
    write(&a, "b.txt", "b\n");
    write(&a, "c.txt", "c\n");
    client.ok(&a, &["commit", "-m", "start"]);
    client.ok(&a, &["land"]);
    client.ok(work, &["init", b.to_str().unwrap()]);
    assert_eq!(fs::read_to_string(b.join("c.txt")).unwrap(), "c\n");

    // A lands while B has uncommitted edits; B syncs and keeps them.
    write(&a, "c.txt", "c from A\n");
    client.ok(&a, &["commit", "-m", "A edits c"]);
    client.ok(&a, &["land"]);
    write(&b, "b.txt", "b from B\n");
    write(&b, "new.txt", "new in B\n");
    let out = client.run(&b, &["sync", "--no-rebase"]);
    assert_eq!(out.status.code(), Some(1));
    assert!(stderr(&out).contains("uncommitted"), "{}", stderr(&out));
    client.ok(&b, &["sync"]);
    assert_eq!(fs::read_to_string(b.join("c.txt")).unwrap(), "c from A\n");
    assert_eq!(fs::read_to_string(b.join("b.txt")).unwrap(), "b from B\n");
    assert_eq!(fs::read_to_string(b.join("new.txt")).unwrap(), "new in B\n");
    let st = client.json(&b, &["status"]);
    let changed: Vec<(&str, &str)> = st["changes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| (c["path"].as_str().unwrap(), c["status"].as_str().unwrap()))
        .collect();
    assert_eq!(changed, [("b.txt", "modified"), ("new.txt", "added")]);
    assert_eq!(st["base"], main_head(&b).as_str());
    assert_eq!(st["on_target"], true);
    client.ok(&b, &["commit", "-m", "B work"]);
    client.ok(&b, &["land"]);

    // A conflicting edit: markers, and commit refuses until they are gone.
    write(&a, "doc.txt", "one from A\ntwo\nthree\n");
    client.ok(&a, &["commit", "-m", "A edits doc"]);
    client.ok(&a, &["land"]);
    write(&b, "doc.txt", "one from B\ntwo\nthree\n");
    write(&b, "extra.txt", "extra\n");
    let out = client.run(&b, &["sync"]);
    assert_eq!(out.status.code(), Some(5), "{}", stderr(&out));
    assert!(stdout(&out).contains("C doc.txt"), "{}", stdout(&out));
    let doc = fs::read_to_string(b.join("doc.txt")).unwrap();
    assert!(doc.contains("<<<<<<<") && doc.contains("one from A") && doc.contains("one from B"));
    // The server's diff3 markers, relabelled by role.
    assert!(
        doc.contains("<<<<<<< main (landed)\n") && doc.contains(">>>>>>> yours\n"),
        "{doc}"
    );
    assert!(client.ok(&b, &["status"]).contains("C doc.txt"));
    let out = client.run(&b, &["commit", "-m", "too soon"]);
    assert_eq!(out.status.code(), Some(5));
    assert!(stderr(&out).contains("conflict"), "{}", stderr(&out));
    let out = client.run(&b, &["land"]);
    assert_eq!(out.status.code(), Some(5));
    write(&b, "doc.txt", "one merged\ntwo\nthree\n");
    client.ok(&b, &["commit", "-m", "merged doc"]);
    client.ok(&b, &["land"]);
    assert_eq!(
        client.ok(&b, &["cat", "main", "doc.txt"]),
        "one merged\ntwo\nthree\n"
    );
    assert_eq!(client.ok(&b, &["cat", "main", "extra.txt"]), "extra\n");

    // Stash: push, list, show, pop.
    write(&b, "b.txt", "stashed edit\n");
    write(&b, "scratch.txt", "scratch\n");
    fs::remove_file(b.join("c.txt")).unwrap();
    let out = client.ok(&b, &["stash", "-m", "half done"]);
    assert!(out.contains("Saved stash 0"), "{out}");
    assert_eq!(fs::read_to_string(b.join("b.txt")).unwrap(), "b from B\n");
    assert_eq!(fs::read_to_string(b.join("c.txt")).unwrap(), "c from A\n");
    assert!(!b.join("scratch.txt").exists());
    assert!(client.ok(&b, &["status"]).contains("1 stash"));
    assert_eq!(client.json(&b, &["status"])["stashes"], 1);
    assert!(client.ok(&b, &["stash", "list"]).contains("half done"));
    let shown = client.ok(&b, &["stash", "show"]);
    assert!(
        shown.contains("M b.txt") && shown.contains("A scratch.txt") && shown.contains("D c.txt"),
        "{shown}"
    );
    let out = client.run(&b, &["stash", "drop", "7"]);
    assert_eq!(out.status.code(), Some(4));
    // Another change lands first; the stash still applies on top of it.
    write(&a, "c.txt", "c again from A\n");
    client.ok(&a, &["commit", "-m", "A edits c again"]);
    client.ok(&a, &["land"]);
    client.ok(&b, &["sync"]);
    // The stash deletes c.txt, which the head changed: no text merge, so the
    // pop keeps the stash and flags the path until it is resolved.
    let out = client.run(&b, &["stash", "pop"]);
    assert_eq!(out.status.code(), Some(5), "{}", stderr(&out));
    assert!(stdout(&out).contains("no text merge"), "{}", stdout(&out));
    assert!(client.ok(&b, &["status"]).contains("C c.txt"));
    client.ok(&b, &["resolve", "c.txt"]);
    client.ok(&b, &["stash", "drop"]);
    assert_eq!(
        fs::read_to_string(b.join("b.txt")).unwrap(),
        "stashed edit\n"
    );
    assert_eq!(
        fs::read_to_string(b.join("scratch.txt")).unwrap(),
        "scratch\n"
    );
    assert!(!b.join("c.txt").exists(), "stashed delete came back");
    assert_eq!(client.ok(&b, &["stash", "list"]), "");
    let out = client.run(&b, &["stash", "pop"]);
    assert_eq!(out.status.code(), Some(4));

    // Restore: one file back, the new one is left unless --force.
    client.ok(&b, &["restore", "b.txt", "c.txt"]);
    assert_eq!(fs::read_to_string(b.join("b.txt")).unwrap(), "b from B\n");
    assert_eq!(
        fs::read_to_string(b.join("c.txt")).unwrap(),
        "c again from A\n"
    );
    client.ok(&b, &["restore", "scratch.txt"]);
    assert!(b.join("scratch.txt").exists());
    client.ok(&b, &["restore", "--force", "scratch.txt"]);
    assert!(!b.join("scratch.txt").exists());
    assert!(client.ok(&b, &["status"]).contains("working copy clean"));
    let first = client.json(&b, &["log", "main"])["entries"][1]["id"]
        .as_str()
        .unwrap()
        .to_string();
    client.ok(&b, &["restore", "--source", &first, "c.txt"]);
    assert_eq!(fs::read_to_string(b.join("c.txt")).unwrap(), "c from A\n");
    client.ok(&b, &["restore", "c.txt"]);

    // Amend: fold an edit and a new message into an unlanded commit.
    write(&b, "f.txt", "f one\n");
    client.ok(&b, &["commit", "-m", "first words"]);
    let before = client.json(&b, &["status"])["base"]
        .as_str()
        .unwrap()
        .to_string();
    write(&b, "f.txt", "f two\n");
    client.ok(&b, &["amend", "-m", "better words"]);
    let st = client.json(&b, &["status"]);
    assert_ne!(st["base"], before.as_str());
    assert_eq!(st["ahead"], 1);
    assert!(client
        .ok(&b, &["status"])
        .contains("base is 1 commit ahead of main"));
    let log = client.json(&b, &["log", "@"]);
    assert_eq!(log["entries"][0]["changeset"]["message"], "better words");
    assert_eq!(client.ok(&b, &["cat", "@", "f.txt"]), "f two\n");
    // A landed base cannot be amended.
    client.ok(&b, &["land"]);
    write(&b, "f.txt", "f three\n");
    let out = client.run(&b, &["amend"]);
    assert_eq!(out.status.code(), Some(1));
    assert!(stderr(&out).contains("already landed"), "{}", stderr(&out));
    client.ok(&b, &["restore", "f.txt"]);

    // Reset: soft undoes a commit and keeps its changes; hard discards.
    write(&b, "g.txt", "g\n");
    client.ok(&b, &["commit", "-m", "to undo"]);
    let committed = client.json(&b, &["status"])["base"]
        .as_str()
        .unwrap()
        .to_string();
    client.ok(&b, &["reset"]);
    assert_eq!(fs::read_to_string(b.join("g.txt")).unwrap(), "g\n");
    let st = client.json(&b, &["status"]);
    assert_eq!(st["base"], main_head(&b).as_str());
    assert_eq!(st["changes"][0]["path"], "g.txt");
    assert_eq!(st["changes"][0]["status"], "added");
    // The undone commit is still on the server.
    assert!(client.ok(&b, &["show", &committed]).contains("to undo"));
    client.ok(&b, &["commit", "-m", "to undo"]);
    write(&b, "b.txt", "scrap\n");
    let out = client.run(&b, &["reset", "--hard", "main"]);
    assert_eq!(out.status.code(), Some(1));
    assert!(stderr(&out).contains("--force"), "{}", stderr(&out));
    assert_eq!(fs::read_to_string(b.join("b.txt")).unwrap(), "scrap\n");
    client.ok(&b, &["reset", "--hard", "--force", "main"]);
    assert_eq!(fs::read_to_string(b.join("b.txt")).unwrap(), "b from B\n");
    assert!(!b.join("g.txt").exists());
    assert_eq!(client.json(&b, &["status"])["base"], main_head(&b).as_str());
    // Reset never moves a ref.
    assert_eq!(main_head(&a), main_head(&b));

    // A binary conflict has no markers: it still blocks commit until resolved.
    fs::write(a.join("img.bin"), b"base\xff").unwrap();
    client.ok(&a, &["commit", "-m", "image"]);
    client.ok(&a, &["land"]);
    client.ok(&b, &["sync"]);
    fs::write(a.join("img.bin"), b"from A\xff").unwrap();
    client.ok(&a, &["commit", "-m", "A image"]);
    client.ok(&a, &["land"]);
    fs::write(b.join("img.bin"), b"from B\xff").unwrap();
    let out = client.run(&b, &["sync"]);
    assert_eq!(out.status.code(), Some(5), "{}", stderr(&out));
    assert!(stdout(&out).contains("no text merge"), "{}", stdout(&out));
    assert_eq!(fs::read(b.join("img.bin")).unwrap(), b"from B\xff");
    let st = client.json(&b, &["status"]);
    assert_eq!(st["changes"][0]["status"], "conflicted");
    let out = client.run(&b, &["commit", "-m", "x"]);
    assert_eq!(out.status.code(), Some(5));
    assert!(stderr(&out).contains("img.bin"), "{}", stderr(&out));
    client.ok(&b, &["resolve", "img.bin"]);
    client.ok(&b, &["commit", "-m", "keep B image"]);
    client.ok(&b, &["land"]);
}

#[test]
fn everyday_workflow_sync_stash_restore_amend_reset() {
    let _g = serial();
    let tmp = tempfile::tempdir().unwrap();
    let stores = Stores::sqlite(&tmp.path().join("data"));
    run_everyday_workflow(&stores, tmp.path());
}

#[test]
fn everyday_workflow_sync_stash_restore_amend_reset_postgres() {
    let _g = serial();
    let Some(db) = PgDb::create() else {
        return;
    };
    let tmp = tempfile::tempdir().unwrap();
    let stores = Stores::with_meta(&tmp.path().join("data"), db.url.clone());
    run_everyday_workflow(&stores, tmp.path());
}

fn run_reflog_and_undo_workflow(stores: &Stores, work: &Path) {
    let mut server = Server::start(stores, work);
    let port = server.addr.port();
    let client = Client::new(&server.url, &work.join("cache"));
    let wc = work.join("wc");
    client.ok(work, &["init", wc.to_str().unwrap()]);
    let head = |dir: &Path, name: &str| -> Option<String> {
        client.json(dir, &["refs"])["refs"][name]
            .as_str()
            .map(String::from)
    };
    let entries = |dir: &Path, args: &[&str]| -> Vec<serde_json::Value> {
        let mut full = vec!["reflog"];
        full.extend_from_slice(args);
        client.json(dir, &full)["entries"]
            .as_array()
            .unwrap()
            .clone()
    };

    write(&wc, "a.txt", "one\n");
    client.ok(&wc, &["commit", "-m", "one"]);
    client.ok(&wc, &["land"]);
    let one = head(&wc, "main").unwrap();
    write(&wc, "a.txt", "two\n");
    client.ok(&wc, &["commit", "-m", "two"]);
    client.ok(&wc, &["land"]);
    let two = head(&wc, "main").unwrap();
    assert_ne!(one, two);

    // Both lands are logged, newest first, with their operation and ids.
    let text = client.ok(&wc, &["reflog"]);
    let lines: Vec<_> = text.lines().collect();
    assert_eq!(lines.len(), 2, "{text}");
    assert!(
        lines[0].starts_with("2  ") && lines[0].contains("  land  main  "),
        "{text}"
    );
    assert!(
        lines[0].contains(&format!("{} -> {}", &one[..12], &two[..12])),
        "{text}"
    );
    assert!(lines[1].contains(&format!("- -> {}", &one[..12])), "{text}");
    let e = entries(&wc, &["main"]);
    assert_eq!(
        (e[0]["old"].as_str(), e[0]["new"].as_str()),
        (Some(one.as_str()), Some(two.as_str()))
    );
    assert_eq!(e[0]["operation"], "land");
    assert!(e[0]["actor"].is_null(), "auth is off");
    assert!(e[0]["time"].as_i64().unwrap() > 1_700_000_000);
    assert_eq!(
        client
            .ok(&wc, &["reflog", "main", "-n", "1"])
            .lines()
            .count(),
        1
    );
    assert_eq!(client.ok(&wc, &["reflog", "nothing"]), "");

    // Undo moves the ref back; the working copy stays where it was.
    let out = client.ok(&wc, &["undo"]);
    assert!(
        out.contains(&format!(
            "Moved main back: {} -> {}",
            &two[..12],
            &one[..12]
        )),
        "{out}"
    );
    assert!(out.contains("To redo: leep undo main --to 3"), "{out}");
    assert!(out.contains("leep sync"), "{out}");
    assert_eq!(head(&wc, "main").as_deref(), Some(one.as_str()));
    assert_eq!(fs::read_to_string(wc.join("a.txt")).unwrap(), "two\n");
    let e = entries(&wc, &["main"]);
    assert_eq!(e[0]["operation"], "undo");
    assert_eq!(e[0]["message"], "undo of 2");
    // A second undo redoes it, and `sync` follows the ref.
    client.ok(&wc, &["undo"]);
    assert_eq!(head(&wc, "main").as_deref(), Some(two.as_str()));
    client.ok(&wc, &["sync"]);

    // A deleted branch comes back; undoing again deletes it once more.
    client.ok(&wc, &["branch", "feature"]);
    client.ok(&wc, &["branch", "-D", "feature"]);
    assert_eq!(head(&wc, "feature"), None);
    let out = client.ok(&wc, &["undo", "feature"]);
    assert!(out.contains("Recreated feature"), "{out}");
    assert_eq!(head(&wc, "feature").as_deref(), Some(two.as_str()));
    let out = client.ok(&wc, &["undo", "feature"]);
    assert!(out.contains("Deleted feature"), "{out}");
    assert_eq!(head(&wc, "feature"), None);
    let out = client.run(&wc, &["undo", "never-existed"]);
    assert_eq!(out.status.code(), Some(1));
    assert!(stderr(&out).contains("no log entries"), "{}", stderr(&out));

    // The server owns the `land` label, so a client cannot forge one.
    let body = format!(r#"{{"expected":null,"new":"{one}","operation":"land"}}"#);
    assert_eq!(post_json(server.addr, "/v1/refs/forged", &body).0, 400);

    // Rewind main, then protect it. Undoing again would put unsigned history
    // back on a protected ref, which the server refuses like any other move.
    client.ok(&wc, &["undo"]);
    assert_eq!(head(&wc, "main").as_deref(), Some(one.as_str()));
    let before = entries(&wc, &[]);
    server.kill();
    let keyfile = work.join("alice.key");
    let out = client.ok(
        work,
        &[
            "keygen",
            "--out",
            keyfile.to_str().unwrap(),
            "--name",
            "alice",
        ],
    );
    let entry: serde_json::Value = serde_json::from_str(out.trim()).unwrap();
    let keys = work.join("keys.json");
    fs::write(&keys, serde_json::json!({ "keys": [entry] }).to_string()).unwrap();
    let server = Server::start_with(
        port,
        stores,
        work,
        &[
            "--signing-keys-file",
            keys.to_str().unwrap(),
            "--require-signatures",
            "main",
        ],
    );
    assert_eq!(entries(&wc, &[]), before, "the log survives a restart");
    let out = client.run(&wc, &["undo"]);
    assert_eq!(out.status.code(), Some(1), "{}", stderr(&out));
    assert!(
        stderr(&out).contains("is not signed by a trusted key"),
        "{}",
        stderr(&out)
    );
    assert_eq!(head(&wc, "main").as_deref(), Some(one.as_str()));
    // Neither can a protected ref be deleted by undoing its creation.
    let out = client.run(&wc, &["undo", "main", "--to", "1"]);
    assert_eq!(out.status.code(), Some(1));
    assert!(stderr(&out).contains("protected"), "{}", stderr(&out));
    assert_eq!(head(&wc, "main").as_deref(), Some(one.as_str()));
    assert_eq!(entries(&wc, &[]), before, "refused undos log nothing");

    // A protected ref only moves forward (security review 2026-10, S-09):
    // undoing a signed land would drop it without review, so undo refuses
    // and points at revert, which lands like any other change.
    client.ok(&wc, &["reset", "--hard", &one]);
    write(&wc, "a.txt", "signed\n");
    client.ok(
        &wc,
        &[
            "commit",
            "-m",
            "signed",
            "--sign",
            keyfile.to_str().unwrap(),
        ],
    );
    client.ok(&wc, &["land"]);
    let signed = head(&wc, "main").unwrap();
    assert_ne!(signed, one);
    let before = entries(&wc, &[]);
    let out = client.run(&wc, &["undo"]);
    assert_eq!(out.status.code(), Some(1), "{}", stderr(&out));
    assert!(
        stderr(&out).contains("only moves forward")
            && stderr(&out).contains(&format!("leep revert {signed}")),
        "{}",
        stderr(&out)
    );
    assert_eq!(head(&wc, "main").as_deref(), Some(signed.as_str()));
    assert_eq!(entries(&wc, &[]), before, "a refused rewind logs nothing");
    drop(server);
}

#[test]
fn reflog_and_undo() {
    let _g = serial();
    let tmp = tempfile::tempdir().unwrap();
    let stores = Stores::sqlite(&tmp.path().join("data"));
    run_reflog_and_undo_workflow(&stores, tmp.path());
}

#[test]
fn reflog_and_undo_postgres() {
    let _g = serial();
    let Some(db) = PgDb::create() else {
        return;
    };
    let tmp = tempfile::tempdir().unwrap();
    let stores = Stores::with_meta(&tmp.path().join("data"), db.url.clone());
    run_reflog_and_undo_workflow(&stores, tmp.path());
}

/// Twelve numbered lines, with some replaced. Edits far enough apart merge
/// as separate hunks.
fn numbered(edits: &[(usize, &str)]) -> String {
    (1..=12)
        .map(|n| match edits.iter().find(|(i, _)| *i == n) {
            Some((_, text)) => format!("{text}\n"),
            None => format!("line {n}\n"),
        })
        .collect()
}

fn run_merge_rebase_revert_workflow(stores: &Stores, work: &Path) {
    let server = Server::start(stores, work);
    let client = Client::new(&server.url, &work.join("cache"));
    let (a, b) = (work.join("a"), work.join("b"));
    let head = |name: &str| {
        client.json(&a, &["refs"])["refs"][name]
            .as_str()
            .unwrap()
            .to_string()
    };
    let base = |dir: &Path| {
        client.json(dir, &["status"])["base"]
            .as_str()
            .unwrap()
            .to_string()
    };
    // The newest `n` first-parent changesets of `rev`, as JSON.
    let log = |dir: &Path, rev: &str| client.json(dir, &["log", rev, "-n", "6"])["entries"].clone();
    let read = |dir: &Path, rel: &str| fs::read_to_string(dir.join(rel)).unwrap();
    let commit_land = |dir: &Path, msg: &str| {
        client.ok(dir, &["commit", "-m", msg]);
        client.ok(dir, &["land"]);
    };

    client.ok(work, &["init", a.to_str().unwrap()]);
    write(&a, "hunk.txt", &numbered(&[]));
    write(&a, "a.txt", "a\n");
    write(&a, "b.txt", "b\n");
    commit_land(&a, "start");
    client.ok(&a, &["branch", "feature"]);
    client.ok(work, &["init", b.to_str().unwrap()]);
    client.ok(&b, &["switch", "feature"]);

    // ---- merge: clean, as a local commit that lands ----
    write(&b, "feature.txt", "feature\n");
    write(&b, "hunk.txt", &numbered(&[(12, "line 12 feature")]));
    commit_land(&b, "feature work");
    write(&a, "a.txt", "a main\n");
    write(&a, "hunk.txt", &numbered(&[(1, "line 1 main")]));
    commit_land(&a, "main work");
    let main_before = head("main");
    let feature_head = head("feature");

    let out = client.ok(&a, &["merge", "feature"]);
    assert!(out.contains("Merged"), "{out}");
    assert_eq!(
        read(&a, "hunk.txt"),
        numbered(&[(1, "line 1 main"), (12, "line 12 feature")]),
        "both hunks"
    );
    assert_eq!(read(&a, "feature.txt"), "feature\n");
    assert_eq!(read(&a, "a.txt"), "a main\n");
    assert_eq!(head("main"), main_before, "merge moves no ref");
    assert!(client.ok(&a, &["status"]).contains("working copy clean"));
    let top = &log(&a, "@")[0]["changeset"];
    assert_eq!(
        top["parents"],
        serde_json::json!([main_before, feature_head])
    );
    assert_eq!(top["message"], "Merge feature into main");
    client.ok(&a, &["land"]);
    assert_eq!(head("main"), base(&a));
    let out = client.ok(&a, &["merge", "feature"]);
    assert!(out.contains("Already up to date"), "{out}");

    // ---- merge: a conflict, abort, then resolve and commit ----
    write(&b, "a.txt", "a feature\n");
    commit_land(&b, "feature edits a");
    let feature_head = head("feature");
    let out = client.run(&a, &["merge", "feature"]);
    assert_eq!(out.status.code(), Some(5), "{}", stderr(&out));
    assert!(stdout(&out).contains("C a.txt"), "{}", stdout(&out));
    let text = read(&a, "a.txt");
    assert!(text.contains("<<<<<<<") && text.contains("a main") && text.contains("a feature"));
    assert!(client.ok(&a, &["status"]).contains("merging"));
    assert_eq!(
        client.json(&a, &["status"])["merging"],
        feature_head.as_str()
    );
    let out = client.run(&a, &["commit", "-m", "too soon"]);
    assert_eq!(out.status.code(), Some(5));
    assert!(stderr(&out).contains("conflict"), "{}", stderr(&out));
    let out = client.run(&a, &["land"]);
    assert_eq!(out.status.code(), Some(5));
    assert!(stderr(&out).contains("in progress"), "{}", stderr(&out));
    client.ok(&a, &["merge", "--abort"]);
    assert_eq!(read(&a, "a.txt"), "a main\n");
    assert!(client.ok(&a, &["status"]).contains("working copy clean"));
    assert!(client.json(&a, &["status"])["merging"].is_null());

    let out = client.run(&a, &["merge", "feature"]);
    assert_eq!(out.status.code(), Some(5));
    write(&a, "a.txt", "a resolved\n");
    client.ok(&a, &["commit"]);
    let top = &log(&a, "@")[0]["changeset"];
    assert_eq!(top["message"], "Merge feature into main");
    assert_eq!(top["parents"].as_array().unwrap().len(), 2);
    assert_eq!(top["parents"][1], feature_head.as_str());
    client.ok(&a, &["land"]);
    assert_eq!(head("main"), base(&a));
    assert_eq!(client.ok(&a, &["cat", "main", "a.txt"]), "a resolved\n");

    // ---- cherry-pick one commit from a branch ----
    write(&b, "pick.txt", "picked\n");
    commit_land(&b, "pick me");
    write(&b, "skip.txt", "skipped\n");
    commit_land(&b, "not me");
    let pick_id = log(&b, "feature")[1]["id"].as_str().unwrap().to_string();
    let out = client.ok(&a, &["cherry-pick", &pick_id]);
    assert!(out.contains("Cherry-picked"), "{out}");
    assert_eq!(read(&a, "pick.txt"), "picked\n");
    assert!(!a.join("skip.txt").exists(), "only the picked commit");
    let top = &log(&a, "@")[0]["changeset"];
    assert_eq!(top["message"], "pick me");
    assert_eq!(top["extra"]["leep.rebased_from"], pick_id.as_str());
    client.ok(&a, &["land"]);
    assert_eq!(client.ok(&a, &["cat", "main", "pick.txt"]), "picked\n");

    // A conflicting pick: markers, then a resolution that commits with the
    // picked message.
    write(&b, "pick.txt", "picked twice\n");
    commit_land(&b, "edit pick");
    let edit_id = log(&b, "feature")[0]["id"].as_str().unwrap().to_string();
    write(&a, "pick.txt", "picked on main\n");
    commit_land(&a, "main edits pick");
    let out = client.run(&a, &["cherry-pick", &edit_id]);
    assert_eq!(out.status.code(), Some(5), "{}", stderr(&out));
    assert!(read(&a, "pick.txt").contains("<<<<<<<"));
    assert!(client.ok(&a, &["status"]).contains("cherry-picking"));
    client.ok(&a, &["cherry-pick", "--abort"]);
    assert_eq!(read(&a, "pick.txt"), "picked on main\n");
    client.run(&a, &["cherry-pick", &edit_id]);
    write(&a, "pick.txt", "picked both\n");
    client.ok(&a, &["commit"]);
    assert_eq!(log(&a, "@")[0]["changeset"]["message"], "edit pick");
    client.ok(&a, &["land"]);

    // ---- revert a landed commit, and land the revert ----
    write(&a, "r.txt", "r\n");
    commit_land(&a, "add r");
    let added = base(&a);
    let out = client.ok(&a, &["revert", "@"]);
    assert!(out.contains("Reverted"), "{out}");
    assert!(!a.join("r.txt").exists());
    let top = &log(&a, "@")[0]["changeset"];
    assert_eq!(
        top["message"],
        format!("Revert \"add r\"\n\nThis reverts {added}.")
    );
    assert_eq!(top["extra"]["leep.revert_of"], added.as_str());
    client.ok(&a, &["land"]);
    assert_eq!(
        client.run(&a, &["cat", "main", "r.txt"]).status.code(),
        Some(4)
    );
    let reverts = client.json(&a, &["log", "main", "--extra", "leep.revert_of"]);
    assert_eq!(reverts["entries"].as_array().unwrap().len(), 1);

    // ---- a revert that needs a real three-way merge ----
    write(&a, "hunk2.txt", &numbered(&[]));
    commit_land(&a, "add hunk2");
    write(&a, "hunk2.txt", &numbered(&[(2, "line 2 CHANGED")]));
    commit_land(&a, "change line 2");
    let change2 = base(&a);
    write(
        &a,
        "hunk2.txt",
        &numbered(&[(2, "line 2 CHANGED"), (11, "line 11 LATER")]),
    );
    commit_land(&a, "change line 11");
    client.ok(&a, &["revert", &change2]);
    assert_eq!(
        read(&a, "hunk2.txt"),
        numbered(&[(11, "line 11 LATER")]),
        "only the reverted hunk went back"
    );
    client.ok(&a, &["land"]);
    assert_eq!(
        client.ok(&a, &["cat", "main", "hunk2.txt"]),
        numbered(&[(11, "line 11 LATER")])
    );

    // The same revert conflicts when the later edit touches the same lines.
    write(&a, "hunk2.txt", &numbered(&[(5, "line 5 A")]));
    commit_land(&a, "change line 5");
    let change5 = base(&a);
    write(&a, "hunk2.txt", &numbered(&[(5, "line 5 B")]));
    commit_land(&a, "change line 5 again");
    let out = client.run(&a, &["revert", &change5]);
    assert_eq!(out.status.code(), Some(5), "{}", stderr(&out));
    assert!(stdout(&out).contains("C hunk2.txt"), "{}", stdout(&out));
    assert!(read(&a, "hunk2.txt").contains("<<<<<<<"));
    client.ok(&a, &["revert", "--abort"]);
    assert_eq!(read(&a, "hunk2.txt"), numbered(&[(5, "line 5 B")]));
    assert!(client.ok(&a, &["status"]).contains("working copy clean"));

    // ---- a dirty working copy refuses all four ----
    write(&a, "scratch.txt", "x\n");
    for args in [
        ["merge", "feature"],
        ["rebase", "main"],
        ["cherry-pick", "feature"],
        ["revert", "@"],
    ] {
        let out = client.run(&a, &args);
        assert_eq!(out.status.code(), Some(1), "{args:?}");
        assert!(
            stderr(&out).contains("uncommitted"),
            "{args:?}: {}",
            stderr(&out)
        );
    }
    client.ok(&a, &["restore", "--force", "scratch.txt"]);

    // ---- rebase a two-commit stack onto an updated branch ----
    client.ok(&b, &["switch", "main"]);
    write(&b, "e.txt", "e\n");
    commit_land(&b, "main moves on");
    write(&a, "x1.txt", "x1\n");
    client.ok(&a, &["commit", "-m", "L1"]);
    write(&a, "x2.txt", "x2\n");
    client.ok(&a, &["commit", "-m", "L2"]);
    let old = base(&a);
    let out = client.ok(&a, &["rebase"]);
    assert!(out.contains("Rebased 2 local commits"), "{out}");
    assert_ne!(base(&a), old);
    assert_eq!(read(&a, "e.txt"), "e\n");
    assert_eq!(read(&a, "x1.txt"), "x1\n");
    assert_eq!(read(&a, "x2.txt"), "x2\n");
    assert_eq!(client.json(&a, &["status"])["ahead"], 2);
    assert_eq!(head("main"), log(&b, "main")[0]["id"].as_str().unwrap());
    client.ok(&a, &["land"]);
    assert_eq!(head("main"), base(&a));
    let msgs: Vec<String> = log(&a, "main")
        .as_array()
        .unwrap()
        .iter()
        .take(3)
        .map(|e| e["changeset"]["message"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(msgs, ["L2", "L1", "main moves on"]);
}

#[test]
fn merge_rebase_cherry_pick_and_revert() {
    let _g = serial();
    let tmp = tempfile::tempdir().unwrap();
    let stores = Stores::sqlite(&tmp.path().join("data"));
    run_merge_rebase_revert_workflow(&stores, tmp.path());
}

#[test]
fn merge_rebase_cherry_pick_and_revert_postgres() {
    let _g = serial();
    let Some(db) = PgDb::create() else {
        return;
    };
    let tmp = tempfile::tempdir().unwrap();
    let stores = Stores::with_meta(&tmp.path().join("data"), db.url.clone());
    run_merge_rebase_revert_workflow(&stores, tmp.path());
}

/// Tokens-file entry from `gen-token`, with extra limit fields merged in.
fn limited_token(name: &str, role: &str, limits: serde_json::Value) -> (String, serde_json::Value) {
    let (token, mut entry) = gen_token(name, role);
    entry
        .as_object_mut()
        .unwrap()
        .extend(limits.as_object().unwrap().clone());
    (token, entry)
}

#[test]
fn request_limits_throttle_one_token_and_the_cli_reports_it() {
    let _g = serial();
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();

    let (hammer_t, hammer) = limited_token(
        "hammer",
        "write",
        serde_json::json!({ "rate": 1, "burst": 5 }),
    );
    let (calm_t, calm) = limited_token("calm", "write", serde_json::json!({}));
    let (patient_t, patient) = limited_token(
        "patient",
        "read",
        serde_json::json!({ "rate": 20, "burst": 1 }),
    );
    let (stingy_t, stingy) = limited_token(
        "stingy",
        "read",
        serde_json::json!({ "rate": 0.01, "burst": 1 }),
    );
    let tokens = root.join("tokens.json");
    fs::write(
        &tokens,
        serde_json::json!({ "tokens": [hammer, calm, patient, stingy] }).to_string(),
    )
    .unwrap();

    let metrics_port = free_port();
    let metrics_listen = format!("127.0.0.1:{metrics_port}");
    let stores = Stores::sqlite(&root.join("data"));
    let server = Server::start_with(
        free_port(),
        &stores,
        root,
        &[
            "--tokens-file",
            tokens.to_str().unwrap(),
            "--metrics-listen",
            &metrics_listen,
        ],
    );

    // One token hammers the server: the burst gets through, then 429s that
    // say when to come back, in the usual JSON error shape.
    let hammer_replies: Vec<_> = (0..40)
        .map(|_| http_get(server.addr, "/v1/refs", Some(&hammer_t)))
        .collect();
    let limited: Vec<_> = hammer_replies.iter().filter(|r| r.0 == 429).collect();
    assert!(
        hammer_replies[..5].iter().all(|r| r.0 == 200),
        "{hammer_replies:?}"
    );
    assert!(limited.len() >= 20, "only {} of 40 limited", limited.len());
    for (_, head, body) in &limited {
        assert!(head.contains("retry-after: 1"), "{head}");
        let v: serde_json::Value = serde_json::from_str(body).unwrap();
        assert!(
            v["error"].as_str().unwrap().contains("rate limit"),
            "{body}"
        );
    }
    // Meanwhile another token sees nothing, and neither does the health probe.
    for _ in 0..40 {
        assert_eq!(http_get(server.addr, "/v1/refs", Some(&calm_t)).0, 200);
        assert_eq!(http_get(server.addr, "/v1/health", None).0, 200);
    }

    // The limits are visible in metrics, by token name and never the secret.
    let (status, _, metrics) = http_get(metrics_listen.parse().unwrap(), "/metrics", None);
    assert_eq!(status, 200);
    assert!(
        metrics
            .lines()
            .any(|l| l.starts_with("leep_requests_limited_total")
                && l.contains("reason=\"rate\"")
                && l.contains("token=\"hammer\"")),
        "{metrics}"
    );
    assert!(!metrics.contains("token=\"calm\""), "{metrics}");
    assert!(!metrics.contains(&hammer_t));

    // The CLI waits out a short Retry-After on reads...
    let client = Client::new(&server.url, &root.join("cache"));
    let calm_user = User {
        client: &client,
        token: calm_t,
    };
    let wc = root.join("wc");
    calm_user.ok(root, &["init", wc.to_str().unwrap()]);
    write(&wc, "a.txt", "a\n");
    calm_user.ok(&wc, &["commit", "-m", "seed"]);
    calm_user.ok(&wc, &["land"]);
    let patient_user = User {
        client: &client,
        token: patient_t,
    };
    for _ in 0..3 {
        let out = patient_user.run(&wc, &["log"]);
        assert_ok(&out, "log through a rate limit");
    }

    // ...and reports a long one instead of hanging.
    let stingy_user = User {
        client: &client,
        token: stingy_t,
    };
    stingy_user.ok(&wc, &["log"]);
    let started = std::time::Instant::now();
    let out = stingy_user.run(&wc, &["log"]);
    assert_eq!(out.status.code(), Some(3), "{}", stderr(&out));
    let err = stderr(&out);
    assert!(
        err.contains("rate limit exceeded") && err.contains("retry after 100s"),
        "{err}"
    );
    assert!(started.elapsed() < std::time::Duration::from_secs(5));
}

#[test]
fn author_falls_back_to_the_token_name_when_nothing_local_names_one() {
    let _g = serial();
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    let (carol_t, entry) = gen_token("carol", "write");
    let (dave_t, mut dave) = gen_token("dave", "write");
    dave["author"] = "Dave Example <dave@example.com>".into();
    let tokens = root.join("tokens.json");
    fs::write(
        &tokens,
        serde_json::json!({ "tokens": [entry, dave] }).to_string(),
    )
    .unwrap();
    let stores = Stores::sqlite(&root.join("data"));
    let server = Server::start_with(
        free_port(),
        &stores,
        root,
        &["--tokens-file", tokens.to_str().unwrap()],
    );
    let client = Client::new(&server.url, &root.join("cache"));
    // Like a container or CI job: no login name at all.
    let token = std::cell::RefCell::new(carol_t);
    let leep = |cwd: &Path, author: Option<&str>, args: &[&str]| {
        let mut c = client.cmd(cwd);
        c.env("LEEP_TOKEN", &*token.borrow())
            .env_remove("LEEP_AUTHOR")
            .env_remove("USER")
            .env_remove("LOGNAME")
            .env_remove("USERNAME");
        if let Some(a) = author {
            c.env("LEEP_AUTHOR", a);
        }
        let out = c.args(args).output().unwrap();
        assert_ok(&out, &format!("leep {args:?}"));
        stdout(&out)
    };
    let author_of_tip = |wc: &Path| -> String {
        let out = leep(wc, None, &["--json", "log", "-n", "1"]);
        let v: serde_json::Value = serde_json::from_str(&out).unwrap();
        v["entries"][0]["changeset"]["author"]
            .as_str()
            .unwrap()
            .to_string()
    };

    let wc = root.join("wc");
    leep(root, None, &["init", wc.to_str().unwrap()]);
    write(&wc, "a.txt", "one\n");
    leep(&wc, None, &["commit", "-m", "first"]);
    assert_eq!(author_of_tip(&wc), "carol");

    // A locally named author still wins over the token.
    write(&wc, "a.txt", "two\n");
    leep(&wc, Some("Carol Example"), &["commit", "-m", "second"]);
    assert_eq!(author_of_tip(&wc), "Carol Example");

    // A token whose entry sets `author` stamps that, not its bare name.
    *token.borrow_mut() = dave_t;
    write(&wc, "a.txt", "three\n");
    leep(&wc, None, &["commit", "-m", "third"]);
    assert_eq!(author_of_tip(&wc), "Dave Example <dave@example.com>");
}

/// A server whose `--advertise-url` does not reach itself refuses to start
/// with land coordination on, instead of stranding every land forwarded to
/// it (ADR 0056); one whose address is outside the peer allow-list too.
#[test]
fn land_coordination_checks_the_advertise_url_at_startup() {
    let tmp = tempfile::tempdir().unwrap();
    let stores = Stores::sqlite(tmp.path());
    let port = free_port();
    let elsewhere = free_port();
    let run = |advertise: String, prefix: &str| {
        let out = std::process::Command::new(bin("leep-server"))
            .args(["--listen", &format!("127.0.0.1:{port}")])
            .args(["--meta", &stores.meta, "--blobs", &stores.blobs])
            .args(["--land-coordination", "on", "--advertise-url", &advertise])
            .args(["--peer-url-prefix", prefix])
            .env("RUST_LOG", "warn")
            .output()
            .expect("run leep-server");
        assert!(!out.status.success(), "started with {advertise}");
        format!("{}{}", stdout(&out), stderr(&out))
    };
    let log = run(format!("http://127.0.0.1:{elsewhere}"), "http://127.0.0.1:");
    assert!(log.contains("does not reach this server"), "{log}");
    let log = run(format!("http://127.0.0.1:{port}"), "https://leep-");
    assert!(log.contains("not allowed"), "{log}");
}
