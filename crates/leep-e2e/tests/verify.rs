//! `leep-server verify` and `repair` against stores written by the real
//! server (ADR 0025): a populated store verifies clean; damage of every kind
//! is reported with exit status 1, naming what and where; `repair rederive`
//! fixes derived damage, and `verify` is clean again.

mod common;

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use common::*;
use leep_core::Hash;
use serde_json::Value;

/// Run `leep-server verify|repair ...` against `stores`.
fn run(stores: &Stores, args: &[&str]) -> Output {
    Command::new(bin("leep-server"))
        .args(["--meta", &stores.meta, "--blobs", &stores.blobs])
        .args(args)
        .env("RUST_LOG", "warn")
        .output()
        .expect("spawn leep-server")
}

fn code(out: &Output) -> i32 {
    out.status.code().expect("exit code")
}

/// Run, expect `want` as the exit code, and parse the JSON report.
fn json(stores: &Stores, args: &[&str], want: i32) -> Value {
    let mut full = args.to_vec();
    full.push("--json");
    let out = run(stores, &full);
    assert_eq!(
        code(&out),
        want,
        "leep-server {args:?}\n--- stdout\n{}\n--- stderr\n{}",
        stdout(&out),
        stderr(&out)
    );
    serde_json::from_str(&stdout(&out))
        .unwrap_or_else(|e| panic!("bad JSON from {args:?}: {e}\n{}", stdout(&out)))
}

fn problems(v: &Value) -> Vec<&Value> {
    v["problems"].as_array().unwrap().iter().collect()
}

fn with_code<'a>(v: &'a Value, code: &str) -> Vec<&'a Value> {
    problems(v)
        .into_iter()
        .filter(|p| p["code"] == code)
        .collect()
}

fn blob_path(stores: &Stores, h: &Hash) -> PathBuf {
    let dir = stores.blobs.strip_prefix("file://").unwrap();
    let hex = h.to_hex();
    Path::new(dir).join(&hex[..2]).join(&hex[2..])
}

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

/// Overwrite one derived row, as a damaged disk would.
fn put_row(stores: &Stores, kind: &str, key: &Hash, value: Vec<u8>) {
    rt().block_on(async {
        let meta = leep_meta::open_meta_store(&stores.meta).await.unwrap();
        meta.put_derived(kind, key, value.into()).await.unwrap();
    });
}

fn get_row(stores: &Stores, kind: &str, key: &Hash) -> Vec<u8> {
    rt().block_on(async {
        let meta = leep_meta::open_meta_store(&stores.meta).await.unwrap();
        meta.get_derived(kind, key).await.unwrap().unwrap().to_vec()
    })
}

/// Run SQL the way a failing disk or a bad restore might have changed a table.
fn sql(stores: &Stores, statement: &str) {
    use sqlx::Connection;
    rt().block_on(async {
        if stores.meta.starts_with("sqlite:") {
            let mut c = sqlx::SqliteConnection::connect(&stores.meta).await.unwrap();
            sqlx::query(statement).execute(&mut c).await.unwrap();
        } else {
            let mut c = sqlx::PgConnection::connect(&stores.meta).await.unwrap();
            sqlx::query(statement).execute(&mut c).await.unwrap();
        }
    });
}

struct Populated {
    /// Changeset ids, oldest first.
    ids: Vec<String>,
    tip_tree: Hash,
}

const ONE: &str = "pub fn one() { println!(\"needle\"); }\n";

/// A history written through the server: three landed changesets by one
/// working copy, one more by a stale one (so the server rebases it), then
/// every kind of derived data a user query creates.
fn populate(stores: &Stores, root: &Path) -> Populated {
    let server = Server::start(stores, root);
    let client = Client::new(&server.url, &root.join("cache"));
    let a = root.join("wc-a");
    let b = root.join("wc-b");
    client.ok(root, &["init", a.to_str().unwrap()]);
    write(&a, "alpha/one.rs", ONE);
    write(&a, "alpha/deep/two.txt", "two\nlines\n");
    write(&a, "README.md", "# demo\n");
    write(&a, "tool.bin", "\0\u{1}binary");
    client.ok(&a, &["commit", "-m", "seed"]);
    client.ok(&a, &["land"]);
    client.ok(root, &["init", b.to_str().unwrap()]);
    write(&a, "alpha/one.rs", &format!("{ONE}// more\n"));
    write(&a, "beta/three.txt", "three\n");
    client.ok(&a, &["commit", "-m", "second"]);
    client.ok(&a, &["land"]);
    fs::remove_file(a.join("README.md")).unwrap();
    client.ok(&a, &["commit", "-m", "drop readme"]);
    client.ok(&a, &["land"]);
    // `b` is based on the first landed changeset, so landing it rebases it.
    write(&b, "gamma/four.txt", "four\n");
    client.ok(&b, &["commit", "-m", "stale work"]);
    client.ok(&b, &["land"]);

    client.ok(&a, &["sync"]);
    client.ok(&a, &["search", "needle"]);
    client.ok(&a, &["blame", "alpha/one.rs"]);
    client.ok(&a, &["log", "main", "--path", "alpha"]);
    client.ok(&a, &["last-change", "alpha/deep/two.txt", "--rev", "main"]);
    let log = client.json(&a, &["log", "main", "-n", "50"]);
    let entries = log["entries"].as_array().unwrap();
    let mut ids: Vec<String> = entries
        .iter()
        .map(|e| e["id"].as_str().unwrap().to_string())
        .collect();
    ids.reverse();
    let tip_tree = entries[0]["root_tree"].as_str().unwrap().parse().unwrap();
    drop(server);
    Populated { ids, tip_tree }
}

fn run_verify_workflow(stores: &Stores, work: &Path) {
    let p = populate(stores, work);
    assert!(p.ids.len() >= 4, "{:?}", p.ids);
    let tip: Hash = p.ids.last().unwrap().parse().unwrap();
    let first = &p.ids[0];

    // A healthy store: every mode is clean, and says what it looked at.
    let out = run(stores, &["verify"]);
    assert_eq!(code(&out), 0, "{}\n{}", stdout(&out), stderr(&out));
    assert!(stdout(&out).contains("result: clean"), "{}", stdout(&out));
    let v = json(stores, &["verify", "--full", "--rehash"], 0);
    assert_eq!(v["ok"], true);
    assert_eq!(v["mode"], "full");
    assert!(v["stats"]["changesets_reachable"].as_u64().unwrap() >= 4);
    assert!(v["stats"]["blobs_rehashed"].as_u64().unwrap() >= 6);
    for kind in [
        "generation",
        "skiplist",
        "root_tree",
        "history-tree-v1",
        "blob-trigrams-v1",
        "tree-trigrams-v1",
        "blame",
    ] {
        assert!(
            v["derived"][kind]["checked"].as_u64().unwrap() > 0,
            "{kind}: {}",
            v["derived"][kind]
        );
    }
    let v = json(stores, &["verify", "--derived", "none", "--ref", "main"], 0);
    assert!(v["derived"].as_object().unwrap().is_empty());

    // Could not run: not damage.
    for args in [
        vec!["verify", "--derived", "no-such-kind"],
        vec!["verify", "--ref", "no-such-ref"],
        vec!["repair", "drop", "--kind", "signature-v1"],
        vec!["repair", "drop", "--kind", "no-such-kind"],
        vec!["repair", "rederive", "--kind", "blame"],
    ] {
        let out = run(stores, &args);
        assert_eq!(code(&out), 2, "{args:?}: {}", stderr(&out));
        assert!(stdout(&out).is_empty() || !stdout(&out).contains("result:"));
    }
    let refused = stderr(&run(stores, &["repair", "drop", "--kind", "review-v1"]));
    assert!(refused.contains("refusing to drop review-v1"), "{refused}");
    if stores.meta.starts_with("sqlite:") {
        let ghost = Stores {
            meta: "sqlite:///nonexistent/dir/meta.db?mode=rwc".into(),
            blobs: stores.blobs.clone(),
        };
        let out = run(&ghost, &["verify"]);
        assert_eq!(code(&out), 2);
        assert!(stderr(&out).contains("does not exist"), "{}", stderr(&out));
    }

    // A blob lost from the blob store: named with its changeset and path.
    let one = Hash::of_blob(ONE.as_bytes());
    let path = blob_path(stores, &one);
    let saved = fs::read(&path).unwrap();
    fs::remove_file(&path).unwrap();
    let v = json(stores, &["verify"], 1);
    let missing = with_code(&v, "missing_blob");
    assert_eq!(missing.len(), 1, "{v:#}");
    assert_eq!(missing[0]["key"], one.to_hex());
    assert_eq!(missing[0]["path"], "alpha/one.rs");
    assert_eq!(missing[0]["changeset"], first.as_str());
    let text = stdout(&run(stores, &["verify"]));
    assert!(
        text.contains("[missing_blob]") && text.contains("alpha/one.rs"),
        "{text}"
    );
    fs::write(&path, &saved).unwrap();
    json(stores, &["verify"], 0);

    // Bytes that no longer match their address are seen only with --rehash.
    let mut bad = saved.clone();
    bad[0] ^= 1;
    fs::write(&path, &bad).unwrap();
    json(stores, &["verify"], 0);
    let v = json(stores, &["verify", "--rehash"], 1);
    assert_eq!(with_code(&v, "corrupt_blob").len(), 1, "{v:#}");
    fs::write(&path, &saved).unwrap();

    // A flipped byte in a tree node.
    let tree_path = blob_path(stores, &p.tip_tree);
    let saved_tree = fs::read(&tree_path).unwrap();
    let mut bad = saved_tree.clone();
    bad[1] ^= 0x20;
    fs::write(&tree_path, &bad).unwrap();
    let v = json(stores, &["verify"], 1);
    let corrupt = with_code(&v, "corrupt_tree");
    assert!(!corrupt.is_empty(), "{v:#}");
    assert_eq!(corrupt[0]["key"], p.tip_tree.to_hex());
    fs::write(&tree_path, &saved_tree).unwrap();
    json(stores, &["verify"], 0);

    // Derived rows: damaged generation, skiplist and trigram rows.
    let good_gen = get_row(stores, "generation", &tip);
    let mut bad = good_gen.clone();
    bad[7] ^= 1;
    put_row(stores, "generation", &tip, bad);
    let v = json(stores, &["verify"], 1);
    let mism = with_code(&v, "derived_mismatch");
    assert_eq!(mism.len(), 1, "{v:#}");
    assert_eq!(mism[0]["kind"], "generation");
    assert_eq!(mism[0]["key"], tip.to_hex());
    assert_eq!(mism[0]["changeset"], tip.to_hex());

    let good_skip = get_row(stores, "skiplist", &tip);
    let mut bad = good_skip.clone();
    bad[3] ^= 0x80;
    put_row(stores, "skiplist", &tip, bad);
    // The search index covers the tip's files, so the newest version of the file.
    let blob_row_key = Hash::of_blob(format!("{ONE}// more\n").as_bytes());
    let good_blob_row = get_row(stores, "blob-trigrams-v1", &blob_row_key);
    put_row(stores, "blob-trigrams-v1", &blob_row_key, vec![0, 2]);
    let v = json(stores, &["verify", "--full"], 1);
    let mut kinds: Vec<&str> = with_code(&v, "derived_mismatch")
        .iter()
        .map(|p| p["kind"].as_str().unwrap())
        .collect();
    kinds.sort();
    assert_eq!(
        kinds,
        ["blob-trigrams-v1", "generation", "skiplist"],
        "{v:#}"
    );

    // A dry run says what is wrong and writes nothing; the repair fixes it.
    let dry = json(
        stores,
        &[
            "repair",
            "rederive",
            "--dry-run",
            "--kind",
            "skiplist",
            "blob-trigrams-v1",
        ],
        1,
    );
    assert_eq!(dry["dry_run"], true);
    assert_eq!(get_row(stores, "generation", &tip).len(), 8);
    json(stores, &["verify", "--full"], 1);
    let fixed = json(
        stores,
        &[
            "repair",
            "rederive",
            "--kind",
            "skiplist",
            "blob-trigrams-v1",
        ],
        0,
    );
    assert_eq!(
        fixed["kind_reports"]["skiplist"]["overwritten"], 1,
        "{fixed:#}"
    );
    assert_eq!(fixed["kind_reports"]["blob-trigrams-v1"]["overwritten"], 1);
    // The generation was repaired as skiplist's dependency.
    assert_eq!(get_row(stores, "generation", &tip), good_gen);
    assert_eq!(get_row(stores, "skiplist", &tip), good_skip);
    assert_eq!(
        get_row(stores, "blob-trigrams-v1", &blob_row_key),
        good_blob_row
    );
    json(stores, &["verify", "--full", "--rehash"], 0);
    // Nothing is wrong now. (Rows no query ever asked for may be filled in: for
    // the lazily derived kinds that is eager derivation, not repair.)
    let again = json(stores, &["repair", "rederive", "--kind", "all"], 0);
    assert!(
        again["kind_reports"]
            .as_object()
            .unwrap()
            .values()
            .all(|k| k["overwritten"] == 0 && k["failed"] == 0),
        "{again:#}"
    );

    // Dropping a derived kind: refused kinds were tried above; a droppable
    // one goes, the store stays clean, and queries bring the rows back.
    let dropped = json(
        stores,
        &["repair", "drop", "--kind", "blame", "tree-trigrams-v1"],
        0,
    );
    assert!(
        dropped["rows"]["blame"].as_u64().unwrap() >= 1,
        "{dropped:#}"
    );
    json(stores, &["verify", "--full"], 0);
    let server = Server::start(stores, work);
    let client = Client::new(&server.url, &work.join("cache"));
    let wc = work.join("wc-a");
    assert!(client
        .ok(&wc, &["search", "needle"])
        .contains("alpha/one.rs"));
    client.ok(&wc, &["blame", "alpha/one.rs"]);
    drop(server);
    json(stores, &["verify", "--full", "--rehash"], 0);

    // A ref that moved without a log entry: nothing to rederive, so repair
    // cannot help, but verify names the ref.
    sql(
        stores,
        "DELETE FROM ref_log WHERE seq = (SELECT max(seq) FROM ref_log WHERE name = 'main')",
    );
    let v = json(stores, &["verify"], 1);
    let div = with_code(&v, "reflog_divergence");
    assert_eq!(div.len(), 1, "{v:#}");
    assert_eq!(div[0]["name"], "main");
}

#[test]
fn verify_and_repair_over_a_populated_sqlite_store() {
    let _g = serial();
    let tmp = tempfile::tempdir().unwrap();
    let stores = Stores::sqlite(&tmp.path().join("data"));
    run_verify_workflow(&stores, tmp.path());
}

#[test]
fn verify_and_repair_over_a_populated_postgres_store() {
    let _g = serial();
    let Some(db) = PgDb::create() else {
        return;
    };
    let tmp = tempfile::tempdir().unwrap();
    let stores = Stores::with_meta(&tmp.path().join("data"), db.url.clone());
    run_verify_workflow(&stores, tmp.path());
}

/// A changeset row damaged in place is damage, not "could not run".
#[test]
fn a_damaged_changeset_row_is_reported_not_fatal() {
    let _g = serial();
    let tmp = tempfile::tempdir().unwrap();
    let stores = Stores::sqlite(&tmp.path().join("data"));
    let p = populate(&stores, tmp.path());
    let victim = &p.ids[1];
    let raw: Vec<u8> = (0..victim.len() / 2)
        .map(|i| u8::from_str_radix(&victim[2 * i..2 * i + 2], 16).unwrap())
        .collect();
    rt().block_on(async {
        use sqlx::Connection;
        let mut c = sqlx::SqliteConnection::connect(&stores.meta).await.unwrap();
        // Another changeset's bytes under this id: it decodes but hashes wrong.
        sqlx::query("UPDATE changesets SET data = (SELECT data FROM changesets WHERE id != ? LIMIT 1) WHERE id = ?")
            .bind(raw.clone())
            .bind(raw)
            .execute(&mut c)
            .await
            .unwrap();
    });
    let v = json(&stores, &["verify"], 1);
    let bad = with_code(&v, "corrupt_changeset");
    assert_eq!(bad.len(), 1, "{v:#}");
    assert_eq!(bad[0]["key"], victim.as_str());
    // Everything above it is still checked, with rows that need the lost
    // changeset skipped rather than reported wrong.
    assert!(v["stats"]["changesets_reachable"].as_u64().unwrap() >= 2);
}

/// Not part of the normal run: builds a store of about 10 000 changesets on
/// SQLite and a filesystem blob store, warms every derived kind, and times
/// `verify` in each mode. The numbers go in docs/PERFORMANCE.md.
///
/// ```sh
/// cargo build --release --workspace
/// cargo test --release -p leep-e2e --test verify -- --ignored --nocapture
/// ```
/// `LEEP_VERIFY_PERF_CHANGESETS` sets the size.
#[test]
#[ignore = "builds 10 000 changesets; run it in release mode"]
fn verify_ten_thousand_changesets() {
    use std::collections::BTreeMap;
    use std::time::Instant;

    use leep_core::api::{Authorship, MergeRequest, MergeResponse};
    use leep_core::{Changeset, ChangesetId, FileChange, FileKind, RepoPath};

    let _g = serial();
    let target: usize = std::env::var("LEEP_VERIFY_PERF_CHANGESETS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10_000);
    // `LEEP_VERIFY_PERF_DIR` keeps the store, to run `verify` under `time -v`.
    let kept = std::env::var_os("LEEP_VERIFY_PERF_DIR").map(std::path::PathBuf::from);
    let tmp = tempfile::tempdir().unwrap();
    let dir = kept.unwrap_or_else(|| tmp.path().to_path_buf());
    let stores = Stores::sqlite(&dir.join("data"));
    let t0 = Instant::now();
    let total = rt().block_on(async {
        let blobs = leep_store::open_blob_store(&stores.blobs).unwrap();
        let meta = leep_meta::open_meta_store(&stores.meta).await.unwrap();
        let repo = leep_derive::Repo::new(blobs, meta);
        let commit = |parent: Option<ChangesetId>, tag: usize, paths: Vec<String>| {
            let repo = repo.clone();
            async move {
                let mut changes = BTreeMap::new();
                for (j, path) in paths.into_iter().enumerate() {
                    let body = format!(
                        "// revision {tag} file {j}\nfn f{tag}() {{ println!(\"{tag}\"); }}\n{}\n",
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
                repo.create_changeset(Changeset {
                    parents: parent.into_iter().collect(),
                    author: "perf".into(),
                    timestamp: 1_700_000_000 + tag as i64,
                    message: format!("change {tag}"),
                    changes,
                    extra: BTreeMap::new(),
                })
                .await
                .unwrap()
            }
        };
        let mut tip = None;
        let mut count = 0;
        let mut i = 0;
        while count < target {
            i += 1;
            if i % 200 == 0 {
                // A side branch on each side of a merge.
                let a = commit(tip, i, vec![format!("side/a{i}/x.rs")]).await;
                let b = commit(tip, i, vec![format!("side/b{i}/x.rs")]).await;
                let m = match repo
                    .merge(MergeRequest {
                        parents: [a, b],
                        message: format!("merge {i}"),
                        authorship: Authorship {
                            author: "perf".into(),
                            timestamp: Some(1_700_000_000),
                        },
                        extra: BTreeMap::new(),
                    })
                    .await
                    .unwrap()
                {
                    MergeResponse::Merged { id } => id,
                    other => panic!("{other:?}"),
                };
                tip = Some(m);
                count += 3;
            } else {
                let paths = vec![
                    format!("pkg{}/mod{}/file{}.rs", i % 40, (i * 3) % 25, i % 60),
                    format!(
                        "pkg{}/mod{}/file{}.rs",
                        (i + 7) % 40,
                        (i * 5) % 25,
                        (i + 1) % 60
                    ),
                ];
                tip = Some(commit(tip, i, paths).await);
                count += 1;
            }
        }
        let tip = tip.unwrap();
        repo.meta().cas_ref("main", None, Some(tip)).await.unwrap();
        eprintln!(
            "built {count} changesets in {:.1}s",
            t0.elapsed().as_secs_f64()
        );
        // The derived kinds a busy server would have: path history, last
        // merge, search rows, blame.
        let t = Instant::now();
        repo.log(tip, Some("pkg1"), 5).await.unwrap();
        repo.merge_base(tip, tip).await.unwrap();
        repo.derive_search(tip).await.unwrap();
        for p in ["pkg1/mod3/file1.rs", "pkg2/mod6/file2.rs"] {
            let _ = repo.blame(tip, &RepoPath::new(p).unwrap()).await;
        }
        eprintln!("warmed derived data in {:.1}s", t.elapsed().as_secs_f64());
        count
    });

    for (name, args) in [
        ("default (sample)", vec!["verify"]),
        ("--full", vec!["verify", "--full"]),
        ("--full --rehash", vec!["verify", "--full", "--rehash"]),
    ] {
        let t = Instant::now();
        let out = run(&stores, &args);
        let secs = t.elapsed().as_secs_f64();
        assert_eq!(code(&out), 0, "{name}\n{}\n{}", stdout(&out), stderr(&out));
        eprintln!("verify {name}: {secs:.1}s for {total} changesets");
        assert!(secs < 1800.0, "{name} took {secs:.0}s");
    }
}
