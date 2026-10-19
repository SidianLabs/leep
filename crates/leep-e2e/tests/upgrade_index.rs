//! `leep-server upgrade-index` on a store imported with `leep-git`: the plan
//! names the missing index and the retired rows and exits 1; dropping needs
//! the explicit acknowledgement; a run derives, a second one drops, and then
//! there is nothing to do and the store verifies clean.

mod common;

use std::path::Path;
use std::process::Output;

use common::*;
use leep_core::Hash;
use serde_json::Value;

fn code(out: &Output) -> i32 {
    out.status.code().expect("exit code")
}

fn run(stores: &Stores, args: &[&str], want: i32) -> String {
    let mut full = vec!["upgrade-index"];
    full.extend_from_slice(args);
    let out = server_cmd(stores, &full);
    assert_eq!(
        code(&out),
        want,
        "upgrade-index {args:?}\n--- stdout\n{}\n--- stderr\n{}",
        stdout(&out),
        stderr(&out)
    );
    stdout(&out)
}

fn import(stores: &Stores, root: &Path) {
    let dir = root.join("git");
    std::fs::create_dir_all(&dir).unwrap();
    git(&dir, &["init", "-q", "-b", "main"]);
    for i in 0..30 {
        write(
            &dir,
            &format!("d{}/f{}.txt", i % 3, i % 4),
            &format!("{i}\n"),
        );
        git(&dir, &["add", "-A"]);
        git(&dir, &["commit", "-q", "-m", &format!("c{i}")]);
    }
    leep_git(&[
        "import",
        "--meta",
        &stores.meta,
        "--blobs",
        &stores.blobs,
        "--git-dir",
        dir.join(".git").to_str().unwrap(),
        "--set-ref",
        "main",
    ]);
}

/// Rows an older server would have left: a v3 index.
fn add_retired_rows(stores: &Stores) {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let meta = leep_meta::open_meta_store(&stores.meta).await.unwrap();
            let ids = meta.list_changesets(None, 1000).await.unwrap();
            for (i, id) in ids.iter().enumerate() {
                meta.put_derived("history-tree-v3", &id.0, vec![3; 50].into())
                    .await
                    .unwrap();
                let node = Hash::of_blob(&(i as u64).to_le_bytes());
                meta.put_derived("history-node-v3", &node, vec![4; 300].into())
                    .await
                    .unwrap();
            }
        });
}

fn upgrade_workflow(stores: &Stores, root: &Path) {
    import(stores, root);
    add_retired_rows(stores);

    let plan = run(stores, &[], 1);
    for want in [
        "30 changesets reachable",
        "history-tree-v4",
        "missing",
        "retired indexes",
        "history-tree-v3",
        "skipped: add --drop-retired --old-servers-stopped",
        "reclaim space",
        "run it: leep-server upgrade-index --yes",
    ] {
        assert!(plan.contains(want), "no {want:?} in\n{plan}");
    }
    let v: Value = serde_json::from_str(&run(stores, &["--plan", "--json"], 1)).unwrap();
    assert_eq!(v["exit_code"], 1);
    assert_eq!(v["plan"]["reachable"], 30);
    let tree = v["plan"]["current"]
        .as_array()
        .unwrap()
        .iter()
        .find(|k| k["kind"] == "history-tree-v4")
        .unwrap();
    assert_eq!(tree["status"], "missing");
    assert_eq!(tree["missing"], 30);
    assert_eq!(v["plan"]["retired"][0]["rows"]["history-node-v3"], 30);

    // Dropping without saying old servers are gone is refused up front.
    run(stores, &["--yes", "--drop-retired"], 2);

    let out = run(stores, &["--yes", "--jobs", "2"], 0);
    assert!(out.contains("derived 30 history-tree-v4 row(s)"), "{out}");
    assert!(
        out.contains("retired rows remain: history-tree-v3"),
        "{out}"
    );

    let out = run(
        stores,
        &[
            "--yes",
            "--drop-retired",
            "--old-servers-stopped",
            "--vacuum",
        ],
        0,
    );
    assert!(out.contains("dropped 30 history-tree-v3 row(s)"), "{out}");
    assert!(out.contains("dropped 30 history-node-v3 row(s)"), "{out}");
    assert!(out.contains("reclaim space"), "{out}");
    assert!(out.contains("the indexes are up to date"), "{out}");

    let out = run(stores, &[], 0);
    assert!(out.contains("nothing to do"), "{out}");
    run(
        stores,
        &["--yes", "--drop-retired", "--old-servers-stopped"],
        0,
    );
    let verify = server_cmd(stores, &["verify"]);
    assert_eq!(code(&verify), 0, "{}", stdout(&verify));
}

#[test]
fn upgrade_index_over_sqlite() {
    let _g = serial();
    let tmp = tempfile::tempdir().unwrap();
    let stores = Stores::sqlite(&tmp.path().join("data"));
    upgrade_workflow(&stores, tmp.path());
}

#[test]
fn upgrade_index_over_postgres() {
    let _g = serial();
    let Some(db) = PgDb::create() else {
        return;
    };
    let tmp = tempfile::tempdir().unwrap();
    let stores = Stores::with_meta(&tmp.path().join("data"), db.url.clone());
    upgrade_workflow(&stores, tmp.path());
}
