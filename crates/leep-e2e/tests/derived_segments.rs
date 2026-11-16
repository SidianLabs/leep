//! Derived segments (ADR 0104 to 0106) with the real binaries: a store moves
//! from rows to segments in mode `build` (every row stays, so a binary that
//! predates segments reads everything and rolling back is free), then
//! `retire` deletes the rows after recording each family's store feature
//! (the code search rows of `content-v1` only those retention keeps); and a
//! restore drill with a metadata snapshot naming a segment the blob copy
//! lacks. Run `cargo build --workspace` first.

mod common;

use std::collections::BTreeMap;
use std::path::Path;
use std::time::{Duration, Instant};

use common::*;
use leep_core::{Changeset, ChangesetId, FileChange, FileKind, Hash, RepoPath};
use leep_derive::derived_segments::{self as ds, ManifestRead, COL_HISTORY_TREE, COL_SKIPLIST};
use leep_derive::Repo;

fn rt() -> &'static tokio::runtime::Runtime {
    static RT: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap()
    })
}

fn open_repo(stores: &Stores) -> Repo {
    let blobs = leep_store::open_blob_store(&stores.blobs).expect("blob store");
    let meta = rt()
        .block_on(leep_meta::open_meta_store(&stores.meta))
        .expect("meta store");
    Repo::new(blobs, meta)
}

/// `n` changesets on `main`, with history-tree-v4 derived for all.
fn plant(stores: &Stores, n: usize) -> Vec<ChangesetId> {
    let repo = open_repo(stores);
    rt().block_on(async {
        let mut tip: Option<ChangesetId> = None;
        let mut made = Vec::new();
        for i in 0..n {
            let body = format!("fn f{i}() {{}}\n");
            let content = repo.put_blob(body.clone().into()).await.unwrap();
            let id = repo
                .create_changeset(Changeset {
                    parents: tip.into_iter().collect(),
                    author: "t".into(),
                    timestamp: 1,
                    message: format!("c{i}"),
                    changes: BTreeMap::from([(
                        RepoPath::new(format!("m{}/f{}.rs", i % 5, i % 13)).unwrap(),
                        FileChange::Modify {
                            content,
                            kind: FileKind::Regular,
                            size: body.len() as u64,
                        },
                    )]),
                    extra: BTreeMap::new(),
                })
                .await
                .unwrap();
            made.push(id);
            tip = Some(id);
        }
        repo.meta().cas_ref("main", None, tip).await.unwrap();
        leep_derive::derived::derive::<leep_derive::derived::HistoryTree>(&repo, tip.unwrap())
            .await
            .unwrap();
        // The search rows of the head patched on its parent's (family
        // content-v1): whole rows, patches and keyframes; the parent's own
        // versions are rows retention no longer keeps.
        repo.derive_search(made[n - 2]).await.unwrap();
        repo.derive_search(made[n - 1]).await.unwrap();
        made
    })
}

/// The search kinds of `content-v1`.
const CONTENT_KINDS: [&str; 4] = [
    "blob-trigrams-v2",
    "blob-trigrams-keyframe-v2",
    "tree-trigrams-v2",
    "tree-trigrams-keyframe-v2",
];

/// The tree nodes and blobs below `id`'s tree.
fn head_content(stores: &Stores, id: ChangesetId) -> Vec<Hash> {
    let repo = open_repo(stores);
    rt().block_on(async {
        let mut keys = Vec::new();
        let mut stack = vec![repo.root_tree(id).await.unwrap()];
        while let Some(t) = stack.pop() {
            keys.push(t);
            for e in repo.read_tree(t).await.unwrap().entries.values() {
                match *e {
                    leep_core::TreeEntry::Dir { tree } => stack.push(tree),
                    leep_core::TreeEntry::File { content, .. } => keys.push(content),
                }
            }
        }
        keys.sort_unstable();
        keys.dedup();
        keys
    })
}

/// Rows of the search kinds as the database holds them: of keys a head
/// holds, and of the others.
fn content_rows(stores: &Stores, head: &[Hash]) -> (u64, u64) {
    let meta = rt()
        .block_on(leep_meta::open_meta_store(&stores.meta))
        .unwrap();
    rt().block_on(async {
        let (mut held, mut other) = (0, 0);
        for kind in CONTENT_KINDS {
            for k in meta.list_derived_keys(kind, None, 1 << 20).await.unwrap() {
                if head.contains(&k) && !kind.contains("keyframe") {
                    held += 1;
                } else {
                    other += 1;
                }
            }
        }
        (held, other)
    })
}

/// The values of every changeset a segment family holds and every
/// changeset has (`skiplist`, `history-tree-v4`; `generation`, `root_tree`
/// of `changeset-v2`) as a new process reads them; history parts by
/// content.
fn values(stores: &Stores, ids: &[ChangesetId]) -> Vec<Option<Vec<u8>>> {
    let repo = open_repo(stores);
    let keys: Vec<Hash> = ids.iter().map(|i| i.0).collect();
    rt().block_on(async {
        let mut out: Vec<Option<Vec<u8>>> = repo
            .meta()
            .get_derived_many("skiplist", &keys)
            .await
            .unwrap()
            .into_iter()
            .map(|v| v.map(|b| b.to_vec()))
            .collect();
        for v in repo
            .meta()
            .get_derived_many("history-tree-v4", &keys)
            .await
            .unwrap()
        {
            out.push(v.map(|b| {
                leep_derive::derived::history_pack::canonical(&b)
                    .unwrap()
                    .into_owned()
            }));
        }
        for kind in ["generation", "root_tree"] {
            out.extend(
                repo.meta()
                    .get_derived_many(kind, &keys)
                    .await
                    .unwrap()
                    .into_iter()
                    .map(|v| v.map(|b| b.to_vec())),
            );
        }
        out
    })
    .into_iter()
    .chain(search_values(stores, *ids.last().unwrap()))
    .collect()
}

/// The search rows of `head`'s content as a new process reads them.
fn search_values(stores: &Stores, head: ChangesetId) -> Vec<Option<Vec<u8>>> {
    let keys = head_content(stores, head);
    let repo = open_repo(stores);
    rt().block_on(async {
        let mut out = Vec::new();
        for kind in ["blob-trigrams-v2", "tree-trigrams-v2"] {
            out.extend(
                repo.meta()
                    .get_derived_many(kind, &keys)
                    .await
                    .unwrap()
                    .into_iter()
                    .map(|v| v.map(|b| b.to_vec())),
            );
        }
        out
    })
}

/// Rows of `skiplist`, `history-tree-v4`, `generation` and `root_tree` as
/// a binary that predates segments reads them: the database alone.
fn rows(stores: &Stores) -> (u64, u64, u64, u64) {
    let meta = rt()
        .block_on(leep_meta::open_meta_store(&stores.meta))
        .unwrap();
    rt().block_on(async {
        (
            meta.count_derived("skiplist").await.unwrap(),
            meta.count_derived("history-tree-v4").await.unwrap(),
            meta.count_derived("generation").await.unwrap(),
            meta.count_derived("root_tree").await.unwrap(),
        )
    })
}

/// Whether live `content-v1` segments hold every search row of the head's
/// content that has a value.
fn search_sealed(stores: &Stores, head: &[Hash]) -> bool {
    let repo = open_repo(stores);
    rt().block_on(async {
        repo.segments().refresh(&**repo.row_meta()).await.unwrap();
        let c = repo.segments().family(ds::FAMILY_CONTENT).clone();
        for (kind, col) in [
            ("blob-trigrams-v2", ds::COL_BLOB_TRIGRAMS),
            ("tree-trigrams-v2", ds::COL_TREE_TRIGRAMS),
        ] {
            let have = repo.row_meta().derived_present(kind, head).await.unwrap();
            if head.iter().zip(have).any(|(k, h)| h && !c.holds(k, col)) {
                return false;
            }
        }
        true
    })
}

/// Changesets of `ids` live segments of both families hold.
fn sealed(stores: &Stores, ids: &[ChangesetId]) -> usize {
    let repo = open_repo(stores);
    rt().block_on(async {
        repo.segments().refresh(&**repo.row_meta()).await.unwrap();
        let v1 = repo.segments().family(ds::FAMILY_CHANGESET).clone();
        let v2 = repo.segments().family(ds::FAMILY_CHANGESET_V2).clone();
        ids.iter()
            .filter(|id| {
                v1.holds(&id.0, COL_SKIPLIST)
                    && v1.holds(&id.0, COL_HISTORY_TREE)
                    && v2.holds(&id.0, ds::COL_GENERATION)
                    && v2.holds(&id.0, ds::COL_ROOT_TREE)
            })
            .count()
    })
}

fn segment_server(stores: &Stores, root: &Path, mode: &str) -> Server {
    Server::start_with(
        free_port(),
        stores,
        root,
        &[
            "--derived-segments",
            mode,
            "--debug-derived-segments-small",
            "--debug-derived-segments-first",
            "0s",
            "--derived-segments-interval",
            "500ms",
            "--derived-segments-retire-families",
            "all",
        ],
    )
}

fn wait_for(what: &str, server: &Server, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(90);
    while !done() {
        assert!(
            Instant::now() < deadline,
            "{what}: timed out\n{}",
            server.log_text()
        );
        std::thread::sleep(Duration::from_millis(200));
    }
}

fn features(stores: &Stores) -> Vec<String> {
    use sqlx::Connection;
    rt().block_on(async {
        if stores.meta.starts_with("sqlite:") {
            let mut c = sqlx::SqliteConnection::connect(&stores.meta).await.unwrap();
            sqlx::query_scalar("SELECT name FROM store_features")
                .fetch_all(&mut c)
                .await
                .unwrap()
        } else {
            let mut c = sqlx::PgConnection::connect(&stores.meta).await.unwrap();
            sqlx::query_scalar("SELECT name FROM store_features")
                .fetch_all(&mut c)
                .await
                .unwrap()
        }
    })
}

fn verify_full(stores: &Stores, what: &str) -> String {
    let out = server_cmd(stores, &["verify", "--full"]);
    assert_ok(&out, what);
    stdout(&out)
}

/// `build`, beside binaries that read rows only, then `retire`.
fn build_then_retire(stores: Stores, root: &Path) {
    let made = plant(&stores, 120);
    let want = values(&stores, &made);
    assert!(want[..4 * made.len()].iter().all(Option::is_some));
    assert!(want[4 * made.len()..].iter().any(Option::is_some));
    let before = rows(&stores);
    assert_eq!(before, (120, 120, 120, 120));
    let head = head_content(&stores, *made.last().unwrap());
    let search_before = content_rows(&stores, &head);
    assert!(
        search_before.0 > 0 && search_before.1 > 0,
        "{search_before:?}"
    );

    // Build: segments appear, every row stays.
    let server = segment_server(&stores, root, "build");
    wait_for("build seals every changeset", &server, || {
        sealed(&stores, &made) == made.len()
    });
    wait_for("build seals the head's search rows", &server, || {
        search_sealed(&stores, &head)
    });
    drop(server);
    assert_eq!(rows(&stores), before, "build mode deletes no row");
    assert_eq!(content_rows(&stores, &head), search_before);
    assert!(
        !features(&stores)
            .iter()
            .any(|f| f.starts_with("derived-segments")),
        "build mode records no feature"
    );
    assert_eq!(values(&stores, &made), want);
    verify_full(&stores, "verify --full in build mode");

    // Rolling every binary back is free: an older binary ignores the
    // manifest and reads the rows. Without the manifest, the store still
    // verifies in full and every value reads from rows.
    if stores.meta.starts_with("sqlite:") {
        let copy = root.join("rollback");
        std::fs::create_dir_all(&copy).unwrap();
        let src = stores
            .meta
            .trim_start_matches("sqlite://")
            .split('?')
            .next()
            .unwrap()
            .to_string();
        std::fs::copy(&src, copy.join("meta.db")).unwrap();
        let old = Stores {
            meta: format!("sqlite://{}?mode=rwc", copy.join("meta.db").display()),
            blobs: stores.blobs.clone(),
        };
        let meta = rt()
            .block_on(leep_meta::open_meta_store(&old.meta))
            .unwrap();
        rt().block_on(async {
            let keys = [
                ds::manifest_key(ds::FAMILY_CHANGESET),
                ds::manifest_key(ds::FAMILY_CHANGESET_V2),
                ds::manifest_key(ds::FAMILY_CONTENT),
            ];
            assert_eq!(
                meta.delete_derived_keys(&[ds::MANIFEST_KIND], &keys)
                    .await
                    .unwrap(),
                3
            );
        });
        drop(meta);
        assert_eq!(values(&old, &made), want, "an older binary's reads");
        verify_full(&old, "verify --full of the rolled-back copy");
    }

    // Retire without naming the second family (a fleet already in retire
    // upgrading): changeset-v1 passes its door, changeset-v2 does not
    // (ADR 0105).
    let server = Server::start_with(
        free_port(),
        &stores,
        root,
        &[
            "--derived-segments",
            "retire",
            "--debug-derived-segments-small",
            "--debug-derived-segments-first",
            "0s",
            "--derived-segments-interval",
            "500ms",
        ],
    );
    wait_for("retire deletes the changeset-v1 rows", &server, || {
        let r = rows(&stores);
        r.0 == 0 && r.1 == 0
    });
    // A few more passes: still none of the second family's rows go.
    std::thread::sleep(Duration::from_secs(2));
    drop(server);
    assert_eq!(rows(&stores), (0, 0, 120, 120));
    assert_eq!(content_rows(&stores, &head), search_before);
    let f = features(&stores);
    assert!(f.iter().any(|g| g == "derived-segments"), "{f:?}");
    assert!(!f.iter().any(|g| g == "derived-segments-2"), "{f:?}");
    assert!(!f.iter().any(|g| g == "derived-segments-3"), "{f:?}");
    assert_eq!(values(&stores, &made), want);

    // Retire naming every family: the feature first, then the rows go;
    // everything still reads.
    let server = segment_server(&stores, root, "retire");
    wait_for("retire deletes the rows", &server, || {
        rows(&stores) == (0, 0, 0, 0) && content_rows(&stores, &head).0 == 0
    });
    drop(server);
    // Of the search rows, those of the head and every keyframe went; the
    // parent's own versions (no head holds them) stay rows, for retention.
    let (held, other) = content_rows(&stores, &head);
    assert_eq!(held, 0);
    assert!(
        other > 0 && other < search_before.1,
        "{other} of {search_before:?}"
    );
    for f in [
        "derived-segments",
        "derived-segments-2",
        "derived-segments-3",
    ] {
        assert!(
            features(&stores).iter().any(|g| g == f),
            "{f}: {:?}",
            features(&stores)
        );
    }
    assert_eq!(values(&stores, &made), want);
    verify_full(&stores, "verify --full after retire");
    // A server with passes off still reads them, and GC keeps them.
    let gc = server_cmd(
        &stores,
        &["gc", "--grace", "0", "--fence", "0s", "--unsafe-no-fence"],
    );
    assert_ok(&gc, "gc");
    let out = stdout(&gc);
    assert!(out.contains("derived segments: "), "{out}");
    assert_eq!(values(&stores, &made), want, "after gc");
    verify_full(&stores, "verify --full after gc");
}

#[test]
fn build_keeps_rows_for_older_binaries_then_retire_deletes_them_sqlite() {
    let _g = serial();
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    build_then_retire(Stores::sqlite(&root.join("data")), root);
}

#[test]
fn build_keeps_rows_for_older_binaries_then_retire_deletes_them_postgres() {
    let Some(db) = PgDb::create("leep_e2e") else {
        return;
    };
    let _g = serial();
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    build_then_retire(Stores::with_meta(&root.join("data"), db.url.clone()), root);
    drop(db);
}

/// A backup whose metadata snapshot names a segment its blob copy lacks
/// (the copy finished before the segment was sealed): the restore reads
/// those values as missing, `verify` notes it, `repair rederive --missing`
/// fills the rows, and the next pass stops naming the segment.
#[test]
fn restore_drill_with_a_manifest_ahead_of_the_blob_copy() {
    let _g = serial();
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    let stores = Stores::sqlite(&root.join("data"));
    let made = plant(&stores, 120);
    let want = values(&stores, &made);
    let server = segment_server(&stores, root, "retire");
    wait_for("retire deletes the rows", &server, || {
        rows(&stores) == (0, 0, 0, 0)
    });
    drop(server);
    let out_dir = root.join("backup");
    let out = server_cmd(
        &stores,
        &["backup", "--out", out_dir.to_str().unwrap(), "--json"],
    );
    assert_ok(&out, "backup");
    let manifest: serde_json::Value = serde_json::from_str(&stdout(&out)).unwrap();
    let name = manifest["manifest"]["name"].as_str().unwrap().to_string();

    // Lose the newest live segment from the blob copy.
    let repo = open_repo(&stores);
    let (_, read) = rt()
        .block_on(ds::read_manifest(&repo, ds::FAMILY_CHANGESET))
        .unwrap();
    let ManifestRead::Read(m) = read else {
        panic!("no manifest")
    };
    let lost = *m.live.last().unwrap();
    for h in [lost.index, lost.data] {
        let hex = h.to_hex();
        let f = out_dir.join("blobs").join(&hex[..2]).join(&hex[2..]);
        assert!(
            f.is_file(),
            "segment blob {hex} is not a file in the backup"
        );
        std::fs::remove_file(f).unwrap();
    }
    drop(repo);

    let dest = Stores::sqlite(&root.join("restored"));
    let out = server_cmd(
        &dest,
        &[
            "restore",
            "--from",
            out_dir.to_str().unwrap(),
            "--snapshot",
            &name,
            "--json",
        ],
    );
    assert_ok(&out, "restore");
    // Rows of the lost segment's keys read as missing: noted, not damage.
    let got = values(&dest, &made);
    let missing = got
        .iter()
        .zip(&want)
        .filter(|(g, w)| g.is_none() && w.is_some())
        .count();
    assert!(
        missing > 0 && missing <= 2 * lost.rows as usize,
        "{missing}"
    );
    let v = verify_full(&dest, "verify --full of the restore");
    assert!(v.contains("is not stored"), "{v}");
    let out = server_cmd(
        &dest,
        &[
            "repair",
            "rederive",
            "--kind",
            "skiplist",
            "--kind",
            "history-tree-v4",
            "--missing",
        ],
    );
    assert_ok(&out, "repair rederive --missing");
    assert_eq!(values(&dest, &made), want);
    verify_full(&dest, "verify --full after repair");
    // The next pass drops the segment the blob copy lacks.
    let server = segment_server(&dest, root, "build");
    wait_for("the lost segment is dropped", &server, || {
        let repo = open_repo(&dest);
        let (_, read) = rt()
            .block_on(ds::read_manifest(&repo, ds::FAMILY_CHANGESET))
            .unwrap();
        matches!(read, ManifestRead::Read(m) if !m.live.iter().any(|s| s.index == lost.index))
    });
    drop(server);
    assert_eq!(values(&dest, &made), want);
}
