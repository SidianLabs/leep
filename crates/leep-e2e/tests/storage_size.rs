//! The storage size guard (ADR 0101): a server with default flags keeps a
//! small history at a bounded size, so a change that makes storage grow
//! fails here instead of in a deployment.
//!
//! A deterministic history is generated in the test (source-like files,
//! commits of one to three files with realistic messages), imported with
//! `leep-git`, and then `LANDS` more edits land through the HTTP API on a
//! server with every storage flag at its default except how soon the
//! background work runs: the automatic delta repack (ADR 0100) checks every
//! second and takes objects 2 s old, the fence is 3 s, and the automatic
//! changeset compaction (ADR 0101) looks every 2 s. Once the blob store and
//! the database stop changing (every loose object packed, every retired
//! pack swept, every changeset row compressed), it checks:
//!
//! - blob store bytes per land and database bytes per land, settled;
//! - all storage (database above an empty one, plus blobs) against Git's
//!   `objects/` after `git gc` for the same history;
//! - `leep_blob_store_size_ratio`.
//!
//! After the lands it runs one `gc` with the server stopped and checks that
//! the marks are segments in the blob store, about 32 bytes a mark, and that
//! no `gc-mark-v1` row is written (ADR 0103).
//!
//! The budgets keep 25% or more over the worst value measured when they were
//! set (docs/PERFORMANCE.md, "Storage size guard"). If a change trips one on
//! purpose, measure again and move the line in the same change, saying
//! why. Runs on Postgres when `LEEP_TEST_POSTGRES_URL` is set (as in CI),
//! on SQLite otherwise; run `cargo build --workspace` first.

mod common;

use std::collections::BTreeMap;
use std::fs;
use std::net::SocketAddr;
use std::path::Path;
use std::time::{Duration, Instant};

use common::*;
use leep_core::api::{CreateChangesetResponse, LandRequest, LandResponse, RefResponse};
use leep_core::{Changeset, ChangesetId, FileChange, FileKind, Hash, RepoPath};

/// Files in the generated tree, commits imported, and lands after.
const FILES: usize = 24;
const IMPORT_COMMITS: usize = 50;
const LANDS: usize = 60;

/// A budget per backend: the database differs, the blob store does not.
struct Budgets {
    /// Blob store bytes per land, settled (pack, index and loose files).
    blob_per_land: u64,
    /// Database bytes per land, settled.
    db_per_land: u64,
    /// All storage over Git's `objects/` after `git gc`.
    ratio_to_git: f64,
    /// `leep_blob_store_size_ratio`, settled: stored over logical bytes.
    blob_store_ratio: f64,
    /// Code search blob rows and keyframes per land (key and value bytes,
    /// the same on both backends), and the most they may be of what whole
    /// rows of the landed files would take (ADR 0102).
    search_blob_per_land: u64,
    search_blob_share: f64,
    /// Bytes of GC mark segments per mark after one `gc` (ADR 0103): 32
    /// and a 33-byte header per segment; a `gc-mark-v1` row was about 167
    /// on Postgres.
    mark_bytes: f64,
    /// Database bytes that `gc` added, per mark.
    gc_db_per_mark: f64,
}

// Measured when set (ADR 0101, two runs each): blobs 469-474 bytes a land on
// both; database 8,669-9,830 (SQLite) and 13,380-13,789 (Postgres) a land;
// all storage 5.8-6.2x (SQLite) and 9.2-9.5x (Postgres) git's 149 KB, most of
// it the search and history indexes of a history this small; blob store
// ratio 0.080. With blob rows as patches (ADR 0102, ten runs on each
// backend): database 7,850-9,966 (SQLite) and 11,605-13,107 (Postgres) a
// land, all storage 5.4-6.3x and 8.8-9.4x. Measured again with GC marks as
// segments (ADR 0103, five runs on each backend): blobs 469-474 on both,
// database 9,284-10,035 (SQLite) and 12,014-13,243 (Postgres), all storage
// 6.01-6.31x and 8.84-9.24x, blob store ratio 0.080, and one `gc` writing
// 749 marks as one segment of 32.0 bytes a mark, the database growing 0
// (SQLite) and 16 KB, 21.9 bytes a mark (Postgres), against about 100 and
// 167 bytes a `gc-mark-v1` row. The budgets keep at least 25% over the worst
// run.
const SQLITE: Budgets = Budgets {
    blob_per_land: 600,
    db_per_land: 12_600,
    ratio_to_git: 7.9,
    blob_store_ratio: 0.10,
    search_blob_per_land: SEARCH_BLOB_PER_LAND,
    search_blob_share: SEARCH_BLOB_SHARE,
    mark_bytes: MARK_BYTES,
    // Pages are 4 KiB: two pages over 749 marks.
    gc_db_per_mark: 12.0,
};

const POSTGRES: Budgets = Budgets {
    blob_per_land: 600,
    db_per_land: 16_600,
    ratio_to_git: 11.6,
    blob_store_ratio: 0.10,
    search_blob_per_land: SEARCH_BLOB_PER_LAND,
    search_blob_share: SEARCH_BLOB_SHARE,
    mark_bytes: MARK_BYTES,
    gc_db_per_mark: 28.0,
};

/// Key and value bytes of the code search blob rows, retired kind included,
/// and their keyframes.
const SEARCH_BLOB_BYTES: &str =
    "SELECT CAST(COALESCE(SUM(LENGTH(key) + LENGTH(value)), 0) AS BIGINT) \
     FROM derived WHERE kind IN ('blob-trigrams-v1', 'blob-trigrams-v2', \
     'blob-trigrams-keyframe-v2')";

// Measured when set (ADR 0102, twenty runs on both backends): 1,267-1,579
// bytes a land, 0.63-0.79 of the 2,001 whole rows would take
// (`blob-trigrams-v1`, 1.0 by definition). A history this short edits each
// file about three times, so most of it is the keyframes promoted from the
// import's whole rows; on corpus C it is a tenth. The budgets keep 25% over
// the worst run and still fail if blob rows are written whole again (2,001
// bytes, a share of 1.0); the paced lands below count patches directly.
const SEARCH_BLOB_PER_LAND: u64 = 1_990;
const SEARCH_BLOB_SHARE: f64 = 0.99;
/// Segment bytes a mark (ADR 0103): 32.0 measured, a 33-byte header per
/// segment on top.
const MARK_BYTES: f64 = 40.0;

/// One-file lands after the measurement, each waiting for the warmer: all
/// but at most one must store their blob row as a patch.
const PACED_LANDS: usize = 10;

/// The server's flags: defaults, but background work runs within seconds.
const FLAGS: &[&str] = &[
    "--request-timeout",
    "2s",
    "--background-repack-interval",
    "1s",
    "--background-repack-fence",
    "3s",
    "--auto-repack-check-interval",
    "1s",
    "--auto-repack-max-age",
    "2s",
    "--auto-compact-interval",
    "2s",
];

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

/// The generated history: the files as they stand, and how to edit them.
struct Corpus {
    rng: Rng,
    words: Vec<String>,
    files: BTreeMap<String, Vec<String>>,
}

impl Corpus {
    fn new() -> Corpus {
        let mut rng = Rng(0x5EED_0101);
        let syll = [
            "ar", "be", "co", "de", "en", "fi", "ga", "hu", "io", "ka", "lu", "me", "no", "pa",
            "re", "si", "ta", "un", "ve", "wo",
        ];
        let words = (0..400)
            .map(|_| {
                (0..2 + rng.below(3))
                    .map(|_| syll[rng.below(syll.len())])
                    .collect()
            })
            .collect();
        let mut c = Corpus {
            rng,
            words,
            files: BTreeMap::new(),
        };
        for i in 0..FILES {
            let path = format!("src/{}/m{i}.rs", ["core", "store", "net", "cli"][i % 4]);
            let n = 60 + c.rng.below(200);
            let lines = (0..n).map(|_| c.line()).collect();
            c.files.insert(path, lines);
        }
        c
    }

    fn word(&mut self) -> String {
        self.words[self.rng.below(self.words.len())].clone()
    }

    fn line(&mut self) -> String {
        match self.rng.below(4) {
            0 => format!(
                "// {} {} {} {}",
                self.word(),
                self.word(),
                self.word(),
                self.word()
            ),
            1 => format!("fn {}_{}(x: u32) -> u32 {{", self.word(), self.word()),
            2 => format!(
                "    let {} = {}.{}({}, {});",
                self.word(),
                self.word(),
                self.word(),
                self.word(),
                self.rng.below(1000)
            ),
            _ => "}".to_string(),
        }
    }

    /// Edit `n` files: one line changed and one to three inserted near it.
    /// Returns the paths and their new contents.
    fn edit(&mut self, n: usize) -> Vec<(String, String)> {
        let paths: Vec<String> = self.files.keys().cloned().collect();
        let mut out = Vec::new();
        for _ in 0..n {
            let p = paths[self.rng.below(paths.len())].clone();
            let at = self.rng.below(self.files[&p].len());
            let changed = self.line();
            let added: Vec<String> = (0..1 + self.rng.below(3)).map(|_| self.line()).collect();
            let lines = self.files.get_mut(&p).unwrap();
            lines[at] = changed;
            for (k, l) in added.into_iter().enumerate() {
                lines.insert(at + k, l);
            }
            out.retain(|(q, _)| *q != p);
            out.push((p.clone(), self.content(&p)));
        }
        out
    }

    fn content(&self, path: &str) -> String {
        self.files[path].join("\n") + "\n"
    }

    /// A commit message shaped like a real one: a subject, a paragraph and
    /// a trailer.
    fn message(&mut self, k: usize) -> String {
        let body: Vec<String> = (0..20 + self.rng.below(30)).map(|_| self.word()).collect();
        let who = self.rng.below(5);
        format!(
            "{}: {} the {} of {} ({k})\n\n{}.\n\nSigned-off-by: Dev {who} <dev{who}@example.org>\n",
            ["core", "store", "net", "cli"][self.rng.below(4)],
            self.word(),
            self.word(),
            self.word(),
            body.join(" ")
        )
    }
}

/// Write `files` into the git work tree and commit them.
fn git_commit(dir: &Path, files: &[(String, String)], msg: &str) {
    for (p, body) in files {
        write(dir, p, body);
    }
    git(dir, &["add", "-A"]);
    git(dir, &["commit", "-q", "-m", msg]);
}

fn dir_bytes(dir: &Path) -> u64 {
    let Ok(rd) = fs::read_dir(dir) else {
        return 0;
    };
    rd.flatten()
        .map(|e| {
            let m = e.metadata().unwrap();
            if m.is_dir() {
                dir_bytes(&e.path())
            } else {
                m.len()
            }
        })
        .sum()
}

/// What the blob store and the database hold.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Sizes {
    blobs: u64,
    loose: usize,
    retired: usize,
    db: u64,
    /// Changeset rows not compressed, and dictionaries stored.
    plain_rows: i64,
    dicts: i64,
    /// Key and value bytes of the code search blob rows and keyframes.
    search_blobs: i64,
    /// `blob-trigrams-v2` rows that are patches.
    patches: i64,
}

fn blobs_dir(stores: &Stores) -> std::path::PathBuf {
    stores.blobs.strip_prefix("file://").unwrap().into()
}

fn sizes(stores: &Stores) -> Sizes {
    use sqlx::Connection;
    let dir = blobs_dir(stores);
    let mut loose = 0;
    for shard in fs::read_dir(&dir).unwrap().flatten() {
        if shard.file_name().len() == 2 {
            loose += fs::read_dir(shard.path()).unwrap().flatten().count();
        }
    }
    let retired = fs::read_to_string(dir.join("packs/retired"))
        .unwrap_or_default()
        .lines()
        .filter(|l| !l.trim().is_empty())
        .count();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let (db, plain_rows, dicts, search_blobs, patches) = rt.block_on(async {
        if stores.meta.starts_with("sqlite:") {
            let mut c = sqlx::SqliteConnection::connect(&stores.meta).await.unwrap();
            let pages: i64 = sqlx::query_scalar("PRAGMA page_count")
                .fetch_one(&mut c)
                .await
                .unwrap();
            let free: i64 = sqlx::query_scalar("PRAGMA freelist_count")
                .fetch_one(&mut c)
                .await
                .unwrap();
            let size: i64 = sqlx::query_scalar("PRAGMA page_size")
                .fetch_one(&mut c)
                .await
                .unwrap();
            let plain: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM changesets WHERE substr(data, 1, 4) <> X'28B52FFD'",
            )
            .fetch_one(&mut c)
            .await
            .unwrap();
            let dicts: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM changeset_dicts")
                .fetch_one(&mut c)
                .await
                .unwrap();
            let search: i64 = sqlx::query_scalar(SEARCH_BLOB_BYTES)
                .fetch_one(&mut c)
                .await
                .unwrap();
            let patches: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM derived WHERE kind = 'blob-trigrams-v2' \
                 AND substr(value, 1, 1) = X'02'",
            )
            .fetch_one(&mut c)
            .await
            .unwrap();
            (
                ((pages - free) * size) as u64,
                plain,
                dicts,
                search,
                patches,
            )
        } else {
            let mut c = sqlx::PgConnection::connect(&stores.meta).await.unwrap();
            let db: i64 = sqlx::query_scalar("SELECT pg_database_size(current_database())")
                .fetch_one(&mut c)
                .await
                .unwrap();
            let plain: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM changesets \
                 WHERE substring(data from 1 for 4) <> '\\x28b52ffd'::bytea",
            )
            .fetch_one(&mut c)
            .await
            .unwrap();
            let dicts: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM changeset_dicts")
                .fetch_one(&mut c)
                .await
                .unwrap();
            let search: i64 = sqlx::query_scalar(SEARCH_BLOB_BYTES)
                .fetch_one(&mut c)
                .await
                .unwrap();
            let patches: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM derived WHERE kind = 'blob-trigrams-v2' \
                 AND get_byte(value, 0) = 2",
            )
            .fetch_one(&mut c)
            .await
            .unwrap();
            (db as u64, plain, dicts, search, patches)
        }
    });
    Sizes {
        blobs: dir_bytes(&dir),
        loose,
        retired,
        db,
        plain_rows,
        dicts,
        search_blobs,
        patches,
    }
}

/// Wait until every loose object is packed, every retired pack swept, the
/// rows compressed (with `compacted`), and nothing changes for 2 s.
fn settle(stores: &Stores, server: &Server, compacted: bool) -> Sizes {
    let deadline = Instant::now() + Duration::from_secs(90);
    let mut last = sizes(stores);
    let mut still = 0;
    loop {
        std::thread::sleep(Duration::from_millis(500));
        let now = sizes(stores);
        let done = now.loose == 0
            && now.retired == 0
            && (!compacted || (now.dicts > 0 && now.plain_rows == 0));
        if done && now == last {
            still += 1;
            if still >= 4 {
                return now;
            }
        } else {
            still = 0;
        }
        assert!(
            Instant::now() < deadline,
            "storage did not settle: {now:?}\n{}",
            server.log_text()
        );
        last = now;
    }
}

/// `gc-mark-v1` rows, and the mark manifest (ADR 0103).
fn gc_record(stores: &Stores) -> (i64, Option<leep_derive::gc_segments::Manifest>) {
    use sqlx::Connection;
    const ROWS: &str = "SELECT COUNT(*) FROM derived WHERE kind = 'gc-mark-v1'";
    const MANIFEST: &str = "SELECT value FROM derived WHERE kind = 'gc-mark-segments-v1'";
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let (rows, value): (i64, Option<Vec<u8>>) = rt.block_on(async {
        if stores.meta.starts_with("sqlite:") {
            let mut c = sqlx::SqliteConnection::connect(&stores.meta).await.unwrap();
            let rows = sqlx::query_scalar(ROWS).fetch_one(&mut c).await.unwrap();
            let v = sqlx::query_scalar(MANIFEST)
                .fetch_optional(&mut c)
                .await
                .unwrap();
            (rows, v)
        } else {
            let mut c = sqlx::PgConnection::connect(&stores.meta).await.unwrap();
            let rows = sqlx::query_scalar(ROWS).fetch_one(&mut c).await.unwrap();
            let v = sqlx::query_scalar(MANIFEST)
                .fetch_optional(&mut c)
                .await
                .unwrap();
            (rows, v)
        }
    });
    let manifest = value.map(|v| leep_derive::gc_segments::Manifest::decode(&v).unwrap());
    (rows, manifest)
}

fn metric(addr: SocketAddr, name: &str) -> Option<f64> {
    let (status, _, body) = http_get(addr, "/metrics", None);
    assert_eq!(status, 200);
    body.lines()
        .find(|l| l.starts_with(name) && l[name.len()..].starts_with(' '))
        .and_then(|l| l.rsplit_once(' ')?.1.parse().ok())
}

fn expect(what: &str, got: std::io::Result<(u16, Vec<u8>)>) -> Vec<u8> {
    let (status, body) = got.unwrap_or_else(|e| panic!("{what}: {e}"));
    assert_eq!(
        status,
        if what == "put blob" { 204 } else { 200 },
        "{what}: {}",
        String::from_utf8_lossy(&body)
    );
    body
}

/// Land one changeset of `files` on `main` through the HTTP API.
fn land(
    addr: SocketAddr,
    parent: ChangesetId,
    files: &[(String, String)],
    msg: &str,
) -> ChangesetId {
    let mut changes = BTreeMap::new();
    for (p, body) in files {
        let h = Hash::of_blob(body.as_bytes());
        expect(
            "put blob",
            http(addr, "PUT", &format!("/v1/blobs/{h}"), body.as_bytes()),
        );
        changes.insert(
            RepoPath::new(p.as_str()).unwrap(),
            FileChange::Modify {
                content: h,
                kind: FileKind::Regular,
                size: body.len() as u64,
            },
        );
    }
    let cs = Changeset {
        parents: vec![parent],
        author: "Dev 1 <dev1@example.org>".into(),
        timestamp: 1_700_000_000,
        message: msg.into(),
        changes,
        extra: BTreeMap::new(),
    };
    let made: CreateChangesetResponse = serde_json::from_slice(&expect(
        "create changeset",
        http(
            addr,
            "POST",
            "/v1/changesets",
            &serde_json::to_vec(&cs).unwrap(),
        ),
    ))
    .unwrap();
    let req = LandRequest {
        changeset: made.id,
        target_ref: "main".into(),
        create: None,
        land_id: None,
    };
    let resp: LandResponse = serde_json::from_slice(&expect(
        "land",
        http(addr, "POST", "/v1/land", &serde_json::to_vec(&req).unwrap()),
    ))
    .unwrap();
    match resp {
        LandResponse::Landed { landed, .. } => landed,
        other => panic!("a lone writer conflicted: {other:?}"),
    }
}

#[test]
fn storage_stays_within_its_budgets() {
    let _g = serial();
    let started = Instant::now();
    let tmp = tempfile::tempdir().unwrap();
    let work = tmp.path();
    let pg = PgDb::create("leep_e2e");
    let (stores, budgets, backend) = match &pg {
        Some(db) => (
            Stores::with_meta(&work.join("data"), db.url.clone()),
            &POSTGRES,
            "postgres",
        ),
        None => (Stores::sqlite(&work.join("data")), &SQLITE, "sqlite"),
    };
    // An empty database at the current schema: the baseline.
    assert_ok(&server_cmd(&stores, &["migrate"]), "leep-server migrate");
    let empty = sizes(&stores);

    // The history, in git, imported.
    let repo = work.join("git");
    fs::create_dir_all(&repo).unwrap();
    git(&repo, &["init", "-q", "-b", "main"]);
    let mut corpus = Corpus::new();
    let all: Vec<(String, String)> = corpus
        .files
        .keys()
        .map(|p| (p.clone(), corpus.content(p)))
        .collect();
    let msg = corpus.message(0);
    git_commit(&repo, &all, &msg);
    for k in 1..IMPORT_COMMITS {
        let n = 1 + corpus.rng.below(3);
        let files = corpus.edit(n);
        let msg = corpus.message(k);
        git_commit(&repo, &files, &msg);
    }
    leep_git(&[
        "import",
        "--meta",
        &stores.meta,
        "--blobs",
        &stores.blobs,
        "--git-dir",
        repo.to_str().unwrap(),
        "--set-ref",
        "main",
    ]);

    let metrics_port = free_port();
    let metrics_addr: SocketAddr = format!("127.0.0.1:{metrics_port}").parse().unwrap();
    let listen = metrics_addr.to_string();
    let mut flags = FLAGS.to_vec();
    flags.extend(["--metrics-listen", &listen]);
    let server = Server::start_with(free_port(), &stores, work, &flags);
    let before = settle(&stores, &server, false);

    // The lands: one file, and one in five three files, each a small edit.
    let head: RefResponse = serde_json::from_slice(&expect(
        "get ref",
        http(server.addr, "GET", "/v1/refs/main", b""),
    ))
    .unwrap();
    let mut tip = head.target.expect("main after the import");
    let mut landed = Vec::new();
    for k in 0..LANDS {
        let files = corpus.edit(if k % 5 == 4 { 3 } else { 1 });
        let msg = corpus.message(IMPORT_COMMITS + k);
        tip = land(server.addr, tip, &files, &msg);
        landed.push((files, msg));
    }
    let after = settle(&stores, &server, true);
    let size_ratio = metric(metrics_addr, "leep_blob_store_size_ratio");
    let dictionary = metric(metrics_addr, "leep_changeset_dictionary");

    // Blob rows as patches, counted directly (ADR 0102): lands that
    // outrun the warmer leave heads unindexed, so the measurement above
    // cannot tell patches from whole rows reliably. These lands each wait
    // until the warmer indexed their head, as at the pace of a CLI; every
    // one edits one file, so each should store one patch.
    let warmed = || {
        metric(
            metrics_addr,
            "leep_index_rows_total{kind=\"tree-trigrams-v2\"}",
        )
        .unwrap_or(0.0)
    };
    for k in 0..PACED_LANDS {
        let files = corpus.edit(1);
        let msg = corpus.message(IMPORT_COMMITS + LANDS + k);
        let before_warm = warmed();
        tip = land(server.addr, tip, &files, &msg);
        let deadline = Instant::now() + Duration::from_secs(10);
        while warmed() <= before_warm {
            assert!(
                Instant::now() < deadline,
                "the warmer did not index paced land {k}\n{}",
                server.log_text()
            );
            std::thread::sleep(Duration::from_millis(20));
        }
    }
    // Not in Git's history below: what is compared is what `after` holds.
    let paced_patches = sizes(&stores).patches - after.patches;
    drop(server);

    // One GC with the servers stopped: its marks are segments in the blob
    // store, 32 bytes a mark, and it writes no `gc-mark-v1` row (ADR 0103).
    let pre_gc = sizes(&stores);
    assert_ok(
        &server_cmd(
            &stores,
            &["gc", "--grace", "0", "--fence", "0s", "--unsafe-no-fence"],
        ),
        "leep-server gc",
    );
    let post_gc = sizes(&stores);
    let (mark_rows, manifest) = gc_record(&stores);
    let manifest = manifest.expect("a mark manifest after gc");
    let marks: u64 = manifest.live.iter().map(|s| s.marks).sum();
    let segment_bytes: u64 = mark_segment_files(&blobs_dir(&stores))
        .iter()
        .map(|p| fs::metadata(p).unwrap().len())
        .sum();
    let mark_bytes = segment_bytes as f64 / marks.max(1) as f64;
    let gc_db_per_mark = post_gc.db.saturating_sub(pre_gc.db) as f64 / marks.max(1) as f64;

    // Git's size for the same history.
    for (files, msg) in &landed {
        git_commit(&repo, files, msg);
    }
    git(&repo, &["gc", "-q"]);
    let git_bytes = dir_bytes(&repo.join(".git/objects"));

    let blob_per_land = (after.blobs.saturating_sub(before.blobs)) / LANDS as u64;
    let search_per_land = (after.search_blobs - before.search_blobs).max(0) as u64 / LANDS as u64;
    // What whole rows (`blob-trigrams-v1`) of the landed versions would take.
    let whole_per_land = landed
        .iter()
        .flat_map(|(files, _)| files.iter())
        .map(|(_, body)| {
            32 + leep_derive::search::trigram::BlobRow::of(body.as_bytes())
                .encode()
                .len() as u64
        })
        .sum::<u64>()
        / LANDS as u64;
    let search_share = search_per_land as f64 / whole_per_land.max(1) as f64;
    let db_per_land = (after.db.saturating_sub(before.db)) / LANDS as u64;
    let total = after.blobs + after.db.saturating_sub(empty.db);
    let ratio = total as f64 / git_bytes as f64;
    let report = format!(
        "[{backend}] blobs {} -> {} ({blob_per_land} per land), db {} -> {} above {} \
         ({db_per_land} per land), total {total} = {ratio:.2}x git ({git_bytes}), \
         blob store ratio {size_ratio:?}, dictionary {dictionary:?}, search blob rows \
         {search_per_land} per land ({search_share:.2} of whole rows' {whole_per_land}), {} \
         patches over {PACED_LANDS} paced lands, gc: {marks} marks in {} segments of \
         {segment_bytes} bytes ({mark_bytes:.1} a mark), db {} -> {} ({gc_db_per_mark:.1} a \
         mark), {mark_rows} gc-mark-v1 rows, {:.1}s",
        before.blobs,
        after.blobs,
        before.db - empty.db,
        after.db - empty.db,
        empty.db,
        paced_patches,
        manifest.live.len(),
        pre_gc.db - empty.db,
        post_gc.db - empty.db,
        started.elapsed().as_secs_f64()
    );
    println!("{report}");
    assert!(
        blob_per_land <= budgets.blob_per_land,
        "blob store bytes per land over budget ({}): {report}",
        budgets.blob_per_land
    );
    assert!(
        db_per_land <= budgets.db_per_land,
        "database bytes per land over budget ({}): {report}",
        budgets.db_per_land
    );
    assert!(
        ratio <= budgets.ratio_to_git,
        "storage over {}x git: {report}",
        budgets.ratio_to_git
    );
    let size_ratio = size_ratio.expect("leep_blob_store_size_ratio");
    assert!(
        size_ratio <= budgets.blob_store_ratio,
        "leep_blob_store_size_ratio over {}: {report}",
        budgets.blob_store_ratio
    );
    assert!(dictionary.unwrap_or(0.0) >= 1.0, "no dictionary: {report}");
    assert!(
        search_per_land <= budgets.search_blob_per_land,
        "search blob rows per land over budget ({}): {report}",
        budgets.search_blob_per_land
    );
    assert!(
        paced_patches >= PACED_LANDS as i64 - 1,
        "fewer than {} of {PACED_LANDS} paced lands stored a patch: {report}",
        PACED_LANDS - 1
    );
    assert!(
        search_share <= budgets.search_blob_share,
        "search blob rows over {} of whole rows: {report}",
        budgets.search_blob_share
    );
    assert_eq!(mark_rows, 0, "gc wrote gc-mark-v1 rows: {report}");
    assert!(marks > 0, "gc marked nothing: {report}");
    assert!(
        mark_bytes <= budgets.mark_bytes,
        "gc mark bytes per mark over {}: {report}",
        budgets.mark_bytes
    );
    assert!(
        gc_db_per_mark <= budgets.gc_db_per_mark,
        "database bytes gc wrote per mark over {}: {report}",
        budgets.gc_db_per_mark
    );
    let out = server_cmd(&stores, &["verify", "--full"]);
    assert_ok(&out, "leep-server verify --full");
    drop(pg);
}
