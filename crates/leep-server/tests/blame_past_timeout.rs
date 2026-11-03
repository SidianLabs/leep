//! Measures a long blame on Postgres (ADR 0078): cold, resumed from its
//! checkpoints after the process that started it died, and retried after a
//! request timeout. Not part of the suite:
//!
//! ```sh
//! LEEP_TEST_POSTGRES_URL=... cargo test --release -p leep-server \
//!     --test blame_past_timeout -- --ignored --nocapture
//! ```
//!
//! `LEEP_MEASURE_VERSIONS` (default 3,000) sets the hot file's versions,
//! `LEEP_MEASURE_FILLER` (default 3) the changesets of other files between
//! two of them, and `LEEP_MEASURE_LINES` (default 1,000) the file's first
//! length (it grows to at most 2.5 times that). The history is kept in
//! schema `leep_measure_blame_<versions>_<filler>_<lines>` and reused by
//! later runs; drop the schema when done.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use bytes::Bytes;
use http_body_util::BodyExt;
use leep_core::{Changeset, ChangesetId, FileChange, FileKind, MetaStore, RepoPath};
use leep_derive::Repo;
use leep_meta::PostgresMetaStore;
use leep_server::read_limits::{ClassLimit, ReadConcurrency};
use leep_store::FsBlobStore;
use tower::ServiceExt;

const BLAME: &str = "blame-v2";

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

struct Lcg(u64);

impl Lcg {
    fn below(&mut self, n: usize) -> usize {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        ((self.0 >> 33) as usize) % n
    }
}

async fn commit(repo: &Repo, parent: Option<ChangesetId>, path: &str, body: String) -> ChangesetId {
    let size = body.len() as u64;
    let content = repo.blobs().put(Bytes::from(body)).await.unwrap();
    repo.create_changeset(Changeset {
        parents: parent.into_iter().collect(),
        author: "t".into(),
        timestamp: 1,
        message: path.into(),
        changes: BTreeMap::from([(
            RepoPath::new(path).unwrap(),
            FileChange::Modify {
                content,
                kind: FileKind::Regular,
                size,
            },
        )]),
        extra: BTreeMap::new(),
    })
    .await
    .unwrap()
}

/// A linear history: `hot.rs` edited `versions` times (a line replaced,
/// inserted or dropped), with `filler` changesets of other files between.
async fn generate(repo: &Repo, versions: usize, filler: usize, first: usize) -> ChangesetId {
    let mut rng = Lcg(7);
    let mut lines: Vec<String> = (0..first)
        .map(|i| format!("    let base_{i} = compute({i});\n"))
        .collect();
    let mut tip = commit(repo, None, "hot.rs", lines.concat()).await;
    let started = Instant::now();
    for v in 1..versions {
        for k in 0..filler {
            let p = format!("other/{}.rs", rng.below(500));
            tip = commit(repo, Some(tip), &p, format!("{v} {k}\n")).await;
        }
        let i = rng.below(lines.len());
        match rng.below(3) {
            0 => lines[i] = format!("    let edit_{v} = compute({v});\n"),
            2 if lines.len() > first / 2 || 2 * lines.len() > 5 * first => {
                lines.remove(i);
            }
            _ => lines.insert(i, format!("    let add_{v} = compute({v});\n")),
        }
        tip = commit(repo, Some(tip), "hot.rs", lines.concat()).await;
        if v % 500 == 0 {
            eprintln!("  {v} versions in {:.0?}", started.elapsed());
        }
    }
    tip
}

async fn blame_rows(meta: &dyn MetaStore) -> (u64, u64) {
    let (mut n, mut bytes, mut after) = (0, 0, None);
    loop {
        let page = meta.list_derived(BLAME, after, 1000).await.unwrap();
        let Some((last, _)) = page.last() else {
            return (n, bytes);
        };
        after = Some(*last);
        n += page.len() as u64;
        bytes += page.iter().map(|(_, v)| v.len() as u64).sum::<u64>();
    }
}

#[test]
#[ignore]
fn blame_past_timeout_on_postgres() {
    let Ok(url) = std::env::var("LEEP_TEST_POSTGRES_URL") else {
        eprintln!("skipping: LEEP_TEST_POSTGRES_URL not set");
        return;
    };
    let versions = env_usize("LEEP_MEASURE_VERSIONS", 3000);
    let filler = env_usize("LEEP_MEASURE_FILLER", 3);
    let first = env_usize("LEEP_MEASURE_LINES", 1000);
    let schema = format!("leep_measure_blame_{versions}_{filler}_{first}");
    let blob_dir = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(&schema);
    let rt = || {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
    };
    let main = rt();
    let open = move || {
        let (url, schema, dir) = (url.clone(), schema.clone(), blob_dir.clone());
        async move {
            let meta: Arc<dyn MetaStore> = Arc::new(
                PostgresMetaStore::connect_with_schema(&url, &schema)
                    .await
                    .unwrap(),
            );
            let blobs = Arc::new(FsBlobStore::new(dir));
            (Repo::new(blobs, meta.clone()), meta)
        }
    };
    let path = RepoPath::new("hot.rs").unwrap();
    let (tip, meta) = main.block_on(async {
        let (repo, meta) = open().await;
        let tip = match meta.get_ref("main").await.unwrap() {
            Some(tip) => tip,
            None => {
                eprintln!("generating {versions} versions, {filler} filler each");
                let tip = generate(&repo, versions, filler, first).await;
                leep_derive::derived::derive::<leep_derive::derived::HistoryTree>(&repo, tip)
                    .await
                    .unwrap();
                assert!(meta.cas_ref("main", None, Some(tip)).await.unwrap());
                tip
            }
        };
        (tip, meta)
    });
    let reset = || {
        main.block_on(async {
            meta.delete_derived_kind(BLAME).await.unwrap();
        })
    };

    // Cold, in a new process (no caches).
    reset();
    let started = Instant::now();
    let want = main.block_on(async {
        let (repo, _) = open().await;
        repo.blame(tip, &path).await.unwrap()
    });
    let cold = started.elapsed();
    let (rows, bytes) = main.block_on(blame_rows(&*meta));
    let changers = want
        .lines
        .iter()
        .map(|l| l.changeset)
        .collect::<std::collections::HashSet<_>>()
        .len();
    println!(
        "cold blame: {cold:.2?}, {} lines from {changers} changesets; \
         {rows} blame rows written ({bytes} bytes)",
        want.lines.len()
    );
    let again = Instant::now();
    main.block_on(async {
        let (repo, _) = open().await;
        repo.blame(tip, &path).await.unwrap()
    });
    println!("cached blame: {:.2?}", again.elapsed());

    // Again from nothing in a new process, Postgres now warm: the time to
    // compare a resumed blame with.
    reset();
    let started = Instant::now();
    main.block_on(async {
        let (repo, _) = open().await;
        repo.blame(tip, &path).await.unwrap()
    });
    let warm = started.elapsed();
    println!("cold blame again (new process, warm Postgres): {warm:.2?}");

    // The process dies once half the checkpoints are written; a new one
    // resumes.
    reset();
    let doomed = rt();
    doomed.spawn({
        let (open, path) = (open.clone(), path.clone());
        async move {
            let (repo, _) = open().await;
            let _ = repo.blame(tip, &path).await;
        }
    });
    let half = (rows.saturating_sub(1) / 2).max(1);
    let waited = Instant::now();
    while main.block_on(meta.count_derived(BLAME)).unwrap() < half {
        assert!(waited.elapsed() < cold * 20, "no checkpoints written");
        std::thread::sleep(std::time::Duration::from_millis(2));
    }
    doomed.shutdown_background();
    let killed_at = waited.elapsed();
    let (left, _) = main.block_on(blame_rows(&*meta));
    let started = Instant::now();
    let resumed = main.block_on(async {
        let (repo, _) = open().await;
        repo.blame(tip, &path).await.unwrap()
    });
    assert_eq!(resumed, want);
    println!(
        "killed after {killed_at:.2?} with {left} checkpoints written; \
         a new process resumed in {:.2?}",
        started.elapsed()
    );

    // Through the router, with a request timeout of 40% of the cold time
    // (the 1M-changeset case: 61 s against 25 s), retried at once.
    reset();
    let timeout = cold.mul_f64(0.4);
    main.block_on(async {
        let (repo, _) = open().await;
        let config = leep_server::reload::ConfigHandle::fixed(
            leep_server::reload::RuntimeConfig::fixed(None, None, None, None, None),
        );
        let app = leep_server::router_with_read_limits(
            repo,
            1 << 20,
            leep_server::limits::BodyLimits::under(1 << 20),
            timeout,
            config,
            leep_server::ReadyOptions::default(),
            ReadConcurrency {
                blame: ClassLimit::Permits(2),
                ..ReadConcurrency::off()
            },
        );
        let uri = format!("/v1/blame/{tip}?path=hot.rs");
        let started = Instant::now();
        let mut tries = Vec::new();
        loop {
            let t = Instant::now();
            let resp = app
                .clone()
                .oneshot(Request::builder().uri(&uri).body(Body::empty()).unwrap())
                .await
                .unwrap();
            let status = resp.status();
            let body = resp.into_body().collect().await.unwrap().to_bytes();
            tries.push(format!("{} in {:.2?}", status.as_u16(), t.elapsed()));
            if status == StatusCode::OK {
                let got: leep_core::api::BlameResponse = serde_json::from_slice(&body).unwrap();
                assert_eq!(got.lines, want.lines);
                break;
            }
            assert!(tries.len() < 20, "{tries:?}");
        }
        println!(
            "timeout {timeout:.2?}: answered after {:.2?}: {}",
            started.elapsed(),
            tries.join(", ")
        );
    });
    let (rows, bytes) = main.block_on(blame_rows(&*meta));
    println!("after the retried blame: {rows} blame rows ({bytes} bytes)");
}
