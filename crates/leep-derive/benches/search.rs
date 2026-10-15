//! Code search: indexed against a full scan of the same revision, on a
//! synthetic repository in memory stores (see docs/PERFORMANCE.md).
//!
//! The repository has 400 leaf directories (`m{0..19}/s{0..4}/p{0..3}`), each
//! with its own vocabulary of 300 pseudo-words drawn from a shared list, like
//! modules that use their own identifiers. `LEEP_BENCH_SEARCH_FILES` sets the
//! file count (default 20000, rounded down to a multiple of 400).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use criterion::{criterion_group, criterion_main, Criterion};
use leep_core::memory::{MemoryBlobStore, MemoryMetaStore};
use leep_core::{BlobStore, Changeset, ChangesetId, FileChange, FileKind, RepoPath};
use leep_derive::search::{BLOB_KIND, TREE_KIND};
use leep_derive::{Repo, SearchQuery};
use tokio::runtime::Runtime;

const DIRS: usize = 400;
const VOCAB_PER_DIR: usize = 300;
const GLOBAL_WORDS: usize = 30_000;
const LINES_PER_FILE: usize = 40;

struct Lcg(u64);

impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0 >> 33
    }

    fn below(&mut self, n: usize) -> usize {
        self.next() as usize % n
    }
}

fn word(rng: &mut Lcg) -> String {
    (0..4 + rng.below(7))
        .map(|_| (b'a' + rng.below(26) as u8) as char)
        .collect()
}

struct Corpus {
    files: Vec<(String, Vec<u8>)>,
    /// A word used by many files of one directory.
    common: String,
    /// A token planted in exactly three files.
    needle: &'static str,
}

fn corpus(n: usize) -> Corpus {
    let mut rng = Lcg(42);
    let global: Vec<String> = (0..GLOBAL_WORDS).map(|_| word(&mut rng)).collect();
    let per_dir = (n / DIRS).max(1);
    let mut files = Vec::new();
    let mut common = String::new();
    for d in 0..DIRS {
        let vocab: Vec<&String> = (0..VOCAB_PER_DIR)
            .map(|_| &global[rng.below(GLOBAL_WORDS)])
            .collect();
        if d == 123 {
            common = vocab[0].clone();
        }
        let dir = format!("m{}/s{}/p{}", d / 20, (d / 4) % 5, d % 4);
        for f in 0..per_dir {
            let mut text = String::new();
            for l in 0..LINES_PER_FILE {
                let kw = ["fn", "let", "return", "use", "pub fn"][rng.below(5)];
                let parts: Vec<&str> = (0..3 + rng.below(4))
                    .map(|_| vocab[rng.below(VOCAB_PER_DIR)].as_str())
                    .collect();
                text.push_str(&format!("{kw} {}({l});\n", parts.join("_")));
            }
            if (d, f) == (7, 3) || (d, f) == (190, 11) || (d, f) == (333, 0) {
                text.push_str("// quux_needle_77 lives here\n");
            }
            files.push((format!("{dir}/f{f}.rs"), text.into_bytes()));
        }
    }
    Corpus {
        files,
        common,
        needle: "quux_needle_77",
    }
}

async fn import(blobs: Arc<MemoryBlobStore>, files: &[(String, Vec<u8>)]) -> (Repo, ChangesetId) {
    let mut changes = BTreeMap::new();
    for (path, data) in files {
        let content = blobs.put(Bytes::copy_from_slice(data)).await.unwrap();
        changes.insert(
            RepoPath::new(path.as_str()).unwrap(),
            FileChange::Modify {
                content,
                kind: FileKind::Regular,
                size: data.len() as u64,
            },
        );
    }
    let repo = Repo::new(blobs, Arc::new(MemoryMetaStore::new()));
    let id = repo
        .create_changeset(Changeset {
            parents: vec![],
            author: "bench".into(),
            timestamp: 1,
            message: "import".into(),
            changes,
            extra: BTreeMap::new(),
        })
        .await
        .unwrap();
    (repo, id)
}

/// Bytes the index occupies in the meta store, for docs/PERFORMANCE.md.
fn index_size(rt: &Runtime, repo: &Repo, id: ChangesetId, files: &[(String, Vec<u8>)]) {
    rt.block_on(async {
        let meta = repo.meta();
        let mut blob_bytes = 0;
        for (_, data) in files {
            let key = leep_core::Hash::of_blob(data);
            blob_bytes += meta.get_derived(BLOB_KIND, &key).await.unwrap().unwrap().len();
        }
        let (mut tree_bytes, mut dense, mut trees) = (0, 0, 0);
        let mut stack = vec![repo.root_tree(id).await.unwrap()];
        while let Some(t) = stack.pop() {
            let row = meta.get_derived(TREE_KIND, &t).await.unwrap().unwrap();
            tree_bytes += row.len();
            dense += (row[0] == 1) as usize;
            trees += 1;
            for e in repo.read_tree(t).await.unwrap().entries.values() {
                if let leep_core::TreeEntry::Dir { tree } = e {
                    stack.push(*tree);
                }
            }
        }
        eprintln!(
            "index size: blob rows {:.1} MiB ({} B each on average), tree rows {:.1} MiB ({trees} rows, {dense} dense)",
            blob_bytes as f64 / (1 << 20) as f64,
            blob_bytes / files.len(),
            tree_bytes as f64 / (1 << 20) as f64,
        );
    });
}

fn search(c: &mut Criterion) {
    let n: usize = std::env::var("LEEP_BENCH_SEARCH_FILES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(20_000);
    let rt = Runtime::new().unwrap();
    let corpus = corpus(n);
    let total: usize = corpus.files.iter().map(|(_, d)| d.len()).sum();
    eprintln!(
        "corpus: {} files, {:.1} MiB",
        corpus.files.len(),
        total as f64 / (1 << 20) as f64
    );
    let blobs = Arc::new(MemoryBlobStore::new());
    let (repo, id) = rt.block_on(import(blobs.clone(), &corpus.files));

    let started = Instant::now();
    let report = rt.block_on(repo.derive_search(id)).unwrap();
    eprintln!(
        "cold derive: {:.2?} for {} blob rows and {} tree rows",
        started.elapsed(),
        report.blob_rows,
        report.tree_rows
    );

    index_size(&rt, &repo, id, &corpus.files);

    let regex = |p: String| SearchQuery {
        regex: true,
        ..SearchQuery::new(p)
    };
    let cases: Vec<(&str, SearchQuery)> = vec![
        ("absent_word", SearchQuery::new("qzjxkvw")),
        ("needle_in_3_files", SearchQuery::new(corpus.needle)),
        ("one_dir_word", SearchQuery::new(corpus.common.clone())),
        (
            "one_dir_word_ignore_case",
            SearchQuery {
                case_insensitive: true,
                ..SearchQuery::new(corpus.common.to_uppercase())
            },
        ),
        (
            "regex_literal_prefix",
            regex(format!("{}_[a-z]+\\(\\d+\\)", corpus.common)),
        ),
        (
            "regex_alternation",
            regex(format!("({}|quux_needle)", corpus.common)),
        ),
        ("common_word_limit_100", SearchQuery::new("return")),
        // Every file has the trigrams of `(39);`, so the index prunes nothing.
        (
            "regex_unselective_trigrams",
            regex("^[a-z]+_[a-z]+\\(39\\);$".into()),
        ),
        // No literal of three bytes: scanned without the index.
        (
            "regex_no_trigram",
            regex("^[a-z]+_[a-z]+\\(\\d\\d\\);$".into()),
        ),
    ];

    let mut g = c.benchmark_group("search");
    g.sample_size(10).measurement_time(Duration::from_secs(5));
    for (name, q) in cases {
        let scan = SearchQuery {
            use_index: false,
            ..q.clone()
        };
        let (a, b) = rt.block_on(async {
            (
                repo.search(id, &q).await.unwrap(),
                repo.search(id, &scan).await.unwrap(),
            )
        });
        assert_eq!(a.matches, b.matches, "{name}: index and scan disagree");
        eprintln!(
            "{name}: {} matches, truncated={}, indexed={}, dirs_pruned={}, files_pruned={}, scanned={}",
            a.matches.len(),
            a.truncated,
            a.stats.indexed,
            a.stats.dirs_pruned,
            a.stats.files_pruned,
            a.stats.files_scanned
        );
        g.bench_function(format!("{name}/indexed"), |bch| {
            bch.to_async(&rt)
                .iter(|| async { repo.search(id, &q).await.unwrap() })
        });
        g.bench_function(format!("{name}/scan"), |bch| {
            bch.to_async(&rt)
                .iter(|| async { repo.search(id, &scan).await.unwrap() })
        });
    }
    g.finish();

    // Rebuilding the index of a revision with an empty meta store.
    let mut g = c.benchmark_group("search_derive");
    g.sample_size(10).measurement_time(Duration::from_secs(5));
    g.bench_function("cold_index", |bch| {
        bch.to_async(&rt).iter_custom(|iters| {
            let (blobs, files) = (blobs.clone(), &corpus.files);
            async move {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    let (repo, id) = import(blobs.clone(), files).await;
                    let start = Instant::now();
                    repo.derive_search(id).await.unwrap();
                    total += start.elapsed();
                }
                total
            }
        })
    });
    g.finish();
}

criterion_group!(benches, search);
criterion_main!(benches);
