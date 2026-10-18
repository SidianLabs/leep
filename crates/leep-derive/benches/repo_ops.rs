//! Latency of repository operations versus repository size, on in-memory
//! stores so the numbers reflect Leep's algorithms and not a database.
//!
//! `LEEP_BENCH_LARGE=1` adds the 100k-file and 100k-changeset cases.

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use leep_core::memory::{MemoryBlobStore, MemoryMetaStore};
use leep_core::{Changeset, ChangesetId, FileChange, FileKind, RepoPath};
use leep_derive::{LogFilter, Repo};
use tokio::runtime::Runtime;

#[path = "support/wide_history.rs"]
mod wide_history;

const FILES_PER_DIR: usize = 1000;

/// Makes every changeset created by the benches unique.
static SEQ: AtomicU64 = AtomicU64::new(0);

fn large() -> bool {
    std::env::var_os("LEEP_BENCH_LARGE").is_some_and(|v| v != "0" && !v.is_empty())
}

fn sizes(default: &[usize], extra: &[usize]) -> Vec<usize> {
    let mut v = default.to_vec();
    if large() {
        v.extend_from_slice(extra);
    }
    v
}

fn new_repo() -> Repo {
    Repo::new(
        Arc::new(MemoryBlobStore::new()),
        Arc::new(MemoryMetaStore::new()),
    )
}

/// About 1000 files per leaf directory, leaves spread over three nesting levels.
fn file_path(i: usize) -> RepoPath {
    let leaf = i / FILES_PER_DIR;
    RepoPath::new(format!(
        "a{}/b{}/c{}/f{i}",
        leaf / 25,
        (leaf / 5) % 5,
        leaf % 5
    ))
    .unwrap()
}

async fn blob(repo: &Repo, data: &[u8]) -> FileChange {
    FileChange::Modify {
        content: repo
            .blobs()
            .put(Bytes::copy_from_slice(data))
            .await
            .unwrap(),
        kind: FileKind::Regular,
        size: data.len() as u64,
    }
}

fn changeset(parents: &[ChangesetId], changes: BTreeMap<RepoPath, FileChange>) -> Changeset {
    Changeset {
        parents: parents.to_vec(),
        author: "bench".into(),
        timestamp: 1,
        message: format!("c{}", SEQ.fetch_add(1, Ordering::Relaxed)),
        changes,
        extra: BTreeMap::new(),
    }
}

/// A changeset on `parent` that rewrites file `i` with fresh content.
async fn edit(repo: &Repo, parent: ChangesetId, i: usize) -> Changeset {
    let seq = SEQ.load(Ordering::Relaxed);
    let change = blob(repo, format!("edit {seq}").as_bytes()).await;
    changeset(&[parent], BTreeMap::from([(file_path(i), change)]))
}

/// A repository of `n` files, imported one directory's worth per changeset.
/// A single giant import changeset would make every child's parent lookup
/// clone `n` changes, which measures the fixture rather than the commit.
async fn big_repo(n: usize) -> (Repo, ChangesetId) {
    let repo = new_repo();
    let change = blob(&repo, b"content").await;
    let mut tip = None;
    for start in (0..n).step_by(FILES_PER_DIR) {
        let end = (start + FILES_PER_DIR).min(n);
        let changes = (start..end)
            .map(|i| (file_path(i), change.clone()))
            .collect();
        let parents: Vec<_> = tip.into_iter().collect();
        tip = Some(
            repo.create_changeset(changeset(&parents, changes))
                .await
                .unwrap(),
        );
    }
    (repo, tip.expect("n > 0"))
}

/// A linear history of `n` changesets, each touching `d{i % 10}/f`, and every
/// `rare_every`-th one also touching `rare/f`.
async fn chain_with(n: usize, rare_every: Option<usize>) -> (Repo, Vec<ChangesetId>) {
    let repo = new_repo();
    let mut ids = Vec::with_capacity(n);
    for i in 0..n {
        let change = blob(&repo, format!("v{i}").as_bytes()).await;
        let path = RepoPath::new(format!("d{}/f", i % 10)).unwrap();
        let mut changes = BTreeMap::from([(path, change.clone())]);
        if rare_every.is_some_and(|k| i % k == 0) {
            changes.insert(RepoPath::new("rare/f").unwrap(), change);
        }
        let parents: Vec<_> = ids.last().copied().into_iter().collect();
        ids.push(
            repo.create_changeset(changeset(&parents, changes))
                .await
                .unwrap(),
        );
    }
    (repo, ids)
}

/// A linear history of `n` changesets, each touching `d{i % 10}/f`.
async fn chain(n: usize) -> (Repo, Vec<ChangesetId>) {
    chain_with(n, None).await
}

/// Runs `prepare` untimed before each iteration and times only `run`.
fn timed<P, R, Fp, Fr, T>(rt: &Runtime, b: &mut criterion::Bencher, prepare: P, run: R)
where
    P: Fn() -> Fp,
    R: Fn(T) -> Fr,
    Fp: Future<Output = T>,
    Fr: Future<Output = ()>,
{
    b.to_async(rt).iter_custom(|iters| {
        let (prepare, run) = (&prepare, &run);
        async move {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let input = prepare().await;
                let start = Instant::now();
                run(input).await;
                total += start.elapsed();
            }
            total
        }
    });
}

fn create_changeset(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut g = c.benchmark_group("create_changeset_one_file");
    g.sample_size(20).measurement_time(Duration::from_secs(3));
    for n in sizes(&[1_000, 10_000], &[100_000]) {
        let (repo, root) = rt.block_on(big_repo(n));
        g.bench_function(BenchmarkId::from_parameter(n), |b| {
            timed(
                &rt,
                b,
                || {
                    let repo = repo.clone();
                    async move {
                        let i = SEQ.load(Ordering::Relaxed) as usize * 7919 % n;
                        edit(&repo, root, i).await
                    }
                },
                |cs| {
                    let repo = repo.clone();
                    async move {
                        repo.create_changeset(cs).await.unwrap();
                    }
                },
            )
        });
    }
    g.finish();
}

fn land(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut g = c.benchmark_group("land");
    g.sample_size(20).measurement_time(Duration::from_secs(3));
    for n in sizes(&[10_000], &[100_000]) {
        let (repo, root) = rt.block_on(big_repo(n));

        g.bench_function(BenchmarkId::new("fast_forward", n), |b| {
            rt.block_on(repo.meta().cas_ref("ff", None, Some(root)))
                .ok();
            timed(
                &rt,
                b,
                || {
                    let repo = repo.clone();
                    async move {
                        // Reset the ref so every iteration fast-forwards from `root`.
                        let cur = repo.meta().get_ref("ff").await.unwrap();
                        repo.meta().cas_ref("ff", cur, Some(root)).await.unwrap();
                        let i = SEQ.load(Ordering::Relaxed) as usize * 7919 % n;
                        let cs = edit(&repo, root, i).await;
                        repo.create_changeset(cs).await.unwrap()
                    }
                },
                |cs| {
                    let repo = repo.clone();
                    async move {
                        repo.land(cs, "ff").await.unwrap();
                    }
                },
            )
        });

        // The head moves by `moved` unrelated changesets touching other files
        // in the same directory as the stack being landed.
        for moved in [1usize, 10, 100] {
            if n > 10_000 && moved == 1 {
                continue;
            }
            let head = rt.block_on(async {
                let mut head = root;
                for k in 0..moved {
                    let cs = edit(&repo, head, 500 + k).await;
                    head = repo.create_changeset(cs).await.unwrap();
                }
                head
            });
            let name = format!("replay_head_moved_{moved}");
            let ref_name = format!("replay{moved}");
            g.bench_function(BenchmarkId::new(name, n), |b| {
                rt.block_on(repo.meta().cas_ref(&ref_name, None, Some(head)))
                    .ok();
                timed(
                    &rt,
                    b,
                    || {
                        let (repo, ref_name) = (repo.clone(), ref_name.clone());
                        async move {
                            let cur = repo.meta().get_ref(&ref_name).await.unwrap();
                            repo.meta()
                                .cas_ref(&ref_name, cur, Some(head))
                                .await
                                .unwrap();
                            let i = SEQ.load(Ordering::Relaxed) as usize % 400;
                            let cs = edit(&repo, root, i).await;
                            repo.create_changeset(cs).await.unwrap()
                        }
                    },
                    |cs| {
                        let (repo, ref_name) = (repo.clone(), ref_name.clone());
                        async move {
                            repo.land(cs, &ref_name).await.unwrap();
                        }
                    },
                )
            });
        }
    }
    g.finish();
}

fn diff(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut g = c.benchmark_group("diff_one_file");
    g.sample_size(20).measurement_time(Duration::from_secs(3));
    for n in sizes(&[10_000], &[100_000]) {
        let (repo, root) = rt.block_on(big_repo(n));
        let child = rt.block_on(async {
            let cs = edit(&repo, root, n / 2).await;
            repo.create_changeset(cs).await.unwrap()
        });
        g.bench_function(BenchmarkId::from_parameter(n), |b| {
            b.to_async(&rt).iter(|| async {
                let d = repo.diff(Some(root), child, "").await.unwrap();
                assert_eq!(d.len(), 1);
            })
        });
    }
    g.finish();
}

fn ancestry(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let max = *sizes(&[10_000], &[100_000]).last().unwrap();
    let (repo, ids) = rt.block_on(chain(max));
    let mut g = c.benchmark_group("ancestry");
    g.sample_size(20).measurement_time(Duration::from_secs(3));
    for n in sizes(&[1_000, 10_000], &[100_000]) {
        // Prefixes of one history are histories of their own.
        let (first, tip, mid) = (ids[0], ids[n - 1], ids[n / 2]);
        g.bench_function(BenchmarkId::new("is_ancestor_root_of_tip", n), |b| {
            b.to_async(&rt)
                .iter(|| async { assert!(repo.is_ancestor(first, tip).await.unwrap()) })
        });
        let side = rt.block_on(async {
            let cs = edit(&repo, mid, 0).await;
            repo.create_changeset(cs).await.unwrap()
        });
        g.bench_function(
            BenchmarkId::new("merge_base_tip_vs_side_from_mid", n),
            |b| {
                b.to_async(&rt).iter(|| async {
                    assert_eq!(repo.merge_base(tip, side).await.unwrap(), Some(mid))
                })
            },
        );
    }
    g.finish();
}

fn log(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    // `rare/f` changes in 10 of the 10k changesets, `d0/f` in every 10th.
    let (repo, ids) = rt.block_on(chain_with(10_000, Some(1_000)));
    let tip = *ids.last().unwrap();
    // The first indexed query derives the index for all of history; time it
    // once, then keep it out of the per-query numbers.
    let started = Instant::now();
    rt.block_on(repo.log(tip, Some("d0/f"), 1)).unwrap();
    eprintln!(
        "history index: derived for 10k changesets in {:?}",
        started.elapsed()
    );
    let mut g = c.benchmark_group("log_path_filter_10k_history");
    g.sample_size(10).measurement_time(Duration::from_secs(3));
    // Limit 10 on `d0/f` needs ~100 changesets of history; limit 1000 wants
    // 1000 matches, so the scan reads all 10k. `rare/f` matches 10 and `nope`
    // none, which also makes the scan read everything. `index` answers from
    // the path-history index, `scan` is the old walk over every ancestor.
    for (name, path, limit) in [
        ("limit_10", "d0/f", 10),
        ("limit_1000", "d0/f", 1000),
        ("rare_path", "rare/f", 1000),
        ("no_match", "nope", 1000),
    ] {
        g.bench_function(format!("index/{name}"), |b| {
            b.to_async(&rt)
                .iter(|| async { repo.log(tip, Some(path), limit).await.unwrap() })
        });
        g.bench_function(format!("scan/{name}"), |b| {
            b.to_async(&rt).iter(|| async {
                repo.log_scan(tip, Some(path), limit, &LogFilter::default())
                    .await
                    .unwrap()
            })
        });
    }
    g.finish();

    // Without a path the walk reads every changeset it yields; an author
    // filter that matches nothing reads all 10k.
    let mut g = c.benchmark_group("log_10k_history");
    g.sample_size(10).measurement_time(Duration::from_secs(3));
    for limit in [100, 10_000] {
        g.bench_function(format!("all/limit_{limit}"), |b| {
            b.to_async(&rt).iter(|| async {
                assert_eq!(repo.log(tip, None, limit).await.unwrap().len(), limit)
            })
        });
    }
    let nobody = LogFilter {
        author: Some("nobody".into()),
        extra: Vec::new(),
    };
    g.bench_function("author_no_match", |b| {
        b.to_async(&rt).iter(|| async {
            let found = repo.log_filtered(tip, None, 10, &nobody).await.unwrap();
            assert!(found.is_empty())
        })
    });
    g.finish();
}

fn last_change(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let (repo, ids) = rt.block_on(chain_with(10_000, Some(1_000)));
    let tip = *ids.last().unwrap();
    rt.block_on(repo.last_change(tip, "d0/f")).unwrap();
    let mut g = c.benchmark_group("last_change_10k_history");
    g.sample_size(20).measurement_time(Duration::from_secs(3));
    for path in ["d0/f", "rare/f", "rare"] {
        g.bench_function(path, |b| {
            b.to_async(&rt)
                .iter(|| async { repo.last_change(tip, path).await.unwrap() })
        });
    }
    g.finish();
}

/// Cost of deriving the index for a fresh history, per changeset.
fn derive_history_index(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut g = c.benchmark_group("derive_history_index");
    g.sample_size(10).measurement_time(Duration::from_secs(3));
    g.bench_function("1000_changesets", |b| {
        timed(
            &rt,
            b,
            || async {
                let (repo, ids) = chain_with(1_000, Some(100)).await;
                (repo, *ids.last().unwrap())
            },
            |(repo, tip)| async move {
                repo.log(tip, Some("d0/f"), 1).await.unwrap();
            },
        )
    });
    // Late-Linux shape: directories of 1,000 to 2,000 files, subsystem
    // branches with topic merges and merges of mainline back into them.
    g.bench_function("wide_merges_300_changesets", |b| {
        timed(
            &rt,
            b,
            || async {
                let repo = new_repo();
                let tip = wide_history::build(&repo, &wide_history::Spec::small(300)).await;
                // A fresh `Repo` on the same stores: nothing cached.
                (Repo::new(repo.blobs().clone(), repo.meta().clone()), tip)
            },
            |(repo, tip)| async move {
                repo.log(tip, Some("wide/w00"), 1).await.unwrap();
            },
        )
    });
    // One merge of a two-commit branch forked 200 changesets back, into a
    // history of 3,000 with a change behind the first parent older than the
    // fork (Linux's `work.thaw` shape, see "Path history on Linux"). The rest
    // is derived first; each iteration derives a fresh copy of the merge on
    // a `Repo` with nothing cached. Its cost should follow the branch and
    // how far mainline moved since the fork, not the length of history.
    let (stores, head) = rt.block_on(async {
        let repo = new_repo();
        let spec = wide_history::Spec::small(3000).with_old_branch(200, 2);
        let head = wide_history::build(&repo, &spec).await;
        let cs = repo.get_changeset(head).await.unwrap();
        for p in &cs.parents {
            repo.log(*p, Some("cherry"), 1).await.unwrap();
        }
        (repo, head)
    });
    g.bench_function("old_base_merge_3000_changesets", |b| {
        timed(
            &rt,
            b,
            || async {
                let mut cs = stores.get_changeset(head).await.unwrap();
                cs.message = format!("again {}", SEQ.fetch_add(1, Ordering::Relaxed));
                let merge = stores.create_changeset(cs).await.unwrap();
                let repo = Repo::new(stores.blobs().clone(), stores.meta().clone());
                (repo, merge)
            },
            |(repo, merge)| async move {
                repo.log(merge, Some("cherry"), 1).await.unwrap();
            },
        )
    });
    g.finish();
}

/// Pure pairing of n deleted and n added text files of about 30 lines. In the
/// similar case every added file is an edit of one deleted file, so all n x n
/// pairs are scored (the worst case for the default 1000 x 1000 limit).
fn detect_renames(c: &mut Criterion) {
    use leep_core::api::FileInfo;
    use leep_core::rename::{detect_renames, RenameOptions};
    use leep_core::Hash;
    use std::collections::HashMap;

    let body = |i: usize, edit: bool| -> Vec<u8> {
        let mut t: String = (0..30).map(|l| format!("file {i} line {l}\n")).collect();
        if edit {
            t.push_str("added line\n");
        }
        t.into_bytes()
    };
    let info = |path: String, b: &[u8]| FileInfo {
        path: RepoPath::new(path).unwrap(),
        content: Hash::of_blob(b),
        kind: FileKind::Regular,
        size: b.len() as u64,
    };
    let mut g = c.benchmark_group("detect_renames");
    g.sample_size(10);
    for (name, edit) in [("exact", false), ("similar", true)] {
        let n = 1000;
        let mut bodies = HashMap::new();
        let (mut del, mut add) = (Vec::new(), Vec::new());
        for i in 0..n {
            let (old, new) = (body(i, false), body(i, edit));
            del.push(info(format!("old/f{i}"), &old));
            add.push(info(format!("new/f{i}"), &new));
            bodies.insert(Hash::of_blob(&old), old);
            bodies.insert(Hash::of_blob(&new), new);
        }
        g.bench_function(BenchmarkId::new(name, "1000x1000"), |b| {
            b.iter(|| {
                let d = detect_renames(del.clone(), add.clone(), RenameOptions::default(), |h| {
                    bodies.get(h).cloned()
                });
                assert_eq!(d.renames.len(), n);
            })
        });
    }
    g.finish();
}

criterion_group!(
    benches,
    detect_renames,
    create_changeset,
    land,
    diff,
    ancestry,
    log,
    last_change,
    derive_history_index
);
criterion_main!(benches);
