//! Blame on a deep linear history: metadata calls per cold blame must
//! follow the file's versions, not the depth of history.

use std::collections::BTreeMap;
use std::sync::Arc;

use bytes::Bytes;
use leep_core::memory::{MemoryBlobStore, MemoryMetaStore};
use leep_core::{Changeset, ChangesetId, FileChange, FileKind, Hash, RepoPath};
use leep_testkit::budget::{Budget, Usage};

use crate::blame::{Cuts, HEAD_STEPS, MIN_STRETCH_SPAN};
use crate::Repo;

/// Metadata reads in `u`: one statement each on a database backend.
fn reads(u: &Usage) -> u64 {
    [
        "meta.get_changeset",
        "meta.get_changesets",
        "meta.get_derived",
        "meta.get_derived_many",
        "meta.get_derived_kinds",
    ]
    .iter()
    .map(|op| u.calls(op))
    .sum()
}

/// A cold blame of a file changed `versions - 1` times, evenly, over a
/// linear history of `len` changesets that otherwise change another file:
/// the store's statements, its skiplist reads, and the answer.
pub(crate) async fn sparse_file_blame(
    len: usize,
    versions: usize,
    linear_jumps: bool,
) -> (Usage, crate::blame::CachedBlame) {
    let blobs = Arc::new(MemoryBlobStore::new());
    let meta = Arc::new(MemoryMetaStore::new());
    let r = Repo::new(blobs.clone(), meta.clone());
    let mut text = "0\n".to_string();
    let mut tip = commit(&r, None, &[("old", &text), ("busy", "0\n")]).await;
    let every = len / (versions - 1);
    for n in 1..len {
        tip = if n % every == every / 2 {
            text.push_str(&format!("{n}\n"));
            commit(&r, Some(tip), &[("old", &text)]).await
        } else {
            commit(&r, Some(tip), &[("busy", &format!("{n}\n"))]).await
        };
    }
    crate::derived::derive::<crate::derived::HistoryTree>(&r, tip)
        .await
        .unwrap();
    let budget = Budget::new(blobs, meta);
    let (b, c, _) = cold_blame(&budget, tip, "old", linear_jumps).await;
    (c, b)
}

async fn commit(r: &Repo, parent: Option<ChangesetId>, files: &[(&str, &str)]) -> ChangesetId {
    let mut changes = BTreeMap::new();
    for (p, t) in files {
        r.blobs().put(Bytes::from(t.to_string())).await.unwrap();
        changes.insert(RepoPath::new(*p).unwrap(), modify(t.as_bytes()));
    }
    r.create_changeset(Changeset {
        parents: parent.into_iter().collect(),
        author: "t".into(),
        timestamp: 1,
        message: "m".into(),
        changes,
        extra: BTreeMap::new(),
    })
    .await
    .unwrap()
}

/// A cold blame of a file with few versions on a linear history goes from
/// each change straight to the next: its statements and skiplist reads
/// follow the versions, and do not grow with the depth of history, as the
/// skiplist descent between changes did; the answer is the descent's.
#[tokio::test(flavor = "multi_thread")]
async fn a_cold_blame_on_a_deep_linear_history_reads_per_version() {
    let versions = 8;
    let mut statements = Vec::new();
    for len in [1000, 8000] {
        let (jumped, b) = sparse_file_blame(len, versions, true).await;
        let (descended, want) = sparse_file_blame(len, versions, false).await;
        assert_eq!(b, want, "{len}");
        assert_eq!(b.1.len(), versions);
        let skips =
            jumped.kind_calls(<crate::derived::Skiplist as crate::derived::DerivedKind>::NAME);
        assert!(
            skips <= 2 * versions as u64 + 2,
            "{skips} skiplist reads at {len}"
        );
        assert!(
            skips * 2 < descended.kind_calls("skiplist"),
            "{skips} skiplist reads at {len}, {} descending",
            descended.kind_calls("skiplist")
        );
        let statements_now = reads(&jumped);
        assert!(
            statements_now <= 6 * versions as u64,
            "{statements_now} statements at {len}: {jumped}"
        );
        statements.push(statements_now);
    }
    assert!(
        statements[1] <= statements[0] + 2,
        "statements grew with history: {statements:?}"
    );
}

/// A linear history of `len` changesets over a wide tree: one or two files
/// changed per changeset, `hot` taking about 1 in 20 edits and `old`
/// changed `old_versions - 1` times, evenly spread. Returns the tip and the
/// version counts of `old`, `src/m00/s00/f0.rs` and `hot`.
pub(crate) async fn deep_history(
    r: &Repo,
    len: usize,
    old_versions: usize,
) -> (ChangesetId, [usize; 3]) {
    const SRC: usize = 4096;
    let src = |i: usize| format!("src/m{:02}/s{:02}/f{}.rs", (i / 16) % 16, i % 16, i / 256);
    let mut rng: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut next = move |n: usize| {
        rng ^= rng >> 12;
        rng ^= rng << 25;
        rng ^= rng >> 27;
        (rng.wrapping_mul(0x2545_F491_4F6C_DD1D) >> 11) as usize % n
    };
    let mut texts: BTreeMap<String, String> = (0..SRC)
        .map(|i| (src(i), format!("// {}\n", src(i))))
        .collect();
    texts.insert("old/a.rs".into(), "// old\n".into());
    texts.insert("hot/h0".into(), "// hot\n".into());
    let ordinary = src(0);
    let mut versions = [1usize; 3];
    let mut changes = BTreeMap::new();
    let mut datas = Vec::new();
    for (p, t) in &texts {
        datas.push(Bytes::from(t.clone()));
        changes.insert(RepoPath::new(p.as_str()).unwrap(), modify(t.as_bytes()));
    }
    r.put_blobs(datas).await.unwrap();
    let mut tip = r
        .create_changeset(Changeset {
            parents: vec![],
            author: "t".into(),
            timestamp: 1,
            message: "root".into(),
            changes,
            extra: BTreeMap::new(),
        })
        .await
        .unwrap();
    let old_every = len / old_versions.max(1);
    for n in 1..len {
        let mut paths = Vec::new();
        if old_versions > 1 && n % old_every == old_every / 2 && versions[0] < old_versions {
            paths.push("old/a.rs".to_string());
        }
        for _ in 0..1 + next(2) {
            let p = if next(20) == 0 {
                "hot/h0".to_string()
            } else {
                src(next(SRC))
            };
            if !paths.contains(&p) {
                paths.push(p);
            }
        }
        let mut changes = BTreeMap::new();
        let mut datas = Vec::new();
        for p in paths {
            let t = texts.get_mut(&p).unwrap();
            t.push_str(&format!("change {n}\n"));
            match p.as_str() {
                "old/a.rs" => versions[0] += 1,
                "hot/h0" => versions[2] += 1,
                _ if p == ordinary => versions[1] += 1,
                _ => {}
            }
            datas.push(Bytes::from(t.clone()));
            changes.insert(RepoPath::new(p.as_str()).unwrap(), modify(t.as_bytes()));
        }
        r.put_blobs(datas).await.unwrap();
        tip = r
            .create_changeset(Changeset {
                parents: vec![tip],
                author: "t".into(),
                timestamp: 1 + n as i64,
                message: format!("change {n}"),
                changes,
                extra: BTreeMap::new(),
            })
            .await
            .unwrap();
    }
    crate::derived::derive::<crate::derived::HistoryTree>(r, tip)
        .await
        .unwrap();
    (tip, versions)
}

fn modify(data: &[u8]) -> FileChange {
    FileChange::Modify {
        content: Hash::of_blob(data),
        kind: FileKind::Regular,
        size: data.len() as u64,
    }
}

/// A cold blame (a new process, no cached blames) of `path`: the answer,
/// the reads it made and how long it took.
async fn cold_blame(
    budget: &Arc<Budget>,
    tip: ChangesetId,
    path: &str,
    linear_jumps: bool,
) -> (crate::blame::CachedBlame, Usage, std::time::Duration) {
    budget
        .inner()
        .1
        .delete_derived_kind(crate::blame::KIND)
        .await
        .unwrap();
    let cold = Repo::new(budget.clone(), budget.clone());
    budget.start();
    let started = std::time::Instant::now();
    let cuts = Cuts {
        head: HEAD_STEPS,
        min_span: MIN_STRETCH_SPAN,
        linear_jumps,
    };
    let (b, _) = cold
        .blame_walk_in(tip, &RepoPath::new(path).unwrap(), Some(cuts), true)
        .await
        .unwrap();
    let took = started.elapsed();
    (b, budget.stop(), took)
}

async fn deep_repo(
    len: usize,
    old_versions: usize,
) -> (Arc<Budget>, Repo, ChangesetId, [usize; 3]) {
    let blobs = Arc::new(MemoryBlobStore::new());
    let meta = Arc::new(MemoryMetaStore::new());
    let writer = Repo::new(blobs.clone(), meta.clone());
    let (tip, versions) = deep_history(&writer, len, old_versions).await;
    (Budget::new(blobs, meta), writer, tip, versions)
}

/// Calls per cold blame on a deep history, by kind (`LEEP_DEEP_LEN=200000
/// cargo test -p leep-derive deep_blame_calls -- --ignored --nocapture`).
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn deep_blame_calls() {
    let len: usize = std::env::var("LEEP_DEEP_LEN")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(100_000);
    let started = std::time::Instant::now();
    let (budget, _writer, tip, versions) = deep_repo(len, 18).await;
    println!("built {len} changesets in {:?}", started.elapsed());
    for (label, path, v) in [
        ("old", "old/a.rs", versions[0]),
        ("ordinary", "src/m00/s00/f0.rs", versions[1]),
        ("hot", "hot/h0", versions[2]),
    ] {
        let mut answers = Vec::new();
        for linear_jumps in [false, true] {
            let (b, c, took) = cold_blame(&budget, tip, path, linear_jumps).await;
            println!(
                "{label} ({v} versions, {} lines) linear_jumps {linear_jumps}: {} statements in {took:?}",
                b.1.len(),
                reads(&c)
            );
            answers.push(b);
            for (kind, t) in &c.kinds {
                println!("    {kind:<24} calls {:>6} keys {:>7}", t.calls, t.keys);
            }
        }
        assert_eq!(answers[0], answers[1], "{label}");
    }
}
