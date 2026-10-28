//! Blame walked in stretches (`Repo::blame_stretches`) against the reference
//! walk over every first-parent ancestor.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use leep_core::memory::{MemoryBlobStore, MemoryMetaStore};
use leep_core::{
    Changeset, ChangesetId, Error, FileChange, FileKind, Hash, MetaStore, RefLogEntry,
    RefUpdateContext, RepoPath,
};

use crate::blame::Cuts;
use crate::Repo;

fn rp(s: &str) -> RepoPath {
    RepoPath::new(s).unwrap()
}

/// Counts reads, and can delay each one or fail one in so many.
#[derive(Default)]
struct Slow {
    inner: MemoryMetaStore,
    latency_us: AtomicU64,
    reads: AtomicUsize,
    /// Fail every read whose number is a multiple of this (0: none).
    fail_every: AtomicUsize,
}

impl Slow {
    async fn read(&self) -> leep_core::Result<()> {
        let n = self.reads.fetch_add(1, Ordering::Relaxed) + 1;
        let us = self.latency_us.load(Ordering::Relaxed);
        if us > 0 {
            tokio::time::sleep(Duration::from_micros(us)).await;
        }
        let every = self.fail_every.load(Ordering::Relaxed);
        if every > 0 && n.is_multiple_of(every) {
            return Err(Error::Storage(format!("read {n} failed")));
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl MetaStore for Slow {
    async fn put_changeset(&self, cs: &Changeset) -> leep_core::Result<ChangesetId> {
        self.inner.put_changeset(cs).await
    }
    async fn get_changeset(&self, id: &ChangesetId) -> leep_core::Result<Option<Changeset>> {
        self.read().await?;
        self.inner.get_changeset(id).await
    }
    async fn get_changesets(
        &self,
        ids: &[ChangesetId],
    ) -> leep_core::Result<Vec<Option<Changeset>>> {
        self.read().await?;
        self.inner.get_changesets(ids).await
    }
    async fn list_changesets(
        &self,
        after: Option<ChangesetId>,
        limit: usize,
    ) -> leep_core::Result<Vec<ChangesetId>> {
        self.inner.list_changesets(after, limit).await
    }
    async fn get_ref(&self, name: &str) -> leep_core::Result<Option<ChangesetId>> {
        self.inner.get_ref(name).await
    }
    async fn cas_ref_logged(
        &self,
        name: &str,
        expected: Option<ChangesetId>,
        new: Option<ChangesetId>,
        ctx: &RefUpdateContext,
    ) -> leep_core::Result<bool> {
        self.inner.cas_ref_logged(name, expected, new, ctx).await
    }
    async fn ref_log(
        &self,
        name: Option<&str>,
        before_seq: Option<u64>,
        limit: usize,
    ) -> leep_core::Result<Vec<RefLogEntry>> {
        self.inner.ref_log(name, before_seq, limit).await
    }
    async fn list_refs(&self) -> leep_core::Result<BTreeMap<String, ChangesetId>> {
        self.inner.list_refs().await
    }
    async fn get_derived(&self, kind: &str, key: &Hash) -> leep_core::Result<Option<Bytes>> {
        self.read().await?;
        self.inner.get_derived(kind, key).await
    }
    async fn get_derived_many(
        &self,
        kind: &str,
        keys: &[Hash],
    ) -> leep_core::Result<Vec<Option<Bytes>>> {
        self.read().await?;
        self.inner.get_derived_many(kind, keys).await
    }
    async fn put_derived(&self, kind: &str, key: &Hash, value: Bytes) -> leep_core::Result<()> {
        self.inner.put_derived(kind, key, value).await
    }
    async fn delete_derived_kind(&self, kind: &str) -> leep_core::Result<u64> {
        self.inner.delete_derived_kind(kind).await
    }
}

fn slow_repo() -> (Repo, Arc<Slow>) {
    let meta = Arc::new(Slow::default());
    (
        Repo::new(Arc::new(MemoryBlobStore::new()), meta.clone()),
        meta,
    )
}

/// `Some(bytes)` writes a file, `None` deletes it.
async fn commit_bytes(
    r: &Repo,
    parents: &[ChangesetId],
    files: &[(&str, Option<&[u8]>)],
) -> ChangesetId {
    let mut changes = BTreeMap::new();
    for (p, data) in files {
        let ch = match data {
            Some(d) => FileChange::Modify {
                content: r.blobs().put(Bytes::copy_from_slice(d)).await.unwrap(),
                kind: FileKind::Regular,
                size: d.len() as u64,
            },
            None => FileChange::Delete,
        };
        changes.insert(rp(p), ch);
    }
    r.create_changeset(Changeset {
        parents: parents.to_vec(),
        author: "t".into(),
        timestamp: 1,
        message: "m".into(),
        changes,
        extra: BTreeMap::new(),
    })
    .await
    .unwrap()
}

async fn commit(r: &Repo, parents: &[ChangesetId], files: &[(&str, Option<&str>)]) -> ChangesetId {
    let files: Vec<(&str, Option<&[u8]>)> = files
        .iter()
        .map(|(p, t)| (*p, t.map(str::as_bytes)))
        .collect();
    commit_bytes(r, parents, &files).await
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

/// The same answer, or the same kind of error, from the reference walk and
/// from stretches of at least `span` generations below `span` steps from
/// the tip (at least one).
async fn agree(r: &Repo, tip: ChangesetId, path: &str, span: u64, cache: bool) -> bool {
    let cuts = Cuts {
        head: span as usize,
        min_span: span,
    };
    let full = r.blame_walk_in(tip, &rp(path), None, false).await;
    let fast = r.blame_walk_in(tip, &rp(path), Some(cuts), cache).await;
    match (full, fast) {
        (Ok(a), Ok(b)) => {
            assert_eq!(a.0, b.0, "{path} at {tip}, stretches of {span}");
            true
        }
        (Err(Error::NotFound(_)), Err(Error::NotFound(_)))
        | (Err(Error::Invalid(_)), Err(Error::Invalid(_))) => false,
        (a, b) => panic!("{path} at {tip}, stretches of {span}: {a:?} vs {b:?}"),
    }
}

/// Many short stretches give the reference walk's answer over histories with
/// edits, reverts, deletes and re-creations, renames (blame does not follow
/// them), binary versions, and merges that bring a side branch's version of
/// a file, delete it or keep the first parent's.
#[tokio::test]
async fn stretches_agree_with_a_full_walk() {
    let text = |tag: usize| -> String { (0..6).map(|i| format!("line {i} v{tag}\n")).collect() };
    let names = ["a", "b", "c", "dir/d", "dir/sub/e"];
    for seed in 1..=5u64 {
        let r = Repo::new(
            Arc::new(MemoryBlobStore::new()),
            Arc::new(MemoryMetaStore::new()),
        );
        let mut rng = Lcg(seed);
        let mut live: BTreeMap<&str, Vec<u8>> = BTreeMap::new();
        let mut files: Vec<(&str, Option<&[u8]>)> = Vec::new();
        let t0 = text(0);
        for n in names {
            live.insert(n, t0.clone().into_bytes());
            files.push((n, Some(t0.as_bytes())));
        }
        let mut main = commit_bytes(&r, &[], &files).await;
        let mut all = vec![main];
        for step in 1..=160usize {
            let n = names[rng.below(names.len())];
            match rng.below(14) {
                0..=4 => {
                    // Edit (or create again) the file, keeping most lines.
                    let old = live
                        .get(n)
                        .map(|t| String::from_utf8_lossy(t).into_owned())
                        .unwrap_or_default();
                    let mut lines: Vec<&str> = old.split_inclusive('\n').collect();
                    let edit = format!("edit {step}\n");
                    lines.insert(rng.below(lines.len() + 1), &edit);
                    let t: String = lines.concat();
                    live.insert(n, t.clone().into_bytes());
                    main = commit(&r, &[main], &[(n, Some(&t))]).await;
                }
                5 => {
                    let gone = live.remove(n).is_some();
                    let new = (!gone).then(|| text(step));
                    if let Some(t) = &new {
                        live.insert(n, t.clone().into_bytes());
                    }
                    main = commit(&r, &[main], &[(n, new.as_deref())]).await;
                }
                6 => {
                    // Rename `n` to another name, replacing what was there.
                    let to = names[rng.below(names.len())];
                    if let (Some(t), true) = (live.get(n).cloned(), to != n) {
                        live.remove(n);
                        live.insert(to, t.clone());
                        main = commit_bytes(&r, &[main], &[(n, None), (to, Some(&t))]).await;
                    }
                }
                7 => {
                    // A binary version, later replaced by text again.
                    let bin = [0xff, 0xfe, step as u8, 0];
                    live.insert(n, bin.to_vec());
                    main = commit_bytes(&r, &[main], &[(n, Some(&bin))]).await;
                }
                8..=10 => {
                    // A side branch from an earlier changeset, merged back
                    // with its version of a file, a deletion, or nothing.
                    let base = all[rng.below(all.len())];
                    let mut side = base;
                    for k in 0..1 + rng.below(4) {
                        let m = names[rng.below(names.len())];
                        side = commit(&r, &[side], &[(m, Some(&text(1000 + step * 10 + k)))]).await;
                        all.push(side);
                    }
                    let m = names[rng.below(names.len())];
                    let change: Option<String> = match rng.below(3) {
                        0 => r
                            .read_file(side, &rp(m))
                            .await
                            .unwrap()
                            .map(|(_, d)| String::from_utf8_lossy(&d).into_owned()),
                        1 => None,
                        _ => Some(text(2000 + step)),
                    };
                    let absent = r.read_file(main, &rp(m)).await.unwrap().is_none();
                    let files: Vec<(&str, Option<&str>)> =
                        if rng.below(4) == 0 || (change.is_none() && absent) {
                            Vec::new()
                        } else {
                            vec![(m, change.as_deref())]
                        };
                    main = commit(&r, &[main, side], &files).await;
                    // Keep `live` in step with what the merge holds.
                    for n in names {
                        match r.read_file(main, &rp(n)).await.unwrap() {
                            Some((_, d)) => {
                                live.insert(n, d.to_vec());
                            }
                            None => {
                                live.remove(n);
                            }
                        }
                    }
                }
                _ => {
                    main = commit(&r, &[main], &[(&format!("e{}", step % 7), Some("x\n"))]).await;
                }
            }
            all.push(main);
        }
        let mut compared = 0;
        for (i, tip) in all.iter().enumerate() {
            if i % 5 != 0 && *tip != main {
                continue;
            }
            for n in names {
                for span in [1, 3, 8] {
                    if agree(&r, *tip, n, span, false).await {
                        compared += 1;
                    }
                }
            }
        }
        assert!(compared > 150, "seed {seed} compared only {compared}");
        // With cached blames at some earlier changesets, the walk stops at
        // the newest cached changer, in whichever stretch it lies.
        for round in 0..6 {
            r.meta().delete_derived_kind("blame").await.unwrap();
            for _ in 0..1 + round {
                let at = all[rng.below(all.len())];
                for n in names {
                    let _ = r.blame(at, &rp(n)).await;
                }
            }
            for n in names {
                agree(&r, main, n, 1 + round as u64, true).await;
            }
        }
    }
}

/// A file deleted and created again between two stretch cuts: the walk
/// ends at the re-creation, and the stretches below, whose tops are before
/// the file first existed, end at once (`absent`) or meet only the old
/// file's history, which must not reach the answer. Every cut placement
/// from one-generation stretches up agrees with the reference walk, and no
/// line is blamed on a changeset before the re-creation.
#[tokio::test]
async fn a_file_recreated_between_cuts_agrees_with_a_full_walk() {
    for same_text in [false, true] {
        let r = Repo::new(
            Arc::new(MemoryBlobStore::new()),
            Arc::new(MemoryMetaStore::new()),
        );
        let mut tip = commit(&r, &[], &[("other", Some("0\n"))]).await;
        let mut ids = vec![tip];
        let step = |r: Repo, tip: ChangesetId, files: Vec<(String, Option<String>)>| async move {
            let files: Vec<(&str, Option<&str>)> = files
                .iter()
                .map(|(p, t)| (p.as_str(), t.as_deref()))
                .collect();
            commit(&r, &[tip], &files).await
        };
        let other = |i: usize| vec![("other".to_string(), Some(format!("{i}\n")))];
        let f = |t: &str| vec![("f".to_string(), Some(t.to_string()))];
        for i in 1..10 {
            tip = step(r.clone(), tip, other(i)).await;
            ids.push(tip);
        }
        let old = "a\nb\nc\n";
        tip = step(r.clone(), tip, f(old)).await;
        ids.push(tip);
        tip = step(r.clone(), tip, f("a\nb\nc\nd\n")).await;
        ids.push(tip);
        for i in 12..17 {
            tip = step(r.clone(), tip, other(i)).await;
            ids.push(tip);
        }
        tip = step(r.clone(), tip, vec![("f".to_string(), None)]).await;
        ids.push(tip);
        for i in 18..21 {
            tip = step(r.clone(), tip, other(i)).await;
            ids.push(tip);
        }
        let again = if same_text {
            "a\nb\nc\nd\n"
        } else {
            "a\nx\nc\n"
        };
        tip = step(r.clone(), tip, f(again)).await;
        let recreated = ids.len();
        ids.push(tip);
        tip = step(r.clone(), tip, f(&format!("{again}e\n"))).await;
        ids.push(tip);
        for i in 23..40 {
            tip = step(r.clone(), tip, other(i)).await;
            ids.push(tip);
        }
        let mut compared = 0;
        for at in [tip, ids[recreated + 1], ids[recreated]] {
            for span in [1, 2, 3, 4, 5, 7, 9, 13] {
                assert!(agree(&r, at, "f", span, false).await);
                compared += 1;
            }
            let (b, _) = r.blame_walk(at, &rp("f"), false, false).await.unwrap();
            let before: Vec<ChangesetId> = ids[..recreated].to_vec();
            assert!(
                b.1.iter().all(|l| !before.contains(&l.changeset)),
                "same text {same_text}: blamed before the re-creation"
            );
        }
        // While it is deleted, there is nothing to blame either way.
        for span in [1, 3] {
            assert!(!agree(&r, ids[recreated - 1], "f", span, false).await);
        }
        assert_eq!(compared, 24);
    }
}

/// The first blame of a file changed on most of a long history walks the
/// stretches at once and gathers their reads: it makes fewer reads than
/// there are changes, and with every read delayed it takes a fraction of
/// the time one read per change, in sequence, would. Short cuts stand in
/// for a long history's (a 32-step head is most of this one).
#[tokio::test]
async fn first_blame_of_a_busy_file_is_not_one_read_per_change() {
    let (r, meta) = slow_repo();
    let mut text = String::new();
    let mut tip = commit(&r, &[], &[("f", Some("0\n")), ("g", Some("0\n"))]).await;
    for i in 0..1200 {
        if i % 3 == 0 {
            tip = commit(&r, &[tip], &[("g", Some(&format!("{i}\n")))]).await;
            continue;
        }
        text.push_str(&format!("line {i}\n"));
        tip = commit(&r, &[tip], &[("f", Some(&text))]).await;
    }
    crate::derived::derive::<crate::derived::HistoryTree>(&r, tip)
        .await
        .unwrap();
    let full = r.blame_walk_in(tip, &rp("f"), None, false).await.unwrap();
    let cuts = Some(Cuts {
        head: 4,
        min_span: 8,
    });
    // The same blame without delays, for the time the replay takes.
    let started = std::time::Instant::now();
    let cold = Repo::new(r.blobs().clone(), meta.clone());
    let (fast, _) = cold
        .blame_walk_in(tip, &rp("f"), cuts, false)
        .await
        .unwrap();
    assert_eq!(fast, full.0);
    let base = started.elapsed();
    let latency = Duration::from_millis(2);
    meta.latency_us
        .store(latency.as_micros() as u64, Ordering::Relaxed);
    meta.reads.store(0, Ordering::Relaxed);
    let started = std::time::Instant::now();
    let cold = Repo::new(r.blobs().clone(), meta.clone());
    let (fast, _) = cold
        .blame_walk_in(tip, &rp("f"), cuts, false)
        .await
        .unwrap();
    let took = started.elapsed();
    meta.latency_us.store(0, Ordering::Relaxed);
    assert_eq!(fast, full.0);
    let reads = meta.reads.load(Ordering::Relaxed);
    assert!(reads < 800, "{reads} reads for about 800 changes");
    // One read per change, in sequence, would add 1.6 s.
    assert!(
        took.saturating_sub(base) < latency * 400,
        "the first blame took {took:?} ({base:?} without delays) over {reads} reads"
    );
}

/// A first blame of a file nobody has changed for most of a long history
/// makes a bounded number of reads, not some per changeset of history.
#[tokio::test]
async fn first_blame_of_an_old_file_reads_little() {
    let mut reads = Vec::new();
    for len in [600, 2400] {
        let (r, meta) = slow_repo();
        let mut tip = commit(&r, &[], &[("old", Some("a\nb\n")), ("hot", Some("0\n"))]).await;
        tip = commit(&r, &[tip], &[("old", Some("a\nb\nc\n"))]).await;
        for i in 0..len {
            tip = commit(&r, &[tip], &[("hot", Some(&format!("{i}\n")))]).await;
        }
        crate::derived::derive::<crate::derived::HistoryTree>(&r, tip)
            .await
            .unwrap();
        let cold = Repo::new(r.blobs().clone(), meta.clone());
        meta.reads.store(0, Ordering::Relaxed);
        let (b, examined) = cold.blame_walk(tip, &rp("old"), true, false).await.unwrap();
        assert_eq!(b.1.len(), 3);
        assert!(examined <= 3, "examined {examined}");
        reads.push(meta.reads.load(Ordering::Relaxed));
    }
    // Four times the history: the stretches are longer, not more numerous.
    assert!(
        reads[1] < reads[0] * 2,
        "reads grew with history: {reads:?}"
    );
    assert!(reads[1] < 600, "{reads:?}");
}

/// The replay's line diffs are those of `similar`'s `TextDiff` over the same
/// lines (split at `\n` only, see `split_lines`), over texts with every kind
/// of line end, a last line without one, empty texts and multi-byte
/// characters.
#[tokio::test]
async fn replay_diffs_are_those_of_text_diff() {
    use similar::TextDiff;
    let pieces = [
        "a\n", "b\n", "c\r\n", "d\r", "\r", "\n", "é\n", "x", "a\r\n", "", "b", "ü\r",
    ];
    let mut rng = Lcg(7);
    for _ in 0..60 {
        let versions: Vec<String> = (0..2 + rng.below(6))
            .map(|_| {
                let most = if rng.below(4) == 0 { 300 } else { 30 };
                (0..rng.below(most))
                    .map(|_| pieces[rng.below(pieces.len())])
                    .collect()
            })
            .collect();
        let (ops, back) = crate::blame::diff_versions(versions.clone()).await.unwrap();
        assert_eq!(back, versions);
        for (i, ops) in ops.iter().enumerate() {
            let old: Vec<&str> = crate::blame::split_lines(&versions[i]).collect();
            let new: Vec<&str> = crate::blame::split_lines(&versions[i + 1]).collect();
            let want: crate::blame::Ops = TextDiff::configure()
                .diff_slices(&old, &new)
                .ops()
                .iter()
                .map(|op| op.as_tag_tuple())
                .collect();
            assert_eq!(*ops, want, "{:?} -> {:?}", versions[i], versions[i + 1]);
        }
    }
}

/// A store that fails reads gives an error, never a different answer, a
/// hang or a panic, wherever among the gathered reads of the stretches the
/// failure falls.
#[tokio::test]
async fn failed_reads_fail_the_blame() {
    let (r, meta) = slow_repo();
    let mut text = String::new();
    let mut tip = commit(&r, &[], &[("f", Some("0\n")), ("g", Some("0\n"))]).await;
    for i in 0..300 {
        if i % 4 == 0 {
            tip = commit(&r, &[tip], &[("g", Some(&format!("{i}\n")))]).await;
            continue;
        }
        text.push_str(&format!("line {i}\n"));
        tip = commit(&r, &[tip], &[("f", Some(&text))]).await;
    }
    crate::derived::derive::<crate::derived::HistoryTree>(&r, tip)
        .await
        .unwrap();
    let full = r.blame_walk_in(tip, &rp("f"), None, false).await.unwrap();
    let cuts = Cuts {
        head: 2,
        min_span: 4,
    };
    let (mut failed, mut answered) = (0, 0);
    for every in [7, 13, 50, 97, 211, 400, 1000] {
        meta.fail_every.store(every, Ordering::Relaxed);
        let cold = Repo::new(r.blobs().clone(), meta.clone());
        let got = tokio::time::timeout(
            Duration::from_secs(60),
            cold.blame_walk_in(tip, &rp("f"), Some(cuts), true),
        )
        .await
        .expect("a blame with failing reads must not hang");
        match got {
            Ok((b, _)) => {
                assert_eq!(b, full.0);
                answered += 1;
            }
            Err(Error::Storage(_)) | Err(Error::Other(_)) => failed += 1,
            Err(e) => panic!("unexpected error {e:?}"),
        }
        meta.fail_every.store(0, Ordering::Relaxed);
        r.meta().delete_derived_kind("blame").await.unwrap();
    }
    assert!(failed > 0, "no failure was injected");
    let cold = Repo::new(r.blobs().clone(), meta.clone());
    let (b, _) = cold
        .blame_walk_in(tip, &rp("f"), Some(cuts), true)
        .await
        .unwrap();
    assert_eq!(b, full.0);
    let _ = answered;
}

/// Blame's lines are `\n`-terminated, as everywhere else Leep reads a file
/// (`\r\n` is just `\n` after a `\r`). A `\r` alone is part of its line.
#[tokio::test]
async fn a_lone_cr_is_part_of_its_line() {
    for (content, want) in [
        ("a\r\rb\n", vec!["a\r\rb"]),
        ("a\r\r\n", vec!["a\r"]),
        ("a\rb\n", vec!["a\rb"]),
        ("a\rb\nc\r", vec!["a\rb", "c\r"]),
        ("\r\n\r\r\n", vec!["", "\r"]),
        ("\r", vec!["\r"]),
    ] {
        let (r, _) = slow_repo();
        let tip = commit(&r, &[], &[("f", Some(content))]).await;
        for span in [1, 4] {
            assert!(agree(&r, tip, "f", span, false).await, "{content:?}");
        }
        let lines = r.blame(tip, &rp("f")).await.unwrap().lines;
        let got: Vec<&str> = lines.iter().map(|l| l.text.as_str()).collect();
        assert_eq!(got, want, "{content:?}");
        for (i, l) in lines.iter().enumerate() {
            assert_eq!((l.changeset, l.origin_line), (tip, i as u32 + 1));
        }
    }
}

/// A lone CR that appears in one version and is gone in the next changes the
/// line it is in, no more: the line after it keeps its credit.
#[tokio::test]
async fn a_lone_cr_that_comes_and_goes_blames_by_lines() {
    let (r, _) = slow_repo();
    let c1 = commit(&r, &[], &[("f", Some("x\ny\nz\n"))]).await;
    let c2 = commit(&r, &[c1], &[("f", Some("x\ry\nz\n"))]).await;
    let c3 = commit(&r, &[c2], &[("f", Some("x\ny\nz\n"))]).await;
    let c4 = commit(&r, &[c3], &[("f", Some("x\ny\nz\nw\r"))]).await;
    let c5 = commit(&r, &[c4], &[("f", Some("x\ny\nz\nw\n"))]).await;
    for (tip, want) in [
        (c2, vec![("x\ry", c2, 1), ("z", c1, 3)]),
        (c3, vec![("x", c3, 1), ("y", c3, 2), ("z", c1, 3)]),
        (
            c4,
            vec![("x", c3, 1), ("y", c3, 2), ("z", c1, 3), ("w\r", c4, 4)],
        ),
        (
            c5,
            vec![("x", c3, 1), ("y", c3, 2), ("z", c1, 3), ("w", c5, 4)],
        ),
    ] {
        for span in [1, 2, 8] {
            assert!(agree(&r, tip, "f", span, false).await);
        }
        let lines = r.blame(tip, &rp("f")).await.unwrap().lines;
        let got: Vec<(&str, ChangesetId, u32)> = lines
            .iter()
            .map(|l| (l.text.as_str(), l.changeset, l.origin_line))
            .collect();
        assert_eq!(got, want, "at {tip}");
    }
}

/// Over random versions with every kind of line end, blame has a line for
/// each `\n`-terminated line of the file, and each line is the one of the
/// version it credits, at the line number it gives.
#[tokio::test]
async fn blame_of_versions_with_every_line_end_is_consistent() {
    let pieces = [
        "a\n", "b\n", "c\r\n", "d\r", "\r", "\n", "é\n", "x", "a\r\n", "", "b", "ü\r",
    ];
    let strip = |l: &str| {
        let stripped = l.strip_suffix("\r\n").or_else(|| l.strip_suffix('\n'));
        stripped.unwrap_or(l).to_string()
    };
    let mut rng = Lcg(11);
    for _ in 0..25 {
        let (r, _) = slow_repo();
        let mut texts: BTreeMap<ChangesetId, String> = BTreeMap::new();
        let mut tip: Option<ChangesetId> = None;
        let mut last = String::new();
        for _ in 0..2 + rng.below(8) {
            let most = if rng.below(4) == 0 { 60 } else { 12 };
            let text: String = (0..1 + rng.below(most))
                .map(|_| pieces[rng.below(pieces.len())])
                .collect();
            if text == last {
                continue;
            }
            let parents: Vec<ChangesetId> = tip.into_iter().collect();
            let id = commit(&r, &parents, &[("f", Some(&text))]).await;
            texts.insert(id, text.clone());
            (tip, last) = (Some(id), text);
        }
        let tip = tip.unwrap();
        for span in [1, 3] {
            assert!(agree(&r, tip, "f", span, false).await, "{last:?}");
        }
        let lines = r.blame(tip, &rp("f")).await.unwrap().lines;
        let want: Vec<String> = last.split_inclusive('\n').map(strip).collect();
        let got: Vec<&String> = lines.iter().map(|l| &l.text).collect();
        assert_eq!(got, want.iter().collect::<Vec<_>>(), "{last:?}");
        for l in &lines {
            let origin: Vec<String> = texts[&l.changeset]
                .split_inclusive('\n')
                .map(strip)
                .collect();
            assert_eq!(
                origin.get(l.origin_line as usize - 1),
                Some(&l.text),
                "{l:?} of {:?}",
                texts[&l.changeset]
            );
        }
    }
}
