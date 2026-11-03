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
    /// Reads of skiplist rows, single or batched.
    skip_reads: AtomicUsize,
    /// Writes of cached blames.
    blame_puts: AtomicUsize,
    /// Reads of cached blames, single or batched.
    blame_reads: AtomicUsize,
    /// The write of a cached blame with this number (0: none) is stored,
    /// says so on `stalled`, and never returns.
    stall_at_blame_put: AtomicUsize,
    stalled: tokio::sync::Notify,
    /// While set, every read says so on `entered` and waits for the
    /// semaphore, which never has permits, to close.
    gate: std::sync::Mutex<Option<Arc<tokio::sync::Semaphore>>>,
    entered: tokio::sync::Notify,
}

impl Slow {
    fn note_kind(&self, kind: &str) {
        if kind == <crate::derived::Skiplist as crate::derived::DerivedKind>::NAME {
            self.skip_reads.fetch_add(1, Ordering::Relaxed);
        }
        if kind == crate::blame::KIND {
            self.blame_reads.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn counts(&self) -> (usize, usize) {
        (
            self.reads.load(Ordering::Relaxed),
            self.skip_reads.load(Ordering::Relaxed),
        )
    }

    fn close_gate(&self) {
        *self.gate.lock().unwrap() = Some(Arc::new(tokio::sync::Semaphore::new(0)));
    }

    fn open_gate(&self) {
        if let Some(g) = self.gate.lock().unwrap().take() {
            g.close();
        }
    }

    async fn read(&self) -> leep_core::Result<()> {
        let gate = self.gate.lock().unwrap().clone();
        if let Some(g) = gate {
            self.entered.notify_one();
            let _ = g.acquire().await;
        }
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
        self.note_kind(kind);
        self.inner.get_derived(kind, key).await
    }
    async fn get_derived_many(
        &self,
        kind: &str,
        keys: &[Hash],
    ) -> leep_core::Result<Vec<Option<Bytes>>> {
        self.read().await?;
        self.note_kind(kind);
        self.inner.get_derived_many(kind, keys).await
    }
    async fn put_derived(&self, kind: &str, key: &Hash, value: Bytes) -> leep_core::Result<()> {
        if kind != crate::blame::KIND {
            return self.inner.put_derived(kind, key, value).await;
        }
        let n = self.blame_puts.fetch_add(1, Ordering::Relaxed) + 1;
        self.inner.put_derived(kind, key, value).await?;
        if n == self.stall_at_blame_put.load(Ordering::Relaxed) {
            self.stalled.notify_one();
            std::future::pending::<()>().await;
        }
        Ok(())
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
        linear_jumps: true,
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
/// edits, reverts, deletes and re-creations, renames, binary versions, and
/// merges that bring a side branch's version of a file, delete it or keep
/// the first parent's.
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
            r.meta()
                .delete_derived_kind(crate::blame::KIND)
                .await
                .unwrap();
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
        linear_jumps: true,
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
        linear_jumps: true,
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
        r.meta()
            .delete_derived_kind(crate::blame::KIND)
            .await
            .unwrap();
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

/// Who blame credits each line of `path` at `tip` with, as (text,
/// changeset), the same from the reference walk, the walk in stretches and
/// a cached `blame`; and every line's `origin_line` names that line in the
/// crediting changeset's version of the file, under some name.
async fn credits(r: &Repo, tip: ChangesetId, path: &str) -> Vec<(String, ChangesetId)> {
    let (full, _) = r.blame_walk(tip, &rp(path), false, false).await.unwrap();
    let (fast, _) = r.blame_walk(tip, &rp(path), true, false).await.unwrap();
    assert_eq!(full, fast, "{path} at {tip}");
    let b = r.blame(tip, &rp(path)).await.unwrap();
    assert_eq!(b.lines, full.1, "{path} at {tip}, cached");
    assert_eq!(r.blame(tip, &rp(path)).await.unwrap().lines, full.1);
    for l in &full.1 {
        let files = r.list_files(l.changeset, "").await.unwrap();
        let mut found = false;
        for f in files {
            let Some((_, d)) = r.read_file(l.changeset, &f.path).await.unwrap() else {
                continue;
            };
            let text = String::from_utf8_lossy(&d).into_owned();
            if text.split('\n').nth(l.origin_line as usize - 1) == Some(l.text.as_str()) {
                found = true;
            }
        }
        assert!(found, "{l:?}: no file of {} has it there", l.changeset);
    }
    full.1.into_iter().map(|l| (l.text, l.changeset)).collect()
}

fn numbered(n: usize, tag: &str) -> String {
    (1..=n).map(|i| format!("{tag} line {i}\n")).collect()
}

/// A file renamed whole, exactly or with some lines dropped (similar enough
/// for `log --follow`), keeps the credit of the changesets that wrote its
/// lines under the old name; the rename itself is credited with nothing.
#[tokio::test]
async fn blame_follows_a_file_across_a_rename() {
    for drop_lines in [0usize, 3] {
        let r = Repo::new(
            Arc::new(MemoryBlobStore::new()),
            Arc::new(MemoryMetaStore::new()),
        );
        let first = numbered(10, "c1");
        let c0 = commit(&r, &[], &[("other", Some("x\n"))]).await;
        let c1 = commit(&r, &[c0], &[("src/old.rs", Some(&first))]).await;
        let second = format!("{first}c2 line a\nc2 line b\n");
        let c2 = commit(&r, &[c1], &[("src/old.rs", Some(&second))]).await;
        let mut lines: Vec<&str> = second.split_inclusive('\n').collect();
        for _ in 0..drop_lines {
            lines.remove(4);
        }
        let moved = lines.concat();
        let c3 = commit(
            &r,
            &[c2],
            &[("src/old.rs", None), ("crates/core/new.rs", Some(&moved))],
        )
        .await;
        let fourth = moved.replace("c1 line 2\n", "c4 line 2\n");
        let c4 = commit(&r, &[c3], &[("crates/core/new.rs", Some(&fourth))]).await;
        let got = credits(&r, c4, "crates/core/new.rs").await;
        let by: Vec<ChangesetId> = got.iter().map(|(_, c)| *c).collect();
        assert!(!by.contains(&c3), "dropping {drop_lines}: {got:?}");
        for (text, c) in &got {
            let want = match &text[..2] {
                "c1" => c1,
                "c2" => c2,
                "c4" => c4,
                _ => unreachable!(),
            };
            assert_eq!(*c, want, "dropping {drop_lines}: {text}");
        }
        assert_eq!(got.len(), 12 - drop_lines);
    }
}

/// A merge credits a line it took unchanged from its second parent to the
/// side branch's changeset that wrote it, a line from its first parent to
/// the main line's, and only lines it wrote itself (a conflict resolution)
/// to the merge. A file created on a side branch and merged in is credited
/// to the side branch too.
#[tokio::test]
async fn blame_credits_lines_merged_from_a_side_branch() {
    let r = Repo::new(
        Arc::new(MemoryBlobStore::new()),
        Arc::new(MemoryMetaStore::new()),
    );
    let base = commit(&r, &[], &[("f", Some("a\nb\nc\n"))]).await;
    let side = commit(
        &r,
        &[base],
        &[("f", Some("a\nside\nb\nc\n")), ("new", Some("s1\ns2\n"))],
    )
    .await;
    let side2 = commit(&r, &[side], &[("other", Some("o\n"))]).await;
    let main = commit(&r, &[base], &[("f", Some("a\nb\nc\nmain\n"))]).await;
    let merge = commit(
        &r,
        &[main, side2],
        &[
            ("f", Some("a\nside\nb\nc\nmain\n")),
            ("new", Some("s1\ns2\n")),
        ],
    )
    .await;
    let got = credits(&r, merge, "f").await;
    let want = [
        ("a", base),
        ("side", side),
        ("b", base),
        ("c", base),
        ("main", main),
    ];
    let want: Vec<(String, ChangesetId)> = want.iter().map(|(t, c)| (t.to_string(), *c)).collect();
    assert_eq!(got, want);
    let got = credits(&r, merge, "new").await;
    assert_eq!(got, [("s1".to_string(), side), ("s2".to_string(), side)]);

    // A resolution written in the merge is the merge's; the rest is not.
    let resolved = commit(
        &r,
        &[main, side2],
        &[("f", Some("a\nside\nb\nresolved\nc\nmain\n"))],
    )
    .await;
    let got = credits(&r, resolved, "f").await;
    let want = [
        ("a", base),
        ("side", side),
        ("b", base),
        ("resolved", resolved),
        ("c", base),
        ("main", main),
    ];
    let want: Vec<(String, ChangesetId)> = want.iter().map(|(t, c)| (t.to_string(), *c)).collect();
    assert_eq!(got, want);

    // Later main-line changes on top keep the side's credit, and a line the
    // side branch and the main line both wrote is the first parent's.
    let both = commit(&r, &[main], &[("f", Some("a\nb\nc\nmain\nboth\n"))]).await;
    let side3 = commit(&r, &[side2], &[("f", Some("a\nside\nb\nc\nboth\n"))]).await;
    let merge2 = commit(
        &r,
        &[both, side3],
        &[("f", Some("a\nside\nb\nc\nmain\nboth\n"))],
    )
    .await;
    let tip = commit(
        &r,
        &[merge2],
        &[("f", Some("top\na\nside\nb\nc\nmain\nboth\n"))],
    )
    .await;
    let got = credits(&r, tip, "f").await;
    let want = [
        ("top", tip),
        ("a", base),
        ("side", side),
        ("b", base),
        ("c", base),
        ("main", main),
        ("both", both),
    ];
    let want: Vec<(String, ChangesetId)> = want.iter().map(|(t, c)| (t.to_string(), *c)).collect();
    assert_eq!(got, want);
}

/// Blame by the definition, over every changeset of a history oldest first
/// (`order`), for every file: a version the same as its first parent's has
/// its blame; otherwise its lines take the credit of the first parent's
/// version (or, for a path new there and not merged in from another parent,
/// of the file it was renamed from there) where a diff keeps them, then of each other parent's differing version in
/// order, and the rest are its own.
async fn reference_blames(
    r: &Repo,
    order: &[ChangesetId],
) -> BTreeMap<(ChangesetId, String), Vec<(ChangesetId, u32)>> {
    use leep_core::rename::RenameOptions;
    use similar::DiffTag;
    type Blames = BTreeMap<(ChangesetId, String), Vec<(ChangesetId, u32)>>;
    async fn text(r: &Repo, c: ChangesetId, p: &str) -> Option<String> {
        r.read_file(c, &rp(p))
            .await
            .unwrap()
            .map(|(_, d)| String::from_utf8(d.to_vec()).unwrap())
    }
    async fn ops(old: &str, new: &str) -> crate::blame::Ops {
        let (mut ops, _) = crate::blame::diff_versions(vec![old.to_string(), new.to_string()])
            .await
            .unwrap();
        ops.remove(0)
    }
    let mut out: Blames = BTreeMap::new();
    for &c in order {
        let parents = r.get_changeset(c).await.unwrap().parents;
        for f in r.list_files(c, "").await.unwrap() {
            let p = f.path.as_str().to_string();
            let own = text(r, c, &p).await.unwrap();
            let p1 = parents.first().copied();
            let mut base: Option<(String, Vec<(ChangesetId, u32)>)> = None;
            if let Some(p1) = p1 {
                match text(r, p1, &p).await {
                    Some(t) if t == own => {
                        let b = out[&(p1, p.clone())].clone();
                        out.insert((c, p), b);
                        continue;
                    }
                    Some(t) => base = Some((t, out[&(p1, p.clone())].clone())),
                    None => {
                        // A merge that took the file from another parent is
                        // not searched for a rename.
                        let mut merged_in = false;
                        for &pk in parents.iter().skip(1) {
                            merged_in |= text(r, pk, &p).await.is_some();
                        }
                        let found = match merged_in {
                            true => Vec::new(),
                            false => {
                                let entries = r.diff(Some(p1), c, "").await.unwrap();
                                r.detect_renames(&entries, &RenameOptions::default())
                                    .await
                                    .unwrap()
                                    .renames
                            }
                        };
                        if let Some(rn) = found.iter().find(|rn| rn.to == f.path) {
                            let from = rn.from.as_str().to_string();
                            let t = text(r, p1, &from).await.unwrap();
                            base = Some((t, out[&(p1, from)].clone()));
                        }
                    }
                }
            }
            let n = crate::blame::split_lines(&own).count();
            let mut lines: Vec<Option<(ChangesetId, u32)>> = vec![None; n];
            if let Some((t, b)) = &base {
                for (tag, old, new) in ops(t, &own).await {
                    if tag == DiffTag::Equal {
                        for (o, l) in old.zip(new) {
                            lines[l] = Some(b[o]);
                        }
                    }
                }
            }
            for &pk in parents.iter().skip(1) {
                let Some(t) = text(r, pk, &p).await else {
                    continue;
                };
                if base.as_ref().is_some_and(|(b, _)| *b == t) {
                    continue;
                }
                let b = &out[&(pk, p.clone())];
                for (tag, old, new) in ops(&t, &own).await {
                    if tag == DiffTag::Equal {
                        for (o, l) in old.zip(new) {
                            if lines[l].is_none() {
                                lines[l] = Some(b[o]);
                            }
                        }
                    }
                }
            }
            let lines = lines
                .into_iter()
                .enumerate()
                .map(|(l, x)| x.unwrap_or((c, l as u32 + 1)))
                .collect();
            out.insert((c, p), lines);
        }
    }
    out
}

/// Over random histories of edits, deletes, renames (exact and similar),
/// side branches merged back with a mix of both sides' lines, files created
/// on a side branch, and octopus merges, blame of every file at many
/// changesets is the definition's (`reference_blames`), from the reference
/// walk, from short stretches, and with blames and checkpoints cached at
/// random changesets.
#[tokio::test]
async fn blame_matches_the_definition_across_merges_and_renames() {
    let names = ["a", "b", "dir/c", "dir/sub/d", "e"];
    for seed in 1..=6u64 {
        let mut r = Repo::new(
            Arc::new(MemoryBlobStore::new()),
            Arc::new(MemoryMetaStore::new()),
        );
        // Checkpoints (ADR 0078) every one to three versions, so the cached
        // rounds below start walks and replays at many of them.
        r.blame_checkpoint_every = 1 + seed as usize % 3;
        let mut rng = Lcg(seed * 31 + 7);
        let mut tag = 0usize;
        let fresh = |rng: &mut Lcg, tag: &mut usize| -> String {
            *tag += 1;
            (0..2 + rng.below(6))
                .map(|i| format!("w{tag} {i}\n"))
                .collect()
        };
        let read = |r: Repo, c: ChangesetId, p: &'static str| async move {
            r.read_file(c, &rp(p))
                .await
                .unwrap()
                .map(|(_, d)| String::from_utf8(d.to_vec()).unwrap())
        };
        let (t0, t1) = (fresh(&mut rng, &mut tag), fresh(&mut rng, &mut tag));
        let root = commit(&r, &[], &[("a", Some(&t0)), ("dir/c", Some(&t1))]).await;
        let mut all = vec![root];
        let mut heads = vec![root];
        let (mut merges, mut renames) = (0, 0);
        for _ in 0..120 {
            let h = rng.below(heads.len());
            let at = heads[h];
            let n = names[rng.below(names.len())];
            let cur = read(r.clone(), at, n).await;
            let next = match rng.below(12) {
                0..=4 => {
                    // Edit: insert, replace or drop a line, or create.
                    let t = match &cur {
                        Some(t) => {
                            let mut lines: Vec<String> =
                                t.split_inclusive('\n').map(str::to_string).collect();
                            let i = rng.below(lines.len() + 1);
                            tag += 1;
                            match rng.below(3) {
                                0 if i < lines.len() => lines[i] = format!("x{tag}\n"),
                                1 if i < lines.len() && lines.len() > 1 => {
                                    lines.remove(i);
                                }
                                _ => lines.insert(i, format!("x{tag}\n")),
                            }
                            lines.concat()
                        }
                        None => fresh(&mut rng, &mut tag),
                    };
                    commit(&r, &[at], &[(n, Some(&t))]).await
                }
                5 if cur.is_some() => commit(&r, &[at], &[(n, None)]).await,
                5 => continue,
                6 | 7 => {
                    // Rename to a free name, exactly or with an edit.
                    let mut to = n;
                    for _ in 0..8 {
                        let o = names[rng.below(names.len())];
                        if read(r.clone(), at, o).await.is_none() {
                            to = o;
                        }
                    }
                    match &cur {
                        Some(t) if to != n => {
                            let mut t = t.clone();
                            if rng.below(2) == 0 {
                                tag += 1;
                                t.push_str(&format!("r{tag}\n"));
                            }
                            renames += 1;
                            commit(&r, &[at], &[(n, None), (to, Some(&t))]).await
                        }
                        _ => continue,
                    }
                }
                8 | 9 => {
                    // Branch off.
                    heads.push(at);
                    continue;
                }
                _ => {
                    // Merge another head (or two) into this one.
                    let others: Vec<ChangesetId> = heads
                        .iter()
                        .copied()
                        .filter(|o| *o != at)
                        .take(1 + rng.below(2))
                        .collect();
                    if others.is_empty() {
                        continue;
                    }
                    let mut parents = vec![at];
                    parents.extend(&others);
                    // Mostly a file the branches disagree on.
                    let mut n = n;
                    for o in names {
                        let differ =
                            read(r.clone(), at, o).await != read(r.clone(), others[0], o).await;
                        if differ && rng.below(3) > 0 {
                            n = o;
                        }
                    }
                    let cur = read(r.clone(), at, n).await;
                    let theirs = read(r.clone(), others[0], n).await;
                    let t: Option<String> = match (rng.below(4), &cur, &theirs) {
                        (0, _, _) => theirs.clone(),
                        (1, Some(a), Some(b)) | (2, Some(a), Some(b)) => {
                            // Ours, then their lines we lack, maybe with a
                            // line of the merge's own.
                            let mut t = a.clone();
                            for l in b.split_inclusive('\n') {
                                if !a.split_inclusive('\n').any(|x| x == l) {
                                    t.push_str(l);
                                }
                            }
                            if rng.below(2) == 0 {
                                tag += 1;
                                t.insert_str(0, &format!("m{tag}\n"));
                            }
                            Some(t)
                        }
                        _ => cur.clone(),
                    };
                    let files: Vec<(&str, Option<&str>)> = match (&t, &cur) {
                        (a, b) if a == b => Vec::new(),
                        _ => vec![(n, t.as_deref())],
                    };
                    let id = commit(&r, &parents, &files).await;
                    merges += usize::from(!files.is_empty());
                    heads.retain(|o| !others.contains(o));
                    let h = heads.iter().position(|o| *o == at).unwrap();
                    heads[h] = id;
                    all.push(id);
                    continue;
                }
            };
            heads[h] = next;
            all.push(next);
        }
        let want = reference_blames(&r, &all).await;
        let mut compared = 0;
        let check = |got: &crate::blame::CachedBlame, c: ChangesetId, p: &str| {
            let lines: Vec<(ChangesetId, u32)> =
                got.1.iter().map(|l| (l.changeset, l.origin_line)).collect();
            assert_eq!(lines, want[&(c, p.to_string())], "seed {seed}: {p} at {c}");
        };
        for (i, &c) in all.iter().enumerate() {
            if i % 3 != 0 && !heads.contains(&c) {
                continue;
            }
            for f in r.list_files(c, "").await.unwrap() {
                let p = f.path.as_str();
                let (full, _) = r.blame_walk_in(c, &f.path, None, false).await.unwrap();
                check(&full, c, p);
                for span in [1, 4] {
                    let cuts = Cuts {
                        head: span,
                        min_span: span as u64,
                        linear_jumps: true,
                    };
                    let (fast, _) = r
                        .blame_walk_in(c, &f.path, Some(cuts), false)
                        .await
                        .unwrap();
                    check(&fast, c, p);
                }
                compared += 1;
            }
        }
        assert!(compared > 25, "seed {seed} compared only {compared}");
        assert!(
            merges >= 3 && renames >= 2,
            "seed {seed}: {merges} merges changing a file, {renames} renames"
        );
        for round in 0..3 {
            r.meta()
                .delete_derived_kind(crate::blame::KIND)
                .await
                .unwrap();
            for _ in 0..3 + round * 4 {
                let at = all[rng.below(all.len())];
                for n in names {
                    let _ = r.blame(at, &rp(n)).await;
                }
            }
            for &h in &heads {
                for f in r.list_files(h, "").await.unwrap() {
                    let (got, _) = r.blame_walk(h, &f.path, true, true).await.unwrap();
                    check(&got, h, f.path.as_str());
                }
            }
        }
    }
}

/// A blame that needs more walks below merges than it may start leaves the
/// merges it did not follow their lines and is not cached, but caches the
/// complete blames at the starts of the (oldest) walks it did, so each
/// repeated request starts its walks above them and the answer becomes
/// complete, and then cached, after a few requests.
#[tokio::test]
async fn repeated_blames_past_the_walk_limit_make_progress() {
    let (r, _) = slow_repo();
    let mut text = String::from("a\n");
    let mut main = commit(&r, &[], &[("f", Some(&text)), ("g", Some("0\n"))]).await;
    for k in 0..12 {
        let mine = format!("{text}side {k}\n");
        let side = commit(&r, &[main], &[("f", Some(&mine))]).await;
        let other = commit(&r, &[main], &[("g", Some(&format!("{k}\n")))]).await;
        main = commit(&r, &[other, side], &[("f", Some(&mine))]).await;
        text = mine;
    }
    let path = rp("f");
    let (want, _) = r.blame_walk_in(main, &path, None, false).await.unwrap();
    let merges: usize = want.1.iter().filter(|l| l.text.starts_with("side")).count();
    assert_eq!(merges, 12);
    let cuts = Some(Cuts {
        head: 32,
        min_span: 32,
        linear_jumps: true,
    });
    let on_merges = |b: &crate::blame::CachedBlame| {
        let mut n = 0;
        for (got, want) in b.1.iter().zip(&want.1) {
            if got != want {
                n += 1;
            }
        }
        n
    };
    let mut left = usize::MAX;
    let mut rounds = 0;
    loop {
        rounds += 1;
        assert!(rounds <= 6, "no progress after {rounds} requests");
        let run = r.blame_run(main, &path, cuts, true, 3).await.unwrap();
        assert!(run.walks <= 3, "{} walks", run.walks);
        let stored = r
            .meta()
            .get_derived(crate::blame::KIND, &crate::blame::cache_key(main, &path))
            .await
            .unwrap();
        if run.complete {
            assert_eq!(run.blame, want);
            assert!(stored.is_some());
            break;
        }
        assert!(stored.is_none(), "an incomplete blame was cached");
        let wrong = on_merges(&run.blame);
        assert!(
            wrong < left,
            "request {rounds}: {wrong} lines still wrong, {left} before"
        );
        left = wrong;
    }
    assert_eq!(rounds, 4, "12 merges, 3 walks a request");
    let again = r.blame_run(main, &path, cuts, true, 3).await.unwrap();
    assert_eq!((again.walks, again.examined, again.complete), (0, 0, true));
}

/// A linear history of `len` changesets, each changing one file: `tracked`
/// files changed 6 to 25 times each at random points (each change appends
/// a line), the rest changing one of a few busy files. Returns the tip.
async fn swarm(r: &Repo, len: usize, tracked: usize, seed: u64) -> (ChangesetId, Vec<String>) {
    let mut rng = Lcg(seed);
    let names: Vec<String> = (0..tracked).map(|i| format!("src/m{i}.rs")).collect();
    let mut at: Vec<(usize, usize)> = Vec::new();
    for f in 0..tracked {
        for _ in 0..6 + rng.below(20) {
            at.push((1 + rng.below(len - 1), f));
        }
    }
    at.sort_unstable();
    let mut texts: Vec<String> = names.iter().map(|n| format!("// {n}\n")).collect();
    let first: Vec<(&str, Option<&str>)> = names
        .iter()
        .zip(&texts)
        .map(|(n, t)| (n.as_str(), Some(t.as_str())))
        .collect();
    let mut tip = commit(r, &[], &first).await;
    let mut next = at.iter().peekable();
    for i in 1..len {
        let mut f = None;
        while let Some(&&(j, g)) = next.peek() {
            if j > i {
                break;
            }
            next.next();
            f = Some(g);
        }
        tip = match f {
            Some(f) => {
                texts[f].push_str(&format!("line {i}\n"));
                commit(r, &[tip], &[(&names[f], Some(&texts[f]))]).await
            }
            None => {
                let busy = format!("busy/{}", rng.below(4));
                commit(r, &[tip], &[(&busy, Some(&format!("{i}\n")))]).await
            }
        };
    }
    (tip, names)
}

/// A blame at a head whose answer is the cached blame of the file's newest
/// changer stores it at the head, once and as the same bytes, so the next
/// blame there is a lookup.
#[tokio::test]
async fn a_blame_answered_by_a_changers_cached_blame_is_kept_at_the_head() {
    let (r, meta) = slow_repo();
    let mut text = String::new();
    let mut tip = commit(&r, &[], &[("f", Some("0\n")), ("g", Some("0\n"))]).await;
    for i in 0..5 {
        text.push_str(&format!("line {i}\n"));
        tip = commit(&r, &[tip], &[("f", Some(&text))]).await;
    }
    let latest = tip;
    for i in 0..40 {
        tip = commit(&r, &[tip], &[("g", Some(&format!("{i}\n")))]).await;
    }
    let path = rp("f");
    r.blame_walk(tip, &path, true, true).await.unwrap();
    for i in 0..40 {
        tip = commit(&r, &[tip], &[("g", Some(&format!("again {i}\n")))]).await;
    }
    crate::derived::derive::<crate::derived::HistoryTree>(&r, tip)
        .await
        .unwrap();
    let (want, _) = r.blame_walk_in(tip, &path, None, false).await.unwrap();

    let blame_at = |id: ChangesetId| {
        let meta = meta.clone();
        let key = crate::blame::cache_key(id, &path);
        async move {
            meta.inner
                .get_derived(crate::blame::KIND, &key)
                .await
                .unwrap()
        }
    };
    assert!(blame_at(tip).await.is_none());
    let puts = meta.blame_puts.load(Ordering::Relaxed);
    meta.reads.store(0, Ordering::Relaxed);
    let cold = Repo::new(r.blobs().clone(), meta.clone());
    let (got, examined) = cold.blame_walk(tip, &path, true, true).await.unwrap();
    let walked = meta.reads.load(Ordering::Relaxed);
    assert_eq!(got, want);
    assert!(
        examined > 0,
        "the newest changer's blame was found by a walk"
    );
    assert_eq!(meta.blame_puts.load(Ordering::Relaxed), puts + 1);
    assert_eq!(blame_at(tip).await, blame_at(latest).await);

    meta.reads.store(0, Ordering::Relaxed);
    let cold = Repo::new(r.blobs().clone(), meta.clone());
    let (again, examined) = cold.blame_walk(tip, &path, true, true).await.unwrap();
    let looked_up = meta.reads.load(Ordering::Relaxed);
    assert_eq!(again, want);
    assert_eq!(examined, 0);
    assert_eq!(meta.blame_puts.load(Ordering::Relaxed), puts + 1);
    assert!(
        looked_up <= walked && looked_up <= 6,
        "{looked_up} reads at the head, {walked} for the walk"
    );
}

/// Skiplists kept by queries are the stored ones, are found again without
/// a read, and stay within the cache's budget.
#[tokio::test]
async fn kept_skiplists_are_the_stored_ones_within_the_budget() {
    let (r, meta) = slow_repo();
    let mut ids = vec![commit(&r, &[], &[("f", Some("0\n"))]).await];
    for i in 0..400 {
        let p = ids[ids.len() - 1];
        ids.push(commit(&r, &[p], &[("f", Some(&format!("{i}\n")))]).await);
    }
    let small = Repo::new(r.blobs().clone(), meta.clone())
        .with_cache_budget(crate::DEFAULT_CACHE_BYTES >> 8);
    for &id in &ids {
        let stored = crate::derived::Skiplist::decode(
            &meta
                .inner
                .get_derived("skiplist", &id.0)
                .await
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(&small.query_skiplist(id).await.unwrap()[..], &stored[..]);
    }
    let held = small.skip_reads.held_bytes();
    assert!(held <= small.skip_reads.budget(), "{held} bytes held");
    assert!(
        small.cached_skips(ids[0]).is_none(),
        "the oldest list is evicted"
    );
    let before = meta.counts().1;
    for &id in &ids[ids.len() - 20..] {
        let kept = small.query_skiplist(id).await.unwrap();
        assert_eq!(kept, r.skiplist(id).await.unwrap());
    }
    assert_eq!(meta.counts().1, before, "kept lists are read again");
}

/// A first blame in a process that blamed other files before reads far
/// fewer skiplists than the first blame in a new one, and answers the same.
#[tokio::test]
async fn a_warm_process_reads_few_skiplists_for_a_new_blame() {
    let (r, meta) = slow_repo();
    let (tip, names) = swarm(&r, 1500, 8, 7).await;
    crate::derived::derive::<crate::derived::HistoryTree>(&r, tip)
        .await
        .unwrap();
    let server = Repo::new(r.blobs().clone(), meta.clone());
    let mut first = 0;
    for n in &names {
        let before = meta.counts().1;
        let (b, _) = server.blame_walk(tip, &rp(n), true, false).await.unwrap();
        first += meta.counts().1 - before;
        let (want, _) = r.blame_walk_in(tip, &rp(n), None, false).await.unwrap();
        assert_eq!(b, want, "{n}");
    }
    // Uncached blames again, at the same head: every list the walks step
    // down is kept.
    let mut warm = 0;
    for n in &names {
        let before = meta.counts().1;
        let (b, _) = server.blame_walk(tip, &rp(n), true, false).await.unwrap();
        warm += meta.counts().1 - before;
        let fresh = Repo::new(r.blobs().clone(), meta.clone());
        assert_eq!(
            b,
            fresh.blame_walk(tip, &rp(n), true, false).await.unwrap().0
        );
    }
    assert!(first > 40, "{first} skiplist reads cold");
    assert!(
        warm * 10 <= first,
        "{warm} skiplist reads warm, {first} cold"
    );
}

/// Metadata calls per blame on a swarm-like history (`cargo test -p
/// leep-derive blame_calls_on_a_swarm_history -- --ignored --nocapture`).
#[tokio::test]
#[ignore]
async fn blame_calls_on_a_swarm_history() {
    let (r, meta) = slow_repo();
    let (mut tip, names) = swarm(&r, 4100, 40, 11).await;
    crate::derived::derive::<crate::derived::HistoryTree>(&r, tip)
        .await
        .unwrap();
    let server = Repo::new(r.blobs().clone(), meta.clone());
    let report = |label: &str, calls: &[(usize, usize)]| {
        let mut all: Vec<usize> = calls.iter().map(|c| c.0).collect();
        all.sort_unstable();
        let skips: usize = calls.iter().map(|c| c.1).sum();
        println!(
            "{label}: calls per blame mean {:.1} median {} max {}, skiplist reads mean {:.1}",
            all.iter().sum::<usize>() as f64 / all.len() as f64,
            all[all.len() / 2],
            all[all.len() - 1],
            skips as f64 / all.len() as f64
        );
    };
    let run = |label: &'static str, tip: ChangesetId, names: &[String]| {
        let (server, meta, names) = (server.clone(), meta.clone(), names.to_vec());
        async move {
            let mut calls = Vec::new();
            for n in &names {
                let before = meta.counts();
                server.blame_walk(tip, &rp(n), true, true).await.unwrap();
                let after = meta.counts();
                calls.push((after.0 - before.0, after.1 - before.1));
            }
            report(label, &calls);
        }
    };
    run("first blames at head 1, files 0-19", tip, &names[..20]).await;
    run("first blames at head 1, files 20-39", tip, &names[20..]).await;
    for i in 0..40 {
        let f = &names[i % 4];
        let t = format!("moved {i}\n");
        tip = match i % 5 {
            0 => {
                let info = r.lookup_file(tip, &rp(f)).await.unwrap().unwrap();
                let old = r.blobs().get(&info.content).await.unwrap().unwrap();
                let new = format!("{}{t}", String::from_utf8(old.to_vec()).unwrap());
                commit(&r, &[tip], &[(f, Some(&new))]).await
            }
            _ => commit(&r, &[tip], &[("busy/0", Some(&t))]).await,
        };
    }
    crate::derived::derive::<crate::derived::HistoryTree>(&r, tip)
        .await
        .unwrap();
    run("blames at head 2 (40 lands later)", tip, &names).await;
    run("again at head 2", tip, &names).await;
}

/// A linear history in which every other changeset or so edits `hot`
/// (replacing, inserting or dropping a line), `versions` versions in all.
async fn hot_file(r: &Repo, versions: usize, seed: u64) -> ChangesetId {
    let mut rng = Lcg(seed);
    let mut lines: Vec<String> = (0..200).map(|i| format!("base {i}\n")).collect();
    let mut tip = commit(
        r,
        &[],
        &[("hot", Some(&lines.concat())), ("other", Some("0\n"))],
    )
    .await;
    for v in 1..versions {
        let i = rng.below(lines.len());
        match rng.below(3) {
            0 => lines[i] = format!("edit {v}\n"),
            2 if lines.len() > 100 => {
                lines.remove(i);
            }
            _ => lines.insert(i, format!("add {v}\n")),
        }
        if v % 3 == 0 {
            tip = commit(r, &[tip], &[("other", Some(&format!("{v}\n")))]).await;
        }
        tip = commit(r, &[tip], &[("hot", Some(&lines.concat()))]).await;
    }
    crate::derived::derive::<crate::derived::HistoryTree>(r, tip)
        .await
        .unwrap();
    tip
}

fn production_cuts() -> Option<Cuts> {
    Some(Cuts {
        head: crate::blame::HEAD_STEPS,
        min_span: crate::blame::MIN_STRETCH_SPAN,
        linear_jumps: true,
    })
}

/// A long blame leaves checkpoints as it replays (ADR 0078). Cut short, the
/// next blame, in a new process, walks and replays only the versions above
/// the newest one and answers the same; uninterrupted, a blame writes about
/// one checkpoint per spacing of versions.
#[tokio::test]
async fn an_interrupted_blame_resumes_from_its_checkpoints() {
    const VERSIONS: usize = 600;
    const EVERY: usize = 64;
    let (mut r, meta) = slow_repo();
    r.blame_checkpoint_every = EVERY;
    let tip = hot_file(&r, VERSIONS, 5).await;
    let path = rp("hot");
    let (want, _) = r.blame_walk_in(tip, &path, None, false).await.unwrap();
    let process = || {
        let mut p = Repo::new(r.blobs().clone(), meta.clone());
        p.blame_checkpoint_every = EVERY;
        p
    };
    let run = |p: Repo| {
        let path = path.clone();
        async move {
            p.blame_run(
                tip,
                &path,
                production_cuts(),
                true,
                crate::blame_follow::FOLLOW_WALKS,
            )
            .await
        }
    };

    // Cut short at its fourth checkpoint, as by a dropped request.
    meta.stall_at_blame_put.store(4, Ordering::Relaxed);
    tokio::select! {
        _ = run(process()) => panic!("the stalled blame finished"),
        _ = meta.stalled.notified() => {}
    }
    meta.stall_at_blame_put.store(0, Ordering::Relaxed);
    assert_eq!(meta.blame_puts.load(Ordering::Relaxed), 4);
    let at_tip = meta
        .inner
        .get_derived(crate::blame::KIND, &crate::blame::cache_key(tip, &path))
        .await
        .unwrap();
    assert!(at_tip.is_none(), "the cut blame cached its answer");

    meta.reads.store(0, Ordering::Relaxed);
    let resumed = run(process()).await.unwrap();
    let resumed_reads = meta.reads.load(Ordering::Relaxed);
    assert_eq!(resumed.blame, want);
    assert!(
        resumed.replayed <= VERSIONS - 4 * EVERY,
        "{} versions replayed after 4 checkpoints",
        resumed.replayed
    );

    // From nothing: every version, and a checkpoint per spacing.
    meta.inner
        .delete_derived_kind(crate::blame::KIND)
        .await
        .unwrap();
    meta.reads.store(0, Ordering::Relaxed);
    let scratch = run(process()).await.unwrap();
    let scratch_reads = meta.reads.load(Ordering::Relaxed);
    assert_eq!(scratch.blame, want);
    assert_eq!(scratch.replayed, VERSIONS);
    assert_eq!(scratch.checkpoints, (VERSIONS - 1) / EVERY);
    assert!(
        resumed_reads < scratch_reads,
        "{resumed_reads} reads resumed, {scratch_reads} from nothing"
    );

    // Uncheckpointed, the same answer and no rows but the blame's own.
    meta.inner
        .delete_derived_kind(crate::blame::KIND)
        .await
        .unwrap();
    let puts = meta.blame_puts.load(Ordering::Relaxed);
    let mut none = process();
    none.blame_checkpoint_every = 0;
    let plain = run(none).await.unwrap();
    assert_eq!((plain.blame, plain.checkpoints), (want, 0));
    assert_eq!(meta.blame_puts.load(Ordering::Relaxed), puts + 2);
}

/// Waits up to ten seconds for `cond`.
async fn until(what: &str, cond: impl Fn() -> bool) {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while !cond() {
        assert!(std::time::Instant::now() < deadline, "{what}");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// Concurrent requests for one blame share one computation, which goes on
/// after the request that started it is dropped, keeps that request's hold
/// to its end, and caches its answer (ADR 0078). With every detached slot
/// taken, a blame runs inline and ends with its caller.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_blames_share_one_computation_that_outlives_its_callers() {
    let (r, meta) = slow_repo();
    let tip = hot_file(&r, 300, 9).await;
    let path = rp("hot");
    let (want, _) = r.blame_walk_in(tip, &path, None, false).await.unwrap();
    let mut server = Repo::new(r.blobs().clone(), meta.clone());
    server.blame_checkpoint_every = 0;
    server.blame_flights = Arc::new(crate::blame_flight::BlameFlights::new(1));
    let puts = meta.blame_puts.load(Ordering::Relaxed);

    meta.close_gate();
    let hold = Arc::new(());
    let first = {
        let (s, p, h) = (server.clone(), path.clone(), hold.clone());
        tokio::spawn(async move { s.blame_held(tip, &p, Some(Box::new(h))).await })
    };
    tokio::time::timeout(Duration::from_secs(10), meta.entered.notified())
        .await
        .expect("the blame reads");
    let joiners: Vec<_> = (0..7)
        .map(|_| {
            let (s, p) = (server.clone(), path.clone());
            tokio::spawn(async move { s.blame(tip, &p).await })
        })
        .collect();
    until("seven requests join", || server.blame_flights.joins() == 7).await;
    assert_eq!(server.blame_flights.running(), 1);
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());

    // The one slot is taken: a blame of another file runs inline, and its
    // caller's going away ends it.
    let other = rp("other");
    let inline = {
        let (s, p) = (server.clone(), other.clone());
        tokio::spawn(async move { s.blame(tip, &p).await })
    };
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(server.blame_flights.running(), 1);
    inline.abort();
    let _ = inline.await;

    meta.open_gate();
    for j in joiners {
        assert_eq!(j.await.unwrap().unwrap().lines, want.1);
    }
    until("the blame lets its hold go", || {
        Arc::strong_count(&hold) == 1
    })
    .await;
    until("the flight ends", || server.blame_flights.running() == 0).await;
    assert_eq!(
        meta.blame_puts.load(Ordering::Relaxed),
        puts + 2,
        "one computation cached its answer"
    );
    let stored = |id: ChangesetId, p: RepoPath| {
        let meta = meta.clone();
        async move {
            meta.inner
                .get_derived(crate::blame::KIND, &crate::blame::cache_key(id, &p))
                .await
                .unwrap()
        }
    };
    assert!(stored(tip, path.clone()).await.is_some());
    assert!(
        stored(tip, other.clone()).await.is_none(),
        "the inline blame ended with its caller"
    );

    // Alone and dropped at once, a detached blame still finishes and
    // caches its answer.
    meta.close_gate();
    let dropped = {
        let (s, p) = (server.clone(), other.clone());
        tokio::spawn(async move { s.blame(tip, &p).await })
    };
    until("the blame starts", || server.blame_flights.running() == 1).await;
    dropped.abort();
    let _ = dropped.await;
    meta.open_gate();
    until("the flight ends", || server.blame_flights.running() == 0).await;
    assert!(stored(tip, other.clone()).await.is_some());
    assert_eq!(server.blame(tip, &other).await.unwrap().lines.len(), 1);
}

/// One pass of the index warmers, then one of the blame warmer, as a
/// server runs them: the blame warmer reads the index only where it is
/// derived.
async fn warm_pass(
    w: &mut crate::warm::Warmer,
    b: &mut crate::blame_warm::BlameWarmer,
) -> crate::blame_warm::BlameWarmReport {
    w.pass().await.unwrap();
    b.pass().await.unwrap()
}

fn warmers(r: &Repo) -> (crate::warm::Warmer, crate::blame_warm::BlameWarmer) {
    (
        crate::warm::Warmer::new(r.clone(), false),
        crate::blame_warm::BlameWarmer::new(r.clone()),
    )
}

/// Blames the warmer extended (ADR 0083) are byte for byte the blames a
/// cold request computes, over random histories of one ref with edits,
/// creations, deletes, renames, binary versions and merged side branches,
/// warmed a few lands at a time, with occasional request blames seeding
/// the tracked files as clients would.
#[tokio::test]
async fn warm_extended_blames_match_cold_blames() {
    use crate::blame::{cache_key, KIND};
    let names = ["a", "b", "dir/c", "dir/sub/d", "e"];
    let mut total_extended = 0;
    for seed in 1..=4u64 {
        let r = Repo::new(
            Arc::new(MemoryBlobStore::new()),
            Arc::new(MemoryMetaStore::new()),
        );
        let mut rng = Lcg(seed * 17 + 3);
        let mut tag = 0usize;
        let read = |r: Repo, c: ChangesetId, p: &'static str| async move {
            r.read_file(c, &rp(p))
                .await
                .unwrap()
                .map(|(_, d)| d.to_vec())
        };
        let edit = |rng: &mut Lcg, tag: &mut usize, cur: Option<Vec<u8>>| -> Vec<u8> {
            *tag += 1;
            let t = cur
                .and_then(|c| String::from_utf8(c).ok())
                .unwrap_or_default();
            let mut lines: Vec<String> = t.split_inclusive('\n').map(str::to_string).collect();
            let i = rng.below(lines.len() + 1);
            match rng.below(4) {
                0 if i < lines.len() => lines[i] = format!("x{tag}\n"),
                1 if i < lines.len() && lines.len() > 1 => {
                    lines.remove(i);
                }
                // A last line without its newline, or with a `\r`.
                2 if i == lines.len() => lines.push(format!("x{tag}\r\ny{tag}")),
                _ => lines.insert(i, format!("x{tag}\n")),
            }
            lines.concat().into_bytes()
        };
        let root = commit(&r, &[], &[("a", Some("a0\n")), ("b", Some("b0\n"))]).await;
        r.meta().cas_ref("main", None, Some(root)).await.unwrap();
        let (mut w, mut b) = warmers(&r);
        warm_pass(&mut w, &mut b).await;
        let mut head = root;
        let mut mains = vec![root];
        let mut all = vec![root];
        let mut side: Option<ChangesetId> = None;
        let (mut merges, mut renames) = (0, 0);
        for step in 0..150 {
            let n = names[rng.below(names.len())];
            let next = match rng.below(14) {
                0..=5 | 12 => {
                    // A land touching one to three files.
                    let mut files: Vec<(&str, Vec<u8>)> = Vec::new();
                    for k in 0..1 + rng.below(3) {
                        let n = if k == 0 {
                            n
                        } else {
                            names[rng.below(names.len())]
                        };
                        if files.iter().any(|(f, _)| *f == n) {
                            continue;
                        }
                        let cur = read(r.clone(), head, n).await;
                        files.push((n, edit(&mut rng, &mut tag, cur)));
                    }
                    let files: Vec<(&str, Option<&[u8]>)> =
                        files.iter().map(|(f, t)| (*f, Some(&t[..]))).collect();
                    commit_bytes(&r, &[head], &files).await
                }
                6 if read(r.clone(), head, n).await.is_some() => {
                    commit(&r, &[head], &[(n, None)]).await
                }
                7 => {
                    // A binary version, which blame refuses.
                    commit_bytes(&r, &[head], &[(n, Some(&[0xff, 0xfe, b'\n'][..]))]).await
                }
                8 | 9 => {
                    let Some(cur) = read(r.clone(), head, n).await else {
                        continue;
                    };
                    // To a free name, or out of the tracked set.
                    let mut to = format!("old/{tag}");
                    tag += 1;
                    for o in names {
                        if read(r.clone(), head, o).await.is_none() && rng.below(2) == 0 {
                            to = o.to_string();
                        }
                    }
                    renames += 1;
                    commit_bytes(&r, &[head], &[(n, None), (&to, Some(&cur[..]))]).await
                }
                10 | 11 => {
                    // Work on a side branch, merged later.
                    let base = side.unwrap_or(head);
                    let cur = read(r.clone(), base, n).await;
                    let t = edit(&mut rng, &mut tag, cur);
                    let s = commit_bytes(&r, &[base], &[(n, Some(&t[..]))]).await;
                    all.push(s);
                    side = Some(s);
                    continue;
                }
                _ => {
                    let Some(s) = side.take() else {
                        continue;
                    };
                    // Theirs, ours, or both for one file they disagree on.
                    let mut files: Vec<(&str, Option<Vec<u8>>)> = Vec::new();
                    for o in names {
                        let (ours, theirs) =
                            (read(r.clone(), head, o).await, read(r.clone(), s, o).await);
                        if ours == theirs || !files.is_empty() {
                            continue;
                        }
                        let t = match (rng.below(3), &ours, &theirs) {
                            (0, _, _) => theirs.clone(),
                            (1, Some(a), Some(b)) => Some([&a[..], &b[..]].concat()),
                            _ => continue,
                        };
                        files.push((o, t));
                    }
                    merges += 1;
                    let files: Vec<(&str, Option<&[u8]>)> =
                        files.iter().map(|(f, t)| (*f, t.as_deref())).collect();
                    commit_bytes(&r, &[head, s], &files).await
                }
            };
            r.meta()
                .cas_ref("main", Some(head), Some(next))
                .await
                .unwrap();
            head = next;
            mains.push(next);
            all.push(next);
            if rng.below(3) == 0 {
                let _ = r.blame(head, &rp(names[rng.below(names.len())])).await;
            }
            if step % 3 == 0 {
                warm_pass(&mut w, &mut b).await;
            }
        }
        warm_pass(&mut w, &mut b).await;
        assert!(
            merges >= 3 && renames >= 2,
            "seed {seed}: {merges} merges, {renames} renames"
        );

        // Lands whose files' blames are cached there: blamed before, since
        // the last merge, rename or binary version of the file.
        let mut warm = 0;
        for &c in &mains {
            let cs = r.get_changeset(c).await.unwrap();
            for (p, ch) in &cs.changes {
                if matches!(ch, FileChange::Modify { .. }) {
                    let row = r.meta().get_derived(KIND, &cache_key(c, p)).await.unwrap();
                    warm += usize::from(row.is_some());
                }
            }
        }
        assert!(warm >= 10, "seed {seed}: {warm} warm");

        // Every blame cached, by the warmer or a request, is the cold one,
        // and so is every request answered from them.
        let mut extended = 0;
        for &c in &all {
            for f in r.list_files(c, "").await.unwrap() {
                let cold = r.blame_walk_in(c, &f.path, None, false).await;
                let row = r
                    .meta()
                    .get_derived(KIND, &cache_key(c, &f.path))
                    .await
                    .unwrap();
                let answer = r.blame_walk(c, &f.path, true, true).await;
                match (cold, answer) {
                    (Ok((cold, _)), Ok((answer, _))) => {
                        let bytes = postcard::to_stdvec(&cold).unwrap();
                        if let Some(row) = row {
                            assert_eq!(
                                &row[..],
                                &bytes[..],
                                "seed {seed}: row of {} at {c}",
                                f.path
                            );
                            extended += 1;
                        }
                        assert_eq!(answer, cold, "seed {seed}: {} at {c}", f.path);
                    }
                    (Err(Error::Invalid(_)), Err(Error::Invalid(_))) => {
                        assert!(row.is_none());
                    }
                    (a, b) => panic!("seed {seed}: {} at {c}: {a:?} vs {b:?}", f.path),
                }
            }
        }
        assert!(extended >= warm, "seed {seed}: {extended} rows compared");
        total_extended += extended;
    }
    assert!(total_extended >= 100, "{total_extended} rows compared");
}

/// A tracked hot file's blame is extended at every land, so a request at
/// the head is a few reads; files nobody blamed cost the warmer no blame
/// reads. A merge that changes the file breaks its chain: the warmer stops
/// on that ref without reading, until a request blames it again.
#[tokio::test]
async fn the_warmer_extends_a_tracked_file_land_by_land() {
    use crate::blame::{cache_key, KIND};
    let (r, meta) = slow_repo();
    let path = rp("hot");
    let mut text = String::from("0\n");
    let root = commit(&r, &[], &[("hot", Some(&text)), ("cold", Some("c\n"))]).await;
    r.meta().cas_ref("main", None, Some(root)).await.unwrap();
    let (mut w, mut b) = warmers(&r);
    warm_pass(&mut w, &mut b).await;
    let cached = |c: ChangesetId| {
        let r = r.clone();
        let path = path.clone();
        async move {
            r.meta()
                .get_derived(KIND, &cache_key(c, &path))
                .await
                .unwrap()
                .is_some()
        }
    };
    let land = |head: ChangesetId, files: Vec<(String, String)>| {
        let r = r.clone();
        async move {
            let files: Vec<(&str, Option<&str>)> = files
                .iter()
                .map(|(p, t)| (p.as_str(), Some(t.as_str())))
                .collect();
            let next = commit(&r, &[head], &files).await;
            r.meta()
                .cas_ref("main", Some(head), Some(next))
                .await
                .unwrap();
            next
        }
    };
    // Nobody blamed it yet: lands cost no blame reads.
    let mut head = root;
    for k in 1..=8 {
        text.push_str(&format!("{k}\n"));
        head = land(head, vec![("hot".into(), text.clone())]).await;
    }
    let before = meta.blame_reads.load(Ordering::Relaxed);
    let report = warm_pass(&mut w, &mut b).await;
    assert_eq!(report.written + report.missed, 0);
    assert_eq!(meta.blame_reads.load(Ordering::Relaxed), before);
    assert!(!cached(head).await);

    r.blame(head, &path).await.unwrap();
    let mut changers = Vec::new();
    for k in 9..=48 {
        text.push_str(&format!("{k}\n"));
        head = land(head, vec![("hot".into(), text.clone())]).await;
        changers.push(head);
        if k % 4 == 0 {
            // Several lands in one pass, still in order.
            warm_pass(&mut w, &mut b).await;
        }
    }
    for &c in &changers {
        assert!(cached(c).await);
    }
    // Lands of another file, then a request at the head: the newest
    // changer's row in a few reads, with nothing examined.
    for k in 0..3 {
        head = land(head, vec![("cold".into(), format!("{k}\n"))]).await;
    }
    warm_pass(&mut w, &mut b).await;
    let before = meta.reads.load(Ordering::Relaxed);
    let (got, examined) = r.blame_walk(head, &path, true, true).await.unwrap();
    let reads = meta.reads.load(Ordering::Relaxed) - before;
    assert_eq!(examined, 1);
    assert!(reads <= 12, "{reads} reads for a warm blame");
    assert_eq!(
        got,
        r.blame_walk_in(head, &path, None, false).await.unwrap().0
    );

    // A merge that changes the file: not extended, and neither is the
    // next change, whose blame would follow the merged branch.
    let s = commit(&r, &[head], &[("hot", Some(&format!("side\n{text}")))]).await;
    text = format!("side\n{text}merged\n");
    let m = commit(&r, &[head, s], &[("hot", Some(&text))]).await;
    r.meta().cas_ref("main", Some(head), Some(m)).await.unwrap();
    head = m;
    text.push_str("after\n");
    head = land(head, vec![("hot".into(), text.clone())]).await;
    let report = warm_pass(&mut w, &mut b).await;
    assert_eq!(report.written, 0);
    assert!(!cached(m).await && !cached(head).await);
    // Cold on this ref now: the next land costs no blame read.
    text.push_str("again\n");
    head = land(head, vec![("hot".into(), text.clone())]).await;
    w.pass().await.unwrap();
    let before = meta.blame_reads.load(Ordering::Relaxed);
    let report = b.pass().await.unwrap();
    assert_eq!((report.written, report.missed), (0, 0));
    assert_eq!(meta.blame_reads.load(Ordering::Relaxed), before);
    // A request caches through the merge and tracks the file again; the
    // warmer extends from there.
    r.blame(head, &path).await.unwrap();
    text.push_str("later\n");
    head = land(head, vec![("hot".into(), text.clone())]).await;
    let report = warm_pass(&mut w, &mut b).await;
    assert_eq!(report.written, 1);
    assert!(cached(head).await);
    let (row, _) = r.blame_walk_in(head, &path, None, false).await.unwrap();
    let stored = r
        .meta()
        .get_derived(KIND, &cache_key(head, &path))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&stored[..], &postcard::to_stdvec(&row).unwrap()[..]);
}

/// The warmer takes the gate's permit before working and does nothing
/// while the gate refuses: the lands wait for a pass it admits, which
/// catches up in order.
#[tokio::test]
async fn the_blame_warmer_waits_for_its_gate() {
    use std::sync::atomic::AtomicBool;
    let r = Repo::new(
        Arc::new(MemoryBlobStore::new()),
        Arc::new(MemoryMetaStore::new()),
    );
    let path = rp("f");
    let mut text = String::from("0\n");
    let mut head = commit(&r, &[], &[("f", Some(&text))]).await;
    r.meta().cas_ref("main", None, Some(head)).await.unwrap();
    let open = Arc::new(AtomicBool::new(false));
    let taken = Arc::new(AtomicUsize::new(0));
    let gate: crate::blame_warm::WarmGate = {
        let (open, taken) = (open.clone(), taken.clone());
        Arc::new(move || {
            open.load(Ordering::Relaxed).then(|| {
                taken.fetch_add(1, Ordering::Relaxed);
                Box::new(()) as crate::BlameHold
            })
        })
    };
    let mut w = crate::warm::Warmer::new(r.clone(), false);
    let mut b = crate::blame_warm::BlameWarmer::new(r.clone()).with_gate(gate);
    warm_pass(&mut w, &mut b).await;
    r.blame(head, &path).await.unwrap();
    for k in 1..=5 {
        text.push_str(&format!("{k}\n"));
        let next = commit(&r, &[head], &[("f", Some(&text))]).await;
        r.meta()
            .cas_ref("main", Some(head), Some(next))
            .await
            .unwrap();
        head = next;
    }
    let report = warm_pass(&mut w, &mut b).await;
    assert!(report.shed);
    assert_eq!(report.written, 0);
    open.store(true, Ordering::Relaxed);
    let report = warm_pass(&mut w, &mut b).await;
    assert_eq!((report.shed, report.written), (false, 5));
    assert_eq!(taken.load(Ordering::Relaxed), 1);
    // Nothing new: no permit taken.
    warm_pass(&mut w, &mut b).await;
    assert_eq!(taken.load(Ordering::Relaxed), 1);
}

/// The tracked set is bounded, forgets the least recently blamed first,
/// and forgets files nobody blamed for a while; a blame warms a file again
/// on every ref.
#[test]
fn the_tracked_files_are_bounded_and_expire() {
    use crate::blame_warm::BlameTracker;
    let t = BlameTracker::new(2, Duration::from_secs(3600));
    t.track(&rp("a"));
    t.track(&rp("b"));
    t.track(&rp("c"));
    assert_eq!(t.len(), 2);
    assert!(!t.wants_for_tests(&rp("a"), "main"));
    assert!(t.wants_for_tests(&rp("c"), "main"));
    t.cool_for_tests(&rp("c"), "main");
    assert!(!t.wants_for_tests(&rp("c"), "main"));
    assert!(t.wants_for_tests(&rp("c"), "review/x"));
    t.track(&rp("c"));
    assert!(t.wants_for_tests(&rp("c"), "main"));
    let off = BlameTracker::new(0, Duration::from_secs(3600));
    off.track(&rp("a"));
    assert_eq!(off.len(), 0);
    let brief = BlameTracker::new(4, Duration::ZERO);
    brief.track(&rp("a"));
    assert!(!brief.wants_for_tests(&rp("a"), "main"));
    assert!(!brief.any_for_tests());
    assert_eq!(brief.len(), 0);
}
