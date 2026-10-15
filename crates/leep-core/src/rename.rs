//! Rename detection: pair the files a diff deleted with the files it added.
//!
//! Changesets record a rename as a delete plus an add, so renames are found
//! when displaying, as git does (ADR 0016). The algorithm is pure so the
//! server and the CLI's working-copy diff share it; callers supply file
//! contents, because only they can read blobs.
//!
//! Pairing runs in stages, each over what the previous one left:
//! 1. **Exact**: same content hash and same kind. Always runs, in linear
//!    time. Identical files pair in sorted path order, so a file copied
//!    several times pairs once and the copies stay adds.
//! 2. **Same content, kind changed** (regular to executable): similarity 100,
//!    `exact: false`, so the mode change is still shown.
//! 3. **Similar**: regular and executable text files only. The score of two
//!    files is `floor(200 * common / (bytes_a + bytes_b))`, where a file is
//!    split into lines (each keeps its `\n`; a final unterminated line is a
//!    line) and `common` is the multiset intersection of the lines, weighted
//!    by line length in bytes. Identical files score 100, files with no line
//!    in common 0. Pairs scoring at least the threshold are taken greedily,
//!    highest score first, then by deleted path, then by added path.
//!
//! Symlinks pair only in stage 1. Binary files (NUL or invalid UTF-8) and
//! files over [`MAX_SIMILARITY_BYTES`] pair only in stages 1 and 2.
//!
//! Similarity work is bounded: with more than `max_pairs` deleted-by-added
//! candidate pairs, or more than `max_bytes` of candidate content, stage 3 is
//! skipped and the result is marked `truncated`. This is git's rename limit
//! with the limit expressed as a pair count: its default of 1000 means
//! 1000 x 1000.

use std::collections::{HashMap, VecDeque};

use crate::api::{DiffEntry, FileInfo, RenameEntry};
use crate::{FileKind, Hash};

/// Minimum similarity, in percent, that makes two files a rename.
pub const DEFAULT_THRESHOLD: u8 = 50;
/// Candidate pairs (deleted x added) above which similarity is skipped.
pub const DEFAULT_MAX_PAIRS: u64 = 1000 * 1000;
/// Candidate content above which similarity is skipped, so memory stays
/// bounded however large the files are.
pub const DEFAULT_MAX_BYTES: u64 = 256 * 1024 * 1024;
/// Largest file compared by content, the same cap as the code search index.
pub const MAX_SIMILARITY_BYTES: u64 = 1024 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RenameOptions {
    /// 0 to 100. At 0 every remaining text pair matches.
    pub threshold: u8,
    pub max_pairs: u64,
    pub max_bytes: u64,
}

impl Default for RenameOptions {
    fn default() -> Self {
        RenameOptions {
            threshold: DEFAULT_THRESHOLD,
            max_pairs: DEFAULT_MAX_PAIRS,
            max_bytes: DEFAULT_MAX_BYTES,
        }
    }
}

/// The detected renames and whether similarity matching was skipped.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Detected {
    /// Sorted by `to`.
    pub renames: Vec<RenameEntry>,
    pub truncated: bool,
}

/// Deleted and added files of a diff. Entries with both sides (modified
/// files) are neither.
pub fn split_entries(entries: &[DiffEntry]) -> (Vec<FileInfo>, Vec<FileInfo>) {
    let mut deleted = Vec::new();
    let mut added = Vec::new();
    for e in entries {
        match (&e.old, &e.new) {
            (Some(old), None) => deleted.push(old.clone()),
            (None, Some(new)) => added.push(new.clone()),
            _ => {}
        }
    }
    (deleted, added)
}

/// FNV-1a, spelled out so scores do not depend on a std hasher's algorithm.
fn fnv1a(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in bytes {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

/// The line multiset of one text file, enough to score it against another
/// without keeping its bytes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Fingerprint {
    total: u64,
    /// `(line hash, count, line length)`, sorted by hash.
    lines: Vec<(u64, u32, u32)>,
}

impl Fingerprint {
    /// `None` when the content is not compared by lines: over
    /// [`MAX_SIMILARITY_BYTES`], containing NUL or not valid UTF-8.
    pub fn of(bytes: &[u8]) -> Option<Fingerprint> {
        if bytes.len() as u64 > MAX_SIMILARITY_BYTES
            || bytes.contains(&0)
            || std::str::from_utf8(bytes).is_err()
        {
            return None;
        }
        let mut lines: Vec<(u64, u32, u32)> = bytes
            .split_inclusive(|&b| b == b'\n')
            .map(|l| (fnv1a(l), 1, l.len() as u32))
            .collect();
        lines.sort_unstable();
        let mut merged: Vec<(u64, u32, u32)> = Vec::with_capacity(lines.len());
        for (h, n, len) in lines {
            match merged.last_mut() {
                Some(last) if last.0 == h => last.1 += n,
                _ => merged.push((h, n, len)),
            }
        }
        Some(Fingerprint {
            total: bytes.len() as u64,
            lines: merged,
        })
    }

    /// Similarity with `other`, 0 to 100 (see the module doc).
    pub fn similarity(&self, other: &Fingerprint) -> u8 {
        let sum = self.total + other.total;
        if sum == 0 {
            return 100;
        }
        let (mut i, mut j, mut common) = (0, 0, 0u64);
        while i < self.lines.len() && j < other.lines.len() {
            let (a, b) = (self.lines[i], other.lines[j]);
            match a.0.cmp(&b.0) {
                std::cmp::Ordering::Less => i += 1,
                std::cmp::Ordering::Greater => j += 1,
                std::cmp::Ordering::Equal => {
                    common += u64::from(a.1.min(b.1)) * u64::from(a.2);
                    i += 1;
                    j += 1;
                }
            }
        }
        (200 * common / sum).min(100) as u8
    }
}

/// Upper bound of the score of two files from their sizes alone: the most
/// they can share is the smaller one.
fn best_possible(a: u64, b: u64) -> u8 {
    let sum = a + b;
    if sum == 0 {
        return 100;
    }
    (200 * a.min(b) / sum).min(100) as u8
}

/// Stage 1 and 2 done; stage 3 waiting for contents. See [`detect_renames`]
/// for the one-call form.
pub struct RenameDetector {
    opts: RenameOptions,
    deleted: Vec<FileInfo>,
    added: Vec<FileInfo>,
    renames: Vec<RenameEntry>,
    /// Indexes into `deleted` and `added` that similarity may still pair.
    open_deleted: Vec<usize>,
    open_added: Vec<usize>,
    truncated: bool,
}

fn entry(from: &FileInfo, to: &FileInfo, similarity: u8, exact: bool) -> RenameEntry {
    RenameEntry {
        from: from.path.clone(),
        to: to.path.clone(),
        similarity,
        exact,
    }
}

impl RenameDetector {
    /// Pair what needs no content. Inputs may be in any order.
    pub fn new(mut deleted: Vec<FileInfo>, mut added: Vec<FileInfo>, opts: RenameOptions) -> Self {
        deleted.sort_by(|a, b| a.path.cmp(&b.path));
        added.sort_by(|a, b| a.path.cmp(&b.path));
        let mut used_del = vec![false; deleted.len()];
        let mut used_add = vec![false; added.len()];
        let mut renames = Vec::new();

        // Stage 1: same content and kind. Queues keep sorted path order.
        let mut by_key: HashMap<(Hash, FileKind), VecDeque<usize>> = HashMap::new();
        for (j, f) in added.iter().enumerate() {
            by_key.entry((f.content, f.kind)).or_default().push_back(j);
        }
        for (i, f) in deleted.iter().enumerate() {
            if let Some(j) = by_key
                .get_mut(&(f.content, f.kind))
                .and_then(VecDeque::pop_front)
            {
                (used_del[i], used_add[j]) = (true, true);
                renames.push(entry(f, &added[j], 100, true));
            }
        }

        // Stage 2: same bytes, regular <-> executable.
        let mut by_content: HashMap<Hash, VecDeque<usize>> = HashMap::new();
        for (j, f) in added.iter().enumerate() {
            if !used_add[j] && f.kind != FileKind::Symlink {
                by_content.entry(f.content).or_default().push_back(j);
            }
        }
        for (i, f) in deleted.iter().enumerate() {
            if used_del[i] || f.kind == FileKind::Symlink {
                continue;
            }
            if let Some(j) = by_content.get_mut(&f.content).and_then(VecDeque::pop_front) {
                (used_del[i], used_add[j]) = (true, true);
                renames.push(entry(f, &added[j], 100, false));
            }
        }

        // Stage 3 candidates: text-sized regular and executable files.
        let open = |files: &[FileInfo], used: &[bool]| -> Vec<usize> {
            (0..files.len())
                .filter(|&k| {
                    !used[k]
                        && files[k].kind != FileKind::Symlink
                        && files[k].size <= MAX_SIMILARITY_BYTES
                })
                .collect()
        };
        let mut open_deleted = open(&deleted, &used_del);
        let mut open_added = open(&added, &used_add);
        let pairs = open_deleted.len() as u64 * open_added.len() as u64;
        let bytes: u64 = open_deleted
            .iter()
            .map(|&i| deleted[i].size)
            .chain(open_added.iter().map(|&j| added[j].size))
            .sum();
        let truncated = pairs > opts.max_pairs || bytes > opts.max_bytes;
        if truncated || pairs == 0 {
            open_deleted.clear();
            open_added.clear();
        }
        RenameDetector {
            opts,
            deleted,
            added,
            renames,
            open_deleted,
            open_added,
            truncated,
        }
    }

    /// Content hashes [`Self::finish`] wants fingerprints for, sorted and
    /// without duplicates. Empty when similarity is skipped or has nothing
    /// to compare.
    pub fn wanted(&self) -> Vec<Hash> {
        let mut out: Vec<Hash> = self
            .open_deleted
            .iter()
            .map(|&i| self.deleted[i].content)
            .chain(self.open_added.iter().map(|&j| self.added[j].content))
            .collect();
        out.sort_unstable_by_key(|h| *h.as_bytes());
        out.dedup();
        out
    }

    /// Run the similarity stage. A file without a fingerprint (binary,
    /// unreadable) is left unpaired.
    pub fn finish(mut self, fingerprints: &HashMap<Hash, Fingerprint>) -> Detected {
        let threshold = self.opts.threshold.min(100);
        let mut scored: Vec<(u8, usize, usize)> = Vec::new();
        for &i in &self.open_deleted {
            let Some(fa) = fingerprints.get(&self.deleted[i].content) else {
                continue;
            };
            for &j in &self.open_added {
                let Some(fb) = fingerprints.get(&self.added[j].content) else {
                    continue;
                };
                if best_possible(fa.total, fb.total) < threshold {
                    continue;
                }
                let score = fa.similarity(fb);
                if score >= threshold {
                    scored.push((score, i, j));
                }
            }
        }
        // Inputs are sorted by path, so index order is path order.
        scored.sort_unstable_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)).then(a.2.cmp(&b.2)));
        let mut used_del = vec![false; self.deleted.len()];
        let mut used_add = vec![false; self.added.len()];
        for (score, i, j) in scored {
            if used_del[i] || used_add[j] {
                continue;
            }
            (used_del[i], used_add[j]) = (true, true);
            self.renames
                .push(entry(&self.deleted[i], &self.added[j], score, false));
        }
        self.renames
            .sort_by(|a, b| a.to.cmp(&b.to).then(a.from.cmp(&b.from)));
        Detected {
            renames: self.renames,
            truncated: self.truncated,
        }
    }
}

/// Detect renames in one call. `content` returns a file's bytes by content
/// hash, or `None` if unavailable; it is asked only for [`RenameDetector::wanted`]
/// hashes, each once.
pub fn detect_renames(
    deleted: Vec<FileInfo>,
    added: Vec<FileInfo>,
    opts: RenameOptions,
    mut content: impl FnMut(&Hash) -> Option<Vec<u8>>,
) -> Detected {
    let det = RenameDetector::new(deleted, added, opts);
    let fps: HashMap<Hash, Fingerprint> = det
        .wanted()
        .into_iter()
        .filter_map(|h| Some((h, Fingerprint::of(&content(&h)?)?)))
        .collect();
    det.finish(&fps)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RepoPath;

    fn file(path: &str, body: &[u8], kind: FileKind) -> FileInfo {
        FileInfo {
            path: RepoPath::new(path).unwrap(),
            content: Hash::of_blob(body),
            kind,
            size: body.len() as u64,
        }
    }

    fn reg(path: &str, body: &str) -> (FileInfo, Vec<u8>) {
        (
            file(path, body.as_bytes(), FileKind::Regular),
            body.as_bytes().to_vec(),
        )
    }

    /// Run detection over `(file, bytes)` pairs.
    fn run(
        del: &[(FileInfo, Vec<u8>)],
        add: &[(FileInfo, Vec<u8>)],
        opts: RenameOptions,
    ) -> Detected {
        let bodies: HashMap<Hash, Vec<u8>> = del
            .iter()
            .chain(add)
            .map(|(f, b)| (f.content, b.clone()))
            .collect();
        detect_renames(
            del.iter().map(|(f, _)| f.clone()).collect(),
            add.iter().map(|(f, _)| f.clone()).collect(),
            opts,
            |h| bodies.get(h).cloned(),
        )
    }

    fn pairs(d: &Detected) -> Vec<(&str, &str, u8, bool)> {
        d.renames
            .iter()
            .map(|r| (r.from.as_str(), r.to.as_str(), r.similarity, r.exact))
            .collect()
    }

    fn lines(n: usize, tag: &str) -> String {
        (0..n).map(|i| format!("{tag} line {i}\n")).collect()
    }

    #[test]
    fn exact_rename() {
        let d = run(
            &[reg("old.txt", "hello\n")],
            &[reg("dir/new.txt", "hello\n")],
            RenameOptions::default(),
        );
        assert_eq!(pairs(&d), [("old.txt", "dir/new.txt", 100, true)]);
        assert!(!d.truncated);
    }

    #[test]
    fn copies_pair_once_in_sorted_order() {
        // One deleted file, three added copies: the first by path pairs.
        let d = run(
            &[reg("a", "same\n")],
            &[reg("z", "same\n"), reg("m", "same\n"), reg("n", "same\n")],
            RenameOptions::default(),
        );
        assert_eq!(pairs(&d), [("a", "m", 100, true)]);
        // Several deleted and several added pair index by index.
        let d = run(
            &[reg("a/y", "s\n"), reg("a/x", "s\n")],
            &[reg("b/y", "s\n"), reg("b/x", "s\n")],
            RenameOptions::default(),
        );
        assert_eq!(
            pairs(&d),
            [("a/x", "b/x", 100, true), ("a/y", "b/y", 100, true)]
        );
    }

    #[test]
    fn exact_requires_the_same_kind_but_mode_change_is_still_a_rename() {
        let body = b"#!/bin/sh\necho hi\n";
        let del = [(file("run", body, FileKind::Regular), body.to_vec())];
        let add = [(file("bin/run", body, FileKind::Executable), body.to_vec())];
        let d = run(&del, &add, RenameOptions::default());
        assert_eq!(pairs(&d), [("run", "bin/run", 100, false)]);
        // Also for binary content, which similarity never reads.
        let bin = [0u8, 1, 2, 3];
        let del = [(file("a", &bin, FileKind::Regular), bin.to_vec())];
        let add = [(file("b", &bin, FileKind::Executable), bin.to_vec())];
        assert_eq!(
            pairs(&run(&del, &add, RenameOptions::default())),
            [("a", "b", 100, false)]
        );
    }

    #[test]
    fn similarity_around_the_threshold() {
        // Ten equal-length lines; changing k of them scores (10 - k) * 10.
        let base = lines(10, "x");
        let edit = |k: usize| -> String {
            base.lines()
                .enumerate()
                .map(|(i, l)| {
                    if i < k {
                        format!("{}\n", l.replace('x', "y"))
                    } else {
                        format!("{l}\n")
                    }
                })
                .collect()
        };
        for (k, score) in [(1, 90), (4, 60), (5, 50), (6, 40)] {
            let opts = RenameOptions::default();
            let d = run(&[reg("a", &base)], &[reg("b", &edit(k))], opts);
            if score >= 50 {
                assert_eq!(pairs(&d), [("a", "b", score, false)], "k={k}");
            } else {
                assert!(d.renames.is_empty(), "k={k}");
            }
        }
        // The threshold is configurable and inclusive.
        let at = |t: u8| RenameOptions {
            threshold: t,
            ..RenameOptions::default()
        };
        let (a, b) = (reg("a", &base), reg("b", &edit(6)));
        assert_eq!(
            run(std::slice::from_ref(&a), std::slice::from_ref(&b), at(60))
                .renames
                .len(),
            0
        );
        assert_eq!(
            run(std::slice::from_ref(&a), std::slice::from_ref(&b), at(40))
                .renames
                .len(),
            1
        );
        assert_eq!(run(&[a], &[b], at(0)).renames.len(), 1);
    }

    #[test]
    fn threshold_zero_pairs_unrelated_text_and_hundred_needs_identity() {
        let (a, b) = (reg("a", "one\n"), reg("b", "two\n"));
        let at = |t: u8| RenameOptions {
            threshold: t,
            ..RenameOptions::default()
        };
        assert_eq!(
            pairs(&run(
                std::slice::from_ref(&a),
                std::slice::from_ref(&b),
                at(0)
            ))
            .len(),
            1
        );
        assert!(
            run(std::slice::from_ref(&a), std::slice::from_ref(&b), at(1))
                .renames
                .is_empty()
        );
        let (a2, b2) = (reg("a", "one\ntwo\n"), reg("b", "one\ntwo\nthree\n"));
        assert!(run(&[a2], &[b2], at(100)).renames.is_empty());
    }

    #[test]
    fn score_is_weighted_by_line_length_and_counts_repeats() {
        let fp = |s: &str| Fingerprint::of(s.as_bytes()).unwrap();
        // A long shared line outweighs a short different one.
        let a = fp("a long shared line here\nx\n");
        let b = fp("a long shared line here\ny\n");
        assert_eq!(u64::from(a.similarity(&b)), 200 * 24 / 52);
        // Repeats count with multiplicity: one of two "r" lines is shared.
        assert_eq!(fp("r\nr\n").similarity(&fp("r\ns\n")), 50);
        // Order does not matter.
        assert_eq!(fp("1\n2\n3\n").similarity(&fp("3\n2\n1\n")), 100);
        // A final line without a newline is a different line: 2 shared of 3 + 4 bytes.
        assert_eq!(fp("a\nb").similarity(&fp("a\nb\n")), 57);
        assert_eq!(fp("same\n").similarity(&fp("same\n")), 100);
        assert_eq!(fp("a\n").similarity(&fp("b\n")), 0);
    }

    #[test]
    fn greedy_pairs_best_first_then_path_order() {
        // `b1` is a near copy of `a1`. `b2` matches `a1` and `a2` equally
        // well, but `a1` is taken by the better pair first.
        let a1 = lines(10, "p");
        let mut b1 = lines(9, "p");
        b1.push_str("changed\n");
        let a2 = lines(10, "q");
        let mut b2 = lines(5, "q");
        b2.push_str(&lines(5, "p"));
        let d = run(
            &[reg("a1", &a1), reg("a2", &a2)],
            &[reg("b1", &b1), reg("b2", &b2)],
            RenameOptions::default(),
        );
        assert_eq!(
            pairs(&d),
            [("a1", "b1", 90, false), ("a2", "b2", 50, false)]
        );
        // Equal scores: lower deleted path first, then lower added path.
        let d = run(
            &[reg("a", "x\ny\n"), reg("b", "x\ny\n")],
            &[reg("c", "x\ny\nz\n"), reg("d", "x\ny\nz\n")],
            RenameOptions::default(),
        );
        assert_eq!(pairs(&d), [("a", "c", 80, false), ("b", "d", 80, false)]);
    }

    #[test]
    fn binary_files_pair_only_when_exact() {
        let bin = |tag: u8| -> Vec<u8> { [vec![0u8, tag], vec![7; 64]].concat() };
        let f = |p: &str, b: Vec<u8>| (file(p, &b, FileKind::Regular), b);
        let (a, b) = (f("a.bin", bin(1)), f("b.bin", bin(2)));
        assert!(
            run(std::slice::from_ref(&a), &[b], RenameOptions::default())
                .renames
                .is_empty()
        );
        let c = f("c.bin", bin(1));
        assert_eq!(
            pairs(&run(&[a], &[c], RenameOptions::default())),
            [("a.bin", "c.bin", 100, true)]
        );
        // Invalid UTF-8 without a NUL is binary too.
        let bad = |p: &str, tail: u8| {
            let b = vec![0xff, 0xfe, tail];
            (file(p, &b, FileKind::Regular), b)
        };
        assert!(
            run(&[bad("a", 1)], &[bad("b", 2)], RenameOptions::default())
                .renames
                .is_empty()
        );
    }

    #[test]
    fn symlinks_pair_only_when_exact() {
        let link = |p: &str, t: &str| {
            let b = t.as_bytes().to_vec();
            (file(p, &b, FileKind::Symlink), b)
        };
        let d = run(
            &[link("l1", "target"), link("l2", "target-a")],
            &[link("m1", "target"), link("m2", "target-b")],
            RenameOptions::default(),
        );
        assert_eq!(pairs(&d), [("l1", "m1", 100, true)]);
        // A symlink never pairs with a file of the same bytes.
        let d = run(
            &[link("l", "x\n")],
            &[reg("f", "x\n")],
            RenameOptions::default(),
        );
        assert!(d.renames.is_empty());
    }

    #[test]
    fn empty_files() {
        let d = run(
            &[reg("e1", ""), reg("e2", "")],
            &[reg("f1", ""), reg("f2", ""), reg("f3", "")],
            RenameOptions::default(),
        );
        assert_eq!(
            pairs(&d),
            [("e1", "f1", 100, true), ("e2", "f2", 100, true)]
        );
        // An empty file shares nothing with a text file.
        let d = run(
            &[reg("e", "")],
            &[reg("t", "text\n")],
            RenameOptions::default(),
        );
        assert!(d.renames.is_empty());
        let d = run(
            &[reg("e", "")],
            &[reg("t", "text\n")],
            RenameOptions {
                threshold: 0,
                ..RenameOptions::default()
            },
        );
        assert_eq!(pairs(&d), [("e", "t", 0, false)]);
    }

    #[test]
    fn too_many_candidates_skip_similarity_but_not_exact() {
        let del: Vec<_> = (0..4)
            .map(|i| reg(&format!("d{i}"), &lines(10, &format!("d{i}"))))
            .collect();
        let mut add: Vec<_> = (0..4)
            .map(|i| {
                let mut body = lines(10, &format!("d{i}"));
                body.push_str("more\n");
                reg(&format!("a{i}"), &body)
            })
            .collect();
        // One exact pair rides along: d0's content, unchanged.
        add.push(reg("exact", &lines(10, "d0")));
        let tight = RenameOptions {
            max_pairs: 11,
            ..RenameOptions::default()
        };
        let d = run(&del, &add, tight);
        assert!(d.truncated);
        assert_eq!(pairs(&d), [("d0", "exact", 100, true)]);
        // 3 deleted x 4 added = 12 pairs fit exactly; 11 do not.
        let fits = RenameOptions {
            max_pairs: 12,
            ..RenameOptions::default()
        };
        let d = run(&del, &add, fits);
        assert!(!d.truncated);
        assert_eq!(d.renames.len(), 4);
        // The byte budget truncates too.
        let small = RenameOptions {
            max_bytes: 10,
            ..RenameOptions::default()
        };
        assert!(run(&del, &add, small).truncated);
    }

    #[test]
    fn nothing_to_pair_is_not_truncated() {
        let d = run(&[], &[reg("a", "x\n")], RenameOptions::default());
        assert_eq!(d, Detected::default());
        let d = run(&[reg("a", "x\n")], &[], RenameOptions::default());
        assert_eq!(d, Detected::default());
    }

    #[test]
    fn large_files_pair_only_when_exact() {
        let big = |tag: &str| "x".repeat(MAX_SIMILARITY_BYTES as usize) + tag + "\n";
        let (a, b) = (reg("a", &big("1")), reg("b", &big("2")));
        assert!(
            run(std::slice::from_ref(&a), &[b], RenameOptions::default())
                .renames
                .is_empty()
        );
        assert_eq!(
            run(
                std::slice::from_ref(&a),
                &[reg("c", &big("1"))],
                RenameOptions::default()
            )
            .renames
            .len(),
            1
        );
        assert!(Fingerprint::of(big("1").as_bytes()).is_none());
    }

    #[test]
    fn output_is_independent_of_input_order() {
        let del = [
            reg("d/a", &lines(8, "a")),
            reg("d/b", &lines(8, "b")),
            reg("d/c", "same\n"),
        ];
        let mut a8 = lines(8, "a");
        a8.push_str("tail\n");
        let add = [
            reg("n/a", &a8),
            reg("n/b", &lines(8, "b")),
            reg("n/c", "same\n"),
        ];
        let want = run(&del, &add, RenameOptions::default());
        assert_eq!(want.renames.len(), 3);
        let mut rev_del = del.to_vec();
        rev_del.reverse();
        let mut rev_add = add.to_vec();
        rev_add.rotate_left(1);
        assert_eq!(run(&rev_del, &rev_add, RenameOptions::default()), want);
        let targets: Vec<&str> = want.renames.iter().map(|r| r.to.as_str()).collect();
        assert!(targets.windows(2).all(|w| w[0] < w[1]));
    }

    #[test]
    fn split_entries_ignores_modifications() {
        let (a, _) = reg("a", "1\n");
        let (b, _) = reg("b", "2\n");
        let (b2, _) = reg("b", "3\n");
        let entries = vec![
            DiffEntry {
                path: a.path.clone(),
                old: Some(a.clone()),
                new: None,
            },
            DiffEntry {
                path: b.path.clone(),
                old: Some(b.clone()),
                new: Some(b2),
            },
            DiffEntry {
                path: b.path.clone(),
                old: None,
                new: Some(b.clone()),
            },
        ];
        let (d, n) = split_entries(&entries);
        assert_eq!((d, n), (vec![a], vec![b]));
    }

    /// splitmix64: a small seeded generator, so failures reproduce.
    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = self.0;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^ (z >> 31)
        }
        fn below(&mut self, n: u64) -> u64 {
            self.next() % n
        }
    }

    fn random_body(rng: &mut Rng) -> (Vec<u8>, FileKind) {
        match rng.below(10) {
            0 => (vec![0, rng.below(4) as u8, 9], FileKind::Regular),
            1 => (
                format!("target{}", rng.below(3)).into_bytes(),
                FileKind::Symlink,
            ),
            _ => {
                let n = rng.below(12);
                // Few distinct lines, so exact copies and near misses are common.
                let body: String = (0..n).map(|_| format!("line {}\n", rng.below(8))).collect();
                let kind = if rng.below(4) == 0 {
                    FileKind::Executable
                } else {
                    FileKind::Regular
                };
                (body.into_bytes(), kind)
            }
        }
    }

    #[test]
    fn random_trees_keep_the_invariants() {
        for seed in 0..300u64 {
            let mut rng = Rng(seed);
            let mut bodies: HashMap<Hash, Vec<u8>> = HashMap::new();
            let mut side = |prefix: &str, n: u64, rng: &mut Rng| -> Vec<FileInfo> {
                (0..n)
                    .map(|i| {
                        let (b, k) = random_body(rng);
                        let f = file(&format!("{prefix}/f{}-{i}", rng.below(5)), &b, k);
                        bodies.insert(f.content, b);
                        f
                    })
                    .collect()
            };
            let del = side("old", rng.below(14), &mut rng);
            let add = side("new", rng.below(14), &mut rng);
            let opts = RenameOptions {
                threshold: rng.below(101) as u8,
                max_pairs: if rng.below(5) == 0 { 6 } else { u64::MAX },
                ..RenameOptions::default()
            };
            let d = detect_renames(del.clone(), add.clone(), opts, |h| bodies.get(h).cloned());
            let by_path = |files: &[FileInfo], p: &RepoPath| -> Option<FileInfo> {
                files.iter().find(|f| &f.path == p).cloned()
            };
            let (mut froms, mut tos) = (Vec::new(), Vec::new());
            for r in &d.renames {
                let (from, to) = (
                    by_path(&del, &r.from).unwrap_or_else(|| panic!("seed {seed}: from added")),
                    by_path(&add, &r.to).unwrap_or_else(|| panic!("seed {seed}: to deleted")),
                );
                froms.push(&r.from);
                tos.push(&r.to);
                assert!(r.similarity <= 100);
                if r.exact {
                    assert_eq!(
                        (from.content, from.kind),
                        (to.content, to.kind),
                        "seed {seed}"
                    );
                    assert_eq!(r.similarity, 100);
                }
                if from.kind == FileKind::Symlink || to.kind == FileKind::Symlink {
                    assert!(r.exact, "seed {seed}: inexact symlink rename");
                }
                if !r.exact {
                    assert!(r.similarity >= opts.threshold, "seed {seed}");
                }
            }
            for list in [&mut froms, &mut tos] {
                let n = list.len();
                list.sort();
                list.dedup();
                assert_eq!(list.len(), n, "seed {seed}: path in two renames");
            }
            assert!(d.renames.windows(2).all(|w| w[0].to < w[1].to));
            // Same input, same output.
            let again = detect_renames(del, add, opts, |h| bodies.get(h).cloned());
            assert_eq!(again, d, "seed {seed}");
        }
    }
}
