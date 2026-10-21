//! Paged file listings and diffs: pages concatenate to the unpaged answer in
//! path order (also across file/directory swaps and renames), a page reads
//! only the trees it needs, and cursors are checked as untrusted input.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use leep_core::api::{DiffEntry, DiffResponse, FileInfo};
use leep_core::memory::{MemoryBlobStore, MemoryMetaStore};
use leep_core::rename::RenameOptions;
use leep_core::store::{BlobMeta, BlobStore};
use leep_core::{ChangesetId, Error, Hash, Result};

use crate::cursor::{self, CursorKind, Fingerprint, PathPos};
use crate::filter_log::tests::Lcg;
use crate::tests::{commit, repo};
use crate::Repo;

/// Names chosen to sort around `/`: `-` and `.` sort before it, `0` and
/// `~` after, so a walk that ordered by name instead of by path would put
/// `a/...` in the wrong place.
const NAMES: [&str; 8] = ["a", "a.txt", "a-b", "a0", "b", "B", "z~", "é"];

fn conflicts(paths: &BTreeSet<String>, p: &str) -> bool {
    paths.contains(p)
        || paths
            .iter()
            .any(|q| p.starts_with(&format!("{q}/")) || q.starts_with(&format!("{p}/")))
}

fn random_path(rng: &mut Lcg) -> String {
    let depth = 1 + rng.below(3);
    (0..depth)
        .map(|_| NAMES[rng.below(NAMES.len())])
        .collect::<Vec<_>>()
        .join("/")
}

fn body(path: &str, v: usize) -> String {
    format!("{path} v{v}\nline two\nline three\n")
}

/// Two changesets: a random tree, and a child that deletes, modifies,
/// renames and adds files, and swaps a file for a directory and back.
async fn two_trees(r: &Repo, rng: &mut Lcg) -> (ChangesetId, ChangesetId) {
    let mut paths: BTreeSet<String> = ["sw".to_string(), "dir/x".to_string()].into();
    while paths.len() < 40 {
        let p = random_path(rng);
        if !conflicts(&paths, &p) {
            paths.insert(p);
        }
    }
    let bodies: BTreeMap<String, String> = paths.iter().map(|p| (p.clone(), body(p, 1))).collect();
    let files: Vec<(&str, Option<&str>)> = bodies
        .iter()
        .map(|(p, b)| (p.as_str(), Some(b.as_str())))
        .collect();
    let c1 = commit(r, &[], &files, "c1").await;

    let mut change: BTreeMap<String, Option<String>> = BTreeMap::new();
    let mut now = paths.clone();
    for p in ["sw", "dir/x"] {
        change.insert(p.into(), None);
        now.remove(p);
    }
    let mut deleted = Vec::new();
    for p in &paths {
        if now.contains(p) {
            match rng.below(5) {
                0 => {
                    change.insert(p.clone(), None);
                    now.remove(p);
                    deleted.push(p.clone());
                }
                1 => {
                    change.insert(p.clone(), Some(body(p, 2)));
                }
                _ => {}
            }
        }
    }
    let mut adds = vec!["sw/in".to_string(), "dir".to_string()];
    while adds.len() < 15 {
        adds.push(random_path(rng));
    }
    for (i, p) in adds.into_iter().enumerate() {
        if conflicts(&now, &p) {
            continue;
        }
        // Some adds carry a deleted file's bytes, so renames are found.
        let text = match deleted.get(i) {
            Some(d) if i % 2 == 0 => bodies[d].clone(),
            _ => body(&p, 3),
        };
        now.insert(p.clone());
        change.insert(p, Some(text));
    }
    let files: Vec<(&str, Option<&str>)> = change
        .iter()
        .map(|(p, b)| (p.as_str(), b.as_deref()))
        .collect();
    let c2 = commit(r, &[c1], &files, "c2").await;
    (c1, c2)
}

async fn file_pages(r: &Repo, id: ChangesetId, prefix: &str, size: usize) -> Vec<FileInfo> {
    let mut out = Vec::new();
    let mut after: Option<String> = None;
    for _ in 0..10_000 {
        let page = r
            .list_files_page(id, prefix, Some(size), after.as_deref())
            .await
            .unwrap();
        assert!(page.files.len() <= size);
        out.extend(page.files.iter().cloned());
        let Some(next) = page.next else {
            return out;
        };
        assert_eq!(page.files.len(), size, "only a full page has a next");
        after = Some(next);
    }
    panic!("paging did not end");
}

async fn diff_pages(
    r: &Repo,
    from: ChangesetId,
    to: ChangesetId,
    prefix: &str,
    renames: Option<&RenameOptions>,
    size: usize,
) -> DiffResponse {
    let mut out = DiffResponse::default();
    let mut after: Option<String> = None;
    for _ in 0..10_000 {
        let page = r
            .diff_page(
                Some(from),
                to,
                prefix,
                renames,
                Some(size),
                after.as_deref(),
            )
            .await
            .unwrap();
        assert!(page.entries.len() <= size);
        let on_page: BTreeSet<_> = page.entries.iter().map(|e| &e.path).collect();
        assert!(page.renames.iter().all(|x| on_page.contains(&x.to)));
        out.entries.extend(page.entries.iter().cloned());
        out.renames.extend(page.renames.iter().cloned());
        out.renames_truncated |= page.renames_truncated;
        let Some(next) = page.next else {
            return out;
        };
        assert_eq!(page.entries.len(), size, "only a full page has a next");
        after = Some(next);
    }
    panic!("paging did not end");
}

/// The diff of two listings, independent of the tree walk.
fn reference_diff(old: &[FileInfo], new: &[FileInfo]) -> Vec<DiffEntry> {
    let old: BTreeMap<_, _> = old.iter().map(|f| (f.path.clone(), f)).collect();
    let new: BTreeMap<_, _> = new.iter().map(|f| (f.path.clone(), f)).collect();
    let paths: BTreeSet<_> = old.keys().chain(new.keys()).collect();
    paths
        .into_iter()
        .filter_map(|p| {
            let (o, n) = (old.get(p), new.get(p));
            (o.map(|f| (f.content, f.kind)) != n.map(|f| (f.content, f.kind))).then(|| DiffEntry {
                path: p.clone(),
                old: o.map(|f| (*f).clone()),
                new: n.map(|f| (*f).clone()),
            })
        })
        .collect()
}

#[tokio::test]
async fn file_and_diff_pages_concatenate_in_path_order() {
    for seed in 0..6 {
        let r = repo();
        let mut rng = Lcg(seed);
        let (c1, c2) = two_trees(&r, &mut rng).await;
        for id in [c1, c2] {
            for prefix in ["", "a", "a/", "sw", "nope"] {
                let all = r.list_files(id, prefix).await.unwrap();
                let mut sorted = all.clone();
                sorted.sort_by(|a, b| a.path.cmp(&b.path));
                assert_eq!(all, sorted, "seed {seed}");
                assert!(all
                    .iter()
                    .all(|f| f.path.is_within(prefix.trim_matches('/'))));
                let unpaged = r.list_files_page(id, prefix, None, None).await.unwrap();
                assert_eq!((&unpaged.files, &unpaged.next), (&all, &None));
                for size in [1, 2, 3, 7, all.len().max(1), all.len() + 1] {
                    let got = file_pages(&r, id, prefix, size).await;
                    assert_eq!(got, all, "seed {seed}, {prefix:?}, size {size}");
                }
            }
        }

        let full = r.diff(Some(c1), c2, "").await.unwrap();
        let old = r.list_files(c1, "").await.unwrap();
        let new = r.list_files(c2, "").await.unwrap();
        assert_eq!(full, reference_diff(&old, &new), "seed {seed}");
        assert!(full.iter().any(|e| e.path.as_str() == "sw/in"));
        assert!(full.iter().any(|e| e.path.as_str() == "dir"));
        let opts = RenameOptions::default();
        for prefix in ["", "a", "dir", "sw"] {
            for renames in [None, Some(&opts)] {
                let whole = r
                    .diff_page(Some(c1), c2, prefix, renames, None, None)
                    .await
                    .unwrap();
                assert_eq!(whole.next, None);
                assert_eq!(whole.entries, r.diff(Some(c1), c2, prefix).await.unwrap());
                if prefix.is_empty() && renames.is_some() {
                    assert!(!whole.renames.is_empty(), "seed {seed}");
                }
                for size in [1, 2, 5, whole.entries.len().max(1), 100] {
                    let got = diff_pages(&r, c1, c2, prefix, renames, size).await;
                    assert_eq!(got, whole, "seed {seed}, {prefix:?}, size {size}");
                }
            }
        }
    }
}

/// A memory store that counts the blobs (here: tree nodes) it is asked for.
struct Counting {
    inner: MemoryBlobStore,
    reads: AtomicUsize,
}

#[async_trait]
impl BlobStore for Counting {
    async fn put_with_hash(&self, hash: Hash, data: Bytes) -> Result<()> {
        self.inner.put_with_hash(hash, data).await
    }

    async fn get(&self, hash: &Hash) -> Result<Option<Bytes>> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        self.inner.get(hash).await
    }

    async fn get_many(&self, hashes: &[Hash]) -> Result<Vec<Option<Bytes>>> {
        self.reads.fetch_add(hashes.len(), Ordering::Relaxed);
        self.inner.get_many(hashes).await
    }

    async fn list_page(&self, after: Option<Hash>, limit: usize) -> Result<Vec<BlobMeta>> {
        self.inner.list_page(after, limit).await
    }

    async fn delete(&self, hash: &Hash) -> Result<()> {
        self.inner.delete(hash).await
    }
}

#[tokio::test]
async fn a_page_reads_only_the_trees_it_needs() {
    let blobs = Arc::new(Counting {
        inner: MemoryBlobStore::new(),
        reads: AtomicUsize::new(0),
    });
    let meta = Arc::new(MemoryMetaStore::new());
    let writer = Repo::new(blobs.clone(), meta.clone());
    let names: Vec<(String, String)> = (0..200)
        .flat_map(|d| (0..10).map(move |f| (format!("d{d:03}/f{f}"), format!("{d} {f}\n"))))
        .collect();
    let files: Vec<(&str, Option<&str>)> = names
        .iter()
        .map(|(p, b)| (p.as_str(), Some(b.as_str())))
        .collect();
    let c1 = commit(&writer, &[], &files, "c1").await;
    // A change in every directory, so the whole diff walks every pair.
    let edits: Vec<(String, String)> = (0..200)
        .map(|d| (format!("d{d:03}/f0"), format!("edit {d}\n")))
        .collect();
    let files: Vec<(&str, Option<&str>)> = edits
        .iter()
        .map(|(p, b)| (p.as_str(), Some(b.as_str())))
        .collect();
    let c2 = commit(&writer, &[c1], &files, "c2").await;

    // A fresh repo for each read, so no cache hides one.
    let fresh = || Repo::new(blobs.clone(), meta.clone());
    let count = || blobs.reads.load(Ordering::Relaxed);

    let n = count();
    assert_eq!(fresh().list_files(c1, "").await.unwrap().len(), 2000);
    let whole = count() - n;
    assert!(whole > 200, "{whole}");

    let n = count();
    let p = fresh()
        .list_files_page(c1, "", Some(5), None)
        .await
        .unwrap();
    let first = count() - n;
    assert_eq!(p.files.len(), 5);
    assert!(first < 20, "first page read {first} blobs");

    // Resuming deep in the listing skips the directories before it.
    let mid = fresh()
        .list_files_page(c1, "", Some(1005), None)
        .await
        .unwrap()
        .next
        .unwrap();
    let n = count();
    let p = fresh()
        .list_files_page(c1, "", Some(3), Some(&mid))
        .await
        .unwrap();
    let resumed = count() - n;
    let paths: Vec<_> = p.files.iter().map(|f| f.path.to_string()).collect();
    assert_eq!(paths, ["d100/f5", "d100/f6", "d100/f7"]);
    assert!(resumed < 20, "resumed page read {resumed} blobs");

    let n = count();
    assert_eq!(fresh().diff(Some(c1), c2, "").await.unwrap().len(), 200);
    let whole_diff = count() - n;
    assert!(whole_diff > 400, "{whole_diff}");

    let n = count();
    let p = fresh()
        .diff_page(Some(c1), c2, "", None, Some(4), None)
        .await
        .unwrap();
    let diff_page = count() - n;
    let paths: Vec<_> = p.entries.iter().map(|e| e.path.to_string()).collect();
    assert_eq!(paths, ["d000/f0", "d001/f0", "d002/f0", "d003/f0"]);
    assert!(p.next.is_some());
    assert!(diff_page < 30, "diff page read {diff_page} blobs");
}

fn is_invalid<T: std::fmt::Debug>(r: Result<T>) -> bool {
    matches!(r, Err(Error::Invalid(_)))
}

#[tokio::test]
async fn path_cursors_are_checked_and_may_point_past_the_end() {
    let r = repo();
    let c1 = commit(&r, &[], &[("a/x", Some("1")), ("b", Some("2"))], "c1").await;
    let c2 = commit(&r, &[c1], &[("a/x", Some("3")), ("c", Some("4"))], "c2").await;

    assert!(is_invalid(r.list_files_page(c1, "", Some(0), None).await));
    assert!(is_invalid(
        r.diff_page(Some(c1), c2, "", None, Some(0), None).await
    ));

    let files = r.list_files_page(c1, "", Some(1), None).await.unwrap();
    let fnext = files.next.unwrap();
    let diff = r
        .diff_page(Some(c1), c2, "", None, Some(1), None)
        .await
        .unwrap();
    let dnext = diff.next.unwrap();
    // Bound to the endpoint and the query.
    assert!(is_invalid(
        r.list_files_page(c1, "a", Some(1), Some(&fnext)).await
    ));
    assert!(is_invalid(
        r.list_files_page(c2, "", Some(1), Some(&fnext)).await
    ));
    assert!(is_invalid(
        r.list_files_page(c1, "", Some(1), Some(&dnext)).await
    ));
    assert!(is_invalid(
        r.diff_page(Some(c1), c2, "", None, Some(1), Some(&fnext))
            .await
    ));
    let opts = RenameOptions::default();
    assert!(is_invalid(
        r.diff_page(Some(c1), c2, "", Some(&opts), Some(1), Some(&dnext))
            .await
    ));
    assert!(is_invalid(
        r.diff_page(None, c2, "", None, Some(1), Some(&dnext)).await
    ));
    for bad in ["", "x", "!!!", &"A".repeat(5000)] {
        assert!(is_invalid(r.list_files_page(c1, "", None, Some(bad)).await));
    }
    // `limit` may change between pages, or go away.
    let rest = r.list_files_page(c1, "", None, Some(&fnext)).await.unwrap();
    assert_eq!(rest.files.len(), 1);
    assert_eq!(rest.next, None);
    // A slash-trimmed prefix is the same query.
    let fp = Fingerprint::of(CursorKind::Files, &(c1, "a"));
    let past = cursor::encode(CursorKind::Files, fp, &PathPos { path: "zz".into() }).unwrap();
    let page = r
        .list_files_page(c1, "/a/", Some(5), Some(&past))
        .await
        .unwrap();
    assert!(page.files.is_empty() && page.next.is_none());

    // A path too long for a cursor is refused rather than ending early.
    let long = format!("d/{}", "n".repeat(4000));
    let c3 = commit(
        &r,
        &[],
        &[(long.as_str(), Some("1")), ("e", Some("2"))],
        "c3",
    )
    .await;
    assert!(is_invalid(r.list_files_page(c3, "", Some(1), None).await));
    assert_eq!(
        r.list_files_page(c3, "", None, None)
            .await
            .unwrap()
            .files
            .len(),
        2
    );
}
