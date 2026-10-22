use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use leep_core::api::SearchResponse;
use leep_core::memory::{MemoryBlobStore, MemoryMetaStore};
use leep_core::{Changeset, ChangesetId, Error, FileChange, FileKind, Hash, RepoPath, TreeEntry};
use regex::RegexBuilder;

use super::index::{BLOB_KIND, TREE_KIND};
use super::trigram::{BlobRow, Bloom, MAX_INDEXED_BYTES};
use super::{SearchQuery, MAX_LIMIT};

use crate::Repo;

fn repo() -> Repo {
    Repo::new(
        Arc::new(MemoryBlobStore::new()),
        Arc::new(MemoryMetaStore::new()),
    )
}

type File<'a> = (&'a str, Option<&'a [u8]>);

async fn commit_kind(
    r: &Repo,
    parents: &[ChangesetId],
    files: &[File<'_>],
    kind: FileKind,
) -> ChangesetId {
    let mut changes = BTreeMap::new();
    for (p, c) in files {
        let change = match c {
            Some(data) => FileChange::Modify {
                content: r.blobs().put(Bytes::copy_from_slice(data)).await.unwrap(),
                kind,
                size: data.len() as u64,
            },
            None => FileChange::Delete,
        };
        changes.insert(RepoPath::new(*p).unwrap(), change);
    }
    r.create_changeset(Changeset {
        parents: parents.to_vec(),
        author: "t".into(),
        timestamp: 1,
        message: format!("c{}", parents.len()),
        changes,
        extra: BTreeMap::new(),
    })
    .await
    .unwrap()
}

async fn commit(r: &Repo, parents: &[ChangesetId], files: &[File<'_>]) -> ChangesetId {
    commit_kind(r, parents, files, FileKind::Regular).await
}

fn hits(r: &SearchResponse) -> Vec<(String, u64)> {
    r.matches
        .iter()
        .map(|m| (m.path.to_string(), m.line_number))
        .collect()
}

#[tokio::test]
async fn finds_lines_with_ranges_in_path_order() {
    let r = repo();
    let c = commit(
        &r,
        &[],
        &[
            ("a/b.txt", Some(b"nothing\nthe needle here\n")),
            ("a.txt", Some(b"needle needle\r\nx")),
            ("a-b", Some(b"last\nneedle")),
            ("z.bin", Some(b"needle\0binary")),
        ],
    )
    .await;
    let res = r.search(c, &SearchQuery::new("needle")).await.unwrap();
    assert_eq!(
        hits(&res),
        [
            ("a-b".into(), 2),
            ("a.txt".into(), 1),
            ("a/b.txt".into(), 2)
        ]
    );
    let ranges: Vec<(u32, u32)> = res.matches[1]
        .ranges
        .iter()
        .map(|r| (r.start, r.end))
        .collect();
    assert_eq!(ranges, [(0, 6), (7, 13)]);
    assert_eq!(res.matches[1].line, "needle needle");
    assert!(!res.truncated && res.stats.indexed);
    assert_eq!(res.stats.files_considered, 4);
    assert_eq!(res.stats.files_skipped, 1);
    assert_eq!(res.stats.files_scanned, 3);
}

#[tokio::test]
async fn case_insensitive_and_regex_queries() {
    let r = repo();
    let c = commit(
        &r,
        &[],
        &[
            ("a.rs", Some(b"fn Main() {}\nfn other() {}\n")),
            ("b.rs", Some(b"MAIN loop\nlet main = 1;\n")),
        ],
    )
    .await;
    let q = SearchQuery::new("main");
    assert_eq!(hits(&r.search(c, &q).await.unwrap()), [("b.rs".into(), 2)]);
    let q = SearchQuery {
        case_insensitive: true,
        ..SearchQuery::new("main")
    };
    assert_eq!(
        hits(&r.search(c, &q).await.unwrap()),
        [("a.rs".into(), 1), ("b.rs".into(), 1), ("b.rs".into(), 2)]
    );
    let q = SearchQuery {
        regex: true,
        ..SearchQuery::new(r"fn\s+(Main|other)\(")
    };
    assert_eq!(
        hits(&r.search(c, &q).await.unwrap()),
        [("a.rs".into(), 1), ("a.rs".into(), 2)]
    );
    // No required trigram: scanned without the index, same answer.
    let q = SearchQuery {
        regex: true,
        ..SearchQuery::new(r"\bfn\b")
    };
    let res = r.search(c, &q).await.unwrap();
    assert!(!res.stats.indexed);
    assert_eq!(res.matches.len(), 2);
    assert_eq!(res.stats.blob_rows_derived, 0);
    assert_eq!(res.stats.tree_rows_derived, 0);
}

#[tokio::test]
async fn invalid_queries_are_invalid_errors() {
    let r = repo();
    let c = commit(&r, &[], &[("a", Some(b"x"))]).await;
    for q in [
        SearchQuery::new(""),
        SearchQuery {
            regex: true,
            ..SearchQuery::new("(")
        },
        SearchQuery {
            glob: Some("[".into()),
            ..SearchQuery::new("x")
        },
    ] {
        assert!(matches!(r.search(c, &q).await, Err(Error::Invalid(_))));
    }
}

#[tokio::test]
async fn path_prefix_and_glob_restrict_the_search() {
    let r = repo();
    let c = commit(
        &r,
        &[],
        &[
            ("src/a.rs", Some(b"token")),
            ("src/deep/b.rs", Some(b"token")),
            ("src/c.md", Some(b"token")),
            ("docs/d.rs", Some(b"token")),
            ("e.rs", Some(b"token")),
        ],
    )
    .await;
    let paths = |res: SearchResponse| -> Vec<String> {
        res.matches.iter().map(|m| m.path.to_string()).collect()
    };
    let with = |path: &str, glob: Option<&str>| SearchQuery {
        path: path.into(),
        glob: glob.map(String::from),
        ..SearchQuery::new("token")
    };
    assert_eq!(
        paths(r.search(c, &with("src", None)).await.unwrap()),
        ["src/a.rs", "src/c.md", "src/deep/b.rs"]
    );
    assert_eq!(
        paths(r.search(c, &with("src/", None)).await.unwrap()),
        ["src/a.rs", "src/c.md", "src/deep/b.rs"]
    );
    assert_eq!(
        paths(r.search(c, &with("e.rs", None)).await.unwrap()),
        ["e.rs"]
    );
    assert!(paths(r.search(c, &with("sr", None)).await.unwrap()).is_empty());
    assert!(paths(r.search(c, &with("nope/x", None)).await.unwrap()).is_empty());
    assert_eq!(
        paths(r.search(c, &with("", Some("**/*.rs"))).await.unwrap()),
        ["docs/d.rs", "e.rs", "src/a.rs", "src/deep/b.rs"]
    );
    // `*` stays within one level.
    assert_eq!(
        paths(r.search(c, &with("", Some("*.rs"))).await.unwrap()),
        ["e.rs"]
    );
    assert_eq!(
        paths(r.search(c, &with("", Some("src/*.rs"))).await.unwrap()),
        ["src/a.rs"]
    );
    assert_eq!(
        paths(r.search(c, &with("src", Some("**/b.rs"))).await.unwrap()),
        ["src/deep/b.rs"]
    );
    // A glob with a literal directory prefix never reads other directories.
    let res = r.search(c, &with("", Some("docs/*.rs"))).await.unwrap();
    assert_eq!(paths(res.clone()), ["docs/d.rs"]);
    assert_eq!(res.stats.files_considered, 1);
}

#[tokio::test]
async fn skips_symlinks_binary_and_oversize_files() {
    let r = repo();
    let big = format!("needle{}", "x".repeat(MAX_INDEXED_BYTES as usize));
    let c = commit(
        &r,
        &[],
        &[
            ("big.txt", Some(big.as_bytes())),
            ("bin", Some(b"needle\0")),
            ("latin1", Some(b"needle \xe9")),
            ("ok.txt", Some(b"needle")),
        ],
    )
    .await;
    let c = commit_kind(&r, &[c], &[("link", Some(b"needle"))], FileKind::Symlink).await;
    for use_index in [true, false] {
        let q = SearchQuery {
            use_index,
            ..SearchQuery::new("needle")
        };
        let res = r.search(c, &q).await.unwrap();
        assert_eq!(hits(&res), [("ok.txt".into(), 1)], "index={use_index}");
        assert_eq!(res.stats.files_skipped, 4, "index={use_index}");
        assert_eq!(res.stats.files_considered, 5);
    }
    // The oversize blob is recorded without being read.
    let row = r
        .meta()
        .get_derived(BLOB_KIND, &Hash::of_blob(big.as_bytes()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        BlobRow::decode(&row).unwrap(),
        BlobRow::NotIndexed(super::trigram::Skip::TooLarge)
    );
}

#[tokio::test]
async fn prunes_directories_and_files_by_trigram() {
    let r = repo();
    let c = commit(
        &r,
        &[],
        &[
            ("src/a.rs", Some(b"let unique_alpha_token = 1;\n")),
            ("src/b.rs", Some(b"let other = 2;\n")),
            ("docs/c.md", Some(b"prose about nothing\n")),
            ("lib/d.rs", Some(b"pub fn lib() {}\n")),
            ("split/x", Some(b"abcxyz\n")),
            ("split/y", Some(b"bcdwvu\n")),
        ],
    )
    .await;
    let res = r
        .search(c, &SearchQuery::new("unique_alpha_token"))
        .await
        .unwrap();
    assert_eq!(hits(&res), [("src/a.rs".into(), 1)]);
    assert_eq!(res.stats.dirs_pruned, 3, "docs, lib and split");
    assert_eq!(res.stats.files_considered, 2);
    assert_eq!(res.stats.files_pruned, 1);
    assert_eq!(res.stats.files_scanned, 1);
    assert_eq!(res.stats.bytes_scanned, 28);

    // Both trigrams exist in `split`, in different files: the directory
    // survives, the files do not.
    let res = r.search(c, &SearchQuery::new("abcd")).await.unwrap();
    assert!(res.matches.is_empty());
    assert_eq!(res.stats.dirs_pruned, 3);
    assert_eq!(res.stats.files_considered, 2);
    assert_eq!(res.stats.files_pruned, 2);
    assert_eq!(res.stats.files_scanned, 0);
    assert_eq!(res.stats.bytes_scanned, 0);
}

#[tokio::test]
async fn derivation_is_lazy_idempotent_and_shared_across_revisions() {
    let r = repo();
    let c1 = commit(
        &r,
        &[],
        &[
            ("a/x/1.txt", Some(b"one one one")),
            ("a/x/2.txt", Some(b"two two two")),
            ("b/3.txt", Some(b"three three")),
        ],
    )
    .await;
    let first = r.search(c1, &SearchQuery::new("three")).await.unwrap();
    assert_eq!(first.stats.blob_rows_derived, 3);
    assert_eq!(first.stats.tree_rows_derived, 4, "root, a, a/x, b");
    let again = r.search(c1, &SearchQuery::new("three")).await.unwrap();
    assert_eq!(
        (again.stats.blob_rows_derived, again.stats.tree_rows_derived),
        (0, 0)
    );
    assert_eq!(first.matches, again.matches);
    let report = r.derive_search(c1).await.unwrap();
    assert_eq!((report.blob_rows, report.tree_rows), (0, 0));

    // Changing one file derives one blob row and the rows on its path only.
    let c2 = commit(&r, &[c1], &[("a/x/1.txt", Some(b"uno uno uno"))]).await;
    let report = r.derive_search(c2).await.unwrap();
    assert_eq!((report.blob_rows, report.tree_rows), (1, 3), "root, a, a/x");
    assert_eq!(r.derive_search(c2).await.unwrap().tree_rows, 0);

    // Rows are a pure function of content: rebuilding them elsewhere gives the
    // same bytes.
    let other = repo();
    let c1b = commit(
        &other,
        &[],
        &[
            ("a/x/1.txt", Some(b"one one one")),
            ("a/x/2.txt", Some(b"two two two")),
            ("b/3.txt", Some(b"three three")),
        ],
    )
    .await;
    other.derive_search(c1b).await.unwrap();
    assert_eq!(c1, c1b);
    let root = r.root_tree(c1).await.unwrap();
    let get = |repo: &Repo, kind: &'static str, key: Hash| {
        let m = repo.meta().clone();
        async move { m.get_derived(kind, &key).await.unwrap().unwrap() }
    };
    assert_eq!(
        get(&r, TREE_KIND, root).await,
        get(&other, TREE_KIND, root).await
    );
    let blob = Hash::of_blob(b"one one one");
    assert_eq!(
        get(&r, BLOB_KIND, blob).await,
        get(&other, BLOB_KIND, blob).await
    );
}

#[tokio::test]
async fn directory_summary_covers_every_file_below() {
    let r = repo();
    let c = commit(
        &r,
        &[],
        &[
            ("a/b/c/deep.txt", Some(b"deepest content")),
            ("a/top.txt", Some(b"upper content")),
            ("link", Some(b"never indexed")),
        ],
    )
    .await;
    r.derive_search(c).await.unwrap();
    let root = r.root_tree(c).await.unwrap();
    let row = r
        .meta()
        .get_derived(TREE_KIND, &root)
        .await
        .unwrap()
        .unwrap();
    let bloom = Bloom::decode(&row).unwrap();
    for t in super::trigram::extract(b"deepest content")
        .into_iter()
        .chain(super::trigram::extract(b"upper content"))
    {
        assert!(bloom.contains(t));
    }
    // Small directories use the compact encoding.
    assert_eq!(row[0], 2);
    assert!(row.len() < 400, "{} bytes", row.len());
    // An empty tree has an empty summary.
    let empty = commit(
        &r,
        &[c],
        &[
            ("a/b/c/deep.txt", None),
            ("a/top.txt", None),
            ("link", None),
        ],
    )
    .await;
    let root = r.root_tree(empty).await.unwrap();
    r.derive_search(empty).await.unwrap();
    assert_eq!(
        r.meta()
            .get_derived(TREE_KIND, &root)
            .await
            .unwrap()
            .unwrap()
            .as_ref(),
        [2]
    );
    let res = r.search(empty, &SearchQuery::new("content")).await.unwrap();
    assert!(res.matches.is_empty() && !res.truncated);
}

#[tokio::test]
async fn symlinks_do_not_enter_the_summary() {
    let r = repo();
    let c = commit_kind(
        &r,
        &[],
        &[("link", Some(b"zzyzx target"))],
        FileKind::Symlink,
    )
    .await;
    let c = commit(&r, &[c], &[("f", Some(b"plain"))]).await;
    r.derive_search(c).await.unwrap();
    let root = r.root_tree(c).await.unwrap();
    let row = r
        .meta()
        .get_derived(TREE_KIND, &root)
        .await
        .unwrap()
        .unwrap();
    let bloom = Bloom::decode(&row).unwrap();
    assert!(!bloom.contains(super::trigram::code(b'z', b'z', b'y')));
    assert!(bloom.contains(super::trigram::code(b'p', b'l', b'a')));
}

#[tokio::test]
async fn limits_truncate_deterministically() {
    let r = repo();
    let body = "hit\nmiss\n".repeat(20);
    let files: Vec<(String, Vec<u8>)> = (0..10)
        .map(|i| (format!("f{i}.txt"), body.clone().into_bytes()))
        .collect();
    let refs: Vec<File> = files
        .iter()
        .map(|(p, b)| (p.as_str(), Some(b.as_slice())))
        .collect();
    let c = commit(&r, &[], &refs).await;
    let q = |limit| SearchQuery {
        limit,
        ..SearchQuery::new("hit")
    };
    let res = r.search(c, &q(25)).await.unwrap();
    assert_eq!(res.matches.len(), 25);
    assert!(res.truncated);
    assert_eq!(res.matches[24].path.as_str(), "f1.txt");
    assert_eq!(res.matches[24].line_number, 9, "5th hit of f1");
    // Exactly as many matches as the limit is not truncation.
    let res = r.search(c, &q(200)).await.unwrap();
    assert_eq!((res.matches.len(), res.truncated), (200, false));
    let res = r.search(c, &q(199)).await.unwrap();
    assert_eq!((res.matches.len(), res.truncated), (199, true));
    // Limits are clamped, never zero.
    assert_eq!(r.search(c, &q(0)).await.unwrap().matches.len(), 1);
    assert_eq!(
        r.search(c, &q(usize::MAX)).await.unwrap().matches.len(),
        200
    );

    // The scan budget stops the walk at a file boundary.
    let budget = SearchQuery {
        max_scan_bytes: 3 * body.len() as u64 + 1,
        ..SearchQuery::new("hit")
    };
    let res = r.search(c, &budget).await.unwrap();
    assert!(res.truncated);
    assert_eq!(res.stats.files_scanned, 3);
    assert_eq!(res.matches.len(), 60);
    assert_eq!(res.matches[59].path.as_str(), "f2.txt");
}

#[tokio::test]
async fn stops_walking_once_the_limit_is_reached() {
    let r = repo();
    let names: Vec<String> = (0..300).map(|i| format!("d{:03}/f.txt", i)).collect();
    let refs: Vec<File> = names
        .iter()
        .map(|p| (p.as_str(), Some(&b"hit\n"[..])))
        .collect();
    let c = commit(&r, &[], &refs).await;
    for use_index in [true, false] {
        let q = SearchQuery {
            limit: 1,
            use_index,
            ..SearchQuery::new("hit")
        };
        let res = r.search(c, &q).await.unwrap();
        assert_eq!(hits(&res), [("d000/f.txt".into(), 1)]);
        assert!(res.truncated);
        assert!(
            res.stats.files_considered < 100,
            "walked {} files for one match",
            res.stats.files_considered
        );
    }
}

#[tokio::test]
async fn a_path_prefix_limits_what_the_first_search_derives() {
    let r = repo();
    let c = commit(
        &r,
        &[],
        &[
            ("a/x/1", Some(b"alpha one")),
            ("a/y/2", Some(b"alpha two")),
            ("b/z/3", Some(b"beta three")),
        ],
    )
    .await;
    let q = SearchQuery {
        path: "a".into(),
        ..SearchQuery::new("alpha")
    };
    let res = r.search(c, &q).await.unwrap();
    assert_eq!(res.matches.len(), 2);
    assert_eq!(res.stats.blob_rows_derived, 2);
    assert_eq!(
        res.stats.tree_rows_derived, 3,
        "a, a/x, a/y; nothing under b"
    );
    // The whole-tree search later reuses those rows and derives the rest.
    let res = r.search(c, &SearchQuery::new("beta")).await.unwrap();
    assert_eq!(res.matches.len(), 1);
    assert_eq!(res.stats.blob_rows_derived, 1);
    assert_eq!(res.stats.tree_rows_derived, 3, "b/z, b and the root");
}

#[tokio::test]
async fn deep_trees_do_not_recurse_on_the_stack() {
    let r = repo();
    let dir = vec!["d"; 600].join("/");
    let path = format!("{dir}/leaf.txt");
    let c = commit(
        &r,
        &[],
        &[
            (path.as_str(), Some(b"bottom needle")),
            ("top", Some(b"needle")),
        ],
    )
    .await;
    let res = r.search(c, &SearchQuery::new("needle")).await.unwrap();
    assert_eq!(hits(&res), [(path, 1), ("top".into(), 1)]);
    assert_eq!(res.stats.tree_rows_derived, 601);
}

#[tokio::test]
async fn unknown_path_through_a_file_is_empty() {
    let r = repo();
    let c = commit(&r, &[], &[("f", Some(b"x"))]).await;
    let q = SearchQuery {
        path: "f/g".into(),
        ..SearchQuery::new("x")
    };
    assert!(r.search(c, &q).await.unwrap().matches.is_empty());
    let root = r.root_tree(c).await.unwrap();
    assert!(matches!(
        r.read_tree(root).await.unwrap().entries["f"],
        TreeEntry::File { .. }
    ));
}

// ---- property test: the index never changes the answer ----

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }

    fn pick<'a, T>(&mut self, s: &'a [T]) -> &'a T {
        &s[self.below(s.len())]
    }
}

const WORDS: &[&str] = &[
    "alpha",
    "beta",
    "gamma",
    "delta",
    "fn main",
    "let x = 1;",
    "TODO",
    "Error",
    "error",
    "foo_bar",
    "fooBar",
    "h\u{e9}llo",
    "\u{65e5}\u{672c}\u{8a9e}",
    "a.b",
    "x{2}",
    "([0-9]+)",
    "return",
    "use std::io;",
    "KELVIN",
    "ab",
    "z",
];

fn random_file(rng: &mut Rng) -> Vec<u8> {
    match rng.below(12) {
        0 => return b"bin\0ary alpha".to_vec(),
        1 => return b"latin \xe9 alpha beta".to_vec(),
        _ => {}
    }
    let mut out = String::new();
    for _ in 0..rng.below(25) {
        for w in 0..1 + rng.below(5) {
            if w > 0 {
                out.push(' ');
            }
            out.push_str(rng.pick(WORDS));
        }
        out.push_str(if rng.below(5) == 0 { "\r\n" } else { "\n" });
    }
    if rng.below(3) == 0 {
        out.push_str("no newline at end");
    }
    out.into_bytes()
}

fn random_query(rng: &mut Rng, texts: &[String]) -> SearchQuery {
    let esc = |w: &str| regex::escape(w);
    let (a, b) = (*rng.pick(WORDS), *rng.pick(WORDS));
    let mut q = if rng.below(2) == 0 {
        // A literal: a word, a slice of real content, or something absent.
        let lit = match rng.below(3) {
            0 => a.to_string(),
            1 if !texts.is_empty() => {
                let t = rng.pick(texts);
                let line = t
                    .lines()
                    .nth(rng.below(t.lines().count().max(1)))
                    .unwrap_or("");
                let chars: Vec<char> = line.chars().collect();
                let start = rng.below(chars.len().max(1));
                let len = 1 + rng.below(8);
                let s: String = chars.iter().skip(start).take(len).collect();
                if s.is_empty() {
                    a.to_string()
                } else {
                    s
                }
            }
            _ => "zzqx".to_string(),
        };
        SearchQuery::new(lit)
    } else {
        let p = match rng.below(15) {
            0 => format!("{}|{}", esc(a), esc(b)),
            1 => format!("{}.*{}", esc(a), esc(b)),
            2 => format!(r"{}\s+{}", esc(a), esc(b)),
            3 => format!("^{}", esc(a)),
            4 => format!("{}$", esc(a)),
            5 => format!("[a-c]{}", esc(a)),
            6 => format!("({}|{})+", esc(a), esc(b)),
            7 => format!("(?i){}", esc(a)),
            8 => format!(r"\b{}\b", esc(a)),
            9 => format!("{}?{}", esc(a), esc(b)),
            10 => ".*".to_string(),
            11 => r"\w+".to_string(),
            12 => "[a-z]+_[a-z]+".to_string(),
            13 => format!("(?i)({}|{}){{2}}", esc(a), esc(b)),
            _ => format!("{}[^x]{}", esc(a), esc(b)),
        };
        SearchQuery {
            regex: true,
            ..SearchQuery::new(p)
        }
    };
    q.case_insensitive = rng.below(3) == 0;
    q.path = ["", "", "src", "lib/x", "docs"][rng.below(5)].to_string();
    q.glob = [
        None,
        None,
        Some("*.rs"),
        Some("**/*.rs"),
        Some("src/**"),
        Some("docs/*"),
    ][rng.below(6)]
    .map(String::from);
    q.limit = *rng.pick(&[1, 3, 1000]);
    q
}

/// Reference answer by brute force over `list_files`, sharing no code with
/// the planner, the index or the walk.
async fn oracle(r: &Repo, id: ChangesetId, q: &SearchQuery) -> (Vec<String>, bool) {
    let src = if q.regex {
        q.pattern.clone()
    } else {
        regex::escape(&q.pattern)
    };
    let re = RegexBuilder::new(&src)
        .case_insensitive(q.case_insensitive)
        .build()
        .unwrap();
    let glob = q.glob.as_ref().map(|g| {
        globset::GlobBuilder::new(g)
            .literal_separator(true)
            .build()
            .unwrap()
            .compile_matcher()
    });
    let mut out = Vec::new();
    for f in r.list_files(id, &q.path).await.unwrap() {
        if f.kind == FileKind::Symlink
            || glob.as_ref().is_some_and(|g| !g.is_match(f.path.as_str()))
        {
            continue;
        }
        let data = r.blobs().get(&f.content).await.unwrap().unwrap();
        if data.len() as u64 > MAX_INDEXED_BYTES || data.contains(&0) {
            continue;
        }
        let Ok(text) = std::str::from_utf8(&data) else {
            continue;
        };
        for (i, line) in text.lines().enumerate() {
            if re.is_match(line) {
                let ranges: Vec<(usize, usize)> = re
                    .find_iter(line)
                    .filter(|m| !m.is_empty())
                    .map(|m| (m.start(), m.end()))
                    .collect();
                out.push(format!("{}:{}:{line}:{ranges:?}", f.path, i + 1));
            }
        }
    }
    let limit = q.limit.clamp(1, MAX_LIMIT);
    let truncated = out.len() > limit;
    out.truncate(limit);
    (out, truncated)
}

fn render(r: &SearchResponse) -> Vec<String> {
    r.matches
        .iter()
        .map(|m| {
            let ranges: Vec<(usize, usize)> = m
                .ranges
                .iter()
                .map(|x| (x.start as usize, x.end as usize))
                .collect();
            format!("{}:{}:{}:{ranges:?}", m.path, m.line_number, m.line)
        })
        .collect()
}

#[tokio::test]
async fn indexed_search_equals_a_naive_scan_on_random_trees() {
    let (mut pruned, mut searches, mut found) = (0, 0, 0);
    for seed in 1..=60u64 {
        let mut rng = Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
        let r = repo();
        let mut parent: Vec<ChangesetId> = Vec::new();
        let mut live: BTreeMap<String, Vec<u8>> = BTreeMap::new();
        for _ in 0..1 + rng.below(3) {
            let mut files: BTreeMap<String, Option<Vec<u8>>> = BTreeMap::new();
            for _ in 0..3 + rng.below(14) {
                // `f1/` and `f1.x/` sit beside files `f1`, `f1.rs` and `f10`, where
                // string order and directory order could disagree.
                let dir = [
                    "",
                    "src/",
                    "src/deep/",
                    "lib/x/",
                    "docs/",
                    "a/b/c/",
                    "f1/",
                    "f1.x/",
                    "f1-/",
                ][rng.below(9)];
                let ext = [".rs", ".md", ".txt", ""][rng.below(4)];
                let path = format!("{dir}f{}{ext}", rng.below(12));
                if files.contains_key(&path) {
                    continue;
                }
                // A prefix directory and a file of the same name cannot coexist.
                let clash = live.keys().chain(files.keys()).any(|p| {
                    p.starts_with(&format!("{path}/")) || path.starts_with(&format!("{p}/"))
                });
                if clash {
                    continue;
                }
                if live.contains_key(&path) && rng.below(3) == 0 {
                    files.insert(path, None);
                } else {
                    files.insert(path, Some(random_file(&mut rng)));
                }
            }
            for (p, c) in &files {
                match c {
                    Some(c) => live.insert(p.clone(), c.clone()),
                    None => live.remove(p),
                };
            }
            let refs: Vec<File> = files
                .iter()
                .map(|(p, c)| (p.as_str(), c.as_deref()))
                .collect();
            parent = vec![commit(&r, &parent, &refs).await];
        }
        let id = parent[0];
        let texts: Vec<String> = live
            .values()
            .filter_map(|b| String::from_utf8(b.clone()).ok())
            .collect();
        for _ in 0..25 {
            let q = random_query(&mut rng, &texts);
            let want = oracle(&r, id, &q).await;
            let got = r.search(id, &q).await.unwrap();
            assert_eq!(
                (render(&got), got.truncated),
                want,
                "seed {seed} indexed query {q:?}"
            );
            let scan = r
                .search(
                    id,
                    &SearchQuery {
                        use_index: false,
                        ..q.clone()
                    },
                )
                .await
                .unwrap();
            assert_eq!(
                (render(&scan), scan.truncated),
                want,
                "seed {seed} scan query {q:?}"
            );
            searches += 1;
            pruned += got.stats.dirs_pruned + got.stats.files_pruned;
            found += got.matches.len();
        }
    }
    assert!(
        searches == 1500 && pruned > 100,
        "pruning was exercised: {pruned}"
    );
    assert!(found > 1000, "queries found something: {found}");
}

/// Directories are opened ahead of the walk on their own tasks, a few at a
/// time. With more of them than run at once, on several levels, answers and
/// their order stay those of a brute-force scan, and a complete search counts
/// every pruned directory once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn opening_directories_ahead_keeps_the_answer() {
    let r = repo();
    let mut files: Vec<(String, Vec<u8>)> = vec![
        ("a.txt".into(), b"needle at the top\n".to_vec()),
        ("d03/z.txt".into(), b"needle in the middle\n".to_vec()),
    ];
    for a in 0..12 {
        for b in 0..12 {
            for f in 0..3 {
                let body = if (a * 12 + b + f) % 5 == 0 {
                    format!("needle {a} {b} {f}\nhay\nneedle again\n")
                } else {
                    format!("hay {a} {b} {f}\n")
                };
                files.push((format!("d{a:02}/e{b:02}/f{f}.txt"), body.into_bytes()));
            }
        }
    }
    let refs: Vec<File> = files
        .iter()
        .map(|(p, b)| (p.as_str(), Some(b.as_slice())))
        .collect();
    let c = commit(&r, &[], &refs).await;
    for pattern in ["needle", "needle again", "nothing here", "hay 7 1"] {
        for limit in [1, 2, 7, 50, 1000] {
            let q = SearchQuery {
                limit,
                ..SearchQuery::new(pattern)
            };
            let want = oracle(&r, c, &q).await;
            let got = r.search(c, &q).await.unwrap();
            assert_eq!(
                (render(&got), got.truncated),
                want,
                "{pattern:?} limit {limit}"
            );
        }
    }
    let q = SearchQuery {
        limit: MAX_LIMIT,
        ..SearchQuery::new("nothing here")
    };
    let res = r.search(c, &q).await.unwrap();
    let scan = r
        .search(
            c,
            &SearchQuery {
                use_index: false,
                ..q.clone()
            },
        )
        .await
        .unwrap();
    assert!(res.matches.is_empty() && !res.truncated);
    assert_eq!(scan.stats.files_considered, files.len() as u64);
    // Every leaf directory is ruled out by its filter, or the whole tree is.
    assert!(res.stats.dirs_pruned >= 1 && res.stats.dirs_pruned <= 1 + 12 + 144);
    // A complete search's counts do not depend on how the reads interleaved.
    for _ in 0..3 {
        assert_eq!(r.search(c, &q).await.unwrap().stats, res.stats);
    }
}

/// A tree of `dirs` directories `d{i}/s`, each file with its own words, so
/// every directory has a filter of a few kilobytes.
async fn wide_tree(r: &Repo, dirs: usize) -> ChangesetId {
    let files: Vec<(String, Vec<u8>)> = (0..dirs)
        .map(|d| {
            let words: String = (0..60)
                .map(|w| format!("w{}x{} ", d * 977 + w * 31, w))
                .collect();
            (
                format!("d{d}/s/f.txt"),
                format!("only_in_{d}_here {words}\n").into_bytes(),
            )
        })
        .collect();
    let refs: Vec<File> = files
        .iter()
        .map(|(p, c)| (p.as_str(), Some(c.as_slice())))
        .collect();
    commit(r, &[], &refs).await
}

#[tokio::test]
async fn repeated_searches_read_directory_filters_from_the_cache() {
    let r = repo();
    let c = wide_tree(&r, 20).await;
    r.derive_search(c).await.unwrap();
    assert!(
        r.search_trees.entries().is_empty(),
        "derivation does not fill it"
    );
    let q = SearchQuery::new("only_in_7_here");
    let first = r.search(c, &q).await.unwrap();
    assert_eq!(hits(&first), [("d7/s/f.txt".into(), 1)]);
    assert_eq!(first.stats.dirs_pruned, 19);

    // Root, the 20 `d*` filters (19 of them pruned) and `d7/s`, each the
    // stored row, counted at its length plus the entry's overhead.
    let cached = r.search_trees.entries();
    assert_eq!(cached.len(), 22);
    let mut total = 0;
    for (h, row, size) in &cached {
        let stored = r.meta().get_derived(TREE_KIND, h).await.unwrap().unwrap();
        assert_eq!(**row, stored);
        assert_eq!(*size, stored.len() + super::index::CACHE_ENTRY_OVERHEAD);
        total += size;
    }
    assert_eq!(r.search_trees.held_bytes(), total);

    // Replace every stored filter with one that admits nothing: a handle
    // with its own caches now finds nothing, and this one, whose cache
    // serves the filters, answers as before without touching them.
    for (h, ..) in &cached {
        r.meta()
            .put_derived(TREE_KIND, h, Bytes::from_static(&[2]))
            .await
            .unwrap();
    }
    let again = r.search(c, &q).await.unwrap();
    assert_eq!(again.matches, first.matches);
    assert_eq!(again.stats, first.stats);
    let fresh = Repo::new(r.blobs().clone(), r.meta().clone());
    let res = fresh.search(c, &q).await.unwrap();
    assert!(res.matches.is_empty() && res.stats.dirs_pruned == 1);
}

#[tokio::test]
async fn the_directory_filter_cache_stays_within_its_budget() {
    let r = repo();
    let c = wide_tree(&r, 60).await;
    r.derive_search(c).await.unwrap();
    let small = Repo::new(r.blobs().clone(), r.meta().clone())
        .with_cache_budget(crate::DEFAULT_CACHE_BYTES >> 12);
    let budget = small.search_trees.budget();
    assert_eq!(budget, 16 << 10);
    let mut stored = 0;
    for (_, row) in r.meta().list_derived(TREE_KIND, None, 1000).await.unwrap() {
        stored += row.len();
    }
    assert!(stored > 4 * budget, "{stored} bytes of filters");
    for round in 0..3 {
        for d in [3, 59, 0, 31, 17, 44] {
            let q = SearchQuery::new(format!("only_in_{d}_here"));
            let got = small.search(c, &q).await.unwrap();
            assert_eq!(got.matches, r.search(c, &q).await.unwrap().matches);
            assert_eq!(hits(&got), [(format!("d{d}/s/f.txt"), 1)], "round {round}");
            let held = small.search_trees.held_bytes();
            assert!(held <= budget, "{held} > {budget}");
            assert!(!small.search_trees.entries().is_empty());
        }
    }
}

/// Counts the batched row reads in flight, and stalls each briefly so
/// concurrent ones overlap.
struct CountingReads {
    inner: Arc<dyn leep_core::MetaStore>,
    now: std::sync::atomic::AtomicUsize,
    most: std::sync::atomic::AtomicUsize,
}

#[async_trait::async_trait]
impl leep_core::MetaStore for CountingReads {
    async fn put_changeset(&self, cs: &Changeset) -> leep_core::Result<ChangesetId> {
        self.inner.put_changeset(cs).await
    }
    async fn get_changeset(&self, id: &ChangesetId) -> leep_core::Result<Option<Changeset>> {
        self.inner.get_changeset(id).await
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
        ctx: &leep_core::RefUpdateContext,
    ) -> leep_core::Result<bool> {
        self.inner.cas_ref_logged(name, expected, new, ctx).await
    }
    async fn find_lands(
        &self,
        name: &str,
        ids: &[leep_core::LandId],
    ) -> leep_core::Result<Vec<Option<leep_core::LandRecord>>> {
        self.inner.find_lands(name, ids).await
    }
    async fn ref_log(
        &self,
        name: Option<&str>,
        before_seq: Option<u64>,
        limit: usize,
    ) -> leep_core::Result<Vec<leep_core::RefLogEntry>> {
        self.inner.ref_log(name, before_seq, limit).await
    }
    async fn list_refs(&self) -> leep_core::Result<BTreeMap<String, ChangesetId>> {
        self.inner.list_refs().await
    }
    async fn get_derived(&self, kind: &str, key: &Hash) -> leep_core::Result<Option<Bytes>> {
        self.inner.get_derived(kind, key).await
    }
    async fn get_derived_many(
        &self,
        kind: &str,
        keys: &[Hash],
    ) -> leep_core::Result<Vec<Option<Bytes>>> {
        use std::sync::atomic::Ordering::SeqCst;
        let now = self.now.fetch_add(1, SeqCst) + 1;
        self.most.fetch_max(now, SeqCst);
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        let rows = self.inner.get_derived_many(kind, keys).await;
        self.now.fetch_sub(1, SeqCst);
        rows
    }
    async fn put_derived(&self, kind: &str, key: &Hash, value: Bytes) -> leep_core::Result<()> {
        self.inner.put_derived(kind, key, value).await
    }
    async fn put_derived_many(&self, rows: &[leep_core::DerivedRow]) -> leep_core::Result<()> {
        self.inner.put_derived_many(rows).await
    }
}

/// The most batched row reads that many concurrent searches had in flight
/// at once, over a wide tree, with `permits` search pool permits.
async fn most_rows_reads_at_once(permits: Option<usize>) -> usize {
    let store = Arc::new(CountingReads {
        inner: Arc::new(MemoryMetaStore::new()),
        now: Default::default(),
        most: Default::default(),
    });
    let r = Repo::new(Arc::new(MemoryBlobStore::new()), store.clone())
        .with_search_pool_permits(permits);
    let mut files: Vec<(String, Vec<u8>)> = Vec::new();
    for a in 0..8 {
        for b in 0..8 {
            files.push((format!("d{a}/e{b}/f.txt"), format!("hay {a} {b}\n").into()));
        }
    }
    let refs: Vec<File> = files
        .iter()
        .map(|(p, b)| (p.as_str(), Some(b.as_slice())))
        .collect();
    let c = commit(&r, &[], &refs).await;
    // Rows first, so the searches below only read them. Derived rather than
    // searched, so the directory filter cache starts empty and every search
    // reads its filters from the store.
    r.derive_search(c).await.unwrap();
    store.most.store(0, std::sync::atomic::Ordering::SeqCst);
    let searches: Vec<_> = (0..8)
        .map(|i| {
            let r = r.clone();
            tokio::spawn(async move {
                let q = SearchQuery {
                    limit: MAX_LIMIT,
                    ..SearchQuery::new(format!("hay {i}"))
                };
                r.search(c, &q).await.unwrap()
            })
        })
        .collect();
    let done = tokio::time::timeout(
        std::time::Duration::from_secs(60),
        futures_util::future::join_all(searches),
    )
    .await
    .expect("searches finish: the pool and ahead permits never wait on each other");
    for (i, s) in done.into_iter().enumerate() {
        assert_eq!(s.unwrap().matches.len(), 8, "search {i}");
    }
    store.most.load(std::sync::atomic::Ordering::SeqCst)
}

/// Searches together hold at most two metadata reads per search pool permit
/// (a directory open joins its filter and row reads), however many run and
/// however far ahead each opens directories; without permits they hold more.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn search_pool_permits_bound_the_reads_of_all_searches() {
    assert!(most_rows_reads_at_once(Some(1)).await <= 2);
    assert!(most_rows_reads_at_once(Some(2)).await <= 4);
    assert!(most_rows_reads_at_once(None).await > 4);
}

// ---- level-batched derivation: rows, closure, batches, round trips ----

/// A metadata store that counts calls, can add latency to each, and can
/// fail or hold `put_derived_many`: for the round-trip, fault and
/// single-flight tests of search derivation.
pub(crate) struct Probe {
    inner: Arc<dyn leep_core::MetaStore>,
    delay: std::time::Duration,
    /// Every call.
    pub(crate) calls: AtomicU64,
    /// `put_derived_many` calls.
    pub(crate) puts: AtomicU64,
    /// The most rows, and bytes, one `put_derived_many` carried.
    pub(crate) most_rows: AtomicUsize,
    pub(crate) most_bytes: AtomicUsize,
    /// How often each row was written by `put_derived_many`.
    written: Mutex<HashMap<(String, Hash), u64>>,
    /// Fail this `put_derived_many` call (1-based), after storing this many
    /// of its rows: after an error any prefix may be stored.
    pub(crate) fail: Mutex<Option<(u64, usize)>>,
    /// While set, `put_derived_many` waits for a permit of `gate`.
    pub(crate) hold: AtomicBool,
    pub(crate) gate: tokio::sync::Semaphore,
    /// Writes waiting at the gate.
    pub(crate) held: AtomicU64,
}

impl Probe {
    pub(crate) fn new(delay: std::time::Duration) -> Arc<Probe> {
        Arc::new(Probe {
            inner: Arc::new(MemoryMetaStore::new()),
            delay,
            calls: AtomicU64::new(0),
            puts: AtomicU64::new(0),
            most_rows: AtomicUsize::new(0),
            most_bytes: AtomicUsize::new(0),
            written: Mutex::default(),
            fail: Mutex::new(None),
            hold: AtomicBool::new(false),
            gate: tokio::sync::Semaphore::new(0),
            held: AtomicU64::new(0),
        })
    }

    pub(crate) fn calls(&self) -> u64 {
        self.calls.load(SeqCst)
    }

    /// Times `put_derived_many` wrote `key` of `kind`.
    pub(crate) fn writes_of(&self, kind: &str, key: Hash) -> u64 {
        let w = self.written.lock().unwrap();
        w.get(&(kind.to_string(), key)).copied().unwrap_or(0)
    }

    async fn call(&self) {
        self.calls.fetch_add(1, SeqCst);
        if !self.delay.is_zero() {
            tokio::time::sleep(self.delay).await;
        }
    }
}

#[async_trait::async_trait]
impl leep_core::MetaStore for Probe {
    async fn put_changeset(&self, cs: &Changeset) -> leep_core::Result<ChangesetId> {
        self.call().await;
        self.inner.put_changeset(cs).await
    }
    async fn get_changeset(&self, id: &ChangesetId) -> leep_core::Result<Option<Changeset>> {
        self.call().await;
        self.inner.get_changeset(id).await
    }
    async fn get_changesets(
        &self,
        ids: &[ChangesetId],
    ) -> leep_core::Result<Vec<Option<Changeset>>> {
        self.call().await;
        self.inner.get_changesets(ids).await
    }
    async fn list_changesets(
        &self,
        after: Option<ChangesetId>,
        limit: usize,
    ) -> leep_core::Result<Vec<ChangesetId>> {
        self.call().await;
        self.inner.list_changesets(after, limit).await
    }
    async fn get_ref(&self, name: &str) -> leep_core::Result<Option<ChangesetId>> {
        self.call().await;
        self.inner.get_ref(name).await
    }
    async fn cas_ref_logged(
        &self,
        name: &str,
        expected: Option<ChangesetId>,
        new: Option<ChangesetId>,
        ctx: &leep_core::RefUpdateContext,
    ) -> leep_core::Result<bool> {
        self.call().await;
        self.inner.cas_ref_logged(name, expected, new, ctx).await
    }
    async fn find_lands(
        &self,
        name: &str,
        ids: &[leep_core::LandId],
    ) -> leep_core::Result<Vec<Option<leep_core::LandRecord>>> {
        self.call().await;
        self.inner.find_lands(name, ids).await
    }
    async fn ref_log(
        &self,
        name: Option<&str>,
        before_seq: Option<u64>,
        limit: usize,
    ) -> leep_core::Result<Vec<leep_core::RefLogEntry>> {
        self.call().await;
        self.inner.ref_log(name, before_seq, limit).await
    }
    async fn list_refs(&self) -> leep_core::Result<BTreeMap<String, ChangesetId>> {
        self.call().await;
        self.inner.list_refs().await
    }
    async fn get_derived(&self, kind: &str, key: &Hash) -> leep_core::Result<Option<Bytes>> {
        self.call().await;
        self.inner.get_derived(kind, key).await
    }
    async fn get_derived_many(
        &self,
        kind: &str,
        keys: &[Hash],
    ) -> leep_core::Result<Vec<Option<Bytes>>> {
        self.call().await;
        self.inner.get_derived_many(kind, keys).await
    }
    async fn list_derived(
        &self,
        kind: &str,
        after: Option<Hash>,
        limit: usize,
    ) -> leep_core::Result<Vec<(Hash, Bytes)>> {
        self.inner.list_derived(kind, after, limit).await
    }
    async fn put_derived(&self, kind: &str, key: &Hash, value: Bytes) -> leep_core::Result<()> {
        self.call().await;
        self.inner.put_derived(kind, key, value).await
    }
    async fn put_derived_many(&self, rows: &[leep_core::DerivedRow]) -> leep_core::Result<()> {
        self.call().await;
        let n = self.puts.fetch_add(1, SeqCst) + 1;
        self.most_rows.fetch_max(rows.len(), SeqCst);
        let bytes = rows.iter().map(|r| r.value.len()).sum();
        self.most_bytes.fetch_max(bytes, SeqCst);
        if self.hold.load(SeqCst) {
            self.held.fetch_add(1, SeqCst);
            let permit = self.gate.acquire().await;
            self.held.fetch_sub(1, SeqCst);
            drop(permit);
        }
        let fail = *self.fail.lock().unwrap();
        let (rows, result) = match fail {
            Some((nth, prefix)) if nth == n => (
                &rows[..prefix.min(rows.len())],
                Err(Error::Storage("injected put_derived_many failure".into())),
            ),
            _ => (rows, Ok(())),
        };
        self.inner.put_derived_many(rows).await?;
        let mut w = self.written.lock().unwrap();
        for r in rows {
            *w.entry((r.kind.clone(), r.key)).or_default() += 1;
        }
        result
    }
}

/// A repo on a [`Probe`] with no added latency.
pub(crate) fn probed() -> (Repo, Arc<Probe>) {
    let probe = Probe::new(std::time::Duration::ZERO);
    (
        Repo::new(Arc::new(MemoryBlobStore::new()), probe.clone()),
        probe,
    )
}

/// Files by path, with content and kind.
pub(crate) type Files = BTreeMap<String, (Vec<u8>, FileKind)>;

pub(crate) async fn commit_files(r: &Repo, parents: &[ChangesetId], files: &Files) -> ChangesetId {
    let mut changes = BTreeMap::new();
    for (p, (data, kind)) in files {
        let content = r.blobs().put(Bytes::copy_from_slice(data)).await.unwrap();
        changes.insert(
            RepoPath::new(p.as_str()).unwrap(),
            FileChange::Modify {
                content,
                kind: *kind,
                size: data.len() as u64,
            },
        );
    }
    r.create_changeset(Changeset {
        parents: parents.to_vec(),
        author: "t".into(),
        timestamp: 1,
        message: format!("c{}", parents.len()),
        changes,
        extra: BTreeMap::new(),
    })
    .await
    .unwrap()
}

/// A random tree for the derivation tests: directories up to four deep,
/// one subtree repeated at depths 1, 2 and 4 (the same missing directory
/// under parents of different heights), files repeated across directories,
/// and symlinks, binary and oversize files.
pub(crate) fn level_tree(seed: u64, n: usize) -> Files {
    let mut rng = Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
    let mut out = Files::new();
    for i in 0..n {
        let mut path = String::new();
        for _ in 0..rng.below(5) {
            path.push_str(["a", "b", "c", "d"][rng.below(4)]);
            path.push('/');
        }
        path.push_str(&format!("f{i}"));
        let kind = if rng.below(15) == 0 {
            FileKind::Symlink
        } else {
            FileKind::Regular
        };
        out.insert(path, (random_file(&mut rng), kind));
    }
    let shared: Vec<Vec<u8>> = (0..1 + rng.below(4))
        .map(|_| random_file(&mut rng))
        .collect();
    for prefix in ["dup", "x/dup", "x/y/z/dup"] {
        for (i, body) in shared.iter().enumerate() {
            out.insert(format!("{prefix}/s{i}"), (body.clone(), FileKind::Regular));
        }
    }
    if rng.below(4) == 0 {
        let huge = vec![b'x'; MAX_INDEXED_BYTES as usize + 1];
        out.insert("big/huge".into(), (huge, FileKind::Regular));
    }
    out
}

/// A directory: its hash, its subdirectories, and its searchable files'
/// contents and sizes.
pub(crate) type DirShape = (Hash, Vec<Hash>, Vec<(Hash, u64)>);

/// Every distinct directory of the tree `root`.
pub(crate) async fn tree_shape(r: &Repo, root: Hash) -> Vec<DirShape> {
    let mut out = Vec::new();
    let mut seen = std::collections::HashSet::from([root]);
    let mut todo = vec![root];
    while let Some(h) = todo.pop() {
        let mut dirs = Vec::new();
        let mut files = Vec::new();
        for e in r.read_tree(h).await.unwrap().entries.into_values() {
            match e {
                TreeEntry::Dir { tree } => {
                    dirs.push(tree);
                    if seen.insert(tree) {
                        todo.push(tree);
                    }
                }
                TreeEntry::File {
                    content,
                    kind,
                    size,
                } if kind != FileKind::Symlink => files.push((content, size)),
                TreeEntry::File { .. } => {}
            }
        }
        out.push((h, dirs, files));
    }
    out
}

async fn stored(meta: &dyn leep_core::MetaStore, kind: &str, key: &Hash) -> Option<Bytes> {
    meta.get_derived(kind, key).await.unwrap()
}

/// Every stored filter below `root` has its whole subtree stored: its
/// subdirectories' filters and its files' rows. Returns the stored filters.
pub(crate) async fn assert_closed(r: &Repo, root: Hash, what: &str) -> usize {
    let meta = r.meta().as_ref();
    let mut n = 0;
    for (h, dirs, files) in tree_shape(r, root).await {
        if stored(meta, TREE_KIND, &h).await.is_none() {
            continue;
        }
        n += 1;
        for d in &dirs {
            assert!(
                stored(meta, TREE_KIND, d).await.is_some(),
                "{what}: filter of {h} stored without that of subdirectory {d}"
            );
        }
        for (f, _) in &files {
            assert!(
                stored(meta, BLOB_KIND, f).await.is_some(),
                "{what}: filter of {h} stored without the row of file {f}"
            );
        }
    }
    n
}

/// Every stored filter below `root` is what `recompute_tree_row` computes
/// with no rows to start from, and every stored blob row is that of the
/// blob's bytes.
pub(crate) async fn assert_rows_recomputed(r: &Repo, root: Hash, what: &str) {
    let fresh = Repo::new(r.blobs().clone(), Arc::new(MemoryMetaStore::new()));
    let mut memo = super::index::TreeRowMemo::new();
    let meta = r.meta().as_ref();
    for (h, _, files) in tree_shape(r, root).await {
        if let Some(row) = stored(meta, TREE_KIND, &h).await {
            let want = fresh.recompute_tree_row(h, &mut memo).await.unwrap();
            assert_eq!(row, want, "{what}: filter of {h}");
        }
        for (f, size) in files {
            if let Some(row) = stored(meta, BLOB_KIND, &f).await {
                let want = if size > MAX_INDEXED_BYTES {
                    BlobRow::NotIndexed(super::trigram::Skip::TooLarge)
                } else {
                    BlobRow::of(&r.blobs().get(&f).await.unwrap().unwrap())
                };
                assert_eq!(row, want.encode(), "{what}: row of blob {f}");
            }
        }
    }
}

/// Store the filters of some directories of `root` (each with its subtree,
/// as derivation does) and the rows of some files.
pub(crate) async fn index_some(r: &Repo, seed: u64, root: Hash) {
    let mut rng = Rng(seed ^ 0x05ee_d0f1_e7e1);
    let c = super::index::Counters::default();
    for (h, _, files) in tree_shape(r, root).await {
        if rng.below(4) == 0 {
            r.tree_row(h, &c).await.unwrap();
        }
        for (f, size) in files {
            if rng.below(3) == 0 {
                r.blob_row(f, size, &c).await.unwrap();
            }
        }
    }
}

/// Derived in level batches over random trees and random partial indexes
/// (an earlier revision's rows, some subtrees', some files'), every stored
/// row is byte for byte what `recompute_tree_row` gives, every stored filter
/// has its subtree stored, and deriving again computes nothing.
#[tokio::test]
async fn level_batches_store_the_rows_recompute_gives() {
    for seed in 1..=40u64 {
        let what = format!("seed {seed}");
        let (r, _) = probed();
        let mut rng = Rng(seed | 1);
        let files = level_tree(seed, 5 + rng.below(70));
        let c1 = commit_files(&r, &[], &files).await;
        if rng.below(2) == 0 {
            r.derive_search(c1).await.unwrap();
        }
        let paths: Vec<&String> = files.keys().collect();
        let mut edits = Files::new();
        for _ in 0..rng.below(8) {
            let p = paths[rng.below(paths.len())].clone();
            edits.insert(p, (random_file(&mut rng), FileKind::Regular));
        }
        let c2 = commit_files(&r, &[c1], &edits).await;
        let root = r.root_tree(c2).await.unwrap();
        if rng.below(3) != 0 {
            index_some(&r, seed, root).await;
        }
        assert_closed(&r, root, &what).await;
        r.derive_search(c2).await.unwrap();
        assert!(stored(r.meta().as_ref(), TREE_KIND, &root).await.is_some());
        let dirs = assert_closed(&r, root, &what).await;
        assert_eq!(dirs, tree_shape(&r, root).await.len(), "{what}");
        assert_rows_recomputed(&r, root, &what).await;
        let again = r.derive_search(c2).await.unwrap();
        assert_eq!((again.blob_rows, again.tree_rows), (0, 0), "{what}");
    }
}

/// A level wider than a batch is split, a directory of more files than a
/// batch is a batch of its own, and every write stays within the rows and
/// bytes a batch may carry, with the rows those of a single derivation.
#[tokio::test]
async fn wide_levels_split_into_bounded_batches() {
    let (r, probe) = probed();
    let mut files = Files::new();
    for i in 0..5_000 {
        files.insert(
            format!("flat/f{i}"),
            (format!("flat {i}\n").into(), FileKind::Regular),
        );
    }
    for d in 0..1_500 {
        for f in 0..2 {
            let body = format!("narrow {d} {f}\n").into_bytes();
            files.insert(format!("n/d{d}/f{f}"), (body, FileKind::Regular));
        }
    }
    // Random printable text: tens of thousands of distinct trigrams, so
    // each of these directories' filters is dense (64 KiB).
    let mut rng = Rng(77);
    for d in 0..300 {
        let body: Vec<u8> = (0..40_000).map(|_| b' ' + rng.below(95) as u8).collect();
        files.insert(format!("dense/d{d}/f"), (body, FileKind::Regular));
    }
    let c = commit_files(&r, &[], &files).await;
    let root = r.root_tree(c).await.unwrap();
    let report = r.derive_search(c).await.unwrap();
    assert_eq!(report.blob_rows, 5_000 + 3_000 + 300);
    assert_eq!(report.tree_rows, 1 + 1 + 1 + 1 + 1_500 + 300);
    let dense = r
        .meta()
        .get_derived(
            TREE_KIND,
            &tree_shape(&r, root)
                .await
                .into_iter()
                .find(|(_, dirs, files)| {
                    dirs.is_empty() && files.len() == 1 && files[0].1 == 40_000
                })
                .unwrap()
                .0,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        dense.len(),
        1 + (super::trigram::BLOOM_BITS / 8),
        "a dense filter"
    );
    assert!(probe.most_rows.load(SeqCst) <= super::index::WRITE_ROWS);
    let most = probe.most_bytes.load(SeqCst);
    assert!(
        most <= super::index::BATCH_BYTES + dense.len(),
        "{most} bytes in one write"
    );
    assert!(
        most > super::index::BATCH_BYTES / 2,
        "{most}: dense filters filled a write"
    );
    // Each row written once.
    for (h, _, files) in tree_shape(&r, root).await {
        assert_eq!(probe.writes_of(TREE_KIND, h), 1);
        for (f, _) in files {
            assert_eq!(probe.writes_of(BLOB_KIND, f), 1);
        }
    }
    assert_closed(&r, root, "wide").await;
    assert_rows_recomputed(&r, root, "wide").await;
}

/// The swarm's shape (agents' directories beside a larger tree), with a
/// land changing two files of one agent.
async fn swarm_land(r: &Repo, parent: ChangesetId, seq: usize) -> ChangesetId {
    let a = (seq * 7919) % 500;
    let mut files = Files::new();
    for f in 0..2 {
        let path = if seq.is_multiple_of(50) {
            format!("hot/h{}", seq % 8)
        } else {
            format!("agents/a{a}/f{f}")
        };
        let body = format!("swarm a{a} s{seq} file {f}\n").into_bytes();
        files.insert(path, (body, FileKind::Regular));
    }
    commit_files(r, &[parent], &files).await
}

/// Metadata round trips of deriving, and of searching, a head `gap` lands
/// past the indexed one, on the swarm's shape.
async fn round_trips_at(gap: usize) -> (u64, u64) {
    let (r, probe) = probed();
    let mut files = Files::new();
    for top in 0..10 {
        for sub in 0..5 {
            for f in 0..10 {
                let body = format!("fn t{top}_s{sub}_f{f}() {{}}\n").into_bytes();
                files.insert(format!("t{top}/s{sub}/f{f}.c"), (body, FileKind::Regular));
            }
        }
    }
    for i in 0..6 {
        files.insert(
            format!("ROOT{i}"),
            (format!("root {i}\n").into(), FileKind::Regular),
        );
    }
    for a in 0..500 {
        for f in 0..2 {
            let body = format!("swarm a{a} file {f}\n").into_bytes();
            files.insert(format!("agents/a{a}/f{f}"), (body, FileKind::Regular));
        }
    }
    let mut head = commit_files(&r, &[], &files).await;
    r.derive_search(head).await.unwrap();
    let mut seq = 0;
    let mut gap_head = |r: Repo, mut head: ChangesetId| {
        let start = seq;
        seq += gap;
        async move {
            for s in start + 1..=start + gap {
                head = swarm_land(&r, head, s).await;
            }
            r.root_tree(head).await.unwrap();
            head
        }
    };
    head = gap_head(r.clone(), head).await;
    let before = probe.calls();
    r.derive_search(head).await.unwrap();
    let derive = probe.calls() - before;
    head = gap_head(r.clone(), head).await;
    let before = probe.calls();
    let res = r
        .search(head, &SearchQuery::new("swarm a17 "))
        .await
        .unwrap();
    assert!(res.stats.tree_rows_derived > 0);
    (derive, probe.calls() - before)
}

/// Deriving or searching a head many lands past the index costs a few
/// metadata round trips per level of the tree, not a few per land. Deriving
/// a directory at a time made 13, 199 and 769 round trips at gaps of 1, 32
/// and 128 (searching: 17, 201, 769); in level batches it is 10 (14) at any
/// gap.
#[tokio::test]
async fn a_gap_of_many_lands_costs_round_trips_per_level() {
    for gap in [1, 32, 128] {
        let (derive, search) = round_trips_at(gap).await;
        eprintln!("gap {gap}: derive_search {derive} round trips, search {search}");
        assert!(derive <= 12, "gap {gap}: derive_search made {derive}");
        assert!(search <= 16, "gap {gap}: search made {search}");
    }
}
