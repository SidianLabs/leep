//! Paged log, search and ref log: concatenated pages equal one unpaged
//! answer, over random histories, and cursors are checked as untrusted input.

use leep_core::api::{ChangesetInfo, SearchMatch};
use leep_core::{ChangesetId, Error, Hash, RefUpdateContext};

use crate::cursor::{self, CursorKind, Fingerprint, FrontierEntry, LogResume, MAX_FRONTIER};
use crate::filter_log::tests::{filters, random_dag, release_dag, Lcg};
use crate::filter_log::SCAN_FIRST;
use crate::history::log_fingerprint;
use crate::tests::{commit, repo};
use crate::{LogFilter, LogParams, Repo, SearchQuery, MAX_CURSOR_CHARS};

fn seeds(default: u64) -> u64 {
    std::env::var("LEEP_PAGE_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

/// Knobs for one paged walk.
#[derive(Clone, Copy)]
struct Knobs {
    size: usize,
    max_frontier: usize,
    scan_first: usize,
}

/// Cursors seen that resume the indexed filter walk with bounds.
static BOUNDED: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// Every page of a log, concatenated, checking each page's shape.
async fn paged(r: &Repo, id: ChangesetId, p: &LogParams, k: Knobs) -> Vec<ChangesetInfo> {
    let mut out = Vec::new();
    let mut after: Option<String> = None;
    for _ in 0..100_000 {
        let page = r
            .log_page_with(
                id,
                p,
                k.size,
                after.as_deref(),
                k.max_frontier,
                k.scan_first,
            )
            .await
            .unwrap();
        assert!(page.entries.len() <= k.size);
        out.extend(page.entries.iter().cloned());
        let Some(next) = page.next else {
            return out;
        };
        assert_eq!(page.entries.len(), k.size, "only a full page has a next");
        assert!(next.len() <= MAX_CURSOR_CHARS);
        let resume: LogResume =
            cursor::decode(&next, CursorKind::Log, log_fingerprint(id, p)).unwrap();
        if let LogResume::Frontier { entries, indexed } = &resume {
            assert!(entries.len() <= k.max_frontier);
            if *indexed && entries.iter().any(|e| e.bound.is_some()) {
                BOUNDED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }
        after = Some(next);
    }
    panic!("paging did not end");
}

/// The unpaged answer, which must also be the reference scan's.
async fn whole(r: &Repo, id: ChangesetId, p: &LogParams) -> Vec<ChangesetInfo> {
    let all = r.log_page(id, p, usize::MAX, None).await.unwrap();
    assert_eq!(all.next, None);
    if !p.follow {
        let scan = r
            .log_scan(id, p.path.as_deref(), usize::MAX, &p.filter)
            .await
            .unwrap();
        assert_eq!(all.entries, scan);
    }
    all.entries
}

fn params(path: Option<&str>, filter: LogFilter) -> LogParams {
    LogParams {
        path: path.map(str::to_string),
        follow: false,
        filter,
    }
}

async fn check_paging(r: &Repo, tip: ChangesetId, p: &LogParams, knobs: &[Knobs], what: &str) {
    let want = whole(r, tip, p).await;
    for &k in knobs {
        let got = paged(r, tip, p, k).await;
        assert_eq!(
            got.iter().map(|i| i.id).collect::<Vec<_>>(),
            want.iter().map(|i| i.id).collect::<Vec<_>>(),
            "{what}, size {}, frontier {}, scan {}, {p:?}",
            k.size,
            k.max_frontier,
            k.scan_first
        );
        assert_eq!(got, want, "{what}");
    }
}

fn knobs(sizes: &[usize], frontiers: &[usize], scans: &[usize]) -> Vec<Knobs> {
    let mut out = Vec::new();
    for &size in sizes {
        for &max_frontier in frontiers {
            for &scan_first in scans {
                out.push(Knobs {
                    size,
                    max_frontier,
                    scan_first,
                });
            }
        }
    }
    out
}

#[tokio::test]
async fn log_pages_concatenate_to_the_whole_log_on_random_histories() {
    for seed in 1..=seeds(4) {
        let mut rng = Lcg(seed);
        let r = repo();
        let n = 40 + rng.below(200);
        let authors = 3 + rng.below(20);
        let ids = random_dag(&r, &mut rng, n, authors).await;
        let tips = [*ids.last().unwrap(), ids[rng.below(ids.len())]];
        let what = format!("seed {seed}");
        for &tip in &tips {
            // Plain and path logs, with the frontier carried, cut to one
            // entry, or replaced by the rescan fallback.
            let walk_knobs = knobs(&[1, 4, 23], &[MAX_FRONTIER, 1, 0], &[SCAN_FIRST]);
            for path in [None, Some("f1"), Some("f3/")] {
                check_paging(
                    &r,
                    tip,
                    &params(path, LogFilter::default()),
                    &walk_knobs,
                    &what,
                )
                .await;
            }
            // Filters, through the scan and through the index.
            let filter_knobs = knobs(&[1, 5], &[MAX_FRONTIER, 0], &[SCAN_FIRST, 0, 3]);
            for (i, filter) in filters(authors).into_iter().enumerate() {
                if i % 3 != seed as usize % 3 {
                    continue;
                }
                check_paging(&r, tip, &params(None, filter.clone()), &filter_knobs, &what).await;
                check_paging(
                    &r,
                    tip,
                    &params(Some("f2"), filter),
                    &knobs(&[2], &[MAX_FRONTIER, 0], &[SCAN_FIRST]),
                    &what,
                )
                .await;
            }
        }
    }
}

/// Release-shaped histories make the indexed filter walk bound the sides
/// of merges; those bounds travel in cursors and the pages still add up.
#[tokio::test]
async fn filter_log_pages_carry_merge_bounds() {
    BOUNDED.store(0, std::sync::atomic::Ordering::Relaxed);
    for seed in 1..=seeds(2) {
        for budget in [0, 7] {
            let mut rng = Lcg(seed);
            let r = repo();
            r.log_filter.set_side_budget(budget);
            let authors = 5 + rng.below(20);
            let (_, back, head) = release_dag(&r, &mut rng, 4, 5, authors).await;
            let what = format!("seed {seed}, budget {budget}");
            for &tip in [head].iter().chain(back.iter().take(1)) {
                for filter in filters(authors) {
                    check_paging(
                        &r,
                        tip,
                        &params(None, filter),
                        &knobs(&[1, 3], &[MAX_FRONTIER], &[0]),
                        &what,
                    )
                    .await;
                }
            }
        }
    }
    assert!(
        BOUNDED.load(std::sync::atomic::Ordering::Relaxed) > 0,
        "some cursor should carry a bounded merge side"
    );
}

fn body(n: usize) -> String {
    (0..n).map(|i| format!("tracked line {i}\n")).collect()
}

/// A history in which a tracked file is edited, renamed (exactly, so the
/// rename is always detected) and merged across branches. Returns the head
/// and the file's name there.
async fn rename_dag(r: &Repo, rng: &mut Lcg, n: usize) -> (ChangesetId, String) {
    let first = body(10);
    let c0 = commit(r, &[], &[("t0.txt", Some(&first)), ("o", Some("0"))], "add").await;
    // Per branch: tip, the tracked file's name and its content.
    let mut branches = vec![(c0, "t0.txt".to_string(), first)];
    for i in 1..n {
        let b = rng.below(branches.len());
        let (tip, name, text) = branches[b].clone();
        match rng.below(10) {
            0 | 1 => {
                let new = format!("t{i}.txt");
                let c = commit(r, &[tip], &[(&name, None), (&new, Some(&text))], "mv").await;
                branches[b] = (c, new, text);
            }
            2 | 3 if branches.len() > 1 => {
                let o = (b + 1 + rng.below(branches.len() - 1)) % branches.len();
                let c = commit(r, &[tip, branches[o].0], &[], "merge").await;
                branches[b].0 = c;
            }
            4 if branches.len() < 4 => branches.push((tip, name, text)),
            5 | 6 => {
                let c = commit(r, &[tip], &[("o", Some(&format!("{i}")))], "other").await;
                branches[b].0 = c;
            }
            _ => {
                let text = format!("{text}edit {i}\n");
                let c = commit(r, &[tip], &[(&name, Some(&text))], "edit").await;
                branches[b] = (c, name, text);
            }
        }
    }
    let (mut head, name, _) = branches[0].clone();
    for (tip, _, _) in branches.iter().skip(1) {
        head = commit(r, &[head, *tip], &[], "merge").await;
    }
    (head, name)
}

#[tokio::test]
async fn follow_pages_concatenate_across_renames_and_merges() {
    for seed in 1..=seeds(6) {
        let mut rng = Lcg(seed);
        let r = repo();
        let n = 30 + rng.below(60);
        let (head, name) = rename_dag(&r, &mut rng, n).await;
        let what = format!("seed {seed}");
        for filter in [
            LogFilter::default(),
            LogFilter {
                author: Some("nobody".into()),
                extra: vec![],
            },
        ] {
            let p = LogParams {
                path: Some(name.clone()),
                follow: true,
                filter,
            };
            let want = whole(&r, head, &p).await;
            let renames = want.iter().filter(|i| i.renamed.is_some()).count();
            if p.filter.is_empty() && seed == 1 {
                assert!(renames > 0, "the history should rename the file");
            }
            check_paging(
                &r,
                head,
                &p,
                &knobs(&[1, 2, 5], &[MAX_FRONTIER, 1, 0], &[SCAN_FIRST]),
                &what,
            )
            .await;
        }
    }
}

fn is_invalid<T: std::fmt::Debug>(r: leep_core::Result<T>) -> bool {
    matches!(r, Err(Error::Invalid(_)))
}

#[tokio::test]
async fn bad_log_cursors_are_invalid() {
    let r = repo();
    let mut rng = Lcg(3);
    let ids = random_dag(&r, &mut rng, 80, 4).await;
    let tip = *ids.last().unwrap();
    let plain = LogParams::default();
    let page = r.log_page(tip, &plain, 5, None).await.unwrap();
    let next = page.next.unwrap();

    // The same query resumes; another query, tip, or endpoint does not.
    assert!(r.log_page(tip, &plain, 5, Some(&next)).await.is_ok());
    let by_path = params(Some("f1"), LogFilter::default());
    assert!(is_invalid(r.log_page(tip, &by_path, 5, Some(&next)).await));
    assert!(is_invalid(r.log_page(ids[3], &plain, 5, Some(&next)).await));
    let filtered = params(
        None,
        LogFilter {
            author: Some("dev".into()),
            extra: vec![],
        },
    );
    assert!(is_invalid(r.log_page(tip, &filtered, 5, Some(&next)).await));
    assert!(is_invalid(r.ref_log_page(None, None, Some(&next), 5).await));

    // Garbage, truncation, oversize.
    let long = "A".repeat(MAX_CURSOR_CHARS + 1);
    for bad in ["", "%%%", "AAAA", &next[..next.len() / 2], &long] {
        assert!(
            is_invalid(r.log_page(tip, &plain, 5, Some(bad)).await),
            "{bad:?}"
        );
    }

    // Well-formed cursors no walk could have made.
    let fp = log_fingerprint(tip, &plain);
    let unknown = ChangesetId(Hash::of_blob(b"no such changeset"));
    let forged = |entries: Vec<FrontierEntry>, indexed: bool| {
        cursor::encode(
            CursorKind::Log,
            fp,
            &LogResume::Frontier { entries, indexed },
        )
        .unwrap()
    };
    let named = FrontierEntry {
        name: Some("f1".into()),
        ..FrontierEntry::id(tip)
    };
    let bounded = FrontierEntry {
        bound: Some((1, tip, tip)),
        ..FrontierEntry::id(tip)
    };
    let too_many: Vec<FrontierEntry> = ids
        .iter()
        .take(MAX_FRONTIER + 1)
        .map(|&i| FrontierEntry::id(i))
        .collect();
    for c in [
        forged(vec![FrontierEntry::id(unknown)], false),
        forged(vec![], false),
        forged(vec![named], false),
        forged(vec![bounded], false),
        forged(too_many, false),
    ] {
        let got = r.log_page(tip, &plain, 5, Some(&c)).await;
        assert!(is_invalid(got), "{c}");
    }
    // A cursor for a different fingerprint, made by hand.
    let other = Fingerprint::of(CursorKind::Log, &"other");
    let c = cursor::encode(
        CursorKind::Log,
        other,
        &LogResume::Below {
            generation: 3,
            id: tip,
        },
    )
    .unwrap();
    assert!(is_invalid(r.log_page(tip, &plain, 5, Some(&c)).await));
    // Follow needs a path, cursor or not.
    let follow = LogParams {
        follow: true,
        ..LogParams::default()
    };
    assert!(is_invalid(r.log_page(tip, &follow, 5, None).await));
}

/// A tree of files with "needle" on some lines, in nested directories (so
/// directory order and the `a`/`a.txt` ordering are exercised).
async fn needle_tree(r: &Repo, rng: &mut Lcg, files: usize, tag: &str) -> ChangesetId {
    let mut owned: Vec<(String, String)> = Vec::new();
    for i in 0..files {
        let dir = match rng.below(4) {
            0 => String::new(),
            1 => "a/".to_string(),
            2 => format!("a/b{}/", rng.below(3)),
            _ => "a.d/".to_string(),
        };
        let mut text = String::new();
        for l in 0..rng.below(12) {
            if rng.below(3) == 0 {
                text.push_str(&format!("needle {tag} {i} {l}\n"));
            } else {
                text.push_str(&format!("hay {l}\n"));
            }
        }
        owned.push((format!("{dir}f{i}.txt"), text));
    }
    owned.push(("a.txt".into(), format!("needle {tag} top\n")));
    let files: Vec<(&str, Option<&str>)> = owned
        .iter()
        .map(|(p, t)| (p.as_str(), Some(t.as_str())))
        .collect();
    commit(r, &[], &files, tag).await
}

async fn search_pages(r: &Repo, rev: &str, q: &SearchQuery) -> Vec<SearchMatch> {
    let mut out = Vec::new();
    let mut after: Option<String> = None;
    for _ in 0..10_000 {
        let page = r.search_page(rev, q, after.as_deref()).await.unwrap();
        assert!(page.matches.len() <= q.limit);
        out.extend(page.matches);
        match page.next {
            Some(next) => {
                assert!(page.truncated);
                after = Some(next);
            }
            None => return out,
        }
    }
    panic!("paging did not end");
}

#[tokio::test]
async fn search_pages_concatenate_to_the_whole_answer() {
    for seed in 1..=seeds(4) {
        let mut rng = Lcg(seed);
        let r = repo();
        let n = 10 + rng.below(40);
        let id = needle_tree(&r, &mut rng, n, "x").await;
        let rev = id.to_string();
        for use_index in [true, false] {
            for (pattern, regex) in [("needle", false), ("needle x [0-9]+ 1", true)] {
                let base = SearchQuery {
                    regex,
                    use_index,
                    limit: crate::search::MAX_LIMIT,
                    ..SearchQuery::new(pattern)
                };
                let want = r.search(id, &base).await.unwrap();
                assert!(!want.truncated);
                for limit in [1, 2, 7] {
                    let q = SearchQuery {
                        limit,
                        ..base.clone()
                    };
                    let got = search_pages(&r, &rev, &q).await;
                    assert_eq!(got, want.matches, "seed {seed}, limit {limit}");
                }
                // A scan budget that runs out every few files (but exceeds
                // any one file): pages resume after the last file finished.
                let q = SearchQuery {
                    max_scan_bytes: 400,
                    ..base.clone()
                };
                let got = search_pages(&r, &rev, &q).await;
                assert_eq!(got, want.matches, "seed {seed}, small budget");
                // Within a path and a glob.
                for (path, glob) in [("a", None), ("", Some("a/**/*.txt"))] {
                    let narrow = SearchQuery {
                        path: path.into(),
                        glob: glob.map(str::to_string),
                        ..base.clone()
                    };
                    let want = r.search(id, &narrow).await.unwrap();
                    let q = SearchQuery { limit: 2, ..narrow };
                    assert_eq!(search_pages(&r, &rev, &q).await, want.matches);
                }
            }
        }
    }
}

#[tokio::test]
async fn search_cursor_pins_the_revision_and_its_query() {
    let r = repo();
    let mut rng = Lcg(9);
    let old = needle_tree(&r, &mut rng, 30, "old").await;
    let new = needle_tree(&r, &mut rng, 30, "new").await;
    let ctx = RefUpdateContext::new("cas");
    assert!(r
        .cas_ref_checked("main", None, Some(old), &ctx)
        .await
        .unwrap());
    let base = SearchQuery {
        limit: crate::search::MAX_LIMIT,
        ..SearchQuery::new("needle")
    };
    let want = r.search(old, &base).await.unwrap().matches;
    let q = SearchQuery {
        limit: 3,
        ..base.clone()
    };
    // The ref moves after the first page; later pages still read `old`.
    let first = r.search_page("main", &q, None).await.unwrap();
    assert!(r
        .cas_ref_checked("main", Some(old), Some(new), &ctx)
        .await
        .unwrap());
    let mut got = first.matches;
    let mut after = first.next;
    while let Some(c) = after {
        let page = r.search_page("main", &q, Some(&c)).await.unwrap();
        got.extend(page.matches);
        after = page.next;
    }
    assert_eq!(got, want);
    let first = r.search_page("main", &q, None).await.unwrap();
    let next = first.next.unwrap();
    let second = r.search_page("main", &q, Some(&next)).await.unwrap();
    assert!(second.matches.iter().all(|m| m.line.contains("new")));

    // Another query or revision text refuses the cursor.
    let other = SearchQuery {
        regex: true,
        ..q.clone()
    };
    assert!(is_invalid(r.search_page("main", &other, Some(&next)).await));
    assert!(is_invalid(
        r.search_page(&new.to_string(), &q, Some(&next)).await
    ));
    assert!(is_invalid(r.search_page("main", &q, Some("garbage")).await));
    // A well-formed cursor naming an unknown revision.
    let fp = Fingerprint::of(
        CursorKind::Search,
        &("main", "needle", false, false, "", None::<&str>),
    );
    let c = cursor::encode(
        CursorKind::Search,
        fp,
        &cursor::SearchPos {
            id: ChangesetId(Hash::of_blob(b"nope")),
            path: "a".into(),
            line: 1,
        },
    )
    .unwrap();
    assert!(is_invalid(r.search_page("main", &q, Some(&c)).await));
}

#[tokio::test]
async fn ref_log_pages_concatenate_and_reject_mixed_bounds() {
    let r = repo();
    let ctx = RefUpdateContext::new("cas");
    let mut prev = [None, None];
    for i in 0..23 {
        let c = commit(&r, &[], &[("f", Some(&format!("{i}")))], "c").await;
        let k = i % 2;
        let name = ["main", "dev"][k];
        assert!(r
            .cas_ref_checked(name, prev[k], Some(c), &ctx)
            .await
            .unwrap());
        prev[k] = Some(c);
    }
    for name in [None, Some("main"), Some("nosuch")] {
        let all = r.ref_log_page(name, None, None, 1000).await.unwrap();
        assert_eq!(all.next, None);
        for limit in [1, 2, 5, 22, 23] {
            let mut got = Vec::new();
            let mut after: Option<String> = None;
            loop {
                let page = r
                    .ref_log_page(name, None, after.as_deref(), limit)
                    .await
                    .unwrap();
                assert!(page.entries.len() <= limit);
                got.extend(page.entries);
                match page.next {
                    Some(n) => after = Some(n),
                    None => break,
                }
            }
            assert_eq!(got, all.entries, "{name:?}, limit {limit}");
        }
    }
    let page = r.ref_log_page(Some("main"), None, None, 2).await.unwrap();
    let next = page.next.unwrap();
    assert!(is_invalid(
        r.ref_log_page(Some("main"), Some(5), Some(&next), 2).await
    ));
    assert!(is_invalid(
        r.ref_log_page(Some("dev"), None, Some(&next), 2).await
    ));
    assert!(is_invalid(r.ref_log_page(None, None, Some(&next), 2).await));
    assert!(is_invalid(r.ref_log_page(None, None, Some("x"), 2).await));
    // `before` alone keeps working, and also answers `next`.
    let old = r
        .ref_log_page(Some("main"), Some(10), None, 2)
        .await
        .unwrap();
    assert!(old.entries.iter().all(|e| e.seq < 10) && old.next.is_some());
}
