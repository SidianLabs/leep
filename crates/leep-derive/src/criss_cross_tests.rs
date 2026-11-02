//! Merges in criss-cross histories, where two changesets have more than one
//! best common ancestor (ADR 0071): an edit survives the merge or conflicts,
//! and never vanishes because one base was picked over another.

use std::collections::{BTreeMap, BTreeSet, HashSet};

use leep_core::api::MergeResponse;
use leep_core::{ChangesetId, FileChange, RepoPath};

use crate::owners::OwnersResolver;
use crate::review::{PathRule, ReviewRules};
use crate::tests::{commit, merge_req, repo, rp, text};
use crate::Repo;

/// A and B fork from O and change `f`; M1 = A+B keeps A's `f`, M2 = B+A keeps
/// B's. The tips descend from M1 and M2. Bases A and B disagree on `f`, and
/// a merge against either one alone takes a side's `f` without a conflict.
#[tokio::test]
async fn disagreeing_resolutions_of_a_criss_cross_conflict() {
    let r = repo();
    let o = commit(&r, &[], &[("f", Some("o\n")), ("g", Some("g\n"))], "o").await;
    let a = commit(&r, &[o], &[("f", Some("a\n"))], "a").await;
    let b = commit(&r, &[o], &[("f", Some("b\n"))], "b").await;
    let m1 = commit(&r, &[a, b], &[], "m1 keeps a").await;
    let m2 = commit(&r, &[b, a], &[], "m2 keeps b").await;
    let x = commit(&r, &[m1], &[("x", Some("x\n"))], "x").await;
    let y = commit(&r, &[m2], &[("y", Some("y\n")), ("g", Some("g2\n"))], "y").await;

    let bases: BTreeSet<_> = r.merge_bases(x, y).await.unwrap().into_iter().collect();
    assert_eq!(bases, BTreeSet::from([a, b]));
    assert!(bases.contains(&r.merge_base(x, y).await.unwrap().unwrap()));

    for (ours, theirs) in [(x, y), (y, x)] {
        let MergeResponse::Conflicts { conflicts, clean } =
            r.merge(merge_req(ours, theirs)).await.unwrap()
        else {
            panic!("the bases disagree on f: a clean merge drops one side's f")
        };
        let paths: Vec<&str> = conflicts.iter().map(|c| c.path.as_str()).collect();
        assert_eq!(paths, ["f"]);
        // Everything else still merges.
        let want: Vec<&str> = if ours == x { vec!["g", "y"] } else { vec!["x"] };
        assert_eq!(clean.keys().map(|p| p.as_str()).collect::<Vec<_>>(), want);
    }
}

/// Both criss-cross merges resolve `f` the same way: every base gives the
/// same answer, so the merge is clean and keeps both tips' work.
#[tokio::test]
async fn agreeing_resolutions_of_a_criss_cross_merge_cleanly() {
    let r = repo();
    let o = commit(&r, &[], &[("f", Some("o\n"))], "o").await;
    let a = commit(&r, &[o], &[("f", Some("a\n"))], "a").await;
    let b = commit(&r, &[o], &[("f", Some("b\n"))], "b").await;
    let m1 = commit(&r, &[a, b], &[("f", Some("ab\n"))], "m1").await;
    let m2 = commit(&r, &[b, a], &[("f", Some("ab\n"))], "m2").await;
    let x = commit(&r, &[m1], &[("x", Some("x\n"))], "x").await;
    let y = commit(&r, &[m2], &[("y", Some("y\n"))], "y").await;
    assert_eq!(r.merge_bases(x, y).await.unwrap().len(), 2);
    let MergeResponse::Merged { id } = r.merge(merge_req(x, y)).await.unwrap() else {
        panic!("all bases agree")
    };
    assert_eq!(text(&r, id, "f").await.as_deref(), Some("ab\n"));
    assert_eq!(text(&r, id, "x").await.as_deref(), Some("x\n"));
    assert_eq!(text(&r, id, "y").await.as_deref(), Some("y\n"));

    // A later edit of `f` on one side conflicts against both bases (each
    // has neither side's `f`), as it would with either one alone.
    let y2 = commit(&r, &[y], &[("f", Some("ab2\n"))], "y2").await;
    let MergeResponse::Conflicts { conflicts, .. } = r.merge(merge_req(x, y2)).await.unwrap()
    else {
        panic!("both sides changed f against every base")
    };
    assert_eq!(conflicts.len(), 1);
}

/// The comment-only and owners rules read merge bases too: a merge that
/// takes a side's version is covered only if the first parent left the path
/// as every base has it.
#[tokio::test]
async fn review_of_a_criss_cross_merge_checks_every_base() {
    let r = repo();
    let o = commit(&r, &[], &[("f", Some("o\n"))], "o").await;
    let a = commit(&r, &[o], &[("f", Some("a\n"))], "a").await;
    let b = commit(&r, &[o], &[("f", Some("b\n"))], "b").await;
    let m1 = commit(&r, &[a, b], &[], "m1 keeps a").await;
    let m2 = commit(&r, &[b, a], &[], "m2 keeps b").await;
    let x = commit(&r, &[m1], &[("x", Some("x\n"))], "x").await;
    let y = commit(&r, &[m2], &[("y", Some("y\n"))], "y").await;
    // A hand-made merge of x and y that takes y's `f` silently.
    let m = commit(&r, &[x, y], &[("f", Some("b\n")), ("y", Some("y\n"))], "m").await;
    let mut owners = OwnersResolver::new(&r, None).await.unwrap();
    let review = r
        .review_changeset(m, &ReviewRules::default(), &[], &mut owners)
        .await
        .unwrap();
    let f = review
        .paths
        .iter()
        .find(|p| p.path.as_str() == "f")
        .unwrap();
    // Against base A the merge left `f` alone on the first parent; against B
    // it threw x's `f` away.
    assert_eq!(f.rule, PathRule::Pending, "taking y's f is not covered");
    let y_path = review
        .paths
        .iter()
        .find(|p| p.path.as_str() == "y")
        .unwrap();
    assert_eq!(y_path.rule, PathRule::MergeParent);
}

/// Every best common ancestor of `a` and `b`, by brute force over whole
/// ancestor sets.
async fn bases_oracle(r: &Repo, a: ChangesetId, b: ChangesetId) -> BTreeSet<ChangesetId> {
    async fn ancestors(r: &Repo, tip: ChangesetId) -> HashSet<ChangesetId> {
        let mut seen = HashSet::from([tip]);
        let mut stack = vec![tip];
        while let Some(c) = stack.pop() {
            for &p in r.parents(c).await.unwrap().iter() {
                if seen.insert(p) {
                    stack.push(p);
                }
            }
        }
        seen
    }
    let (sa, sb) = (ancestors(r, a).await, ancestors(r, b).await);
    let common: Vec<ChangesetId> = sa.intersection(&sb).copied().collect();
    let mut out = BTreeSet::new();
    for &c in &common {
        let mut behind = false;
        for &d in &common {
            if d != c && ancestors(r, d).await.contains(&c) {
                behind = true;
                break;
            }
        }
        if !behind {
            out.insert(c);
        }
    }
    out
}

const FILES: [&str; 4] = ["f0", "f1", "f2", "f3"];

/// Random histories of a few branches that commit and merge each other with
/// arbitrary resolutions (keep ours, take theirs, or write something new),
/// so criss-crosses with disagreeing resolutions are common. Every file
/// holds one token, so a path both sides changed differently against a
/// base is a conflict against it. Merging two tips must then match the
/// oracle: a path is clean only if every best common ancestor resolves it
/// the same way, and a side's edit is never dropped without a conflict.
#[tokio::test]
async fn random_criss_cross_merges_never_drop_an_edit() {
    let mut seed: u64 = 0xc055_c055;
    let mut rand = move |n: usize| {
        seed = seed
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        ((seed >> 33) as usize) % n
    };
    let (mut checked, mut multi, mut conflicted) = (0, 0, 0);
    let mut pairs = Vec::new();
    for round in 0..40 {
        let r = repo();
        let init: Vec<(&str, Option<&str>)> = FILES.iter().map(|f| (*f, Some("0"))).collect();
        let root = commit(&r, &[], &init, "root").await;
        let mut tips = [root; 3];
        let mut all = vec![root];
        for step in 0..20 {
            let i = rand(tips.len());
            let j = (i + 1 + rand(tips.len() - 1)) % tips.len();
            let kind = rand(4);
            if kind < 2 && tips[i] == tips[j] {
                continue;
            }
            // 0: i and j merge each other (a criss-cross); 1: i merges j;
            // otherwise i commits.
            let moves: Vec<(usize, Vec<ChangesetId>)> = match kind {
                0 => vec![(i, vec![tips[i], tips[j]]), (j, vec![tips[j], tips[i]])],
                1 => vec![(i, vec![tips[i], tips[j]])],
                _ => vec![(i, vec![tips[i]])],
            };
            let mut next = Vec::new();
            for (k, (slot, parents)) in moves.into_iter().enumerate() {
                let tok = format!("r{round}s{step}m{k}");
                let mut files: Vec<(&str, Option<String>)> = Vec::new();
                for f in FILES {
                    let pick = rand(6);
                    let has = text(&r, parents[0], f).await.is_some();
                    match (parents.len(), pick) {
                        // A hand-made merge: each file as ours, theirs or new.
                        (2, 0..=1) => {}
                        (2, 2..=3) => {
                            let theirs = text(&r, parents[1], f).await;
                            if has || theirs.is_some() {
                                files.push((f, theirs));
                            }
                        }
                        (_, 4) => files.push((f, Some(format!("{tok}{f}")))),
                        (_, 5) if has && rand(4) == 0 => files.push((f, None)),
                        _ => {}
                    }
                }
                let refs: Vec<(&str, Option<&str>)> =
                    files.iter().map(|(p, c)| (*p, c.as_deref())).collect();
                next.push((slot, commit(&r, &parents, &refs, &tok).await));
            }
            for (slot, id) in next {
                tips[slot] = id;
                all.push(id);
            }
        }
        for _ in 0..6 {
            if rand(2) == 0 {
                let i = rand(tips.len());
                let j = (i + 1 + rand(tips.len() - 1)) % tips.len();
                pairs.push((tips[i], tips[j]));
            } else {
                pairs.push((all[rand(all.len())], all[rand(all.len())]));
            }
        }
        for (ours, theirs) in std::mem::take(&mut pairs) {
            let bases = bases_oracle(&r, ours, theirs).await;
            let got: BTreeSet<_> = r
                .merge_bases(ours, theirs)
                .await
                .unwrap()
                .into_iter()
                .collect();
            assert_eq!(got, bases, "merge_bases({ours}, {theirs}) in round {round}");
            if bases.len() > 1 {
                multi += 1;
            }
            let (merged_id, result, conflicts) =
                match r.merge(merge_req(ours, theirs)).await.unwrap() {
                    MergeResponse::Merged { id } => (Some(id), BTreeMap::new(), BTreeSet::new()),
                    MergeResponse::Conflicts { conflicts, clean } => {
                        conflicted += 1;
                        let paths: BTreeSet<String> =
                            conflicts.into_iter().map(|c| c.path.to_string()).collect();
                        (None, clean, paths)
                    }
                };
            let result: BTreeMap<RepoPath, FileChange> = result;
            for f in FILES {
                let o = text(&r, ours, f).await;
                let t = text(&r, theirs, f).await;
                let mut want: Option<Option<String>> = None;
                let mut clean = true;
                let mut edits = Vec::new();
                for &b in &bases {
                    let at = text(&r, b, f).await;
                    let end = if o == t || t == at {
                        Some(o.clone())
                    } else if o == at {
                        Some(t.clone())
                    } else {
                        None
                    };
                    for side in [&o, &t] {
                        if *side != at {
                            edits.push(side.clone());
                        }
                    }
                    match (end, &want) {
                        (None, _) => clean = false,
                        (Some(e), None) => want = Some(e),
                        (Some(e), Some(w)) if &e != w => clean = false,
                        _ => {}
                    }
                }
                let ctx = format!("{f} merging {ours} and {theirs} in round {round}");
                assert_eq!(!clean, conflicts.contains(f), "{ctx}");
                if !clean {
                    continue;
                }
                let want = want.expect("a clean path has an answer");
                // A side that changed the path against any base wins it:
                // no edit vanishes without a conflict.
                for e in &edits {
                    assert_eq!(*e, want, "{ctx}: an edit vanished");
                }
                match merged_id {
                    Some(id) => assert_eq!(text(&r, id, f).await, want, "{ctx}"),
                    None => {
                        let p = rp(f);
                        let now = match result.get(&p) {
                            None => o.clone(),
                            Some(FileChange::Delete) => None,
                            Some(FileChange::Modify { content, .. }) => Some(
                                String::from_utf8(
                                    r.blobs().get(content).await.unwrap().unwrap().to_vec(),
                                )
                                .unwrap(),
                            ),
                        };
                        assert_eq!(now, want, "{ctx}");
                    }
                }
            }
            checked += 1;
        }
    }
    assert!(checked >= 200, "{checked}");
    assert!(
        multi >= 20,
        "too few criss-crosses to mean anything: {multi}"
    );
    assert!(conflicted > 0);
}

/// A review asks for a merge's bases at every changed path; in a
/// criss-cross each answer is a walk, so a review finds them once per side.
#[tokio::test]
async fn a_review_finds_merge_bases_once_per_side() {
    use crate::ancestry::MERGE_BASES_CALLS;
    let r = repo();
    let o = commit(&r, &[], &[("f", Some("o\n"))], "o").await;
    let a = commit(&r, &[o], &[("f", Some("a\n"))], "a").await;
    let b = commit(&r, &[o], &[("f", Some("b\n"))], "b").await;
    let m1 = commit(&r, &[a, b], &[], "m1").await;
    let m2 = commit(&r, &[b, a], &[], "m2").await;
    let paths: Vec<String> = (0..40).map(|i| format!("d/{i}.txt")).collect();
    let files: Vec<(&str, Option<&str>)> =
        paths.iter().map(|p| (p.as_str(), Some("y\n"))).collect();
    let y = commit(&r, &[m2], &files, "y").await;
    let x = commit(&r, &[m1], &[("x", Some("x\n"))], "x").await;
    // Takes all of y's files, and y's `f`.
    let mut taken = files.clone();
    taken.push(("f", Some("b\n")));
    let m = commit(&r, &[x, y], &taken, "m").await;
    let calls = MERGE_BASES_CALLS
        .scope(std::cell::Cell::new(0), async {
            let mut owners = OwnersResolver::new(&r, None).await.unwrap();
            let review = r
                .review_changeset(m, &ReviewRules::default(), &[], &mut owners)
                .await
                .unwrap();
            assert_eq!(review.paths.len(), 41);
            MERGE_BASES_CALLS.with(|c| c.get())
        })
        .await;
    assert_eq!(calls, 1, "one side, one merge_bases");
}
