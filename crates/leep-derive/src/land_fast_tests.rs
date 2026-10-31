//! The land fast path and the slow path's restricted merge (ADR 0066): each
//! must answer exactly what the full replay answers.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use leep_core::api::LandResponse;
use leep_core::memory::{MemoryBlobStore, MemoryMetaStore};
use leep_core::review::{Review, Verdict};
use leep_core::{
    Changeset, ChangesetId, Error, FileChange, FileKind, LandId, RefLogEntry, RepoPath, Result,
};

use crate::land_fast::Lookup;
use crate::owners::OwnersResolver;
use crate::review::ReviewRules;
use crate::{LandPolicy, Repo};

pub(crate) struct Rng(pub(crate) u64);

impl Rng {
    pub(crate) fn new(seed: u64) -> Self {
        Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    pub(crate) fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

fn plain_repo() -> Repo {
    Repo::new(
        Arc::new(MemoryBlobStore::new()),
        Arc::new(MemoryMetaStore::new()),
    )
}

/// A file's content and whether it is executable.
pub(crate) type Body = (String, bool);
/// The files of a tree, by path.
pub(crate) type Files = BTreeMap<String, Body>;
/// A changeset's changes in model form: `None` deletes.
pub(crate) type Edits = BTreeMap<String, Option<Body>>;

/// Paths that nest inside each other, so random edits clash as files and
/// directories as well as on the same file.
const NAMES: &[&str] = &["a", "b", "c"];

/// A random path: under `own` mostly, when given, so agents overlap now and
/// then rather than always.
fn random_path(rng: &mut Rng, own: &str) -> String {
    let depth = 1 + rng.below(3);
    let mut parts: Vec<String> = Vec::new();
    if !own.is_empty() && rng.below(5) != 0 {
        parts.push(own.to_string());
    }
    for _ in 0..depth {
        parts.push(NAMES[rng.below(NAMES.len())].to_string());
    }
    parts.join("/")
}

fn within(p: &str, dir: &str) -> bool {
    p.len() > dir.len() && p.starts_with(dir) && p.as_bytes()[dir.len()] == b'/'
}

/// A file of `work`, under `own` mostly when it has any there.
fn pick(rng: &mut Rng, work: &Files, own: &str) -> Option<String> {
    let mine: Vec<&String> = work.keys().filter(|k| within(k, own)).collect();
    if !own.is_empty() && !mine.is_empty() && rng.below(5) != 0 {
        return Some(mine[rng.below(mine.len())].clone());
    }
    work.keys().nth(rng.below(work.len().max(1))).cloned()
}

/// Make room for a file at `p` in `work`: a file on the way, or a directory
/// where the file goes, is deleted first.
fn clear_for(work: &mut Files, p: &str) {
    let mut prefix = String::new();
    for comp in p.split('/') {
        if !prefix.is_empty() {
            prefix.push('/');
        }
        prefix.push_str(comp);
        if prefix != p {
            work.remove(&prefix);
        }
    }
    work.retain(|k, _| !within(k, p));
}

/// Random valid edits of `files`, `ops` of them: modifies (sometimes to
/// the same content, a no-op change, or to another kind), deletes, renames
/// (a delete and an add), and adds that replace a file with a directory or
/// a directory with a file.
pub(crate) fn random_edits(rng: &mut Rng, files: &Files, ops: usize, own: &str) -> Edits {
    let mut work = files.clone();
    let body = |rng: &mut Rng| -> Body { (format!("v{}\n", rng.below(4)), rng.below(8) == 0) };
    for _ in 0..ops {
        match rng.below(5) {
            0 => {
                if let Some(k) = pick(rng, &work, own) {
                    work.remove(&k);
                }
            }
            1 => {
                if let Some(k) = pick(rng, &work, own) {
                    let b = body(rng);
                    work.insert(k, b);
                }
            }
            2 => {
                if let Some(k) = pick(rng, &work, own) {
                    let b = work.remove(&k).unwrap();
                    let p = random_path(rng, own);
                    clear_for(&mut work, &p);
                    work.insert(p, b);
                }
            }
            _ => {
                let p = random_path(rng, own);
                clear_for(&mut work, &p);
                let b = body(rng);
                work.insert(p, b);
            }
        }
    }
    let mut edits = Edits::new();
    for (k, v) in &work {
        // Unchanged files stay out, except now and then as a no-op modify.
        if files.get(k) != Some(v) || rng.below(12) == 0 {
            edits.insert(k.clone(), Some(v.clone()));
        }
    }
    for k in files.keys() {
        if !work.contains_key(k) {
            edits.insert(k.clone(), None);
        }
    }
    edits
}

pub(crate) fn apply(files: &Files, edits: &Edits) -> Files {
    let mut out = files.clone();
    for (k, v) in edits {
        match v {
            Some(b) => out.insert(k.clone(), b.clone()),
            None => out.remove(k),
        };
    }
    out
}

pub(crate) async fn create(
    r: &Repo,
    parents: &[ChangesetId],
    edits: &Edits,
    msg: &str,
) -> ChangesetId {
    let mut changes = BTreeMap::new();
    for (path, body) in edits {
        let change = match body {
            Some((text, exec)) => FileChange::Modify {
                content: r.put_blob(Bytes::from(text.clone())).await.unwrap(),
                kind: if *exec {
                    FileKind::Executable
                } else {
                    FileKind::Regular
                },
                size: text.len() as u64,
            },
            None => FileChange::Delete,
        };
        changes.insert(RepoPath::new(path.as_str()).unwrap(), change);
    }
    r.create_changeset(Changeset {
        parents: parents.to_vec(),
        author: "t".into(),
        timestamp: 1_700_000_000,
        message: msg.into(),
        changes,
        extra: BTreeMap::new(),
    })
    .await
    .unwrap()
}

/// Reading `ours` only at the paths `theirs` changed merges exactly as
/// diffing the whole of `ours` did: same changes, same conflicts, over
/// random trees with nested files, file/directory swaps, no-op modifies,
/// kind changes and text merges.
#[tokio::test]
async fn a_restricted_merge_matches_the_full_merge() {
    let mut differ = 0;
    for seed in 1..=300u64 {
        let mut rng = Rng::new(seed);
        let r = plain_repo();
        let base_files = random_edits(&mut rng, &Files::new(), 6, "")
            .into_iter()
            .filter_map(|(k, v)| Some((k, v?)))
            .collect::<Files>();
        let base_edits: Edits = base_files
            .iter()
            .map(|(k, v)| (k.clone(), Some(v.clone())))
            .collect();
        let base = create(&r, &[], &base_edits, "base").await;
        let ours_ops = rng.below(8);
        let ours = create(
            &r,
            &[base],
            &random_edits(&mut rng, &base_files, ours_ops, ""),
            "ours",
        )
        .await;
        let theirs_ops = 1 + rng.below(4);
        let theirs = create(
            &r,
            &[base],
            &random_edits(&mut rng, &base_files, theirs_ops, ""),
            "theirs",
        )
        .await;
        let (b, o, t) = (
            r.root_tree(base).await.unwrap(),
            r.root_tree(ours).await.unwrap(),
            r.root_tree(theirs).await.unwrap(),
        );
        let got = r.merge_trees(b, o, t).await.unwrap();
        let want = r.merge_trees_full(b, o, t).await.unwrap();
        assert_eq!(got.changes, want.changes, "seed {seed}");
        assert_eq!(got.conflicts, want.conflicts, "seed {seed}");
        differ += usize::from(!want.conflicts.is_empty());
    }
    assert!(differ > 20, "some merges conflicted ({differ})");
}

/// The files of `id`'s tree, in model form.
async fn files_of(r: &Repo, id: ChangesetId) -> Files {
    let mut out = Files::new();
    for f in r.list_files(id, "").await.unwrap() {
        let text = r.blobs().get(&f.content).await.unwrap().unwrap();
        out.insert(
            f.path.as_str().to_string(),
            (
                String::from_utf8(text.to_vec()).unwrap(),
                f.kind == FileKind::Executable,
            ),
        );
    }
    out
}

fn outcome(r: &Result<LandResponse>) -> String {
    match r {
        Ok(resp) => format!("{resp:?}"),
        Err(e) => format!("error: {e}"),
    }
}

fn log_steps(entries: &[RefLogEntry]) -> Vec<String> {
    entries
        .iter()
        .rev()
        .map(|e| {
            format!(
                "{:?} -> {:?}: {} {:?}",
                e.old, e.new, e.operation, e.message
            )
        })
        .collect()
}

fn lookups(r: &Repo) -> Vec<Lookup> {
    r.land_queues
        .lookups
        .lock()
        .unwrap()
        .iter()
        .map(|(_, l)| *l)
        .collect()
}

/// Two repositories built alike: one lands with the fast path, one without.
struct Pair {
    fast: Repo,
    slow: Repo,
}

impl Pair {
    fn new() -> Pair {
        Pair {
            fast: plain_repo(),
            slow: plain_repo().with_land_fast_path(false),
        }
    }

    fn with(fast: Repo, slow: Repo) -> Pair {
        Pair { fast, slow }
    }

    /// Create the same changeset in both; the ids must agree.
    async fn create(&self, parents: &[ChangesetId], edits: &Edits, msg: &str) -> ChangesetId {
        let a = create(&self.fast, parents, edits, msg).await;
        let b = create(&self.slow, parents, edits, msg).await;
        assert_eq!(a, b, "{msg}: both repositories hold the same history");
        a
    }

    /// Land `cs` in both, one after the other; the answers must agree.
    async fn land(&self, cs: ChangesetId) -> String {
        let a = outcome(&self.fast.land(cs, "main").await);
        let b = outcome(&self.slow.land(cs, "main").await);
        assert_eq!(a, b, "land of {cs}");
        a
    }

    async fn head(&self) -> ChangesetId {
        let a = self.fast.meta().get_ref("main").await.unwrap().unwrap();
        let b = self.slow.meta().get_ref("main").await.unwrap().unwrap();
        assert_eq!(a, b, "same head");
        assert_eq!(
            self.fast.root_tree(a).await.unwrap(),
            self.slow.root_tree(b).await.unwrap()
        );
        a
    }

    async fn assert_same_logs(&self) {
        let a = self.fast.meta().ref_log(Some("main"), None, 100_000).await;
        let b = self.slow.meta().ref_log(Some("main"), None, 100_000).await;
        assert_eq!(log_steps(&a.unwrap()), log_steps(&b.unwrap()));
    }
}

fn edits(list: &[(&str, Option<&str>)]) -> Edits {
    list.iter()
        .map(|(p, b)| (p.to_string(), b.map(|b| (b.to_string(), false))))
        .collect()
}

/// A land nothing on the ref touched since its base is copied without a
/// merge, and the copy is the slow path's to the byte: the same ids for
/// every member of the stack, the same root trees, the same rebase links,
/// the same ref log. The stack drops a no-op modify (as the merge's tree
/// diff does), deletes a file, swaps a file for a directory and changes a
/// file's kind.
#[tokio::test]
async fn a_disjoint_land_copies_to_the_slow_paths_bytes() {
    let p = Pair::new();
    let base = p
        .create(
            &[],
            &edits(&[
                ("lib/a", Some("a\n")),
                ("lib/b", Some("b\n")),
                ("mine/x", Some("x\n")),
                ("mine/y", Some("y\n")),
                ("mine/f", Some("f\n")),
            ]),
            "base",
        )
        .await;
    p.land(base).await;
    // The agent's stack, based on `base`.
    let mut e1 = edits(&[
        ("mine/x", Some("x2\n")),
        ("mine/y", None),
        // A no-op: the file already holds this.
        ("lib/a", Some("a\n")),
        ("mine/f", None),
        ("mine/f/g", Some("g\n")),
    ]);
    e1.insert("mine/z".into(), Some(("z\n".into(), true)));
    let s1 = p.create(&[base], &e1, "s1").await;
    let s2 = p
        .create(&[s1], &edits(&[("mine/w", Some("w\n"))]), "s2")
        .await;
    // Meanwhile the ref moves on, elsewhere in the tree, through the same
    // leader, so the fast repository's map knows these heads.
    for i in 0..5 {
        let head = p.head().await;
        let other = p
            .create(
                &[head],
                &edits(&[(format!("lib/n{i}").as_str(), Some("n\n"))]),
                &format!("trunk {i}"),
            )
            .await;
        p.land(other).await;
    }
    // Both took the slow path for nothing so far: trunk lands fast-forward.
    let answer = p.land(s2).await;
    assert!(answer.contains("rebased: true"), "{answer}");
    assert_eq!(lookups(&p.fast), vec![Lookup::Hit]);
    assert!(
        lookups(&p.slow).is_empty(),
        "the slow repository never looks"
    );
    let head = p.head().await;
    // The copy of s1 dropped the no-op and kept the rest.
    let copy2 = p.fast.get_changeset(head).await.unwrap();
    let copy1 = p.fast.get_changeset(copy2.parents[0]).await.unwrap();
    assert!(!copy1.changes.contains_key(&RepoPath::new("lib/a").unwrap()));
    assert_eq!(copy1.changes.len(), 5);
    for id in [head, copy2.parents[0]] {
        assert_eq!(
            p.fast.rebased_origin(id).await.unwrap(),
            p.slow.rebased_origin(id).await.unwrap()
        );
    }
    p.assert_same_logs().await;
}

/// Lands the map cannot show disjoint replay the slow way with the same
/// result: the same file, a file where the ref made a directory, a
/// directory where the ref made a file, a delete under a directory the ref
/// wrote into.
#[tokio::test]
async fn overlapping_lands_take_the_slow_path_with_the_same_answers() {
    let cases: Vec<(Edits, Edits)> = vec![
        // Same file, merging cleanly (different lines).
        (
            edits(&[("t", Some("1\n2\nX\n"))]),
            edits(&[("t", Some("Y\n2\n3\n"))]),
        ),
        // Same file, conflicting.
        (edits(&[("t", Some("A\n"))]), edits(&[("t", Some("B\n"))])),
        // The ref made `d` a file; the land writes under `d/`.
        (
            edits(&[("d/one", None), ("d", Some("file\n"))]),
            edits(&[("d/two", Some("2\n"))]),
        ),
        // The ref wrote under `e/`; the land makes `e` a file.
        (
            edits(&[("e/new", Some("n\n"))]),
            edits(&[("e/one", None), ("e", Some("file\n"))]),
        ),
        // The ref and the land write different files of one directory.
        (
            edits(&[("d/three", Some("3\n"))]),
            edits(&[("d/one", None)]),
        ),
    ];
    for (i, (trunk, land)) in cases.into_iter().enumerate() {
        let p = Pair::new();
        let base = p
            .create(
                &[],
                &edits(&[
                    ("t", Some("1\n2\n3\n")),
                    ("d/one", Some("1\n")),
                    ("e/one", Some("1\n")),
                ]),
                "base",
            )
            .await;
        p.land(base).await;
        let theirs = p.create(&[base], &land, "land").await;
        let ours = p.create(&[base], &trunk, "trunk").await;
        p.land(ours).await;
        let answer = p.land(theirs).await;
        let looked = lookups(&p.fast);
        if i == 4 {
            assert_eq!(looked, vec![Lookup::Hit], "case {i}: {answer}");
        } else {
            assert_eq!(looked, vec![Lookup::Overlap], "case {i}: {answer}");
        }
        p.head().await;
        p.assert_same_logs().await;
    }
}

/// A cold map (a new leader after a restart or failover), a ref moved by
/// another server, and a base older than the map's window all replay the
/// slow way, with the slow path's answers; the map warms again after.
#[tokio::test]
async fn a_cold_map_or_a_base_out_of_the_window_replays_the_slow_way() {
    let p = Pair::new();
    let base = p
        .create(&[], &edits(&[("seed", Some("s\n"))]), "base")
        .await;
    p.land(base).await;
    let early = p
        .create(&[base], &edits(&[("early", Some("e\n"))]), "early")
        .await;
    // Past the window: more trunk changesets than the map holds.
    for i in 0..crate::land_fast::MAP_TIPS + 2 {
        let head = p.head().await;
        let c = p
            .create(
                &[head],
                &edits(&[(format!("t/{}", i % 7).as_str(), Some(&format!("{i}\n")))]),
                &format!("t{i}"),
            )
            .await;
        // Replayed, not fast-forwarded, so the map follows its own copies.
        let side = p
            .create(
                &[c],
                &edits(&[(format!("s/{i}").as_str(), Some("s\n"))]),
                &format!("s{i}"),
            )
            .await;
        p.land(side).await;
        p.land(c).await;
    }
    p.land(early).await;
    assert_eq!(lookups(&p.fast).last(), Some(&Lookup::Window));

    // A fresh handle on the same stores is a new leader: its map is cold,
    // and knows nothing before the head its first batch reads.
    let head = p.head().await;
    let stale = p
        .create(&[head], &edits(&[("cold/a", Some("a\n"))]), "stale")
        .await;
    let next = p
        .create(&[head], &edits(&[("cold/b", Some("b\n"))]), "next")
        .await;
    p.land(next).await;
    let restarted = Repo::new(Arc::clone(p.fast.blobs()), Arc::clone(p.fast.meta()));
    let p = Pair::with(restarted, p.slow);
    p.land(stale).await;
    assert_eq!(lookups(&p.fast), vec![Lookup::Cold]);

    // Another server moves the ref: the map no longer describes the head.
    let head = p.head().await;
    let mine = p
        .create(&[head], &edits(&[("warm/a", Some("a\n"))]), "mine")
        .await;
    let theirs = p
        .create(&[head], &edits(&[("warm/b", Some("b\n"))]), "theirs")
        .await;
    for r in [&p.fast, &p.slow] {
        assert!(r
            .meta()
            .cas_ref("main", Some(head), Some(theirs))
            .await
            .unwrap());
    }
    p.land(mine).await;
    assert_eq!(lookups(&p.fast).last(), Some(&Lookup::Cold));

    // Warm again: a land based on the head the map now knows.
    let head = p.head().await;
    let a = p
        .create(&[head], &edits(&[("warm/c", Some("c\n"))]), "a")
        .await;
    let b = p
        .create(&[head], &edits(&[("warm/d", Some("d\n"))]), "b")
        .await;
    p.land(a).await;
    p.land(b).await;
    assert_eq!(lookups(&p.fast).last(), Some(&Lookup::Hit));
    p.head().await;
    p.assert_same_logs().await;
}

/// The map stays within its bounds, however much the ref writes.
#[tokio::test]
async fn the_write_map_stays_bounded() {
    let r = plain_repo();
    let base = create(&r, &[], &edits(&[("x", Some("x\n"))]), "base").await;
    r.land(base, "main").await.unwrap();
    for i in 0..crate::land_fast::MAP_TIPS * 2 {
        let head = r.meta().get_ref("main").await.unwrap().unwrap();
        let many: Vec<(String, Option<&str>)> = (0..9)
            .map(|j| (format!("d{i}/e{j}/f"), Some("v\n")))
            .collect();
        let many: Vec<(&str, Option<&str>)> = many.iter().map(|(p, b)| (p.as_str(), *b)).collect();
        let c = create(&r, &[head], &edits(&many), &format!("c{i}")).await;
        r.land(c, "main").await.unwrap();
        let (tips, entries) = r.land_queues.write_map_size("main");
        assert!(tips <= crate::land_fast::MAP_TIPS, "{tips} tips");
        assert!(
            entries <= crate::land_fast::MAP_ENTRIES,
            "{entries} entries"
        );
    }
}

/// Random histories of agents landing stacks at once, some on old bases,
/// with mixed overlap: the same file, the same directory, a file against a
/// directory, deletes, renames as a delete and an add, no-op modifies, and
/// stacks on a landed copy of their own. Landed concurrently with the fast
/// path, and one at a time in the batches' order without it: the same
/// answers, heads, trees and ref logs, and never an error.
/// `LEEP_LAND_FAST_SEEDS` sets how many histories (40; 25 for strict).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fast_and_slow_lands_agree_on_random_histories() {
    let (mut hits, mut slow, mut conflicts) = (0, 0, 0);
    for seed in 1..=std::env::var("LEEP_LAND_FAST_SEEDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(40u64)
    {
        let (h, s, c) = differential(seed, false).await;
        hits += h;
        slow += s;
        conflicts += c;
    }
    assert!(hits > 100, "many lands took the fast path ({hits})");
    assert!(slow > 50, "many took the slow path ({slow})");
    assert!(conflicts > 10, "some conflicted ({conflicts})");
}

/// One random history, landed by both: (fast-path lands, slow-path lands,
/// conflicts).
pub(crate) async fn differential(seed: u64, strict: bool) -> (usize, usize, usize) {
    let mut rng = Rng::new(seed);
    let p = if strict {
        Pair::with(
            plain_repo().with_land_conflicts(crate::LandConflicts::strict()),
            plain_repo()
                .with_land_fast_path(false)
                .with_land_conflicts(crate::LandConflicts::strict()),
        )
    } else {
        Pair::new()
    };
    let base_edits = random_edits(&mut rng, &Files::new(), 10, "");
    let base_edits: Edits = base_edits
        .into_iter()
        .filter(|(_, v)| v.is_some())
        .collect();
    let base = p.create(&[], &base_edits, "base").await;
    p.land(base).await;
    // Unlanded stacks: tip, the head they were built on (`None` for one on
    // a landed copy), and the files of their tip.
    let mut pending: Vec<(ChangesetId, Option<ChangesetId>)> = Vec::new();
    let mut seen_batches = p.fast.land_queues.batches.lock().unwrap().len();
    let mut conflicts = 0;
    let mut agent = 0;
    for round in 0..10 {
        let head = p.head().await;
        let files = files_of(&p.fast, head).await;
        for _ in 0..1 + rng.below(5) {
            agent += 1;
            let own = format!("p{}", rng.below(5));
            let mut parent = head;
            let mut have = files.clone();
            for d in 0..1 + rng.below(3) {
                let ops = 1 + rng.below(3);
                let e = random_edits(&mut rng, &have, ops, &own);
                have = apply(&have, &e);
                parent = p
                    .create(
                        &[parent],
                        &e,
                        &format!("seed {seed} agent {agent} step {d}"),
                    )
                    .await;
            }
            pending.push((parent, Some(head)));
        }
        // Land some of what is pending, older stacks among them, at once.
        let mut now = Vec::new();
        pending.retain(|&(cs, base)| {
            if rng.below(3) != 0 || round == 9 {
                now.push((cs, base));
                false
            } else {
                true
            }
        });
        let tasks: Vec<_> = now
            .iter()
            .map(|&(cs, _)| {
                let r = p.fast.clone();
                let yields = rng.below(4);
                tokio::spawn(async move {
                    for _ in 0..yields {
                        tokio::task::yield_now().await;
                    }
                    (cs, outcome(&r.land(cs, "main").await))
                })
            })
            .collect();
        let mut got = HashMap::new();
        for t in tasks {
            let (cs, answer) = t.await.unwrap();
            got.insert(cs, answer);
        }
        let order: Vec<ChangesetId> = {
            let b = p.fast.land_queues.batches.lock().unwrap();
            let order = b[seen_batches..].iter().flatten().copied().collect();
            seen_batches = b.len();
            order
        };
        assert_eq!(
            order.len(),
            now.len(),
            "seed {seed}: every land batched once"
        );
        let bases: HashMap<ChangesetId, Option<ChangesetId>> = now.iter().copied().collect();
        for cs in order {
            let before = p.slow.meta().get_ref("main").await.unwrap().unwrap();
            let want = outcome(&p.slow.land(cs, "main").await);
            assert_eq!(got[&cs], want, "seed {seed} round {round}: land of {cs}");
            assert!(!want.starts_with("error"), "seed {seed}: {want}");
            if want.starts_with("Conflicts") {
                conflicts += 1;
            }
            if strict {
                if let Some(Some(fork)) = bases.get(&cs) {
                    check_strict(&p.slow, *fork, before, cs, &want).await;
                }
            }
            // Now and then the agent builds on its own landed changeset.
            if want.contains("rebased: true") && rng.below(4) == 0 {
                let have = files_of(&p.fast, cs).await;
                let e = random_edits(&mut rng, &have, 2, "q");
                agent += 1;
                let child = p
                    .create(&[cs], &e, &format!("seed {seed} agent {agent} on a copy"))
                    .await;
                pending.push((child, None));
            }
        }
    }
    p.head().await;
    p.assert_same_logs().await;
    let looked = lookups(&p.fast);
    let hits = looked.iter().filter(|l| **l == Lookup::Hit).count();
    (hits, looked.len() - hits, conflicts)
}

/// Under the strict policy a land conflicts exactly when it changes a path
/// the ref changed since its base (or clashes as a file against a
/// directory, which conflicts under either policy).
async fn check_strict(
    r: &Repo,
    fork: ChangesetId,
    head: ChangesetId,
    cs: ChangesetId,
    answer: &str,
) {
    let tree = |id| r.root_tree(id);
    let (f, h, c) = (
        tree(fork).await.unwrap(),
        tree(head).await.unwrap(),
        tree(cs).await.unwrap(),
    );
    let ours: BTreeSet<RepoPath> = r
        .diff_trees(f, h, "")
        .await
        .unwrap()
        .into_iter()
        .map(|e| e.path)
        .collect();
    let theirs: BTreeSet<RepoPath> = r
        .diff_trees(f, c, "")
        .await
        .unwrap()
        .into_iter()
        .map(|e| e.path)
        .collect();
    let overlap: Vec<&RepoPath> = ours.intersection(&theirs).collect();
    if overlap.is_empty() {
        if answer.starts_with("Conflicts") {
            // Only file/directory clashes, never a path both sides changed.
            for path in conflict_paths(answer) {
                assert!(
                    !ours.contains(&path),
                    "{path} conflicted without an overlap"
                );
            }
        }
        return;
    }
    assert!(
        answer.starts_with("Conflicts"),
        "{overlap:?} overlap: {answer}"
    );
    let paths = conflict_paths(answer);
    for p in overlap {
        assert!(
            paths.contains(p),
            "{p} overlaps and is not a conflict: {answer}"
        );
    }
}

/// The paths of a `Conflicts` answer, from its debug form.
fn conflict_paths(answer: &str) -> BTreeSet<RepoPath> {
    answer
        .split("Conflict { path: \"")
        .skip(1)
        .map(|s| RepoPath::new(&s[..s.find('"').unwrap()]).unwrap())
        .collect()
}

/// Approves a changeset only when every path is approved or covered, the
/// way a review-protected ref decides (ADR 0011), including the paths a
/// merge drops from a side (#215).
struct Reviewed;

#[async_trait]
impl LandPolicy for Reviewed {
    fn applies_to(&self, target_ref: &str) -> bool {
        target_ref == "main"
    }

    fn depends_on_head(&self) -> bool {
        true
    }

    async fn check_land(
        &self,
        repo: &Repo,
        _: &str,
        head: Option<ChangesetId>,
        new_history: &[ChangesetId],
    ) -> Result<()> {
        for &id in new_history {
            let reviews: Vec<Review> = repo.get_review(id, "alice").await?.into_iter().collect();
            let mut owners = OwnersResolver::new(repo, head).await?;
            let s = repo
                .review_changeset(id, &ReviewRules::default(), &reviews, &mut owners)
                .await?;
            if !s.approved() {
                return Err(Error::Invalid(format!("{id} is not reviewed")));
            }
        }
        Ok(())
    }
}

async fn approve(p: &Pair, id: ChangesetId) {
    let review = Review {
        reviewer: "alice".into(),
        verdict: Verdict::Approve,
        comment: String::new(),
        timestamp: 1,
    };
    for r in [&p.fast, &p.slow] {
        r.put_review(id, &review).await.unwrap();
    }
}

/// The review policy judges every land the fast path copies exactly as it
/// judged it before: an unreviewed member of a disjoint stack refuses the
/// land, and so does an empty merge that drops the ref's change (#215).
#[tokio::test]
async fn review_still_judges_every_land_the_fast_path_takes() {
    let policy: Arc<dyn LandPolicy> = Arc::new(Reviewed);
    let p = Pair::with(
        plain_repo().with_land_policy(Arc::clone(&policy)),
        plain_repo()
            .with_land_fast_path(false)
            .with_land_policy(policy),
    );
    let base = p
        .create(
            &[],
            &edits(&[("a.rs", Some("old\n")), ("b/x", Some("x\n"))]),
            "base",
        )
        .await;
    approve(&p, base).await;
    p.land(base).await;
    let s1 = p
        .create(&[base], &edits(&[("b/y", Some("y\n"))]), "s1")
        .await;
    let s2 = p.create(&[s1], &edits(&[("b/z", Some("z\n"))]), "s2").await;
    approve(&p, s2).await;
    let fix = p
        .create(&[base], &edits(&[("a.rs", Some("fixed\n"))]), "fix")
        .await;
    approve(&p, fix).await;
    p.land(fix).await;
    // s1 is not reviewed: the whole stack is refused, fast path or not.
    let refused = p.land(s2).await;
    assert!(refused.contains("is not reviewed"), "{refused}");
    approve(&p, s1).await;
    let landed = p.land(s2).await;
    assert!(landed.contains("rebased: true"), "{landed}");
    assert_eq!(lookups(&p.fast), vec![Lookup::Hit]);
    // An empty merge of the old base with the head as its side: a
    // fast-forward whose tree drops `fix`. Approved as a changeset, it
    // still needs a review of the path it drops.
    let head = p.head().await;
    let m = p.create(&[base, head], &Edits::new(), "m").await;
    let refused = p.land(m).await;
    assert!(refused.contains("is not reviewed"), "{refused}");
    p.head().await;
    p.assert_same_logs().await;
}

/// Land ids keep keyed retries idempotent, and the landed-copy scan still
/// runs for every land, keyed or not: a keyed land of a changeset another
/// land already put on the ref records nothing to answer it from, and only
/// the scan keeps it from landing a second, empty copy (ADR 0057).
#[tokio::test]
async fn retries_are_answered_once_with_or_without_land_ids() {
    let p = Pair::new();
    let base = p.create(&[], &edits(&[("x", Some("x\n"))]), "base").await;
    p.land(base).await;
    let a = p.create(&[base], &edits(&[("a", Some("a\n"))]), "a").await;
    let b = p.create(&[base], &edits(&[("b", Some("b\n"))]), "b").await;
    let c = p.create(&[base], &edits(&[("c", Some("c\n"))]), "c").await;
    p.land(a).await;
    let id = |n: u8| LandId::from_bytes(&[n; 16]).unwrap();
    for r in [&p.fast, &p.slow] {
        let first = r.land_keyed(b, "main", None, true, Some(id(1))).await;
        let head = r.meta().get_ref("main").await.unwrap();
        // The resend under the same id, from the record.
        let again = r.land_keyed(b, "main", None, true, Some(id(1))).await;
        assert_eq!(outcome(&first), outcome(&again));
        // An unkeyed resend, from the scan.
        let unkeyed = r.land(b, "main").await;
        assert_eq!(outcome(&first), outcome(&unkeyed));
        // A new id for a changeset already landed: from the scan too.
        let fresh = r.land_keyed(b, "main", None, true, Some(id(2))).await;
        assert_eq!(outcome(&first), outcome(&fresh));
        assert_eq!(
            r.meta().get_ref("main").await.unwrap(),
            head,
            "nothing moved"
        );
        // And an unkeyed land, then its keyed retry.
        let plain = r.land(c, "main").await;
        let keyed = r.land_keyed(c, "main", None, true, Some(id(3))).await;
        assert_eq!(outcome(&plain), outcome(&keyed));
    }
    p.head().await;
    p.assert_same_logs().await;
}

/// A batch attempt builds each land's tree on the nodes the land before it
/// wrote, from memory: the same root as building from the stores, and the
/// nodes it wrote are served decoded without a read.
#[tokio::test]
async fn a_batch_builds_trees_from_the_nodes_it_holds() {
    let r = plain_repo();
    let base = create(
        &r,
        &[],
        &edits(&[("a/b/c", Some("1\n")), ("d", Some("d\n"))]),
        "base",
    )
    .await;
    let staging = Arc::new(crate::land_stage::Staging::default());
    let staged = staging.repo(&r);
    let tree = r.root_tree(base).await.unwrap();
    let mut changes = BTreeMap::new();
    changes.insert(
        RepoPath::new("a/b/e").unwrap(),
        FileChange::Modify {
            content: r.put_blob(Bytes::from_static(b"e\n")).await.unwrap(),
            kind: FileKind::Regular,
            size: 2,
        },
    );
    let built = crate::tree_ops::apply_changes(&staged, tree, &changes)
        .await
        .unwrap();
    let want = crate::tree_ops::apply_changes(&r, tree, &changes)
        .await
        .unwrap();
    assert_eq!(built, want);
    let memo = staged.node_memo.as_ref().unwrap();
    let root = memo.get(&built).expect("the new root is held decoded");
    assert_eq!(root.hash(), built);
    assert_eq!(staged.read_tree(built).await.unwrap(), root);
    assert!(staging.tree_time() > std::time::Duration::ZERO);
    // A plain repository holds nothing between calls.
    assert!(r.node_memo.is_none());
}

/// The strict policy lands disjoint lands the fast way and answers every
/// same-path overlap with a conflict, the same with and without the fast
/// path.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_strict_policy_conflicts_exactly_on_same_path_overlaps() {
    let (mut hits, mut conflicts) = (0, 0);
    for seed in 1..=std::env::var("LEEP_LAND_FAST_SEEDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(25u64)
    {
        let (h, _, c) = differential(seed, true).await;
        hits += h;
        conflicts += c;
    }
    assert!(hits > 50, "disjoint lands took the fast path ({hits})");
    assert!(conflicts > 20, "overlaps conflicted ({conflicts})");
}

/// One same-file change that merges cleanly: landed under `merge`, refused
/// under `strict`, and only on the refs `strict` names.
#[tokio::test]
async fn strict_refs_refuse_what_merge_refs_merge() {
    let conflicts = crate::LandConflicts::new(crate::ConflictPolicy::Merge, ["prod".to_string()]);
    let r = plain_repo().with_land_conflicts(conflicts);
    let base = create(&r, &[], &edits(&[("t", Some("1\n2\n3\n"))]), "base").await;
    for name in ["main", "prod"] {
        r.land(base, name).await.unwrap();
    }
    let ours = create(&r, &[base], &edits(&[("t", Some("1\n2\nX\n"))]), "ours").await;
    let theirs = create(&r, &[base], &edits(&[("t", Some("Y\n2\n3\n"))]), "theirs").await;
    for name in ["main", "prod"] {
        r.land(ours, name).await.unwrap();
    }
    let merged = r.land(theirs, "main").await.unwrap();
    assert!(
        matches!(merged, LandResponse::Landed { rebased: true, .. }),
        "{merged:?}"
    );
    let refused = r.land(theirs, "prod").await.unwrap();
    let LandResponse::Conflicts { conflicts, .. } = refused else {
        panic!("{refused:?}");
    };
    assert_eq!(conflicts.len(), 1);
    assert_eq!(conflicts[0].path.as_str(), "t");
    assert_eq!(conflicts[0].merged_with_markers, None);
    assert_eq!(
        crate::ConflictPolicy::parse("strict"),
        Some(crate::ConflictPolicy::Strict)
    );
    assert_eq!(crate::ConflictPolicy::parse("rebase"), None);
}

/// A land that replaces a directory with a file conflicts, instead of
/// failing with 400, when the ref added files to that directory since the
/// land's base: deleting the files the land knew of does not empty it.
#[tokio::test]
async fn a_file_over_a_directory_the_ref_grew_is_a_conflict() {
    let p = Pair::new();
    let base = p.create(&[], &edits(&[("d/x", Some("x\n"))]), "base").await;
    p.land(base).await;
    let theirs = p
        .create(
            &[base],
            &edits(&[("d/x", None), ("d", Some("f\n"))]),
            "theirs",
        )
        .await;
    let ours = p
        .create(&[base], &edits(&[("d/y", Some("y\n"))]), "ours")
        .await;
    p.land(ours).await;
    let answer = p.land(theirs).await;
    assert!(answer.starts_with("Conflicts"), "{answer}");
    assert_eq!(
        conflict_paths(&answer),
        BTreeSet::from([RepoPath::new("d").unwrap()])
    );
    // Without the new file it lands, a directory swapped for a file.
    let p = Pair::new();
    let base = p.create(&[], &edits(&[("d/x", Some("x\n"))]), "base").await;
    p.land(base).await;
    let theirs = p
        .create(
            &[base],
            &edits(&[("d/x", None), ("d", Some("f\n"))]),
            "theirs",
        )
        .await;
    let ours = p
        .create(&[base], &edits(&[("e", Some("e\n"))]), "ours")
        .await;
    p.land(ours).await;
    let answer = p.land(theirs).await;
    assert!(answer.contains("rebased: true"), "{answer}");
}

/// Counts the reads that reach a blob store.
struct Counting {
    inner: MemoryBlobStore,
    gets: std::sync::atomic::AtomicUsize,
    batches: std::sync::atomic::AtomicUsize,
}

#[async_trait]
impl leep_core::BlobStore for Counting {
    async fn put_with_hash(&self, hash: leep_core::Hash, data: Bytes) -> Result<()> {
        self.inner.put_with_hash(hash, data).await
    }
    async fn get(&self, hash: &leep_core::Hash) -> Result<Option<Bytes>> {
        self.gets.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inner.get(hash).await
    }
    async fn get_many(&self, hashes: &[leep_core::Hash]) -> Result<Vec<Option<Bytes>>> {
        self.batches
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let mut out = Vec::new();
        for h in hashes {
            out.push(self.inner.get(h).await?);
        }
        Ok(out)
    }
    async fn list_page(
        &self,
        after: Option<leep_core::Hash>,
        limit: usize,
    ) -> Result<Vec<leep_core::store::BlobMeta>> {
        self.inner.list_page(after, limit).await
    }
    async fn delete(&self, hash: &leep_core::Hash) -> Result<()> {
        self.inner.delete(hash).await
    }
}

/// Tree nodes are read from the store once and then served decoded, and a
/// staged batch reads the nodes it did not write in one batched read.
#[tokio::test]
async fn tree_nodes_are_read_once_and_in_batches() {
    use std::sync::atomic::Ordering::SeqCst;
    let blobs = Arc::new(Counting {
        inner: MemoryBlobStore::new(),
        gets: Default::default(),
        batches: Default::default(),
    });
    let r = Repo::new(blobs.clone(), Arc::new(MemoryMetaStore::new()));
    let base = create(
        &r,
        &[],
        &edits(&[
            ("a/x", Some("1\n")),
            ("b/x", Some("2\n")),
            ("c/x", Some("3\n")),
        ]),
        "base",
    )
    .await;
    let root = r.root_tree(base).await.unwrap();
    // A fresh handle has nothing cached.
    let r = Repo::new(blobs.clone(), Arc::clone(r.meta()));
    let dirs: Vec<leep_core::Hash> = r
        .read_tree(root)
        .await
        .unwrap()
        .entries
        .values()
        .map(|e| match e {
            leep_core::TreeEntry::Dir { tree } => *tree,
            _ => panic!("a directory"),
        })
        .collect();
    let staging = Arc::new(crate::land_stage::Staging::default());
    let staged = staging.repo(&r);
    let (gets, batches) = (blobs.gets.load(SeqCst), blobs.batches.load(SeqCst));
    staged.read_trees(&dirs).await.unwrap();
    assert_eq!(blobs.batches.load(SeqCst), batches + 1, "one batched read");
    assert_eq!(blobs.gets.load(SeqCst), gets, "no single reads");
    // Read again, through either handle: from memory.
    staged.read_trees(&dirs).await.unwrap();
    r.read_trees(&dirs).await.unwrap();
    r.read_tree(root).await.unwrap();
    assert_eq!(blobs.batches.load(SeqCst), batches + 1);
    assert_eq!(blobs.gets.load(SeqCst), gets);
}
