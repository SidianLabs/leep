//! `leep bisect`: the state kept in `.leep/bisect.json` and the choice of
//! the next changeset to test.
//!
//! The candidates are the ancestors of the bad changeset that are not
//! ancestors of any good one, as in git. History lives on the server, so they
//! are found by reading logs, which list ancestors newest first by
//! generation: the bad changeset's log is read only until every parent of a
//! candidate has been seen, and each good changeset's log only down to the
//! generation reached, to tell which of those parents it already contains.

use std::collections::{HashMap, HashSet, VecDeque};
use std::fs;
use std::path::{Path, PathBuf};

use leep_core::ChangesetId;
use serde::{Deserialize, Serialize};

use crate::error::{CliError, Result};
use crate::state::{atomic_write, DIR};

/// Most candidates one bisect step will hold; more means the good changeset
/// is too far back, and a newer one narrows the search.
pub const MAX_CANDIDATES: usize = 100_000;
/// Most log entries one step reads, over all logs, so a good changeset on a
/// long unrelated history cannot make a step read without end.
pub const MAX_SCANNED: usize = 1_000_000;
/// Up to this many candidates every one is weighed exactly; above it, only
/// the [`SAMPLE`] nearest the middle of the generation order.
const EXACT_LIMIT: usize = 2_048;
const SAMPLE: usize = 256;
/// First log page size; each later page of the same log doubles, up to the
/// client's page cap, so a short walk asks for little.
const FIRST_PAGE: u32 = 128;

const FILE: &str = "bisect.json";

/// What a bisect knows, saved in `.leep/bisect.json` while one runs. Not an
/// API: only this CLI reads it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BisectState {
    /// The base when the bisect started; `bisect reset` returns to it.
    pub original_base: ChangesetId,
    /// The target ref when the bisect started; `bisect reset` restores it.
    pub original_ref: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bad: Option<ChangesetId>,
    #[serde(default)]
    pub good: Vec<ChangesetId>,
    #[serde(default)]
    pub skip: Vec<ChangesetId>,
    /// Every mark, oldest first, for `bisect log`.
    #[serde(default)]
    pub log: Vec<LogEntry>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogEntry {
    pub op: Mark,
    pub id: ChangesetId,
    /// First line of the changeset's message, when it was marked.
    #[serde(default)]
    pub subject: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Mark {
    Bad,
    Good,
    Skip,
    /// Not a mark: the answer, logged once it is known.
    FirstBad,
}

impl Mark {
    pub fn name(self) -> &'static str {
        match self {
            Mark::Bad => "bad",
            Mark::Good => "good",
            Mark::Skip => "skip",
            Mark::FirstBad => "first bad changeset",
        }
    }
}

fn path(root: &Path) -> PathBuf {
    root.join(DIR).join(FILE)
}

/// Whether a bisect is in progress in the working copy at `root`.
pub fn in_progress(root: &Path) -> bool {
    path(root).exists()
}

impl BisectState {
    pub fn load(root: &Path) -> Result<Option<Self>> {
        match fs::read(path(root)) {
            Ok(b) => serde_json::from_slice(&b)
                .map(Some)
                .map_err(|e| CliError::user(format!("bad .leep/{FILE}: {e}"))),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(CliError::user(format!("cannot read .leep/{FILE}: {e}"))),
        }
    }

    pub fn save(&self, root: &Path) -> Result<()> {
        atomic_write(&path(root), &serde_json::to_vec_pretty(self)?)
    }

    pub fn remove(root: &Path) -> Result<()> {
        match fs::remove_file(path(root)) {
            Err(e) if e.kind() != std::io::ErrorKind::NotFound => Err(e.into()),
            _ => Ok(()),
        }
    }

    /// Record a mark. A changeset has one mark: marking it again replaces
    /// the old one.
    pub fn mark(&mut self, op: Mark, id: ChangesetId, subject: String) -> Result<()> {
        match op {
            Mark::Bad => {
                if self.good.contains(&id) {
                    return Err(already(id, "good"));
                }
                self.bad = Some(id);
                self.skip.retain(|s| *s != id);
            }
            Mark::Good => {
                if self.bad == Some(id) {
                    return Err(already(id, "bad"));
                }
                if !self.good.contains(&id) {
                    self.good.push(id);
                }
                self.skip.retain(|s| *s != id);
            }
            Mark::Skip => {
                if self.bad == Some(id) || self.good.contains(&id) {
                    return Err(already(
                        id,
                        if self.bad == Some(id) { "bad" } else { "good" },
                    ));
                }
                if !self.skip.contains(&id) {
                    self.skip.push(id);
                }
            }
            Mark::FirstBad => {}
        }
        self.log.push(LogEntry { op, id, subject });
        Ok(())
    }
}

fn already(id: ChangesetId, what: &str) -> CliError {
    CliError::user(format!(
        "{} is already marked {what}; `leep bisect log` shows the marks, \
         `leep bisect reset` starts over",
        crate::fmt::short(id)
    ))
}

/// A changeset as a log lists it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Node {
    pub id: ChangesetId,
    pub parents: Vec<ChangesetId>,
    pub generation: u64,
}

/// Where the history comes from: the server, or a table in tests.
pub trait History {
    /// One page of the log of `from` (ancestors, `from` first, newest first
    /// by generation), after the cursor `after`.
    fn page(
        &mut self,
        from: &ChangesetId,
        after: Option<&str>,
        limit: u32,
    ) -> Result<(Vec<Node>, Option<String>)>;
    fn generation(&mut self, id: &ChangesetId) -> Result<u64>;
}

/// One log, read a page at a time as the walk needs it.
struct Walk {
    from: ChangesetId,
    buf: VecDeque<Node>,
    next: Option<String>,
    ended: bool,
    limit: u32,
    last_gen: u64,
}

impl Walk {
    fn new(from: ChangesetId) -> Self {
        Walk {
            from,
            buf: VecDeque::new(),
            next: None,
            ended: false,
            limit: FIRST_PAGE,
            last_gen: u64::MAX,
        }
    }

    fn peek(&mut self, h: &mut dyn History, scanned: &mut usize) -> Result<Option<&Node>> {
        while self.buf.is_empty() && !self.ended {
            let (entries, next) = h.page(&self.from, self.next.as_deref(), self.limit)?;
            *scanned += entries.len();
            if *scanned > MAX_SCANNED {
                return Err(CliError::user(format!(
                    "bisecting would read more than {MAX_SCANNED} log entries; \
                     mark a newer good changeset"
                )));
            }
            for e in &entries {
                if e.generation > self.last_gen {
                    return Err(CliError::server(
                        "the server's log is not ordered by generation; \
                         bisect needs a newer server",
                    ));
                }
                self.last_gen = e.generation;
            }
            self.limit = (self.limit * 2).min(crate::client::LOG_PAGE);
            self.ended = next.is_none() || entries.is_empty();
            self.next = next;
            self.buf.extend(entries);
        }
        Ok(self.buf.front())
    }

    fn pop(&mut self, h: &mut dyn History, scanned: &mut usize) -> Result<Option<Node>> {
        self.peek(h, scanned)?;
        Ok(self.buf.pop_front())
    }
}

/// The candidates of one step: `nodes[0]` is the bad changeset, the rest
/// follow newest first by generation, so every parent comes after its
/// children. `parents` holds indices into `nodes`, only for parents that are
/// candidates too.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Candidates {
    pub nodes: Vec<ChangesetId>,
    pub parents: Vec<Vec<usize>>,
    /// Log entries read to find them.
    pub scanned: usize,
}

/// Ancestors of `bad` (itself included) that are not ancestors of any of
/// `goods`. Iterative: histories are deep.
pub fn candidates(
    h: &mut dyn History,
    bad: ChangesetId,
    goods: &[ChangesetId],
) -> Result<Candidates> {
    let mut scanned = 0;
    let mut good_walks = Vec::new();
    for g in goods {
        good_walks.push((h.generation(g)?, Walk::new(*g)));
    }
    let mut in_good: HashSet<ChangesetId> = HashSet::new();
    let mut walk = Walk::new(bad);
    // Parents of candidates not met yet; each is a candidate unless a good
    // changeset has it. A candidate's children on the way from `bad` are all
    // candidates, so nothing outside this set needs a look.
    let mut pending: HashSet<ChangesetId> = HashSet::from([bad]);
    let mut nodes = Vec::new();
    let mut raw_parents = Vec::new();
    while !pending.is_empty() {
        let Some(n) = walk.pop(h, &mut scanned)? else {
            break;
        };
        if !pending.remove(&n.id) {
            continue;
        }
        // A good changeset's ancestor at this generation is in its log
        // before anything older.
        for (gen, w) in &mut good_walks {
            if *gen < n.generation {
                continue;
            }
            while let Some(x) = w.peek(h, &mut scanned)? {
                if x.generation < n.generation {
                    break;
                }
                let id = x.id;
                w.buf.pop_front();
                in_good.insert(id);
            }
        }
        if in_good.contains(&n.id) {
            continue;
        }
        if nodes.len() >= MAX_CANDIDATES {
            return Err(CliError::user(format!(
                "more than {MAX_CANDIDATES} changesets to bisect; mark a newer good changeset"
            )));
        }
        pending.extend(n.parents.iter().copied());
        nodes.push(n.id);
        raw_parents.push(n.parents);
    }
    let index: HashMap<ChangesetId, usize> =
        nodes.iter().enumerate().map(|(i, id)| (*id, i)).collect();
    let parents = raw_parents
        .iter()
        .map(|ps| ps.iter().filter_map(|p| index.get(p).copied()).collect())
        .collect();
    Ok(Candidates {
        nodes,
        parents,
        scanned,
    })
}

/// What to do next.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Next {
    /// Test `id`. `remaining` is how many would be left to test after it, at
    /// worst; `steps` roughly how many tests that takes.
    Test {
        id: ChangesetId,
        remaining: usize,
        steps: u32,
    },
    /// The first bad changeset.
    Found(ChangesetId),
    /// Only skipped changesets are left: the first bad one is one of these
    /// (the skipped ones, then the bad one).
    OnlySkipped(Vec<ChangesetId>),
}

/// Pick the candidate that best halves the set: the one maximising
/// `min(reach, n - reach)`, where `reach` counts the candidates it has in
/// its history, itself included (git's bisection weight). Skipped
/// changesets count but are never picked.
pub fn choose(c: &Candidates, skip: &HashSet<ChangesetId>) -> Next {
    let n = c.nodes.len();
    let bad = c.nodes[0];
    let testable: Vec<usize> = (1..n).filter(|i| !skip.contains(&c.nodes[*i])).collect();
    if testable.is_empty() {
        let mut left: Vec<ChangesetId> = c.nodes[1..].to_vec();
        if left.is_empty() {
            return Next::Found(bad);
        }
        left.push(bad);
        return Next::OnlySkipped(left);
    }
    let weigh: Vec<usize> = if n <= EXACT_LIMIT {
        testable
    } else {
        let mid = n / 2;
        let mut near = testable;
        near.sort_by_key(|i| (i.abs_diff(mid), *i));
        near.truncate(SAMPLE);
        near.sort_unstable();
        near
    };
    let mut seen = vec![0u32; n];
    let mut stamp = 0;
    let mut queue = Vec::new();
    let mut best: Option<(usize, usize)> = None; // (score, index)
    let mut best_reach = 0;
    for &i in &weigh {
        stamp += 1;
        queue.clear();
        queue.push(i);
        seen[i] = stamp;
        let mut reach = 0;
        while let Some(x) = queue.pop() {
            reach += 1;
            for &p in &c.parents[x] {
                if seen[p] != stamp {
                    seen[p] = stamp;
                    queue.push(p);
                }
            }
        }
        let score = reach.min(n - reach);
        if best.is_none_or(|(s, _)| score > s) {
            best = Some((score, i));
            best_reach = reach;
        }
    }
    let (_, i) = best.expect("at least one candidate weighed");
    let skipped = c.nodes[1..].iter().filter(|id| skip.contains(id)).count();
    let remaining = (best_reach - 1).max(n - best_reach - 1);
    Next::Test {
        id: c.nodes[i],
        remaining,
        steps: estimate_steps(n - skipped),
    }
}

/// git's `estimate_bisect_steps`: tests left for `all` candidates.
pub fn estimate_steps(all: usize) -> u32 {
    if all < 3 {
        return 0;
    }
    let n = usize::BITS - 1 - all.leading_zeros();
    let e = 1usize << n;
    let x = all - e;
    if e < 3 * x {
        n
    } else {
        n - 1
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use leep_core::Hash;

    fn id(i: usize) -> ChangesetId {
        ChangesetId(Hash::of_blob(format!("c{i}").as_bytes()))
    }

    /// A DAG given as parent lists; logs are served in pages of `page`.
    struct Table {
        parents: HashMap<ChangesetId, Vec<ChangesetId>>,
        gen: HashMap<ChangesetId, u64>,
        page: usize,
    }

    impl Table {
        fn new(edges: &[(usize, &[usize])]) -> Self {
            let mut t = Table {
                parents: HashMap::new(),
                gen: HashMap::new(),
                page: 3,
            };
            for (c, ps) in edges {
                let ps: Vec<_> = ps.iter().map(|p| id(*p)).collect();
                let g = ps.iter().map(|p| t.gen[p]).max().unwrap_or(0) + 1;
                t.gen.insert(id(*c), g);
                t.parents.insert(id(*c), ps);
            }
            t
        }

        fn log(&self, from: ChangesetId) -> Vec<Node> {
            let mut seen = HashSet::from([from]);
            let mut todo = vec![from];
            while let Some(c) = todo.pop() {
                for p in &self.parents[&c] {
                    if seen.insert(*p) {
                        todo.push(*p);
                    }
                }
            }
            let mut v: Vec<Node> = seen
                .into_iter()
                .map(|c| Node {
                    id: c,
                    parents: self.parents[&c].clone(),
                    generation: self.gen[&c],
                })
                .collect();
            v.sort_by(|a, b| b.generation.cmp(&a.generation).then(b.id.cmp(&a.id)));
            v
        }
    }

    impl History for Table {
        fn page(
            &mut self,
            from: &ChangesetId,
            after: Option<&str>,
            _limit: u32,
        ) -> Result<(Vec<Node>, Option<String>)> {
            let all = self.log(*from);
            let start: usize = after.map_or(0, |a| a.parse().unwrap());
            let end = (start + self.page).min(all.len());
            let next = (end < all.len()).then(|| end.to_string());
            Ok((all[start..end].to_vec(), next))
        }

        fn generation(&mut self, id: &ChangesetId) -> Result<u64> {
            Ok(self.gen[id])
        }
    }

    fn linear(n: usize) -> Table {
        let edges: Vec<(usize, Vec<usize>)> = (1..=n)
            .map(|i| (i, if i == 1 { vec![] } else { vec![i - 1] }))
            .collect();
        let refs: Vec<(usize, &[usize])> = edges.iter().map(|(c, p)| (*c, p.as_slice())).collect();
        Table::new(&refs)
    }

    /// Bisect with `is_bad`, returning the answer and the number of tests.
    fn bisect(
        t: &mut Table,
        bad: usize,
        good: usize,
        is_bad: impl Fn(usize) -> bool,
    ) -> (Next, usize) {
        let lookup = |c: ChangesetId| (0..1000).find(|i| id(*i) == c).unwrap();
        let (mut b, mut goods) = (id(bad), vec![id(good)]);
        let mut tests = 0;
        loop {
            let c = candidates(t, b, &goods).unwrap();
            match choose(&c, &HashSet::new()) {
                Next::Test { id: x, .. } => {
                    tests += 1;
                    if is_bad(lookup(x)) {
                        b = x;
                    } else {
                        goods.push(x);
                    }
                }
                other => return (other, tests),
            }
        }
    }

    #[test]
    fn linear_history_halves() {
        let mut t = linear(20);
        let c = candidates(&mut t, id(20), &[id(1)]).unwrap();
        assert_eq!(c.nodes.len(), 19);
        assert_eq!(c.nodes[0], id(20));
        // Stops reading the bad log at the good changeset, not at the root.
        assert!(c.scanned < 30, "{}", c.scanned);
        match choose(&c, &HashSet::new()) {
            Next::Test {
                id: x,
                remaining,
                steps,
            } => {
                assert!(x == id(10) || x == id(11), "{x}");
                assert_eq!(remaining, 9);
                assert_eq!(steps, 3);
            }
            other => panic!("{other:?}"),
        }
        let (found, tests) = bisect(&mut t, 20, 1, |i| i >= 13);
        assert_eq!(found, Next::Found(id(13)));
        assert!(tests <= 5, "{tests}");
    }

    #[test]
    fn merges_find_the_side_commit() {
        // 1 - 2 - 3 - 4 ------- 8 - 9
        //      \               /
        //       5 - 6 - 7 ----'      (6 is the culprit; 8 merges it)
        let mut t = Table::new(&[
            (1, &[]),
            (2, &[1]),
            (3, &[2]),
            (4, &[3]),
            (5, &[2]),
            (6, &[5]),
            (7, &[6]),
            (8, &[4, 7]),
            (9, &[8]),
        ]);
        let c = candidates(&mut t, id(9), &[id(1)]).unwrap();
        assert_eq!(c.nodes.len(), 8);
        let bad = |i: usize| (6..=9).contains(&i);
        assert_eq!(bisect(&mut t, 9, 1, bad).0, Next::Found(id(6)));
        // A good changeset on the side branch takes its history out.
        let c = candidates(&mut t, id(9), &[id(4), id(5)]).unwrap();
        let mut got: Vec<_> = c.nodes.clone();
        got.sort();
        let mut want = vec![id(6), id(7), id(8), id(9)];
        want.sort();
        assert_eq!(got, want);
    }

    #[test]
    fn skipped_neighbours_leave_a_choice() {
        let mut t = linear(5);
        let c = candidates(&mut t, id(4), &[id(2)]).unwrap();
        let skip = HashSet::from([id(3)]);
        assert_eq!(choose(&c, &skip), Next::OnlySkipped(vec![id(3), id(4)]));
        let c = candidates(&mut t, id(3), &[id(2)]).unwrap();
        assert_eq!(choose(&c, &HashSet::new()), Next::Found(id(3)));
        // A bad changeset a good one contains leaves nothing.
        let c = candidates(&mut t, id(2), &[id(4)]).unwrap();
        assert!(c.nodes.is_empty());
    }

    #[test]
    fn large_sets_sample_the_middle() {
        let mut t = linear(EXACT_LIMIT * 2);
        t.page = 10_000;
        let c = candidates(&mut t, id(EXACT_LIMIT * 2), &[id(1)]).unwrap();
        match choose(&c, &HashSet::new()) {
            Next::Test { id: x, .. } => {
                let i = (0..EXACT_LIMIT * 2 + 1).find(|i| id(*i) == x).unwrap();
                assert!(i.abs_diff(EXACT_LIMIT) <= 1, "{i}");
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn steps_match_git() {
        let got: Vec<u32> = [1, 2, 3, 4, 5, 7, 8, 9, 19, 100, 1024]
            .iter()
            .map(|n| estimate_steps(*n))
            .collect();
        assert_eq!(got, vec![0, 0, 1, 1, 1, 2, 2, 2, 3, 6, 9]);
    }

    #[test]
    fn marks_replace_each_other() {
        let mut s = BisectState {
            original_base: id(1),
            original_ref: "main".into(),
            bad: None,
            good: vec![],
            skip: vec![],
            log: vec![],
        };
        s.mark(Mark::Skip, id(3), String::new()).unwrap();
        s.mark(Mark::Good, id(3), String::new()).unwrap();
        assert!(s.skip.is_empty() && s.good == vec![id(3)]);
        assert!(s.mark(Mark::Bad, id(3), String::new()).is_err());
        s.mark(Mark::Bad, id(5), String::new()).unwrap();
        assert!(s.mark(Mark::Skip, id(5), String::new()).is_err());
        assert_eq!(s.log.len(), 3);
        let back: BisectState = serde_json::from_slice(&serde_json::to_vec(&s).unwrap()).unwrap();
        assert_eq!(back, s);
    }
}
