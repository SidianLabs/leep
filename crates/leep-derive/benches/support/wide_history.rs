//! A synthetic history shaped like late Linux, for timing path-history
//! derivation: wide directories of thousands of files, tens of thousands of
//! files in all, and subsystem branches that take topic merges, merge
//! mainline back into themselves and are merged into mainline, so many
//! merges carry hundreds of paths from their second parent. Deterministic for
//! a given [`Spec`], so two builds give identical changesets.
//!
//! Shared by the `repo_ops` bench and the `history_profile` example of
//! `leep-server` (included with `#[path]`).

#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet, HashMap};

use bytes::Bytes;
use leep_core::{Changeset, ChangesetId, FileChange, FileKind, RepoPath};
use leep_derive::Repo;

/// Shape of the history.
#[derive(Clone, Debug)]
pub struct Spec {
    /// Directories of files only, `wide/w{i}`, holding `wide_min +
    /// i * wide_step` files at the start.
    pub wide_dirs: usize,
    pub wide_min: usize,
    pub wide_step: usize,
    /// `deep/s{a}/d{b}` for `a < deep.0`, `b < deep.1`, `deep.2` files each.
    pub deep: (usize, usize, usize),
    /// Changesets after the initial import.
    pub commits: usize,
    /// Long-lived subsystem branches.
    pub branches: usize,
    pub seed: u64,
    /// `(back, commits)`: at the end, a branch of `commits` small changesets
    /// forked from mainline as it was `back` changesets before the end is
    /// merged into mainline, like Linux's `work.thaw` (2 commits on a base
    /// five months old). Its merge's second parent differs from the first
    /// in every path mainline changed since, though it brings in little.
    /// The branch also adds a file to `cherry/`, where mainline holds a file
    /// that a side branch forked at the import and mainline made identical
    /// changes to (a patch applied twice) after the fork, so the side
    /// branch's old change is still among mainline's newest for `cherry`.
    pub old_branch: Option<(usize, usize)>,
}

impl Spec {
    /// About 80,000 files, 12 directories of 2,000 to 5,000 files.
    pub fn linux_like(commits: usize) -> Self {
        Spec {
            wide_dirs: 12,
            wide_min: 2000,
            wide_step: 270,
            deep: (40, 20, 52),
            commits,
            branches: 8,
            seed: 0x5eed,
            old_branch: None,
        }
    }

    /// A small version for benches: about 8,000 files in directories of
    /// 1,000 to 2,000.
    pub fn small(commits: usize) -> Self {
        Spec {
            wide_dirs: 4,
            wide_min: 1000,
            wide_step: 330,
            deep: (10, 5, 30),
            commits,
            branches: 4,
            seed: 0x5eed,
            old_branch: None,
        }
    }

    /// Shaped like git/git's tree: about 4,300 files, a root-like directory
    /// of 450 and two of 725 and 1,000 (`Documentation/`, `t/`), and 48
    /// small directories.
    pub fn git_like(commits: usize) -> Self {
        Spec {
            wide_dirs: 3,
            wide_min: 450,
            wide_step: 275,
            deep: (8, 6, 45),
            commits,
            branches: 4,
            seed: 0x5eed,
            old_branch: None,
        }
    }

    /// This shape ending in a merge of an old-based branch (see
    /// [`Spec::old_branch`]).
    pub fn with_old_branch(mut self, back: usize, commits: usize) -> Self {
        self.old_branch = Some((back, commits));
        self
    }
}

struct Lcg(u64);

impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0 >> 33
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }

    fn chance(&mut self, percent: u64) -> bool {
        self.next() % 100 < percent
    }
}

/// File path -> version (0 means deleted).
type State = HashMap<String, u32>;

struct Line {
    tip: ChangesetId,
    state: State,
    /// Paths this line changed since it last merged into its parent line.
    dirty: BTreeSet<String>,
    /// How much of mainline's change log it has merged.
    synced: usize,
    /// Directories it mostly works in.
    home: Vec<usize>,
    commits: usize,
    merge_after: usize,
}

struct Gen<'a> {
    repo: &'a Repo,
    rng: Lcg,
    dirs: Vec<(String, Vec<String>)>,
    wide: usize,
    seq: u64,
    next_name: usize,
}

impl Gen<'_> {
    async fn content(&mut self, path: &str, version: u32) -> FileChange {
        let data = format!("{path} v{version}\n");
        let content = self
            .repo
            .blobs()
            .put(Bytes::from(data.clone().into_bytes()))
            .await
            .unwrap();
        FileChange::Modify {
            content,
            kind: FileKind::Regular,
            size: data.len() as u64,
        }
    }

    async fn commit(
        &mut self,
        parents: Vec<ChangesetId>,
        updates: BTreeMap<String, u32>,
    ) -> ChangesetId {
        let mut changes = BTreeMap::new();
        for (path, v) in updates {
            let change = if v == 0 {
                FileChange::Delete
            } else {
                self.content(&path, v).await
            };
            changes.insert(RepoPath::new(path).unwrap(), change);
        }
        self.seq += 1;
        let cs = Changeset {
            parents,
            author: format!("dev{}", self.seq % 97),
            timestamp: 1_100_000_000 + self.seq as i64 * 600,
            message: format!("change {}", self.seq),
            changes,
            extra: BTreeMap::new(),
        };
        self.repo.create_changeset(cs).await.unwrap()
    }

    /// A random path for an edit on a line working in `home`: an existing
    /// file, sometimes a new one, sometimes a deletion.
    fn pick(&mut self, home: &[usize], state: &State) -> (String, u32) {
        let d = if self.rng.chance(85) {
            home[self.rng.below(home.len())]
        } else {
            self.rng.below(self.dirs.len())
        };
        if self.rng.chance(12) {
            self.next_name += 1;
            let name = format!("n{:06}.c", self.next_name * 7919 % 1_000_000);
            let path = format!("{}/{name}", self.dirs[d].0);
            self.dirs[d].1.push(name);
            return (path, 1);
        }
        let files = &self.dirs[d].1;
        let path = format!("{}/{}", self.dirs[d].0, files[self.rng.below(files.len())]);
        let v = state.get(&path).copied().unwrap_or(0);
        if v != 0 && self.rng.chance(3) {
            return (path, 0);
        }
        (path, v + 1 + self.rng.below(1000) as u32 * 1000)
    }

    /// One commit of 1-6 files on `line`.
    async fn work(&mut self, line: &mut Line) {
        let mut updates = BTreeMap::new();
        for _ in 0..1 + self.rng.below(6) {
            let (path, v) = self.pick(&line.home.clone(), &line.state);
            updates.insert(path, v);
        }
        line.tip = self.commit(vec![line.tip], updates.clone()).await;
        for (p, v) in updates {
            line.state.insert(p.clone(), v);
            line.dirty.insert(p);
        }
        line.commits += 1;
    }
}

/// Merge `from` into `into`: `from`'s dirty paths win. Returns the paths
/// whose content changed on `into`.
async fn merge_into(g: &mut Gen<'_>, into: &mut Line, from: &Line) -> Vec<String> {
    let mut updates = BTreeMap::new();
    for p in &from.dirty {
        let v = from.state.get(p).copied().unwrap_or(0);
        if into.state.get(p).copied().unwrap_or(0) != v {
            updates.insert(p.clone(), v);
        }
    }
    into.tip = g.commit(vec![into.tip, from.tip], updates.clone()).await;
    let changed: Vec<String> = updates.keys().cloned().collect();
    for (p, v) in updates {
        into.state.insert(p, v);
    }
    changed
}

/// The directories of the import and their files, wide ones first.
fn layout(spec: &Spec) -> Vec<(String, Vec<String>)> {
    let mut dirs = Vec::new();
    for i in 0..spec.wide_dirs {
        let n = spec.wide_min + i * spec.wide_step;
        dirs.push((
            format!("wide/w{i:02}"),
            (0..n).map(|f| format!("f{f:05}.c")).collect::<Vec<_>>(),
        ));
    }
    for a in 0..spec.deep.0 {
        for b in 0..spec.deep.1 {
            dirs.push((
                format!("deep/s{a:02}/d{b:02}"),
                (0..spec.deep.2).map(|f| format!("g{f:03}.c")).collect(),
            ));
        }
    }
    dirs
}

/// `n` changesets on one first-parent chain from `tip`, as lands of agents
/// make: each edits 1 to 3 files of the import, in a wide directory 30% of
/// the time, with contents no other chain has (`seed`). Returns them oldest
/// first.
pub async fn agent_chain(
    repo: &Repo,
    spec: &Spec,
    mut tip: ChangesetId,
    n: usize,
    seed: u64,
) -> Vec<ChangesetId> {
    let mut g = Gen {
        repo,
        rng: Lcg(seed),
        dirs: layout(spec),
        wide: spec.wide_dirs,
        seq: 10_000_000 * (1 + seed % 100),
        next_name: 0,
    };
    let deep = g.dirs.len() - g.wide;
    let mut out = Vec::with_capacity(n);
    for k in 0..n {
        let mut updates = BTreeMap::new();
        for _ in 0..1 + g.rng.below(3) {
            let d = if g.wide > 0 && g.rng.chance(30) {
                g.rng.below(g.wide)
            } else {
                g.wide + g.rng.below(deep)
            };
            let files = &g.dirs[d].1;
            let path = format!("{}/{}", g.dirs[d].0, files[g.rng.below(files.len())]);
            updates.insert(path, 1_000_000 + (seed as u32 % 1000) * 100_000 + k as u32);
        }
        tip = g.commit(vec![tip], updates).await;
        out.push(tip);
    }
    out
}

/// Build the history in `repo`; returns its head.
pub async fn build(repo: &Repo, spec: &Spec) -> ChangesetId {
    let dirs = layout(spec);
    let mut g = Gen {
        repo,
        rng: Lcg(spec.seed),
        dirs,
        wide: spec.wide_dirs,
        seq: 0,
        next_name: 0,
    };

    // Import, about 2,000 files per changeset.
    let all: Vec<String> = g
        .dirs
        .iter()
        .flat_map(|(d, fs)| fs.iter().map(move |f| format!("{d}/{f}")))
        .collect();
    let mut state = State::new();
    let mut tip: Option<ChangesetId> = None;
    for chunk in all.chunks(2000) {
        let updates: BTreeMap<String, u32> = chunk.iter().map(|p| (p.clone(), 1)).collect();
        tip = Some(g.commit(tip.into_iter().collect(), updates).await);
        state.extend(chunk.iter().map(|p| (p.clone(), 1)));
    }
    let mut main = Line {
        tip: tip.expect("files to import"),
        state,
        dirty: BTreeSet::new(),
        synced: 0,
        home: (0..g.dirs.len()).collect(),
        commits: 0,
        merge_after: 0,
    };
    // Every path mainline changed, in order, so a branch can merge what it
    // has not seen yet.
    let mut main_log: Vec<String> = Vec::new();

    let ndirs = g.dirs.len();
    let mut branches: Vec<Line> = (0..spec.branches)
        .map(|b| {
            let mut home = vec![b % g.wide, (b + 5) % g.wide];
            for k in 0..4 {
                home.push(g.wide + (b * 37 + k * 11) % (ndirs - g.wide));
            }
            Line {
                tip: main.tip,
                state: main.state.clone(),
                dirty: BTreeSet::new(),
                synced: 0,
                home,
                commits: 0,
                merge_after: 5 + (b * 13) % 40,
            }
        })
        .collect();

    let start = g.seq;
    let mut fork: Option<Line> = None;
    let cherry = (String::from("cherry/pick.c"), 1);
    let mut side = None;
    if spec.old_branch.is_some() {
        let updates = BTreeMap::from([cherry.clone()]);
        side = Some(Line {
            tip: g.commit(vec![main.tip], updates.clone()).await,
            state: updates.clone().into_iter().collect(),
            dirty: updates.into_keys().collect(),
            synced: 0,
            home: Vec::new(),
            commits: 0,
            merge_after: 0,
        });
    }
    while ((g.seq - start) as usize) < spec.commits {
        if let Some((back, _)) = spec.old_branch {
            if fork.is_none() && (g.seq - start) as usize + back >= spec.commits {
                fork = Some(Line {
                    tip: main.tip,
                    state: main.state.clone(),
                    dirty: BTreeSet::new(),
                    synced: 0,
                    home: vec![0, g.wide + 3],
                    commits: 0,
                    merge_after: 0,
                });
                // The same change on mainline, then the side branch merged:
                // the merge changes nothing, so both changes stay newest.
                let updates = BTreeMap::from([cherry.clone()]);
                main.tip = g.commit(vec![main.tip], updates).await;
                main.state.insert(cherry.0.clone(), cherry.1);
                let side = side.take().expect("made above");
                merge_into(&mut g, &mut main, &side).await;
            }
        }
        let r = g.rng.below(100);
        if r < 8 {
            // A direct commit on mainline.
            let before = main.state.clone();
            g.work(&mut main).await;
            main_log.extend(
                main.dirty
                    .iter()
                    .filter(|p| before.get(*p) != main.state.get(*p))
                    .cloned(),
            );
            main.dirty.clear();
            continue;
        }
        let b = g.rng.below(branches.len());
        let mut line = std::mem::replace(
            &mut branches[b],
            Line {
                tip: main.tip,
                state: State::new(),
                dirty: BTreeSet::new(),
                synced: 0,
                home: Vec::new(),
                commits: 0,
                merge_after: 0,
            },
        );
        if r < 18 {
            // A topic branch merged into the subsystem branch, after the
            // subsystem moved on a little.
            let mut topic = Line {
                tip: line.tip,
                state: line.state.clone(),
                dirty: BTreeSet::new(),
                synced: line.synced,
                home: line.home.clone(),
                commits: 0,
                merge_after: 0,
            };
            for _ in 0..2 + g.rng.below(7) {
                g.work(&mut topic).await;
            }
            for _ in 0..1 + g.rng.below(3) {
                g.work(&mut line).await;
            }
            let changed = merge_into(&mut g, &mut line, &topic).await;
            line.dirty.extend(changed);
        } else if r < 21 && line.synced < main_log.len() {
            // Merge mainline back: every path mainline changed since, which
            // the branch did not change itself.
            let seen: BTreeSet<String> = main_log[line.synced..].iter().cloned().collect();
            let mut updates = BTreeMap::new();
            for p in seen {
                let v = main.state.get(&p).copied().unwrap_or(0);
                if !line.dirty.contains(&p) && line.state.get(&p).copied().unwrap_or(0) != v {
                    updates.insert(p, v);
                }
            }
            line.tip = g.commit(vec![line.tip, main.tip], updates.clone()).await;
            line.state.extend(updates);
            line.synced = main_log.len();
        } else {
            g.work(&mut line).await;
        }
        if line.commits >= line.merge_after && !line.dirty.is_empty() {
            let changed = merge_into(&mut g, &mut main, &line).await;
            main_log.extend(changed);
            line.dirty.clear();
            line.commits = 0;
            line.merge_after = 5 + g.rng.below(40);
            if g.rng.chance(40) {
                // Start the next cycle from mainline.
                line.tip = main.tip;
                line.state = main.state.clone();
                line.synced = main_log.len();
            }
        }
        branches[b] = line;
    }
    if let (Some(mut line), Some((_, commits))) = (fork, spec.old_branch) {
        for _ in 0..commits.saturating_sub(1) {
            g.work(&mut line).await;
        }
        let mut updates = BTreeMap::from([("cherry/new.c".to_string(), 1)]);
        let (path, v) = g.pick(&line.home.clone(), &line.state);
        updates.insert(path, v);
        line.tip = g.commit(vec![line.tip], updates.clone()).await;
        line.dirty.extend(updates.keys().cloned());
        line.state.extend(updates);
        merge_into(&mut g, &mut main, &line).await;
    }
    main.tip
}
