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
        }
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

/// Build the history in `repo`; returns its head.
pub async fn build(repo: &Repo, spec: &Spec) -> ChangesetId {
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
    while ((g.seq - start) as usize) < spec.commits {
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
    main.tip
}
