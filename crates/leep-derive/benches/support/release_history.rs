//! A synthetic history shaped like Linux's release cycles, for timing
//! `log-filter-v1` derivation at back-merges: each cycle mainline merges
//! hundreds of subsystem branches (most based on the last release, some on
//! a recent rc, a few on releases long past), each with topic branches
//! merged into it, and a few `for-next` branches forked from an older
//! release merge the new release back into themselves (like Linux's
//! "Merge tag 'v4.9-rc3' into xtensa-for-next"), so the back-merge brings in
//! a whole cycle or two: tens of thousands of changesets and thousands of
//! merges. Those branches are merged into mainline in the next cycle.
//! Changesets have no files. Deterministic for a given [`Spec`].
//!
//! Used by the `log_filter_profile` example of `leep-server` (included with
//! `#[path]`).

#![allow(dead_code)]

use std::collections::BTreeMap;

use leep_core::{Changeset, ChangesetId};
use leep_derive::Repo;

#[derive(Clone, Debug)]
pub struct Spec {
    pub cycles: usize,
    /// Subsystem branches merged per cycle.
    pub branches: usize,
    /// Changesets per subsystem branch, before topics: `1..=branch_len`.
    pub branch_len: usize,
    /// Percent of a branch's changesets followed by a merged topic of 2-8.
    pub topic_percent: usize,
    /// Percent of branches based on a release 2 to `old_back` cycles back; 25% more are
    /// based anywhere in the last two cycles, the rest on the last release.
    pub old_percent: usize,
    /// Most cycles back an old branch is based.
    pub old_back: usize,
    /// Most direct changes between two merges of the merge window.
    pub direct: usize,
    /// Percent of a branch's changesets followed by a merge of a newer
    /// mainline changeset.
    pub back_percent: usize,
    /// `for-next` branches per cycle that merge the new release back.
    pub next_branches: usize,
    /// Cycles back the `for-next` branches fork from (1: the last release).
    pub next_back: usize,
    pub authors: usize,
    pub seed: u64,
}

impl Spec {
    /// About 14,000 changesets and 1,100 merges a cycle.
    pub fn linux_like(cycles: usize) -> Self {
        Spec {
            cycles,
            branches: 400,
            branch_len: 30,
            topic_percent: 6,
            old_percent: 15,
            back_percent: 2,
            old_back: 8,
            direct: 1,
            next_branches: 2,
            next_back: 2,
            authors: 3000,
            seed: 0x11_4e75,
        }
    }
}

/// What [`build`] made.
pub struct Built {
    pub head: ChangesetId,
    /// The `for-next` branches' merges of a release, oldest first.
    pub back_merges: Vec<ChangesetId>,
    pub changesets: usize,
}

struct Lcg(u64);

impl Lcg {
    fn below(&mut self, n: usize) -> usize {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        ((self.0 >> 33) as usize) % n.max(1)
    }

    /// Zipf with exponent 1 over `0..n`.
    fn zipf(&mut self, n: usize) -> usize {
        let h = (n as f64).ln() + 0.5772;
        let u = (self.below(1 << 30) as f64 + 0.5) / (1u64 << 30) as f64;
        ((u * h - 0.5772).exp() as usize).min(n - 1)
    }
}

struct Gen<'a> {
    repo: &'a Repo,
    rng: Lcg,
    authors: usize,
    n: usize,
}

impl Gen<'_> {
    async fn commit(&mut self, parents: Vec<ChangesetId>) -> ChangesetId {
        self.n += 1;
        let a = self.rng.zipf(self.authors);
        let merge = parents.len() > 1;
        self.repo
            .create_changeset(Changeset {
                parents,
                author: format!("Developer {a} <dev{a}@kernel.example>"),
                timestamp: 1_100_000_000 + self.n as i64 * 60,
                message: if merge { "merge" } else { "change" }.into(),
                changes: BTreeMap::new(),
                extra: BTreeMap::new(),
            })
            .await
            .unwrap()
    }

    async fn chain(&mut self, mut tip: ChangesetId, n: usize) -> ChangesetId {
        for _ in 0..n {
            tip = self.commit(vec![tip]).await;
        }
        tip
    }
}

pub async fn build(repo: &Repo, spec: &Spec) -> Built {
    let mut g = Gen {
        repo,
        rng: Lcg(spec.seed),
        authors: spec.authors,
        n: 0,
    };
    let root = g.commit(vec![]).await;
    let mut main = g.chain(root, 200).await;
    // Mainline's first-parent history and each release's position in it.
    let mut history = vec![main];
    let mut tags = vec![main];
    let mut back_merges = Vec::new();
    // `for-next` branches waiting to merge the next release, and those that
    // did, waiting to be merged into mainline.
    let mut next: Vec<ChangesetId> = Vec::new();
    let mut done_next: Vec<ChangesetId> = Vec::new();
    let mut tag_at = vec![0usize];
    for _ in 0..spec.cycles {
        let rc_from = tag_at[tag_at.len().saturating_sub(2)];
        let mut tips = std::mem::take(&mut done_next);
        for _ in 0..spec.branches {
            let roll = g.rng.below(100);
            let at = if roll < spec.old_percent && tag_at.len() > 2 {
                let back = 2 + g.rng.below(spec.old_back.max(2) - 1);
                tag_at[tag_at.len().saturating_sub(1 + back)]
            } else if roll < spec.old_percent + 25 {
                rc_from + g.rng.below(history.len() - rc_from)
            } else {
                *tag_at.last().unwrap()
            };
            let mut tip = history[at];
            for _ in 0..1 + g.rng.below(spec.branch_len) {
                tip = g.commit(vec![tip]).await;
                let roll = g.rng.below(100);
                if roll < spec.topic_percent {
                    let n = 2 + g.rng.below(7);
                    let topic = g.chain(tip, n).await;
                    let n = g.rng.below(3);
                    tip = g.chain(tip, n).await;
                    tip = g.commit(vec![tip, topic]).await;
                } else if roll < spec.topic_percent + spec.back_percent && at + 1 < history.len() {
                    // Merge a newer mainline back, like "Merge tag 'v4.8-rc5'
                    // into drm-next".
                    let rc = history[at + 1 + g.rng.below(history.len() - at - 1)];
                    tip = g.commit(vec![tip, rc]).await;
                }
            }
            tips.push(tip);
        }
        // The merge window, with direct changes between merges.
        for tip in tips {
            for _ in 0..g.rng.below(spec.direct + 1) {
                main = g.commit(vec![main]).await;
                history.push(main);
            }
            main = g.commit(vec![main, tip]).await;
            history.push(main);
        }
        // Release candidates: fixes.
        for _ in 0..spec.branches / 4 {
            main = g.commit(vec![main]).await;
            history.push(main);
        }
        tags.push(main);
        tag_at.push(history.len() - 1);
        // Last cycle's `for-next` branches merge this release back.
        for tip in std::mem::take(&mut next) {
            let m = g.commit(vec![tip, main]).await;
            back_merges.push(m);
            let n = 1 + g.rng.below(3);
            let m = g.chain(m, n).await;
            done_next.push(m);
        }
        let fork = tags[tags.len().saturating_sub(1 + spec.next_back)];
        for i in 0..spec.next_branches {
            // The first adds nothing of its own, like xtensa-for-next.
            let n = if i == 0 { 0 } else { 1 + g.rng.below(40) };
            let tip = g.chain(fork, n).await;
            next.push(tip);
        }
    }
    let mut parents = vec![main];
    parents.extend(done_next);
    let head = if parents.len() > 1 {
        g.commit(parents).await
    } else {
        main
    };
    Built {
        head,
        back_merges,
        changesets: g.n,
    }
}
