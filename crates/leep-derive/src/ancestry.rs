//! Ancestry queries: `is_ancestor` and `merge_base`.

use std::collections::{BinaryHeap, HashMap, HashSet};

use leep_core::{ChangesetId, Result};

use crate::derived::{self, LastMerge, Skiplist};
use crate::Repo;

const P1: u8 = 1;
const P2: u8 = 2;
const STALE: u8 = 4;

impl Repo {
    /// True if `ancestor` is reachable from `descendant` (inclusive).
    pub async fn is_ancestor(
        &self,
        ancestor: ChangesetId,
        descendant: ChangesetId,
    ) -> Result<bool> {
        if ancestor == descendant {
            return Ok(true);
        }
        let target = self.generation(ancestor).await?;
        if self.generation(descendant).await? <= target {
            return Ok(false);
        }
        // Fast path: ride the first-parent chain down via skip pointers.
        let mut x = descendant;
        loop {
            if x == ancestor {
                return Ok(true);
            }
            let list = Skiplist::decode(&derived::derive::<Skiplist>(self, x).await?)?;
            match list.iter().rev().find(|e| e.generation >= target) {
                Some(e) => x = e.id,
                None => break,
            }
        }
        // Full search over all parents, pruned by generation.
        let mut seen = HashSet::from([descendant]);
        let mut stack = vec![descendant];
        while let Some(c) = stack.pop() {
            for &p in self.parents(c).await?.iter() {
                if p == ancestor {
                    return Ok(true);
                }
                if seen.insert(p) && self.generation(p).await? > target {
                    stack.push(p);
                }
            }
        }
        Ok(false)
    }

    /// Best common ancestor of `a` and `b`, or `None` for unrelated histories.
    /// With several candidates, the one with the highest generation wins.
    pub async fn merge_base(&self, a: ChangesetId, b: ChangesetId) -> Result<Option<ChangesetId>> {
        if a == b {
            return Ok(Some(a));
        }
        // Fast path: the deepest common node of the two first-parent chains,
        // found with skiplist jumps. If neither chain has a merge above it,
        // every ancestor of `a` or `b` above it lies on those chains, so it
        // is the unique best common ancestor and the walk below would return
        // the same answer.
        if let Some(c) = self.first_parent_lca(a, b).await? {
            let gc = self.generation(c).await?;
            if self.last_merge(a).await? <= gc && self.last_merge(b).await? <= gc {
                return Ok(Some(c));
            }
        }
        self.merge_base_walk(a, b).await
    }

    async fn last_merge(&self, id: ChangesetId) -> Result<u64> {
        derived::decode_u64(&derived::derive::<LastMerge>(self, id).await?)
    }

    /// Deepest changeset on both first-parent chains, or `None` if they share
    /// no root. Jumps the chain with the higher generation down to the other's
    /// generation via the skiplist; equal generations step one at a time, so
    /// cost is logarithmic in history plus the length of genuinely parallel
    /// stretches.
    async fn first_parent_lca(
        &self,
        a: ChangesetId,
        b: ChangesetId,
    ) -> Result<Option<ChangesetId>> {
        let (mut x, mut y) = (a, b);
        loop {
            if x == y {
                return Ok(Some(x));
            }
            let (mut gx, gy) = (self.generation(x).await?, self.generation(y).await?);
            if gx < gy {
                std::mem::swap(&mut x, &mut y);
                gx = gy;
            }
            let target = self.generation(y).await?;
            let list = Skiplist::decode(&derived::derive::<Skiplist>(self, x).await?)?;
            let Some(parent) = list.first() else {
                return Ok(None);
            };
            x = if gx > target {
                // Farthest jump that stays at or above `y`'s generation; if
                // even the parent is below it, step to the parent anyway.
                list.iter()
                    .rev()
                    .find(|e| e.generation >= target)
                    .map_or(parent.id, |e| e.id)
            } else {
                parent.id
            };
        }
    }

    /// Reference algorithm: walk both histories by generation. Linear in the
    /// distance from the tips to the merge base.
    pub(crate) async fn merge_base_walk(
        &self,
        a: ChangesetId,
        b: ChangesetId,
    ) -> Result<Option<ChangesetId>> {
        let mut flags: HashMap<ChangesetId, u8> = HashMap::new();
        let mut gens: HashMap<ChangesetId, u64> = HashMap::new();
        let mut heap: BinaryHeap<(u64, ChangesetId, bool)> = BinaryHeap::new();
        let mut live = 0usize; // heap entries pushed without STALE
        for (id, f) in [(a, P1), (b, P2)] {
            let g = self.generation(id).await?;
            gens.insert(id, g);
            *flags.entry(id).or_default() |= f;
            heap.push((g, id, false));
            live += 1;
        }
        let mut candidates = Vec::new();
        let mut result: HashSet<ChangesetId> = HashSet::new();
        while live > 0 {
            let Some((_, id, stale_at_push)) = heap.pop() else {
                break;
            };
            if !stale_at_push {
                live -= 1;
            }
            let mut f = flags[&id];
            if f & (P1 | P2) == (P1 | P2) && f & STALE == 0 && result.insert(id) {
                candidates.push(id);
                f |= STALE;
                flags.insert(id, f);
            }
            for &p in self.parents(id).await?.iter() {
                let old = flags.get(&p).copied().unwrap_or(0);
                let new = old | f;
                if new == old {
                    continue;
                }
                flags.insert(p, new);
                let g = match gens.get(&p) {
                    Some(g) => *g,
                    None => {
                        let g = self.generation(p).await?;
                        gens.insert(p, g);
                        g
                    }
                };
                heap.push((g, p, new & STALE != 0));
                if new & STALE == 0 {
                    live += 1;
                }
            }
        }
        // Drop candidates that are ancestors of other candidates.
        let mut best: Option<(u64, ChangesetId)> = None;
        'outer: for &c in &candidates {
            for &o in &candidates {
                if o != c && self.is_ancestor(c, o).await? {
                    continue 'outer;
                }
            }
            let key = (gens[&c], c);
            if best.is_none_or(|b| key > b) {
                best = Some(key);
            }
        }
        Ok(best.map(|(_, id)| id))
    }
}
