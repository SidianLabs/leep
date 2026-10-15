//! Ancestry queries: `is_ancestor` and `merge_base`.

use std::collections::{BinaryHeap, HashMap, HashSet};

use leep_core::{ChangesetId, Result};

use crate::derived::{self, Skiplist};
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
            for p in self.get_changeset(c).await?.parents {
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
            for p in self.get_changeset(id).await?.parents {
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
