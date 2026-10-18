//! Ancestry queries: `is_ancestor` and `merge_base`.

use std::collections::{BinaryHeap, HashMap, HashSet};
use std::sync::Arc;

use leep_core::{ChangesetId, Result};

use crate::derived::{self, LastMerge, SkipEntry};
use crate::prefetch::{ReadAhead, MAX_BATCH};
use crate::repo::SkiplistCache;
use crate::Repo;

tokio::task_local! {
    /// Skiplist rows read by ancestry queries during one derivation that
    /// asks many of them (path history at merges: a `merge_base` per merge,
    /// riding the same first-parent chains). Kept for that derivation only:
    /// `SkiplistCache` keeps no rows read by queries, so a row repaired
    /// later is read again, and a derivation reads each row once anyway.
    pub(crate) static SKIPLIST_READS: Option<Arc<SkiplistCache>>;
}

/// What a derivation scopes [`SKIPLIST_READS`] to: the enclosing
/// derivation's cache, else a new one if `keep`.
pub(crate) fn skiplist_reads_for(keep: bool) -> Option<Arc<SkiplistCache>> {
    match SKIPLIST_READS.try_with(|c| c.clone()) {
        Ok(Some(c)) => Some(c),
        _ => keep.then(Arc::default),
    }
}

const P1: u8 = 1;
const P2: u8 = 2;
const BOTH: u8 = P1 | P2;
#[cfg(test)]
const STALE: u8 = 4;

/// Skiplist reads `first_parent_lca` may make. Reaching a first-parent
/// ancestor takes at most two per bit of the distance, so 64 covers any
/// history; only parallel stretches run out.
const FIRST_PARENT_STEPS: usize = 64;

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
            match self
                .skiplist_noting_generations(x)
                .await?
                .iter()
                .rev()
                .find(|e| e.generation >= target)
            {
                Some(e) => x = e.id,
                None => break,
            }
        }
        // Full search over all parents. Nothing at or above `ancestor` can be
        // a common ancestor except `ancestor` itself.
        let floor = (target, ancestor);
        Ok(self
            .newest_common_ancestor(descendant, ancestor, Some(floor))
            .await?
            .is_some())
    }

    /// Best common ancestor of `a` and `b`, or `None` for unrelated histories:
    /// the common ancestor with the highest generation, ties going to the
    /// highest id. No other common ancestor can descend from it, since a
    /// descendant has a higher generation.
    pub async fn merge_base(&self, a: ChangesetId, b: ChangesetId) -> Result<Option<ChangesetId>> {
        if a == b {
            return Ok(Some(a));
        }
        // The deepest common node of the two first-parent chains, found with
        // skiplist jumps, is a common ancestor, so the answer is at or above
        // it. Without one (unrelated chains, or long parallel stretches), the
        // walk alone decides.
        let Some(c) = self.first_parent_lca(a, b, FIRST_PARENT_STEPS).await? else {
            return self.newest_common_ancestor(a, b, None).await;
        };
        // One side is a first-parent ancestor of the other. Every common
        // ancestor is an ancestor of that side, so it is the best one.
        if c == a || c == b {
            return Ok(Some(c));
        }
        // If neither chain has a merge above `c`, every ancestor of `a` or
        // `b` above it lies on those chains, so nothing above it is common.
        let gc = self.generation(c).await?;
        if self.last_merge(a).await? <= gc && self.last_merge(b).await? <= gc {
            return Ok(Some(c));
        }
        Ok(Some(
            self.newest_common_ancestor(a, b, Some((gc, c)))
                .await?
                .unwrap_or(c),
        ))
    }

    async fn last_merge(&self, id: ChangesetId) -> Result<u64> {
        derived::decode_u64(&derived::derive::<LastMerge>(self, id).await?)
    }

    /// Skiplist of `id`. The generations it names are kept, so riding it
    /// down does not read them again.
    async fn skiplist_noting_generations(&self, id: ChangesetId) -> Result<Arc<[SkipEntry]>> {
        let reads = SKIPLIST_READS.try_with(|c| c.clone()).ok().flatten();
        let list = match reads.as_ref().and_then(|c| c.get(&id)) {
            Some(list) => list,
            None => {
                let list = self.skiplist(id).await?;
                if let Some(c) = &reads {
                    c.insert(id, list.clone());
                }
                list
            }
        };
        for e in list.iter() {
            self.remember_generation(e.id, e.generation);
        }
        Ok(list)
    }

    /// Deepest changeset on both first-parent chains, or `None` if they share
    /// no root or finding it takes more than `max_steps` skiplist reads. Jumps the chain with the higher generation down to the other's
    /// generation via the skiplist; equal generations step one at a time, so
    /// cost is logarithmic in history plus the length of genuinely parallel
    /// stretches. Those are read one round trip per step, so long ones are
    /// left to the batched walk.
    async fn first_parent_lca(
        &self,
        a: ChangesetId,
        b: ChangesetId,
        max_steps: usize,
    ) -> Result<Option<ChangesetId>> {
        let (mut x, mut y) = (a, b);
        for _ in 0..max_steps {
            if x == y {
                return Ok(Some(x));
            }
            let (mut gx, gy) = (self.generation(x).await?, self.generation(y).await?);
            if gx < gy {
                std::mem::swap(&mut x, &mut y);
                gx = gy;
            }
            let target = self.generation(y).await?;
            let list = self.skiplist_noting_generations(x).await?;
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
        Ok(None)
    }

    /// The common ancestor of `a` and `b` with the highest (generation, id)
    /// among those at or above `floor`, or `None`.
    ///
    /// Walks both histories newest first by (generation, id), marking each
    /// changeset with the sides that reach it. Parents rank below their
    /// children, so a changeset pops only after all its walked children and
    /// its marks are final then: the first one marked by both sides is the
    /// answer. Once either side has nothing left on the heap, nothing popped
    /// later can gain its mark, and the walk stops. Changesets below `floor`
    /// are never queued; their ancestors rank lower still.
    ///
    /// This returns what painting the whole history down and then dropping
    /// redundant candidates (git's approach, kept as `merge_base_walk` in
    /// tests) returns, without walking past the answer.
    async fn newest_common_ancestor(
        &self,
        a: ChangesetId,
        b: ChangesetId,
        floor: Option<(u64, ChangesetId)>,
    ) -> Result<Option<ChangesetId>> {
        let below = |key: (u64, ChangesetId)| floor.is_some_and(|f| key < f);
        let mut reader = Reader::default();
        let tips = reader.generations(self, &[a, b], u64::MAX).await?;
        if a == b {
            return Ok((!below((tips[0], a))).then_some(a));
        }
        let mut marks: HashMap<ChangesetId, u8> = HashMap::new();
        let mut pruned: HashSet<ChangesetId> = HashSet::new();
        let mut heap: BinaryHeap<(u64, ChangesetId)> = BinaryHeap::new();
        // Queued changesets carrying each side's mark.
        let mut queued = [0usize; 2];
        let count = |queued: &mut [usize; 2], m: u8, add: bool| {
            for (i, side) in [P1, P2].into_iter().enumerate() {
                if m & side != 0 {
                    if add {
                        queued[i] += 1;
                    } else {
                        queued[i] -= 1;
                    }
                }
            }
        };
        for (id, g, m) in [(a, tips[0], P1), (b, tips[1], P2)] {
            if !below((g, id)) {
                marks.insert(id, m);
                heap.push((g, id));
                count(&mut queued, m, true);
            }
        }
        while queued[0] > 0 && queued[1] > 0 {
            let (g, id) = heap.pop().expect("queued changesets are on the heap");
            let m = marks[&id];
            count(&mut queued, m, false);
            if m == BOTH {
                return Ok(Some(id));
            }
            let parents = reader.parents(self, id, g).await?;
            let new: Vec<ChangesetId> = parents
                .iter()
                .copied()
                .filter(|p| !marks.contains_key(p) && !pruned.contains(p))
                .collect();
            let gens: HashMap<ChangesetId, u64> = new
                .iter()
                .copied()
                .zip(reader.generations(self, &new, g).await?)
                .collect();
            for p in parents.iter() {
                if let Some(old) = marks.get_mut(p) {
                    // Still queued: it ranks below `id`.
                    let added = m & !*old;
                    *old |= added;
                    count(&mut queued, added, true);
                } else if let Some(&gp) = gens.get(p) {
                    if below((gp, *p)) {
                        pruned.insert(*p);
                    } else {
                        marks.insert(*p, m);
                        heap.push((gp, *p));
                        count(&mut queued, m, true);
                    }
                }
            }
        }
        Ok(None)
    }
}

/// The ancestors of one changeset down to a generation that only moves
/// down, so many ancestry questions against the same descendant share one
/// walk instead of making one each (`is_ancestor` walks from the descendant
/// to the other side's generation every time it is asked).
pub(crate) struct Ancestors {
    reader: Reader,
    /// Every changeset reached, all ancestors (inclusive).
    seen: HashSet<ChangesetId>,
    /// Reached but not yet expanded, newest first.
    heap: BinaryHeap<(u64, ChangesetId)>,
}

impl Ancestors {
    pub(crate) fn new(tip: ChangesetId, generation: u64) -> Self {
        Ancestors {
            reader: Reader::default(),
            seen: HashSet::from([tip]),
            heap: BinaryHeap::from([(generation, tip)]),
        }
    }

    /// Whether `id`, of generation `generation`, is an ancestor of the tip.
    /// Every ancestor at or above that generation is reached through
    /// changesets above it, so expanding down to it settles the answer.
    pub(crate) async fn contains(
        &mut self,
        repo: &Repo,
        id: ChangesetId,
        generation: u64,
    ) -> Result<bool> {
        while let Some(&(g, top)) = self.heap.peek() {
            if self.seen.contains(&id) || g <= generation {
                break;
            }
            self.heap.pop();
            let parents = self.reader.parents(repo, top, g).await?;
            let new: Vec<ChangesetId> = parents
                .iter()
                .copied()
                .filter(|p| !self.seen.contains(p))
                .collect();
            let gens = self.reader.generations(repo, &new, g).await?;
            for (p, gp) in new.into_iter().zip(gens) {
                if self.seen.insert(p) {
                    self.heap.push((gp, p));
                }
            }
        }
        Ok(self.seen.contains(&id))
    }
}

/// Parents and generations for a newest-first walk: from `Repo`'s ancestry
/// cache if seen before, else read ahead in batches (see [`ReadAhead`]) and
/// kept in that cache. What a batch misses is read singly, as before.
struct Reader {
    ahead: ReadAhead,
}

impl Default for Reader {
    fn default() -> Self {
        Reader {
            ahead: ReadAhead::new(false),
        }
    }
}

impl Reader {
    /// `at`: the generation the walk has reached, which bounds read-ahead.
    async fn parents(
        &mut self,
        repo: &Repo,
        id: ChangesetId,
        at: u64,
    ) -> Result<Arc<[ChangesetId]>> {
        if let Some(p) = repo.cached_parents(id) {
            return Ok(p);
        }
        self.ahead.ensure(repo, &[id], at, MAX_BATCH).await;
        match self.ahead.take(id).0 {
            Some(cs) => {
                let p: Arc<[ChangesetId]> = cs.parents.into();
                repo.remember_parents(id, p.clone());
                Ok(p)
            }
            None => repo.parents(id).await,
        }
    }

    async fn generations(&mut self, repo: &Repo, ids: &[ChangesetId], at: u64) -> Result<Vec<u64>> {
        let missing: Vec<ChangesetId> = ids
            .iter()
            .copied()
            .filter(|&id| repo.cached_generation(id).is_none())
            .collect();
        if !missing.is_empty() {
            self.ahead.ensure(repo, &missing, at, MAX_BATCH).await;
        }
        let mut out = Vec::with_capacity(ids.len());
        for &id in ids {
            let g = match repo.cached_generation(id) {
                Some(g) => g,
                None => {
                    let g = self.ahead.generation(repo, id).await?;
                    repo.remember_generation(id, g);
                    g
                }
            };
            out.push(g);
        }
        Ok(out)
    }
}

#[cfg(test)]
impl Repo {
    /// `merge_base` as it was before it stopped at the first common
    /// ancestor, for tests and measurements.
    pub(crate) async fn merge_base_reference(
        &self,
        a: ChangesetId,
        b: ChangesetId,
    ) -> Result<Option<ChangesetId>> {
        if a == b {
            return Ok(Some(a));
        }
        if let Some(c) = self.first_parent_lca(a, b, usize::MAX).await? {
            let gc = self.generation(c).await?;
            if self.last_merge(a).await? <= gc && self.last_merge(b).await? <= gc {
                return Ok(Some(c));
            }
        }
        self.merge_base_walk(a, b).await
    }

    /// `is_ancestor` as it was before batched reads, for tests.
    pub(crate) async fn is_ancestor_reference(
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
            let list =
                derived::Skiplist::decode(&derived::derive::<derived::Skiplist>(self, x).await?)?;
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

    /// `merge_base` as it was before it stopped at the first common ancestor:
    /// paint both histories down by generation until no live entry is left,
    /// then drop candidates that are ancestors of others. Tests check the
    /// current algorithm against it.
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
                if o != c && self.is_ancestor_reference(c, o).await? {
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    use bytes::Bytes;
    use leep_core::memory::{MemoryBlobStore, MemoryMetaStore};
    use leep_core::{BlobStore, Changeset, Error, Hash, MetaStore, RefLogEntry, RefUpdateContext};

    use super::*;

    /// Counts reads and can delay each call or refuse batched ones.
    #[derive(Default)]
    struct Counter {
        inner: MemoryMetaStore,
        /// Microseconds each read waits.
        latency_us: AtomicU64,
        fail_batches: bool,
        singles: AtomicUsize,
        batches: AtomicUsize,
        cs_ids: AtomicUsize,
    }

    impl Counter {
        async fn call(&self, batch: bool) -> leep_core::Result<()> {
            let n = if batch { &self.batches } else { &self.singles };
            n.fetch_add(1, Ordering::Relaxed);
            let us = self.latency_us.load(Ordering::Relaxed);
            if us > 0 {
                tokio::time::sleep(Duration::from_micros(us)).await;
            }
            if batch && self.fail_batches {
                return Err(Error::Storage("no batches".into()));
            }
            Ok(())
        }

        fn take(&self) -> (usize, usize) {
            (
                self.singles.swap(0, Ordering::Relaxed),
                self.batches.swap(0, Ordering::Relaxed),
            )
        }
    }

    #[async_trait::async_trait]
    impl MetaStore for Counter {
        async fn put_changeset(&self, cs: &Changeset) -> leep_core::Result<ChangesetId> {
            self.inner.put_changeset(cs).await
        }
        async fn get_changeset(&self, id: &ChangesetId) -> leep_core::Result<Option<Changeset>> {
            self.call(false).await?;
            self.inner.get_changeset(id).await
        }
        async fn get_changesets(
            &self,
            ids: &[ChangesetId],
        ) -> leep_core::Result<Vec<Option<Changeset>>> {
            self.call(true).await?;
            self.cs_ids.fetch_add(ids.len(), Ordering::Relaxed);
            self.inner.get_changesets(ids).await
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
            ctx: &RefUpdateContext,
        ) -> leep_core::Result<bool> {
            self.inner.cas_ref_logged(name, expected, new, ctx).await
        }
        async fn ref_log(
            &self,
            name: Option<&str>,
            before_seq: Option<u64>,
            limit: usize,
        ) -> leep_core::Result<Vec<RefLogEntry>> {
            self.inner.ref_log(name, before_seq, limit).await
        }
        async fn list_refs(&self) -> leep_core::Result<BTreeMap<String, ChangesetId>> {
            self.inner.list_refs().await
        }
        async fn get_derived(&self, kind: &str, key: &Hash) -> leep_core::Result<Option<Bytes>> {
            self.call(false).await?;
            self.inner.get_derived(kind, key).await
        }
        async fn get_derived_many(
            &self,
            kind: &str,
            keys: &[Hash],
        ) -> leep_core::Result<Vec<Option<Bytes>>> {
            self.call(true).await?;
            self.inner.get_derived_many(kind, keys).await
        }
        async fn put_derived(&self, kind: &str, key: &Hash, value: Bytes) -> leep_core::Result<()> {
            self.inner.put_derived(kind, key, value).await
        }
        async fn delete_derived_kind(&self, kind: &str) -> leep_core::Result<u64> {
            self.inner.delete_derived_kind(kind).await
        }
    }

    struct Lcg(u64);

    impl Lcg {
        fn below(&mut self, n: usize) -> usize {
            self.0 = self
                .0
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            ((self.0 >> 33) as usize) % n
        }
    }

    struct Fixture {
        blobs: Arc<dyn BlobStore>,
        meta: Arc<Counter>,
        next: u64,
    }

    impl Fixture {
        fn new(counter: Counter) -> Self {
            Fixture {
                blobs: Arc::new(MemoryBlobStore::new()),
                meta: Arc::new(counter),
                next: 0,
            }
        }

        /// A handle with an empty ancestry cache, as in a fresh process.
        fn cold(&self) -> Repo {
            Repo::new(self.blobs.clone(), self.meta.clone())
        }

        async fn add(&mut self, r: &Repo, parents: Vec<ChangesetId>) -> ChangesetId {
            self.next += 1;
            r.create_changeset(Changeset {
                parents,
                author: "t".into(),
                timestamp: self.next as i64,
                message: format!("c{}", self.next),
                changes: BTreeMap::new(),
                extra: BTreeMap::new(),
            })
            .await
            .unwrap()
        }
    }

    /// Random DAG: branches off recent and old changesets, two-parent
    /// merges (criss-crosses among them), octopus merges and extra roots.
    async fn random_dag(f: &mut Fixture, rng: &mut Lcg, n: usize) -> Vec<ChangesetId> {
        let r = f.cold();
        let mut ids: Vec<ChangesetId> = Vec::new();
        for i in 0..n {
            let len = ids.len();
            let pick = |rng: &mut Lcg, span: usize| ids[len - 1 - rng.below(len.min(span))];
            let parents = match (len, rng.below(100)) {
                (0, _) => vec![],
                (_, 0..=3) if i < n / 2 => vec![],
                (l, 4..=23) if l >= 2 => {
                    let mut ps = vec![pick(rng, 6)];
                    let extra = if rng.below(6) == 0 {
                        2 + rng.below(2)
                    } else {
                        1
                    };
                    for _ in 0..extra {
                        let p = if rng.below(2) == 0 {
                            pick(rng, 25)
                        } else {
                            ids[rng.below(len)]
                        };
                        if !ps.contains(&p) {
                            ps.push(p);
                        }
                    }
                    ps
                }
                (_, 24..=40) => vec![ids[rng.below(len)]],
                _ => vec![pick(rng, 4)],
            };
            ids.push(f.add(&r, parents).await);
        }
        ids
    }

    #[tokio::test]
    async fn merge_base_matches_the_reference_walk_on_random_dags() {
        for seed in 1..=12u64 {
            let mut rng = Lcg(seed);
            let mut f = Fixture::new(Counter {
                fail_batches: seed % 4 == 0,
                ..Default::default()
            });
            let n = 30 + rng.below(220);
            let ids = random_dag(&mut f, &mut rng, n).await;
            if seed % 3 == 0 {
                // Rows the read-ahead misses are derived again on demand.
                f.meta.delete_derived_kind("skiplist").await.unwrap();
                f.meta.delete_derived_kind("generation").await.unwrap();
            }
            let warm = f.cold();
            let reference = f.cold();
            for k in 0..400 {
                let (a, b) = (ids[rng.below(ids.len())], ids[rng.below(ids.len())]);
                let want = reference.merge_base_walk(a, b).await.unwrap();
                assert_eq!(
                    reference.merge_base_reference(a, b).await.unwrap(),
                    want,
                    "seed {seed}"
                );
                // Alternate a cache shared across queries with a fresh one.
                let r = if k % 2 == 0 { warm.clone() } else { f.cold() };
                assert_eq!(
                    r.merge_base(a, b).await.unwrap(),
                    want,
                    "merge_base({a}, {b}), seed {seed}"
                );
                assert_eq!(
                    f.cold().newest_common_ancestor(a, b, None).await.unwrap(),
                    want,
                    "seed {seed}"
                );
                assert_eq!(
                    r.is_ancestor(a, b).await.unwrap(),
                    reference.is_ancestor_reference(a, b).await.unwrap(),
                    "is_ancestor({a}, {b}), seed {seed}"
                );
            }
        }
    }

    struct GitLike {
        /// First-parent chain of the head, newest first: `main[k]` is
        /// `head~k`.
        main: Vec<ChangesetId>,
        /// Topic tips merged into main, with the `k` of their merge.
        merged: Vec<(ChangesetId, usize)>,
        /// A topic forked from `head~fork` and never merged.
        open_topic: (ChangesetId, usize),
        total: usize,
    }

    /// A history shaped like git/git: a long first-parent chain into which
    /// topic branches, forked from bases up to a few hundred changesets
    /// old, are merged; some topics merge main back in first.
    async fn git_like(f: &mut Fixture, merges: usize) -> GitLike {
        let r = f.cold();
        let mut rng = Lcg(99);
        let mut main = vec![f.add(&r, vec![]).await];
        let mut merged_at = Vec::new();
        let mut total = 1;
        for _ in 0..merges {
            for _ in 0..rng.below(3) {
                let tip = *main.last().unwrap();
                main.push(f.add(&r, vec![tip]).await);
                total += 1;
            }
            let len = main.len();
            let mut topic = main[len - 1 - rng.below(len.min(400))];
            for i in 0..1 + rng.below(6) {
                let mut ps = vec![topic];
                if i == 2 && rng.below(4) == 0 {
                    ps.push(main[len - 1 - rng.below(len.min(50))]);
                    ps.dedup();
                }
                topic = f.add(&r, ps).await;
                total += 1;
            }
            let tip = *main.last().unwrap();
            let m = f.add(&r, vec![tip, topic]).await;
            main.push(m);
            merged_at.push((topic, main.len() - 1));
            total += 1;
        }
        let fork = main.len() - 1 - 800;
        let mut topic = main[fork];
        for _ in 0..5 {
            topic = f.add(&r, vec![topic]).await;
            total += 1;
        }
        let len = main.len();
        main.reverse();
        GitLike {
            main,
            merged: merged_at
                .into_iter()
                .map(|(t, at)| (t, len - 1 - at))
                .collect(),
            open_topic: (topic, len - 1 - fork),
            total,
        }
    }

    /// The cases measured, with the answer each must give.
    fn cases(g: &GitLike) -> Vec<(&'static str, ChangesetId, ChangesetId, ChangesetId)> {
        let head = g.main[0];
        let (side, _) = *g
            .merged
            .iter()
            .filter(|(_, k)| *k >= 1000)
            .min_by_key(|(_, k)| *k)
            .expect("a topic merged 1000 back");
        let root = *g.main.last().unwrap();
        vec![
            ("head vs head~1000", head, g.main[1000], g.main[1000]),
            ("head vs root", head, root, root),
            ("head vs topic merged ~1000 back", head, side, side),
            (
                "head vs open topic forked at head~800",
                head,
                g.open_topic.0,
                g.main[g.open_topic.1],
            ),
        ]
    }

    #[tokio::test]
    async fn merge_base_on_a_git_like_history_reads_little() {
        let mut f = Fixture::new(Counter::default());
        let g = git_like(&mut f, 1500).await;
        assert!(g.main.len() > 2500, "{} on main", g.main.len());
        for (name, a, b, want) in cases(&g) {
            let r = f.cold();
            f.meta.take();
            assert_eq!(r.merge_base(a, b).await.unwrap(), Some(want), "{name}");
            let (singles, batches) = f.meta.take();
            // Asked again, the walk is answered from the ancestry cache; only
            // skiplist and `last_merge-v1` rows are read again.
            assert_eq!(r.merge_base(a, b).await.unwrap(), Some(want), "{name}");
            let (warm_singles, warm_batches) = f.meta.take();
            assert!(
                warm_batches == 0 && warm_singles <= 40,
                "{name}, warm: {warm_singles} + {warm_batches}"
            );
            let reference = f.cold();
            assert_eq!(
                reference.merge_base_reference(a, b).await.unwrap(),
                Some(want),
                "{name}"
            );
            let (old_singles, old_batches) = f.meta.take();
            eprintln!(
                "{name}: {singles} single + {batches} batched reads \
                 (before: {old_singles} + {old_batches}) of {} changesets",
                g.total
            );
            if name == "head vs head~1000" || name == "head vs root" {
                // Skiplist jumps only: logarithmic in history.
                assert!(singles + batches <= 40, "{name}: {singles} + {batches}");
            } else {
                assert!(
                    singles + batches < old_singles / 20,
                    "{name}: {singles} + {batches} against {old_singles}"
                );
            }
        }
    }

    /// Before/after cost with 1 ms per metadata call, as on a busy database
    /// host. `cargo test -p leep-derive --release merge_base_cold_cost -- \
    /// --ignored --nocapture`
    #[tokio::test]
    #[ignore = "measurement; prints a table"]
    async fn merge_base_cold_cost() {
        let mut f = Fixture::new(Counter::default());
        let g = git_like(&mut f, 3000).await;
        f.meta.latency_us.store(1000, Ordering::Relaxed);
        eprintln!(
            "{} changesets, {} on the first-parent chain",
            g.total,
            g.main.len()
        );
        for (name, a, b, want) in cases(&g) {
            for (label, new) in [("before", false), ("after", true)] {
                let r = f.cold();
                f.meta.take();
                let mut times = Vec::new();
                let mut reads = Vec::new();
                for _ in 0..2 {
                    let t = Instant::now();
                    let got = if new {
                        r.merge_base(a, b).await.unwrap()
                    } else {
                        r.merge_base_reference(a, b).await.unwrap()
                    };
                    times.push(t.elapsed());
                    let (singles, batches) = f.meta.take();
                    reads.push((singles, batches, f.meta.cs_ids.swap(0, Ordering::Relaxed)));
                    assert_eq!(got, Some(want), "{name}");
                }
                eprintln!(
                    "| {name} | {label} | {:.1?}, {} + {} reads ({} changesets batched) \
                     | {:.1?}, {} + {} reads |",
                    times[0], reads[0].0, reads[0].1, reads[0].2, times[1], reads[1].0, reads[1].1
                );
            }
        }
    }
}
