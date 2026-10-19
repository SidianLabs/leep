//! Deriving a range of changesets on several cores (ADR 0050).
//!
//! A per-changeset row depends only on the changeset and its parents' rows,
//! so once every parent in the range is finished a changeset can run beside
//! any other such changeset. [`run`] keeps the range in a parents-first
//! order, starts the lowest-placed ready changesets up to `jobs` at a time
//! on the runtime's threads, and hands results back in an order where every
//! parent precedes its children, which is the order rows must be stored in.
//! With one job it runs exactly the serial order. Changesets are read and
//! prepared a window at a time, ahead of the work, and only a bounded
//! stretch past the oldest unfinished changeset is held in memory.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, VecDeque};
use std::future::Future;
use std::sync::Arc;

use async_trait::async_trait;
use futures_util::future::BoxFuture;
use leep_core::{Changeset, ChangesetId, Error, Result};
use tokio::task::JoinSet;

use super::{get_changesets, BATCH, HELD};
use crate::ancestry::SKIPLIST_READS;
use crate::Repo;

/// A parent outside the range (already derived, or unreadable).
pub(crate) const OUTSIDE: u32 = u32::MAX;

/// Windows of changesets read ahead of the oldest unfinished one, at least
/// (four per job beyond that). A window is read and prepared while earlier
/// ones derive; more than one ahead lets work start past a changeset that
/// takes long (a wide merge), and in a parents-first order the branches
/// that can run beside each other lie hundreds of positions apart.
const AHEAD: usize = 8;

/// A range of changesets in a parents-first order, with the links between
/// them as positions in that order. About 50 bytes a changeset, so a range
/// of all of Linux (1.5 million) is held in under 100 MB.
pub(crate) struct Dag {
    order: Vec<ChangesetId>,
    parent_start: Vec<u32>,
    /// Each changeset's parents in its own order, [`OUTSIDE`] for those not
    /// in the range.
    parents: Vec<u32>,
    child_start: Vec<u32>,
    /// One entry per parent link, so a parent named twice counts twice.
    children: Vec<u32>,
}

impl Dag {
    /// `order` must list parents before children; `parents_of` gives each
    /// changeset's parents.
    pub fn new<'a>(
        order: Vec<ChangesetId>,
        parents_of: impl Fn(&ChangesetId) -> &'a [ChangesetId],
    ) -> Dag {
        assert!(order.len() < OUTSIDE as usize, "range too long");
        let index: HashMap<ChangesetId, u32> = order
            .iter()
            .enumerate()
            .map(|(i, id)| (*id, i as u32))
            .collect();
        let mut parent_start = Vec::with_capacity(order.len() + 1);
        let mut parents = Vec::with_capacity(order.len() + order.len() / 4);
        let mut child_count = vec![0u32; order.len() + 1];
        for (i, id) in order.iter().enumerate() {
            parent_start.push(parents.len() as u32);
            for p in parents_of(id) {
                let at = index.get(p).copied().unwrap_or(OUTSIDE);
                if at != OUTSIDE {
                    assert!((at as usize) < i, "{id} comes before its parent {p}");
                    child_count[at as usize] += 1;
                }
                parents.push(at);
            }
        }
        parent_start.push(parents.len() as u32);
        drop(index);
        // Counts become start offsets, then fill positions.
        let mut child_start = Vec::with_capacity(order.len() + 1);
        let mut sum = 0u32;
        for c in &child_count {
            child_start.push(sum);
            sum += c;
        }
        let mut fill = child_start.clone();
        let mut children = vec![0u32; sum as usize];
        for i in 0..order.len() {
            for &p in &parents[parent_start[i] as usize..parent_start[i + 1] as usize] {
                if p != OUTSIDE {
                    children[fill[p as usize] as usize] = i as u32;
                    fill[p as usize] += 1;
                }
            }
        }
        Dag {
            order,
            parent_start,
            parents,
            child_start,
            children,
        }
    }

    pub fn len(&self) -> usize {
        self.order.len()
    }

    pub fn id(&self, i: usize) -> ChangesetId {
        self.order[i]
    }

    pub fn parents(&self, i: usize) -> &[u32] {
        &self.parents[self.parent_start[i] as usize..self.parent_start[i + 1] as usize]
    }

    pub fn children(&self, i: usize) -> &[u32] {
        &self.children[self.child_start[i] as usize..self.child_start[i + 1] as usize]
    }
}

/// Changesets of a window, in order, as read for [`Wave::prepare`].
pub(crate) type Window = Arc<[(ChangesetId, Arc<Changeset>)]>;

/// See [`Wave::preparer`].
pub(crate) type Preparer = Arc<dyn Fn(Repo, Window) -> BoxFuture<'static, ()> + Send + Sync>;

/// What [`run`] does with each changeset.
#[async_trait]
pub(crate) trait Wave: Send {
    type Out: Send + 'static;

    /// What reads ahead what a window's work will need. It runs beside the
    /// work of earlier windows, one window at a time, in order.
    fn preparer(&self) -> Preparer;

    /// The work for changeset `i`, every parent of which in the range has
    /// finished. It runs as a task of its own.
    fn start(
        &mut self,
        dag: &Dag,
        i: usize,
        cs: Arc<Changeset>,
    ) -> Result<BoxFuture<'static, Result<Self::Out>>>;

    /// Work started for `i` is done. Called one at a time, parents before
    /// children, before any child of `i` starts.
    async fn finish(&mut self, dag: &Dag, i: usize, out: Self::Out) -> Result<()>;

    /// Work that finished after another failed, and is not passed to
    /// [`Wave::finish`].
    fn discard(&mut self, _out: Self::Out) {}
}

enum Event<T> {
    Loaded(usize, Result<Vec<Arc<Changeset>>>),
    Done(usize, Result<T>),
}

/// `fut` as a task of its own that still sees the derivation's task locals:
/// the kinds whose lock it holds (a merge asking for its merge base must not
/// wait on its own lock) and its skiplist reads.
pub(crate) fn scoped<F>(fut: F) -> impl Future<Output = F::Output> + Send + 'static
where
    F: Future + Send + 'static,
    F::Output: Send,
{
    let held = HELD.try_with(|h| h.borrow().clone()).ok();
    let reads = SKIPLIST_READS.try_with(|c| c.clone()).ok();
    async move {
        match (held, reads) {
            (Some(h), Some(r)) => {
                HELD.scope(std::cell::RefCell::new(h), SKIPLIST_READS.scope(r, fut))
                    .await
            }
            (Some(h), None) => HELD.scope(std::cell::RefCell::new(h), fut).await,
            (None, Some(r)) => SKIPLIST_READS.scope(r, fut).await,
            (None, None) => fut.await,
        }
    }
}

/// Run `wave` over every changeset of `dag`, up to `jobs` at a time. The
/// first error stops new work; work already running is waited for (and
/// discarded) before it is returned.
pub(crate) async fn run<W: Wave>(repo: &Repo, dag: &Dag, jobs: usize, wave: &mut W) -> Result<()> {
    let n = dag.len();
    let jobs = jobs.max(1);
    let mut waiting: Vec<u32> = (0..n)
        .map(|i| dag.parents(i).iter().filter(|&&p| p != OUTSIDE).count() as u32)
        .collect();
    let mut finished = vec![false; n];
    // Changesets read, from position `base` to `loaded`.
    let mut slots: VecDeque<Arc<Changeset>> = VecDeque::new();
    let mut base = 0usize;
    let mut loaded = 0usize;
    let mut loading = false;
    // Oldest unfinished position.
    let mut low = 0usize;
    let mut ready: BinaryHeap<Reverse<u32>> = BinaryHeap::new();
    let mut tasks: JoinSet<Event<W::Out>> = JoinSet::new();
    let mut running = 0usize;
    let ahead = AHEAD.max(4 * jobs);
    let mut error: Option<Error> = None;
    loop {
        if error.is_none() {
            if !loading && loaded < n && loaded < low + ahead * BATCH {
                let end = (loaded + BATCH).min(n);
                let ids = dag.order[loaded..end].to_vec();
                let repo = repo.clone();
                let start = loaded;
                let prepare = wave.preparer();
                tasks.spawn(scoped(async move {
                    let read = match get_changesets(&repo, &ids).await {
                        Ok(v) => v,
                        Err(e) => return Event::Loaded(start, Err(e)),
                    };
                    let window: Window = ids
                        .into_iter()
                        .zip(read.into_iter().map(Arc::new))
                        .collect();
                    prepare(repo, window.clone()).await;
                    Event::Loaded(start, Ok(window.iter().map(|(_, cs)| cs.clone()).collect()))
                }));
                loading = true;
            }
            while running < jobs {
                let Some(Reverse(i)) = ready.pop() else { break };
                let i = i as usize;
                let cs = slots[i - base].clone();
                match wave.start(dag, i, cs) {
                    Ok(fut) => {
                        tasks.spawn(scoped(async move { Event::Done(i, fut.await) }));
                        running += 1;
                    }
                    Err(e) => {
                        error = Some(e);
                        break;
                    }
                }
            }
        }
        let Some(joined) = tasks.join_next().await else {
            break;
        };
        let event = match joined {
            Ok(ev) => ev,
            Err(e) if e.is_panic() => std::panic::resume_unwind(e.into_panic()),
            Err(e) => {
                error.get_or_insert(Error::Other(format!("derivation task failed: {e}")));
                continue;
            }
        };
        match event {
            Event::Loaded(start, read) => {
                loading = false;
                match read {
                    Ok(window) => {
                        debug_assert_eq!(start, loaded);
                        for (k, cs) in window.into_iter().enumerate() {
                            let i = start + k;
                            slots.push_back(cs);
                            if waiting[i] == 0 {
                                ready.push(Reverse(i as u32));
                            }
                        }
                        loaded = base + slots.len();
                    }
                    Err(e) => {
                        error.get_or_insert(e);
                    }
                }
            }
            Event::Done(i, out) => {
                running -= 1;
                let out = match out {
                    Ok(out) if error.is_none() => out,
                    Ok(out) => {
                        wave.discard(out);
                        continue;
                    }
                    Err(e) => {
                        error.get_or_insert(e);
                        continue;
                    }
                };
                if let Err(e) = wave.finish(dag, i, out).await {
                    error = Some(e);
                    continue;
                }
                finished[i] = true;
                for &c in dag.children(i) {
                    let c = c as usize;
                    waiting[c] -= 1;
                    if waiting[c] == 0 && c < loaded {
                        ready.push(Reverse(c as u32));
                    }
                }
                while low < n && finished[low] {
                    low += 1;
                }
                while base < low && !slots.is_empty() {
                    slots.pop_front();
                    base += 1;
                }
            }
        }
    }
    if let Some(e) = error {
        return Err(e);
    }
    if low < n {
        return Err(Error::Other(format!(
            "derivation stopped with {} of {n} changesets left",
            n - low
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use leep_core::Hash;

    fn id(n: u8) -> ChangesetId {
        ChangesetId(Hash([n; 32]))
    }

    /// Links become positions, parents outside the range are marked, and a
    /// parent named twice (an octopus listing it again) links twice.
    #[test]
    fn a_dag_links_positions_both_ways() {
        let parents: HashMap<ChangesetId, Vec<ChangesetId>> = HashMap::from([
            (id(1), vec![id(9)]),
            (id(2), vec![id(1)]),
            (id(3), vec![id(1)]),
            (id(4), vec![id(2), id(3), id(2)]),
        ]);
        let dag = Dag::new(vec![id(1), id(2), id(3), id(4)], |c| &parents[c]);
        assert_eq!(dag.len(), 4);
        assert_eq!(dag.id(3), id(4));
        assert_eq!(dag.parents(0), &[OUTSIDE]);
        assert_eq!(dag.parents(3), &[1, 2, 1]);
        assert_eq!(dag.children(0), &[1, 2]);
        assert_eq!(dag.children(1), &[3, 3]);
        assert!(dag.children(3).is_empty());
    }

    #[test]
    #[should_panic(expected = "comes before its parent")]
    fn a_dag_refuses_children_before_parents() {
        let parents: HashMap<ChangesetId, Vec<ChangesetId>> =
            HashMap::from([(id(1), vec![]), (id(2), vec![id(1)])]);
        Dag::new(vec![id(2), id(1)], |c| &parents[c]);
    }
}
