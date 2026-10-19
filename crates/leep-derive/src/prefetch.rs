//! Batched read-ahead for newest-first history walks (ADR 0030).
//!
//! A walk needs, per changeset, the changeset itself, its generation (to
//! order it) and often its root tree. Read one at a time, that is several
//! metadata round trips per step. [`ReadAhead`] guesses which changesets the
//! walk will reach next and reads them, their generations, root trees and
//! skiplists with one batched call of each kind, issued together.
//!
//! The guesses never change what a walk returns: the walk keeps its own
//! heap and `seen` set and asks for data by id. Whatever a batch did not
//! supply (a miss, an unreadable row, a failed batch) is read again by the
//! same single-item call as before, so derivation and errors happen exactly
//! where they did.
//!
//! Guesses come from two sources. A fetched changeset's parents are needed
//! when it pops. Its skiplist names first-parent ancestors 1, 2, 4, ...
//! steps up, with their generations; once a run of consecutive ancestors has
//! been fetched, their skiplists name every ancestor of the next run, so a
//! long chain is read a window at a time rather than one level per round
//! trip.
//!
//! The path walk (`path_history`) does not use this: each changeset it
//! yields is found in the index at the previous one's parents, so there is
//! nothing to read ahead of, and batches of one were slower than plain reads.

use std::collections::hash_map::Entry;
use std::collections::{BinaryHeap, HashMap};

use leep_core::{Changeset, ChangesetId, Hash, Result};

use crate::derived::{decode_hash, decode_u64, DerivedKind, Generation, RootTree, Skiplist};
use crate::Repo;

/// Most changesets one batch asks for, bounding both the read-ahead past
/// the end of a limited walk and the size of one query.
pub(crate) const MAX_BATCH: usize = 256;
/// Most batches one read-ahead issues together, for a walk that expects to
/// visit several batches' worth, so that a long log waits for several round
/// trips at once and decodes their answers on several threads. A batch is four
/// queries; two leave half of a 16-connection pool to other requests.
const MAX_LANES: usize = 2;

enum State {
    /// Named by a fetched changeset, not yet asked for; with its generation
    /// if a skiplist gave it.
    Guessed(Option<u64>),
    /// Asked for; what the store had, until the walk takes it.
    Asked(Ahead),
}

#[derive(Default)]
struct Ahead {
    cs: Option<Changeset>,
    generation: Option<u64>,
    root: Option<Hash>,
    /// The walk has taken this changeset.
    taken: bool,
}

pub(crate) struct ReadAhead {
    roots: bool,
    known: HashMap<ChangesetId, State, crate::repo::ByKey>,
    /// Guesses not yet asked for, newest (best estimate) first.
    guesses: BinaryHeap<(u64, ChangesetId)>,
    /// Changesets asked for and not yet taken by the walk.
    waiting: usize,
}

impl ReadAhead {
    /// `roots`: also read root trees, for walks that report them.
    pub(crate) fn new(roots: bool) -> Self {
        ReadAhead {
            roots,
            known: HashMap::default(),
            guesses: BinaryHeap::new(),
            waiting: 0,
        }
    }

    /// Make sure `ids` have been asked for. If any has not, read a batch of
    /// them plus the best guesses, up to `want` changesets (the number the
    /// walk still expects to visit) or [`MAX_BATCH`].
    ///
    /// `at` is the generation the walk has reached. A changeset of generation
    /// `at - want` or below cannot be among the next `want` visited: the
    /// longest chain below the walk's newest pending changeset has one
    /// unvisited ancestor at every generation in between. Guesses that low
    /// wait for a later batch.
    pub(crate) async fn ensure(&mut self, repo: &Repo, ids: &[ChangesetId], at: u64, want: usize) {
        self.ensure_skipping(repo, ids, at, want, |_| false).await
    }

    /// [`ReadAhead::ensure`], leaving out guesses for which `skip` says the
    /// walk already has what it needs.
    pub(crate) async fn ensure_skipping(
        &mut self,
        repo: &Repo,
        ids: &[ChangesetId],
        at: u64,
        want: usize,
        skip: impl Fn(ChangesetId) -> bool,
    ) {
        let (mut batch, mut gens) = (Vec::new(), Vec::new());
        for &id in ids {
            if let Some(g) = self.ask(repo, id) {
                batch.push(id);
                gens.push(g);
            }
        }
        if batch.is_empty() {
            return;
        }
        let window = want.clamp(1, MAX_BATCH);
        let lanes = want.div_ceil(MAX_BATCH).clamp(1, MAX_LANES);
        let floor = at.saturating_sub(u64::try_from(want).unwrap_or(u64::MAX));
        let mut batches = vec![(batch, gens)];
        // Room for the extra batches: what the walk expects to visit, less
        // what it has been given and not reached. The generation floor alone
        // admits several times `want` changesets where branches run side by
        // side.
        let mut room = 0usize;
        'fill: loop {
            let first = batches.len() == 1;
            let (batch, gens) = batches.last_mut().expect("one batch at least");
            while batch.len() < window && (first || room > 0) {
                match self.guesses.peek() {
                    Some(&(g, _)) if g > floor => {}
                    _ => break 'fill,
                }
                let (_, id) = self.guesses.pop().expect("peeked");
                if skip(id) {
                    continue;
                }
                if let Some(g) = self.ask(repo, id) {
                    batch.push(id);
                    gens.push(g);
                    room = room.saturating_sub(usize::from(!first));
                }
            }
            if first {
                room = want.saturating_sub(self.waiting);
            }
            if batches.len() == lanes || room == 0 {
                break;
            }
            batches.push((Vec::new(), Vec::new()));
        }
        batches.retain(|(batch, _)| !batch.is_empty());
        let roots = self.roots;
        if let [_] = batches[..] {
            let (batch, gens) = batches.pop().expect("one batch");
            let read = read_batch(repo.clone(), batch, gens, roots).await;
            self.apply(read, window);
            return;
        }
        // Each batch on its own task, so that decoding the answers, which
        // costs about as much as waiting for them, runs on several threads.
        let tasks: Vec<_> = batches
            .into_iter()
            .map(|(batch, gens)| tokio::spawn(read_batch(repo.clone(), batch, gens, roots)))
            .collect();
        for task in tasks {
            // A batch whose task failed only loses its read-ahead, like a
            // failed read: its items are read singly.
            if let Ok(read) = task.await {
                self.apply(read, window * lanes);
            }
        }
    }

    /// Mark `id` asked for, and return the generation a skiplist or the
    /// ancestry cache already gave, which is not read again; `None` if it was
    /// asked for before.
    fn ask(&mut self, repo: &Repo, id: ChangesetId) -> Option<Option<u64>> {
        let state = self.known.entry(id).or_insert(State::Guessed(None));
        let known = match state {
            State::Asked(_) => return None,
            State::Guessed(g) => *g,
        };
        *state = State::Asked(Ahead::default());
        self.waiting += 1;
        Some(known.or_else(|| repo.cached_generation(id)))
    }

    /// Record what [`read_batch`] got, and queue what it names: the parents
    /// of each changeset, and its skiplist's ancestors up to `reach` steps.
    fn apply(&mut self, read: Read, reach: usize) {
        let Read {
            batch,
            gens,
            css,
            roots,
            skips,
        } = read;
        let mut css = css.map(|v| v.into_iter());
        for (i, id) in batch.iter().enumerate() {
            let cs = css.as_mut().and_then(|it| it.next()).flatten();
            let generation = gens[i];
            let root = roots
                .as_ref()
                .and_then(|v| v[i].as_deref())
                .and_then(|b| decode_hash(b).ok());
            if let Some(cs) = &cs {
                let below = generation.map_or(0, |g| g.saturating_sub(1));
                for &p in &cs.parents {
                    self.guess(below, None, p);
                }
            }
            // Entry k is 2^k steps up; farther ones are guessed again from
            // nearer changesets' skiplists once the walk gets there.
            if let Some(list) = skips
                .as_ref()
                .and_then(|v| v[i].as_deref())
                .and_then(|b| Skiplist::decode(b).ok())
            {
                for (k, e) in list.iter().enumerate() {
                    if (1usize << k.min(63)) > reach {
                        break;
                    }
                    self.guess(e.generation, Some(e.generation), e.id);
                }
            }
            if let Some(State::Asked(a)) = self.known.get_mut(id) {
                *a = Ahead {
                    cs,
                    generation,
                    root,
                    taken: false,
                };
            }
        }
    }

    /// Queue `id` at `estimate`; `exact` is its generation when known.
    fn guess(&mut self, estimate: u64, exact: Option<u64>, id: ChangesetId) {
        match self.known.entry(id) {
            Entry::Vacant(e) => {
                e.insert(State::Guessed(exact));
                self.guesses.push((estimate, id));
            }
            Entry::Occupied(mut e) => {
                if let (State::Guessed(g @ None), Some(_)) = (e.get_mut(), exact) {
                    *g = exact;
                }
            }
        }
    }

    /// Generation of `id`, read ahead or else by [`Repo::generation`].
    pub(crate) async fn generation(&self, repo: &Repo, id: ChangesetId) -> Result<u64> {
        match self.known.get(&id) {
            Some(State::Asked(Ahead {
                generation: Some(g),
                ..
            })) => Ok(*g),
            _ => repo.generation(id).await,
        }
    }

    /// The changeset `id` and its root tree if read ahead, consuming them;
    /// each changeset is visited once.
    pub(crate) fn take(&mut self, id: ChangesetId) -> (Option<Changeset>, Option<Hash>) {
        match self.known.get_mut(&id) {
            Some(State::Asked(a)) => {
                let was = std::mem::take(a);
                if !was.taken {
                    self.waiting -= 1;
                }
                a.taken = true;
                (was.cs, was.root)
            }
            _ => (None, None),
        }
    }
}

/// One batch as the store answered it; a failed or malformed answer of a kind
/// is `None`, and the walk reads those items singly.
struct Read {
    batch: Vec<ChangesetId>,
    gens: Vec<Option<u64>>,
    css: Option<Vec<Option<Changeset>>>,
    roots: Option<Vec<Option<bytes::Bytes>>>,
    skips: Option<Vec<Option<bytes::Bytes>>>,
}

/// Read `batch`'s changesets, the generations `gens` lacks, root trees (with
/// `want_roots`) and skiplists, one batched call of each kind, together.
async fn read_batch(
    repo: Repo,
    batch: Vec<ChangesetId>,
    mut gens: Vec<Option<u64>>,
    want_roots: bool,
) -> Read {
    let meta = repo.meta();
    let keys: Vec<Hash> = batch.iter().map(|id| id.0).collect();
    let unknown: Vec<usize> = (0..batch.len()).filter(|&i| gens[i].is_none()).collect();
    let gen_keys: Vec<Hash> = unknown.iter().map(|&i| keys[i]).collect();
    let (css, read, roots, skips) = tokio::join!(
        meta.get_changesets(&batch),
        async {
            if gen_keys.is_empty() {
                Ok(Vec::new())
            } else {
                meta.get_derived_many(Generation::NAME, &gen_keys).await
            }
        },
        async {
            if want_roots {
                meta.get_derived_many(RootTree::NAME, &keys).await
            } else {
                Ok(Vec::new())
            }
        },
        meta.get_derived_many(Skiplist::NAME, &keys),
    );
    let n = batch.len();
    for (&i, v) in unknown
        .iter()
        .zip(slot(read, unknown.len()).unwrap_or_default())
    {
        gens[i] = v.as_deref().and_then(|b| decode_u64(b).ok());
    }
    Read {
        css: slot(css, n),
        roots: slot(roots, n),
        skips: slot(skips, n),
        batch,
        gens,
    }
}

/// A batch answer, or `None` if it failed or has the wrong number of slots.
fn slot<T>(r: Result<Vec<T>>, n: usize) -> Option<Vec<T>> {
    r.ok().filter(|v| v.len() == n)
}
