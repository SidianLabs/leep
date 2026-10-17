//! `land`: replay unlanded changesets onto the ref head and compare-and-swap,
//! no merge queue. Concurrent lands of one ref in one process are grouped
//! into batches that move the ref once (ADR 0032).

use std::collections::hash_map::RandomState;
use std::collections::{HashMap, HashSet, VecDeque};
use std::hash::BuildHasher;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use leep_core::api::{LandResponse, RebaseResponse};
use leep_core::refname::{self, MISSING_REF_MARKER, NOT_FORWARD_MARKER};
use leep_core::{ChangesetId, Error, RefStep, RefUpdateContext, Result};
use tokio::sync::oneshot;

use crate::Repo;

/// Lands within one process are batched per ref, so losses only come from
/// other server instances sharing the store; this bounds those retries.
const MAX_LAND_ATTEMPTS: u32 = 32;
const BACKOFF_CAP_MS: u64 = 200;
/// Consecutive failed commits tolerated before the error is returned. Each is
/// re-driven from a fresh read, which resolves an ambiguous commit.
const MAX_COMMIT_ERRORS: u32 = 3;
/// Changesets examined when looking for an earlier landing of the same stack.
const LANDED_COPY_SCAN: usize = 1024;
/// Lands one batch takes at most. It bounds the commit (one ref log row per
/// land) and how long a land waits behind replays of the lands queued before
/// it; the next batch starts as soon as this one commits.
pub(crate) const MAX_BATCH: usize = 64;

/// Full-jitter exponential backoff, so racing servers spread out instead of
/// colliding again in lockstep.
fn backoff(attempt: u32) -> Duration {
    let cap = (1u64 << attempt.min(8)).min(BACKOFF_CAP_MS);
    Duration::from_millis(RandomState::new().hash_one(attempt) % (cap + 1))
}

/// Changesets that passed the land policy, and the head they passed at.
#[derive(Default)]
struct Checked {
    head: Option<Option<ChangesetId>>,
    ids: HashSet<ChangesetId>,
}

/// What one land call saw, for metrics only.
#[derive(Default)]
struct LandStats {
    attempts: u32,
    exhausted: bool,
}

impl LandStats {
    fn record(&self, result: &Result<LandResponse>, elapsed: Duration) {
        let outcome = match result {
            Ok(LandResponse::Landed { .. }) => "landed",
            Ok(LandResponse::Conflicts { .. }) => "conflicts",
            Err(_) if self.exhausted => "exhausted",
            Err(_) => "error",
        };
        metrics::counter!("leep_land_total", "outcome" => outcome).increment(1);
        metrics::histogram!("leep_land_duration_seconds").record(elapsed.as_secs_f64());
        if self.attempts > 0 {
            metrics::histogram!("leep_land_attempts").record(f64::from(self.attempts));
        }
    }
}

/// The land queues of one process, one per target ref.
#[derive(Default)]
pub(crate) struct LandQueues {
    queues: Mutex<HashMap<String, Arc<Mutex<RefQueue>>>>,
    /// The lands each batch took, in order, for tests to replay one by one.
    #[cfg(test)]
    pub(crate) batches: Mutex<Vec<Vec<ChangesetId>>>,
}

impl LandQueues {
    fn queue(&self, target_ref: &str) -> Arc<Mutex<RefQueue>> {
        let mut queues = self.queues.lock().unwrap_or_else(|e| e.into_inner());
        Arc::clone(queues.entry(target_ref.to_string()).or_default())
    }

    /// Lands waiting for a batch on `target_ref`.
    #[cfg(test)]
    pub(crate) fn pending(&self, target_ref: &str) -> usize {
        lock(&self.queue(target_ref)).pending.len()
    }
}

#[derive(Default)]
struct RefQueue {
    pending: VecDeque<Queued>,
    /// A leader task is draining `pending`; whoever finds none starts one.
    leading: bool,
}

fn lock(queue: &Mutex<RefQueue>) -> MutexGuard<'_, RefQueue> {
    queue.lock().unwrap_or_else(|e| e.into_inner())
}

/// A land waiting for its batch.
struct Queued {
    /// The caller's handle: same stores, and the land policy the request
    /// arrived under.
    repo: Repo,
    cs: ChangesetId,
    /// Whether the land may create the ref.
    create: bool,
    ctx: RefUpdateContext,
    queued: Instant,
    reply: oneshot::Sender<Answer>,
}

struct Answer {
    result: Result<LandResponse>,
    attempts: u32,
    exhausted: bool,
}

/// A land in a batch, kept across the batch's attempts.
struct InFlight {
    q: Queued,
    /// A changeset that passed the policy need not be checked again when a
    /// lost CAS redoes the batch, unless the policy reads the head.
    checked: Checked,
}

impl InFlight {
    fn answer(self, result: Result<LandResponse>, attempts: u32, exhausted: bool) {
        // A caller that went away no longer wants the answer.
        let _ = self.q.reply.send(Answer {
            result,
            attempts,
            exhausted,
        });
    }
}

/// One land's part of a batch attempt, computed on the head left by the
/// lands before it.
enum Step {
    /// Answered without moving the ref: already landed, or conflicts.
    Done(LandResponse),
    /// The ref moves to this changeset.
    Move(ChangesetId),
}

fn is_move(step: &Result<Step>) -> bool {
    matches!(step, Ok(Step::Move(_)))
}

/// The same error for another caller (`Error` is not `Clone`).
fn duplicate(e: &Error) -> Error {
    match e {
        Error::InvalidPath(p, why) => Error::InvalidPath(p.clone(), why),
        Error::InvalidHash(m) => Error::InvalidHash(m.clone()),
        Error::Invalid(m) => Error::Invalid(m.clone()),
        Error::Conflict(m) => Error::Conflict(m.clone()),
        Error::NotFound(m) => Error::NotFound(m.clone()),
        Error::Encoding(m) => Error::Encoding(m.clone()),
        Error::Storage(m) => Error::Storage(m.clone()),
        Error::Other(m) => Error::Other(m.clone()),
    }
}

/// Clears the queue if the leader stops without draining it (a panic), so
/// waiting lands get an error instead of waiting forever and the next land
/// starts a new leader.
struct Leading {
    queue: Arc<Mutex<RefQueue>>,
    done: bool,
}

impl Drop for Leading {
    fn drop(&mut self) {
        if !self.done {
            let mut q = lock(&self.queue);
            q.leading = false;
            q.pending.clear();
        }
    }
}

/// Run batches until the queue is empty. Spawned, so a cancelled request
/// never cancels the batch the other requests wait on.
async fn lead(repo: Repo, target_ref: String, queue: Arc<Mutex<RefQueue>>) {
    let mut leading = Leading {
        queue: Arc::clone(&queue),
        done: false,
    };
    loop {
        let batch: Vec<Queued> = {
            let mut q = lock(&queue);
            if q.pending.is_empty() {
                q.leading = false;
                leading.done = true;
                return;
            }
            let n = q.pending.len().min(MAX_BATCH);
            q.pending.drain(..n).collect()
        };
        repo.land_batch(&target_ref, batch).await;
    }
}

impl Repo {
    /// Land `cs` on `target_ref`. Fast-forwards when the head is an ancestor;
    /// otherwise replays the unlanded stack under `cs` onto the head. Retries
    /// when the CAS loses; conflicts are returned, never resolved here.
    ///
    /// Concurrent lands on the same ref in this process are group committed:
    /// they queue, and one leader replays a batch of them in arrival order,
    /// each onto the result of the one before, then moves the ref once with a
    /// log entry per land. Each land's answer is the one it would get if the
    /// lands ran one after another in that order. The CAS still guards
    /// against other processes: a lost one redoes the batch from the new
    /// head, with jittered backoff between losses.
    ///
    /// A land cancelled while queued is dropped from its batch; once the
    /// batch has replayed it, it stays in the batch and lands.
    pub async fn land(&self, cs: ChangesetId, target_ref: &str) -> Result<LandResponse> {
        self.land_as(cs, target_ref, None).await
    }

    /// [`land`](Self::land) recording `actor` (the caller's token name) in the
    /// ref log entry of the move.
    pub async fn land_as(
        &self,
        cs: ChangesetId,
        target_ref: &str,
        actor: Option<&str>,
    ) -> Result<LandResponse> {
        self.land_with(cs, target_ref, actor, true).await
    }

    /// [`land_as`](Self::land_as), refusing with `NotFound` when `create` is
    /// false and `target_ref` does not exist at the head the land reads, so a
    /// typo cannot make a ref (ADR 0035). The check is made where the head
    /// is read for the compare-and-swap, so a ref deleted meanwhile is not
    /// silently recreated.
    pub async fn land_with(
        &self,
        cs: ChangesetId,
        target_ref: &str,
        actor: Option<&str>,
        create: bool,
    ) -> Result<LandResponse> {
        let started = Instant::now();
        let mut stats = LandStats::default();
        let ctx = RefUpdateContext::new("land")
            .actor(actor)
            .message(format!("landed {cs}"));
        let result = self
            .land_inner(cs, target_ref, create, ctx, &mut stats)
            .await;
        stats.record(&result, started.elapsed());
        result
    }

    async fn land_inner(
        &self,
        cs: ChangesetId,
        target_ref: &str,
        create: bool,
        ctx: RefUpdateContext,
        stats: &mut LandStats,
    ) -> Result<LandResponse> {
        refname::validate(target_ref)?;
        if refname::is_tag(target_ref) {
            return Err(Error::Invalid(format!(
                "cannot land onto {target_ref:?}: tags are immutable"
            )));
        }
        self.get_changeset(cs).await?;
        let (reply, answer) = oneshot::channel();
        let queue = self.land_queues.queue(target_ref);
        let start_leader = {
            let mut q = lock(&queue);
            q.pending.push_back(Queued {
                repo: self.clone(),
                cs,
                create,
                ctx,
                queued: Instant::now(),
                reply,
            });
            !std::mem::replace(&mut q.leading, true)
        };
        if start_leader {
            tokio::spawn(lead(self.clone(), target_ref.to_string(), queue));
        }
        let answer = answer.await.map_err(|_| {
            Error::Other(format!(
                "land onto {target_ref:?} was abandoned before it finished; retry"
            ))
        })?;
        stats.attempts = answer.attempts;
        stats.exhausted = answer.exhausted;
        answer.result
    }

    /// Land a batch: replay each land onto the head the previous one left,
    /// then move the ref once. Lands answered before anything in the batch
    /// moved the ref are answered at once, as a lone land would be.
    async fn land_batch(&self, target_ref: &str, batch: Vec<Queued>) {
        let mut lands: Vec<InFlight> = batch
            .into_iter()
            .map(|q| {
                metrics::histogram!("leep_land_lock_wait_seconds")
                    .record(q.queued.elapsed().as_secs_f64());
                InFlight {
                    q,
                    checked: Checked::default(),
                }
            })
            .collect();
        let mut commit_errors = 0;
        let mut last_error = None;
        for attempt in 1..=MAX_LAND_ATTEMPTS {
            let start = match self.meta().get_ref(target_ref).await {
                Ok(h) => h,
                Err(e) => {
                    for l in lands {
                        l.answer(Err(duplicate(&e)), attempt, false);
                    }
                    return;
                }
            };
            let mut head = start;
            // Origins read by this attempt's landed-copy scans: the lands
            // of a batch walk the same stretch of history.
            let mut origins = HashMap::new();
            let mut steps: Vec<RefStep> = Vec::new();
            let mut waiting: Vec<(InFlight, Result<Step>)> = Vec::new();
            let mut taken = Vec::new();
            for mut l in std::mem::take(&mut lands) {
                if attempt == 1 && l.q.reply.is_closed() {
                    metrics::counter!("leep_land_cancelled_total").increment(1);
                    continue;
                }
                taken.push(l.q.cs);
                let repo = l.q.repo.clone();
                let step = repo
                    .land_step(
                        l.q.cs,
                        l.q.create,
                        head,
                        target_ref,
                        attempt,
                        &mut l.checked,
                        &mut origins,
                    )
                    .await;
                match step {
                    Ok(Step::Move(new)) => {
                        steps.push(RefStep {
                            new: Some(new),
                            ctx: l.q.ctx.clone(),
                        });
                        head = Some(new);
                        waiting.push((l, Ok(Step::Move(new))));
                    }
                    Ok(Step::Done(resp)) if steps.is_empty() => l.answer(Ok(resp), attempt, false),
                    Err(e) if steps.is_empty() => l.answer(Err(e), attempt, false),
                    other => waiting.push((l, other)),
                }
            }
            if attempt == 1 && !taken.is_empty() {
                metrics::histogram!("leep_land_batch_size").record(taken.len() as f64);
                #[cfg(test)]
                self.land_queues.batches.lock().unwrap().push(taken);
            }
            if steps.is_empty() {
                return;
            }
            let applied = if let [step] = &steps[..] {
                self.meta()
                    .cas_ref_logged(target_ref, start, step.new, &step.ctx)
                    .await
                    .map(usize::from)
            } else {
                self.meta()
                    .cas_ref_logged_chain(target_ref, start, &steps)
                    .await
            };
            match applied {
                Ok(n) => {
                    if n < steps.len() {
                        commit_errors = 0;
                        metrics::counter!("leep_land_cas_losses_total").increment(1);
                    }
                    // Settle every land whose result sits on applied moves:
                    // all of them, unless a store without an atomic chain
                    // stopped part way.
                    let mut moves = 0;
                    for (l, step) in waiting {
                        moves += usize::from(is_move(&step));
                        if moves > n {
                            lands.push(l);
                            continue;
                        }
                        let cs = l.q.cs;
                        let result = step.map(|s| match s {
                            Step::Done(resp) => resp,
                            Step::Move(new) => LandResponse::Landed {
                                landed: new,
                                rebased: new != cs,
                                attempts: attempt,
                            },
                        });
                        l.answer(result, attempt, false);
                    }
                }
                // The commit may or may not have applied. Another pass reads
                // the head again and either finds each stack in it or replays.
                Err(e) => {
                    commit_errors += 1;
                    if commit_errors >= MAX_COMMIT_ERRORS {
                        for (l, _) in waiting {
                            l.answer(Err(duplicate(&e)), attempt, false);
                        }
                        return;
                    }
                    last_error = Some(e);
                    lands.extend(waiting.into_iter().map(|(l, _)| l));
                }
            }
            if lands.is_empty() {
                return;
            }
            tokio::time::sleep(backoff(attempt)).await;
        }
        for l in lands {
            match &last_error {
                Some(e) => l.answer(Err(duplicate(e)), MAX_LAND_ATTEMPTS, false),
                None => l.answer(
                    Err(Error::Conflict(format!(
                        "land onto {target_ref:?} lost the race {MAX_LAND_ATTEMPTS} times; retry"
                    ))),
                    MAX_LAND_ATTEMPTS,
                    true,
                ),
            }
        }
    }

    /// What landing `cs` does on top of `head`: what one attempt of a lone
    /// land did between reading the head and swapping it.
    #[allow(clippy::too_many_arguments)]
    async fn land_step(
        &self,
        cs: ChangesetId,
        create: bool,
        head: Option<ChangesetId>,
        target_ref: &str,
        attempt: u32,
        checked: &mut Checked,
        origins: &mut HashMap<ChangesetId, Option<ChangesetId>>,
    ) -> Result<Step> {
        if head.is_none() && !create {
            return Err(Error::NotFound(format!(
                "ref {target_ref:?} {MISSING_REF_MARKER}"
            )));
        }
        let mut fast_forward = false;
        if let Some(h) = head {
            if h == cs || self.is_ancestor(cs, h).await? {
                return Ok(Step::Done(LandResponse::Landed {
                    landed: cs,
                    rebased: false,
                    attempts: attempt,
                }));
            }
            // Head is an ancestor (directly or through a stack of unlanded
            // changesets): fast-forward.
            fast_forward = self.is_ancestor(h, cs).await?;
            // An earlier call may have landed this stack and lost its answer;
            // replaying it would add the stack a second time.
            if !fast_forward {
                if let Some(copy) = self.landed_copy(cs, h, origins).await? {
                    return Ok(Step::Done(LandResponse::Landed {
                        landed: copy,
                        rebased: true,
                        attempts: attempt,
                    }));
                }
            }
        }
        self.enforce_land_policy(cs, head, target_ref, checked)
            .await?;
        match head {
            Some(h) if !fast_forward => match self.replay_stack(cs, h).await? {
                Ok(id) => Ok(Step::Move(id)),
                Err(resp) => Ok(Step::Done(resp)),
            },
            _ => Ok(Step::Move(cs)),
        }
    }

    /// The server's rebased copy of `cs` among `head`'s ancestors, if an
    /// earlier land of `cs` already put one there. Only rows written by
    /// `rebase` count, so a changeset that merely claims `leep.rebased_from`
    /// cannot make a land look done.
    ///
    /// A copy sits on a head that was not below `cs`, so its generation is
    /// above `cs`'s; the walk stops at that bound, and after
    /// [`LANDED_COPY_SCAN`] changesets, past which a retry may land twice.
    ///
    /// `origins` remembers the rows read, for the other lands of a batch.
    async fn landed_copy(
        &self,
        cs: ChangesetId,
        head: ChangesetId,
        origins: &mut HashMap<ChangesetId, Option<ChangesetId>>,
    ) -> Result<Option<ChangesetId>> {
        let floor = self.generation(cs).await?;
        let mut seen = HashSet::from([head]);
        let mut stack = vec![head];
        let mut scanned = 0;
        while let Some(id) = stack.pop() {
            let origin = match origins.get(&id) {
                Some(o) => *o,
                None => {
                    let o = self.rebased_origin(id).await?;
                    origins.insert(id, o);
                    o
                }
            };
            if origin == Some(cs) {
                return Ok(Some(id));
            }
            scanned += 1;
            if scanned >= LANDED_COPY_SCAN {
                break;
            }
            for &p in self.parents(id).await?.iter() {
                if seen.insert(p) && self.generation(p).await? > floor {
                    stack.push(p);
                }
            }
        }
        Ok(None)
    }

    /// `MetaStore::cas_ref` for callers outside `land`, with the ref rules and
    /// land policy applied, so a raw ref update cannot skip them: names must
    /// be valid, a tag can be created or deleted but never moved, a ref a
    /// policy protects cannot be deleted or moved anywhere but forward (a
    /// rewind would drop reviewed or signed history without any review), and
    /// the policy checks what the ref would gain. A refused move records
    /// nothing in the ref log; `ctx` is stored with the entry of one that
    /// happens.
    pub async fn cas_ref_checked(
        &self,
        name: &str,
        expected: Option<ChangesetId>,
        new: Option<ChangesetId>,
        ctx: &RefUpdateContext,
    ) -> Result<bool> {
        // Deletes skip the name check so a ref that predates the validator
        // can still be removed.
        if new.is_some() {
            refname::validate(name)?;
        }
        if refname::is_tag(name) && expected.is_some() && new.is_some() {
            return Err(Error::Invalid(format!(
                "tag {name:?} is immutable and cannot be moved; delete it and create it again"
            )));
        }
        let protected = self
            .land_policy
            .as_ref()
            .is_some_and(|p| p.applies_to(name));
        match new {
            Some(new) => {
                // `expected` is the current value whenever the swap can win.
                if let Some(old) = expected.filter(|_| protected) {
                    if !self.is_ancestor(old, new).await? {
                        return Err(Error::Invalid(format!(
                            "cannot move {name:?} from {old} to {new}: it is protected by a \
                             land policy and {NOT_FORWARD_MARKER}; to undo a change, revert \
                             it and land the revert"
                        )));
                    }
                }
                self.enforce_land_policy(new, expected, name, &mut Checked::default())
                    .await?;
            }
            None => {
                if protected {
                    return Err(Error::Invalid(format!(
                        "cannot delete {name:?}: it is protected by a land policy"
                    )));
                }
            }
        }
        self.meta().cas_ref_logged(name, expected, new, ctx).await
    }

    /// Ask the policy about the submitted changesets this land would add to
    /// the ref. Skipped when the ref already contains `cs`.
    async fn enforce_land_policy(
        &self,
        cs: ChangesetId,
        head: Option<ChangesetId>,
        target_ref: &str,
        checked: &mut Checked,
    ) -> Result<()> {
        let Some(policy) = self
            .land_policy
            .as_ref()
            .filter(|p| p.applies_to(target_ref))
        else {
            return Ok(());
        };
        if policy.depends_on_head() && checked.head != Some(head) {
            checked.ids.clear();
        }
        checked.head = Some(head);
        let fresh: Vec<_> = self
            .new_history(cs, head)
            .await?
            .into_iter()
            .filter(|id| !checked.ids.contains(id))
            .collect();
        if fresh.is_empty() {
            return Ok(());
        }
        policy.check_land(self, target_ref, head, &fresh).await?;
        checked.ids.extend(fresh);
        Ok(())
    }

    /// Rebase every changeset of the stack under `cs` onto `head`, oldest
    /// first. The stack is the first-parent chain from `cs` down to (not
    /// including) the first changeset already in `head`'s history. Replaying
    /// only `cs` would silently drop the rest of the stack.
    ///
    /// On conflict, returns the whole stack squashed into one three-way merge
    /// relative to `head`, so the client can resolve it in a single commit.
    async fn replay_stack(
        &self,
        cs: ChangesetId,
        head: ChangesetId,
    ) -> Result<Result<ChangesetId, LandResponse>> {
        let mut stack = Vec::new();
        let mut fork = None;
        let mut cur = cs;
        loop {
            let c = self.get_changeset(cur).await?;
            if c.parents.len() > 1 {
                return Err(Error::Invalid(format!(
                    "cannot land {cs}: unlanded changeset {cur} is a merge; merge {head} into it instead"
                )));
            }
            stack.push(cur);
            match c.parents.first() {
                Some(&p) if self.is_ancestor(p, head).await? => {
                    fork = Some(p);
                    break;
                }
                Some(&p) => cur = p,
                None => break,
            }
        }

        let mut onto = head;
        for &id in stack.iter().rev() {
            match self.rebase(id, onto).await? {
                RebaseResponse::Rebased { id } => onto = id,
                RebaseResponse::Conflicts { .. } => {
                    let m = self
                        .merge_trees(
                            self.tree_or_empty(fork).await?,
                            self.root_tree(head).await?,
                            self.root_tree(cs).await?,
                        )
                        .await?;
                    return Ok(Err(LandResponse::Conflicts {
                        onto: head,
                        conflicts: m.conflicts,
                        clean: m.changes,
                    }));
                }
            }
        }
        Ok(Ok(onto))
    }
}
