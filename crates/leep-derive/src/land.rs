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
use leep_core::{
    ChangesetId, Error, LandId, LandKey, LandRecord, RefStep, RefUpdateContext, Result,
};
use tokio::sync::oneshot;

use crate::changeset_gc::RescueBy;
use crate::land_stage::Staging;
use crate::Repo;

/// Lands within one process are batched per ref, so losses only come from
/// other server instances sharing the store; this bounds those retries.
const MAX_LAND_ATTEMPTS: u32 = 32;
const BACKOFF_CAP_MS: u64 = 200;
/// Consecutive failed commits tolerated before the error is returned. Each is
/// re-driven from a fresh read, which resolves an ambiguous commit.
const MAX_COMMIT_ERRORS: u32 = 3;
/// Changesets examined when looking for an earlier landing of the same stack.
pub(crate) const LANDED_COPY_SCAN: usize = 1024;
/// Lands one batch takes at most. It bounds the commit (one ref log row per
/// land) and how long a land waits behind replays of the lands queued before
/// it; the next batch starts as soon as this one commits.
pub(crate) const MAX_BATCH: usize = 64;
/// How long a batch may keep trying, from its first attempt. A batch runs
/// detached from the requests that asked for it, so this bounds how long
/// after its last fence read it can still move a ref, which the changeset
/// sweep's fence must outlast (ADR 0059 section 3). A batch past it answers
/// its lands with a retryable error; keyed ones are then answered from their
/// record, so none lands twice.
pub(crate) const MAX_BATCH_LIFETIME: Duration = Duration::from_secs(60);

/// The answer for the lands of a batch past [`MAX_BATCH_LIFETIME`].
fn past_deadline(target_ref: &str) -> Error {
    metrics::counter!("leep_land_batch_deadline_total").increment(1);
    Error::Conflict(format!(
        "land onto {target_ref:?} did not finish within {}s; retry",
        MAX_BATCH_LIFETIME.as_secs()
    ))
}

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
    /// Leader tasks, so a test can kill a simulated server mid-batch.
    #[cfg(test)]
    tasks: Mutex<Vec<tokio::task::AbortHandle>>,
    /// The lands each batch took, in order, for tests to replay one by one.
    #[cfg(test)]
    pub(crate) batches: Mutex<Vec<Vec<ChangesetId>>>,
}

impl LandQueues {
    fn queue(&self, target_ref: &str) -> Arc<Mutex<RefQueue>> {
        let mut queues = self.queues.lock().unwrap_or_else(|e| e.into_inner());
        Arc::clone(queues.entry(target_ref.to_string()).or_default())
    }

    #[cfg(test)]
    fn track(&self, task: tokio::task::AbortHandle) {
        self.tasks.lock().unwrap().push(task);
    }

    #[cfg(not(test))]
    fn track(&self, _: tokio::task::AbortHandle) {}

    /// Abort every leader task, as a killed process would stop them.
    #[cfg(test)]
    pub(crate) fn abort_leaders(&self) {
        for t in self.tasks.lock().unwrap().drain(..) {
            t.abort();
        }
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
    /// Queued because this server holds the ref's land lease (ADR 0056):
    /// handed back to be routed again if the lease is lost.
    led: bool,
}

struct Answer {
    result: Result<LandResponse>,
    attempts: u32,
    exhausted: bool,
    /// This server stopped coordinating the ref before the land was
    /// replayed; route it again.
    rerouted: bool,
}

/// How a queued land left its queue.
enum Settled {
    Answered(Result<LandResponse>),
    Rerouted,
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
            rerouted: false,
        });
    }

    fn reroute(self) {
        let _ = self.q.reply.send(Answer {
            result: Err(Error::Other("rerouted".into())),
            attempts: 0,
            exhausted: false,
            rerouted: true,
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

/// The answer for a land whose id the ref already recorded: that land's
/// result, or `Invalid` if the id named another changeset. `None` when the
/// land must run.
fn recorded_step(
    q: &Queued,
    recorded: &Result<HashMap<LandId, LandRecord>>,
    attempt: u32,
) -> Option<Result<Step>> {
    let key = q.ctx.land.as_ref()?;
    let found = match recorded {
        Ok(found) => found.get(&key.id)?,
        Err(e) => return Some(Err(duplicate(e))),
    };
    if found.changeset != key.changeset {
        return Some(Err(Error::Invalid(format!(
            "land id {} already landed {}; use a new land id to land {}",
            key.id, found.changeset, key.changeset
        ))));
    }
    metrics::counter!("leep_land_recorded_total").increment(1);
    metrics::counter!("leep_land_answered_from_record_total", "original" => "present").increment(1);
    Some(Ok(Step::Done(LandResponse::Landed {
        landed: found.landed,
        rebased: found.landed != key.changeset,
        attempts: attempt,
    })))
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
        Error::Indexing {
            kind,
            behind,
            retry_after_ms,
        } => Error::Indexing {
            kind: kind.clone(),
            behind: *behind,
            retry_after_ms: *retry_after_ms,
        },
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
    /// typo cannot make a ref (ADR 0036). The check is made where the head
    /// is read for the compare-and-swap, so a ref deleted meanwhile is not
    /// silently recreated.
    pub async fn land_with(
        &self,
        cs: ChangesetId,
        target_ref: &str,
        actor: Option<&str>,
        create: bool,
    ) -> Result<LandResponse> {
        self.land_keyed(cs, target_ref, actor, create, None).await
    }

    /// [`land_with`](Self::land_with) under the client's `land_id`
    /// (ADR 0057): the move records it, and a land whose id the ref already
    /// recorded answers with that land's result instead of landing again,
    /// however many lands came after it.
    pub async fn land_keyed(
        &self,
        cs: ChangesetId,
        target_ref: &str,
        actor: Option<&str>,
        create: bool,
        land_id: Option<LandId>,
    ) -> Result<LandResponse> {
        self.land_queued(cs, target_ref, actor, create, land_id, false)
            .await
            .unwrap_or_else(|| {
                Err(Error::Other(
                    "land was handed back unexpectedly; retry".into(),
                ))
            })
    }

    /// [`land_keyed`](Self::land_keyed) for a server holding `target_ref`'s
    /// land lease (ADR 0056): `None` when it lost the lease before the land
    /// was replayed, and the caller must route the land again.
    pub(crate) async fn land_led(
        &self,
        cs: ChangesetId,
        target_ref: &str,
        actor: Option<&str>,
        create: bool,
        land_id: Option<LandId>,
    ) -> Option<Result<LandResponse>> {
        self.land_queued(cs, target_ref, actor, create, land_id, true)
            .await
    }

    async fn land_queued(
        &self,
        cs: ChangesetId,
        target_ref: &str,
        actor: Option<&str>,
        create: bool,
        land_id: Option<LandId>,
        led: bool,
    ) -> Option<Result<LandResponse>> {
        let started = Instant::now();
        let mut stats = LandStats::default();
        let ctx = RefUpdateContext::new("land")
            .actor(actor)
            .message(format!("landed {cs}"))
            .land(land_id.map(|id| LandKey { id, changeset: cs }));
        match self
            .land_inner(cs, target_ref, create, ctx, &mut stats, led)
            .await
        {
            Settled::Answered(result) => {
                stats.record(&result, started.elapsed());
                Some(result)
            }
            Settled::Rerouted => None,
        }
    }

    async fn land_inner(
        &self,
        cs: ChangesetId,
        target_ref: &str,
        create: bool,
        ctx: RefUpdateContext,
        stats: &mut LandStats,
        led: bool,
    ) -> Settled {
        match self
            .land_enqueue(cs, target_ref, create, ctx, stats, led)
            .await
        {
            Ok(Some(answer)) => Settled::Answered(answer),
            Ok(None) => Settled::Rerouted,
            Err(e) => Settled::Answered(Err(e)),
        }
    }

    async fn land_enqueue(
        &self,
        cs: ChangesetId,
        target_ref: &str,
        create: bool,
        ctx: RefUpdateContext,
        stats: &mut LandStats,
        led: bool,
    ) -> Result<Option<Result<LandResponse>>> {
        refname::validate(target_ref)?;
        if refname::is_tag(target_ref) {
            return Err(Error::Invalid(format!(
                "cannot land onto {target_ref:?}: tags are immutable"
            )));
        }
        match self.get_changeset(cs).await {
            Ok(_) => {}
            // A keyed retry whose original was garbage collected (ADR 0059
            // section 5): its record answers it. Present changesets, the
            // usual case, read nothing more.
            Err(Error::NotFound(m)) => {
                return match self.answer_from_record(cs, target_ref, &ctx).await? {
                    Some(answer) => Ok(Some(Ok(answer))),
                    None => Err(Error::NotFound(m)),
                }
            }
            Err(e) => return Err(e),
        }
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
                led,
            });
            !std::mem::replace(&mut q.leading, true)
        };
        if start_leader {
            let task = tokio::spawn(lead(self.clone(), target_ref.to_string(), queue));
            self.land_queues.track(task.abort_handle());
        }
        let answer = answer.await.map_err(|_| {
            Error::Other(format!(
                "land onto {target_ref:?} was abandoned before it finished; retry"
            ))
        })?;
        if answer.rerouted {
            return Ok(None);
        }
        stats.attempts = answer.attempts;
        stats.exhausted = answer.exhausted;
        Ok(Some(answer.result))
    }

    /// The result recorded for `ctx`'s land id, for a land whose changeset
    /// is not stored: `Ok(None)` without an id or a record, `Invalid` when
    /// the id landed another changeset. Moves nothing.
    async fn answer_from_record(
        &self,
        cs: ChangesetId,
        target_ref: &str,
        ctx: &RefUpdateContext,
    ) -> Result<Option<LandResponse>> {
        let Some(key) = &ctx.land else {
            return Ok(None);
        };
        let found = self
            .meta()
            .find_lands(target_ref, std::slice::from_ref(&key.id))
            .await?;
        let Some(record) = found.into_iter().next().flatten() else {
            return Ok(None);
        };
        if record.changeset != cs {
            return Err(Error::Invalid(format!(
                "land id {} already landed {}; use a new land id to land {cs}",
                key.id, record.changeset
            )));
        }
        metrics::counter!("leep_land_answered_from_record_total", "original" => "missing")
            .increment(1);
        Ok(Some(LandResponse::Landed {
            landed: record.landed,
            rebased: record.landed != cs,
            attempts: 1,
        }))
    }

    /// Before an attempt replays a land, while a changeset sweep may have
    /// condemned something (ADR 0059): rescue the land's changeset and its
    /// unlanded stack, which the move will make reachable. The fence state
    /// of `cs` is read before anything else of it.
    async fn rescue_land(&self, cs: ChangesetId, head: Option<ChangesetId>) -> Result<()> {
        self.rescue_condemned(&[cs], RescueBy::Land).await?;
        let stack = self.new_history(cs, head).await?;
        self.rescue_condemned(&stack, RescueBy::Land).await
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
        self.learn_ancestry(&lands.iter().map(|l| l.q.cs).collect::<Vec<_>>())
            .await;
        let started = tokio::time::Instant::now();
        let mut commit_errors = 0;
        let mut lookup_errors = 0;
        let mut fence_errors = 0;
        let mut last_error = None;
        // `rebased-origin-v1` rows read by earlier attempts of this batch,
        // stored ones only (see `Staging::retain_stored`). A stored
        // changeset's row is written with it, before any ref can reach it,
        // and never changes, so a later attempt reads only the rows of the
        // head's new ancestors instead of scanning the whole stretch again.
        let mut carried: HashMap<ChangesetId, Option<ChangesetId>> = HashMap::new();
        for attempt in 1..=MAX_LAND_ATTEMPTS {
            if started.elapsed() >= MAX_BATCH_LIFETIME {
                for l in lands {
                    l.answer(Err(past_deadline(target_ref)), attempt, false);
                }
                return;
            }
            // A server that lost the ref's lease stops retrying: the lands
            // it was coordinating go back to be routed to the new holder.
            if let Some(coord) = &self.land_coordinator {
                if lands.iter().any(|l| l.q.led) && !coord.still_leading(target_ref).await {
                    let (gone, keep): (Vec<_>, Vec<_>) = std::mem::take(&mut lands)
                        .into_iter()
                        .partition(|l| l.q.led);
                    for l in gone {
                        l.reroute();
                    }
                    lands = keep;
                    if lands.is_empty() {
                        return;
                    }
                }
            }
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
            // The replays write here; it is stored just before the ref moves.
            let staging = Arc::new(Staging::default());
            let ids: Vec<ChangesetId> = lands.iter().map(|l| l.q.cs).collect();
            let staged = staging.repo(self);
            // Origins read by this attempt's landed-copy scans: the lands
            // of a batch walk the same stretch of history.
            let (fenced, mut recorded, _, mut origins) = tokio::join!(
                // Every attempt reads the sweep's flag afresh: the batch may
                // outlive the requests that read it (ADR 0059).
                self.changeset_fence_active(true),
                // After the head: a land recorded since would have moved it,
                // so the swap from `start` cannot repeat it.
                self.recorded_lands(target_ref, &lands),
                async {
                    // A lone land reads no more than the prefetch would.
                    if ids.len() > 1 {
                        staging.prefetch(self, &ids).await;
                    }
                },
                async {
                    let mut origins = std::mem::take(&mut carried);
                    if let Some(h) = start {
                        staged.prefetch_origins(h, &ids, &mut origins).await;
                    }
                    origins
                }
            );
            // A failed flag read holds back the whole attempt, until it has
            // failed as often as a commit may.
            let fenced = match fenced {
                Ok(f) => {
                    fence_errors = 0;
                    f
                }
                Err(e) => {
                    fence_errors += 1;
                    if fence_errors >= MAX_COMMIT_ERRORS {
                        for l in lands {
                            l.answer(Err(duplicate(&e)), attempt, false);
                        }
                        return;
                    }
                    carried = origins;
                    tokio::time::sleep(backoff(attempt)).await;
                    continue;
                }
            };
            // A failed land id lookup holds back only the lands with ids, to
            // the next attempt, until it has failed as often as a commit may.
            match &recorded {
                Ok(_) => lookup_errors = 0,
                Err(_) => lookup_errors += 1,
            }
            let hold_keyed = recorded.is_err() && lookup_errors < MAX_COMMIT_ERRORS;
            let mut steps: Vec<RefStep> = Vec::new();
            let mut waiting: Vec<(InFlight, Result<Step>)> = Vec::new();
            let mut taken = Vec::new();
            for mut l in std::mem::take(&mut lands) {
                if attempt == 1 && l.q.reply.is_closed() {
                    metrics::counter!("leep_land_cancelled_total").increment(1);
                    continue;
                }
                if hold_keyed && l.q.ctx.land.is_some() {
                    lands.push(l);
                    continue;
                }
                taken.push(l.q.cs);
                let repo = staging.repo(&l.q.repo);
                let step = match recorded_step(&l.q, &recorded, attempt) {
                    Some(step) => step,
                    None if fenced => match self.rescue_land(l.q.cs, start).await {
                        Ok(()) => {
                            repo.land_step(
                                l.q.cs,
                                l.q.create,
                                head,
                                target_ref,
                                attempt,
                                &mut l.checked,
                                &mut origins,
                            )
                            .await
                        }
                        Err(e) => Err(e),
                    },
                    None => {
                        repo.land_step(
                            l.q.cs,
                            l.q.create,
                            head,
                            target_ref,
                            attempt,
                            &mut l.checked,
                            &mut origins,
                        )
                        .await
                    }
                };
                match step {
                    Ok(Step::Move(new)) => {
                        // A duplicate later in the batch answers with this.
                        // Its log entry's `seq` is unknown until the commit,
                        // and nothing in `land` reads it.
                        if let (Ok(r), Some(key)) = (&mut recorded, &l.q.ctx.land) {
                            r.insert(
                                key.id.clone(),
                                LandRecord {
                                    changeset: key.changeset,
                                    landed: new,
                                    seq: 0,
                                },
                            );
                        }
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
            staging.retain_stored(&mut origins);
            carried = origins;
            if steps.is_empty() {
                if lands.is_empty() {
                    return;
                }
                tokio::time::sleep(backoff(attempt)).await;
                continue;
            }
            // The fence read of this attempt bounds how late its move may
            // come; past the batch's lifetime it must not come at all.
            if started.elapsed() >= MAX_BATCH_LIFETIME {
                for l in waiting.into_iter().map(|(l, _)| l).chain(lands) {
                    l.answer(Err(past_deadline(target_ref)), attempt, false);
                }
                return;
            }
            // Whether an error came from the swap itself.
            let applied = match staging.flush(self).await {
                Err(e) => Err((e, false)),
                Ok(()) => self
                    .commit_steps(target_ref, start, &steps)
                    .await
                    .map_err(|e| (e, true)),
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
                // A land id another server recorded after this attempt's
                // lookup (the ref came back to `start` meanwhile): the swap
                // did not apply. Redo the attempt like a lost race; its lookup
                // answers that land.
                Err((Error::Conflict(_), true)) => {
                    commit_errors = 0;
                    metrics::counter!("leep_land_cas_losses_total").increment(1);
                    lands.extend(waiting.into_iter().map(|(l, _)| l));
                }
                // The commit may or may not have applied. Another pass reads
                // the head again and either finds each stack in it or replays.
                Err((e, _)) => {
                    commit_errors += 1;
                    if commit_errors >= MAX_COMMIT_ERRORS {
                        for l in waiting.into_iter().map(|(l, _)| l).chain(lands) {
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

    /// What `target_ref` recorded for the land ids in `lands`; nothing is read
    /// when none carries one.
    async fn recorded_lands(
        &self,
        target_ref: &str,
        lands: &[InFlight],
    ) -> Result<HashMap<LandId, LandRecord>> {
        let ids: Vec<LandId> = lands
            .iter()
            .filter_map(|l| l.q.ctx.land.as_ref().map(|k| k.id.clone()))
            .collect();
        if ids.is_empty() {
            return Ok(HashMap::new());
        }
        let found = self.meta().find_lands(target_ref, &ids).await?;
        Ok(ids
            .into_iter()
            .zip(found)
            .filter_map(|(id, r)| r.map(|r| (id, r)))
            .collect())
    }

    /// Learn the parents and generations of a batch's changesets this
    /// process has not seen, in one round of batched reads. A land created
    /// through another server (forwarded to this one, ADR 0056, or landed
    /// after a restart) would otherwise read them one at a time in its
    /// replay, and the batch's prefetch of rebase links, which needs the
    /// generations, would read nothing. A failed read is left to the
    /// replays, which read singly.
    async fn learn_ancestry(&self, ids: &[ChangesetId]) {
        let missing: Vec<ChangesetId> = ids
            .iter()
            .copied()
            .filter(|id| {
                self.cached_generation(*id).is_none() || self.cached_parents(*id).is_none()
            })
            .collect();
        if missing.is_empty() {
            return;
        }
        let keys: Vec<_> = missing.iter().map(|id| id.0).collect();
        let (changesets, generations) = tokio::join!(
            self.meta().get_changesets(&missing),
            self.meta().get_derived_many(
                <crate::derived::Generation as crate::derived::DerivedKind>::NAME,
                &keys
            )
        );
        if let Ok(changesets) = changesets {
            for (id, cs) in missing.iter().zip(changesets) {
                if let Some(cs) = cs {
                    self.remember_parents(*id, cs.parents.into());
                }
            }
        }
        if let Ok(generations) = generations {
            for (id, g) in missing.iter().zip(generations) {
                if let Some(g) = g.and_then(|g| crate::derived::decode_u64(&g).ok()) {
                    self.remember_generation(*id, g);
                }
            }
        }
    }

    /// One land the way it ran before batches were staged: replayed straight
    /// into the stores, then swapped. The oracle staged batches are checked
    /// against.
    #[cfg(test)]
    pub(crate) async fn land_unstaged(
        &self,
        cs: ChangesetId,
        target_ref: &str,
        actor: Option<&str>,
    ) -> Result<LandResponse> {
        let ctx = RefUpdateContext::new("land")
            .actor(actor)
            .message(format!("landed {cs}"));
        let head = self.meta().get_ref(target_ref).await?;
        let step = self
            .land_step(
                cs,
                true,
                head,
                target_ref,
                1,
                &mut Checked::default(),
                &mut HashMap::new(),
            )
            .await?;
        match step {
            Step::Done(resp) => Ok(resp),
            Step::Move(new) => {
                let step = RefStep {
                    new: Some(new),
                    ctx,
                };
                assert_eq!(self.commit_steps(target_ref, head, &[step]).await?, 1);
                Ok(LandResponse::Landed {
                    landed: new,
                    rebased: new != cs,
                    attempts: 1,
                })
            }
        }
    }

    /// Move the ref through `steps` from `start`; a batch of one makes the
    /// statements a lone land always made.
    async fn commit_steps(
        &self,
        target_ref: &str,
        start: Option<ChangesetId>,
        steps: &[RefStep],
    ) -> Result<usize> {
        if let [step] = steps {
            self.meta()
                .cas_ref_logged(target_ref, start, step.new, &step.ctx)
                .await
                .map(usize::from)
        } else {
            self.meta()
                .cas_ref_logged_chain(target_ref, start, steps)
                .await
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
            Some(h) if !fast_forward => match self.replay_stack(cs, h, origins).await? {
                Ok(id) => Ok(Step::Move(id)),
                Err(resp) => Ok(Step::Done(resp)),
            },
            _ => Ok(Step::Move(cs)),
        }
    }

    /// Read in one round trip the `rebased-origin-v1` rows the batch's
    /// landed-copy scans will want: those of `head`'s ancestors above the
    /// lowest generation among `ids`, as far as [`landed_copy`] walks and
    /// this process already knows the ancestry, so the walk itself reads
    /// nothing. The first scan of a batch otherwise reads them one by one.
    /// Anything not read here the scans read as before. Rows already in
    /// `origins` (carried from an earlier attempt) are not read again.
    ///
    /// [`landed_copy`]: Self::landed_copy
    async fn prefetch_origins(
        &self,
        head: ChangesetId,
        ids: &[ChangesetId],
        origins: &mut HashMap<ChangesetId, Option<ChangesetId>>,
    ) {
        let Some(floor) = ids
            .iter()
            .filter_map(|id| self.cached_generation(*id))
            .min()
        else {
            return;
        };
        let mut want = Vec::new();
        let mut walked = 0;
        let mut seen = HashSet::from([head]);
        let mut stack = vec![head];
        while let Some(id) = stack.pop() {
            if !self.cached_generation(id).is_some_and(|g| g > floor) {
                continue;
            }
            if !origins.contains_key(&id) {
                want.push(id);
            }
            walked += 1;
            if walked >= LANDED_COPY_SCAN {
                break;
            }
            if let Some(parents) = self.cached_parents(id) {
                stack.extend(parents.iter().filter(|p| seen.insert(**p)));
            }
        }
        if want.is_empty() {
            return;
        }
        let keys: Vec<_> = want.iter().map(|id| id.0).collect();
        let Ok(rows) = self
            .meta()
            .get_derived_many(crate::REBASED_ORIGIN_KIND, &keys)
            .await
        else {
            return;
        };
        for (id, row) in want.into_iter().zip(rows) {
            match row {
                None => {
                    origins.insert(id, None);
                }
                Some(v) => {
                    if let Ok(arr) = <[u8; 32]>::try_from(v.as_ref()) {
                        origins.insert(id, Some(ChangesetId(leep_core::Hash(arr))));
                    }
                }
            }
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
        if let Some(new) = new {
            refname::validate(name)?;
            self.rescue_changesets(&[new], RescueBy::Ref).await?;
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
    /// including) the first changeset already in `head`'s history, or whose
    /// rebased copy is (an agent that landed `a` and then built `b` on its own
    /// `a`). Replaying only `cs` would silently drop the rest of the stack;
    /// replaying a landed member again would add an empty duplicate of it, or
    /// a false conflict once the ref has changed its files since.
    ///
    /// On conflict, returns the whole stack squashed into one three-way merge
    /// relative to `head`, so the client can resolve it in a single commit.
    async fn replay_stack(
        &self,
        cs: ChangesetId,
        head: ChangesetId,
        origins: &mut HashMap<ChangesetId, Option<ChangesetId>>,
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
                Some(&p) => {
                    if self.landed_copy(p, head, origins).await?.is_some() {
                        fork = Some(p);
                        break;
                    }
                    cur = p;
                }
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
