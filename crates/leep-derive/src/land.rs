//! `land`: replay unlanded changesets onto the ref head and compare-and-swap,
//! no merge queue. Concurrent lands of one ref in one process are grouped
//! into batches that move the ref once (ADR 0032).

use std::collections::hash_map::RandomState;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::hash::BuildHasher;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use leep_core::api::{
    LandRefusal, LandResponse, RebaseOntoResponse, RebaseResponse, REFUSED_ADMISSION,
};
use leep_core::refname::{self, MISSING_REF_MARKER, NOT_FORWARD_MARKER};
use leep_core::{
    ChangesetId, Error, FileChange, Hash, LandId, LandKey, LandRecord, RefStep, RefUpdateContext,
    Result,
};
use tokio::sync::oneshot;

use crate::changeset_gc::RescueBy;
use crate::land_fast::{ConflictPolicy, Lookup, WriteMap};
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
/// Lands one batch takes at most, unless [`Repo::with_land_max_batch`] says
/// otherwise. It bounds the commit (one ref log row per land) and how long a
/// land waits behind replays of the lands queued before it; the next batch
/// starts as soon as this one commits.
pub const MAX_BATCH: usize = 64;
/// Replay time after which a batch's first attempt stops taking lands and
/// hands the rest back to the front of the queue, for the next batch,
/// unless [`Repo::with_land_batch_replay_limit`] says otherwise (ADR 0072).
/// Slow replays (large merges, cold caches) then close a batch early
/// instead of holding every land behind 64 of them; fast ones fill it.
pub const MAX_BATCH_REPLAY: Duration = Duration::from_millis(200);
/// Fewest batches `--land-queue-seconds` admits a land behind, whatever the
/// batch time: one would collapse each batch to about one land.
pub const MIN_QUEUE_BATCHES: usize = 2;
/// How long a batch may keep trying, from its first attempt. A batch runs
/// detached from the requests that asked for it, so this bounds how long
/// after its last fence read it can still move a ref, which the changeset
/// sweep's fence must outlast (ADR 0059 section 3). A batch past it answers
/// its lands with a retryable error; keyed ones are then answered from their
/// record, so none lands twice.
pub const MAX_BATCH_LIFETIME: Duration = Duration::from_secs(60);

/// Most unlanded changesets one land replays (#158). A replay runs on the
/// ref's leader, ahead of every land queued behind it, and costs a rebase
/// per member; a longer stack is refused up front, to be landed in pieces.
/// It matches how far back a landed copy is looked for, so a stack the cap
/// admits is also one whose earlier landing is found.
pub const MAX_LAND_STACK: usize = LANDED_COPY_SCAN;

/// Copies a batch reads skiplists ahead for (ADR 0079). Each level reads up
/// to one row per copy, about 800 bytes at a depth of a million, so a plan
/// keeps at most a few thousand lists, well inside the `SkiplistCache`.
const MAX_PLANNED_COPIES: usize = 256;

/// Copies planned beyond one per land, for stacks of more than one.
const PLANNED_COPY_SLACK: usize = 8;
/// How far into its batch a land's replay may run before that land alone
/// answers past-deadline (retryable). Half the batch lifetime, so the lands
/// replayed before it still have time to commit.
pub const REPLAY_BUDGET: Duration = Duration::from_secs(MAX_BATCH_LIFETIME.as_secs() / 2);

/// Bounds of the `Retry-After` of a land refused at a full queue, in
/// seconds (ADR 0060, section 2). The answer is the time until a slot
/// frees, about one batch time, so the ceiling must reach the longest batch
/// time there can be: a ceiling under it (10 s, before ADR 0066) sends
/// clients back while the batch they wait for still runs, to be refused
/// again (13,000 to 28,000 refusals at 1,024 agents with 20 s batches). A
/// batch runs at most [`MAX_BATCH_LIFETIME`]; half of it covers every batch
/// a healthy server makes, and `leep land` waits that out with its jitter
/// (up to 45 s) inside its 60 s budget for admission waits.
const ADMISSION_RETRY_MIN_SECS: u64 = 1;
const ADMISSION_RETRY_MAX_SECS: u64 = MAX_BATCH_LIFETIME.as_secs() / 2;
/// Weight of the newest batch in a queue's moving average of batch times.
const BATCH_TIME_WEIGHT: f64 = 0.2;
/// The ref log operation of a `rebase-onto` move: the git mirror's.
pub const GIT_MIRROR_OPERATION: &str = "git-mirror";

/// The answer for the lands of a batch past [`MAX_BATCH_LIFETIME`].
fn past_deadline(target_ref: &str) -> Error {
    metrics::counter!("leep_land_batch_deadline_total").increment(1);
    Error::Conflict(format!(
        "land onto {target_ref:?} did not finish within {}s; retry",
        MAX_BATCH_LIFETIME.as_secs()
    ))
}

/// When a land's replay must stop: past `at`, that land answers
/// [`past_deadline`] instead of holding the ref's queue (#158).
#[derive(Clone, Copy)]
struct ReplayDeadline<'a> {
    at: tokio::time::Instant,
    target_ref: &'a str,
}

impl ReplayDeadline<'_> {
    fn check(&self) -> Result<()> {
        if tokio::time::Instant::now() >= self.at {
            return Err(past_deadline(self.target_ref));
        }
        Ok(())
    }
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

/// How long a ref's land queue may grow before new lands are refused (ADR
/// 0060 section 2, ADR 0068).
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum LandQueueBound {
    /// At most this many batches ahead, its own included
    /// (`--land-queue-batches`).
    Batches(usize),
    /// At most this long a wait: the ref's moving batch time turned into
    /// batches (`--land-queue-seconds`).
    Seconds(Duration),
}

/// The land queues of one process, one per target ref.
#[derive(Default)]
pub(crate) struct LandQueues {
    queues: Mutex<HashMap<String, Arc<Mutex<RefQueue>>>>,
    /// Lands per batch, for queues made from now on; 0 is [`MAX_BATCH`].
    pub(crate) max_batch: std::sync::atomic::AtomicUsize,
    /// Batches are not pipelined (ADR 0072): each starts once the one
    /// before it ended.
    pub(crate) no_pipeline: std::sync::atomic::AtomicBool,
    /// Replay limit of a batch's first attempt in microseconds; 0 is
    /// [`MAX_BATCH_REPLAY`], `u64::MAX` none.
    pub(crate) replay_limit_us: std::sync::atomic::AtomicU64,
    /// How many queues hold each number of batches above zero, so the
    /// largest is known without visiting every ref's queue.
    depths: Mutex<BTreeMap<usize, usize>>,
    /// Leader tasks, so a test can kill a simulated server mid-batch.
    #[cfg(test)]
    tasks: Mutex<Vec<tokio::task::AbortHandle>>,
    /// The lands each batch took, in order, for tests to replay one by one.
    #[cfg(test)]
    pub(crate) batches: Mutex<Vec<Vec<ChangesetId>>>,
    /// What the fast path made of each replayed land, in order.
    #[cfg(test)]
    pub(crate) lookups: Mutex<Vec<(ChangesetId, Lookup)>>,
    /// Each speculative attempt (ADR 0072), in order: its lands, and
    /// whether it was kept.
    #[cfg(test)]
    pub(crate) speculated: Mutex<Vec<(Vec<ChangesetId>, bool)>>,
}

impl LandQueues {
    pub(crate) fn pipeline(&self) -> bool {
        !self.no_pipeline.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub(crate) fn replay_limit(&self) -> Option<Duration> {
        match self
            .replay_limit_us
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            0 => Some(MAX_BATCH_REPLAY),
            u64::MAX => None,
            us => Some(Duration::from_micros(us)),
        }
    }

    fn queue(&self, target_ref: &str) -> Arc<Mutex<RefQueue>> {
        let mut queues = self.queues.lock().unwrap_or_else(|e| e.into_inner());
        let max_batch = self.max_batch.load(std::sync::atomic::Ordering::Relaxed);
        Arc::clone(queues.entry(target_ref.to_string()).or_insert_with(|| {
            Arc::new(Mutex::new(RefQueue {
                max_batch,
                ..RefQueue::default()
            }))
        }))
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

    /// Tips and entries of `target_ref`'s write map.
    #[cfg(test)]
    pub(crate) fn write_map_size(&self, target_ref: &str) -> (usize, usize) {
        let q = self.queue(target_ref);
        let q = lock(&q);
        (q.writes.tips(), q.writes.entries())
    }

    /// No leader runs for `target_ref`.
    #[cfg(test)]
    pub(crate) fn idle(&self, target_ref: &str) -> bool {
        !lock(&self.queue(target_ref)).leading
    }

    /// Set `target_ref`'s moving batch time.
    #[cfg(test)]
    pub(crate) fn set_batch_secs(&self, target_ref: &str, secs: f64) {
        lock(&self.queue(target_ref)).batch_secs = Some(secs);
    }

    /// Batches `target_ref`'s queue holds, as admission counts them.
    #[cfg(test)]
    pub(crate) fn batches(&self, target_ref: &str) -> usize {
        lock(&self.queue(target_ref)).batches()
    }

    /// Record `q`'s size after a change, for `leep_land_queue_batches`: the
    /// largest queue in batches over the refs this process batches. Called
    /// under `q`'s lock; this lock is never held while taking a queue's.
    fn report(&self, q: &mut RefQueue) {
        let now = q.batches();
        if now == q.reported {
            return;
        }
        let mut depths = self.depths.lock().unwrap_or_else(|e| e.into_inner());
        if q.reported > 0 {
            if let Some(n) = depths.get_mut(&q.reported) {
                *n -= 1;
                if *n == 0 {
                    depths.remove(&q.reported);
                }
            }
        }
        if now > 0 {
            *depths.entry(now).or_default() += 1;
        }
        q.reported = now;
        let largest = depths.keys().next_back().copied().unwrap_or(0);
        metrics::gauge!("leep_land_queue_batches").set(largest as f64);
    }
}

#[derive(Default)]
struct RefQueue {
    pending: VecDeque<Job>,
    /// A leader task is draining `pending`; whoever finds none starts one.
    leading: bool,
    /// Lands admitted and not yet queued: they are reading their changeset.
    reserved: usize,
    /// Batches the leader took from `pending` that have not ended: one, or
    /// two while one commits and the next replays onto it (ADR 0072).
    in_flight: usize,
    /// Moving average of this ref's batch times, in seconds.
    batch_secs: Option<f64>,
    /// [`RefQueue::batches`] as last given to [`LandQueues::report`].
    reported: usize,
    /// What the ref's recent changesets wrote, for the fast path (ADR
    /// 0066). The leader takes it for a batch and puts it back after; a
    /// leader that panics loses it, and the next starts cold.
    writes: WriteMap,
    /// Lands per batch; 0 is [`MAX_BATCH`].
    max_batch: usize,
    /// Told of every land queued, so a leader waiting on a commit with
    /// nothing queued takes the next batch as soon as one arrives.
    arrived: Arc<tokio::sync::Notify>,
}

impl RefQueue {
    /// The batches this queue holds (ADR 0060, section 2): the lands
    /// waiting, admitted ones included, in batches of [`MAX_BATCH`] rounded
    /// up, plus one while any is in flight. A land waits about one batch
    /// time per batch ahead of it, whatever the batches' sizes. Two
    /// pipelined batches in flight count as one (ADR 0072): the batch time
    /// is the interval between their ends, and the one committing ends
    /// within it while the next replays; counting both would make the
    /// smallest bound, two, refuse every land while batches pipeline.
    fn batches(&self) -> usize {
        self.batches_with(0)
    }

    /// [`RefQueue::batches`] with `more` lands added: the leader takes up
    /// to [`MAX_BATCH`] waiting lands at once, so a new land joins the last
    /// batch while it has room, and waits as long as the lands already in
    /// it.
    fn batches_with(&self, more: usize) -> usize {
        (self.pending.len() + self.reserved + more).div_ceil(self.batch_cap())
            + self.in_flight.min(1)
    }

    /// Lands the leader takes into one batch.
    fn batch_cap(&self) -> usize {
        match self.max_batch {
            0 => MAX_BATCH,
            n => n,
        }
    }

    /// The batches a land may wait behind under `bound` (ADR 0060 section
    /// 2; ADR 0068 for seconds): a count as given, or as many batch times
    /// of this ref as fit in the seconds, at least [`MIN_QUEUE_BATCHES`].
    ///
    /// Before a batch of the ref ended there is no batch time to turn the
    /// seconds into, and the seconds refuse nothing (ADR 0072): counting a
    /// batch as 1 s, as before, refused most of a herd arriving at a fresh
    /// server (483 of 1,024 agents in the first 2 s) whose batches take
    /// 0.3 s. The first batch to end sets the estimate; the herd is no
    /// larger than the clients sending it.
    fn bound_batches(&self, bound: LandQueueBound) -> usize {
        match bound {
            LandQueueBound::Batches(n) => n,
            LandQueueBound::Seconds(limit) => match self.batch_secs {
                None => usize::MAX,
                Some(per_batch) => ((limit.as_secs_f64() / per_batch.max(1e-3)).floor() as usize)
                    .max(MIN_QUEUE_BATCHES),
            },
        }
    }

    /// `Retry-After` for `batches` batch times, rounded up and clamped.
    fn retry_after_secs(&self, batches: usize) -> u64 {
        let secs = (batches as f64 * self.batch_secs.unwrap_or(1.0)).ceil() as u64;
        secs.clamp(ADMISSION_RETRY_MIN_SECS, ADMISSION_RETRY_MAX_SECS)
    }

    /// `Retry-After` for a land refused at `bound`: about when a slot frees.
    /// The queue holds `ahead >= bound` batches; each batch replayed frees
    /// one, so `ahead + 1 - bound` of them must finish, not all `ahead`.
    fn refused_retry_after_secs(&self, bound: usize) -> u64 {
        let ahead = self.batches();
        self.retry_after_secs((ahead + 1).saturating_sub(bound).max(1))
    }

    /// Fold a finished batch's time into the moving average.
    fn record_batch(&mut self, took: Duration) {
        let took = took.as_secs_f64();
        metrics::histogram!("leep_land_batch_seconds").record(took);
        self.batch_secs = Some(match self.batch_secs {
            Some(avg) => avg + BATCH_TIME_WEIGHT * (took - avg),
            None => took,
        });
    }
}

/// A place in a ref's queue taken by an admitted land while it reads its
/// changeset; given back if the land ends before it is queued.
struct Admitted<'a> {
    queues: &'a LandQueues,
    queue: Arc<Mutex<RefQueue>>,
    held: bool,
}

impl Admitted<'_> {
    /// Queue `land` in the place this held.
    fn enqueue(mut self, land: Queued) -> bool {
        let mut q = lock(&self.queue);
        q.reserved -= 1;
        self.held = false;
        q.pending.push_back(Job::Land(land));
        q.arrived.notify_one();
        self.queues.report(&mut q);
        !std::mem::replace(&mut q.leading, true)
    }
}

impl Drop for Admitted<'_> {
    fn drop(&mut self) {
        if self.held {
            let mut q = lock(&self.queue);
            q.reserved -= 1;
            self.queues.report(&mut q);
        }
    }
}

fn lock(queue: &Mutex<RefQueue>) -> MutexGuard<'_, RefQueue> {
    queue.lock().unwrap_or_else(|e| e.into_inner())
}

/// What a ref's queue holds, in arrival order: lands, batched together, and
/// replays of the ref onto a new base (ADR 0062), each run alone between
/// batches, so lands and replays never race each other's swaps.
enum Job {
    Land(Queued),
    RebaseOnto(QueuedRebase),
}

/// A `rebase-onto` waiting its turn.
struct QueuedRebase {
    repo: Repo,
    onto: ChangesetId,
    expected_base: ChangesetId,
    ctx: RefUpdateContext,
    /// `None`: this server stopped coordinating the ref; route it again.
    reply: oneshot::Sender<Option<Result<RebaseOntoResponse>>>,
    led: bool,
}

/// What the leader runs next.
enum Next {
    Lands(Vec<Queued>),
    RebaseOnto(Box<QueuedRebase>),
}

/// What one attempt of a `rebase-onto` came to before its swap.
enum Replayed {
    Done(RebaseOntoResponse),
    Move {
        head: ChangesetId,
        rebased: Vec<(ChangesetId, ChangesetId)>,
    },
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
        return Some(Err(land_id_reused(&key.id, found, key.changeset)));
    }
    metrics::counter!("leep_land_recorded_total").increment(1);
    metrics::counter!("leep_land_answered_from_record_total", "original" => "present").increment(1);
    Some(Ok(Step::Done(LandResponse::Landed {
        landed: found.landed,
        rebased: found.landed != key.changeset,
        attempts: attempt,
    })))
}

/// The refusal of a land whose `id` the ref recorded for another changeset.
fn land_id_reused(id: &LandId, record: &LandRecord, requested: ChangesetId) -> Error {
    Error::LandIdReused {
        refusal: Box::new(LandRefusal {
            land_id: id.clone(),
            changeset: record.changeset,
            landed: Some(record.landed),
        }),
        requested,
    }
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
        Error::LandIdReused { refusal, requested } => Error::LandIdReused {
            refusal: refusal.clone(),
            requested: *requested,
        },
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
        Error::Refused {
            refused,
            retry_after_ms,
            message,
        } => Error::Refused {
            refused,
            retry_after_ms: *retry_after_ms,
            message: message.clone(),
        },
    }
}

/// Clears the queue if the leader stops without draining it (a panic), so
/// waiting lands get an error instead of waiting forever and the next land
/// starts a new leader.
struct Leading {
    queues: Arc<LandQueues>,
    queue: Arc<Mutex<RefQueue>>,
    done: bool,
}

impl Drop for Leading {
    fn drop(&mut self) {
        if !self.done {
            let mut q = lock(&self.queue);
            q.leading = false;
            q.in_flight = 0;
            q.pending.clear();
            self.queues.report(&mut q);
        }
    }
}

/// What a batch about to commit offers the batch taken behind it (ADR
/// 0072): the head its commit will leave and what it staged, to replay onto
/// before the commit is known.
struct Ahead {
    head: Option<ChangesetId>,
    staging: Arc<Staging>,
    /// The land ids its moves record, which the ref log may not show yet.
    recorded: HashMap<LandId, LandRecord>,
    /// `true` once its commit applied whole and it has no land left to
    /// retry; `false`, or closed, otherwise.
    landed: oneshot::Receiver<bool>,
    /// Closed once its batch has ended.
    done: oneshot::Receiver<()>,
}

/// Take the next job from `q`: a rebase, or up to a batch of lands.
fn take_next(q: &mut RefQueue) -> Option<Next> {
    match q.pending.pop_front()? {
        Job::RebaseOnto(r) => Some(Next::RebaseOnto(Box::new(r))),
        Job::Land(first) => {
            let mut batch = vec![first];
            let cap = q.batch_cap();
            while batch.len() < cap {
                match q.pending.pop_front() {
                    Some(Job::Land(l)) => batch.push(l),
                    Some(other) => {
                        q.pending.push_front(other);
                        break;
                    }
                    None => break,
                }
            }
            Some(Next::Lands(batch))
        }
    }
}

/// Note a job's end: it leaves the queue's count, and its time since it
/// started or the job before it ended, whichever is later, joins the batch
/// time. With pipelined batches that is the interval between commits, the
/// rate the queue drains at, not the batch's own longer life.
fn job_ended(queues: &LandQueues, queue: &Mutex<RefQueue>, started: Instant, last: &mut Instant) {
    let now = Instant::now();
    let from = started.max(*last);
    *last = now;
    let mut q = lock(queue);
    q.in_flight = q.in_flight.saturating_sub(1);
    q.record_batch(now - from);
    queues.report(&mut q);
}

/// Run batches until the queue is empty. Spawned, so a cancelled request
/// never cancels the batch the other requests wait on.
///
/// Pipelined (ADR 0072): once a batch starts its commit, the next is taken
/// and replays onto the head that commit will leave, in this task, while
/// the commit runs. At most one batch replays behind the one committing; it
/// commits only after that one did, and redoes its attempt from the ref
/// otherwise. A batch that offers nothing to build on (pipelining off, no
/// move, a rebase) is waited out before the next starts.
async fn lead(repo: Repo, target_ref: String, queue: Arc<Mutex<RefQueue>>) {
    use futures_util::stream::{FuturesUnordered, StreamExt};
    let queues = Arc::clone(&repo.land_queues);
    let mut leading = Leading {
        queues: Arc::clone(&queues),
        queue: Arc::clone(&queue),
        done: false,
    };
    let mut last_end = Instant::now();
    // Batches started and not yet ended, each answering with its start.
    let mut running = FuturesUnordered::new();
    // What the newest batch offered to build on.
    let mut ahead: Option<Ahead> = None;
    loop {
        if ahead.is_none() {
            while let Some(started) = running.next().await {
                job_ended(&queues, &queue, started, &mut last_end);
            }
        }
        let next = {
            let mut q = lock(&queue);
            match take_next(&mut q) {
                None if running.is_empty() => {
                    q.leading = false;
                    q.in_flight = 0;
                    queues.report(&mut q);
                    leading.done = true;
                    return;
                }
                None => None,
                Some(next) => {
                    q.in_flight += 1;
                    queues.report(&mut q);
                    Some(next)
                }
            }
        };
        match next {
            // Nothing waiting while a batch commits: the first land to
            // arrive meanwhile may still replay onto it.
            None => {
                let arrived = Arc::clone(&lock(&queue).arrived);
                tokio::select! {
                    () = arrived.notified() => {}
                    Some(started) = running.next() => {
                        job_ended(&queues, &queue, started, &mut last_end);
                    }
                }
            }
            Some(Next::Lands(batch)) => {
                let (offer, mut offered) = oneshot::channel();
                let (repo, target_ref, q, spec) = (&repo, &target_ref, &*queue, ahead.take());
                running.push(async move {
                    let started = Instant::now();
                    repo.land_batch(target_ref, batch, q, spec, offer).await;
                    started
                });
                // Every batch in flight moves on while this one replays.
                ahead = loop {
                    tokio::select! {
                        a = &mut offered => break a.ok().flatten(),
                        Some(started) = running.next() => {
                            job_ended(&queues, &queue, started, &mut last_end);
                        }
                    }
                };
            }
            Some(Next::RebaseOnto(r)) => {
                while let Some(started) = running.next().await {
                    job_ended(&queues, &queue, started, &mut last_end);
                }
                ahead = None;
                let started = Instant::now();
                let QueuedRebase {
                    repo,
                    onto,
                    expected_base,
                    ctx,
                    reply,
                    led,
                } = *r;
                let result = repo
                    .rebase_onto_attempts(&target_ref, onto, expected_base, &ctx, led)
                    .await;
                let _ = reply.send(result);
                job_ended(&queues, &queue, started, &mut last_end);
            }
        }
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
        self.land_queued(cs, target_ref, actor, create, land_id, false, false)
            .await
            .unwrap_or_else(|| {
                Err(Error::Other(
                    "land was handed back unexpectedly; retry".into(),
                ))
            })
    }

    /// [`land_keyed`](Self::land_keyed) for a server holding `target_ref`'s
    /// land lease (ADR 0056): `None` when it lost the lease before the land
    /// was replayed, and the caller must route the land again. `forwarded`
    /// marks a land another server sent here, for the admission metric.
    pub(crate) async fn land_led(
        &self,
        cs: ChangesetId,
        target_ref: &str,
        actor: Option<&str>,
        create: bool,
        land_id: Option<LandId>,
        forwarded: bool,
    ) -> Option<Result<LandResponse>> {
        self.land_queued(cs, target_ref, actor, create, land_id, true, forwarded)
            .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn land_queued(
        &self,
        cs: ChangesetId,
        target_ref: &str,
        actor: Option<&str>,
        create: bool,
        land_id: Option<LandId>,
        led: bool,
        forwarded: bool,
    ) -> Option<Result<LandResponse>> {
        let started = Instant::now();
        let mut stats = LandStats::default();
        let ctx = RefUpdateContext::new("land")
            .actor(actor)
            .message(format!("landed {cs}"))
            .land(land_id.map(|id| LandKey { id, changeset: cs }));
        match self
            .land_inner(cs, target_ref, create, ctx, &mut stats, led, forwarded)
            .await
        {
            // Not admitted: counted by the admission metric alone.
            Settled::Answered(result @ Err(Error::Refused { .. })) => Some(result),
            Settled::Answered(result) => {
                stats.record(&result, started.elapsed());
                Some(result)
            }
            Settled::Rerouted => None,
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn land_inner(
        &self,
        cs: ChangesetId,
        target_ref: &str,
        create: bool,
        ctx: RefUpdateContext,
        stats: &mut LandStats,
        led: bool,
        forwarded: bool,
    ) -> Settled {
        match self
            .land_enqueue(cs, target_ref, create, ctx, stats, led, forwarded)
            .await
        {
            Ok(Some(answer)) => Settled::Answered(answer),
            Ok(None) => Settled::Rerouted,
            Err(e) => Settled::Answered(Err(e)),
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn land_enqueue(
        &self,
        cs: ChangesetId,
        target_ref: &str,
        create: bool,
        ctx: RefUpdateContext,
        stats: &mut LandStats,
        led: bool,
        forwarded: bool,
    ) -> Result<Option<Result<LandResponse>>> {
        refname::validate(target_ref)?;
        if refname::is_tag(target_ref) {
            return Err(Error::Invalid(format!(
                "cannot land onto {target_ref:?}: tags are immutable"
            )));
        }
        let queue = self.land_queues.queue(target_ref);
        let admitted = match self.admit(&queue, cs, target_ref, &ctx, forwarded).await? {
            Ok(admitted) => admitted,
            Err(answer) => return Ok(Some(Ok(answer))),
        };
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
        let start_leader = admitted.enqueue(Queued {
            repo: self.clone(),
            cs,
            create,
            ctx,
            queued: Instant::now(),
            reply,
            led,
        });
        if start_leader {
            // In the land lane (ADR 0067): request handlers holding every shared
            // metadata connection must not queue the batch they all wait on.
            let leader = lead(self.clone(), target_ref.to_string(), queue);
            let task = tokio::spawn(leep_core::lane::scope(leep_core::lane::Lane::Land, leader));
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

    /// Admit a land to `queue` (ADR 0060, section 2), holding its place
    /// while it reads its changeset. A queue where it would wait behind
    /// more than `--land-queue-batches` batches, its own included, refuses
    /// it with [`Error::Refused`], having read nothing; a keyed land first
    /// reads its id, and one the ref already recorded gets that answer (the
    /// `Err` of the inner result) instead of a refusal. Queued lands are
    /// never dropped to make room.
    async fn admit<'a>(
        &'a self,
        queue: &Arc<Mutex<RefQueue>>,
        cs: ChangesetId,
        target_ref: &str,
        ctx: &RefUpdateContext,
        forwarded: bool,
    ) -> Result<std::result::Result<Admitted<'a>, LandResponse>> {
        let refused = {
            let mut q = lock(queue);
            // Refused when it would wait behind more than `bound` batches,
            // its own included: a land that joins a batch still filling
            // waits no longer than the lands already in it.
            let ahead = q.batches();
            match self.land_queue_bound.map(|b| (b, q.bound_batches(b))) {
                Some((how, bound)) if q.batches_with(1) > bound => {
                    Some((ahead, how, bound, q.refused_retry_after_secs(bound)))
                }
                _ => {
                    q.reserved += 1;
                    self.land_queues.report(&mut q);
                    None
                }
            }
        };
        let Some((ahead, how, bound, secs)) = refused else {
            return Ok(Ok(Admitted {
                queues: &self.land_queues,
                queue: Arc::clone(queue),
                held: true,
            }));
        };
        // A resend whose first send landed and lost its answer would
        // otherwise be refused while the queue stays full.
        if ctx.land.is_some() {
            match self.recorded_answer(cs, target_ref, ctx).await {
                Ok(Some(answer)) => {
                    metrics::counter!("leep_land_answered_from_record_total", "original" => "present")
                        .increment(1);
                    return Ok(Err(answer));
                }
                Ok(None) => {}
                Err(e @ Error::LandIdReused { .. }) => return Err(e),
                // Nothing was done either way: refusing is still true.
                // A store error the refusal hides from the client.
                Err(e) => {
                    tracing::warn!(r#ref = target_ref, error = %e, "land id lookup failed at a full queue")
                }
            }
        }
        let route = if forwarded { "forwarded" } else { "direct" };
        metrics::counter!("leep_land_admission_refused_total", "route" => route).increment(1);
        tracing::debug!(
            r#ref = target_ref,
            batches = ahead,
            retry_after = secs,
            route,
            "land refused: queue full"
        );
        Err(Error::Refused {
            refused: REFUSED_ADMISSION,
            retry_after_ms: secs * 1000,
            message: match how {
                LandQueueBound::Batches(_) => format!(
                    "land not admitted: the land queue of {target_ref:?} holds {ahead} batches \
                     (--land-queue-batches {bound}); nothing was done, send it again in {secs}s"
                ),
                LandQueueBound::Seconds(limit) => format!(
                    "land not admitted: the land queue of {target_ref:?} holds {ahead} batches, \
                     more than {} s of them (--land-queue-seconds, {bound} batches now); \
                     nothing was done, send it again in {secs}s",
                    limit.as_secs_f64()
                ),
            },
        })
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
        let answer = self.recorded_answer(cs, target_ref, ctx).await?;
        if answer.is_some() {
            metrics::counter!("leep_land_answered_from_record_total", "original" => "missing")
                .increment(1);
        }
        Ok(answer)
    }

    /// What `target_ref` recorded for `ctx`'s land id, read with one
    /// `find_lands`: `Ok(None)` without an id or a record, `LandIdReused`
    /// when the id landed another changeset.
    async fn recorded_answer(
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
            return Err(land_id_reused(&key.id, &record, cs));
        }
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
    ///
    /// Two rescues, each of which may wait up to `DOOM_WAIT` (30 s) for a
    /// delete in progress, so one attempt can wait 60 s, past
    /// [`MAX_BATCH_LIFETIME`]. That is safe: the deadline is checked again
    /// just before the swap, so a late attempt moves nothing and answers a
    /// retryable error, and a keyed land is then answered from its record
    /// when resent (ADR 0059 section 11).
    async fn rescue_land(&self, cs: ChangesetId, head: Option<ChangesetId>) -> Result<()> {
        self.rescue_condemned(&[cs], RescueBy::Land).await?;
        let stack = self.new_history(cs, head).await?;
        self.rescue_condemned(&stack, RescueBy::Land).await
    }

    /// Land a batch: replay each land onto the head the previous one left,
    /// then move the ref once. Lands answered before anything in the batch
    /// moved the ref are answered at once, as a lone land would be.
    ///
    /// With `spec`, the batch ahead is committing (ADR 0072): the first
    /// attempt replays onto the head that commit will leave, over its
    /// staging, answers nothing and stores nothing until that commit is
    /// known, and then either goes on as an attempt from that head or is
    /// dropped and done again from the ref once the batch ahead ended.
    /// `offer` tells the leader, when this batch starts its first commit,
    /// what the batch behind it may build on.
    async fn land_batch(
        &self,
        target_ref: &str,
        batch: Vec<Queued>,
        queue: &Mutex<RefQueue>,
        mut spec: Option<Ahead>,
        offer: oneshot::Sender<Option<Ahead>>,
    ) {
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
        let mut offer = Some(offer);
        // Held from this batch's first commit: what the batch behind waits on.
        let mut landed: Option<oneshot::Sender<bool>> = None;
        let mut _ended: Option<oneshot::Sender<()>> = None;
        // No attempt replayed a land yet: the batch may still hand lands
        // back to the queue, and is counted once.
        let mut first_replay = true;
        let mut attempt = 0;
        while attempt < MAX_LAND_ATTEMPTS {
            attempt += 1;
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
            // After the lease check, so lands a lost lease reroutes go to the
            // new holder instead of failing.
            if started.elapsed() >= MAX_BATCH_LIFETIME {
                for l in lands {
                    l.answer(Err(past_deadline(target_ref)), attempt, false);
                }
                return;
            }
            let start = match &spec {
                Some(a) => a.head,
                None => match self.meta().get_ref(target_ref).await {
                    Ok(h) => h,
                    Err(e) => {
                        for l in lands {
                            l.answer(Err(duplicate(&e)), attempt, false);
                        }
                        return;
                    }
                },
            };
            let mut head = start;
            // The replays write here; it is stored just before the ref moves.
            let staging = Arc::new(match &spec {
                Some(a) => Staging::over(&a.staging),
                None => Staging::default(),
            });
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
            // The rows the copies' skiplists are built from, read a level at
            // a time for the whole batch beside its replays, which wait for
            // them only to build a copy (ADR 0079). Started after the reads
            // above, so it never competes with them for the land lane's
            // connections. A land usually makes one copy; the slack covers a
            // few longer stacks, and copies past it (or of a batch that found
            // no plan permit) read what they lack.
            if let Some(h) = start {
                let copies = (ids.len() + PLANNED_COPY_SLACK).min(MAX_PLANNED_COPIES);
                if let Some(plan) = staged.start_copy_skiplists(h, copies) {
                    staging.set_skip_plan(plan);
                }
            }
            // The batch ahead's moves are not in the ref log until it
            // commits; this attempt is kept only if they are.
            if let (Ok(r), Some(a)) = (&mut recorded, &spec) {
                for (id, record) in &a.recorded {
                    r.entry(id.clone()).or_insert(*record);
                }
            }
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
                    staging.retain_stored(&mut origins);
                    carried = origins;
                    tokio::time::sleep(backoff(attempt)).await;
                    continue;
                }
            };
            // A sweep under way rescues each land's history at the head it
            // replays onto, which must be stored: wait for the batch ahead.
            if fenced {
                if let Some(a) = spec.take() {
                    metrics::counter!("leep_land_pipeline_total", "outcome" => "fenced")
                        .increment(1);
                    let _ = a.done.await;
                    attempt -= 1;
                    continue;
                }
            }
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
            let mut handed_back = Vec::new();
            // Answers rest on the batch ahead until its commit is known.
            let speculative = spec.is_some();
            let replay_limit = self.land_queues.replay_limit().filter(|_| first_replay);
            let mut writes = std::mem::take(&mut lock(queue).writes);
            if self.land_fast_path {
                writes.sync(start);
            }
            let replaying = Instant::now();
            #[cfg(test)]
            let mut recorded_at: Option<usize> = None;
            let mut todo = std::mem::take(&mut lands).into_iter();
            while let Some(mut l) = todo.next() {
                if attempt == 1 && l.q.reply.is_closed() {
                    metrics::counter!("leep_land_cancelled_total").increment(1);
                    continue;
                }
                // A batch of slow replays closes early: the lands it has not
                // touched wait for the next batch, at the queue's front.
                if replay_limit.is_some_and(|max| replaying.elapsed() >= max) && !taken.is_empty() {
                    handed_back.push(l);
                    handed_back.extend(todo.by_ref());
                    break;
                }
                if hold_keyed && l.q.ctx.land.is_some() {
                    lands.push(l);
                    continue;
                }
                taken.push(l.q.cs);
                // Recorded as taken, before any answer, so a test that has
                // its answers finds the batch.
                #[cfg(test)]
                if first_replay {
                    let mut b = self.land_queues.batches.lock().unwrap();
                    match recorded_at {
                        Some(i) => b[i].push(l.q.cs),
                        None => {
                            recorded_at = Some(b.len());
                            b.push(vec![l.q.cs]);
                        }
                    }
                }
                let repo = staging.repo(&l.q.repo);
                let fast = self.land_fast_path.then_some(&writes);
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
                                fast,
                                started + REPLAY_BUDGET,
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
                            fast,
                            started + REPLAY_BUDGET,
                        )
                        .await
                    }
                };
                // The ref moves to the changeset itself: store its tree if
                // its create left it out (ADR 0069).
                let step = match step {
                    Ok(Step::Move(new)) if new == l.q.cs => staging
                        .stage_tree(&repo, new)
                        .await
                        .map(|()| Step::Move(new)),
                    s => s,
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
                        if self.land_fast_path {
                            staged.follow_move(&mut writes, head, new).await;
                        }
                        head = Some(new);
                        waiting.push((l, Ok(Step::Move(new))));
                    }
                    Ok(Step::Done(resp)) if steps.is_empty() && !speculative => {
                        let result = self.store_clean(&staging, resp).await;
                        l.answer(result, attempt, false)
                    }
                    Err(e) if steps.is_empty() && !speculative => l.answer(Err(e), attempt, false),
                    other => waiting.push((l, other)),
                }
            }
            lock(queue).writes = writes;
            if !handed_back.is_empty() {
                metrics::counter!("leep_land_batch_closed_early_total").increment(1);
                let mut q = lock(queue);
                for l in handed_back.into_iter().rev() {
                    q.pending.push_front(Job::Land(l.q));
                }
                self.land_queues.report(&mut q);
            }
            if !taken.is_empty() {
                metrics::histogram!("leep_land_batch_replay_seconds")
                    .record(replaying.elapsed().as_secs_f64());
                metrics::histogram!("leep_land_batch_tree_seconds")
                    .record(staging.tree_time().as_secs_f64());
            }
            if first_replay && !taken.is_empty() {
                first_replay = false;
                metrics::histogram!("leep_land_batch_size").record(taken.len() as f64);
            }
            staging.retain_stored(&mut origins);
            carried = origins;
            // The batch ahead's commit decides this attempt, before anything
            // of it is answered or stored.
            if let Some(a) = spec.take() {
                let kept = a.landed.await.unwrap_or(false)
                    && self.still_leading_for(target_ref, &waiting, &lands).await;
                #[cfg(test)]
                self.land_queues.speculated.lock().unwrap().push((
                    waiting
                        .iter()
                        .map(|(l, _)| l.q.cs)
                        .chain(lands.iter().map(|l| l.q.cs))
                        .collect(),
                    kept,
                ));
                if !kept {
                    metrics::counter!("leep_land_pipeline_total", "outcome" => "discarded")
                        .increment(1);
                    lands.extend(waiting.into_iter().map(|(l, _)| l));
                    // Rows of the batch ahead may be among them.
                    carried.clear();
                    // Uncounted, yet bounded: `spec` is gone, so the redo
                    // reads the ref and counts, and the wait ends because
                    // the batch ahead ends (its own 32 attempts,
                    // `MAX_COMMIT_ERRORS` and lifetime bound it).
                    let _ = a.done.await;
                    attempt -= 1;
                    continue;
                }
                metrics::counter!("leep_land_pipeline_total", "outcome" => "kept").increment(1);
                // What it staged is stored now.
                staging.detach();
                if steps.is_empty() {
                    // Nothing moved: these answers preceded every move.
                    for (l, step) in std::mem::take(&mut waiting) {
                        let result = match step {
                            Ok(Step::Done(resp)) => self.store_clean(&staging, resp).await,
                            Ok(Step::Move(_)) => unreachable!("no move was staged"),
                            Err(e) => Err(e),
                        };
                        l.answer(result, attempt, false);
                    }
                }
            }
            if steps.is_empty() {
                if lands.is_empty() {
                    return;
                }
                tokio::time::sleep(backoff(attempt)).await;
                continue;
            }
            // The fence read of this attempt bounds how late its move may
            // come; past the batch's lifetime it must not come at all. Every
            // waiting result follows a move of this attempt and may rest on
            // it (a duplicate answered with that move, a stack found in or
            // conflicting with replayed history), so none is answered as is.
            if started.elapsed() >= MAX_BATCH_LIFETIME {
                for l in waiting.into_iter().map(|(l, _)| l).chain(lands) {
                    l.answer(Err(past_deadline(target_ref)), attempt, false);
                }
                return;
            }
            // The next batch may replay onto this attempt's head while it
            // commits (ADR 0072). Only the first commit is offered.
            if let Some(tx) = offer.take() {
                let ahead = self.land_queues.pipeline().then(|| {
                    let (landed_tx, landed_rx) = oneshot::channel();
                    let (ended_tx, ended_rx) = oneshot::channel();
                    landed = Some(landed_tx);
                    _ended = Some(ended_tx);
                    Ahead {
                        head,
                        staging: Arc::clone(&staging),
                        recorded: recorded.as_ref().cloned().unwrap_or_default(),
                        landed: landed_rx,
                        done: ended_rx,
                    }
                });
                let _ = tx.send(ahead);
            }
            // Whether an error came from the swap itself.
            let applied = match staging.flush(self).await {
                Err(e) => Err((e, false)),
                Ok(()) => self
                    .commit_steps(target_ref, start, &steps)
                    .await
                    .map_err(|e| (e, true)),
            };
            let mut whole = false;
            match applied {
                Ok(n) => {
                    whole = n == steps.len();
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
                        // A conflict's merged text was stored with the
                        // batch, by `staging.flush` above.
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
            // The batch behind keeps its replays only if this commit left
            // exactly the head they were made on, and nothing of this batch
            // is still to land after them.
            if let Some(tx) = landed.take() {
                let _ = tx.send(whole && lands.is_empty());
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

    /// Whether this server still coordinates `target_ref`, when any of
    /// these lands came through its lease; a batch whose replays rested on
    /// another's commit asks again before it commits them itself.
    async fn still_leading_for(
        &self,
        target_ref: &str,
        waiting: &[(InFlight, Result<Step>)],
        held: &[InFlight],
    ) -> bool {
        let Some(coord) = &self.land_coordinator else {
            return true;
        };
        let led = waiting.iter().map(|(l, _)| l).chain(held).any(|l| l.q.led);
        !led || coord.still_leading(target_ref).await
    }

    /// Store the merged text a conflict answer's `clean` changes name, which
    /// the replay staged: the client applies them, and a land that moves
    /// nothing never flushes its staging. Written through the GC rescue
    /// check and made durable before the answer; a failure is the land's
    /// answer (nothing moved, so the client may retry).
    async fn store_clean(&self, staging: &Staging, resp: LandResponse) -> Result<LandResponse> {
        if let LandResponse::Conflicts { clean, .. } = &resp {
            let hashes: Vec<Hash> = clean
                .values()
                .filter_map(|c| match c {
                    FileChange::Modify { content, .. } => Some(*content),
                    FileChange::Delete => None,
                })
                .collect();
            let blobs = staging.staged_blobs(&hashes);
            if !blobs.is_empty() {
                self.put_rescuing_many(blobs, &[]).await?;
            }
        }
        Ok(resp)
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
                None,
                tokio::time::Instant::now() + REPLAY_BUDGET,
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
        fast: Option<&WriteMap>,
        deadline: tokio::time::Instant,
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
            Some(h) if !fast_forward => match self
                .replay_stack(
                    cs,
                    h,
                    origins,
                    fast,
                    self.land_conflicts.for_ref(target_ref),
                    ReplayDeadline {
                        at: deadline,
                        target_ref,
                    },
                )
                .await?
            {
                // The stack holds `cs` at least.
                Ok(pairs) => Ok(Step::Move(pairs.last().map_or(cs, |p| p.1))),
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
    /// Otherwise each replayed changeset with its copy, oldest first.
    async fn replay_stack(
        &self,
        cs: ChangesetId,
        head: ChangesetId,
        origins: &mut HashMap<ChangesetId, Option<ChangesetId>>,
        fast: Option<&WriteMap>,
        policy: ConflictPolicy,
        deadline: ReplayDeadline<'_>,
    ) -> Result<Result<Vec<(ChangesetId, ChangesetId)>, LandResponse>> {
        let mut stack = Vec::new();
        let mut members = Vec::new();
        let mut fork = None;
        // Whether `fork` is in `head`'s history (not a member whose copy is).
        let mut trunk = false;
        let mut cur = cs;
        loop {
            deadline.check()?;
            let c = self.get_changeset(cur).await?;
            if c.parents.len() > 1 {
                return Err(Error::Invalid(format!(
                    "cannot land {cs}: unlanded changeset {cur} is a merge; merge {head} into it instead"
                )));
            }
            stack.push(cur);
            let parent = c.parents.first().copied();
            members.push(c);
            match parent {
                Some(p) if self.is_ancestor(p, head).await? => {
                    fork = Some(p);
                    trunk = true;
                    break;
                }
                Some(p) => {
                    if self.landed_copy(p, head, origins).await?.is_some() {
                        fork = Some(p);
                        break;
                    }
                    // `p` would be one member more: a stack of exactly the
                    // cap still lands.
                    if stack.len() >= MAX_LAND_STACK {
                        return Err(Error::Invalid(format!(
                            "cannot land {cs}: its stack of unlanded changesets is longer \
                             than {MAX_LAND_STACK}; land it in pieces, an ancestor first"
                        )));
                    }
                    cur = p;
                }
                None => break,
            }
        }

        let started = Instant::now();
        if let Some(map) = fast {
            let base = fork.filter(|_| trunk);
            if let Some(pairs) = self
                .replay_fast(&stack, &members, base, head, map, deadline)
                .await?
            {
                metrics::counter!("leep_land_replays_total", "path" => "fast").increment(1);
                metrics::histogram!("leep_land_replay_seconds", "path" => "fast")
                    .record(started.elapsed().as_secs_f64());
                return Ok(Ok(pairs));
            }
        }
        let replayed = self
            .replay_slow(cs, head, &stack, fork, policy, deadline)
            .await;
        metrics::counter!("leep_land_replays_total", "path" => "slow").increment(1);
        metrics::histogram!("leep_land_replay_seconds", "path" => "slow")
            .record(started.elapsed().as_secs_f64());
        replayed
    }

    /// The fast path (ADR 0066): when no change of the stack meets anything
    /// the ref wrote since `base` (the tip of the ref's first-parent line
    /// the stack starts from), each member is copied onto the head with its
    /// own changes, less those that change nothing, and no merge. That is
    /// what [`Repo::replay_slow`] makes of such a stack, to the byte: every
    /// path the stack changes is as `ours` and `base` both have it, so each
    /// three-way merge takes `theirs`, and nothing collides. `None` when the
    /// map cannot show the stack disjoint; the slow path decides then.
    async fn replay_fast(
        &self,
        stack: &[ChangesetId],
        members: &[leep_core::Changeset],
        base: Option<ChangesetId>,
        head: ChangesetId,
        map: &WriteMap,
        deadline: ReplayDeadline<'_>,
    ) -> Result<Option<Vec<(ChangesetId, ChangesetId)>>> {
        let since = match base {
            None => Err(Lookup::Ineligible),
            Some(_) if map.head() != Some(head) => Err(Lookup::Cold),
            Some(b) => map.base(b),
        };
        let since = match since {
            Ok(s) => s,
            Err(why) => {
                self.note_lookup(stack[0], why);
                return Ok(None);
            }
        };
        let touched = members
            .iter()
            .flat_map(|c| c.changes.keys())
            .any(|p| map.touched_since(since, p.as_str()));
        if touched {
            self.note_lookup(stack[0], Lookup::Overlap);
            return Ok(None);
        }
        self.note_lookup(stack[0], Lookup::Hit);
        let mut onto = head;
        let mut pairs = Vec::with_capacity(stack.len());
        for (&id, c) in stack.iter().zip(members).rev() {
            deadline.check()?;
            let parent = c.parents[0];
            // As `rebase` answers a changeset already on `onto`.
            if parent == onto {
                pairs.push((id, id));
                onto = id;
                continue;
            }
            self.rescue_changesets(&[id, onto], RescueBy::Create)
                .await?;
            let changes = self.effective_changes(parent, &c.changes).await?;
            let copy = self.rebased_copy(id, onto, changes).await?;
            pairs.push((id, copy));
            onto = copy;
        }
        Ok(Some(pairs))
    }

    fn note_lookup(&self, cs: ChangesetId, lookup: Lookup) {
        lookup.record();
        #[cfg(test)]
        self.land_queues.lookups.lock().unwrap().push((cs, lookup));
        #[cfg(not(test))]
        let _ = cs;
    }

    /// The slow path: rebase each member of `stack` (newest first) onto the
    /// head with a three-way merge, or answer the conflicts.
    async fn replay_slow(
        &self,
        cs: ChangesetId,
        head: ChangesetId,
        stack: &[ChangesetId],
        fork: Option<ChangesetId>,
        policy: ConflictPolicy,
        deadline: ReplayDeadline<'_>,
    ) -> Result<Result<Vec<(ChangesetId, ChangesetId)>, LandResponse>> {
        let mut onto = head;
        let mut pairs = Vec::with_capacity(stack.len());
        for &id in stack.iter().rev() {
            deadline.check()?;
            match self.rebase_as(id, onto, policy).await? {
                RebaseResponse::Rebased { id: copy } => {
                    pairs.push((id, copy));
                    onto = copy;
                }
                RebaseResponse::Conflicts { .. } => {
                    let m = self
                        .merge_trees_as(
                            self.tree_or_empty(fork).await?,
                            self.root_tree(head).await?,
                            self.root_tree(cs).await?,
                            policy,
                        )
                        .await?;
                    if m.conflicts.is_empty() {
                        // A member clashes with `head` alone, but the stack
                        // as a whole merges cleanly (`head` already has what
                        // the stack ends with). Land the stack as one copy of
                        // `cs`; an empty change is a legitimate copy.
                        let copy = self.rebased_copy(cs, head, m.changes).await?;
                        return Ok(Ok(vec![(cs, copy)]));
                    }
                    debug_assert!(!m.conflicts.is_empty());
                    return Ok(Err(LandResponse::Conflicts {
                        onto: head,
                        conflicts: m.conflicts,
                        clean: m.changes,
                    }));
                }
            }
        }
        Ok(Ok(pairs))
    }
}

/// `rebase-onto` (ADR 0062): the git mirror's replay of a git-primary ref's
/// unpushed lands onto what git gained. It queues with the ref's lands, so
/// it neither loses swaps to them nor drops one.
impl Repo {
    /// Replay the first-parent stack from `target_ref`'s head down to
    /// `expected_base` onto `onto`, one copy per changeset, and move the ref
    /// to the last copy in one logged move (operation `git-mirror`). 409
    /// unless the merge base of the head and `onto` is `expected_base`, so a
    /// caller deciding from an old read moves nothing. The land policy checks
    /// `onto`'s changesets the head lacks; the stack's were checked when they
    /// landed. Refused on a ref the policy [`protects`], which only moves
    /// forward. Conflicts store no changeset and move nothing.
    ///
    /// [`protects`]: crate::LandPolicy::protects
    pub async fn rebase_onto(
        &self,
        target_ref: &str,
        onto: ChangesetId,
        expected_base: ChangesetId,
        actor: Option<&str>,
        land_id: Option<LandId>,
    ) -> Result<RebaseOntoResponse> {
        self.rebase_onto_queued(target_ref, onto, expected_base, actor, land_id, false)
            .await
            .unwrap_or_else(|| {
                Err(Error::Other(
                    "rebase-onto was handed back unexpectedly; retry".into(),
                ))
            })
    }

    /// [`rebase_onto`](Self::rebase_onto) on a server holding the ref's land
    /// lease: `None` when it lost the lease first, and the caller routes the
    /// request again.
    pub(crate) async fn rebase_onto_led(
        &self,
        target_ref: &str,
        onto: ChangesetId,
        expected_base: ChangesetId,
        actor: Option<&str>,
        land_id: Option<LandId>,
    ) -> Option<Result<RebaseOntoResponse>> {
        self.rebase_onto_queued(target_ref, onto, expected_base, actor, land_id, true)
            .await
    }

    /// The refusals that need no read: a bad name, a tag, a protected ref.
    pub(crate) fn check_rebase_onto(&self, target_ref: &str) -> Result<()> {
        refname::validate(target_ref)?;
        if refname::is_tag(target_ref) {
            return Err(Error::Invalid(format!(
                "cannot rebase {target_ref:?}: tags are immutable"
            )));
        }
        if self
            .land_policy
            .as_ref()
            .is_some_and(|p| p.protects(target_ref))
        {
            return Err(Error::Invalid(format!(
                "cannot rebase {target_ref:?}: it is protected (--protect-ref) and \
                 {NOT_FORWARD_MARKER}; a ref git leads cannot be protected in Leep, so \
                 protect the branch on GitHub instead"
            )));
        }
        Ok(())
    }

    async fn rebase_onto_queued(
        &self,
        target_ref: &str,
        onto: ChangesetId,
        expected_base: ChangesetId,
        actor: Option<&str>,
        land_id: Option<LandId>,
        led: bool,
    ) -> Option<Result<RebaseOntoResponse>> {
        if let Err(e) = self.check_rebase_onto(target_ref) {
            return Some(Err(e));
        }
        let ctx = RefUpdateContext::new(GIT_MIRROR_OPERATION)
            .actor(actor)
            .message(format!("rebased onto {onto}"))
            .land(land_id.map(|id| LandKey {
                id,
                changeset: onto,
            }));
        let queue = self.land_queues.queue(target_ref);
        let (reply, answer) = oneshot::channel();
        let start_leader = {
            let mut q = lock(&queue);
            q.pending.push_back(Job::RebaseOnto(QueuedRebase {
                repo: self.clone(),
                onto,
                expected_base,
                ctx,
                reply,
                led,
            }));
            q.arrived.notify_one();
            self.land_queues.report(&mut q);
            !std::mem::replace(&mut q.leading, true)
        };
        if start_leader {
            // In the land lane (ADR 0067): request handlers holding every shared
            // metadata connection must not queue the batch they all wait on.
            let leader = lead(self.clone(), target_ref.to_string(), queue);
            let task = tokio::spawn(leep_core::lane::scope(leep_core::lane::Lane::Land, leader));
            self.land_queues.track(task.abort_handle());
        }
        answer.await.unwrap_or_else(|_| {
            Some(Err(Error::Other(format!(
                "rebase-onto of {target_ref:?} was abandoned before it finished; retry"
            ))))
        })
    }

    /// Run a queued `rebase-onto` to its answer, retrying lost swaps (other
    /// servers sharing the store) as a land batch does. `None` when a lost
    /// lease hands it back.
    async fn rebase_onto_attempts(
        &self,
        target_ref: &str,
        onto: ChangesetId,
        expected_base: ChangesetId,
        ctx: &RefUpdateContext,
        led: bool,
    ) -> Option<Result<RebaseOntoResponse>> {
        let started = tokio::time::Instant::now();
        let mut checked = Checked::default();
        let mut commit_errors = 0;
        let mut last_error = None;
        for attempt in 1..=MAX_LAND_ATTEMPTS {
            if let Some(coord) = self.land_coordinator.as_ref().filter(|_| led) {
                if !coord.still_leading(target_ref).await {
                    return None;
                }
            }
            if started.elapsed() >= MAX_BATCH_LIFETIME {
                return Some(Err(past_deadline(target_ref)));
            }
            let start = match self.meta().get_ref(target_ref).await {
                Ok(h) => h,
                Err(e) => return Some(Err(e)),
            };
            let staging = Arc::new(Staging::default());
            let replayed = self
                .rebase_onto_step(
                    target_ref,
                    start,
                    onto,
                    expected_base,
                    ctx,
                    &mut checked,
                    &staging,
                    started + REPLAY_BUDGET,
                )
                .await;
            let (head, rebased) = match replayed {
                Ok(Replayed::Move { head, rebased }) => (head, rebased),
                Ok(Replayed::Done(resp)) => return Some(Ok(resp)),
                Err(e) => return Some(Err(e)),
            };
            if started.elapsed() >= MAX_BATCH_LIFETIME {
                return Some(Err(past_deadline(target_ref)));
            }
            let step = RefStep {
                new: Some(head),
                ctx: ctx.clone(),
            };
            let applied = match staging.flush(self).await {
                Err(e) => Err(e),
                Ok(()) => self.commit_steps(target_ref, start, &[step]).await,
            };
            match applied {
                Ok(1) => return Some(Ok(RebaseOntoResponse::Moved { head, rebased })),
                // Lost to another server's move, or its record of this land
                // id: the next attempt reads both again.
                Ok(_) | Err(Error::Conflict(_)) => {
                    commit_errors = 0;
                    metrics::counter!("leep_land_cas_losses_total").increment(1);
                }
                Err(e) => {
                    commit_errors += 1;
                    if commit_errors >= MAX_COMMIT_ERRORS {
                        return Some(Err(e));
                    }
                    last_error = Some(e);
                }
            }
            tokio::time::sleep(backoff(attempt)).await;
        }
        Some(Err(last_error.unwrap_or_else(|| {
            Error::Conflict(format!(
                "rebase-onto of {target_ref:?} lost the race {MAX_LAND_ATTEMPTS} times; retry"
            ))
        })))
    }

    /// One attempt from the head `start`, up to the swap.
    #[allow(clippy::too_many_arguments)]
    async fn rebase_onto_step(
        &self,
        target_ref: &str,
        start: Option<ChangesetId>,
        onto: ChangesetId,
        expected_base: ChangesetId,
        ctx: &RefUpdateContext,
        checked: &mut Checked,
        staging: &Arc<Staging>,
        deadline: tokio::time::Instant,
    ) -> Result<Replayed> {
        // After the head: a replay recorded since would have moved it.
        if let Some(key) = &ctx.land {
            let found = self
                .meta()
                .find_lands(target_ref, std::slice::from_ref(&key.id))
                .await?;
            if let Some(record) = found.into_iter().next().flatten() {
                if record.changeset != key.changeset {
                    return Err(land_id_reused(&key.id, &record, key.changeset));
                }
                metrics::counter!("leep_land_recorded_total").increment(1);
                return Ok(Replayed::Done(RebaseOntoResponse::Moved {
                    head: record.landed,
                    rebased: self.recorded_copies(record.landed, onto).await?,
                }));
            }
        }
        let Some(head) = start else {
            return Err(Error::NotFound(format!(
                "ref {target_ref:?} does not exist"
            )));
        };
        let base = self.merge_base(head, onto).await?;
        if base != Some(expected_base) {
            let found = base.map_or_else(|| "none".to_string(), |b| b.to_string());
            return Err(Error::Conflict(format!(
                "the merge base of {target_ref:?} at {head} and {onto} is {found}, not the \
                 expected {expected_base}; read the ref again"
            )));
        }
        // `onto` is in the head already: nothing to do.
        if expected_base == onto {
            return Ok(Replayed::Done(RebaseOntoResponse::Moved {
                head,
                rebased: Vec::new(),
            }));
        }
        // Both sides become reachable from the ref; see `rescue_land`.
        if self.changeset_fence_active(true).await? {
            self.rescue_land(onto, Some(head)).await?;
            self.rescue_land(head, Some(onto)).await?;
        }
        // `onto`'s changesets the ref never had, and not the stack's: those
        // passed when they landed, and their copies keep that standing.
        self.enforce_land_policy(onto, Some(head), target_ref, checked)
            .await?;
        if expected_base == head {
            return Ok(Replayed::Move {
                head: onto,
                rebased: Vec::new(),
            });
        }
        let staged = staging.repo(self);
        match staged
            .replay_stack(
                head,
                onto,
                &mut HashMap::new(),
                None,
                ConflictPolicy::Merge,
                ReplayDeadline {
                    at: deadline,
                    target_ref,
                },
            )
            .await?
        {
            Ok(rebased) => Ok(Replayed::Move {
                head: rebased.last().map_or(onto, |p| p.1),
                rebased,
            }),
            Err(resp) => match self.store_clean(staging, resp).await? {
                LandResponse::Conflicts {
                    onto,
                    conflicts,
                    clean,
                } => Ok(Replayed::Done(RebaseOntoResponse::Conflicts {
                    onto,
                    conflicts,
                    clean,
                })),
                LandResponse::Landed { .. } => {
                    Err(Error::Other("replay answered a land, not conflicts".into()))
                }
            },
        }
    }

    /// The pairs of an applied `rebase-onto`, read back for a retry: the
    /// server-made copies on the first-parent chain from `head` down to
    /// `onto`, oldest first.
    async fn recorded_copies(
        &self,
        head: ChangesetId,
        onto: ChangesetId,
    ) -> Result<Vec<(ChangesetId, ChangesetId)>> {
        let mut pairs = Vec::new();
        let mut cur = head;
        while cur != onto && pairs.len() < LANDED_COPY_SCAN {
            let Some(original) = self.rebased_origin(cur).await? else {
                break;
            };
            pairs.push((original, cur));
            match self.parents(cur).await?.first() {
                Some(&p) => cur = p,
                None => break,
            }
        }
        pairs.reverse();
        Ok(pairs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn secs(s: f64) -> Duration {
        Duration::from_secs_f64(s)
    }

    /// A queue with `waiting` lands and a batch replaying or not; the
    /// waiting lands are counted as admitted places, which `batches` counts
    /// like queued ones.
    fn queue(waiting: usize, replaying: bool) -> RefQueue {
        RefQueue {
            reserved: waiting,
            in_flight: usize::from(replaying),
            ..RefQueue::default()
        }
    }

    #[test]
    fn batch_times_are_a_moving_average() {
        let mut q = RefQueue::default();
        assert_eq!(q.batch_secs, None);
        q.record_batch(secs(2.0));
        assert_eq!(q.batch_secs, Some(2.0), "the first batch is the average");
        q.record_batch(secs(7.0));
        assert!((q.batch_secs.unwrap() - 3.0).abs() < 1e-9, "2 + 0.2 * 5");
        q.record_batch(secs(3.0));
        assert!((q.batch_secs.unwrap() - 3.0).abs() < 1e-9);
        for _ in 0..200 {
            q.record_batch(secs(0.5));
        }
        assert!((q.batch_secs.unwrap() - 0.5).abs() < 1e-6, "converges");
    }

    #[test]
    fn retry_after_is_batch_times_rounded_up_and_clamped() {
        let mut q = RefQueue::default();
        // Before any batch finished, a batch counts as 1 s.
        assert_eq!(q.retry_after_secs(1), 1);
        assert_eq!(q.retry_after_secs(3), 3);
        assert_eq!(q.retry_after_secs(40), ADMISSION_RETRY_MAX_SECS);
        q.record_batch(secs(2.5));
        assert_eq!(q.retry_after_secs(1), 3);
        assert_eq!(q.retry_after_secs(2), 5);
        assert_eq!(q.retry_after_secs(5), 13);
        assert_eq!(q.retry_after_secs(20), ADMISSION_RETRY_MAX_SECS);
        let mut fast = RefQueue::default();
        fast.record_batch(secs(0.01));
        assert_eq!(fast.retry_after_secs(3), ADMISSION_RETRY_MIN_SECS);
        assert_eq!(fast.retry_after_secs(0), ADMISSION_RETRY_MIN_SECS);
    }

    /// A refused land waits for the next freed slot: `ahead + 1 - bound`
    /// batch times, not all `ahead` of them.
    #[test]
    fn a_refusal_waits_for_the_next_freed_slot() {
        // Bound 1, a batch replaying: one batch must finish.
        let mut q = queue(0, true);
        q.record_batch(secs(2.5));
        assert_eq!(q.batches(), 1);
        assert_eq!(q.refused_retry_after_secs(1), 3);
        // Bound 2, a batch replaying and a full one waiting: still one.
        let mut q = queue(MAX_BATCH, true);
        q.record_batch(secs(2.5));
        assert_eq!(q.batches(), 2);
        assert_eq!(q.batches_with(1), 3, "refused at a bound of 2");
        assert_eq!(q.refused_retry_after_secs(2), 3);
        // The same queue at a bound of 1 (lowered meanwhile): two.
        assert_eq!(q.refused_retry_after_secs(1), 5);
        // Bound 1, no batch replaying and a full batch waiting: one.
        let mut q = queue(MAX_BATCH, false);
        q.record_batch(secs(4.0));
        assert_eq!(q.batches_with(1), 2);
        assert_eq!(q.refused_retry_after_secs(1), 4);
        // Never more than the clamp.
        let mut q = queue(3 * MAX_BATCH, true);
        q.record_batch(secs(9.0));
        assert_eq!(q.refused_retry_after_secs(1), ADMISSION_RETRY_MAX_SECS);
    }

    /// Slow batches are waited out, not raced: a 20 s batch gets 20 s, and
    /// the ceiling follows the longest a batch may run.
    #[test]
    fn retry_after_reaches_the_batch_time_of_slow_batches() {
        assert_eq!(ADMISSION_RETRY_MAX_SECS, MAX_BATCH_LIFETIME.as_secs() / 2);
        let mut q = queue(MAX_BATCH, true);
        q.record_batch(secs(20.0));
        assert_eq!(q.refused_retry_after_secs(2), 20);
        // Twice that is past the ceiling.
        assert_eq!(q.refused_retry_after_secs(1), ADMISSION_RETRY_MAX_SECS);
    }

    /// `--land-queue-seconds` is as many batches as fit at the moving batch
    /// time, never fewer than two; a count is taken as given.
    #[test]
    fn a_time_bound_is_the_batches_that_fit_in_it() {
        let five = LandQueueBound::Seconds(secs(5.0));
        let mut q = RefQueue::default();
        assert_eq!(
            q.bound_batches(five),
            usize::MAX,
            "no batch time yet: the seconds refuse nothing"
        );
        q.record_batch(secs(0.25));
        assert_eq!(q.bound_batches(five), 20);
        let mut slow = RefQueue::default();
        slow.record_batch(secs(4.0));
        assert_eq!(slow.bound_batches(five), MIN_QUEUE_BATCHES);
        slow.record_batch(secs(40.0));
        assert_eq!(slow.bound_batches(five), MIN_QUEUE_BATCHES);
        assert_eq!(q.bound_batches(LandQueueBound::Batches(3)), 3);
    }

    /// The batch size counts in admission: batches of 4 fill sooner.
    #[test]
    fn batches_count_in_the_configured_batch_size() {
        let q = RefQueue {
            reserved: 8,
            max_batch: 4,
            ..RefQueue::default()
        };
        assert_eq!(q.batches(), 2);
        assert_eq!(q.batches_with(1), 3);
        let big = RefQueue {
            reserved: 200,
            max_batch: 256,
            ..RefQueue::default()
        };
        assert_eq!(big.batches(), 1);
    }

    /// A batch committing and the next replaying onto it count as one
    /// batch time ahead of a new land (ADR 0072), so the smallest bound
    /// still admits a filling batch behind them.
    #[test]
    fn pipelined_batches_count_once() {
        let q = RefQueue {
            reserved: 1,
            in_flight: 2,
            ..RefQueue::default()
        };
        assert_eq!(q.batches(), 2);
        assert_eq!(q.batches_with(MAX_BATCH - 1), 2);
        assert_eq!(q.batches_with(MAX_BATCH), 3);
        let five = LandQueueBound::Seconds(secs(5.0));
        let mut slow = q;
        slow.record_batch(secs(4.0));
        assert_eq!(slow.bound_batches(five), MIN_QUEUE_BATCHES);
        assert!(slow.batches_with(1) <= MIN_QUEUE_BATCHES);
    }

    #[test]
    fn a_land_joining_a_filling_batch_is_admitted() {
        // At a bound of 2, a replaying batch and 63 waiting: the 64th fits.
        let q = queue(MAX_BATCH - 1, true);
        assert_eq!(q.batches_with(1), 2);
        let q = queue(MAX_BATCH, true);
        assert_eq!(q.batches_with(1), 3);
    }
}
