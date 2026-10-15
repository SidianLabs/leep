//! `land`: replay unlanded changesets onto the ref head and compare-and-swap,
//! no merge queue.

use std::collections::hash_map::RandomState;
use std::collections::HashSet;
use std::hash::BuildHasher;
use std::sync::Arc;
use std::time::{Duration, Instant};

use leep_core::api::{LandResponse, RebaseResponse};
use leep_core::refname::{self, NOT_FORWARD_MARKER};
use leep_core::{ChangesetId, Error, RefUpdateContext, Result};

use crate::Repo;

/// Lands within one process are serialized per ref, so losses only come from
/// other server instances sharing the store; this bounds those retries.
const MAX_LAND_ATTEMPTS: u32 = 32;
const BACKOFF_CAP_MS: u64 = 200;
/// Consecutive failed commits tolerated before the error is returned. Each is
/// re-driven from a fresh read, which resolves an ambiguous commit.
const MAX_COMMIT_ERRORS: u32 = 3;
/// Changesets examined when looking for an earlier landing of the same stack.
const LANDED_COPY_SCAN: usize = 1024;

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

impl Repo {
    /// Land `cs` on `target_ref`. Fast-forwards when the head is an ancestor;
    /// otherwise replays the unlanded stack under `cs` onto the head. Retries
    /// when the CAS loses; conflicts are returned, never resolved here.
    ///
    /// Lands on the same ref are serialized within this process: under
    /// contention every attempt would otherwise redo the replay only to lose
    /// the CAS to a sibling request. The CAS still guards against other
    /// processes, with jittered backoff between losses.
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
        let started = Instant::now();
        let mut stats = LandStats::default();
        let ctx = RefUpdateContext::new("land")
            .actor(actor)
            .message(format!("landed {cs}"));
        let result = self.land_inner(cs, target_ref, &ctx, &mut stats).await;
        stats.record(&result, started.elapsed());
        result
    }

    async fn land_inner(
        &self,
        cs: ChangesetId,
        target_ref: &str,
        ctx: &RefUpdateContext,
        stats: &mut LandStats,
    ) -> Result<LandResponse> {
        refname::validate(target_ref)?;
        if refname::is_tag(target_ref) {
            return Err(Error::Invalid(format!(
                "cannot land onto {target_ref:?}: tags are immutable"
            )));
        }
        self.get_changeset(cs).await?;
        let lock = {
            let mut locks = self.land_locks.lock().expect("land lock map poisoned");
            Arc::clone(locks.entry(target_ref.to_string()).or_default())
        };
        let waited = Instant::now();
        let _guard = lock.lock().await;
        metrics::histogram!("leep_land_lock_wait_seconds").record(waited.elapsed().as_secs_f64());
        // A changeset that passed the policy need not be checked again when a
        // lost CAS retries, unless the policy reads the head (see `Checked`).
        let mut checked = Checked::default();
        let mut commit_errors = 0;
        let mut last_error = None;
        for attempt in 1..=MAX_LAND_ATTEMPTS {
            stats.attempts = attempt;
            let head = self.meta().get_ref(target_ref).await?;
            let mut fast_forward = false;
            if let Some(h) = head {
                if h == cs || self.is_ancestor(cs, h).await? {
                    return Ok(LandResponse::Landed {
                        landed: cs,
                        rebased: false,
                        attempts: attempt,
                    });
                }
                // Head is an ancestor (directly or through a stack of
                // unlanded changesets): fast-forward.
                fast_forward = self.is_ancestor(h, cs).await?;
                // An earlier call may have landed this stack and lost its
                // answer; replaying it would add the stack a second time.
                if !fast_forward {
                    if let Some(copy) = self.landed_copy(cs, h).await? {
                        return Ok(LandResponse::Landed {
                            landed: copy,
                            rebased: true,
                            attempts: attempt,
                        });
                    }
                }
            }
            self.enforce_land_policy(cs, head, target_ref, &mut checked)
                .await?;
            let new = match head {
                Some(h) if !fast_forward => match self.replay_stack(cs, h).await? {
                    Ok(id) => id,
                    Err(resp) => return Ok(resp),
                },
                _ => cs,
            };
            match self
                .meta()
                .cas_ref_logged(target_ref, head, Some(new), ctx)
                .await
            {
                Ok(true) => {
                    return Ok(LandResponse::Landed {
                        landed: new,
                        rebased: new != cs,
                        attempts: attempt,
                    });
                }
                Ok(false) => {
                    commit_errors = 0;
                    metrics::counter!("leep_land_cas_losses_total").increment(1);
                }
                // The commit may or may not have applied. Another pass reads
                // the head again and either finds the stack in it or retries.
                Err(e) => {
                    commit_errors += 1;
                    if commit_errors >= MAX_COMMIT_ERRORS {
                        return Err(e);
                    }
                    last_error = Some(e);
                }
            }
            tokio::time::sleep(backoff(attempt)).await;
        }
        if let Some(e) = last_error {
            return Err(e);
        }
        stats.exhausted = true;
        Err(Error::Conflict(format!(
            "land onto {target_ref:?} lost the race {MAX_LAND_ATTEMPTS} times; retry"
        )))
    }

    /// The server's rebased copy of `cs` among `head`'s ancestors, if an
    /// earlier land of `cs` already put one there. Only rows written by
    /// `rebase` count, so a changeset that merely claims `leep.rebased_from`
    /// cannot make a land look done.
    ///
    /// A copy sits on a head that was not below `cs`, so its generation is
    /// above `cs`'s; the walk stops at that bound, and after
    /// [`LANDED_COPY_SCAN`] changesets, past which a retry may land twice.
    async fn landed_copy(&self, cs: ChangesetId, head: ChangesetId) -> Result<Option<ChangesetId>> {
        let floor = self.generation(cs).await?;
        let mut seen = HashSet::from([head]);
        let mut stack = vec![head];
        let mut scanned = 0;
        while let Some(id) = stack.pop() {
            if self.rebased_origin(id).await? == Some(cs) {
                return Ok(Some(id));
            }
            scanned += 1;
            if scanned >= LANDED_COPY_SCAN {
                break;
            }
            for p in self.get_changeset(id).await?.parents {
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
