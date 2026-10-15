//! `land`: replay unlanded changesets onto the ref head and compare-and-swap,
//! no merge queue.

use std::collections::hash_map::RandomState;
use std::hash::BuildHasher;
use std::sync::Arc;
use std::time::Duration;

use leep_core::api::{LandResponse, RebaseResponse};
use leep_core::{ChangesetId, Error, Result};

use crate::Repo;

/// Lands within one process are serialized per ref, so losses only come from
/// other server instances sharing the store; this bounds those retries.
const MAX_LAND_ATTEMPTS: u32 = 32;
const BACKOFF_CAP_MS: u64 = 200;

/// Full-jitter exponential backoff, so racing servers spread out instead of
/// colliding again in lockstep.
fn backoff(attempt: u32) -> Duration {
    let cap = (1u64 << attempt.min(8)).min(BACKOFF_CAP_MS);
    Duration::from_millis(RandomState::new().hash_one(attempt) % (cap + 1))
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
        self.get_changeset(cs).await?;
        let lock = {
            let mut locks = self.land_locks.lock().expect("land lock map poisoned");
            Arc::clone(locks.entry(target_ref.to_string()).or_default())
        };
        let _guard = lock.lock().await;
        for attempt in 1..=MAX_LAND_ATTEMPTS {
            let head = self.meta().get_ref(target_ref).await?;
            let new = match head {
                None => cs,
                Some(h) if h == cs || self.is_ancestor(cs, h).await? => {
                    return Ok(LandResponse::Landed {
                        landed: cs,
                        rebased: false,
                        attempts: attempt,
                    });
                }
                // Head is an ancestor (directly or through a stack of
                // unlanded changesets): fast-forward.
                Some(h) if self.is_ancestor(h, cs).await? => cs,
                Some(h) => match self.replay_stack(cs, h).await? {
                    Ok(id) => id,
                    Err(resp) => return Ok(resp),
                },
            };
            if self.meta().cas_ref(target_ref, head, Some(new)).await? {
                return Ok(LandResponse::Landed {
                    landed: new,
                    rebased: new != cs,
                    attempts: attempt,
                });
            }
            tokio::time::sleep(backoff(attempt)).await;
        }
        Err(Error::Conflict(format!(
            "land onto {target_ref:?} lost the race {MAX_LAND_ATTEMPTS} times; retry"
        )))
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
