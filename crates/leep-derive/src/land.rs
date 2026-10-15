//! `land`: replay unlanded changesets onto the ref head and compare-and-swap,
//! no merge queue.

use leep_core::api::{LandResponse, RebaseResponse};
use leep_core::{ChangesetId, Error, Result};

use crate::Repo;

const MAX_LAND_ATTEMPTS: u32 = 16;

impl Repo {
    /// Land `cs` on `target_ref`. Fast-forwards when the head is an ancestor;
    /// otherwise replays the unlanded stack under `cs` onto the head. Retries
    /// when the CAS loses; conflicts are returned, never resolved here.
    pub async fn land(&self, cs: ChangesetId, target_ref: &str) -> Result<LandResponse> {
        self.get_changeset(cs).await?;
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
            tokio::task::yield_now().await;
        }
        Err(Error::Other(format!(
            "land onto {target_ref:?} lost the race {MAX_LAND_ATTEMPTS} times"
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
