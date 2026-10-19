//! Visiting history oldest first without holding it in memory.

use std::collections::{HashMap, HashSet};
use std::ops::Range;

use async_trait::async_trait;
use leep_core::{Changeset, ChangesetId, MetaStore, Result};

use super::problems::{damage_of, Damage};
use crate::derived::wavefront::Dag;

/// What a walk calls back. Parents are always visited before their children.
#[async_trait]
pub(crate) trait Visitor: Unreadable {
    /// A changeset, after all its readable ancestors. `parent_broken` is true
    /// when a parent could not be read, so rows computed from parents cannot
    /// be checked.
    async fn visit(&mut self, id: ChangesetId, cs: &Changeset, parent_broken: bool) -> Result<()>;
}

/// What a walk tells about changesets it cannot read.
#[async_trait]
pub(crate) trait Unreadable: Send {
    /// `id` could not be read. `referenced_by` is the child that names it, or
    /// `None` when it was a starting point.
    async fn broken(
        &mut self,
        id: ChangesetId,
        referenced_by: Option<ChangesetId>,
        damage: Damage,
        detail: String,
    ) -> Result<()>;
}

struct Frame {
    id: ChangesetId,
    parents: Vec<ChangesetId>,
    next: usize,
    broken_parent: bool,
}

/// A depth-first, post-order walk over any number of starting points, with one
/// visited set shared between them. The stack holds an id and a parent list
/// per level, never changesets, so a history of any depth costs about a
/// hundred bytes a level; each changeset is read once on the way down and once
/// to be visited.
#[derive(Default)]
pub(crate) struct HistoryWalk {
    visited: HashSet<ChangesetId>,
    broken: HashSet<ChangesetId>,
}

enum Read {
    Ok(Changeset),
    Damaged(Damage, String),
}

async fn read(meta: &dyn MetaStore, id: ChangesetId) -> Result<Read> {
    match meta.get_changeset(&id).await {
        Ok(Some(cs)) => Ok(Read::Ok(cs)),
        Ok(None) => Ok(Read::Damaged(Damage::Missing, "no such changeset".into())),
        Err(e) => match damage_of(&e) {
            Some(d) => Ok(Read::Damaged(d, e.to_string())),
            None => Err(e),
        },
    }
}

impl HistoryWalk {
    pub fn new() -> Self {
        Self::default()
    }

    /// Visit `tip` and every ancestor not visited by an earlier call.
    pub async fn run_from(
        &mut self,
        meta: &dyn MetaStore,
        tip: ChangesetId,
        v: &mut dyn Visitor,
    ) -> Result<()> {
        if !self.visited.insert(tip) {
            return Ok(());
        }
        match read(meta, tip).await? {
            Read::Ok(cs) => {
                let mut stack = vec![Frame {
                    id: tip,
                    parents: cs.parents,
                    next: 0,
                    broken_parent: false,
                }];
                self.drain(meta, &mut stack, v).await
            }
            Read::Damaged(damage, detail) => {
                self.broken.insert(tip);
                v.broken(tip, None, damage, detail).await
            }
        }
    }

    async fn drain(
        &mut self,
        meta: &dyn MetaStore,
        stack: &mut Vec<Frame>,
        v: &mut dyn Visitor,
    ) -> Result<()> {
        while let Some(top) = stack.last_mut() {
            if let Some(&p) = top.parents.get(top.next) {
                top.next += 1;
                if !self.visited.insert(p) {
                    top.broken_parent |= self.broken.contains(&p);
                    continue;
                }
                match read(meta, p).await? {
                    Read::Ok(cs) => stack.push(Frame {
                        id: p,
                        parents: cs.parents,
                        next: 0,
                        broken_parent: false,
                    }),
                    Read::Damaged(damage, detail) => {
                        self.broken.insert(p);
                        top.broken_parent = true;
                        let child = top.id;
                        v.broken(p, Some(child), damage, detail).await?;
                    }
                }
            } else {
                let f = stack.pop().expect("non-empty");
                match read(meta, f.id).await? {
                    Read::Ok(cs) => v.visit(f.id, &cs, f.broken_parent).await?,
                    Read::Damaged(damage, detail) => {
                        self.broken.insert(f.id);
                        v.broken(f.id, None, damage, detail).await?;
                    }
                }
            }
        }
        Ok(())
    }
}

/// Changesets read per round trip while planning.
const PLAN_BATCH: usize = 256;

/// Every readable changeset reachable from `starts`, parents first, for a
/// rederivation on several cores (ADR 0050). Unreadable changesets are
/// reported to `v` once, as [`HistoryWalk`] does, and left out: a changeset
/// whose parent is outside the result has a broken parent. Changesets are
/// read a batch per round trip and only their parents kept; the order is the
/// post-order of a walk from the starts in turn, parents in order, so the
/// visits run in [`HistoryWalk`]'s order with one job.
pub(crate) async fn plan(
    meta: &dyn MetaStore,
    starts: &[ChangesetId],
    v: &mut dyn Unreadable,
) -> Result<Dag> {
    let mut parents: HashMap<ChangesetId, Range<usize>> = HashMap::new();
    let mut arena: Vec<ChangesetId> = Vec::new();
    let mut seen: HashSet<ChangesetId> = HashSet::new();
    let mut frontier: Vec<(ChangesetId, Option<ChangesetId>)> = starts
        .iter()
        .filter(|id| seen.insert(**id))
        .map(|id| (*id, None))
        .collect();
    while !frontier.is_empty() {
        let mut next = Vec::new();
        for chunk in frontier.chunks(PLAN_BATCH) {
            let ids: Vec<ChangesetId> = chunk.iter().map(|(id, _)| *id).collect();
            for ((id, child), got) in chunk.iter().zip(read_many(meta, &ids).await?) {
                match got {
                    Read::Ok(cs) => {
                        let start = arena.len();
                        arena.extend_from_slice(&cs.parents);
                        parents.insert(*id, start..arena.len());
                        next.extend(
                            cs.parents
                                .iter()
                                .filter(|p| seen.insert(**p))
                                .map(|p| (*p, Some(*id))),
                        );
                    }
                    Read::Damaged(damage, detail) => v.broken(*id, *child, damage, detail).await?,
                }
            }
        }
        frontier = next;
    }
    drop(seen);

    let mut order = Vec::with_capacity(parents.len());
    let mut done: HashSet<ChangesetId> = HashSet::with_capacity(parents.len());
    for &tip in starts {
        if !parents.contains_key(&tip) || !done.insert(tip) {
            continue;
        }
        let mut stack = vec![(tip, 0usize)];
        while let Some((top, next)) = stack.last_mut() {
            let ps = &arena[parents[top].clone()];
            if let Some(&p) = ps.get(*next) {
                *next += 1;
                if parents.contains_key(&p) && done.insert(p) {
                    stack.push((p, 0));
                }
            } else {
                order.push(*top);
                stack.pop();
            }
        }
    }
    drop(done);
    Ok(Dag::new(order, |id| &arena[parents[id].clone()]))
}

/// `ids`, in one read if the store can, else one by one, which tells damage
/// apart from a missing changeset.
async fn read_many(meta: &dyn MetaStore, ids: &[ChangesetId]) -> Result<Vec<Read>> {
    if let Ok(v) = meta.get_changesets(ids).await {
        if v.len() == ids.len() && v.iter().all(Option::is_some) {
            return Ok(v
                .into_iter()
                .map(|cs| Read::Ok(cs.expect("checked")))
                .collect());
        }
    }
    let mut out = Vec::with_capacity(ids.len());
    for &id in ids {
        out.push(read(meta, id).await?);
    }
    Ok(out)
}
