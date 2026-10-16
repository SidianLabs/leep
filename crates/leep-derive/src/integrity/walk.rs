//! Visiting history oldest first without holding it in memory.

use std::collections::HashSet;

use async_trait::async_trait;
use leep_core::{Changeset, ChangesetId, MetaStore, Result};

use super::problems::{damage_of, Damage};

/// What a walk calls back. Parents are always visited before their children.
#[async_trait]
pub(crate) trait Visitor: Send {
    /// A changeset, after all its readable ancestors. `parent_broken` is true
    /// when a parent could not be read, so rows computed from parents cannot
    /// be checked.
    async fn visit(&mut self, id: ChangesetId, cs: &Changeset, parent_broken: bool) -> Result<()>;

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
