//! The writers' side of garbage collection of unreachable changesets
//! (ADR 0059): the fence rows a sweep will write, and the rescues every
//! writer makes before it stores something that names a changeset.
//!
//! The protocol is ADR 0012's for blobs. A sweep condemns unreachable
//! changesets, waits a fence, and deletes the ones still condemned. A writer
//! about to make durable something that names an existing changeset *X* (a
//! child, a ref move, a land, a rebase link, an attestation, a mirror map)
//! first reads *X*'s fence state, before it checks that *X* exists. If *X*
//! is condemned it writes `cs-rescue-v1` for *X* and every condemned
//! ancestor, then reads the delete handshake (`cs-doom-v1`) and waits out a
//! delete in progress. Collect writes the handshake before it reads the
//! rescue, so one of the two always sees the other.
//!
//! Requests read `gc-fence-active-v1` at most once every
//! [`FENCE_FLAG_TTL`] and skip every per-changeset read while it is 0, so
//! nothing costs anything until a sweep condemns. The sweep itself is in
//! `changeset_sweep`.

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use std::time::Duration;

use leep_core::{ChangesetId, DerivedRow, Error, Hash, Result};

use crate::gc::{decode_doom_of, decode_time, time_value, Doom, DOOM_POLL, DOOM_WAIT};
use crate::Repo;

/// A changeset a sweep has condemned (value: unix seconds, big-endian, from
/// the sweep host's clock). Condemned iff greater than its rescue.
pub const CS_CONDEMNED: &str = "cs-condemned-v1";

/// The condemnation a writer answered (value: that condemnation's time,
/// big-endian), so racing writers store the same bytes.
pub const CS_RESCUE: &str = "cs-rescue-v1";

/// The delete handshake (value: condemnation time, big-endian, then one
/// state byte: 0 deleting, 1 abandoned, 2 deleted), as `blob-doom-v1`.
pub const CS_DOOM: &str = "cs-doom-v1";

/// One row, under [`fence_flag_key`]: unix seconds, big-endian. Nonzero
/// while a condemnation round is open: when the first condemn of the round
/// began, and from its end on when the latest condemn finished (collect
/// waits a fence after that). 0 when nothing is condemned.
pub const GC_FENCE_ACTIVE: &str = "gc-fence-active-v1";

/// How long a request may act on a cached [`GC_FENCE_ACTIVE`]. The sweep's
/// fence must exceed the request timeout plus this (ADR 0059 section 3).
pub const FENCE_FLAG_TTL: Duration = Duration::from_secs(10);

/// The key of the [`GC_FENCE_ACTIVE`] row. No changeset id is all zeros.
pub fn fence_flag_key() -> Hash {
    Hash([0; 32])
}

/// What a writer was about to do when it rescued, for
/// `leep_gc_changesets_rescued_total{by}`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RescueBy {
    /// A changeset with the rescued one as a parent, or a merge or rebase
    /// of it.
    Create,
    /// A land of it or of a stack it is in.
    Land,
    /// A ref move to it.
    Ref,
    /// A signature or review of it.
    Attest,
    /// A git mirror map entry for it.
    Mirror,
}

impl RescueBy {
    fn label(self) -> &'static str {
        match self {
            RescueBy::Create => "create",
            RescueBy::Land => "land",
            RescueBy::Ref => "ref",
            RescueBy::Attest => "attest",
            RescueBy::Mirror => "mirror",
        }
    }
}

/// This process's last read of [`GC_FENCE_ACTIVE`].
#[derive(Default)]
pub(crate) struct FenceFlag {
    read: Mutex<Option<(tokio::time::Instant, bool)>>,
}

/// One changeset's fence rows, read condemnation first.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct CsFence {
    pub(crate) condemned: Option<i64>,
    pub(crate) rescued: Option<i64>,
    pub(crate) doom: Option<(i64, Doom)>,
}

impl CsFence {
    pub(crate) fn is_condemned(&self) -> bool {
        match self.condemned {
            Some(c) => self.rescued.is_none_or(|r| c > r),
            None => false,
        }
    }

    /// The handshake state of the current condemnation.
    pub(crate) fn doom_state(&self) -> Option<Doom> {
        match (self.condemned, self.doom) {
            (Some(c), Some((e, d))) if e == c => Some(d),
            _ => None,
        }
    }
}

/// Whether a [`GC_FENCE_ACTIVE`] value says a round is under way. A value
/// that does not decode counts as active: it only costs reads.
fn flag_active(v: Option<&[u8]>) -> bool {
    match v {
        None => false,
        Some(v) => decode_time(GC_FENCE_ACTIVE, v).map_or(true, |t| t != 0),
    }
}

impl Repo {
    /// Whether a changeset sweep may have condemned anything. Requests use a
    /// read at most [`FENCE_FLAG_TTL`] old; `fresh` reads the row now (land
    /// batch attempts, which may outlive their request).
    pub(crate) async fn changeset_fence_active(&self, fresh: bool) -> Result<bool> {
        if !fresh {
            let cached = *self.fence_flag.read.lock().unwrap();
            if let Some((at, active)) = cached {
                if at.elapsed() < FENCE_FLAG_TTL {
                    return Ok(active);
                }
            }
        }
        let started = tokio::time::Instant::now();
        let v = self
            .meta()
            .get_derived(GC_FENCE_ACTIVE, &fence_flag_key())
            .await?;
        let active = flag_active(v.as_deref());
        metrics::gauge!("leep_gc_fence_active").set(f64::from(u8::from(active)));
        *self.fence_flag.read.lock().unwrap() = Some((started, active));
        Ok(active)
    }

    /// Call before storing anything that names `ids`, and before checking
    /// that they exist: rescues every condemned one and its condemned
    /// ancestors, and waits out a delete of any of them in progress (up to
    /// 30 s, then a retryable `Conflict`). A changeset already deleted is
    /// left to the caller's existence check, which then fails as if it had
    /// never been stored. Reads one cached row unless a sweep is under way.
    ///
    /// Handles whose writes are staged (a land batch's replays) skip this:
    /// the batch rescues for them, uncached, before it moves the ref.
    pub async fn rescue_changesets(&self, ids: &[ChangesetId], by: RescueBy) -> Result<()> {
        if ids.is_empty() || !self.rescues_on_write {
            return Ok(());
        }
        if !self.changeset_fence_active(false).await? {
            return Ok(());
        }
        self.rescue_condemned(ids, by).await
    }

    /// [`rescue_changesets`](Self::rescue_changesets) without the flag read,
    /// for a caller that read it itself.
    pub(crate) async fn rescue_condemned(&self, ids: &[ChangesetId], by: RescueBy) -> Result<()> {
        let mut fences: HashMap<ChangesetId, CsFence> = HashMap::new();
        let first = self.cs_fences(ids).await?;
        let mut seen: HashSet<ChangesetId> = HashSet::new();
        let mut stack = Vec::new();
        // A rescued changeset whose delete was announced still waits for
        // collect to abandon it.
        let mut check = Vec::new();
        for (&id, f) in ids.iter().zip(first) {
            if !seen.insert(id) {
                continue;
            }
            if f.is_condemned() {
                stack.push(id);
            } else if f.doom_state() == Some(Doom::Deleting) {
                check.push(id);
            }
            fences.insert(id, f);
        }
        if stack.is_empty() && check.is_empty() {
            return Ok(());
        }
        // Condemned ancestors, iteratively: a sweep condemns a changeset only
        // with every descendant, so a condemned changeset's live parents end
        // the walk.
        let mut order = Vec::new();
        let mut parents_of: HashMap<ChangesetId, Vec<ChangesetId>> = HashMap::new();
        let mut gone = HashSet::new();
        while let Some(id) = stack.pop() {
            let parents = match self.meta().get_changeset(&id).await? {
                Some(cs) => cs.parents,
                // Deleted: the caller's existence check reports it, and a
                // rescue row would only be an orphan for the tombstone pass.
                None => {
                    gone.insert(id);
                    continue;
                }
            };
            order.push(id);
            let fresh: Vec<ChangesetId> = parents
                .iter()
                .copied()
                .filter(|p| seen.insert(*p))
                .collect();
            parents_of.insert(id, parents);
            if fresh.is_empty() {
                continue;
            }
            for (p, f) in fresh.iter().zip(self.cs_fences(&fresh).await?) {
                if f.is_condemned() {
                    stack.push(*p);
                }
                fences.insert(*p, f);
            }
        }
        // Ancestors first, so that any prefix of the rows a failed write
        // leaves (`put_derived_many` may store one) rescues only changesets
        // whose condemned ancestors are rescued too. Collect then never
        // finds a rescued changeset with a deleted parent; the next writer
        // walks again from what is still condemned.
        let rows: Vec<DerivedRow> = ancestors_first(&order, &parents_of)
            .into_iter()
            .filter_map(|id| {
                let c = fences.get(&id)?.condemned?;
                Some(DerivedRow::new(CS_RESCUE, id.0, time_value(c)))
            })
            .collect();
        if !rows.is_empty() {
            self.meta().put_derived_many(&rows).await?;
            metrics::counter!("leep_gc_changesets_rescued_total", "by" => by.label())
                .increment(rows.len() as u64);
        }
        check.extend(order);
        check.extend(gone);
        self.wait_for_doom(&check).await
    }

    /// After writing rescues: wait while collect is deleting any of `ids`.
    async fn wait_for_doom(&self, ids: &[ChangesetId]) -> Result<()> {
        let mut waited = Duration::ZERO;
        loop {
            let deleting: Vec<ChangesetId> = ids
                .iter()
                .zip(self.cs_fences(ids).await?)
                .filter(|(_, f)| f.doom_state() == Some(Doom::Deleting))
                .map(|(id, _)| *id)
                .collect();
            let Some(first) = deleting.first() else {
                return Ok(());
            };
            if waited >= DOOM_WAIT {
                // A delete announced and never finished (a collect killed
                // between doom and delete) keeps answering this until the
                // next collect: `LeepChangesetDoomStuck`.
                metrics::counter!("leep_gc_changeset_doom_conflicts_total").increment(1);
                return Err(Error::Conflict(format!(
                    "changeset {first} is being garbage collected; retry"
                )));
            }
            tokio::time::sleep(DOOM_POLL).await;
            waited += DOOM_POLL;
        }
    }

    /// The fence rows of `ids`, one per id, read from durable meta:
    /// condemnations in one batch, then the rescue and doom rows of the
    /// condemned ones.
    pub(crate) async fn cs_fences(&self, ids: &[ChangesetId]) -> Result<Vec<CsFence>> {
        let keys: Vec<Hash> = ids.iter().map(|id| id.0).collect();
        let condemned = self.meta().get_derived_many(CS_CONDEMNED, &keys).await?;
        let mut out: Vec<CsFence> = Vec::with_capacity(ids.len());
        let mut ask = Vec::new();
        for (i, row) in condemned.iter().enumerate() {
            let condemned = row
                .as_ref()
                .map(|v| decode_time(CS_CONDEMNED, v))
                .transpose()?;
            if condemned.is_some() {
                ask.push(i);
            }
            out.push(CsFence {
                condemned,
                ..CsFence::default()
            });
        }
        if ask.is_empty() {
            return Ok(out);
        }
        let ask_keys: Vec<Hash> = ask.iter().map(|&i| keys[i]).collect();
        let (rescues, dooms) = tokio::join!(
            self.meta().get_derived_many(CS_RESCUE, &ask_keys),
            self.meta().get_derived_many(CS_DOOM, &ask_keys)
        );
        for ((&i, r), d) in ask.iter().zip(rescues?).zip(dooms?) {
            out[i].rescued = r.map(|v| decode_time(CS_RESCUE, &v)).transpose()?;
            out[i].doom = d.map(|v| decode_doom_of(CS_DOOM, &v)).transpose()?;
        }
        Ok(out)
    }
}

/// `ids` ordered so every changeset comes after those of its ancestors
/// that are in `ids`: an iterative post-order over parent edges. A parent
/// outside `ids` (live, or not read) imposes nothing.
pub(crate) fn ancestors_first(
    ids: &[ChangesetId],
    parents_of: &HashMap<ChangesetId, Vec<ChangesetId>>,
) -> Vec<ChangesetId> {
    let members: HashSet<ChangesetId> = ids.iter().copied().collect();
    let mut done: HashSet<ChangesetId> = HashSet::with_capacity(ids.len());
    let mut out = Vec::with_capacity(ids.len());
    for &start in ids {
        if done.contains(&start) {
            continue;
        }
        let mut stack = vec![(start, false)];
        while let Some((id, expanded)) = stack.pop() {
            if done.contains(&id) {
                continue;
            }
            if expanded {
                done.insert(id);
                out.push(id);
                continue;
            }
            stack.push((id, true));
            for p in parents_of.get(&id).into_iter().flatten() {
                if members.contains(p) && !done.contains(p) {
                    stack.push((*p, false));
                }
            }
        }
    }
    out
}

/// Test support: condemn `ids` at `at` and raise the flag, as a sweep's
/// condemn would, without the mark. `repo` forgets its cached flag, as if
/// [`FENCE_FLAG_TTL`] had passed; other handles keep theirs.
#[cfg(test)]
pub(crate) async fn condemn_for_test(repo: &Repo, ids: &[ChangesetId], at: i64) {
    *repo.fence_flag.read.lock().unwrap() = None;
    let mut rows: Vec<DerivedRow> = ids
        .iter()
        .map(|id| DerivedRow::new(CS_CONDEMNED, id.0, time_value(at)))
        .collect();
    rows.push(DerivedRow::new(
        GC_FENCE_ACTIVE,
        fence_flag_key(),
        time_value(at),
    ));
    repo.meta().put_derived_many(&rows).await.unwrap();
}

/// Test support: whether `id` is condemned now.
#[cfg(test)]
pub(crate) async fn is_condemned_for_test(repo: &Repo, id: ChangesetId) -> bool {
    repo.cs_fences(&[id]).await.unwrap()[0].is_condemned()
}

/// Test support: announce or finish the delete of `id` for its current
/// condemnation, as collect would.
#[cfg(test)]
pub(crate) async fn doom_for_test(repo: &Repo, id: ChangesetId, state: Doom) {
    let c = repo.cs_fences(&[id]).await.unwrap()[0]
        .condemned
        .expect("condemned");
    repo.meta()
        .put_derived(CS_DOOM, &id.0, crate::gc::doom_value(c, state))
        .await
        .unwrap();
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::{commit, repo};

    /// Golden bytes: changing them orphans every condemnation and rescue.
    #[tokio::test(start_paused = true)]
    async fn cs_row_encodings_are_stable() {
        assert_eq!(fence_flag_key(), Hash([0; 32]));
        let r = repo();
        let a = commit(&r, &[], &[("f", Some("1"))], "a").await;
        condemn_for_test(&r, &[a], 1_700_000_000).await;
        let t = [0, 0, 0, 0, 0x65, 0x53, 0xf1, 0x00];
        let flag = r.meta().get_derived(GC_FENCE_ACTIVE, &Hash([0; 32]));
        assert_eq!(flag.await.unwrap().as_deref(), Some(&t[..]));
        r.rescue_changesets(&[a], RescueBy::Create).await.unwrap();
        let rescue = r.meta().get_derived(CS_RESCUE, &a.0).await.unwrap();
        assert_eq!(rescue.as_deref(), Some(&t[..]));
        doom_for_test(&r, a, Doom::Deleted).await;
        let doom = r.meta().get_derived(CS_DOOM, &a.0).await.unwrap().unwrap();
        assert_eq!(&doom[..8], &t[..]);
        assert_eq!(doom[8], 2);
        assert_eq!(
            crate::gc::decode_row(CS_DOOM, &doom).unwrap(),
            (1_700_000_000, Some(2))
        );
    }

    #[test]
    fn the_flag_is_off_only_when_absent_or_zero() {
        assert!(!flag_active(None));
        assert!(!flag_active(Some(&0i64.to_be_bytes())));
        assert!(flag_active(Some(&7i64.to_be_bytes())));
        assert!(
            flag_active(Some(b"junk")),
            "a damaged flag costs reads, never safety"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn nothing_is_read_while_the_flag_is_down() {
        let r = repo();
        let a = commit(&r, &[], &[("f", Some("1"))], "a").await;
        condemn_for_test(&r, &[a], 100).await;
        r.meta()
            .put_derived(GC_FENCE_ACTIVE, &fence_flag_key(), time_value(0))
            .await
            .unwrap();
        // Down: the condemned row is not even looked at.
        r.rescue_changesets(&[a], RescueBy::Create).await.unwrap();
        assert!(is_condemned_for_test(&r, a).await);
        // Raised: a request acts on its cached read until the TTL passes.
        r.meta()
            .put_derived(GC_FENCE_ACTIVE, &fence_flag_key(), time_value(100))
            .await
            .unwrap();
        r.rescue_changesets(&[a], RescueBy::Create).await.unwrap();
        assert!(is_condemned_for_test(&r, a).await, "cached as down");
        tokio::time::advance(FENCE_FLAG_TTL).await;
        r.rescue_changesets(&[a], RescueBy::Create).await.unwrap();
        assert!(!is_condemned_for_test(&r, a).await, "rescued");
        // An uncached read sees a change at once.
        r.meta()
            .put_derived(GC_FENCE_ACTIVE, &fence_flag_key(), time_value(0))
            .await
            .unwrap();
        assert!(!r.changeset_fence_active(true).await.unwrap());
    }

    #[tokio::test(start_paused = true)]
    async fn a_rescue_covers_condemned_ancestors_and_stops_at_live_ones() {
        let r = repo();
        let base = commit(&r, &[], &[("f", Some("0"))], "base").await;
        let a = commit(&r, &[base], &[("f", Some("1"))], "a").await;
        let b = commit(&r, &[a], &[("f", Some("2"))], "b").await;
        let other = commit(&r, &[base], &[("g", Some("1"))], "other").await;
        condemn_for_test(&r, &[a, b, other], 100).await;
        r.rescue_changesets(&[b], RescueBy::Land).await.unwrap();
        assert!(!is_condemned_for_test(&r, b).await);
        assert!(!is_condemned_for_test(&r, a).await);
        assert!(is_condemned_for_test(&r, other).await, "not an ancestor");
        let row = r.meta().get_derived(CS_RESCUE, &a.0).await.unwrap();
        assert_eq!(row.as_deref(), Some(&100i64.to_be_bytes()[..]));
        // A later condemnation makes the old rescue stale.
        condemn_for_test(&r, &[a], 200).await;
        assert!(is_condemned_for_test(&r, a).await);
    }

    /// A rescue of two condemned siblings and their condemned ancestry,
    /// cut short after every possible prefix of its rows (the trait lets
    /// `put_derived_many` store any prefix, and the fault wrapper writes
    /// row by row): no prefix rescues a changeset whose condemned parent it
    /// leaves condemned. Reverse discovery order wrote a sibling before the
    /// parent they share.
    #[tokio::test(start_paused = true)]
    async fn rescue_rows_are_written_ancestors_first() {
        use leep_testkit::{Fault, FaultPlan, FaultyMetaStore, Op};
        for cut in 1..=5u64 {
            let inner = std::sync::Arc::new(leep_core::memory::MemoryMetaStore::new());
            let blobs = std::sync::Arc::new(leep_core::memory::MemoryBlobStore::new());
            let clean = Repo::new(blobs.clone(), inner.clone());
            let g = commit(&clean, &[], &[("f", Some("g"))], "g").await;
            let p = commit(&clean, &[g], &[("f", Some("p"))], "p").await;
            let c1 = commit(&clean, &[p], &[("f", Some("c1"))], "c1").await;
            let c2 = commit(&clean, &[p], &[("f", Some("c2"))], "c2").await;
            condemn_for_test(&clean, &[g, p, c1, c2], 100).await;
            let plan = FaultPlan::new(cut);
            plan.script(Op::PutDerived, cut, Fault::Fail);
            let faulty = Repo::new(
                blobs,
                std::sync::Arc::new(FaultyMetaStore::new(inner, plan)),
            );
            let res = faulty.rescue_condemned(&[c1, c2], RescueBy::Create).await;
            assert_eq!(res.is_ok(), cut == 5, "cut {cut}: {res:?}");
            let parent = HashMap::from([(c1, p), (c2, p), (p, g)]);
            for (child, par) in parent {
                if !is_condemned_for_test(&clean, child).await {
                    assert!(
                        !is_condemned_for_test(&clean, par).await,
                        "cut {cut}: {child} rescued but its parent {par} still condemned"
                    );
                }
            }
        }
    }

    #[test]
    fn ancestors_first_orders_by_parent_edges() {
        let id = |n: u8| ChangesetId(Hash([n; 32]));
        let (g, p, c1, c2, x) = (id(1), id(2), id(3), id(4), id(5));
        let parents = HashMap::from([(p, vec![g]), (c1, vec![p]), (c2, vec![p, x]), (g, vec![])]);
        let out = ancestors_first(&[c1, c2, p, g], &parents);
        let pos = |i| out.iter().position(|o| *o == i).unwrap();
        assert_eq!(out.len(), 4);
        assert!(pos(g) < pos(p) && pos(p) < pos(c1) && pos(p) < pos(c2));
    }

    #[tokio::test(start_paused = true)]
    async fn a_delete_in_progress_is_waited_out_then_refused() {
        let r = repo();
        let a = commit(&r, &[], &[("f", Some("1"))], "a").await;
        condemn_for_test(&r, &[a], 100).await;
        doom_for_test(&r, a, Doom::Deleting).await;
        let err = r.rescue_changesets(&[a], RescueBy::Ref).await.unwrap_err();
        assert!(matches!(err, Error::Conflict(_)), "{err}");
        // Collect abandons once it sees the rescue: the writer goes on.
        doom_for_test(&r, a, Doom::Abandoned).await;
        r.rescue_changesets(&[a], RescueBy::Ref).await.unwrap();
        // Deleted: left to the caller's existence check.
        condemn_for_test(&r, &[a], 300).await;
        doom_for_test(&r, a, Doom::Deleted).await;
        r.rescue_changesets(&[a], RescueBy::Ref).await.unwrap();
    }
}
