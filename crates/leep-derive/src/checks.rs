//! Check attestations, known-good refs and suspect finding (ADR 0091).
//!
//! A check result is a derived-data row beside the changeset, keyed by
//! (changeset, check name) as reviews are keyed by reviewer, so a rerun
//! replaces the earlier result and two checks never race. `green/<ref>` is
//! moved only here, by a compare-and-swap that goes forward along `<ref>`'s
//! first-parent chain; nothing in this module is on the land path.

use std::collections::{BTreeSet, HashMap};

use bytes::Bytes;
use leep_core::api::{
    CulpritsResponse, Promotion, RedState, RefChecksResponse, Suspect, MAX_SUSPECTS,
    MAX_SUSPECT_PATHS,
};
use leep_core::changeset::EXTRA_AGENT_MODEL;
use leep_core::check::{green_ref, Check, Outcome, CHECK_KIND};
use leep_core::{ChangesetId, Error, Hash, RefLogEntry, RefUpdateContext, Result};

use crate::changeset_gc::RescueBy;
use crate::Repo;

/// Most ref log entries one request reads looking for the entry that
/// brought a changeset: each costs up to two ancestry checks.
pub const MAX_SEEK: usize = 1024;
/// Ref log entries read per page of those walks.
const LOG_PAGE: usize = 512;
/// Most first-parent changesets the red walk looks at below the head.
pub const MAX_RED_WALK: usize = 1024;
/// Most changesets one suspect's paths are gathered from (a stack land).
const MAX_SUSPECT_STACK: usize = 32;
/// Compare-and-swap attempts one promotion makes. Each loss means another
/// promotion moved green; only a newer one can keep it from being needed.
const MAX_PROMOTE_ATTEMPTS: usize = 16;

pub(crate) fn storage_key(id: ChangesetId, check: &str) -> Hash {
    let mut bytes = Vec::with_capacity(32 + check.len());
    bytes.extend_from_slice(id.0.as_bytes());
    bytes.extend_from_slice(check.as_bytes());
    Hash::of_object("check", &bytes)
}

impl Repo {
    /// Store `check` for `id`, replacing that check's earlier result.
    pub async fn put_check(&self, id: ChangesetId, check: &Check) -> Result<()> {
        check.validate()?;
        self.rescue_changesets(&[id], RescueBy::Attest).await?;
        self.get_changeset(id).await?;
        self.meta()
            .put_derived(
                CHECK_KIND,
                &storage_key(id, &check.check),
                Bytes::from(check.encode()),
            )
            .await
    }

    /// The newest result of check `name` on `id`. A row that does not decode
    /// or names another check counts as absent.
    pub async fn get_check(&self, id: ChangesetId, name: &str) -> Result<Option<Check>> {
        let row = self
            .meta()
            .get_derived(CHECK_KIND, &storage_key(id, name))
            .await?;
        Ok(row
            .and_then(|b| Check::decode(&b).ok())
            .filter(|c| c.check == name))
    }

    /// The stored results of each of `ids` for each of `names`, in batched
    /// lookups, each list in the order of `names`. Ids with none are left out.
    pub async fn checks_by(
        &self,
        ids: &[ChangesetId],
        names: &[String],
    ) -> Result<HashMap<ChangesetId, Vec<Check>>> {
        const BATCH: usize = 512;
        let pairs: Vec<(ChangesetId, &String)> = ids
            .iter()
            .flat_map(|id| names.iter().map(move |n| (*id, n)))
            .collect();
        let mut out: HashMap<ChangesetId, Vec<Check>> = HashMap::new();
        for chunk in pairs.chunks(BATCH) {
            let keys: Vec<Hash> = chunk.iter().map(|(id, n)| storage_key(*id, n)).collect();
            let rows = self.meta().get_derived_many(CHECK_KIND, &keys).await?;
            for ((id, name), row) in chunk.iter().zip(rows) {
                let Some(c) = row.and_then(|b| Check::decode(&b).ok()) else {
                    continue;
                };
                if &c.check == *name {
                    out.entry(*id).or_default().push(c);
                }
            }
        }
        Ok(out)
    }

    /// Whether `id` is on `head`'s first-parent chain (inclusive).
    async fn on_first_parent(&self, id: ChangesetId, head: ChangesetId) -> Result<bool> {
        if id == head {
            return Ok(true);
        }
        let target = self.generation(id).await?;
        if self.generation(head).await? <= target {
            return Ok(false);
        }
        self.on_first_parent_chain(id, target, head).await
    }

    /// Move `green/<target_ref>` forward to `id` if `id` is on the ref's
    /// first-parent chain, every one of `required` has a passing result on
    /// it, and green is at an ancestor of it (or does not exist yet). Never
    /// rewinds: a result for a changeset at or below green does nothing. A
    /// lost swap reads green again, so concurrent promotions end at the
    /// newest of them. Logged with `ctx` (operation `promote`).
    pub async fn promote_green(
        &self,
        target_ref: &str,
        id: ChangesetId,
        required: &[String],
        ctx: &RefUpdateContext,
    ) -> Result<Option<Promotion>> {
        if required.is_empty() {
            return Ok(None);
        }
        // Read once, outside the retry loop: policy refs only move forward,
        // so a changeset on this head's first-parent chain stays on it.
        let Some(head) = self.meta().get_ref(target_ref).await? else {
            return Ok(None);
        };
        if !self.on_first_parent(id, head).await? {
            return Ok(None);
        }
        let passed: BTreeSet<String> = self
            .checks_by(&[id], required)
            .await?
            .remove(&id)
            .unwrap_or_default()
            .into_iter()
            .filter(|c| c.outcome == Outcome::Pass)
            .map(|c| c.check)
            .collect();
        if !required.iter().all(|r| passed.contains(r)) {
            return Ok(None);
        }
        let green = green_ref(target_ref);
        for _ in 0..MAX_PROMOTE_ATTEMPTS {
            let from = self.meta().get_ref(&green).await?;
            if let Some(g) = from {
                if g == id || !self.is_ancestor(g, id).await? {
                    return Ok(None);
                }
            }
            self.rescue_changesets(&[id], RescueBy::Ref).await?;
            if self
                .meta()
                .cas_ref_logged(&green, from, Some(id), ctx)
                .await?
            {
                return Ok(Some(Promotion {
                    target_ref: target_ref.to_string(),
                    green_ref: green,
                    from,
                    to: id,
                }));
            }
        }
        Err(Error::Conflict(format!(
            "{green} kept moving while promoting it to {id}; the check is stored, post it again \
             to retry the promotion"
        )))
    }

    /// Move `green/<target_ref>` to the newest changeset on the ref's
    /// first-parent chain above green, within [`MAX_RED_WALK`] of the head,
    /// that has a passing result for every one of `required`. Run when the
    /// checks policy loads or changes: dropping a flaky required check then
    /// unblocks green at once instead of at the next pass.
    pub async fn catch_up_green(
        &self,
        target_ref: &str,
        required: &[String],
        ctx: &RefUpdateContext,
    ) -> Result<Option<Promotion>> {
        if required.is_empty() {
            return Ok(None);
        }
        let Some(head) = self.meta().get_ref(target_ref).await? else {
            return Ok(None);
        };
        let green = self.meta().get_ref(&green_ref(target_ref)).await?;
        let mut chain = Vec::new();
        let mut cur = Some(head);
        while let Some(id) = cur {
            if Some(id) == green || chain.len() >= MAX_RED_WALK {
                break;
            }
            chain.push(id);
            cur = self.parents(id).await?.first().copied();
        }
        let rows = self.checks_by(&chain, required).await?;
        let newest = chain.iter().find(|id| {
            rows.get(id).is_some_and(|r| {
                required.iter().all(|n| {
                    r.iter()
                        .any(|c| &c.check == n && c.outcome == Outcome::Pass)
                })
            })
        });
        match newest {
            Some(id) => self.promote_green(target_ref, *id, required, ctx).await,
            None => Ok(None),
        }
    }

    /// The refs among `names` whose `green/<ref>` holds `id`, in that order.
    pub async fn green_on(&self, id: ChangesetId, names: &[String]) -> Result<Vec<String>> {
        let mut out = Vec::new();
        for name in names {
            if let Some(g) = self.meta().get_ref(&green_ref(name)).await? {
                if self.is_ancestor(id, g).await? {
                    out.push(name.clone());
                }
            }
        }
        Ok(out)
    }

    /// Where `name` stands: its head, its known-good ref, how far behind that
    /// is and whether the head is red. Red is derived, never stored: the
    /// newest changeset above green on the head's first-parent chain that has
    /// a result for one of `names` failed one. `names` are the required
    /// checks, or every check the server knows when the ref requires none.
    pub async fn ref_checks(
        &self,
        name: &str,
        required: &[String],
        names: &[String],
    ) -> Result<RefChecksResponse> {
        let head = self
            .meta()
            .get_ref(name)
            .await?
            .ok_or_else(|| Error::NotFound(format!("ref {name:?}")))?;
        let green_name = green_ref(name);
        let green = self.meta().get_ref(&green_name).await?;
        let behind = match green {
            Some(g) if self.is_ancestor(g, head).await? => {
                Some(self.generation(head).await? - self.generation(g).await?)
            }
            _ => None,
        };
        let red = self.red(name, head, green, names).await?;
        Ok(RefChecksResponse {
            name: name.to_string(),
            head,
            green_ref: green_name,
            green,
            required: required.to_vec(),
            behind,
            red,
        })
    }

    async fn red(
        &self,
        name: &str,
        head: ChangesetId,
        green: Option<ChangesetId>,
        names: &[String],
    ) -> Result<Option<RedState>> {
        if names.is_empty() {
            return Ok(None);
        }
        // Newest first, stopping at green, a root or the bound.
        let mut chain = Vec::new();
        let mut cur = Some(head);
        while let Some(id) = cur {
            if Some(id) == green || chain.len() >= MAX_RED_WALK {
                break;
            }
            chain.push(id);
            cur = self.parents(id).await?.first().copied();
        }
        let rows = self.checks_by(&chain, names).await?;
        // Each check is judged by its own newest result above green; of the
        // checks whose newest result failed, the one that has failed longest
        // says since when the ref is red.
        let mut red: Option<(usize, String)> = None;
        for name in names {
            let result = |id: &ChangesetId| {
                rows.get(id)
                    .and_then(|r| r.iter().find(|c| &c.check == name))
                    .map(|c| c.outcome)
            };
            let Some(newest) = chain.iter().position(|id| result(id).is_some()) else {
                continue;
            };
            if result(&chain[newest]) != Some(Outcome::Fail) {
                continue;
            }
            // Down from the newest result to the last one that passed.
            let mut first = newest;
            for (i, id) in chain.iter().enumerate().skip(newest) {
                match result(id) {
                    Some(Outcome::Fail) => first = i,
                    Some(_) => break,
                    None => {}
                }
            }
            if red.as_ref().is_none_or(|(at, _)| first > *at) {
                red = Some((first, name.clone()));
            }
        }
        let Some((at, check)) = red else {
            return Ok(None);
        };
        let first_failed = chain[at];
        let since_seq = self
            .introducing_entry(name, first_failed, MAX_SEEK)
            .await?
            .0
            .map(|e| e.seq);
        Ok(Some(RedState {
            first_failed,
            check,
            since_seq,
        }))
    }

    /// The ref log entry of `name` that brought `id` onto it, newest first
    /// within `max` entries, the entries read above it, and where
    /// the walk can go on. An entry brings `id` when it moved to `id`, or
    /// moved to a descendant of `id` from a changeset that did not hold it.
    async fn introducing_entry(
        &self,
        name: &str,
        id: ChangesetId,
        max: usize,
    ) -> Result<(Option<RefLogEntry>, LogWalk)> {
        let mut walk = LogWalk::new(max);
        while let Some(e) = walk.next(self, name).await? {
            let Some(new) = e.new else {
                continue;
            };
            if new == id {
                return Ok((Some(e), walk));
            }
            let brought = match e.old {
                Some(old) => !self.is_ancestor(id, old).await? && self.is_ancestor(id, new).await?,
                None => self.is_ancestor(id, new).await?,
            };
            if brought {
                return Ok((Some(e), walk));
            }
        }
        Ok((None, walk))
    }

    /// The suspects for a failure at `bad` (default: the head): the ref
    /// moves of `name` that brought anything above `good` (default: the
    /// known-good `green/<name>`) up to and including `bad`, newest first,
    /// one page of at most `limit` at a time (`before`: the `next` of the
    /// previous page). A suspect is a land in the range, not a verdict: a
    /// land whose test fails only together with a later one shows the later
    /// one too. Finding `bad`'s entry reads at most [`MAX_SEEK`] entries;
    /// `truncated` says it gave up.
    pub async fn culprits(
        &self,
        name: &str,
        bad: Option<ChangesetId>,
        good: Option<ChangesetId>,
        before: Option<u64>,
        limit: usize,
    ) -> Result<CulpritsResponse> {
        self.culprits_within(name, bad, good, before, limit, MAX_SEEK)
            .await
    }

    /// [`Repo::culprits`] seeking through at most `seek` ref log entries.
    pub(crate) async fn culprits_within(
        &self,
        name: &str,
        bad: Option<ChangesetId>,
        good: Option<ChangesetId>,
        before: Option<u64>,
        limit: usize,
        seek: usize,
    ) -> Result<CulpritsResponse> {
        let limit = limit.clamp(1, MAX_SUSPECTS);
        let head = self
            .meta()
            .get_ref(name)
            .await?
            .ok_or_else(|| Error::NotFound(format!("ref {name:?}")))?;
        let bad = bad.unwrap_or(head);
        if !self.is_ancestor(bad, head).await? {
            return Err(Error::Invalid(format!(
                "{bad} is not in the history of {name:?}"
            )));
        }
        let good = match good {
            // A sheriff looking below green names the last good run itself.
            Some(g) => {
                if g == bad || !self.is_ancestor(g, bad).await? {
                    return Err(Error::Invalid(format!(
                        "good {g} is not below bad {bad}: name an ancestor of it"
                    )));
                }
                Some(g)
            }
            None => {
                let g = self.meta().get_ref(&green_ref(name)).await?;
                if let Some(g) = g {
                    if self.is_ancestor(bad, g).await? {
                        return Err(Error::Invalid(format!(
                            "{bad} is at or below {}, which passed every required check; \
                             name a changeset above it, or pass good to look below it",
                            green_ref(name)
                        )));
                    }
                }
                g
            }
        };
        let answer = |suspects, next, truncated| CulpritsResponse {
            name: name.to_string(),
            good,
            bad,
            suspects,
            truncated,
            next,
        };
        // Where the page starts: the entry that brought `bad`, or the
        // cursor of the previous page.
        let (mut pending, mut walk) = match before {
            None => {
                let (start, mut walk) = self.introducing_entry(name, bad, seek).await?;
                let Some(start) = start else {
                    return Ok(answer(Vec::new(), None, true));
                };
                walk.max = walk.read + limit;
                (Some(start), walk)
            }
            Some(b) => {
                let mut walk = LogWalk::new(limit + 1);
                walk.before = Some(b);
                (None, walk)
            }
        };
        let mut entries: Vec<RefLogEntry> = Vec::new();
        let mut next = None;
        loop {
            let e = match pending.take() {
                Some(e) => e,
                None => match walk.next(self, name).await? {
                    Some(e) => e,
                    // Where logging began, or the page's one extra entry.
                    None => break,
                },
            };
            // A delete, or a move to good or below it: nothing above.
            let Some(new) = e.new else { break };
            if let Some(g) = good {
                if self.is_ancestor(new, g).await? {
                    break;
                }
            }
            if entries.len() == limit {
                next = entries.last().map(|l| l.seq);
                break;
            }
            // This entry began at or below good: it is the last suspect.
            let reached = match (e.old, good) {
                (None, _) => true,
                (Some(old), Some(g)) => self.is_ancestor(old, g).await?,
                (Some(_), None) => false,
            };
            entries.push(e);
            if reached {
                break;
            }
        }
        // Paths and creators for this page only.
        let mut suspects = Vec::with_capacity(entries.len());
        for e in entries {
            suspects.push(self.suspect(e).await?);
        }
        Ok(answer(suspects, next, false))
    }

    async fn suspect(&self, e: RefLogEntry) -> Result<Suspect> {
        let new = e.new.expect("suspects are moves to a changeset");
        let cs = self.get_changeset(new).await?;
        // Who made it: the changeset the land asked for, else the original
        // a server-made rebased copy came from.
        // The nearest one in the rebase chain with a recorded creator.
        let about = e.land_changeset.unwrap_or(new);
        let created_by = self
            .created_by_through_rebases(about)
            .await?
            .map(|c| c.principal);
        // Every changeset the move added on the first-parent chain: a land
        // of a stack moves the ref once.
        let floor = match e.old {
            Some(old) => Some(self.generation(old).await?),
            None => None,
        };
        let mut paths = BTreeSet::new();
        let mut cur = Some(new);
        let mut steps = 0;
        let mut more_paths = false;
        while let Some(id) = cur {
            if Some(id) == e.old {
                break;
            }
            if steps >= MAX_SUSPECT_STACK {
                more_paths = true;
                break;
            }
            // At or below the old head's generation the chain left the move.
            if steps > 0 {
                if let Some(f) = floor {
                    if self.generation(id).await? <= f {
                        break;
                    }
                }
            }
            let c = if id == new {
                cs.clone()
            } else {
                self.get_changeset(id).await?
            };
            for p in c.changes.keys() {
                if paths.len() >= MAX_SUSPECT_PATHS && !paths.contains(p) {
                    more_paths = true;
                } else {
                    paths.insert(p.clone());
                }
            }
            cur = c.parents.first().copied();
            steps += 1;
        }
        Ok(Suspect {
            changeset: new,
            seq: e.seq,
            operation: e.operation,
            actor: e.actor,
            time: e.time,
            land_id: e.land_id,
            land_changeset: e.land_changeset,
            created_by,
            author: cs.author.clone(),
            title: cs.message.lines().next().unwrap_or_default().to_string(),
            agent_model: cs.extra.get(EXTRA_AGENT_MODEL).cloned(),
            paths: paths.into_iter().collect(),
            more_paths,
        })
    }
}

/// A newest-first walk over one ref's log, in pages, bounded by `max`
/// entries.
struct LogWalk {
    page: std::collections::VecDeque<RefLogEntry>,
    before: Option<u64>,
    done: bool,
    read: usize,
    max: usize,
}

impl LogWalk {
    fn new(max: usize) -> Self {
        LogWalk {
            page: Default::default(),
            before: None,
            done: false,
            read: 0,
            max,
        }
    }

    async fn next(&mut self, repo: &Repo, name: &str) -> Result<Option<RefLogEntry>> {
        if self.read >= self.max {
            return Ok(None);
        }
        if self.page.is_empty() && !self.done {
            let want = LOG_PAGE.min(self.max - self.read);
            let page = repo.meta().ref_log(Some(name), self.before, want).await?;
            self.done = page.len() < want;
            self.before = page.last().map(|e| e.seq);
            self.page.extend(page);
        }
        let e = self.page.pop_front();
        if e.is_some() {
            self.read += 1;
        }
        Ok(e)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;
    use leep_core::check::{PROMOTE_OPERATION, PROMOTION_ONLY_MARKER};

    use super::*;
    use crate::tests::{commit, repo};
    use crate::LandPolicy;

    /// `main` protected, `green/main` moved only by promotion.
    struct Green;

    #[async_trait]
    impl LandPolicy for Green {
        fn applies_to(&self, r: &str) -> bool {
            r == "main" || r == "green/main"
        }
        fn promotion_only(&self, r: &str) -> bool {
            r == "green/main"
        }
        async fn check_land(
            &self,
            _: &Repo,
            _: &str,
            _: Option<ChangesetId>,
            _: &[ChangesetId],
        ) -> Result<()> {
            Ok(())
        }
    }

    fn ctx() -> RefUpdateContext {
        RefUpdateContext::new(PROMOTE_OPERATION).actor(Some("ci"))
    }

    fn req(names: &[&str]) -> Vec<String> {
        names.iter().map(|s| s.to_string()).collect()
    }

    async fn post(r: &Repo, id: ChangesetId, name: &str, outcome: Outcome) {
        r.put_check(
            id,
            &Check {
                check: name.into(),
                outcome,
                by: "ci".into(),
                url: String::new(),
                summary: String::new(),
                timestamp: 1,
            },
        )
        .await
        .unwrap();
    }

    /// `main` with `n` lands on a base; the base and each land's head.
    async fn landed(r: &Repo, n: usize) -> Vec<ChangesetId> {
        let base = commit(r, &[], &[("f", Some("0"))], "base").await;
        r.meta()
            .cas_ref_logged("main", None, Some(base), &RefUpdateContext::new("cas"))
            .await
            .unwrap();
        let mut heads = vec![base];
        for i in 0..n {
            let p = format!("d{i}/f");
            let c = commit(r, &[base], &[(&p, Some("x"))], &format!("land {i}")).await;
            r.land(c, "main").await.unwrap();
            heads.push(r.meta().get_ref("main").await.unwrap().unwrap());
        }
        heads
    }

    async fn green(r: &Repo) -> Option<ChangesetId> {
        r.meta().get_ref("green/main").await.unwrap()
    }

    #[tokio::test]
    async fn promotion_needs_every_required_pass_and_only_goes_forward() {
        let r = repo();
        let h = landed(&r, 5).await;
        let required = req(&["clippy", "test"]);
        post(&r, h[3], "test", Outcome::Pass).await;
        assert!(r
            .promote_green("main", h[3], &required, &ctx())
            .await
            .unwrap()
            .is_none());
        post(&r, h[3], "clippy", Outcome::Fail).await;
        assert!(r
            .promote_green("main", h[3], &required, &ctx())
            .await
            .unwrap()
            .is_none());
        // A rerun replaces the earlier result.
        post(&r, h[3], "clippy", Outcome::Pass).await;
        let p = r
            .promote_green("main", h[3], &required, &ctx())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            (p.from, p.to, p.green_ref.as_str()),
            (None, h[3], "green/main")
        );
        assert_eq!(green(&r).await, Some(h[3]));
        // Out of order: an older pass arriving later never rewinds.
        for n in ["clippy", "test"] {
            post(&r, h[1], n, Outcome::Pass).await;
        }
        assert!(r
            .promote_green("main", h[1], &required, &ctx())
            .await
            .unwrap()
            .is_none());
        assert!(r
            .promote_green("main", h[3], &required, &ctx())
            .await
            .unwrap()
            .is_none());
        assert_eq!(green(&r).await, Some(h[3]));
        for n in ["clippy", "test"] {
            post(&r, h[5], n, Outcome::Pass).await;
        }
        let p = r
            .promote_green("main", h[5], &required, &ctx())
            .await
            .unwrap()
            .unwrap();
        assert_eq!((p.from, p.to), (Some(h[3]), h[5]));
        // The log says who and how.
        let log = r
            .meta()
            .ref_log(Some("green/main"), None, 10)
            .await
            .unwrap();
        assert_eq!(log.len(), 2);
        assert!(log.iter().all(|e| e.operation == PROMOTE_OPERATION));
        assert!(log.iter().all(|e| e.actor.as_deref() == Some("ci")));
        // Every promotion moved forward.
        for e in &log {
            if let (Some(o), Some(n)) = (e.old, e.new) {
                assert!(r.is_ancestor(o, n).await.unwrap());
            }
        }
    }

    #[tokio::test]
    async fn changesets_off_the_first_parent_chain_never_promote() {
        let r = repo();
        let h = landed(&r, 2).await;
        let required = req(&["test"]);
        // Unlanded: a child of the head nobody landed.
        let wip = commit(&r, &[h[2]], &[("w", Some("1"))], "wip").await;
        // A merge's second parent: on main's history, not its first-parent
        // chain.
        let side = commit(&r, &[h[0]], &[("s", Some("1"))], "side").await;
        let merge = commit(&r, &[h[2], side], &[("s", Some("1"))], "merge").await;
        r.land(merge, "main").await.unwrap();
        assert_eq!(r.meta().get_ref("main").await.unwrap(), Some(merge));
        assert!(r.is_ancestor(side, merge).await.unwrap());
        for id in [wip, side] {
            post(&r, id, "test", Outcome::Pass).await;
            assert!(r
                .promote_green("main", id, &required, &ctx())
                .await
                .unwrap()
                .is_none());
        }
        assert_eq!(green(&r).await, None);
        // A ref that does not exist promotes nothing either.
        assert!(r
            .promote_green("nope", h[1], &required, &ctx())
            .await
            .unwrap()
            .is_none());
        post(&r, merge, "test", Outcome::Pass).await;
        assert!(r
            .promote_green("main", merge, &required, &ctx())
            .await
            .unwrap()
            .is_some());
    }

    #[tokio::test]
    async fn concurrent_promotions_end_at_the_newest() {
        let r = Arc::new(repo());
        let h = landed(&r, 24).await;
        let required = req(&["test"]);
        for id in &h {
            post(&r, *id, "test", Outcome::Pass).await;
        }
        let mut tasks = Vec::new();
        // Newest last, so most tasks race a promotion that moved past them.
        for id in h.iter().copied() {
            let r = r.clone();
            let required = required.clone();
            tasks.push(tokio::spawn(async move {
                r.promote_green("main", id, &required, &ctx()).await
            }));
        }
        for t in tasks {
            t.await.unwrap().unwrap();
        }
        assert_eq!(green(&r).await, h.last().copied());
        let log = r
            .meta()
            .ref_log(Some("green/main"), None, 100)
            .await
            .unwrap();
        for e in &log {
            if let (Some(o), Some(n)) = (e.old, e.new) {
                assert!(o != n && r.is_ancestor(o, n).await.unwrap(), "rewound");
            }
        }
    }

    #[tokio::test]
    async fn green_refuses_land_cas_undo_and_delete() {
        let r = repo().with_land_policy(Arc::new(Green));
        let h = landed(&r, 3).await;
        post(&r, h[2], "test", Outcome::Pass).await;
        r.promote_green("main", h[2], &req(&["test"]), &ctx())
            .await
            .unwrap()
            .unwrap();
        let refused = |e: Error| {
            assert!(
                matches!(&e, Error::Policy(m) if m.contains(PROMOTION_ONLY_MARKER)),
                "{e}"
            );
        };
        refused(r.land(h[3], "green/main").await.unwrap_err());
        let c = RefUpdateContext::new("cas");
        // Forward, backward (what `leep undo` sends) and delete.
        for new in [Some(h[3]), Some(h[1]), None] {
            refused(
                r.cas_ref_checked("green/main", Some(h[2]), new, &c)
                    .await
                    .unwrap_err(),
            );
        }
        assert_eq!(green(&r).await, Some(h[2]));
        // The whole namespace is reserved, policy or not: nobody plants a
        // known-good ref.
        refused(r.land(h[3], "green/other").await.unwrap_err());
        refused(
            r.cas_ref_checked("green/other", None, Some(h[1]), &c)
                .await
                .unwrap_err(),
        );
        // One left over from a policy that no longer names it may be
        // deleted, which is the recovery path.
        r.meta()
            .cas_ref("green/old", None, Some(h[1]))
            .await
            .unwrap();
        refused(
            r.cas_ref_checked("green/old", Some(h[1]), Some(h[2]), &c)
                .await
                .unwrap_err(),
        );
        assert!(r
            .cas_ref_checked("green/old", Some(h[1]), None, &c)
            .await
            .unwrap());
        // `main` itself is protected: a rewind past green is refused.
        let e = r
            .cas_ref_checked("main", Some(h[3]), Some(h[1]), &c)
            .await
            .unwrap_err();
        assert!(
            matches!(&e, Error::Policy(m) if m.contains(leep_core::refname::NOT_FORWARD_MARKER)),
            "{e}"
        );
    }

    #[tokio::test]
    async fn culprits_are_the_log_entries_between_good_and_bad() {
        let r = repo();
        let h = landed(&r, 10).await;
        post(&r, h[3], "test", Outcome::Pass).await;
        r.promote_green("main", h[3], &req(&["test"]), &ctx())
            .await
            .unwrap()
            .unwrap();
        let log = r.meta().ref_log(Some("main"), None, 100).await.unwrap();
        // Newest first: entries 0..=9 are lands 10..=1; lands 4..=10 are
        // above green.
        let want: Vec<ChangesetId> = log[..7].iter().map(|e| e.new.unwrap()).collect();
        let c = r.culprits("main", None, None, None, 100).await.unwrap();
        assert_eq!((c.good, c.bad, c.truncated), (Some(h[3]), h[10], false));
        let got: Vec<ChangesetId> = c.suspects.iter().map(|s| s.changeset).collect();
        assert_eq!(got, want);
        assert_eq!(c.suspects[0].seq, log[0].seq);
        assert_eq!(c.suspects[0].operation, "land");
        assert_eq!(c.suspects[0].title, "land 9");
        assert_eq!(
            c.suspects[0].paths,
            [leep_core::RepoPath::new("d9/f").unwrap()]
        );
        // A bad below the head: the suspects stop at the land that brought it.
        let c = r
            .culprits("main", Some(h[6]), None, None, 100)
            .await
            .unwrap();
        let got: Vec<ChangesetId> = c.suspects.iter().map(|s| s.changeset).collect();
        assert_eq!(got, [h[6], h[5], h[4]]);
        // `good` looks below green.
        let c = r
            .culprits("main", Some(h[5]), Some(h[1]), None, 100)
            .await
            .unwrap();
        let got: Vec<ChangesetId> = c.suspects.iter().map(|s| s.changeset).collect();
        assert_eq!((c.good, got), (Some(h[1]), vec![h[5], h[4], h[3], h[2]]));
        // Refusals: bad at or below green; good not below bad; bad off the ref.
        assert!(matches!(
            r.culprits("main", Some(h[2]), None, None, 100).await,
            Err(Error::Invalid(_))
        ));
        assert!(matches!(
            r.culprits("main", Some(h[2]), Some(h[5]), None, 100).await,
            Err(Error::Invalid(_))
        ));
        let wip = commit(&r, &[h[10]], &[("w", Some("1"))], "wip").await;
        assert!(matches!(
            r.culprits("main", Some(wip), None, None, 100).await,
            Err(Error::Invalid(_))
        ));
        // Pages: three at a time, continued by `next`, give the same list.
        let mut paged = Vec::new();
        let mut before = None;
        loop {
            let c = r.culprits("main", None, None, before, 3).await.unwrap();
            assert!(c.suspects.len() <= 3 && !c.truncated);
            paged.extend(c.suspects.iter().map(|s| s.changeset));
            match c.next {
                Some(n) => before = Some(n),
                None => break,
            }
        }
        assert_eq!(paged, want);
        // Bounded: a seek that cannot reach bad's land gives up and says so.
        let c = r
            .culprits_within("main", Some(h[5]), None, None, 100, 3)
            .await
            .unwrap();
        assert!(c.suspects.is_empty() && c.truncated);
        // Without green, the walk goes to the ref's first entry.
        let r2 = repo();
        let h2 = landed(&r2, 3).await;
        let c = r2.culprits("main", None, None, None, 100).await.unwrap();
        let got: Vec<ChangesetId> = c.suspects.iter().map(|s| s.changeset).collect();
        assert_eq!(
            (got, c.truncated),
            (vec![h2[3], h2[2], h2[1], h2[0]], false)
        );
    }

    #[tokio::test]
    async fn catch_up_promotes_to_the_newest_fully_passing_changeset() {
        let r = repo();
        let h = landed(&r, 5).await;
        let both = req(&["flaky", "test"]);
        for id in &h[1..] {
            post(&r, *id, "test", Outcome::Pass).await;
        }
        post(&r, h[1], "flaky", Outcome::Pass).await;
        post(&r, h[4], "flaky", Outcome::Fail).await;
        // Under the old policy only h[1] passed everything.
        let p = r
            .catch_up_green("main", &both, &ctx())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(p.to, h[1]);
        assert!(r
            .catch_up_green("main", &both, &ctx())
            .await
            .unwrap()
            .is_none());
        // The flaky check is dropped: green moves to the newest pass at once.
        let p = r
            .catch_up_green("main", &req(&["test"]), &ctx())
            .await
            .unwrap()
            .unwrap();
        assert_eq!((p.from, p.to), (Some(h[1]), h[5]));
        // Re-requiring it never rewinds.
        assert!(r
            .catch_up_green("main", &both, &ctx())
            .await
            .unwrap()
            .is_none());
        assert_eq!(green(&r).await, Some(h[5]));
        assert!(r
            .catch_up_green("nope", &both, &ctx())
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn red_is_the_newest_attested_failure_above_green() {
        let r = repo();
        let h = landed(&r, 6).await;
        let names = req(&["test"]);
        post(&r, h[2], "test", Outcome::Pass).await;
        r.promote_green("main", h[2], &names, &ctx())
            .await
            .unwrap()
            .unwrap();
        let s = r.ref_checks("main", &names, &names).await.unwrap();
        assert_eq!((s.green, s.behind, s.red), (Some(h[2]), Some(4), None));
        post(&r, h[4], "test", Outcome::Fail).await;
        post(&r, h[6], "test", Outcome::Fail).await;
        let s = r.ref_checks("main", &names, &names).await.unwrap();
        let red = s.red.unwrap();
        assert_eq!((red.first_failed, red.check.as_str()), (h[4], "test"));
        let log = r.meta().ref_log(Some("main"), None, 100).await.unwrap();
        assert_eq!(
            red.since_seq,
            log.iter().find(|e| e.new == Some(h[4])).map(|e| e.seq)
        );
        // A newer pass clears red.
        post(&r, h[6], "test", Outcome::Pass).await;
        let s = r.ref_checks("main", &names, &names).await.unwrap();
        assert_eq!(s.red, None);
        // Each check by its own newest result: lint failing on h[5] keeps
        // the ref red even though h[6], newer, has only a test pass.
        let both = req(&["lint", "test"]);
        post(&r, h[5], "lint", Outcome::Fail).await;
        let red = r
            .ref_checks("main", &both, &both)
            .await
            .unwrap()
            .red
            .unwrap();
        assert_eq!((red.first_failed, red.check.as_str()), (h[5], "lint"));
    }
}
