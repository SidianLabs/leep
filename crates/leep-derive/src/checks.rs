//! Check attestations, known-good refs and culprit finding (ADR 0091).
//!
//! A check result is a derived-data row beside the changeset, keyed by
//! (changeset, check name) as reviews are keyed by reviewer, so a rerun
//! replaces the earlier result and two checks never race. `green/<ref>` is
//! moved only here, by a compare-and-swap that goes forward along `<ref>`'s
//! first-parent chain; nothing in this module is on the land path.

use std::collections::{BTreeSet, HashMap};

use bytes::Bytes;
use leep_core::api::{
    CulpritsResponse, Promotion, RedState, RefChecksResponse, Suspect, MAX_SUSPECT_PATHS,
};
use leep_core::changeset::EXTRA_AGENT_MODEL;
use leep_core::check::{green_ref, Check, Outcome, CHECK_KIND};
use leep_core::review_request::{CreatedBy, CREATED_BY_KIND};
use leep_core::{ChangesetId, Error, Hash, RefLogEntry, RefUpdateContext, Result};

use crate::changeset_gc::RescueBy;
use crate::Repo;

/// Most ref log entries one culprit or red walk reads.
pub const MAX_LOG_WALK: usize = 4096;
/// Ref log entries read per page of those walks.
const LOG_PAGE: usize = 512;
/// Most first-parent changesets the red walk looks at below the head.
pub const MAX_RED_WALK: usize = 1024;
/// Most changesets one suspect's paths are gathered from (a stack land).
const MAX_SUSPECT_STACK: usize = 256;
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
        let Some((newest, failing)) = chain.iter().find_map(|id| {
            let rows = rows.get(id)?;
            let fail = rows.iter().find(|c| c.outcome == Outcome::Fail);
            Some((*id, fail.map(|c| c.check.clone())))
        }) else {
            return Ok(None);
        };
        let Some(check) = failing else {
            return Ok(None);
        };
        // The failing run of that check: down from the newest attested
        // changeset to the last one that passed it.
        let mut first_failed = newest;
        for id in chain.iter().skip_while(|id| **id != newest) {
            match rows
                .get(id)
                .and_then(|r| r.iter().find(|c| c.check == check))
            {
                Some(c) if c.outcome == Outcome::Fail => first_failed = *id,
                Some(_) => break,
                None => {}
            }
        }
        let since_seq = self
            .introducing_entry(name, first_failed, MAX_LOG_WALK)
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
    /// within [`MAX_LOG_WALK`] entries, the entries read above it, and where
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

    /// The ref moves of `name` above its known-good ref, from the one that
    /// brought `bad` (default: the head) down to the first above green,
    /// newest first: what a sheriff looks through when `bad` fails a check.
    /// Reads at most [`MAX_LOG_WALK`] entries; `truncated` says it stopped
    /// before reaching green (or the ref's first entry, without green).
    pub async fn culprits(&self, name: &str, bad: Option<ChangesetId>) -> Result<CulpritsResponse> {
        self.culprits_within(name, bad, MAX_LOG_WALK).await
    }

    /// [`Repo::culprits`] reading at most `max` ref log entries.
    pub(crate) async fn culprits_within(
        &self,
        name: &str,
        bad: Option<ChangesetId>,
        max: usize,
    ) -> Result<CulpritsResponse> {
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
        let good = self.meta().get_ref(&green_ref(name)).await?;
        if let Some(g) = good {
            if self.is_ancestor(bad, g).await? {
                return Err(Error::Invalid(format!(
                    "{bad} is at or below {}, which passed every required check; name a \
                     changeset above it",
                    green_ref(name)
                )));
            }
        }
        let (start, mut walk) = self.introducing_entry(name, bad, max).await?;
        let Some(start) = start else {
            return Ok(CulpritsResponse {
                name: name.to_string(),
                good,
                bad,
                suspects: Vec::new(),
                truncated: true,
            });
        };
        let mut entries = vec![start];
        let truncated = loop {
            let last = entries.last().expect("the walk starts with one entry");
            // This entry began at or below green: it is the last suspect.
            let reached = match (last.old, good) {
                (None, _) => true,
                (Some(old), Some(g)) => self.is_ancestor(old, g).await?,
                (Some(_), None) => false,
            };
            if reached {
                break false;
            }
            let Some(e) = walk.next(self, name).await? else {
                // Out of entries: at the bound, or where logging began.
                break walk.read >= walk.max;
            };
            match (e.new, good) {
                // A delete, or a move to green or below it: nothing above.
                (None, _) => break false,
                (Some(new), Some(g)) if self.is_ancestor(new, g).await? => break false,
                _ => entries.push(e),
            }
        };
        let mut suspects = Vec::with_capacity(entries.len());
        for e in entries {
            suspects.push(self.suspect(e).await?);
        }
        Ok(CulpritsResponse {
            name: name.to_string(),
            good,
            bad,
            suspects,
            truncated,
        })
    }

    async fn suspect(&self, e: RefLogEntry) -> Result<Suspect> {
        let new = e.new.expect("suspects are moves to a changeset");
        let cs = self.get_changeset(new).await?;
        let about = e.land_changeset.unwrap_or(new);
        let created_by = match self.meta().get_derived(CREATED_BY_KIND, &about.0).await? {
            Some(b) => CreatedBy::decode(&b).ok().map(|c| c.principal),
            None => None,
        };
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
            if Some(id) == e.old || steps >= MAX_SUSPECT_STACK {
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
