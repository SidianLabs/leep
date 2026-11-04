//! Review requests and who created a changeset (ADR 0086, 0087).
//!
//! Requests are rows of their own store table: the server opens them for
//! the authenticated principal, lists them by reviewer or requester, and
//! closes them when their changeset, or its reviewed change, lands
//! (ADR 0087). Closing is bookkeeping a land
//! must never wait on or fail for, so the land path calls
//! [`Repo::close_landed_review_requests`] after the ref moved and ignores
//! its errors, and listing closes what a move the land path did not see
//! (a raw ref update, an import) put on the ref.

use std::collections::{HashMap, HashSet};

use bytes::Bytes;
use leep_core::api::{ReviewRequestEntry, REQUEST_ENTRY_PATHS};
use leep_core::changeset::EXTRA_REBASED_FROM;
use leep_core::review_request::{
    check_request, CreatedBy, NewReviewRequest, ReviewRequest, ReviewRequestFilter,
    ReviewRequestState, CREATED_BY_KIND, MAX_OPEN_REQUESTS, TOO_MANY_REQUESTS_MARKER,
};
use leep_core::store::unix_now;
use leep_core::{refname, Changeset, ChangesetId, DerivedRow, Error, RepoPath, Result};

use crate::review::ReviewRules;
use crate::Repo;

/// What a request entry shows about its changeset: creator, title, the
/// first paths and how many it changes.
type About = (Option<CreatedBy>, Option<String>, Vec<RepoPath>, usize);

/// Open requests of one ref a land looks at; more stay open until listed.
const LAND_CLOSE_LIMIT: usize = 500;

/// First-parent steps a land walks from its new head over the changesets
/// it added (its copies, a client's resolving commit).
const COPY_WALK_LIMIT: usize = 256;

/// Newest ref log entries a land reads to find the head it moved from.
const LAND_LOG_LOOKBACK: usize = 64;

/// First-parent steps a listing walks from a ref's head looking for
/// changesets that name a requested one in `leep.rebased_from`.
const LIST_WALK_LIMIT: usize = 64;

/// Carry candidates one listing's walk of a ref collects before it stops.
const LIST_CANDIDATE_LIMIT: usize = 256;

/// Refs one listing walks; requests on others wait for a listing of fewer
/// refs (or filtered by `ref`) or their land's close.
const LIST_WALK_REFS: usize = 8;

/// A changeset near a ref's head that names others it was made from.
struct Hinted {
    id: ChangesetId,
    /// The server's `rebased-origin-v1` row: `id` is a copy of it.
    origin: Option<ChangesetId>,
    /// What [`Repo::carry_candidates`] finds from `id`'s hints.
    candidates: Vec<ChangesetId>,
}

fn unsupported() -> Error {
    Error::Invalid(
        "this server's metadata store does not keep review requests (run `leep-server migrate`)"
            .into(),
    )
}

impl Repo {
    /// [`Repo::create_changeset`], recording `principal` as the creator in a
    /// `created-by-v1` row written only by the store write that inserts the
    /// changeset, atomically with it: posting the bytes of a changeset that
    /// is already stored (an old commit, a server-made one, one stored
    /// without authentication) never makes the poster its creator, and of
    /// concurrent posts exactly the first records one. `None` (no
    /// authentication) records nothing.
    pub async fn create_changeset_by(
        &self,
        cs: Changeset,
        principal: Option<&str>,
    ) -> Result<ChangesetId> {
        let Some(principal) = principal else {
            return self.create_changeset(cs).await;
        };
        let id = cs.id();
        let row = CreatedBy {
            principal: principal.to_string(),
            timestamp: unix_now(),
        };
        let if_new = vec![DerivedRow::new(
            CREATED_BY_KIND,
            id.0,
            Bytes::from(row.encode()),
        )];
        self.store_changeset(cs, true, Vec::new(), if_new).await
    }

    /// Who created `id`, if a server recorded it. A row that does not
    /// decode counts as absent.
    pub async fn created_by(&self, id: ChangesetId) -> Result<Option<CreatedBy>> {
        let row = self.meta().get_derived(CREATED_BY_KIND, &id.0).await?;
        Ok(row.and_then(|b| CreatedBy::decode(&b).ok()))
    }

    /// Open a review request, or answer the open one of the same requester,
    /// changeset, ref and reviewers (true). The changeset must exist and
    /// not be on the ref already. `rules` are the review policy's, for
    /// closing landed requests before the cap is checked.
    pub async fn request_review(
        &self,
        mut req: NewReviewRequest,
        rules: Option<&ReviewRules>,
    ) -> Result<(ReviewRequest, bool)> {
        if !self.meta().supports_review_requests() {
            return Err(unsupported());
        }
        req.reviewers.sort();
        req.reviewers.dedup();
        check_request(&req)?;
        refname::validate(&req.target_ref)?;
        self.get_changeset(req.changeset).await?;
        // A missing ref is fine only where a land policy will judge the land
        // that creates it: the first change of a protected ref needs review
        // too. Anywhere else nothing could ever close the request.
        match self.meta().get_ref(&req.target_ref).await? {
            None if !self
                .land_policy()
                .is_some_and(|p| p.applies_to(&req.target_ref)) =>
            {
                return Err(Error::NotFound(format!(
                    "ref {} does not exist; a review request names the ref its change would land on",
                    req.target_ref
                )));
            }
            Some(head) if self.on_first_parent_of(req.changeset, head).await? => {
                return Err(Error::Invalid(format!(
                    "changeset {} is already on {}",
                    req.changeset, req.target_ref
                )));
            }
            _ => {}
        }
        let same = self
            .meta()
            .list_review_requests(&ReviewRequestFilter {
                requester: Some(req.requester.clone()),
                changeset: Some(req.changeset),
                target_ref: Some(req.target_ref.clone()),
                states: vec![ReviewRequestState::Open],
                // The cap bounds them, so this sees every open one.
                limit: MAX_OPEN_REQUESTS,
                ..Default::default()
            })
            .await?;
        if let Some(r) = same.into_iter().find(|r| r.reviewers == req.reviewers) {
            return Ok((r, true));
        }
        self.check_open_requests(&req.requester, rules).await?;
        Ok((self.meta().put_review_request(&req).await?, false))
    }

    /// Refuse a requester at [`MAX_OPEN_REQUESTS`] open requests. At the
    /// cap, a listing first closes the ones that landed by a move the land
    /// path did not report, so only requests really waiting count.
    async fn check_open_requests(
        &self,
        requester: &str,
        rules: Option<&ReviewRules>,
    ) -> Result<()> {
        let filter = ReviewRequestFilter {
            requester: Some(requester.to_string()),
            states: vec![ReviewRequestState::Open],
            limit: MAX_OPEN_REQUESTS,
            ..Default::default()
        };
        if self.meta().list_review_requests(&filter).await?.len() < MAX_OPEN_REQUESTS
            || self.review_requests(&filter, rules).await?.0.len() < MAX_OPEN_REQUESTS
        {
            return Ok(());
        }
        Err(Error::Invalid(format!(
            "{requester} has {MAX_OPEN_REQUESTS} {TOO_MANY_REQUESTS_MARKER}; withdraw some \
             (`leep review cancel`) or wait for them to land"
        )))
    }

    /// The requests `filter` matches, and the `before` of the next page
    /// when this one was full. Open ones whose change is now on their ref
    /// are closed as landed first (a move the land path did not report,
    /// such as a raw ref update, or a close it missed), and left out when
    /// only open ones were asked for: a changeset on the ref's first-parent
    /// chain, or one that a changeset among the newest
    /// [`LIST_WALK_LIMIT`] there copies (the server's row) or names and
    /// lands the reviewed change of (ADR 0087). `rules` are the review
    /// policy's; without them no auto rule holds, which closes less.
    pub async fn review_requests(
        &self,
        filter: &ReviewRequestFilter,
        rules: Option<&ReviewRules>,
    ) -> Result<(Vec<ReviewRequest>, Option<u64>)> {
        if !self.meta().supports_review_requests() {
            return Err(unsupported());
        }
        let default_rules = ReviewRules::default();
        let rules = rules.unwrap_or(&default_rules);
        let mut reqs = self.meta().list_review_requests(filter).await?;
        let next = reqs
            .last()
            .map(|r| r.id)
            .filter(|_| filter.limit > 0 && reqs.len() >= filter.limit);
        let mut heads: HashMap<String, Option<ChangesetId>> = HashMap::new();
        let mut walks: HashMap<String, Vec<Hinted>> = HashMap::new();
        let now = unix_now();
        for r in reqs.iter_mut() {
            if r.state != ReviewRequestState::Open {
                continue;
            }
            let head = match heads.get(&r.target_ref) {
                Some(h) => *h,
                None => {
                    let h = self.meta().get_ref(&r.target_ref).await?;
                    heads.insert(r.target_ref.clone(), h);
                    h
                }
            };
            let Some(head) = head else { continue };
            // A changeset that cannot be judged (collected, unreadable)
            // stays open: listing is no place to fail.
            let mut landed_as = None;
            if self
                .on_first_parent_of(r.changeset, head)
                .await
                .unwrap_or(false)
            {
                landed_as = Some(r.changeset);
            } else {
                if !walks.contains_key(&r.target_ref) {
                    if walks.len() >= LIST_WALK_REFS {
                        continue;
                    }
                    let walk = self.hinted_below(head).await;
                    walks.insert(r.target_ref.clone(), walk);
                }
                // Oldest first: the changeset that first put it there.
                for h in walks[&r.target_ref].iter().rev() {
                    if h.origin == Some(r.changeset)
                        || (h.candidates.contains(&r.changeset)
                            && self
                                .lands_reviewed_change(
                                    r.changeset,
                                    h.id,
                                    rules,
                                    self.carry_merges(&r.target_ref),
                                )
                                .await
                                .unwrap_or(false))
                    {
                        landed_as = Some(h.id);
                        break;
                    }
                }
            }
            if landed_as.is_none() {
                continue;
            }
            if self
                .meta()
                .close_review_request(r.id, ReviewRequestState::Landed, landed_as, now)
                .await?
            {
                metrics::counter!("leep_review_requests_closed_total", "state" => "landed")
                    .increment(1);
                r.state = ReviewRequestState::Landed;
                r.closed = Some(now);
                r.landed_as = landed_as;
            } else if let Some(fresh) = self.meta().get_review_request(r.id).await? {
                *r = fresh;
            }
        }
        if !filter.states.is_empty() {
            reqs.retain(|r| filter.states.contains(&r.state));
        }
        Ok((reqs, next))
    }

    /// The changesets among the newest [`LIST_WALK_LIMIT`] on `head`'s
    /// first-parent chain that name others in `leep.rebased_from`, newest
    /// first, until [`LIST_CANDIDATE_LIMIT`] candidates are collected. A
    /// read that fails ends the walk with what it found: listing is no
    /// place to fail.
    async fn hinted_below(&self, head: ChangesetId) -> Vec<Hinted> {
        let mut out = Vec::new();
        let mut total = 0;
        let mut cur = Some(head);
        for _ in 0..LIST_WALK_LIMIT {
            let Some(id) = cur else { break };
            let Ok(cs) = self.get_changeset(id).await else {
                break;
            };
            cur = cs.parents.first().copied();
            if !cs.extra.contains_key(EXTRA_REBASED_FROM) {
                continue;
            }
            let (Ok(candidates), Ok(origin)) = (
                self.carry_candidates(id).await,
                self.rebased_origin(id).await,
            ) else {
                break;
            };
            total += candidates.len();
            out.push(Hinted {
                id,
                origin,
                candidates,
            });
            if total >= LIST_CANDIDATE_LIMIT {
                break;
            }
        }
        out
    }

    pub async fn review_request(&self, id: u64) -> Result<ReviewRequest> {
        if !self.meta().supports_review_requests() {
            return Err(unsupported());
        }
        self.meta()
            .get_review_request(id)
            .await?
            .ok_or_else(|| Error::NotFound(format!("review request {id}")))
    }

    /// Withdraw request `id` if it is open; answer it as it is now either
    /// way. Who may is the caller's decision.
    pub async fn withdraw_review_request(&self, id: u64) -> Result<ReviewRequest> {
        let r = self.review_request(id).await?;
        if r.state == ReviewRequestState::Open
            && self
                .meta()
                .close_review_request(id, ReviewRequestState::Withdrawn, None, unix_now())
                .await?
        {
            metrics::counter!("leep_review_requests_closed_total", "state" => "withdrawn")
                .increment(1);
        }
        self.review_request(id).await
    }

    /// Close the open requests of `target_ref` that a land of `submitted`,
    /// which put `landed` at the head, satisfied (ADR 0086, 0087): the
    /// submitted changeset, the changesets of its stack (as themselves or as
    /// the copies the land replayed them as), anything else now on the
    /// head's first-parent chain, and changesets that a changeset Z the land
    /// added names in `leep.rebased_from` when either
    ///
    /// - Z lands their reviewed change, judged by content alone as a carried
    ///   verdict is ([`Repo::lands_reviewed_change`]), so a forged hint
    ///   closes nothing whose reviewed change did not land; or
    /// - `actor`, who ran the land, made the request, and the ref log entry
    ///   of the move to `landed` names `actor`: a requester may withdraw
    ///   their own request anyway, and this covers a stack entry whose
    ///   reviewed file a lower entry also changed.
    ///
    /// The changesets the land added are found from the newest ref log
    /// entry of the move to `landed` (the first-parent chain down to the
    /// head it moved from). Without one (a land that moved nothing: already
    /// on the ref, a replayed land id; or a log out of reach), `landed` and
    /// the server copies directly below it are checked by content and copy
    /// row only, since they may be other agents' lands. Returns how many it
    /// closed. Callers ignore its errors: the
    /// land has happened whatever this does.
    pub async fn close_landed_review_requests(
        &self,
        target_ref: &str,
        submitted: ChangesetId,
        landed: ChangesetId,
        actor: Option<&str>,
        rules: Option<&ReviewRules>,
    ) -> Result<u64> {
        if !self.meta().supports_review_requests() {
            return Ok(0);
        }
        let default_rules = ReviewRules::default();
        let rules = rules.unwrap_or(&default_rules);
        let open = self
            .meta()
            .list_review_requests(&ReviewRequestFilter {
                target_ref: Some(target_ref.to_string()),
                states: vec![ReviewRequestState::Open],
                limit: LAND_CLOSE_LIMIT,
                ..Default::default()
            })
            .await?;
        if open.is_empty() {
            return Ok(0);
        }
        let mut copies = HashMap::new();
        let mut named = Vec::new();
        let (added, logged_by) = self.land_added(target_ref, landed).await?;
        // The requester rule needs the move to be this actor's own: an
        // older entry by someone else (a re-land of their changeset) or no
        // entry at all grants nothing beyond content.
        let requester_rule = actor.is_some() && logged_by.as_deref() == actor;
        let merges = self.carry_merges(target_ref);
        for z in added {
            if let Some(origin) = self.rebased_origin(z).await? {
                copies.entry(origin).or_insert(z);
            }
            let candidates = self.carry_candidates(z).await?;
            if !candidates.is_empty() {
                named.push((z, candidates));
            }
        }
        let now = unix_now();
        let mut closed = 0;
        for r in open {
            let mut landed_as = if r.changeset == submitted || r.changeset == landed {
                Some(landed)
            } else if let Some(copy) = copies.get(&r.changeset) {
                Some(*copy)
            } else if self
                .on_first_parent_of(r.changeset, landed)
                .await
                .unwrap_or(false)
            {
                Some(r.changeset)
            } else {
                None
            };
            // Oldest first: the changeset that first put it there.
            for (z, candidates) in named.iter().rev() {
                if landed_as.is_some() {
                    break;
                }
                if !candidates.contains(&r.changeset) {
                    continue;
                }
                if (requester_rule && actor == Some(r.requester.as_str()))
                    || self
                        .lands_reviewed_change(r.changeset, *z, rules, merges)
                        .await
                        .unwrap_or(false)
                {
                    landed_as = Some(*z);
                }
            }
            if landed_as.is_none() {
                continue;
            }
            if self
                .meta()
                .close_review_request(r.id, ReviewRequestState::Landed, landed_as, now)
                .await?
            {
                closed += 1;
            }
        }
        if closed > 0 {
            metrics::counter!("leep_review_requests_closed_total", "state" => "landed")
                .increment(closed);
        }
        Ok(closed)
    }

    /// The changesets a land that moved `target_ref` to `landed` added,
    /// newest first, and the actor of the ref log entry they come from:
    /// `landed`'s first-parent chain down to the head the newest entry
    /// moving the ref to `landed` says it moved from, at most
    /// [`COPY_WALK_LIMIT`]. Without such an entry among the newest
    /// [`LAND_LOG_LOOKBACK`] (the land moved nothing, or the log is out of
    /// reach), `landed` and the server copies directly below it, with no
    /// actor.
    async fn land_added(
        &self,
        target_ref: &str,
        landed: ChangesetId,
    ) -> Result<(Vec<ChangesetId>, Option<String>)> {
        let log = self
            .meta()
            .ref_log(Some(target_ref), None, LAND_LOG_LOOKBACK)
            .await
            .unwrap_or_default();
        let entry = log.iter().find(|e| e.new == Some(landed));
        let from = entry.map(|e| e.old);
        let logged_by = entry.and_then(|e| e.actor.clone());
        let mut out = Vec::new();
        let mut seen = HashSet::new();
        let mut cur = Some(landed);
        while let Some(id) = cur.filter(|_| out.len() < COPY_WALK_LIMIT) {
            if !seen.insert(id) {
                break;
            }
            match from {
                Some(old) if old == Some(id) => break,
                None if id != landed && self.rebased_origin(id).await?.is_none() => break,
                _ => {}
            }
            out.push(id);
            cur = match self.get_changeset(id).await {
                Ok(cs) => cs.parents.first().copied(),
                Err(Error::NotFound(_)) => None,
                Err(e) => return Err(e),
            };
        }
        Ok((out, logged_by))
    }

    /// The refs among `names` whose first-parent chain holds `id`, in the
    /// order given: where a verdict on `id` no longer decides a land.
    pub async fn refs_holding(&self, id: ChangesetId, names: &[String]) -> Result<Vec<String>> {
        let mut out = Vec::new();
        for name in names {
            let Some(head) = self.meta().get_ref(name).await? else {
                continue;
            };
            if self.on_first_parent_of(id, head).await.unwrap_or(false) {
                out.push(name.clone());
            }
        }
        Ok(out)
    }

    /// Whether `id` is on `head`'s first-parent chain: logarithmic in the
    /// distance, unlike a full ancestry search.
    async fn on_first_parent_of(&self, id: ChangesetId, head: ChangesetId) -> Result<bool> {
        if id == head {
            return Ok(true);
        }
        let target = self.generation(id).await?;
        if self.generation(head).await? <= target {
            return Ok(false);
        }
        self.on_first_parent_chain(id, target, head).await
    }

    /// `reqs` with what a reviewer wants to see about each changeset: who
    /// created it, its title and the paths it changes, read in two batches.
    pub async fn review_request_entries(
        &self,
        reqs: Vec<ReviewRequest>,
    ) -> Result<Vec<ReviewRequestEntry>> {
        let mut ids: Vec<ChangesetId> = reqs.iter().map(|r| r.changeset).collect();
        ids.sort();
        ids.dedup();
        let changesets = self.meta().get_changesets(&ids).await?;
        let keys: Vec<_> = ids.iter().map(|id| id.0).collect();
        let rows = self.meta().get_derived_many(CREATED_BY_KIND, &keys).await?;
        let mut about: HashMap<ChangesetId, About> = HashMap::new();
        for ((id, cs), row) in ids.iter().zip(changesets).zip(rows) {
            let title = cs
                .as_ref()
                .map(|c| c.message.lines().next().unwrap_or("").to_string());
            let (paths, changed) = cs.map_or((Vec::new(), 0), |c| {
                let paths = c
                    .changes
                    .keys()
                    .take(REQUEST_ENTRY_PATHS)
                    .cloned()
                    .collect();
                (paths, c.changes.len())
            });
            let by = row.and_then(|b| CreatedBy::decode(&b).ok());
            about.insert(*id, (by, title, paths, changed));
        }
        Ok(reqs
            .into_iter()
            .map(|request| {
                let (created_by, title, paths, changed) =
                    about.get(&request.changeset).cloned().unwrap_or_default();
                ReviewRequestEntry {
                    request,
                    created_by,
                    title,
                    approved_by: Vec::new(),
                    rejected_by: Vec::new(),
                    paths,
                    changed,
                }
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use leep_core::{FileChange, FileKind, Hash, RepoPath};

    use super::*;
    use crate::review::AutoRule;
    use crate::tests::{commit as files_commit, repo};

    async fn commit(r: &Repo, parent: Option<ChangesetId>, n: u8) -> ChangesetId {
        let body = vec![n; 3];
        let content = r.put_blob(Bytes::from(body.clone())).await.unwrap();
        let mut changes = BTreeMap::new();
        changes.insert(
            RepoPath::new(format!("f{n}")).unwrap(),
            FileChange::Modify {
                content,
                kind: FileKind::Regular,
                size: body.len() as u64,
            },
        );
        r.create_changeset(Changeset {
            parents: parent.into_iter().collect(),
            author: "t".into(),
            timestamp: i64::from(n),
            message: format!("change {n}\n\nbody"),
            changes,
            extra: BTreeMap::new(),
        })
        .await
        .unwrap()
    }

    fn ask(c: ChangesetId, by: &str, to: &[&str]) -> NewReviewRequest {
        NewReviewRequest {
            changeset: c,
            target_ref: "main".into(),
            requester: by.into(),
            reviewers: to.iter().map(|s| s.to_string()).collect(),
            message: String::new(),
            created: 1,
        }
    }

    fn all_auto() -> ReviewRules {
        ReviewRules::new(
            [
                AutoRule::CommentOnly,
                AutoRule::WhitespaceOnly,
                AutoRule::DocsOnly,
            ],
            &["**/*.md".into()],
        )
        .unwrap()
    }

    fn docs() -> ReviewRules {
        ReviewRules::new([AutoRule::DocsOnly], &["**/*.md".into()]).unwrap()
    }

    /// A changeset with `files`, naming `from` in `leep.rebased_from`
    /// (newest first, as the CLI writes a resolved stack).
    async fn naming(
        r: &Repo,
        parent: ChangesetId,
        files: &[(&str, Option<&str>)],
        from: &[ChangesetId],
    ) -> ChangesetId {
        let id = files_commit(r, &[parent], files, "resolve").await;
        let mut cs = r.get_changeset(id).await.unwrap();
        let named: Vec<String> = from.iter().map(|c| c.to_string()).collect();
        cs.extra.insert(EXTRA_REBASED_FROM.into(), named.join(","));
        r.create_changeset(cs).await.unwrap()
    }

    async fn landed_by(r: &Repo, id: ChangesetId, actor: &str) -> ChangesetId {
        match r.land_as(id, "main", Some(actor)).await.unwrap() {
            leep_core::api::LandResponse::Landed { landed, .. } => landed,
            other => panic!("{other:?}"),
        }
    }

    async fn state(r: &Repo, q: &ReviewRequest) -> (ReviewRequestState, Option<ChangesetId>) {
        let q = r.review_request(q.id).await.unwrap();
        (q.state, q.landed_as)
    }

    /// Main with `a.rs` and a changelog; ada's X changes both and is
    /// requested; bo's change to the changelog lands first, so X conflicts.
    async fn conflicted(r: &Repo, x_files: &[(&str, Option<&str>)]) -> (ChangesetId, ChangesetId) {
        let base = files_commit(
            r,
            &[],
            &[("a.rs", Some("1\n")), ("CHANGELOG.md", Some("c\n"))],
            "base",
        )
        .await;
        r.meta().cas_ref("main", None, Some(base)).await.unwrap();
        let x = files_commit(r, &[base], x_files, "x").await;
        let m = files_commit(r, &[base], &[("CHANGELOG.md", Some("c\nm\n"))], "m").await;
        landed_by(r, m, "bo").await;
        (x, m)
    }

    /// The client resolves X's changelog conflict in a commit of its own
    /// that names X: the reviewed code is byte for byte X's, so the land
    /// closes the request by content, whoever ran it. Without the policy's
    /// docs rule the changelog counts as reviewed, and this resolution
    /// (neither the server's merge nor a union of the two lines) closes
    /// nothing.
    #[tokio::test]
    async fn a_client_resolution_naming_the_request_closes_it() {
        let r = repo();
        let (x, m) = conflicted(
            &r,
            &[("a.rs", Some("2\n")), ("CHANGELOG.md", Some("c\nx\n"))],
        )
        .await;
        let (q, _) = r
            .request_review(ask(x, "ada", &["rhea"]), None)
            .await
            .unwrap();
        let z = naming(
            &r,
            m,
            &[("a.rs", Some("2\n")), ("CHANGELOG.md", Some("c\nm, x\n"))],
            &[x],
        )
        .await;
        assert_eq!(landed_by(&r, z, "cy").await, z);
        assert_eq!(
            r.close_landed_review_requests("main", z, z, Some("cy"), None)
                .await
                .unwrap(),
            0
        );
        assert_eq!(state(&r, &q).await, (ReviewRequestState::Open, None));
        assert_eq!(
            r.close_landed_review_requests("main", z, z, Some("cy"), Some(&docs()))
                .await
                .unwrap(),
            1
        );
        assert_eq!(state(&r, &q).await, (ReviewRequestState::Landed, Some(z)));
    }

    /// The client's resolution makes X's reviewed change to `a.rs` again on
    /// a base where `a.rs` moved: it is the server's own rebase of X's change
    /// there, and the changelog keeps both appended lines. That lands the
    /// reviewed change (ADR 0089), so the request closes, even with no auto
    /// rule. One extra line closes nothing, and neither does any merge on a
    /// ref whose lands never merge contents.
    #[tokio::test]
    async fn a_request_closes_when_its_change_lands_rebased_by_the_client() {
        for strict in [false, true] {
            let mut r = repo();
            if strict {
                r = r.with_land_conflicts(crate::LandConflicts::strict());
            }
            const A: &str = "top\n\nmid\n\nend\n";
            let base = files_commit(
                &r,
                &[],
                &[("a.rs", Some(A)), ("CHANGELOG.md", Some("c\n"))],
                "base",
            )
            .await;
            r.meta().cas_ref("main", None, Some(base)).await.unwrap();
            let x = files_commit(
                &r,
                &[base],
                &[
                    ("a.rs", Some("top\n\nmid\n\nend\nx\n")),
                    ("CHANGELOG.md", Some("c\nx\n")),
                ],
                "x",
            )
            .await;
            let m = files_commit(
                &r,
                &[base],
                &[
                    ("a.rs", Some("TOP\n\nmid\n\nend\n")),
                    ("CHANGELOG.md", Some("c\nm\n")),
                ],
                "m",
            )
            .await;
            landed_by(&r, m, "bo").await;
            let (q, _) = r
                .request_review(ask(x, "ada", &["rhea"]), None)
                .await
                .unwrap();
            let rules = ReviewRules::default();
            let evil = naming(
                &r,
                m,
                &[
                    ("a.rs", Some("TOP\n\nmid\n\nend\nx\nevil\n")),
                    ("CHANGELOG.md", Some("c\nm\nx\n")),
                ],
                &[x],
            )
            .await;
            assert!(!r
                .lands_reviewed_change(x, evil, &rules, true)
                .await
                .unwrap());
            let good = naming(
                &r,
                m,
                &[
                    ("a.rs", Some("TOP\n\nmid\n\nend\nx\n")),
                    ("CHANGELOG.md", Some("c\nm\nx\n")),
                ],
                &[x],
            )
            .await;
            assert!(r
                .lands_reviewed_change(x, good, &rules, true)
                .await
                .unwrap());
            assert!(!r
                .lands_reviewed_change(x, good, &rules, false)
                .await
                .unwrap());
            assert_eq!(landed_by(&r, good, "cy").await, good);
            assert_eq!(
                r.close_landed_review_requests("main", good, good, Some("cy"), None)
                    .await
                    .unwrap(),
                u64::from(!strict),
                "strict: {strict}"
            );
            let want = if strict {
                (ReviewRequestState::Open, None)
            } else {
                (ReviewRequestState::Landed, Some(good))
            };
            assert_eq!(state(&r, &q).await, want);
        }
    }

    /// One changed byte in a reviewed file: the resolution is not the
    /// reviewed change, and only its requester's own land closes it.
    #[tokio::test]
    async fn a_resolution_that_changed_a_reviewed_byte_stays_open() {
        let r = repo();
        let (x, m) = conflicted(
            &r,
            &[("a.rs", Some("2\n")), ("CHANGELOG.md", Some("c\nx\n"))],
        )
        .await;
        let (q, _) = r
            .request_review(ask(x, "ada", &["rhea"]), None)
            .await
            .unwrap();
        let z = naming(
            &r,
            m,
            &[("a.rs", Some("2!\n")), ("CHANGELOG.md", Some("c\nm\nx\n"))],
            &[x],
        )
        .await;
        landed_by(&r, z, "cy").await;
        assert_eq!(
            r.close_landed_review_requests("main", z, z, Some("cy"), Some(&all_auto()))
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            r.review_requests(&open_for("rhea"), Some(&all_auto()))
                .await
                .unwrap()
                .0
                .len(),
            1
        );
        assert_eq!(state(&r, &q).await, (ReviewRequestState::Open, None));
    }

    /// X changed only the changelog, which the docs rule approves: it
    /// closes once the changelog lands, in whatever docs form, under a
    /// changeset naming it. One that leaves the file alone does not.
    #[tokio::test]
    async fn an_auto_only_request_closes_when_its_docs_land_differently() {
        let r = repo();
        let (x, m) = conflicted(&r, &[("CHANGELOG.md", Some("c\nx\n"))]).await;
        let (q, _) = r
            .request_review(ask(x, "ada", &["rhea"]), None)
            .await
            .unwrap();
        let elsewhere = naming(&r, m, &[("README.md", Some("r\n"))], &[x]).await;
        landed_by(&r, elsewhere, "cy").await;
        assert_eq!(
            r.close_landed_review_requests("main", elsewhere, elsewhere, Some("cy"), Some(&docs()))
                .await
                .unwrap(),
            0
        );
        let z = naming(&r, elsewhere, &[("CHANGELOG.md", Some("c\nm, x\n"))], &[x]).await;
        landed_by(&r, z, "cy").await;
        // Without the docs rule the changelog is reviewed content that
        // differs.
        assert_eq!(
            r.close_landed_review_requests("main", z, z, Some("cy"), None)
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            r.close_landed_review_requests("main", z, z, Some("cy"), Some(&docs()))
                .await
                .unwrap(),
            1
        );
        assert_eq!(state(&r, &q).await, (ReviewRequestState::Landed, Some(z)));
    }

    /// A stack S1 then S2, both changing `a.rs`, resolved into one commit
    /// naming both: S2's reviewed old content is S1's, so the content rule
    /// refuses both.
    async fn resolved_stack(r: &Repo) -> (ReviewRequest, ReviewRequest, ChangesetId) {
        let (s1, m) = conflicted(
            r,
            &[("a.rs", Some("2\n")), ("CHANGELOG.md", Some("c\ns1\n"))],
        )
        .await;
        let s2 = files_commit(r, &[s1], &[("a.rs", Some("3\n"))], "s2").await;
        let (q1, _) = r
            .request_review(ask(s1, "ada", &["rhea"]), None)
            .await
            .unwrap();
        let (q2, _) = r
            .request_review(ask(s2, "ada", &["rhea"]), None)
            .await
            .unwrap();
        let z = naming(
            r,
            m,
            &[("a.rs", Some("3\n")), ("CHANGELOG.md", Some("c\nm\ns1\n"))],
            &[s2, s1],
        )
        .await;
        (q1, q2, z)
    }

    #[tokio::test]
    async fn the_requesters_own_land_closes_a_stack_entry_the_content_rule_refuses() {
        let r = repo();
        let (q1, q2, z) = resolved_stack(&r).await;
        assert!(!r
            .lands_reviewed_change(q2.changeset, z, &docs(), true)
            .await
            .unwrap());
        landed_by(&r, z, "ada").await;
        assert_eq!(
            r.close_landed_review_requests("main", z, z, Some("ada"), Some(&docs()))
                .await
                .unwrap(),
            2
        );
        for q in [&q1, &q2] {
            assert_eq!(state(&r, q).await, (ReviewRequestState::Landed, Some(z)));
        }
    }

    #[tokio::test]
    async fn another_agents_land_naming_it_does_not() {
        let r = repo();
        let (q1, q2, z) = resolved_stack(&r).await;
        landed_by(&r, z, "bo").await;
        assert_eq!(
            r.close_landed_review_requests("main", z, z, Some("bo"), Some(&all_auto()))
                .await
                .unwrap(),
            0
        );
        // A listing has no actor: it closes by content only.
        assert_eq!(
            r.review_requests(&open_for("rhea"), Some(&all_auto()))
                .await
                .unwrap()
                .0
                .len(),
            2
        );
        for q in [&q1, &q2] {
            assert_eq!(state(&r, q).await, (ReviewRequestState::Open, None));
        }
    }

    /// A resolution put on the ref by a raw ref update, which no land path
    /// reports: a listing closes the request by the same content rule, with
    /// the policy's rules.
    #[tokio::test]
    async fn a_listing_closes_a_resolution_moved_by_a_raw_ref_update() {
        let r = repo();
        let (x, m) = conflicted(
            &r,
            &[("a.rs", Some("2\n")), ("CHANGELOG.md", Some("c\nx\n"))],
        )
        .await;
        let (q, _) = r
            .request_review(ask(x, "ada", &["rhea"]), None)
            .await
            .unwrap();
        let z = naming(
            &r,
            m,
            &[("a.rs", Some("2\n")), ("CHANGELOG.md", Some("c\nm, x\n"))],
            &[x],
        )
        .await;
        // Something unrelated lands on top, so z is below the head.
        let top = files_commit(&r, &[z], &[("b.rs", Some("b\n"))], "top").await;
        r.meta().cas_ref("main", Some(m), Some(z)).await.unwrap();
        r.meta().cas_ref("main", Some(z), Some(top)).await.unwrap();
        assert_eq!(
            r.review_requests(&open_for("rhea"), None)
                .await
                .unwrap()
                .0
                .len(),
            1
        );
        assert!(r
            .review_requests(&open_for("rhea"), Some(&docs()))
            .await
            .unwrap()
            .0
            .is_empty());
        assert_eq!(state(&r, &q).await, (ReviewRequestState::Landed, Some(z)));
    }

    fn open_for(reviewer: &str) -> ReviewRequestFilter {
        ReviewRequestFilter {
            reviewer: Some(reviewer.into()),
            states: vec![ReviewRequestState::Open],
            limit: 50,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn created_by_is_written_once_with_the_changeset() {
        let r = repo();
        let base = commit(&r, None, 1).await;
        assert_eq!(r.created_by(base).await.unwrap(), None);
        let cs = r.get_changeset(base).await.unwrap();
        let next = Changeset {
            parents: vec![base],
            message: "by ada".into(),
            ..cs.clone()
        };
        let id = r
            .create_changeset_by(next.clone(), Some("ada"))
            .await
            .unwrap();
        let by = r.created_by(id).await.unwrap().unwrap();
        assert_eq!(by.principal, "ada");
        assert!(by.timestamp > 1_700_000_000);
        // A second create, by anyone, keeps the first creator.
        r.create_changeset_by(next, Some("bo")).await.unwrap();
        assert_eq!(r.created_by(id).await.unwrap().unwrap(), by);
        assert_eq!(r.info(id).await.unwrap().created_by, Some(by));
        // Without authentication nothing is recorded.
        let anon = Changeset {
            parents: vec![base],
            message: "anonymous".into(),
            ..cs
        };
        let id = r.create_changeset_by(anon, None).await.unwrap();
        assert_eq!(r.info(id).await.unwrap().created_by, None);
    }

    #[tokio::test]
    async fn requests_are_listed_deduplicated_and_withdrawn() {
        let r = repo();
        let base = commit(&r, None, 1).await;
        r.meta().cas_ref("main", None, Some(base)).await.unwrap();
        let c = commit(&r, Some(base), 2).await;
        let (a, existing) = r
            .request_review(ask(c, "ada", &["rhea", "bo", "rhea"]), None)
            .await
            .unwrap();
        assert!(!existing);
        assert_eq!(a.reviewers, ["bo", "rhea"]);
        let (again, existing) = r
            .request_review(ask(c, "ada", &["bo", "rhea"]), None)
            .await
            .unwrap();
        assert!(existing);
        assert_eq!(again, a);
        let (other, existing) = r
            .request_review(ask(c, "ada", &["cy"]), None)
            .await
            .unwrap();
        assert!(!existing && other.id != a.id);
        assert_eq!(
            r.review_requests(&open_for("rhea"), None).await.unwrap().0,
            std::slice::from_ref(&a)
        );
        let entries = r.review_request_entries(vec![a.clone()]).await.unwrap();
        assert_eq!(entries[0].title.as_deref(), Some("change 2"));
        assert_eq!(entries[0].created_by, None);
        // Already on the ref, unknown, a bad ref: refused.
        assert!(r.request_review(ask(base, "ada", &[]), None).await.is_err());
        let ghost = ChangesetId(Hash::of_blob(b"ghost"));
        assert!(matches!(
            r.request_review(ask(ghost, "ada", &[]), None).await,
            Err(Error::NotFound(_))
        ));
        let mut bad = ask(c, "ada", &[]);
        bad.target_ref = "a..b".into();
        assert!(r.request_review(bad, None).await.is_err());
        // A ref that does not exist: nothing could ever close it.
        let mut nowhere = ask(c, "ada", &[]);
        nowhere.target_ref = "nowhere".into();
        assert!(matches!(
            r.request_review(nowhere, None).await,
            Err(Error::NotFound(_))
        ));

        let w = r.withdraw_review_request(a.id).await.unwrap();
        assert_eq!(w.state, ReviewRequestState::Withdrawn);
        assert!(r
            .review_requests(&open_for("rhea"), None)
            .await
            .unwrap()
            .0
            .is_empty());
        // Withdrawing again answers it as it is.
        assert_eq!(r.withdraw_review_request(a.id).await.unwrap(), w);
        assert!(matches!(
            r.withdraw_review_request(9999).await,
            Err(Error::NotFound(_))
        ));
    }

    /// One requester's open requests are capped; withdrawing one, or one
    /// landing, makes room.
    #[tokio::test]
    async fn open_requests_per_requester_are_capped() {
        let r = repo();
        let base = commit(&r, None, 1).await;
        r.meta().cas_ref("main", None, Some(base)).await.unwrap();
        let c = commit(&r, Some(base), 2).await;
        let mut first = None;
        for i in 0..MAX_OPEN_REQUESTS {
            let name = format!("r{i}");
            let (q, existing) = r
                .request_review(ask(c, "ada", &[&name]), None)
                .await
                .unwrap();
            assert!(!existing);
            first.get_or_insert(q.id);
        }
        let err = r
            .request_review(ask(c, "ada", &["one-more"]), None)
            .await
            .unwrap_err();
        assert!(err.to_string().contains(TOO_MANY_REQUESTS_MARKER), "{err}");
        // An existing one is still answered, and others may still ask.
        assert!(
            r.request_review(ask(c, "ada", &["r0"]), None)
                .await
                .unwrap()
                .1
        );
        r.request_review(ask(c, "bo", &["one-more"]), None)
            .await
            .unwrap();
        r.withdraw_review_request(first.unwrap()).await.unwrap();
        r.request_review(ask(c, "ada", &["one-more"]), None)
            .await
            .unwrap();
        // Requests whose change reached the ref by a raw move stop counting.
        let err = r
            .request_review(ask(c, "ada", &["two-more"]), None)
            .await
            .unwrap_err();
        assert!(err.to_string().contains(TOO_MANY_REQUESTS_MARKER), "{err}");
        r.meta().cas_ref("main", Some(base), Some(c)).await.unwrap();
        let d = commit(&r, Some(c), 3).await;
        r.request_review(ask(d, "ada", &["two-more"]), None)
            .await
            .unwrap();
    }

    /// `leep.rebased_from` is client data: a changeset that names another
    /// agent's unlanded changeset in it, landed and then built on by a
    /// replayed land, must not close that agent's request.
    #[tokio::test]
    async fn a_forged_rebased_from_closes_nothing() {
        let r = repo();
        let base = commit(&r, None, 1).await;
        r.meta().cas_ref("main", None, Some(base)).await.unwrap();
        let x = commit(&r, Some(base), 2).await;
        let (qa, _) = r
            .request_review(ask(x, "ada", &["rhea"]), None)
            .await
            .unwrap();
        // Bo's changeset claims to be a copy of ada's and lands.
        let mut forged = r
            .get_changeset(commit(&r, Some(base), 3).await)
            .await
            .unwrap();
        forged.message = "not a copy".into();
        forged.extra.insert(
            leep_core::changeset::EXTRA_REBASED_FROM.into(),
            x.to_string(),
        );
        let y = r.create_changeset(forged).await.unwrap();
        r.land_as(y, "main", Some("bo")).await.unwrap();
        // Bo's own land of it closes nothing, whatever the auto rules.
        assert_eq!(
            r.close_landed_review_requests("main", y, y, Some("bo"), Some(&all_auto()))
                .await
                .unwrap(),
            0
        );
        // Cy's land is replayed on top of it.
        let w = commit(&r, Some(base), 4).await;
        let landed = match r.land_as(w, "main", Some("cy")).await.unwrap() {
            leep_core::api::LandResponse::Landed {
                landed, rebased, ..
            } => {
                assert!(rebased);
                landed
            }
            other => panic!("{other:?}"),
        };
        assert_eq!(r.get_changeset(landed).await.unwrap().parents, [y]);
        assert_eq!(
            r.close_landed_review_requests("main", w, landed, None, None)
                .await
                .unwrap(),
            0
        );
        // Ada's own land on top: the requester rule counts only what her
        // land added, not bo's forged changeset below it.
        let v = commit(&r, Some(landed), 5).await;
        r.land_as(v, "main", Some("ada")).await.unwrap();
        assert_eq!(
            r.close_landed_review_requests("main", v, v, Some("ada"), None)
                .await
                .unwrap(),
            0
        );
        // Nor does a listing, which walks past the forged changeset.
        assert_eq!(
            r.review_requests(&open_for("rhea"), Some(&all_auto()))
                .await
                .unwrap()
                .0
                .len(),
            1
        );
        let qa = r.review_request(qa.id).await.unwrap();
        assert_eq!((qa.state, qa.landed_as), (ReviewRequestState::Open, None));
    }

    /// A land that moves nothing (a re-land of what is already on the ref,
    /// a replayed land id) writes no ref log entry. Its requester must not
    /// close their request through another agent's forged-hint land: not by
    /// that agent's older entry, nor by the no-entry fallback.
    #[tokio::test]
    async fn a_re_land_by_the_requester_closes_nothing_by_hint() {
        let r = repo();
        let base = commit(&r, None, 1).await;
        r.meta().cas_ref("main", None, Some(base)).await.unwrap();
        let x = commit(&r, Some(base), 2).await;
        let (qa, _) = r
            .request_review(ask(x, "ada", &["rhea"]), None)
            .await
            .unwrap();
        let m = commit(&r, Some(base), 3).await;
        r.land_as(m, "main", Some("bo")).await.unwrap();
        // Bo's forged hint, replayed into a server copy that names it.
        let mut forged = r
            .get_changeset(commit(&r, Some(base), 4).await)
            .await
            .unwrap();
        forged
            .extra
            .insert(EXTRA_REBASED_FROM.into(), x.to_string());
        let y = r.create_changeset(forged).await.unwrap();
        let id = leep_core::store::LandId::from_bytes(&[7; 16]).unwrap();
        let landed_of = |resp| match resp {
            leep_core::api::LandResponse::Landed { landed, .. } => landed,
            other => panic!("{other:?}"),
        };
        let copy = landed_of(
            r.land_keyed(y, "main", Some("bo"), true, Some(id.clone()))
                .await
                .unwrap(),
        );
        assert_ne!(copy, y);
        assert!(r.carry_candidates(copy).await.unwrap().contains(&x));
        let rules = all_auto();
        let closes = |submitted, landed| {
            let r = r.clone();
            let rules = rules.clone();
            async move {
                r.close_landed_review_requests("main", submitted, landed, Some("ada"), Some(&rules))
                    .await
                    .unwrap()
            }
        };
        // Ada re-lands the copy (already the head) and the original (already
        // landed as the copy), and replays bo's land id: bo's entry is the
        // newest naming the copy, so nothing is hers.
        assert_eq!(
            landed_of(r.land_as(copy, "main", Some("ada")).await.unwrap()),
            copy
        );
        assert_eq!(closes(copy, copy).await, 0);
        assert_eq!(
            landed_of(r.land_as(y, "main", Some("ada")).await.unwrap()),
            copy
        );
        assert_eq!(closes(y, copy).await, 0);
        let replay = r
            .land_keyed(y, "main", Some("ada"), true, Some(id))
            .await
            .unwrap();
        assert_eq!(landed_of(replay), copy);
        assert_eq!(closes(y, copy).await, 0);
        // With the entry out of reach, the fallback checks the copy by
        // content only.
        let a = commit(&r, Some(copy), 5).await;
        let b = commit(&r, Some(a), 6).await;
        r.meta().cas_ref("main", Some(copy), Some(a)).await.unwrap();
        for i in 0..LAND_LOG_LOOKBACK {
            let (from, to) = if i % 2 == 0 { (a, b) } else { (b, a) };
            r.meta()
                .cas_ref("main", Some(from), Some(to))
                .await
                .unwrap();
        }
        assert_eq!(
            landed_of(r.land_as(copy, "main", Some("ada")).await.unwrap()),
            copy
        );
        assert_eq!(closes(copy, copy).await, 0);
        assert_eq!(
            r.review_request(qa.id).await.unwrap().state,
            ReviewRequestState::Open
        );
    }

    /// Of concurrent creates of one changeset by different tokens exactly
    /// one records itself, and a later create of a stored changeset never
    /// does, even when no creator was recorded.
    #[tokio::test]
    async fn only_the_insert_records_a_creator() {
        let r = repo();
        let base = commit(&r, None, 1).await;
        let cs = r.get_changeset(base).await.unwrap();
        let old = r
            .create_changeset_by(cs.clone(), Some("mallory"))
            .await
            .unwrap();
        assert_eq!(r.created_by(old).await.unwrap(), None);
        let next = Changeset {
            parents: vec![base],
            message: "raced".into(),
            ..cs
        };
        let names = ["a", "b", "c", "d", "e", "f", "g", "h"];
        let made = futures_util::future::join_all(
            names
                .iter()
                .map(|n| r.create_changeset_by(next.clone(), Some(n))),
        )
        .await;
        let id = made[0].as_ref().unwrap();
        assert!(made.iter().all(|m| m.as_ref().unwrap() == id));
        let by = r.created_by(*id).await.unwrap().unwrap();
        assert!(names.contains(&by.principal.as_str()));
        r.create_changeset_by(next, Some("mallory")).await.unwrap();
        assert_eq!(r.created_by(*id).await.unwrap(), Some(by));
    }

    /// A land closes the request of what it landed, of the stack below
    /// it (by the copies it replayed) and of anything now on the ref,
    /// and leaves other requests open.
    #[tokio::test]
    async fn landing_closes_requests_of_the_stack_and_its_copies() {
        let r = repo();
        let base = commit(&r, None, 1).await;
        r.meta().cas_ref("main", None, Some(base)).await.unwrap();
        let s1 = commit(&r, Some(base), 2).await;
        let s2 = commit(&r, Some(s1), 3).await;
        let elsewhere = commit(&r, Some(base), 4).await;
        // Someone else lands first, so the stack is replayed.
        let moved = commit(&r, Some(base), 5).await;
        r.land(moved, "main").await.unwrap();
        let (q1, _) = r
            .request_review(ask(s1, "ada", &["rhea"]), None)
            .await
            .unwrap();
        let (q2, _) = r
            .request_review(ask(s2, "ada", &["rhea"]), None)
            .await
            .unwrap();
        let (q3, _) = r
            .request_review(ask(elsewhere, "bo", &["rhea"]), None)
            .await
            .unwrap();
        r.meta().cas_ref("dev", None, Some(base)).await.unwrap();
        let mut dev = ask(s2, "ada", &["rhea"]);
        dev.target_ref = "dev".into();
        let (q4, _) = r.request_review(dev, None).await.unwrap();
        let landed = match r.land(s2, "main").await.unwrap() {
            leep_core::api::LandResponse::Landed {
                landed, rebased, ..
            } => {
                assert!(rebased);
                landed
            }
            other => panic!("{other:?}"),
        };
        assert_eq!(
            r.close_landed_review_requests("main", s2, landed, None, None)
                .await
                .unwrap(),
            2
        );
        let q2 = r.review_request(q2.id).await.unwrap();
        assert_eq!(
            (q2.state, q2.landed_as),
            (ReviewRequestState::Landed, Some(landed))
        );
        let q1 = r.review_request(q1.id).await.unwrap();
        let copy_of_s1 = r.get_changeset(landed).await.unwrap().parents[0];
        assert_eq!(
            (q1.state, q1.landed_as),
            (ReviewRequestState::Landed, Some(copy_of_s1))
        );
        for q in [q3.id, q4.id] {
            assert_eq!(
                r.review_request(q).await.unwrap().state,
                ReviewRequestState::Open
            );
        }
        // Closing again closes nothing.
        assert_eq!(
            r.close_landed_review_requests("main", s2, landed, None, None)
                .await
                .unwrap(),
            0
        );

        // A move the land path never reported: listing notices.
        let ff = commit(&r, Some(landed), 6).await;
        let (q5, _) = r
            .request_review(ask(ff, "bo", &["rhea"]), None)
            .await
            .unwrap();
        r.meta()
            .cas_ref("main", Some(landed), Some(ff))
            .await
            .unwrap();
        let open: Vec<u64> = r
            .review_requests(&open_for("rhea"), None)
            .await
            .unwrap()
            .0
            .iter()
            .map(|q| q.id)
            .collect();
        assert_eq!(open, [q4.id, q3.id]);
        let q5 = r.review_request(q5.id).await.unwrap();
        assert_eq!(
            (q5.state, q5.landed_as),
            (ReviewRequestState::Landed, Some(ff))
        );
    }
}
