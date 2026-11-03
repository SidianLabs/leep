//! Review requests and who created a changeset (ADR 0086).
//!
//! Requests are rows of their own store table: the server opens them for
//! the authenticated principal, lists them by reviewer or requester, and
//! closes them when their changeset lands. Closing is bookkeeping a land
//! must never wait on or fail for, so the land path calls
//! [`Repo::close_landed_review_requests`] after the ref moved and ignores
//! its errors, and listing closes what a move the land path did not see
//! (a raw ref update, an import) put on the ref.

use std::collections::{HashMap, HashSet};

use bytes::Bytes;
use leep_core::api::ReviewRequestEntry;
use leep_core::review_request::{
    check_request, CreatedBy, NewReviewRequest, ReviewRequest, ReviewRequestFilter,
    ReviewRequestState, CREATED_BY_KIND, MAX_OPEN_REQUESTS, TOO_MANY_REQUESTS_MARKER,
};
use leep_core::store::unix_now;
use leep_core::{refname, Changeset, ChangesetId, DerivedRow, Error, Result};

use crate::Repo;

/// Open requests of one ref a land looks at; more stay open until listed.
const LAND_CLOSE_LIMIT: usize = 500;

/// First-parent steps a land walks from its new head looking for the
/// copies it made of requested changesets.
const COPY_WALK_LIMIT: usize = 256;

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
    /// not be on the ref already.
    pub async fn request_review(&self, mut req: NewReviewRequest) -> Result<(ReviewRequest, bool)> {
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
        self.check_open_requests(&req.requester).await?;
        Ok((self.meta().put_review_request(&req).await?, false))
    }

    /// Refuse a requester at [`MAX_OPEN_REQUESTS`] open requests. At the
    /// cap, a listing first closes the ones that landed by a move the land
    /// path did not report, so only requests really waiting count.
    async fn check_open_requests(&self, requester: &str) -> Result<()> {
        let filter = ReviewRequestFilter {
            requester: Some(requester.to_string()),
            states: vec![ReviewRequestState::Open],
            limit: MAX_OPEN_REQUESTS,
            ..Default::default()
        };
        if self.meta().list_review_requests(&filter).await?.len() < MAX_OPEN_REQUESTS
            || self.review_requests(&filter).await?.0.len() < MAX_OPEN_REQUESTS
        {
            return Ok(());
        }
        Err(Error::Invalid(format!(
            "{requester} has {MAX_OPEN_REQUESTS} {TOO_MANY_REQUESTS_MARKER}; withdraw some \
             (`leep review cancel`) or wait for them to land"
        )))
    }

    /// The requests `filter` matches, and the `before` of the next page
    /// when this one was full. Open ones whose changeset is now on their
    /// ref's first-parent chain are closed as landed first (a move the land
    /// path did not report, such as a raw ref update), and left out when
    /// only open ones were asked for.
    pub async fn review_requests(
        &self,
        filter: &ReviewRequestFilter,
    ) -> Result<(Vec<ReviewRequest>, Option<u64>)> {
        if !self.meta().supports_review_requests() {
            return Err(unsupported());
        }
        let mut reqs = self.meta().list_review_requests(filter).await?;
        let next = reqs
            .last()
            .map(|r| r.id)
            .filter(|_| filter.limit > 0 && reqs.len() >= filter.limit);
        let mut heads: HashMap<String, Option<ChangesetId>> = HashMap::new();
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
            if !self
                .on_first_parent_of(r.changeset, head)
                .await
                .unwrap_or(false)
            {
                continue;
            }
            let landed_as = Some(r.changeset);
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
    /// which put `landed` at the head, satisfied: the submitted changeset,
    /// the changesets of its stack (as themselves or as the copies the land
    /// replayed them as) and anything else now on the head's first-parent
    /// chain. Returns how many it closed. Callers ignore its errors: the
    /// land has happened whatever this does.
    pub async fn close_landed_review_requests(
        &self,
        target_ref: &str,
        submitted: ChangesetId,
        landed: ChangesetId,
    ) -> Result<u64> {
        if !self.meta().supports_review_requests() {
            return Ok(0);
        }
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
        let copies = if submitted == landed {
            HashMap::new()
        } else {
            self.copies_below(landed).await?
        };
        let now = unix_now();
        let mut closed = 0;
        for r in open {
            let landed_as = if r.changeset == submitted || r.changeset == landed {
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
                continue;
            };
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

    /// The server-made copies on `head`'s first-parent chain, by the
    /// changeset each copies, down to the first changeset that is not one.
    /// Only the `rebased-origin-v1` row the server writes when it makes a
    /// copy counts: a changeset's own `leep.rebased_from` is whatever its
    /// client wrote, and trusting it would let one agent's land close
    /// another's request.
    async fn copies_below(&self, head: ChangesetId) -> Result<HashMap<ChangesetId, ChangesetId>> {
        let mut out = HashMap::new();
        let mut cur = Some(head);
        let mut seen = HashSet::new();
        while let Some(id) = cur.filter(|_| out.len() < COPY_WALK_LIMIT) {
            if !seen.insert(id) {
                break;
            }
            let Some(origin) = self.rebased_origin(id).await? else {
                break;
            };
            out.insert(origin, id);
            cur = self.get_changeset(id).await?.parents.first().copied();
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
    /// created it and its title, read in two batches.
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
        let mut about: HashMap<ChangesetId, (Option<CreatedBy>, Option<String>)> = HashMap::new();
        for ((id, cs), row) in ids.iter().zip(changesets).zip(rows) {
            let title = cs.map(|c| c.message.lines().next().unwrap_or("").to_string());
            let by = row.and_then(|b| CreatedBy::decode(&b).ok());
            about.insert(*id, (by, title));
        }
        Ok(reqs
            .into_iter()
            .map(|request| {
                let (created_by, title) =
                    about.get(&request.changeset).cloned().unwrap_or_default();
                ReviewRequestEntry {
                    request,
                    created_by,
                    title,
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
    use crate::tests::repo;

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
            .request_review(ask(c, "ada", &["rhea", "bo", "rhea"]))
            .await
            .unwrap();
        assert!(!existing);
        assert_eq!(a.reviewers, ["bo", "rhea"]);
        let (again, existing) = r
            .request_review(ask(c, "ada", &["bo", "rhea"]))
            .await
            .unwrap();
        assert!(existing);
        assert_eq!(again, a);
        let (other, existing) = r.request_review(ask(c, "ada", &["cy"])).await.unwrap();
        assert!(!existing && other.id != a.id);
        assert_eq!(
            r.review_requests(&open_for("rhea")).await.unwrap().0,
            std::slice::from_ref(&a)
        );
        let entries = r.review_request_entries(vec![a.clone()]).await.unwrap();
        assert_eq!(entries[0].title.as_deref(), Some("change 2"));
        assert_eq!(entries[0].created_by, None);
        // Already on the ref, unknown, a bad ref: refused.
        assert!(r.request_review(ask(base, "ada", &[])).await.is_err());
        let ghost = ChangesetId(Hash::of_blob(b"ghost"));
        assert!(matches!(
            r.request_review(ask(ghost, "ada", &[])).await,
            Err(Error::NotFound(_))
        ));
        let mut bad = ask(c, "ada", &[]);
        bad.target_ref = "a..b".into();
        assert!(r.request_review(bad).await.is_err());
        // A ref that does not exist: nothing could ever close it.
        let mut nowhere = ask(c, "ada", &[]);
        nowhere.target_ref = "nowhere".into();
        assert!(matches!(
            r.request_review(nowhere).await,
            Err(Error::NotFound(_))
        ));

        let w = r.withdraw_review_request(a.id).await.unwrap();
        assert_eq!(w.state, ReviewRequestState::Withdrawn);
        assert!(r
            .review_requests(&open_for("rhea"))
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
            let (q, existing) = r.request_review(ask(c, "ada", &[&name])).await.unwrap();
            assert!(!existing);
            first.get_or_insert(q.id);
        }
        let err = r
            .request_review(ask(c, "ada", &["one-more"]))
            .await
            .unwrap_err();
        assert!(err.to_string().contains(TOO_MANY_REQUESTS_MARKER), "{err}");
        // An existing one is still answered, and others may still ask.
        assert!(r.request_review(ask(c, "ada", &["r0"])).await.unwrap().1);
        r.request_review(ask(c, "bo", &["one-more"])).await.unwrap();
        r.withdraw_review_request(first.unwrap()).await.unwrap();
        r.request_review(ask(c, "ada", &["one-more"]))
            .await
            .unwrap();
        // Requests whose change reached the ref by a raw move stop counting.
        let err = r
            .request_review(ask(c, "ada", &["two-more"]))
            .await
            .unwrap_err();
        assert!(err.to_string().contains(TOO_MANY_REQUESTS_MARKER), "{err}");
        r.meta().cas_ref("main", Some(base), Some(c)).await.unwrap();
        let d = commit(&r, Some(c), 3).await;
        r.request_review(ask(d, "ada", &["two-more"]))
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
        let (qa, _) = r.request_review(ask(x, "ada", &["rhea"])).await.unwrap();
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
        r.land(y, "main").await.unwrap();
        // Cy's land is replayed on top of it.
        let w = commit(&r, Some(base), 4).await;
        let landed = match r.land(w, "main").await.unwrap() {
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
            r.close_landed_review_requests("main", w, landed)
                .await
                .unwrap(),
            0
        );
        let qa = r.review_request(qa.id).await.unwrap();
        assert_eq!((qa.state, qa.landed_as), (ReviewRequestState::Open, None));
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
        let (q1, _) = r.request_review(ask(s1, "ada", &["rhea"])).await.unwrap();
        let (q2, _) = r.request_review(ask(s2, "ada", &["rhea"])).await.unwrap();
        let (q3, _) = r
            .request_review(ask(elsewhere, "bo", &["rhea"]))
            .await
            .unwrap();
        r.meta().cas_ref("dev", None, Some(base)).await.unwrap();
        let mut dev = ask(s2, "ada", &["rhea"]);
        dev.target_ref = "dev".into();
        let (q4, _) = r.request_review(dev).await.unwrap();
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
            r.close_landed_review_requests("main", s2, landed)
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
            r.close_landed_review_requests("main", s2, landed)
                .await
                .unwrap(),
            0
        );

        // A move the land path never reported: listing notices.
        let ff = commit(&r, Some(landed), 6).await;
        let (q5, _) = r.request_review(ask(ff, "bo", &["rhea"])).await.unwrap();
        r.meta()
            .cas_ref("main", Some(landed), Some(ff))
            .await
            .unwrap();
        let open: Vec<u64> = r
            .review_requests(&open_for("rhea"))
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
