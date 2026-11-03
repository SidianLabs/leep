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
use leep_core::changeset::EXTRA_REBASED_FROM;
use leep_core::review_request::{
    check_request, CreatedBy, NewReviewRequest, ReviewRequest, ReviewRequestFilter,
    ReviewRequestState, CREATED_BY_KIND,
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
    /// `created-by-v1` row written with the changeset. The first creator
    /// stays: a changeset that already has a row is stored without a new
    /// one. `None` (no authentication) records nothing.
    pub async fn create_changeset_by(
        &self,
        cs: Changeset,
        principal: Option<&str>,
    ) -> Result<ChangesetId> {
        let Some(principal) = principal else {
            return self.create_changeset(cs).await;
        };
        let id = cs.id();
        if self
            .meta()
            .get_derived(CREATED_BY_KIND, &id.0)
            .await?
            .is_some()
        {
            return self.create_changeset(cs).await;
        }
        let row = CreatedBy {
            principal: principal.to_string(),
            timestamp: unix_now(),
        };
        self.create_changeset_with_rows(
            cs,
            vec![DerivedRow::new(
                CREATED_BY_KIND,
                id.0,
                Bytes::from(row.encode()),
            )],
        )
        .await
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
        if let Some(head) = self.meta().get_ref(&req.target_ref).await? {
            if self.on_first_parent_of(req.changeset, head).await? {
                return Err(Error::Invalid(format!(
                    "changeset {} is already on {}",
                    req.changeset, req.target_ref
                )));
            }
        }
        let same = self
            .meta()
            .list_review_requests(&ReviewRequestFilter {
                requester: Some(req.requester.clone()),
                changeset: Some(req.changeset),
                target_ref: Some(req.target_ref.clone()),
                states: vec![ReviewRequestState::Open],
                limit: 50,
                ..Default::default()
            })
            .await?;
        if let Some(r) = same.into_iter().find(|r| r.reviewers == req.reviewers) {
            return Ok((r, true));
        }
        Ok((self.meta().put_review_request(&req).await?, false))
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
    async fn copies_below(&self, head: ChangesetId) -> Result<HashMap<ChangesetId, ChangesetId>> {
        let mut out = HashMap::new();
        let mut cur = Some(head);
        let mut seen = HashSet::new();
        while let Some(id) = cur.filter(|_| out.len() < COPY_WALK_LIMIT) {
            if !seen.insert(id) {
                break;
            }
            let cs = self.get_changeset(id).await?;
            let Some(origin) = cs
                .extra
                .get(EXTRA_REBASED_FROM)
                .and_then(|o| o.parse::<ChangesetId>().ok())
            else {
                break;
            };
            out.insert(origin, id);
            cur = cs.parents.first().copied();
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
