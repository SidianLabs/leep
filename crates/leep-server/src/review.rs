//! The review service: policy file, reviewer lookup and the `--require-review`
//! land policy (ADR 0011).
//!
//! Reviews are keyed by (changeset, reviewer name) and the store has no prefix
//! listing, so a changeset's reviews are found with one lookup per principal:
//! the write-role tokens from the tokens file. Reviewers are therefore bound
//! to the token names the server was started with.
//!
//! The policy file is read at startup and again on SIGHUP (ADR 0024), and is
//! strict like the others:
//!
//! ```json
//! { "refs": ["main"],
//!   "auto_approve": ["comment_only", "whitespace_only", "docs_only"],
//!   "docs_globs": ["**/*.md", "docs/**"],
//!   "identical_only_globs": ["**/Cargo.lock"] }
//! ```
//!
//! `identical_only_globs` (ADR 0092) name paths whose verdicts carry only to
//! identical changes, never to a rebase or union (ADR 0089).

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use leep_core::api::{
    OriginReview, OriginStatus, PathReview, ReviewEntry, ReviewStatusResponse, ReviewsResponse,
};
use leep_core::review::{Review, ReviewSource, Verdict, NEEDS_REVIEW_MARKER};
use leep_core::{ChangesetId, Error, Result};
use leep_derive::owners::OwnersResolver;
use leep_derive::review::{
    AutoRule, Carried, CarryRefusal, CarryScope, ChangesetReview, PathRule, ReviewRules,
};
use leep_derive::{LandPolicy, Repo};
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PolicyFile {
    #[serde(default)]
    refs: Vec<String>,
    #[serde(default)]
    auto_approve: Vec<String>,
    #[serde(default)]
    docs_globs: Vec<String>,
    #[serde(default)]
    identical_only_globs: Vec<String>,
}

/// Most candidates with reviews one judgement compares (ADR 0085).
pub const MAX_CARRY_CHECKS: usize = 16;

/// Reviews of carry candidates read once per status request or land check.
#[derive(Default)]
pub struct CarryCache {
    reviews: HashMap<ChangesetId, Vec<Review>>,
}

/// The parsed policy file: the refs that require review and the auto rules.
pub struct ReviewPolicy {
    pub refs: Vec<String>,
    pub rules: ReviewRules,
    /// The rule names and globs as written, so a reload can tell whether the
    /// rules changed (`ReviewRules` is opaque).
    pub auto_approve: Vec<String>,
    pub docs_globs: Vec<String>,
    pub identical_only_globs: Vec<String>,
}

impl ReviewPolicy {
    pub fn parse(text: &str) -> std::result::Result<ReviewPolicy, String> {
        let file: PolicyFile = serde_json::from_str(text).map_err(|e| format!("bad JSON: {e}"))?;
        if file.refs.iter().any(|r| r.trim().is_empty()) {
            return Err("empty ref name in refs".into());
        }
        let mut auto = Vec::new();
        for name in &file.auto_approve {
            auto.push(AutoRule::parse(name).ok_or_else(|| {
                let valid: Vec<_> = AutoRule::ALL.iter().map(|r| r.name()).collect();
                format!(
                    "unknown auto_approve rule {name:?} (expected one of {})",
                    valid.join(", ")
                )
            })?);
        }
        let rules = ReviewRules::new(auto, &file.docs_globs)
            .and_then(|r| r.with_identical_only(&file.identical_only_globs))
            .map_err(|e| {
                e.to_string()
                    .trim_start_matches("invalid input: ")
                    .to_string()
            })?;
        Ok(ReviewPolicy {
            refs: file.refs,
            rules,
            auto_approve: file.auto_approve,
            docs_globs: file.docs_globs,
            identical_only_globs: file.identical_only_globs,
        })
    }

    pub fn from_file(path: &Path) -> std::result::Result<ReviewPolicy, String> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| format!("cannot read review policy file {}: {e}", path.display()))?;
        ReviewPolicy::parse(&text)
            .map_err(|e| format!("review policy file {}: {e}", path.display()))
    }
}

/// Reads and judges reviews for the endpoints and the land policy.
pub struct ReviewService {
    rules: ReviewRules,
    reviewers: Vec<String>,
}

impl ReviewService {
    /// `reviewers` are the principals whose reviews count (see `Auth::reviewers`).
    pub fn new(reviewers: Vec<String>, rules: ReviewRules) -> Self {
        ReviewService { rules, reviewers }
    }

    /// The policy's auto rules, for closing review requests by content.
    pub fn rules(&self) -> &ReviewRules {
        &self.rules
    }

    /// The principals whose reviews count.
    pub fn reviewers(&self) -> &[String] {
        &self.reviewers
    }

    /// Stored reviews of exactly `id`, one lookup per principal, sorted by
    /// reviewer. A row that does not decode or names another reviewer counts
    /// as absent.
    pub async fn reviews_for(&self, repo: &Repo, id: ChangesetId) -> Result<Vec<Review>> {
        let mut out = Vec::new();
        for name in &self.reviewers {
            match repo.get_review(id, name).await {
                Ok(Some(r)) if &r.reviewer == name => out.push(r),
                Ok(_) | Err(Error::Encoding(_)) => {}
                Err(e) => return Err(e),
            }
        }
        out.sort_by(|a, b| a.reviewer.cmp(&b.reviewer));
        Ok(out)
    }

    /// [`Self::reviews_for`] with each review's `review-source-v1` row, when
    /// there is one.
    async fn entries_for(&self, repo: &Repo, id: ChangesetId) -> Result<Vec<ReviewEntry>> {
        let mut out = Vec::new();
        for review in self.reviews_for(repo, id).await? {
            let row = repo.get_review_source(id, &review.reviewer).await?;
            out.push(ReviewEntry {
                source: row
                    .as_ref()
                    .map(|s| s.source.clone())
                    .filter(|s| !s.is_empty()),
                via: row.map(|s| s.via),
                review,
                carried_from: None,
            });
        }
        Ok(out)
    }

    /// The `GET .../reviews` answer: `id`'s own reviews, then those that
    /// carry to it (ADR 0085) with `carried_from` naming the changeset
    /// reviewed, one per reviewer and candidate, as the judge reads them.
    /// A changeset with neither but recorded as a server-made rebase of one
    /// that has some reports those, with `via` naming the original: they do
    /// not count, since the copy does not reproduce their change.
    pub async fn describe(&self, repo: &Repo, id: ChangesetId) -> Result<ReviewsResponse> {
        let mut own = self.entries_for(repo, id).await?;
        let mine: Vec<Review> = own.iter().map(|e| e.review.clone()).collect();
        // A listing names no ref: judge as the server's default lands do,
        // with the policy's identical-only globs but no ref's `OWNERS`.
        let carried = self
            .carried(
                repo,
                id,
                &mine,
                &mut CarryScope::for_ref(repo.carry_merges(""), None),
                &mut CarryCache::default(),
                None,
            )
            .await?;
        for c in carried {
            for review in c.reviews {
                let row = repo.get_review_source(c.from, &review.reviewer).await?;
                own.push(ReviewEntry {
                    source: row
                        .as_ref()
                        .map(|s| s.source.clone())
                        .filter(|s| !s.is_empty()),
                    via: row.map(|s| s.via),
                    review,
                    carried_from: Some(c.from),
                });
            }
        }
        if !own.is_empty() {
            return Ok(ReviewsResponse {
                reviews: own,
                via: None,
            });
        }
        for origin in repo.rebase_chain(id).await? {
            let found = self.entries_for(repo, origin).await?;
            if !found.is_empty() {
                return Ok(ReviewsResponse {
                    reviews: found,
                    via: Some(origin),
                });
            }
        }
        Ok(ReviewsResponse {
            reviews: Vec::new(),
            via: None,
        })
    }

    /// Judge `id` for a land on `target_ref` with its own reviews and those
    /// carried from the changesets it names as its origins whose reviewed
    /// changes it reproduces (ADR 0085, 0089). `seen` keeps each
    /// candidate's reviews for the rest of the request or land check;
    /// `origins`, when given, gets what was found about each candidate.
    async fn judge(
        &self,
        repo: &Repo,
        id: ChangesetId,
        target_ref: &str,
        owners: &mut OwnersResolver<'_>,
        seen: &mut CarryCache,
        origins: Option<&mut Vec<OriginStatus>>,
    ) -> Result<ChangesetReview> {
        let reviews = self.reviews_for(repo, id).await?;
        let mut scope = CarryScope::for_ref(repo.carry_merges(target_ref), Some(&mut *owners));
        let carried = self
            .carried(repo, id, &reviews, &mut scope, seen, origins)
            .await?;
        repo.review_changeset_carried(id, &self.rules, &reviews, &carried, owners)
            .await
    }

    /// The verdicts `id` takes from candidate origins. A reviewer with any
    /// review of `id` itself, a withdrawal included, has said what they
    /// think of `id`, so only reviewers without one carry. Withdrawals on a
    /// candidate are kept: each reviewer's newest review among the
    /// candidates that reproduce a path decides it. Each candidate counts on
    /// its own, so a resolved stack carries every entry whose reviewed
    /// changes it kept exactly.
    ///
    /// At most [`MAX_CARRY_CHECKS`] candidates with reviews are compared.
    /// One more and nothing carries at all: stopping early could drop a
    /// newer reject that a later candidate holds. `scope` says where a path
    /// may carry as the server's rebase or a union of insertions (ADR 0089,
    /// 0092).
    /// `origins`, when given, gets each compared candidate with its
    /// verdicts and why it did not carry.
    async fn carried(
        &self,
        repo: &Repo,
        id: ChangesetId,
        own: &[Review],
        scope: &mut CarryScope<'_, '_>,
        seen: &mut CarryCache,
        mut origins: Option<&mut Vec<OriginStatus>>,
    ) -> Result<Vec<Carried>> {
        let mut out = Vec::new();
        let mine: HashSet<&str> = own.iter().map(|r| r.reviewer.as_str()).collect();
        let mut checks = 0;
        for from in repo.carry_candidates(id).await? {
            let reviews: Vec<Review> = match seen.reviews.get(&from) {
                Some(r) => r.clone(),
                None => {
                    let r = self.reviews_for(repo, from).await?;
                    seen.reviews.insert(from, r.clone());
                    r
                }
            };
            let reviews: Vec<Review> = reviews
                .into_iter()
                .filter(|r| !mine.contains(r.reviewer.as_str()))
                .collect();
            if reviews.is_empty() {
                continue;
            }
            checks += 1;
            if checks > MAX_CARRY_CHECKS {
                tracing::info!(%id, "review carry: too many reviewed candidates, none carried");
                if let Some(o) = origins {
                    for s in o.iter_mut() {
                        s.carried = false;
                        s.differs.clear();
                        s.reason = Some(format!(
                            "more than {MAX_CARRY_CHECKS} reviewed origins; none carry"
                        ));
                    }
                }
                return Ok(Vec::new());
            }
            let check = repo.carry_check(from, id, &self.rules, scope).await?;
            if let Some(o) = origins.as_deref_mut() {
                let (carried, differs, reason) = match &check {
                    Ok(paths) if !paths.is_empty() => (true, Vec::new(), None),
                    Ok(_) => (
                        false,
                        Vec::new(),
                        Some("every path of it is approved by an auto rule".to_string()),
                    ),
                    Err(CarryRefusal::Differs(p)) => (false, vec![p.clone()], None),
                    Err(why @ CarryRefusal::IdenticalOnly { path, .. }) => {
                        (false, vec![path.clone()], Some(why.to_string()))
                    }
                    Err(why) => (false, Vec::new(), Some(why.to_string())),
                };
                o.push(OriginStatus {
                    from,
                    reviews: reviews
                        .iter()
                        .map(|r| OriginReview {
                            reviewer: r.reviewer.clone(),
                            verdict: r.verdict,
                        })
                        .collect(),
                    carried,
                    differs,
                    reason,
                });
            }
            // An empty set reproduces nothing a verdict could count on.
            match check {
                Ok(paths) if !paths.is_empty() => out.push(Carried {
                    from,
                    paths,
                    reviews,
                }),
                _ => {}
            }
        }
        Ok(out)
    }

    /// After `reviewer` rejects or withdraws on `id`, the same verdict on
    /// each candidate whose approval by them would carry to `id`. Otherwise
    /// a later copy naming only that candidate would carry the approval the
    /// reviewer just took back on an identical change. Returns the
    /// candidates written.
    ///
    /// The verdict being posted is the reviewer's newest word whatever the
    /// clocks say, so an approval stamped later (skewed servers, a clock
    /// stepped back) is still taken back, and the row written is stamped no
    /// earlier than it so the judge never ranks it below. Not capped: every
    /// approval left would stay live, and the hint walk bounds the work.
    pub async fn take_back(
        &self,
        repo: &Repo,
        id: ChangesetId,
        review: &Review,
        via: &str,
    ) -> Result<Vec<ChangesetId>> {
        if review.verdict == Verdict::Approve {
            return Ok(Vec::new());
        }
        let mut written = Vec::new();
        for from in repo.carry_candidates(id).await? {
            let theirs = match repo.get_review(from, &review.reviewer).await {
                Ok(Some(r)) => r,
                Ok(None) | Err(Error::Encoding(_)) => continue,
                Err(e) => return Err(e),
            };
            if theirs.verdict != Verdict::Approve {
                continue;
            }
            // With merges whatever the ref or its files declare: taking
            // back more than any ref would carry is the safe side.
            if !repo
                .carried_paths(from, id, &self.rules, &mut CarryScope::widest())
                .await?
                .is_some_and(|p| !p.is_empty())
            {
                continue;
            }
            let timestamp = if theirs.timestamp >= review.timestamp {
                theirs.timestamp + 1
            } else {
                review.timestamp
            };
            repo.put_review_source(
                from,
                &review.reviewer,
                &ReviewSource {
                    via: via.to_string(),
                    source: format!("leep:taken-back-on:{id}"),
                    timestamp,
                },
            )
            .await?;
            repo.put_review(
                from,
                &Review {
                    reviewer: review.reviewer.clone(),
                    verdict: review.verdict,
                    comment: format!(
                        "{} on {id}, which makes this change",
                        review.verdict.as_str()
                    ),
                    timestamp,
                },
            )
            .await?;
            written.push(from);
        }
        Ok(written)
    }

    /// The `GET .../review-status` answer: `id` judged against the current
    /// head of `target_ref` as the land policy judges it, with its own
    /// reviews and any carried from an origin it reproduces (ADR 0085).
    /// `on_refs` are the refs to report as already holding `id`, among
    /// `target_ref` and `policy_refs`.
    pub async fn status(
        &self,
        repo: &Repo,
        id: ChangesetId,
        target_ref: &str,
        policy_refs: &[String],
    ) -> Result<ReviewStatusResponse> {
        repo.get_changeset(id).await?;
        let head = repo.meta().get_ref(target_ref).await?;
        let mut owners = OwnersResolver::new(repo, head).await?;
        let mut origins = Vec::new();
        let review = self
            .judge(
                repo,
                id,
                target_ref,
                &mut owners,
                &mut CarryCache::default(),
                Some(&mut origins),
            )
            .await?;
        let on_refs = {
            let mut refs = vec![target_ref.to_string()];
            refs.extend(policy_refs.iter().filter(|r| *r != target_ref).cloned());
            repo.refs_holding(id, &refs).await?
        };
        // A changeset already on a ref has nothing left to refuse.
        let refused = if on_refs.is_empty() {
            bad_owners_text(repo, id).await?
        } else {
            None
        };
        // The carry rule of each path, so a reviewer sees where only an
        // identical change keeps their verdict (ADR 0092).
        let mut carry = Vec::with_capacity(review.paths.len());
        let merges = repo.carry_merges(target_ref);
        for p in &review.paths {
            let mut scope = CarryScope::for_ref(merges, Some(&mut owners));
            let identical = scope.identical_only(&self.rules, &p.path).await?.is_some();
            carry.push(if identical { "identical" } else { "merged" });
        }
        Ok(ReviewStatusResponse {
            changeset: id,
            target_ref: target_ref.to_string(),
            approved: review.approved() && refused.is_none(),
            on_refs,
            paths: review
                .paths
                .into_iter()
                .zip(carry)
                .map(|(p, carry)| PathReview {
                    path: p.path,
                    rule: p.rule.label(),
                    owners: p.owners,
                    approved_by: p.approved_by,
                    rejected_by: p.rejected_by,
                    carried_from: p.carried_from,
                    carried_rule: p.carried_rule.map(|r| r.name().to_string()),
                    carry: Some(carry.to_string()),
                })
                .collect(),
            origins,
            refused,
        })
    }
}

/// The refusal for an `OWNERS` line `id` writes that starts with `carry`
/// but is no directive (ADR 0092): a misspelled `carry identical` must not
/// land and leave the directory on the looser default unnoticed.
async fn bad_owners_text(repo: &Repo, id: ChangesetId) -> Result<Option<String>> {
    Ok(repo.unknown_carry_line(id).await?.map(|line| {
        format!(
            "changeset {id} has an OWNERS line that is no directive: {line} \
             (write `carry identical` or `carry merged`)"
        )
    }))
}

/// `land` onto any of `refs` needs every changeset it adds to be approved.
pub struct RequireReview {
    service: Arc<ReviewService>,
    refs: HashSet<String>,
}

impl RequireReview {
    pub fn new(service: Arc<ReviewService>, refs: impl IntoIterator<Item = String>) -> Self {
        RequireReview {
            service,
            refs: refs.into_iter().collect(),
        }
    }
}

fn blocker_text(id: ChangesetId, p: &leep_derive::review::PathStatus) -> String {
    let why = match p.rule {
        PathRule::Rejected => format!("rejected by {}", p.rejected_by.join(", ")),
        _ if p.owners.is_empty() => "needs approval from any reviewer".to_string(),
        _ => format!("needs approval from one of [{}]", p.owners.join(", ")),
    };
    format!(
        "changeset {id} {NEEDS_REVIEW_MARKER}: path {} {why}",
        p.path
    )
}

#[async_trait]
impl LandPolicy for RequireReview {
    fn applies_to(&self, target_ref: &str) -> bool {
        self.refs.contains(target_ref)
    }

    /// Owners come from the head's tree, so a decision is only good for it.
    fn depends_on_head(&self) -> bool {
        true
    }

    async fn check_land(
        &self,
        repo: &Repo,
        target_ref: &str,
        head: Option<ChangesetId>,
        new_history: &[ChangesetId],
    ) -> Result<()> {
        let mut owners = OwnersResolver::new(repo, head).await?;
        let mut seen = CarryCache::default();
        for id in new_history {
            if let Some(why) = bad_owners_text(repo, *id).await? {
                tracing::info!(%id, target_ref, "land refused: bad OWNERS line");
                return Err(Error::Policy(why));
            }
            let review = self
                .service
                .judge(repo, *id, target_ref, &mut owners, &mut seen, None)
                .await?;
            if let Some(p) = review.first_blocker() {
                tracing::info!(%id, target_ref, path = %p.path, "land refused: needs review");
                return Err(Error::Policy(blocker_text(*id, p)));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn err(text: &str) -> String {
        ReviewPolicy::parse(text).err().expect("should be rejected")
    }

    #[test]
    fn parses_a_full_file() {
        let p = ReviewPolicy::parse(
            r#"{"refs":["main","release/1"],
                "auto_approve":["comment_only","whitespace_only","docs_only"],
                "docs_globs":["**/*.md","docs/**"]}"#,
        )
        .unwrap();
        assert_eq!(p.refs, ["main", "release/1"]);
        assert!(p.rules.enabled(AutoRule::CommentOnly));
        assert!(p.rules.is_docs("docs/a/b.png"));
        assert_eq!(p.rules.identical_only_glob("Cargo.lock"), None);
    }

    #[test]
    fn identical_only_globs_need_no_auto_rule_and_name_the_first_match() {
        let p = ReviewPolicy::parse(
            r#"{"identical_only_globs":["**/Cargo.lock","*.lock","migrations/**"]}"#,
        )
        .unwrap();
        assert_eq!(p.identical_only_globs.len(), 3);
        assert_eq!(
            p.rules.identical_only_glob("Cargo.lock"),
            Some("**/Cargo.lock")
        );
        assert_eq!(
            p.rules.identical_only_glob("a/Cargo.lock"),
            Some("**/Cargo.lock")
        );
        assert_eq!(p.rules.identical_only_glob("yarn.lock"), Some("*.lock"));
        assert_eq!(p.rules.identical_only_glob("a/yarn.lock"), None);
        assert_eq!(
            p.rules.identical_only_glob("migrations/1.sql"),
            Some("migrations/**")
        );
        assert_eq!(p.rules.identical_only_glob("src/lib.rs"), None);
    }

    #[test]
    fn every_field_is_optional() {
        let p = ReviewPolicy::parse("{}").unwrap();
        assert!(p.refs.is_empty());
        assert!(!p.rules.enabled(AutoRule::CommentOnly));
    }

    #[test]
    fn rejects_bad_files() {
        assert!(err("nope").contains("bad JSON"));
        assert!(err(r#"{"refs":[],"mystery":1}"#).contains("unknown field"));
        assert!(err(r#"{"refs":[" "]}"#).contains("empty ref"));
        let e = err(r#"{"auto_approve":["everything"]}"#);
        assert!(
            e.contains("everything") && e.contains("comment_only"),
            "{e}"
        );
        assert!(err(r#"{"auto_approve":["docs_only"]}"#).contains("docs_globs"));
        assert!(err(r#"{"docs_globs":["a"]}"#).contains("docs_only"));
        assert!(
            err(r#"{"auto_approve":["docs_only"],"docs_globs":["["]}"#).contains("bad docs glob")
        );
        assert!(err(r#"{"auto_approve":"comment_only"}"#).contains("bad JSON"));
        assert!(err(r#"{"identical_only_globs":["["]}"#).contains("bad identical_only glob"));
    }

    #[test]
    fn missing_file_names_the_path() {
        let e = ReviewPolicy::from_file(Path::new("/nonexistent/policy.json"))
            .err()
            .unwrap();
        assert!(e.contains("/nonexistent/policy.json"), "{e}");
    }
}
