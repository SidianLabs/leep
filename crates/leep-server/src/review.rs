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
//!   "docs_globs": ["**/*.md", "docs/**"] }
//! ```

use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use leep_core::api::{PathReview, ReviewStatusResponse, ReviewsResponse};
use leep_core::review::{Review, NEEDS_REVIEW_MARKER};
use leep_core::{ChangesetId, Error, Result};
use leep_derive::owners::OwnersResolver;
use leep_derive::review::{AutoRule, ChangesetReview, PathRule, ReviewRules};
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
}

/// The parsed policy file: the refs that require review and the auto rules.
pub struct ReviewPolicy {
    pub refs: Vec<String>,
    pub rules: ReviewRules,
    /// The rule names and globs as written, so a reload can tell whether the
    /// rules changed (`ReviewRules` is opaque).
    pub auto_approve: Vec<String>,
    pub docs_globs: Vec<String>,
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
        let rules = ReviewRules::new(auto, &file.docs_globs).map_err(|e| {
            e.to_string()
                .trim_start_matches("invalid input: ")
                .to_string()
        })?;
        Ok(ReviewPolicy {
            refs: file.refs,
            rules,
            auto_approve: file.auto_approve,
            docs_globs: file.docs_globs,
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

    /// The `GET .../reviews` answer. A changeset with none of its own but
    /// recorded as a server-made rebase of one that has some reports those,
    /// with `via` naming the original.
    pub async fn describe(&self, repo: &Repo, id: ChangesetId) -> Result<ReviewsResponse> {
        let own = self.reviews_for(repo, id).await?;
        if !own.is_empty() {
            return Ok(ReviewsResponse {
                reviews: own,
                via: None,
            });
        }
        for origin in repo.rebase_chain(id).await? {
            let found = self.reviews_for(repo, origin).await?;
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

    async fn judge(
        &self,
        repo: &Repo,
        id: ChangesetId,
        owners: &mut OwnersResolver<'_>,
    ) -> Result<ChangesetReview> {
        let reviews = self.reviews_for(repo, id).await?;
        repo.review_changeset(id, &self.rules, &reviews, owners)
            .await
    }

    /// The `GET .../review-status` answer: `id` judged against the current
    /// head of `target_ref`, with the changeset's own reviews only (a rebased
    /// copy's original is not consulted, as in the land policy).
    pub async fn status(
        &self,
        repo: &Repo,
        id: ChangesetId,
        target_ref: &str,
    ) -> Result<ReviewStatusResponse> {
        repo.get_changeset(id).await?;
        let head = repo.meta().get_ref(target_ref).await?;
        let mut owners = OwnersResolver::new(repo, head).await?;
        let review = self.judge(repo, id, &mut owners).await?;
        Ok(ReviewStatusResponse {
            changeset: id,
            target_ref: target_ref.to_string(),
            approved: review.approved(),
            paths: review
                .paths
                .into_iter()
                .map(|p| PathReview {
                    path: p.path,
                    rule: p.rule.label(),
                    owners: p.owners,
                    approved_by: p.approved_by,
                    rejected_by: p.rejected_by,
                })
                .collect(),
        })
    }
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
        for id in new_history {
            let review = self.service.judge(repo, *id, &mut owners).await?;
            if let Some(p) = review.first_blocker() {
                tracing::info!(%id, target_ref, path = %p.path, "land refused: needs review");
                return Err(Error::Invalid(blocker_text(*id, p)));
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
    }

    #[test]
    fn missing_file_names_the_path() {
        let e = ReviewPolicy::from_file(Path::new("/nonexistent/policy.json"))
            .err()
            .unwrap();
        assert!(e.contains("/nonexistent/policy.json"), "{e}");
    }
}
