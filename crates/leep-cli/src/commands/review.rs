//! `leep review` and `leep reviews`: record verdicts and see what a changeset
//! still needs before it can land (ADR 0011).

use clap::{Args, Subcommand};
use leep_core::api::{AddReviewRequest, ReviewStatusResponse, ReviewsResponse};
use leep_core::review::Verdict;

use super::{print_json, Remote};
use crate::error::{exit, Result};
use crate::fmt::{plural, short, utc_time};

#[derive(Subcommand, Debug)]
pub enum ReviewCmd {
    /// Approve a changeset as the authenticated user.
    Approve(ApproveArgs),
    /// Reject a changeset; an owner's rejection blocks it from landing.
    Reject(RejectArgs),
    /// Show, per changed path, who still has to approve (exit 1 if not approved).
    Status(StatusArgs),
    /// List the reviews a changeset has.
    List(ListArgs),
}

#[derive(Args, Debug)]
pub struct ApproveArgs {
    /// Changeset id, ref name, or `@` for the working copy base.
    pub rev: String,
    /// Optional comment.
    #[arg(short = 'm', long)]
    pub message: Option<String>,
}

#[derive(Args, Debug)]
pub struct RejectArgs {
    pub rev: String,
    /// Why it is rejected.
    #[arg(short = 'm', long)]
    pub message: String,
}

#[derive(Args, Debug)]
pub struct StatusArgs {
    pub rev: String,
    /// Ref whose owners and head to judge against (default: the target ref).
    #[arg(long = "ref")]
    pub target_ref: Option<String>,
}

#[derive(Args, Debug)]
pub struct ListArgs {
    pub rev: String,
}

pub fn run(r: &Remote, cmd: &ReviewCmd) -> Result<i32> {
    match cmd {
        ReviewCmd::Approve(a) => verdict(r, &a.rev, Verdict::Approve, a.message.clone()),
        ReviewCmd::Reject(a) => verdict(r, &a.rev, Verdict::Reject, Some(a.message.clone())),
        ReviewCmd::Status(a) => status(r, a),
        ReviewCmd::List(a) => list(r, a),
    }
}

fn verdict(r: &Remote, rev: &str, v: Verdict, comment: Option<String>) -> Result<i32> {
    let id = r.resolve(rev)?;
    let resp = r.client.add_review(
        &id,
        &AddReviewRequest {
            verdict: v,
            comment: comment.unwrap_or_default(),
        },
    )?;
    if r.json {
        print_json(&resp)?;
    } else {
        let word = match v {
            Verdict::Approve => "Approved",
            Verdict::Reject => "Rejected",
        };
        println!("{word} {} as {}", short(id), resp.reviewer);
    }
    Ok(0)
}

pub fn status(r: &Remote, args: &StatusArgs) -> Result<i32> {
    let id = r.resolve(&args.rev)?;
    let target = args.target_ref.as_deref().unwrap_or(r.target_ref);
    let resp = r.client.review_status(&id, target)?;
    if r.json {
        print_json(&resp)?;
    } else {
        for line in status_lines(&resp) {
            println!("{line}");
        }
    }
    // A predicate: "not approved" is exit 1, not an error (ADR 0033).
    Ok(if resp.approved { exit::OK } else { exit::USER })
}

pub fn list(r: &Remote, args: &ListArgs) -> Result<i32> {
    let id = r.resolve(&args.rev)?;
    let resp = r.client.reviews(&id)?;
    if r.json {
        print_json(&resp)?;
    } else {
        for line in review_lines(id, &resp) {
            println!("{line}");
        }
    }
    Ok(0)
}

/// Human-readable review status, one line per changed path.
pub fn status_lines(resp: &ReviewStatusResponse) -> Vec<String> {
    let mut out = vec![format!(
        "changeset {} on {}: {}",
        short(resp.changeset),
        resp.target_ref,
        if resp.approved {
            "approved"
        } else {
            "not approved"
        }
    )];
    for p in &resp.paths {
        let state = match p.rule.as_str() {
            "pending" => "PENDING",
            "rejected" => "REJECTED",
            _ => "ok",
        };
        let mut notes = Vec::new();
        if p.owners.is_empty() {
            notes.push("no owners: any reviewer".to_string());
        } else {
            notes.push(format!("owners: {}", p.owners.join(", ")));
        }
        if !p.approved_by.is_empty() {
            notes.push(format!("approved by: {}", p.approved_by.join(", ")));
        }
        if !p.rejected_by.is_empty() {
            notes.push(format!("rejected by: {}", p.rejected_by.join(", ")));
        }
        out.push(format!(
            "  {state:<8} {:<20} {} ({})",
            p.rule,
            p.path,
            notes.join("; ")
        ));
    }
    if resp.paths.is_empty() {
        out.push("  (no changed paths)".into());
    }
    out
}

/// Human-readable review list.
pub fn review_lines(id: leep_core::ChangesetId, resp: &ReviewsResponse) -> Vec<String> {
    if resp.reviews.is_empty() {
        return vec![format!("changeset {} has no reviews", short(id))];
    }
    let mut out = vec![format!(
        "{} on {}:",
        plural(resp.reviews.len(), "review"),
        short(id)
    )];
    if let Some(via) = resp.via {
        out.push(format!(
            "  (reviews of {}, which this changeset was rebased from; they do not count for landing)",
            short(via)
        ));
    }
    for r in &resp.reviews {
        let mut line = format!(
            "  {} {} {}",
            r.reviewer,
            r.verdict.as_str(),
            utc_time(r.timestamp)
        );
        if !r.comment.is_empty() {
            line.push_str(&format!(" {:?}", r.comment));
        }
        out.push(line);
    }
    out
}

#[cfg(test)]
mod tests {
    use leep_core::api::PathReview;
    use leep_core::review::Review;
    use leep_core::{ChangesetId, Hash, RepoPath};

    use super::*;

    fn id() -> ChangesetId {
        ChangesetId(Hash::of_blob(b"x"))
    }

    fn path(p: &str, rule: &str, owners: &[&str], ok: &[&str], no: &[&str]) -> PathReview {
        let v = |s: &[&str]| s.iter().map(|x| x.to_string()).collect();
        PathReview {
            path: RepoPath::new(p).unwrap(),
            rule: rule.into(),
            owners: v(owners),
            approved_by: v(ok),
            rejected_by: v(no),
        }
    }

    #[test]
    fn status_lists_each_path() {
        let resp = ReviewStatusResponse {
            changeset: id(),
            target_ref: "main".into(),
            approved: false,
            paths: vec![
                path("a.rs", "auto:comment_only", &["alice"], &[], &[]),
                path("b.rs", "pending", &["alice", "bob"], &[], &[]),
                path("c.rs", "rejected", &[], &["carol"], &["dan"]),
            ],
        };
        let lines = status_lines(&resp);
        assert!(lines[0].ends_with("on main: not approved"), "{lines:?}");
        assert!(
            lines[1].contains("ok")
                && lines[1].contains("auto:comment_only")
                && lines[1].contains(" a.rs ")
        );
        assert!(lines[2].contains("PENDING") && lines[2].contains("owners: alice, bob"));
        assert!(lines[3].contains("REJECTED"));
        assert!(lines[3].contains("no owners") && lines[3].contains("rejected by: dan"));
        assert!(lines[3].contains("approved by: carol"));
    }

    #[test]
    fn empty_status_says_so() {
        let resp = ReviewStatusResponse {
            changeset: id(),
            target_ref: "main".into(),
            approved: true,
            paths: vec![],
        };
        let lines = status_lines(&resp);
        assert!(lines[0].ends_with(": approved"));
        assert_eq!(lines[1], "  (no changed paths)");
    }

    #[test]
    fn review_list_mentions_via() {
        let none = ReviewsResponse {
            reviews: vec![],
            via: None,
        };
        assert!(review_lines(id(), &none)[0].contains("no reviews"));
        let resp = ReviewsResponse {
            reviews: vec![Review {
                reviewer: "alice".into(),
                verdict: Verdict::Approve,
                comment: "ok".into(),
                timestamp: 0,
            }],
            via: Some(ChangesetId(Hash::of_blob(b"orig"))),
        };
        let lines = review_lines(id(), &resp);
        assert!(lines[0].starts_with("1 review "), "{lines:?}");
        assert!(lines[1].contains("rebased from"));
        assert!(lines[2].contains("alice approve 1970-01-01") && lines[2].contains("\"ok\""));
    }
}
