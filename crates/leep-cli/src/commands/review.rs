//! `leep review` and `leep reviews`: record verdicts and see what a changeset
//! still needs before it can land (ADR 0011).

use clap::{Args, Subcommand};
use leep_core::api::{AddReviewRequest, PathReview, ReviewStatusResponse, ReviewsResponse};
use leep_core::outln;
use leep_core::review::Verdict;

use super::{print_json, Remote};
use crate::error::{exit, Result};
use crate::fmt::{plural, short, utc_time};

#[derive(Subcommand, Debug)]
pub enum ReviewCmd {
    /// Approve a changeset as the authenticated user, then say whether it
    /// may now land on the target ref or which paths still wait.
    Approve(ApproveArgs),
    /// Reject a changeset; an owner's rejection blocks it from landing.
    /// Says where that leaves it, as `approve` does.
    Reject(RejectArgs),
    /// Take back your earlier approval or rejection; it then counts as no review.
    Withdraw(ApproveArgs),
    /// Show, per changed path, who still has to approve (exit 1 if not approved).
    Status(StatusArgs),
    /// List the reviews a changeset has.
    List(ListArgs),
    /// Ask for a review of a changeset before it lands (default reviewers:
    /// the owners of the paths still pending).
    Request(super::review_request::RequestArgs),
    /// List the review requests waiting for you (--mine: the ones you made).
    Inbox(super::review_request::InboxArgs),
    /// Withdraw a review request you made.
    Cancel(super::review_request::CancelArgs),
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
        ReviewCmd::Withdraw(a) => verdict(r, &a.rev, Verdict::Withdrawn, a.message.clone()),
        ReviewCmd::Status(a) => status(r, a),
        ReviewCmd::List(a) => list(r, a),
        ReviewCmd::Request(a) => super::review_request::request(r, a),
        ReviewCmd::Inbox(a) => super::review_request::inbox(r, a),
        ReviewCmd::Cancel(a) => super::review_request::cancel(r, a),
    }
}

fn verdict(r: &Remote, rev: &str, v: Verdict, comment: Option<String>) -> Result<i32> {
    let id = r.resolve(rev)?;
    let resp = r.client.add_review(
        &id,
        &AddReviewRequest {
            verdict: v,
            comment: comment.unwrap_or_default(),
            reviewer: None,
            source: None,
        },
    )?;
    if r.json {
        print_json(&resp)?;
    } else {
        let word = match v {
            Verdict::Approve => "Approved",
            Verdict::Reject => "Rejected",
            Verdict::Withdrawn => "Withdrew review of",
        };
        outln!("{word} {} as {}", short(id), resp.reviewer);
        // Only a hint: the verdict is recorded whatever this says.
        match r.client.review_status(&id, r.target_ref) {
            Ok(st) => {
                for line in verdict_follow_up(&st) {
                    outln!("{line}");
                }
            }
            Err(e) => eprintln!(
                "(could not read the review status on {}: {e})",
                r.target_ref
            ),
        }
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
            outln!("{line}");
        }
    }
    // A predicate: "not approved" is exit 1, not an error (ADR 0034).
    Ok(if resp.approved { exit::OK } else { exit::USER })
}

pub fn list(r: &Remote, args: &ListArgs) -> Result<i32> {
    let id = r.resolve(&args.rev)?;
    let resp = r.client.reviews(&id)?;
    if r.json {
        print_json(&resp)?;
    } else {
        for line in review_lines(id, &resp) {
            outln!("{line}");
        }
    }
    Ok(0)
}

/// Human-readable review status, one line per changed path, then how to
/// see the change. The server's rule names stay in `--json`; here each path
/// says what satisfied it or what it still needs.
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
    let width = resp
        .paths
        .iter()
        .map(|p| p.path.as_str().chars().count())
        .max()
        .unwrap_or(0);
    for p in &resp.paths {
        let (state, why) = path_state(p);
        out.push(format!("  {state:<8} {:<width$}  {why}", p.path.as_str()));
    }
    if resp.paths.is_empty() {
        out.push("  (no changed paths)".into());
    }
    out.push(format!("diff: leep diff {}", short(resp.changeset)));
    out
}

/// The state column and the explanation for one path.
fn path_state(p: &PathReview) -> (&'static str, String) {
    let who = |names: &[String]| names.join(", ");
    let whose = if p.owners.is_empty() {
        "any reviewer"
    } else {
        "owner"
    };
    let rejected = || {
        let mut why = format!("rejected by {}", who(&p.rejected_by));
        if !p.approved_by.is_empty() {
            why.push_str(&format!("; approved by {}", who(&p.approved_by)));
        }
        why
    };
    match p.rule.as_str() {
        "pending" if p.owners.is_empty() => {
            ("PENDING", "needs an approval from any reviewer".into())
        }
        "pending" => ("PENDING", format!("needs one of: {}", who(&p.owners))),
        "rejected" => ("REJECTED", rejected()),
        "owner" => (
            "ok",
            format!("approved by {} ({whose})", who(&p.approved_by)),
        ),
        "merge_parent" => ("ok", "unchanged from a merge parent".into()),
        rule if rule.starts_with("auto:") => ("ok", format!("needs no review ({rule})")),
        // A rule this client does not know: say it as the server does.
        rule => ("?", format!("rule {rule}")),
    }
}

/// After a verdict: whether the changeset may now land on `target_ref`, or
/// which paths still hold it back.
pub fn verdict_follow_up(resp: &ReviewStatusResponse) -> Vec<String> {
    if resp.approved {
        return vec![format!("now approved for {}", resp.target_ref)];
    }
    let mut out = vec![format!("not yet approved for {}:", resp.target_ref)];
    for p in &resp.paths {
        let (state, why) = path_state(p);
        if state != "ok" {
            out.push(format!("  {state:<8} {}  {why}", p.path));
        }
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
        match (&r.source, &r.via) {
            (Some(s), Some(v)) => line.push_str(&format!(" [{s} via {v}]")),
            (Some(s), None) => line.push_str(&format!(" [{s}]")),
            (None, Some(v)) => line.push_str(&format!(" [via {v}]")),
            (None, None) => {}
        }
        out.push(line);
    }
    out
}

#[cfg(test)]
mod tests {
    use leep_core::api::ReviewEntry;
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
    fn status_says_what_each_path_needs() {
        let resp = ReviewStatusResponse {
            changeset: id(),
            target_ref: "main".into(),
            approved: false,
            paths: vec![
                path("a.rs", "auto:comment_only", &["alice"], &[], &[]),
                path("b.rs", "pending", &["alice", "bob"], &[], &[]),
                path("c.rs", "rejected", &[], &["carol"], &["dan"]),
                path("d.rs", "owner", &[], &["rhea"], &[]),
                path("e.rs", "owner", &["alice"], &["alice"], &[]),
                path("f.rs", "pending", &[], &[], &[]),
                path("g.rs", "merge_parent", &[], &[], &[]),
                path("h.rs", "future_rule", &[], &[], &[]),
            ],
        };
        let lines = status_lines(&resp);
        let short = short(id());
        assert_eq!(
            lines,
            [
                format!("changeset {short} on main: not approved"),
                "  ok       a.rs  needs no review (auto:comment_only)".into(),
                "  PENDING  b.rs  needs one of: alice, bob".into(),
                "  REJECTED c.rs  rejected by dan; approved by carol".into(),
                "  ok       d.rs  approved by rhea (any reviewer)".into(),
                "  ok       e.rs  approved by alice (owner)".into(),
                "  PENDING  f.rs  needs an approval from any reviewer".into(),
                "  ok       g.rs  unchanged from a merge parent".into(),
                "  ?        h.rs  rule future_rule".into(),
                format!("diff: leep diff {short}"),
            ]
        );
        for l in &lines {
            assert!(
                !l.contains("PENDING  pending") && !l.contains(" owner "),
                "{l}"
            );
        }
        let follow = verdict_follow_up(&resp);
        assert_eq!(follow[0], "not yet approved for main:");
        assert_eq!(follow.len(), 5, "{follow:?}");
        assert!(
            follow[1].contains("b.rs") && follow[3].contains("f.rs"),
            "{follow:?}"
        );
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
        assert_eq!(verdict_follow_up(&resp), ["now approved for main"]);
    }

    #[test]
    fn review_list_mentions_via() {
        let none = ReviewsResponse {
            reviews: vec![],
            via: None,
        };
        assert!(review_lines(id(), &none)[0].contains("no reviews"));
        let resp = ReviewsResponse {
            reviews: vec![ReviewEntry {
                source: Some("github:o/r#1".into()),
                via: Some("mirror".into()),
                review: Review {
                    reviewer: "alice".into(),
                    verdict: Verdict::Approve,
                    comment: "ok".into(),
                    timestamp: 0,
                },
            }],
            via: Some(ChangesetId(Hash::of_blob(b"orig"))),
        };
        let lines = review_lines(id(), &resp);
        assert!(lines[0].starts_with("1 review "), "{lines:?}");
        assert!(lines[1].contains("rebased from"));
        assert!(lines[2].contains("alice approve 1970-01-01") && lines[2].contains("\"ok\""));
        assert!(lines[2].ends_with("[github:o/r#1 via mirror]"), "{lines:?}");
    }
}
