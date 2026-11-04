//! `leep review request`, `leep review inbox` and `leep review cancel`:
//! ask for a review through the server and see what waits for you
//! (ADR 0086).

use clap::Args;
use leep_core::api::{CreateReviewRequest, ReviewRequestEntry, ReviewRequestState};
use leep_core::outln;

use super::{print_json, Remote};
use crate::error::{CliError, ErrorKind, Result};
use crate::fmt::{short, utc_time};

#[derive(Args, Debug)]
pub struct RequestArgs {
    /// Changeset to review: id, ref name, or `@` for the working copy base.
    pub rev: String,
    /// Reviewer to ask, by token name; repeat for more. Default: the owners
    /// of the paths still waiting for approval, else anyone who may review.
    #[arg(long = "to", value_name = "NAME")]
    pub to: Vec<String>,
    /// A note for the reviewers.
    #[arg(short = 'm', long)]
    pub message: Option<String>,
    /// Ref the changeset is meant to land on (default: the target ref).
    #[arg(long = "ref")]
    pub target_ref: Option<String>,
}

#[derive(Args, Debug)]
pub struct InboxArgs {
    /// Requests you made, instead of the ones waiting for you.
    #[arg(long)]
    pub mine: bool,
    /// Include closed requests (landed or withdrawn).
    #[arg(long)]
    pub all: bool,
    /// Show at most this many (default 50).
    #[arg(short = 'n', long, value_name = "N")]
    pub limit: Option<usize>,
}

#[derive(Args, Debug)]
pub struct CancelArgs {
    /// The request's number, as `leep review inbox --mine` shows it.
    pub id: u64,
}

pub fn request(r: &Remote, args: &RequestArgs) -> Result<i32> {
    let id = r.resolve(&args.rev)?;
    let target = args.target_ref.as_deref().unwrap_or(r.target_ref);
    let resp = r
        .client
        .request_review(&CreateReviewRequest {
            changeset: id,
            target_ref: target.to_string(),
            reviewers: args.to.clone(),
            message: args.message.clone().unwrap_or_default(),
        })
        .map_err(unsupported)?;
    if r.json {
        print_json(&resp)?;
        return Ok(0);
    }
    let q = &resp.request.request;
    let verb = if resp.existing {
        "Already requested"
    } else {
        "Requested"
    };
    outln!(
        "{verb} review of {} on {} from {} (#{})",
        short(q.changeset),
        q.target_ref,
        who_reviews(&q.reviewers),
        q.id
    );
    Ok(0)
}

pub fn inbox(r: &Remote, args: &InboxArgs) -> Result<i32> {
    let me = r
        .client
        .whoami()?
        .name
        .ok_or_else(|| CliError::user("review requests need a token: set LEEP_TOKEN"))?;
    let mut query = vec![(if args.mine { "requester" } else { "reviewer" }, me)];
    if !args.all {
        query.push(("open", "true".into()));
    }
    if let Some(n) = args.limit {
        query.push(("limit", n.to_string()));
    }
    let resp = r.client.review_requests(&query).map_err(unsupported)?;
    if r.json {
        print_json(&resp)?;
        return Ok(0);
    }
    if resp.requests.is_empty() {
        outln!(
            "{}",
            match (args.mine, args.all) {
                (true, false) => "You have no open review requests.",
                (true, true) => "You have made no review requests.",
                (false, false) => "No review requests are waiting for you.",
                (false, true) => "No review requests name you.",
            }
        );
        return Ok(0);
    }
    for e in &resp.requests {
        for line in entry_lines(e) {
            outln!("{line}");
        }
    }
    Ok(0)
}

pub fn cancel(r: &Remote, args: &CancelArgs) -> Result<i32> {
    let e = r
        .client
        .withdraw_review_request(args.id)
        .map_err(unsupported)?;
    if r.json {
        print_json(&e)?;
        return Ok(0);
    }
    match e.request.state {
        ReviewRequestState::Withdrawn => outln!("Withdrew review request #{}", e.request.id),
        s => outln!("Review request #{} is already {}", e.request.id, s.as_str()),
    }
    Ok(0)
}

fn who_reviews(reviewers: &[String]) -> String {
    if reviewers.is_empty() {
        "any reviewer".into()
    } else {
        reviewers.join(", ")
    }
}

/// An older server has no `/v1/review-requests`.
fn unsupported(e: CliError) -> CliError {
    if e.kind() == ErrorKind::NotFound && e.server_code().is_none() {
        return CliError::user("this server does not take review requests; upgrade it (ADR 0086)");
    }
    e
}

/// How `leep review inbox` shows one request: a header line, then the
/// changeset's title and the requester's note, indented.
pub fn entry_lines(e: &ReviewRequestEntry) -> Vec<String> {
    let q = &e.request;
    let mut head = format!(
        "#{} {} {} -> {} by {}",
        q.id,
        q.state.as_str(),
        short(q.changeset),
        q.target_ref,
        q.requester
    );
    if let Some(c) = e.created_by.as_ref().filter(|c| c.principal != q.requester) {
        head.push_str(&format!(" (created by {})", c.principal));
    }
    head.push_str(&format!(
        ", for {} ({})",
        who_reviews(&q.reviewers),
        utc_time(q.created)
    ));
    if let Some(at) = q.landed_as.filter(|l| *l != q.changeset) {
        head.push_str(&format!(", landed as {}", short(at)));
    }
    let mut out = vec![head];
    if let Some(t) = e.title.as_deref().filter(|t| !t.is_empty()) {
        out.push(format!("    {t}"));
    }
    for line in q.message.lines() {
        out.push(format!("    > {line}"));
    }
    out
}

#[cfg(test)]
mod tests {
    use leep_core::api::{CreatedBy, ReviewRequest};
    use leep_core::{ChangesetId, Hash};

    use super::*;

    #[test]
    fn entries_show_requester_creator_reviewers_and_note() {
        let c = ChangesetId(Hash::of_blob(b"c"));
        let mut e = ReviewRequestEntry {
            request: ReviewRequest {
                id: 7,
                changeset: c,
                target_ref: "main".into(),
                requester: "ada".into(),
                reviewers: vec!["bo".into(), "rhea".into()],
                message: "api change\nsee ADR".into(),
                created: 0,
                state: ReviewRequestState::Open,
                closed: None,
                landed_as: None,
            },
            created_by: Some(CreatedBy {
                principal: "ada".into(),
                timestamp: 0,
            }),
            title: Some("core: add a field".into()),
            approved_by: Default::default(),
            rejected_by: Default::default(),
        };
        let lines = entry_lines(&e);
        assert_eq!(
            lines,
            [
                format!(
                    "#7 open {} -> main by ada, for bo, rhea ({})",
                    short(c),
                    utc_time(0)
                ),
                "    core: add a field".to_string(),
                "    > api change".to_string(),
                "    > see ADR".to_string(),
            ]
        );
        e.created_by.as_mut().unwrap().principal = "cy".into();
        e.request.reviewers.clear();
        let copy = ChangesetId(Hash::of_blob(b"copy"));
        e.request.state = ReviewRequestState::Landed;
        e.request.landed_as = Some(copy);
        assert_eq!(
            entry_lines(&e)[0],
            format!(
                "#7 landed {} -> main by ada (created by cy), for any reviewer ({}), landed as {}",
                short(c),
                utc_time(0),
                short(copy)
            )
        );
    }
}
