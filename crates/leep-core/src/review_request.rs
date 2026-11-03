//! Review requests and the principal that created a changeset (ADR 0086).
//!
//! A review request is server state, not an attestation: it is opened by
//! one principal, read by many and closed when its changeset lands, so it
//! lives in its own SQL table (JSON on the wire, columns at rest, like the
//! ref log) where concurrent requests are separate rows and listing by
//! reviewer is an index lookup. Who created a changeset is a derived row
//! written with the changeset, because it never changes once written.

use serde::{Deserialize, Serialize};

use crate::{ChangesetId, Error, Result};

/// Derived-data kind holding one [`CreatedBy`] per changeset id, written in
/// the same store write as the changeset when an authenticated client
/// creates it.
pub const CREATED_BY_KIND: &str = "created-by-v1";

/// The token that first stored a changeset. Persisted with postcard (field
/// order is part of the format, see FORMATS.md); on the wire as JSON in
/// `ChangesetInfo::created_by`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreatedBy {
    /// Token name of the authenticated client, never taken from the body.
    pub principal: String,
    /// Unix seconds by the server that stored it.
    pub timestamp: i64,
}

impl CreatedBy {
    pub fn encode(&self) -> Vec<u8> {
        postcard::to_stdvec(self).expect("created-by encoding cannot fail")
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

/// Longest accepted review request message, in bytes.
pub const MAX_REQUEST_MESSAGE_BYTES: usize = 4 * 1024;

/// Most open review requests one requester may have at once; one more is
/// refused with [`TOO_MANY_REQUESTS_MARKER`] (code
/// `too_many_review_requests`). Keeps a runaway agent from filling the
/// table and every inbox.
pub const MAX_OPEN_REQUESTS: usize = 100;

/// Stable text in the 400 for a requester at [`MAX_OPEN_REQUESTS`].
pub const TOO_MANY_REQUESTS_MARKER: &str = "open review requests, the most one requester may have";

/// Most reviewers one request may name.
pub const MAX_REQUEST_REVIEWERS: usize = 32;

/// Where a review request is in its life. Stored as its [`as_str`] text.
///
/// [`as_str`]: ReviewRequestState::as_str
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReviewRequestState {
    /// Waiting for its reviewers.
    Open,
    /// Its changeset, or the copy a land made of it, reached the target ref.
    Landed,
    /// The requester took it back.
    Withdrawn,
}

impl ReviewRequestState {
    pub const ALL: [ReviewRequestState; 3] = [
        ReviewRequestState::Open,
        ReviewRequestState::Landed,
        ReviewRequestState::Withdrawn,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            ReviewRequestState::Open => "open",
            ReviewRequestState::Landed => "landed",
            ReviewRequestState::Withdrawn => "withdrawn",
        }
    }

    pub fn parse(s: &str) -> Result<Self> {
        Self::ALL
            .into_iter()
            .find(|v| v.as_str() == s)
            .ok_or_else(|| {
                Error::Invalid(format!(
                    "unknown review request state {s:?} (expected open, landed or withdrawn)"
                ))
            })
    }
}

/// One review request. JSON on the wire (`/v1/review-requests`) and SQL
/// columns at rest; never hashed or encoded with postcard, so it may grow
/// optional fields.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReviewRequest {
    /// Assigned by the store, increasing.
    pub id: u64,
    pub changeset: ChangesetId,
    /// The ref the requester means to land on; its owners decide.
    pub target_ref: String,
    /// Token name of the principal that opened it.
    pub requester: String,
    /// Token names asked to review, sorted. Empty means any reviewer.
    #[serde(default)]
    pub reviewers: Vec<String>,
    #[serde(default)]
    pub message: String,
    /// Unix seconds by the server that stored it.
    pub created: i64,
    pub state: ReviewRequestState,
    /// When it stopped being open.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub closed: Option<i64>,
    /// For a landed request, the changeset the ref got for it: the changeset
    /// itself, or the copy a land replayed it as. Absent when not known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub landed_as: Option<ChangesetId>,
}

/// What [`crate::MetaStore::put_review_request`] stores; the store assigns
/// the id and the request starts open.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NewReviewRequest {
    pub changeset: ChangesetId,
    pub target_ref: String,
    pub requester: String,
    /// Sorted and deduplicated by the caller.
    pub reviewers: Vec<String>,
    pub message: String,
    pub created: i64,
}

/// Which requests [`crate::MetaStore::list_review_requests`] returns, newest
/// (highest id) first. Every set field must match.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ReviewRequestFilter {
    /// Requests that name this reviewer, and requests open to any reviewer
    /// that someone else made.
    pub reviewer: Option<String>,
    pub requester: Option<String>,
    pub changeset: Option<ChangesetId>,
    pub target_ref: Option<String>,
    /// Any of these states; empty means every state.
    pub states: Vec<ReviewRequestState>,
    /// Only ids below this, for paging.
    pub before: Option<u64>,
    /// At most this many; 0 means none.
    pub limit: usize,
}

impl ReviewRequestFilter {
    /// Whether `r` passes, for stores that filter in memory.
    pub fn matches(&self, r: &ReviewRequest) -> bool {
        let reviewer_ok = match &self.reviewer {
            None => true,
            Some(x) => {
                r.reviewers.iter().any(|n| n == x) || (r.reviewers.is_empty() && &r.requester != x)
            }
        };
        reviewer_ok
            && self.requester.as_ref().is_none_or(|x| &r.requester == x)
            && self.changeset.is_none_or(|c| r.changeset == c)
            && self.target_ref.as_ref().is_none_or(|t| &r.target_ref == t)
            && (self.states.is_empty() || self.states.contains(&r.state))
            && self.before.is_none_or(|b| r.id < b)
    }
}

/// Check a request's free-form parts before it is stored.
pub fn check_request(req: &NewReviewRequest) -> Result<()> {
    if req.requester.trim().is_empty() {
        return Err(Error::Invalid("review request has no requester".into()));
    }
    if req.message.len() > MAX_REQUEST_MESSAGE_BYTES {
        return Err(Error::Invalid(format!(
            "review request message is {} bytes; the limit is {MAX_REQUEST_MESSAGE_BYTES}",
            req.message.len()
        )));
    }
    if req
        .message
        .chars()
        .any(|c| c.is_control() && c != '\n' && c != '\t')
    {
        return Err(Error::Invalid(
            "review request message may not contain control characters".into(),
        ));
    }
    if req.reviewers.len() > MAX_REQUEST_REVIEWERS {
        return Err(Error::Invalid(format!(
            "a review request names at most {MAX_REQUEST_REVIEWERS} reviewers, got {}",
            req.reviewers.len()
        )));
    }
    if req.reviewers.iter().any(|r| r.trim().is_empty()) {
        return Err(Error::Invalid("empty reviewer name".into()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Hash;

    fn req(id: u64, requester: &str, reviewers: &[&str]) -> ReviewRequest {
        ReviewRequest {
            id,
            changeset: ChangesetId(Hash::of_blob(b"c")),
            target_ref: "main".into(),
            requester: requester.into(),
            reviewers: reviewers.iter().map(|s| s.to_string()).collect(),
            message: String::new(),
            created: 1,
            state: ReviewRequestState::Open,
            closed: None,
            landed_as: None,
        }
    }

    /// The stored text of each state is fixed: it is in every database.
    #[test]
    fn state_names_are_stable() {
        let names: Vec<_> = ReviewRequestState::ALL.iter().map(|s| s.as_str()).collect();
        assert_eq!(names, ["open", "landed", "withdrawn"]);
        for s in ReviewRequestState::ALL {
            assert_eq!(ReviewRequestState::parse(s.as_str()).unwrap(), s);
            assert_eq!(
                serde_json::to_value(s).unwrap(),
                serde_json::json!(s.as_str())
            );
        }
        assert!(ReviewRequestState::parse("closed").is_err());
    }

    /// Golden bytes: a change to `CreatedBy` changes stored rows.
    #[test]
    fn created_by_encoding_is_stable() {
        let c = CreatedBy {
            principal: "ada".into(),
            timestamp: 1_700_000_000,
        };
        let bytes = c.encode();
        assert_eq!(hex::encode(&bytes), "0361646180c49fd50c");
        assert_eq!(CreatedBy::decode(&bytes).unwrap(), c);
        assert!(CreatedBy::decode(&[0xff]).is_err());
    }

    #[test]
    fn reviewer_filter_takes_any_reviewer_requests_of_others() {
        let f = ReviewRequestFilter {
            reviewer: Some("rhea".into()),
            ..Default::default()
        };
        assert!(f.matches(&req(1, "ada", &["rhea"])));
        assert!(f.matches(&req(2, "ada", &[])));
        assert!(!f.matches(&req(3, "ada", &["bo"])));
        assert!(!f.matches(&req(4, "rhea", &[])), "not your own");
    }

    #[test]
    fn old_and_new_shapes_decode() {
        let v = serde_json::json!({
            "id": 3,
            "changeset": ChangesetId(Hash::of_blob(b"c")).to_string(),
            "target_ref": "main",
            "requester": "ada",
            "created": 5,
            "state": "open",
        });
        let r: ReviewRequest = serde_json::from_value(v).unwrap();
        assert!(r.reviewers.is_empty() && r.message.is_empty() && r.closed.is_none());
        let back = serde_json::to_value(&r).unwrap();
        assert!(back.get("closed").is_none() && back.get("landed_as").is_none());
    }

    #[test]
    fn requests_are_checked() {
        let ok = NewReviewRequest {
            changeset: ChangesetId(Hash::of_blob(b"c")),
            target_ref: "main".into(),
            requester: "ada".into(),
            reviewers: vec!["rhea".into()],
            message: "please look\n\tthanks".into(),
            created: 1,
        };
        check_request(&ok).unwrap();
        for bad in [
            NewReviewRequest {
                requester: " ".into(),
                ..ok.clone()
            },
            NewReviewRequest {
                message: "x".repeat(MAX_REQUEST_MESSAGE_BYTES + 1),
                ..ok.clone()
            },
            NewReviewRequest {
                message: "bell\u{7}".into(),
                ..ok.clone()
            },
            NewReviewRequest {
                reviewers: vec![String::new()],
                ..ok.clone()
            },
            NewReviewRequest {
                reviewers: (0..=MAX_REQUEST_REVIEWERS).map(|i| i.to_string()).collect(),
                ..ok.clone()
            },
        ] {
            assert!(check_request(&bad).is_err(), "{bad:?}");
        }
    }
}
