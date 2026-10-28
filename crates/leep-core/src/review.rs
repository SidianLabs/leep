//! Review verdicts (ADR 0011).
//!
//! Like signatures, a review is an attestation stored beside the changeset it
//! is about, keyed by its id, so `Changeset` is untouched. This module holds
//! what server and client must agree on: the stored type, the derived-data
//! kind and the texts they match on.

use serde::{Deserialize, Serialize};

use crate::{Error, Result};

/// Derived-data kind holding one stored [`Review`] per (changeset, reviewer).
pub const REVIEW_KIND: &str = "review-v1";

/// Name of the owners file in any directory of the tree.
pub const OWNERS_FILE: &str = "OWNERS";

/// Error text shared by the server's policy and the CLI's hint.
pub const NEEDS_REVIEW_MARKER: &str = "needs review";

/// Longest accepted review comment, in bytes.
pub const MAX_COMMENT_BYTES: usize = 64 * 1024;

/// A reviewer's decision. Externally tagged (unit variants), never
/// `#[serde(tag)]`, because postcard stores it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Verdict {
    Approve,
    Reject,
    /// Takes back the reviewer's earlier verdict (derived rows have no
    /// delete). The policy counts it as no review. Appended last so stored
    /// rows keep decoding (ADR 0063).
    Withdrawn,
}

impl Verdict {
    pub fn as_str(self) -> &'static str {
        match self {
            Verdict::Approve => "approve",
            Verdict::Reject => "reject",
            Verdict::Withdrawn => "withdrawn",
        }
    }
}

/// Derived-data kind holding one [`ReviewSource`] per (changeset, reviewer),
/// keyed as [`REVIEW_KIND`] is, written when a review is posted on behalf of
/// someone else (ADR 0063).
pub const REVIEW_SOURCE_KIND: &str = "review-source-v1";

/// Longest accepted `source` string, in bytes.
pub const MAX_SOURCE_BYTES: usize = 1024;

/// Where a review posted for a named reviewer came from. Persisted with
/// postcard (field order is part of the format, see FORMATS.md).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReviewSource {
    /// Token name that posted the review.
    pub via: String,
    /// Free-form origin such as `github:o/r#12/reviews/34`.
    pub source: String,
    /// Unix seconds when the server stored it.
    pub timestamp: i64,
}

impl ReviewSource {
    pub fn encode(&self) -> Vec<u8> {
        postcard::to_stdvec(self).expect("review source encoding cannot fail")
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

/// Reject a `source` too long to be a short origin string.
pub fn check_source(source: &str) -> Result<()> {
    if source.len() > MAX_SOURCE_BYTES {
        return Err(Error::Invalid(format!(
            "review source is {} bytes; the limit is {MAX_SOURCE_BYTES}",
            source.len()
        )));
    }
    Ok(())
}

/// One stored review. Persisted with postcard (field order is part of the
/// format, see FORMATS.md).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Review {
    /// Token name of the authenticated reviewer, never taken from a request body.
    pub reviewer: String,
    pub verdict: Verdict,
    pub comment: String,
    /// Unix seconds when the server stored it.
    pub timestamp: i64,
}

impl Review {
    pub fn encode(&self) -> Vec<u8> {
        postcard::to_stdvec(self).expect("review encoding cannot fail")
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        Ok(postcard::from_bytes(bytes)?)
    }
}

/// Reject comments too large to be a comment.
pub fn check_comment(comment: &str) -> Result<()> {
    if comment.len() > MAX_COMMENT_BYTES {
        return Err(Error::Invalid(format!(
            "review comment is {} bytes; the limit is {MAX_COMMENT_BYTES}",
            comment.len()
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stored_encoding_round_trips_and_is_stable() {
        let r = Review {
            reviewer: "alice".into(),
            verdict: Verdict::Reject,
            comment: "no".into(),
            timestamp: 7,
        };
        let bytes = r.encode();
        // len-prefixed name (1 + 5), variant index 1, len-prefixed comment
        // (1 + 2), zigzag varint timestamp (1).
        assert_eq!(
            bytes,
            [5, b'a', b'l', b'i', b'c', b'e', 1, 2, b'n', b'o', 14]
        );
        assert_eq!(Review::decode(&bytes).unwrap(), r);
        assert!(Review::decode(&bytes[..bytes.len() - 1]).is_err());
    }

    #[test]
    fn rows_written_before_withdrawn_existed_still_decode() {
        // Bytes the encoder produced when Verdict had two variants.
        let approve = [5, b'a', b'l', b'i', b'c', b'e', 0, 0, 14];
        let reject = [5, b'a', b'l', b'i', b'c', b'e', 1, 0, 14];
        assert_eq!(Review::decode(&approve).unwrap().verdict, Verdict::Approve);
        assert_eq!(Review::decode(&reject).unwrap().verdict, Verdict::Reject);
        let w = Review {
            reviewer: "alice".into(),
            verdict: Verdict::Withdrawn,
            comment: String::new(),
            timestamp: 7,
        };
        assert_eq!(w.encode()[6], 2);
        assert_eq!(Review::decode(&w.encode()).unwrap(), w);
    }

    #[test]
    fn review_source_encoding_is_stable() {
        let s = ReviewSource {
            via: "m".into(),
            source: "gh".into(),
            timestamp: 7,
        };
        let bytes = s.encode();
        assert_eq!(bytes, [1, b'm', 2, b'g', b'h', 14]);
        assert_eq!(ReviewSource::decode(&bytes).unwrap(), s);
        assert!(check_source(&"x".repeat(MAX_SOURCE_BYTES + 1)).is_err());
    }

    #[test]
    fn verdict_json_is_snake_case() {
        assert_eq!(
            serde_json::from_str::<Verdict>("\"withdrawn\"").unwrap(),
            Verdict::Withdrawn
        );
        assert_eq!(
            serde_json::to_string(&Verdict::Approve).unwrap(),
            "\"approve\""
        );
        assert_eq!(
            serde_json::from_str::<Verdict>("\"reject\"").unwrap(),
            Verdict::Reject
        );
        assert!(serde_json::from_str::<Verdict>("\"maybe\"").is_err());
    }

    #[test]
    fn oversized_comments_are_refused() {
        assert!(check_comment(&"x".repeat(MAX_COMMENT_BYTES)).is_ok());
        assert!(check_comment(&"x".repeat(MAX_COMMENT_BYTES + 1)).is_err());
    }
}
