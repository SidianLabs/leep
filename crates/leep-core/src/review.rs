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
}

impl Verdict {
    pub fn as_str(self) -> &'static str {
        match self {
            Verdict::Approve => "approve",
            Verdict::Reject => "reject",
        }
    }
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
    fn verdict_json_is_snake_case() {
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
