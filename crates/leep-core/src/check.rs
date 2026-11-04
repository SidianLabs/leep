//! Check attestations and known-good refs (ADR 0091).
//!
//! A check result is an attestation beside the changeset it is about, like a
//! review: CI posts it through a token allowed to name that check, and the
//! server keeps the newest result per (changeset, check). `green/<ref>` is a
//! ref only the server moves, forward along `<ref>`'s first-parent chain, to
//! the newest changeset whose required checks all passed.

use serde::{Deserialize, Serialize};

use crate::{Error, Result};

/// Derived-data kind holding one stored [`Check`] per (changeset, check name).
pub const CHECK_KIND: &str = "check-v1";

/// Namespace of known-good refs: `green/main` follows `main`.
pub const GREEN_PREFIX: &str = "green/";

/// The ref-log operation of a promotion. Reserved: clients cannot record it.
pub const PROMOTE_OPERATION: &str = "promote";

/// Stable text in the refusal of any client write to a green ref.
pub const PROMOTION_ONLY_MARKER: &str = "moves only by promotion";

/// Longest check name, in bytes.
pub const MAX_NAME_BYTES: usize = 64;
/// Longest `url`, in bytes.
pub const MAX_URL_BYTES: usize = 2048;
/// Longest `summary`, in bytes.
pub const MAX_SUMMARY_BYTES: usize = 4096;

/// Whether `name` is a known-good ref.
pub fn is_green(name: &str) -> bool {
    name.starts_with(GREEN_PREFIX)
}

/// The known-good ref of `name`.
pub fn green_ref(name: &str) -> String {
    format!("{GREEN_PREFIX}{name}")
}

/// What a check run concluded. Externally tagged unit variants, never
/// `#[serde(tag)]`, because postcard stores it; append new variants last.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    Pass,
    Fail,
}

impl Outcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Outcome::Pass => "pass",
            Outcome::Fail => "fail",
        }
    }
}

/// One stored check result. Persisted with postcard (field order is part of
/// the format, see FORMATS.md).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Check {
    /// The check's name, such as `test`.
    pub check: String,
    pub outcome: Outcome,
    /// Token name that posted it, never taken from a request body.
    pub by: String,
    /// Where the run's log is, if the poster said.
    pub url: String,
    /// One short line about the run.
    pub summary: String,
    /// Unix seconds when the server stored it.
    pub timestamp: i64,
}

impl Check {
    pub fn encode(&self) -> Vec<u8> {
        postcard::to_stdvec(self).expect("check encoding cannot fail")
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        Ok(postcard::from_bytes(bytes)?)
    }

    /// Reject a result whose name or texts are out of bounds.
    pub fn validate(&self) -> Result<()> {
        check_name(&self.check)?;
        if self.url.len() > MAX_URL_BYTES || self.url.chars().any(char::is_control) {
            return Err(Error::Invalid(format!(
                "check url must be up to {MAX_URL_BYTES} bytes without control characters"
            )));
        }
        if self.summary.len() > MAX_SUMMARY_BYTES {
            return Err(Error::Invalid(format!(
                "check summary is {} bytes; the limit is {MAX_SUMMARY_BYTES}",
                self.summary.len()
            )));
        }
        Ok(())
    }
}

/// A check name: 1 to 64 of `a-z 0-9 - _ .`, so it is safe in URLs, logs,
/// ref log messages and the tokens file.
pub fn check_name(name: &str) -> Result<()> {
    let ok = !name.is_empty()
        && name.len() <= MAX_NAME_BYTES
        && name
            .bytes()
            .all(|b| matches!(b, b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.'));
    if ok {
        Ok(())
    } else {
        Err(Error::Invalid(format!(
            "bad check name {name:?}: expected 1 to {MAX_NAME_BYTES} of a-z 0-9 - _ ."
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encoding_is_frozen() {
        let c = Check {
            check: "t".into(),
            outcome: Outcome::Fail,
            by: "ci".into(),
            url: "u".into(),
            summary: String::new(),
            timestamp: 1,
        };
        // Field order and variant indexes are the format (FORMATS.md).
        assert_eq!(c.encode(), [1, b't', 1, 2, b'c', b'i', 1, b'u', 0, 2]);
        assert_eq!(Check::decode(&c.encode()).unwrap(), c);
        assert_eq!(postcard::to_stdvec(&Outcome::Pass).unwrap(), [0]);
    }

    #[test]
    fn names_and_texts_are_bounded() {
        for good in ["test", "clippy", "e2e.linux-x86_64", "a_b"] {
            check_name(good).unwrap();
        }
        for bad in ["", "Test", "a b", "a/b", &"x".repeat(65)] {
            assert!(check_name(bad).is_err(), "{bad:?}");
        }
        let mut c = Check {
            check: "test".into(),
            outcome: Outcome::Pass,
            by: "ci".into(),
            url: "https://ci/1".into(),
            summary: "ok".into(),
            timestamp: 0,
        };
        c.validate().unwrap();
        c.url = "a\nb".into();
        assert!(c.validate().is_err());
        c.url.clear();
        c.summary = "x".repeat(MAX_SUMMARY_BYTES + 1);
        assert!(c.validate().is_err());
    }

    #[test]
    fn green_names() {
        assert_eq!(green_ref("main"), "green/main");
        assert!(is_green("green/main") && !is_green("main") && !is_green("greenish"));
    }
}
