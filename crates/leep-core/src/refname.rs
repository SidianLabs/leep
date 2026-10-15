//! Ref names: one validator for every ref write, and the branch/tag split.
//!
//! The rules follow git's, so names survive the git bridge and stay safe in
//! URLs, shells and file names. Branches are any valid name outside `tags/`;
//! tags live under `tags/` and are immutable (enforced by `leep-derive`).

use crate::error::{Error, Result};

/// Namespace of tags. A tag named `v1` is the ref `tags/v1`.
pub const TAGS_PREFIX: &str = "tags/";

/// Stable text in the error for a move of a protected ref that is not a
/// fast-forward; the CLI matches it to suggest a revert.
pub const NOT_FORWARD_MARKER: &str = "only moves forward";

/// Longest accepted ref name, in bytes.
pub const MAX_LEN: usize = 255;

/// Characters git forbids besides control characters and space.
const FORBIDDEN: &[char] = &['~', '^', ':', '?', '*', '[', '\\'];

/// Whether `name` is in the tag namespace.
pub fn is_tag(name: &str) -> bool {
    name.starts_with(TAGS_PREFIX)
}

/// The ref that holds tag `short`.
pub fn tag_ref(short: &str) -> String {
    format!("{TAGS_PREFIX}{short}")
}

/// Check a ref name; the error says which rule failed. `Error::Invalid`, so
/// the server answers 400.
pub fn validate(name: &str) -> Result<()> {
    let bad = |why: &str| Err(Error::Invalid(format!("invalid ref name {name:?}: {why}")));
    if name.is_empty() {
        return bad("empty");
    }
    if name.len() > MAX_LEN {
        return bad("longer than 255 bytes");
    }
    if let Some(c) = name.chars().find(|c| !c.is_ascii_graphic()) {
        return bad(&format!(
            "contains {c:?}; only printable ASCII without spaces"
        ));
    }
    if let Some(c) = name.chars().find(|c| FORBIDDEN.contains(c)) {
        return bad(&format!("contains {c:?}"));
    }
    if name.contains("..") {
        return bad("contains `..`");
    }
    if name.contains("@{") {
        return bad("contains `@{`");
    }
    if name.starts_with('/') || name.ends_with('/') {
        return bad("starts or ends with `/`");
    }
    if name.contains("//") {
        return bad("contains `//`");
    }
    for part in name.split('/') {
        if part.starts_with('.') || part.starts_with('-') {
            return bad("a component starts with `.` or `-`");
        }
        if part.ends_with(".lock") {
            return bad("a component ends with `.lock`");
        }
    }
    // `@` means the working copy base, and a full id would shadow the
    // changeset it spells when revisions are resolved.
    if name == "@" || (name.len() == 64 && name.bytes().all(|b| b.is_ascii_hexdigit())) {
        return bad("ambiguous with a revision (`@` or a 64-digit hex id)");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_ordinary_names() {
        for n in [
            "main",
            "release/1.0",
            "team/x/feature-1",
            "tags/v1.2.3",
            "a",
            "under_score",
            "x@y",
            "a.b",
            "UPPER",
        ] {
            validate(n).unwrap_or_else(|e| panic!("{n}: {e}"));
        }
        validate(&"a".repeat(MAX_LEN)).unwrap();
    }

    #[test]
    fn rejects_bad_names() {
        let long = "a".repeat(MAX_LEN + 1);
        let hex = "ab".repeat(32);
        for n in [
            "",
            long.as_str(),
            "has space",
            "tab\t",
            "nul\0",
            "caf\u{e9}",
            "del\x7f",
            "a~b",
            "a^b",
            "a:b",
            "a?b",
            "a*b",
            "a[b",
            "a\\b",
            "a..b",
            "..",
            "a@{b",
            "/lead",
            "trail/",
            "a//b",
            ".hidden",
            "a/.hidden",
            "-dash",
            "a/-dash",
            "x.lock",
            "a.lock/b",
            "@",
            hex.as_str(),
        ] {
            let e = validate(n).unwrap_err();
            assert!(matches!(e, Error::Invalid(_)), "{n:?}: {e}");
        }
    }

    #[test]
    fn tag_namespace() {
        assert!(is_tag("tags/v1"));
        assert!(!is_tag("tagsy"));
        assert!(!is_tag("main"));
        assert_eq!(tag_ref("v1"), "tags/v1");
    }
}
