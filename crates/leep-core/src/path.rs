use std::fmt;

use serde::{Deserialize, Deserializer, Serialize};

use crate::Error;

/// A normalized, repository-relative, `/`-separated path to a file.
///
/// Invariants: non-empty, no leading or trailing `/`, no empty, `.` or `..`
/// components, no NUL bytes. Ordering is plain byte order, which is also the
/// order entries appear in trees and changesets.
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize)]
#[serde(transparent)]
pub struct RepoPath(String);

impl RepoPath {
    pub fn new(s: impl Into<String>) -> Result<Self, Error> {
        let s = s.into();
        validate(&s)?;
        Ok(RepoPath(s))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn components(&self) -> impl Iterator<Item = &str> {
        self.0.split('/')
    }

    /// The final component (file name).
    pub fn file_name(&self) -> &str {
        self.0.rsplit('/').next().unwrap_or(&self.0)
    }

    /// The containing directory, or `None` for a top-level file.
    pub fn parent(&self) -> Option<&str> {
        self.0.rsplit_once('/').map(|(dir, _)| dir)
    }

    /// True if this path is `dir` itself or lies under it. An empty `dir`
    /// means the repository root and matches everything.
    pub fn is_within(&self, dir: &str) -> bool {
        dir.is_empty()
            || self.0 == dir
            || (self.0.starts_with(dir) && self.0.as_bytes().get(dir.len()) == Some(&b'/'))
    }
}

fn validate(s: &str) -> Result<(), Error> {
    let bad = |why| Err(Error::InvalidPath(s.to_string(), why));
    if s.is_empty() {
        return bad("empty");
    }
    if s.contains('\0') {
        return bad("contains NUL");
    }
    for c in s.split('/') {
        match c {
            "" => return bad("empty component or leading/trailing slash"),
            "." | ".." => return bad("relative component"),
            _ => {}
        }
    }
    Ok(())
}

impl<'de> Deserialize<'de> for RepoPath {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        RepoPath::new(String::deserialize(d)?).map_err(serde::de::Error::custom)
    }
}

impl fmt::Display for RepoPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl fmt::Debug for RepoPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

impl TryFrom<&str> for RepoPath {
    type Error = Error;
    fn try_from(s: &str) -> Result<Self, Error> {
        RepoPath::new(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validation() {
        for ok in ["a", "a/b.txt", "src/main.rs", ".gitignore"] {
            RepoPath::new(ok).unwrap();
        }
        for bad in ["", "/a", "a/", "a//b", "./a", "a/../b", "a\0b"] {
            assert!(RepoPath::new(bad).is_err(), "{bad:?} should be rejected");
        }
    }

    #[test]
    fn helpers() {
        let p = RepoPath::new("src/lib/mod.rs").unwrap();
        assert_eq!(p.file_name(), "mod.rs");
        assert_eq!(p.parent(), Some("src/lib"));
        assert!(p.is_within("src"));
        assert!(p.is_within(""));
        assert!(!p.is_within("sr"));
        assert!(!RepoPath::new("srcx/a").unwrap().is_within("src"));
    }
}
