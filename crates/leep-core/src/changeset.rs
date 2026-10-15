use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::{Error, Hash, RepoPath, Result};

/// Identity of a changeset: the hash of its canonical encoding.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ChangesetId(pub Hash);

impl fmt::Display for ChangesetId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl fmt::Debug for ChangesetId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ChangesetId({})", &self.0.to_hex()[..12])
    }
}

impl FromStr for ChangesetId {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self> {
        Ok(ChangesetId(s.parse()?))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FileKind {
    Regular,
    Executable,
    /// Blob content is the link target.
    Symlink,
}

/// What a changeset does to one path, relative to its first parent.
/// Externally tagged (`"delete"` / `{"modify": {..}}`) so it also round-trips
/// through postcard, which cannot decode internally tagged enums.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FileChange {
    /// Create or replace the file at this path.
    Modify { content: Hash, kind: FileKind, size: u64 },
    Delete,
}

/// The unit of history. Unlike a git commit it does not point at a full tree:
/// it lists only the paths that changed relative to `parents[0]`. Full trees,
/// blame, history and so on are derived data computed from this.
///
/// A path and its ancestor directory can never both be files. A `Modify` that
/// turns a directory into a file (or vice versa) must also `Delete` the files
/// it replaces; derivation rejects changesets that leave the tree inconsistent.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Changeset {
    /// Empty for a root changeset. Merges have two or more; file changes are
    /// always relative to the first.
    pub parents: Vec<ChangesetId>,
    pub author: String,
    /// Unix seconds.
    pub timestamp: i64,
    pub message: String,
    pub changes: BTreeMap<RepoPath, FileChange>,
    /// Free-form metadata. Keys under `agent.` record how an AI agent produced
    /// the change (see `EXTRA_*` constants). Hashed like everything else.
    #[serde(default)]
    pub extra: BTreeMap<String, String>,
}

/// Conventional `extra` keys for agent provenance.
pub const EXTRA_AGENT_MODEL: &str = "agent.model";
pub const EXTRA_AGENT_SESSION: &str = "agent.session";
pub const EXTRA_AGENT_PROMPT: &str = "agent.prompt";
pub const EXTRA_AGENT_TESTS: &str = "agent.tests";
/// Set by the server when it rebases a changeset; value is the original id.
pub const EXTRA_REBASED_FROM: &str = "leep.rebased_from";

impl Changeset {
    pub const KIND: &'static str = "changeset-v1";

    /// Canonical binary encoding. `BTreeMap` ordering makes it deterministic.
    pub fn encode(&self) -> Vec<u8> {
        postcard::to_stdvec(self).expect("changeset encoding cannot fail")
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        Ok(postcard::from_bytes(bytes)?)
    }

    pub fn id(&self) -> ChangesetId {
        ChangesetId(Hash::of_object(Self::KIND, &self.encode()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Changeset {
        let mut changes = BTreeMap::new();
        changes.insert(
            RepoPath::new("a.txt").unwrap(),
            FileChange::Modify { content: Hash::of_blob(b"hi\n"), kind: FileKind::Regular, size: 3 },
        );
        changes.insert(RepoPath::new("old.txt").unwrap(), FileChange::Delete);
        Changeset {
            parents: vec![],
            author: "Ada <ada@example.com>".into(),
            timestamp: 1_760_000_000,
            message: "first".into(),
            changes,
            extra: BTreeMap::from([(EXTRA_AGENT_MODEL.into(), "test-model".into())]),
        }
    }

    #[test]
    fn id_is_stable_and_content_sensitive() {
        let a = sample();
        let b = Changeset::decode(&a.encode()).unwrap();
        assert_eq!(a, b);
        assert_eq!(a.id(), b.id());
        let mut c = a.clone();
        c.message.push('!');
        assert_ne!(a.id(), c.id());
    }

    #[test]
    fn json_shape() {
        let v = serde_json::to_value(sample()).unwrap();
        assert_eq!(v["changes"]["old.txt"], "delete");
        assert_eq!(v["changes"]["a.txt"]["modify"]["kind"], "regular");
    }
}
