use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::{FileKind, Hash, Result};

/// One directory of a full snapshot ("manifest"). Tree nodes are derived data:
/// they are computed from changesets, stored content-addressed in the
/// `BlobStore` under `TreeNode::hash`, and shared between snapshots.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TreeNode {
    /// Keyed by single path component.
    pub entries: BTreeMap<String, TreeEntry>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TreeEntry {
    File {
        content: Hash,
        kind: FileKind,
        size: u64,
    },
    Dir {
        tree: Hash,
    },
}

impl TreeNode {
    pub const KIND: &'static str = "tree-v1";

    pub fn encode(&self) -> Vec<u8> {
        postcard::to_stdvec(self).expect("tree encoding cannot fail")
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        Ok(postcard::from_bytes(bytes)?)
    }

    /// Address of this node. Note this is NOT `Hash::of_blob(encode())`, so a
    /// tree must be stored with `BlobStore::put_with_hash`.
    pub fn hash(&self) -> Hash {
        Hash::of_object(Self::KIND, &self.encode())
    }

    /// The empty tree, root of a repository with no files.
    pub fn empty_hash() -> Hash {
        TreeNode::default().hash()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let mut t = TreeNode::default();
        t.entries.insert(
            "a".into(),
            TreeEntry::Dir {
                tree: TreeNode::empty_hash(),
            },
        );
        t.entries.insert(
            "b".into(),
            TreeEntry::File {
                content: Hash::of_blob(b"b"),
                kind: FileKind::Executable,
                size: 1,
            },
        );
        assert_eq!(TreeNode::decode(&t.encode()).unwrap(), t);
        assert_ne!(t.hash(), TreeNode::empty_hash());
    }
}
