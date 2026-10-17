//! Postcard decoders of persisted objects. A decode may fail but never
//! panics; whatever decodes must re-encode to something that decodes to the
//! same value, with the same hash for content-addressed objects.

use leep_core::review::{Review, Verdict};
use leep_core::signature::Signature;
use leep_core::{Changeset, Hash, TreeNode};
use leep_derive::derived::{flat_row, HistoryNode, StoredHistoryNode};

pub fn changeset(data: &[u8]) {
    let Ok(cs) = Changeset::decode(data) else {
        return;
    };
    let enc = cs.encode();
    let again = Changeset::decode(&enc).expect("re-encoded changeset must decode");
    assert_eq!(again, cs);
    assert_eq!(again.encode(), enc, "encoding is canonical");
    assert_eq!(again.id(), cs.id());
    assert_eq!(cs.id().0, Hash::of_object(Changeset::KIND, &enc));
    // The same type is the body of `POST /v1/changesets`.
    let json = serde_json::to_vec(&cs).expect("changeset serializes");
    let back: Changeset = serde_json::from_slice(&json).expect("own JSON must parse");
    assert_eq!(back, cs);
}

pub fn tree_node(data: &[u8]) {
    let Ok(node) = TreeNode::decode(data) else {
        return;
    };
    let enc = node.encode();
    let again = TreeNode::decode(&enc).expect("re-encoded tree must decode");
    assert_eq!(again, node);
    assert_eq!(again.encode(), enc);
    assert_eq!(again.hash(), node.hash());
    assert_eq!(node.hash(), Hash::of_object(TreeNode::KIND, &enc));
}

/// A stored `history-node-v3` row: a whole node or chunk, a chunk list, or a patch of either.
pub fn history_node(data: &[u8]) {
    let Ok(stored) = StoredHistoryNode::decode(data) else {
        return;
    };
    let enc = stored.encode();
    let again = StoredHistoryNode::decode(&enc).expect("re-encoded history row must decode");
    assert_eq!(again, stored);
    assert_eq!(again.encode(), enc);
    // Applying a patch to any keyframe fails or gives a keyframe.
    for key in [
        StoredHistoryNode::Flat(HistoryNode::default()),
        StoredHistoryNode::Split(Vec::new()),
    ] {
        if let Ok(r) = stored.clone().resolve(Some(&key)) {
            assert!(r.base().is_none());
        }
    }
    let StoredHistoryNode::Flat(node) = stored else {
        return;
    };
    // Stored as an edit of itself without its last entry, the row must read
    // back as the node.
    let mut key = node.clone();
    if key.entries.pop_last().is_some() {
        let row = flat_row(&node, Some((key.hash(), &key)));
        let back = match row.node.base() {
            Some(_) => row
                .node
                .clone()
                .resolve(Some(&StoredHistoryNode::Flat(key))),
            None => Ok(row.node.clone()),
        };
        assert_eq!(back, Ok(StoredHistoryNode::Flat(node.clone())));
    }
    // Storing the node again must give rows that hold the same entries.
    let rows = node.rows();
    let (top, chunks) = rows.split_last().expect("a node has a top row");
    assert_eq!(top.hash, node.hash());
    let joined = match &top.node {
        StoredHistoryNode::Flat(n) => {
            assert!(chunks.is_empty());
            n.clone()
        }
        StoredHistoryNode::Split(refs) => {
            assert_eq!(refs.len(), chunks.len());
            let mut joined = HistoryNode::default();
            for c in chunks {
                let StoredHistoryNode::Flat(n) = &c.node else {
                    panic!("chunks are stored whole");
                };
                joined.entries.extend(n.entries.clone());
            }
            joined
        }
        _ => unreachable!("rows() writes keyframes only"),
    };
    assert_eq!(joined, node);
    let enc = node.encode();
    let again = HistoryNode::decode(&enc).expect("re-encoded history node must decode");
    assert_eq!(again, node);
    assert_eq!(again.encode(), enc);
    assert_eq!(again.hash(), node.hash());
    for entry in node.entries.values() {
        // The accessors must agree with the variant.
        assert_eq!(
            entry.node().is_some(),
            matches!(entry, leep_derive::derived::HistoryEntry::Tree { .. })
        );
        let _ = entry.changed();
    }
}

pub fn signature(data: &[u8]) {
    let Ok(sig) = Signature::decode(data) else {
        return;
    };
    let enc = sig.encode();
    assert_eq!(Signature::decode(&enc).expect("re-encoded signature"), sig);
    assert_eq!(Signature::decode(&enc).unwrap().encode(), enc);
}

pub fn review(data: &[u8]) {
    let Ok(r) = Review::decode(data) else {
        return;
    };
    let enc = r.encode();
    let again = Review::decode(&enc).expect("re-encoded review");
    assert_eq!(again, r);
    assert_eq!(again.encode(), enc);
    let json = serde_json::to_string(&r.verdict).expect("verdict serializes");
    assert_eq!(serde_json::from_str::<Verdict>(&json).unwrap(), r.verdict);
    assert_eq!(json, format!("\"{}\"", r.verdict.as_str()));
}
