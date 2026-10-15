//! Postcard decoders of persisted objects. A decode may fail but never
//! panics; whatever decodes must re-encode to something that decodes to the
//! same value, with the same hash for content-addressed objects.

use leep_core::review::{Review, Verdict};
use leep_core::signature::Signature;
use leep_core::{Changeset, Hash, TreeNode};
use leep_derive::derived::HistoryNode;

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

pub fn history_node(data: &[u8]) {
    let Ok(node) = HistoryNode::decode(data) else {
        return;
    };
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
