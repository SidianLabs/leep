//! Postcard decoders of persisted objects. A decode may fail but never
//! panics; whatever decodes must re-encode to something that decodes to the
//! same value, with the same hash for content-addressed objects.

use leep_core::review::{Review, Verdict};
use leep_core::signature::Signature;
use std::sync::Arc;

use leep_core::{Changeset, ChangesetId, Hash, TreeNode};
use leep_derive::derived::history_pack::{decode_part, decode_unchecked, PackBuilder, RawNode};
use leep_derive::derived::{flat_row, HistoryNode, NodeRef, Row, StoredHistoryNode};

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

/// A part of a `history-tree-v4` pack (ADR 0045), whatever its checksum
/// says: a decode may fail but never panics; a node resolves against a
/// keyframe or says why not; and a decoded keyframe, stored again in a pack
/// of its own, or as an edit of itself without its last entry, reads back
/// as the same node.
pub fn history_node(data: &[u8]) {
    let cs = ChangesetId(Hash([7; 32]));
    let other = ChangesetId(Hash([8; 32]));
    for part in [0, 1] {
        let Ok(decoded) = decode_unchecked(data, cs, part) else {
            continue;
        };
        for (node, _) in decoded.nodes {
            for key in [
                StoredHistoryNode::Flat(HistoryNode::default()),
                StoredHistoryNode::Split(Vec::new()),
            ] {
                if let Ok(named) = node.clone().resolve(Some(&key)) {
                    if let Ok(r) = named.resolve(Some(&key)) {
                        assert!(r.base().is_none());
                    }
                }
            }
            let RawNode::Ready(n) = node else {
                continue;
            };
            if n.base().is_some() {
                continue;
            }
            let mut b = PackBuilder::new(other);
            let at = b.stage(n.clone(), None);
            let pack = b.finish(at);
            let back = decode_part(&pack.value, other, 0).expect("own pack must decode");
            assert_eq!(back.nodes.len(), 1);
            assert_eq!(back.nodes[0].0, RawNode::Ready(n.clone()));
            let StoredHistoryNode::Flat(node) = n else {
                continue;
            };
            for entry in node.entries.values() {
                // The accessors must agree with the variant.
                assert_eq!(
                    entry.node().is_some(),
                    matches!(entry, leep_derive::derived::HistoryEntry::Tree { .. })
                );
            }
            let mut key = node.clone();
            if key.entries.pop_last().is_none() {
                continue;
            }
            let key = Arc::new(StoredHistoryNode::Flat(key));
            let base = NodeRef {
                cs,
                part: 0,
                idx: 0,
            };
            let (row, row_key) = match flat_row(other, &node, Some((base, &key))) {
                Row::New(row, k) => (row, k),
                Row::Keep(_) => panic!("a node with one more entry is not its keyframe"),
            };
            let mut b = PackBuilder::new(other);
            let at = b.stage(row, row_key);
            let pack = b.finish(at);
            let (raw, _) = decode_part(&pack.value, other, 0)
                .expect("own pack must decode")
                .nodes
                .remove(0);
            let named = raw.resolve(Some(&key)).expect("own patch must resolve");
            let whole = match named.base() {
                Some(_) => named.resolve(Some(&key)),
                None => Ok(named),
            };
            assert_eq!(whole, Ok(StoredHistoryNode::Flat(node.clone())));
        }
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
