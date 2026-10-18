use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use leep_core::memory::{MemoryBlobStore, MemoryMetaStore};
use leep_core::{ChangesetId, Hash, MetaStore};

use super::*;
use crate::derived::history_pack;
use crate::derived::history_pack::{
    canonical, cost, decode_part, decode_root, decode_unchecked, part_key, PackBuilder, RawNode,
    PART_BYTES,
};

fn id(b: u8) -> ChangesetId {
    ChangesetId(Hash([b; 32]))
}

/// A distinct changeset id for each `n`, standing in for the changesets
/// whose derivations write the packs of these tests.
fn writer(n: u32) -> ChangesetId {
    ChangesetId(Hash::of_object("test-writer", &n.to_le_bytes()))
}

fn leaf(b: u8) -> HistoryEntry {
    HistoryEntry::Leaf {
        changed: vec![id(b)],
    }
}

fn wide(n: usize) -> HistoryNode {
    HistoryNode {
        entries: (0..n).map(|i| (format!("f{i:04}"), leaf(1))).collect(),
    }
}

fn mem() -> (Arc<MemoryMetaStore>, Repo) {
    let meta = Arc::new(MemoryMetaStore::new());
    let repo = Repo::new(Arc::new(MemoryBlobStore::new()), meta.clone());
    (meta, repo)
}

/// A repo on the same rows with empty caches, as another server would read.
fn cold(meta: &Arc<MemoryMetaStore>) -> Repo {
    Repo::new(Arc::new(MemoryBlobStore::new()), meta.clone())
}

/// Finish and store `pending` with `root` as the root of its writer, as a
/// derivation would, and return the root's address.
async fn commit_pack(repo: &Repo, pending: PackBuilder, root: NodeRef) -> NodeRef {
    let w = pending.cs;
    let pack = pending.finish(root);
    let at = decode_root(w, &pack.value).unwrap();
    let value = repo.write_history_pack(pack).await.unwrap();
    repo.meta()
        .put_derived(HistoryTree::NAME, &w.0, value)
        .await
        .unwrap();
    at
}

fn checksum_of(cs: ChangesetId, part: u32, rest: &[u8]) -> Vec<u8> {
    let mut b = cs.0 .0.to_vec();
    b.extend_from_slice(&part.to_le_bytes());
    b.extend_from_slice(rest);
    Hash::of_object(NODE_KIND, &b).0[..8].to_vec()
}

/// Golden bytes: changing them orphans every stored pack. A new encoding
/// needs new kind names (`history-tree-v5`, `history-node-v5`).
#[test]
fn pack_encoding_is_stable() {
    // A pack of one whole node: `a` changed by the writer itself, `ab` a
    // directory last changed by changeset 1, whose node is node 3 of part 2
    // of changeset 1's pack.
    let w = id(9);
    let node = HistoryNode {
        entries: BTreeMap::from([
            ("a".to_string(), HistoryEntry::Leaf { changed: vec![w] }),
            (
                "ab".to_string(),
                HistoryEntry::Tree {
                    changed: vec![id(1)],
                    node: NodeRef {
                        cs: id(1),
                        part: 2,
                        idx: 3,
                    },
                },
            ),
        ]),
    };
    let mut b = PackBuilder::new(w);
    let at = b.stage(StoredHistoryNode::Flat(node.clone()), None);
    let pack = b.finish(at);
    let mut rest = vec![1]; // one id in the table
    rest.extend([1; 32]);
    rest.extend([0, 0, 0]); // root: this pack (0), part 0 + 0, node 0
    rest.extend([1, 15]); // one node, of 15 bytes
    rest.extend([0, 2]); // Flat, two entries
    rest.extend([0, 1, b'a', 2, 0]); // no prefix, "a"; one id, leaf; the writer
    rest.extend([1, 1, b'b', 3, 1]); // prefix "a" + "b"; one id, tree; table id 1
    rest.extend([1, 2, 3]); // its node: changeset 1, part 2, node 3
    let mut want = checksum_of(w, 0, &rest);
    want.push(0); // stored uncompressed
    want.extend(&rest);
    assert_eq!(pack.value.to_vec(), want);
    assert!(pack.rows.is_empty());
    let back = decode_part(&pack.value, w, 0).unwrap();
    assert_eq!(
        back.root,
        Some(NodeRef {
            cs: w,
            part: 0,
            idx: 0
        })
    );
    assert_eq!(back.table, vec![id(1)]);
    assert_eq!(
        back.nodes[0].0,
        RawNode::Ready(StoredHistoryNode::Flat(node.clone()))
    );

    // A patch names the keyframe entries it replaces by position (the step
    // from the previous one, from -1), and new names as 0 then the name.
    let key = HistoryNode {
        entries: ["a", "b", "c"]
            .iter()
            .map(|n| (n.to_string(), leaf(3)))
            .collect(),
    };
    let base = NodeRef {
        cs: id(5),
        part: 0,
        idx: 4,
    };
    let w2 = id(8);
    let over = HistoryNode {
        entries: BTreeMap::from([
            ("b".to_string(), HistoryEntry::Leaf { changed: vec![w2] }),
            ("d".to_string(), leaf(2)),
        ]),
    };
    let mut b = PackBuilder::new(w2);
    let at = b.stage(
        StoredHistoryNode::Patch {
            base,
            over: over.clone(),
        },
        Some(Arc::new(StoredHistoryNode::Flat(key.clone()))),
    );
    let pack = b.finish(at);
    let mut rest = vec![2];
    rest.extend([5; 32]);
    rest.extend([2; 32]);
    rest.extend([0, 0, 0, 1, 14]); // root, one node of 14 bytes
    rest.extend([2, 1, 0, 4, 2]); // Patch, base (table id 1, part 0, node 4), two entries
    rest.extend([2, 2, 0]); // "b" is the keyframe's entry 1; the writer
    rest.extend([0, 0, 1, b'd', 2, 2]); // "d" is new; table id 2
    assert_eq!(canonical(&pack.value).unwrap().to_vec(), rest);
    assert_eq!(pack.value[..8], checksum_of(w2, 0, &rest)[..]);
    let back = decode_part(&pack.value, w2, 0).unwrap();
    let resolved = back.nodes[0]
        .0
        .clone()
        .resolve(Some(&StoredHistoryNode::Flat(key.clone())))
        .unwrap();
    assert_eq!(resolved, StoredHistoryNode::Patch { base, over });

    // A list patch: chunk 1 of the keyframe list set, a new chunk "z" set,
    // chunk 0 removed; successors are not stored.
    let list: Vec<ChunkRef> = ["a", "m"]
        .iter()
        .map(|f| ChunkRef {
            first: f.to_string(),
            node: base,
        })
        .collect();
    let mut refs = BTreeMap::new();
    refs.insert("a".to_string(), None);
    for f in ["m", "z"] {
        refs.insert(
            f.to_string(),
            Some(SplitChange {
                node: NodeRef {
                    cs: id(5),
                    part: 1,
                    idx: 0,
                },
                next: None,
            }),
        );
    }
    let mut b = PackBuilder::new(w2);
    let at = b.stage(
        StoredHistoryNode::SplitPatch { base, refs },
        Some(Arc::new(StoredHistoryNode::Split(list.clone()))),
    );
    let pack = b.finish(at);
    let mut rest = vec![1];
    rest.extend([5; 32]);
    rest.extend([0, 0, 0, 1, 20]);
    rest.extend([3, 1, 0, 4, 3]); // SplitPatch, base, three changes
    rest.extend([1, 0]); // keyframe chunk 0, removed
    rest.extend([1, 1, 1, 1, 0]); // keyframe chunk 1, set to (table id 1, part 1, node 0)
    rest.extend([0, 0, 1, b'z', 1, 1, 1, 0]); // new chunk "z", set
    assert_eq!(canonical(&pack.value).unwrap().to_vec(), rest);
    let back = decode_part(&pack.value, w2, 0).unwrap();
    let StoredHistoryNode::SplitPatch { refs, .. } = back.nodes[0]
        .0
        .clone()
        .resolve(Some(&StoredHistoryNode::Split(list)))
        .unwrap()
    else {
        panic!("a list patch")
    };
    let next: Vec<Option<String>> = refs
        .values()
        .map(|c| c.as_ref().and_then(|c| c.next.clone()))
        .collect();
    assert_eq!(next, [None, Some("z".to_string()), None]);

    // Parts after the first are keyed by writer and number.
    let mut k = id(4).0 .0.to_vec();
    k.extend(7u32.to_le_bytes());
    assert_eq!(part_key(id(4), 7), Hash::of_object("history-node-v4", &k));

    assert_eq!(HistoryTree::NAME, "history-tree-v4");
    assert_eq!(NODE_KIND, "history-node-v4");
    assert_eq!(PATCH_BUDGET, 96);
    assert_eq!(PART_BYTES, 16384);
    assert!(worth_patching(400, 1667) && !worth_patching(401, 1667));
}

/// Large parts are stored as zstd frames; the checksum covers the bytes
/// before compression, and two encodings of the same part compare equal.
#[test]
fn large_parts_are_compressed_and_compare_by_content() {
    let w = writer(1);
    let mut b = PackBuilder::new(w);
    let at = b.stage(StoredHistoryNode::Flat(wide(60)), None);
    let pack = b.finish(at);
    assert_eq!(pack.value[8], 1, "a compressible part is compressed");
    let rest = canonical(&pack.value).unwrap().to_vec();
    assert_eq!(pack.value[..8], checksum_of(w, 0, &rest)[..]);
    assert!(pack.value.len() < rest.len());
    let mut raw = pack.value[..8].to_vec();
    raw.push(0);
    raw.extend(&rest);
    assert!(history_pack::same_part(&raw, &pack.value));
    assert_eq!(
        decode_part(&raw, w, 0).unwrap().nodes,
        decode_part(&pack.value, w, 0).unwrap().nodes
    );
    let mut other = raw.clone();
    *other.last_mut().unwrap() ^= 1;
    assert!(!history_pack::same_part(&other, &pack.value));
}

/// When a patch is chosen over a keyframe is part of the format: it decides
/// which nodes, and so which bytes, an edit produces.
#[test]
fn patches_are_chosen_while_small_and_name_their_keyframe() {
    let w = writer(7);
    let key = wide(40);
    let key_ref = NodeRef {
        cs: writer(1),
        part: 0,
        idx: 0,
    };
    let key_row = Arc::new(StoredHistoryNode::Flat(key.clone()));
    // Each changed entry names another changeset, so the patch grows by an
    // id table entry per change, and a keyframe wins after a few.
    let mut node = key.clone();
    let mut kinds = Vec::new();
    for i in 0..10u8 {
        node.entries.insert(format!("f{:04}", i * 3), leaf(10 + i));
        let row = flat_row(w, &node, Some((key_ref, &key_row)));
        let over = HistoryNode {
            entries: node
                .entries
                .iter()
                .filter(|(n, e)| key.entries.get(*n) != Some(e))
                .map(|(n, e)| (n.clone(), e.clone()))
                .collect(),
        };
        let patch = StoredHistoryNode::Patch {
            base: key_ref,
            over,
        };
        let want = worth_patching(
            cost(w, &patch, Some(&key_row)),
            cost(w, &StoredHistoryNode::Flat(node.clone()), None),
        );
        match row {
            Row::New(StoredHistoryNode::Patch { base, over }, Some(k)) => {
                assert!(want, "{i}");
                assert_eq!(base, key_ref);
                assert_eq!(over.entries.len(), i as usize + 1);
                assert_eq!(*k, *key_row);
                let resolved = StoredHistoryNode::Patch { base, over }.resolve(Some(&key_row));
                assert_eq!(resolved, Ok(StoredHistoryNode::Flat(node.clone())));
                kinds.push('p');
            }
            Row::New(StoredHistoryNode::Flat(n), None) => {
                assert!(!want, "{i}");
                assert_eq!(n, node);
                kinds.push('k');
            }
            _ => panic!("a whole node is stored as a patch or a keyframe"),
        }
    }
    assert!(
        kinds.starts_with(&['p', 'p']) && kinds.ends_with(&['k']),
        "{kinds:?}"
    );

    // Unchanged: the keyframe itself. A name of the keyframe missing from
    // the node cannot be expressed as a patch.
    assert!(matches!(
        flat_row(w, &key, Some((key_ref, &key_row))),
        Row::Keep(r) if r == key_ref
    ));
    let mut fewer = key.clone();
    fewer.entries.remove("f0001");
    assert!(matches!(
        flat_row(w, &fewer, Some((key_ref, &key_row))),
        Row::New(StoredHistoryNode::Flat(_), None)
    ));

    // Lists: a changed ref and a removed one.
    let chunk = |i: u32| NodeRef {
        cs: writer(2),
        part: 0,
        idx: i,
    };
    let refs: Vec<ChunkRef> = (0..40)
        .map(|i| ChunkRef {
            first: format!("n{i:02}"),
            node: chunk(i),
        })
        .collect();
    let list = Arc::new(StoredHistoryNode::Split(refs.clone()));
    let list_ref = NodeRef {
        cs: writer(3),
        part: 0,
        idx: 9,
    };
    let mut edited = refs.clone();
    edited[3].node = chunk(99);
    edited.remove(7);
    let Row::New(patch, Some(_)) = split_row(w, edited.clone(), Some((list_ref, &list))) else {
        panic!("a small list edit is a patch")
    };
    let want = StoredHistoryNode::SplitPatch {
        base: list_ref,
        refs: BTreeMap::from([
            (
                "n03".to_string(),
                Some(SplitChange {
                    node: chunk(99),
                    next: Some("n04".to_string()),
                }),
            ),
            ("n07".to_string(), None),
        ]),
    };
    assert_eq!(patch, want);
    assert_eq!(
        want.resolve(Some(&list)),
        Ok(StoredHistoryNode::Split(edited))
    );
    assert!(matches!(
        split_row(w, refs, Some((list_ref, &list))),
        Row::Keep(r) if r == list_ref
    ));
}

#[test]
fn patches_only_resolve_against_a_keyframe_of_their_shape() {
    let r = |i| NodeRef {
        cs: id(1),
        part: 0,
        idx: i,
    };
    let flat = StoredHistoryNode::Flat(wide(3));
    let split = StoredHistoryNode::Split(vec![ChunkRef {
        first: "a".into(),
        node: r(1),
    }]);
    let patch = StoredHistoryNode::Patch {
        base: r(2),
        over: wide(1),
    };
    let split_patch = StoredHistoryNode::SplitPatch {
        base: r(3),
        refs: BTreeMap::from([("a".to_string(), None)]),
    };
    assert!(patch.clone().resolve(None).is_err());
    assert!(patch.clone().resolve(Some(&split)).is_err());
    assert!(patch.clone().resolve(Some(&patch)).is_err());
    assert!(split_patch.clone().resolve(Some(&flat)).is_err());
    assert!(
        split_patch.clone().resolve(Some(&split)).is_err(),
        "removing the only chunk leaves none"
    );
    assert!(flat.clone().resolve(Some(&flat)).is_err());
    assert_eq!(flat.clone().resolve(None), Ok(flat.clone()));
    // Positions past the keyframe's end are damage, not a panic.
    let raw = RawNode::Patch {
        base: r(2),
        over: vec![(history_pack::Slot::Pos(3), leaf(1))],
    };
    assert!(raw.clone().resolve(Some(&flat)).is_err());
    assert!(raw.resolve(Some(&split)).is_err());
}

/// Chunk boundaries are part of the format: a different choice gives the
/// same logical node other nodes.
#[test]
fn chunk_boundaries_are_stable() {
    let ends: Vec<usize> = (0..1000)
        .filter(|i| ends_chunk(&format!("f{i:04}")))
        .take(6)
        .collect();
    assert_eq!(ends, [29, 40, 76, 142, 152, 159]);
    assert_eq!(wide(1000).chunks().unwrap().len(), 33);
}

#[test]
fn small_nodes_are_flat_and_large_ones_split_within_bounds() {
    assert!(wide(FLAT_MAX).chunks().is_none());
    for n in [FLAT_MAX + 1, 300, 1000, 5000] {
        let node = wide(n);
        let chunks = node.chunks().expect("split");
        let mut joined = HistoryNode::default();
        for chunk in &chunks {
            assert!(!chunk.entries.is_empty() && chunk.entries.len() <= CHUNK_MAX);
            joined.entries.extend(chunk.entries.clone());
        }
        assert_eq!(joined, node);
    }
    // A long run of names without a boundary is cut at the cap.
    let names: Vec<String> = (0..)
        .map(|i| format!("n{i}"))
        .filter(|n| !ends_chunk(n))
        .take(3 * CHUNK_MAX)
        .collect();
    let node = HistoryNode {
        entries: names.into_iter().map(|n| (n, leaf(1))).collect(),
    };
    assert_eq!(node.chunks().unwrap().len(), 3);
}

/// Inserting a name replaces at most two chunks (the one it lands in, cut
/// in two if the name is a boundary); every other chunk is shared.
#[test]
fn an_insert_changes_at_most_two_chunks() {
    let base = wide(1000);
    let before: Vec<HistoryNode> = base.chunks().unwrap();
    for extra in ["f0000a", "f0500x", "f0999z", "a", "zzz", "f0123_"] {
        let mut node = base.clone();
        node.entries.insert(extra.to_string(), leaf(2));
        let new = node
            .chunks()
            .unwrap()
            .into_iter()
            .filter(|c| !before.contains(c))
            .count();
        assert!((1..=2).contains(&new), "{extra}: {new} new chunks");
    }
}

/// Edits of wide nodes, written chunk by chunk, store what a full rebuild
/// of the joined node does (`stage_history_edit` asserts that in tests) and
/// read back as the edited node: replaced and inserted names, names before
/// the first and after the last, boundary names, and long runs without a
/// boundary, which the 128-entry cap cuts.
#[tokio::test]
async fn wide_edits_match_full_rebuilds() {
    let (meta, repo) = mem();
    let run: Vec<String> = (0..)
        .map(|i| format!("r{i:05}"))
        .filter(|n| !ends_chunk(n))
        .take(3 * CHUNK_MAX)
        .collect();
    let boundaries: Vec<String> = (0..)
        .map(|i| format!("m{i:05}"))
        .filter(|n| ends_chunk(n))
        .take(40)
        .collect();
    let mut node = wide(2000);
    for n in &run {
        node.entries.insert(n.clone(), leaf(1));
    }
    let mut h = repo.put_history_node(writer(0), &node, None).await.unwrap();
    let mut seed = 7u64;
    let mut next = |n: usize| {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        (seed >> 33) as usize % n
    };
    for step in 0..120u32 {
        let mut updates = BTreeMap::new();
        for _ in 0..1 + next(6) {
            let name = match next(7) {
                0 => format!("f{:04}", next(2000)),
                1 => format!("f{:04}x{step}", next(2000)),
                2 => format!("a{step:03}"),
                3 => format!("z{step:03}"),
                4 => boundaries[next(boundaries.len())].clone(),
                5 => run[next(run.len())].clone(),
                _ => format!("{}y", run[next(run.len())]),
            };
            updates.insert(name, leaf((step % 250) as u8));
        }
        node.entries.extend(updates.clone());
        let mut pending = PackBuilder::new(writer(step + 1));
        let root = repo
            .stage_history_edit(Some(h), updates.into_iter().collect(), &mut pending)
            .await
            .unwrap();
        h = commit_pack(&repo, pending, root).await;
        if step % 10 == 0 {
            assert_eq!(
                cold(&meta).read_history_node(h).await.unwrap(),
                node,
                "step {step}"
            );
        }
    }
    // A small node edited past the whole-node limit, and from nothing.
    let small = wide(60);
    let hs = repo
        .put_history_node(writer(500), &small, None)
        .await
        .unwrap();
    let more: Vec<(String, HistoryEntry)> = (0..10).map(|i| (format!("g{i}"), leaf(3))).collect();
    let mut pending = PackBuilder::new(writer(501));
    let root = repo
        .stage_history_edit(Some(hs), more.clone(), &mut pending)
        .await
        .unwrap();
    let h2 = commit_pack(&repo, pending, root).await;
    let mut want = small.clone();
    want.entries.extend(more.clone());
    assert_eq!(repo.read_history_node(h2).await.unwrap(), want);
    // An edit that changes nothing is the base itself, and writes nothing.
    let mut pending = PackBuilder::new(writer(502));
    let same = repo
        .stage_history_edit(Some(h2), more.clone(), &mut pending)
        .await
        .unwrap();
    assert_eq!(same, h2);
    assert!(pending.is_empty());
    let mut pending = PackBuilder::new(writer(503));
    let same = repo
        .stage_history_edit(
            Some(h),
            vec![("f0001".into(), node.entries["f0001"].clone())],
            &mut pending,
        )
        .await
        .unwrap();
    assert_eq!(same, h);
    assert!(pending.is_empty());
}

/// Path history of a merge-heavy history with wide directories, from the
/// benches' generator: the index answers path logs as scanning every
/// ancestor does, and the last merges derived a window at a time are the
/// ones derived one by one.
#[tokio::test]
async fn a_wide_merge_heavy_history_derives_correctly() {
    use crate::derived::{derive, LastMerge};
    use crate::wide_history::{build, Spec};
    use crate::LogFilter;
    let meta = Arc::new(MemoryMetaStore::new());
    let blobs = Arc::new(MemoryBlobStore::new());
    let mut spec = Spec::small(400);
    spec.wide_min = 300;
    spec.wide_step = 150;
    let head = build(&Repo::new(blobs.clone(), meta.clone()), &spec).await;
    let repo = Repo::new(blobs.clone(), meta.clone());
    repo.history_root(head).await.unwrap();
    let mut paths = vec!["wide".to_string(), "deep".to_string()];
    for i in 0..spec.wide_dirs {
        paths.push(format!("wide/w{i:02}"));
        paths.push(format!("wide/w{i:02}/f00010.c"));
        paths.push(format!("wide/w{i:02}/f00200.c"));
    }
    paths.push("deep/s03/d02/g007.c".into());
    for path in &paths {
        let indexed = repo.log(head, Some(path), 50).await.unwrap();
        let scanned = repo
            .log_scan(head, Some(path), 50, &LogFilter::default())
            .await
            .unwrap();
        let ids = |l: &[leep_core::api::ChangesetInfo]| -> Vec<ChangesetId> {
            l.iter().map(|i| i.id).collect()
        };
        assert_eq!(ids(&indexed), ids(&scanned), "{path}");
    }
    let windowed = meta
        .list_derived(LastMerge::NAME, None, usize::MAX)
        .await
        .unwrap();
    assert!(windowed.len() > 300);
    meta.delete_derived_kind(LastMerge::NAME).await.unwrap();
    let fresh = Repo::new(blobs, meta.clone());
    derive::<LastMerge>(&fresh, head).await.unwrap();
    let single = meta
        .list_derived(LastMerge::NAME, None, usize::MAX)
        .await
        .unwrap();
    let single: BTreeMap<Hash, Bytes> = single.into_iter().collect();
    for (k, v) in &windowed {
        if let Some(s) = single.get(k) {
            assert_eq!(s, v, "last merge of {k}");
        }
    }
}

/// A merge of a small branch forked long before the tip, with a change
/// older than the fork still among the first parent's newest (the shape of
/// Linux's `work.thaw` merge): the index answers as the scan does, and the
/// merge base is the only common ancestor the branch side has.
#[tokio::test]
async fn an_old_based_merge_derives_correctly() {
    use crate::wide_history::{build, Spec};
    use crate::LogFilter;
    let (_, repo) = mem();
    let mut spec = Spec::small(500).with_old_branch(150, 2);
    spec.wide_min = 200;
    spec.wide_step = 100;
    let head = build(&repo, &spec).await;
    let cs = repo.get_changeset(head).await.unwrap();
    let [p1, p2] = cs.parents[..] else {
        panic!("the head is a two-parent merge")
    };
    let m = repo.merge_base(p1, p2).await.unwrap().unwrap();
    let gm = repo.generation(m).await.unwrap();
    assert_eq!(repo.generation(p2).await.unwrap(), gm + 2);
    assert!(repo.side_is_clear(p2, (gm, m), 10).await.unwrap());
    for path in ["cherry", "cherry/pick.c", "cherry/new.c", "wide/w00"] {
        let indexed = repo.log(head, Some(path), 50).await.unwrap();
        let scanned = repo
            .log_scan(head, Some(path), 50, &LogFilter::default())
            .await
            .unwrap();
        let ids = |l: &[leep_core::api::ChangesetInfo]| -> Vec<ChangesetId> {
            l.iter().map(|i| i.id).collect()
        };
        assert_eq!(ids(&indexed), ids(&scanned), "{path}");
    }
    // Both copies of the twice-applied change stay newest behind the first
    // parent.
    let at_p1 = repo.log(p1, Some("cherry/pick.c"), 10).await.unwrap();
    assert_eq!(at_p1.len(), 2);
}

/// Every entry of the index, for every path that ever existed and every
/// changeset of a merge-heavy history with wide directories, is what the
/// definition says: the maximal changesets, among the changeset and its
/// ancestors, whose entry for the path differs from their first parent's.
/// This is the meaning `history-tree-v3` stored too, so `log` and
/// `last-change` answers do not change with the encoding.
#[tokio::test]
async fn every_entry_is_the_maximal_set_of_changes() {
    use crate::wide_history::{build, Spec};
    let (_, repo) = mem();
    let spec = Spec {
        wide_dirs: 2,
        wide_min: 66,
        wide_step: 30,
        deep: (2, 2, 5),
        commits: 160,
        branches: 3,
        seed: 0xfeed,
        old_branch: Some((60, 2)),
    };
    let head = build(&repo, &spec).await;
    // Every changeset, parents first, with its parents.
    let mut order: Vec<ChangesetId> = Vec::new();
    let mut parents: HashMap<ChangesetId, Vec<ChangesetId>> = HashMap::new();
    let mut stack = vec![(head, false)];
    while let Some((c, done)) = stack.pop() {
        if done {
            order.push(c);
            continue;
        }
        if parents.contains_key(&c) {
            continue;
        }
        let ps = repo.get_changeset(c).await.unwrap().parents;
        parents.insert(c, ps.clone());
        stack.push((c, true));
        stack.extend(ps.into_iter().map(|p| (p, false)));
    }
    let mut dedup = HashSet::new();
    order.retain(|c| dedup.insert(*c));
    let index: HashMap<ChangesetId, usize> =
        order.iter().enumerate().map(|(i, c)| (*c, i)).collect();
    // Every path's entry (file content or directory tree) in each changeset.
    let mut entries: Vec<HashMap<String, TreeEntry>> = Vec::new();
    for c in &order {
        let mut all = HashMap::new();
        let mut dirs = vec![(String::new(), repo.root_tree(*c).await.unwrap())];
        while let Some((dir, tree)) = dirs.pop() {
            for (name, e) in repo.read_tree(tree).await.unwrap().entries {
                let path = join(&dir, &name);
                if let TreeEntry::Dir { tree } = &e {
                    dirs.push((path.clone(), *tree));
                }
                all.insert(path, e);
            }
        }
        entries.push(all);
    }
    let paths: BTreeSet<String> = entries.iter().flat_map(|e| e.keys().cloned()).collect();
    // Ancestors of each changeset, itself included.
    let mut ancestors: Vec<HashSet<usize>> = Vec::new();
    for (i, c) in order.iter().enumerate() {
        let mut a = HashSet::from([i]);
        for p in &parents[c] {
            a.extend(ancestors[index[p]].iter().copied());
        }
        ancestors.push(a);
    }
    let changed_by = |i: usize, p: &str| -> bool {
        let before = parents[&order[i]]
            .first()
            .and_then(|fp| entries[index[fp]].get(p));
        entries[i].get(p) != before
    };
    let mut checked = 0;
    for (x, cid) in order.iter().enumerate().step_by(3) {
        for p in &paths {
            let changes: Vec<usize> = ancestors[x]
                .iter()
                .copied()
                .filter(|&i| changed_by(i, p))
                .collect();
            let mut want: Vec<ChangesetId> = changes
                .iter()
                .filter(|&&i| !changes.iter().any(|&j| j != i && ancestors[j].contains(&i)))
                .map(|&i| order[i])
                .collect();
            want.sort();
            let got = repo.history_entry(*cid, p).await.unwrap();
            match got {
                None => assert!(want.is_empty(), "{p} at {cid}"),
                Some(e) => assert_eq!(e.changed(), want.as_slice(), "{p} at {cid}"),
            }
            checked += 1;
        }
    }
    assert!(checked > 5000, "{checked}");
}

#[tokio::test]
async fn a_split_node_round_trips_and_an_edit_writes_one_chunk() {
    let (meta, repo) = mem();
    let node = wide(1000);
    let mut pending = PackBuilder::new(writer(1));
    let h = repo
        .stage_history_node(&node, None, &mut pending)
        .await
        .unwrap();
    assert_eq!(pending.len(), 34, "33 chunks and the list");
    let h = commit_pack(&repo, pending, h).await;
    assert_eq!(repo.read_history_node(h).await.unwrap(), node);

    let mut edited = node.clone();
    edited.entries.insert("f0500".into(), leaf(9));
    let mut pending = PackBuilder::new(writer(2));
    let h2 = repo
        .stage_history_edit(Some(h), vec![("f0500".into(), leaf(9))], &mut pending)
        .await
        .unwrap();
    assert_eq!(pending.len(), 2, "one chunk and the list");
    let h2 = commit_pack(&repo, pending, h2).await;
    // Read back without the cache, as another server would.
    let c = cold(&meta);
    assert_eq!(c.read_history_node(h2).await.unwrap(), edited);
    assert_eq!(c.read_history_node(h).await.unwrap(), node);
    for name in ["f0000", "f0500", "f0999", "f0501"] {
        assert_eq!(
            c.history_child(h2, name).await.unwrap(),
            edited.entries.get(name).cloned()
        );
    }
    for name in ["", "a", "f05", "zzz"] {
        assert_eq!(c.history_child(h2, name).await.unwrap(), None);
    }
    // Both new nodes are patches on the keyframes they replace.
    let StoredHistoryNode::SplitPatch { base, refs } = &*c.read_history_row(h2).await.unwrap()
    else {
        panic!("an edit of one chunk is a list patch");
    };
    assert_eq!(*base, h);
    assert_eq!(refs.len(), 1);
    let chunk = refs.values().next().unwrap().as_ref().unwrap().node;
    assert_eq!(chunk.cs, writer(2));
    assert!(matches!(
        &*c.read_history_row(chunk).await.unwrap(),
        StoredHistoryNode::Patch { over, .. } if over.entries.len() == 1
    ));

    // Patches name their keyframe's entries by position, so with the
    // keyframes gone a lookup reports the damage instead of a wrong answer.
    meta.delete_derived_kind(HistoryTree::NAME).await.unwrap();
    let mut pending = PackBuilder::new(writer(2));
    let again = repo
        .stage_history_edit(Some(h), vec![("f0500".into(), leaf(9))], &mut pending)
        .await
        .unwrap();
    commit_pack(&repo, pending, again).await;
    for name in ["f0500", "f0501", "f0000"] {
        let e = cold(&meta)
            .history_child(h2, name)
            .await
            .unwrap_err()
            .to_string();
        assert!(e.contains("missing"), "{name}: {e}");
    }
}

/// The bytes of all the parts in `meta`.
async fn stored_bytes(meta: &MemoryMetaStore) -> usize {
    let mut n = 0;
    for kind in [HistoryTree::NAME, NODE_KIND] {
        for (_, v) in meta.list_derived(kind, None, usize::MAX).await.unwrap() {
            n += v.len();
        }
    }
    n
}

/// Every node in `meta`, as decoded.
async fn all_nodes(meta: &MemoryMetaStore) -> Vec<(NodeRef, RawNode)> {
    let mut out = Vec::new();
    for (k, v) in meta
        .list_derived(HistoryTree::NAME, None, usize::MAX)
        .await
        .unwrap()
    {
        let cs = ChangesetId(k);
        let mut parts = vec![(0, v)];
        for p in 1.. {
            match meta.get_derived(NODE_KIND, &part_key(cs, p)).await.unwrap() {
                Some(b) => parts.push((p, b)),
                None => break,
            }
        }
        for (p, b) in parts {
            for (i, (n, _)) in decode_part(&b, cs, p)
                .unwrap()
                .nodes
                .into_iter()
                .enumerate()
            {
                out.push((
                    NodeRef {
                        cs,
                        part: p,
                        idx: i as u32,
                    },
                    n,
                ));
            }
        }
    }
    out
}

/// A long run of edits, each stored against the previous version: every
/// version reads back from a cold cache, patches always point at a
/// keyframe, keyframes restart as patches grow, and the bytes are a small
/// part of what keyframes alone would take.
#[tokio::test]
async fn a_chain_of_edits_stays_two_reads_deep_and_small() {
    let (meta, repo) = mem();
    let mut node = wide(300);
    let mut h = repo.put_history_node(writer(0), &node, None).await.unwrap();
    let mut versions = vec![(h, node.clone())];
    let mut whole = 0;
    for step in 0..200u32 {
        let name = match step % 3 {
            0 => format!("f{:04}", (step * 37) % 300),
            1 => format!("g{step:04}"),
            _ => "f0150".to_string(),
        };
        let w = writer(step + 1);
        let e = HistoryEntry::Leaf { changed: vec![w] };
        node.entries.insert(name.clone(), e.clone());
        whole += node
            .chunks()
            .unwrap()
            .into_iter()
            .map(|c| cost(w, &StoredHistoryNode::Flat(c), None))
            .sum::<usize>();
        let mut pending = PackBuilder::new(w);
        let root = repo
            .stage_history_edit(Some(h), vec![(name, e)], &mut pending)
            .await
            .unwrap();
        h = commit_pack(&repo, pending, root).await;
        versions.push((h, node.clone()));
    }
    let nodes = all_nodes(&meta).await;
    let kinds: HashMap<NodeRef, bool> = nodes
        .iter()
        .map(|(r, n)| (*r, n.base().is_none()))
        .collect();
    let mut patches = 0;
    for (r, n) in &nodes {
        if let Some(base) = n.base() {
            patches += 1;
            assert!(kinds[&base], "{r}: patch on a patch");
        }
    }
    assert!(patches > nodes.len() / 2, "{patches} of {}", nodes.len());
    let stored = stored_bytes(&meta).await;
    assert!(
        stored * 5 < whole,
        "stored {stored} bytes against {whole} for keyframes only"
    );
    let c = cold(&meta);
    for (h, node) in &versions {
        assert_eq!(&c.read_history_node(*h).await.unwrap(), node);
    }
}

/// Diffs that skip shared chunks, and lookups through a prepared node,
/// agree with whole nodes read cold, across patched, split and whole
/// versions.
#[tokio::test]
async fn chunked_diffs_and_lookups_match_whole_nodes() {
    let (meta, repo) = mem();
    let mut node = wide(40);
    let mut h = repo.put_history_node(writer(0), &node, None).await.unwrap();
    let mut versions = vec![h];
    for step in 0..120u32 {
        let name = match step % 4 {
            0 => format!("f{:04}", (step * 37) % 300),
            1 | 2 => format!("g{step:04}"),
            _ => "f0010".to_string(),
        };
        node.entries.insert(name, leaf((step % 250) as u8));
        h = repo
            .put_history_node(writer(step + 1), &node, Some(h))
            .await
            .unwrap();
        versions.push(h);
    }
    let c = cold(&meta);
    for (i, &a) in versions.iter().enumerate().step_by(7) {
        let whole_a = c.read_history_node(a).await.unwrap();
        let lookup = c.node_lookup(a).await.unwrap();
        for name in ["f0000", "f0010", "f0039", "g0001", "g0118", "a", "zz"] {
            assert_eq!(
                c.lookup_child(&lookup, name).await.unwrap().as_ref(),
                whole_a.entries.get(name),
                "{name} in version {i}"
            );
        }
        for &b in versions.iter().skip(i).step_by(11) {
            let whole_b = c.read_history_node(b).await.unwrap();
            let mut want = Vec::new();
            let names: BTreeSet<&String> = whole_a
                .entries
                .keys()
                .chain(whole_b.entries.keys())
                .collect();
            for n in names {
                let (ea, eb) = (whole_a.entries.get(n), whole_b.entries.get(n));
                if ea != eb {
                    want.push((n.clone(), ea.cloned(), eb.cloned()));
                }
            }
            assert_eq!(c.diff_history_nodes(a, b).await.unwrap(), want);
        }
    }
}

/// A pack too large for one part is cut into parts: the root's is part 0,
/// the others are written deepest first, and nodes in every part read back
/// cold, through references across parts.
#[tokio::test]
async fn a_large_pack_is_cut_into_parts() {
    let (meta, repo) = mem();
    let w = writer(1);
    // Wide directories whose entries name many changesets, under one root.
    let mut pending = PackBuilder::new(w);
    let mut root = HistoryNode::default();
    let mut dirs = BTreeMap::new();
    for d in 0..12u32 {
        let node = HistoryNode {
            entries: (0..400u32)
                .map(|i| {
                    let e = HistoryEntry::Leaf {
                        changed: vec![writer(1000 + d * 400 + i)],
                    };
                    (format!("file{i:04}.c"), e)
                })
                .collect(),
        };
        let at = repo
            .stage_history_node(&node, None, &mut pending)
            .await
            .unwrap();
        root.entries.insert(
            format!("d{d:02}"),
            HistoryEntry::Tree {
                changed: vec![w],
                node: at,
            },
        );
        dirs.insert(format!("d{d:02}"), node);
    }
    let top = repo
        .stage_history_node(&root, None, &mut pending)
        .await
        .unwrap();
    let pack = pending.clone().finish(top);
    assert!(pack.rows.len() >= 4, "{} parts", pack.rows.len() + 1);
    let parts: Vec<u32> = pack
        .rows
        .iter()
        .map(|(k, _)| {
            (1..=pack.rows.len() as u32)
                .find(|p| part_key(w, *p) == *k)
                .expect("a part key")
        })
        .collect();
    assert!(
        parts.windows(2).all(|p| p[0] > p[1]),
        "deepest parts first: {parts:?}"
    );
    for (_, b) in &pack.rows {
        assert!(b.len() <= PART_BYTES + 256, "part of {} bytes", b.len());
    }
    let at = commit_pack(&repo, pending, top).await;
    assert_eq!(at.part, 0);
    let c = cold(&meta);
    let back = c.read_history_node(at).await.unwrap();
    assert_eq!(back.entries.len(), 12);
    for (name, e) in &back.entries {
        assert_eq!(
            c.read_history_node(e.node().unwrap()).await.unwrap(),
            dirs[name]
        );
    }
}

/// Damaged parts, and parts stored under another part's key, are errors.
#[tokio::test]
async fn damaged_or_misplaced_parts_are_corrupt() {
    let (meta, repo) = mem();
    let a = repo
        .put_history_node(writer(1), &wide(30), None)
        .await
        .unwrap();
    let b = repo
        .put_history_node(writer(2), &wide(31), None)
        .await
        .unwrap();
    let va = meta
        .get_derived(HistoryTree::NAME, &a.cs.0)
        .await
        .unwrap()
        .unwrap();
    let vb = meta
        .get_derived(HistoryTree::NAME, &b.cs.0)
        .await
        .unwrap()
        .unwrap();
    for (i, bad) in [
        vb.clone(),
        {
            let mut v = va.to_vec();
            let n = v.len();
            v[n - 1] ^= 1;
            Bytes::from(v)
        },
        va.slice(..va.len() - 1),
        Bytes::from_static(&[0; 4]),
    ]
    .into_iter()
    .enumerate()
    {
        meta.put_derived(HistoryTree::NAME, &a.cs.0, bad)
            .await
            .unwrap();
        let e = cold(&meta)
            .read_history_row(a)
            .await
            .unwrap_err()
            .to_string();
        assert!(e.contains("corrupt history node"), "{i}: {e}");
    }
    meta.put_derived(HistoryTree::NAME, &a.cs.0, va)
        .await
        .unwrap();
    assert!(cold(&meta).read_history_row(a).await.is_ok());
    // A node index past the part's end is missing.
    let gone = NodeRef { idx: 5, ..a };
    let e = cold(&meta)
        .read_history_row(gone)
        .await
        .unwrap_err()
        .to_string();
    assert!(e.contains("missing"), "{e}");
}

/// Arbitrary bytes never panic the decoder.
#[test]
fn decoding_garbage_fails_cleanly() {
    let w = writer(3);
    let mut b = PackBuilder::new(w);
    let at = b.stage(StoredHistoryNode::Flat(wide(20)), None);
    let good = b.finish(at).value;
    let raw = canonical(&good).unwrap().to_vec();
    let mut seed = 1u64;
    for round in 0..2000 {
        seed = seed
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        let mut v = raw.clone();
        match round % 3 {
            0 => v.truncate((seed >> 33) as usize % (raw.len() + 1)),
            1 => {
                let i = (seed >> 33) as usize % raw.len();
                v[i] = (seed >> 13) as u8;
            }
            _ => {
                v = (0..(seed >> 40) % 64)
                    .map(|i| (seed >> (i % 50)) as u8)
                    .collect()
            }
        }
        let mut bytes = vec![0; 8];
        bytes.push(0);
        bytes.extend(&v);
        let _ = decode_unchecked(&bytes, w, (round % 3) as u32);
        let _ = decode_part(&bytes, w, 0);
    }
}

#[tokio::test]
async fn a_patch_on_a_patch_is_corrupt() {
    let (meta, repo) = mem();
    let key = wide(10);
    let k = repo.put_history_node(writer(1), &key, None).await.unwrap();
    let mut with_x = key.clone();
    with_x.entries.insert("x".into(), leaf(2));
    let mut pending = PackBuilder::new(writer(2));
    let first = pending.stage(
        StoredHistoryNode::Patch {
            base: k,
            over: HistoryNode {
                entries: BTreeMap::from([("x".to_string(), leaf(2))]),
            },
        },
        Some(Arc::new(StoredHistoryNode::Flat(key))),
    );
    let first = commit_pack(&repo, pending, first).await;
    let mut pending = PackBuilder::new(writer(3));
    let second = pending.stage(
        StoredHistoryNode::Patch {
            base: first,
            over: HistoryNode {
                entries: BTreeMap::from([("y".to_string(), leaf(3))]),
            },
        },
        Some(Arc::new(StoredHistoryNode::Flat(with_x))),
    );
    let second = commit_pack(&repo, pending, second).await;
    assert_eq!(
        repo.read_history_node(first).await.unwrap().entries.len(),
        11
    );
    for repo in [repo.clone(), cold(&meta)] {
        let e = repo
            .read_history_node(second)
            .await
            .unwrap_err()
            .to_string();
        assert!(e.contains("corrupt history node"), "{e}");
    }
}

#[tokio::test]
async fn a_chunk_that_does_not_fit_its_list_is_corrupt() {
    let (_, repo) = mem();
    let a = repo
        .put_history_node(writer(1), &wide(10), None)
        .await
        .unwrap();
    // The list names "f0005" first, but the chunk starts at "f0000".
    let mut pending = PackBuilder::new(writer(2));
    let h = pending.stage(
        StoredHistoryNode::Split(vec![ChunkRef {
            first: "f0005".into(),
            node: a,
        }]),
        None,
    );
    let h = commit_pack(&repo, pending, h).await;
    let e = repo.read_history_node(h).await.unwrap_err().to_string();
    assert!(e.contains("corrupt history node"), "{e}");
}
