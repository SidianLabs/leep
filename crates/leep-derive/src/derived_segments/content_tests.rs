//! Family `content-v1` (ADR 0106): the code search rows and their keyframes
//! sealed verbatim, numbered by a walk of the heads' trees, only what
//! retention keeps live, and compared by the set or filter they stand for
//! before a row is deleted.

use std::collections::{BTreeMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use leep_core::memory::{MemoryBlobStore, MemoryMetaStore};
use leep_core::{Changeset, ChangesetId, FileKind, Hash, MetaStore, Result};

use super::codec::{
    self, SealRow, SegmentRef, COL_BLOB_KEYFRAME, COL_BLOB_TRIGRAMS, COL_TREE_KEYFRAME,
    COL_TREE_TRIGRAMS, FAMILY_CONTENT,
};
use super::tests::{feature_hook, fresh, pass, settle};
use super::*;
use crate::search::retention::{RetentionOptions, RetentionState};
use crate::search::tests::{commit_files, Files};
use crate::search::trigram::{BlobRow, Bloom, StoredBlob, TreeRow};
use crate::search::{SearchQuery, BLOB_KEYFRAME_KIND, BLOB_KIND, KEYFRAME_KIND, TREE_KIND};
use crate::Repo;

/// `generation`, a kind of family `changeset-v2`.
const GENERATION: &str = <crate::derived::Generation as crate::derived::DerivedKind>::NAME;

/// The kinds of `content-v1`, in column order.
const KINDS: [&str; 4] = [BLOB_KIND, BLOB_KEYFRAME_KIND, TREE_KIND, KEYFRAME_KIND];

fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

fn repo() -> Repo {
    Repo::new(
        Arc::new(MemoryBlobStore::new()),
        Arc::new(MemoryMetaStore::new()),
    )
}

fn file(body: String) -> (Vec<u8>, FileKind) {
    (body.into_bytes(), FileKind::Regular)
}

/// A base of 20 files in four directories, then `lands` edits alternating
/// between two files on `main`, each derived as the warmer derives it: the
/// rows are whole at the base, patches on keyframes after.
async fn chain(r: &Repo, lands: usize) -> Vec<ChangesetId> {
    let mut files = Files::new();
    for d in 0..4 {
        for f in 0..5 {
            files.insert(
                format!("src/d{d}/f{f}.rs"),
                file(format!("fn d{d}_f{f}() {{ needle_{d}_{f} }}\n").repeat(20)),
            );
        }
    }
    let base = commit_files(r, &[], &files).await;
    r.meta().cas_ref("main", None, Some(base)).await.unwrap();
    r.derive_search(base).await.unwrap();
    let mut ids = vec![base];
    let mut bodies = [
        files["src/d1/f1.rs"].0.clone(),
        files["src/d2/f0.rs"].0.clone(),
    ];
    for i in 0..lands {
        let (path, body) = if i % 2 == 0 {
            ("src/d1/f1.rs", &mut bodies[0])
        } else {
            ("src/d2/f0.rs", &mut bodies[1])
        };
        body.extend(format!("fn v{i}() {{ edit_{i} }}\n").as_bytes());
        let one = Files::from([(path.to_string(), (body.clone(), FileKind::Regular))]);
        let prev = *ids.last().unwrap();
        let id = commit_files(r, &[prev], &one).await;
        r.meta()
            .cas_ref("main", Some(prev), Some(id))
            .await
            .unwrap();
        r.derive_search(id).await.unwrap();
        ids.push(id);
    }
    ids
}

/// Small limits, every family named, retention's window of zero judged
/// after every ref log entry: only the current heads are live.
fn opts(mode: Mode) -> CompactOptions {
    CompactOptions {
        content_keep: Some(Duration::ZERO),
        content_skew: Duration::ZERO,
        content_now: Some(now() + 10),
        ..CompactOptions::small(mode)
    }
}

/// Every row of the family's kinds, from the rows themselves.
async fn rows(r: &Repo) -> BTreeMap<(&'static str, Hash), Bytes> {
    let mut out = BTreeMap::new();
    for kind in KINDS {
        for (k, v) in r
            .row_meta()
            .list_derived(kind, None, usize::MAX)
            .await
            .unwrap()
        {
            out.insert((kind, k), v);
        }
    }
    out
}

/// Keys live `content-v1` segments hold a value of `col` for.
async fn sealed(r: &Repo, col: u8) -> HashSet<Hash> {
    let f = fresh(r);
    f.segments().refresh(&**f.row_meta()).await.unwrap();
    f.segments().family(FAMILY_CONTENT).keys_with(col)
}

/// The live set as a pass of [`opts`] sees it.
async fn live(r: &Repo) -> crate::search::retention::ContentLive {
    r.content_live(Some(Duration::ZERO), Duration::ZERO, now() + 10)
        .await
        .unwrap()
}

/// What searches of `id` find, through a new handle.
async fn answers(r: &Repo, id: ChangesetId) -> Vec<String> {
    let mut out = Vec::new();
    for q in [
        "needle_1_1",
        "needle_2_0",
        "edit_3",
        "edit_",
        "fn d3_f4",
        "not_anywhere_xyz",
    ] {
        let found = fresh(r).search(id, &SearchQuery::new(q)).await.unwrap();
        for m in found.matches {
            out.push(format!("{q} {} {} {}", m.path, m.line_number, m.line));
        }
    }
    out
}

fn seal_rows(
    rows: &BTreeMap<(&'static str, Hash), Bytes>,
    order: &dyn Fn(&Hash) -> u64,
) -> Vec<SealRow> {
    let mut by_key: BTreeMap<Hash, Vec<(u8, Bytes)>> = BTreeMap::new();
    for ((kind, k), v) in rows {
        let col = codec::family(FAMILY_CONTENT)
            .unwrap()
            .column_of(kind)
            .unwrap();
        by_key.entry(*k).or_default().push((col, v.clone()));
    }
    by_key
        .into_iter()
        .map(|(key, mut values)| {
            values.sort_by_key(|(c, _)| *c);
            SealRow {
                key,
                order: order(&key),
                values,
            }
        })
        .collect()
}

/// Real rows of a head tree (whole rows, patches, keyframes of both kinds)
/// come back from a segment as they went in, and a directory's filter and
/// its files' rows are neighbours in the walk's numbering.
#[tokio::test]
async fn content_segments_round_trip_real_head_trees() {
    let r = repo();
    let ids = chain(&r, 9).await;
    let all = rows(&r).await;
    for kind in KINDS {
        assert!(all.keys().any(|(k, _)| *k == kind), "{kind}");
    }
    assert!(all
        .iter()
        .any(|((k, _), v)| *k == BLOB_KIND
            && matches!(StoredBlob::parse(v), Ok(StoredBlob::Patch(_)))));
    assert!(all
        .iter()
        .any(|((k, _), v)| *k == TREE_KIND && matches!(TreeRow::parse(v), Ok(TreeRow::Patch(_)))));
    let l = live(&r).await;
    let seal = seal_rows(&all, &|k| l.order(k).map_or(1 << 40, u64::from));
    let (enc, bad) = codec::encode(FAMILY_CONTENT, &seal, 3).unwrap();
    assert!(bad.is_empty(), "{bad:?}");
    let idx = codec::decode_index(&enc.index.0, &enc.index.1).unwrap();
    let data = codec::decode_data(&idx, &enc.data.0, enc.data.1.clone()).unwrap();
    let mut back = BTreeMap::new();
    for (k, col, v) in codec::read_all(&idx, &data).unwrap() {
        back.insert((codec::column_kind(FAMILY_CONTENT, col).unwrap(), k), v);
    }
    assert_eq!(back, all);
    // A directory, then its files, in the walk.
    let head = *ids.last().unwrap();
    let root = r.root_tree(head).await.unwrap();
    let src = r.read_tree(root).await.unwrap();
    let leep_core::TreeEntry::Dir { tree: src } = src.entries["src"] else {
        panic!()
    };
    let src = r.read_tree(src).await.unwrap();
    let leep_core::TreeEntry::Dir { tree: d1 } = src.entries["d1"] else {
        panic!()
    };
    let at = l.order(&d1).unwrap();
    let node = r.read_tree(d1).await.unwrap();
    for (i, e) in node.entries.values().enumerate() {
        let leep_core::TreeEntry::File { content, .. } = e else {
            panic!()
        };
        assert_eq!(l.order(content), Some(at + 1 + i as u32));
    }
    assert!(l.live(&d1) && !l.all);
}

/// A value that is not its kind is never sealed: the row stays.
#[test]
fn content_values_that_are_not_their_kind_are_not_sealed() {
    let whole = Bytes::from(BlobRow::Trigrams(vec![1, 2, 3]).encode());
    let cases: [(u8, &[u8]); 6] = [
        (COL_BLOB_TRIGRAMS, b"\x07junk"),
        (COL_BLOB_TRIGRAMS, b"\x01\x80"),
        (COL_BLOB_KEYFRAME, &[0, 1]),
        (COL_TREE_TRIGRAMS, b"\x09"),
        (COL_TREE_KEYFRAME, &[3]),
        (COL_TREE_KEYFRAME, b"\x02\xff\xff\xff\xff\xff"),
    ];
    for (i, (col, v)) in cases.iter().enumerate() {
        let rows = vec![
            SealRow {
                key: Hash([i as u8 + 1; 32]),
                order: 0,
                values: vec![(*col, Bytes::copy_from_slice(v))],
            },
            SealRow {
                key: Hash([0xee; 32]),
                order: 1,
                values: vec![(COL_BLOB_TRIGRAMS, whole.clone())],
            },
        ];
        let (enc, bad) = codec::encode(FAMILY_CONTENT, &rows, 1).unwrap();
        assert_eq!(bad.len(), 1, "case {i}");
        assert_eq!(enc.seg.rows, 2, "case {i}: the key still has its place");
    }
}

/// Fixed rows of every column: their segment bytes never change.
pub(crate) fn golden_content_rows() -> Vec<SealRow> {
    let whole = Bytes::from(BlobRow::Trigrams(vec![1, 70, 4000, 65_000]).encode());
    let mut f = Bloom::default();
    for c in [3, 99, 12_345] {
        f.insert(c);
    }
    let filter = Bytes::from(f.encode_row());
    vec![
        SealRow {
            key: Hash([1; 32]),
            order: 2,
            values: vec![
                (COL_BLOB_TRIGRAMS, whole.clone()),
                (COL_BLOB_KEYFRAME, whole.clone()),
            ],
        },
        SealRow {
            key: Hash([2; 32]),
            order: 0,
            values: vec![
                (COL_TREE_TRIGRAMS, filter.clone()),
                (COL_TREE_KEYFRAME, filter),
            ],
        },
        SealRow {
            key: Hash([3; 32]),
            order: 1,
            values: vec![(COL_TREE_TRIGRAMS, Bytes::from_static(&[3]))],
        },
        SealRow {
            key: Hash([4; 32]),
            order: 3,
            values: vec![(COL_BLOB_TRIGRAMS, Bytes::from_static(&[0, 1]))],
        },
    ]
}

const GOLDEN_CONTENT_INDEX: &str =
    "d8ed2abe1042da3fbe92f04d1858e9eb76e43b1d72e2512d9fdff50f3eba0225";
const GOLDEN_CONTENT_DATA: &str =
    "4c3d92f7102e41722b2009aa0b48375a84a6ae2b98fbe59c4e18fdf22929fdd0";

/// The bytes of a `content-v1` segment are fixed (FORMATS.md), and every
/// flipped bit of either blob is refused.
#[test]
fn content_golden_bytes_and_every_damaged_bit_is_refused() {
    let (enc, bad) = codec::encode(FAMILY_CONTENT, &golden_content_rows(), 11).unwrap();
    assert!(bad.is_empty());
    assert_eq!(
        (enc.index.0.to_string(), enc.data.0.to_string()),
        (
            GOLDEN_CONTENT_INDEX.to_string(),
            GOLDEN_CONTENT_DATA.to_string()
        ),
        "the content-v1 segment bytes changed"
    );
    let (ih, index) = &enc.index;
    let (dh, data) = &enc.data;
    let idx = codec::decode_index(ih, index).unwrap();
    assert_eq!(idx.family, FAMILY_CONTENT);
    for bit in 0..index.len() * 8 {
        let mut d = index.to_vec();
        d[bit / 8] ^= 1 << (bit % 8);
        assert!(codec::decode_index(ih, &d).is_err(), "index bit {bit}");
    }
    for bit in 0..data.len() * 8 {
        let mut d = data.to_vec();
        d[bit / 8] ^= 1 << (bit % 8);
        assert!(
            codec::decode_data(&idx, dh, d.into()).is_err(),
            "data bit {bit}"
        );
    }
    let d = codec::decode_data(&idx, dh, data.clone()).unwrap();
    let back = codec::read_all(&idx, &d).unwrap();
    assert_eq!(back.len(), 6);
}

/// Sealed and retired, the search rows of the head are gone from the rows
/// and every search answers as before; rows retention no longer keeps
/// (superseded versions) are neither sealed nor retired; keyframes all are.
#[tokio::test]
async fn sealed_and_retired_search_answers_as_rows_did() {
    let r = repo();
    let ids = chain(&r, 9).await;
    let head = *ids.last().unwrap();
    let want = answers(&r, head).await;
    let old = answers(&r, ids[3]).await;
    let before = rows(&r).await;
    let log = Arc::new(Mutex::new(Vec::new()));
    let hook = feature_hook(log.clone());
    let report = settle(
        &r,
        &opts(Mode::Retire),
        &mut CompactState::default(),
        Some(&hook),
    )
    .await;
    assert!(report.retired_rows > 0, "{report:?}");
    assert_eq!(report.mismatched, 0);
    assert!(log.lock().unwrap().contains(&FEATURE_3), "{log:?}");
    let l = live(&r).await;
    let after = rows(&r).await;
    for (kind, k) in before.keys() {
        let kept = after.contains_key(&(*kind, *k));
        match *kind {
            BLOB_KEYFRAME_KIND | KEYFRAME_KIND => assert!(!kept, "{kind} {k}"),
            _ => assert_eq!(kept, !l.live(k), "{kind} {k}"),
        }
    }
    assert!(
        after.keys().any(|(k, _)| *k == BLOB_KIND),
        "superseded rows stay rows"
    );
    for col in [COL_BLOB_TRIGRAMS, COL_TREE_TRIGRAMS] {
        assert!(sealed(&r, col).await.iter().all(|k| l.live(k)));
    }
    assert_eq!(answers(&r, head).await, want);
    assert_eq!(answers(&r, ids[3]).await, old);
    // The head derives nothing: every row it needs is sealed.
    let f = fresh(&r);
    let derived = f.derive_search(head).await.unwrap();
    assert_eq!((derived.blob_rows, derived.tree_rows), (0, 0));
    assert_eq!(rows(&r).await, after);
}

/// As the heads move on, merges leave out the blob rows retention no
/// longer keeps, and no live segment is left a third dead; keyframes and
/// directory filters stay (a stored filter means its subtree's are).
#[tokio::test]
async fn merges_leave_dead_keys_out_and_rewrite_mostly_dead_segments() {
    let r = repo();
    let ids = chain(&r, 2).await;
    let mut st = CompactState::default();
    let o = CompactOptions {
        fanout: 64,
        ..opts(Mode::Build)
    };
    settle(&r, &o, &mut st, None).await;
    let first = sealed(&r, COL_BLOB_TRIGRAMS).await;
    let keyframes = sealed(&r, COL_BLOB_KEYFRAME).await;
    let trees = sealed(&r, COL_TREE_TRIGRAMS).await;
    assert!(!first.is_empty() && !keyframes.is_empty() && !trees.is_empty());
    // Many more lands: the first segments' versions of the two files and
    // the directories above them go dead.
    let mut files = Files::new();
    let mut prev = *ids.last().unwrap();
    for i in 0..12 {
        for d in 0..4 {
            files.insert(
                format!("src/d{d}/f{}.rs", i % 5),
                file(format!("fn later_{i}_{d}() {{}}\n").repeat(10)),
            );
        }
        let id = commit_files(&r, &[prev], &files).await;
        r.meta()
            .cas_ref("main", Some(prev), Some(id))
            .await
            .unwrap();
        r.derive_search(id).await.unwrap();
        prev = id;
        files.clear();
    }
    let want = answers(&r, prev).await;
    let o = CompactOptions {
        content_now: Some(now() + 10),
        ..o
    };
    let report = settle(&r, &o, &mut st, None).await;
    assert!(report.merges > 0, "{report:?}");
    let l = live(&r).await;
    let (_, view::ManifestRead::Read(m)) = read_manifest(&r, FAMILY_CONTENT).await.unwrap() else {
        panic!()
    };
    for s in &m.live {
        let idx = view::load_index(&**r.blobs(), FAMILY_CONTENT, s)
            .await
            .unwrap()
            .unwrap();
        let (mut total, mut dead) = (0, 0);
        for col in [
            COL_BLOB_TRIGRAMS,
            COL_BLOB_KEYFRAME,
            COL_TREE_TRIGRAMS,
            COL_TREE_KEYFRAME,
        ] {
            for k in idx.keys_with(col) {
                total += 1;
                if col == COL_BLOB_TRIGRAMS && !l.live(k) {
                    dead += 1;
                }
            }
        }
        assert!(
            dead * 3 < total,
            "segment {} is {dead} of {total} dead",
            s.index
        );
    }
    let now_sealed = sealed(&r, COL_BLOB_TRIGRAMS).await;
    assert!(
        first.iter().any(|k| !l.live(k) && !now_sealed.contains(k)),
        "a dead key was left out"
    );
    assert!(keyframes.is_subset(&sealed(&r, COL_BLOB_KEYFRAME).await));
    assert!(trees.iter().any(|k| !l.live(k)), "a directory filter died");
    assert!(trees.is_subset(&sealed(&r, COL_TREE_TRIGRAMS).await));
    assert_eq!(answers(&r, prev).await, want);
}

/// A row rewritten as another patch (here the whole set) of what its
/// segment holds is retired: compared by the set or filter, not the
/// bytes. A row standing for another set stays and is counted once.
#[tokio::test]
async fn rows_are_compared_by_the_set_they_stand_for() {
    let r = repo();
    let ids = chain(&r, 9).await;
    let head = *ids.last().unwrap();
    let mut st = CompactState::default();
    settle(&r, &opts(Mode::Build), &mut st, None).await;
    let all = rows(&r).await;
    let l = live(&r).await;
    let (blob, patch) = all
        .iter()
        .find_map(|((kind, k), v)| {
            (*kind == BLOB_KIND
                && l.live(k)
                && matches!(StoredBlob::parse(v), Ok(StoredBlob::Patch(_))))
            .then(|| (*k, v.clone()))
        })
        .expect("a live blob patch");
    let base = StoredBlob::parse(&patch).unwrap().base().unwrap();
    let kf = all[&(BLOB_KEYFRAME_KIND, base)].clone();
    let whole = Bytes::from(BlobRow::of_stored(&patch, Some(&kf)).unwrap().encode());
    assert_ne!(whole, patch);
    r.row_meta()
        .put_derived(BLOB_KIND, &blob, whole)
        .await
        .unwrap();
    let (tree, tpatch) = all
        .iter()
        .find_map(|((kind, k), v)| {
            (*kind == TREE_KIND && l.live(k) && matches!(TreeRow::parse(v), Ok(TreeRow::Patch(_))))
                .then(|| (*k, v.clone()))
        })
        .expect("a live tree patch");
    let TreeRow::Patch(p) = TreeRow::parse(&tpatch).unwrap() else {
        panic!()
    };
    let tkf = all[&(KEYFRAME_KIND, p.base)].clone();
    let twhole = Bytes::from(
        Bloom::of_row(&tpatch, Some(&tkf))
            .unwrap()
            .unwrap()
            .encode_row(),
    );
    r.row_meta()
        .put_derived(TREE_KIND, &tree, twhole)
        .await
        .unwrap();
    // Another blob's row, standing for another set.
    let (other, wrong) = all
        .iter()
        .find_map(|((kind, k), v)| {
            (*kind == BLOB_KIND && l.live(k) && *k != blob && v.first() == Some(&1))
                .then(|| (*k, v.clone()))
        })
        .unwrap();
    let wrong_for = all
        .keys()
        .find(|(kind, k)| *kind == BLOB_KIND && l.live(k) && *k != other && *k != blob)
        .unwrap()
        .1;
    r.row_meta()
        .put_derived(BLOB_KIND, &wrong_for, wrong)
        .await
        .unwrap();
    let hook = feature_hook(Arc::default());
    let report = settle(&r, &opts(Mode::Retire), &mut st, Some(&hook)).await;
    assert_eq!(report.mismatched, 1, "{report:?}");
    let left = rows(&r).await;
    assert!(!left.contains_key(&(BLOB_KIND, blob)));
    assert!(!left.contains_key(&(TREE_KIND, tree)));
    assert!(left.contains_key(&(BLOB_KIND, wrong_for)), "rows win");
    // Once more: not read or counted again.
    let again = settle(&r, &opts(Mode::Retire), &mut st, Some(&hook)).await;
    assert_eq!(again.mismatched, 0);
    let _ = head;
}

/// Keyframes lost while the patches on them are sealed (a `repair drop`
/// race, a lost segment): the patch reads as not stored, a search derives
/// the row whole and answers right, and a pass never retires a row against
/// a patch it cannot resolve.
#[tokio::test]
async fn a_sealed_patch_whose_keyframe_is_lost_reads_as_not_stored() {
    let r = repo();
    let ids = chain(&r, 9).await;
    let head = *ids.last().unwrap();
    let want = answers(&r, head).await;
    let mut st = CompactState::default();
    let hook = feature_hook(Arc::default());
    settle(&r, &opts(Mode::Retire), &mut st, Some(&hook)).await;
    assert!(rows(&r)
        .await
        .keys()
        .all(|(k, _)| *k != BLOB_KEYFRAME_KIND && *k != KEYFRAME_KIND));
    let patched: Vec<Hash> = {
        let f = fresh(&r);
        let mut out = Vec::new();
        for k in sealed(&r, COL_BLOB_TRIGRAMS).await {
            let v = f.meta().get_derived(BLOB_KIND, &k).await.unwrap().unwrap();
            if matches!(StoredBlob::parse(&v), Ok(StoredBlob::Patch(_))) {
                out.push(k);
            }
        }
        out
    };
    assert!(!patched.is_empty());
    // Every keyframe column cleared: the keyframes are nowhere.
    let lease = acquire_for(&r, "test").await.unwrap();
    drop_columns(
        &r,
        &lease,
        FAMILY_CONTENT,
        &[COL_BLOB_KEYFRAME, COL_TREE_KEYFRAME],
    )
    .await
    .unwrap();
    crate::gc_lease::release(lease, Ok(())).await.unwrap();
    assert!(sealed(&r, COL_BLOB_KEYFRAME).await.is_empty());
    assert_eq!(answers(&r, head).await, want);
    // The search wrote whole rows for the patched blobs it probed.
    let written = rows(&r).await;
    let restored: Vec<&Hash> = patched
        .iter()
        .filter(|k| written.contains_key(&(BLOB_KIND, **k)))
        .collect();
    assert!(!restored.is_empty());
    for k in &restored {
        assert_eq!(written[&(BLOB_KIND, **k)].first(), Some(&1));
    }
    // A pass compares them with patches it cannot resolve: neither equal
    // nor counted unequal. The rows resolve alone, so they are sealed again
    // into a new segment, which answers for them, and then retired: the
    // store converges instead of comparing them every pass.
    let report = settle(&r, &opts(Mode::Retire), &mut st, Some(&hook)).await;
    assert_eq!(report.mismatched, 0, "{report:?}");
    // The pass that finds them only marks them; the next ones seal and
    // retire.
    let report = settle(&r, &opts(Mode::Retire), &mut st, Some(&hook)).await;
    assert_eq!(report.mismatched, 0, "{report:?}");
    assert!(
        report.sealed_segments > 0 && report.retired_rows > 0,
        "{report:?}"
    );
    let still = rows(&r).await;
    let f = fresh(&r);
    for k in &restored {
        assert!(!still.contains_key(&(BLOB_KIND, **k)), "{k} was retired");
        let v = f.meta().get_derived(BLOB_KIND, k).await.unwrap().unwrap();
        assert_eq!(v.first(), Some(&1), "the whole row answers now");
    }
    let again = settle(&r, &opts(Mode::Retire), &mut st, Some(&hook)).await;
    assert_eq!(
        (again.sealed_segments, again.retired_rows),
        (0, 0),
        "{again:?}"
    );
    assert_eq!(answers(&r, head).await, want);
}

/// A content-v1 read never fails for another family's segments, and the
/// reverse: an index blob of `content-v1` that cannot be read now (a
/// transient error) does not fail a `generation` read.
#[tokio::test]
async fn one_familys_failing_index_does_not_fail_anothers_reads() {
    let blobs: Arc<dyn leep_core::BlobStore> = Arc::new(MemoryBlobStore::new());
    let flaky = Arc::new(super::tests::Flaky {
        inner: blobs.clone(),
        failing: Mutex::default(),
    });
    let r = Repo::new(flaky.clone(), Arc::new(MemoryMetaStore::new()));
    let ids = chain(&r, 5).await;
    let hook = feature_hook(Arc::default());
    settle(
        &r,
        &opts(Mode::Retire),
        &mut CompactState::default(),
        Some(&hook),
    )
    .await;
    assert_eq!(r.row_meta().count_derived(GENERATION).await.unwrap(), 0);
    let (_, view::ManifestRead::Read(m)) = read_manifest(&r, FAMILY_CONTENT).await.unwrap() else {
        panic!()
    };
    for s in &m.live {
        flaky.failing.lock().unwrap().insert(s.index);
    }
    let keys: Vec<Hash> = ids.iter().map(|i| i.0).collect();
    let f = fresh(&r);
    let one = f.meta().get_derived(GENERATION, &keys[3]).await.unwrap();
    assert!(one.is_some());
    let f = fresh(&r);
    let many = f.meta().get_derived_many(GENERATION, &keys).await.unwrap();
    assert!(many.iter().all(Option::is_some));
    let f = fresh(&r);
    let kinds = f
        .meta()
        .get_derived_kinds(&[(GENERATION, &keys)])
        .await
        .unwrap();
    assert!(kinds[0].iter().all(Option::is_some));
    // The search rows' own read leaves the unreadable segments out: the
    // key reads as missing (it derives again), not as an error.
    let f = fresh(&r);
    let root = f.root_tree(ids[5]).await.unwrap();
    assert!(f.meta().get_derived(TREE_KIND, &root).await.is_ok());
}

/// Small segments are merged even when larger ones sit between them: a
/// small one between two others joins its smaller neighbour when they fit
/// (the newest waits for the next seal), and a run of small ones merges.
#[tokio::test]
async fn isolated_small_segments_merge_with_a_neighbour() {
    let r = repo();
    let o = CompactOptions {
        content_max_bytes: 1000,
        content_max_rows: 1000,
        ..opts(Mode::Build)
    };
    let none = None;
    let fam = codec::family(FAMILY_CONTENT).unwrap();
    let p = compactor::Pass::load(&r, fam, &o, &none, None)
        .await
        .unwrap();
    let seg = |n: u8, len: u32| SegmentRef {
        index: Hash([n; 32]),
        data: Hash([n + 100; 32]),
        rows: 10,
        columns: 2,
        data_len: len,
    };
    let fits = |run: &[SegmentRef]| {
        let mut b = 0;
        run.iter()
            .take_while(|s| {
                b += s.data_len;
                b <= 1000
            })
            .count()
    };
    let all = |_: &SegmentRef| true;
    let oversize = HashSet::new();
    // [F F r F F r]: the middle small one joins a neighbour.
    let live = [
        seg(1, 600),
        seg(2, 600),
        seg(3, 100),
        seg(4, 600),
        seg(5, 600),
        seg(6, 100),
    ];
    assert_eq!(p.content_run(&live, &all, &fits, &oversize), Some((1, 2)));
    // Only the newest is small: it waits.
    assert_eq!(p.content_run(&live[3..], &all, &fits, &oversize), None);
    // A run of small ones merges, as many as fit.
    let live = [
        seg(1, 300),
        seg(2, 300),
        seg(3, 300),
        seg(4, 300),
        seg(5, 900),
    ];
    assert_eq!(p.content_run(&live, &all, &fits, &oversize), Some((0, 3)));
}

/// Over many small seals the live segments stay bounded: about twice the
/// family's bytes over the cap.
#[tokio::test]
async fn content_segments_stay_bounded_over_many_seals() {
    let r = repo();
    let o = CompactOptions {
        content_max_bytes: 4096,
        ..opts(Mode::Build)
    };
    let mut st = CompactState::default();
    let mut files = Files::new();
    let base = {
        for d in 0..6 {
            for f in 0..4 {
                files.insert(
                    format!("src/d{d}/f{f}.rs"),
                    file(format!("fn d{d}_f{f}() {{ token_{d}_{f} }}\n").repeat(8)),
                );
            }
        }
        commit_files(&r, &[], &files).await
    };
    r.meta().cas_ref("main", None, Some(base)).await.unwrap();
    r.derive_search(base).await.unwrap();
    let mut prev = base;
    for i in 0..40 {
        let one = Files::from([(
            format!("src/d{}/f{}.rs", i % 6, i % 4),
            file(format!("fn land_{i}() {{ edit_{i} }}\n").repeat(6)),
        )]);
        let id = commit_files(&r, &[prev], &one).await;
        r.meta()
            .cas_ref("main", Some(prev), Some(id))
            .await
            .unwrap();
        r.derive_search(id).await.unwrap();
        prev = id;
        let o = CompactOptions {
            content_now: Some(now() + 10),
            ..o.clone()
        };
        settle(&r, &o, &mut st, None).await;
    }
    let (_, view::ManifestRead::Read(m)) = read_manifest(&r, FAMILY_CONTENT).await.unwrap() else {
        panic!()
    };
    let bytes: u64 = m.live.iter().map(|s| s.data_len as u64).sum();
    let bound = 2 * bytes / 4096 + 2;
    assert!(
        m.live.len() as u64 <= bound,
        "{} live segments for {bytes} bytes (bound {bound})",
        m.live.len()
    );
    assert!(m.live.len() > 1, "the cap splits the family");
}

/// Retention deletes rows a pass is sealing (the head moved back between
/// the pass's walk and retention's): the sealed values stay right, and the
/// passes after it leave the now dead keys out and settle.
#[tokio::test]
async fn retention_deleting_rows_a_pass_is_sealing() {
    let r = repo();
    let ids = chain(&r, 9).await;
    let head = *ids.last().unwrap();
    let base = ids[0];
    let fired = Arc::new(Mutex::new(false));
    let (r2, f2) = (r.clone(), fired.clone());
    let mut o = opts(Mode::Build);
    o.pause =
        Some(PauseHook(Arc::new(move |point| {
            let (r, fired) = (r2.clone(), f2.clone());
            Box::pin(async move {
                if point != "content-v1:seal-stored"
                    || std::mem::replace(&mut *fired.lock().unwrap(), true)
                {
                    return;
                }
                assert!(r
                    .meta()
                    .cas_ref("main", Some(head), Some(base))
                    .await
                    .unwrap());
                let keep = RetentionOptions {
                    keep: Duration::ZERO,
                    skew: Duration::ZERO,
                    rest_factor: 0,
                    ..RetentionOptions::default()
                };
                let rep = r
                    .retire_search_rows(&keep, now() + 10, &mut RetentionState::default(), &|| {
                        Ok(())
                    })
                    .await
                    .unwrap();
                assert!(rep.deleted > 0, "{rep:?}");
            })
        })));
    let mut st = CompactState::default();
    settle(&r, &o, &mut st, None).await;
    assert!(*fired.lock().unwrap());
    // Values the head held are still answered, by the segments.
    let base_answers = answers(&r, base).await;
    let head_answers = answers(&r, head).await;
    let plain = repo();
    let pids = chain(&plain, 9).await;
    assert_eq!(head_answers, answers(&plain, pids[9]).await);
    assert_eq!(base_answers, answers(&plain, pids[0]).await);
    // Later passes see only the base live and settle.
    let o = CompactOptions {
        content_now: Some(now() + 10),
        pause: None,
        ..o
    };
    let mut st = CompactState::default();
    settle(&r, &o, &mut st, None).await;
    let l = live(&r).await;
    assert!(l.live(&r.root_tree(base).await.unwrap()));
}

/// A search storing a row (`put_derived_if_absent`) for a key whose row
/// was retired: the next pass compares it with the sealed value, equal by
/// the set it stands for, and deletes it again.
#[tokio::test]
async fn a_row_resurrected_for_a_sealed_key_converges() {
    let r = repo();
    chain(&r, 9).await;
    let mut st = CompactState::default();
    let hook = feature_hook(Arc::default());
    settle(&r, &opts(Mode::Retire), &mut st, Some(&hook)).await;
    let l = live(&r).await;
    let key = sealed(&r, COL_BLOB_TRIGRAMS)
        .await
        .into_iter()
        .find(|k| l.live(k))
        .unwrap();
    let data = r.read_content(&key).await.unwrap().unwrap();
    let whole = Bytes::from(BlobRow::of(&data).encode());
    let kept = r
        .meta()
        .put_derived_if_absent(BLOB_KIND, &key, whole.clone())
        .await
        .unwrap();
    assert_eq!(kept, whole, "no row was there");
    let report = settle(&r, &opts(Mode::Retire), &mut st, Some(&hook)).await;
    assert_eq!(
        (report.retired_rows, report.mismatched),
        (1, 0),
        "{report:?}"
    );
    assert!(r
        .row_meta()
        .get_derived(BLOB_KIND, &key)
        .await
        .unwrap()
        .is_none());
}

/// A derivation rewriting a row (as another right patch) between a pass's
/// compare and its delete: the delete is value-conditional, so the row
/// stays, and the next pass retires it.
#[tokio::test]
async fn a_row_rewritten_between_compare_and_delete_stays() {
    let inner = Arc::new(MemoryMetaStore::new());
    let racing = Arc::new(Racing {
        inner: inner.clone(),
        rewrite: Mutex::new(None),
    });
    let r = Repo::new(Arc::new(MemoryBlobStore::new()), racing.clone());
    chain(&r, 9).await;
    let mut st = CompactState::default();
    settle(&r, &opts(Mode::Build), &mut st, None).await;
    let l = live(&r).await;
    let all = rows(&r).await;
    let (key, patch) = all
        .iter()
        .find_map(|((kind, k), v)| {
            (*kind == BLOB_KIND
                && l.live(k)
                && matches!(StoredBlob::parse(v), Ok(StoredBlob::Patch(_))))
            .then(|| (*k, v.clone()))
        })
        .unwrap();
    let base = StoredBlob::parse(&patch).unwrap().base().unwrap();
    let whole = Bytes::from(
        BlobRow::of_stored(&patch, Some(&all[&(BLOB_KEYFRAME_KIND, base)]))
            .unwrap()
            .encode(),
    );
    *racing.rewrite.lock().unwrap() = Some((key, whole.clone()));
    let hook = feature_hook(Arc::default());
    let o = opts(Mode::Retire);
    pass(&r, &o, &mut st, Some(&hook)).await;
    assert!(racing.rewrite.lock().unwrap().is_none(), "the race fired");
    assert_eq!(
        inner.get_derived(BLOB_KIND, &key).await.unwrap(),
        Some(whole),
        "the rewritten row stays"
    );
    settle(&r, &o, &mut st, Some(&hook)).await;
    assert!(inner.get_derived(BLOB_KIND, &key).await.unwrap().is_none());
}

/// `repair drop --kind blob-trigrams-v2` (and the tree kind) clears both of
/// its columns, keyframes with patches, before deleting the rows; `verify`
/// and `repair rederive` see the keys only segments hold.
#[tokio::test]
async fn repair_and_verify_see_sealed_content_keys() {
    let r = repo();
    let ids = chain(&r, 9).await;
    let head = *ids.last().unwrap();
    let want = answers(&r, head).await;
    let mut st = CompactState::default();
    let hook = feature_hook(Arc::default());
    settle(&r, &opts(Mode::Retire), &mut st, Some(&hook)).await;
    let n_blob = sealed(&r, COL_BLOB_TRIGRAMS).await.len() as u64;
    let n_rows = rows(&r)
        .await
        .keys()
        .filter(|(k, _)| *k == BLOB_KIND)
        .count() as u64;
    let full = crate::integrity::VerifyOptions {
        full: true,
        derived: KINDS.to_vec(),
        ..crate::integrity::VerifyOptions::default()
    };
    let v = crate::integrity::verify(&r, &full, &crate::integrity::Quiet)
        .await
        .unwrap();
    assert!(v.ok(), "{:?}", v.problems);
    assert_eq!(
        v.derived[BLOB_KIND].checked,
        n_blob + n_rows,
        "{:?}",
        v.derived
    );
    assert!(v.derived[TREE_KIND].checked >= sealed(&r, COL_TREE_TRIGRAMS).await.len() as u64);
    // A wrong value sealed (a row written wrong, then sealed and retired):
    // verify finds it through the segment, and rederive writes a row,
    // which wins.
    let r2 = repo();
    let ids2 = chain(&r2, 3).await;
    let l = live(&r2).await;
    let victim = rows(&r2)
        .await
        .into_iter()
        .find(|((kind, k), v)| *kind == BLOB_KIND && l.live(k) && v.first() == Some(&1))
        .unwrap()
        .0
         .1;
    let wrong = Bytes::from(BlobRow::Trigrams(vec![5, 6, 7]).encode());
    r2.row_meta()
        .put_derived(BLOB_KIND, &victim, wrong)
        .await
        .unwrap();
    let mut st2 = CompactState::default();
    settle(&r2, &opts(Mode::Retire), &mut st2, Some(&hook)).await;
    assert!(r2
        .row_meta()
        .get_derived(BLOB_KIND, &victim)
        .await
        .unwrap()
        .is_none());
    let v = crate::integrity::verify(&r2, &full, &crate::integrity::Quiet)
        .await
        .unwrap();
    assert_eq!(v.derived[BLOB_KIND].mismatched, 1, "{:?}", v.problems);
    let opts_r = crate::integrity::RederiveOptions {
        kinds: vec![BLOB_KIND],
        scope: crate::integrity::Scope::All,
        dry_run: false,
        max_problems: 100,
        jobs: 1,
        missing_only: false,
    };
    crate::integrity::rederive(&r2, &opts_r, &crate::integrity::Quiet)
        .await
        .unwrap();
    assert!(r2
        .row_meta()
        .get_derived(BLOB_KIND, &victim)
        .await
        .unwrap()
        .is_some());
    assert!(
        crate::integrity::verify(&r2, &full, &crate::integrity::Quiet)
            .await
            .unwrap()
            .ok()
    );
    let _ = ids2;
    // Drop: both columns of each kind cleared, the rows gone, the answers
    // derived again the same.
    crate::integrity::drop_kinds(&r, &[BLOB_KIND, TREE_KIND], false, &crate::integrity::Quiet)
        .await
        .unwrap();
    for col in [
        COL_BLOB_TRIGRAMS,
        COL_BLOB_KEYFRAME,
        COL_TREE_TRIGRAMS,
        COL_TREE_KEYFRAME,
    ] {
        assert!(sealed(&r, col).await.is_empty(), "column {col}");
    }
    assert!(rows(&r).await.is_empty());
    assert_eq!(answers(&r, head).await, want);
}

/// Rewrites one row (`rewrite`) just before the first value-conditional
/// delete of `blob-trigrams-v2` rows: a derivation racing a pass.
struct Racing {
    inner: Arc<MemoryMetaStore>,
    rewrite: Mutex<Option<(Hash, Bytes)>>,
}

#[async_trait::async_trait]
impl MetaStore for Racing {
    async fn delete_derived_values(&self, kind: &str, rows: &[(Hash, Bytes)]) -> Result<u64> {
        if kind == BLOB_KIND {
            let fire = {
                let mut w = self.rewrite.lock().unwrap();
                match &*w {
                    Some((k, _)) if rows.iter().any(|(r, _)| r == k) => w.take(),
                    _ => None,
                }
            };
            if let Some((k, v)) = fire {
                self.inner.put_derived(kind, &k, v).await?;
            }
        }
        self.inner.delete_derived_values(kind, rows).await
    }
    async fn put_changeset(&self, cs: &Changeset) -> Result<ChangesetId> {
        self.inner.put_changeset(cs).await
    }
    async fn get_changeset(&self, id: &ChangesetId) -> Result<Option<Changeset>> {
        self.inner.get_changeset(id).await
    }
    async fn get_changesets(&self, ids: &[ChangesetId]) -> Result<Vec<Option<Changeset>>> {
        self.inner.get_changesets(ids).await
    }
    async fn changesets_stored_at(&self, ids: &[ChangesetId]) -> Result<Vec<Option<u64>>> {
        self.inner.changesets_stored_at(ids).await
    }
    async fn list_changesets(
        &self,
        after: Option<ChangesetId>,
        limit: usize,
    ) -> Result<Vec<ChangesetId>> {
        self.inner.list_changesets(after, limit).await
    }
    async fn get_ref(&self, name: &str) -> Result<Option<ChangesetId>> {
        self.inner.get_ref(name).await
    }
    async fn cas_ref(
        &self,
        name: &str,
        expected: Option<ChangesetId>,
        new: Option<ChangesetId>,
    ) -> Result<bool> {
        self.inner.cas_ref(name, expected, new).await
    }
    async fn cas_ref_logged(
        &self,
        name: &str,
        expected: Option<ChangesetId>,
        new: Option<ChangesetId>,
        ctx: &leep_core::RefUpdateContext,
    ) -> Result<bool> {
        self.inner.cas_ref_logged(name, expected, new, ctx).await
    }
    async fn ref_log(
        &self,
        name: Option<&str>,
        before_seq: Option<u64>,
        limit: usize,
    ) -> Result<Vec<leep_core::RefLogEntry>> {
        self.inner.ref_log(name, before_seq, limit).await
    }
    async fn list_refs(&self) -> Result<BTreeMap<String, ChangesetId>> {
        self.inner.list_refs().await
    }
    async fn get_derived(&self, kind: &str, key: &Hash) -> Result<Option<Bytes>> {
        self.inner.get_derived(kind, key).await
    }
    async fn get_derived_many(&self, kind: &str, keys: &[Hash]) -> Result<Vec<Option<Bytes>>> {
        self.inner.get_derived_many(kind, keys).await
    }
    async fn get_derived_kinds(
        &self,
        reads: &[(&str, &[Hash])],
    ) -> Result<Vec<Vec<Option<Bytes>>>> {
        self.inner.get_derived_kinds(reads).await
    }
    async fn derived_present(&self, kind: &str, keys: &[Hash]) -> Result<Vec<bool>> {
        self.inner.derived_present(kind, keys).await
    }
    async fn put_derived(&self, kind: &str, key: &Hash, value: Bytes) -> Result<()> {
        self.inner.put_derived(kind, key, value).await
    }
    async fn put_derived_if_absent(&self, kind: &str, key: &Hash, value: Bytes) -> Result<Bytes> {
        self.inner.put_derived_if_absent(kind, key, value).await
    }
    async fn put_derived_many(&self, rows: &[leep_core::DerivedRow]) -> Result<()> {
        self.inner.put_derived_many(rows).await
    }
    async fn put_changeset_with_derived(
        &self,
        cs: &Changeset,
        rows: &[leep_core::DerivedRow],
    ) -> Result<ChangesetId> {
        self.inner.put_changeset_with_derived(cs, rows).await
    }
    async fn list_derived(
        &self,
        kind: &str,
        after: Option<Hash>,
        limit: usize,
    ) -> Result<Vec<(Hash, Bytes)>> {
        self.inner.list_derived(kind, after, limit).await
    }
    async fn list_derived_keys(
        &self,
        kind: &str,
        after: Option<Hash>,
        limit: usize,
    ) -> Result<Vec<Hash>> {
        self.inner.list_derived_keys(kind, after, limit).await
    }
    async fn count_derived(&self, kind: &str) -> Result<u64> {
        self.inner.count_derived(kind).await
    }
    async fn estimate_derived(&self, kind: &str) -> Result<leep_core::RowCount> {
        self.inner.estimate_derived(kind).await
    }
    async fn delete_derived_keys(&self, kinds: &[&str], keys: &[Hash]) -> Result<u64> {
        self.inner.delete_derived_keys(kinds, keys).await
    }
    async fn cas_derived(
        &self,
        kind: &str,
        key: &Hash,
        expected: Option<&[u8]>,
        new: Bytes,
    ) -> Result<bool> {
        self.inner.cas_derived(kind, key, expected, new).await
    }
    fn supports_leases(&self) -> bool {
        self.inner.supports_leases()
    }
    async fn lease(
        &self,
        key: &leep_core::LeaseKey,
        timing: leep_core::LeaseTiming,
    ) -> Result<Option<leep_core::Lease>> {
        self.inner.lease(key, timing).await
    }
    async fn acquire_lease(
        &self,
        claim: &leep_core::LeaseClaim<'_>,
        timing: leep_core::LeaseTiming,
    ) -> Result<leep_core::Lease> {
        self.inner.acquire_lease(claim, timing).await
    }
    async fn renew_leases(
        &self,
        holder: &str,
        held: &[(leep_core::LeaseKey, u64)],
        timing: leep_core::LeaseTiming,
    ) -> Result<Vec<Option<Duration>>> {
        self.inner.renew_leases(holder, held, timing).await
    }
    async fn release_lease(
        &self,
        key: &leep_core::LeaseKey,
        holder: &str,
        epoch: u64,
    ) -> Result<bool> {
        self.inner.release_lease(key, holder, epoch).await
    }
}

/// A row marked to seal again whose row then goes (deleted here, as
/// retention or `repair drop` would) is forgotten at the next listing: it
/// does not make every later listing seal at once, below the thresholds.
#[tokio::test]
async fn a_stale_reseal_mark_does_not_bypass_the_seal_threshold() {
    let r = repo();
    let ids = chain(&r, 9).await;
    let head = *ids.last().unwrap();
    let mut st = CompactState::default();
    let hook = feature_hook(Arc::default());
    settle(&r, &opts(Mode::Retire), &mut st, Some(&hook)).await;
    let lease = acquire_for(&r, "test").await.unwrap();
    drop_columns(
        &r,
        &lease,
        FAMILY_CONTENT,
        &[COL_BLOB_KEYFRAME, COL_TREE_KEYFRAME],
    )
    .await
    .unwrap();
    crate::gc_lease::release(lease, Ok(())).await.unwrap();
    // A search writes whole rows for the patched blobs it probes.
    answers(&r, head).await;
    let restored: Vec<(&'static str, Hash)> = rows(&r).await.into_keys().collect();
    assert!(restored.iter().any(|(k, _)| *k == BLOB_KIND));
    let hold = CompactOptions {
        seal_rows: 1_000,
        idle_rows: 1_000,
        ..opts(Mode::Retire)
    };
    // This pass marks them to seal again (their sealed patches cannot be
    // resolved); then their rows go.
    pass(&r, &hold, &mut st, Some(&hook)).await;
    for (kind, key) in &restored {
        r.row_meta()
            .delete_derived_keys(&[kind], &[*key])
            .await
            .unwrap();
    }
    // A new land: a few unsealed rows, far below the thresholds.
    let one = Files::from([(
        "src/d3/f4.rs".to_string(),
        file("fn later() { fresh_token }\n".repeat(5)),
    )]);
    let id = commit_files(&r, &[head], &one).await;
    r.meta()
        .cas_ref("main", Some(head), Some(id))
        .await
        .unwrap();
    r.derive_search(id).await.unwrap();
    let hold = CompactOptions {
        content_now: Some(now() + 10),
        ..hold
    };
    for _ in 0..3 {
        let p = pass(&r, &hold, &mut st, Some(&hook)).await;
        let c = p.family(codec::FAMILY_CONTENT_NAME).cloned().unwrap();
        assert_eq!(c.sealed_segments, 0, "{c:?}");
    }
}

/// A blob held by two directories (and by two versions of a directory) is
/// numbered once, after the first directory that holds it, and is live.
#[tokio::test]
async fn a_file_shared_across_directories_is_numbered_once() {
    let r = repo();
    let body = file("fn shared() { same_bytes }\n".repeat(4));
    let mut files = Files::new();
    files.insert("a/x.rs".into(), body.clone());
    files.insert("b/y.rs".into(), body.clone());
    files.insert("b/z.rs".into(), file("fn z() {}\n".into()));
    let base = commit_files(&r, &[], &files).await;
    let next = commit_files(
        &r,
        &[base],
        &Files::from([("b/z.rs".to_string(), file("fn z2() {}\n".into()))]),
    )
    .await;
    r.meta().cas_ref("main", None, Some(base)).await.unwrap();
    r.meta()
        .cas_ref("main", Some(base), Some(next))
        .await
        .unwrap();
    let blob = Hash::of_blob(&body.0);
    for keep in [None, Some(Duration::from_secs(3600))] {
        let l = r.content_live(keep, Duration::ZERO, now()).await.unwrap();
        assert!(l.live(&blob));
        let root = r.root_tree(next).await.unwrap();
        let at = l.order(&blob).unwrap();
        assert!(at > l.order(&root).unwrap());
        // `a/` comes first in the walk: the shared file sits right after it.
        let node = r.read_tree(root).await.unwrap();
        let leep_core::TreeEntry::Dir { tree: a } = node.entries["a"] else {
            panic!()
        };
        assert_eq!(l.order(&a).unwrap() + 1, at);
    }
}
