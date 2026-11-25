//! Tests of ADR 0105: family `changeset-v2` (its column codecs, golden
//! bytes and damage), several families side by side (the compactor's
//! per-family state, GC, verify and the upgrade counts), and columns this
//! binary does not know.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use leep_core::memory::{MemoryBlobStore, MemoryMetaStore};
use leep_core::{ChangesetId, Hash};

use super::codec::{self, Manifest, SealRow, FAMILY_CHANGESET_V2};
use super::tests::{build, feature_hook, fresh, settle};
use super::*;
use crate::derived::{DerivedKind, FilterRow, Generation, LastMerge, LogFilterIndex, RootTree};
use crate::Repo;

fn repo() -> Repo {
    Repo::new(
        Arc::new(MemoryBlobStore::new()),
        Arc::new(MemoryMetaStore::new()),
    )
}

fn h(b: u8) -> Hash {
    Hash([b; 32])
}

const V2: [(u8, &str); 4] = [
    (COL_GENERATION, Generation::NAME),
    (COL_LAST_MERGE, LastMerge::NAME),
    (COL_ROOT_TREE, RootTree::NAME_STR),
    (COL_LOG_FILTER, LogFilterIndex::NAME),
];

/// The rows of family `changeset-v2` a store holds, ordered by generation.
async fn v2_rows(r: &Repo) -> Vec<SealRow> {
    let mut by: BTreeMap<Hash, Vec<(u8, Bytes)>> = BTreeMap::new();
    for (col, kind) in V2 {
        for (k, v) in r
            .row_meta()
            .list_derived(kind, None, usize::MAX)
            .await
            .unwrap()
        {
            by.entry(k).or_default().push((col, v));
        }
    }
    let mut out = Vec::new();
    for (key, values) in by {
        let order = r.generation(ChangesetId(key)).await.unwrap();
        out.push(SealRow { key, order, values });
    }
    out
}

fn read_back(enc: &codec::Encoded) -> BTreeMap<(Hash, u8), Bytes> {
    let idx = codec::decode_index(&enc.index.0, &enc.index.1).unwrap();
    let data = codec::decode_data(&idx, &enc.data.0, enc.data.1.clone()).unwrap();
    codec::read_all(&idx, &data)
        .unwrap()
        .into_iter()
        .map(|(k, c, v)| ((k, c), v))
        .collect()
}

/// Every value of every column of a real history comes back byte for byte
/// (all four columns compare bytes), whole or cut into small segments, and
/// the segment is far smaller than the rows.
#[tokio::test]
async fn changeset_v2_rows_of_real_histories_round_trip() {
    let r = repo();
    build(&r, None).await;
    let rows = v2_rows(&r).await;
    assert!(rows.len() > 50);
    assert!(rows.iter().filter(|s| s.values.len() == 4).count() > 10);
    let want: BTreeMap<(Hash, u8), Bytes> = rows
        .iter()
        .flat_map(|s| s.values.iter().map(|(c, v)| ((s.key, *c), v.clone())))
        .collect();
    let (enc, bad) = codec::encode(FAMILY_CHANGESET_V2, &rows, 5).unwrap();
    assert!(bad.is_empty(), "{bad:?}");
    assert_eq!(read_back(&enc), want);
    let idx = codec::decode_index(&enc.index.0, &enc.index.1).unwrap();
    assert_eq!(idx.family, FAMILY_CHANGESET_V2);
    assert_eq!(
        idx.column_mask(),
        codec::family(FAMILY_CHANGESET_V2).unwrap().mask()
    );
    for (_, col) in want.keys() {
        let fam = codec::family(FAMILY_CHANGESET_V2).unwrap();
        assert!(fam.column(*col).unwrap().equal.sealed_as(b"x", b"x"));
    }
    for chunk in rows.chunks(5) {
        let (enc, bad) = codec::encode(FAMILY_CHANGESET_V2, chunk, 2).unwrap();
        assert!(bad.is_empty());
        let part: BTreeMap<(Hash, u8), Bytes> = chunk
            .iter()
            .flat_map(|s| s.values.iter().map(|(c, v)| ((s.key, *c), v.clone())))
            .collect();
        assert_eq!(read_back(&enc), part);
    }
    let row_bytes: usize = want.values().map(|v| 32 + v.len()).sum();
    assert!(
        enc.index.1.len() + enc.data.1.len() < row_bytes * 3 / 4,
        "{} + {} against {row_bytes}",
        enc.index.1.len(),
        enc.data.1.len()
    );
}

/// Values that are not their kind's one encoding are never sealed: a
/// generation of seven bytes, a root tree of 31, a log filter that does not
/// decode, and one that decodes but with a varint that is not minimal (it
/// would come back as other bytes).
#[test]
fn changeset_v2_values_that_do_not_decode_are_not_sealed() {
    let good_filter = FilterRow {
        depth: 0,
        last_merge: 0,
        levels: vec![crate::derived::Level::of(vec![3, 9])],
        stops: vec![],
    }
    .encode();
    let mut long_varint = vec![0x80, 0x00];
    long_varint.extend_from_slice(&good_filter[1..]);
    assert!(FilterRow::decode(&long_varint).is_ok());
    let rows = vec![
        SealRow {
            key: h(1),
            order: 1,
            values: vec![
                (COL_GENERATION, Bytes::from_static(&[0; 7])),
                (COL_ROOT_TREE, Bytes::from_static(&[1; 31])),
                (COL_LOG_FILTER, good_filter.clone()),
            ],
        },
        SealRow {
            key: h(2),
            order: 2,
            values: vec![
                (COL_LAST_MERGE, Bytes::from_static(&[0; 8])),
                (COL_LOG_FILTER, Bytes::from(long_varint)),
            ],
        },
        SealRow {
            key: h(3),
            order: 3,
            values: vec![(COL_LOG_FILTER, Bytes::from_static(b"\x05junk"))],
        },
    ];
    let (enc, bad) = codec::encode(FAMILY_CHANGESET_V2, &rows, 1).unwrap();
    let mut bad: Vec<(Hash, u8)> = bad.iter().map(|b| (b.key, b.col)).collect();
    bad.sort();
    assert_eq!(
        bad,
        [
            (h(1), COL_GENERATION),
            (h(1), COL_ROOT_TREE),
            (h(2), COL_LOG_FILTER),
            (h(3), COL_LOG_FILTER)
        ]
    );
    let got = read_back(&enc);
    assert_eq!(got.len(), 2);
    assert_eq!(got[&(h(1), COL_LOG_FILTER)], good_filter);
    // A column of the other family is refused outright.
    assert!(codec::encode(
        FAMILY_CHANGESET_V2,
        &[SealRow {
            key: h(4),
            order: 1,
            values: vec![(7, Bytes::new())],
        }],
        1
    )
    .is_err());
}

fn golden_v2_rows() -> Vec<SealRow> {
    let filter = |depth: u64, stop: Option<u8>| {
        FilterRow {
            depth,
            last_merge: if stop.is_some() { 3 } else { 0 },
            levels: (0..crate::derived::level_count(depth))
                .map(|j| crate::derived::Level::of(vec![7 + j as u64, 1 << 40]))
                .collect(),
            stops: stop
                .map(|b| vec![Some((1, ChangesetId(h(b))))])
                .unwrap_or_default(),
        }
        .encode()
    };
    (1u8..=3)
        .map(|b| SealRow {
            key: h(b),
            order: b as u64,
            values: vec![
                (
                    COL_GENERATION,
                    Bytes::copy_from_slice(&(b as u64).to_be_bytes()),
                ),
                (
                    COL_LAST_MERGE,
                    Bytes::copy_from_slice(&(if b == 3 { 3u64 } else { 0 }).to_be_bytes()),
                ),
                (COL_ROOT_TREE, Bytes::copy_from_slice(&h(b + 10).0)),
                (COL_LOG_FILTER, filter(b as u64 - 1, (b == 3).then_some(9))),
            ],
        })
        .collect()
}

/// Golden bytes of `changeset-v2`: changing a column codec, a column number
/// or the family number orphans every stored segment of the family.
#[test]
fn changeset_v2_golden_bytes() {
    let (enc, bad) = codec::encode(
        FAMILY_CHANGESET_V2,
        &golden_v2_rows(),
        0x0102_0304_0506_0708,
    )
    .unwrap();
    assert!(bad.is_empty());
    let (ih, index) = &enc.index;
    let (dh, data) = &enc.data;
    assert_eq!(&index[..30], b"leep:derived-segment-index-v1\0");
    assert_eq!(&index[30..39], &[1, 1, 2, 3, 4, 5, 6, 7, 8]);
    // Family 2, three keys.
    assert_eq!(&index[39..41], &[2, 3]);
    // No id is named by reference: the stop's id 9 is external, nothing
    // else names a changeset.
    assert_eq!(data[38], 0);
    let idx = codec::decode_index(ih, index).unwrap();
    assert_eq!(
        idx.columns.iter().map(|c| c.id).collect::<Vec<_>>(),
        [1, 2, 3, 4]
    );
    let got = read_back(&enc);
    for s in golden_v2_rows() {
        for (c, v) in s.values {
            assert_eq!(got[&(s.key, c)], v);
        }
    }
    let m = Manifest {
        family: FAMILY_CHANGESET_V2,
        seq: 7,
        live: vec![enc.seg],
        retired: vec![],
    };
    let v = m.encode();
    assert_eq!(&v[..3], &[1, 0, 2]);
    assert_eq!(Manifest::decode(&v).unwrap(), m);
    assert_eq!(
        manifest_key(FAMILY_CHANGESET_V2),
        Hash::of_object("derived-segment-family", b"changeset-v2")
    );
    // A family-2 manifest under family 1's key is damage.
    assert_eq!(
        ManifestRead::of(FAMILY_CHANGESET, Some(&v)),
        ManifestRead::Damaged
    );
    assert_eq!(
        (ih.to_hex(), dh.to_hex(), Hash::of_blob(&v).to_hex()),
        (
            GOLDEN_V2_INDEX.to_string(),
            GOLDEN_V2_DATA.to_string(),
            GOLDEN_V2_MANIFEST.to_string()
        )
    );
}

const GOLDEN_V2_INDEX: &str = "74a53f8f3f8f5a1f67f6fdf5e5d98669feeb7a6648ec240347ebaa561ac9c321";
const GOLDEN_V2_DATA: &str = "ea823e4bd5944f46a6218b87f52d71ed37ac9e7b013670d9c2160636bf9e546e";
const GOLDEN_V2_MANIFEST: &str = "83c37a4788b8c2b681a3a6809cc9362dd78208507e7aecce6b4f6a18923f380d";

/// Every flipped bit of a `changeset-v2` segment's blobs is refused.
#[test]
fn changeset_v2_every_damaged_bit_is_refused() {
    let (enc, _) = codec::encode(FAMILY_CHANGESET_V2, &golden_v2_rows(), 9).unwrap();
    let (ih, index) = &enc.index;
    let (dh, data) = &enc.data;
    let idx = codec::decode_index(ih, index).unwrap();
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
}

/// Every column a family this binary knows has a slot in the compactor's
/// per-key table.
#[test]
fn known_columns_fit_the_row_slots() {
    for f in FAMILIES {
        for c in f.columns {
            assert!((c.id as usize) < compactor::SLOTS, "{} {}", f.name, c.id);
        }
        assert_eq!(codec::family_by_name(f.name), Some(f.id));
    }
}

/// `encoded`'s index with column `from` renamed `to`, at its new address:
/// a segment as a newer binary with another column would write it.
fn rename_column(enc: &codec::Encoded, from: u8, to: u8) -> (Hash, Bytes) {
    let index = &enc.index.1;
    let mut r = codec::Reader::new(index);
    r.take(30 + 1 + 8).unwrap();
    r.varint().unwrap();
    let n = r.varint().unwrap() as usize;
    r.take(32 * n).unwrap();
    for _ in 0..n {
        r.varint().unwrap();
    }
    let cols = r.varint().unwrap();
    for _ in 0..cols {
        let at = r.pos;
        let id = r.varint().unwrap();
        if id == from as u64 {
            let mut out = index.to_vec();
            out[at] = to;
            let out = Bytes::from(out);
            return (Hash::of_blob(&out), out);
        }
        r.take(n.div_ceil(8)).unwrap();
        for _ in 0..r.varint().unwrap() * 4 {
            r.varint().unwrap();
        }
    }
    panic!("no column {from}");
}

/// A column this binary does not know: `changeset-v1` reads it as damage
/// (as ADR 0104 binaries do); `changeset-v2` ignores it. The compactor
/// never merges such a segment, never drops it, and `repair drop` keeps
/// its bit; its known columns are read and retired like any.
#[tokio::test]
async fn unknown_columns_are_ignored_never_merged_and_never_dropped() {
    // changeset-v1: damage.
    let (enc1, _) = codec::encode(FAMILY_CHANGESET, &super::tests::golden_rows(), 3).unwrap();
    let (h1, b1) = rename_column(&enc1, COL_SKIPLIST, 9);
    assert!(codec::decode_index(&h1, &b1).is_err());

    // changeset-v2: a segment of a real history with its log filters in an
    // unknown column 9 (the last, so columns stay ascending), beside four ordinary segments.
    let r = repo();
    build(&r, None).await;
    let rows = v2_rows(&r).await;
    let (enc, _) = codec::encode(FAMILY_CHANGESET_V2, &rows[..20], 4).unwrap();
    let (ih, ib) = rename_column(&enc, COL_LOG_FILTER, 9);
    let idx = codec::decode_index(&ih, &ib).unwrap();
    assert!(idx.has_unknown_columns());
    let data = codec::decode_data(&idx, &enc.data.0, enc.data.1.clone()).unwrap();
    let all = codec::read_all(&idx, &data).unwrap();
    assert!(all.iter().all(|(_, c, _)| *c != 9 && *c != COL_LOG_FILTER));
    assert!(all.iter().any(|(_, c, _)| *c == COL_GENERATION));
    r.put_rescuing_many_loose(vec![enc.data.clone(), (ih, ib.clone())], &[])
        .await
        .unwrap();
    let mut seg = enc.seg;
    seg.index = ih;
    seg.columns = (seg.columns & !(1 << COL_LOG_FILTER)) | 1 << 9;
    let mut m = Manifest::new(FAMILY_CHANGESET_V2);
    m.live.push(seg);
    swap_manifest(&r, &None, None, &mut m).await.unwrap();

    // Many passes with merges due: the segment stays live, unmerged.
    let log = Arc::new(Mutex::new(Vec::new()));
    let hook = feature_hook(log.clone());
    let mut st = CompactState::default();
    settle(
        &r,
        &CompactOptions::small(Mode::Retire),
        &mut st,
        Some(&hook),
    )
    .await;
    let (_, ManifestRead::Read(m)) = read_manifest(&r, FAMILY_CHANGESET_V2).await.unwrap() else {
        panic!()
    };
    assert!(m.live.iter().any(|s| s.index == ih), "{m:?}");
    assert!(m.live.len() > 1);
    // Its generations were retired like any: they read from it.
    let f = fresh(&r);
    let k = rows[3].key;
    assert!(r
        .row_meta()
        .get_derived(Generation::NAME, &k)
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        f.meta().get_derived(Generation::NAME, &k).await.unwrap(),
        Some(Bytes::copy_from_slice(&rows[3].order.to_be_bytes()))
    );
    // `repair drop` of its known columns keeps it, for the unknown one.
    let lease = acquire_for(&r, "test").await.unwrap();
    drop_columns(
        &r,
        &lease,
        FAMILY_CHANGESET_V2,
        &[COL_GENERATION, COL_LAST_MERGE, COL_ROOT_TREE],
    )
    .await
    .unwrap();
    crate::gc_lease::release(lease, Ok(())).await.unwrap();
    let (_, ManifestRead::Read(m)) = read_manifest(&r, FAMILY_CHANGESET_V2).await.unwrap() else {
        panic!()
    };
    let kept = m.live.iter().find(|s| s.index == ih).expect("kept");
    assert_eq!(kept.columns, 1 << 9);
}

/// Each family records its own feature before its first delete, and a
/// failing family does not stop the other: a damaged `changeset-v2`
/// manifest on a store with its feature recorded fails the pass, while
/// `changeset-v1` still seals and retires.
#[tokio::test]
async fn families_are_independent() {
    let r = repo();
    let ids = build(&r, None).await;
    let log = Arc::new(Mutex::new(Vec::new()));
    let hook = feature_hook(log.clone());
    log.lock().unwrap().push(FEATURE_2);
    r.row_meta()
        .put_derived(
            MANIFEST_KIND,
            &manifest_key(FAMILY_CHANGESET_V2),
            Bytes::from_static(b"junk"),
        )
        .await
        .unwrap();
    let mut st = CompactState::default();
    let err = compact_pass(
        &r,
        &CompactOptions::small(Mode::Retire),
        &mut st,
        Some(&hook),
    )
    .await
    .expect_err("changeset-v2 refuses");
    assert!(err.to_string().contains("changeset-v2"), "{err}");
    let (_, ManifestRead::Read(m)) = read_manifest(&r, FAMILY_CHANGESET).await.unwrap() else {
        panic!("changeset-v1 sealed")
    };
    assert!(!m.live.is_empty());
    assert_eq!(*log.lock().unwrap(), [FEATURE_2, FEATURE]);
    assert!(r
        .row_meta()
        .list_derived(crate::derived::Skiplist::NAME, None, usize::MAX)
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        r.row_meta().count_derived(Generation::NAME).await.unwrap(),
        ids.len() as u64
    );
}

/// `verify --full` checks values of the sealed and retired kinds of both
/// families (it walks changesets and reads through the view), and the
/// upgrade plan counts their sealed keys, so a retired store is not seen
/// as missing rows.
#[tokio::test]
async fn verify_and_upgrade_see_sealed_keys_of_every_family() {
    use crate::integrity::{plan_upgrade_with, verify, PlanOptions, Quiet, VerifyOptions};
    let r = repo();
    let ids = build(&r, None).await;
    let hook = feature_hook(Arc::default());
    settle(
        &r,
        &CompactOptions::small(Mode::Retire),
        &mut CompactState::default(),
        Some(&hook),
    )
    .await;
    for (_, kind) in V2 {
        assert!(r
            .row_meta()
            .list_derived(kind, None, usize::MAX)
            .await
            .unwrap()
            .is_empty());
    }
    let opts = VerifyOptions {
        full: true,
        ..VerifyOptions::default()
    };
    let rep = verify(&fresh(&r), &opts, &Quiet).await.unwrap();
    assert!(rep.problems.is_empty(), "{:?}", rep.problems);
    for kind in [
        Generation::NAME,
        RootTree::NAME_STR,
        LogFilterIndex::NAME,
        crate::derived::Skiplist::NAME,
    ] {
        let checked = rep.derived.get(kind).map_or(0, |k| k.checked);
        assert!(checked as usize >= ids.len() / 2, "{kind}: {checked}");
    }
    for exact in [false, true] {
        let plan = plan_upgrade_with(
            &fresh(&r),
            PlanOptions {
                exact_counts: exact,
            },
            &Quiet,
        )
        .await
        .unwrap();
        for kind in [Generation::NAME, RootTree::NAME_STR] {
            let k = plan.current.iter().find(|k| k.kind == kind).unwrap();
            assert_eq!(k.missing, 0, "{kind} exact {exact}: {k:?}");
            assert_eq!(k.rows, ids.len() as u64, "{kind} exact {exact}");
        }
    }
}

/// Every key a live segment of `family` holds a value of `col` for, with
/// how many live segments hold it.
async fn holders(r: &Repo, family: u8, col: u8) -> BTreeMap<Hash, usize> {
    let (_, read) = read_manifest(r, family).await.unwrap();
    let ManifestRead::Read(m) = read else {
        return BTreeMap::new();
    };
    let mut out = BTreeMap::new();
    for s in m.live.iter().filter(|s| s.has(col)) {
        let idx = view::load_index(&**r.blobs(), family, s)
            .await
            .unwrap()
            .unwrap();
        for k in idx.keys_with(col) {
            *out.entry(*k).or_insert(0) += 1;
        }
    }
    out
}

/// Live segment indexes of `family`.
async fn live(r: &Repo, family: u8) -> Vec<codec::SegmentRef> {
    match read_manifest(r, family).await.unwrap().1 {
        ManifestRead::Read(m) => m.live,
        _ => Vec::new(),
    }
}

/// A merge leaves out changesets no longer stored, in both families,
/// although a sealed `changeset-v2` segment still answers their generation
/// (ADR 0104 §4.2; ADR 0105 §6).
#[tokio::test]
async fn merges_shed_swept_changesets_in_every_family() {
    let r = repo();
    let ids = build(&r, None).await;
    // A branch no ref keeps, its rows all derived.
    let mut tip = ids[10];
    let mut side = Vec::new();
    for i in 0..6 {
        let path = format!("side/{i}");
        tip = crate::tests::commit(&r, &[tip], &[(&path, Some("s"))], "side").await;
        side.push(tip);
    }
    crate::derived::derive::<crate::derived::HistoryTree>(&r, tip)
        .await
        .unwrap();
    crate::derived::derive::<LogFilterIndex>(&r, tip)
        .await
        .unwrap();
    // Sealed in many small segments, never merged, and retired.
    let hook = feature_hook(Arc::default());
    let mut st = CompactState::default();
    let small = CompactOptions {
        max_rows: 8,
        fanout: 64,
        ..CompactOptions::small(Mode::Retire)
    };
    settle(&r, &small, &mut st, Some(&hook)).await;
    for fam in [FAMILY_CHANGESET, FAMILY_CHANGESET_V2] {
        let col = codec::family(fam).unwrap().columns[0].id;
        let held = holders(&r, fam, col).await;
        assert!(side.iter().all(|s| held.contains_key(&s.0)), "{fam}");
    }
    // The changeset sweep deletes them, rows and all.
    let kinds: Vec<&str> = FAMILIES
        .iter()
        .flat_map(|f| f.columns.iter().map(|c| c.kind))
        .collect();
    r.row_meta().delete_changesets(&side, &kinds).await.unwrap();
    // Their generation still reads, from the segment.
    assert!(fresh(&r)
        .meta()
        .get_derived(Generation::NAME, &side[0].0)
        .await
        .unwrap()
        .is_some());
    // Merges in both families: no merged segment names them.
    let old: std::collections::HashSet<Hash> = live(&r, FAMILY_CHANGESET)
        .await
        .into_iter()
        .chain(live(&r, FAMILY_CHANGESET_V2).await)
        .map(|s| s.index)
        .collect();
    let merge = CompactOptions {
        max_rows: 4096,
        fanout: 2,
        ..CompactOptions::small(Mode::Retire)
    };
    let p = super::tests::pass(&r, &merge, &mut st, Some(&hook)).await;
    for name in [
        codec::FAMILY_CHANGESET_NAME,
        codec::FAMILY_CHANGESET_V2_NAME,
    ] {
        assert!(p.family(name).unwrap().merges > 0, "{name}: {p:?}");
    }
    for fam in [FAMILY_CHANGESET, FAMILY_CHANGESET_V2] {
        for s in live(&r, fam)
            .await
            .iter()
            .filter(|s| !old.contains(&s.index))
        {
            let idx = view::load_index(&**r.blobs(), fam, s)
                .await
                .unwrap()
                .unwrap();
            for id in &side {
                assert!(idx.keys().binary_search(&id.0).is_err(), "{fam}: {id}");
            }
        }
    }
}

/// The second one-way door (ADR 0105): a store with only
/// `derived-segments` recorded, under `retire` that does not name
/// `changeset-v2`, retires `changeset-v1` rows and neither records
/// `derived-segments-2` nor deletes a `changeset-v2` row. Naming the family
/// passes the door; so does a feature another server recorded.
#[tokio::test]
async fn retire_does_not_pass_a_new_familys_door_unless_named() {
    let r = repo();
    let ids = build(&r, None).await;
    let log = Arc::new(Mutex::new(vec![FEATURE]));
    let hook = feature_hook(log.clone());
    let unnamed = CompactOptions {
        retire_families: vec![codec::FAMILY_CHANGESET_NAME.to_string()],
        ..CompactOptions::small(Mode::Retire)
    };
    let mut st = CompactState::default();
    settle(&r, &unnamed, &mut st, Some(&hook)).await;
    assert!(!log.lock().unwrap().contains(&FEATURE_2), "{log:?}");
    assert_eq!(
        r.row_meta()
            .count_derived(crate::derived::Skiplist::NAME)
            .await
            .unwrap(),
        0
    );
    for (_, kind) in V2 {
        let n = r.row_meta().count_derived(kind).await.unwrap();
        assert!(n as usize >= ids.len() / 2, "{kind}: {n}");
    }
    assert_eq!(
        r.row_meta().count_derived(Generation::NAME).await.unwrap(),
        ids.len() as u64
    );
    // The family still seals (build is free), only no row goes.
    assert!(!live(&r, FAMILY_CHANGESET_V2).await.is_empty());
    // Named: the feature first, then the rows.
    let named = CompactOptions {
        retire_families: CompactOptions::parse_retire_families("changeset-v2").unwrap(),
        ..unnamed.clone()
    };
    settle(&r, &named, &mut st, Some(&hook)).await;
    assert!(log.lock().unwrap().contains(&FEATURE_2), "{log:?}");
    assert_eq!(
        r.row_meta().count_derived(Generation::NAME).await.unwrap(),
        0
    );
    // Another process, not naming it, goes on once the store has it.
    let r2 = repo();
    build(&r2, None).await;
    let log2 = Arc::new(Mutex::new(vec![FEATURE, FEATURE_2]));
    let hook2 = feature_hook(log2);
    settle(&r2, &unnamed, &mut CompactState::default(), Some(&hook2)).await;
    assert_eq!(
        r2.row_meta().count_derived(Generation::NAME).await.unwrap(),
        0
    );
    // The flag's parser.
    assert_eq!(
        CompactOptions::parse_retire_families("").unwrap(),
        ["changeset-v1"]
    );
    assert_eq!(
        CompactOptions::parse_retire_families("all").unwrap(),
        ["changeset-v1", "changeset-v2", "content-v1"]
    );
    assert_eq!(
        CompactOptions::parse_retire_families("content-v1").unwrap(),
        ["changeset-v1", "content-v1"]
    );
    assert!(CompactOptions::parse_retire_families("content-v9").is_err());
    assert_eq!(CompactOptions::default().retire_families, ["changeset-v1"]);
}

/// A `retire` server holding a family back deletes none of its rows, so it
/// replaces the family's damaged manifest like `build`; once it names the
/// family, or the store has its feature, it refuses.
#[tokio::test]
async fn a_held_back_familys_damaged_manifest_is_replaced() {
    let r = repo();
    build(&r, None).await;
    let log = Arc::new(Mutex::new(vec![FEATURE]));
    let hook = feature_hook(log.clone());
    let unnamed = CompactOptions {
        retire_families: vec![codec::FAMILY_CHANGESET_NAME.to_string()],
        ..CompactOptions::small(Mode::Retire)
    };
    let mut st = CompactState::default();
    settle(&r, &unnamed, &mut st, Some(&hook)).await;
    let junk = Bytes::from_static(b"junk");
    let damage = || async {
        r.row_meta()
            .put_derived(
                MANIFEST_KIND,
                &manifest_key(FAMILY_CHANGESET_V2),
                junk.clone(),
            )
            .await
            .unwrap();
    };
    damage().await;
    let p = super::tests::pass(&r, &unnamed, &mut st, Some(&hook)).await;
    let v2 = p.family(codec::FAMILY_CHANGESET_V2_NAME).unwrap();
    assert!(v2.damaged_manifest, "{p:?}");
    assert!(matches!(
        read_manifest(&r, FAMILY_CHANGESET_V2).await.unwrap().1,
        ManifestRead::Read(_)
    ));
    assert!(!log.lock().unwrap().contains(&FEATURE_2), "{log:?}");
    // Named: it would retire, so it refuses.
    let named = CompactOptions {
        retire_families: CompactOptions::parse_retire_families("all").unwrap(),
        ..unnamed.clone()
    };
    damage().await;
    let err = compact_pass(&r, &named, &mut st, Some(&hook))
        .await
        .expect_err("named");
    assert!(err.to_string().contains("not replaced"), "{err}");
    // The feature recorded elsewhere: refused unnamed too.
    log.lock().unwrap().push(FEATURE_2);
    let err = compact_pass(&r, &unnamed, &mut CompactState::default(), Some(&hook))
        .await
        .expect_err("recorded");
    assert!(err.to_string().contains("not replaced"), "{err}");
    assert_eq!(
        read_manifest(&r, FAMILY_CHANGESET_V2).await.unwrap().0,
        Some(junk.clone())
    );
}

/// A held-back family asks the store whether its feature is recorded at
/// most once per `FEATURE_RECHECK`, not every pass, and goes on once the
/// answer changes.
#[tokio::test(start_paused = true)]
async fn a_held_back_family_asks_for_its_feature_once_a_minute() {
    let r = repo();
    let ids = build(&r, None).await;
    settle(
        &r,
        &CompactOptions::small(Mode::Build),
        &mut CompactState::default(),
        None,
    )
    .await;
    let log = Arc::new(Mutex::new(vec![FEATURE]));
    let asked = Arc::new(Mutex::new(0usize));
    let inner = feature_hook(log.clone());
    let probe = inner.recorded.clone();
    let count = asked.clone();
    let hook = Features {
        record: inner.record.clone(),
        recorded: Arc::new(move |name| {
            if name == FEATURE_2 {
                *count.lock().unwrap() += 1;
            }
            probe(name)
        }),
    };
    let unnamed = CompactOptions {
        retire_families: vec![codec::FAMILY_CHANGESET_NAME.to_string()],
        ..CompactOptions::small(Mode::Retire)
    };
    let mut st = CompactState::default();
    for _ in 0..4 {
        super::tests::pass(&r, &unnamed, &mut st, Some(&hook)).await;
    }
    assert_eq!(*asked.lock().unwrap(), 1);
    // Another server passes the door: seen after the recheck interval.
    log.lock().unwrap().push(FEATURE_2);
    super::tests::pass(&r, &unnamed, &mut st, Some(&hook)).await;
    assert_eq!(
        r.row_meta().count_derived(Generation::NAME).await.unwrap(),
        ids.len() as u64
    );
    tokio::time::advance(compactor::FEATURE_RECHECK + std::time::Duration::from_secs(1)).await;
    settle(&r, &unnamed, &mut st, Some(&hook)).await;
    assert_eq!(*asked.lock().unwrap(), 2);
    assert_eq!(
        r.row_meta().count_derived(Generation::NAME).await.unwrap(),
        0
    );
}

/// Rows of a segment whose blob carries a blob GC condemnation are not
/// retired, and the condemnations are read again after the family's
/// feature is recorded: one written while it is recorded still holds.
#[tokio::test]
async fn condemned_segments_rows_are_not_retired() {
    let r = repo();
    let ids = build(&r, None).await;
    settle(
        &r,
        &CompactOptions::small(Mode::Build),
        &mut CompactState::default(),
        None,
    )
    .await;
    let blobs: Vec<Hash> = live(&r, FAMILY_CHANGESET_V2)
        .await
        .iter()
        .map(|s| s.data)
        .collect();
    assert!(!blobs.is_empty());
    // An older `gc` condemns the segments just as the feature is recorded.
    let log = Arc::new(Mutex::new(vec![FEATURE]));
    let inner = feature_hook(log.clone());
    let record = inner.record.clone();
    let (gc_repo, marks) = (r.clone(), blobs.clone());
    let hook = Features {
        record: Arc::new(move |name| {
            let (gc_repo, marks, done) = (gc_repo.clone(), marks.clone(), record(name));
            Box::pin(async move {
                for h in marks.iter().filter(|_| name == FEATURE_2) {
                    gc_repo
                        .row_meta()
                        .put_derived(crate::gc::BLOB_CONDEMNED, h, crate::gc::time_value(1))
                        .await?;
                }
                done.await
            })
        }),
        recorded: inner.recorded.clone(),
    };
    let all = CompactOptions {
        retire_families: CompactOptions::parse_retire_families("all").unwrap(),
        ..CompactOptions::small(Mode::Retire)
    };
    let mut st = CompactState::default();
    settle(&r, &all, &mut st, Some(&hook)).await;
    assert!(log.lock().unwrap().contains(&FEATURE_2), "{log:?}");
    assert_eq!(
        r.row_meta().count_derived(Generation::NAME).await.unwrap(),
        ids.len() as u64
    );
    // Collected or cleared: the rows go.
    r.row_meta()
        .delete_derived_keys(&[crate::gc::BLOB_CONDEMNED], &blobs)
        .await
        .unwrap();
    settle(&r, &all, &mut st, Some(&hook)).await;
    assert_eq!(
        r.row_meta().count_derived(Generation::NAME).await.unwrap(),
        0
    );
}

/// A transient error reading one family's index blob does not keep the
/// other family's manifest from being applied, nor the family's other
/// segments from being used.
#[tokio::test]
async fn one_familys_failing_index_does_not_freeze_the_other() {
    let blobs: Arc<dyn leep_core::BlobStore> = Arc::new(MemoryBlobStore::new());
    let flaky = Arc::new(super::tests::Flaky {
        inner: blobs.clone(),
        failing: Mutex::default(),
    });
    let meta: Arc<dyn leep_core::MetaStore> = Arc::new(MemoryMetaStore::new());
    let r = Repo::new(blobs.clone(), meta.clone());
    build(&r, None).await;
    settle(
        &r,
        &CompactOptions::small(Mode::Build),
        &mut CompactState::default(),
        None,
    )
    .await;
    let v1 = live(&r, FAMILY_CHANGESET).await;
    flaky.failing.lock().unwrap().insert(v1[0].index);
    let reader = Repo::new(flaky.clone(), meta.clone());
    for single in [false, true] {
        let segs = reader.segments();
        let got = if single {
            segs.refresh_singly(&**reader.row_meta()).await
        } else {
            segs.refresh(&**reader.row_meta()).await
        };
        got.unwrap();
        assert_eq!(
            segs.family(FAMILY_CHANGESET_V2).segments().len(),
            live(&r, FAMILY_CHANGESET_V2).await.len(),
            "single {single}"
        );
        assert_eq!(
            segs.family(FAMILY_CHANGESET).segments().len(),
            v1.len() - 1,
            "single {single}"
        );
    }
}

/// `repair drop` of one kind while its family's manifest does not decode
/// would empty the whole family: it is refused unless every kind of the
/// family is named, and nothing is deleted.
#[tokio::test]
async fn repair_drop_with_a_damaged_manifest_needs_every_kind_of_the_family() {
    let r = repo();
    let ids = build(&r, None).await;
    settle(
        &r,
        &CompactOptions::small(Mode::Build),
        &mut CompactState::default(),
        None,
    )
    .await;
    r.row_meta()
        .put_derived(
            MANIFEST_KIND,
            &manifest_key(FAMILY_CHANGESET_V2),
            Bytes::from_static(b"junk"),
        )
        .await
        .unwrap();
    let q = crate::integrity::Quiet;
    let err = crate::integrity::drop_kinds(&r, &[Generation::NAME], false, &q)
        .await
        .expect_err("one kind");
    assert!(err.to_string().contains("drop them together"), "{err}");
    assert_eq!(
        r.row_meta().count_derived(Generation::NAME).await.unwrap(),
        ids.len() as u64
    );
    let all: Vec<&str> = V2.iter().map(|(_, k)| *k).collect();
    crate::integrity::drop_kinds(&r, &all, false, &q)
        .await
        .unwrap();
    assert!(matches!(
        read_manifest(&r, FAMILY_CHANGESET_V2).await.unwrap().1,
        ManifestRead::Read(m) if m.live.is_empty()
    ));
}

/// `changeset-v1` seals and merges with generations that exist only in
/// `changeset-v2` segments; and when that family's manifest is damaged or
/// its segment lost, from derivations.
#[tokio::test]
async fn changeset_v1_bootstraps_from_sealed_or_lost_generations() {
    for case in ["sealed", "damaged", "lost"] {
        let r = repo();
        let ids = build(&r, None).await;
        let hook = feature_hook(Arc::default());
        let mut st = CompactState::default();
        settle(
            &r,
            &CompactOptions::small(Mode::Retire),
            &mut st,
            Some(&hook),
        )
        .await;
        assert_eq!(
            r.row_meta().count_derived(Generation::NAME).await.unwrap(),
            0
        );
        // changeset-v1 starts over: no segment, its rows derived again.
        let lease = acquire_for(&r, "test").await.unwrap();
        drop_columns(
            &r,
            &lease,
            FAMILY_CHANGESET,
            &[COL_SKIPLIST, COL_HISTORY_TREE],
        )
        .await
        .unwrap();
        crate::gc_lease::release(lease, Ok(())).await.unwrap();
        match case {
            "damaged" => r
                .row_meta()
                .put_derived(
                    MANIFEST_KIND,
                    &manifest_key(FAMILY_CHANGESET_V2),
                    Bytes::from_static(b"junk"),
                )
                .await
                .unwrap(),
            "lost" => {
                for s in live(&r, FAMILY_CHANGESET_V2).await {
                    r.blobs().delete(&s.data).await.unwrap();
                }
            }
            _ => {}
        }
        let f = fresh(&r);
        for id in &ids {
            crate::derived::derive::<crate::derived::Skiplist>(&f, *id)
                .await
                .unwrap();
            crate::derived::derive::<crate::derived::HistoryTree>(&f, *id)
                .await
                .unwrap();
        }
        let f = fresh(&r);
        // Sealed in pages of 8 first, then merged.
        let pages = CompactOptions {
            max_rows: 8,
            fanout: 64,
            ..CompactOptions::small(Mode::Build)
        };
        let merging = CompactOptions::small(Mode::Build);
        let mut st = CompactState::default();
        let mut merges = 0;
        for i in 0..20 {
            let opts = if i < 10 { &pages } else { &merging };
            let p = match compact_pass(&f, opts, &mut st, Some(&hook)).await {
                Ok(Some(p)) => p,
                // A damaged changeset-v2 manifest on a store with its
                // feature fails that family's pass; changeset-v1 goes on.
                Err(e) if case == "damaged" => {
                    assert!(e.to_string().contains("changeset-v2"), "{e}");
                    let held = holders(&f, FAMILY_CHANGESET, COL_SKIPLIST).await;
                    if held.len() == ids.len() {
                        break;
                    }
                    continue;
                }
                other => panic!("{case}: {other:?}"),
            };
            merges += p.family(codec::FAMILY_CHANGESET_NAME).unwrap().merges;
            if i >= 10 && !p.more && p.sealed_segments + p.merges == 0 {
                break;
            }
        }
        let held = holders(&f, FAMILY_CHANGESET, COL_SKIPLIST).await;
        assert_eq!(held.len(), ids.len(), "{case}");
        if case != "damaged" {
            assert!(merges > 0, "{case}");
        }
        // The values read as a row-only store's would.
        let plain = repo();
        build(&plain, None).await;
        assert_eq!(
            super::tests::values(&f, &ids).await[..2 * ids.len()],
            super::tests::values(&plain, &ids).await[..2 * ids.len()],
            "{case}"
        );
    }
}

/// Two processes alternating passes, each with its own carried state per
/// family (small pages, so backfills carry): every key of both families is
/// sealed, no value is held by two live segments of a family, and values
/// read as rows did.
#[tokio::test]
async fn two_processes_alternating_passes_seal_each_key_once() {
    let r = repo();
    let ids = build(&r, None).await;
    let want = super::tests::values(&r, &ids).await;
    let a = r.clone();
    let b = fresh(&r);
    let opts = CompactOptions {
        max_rows: 8,
        max_seal_pages: 1,
        fanout: 64,
        list_every: std::time::Duration::from_secs(600),
        ..CompactOptions::small(Mode::Build)
    };
    let (mut sa, mut sb) = (CompactState::default(), CompactState::default());
    for i in 0..80 {
        let (repo, st) = if i % 2 == 0 {
            (&a, &mut sa)
        } else {
            (&b, &mut sb)
        };
        super::tests::pass(repo, &opts, st, None).await;
    }
    for fam in FAMILIES
        .iter()
        .filter(|f| f.keys == super::Keys::Changesets)
    {
        for c in fam
            .columns
            .iter()
            .filter(|c| [COL_SKIPLIST, COL_GENERATION].contains(&c.id))
        {
            let held = holders(&r, fam.id, c.id).await;
            assert_eq!(held.len(), ids.len(), "{} {}", fam.name, c.kind);
            assert!(held.values().all(|n| *n == 1), "{} {}", fam.name, c.kind);
        }
    }
    assert_eq!(super::tests::values(&fresh(&r), &ids).await, want);
}

/// A column this binary does not know, in a family that allows them, is not
/// parsed at all: its blocks may be framed otherwise (here they are not
/// zstd), and the segment still reads.
#[test]
fn an_unknown_columns_blocks_are_not_parsed() {
    let (enc, _) = codec::encode(FAMILY_CHANGESET_V2, &golden_v2_rows(), 5).unwrap();
    let (_, renamed) = rename_column(&enc, COL_LOG_FILTER, 9);
    let idx = codec::decode_index(&Hash::of_blob(&renamed), &renamed).unwrap();
    let col = idx.columns.iter().find(|c| c.id == 9).unwrap();
    let mut data = enc.data.1.to_vec();
    for b in &col.blocks {
        for x in &mut data[b.offset as usize..(b.offset + b.len as u64) as usize] {
            *x = 0xff;
        }
    }
    let data_hash = Hash::of_blob(&data);
    // The index ends with the data blob's address and its length.
    let mut index = renamed.to_vec();
    let mut len = Vec::new();
    codec::put_varint(&mut len, data.len() as u64);
    let at = index.len() - len.len() - 32;
    index[at..at + 32].copy_from_slice(&data_hash.0);
    let ih = Hash::of_blob(&index);
    let idx = codec::decode_index(&ih, &index).unwrap();
    let d = codec::decode_data(&idx, &data_hash, Bytes::from(data)).unwrap();
    let all = codec::read_all(&idx, &d).unwrap();
    assert_eq!(all.len(), 9, "{all:?}");
}
