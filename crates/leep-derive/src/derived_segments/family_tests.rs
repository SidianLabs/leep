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
        assert!((fam.column(*col).unwrap().equal)(b"x", b"x"));
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
