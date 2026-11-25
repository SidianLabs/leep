//! Tests of derived segments (ADR 0104, 0105): the codec, the read path,
//! the compactor over every family, and that a store whose rows were sealed
//! and retired answers every query exactly as the row-only store does.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::future::BoxFuture;
use leep_core::memory::{MemoryBlobStore, MemoryMetaStore};
use leep_core::{ChangesetId, Error, Hash, MetaStore};

use super::codec::{self, Manifest, SealRow, SegmentRef, COL_HISTORY_TREE, COL_SKIPLIST};
use super::compactor::Pass;
use super::*;
use crate::derived::history_pack::same_part;
use crate::derived::{
    derive, DerivedKind, Generation, HistoryTree, LastMerge, LogFilterIndex, RootTree, Skiplist,
};
use crate::tests::commit;
use crate::Repo;

fn h(b: u8) -> Hash {
    Hash([b; 32])
}

/// The `changeset-v1` part of a pass's report: the tests written for ADR
/// 0104 count that family's segments.
fn v1(p: &PassReport) -> PassReport {
    p.family(codec::FAMILY_CHANGESET_NAME).cloned().unwrap()
}

/// The `changeset-v2` part of a pass's report.
fn v2(p: &PassReport) -> PassReport {
    p.family(codec::FAMILY_CHANGESET_V2_NAME).cloned().unwrap()
}

/// The kinds of family `changeset-v2`, in column order.
const V2_KINDS: [&str; 4] = [
    Generation::NAME,
    LastMerge::NAME,
    RootTree::NAME_STR,
    LogFilterIndex::NAME,
];

/// A small deterministic PRNG (xorshift), so failures reproduce.
pub(crate) struct Rng(pub(crate) u64);

impl Rng {
    pub(crate) fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    pub(crate) fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
}

fn repo() -> Repo {
    Repo::new(
        Arc::new(MemoryBlobStore::new()),
        Arc::new(MemoryMetaStore::new()),
    )
}

/// A handle on the same stores with nothing cached: a new process.
pub(crate) fn fresh(r: &Repo) -> Repo {
    Repo::new(r.blobs().clone(), r.meta().clone())
}

type Each<'a> = &'a mut (dyn FnMut() -> BoxFuture<'static, ()> + Send);

/// A history with a linear stretch, side branches merged back, deletions
/// and a criss-cross (two branches merging each other). Runs `each` after
/// every few changesets, with history-tree-v4 and log-filter-v1 derived up
/// to the newest (as the warmer does).
/// Deterministic: the same calls build the same ids.
pub(crate) async fn build(r: &Repo, mut each: Option<Each<'_>>) -> Vec<ChangesetId> {
    let mut all = Vec::new();
    let mut head = commit(
        r,
        &[],
        &[("a/x0", Some("root")), ("b/y", Some("y"))],
        "root",
    )
    .await;
    all.push(head);
    for i in 0..36u32 {
        let f = format!("a/x{}", i % 4);
        let t = format!("v{i}");
        let next = if i % 9 == 4 {
            let mut side = head;
            for j in 0..3 {
                let sf = format!("s/{i}/{j}");
                side = commit(r, &[side], &[(&sf, Some("s"))], &format!("side {i} {j}")).await;
                all.push(side);
            }
            commit(r, &[head, side], &[(&f, Some(&t))], &format!("merge {i}")).await
        } else if i == 20 {
            let p = commit(r, &[head], &[("c/p", Some("p"))], "p").await;
            let q = commit(r, &[head], &[("c/q", Some("q"))], "q").await;
            let pq = commit(r, &[p, q], &[("c/p", Some("pq"))], "pq").await;
            let qp = commit(r, &[q, p], &[("c/q", Some("qp"))], "qp").await;
            all.extend([p, q, pq, qp]);
            commit(r, &[pq, qp], &[(&f, Some(&t))], "cc").await
        } else if i % 7 == 6 {
            commit(
                r,
                &[head],
                &[(&f, None), ("b/y", Some(&t))],
                &format!("del {i}"),
            )
            .await
        } else {
            commit(r, &[head], &[(&f, Some(&t))], &format!("edit {i}")).await
        };
        head = next;
        all.push(head);
        if i % 3 == 2 {
            derive::<HistoryTree>(r, head).await.unwrap();
            derive::<LogFilterIndex>(r, head).await.unwrap();
            if let Some(f) = each.as_mut() {
                f().await;
            }
        }
    }
    derive::<HistoryTree>(r, head).await.unwrap();
    derive::<LogFilterIndex>(r, head).await.unwrap();
    all
}

/// Every row of `kind`, from the rows themselves.
async fn rows(r: &Repo, kind: &str) -> BTreeMap<Hash, Bytes> {
    r.row_meta()
        .list_derived(kind, None, usize::MAX)
        .await
        .unwrap()
        .into_iter()
        .collect()
}

async fn seal_rows(r: &Repo) -> Vec<SealRow> {
    let skips = rows(r, Skiplist::NAME).await;
    let hist = rows(r, HistoryTree::NAME).await;
    let mut out = Vec::new();
    for (k, v) in &skips {
        let g = r.generation(ChangesetId(*k)).await.unwrap();
        let mut values = vec![(COL_SKIPLIST, v.clone())];
        if let Some(hv) = hist.get(k) {
            values.push((COL_HISTORY_TREE, hv.clone()));
        }
        out.push(SealRow {
            key: *k,
            order: g,
            values,
        });
    }
    out
}

fn decode_both(enc: &codec::Encoded) -> (codec::SegmentIndex, codec::SegmentData) {
    let idx = codec::decode_index(&enc.index.0, &enc.index.1).unwrap();
    let data = codec::decode_data(&idx, &enc.data.0, enc.data.1.clone()).unwrap();
    (idx, data)
}

/// Skiplists byte for byte; history parts by ADR 0045's definition, and
/// given back uncompressed.
fn same_value(col: u8, input: &[u8], got: &[u8]) -> bool {
    if col == COL_SKIPLIST {
        input == got
    } else {
        same_part(input, got) && got[8] == 0
    }
}

fn check_round_trip(rows: &[SealRow], enc: &codec::Encoded) {
    let (idx, data) = decode_both(enc);
    let got = codec::read_all(&idx, &data).unwrap();
    let mut want: BTreeMap<(Hash, u8), Bytes> = BTreeMap::new();
    for r in rows {
        for (c, v) in &r.values {
            want.insert((r.key, *c), v.clone());
        }
    }
    assert_eq!(got.len(), want.len());
    for (k, c, v) in got {
        assert!(same_value(c, &want[&(k, c)], &v), "{k} column {c}");
        assert!(idx.locate(&k, c).is_some());
    }
    assert_eq!(enc.seg.rows as usize, rows.len());
}

#[tokio::test]
async fn every_row_of_real_histories_round_trips() {
    let r = repo();
    build(&r, None).await;
    let rows = seal_rows(&r).await;
    assert!(rows.len() > 50);
    assert!(rows.iter().filter(|r| r.values.len() == 2).count() > 50);
    let (enc, bad) = codec::encode(codec::FAMILY_CHANGESET, &rows, 7).unwrap();
    assert!(bad.is_empty(), "{bad:?}");
    check_round_trip(&rows, &enc);
    // Several blocks per column (tests use small blocks).
    let (idx, _) = decode_both(&enc);
    assert!(idx.columns.iter().all(|c| c.blocks.len() > 1));
    assert_eq!(idx.column_mask(), 1 << COL_SKIPLIST | 1 << COL_HISTORY_TREE);
    // Any part of the rows on its own, so most references are external.
    for chunk in rows.chunks(7) {
        let (enc, _) = codec::encode(codec::FAMILY_CHANGESET, chunk, 1).unwrap();
        check_round_trip(chunk, &enc);
    }
    // A segment is far smaller than its rows.
    let row_bytes: usize = rows
        .iter()
        .flat_map(|r| r.values.iter().map(|(_, v)| 32 + v.len()))
        .sum();
    assert!(
        enc.data.1.len() + enc.index.1.len() < row_bytes / 2,
        "{} + {} against {row_bytes}",
        enc.data.1.len(),
        enc.index.1.len()
    );
}

/// decode(encode(rows)) is the rows for arbitrary orders (generations that
/// say nothing of ancestry), ids inside and outside the segment, keys with
/// one column or the other, and empty skiplists.
#[tokio::test]
async fn arbitrary_rows_round_trip() {
    let r = repo();
    build(&r, None).await;
    let real = seal_rows(&r).await;
    let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
    for round in 0..60u64 {
        let mut rows: Vec<SealRow> = Vec::new();
        for s in &real {
            if rng.below(3) == 0 {
                continue;
            }
            let mut values = s.values.clone();
            if values.len() == 2 && rng.below(4) == 0 {
                values.remove(rng.below(2) as usize);
            }
            rows.push(SealRow {
                key: s.key,
                order: rng.below(40),
                values,
            });
        }
        for i in 0..rng.below(20) {
            let mut key = [0u8; 32];
            key[..8].copy_from_slice(&rng.next().to_be_bytes());
            key[31] = round as u8;
            let n = rng.below(12);
            let mut g = 1u64 << 40;
            let mut list = Vec::new();
            for _ in 0..n {
                let id = match rng.below(3) {
                    0 if !rows.is_empty() => rows[rng.below(rows.len() as u64) as usize].key,
                    _ => {
                        let mut b = [0u8; 32];
                        b[..8].copy_from_slice(&rng.next().to_be_bytes());
                        Hash(b)
                    }
                };
                if id == Hash(key) {
                    continue;
                }
                list.push(crate::derived::SkipEntry {
                    id: ChangesetId(id),
                    generation: g,
                });
                g -= 1 + rng.below(1 << (i % 30));
            }
            rows.push(SealRow {
                key: Hash(key),
                order: rng.next(),
                values: vec![(COL_SKIPLIST, Skiplist::encode(&list))],
            });
        }
        let (enc, bad) = codec::encode(codec::FAMILY_CHANGESET, &rows, rng.next()).unwrap();
        assert!(bad.is_empty(), "round {round}: {bad:?}");
        check_round_trip(&rows, &enc);
    }
}

/// Values that do not decode never go into a segment.
#[tokio::test]
async fn damaged_values_are_not_sealed() {
    let r = repo();
    build(&r, None).await;
    let mut rows = seal_rows(&r).await;
    rows[3].values[0].1 = Bytes::from_static(&[1, 2, 3]);
    let hist = rows.iter().position(|r| r.values.len() == 2).unwrap();
    let mut v = rows[hist].values[1].1.to_vec();
    let last = v.len() - 1;
    v[last] ^= 1;
    rows[hist].values[1].1 = v.into();
    let (enc, bad) = codec::encode(codec::FAMILY_CHANGESET, &rows, 3).unwrap();
    assert_eq!(bad.len(), 2, "{bad:?}");
    let (idx, _) = decode_both(&enc);
    assert!(!idx.has(&rows[3].key, COL_SKIPLIST));
    assert!(!idx.has(&rows[hist].key, COL_HISTORY_TREE));
    assert!(idx.has(&rows[hist].key, COL_SKIPLIST));
    // A skiplist naming its own changeset is damage too.
    let own = SealRow {
        key: h(9),
        order: 3,
        values: vec![(
            COL_SKIPLIST,
            Skiplist::encode(&[crate::derived::SkipEntry {
                id: ChangesetId(h(9)),
                generation: 2,
            }]),
        )],
    };
    let (_, bad) = codec::encode(codec::FAMILY_CHANGESET, &[own], 3).unwrap();
    assert_eq!(bad.len(), 1);
}

pub(crate) fn golden_rows() -> Vec<SealRow> {
    let e = |b: u8, g: u64| crate::derived::SkipEntry {
        id: ChangesetId(h(b)),
        generation: g,
    };
    vec![
        SealRow {
            key: h(1),
            order: 1,
            values: vec![(COL_SKIPLIST, Bytes::new())],
        },
        SealRow {
            key: h(2),
            order: 2,
            values: vec![(COL_SKIPLIST, Skiplist::encode(&[e(1, 1)]))],
        },
        SealRow {
            key: h(3),
            order: 3,
            values: vec![(COL_SKIPLIST, Skiplist::encode(&[e(2, 2), e(7, 1)]))],
        },
    ]
}

/// Golden bytes: changing the encoding, a prefix or a column number
/// orphans every stored segment, so it must fail here first.
#[test]
fn golden_bytes() {
    let (enc, bad) = codec::encode(
        codec::FAMILY_CHANGESET,
        &golden_rows(),
        0x0102_0304_0506_0708,
    )
    .unwrap();
    assert!(bad.is_empty());
    let (ih, index) = &enc.index;
    let (dh, data) = &enc.data;
    assert_eq!(&index[..30], b"leep:derived-segment-index-v1\0");
    assert_eq!(&data[..29], b"leep:derived-segment-data-v1\0");
    assert_eq!(&index[30..39], &[1, 1, 2, 3, 4, 5, 6, 7, 8]);
    assert_eq!(&data[29..38], &[1, 1, 2, 3, 4, 5, 6, 7, 8]);
    // Family 1, three keys, ascending; ordinals 0, 1, 2.
    assert_eq!(&index[39..41], &[1, 3]);
    assert_eq!(&index[41..73], &[1u8; 32]);
    assert_eq!(&index[137..140], &[0, 1, 2]);
    // One external id: 7, named by key 3's second entry.
    assert_eq!(data[38], 1);
    assert_eq!(&data[39..71], &[7u8; 32]);
    assert!(leep_core::hash::is_object_preimage(index));
    assert!(leep_core::hash::is_object_preimage(data));
    assert_eq!(*ih, Hash::of_object(codec::INDEX_KIND, &index[30..]));
    assert_eq!(*dh, Hash::of_object(codec::DATA_KIND, &data[29..]));
    assert!(codec::is_segment_blob(ih, index));
    assert!(codec::is_segment_blob(dh, data));
    assert!(!codec::is_segment_blob(&Hash::of_blob(b"x"), b"x"));
    // Key 3's skiplist row: two entries, key 2 one back (0) and 7 the
    // first external id (1), generation 2 then 2 - 1.
    let (idx, d) = decode_both(&enc);
    let all = codec::read_all(&idx, &d).unwrap();
    assert_eq!(all.len(), 3);
    for (k, c, v) in all {
        let want = golden_rows()
            .into_iter()
            .find(|r| r.key == k)
            .unwrap()
            .values[0]
            .1
            .clone();
        assert_eq!((c, v), (COL_SKIPLIST, want));
    }
    let m = Manifest {
        family: codec::FAMILY_CHANGESET,
        seq: 7,
        live: vec![enc.seg],
        retired: vec![h(9)],
    };
    let v = m.encode();
    assert_eq!(&v[..15], &[1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 7, 0, 0, 0, 1]);
    assert_eq!(v.len(), 3 + 8 + 4 + 80 + 4 + 32 + 32);
    assert_eq!(Manifest::decode(&v).unwrap(), m);
    assert_eq!(
        manifest_key(FAMILY_CHANGESET),
        Hash::of_object("derived-segment-family", b"changeset-v1")
    );
    assert_eq!(
        (ih.to_hex(), dh.to_hex(), Hash::of_blob(&v).to_hex()),
        (
            GOLDEN_INDEX.to_string(),
            GOLDEN_DATA.to_string(),
            GOLDEN_MANIFEST.to_string()
        )
    );
}

const GOLDEN_INDEX: &str = "1879b9d8ae9d69329ad1a98661dcf422e8d8f81e9b1d3c656e39c4df84b74dff";
const GOLDEN_DATA: &str = "7a9e9903bd91483e6abb114a609642e806682c61158744b0334159a443605703";
const GOLDEN_MANIFEST: &str = "3c79193c20e1e1d114bcd4661f87c6b4c7fa53eb446382a4a518467f6f1ce858";

/// Every flipped bit of either blob or of the manifest is refused.
#[test]
fn every_damaged_bit_is_refused() {
    let (enc, _) = codec::encode(codec::FAMILY_CHANGESET, &golden_rows(), 9).unwrap();
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
    let m = Manifest {
        family: codec::FAMILY_CHANGESET,
        seq: 7,
        live: vec![enc.seg],
        retired: vec![h(4)],
    }
    .encode();
    for bit in 0..m.len() * 8 {
        let mut d = m.to_vec();
        d[bit / 8] ^= 1 << (bit % 8);
        assert!(Manifest::decode(&d).is_err(), "manifest bit {bit}");
    }
    assert!(codec::decode_index(ih, &index[..index.len() - 1]).is_err());
    // The right bytes without their prefix, at their own address.
    let bare = &index[30..];
    assert!(codec::decode_index(&Hash::of_blob(bare), bare).is_err());
    // A data blob of another index (another nonce).
    let (other, _) = codec::encode(codec::FAMILY_CHANGESET, &golden_rows(), 10).unwrap();
    assert!(codec::decode_data(&idx, &other.data.0, other.data.1.clone()).is_err());
}

#[test]
fn manifest_retired_list_is_bounded_and_never_names_a_live_blob() {
    let seg = |b: u8| SegmentRef {
        index: h(b),
        data: h(b + 100),
        rows: 1,
        columns: 2,
        data_len: 10,
    };
    let mut m = Manifest::new(codec::FAMILY_CHANGESET);
    m.live.push(seg(1));
    m.retire([seg(1), seg(2)]);
    assert_eq!(m.retired, vec![h(2), h(102)]);
    let many: Vec<SegmentRef> = (0..2000u32)
        .map(|i| {
            let mut b = [0u8; 32];
            b[..4].copy_from_slice(&i.to_be_bytes());
            let mut d = b;
            d[31] = 1;
            SegmentRef {
                index: Hash(b),
                data: Hash(d),
                rows: 1,
                columns: 2,
                data_len: 1,
            }
        })
        .collect();
    m.retire(many);
    assert_eq!(m.retired.len(), codec::MAX_RETIRED);
    assert!(!m.retired.contains(&h(1)) && !m.retired.contains(&h(101)));
}

// --- the compactor and the read path ------------------------------------

/// Records each call, so a test can check the feature was recorded before
/// the first row was deleted.
pub(crate) fn feature_hook(log: Arc<Mutex<Vec<&'static str>>>) -> Features {
    let seen = log.clone();
    Features {
        record: Arc::new(move |name| {
            let log = log.clone();
            Box::pin(async move {
                log.lock().unwrap().push(name);
                Ok(())
            })
        }),
        recorded: Arc::new(move |name| {
            let seen = seen.clone();
            Box::pin(async move { Ok(seen.lock().unwrap().contains(&name)) })
        }),
    }
}

pub(crate) async fn pass(
    r: &Repo,
    opts: &CompactOptions,
    state: &mut CompactState,
    f: Option<&Features>,
) -> PassReport {
    compact_pass(r, opts, state, f)
        .await
        .unwrap()
        .expect("lease")
}

/// Run passes until one does nothing new.
pub(crate) async fn settle(
    r: &Repo,
    opts: &CompactOptions,
    state: &mut CompactState,
    f: Option<&Features>,
) -> PassReport {
    let mut total = PassReport::default();
    for _ in 0..20 {
        let p = pass(r, opts, state, f).await;
        let idle = p.sealed_segments == 0 && p.merges == 0 && p.retired_rows == 0 && p.dropped == 0;
        total.sealed_segments += p.sealed_segments;
        total.sealed_keys += p.sealed_keys;
        total.merges += p.merges;
        total.retired_rows += p.retired_rows;
        total.mismatched += p.mismatched;
        total.live_segments = p.live_segments;
        if idle {
            return total;
        }
    }
    panic!("passes kept finding work");
}

/// Every value of every segmented kind of `ids`, read through a new
/// handle; history parts as their content.
pub(crate) async fn values(r: &Repo, ids: &[ChangesetId]) -> Vec<Option<Bytes>> {
    let f = fresh(r);
    let keys: Vec<Hash> = ids.iter().map(|i| i.0).collect();
    let mut out = f
        .meta()
        .get_derived_many(Skiplist::NAME, &keys)
        .await
        .unwrap();
    let hist = f
        .meta()
        .get_derived_many(HistoryTree::NAME, &keys)
        .await
        .unwrap();
    out.extend(hist.into_iter().map(|v| {
        v.map(|b| {
            Bytes::from(
                crate::derived::history_pack::canonical(&b)
                    .unwrap()
                    .into_owned(),
            )
        })
    }));
    for kind in V2_KINDS {
        out.extend(f.meta().get_derived_many(kind, &keys).await.unwrap());
    }
    out
}

/// What the queries answer, for comparing two stores.
pub(crate) async fn answers(r: &Repo, ids: &[ChangesetId]) -> Vec<String> {
    let r = fresh(r);
    let tip = *ids.last().unwrap();
    let mut out = Vec::new();
    for &id in ids.iter().step_by(3).chain([tip].iter()) {
        out.push(format!(
            "{:?}",
            r.log(id, None, 1000).await.map(|v| v.len())
        ));
        for p in ["a", "a/x1", "b/y", "c", "s"] {
            let l = r.log(id, Some(p), 1000).await;
            out.push(format!(
                "{p} {:?}",
                l.map(|v| v.iter().map(|c| c.id).collect::<Vec<_>>())
            ));
            out.push(format!(
                "{p} {:?}",
                r.last_change(id, p).await.map(|(c, n)| (c.id, n))
            ));
        }
        for &other in ids.iter().step_by(5) {
            out.push(format!("{:?}", r.is_ancestor(other, id).await));
            out.push(format!("{:?}", r.merge_bases(other, id).await));
        }
    }
    for p in ["a/x1", "b/y", "c/p"] {
        out.push(format!(
            "{:?}",
            r.blame(tip, &crate::tests::rp(p))
                .await
                .map(|b| serde_json::to_string(&b).unwrap())
        ));
    }
    out
}

/// A closure for [`build`] running a pass of `opts` on `r` every few
/// changesets, collecting the reports.
pub(crate) fn passes_while_building(
    r: &Repo,
    opts: CompactOptions,
    state: Arc<tokio::sync::Mutex<CompactState>>,
    reports: Arc<Mutex<Vec<PassReport>>>,
) -> impl FnMut() -> BoxFuture<'static, ()> + Send {
    let r = r.clone();
    move || {
        let (r, st, rep, opts) = (r.clone(), state.clone(), reports.clone(), opts.clone());
        Box::pin(async move {
            let mut st = st.lock().await;
            let p = pass(&r, &opts, &mut st, None).await;
            rep.lock().unwrap().push(p);
        })
    }
}

/// A store sealed and retired by the compactor answers every query, and
/// every read of the six kinds of both families, exactly as the row-only
/// store does.
#[tokio::test]
async fn sealed_and_retired_store_answers_as_rows_do() {
    let plain = repo();
    let ids = build(&plain, None).await;

    let sealed = repo();
    let opts = CompactOptions::small(Mode::Build);
    let state = Arc::new(tokio::sync::Mutex::new(CompactState::default()));
    let reports = Arc::new(Mutex::new(Vec::new()));
    let mut each = passes_while_building(&sealed, opts.clone(), state.clone(), reports.clone());
    let ids2 = build(&sealed, Some(&mut each)).await;
    drop(each);
    assert_eq!(ids, ids2);
    let mut state = Arc::try_unwrap(state).ok().unwrap().into_inner();
    let built = reports.lock().unwrap().clone();
    assert!(
        built.iter().map(|p| p.sealed_segments).sum::<u64>() > 4,
        "{built:?}"
    );
    assert!(built.iter().map(|p| p.merges).sum::<u64>() > 0, "{built:?}");
    settle(&sealed, &opts, &mut state, None).await;
    // Build mode deletes nothing: an older binary reads every row.
    assert_eq!(
        rows(&sealed, Skiplist::NAME).await,
        rows(&plain, Skiplist::NAME).await
    );
    assert_eq!(
        rows(&sealed, HistoryTree::NAME).await.len(),
        rows(&plain, HistoryTree::NAME).await.len()
    );
    for kind in V2_KINDS {
        assert_eq!(
            rows(&sealed, kind).await,
            rows(&plain, kind).await,
            "{kind}"
        );
    }
    let want_values = values(&plain, &ids).await;
    let want = answers(&plain, &ids).await;
    assert_eq!(values(&sealed, &ids).await, want_values);

    // Retire: each family's feature first, then its rows go; the answers
    // stay.
    let log = Arc::new(Mutex::new(Vec::new()));
    let hook = feature_hook(log.clone());
    let retire = CompactOptions::small(Mode::Retire);
    let done = settle(&sealed, &retire, &mut state, Some(&hook)).await;
    assert!(done.retired_rows > 300, "{done:?}");
    assert_eq!(done.mismatched, 0);
    // Each family's feature, once (the order rotates with the families).
    let mut got = log.lock().unwrap().clone();
    got.sort_unstable();
    assert_eq!(got, [FEATURE, FEATURE_2]);
    assert!(rows(&sealed, Skiplist::NAME).await.is_empty());
    assert!(rows(&sealed, HistoryTree::NAME).await.is_empty());
    for kind in V2_KINDS {
        assert!(rows(&sealed, kind).await.is_empty(), "{kind}");
    }
    assert_eq!(values(&sealed, &ids).await, want_values);
    // Nothing is derived again to answer the queries.
    let probe = fresh(&sealed);
    assert_eq!(answers(&probe, &ids).await, want);
    assert_eq!(probe.derived_count(), 0);
    // Counting sees the sealed keys.
    for kind in [Skiplist::NAME, Generation::NAME, RootTree::NAME_STR] {
        assert_eq!(
            probe.meta().count_derived(kind).await.unwrap(),
            ids.len() as u64,
            "{kind}"
        );
    }
    assert!(
        probe
            .meta()
            .derived_present(HistoryTree::NAME, &[ids[5].0])
            .await
            .unwrap()[0]
    );

    // A changeset landed after the retirement reads its parents' values
    // from segments.
    let tip = *ids.last().unwrap();
    let next = commit(&sealed, &[tip], &[("a/x1", Some("later"))], "later").await;
    derive::<HistoryTree>(&sealed, next).await.unwrap();
    let p = commit(&plain, &[tip], &[("a/x1", Some("later"))], "later").await;
    assert_eq!(p, next);
    derive::<HistoryTree>(&plain, p).await.unwrap();
    let mut all = ids.clone();
    all.push(next);
    assert_eq!(values(&sealed, &all).await, values(&plain, &all).await);
}

/// A pass that lost its lease and wakes after another wrote the manifest
/// fails on the swap and changes nothing.
#[tokio::test]
async fn a_stale_pass_loses_the_swap() {
    let r = repo();
    build(&r, None).await;
    let opts = CompactOptions::small(Mode::Build);
    let none = None;
    let mut stale = Pass::load(&r, &FAMILIES[0], &opts, &none, None)
        .await
        .unwrap();
    let mut first = Pass::load(&r, &FAMILIES[0], &opts, &none, None)
        .await
        .unwrap();
    first.run(&mut CompactState::default()).await.unwrap();
    let (after, _) = read_manifest(&r, FAMILY_CHANGESET).await.unwrap();
    assert!(after.is_some());
    let err = stale
        .run(&mut CompactState::default())
        .await
        .expect_err("stale");
    assert!(matches!(err, Error::Conflict(_)), "{err}");
    assert_eq!(read_manifest(&r, FAMILY_CHANGESET).await.unwrap().0, after);
}

/// One pass at a time: a second server finds the lease held and skips.
#[tokio::test]
async fn passes_take_turns_under_the_lease() {
    let r = repo();
    build(&r, None).await;
    let held = crate::gc_lease::GcLease::acquire_key(r.row_meta(), lease_key(), "other")
        .await
        .unwrap();
    let opts = CompactOptions::small(Mode::Build);
    let got = compact_pass(&r, &opts, &mut CompactState::default(), None)
        .await
        .unwrap();
    assert!(got.is_none());
    crate::gc_lease::release(held, Ok(())).await.unwrap();
    assert!(compact_pass(&r, &opts, &mut CompactState::default(), None)
        .await
        .unwrap()
        .is_some());
}

/// Thresholds: below `seal_rows` nothing seals until the rows stop growing
/// for `idle`; a changeset with one row waits `lone_wait` for the other.
#[tokio::test(start_paused = true)]
async fn seals_wait_for_enough_rows_or_quiet() {
    let r = repo();
    let a = commit(&r, &[], &[("f", Some("1"))], "a").await;
    let b = commit(&r, &[a], &[("f", Some("2"))], "b").await;
    let c = commit(&r, &[b], &[("f", Some("3"))], "c").await;
    let opts = CompactOptions {
        seal_rows: 100,
        idle_rows: 2,
        idle: Duration::from_secs(600),
        lone_wait: Duration::from_secs(300),
        ..CompactOptions::small(Mode::Build)
    };
    let mut st = CompactState::default();
    // Skiplists only: lone, and too few. (`changeset-v2` likewise: no
    // log filter yet.)
    assert_eq!(pass(&r, &opts, &mut st, None).await.sealed_segments, 0);
    tokio::time::advance(Duration::from_secs(400)).await;
    // Lone long enough, but not quiet long enough.
    assert_eq!(pass(&r, &opts, &mut st, None).await.sealed_segments, 0);
    tokio::time::advance(Duration::from_secs(300)).await;
    let p = pass(&r, &opts, &mut st, None).await;
    assert_eq!(
        (v1(&p).sealed_segments, v1(&p).sealed_keys),
        (1, 3),
        "{p:?}"
    );
    assert_eq!(
        (v2(&p).sealed_segments, v2(&p).sealed_keys),
        (1, 3),
        "{p:?}"
    );
    // Their history rows arrive later and seal in a segment of their own:
    // their skiplists are sealed, so they need not wait.
    derive::<HistoryTree>(&r, c).await.unwrap();
    pass(&r, &opts, &mut st, None).await;
    tokio::time::advance(Duration::from_secs(700)).await;
    let p = pass(&r, &opts, &mut st, None).await;
    assert_eq!(
        (v1(&p).live_segments, v1(&p).unsealed_rows),
        (2, 0),
        "{p:?}"
    );
    let v = fresh(&r);
    v.segments().refresh(&**v.row_meta()).await.unwrap();
    assert!(v
        .segments()
        .family(FAMILY_CHANGESET)
        .holds(&c.0, COL_HISTORY_TREE));
    assert!(v
        .segments()
        .family(FAMILY_CHANGESET)
        .holds(&a.0, COL_SKIPLIST));
    assert!(v
        .segments()
        .family(FAMILY_CHANGESET_V2)
        .holds(&a.0, COL_GENERATION));
}

/// Retirement waits until a segment was live long enough, and keeps a row
/// that differs from its segment's value (rows win).
#[tokio::test(start_paused = true)]
async fn retirement_waits_and_keeps_unequal_rows() {
    let r = repo();
    let ids = build(&r, None).await;
    let opts = CompactOptions {
        retire_after: Duration::from_secs(600),
        ..CompactOptions::small(Mode::Retire)
    };
    let log = Arc::new(Mutex::new(Vec::new()));
    let hook = feature_hook(log.clone());
    let mut st = CompactState::default();
    let p = pass(&r, &opts, &mut st, Some(&hook)).await;
    assert!(p.sealed_segments > 0);
    assert_eq!(p.retired_rows, 0);
    assert!(log.lock().unwrap().is_empty(), "no delete, no feature yet");
    // A row rewritten with another value (as `repair rederive` would).
    let victim = ids[10];
    let mut other = Skiplist::decode(&rows(&r, Skiplist::NAME).await[&victim.0]).unwrap();
    other.truncate(1);
    r.row_meta()
        .put_derived(Skiplist::NAME, &victim.0, Skiplist::encode(&other))
        .await
        .unwrap();
    tokio::time::advance(Duration::from_secs(601)).await;
    let p = pass(&r, &opts, &mut st, Some(&hook)).await;
    assert!(p.retired_rows > 0);
    assert_eq!(p.mismatched, 1);
    let left = rows(&r, Skiplist::NAME).await;
    assert_eq!(left.keys().copied().collect::<Vec<_>>(), [victim.0]);
    assert_eq!(
        fresh(&r)
            .meta()
            .get_derived(Skiplist::NAME, &victim.0)
            .await
            .unwrap(),
        Some(Skiplist::encode(&other))
    );
    // Without a way to record the feature, retire refuses to delete.
    let r2 = repo();
    build(&r2, None).await;
    let err = compact_pass(
        &r2,
        &CompactOptions::small(Mode::Retire),
        &mut CompactState::default(),
        None,
    )
    .await
    .expect_err("needs the feature");
    assert!(matches!(err, Error::Invalid(_)), "{err}");
    assert!(!rows(&r2, Skiplist::NAME).await.is_empty());
}

/// A segment whose data blob is gone (collected, or lost) reads as missing
/// rows: values derive again, equal, and the next pass drops it and seals
/// the rows again.
#[tokio::test]
async fn a_lost_segment_reads_as_missing_rows() {
    let r = repo();
    let ids = build(&r, None).await;
    let want = values(&r, &ids).await;
    let hook = feature_hook(Arc::default());
    let mut st = CompactState::default();
    settle(
        &r,
        &CompactOptions::small(Mode::Retire),
        &mut st,
        Some(&hook),
    )
    .await;
    let (_, ManifestRead::Read(m)) = read_manifest(&r, FAMILY_CHANGESET).await.unwrap() else {
        panic!("no manifest")
    };
    let lost = m.live[0];
    r.blobs().delete(&lost.data).await.unwrap();
    // A raw read misses; a derivation computes the value again, equal.
    let f = fresh(&r);
    assert!(f
        .meta()
        .get_derived(Skiplist::NAME, &ids[1].0)
        .await
        .unwrap()
        .is_none());
    for id in &ids {
        derive::<Skiplist>(&f, *id).await.unwrap();
        derive::<HistoryTree>(&f, *id).await.unwrap();
    }
    assert_eq!(values(&r, &ids).await, want);
    // Reading derived the values again as rows; a pass drops the segment
    // and seals them again.
    // This process retired rows, so it drops a segment only when two
    // passes agree.
    let p = pass(&r, &CompactOptions::small(Mode::Build), &mut st, None).await;
    assert_eq!(p.dropped, 0, "{p:?}");
    let p = pass(&r, &CompactOptions::small(Mode::Build), &mut st, None).await;
    assert_eq!(p.dropped, 1, "{p:?}");
    let (_, ManifestRead::Read(m)) = read_manifest(&r, FAMILY_CHANGESET).await.unwrap() else {
        panic!()
    };
    assert!(!m.live.iter().any(|s| s.index == lost.index));
    assert!(m.retired.contains(&lost.index));
    settle(&r, &CompactOptions::small(Mode::Build), &mut st, None).await;
    assert_eq!(values(&r, &ids).await, want);
}

/// A view reads the manifests again only for a miss, at most every
/// `REFRESH`: in mode build (every row there) never; a single-row read's
/// miss reads each family's with a single-row read (three families), and a
/// batched read of several kinds carries them all in the same call.
#[tokio::test(start_paused = true)]
async fn the_manifest_is_read_only_for_misses() {
    let r = repo();
    let ids = build(&r, None).await;
    let mut st = CompactState::default();
    settle(&r, &CompactOptions::small(Mode::Build), &mut st, None).await;
    let counter = Arc::new(Counting::new(r.row_meta().clone()));
    let v = Repo::new(r.blobs().clone(), counter.clone());
    v.meta()
        .get_derived(Skiplist::NAME, &ids[3].0)
        .await
        .unwrap();
    v.meta()
        .get_derived_many(HistoryTree::NAME, &[ids[3].0, ids[4].0])
        .await
        .unwrap();
    assert_eq!((counter.calls(), counter.manifest_reads()), (2, 0));
    // Retired: the first miss reads the manifest, the next do not.
    let hook = feature_hook(Arc::default());
    settle(
        &r,
        &CompactOptions::small(Mode::Retire),
        &mut st,
        Some(&hook),
    )
    .await;
    let v = Repo::new(r.blobs().clone(), counter.clone());
    let base = counter.calls();
    let got = v
        .meta()
        .get_derived(Skiplist::NAME, &ids[3].0)
        .await
        .unwrap();
    assert!(got.is_some());
    assert_eq!((counter.calls() - base, counter.manifest_reads()), (4, 3));
    v.meta()
        .get_derived_many(HistoryTree::NAME, &[ids[3].0, ids[4].0])
        .await
        .unwrap();
    v.meta()
        .get_derived(Generation::NAME, &ids[3].0)
        .await
        .unwrap();
    // `generation` is sealed too (family changeset-v2): its miss finds the
    // manifest read above.
    assert_eq!((counter.calls() - base, counter.manifest_reads()), (6, 3));
    tokio::time::advance(view::REFRESH).await;
    let keys = [ids[3].0];
    v.meta()
        .get_derived_kinds(&[(Skiplist::NAME, &keys), (Generation::NAME, &keys)])
        .await
        .unwrap();
    assert_eq!((counter.calls() - base, counter.manifest_reads()), (7, 4));
}

/// Counts derived reads, and those that bring the manifest.
struct Counting {
    inner: Arc<dyn MetaStore>,
    calls: AtomicUsize,
    manifest: AtomicUsize,
}

impl Counting {
    fn new(inner: Arc<dyn MetaStore>) -> Self {
        Counting {
            inner,
            calls: AtomicUsize::new(0),
            manifest: AtomicUsize::new(0),
        }
    }
    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
    fn manifest_reads(&self) -> usize {
        self.manifest.load(Ordering::SeqCst)
    }
    fn note(&self, kinds: &[&str]) {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if kinds.contains(&MANIFEST_KIND) {
            self.manifest.fetch_add(1, Ordering::SeqCst);
        }
    }
}

#[async_trait]
impl MetaStore for Counting {
    async fn get_derived(&self, kind: &str, key: &Hash) -> leep_core::Result<Option<Bytes>> {
        self.note(&[kind]);
        self.inner.get_derived(kind, key).await
    }
    async fn get_derived_many(
        &self,
        kind: &str,
        keys: &[Hash],
    ) -> leep_core::Result<Vec<Option<Bytes>>> {
        self.note(&[kind]);
        self.inner.get_derived_many(kind, keys).await
    }
    async fn get_derived_kinds(
        &self,
        reads: &[(&str, &[Hash])],
    ) -> leep_core::Result<Vec<Vec<Option<Bytes>>>> {
        let kinds: Vec<&str> = reads.iter().map(|(k, _)| *k).collect();
        self.note(&kinds);
        self.inner.get_derived_kinds(reads).await
    }
    async fn put_changeset(&self, cs: &leep_core::Changeset) -> leep_core::Result<ChangesetId> {
        self.inner.put_changeset(cs).await
    }
    async fn get_changeset(
        &self,
        id: &ChangesetId,
    ) -> leep_core::Result<Option<leep_core::Changeset>> {
        self.inner.get_changeset(id).await
    }
    async fn list_changesets(
        &self,
        after: Option<ChangesetId>,
        limit: usize,
    ) -> leep_core::Result<Vec<ChangesetId>> {
        self.inner.list_changesets(after, limit).await
    }
    async fn get_ref(&self, name: &str) -> leep_core::Result<Option<ChangesetId>> {
        self.inner.get_ref(name).await
    }
    async fn cas_ref_logged(
        &self,
        name: &str,
        expected: Option<ChangesetId>,
        new: Option<ChangesetId>,
        ctx: &leep_core::RefUpdateContext,
    ) -> leep_core::Result<bool> {
        self.inner.cas_ref_logged(name, expected, new, ctx).await
    }
    async fn ref_log(
        &self,
        name: Option<&str>,
        before_seq: Option<u64>,
        limit: usize,
    ) -> leep_core::Result<Vec<leep_core::RefLogEntry>> {
        self.inner.ref_log(name, before_seq, limit).await
    }
    async fn list_refs(&self) -> leep_core::Result<BTreeMap<String, ChangesetId>> {
        self.inner.list_refs().await
    }
    async fn put_derived(&self, kind: &str, key: &Hash, value: Bytes) -> leep_core::Result<()> {
        self.inner.put_derived(kind, key, value).await
    }
    async fn cas_derived(
        &self,
        kind: &str,
        key: &Hash,
        expected: Option<&[u8]>,
        new: Bytes,
    ) -> leep_core::Result<bool> {
        self.inner.cas_derived(kind, key, expected, new).await
    }
}

/// Blob GC never condemns a live segment's blobs, and collects the blobs of
/// segments merges replaced through condemn, fence and collect, reporting
/// them apart from other blobs.
#[tokio::test]
async fn gc_keeps_live_segments_and_collects_merged_ones() {
    use crate::gc::{self, Clock, GcOptions};
    struct Stepping(Mutex<i64>);
    #[async_trait]
    impl Clock for Stepping {
        fn now(&self) -> i64 {
            *self.0.lock().unwrap()
        }
        async fn sleep(&self, d: Duration) {
            *self.0.lock().unwrap() += d.as_secs() as i64;
        }
    }
    let r = repo();
    let state = Arc::new(tokio::sync::Mutex::new(CompactState::default()));
    let mut each = passes_while_building(
        &r,
        CompactOptions::small(Mode::Build),
        state.clone(),
        Arc::default(),
    );
    let ids = build(&r, Some(&mut each)).await;
    drop(each);
    // Every family's segments: GC unions the manifests.
    let mut m = Manifest::new(FAMILY_CHANGESET);
    for fam in FAMILIES
        .iter()
        .filter(|f| f.keys == super::Keys::Changesets)
    {
        let (_, ManifestRead::Read(fm)) = read_manifest(&r, fam.id).await.unwrap() else {
            panic!("no manifest of {}", fam.name)
        };
        assert!(
            !fm.retired.is_empty(),
            "merges retired segments of {}",
            fam.name
        );
        m.live.extend(fm.live);
        m.retired.extend(fm.retired);
    }
    let retired: Vec<Hash> = m.retired.clone();
    let want = values(&r, &ids).await;
    let opts = GcOptions {
        grace: Duration::ZERO,
        fence: Duration::from_secs(600),
        unsafe_no_fence: false,
        dry_run: false,
        backup_fence: None,
    };
    let clock = Stepping(Mutex::new(1_800_000_000));
    let rep = gc::run(&r, &opts, &clock).await.unwrap();
    assert_eq!(rep.condemn.derived_segments, m.live.len() as u64);
    assert_eq!(rep.condemn.derived_segments_condemned, retired.len() as u64);
    assert_eq!(rep.collect.derived_segments_deleted, retired.len() as u64);
    assert_eq!(rep.collect.deleted, 0, "{rep:?}");
    for h in &retired {
        assert!(!r.blobs().contains(h).await.unwrap());
    }
    for b in m.live_blobs() {
        assert!(r.blobs().contains(&b).await.unwrap());
    }
    let rep = gc::run(&r, &opts, &clock).await.unwrap();
    assert_eq!(
        rep.condemn.condemned + rep.condemn.derived_segments_condemned,
        0,
        "{rep:?}"
    );
    assert_eq!(values(&r, &ids).await, want);
    // The next pass forgets the collected blobs.
    pass(
        &r,
        &CompactOptions::small(Mode::Build),
        &mut *state.lock().await,
        None,
    )
    .await;
    for fam in FAMILIES
        .iter()
        .filter(|f| f.keys == super::Keys::Changesets)
    {
        let (_, ManifestRead::Read(m)) = read_manifest(&r, fam.id).await.unwrap() else {
            panic!()
        };
        assert!(m.retired.is_empty(), "{:?}", m.retired);
    }
    // A damaged manifest of either family does not stop GC: it cannot tell
    // that family's live segments from orphans, so it keeps every blob that
    // starts like a segment, and collects the rest.
    for fam in FAMILIES
        .iter()
        .filter(|f| f.keys == super::Keys::Changesets)
    {
        let (good, _) = read_manifest(&r, fam.id).await.unwrap();
        r.row_meta()
            .put_derived(
                MANIFEST_KIND,
                &manifest_key(fam.id),
                Bytes::from_static(b"junk"),
            )
            .await
            .unwrap();
        let mut unnamed = leep_core::hash::OBJECT_DOMAIN.to_vec();
        unnamed.extend_from_slice(codec::INDEX_KIND.as_bytes());
        unnamed.extend_from_slice(b"\0 a segment no readable manifest names");
        let unnamed = r.blobs().put(Bytes::from(unnamed)).await.unwrap();
        let orphan = r
            .blobs()
            .put(Bytes::from(format!("an orphan {}", fam.name)))
            .await
            .unwrap();
        let rep = gc::run(&r, &opts, &clock).await.unwrap();
        assert_eq!(rep.condemn.damaged_derived_manifests, vec![fam.name]);
        assert!(rep.condemn.kept_derived_unknown >= 1, "{rep:?}");
        assert!(r.blobs().contains(&unnamed).await.unwrap());
        assert!(!r.blobs().contains(&orphan).await.unwrap(), "{rep:?}");
        for b in m.live_blobs() {
            assert!(r.blobs().contains(&b).await.unwrap());
        }
        r.row_meta()
            .put_derived(MANIFEST_KIND, &manifest_key(fam.id), good.unwrap())
            .await
            .unwrap();
        // Fixed: the unnamed one is an orphan again.
        gc::run(&r, &opts, &clock).await.unwrap();
        assert!(!r.blobs().contains(&unnamed).await.unwrap());
    }
    assert!(gc::run(&r, &opts, &clock).await.is_ok());
    // So does a manifest row of a family this binary does not know: a
    // newer binary's live segments are not orphans.
    let unknown = Hash::of_object("derived-segment-family", b"content-v9");
    r.row_meta()
        .put_derived(MANIFEST_KIND, &unknown, Bytes::from_static(b"x"))
        .await
        .unwrap();
    let err = gc::run(&r, &opts, &clock).await.unwrap_err();
    assert!(err.to_string().contains("does not know"), "{err}");
}

/// `repair drop` of a kind stops segments answering for it (its values
/// derive again, equal); a manifest that does not decode is replaced in
/// build mode and refused in retire mode.
#[tokio::test]
async fn repair_drop_clears_columns_and_damage_is_handled() {
    let r = repo();
    let ids = build(&r, None).await;
    let want = values(&r, &ids).await;
    let mut st = CompactState::default();
    settle(&r, &CompactOptions::small(Mode::Build), &mut st, None).await;
    crate::integrity::drop_kinds(&r, &[Skiplist::NAME], false, &crate::integrity::Quiet)
        .await
        .unwrap();
    let (_, ManifestRead::Read(m)) = read_manifest(&r, FAMILY_CHANGESET).await.unwrap() else {
        panic!()
    };
    assert!(!m.live.is_empty());
    assert!(m
        .live
        .iter()
        .all(|s| !s.has(COL_SKIPLIST) && s.has(COL_HISTORY_TREE)));
    let f = fresh(&r);
    let keys: Vec<Hash> = ids.iter().map(|i| i.0).collect();
    assert!(f
        .meta()
        .get_derived_many(Skiplist::NAME, &keys)
        .await
        .unwrap()
        .iter()
        .all(Option::is_none));
    for id in &ids {
        derive::<Skiplist>(&r, *id).await.unwrap();
    }
    assert_eq!(values(&r, &ids).await, want);
    // Damage.
    r.row_meta()
        .put_derived(
            MANIFEST_KIND,
            &manifest_key(FAMILY_CHANGESET),
            Bytes::from_static(b"junk"),
        )
        .await
        .unwrap();
    assert!(
        compact_pass(&r, &CompactOptions::small(Mode::Retire), &mut st, None)
            .await
            .is_err()
    );
    let p = pass(&r, &CompactOptions::small(Mode::Build), &mut st, None).await;
    assert!(p.damaged_manifest);
    assert!(matches!(
        read_manifest(&r, FAMILY_CHANGESET).await.unwrap().1,
        ManifestRead::Read(_)
    ));
    assert_eq!(values(&r, &ids).await, want);
}

/// `verify` reads segment values like rows, reports a manifest or a
/// segment that does not decode, and notes a segment that is not stored.
#[tokio::test]
async fn verify_checks_the_manifest_and_segments() {
    use crate::integrity::{verify, Quiet, VerifyOptions};
    let r = repo();
    build(&r, None).await;
    let hook = feature_hook(Arc::default());
    settle(
        &r,
        &CompactOptions::small(Mode::Retire),
        &mut CompactState::default(),
        Some(&hook),
    )
    .await;
    let opts = VerifyOptions {
        full: true,
        ..VerifyOptions::default()
    };
    let rep = verify(&fresh(&r), &opts, &Quiet).await.unwrap();
    assert!(rep.problems.is_empty(), "{:?}", rep.problems);
    let checked = rep.derived.get(Skiplist::NAME).map_or(0, |k| k.checked);
    assert!(checked > 40, "segment values are checked: {checked}");
    let (_, ManifestRead::Read(m)) = read_manifest(&r, FAMILY_CHANGESET).await.unwrap() else {
        panic!()
    };
    let bad = m.live[0];
    r.blobs().delete(&bad.index).await.unwrap();
    r.blobs()
        .put_with_hash(
            bad.index,
            Bytes::from_static(b"leep:derived-segment-index-v1\0junk"),
        )
        .await
        .unwrap();
    let rep = verify(&fresh(&r), &opts, &Quiet).await.unwrap();
    assert!(
        rep.problems.iter().any(|p| p.code == "bad_derived_segment"),
        "{:?}",
        rep.problems
    );
    r.blobs().delete(&bad.index).await.unwrap();
    let rep = verify(&fresh(&r), &opts, &Quiet).await.unwrap();
    assert!(!rep.problems.iter().any(|p| p.code == "bad_derived_segment"));
    assert!(
        rep.notes.iter().any(|n| n.contains("is not stored")),
        "{:?}",
        rep.notes
    );
    r.row_meta()
        .put_derived(
            MANIFEST_KIND,
            &manifest_key(FAMILY_CHANGESET),
            Bytes::from_static(b"junk"),
        )
        .await
        .unwrap();
    let rep = verify(&fresh(&r), &opts, &Quiet).await.unwrap();
    assert!(rep
        .problems
        .iter()
        .any(|p| p.code == "bad_derived_manifest"));
}

/// The view keeps every `MetaStore` contract of the store under it, and a
/// second handle wraps the store under a view once.
#[tokio::test]
async fn the_view_keeps_the_store_contracts() {
    let make = || -> Arc<dyn MetaStore> { repo().meta().clone() };
    leep_core::memory::check_meta_store_contract(&*make()).await;
    leep_core::memory::check_cas_derived_contract(&*make()).await;
    leep_core::memory::check_insert_once_contract(&*make()).await;
    leep_core::memory::check_lease_contract(&*make()).await;
    leep_core::memory::check_review_request_contract(&*make()).await;
    let r = repo();
    let again = Repo::new(r.blobs().clone(), r.meta().clone());
    assert!(again.meta().view_base().is_some());
    assert!(again.row_meta().view_base().is_none());
}

/// A segment whose data blob cannot be read is left out for `REFRESH`,
/// then tried again: a blob a repack was moving reads again.
#[tokio::test(start_paused = true)]
async fn an_unreadable_segment_is_tried_again_after_a_refresh() {
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
    let (_, ManifestRead::Read(m)) = read_manifest(&r, FAMILY_CHANGESET).await.unwrap() else {
        panic!("no manifest")
    };
    let seg = m.live[0];
    let data = r.blobs().get(&seg.data).await.unwrap().unwrap();
    r.blobs().delete(&seg.data).await.unwrap();
    let v = fresh(&r);
    let key = ids[2].0;
    assert!(v
        .meta()
        .get_derived(Skiplist::NAME, &key)
        .await
        .unwrap()
        .is_none());
    r.blobs().put_with_hash(seg.data, data).await.unwrap();
    // Still left out within the interval...
    assert!(v
        .meta()
        .get_derived(Skiplist::NAME, &key)
        .await
        .unwrap()
        .is_none());
    // ...and read again after it.
    tokio::time::advance(view::REFRESH).await;
    assert!(v
        .meta()
        .get_derived(Skiplist::NAME, &key)
        .await
        .unwrap()
        .is_some());
}

// --- review fixes: store state over process flags, monotonic manifests ---

/// A blob store that fails reads of chosen blobs with a transient error.
pub(crate) struct Flaky {
    pub(crate) inner: Arc<dyn leep_core::BlobStore>,
    pub(crate) failing: Mutex<std::collections::HashSet<Hash>>,
}

#[async_trait]
impl leep_core::BlobStore for Flaky {
    async fn put_with_hash(&self, hash: Hash, data: Bytes) -> leep_core::Result<()> {
        self.inner.put_with_hash(hash, data).await
    }
    async fn get(&self, hash: &Hash) -> leep_core::Result<Option<Bytes>> {
        if self.failing.lock().unwrap().contains(hash) {
            return Err(Error::Storage("connection reset".into()));
        }
        self.inner.get(hash).await
    }
    async fn list_page(
        &self,
        after: Option<Hash>,
        limit: usize,
    ) -> leep_core::Result<Vec<leep_core::BlobMeta>> {
        self.inner.list_page(after, limit).await
    }
    async fn delete(&self, hash: &Hash) -> leep_core::Result<()> {
        self.inner.delete(hash).await
    }
}

fn recorded_features() -> Features {
    Features {
        record: Arc::new(|_| Box::pin(async { Ok(()) })),
        recorded: Arc::new(|_| Box::pin(async { Ok(true) })),
    }
}

/// A damaged manifest is replaced in mode build only while the store has
/// no `derived-segments` feature: another server may have retired rows.
#[tokio::test]
async fn a_damaged_manifest_is_kept_once_the_feature_is_recorded() {
    let r = repo();
    build(&r, None).await;
    let mut st = CompactState::default();
    settle(&r, &CompactOptions::small(Mode::Build), &mut st, None).await;
    let junk = Bytes::from_static(b"junk");
    r.row_meta()
        .put_derived(MANIFEST_KIND, &manifest_key(FAMILY_CHANGESET), junk.clone())
        .await
        .unwrap();
    let f = recorded_features();
    let err = compact_pass(&r, &CompactOptions::small(Mode::Build), &mut st, Some(&f))
        .await
        .expect_err("refused");
    assert!(err.to_string().contains("not replaced"), "{err}");
    assert_eq!(
        read_manifest(&r, FAMILY_CHANGESET).await.unwrap().0,
        Some(junk)
    );
    // Without the feature, build replaces it.
    let p = pass(&r, &CompactOptions::small(Mode::Build), &mut st, None).await;
    assert!(p.damaged_manifest);
}

/// A manifest with a good checksum but an unknown version is a newer
/// binary's: never damage, never replaced. Passes do nothing, GC and
/// `repair drop` refuse, readers read rows only.
#[tokio::test]
async fn a_newer_manifest_is_left_alone() {
    let r = repo();
    let ids = build(&r, None).await;
    let mut body = vec![2u8, 0, 1];
    body.extend_from_slice(&[0; 12]);
    let sum = Hash::of_blob(&body);
    body.extend_from_slice(&sum.0);
    let newer = Bytes::from(body);
    assert!(matches!(
        codec::Manifest::parse(&newer),
        Err(codec::ManifestFault::Newer(_))
    ));
    r.row_meta()
        .put_derived(
            MANIFEST_KIND,
            &manifest_key(FAMILY_CHANGESET),
            newer.clone(),
        )
        .await
        .unwrap();
    for mode in [Mode::Build, Mode::Retire] {
        let p = compact_pass(
            &r,
            &CompactOptions::small(mode),
            &mut CompactState::default(),
            None,
        )
        .await;
        // The other family goes on (and in mode retire, without a way to
        // record its feature, fails on its own).
        let p = match p {
            Ok(Some(p)) => p,
            Err(e) => {
                assert_eq!(mode, Mode::Retire, "{e}");
                continue;
            }
            Ok(None) => panic!("lease"),
        };
        let p = v1(&p);
        assert!(p.newer_manifest && p.sealed_segments == 0 && p.retired_rows == 0);
    }
    assert_eq!(
        read_manifest(&r, FAMILY_CHANGESET).await.unwrap().0,
        Some(newer.clone())
    );
    assert!(
        crate::integrity::drop_kinds(&r, &[Skiplist::NAME], false, &crate::integrity::Quiet)
            .await
            .is_err()
    );
    assert_eq!(
        read_manifest(&r, FAMILY_CHANGESET).await.unwrap().0,
        Some(newer)
    );
    assert!(values(&r, &ids).await.iter().all(Option::is_some));
}

/// A pass probes segments itself: a transient read error drops nothing
/// (before `drop_failing_after`), whatever a reader marked broken; on a
/// store with the feature recorded, a lost segment is dropped only when two
/// passes agree.
#[tokio::test]
async fn transient_errors_drop_nothing_and_losses_need_two_passes() {
    let blobs: Arc<dyn leep_core::BlobStore> = Arc::new(MemoryBlobStore::new());
    let flaky = Arc::new(Flaky {
        inner: blobs.clone(),
        failing: Mutex::default(),
    });
    let r = Repo::new(flaky.clone(), Arc::new(MemoryMetaStore::new()));
    let ids = build(&r, None).await;
    let f = recorded_features();
    let mut st = CompactState::default();
    settle(&r, &CompactOptions::small(Mode::Retire), &mut st, Some(&f)).await;
    let (before, ManifestRead::Read(m)) = read_manifest(&r, FAMILY_CHANGESET).await.unwrap() else {
        panic!()
    };
    let seg = m.live[0];
    // A reader in another handle finds the data unreadable and marks it.
    flaky.failing.lock().unwrap().insert(seg.data);
    let reader = fresh(&r);
    reader
        .meta()
        .get_derived(Skiplist::NAME, &ids[1].0)
        .await
        .ok();
    // A new process's pass keeps the segment through a transient error
    // (others read), or fails when every segment fails; it drops nothing.
    let mut fresh_state = CompactState::default();
    let slow = CompactOptions {
        drop_failing_after: Duration::from_secs(3600),
        ..CompactOptions::small(Mode::Retire)
    };
    for _ in 0..4 {
        if let Ok(Some(p)) = compact_pass(&r, &slow, &mut fresh_state, Some(&f)).await {
            assert_eq!(v1(&p).dropped, 0);
            assert_eq!(v1(&p).failing, 1);
        }
    }
    assert_eq!(read_manifest(&r, FAMILY_CHANGESET).await.unwrap().0, before);
    flaky.failing.lock().unwrap().clear();
    // Lost for real: the first pass suspects it, the second drops it.
    let data = blobs.get(&seg.data).await.unwrap().unwrap();
    blobs.delete(&seg.data).await.unwrap();
    let p = pass(
        &r,
        &CompactOptions::small(Mode::Retire),
        &mut fresh_state,
        Some(&f),
    )
    .await;
    assert_eq!(p.dropped, 0);
    // It came back meanwhile (a repack was moving it): nothing dropped.
    blobs.put_with_hash(seg.data, data).await.unwrap();
    let p = pass(
        &r,
        &CompactOptions::small(Mode::Retire),
        &mut fresh_state,
        Some(&f),
    )
    .await;
    assert_eq!(p.dropped, 0);
    blobs.delete(&seg.data).await.unwrap();
    let p = pass(
        &r,
        &CompactOptions::small(Mode::Retire),
        &mut fresh_state,
        Some(&f),
    )
    .await;
    assert_eq!(p.dropped, 0);
    let p = pass(
        &r,
        &CompactOptions::small(Mode::Retire),
        &mut fresh_state,
        Some(&f),
    )
    .await;
    assert_eq!(p.dropped, 1, "{p:?}");
}

/// A manifest read late never replaces a newer one a view already uses.
#[tokio::test]
async fn a_stale_manifest_is_not_applied_over_a_newer_one() {
    let r = repo();
    build(&r, None).await;
    let mut st = CompactState::default();
    pass(&r, &CompactOptions::small(Mode::Build), &mut st, None).await;
    let (old, _) = read_manifest(&r, FAMILY_CHANGESET).await.unwrap();
    let state = Arc::new(tokio::sync::Mutex::new(st));
    let mut each = passes_while_building(
        &r,
        CompactOptions::small(Mode::Build),
        state.clone(),
        Arc::default(),
    );
    each().await;
    let mut next = commit(&r, &[], &[("z", Some("z"))], "z").await;
    for i in 0..4 {
        next = commit(&r, &[next], &[("z", Some(&format!("z{i}")))], "z").await;
    }
    derive::<HistoryTree>(&r, next).await.unwrap();
    each().await;
    let (new, ManifestRead::Read(nm)) = read_manifest(&r, FAMILY_CHANGESET).await.unwrap() else {
        panic!()
    };
    assert_ne!(old, new);
    let v = fresh(&r);
    v.segments()
        .family(FAMILY_CHANGESET)
        .apply(new.clone())
        .await
        .unwrap();
    v.segments()
        .family(FAMILY_CHANGESET)
        .apply(old)
        .await
        .unwrap();
    assert_eq!(v.segments().family(FAMILY_CHANGESET).raw(), new);
    assert_eq!(
        v.segments().family(FAMILY_CHANGESET).segments().len(),
        nm.live.len()
    );
}

/// Two handles: one holds a manifest read before a segment was sealed,
/// while another seals, waits out `RETIRE_AFTER` and deletes the rows. The
/// stale handle's manifest is then older than `REFRESH`, so it reads the
/// manifest again before declaring a miss, and every value is right; one
/// whose segment was merged away and collected reads again and finds the
/// merged one.
#[tokio::test(start_paused = true)]
async fn a_stale_handle_still_reads_right_values() {
    let r = repo();
    let ids = build(&r, None).await;
    let want = values(&r, &ids).await;
    let stale = fresh(&r);
    // The stale handle reads its (empty) manifest now.
    stale
        .segments()
        .family(FAMILY_CHANGESET)
        .apply(read_manifest(&r, FAMILY_CHANGESET).await.unwrap().0)
        .await
        .unwrap();
    let opts = CompactOptions {
        retire_after: RETIRE_AFTER,
        ..CompactOptions::small(Mode::Retire)
    };
    let hook = feature_hook(Arc::default());
    let mut st = CompactState::default();
    pass(&r, &opts, &mut st, Some(&hook)).await;
    tokio::time::advance(RETIRE_AFTER).await;
    let p = pass(&r, &opts, &mut st, Some(&hook)).await;
    assert!(p.retired_rows > 0, "{p:?}");
    let keys: Vec<Hash> = ids.iter().map(|i| i.0).collect();
    let got = stale
        .meta()
        .get_derived_many(Skiplist::NAME, &keys)
        .await
        .unwrap();
    assert_eq!(got, want[..ids.len()]);
    assert_eq!(stale.derived_count(), 0);
}

/// In mode build a family's rows are listed again only once they grew by
/// `seal_rows` times its columns or `list_every` passed.
#[tokio::test(start_paused = true)]
async fn build_mode_lists_rows_only_when_they_grew() {
    let r = repo();
    let ids = build(&r, None).await;
    let opts = CompactOptions {
        seal_rows: 4,
        list_every: Duration::from_secs(600),
        ..CompactOptions::small(Mode::Build)
    };
    let mut st = CompactState::default();
    assert!(v1(&pass(&r, &opts, &mut st, None).await).listed);
    assert!(!v1(&pass(&r, &opts, &mut st, None).await).listed);
    let mut tip = *ids.last().unwrap();
    for i in 0..3 {
        tip = commit(&r, &[tip], &[("g", Some(&format!("{i}")))], "g").await;
    }
    derive::<HistoryTree>(&r, tip).await.unwrap();
    // Six more rows: under `seal_rows` times two columns.
    assert!(!v1(&pass(&r, &opts, &mut st, None).await).listed);
    tip = commit(&r, &[tip], &[("g", Some("3"))], "g").await;
    derive::<HistoryTree>(&r, tip).await.unwrap();
    // Eight: enough.
    let p = v1(&pass(&r, &opts, &mut st, None).await);
    assert!(p.listed && p.sealed_keys == 4, "{p:?}");
    assert!(!v1(&pass(&r, &opts, &mut st, None).await).listed);
    tokio::time::advance(Duration::from_secs(600)).await;
    assert!(v1(&pass(&r, &opts, &mut st, None).await).listed);
}

/// Four small segments of one tier, sealed without merging.
async fn four_segments(r: &Repo, st: &mut CompactState) -> Vec<SegmentRef> {
    let opts = CompactOptions {
        fanout: 64,
        ..CompactOptions::small(Mode::Build)
    };
    let mut tip = commit(r, &[], &[("f", Some("0"))], "0").await;
    for b in 0..4 {
        for i in 0..4 {
            tip = commit(r, &[tip], &[("f", Some(&format!("{b}.{i}")))], "x").await;
        }
        derive::<HistoryTree>(r, tip).await.unwrap();
        pass(r, &opts, st, None).await;
    }
    let (_, ManifestRead::Read(m)) = read_manifest(r, FAMILY_CHANGESET).await.unwrap() else {
        panic!()
    };
    assert_eq!(m.live.len(), 4, "{m:?}");
    m.live
}

/// A merge whose run holds a segment it cannot read skips that segment for
/// the rest of the pass instead of picking the same run forever (with the
/// lease held). On a store with the feature the next passes drop it once
/// two agree.
#[tokio::test(start_paused = true)]
async fn a_merge_over_an_unreadable_segment_ends_the_pass() {
    let r = repo();
    let mut st = CompactState::default();
    let live = four_segments(&r, &mut st).await;
    r.blobs().delete(&live[1].data).await.unwrap();
    let f = recorded_features();
    let merge = CompactOptions::small(Mode::Build);
    let p = tokio::time::timeout(
        Duration::from_secs(30),
        compact_pass(&r, &merge, &mut st, Some(&f)),
    )
    .await
    .expect("the pass ends")
    .unwrap()
    .expect("lease");
    assert_eq!((v1(&p).merges, v1(&p).dropped), (0, 0), "{p:?}");
    // The lease was given up: the next pass runs, and agrees.
    let p = pass(&r, &merge, &mut st, Some(&f)).await;
    assert_eq!(v1(&p).dropped, 1, "{p:?}");
}

/// In mode build, a segment verified earlier whose data later reads
/// damaged: the merge skips it, the next pass reads it whole and drops it.
#[tokio::test(start_paused = true)]
async fn a_verified_segment_turning_corrupt_is_dropped_next_pass() {
    let r = repo();
    let mut st = CompactState::default();
    let live = four_segments(&r, &mut st).await;
    r.blobs().delete(&live[2].data).await.unwrap();
    r.blobs()
        .put_with_hash(
            live[2].data,
            Bytes::from_static(b"leep:derived-segment-data-v1\0x"),
        )
        .await
        .unwrap();
    let merge = CompactOptions::small(Mode::Build);
    let p = tokio::time::timeout(
        Duration::from_secs(30),
        compact_pass(&r, &merge, &mut st, None),
    )
    .await
    .expect("the pass ends")
    .unwrap()
    .expect("lease");
    assert_eq!(p.dropped, 0, "{p:?}");
    let p = pass(&r, &merge, &mut st, None).await;
    assert_eq!(p.dropped, 1, "{p:?}");
}

/// A first seal of more candidates than a pass's pages says there is more,
/// and the next pass lists again and goes on.
#[tokio::test]
async fn a_backfill_continues_promptly() {
    let r = repo();
    let ids = build(&r, None).await;
    let opts = CompactOptions {
        max_rows: 8,
        max_seal_pages: 2,
        list_every: Duration::from_secs(600),
        fanout: 64,
        ..CompactOptions::small(Mode::Build)
    };
    let mut st = CompactState::default();
    let p = v1(&pass(&r, &opts, &mut st, None).await);
    assert!(p.listed && p.more && p.sealed_keys == 16, "{p:?}");
    let mut sealed = p.sealed_keys;
    for _ in 0..20 {
        // The following passes go on with the listing's candidates.
        let p = v1(&pass(&r, &opts, &mut st, None).await);
        assert!(!p.listed && p.sealed_keys > 0, "{p:?}");
        sealed += p.sealed_keys;
        if !p.more {
            break;
        }
    }
    assert_eq!(sealed as usize, ids.len());
    let p = v1(&pass(&r, &opts, &mut st, None).await);
    assert!(!p.more && p.sealed_keys == 0, "{p:?}");
}

/// A backfill whose generations take longer than a pass may spend reads
/// them over several passes, then seals in `(generation, id)` order.
#[tokio::test]
async fn a_backfill_reads_generations_over_several_passes() {
    let r = repo();
    let ids = build(&r, None).await;
    let opts = CompactOptions {
        max_rows: 8,
        max_seal_pages: 2,
        // One batch of generations a pass (16 of 1 key), then out of time.
        batch: 1,
        max_seal_time: Duration::ZERO,
        list_every: Duration::from_secs(600),
        fanout: 64,
        ..CompactOptions::small(Mode::Build)
    };
    let mut st = CompactState::default();
    let mut passes = 0;
    let mut sealed = 0;
    loop {
        let p = v1(&pass(&r, &opts, &mut st, None).await);
        passes += 1;
        sealed += p.sealed_keys;
        if !p.more {
            break;
        }
        assert!(passes < 40, "{p:?}");
    }
    assert_eq!(sealed as usize, ids.len());
    assert!(passes > ids.len() / 16, "{passes}");
    let (_, ManifestRead::Read(m)) = read_manifest(&r, FAMILY_CHANGESET).await.unwrap() else {
        panic!()
    };
    // Sealed oldest first: each segment's keys are older than the next's.
    let mut gens = Vec::new();
    for s in &m.live {
        let idx = view::load_index(&**r.blobs(), FAMILY_CHANGESET, s)
            .await
            .unwrap()
            .unwrap();
        let mut g = Vec::new();
        for k in idx.keys() {
            g.push(r.generation(ChangesetId(*k)).await.unwrap());
        }
        gens.push((*g.iter().min().unwrap(), *g.iter().max().unwrap()));
    }
    assert!(gens.windows(2).all(|w| w[0].1 <= w[1].0), "{gens:?}");
}

/// Work carried across passes is forgotten when a live segment appears
/// that it does not account for (another server's pass): the next pass
/// lists again and seals nothing twice.
#[tokio::test]
async fn a_carried_backfill_is_dropped_when_another_server_seals() {
    let r = repo();
    let ids = build(&r, None).await;
    let opts = CompactOptions {
        max_rows: 8,
        max_seal_pages: 1,
        list_every: Duration::from_secs(600),
        fanout: 64,
        ..CompactOptions::small(Mode::Build)
    };
    let mut mine = CompactState::default();
    let mut theirs = CompactState::default();
    let p = v1(&pass(&r, &opts, &mut mine, None).await);
    assert!(p.more && p.sealed_keys == 8, "{p:?}");
    let p = v1(&pass(&r, &opts, &mut theirs, None).await);
    assert!(p.listed && p.sealed_keys == 8, "{p:?}");
    let p = v1(&pass(&r, &opts, &mut mine, None).await);
    assert!(p.listed, "{p:?}");
    let mut sealed = 16 + p.sealed_keys;
    for _ in 0..20 {
        let p = v1(&pass(&r, &opts, &mut mine, None).await);
        sealed += p.sealed_keys;
        if !p.more {
            break;
        }
    }
    assert_eq!(sealed as usize, ids.len());
}

/// An index the pass takes from the read view is probed too: a lost index
/// blob is noticed, and its segment dropped.
#[tokio::test(start_paused = true)]
async fn a_lost_index_blob_is_noticed_while_the_view_holds_it() {
    let r = repo();
    let mut st = CompactState::default();
    let live = four_segments(&r, &mut st).await;
    assert!(r
        .segments()
        .family(FAMILY_CHANGESET)
        .segments()
        .iter()
        .any(|l| l.r.index == live[1].index));
    r.blobs().delete(&live[1].index).await.unwrap();
    let opts = CompactOptions {
        fanout: 64,
        ..CompactOptions::small(Mode::Build)
    };
    let p = v1(&pass(&r, &opts, &mut st, None).await);
    assert_eq!(p.dropped, 1, "{p:?}");
    let (_, ManifestRead::Read(m)) = read_manifest(&r, FAMILY_CHANGESET).await.unwrap() else {
        panic!()
    };
    assert!(m.live.iter().all(|s| s.index != live[1].index));
}

/// Rows whose values do not decode, ahead of good ones in generation
/// order: the backfill goes past them and ends, and they are not made
/// candidates again by later passes.
#[tokio::test(start_paused = true)]
async fn undecodable_rows_ahead_of_good_ones_do_not_keep_passes_busy() {
    let r = repo();
    let ids = build(&r, None).await;
    let bad: Vec<Hash> = ids[..24].iter().map(|i| i.0).collect();
    r.row_meta()
        .delete_derived_keys(&[HistoryTree::NAME], &bad)
        .await
        .unwrap();
    for k in &bad {
        r.row_meta()
            .put_derived(Skiplist::NAME, k, Bytes::from_static(b"bad"))
            .await
            .unwrap();
    }
    let opts = CompactOptions {
        max_rows: 8,
        max_seal_pages: 2,
        retire_after: Duration::from_secs(600),
        fanout: 64,
        list_every: Duration::from_secs(600),
        ..CompactOptions::small(Mode::Retire)
    };
    let hook = feature_hook(Arc::default());
    let mut st = CompactState::default();
    let mut bad_values = 0;
    for i in 0.. {
        assert!(i < 20, "passes kept saying there is more");
        let p = v1(&pass(&r, &opts, &mut st, Some(&hook)).await);
        bad_values += p.bad_values;
        if !p.more {
            break;
        }
    }
    assert_eq!(bad_values as usize, bad.len());
    for _ in 0..3 {
        let p = v1(&pass(&r, &opts, &mut st, Some(&hook)).await);
        // Every good row is held; the bad ones are not candidates.
        assert!(!p.more && p.unsealed_rows == 0, "{p:?}");
        assert_eq!((p.sealed_keys, p.bad_values), (0, 0), "{p:?}");
    }
}

/// A blob store counting reads of each blob.
struct CountingBlobs {
    inner: Arc<dyn leep_core::BlobStore>,
    gets: Mutex<std::collections::HashMap<Hash, usize>>,
}

#[async_trait]
impl leep_core::BlobStore for CountingBlobs {
    async fn put_with_hash(&self, hash: Hash, data: Bytes) -> leep_core::Result<()> {
        self.inner.put_with_hash(hash, data).await
    }
    async fn contains(&self, hash: &Hash) -> leep_core::Result<bool> {
        self.inner.contains(hash).await
    }
    async fn get(&self, hash: &Hash) -> leep_core::Result<Option<Bytes>> {
        *self.gets.lock().unwrap().entry(*hash).or_default() += 1;
        self.inner.get(hash).await
    }
    async fn list_page(
        &self,
        after: Option<Hash>,
        limit: usize,
    ) -> leep_core::Result<Vec<leep_core::BlobMeta>> {
        self.inner.list_page(after, limit).await
    }
    async fn delete(&self, hash: &Hash) -> leep_core::Result<()> {
        self.inner.delete(hash).await
    }
}

/// A retirement step whose segment value cannot be read stops at the
/// first such row (the data blob is read once), deletes nothing it did
/// not compare, and leaves the segment to the next pass's probe.
#[tokio::test(start_paused = true)]
async fn a_retirement_step_stops_at_an_unreadable_value() {
    let blobs = Arc::new(CountingBlobs {
        inner: Arc::new(MemoryBlobStore::new()),
        gets: Mutex::default(),
    });
    let r = Repo::new(blobs.clone(), Arc::new(MemoryMetaStore::new()));
    build(&r, None).await;
    let mut st = CompactState::default();
    let opts = CompactOptions {
        fanout: 64,
        ..CompactOptions::small(Mode::Build)
    };
    let p = v1(&pass(&r, &opts, &mut st, None).await);
    assert_eq!(p.sealed_segments, 1, "{p:?}");
    let (_, ManifestRead::Read(m)) = read_manifest(&r, FAMILY_CHANGESET).await.unwrap() else {
        panic!()
    };
    let seg = m.live[0];
    r.blobs().delete(&seg.data).await.unwrap();
    r.blobs()
        .put_with_hash(
            seg.data,
            Bytes::from_static(b"leep:derived-segment-data-v1\0x"),
        )
        .await
        .unwrap();
    blobs.gets.lock().unwrap().clear();
    let retire = CompactOptions {
        fanout: 64,
        ..CompactOptions::small(Mode::Retire)
    };
    let hook = feature_hook(Arc::default());
    let p = v1(&pass(&r, &retire, &mut st, Some(&hook)).await);
    assert_eq!(
        (p.retired_rows, p.mismatched, p.more),
        (0, 0, false),
        "{p:?}"
    );
    assert_eq!(blobs.gets.lock().unwrap().get(&seg.data), Some(&1));
    let p = v1(&pass(&r, &retire, &mut st, Some(&hook)).await);
    assert_eq!(p.dropped, 1, "{p:?}");
}
