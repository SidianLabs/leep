//! Where a store's derived bytes go, per kind, and how small the candidate
//! segment encodings of the per-changeset kinds get, as measured in
//! docs/PERFORMANCE.md ("Derived segments, family changeset-v2") and ADR
//! 0105. Reads a store no server is writing:
//!
//! ```sh
//! cargo run --release -p leep-server --example derived_breakdown -- <meta url> <blob url>
//! ```
//!
//! Prints, for every derived kind with rows: rows, key and value bytes, and
//! an estimate of what Postgres stores for them (heap tuple, line pointer and
//! primary key entry, TOAST compression ignored; `SELECT kind,
//! sum(pg_column_size(d.*))` measures the heap part exactly). Then, for the
//! kinds of family `changeset-v2` (`generation`, `last_merge-v1`,
//! `root_tree`, `log-filter-v1`) and the code search kinds, zstd -9 of 64 KiB
//! blocks of the rows in segment order: verbatim, and with the generation and
//! last merge as varints (a candidate codec), plus an estimate of the key
//! index, and the bytes of a segment holding them as this binary encodes
//! it (ADR 0105's codecs: generations and last merges as their eight bytes,
//! which compress better than varints in generation order).

use std::collections::{BTreeMap, HashMap};

use bytes::Bytes;
use leep_core::{Hash, MetaStore};
use leep_derive::derived_segments::codec::{self, SealRow};

const BLOCK: usize = 64 << 10;

fn put_varint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push(v as u8 | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

/// What Postgres stores for one row of `derived (kind text, key bytea,
/// value bytea, primary key (kind, key))`: a heap tuple (23-byte header
/// aligned to 24, the three columns with their varlena headers, aligned to
/// 8) and its 4-byte line pointer, plus a btree entry (8-byte header, kind
/// and key, aligned to 8, and its line pointer) at the default 90% leaf fill.
fn pg_bytes(kind: &str, key: usize, value: usize) -> f64 {
    let vh = |n: usize| if n < 127 { 1 } else { 4 };
    let align = |n: usize| n.div_ceil(8) * 8;
    let heap = align(24 + 1 + kind.len() + vh(key) + key + vh(value) + value) + 4;
    let index = align(8 + 1 + kind.len() + vh(key) + key) + 4;
    heap as f64 + index as f64 / 0.9
}

/// zstd -9 of `rows` cut into blocks of about [`BLOCK`] bytes of `varint
/// length | row`, as a segment column stores them.
fn blocks(rows: &[Vec<u8>]) -> usize {
    let mut total = 0;
    let mut buf = Vec::new();
    for r in rows {
        put_varint(&mut buf, r.len() as u64);
        buf.extend_from_slice(r);
        if buf.len() >= BLOCK {
            total += zstd::bulk::compress(&buf, 9).unwrap().len();
            buf.clear();
        }
    }
    if !buf.is_empty() {
        total += zstd::bulk::compress(&buf, 9).unwrap().len();
    }
    total
}

#[derive(Default)]
struct Kind {
    rows: u64,
    kv: u64,
    pg: f64,
}

async fn all_rows(meta: &dyn MetaStore, kind: &str) -> Vec<(Hash, Bytes)> {
    let mut out = Vec::new();
    let mut after = None;
    loop {
        let page = meta.list_derived(kind, after, 1000).await.unwrap();
        let Some((last, _)) = page.last() else {
            return out;
        };
        after = Some(*last);
        out.extend(page);
    }
}

fn u64_of(v: &[u8]) -> Option<u64> {
    Some(u64::from_be_bytes(v.try_into().ok()?))
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let (meta_url, blob_url) = match args.as_slice() {
        [m, b, ..] => (m.clone(), b.clone()),
        _ => {
            eprintln!("usage: derived_breakdown <meta url> <blob url>");
            std::process::exit(2);
        }
    };
    let meta = leep_meta::open_meta_store(&meta_url).await.unwrap();
    let blobs = leep_store::open_blob_store(&blob_url).unwrap();
    let mut names: Vec<&str> = leep_derive::integrity::kinds::kinds()
        .iter()
        .chain(leep_derive::integrity::kinds::retired())
        .map(|k| k.name)
        // The importer's maps (leep-git), which no other crate names.
        .chain(["git-to-leep-v1", "leep-to-git-v1"])
        .collect();
    names.sort_unstable();
    names.dedup();

    let mut per: BTreeMap<&str, Kind> = BTreeMap::new();
    let mut held: HashMap<&str, Vec<(Hash, Bytes)>> = HashMap::new();
    for name in &names {
        let rows = all_rows(&*meta, name).await;
        if rows.is_empty() {
            continue;
        }
        let k = per.entry(name).or_default();
        for (key, v) in &rows {
            k.rows += 1;
            k.kv += (key.0.len() + v.len()) as u64;
            k.pg += pg_bytes(name, key.0.len(), v.len());
        }
        held.insert(name, rows);
    }
    let total_kv: u64 = per.values().map(|k| k.kv).sum();
    let total_pg: f64 = per.values().map(|k| k.pg).sum();
    println!(
        "{:<28} {:>8} {:>12} {:>9} {:>12} {:>7}",
        "kind", "rows", "key+value", "per row", "est. pg", "share"
    );
    let mut order: Vec<(&&str, &Kind)> = per.iter().collect();
    order.sort_by(|a, b| b.1.pg.total_cmp(&a.1.pg));
    for (name, k) in order {
        println!(
            "{:<28} {:>8} {:>12} {:>9.1} {:>12.0} {:>6.1}%",
            name,
            k.rows,
            k.kv,
            k.kv as f64 / k.rows as f64,
            k.pg,
            100.0 * k.pg / total_pg
        );
    }
    println!(
        "{:<28} {:>8} {:>12} {:>9} {:>12.0}",
        "all", "", total_kv, "", total_pg
    );

    // Family changeset-v2: keys ordered by (generation, id).
    let empty = Vec::new();
    let rows_of = |k: &str| held.get(k).unwrap_or(&empty);
    let gens: HashMap<Hash, u64> = rows_of("generation")
        .iter()
        .filter_map(|(k, v)| Some((*k, u64_of(v)?)))
        .collect();
    let mut keys: Vec<Hash> = ["generation", "last_merge-v1", "root_tree", "log-filter-v1"]
        .iter()
        .flat_map(|k| rows_of(k).iter().map(|(h, _)| *h))
        .collect();
    keys.sort_unstable();
    keys.dedup();
    keys.sort_by_key(|k| (gens.get(k).copied().unwrap_or(0), *k));
    println!("\nfamily changeset-v2: {} keys", keys.len());
    println!(
        "{:<16} {:>10} {:>12} {:>12} {:>12} {:>12}",
        "column", "rows", "key+value", "est. pg", "zstd rows", "zstd codec"
    );
    let (mut all_pg, mut all_codec) = (0.0, 0usize);
    for kind in ["generation", "last_merge-v1", "root_tree", "log-filter-v1"] {
        let by: HashMap<Hash, &Bytes> = rows_of(kind).iter().map(|(k, v)| (*k, v)).collect();
        let mut verbatim = Vec::new();
        let mut coded = Vec::new();
        for k in &keys {
            let Some(v) = by.get(k) else { continue };
            verbatim.push(v.to_vec());
            let mut c = Vec::new();
            match kind {
                "generation" | "last_merge-v1" => put_varint(&mut c, u64_of(v).unwrap_or(0)),
                _ => c.extend_from_slice(v),
            }
            coded.push(c);
        }
        let k = per.get(kind).map_or((0, 0, 0.0), |k| (k.rows, k.kv, k.pg));
        let z = blocks(&coded);
        all_pg += k.2;
        all_codec += z;
        println!(
            "{:<16} {:>10} {:>12} {:>12.0} {:>12} {:>12}",
            kind,
            k.0,
            k.1,
            k.2,
            blocks(&verbatim),
            z
        );
    }
    // `last_merge-v1` as the distance below the changeset's own generation:
    // constant along a first-parent run, and needs `generation` to decode.
    let lm: HashMap<Hash, u64> = rows_of("last_merge-v1")
        .iter()
        .filter_map(|(k, v)| Some((*k, u64_of(v)?)))
        .collect();
    let dist: Vec<Vec<u8>> = keys
        .iter()
        .filter_map(|k| {
            let (g, l) = (gens.get(k)?, lm.get(k)?);
            let mut c = Vec::new();
            put_varint(&mut c, g.saturating_sub(*l));
            Some(c)
        })
        .collect();
    println!(
        "{:<16} {:>10} {:>12} {:>12} {:>12} {:>12}",
        "  (gen - lm)",
        dist.len(),
        "",
        "",
        "",
        blocks(&dist)
    );
    // The key index: 32 bytes a key, its ordinal, a presence bit per column.
    let n = keys.len();
    let mut ords = Vec::new();
    for o in 0..n {
        put_varint(&mut ords, o as u64);
    }
    let index = n * 32 + ords.len() + 4 * n.div_ceil(8);
    println!(
        "key index about {index} bytes; segment about {} bytes against {all_pg:.0} estimated \
         on Postgres ({:.3})",
        index + all_codec,
        (index + all_codec) as f64 / all_pg.max(1.0)
    );
    // As this binary encodes it, when it knows the family.
    if let Some(family) = codec::family_by_name("changeset-v2") {
        let cols: Vec<(u8, &str)> = codec::family(family)
            .unwrap()
            .columns
            .iter()
            .map(|c| (c.id, c.kind))
            .collect();
        let by: Vec<HashMap<Hash, &Bytes>> = cols
            .iter()
            .map(|(_, k)| rows_of(k).iter().map(|(h, v)| (*h, v)).collect())
            .collect();
        let seal: Vec<SealRow> = keys
            .iter()
            .map(|k| SealRow {
                key: *k,
                order: gens.get(k).copied().unwrap_or(0),
                values: cols
                    .iter()
                    .zip(&by)
                    .filter_map(|((c, _), m)| m.get(k).map(|v| (*c, (*v).clone())))
                    .collect(),
            })
            .filter(|r| !r.values.is_empty())
            .collect();
        let (mut idx, mut data, mut segs, mut bad) = (0, 0, 0, 0);
        for chunk in seal.chunks(16_384) {
            let (enc, b) = codec::encode(family, chunk, 1).unwrap();
            idx += enc.index.1.len();
            data += enc.data.1.len();
            segs += 1;
            bad += b.len();
        }
        println!(
            "encoded by this binary: {segs} segment(s), index {idx} + data {data} = {} bytes \
             ({:.1} a key), {bad} values refused",
            idx + data,
            (idx + data) as f64 / n.max(1) as f64
        );
    }

    // Code search kinds, verbatim in key order.
    println!("\ncode search, verbatim rows in key order");
    for kind in [
        "blob-trigrams-v2",
        "blob-trigrams-keyframe-v2",
        "tree-trigrams-v2",
        "tree-trigrams-keyframe-v2",
    ] {
        let rows: Vec<Vec<u8>> = rows_of(kind).iter().map(|(_, v)| v.to_vec()).collect();
        if rows.is_empty() {
            continue;
        }
        let k = &per[kind];
        let z = blocks(&rows);
        println!(
            "{:<28} {:>8} rows, key+value {:>10}, est. pg {:>10.0}, zstd -9 blocks {:>10} \
             (+{} of keys)",
            kind,
            k.rows,
            k.kv,
            k.pg,
            z,
            rows.len() * 33
        );
    }

    // What the blob store holds of derived segments now.
    let repo = leep_derive::Repo::new(blobs.clone(), meta.clone());
    for fam in leep_derive::derived_segments::FAMILIES {
        if let Ok((_, leep_derive::derived_segments::ManifestRead::Read(m))) =
            leep_derive::derived_segments::read_manifest(&repo, fam.id).await
        {
            let data: u64 = m.live.iter().map(|s| s.data_len as u64).sum();
            println!(
                "derived segments of {}: {} live, {data} bytes of data",
                fam.name,
                m.live.len()
            );
        }
    }
}
