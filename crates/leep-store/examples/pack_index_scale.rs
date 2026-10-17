//! Lookup time and memory of a pack index at large-history scale, as
//! measured in docs/PERFORMANCE.md ("Packed blobs"). Builds a synthetic
//! index (random hashes, frames back to back, most entries deltas against a
//! recent entry), encodes it, loads it, and times `find`. `load` reads either
//! index layout.
//!
//! ```sh
//! cargo run --release -p leep-store --example pack_index_scale -- build <file> [objects]
//! cargo run --release -p leep-store --example pack_index_scale -- load <file> [lookups]
//! ```
//! `load` runs in a fresh process so its resident-memory delta is the
//! loaded index alone.

use std::time::Instant;

use leep_core::Hash;
use leep_store::pack::{IdxEntry, PackIndex};

fn rss_kib() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|l| l.starts_with("VmRSS:"))
                .and_then(|l| l.split_whitespace().nth(1)?.parse().ok())
        })
        .unwrap_or(0)
}

fn main() {
    let mut args = std::env::args().skip(1);
    let usage = "usage: pack_index_scale build|load <file> [count]";
    let mode = args.next().expect(usage);
    let path = args.next().expect(usage);
    let count: Option<usize> = args.next().map(|c| c.parse().unwrap());
    match mode.as_str() {
        "build" => build(&path, count.unwrap_or(5_000_000)),
        "load" => load(&path, count.unwrap_or(1_000_000)),
        _ => panic!("{usage}"),
    }
}

fn xorshift(mut seed: u64) -> impl FnMut() -> u64 {
    move || {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed
    }
}

fn build(path: &str, n: usize) {
    let seed = 0x9e37_79b9_7f4a_7c15u64;
    let mut next = xorshift(seed);
    let mut entries = Vec::with_capacity(n);
    let mut offset = 8u64;
    for i in 0..n {
        let mut h = [0u8; 32];
        for c in h.chunks_mut(8) {
            c.copy_from_slice(&next().to_be_bytes());
        }
        let stored = 100 + next() % 900;
        let base =
            (i > 0 && !next().is_multiple_of(16)).then(|| i - 1 - (next() as usize % i.min(5000)));
        entries.push((Hash(h), offset, stored, 2000 + next() % 50_000, base));
        offset += stored;
    }
    let built: Vec<IdxEntry> = entries
        .iter()
        .map(|&(hash, offset, stored_len, len, base)| IdxEntry {
            hash,
            offset,
            stored_len,
            len,
            base: base.map(|b| b as u32),
            modified: 1_700_000_000 + (offset % 100_000) as i64,
        })
        .collect();
    let bytes = PackIndex::encode(offset, [0; 32], &built).unwrap();
    std::fs::write(path, &bytes).unwrap();
    println!(
        "{n} objects: index {} bytes ({:.1} per object)",
        bytes.len(),
        bytes.len() as f64 / n as f64
    );
}

fn load(path: &str, lookups: usize) {
    let before = rss_kib();
    let t = Instant::now();
    let bytes = std::fs::read(path).unwrap();
    let index = PackIndex::decode(bytes).unwrap();
    let load = t.elapsed();
    let after = rss_kib();
    let n = index.len();
    let mut next = xorshift(7);
    let probes: Vec<Hash> = (0..lookups)
        .map(|_| index.hash_at(next() as usize % n))
        .collect();
    let t = Instant::now();
    let mut found = 0u64;
    for p in &probes {
        found += index.find(p).map_or(0, |e| e.len);
    }
    let hit = t.elapsed();
    let t = Instant::now();
    let mut missed = 0;
    for p in &probes {
        let mut q = *p;
        q.0[31] ^= 1;
        missed += index.find(&q).is_none() as usize;
    }
    let miss = t.elapsed();
    println!(
        "{n} objects ({}): load {:.0} ms, resident {:.1} bytes per object, \
         find hit {:.0} ns, miss {:.0} ns ({found} {missed})",
        String::from_utf8_lossy(&index.magic()),
        load.as_secs_f64() * 1e3,
        (after.saturating_sub(before) * 1024) as f64 / n as f64,
        hit.as_secs_f64() * 1e9 / lookups as f64,
        miss.as_secs_f64() * 1e9 / lookups as f64,
    );
}
