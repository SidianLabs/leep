//! How many objects of a filesystem blob store are stored more than once:
//! every entry of every published `.idx` (minus tombstoned ones) and every
//! loose file, against the distinct hashes among them. Used to check that
//! land packs (ADR 0068) do not store objects again.
//!
//! ```sh
//! cargo run --release -p leep-store --example pack_dups -- <blob store dir>
//! ```

use std::collections::{HashMap, HashSet};
use std::path::Path;

use leep_core::Hash;
use leep_store::pack::PackIndex;

fn main() {
    let root = std::env::args().nth(1).expect("usage: pack_dups <dir>");
    let root = Path::new(&root);
    let mut tomb: HashMap<String, HashSet<Hash>> = HashMap::new();
    if let Ok(t) = std::fs::read_to_string(root.join("packs/tombstones")) {
        for l in t.lines() {
            if let Some((id, h)) = l.split_once(' ') {
                if let Ok(h) = h.parse() {
                    tomb.entry(id.to_string()).or_default().insert(h);
                }
            }
        }
    }
    // hash -> (copies, logical length)
    let mut seen: HashMap<Hash, (u64, u64)> = HashMap::new();
    let (mut packs, mut entries, mut entry_bytes, mut pack_bytes) = (0u64, 0u64, 0u64, 0u64);
    let mut in_packs = 0u64;
    if let Ok(rd) = std::fs::read_dir(root.join("packs")) {
        for e in rd.flatten() {
            let name = e.file_name().to_string_lossy().into_owned();
            let Some(id) = name.strip_suffix(".idx") else {
                continue;
            };
            let idx = PackIndex::decode(std::fs::read(e.path()).unwrap()).unwrap();
            packs += 1;
            pack_bytes += idx.pack_len();
            let dead = tomb.get(id);
            for ent in idx.iter() {
                if dead.is_some_and(|d| d.contains(&ent.hash)) {
                    continue;
                }
                entries += 1;
                entry_bytes += ent.len;
                let s = seen.entry(ent.hash).or_insert((0, ent.len));
                s.0 += 1;
            }
        }
        in_packs = seen.len() as u64;
    }
    let (mut loose, mut loose_bytes, mut loose_also_packed) = (0u64, 0u64, 0u64);
    for shard in 0..=255u8 {
        let Ok(rd) = std::fs::read_dir(root.join(format!("{shard:02x}"))) else {
            continue;
        };
        for e in rd.flatten() {
            let name = format!("{shard:02x}{}", e.file_name().to_string_lossy());
            let Ok(h) = name.parse::<Hash>() else {
                continue;
            };
            let len = e.metadata().map(|m| m.len()).unwrap_or(0);
            loose += 1;
            loose_bytes += len;
            let s = seen.entry(h).or_insert((0, len));
            if s.0 > 0 {
                loose_also_packed += 1;
            }
            s.0 += 1;
        }
    }
    let distinct = seen.len() as u64;
    let distinct_bytes: u64 = seen.values().map(|(_, l)| l).sum();
    let multi_packed = seen.values().filter(|(n, _)| *n > 1).count();
    println!("packs {packs}, pack bytes {pack_bytes}");
    println!("pack entries {entries} ({entry_bytes} logical bytes), distinct in packs {in_packs}");
    println!("loose {loose} ({loose_bytes} bytes), loose also packed {loose_also_packed}");
    println!("distinct objects {distinct} ({distinct_bytes} logical bytes)");
    println!(
        "copies per object {:.3}, logical bytes stored / distinct {:.3}, objects stored more than once {multi_packed}",
        (entries + loose) as f64 / distinct.max(1) as f64,
        (entry_bytes + loose_bytes) as f64 / distinct_bytes.max(1) as f64
    );
}
