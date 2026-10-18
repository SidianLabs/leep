//! Peak memory and time of a full repack at large object counts, as
//! measured in docs/PERFORMANCE.md ("Repack memory and time"). Objects are
//! small synthetic text files in chains of 32 versions, each hinted against
//! the previous version, as the history walk would.
//!
//! ```sh
//! cargo run --release -p leep-store --example repack_scale -- loose <dir> <objects>
//! cargo run --release -p leep-store --example repack_scale -- packed <dir> <objects> [batch]
//! cargo run --release -p leep-store --example repack_scale -- full <dir> <objects> [threads]
//! ```
//! `loose` writes the objects as loose files; `packed` writes them in
//! batches, each packed by an incremental repack, so a store of millions of
//! objects stays small on disk. `full` repacks everything into one pack in
//! a fresh process and prints its peak resident memory (`VmHWM`).

use std::path::Path;
use std::time::Instant;

use leep_core::intern::{PackHints, NO_ID};
use leep_core::Hash;
use leep_store::{FsBlobStore, RepackMode, RepackOptions};

const CHAIN: u64 = 32;

fn status_kib(field: &str) -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|l| l.starts_with(field))
                .and_then(|l| l.split_whitespace().nth(1)?.parse().ok())
        })
        .unwrap_or(0)
}

/// Version `i % CHAIN` of file `i / CHAIN`: a few hundred bytes, mostly
/// shared with the other versions of the same file.
fn object(i: u64) -> Vec<u8> {
    let (file, version) = (i / CHAIN, i % CHAIN);
    let mut out = format!("file {file}\n").into_bytes();
    for line in 0..8u64 {
        let v = if line == version % 8 { version } else { 0 };
        out.extend_from_slice(
            format!(
                "line {line} of {file}: {} v{v}\n",
                file.wrapping_mul(2654435761) ^ line
            )
            .as_bytes(),
        );
    }
    out
}

/// Hints for objects `from..to`, as the walk gives them: each hash once.
fn hints(from: u64, to: u64) -> PackHints {
    let hashes: Vec<Hash> = (from..to).map(|i| Hash::of_blob(&object(i))).collect();
    let objects = (from..to)
        .map(|i| {
            let id = (i - from) as u32;
            let base = if i % CHAIN == 0 || i == from {
                NO_ID
            } else {
                id - 1
            };
            (id, base)
        })
        .collect();
    PackHints { hashes, objects }
}

fn write_loose(root: &Path, from: u64, to: u64) {
    for i in from..to {
        let data = object(i);
        let hex = Hash::of_blob(&data).to_hex();
        let dir = root.join(&hex[..2]);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join(&hex[2..]), &data).unwrap();
    }
}

#[tokio::main]
async fn main() {
    let mut args = std::env::args().skip(1);
    let usage = "usage: repack_scale loose|packed|full <dir> <objects> [batch|threads]";
    let mode = args.next().expect(usage);
    let root = std::path::PathBuf::from(args.next().expect(usage));
    let n: u64 = args.next().expect(usage).parse().unwrap();
    let extra: Option<u64> = args.next().map(|c| c.parse().unwrap());
    let store = FsBlobStore::new(&root);
    match mode.as_str() {
        "loose" => write_loose(&root, 0, n),
        "packed" => {
            let batch = extra.unwrap_or(500_000);
            let mut from = 0;
            while from < n {
                let to = (from + batch).min(n);
                write_loose(&root, from, to);
                let opts = RepackOptions {
                    mode: RepackMode::Incremental {
                        factor: 2,
                        max_packs: 8,
                    },
                    ..RepackOptions::default()
                };
                let p = store.repack_publish(hints(from, to), opts).await.unwrap();
                p.finish().await.unwrap();
                eprintln!("packed {to} objects");
                from = to;
            }
        }
        "full" => {
            let started = Instant::now();
            let h = hints(0, n);
            let hints_kib = status_kib("VmRSS:");
            let opts = RepackOptions {
                threads: extra.unwrap_or(0) as usize,
                ..RepackOptions::default()
            };
            let p = store.repack_publish(h, opts).await.unwrap();
            let written = started.elapsed();
            let r = p.finish().await.unwrap();
            println!(
                "{{\"objects\":{},\"deltas\":{},\"frames_reused\":{},\"pack_bytes\":{},\
                 \"index_bytes\":{},\"rss_after_hints_kib\":{hints_kib},\"peak_kib\":{},\
                 \"write_seconds\":{:.1},\"pack_id\":{:?}}}",
                r.objects,
                r.deltas,
                r.frames_reused,
                r.pack_bytes,
                r.index_bytes,
                status_kib("VmHWM:"),
                written.as_secs_f64(),
                r.pack_id.unwrap_or_default(),
            );
        }
        _ => panic!("{usage}"),
    }
}
