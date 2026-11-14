//! Cold reads of a file at the end of a long delta chain, for several
//! writer chain caps (ADR 0101; docs/PERFORMANCE.md, "Delta chain cap").
//!
//! ```sh
//! cargo run --release -p leep-store --example chain_depth_latency -- [caps] [samples]
//! ```
//! `caps` is a comma list (default `32,63,128`). For each cap it writes 260
//! versions of a 6 KiB and a 53 KiB text file (one line changed per
//! version, a line added every eighth), packs them with a full repack whose
//! hints chain each version to the one before, as the history walk does,
//! and times the first `get` of the deepest version and of every version:
//!
//! - on a filesystem store, with the pack's pages dropped from the page
//!   cache (`posix_fadvise DONTNEED`) and a fresh store (no decoded
//!   objects) before each read;
//! - on an object store simulated as S3 (`ThrottledStore`: 20 ms a request
//!   and 10 ns a byte), a fresh store before each read.
//!
//! Both stores first read a version of the other file, so the index is
//! loaded and the timed read pays only for its own chain.

use std::sync::Arc;
use std::time::{Duration, Instant};

use leep_core::intern::{PackHints, NO_ID};
use leep_core::{BlobStore, Hash};
use leep_store::{FsBlobStore, ObjectBlobStore, RepackOptions};
use object_store::memory::InMemory;
use object_store::throttle::{ThrottleConfig, ThrottledStore};

const VERSIONS: usize = 260;

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}

/// Every version of a text file of about `bytes`, each a small edit of the
/// one before.
fn versions(bytes: usize, seed: u64) -> Vec<Vec<u8>> {
    let mut rng = Rng(seed);
    let mut lines: Vec<String> = (0..bytes / 48)
        .map(|i| {
            format!(
                "    let value_{i} = compute({}, {});",
                rng.next() % 997,
                i * 7
            )
        })
        .collect();
    let mut out = Vec::with_capacity(VERSIONS);
    for v in 0..VERSIONS {
        if v > 0 {
            let at = (rng.next() % lines.len() as u64) as usize;
            lines[at] = format!(
                "    let value_{at} = compute({}, {v}); // v{v}",
                rng.next() % 997
            );
            if v % 8 == 0 {
                lines.insert(at, format!("    // note {v}: {}", rng.next() % 10_000));
            }
        }
        out.push(lines.join("\n").into_bytes());
    }
    out
}

fn hints(files: &[Vec<Vec<u8>>]) -> PackHints {
    let mut hashes = Vec::new();
    let mut objects = Vec::new();
    for f in files {
        for (v, data) in f.iter().enumerate() {
            let id = hashes.len() as u32;
            hashes.push(Hash::of_blob(data));
            objects.push((id, if v == 0 { NO_ID } else { id - 1 }));
        }
    }
    PackHints { hashes, objects }
}

fn evict(dir: &std::path::Path) {
    #[cfg(target_os = "linux")]
    for e in std::fs::read_dir(dir.join("packs")).unwrap().flatten() {
        let f = std::fs::File::open(e.path()).unwrap();
        let _ = rustix::fs::fadvise(&f, 0, None, rustix::fs::Advice::DontNeed);
    }
    #[cfg(not(target_os = "linux"))]
    let _ = dir;
}

/// The chain depth and frame bytes of `hash` in the store's only pack.
fn chain_of(dir: &std::path::Path, hash: &Hash) -> (usize, u64) {
    for e in std::fs::read_dir(dir.join("packs")).unwrap().flatten() {
        if e.path().extension().is_some_and(|x| x == "idx") {
            let idx =
                leep_store::pack::PackIndex::decode(std::fs::read(e.path()).unwrap()).unwrap();
            let mut cur = (0..idx.len())
                .map(|r| idx.entry(r as u32))
                .find(|x| x.hash == *hash)
                .unwrap();
            let (mut depth, mut bytes) = (0, cur.stored_len);
            while let Some(b) = cur.base {
                cur = idx.entry(b);
                depth += 1;
                bytes += cur.stored_len;
            }
            return (depth, bytes);
        }
    }
    unreachable!("no pack")
}

fn stats(mut t: Vec<Duration>) -> String {
    t.sort();
    let mean = t.iter().sum::<Duration>() / t.len() as u32;
    let p = |q: f64| t[((t.len() - 1) as f64 * q).round() as usize];
    format!(
        "mean {:>8.1?} p50 {:>8.1?} p99 {:>8.1?}",
        mean,
        p(0.5),
        p(0.99)
    )
}

fn throttled() -> Arc<ThrottledStore<InMemory>> {
    let cfg = ThrottleConfig {
        wait_get_per_call: Duration::from_millis(20),
        wait_get_per_byte: Duration::from_nanos(10),
        ..ThrottleConfig::default()
    };
    Arc::new(ThrottledStore::new(InMemory::new(), cfg))
}

#[tokio::main]
async fn main() {
    let mut args = std::env::args().skip(1);
    let caps: Vec<usize> = args
        .next()
        .unwrap_or_else(|| "32,63,128".into())
        .split(',')
        .map(|c| c.parse().unwrap())
        .collect();
    let samples: usize = args.next().map_or(40, |s| s.parse().unwrap());
    let files = vec![versions(8 << 10, 1), versions(64 << 10, 2)];
    for cap in caps {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_path_buf();
        let fs = FsBlobStore::new(&dir);
        for f in &files {
            for v in f {
                fs.put(v.clone().into()).await.unwrap();
            }
        }
        let opts = || RepackOptions {
            max_chain: cap,
            ..RepackOptions::default()
        };
        let r = fs
            .repack_publish(hints(&files), opts())
            .await
            .unwrap()
            .finish()
            .await
            .unwrap();
        // The same objects in a simulated bucket, packed the same way.
        let bucket = throttled();
        let obj = ObjectBlobStore::new(bucket.clone());
        for f in &files {
            for v in f {
                obj.put(v.clone().into()).await.unwrap();
            }
        }
        obj.repack_publish(hints(&files), opts())
            .await
            .unwrap()
            .finish()
            .await
            .unwrap();
        println!(
            "cap {cap}: pack {} bytes, {} deltas of {} objects",
            r.pack_bytes, r.deltas, r.objects
        );
        for (fi, f) in files.iter().enumerate() {
            let other = Hash::of_blob(&files[1 - fi][0]);
            let deepest = f
                .iter()
                .map(|v| Hash::of_blob(v))
                .max_by_key(|h| chain_of(&dir, h).0)
                .unwrap();
            let (depth, bytes) = chain_of(&dir, &deepest);
            let mut fs_deep = Vec::new();
            let mut obj_deep = Vec::new();
            for _ in 0..samples {
                let s = FsBlobStore::new(&dir);
                s.get(&other).await.unwrap().unwrap();
                evict(&dir);
                let t = Instant::now();
                s.get(&deepest).await.unwrap().unwrap();
                fs_deep.push(t.elapsed());
            }
            for _ in 0..samples.min(10) {
                let s = ObjectBlobStore::new(bucket.clone());
                s.get(&other).await.unwrap().unwrap();
                let t = Instant::now();
                s.get(&deepest).await.unwrap().unwrap();
                obj_deep.push(t.elapsed());
            }
            let mut fs_all = Vec::new();
            for v in f {
                let s = FsBlobStore::new(&dir);
                s.get(&other).await.unwrap().unwrap();
                evict(&dir);
                let t = Instant::now();
                s.get(&Hash::of_blob(v)).await.unwrap().unwrap();
                fs_all.push(t.elapsed());
            }
            println!(
                "  {:>3} KiB file, deepest depth {depth} ({bytes} frame bytes):\n    fs cold deepest  {}\n    fs cold any      {}\n    s3 sim deepest   {}",
                f[0].len() >> 10,
                stats(fs_deep),
                stats(fs_all),
                stats(obj_deep),
            );
        }
    }
}
