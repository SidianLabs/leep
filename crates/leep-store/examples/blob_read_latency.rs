//! Time `get` of random objects from a filesystem blob store, loose or
//! packed, as measured in docs/PERFORMANCE.md ("Packed blobs").
//!
//! ```sh
//! cargo run --release -p leep-store --example blob_read_latency -- <blob dir> [count] [seed] [decoded cache bytes]
//! ```
//! The first pass uses a fresh store (nothing decoded yet), the second reads
//! the same objects again from the same store. The last argument sets the
//! store's cache of decoded packed objects (default 64 MiB).

use std::time::{Duration, Instant};

use leep_core::{BlobStore, Hash};
use leep_store::FsBlobStore;

fn percentile(sorted: &[Duration], p: f64) -> Duration {
    sorted[((sorted.len() - 1) as f64 * p).round() as usize]
}

#[tokio::main]
async fn main() {
    let mut args = std::env::args().skip(1);
    let dir = args
        .next()
        .expect("usage: blob_read_latency <blob dir> [count] [seed] [decoded cache bytes]");
    let count: usize = args.next().map_or(1000, |c| c.parse().unwrap());
    let mut seed: u64 = args.next().map_or(42, |s| s.parse().unwrap());
    let cache: Option<usize> = args.next().map(|c| c.parse().unwrap());
    let store = FsBlobStore::new(&dir);
    let mut all: Vec<Hash> = Vec::new();
    let mut after = None;
    loop {
        let page = store.list_page(after, 1000).await.unwrap();
        let Some(last) = page.last() else { break };
        after = Some(last.hash);
        all.extend(page.iter().map(|m| m.hash));
    }
    let mut picked = Vec::with_capacity(count);
    for _ in 0..count.min(all.len()) {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        picked.push(all[(seed % all.len() as u64) as usize]);
    }
    let store = match cache {
        Some(bytes) => FsBlobStore::with_decoded_cache(&dir, bytes),
        None => FsBlobStore::new(&dir),
    };
    for pass in ["cold", "warm"] {
        let mut times = Vec::with_capacity(picked.len());
        let mut bytes = 0;
        let start = Instant::now();
        for h in &picked {
            let t = Instant::now();
            bytes += store.get(h).await.unwrap().expect("listed object").len();
            times.push(t.elapsed());
        }
        let total = start.elapsed();
        times.sort();
        println!(
            "{pass}: {} gets, {bytes} bytes, total {:.1} ms, mean {:.1} us, p50 {:.1} us, p99 {:.1} us, max {:.1} us",
            times.len(),
            total.as_secs_f64() * 1e3,
            total.as_secs_f64() * 1e6 / times.len() as f64,
            percentile(&times, 0.5).as_secs_f64() * 1e6,
            percentile(&times, 0.99).as_secs_f64() * 1e6,
            times.last().unwrap().as_secs_f64() * 1e6,
        );
    }
}
