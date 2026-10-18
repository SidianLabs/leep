//! `leep-server repack` (ADR 0029, ADR 0039, ADR 0040): pack the loose
//! objects of a filesystem blob store into a new pack, merging small packs
//! so their count stays low, or with `--full` pack every live object into
//! one pack. An object store (`s3://`) is always packed in full for now.
//! Each file version is compressed against the previous one at its path and
//! each tree node against the same directory in the first parent. Safe
//! beside running servers: the old layout stays readable until a fence after
//! the new pack is published.

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Context;
use clap::Args;
use leep_core::BlobStore;
use leep_derive::Repo;
use leep_store::{
    FsBlobStore, ObjectBlobStore, PublishedObjectPack, PublishedPack, RepackMode, RepackOptions,
    RepackReport,
};

#[derive(Args)]
pub struct RepackArgs {
    /// Pack every live object, loose and packed, into one pack, walking all
    /// of history for delta bases. Drops objects GC deleted from packs.
    /// Without it, only loose objects are packed and the walk covers only
    /// the history they belong to. Object stores always pack in full.
    #[arg(long)]
    full: bool,
    /// Merge packs until each is at least this many times the size (in
    /// objects) of the next smaller one, like `git repack --geometric`.
    #[arg(long, default_value_t = 2, value_parser = clap::value_parser!(u64).range(2..), conflicts_with = "full")]
    geometric: u64,
    /// Then merge the smallest packs until at most this many remain.
    #[arg(long, default_value_t = 8, value_parser = clap::value_parser!(u64).range(1..), conflicts_with = "full")]
    max_packs: u64,
    /// zstd compression level, 1 (fast) to 22 (small).
    #[arg(long, default_value_t = 9, value_parser = clap::value_parser!(i32).range(1..=22))]
    level: i32,
    /// Wait this long after publishing the new pack before deleting the loose
    /// copies and old packs it replaces. Must exceed the servers'
    /// --request-timeout plus clock skew, like `gc --fence`.
    #[arg(long, default_value = "10m", value_parser = super::parse_grace)]
    fence: Duration,
    /// Allow --fence 0. Only safe while no server is running.
    #[arg(long)]
    unsafe_no_fence: bool,
    /// Threads compressing the pack; 0 for one per CPU. The pack's bytes
    /// are the same for any value.
    #[arg(long, default_value_t = 0)]
    threads: usize,
    /// Print the report as one JSON object.
    #[arg(long)]
    json: bool,
}

/// The blob stores that can hold packs.
enum Target {
    Fs(FsBlobStore),
    Object(ObjectBlobStore),
}

enum Published {
    Fs(PublishedPack),
    Object(PublishedObjectPack),
}

impl Target {
    fn open(blobs: &str) -> anyhow::Result<Target> {
        if let Some(root) = leep_store::fs_blob_root(blobs) {
            return Ok(Target::Fs(FsBlobStore::new(root)));
        }
        if let Some(store) = leep_store::open_object_blob_store(blobs)? {
            return Ok(Target::Object(store));
        }
        anyhow::bail!(
            "repack works on filesystem (file:// or a path) and object store (s3://) \
             blob stores; {blobs:?} is neither"
        )
    }

    fn blob_store(&self) -> Arc<dyn BlobStore> {
        match self {
            Target::Fs(s) => Arc::new(s.clone()),
            Target::Object(s) => Arc::new(s.clone()),
        }
    }

    async fn publish(
        &self,
        hints: leep_core::intern::PackHints,
        opts: RepackOptions,
    ) -> leep_core::Result<Published> {
        Ok(match self {
            Target::Fs(s) => Published::Fs(s.repack_publish(hints, opts).await?),
            Target::Object(s) => Published::Object(s.repack_publish(hints, opts).await?),
        })
    }
}

impl Published {
    async fn finish(self) -> leep_core::Result<RepackReport> {
        match self {
            Published::Fs(p) => p.finish().await,
            Published::Object(p) => p.finish().await,
        }
    }
}

pub async fn run_repack(meta: &str, blobs: &str, args: RepackArgs) -> anyhow::Result<()> {
    let target = Target::open(blobs)?;
    if args.fence.is_zero() && !args.unsafe_no_fence {
        anyhow::bail!(
            "a zero fence can fail reads of running servers; pass a positive fence \
             (longer than the servers' request timeout) or --unsafe-no-fence"
        );
    }
    let started = Instant::now();
    let meta = super::open_meta(meta).await.context("opening meta store")?;
    let repo = Repo::new(target.blob_store(), meta);
    let mode = match &target {
        Target::Fs(_) if !args.full => RepackMode::Incremental {
            factor: args.geometric,
            max_packs: args.max_packs as usize,
        },
        _ => RepackMode::Full,
    };
    tracing::info!(?mode, "walking history for delta bases");
    let order = match &target {
        Target::Fs(s) if mode != RepackMode::Full => {
            let s = s.clone();
            leep_derive::pack_order::pack_order_loose(&repo, &move |h| s.is_loose(h)).await?
        }
        _ => leep_derive::pack_order::pack_order(&repo).await?,
    };
    let walked = started.elapsed();
    tracing::info!(
        changesets = order.changesets,
        objects = order.objects.len(),
        "writing the pack"
    );
    let full = mode == RepackMode::Full;
    let opts = RepackOptions {
        level: args.level,
        mode,
        threads: args.threads,
        ..RepackOptions::default()
    };
    let changesets = order.changesets;
    let published = target.publish(order.objects, opts).await?;
    let written = started.elapsed();
    tracing::info!(fence = ?args.fence, "published; waiting out the fence before cleanup");
    tokio::time::sleep(args.fence).await;
    let fenced = started.elapsed();
    let report = published.finish().await?;
    let total = started.elapsed();
    let timing = Timing {
        walk: walked,
        write: written - walked,
        fence: fenced - written,
        cleanup: total - fenced,
        total,
    };
    if args.json {
        println!("{}", json(&report, full, changesets, &timing));
    } else {
        print_report(&report, full, changesets, &timing);
    }
    Ok(())
}

struct Timing {
    walk: Duration,
    write: Duration,
    fence: Duration,
    cleanup: Duration,
    total: Duration,
}

fn json(r: &RepackReport, full: bool, changesets: u64, t: &Timing) -> serde_json::Value {
    serde_json::json!({
        "mode": if full { "full" } else { "incremental" },
        "changesets": changesets,
        "loose_before": r.loose_before,
        "loose_bytes_before": r.loose_bytes_before,
        "packs_before": r.packs_before,
        "packed_before": r.packed_before,
        "pack_bytes_before": r.pack_bytes_before,
        "packs_merged": r.packs_merged,
        "pack_id": r.pack_id,
        "objects": r.objects,
        "logical_bytes": r.logical_bytes,
        "deltas": r.deltas,
        "whole": r.whole,
        "frames_reused": r.frames_reused,
        "pack_bytes": r.pack_bytes,
        "index_bytes": r.index_bytes,
        "dropped_tombstoned": r.dropped_tombstoned,
        "corrupt": r.corrupt.iter().map(|h| h.to_hex()).collect::<Vec<_>>(),
        "vanished": r.vanished,
        "late_tombstones": r.late_tombstones,
        "garbage_removed": r.garbage_removed,
        "leftovers_restored": r.leftovers_restored,
        "loose_removed": r.loose_removed,
        "loose_kept": r.loose_kept,
        "old_packs_removed": r.old_packs_removed,
        "packs_after": r.packs_after,
        "loose_after": r.loose_after,
        "loose_bytes_after": r.loose_bytes_after,
        "seconds": {
            "walk": t.walk.as_secs_f64(),
            "write": t.write.as_secs_f64(),
            "fence": t.fence.as_secs_f64(),
            "cleanup": t.cleanup.as_secs_f64(),
            "total": t.total.as_secs_f64(),
        },
    })
}

fn print_report(r: &RepackReport, full: bool, changesets: u64, t: &Timing) {
    println!(
        "mode: {}",
        if full {
            "full"
        } else {
            "incremental (loose objects, merging packs by the geometric rule)"
        }
    );
    println!("changesets walked: {changesets}");
    println!(
        "before: {} loose objects ({} bytes), {} packs ({} objects, {} bytes)",
        r.loose_before, r.loose_bytes_before, r.packs_before, r.packed_before, r.pack_bytes_before
    );
    match &r.pack_id {
        Some(id) => println!(
            "new pack: {id} ({} bytes, index {} bytes)",
            r.pack_bytes, r.index_bytes
        ),
        None => println!("new pack: none (nothing to pack)"),
    }
    println!(
        "objects packed: {} ({} bytes uncompressed): {} as delta, {} whole",
        r.objects, r.logical_bytes, r.deltas, r.whole
    );
    println!(
        "packs merged into it: {}; packed objects copied without recompressing: {}",
        r.packs_merged, r.frames_reused
    );
    println!("dropped (deleted by gc): {}", r.dropped_tombstoned);
    if r.late_tombstones > 0 {
        println!("deleted by gc during the repack: {}", r.late_tombstones);
    }
    if r.vanished > 0 {
        println!("deleted before they were read: {}", r.vanished);
    }
    if !r.corrupt.is_empty() {
        println!(
            "corrupt loose objects left in place (run `leep-server verify --full --rehash`): {}",
            r.corrupt.len()
        );
        for h in &r.corrupt {
            println!("  {h}");
        }
    }
    if r.garbage_removed + r.leftovers_restored > 0 {
        println!(
            "left by an earlier killed repack: {} files deleted, {} loose copies put back",
            r.garbage_removed, r.leftovers_restored
        );
    }
    println!(
        "loose copies deleted: {}, kept (written again meanwhile): {}",
        r.loose_removed, r.loose_kept
    );
    println!(
        "old packs deleted: {}, packs now: {}",
        r.old_packs_removed, r.packs_after
    );
    println!(
        "after: {} loose objects ({} bytes), pack {} bytes",
        r.loose_after, r.loose_bytes_after, r.pack_bytes
    );
    println!(
        "elapsed: {:.1}s (walk {:.1}s, write {:.1}s, fence {:.1}s, cleanup {:.1}s)",
        t.total.as_secs_f64(),
        t.walk.as_secs_f64(),
        t.write.as_secs_f64(),
        t.fence.as_secs_f64(),
        t.cleanup.as_secs_f64()
    );
}
