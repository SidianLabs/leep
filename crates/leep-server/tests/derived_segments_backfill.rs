//! Measures a derived segment backfill and retirement on Postgres (ADR
//! 0104, "Backfill cost"; ADR 0105, "Cost"): a linear chain of changesets
//! (stored, with no changes) and their `skiplist` (family `changeset-v1`)
//! and `generation` rows (family `changeset-v2`), sealed by passes of the
//! default limits until none says there is more, then retired, both
//! families. Prints each pass's time. The synthetic changesets have no
//! `history-tree-v4` part, root tree, last merge or log filter (they seal
//! as lone rows after the wait, cut here); a real store lists those keys
//! too. Not part of the suite:
//!
//! ```sh
//! LEEP_TEST_POSTGRES_URL=... cargo test --release -p leep-server \
//!     --test derived_segments_backfill -- --ignored --nocapture
//! ```
//!
//! `LEEP_MEASURE_CHANGESETS` (default 200,000) sets the chain's length.

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use leep_core::{BlobStore, Changeset, ChangesetId, DerivedRow, MetaStore};
use leep_derive::derived_segments::{compact_pass, CompactOptions, CompactState, Features, Mode};
use leep_derive::Repo;
use leep_store::FsBlobStore;

/// The skiplist of chain member `i` (generation `i + 1`), given every
/// member's id: ancestors at distances 1, 2, 4, ...
fn skiplist(ids: &[ChangesetId], i: u64) -> Bytes {
    let mut out = Vec::new();
    let mut d = 1;
    while d <= i {
        out.extend_from_slice(ids[(i - d) as usize].0.as_bytes());
        out.extend_from_slice(&(i - d + 1).to_be_bytes());
        d *= 2;
    }
    Bytes::from(out)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn measure_backfill_and_retire() {
    let Some(schema) = leep_meta::pg_test::TestSchema::create("leep_measure_ds_backfill") else {
        eprintln!("skipping: LEEP_TEST_POSTGRES_URL not set");
        return;
    };
    let n: u64 = std::env::var("LEEP_MEASURE_CHANGESETS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(200_000);
    let meta: Arc<dyn MetaStore> = Arc::new(
        leep_meta::PostgresMetaStore::connect_with_schema(&schema.url, &schema.name)
            .await
            .unwrap(),
    );
    let dir = tempfile::tempdir().unwrap();
    let blobs: Arc<dyn BlobStore> = Arc::new(FsBlobStore::new(dir.path()));
    let r = Repo::new(blobs, meta.clone());
    let started = Instant::now();
    let mut ids: Vec<ChangesetId> = Vec::with_capacity(n as usize);
    for from in (0..n).step_by(5_000) {
        let mut changesets = Vec::new();
        let mut rows = Vec::new();
        for i in from..(from + 5_000).min(n) {
            let cs = Changeset {
                parents: ids.last().copied().into_iter().collect(),
                author: "t".into(),
                timestamp: 1,
                message: format!("c{i}"),
                changes: Default::default(),
                extra: Default::default(),
            };
            let id = cs.id();
            ids.push(id);
            rows.push(DerivedRow {
                kind: "skiplist".into(),
                key: id.0,
                value: skiplist(&ids, i),
            });
            rows.push(DerivedRow {
                kind: "generation".into(),
                key: id.0,
                value: Bytes::copy_from_slice(&(i + 1).to_be_bytes()),
            });
            changesets.push(cs);
        }
        meta.put_changesets_with_derived(&changesets, &rows)
            .await
            .unwrap();
    }
    eprintln!("{n} changesets written in {:.1?}", started.elapsed());
    let hook = Features {
        record: Arc::new(|_| Box::pin(async { Ok(()) })),
        recorded: Arc::new(|_| Box::pin(async { Ok(true) })),
    };
    let mut state = CompactState::default();
    for (mode, retire_after) in [
        (Mode::Build, Duration::from_secs(600)),
        (Mode::Retire, Duration::ZERO),
    ] {
        let opts = CompactOptions {
            mode,
            retire_after,
            lone_wait: Duration::ZERO,
            retire_families: CompactOptions::parse_retire_families("all").unwrap(),
            ..CompactOptions::default()
        };
        let all = Instant::now();
        let mut longest = Duration::ZERO;
        for i in 0.. {
            let t = Instant::now();
            let p = compact_pass(&r, &opts, &mut state, Some(&hook))
                .await
                .unwrap()
                .expect("lease");
            let took = t.elapsed();
            longest = longest.max(took);
            let by: Vec<String> = p
                .families
                .iter()
                .map(|(f, r)| {
                    format!(
                        "{f}: sealed {} merged {} retired {} listed {}",
                        r.sealed_keys, r.merges, r.retired_rows, r.listed
                    )
                })
                .collect();
            eprintln!(
                "{mode} pass {i}: {took:.2?} sealed {} merged {} retired {} bad {} unsealed {} \
                 more {} [{}]",
                p.sealed_keys,
                p.merges,
                p.retired_rows,
                p.bad_values,
                p.unsealed_rows,
                p.more,
                by.join("; ")
            );
            if !p.more {
                break;
            }
        }
        eprintln!(
            "{mode}: {:.1?} in all, longest pass {longest:.2?}",
            all.elapsed()
        );
    }
    assert_eq!(meta.count_derived("skiplist").await.unwrap(), 0);
    assert_eq!(meta.count_derived("generation").await.unwrap(), 0);
}
