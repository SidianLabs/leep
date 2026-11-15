//! Derived segments (ADR 0104) under faults and kills: lands, the warmer,
//! compactor passes (seal, merge, retire) killed at random points with
//! their writes failing before or after landing, blob GC and the changeset
//! sweep, interleaved. After every round, through a fault-free handle:
//! every live segment is stored and decodes, no row of a stored changeset
//! is gone unless a live segment holds its value, every value equals a
//! fresh derivation, and nothing a changeset references is lost.

use super::*;
use crate::changeset_sweep::{self, SweepOptions};
use crate::derived::{derive, DerivedKind, HistoryTree, Skiplist};
use crate::derived_segments::view::{Found, ManifestRead};
use crate::derived_segments::{
    self as ds, codec, compact_pass, CompactOptions, CompactState, Features, Mode,
};

const DAY: i64 = 86_400;

fn sweep_opts() -> SweepOptions {
    SweepOptions {
        grace: Duration::from_secs(DAY as u64),
        fence: Duration::from_secs(15 * 60),
        batch: 3,
        rate: 0,
        max_delete: u64::MAX,
        index_fence: Duration::from_secs(60),
        tombstone_retention: Duration::from_secs(DAY as u64),
        ..SweepOptions::default()
    }
}

/// Compactor limits small enough for a few dozen changesets to seal in
/// several segments, merge and retire.
fn compact_opts(rng: &mut Rng, mode: Mode) -> CompactOptions {
    CompactOptions {
        seal_rows: 2 + rng.below(4),
        idle_rows: 1,
        max_rows: 12 + rng.below(30),
        batch: 1 + rng.below(6),
        ..CompactOptions::small(mode)
    }
}

fn no_feature() -> Features {
    Features {
        record: Arc::new(|| Box::pin(async { Ok(()) })),
        recorded: Arc::new(|| Box::pin(async { Ok(true) })),
    }
}

/// One commit of `path` on `parent` through `repo`, retried through faults.
async fn change(
    seed: u64,
    repo: &Repo,
    parent: ChangesetId,
    path: &str,
    body: &str,
) -> ChangesetId {
    let content = retry(seed, "upload", || {
        repo.put_blob(Bytes::from(body.to_string()))
    })
    .await;
    let mut changes = BTreeMap::new();
    changes.insert(
        rp(path),
        FileChange::Modify {
            content,
            kind: FileKind::Regular,
            size: body.len() as u64,
        },
    );
    let cs = Changeset {
        parents: vec![parent],
        author: "t".into(),
        timestamp: 1,
        message: format!("{path} {body}"),
        changes,
        extra: BTreeMap::new(),
    };
    retry(seed, "commit", || repo.create_changeset(cs.clone())).await
}

/// The invariants above, through a fresh fault-free handle.
async fn check(seed: u64, step: usize, w: &World) {
    let clean = Repo::new(w.blobs.clone(), w.meta.clone());
    let (raw, read) = ds::read_manifest(&clean).await.unwrap();
    let m = match read {
        ManifestRead::Absent => codec::Manifest::new(codec::FAMILY_CHANGESET),
        ManifestRead::Read(m) => m,
        ManifestRead::Damaged | ManifestRead::Newer => {
            panic!("seed {seed} step {step}: the manifest does not decode")
        }
    };
    for s in &m.live {
        let idx = ds::view::load_index(&*w.blobs, s)
            .await
            .unwrap()
            .unwrap_or_else(|| panic!("seed {seed} step {step}: live segment {} lost", s.index));
        let data = w
            .blobs
            .get(&s.data)
            .await
            .unwrap()
            .unwrap_or_else(|| panic!("seed {seed} step {step}: data of {} lost", s.index));
        let data = codec::decode_data(&idx, &s.data, data).unwrap();
        codec::read_all(&idx, &data).unwrap();
    }
    clean.segments().apply(raw).await.unwrap();
    let (oracle_meta, ids) = without_derived(&clean).await;
    let oracle = Repo::new(w.blobs.clone(), oracle_meta);
    for id in ids {
        for (col, kind) in [
            (codec::COL_SKIPLIST, Skiplist::NAME),
            (codec::COL_HISTORY_TREE, HistoryTree::NAME),
        ] {
            let row = w.meta.get_derived(kind, &id.0).await.unwrap();
            let seg = match clean.segments().lookup(&id.0, col).await.unwrap() {
                Found::Yes(v) => Some(v),
                Found::No => None,
                Found::Gone(h) => panic!("seed {seed} step {step}: segment {h} unreadable"),
            };
            if kind == HistoryTree::NAME && row.is_none() && seg.is_none() {
                // Not derived yet: the warmer is allowed to lag.
                continue;
            }
            assert!(
                row.is_some() || seg.is_some(),
                "seed {seed} step {step}: the {kind} row of {id} is gone and no live segment \
                 holds it"
            );
            let want = if kind == Skiplist::NAME {
                derive::<Skiplist>(&oracle, id).await.unwrap()
            } else {
                derive::<HistoryTree>(&oracle, id).await.unwrap()
            };
            for (what, v) in [("row", row), ("segment", seg)] {
                let Some(v) = v else { continue };
                let same = if kind == Skiplist::NAME {
                    v == want
                } else {
                    crate::derived::history_pack::same_part(&v, &want)
                };
                assert!(
                    same,
                    "seed {seed} step {step}: the {kind} {what} of {id} is wrong"
                );
            }
        }
    }
    assert_no_dangling(seed, w).await;
}

async fn segments_under_faults(seed: u64) {
    let w = World::new(seed);
    let mut rng = Rng::new(seed ^ 0x5E6);
    let ids = small_history(seed, &w.clean).await;
    let mut tip = *ids.last().unwrap();
    w.meta.cas_ref("main", None, Some(tip)).await.unwrap();
    derive::<HistoryTree>(&w.clean, tip).await.unwrap();
    age_all(&w.blobs).await;

    // The compactor sees failing writes and reads, never damaged ones: like
    // a client deriving rows, a damaged read could seal a wrong value.
    let compact_plan = FaultPlan::new(seed ^ 0x11);
    compact_plan
        .jitter(2)
        .rate(Op::WRITES, 12, Fault::FailAfter)
        .rate(Op::WRITES, 12, Fault::Fail)
        .rate(Op::META, 8, Fault::Fail)
        .rate(Op::BLOB, 8, Fault::Fail);
    let gc_plan = FaultPlan::new(seed ^ 0x12);
    gc_plan
        .jitter(2)
        .rate(Op::WRITES, 30, Fault::FailAfter)
        .rate(Op::WRITES, 30, Fault::Fail)
        .rate(Op::META, 4, Fault::Fail)
        .rate(&[Op::GetDerived, Op::Get], 5, Fault::Corrupt);
    let client_plan = FaultPlan::new(seed ^ 0x13);
    client_plan
        .jitter(2)
        .rate(Op::WRITES, 20, Fault::FailAfter)
        .rate(Op::WRITES, 20, Fault::Fail)
        .rate(Op::META, 6, Fault::Fail);
    let compactor = w.server_with(&compact_plan);
    let gc_repo = w.server_with(&gc_plan);
    let client = w.server_with(&client_plan);
    let feature = no_feature();
    let mut state = CompactState::default();
    let retire_from = 3 + rng.below(6);
    let mut t = now() + 1000;
    for step in 0..12 {
        // Lands on main, and now and then a branch no ref keeps (the
        // changeset sweep's prey).
        for k in 0..1 + rng.below(3) {
            let path = format!("d{}/f{}.txt", rng.below(3), rng.below(4));
            let next = change(seed, &client, tip, &path, &format!("{step}.{k}")).await;
            retry(seed, "ref", || async {
                w.meta.cas_ref("main", Some(tip), Some(next)).await
            })
            .await;
            tip = next;
        }
        let side = if rng.below(3) == 0 {
            Some(change(seed, &client, tip, "side.txt", &format!("side {step}")).await)
        } else {
            None
        };
        // The warmer, through faults.
        for id in std::iter::once(tip).chain(side) {
            if rng.below(4) > 0 {
                retry(seed, "warm", || derive::<HistoryTree>(&client, id)).await;
            }
        }
        // A pass, killed at a random point.
        let mode = if step >= retire_from && !seed.is_multiple_of(3) {
            Mode::Retire
        } else {
            Mode::Build
        };
        let opts = compact_opts(&mut rng, mode);
        let kill_after = if rng.below(3) == 0 {
            rng.below(80)
        } else {
            usize::MAX
        };
        let _ = leep_testkit::cancel_after_polls(
            compact_pass(&compactor, &opts, &mut state, Some(&feature)),
            kill_after,
        )
        .await;
        // Blob GC now and then, killed too.
        if rng.below(3) == 0 {
            let (r, o, clock) = (gc_repo.clone(), gc_opts(), TestClock::at(t));
            let kill = rng.below(120);
            if rng.below(2) == 0 {
                let _ = leep_testkit::cancel_after_polls(gc::condemn(&r, &o, &clock), kill).await;
            } else {
                let _ = leep_testkit::cancel_after_polls(gc::collect(&r, &o, &clock), kill).await;
            }
            t += FENCE + 1;
        }
        // The changeset sweep now and then: unreferenced branches go,
        // rows and all; their keys stay in segments until a merge.
        if rng.below(4) == 0 {
            let o = sweep_opts();
            let later = leep_core::store::unix_now() + 10 * DAY;
            let _ = changeset_sweep::condemn(&gc_repo, &o, &TestClock::at(later)).await;
            let _ = changeset_sweep::collect(
                &gc_repo,
                &o,
                &TestClock::at(later + o.fence.as_secs() as i64 + 1),
            )
            .await;
        }
        check(seed, step, &w).await;
    }

    // Fault-free passes finish what the dead ones left; with GC run after,
    // only live segments remain and every value still reads.
    let clean = Repo::new(w.blobs.clone(), w.meta.clone());
    let mut st = CompactState::default();
    let opts = CompactOptions::small(if !seed.is_multiple_of(3) {
        Mode::Retire
    } else {
        Mode::Build
    });
    for _ in 0..20 {
        let p = compact_pass(&clean, &opts, &mut st, Some(&feature))
            .await
            .unwrap()
            .expect("lease");
        if p.sealed_segments + p.merges + p.retired_rows + p.dropped == 0 {
            break;
        }
    }
    t += 3 * HOUR;
    age_all(&w.blobs).await;
    gc::condemn(&clean, &gc_opts(), &TestClock::at(t))
        .await
        .unwrap();
    t += FENCE + 1;
    gc::collect(&clean, &gc_opts(), &TestClock::at(t))
        .await
        .unwrap();
    check(seed, 99, &w).await;
    // The sweep exercised what it means to: segments, and in mode retire
    // rows deleted.
    let ManifestRead::Read(m) = ds::read_manifest(&clean).await.unwrap().1 else {
        panic!("seed {seed}: no manifest after the clean passes")
    };
    assert!(!m.live.is_empty(), "seed {seed}");
    if opts.mode == Mode::Retire {
        let rows = w.meta.count_derived(Skiplist::NAME).await.unwrap();
        let stored = w
            .meta
            .list_changesets(None, usize::MAX)
            .await
            .unwrap()
            .len() as u64;
        assert!(
            rows * 4 < stored,
            "seed {seed}: {rows} of {stored} rows left"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn derived_segments_under_faults_keep_every_value() {
    for seed in seeds() {
        segments_under_faults(seed).await;
    }
}
