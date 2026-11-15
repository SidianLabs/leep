//! Derived segments (ADR 0104, 0105; every family) under faults and kills:
//! lands, the warmer,
//! compactor passes (seal, merge, retire) killed at random points with
//! their writes failing before or after landing, blob GC and the changeset
//! sweep, interleaved. After every round, through a fault-free handle:
//! every live segment is stored and decodes, no row of a stored changeset
//! is gone unless a live segment holds its value, every value equals a
//! fresh derivation, and nothing a changeset references is lost.

use super::*;
use crate::changeset_sweep::{self, SweepOptions};
use crate::derived::{derive, DerivedKind, Generation, HistoryTree, LogFilterIndex, Skiplist};
use crate::derived_segments::view::{Found, ManifestRead};
use crate::derived_segments::{
    self as ds, codec, compact_pass, CompactOptions, CompactState, Features, Mode, FAMILIES,
    FAMILY_CHANGESET, FAMILY_CHANGESET_V2,
};
use leep_core::Hash;

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

/// Store features as a store records them: a pass reads back what it (or
/// an earlier pass) recorded.
type Recorded = Arc<std::sync::Mutex<std::collections::BTreeSet<&'static str>>>;

fn recording_features(set: Recorded) -> Features {
    let seen = set.clone();
    Features {
        record: Arc::new(move |name| {
            set.lock().unwrap().insert(name);
            Box::pin(async { Ok(()) })
        }),
        recorded: Arc::new(move |name| {
            let has = seen.lock().unwrap().contains(name);
            Box::pin(async move { Ok(has) })
        }),
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

/// The invariants above, for every family, through a fresh fault-free
/// handle; and no row of a family was deleted before its store feature was
/// recorded.
async fn check(seed: u64, step: usize, w: &World, recorded: &Recorded) {
    let clean = Repo::new(w.blobs.clone(), w.meta.clone());
    let stored: Vec<Hash> = w
        .meta
        .list_changesets(None, usize::MAX)
        .await
        .unwrap()
        .into_iter()
        .map(|c| c.0)
        .collect();
    // Written with every changeset: all there until the family's door.
    for (feature, kind) in [
        (ds::FEATURE, Skiplist::NAME),
        (ds::FEATURE_2, Generation::NAME),
    ] {
        if recorded.lock().unwrap().contains(feature) {
            continue;
        }
        let rows = w.meta.get_derived_many(kind, &stored).await.unwrap();
        assert!(
            rows.iter().all(Option::is_some),
            "seed {seed} step {step}: a {kind} row was deleted before {feature} was recorded"
        );
    }
    for fam in FAMILIES {
        let (raw, read) = ds::read_manifest(&clean, fam.id).await.unwrap();
        let m = match read {
            ManifestRead::Absent => codec::Manifest::new(fam.id),
            ManifestRead::Read(m) => m,
            ManifestRead::Damaged | ManifestRead::Newer => {
                panic!(
                    "seed {seed} step {step}: the {} manifest does not decode",
                    fam.name
                )
            }
        };
        for s in &m.live {
            let idx = ds::view::load_index(&*w.blobs, fam.id, s)
                .await
                .unwrap()
                .unwrap_or_else(|| {
                    panic!("seed {seed} step {step}: live segment {} lost", s.index)
                });
            let data = w
                .blobs
                .get(&s.data)
                .await
                .unwrap()
                .unwrap_or_else(|| panic!("seed {seed} step {step}: data of {} lost", s.index));
            let data = codec::decode_data(&idx, &s.data, data).unwrap();
            codec::read_all(&idx, &data).unwrap();
        }
        clean.segments().family(fam.id).apply(raw).await.unwrap();
    }
    let (oracle_meta, ids) = without_derived(&clean).await;
    let oracle = Repo::new(w.blobs.clone(), oracle_meta);
    for id in ids {
        for fam in FAMILIES {
            for column in fam.columns {
                let (col, kind) = (column.id, column.kind);
                let row = w.meta.get_derived(kind, &id.0).await.unwrap();
                let seg = match clean
                    .segments()
                    .family(fam.id)
                    .lookup(&id.0, col)
                    .await
                    .unwrap()
                {
                    Found::Yes(v) => Some(v),
                    Found::No => None,
                    Found::Gone(h) => panic!("seed {seed} step {step}: segment {h} unreadable"),
                };
                // Written with the changeset: never missing. The others
                // are the warmer's or a query's, which may lag.
                let with_changeset = [Skiplist::NAME, Generation::NAME, "root_tree"];
                if !with_changeset.contains(&kind) && row.is_none() && seg.is_none() {
                    continue;
                }
                assert!(
                    row.is_some() || seg.is_some(),
                    "seed {seed} step {step}: the {kind} row of {id} is gone and no live \
                     segment holds it"
                );
                let want = crate::integrity::rows::derive_kind(kind, &oracle, id)
                    .await
                    .unwrap();
                for (what, v) in [("row", row), ("segment", seg)] {
                    let Some(v) = v else { continue };
                    assert!(
                        (column.equal)(&v, &want),
                        "seed {seed} step {step}: the {kind} {what} of {id} is wrong"
                    );
                }
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
    let recorded = Recorded::default();
    let feature = recording_features(recorded.clone());
    let mut swept: std::collections::HashSet<Hash> = Default::default();
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
            if rng.below(4) > 0 {
                retry(seed, "warm", || derive::<LogFilterIndex>(&client, id)).await;
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
            let before = w.meta.list_changesets(None, usize::MAX).await.unwrap();
            let o = sweep_opts();
            let later = leep_core::store::unix_now() + 10 * DAY;
            let _ = changeset_sweep::condemn(&gc_repo, &o, &TestClock::at(later)).await;
            let _ = changeset_sweep::collect(
                &gc_repo,
                &o,
                &TestClock::at(later + o.fence.as_secs() as i64 + 1),
            )
            .await;
            let after: std::collections::HashSet<ChangesetId> = w
                .meta
                .list_changesets(None, usize::MAX)
                .await
                .unwrap()
                .into_iter()
                .collect();
            swept.extend(
                before
                    .into_iter()
                    .filter(|c| !after.contains(c))
                    .map(|c| c.0),
            );
        }
        check(seed, step, &w, &recorded).await;
    }

    // Fault-free passes finish what the dead ones left; with GC run after,
    // only live segments remain and every value still reads.
    let clean = Repo::new(w.blobs.clone(), w.meta.clone());
    let mut live_before: std::collections::HashSet<Hash> = Default::default();
    for fam in FAMILIES {
        if let ManifestRead::Read(m) = ds::read_manifest(&clean, fam.id).await.unwrap().1 {
            live_before.extend(m.live.iter().map(|s| s.index));
        }
    }
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
    check(seed, 99, &w, &recorded).await;
    // Swept changesets stay in old segments until a merge; no segment
    // written by the clean passes (a seal or a merge) holds one.
    for fam in FAMILIES {
        let ManifestRead::Read(m) = ds::read_manifest(&clean, fam.id).await.unwrap().1 else {
            continue;
        };
        for s in m.live.iter().filter(|s| !live_before.contains(&s.index)) {
            let idx = ds::view::load_index(&*w.blobs, fam.id, s)
                .await
                .unwrap()
                .unwrap();
            assert!(
                !idx.keys().iter().any(|k| swept.contains(k)),
                "seed {seed}: a segment of {} written after the sweep holds a swept changeset",
                fam.name
            );
        }
    }
    // The sweep exercised what it means to: segments of both families,
    // and in mode retire rows deleted.
    for family in [FAMILY_CHANGESET, FAMILY_CHANGESET_V2] {
        let ManifestRead::Read(m) = ds::read_manifest(&clean, family).await.unwrap().1 else {
            panic!("seed {seed}: no manifest of family {family} after the clean passes")
        };
        assert!(!m.live.is_empty(), "seed {seed} family {family}");
    }
    if opts.mode == Mode::Retire {
        let stored = w
            .meta
            .list_changesets(None, usize::MAX)
            .await
            .unwrap()
            .len() as u64;
        for kind in [Skiplist::NAME, Generation::NAME, LogFilterIndex::NAME] {
            let rows = w.meta.count_derived(kind).await.unwrap();
            assert!(
                rows * 4 < stored,
                "seed {seed}: {rows} {kind} rows of {stored} changesets left"
            );
        }
    }
}

#[tokio::test(start_paused = true)]
async fn derived_segments_under_faults_keep_every_value() {
    for seed in seeds() {
        segments_under_faults(seed).await;
    }
}
