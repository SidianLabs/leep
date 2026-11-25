//! Two servers alternating on the derived-segments lease during a backfill
//! keep each other's carried work.
//!
//! A backfill's candidates are carried across passes so that rows are
//! listed once (ADR 0104, "Changed after a third review": listing again
//! every pass made a backfill quadratic). A live segment the process does
//! not account for, which is what the other server's pass publishes, only
//! takes its keys out of the carried candidates; two servers taking turns
//! (both come back 5 s after a pass that says `more`) list about once each.

use std::collections::BTreeMap;
use std::sync::Arc;

use bytes::Bytes;
use leep_core::memory::{MemoryBlobStore, MemoryMetaStore};
use leep_core::{Changeset, ChangesetId, DerivedRow, MetaStore};
use leep_derive::derived_segments::{compact_pass, CompactOptions, CompactState, Mode};
use leep_derive::Repo;

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

/// A chain of `n` stored changesets with their `skiplist` and `generation`
/// rows, as `derived_segments_backfill.rs` builds it.
async fn store(n: u64) -> Repo {
    let meta: Arc<dyn MetaStore> = Arc::new(MemoryMetaStore::new());
    let r = Repo::new(Arc::new(MemoryBlobStore::new()), meta.clone());
    let mut ids: Vec<ChangesetId> = Vec::new();
    let mut changesets = Vec::new();
    let mut rows = Vec::new();
    for i in 0..n {
        let cs = Changeset {
            parents: ids.last().copied().into_iter().collect(),
            author: "t".into(),
            timestamp: 1,
            message: format!("c{i}"),
            changes: BTreeMap::new(),
            extra: BTreeMap::new(),
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
    r
}

fn opts() -> CompactOptions {
    CompactOptions {
        // The shape of the defaults, scaled down: a page of 64 keys and one
        // page a pass, so 2,000 changesets take about 32 passes a family.
        max_seal_pages: 1,
        // As a server in mode `build` between listings: not every pass.
        list_every: std::time::Duration::from_secs(600),
        ..CompactOptions::small(Mode::Build)
    }
}

/// Run passes, the states taking turns, until none says `more`. Returns
/// (passes, passes that listed the rows of `changeset-v1`).
async fn backfill(r: &Repo, states: &mut [CompactState]) -> (u64, u64) {
    let o = opts();
    let (mut passes, mut listed) = (0u64, 0u64);
    let mut quiet = 0;
    for turn in 0..2000usize {
        let st = &mut states[turn % states.len()];
        let p = compact_pass(r, &o, st, None).await.unwrap().expect("lease");
        passes += 1;
        if p.family("changeset-v1").is_some_and(|f| f.listed) {
            listed += 1;
        }
        // Each server stops coming back early once its own pass has no more.
        quiet = if p.more { 0 } else { quiet + 1 };
        if quiet >= states.len() {
            break;
        }
    }
    (passes, listed)
}

#[tokio::test]
async fn two_servers_taking_turns_keep_their_backfills() {
    let n = 2000;
    let one = store(n).await;
    let (_, listed1) = backfill(&one, &mut [CompactState::default()]).await;
    let two = store(n).await;
    let (passes2, listed2) = backfill(
        &two,
        &mut [CompactState::default(), CompactState::default()],
    )
    .await;
    assert!(
        listed1 <= 2,
        "one server lists once (and once to see it is done)"
    );
    assert!(
        listed2 <= 2 * listed1.max(1) + 2,
        "two servers list about once each ({listed2} listings in {passes2} passes, against \
         {listed1})"
    );
    // Everything is sealed once: no key in two segments.
    let segs = two.segments();
    two.meta()
        .get_derived("skiplist", &leep_core::Hash::of_blob(b"refresh"))
        .await
        .unwrap();
    let mut seen = std::collections::HashSet::new();
    for l in segs
        .family(leep_derive::derived_segments::FAMILY_CHANGESET)
        .segments()
    {
        for k in l.idx.keys() {
            assert!(seen.insert(*k), "{k} sealed twice");
        }
    }
    assert_eq!(seen.len() as u64, n);
}
