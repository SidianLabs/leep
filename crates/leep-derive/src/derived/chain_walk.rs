//! Finding a missing range a first-parent chain at a time (ADR 0077). The
//! frontier walk of [`super::derive_range`] reads one changeset per round
//! trip when history is linear, so a gap of N changesets costs about 2N
//! serial round trips before anything derives. Here the chain below a
//! changeset is listed down the skiplists, a level of them per batched
//! read, then checked and read a batch at a time.

use std::collections::{HashMap, HashSet};
use std::ops::Range;

use leep_core::{Changeset, ChangesetId, Result};

use super::{get_changesets, get_derived_many, Skiplist, BATCH};
use crate::Repo;

/// A changeset found on the chain: its distance below the start, and the
/// skiplist entries below which its own entries are still to be followed
/// (`j < limit`), so each distance is reached exactly once.
#[derive(Clone, Copy)]
struct Node {
    distance: u64,
    limit: usize,
    id: ChangesetId,
}

/// Find the first-parent chain from `start` (itself included) whose
/// changesets lack a row of `kind`, stopping at the first that has one or
/// that `seen` already holds, as the frontier walk would. Records each
/// one's parents in `parents`/`arena`, marks them seen, and pushes their
/// parents not seen yet onto `next` for the frontier walk (side branches,
/// and wherever the chain could not be listed).
///
/// `start` is in `seen` and its row is unchecked. Every changeset recorded
/// is one the frontier walk would record: its row is checked, and its link
/// to the one above it is read from the changeset itself, so a damaged or
/// missing skiplist row only shortens the chain.
pub(super) async fn follow_chain(
    repo: &Repo,
    kind: &str,
    start: ChangesetId,
    seen: &mut HashSet<ChangesetId>,
    parents: &mut HashMap<ChangesetId, Range<usize>>,
    arena: &mut Vec<ChangesetId>,
    next: &mut Vec<ChangesetId>,
) -> Result<()> {
    let meta = repo.meta();
    // Distances below `stop` lack a row; at `stop` the chain meets a row
    // (`stop_at` names it) or a changeset seen elsewhere.
    let mut stop = u64::MAX;
    let mut stop_at: Option<ChangesetId> = None;
    let mut missing: HashMap<u64, ChangesetId> = HashMap::new();
    let mut level = vec![Node {
        distance: 0,
        limit: usize::MAX,
        id: start,
    }];
    while !level.is_empty() {
        let mut check = Vec::with_capacity(level.len());
        for n in level.drain(..) {
            if n.distance > 0 && seen.contains(&n.id) {
                if n.distance < stop {
                    (stop, stop_at) = (n.distance, None);
                }
            } else {
                check.push(n);
            }
        }
        let mut lacking = Vec::with_capacity(check.len());
        for chunk in check.chunks(BATCH) {
            let ids: Vec<ChangesetId> = chunk.iter().map(|n| n.id).collect();
            let have = get_derived_many(meta.as_ref(), kind, &ids).await?;
            for (n, v) in chunk.iter().zip(have) {
                match v {
                    Some(_) if n.distance < stop => (stop, stop_at) = (n.distance, Some(n.id)),
                    Some(_) => {}
                    None => lacking.push(*n),
                }
            }
        }
        lacking.retain(|n| n.distance < stop);
        for n in &lacking {
            missing.insert(n.distance, n.id);
        }
        // Follow the entries of those whose next distance is still short
        // of a row.
        let expand: Vec<Node> = lacking
            .into_iter()
            .filter(|n| n.limit > 0 && n.distance + 1 < stop)
            .collect();
        for chunk in expand.chunks(BATCH) {
            for (n, list) in chunk.iter().zip(skiplists(repo, chunk).await?) {
                let Some(list) = list else { continue };
                for (j, e) in list.iter().enumerate().take(n.limit) {
                    let Some(d) = 1u64
                        .checked_shl(j as u32)
                        .and_then(|step| n.distance.checked_add(step))
                    else {
                        break;
                    };
                    if d >= stop {
                        break;
                    }
                    level.push(Node {
                        distance: d,
                        limit: j,
                        id: *e,
                    });
                }
            }
        }
    }

    let mut chain: Vec<ChangesetId> = Vec::new();
    while (chain.len() as u64) < stop {
        match missing.get(&(chain.len() as u64)) {
            Some(id) => chain.push(*id),
            None => break,
        }
    }
    drop(missing);
    if chain.is_empty() {
        return Ok(());
    }

    // Read the chain a batch at a time, keeping each changeset only while
    // it is the first parent of the one above.
    let mut kept = 0;
    'read: for chunk in chain.chunks(BATCH) {
        for (cid, cs) in chunk.iter().zip(read_changesets(repo, chunk).await?) {
            let Some(cs) = cs else { break 'read };
            if kept > 0 && arena[parents[&chain[kept - 1]].clone()].first() != Some(cid) {
                break 'read;
            }
            let at = arena.len();
            arena.extend_from_slice(&cs.parents);
            parents.insert(*cid, at..arena.len());
            kept += 1;
        }
    }
    chain.truncate(kept);
    for id in &chain {
        seen.insert(*id);
    }
    if kept as u64 == stop {
        if let Some(r) = stop_at {
            // Its row was just read: no need to check it again.
            seen.insert(r);
        }
    }
    for id in &chain {
        for p in &arena[parents[id].clone()] {
            if seen.insert(*p) {
                next.push(*p);
            }
        }
    }
    Ok(())
}

/// Each node's skiplist, from the cache or read in one batch; `None` where
/// there is no usable row (the chain is then not listed past it).
async fn skiplists(repo: &Repo, nodes: &[Node]) -> Result<Vec<Option<Vec<ChangesetId>>>> {
    let mut out: Vec<Option<Vec<ChangesetId>>> = nodes
        .iter()
        .map(|n| {
            repo.skiplists
                .get(&n.id)
                .map(|l| l.iter().map(|e| e.id).collect())
        })
        .collect();
    let unread: Vec<usize> = (0..nodes.len()).filter(|&i| out[i].is_none()).collect();
    if unread.is_empty() {
        return Ok(out);
    }
    let ids: Vec<ChangesetId> = unread.iter().map(|&i| nodes[i].id).collect();
    let rows = get_derived_many(
        repo.meta().as_ref(),
        <Skiplist as super::DerivedKind>::NAME,
        &ids,
    )
    .await?;
    for (&i, row) in unread.iter().zip(rows) {
        out[i] = row
            .and_then(|b| Skiplist::decode_of(nodes[i].id, &b).ok())
            .map(|l| l.iter().map(|e| e.id).collect());
    }
    Ok(out)
}

/// The changesets `ids`, `None` for any not stored.
async fn read_changesets(repo: &Repo, ids: &[ChangesetId]) -> Result<Vec<Option<Changeset>>> {
    if let Ok(v) = repo.meta().get_changesets(ids).await {
        if v.len() == ids.len() {
            return Ok(v);
        }
    }
    Ok(get_changesets(repo, ids)
        .await?
        .into_iter()
        .map(Some)
        .collect())
}
