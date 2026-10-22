//! Incremental reachability of changesets (ADR 0059 section 4): which
//! stored changesets no root reaches, for the changeset sweep to condemn.
//!
//! A `cs-reach-v1` row on *X* means: *X* is the target of a ref or of a ref
//! log entry, or an ancestor of one, and every parent of *X* has a row. The
//! ref log is never pruned, so a row's reason never goes away and rows are
//! never removed. Rows are written parents first, in batches, so a run
//! killed anywhere leaves only rows whose parents have rows. A run walks
//! from the ref heads and the log entries after the last run's, and stops
//! at rowed changesets, like the incremental blob mark (ADR 0033).
//!
//! The other roots change from run to run, so they get no rows: the
//! landed-copy scan window of every head, attestations, mirror maps and
//! grace. Each run walks them from scratch, through the few changesets
//! nothing rowed reaches.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use bytes::Bytes;
use leep_core::review::{Review, REVIEW_KIND};
use leep_core::signature::{Signature, SIGNATURE_KIND};
use leep_core::{ChangesetId, DerivedRow, Error, Hash, Result};

use crate::derived::{decode_u64, DerivedKind, Generation};
use crate::signatures::REBASED_ORIGIN_KIND;
use crate::Repo;

/// Derived kind recording reachability (see the module docs). Value: the
/// single byte `0x01`. A row counts whatever value it reads with (see
/// `is_row`).
pub const CS_REACH: &str = "cs-reach-v1";

/// The value of every [`CS_REACH`] row.
const REACHED: &[u8] = &[1];

/// The key of the progress row of [`CS_REACH`]: the `seq` of the newest ref
/// log entry whose targets' ancestries have rows (8 bytes, big-endian). No
/// changeset id is all zeros.
pub fn reach_progress_key() -> Hash {
    Hash([0; 32])
}

/// Check a [`CS_REACH`] row: the progress row holds a `seq`, every other
/// row the single byte `0x01`. For verification.
pub fn decode_reach_row(key: &Hash, v: &[u8]) -> Result<()> {
    let ok = if *key == reach_progress_key() {
        v.len() == 8
    } else {
        v == REACHED
    };
    if ok {
        Ok(())
    } else {
        Err(Error::Other(format!("corrupt {CS_REACH} row")))
    }
}

/// Rows per write and ids per batched read.
const BATCH: usize = 1000;

/// The git bridge's map from changeset to commit (`leep_git::LEEP_TO_GIT`),
/// which this crate cannot name.
const LEEP_TO_GIT: &str = "leep-to-git-v1";

#[derive(Clone, Debug)]
pub struct ReachOptions {
    /// Unix seconds that grace is measured back from.
    pub now: i64,
    /// Changesets stored less than this long ago are roots.
    pub grace: Duration,
    /// Ignore the rows and the progress: walk from every root and write
    /// every row again.
    pub full: bool,
}

/// What a run found.
#[derive(Clone, Debug, Default)]
pub struct Reach {
    /// Stored changesets no root reaches, in id order: what a condemn may
    /// condemn.
    pub unreachable: Vec<ChangesetId>,
    /// Rows this run wrote.
    pub rows_written: u64,
    /// Changesets the walk from refs and log entries visited without a row.
    pub walked: u64,
    /// Changesets kept only by a transient root (scan window, attestation,
    /// mirror map, grace).
    pub transient: u64,
    /// Ref log entries read.
    pub log_entries: u64,
    /// Changesets a root reaches that are not stored, so their descendants
    /// cannot get rows. A healthy store has none; `verify` reports them.
    pub missing: u64,
    /// Stored changesets listed.
    pub stored: u64,
}

/// Whether a row is there, whatever its value: only [`REACHED`] is ever
/// written, so a wrong value is a damaged read of a row, and reading it as
/// none would offer a reachable changeset to the sweep. `verify` reports
/// the bad value.
fn is_row(v: &Option<Bytes>) -> bool {
    v.is_some()
}

/// Every row of `kind`, paged.
async fn all_rows(repo: &Repo, kind: &str) -> Result<Vec<(Hash, Bytes)>> {
    let mut out = Vec::new();
    let mut after = None;
    loop {
        let page = repo.meta().list_derived(kind, after, BATCH).await?;
        let Some((last, _)) = page.last() else {
            return Ok(out);
        };
        after = Some(*last);
        out.extend(page);
    }
}

/// Which of `ids` have a [`CS_REACH`] row.
async fn rowed(repo: &Repo, ids: &[ChangesetId]) -> Result<Vec<bool>> {
    let mut out = Vec::with_capacity(ids.len());
    for chunk in ids.chunks(BATCH) {
        let keys: Vec<Hash> = chunk.iter().map(|id| id.0).collect();
        let rows = repo.meta().get_derived_many(CS_REACH, &keys).await?;
        out.extend(rows.iter().map(is_row));
    }
    Ok(out)
}

/// The parents of `ids`, `None` for one that is not stored.
async fn parents_of(repo: &Repo, ids: &[ChangesetId]) -> Result<Vec<Option<Vec<ChangesetId>>>> {
    let mut out = Vec::with_capacity(ids.len());
    for chunk in ids.chunks(BATCH) {
        let found = repo.meta().get_changesets(chunk).await?;
        out.extend(found.into_iter().map(|c| c.map(|c| c.parents)));
    }
    Ok(out)
}

/// The roots that get rows: ref heads and the targets of log entries newer
/// than `after` (all of them when `None`). Returns them, the newest `seq`
/// read and how many entries were read. `seq` follows commit order on every
/// backend (ARCHITECTURE §8, "Ref log"), so no entry commits later with a
/// `seq` at or below one already read.
async fn persistent_roots(
    repo: &Repo,
    after: Option<u64>,
) -> Result<(Vec<ChangesetId>, Option<u64>, u64)> {
    let mut roots: Vec<ChangesetId> = repo.meta().list_refs().await?.into_values().collect();
    let mut newest = after;
    let mut read = 0;
    let mut before = None;
    'pages: loop {
        let page = repo.meta().ref_log(None, before, BATCH).await?;
        let Some(last) = page.last() else {
            break;
        };
        before = Some(last.seq);
        for e in page {
            if after.is_some_and(|a| e.seq <= a) {
                break 'pages;
            }
            read += 1;
            newest = Some(newest.map_or(e.seq, |n| n.max(e.seq)));
            roots.extend(e.old);
            roots.extend(e.new);
        }
    }
    Ok((roots, newest, read))
}

/// Run the mark: write rows for what refs and log entries reach, then find
/// what no root reaches. Writes only [`CS_REACH`] rows.
pub async fn mark(repo: &Repo, opts: &ReachOptions) -> Result<Reach> {
    let mut reach = Reach::default();
    let progress = if opts.full {
        None
    } else {
        match repo
            .meta()
            .get_derived(CS_REACH, &reach_progress_key())
            .await?
        {
            Some(v) => Some(
                decode_u64(&v)
                    .map_err(|_| Error::Other(format!("corrupt {CS_REACH} progress row")))?,
            ),
            None => None,
        }
    };
    let (roots, newest, read) = persistent_roots(repo, progress).await?;
    reach.log_entries = read;

    // Walk from the roots through unrowed changesets, a level at a time.
    let mut parents: HashMap<ChangesetId, Vec<ChangesetId>> = HashMap::new();
    let mut missing: HashSet<ChangesetId> = HashSet::new();
    let mut seen: HashSet<ChangesetId> = HashSet::new();
    let mut frontier: Vec<ChangesetId> = roots.into_iter().filter(|r| seen.insert(*r)).collect();
    while !frontier.is_empty() {
        let has = if opts.full {
            vec![false; frontier.len()]
        } else {
            rowed(repo, &frontier).await?
        };
        let open: Vec<ChangesetId> = frontier
            .iter()
            .zip(has)
            .filter(|(_, r)| !r)
            .map(|(id, _)| *id)
            .collect();
        let mut next = Vec::new();
        for (id, ps) in open.iter().zip(parents_of(repo, &open).await?) {
            match ps {
                None => {
                    missing.insert(*id);
                }
                Some(ps) => {
                    next.extend(ps.iter().copied().filter(|p| seen.insert(*p)));
                    parents.insert(*id, ps);
                }
            }
        }
        frontier = next;
    }
    reach.walked = parents.len() as u64;
    reach.missing = missing.len() as u64;

    // Rows parents first. A changeset with an ancestor that is not stored
    // gets none (its row would claim a closure that does not exist), and
    // then the progress stays where it was, so the next run walks again
    // from the same entries.
    let mut blocked: HashSet<ChangesetId> = missing.clone();
    let mut done: HashSet<ChangesetId> = HashSet::new();
    let mut pending: Vec<DerivedRow> = Vec::new();
    let mut ids: Vec<ChangesetId> = parents.keys().copied().collect();
    ids.sort_unstable();
    for start in ids {
        if done.contains(&start) || blocked.contains(&start) {
            continue;
        }
        // (changeset, whether its parents were pushed)
        let mut stack = vec![(start, false)];
        while let Some((id, expanded)) = stack.pop() {
            if done.contains(&id) || blocked.contains(&id) {
                continue;
            }
            let Some(ps) = parents.get(&id) else {
                // Rowed before this run.
                done.insert(id);
                continue;
            };
            if !expanded {
                stack.push((id, true));
                for p in ps {
                    if !done.contains(p) && !blocked.contains(p) {
                        stack.push((*p, false));
                    }
                }
                continue;
            }
            if ps.iter().any(|p| blocked.contains(p)) {
                blocked.insert(id);
                continue;
            }
            done.insert(id);
            pending.push(DerivedRow::new(CS_REACH, id.0, Bytes::from_static(REACHED)));
            if pending.len() >= BATCH {
                repo.meta().put_derived_many(&pending).await?;
                reach.rows_written += pending.len() as u64;
                pending.clear();
            }
        }
    }
    if !pending.is_empty() {
        repo.meta().put_derived_many(&pending).await?;
        reach.rows_written += pending.len() as u64;
    }
    if let (Some(n), true) = (newest, blocked.is_empty()) {
        if Some(n) != progress {
            repo.meta()
                .put_derived(
                    CS_REACH,
                    &reach_progress_key(),
                    Bytes::copy_from_slice(&n.to_be_bytes()),
                )
                .await?;
        }
    }

    // Everything stored that has no row and was not walked: unreachable
    // unless a transient root reaches it.
    let walked: HashSet<ChangesetId> = parents.keys().copied().collect();
    let grace_cut = opts
        .now
        .saturating_sub(i64::try_from(opts.grace.as_secs()).unwrap_or(i64::MAX));
    let mut unmarked: HashMap<ChangesetId, Vec<ChangesetId>> = HashMap::new();
    let mut transient: Vec<ChangesetId> = Vec::new();
    let mut after = None;
    loop {
        let page = repo.meta().list_changesets(after, BATCH).await?;
        let Some(last) = page.last().copied() else {
            break;
        };
        after = Some(last);
        reach.stored += page.len() as u64;
        let page: Vec<ChangesetId> = page.into_iter().filter(|id| !walked.contains(id)).collect();
        let (has, stored_at) =
            tokio::join!(rowed(repo, &page), repo.meta().changesets_stored_at(&page));
        let open: Vec<(ChangesetId, Option<u64>)> = page
            .iter()
            .zip(has?)
            .zip(stored_at?)
            .filter(|((_, r), _)| !r)
            .map(|((id, _), at)| (*id, at))
            .collect();
        let ids: Vec<ChangesetId> = open.iter().map(|(id, _)| *id).collect();
        for ((id, at), ps) in open.into_iter().zip(parents_of(repo, &ids).await?) {
            // Listed but gone since: nothing to keep.
            let Some(ps) = ps else { continue };
            // Unknown age counts as young.
            if at.is_none_or(|t| i64::try_from(t).unwrap_or(i64::MAX) > grace_cut) {
                transient.push(id);
            }
            unmarked.insert(id, ps);
        }
    }
    if !unmarked.is_empty() {
        let mut ids: Vec<ChangesetId> = unmarked.keys().copied().collect();
        ids.sort_unstable();
        transient.extend(attested(repo, &ids).await?);
        transient.extend(mirrored(repo, &ids).await?);
        transient.extend(window_originals(repo).await?);
    }

    // Through unmarked changesets only: every other stored changeset has a
    // row or was walked, and so is kept with its ancestry already.
    let mut kept: HashSet<ChangesetId> = HashSet::new();
    let mut stack: Vec<ChangesetId> = transient
        .into_iter()
        .filter(|id| unmarked.contains_key(id))
        .collect();
    while let Some(id) = stack.pop() {
        if !kept.insert(id) {
            continue;
        }
        for p in &unmarked[&id] {
            if unmarked.contains_key(p) && !kept.contains(p) {
                stack.push(*p);
            }
        }
    }
    reach.transient = kept.len() as u64;
    let mut unreachable: Vec<ChangesetId> = unmarked
        .into_keys()
        .filter(|id| !kept.contains(id))
        .collect();
    unreachable.sort_unstable();
    reach.unreachable = unreachable;
    Ok(reach)
}

/// Those of `ids` some signature or review names.
async fn attested(repo: &Repo, ids: &[ChangesetId]) -> Result<Vec<ChangesetId>> {
    let mut out = Vec::new();
    let mut signers = HashSet::new();
    for (_, v) in all_rows(repo, SIGNATURE_KIND).await? {
        if let Ok(s) = Signature::decode(&v) {
            signers.insert(s.key_id);
        }
    }
    for k in &signers {
        out.extend(
            having(repo, SIGNATURE_KIND, ids, |id| {
                crate::signatures::storage_key(id, k)
            })
            .await?,
        );
    }
    let mut reviewers = HashSet::new();
    for (_, v) in all_rows(repo, REVIEW_KIND).await? {
        if let Ok(r) = Review::decode(&v) {
            reviewers.insert(r.reviewer);
        }
    }
    for k in &reviewers {
        out.extend(
            having(repo, REVIEW_KIND, ids, |id| {
                crate::reviews::storage_key(id, k)
            })
            .await?,
        );
    }
    Ok(out)
}

/// Those of `ids` the git mirror maps to a commit.
async fn mirrored(repo: &Repo, ids: &[ChangesetId]) -> Result<Vec<ChangesetId>> {
    having(repo, LEEP_TO_GIT, ids, |id| id.0).await
}

/// Those of `ids` with a row of `kind` at `key(id)`.
async fn having(
    repo: &Repo,
    kind: &str,
    ids: &[ChangesetId],
    key: impl Fn(ChangesetId) -> Hash,
) -> Result<Vec<ChangesetId>> {
    let mut out = Vec::new();
    for chunk in ids.chunks(BATCH) {
        let keys: Vec<Hash> = chunk.iter().map(|id| key(*id)).collect();
        let rows = repo.meta().get_derived_many(kind, &keys).await?;
        out.extend(
            chunk
                .iter()
                .zip(rows)
                .filter(|(_, r)| r.is_some())
                .map(|(id, _)| *id),
        );
    }
    Ok(out)
}

/// The originals of every rebased copy in some head's landed-copy scan
/// window (ADR 0059 root 3): the copies among the head's ancestors whose
/// generation is at most `LANDED_COPY_SCAN - 1` below the head's. The scan
/// in `land` visits nothing outside that band.
async fn window_originals(repo: &Repo) -> Result<Vec<ChangesetId>> {
    let heads: HashSet<ChangesetId> = repo.meta().list_refs().await?.into_values().collect();
    let band = crate::land::LANDED_COPY_SCAN as u64 - 1;
    let mut out = Vec::new();
    for head in heads {
        let Some(top) = generations(repo, &[head]).await?[0] else {
            continue;
        };
        let floor = top.saturating_sub(band);
        let mut seen = HashSet::from([head]);
        let mut level = vec![head];
        while !level.is_empty() {
            let keys: Vec<Hash> = level.iter().map(|id| id.0).collect();
            let origins = repo
                .meta()
                .get_derived_many(REBASED_ORIGIN_KIND, &keys)
                .await?;
            out.extend(origins.into_iter().flatten().filter_map(|v| {
                <[u8; 32]>::try_from(v.as_ref())
                    .ok()
                    .map(|a| ChangesetId(Hash(a)))
            }));
            let mut next: Vec<ChangesetId> = Vec::new();
            for ps in parents_of(repo, &level).await?.into_iter().flatten() {
                next.extend(ps.into_iter().filter(|p| seen.insert(*p)));
            }
            let gens = generations(repo, &next).await?;
            level = next
                .into_iter()
                .zip(gens)
                .filter(|(_, g)| g.is_some_and(|g| g >= floor))
                .map(|(id, _)| id)
                .collect();
        }
    }
    Ok(out)
}

/// The generations of `ids`: stored rows in one batch, any missing one
/// derived. `None` for a changeset that is not stored.
async fn generations(repo: &Repo, ids: &[ChangesetId]) -> Result<Vec<Option<u64>>> {
    let mut out = Vec::with_capacity(ids.len());
    for chunk in ids.chunks(BATCH) {
        let keys: Vec<Hash> = chunk.iter().map(|id| id.0).collect();
        let rows = repo
            .meta()
            .get_derived_many(Generation::NAME, &keys)
            .await?;
        for (id, row) in chunk.iter().zip(rows) {
            out.push(match row.and_then(|v| decode_u64(&v).ok()) {
                Some(g) => Some(g),
                None => match repo.generation(*id).await {
                    Ok(g) => Some(g),
                    Err(Error::NotFound(_)) => None,
                    Err(e) => return Err(e),
                },
            });
        }
    }
    Ok(out)
}

/// Every [`CS_REACH`] row's closure is stored: each rowed changeset's
/// parents have rows. What a run killed anywhere must leave; for tests and
/// `verify`.
pub async fn check_reach_rows(repo: &Repo) -> Result<()> {
    let rows = all_rows(repo, CS_REACH).await?;
    let ids: Vec<ChangesetId> = rows
        .iter()
        .filter(|(k, v)| *k != reach_progress_key() && v.as_ref() == REACHED)
        .map(|(k, _)| ChangesetId(*k))
        .collect();
    let have: HashSet<ChangesetId> = ids.iter().copied().collect();
    for (id, ps) in ids.iter().zip(parents_of(repo, &ids).await?) {
        let ps =
            ps.ok_or_else(|| Error::Other(format!("{CS_REACH} row on {id}, which is not stored")))?;
        if let Some(p) = ps.iter().find(|p| !have.contains(p)) {
            return Err(Error::Other(format!(
                "{CS_REACH} row on {id} but not on its parent {p}"
            )));
        }
    }
    Ok(())
}

/// Reachability from scratch, in memory, by the definitions alone: the
/// oracle the incremental mark is checked against.
#[cfg(test)]
pub(crate) async fn unreachable_oracle(repo: &Repo, opts: &ReachOptions) -> Vec<ChangesetId> {
    let all = repo.meta().list_changesets(None, usize::MAX).await.unwrap();
    let mut parents: HashMap<ChangesetId, Vec<ChangesetId>> = HashMap::new();
    for (id, ps) in all.iter().zip(parents_of(repo, &all).await.unwrap()) {
        parents.insert(*id, ps.unwrap());
    }
    let mut roots: Vec<ChangesetId> = repo
        .meta()
        .list_refs()
        .await
        .unwrap()
        .into_values()
        .collect();
    for e in repo.meta().ref_log(None, None, usize::MAX).await.unwrap() {
        roots.extend(e.old);
        roots.extend(e.new);
    }
    // Scan windows, by walking every head's ancestry and testing the band.
    for head in repo.meta().list_refs().await.unwrap().into_values() {
        let top = repo.generation(head).await.unwrap();
        let mut seen = HashSet::from([head]);
        let mut queue = std::collections::VecDeque::from([head]);
        while let Some(id) = queue.pop_front() {
            let g = repo.generation(id).await.unwrap();
            if g + crate::land::LANDED_COPY_SCAN as u64 > top {
                if let Some(o) = repo.rebased_origin(id).await.unwrap() {
                    roots.push(o);
                }
            }
            for p in &parents[&id] {
                if seen.insert(*p) {
                    queue.push_back(*p);
                }
            }
        }
    }
    let at = repo.meta().changesets_stored_at(&all).await.unwrap();
    let cut = opts.now - opts.grace.as_secs() as i64;
    for (id, t) in all.iter().zip(at) {
        if t.unwrap() as i64 > cut {
            roots.push(*id);
        }
    }
    roots.extend(attested(repo, &all).await.unwrap());
    roots.extend(mirrored(repo, &all).await.unwrap());
    let mut kept = HashSet::new();
    let mut stack: Vec<ChangesetId> = roots
        .into_iter()
        .filter(|r| parents.contains_key(r))
        .collect();
    while let Some(id) = stack.pop() {
        if kept.insert(id) {
            stack.extend(parents[&id].iter().copied());
        }
    }
    let mut out: Vec<ChangesetId> = all.into_iter().filter(|id| !kept.contains(id)).collect();
    out.sort_unstable();
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::{commit, repo};
    use leep_core::memory::{MemoryBlobStore, MemoryMetaStore};
    use leep_core::RefUpdateContext;
    use std::sync::Arc;

    fn later() -> ReachOptions {
        ReachOptions {
            now: leep_core::store::unix_now() + 30 * 86_400,
            grace: Duration::from_secs(7 * 86_400),
            full: false,
        }
    }

    #[tokio::test]
    async fn refs_log_and_transient_roots_keep_what_they_reach() {
        let meta = Arc::new(MemoryMetaStore::new());
        let r = Repo::new(Arc::new(MemoryBlobStore::new()), meta.clone());
        let base = commit(&r, &[], &[("f", Some("0"))], "base").await;
        r.meta()
            .cas_ref_logged("main", None, Some(base), &RefUpdateContext::new("cas"))
            .await
            .unwrap();
        let one = commit(&r, &[base], &[("a", Some("1"))], "one").await;
        let two = commit(&r, &[base], &[("b", Some("2"))], "two").await;
        r.land(one, "main").await.unwrap();
        r.land(two, "main").await.unwrap();
        // Abandoned work; a young snapshot; a deleted branch's tip.
        let wip = commit(&r, &[base], &[("w", Some("1"))], "wip").await;
        let young = commit(&r, &[wip], &[("y", Some("1"))], "young").await;
        let gone = commit(&r, &[base], &[("g", Some("1"))], "gone").await;
        r.meta().cas_ref("tmp", None, Some(gone)).await.unwrap();
        r.meta().cas_ref("tmp", Some(gone), None).await.unwrap();
        let mirrored = commit(&r, &[base], &[("m", Some("1"))], "mirrored").await;
        r.meta()
            .put_derived(LEEP_TO_GIT, &mirrored.0, Bytes::from_static(&[9; 20]))
            .await
            .unwrap();
        let o = later();
        meta.set_stored_at(&young, o.now as u64);

        let first = mark(&r, &o).await.unwrap();
        check_reach_rows(&r).await.unwrap();
        // `wip` is kept by its young child; `two` by its copy's window;
        // `gone` by its log entries.
        assert!(first.unreachable.is_empty(), "{:?}", first.unreachable);
        assert_eq!(first.unreachable, unreachable_oracle(&r, &o).await);
        // Once `young` ages, `wip` and `young` go; nothing else does.
        meta.set_stored_at(&young, 0);
        let second = mark(&r, &o).await.unwrap();
        let mut want = vec![wip, young];
        want.sort_unstable();
        assert_eq!(second.unreachable, want);
        assert_eq!(second.unreachable, unreachable_oracle(&r, &o).await);
        assert_eq!(second.rows_written, 0, "nothing new to row");
        assert_eq!(second.log_entries, 0, "no new log entries");
        // `full` agrees, and rewrites every row.
        let full = mark(
            &r,
            &ReachOptions {
                full: true,
                ..o.clone()
            },
        )
        .await
        .unwrap();
        assert_eq!(full.unreachable, second.unreachable);
        assert_eq!(full.rows_written, first.rows_written);
        // A land of the abandoned work makes it reachable for good.
        r.land(young, "main").await.unwrap();
        let third = mark(&r, &o).await.unwrap();
        assert!(third.unreachable.is_empty(), "{:?}", third.unreachable);
        assert!(third.rows_written > 0);
        check_reach_rows(&r).await.unwrap();
    }

    #[tokio::test]
    async fn a_missing_ancestor_gets_no_rows_and_holds_the_progress() {
        let meta = Arc::new(MemoryMetaStore::new());
        let r = Repo::new(Arc::new(MemoryBlobStore::new()), meta.clone());
        let a = commit(&r, &[], &[("f", Some("0"))], "a").await;
        let b = commit(&r, &[a], &[("f", Some("1"))], "b").await;
        r.meta().cas_ref("main", None, Some(b)).await.unwrap();
        assert!(meta.remove_changeset(&a));
        let run = mark(&r, &later()).await.unwrap();
        assert_eq!(run.missing, 1);
        assert_eq!(run.rows_written, 0);
        assert!(run.unreachable.is_empty(), "`b` is walked, so kept");
        let again = mark(&r, &later()).await.unwrap();
        assert_eq!(again.log_entries, 1, "the progress did not move");
        check_reach_rows(&r).await.unwrap();
    }

    #[tokio::test]
    async fn rows_are_never_written_without_their_parents() {
        let r = repo();
        let a = commit(&r, &[], &[("f", Some("0"))], "a").await;
        let b = commit(&r, &[a], &[("f", Some("1"))], "b").await;
        r.meta()
            .put_derived(CS_REACH, &b.0, Bytes::from_static(REACHED))
            .await
            .unwrap();
        assert!(check_reach_rows(&r).await.is_err());
    }
}
