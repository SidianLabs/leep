//! git -> Leep for the mirror: create changesets over the HTTP API for
//! commits that arrived on the remote. A changeset is built exactly as
//! `leep-git import` builds it, so a commit gets the same id either way.

use std::collections::{HashMap, HashSet};

use anyhow::{anyhow, bail, Context, Result};
use gix::ObjectId;
use leep_core::{ChangesetId, Hash};

use super::export::Exporter;
use super::map::Origin;
use super::Leep;
use crate::export::changeset_trailer;
use crate::import::{changeset_for, prepare};

#[derive(Debug, Default)]
pub struct Imported {
    pub head: Option<ChangesetId>,
    pub commits: u64,
    pub blobs_uploaded: u64,
}

/// Did this commit come from Leep? Either the map says so, or its
/// `Leep-Changeset:` trailer names a changeset whose export is exactly this
/// commit (a trailer alone proves nothing: it survives a cherry-pick).
pub fn leep_origin(
    leep: &dyn Leep,
    ex: &mut Exporter,
    oid: ObjectId,
) -> Result<Option<ChangesetId>> {
    if let Some((id, _)) = ex.map.leep(&oid) {
        return Ok(Some(id));
    }
    let message = {
        let commit = ex.repo.find_object(oid)?.try_into_commit()?;
        let decoded = commit.decode()?;
        String::from_utf8_lossy(decoded.message).into_owned()
    };
    let Some(id) = changeset_trailer(&message) else {
        return Ok(None);
    };
    if !leep.has_changeset(&id)? {
        return Ok(None);
    }
    Ok((ex.export(leep, id)? == oid).then_some(id))
}

/// Import the commits reachable from `head` that Leep does not have, parents
/// first, and return the changeset for `head`.
pub fn import(leep: &dyn Leep, ex: &mut Exporter, head: ObjectId) -> Result<Imported> {
    let mut report = Imported::default();
    let mut order = Vec::new();
    let mut seen = HashSet::new();
    let mut stack = vec![(head, false)];
    while let Some((oid, expanded)) = stack.pop() {
        if expanded {
            order.push(oid);
            continue;
        }
        if !seen.insert(oid) || leep_origin(leep, ex, oid)?.is_some() {
            continue;
        }
        stack.push((oid, true));
        let parents: Vec<ObjectId> = {
            let c = ex.repo.find_object(oid)?.try_into_commit()?;
            let d = c.decode()?;
            d.parents().collect()
        };
        for p in parents.into_iter().rev() {
            stack.push((p, false));
        }
    }

    let ts = ex.repo.clone().into_sync();
    for oid in order {
        let p = prepare(&ts, oid).with_context(|| format!("reading commit {oid}"))?;
        let mut parents = Vec::with_capacity(p.parents.len());
        for gp in &p.parents {
            let (id, _) = ex
                .map
                .leep(gp)
                .ok_or_else(|| anyhow!("parent {gp} of {oid} was not imported"))?;
            parents.push(id);
        }
        // Hash every blob the commit adds, then upload the ones the server
        // lacks, reading each again so only one is held at a time.
        let mut stored: HashMap<ObjectId, (Hash, u64)> = HashMap::new();
        for (path, change) in &p.changes {
            let Some((_, b)) = change else { continue };
            if stored.contains_key(b) {
                continue;
            }
            let data = ex.repo.find_object(*b)?.detach().data;
            if leep_core::hash::is_object_preimage(&data) {
                bail!(
                    "commit {oid}: {path} starts with \"leep:\" and a NUL-terminated \
                     kind, which Leep reserves for its own objects (ADR 0041)"
                );
            }
            stored.insert(*b, (Hash::of_blob(&data), data.len() as u64));
        }
        let by_hash: HashMap<Hash, ObjectId> = stored.iter().map(|(o, (h, _))| (*h, *o)).collect();
        let missing = leep.missing_blobs(by_hash.keys().copied().collect())?;
        for h in missing {
            let b = by_hash[&h];
            let data = ex.repo.find_object(b)?.detach().data;
            leep.put_blob(&h, data)?;
            report.blobs_uploaded += 1;
        }
        let cs = changeset_for(oid, p, parents, |b| {
            stored
                .get(b)
                .copied()
                .ok_or_else(|| leep_core::Error::Other(format!("blob {b} of {oid} not hashed")))
        })?;
        let want = cs.id();
        let id = leep.create_changeset(&cs)?;
        if id != want {
            bail!("server stored commit {oid} as {id}, expected {want}");
        }
        ex.map.insert(id, oid, Origin::Imported)?;
        report.commits += 1;
    }
    // Durable before anything lands: the map is how a later export knows
    // these changesets came from these exact commits.
    ex.map.sync().context("syncing the mirror map")?;
    report.head = ex.map.leep(&head).map(|(id, _)| id);
    Ok(report)
}
