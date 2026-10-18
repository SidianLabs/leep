//! Incremental export for the mirror: each new changeset becomes a commit
//! whose tree is its first parent's tree with the changeset's own changes
//! applied, so only changed directories are rewritten and only changed blobs
//! are downloaded.

use std::collections::{BTreeMap, HashMap, HashSet};

use anyhow::{anyhow, bail, Context, Result};
use gix::objs::tree::{Entry, EntryKind, EntryMode};
use gix::ObjectId;
use leep_core::{Changeset, ChangesetId, FileChange, FileKind, Hash};

use super::map::{Map, Origin};
use super::Leep;
use crate::export::{commit_object, sort_entries};
use crate::EXTRA_GIT_COMMIT;

/// Changesets kept in memory between planning and writing; beyond this a
/// long first export fetches them again rather than holding them all.
const PLAN_CACHE: usize = 10_000;
/// Blob ids remembered across exports, to skip downloading a blob a later
/// changeset names again.
const BLOB_CACHE: usize = 200_000;

/// Export options that change commit bytes. They must not change between
/// runs of one mirror, or new exports of old changesets would not match what
/// the remote already has.
#[derive(Clone, Copy, Debug)]
pub struct CommitStyle {
    pub provenance_trailers: bool,
    pub changeset_trailer: bool,
}

pub struct Exporter {
    pub repo: gix::Repository,
    pub map: Map,
    style: CommitStyle,
    blobs: HashMap<Hash, ObjectId>,
    pub commits_written: u64,
}

#[derive(Default)]
struct Edit<'a> {
    files: BTreeMap<&'a str, Option<(EntryKind, ObjectId)>>,
    dirs: BTreeMap<&'a str, Edit<'a>>,
}

fn kind(k: FileKind) -> EntryKind {
    match k {
        FileKind::Regular => EntryKind::Blob,
        FileKind::Executable => EntryKind::BlobExecutable,
        FileKind::Symlink => EntryKind::Link,
    }
}

impl Exporter {
    pub fn new(repo: gix::Repository, map: Map, style: CommitStyle) -> Exporter {
        Exporter {
            repo,
            map,
            style,
            blobs: HashMap::new(),
            commits_written: 0,
        }
    }

    /// The commit of a changeset the map knows and whose object is present.
    pub fn known(&self, id: &ChangesetId) -> Option<ObjectId> {
        self.map.git(id).filter(|oid| self.repo.has_object(oid))
    }

    /// The commit for `rev`, writing every ancestor git does not have yet,
    /// parents first. Deterministic: the same changeset always yields the
    /// same commit, so an export repeated after a crash changes nothing.
    pub fn export(&mut self, leep: &dyn Leep, rev: ChangesetId) -> Result<ObjectId> {
        if let Some(oid) = self.known(&rev) {
            return Ok(oid);
        }
        let mut order = Vec::new();
        let mut fetched: HashMap<ChangesetId, Changeset> = HashMap::new();
        let mut seen = HashSet::new();
        let mut stack = vec![(rev, false)];
        while let Some((id, expanded)) = stack.pop() {
            if expanded {
                order.push(id);
                continue;
            }
            if !seen.insert(id) || self.known(&id).is_some() {
                continue;
            }
            let cs = fetch(leep, &id)?;
            stack.push((id, true));
            for p in cs.parents.iter().rev() {
                stack.push((*p, false));
            }
            if fetched.len() < PLAN_CACHE {
                fetched.insert(id, cs);
            }
        }
        let last = order.len().saturating_sub(1);
        for (n, id) in order.into_iter().enumerate() {
            let cs = match fetched.remove(&id) {
                Some(cs) => cs,
                None => fetch(leep, &id)?,
            };
            self.write_commit(leep, id, &cs)?;
            if n < last {
                super::crash_point("export-commit");
            }
        }
        self.map.sync().context("syncing the mirror map")?;
        self.known(&rev)
            .ok_or_else(|| anyhow!("changeset {rev} has no commit after export"))
    }

    fn write_commit(&mut self, leep: &dyn Leep, id: ChangesetId, cs: &Changeset) -> Result<()> {
        let mut parents = Vec::with_capacity(cs.parents.len());
        for p in &cs.parents {
            parents.push(
                self.known(p)
                    .ok_or_else(|| anyhow!("parent {p} of {id} has no commit"))?,
            );
        }
        let base = match parents.first() {
            Some(p) => Some(
                self.repo
                    .find_object(*p)?
                    .try_into_commit()?
                    .tree_id()?
                    .detach(),
            ),
            None => None,
        };
        let mut edit = Edit::default();
        for (path, change) in &cs.changes {
            let entry = match change {
                FileChange::Delete => None,
                FileChange::Modify {
                    content, kind: k, ..
                } => Some((kind(*k), self.blob(leep, content)?)),
            };
            let mut node = &mut edit;
            let mut parts = path.as_str().split('/').peekable();
            while let Some(part) = parts.next() {
                if parts.peek().is_some() {
                    node = node.dirs.entry(part).or_default();
                } else {
                    node.files.insert(part, entry);
                }
            }
        }
        let tree = match apply(&self.repo, base, &edit)? {
            Some(t) => t,
            None => self.repo.write_object(gix::objs::Tree::empty())?.detach(),
        };
        let mut commit = commit_object(
            cs,
            tree,
            parents.clone(),
            self.style.provenance_trailers,
            None,
        )?;
        // Only a changeset whose export is the commit it came from goes
        // without the trailer. One that names a `git.commit` it does not
        // reproduce (the server's rebased copy of an imported changeset, as
        // a land branch makes) is Leep's own and must say so.
        if self.style.changeset_trailer && !reproduces(self.repo.object_hash(), cs, &commit)? {
            commit = commit_object(cs, tree, parents, self.style.provenance_trailers, Some(id))?;
        }
        let oid = self.repo.write_object(commit)?.detach();
        self.map.insert(id, oid, Origin::Exported)?;
        self.commits_written += 1;
        Ok(())
    }

    fn blob(&mut self, leep: &dyn Leep, hash: &Hash) -> Result<ObjectId> {
        if let Some(oid) = self.blobs.get(hash) {
            return Ok(*oid);
        }
        let data = leep.blob(hash)?;
        if Hash::of_blob(&data) != *hash {
            bail!("blob {hash} from the server does not match its hash");
        }
        let oid = self.repo.write_blob(&data)?.detach();
        if self.blobs.len() >= BLOB_CACHE {
            self.blobs.clear();
        }
        self.blobs.insert(*hash, oid);
        Ok(oid)
    }
}

/// Is `commit` the commit `cs` was imported from?
fn reproduces(kind: gix::hash::Kind, cs: &Changeset, commit: &gix::objs::Commit) -> Result<bool> {
    let Some(want) = cs.extra.get(EXTRA_GIT_COMMIT) else {
        return Ok(false);
    };
    let mut buf = Vec::new();
    gix::objs::WriteTo::write_to(commit, &mut buf)?;
    let oid = gix::objs::compute_hash(kind, gix::objs::Kind::Commit, &buf)?;
    Ok(want.parse::<ObjectId>().is_ok_and(|w| w == oid))
}

fn fetch(leep: &dyn Leep, id: &ChangesetId) -> Result<Changeset> {
    let cs = leep.changeset(id)?;
    if cs.id() != *id {
        bail!("changeset {id} from the server does not match its id");
    }
    Ok(cs)
}

/// Apply `edit` to the tree `base` (None = empty) and write every changed
/// tree. Returns None when the result is empty. Recursion follows directory
/// depth, which path limits bound, not history.
fn apply(repo: &gix::Repository, base: Option<ObjectId>, edit: &Edit) -> Result<Option<ObjectId>> {
    let mut entries: BTreeMap<Vec<u8>, (EntryMode, ObjectId)> = BTreeMap::new();
    if let Some(t) = base {
        let tree = repo.find_object(t)?.try_into_tree()?;
        for e in tree.decode()?.entries {
            entries.insert(e.filename.to_vec(), (e.mode, e.oid.to_owned()));
        }
    }
    let names: std::collections::BTreeSet<&str> =
        edit.files.keys().chain(edit.dirs.keys()).copied().collect();
    for name in names {
        let cur = entries.remove(name.as_bytes());
        let mut new = cur;
        if let Some(sub) = edit.dirs.get(name) {
            let sub_base = cur.filter(|(m, _)| m.is_tree()).map(|(_, o)| o);
            match apply(repo, sub_base, sub)? {
                Some(t) => new = Some((EntryKind::Tree.into(), t)),
                None if sub_base.is_some() => new = None,
                None => {}
            }
        }
        match edit.files.get(name) {
            Some(None) if new.is_some_and(|(m, _)| !m.is_tree()) => new = None,
            Some(Some((k, oid))) => new = Some(((*k).into(), *oid)),
            _ => {}
        }
        if let Some(n) = new {
            entries.insert(name.as_bytes().to_vec(), n);
        }
    }
    if entries.is_empty() {
        return Ok(None);
    }
    let mut list: Vec<Entry> = entries
        .into_iter()
        .map(|(filename, (mode, oid))| Entry {
            mode,
            filename: filename.into(),
            oid,
        })
        .collect();
    sort_entries(&mut list);
    Ok(Some(
        repo.write_object(gix::objs::Tree { entries: list })?
            .detach(),
    ))
}
