use std::collections::{HashMap, HashSet};
use std::path::Path;

use gix::objs::tree::{Entry, EntryKind};
use gix::ObjectId;
use leep_core::{ChangesetId, Error, FileKind, Hash, Result};
use leep_derive::Repo;

use leep_gitobj::{commit_object, sort_entries};

use crate::{gerr, git_key, lookup_git, lookup_leep, GIT_TO_LEEP, LEEP_TO_GIT};

#[derive(Clone, Debug, Default)]
pub struct ExportOptions {
    /// Append `agent.*` extras as `Leep-Agent-<Key>: value` trailers.
    pub provenance_trailers: bool,
}

#[derive(Clone, Debug, Default)]
pub struct ExportReport {
    pub commits_written: usize,
    pub commits_skipped: usize,
    pub blobs_written: usize,
    pub head: Option<ObjectId>,
}

struct Files {
    /// Directory path -> entries directly inside it.
    dirs: HashMap<String, Vec<Entry>>,
}

fn write_trees(g: &gix::Repository, mut files: Files) -> Result<ObjectId> {
    let mut keys: Vec<String> = files.dirs.keys().cloned().collect();
    // Deepest first so children exist before their parents are written.
    keys.sort_by_key(|k| {
        std::cmp::Reverse(if k.is_empty() {
            0
        } else {
            k.split('/').count()
        })
    });
    let mut root = None;
    for dir in keys {
        let mut entries = files.dirs.remove(&dir).unwrap_or_default();
        sort_entries(&mut entries);
        let oid = g
            .write_object(gix::objs::Tree { entries })
            .map_err(gerr)?
            .detach();
        match dir.rsplit_once('/') {
            Some((parent, name)) => files
                .dirs
                .entry(parent.to_string())
                .or_default()
                .push(Entry {
                    mode: EntryKind::Tree.into(),
                    filename: name.into(),
                    oid,
                }),
            None if dir.is_empty() => root = Some(oid),
            None => files.dirs.entry(String::new()).or_default().push(Entry {
                mode: EntryKind::Tree.into(),
                filename: dir.as_str().into(),
                oid,
            }),
        }
    }
    root.ok_or_else(|| Error::Other("no root tree".into()))
}

/// Unmapped (or missing from `g`) changesets reachable from `rev`, parents
/// first; plus the count of commits found already present.
async fn plan(
    repo: &Repo,
    ts: &gix::ThreadSafeRepository,
    rev: ChangesetId,
) -> Result<(Vec<ChangesetId>, usize)> {
    let mut order = Vec::new();
    let mut seen = HashSet::new();
    let mut skipped = 0;
    let mut stack = vec![(rev, false)];
    while let Some((id, expanded)) = stack.pop() {
        if expanded {
            order.push(id);
            continue;
        }
        if !seen.insert(id) {
            continue;
        }
        if let Some(oid) = lookup_git(&**repo.meta(), &id).await? {
            if ts.to_thread_local().has_object(oid) {
                skipped += 1;
                continue;
            }
        }
        stack.push((id, true));
        let cs = repo.get_changeset(id).await?;
        for p in cs.parents.into_iter().rev() {
            stack.push((p, false));
        }
    }
    Ok((order, skipped))
}

/// Write every changeset reachable from `rev` that git does not have yet and
/// point `refs/heads/<branch>` at the result. Initializes a bare repository if
/// `git_dir` does not exist.
pub async fn export(
    repo: &Repo,
    rev: ChangesetId,
    git_dir: &Path,
    branch: &str,
    opts: &ExportOptions,
) -> Result<ExportReport> {
    let ts = if git_dir.exists() {
        crate::open_git(git_dir)?
    } else {
        gix::init_bare(git_dir).map_err(gerr)?
    }
    .into_sync();
    let (order, skipped) = plan(repo, &ts, rev).await?;
    let mut report = ExportReport {
        commits_skipped: skipped,
        ..Default::default()
    };
    let meta = repo.meta();
    let mut done: HashMap<ChangesetId, ObjectId> = HashMap::new();
    let mut blob_cache: HashMap<Hash, ObjectId> = HashMap::new();

    for id in order {
        let cs = repo.get_changeset(id).await?;
        let mut parents = Vec::new();
        for p in &cs.parents {
            let oid = match done.get(p) {
                Some(o) => *o,
                None => lookup_git(&**meta, p)
                    .await?
                    .ok_or_else(|| Error::Other(format!("parent {p} has no git commit")))?,
            };
            parents.push(oid);
        }

        let mut files = Files {
            dirs: HashMap::from([(String::new(), Vec::new())]),
        };
        for f in repo.list_files(id, "").await? {
            let oid = match blob_cache.get(&f.content) {
                Some(o) => *o,
                None => {
                    let data = repo
                        .blobs()
                        .get(&f.content)
                        .await?
                        .ok_or_else(|| Error::Storage(format!("blob {} missing", f.content)))?;
                    let oid = ts
                        .to_thread_local()
                        .write_blob(&data)
                        .map_err(gerr)?
                        .detach();
                    report.blobs_written += 1;
                    blob_cache.insert(f.content, oid);
                    oid
                }
            };
            let kind = match f.kind {
                FileKind::Regular => EntryKind::Blob,
                FileKind::Executable => EntryKind::BlobExecutable,
                FileKind::Symlink => EntryKind::Link,
            };
            let dir = f.path.parent().unwrap_or("").to_string();
            // Register every ancestor directory so each gets a tree.
            let mut d = dir.as_str();
            while !d.is_empty() {
                files.dirs.entry(d.to_string()).or_default();
                d = d.rsplit_once('/').map_or("", |(p, _)| p);
            }
            files.dirs.entry(dir).or_default().push(Entry {
                mode: kind.into(),
                filename: f.path.file_name().into(),
                oid,
            });
        }

        let g = ts.to_thread_local();
        let tree = write_trees(&g, files)?;
        let commit = commit_object(&cs, tree, parents, opts.provenance_trailers, None)?;
        let oid =
            gix::prelude::Write::write_buf(&g, gix::objs::Kind::Commit, &commit).map_err(gerr)?;
        done.insert(id, oid);
        report.commits_written += 1;

        if lookup_git(&**meta, &id).await?.is_none() {
            // The map names `id`, which a changeset sweep may have condemned
            // since it was read (ADR 0059).
            repo.rescue_changesets(&[id], leep_derive::changeset_gc::RescueBy::Mirror)
                .await?;
            meta.put_derived(
                LEEP_TO_GIT,
                &id.0,
                bytes::Bytes::copy_from_slice(oid.as_bytes()),
            )
            .await?;
        }
        if lookup_leep(&**meta, &oid).await?.is_none() {
            meta.put_derived(
                GIT_TO_LEEP,
                &git_key(oid.as_bytes()),
                bytes::Bytes::copy_from_slice(id.0.as_bytes()),
            )
            .await?;
        }
    }

    let head = match done.get(&rev) {
        Some(o) => *o,
        None => lookup_git(&**meta, &rev)
            .await?
            .ok_or_else(|| Error::Other("head has no git commit".into()))?,
    };
    let mut g = ts.to_thread_local();
    // Updating a ref writes a reflog entry, which needs a committer. Machines
    // without a git identity (CI runners, containers) have none.
    if g.committer().is_none() {
        let mut cfg = g.config_snapshot_mut();
        cfg.set_raw_value(gix::config::tree::Committer::NAME, "leep-git")
            .map_err(gerr)?;
        cfg.set_raw_value(gix::config::tree::Committer::EMAIL, "leep-git@localhost")
            .map_err(gerr)?;
    }
    g.reference(
        format!("refs/heads/{branch}"),
        head,
        gix::refs::transaction::PreviousValue::Any,
        "leep-git export",
    )
    .map_err(gerr)?;
    report.head = Some(head);
    Ok(report)
}
