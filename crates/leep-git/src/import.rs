use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;

use bytes::Bytes;
use gix::bstr::ByteSlice;
use gix::objs::tree::EntryKind;
use gix::ObjectId;
use leep_core::{Changeset, ChangesetId, Error, FileChange, FileKind, Hash, RepoPath, Result};
use leep_derive::Repo;

use crate::{
    format_tz, gerr, git_key, lookup_leep, EXTRA_GIT_AUTHOR_TZ, EXTRA_GIT_COMMIT,
    EXTRA_GIT_COMMITTER, EXTRA_GIT_COMMITTER_TZ, EXTRA_GIT_ENCODING, EXTRA_GIT_HEADERS,
    GIT_TO_LEEP, LEEP_TO_GIT,
};

#[derive(Clone, Debug, Default)]
pub struct ImportOptions {
    /// Leep ref to fast-forward to the imported head.
    pub set_ref: Option<String>,
}

#[derive(Clone, Debug, Default)]
pub struct ImportReport {
    pub commits_imported: usize,
    pub commits_skipped: usize,
    pub blobs_uploaded: usize,
    pub submodules_skipped: usize,
    pub head: Option<ChangesetId>,
}

/// One git commit, decoded and diffed against its first parent.
struct Prepared {
    parents: Vec<ObjectId>,
    author: String,
    author_tz: i32,
    time: i64,
    committer: String,
    committer_time: i64,
    committer_tz: i32,
    message: String,
    encoding: Option<String>,
    headers: Vec<(String, String)>,
    changes: Vec<(String, Option<(FileKind, ObjectId)>)>,
    submodules: usize,
}

type Entry = (EntryKind, ObjectId);

fn read_tree(g: &gix::Repository, id: ObjectId) -> Result<BTreeMap<String, Entry>> {
    let obj = g.find_object(id).map_err(gerr)?;
    let tree = obj.try_into_tree().map_err(gerr)?;
    let mut out = BTreeMap::new();
    for e in tree.decode().map_err(gerr)?.entries {
        let name = e
            .filename
            .to_str()
            .map_err(|_| Error::Invalid(format!("non-UTF-8 path in tree {id}")))?;
        out.insert(name.to_string(), (e.mode.kind(), e.oid.to_owned()));
    }
    Ok(out)
}

fn file_kind(k: EntryKind) -> Option<FileKind> {
    match k {
        EntryKind::Blob => Some(FileKind::Regular),
        EntryKind::BlobExecutable => Some(FileKind::Executable),
        EntryKind::Link => Some(FileKind::Symlink),
        _ => None,
    }
}

fn join(prefix: &str, name: &str) -> String {
    if prefix.is_empty() {
        name.to_string()
    } else {
        format!("{prefix}/{name}")
    }
}

/// All files under a tree, as `(path, entry)`. Gitlinks are counted, not listed.
fn flatten(
    g: &gix::Repository,
    prefix: &str,
    tree: ObjectId,
    subs: &mut usize,
) -> Result<Vec<(String, Entry)>> {
    let mut out = Vec::new();
    let mut stack = vec![(prefix.to_string(), tree)];
    while let Some((p, t)) = stack.pop() {
        for (name, (k, oid)) in read_tree(g, t)? {
            match k {
                EntryKind::Tree => stack.push((join(&p, &name), oid)),
                EntryKind::Commit => *subs += 1,
                _ => out.push((join(&p, &name), (k, oid))),
            }
        }
    }
    Ok(out)
}

fn prepare(ts: &gix::ThreadSafeRepository, id: ObjectId) -> Result<Prepared> {
    let g = ts.to_thread_local();
    let commit = g
        .find_object(id)
        .map_err(gerr)?
        .try_into_commit()
        .map_err(gerr)?;
    let c = commit.decode().map_err(gerr)?;
    let tree = c.tree();
    let parents: Vec<ObjectId> = c.parents().collect();
    let author = c.author().map_err(gerr)?;
    let committer = c.committer().map_err(gerr)?;
    let ident = |s: &gix::actor::SignatureRef<'_>| {
        format!("{} <{}>", s.name.to_str_lossy(), s.email.to_str_lossy())
    };
    let (atime, ctime) = (
        author.time().map_err(gerr)?,
        committer.time().map_err(gerr)?,
    );
    let message = c.message.to_str_lossy().into_owned();
    let encoding = c.encoding.map(|e| e.to_str_lossy().into_owned());
    let headers: Vec<(String, String)> = c
        .extra_headers
        .iter()
        .map(|(k, v)| (k.to_str_lossy().into_owned(), v.to_str_lossy().into_owned()))
        .collect();

    let old_tree = match parents.first() {
        Some(p) => {
            let pc = g
                .find_object(*p)
                .map_err(gerr)?
                .try_into_commit()
                .map_err(gerr)?;
            Some(pc.tree_id().map_err(gerr)?.detach())
        }
        None => None,
    };

    let mut changes = Vec::new();
    let mut subs = 0usize;
    let mut stack: Vec<(String, Option<ObjectId>, Option<ObjectId>)> =
        vec![(String::new(), old_tree, Some(tree))];
    while let Some((prefix, old, new)) = stack.pop() {
        if old == new {
            continue;
        }
        let olds = old
            .map(|t| read_tree(&g, t))
            .transpose()?
            .unwrap_or_default();
        let news = new
            .map(|t| read_tree(&g, t))
            .transpose()?
            .unwrap_or_default();
        let names: HashSet<&String> = olds.keys().chain(news.keys()).collect();
        for name in names {
            let path = join(&prefix, name);
            let (o, n) = (olds.get(name).copied(), news.get(name).copied());
            if o == n {
                continue;
            }
            let o_tree = o.filter(|e| e.0 == EntryKind::Tree).map(|e| e.1);
            let n_tree = n.filter(|e| e.0 == EntryKind::Tree).map(|e| e.1);
            if o_tree.is_some() && n_tree.is_some() {
                stack.push((path, o_tree, n_tree));
                continue;
            }
            if let Some(t) = o_tree {
                let mut ignored = 0;
                for (p, _) in flatten(&g, &path, t, &mut ignored)? {
                    changes.push((p, None));
                }
            }
            if let Some(t) = n_tree {
                for (p, (k, oid)) in flatten(&g, &path, t, &mut subs)? {
                    changes.push((p, Some((file_kind(k).expect("flatten yields files"), oid))));
                }
            }
            if matches!(n, Some((EntryKind::Commit, _))) {
                subs += 1;
            }
            match (n, o) {
                (Some((k, oid)), _) if file_kind(k).is_some() => {
                    changes.push((path, Some((file_kind(k).expect("checked"), oid))));
                }
                // Replaced by a directory, a gitlink or nothing.
                (_, Some((k, _))) if file_kind(k).is_some() => changes.push((path, None)),
                _ => {}
            }
        }
    }
    Ok(Prepared {
        parents,
        author: ident(&author),
        author_tz: atime.offset,
        time: atime.seconds,
        committer: ident(&committer),
        committer_time: ctime.seconds,
        committer_tz: ctime.offset,
        message,
        encoding,
        headers,
        changes,
        submodules: subs,
    })
}

fn read_blob(ts: &gix::ThreadSafeRepository, oid: ObjectId) -> Result<Vec<u8>> {
    let g = ts.to_thread_local();
    let data = g.find_object(oid).map_err(gerr)?.detach().data;
    Ok(data)
}

/// Commits reachable from `head` that are not yet mapped, parents first, plus
/// the number of mapped commits met on the way.
async fn plan(
    repo: &Repo,
    ts: &gix::ThreadSafeRepository,
    head: ObjectId,
) -> Result<(Vec<ObjectId>, usize)> {
    let mut order = Vec::new();
    let mut seen = HashSet::new();
    let mut skipped = 0;
    let mut stack = vec![(head, false)];
    while let Some((oid, expanded)) = stack.pop() {
        if expanded {
            order.push(oid);
            continue;
        }
        if !seen.insert(oid) {
            continue;
        }
        if lookup_leep(&**repo.meta(), &oid).await?.is_some() {
            skipped += 1;
            continue;
        }
        stack.push((oid, true));
        let parents: Vec<ObjectId> = {
            let g = ts.to_thread_local();
            let c = g
                .find_object(oid)
                .map_err(gerr)?
                .try_into_commit()
                .map_err(gerr)?;
            let d = c.decode().map_err(gerr)?;
            d.parents().collect()
        };
        // Reversed so the first parent is emitted first.
        for p in parents.into_iter().rev() {
            stack.push((p, false));
        }
    }
    Ok((order, skipped))
}

/// Import the commit DAG reachable from `rev`. Already-mapped commits are
/// skipped, so repeated runs only import what is new.
pub async fn import(
    repo: &Repo,
    git_dir: &Path,
    rev: &str,
    opts: &ImportOptions,
) -> Result<ImportReport> {
    let ts = gix::open(git_dir).map_err(gerr)?.into_sync();
    let head = {
        let g = ts.to_thread_local();
        let obj = g
            .rev_parse_single(rev)
            .map_err(gerr)?
            .object()
            .map_err(gerr)?;
        let id = obj.peel_to_commit().map_err(gerr)?.id;
        id
    };
    let (order, skipped) = plan(repo, &ts, head).await?;
    let mut report = ImportReport {
        commits_skipped: skipped,
        ..Default::default()
    };
    let mut blob_cache: HashMap<ObjectId, (Hash, u64)> = HashMap::new();
    let meta = repo.meta();

    for oid in order {
        let p = prepare(&ts, oid)?;
        let mut parents = Vec::new();
        for gp in &p.parents {
            parents.push(
                lookup_leep(&**meta, gp)
                    .await?
                    .ok_or_else(|| Error::Other(format!("parent {gp} of {oid} not imported")))?,
            );
        }
        let mut changes = BTreeMap::new();
        for (path, change) in p.changes {
            let path = RepoPath::new(path)?;
            let change = match change {
                None => FileChange::Delete,
                Some((kind, blob)) => {
                    let (content, size) = match blob_cache.get(&blob) {
                        Some(v) => *v,
                        None => {
                            let data = read_blob(&ts, blob)?;
                            let size = data.len() as u64;
                            let h = repo.put_blob(Bytes::from(data)).await?;
                            report.blobs_uploaded += 1;
                            blob_cache.insert(blob, (h, size));
                            (h, size)
                        }
                    };
                    FileChange::Modify {
                        content,
                        kind,
                        size,
                    }
                }
            };
            changes.insert(path, change);
        }
        report.submodules_skipped += p.submodules;
        let mut extra = BTreeMap::from([
            (EXTRA_GIT_COMMIT.to_string(), oid.to_string()),
            (
                EXTRA_GIT_COMMITTER.to_string(),
                format!("{} {}", p.committer, p.committer_time),
            ),
            (EXTRA_GIT_AUTHOR_TZ.to_string(), format_tz(p.author_tz)),
            (
                EXTRA_GIT_COMMITTER_TZ.to_string(),
                format_tz(p.committer_tz),
            ),
        ]);
        if let Some(e) = p.encoding {
            extra.insert(EXTRA_GIT_ENCODING.to_string(), e);
        }
        if !p.headers.is_empty() {
            let json = serde_json::to_string(&p.headers).map_err(gerr)?;
            extra.insert(EXTRA_GIT_HEADERS.to_string(), json);
        }
        let id = repo
            .create_changeset(Changeset {
                parents,
                author: p.author,
                timestamp: p.time,
                message: p.message,
                changes,
                extra,
            })
            .await?;
        meta.put_derived(
            GIT_TO_LEEP,
            &git_key(oid.as_bytes()),
            Bytes::copy_from_slice(id.0.as_bytes()),
        )
        .await?;
        meta.put_derived(LEEP_TO_GIT, &id.0, Bytes::copy_from_slice(oid.as_bytes()))
            .await?;
        report.commits_imported += 1;
    }

    let head_id = lookup_leep(&**meta, &head)
        .await?
        .ok_or_else(|| Error::Other("head not mapped after import".into()))?;
    report.head = Some(head_id);

    if let Some(name) = &opts.set_ref {
        let cur = meta.get_ref(name).await?;
        if cur != Some(head_id) {
            if let Some(c) = cur {
                if !repo.is_ancestor(c, head_id).await? {
                    return Err(Error::Invalid(format!(
                        "ref {name} at {c} is not an ancestor of {head_id}; refusing non-fast-forward"
                    )));
                }
            }
            if !meta.cas_ref(name, cur, Some(head_id)).await? {
                return Err(Error::Other(format!("ref {name} changed concurrently")));
            }
        }
    }
    Ok(report)
}
