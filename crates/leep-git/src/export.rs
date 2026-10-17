use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;

use gix::objs::tree::{Entry, EntryKind};
use gix::ObjectId;
use leep_core::{ChangesetId, Error, FileKind, Hash, Result};
use leep_derive::Repo;

use crate::{
    gerr, git_key, lookup_git, lookup_leep, parse_ident, parse_tz, EXTRA_GIT_AUTHOR_TZ,
    EXTRA_GIT_COMMITTER, EXTRA_GIT_COMMITTER_TZ, EXTRA_GIT_ENCODING, EXTRA_GIT_HEADERS,
    GIT_TO_LEEP, LEEP_TO_GIT,
};

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

fn sig(ident: &str, seconds: i64, tz: &str) -> gix::actor::Signature {
    let (name, email) = parse_ident(ident);
    gix::actor::Signature {
        name: name.into(),
        email: email.into(),
        time: gix::date::Time::new(seconds, parse_tz(tz)),
    }
}

/// `agent.session_id` -> `Leep-Agent-Session-Id`.
fn trailer_key(key: &str) -> String {
    let rest = key.strip_prefix("agent.").unwrap_or(key);
    let words: Vec<String> = rest
        .split(['.', '_', '-', ' '])
        .filter(|w| !w.is_empty())
        .map(|w| {
            let mut c = w.chars();
            c.next()
                .map(|f| f.to_uppercase().collect::<String>() + c.as_str())
                .unwrap_or_default()
        })
        .collect();
    format!("Leep-Agent-{}", words.join("-"))
}

fn with_trailers(message: &str, extra: &BTreeMap<String, String>) -> String {
    let lines: Vec<String> = extra
        .iter()
        .filter(|(k, _)| k.starts_with("agent."))
        .map(|(k, v)| format!("{}: {}", trailer_key(k), v.replace(['\n', '\r'], " ")))
        .collect();
    if lines.is_empty() {
        return message.to_string();
    }
    let mut m = message.trim_end_matches('\n').to_string();
    // Trailers must form their own final paragraph.
    m.push_str("\n\n");
    m.push_str(&lines.join("\n"));
    m.push('\n');
    m
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
        entries.sort_by(|a, b| {
            let key = |e: &Entry| {
                let mut k = e.filename.to_vec();
                if e.mode.is_tree() {
                    k.push(b'/');
                }
                k
            };
            key(a).cmp(&key(b))
        });
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
        let tz = |k: &str| cs.extra.get(k).map(String::as_str).unwrap_or("+0000");
        let author = sig(&cs.author, cs.timestamp, tz(EXTRA_GIT_AUTHOR_TZ));
        let committer = match cs.extra.get(EXTRA_GIT_COMMITTER) {
            Some(c) => match c.rsplit_once(' ') {
                Some((ident, t)) => sig(
                    ident,
                    t.parse().unwrap_or(cs.timestamp),
                    tz(EXTRA_GIT_COMMITTER_TZ),
                ),
                None => sig(c, cs.timestamp, tz(EXTRA_GIT_COMMITTER_TZ)),
            },
            None => author.clone(),
        };
        let extra_headers: Vec<(gix::bstr::BString, gix::bstr::BString)> =
            match cs.extra.get(EXTRA_GIT_HEADERS) {
                Some(json) => serde_json::from_str::<Vec<(String, String)>>(json)
                    .map_err(gerr)?
                    .into_iter()
                    .map(|(k, v)| (k.into(), v.into()))
                    .collect(),
                None => Vec::new(),
            };
        let message = if opts.provenance_trailers {
            with_trailers(&cs.message, &cs.extra)
        } else {
            cs.message.clone()
        };
        let oid = g
            .write_object(gix::objs::Commit {
                tree,
                parents: parents.into(),
                author,
                committer,
                encoding: cs.extra.get(EXTRA_GIT_ENCODING).map(|e| e.as_str().into()),
                message: message.into(),
                extra_headers,
            })
            .map_err(gerr)?
            .detach();
        done.insert(id, oid);
        report.commits_written += 1;

        if lookup_git(&**meta, &id).await?.is_none() {
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
