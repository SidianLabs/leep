use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;

use gix::objs::tree::{Entry, EntryKind};
use gix::ObjectId;
use leep_core::{ChangesetId, Error, FileKind, Hash, Result};
use leep_derive::Repo;

use crate::{
    gerr, git_key, lookup_git, lookup_leep, parse_ident, parse_tz, EXTRA_GIT_AUTHOR_TZ,
    EXTRA_GIT_COMMITTER, EXTRA_GIT_COMMITTER_TZ, EXTRA_GIT_ENCODING, EXTRA_GIT_HEADERS,
    EXTRA_GIT_RAW, GIT_TO_LEEP, LEEP_TO_GIT, NEGATIVE_ZERO_TZ,
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

fn sig(ident: &[u8], seconds: i64, tz: &str) -> gix::actor::Signature {
    let (name, email) = parse_ident(ident);
    gix::actor::Signature {
        name: name.into(),
        email: email.into(),
        time: gix::date::Time::new(seconds, parse_tz(tz)),
    }
}

/// The original bytes of commit fields that were not UTF-8 (`git.raw`).
/// A field counts only while its lossy text still equals what the changeset
/// holds, so an edited message or author is never overwritten by stale bytes.
type RawHeaders = Vec<(Vec<u8>, Vec<u8>)>;

struct RawFields(BTreeMap<String, serde_json::Value>);

impl RawFields {
    fn of(cs: &leep_core::Changeset) -> Result<Self> {
        match cs.extra.get(EXTRA_GIT_RAW) {
            Some(json) => Ok(Self(serde_json::from_str(json).map_err(gerr)?)),
            None => Ok(Self(BTreeMap::new())),
        }
    }

    fn decode(v: &serde_json::Value) -> Result<Vec<u8>> {
        use base64::Engine;
        let s = v.as_str().ok_or_else(|| gerr("git.raw: not a string"))?;
        base64::engine::general_purpose::STANDARD
            .decode(s)
            .map_err(gerr)
    }

    /// The bytes of `key` if they still read as `text`.
    fn bytes(&self, key: &str, text: &str) -> Result<Option<Vec<u8>>> {
        let Some(v) = self.0.get(key) else {
            return Ok(None);
        };
        let b = Self::decode(v)?;
        Ok((String::from_utf8_lossy(&b) == text).then_some(b))
    }

    fn headers(&self, text: &[(String, String)]) -> Result<Option<RawHeaders>> {
        let Some(v) = self.0.get("headers") else {
            return Ok(None);
        };
        let pairs: Vec<[serde_json::Value; 2]> = serde_json::from_value(v.clone()).map_err(gerr)?;
        let mut out = Vec::new();
        for [k, v] in &pairs {
            out.push((Self::decode(k)?, Self::decode(v)?));
        }
        let same = out.len() == text.len()
            && out.iter().zip(text).all(|((rk, rv), (k, v))| {
                String::from_utf8_lossy(rk) == *k && String::from_utf8_lossy(rv) == *v
            });
        Ok(same.then_some(out))
    }
}

/// Rewrite the time zone of the `author` or `committer` line of a serialized
/// commit to `-0000`, which gix cannot express (it writes `+0000` for offset
/// zero).
fn set_negative_zero(commit: &mut [u8], prefix: &[u8]) {
    let mut at = 0;
    while at < commit.len() {
        let end = commit[at..]
            .iter()
            .position(|&b| b == b'\n')
            .map_or(commit.len(), |i| at + i);
        let line = &commit[at..end];
        if line.is_empty() {
            return;
        }
        if line.starts_with(prefix) && line.len() >= NEGATIVE_ZERO_TZ.len() {
            commit[end - NEGATIVE_ZERO_TZ.len()..end].copy_from_slice(NEGATIVE_ZERO_TZ.as_bytes());
            return;
        }
        at = end + 1;
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

/// Git's tree order: by name, with a directory compared as if its name
/// ended in `/`.
pub(crate) fn sort_entries(entries: &mut [Entry]) {
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

/// The git commit for `cs`, given its tree and parent commits. Changesets
/// imported from git carry what it takes to reproduce the original bytes
/// (committer, time zones, encoding, extra headers). `changeset_trailer`
/// names the changeset in a `Leep-Changeset:` trailer, which the live mirror
/// adds to changesets that did not come from git (ADR 0046).
pub(crate) fn commit_object(
    cs: &leep_core::Changeset,
    tree: ObjectId,
    parents: Vec<ObjectId>,
    provenance_trailers: bool,
    changeset_trailer: Option<ChangesetId>,
) -> Result<Vec<u8>> {
    let raw = RawFields::of(cs)?;
    let tz = |k: &str| cs.extra.get(k).map(String::as_str).unwrap_or("+0000");
    let (author_tz, committer_tz) = (tz(EXTRA_GIT_AUTHOR_TZ), tz(EXTRA_GIT_COMMITTER_TZ));
    let author_ident = raw
        .bytes("author", &cs.author)?
        .unwrap_or_else(|| cs.author.clone().into_bytes());
    let author = sig(&author_ident, cs.timestamp, author_tz);
    let committer = match cs.extra.get(EXTRA_GIT_COMMITTER) {
        Some(c) => {
            let (ident, secs) = match c.rsplit_once(' ') {
                Some((ident, t)) => (ident, t.parse().unwrap_or(cs.timestamp)),
                None => (c.as_str(), cs.timestamp),
            };
            let ident = raw
                .bytes("committer", ident)?
                .unwrap_or_else(|| ident.as_bytes().to_vec());
            sig(&ident, secs, committer_tz)
        }
        None => author.clone(),
    };
    let text_headers: Vec<(String, String)> = match cs.extra.get(EXTRA_GIT_HEADERS) {
        Some(json) => serde_json::from_str(json).map_err(gerr)?,
        None => Vec::new(),
    };
    let extra_headers: Vec<(gix::bstr::BString, gix::bstr::BString)> =
        match raw.headers(&text_headers)? {
            Some(pairs) => pairs
                .into_iter()
                .map(|(k, v)| (k.into(), v.into()))
                .collect(),
            None => text_headers
                .into_iter()
                .map(|(k, v)| (k.into(), v.into()))
                .collect(),
        };
    let mut message = if provenance_trailers {
        with_trailers(&cs.message, &cs.extra)
    } else {
        cs.message.clone()
    };
    if let Some(id) = changeset_trailer {
        message = with_changeset_trailer(&message, id);
    }
    // Trailers rewrite the message, so the original bytes apply only when
    // none were added.
    let message = match message == cs.message {
        true => raw.bytes("message", &cs.message)?,
        false => None,
    }
    .unwrap_or_else(|| message.into_bytes());
    let encoding = match cs.extra.get(EXTRA_GIT_ENCODING) {
        Some(e) => Some(
            raw.bytes("encoding", e)?
                .unwrap_or_else(|| e.clone().into_bytes()),
        ),
        None => None,
    };
    let commit = gix::objs::Commit {
        tree,
        parents: parents.into(),
        author,
        committer,
        encoding: encoding.map(Into::into),
        message: message.into(),
        extra_headers,
    };
    let mut buf = Vec::new();
    gix::objs::WriteTo::write_to(&commit, &mut buf).map_err(gerr)?;
    if author_tz == NEGATIVE_ZERO_TZ {
        set_negative_zero(&mut buf, b"author ");
    }
    if committer_tz == NEGATIVE_ZERO_TZ {
        set_negative_zero(&mut buf, b"committer ");
    }
    Ok(buf)
}

/// Trailer key the mirror uses to name the changeset a commit came from.
pub const CHANGESET_TRAILER: &str = "Leep-Changeset";

/// Append `Leep-Changeset: <id>` to the final trailer paragraph, or start one.
fn with_changeset_trailer(message: &str, id: ChangesetId) -> String {
    let body = message.trim_end_matches('\n');
    let last = body.rsplit("\n\n").next().unwrap_or("");
    let in_trailers = !body.is_empty()
        && body.contains("\n\n")
        && last.lines().all(|l| {
            l.split_once(": ")
                .is_some_and(|(k, _)| !k.is_empty() && !k.contains(' '))
        });
    let sep = if body.is_empty() {
        ""
    } else if in_trailers {
        "\n"
    } else {
        "\n\n"
    };
    format!("{body}{sep}{CHANGESET_TRAILER}: {id}\n")
}

/// The changeset a commit message names in its `Leep-Changeset:` trailer.
pub(crate) fn changeset_trailer(message: &str) -> Option<ChangesetId> {
    let body = message.trim_end_matches('\n');
    let last = body.rsplit("\n\n").next()?;
    last.lines().rev().find_map(|l| {
        let v = l.strip_prefix(CHANGESET_TRAILER)?.strip_prefix(": ")?;
        v.trim().parse().ok()
    })
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

#[cfg(test)]
mod tests {
    use super::*;

    fn changeset(message: &str, raw_message: &[u8]) -> leep_core::Changeset {
        use base64::Engine;
        let raw = serde_json::json!({
            "message": base64::engine::general_purpose::STANDARD.encode(raw_message),
        });
        leep_core::Changeset {
            parents: vec![],
            author: "Ada <ada@example.com>".into(),
            timestamp: 1_700_000_000,
            message: message.into(),
            changes: Default::default(),
            extra: BTreeMap::from([(EXTRA_GIT_RAW.to_string(), raw.to_string())]),
        }
    }

    fn body(commit: &[u8]) -> &[u8] {
        let at = commit.windows(2).position(|w| w == b"\n\n").unwrap();
        &commit[at + 2..]
    }

    #[test]
    fn raw_message_applies_only_while_the_text_matches() {
        let tree = ObjectId::empty_tree(gix::hash::Kind::Sha1);
        let same = changeset("caf\u{fffd}\n", b"caf\xe9\n");
        let c = commit_object(&same, tree, vec![], false, None).unwrap();
        assert_eq!(body(&c), b"caf\xe9\n");
        // Someone reworded the changeset: the old bytes must not come back.
        let edited = changeset("coffee\n", b"caf\xe9\n");
        let c = commit_object(&edited, tree, vec![], false, None).unwrap();
        assert_eq!(body(&c), b"coffee\n");
    }
}
