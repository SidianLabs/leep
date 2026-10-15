//! Put a changeset's tree on disk as *uncommitted* changes on top of a new
//! base, without losing what is in the working copy.
//!
//! `sync` and `stash apply` snapshot the local changes on the server, ask it
//! to rebase the snapshot onto the new base, and then call [`apply`] with the
//! result. [`apply`] touches the disk only after everything it needs has been
//! fetched, writes each file through a temp file and rename, and saves the
//! state last. If any step fails, files already written are put back.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fs;

use leep_core::api::{FileInfo, RebaseRequest, RebaseResponse};
use leep_core::{ChangesetId, FileChange, FileKind, Hash};

use crate::commands::Ctx;
use crate::error::{CliError, Result};
use crate::materialize::{check_not_in_the_way, list_files, remove_file, write_file, Stats};
use crate::state::FileState;
use crate::status::{self, mtime_ns, read_content, scan_file, Change};

/// What a path should contain once the overlay is applied.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Want {
    File {
        content: Hash,
        kind: FileKind,
        size: u64,
    },
    /// Conflict markers written by the CLI from the server's merge result.
    Text { text: String, kind: FileKind },
}

impl Want {
    fn from_info(i: &FileInfo) -> Want {
        Want::File {
            content: i.content,
            kind: i.kind,
            size: i.size,
        }
    }

    fn hash(&self) -> Hash {
        match self {
            Want::File { content, .. } => *content,
            Want::Text { text, .. } => Hash::of_blob(text.as_bytes()),
        }
    }

    fn kind(&self) -> FileKind {
        match self {
            Want::File { kind, .. } | Want::Text { kind, .. } => *kind,
        }
    }
}

#[derive(Debug)]
pub struct Plan {
    pub new_base: ChangesetId,
    /// Files of `new_base` under the sparse prefixes; becomes the state.
    pub base_files: BTreeMap<String, FileInfo>,
    /// Desired disk content; a path not listed must not exist.
    pub want: BTreeMap<String, Want>,
    /// Paths that got conflict markers.
    pub conflicts: Vec<String>,
    /// Conflicting paths with no text merge: the local version was kept.
    pub unmerged: Vec<String>,
}

/// Ask the server to rebase `change` (a snapshot) onto `onto` and turn the
/// answer into a [`Plan`]. Nothing local changes. Conflicts are never
/// resolved here: they become marker files, as in `land`.
pub fn plan(ctx: &Ctx, change: ChangesetId, onto: ChangesetId) -> Result<Plan> {
    let base_files = list_files(ctx, &onto)?;
    let resp = ctx.client.rebase(&RebaseRequest {
        changeset: change,
        onto,
    })?;
    let mut conflicts = Vec::new();
    let mut unmerged = Vec::new();
    let want = match resp {
        RebaseResponse::Rebased { id } => list_files(ctx, &id)?
            .into_iter()
            .map(|(p, f)| (p, Want::from_info(&f)))
            .collect(),
        RebaseResponse::Conflicts {
            conflicts: found,
            clean,
        } => {
            let outside: Vec<String> = clean
                .keys()
                .chain(found.iter().map(|c| &c.path))
                .map(|p| p.to_string())
                .filter(|p| !ctx.wc.in_scope(p))
                .collect();
            if !outside.is_empty() {
                return Err(CliError::user(format!(
                    "cannot apply here: the changes touch paths outside this sparse \
                     checkout ({}); widen it with `leep checkout @ --prefix ...`",
                    outside.join(", ")
                )));
            }
            let mut want: BTreeMap<String, Want> = base_files
                .iter()
                .map(|(p, f)| (p.clone(), Want::from_info(f)))
                .collect();
            for (path, change) in clean {
                match change {
                    FileChange::Modify {
                        content,
                        kind,
                        size,
                    } => want.insert(
                        path.to_string(),
                        Want::File {
                            content,
                            kind,
                            size,
                        },
                    ),
                    FileChange::Delete => want.remove(path.as_str()),
                };
            }
            for c in found {
                let path = c.path.to_string();
                match (c.merged_with_markers, &c.theirs) {
                    (Some(text), theirs) => {
                        let kind = theirs
                            .as_ref()
                            .or(c.ours.as_ref())
                            .map_or(FileKind::Regular, |i| i.kind);
                        want.insert(path.clone(), Want::Text { text, kind });
                        conflicts.push(path);
                    }
                    (None, Some(t)) => {
                        want.insert(path.clone(), Want::from_info(t));
                        unmerged.push(path);
                    }
                    (None, None) => {
                        want.remove(&path);
                        unmerged.push(path);
                    }
                }
            }
            want
        }
    };
    Ok(Plan {
        new_base: onto,
        base_files,
        want,
        conflicts,
        unmerged,
    })
}

/// What a file undone by the journal looked like before it was changed.
struct Undo {
    path: String,
    prev: Option<(Vec<u8>, FileKind)>,
}

/// Make the disk match `plan.want`, and record `plan.new_base` and its files
/// as the state, so the difference shows up as local changes.
pub fn apply(ctx: &mut Ctx, plan: &Plan) -> Result<Stats> {
    let st = status::status(&ctx.wc)?;
    // What is on disk now: the recorded files, except where status saw an edit.
    let mut disk: BTreeMap<String, Option<(Hash, FileKind)>> = ctx
        .wc
        .state
        .files
        .iter()
        .map(|(p, f)| (p.clone(), Some((f.content, f.kind))))
        .collect();
    let mut dirty: HashSet<String> = HashSet::new();
    for e in &st.entries {
        dirty.insert(e.path.clone());
        match (e.change, &e.now) {
            (Change::Deleted, _) => disk.insert(e.path.clone(), None),
            (_, Some(n)) => disk.insert(e.path.clone(), Some((n.content, n.kind))),
            (_, None) => None,
        };
    }

    let mut todo: BTreeSet<&String> = BTreeSet::new();
    for p in plan.want.keys().chain(disk.keys()) {
        let now = disk.get(p).copied().flatten();
        let same = match (plan.want.get(p), now) {
            (None, None) => true,
            (Some(w), Some((h, k))) => w.hash() == h && w.kind() == k,
            _ => false,
        };
        if !same {
            todo.insert(p);
        }
    }

    // Everything that can fail without touching the disk happens first.
    for p in &todo {
        let abs = ctx.wc.abs(p);
        match plan.want.get(*p) {
            Some(Want::File { content, .. }) => {
                ctx.cache.fetch(&ctx.client, content)?;
                if disk.get(*p).copied().flatten().is_none() {
                    check_not_in_the_way(&abs, p, content)?;
                }
            }
            Some(w @ Want::Text { .. }) if disk.get(*p).copied().flatten().is_none() => {
                check_not_in_the_way(&abs, p, &w.hash())?;
            }
            Some(Want::Text { .. }) => {}
            None => {}
        }
    }

    let mut journal: Vec<Undo> = Vec::new();
    let mut stats = Stats::default();
    let mut written: HashSet<&str> = HashSet::new();
    let outcome = (|| -> Result<()> {
        for p in &todo {
            let abs = ctx.wc.abs(p);
            let prev = match fs::symlink_metadata(&abs) {
                Ok(md) => {
                    let kind = scan_file(&abs, &md, None)?.kind;
                    Some((read_content(&abs, &md)?, kind))
                }
                Err(_) => None,
            };
            journal.push(Undo {
                path: p.to_string(),
                prev,
            });
            match plan.want.get(*p) {
                Some(Want::File { content, kind, .. }) => {
                    let data = ctx.cache.get(content).ok_or_else(|| {
                        CliError::server(format!("blob {content} vanished from the cache"))
                    })?;
                    write_file(&abs, &data, *kind)?;
                    stats.written += 1;
                    written.insert(p.as_str());
                }
                Some(Want::Text { text, kind }) => {
                    write_file(&abs, text.as_bytes(), *kind)?;
                    stats.written += 1;
                    written.insert(p.as_str());
                }
                None => {
                    if remove_file(&ctx.wc.root, &abs)? {
                        stats.removed += 1;
                    }
                }
            }
        }
        Ok(())
    })();
    let before = ctx.wc.state.clone();
    let outcome = outcome.and_then(|()| {
        let mut files = BTreeMap::new();
        for (p, nb) in &plan.base_files {
            let matches_base = matches!(plan.want.get(p), Some(Want::File { content, kind, .. })
                if *content == nb.content && *kind == nb.kind);
            let reuse = before
                .files
                .get(p)
                .filter(|o| {
                    o.content == nb.content
                        && o.kind == nb.kind
                        && !dirty.contains(p)
                        && !written.contains(p.as_str())
                })
                .cloned();
            let state = match (matches_base, reuse) {
                (true, Some(o)) => o,
                (true, None) => FileState {
                    content: nb.content,
                    kind: nb.kind,
                    size: nb.size,
                    mtime_ns: mtime_ns(&fs::symlink_metadata(ctx.wc.abs(p))?),
                },
                // Differs from the base on disk: an impossible mtime makes
                // status rehash it instead of trusting size and time.
                (false, _) => FileState {
                    content: nb.content,
                    kind: nb.kind,
                    size: nb.size,
                    mtime_ns: 0,
                },
            };
            files.insert(p.clone(), state);
        }
        ctx.wc.state.files = files;
        ctx.wc.state.base = Some(plan.new_base);
        ctx.wc.state.conflicts = plan.conflicts.clone();
        ctx.wc.state.unresolved = plan.unmerged.clone();
        ctx.wc.save_state()
    });
    if let Err(e) = outcome {
        ctx.wc.state = before;
        let failed = rollback(ctx, journal);
        return Err(if failed.is_empty() {
            e
        } else {
            CliError::user(format!(
                "{e}; could not put back: {} (their content is in the snapshot)",
                failed.join(", ")
            ))
        });
    }
    Ok(stats)
}

/// Undo the journal newest first; returns the paths that could not be put back.
fn rollback(ctx: &Ctx, journal: Vec<Undo>) -> Vec<String> {
    let mut failed = Vec::new();
    for u in journal.into_iter().rev() {
        let abs = ctx.wc.abs(&u.path);
        let ok = match &u.prev {
            Some((data, kind)) => write_file(&abs, data, *kind).is_ok(),
            None => remove_file(&ctx.wc.root, &abs).is_ok(),
        };
        if !ok {
            failed.push(u.path);
        }
    }
    failed
}

/// Whether `a` is in the history of `b` (inclusive), by the merge base.
pub fn contained_in(ctx: &Ctx, a: ChangesetId, b: ChangesetId) -> Result<bool> {
    Ok(ctx.client.merge_base(&a, &b)?.base == Some(a))
}

/// Move the unlanded commits ending at `base` onto `head`, oldest first, with
/// the server's single-changeset rebase. Returns the new tip and how many
/// commits moved, or `None` when `base` is already in `head`'s history.
///
/// A conflict inside the stack is an error rather than markers: `land` is
/// the command that squashes a stack and reports conflicts for it.
pub fn rebase_stack(
    ctx: &Ctx,
    base: ChangesetId,
    head: ChangesetId,
) -> Result<Option<(ChangesetId, usize)>> {
    let fork = ctx.client.merge_base(&base, &head)?.base;
    if fork == Some(base) {
        return Ok(None);
    }
    let Some(fork) = fork else {
        return Err(CliError::user(
            "the base shares no history with the target ref; \
             use `leep checkout` to move to it",
        ));
    };
    let mut stack = Vec::new();
    let mut cur = base;
    while cur != fork {
        let info = ctx.client.get_changeset(&cur)?.changeset;
        if info.parents.len() > 1 {
            return Err(CliError::user(
                "local commits include a merge, which cannot be rebased; \
                 `leep land` or `leep checkout` instead",
            ));
        }
        stack.push(cur);
        cur = *info.parents.first().ok_or_else(|| {
            CliError::user("local commits do not descend from the target ref's history")
        })?;
    }
    let mut onto = head;
    for c in stack.iter().rev() {
        match ctx.client.rebase(&RebaseRequest {
            changeset: *c,
            onto,
        })? {
            RebaseResponse::Rebased { id } => onto = id,
            RebaseResponse::Conflicts { conflicts, .. } => {
                return Err(CliError::user(format!(
                    "local commit {} conflicts with the target ref in {} file{}; \
                     nothing changed. Run `leep land` to bring it up to date and \
                     resolve the conflicts",
                    crate::fmt::short(c),
                    conflicts.len(),
                    if conflicts.len() == 1 { "" } else { "s" }
                )));
            }
        }
    }
    Ok(Some((onto, stack.len())))
}
