//! Turning uncommitted changes into a changeset: shared by `commit`, `amend`,
//! `sync` and `stash`.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use leep_core::changeset::EXTRA_SNAPSHOT;
use leep_core::{Changeset, ChangesetId, FileChange, Hash, RepoPath};

use crate::commands::Ctx;
use crate::error::{CliError, Result};
use crate::status::{read_content, Change, Status};

/// Unix seconds now.
pub fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// `--author`, else `$LEEP_AUTHOR`, else the login name, else what the server
/// says for our token (its `author`, else its name). Containers and CI often
/// set no login name, and the token is still a real identity there.
pub fn author(ctx: &Ctx, flag: Option<&str>) -> String {
    local_author(flag)
        .or_else(|| {
            let w = ctx.client.whoami().ok()?;
            w.author.or(w.name)
        })
        .unwrap_or_else(|| "unknown".into())
}

/// The author named without asking the server. `USERNAME` is Windows' name
/// for `USER`, and `LOGNAME` is set by login(1) where `USER` may not be.
fn local_author(flag: Option<&str>) -> Option<String> {
    let env = |k: &str| {
        std::env::var(k)
            .ok()
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())
    };
    flag.map(str::to_string)
        .or_else(|| env("LEEP_AUTHOR"))
        .or_else(|| env("USER"))
        .or_else(|| env("LOGNAME"))
        .or_else(|| env("USERNAME"))
}

/// The changes `st` describes, relative to the base, with their blobs
/// uploaded. Conflicted entries are skipped: callers refuse those first.
pub fn upload_changes(ctx: &Ctx, st: &Status) -> Result<BTreeMap<RepoPath, FileChange>> {
    let mut changes = BTreeMap::new();
    // hash -> repo path, to re-read bytes for upload.
    let mut sources: BTreeMap<Hash, String> = BTreeMap::new();
    for e in &st.entries {
        let path = RepoPath::new(e.path.as_str())?;
        match (&e.now, e.change) {
            (Some(now), Change::Added | Change::Modified) => {
                sources.entry(now.content).or_insert_with(|| e.path.clone());
                changes.insert(
                    path,
                    FileChange::Modify {
                        content: now.content,
                        kind: now.kind,
                        size: now.size,
                    },
                );
            }
            (None, Change::Deleted) => {
                changes.insert(path, FileChange::Delete);
            }
            _ => {}
        }
    }

    let hashes: Vec<Hash> = sources.keys().copied().collect();
    let missing: BTreeSet<Hash> = if hashes.is_empty() {
        BTreeSet::new()
    } else {
        ctx.client.missing_blobs(hashes)?.into_iter().collect()
    };
    let work: Vec<(&Hash, &String)> = sources.iter().collect();
    for_each_parallel(&work, upload_jobs(), |&(hash, path)| {
        let abs = ctx.wc.abs(path);
        let data = read_content(&abs, &fs::symlink_metadata(&abs)?)?;
        if Hash::of_blob(&data) != *hash {
            return Err(CliError::user(format!(
                "{path} changed while committing; retry"
            )));
        }
        ctx.cache.put(hash, &data)?;
        if missing.contains(hash) {
            ctx.client.put_blob(hash, data)?;
        }
        Ok(())
    })?;
    Ok(changes)
}

/// Blob uploads in flight at once, unless `LEEP_UPLOAD_JOBS` says otherwise.
/// Each upload is one small request, so a sequential commit of many files
/// waits on round trips, not bandwidth.
const UPLOAD_JOBS: usize = 8;

fn upload_jobs() -> usize {
    std::env::var("LEEP_UPLOAD_JOBS")
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .filter(|n| *n > 0)
        .map_or(UPLOAD_JOBS, |n| n.min(256))
}

/// Run `f` on every item with at most `jobs` threads, each holding one
/// item's bytes at a time. After a failure no new item starts. Items are
/// taken in order, so every item before a failing one has run, and the
/// error returned is the earliest item's: the one a sequential loop would
/// have reported.
fn for_each_parallel<T: Sync>(
    items: &[T],
    jobs: usize,
    f: impl Fn(&T) -> Result<()> + Sync,
) -> Result<()> {
    let jobs = jobs.clamp(1, items.len().max(1));
    if jobs == 1 {
        return items.iter().try_for_each(f);
    }
    let (tx, rx) = mpsc::channel();
    for item in items.iter().enumerate() {
        tx.send(item).expect("receiver is alive");
    }
    drop(tx);
    let queue = Mutex::new(rx);
    let stop = AtomicBool::new(false);
    let first: Mutex<Option<(usize, CliError)>> = Mutex::new(None);
    std::thread::scope(|s| {
        for _ in 0..jobs {
            s.spawn(|| loop {
                if stop.load(Ordering::Relaxed) {
                    break;
                }
                let Ok((i, item)) = queue.lock().unwrap().recv() else {
                    break;
                };
                if let Err(e) = f(item) {
                    stop.store(true, Ordering::Relaxed);
                    let mut first = first.lock().unwrap();
                    if first.as_ref().is_none_or(|(j, _)| i < *j) {
                        *first = Some((i, e));
                    }
                }
            });
        }
    });
    match first.into_inner().unwrap() {
        Some((_, e)) => Err(e),
        None => Ok(()),
    }
}

/// Store the working copy's uncommitted changes as a changeset on top of the
/// base, marked `leep.snapshot = kind`. No ref points at it and the working
/// copy is not touched.
pub fn create(ctx: &Ctx, st: &Status, message: String, kind: &str) -> Result<ChangesetId> {
    let changes = upload_changes(ctx, st)?;
    let cs = Changeset {
        parents: ctx.wc.state.base.into_iter().collect(),
        author: author(ctx, None),
        timestamp: now_secs(),
        message,
        changes,
        extra: BTreeMap::from([(EXTRA_SNAPSHOT.to_string(), kind.to_string())]),
    };
    ctx.client.create_changeset(&cs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    #[test]
    fn every_item_runs_once_whatever_the_job_count() {
        for jobs in [0, 1, 3, 8, 500] {
            let seen: Vec<AtomicUsize> = (0..100).map(|_| AtomicUsize::new(0)).collect();
            let items: Vec<usize> = (0..100).collect();
            for_each_parallel(&items, jobs, |&i| {
                seen[i].fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
            .unwrap();
            assert!(seen.iter().all(|n| n.load(Ordering::SeqCst) == 1), "{jobs}");
        }
        for_each_parallel(&[] as &[u8], 8, |_| unreachable!()).unwrap();
    }

    #[test]
    fn the_earliest_failure_wins_and_later_items_do_not_start() {
        let items: Vec<usize> = (0..1000).collect();
        let ran = AtomicUsize::new(0);
        let e = for_each_parallel(&items, 8, |&i| {
            ran.fetch_add(1, Ordering::SeqCst);
            if i == 10 {
                // A later item fails first; the earlier one still wins.
                std::thread::sleep(std::time::Duration::from_millis(50));
            }
            if [10, 11, 500].contains(&i) {
                return Err(CliError::user(format!("item {i}")));
            }
            Ok(())
        })
        .unwrap_err();
        assert_eq!(e.to_string(), "item 10");
        assert!(ran.load(Ordering::SeqCst) < 1000, "stopped early");
    }
}
