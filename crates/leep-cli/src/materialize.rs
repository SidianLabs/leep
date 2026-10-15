//! Bring the working directory to a changeset: list files under the sparse
//! prefixes, fetch missing blobs through the cache, write, and prune.

use std::collections::{BTreeMap, HashSet};
use std::fs;
use std::path::Path;

use leep_core::api::FileInfo;
use leep_core::{ChangesetId, FileKind, Hash};

use crate::commands::Ctx;
use crate::error::{CliError, Result};
use crate::state::FileState;
use crate::status::{self, scan_file};

#[derive(Debug, Default, PartialEq, Eq)]
pub struct Stats {
    pub written: usize,
    pub removed: usize,
}

/// Files of `id` under the configured prefixes, keyed by path.
pub fn list_files(ctx: &Ctx, id: &ChangesetId) -> Result<BTreeMap<String, FileInfo>> {
    let mut out = BTreeMap::new();
    for p in ctx.wc.prefixes() {
        for f in ctx.client.files(id, &p)?.files {
            out.insert(f.path.to_string(), f);
        }
    }
    Ok(out)
}

/// Make the working copy match `target` and record it as the base.
///
/// Only paths whose recorded content differs are touched. Untracked files in
/// the way are an error unless `force`; with `force`, locally modified tracked
/// files are overwritten too. State is saved even on failure so partial
/// progress is not mistaken for local edits.
pub fn materialize(ctx: &mut Ctx, target: ChangesetId, force: bool) -> Result<Stats> {
    let result = run(ctx, target, force);
    let saved = ctx.wc.save_state();
    let stats = result?;
    saved?;
    Ok(stats)
}

fn run(ctx: &mut Ctx, target: ChangesetId, force: bool) -> Result<Stats> {
    let listing = list_files(ctx, &target)?;
    let dirty: HashSet<String> = if force {
        status::status(&ctx.wc)?
            .entries
            .into_iter()
            .map(|e| e.path)
            .collect()
    } else {
        HashSet::new()
    };
    let mut stats = Stats::default();

    let stale: Vec<String> = ctx
        .wc
        .state
        .files
        .keys()
        .filter(|p| !listing.contains_key(*p))
        .cloned()
        .collect();
    for path in stale {
        let abs = ctx.wc.abs(&path);
        match fs::remove_file(&abs) {
            Ok(()) => stats.removed += 1,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
        prune_empty_dirs(&ctx.wc.root, &abs);
        ctx.wc.state.files.remove(&path);
    }

    for (path, info) in &listing {
        let current = ctx.wc.state.files.get(path);
        let up_to_date = current.is_some_and(|c| c.content == info.content && c.kind == info.kind);
        if up_to_date && !dirty.contains(path) {
            continue;
        }
        let abs = ctx.wc.abs(path);
        let data = ctx.cache.fetch(&ctx.client, &info.content)?;
        if current.is_none() && !force {
            check_not_in_the_way(&abs, path, &info.content)?;
        }
        write_file(&abs, &data, info.kind)?;
        let md = fs::symlink_metadata(&abs)?;
        let fs_state = scan_file(&abs, &md, None)?;
        ctx.wc.state.files.insert(
            path.clone(),
            FileState {
                content: info.content,
                kind: info.kind,
                size: info.size,
                mtime_ns: fs_state.mtime_ns,
            },
        );
        stats.written += 1;
    }

    ctx.wc.state.conflicts.clear();
    ctx.wc.state.base = Some(target);
    Ok(stats)
}

fn check_not_in_the_way(abs: &Path, path: &str, content: &Hash) -> Result<()> {
    let Ok(md) = fs::symlink_metadata(abs) else {
        return Ok(());
    };
    if md.is_file() && Hash::of_blob(&fs::read(abs)?) == *content {
        return Ok(());
    }
    Err(CliError::user(format!(
        "untracked file {path} would be overwritten (use --force)"
    )))
}

fn prune_empty_dirs(root: &Path, file: &Path) {
    let mut dir = file.parent();
    while let Some(d) = dir {
        if d == root || fs::remove_dir(d).is_err() {
            break;
        }
        dir = d.parent();
    }
}

/// Write one file with the right type and mode.
pub fn write_file(abs: &Path, data: &[u8], kind: FileKind) -> Result<()> {
    if let Some(parent) = abs.parent() {
        fs::create_dir_all(parent)?;
    }
    if fs::symlink_metadata(abs).is_ok_and(|m| m.file_type().is_symlink()) {
        fs::remove_file(abs)?;
    }
    match kind {
        FileKind::Symlink => {
            let _ = fs::remove_file(abs);
            make_symlink(data, abs)?;
        }
        FileKind::Regular | FileKind::Executable => {
            fs::write(abs, data)?;
            set_exec(abs, kind == FileKind::Executable)?;
        }
    }
    Ok(())
}

#[cfg(unix)]
fn make_symlink(target: &[u8], abs: &Path) -> Result<()> {
    let t = String::from_utf8_lossy(target).into_owned();
    std::os::unix::fs::symlink(t, abs)?;
    Ok(())
}

#[cfg(not(unix))]
fn make_symlink(target: &[u8], abs: &Path) -> Result<()> {
    fs::write(abs, target)?;
    Ok(())
}

#[cfg(unix)]
fn set_exec(abs: &Path, exec: bool) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(
        abs,
        fs::Permissions::from_mode(if exec { 0o755 } else { 0o644 }),
    )?;
    Ok(())
}

#[cfg(not(unix))]
fn set_exec(_: &Path, _: bool) -> Result<()> {
    Ok(())
}
