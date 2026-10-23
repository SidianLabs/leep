//! `leep restore`: discard uncommitted changes to chosen paths, or bring in
//! their content from another revision.
//!
//! Every file is replaced through a temp file and rename, so a path is either
//! untouched or fully restored. Only the paths printed are changed.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;

use clap::Args;
use leep_core::api::FileInfo;
use leep_core::outln;

use super::{print_json, Ctx};
use crate::error::{CliError, Result};
use crate::fmt::plural;
use crate::materialize::{remove_file, write_file};
use crate::state::normalize_prefix;
use crate::status::{self, mtime_ns, Change, StatusEntry};

#[derive(Args, Debug)]
pub struct RestoreArgs {
    /// Files or directories to restore (relative to the current directory).
    #[arg(required = true)]
    pub paths: Vec<String>,
    /// Take the content from this revision instead of the base; the result
    /// stays an uncommitted change.
    #[arg(short, long)]
    pub source: Option<String>,
    /// Also delete new files (those not in the base) and overwrite them when
    /// `--source` has the path. Without it they are left alone.
    #[arg(long)]
    pub force: bool,
}

/// What a restore did, by path.
#[derive(Debug, Default, serde::Serialize)]
pub struct Discarded {
    pub restored: Vec<String>,
    pub removed: Vec<String>,
    /// New files left alone because `--force` was not given.
    pub kept: Vec<String>,
}

/// True if `path` is `spec` or below it; the empty spec is the whole tree.
pub fn within(path: &str, spec: &str) -> bool {
    spec.is_empty()
        || path == spec
        || path
            .strip_prefix(spec)
            .is_some_and(|rest| rest.starts_with('/'))
}

/// A user-supplied path as a repository path: relative to the current
/// directory when that is inside the working copy, else to its root.
pub fn repo_spec(ctx: &Ctx, raw: &str) -> Result<String> {
    let root = fs::canonicalize(&ctx.wc.root).unwrap_or_else(|_| ctx.wc.root.clone());
    let cwd_rel = std::env::current_dir()
        .ok()
        .and_then(|c| fs::canonicalize(c).ok())
        .and_then(|c| {
            c.strip_prefix(&root)
                .ok()
                .map(|p| p.to_string_lossy().into_owned())
        })
        .unwrap_or_default();
    let joined = match std::path::Path::new(raw).strip_prefix(&root) {
        Ok(rel) => rel.to_string_lossy().into_owned(),
        Err(_) if raw.starts_with('/') => {
            return Err(CliError::user(format!("{raw} is outside the working copy")))
        }
        Err(_) if cwd_rel.is_empty() => raw.to_string(),
        Err(_) => format!("{cwd_rel}/{raw}"),
    };
    let parts: Vec<&str> = joined
        .split('/')
        .filter(|c| !c.is_empty() && *c != ".")
        .collect();
    normalize_prefix(&parts.join("/"))
}

fn check_scope(ctx: &Ctx, spec: &str) -> Result<()> {
    if spec.is_empty() || ctx.wc.in_scope(spec) || ctx.wc.prefixes().iter().any(|p| within(p, spec))
    {
        Ok(())
    } else {
        Err(CliError::user(format!(
            "{spec} is outside this sparse checkout"
        )))
    }
}

pub fn run(ctx: &mut Ctx, args: &RestoreArgs) -> Result<i32> {
    let specs: Vec<String> = args
        .paths
        .iter()
        .map(|p| repo_spec(ctx, p))
        .collect::<Result<_>>()?;
    for s in &specs {
        check_scope(ctx, s)?;
    }
    let st = status::status(&ctx.wc)?;
    let done = match &args.source {
        None => {
            let picked: Vec<StatusEntry> = st
                .entries
                .iter()
                .filter(|e| specs.iter().any(|s| within(&e.path, s)))
                .cloned()
                .collect();
            if picked.is_empty() {
                for (raw, s) in args.paths.iter().zip(&specs) {
                    let known =
                        ctx.wc.state.files.keys().any(|p| within(p, s)) || ctx.wc.abs(s).exists();
                    if !known {
                        return Err(CliError::not_found(format!(
                            "{raw} does not match any file"
                        )));
                    }
                }
            }
            discard(ctx, &picked, args.force)?
        }
        Some(rev) => from_source(ctx, rev, &specs, &st.entries, args.force)?,
    };
    report(ctx, &done, args.source.as_deref())?;
    Ok(0)
}

/// Put the base content back for each entry. New files (not in the base) are
/// deleted only if `remove_new`. State is saved even when a write fails, so
/// finished work is not mistaken for local edits.
pub fn discard(ctx: &mut Ctx, entries: &[StatusEntry], remove_new: bool) -> Result<Discarded> {
    let mut done = Discarded::default();
    // Fetch first: a network failure must not leave a half-restored tree.
    for e in entries {
        if let Some(f) = ctx.wc.state.files.get(&e.path) {
            ctx.cache.fetch(&ctx.client, &f.content)?;
        }
    }
    let result = (|| -> Result<()> {
        for e in entries {
            let abs = ctx.wc.abs_for_write(&e.path)?;
            match ctx.wc.state.files.get(&e.path).cloned() {
                Some(f) => {
                    let data = ctx.cache.get(&f.content).ok_or_else(|| {
                        CliError::server(format!("blob {} vanished from the cache", f.content))
                    })?;
                    write_file(&abs, &data, f.kind)?;
                    let mtime = mtime_ns(&fs::symlink_metadata(&abs)?);
                    if let Some(s) = ctx.wc.state.files.get_mut(&e.path) {
                        s.mtime_ns = mtime;
                    }
                    ctx.wc.state.conflicts.retain(|c| c != &e.path);
                    ctx.wc.state.unresolved.retain(|c| c != &e.path);
                    done.restored.push(e.path.clone());
                }
                None if remove_new => {
                    remove_file(&ctx.wc.root, &abs)?;
                    ctx.wc.state.conflicts.retain(|c| c != &e.path);
                    ctx.wc.state.unresolved.retain(|c| c != &e.path);
                    done.removed.push(e.path.clone());
                }
                None => done.kept.push(e.path.clone()),
            }
        }
        Ok(())
    })();
    let saved = ctx.wc.save_state();
    result?;
    saved?;
    Ok(done)
}

fn from_source(
    ctx: &mut Ctx,
    rev: &str,
    specs: &[String],
    entries: &[StatusEntry],
    force: bool,
) -> Result<Discarded> {
    let id = ctx.remote().resolve(rev)?;
    let mut target: BTreeMap<String, FileInfo> = BTreeMap::new();
    for s in specs {
        let roots = if s.is_empty() {
            ctx.wc.prefixes()
        } else {
            vec![s.clone()]
        };
        for r in roots {
            for f in ctx.client.files(&id, &r)?.files {
                if ctx.wc.in_scope(f.path.as_str()) {
                    target.insert(f.path.to_string(), f);
                }
            }
        }
    }
    let entry_of: BTreeMap<&str, &StatusEntry> =
        entries.iter().map(|e| (e.path.as_str(), e)).collect();
    let candidates: BTreeSet<String> = target
        .keys()
        .cloned()
        .chain(
            ctx.wc
                .state
                .files
                .keys()
                .filter(|p| specs.iter().any(|s| within(p, s)))
                .cloned(),
        )
        .collect();

    let mut plan: Vec<(String, Option<FileInfo>)> = Vec::new();
    let mut blocked = Vec::new();
    for p in candidates {
        let want = target.get(&p).cloned();
        let tracked = ctx.wc.state.files.get(&p);
        let entry = entry_of.get(p.as_str());
        let now = match entry {
            Some(e) if e.change == Change::Deleted => None,
            Some(e) => e.now.as_ref().or(tracked),
            None => tracked,
        };
        let same = match (&want, now) {
            (Some(w), Some(n)) => w.content == n.content && w.kind == n.kind,
            (None, None) => true,
            _ => false,
        };
        if same {
            continue;
        }
        // A new file that `--source` would replace is the user's only copy.
        if tracked.is_none() && now.is_some() {
            if want.is_none() {
                continue;
            }
            if !force {
                blocked.push(p.clone());
                continue;
            }
        }
        plan.push((p, want));
    }
    if !blocked.is_empty() {
        return Err(CliError::user(format!(
            "would overwrite new file{} ({}); nothing changed. Use --force to replace {}",
            if blocked.len() == 1 { "" } else { "s" },
            blocked.join(", "),
            if blocked.len() == 1 { "it" } else { "them" }
        )));
    }
    for (_, w) in &plan {
        if let Some(w) = w {
            ctx.cache.fetch(&ctx.client, &w.content)?;
        }
    }

    let mut done = Discarded::default();
    let result = (|| -> Result<()> {
        for (p, want) in &plan {
            let abs = ctx.wc.abs_for_write(p)?;
            match want {
                Some(w) => {
                    let data = ctx.cache.get(&w.content).ok_or_else(|| {
                        CliError::server(format!("blob {} vanished from the cache", w.content))
                    })?;
                    write_file(&abs, &data, w.kind)?;
                    done.restored.push(p.clone());
                }
                None => {
                    remove_file(&ctx.wc.root, &abs)?;
                    done.removed.push(p.clone());
                }
            }
            ctx.wc.state.conflicts.retain(|c| c != p);
            ctx.wc.state.unresolved.retain(|c| c != p);
        }
        Ok(())
    })();
    let saved = ctx.wc.save_state();
    result?;
    saved?;
    Ok(done)
}

fn report(ctx: &Ctx, d: &Discarded, source: Option<&str>) -> Result<()> {
    if ctx.json {
        return print_json(d);
    }
    for p in &d.restored {
        outln!("restored {p}");
    }
    for p in &d.removed {
        outln!("removed {p}");
    }
    for p in &d.kept {
        outln!("kept new file {p} (use --force to delete it)");
    }
    let from = source.map(|s| format!(" from {s}")).unwrap_or_default();
    if d.restored.is_empty() && d.removed.is_empty() {
        outln!("Nothing to restore{from}");
    } else {
        outln!(
            "Restored {}{from}, removed {}",
            plural(d.restored.len(), "file"),
            plural(d.removed.len(), "file")
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn within_matches_whole_components() {
        assert!(within("a/b.txt", ""));
        assert!(within("a/b.txt", "a"));
        assert!(within("a/b.txt", "a/b.txt"));
        assert!(!within("ab/c.txt", "a"));
        assert!(!within("a", "a/b"));
    }
}
