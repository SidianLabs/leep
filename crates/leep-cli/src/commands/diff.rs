//! `leep diff`.

use std::collections::HashMap;
use std::fs;

use clap::Args;
use leep_core::api::{DiffResponse, FileInfo, RenameEntry};
use leep_core::rename::RenameOptions;
use leep_core::FileKind;

use super::{print_json, Ctx, Remote};
use crate::error::{CliError, Result};
use crate::fmt::{file_diff, rename_line, renamed_diff};
use crate::status::{read_content, status, working_renames, Change};
use crate::term;

#[derive(Args, Debug, Default)]
pub struct DiffArgs {
    /// Either nothing (working copy vs base) or `FROM TO` revisions.
    #[arg(num_args = 0..=2, value_name = "FROM TO")]
    pub revs: Vec<String>,
    /// Show a renamed file as a delete and an add.
    #[arg(long, conflicts_with = "find_renames")]
    pub no_renames: bool,
    /// Detect renames with at least N percent similarity (default 50).
    #[arg(
        short = 'M',
        long = "find-renames",
        value_name = "N",
        num_args = 0..=1,
        require_equals = true,
        default_missing_value = "50",
        value_parser = clap::value_parser!(u8).range(0..=100),
    )]
    pub find_renames: Option<u8>,
}

impl DiffArgs {
    /// `None` when renames are off, else the similarity threshold to ask for.
    pub fn threshold(&self) -> Option<u8> {
        (!self.no_renames).then(|| {
            self.find_renames
                .unwrap_or(leep_core::rename::DEFAULT_THRESHOLD)
        })
    }
}

pub fn run(ctx: &Ctx, args: &DiffArgs) -> Result<i32> {
    match args.revs.as_slice() {
        [] => working_copy(ctx, args.threshold()),
        [from, to] => revisions(&ctx.remote(), from, to, args.threshold()),
        _ => Err(CliError::user("diff takes no arguments or exactly FROM TO")),
    }
}

type Content = Option<(Vec<u8>, FileKind)>;

fn side(c: &Content) -> Option<(&[u8], FileKind)> {
    c.as_ref().map(|(d, k)| (d.as_slice(), *k))
}

fn working_copy(ctx: &Ctx, threshold: Option<u8>) -> Result<i32> {
    let (text, renames) = working_diff(ctx, threshold)?;
    if ctx.json {
        print_json(&serde_json::json!({ "diff": text, "renames": renames }))?;
    } else {
        term::page(&term::diff(&text), false)?;
    }
    Ok(0)
}

/// Diff text of the working copy against its base, and the renames it
/// folded in (`None` threshold: renames off).
pub fn working_diff(ctx: &Ctx, threshold: Option<u8>) -> Result<(String, Vec<RenameEntry>)> {
    let st = status(&ctx.wc)?;
    let found = match threshold {
        Some(threshold) => working_renames(
            &ctx.wc,
            &st,
            RenameOptions {
                threshold,
                ..RenameOptions::default()
            },
            |h| ctx.cache.fetch(&ctx.client, h).ok(),
        ),
        None => Default::default(),
    };
    let moved_to: HashMap<&str, _> = found.renames.iter().map(|r| (r.to.as_str(), r)).collect();
    let moved_from: Vec<&str> = found.renames.iter().map(|r| r.from.as_str()).collect();
    let old_of = |path: &str| -> Result<Content> {
        match ctx.wc.state.files.get(path) {
            Some(f) => Ok(Some((ctx.cache.fetch(&ctx.client, &f.content)?, f.kind))),
            None => Ok(None),
        }
    };
    let mut text = String::new();
    for e in &st.entries {
        if moved_from.contains(&e.path.as_str()) && e.change == Change::Deleted {
            continue;
        }
        let new: Content = match (&e.now, e.change) {
            (Some(now), Change::Added | Change::Modified | Change::Conflicted) => {
                let abs = ctx.wc.abs(&e.path);
                Some((read_content(&abs, &fs::symlink_metadata(&abs)?)?, now.kind))
            }
            _ => None,
        };
        match moved_to.get(e.path.as_str()) {
            Some(r) if e.change == Change::Added => {
                text.push_str(&rename_line(r.from.as_str(), &e.path, r.similarity));
                text.push('\n');
                text.push_str(&renamed_diff(
                    r.from.as_str(),
                    &e.path,
                    side(&old_of(r.from.as_str())?),
                    side(&new),
                ));
            }
            _ => text.push_str(&file_diff(&e.path, side(&old_of(&e.path)?), side(&new))),
        }
    }
    Ok((text, found.renames))
}

fn revisions(r: &Remote, from: &str, to: &str, threshold: Option<u8>) -> Result<i32> {
    let resp = revisions_diff(r, from, to, threshold)?;
    if r.json {
        print_json(&resp)?;
        return Ok(0);
    }
    term::page(&term::diff(&diff_text(r, &resp)?), false)?;
    Ok(0)
}

/// Ask the server for `FROM TO`; `None` threshold asks without renames.
pub fn revisions_diff(
    r: &Remote,
    from: &str,
    to: &str,
    threshold: Option<u8>,
) -> Result<DiffResponse> {
    let from = r.resolve(from)?;
    let to = r.resolve(to)?;
    match threshold {
        Some(t) => r.client.diff_renames(Some(&from), &to, None, Some(t)),
        None => r.client.diff(Some(&from), &to, None),
    }
}

/// The unified diff of every entry, fetching blobs through the cache.
pub fn diff_text(r: &Remote, resp: &DiffResponse) -> Result<String> {
    let fetch = |f: &Option<FileInfo>| -> Result<Content> {
        match f {
            Some(f) => Ok(Some((r.cache.fetch(r.client, &f.content)?, f.kind))),
            None => Ok(None),
        }
    };
    Ok(render(resp, fetch)?.concat())
}

/// One text block per diff entry. A rename replaces its delete and add: the
/// block sits at the new path and starts with its `renamed:` line.
fn render(
    resp: &DiffResponse,
    mut fetch: impl FnMut(&Option<FileInfo>) -> Result<Content>,
) -> Result<Vec<String>> {
    let moved_to: HashMap<&str, _> = resp.renames.iter().map(|r| (r.to.as_str(), r)).collect();
    let old_by_path: HashMap<&str, &Option<FileInfo>> = resp
        .renames
        .iter()
        .filter_map(|r| {
            let e = resp.entries.iter().find(|e| e.path == r.from)?;
            Some((r.to.as_str(), &e.old))
        })
        .collect();
    let moved_from: Vec<&str> = resp.renames.iter().map(|r| r.from.as_str()).collect();
    let mut out = Vec::new();
    for e in &resp.entries {
        let path = e.path.as_str();
        if e.new.is_none() && moved_from.contains(&path) {
            continue;
        }
        let new = fetch(&e.new)?;
        match (moved_to.get(path), old_by_path.get(path)) {
            (Some(r), Some(old)) if e.old.is_none() => {
                let old = fetch(old)?;
                out.push(format!(
                    "{}\n{}",
                    rename_line(r.from.as_str(), path, r.similarity),
                    renamed_diff(r.from.as_str(), path, side(&old), side(&new))
                ));
            }
            _ => out.push(file_diff(path, side(&fetch(&e.old)?), side(&new))),
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use leep_core::api::{DiffEntry, RenameEntry};
    use leep_core::{Hash, RepoPath};

    use super::*;

    fn info(path: &str, body: &str, kind: FileKind) -> FileInfo {
        FileInfo {
            path: RepoPath::new(path).unwrap(),
            content: Hash::of_blob(body.as_bytes()),
            kind,
            size: body.len() as u64,
        }
    }

    fn entry(old: Option<FileInfo>, new: Option<FileInfo>) -> DiffEntry {
        let path = old.as_ref().or(new.as_ref()).unwrap().path.clone();
        DiffEntry { path, old, new }
    }

    fn fetch_from(bodies: &[&str]) -> impl FnMut(&Option<FileInfo>) -> Result<Content> {
        let map: HashMap<Hash, Vec<u8>> = bodies
            .iter()
            .map(|b| (Hash::of_blob(b.as_bytes()), b.as_bytes().to_vec()))
            .collect();
        move |f| Ok(f.as_ref().map(|f| (map[&f.content].clone(), f.kind)))
    }

    #[test]
    fn renames_replace_their_delete_and_add() {
        let resp = DiffResponse {
            entries: vec![
                entry(Some(info("a.txt", "x\ny\n", FileKind::Regular)), None),
                entry(None, Some(info("b.txt", "x\ny\nz\n", FileKind::Regular))),
                entry(Some(info("c", "1\n", FileKind::Regular)), None),
                entry(None, Some(info("d", "1\n", FileKind::Regular))),
                entry(None, Some(info("e", "e\n", FileKind::Regular))),
            ],
            renames: vec![
                RenameEntry {
                    from: RepoPath::new("a.txt").unwrap(),
                    to: RepoPath::new("b.txt").unwrap(),
                    similarity: 80,
                    exact: false,
                },
                RenameEntry {
                    from: RepoPath::new("c").unwrap(),
                    to: RepoPath::new("d").unwrap(),
                    similarity: 100,
                    exact: true,
                },
            ],
            renames_truncated: false,
        };
        let blocks = render(&resp, fetch_from(&["x\ny\n", "x\ny\nz\n", "1\n", "e\n"])).unwrap();
        assert_eq!(blocks.len(), 3);
        assert!(blocks[0].starts_with("renamed: a.txt -> b.txt (80%)\n"));
        assert!(blocks[0].contains("--- a/a.txt\n+++ b/b.txt\n") && blocks[0].contains("+z"));
        assert_eq!(blocks[1], "renamed: c -> d (100%)\n");
        assert!(blocks[2].contains("--- /dev/null") && blocks[2].contains("+e"));
    }

    #[test]
    fn without_renames_the_entries_print_as_before() {
        let resp = DiffResponse {
            entries: vec![
                entry(Some(info("a", "1\n", FileKind::Regular)), None),
                entry(None, Some(info("b", "1\n", FileKind::Regular))),
            ],
            ..Default::default()
        };
        let blocks = render(&resp, fetch_from(&["1\n"])).unwrap();
        assert!(blocks[0].contains("+++ /dev/null") && blocks[0].contains("-1"));
        assert!(blocks[1].contains("--- /dev/null") && blocks[1].contains("+1"));
    }
}
