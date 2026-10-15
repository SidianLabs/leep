//! Working-copy scanning: walk files under the sparse prefixes, honour
//! `.leepignore`, and compare against the materialized state.

use std::collections::BTreeMap;
use std::fs::{self, Metadata};
use std::path::Path;
use std::time::UNIX_EPOCH;

use ignore::gitignore::{Gitignore, GitignoreBuilder};
use leep_core::{FileKind, Hash};
use serde::Serialize;

use crate::error::Result;
use crate::state::{FileState, WorkCopy, DIR};

/// Marker line `leep land` writes into conflicted files.
pub const CONFLICT_MARKER: &str = "<<<<<<< ";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Change {
    Added,
    Modified,
    Deleted,
    Conflicted,
}

impl Change {
    pub fn letter(self) -> char {
        match self {
            Change::Added => 'A',
            Change::Modified => 'M',
            Change::Deleted => 'D',
            Change::Conflicted => 'C',
        }
    }
}

#[derive(Clone, Debug)]
pub struct StatusEntry {
    pub path: String,
    pub change: Change,
    /// Current on-disk state; `None` for deletions.
    pub now: Option<FileState>,
}

#[derive(Debug, Default)]
pub struct Status {
    pub entries: Vec<StatusEntry>,
}

impl Status {
    pub fn is_clean(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn has_conflicts(&self) -> bool {
        self.entries.iter().any(|e| e.change == Change::Conflicted)
    }
}

fn load_ignore(root: &Path) -> Gitignore {
    let mut b = GitignoreBuilder::new(root);
    let f = root.join(".leepignore");
    if f.is_file() {
        let _ = b.add(f);
    }
    b.build().unwrap_or_else(|_| Gitignore::empty())
}

fn has_tracked_under(files: &BTreeMap<String, FileState>, dir: &str) -> bool {
    let start = format!("{dir}/");
    files
        .range(start.clone()..)
        .next()
        .is_some_and(|(k, _)| k.starts_with(&start))
}

/// All non-ignored files (and symlinks) under `prefixes`, by repo path.
/// Tracked files are listed even if an ignore pattern matches them.
pub fn walk(
    root: &Path,
    prefixes: &[String],
    tracked: &BTreeMap<String, FileState>,
) -> Result<BTreeMap<String, Metadata>> {
    let ignore = load_ignore(root);
    let mut out = BTreeMap::new();
    let mut stack: Vec<String> = Vec::new();
    for p in prefixes {
        if p.is_empty() {
            stack.push(String::new());
            continue;
        }
        let Ok(md) = fs::symlink_metadata(root.join(p)) else {
            continue;
        };
        if md.is_dir() {
            stack.push(p.clone());
        } else if tracked.contains_key(p)
            || !ignore.matched_path_or_any_parents(p, false).is_ignore()
        {
            out.insert(p.clone(), md);
        }
    }
    while let Some(dir) = stack.pop() {
        let abs = if dir.is_empty() {
            root.to_path_buf()
        } else {
            root.join(&dir)
        };
        let Ok(rd) = fs::read_dir(&abs) else { continue };
        for entry in rd {
            let entry = entry?;
            let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
                continue;
            };
            if name == DIR || name == ".git" {
                continue;
            }
            let rel = if dir.is_empty() {
                name
            } else {
                format!("{dir}/{name}")
            };
            let md = entry.metadata()?;
            if md.is_dir() {
                if ignore.matched(&rel, true).is_ignore() && !has_tracked_under(tracked, &rel) {
                    continue;
                }
                stack.push(rel);
            } else if tracked.contains_key(&rel) || !ignore.matched(&rel, false).is_ignore() {
                out.insert(rel, md);
            }
        }
    }
    Ok(out)
}

fn mtime_ns(md: &Metadata) -> i64 {
    md.modified()
        .ok()
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}

#[cfg(unix)]
fn is_executable(md: &Metadata) -> bool {
    use std::os::unix::fs::PermissionsExt;
    md.permissions().mode() & 0o111 != 0
}

#[cfg(not(unix))]
fn is_executable(_: &Metadata) -> bool {
    false
}

/// Bytes that represent a file in a blob: contents, or the link target.
pub fn read_content(path: &Path, md: &Metadata) -> Result<Vec<u8>> {
    if md.file_type().is_symlink() {
        let t = fs::read_link(path)?;
        Ok(t.to_string_lossy().into_owned().into_bytes())
    } else {
        Ok(fs::read(path)?)
    }
}

/// Describe a file on disk, reusing the cached hash when mtime and size match.
pub fn scan_file(abs: &Path, md: &Metadata, prev: Option<&FileState>) -> Result<FileState> {
    let mtime = mtime_ns(md);
    if md.file_type().is_symlink() {
        let data = read_content(abs, md)?;
        return Ok(FileState {
            content: Hash::of_blob(&data),
            kind: FileKind::Symlink,
            size: data.len() as u64,
            mtime_ns: mtime,
        });
    }
    let kind = if is_executable(md) {
        FileKind::Executable
    } else {
        FileKind::Regular
    };
    let content = match prev {
        Some(p) if p.size == md.len() && p.mtime_ns == mtime && p.kind != FileKind::Symlink => {
            p.content
        }
        _ => Hash::of_blob(&fs::read(abs)?),
    };
    Ok(FileState {
        content,
        kind,
        size: md.len(),
        mtime_ns: mtime,
    })
}

/// Compare the working directory with the materialized state.
pub fn status(wc: &WorkCopy) -> Result<Status> {
    let prefixes = wc.prefixes();
    let found = walk(&wc.root, &prefixes, &wc.state.files)?;
    let mut entries = Vec::new();
    for (path, md) in &found {
        let prev = wc.state.files.get(path);
        let now = scan_file(&wc.abs(path), md, prev)?;
        let change = match prev {
            None => Change::Added,
            Some(p) if p.content != now.content || p.kind != now.kind => Change::Modified,
            Some(_) => continue,
        };
        entries.push(StatusEntry {
            path: path.clone(),
            change,
            now: Some(now),
        });
    }
    for path in wc.state.files.keys() {
        if !found.contains_key(path) && wc.in_scope(path) {
            entries.push(StatusEntry {
                path: path.clone(),
                change: Change::Deleted,
                now: None,
            });
        }
    }
    for path in &wc.state.conflicts {
        if still_conflicted(wc, path) {
            match entries.iter_mut().find(|e| &e.path == path) {
                Some(e) => e.change = Change::Conflicted,
                None => {
                    // Marker content identical to the recorded state.
                    entries.push(StatusEntry {
                        path: path.clone(),
                        change: Change::Conflicted,
                        now: None,
                    });
                }
            }
        }
    }
    entries.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(Status { entries })
}

fn still_conflicted(wc: &WorkCopy, path: &str) -> bool {
    fs::read_to_string(wc.abs(path))
        .map(|s| s.lines().any(|l| l.starts_with(CONFLICT_MARKER)))
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::Config;

    fn wc(dir: &Path) -> WorkCopy {
        WorkCopy::init(
            dir,
            Config {
                server: "http://x".into(),
                target_ref: "main".into(),
                prefixes: vec![],
            },
        )
        .unwrap()
    }

    fn write(dir: &Path, rel: &str, body: &str) {
        let p = dir.join(rel);
        fs::create_dir_all(p.parent().unwrap()).unwrap();
        fs::write(p, body).unwrap();
    }

    /// Record every walked file as materialized.
    fn track_all(wc: &mut WorkCopy) {
        let found = walk(&wc.root, &wc.prefixes(), &wc.state.files).unwrap();
        for (p, md) in found {
            let fs = scan_file(&wc.abs(&p), &md, None).unwrap();
            wc.state.files.insert(p, fs);
        }
    }

    #[test]
    fn walk_skips_meta_dirs_and_ignored() {
        let d = tempfile::tempdir().unwrap();
        let wc = wc(d.path());
        write(d.path(), ".leepignore", "*.log\ntarget/\n!keep.log\n");
        write(d.path(), "a.txt", "a");
        write(d.path(), "src/b.rs", "b");
        write(d.path(), "x.log", "x");
        write(d.path(), "keep.log", "k");
        write(d.path(), "target/out", "o");
        write(d.path(), "src/target/deep", "o");
        write(d.path(), ".git/config", "g");
        let found = walk(&wc.root, &wc.prefixes(), &BTreeMap::new()).unwrap();
        let keys: Vec<_> = found.keys().map(String::as_str).collect();
        assert_eq!(keys, [".leepignore", "a.txt", "keep.log", "src/b.rs"]);
    }

    #[test]
    fn walk_honours_prefixes() {
        let d = tempfile::tempdir().unwrap();
        write(d.path(), "a.txt", "a");
        write(d.path(), "src/b.rs", "b");
        write(d.path(), "docs/c.md", "c");
        let found = walk(d.path(), &["src".into(), "a.txt".into()], &BTreeMap::new()).unwrap();
        let keys: Vec<_> = found.keys().map(String::as_str).collect();
        assert_eq!(keys, ["a.txt", "src/b.rs"]);
    }

    fn change_of(s: &Status, p: &str) -> Option<Change> {
        s.entries.iter().find(|e| e.path == p).map(|e| e.change)
    }

    #[test]
    fn detects_add_modify_delete() {
        let d = tempfile::tempdir().unwrap();
        let mut wc = wc(d.path());
        write(d.path(), "keep.txt", "keep");
        write(d.path(), "mod.txt", "old");
        write(d.path(), "del.txt", "bye");
        track_all(&mut wc);
        assert!(status(&wc).unwrap().is_clean());

        write(d.path(), "mod.txt", "new!");
        fs::remove_file(d.path().join("del.txt")).unwrap();
        write(d.path(), "add.txt", "added");
        let s = status(&wc).unwrap();
        assert_eq!(change_of(&s, "mod.txt"), Some(Change::Modified));
        assert_eq!(change_of(&s, "del.txt"), Some(Change::Deleted));
        assert_eq!(change_of(&s, "add.txt"), Some(Change::Added));
        assert_eq!(change_of(&s, "keep.txt"), None);
    }

    #[test]
    fn same_size_edit_is_detected() {
        let d = tempfile::tempdir().unwrap();
        let mut wc = wc(d.path());
        write(d.path(), "f.txt", "aaaa");
        track_all(&mut wc);
        // Same size, different content, mtime advanced: must rehash.
        std::thread::sleep(std::time::Duration::from_millis(20));
        write(d.path(), "f.txt", "bbbb");
        let s = status(&wc).unwrap();
        assert_eq!(change_of(&s, "f.txt"), Some(Change::Modified));
    }

    #[cfg(unix)]
    #[test]
    fn detects_exec_bit_and_symlink_changes() {
        use std::os::unix::fs::{symlink, PermissionsExt};
        let d = tempfile::tempdir().unwrap();
        let mut wc = wc(d.path());
        write(d.path(), "run.sh", "#!/bin/sh\n");
        symlink("target-a", d.path().join("link")).unwrap();
        track_all(&mut wc);
        assert_eq!(wc.state.files["link"].kind, FileKind::Symlink);
        assert!(status(&wc).unwrap().is_clean());

        let p = d.path().join("run.sh");
        let mut perm = fs::metadata(&p).unwrap().permissions();
        perm.set_mode(0o755);
        fs::set_permissions(&p, perm).unwrap();
        fs::remove_file(d.path().join("link")).unwrap();
        symlink("target-b", d.path().join("link")).unwrap();
        let s = status(&wc).unwrap();
        assert_eq!(change_of(&s, "run.sh"), Some(Change::Modified));
        assert_eq!(change_of(&s, "link"), Some(Change::Modified));
    }

    #[test]
    fn conflicts_show_until_markers_removed() {
        let d = tempfile::tempdir().unwrap();
        let mut wc = wc(d.path());
        write(d.path(), "c.txt", "base");
        track_all(&mut wc);
        write(
            d.path(),
            "c.txt",
            "<<<<<<< ours\nx\n=======\ny\n>>>>>>> theirs\n",
        );
        wc.state.conflicts.push("c.txt".into());
        let s = status(&wc).unwrap();
        assert_eq!(change_of(&s, "c.txt"), Some(Change::Conflicted));
        assert!(s.has_conflicts());
        write(d.path(), "c.txt", "resolved");
        let s = status(&wc).unwrap();
        assert_eq!(change_of(&s, "c.txt"), Some(Change::Modified));
    }
}
