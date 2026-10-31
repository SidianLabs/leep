//! Working-copy scanning: walk files under the sparse prefixes, honour
//! `.gitignore` and `.leepignore`, and compare against the materialized state.

use std::collections::BTreeMap;
use std::fs::{self, Metadata};
use std::path::Path;
use std::time::UNIX_EPOCH;

use ignore::gitignore::{Gitignore, GitignoreBuilder};
use leep_core::api::FileInfo;
use leep_core::rename::{Detected, Fingerprint, RenameDetector, RenameOptions};
use leep_core::{FileKind, Hash, RepoPath};
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

    /// Refuse to go on while any path is conflicted, naming them.
    pub fn refuse_conflicts(&self) -> Result<()> {
        let paths: Vec<&str> = self
            .entries
            .iter()
            .filter(|e| e.change == Change::Conflicted)
            .map(|e| e.path.as_str())
            .collect();
        if paths.is_empty() {
            return Ok(());
        }
        Err(crate::error::CliError::conflict(format!(
            "unresolved conflicts in {}; fix the markers, or `leep resolve PATH` once \
             you have chosen (`leep restore PATH` takes the base version); see `leep status`",
            paths.join(", ")
        )))
    }
}

/// Ignore files read from the working copy root, in this order. A later
/// rule overrides an earlier one, so `.leepignore` wins over `.gitignore`:
/// a repository migrated from git keeps its ignores, and `!pattern` in
/// `.leepignore` can still take a path back. Nested ignore files are not
/// read.
pub const IGNORE_FILES: [&str; 2] = [".gitignore", ".leepignore"];

fn load_ignore(root: &Path) -> Gitignore {
    let mut b = GitignoreBuilder::new(root);
    for name in IGNORE_FILES {
        let f = root.join(name);
        if f.is_file() {
            let _ = b.add(f);
        }
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

/// Only regular files and symlinks can be stored. Sockets, FIFOs and devices
/// are skipped, as git does: reading a socket fails and opening a FIFO
/// blocks until a writer appears.
fn is_recordable(md: &Metadata) -> bool {
    md.is_file() || md.file_type().is_symlink()
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
    // Each directory carries whether it is ignored: one is still walked when
    // it holds tracked files, and then only those are listed, as in git.
    let mut stack: Vec<(String, bool)> = Vec::new();
    for p in prefixes {
        if p.is_empty() {
            stack.push((String::new(), false));
            continue;
        }
        let Ok(md) = fs::symlink_metadata(root.join(p)) else {
            continue;
        };
        if md.is_dir() {
            let ignored = ignore.matched_path_or_any_parents(p, true).is_ignore();
            stack.push((p.clone(), ignored));
        } else if is_recordable(&md)
            && (tracked.contains_key(p)
                || !ignore.matched_path_or_any_parents(p, false).is_ignore())
        {
            out.insert(p.clone(), md);
        }
    }
    while let Some((dir, dir_ignored)) = stack.pop() {
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
                let ignored = dir_ignored || ignore.matched(&rel, true).is_ignore();
                if ignored && !has_tracked_under(tracked, &rel) {
                    continue;
                }
                stack.push((rel, ignored));
            } else if is_recordable(&md)
                && (tracked.contains_key(&rel)
                    || !(dir_ignored || ignore.matched(&rel, false).is_ignore()))
            {
                out.insert(rel, md);
            }
        }
    }
    Ok(out)
}

pub fn mtime_ns(md: &Metadata) -> i64 {
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

/// The kind of a plain file on disk. The exec bit decides.
#[cfg(unix)]
fn local_kind(md: &Metadata, _prev: Option<&FileState>) -> FileKind {
    if is_executable(md) {
        FileKind::Executable
    } else {
        FileKind::Regular
    }
}

/// The kind of a plain file on disk where the filesystem keeps no exec bit
/// (Windows): the stored kind is kept, as git does with
/// `core.filemode=false`, so a checkout never reports a kind change it could
/// not have made. A new file is regular; the bit cannot be set from here.
#[cfg(not(unix))]
fn local_kind(_md: &Metadata, prev: Option<&FileState>) -> FileKind {
    match prev {
        Some(p) if p.kind == FileKind::Executable => FileKind::Executable,
        _ => FileKind::Regular,
    }
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
    let kind = local_kind(md, prev);
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
    for path in &wc.state.unresolved {
        match entries.iter_mut().find(|e| &e.path == path) {
            Some(e) => e.change = Change::Conflicted,
            None => entries.push(StatusEntry {
                path: path.clone(),
                change: Change::Conflicted,
                now: None,
            }),
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

/// Renames among the files `st` reports deleted and added. Exact renames
/// read nothing. For similarity, a deleted file's content comes from
/// `fetch_old` (the blob cache) and an added file's from disk; a file whose
/// content cannot be read is left unpaired.
pub fn working_renames(
    wc: &WorkCopy,
    st: &Status,
    opts: RenameOptions,
    mut fetch_old: impl FnMut(&Hash) -> Option<Vec<u8>>,
) -> Detected {
    let info = |path: &str, s: &FileState| -> Option<FileInfo> {
        Some(FileInfo {
            path: RepoPath::new(path).ok()?,
            content: s.content,
            kind: s.kind,
            size: s.size,
        })
    };
    let mut deleted = Vec::new();
    let mut added = Vec::new();
    let mut on_disk: BTreeMap<Hash, &str> = BTreeMap::new();
    for e in &st.entries {
        match (e.change, &e.now) {
            (Change::Deleted, _) => {
                deleted.extend(wc.state.files.get(&e.path).and_then(|s| info(&e.path, s)))
            }
            (Change::Added, Some(now)) => {
                added.extend(info(&e.path, now));
                on_disk.entry(now.content).or_insert(&e.path);
            }
            _ => {}
        }
    }
    let det = RenameDetector::new(deleted, added, opts);
    let fingerprints = det
        .wanted()
        .into_iter()
        .filter_map(|h| {
            let bytes = match on_disk.get(&h) {
                Some(p) => fs::read(wc.abs(p)).ok(),
                None => fetch_old(&h),
            }?;
            Some((h, Fingerprint::of(&bytes)?))
        })
        .collect();
    det.finish(&fingerprints)
}

pub fn still_conflicted(wc: &WorkCopy, path: &str) -> bool {
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
                token: None,
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
    fn gitignore_is_read_and_leepignore_wins() {
        let d = tempfile::tempdir().unwrap();
        let wc = wc(d.path());
        write(d.path(), ".gitignore", "*.log\nbuild/\n/env\n");
        write(d.path(), ".leepignore", "!keep.log\n*.tmp\n");
        write(d.path(), "a.txt", "a");
        write(d.path(), "x.log", "x");
        write(d.path(), "keep.log", "k");
        write(d.path(), "y.tmp", "y");
        write(d.path(), "build/out", "o");
        write(d.path(), "env", "secret");
        write(d.path(), "src/env", "kept: anchored to the root");
        let found = walk(&wc.root, &wc.prefixes(), &BTreeMap::new()).unwrap();
        let keys: Vec<_> = found.keys().map(String::as_str).collect();
        assert_eq!(
            keys,
            [".gitignore", ".leepignore", "a.txt", "keep.log", "src/env"]
        );
    }

    /// Only the root's ignore files are read.
    #[test]
    fn nested_ignore_files_are_not_read() {
        let d = tempfile::tempdir().unwrap();
        let wc = wc(d.path());
        write(d.path(), "sub/.gitignore", "*.txt\n");
        write(d.path(), "sub/.leepignore", "*.txt\n");
        write(d.path(), "sub/a.txt", "a");
        let found = walk(&wc.root, &wc.prefixes(), &BTreeMap::new()).unwrap();
        assert!(found.contains_key("sub/a.txt"), "{:?}", found.keys());
    }

    /// A `.git` directory anywhere (a git checkout converted in place, or a
    /// vendored submodule) is never walked, ignore files or not.
    #[test]
    fn git_directories_are_never_walked() {
        let d = tempfile::tempdir().unwrap();
        let wc = wc(d.path());
        write(d.path(), ".git/HEAD", "ref: refs/heads/main");
        write(d.path(), ".git/objects/ab/cdef", "x");
        write(d.path(), "vendor/lib/.git/config", "g");
        write(d.path(), "vendor/lib/src.c", "c");
        let found = walk(&wc.root, &wc.prefixes(), &BTreeMap::new()).unwrap();
        let keys: Vec<_> = found.keys().map(String::as_str).collect();
        assert_eq!(keys, ["vendor/lib/src.c"]);
    }

    #[test]
    fn tracked_files_matching_gitignore_still_show() {
        let d = tempfile::tempdir().unwrap();
        let mut wc = wc(d.path());
        write(d.path(), "config.local", "v1");
        write(d.path(), "out/keep.bin", "b1");
        track_all(&mut wc);
        // The rule arrives after the files were tracked, as when a repository
        // migrated from git carries files its .gitignore also matches.
        write(d.path(), ".gitignore", "*.local\nout/\n");
        write(d.path(), "other.local", "untracked");
        write(d.path(), "out/new.bin", "untracked");
        let s = status(&wc).unwrap();
        assert_eq!(change_of(&s, ".gitignore"), Some(Change::Added));
        assert_eq!(change_of(&s, "config.local"), None);
        assert_eq!(change_of(&s, "other.local"), None);
        assert_eq!(change_of(&s, "out/new.bin"), None);

        write(d.path(), "config.local", "v2, longer");
        write(d.path(), "out/keep.bin", "b2, longer");
        let s = status(&wc).unwrap();
        assert_eq!(change_of(&s, "config.local"), Some(Change::Modified));
        assert_eq!(change_of(&s, "out/keep.bin"), Some(Change::Modified));
        assert_eq!(change_of(&s, "other.local"), None);
        assert_eq!(change_of(&s, "out/new.bin"), None);
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

    #[cfg(unix)]
    #[test]
    fn status_skips_sockets() {
        let d = tempfile::tempdir().unwrap();
        let mut wc = wc(d.path());
        write(d.path(), "a.txt", "a");
        track_all(&mut wc);
        let _sock = std::os::unix::net::UnixListener::bind(d.path().join("app.sock")).unwrap();
        let s = status(&wc).unwrap();
        assert!(s.is_clean(), "{:?}", s.entries);
    }

    /// A FIFO must be skipped without being opened: opening one for reading
    /// blocks until a writer appears, so a regression hangs rather than fails.
    #[cfg(unix)]
    #[test]
    fn status_skips_fifos_without_opening_them() {
        let d = tempfile::tempdir().unwrap();
        let mut wc = wc(d.path());
        write(d.path(), "a.txt", "a");
        track_all(&mut wc);
        let made = std::process::Command::new("mkfifo")
            .arg(d.path().join("x.fifo"))
            .status()
            .unwrap();
        assert!(made.success());
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let r = status(&wc).map(|s| s.entries.len());
            let _ = tx.send(r.map_err(|e| e.to_string()));
        });
        let got = rx
            .recv_timeout(std::time::Duration::from_secs(10))
            .expect("status hung on a FIFO");
        assert_eq!(got.unwrap(), 0);
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

    /// Without an exec bit to read, a tracked executable stays one and a
    /// new file is regular: no kind change can come from this platform.
    #[cfg(not(unix))]
    #[test]
    fn without_an_exec_bit_the_stored_kind_is_kept() {
        let d = tempfile::tempdir().unwrap();
        write(d.path(), "run.sh", "#!/bin/sh\n");
        let p = d.path().join("run.sh");
        let md = fs::metadata(&p).unwrap();
        let stored = FileState {
            content: Hash::of_blob(b"other"),
            kind: FileKind::Executable,
            size: 1,
            mtime_ns: 0,
        };
        let s = scan_file(&p, &md, Some(&stored)).unwrap();
        assert_eq!(s.kind, FileKind::Executable);
        assert_eq!(
            s.content,
            Hash::of_blob(b"#!/bin/sh\n"),
            "content is rehashed"
        );
        assert_eq!(scan_file(&p, &md, None).unwrap().kind, FileKind::Regular);
        let regular = FileState {
            kind: FileKind::Regular,
            ..stored
        };
        assert_eq!(
            scan_file(&p, &md, Some(&regular)).unwrap().kind,
            FileKind::Regular
        );
    }

    fn renames_of(wc: &WorkCopy, old: &BTreeMap<Hash, Vec<u8>>) -> Vec<(String, String, u8)> {
        let st = status(wc).unwrap();
        working_renames(wc, &st, RenameOptions::default(), |h| old.get(h).cloned())
            .renames
            .into_iter()
            .map(|r| (r.from.to_string(), r.to.to_string(), r.similarity))
            .collect()
    }

    #[test]
    fn working_copy_renames_exact_and_similar() {
        let d = tempfile::tempdir().unwrap();
        let mut wc = wc(d.path());
        let long: String = (0..10).map(|i| format!("line {i}\n")).collect();
        write(d.path(), "same.txt", "unchanged body\n");
        write(d.path(), "edit.txt", &long);
        write(d.path(), "gone.txt", "only deleted\n");
        track_all(&mut wc);
        let old: BTreeMap<Hash, Vec<u8>> = [
            ("unchanged body\n".to_string()),
            long.clone(),
            "only deleted\n".to_string(),
        ]
        .into_iter()
        .map(|b| (Hash::of_blob(b.as_bytes()), b.into_bytes()))
        .collect();
        assert!(renames_of(&wc, &old).is_empty());

        fs::remove_file(d.path().join("same.txt")).unwrap();
        fs::remove_file(d.path().join("edit.txt")).unwrap();
        fs::remove_file(d.path().join("gone.txt")).unwrap();
        write(d.path(), "dir/same.txt", "unchanged body\n");
        write(d.path(), "dir/edit.txt", &format!("{long}more\n"));
        write(d.path(), "fresh.txt", "brand new\n");
        let got = renames_of(&wc, &old);
        assert_eq!(got.len(), 2, "{got:?}");
        assert_eq!(got[0], ("edit.txt".into(), "dir/edit.txt".into(), 96));
        assert_eq!(got[1], ("same.txt".into(), "dir/same.txt".into(), 100));
        // Without the old blobs only the exact rename is found.
        let st = status(&wc).unwrap();
        let exact = working_renames(&wc, &st, RenameOptions::default(), |_| None);
        assert_eq!(exact.renames.len(), 1);
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
