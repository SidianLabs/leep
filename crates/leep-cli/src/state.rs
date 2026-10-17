//! Working copy layout: `.leep/config.json` (user settings) and
//! `.leep/state.json` (what is materialized on disk).

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use leep_core::{ChangesetId, FileKind, Hash, RepoPath};
use serde::{Deserialize, Serialize};

use crate::error::{CliError, Result};

pub const DIR: &str = ".leep";

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Config {
    pub server: String,
    #[serde(default = "default_ref")]
    pub target_ref: String,
    /// Sparse-checkout roots; empty means the whole repository.
    #[serde(default)]
    pub prefixes: Vec<String>,
    /// Bearer token; lowest precedence after `--token` and `LEEP_TOKEN`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
}

// Manual so the token can never reach logs through `{:?}`.
impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Config")
            .field("server", &self.server)
            .field("target_ref", &self.target_ref)
            .field("prefixes", &self.prefixes)
            .field("token", &self.token.as_ref().map(|_| "<redacted>"))
            .finish()
    }
}

/// Pick the token: `--token` flag, then `LEEP_TOKEN`, then the config file.
/// Empty values count as unset.
pub fn resolve_token(
    flag: Option<&str>,
    env: Option<&str>,
    config: Option<&str>,
) -> Option<String> {
    [flag, env, config]
        .into_iter()
        .flatten()
        .map(str::trim)
        .find(|t| !t.is_empty())
        .map(str::to_string)
}

fn default_ref() -> String {
    "main".into()
}

/// A materialized file as of the last checkout/commit. `mtime_ns` and `size`
/// let status skip rehashing unchanged files.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileState {
    pub content: Hash,
    pub kind: FileKind,
    pub size: u64,
    pub mtime_ns: i64,
}

/// A stash entry. The content lives on the server as an ordinary changeset
/// (`leep.snapshot = "stash"`); only this list is local.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Stash {
    pub id: ChangesetId,
    /// Base the stashed changes were made against.
    pub base: Option<ChangesetId>,
    pub message: String,
    /// Unix seconds.
    pub timestamp: i64,
}

/// A merge that stopped on conflicts. The base is still the first parent;
/// the next `leep commit` adds `other` as the second.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MergeState {
    pub other: ChangesetId,
    /// Default message for the merge commit.
    pub message: String,
    /// Paths the merge created that the base lacks, so `--abort` can remove
    /// them (they would otherwise look like the user's new files).
    #[serde(default)]
    pub added: Vec<String>,
}

/// A cherry-pick or revert that stopped on conflicts. The next `leep commit`
/// records the resolved change with `message` as its default.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PickState {
    /// The changeset being picked or reverted.
    pub from: ChangesetId,
    pub message: String,
    #[serde(default)]
    pub revert: bool,
    /// See [`MergeState::added`].
    #[serde(default)]
    pub added: Vec<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct State {
    pub base: Option<ChangesetId>,
    pub files: BTreeMap<String, FileState>,
    /// Paths where `leep land` wrote conflict markers.
    #[serde(default)]
    pub conflicts: Vec<String>,
    /// Paths whose conflict had no text merge: the local version was kept,
    /// without markers. They block `commit` and `land` until `leep resolve`
    /// or `leep restore` clears them.
    #[serde(default)]
    pub unresolved: Vec<String>,
    /// Newest first. Kept in `state.json` so it is saved with the base it
    /// refers to.
    #[serde(default)]
    pub stashes: Vec<Stash>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub merge_in_progress: Option<MergeState>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pick_in_progress: Option<PickState>,
}

#[derive(Debug)]
pub struct WorkCopy {
    pub root: PathBuf,
    pub config: Config,
    pub state: State,
}

/// Normalize a user-supplied prefix: strip slashes, validate; `""` = root.
pub fn normalize_prefix(p: &str) -> Result<String> {
    let t = p.trim_start_matches("./").trim_matches('/');
    if t.is_empty() || t == "." {
        return Ok(String::new());
    }
    RepoPath::new(t)?;
    Ok(t.to_string())
}

/// Write `bytes` to `path` via a temp file and rename.
pub fn atomic_write(path: &Path, bytes: &[u8]) -> Result<()> {
    atomic_write_mode(path, bytes, false)
}

/// Like [`atomic_write`]; `private` creates the file owner-only (0600 on unix)
/// before any bytes are written, so a secret is never briefly world-readable.
pub fn atomic_write_mode(path: &Path, bytes: &[u8], private: bool) -> Result<()> {
    let tmp = path.with_extension(format!("tmp-{}", std::process::id()));
    if private {
        use std::io::Write;
        // A leftover temp file would keep its old, possibly wider mode:
        // `mode` applies only to a file this call creates.
        let _ = fs::remove_file(&tmp);
        let mut opts = fs::OpenOptions::new();
        opts.write(true).create_new(true);
        #[cfg(unix)]
        std::os::unix::fs::OpenOptionsExt::mode(&mut opts, 0o600);
        opts.open(&tmp)?.write_all(bytes)?;
    } else {
        fs::write(&tmp, bytes)?;
    }
    fs::rename(&tmp, path).inspect_err(|_| {
        let _ = fs::remove_file(&tmp);
    })?;
    Ok(())
}

/// Whether a path component names a control directory once the filesystem
/// has normalized it: case-insensitively, ignoring the code points HFS+
/// drops and the trailing dots and spaces Windows drops, and including the
/// 8.3 short names Windows gives such directories.
fn reserved_name(component: &str) -> bool {
    let ignorable = |c: &char| {
        matches!(
            c,
            '\u{200b}'..='\u{200f}'
                | '\u{202a}'..='\u{202e}'
                | '\u{2060}'..='\u{2064}'
                | '\u{206a}'..='\u{206f}'
                | '\u{feff}'
        )
    };
    let folded: String = component
        .chars()
        .filter(|c| !ignorable(c))
        .collect::<String>()
        .trim_end_matches(['.', ' '])
        .to_lowercase();
    matches!(folded.as_str(), ".leep" | ".git" | "leep~1" | "git~1")
}

/// Names Windows maps to devices whatever the extension.
fn windows_device(component: &str) -> bool {
    let stem = component.split('.').next().unwrap_or("").trim_end();
    let stem = stem.to_ascii_lowercase();
    matches!(
        stem.as_str(),
        "con" | "prn" | "aux" | "nul" | "conin$" | "conout$"
    ) || ((stem.starts_with("com") || stem.starts_with("lpt"))
        && stem.len() == 4
        && stem.as_bytes()[3].is_ascii_digit()
        && stem.as_bytes()[3] != b'0')
}

/// Refuse a repository path the CLI must never write: one with a component
/// that names `.leep` (whose `config.json` holds the server URL and token)
/// or `.git` (whose hooks run as the user), in any spelling the filesystem
/// treats as the same name. With `windows`, also components that Windows
/// reads as a path separator, a drive or a device. The server allows these
/// names, so the check is here, where files are written.
pub fn check_repo_path(path: &str, windows: bool) -> Result<()> {
    for c in path.split('/') {
        let bad = if reserved_name(c) {
            Some("names a control directory")
        } else if windows && c.contains(['\\', ':']) {
            Some("contains a character Windows reads as a separator or drive")
        } else if windows && windows_device(c) {
            Some("is a Windows device name")
        } else {
            None
        };
        if let Some(why) = bad {
            return Err(CliError::server(format!(
                "refusing to write repository path {path:?}: component {c:?} {why}"
            ))
            .retryable(false));
        }
    }
    Ok(())
}

impl WorkCopy {
    pub fn init(root: &Path, config: Config) -> Result<Self> {
        let dir = root.join(DIR);
        if dir.exists() {
            return Err(CliError::user(format!(
                "{} is already a leep working copy",
                root.display()
            )));
        }
        fs::create_dir_all(&dir)?;
        let wc = WorkCopy {
            root: root.to_path_buf(),
            config,
            state: State::default(),
        };
        wc.save_config()?;
        wc.save_state()?;
        Ok(wc)
    }

    /// Find the working copy containing `start` by walking up to a `.leep`.
    pub fn discover(start: &Path) -> Result<Self> {
        let start = fs::canonicalize(start)?;
        for dir in start.ancestors() {
            if dir.join(DIR).is_dir() {
                return Self::open(dir);
            }
        }
        Err(CliError::user(
            "not a leep working copy (no .leep found); run `leep init`",
        ))
    }

    pub fn open(root: &Path) -> Result<Self> {
        let dir = root.join(DIR);
        let read = |name: &str| -> Result<Vec<u8>> {
            fs::read(dir.join(name))
                .map_err(|e| CliError::user(format!("cannot read .leep/{name}: {e}")))
        };
        let config = serde_json::from_slice(&read("config.json")?)
            .map_err(|e| CliError::user(format!("bad .leep/config.json: {e}")))?;
        let state = serde_json::from_slice(&read("state.json")?)
            .map_err(|e| CliError::user(format!("bad .leep/state.json: {e}")))?;
        Ok(WorkCopy {
            root: root.to_path_buf(),
            config,
            state,
        })
    }

    pub fn save_config(&self) -> Result<()> {
        let b = serde_json::to_vec_pretty(&self.config)?;
        atomic_write_mode(
            &self.root.join(DIR).join("config.json"),
            &b,
            self.config.token.is_some(),
        )
    }

    pub fn save_state(&self) -> Result<()> {
        let b = serde_json::to_vec(&self.state)?;
        atomic_write(&self.root.join(DIR).join("state.json"), &b)
    }

    pub fn abs(&self, path: &str) -> PathBuf {
        self.root.join(path)
    }

    /// Where to write or remove the repository file `path`, refusing what a
    /// malicious server or repository could use to reach outside the working
    /// copy or into control directories: a reserved name (see
    /// [`check_repo_path`]) or a directory on the way that is a symbolic
    /// link, such as one an earlier write of the same checkout created. Check
    /// right before each write, since earlier writes change the answer.
    pub fn abs_for_write(&self, path: &str) -> Result<PathBuf> {
        check_repo_path(path, cfg!(windows))?;
        let mut dir = self.root.clone();
        let mut parts = path.split('/').peekable();
        while let Some(part) = parts.next() {
            if parts.peek().is_none() {
                break;
            }
            dir.push(part);
            match fs::symlink_metadata(&dir) {
                Ok(md) if md.file_type().is_symlink() => {
                    return Err(CliError::server(format!(
                        "refusing to write {path}: {} is a symbolic link, so the file \
                         would land outside the working copy",
                        dir.strip_prefix(&self.root).unwrap_or(&dir).display()
                    ))
                    .retryable(false));
                }
                Ok(_) => {}
                Err(_) => break,
            }
        }
        Ok(self.root.join(path))
    }

    /// Sparse prefixes, with `[""]` (whole repo) when none are configured.
    pub fn prefixes(&self) -> Vec<String> {
        if self.config.prefixes.is_empty() {
            vec![String::new()]
        } else {
            self.config.prefixes.clone()
        }
    }

    pub fn in_scope(&self, path: &str) -> bool {
        self.prefixes()
            .iter()
            .any(|p| RepoPath::new(path).is_ok_and(|rp| rp.is_within(p)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn state_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let mut wc = WorkCopy::init(
            dir.path(),
            Config {
                server: "http://x".into(),
                target_ref: "main".into(),
                prefixes: vec!["src".into()],
                token: None,
            },
        )
        .unwrap();
        wc.state.base = Some(ChangesetId(Hash::of_blob(b"cs")));
        wc.state.files.insert(
            "src/a.rs".into(),
            FileState {
                content: Hash::of_blob(b"a"),
                kind: FileKind::Executable,
                size: 1,
                mtime_ns: 1_700_000_000_123_456_789,
            },
        );
        wc.state.conflicts.push("src/a.rs".into());
        wc.state.unresolved.push("src/b.bin".into());
        wc.state.stashes.push(Stash {
            id: ChangesetId(Hash::of_blob(b"stash")),
            base: wc.state.base,
            message: "wip".into(),
            timestamp: 5,
        });
        wc.state.merge_in_progress = Some(MergeState {
            other: ChangesetId(Hash::of_blob(b"other")),
            message: "Merge x".into(),
            added: vec!["src/new.rs".into()],
        });
        wc.state.pick_in_progress = Some(PickState {
            from: ChangesetId(Hash::of_blob(b"from")),
            message: "Revert \"x\"".into(),
            revert: true,
            added: vec![],
        });
        wc.save_state().unwrap();
        let back = WorkCopy::open(dir.path()).unwrap();
        assert_eq!(back.state, wc.state);
        assert_eq!(back.config, wc.config);
        assert!(WorkCopy::init(dir.path(), wc.config.clone()).is_err());
    }

    /// Security review 2026-10, S-02.
    #[test]
    fn control_directories_are_never_written_in_any_spelling() {
        for bad in [
            ".leep/config.json",
            "sub/.leep/state.json",
            ".LEEP/config.json",
            ".git/hooks/post-checkout",
            "a/.Git/config",
            ".git./hooks/x",
            ".leep /x",
            ".g\u{200c}it/hooks/x",
            "\u{feff}.leep/x",
            "GIT~1/hooks/x",
        ] {
            assert!(check_repo_path(bad, false).is_err(), "{bad:?}");
            assert!(check_repo_path(bad, true).is_err(), "{bad:?}");
        }
        for ok in [
            ".leepignore",
            ".gitignore",
            "a/.github/x",
            "leep/x",
            "x.git",
        ] {
            check_repo_path(ok, false).unwrap();
            check_repo_path(ok, true).unwrap();
        }
        // Only Windows reads these as separators, drives or devices.
        for win in [
            "a\\..\\..\\x",
            "C:",
            "a/c:evil",
            "CON",
            "dir/nul.txt",
            "com1.c",
            "LPT9",
        ] {
            check_repo_path(win, false).unwrap();
            assert!(check_repo_path(win, true).is_err(), "{win:?}");
        }
        for ok in ["com0", "console.c", "nullable.rs", "com10"] {
            check_repo_path(ok, true).unwrap();
        }
    }

    #[cfg(unix)]
    #[test]
    fn writes_never_go_through_a_symlinked_directory() {
        let dir = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let wc = WorkCopy::init(
            dir.path(),
            Config {
                server: "http://x".into(),
                target_ref: "main".into(),
                prefixes: vec![],
                token: None,
            },
        )
        .unwrap();
        std::os::unix::fs::symlink(outside.path(), dir.path().join("a")).unwrap();
        let e = wc.abs_for_write("a/pwned").unwrap_err().to_string();
        assert!(e.contains("symbolic link"), "{e}");
        assert!(wc.abs_for_write("a/b/c").is_err());
        // The link itself may be replaced, and real directories are fine.
        assert_eq!(wc.abs_for_write("a").unwrap(), dir.path().join("a"));
        fs::create_dir_all(dir.path().join("d/e")).unwrap();
        assert_eq!(wc.abs_for_write("d/e/f").unwrap(), dir.path().join("d/e/f"));
        assert!(wc.abs_for_write("new/dir/f").is_ok());
    }

    #[test]
    fn token_precedence() {
        let r = resolve_token;
        assert_eq!(r(Some("f"), Some("e"), Some("c")).as_deref(), Some("f"));
        assert_eq!(r(None, Some("e"), Some("c")).as_deref(), Some("e"));
        assert_eq!(r(None, None, Some("c")).as_deref(), Some("c"));
        assert_eq!(r(None, None, None), None);
        // Empty or blank values fall through.
        assert_eq!(r(Some(""), Some(" "), Some("c")).as_deref(), Some("c"));
        assert_eq!(r(Some(" f \n"), None, None).as_deref(), Some("f"));
    }

    #[test]
    fn token_in_config_is_private_and_redacted() {
        let dir = tempfile::tempdir().unwrap();
        let wc = WorkCopy::init(
            dir.path(),
            Config {
                server: "http://x".into(),
                target_ref: "main".into(),
                prefixes: vec![],
                token: Some("s3cret".into()),
            },
        )
        .unwrap();
        let cfg = dir.path().join(DIR).join("config.json");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = fs::metadata(&cfg).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o600);
            // Re-saving keeps it private.
            wc.save_config().unwrap();
            let mode = fs::metadata(&cfg).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o600);
        }
        assert!(!format!("{wc:?}").contains("s3cret"));
        assert_eq!(
            WorkCopy::open(dir.path()).unwrap().config.token.as_deref(),
            Some("s3cret")
        );
    }

    #[test]
    fn old_config_without_token_loads() {
        let c: Config = serde_json::from_str(r#"{"server":"http://x"}"#).unwrap();
        assert_eq!(c.token, None);
        let out = serde_json::to_string(&c).unwrap();
        assert!(!out.contains("token"));
    }

    #[test]
    fn state_without_stashes_loads() {
        let old = r#"{"base":null,"files":{},"conflicts":[]}"#;
        let st: State = serde_json::from_str(old).unwrap();
        assert!(st.stashes.is_empty());
        assert!(st.merge_in_progress.is_none() && st.pick_in_progress.is_none());
        let out = serde_json::to_string(&st).unwrap();
        assert!(!out.contains("in_progress"), "unset fields are not written");
    }

    #[test]
    fn prefixes() {
        assert_eq!(normalize_prefix("/src/").unwrap(), "src");
        assert_eq!(normalize_prefix("./a/b").unwrap(), "a/b");
        assert_eq!(normalize_prefix(".").unwrap(), "");
        assert!(normalize_prefix("a/../b").is_err());
    }
}
