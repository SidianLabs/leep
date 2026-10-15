//! Working copy layout: `.leep/config.json` (user settings) and
//! `.leep/state.json` (what is materialized on disk).

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use leep_core::{ChangesetId, FileKind, Hash, RepoPath};
use serde::{Deserialize, Serialize};

use crate::error::{CliError, Result};

pub const DIR: &str = ".leep";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Config {
    pub server: String,
    #[serde(default = "default_ref")]
    pub target_ref: String,
    /// Sparse-checkout roots; empty means the whole repository.
    #[serde(default)]
    pub prefixes: Vec<String>,
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

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct State {
    pub base: Option<ChangesetId>,
    pub files: BTreeMap<String, FileState>,
    /// Paths where `leep land` wrote conflict markers.
    #[serde(default)]
    pub conflicts: Vec<String>,
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
    let tmp = path.with_extension(format!("tmp-{}", std::process::id()));
    fs::write(&tmp, bytes)?;
    fs::rename(&tmp, path).inspect_err(|_| {
        let _ = fs::remove_file(&tmp);
    })?;
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
        atomic_write(&self.root.join(DIR).join("config.json"), &b)
    }

    pub fn save_state(&self) -> Result<()> {
        let b = serde_json::to_vec(&self.state)?;
        atomic_write(&self.root.join(DIR).join("state.json"), &b)
    }

    pub fn abs(&self, path: &str) -> PathBuf {
        self.root.join(path)
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
        wc.save_state().unwrap();
        let back = WorkCopy::open(dir.path()).unwrap();
        assert_eq!(back.state, wc.state);
        assert_eq!(back.config, wc.config);
        assert!(WorkCopy::init(dir.path(), wc.config.clone()).is_err());
    }

    #[test]
    fn prefixes() {
        assert_eq!(normalize_prefix("/src/").unwrap(), "src");
        assert_eq!(normalize_prefix("./a/b").unwrap(), "a/b");
        assert_eq!(normalize_prefix(".").unwrap(), "");
        assert!(normalize_prefix("a/../b").is_err());
    }
}
