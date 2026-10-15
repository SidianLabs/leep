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
    atomic_write_mode(path, bytes, false)
}

/// Like [`atomic_write`]; `private` creates the file owner-only (0600 on unix)
/// before any bytes are written, so a secret is never briefly world-readable.
pub fn atomic_write_mode(path: &Path, bytes: &[u8], private: bool) -> Result<()> {
    let tmp = path.with_extension(format!("tmp-{}", std::process::id()));
    if private {
        use std::io::Write;
        let mut opts = fs::OpenOptions::new();
        opts.write(true).create(true).truncate(true);
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
        wc.save_state().unwrap();
        let back = WorkCopy::open(dir.path()).unwrap();
        assert_eq!(back.state, wc.state);
        assert_eq!(back.config, wc.config);
        assert!(WorkCopy::init(dir.path(), wc.config.clone()).is_err());
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
    fn prefixes() {
        assert_eq!(normalize_prefix("/src/").unwrap(), "src");
        assert_eq!(normalize_prefix("./a/b").unwrap(), "a/b");
        assert_eq!(normalize_prefix(".").unwrap(), "");
        assert!(normalize_prefix("a/../b").is_err());
    }
}
