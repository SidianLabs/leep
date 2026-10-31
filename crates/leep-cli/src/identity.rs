//! Who new changesets are by. Someone arriving from git has already told git
//! their name, so after Leep's own settings the author is taken from
//! `git config`, and only then from the login name.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use serde::{Deserialize, Serialize};

use crate::error::{CliError, Result};
use crate::state::{atomic_write, WorkCopy};

/// Where an author came from, in precedence order.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Source {
    /// `--author`.
    Flag,
    /// `LEEP_AUTHOR`.
    Env,
    /// `author` in `.leep/config.json`.
    Repo,
    /// `author` in the user config file ([`user_config_path`]).
    User,
    /// `git config user.name` and `user.email`.
    Git,
    /// `USER`, `LOGNAME` or `USERNAME`.
    Login,
    /// Nothing local: the server names the token's owner.
    Server,
}

impl Source {
    /// For human output: where the value was found.
    pub fn describe(self) -> String {
        match self {
            Source::Flag => "--author".into(),
            Source::Env => "LEEP_AUTHOR".into(),
            Source::Repo => ".leep/config.json".into(),
            Source::User => user_config_path()
                .map(|p| p.display().to_string())
                .unwrap_or_else(|| "the user config file".into()),
            Source::Git => "git config user.name and user.email".into(),
            Source::Login => "the login name".into(),
            Source::Server => "the server's name for the token".into(),
        }
    }
}

/// The per-user config file. Keys this version does not know are kept when
/// it rewrites the file, so an older CLI never drops a newer one's settings.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct UserConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub author: Option<String>,
    #[serde(flatten)]
    pub other: serde_json::Map<String, serde_json::Value>,
}

fn env_var(k: &str) -> Option<String> {
    std::env::var(k)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

/// `$XDG_CONFIG_HOME/leep/config.json`, else `%APPDATA%\leep\config.json` on
/// Windows, else `~/.config/leep/config.json`.
pub fn user_config_path() -> Option<PathBuf> {
    user_config_path_from(&env_var, cfg!(windows))
}

fn user_config_path_from(env: &dyn Fn(&str) -> Option<String>, windows: bool) -> Option<PathBuf> {
    // The XDG spec says a relative value is to be ignored.
    let base = env("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .filter(|p| p.is_absolute())
        .or_else(|| {
            if windows {
                env("APPDATA").map(PathBuf::from)
            } else {
                env("HOME").map(|h| PathBuf::from(h).join(".config"))
            }
        })?;
    Some(base.join("leep").join("config.json"))
}

impl UserConfig {
    /// The file's contents; a missing file is an empty config.
    pub fn load(path: &Path) -> Result<UserConfig> {
        match fs::read(path) {
            Ok(b) => serde_json::from_slice(&b)
                .map_err(|e| CliError::user(format!("bad {}: {e}", path.display()))),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(UserConfig::default()),
            Err(e) => Err(CliError::user(format!(
                "cannot read {}: {e}",
                path.display()
            ))),
        }
    }

    pub fn save(&self, path: &Path) -> Result<()> {
        if let Some(dir) = path.parent() {
            fs::create_dir_all(dir)?;
        }
        atomic_write(path, &serde_json::to_vec_pretty(self)?)
    }
}

/// `user.name <user.email>` from git, run in `dir` so a repository's own
/// identity wins over the global one. Any failure (no git, no name) is
/// `None`: git is a convenience here, never a requirement.
pub fn git_author(dir: Option<&Path>) -> Option<String> {
    let get = |key: &str| -> Option<String> {
        let mut c = Command::new("git");
        if let Some(d) = dir {
            c.current_dir(d);
        }
        let out = c
            .args(["config", "--get", key])
            .stdin(Stdio::null())
            .stderr(Stdio::null())
            .output()
            .ok()?;
        if !out.status.success() {
            return None;
        }
        let v = String::from_utf8(out.stdout).ok()?.trim().to_string();
        (!v.is_empty()).then_some(v)
    };
    let name = get("user.name")?;
    Some(match get("user.email") {
        Some(email) => format!("{name} <{email}>"),
        None => name,
    })
}

/// The author named without asking the server, and where it came from:
/// `--author`, `LEEP_AUTHOR`, the working copy's config, the user config,
/// git, then the login name. `None` leaves it to the server.
pub fn resolve(flag: Option<&str>, wc: Option<&WorkCopy>) -> Result<Option<(String, Source)>> {
    let root = wc.map(|w| w.root.as_path());
    let mut user_err = None;
    let found = resolve_with(
        flag,
        &env_var,
        wc.and_then(|w| w.config.author.as_deref()),
        &mut || match user_config_path().map(|p| UserConfig::load(&p)) {
            Some(Ok(c)) => c.author,
            Some(Err(e)) => {
                user_err = Some(e);
                None
            }
            None => None,
        },
        &mut || git_author(root),
    );
    match (found, user_err) {
        (_, Some(e)) => Err(e),
        (found, None) => Ok(found),
    }
}

/// [`resolve`] with its inputs passed in. The user config and git are only
/// consulted when everything above them is unset.
fn resolve_with(
    flag: Option<&str>,
    env: &dyn Fn(&str) -> Option<String>,
    repo: Option<&str>,
    user: &mut dyn FnMut() -> Option<String>,
    git: &mut dyn FnMut() -> Option<String>,
) -> Option<(String, Source)> {
    let set = |v: Option<String>| v.map(|v| v.trim().to_string()).filter(|v| !v.is_empty());
    if let Some(f) = flag {
        return Some((f.to_string(), Source::Flag));
    }
    let from = |v: Option<String>, s: Source| set(v).map(|v| (v, s));
    from(env("LEEP_AUTHOR"), Source::Env)
        .or_else(|| from(repo.map(str::to_string), Source::Repo))
        .or_else(|| from(user(), Source::User))
        .or_else(|| from(git(), Source::Git))
        // `USERNAME` is Windows' name for `USER`, and `LOGNAME` is set by
        // login(1) where `USER` may not be.
        .or_else(|| {
            ["USER", "LOGNAME", "USERNAME"]
                .into_iter()
                .find_map(|k| from(env(k), Source::Login))
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn run(
        flag: Option<&str>,
        env: &[(&str, &str)],
        repo: Option<&str>,
        user: Option<&str>,
        git: Option<&str>,
    ) -> Option<(String, Source)> {
        let env: BTreeMap<String, String> = env
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        resolve_with(
            flag,
            &|k| env.get(k).cloned(),
            repo,
            &mut || user.map(str::to_string),
            &mut || git.map(str::to_string),
        )
    }

    #[test]
    fn precedence() {
        let env = [("LEEP_AUTHOR", "env"), ("USER", "login")];
        let all = |flag| run(flag, &env, Some("repo"), Some("user"), Some("git"));
        assert_eq!(all(Some("flag")), Some(("flag".into(), Source::Flag)));
        assert_eq!(all(None), Some(("env".into(), Source::Env)));
        let login = [("USER", "login")];
        assert_eq!(
            run(None, &login, Some("repo"), Some("user"), Some("git")),
            Some(("repo".into(), Source::Repo))
        );
        assert_eq!(
            run(None, &login, None, Some("user"), Some("git")),
            Some(("user".into(), Source::User))
        );
        assert_eq!(
            run(None, &login, None, None, Some("git")),
            Some(("git".into(), Source::Git))
        );
        assert_eq!(
            run(None, &login, None, None, None),
            Some(("login".into(), Source::Login))
        );
        assert_eq!(
            run(
                None,
                &[("LOGNAME", "ln"), ("USERNAME", "un")],
                None,
                None,
                None
            ),
            Some(("ln".into(), Source::Login))
        );
        assert_eq!(run(None, &[], None, None, None), None);
    }

    #[test]
    fn blank_values_fall_through() {
        assert_eq!(
            run(
                None,
                &[("LEEP_AUTHOR", " ")],
                Some(""),
                Some(" \n"),
                Some("g")
            ),
            Some(("g".into(), Source::Git))
        );
    }

    #[test]
    fn user_and_git_are_not_consulted_when_something_above_is_set() {
        let mut asked = 0;
        let found = resolve_with(
            None,
            &|_| None,
            Some("repo"),
            &mut || {
                asked += 1;
                None
            },
            &mut || unreachable!(),
        );
        assert_eq!(found, Some(("repo".into(), Source::Repo)));
        assert_eq!(asked, 0);
    }

    #[test]
    fn user_config_location() {
        let env = |pairs: &'static [(&'static str, &'static str)]| {
            move |k: &str| {
                pairs
                    .iter()
                    .find(|(n, _)| *n == k)
                    .map(|(_, v)| v.to_string())
            }
        };
        let abs = if cfg!(windows) { "C:\\xdg" } else { "/xdg" };
        let xdg: &'static [(&str, &str)] = if cfg!(windows) {
            &[
                ("XDG_CONFIG_HOME", "C:\\xdg"),
                ("HOME", "/h"),
                ("APPDATA", "C:\\ad"),
            ]
        } else {
            &[
                ("XDG_CONFIG_HOME", "/xdg"),
                ("HOME", "/h"),
                ("APPDATA", "C:\\ad"),
            ]
        };
        let want = PathBuf::from(abs).join("leep").join("config.json");
        assert_eq!(user_config_path_from(&env(xdg), false), Some(want.clone()));
        assert_eq!(user_config_path_from(&env(xdg), true), Some(want));
        let rel = &[
            ("XDG_CONFIG_HOME", "rel"),
            ("HOME", "/h"),
            ("APPDATA", "C:\\ad"),
        ];
        assert_eq!(
            user_config_path_from(&env(rel), false),
            Some(PathBuf::from("/h/.config/leep/config.json"))
        );
        assert_eq!(
            user_config_path_from(&env(rel), true),
            Some(PathBuf::from("C:\\ad").join("leep").join("config.json"))
        );
        assert_eq!(user_config_path_from(&env(&[]), false), None);
    }

    #[test]
    fn user_config_keeps_unknown_keys() {
        let d = tempfile::tempdir().unwrap();
        let p = d.path().join("sub/config.json");
        assert!(UserConfig::load(&p).unwrap().author.is_none());
        fs::create_dir_all(p.parent().unwrap()).unwrap();
        fs::write(&p, r#"{"future": {"x": 1}, "author": "a"}"#).unwrap();
        let mut c = UserConfig::load(&p).unwrap();
        assert_eq!(c.author.as_deref(), Some("a"));
        c.author = None;
        c.save(&p).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&fs::read(&p).unwrap()).unwrap();
        assert_eq!(v, serde_json::json!({"future": {"x": 1}}));
        fs::write(&p, "not json").unwrap();
        assert!(UserConfig::load(&p).is_err());
    }
}
