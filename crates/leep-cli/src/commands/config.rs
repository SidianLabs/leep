//! `leep config`: Leep's own settings, kept in the working copy's
//! `.leep/config.json` or, with `--global`, in the user config file. Only
//! `author` exists so far; it needs no server.

use clap::builder::PossibleValuesParser;
use clap::Args;
use leep_core::outln;
use serde::Serialize;

use super::print_json;
use crate::error::{CliError, Result};
use crate::identity::{self, user_config_path, Source, UserConfig};
use crate::state::WorkCopy;

/// The settings `leep config` knows.
pub const KEYS: &[&str] = &["author"];

#[derive(Args, Debug)]
pub struct ConfigArgs {
    /// Setting to read or change (supported: author).
    #[arg(value_parser = PossibleValuesParser::new(KEYS))]
    pub key: String,
    /// New value. Without one, print the value in effect and where it comes
    /// from.
    pub value: Option<String>,
    /// Use the user config file instead of this working copy's
    /// `.leep/config.json`.
    #[arg(long)]
    pub global: bool,
    /// Remove the setting.
    #[arg(long, conflicts_with = "value")]
    pub unset: bool,
}

#[derive(Serialize)]
struct Out {
    author: Option<String>,
    source: Option<Source>,
}

fn user_path() -> Result<std::path::PathBuf> {
    user_config_path().ok_or_else(|| {
        CliError::user("cannot locate the user config file; set XDG_CONFIG_HOME or HOME")
    })
}

fn here() -> Result<WorkCopy> {
    WorkCopy::discover(&std::env::current_dir()?)
}

pub fn run(args: &ConfigArgs, json: bool) -> Result<i32> {
    debug_assert_eq!(args.key, "author");
    if args.unset {
        return write(None, args.global, json);
    }
    if let Some(v) = &args.value {
        let v = v.trim();
        if v.is_empty() {
            return Err(CliError::user(
                "the author is empty; use `leep config --unset author` to remove it",
            ));
        }
        if v.contains(char::is_control) {
            return Err(CliError::user(
                "the author must be one line without control characters",
            ));
        }
        return write(Some(v.to_string()), args.global, json);
    }
    let out = if args.global {
        let path = user_path()?;
        let author = UserConfig::load(&path)?.author;
        Out {
            source: author.as_ref().map(|_| Source::User),
            author,
        }
    } else {
        let wc = here().ok();
        match identity::resolve(None, wc.as_ref())? {
            Some((a, s)) => Out {
                author: Some(a),
                source: Some(s),
            },
            None => Out {
                author: None,
                source: Some(Source::Server),
            },
        }
    };
    if json {
        print_json(&out)?;
    } else {
        match (&out.author, out.source) {
            (Some(a), Some(s)) => {
                outln!("{a}");
                eprintln!("(from {})", s.describe());
            }
            (None, Some(Source::Server)) => eprintln!(
                "no author set locally; commits use {}",
                Source::Server.describe()
            ),
            _ => eprintln!("no author set in {}", Source::User.describe()),
        }
    }
    Ok(0)
}

/// Set or (with `None`) remove the author at one level.
fn write(author: Option<String>, global: bool, json: bool) -> Result<i32> {
    let level = if global {
        let path = user_path()?;
        let mut c = UserConfig::load(&path)?;
        // Removing from a file that does not exist creates nothing.
        if author.is_some() || path.exists() {
            c.author = author.clone();
            c.save(&path)?;
        }
        Source::User
    } else {
        let mut wc = here()?;
        wc.config.author = author.clone();
        wc.save_config()?;
        Source::Repo
    };
    if json {
        print_json(&Out {
            author: author.clone(),
            source: Some(level),
        })?;
    } else {
        match &author {
            Some(a) => eprintln!("author set to {a:?} in {}", level.describe()),
            None => eprintln!("author removed from {}", level.describe()),
        }
    }
    // Say so when a value above this level hides the change.
    if author.is_some() {
        if let Ok(Some((a, s))) = identity::resolve(None, here().ok().as_ref()) {
            if s < level {
                eprintln!(
                    "note: {} sets the author to {a:?}, which wins",
                    s.describe()
                );
            }
        }
    }
    Ok(0)
}
