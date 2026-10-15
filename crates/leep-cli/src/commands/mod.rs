//! One module per command family. Commands return the process exit code.

pub mod branch;
pub mod checkout;
pub mod commit;
pub mod diff;
pub mod history;
pub mod init;
pub mod keygen;
pub mod land;
pub mod reflog;
pub mod review;
pub mod search;
pub mod status;

use leep_core::ChangesetId;
use serde::Serialize;

use crate::cache::Cache;
use crate::client::Client;
use crate::error::{CliError, Result};
use crate::state::WorkCopy;

/// Everything a working-copy command needs.
pub struct Ctx {
    pub wc: WorkCopy,
    pub client: Client,
    pub cache: Cache,
    pub json: bool,
}

impl Ctx {
    pub fn require_clean(&self) -> Result<()> {
        let st = crate::status::status(&self.wc)?;
        if st.is_clean() {
            Ok(())
        } else {
            Err(CliError::user(
                "working copy has uncommitted changes (commit them, or use --force where supported)",
            ))
        }
    }

    pub fn remote(&self) -> Remote<'_> {
        Remote {
            client: &self.client,
            cache: &self.cache,
            base: self.wc.state.base,
            target_ref: &self.wc.config.target_ref,
            json: self.json,
        }
    }
}

/// What read-only server queries need; works without a working copy.
pub struct Remote<'a> {
    pub client: &'a Client,
    pub cache: &'a Cache,
    pub base: Option<ChangesetId>,
    pub target_ref: &'a str,
    pub json: bool,
}

impl Remote<'_> {
    /// `@` = working copy base; 64-hex = id; otherwise a ref name.
    pub fn resolve(&self, rev: &str) -> Result<ChangesetId> {
        if rev == "@" {
            return self
                .base
                .ok_or_else(|| CliError::user("working copy has no base changeset yet"));
        }
        if rev.len() == 64 {
            if let Ok(id) = rev.parse::<ChangesetId>() {
                return Ok(id);
            }
        }
        self.client.resolve(rev)
    }

    /// Default revision: the working copy base, else the target ref.
    pub fn resolve_default(&self, rev: Option<&str>) -> Result<ChangesetId> {
        match rev {
            Some(r) => self.resolve(r),
            None => match self.base {
                Some(b) => Ok(b),
                None => self.resolve(self.target_ref),
            },
        }
    }
}

pub fn print_json<T: Serialize>(v: &T) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(v)?);
    Ok(())
}
