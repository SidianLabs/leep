//! One module per command family. Commands return the process exit code.

pub mod amend;
pub mod branch;
pub mod checkout;
pub mod commit;
pub mod diff;
pub mod history;
pub mod init;
pub mod integrate;
pub mod keygen;
pub mod land;
pub mod merge;
pub mod pick;
pub mod rebase;
pub mod reflog;
pub mod reset;
pub mod resolve;
pub mod restore;
pub mod review;
pub mod search;
pub mod stash;
pub mod status;

use leep_core::outln;
use leep_core::prefix::looks_like_prefix;
use leep_core::ChangesetId;
use serde::Serialize;

use crate::cache::Cache;
use crate::client::Client;
use crate::error::{CliError, ErrorKind, Result};
use crate::state::WorkCopy;

/// Everything a working-copy command needs.
pub struct Ctx {
    pub wc: WorkCopy,
    pub client: Client,
    pub cache: Cache,
    pub json: bool,
}

impl Ctx {
    /// Refuse to start something new while a merge, cherry-pick or revert
    /// waits for its commit. Finishing or aborting it is the way out.
    pub fn refuse_in_progress(&self) -> Result<()> {
        self.refuse_interrupted()?;
        let state = &self.wc.state;
        if let Some(m) = &state.merge_in_progress {
            return Err(CliError::conflict(format!(
                "a merge of {} is in progress; resolve the conflicts and `leep commit`, \
                 or `leep merge --abort`",
                crate::fmt::short(m.other)
            )));
        }
        if let Some(p) = &state.pick_in_progress {
            let (what, abort) = if p.revert {
                ("revert", "leep revert --abort")
            } else {
                ("cherry-pick", "leep cherry-pick --abort")
            };
            return Err(CliError::conflict(format!(
                "a {what} of {} is in progress; resolve the conflicts and `leep commit`, \
                 or `{abort}`",
                crate::fmt::short(p.from)
            )));
        }
        Ok(())
    }

    /// Refuse while a checkout was cut off part-way: the files are a mix of
    /// two changesets, so status and commit would misreport them.
    pub fn refuse_interrupted(&self) -> Result<()> {
        match self.wc.state.materializing {
            Some(id) => Err(CliError::user(format!(
                "a checkout of {} was interrupted and the working copy is part-way \
                 there; run `leep checkout {id}` to finish it (`--force` to discard \
                 changes made since)",
                crate::fmt::short(id)
            ))),
            None => Ok(()),
        }
    }

    pub fn require_clean(&self) -> Result<()> {
        self.refuse_in_progress()?;
        let st = crate::status::status(&self.wc)?;
        st.refuse_conflicts()?;
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
    /// `@` = working copy base; 64-hex = id; otherwise a ref or tag name,
    /// then a short id of 4 or more hex digits (ADR 0035). A ref wins over a
    /// short id that spells the same.
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
        let missing = match self.client.resolve(rev) {
            Err(e) if e.kind() == ErrorKind::NotFound => e.with_hint(
                "`leep refs` lists branches and tags, `leep log` shows changeset ids; \
                 a short id needs at least 4 hex digits",
            ),
            other => return other,
        };
        if !looks_like_prefix(rev) {
            return Err(missing);
        }
        match self.client.resolve_prefix(rev) {
            Err(e) if e.kind() == ErrorKind::NotFound => Err(missing),
            other => other,
        }
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
    outln!("{}", serde_json::to_string_pretty(v)?);
    Ok(())
}
