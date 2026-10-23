//! `--protect-ref`: branch protection on its own (ADR 0015).
//!
//! The forward-only and no-delete rules of `cas_ref_checked` apply to every
//! ref a land policy covers, so protection was a side effect of requiring
//! signatures or review. A policy that covers refs and checks nothing gives
//! a ref those rules without either requirement: any writer can still land on
//! it, but nobody can rewind or delete it through the API.

use std::collections::HashSet;

use async_trait::async_trait;
use leep_core::{ChangesetId, Result};
use leep_derive::{LandPolicy, Repo};

/// Covers `refs` and accepts every land onto them.
pub struct ProtectOnly {
    refs: HashSet<String>,
}

impl ProtectOnly {
    pub fn new(refs: impl IntoIterator<Item = String>) -> Self {
        ProtectOnly {
            refs: refs.into_iter().collect(),
        }
    }
}

#[async_trait]
impl LandPolicy for ProtectOnly {
    fn applies_to(&self, target_ref: &str) -> bool {
        self.refs.contains(target_ref)
    }

    fn protects(&self, target_ref: &str) -> bool {
        self.applies_to(target_ref)
    }

    async fn check_land(
        &self,
        _repo: &Repo,
        _target_ref: &str,
        _head: Option<ChangesetId>,
        _new_history: &[ChangesetId],
    ) -> Result<()> {
        Ok(())
    }
}
