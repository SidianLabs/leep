//! Storage of signature attestations and the land policy hook (ADR 0010).
//!
//! Signatures are not changeset fields (that would change the changeset id).
//! Each one is a derived-data row keyed by `(changeset id, key id)`, so adding
//! a signature never reads then writes: two signers racing touch different
//! rows. The server knows its trusted keys, so it lists a changeset's
//! signatures by looking up one row per trusted key.

use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use leep_core::signature::{Signature, SIGNATURE_KIND};
use leep_core::{ChangesetId, DerivedRow, Error, Hash, Result};

use crate::Repo;

/// Derived-data kind: key = rebased copy id, value = the 32-byte id of the
/// changeset it was rebased from. Written only by [`Repo::rebase`], so unlike
/// the `leep.rebased_from` extra key a client cannot forge it.
pub const REBASED_ORIGIN_KIND: &str = "rebased-origin-v1";

/// Longest rebase chain followed when looking for an original.
const MAX_REBASE_CHAIN: usize = 64;

/// Veto point for `land`, so policy lives outside this backend-agnostic crate.
#[async_trait]
pub trait LandPolicy: Send + Sync + 'static {
    /// Whether `target_ref` is subject to this policy; lets `land` skip
    /// walking history for refs that are not.
    fn applies_to(&self, target_ref: &str) -> bool;

    /// Whether a decision can change when the ref's head moves. `land` reuses
    /// a passed decision across CAS retries only for policies that say no;
    /// signatures never go away, but owners come from the head's tree.
    fn depends_on_head(&self) -> bool {
        false
    }

    /// Whether `target_ref` is protected for its own sake (`--protect-ref`),
    /// not only as the ref a check covers. `rebase-onto` refuses such refs:
    /// it rewrites the ref, which protection exists to forbid (ADR 0062).
    fn protects(&self, target_ref: &str) -> bool {
        let _ = target_ref;
        false
    }

    /// Called before `land` moves `target_ref`, which is at `head`.
    /// `new_history` lists the changesets the ref would gain that the caller
    /// submitted (never the server-made rebased copies), nearest to the tip
    /// first. Return an error (normally `Error::Invalid`) to refuse the land.
    async fn check_land(
        &self,
        repo: &Repo,
        target_ref: &str,
        head: Option<ChangesetId>,
        new_history: &[ChangesetId],
    ) -> Result<()>;
}

/// Several policies that must all pass, in order. `Repo::with_land_policy`
/// builds one when called twice.
pub struct PolicySet(Vec<Arc<dyn LandPolicy>>);

impl PolicySet {
    pub fn new(policies: Vec<Arc<dyn LandPolicy>>) -> Self {
        PolicySet(policies)
    }
}

#[async_trait]
impl LandPolicy for PolicySet {
    fn applies_to(&self, target_ref: &str) -> bool {
        self.0.iter().any(|p| p.applies_to(target_ref))
    }

    fn depends_on_head(&self) -> bool {
        self.0.iter().any(|p| p.depends_on_head())
    }

    fn protects(&self, target_ref: &str) -> bool {
        self.0.iter().any(|p| p.protects(target_ref))
    }

    async fn check_land(
        &self,
        repo: &Repo,
        target_ref: &str,
        head: Option<ChangesetId>,
        new_history: &[ChangesetId],
    ) -> Result<()> {
        for p in self.0.iter().filter(|p| p.applies_to(target_ref)) {
            p.check_land(repo, target_ref, head, new_history).await?;
        }
        Ok(())
    }
}

pub(crate) fn storage_key(id: ChangesetId, key_id: &str) -> Hash {
    let mut bytes = Vec::with_capacity(32 + key_id.len());
    bytes.extend_from_slice(id.0.as_bytes());
    bytes.extend_from_slice(key_id.as_bytes());
    Hash::of_object("signature", &bytes)
}

impl Repo {
    /// Store `sig` for `id`. Idempotent. Does not verify: the caller (the
    /// server) holds the keys.
    pub async fn put_signature(&self, id: ChangesetId, sig: &Signature) -> Result<()> {
        self.rescue_changesets(&[id], crate::changeset_gc::RescueBy::Attest)
            .await?;
        self.get_changeset(id).await?;
        self.meta()
            .put_derived(
                SIGNATURE_KIND,
                &storage_key(id, &sig.key_id),
                Bytes::from(sig.encode()),
            )
            .await
    }

    /// The signature `key_id` made over `id`, if any.
    pub async fn get_signature(&self, id: ChangesetId, key_id: &str) -> Result<Option<Signature>> {
        match self
            .meta()
            .get_derived(SIGNATURE_KIND, &storage_key(id, key_id))
            .await?
        {
            Some(b) => Ok(Some(Signature::decode(&b)?)),
            None => Ok(None),
        }
    }

    /// The changeset the server rebased to produce `id`, if it did.
    pub async fn rebased_origin(&self, id: ChangesetId) -> Result<Option<ChangesetId>> {
        match self.meta().get_derived(REBASED_ORIGIN_KIND, &id.0).await? {
            Some(b) => {
                let arr: [u8; 32] = b
                    .as_ref()
                    .try_into()
                    .map_err(|_| Error::Encoding("bad rebased-origin value".into()))?;
                Ok(Some(ChangesetId(Hash(arr))))
            }
            None => Ok(None),
        }
    }

    /// Rebase ancestors of `id`, nearest first, bounded and cycle-safe.
    pub async fn rebase_chain(&self, id: ChangesetId) -> Result<Vec<ChangesetId>> {
        let mut out = Vec::new();
        let mut seen = HashSet::from([id]);
        let mut cur = id;
        while out.len() < MAX_REBASE_CHAIN {
            match self.rebased_origin(cur).await? {
                Some(o) if seen.insert(o) => {
                    out.push(o);
                    cur = o;
                }
                _ => break,
            }
        }
        Ok(out)
    }

    /// The row recording that the server made `copy` by rebasing `original`.
    pub(crate) fn rebase_link(copy: ChangesetId, original: ChangesetId) -> DerivedRow {
        DerivedRow::new(
            REBASED_ORIGIN_KIND,
            copy.0,
            Bytes::copy_from_slice(original.0.as_bytes()),
        )
    }

    /// Changesets landing `cs` onto a ref at `head` would add to it: every
    /// ancestor of `cs` (itself included) that is not in `head`'s history,
    /// found iteratively, nearest to the tip first. With no head that is all
    /// of `cs`'s history.
    pub(crate) async fn new_history(
        &self,
        cs: ChangesetId,
        head: Option<ChangesetId>,
    ) -> Result<Vec<ChangesetId>> {
        let mut out = Vec::new();
        let mut seen = HashSet::from([cs]);
        let mut queue = std::collections::VecDeque::from([cs]);
        while let Some(id) = queue.pop_front() {
            if let Some(h) = head {
                if id == h || self.is_ancestor(id, h).await? {
                    continue;
                }
            }
            out.push(id);
            for p in self.get_changeset(id).await?.parents {
                if seen.insert(p) {
                    queue.push_back(p);
                }
            }
        }
        Ok(out)
    }
}
