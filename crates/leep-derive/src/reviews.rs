//! Storage of review attestations (ADR 0011).
//!
//! Like signatures, a review is a derived-data row beside the changeset, not a
//! changeset field. The row key hashes the changeset id and the reviewer's
//! name, so each reviewer owns one row: a second verdict overwrites the first
//! (a reviewer can change their mind) and two reviewers never race. The server
//! knows its principals, so it lists a changeset's reviews with one lookup per
//! principal.

use bytes::Bytes;
use leep_core::review::{check_comment, Review, REVIEW_KIND};
use leep_core::{ChangesetId, Hash, Result};

use crate::Repo;

pub(crate) fn storage_key(id: ChangesetId, reviewer: &str) -> Hash {
    let mut bytes = Vec::with_capacity(32 + reviewer.len());
    bytes.extend_from_slice(id.0.as_bytes());
    bytes.extend_from_slice(reviewer.as_bytes());
    Hash::of_object("review", &bytes)
}

impl Repo {
    /// Store `review` for `id`, replacing the reviewer's earlier verdict.
    pub async fn put_review(&self, id: ChangesetId, review: &Review) -> Result<()> {
        self.rescue_changesets(&[id], crate::changeset_gc::RescueBy::Attest)
            .await?;
        self.get_changeset(id).await?;
        check_comment(&review.comment)?;
        self.meta()
            .put_derived(
                REVIEW_KIND,
                &storage_key(id, &review.reviewer),
                Bytes::from(review.encode()),
            )
            .await
    }

    /// `reviewer`'s current verdict on `id`, if any.
    pub async fn get_review(&self, id: ChangesetId, reviewer: &str) -> Result<Option<Review>> {
        match self
            .meta()
            .get_derived(REVIEW_KIND, &storage_key(id, reviewer))
            .await?
        {
            Some(b) => Ok(Some(Review::decode(&b)?)),
            None => Ok(None),
        }
    }
}
