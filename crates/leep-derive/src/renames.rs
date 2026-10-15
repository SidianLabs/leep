//! Rename detection over a diff (ADR 0016). The pairing is the pure
//! algorithm in `leep_core::rename`; this reads the blobs it asks for.

use std::collections::HashMap;

use futures_util::{stream, StreamExt};
use leep_core::api::DiffEntry;
use leep_core::rename::{split_entries, Detected, Fingerprint, RenameDetector, RenameOptions};
use leep_core::{Error, Hash, Result};

use crate::Repo;

/// Blobs read at once. Only fingerprints are kept, so memory stays small.
const CONCURRENCY: usize = 16;

impl Repo {
    /// Pair the deleted and added files of `entries`. Exact renames cost no
    /// blob reads; similarity reads each candidate blob once.
    pub async fn detect_renames(
        &self,
        entries: &[DiffEntry],
        opts: &RenameOptions,
    ) -> Result<Detected> {
        let (deleted, added) = split_entries(entries);
        let det = RenameDetector::new(deleted, added, *opts);
        let mut fingerprints = HashMap::new();
        let mut rows = stream::iter(det.wanted())
            .map(|h| async move { Ok::<_, Error>((h, self.fingerprint(h).await?)) })
            .buffered(CONCURRENCY);
        while let Some(row) = rows.next().await {
            if let (h, Some(fp)) = row? {
                fingerprints.insert(h, fp);
            }
        }
        drop(rows);
        Ok(det.finish(&fingerprints))
    }

    async fn fingerprint(&self, h: Hash) -> Result<Option<Fingerprint>> {
        let data = self
            .blobs()
            .get(&h)
            .await?
            .ok_or_else(|| Error::Storage(format!("blob {h} missing")))?;
        Ok(Fingerprint::of(&data))
    }
}
