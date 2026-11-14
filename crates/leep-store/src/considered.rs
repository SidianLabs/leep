//! Packs the automatic delta repack has already compressed (ADR 0100). A
//! pack with no delta record was written whole (a land batch, an import)
//! and is worth compressing against its previous versions, unless a delta
//! repack wrote it so (a history with nothing to delta against) or merged
//! it from such packs: those are listed here, so no process, after any
//! restart, compresses them again.
//!
//! Advisory: losing the record costs one more pass over those packs, and an
//! older binary ignores it. A filesystem store keeps `packs/considered`,
//! one pack id per line; a bucket `packs/.considered`, and the time of its
//! last automatic full repack in `packs/.last-full` (unix seconds). Every
//! write is a whole-file replace under the repack lock (or lease) and keeps
//! only ids still published, so the record never grows past the pack count.

use std::collections::HashSet;
use std::io::ErrorKind;
use std::path::Path;

use leep_core::Result;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};

use crate::packset::{published_ids, sync_dir};
use crate::repack::write_durably;
use crate::storage_err;

/// `packs/considered` in a filesystem store.
pub(crate) const CONSIDERED: &str = "considered";
/// The bucket's record and the time of its last automatic full repack.
pub(crate) const CONSIDERED_KEY: &str = "packs/.considered";
pub(crate) const LAST_FULL_KEY: &str = "packs/.last-full";

fn is_pack_id(s: &str) -> bool {
    s.len() == 64 && s.bytes().all(|c| c.is_ascii_hexdigit())
}

/// The ids in a record; malformed or torn lines are ignored.
pub(crate) fn parse(text: &[u8]) -> HashSet<String> {
    let complete = match text.iter().rposition(|&b| b == b'\n') {
        Some(i) => &text[..=i],
        None => return HashSet::new(),
    };
    complete
        .split(|&b| b == b'\n')
        .filter_map(|l| std::str::from_utf8(l).ok())
        .filter(|l| is_pack_id(l))
        .map(|l| l.to_ascii_lowercase())
        .collect()
}

fn encode(ids: &HashSet<String>) -> String {
    let mut v: Vec<&String> = ids.iter().collect();
    v.sort();
    v.into_iter().map(|id| format!("{id}\n")).collect()
}

/// The record of the filesystem store whose `packs/` is `dir`.
pub(crate) fn read_fs(dir: &Path) -> Result<HashSet<String>> {
    match std::fs::read(dir.join(CONSIDERED)) {
        Ok(b) => Ok(parse(&b)),
        Err(e) if e.kind() == ErrorKind::NotFound => Ok(HashSet::new()),
        Err(e) => Err(storage_err(e)),
    }
}

/// Add `ids` to the record and drop ids no longer published. Only under
/// the repack lock.
pub(crate) fn mark_fs(dir: &Path, ids: &[String]) -> Result<()> {
    let published: HashSet<String> = published_ids(dir)?.into_iter().collect();
    let mut set = read_fs(dir)?;
    set.extend(ids.iter().cloned());
    set.retain(|id| published.contains(id));
    let tmp = write_durably(dir, "considered", encode(&set).as_bytes())?;
    std::fs::rename(&tmp, dir.join(CONSIDERED)).map_err(storage_err)?;
    sync_dir(dir)
}

async fn get_text(store: &dyn ObjectStore, key: &str) -> Result<Option<bytes::Bytes>> {
    match store.get(&object_store::path::Path::from(key)).await {
        Ok(r) => match r.bytes().await {
            Ok(b) => Ok(Some(b)),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(storage_err(e)),
        },
        Err(object_store::Error::NotFound { .. }) => Ok(None),
        Err(e) => Err(storage_err(e)),
    }
}

/// The bucket's record.
pub(crate) async fn read_object(store: &dyn ObjectStore) -> Result<HashSet<String>> {
    Ok(get_text(store, CONSIDERED_KEY)
        .await?
        .map(|b| parse(&b))
        .unwrap_or_default())
}

/// Add `ids`, keeping only ids in `published`. Only under the lease.
pub(crate) async fn mark_object(
    store: &dyn ObjectStore,
    ids: &[String],
    published: &HashSet<String>,
) -> Result<()> {
    let mut set = read_object(store).await?;
    set.extend(ids.iter().cloned());
    set.retain(|id| published.contains(id));
    store
        .put(
            &object_store::path::Path::from(CONSIDERED_KEY),
            PutPayload::from(encode(&set).into_bytes()),
        )
        .await
        .map_err(storage_err)?;
    Ok(())
}

/// When the bucket last had an automatic full repack, in unix seconds.
pub(crate) async fn read_last_full(store: &dyn ObjectStore) -> Result<Option<i64>> {
    Ok(get_text(store, LAST_FULL_KEY)
        .await?
        .and_then(|b| std::str::from_utf8(&b).ok()?.trim().parse().ok()))
}

pub(crate) async fn write_last_full(store: &dyn ObjectStore, at: i64) -> Result<()> {
    store
        .put(
            &object_store::path::Path::from(LAST_FULL_KEY),
            PutPayload::from(format!("{at}\n").into_bytes()),
        )
        .await
        .map_err(storage_err)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_parse_and_skip_torn_and_bad_lines() {
        let a = "a".repeat(64);
        let b = "B".repeat(64);
        let text = format!("{a}\nnot an id\n{b}\n{}", "c".repeat(64));
        let set = parse(text.as_bytes());
        assert_eq!(set.len(), 2);
        assert!(set.contains(&a) && set.contains(&"b".repeat(64)));
        assert_eq!(parse(encode(&set).as_bytes()), set);
    }
}
