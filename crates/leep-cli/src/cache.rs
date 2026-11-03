//! Machine-wide content-addressed blob cache. Keys are content hashes, so it
//! needs no invalidation and can be shared by every working copy.
//!
//! Changesets are kept beside the blobs, under `changesets/`, keyed by id
//! (ADR 0082): what `GET /v1/changesets/{id}` answers is a function of the
//! id, so `leep show` of a changeset seen before makes no request for it.
//! Only display paths read them; nothing takes a cached changeset as proof
//! that a server has it.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use leep_core::api::ChangesetInfo;
use leep_core::{ChangesetId, Hash};

use crate::client::Client;
use crate::error::{CliError, Result};

static TMP_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Debug)]
pub struct Cache {
    dir: PathBuf,
}

impl Cache {
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Cache { dir: dir.into() }
    }

    /// `$LEEP_CACHE_DIR`, else `$XDG_CACHE_HOME/leep/objects`, else
    /// `~/.cache/leep/objects`.
    pub fn from_env() -> Result<Self> {
        let var = |k: &str| std::env::var_os(k).filter(|v| !v.is_empty());
        let dir = if let Some(d) = var("LEEP_CACHE_DIR") {
            PathBuf::from(d)
        } else if let Some(x) = var("XDG_CACHE_HOME") {
            PathBuf::from(x).join("leep/objects")
        } else if let Some(h) = var("HOME") {
            PathBuf::from(h).join(".cache/leep/objects")
        } else {
            return Err(CliError::user(
                "cannot locate cache directory; set LEEP_CACHE_DIR",
            ));
        };
        Ok(Cache::new(dir))
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    fn path(&self, h: &Hash) -> PathBuf {
        let hex = h.to_hex();
        self.dir.join(&hex[..2]).join(&hex[2..])
    }

    /// Cached bytes, re-verified; a corrupt entry is dropped and reads as a miss.
    pub fn get(&self, h: &Hash) -> Option<Vec<u8>> {
        let p = self.path(h);
        let data = fs::read(&p).ok()?;
        if Hash::of_blob(&data) == *h {
            Some(data)
        } else {
            let _ = fs::remove_file(&p);
            None
        }
    }

    /// Store `data` under `h` after checking it hashes to `h`. Atomic.
    pub fn put(&self, h: &Hash, data: &[u8]) -> Result<()> {
        if Hash::of_blob(data) != *h {
            return Err(CliError::server(format!(
                "blob content does not match hash {h}; refusing to cache"
            ))
            .retryable(false));
        }
        let path = self.path(h);
        if path.exists() {
            return Ok(());
        }
        self.write_atomic(&path, data)
    }

    fn changeset_path(&self, id: &ChangesetId) -> PathBuf {
        let hex = id.to_string();
        self.dir.join("changesets").join(&hex[..2]).join(&hex[2..])
    }

    /// A changeset `GET /v1/changesets/{id}` answered before. The entry
    /// must decode and its changeset hash to `id`; anything else is dropped
    /// and reads as a miss.
    pub fn get_changeset(&self, id: &ChangesetId) -> Option<ChangesetInfo> {
        let p = self.changeset_path(id);
        let data = fs::read(&p).ok()?;
        match serde_json::from_slice::<ChangesetInfo>(&data) {
            Ok(info) if info.id == *id && info.changeset.id() == *id => Some(info),
            _ => {
                let _ = fs::remove_file(&p);
                None
            }
        }
    }

    /// Keep `info` for [`Cache::get_changeset`] if its changeset hashes to
    /// its id and it is a plain answer (no `follow` fields). Best effort: a
    /// cache that cannot be written only costs a request later.
    pub fn put_changeset(&self, info: &ChangesetInfo) {
        if info.changeset.id() != info.id || info.path_at.is_some() || info.renamed.is_some() {
            return;
        }
        let path = self.changeset_path(&info.id);
        if path.exists() {
            return;
        }
        let Ok(data) = serde_json::to_vec(info) else {
            return;
        };
        let _ = self.write_atomic(&path, &data);
    }

    /// [`Cache::get_changeset`], asking the server on a miss.
    pub fn changeset(&self, client: &Client, id: &ChangesetId) -> Result<ChangesetInfo> {
        if let Some(info) = self.get_changeset(id) {
            return Ok(info);
        }
        let info = client.get_changeset(id)?;
        self.put_changeset(&info);
        Ok(info)
    }

    fn write_atomic(&self, path: &Path, data: &[u8]) -> Result<()> {
        let shard = path.parent().expect("cache path has a parent");
        fs::create_dir_all(shard)?;
        let tmp = shard.join(format!(
            ".tmp-{}-{}",
            std::process::id(),
            TMP_COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        fs::write(&tmp, data)?;
        fs::rename(&tmp, path).inspect_err(|_| {
            let _ = fs::remove_file(&tmp);
        })?;
        Ok(())
    }

    /// Cached bytes, downloading (and verifying) on a miss.
    pub fn fetch(&self, client: &Client, h: &Hash) -> Result<Vec<u8>> {
        if let Some(d) = self.get(h) {
            return Ok(d);
        }
        let data = client.get_blob(h)?;
        self.put(h, &data)?;
        Ok(data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_get_and_layout() {
        let dir = tempfile::tempdir().unwrap();
        let c = Cache::new(dir.path());
        let h = Hash::of_blob(b"hello");
        assert!(c.get(&h).is_none());
        c.put(&h, b"hello").unwrap();
        assert_eq!(c.get(&h).unwrap(), b"hello");
        let hex = h.to_hex();
        assert!(dir.path().join(&hex[..2]).join(&hex[2..]).is_file());
    }

    #[test]
    fn put_rejects_mismatched_content() {
        let dir = tempfile::tempdir().unwrap();
        let c = Cache::new(dir.path());
        let h = Hash::of_blob(b"hello");
        assert!(c.put(&h, b"evil").is_err());
        assert!(c.get(&h).is_none());
    }

    fn info(message: &str) -> ChangesetInfo {
        let cs = leep_core::Changeset {
            parents: Vec::new(),
            author: "a".into(),
            timestamp: 0,
            message: message.into(),
            changes: Default::default(),
            extra: Default::default(),
        };
        ChangesetInfo {
            id: cs.id(),
            changeset: cs,
            generation: 1,
            root_tree: Hash::of_blob(b"root"),
            path_at: None,
            renamed: None,
            created_by: None,
        }
    }

    #[test]
    fn changesets_are_kept_by_id_and_checked_on_read() {
        let dir = tempfile::tempdir().unwrap();
        let c = Cache::new(dir.path());
        let a = info("a");
        assert!(c.get_changeset(&a.id).is_none());
        c.put_changeset(&a);
        assert_eq!(c.get_changeset(&a.id), Some(a.clone()));
        // An entry whose changeset does not hash to its key is dropped.
        let b = info("b");
        fs::create_dir_all(c.changeset_path(&b.id).parent().unwrap()).unwrap();
        fs::write(c.changeset_path(&b.id), serde_json::to_vec(&a).unwrap()).unwrap();
        assert!(c.get_changeset(&b.id).is_none());
        assert!(!c.changeset_path(&b.id).exists());
        // An answer whose id is not its changeset's is not kept.
        let mut forged = info("c");
        forged.id = b.id;
        c.put_changeset(&forged);
        assert!(!c.changeset_path(&b.id).exists());
    }

    #[test]
    fn corrupt_entry_is_dropped() {
        let dir = tempfile::tempdir().unwrap();
        let c = Cache::new(dir.path());
        let h = Hash::of_blob(b"hello");
        c.put(&h, b"hello").unwrap();
        fs::write(c.path(&h), b"garbage").unwrap();
        assert!(c.get(&h).is_none());
        assert!(!c.path(&h).exists());
    }
}
