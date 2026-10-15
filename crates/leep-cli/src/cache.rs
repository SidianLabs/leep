//! Machine-wide content-addressed blob cache. Keys are content hashes, so it
//! needs no invalidation and can be shared by every working copy.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use leep_core::Hash;

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
            )));
        }
        let path = self.path(h);
        if path.exists() {
            return Ok(());
        }
        let shard = path.parent().expect("cache path has a parent");
        fs::create_dir_all(shard)?;
        let tmp = shard.join(format!(
            ".tmp-{}-{}",
            std::process::id(),
            TMP_COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        fs::write(&tmp, data)?;
        fs::rename(&tmp, &path).inspect_err(|_| {
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
