//! Deferred directory syncs for the filesystem store (ADR 0044).
//!
//! A loose blob is durable once its file is synced (done by the put) and the
//! rename that published it is synced, which needs an fsync of its shard
//! directory, and of the root when the shard is new. Syncing a directory per
//! put cost one more disk flush per blob; instead a put records its shard
//! here, and [`DirSyncer::flush`] syncs every recorded directory once, before
//! anything that depends on the blobs is acknowledged.
//!
//! Flushes are serialized and coalesce: each mark takes a sequence number,
//! and a flush returns as soon as a sync wave that drained the set after its
//! marks has finished, whoever ran it. While one wave runs, later marks wait
//! for the next, which then covers every flush queued behind the first.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

use leep_core::{Error, Result};

use crate::packset::sync_dir;

/// Blocking tasks one sync wave is split across. A directory fsync mostly
/// waits for the journal and a device flush; a thread per directory cost
/// more in scheduling than it gained, measured with waves of 4 to 128.
const WAVE_TASKS: usize = 4;

/// 256 shards as a bit set.
#[derive(Clone, Copy, Default, PartialEq, Eq)]
struct Shards([u64; 4]);

impl Shards {
    fn insert(&mut self, s: u8) {
        self.0[usize::from(s >> 6)] |= 1 << (s & 63);
    }

    fn contains(&self, s: u8) -> bool {
        self.0[usize::from(s >> 6)] & (1 << (s & 63)) != 0
    }

    fn union(&mut self, o: &Shards) {
        for (a, b) in self.0.iter_mut().zip(o.0) {
            *a |= b;
        }
    }

    fn is_empty(&self) -> bool {
        self.0 == [0; 4]
    }

    fn iter(&self) -> impl Iterator<Item = u8> + '_ {
        (0..=255u8).filter(|s| self.contains(*s))
    }
}

#[derive(Default)]
struct State {
    /// Shard directories with a rename or a found blob not yet synced.
    shards: Shards,
    /// The root needs a sync: some recorded shard's own entry in it is not
    /// known durable.
    root: bool,
    /// Shards whose entry in the root this process has synced.
    durable_shards: Shards,
    /// Marks so far; each mark's number is the count after it.
    marked: u64,
    /// Every mark numbered up to this is durable.
    synced: u64,
}

pub(crate) struct DirSyncer {
    root: PathBuf,
    state: Mutex<State>,
    /// Held by the one flush running a sync wave.
    wave: tokio::sync::Mutex<()>,
    /// Directory fsyncs issued, for tests.
    syncs: AtomicU64,
}

impl DirSyncer {
    pub fn new(root: &Path) -> Self {
        DirSyncer {
            root: root.to_path_buf(),
            state: Mutex::default(),
            wave: tokio::sync::Mutex::new(()),
            syncs: AtomicU64::new(0),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Record that `shard` holds a blob that must become durable at the next
    /// flush: one this process renamed into place, or found there (it may be
    /// another writer's rename that nobody has synced yet). Call after the
    /// rename or the lookup, so a wave that drains this mark syncs after it.
    pub fn mark(&self, shard: u8) {
        let mut s = self.lock();
        s.shards.insert(shard);
        if !s.durable_shards.contains(shard) {
            s.root = true;
        }
        s.marked += 1;
    }

    pub fn syncs(&self) -> u64 {
        self.syncs.load(Ordering::Relaxed)
    }

    /// Directories recorded and not yet synced (the root counts as one).
    pub fn pending(&self) -> usize {
        let s = self.lock();
        s.shards.iter().count() + usize::from(s.root)
    }

    /// Mark `shards`, then return once every mark made before this call
    /// returned is durable.
    pub async fn flush(&self, shards: impl IntoIterator<Item = u8>) -> Result<()> {
        for shard in shards {
            self.mark(shard);
        }
        let mine = self.lock().marked;
        if self.lock().synced >= mine {
            return Ok(());
        }
        let _wave = self.wave.lock().await;
        let (shards, root, upto) = {
            let mut s = self.lock();
            if s.synced >= mine {
                return Ok(());
            }
            let taken = (std::mem::take(&mut s.shards), s.root, s.marked);
            s.root = false;
            taken
        };
        match self.sync_wave(&shards, root).await {
            Ok(()) => {
                let mut s = self.lock();
                s.synced = upto;
                if root {
                    s.durable_shards.union(&shards);
                }
                Ok(())
            }
            Err(e) => {
                // Put the wave back so the next flush tries it again.
                let mut s = self.lock();
                s.shards.union(&shards);
                s.root |= root;
                Err(e)
            }
        }
    }

    /// Sync the shard directories and, if asked, the root, in up to
    /// [`WAVE_TASKS`] blocking tasks.
    async fn sync_wave(&self, shards: &Shards, root: bool) -> Result<()> {
        if shards.is_empty() && !root {
            return Ok(());
        }
        let dirs: Vec<PathBuf> = shards
            .iter()
            .map(|s| self.root.join(format!("{s:02x}")))
            .chain(root.then(|| self.root.clone()))
            .collect();
        self.syncs.fetch_add(dirs.len() as u64, Ordering::Relaxed);
        let mut groups: Vec<Vec<PathBuf>> = vec![Vec::new(); WAVE_TASKS.min(dirs.len())];
        for (i, d) in dirs.into_iter().enumerate() {
            groups[i % WAVE_TASKS].push(d);
        }
        let tasks: Vec<_> = groups
            .into_iter()
            .map(|g| {
                tokio::task::spawn_blocking(move || {
                    // Every directory is tried, so one error does not leave
                    // the rest of the wave for the retry.
                    let mut res = Ok(());
                    for d in &g {
                        if let Err(e) = sync_existing_dir(d) {
                            res = Err(e);
                        }
                    }
                    res
                })
            })
            .collect();
        let mut first_err = None;
        for t in tasks {
            let r = t
                .await
                .map_err(|e| Error::Storage(format!("directory sync task failed: {e}")))
                .and_then(|r| r);
            if let Err(e) = r {
                first_err.get_or_insert(e);
            }
        }
        first_err.map_or(Ok(()), Err)
    }
}

/// A shard named only through `flush(named)` may have no directory (its
/// blobs are all packed); there is then nothing loose to make durable.
fn sync_existing_dir(dir: &Path) -> Result<()> {
    match std::fs::metadata(dir) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        _ => sync_dir(dir),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn marks_coalesce_and_root_syncs_once_per_shard() {
        let dir = tempfile::tempdir().unwrap();
        for s in ["00", "01", "ff"] {
            std::fs::create_dir(dir.path().join(s)).unwrap();
        }
        let d = DirSyncer::new(dir.path());
        d.flush([]).await.unwrap();
        assert_eq!(d.syncs(), 0, "nothing marked, nothing synced");
        d.mark(0);
        d.mark(0);
        d.mark(1);
        assert_eq!(d.pending(), 3);
        d.flush([]).await.unwrap();
        assert_eq!(d.syncs(), 3, "two shards and the root, once each");
        assert_eq!(d.pending(), 0);
        d.flush([]).await.unwrap();
        assert_eq!(d.syncs(), 3, "a covered flush syncs nothing");
        d.flush([0]).await.unwrap();
        assert_eq!(d.syncs(), 4, "a known shard needs no root sync");
        d.flush([0xff, 0x7f]).await.unwrap();
        assert_eq!(d.syncs(), 7, "a missing shard directory is skipped");
    }

    #[tokio::test]
    async fn a_failed_wave_is_retried_by_the_next_flush() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("root");
        // A file where the root should be: the shard path is not a directory.
        std::fs::write(&root, b"x").unwrap();
        let d = DirSyncer::new(&root);
        d.mark(3);
        assert!(d.flush([]).await.is_err());
        assert_eq!(d.pending(), 2, "the wave is put back");
        std::fs::remove_file(&root).unwrap();
        std::fs::create_dir_all(root.join("03")).unwrap();
        d.flush([]).await.unwrap();
        assert_eq!(d.pending(), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_flushes_all_return_covered() {
        let dir = tempfile::tempdir().unwrap();
        for s in 0..=255u8 {
            std::fs::create_dir(dir.path().join(format!("{s:02x}"))).unwrap();
        }
        let d = std::sync::Arc::new(DirSyncer::new(dir.path()));
        let tasks: Vec<_> = (0..64u32)
            .map(|i| {
                let d = d.clone();
                tokio::spawn(async move {
                    for j in 0..8u32 {
                        let shard = ((i * 8 + j) % 256) as u8;
                        d.mark(shard);
                        d.flush([]).await.unwrap();
                        let s = d.lock();
                        assert!(s.durable_shards.contains(shard));
                    }
                })
            })
            .collect();
        for t in tasks {
            t.await.unwrap();
        }
        assert_eq!(d.pending(), 0);
        assert!(
            d.syncs() < 64 * 8 * 2,
            "waves were shared: {} syncs",
            d.syncs()
        );
    }
}
