//! Deferred directory syncs for the filesystem store (ADR 0044, ADR 0048).
//!
//! A loose blob is durable once its file is synced (done by the put) and the
//! rename that published it is synced, which needs an fsync of its shard
//! directory, and of the root when the shard is new. Syncing a directory per
//! put cost one more disk flush per blob; instead a put records its shard
//! here, and a flush syncs the recorded directories before anything that
//! depends on the blobs is acknowledged.
//!
//! Each directory is tracked on its own: a mark takes a sequence number, and
//! a sync of a directory that started at sequence `n` covers every mark of it
//! numbered up to `n`. A flush syncs only the directories it needs, starting
//! their syncs at once, and waits instead for a sync already running that
//! started after its marks. A create thus waits for its own few directories,
//! not for every directory other requests' uploads dirtied, and concurrent
//! flushes of the same directory share a sync.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use leep_core::{Error, Result};

use crate::packset::sync_dir;

/// Blocking tasks one flush's syncs are split across. A directory fsync
/// mostly waits for the journal and a device flush; a thread per directory
/// cost more in scheduling than it gained, measured with waves of 4 to 128.
const WAVE_TASKS: usize = 4;

/// Index of the store root in the per-directory arrays; shards are 0..=255.
const ROOT: usize = 256;
const DIRS: usize = 257;

/// 256 shards as a bit set.
#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct Shards([u64; 4]);

impl Shards {
    pub fn insert(&mut self, s: u8) {
        self.0[usize::from(s >> 6)] |= 1 << (s & 63);
    }

    pub fn contains(&self, s: u8) -> bool {
        self.0[usize::from(s >> 6)] & (1 << (s & 63)) != 0
    }

    fn iter(&self) -> impl Iterator<Item = u8> + '_ {
        (0..=255u8).filter(|s| self.contains(*s))
    }
}

struct State {
    /// Marks so far; each mark's number is the count after it.
    seq: u64,
    /// Per directory (shards, then the root): the number of its last mark.
    marked: [u64; DIRS],
    /// Per directory: every mark numbered up to this is durable.
    synced: [u64; DIRS],
    /// Per directory: the start number of the latest sync running, or 0.
    running: [u64; DIRS],
    /// Per shard: the number of its first mark, after which the directory
    /// exists, so a root sync started later makes its entry durable.
    first: [u64; 256],
    /// Shards whose entry in the root this process has synced.
    in_root: Shards,
}

impl Default for State {
    fn default() -> Self {
        State {
            seq: 0,
            marked: [0; DIRS],
            synced: [0; DIRS],
            running: [0; DIRS],
            first: [0; 256],
            in_root: Shards::default(),
        }
    }
}

impl State {
    fn mark(&mut self, shard: u8) {
        self.seq += 1;
        let s = usize::from(shard);
        self.marked[s] = self.seq;
        if self.first[s] == 0 {
            self.first[s] = self.seq;
        }
        if !self.in_root.contains(shard) {
            self.marked[ROOT] = self.seq;
        }
    }

    /// What `shards` need synced before their marks so far are durable: each
    /// directory with the mark number a sync must start at or after.
    fn targets(&self, shards: &Shards) -> Vec<(usize, u64)> {
        let mut out = Vec::new();
        let mut root = false;
        for shard in shards.iter() {
            let s = usize::from(shard);
            if self.synced[s] < self.marked[s] {
                out.push((s, self.marked[s]));
            }
            root |= !self.in_root.contains(shard) && self.first[s] != 0;
        }
        if root && self.synced[ROOT] < self.marked[ROOT] {
            out.push((ROOT, self.marked[ROOT]));
        }
        out
    }

    /// Every shard with a mark not yet durable.
    fn dirty(&self) -> Shards {
        let mut out = Shards::default();
        for shard in 0..=255u8 {
            let s = usize::from(shard);
            if self.synced[s] < self.marked[s] {
                out.insert(shard);
            }
        }
        out
    }

    fn finish(&mut self, dirs: &[usize], started: u64, ok: bool) {
        for &d in dirs {
            if ok {
                self.synced[d] = self.synced[d].max(started);
                if d == ROOT {
                    for shard in 0..=255u8 {
                        let f = self.first[usize::from(shard)];
                        if f != 0 && f <= started {
                            self.in_root.insert(shard);
                        }
                    }
                }
            }
            if self.running[d] == started {
                self.running[d] = 0;
            }
        }
    }
}

/// What the blocking sync tasks update themselves, so a sync is recorded as
/// finished even if the flush that started it was cancelled meanwhile.
#[derive(Default)]
struct Shared {
    state: Mutex<State>,
    /// Woken whenever a sync finishes, so flushes waiting on it re-check.
    done: tokio::sync::Notify,
}

impl Shared {
    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }
}

pub(crate) struct DirSyncer {
    root: PathBuf,
    shared: Arc<Shared>,
    /// Directory fsyncs issued, for tests.
    syncs: AtomicU64,
}

impl DirSyncer {
    pub fn new(root: &Path) -> Self {
        DirSyncer {
            root: root.to_path_buf(),
            shared: Arc::default(),
            syncs: AtomicU64::new(0),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.shared.lock()
    }

    /// Record that `shard` holds a blob that must become durable at the next
    /// flush that covers it: one this process renamed into place, or found
    /// there (it may be another writer's rename that nobody has synced yet).
    /// Call after the rename or the lookup, so a sync that covers this mark
    /// starts after it.
    pub fn mark(&self, shard: u8) {
        self.lock().mark(shard);
    }

    pub fn syncs(&self) -> u64 {
        self.syncs.load(Ordering::Relaxed)
    }

    /// Directories with marks not yet durable (the root counts as one).
    pub fn pending(&self) -> usize {
        let s = self.lock();
        s.dirty().iter().count() + usize::from(s.synced[ROOT] < s.marked[ROOT])
    }

    /// Mark `named`, then return once every mark made so far, of any shard,
    /// is durable.
    pub async fn flush_all(&self, named: impl IntoIterator<Item = u8>) -> Result<()> {
        let targets = {
            let mut s = self.lock();
            for shard in named {
                s.mark(shard);
            }
            let dirty = s.dirty();
            s.targets(&dirty)
        };
        self.reach(targets).await
    }

    /// Mark `shards`, then return once their marks so far are durable,
    /// without waiting for any other shard.
    pub async fn flush_shards(&self, shards: impl IntoIterator<Item = u8>) -> Result<()> {
        let targets = {
            let mut s = self.lock();
            let mut set = Shards::default();
            for shard in shards {
                s.mark(shard);
                set.insert(shard);
            }
            s.targets(&set)
        };
        self.reach(targets).await
    }

    /// Return once every `(directory, mark)` in `targets` is durable, starting
    /// a sync of each directory no running sync covers and waiting for the
    /// rest.
    async fn reach(&self, mut targets: Vec<(usize, u64)>) -> Result<()> {
        loop {
            let notified = self.shared.done.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let (start, started) = {
                let mut s = self.lock();
                targets.retain(|&(d, t)| s.synced[d] < t);
                if targets.is_empty() {
                    return Ok(());
                }
                let started = s.seq;
                let start: Vec<usize> = targets
                    .iter()
                    .filter(|&&(d, t)| s.running[d] < t)
                    .map(|&(d, _)| d)
                    .collect();
                for &d in &start {
                    s.running[d] = started;
                }
                (start, started)
            };
            if start.is_empty() {
                notified.await;
                continue;
            }
            self.sync_dirs(&start, started).await?;
        }
    }

    /// Sync the directories `dirs` (shard numbers and [`ROOT`]), whose syncs
    /// were recorded as started at `started`, in up to [`WAVE_TASKS`]
    /// blocking tasks. Each task records its directories when it finishes.
    async fn sync_dirs(&self, dirs: &[usize], started: u64) -> Result<()> {
        self.syncs.fetch_add(dirs.len() as u64, Ordering::Relaxed);
        let mut groups: Vec<Vec<usize>> = vec![Vec::new(); WAVE_TASKS.min(dirs.len())];
        for (i, &d) in dirs.iter().enumerate() {
            groups[i % WAVE_TASKS].push(d);
        }
        let tasks: Vec<_> = groups
            .into_iter()
            .map(|g| {
                let paths: Vec<PathBuf> = g.iter().map(|&d| self.path(d)).collect();
                let shared = self.shared.clone();
                tokio::task::spawn_blocking(move || {
                    // Every directory is tried, so one error does not leave
                    // the rest for the retry.
                    let mut res = Ok(());
                    for p in &paths {
                        if let Err(e) = sync_existing_dir(p) {
                            res = Err(e);
                        }
                    }
                    shared.lock().finish(&g, started, res.is_ok());
                    shared.done.notify_waiters();
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

    fn path(&self, dir: usize) -> PathBuf {
        if dir == ROOT {
            self.root.clone()
        } else {
            self.root.join(format!("{dir:02x}"))
        }
    }
}

/// A shard named only through a flush may have no directory (its blobs are
/// all packed); there is then nothing loose to make durable.
fn sync_existing_dir(dir: &Path) -> Result<()> {
    match std::fs::metadata(dir) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        _ => sync_dir(dir),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shard_dirs(root: &Path) {
        for s in 0..=255u8 {
            std::fs::create_dir(root.join(format!("{s:02x}"))).unwrap();
        }
    }

    #[tokio::test]
    async fn marks_coalesce_and_root_syncs_once_per_shard() {
        let dir = tempfile::tempdir().unwrap();
        for s in ["00", "01", "ff"] {
            std::fs::create_dir(dir.path().join(s)).unwrap();
        }
        let d = DirSyncer::new(dir.path());
        d.flush_all([]).await.unwrap();
        assert_eq!(d.syncs(), 0, "nothing marked, nothing synced");
        d.mark(0);
        d.mark(0);
        d.mark(1);
        assert_eq!(d.pending(), 3);
        d.flush_all([]).await.unwrap();
        assert_eq!(d.syncs(), 3, "two shards and the root, once each");
        assert_eq!(d.pending(), 0);
        d.flush_all([]).await.unwrap();
        assert_eq!(d.syncs(), 3, "a covered flush syncs nothing");
        d.flush_all([0]).await.unwrap();
        assert_eq!(d.syncs(), 4, "a known shard needs no root sync");
        d.flush_all([0xff, 0x7f]).await.unwrap();
        assert_eq!(d.syncs(), 7, "a missing shard directory is skipped");
    }

    #[tokio::test]
    async fn a_shard_flush_syncs_only_its_shards() {
        let dir = tempfile::tempdir().unwrap();
        shard_dirs(dir.path());
        let d = DirSyncer::new(dir.path());
        d.mark(1);
        d.mark(2);
        d.mark(3);
        d.flush_shards([1]).await.unwrap();
        assert_eq!(d.syncs(), 2, "shard 1 and the root");
        assert_eq!(d.pending(), 2, "shards 2 and 3; the root sync covered them");
        d.flush_shards([1]).await.unwrap();
        assert_eq!(
            d.syncs(),
            3,
            "naming a shard asks for a sync after the call"
        );
        d.flush_shards([]).await.unwrap();
        assert_eq!(d.syncs(), 3);
        d.flush_all([]).await.unwrap();
        assert_eq!(d.syncs(), 5, "the rest, without the root");
        assert_eq!(d.pending(), 0);
    }

    #[tokio::test]
    async fn a_failed_wave_is_retried_by_the_next_flush() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("root");
        // A file where the root should be: the shard path is not a directory.
        std::fs::write(&root, b"x").unwrap();
        let d = DirSyncer::new(&root);
        d.mark(3);
        assert!(d.flush_all([]).await.is_err());
        assert!(d.pending() >= 1, "the shard stays pending");
        assert!(d.flush_shards([3]).await.is_err());
        std::fs::remove_file(&root).unwrap();
        std::fs::create_dir_all(root.join("03")).unwrap();
        d.flush_shards([3]).await.unwrap();
        assert_eq!(d.pending(), 0);
    }

    #[tokio::test]
    async fn a_cancelled_flush_does_not_strand_its_syncs() {
        let dir = tempfile::tempdir().unwrap();
        shard_dirs(dir.path());
        let d = DirSyncer::new(dir.path());
        d.mark(5);
        // Polled once (its syncs start), then dropped.
        let _ = tokio::time::timeout(std::time::Duration::ZERO, d.flush_shards([5])).await;
        // Waits for the dropped flush's sync of shard 5, which still finishes.
        tokio::time::timeout(std::time::Duration::from_secs(30), d.flush_all([]))
            .await
            .expect("a sync started by a cancelled flush is still recorded")
            .unwrap();
        assert_eq!(d.pending(), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_flushes_all_return_covered() {
        let dir = tempfile::tempdir().unwrap();
        shard_dirs(dir.path());
        let d = std::sync::Arc::new(DirSyncer::new(dir.path()));
        let tasks: Vec<_> = (0..64u32)
            .map(|i| {
                let d = d.clone();
                tokio::spawn(async move {
                    for j in 0..8u32 {
                        let shard = ((i * 8 + j) % 256) as u8;
                        let other = shard.wrapping_add(1);
                        d.mark(shard);
                        d.mark(other);
                        let marked = d.lock().marked[usize::from(shard)];
                        if j % 2 == 0 {
                            d.flush_shards([shard]).await.unwrap();
                        } else {
                            d.flush_all([]).await.unwrap();
                        }
                        let s = d.lock();
                        assert!(s.synced[usize::from(shard)] >= marked);
                        assert!(s.in_root.contains(shard));
                        if j % 2 == 1 {
                            assert!(s.in_root.contains(other));
                        }
                    }
                })
            })
            .collect();
        for t in tasks {
            t.await.unwrap();
        }
        d.flush_all([]).await.unwrap();
        assert_eq!(d.pending(), 0);
    }
}
