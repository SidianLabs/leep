//! Group commit for loose blob writes in the filesystem store (ADR 0076).
//!
//! A put writes its temp file, then hands it here. While other puts are in
//! flight, the first file handed over opens a short window; the files that
//! arrive in it are made durable together (one `syncfs` on Linux 5.8+,
//! instead of an fsync each), and only then renamed into place, so a blob
//! is under its final name only once its bytes are durable, as before. A
//! put alone is synced and renamed at once, exactly as with no window.
//!
//! Directory entries stay deferred to `flush` (ADR 0044, ADR 0048): each
//! rename marks its shard in the [`DirSyncer`] as a put always did.

use std::fs::File;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use leep_core::{Error, Result};
use tokio::sync::{oneshot, Notify};

use crate::dirsync::DirSyncer;
use crate::storage_err;

/// Files that close a window early: past this the batch saves little more
/// and its puts have waited for nothing.
const MAX_BATCH: usize = 256;

/// A temp file written and not yet synced, and where it goes.
pub(crate) struct Job {
    pub tmp: PathBuf,
    pub path: PathBuf,
    /// Closed once the job joins a batch synced by `syncfs`, so a backlog of
    /// queued puts does not hold a descriptor each.
    pub file: Option<File>,
    pub shard: u8,
    /// [`GroupSync::failures`] when the file was created: a shared sync that
    /// failed since may have taken the error that was this file's.
    pub failures: u64,
}

/// What [`GroupSync::submit`] did with a file.
pub(crate) enum Submitted {
    /// Synced and renamed on the calling thread.
    Done(Result<()>),
    /// Joined a batch; answers when the blob is in place.
    Queued(oneshot::Receiver<Result<()>>),
}

struct Waiting {
    job: Job,
    done: oneshot::Sender<Result<()>>,
}

#[derive(Default)]
struct Queue {
    jobs: Vec<Waiting>,
    /// Set while a batch task is collecting `jobs`; wakes it when full.
    open: Option<Arc<Notify>>,
}

pub(crate) struct GroupSync {
    window: Duration,
    root: PathBuf,
    dirs: Arc<DirSyncer>,
    queue: Mutex<Queue>,
    /// Puts between starting and handing their file over (or giving up).
    in_flight: AtomicUsize,
    /// Calls that made blob bytes durable: one per fsync or syncfs.
    syncs: AtomicU64,
    /// Shared syncs that failed. Bumped under `shared`.
    failures: AtomicU64,
    /// Held across a shared sync and the failure count it may bump, so a
    /// sync that took another batch's writeback error is counted before
    /// that batch's own sync returns.
    shared: Mutex<()>,
    /// Opened before the first file a shared sync covers is written: Linux
    /// reports to `syncfs` only writeback errors raised after the
    /// descriptor was opened.
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    root_fd: OnceLock<std::io::Result<File>>,
    #[cfg(test)]
    pub hook: Mutex<Option<SyncHook>>,
}

/// Runs just before a sync, for tests, with the final paths of the
/// files it covers; an error fails the sync.
#[cfg(test)]
pub(crate) type SyncHook = Box<dyn Fn(&[PathBuf]) -> std::io::Result<()> + Send + Sync>;

/// Decrements [`GroupSync::in_flight`] when dropped, however the put ends.
pub(crate) struct InFlight(Arc<GroupSync>);

impl Drop for InFlight {
    fn drop(&mut self) {
        self.0.in_flight.fetch_sub(1, Ordering::AcqRel);
    }
}

impl GroupSync {
    pub fn new(root: PathBuf, dirs: Arc<DirSyncer>, window: Duration) -> Self {
        Self {
            window,
            root,
            dirs,
            queue: Mutex::default(),
            in_flight: AtomicUsize::new(0),
            syncs: AtomicU64::new(0),
            failures: AtomicU64::new(0),
            shared: Mutex::new(()),
            root_fd: OnceLock::new(),
            #[cfg(test)]
            hook: Mutex::new(None),
        }
    }

    pub fn window(&self) -> Duration {
        self.window
    }

    pub fn syncs(&self) -> u64 {
        self.syncs.load(Ordering::Relaxed)
    }

    pub fn count_sync(&self) {
        self.syncs.fetch_add(1, Ordering::Relaxed);
    }

    /// Call before the put's blocking work starts, so puts writing their
    /// files hold a window open for each other.
    pub fn enter(self: &Arc<Self>) -> InFlight {
        self.in_flight.fetch_add(1, Ordering::AcqRel);
        InFlight(self.clone())
    }

    /// The failure count to put in a new [`Job`], after making sure the
    /// descriptor shared syncs report errors through is open. Call once
    /// the shard directory exists, before writing the temp file.
    pub fn prepare(&self) -> u64 {
        #[cfg(target_os = "linux")]
        self.root_fd.get_or_init(|| File::open(&self.root));
        self.failures.load(Ordering::Acquire)
    }

    /// Make `job` durable and rename it into place: at once if no other put
    /// is in flight or waiting, else with the batch it joins. Runs on a
    /// blocking thread. A batch completes even if nobody waits for it, so a
    /// cancelled put still lands, as one with no window does.
    pub fn submit(
        self: &Arc<Self>,
        job: Job,
        in_flight: InFlight,
        rt: &tokio::runtime::Handle,
    ) -> Submitted {
        let mut q = self.queue.lock().unwrap();
        let alone =
            q.open.is_none() && q.jobs.is_empty() && self.in_flight.load(Ordering::Acquire) == 1;
        if alone {
            drop(q);
            drop(in_flight);
            let synced = self.sync(std::slice::from_ref(&job));
            return Submitted::Done(self.publish(job, synced));
        }
        let mut job = job;
        if self.shares_syncs() {
            job.file = None;
        } else if q.jobs.len() >= MAX_BATCH {
            // Each queued file holds a descriptor until its own fsync: past
            // a full batch, sync this one now rather than pile them up.
            drop(q);
            drop(in_flight);
            let synced = self.sync(std::slice::from_ref(&job));
            return Submitted::Done(self.publish(job, synced));
        }
        let (tx, rx) = oneshot::channel();
        q.jobs.push(Waiting { job, done: tx });
        match &q.open {
            Some(full) if q.jobs.len() >= MAX_BATCH => full.notify_one(),
            Some(_) => {}
            None => {
                let full = Arc::new(Notify::new());
                q.open = Some(full.clone());
                let group = self.clone();
                rt.spawn(async move { group.run(full).await });
            }
        }
        drop(q);
        drop(in_flight);
        Submitted::Queued(rx)
    }

    /// One batch: wait out the window, then sync whatever has queued by the
    /// time the previous shared sync is done.
    async fn run(self: Arc<Self>, full: Arc<Notify>) {
        tokio::select! {
            () = tokio::time::sleep(self.window) => {}
            () = full.notified() => {}
        }
        let group = self.clone();
        let res = tokio::task::spawn_blocking(move || {
            let _shared = group.shared.lock().unwrap_or_else(|p| p.into_inner());
            let jobs = {
                let mut q = group.queue.lock().unwrap();
                q.open = None;
                std::mem::take(&mut q.jobs)
            };
            group.commit_locked(jobs);
        })
        .await;
        if let Err(e) = res {
            // The waiting puts see their senders dropped and fail.
            tracing::error!(error = %e, "blob sync batch failed");
        }
    }

    /// Sync and rename a batch, answering each waiting put.
    fn commit_locked(&self, waiting: Vec<Waiting>) {
        if waiting.is_empty() {
            return;
        }
        let (jobs, done): (Vec<Job>, Vec<_>) = waiting.into_iter().map(|w| (w.job, w.done)).unzip();
        let synced = self.sync(&jobs);
        for (job, done) in jobs.into_iter().zip(done) {
            let _ = done.send(self.publish(job, synced.clone()));
        }
    }

    /// Make every file of `jobs` durable: an fsync for one, a `syncfs` for
    /// more where it reports writeback errors, else an fsync each.
    fn sync(&self, jobs: &[Job]) -> std::result::Result<(), String> {
        #[cfg(test)]
        if let Some(hook) = &*self.hook.lock().unwrap() {
            let paths: Vec<PathBuf> = jobs.iter().map(|j| j.path.clone()).collect();
            hook(&paths).map_err(|e| e.to_string())?;
        }
        if jobs.len() > 1 || jobs.iter().any(|j| j.file.is_none()) {
            if let Some(res) = self.sync_fs(jobs) {
                return res;
            }
        }
        for j in jobs {
            let Some(file) = &j.file else {
                // Only closed when `syncfs` is usable, which it then was.
                return Err("blob file closed before its sync".into());
            };
            self.count_sync();
            file.sync_all().map_err(|e| e.to_string())?;
        }
        Ok(())
    }

    /// Whether a batch is synced with one `syncfs` here: Linux 5.8+, with
    /// the root's descriptor open.
    pub(crate) fn shares_syncs(&self) -> bool {
        #[cfg(target_os = "linux")]
        return syncfs_reports_errors() && matches!(self.root_fd.get(), Some(Ok(_)));
        #[cfg(not(target_os = "linux"))]
        false
    }

    /// One `syncfs` for the batch, or None where that is not safe to rely
    /// on. Called with `shared` held, by batch tasks only.
    #[cfg(target_os = "linux")]
    fn sync_fs(&self, jobs: &[Job]) -> Option<std::result::Result<(), String>> {
        if !syncfs_reports_errors() {
            return None;
        }
        let Some(Ok(fd)) = self.root_fd.get() else {
            return None;
        };
        self.count_sync();
        if let Err(e) = rustix::fs::syncfs(fd) {
            self.failures.fetch_add(1, Ordering::AcqRel);
            return Some(Err(e.to_string()));
        }
        // A sync that failed after a file was created may have reported
        // that file's error to another batch; this one then saw none.
        let now = self.failures.load(Ordering::Acquire);
        if jobs.iter().any(|j| j.failures != now) {
            return Some(Err("an earlier shared blob sync failed".into()));
        }
        Some(Ok(()))
    }

    #[cfg(not(target_os = "linux"))]
    fn sync_fs(&self, _jobs: &[Job]) -> Option<std::result::Result<(), String>> {
        None
    }

    /// Rename a synced file into place and mark its shard, or remove it.
    fn publish(&self, job: Job, synced: std::result::Result<(), String>) -> Result<()> {
        let Job {
            tmp,
            path,
            file,
            shard,
            ..
        } = job;
        drop(file);
        let res = synced
            .map_err(Error::Storage)
            .and_then(|()| std::fs::rename(&tmp, &path).map_err(storage_err));
        match res {
            Ok(()) => {
                self.dirs.mark(shard);
                Ok(())
            }
            Err(e) => {
                let _ = std::fs::remove_file(&tmp);
                Err(e)
            }
        }
    }
}

/// Since Linux 5.8 `syncfs` returns writeback errors (before, it returned
/// 0 whatever happened, so a shared sync could hide a lost write).
#[cfg(target_os = "linux")]
fn syncfs_reports_errors() -> bool {
    static OK: OnceLock<bool> = OnceLock::new();
    *OK.get_or_init(|| {
        let Ok(release) = std::fs::read_to_string("/proc/sys/kernel/osrelease") else {
            return false;
        };
        let mut parts = release.trim().split(['.', '-']);
        let major: u32 = parts.next().and_then(|p| p.parse().ok()).unwrap_or(0);
        let minor: u32 = parts.next().and_then(|p| p.parse().ok()).unwrap_or(0);
        (major, minor) >= (5, 8)
    })
}
