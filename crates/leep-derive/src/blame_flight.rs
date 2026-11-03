//! Blames that outlive their request (ADR 0078).
//!
//! A blame of a file changed thousands of times can take longer than the
//! server's request timeout, which drops the handler; if the blame ran in
//! the handler, every retry started again from nothing and timed out again.
//! So a blame runs on its own task, joined by every request for the same
//! blame while it runs, and finishes (caching its answer) whether or not
//! anyone still waits for it.

use std::any::Any;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use leep_core::{ChangesetId, Error, RepoPath, Result};
use tokio::sync::{watch, OwnedSemaphorePermit, Semaphore};

use crate::blame::CachedBlame;
use crate::Repo;

/// Something a detached blame keeps until it ends, such as the request's
/// read-class permit (ADR 0074): a blame that outlives its request still
/// counts against the class, so it cannot bypass load shedding.
pub type BlameHold = Box<dyn Any + Send + Sync>;

/// A blame's answer, shared by everyone waiting for it.
type Outcome = std::result::Result<Arc<CachedBlame>, Arc<Error>>;

/// One blame: its changeset, its path, and whether it is bounded (ADR
/// 0058), whose answer may be incomplete where an unbounded one is not.
type Key = (ChangesetId, RepoPath, bool);

/// The blames running detached in this process, shared by clones of a
/// [`Repo`].
pub(crate) struct BlameFlights {
    running: Mutex<HashMap<Key, watch::Receiver<Option<Outcome>>>>,
    /// Detached blames at once, at most. On a server each also holds a
    /// read-class permit, which bounds them more tightly; this bounds them
    /// where the class is off, and for embedders without classes.
    slots: Arc<Semaphore>,
    /// Requests that joined a running blame.
    joins: AtomicUsize,
}

impl BlameFlights {
    pub(crate) fn new(slots: usize) -> Self {
        BlameFlights {
            running: Mutex::default(),
            slots: Arc::new(Semaphore::new(slots)),
            joins: AtomicUsize::new(0),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<Key, watch::Receiver<Option<Outcome>>>> {
        self.running.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Blames running detached now.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn running(&self) -> usize {
        self.lock().len()
    }

    /// Requests that joined a running blame so far.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn joins(&self) -> usize {
        self.joins.load(Ordering::Relaxed)
    }
}

impl Default for BlameFlights {
    fn default() -> Self {
        let cores = std::thread::available_parallelism().map_or(2, |n| n.get());
        BlameFlights::new(2 * cores.max(2))
    }
}

/// Removes a flight from the map when its task ends, even by a panic.
struct Leave {
    flights: Arc<BlameFlights>,
    key: Key,
}

impl Drop for Leave {
    fn drop(&mut self) {
        self.flights.lock().remove(&self.key);
        metrics::gauge!("leep_blame_detached").decrement(1.0);
    }
}

/// `e` again, for the second and later waiters of a failed blame.
fn again(e: &Error) -> Error {
    match e {
        Error::InvalidPath(p, why) => Error::InvalidPath(p.clone(), why),
        Error::InvalidHash(h) => Error::InvalidHash(h.clone()),
        Error::Invalid(m) => Error::Invalid(m.clone()),
        Error::Conflict(m) => Error::Conflict(m.clone()),
        Error::NotFound(m) => Error::NotFound(m.clone()),
        Error::Indexing {
            kind,
            behind,
            retry_after_ms,
        } => Error::Indexing {
            kind: kind.clone(),
            behind: *behind,
            retry_after_ms: *retry_after_ms,
        },
        Error::Refused {
            refused,
            retry_after_ms,
            message,
        } => Error::Refused {
            refused,
            retry_after_ms: *retry_after_ms,
            message: message.clone(),
        },
        Error::Encoding(m) => Error::Encoding(m.clone()),
        Error::Storage(m) => Error::Storage(m.clone()),
        other => Error::Other(other.to_string()),
    }
}

impl Repo {
    /// [`Repo::blame_lines`] on a task of its own, shared with every
    /// concurrent request for the same blame: a request that times out and
    /// is sent again joins the blame still running instead of starting
    /// over, and the blame caches its answer even if nobody waits for it
    /// any more. `hold` is kept until the blame ends. With every detached
    /// slot taken the blame runs here, as before, and ends with the
    /// request.
    pub(crate) async fn blame_shared(
        &self,
        id: ChangesetId,
        path: &RepoPath,
        hold: Option<BlameHold>,
    ) -> Result<CachedBlame> {
        let key: Key = (id, path.clone(), self.bounded.is_some());
        let rx = {
            let mut running = self.blame_flights.lock();
            match running.get(&key) {
                Some(rx) => {
                    self.blame_flights.joins.fetch_add(1, Ordering::Relaxed);
                    metrics::counter!("leep_blame_joined_total").increment(1);
                    Some(rx.clone())
                }
                None => match self.blame_flights.slots.clone().try_acquire_owned() {
                    Ok(slot) => {
                        let (tx, rx) = watch::channel(None);
                        running.insert(key.clone(), rx.clone());
                        metrics::gauge!("leep_blame_detached").increment(1.0);
                        let leave = Leave {
                            flights: self.blame_flights.clone(),
                            key,
                        };
                        self.spawn_blame(id, path.clone(), tx, leave, slot, hold);
                        Some(rx)
                    }
                    Err(_) => None,
                },
            }
        };
        let Some(mut rx) = rx else {
            return self.blame_lines(id, path).await;
        };
        let done = rx
            .wait_for(Option::is_some)
            .await
            .map_err(|_| Error::Other(format!("blame of {path} at {id} failed")))?;
        match done.as_ref().expect("waited for an outcome") {
            Ok(blame) => Ok(CachedBlame::clone(blame)),
            Err(e) => Err(again(e)),
        }
    }

    fn spawn_blame(
        &self,
        id: ChangesetId,
        path: RepoPath,
        tx: watch::Sender<Option<Outcome>>,
        leave: Leave,
        slot: OwnedSemaphorePermit,
        hold: Option<BlameHold>,
    ) {
        let repo = self.clone();
        tokio::spawn(async move {
            let out = repo.blame_lines(id, &path).await;
            // The permit and the slot go back before anyone hears the
            // answer, so a request sent after it is not refused for them;
            // the flight leaves the map first, so such a request reads the
            // cached answer rather than this flight's.
            drop((hold, slot, leave));
            let _ = tx.send(Some(out.map(Arc::new).map_err(Arc::new)));
        });
    }
}
