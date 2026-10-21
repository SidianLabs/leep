//! Who builds the indexes, and what queries do while they lag (ADR 0058).
//!
//! Three index groups are built outside the write path: path history, the
//! log filter and code search. In `local` mode every server warms all of
//! them and queries derive what they miss, as before. In `lease` mode one
//! server per group, the holder of the group's index lease, builds it; in
//! `lease` and `off` modes queries never derive an index range while a live
//! lease exists. They answer from the rows that exist plus a bounded scan of
//! what is missing ([`bounded`]), or report how far behind the index is.

pub(crate) mod bounded;
pub mod lease;

use std::sync::Arc;
use std::time::Duration;

use leep_core::ChangesetId;

use crate::derived::{DerivedKind, HistoryTree, LogFilterIndex};

pub use bounded::Freshness;
pub use lease::{
    parse_holder_meta, HolderMeta, IndexLeaseConfig, IndexLeases, ToolLease, INDEX_LEASE_PROTOCOL,
};

/// A set of index kinds built together, named by its primary kind. One
/// lease per group (ADR 0058 section 2).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum IndexGroup {
    /// `history-tree-v4` and `history-node-v4`, with the write-path kinds
    /// they read (`last_merge-v1` among them).
    History,
    /// `log-filter-v1` and `log-filter-name-v1`.
    LogFilter,
    /// `tree-trigrams-v1` and `blob-trigrams-v1`.
    Search,
}

impl IndexGroup {
    pub const ALL: [IndexGroup; 3] = [
        IndexGroup::History,
        IndexGroup::LogFilter,
        IndexGroup::Search,
    ];

    /// The lease name: the group's primary kind, versioned, so servers of
    /// different index versions never share a lease.
    pub fn lease_name(self) -> &'static str {
        match self {
            IndexGroup::History => HistoryTree::NAME,
            IndexGroup::LogFilter => LogFilterIndex::NAME,
            IndexGroup::Search => crate::search::TREE_KIND,
        }
    }

    /// A group by lease name, or by `history`, `log-filter` or `search`.
    pub fn parse(s: &str) -> Option<Self> {
        IndexGroup::ALL
            .into_iter()
            .find(|g| g.lease_name() == s || g.short_name() == s)
    }

    pub fn short_name(self) -> &'static str {
        match self {
            IndexGroup::History => "history",
            IndexGroup::LogFilter => "log-filter",
            IndexGroup::Search => "search",
        }
    }

    /// The group that builds `kind`, if an index group does.
    pub fn of_kind(kind: &str) -> Option<Self> {
        match kind {
            k if k == HistoryTree::NAME || k == crate::derived::HISTORY_NODE_KIND => {
                Some(IndexGroup::History)
            }
            k if k == LogFilterIndex::NAME || k == crate::derived::LOG_FILTER_NAME_KIND => {
                Some(IndexGroup::LogFilter)
            }
            k if k == crate::search::TREE_KIND || k == crate::search::BLOB_KIND => {
                Some(IndexGroup::Search)
            }
            _ => None,
        }
    }
}

/// Who builds indexes in this process (`--indexing`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IndexMode {
    /// Every server warms every group and queries derive what they miss.
    Local,
    /// Warm only the groups whose lease this process holds; queries stay
    /// bounded while a lease lives.
    Lease,
    /// Never warm; queries stay bounded while a lease lives.
    Off,
}

/// How much un-indexed history a query examines directly before it gives up
/// (ADR 0058 section 3). The defaults are deliberately low guesses, to be
/// raised once the hybrid walks are measured.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Budgets {
    /// Changesets without a row a path log, last-change or filtered log
    /// examines (`--query-scan-budget`).
    pub scan: usize,
    /// First-parent steps without a row a blame walks one by one, each a
    /// file read (`--blame-scan-budget`).
    pub blame: usize,
    /// Files without a trigram row a search fetches (`--search-scan-budget`).
    pub search_files: usize,
    /// Bytes of those files (`--search-scan-bytes`).
    pub search_bytes: u64,
    /// Requests without a `Leep-Indexing` header get this many times the
    /// budgets once their wait is over (`--legacy-scan-factor`).
    pub legacy_factor: usize,
}

impl Default for Budgets {
    fn default() -> Self {
        Budgets {
            scan: 1024,
            blame: 128,
            search_files: 512,
            search_bytes: 16 << 20,
            legacy_factor: 8,
        }
    }
}

impl Budgets {
    /// These budgets multiplied by `factor`, for requests without a header.
    pub fn times(self, factor: usize) -> Budgets {
        Budgets {
            scan: self.scan.saturating_mul(factor),
            blame: self.blame.saturating_mul(factor),
            search_files: self.search_files.saturating_mul(factor),
            search_bytes: self.search_bytes.saturating_mul(factor as u64),
            legacy_factor: self.legacy_factor,
        }
    }
}

/// What queries in this process do about index groups (ADR 0058 section 3).
/// A [`crate::Repo`] without one behaves as in `local` mode.
pub struct IndexPolicy {
    pub mode: IndexMode,
    pub budgets: Budgets,
    /// Reads (and in `lease` mode holds) the index leases.
    pub leases: Arc<IndexLeases>,
    /// How long a request without a header waits for the index
    /// (`--index-wait`), at most its remaining time less a second.
    pub index_wait: Duration,
    /// The server's `--request-timeout` (zero for none), which bounds waits.
    pub request_timeout: Duration,
    /// Requests that may wait at once (`--index-waiters`).
    waiting: Arc<tokio::sync::Semaphore>,
    /// Changesets a filtered log scans before it uses its index (tests make
    /// it small to reach the index on short histories).
    pub(crate) scan_first: usize,
}

impl IndexPolicy {
    pub fn new(mode: IndexMode, leases: Arc<IndexLeases>) -> Self {
        IndexPolicy {
            mode,
            budgets: Budgets::default(),
            leases,
            index_wait: Duration::from_secs(10),
            request_timeout: Duration::ZERO,
            waiting: Arc::new(tokio::sync::Semaphore::new(64)),
            scan_first: crate::filter_log::SCAN_FIRST,
        }
    }

    /// Scan this many changesets of a filtered log before its index.
    #[doc(hidden)]
    pub fn with_scan_first(mut self, n: usize) -> Self {
        self.scan_first = n;
        self
    }

    pub fn with_budgets(mut self, budgets: Budgets) -> Self {
        self.budgets = budgets;
        self
    }

    pub fn with_index_wait(mut self, wait: Duration, waiters: usize) -> Self {
        self.index_wait = wait;
        self.waiting = Arc::new(tokio::sync::Semaphore::new(waiters));
        self
    }

    pub fn with_request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    pub(crate) fn waiting(&self) -> &Arc<tokio::sync::Semaphore> {
        &self.waiting
    }
}

/// A query's answer, and where it was answered when that is not the
/// changeset asked about (`Leep-Indexing: stale-ok`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Answered<T> {
    pub value: T,
    /// The changeset the answer is for, when it is not the requested one.
    pub answered_at: Option<ChangesetId>,
    /// Generations between `answered_at` and the requested changeset.
    pub behind: Option<u64>,
    /// Changesets (files, for search) without index rows that the answer
    /// examined directly (`Leep-Index-Scanned`).
    pub scanned: u64,
}

impl<T> Answered<T> {
    pub(crate) fn fresh(value: T) -> Self {
        Answered {
            value,
            answered_at: None,
            behind: None,
            scanned: 0,
        }
    }
}
