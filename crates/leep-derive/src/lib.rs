//! Leep derived data and repository operations.
//!
//! [`Repo`] wraps a `BlobStore` and a `MetaStore` and implements everything the
//! server exposes: changeset creation, trees, history, diff, blame, merge,
//! rebase, land and code search. Full trees, generation numbers, ancestry
//! indexes, blame and search indexes are derived data (see [`derived`] and
//! [`search`]) computed from parents or content and cached in the
//! `MetaStore`. [`gc`] collects unreferenced blobs. See
//! `docs/ARCHITECTURE.md`.

mod ancestry;
mod batched_reads;
mod blame;
mod blame_flight;
mod blame_follow;
pub mod blame_warm;
pub mod carry;
pub mod changeset_gc;
pub mod changeset_reach;
pub mod changeset_sweep;
pub mod checks;
mod comments;
pub mod context;
mod cursor;
pub mod derived;
mod filter_log;
pub mod gc;
pub mod gc_lease;
pub mod gc_unreachable;
pub mod git_map;
mod history;
pub mod indexing;
pub mod integrity;
mod land;
pub mod land_coord;
mod land_fast;
mod land_stage;
pub mod lazy_tree;
mod merge;
pub mod notes;
pub mod notices;
pub mod owners;
pub mod pack_order;
mod path_history;
mod prefetch;
pub mod read_stats;
mod renames;
mod repo;
pub mod review;
mod review_requests;
mod reviews;
mod revision;
pub mod search;
pub mod sessions;
mod signatures;
mod tree_ops;
pub mod warm;

#[cfg(test)]
mod blame_tests;
#[cfg(test)]
mod bounded_tests;
#[cfg(test)]
mod budget_tests;
#[cfg(test)]
mod context_tests;
#[cfg(test)]
mod criss_cross_tests;
#[cfg(test)]
mod deep_blame_tests;
#[cfg(test)]
mod derive_walk_tests;
#[cfg(test)]
mod fault_tests;
#[cfg(test)]
mod flush_tests;
#[cfg(test)]
mod git_map_tests;
#[cfg(test)]
mod index_lease_tests;
#[cfg(test)]
mod land_batch_tests;
#[cfg(test)]
mod land_coord_tests;
#[cfg(test)]
mod land_fast_tests;
#[cfg(test)]
mod land_pipeline_tests;
#[cfg(test)]
mod land_stack_tests;
#[cfg(test)]
mod lazy_tree_tests;
#[cfg(test)]
mod notices_tests;
#[cfg(test)]
mod page_tests;
#[cfg(test)]
mod path_page_tests;
#[cfg(test)]
mod rebase_onto_tests;
#[cfg(test)]
mod sessions_equivalence_tests;
#[cfg(test)]
mod tests;

pub use blame_flight::BlameHold;
/// The comment lexer, public only so the fuzz harness can check it directly.
#[doc(hidden)]
pub use comments::{strip_comments, strip_comments_unchecked};
pub use cursor::MAX_CURSOR_CHARS;
pub use history::{ExtraMatch, LogFilter, LogPage, LogParams};
pub use land::{
    LandQueueBound, GIT_MIRROR_OPERATION, MAX_BATCH, MAX_BATCH_LIFETIME, MAX_BATCH_REPLAY,
    MIN_QUEUE_BATCHES,
};
pub use land_fast::{ConflictPolicy, LandConflicts};
pub use repo::{
    default_derive_jobs, PrefixMatch, Repo, DEFAULT_CACHE_BYTES, MAX_DEFAULT_DERIVE_JOBS,
    MAX_PREFIX_CANDIDATES,
};
pub use search::{SearchIndexReport, SearchQuery};
pub use signatures::{LandPolicy, PolicySet, REBASED_ORIGIN_KIND};

/// The benches' synthetic late-Linux-shaped history, for tests.
#[cfg(test)]
#[path = "../benches/support/wide_history.rs"]
mod wide_history;
#[cfg(test)]
extern crate self as leep_derive;
