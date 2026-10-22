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
pub mod changeset_gc;
pub mod changeset_reach;
mod comments;
mod cursor;
pub mod derived;
mod filter_log;
pub mod gc;
pub mod gc_lease;
pub mod gc_unreachable;
mod history;
pub mod indexing;
pub mod integrity;
mod land;
pub mod land_coord;
mod land_stage;
mod merge;
pub mod owners;
pub mod pack_order;
mod path_history;
mod prefetch;
mod renames;
mod repo;
pub mod review;
mod reviews;
pub mod search;
mod signatures;
mod tree_ops;
pub mod warm;

#[cfg(test)]
mod blame_tests;
#[cfg(test)]
mod bounded_tests;
#[cfg(test)]
mod fault_tests;
#[cfg(test)]
mod flush_tests;
#[cfg(test)]
mod index_lease_tests;
#[cfg(test)]
mod land_batch_tests;
#[cfg(test)]
mod land_coord_tests;
#[cfg(test)]
mod page_tests;
#[cfg(test)]
mod path_page_tests;
#[cfg(test)]
mod tests;

/// The comment lexer, public only so the fuzz harness can check it directly.
#[doc(hidden)]
pub use comments::strip_comments;
pub use cursor::MAX_CURSOR_CHARS;
pub use history::{ExtraMatch, LogFilter, LogPage, LogParams};
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
