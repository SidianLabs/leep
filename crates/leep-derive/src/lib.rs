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
mod blame;
mod comments;
pub mod derived;
pub mod gc;
mod history;
pub mod integrity;
mod land;
mod merge;
pub mod owners;
pub mod pack_order;
mod path_history;
mod renames;
mod repo;
pub mod review;
mod reviews;
pub mod search;
mod signatures;
mod tree_ops;
pub mod warm;

#[cfg(test)]
mod fault_tests;
#[cfg(test)]
mod tests;

/// The comment lexer, public only so the fuzz harness can check it directly.
#[doc(hidden)]
pub use comments::strip_comments;
pub use history::{ExtraMatch, LogFilter};
pub use repo::Repo;
pub use search::{SearchIndexReport, SearchQuery};
pub use signatures::{LandPolicy, PolicySet, REBASED_ORIGIN_KIND};
