//! Leep derived data and repository operations.
//!
//! [`Repo`] wraps a `BlobStore` and a `MetaStore` and implements everything the
//! server exposes: changeset creation, trees, history, diff, blame, merge,
//! rebase and land. Full trees, generation numbers, ancestry indexes and blame
//! are derived data (see [`derived`]) computed iteratively from parents and
//! cached in the `MetaStore`. [`gc`] collects unreferenced blobs. See
//! `docs/ARCHITECTURE.md`.

mod ancestry;
mod blame;
pub mod derived;
pub mod gc;
mod history;
mod land;
mod merge;
mod repo;
mod signatures;
mod tree_ops;

#[cfg(test)]
mod tests;

pub use history::{ExtraMatch, LogFilter};
pub use repo::Repo;
pub use signatures::{LandPolicy, REBASED_ORIGIN_KIND};
