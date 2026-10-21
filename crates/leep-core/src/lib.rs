//! Leep core: the data model and storage contracts shared by every Leep crate.
//!
//! Everything a changeset refers to is immutable and content-addressed, so any
//! layer may cache it forever. The only mutable state is refs, which change by
//! compare-and-swap. See `docs/ARCHITECTURE.md`.

pub mod api;
pub mod changeset;
pub mod error;
pub mod hash;
pub mod intern;
pub mod memory;
pub mod path;
pub mod prefix;
pub mod refname;
pub mod rename;
pub mod review;
pub mod signature;
pub mod store;
pub mod tree;

pub use changeset::{Changeset, ChangesetId, FileChange, FileKind};
pub use error::{Error, Result};
pub use hash::Hash;
pub use path::RepoPath;
pub use store::{
    check_land_key, check_ref_chain, land_id_taken, BlobMeta, BlobStore, DeleteStep, DerivedRow,
    LandId, LandKey, LandRecord, Lease, LeaseClaim, LeaseKey, LeaseTiming, MetaStore, RefLogEntry,
    RefStep, RefUpdateContext, RowCount, INDEX_LEASE_SCOPE, LAND_LEASE_SCOPE,
};
pub use tree::{TreeEntry, TreeNode};
