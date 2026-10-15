//! Fault injection for the storage traits (ADR 0021). A dev-dependency only:
//! no shipped binary links it, so injection can never be switched on in
//! production.
//!
//! Wrap any store in [`FaultyBlobStore`] or [`FaultyMetaStore`] and drive it
//! with a seeded [`FaultPlan`]. The same seed and the same sequence of calls
//! inject the same faults, so a failing run replays from its printed seed.

mod blob;
mod meta;
mod plan;
mod rng;

pub use blob::FaultyBlobStore;
pub use meta::FaultyMetaStore;
pub use plan::{is_injected, Fault, FaultPlan, Injected, Op};
pub use rng::{cancel_after_polls, Rng};
