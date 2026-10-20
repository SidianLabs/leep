//! Offline verification and repair of a store (ADR 0025): `verify` checks
//! reachability and recomputes derived rows to compare them with what is
//! stored; `repair` overwrites rows that differ, or drops a kind so it derives
//! again on demand. Everything runs against stores opened directly, never
//! through the HTTP API.

pub mod kinds;
mod problems;
pub mod repair;
mod rows;
mod shadow;
pub mod upgrade;
pub mod verify;
mod walk;

#[cfg(test)]
mod tests;

pub use problems::{Problem, Progress, Quiet};
pub use repair::{drop_kinds, rederive, DropReport, RederiveOptions, RederiveReport, Scope};
pub use upgrade::{
    plan_upgrade, plan_upgrade_with, run_upgrade, CurrentKind, IndexStatus, PlanOptions,
    RetiredKind, UpgradeOptions, UpgradePlan, UpgradeReport,
};
pub use verify::{verify, KindStats, SignatureCheck, Stats, VerifyOptions, VerifyReport};
