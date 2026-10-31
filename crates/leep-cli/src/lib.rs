//! `leep`: command-line client for Leep.
//!
//! Depends only on `leep-core` for types; everything else is spoken over the
//! HTTP API documented in `leep_core::api`.

// Output goes through `leep_core::outln!`, which exits quietly when the
// reader of stdout has gone instead of panicking like `println!`.
#![warn(clippy::print_stdout)]

pub mod bisect;
pub mod cache;
pub mod cli;
pub mod client;
pub mod commands;
pub mod error;
pub mod fmt;
pub mod help;
pub mod identity;
pub mod markers;
pub mod materialize;
pub mod overlay;
pub mod sign;
pub mod snapshot;
pub mod state;
pub mod status;
pub mod term;
