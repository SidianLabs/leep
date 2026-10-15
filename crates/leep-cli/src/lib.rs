//! `leep`: command-line client for Leep.
//!
//! Depends only on `leep-core` for types; everything else is spoken over the
//! HTTP API documented in `leep_core::api`.

pub mod cache;
pub mod cli;
pub mod client;
pub mod commands;
pub mod error;
pub mod fmt;
pub mod materialize;
pub mod state;
pub mod status;
