//! MCP (Model Context Protocol) server for Leep.
//!
//! A thin client of `leep-server`: it speaks JSON-RPC over stdio to the agent
//! and HTTP to the server through `leep_cli::client::Client`. It never opens
//! a store itself.

mod config;
mod protocol;
pub mod tools;

pub use config::Config;
pub use protocol::{Server, PROTOCOL_VERSION};
