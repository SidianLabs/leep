//! CLI error type with exit-code semantics.

use std::fmt;

/// Exit code 1 for [`CliError::User`], 2 for [`CliError::Server`].
#[derive(Debug)]
pub enum CliError {
    /// Bad input, refused operation, or a 4xx answer from the server.
    User(String),
    /// Network failure or 5xx answer from the server.
    Server(String),
}

pub type Result<T, E = CliError> = std::result::Result<T, E>;

impl CliError {
    pub fn user(msg: impl Into<String>) -> Self {
        CliError::User(msg.into())
    }

    pub fn server(msg: impl Into<String>) -> Self {
        CliError::Server(msg.into())
    }

    pub fn exit_code(&self) -> i32 {
        match self {
            CliError::User(_) => 1,
            CliError::Server(_) => 2,
        }
    }
}

impl fmt::Display for CliError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CliError::User(m) | CliError::Server(m) => f.write_str(m),
        }
    }
}

impl std::error::Error for CliError {}

impl From<std::io::Error> for CliError {
    fn from(e: std::io::Error) -> Self {
        CliError::User(format!("io error: {e}"))
    }
}

impl From<leep_core::Error> for CliError {
    fn from(e: leep_core::Error) -> Self {
        CliError::User(e.to_string())
    }
}

impl From<serde_json::Error> for CliError {
    fn from(e: serde_json::Error) -> Self {
        CliError::User(format!("json error: {e}"))
    }
}
