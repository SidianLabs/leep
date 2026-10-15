use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("invalid path {0:?}: {1}")]
    InvalidPath(String, &'static str),
    #[error("invalid hash: {0}")]
    InvalidHash(String),
    /// Caller error: malformed or inconsistent input. Maps to HTTP 400.
    #[error("invalid input: {0}")]
    Invalid(String),
    /// A concurrent update won repeatedly; retrying later can succeed. Maps
    /// to HTTP 409.
    #[error("conflict: {0}")]
    Conflict(String),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("encoding error: {0}")]
    Encoding(String),
    #[error("storage error: {0}")]
    Storage(String),
    #[error("{0}")]
    Other(String),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl From<postcard::Error> for Error {
    fn from(e: postcard::Error) -> Self {
        Error::Encoding(e.to_string())
    }
}
