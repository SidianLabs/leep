use thiserror::Error;

use crate::api::LandRefusal;
use crate::ChangesetId;

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
    /// A land id the ref already recorded for another changeset (ADR 0057).
    /// Maps to HTTP 400 like `Invalid`, with the same text, and carries the
    /// record so clients get it as data ([`LandRefusal`]).
    #[error(
        "invalid input: land id {} already landed {}; use a new land id to land {requested}",
        .refusal.land_id,
        .refusal.changeset
    )]
    LandIdReused {
        refusal: Box<LandRefusal>,
        /// The changeset this land asked for.
        requested: ChangesetId,
    },
    #[error("not found: {0}")]
    NotFound(String),
    /// The index a query needs is further behind than the query may scan
    /// without it (ADR 0058); retrying after `retry_after_ms` can succeed.
    /// Maps to HTTP 503 with `Retry-After`.
    #[error("indexing: {kind} is {behind} changesets behind; retry in {}s", retry_after_ms.div_ceil(1000))]
    Indexing {
        /// The index group's lease name, such as `history-tree-v4`.
        kind: String,
        /// Generations between the requested changeset and the newest one
        /// indexed (a lower bound when that was too far to find).
        behind: u64,
        retry_after_ms: u64,
    },
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
