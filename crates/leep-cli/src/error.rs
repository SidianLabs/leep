//! CLI error type. Every error has a kind, and the kind alone decides the
//! process exit code and the stable `code` of `--json` errors (ADR 0034).

use std::fmt;

/// Documented process exit codes (ADR 0034). Commands that finish but report
/// an outcome (a land conflict, an unapproved review) return these too.
pub mod exit {
    pub const OK: i32 = 0;
    /// Generic failure: bad input, a refused operation, an authentication
    /// problem. Also "no" from the predicate commands (`search` without a
    /// match, `review status` when not approved).
    pub const USER: i32 = 1;
    /// The command line itself is wrong (clap).
    pub const USAGE: i32 = 2;
    /// The server or the network failed; running the command again may work.
    pub const SERVER: i32 = 3;
    pub const NOT_FOUND: i32 = 4;
    /// Content conflicts to resolve, or a concurrent update that won.
    pub const CONFLICT: i32 = 5;
    /// The requested change is already the state (`commit`, `amend` and
    /// `stash push` with nothing to record).
    pub const NOTHING_TO_DO: i32 = 6;
    /// Killed by SIGINT: shells report 128 + 2. Leep installs no handler.
    pub const INTERRUPTED: i32 = 130;
}

/// What went wrong, as far as a caller deciding what to do next cares.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ErrorKind {
    /// Bad input or a refused operation; also the 4xx answers not listed below.
    User,
    /// 401 or 403: missing, unknown or read-only token.
    Auth,
    /// A short changeset id matches more than one changeset.
    Ambiguous,
    /// The command line does not parse.
    Usage,
    /// The server could not be reached at all.
    Unreachable,
    /// A 5xx answer, a response that does not decode, or bad data from the
    /// server.
    Server,
    /// 408: the request may or may not have taken effect.
    Timeout,
    /// 429 after the client's own retries.
    RateLimited,
    NotFound,
    Conflict,
    NothingToDo,
    /// 503 from a query whose index is too far behind, after the client's
    /// own retries (ADR 0058).
    Indexing,
}

impl ErrorKind {
    /// The stable `code` string of a `--json` error.
    pub fn code(self) -> &'static str {
        match self {
            ErrorKind::User => "user",
            ErrorKind::Auth => "auth",
            ErrorKind::Ambiguous => "ambiguous",
            ErrorKind::Usage => "usage",
            ErrorKind::Unreachable => "unreachable",
            ErrorKind::Server => "server",
            ErrorKind::Timeout => "timeout",
            ErrorKind::RateLimited => "rate_limited",
            ErrorKind::NotFound => "not_found",
            ErrorKind::Conflict => "conflict",
            ErrorKind::NothingToDo => "nothing_to_do",
            ErrorKind::Indexing => "indexing",
        }
    }

    pub fn exit_code(self) -> i32 {
        match self {
            ErrorKind::User | ErrorKind::Auth | ErrorKind::Ambiguous => exit::USER,
            ErrorKind::Usage => exit::USAGE,
            ErrorKind::Unreachable
            | ErrorKind::Server
            | ErrorKind::Timeout
            | ErrorKind::RateLimited
            | ErrorKind::Indexing => exit::SERVER,
            ErrorKind::NotFound => exit::NOT_FOUND,
            ErrorKind::Conflict => exit::CONFLICT,
            ErrorKind::NothingToDo => exit::NOTHING_TO_DO,
        }
    }

    fn retryable_by_default(self) -> bool {
        matches!(
            self,
            ErrorKind::Unreachable
                | ErrorKind::Server
                | ErrorKind::Timeout
                | ErrorKind::RateLimited
                | ErrorKind::Indexing
        )
    }
}

#[derive(Debug)]
pub struct CliError {
    kind: ErrorKind,
    message: String,
    hint: Option<String>,
    retryable: bool,
    /// Full ids an ambiguous short id matched.
    candidates: Vec<String>,
    /// When the server said to retry (an indexing 503).
    retry_after_ms: Option<u64>,
}

pub type Result<T, E = CliError> = std::result::Result<T, E>;

impl CliError {
    pub fn new(kind: ErrorKind, msg: impl Into<String>) -> Self {
        CliError {
            kind,
            message: msg.into(),
            hint: None,
            retryable: kind.retryable_by_default(),
            candidates: Vec::new(),
            retry_after_ms: None,
        }
    }

    /// When the server asked to be asked again.
    pub fn with_retry_after_ms(mut self, ms: u64) -> Self {
        self.retry_after_ms = Some(ms);
        self
    }

    pub fn retry_after_ms(&self) -> Option<u64> {
        self.retry_after_ms
    }

    pub fn user(msg: impl Into<String>) -> Self {
        Self::new(ErrorKind::User, msg)
    }

    /// A server or transport failure that running the command again may fix.
    pub fn server(msg: impl Into<String>) -> Self {
        Self::new(ErrorKind::Server, msg)
    }

    pub fn not_found(msg: impl Into<String>) -> Self {
        Self::new(ErrorKind::NotFound, msg)
    }

    /// Content conflicts: resolving them, not retrying, is the way on.
    pub fn conflict(msg: impl Into<String>) -> Self {
        Self::new(ErrorKind::Conflict, msg)
    }

    /// Someone else moved the same ref first; re-reading and retrying works.
    pub fn lost_race(msg: impl Into<String>) -> Self {
        Self::new(ErrorKind::Conflict, msg).retryable(true)
    }

    pub fn nothing_to_do(msg: impl Into<String>) -> Self {
        Self::new(ErrorKind::NothingToDo, msg)
    }

    pub fn with_hint(mut self, hint: impl Into<String>) -> Self {
        self.hint = Some(hint.into());
        self
    }

    pub fn retryable(mut self, yes: bool) -> Self {
        self.retryable = yes;
        self
    }

    pub fn with_candidates(mut self, candidates: Vec<String>) -> Self {
        self.candidates = candidates;
        self
    }

    /// The same error with a rewritten message: kind, hint and the rest stay.
    pub fn map_message(mut self, f: impl FnOnce(String) -> String) -> Self {
        self.message = f(self.message);
        self
    }

    pub fn kind(&self) -> ErrorKind {
        self.kind
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn hint(&self) -> Option<&str> {
        self.hint.as_deref()
    }

    pub fn is_retryable(&self) -> bool {
        self.retryable
    }

    pub fn candidates(&self) -> &[String] {
        &self.candidates
    }

    pub fn exit_code(&self) -> i32 {
        self.kind.exit_code()
    }

    /// The `--json` form: `{"error":{"code","message","hint","retryable"}}`,
    /// plus `candidates` for an ambiguous short id.
    pub fn to_json(&self) -> serde_json::Value {
        let mut e = serde_json::json!({
            "code": self.kind.code(),
            "message": self.message,
            "hint": self.hint,
            "retryable": self.retryable,
        });
        if !self.candidates.is_empty() {
            e["candidates"] = serde_json::json!(self.candidates);
        }
        if let Some(ms) = self.retry_after_ms {
            e["retry_after_ms"] = serde_json::json!(ms);
        }
        serde_json::json!({ "error": e })
    }

    /// Print `leep: message` plus any candidates and hint on stderr, and
    /// with `json` also the [`to_json`](Self::to_json) document on stdout,
    /// where agents read results (ADR 0034).
    pub fn report(&self, json: bool) {
        if json {
            println!("{}", self.to_json());
        }
        eprintln!("leep: {}", self.message);
        for c in &self.candidates {
            eprintln!("  {c}");
        }
        if let Some(h) = &self.hint {
            eprintln!("hint: {h}");
        }
    }
}

impl fmt::Display for CliError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for CliError {}

impl From<std::io::Error> for CliError {
    fn from(e: std::io::Error) -> Self {
        CliError::user(format!("io error: {e}"))
    }
}

impl From<leep_core::Error> for CliError {
    fn from(e: leep_core::Error) -> Self {
        let kind = match &e {
            leep_core::Error::NotFound(_) => ErrorKind::NotFound,
            leep_core::Error::Conflict(_) => ErrorKind::Conflict,
            _ => ErrorKind::User,
        };
        CliError::new(kind, e.to_string())
    }
}

impl From<serde_json::Error> for CliError {
    fn from(e: serde_json::Error) -> Self {
        CliError::user(format!("json error: {e}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_kind_has_a_documented_code_and_exit() {
        let table = [
            (ErrorKind::User, "user", 1, false),
            (ErrorKind::Auth, "auth", 1, false),
            (ErrorKind::Ambiguous, "ambiguous", 1, false),
            (ErrorKind::Usage, "usage", 2, false),
            (ErrorKind::Unreachable, "unreachable", 3, true),
            (ErrorKind::Server, "server", 3, true),
            (ErrorKind::Timeout, "timeout", 3, true),
            (ErrorKind::RateLimited, "rate_limited", 3, true),
            (ErrorKind::NotFound, "not_found", 4, false),
            (ErrorKind::Conflict, "conflict", 5, false),
            (ErrorKind::NothingToDo, "nothing_to_do", 6, false),
            (ErrorKind::Indexing, "indexing", 3, true),
        ];
        for (kind, code, exit, retry) in table {
            let e = CliError::new(kind, "m");
            assert_eq!(
                (kind.code(), e.exit_code(), e.is_retryable()),
                (code, exit, retry)
            );
        }
    }

    #[test]
    fn json_shape_is_stable() {
        let e = CliError::not_found("revision \"x\" not found").with_hint("try `leep log`");
        assert_eq!(
            e.to_json(),
            serde_json::json!({ "error": {
                "code": "not_found", "message": "revision \"x\" not found",
                "hint": "try `leep log`", "retryable": false,
            }})
        );
        let e = CliError::new(ErrorKind::Ambiguous, "m").with_candidates(vec!["ab".into()]);
        assert_eq!(e.to_json()["error"]["hint"], serde_json::Value::Null);
        assert_eq!(
            e.to_json()["error"]["candidates"],
            serde_json::json!(["ab"])
        );
        assert_eq!(
            CliError::lost_race("m").to_json()["error"]["retryable"],
            true
        );
        let e = CliError::new(ErrorKind::Indexing, "m").with_retry_after_ms(4000);
        assert_eq!(e.to_json()["error"]["retry_after_ms"], 4000);
        assert_eq!(e.to_json()["error"]["code"], "indexing");
        assert!(CliError::user("m").to_json()["error"]
            .get("retry_after_ms")
            .is_none());
    }
}
