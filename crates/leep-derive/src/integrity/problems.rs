//! What verification and repair report: problems with enough context to act
//! on, a progress hook, and the rule that separates damage from failure.

use std::time::{Duration, Instant};

use leep_core::{ChangesetId, Error, Hash};
use serde::Serialize;

/// One thing wrong with the store. `check` names the phase (`ref`, `history`,
/// `tree`, `blob`, `reflog`, `derived`, `attestation`), `code` the exact
/// finding. The remaining fields say where: the derived kind and row key, the
/// changeset that references or owns the object, a path inside its tree, a ref
/// name.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Problem {
    pub check: &'static str,
    pub code: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub changeset: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub message: String,
}

impl Problem {
    pub fn new(check: &'static str, code: &'static str, message: impl Into<String>) -> Self {
        Problem {
            check,
            code,
            kind: None,
            key: None,
            changeset: None,
            path: None,
            name: None,
            message: message.into(),
        }
    }

    pub fn kind(mut self, kind: &str) -> Self {
        self.kind = Some(kind.to_string());
        self
    }

    pub fn key(mut self, key: &Hash) -> Self {
        self.key = Some(key.to_hex());
        self
    }

    pub fn changeset(mut self, id: ChangesetId) -> Self {
        self.changeset = Some(id.to_string());
        self
    }

    pub fn path(mut self, path: impl Into<String>) -> Self {
        self.path = Some(path.into());
        self
    }

    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }
}

impl std::fmt::Display for Problem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}] {}", self.code, self.message)?;
        let mut ctx = Vec::new();
        if let Some(k) = &self.kind {
            ctx.push(format!("kind {k}"));
        }
        if let Some(k) = &self.key {
            ctx.push(format!("key {k}"));
        }
        if let Some(c) = &self.changeset {
            ctx.push(format!("changeset {c}"));
        }
        if let Some(p) = &self.path {
            ctx.push(format!("path {p}"));
        }
        if let Some(n) = &self.name {
            ctx.push(format!("ref {n}"));
        }
        if !ctx.is_empty() {
            write!(f, " ({})", ctx.join(", "))?;
        }
        Ok(())
    }
}

/// Problems found so far. Only the first `cap` are kept, so a store that is
/// broken everywhere cannot exhaust memory; `total` still counts them all.
#[derive(Debug)]
pub(crate) struct ProblemLog {
    kept: Vec<Problem>,
    total: u64,
    cap: usize,
}

impl ProblemLog {
    pub fn new(cap: usize) -> Self {
        ProblemLog {
            kept: Vec::new(),
            total: 0,
            cap,
        }
    }

    pub fn push(&mut self, p: Problem) {
        self.total += 1;
        if self.kept.len() < self.cap {
            self.kept.push(p);
        }
    }

    pub fn total(&self) -> u64 {
        self.total
    }

    pub fn into_parts(self) -> (Vec<Problem>, u64) {
        (self.kept, self.total)
    }
}

/// Where long operations report what they are doing. The server prints these
/// lines to stderr.
pub trait Progress: Send + Sync {
    fn info(&self, message: &str);
}

/// Discards progress, for tests and library callers.
pub struct Quiet;

impl Progress for Quiet {
    fn info(&self, _: &str) {}
}

/// Rate limiter for progress lines.
pub(crate) struct Ticker {
    last: Instant,
    every: Duration,
}

impl Ticker {
    pub fn new(every: Duration) -> Self {
        Ticker {
            last: Instant::now(),
            every,
        }
    }

    pub fn due(&mut self) -> bool {
        if self.last.elapsed() >= self.every {
            self.last = Instant::now();
            true
        } else {
            false
        }
    }
}

/// How a read failed when the store answered but the data is wrong.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Damage {
    Corrupt,
    Missing,
}

/// Whether `e` says the data is damaged (a finding) rather than that the
/// store could not be used (which stops the run). The stores report damage as
/// `Encoding`, `NotFound`, or a `Storage` error whose text starts with
/// "corrupt" or says "missing"; anything else, such as a lost connection, is
/// not a finding.
pub(crate) fn damage_of(e: &Error) -> Option<Damage> {
    match e {
        Error::Encoding(_) => Some(Damage::Corrupt),
        Error::NotFound(_) => Some(Damage::Missing),
        Error::Storage(m) if m.starts_with("corrupt") => Some(Damage::Corrupt),
        Error::Storage(m) if m.contains("missing") => Some(Damage::Missing),
        _ => None,
    }
}

/// Errors a recomputation can end in without the store being unusable:
/// damaged inputs, or a changeset that does not apply to its parent's tree.
pub(crate) fn recoverable(e: &Error) -> bool {
    damage_of(e).is_some() || matches!(e, Error::Invalid(_))
}
