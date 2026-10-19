//! Plan and cursor of `delete_derived_step` for both backends (ADR 0052).
//!
//! A large drop walks the table in physical order, a range of pages (rowids
//! in SQLite) per statement, deleting the rows of the kinds it meets. Each
//! page is read once, in order, and changed once. Deleting in key order
//! instead touches pages at random: every batch re-reads pages from disk,
//! and Postgres logs a full copy of a page each time it is first changed
//! after a checkpoint, again and again for the same page. A small kind is
//! deleted in key order from the primary key instead, so the rest of the
//! table is never read.

use std::time::Duration;

use leep_core::{DeleteStep, Error, Result};

/// Rows deleted per key-order statement; small in tests so a test kind
/// takes several.
pub(crate) const KEY_BATCH: i64 = if cfg!(test) { 100 } else { 10_000 };

/// A walk starts with this many pages per statement and adapts towards
/// [`PACE`]: few enough that a test table takes several steps.
pub(crate) const FIRST_SPAN: u64 = 32;
const MIN_SPAN: u64 = 8;
const MAX_SPAN: u64 = 1 << 16;
/// Target statement length: long enough that per-statement overhead is
/// noise, short enough that locks are held briefly, progress shows, and
/// steps stay under sqlx's one-second slow-statement warning.
const PACE: (Duration, Duration) = (Duration::from_millis(250), Duration::from_secs(1));

/// Walk the table rather than the key index when the kinds hold at least
/// one row per four pages (rowids): a row deleted from the index costs a
/// random read, a page walked a sequential one, about four times cheaper
/// (Postgres's default `random_page_cost`).
pub(crate) fn walks(rows: u64, pages: u64) -> bool {
    rows.saturating_mul(4) >= pages
}

/// The next span after a statement over `span` units took `took`.
pub(crate) fn next_span(span: u64, took: Duration) -> u64 {
    if took < PACE.0 {
        (span * 2).min(MAX_SPAN)
    } else if took > PACE.1 {
        (span / 2).max(MIN_SPAN)
    } else {
        span
    }
}

/// Where a drop is. Opaque to callers; it only has to survive between the
/// steps of one drop, so the encoding can change freely.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Cursor {
    /// Walking the table: the next statement covers `[at, at + span)`.
    Walk { at: u64, span: u64 },
    /// Deleting `kinds[kind]` in key order after `after` (empty: from the
    /// start); `done` of `total` rows deleted so far.
    Keys {
        kind: usize,
        after: Vec<u8>,
        done: u64,
        total: u64,
    },
}

impl Cursor {
    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        match self {
            Cursor::Walk { at, span } => {
                out.push(b'W');
                out.extend(at.to_be_bytes());
                out.extend(span.to_be_bytes());
            }
            Cursor::Keys {
                kind,
                after,
                done,
                total,
            } => {
                out.push(b'K');
                out.extend((*kind as u64).to_be_bytes());
                out.extend(done.to_be_bytes());
                out.extend(total.to_be_bytes());
                out.extend(after);
            }
        }
        out
    }

    pub(crate) fn decode(b: &[u8]) -> Result<Self> {
        let bad = || Error::Invalid("not a derived-row delete cursor".into());
        let u64_at = |i: usize| -> Result<u64> {
            let s = b.get(i..i + 8).ok_or_else(bad)?;
            Ok(u64::from_be_bytes(s.try_into().unwrap()))
        };
        match b.first() {
            Some(b'W') if b.len() == 17 => Ok(Cursor::Walk {
                at: u64_at(1)?,
                span: u64_at(9)?.clamp(MIN_SPAN, MAX_SPAN),
            }),
            Some(b'K') if b.len() >= 25 => Ok(Cursor::Keys {
                kind: usize::try_from(u64_at(1)?).map_err(|_| bad())?,
                done: u64_at(9)?,
                total: u64_at(17)?,
                after: b[25..].to_vec(),
            }),
            _ => Err(bad()),
        }
    }
}

/// The first cursor of a drop of `rows` rows from a table of `pages`
/// pages; `None` when there is nothing to delete.
pub(crate) fn plan(rows: u64, pages: u64) -> Option<Cursor> {
    if rows == 0 {
        None
    } else if walks(rows, pages) {
        Some(Cursor::Walk {
            at: 0,
            span: FIRST_SPAN,
        })
    } else {
        Some(Cursor::Keys {
            kind: 0,
            after: Vec::new(),
            done: 0,
            total: rows,
        })
    }
}

/// A step that deleted nothing and leaves nothing to do.
pub(crate) fn nothing_left(kinds: &[&str]) -> DeleteStep {
    DeleteStep {
        deleted: vec![0; kinds.len()],
        next: None,
        done: 0,
        total: 0,
        unit: "rows",
    }
}

/// Add each deleted row's kind to its count in `deleted`.
pub(crate) fn tally(
    kinds: &[&str],
    deleted: &mut [u64],
    gone: impl IntoIterator<Item = (String, u64)>,
) {
    for (kind, n) in gone {
        if let Some(i) = kinds.iter().position(|k| *k == kind) {
            deleted[i] += n;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursors_round_trip() {
        for c in [
            Cursor::Walk { at: 0, span: 32 },
            Cursor::Walk {
                at: u64::from(u32::MAX) + 7,
                span: MAX_SPAN,
            },
            Cursor::Keys {
                kind: 1,
                after: vec![],
                done: 0,
                total: 3,
            },
            Cursor::Keys {
                kind: 0,
                after: vec![9; 32],
                done: 10_000,
                total: 25_000_000,
            },
        ] {
            assert_eq!(Cursor::decode(&c.encode()).unwrap(), c);
        }
        for junk in [&b""[..], b"W", b"K123", b"X0000000000000000"] {
            assert!(matches!(Cursor::decode(junk), Err(Error::Invalid(_))));
        }
    }

    #[test]
    fn the_plan_walks_only_a_large_share_of_the_table() {
        assert_eq!(plan(0, 100), None);
        // history-node-v3 and -tree-v3 on Linux: 26.9M rows in 4M pages.
        assert!(matches!(
            plan(26_900_000, 4_000_000),
            Some(Cursor::Walk { .. })
        ));
        // A few thousand rows of a small kind in the same table.
        assert!(matches!(plan(5_000, 4_000_000), Some(Cursor::Keys { .. })));
        assert!(walks(1, 4) && !walks(1, 5));
    }

    #[test]
    fn the_span_adapts_within_bounds() {
        assert_eq!(next_span(32, Duration::from_millis(10)), 64);
        assert_eq!(next_span(1024, Duration::from_millis(600)), 1024);
        assert_eq!(next_span(1024, Duration::from_secs(5)), 512);
        assert_eq!(next_span(MAX_SPAN, Duration::ZERO), MAX_SPAN);
        assert_eq!(next_span(MIN_SPAN, Duration::from_secs(60)), MIN_SPAN);
    }
}
