//! Opaque page cursors for the list endpoints: log, code search and the ref
//! log. A page that stops short of the end carries a `next` cursor; sending it
//! back as `after` with the same query resumes exactly where the page ended.
//!
//! The text is `base64url(version | kind | fingerprint | postcard(body))`.
//! Clients must not look inside: the layout may change between server
//! versions, and a cursor from an older layout is refused (the client starts
//! over). Cursors are untrusted input. The text is size-bounded before it is
//! decoded, the fingerprint binds a cursor to the query that made it, and any
//! failure is [`Error::Invalid`] (400).

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use leep_core::api::RefLogResponse;
use leep_core::{ChangesetId, Error, Hash, Result};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

/// Longest cursor accepted or produced, in characters. Keeps a cursor well
/// inside the URL limits of common proxies.
pub const MAX_CURSOR_CHARS: usize = 4096;

/// Layout version, the first byte of every cursor.
const VERSION: u8 = 1;

/// Bytes of the query digest a cursor carries.
const FINGERPRINT_BYTES: usize = 16;

/// Most queued changesets a log cursor carries. A wider queue falls back to
/// [`LogResume::Below`], which is correct but rescans from the tip.
pub(crate) const MAX_FRONTIER: usize = 64;

/// Which endpoint a cursor belongs to, so one is never replayed on another.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum CursorKind {
    Log = 1,
    Search = 2,
    RefLog = 3,
}

/// Digest of the query that made a cursor. A cursor sent with different
/// parameters would resume some other walk, so it is refused instead.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Fingerprint([u8; FINGERPRINT_BYTES]);

impl Fingerprint {
    /// Digest of `query`, any serializable value that names every parameter
    /// that changes the walk (and none that only size the page).
    pub(crate) fn of<T: Serialize>(kind: CursorKind, query: &T) -> Self {
        let mut bytes = vec![kind as u8];
        bytes.extend(postcard::to_stdvec(query).expect("query encodes"));
        let h = Hash::of_blob(&bytes);
        let mut fp = [0u8; FINGERPRINT_BYTES];
        fp.copy_from_slice(&h.as_bytes()[..FINGERPRINT_BYTES]);
        Fingerprint(fp)
    }
}

/// The cursor text for `body`, or `None` if it would exceed
/// [`MAX_CURSOR_CHARS`].
pub(crate) fn encode<T: Serialize>(kind: CursorKind, fp: Fingerprint, body: &T) -> Option<String> {
    let mut bytes = vec![VERSION, kind as u8];
    bytes.extend_from_slice(&fp.0);
    bytes.extend(postcard::to_stdvec(body).ok()?);
    let text = URL_SAFE_NO_PAD.encode(bytes);
    (text.len() <= MAX_CURSOR_CHARS).then_some(text)
}

/// Decode cursor text made by [`encode`] for the same kind and query.
pub(crate) fn decode<T: DeserializeOwned>(
    text: &str,
    kind: CursorKind,
    fp: Fingerprint,
) -> Result<T> {
    let bad = |why: &str| Error::Invalid(format!("bad cursor: {why}"));
    if text.len() > MAX_CURSOR_CHARS {
        return Err(bad(&format!("longer than {MAX_CURSOR_CHARS} characters")));
    }
    let bytes = URL_SAFE_NO_PAD
        .decode(text)
        .map_err(|_| bad("not base64url"))?;
    let header = 2 + FINGERPRINT_BYTES;
    if bytes.len() < header {
        return Err(bad("too short"));
    }
    if bytes[0] != VERSION {
        return Err(bad(
            "made by a different server version; start again without `after`",
        ));
    }
    if bytes[1] != kind as u8 {
        return Err(bad("it belongs to a different endpoint"));
    }
    if bytes[2..header] != fp.0 {
        return Err(bad(
            "it belongs to a different query; resend the parameters of the request that returned it",
        ));
    }
    let (body, rest) =
        postcard::take_from_bytes::<T>(&bytes[header..]).map_err(|_| bad("malformed body"))?;
    if !rest.is_empty() {
        return Err(bad("trailing bytes"));
    }
    Ok(body)
}

/// Where the next log page starts.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum LogResume {
    /// The walk's queue when the page ended. Every walk yields changesets in
    /// descending `(generation, id)` and queues only ancestors of what it
    /// popped, so everything it has yet to yield is at or below the queue,
    /// and everything it yielded ranks above it: a walk seeded with the queue
    /// continues exactly as the original would.
    Frontier {
        entries: Vec<FrontierEntry>,
        /// The filter walk had switched to the `log-filter-v1` index (its
        /// entries may carry bounds).
        indexed: bool,
    },
    /// Restart from the tip and skip every changeset ranking at or above
    /// this one. The fallback for a queue too wide to carry.
    Below { generation: u64, id: ChangesetId },
}

/// One queued changeset of a log walk.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct FrontierEntry {
    pub(crate) id: ChangesetId,
    /// `log --follow`: the file's name at `id`.
    pub(crate) name: Option<String>,
    /// The filter walk's bound on a merge's side: `(stop generation, stop
    /// id, the merge's first parent)`.
    pub(crate) bound: Option<(u64, ChangesetId, ChangesetId)>,
}

impl FrontierEntry {
    pub(crate) fn id(id: ChangesetId) -> Self {
        FrontierEntry {
            id,
            name: None,
            bound: None,
        }
    }
}

/// How a log walk starts: from the tip, or from a page's [`LogResume`].
#[derive(Clone, Debug, Default)]
pub(crate) struct LogFrom {
    /// Seed the queue with these instead of the tip.
    pub(crate) seeds: Option<Vec<FrontierEntry>>,
    /// The seeds came from the filter walk's indexed phase.
    pub(crate) indexed: bool,
    /// Yield only changesets ranking below this `(generation, id)`.
    pub(crate) below: Option<(u64, ChangesetId)>,
}

impl LogFrom {
    pub(crate) fn tip() -> Self {
        LogFrom::default()
    }

    pub(crate) fn admits(&self, generation: u64, id: ChangesetId) -> bool {
        self.below.is_none_or(|b| (generation, id) < b)
    }
}

impl From<LogResume> for LogFrom {
    fn from(r: LogResume) -> Self {
        match r {
            LogResume::Frontier { entries, indexed } => LogFrom {
                seeds: Some(entries),
                indexed,
                below: None,
            },
            LogResume::Below { generation, id } => LogFrom {
                seeds: None,
                indexed: false,
                below: Some((generation, id)),
            },
        }
    }
}

/// A log page from one walk: the entries, and the queue left behind (`None`
/// when the walk is exhausted).
pub(crate) struct LogPageRaw {
    pub(crate) entries: Vec<leep_core::api::ChangesetInfo>,
    pub(crate) rest: Option<(Vec<FrontierEntry>, bool)>,
}

/// Where the next search page starts: after line `line` of `path` in the
/// tree of `id`. `line == u64::MAX` means after the whole file.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SearchPos {
    pub(crate) id: ChangesetId,
    pub(crate) path: String,
    pub(crate) line: u64,
}

/// Where the next ref log page starts: entries with a smaller `seq`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct RefLogPos {
    pub(crate) before: u64,
}

impl crate::Repo {
    /// One page of the ref log (every ref, or the ref `name`), newest first.
    /// `before` is the old `seq` bound; `after` the `next` cursor of the
    /// previous page; giving both is `Invalid`.
    pub async fn ref_log_page(
        &self,
        name: Option<&str>,
        before: Option<u64>,
        after: Option<&str>,
        limit: usize,
    ) -> Result<RefLogResponse> {
        let fp = Fingerprint::of(CursorKind::RefLog, &name);
        let before = match (before, after) {
            (Some(_), Some(_)) => {
                return Err(Error::Invalid("give `before` or `after`, not both".into()))
            }
            (before, None) => before,
            (None, Some(text)) => Some(decode::<RefLogPos>(text, CursorKind::RefLog, fp)?.before),
        };
        // One more than the page, to tell a full last page from a middle one.
        let mut entries = self
            .meta()
            .ref_log(name, before, limit.saturating_add(1))
            .await?;
        let mut next = None;
        if entries.len() > limit {
            entries.truncate(limit);
            next = entries
                .last()
                .and_then(|e| encode(CursorKind::RefLog, fp, &RefLogPos { before: e.seq }));
        }
        Ok(RefLogResponse { entries, next })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fp(q: &str) -> Fingerprint {
        Fingerprint::of(CursorKind::RefLog, &q)
    }

    #[test]
    fn round_trips_and_binds_to_kind_and_query() {
        let body = RefLogPos { before: 42 };
        let text = encode(CursorKind::RefLog, fp("a"), &body).unwrap();
        assert!(text
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'));
        let back: RefLogPos = decode(&text, CursorKind::RefLog, fp("a")).unwrap();
        assert_eq!(back, body);
        for err in [
            decode::<RefLogPos>(&text, CursorKind::RefLog, fp("b")).unwrap_err(),
            decode::<RefLogPos>(&text, CursorKind::Search, fp("a")).unwrap_err(),
        ] {
            assert!(matches!(err, Error::Invalid(_)), "{err}");
        }
    }

    #[test]
    fn garbage_is_invalid_and_never_panics() {
        let f = fp("q");
        let long = "A".repeat(MAX_CURSOR_CHARS + 1);
        let good = encode(CursorKind::RefLog, f, &RefLogPos { before: 7 }).unwrap();
        let mut cases: Vec<String> = vec![
            String::new(),
            "!!!".into(),
            "AAAA".into(),
            long,
            format!("{good}AA"),
        ];
        // Every truncation and a flipped byte in each position.
        for n in 0..good.len() {
            cases.push(good[..n].to_string());
        }
        let raw = URL_SAFE_NO_PAD.decode(&good).unwrap();
        for i in 0..raw.len() {
            let mut b = raw.clone();
            b[i] ^= 0x5a;
            cases.push(URL_SAFE_NO_PAD.encode(b));
        }
        for c in cases {
            if c == good {
                continue;
            }
            match decode::<RefLogPos>(&c, CursorKind::RefLog, f) {
                Err(Error::Invalid(_)) => {}
                // A flipped bit inside the varint can still decode.
                Ok(_) => {}
                Err(e) => panic!("{c:?}: {e}"),
            }
        }
        let body = LogResume::Frontier {
            entries: vec![FrontierEntry::id(ChangesetId(Hash::of_blob(b"x")))],
            indexed: false,
        };
        let text = encode(CursorKind::Log, f, &body).unwrap();
        // A log cursor read as a ref log cursor is refused by kind.
        assert!(decode::<RefLogPos>(&text, CursorKind::RefLog, f).is_err());
    }

    #[test]
    fn oversized_bodies_are_not_encoded() {
        let entries: Vec<FrontierEntry> = (0..200u32)
            .map(|i| FrontierEntry::id(ChangesetId(Hash::of_blob(&i.to_le_bytes()))))
            .collect();
        let body = LogResume::Frontier {
            entries,
            indexed: false,
        };
        assert!(encode(CursorKind::Log, fp("q"), &body).is_none());
    }
}
