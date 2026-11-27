//! Loose objects a repack leaves where they are (storage PR 7 stage 2,
//! ADR 0108). The server's segment blobs (derived-data segments, GC mark
//! segments) are compressed already, never a delta base and short-lived:
//! packed, a GC delete only tombstones their bytes until a merge drops
//! them; loose, it unlinks them at once. The store does not know those
//! kinds (`leep-store` does not depend on `leep-derive`), so the caller
//! passes their byte prefixes as data.

use std::io::{ErrorKind, Read};
use std::path::Path;
use std::sync::Arc;

use leep_core::{Error, Hash, Result};

/// Byte prefixes of loose objects a repack leaves loose unless its hints
/// list them. Only an object no walk lists is left out, so a file a
/// changeset names that happens to start with one (stored before ADR 0041
/// refused such uploads) is packed as before; one no walk lists merely
/// stays loose. Content addressing is untouched either way.
#[derive(Clone, Default)]
pub struct LeaveLoose(Arc<[Vec<u8>]>);

impl LeaveLoose {
    pub fn new(prefixes: impl IntoIterator<Item = Vec<u8>>) -> Self {
        LeaveLoose(prefixes.into_iter().filter(|p| !p.is_empty()).collect())
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// The bytes to read of an object to decide.
    pub fn head_len(&self) -> usize {
        self.0.iter().map(Vec::len).max().unwrap_or(0)
    }

    /// Whether bytes that begin with `head` (at least [`head_len`] of them,
    /// or the whole object) start with one of the prefixes.
    ///
    /// [`head_len`]: Self::head_len
    pub fn matches(&self, head: &[u8]) -> bool {
        self.0.iter().any(|p| head.starts_with(p))
    }
}

impl std::fmt::Debug for LeaveLoose {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list()
            .entries(self.0.iter().map(|p| String::from_utf8_lossy(p)))
            .finish()
    }
}

/// Whether the loose file at `path` starts with one of the prefixes; a
/// file that is gone does not (the repack then finds it vanished).
pub(crate) fn file_matches(path: &Path, leave: &LeaveLoose) -> Result<bool> {
    let n = leave.head_len();
    let file = match std::fs::File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok(false),
        Err(e) => return Err(Error::Storage(format!("{}: {e}", path.display()))),
    };
    let mut head = Vec::with_capacity(n);
    file.take(n as u64)
        .read_to_end(&mut head)
        .map_err(|e| Error::Storage(format!("{}: {e}", path.display())))?;
    Ok(leave.matches(&head))
}

/// The loose objects among `hashes` that start with one of the prefixes.
pub(crate) fn matching<'a>(
    hashes: impl IntoIterator<Item = &'a Hash>,
    leave: &LeaveLoose,
    path_for: impl Fn(&Hash) -> std::path::PathBuf,
) -> Result<Vec<Hash>> {
    let mut out = Vec::new();
    if leave.is_empty() {
        return Ok(out);
    }
    for h in hashes {
        if file_matches(&path_for(h), leave)? {
            out.push(*h);
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_only_a_whole_prefix() {
        let l = LeaveLoose::new([b"leep:a\0".to_vec(), b"leep:bb\0".to_vec(), Vec::new()]);
        assert_eq!(l.head_len(), 8);
        assert!(l.matches(b"leep:a\0rest"));
        assert!(l.matches(b"leep:bb\0"));
        assert!(!l.matches(b"leep:a"));
        assert!(!l.matches(b"leep:b\0"));
        assert!(!l.matches(b""));
        assert!(!LeaveLoose::default().matches(b"leep:a\0"));
        assert!(LeaveLoose::default().is_empty());
    }
}
