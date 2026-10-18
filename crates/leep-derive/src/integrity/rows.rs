//! Recomputing derived rows: the part verification and repair share.

use bytes::Bytes;
use leep_core::{Changeset, ChangesetId, Error, Hash, MetaStore, Result};

use crate::derived::{
    self, DerivedKind, Generation, HistoryTree, LastMerge, LogFilterIndex, RootTree, Skiplist,
};
use crate::search::trigram::BlobRow;
use crate::Repo;

/// Rows fetched per listing call.
pub(crate) const PAGE: usize = 500;

async fn recompute_with<K: DerivedKind>(
    repo: &Repo,
    id: ChangesetId,
    cs: &Changeset,
) -> Result<Bytes> {
    // The parents' rows are the stored ones (or the verifier's substitutes for
    // damaged ones): the same inputs derivation uses.
    let mut parents = Vec::with_capacity(cs.parents.len());
    for p in &cs.parents {
        parents.push(derived::derive::<K>(repo, *p).await?);
    }
    K::derive(repo, id, cs, &parents).await
}

/// The row `kind` holds for `id`, computed by the code that derives it.
pub(crate) async fn recompute(
    kind: &str,
    repo: &Repo,
    id: ChangesetId,
    cs: &Changeset,
) -> Result<Bytes> {
    if kind == Generation::NAME {
        recompute_with::<Generation>(repo, id, cs).await
    } else if kind == Skiplist::NAME {
        recompute_with::<Skiplist>(repo, id, cs).await
    } else if kind == LastMerge::NAME {
        recompute_with::<LastMerge>(repo, id, cs).await
    } else if kind == RootTree::NAME {
        recompute_with::<RootTree>(repo, id, cs).await
    } else if kind == HistoryTree::NAME {
        recompute_with::<HistoryTree>(repo, id, cs).await
    } else if kind == LogFilterIndex::NAME {
        recompute_with::<LogFilterIndex>(repo, id, cs).await
    } else {
        Err(Error::Other(format!("{kind} is not a per-changeset kind")))
    }
}

/// The row a blob's bytes should have.
pub(crate) fn expected_blob_row(data: &[u8]) -> Bytes {
    Bytes::from(BlobRow::of(data).encode())
}

fn to_hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn hex_prefix(b: &[u8]) -> String {
    let n = b.len().min(16);
    let mut s = to_hex(&b[..n]);
    if b.len() > n {
        s.push_str("...");
    }
    s
}

fn u64_of(b: &[u8]) -> Option<u64> {
    <[u8; 8]>::try_from(b).ok().map(u64::from_be_bytes)
}

fn shape(b: &[u8]) -> String {
    format!("{} bytes ({})", b.len(), hex_prefix(b))
}

/// Say how a stored row differs from the recomputed one, for a person.
pub(crate) fn describe(kind: &str, stored: &[u8], expected: &[u8]) -> String {
    if kind == Generation::NAME || kind == LastMerge::NAME {
        match (u64_of(stored), u64_of(expected)) {
            (Some(s), Some(e)) => format!("stored {s}, recomputed {e}"),
            _ => format!("stored {}, recomputed {}", shape(stored), shape(expected)),
        }
    } else if kind == RootTree::NAME || kind == HistoryTree::NAME {
        if stored.len() == 32 && expected.len() == 32 {
            format!("stored {}, recomputed {}", to_hex(stored), to_hex(expected))
        } else {
            format!("stored {}, recomputed {}", shape(stored), shape(expected))
        }
    } else if kind == Skiplist::NAME {
        match (Skiplist::decode(stored), Skiplist::decode(expected)) {
            (Ok(s), Ok(e)) => {
                let at = (0..s.len().max(e.len())).find(|&i| s.get(i) != e.get(i));
                match at {
                    Some(i) => format!(
                        "entry {i} differs: stored {}, recomputed {}",
                        s.get(i).map_or("none".into(), |x| format!(
                            "{} at generation {}",
                            x.id, x.generation
                        )),
                        e.get(i).map_or("none".into(), |x| format!(
                            "{} at generation {}",
                            x.id, x.generation
                        )),
                    ),
                    None => "entries are equal".into(),
                }
            }
            _ => format!("stored {}, recomputed {}", shape(stored), shape(expected)),
        }
    } else {
        format!("stored {}, recomputed {}", shape(stored), shape(expected))
    }
}

/// Which rows of a kind a scan visits: all of them, or a sample taken from
/// evenly spaced places in key order. Keys are hashes, so the places are
/// unbiased, and the same sample is taken every time.
pub(crate) struct RowScan {
    kind: &'static str,
    full: bool,
    windows: usize,
    per_window: usize,
    window: usize,
    taken: usize,
    cursor: Option<Hash>,
    done: bool,
}

const WINDOWS: usize = 8;

impl RowScan {
    pub fn new(kind: &'static str, full: bool, sample: usize) -> Self {
        RowScan {
            kind,
            full,
            windows: WINDOWS,
            per_window: sample.div_ceil(WINDOWS).max(1),
            window: 0,
            taken: 0,
            cursor: None,
            done: sample == 0 && !full,
        }
    }

    fn window_start(&self) -> Option<Hash> {
        (self.window > 0).then(|| {
            let mut h = [0u8; 32];
            h[0] = (self.window * 256 / self.windows) as u8;
            Hash(h)
        })
    }

    /// The next page of rows, or `None` when the scan is over.
    pub async fn next(&mut self, meta: &dyn MetaStore) -> Result<Option<Vec<(Hash, Bytes)>>> {
        loop {
            if self.done {
                return Ok(None);
            }
            let limit = if self.full {
                PAGE
            } else {
                PAGE.min(self.per_window - self.taken)
            };
            let page = meta.list_derived(self.kind, self.cursor, limit).await?;
            if page.is_empty() {
                self.advance_window();
                continue;
            }
            self.cursor = page.last().map(|(k, _)| *k);
            if !self.full {
                self.taken += page.len();
                if self.taken >= self.per_window {
                    self.advance_window();
                }
            }
            return Ok(Some(page));
        }
    }

    fn advance_window(&mut self) {
        if self.full {
            self.done = true;
            return;
        }
        self.window += 1;
        self.taken = 0;
        if self.window >= self.windows {
            self.done = true;
            return;
        }
        // Never go backwards: windows over a sparse kind would overlap.
        // `after` is exclusive, so a start key itself is skipped; harmless
        // for a sample, and a hash is never all zeros but its first byte.
        let start = self.window_start();
        if start > self.cursor {
            self.cursor = start;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use leep_core::memory::MemoryMetaStore;

    async fn store(n: u32) -> MemoryMetaStore {
        let m = MemoryMetaStore::new();
        for i in 0..n {
            let key = Hash::of_blob(&i.to_be_bytes());
            m.put_derived("k", &key, Bytes::from(i.to_be_bytes().to_vec()))
                .await
                .unwrap();
        }
        m
    }

    async fn collect(m: &MemoryMetaStore, full: bool, sample: usize) -> Vec<Hash> {
        let mut scan = RowScan::new("k", full, sample);
        let mut keys = Vec::new();
        while let Some(page) = scan.next(m).await.unwrap() {
            keys.extend(page.into_iter().map(|(k, _)| k));
        }
        keys
    }

    #[tokio::test]
    async fn a_full_scan_returns_every_row_once_in_order() {
        let m = store(1234).await;
        let keys = collect(&m, true, 0).await;
        assert_eq!(keys.len(), 1234);
        assert!(keys.windows(2).all(|w| w[0] < w[1]));
    }

    #[tokio::test]
    async fn a_sample_is_small_spread_out_and_repeatable() {
        let m = store(5000).await;
        let a = collect(&m, false, 64).await;
        assert!(a.len() >= 64 && a.len() <= 64 + WINDOWS, "{}", a.len());
        assert!(a.windows(2).all(|w| w[0] < w[1]), "ascending, no repeats");
        let spread: std::collections::HashSet<u8> = a.iter().map(|k| k.0[0] / 32).collect();
        assert!(spread.len() >= 6, "{spread:?}");
        assert_eq!(a, collect(&m, false, 64).await);
        assert!(collect(&m, false, 0).await.is_empty());
    }

    #[tokio::test]
    async fn a_sample_of_a_small_kind_is_all_of_it_without_repeats() {
        let m = store(20).await;
        let keys = collect(&m, false, 256).await;
        assert_eq!(keys.len(), 20);
        assert!(keys.windows(2).all(|w| w[0] < w[1]));
        assert!(collect(&store(0).await, false, 10).await.is_empty());
    }
}
