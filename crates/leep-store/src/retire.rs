//! Durable retirement of replaced packs (ADR 0080). A repack that publishes
//! a pack holding every live object of the packs it merged records them in
//! `packs/retired` (a bucket's `packs/.retired`): from then on no view
//! scanned after reading the record searches or plans them, in any process
//! and after any restart, and a holder of the repack lock (or lease) deletes
//! them once they have been retired for a fence. Before this, "replaced;
//! delete after the fence" lived only in the memory of the process that
//! merged them, so a restart inside the fence left them published, searched
//! and merged again.
//!
//! The record is text, one `<pack id> <unix seconds>\n` line per retired
//! pack. Lines are appended (or, in a bucket, the object rewritten) only by
//! a lock or lease holder after the replacing pack is published, and
//! dropped only after the pack they name is gone, so readers that read the
//! record before listing `packs/` never pair a listed pack with a record
//! that has forgotten it.

use std::collections::{BTreeMap, HashSet};
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::time::{Duration, SystemTime};

use leep_core::{Error, Result};

use crate::fs::unix_secs;
use crate::packset::{published_ids, sync_dir};
use crate::repack::{remove_if_present, remove_unpublished, rewrite_tombstones, write_durably};
use crate::{storage_err, FsBlobStore};

/// `packs/retired` in a filesystem store.
pub(crate) const RETIRED: &str = "retired";

/// Retired pack id → when it was retired (unix seconds; the earliest, if a
/// pack was retired twice).
pub(crate) type Retired = BTreeMap<String, i64>;

/// What a [`FsBlobStore::retire_sweep`] (or an object store's) did.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct SweepReport {
    /// Temp files and packs without an index (left by a killed or failed
    /// pack write or repack) deleted.
    pub garbage_removed: u64,
    /// Retired packs deleted because their fence had passed, and the bytes
    /// of their `.pack` files.
    pub retired_removed: u64,
    pub retired_bytes_removed: u64,
    /// Retired packs still inside their fence, left for a later sweep.
    pub retired_waiting: u64,
}

pub(crate) fn retired_line(id: &str, at: i64) -> String {
    format!("{id} {at}\n")
}

fn is_pack_id(s: &str) -> bool {
    s.len() == 64 && s.bytes().all(|c| c.is_ascii_hexdigit())
}

/// Parse the record. A last line without its newline is a torn append and
/// is ignored; a malformed line is skipped with a warning.
pub(crate) fn parse_retired(text: &[u8]) -> Retired {
    let mut out = Retired::new();
    let complete = match text.iter().rposition(|&b| b == b'\n') {
        Some(i) => &text[..=i],
        None => return out,
    };
    for line in complete.split(|&b| b == b'\n') {
        if line.is_empty() {
            continue;
        }
        let parsed = std::str::from_utf8(line).ok().and_then(|l| {
            let (id, at) = l.split_once(' ')?;
            Some((is_pack_id(id).then_some(id)?, at.parse::<i64>().ok()?))
        });
        match parsed {
            Some((id, at)) => {
                let e = out.entry(id.to_ascii_lowercase()).or_insert(at);
                *e = (*e).min(at);
            }
            None => tracing::warn!("ignoring a malformed line in the retired packs record"),
        }
    }
    out
}

pub(crate) fn encode_retired(r: &Retired) -> String {
    r.iter().map(|(id, at)| retired_line(id, *at)).collect()
}

/// Whether a pack retired at `at` (whole seconds, rounded down) has been
/// retired for at least `fence` at `now`. A second is added for the
/// rounding; a zero fence (no reader in any other process) is always past.
pub(crate) fn past_fence(at: i64, fence: Duration, now: i64) -> bool {
    if fence.is_zero() {
        return true;
    }
    let fence = fence.as_secs() + u64::from(fence.subsec_nanos() > 0);
    now.saturating_sub(at) > fence.min(i64::MAX as u64) as i64
}

pub(crate) fn now_secs() -> i64 {
    unix_secs(SystemTime::now()).unwrap_or(0)
}

/// The record of the filesystem store whose `packs/` is `dir`.
pub(crate) fn read_retired(dir: &Path) -> Result<Retired> {
    match std::fs::read(dir.join(RETIRED)) {
        Ok(b) => Ok(parse_retired(&b)),
        Err(e) if e.kind() == ErrorKind::NotFound => Ok(Retired::new()),
        Err(e) => Err(storage_err(e)),
    }
}

/// Append `ids` as retired at `at`, durably. Only under the repack lock,
/// and only once a published pack holds every live object of each.
pub(crate) fn append_retired(dir: &Path, ids: &[String], at: i64) -> Result<()> {
    if ids.is_empty() {
        return Ok(());
    }
    let path = dir.join(RETIRED);
    let existed = cut_torn_line(&path)?;
    let lines: String = ids.iter().map(|id| retired_line(id, at)).collect();
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .map_err(storage_err)?;
    f.write_all(lines.as_bytes()).map_err(storage_err)?;
    f.sync_all().map_err(storage_err)?;
    if !existed {
        sync_dir(dir)?;
    }
    Ok(())
}

/// Cut off a last line without its newline (an append that died part way),
/// so the next append does not join it. Whether the file exists.
fn cut_torn_line(path: &Path) -> Result<bool> {
    let mut f = match std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
    {
        Ok(f) => f,
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok(false),
        Err(e) => return Err(storage_err(e)),
    };
    let len = f.metadata().map_err(storage_err)?.len();
    if len == 0 {
        return Ok(true);
    }
    let mut last = [0u8; 1];
    f.seek(SeekFrom::Start(len - 1)).map_err(storage_err)?;
    f.read_exact(&mut last).map_err(storage_err)?;
    if last[0] == b'\n' {
        return Ok(true);
    }
    let mut text = Vec::new();
    f.seek(SeekFrom::Start(0)).map_err(storage_err)?;
    f.read_to_end(&mut text).map_err(storage_err)?;
    let keep = text.iter().rposition(|&b| b == b'\n').map_or(0, |i| i + 1);
    f.set_len(keep as u64).map_err(storage_err)?;
    f.sync_all().map_err(storage_err)?;
    Ok(true)
}

/// Keep only the lines of packs still published, rewritten by rename like
/// `tombstones`. Only under the repack lock, after the packs are deleted.
pub(crate) fn rewrite_retired(dir: &Path) -> Result<()> {
    let retired = read_retired(dir)?;
    if retired.is_empty() && !dir.join(RETIRED).exists() {
        return Ok(());
    }
    let live: HashSet<String> = published_ids(dir)?.into_iter().collect();
    let keep: Retired = retired
        .iter()
        .filter(|(id, _)| live.contains(*id))
        .map(|(id, at)| (id.clone(), *at))
        .collect();
    if keep.is_empty() {
        remove_if_present(&dir.join(RETIRED))?;
        return sync_dir(dir);
    }
    if keep.len() == retired.len() {
        // Nothing to drop; a torn tail is cut by the next append.
        return Ok(());
    }
    let tmp = write_durably(dir, "retired", encode_retired(&keep).as_bytes())?;
    std::fs::rename(&tmp, dir.join(RETIRED)).map_err(storage_err)?;
    sync_dir(dir)
}

/// The sweep of the filesystem store whose `packs/` is `dir`, with the
/// repack lock held: garbage first, then the retired packs past `fence`,
/// each as a repack's cleanup deletes a replaced pack (index, directory
/// sync, pack), then the lines and tombstones of packs that are gone.
pub(crate) fn sweep_locked(dir: &Path, fence: Duration) -> Result<SweepReport> {
    let mut r = SweepReport {
        garbage_removed: remove_unpublished(dir)?,
        ..SweepReport::default()
    };
    let retired = read_retired(dir)?;
    if retired.is_empty() {
        return Ok(r);
    }
    let published: HashSet<String> = published_ids(dir)?.into_iter().collect();
    let now = now_secs();
    let mut stale = false;
    for (id, at) in &retired {
        if !published.contains(id) {
            stale = true;
            continue;
        }
        if !past_fence(*at, fence, now) {
            r.retired_waiting += 1;
            continue;
        }
        let pack = dir.join(format!("{id}.pack"));
        let bytes = std::fs::metadata(&pack).map(|m| m.len()).unwrap_or(0);
        // The index first: without it the pack is unpublished garbage.
        remove_if_present(&dir.join(format!("{id}.idx")))?;
        sync_dir(dir)?;
        remove_if_present(&pack)?;
        r.retired_removed += 1;
        r.retired_bytes_removed += bytes;
    }
    if r.retired_removed > 0 {
        sync_dir(dir)?;
    }
    if stale || r.retired_removed > 0 {
        rewrite_retired(dir)?;
        rewrite_tombstones(dir)?;
    }
    Ok(r)
}

impl FsBlobStore {
    /// Clean up after repacks, safe beside running servers (ADR 0080):
    /// delete temp files and packs without an index in `packs/`, delete the
    /// packs retired at least `fence` ago, and forget packs that are gone.
    /// `None`, doing nothing, while another repack holds the store. Run at
    /// server start, by the background repack, and by `leep-server repack`.
    pub async fn retire_sweep(&self, fence: Duration) -> Result<Option<SweepReport>> {
        let store = self.clone();
        tokio::task::spawn_blocking(move || store.retire_sweep_blocking(fence))
            .await
            .map_err(|e| Error::Storage(format!("sweep task failed: {e}")))?
    }

    /// [`retire_sweep`](Self::retire_sweep) on this thread.
    pub fn retire_sweep_blocking(&self, fence: Duration) -> Result<Option<SweepReport>> {
        let dir = self.packs.dir().to_path_buf();
        if !dir.is_dir() {
            // No packs: nothing to clean, and no `packs/` to create.
            return Ok(Some(SweepReport::default()));
        }
        let _lock = match crate::repack::take_repack_lock(&dir) {
            Ok(l) => l,
            Err(Error::Conflict(_)) => return Ok(None),
            Err(e) => return Err(e),
        };
        let r = sweep_locked(&dir, fence)?;
        if r.garbage_removed + r.retired_removed > 0 {
            self.packs.rescan(crate::packset::ScanReason::Repack)?;
        }
        Ok(Some(r))
    }

    /// The packs `packs/retired` names, with when each was retired.
    pub fn retired_packs(&self) -> Result<Vec<(String, i64)>> {
        Ok(read_retired(self.packs.dir())?.into_iter().collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(n: u8) -> String {
        format!("{n:02x}").repeat(32)
    }

    #[test]
    fn lines_parse_keep_the_earliest_time_and_ignore_torn_tails() {
        let mut text = retired_line(&id(1), 100);
        text.push_str("not a line\n");
        text.push_str(&format!("{} x\n", id(3)));
        text.push_str(&retired_line(&id(2), 50));
        text.push_str(&retired_line(&id(1), 90));
        text.push_str(&retired_line(&id(4), 7)[..30]);
        let r = parse_retired(text.as_bytes());
        assert_eq!(r.len(), 2, "{r:?}");
        assert_eq!(r[&id(1)], 90);
        assert_eq!(r[&id(2)], 50);
        assert!(parse_retired(b"").is_empty());
        assert!(parse_retired(retired_line(&id(5), 1).trim_end().as_bytes()).is_empty());
        assert_eq!(parse_retired(encode_retired(&r).as_bytes()), r);
    }

    #[test]
    fn the_fence_counts_whole_seconds_with_a_margin() {
        let f = Duration::from_secs(10);
        assert!(
            !past_fence(100, f, 110),
            "rounding may hide most of a second"
        );
        assert!(past_fence(100, f, 111));
        assert!(!past_fence(100, Duration::from_millis(10_500), 111));
        assert!(past_fence(100, Duration::from_millis(10_500), 112));
        assert!(past_fence(100, Duration::ZERO, 100));
        assert!(!past_fence(100, f, 50), "a clock that went back waits");
    }

    #[test]
    fn appends_cut_a_torn_line_and_rewrites_keep_published_packs() {
        let dir = tempfile::tempdir().unwrap();
        let d = dir.path();
        append_retired(d, &[id(1)], 5).unwrap();
        // An append that died part way.
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(d.join(RETIRED))
            .unwrap();
        f.write_all(&retired_line(&id(9), 6).as_bytes()[..20])
            .unwrap();
        drop(f);
        append_retired(d, &[id(2), id(3)], 7).unwrap();
        let r = read_retired(d).unwrap();
        assert_eq!(r.keys().cloned().collect::<Vec<_>>(), [id(1), id(2), id(3)]);

        // Only id(2) is still published.
        std::fs::write(d.join(format!("{}.idx", id(2))), b"").unwrap();
        rewrite_retired(d).unwrap();
        assert_eq!(read_retired(d).unwrap(), Retired::from([(id(2), 7)]));
        std::fs::remove_file(d.join(format!("{}.idx", id(2)))).unwrap();
        rewrite_retired(d).unwrap();
        assert!(!d.join(RETIRED).exists());
        let junk: Vec<_> = std::fs::read_dir(d)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|n| n.starts_with(".tmp-"))
            .collect();
        assert!(junk.is_empty(), "{junk:?}");
    }
}
