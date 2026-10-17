//! The packs a filesystem store reads from: their indexes, the tombstones
//! that hide deleted objects in them, and a cache of decoded objects.
//!
//! Everything here is synchronous file I/O; the async store calls it from
//! blocking tasks.

use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};

use bytes::Bytes;
use leep_core::{BlobMeta, Error, Hash, Result};

use crate::cache::Lru;
use crate::pack::{self, IdxEntry, PackIndex, PACK_MAGIC, READ_CHAIN_LIMIT};
use crate::storage_err;

pub(crate) const PACKS_DIR: &str = "packs";
pub(crate) const TOMBSTONES: &str = "tombstones";
/// Held while appending to or rewriting `tombstones`. A separate file, because
/// a rewrite renames a new `tombstones` into place and an append that locked
/// the old one would land in an unlinked file.
pub(crate) const TOMBSTONES_LOCK: &str = "tombstones.lock";
/// Decoded objects kept for delta chains: a read of a recent version usually
/// decodes the older versions it is based on, which the next read needs again.
pub(crate) const DEFAULT_DECODED_CACHE: usize = 64 << 20;

/// One published pack, open for reading. The open handle keeps the bytes
/// readable after a repack deletes the file.
pub(crate) struct Pack {
    pub id: String,
    pub index: PackIndex,
    file: File,
}

impl Pack {
    /// `None` if the pack vanished (a repack replaced it) while loading.
    fn open(dir: &Path, id: &str) -> Result<Option<Pack>> {
        let idx_bytes = match std::fs::read(dir.join(format!("{id}.idx"))) {
            Ok(b) => b,
            Err(e) if e.kind() == ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(storage_err(e)),
        };
        if pack::pack_id(&idx_bytes) != id {
            return Err(Error::Storage(format!(
                "corrupt pack index {id}.idx: its hash is not its name"
            )));
        }
        let index =
            PackIndex::decode(idx_bytes).map_err(|e| Error::Storage(format!("pack {id}: {e}")))?;
        let file = match File::open(dir.join(format!("{id}.pack"))) {
            Ok(f) => f,
            Err(e) if e.kind() == ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(storage_err(e)),
        };
        let len = file.metadata().map_err(storage_err)?.len();
        let mut magic = [0u8; 8];
        if len != index.pack_len() || read_at(&file, &mut magic, 0).is_err() || magic != PACK_MAGIC
        {
            return Err(Error::Storage(format!(
                "corrupt pack {id}.pack: {len} bytes or bad magic, index says {}",
                index.pack_len()
            )));
        }
        Ok(Some(Pack {
            id: id.to_string(),
            index,
            file,
        }))
    }

    pub fn frame(&self, e: &IdxEntry) -> Result<Vec<u8>> {
        let len = usize::try_from(e.stored_len)
            .map_err(|_| Error::Storage("pack entry too large".into()))?;
        let mut buf = vec![0u8; len];
        read_at(&self.file, &mut buf, e.offset).map_err(storage_err)?;
        Ok(buf)
    }
}

fn read_at(f: &File, buf: &mut [u8], offset: u64) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        std::os::unix::fs::FileExt::read_exact_at(f, buf, offset)
    }
    #[cfg(windows)]
    {
        let mut done = 0;
        while done < buf.len() {
            let n = std::os::windows::fs::FileExt::seek_read(
                f,
                &mut buf[done..],
                offset + done as u64,
            )?;
            if n == 0 {
                return Err(ErrorKind::UnexpectedEof.into());
            }
            done += n;
        }
        Ok(())
    }
}

/// Enough of a file's identity to notice that it changed or was replaced.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct FileSig {
    len: u64,
    modified: Option<std::time::SystemTime>,
    ino: u64,
}

pub(crate) fn file_sig(path: &Path) -> Result<Option<FileSig>> {
    match std::fs::metadata(path) {
        Ok(m) => Ok(Some(FileSig {
            len: m.len(),
            modified: m.modified().ok(),
            #[cfg(unix)]
            ino: std::os::unix::fs::MetadataExt::ino(&m),
            #[cfg(not(unix))]
            ino: 0,
        })),
        Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
        Err(e) => Err(storage_err(e)),
    }
}

/// Pack id → hashes deleted from that pack.
pub(crate) type Tombstones = HashMap<String, HashSet<Hash>>;

/// One line per deleted object: `<pack id hex> <hash hex>\n`. A last line
/// without its newline is a torn append and is ignored.
pub(crate) fn parse_tombstones(text: &[u8]) -> Tombstones {
    let mut out = Tombstones::new();
    let complete = match text.iter().rposition(|&b| b == b'\n') {
        Some(i) => &text[..=i],
        None => return out,
    };
    for line in complete.split(|&b| b == b'\n') {
        if line.is_empty() {
            continue;
        }
        let parsed = std::str::from_utf8(line).ok().and_then(|l| {
            let (id, hash) = l.split_once(' ')?;
            let ok = id.len() == 64 && id.bytes().all(|c| c.is_ascii_hexdigit());
            Some((ok.then_some(id)?, hash.parse::<Hash>().ok()?))
        });
        match parsed {
            Some((id, hash)) => {
                out.entry(id.to_string()).or_default().insert(hash);
            }
            None => tracing::warn!("ignoring a malformed line in packs/tombstones"),
        }
    }
    out
}

pub(crate) fn tombstone_line(pack_id: &str, hash: &Hash) -> String {
    format!("{pack_id} {hash}\n")
}

fn read_tombstones(dir: &Path) -> Result<(Option<FileSig>, Tombstones)> {
    let path = dir.join(TOMBSTONES);
    // Signature first: a change after it is noticed next time.
    let sig = file_sig(&path)?;
    match std::fs::read(&path) {
        Ok(b) => Ok((sig, parse_tombstones(&b))),
        Err(e) if e.kind() == ErrorKind::NotFound => Ok((None, Tombstones::new())),
        Err(e) => Err(storage_err(e)),
    }
}

/// Ids of the published packs (those with an index), sorted.
pub(crate) fn published_ids(dir: &Path) -> Result<Vec<String>> {
    let rd = match std::fs::read_dir(dir) {
        Ok(rd) => rd,
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(storage_err(e)),
    };
    let mut ids = Vec::new();
    for entry in rd {
        let name = entry.map_err(storage_err)?.file_name();
        let name = name.to_string_lossy();
        if let Some(id) = name.strip_suffix(".idx") {
            if id.len() == 64 && id.bytes().all(|c| c.is_ascii_hexdigit()) {
                ids.push(id.to_string());
            }
        }
    }
    ids.sort();
    Ok(ids)
}

/// The packs and tombstones as of one scan.
pub(crate) struct View {
    /// Each pack with the hashes deleted from it.
    pub packs: Vec<(Arc<Pack>, HashSet<Hash>)>,
    ids: Vec<String>,
    tombstones: Option<FileSig>,
}

impl View {
    /// The first live packed copy of `hash`.
    pub fn locate(&self, hash: &Hash) -> Option<(&Arc<Pack>, IdxEntry)> {
        self.packs.iter().find_map(|(p, dead)| {
            let e = p.index.find(hash)?;
            (!dead.contains(hash)).then_some((p, e))
        })
    }

    pub fn is_empty(&self) -> bool {
        self.packs.is_empty()
    }
}

pub(crate) struct PackSet {
    dir: PathBuf,
    view: RwLock<Option<Arc<View>>>,
    refreshing: Mutex<()>,
    decoded: DecodedCache,
}

impl PackSet {
    pub fn new(root: &Path, decoded_capacity: usize) -> Self {
        PackSet {
            dir: root.join(PACKS_DIR),
            view: RwLock::new(None),
            refreshing: Mutex::new(()),
            decoded: DecodedCache::new(decoded_capacity),
        }
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// The current view, scanning on first use.
    pub fn view(&self) -> Result<Arc<View>> {
        if let Some(v) = self.view.read().unwrap().as_ref() {
            return Ok(v.clone());
        }
        self.refresh()
    }

    /// Scan `packs/` again, reusing packs already open. Another process may
    /// have published a pack (and deleted the loose copies it holds), deleted
    /// replaced packs, or written tombstones.
    pub fn refresh(&self) -> Result<Arc<View>> {
        let _one = self.refreshing.lock().unwrap();
        // Tombstones before the directory: a repack deletes replaced indexes
        // before it drops their tombstones, so this order never pairs an old
        // pack with a tombstone file that has forgotten it.
        let (tomb_sig, mut tombstones) = read_tombstones(&self.dir)?;
        let ids = published_ids(&self.dir)?;
        let current = self.view.read().unwrap().clone();
        if let Some(v) = &current {
            if v.ids == ids && v.tombstones == tomb_sig {
                return Ok(v.clone());
            }
        }
        let mut open: HashMap<&str, &Arc<Pack>> = HashMap::new();
        if let Some(v) = &current {
            for (p, _) in &v.packs {
                open.insert(&p.id, p);
            }
        }
        let mut packs = Vec::new();
        let mut loaded = Vec::new();
        for id in &ids {
            let pack = match open.get(id.as_str()) {
                Some(p) => (*p).clone(),
                None => match Pack::open(&self.dir, id)? {
                    Some(p) => Arc::new(p),
                    None => continue,
                },
            };
            loaded.push(id.clone());
            let dead = tombstones.remove(id).unwrap_or_default();
            packs.push((pack, dead));
        }
        let view = Arc::new(View {
            packs,
            // Only what loaded, so a pack seen mid-delete is retried next time.
            ids: if loaded == ids { ids } else { loaded },
            tombstones: tomb_sig,
        });
        *self.view.write().unwrap() = Some(view.clone());
        Ok(view)
    }

    /// The view, rescanned first if the tombstones file changed since. One
    /// `stat`, so a caller that must not report a deleted object can afford it.
    pub fn fresh_tombstones(&self) -> Result<Arc<View>> {
        let v = self.view()?;
        if file_sig(&self.dir.join(TOMBSTONES))? == v.tombstones {
            return Ok(v);
        }
        self.refresh()
    }

    /// Bytes of a packed object, `None` if no live packed copy exists. Looks
    /// again after a rescan before answering `None`.
    pub fn get(&self, hash: &Hash) -> Result<Option<Bytes>> {
        let v = self.view()?;
        if let Some((p, e)) = v.locate(hash) {
            return self.decode(p, &e).map(Some);
        }
        let v = self.refresh()?;
        match v.locate(hash) {
            Some((p, e)) => self.decode(p, &e).map(Some),
            None => Ok(None),
        }
    }

    pub fn contains(&self, hash: &Hash) -> Result<bool> {
        if self.fresh_tombstones()?.locate(hash).is_some() {
            return Ok(true);
        }
        Ok(self.refresh()?.locate(hash).is_some())
    }

    /// Decode an entry and check it against its hash.
    pub fn decode(&self, pack: &Pack, entry: &IdxEntry) -> Result<Bytes> {
        let chain = self.decoded.plan(&pack.id, &pack.index, entry)?;
        self.decoded
            .finish(&pack.id, entry, chain, |e| pack.frame(e))
    }

    /// Hide packed copies of `hash` from every pack that has one. Durable
    /// before it returns.
    pub fn tombstone(&self, hash: &Hash) -> Result<()> {
        let v = self.refresh()?;
        let lines: String = v
            .packs
            .iter()
            .filter(|(p, dead)| p.index.find(hash).is_some() && !dead.contains(hash))
            .map(|(p, _)| tombstone_line(&p.id, hash))
            .collect();
        if lines.is_empty() {
            return Ok(());
        }
        self.append_tombstones(&lines)?;
        self.refresh()?;
        Ok(())
    }

    pub fn append_tombstones(&self, lines: &str) -> Result<()> {
        append_tombstones(&self.dir, lines)
    }
}

/// Decoded packed objects, shared by every pack of a store. A read of a
/// recent version usually decodes the older versions it is based on, which
/// the next read needs again.
pub(crate) struct DecodedCache {
    lru: Mutex<Lru>,
    capacity: usize,
}

/// What a delta chain needs: entries to decode, the wanted one first and the
/// deepest base last, and the cached object below them, if any.
pub(crate) struct Chain {
    pub entries: Vec<IdxEntry>,
    start: Option<Bytes>,
}

fn corrupt(pack_id: &str, hash: &Hash, why: String) -> Error {
    Error::Storage(format!(
        "corrupt packed object {hash} in pack {pack_id}: {why}"
    ))
}

impl DecodedCache {
    pub fn new(capacity: usize) -> Self {
        DecodedCache {
            lru: Mutex::default(),
            capacity,
        }
    }

    /// Follow `entry`'s chain iteratively down to a whole entry or a cached
    /// object. Separate from [`finish`](Self::finish) so a remote reader can
    /// fetch every frame of the chain in one batch.
    pub fn plan(&self, pack_id: &str, index: &PackIndex, entry: &IdxEntry) -> Result<Chain> {
        let mut entries = vec![*entry];
        loop {
            let cur = *entries.last().unwrap();
            if let Some(b) = self.lru.lock().unwrap().get(&cur.hash) {
                entries.pop();
                return Ok(Chain {
                    entries,
                    start: Some(b),
                });
            }
            // In range: decoding the index checked every base.
            let Some(base) = cur.base else {
                return Ok(Chain {
                    entries,
                    start: None,
                });
            };
            if entries.len() >= READ_CHAIN_LIMIT {
                return Err(corrupt(pack_id, &entry.hash, "delta chain too long".into()));
            }
            entries.push(index.entry(base));
        }
    }

    /// Decode a planned chain back up, given each entry's frame. Every decoded
    /// object is checked against its hash, so damage is an error and never
    /// wrong bytes.
    pub fn finish<F, B>(
        &self,
        pack_id: &str,
        entry: &IdxEntry,
        chain: Chain,
        mut frame: F,
    ) -> Result<Bytes>
    where
        F: FnMut(&IdxEntry) -> Result<B>,
        B: AsRef<[u8]>,
    {
        let Chain {
            mut entries,
            start: mut bytes,
        } = chain;
        let bad = |why: String| corrupt(pack_id, &entry.hash, why);
        while let Some(e) = entries.pop() {
            let frame = frame(&e)?;
            let data = pack::decompress(frame.as_ref(), bytes.as_deref(), e.len)
                .map_err(|err| bad(err.to_string()))?;
            if !pack::object_intact(&e.hash, &data) {
                return Err(bad(format!("{} decodes to other bytes", e.hash)));
            }
            let data = Bytes::from(data);
            self.lru
                .lock()
                .unwrap()
                .insert(e.hash, data.clone(), self.capacity);
            bytes = Some(data);
        }
        Ok(bytes.expect("the chain decodes at least one entry"))
    }
}

/// Packed objects above `after`, minus each pack's deleted ones, merged with
/// `loose` (which wins) in hash order, at most `limit`. A copy in several
/// packs is listed once, with its latest modification time.
pub(crate) fn merge_listing<'a>(
    packs: impl IntoIterator<Item = (&'a PackIndex, &'a HashSet<Hash>)>,
    loose: Vec<BlobMeta>,
    after: Option<Hash>,
    limit: usize,
) -> Vec<BlobMeta> {
    let mut merged = std::collections::BTreeMap::new();
    for (index, dead) in packs {
        let from = after.map_or(0, |a| match index.rank_of(&a) {
            Ok(r) => r + 1,
            Err(r) => r,
        });
        for e in (from..index.len())
            .map(|r| index.entry(r as u32))
            .filter(|e| !dead.contains(&e.hash))
            .take(limit)
        {
            let m = BlobMeta {
                hash: e.hash,
                size: e.len,
                modified: Some(e.modified),
            };
            merged
                .entry(e.hash)
                .and_modify(|old: &mut BlobMeta| old.modified = old.modified.max(m.modified))
                .or_insert(m);
        }
    }
    for m in loose {
        merged.insert(m.hash, m);
    }
    merged.into_values().take(limit).collect()
}

/// Append lines to `packs/tombstones` under its lock, durably.
pub(crate) fn append_tombstones(dir: &Path, lines: &str) -> Result<()> {
    let _lock = lock_tombstones(dir)?;
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(dir.join(TOMBSTONES))
        .map_err(storage_err)?;
    f.write_all(lines.as_bytes()).map_err(storage_err)?;
    f.sync_all().map_err(storage_err)?;
    sync_dir(dir)
}

/// Exclusive lock on `packs/tombstones.lock`, released on drop.
pub(crate) fn lock_tombstones(dir: &Path) -> Result<File> {
    std::fs::create_dir_all(dir).map_err(storage_err)?;
    let f = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(dir.join(TOMBSTONES_LOCK))
        .map_err(storage_err)?;
    f.lock().map_err(storage_err)?;
    Ok(f)
}

/// Make renames and creations in `dir` durable.
pub(crate) fn sync_dir(dir: &Path) -> Result<()> {
    #[cfg(unix)]
    File::open(dir)
        .and_then(|d| d.sync_all())
        .map_err(storage_err)?;
    let _ = dir;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tombstone_lines_parse_and_torn_tails_are_ignored() {
        let id = "ab".repeat(32);
        let h1 = Hash::of_blob(b"one");
        let h2 = Hash::of_blob(b"two");
        let mut text = tombstone_line(&id, &h1);
        text.push_str("garbage line\n");
        text.push_str(&tombstone_line(&id, &h2)[..40]);
        let t = parse_tombstones(text.as_bytes());
        assert_eq!(t.len(), 1);
        assert_eq!(t[&id], HashSet::from([h1]));
        assert!(parse_tombstones(b"").is_empty());
    }
}
