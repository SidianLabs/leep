//! Incremental blob copy between two stores, for backup and restore
//! (ADR 0026), and the copy of whole packs that goes first when both sides
//! keep packs (ADR 0039).
//!
//! Blobs are immutable and named by their content, so "copy what the other
//! side lacks" is always safe: a blob present under a hash holds the bytes of
//! that hash, whenever and by whomever it was written. The two ascending
//! listings are merged, so an unchanged store costs two listings and no reads.

use std::collections::{HashSet, VecDeque};
use std::sync::{Arc, Mutex};

use futures::stream::{self, StreamExt};
use leep_core::{BlobMeta, BlobStore, Error, Hash, Result, TreeEntry, TreeNode};

use crate::pack::{self, PackIndex};
use crate::packfiles::{read_all, PackFiles};

/// Listing page size.
const PAGE: usize = 1000;

/// Listings of the source's packs one [`sync_packs`] makes at most.
const PACK_PASSES: usize = 4;

/// Told about progress about every five seconds.
pub type SyncProgress = Arc<dyn Fn(&SyncReport) + Send + Sync>;

#[derive(Clone)]
pub struct SyncOptions {
    /// Blobs copied at once. Each is held in memory while it is copied, so
    /// this times the largest blob bounds memory.
    pub concurrency: usize,
    /// Called with the totals so far.
    pub progress: Option<SyncProgress>,
}

impl Default for SyncOptions {
    fn default() -> Self {
        SyncOptions {
            concurrency: 8,
            progress: None,
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct SyncReport {
    /// Blobs the source listed.
    pub listed: u64,
    /// Already in the destination with the same size.
    pub present: u64,
    /// Copied (includes `replaced`).
    pub copied: u64,
    pub bytes_copied: u64,
    /// Present in the destination with a different size, so deleted and
    /// copied again (a torn or foreign object).
    pub replaced: u64,
    /// Listed by the source and gone when read: garbage collection deleted an
    /// unreferenced blob in between. Nothing to copy.
    pub vanished: u64,
    /// Bytes that do not hash to their name; not copied. A snapshot that
    /// references one fails its closure check.
    pub corrupt: Vec<Hash>,
    /// In the destination only: blobs the source has since deleted, or that an
    /// earlier snapshot needs. Kept.
    pub only_in_destination: u64,
    /// Tree nodes the listing missed below a tree node it copied, copied
    /// after it (included in `copied`): the source wrote them while it was
    /// listed.
    pub completed: u64,
}

/// Do `data` hash to `hash`, as a file blob or as a tree node?
pub fn blob_intact(hash: &Hash, data: &[u8]) -> bool {
    Hash::of_blob(data) == *hash || TreeNode::decode(data).is_ok_and(|n| n.hash() == *hash)
}

/// A listing read a page at a time.
struct Cursor<'a> {
    store: &'a dyn BlobStore,
    buf: VecDeque<BlobMeta>,
    after: Option<Hash>,
    done: bool,
}

impl<'a> Cursor<'a> {
    fn new(store: &'a dyn BlobStore) -> Self {
        Cursor {
            store,
            buf: VecDeque::new(),
            after: None,
            done: false,
        }
    }

    async fn fill(&mut self) -> Result<()> {
        if self.buf.is_empty() && !self.done {
            let page = self.store.list_page(self.after, PAGE).await?;
            match page.last() {
                Some(last) => self.after = Some(last.hash),
                None => self.done = true,
            }
            self.buf.extend(page);
        }
        Ok(())
    }

    /// Drop everything below `hash`, returning how many were dropped, then
    /// the entry at `hash` if there is one.
    async fn seek(&mut self, hash: &Hash, skipped: &mut u64) -> Result<Option<BlobMeta>> {
        loop {
            self.fill().await?;
            match self.buf.front() {
                None => return Ok(None),
                Some(m) if m.hash < *hash => {
                    *skipped += 1;
                    self.buf.pop_front();
                }
                Some(m) if m.hash == *hash => return Ok(Some(*m)),
                Some(_) => return Ok(None),
            }
        }
    }

    async fn count_rest(&mut self) -> Result<u64> {
        let mut n = 0;
        loop {
            self.fill().await?;
            if self.buf.is_empty() {
                return Ok(n);
            }
            n += self.buf.len() as u64;
            self.buf.clear();
        }
    }
}

enum Outcome {
    /// Its size, and the directories it names if it is a tree node.
    Copied(u64, Option<Vec<Hash>>),
    Vanished,
    Corrupt(Hash),
}

/// The directories `data` names if it is the tree node `hash`, `None` if it
/// is not a tree node.
fn tree_dirs(hash: &Hash, data: &[u8]) -> Option<Vec<Hash>> {
    let node = TreeNode::decode(data).ok().filter(|n| n.hash() == *hash)?;
    Some(
        node.entries
            .values()
            .filter_map(|e| match e {
                TreeEntry::Dir { tree } => Some(*tree),
                TreeEntry::File { .. } => None,
            })
            .collect(),
    )
}

async fn copy_one(
    src: &dyn BlobStore,
    dst: &dyn BlobStore,
    m: BlobMeta,
    replace: bool,
) -> Result<Outcome> {
    let Some(data) = src.get(&m.hash).await? else {
        return Ok(Outcome::Vanished);
    };
    let dirs = if Hash::of_blob(&data) == m.hash {
        None
    } else {
        match tree_dirs(&m.hash, &data) {
            Some(d) => Some(d),
            None => return Ok(Outcome::Corrupt(m.hash)),
        }
    };
    if replace {
        dst.delete(&m.hash).await?;
    }
    let n = data.len() as u64;
    dst.put_with_hash(m.hash, data).await?;
    Ok(Outcome::Copied(n, dirs))
}

/// Copy the blobs `hashes` that `dst` lacks, before any listing: blobs a
/// snapshot names that garbage collection may delete while a long listing
/// copy is still on its way to them (a backup's live segments, which a
/// merge can retire and GC collect within the copy). Counts like
/// [`sync_blobs`]; one not stored in `src` is `vanished`.
pub async fn copy_named(
    src: &dyn BlobStore,
    dst: &dyn BlobStore,
    hashes: &[Hash],
    concurrency: usize,
) -> Result<SyncReport> {
    let mut report = SyncReport::default();
    let mut results = stream::iter(hashes.iter().map(|h| async move {
        if dst.contains(h).await? {
            return Ok(None);
        }
        let m = BlobMeta {
            hash: *h,
            size: 0,
            modified: None,
        };
        copy_one(src, dst, m, false).await.map(Some)
    }))
    .buffer_unordered(concurrency.max(1));
    while let Some(r) = results.next().await {
        report.listed += 1;
        match r? {
            None => report.present += 1,
            Some(Outcome::Copied(n, _)) => {
                report.copied += 1;
                report.bytes_copied += n;
            }
            Some(Outcome::Vanished) => report.vanished += 1,
            Some(Outcome::Corrupt(h)) => report.corrupt.push(h),
        }
    }
    drop(results);
    dst.flush(&[]).await?;
    Ok(report)
}

/// Copy the tree nodes below the copied nodes that `dst` still lacks,
/// reading them from `src` exactly.
///
/// A listing is not a snapshot. A tree created without its nodes (ADR
/// 0069) is stored later, by the land that moves a ref to it or by the
/// changeset sweep, nodes first and root last; a listing running meanwhile
/// can pass a node's shard (or miss a pack published after its view) before
/// the node is written, then reach the root's shard after the root is. The
/// copy would then hold a root without its whole tree, which every reader
/// takes as stored whole, and its changeset is in the snapshot (created,
/// unlanded, before it). Children are written before their parents, so the
/// source holds every node below a root it listed.
async fn complete_trees(
    src: &dyn BlobStore,
    dst: &dyn BlobStore,
    copied: HashSet<Hash>,
    mut todo: Vec<Hash>,
    concurrency: usize,
    report: &mut SyncReport,
) -> Result<()> {
    let empty = TreeNode::empty_hash();
    let mut seen = copied;
    while !todo.is_empty() {
        let mut batch = std::mem::take(&mut todo);
        batch.retain(|h| *h != empty && seen.insert(*h));
        for chunk in batch.chunks(PAGE) {
            let have = dst.contains_many(chunk).await?;
            let lacking: Vec<Hash> = chunk
                .iter()
                .zip(have)
                .filter(|(_, have)| !have)
                .map(|(h, _)| *h)
                .collect();
            let mut results = stream::iter(lacking.into_iter().map(|h| async move {
                let data = match src.get(&h).await? {
                    Some(d) => Some(d),
                    None => src.get_exact(&h).await?,
                };
                // Absent or damaged in the source too: the closure check
                // reports it.
                let Some(data) = data else { return Ok(None) };
                let Some(dirs) = tree_dirs(&h, &data) else {
                    return Ok(None);
                };
                let n = data.len() as u64;
                dst.put_with_hash(h, data).await?;
                Ok::<_, Error>(Some((n, dirs)))
            }))
            .buffer_unordered(concurrency);
            while let Some(r) = results.next().await {
                if let Some((n, dirs)) = r? {
                    report.copied += 1;
                    report.completed += 1;
                    report.bytes_copied += n;
                    todo.extend(dirs);
                }
            }
        }
    }
    Ok(())
}

/// Copy every blob of `src` that `dst` lacks. Blobs only in `dst` stay.
/// Safe to repeat and to interrupt: a half-finished run leaves whole blobs
/// only (the stores write atomically) and the next run continues from there.
/// Everything copied is flushed (`BlobStore::flush`) before it returns.
pub async fn sync_blobs(
    src: &dyn BlobStore,
    dst: &dyn BlobStore,
    opts: &SyncOptions,
) -> Result<SyncReport> {
    if opts.concurrency == 0 {
        return Err(Error::Invalid("concurrency must be at least 1".into()));
    }
    let mut report = SyncReport::default();
    let mut dst_cursor = Cursor::new(dst);
    let mut after: Option<Hash> = None;
    let mut last_progress = std::time::Instant::now();
    // Tree nodes copied, and the directories they name.
    let mut copied_trees = HashSet::new();
    let mut dirs_named = Vec::new();
    loop {
        let page = src.list_page(after, PAGE).await?;
        let Some(last) = page.last() else { break };
        after = Some(last.hash);
        let mut todo: Vec<(BlobMeta, bool)> = Vec::new();
        for m in &page {
            report.listed += 1;
            match dst_cursor
                .seek(&m.hash, &mut report.only_in_destination)
                .await?
            {
                Some(d) if d.size == m.size => {
                    report.present += 1;
                    dst_cursor.buf.pop_front();
                }
                Some(_) => {
                    dst_cursor.buf.pop_front();
                    report.replaced += 1;
                    todo.push((*m, true));
                }
                None => todo.push((*m, false)),
            }
        }
        let mut results = stream::iter(todo.into_iter().map(|(m, replace)| async move {
            copy_one(src, dst, m, replace)
                .await
                .map(|o| (m, replace, o))
        }))
        .buffer_unordered(opts.concurrency);
        while let Some(r) = results.next().await {
            match r? {
                (m, _, Outcome::Copied(n, dirs)) => {
                    report.copied += 1;
                    report.bytes_copied += n;
                    if let Some(dirs) = dirs {
                        copied_trees.insert(m.hash);
                        dirs_named.extend(dirs);
                    }
                }
                (_, replace, Outcome::Vanished) => {
                    report.vanished += 1;
                    if replace {
                        report.replaced -= 1;
                    }
                }
                (_, replace, Outcome::Corrupt(h)) => {
                    if replace {
                        report.replaced -= 1;
                    }
                    report.corrupt.push(h);
                }
            }
        }
        drop(results);
        if let Some(p) = &opts.progress {
            if last_progress.elapsed() >= std::time::Duration::from_secs(5) {
                last_progress = std::time::Instant::now();
                p(&report);
            }
        }
    }
    report.only_in_destination += dst_cursor.count_rest().await?;
    complete_trees(
        src,
        dst,
        copied_trees,
        dirs_named,
        opts.concurrency,
        &mut report,
    )
    .await?;
    // A backup or restore is sealed after this returns, so every copy must
    // survive a power loss by then (ADR 0044).
    dst.flush(&[]).await?;
    Ok(report)
}

/// What a pack copy is for, which decides what happens to tombstones.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PackSyncMode {
    /// Into a backup pool, the union of every sealed snapshot. The pool only
    /// grows: the source's tombstones are not copied, since an older
    /// snapshot may still name an object the source has since deleted (the
    /// changeset sweep, ADR 0059, issue #155). Pool packs a source repack
    /// merged away are pruned when other pool packs hold every object they
    /// hold, or once the objects only they hold are copied into a pool pack
    /// of their own (ADR 0107).
    Pool,
    /// Into a store being restored. The source's tombstones of the packs
    /// the destination holds are copied, so the target reads exactly as the
    /// source (the pool) does. Nothing is pruned.
    Restore,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct PackSyncReport {
    /// Packs the source has published.
    pub listed: u64,
    /// Already published in the destination.
    pub present: u64,
    /// Copied, and their pack plus index bytes.
    pub copied: u64,
    pub bytes_copied: u64,
    /// Replaced in the source (by a repack) before they could be read. Their
    /// objects are in the pack that replaced them, or copied loose.
    pub vanished: u64,
    /// Source packs whose bytes do not match their index; not copied.
    pub corrupt: Vec<String>,
    /// Tombstone lines copied for packs the destination holds; always zero
    /// into a pool.
    pub tombstones_added: u64,
    /// Destination packs the source no longer has, deleted because other
    /// destination packs hold every object they do.
    pub pruned: u64,
    /// Objects only such packs held (objects the source deleted, which an
    /// older snapshot may need), copied into new pool packs so those packs
    /// could be pruned too; and how many such packs were written.
    pub rescued_objects: u64,
    pub rescue_packs: u64,
}

/// Copy the packs `src` has published that `dst` lacks, each as its two
/// files, then (on a restore) the tombstones of the packs `dst` holds. Run
/// it before [`sync_blobs`]: a destination that reads packs then lists
/// their objects as present, so only loose objects are copied one by one,
/// and one that does not still gets every object loose.
///
/// The pack file goes first and the index publishes it, as in a repack, so
/// an interrupted copy leaves an unpublished pack that the next run writes
/// again. Each pack is checked against the length and BLAKE3 its index
/// records while it streams, and the index against its name, so a damaged
/// source pack is reported and never published in the destination.
///
/// Into a pool, a destination pack the source no longer has (a repack
/// merged it into a bigger one) is deleted when the destination's other
/// packs hold every object it holds, so a backup pool does not keep every
/// intermediate pack. When it holds objects no other pack does (a repack
/// dropped them because the source deleted them), those are first written
/// into a new pool pack, so it can go too: a pool does not keep a whole
/// copy of the source's packs for every repack that dropped one object.
/// Objects are never lost that way.
pub async fn sync_packs(
    src: &dyn PackFiles,
    dst: &dyn PackFiles,
    mode: PackSyncMode,
) -> Result<PackSyncReport> {
    sync_packs_with(src, dst, mode, RESCUE_MAX_BYTES).await
}

/// [`sync_packs`] with rescue packs of at most `rescue_max` stored bytes.
async fn sync_packs_with(
    src: &dyn PackFiles,
    dst: &dyn PackFiles,
    mode: PackSyncMode,
    rescue_max: u64,
) -> Result<PackSyncReport> {
    let mut report = PackSyncReport::default();
    let mut src_ids = src.pack_ids().await?;
    let mut seen: HashSet<String> = HashSet::new();
    // A pack that vanished was replaced by a repack after the listing (a
    // consolidation can rewrite every delta pack at any time): list again
    // and copy the packs that replaced it, rather than leave its objects
    // to the loose copy. A few passes at most, each only after a vanish.
    for _ in 0..PACK_PASSES {
        let todo: Vec<String> = src_ids
            .iter()
            .filter(|id| seen.insert((*id).clone()))
            .cloned()
            .collect();
        if todo.is_empty() {
            break;
        }
        report.listed += todo.len() as u64;
        let dst_ids: HashSet<String> = dst.pack_ids().await?.into_iter().collect();
        let vanished = report.vanished;
        for id in &todo {
            if dst_ids.contains(id) {
                report.present += 1;
                continue;
            }
            let idx_name = format!("{id}.idx");
            let pack_name = format!("{id}.pack");
            let Some(idx) = read_all(src, &idx_name).await? else {
                report.vanished += 1;
                continue;
            };
            let index = match (pack::pack_id(&idx) == *id).then(|| PackIndex::decode(idx.clone())) {
                Some(Ok(i)) => i,
                _ => {
                    report.corrupt.push(id.clone());
                    continue;
                }
            };
            let Some(data) = src.read(&pack_name).await? else {
                report.vanished += 1;
                continue;
            };
            let hasher = Arc::new(Mutex::new((blake3::Hasher::new(), 0u64)));
            let h = hasher.clone();
            let want = index.pack_len();
            let data = data
                .map(move |chunk| {
                    let chunk = chunk?;
                    let mut g = h.lock().unwrap();
                    g.0.update(&chunk);
                    g.1 += chunk.len() as u64;
                    if g.1 > want {
                        return Err(Error::Storage("pack longer than its index says".into()));
                    }
                    Ok(chunk)
                })
                .boxed();
            let n = match dst.write(&pack_name, data).await {
                Ok(n) => n,
                Err(_) if hasher.lock().unwrap().1 > want => {
                    report.corrupt.push(id.clone());
                    continue;
                }
                Err(e) => return Err(e),
            };
            let digest = *hasher.lock().unwrap().0.finalize().as_bytes();
            if n != want || &digest != index.pack_hash() {
                dst.delete(&pack_name).await?;
                report.corrupt.push(id.clone());
                continue;
            }
            let idx_len = idx.len() as u64;
            dst.write(&idx_name, stream::once(async { Ok(idx.into()) }).boxed())
                .await?;
            report.copied += 1;
            report.bytes_copied += n + idx_len;
        }
        if report.vanished == vanished {
            break;
        }
        src_ids = src.pack_ids().await?;
    }

    match mode {
        PackSyncMode::Restore => report.tombstones_added = copy_tombstones(src, dst).await?,
        PackSyncMode::Pool => {
            let source: HashSet<&String> = src_ids.iter().collect();
            prune_superseded(dst, &source, &mut report, rescue_max).await?;
        }
    }
    Ok(report)
}

/// Add the source's tombstones of the packs `dst` holds that `dst` lacks.
async fn copy_tombstones(src: &dyn PackFiles, dst: &dyn PackFiles) -> Result<u64> {
    let held: HashSet<String> = dst.pack_ids().await?.into_iter().collect();
    let theirs = src.tombstones().await?;
    let ours = dst.tombstones().await?;
    let mut add = Vec::new();
    for (id, hashes) in &theirs {
        if !held.contains(id) {
            continue;
        }
        let mut new: Vec<&Hash> = hashes
            .iter()
            .filter(|h| !ours.get(id).is_some_and(|o| o.contains(h)))
            .collect();
        new.sort();
        add.extend(new.into_iter().map(|h| (id.clone(), *h)));
    }
    add.sort();
    if !add.is_empty() {
        dst.add_tombstones(&add).await?;
    }
    Ok(add.len() as u64)
}

/// The stored bytes one rescue pack takes at most: its objects are held in
/// memory while it is written. More is rescued in several packs.
const RESCUE_MAX_BYTES: u64 = 256 << 20;

/// zstd level of the frames a rescue compresses again (objects that were
/// deltas against a base it does not copy).
const RESCUE_LEVEL: i32 = 9;

/// Delete destination packs the source no longer has when every object they
/// hold is in another destination pack, and readable there wherever it is
/// readable in them. Index first, as a repack does. Tombstones never make
/// an object prunable: a pool's tombstones (only written before issue #155
/// was fixed) may hide what an older snapshot needs, and its bytes must stay.
///
/// A pack that also holds objects no other pack does (the source deleted
/// them, and its repack dropped them; an older snapshot may need them, ADR
/// 0026) is pruned once those are copied, decoded and checked against their
/// hashes, into a new pool pack of whole frames, published before the old
/// one is deleted. Earlier such packs that only hold objects of their own
/// join it while they are no larger than what it takes in already, so they
/// stay few without being rewritten at every backup. A pool pack is only
/// ever rewritten when that removes copies other packs hold: never for
/// nothing.
async fn prune_superseded(
    dst: &dyn PackFiles,
    source: &HashSet<&String>,
    report: &mut PackSyncReport,
    rescue_max: u64,
) -> Result<()> {
    let ids = dst.pack_ids().await?;
    if ids.iter().all(|id| source.contains(id)) {
        return Ok(());
    }
    let dead = dst.tombstones().await?;
    let mut packs: Vec<(String, PackIndex)> = Vec::new();
    for id in ids {
        if let Some(bytes) = read_all(dst, &format!("{id}.idx")).await? {
            if pack::pack_id(&bytes) != id {
                continue;
            }
            packs.push((id, PackIndex::decode(bytes)?));
        }
    }
    let no_dead = HashSet::new();
    let live = |id: &str, h: &Hash| !dead.get(id).unwrap_or(&no_dead).contains(h);
    // Another pack keeps `h`'s bytes, and keeps it readable if `id` does.
    let kept = |i: usize, id: &str, h: &Hash, packs: &[(String, PackIndex)]| {
        packs
            .iter()
            .enumerate()
            .any(|(j, (oid, o))| j != i && o.find(h).is_some() && (live(oid, h) || !live(id, h)))
    };
    // Ranks of the objects of pack `i` no other pack keeps.
    let own = |i: usize, packs: &[(String, PackIndex)]| -> Vec<u32> {
        let (id, index) = &packs[i];
        (0..index.len() as u32)
            .filter(|&r| !kept(i, id, &index.hash_at(r as usize), packs))
            .collect()
    };
    let mut i = 0;
    while i < packs.len() {
        if source.contains(&packs[i].0) {
            i += 1;
            continue;
        }
        let mine = own(i, &packs);
        if mine.is_empty() {
            let id = packs.remove(i).0;
            dst.delete(&format!("{id}.idx")).await?;
            dst.delete(&format!("{id}.pack")).await?;
            report.pruned += 1;
            continue;
        }
        let (id, index) = &packs[i];
        // Nothing here is held twice (an earlier rescue pack), or a pool
        // tombstone to keep exactly (written before issue #155): it stays.
        let tombstoned = dead.get(id).is_some_and(|d| !d.is_empty());
        if mine.len() == index.len() || tombstoned {
            i += 1;
            continue;
        }
        // In packs of at most `rescue_max` stored bytes each (they are held
        // in memory while written), however much only this pack holds.
        let mut chunks: Vec<Vec<(usize, Vec<u32>)>> = Vec::new();
        let (mut cur, mut cur_bytes) = (Vec::new(), 0u64);
        for r in mine {
            let n = index.entry(r).stored_len;
            if !cur.is_empty() && cur_bytes + n > rescue_max {
                chunks.push(vec![(i, std::mem::take(&mut cur))]);
                cur_bytes = 0;
            }
            cur.push(r);
            cur_bytes += n;
        }
        let mut last = vec![(i, cur)];
        // Earlier rescue packs (nothing in them held elsewhere) join the
        // last, smallest first, while each is at most twice what it holds
        // so far: they stay few (about one per doubling) and each object is
        // rewritten about once per doubling.
        let mut small: Vec<(u64, usize)> = (0..packs.len())
            .filter(|&j| j != i && !source.contains(&packs[j].0))
            .map(|j| (packs[j].1.pack_len(), j))
            .collect();
        small.sort();
        let mut total = cur_bytes;
        for (len, j) in small {
            if len > 2 * total || total + len > rescue_max {
                break;
            }
            let theirs = own(j, &packs);
            if theirs.len() != packs[j].1.len()
                || dead.get(&packs[j].0).is_some_and(|d| !d.is_empty())
            {
                continue;
            }
            total += len;
            last.push((j, theirs));
        }
        chunks.push(last);
        let mut new_packs: Vec<(String, PackIndex)> = Vec::new();
        let mut readable = true;
        'chunks: for chunk in &chunks {
            let mut objects: Vec<(Hash, Vec<u8>, u64, i64)> = Vec::new();
            for (j, ranks) in chunk {
                let (id, index) = &packs[*j];
                match rescue_objects(dst, id, index, ranks).await? {
                    Some(o) => objects.extend(o),
                    None => {
                        readable = false;
                        break 'chunks;
                    }
                }
            }
            report.rescued_objects += objects.len() as u64;
            new_packs.push(write_whole_pack(dst, objects).await?);
            report.rescue_packs += 1;
        }
        if !readable {
            // A damaged pack keeps whatever it holds; `verify` reports it.
            // Rescue packs written already only hold copies of its objects,
            // so the next run prunes them.
            i += 1;
            continue;
        }
        let mut gone: Vec<usize> = chunks.iter().flatten().map(|(j, _)| *j).collect();
        gone.sort_unstable();
        gone.dedup();
        for j in gone.into_iter().rev() {
            let id = packs.remove(j).0;
            if new_packs.iter().any(|(n, _)| *n == id) {
                continue;
            }
            dst.delete(&format!("{id}.idx")).await?;
            dst.delete(&format!("{id}.pack")).await?;
            report.pruned += 1;
        }
        for (new_id, new_index) in new_packs {
            if !packs.iter().any(|(id, _)| *id == new_id) {
                packs.push((new_id, new_index));
            }
        }
        // Indexes moved: look again from the start. Each rescue removes a
        // pack with copies held elsewhere and adds one with none, so this
        // ends.
        i = 0;
    }
    Ok(())
}

/// Read the objects at `ranks` of pack `id` (a stream of its file, so
/// memory holds only them and the bases on their chains), decoded and
/// checked against their hashes: each as its frame if the frame is whole,
/// else compressed again whole. `None` if the pack is missing or damaged.
async fn rescue_objects(
    dst: &dyn PackFiles,
    id: &str,
    index: &PackIndex,
    ranks: &[u32],
) -> Result<Option<Vec<(Hash, Vec<u8>, u64, i64)>>> {
    let want: HashSet<u32> = ranks.iter().copied().collect();
    // Every frame on the chains of the wanted ones, and how many of the
    // needed frames use each as their base.
    let mut needed: HashSet<u32> = HashSet::new();
    let mut uses: std::collections::HashMap<u32, u32> = std::collections::HashMap::new();
    for &r in ranks {
        let mut cur = r;
        let mut depth = 0;
        while needed.insert(cur) {
            let Some(b) = index.entry(cur).base else {
                break;
            };
            *uses.entry(b).or_default() += 1;
            depth += 1;
            if depth > pack::READ_CHAIN_LIMIT {
                return Ok(None);
            }
            cur = b;
        }
    }
    let mut order: Vec<u32> = needed.into_iter().collect();
    order.sort_by_key(|&r| index.entry(r).offset);
    let Some(stream) = dst.read(&format!("{id}.pack")).await? else {
        return Ok(None);
    };
    let mut ahead = Ahead {
        stream,
        buf: Vec::new(),
        at: 0,
    };
    let mut bases: std::collections::HashMap<u32, Vec<u8>> = std::collections::HashMap::new();
    let mut out = Vec::with_capacity(ranks.len());
    for r in order {
        let e = index.entry(r);
        let Ok(len) = usize::try_from(e.stored_len) else {
            return Ok(None);
        };
        let Some(frame) = ahead.frame(e.offset, len).await? else {
            return Ok(None);
        };
        let base = match e.base {
            Some(b) => match bases.get(&b) {
                Some(d) => Some(d.as_slice()),
                None => return Ok(None),
            },
            None => None,
        };
        let Ok(data) = pack::decompress(&frame, base, e.len) else {
            return Ok(None);
        };
        if let Some(b) = e.base {
            let n = uses.get_mut(&b).expect("counted");
            *n -= 1;
            if *n == 0 {
                bases.remove(&b);
            }
        }
        let hash = index.hash_at(r as usize);
        if want.contains(&r) {
            if !pack::object_intact(&hash, &data) {
                return Ok(None);
            }
            let stored = match e.base {
                None => frame,
                Some(_) => pack::compress_whole(&data, RESCUE_LEVEL)?,
            };
            out.push((hash, stored, e.len, e.modified));
        }
        if uses.get(&r).is_some_and(|&n| n > 0) {
            bases.insert(r, data);
        }
    }
    Ok(Some(out))
}

/// A pack file read front to back, frame by frame, at rising offsets.
struct Ahead {
    stream: crate::packfiles::ByteStream,
    buf: Vec<u8>,
    /// The file offset of `buf[0]`.
    at: u64,
}

impl Ahead {
    /// The `len` bytes at `off`, at or past the end of the last frame read;
    /// `None` if the file ends first.
    async fn frame(&mut self, off: u64, len: usize) -> Result<Option<Vec<u8>>> {
        if off < self.at {
            return Ok(None);
        }
        loop {
            let skip = ((off - self.at) as usize).min(self.buf.len());
            self.buf.drain(..skip);
            self.at += skip as u64;
            if self.at == off && self.buf.len() >= len {
                let f: Vec<u8> = self.buf.drain(..len).collect();
                self.at += len as u64;
                return Ok(Some(f));
            }
            match self.stream.next().await {
                Some(chunk) => self.buf.extend_from_slice(&chunk?),
                None => return Ok(None),
            }
        }
    }
}

/// Write `objects` (hash, whole frame, length, write time) as one pack and
/// publish it: the pack file, then its index. Returns its id and index.
async fn write_whole_pack(
    dst: &dyn PackFiles,
    mut objects: Vec<(Hash, Vec<u8>, u64, i64)>,
) -> Result<(String, PackIndex)> {
    objects.sort_by_key(|o| o.0);
    objects.dedup_by_key(|o| o.0);
    let mut data = pack::PACK_MAGIC.to_vec();
    let mut entries = Vec::with_capacity(objects.len());
    for (hash, frame, len, modified) in objects {
        entries.push(pack::IdxEntry {
            hash,
            offset: data.len() as u64,
            stored_len: frame.len() as u64,
            len,
            base: None,
            modified,
        });
        data.extend_from_slice(&frame);
    }
    let pack_hash = *blake3::hash(&data).as_bytes();
    let idx = PackIndex::encode(data.len() as u64, pack_hash, &entries)?;
    let id = pack::pack_id(&idx);
    let index = PackIndex::decode(idx.clone())?;
    if dst.len(&format!("{id}.idx")).await?.is_none() {
        let data = bytes::Bytes::from(data);
        dst.write(
            &format!("{id}.pack"),
            stream::once(async { Ok(data) }).boxed(),
        )
        .await?;
        let idx = bytes::Bytes::from(idx);
        dst.write(
            &format!("{id}.idx"),
            stream::once(async { Ok(idx) }).boxed(),
        )
        .await?;
    }
    Ok((id, index))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use leep_core::memory::MemoryBlobStore;
    use leep_core::{FileKind, TreeEntry};

    async fn fill(store: &MemoryBlobStore, n: usize, tag: &str) -> Vec<Hash> {
        let mut v = Vec::new();
        for i in 0..n {
            v.push(store.put(Bytes::from(format!("{tag}-{i}"))).await.unwrap());
        }
        v
    }

    fn opts() -> SyncOptions {
        SyncOptions {
            concurrency: 4,
            progress: None,
        }
    }

    #[tokio::test]
    async fn copies_what_is_missing_and_only_that() {
        let (src, dst) = (MemoryBlobStore::new(), MemoryBlobStore::new());
        // More than one listing page, so the merge crosses page boundaries.
        let shared = fill(&src, 1500, "shared").await;
        for h in shared.iter().step_by(2) {
            dst.put_with_hash(*h, src.get(h).await.unwrap().unwrap())
                .await
                .unwrap();
        }
        fill(&src, 300, "only-src").await;
        let extra = fill(&dst, 40, "only-dst").await;

        let r = sync_blobs(&src, &dst, &opts()).await.unwrap();
        assert_eq!(r.listed, 1800);
        assert_eq!(r.present, 750);
        assert_eq!(r.copied, 1050);
        assert_eq!(r.only_in_destination, 40);
        assert_eq!((r.vanished, r.replaced, r.corrupt.len()), (0, 0, 0));
        assert_eq!(dst.len(), 1840);
        for h in &extra {
            assert!(
                dst.contains(h).await.unwrap(),
                "destination-only blobs stay"
            );
        }

        // Incremental: nothing left to copy, (the second run lists both sides and reads no blob).
        let r = sync_blobs(&src, &dst, &opts()).await.unwrap();
        assert_eq!((r.copied, r.present, r.only_in_destination), (0, 1800, 40));
    }

    #[tokio::test]
    async fn tree_nodes_copy_under_their_own_address() {
        let (src, dst) = (MemoryBlobStore::new(), MemoryBlobStore::new());
        let mut node = TreeNode::default();
        node.entries.insert(
            "a".into(),
            TreeEntry::File {
                content: Hash::of_blob(b"a"),
                kind: FileKind::Regular,
                size: 1,
            },
        );
        src.put_with_hash(node.hash(), Bytes::from(node.encode()))
            .await
            .unwrap();
        let r = sync_blobs(&src, &dst, &opts()).await.unwrap();
        assert_eq!((r.copied, r.corrupt.len()), (1, 0));
        assert!(dst.contains(&node.hash()).await.unwrap());
    }

    #[tokio::test]
    async fn corrupt_source_blobs_are_reported_not_copied() {
        let (src, dst) = (MemoryBlobStore::new(), MemoryBlobStore::new());
        let good = fill(&src, 3, "good").await;
        let bad = Hash::of_blob(b"what it should hold");
        src.put_with_hash(bad, Bytes::from_static(b"what it holds"))
            .await
            .unwrap();
        let r = sync_blobs(&src, &dst, &opts()).await.unwrap();
        assert_eq!((r.copied, r.corrupt.clone()), (3, vec![bad]));
        assert!(!dst.contains(&bad).await.unwrap());
        assert!(dst.contains(&good[0]).await.unwrap());
    }

    #[tokio::test]
    async fn a_blob_of_the_wrong_size_in_the_destination_is_replaced() {
        let (src, dst) = (MemoryBlobStore::new(), MemoryBlobStore::new());
        let h = src
            .put(Bytes::from_static(b"the whole blob"))
            .await
            .unwrap();
        dst.put_with_hash(h, Bytes::from_static(b"torn"))
            .await
            .unwrap();
        let r = sync_blobs(&src, &dst, &opts()).await.unwrap();
        assert_eq!((r.replaced, r.copied), (1, 1));
        assert_eq!(dst.get(&h).await.unwrap().unwrap(), "the whole blob");
    }

    /// A source that loses a blob between listing it and reading it, as when
    /// garbage collection deletes an unreferenced one during the copy.
    struct Losing {
        inner: MemoryBlobStore,
        victim: Hash,
    }

    #[async_trait::async_trait]
    impl BlobStore for Losing {
        async fn put_with_hash(&self, h: Hash, d: Bytes) -> Result<()> {
            self.inner.put_with_hash(h, d).await
        }
        async fn get(&self, h: &Hash) -> Result<Option<Bytes>> {
            self.inner.delete(&self.victim).await?;
            self.inner.get(h).await
        }
        async fn list_page(&self, a: Option<Hash>, l: usize) -> Result<Vec<BlobMeta>> {
            self.inner.list_page(a, l).await
        }
        async fn delete(&self, h: &Hash) -> Result<()> {
            self.inner.delete(h).await
        }
    }

    #[tokio::test]
    async fn a_blob_deleted_during_the_copy_is_skipped_not_an_error() {
        let inner = MemoryBlobStore::new();
        let hashes = fill(&inner, 10, "x").await;
        let victim = *hashes.iter().max().unwrap();
        let src = Losing { inner, victim };
        let dst = MemoryBlobStore::new();
        let r = sync_blobs(
            &src,
            &dst,
            &SyncOptions {
                concurrency: 1,
                progress: None,
            },
        )
        .await
        .unwrap();
        assert_eq!(r.listed, 10);
        assert_eq!(r.vanished, 1);
        assert_eq!(r.copied, 9);
        assert!(!dst.contains(&victim).await.unwrap());
    }

    #[tokio::test]
    async fn zero_concurrency_is_refused() {
        let s = MemoryBlobStore::new();
        let o = SyncOptions {
            concurrency: 0,
            progress: None,
        };
        assert!(sync_blobs(&s, &s, &o).await.is_err());
    }

    mod packs {
        use super::*;
        use crate::{
            FsBlobStore, FsPackFiles, ObjectBlobStore, ObjectPackFiles, RepackMode, RepackOptions,
        };
        use object_store::memory::InMemory;
        use object_store::ObjectStoreExt;

        async fn repack(store: &FsBlobStore, mode: RepackMode) {
            let opts = RepackOptions {
                mode,
                ..RepackOptions::default()
            };
            store
                .repack_publish(Vec::new(), opts)
                .await
                .unwrap()
                .finish()
                .await
                .unwrap();
        }

        const INCREMENTAL: RepackMode = RepackMode::Incremental {
            factor: 2,
            max_packs: 8,
        };

        /// A source whose first listing of packs is one taken before a
        /// repack replaced them: a backup slower than the fence.
        struct Stale {
            inner: FsPackFiles,
            first: Mutex<Option<Vec<String>>>,
        }

        #[async_trait::async_trait]
        impl PackFiles for Stale {
            async fn pack_ids(&self) -> Result<Vec<String>> {
                let first = self.first.lock().unwrap().take();
                match first {
                    Some(ids) => Ok(ids),
                    None => self.inner.pack_ids().await,
                }
            }
            async fn len(&self, name: &str) -> Result<Option<u64>> {
                self.inner.len(name).await
            }
            async fn read(&self, name: &str) -> Result<Option<crate::packfiles::ByteStream>> {
                self.inner.read(name).await
            }
            async fn write(&self, name: &str, data: crate::packfiles::ByteStream) -> Result<u64> {
                self.inner.write(name, data).await
            }
            async fn delete(&self, name: &str) -> Result<()> {
                self.inner.delete(name).await
            }
            async fn tombstones(&self) -> Result<crate::packset::Tombstones> {
                self.inner.tombstones().await
            }
            async fn add_tombstones(&self, add: &[(String, Hash)]) -> Result<()> {
                self.inner.add_tombstones(add).await
            }
        }

        /// The packs a backup listed were replaced before it read them: it
        /// lists again and copies the pack that replaced them, instead of
        /// leaving their objects to the loose copy.
        #[tokio::test]
        async fn packs_replaced_after_the_listing_are_copied_from_a_new_listing() {
            let (a, b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
            let (src, _) = source(a.path()).await;
            let before = FsPackFiles::new(a.path()).pack_ids().await.unwrap();
            repack(&src, RepackMode::Full).await;
            let after = FsPackFiles::new(a.path()).pack_ids().await.unwrap();
            assert!(after.iter().all(|id| !before.contains(id)));
            let sp = Stale {
                inner: FsPackFiles::new(a.path()),
                first: Mutex::new(Some(before.clone())),
            };
            let dp = FsPackFiles::new(b.path());
            let r = sync_packs(&sp, &dp, PackSyncMode::Pool).await.unwrap();
            assert_eq!(r.vanished, before.len() as u64, "{r:?}");
            assert_eq!(r.copied, after.len() as u64, "{r:?}");
            assert_eq!(dp.pack_ids().await.unwrap(), after);
        }

        /// What only a superseded pool pack holds is rescued however much
        /// it is, in several packs of at most the cap each.
        #[tokio::test]
        async fn a_large_rescue_is_written_in_several_packs() {
            let (a, b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
            let (src, hashes) = source(a.path()).await;
            let (sp, dp) = (FsPackFiles::new(a.path()), FsPackFiles::new(b.path()));
            sync_packs(&sp, &dp, PackSyncMode::Pool).await.unwrap();
            for h in &hashes[5..15] {
                src.delete(h).await.unwrap();
            }
            repack(&src, RepackMode::Full).await;
            // Each object is about 20 stored bytes: a few to a pack.
            let r = sync_packs_with(&sp, &dp, PackSyncMode::Pool, 64)
                .await
                .unwrap();
            assert_eq!(r.rescued_objects, 11, "{r:?}");
            assert!(r.rescue_packs >= 3, "{r:?}");
            assert_eq!(r.pruned, 2, "{r:?}");
            let pool = FsBlobStore::new(b.path());
            for h in &hashes {
                assert!(pool.get(h).await.unwrap().is_some(), "the pool keeps {h}");
            }
            // Nothing more to do.
            let r = sync_packs_with(&sp, &dp, PackSyncMode::Pool, 64)
                .await
                .unwrap();
            assert_eq!((r.rescued_objects, r.pruned), (0, 0), "{r:?}");
        }

        async fn all(store: &dyn BlobStore) -> Vec<Hash> {
            let mut out = Vec::new();
            let mut after = None;
            loop {
                let page = store.list_page(after, 100).await.unwrap();
                let Some(last) = page.last() else { return out };
                after = Some(last.hash);
                out.extend(page.iter().map(|m| m.hash));
            }
        }

        /// A packed source with loose objects beside its packs and a
        /// deleted packed object.
        async fn source(dir: &std::path::Path) -> (FsBlobStore, Vec<Hash>) {
            let src = FsBlobStore::new(dir);
            let mut hashes = Vec::new();
            for i in 0..40 {
                hashes.push(src.put(Bytes::from(format!("object {i}"))).await.unwrap());
            }
            repack(&src, RepackMode::Full).await;
            for i in 40..45 {
                hashes.push(src.put(Bytes::from(format!("object {i}"))).await.unwrap());
            }
            repack(&src, INCREMENTAL).await;
            src.delete(&hashes[3]).await.unwrap();
            for i in 45..48 {
                hashes.push(src.put(Bytes::from(format!("object {i}"))).await.unwrap());
            }
            (src, hashes)
        }

        #[tokio::test]
        async fn packs_copy_whole_to_a_filesystem_store_and_objects_follow() {
            let (a, b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
            let (src, hashes) = source(a.path()).await;
            let dst = FsBlobStore::new(b.path());
            let (sp, dp) = (FsPackFiles::new(a.path()), FsPackFiles::new(b.path()));
            let r = sync_packs(&sp, &dp, PackSyncMode::Pool).await.unwrap();
            assert_eq!((r.listed, r.copied, r.present), (2, 2, 0), "{r:?}");
            assert_eq!(r.tombstones_added, 0, "{r:?}");
            assert!(dp.tombstones().await.unwrap().is_empty());
            assert!(r.corrupt.is_empty());
            let b2 = sync_blobs(&src, &dst, &SyncOptions::default())
                .await
                .unwrap();
            assert_eq!(b2.copied, 3, "only the loose objects: {b2:?}");
            assert_eq!(b2.only_in_destination, 1, "{b2:?}");
            // The pool keeps what the source deleted: an older snapshot may
            // need it (issue #155).
            for h in &hashes {
                assert!(dst.get(h).await.unwrap().is_some());
            }
            // Again: nothing to do.
            let r = sync_packs(&sp, &dp, PackSyncMode::Pool).await.unwrap();
            assert_eq!(
                (r.copied, r.present, r.tombstones_added),
                (0, 2, 0),
                "{r:?}"
            );

            // The source merges its packs and the full repack drops the
            // deleted object. The pool gets the merged pack and drops the
            // small pack it covers; the big one still holds the deleted
            // object, so that one object is copied into a pool pack of its
            // own (ADR 0107) and the big one goes too.
            repack(&src, RepackMode::Full).await;
            let r = sync_packs(&sp, &dp, PackSyncMode::Pool).await.unwrap();
            assert_eq!((r.copied, r.pruned), (1, 2), "{r:?}");
            assert_eq!((r.rescued_objects, r.rescue_packs), (1, 1), "{r:?}");
            assert_eq!(dp.pack_ids().await.unwrap().len(), 2);
            let fresh = FsBlobStore::new(b.path());
            for h in &hashes {
                assert!(fresh.get(h).await.unwrap().is_some());
            }
        }

        #[tokio::test]
        async fn what_only_a_pool_pack_holds_is_kept() {
            let (a, b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
            let (src, hashes) = source(a.path()).await;
            let (sp, dp) = (FsPackFiles::new(a.path()), FsPackFiles::new(b.path()));
            sync_packs(&sp, &dp, PackSyncMode::Pool).await.unwrap();
            // GC deletes another packed object; the full repack drops it, so
            // only the pool's old pack still has it.
            src.delete(&hashes[5]).await.unwrap();
            repack(&src, RepackMode::Full).await;
            let r = sync_packs(&sp, &dp, PackSyncMode::Pool).await.unwrap();
            assert_eq!((r.copied, r.pruned), (1, 2), "{r:?}");
            assert_eq!(r.rescued_objects, 2, "{r:?}");
            let pool = FsBlobStore::new(b.path());
            for h in &hashes {
                assert!(pool.get(h).await.unwrap().is_some(), "the pool keeps {h}");
            }
        }

        /// A pool written before issue #155 was fixed carries tombstones. A
        /// tombstoned object never makes a pool pack prunable: its bytes may
        /// be what an older snapshot needs. Nor is such a pack rescued, so
        /// it reads exactly as before.
        #[tokio::test]
        async fn pool_tombstones_never_make_a_pack_prunable() {
            let (a, b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
            let (src, _) = source(a.path()).await;
            let (sp, dp) = (FsPackFiles::new(a.path()), FsPackFiles::new(b.path()));
            sync_packs(&sp, &dp, PackSyncMode::Pool).await.unwrap();
            // What an old backup did: copy the source's tombstones.
            let old: Vec<(String, Hash)> = sp
                .tombstones()
                .await
                .unwrap()
                .into_iter()
                .flat_map(|(id, hs)| hs.into_iter().map(move |h| (id.clone(), h)))
                .collect();
            assert_eq!(old.len(), 1);
            dp.add_tombstones(&old).await.unwrap();
            repack(&src, RepackMode::Full).await;
            let r = sync_packs(&sp, &dp, PackSyncMode::Pool).await.unwrap();
            assert_eq!((r.copied, r.pruned), (1, 1), "{r:?}");
            let big = &old[0].0;
            assert!(
                dp.pack_ids().await.unwrap().contains(big),
                "the pack holding the tombstoned object stays"
            );
        }

        #[tokio::test]
        async fn packs_copy_to_an_object_store_under_packs_keys() {
            let a = tempfile::tempdir().unwrap();
            let (src, hashes) = source(a.path()).await;
            let bucket: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
            let dp = ObjectPackFiles::new(bucket.clone());
            let sp = FsPackFiles::new(a.path());
            let r = sync_packs(&sp, &dp, PackSyncMode::Pool).await.unwrap();
            assert_eq!(r.copied, 2, "{r:?}");
            let ids = sp.pack_ids().await.unwrap();
            assert_eq!(dp.pack_ids().await.unwrap(), ids);
            for id in &ids {
                for ext in ["pack", "idx"] {
                    let key = object_store::path::Path::from(format!("packs/{id}.{ext}"));
                    let got = bucket.get(&key).await.unwrap().bytes().await.unwrap();
                    let want = std::fs::read(a.path().join(format!("packs/{id}.{ext}"))).unwrap();
                    assert_eq!(got, want);
                }
            }
            assert!(dp.tombstones().await.unwrap().is_empty());
            // The object store reads the packs, so only loose objects follow
            // one by one, and it lists what the source does plus the object
            // the source deleted.
            let dst = ObjectBlobStore::new(bucket);
            let r = sync_blobs(&src, &dst, &SyncOptions::default())
                .await
                .unwrap();
            assert_eq!(r.copied, 3, "{r:?}");
            let mut want = all(&src).await;
            want.push(hashes[3]);
            want.sort();
            assert_eq!(all(&dst).await, want);
            // And back: packs restore from the bucket into a fresh store.
            let c = tempfile::tempdir().unwrap();
            let r = sync_packs(&dp, &FsPackFiles::new(c.path()), PackSyncMode::Restore)
                .await
                .unwrap();
            assert_eq!((r.copied, r.tombstones_added), (2, 0), "{r:?}");
            assert_eq!(all(&FsBlobStore::new(c.path())).await.len(), 45);
        }

        /// A restore copies the tombstones its source holds, so the target
        /// reads as the source does.
        #[tokio::test]
        async fn a_restore_copies_the_source_tombstones() {
            let (a, c) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
            let (_, hashes) = source(a.path()).await;
            let (sp, cp) = (FsPackFiles::new(a.path()), FsPackFiles::new(c.path()));
            let r = sync_packs(&sp, &cp, PackSyncMode::Restore).await.unwrap();
            assert_eq!((r.copied, r.tombstones_added, r.pruned), (2, 1, 0), "{r:?}");
            assert_eq!(
                cp.tombstones().await.unwrap(),
                sp.tombstones().await.unwrap()
            );
            assert!(FsBlobStore::new(c.path())
                .get(&hashes[3])
                .await
                .unwrap()
                .is_none());
        }

        #[tokio::test]
        async fn a_damaged_source_pack_is_reported_and_not_published() {
            let (a, b) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
            source(a.path()).await;
            let sp = FsPackFiles::new(a.path());
            let ids = sp.pack_ids().await.unwrap();
            let path = a.path().join(format!("packs/{}.pack", ids[0]));
            let mut bytes = std::fs::read(&path).unwrap();
            let last = bytes.len() - 1;
            bytes[last] ^= 1;
            std::fs::write(&path, bytes).unwrap();
            let dp = FsPackFiles::new(b.path());
            let r = sync_packs(&sp, &dp, PackSyncMode::Restore).await.unwrap();
            assert_eq!(r.corrupt, vec![ids[0].clone()], "{r:?}");
            assert_eq!(r.copied, 1);
            assert_eq!(dp.pack_ids().await.unwrap(), vec![ids[1].clone()]);
            assert!(dp.len(&format!("{}.pack", ids[0])).await.unwrap().is_none());
        }
    }
}
