//! `log-filter-v1`: which authors and `agent.*` values occur in stretches of
//! history, so a log filtered by them can step over stretches with no match
//! instead of reading every changeset (ADR 0047).
//!
//! A changeset at first-parent depth `d` (roots are 0) stores one set of
//! value fingerprints per level `j` in `0..=tz(d)`, `tz` being the number of
//! trailing zero bits. Level `j` holds (a superset of) the values of every
//! changeset reachable from it but not from its first-parent ancestor `2^j`
//! steps up, side branches merged in that stretch included. Level 0 is the
//! changeset itself plus, at a merge, what its other parents bring in; level
//! `j` is level `j - 1` united with level `j - 1` of the ancestor `2^(j-1)`
//! steps up, whose depth has exactly `j - 1` trailing zeros. Like a Fenwick
//! tree, a few levels cover any first-parent stretch, and the walk in
//! `filter_log` uses them to skip.
//!
//! A merge's level 0 holds exactly what its other parents bring in, found
//! by one walk of both parents' histories (see [`merge_side`]), or, past
//! [`SIDE_BUDGET`] changesets, a superset from the sides' own levels.

use std::collections::{BinaryHeap, HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use leep_core::changeset::{EXTRA_AGENT_MODEL, EXTRA_AGENT_SESSION};
use leep_core::{Changeset, ChangesetId, DerivedRow, Error, Hash, Result};

use super::{derive, DerivedKind, SkipEntry};
use crate::ancestry::Reader;
use crate::repo::{ByKey, CacheScale, SizedCache};
use crate::Repo;

/// Per-changeset kind; see the module docs.
pub struct LogFilterIndex;

/// Lower-cased author names by fingerprint, so a substring filter can be
/// turned into the fingerprints it matches. Keyed by [`name_key`].
pub const NAME_KIND: &str = "log-filter-name-v1";

/// Fingerprint classes, in the top two bits so each class sorts together.
pub const CLASS_AUTHOR: u64 = 0;
pub const CLASS_KEY: u64 = 1;
pub const CLASS_VALUE: u64 = 2;
const CLASSES: u64 = 3;

/// `extra` keys whose values are indexed; every key's presence is. Other
/// values (prompts, request ids) are mostly unique, so indexing them would
/// grow every level for little gain; a filter on them is narrowed by the
/// key's presence.
pub const INDEXED_VALUES: &[&str] = &[EXTRA_AGENT_MODEL, EXTRA_AGENT_SESSION];

/// Most fingerprints of one class a level keeps. Past it the class is marked
/// as overflowing (anything may be present) and its fingerprints dropped, so
/// a row stays bounded however many distinct sessions a stretch holds.
pub const LEVEL_CAP: usize = 1 << 16;

fn fingerprint(class: u64, parts: &[&[u8]]) -> u64 {
    let mut buf = vec![class as u8];
    for p in parts {
        buf.extend_from_slice(p);
        buf.push(0);
    }
    let h = Hash::of_object("log-filter-fp", &buf);
    let v = u64::from_be_bytes(h.0[..8].try_into().expect("8 bytes"));
    (v >> 2) | (class << 62)
}

/// Fingerprint of an author, case-folded the way the author filter compares.
pub fn author_fingerprint(author: &str) -> u64 {
    fingerprint(CLASS_AUTHOR, &[author.to_lowercase().as_bytes()])
}

/// Fingerprint of an `extra` key's presence.
pub fn key_fingerprint(key: &str) -> u64 {
    fingerprint(CLASS_KEY, &[key.as_bytes()])
}

/// Fingerprint of an `extra` key with a value; only for [`INDEXED_VALUES`].
pub fn value_fingerprint(key: &str, value: &str) -> u64 {
    fingerprint(CLASS_VALUE, &[key.as_bytes(), value.as_bytes()])
}

/// Where the name behind an author fingerprint is stored.
pub fn name_key(fp: u64) -> Hash {
    Hash::of_object(NAME_KIND, &fp.to_be_bytes())
}

/// The class of a fingerprint.
pub fn class_of(fp: u64) -> u64 {
    fp >> 62
}

/// Fingerprints of one changeset's own values.
pub fn own_fingerprints(cs: &Changeset) -> Vec<u64> {
    let mut out = vec![author_fingerprint(&cs.author)];
    for (k, v) in &cs.extra {
        out.push(key_fingerprint(k));
        if INDEXED_VALUES.contains(&k.as_str()) {
            out.push(value_fingerprint(k, v));
        }
    }
    out
}

/// One level: sorted, distinct fingerprints, and a bit per class that
/// overflowed [`LEVEL_CAP`] (its fingerprints are then absent).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Level {
    pub fps: Vec<u64>,
    pub overflow: u8,
}

impl Level {
    pub fn of(mut fps: Vec<u64>) -> Level {
        fps.sort_unstable();
        fps.dedup();
        let mut l = Level { fps, overflow: 0 };
        l.cap();
        l
    }

    /// Both levels' values.
    pub fn union(&self, other: &Level) -> Level {
        let (a, b) = (&self.fps, &other.fps);
        let mut fps = Vec::with_capacity(a.len() + b.len());
        let (mut i, mut j) = (0, 0);
        while i < a.len() && j < b.len() {
            match a[i].cmp(&b[j]) {
                std::cmp::Ordering::Less => {
                    fps.push(a[i]);
                    i += 1;
                }
                std::cmp::Ordering::Greater => {
                    fps.push(b[j]);
                    j += 1;
                }
                std::cmp::Ordering::Equal => {
                    fps.push(a[i]);
                    i += 1;
                    j += 1;
                }
            }
        }
        fps.extend_from_slice(&a[i..]);
        fps.extend_from_slice(&b[j..]);
        let mut l = Level {
            fps,
            overflow: self.overflow | other.overflow,
        };
        l.cap();
        l
    }

    fn cap(&mut self) {
        for class in 0..CLASSES {
            let r = self.range(class);
            if self.overflow & (1 << class) != 0 || r.len() > LEVEL_CAP {
                self.overflow |= 1 << class;
                self.fps.drain(r);
            }
        }
    }

    fn range(&self, class: u64) -> std::ops::Range<usize> {
        let lo = self.fps.partition_point(|&f| class_of(f) < class);
        let hi = self.fps.partition_point(|&f| class_of(f) <= class);
        lo..hi
    }

    /// This level's fingerprints of `class`, or `None` if it overflowed.
    pub fn class(&self, class: u64) -> Option<&[u64]> {
        (self.overflow & (1 << class) == 0).then(|| &self.fps[self.range(class)])
    }

    /// Whether any of `want` (sorted, all of `class`) may occur here.
    pub fn may_contain_any(&self, class: u64, want: &[u64]) -> bool {
        let Some(have) = self.class(class) else {
            return true;
        };
        let (small, large) = if want.len() <= have.len() {
            (want, have)
        } else {
            (have, want)
        };
        small.iter().any(|f| large.binary_search(f).is_ok())
    }
}

/// A decoded `log-filter-v1` value.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FilterRow {
    /// First-parent steps to a root.
    pub depth: u64,
    /// Generation of the nearest merge on the first-parent chain, the
    /// changeset included; 0 if none (as `last_merge-v1`). A level whose far
    /// end ranks at or above it spans no merge.
    pub last_merge: u64,
    /// Levels `0..=tz(depth)`; a root has level 0 only.
    pub levels: Vec<Level>,
    /// At a merge, per parent after the first: the first changeset on that
    /// parent's first-parent chain that the first parent reaches, with its
    /// generation, or `None` if the chain reaches a root first. Lets a log
    /// walk into the side stop where the first parent's walk takes over.
    pub stops: Vec<Option<(u64, ChangesetId)>>,
}

fn put_varint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push(v as u8 | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

fn get_varint(b: &[u8], at: &mut usize) -> Result<u64> {
    let mut v = 0u64;
    for shift in (0..64).step_by(7) {
        let byte = *b
            .get(*at)
            .ok_or_else(|| Error::Encoding("truncated log-filter value".into()))?;
        *at += 1;
        v |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(v);
        }
    }
    Err(Error::Encoding("bad varint in log-filter value".into()))
}

/// Levels a changeset at `depth` stores.
pub fn level_count(depth: u64) -> usize {
    if depth == 0 {
        1
    } else {
        depth.trailing_zeros() as usize + 1
    }
}

impl FilterRow {
    /// Varint depth, last merge and level count, then per level an overflow byte, a
    /// varint count and the fingerprints as varint deltas, ascending.
    pub fn encode(&self) -> Bytes {
        let mut out = Vec::new();
        put_varint(&mut out, self.depth);
        put_varint(&mut out, self.last_merge);
        put_varint(&mut out, self.levels.len() as u64);
        for l in &self.levels {
            out.push(l.overflow);
            put_varint(&mut out, l.fps.len() as u64);
            let mut prev = 0u64;
            for &f in &l.fps {
                put_varint(&mut out, f - prev);
                prev = f;
            }
        }
        put_varint(&mut out, self.stops.len() as u64);
        for stop in &self.stops {
            match stop {
                None => out.push(0),
                Some((g, id)) => {
                    out.push(1);
                    put_varint(&mut out, *g);
                    out.extend_from_slice(id.0.as_bytes());
                }
            }
        }
        Bytes::from(out)
    }

    pub fn decode(b: &[u8]) -> Result<FilterRow> {
        let mut at = 0;
        let depth = get_varint(b, &mut at)?;
        let last_merge = get_varint(b, &mut at)?;
        let n = get_varint(b, &mut at)? as usize;
        if n != level_count(depth) {
            return Err(Error::Encoding("bad log-filter level count".into()));
        }
        let mut levels = Vec::with_capacity(n);
        for _ in 0..n {
            let overflow = *b
                .get(at)
                .ok_or_else(|| Error::Encoding("truncated log-filter value".into()))?;
            at += 1;
            let count = get_varint(b, &mut at)? as usize;
            if count > b.len() {
                return Err(Error::Encoding("bad log-filter count".into()));
            }
            let mut fps = Vec::with_capacity(count);
            let mut prev = 0u64;
            for i in 0..count {
                let d = get_varint(b, &mut at)?;
                if i > 0 && d == 0 {
                    return Err(Error::Encoding("unsorted log-filter level".into()));
                }
                prev = prev
                    .checked_add(d)
                    .ok_or_else(|| Error::Encoding("bad log-filter delta".into()))?;
                fps.push(prev);
            }
            levels.push(Level { fps, overflow });
        }
        let n = get_varint(b, &mut at)? as usize;
        if n > b.len() {
            return Err(Error::Encoding("bad log-filter stop count".into()));
        }
        let mut stops = Vec::with_capacity(n);
        for _ in 0..n {
            let tag = *b
                .get(at)
                .ok_or_else(|| Error::Encoding("truncated log-filter value".into()))?;
            at += 1;
            stops.push(match tag {
                0 => None,
                1 => {
                    let g = get_varint(b, &mut at)?;
                    let id: [u8; 32] = b
                        .get(at..at + 32)
                        .and_then(|s| s.try_into().ok())
                        .ok_or_else(|| Error::Encoding("truncated log-filter stop".into()))?;
                    at += 32;
                    Some((g, ChangesetId(Hash(id))))
                }
                _ => return Err(Error::Encoding("bad log-filter stop".into())),
            });
        }
        if at != b.len() {
            return Err(Error::Encoding("trailing bytes in log-filter value".into()));
        }
        Ok(FilterRow {
            depth,
            last_merge,
            levels,
            stops,
        })
    }

    /// Bytes counted against the row cache, about the decoded size.
    fn weight(&self) -> usize {
        64 + self
            .levels
            .iter()
            .map(|l| 32 + 8 * l.fps.len())
            .sum::<usize>()
    }
}

/// Bytes of decoded rows a `Repo` keeps.
const ROW_CACHE_BYTES: usize = 64 << 20;

/// Names kept in memory, counted at their length plus overhead.
const NAME_CACHE_BYTES: usize = 16 << 20;

/// Bytes of skiplists kept for filtered logs.
const SKIP_CACHE_BYTES: usize = 32 << 20;

/// Bytes of [`own_fingerprints`] kept, counting about 80 bytes of overhead
/// per changeset: a few hundred thousand changesets.
const FPS_CACHE_BYTES: usize = 48 << 20;

/// What the caches below hold in total at their defaults.
pub(crate) const LOG_FILTER_CACHE_BYTES: usize =
    ROW_CACHE_BYTES + NAME_CACHE_BYTES + SKIP_CACHE_BYTES + FPS_CACHE_BYTES;

/// Author fingerprints whose name row this process has staged, so each
/// author's row is written once per process rather than once per changeset.
const WRITTEN_NAMES: usize = 1 << 20;

/// Per-`Repo` caches for this kind. Rows and names are immutable, so entries
/// never go stale.
#[derive(Default)]
pub(crate) struct LogFilterCaches {
    rows: SizedCache<FilterRow, ROW_CACHE_BYTES>,
    names: SizedCache<Option<String>, NAME_CACHE_BYTES>,
    /// Skiplists read by filtered logs, which `Repo`'s skiplist cache does
    /// not keep.
    skips: SizedCache<Arc<[SkipEntry]>, SKIP_CACHE_BYTES>,
    /// Fingerprints of changesets' own values, which merge walks read for
    /// every changeset a side brings in, often the same ones merge after
    /// merge.
    fps: SizedCache<Vec<u64>, FPS_CACHE_BYTES>,
    written: Mutex<HashSet<u64>>,
    /// [`SIDE_BUDGET`] unless set otherwise (0: the default).
    side_budget: AtomicUsize,
}

impl LogFilterCaches {
    pub(crate) fn scaled(s: CacheScale) -> Self {
        LogFilterCaches {
            rows: SizedCache::scaled(s),
            names: SizedCache::scaled(s),
            skips: SizedCache::scaled(s),
            fps: SizedCache::scaled(s),
            ..Default::default()
        }
    }

    fn side_budget(&self) -> usize {
        match self.side_budget.load(Ordering::Relaxed) {
            0 => SIDE_BUDGET,
            n => n,
        }
    }

    /// Give up on merge sides past `n` changesets: tests lower it, and
    /// `verify` lifts it to tell rows written without it from damage.
    pub(crate) fn set_side_budget(&self, n: usize) {
        self.side_budget.store(n, Ordering::Relaxed);
    }

    /// Use `other`'s side budget: verification recomputes rows as the
    /// repository it checks would.
    pub(crate) fn copy_side_budget(&self, other: &LogFilterCaches) {
        let n = other.side_budget.load(Ordering::Relaxed);
        self.side_budget.store(n, Ordering::Relaxed);
    }
}

impl Repo {
    /// The `log-filter-v1` row of a stored changeset, deriving it if needed.
    pub(crate) async fn filter_row(&self, id: ChangesetId) -> Result<Arc<FilterRow>> {
        if let Some(r) = self.log_filter.rows.get(&id.0) {
            return Ok(r);
        }
        let row = Arc::new(FilterRow::decode(
            &derive::<LogFilterIndex>(self, id).await?,
        )?);
        let weight = row.weight();
        self.log_filter.rows.insert(id.0, row.clone(), weight / 2);
        Ok(row)
    }

    /// Fingerprints of `cs`'s own values, kept for later merge walks.
    fn own_fps(&self, id: ChangesetId, cs: &Changeset) -> Arc<Vec<u64>> {
        let fps = Arc::new(own_fingerprints(cs));
        self.log_filter
            .fps
            .insert(id.0, fps.clone(), 24 + 8 * fps.len());
        fps
    }

    fn cached_own_fps(&self, id: ChangesetId) -> Option<Arc<Vec<u64>>> {
        self.log_filter.fps.get(&id.0)
    }

    /// A row this process has decoded, without a read.
    pub(crate) fn cached_filter_row(&self, id: ChangesetId) -> Option<Arc<FilterRow>> {
        self.log_filter.rows.get(&id.0)
    }

    /// Keep a row read in a batch.
    pub(crate) fn cache_filter_row(&self, id: ChangesetId, row: Arc<FilterRow>) {
        let weight = row.weight();
        self.log_filter.rows.insert(id.0, row, weight / 2);
    }

    pub(crate) fn cached_skips(&self, id: ChangesetId) -> Option<Arc<[SkipEntry]>> {
        self.log_filter.skips.get(&id.0).map(|s| (*s).clone())
    }

    pub(crate) fn cache_skips(&self, id: ChangesetId, sk: Arc<[SkipEntry]>) {
        let n = 40 * sk.len();
        self.log_filter.skips.insert(id.0, Arc::new(sk), n);
    }

    /// Names behind author fingerprints, `None` where no name row exists (a
    /// caller must then assume the name matches).
    pub(crate) async fn filter_names(&self, fps: &[u64]) -> Result<Vec<Option<String>>> {
        let mut out: Vec<Option<Arc<Option<String>>>> = fps
            .iter()
            .map(|&f| self.log_filter.names.get(&name_key(f)))
            .collect();
        let missing: Vec<usize> = (0..fps.len()).filter(|&i| out[i].is_none()).collect();
        for chunk in missing.chunks(1024) {
            let keys: Vec<Hash> = chunk.iter().map(|&i| name_key(fps[i])).collect();
            let rows = match self.meta().get_derived_many(NAME_KIND, &keys).await {
                Ok(rows) if rows.len() == keys.len() => rows,
                _ => {
                    let mut rows = Vec::with_capacity(keys.len());
                    for k in &keys {
                        rows.push(self.meta().get_derived(NAME_KIND, k).await?);
                    }
                    rows
                }
            };
            for ((&i, key), row) in chunk.iter().zip(keys).zip(rows) {
                let name = row.and_then(|b| String::from_utf8(b.to_vec()).ok());
                // A name that does not hash to its key is treated as absent.
                let name = name.filter(|n| author_fingerprint(n) == fps[i]);
                let v = Arc::new(name);
                // Absent names are not cached: a later write may add them.
                if let Some(n) = v.as_ref() {
                    self.log_filter.names.insert(key, v.clone(), 64 + n.len());
                }
                out[i] = Some(v);
            }
        }
        Ok(out
            .into_iter()
            .map(|v| v.and_then(|v| (*v).clone()))
            .collect())
    }

    /// Whether this process still has to write the name row of `fp`; marks it
    /// as written.
    fn name_unwritten(&self, fp: u64) -> bool {
        let mut w = self.log_filter.written.lock().unwrap();
        if w.len() >= WRITTEN_NAMES {
            w.clear();
        }
        w.insert(fp)
    }
}

/// Most changesets one merge's side walk ([`merge_side`]) visits, counting
/// both those its other parents bring in and the first parent's ancestors
/// walked to tell them apart, so one merge never holds derivation up for
/// long. Past it, level 0 is a superset found from the sides' levels (see
/// [`given_up`]). Rows depend on it: changing it changes the rows of merges
/// between the two values (`verify` would report them), never answers.
pub const SIDE_BUDGET: usize = 200_000;

const FIRST: u8 = 1;
const SIDE: u8 = 2;

/// What a merge's other parents bring in: the values of exactly the
/// changesets reachable from one of `sides` but not from `first`, and per
/// side its stop (see [`FilterRow::stops`]).
///
/// One walk over both histories, newest first by (generation, id), marking
/// each changeset with the sides that reach it: `first`, or only `sides`.
/// A changeset pops only after every walked changeset that reaches it, so
/// its mark is final then: marked by `sides` alone, it is brought in. The
/// walk ends when no changeset marked by `sides` alone is left, since every
/// one not yet reached is then behind a changeset `first` reaches. That is
/// `git rev-list first..side`, and it costs what the side brings in plus the
/// first parent's history down to the oldest changeset brought in, once per
/// merge however many merges the side holds. A side's stop is where its
/// first-parent chain first meets a changeset marked by `first`.
///
/// Past `budget` changesets the walk gives up (see [`given_up`]).
pub(crate) async fn merge_side(
    repo: &Repo,
    first: ChangesetId,
    sides: &[ChangesetId],
    budget: usize,
) -> Result<(Level, Vec<Option<(u64, ChangesetId)>>)> {
    let mut walk = Paint::default();
    let mut ids = vec![first];
    ids.extend_from_slice(sides);
    let gens = walk.reader.generations(repo, &ids, u64::MAX).await?;
    walk.mark(first, gens[0], FIRST);
    for (&s, &g) in sides.iter().zip(&gens[1..]) {
        walk.mark(s, g, SIDE);
    }
    // Per side: the changeset on its first-parent chain still to be told
    // apart, or its stop.
    let mut stops: Vec<Result<Option<(u64, ChangesetId)>, ChangesetId>> =
        sides.iter().map(|&s| Err(s)).collect();
    let mut fps = Vec::new();
    let mut visited = 0usize;
    while walk.side_only > 0 {
        if visited == budget {
            return given_up(repo, first, sides, stops, &walk).await;
        }
        visited += 1;
        let (g, x) = walk.heap.pop().expect("marked changesets are queued");
        let (m, _) = walk.marks.remove(&x).expect("queued changesets are marked");
        if m & FIRST != 0 {
            for stop in stops.iter_mut() {
                if *stop == Err(x) {
                    *stop = Ok(Some((g, x)));
                }
            }
            let parents = match repo.cached_parents(x) {
                Some(p) => {
                    // Read ahead anyway: keep its values for later walks.
                    if let Some(cs) = walk.reader.take_read(x) {
                        repo.own_fps(x, &cs);
                    }
                    p
                }
                None => {
                    let cs = walk.reader.changeset(repo, x, g).await?;
                    repo.own_fps(x, &cs);
                    cs.parents.into()
                }
            };
            walk.queue(repo, &parents, g, FIRST).await?;
        } else {
            walk.side_only -= 1;
            let (parents, own) = match (repo.cached_parents(x), repo.cached_own_fps(x)) {
                (Some(p), Some(own)) => (p, own),
                _ => {
                    let cs = walk.reader.changeset(repo, x, g).await?;
                    let own = repo.own_fps(x, &cs);
                    (cs.parents.into(), own)
                }
            };
            fps.extend_from_slice(&own);
            if fps.len() > 1 << 20 {
                fps.sort_unstable();
                fps.dedup();
            }
            for stop in stops.iter_mut() {
                if *stop == Err(x) {
                    *stop = match parents.first() {
                        Some(&p) => Err(p),
                        None => Ok(None),
                    };
                }
            }
            walk.queue(repo, &parents, g, SIDE).await?;
        }
    }
    // Every changeset still queued is marked by `first`, a chain's next
    // changeset among them.
    let stops = stops
        .into_iter()
        .map(|stop| match stop {
            Ok(stop) => stop,
            Err(x) => walk.marks.get(&x).map(|&(_, g)| (g, x)),
        })
        .collect();
    Ok((Level::of(fps), stops))
}

/// [`merge_side`] past its budget: each side's stop as far as the walk got,
/// else the first changeset both first-parent chains share, and the levels
/// down each side's chain to its stop. Those cover every changeset the side
/// reaches but its stop does not, a superset of what it brings in since
/// `first` reaches the stop.
async fn given_up(
    repo: &Repo,
    first: ChangesetId,
    sides: &[ChangesetId],
    stops: Vec<Result<Option<(u64, ChangesetId)>, ChangesetId>>,
    walk: &Paint,
) -> Result<(Level, Vec<Option<(u64, ChangesetId)>>)> {
    let mut level = Level::default();
    let mut out = Vec::with_capacity(stops.len());
    for (stop, &side) in stops.into_iter().zip(sides) {
        let stop = match stop {
            Ok(stop) => stop,
            Err(x) => match walk.marks.get(&x) {
                Some(&(m, g)) if m & FIRST != 0 => Some((g, x)),
                _ => match repo.first_parent_lca(first, side, 64).await? {
                    Some(c) => Some((repo.generation(c).await?, c)),
                    None => None,
                },
            },
        };
        level = level.union(&chain_levels(repo, side, stop.map(|s| s.1)).await?);
        out.push(stop);
    }
    Ok((level, out))
}

/// The union of levels covering `from`'s first-parent chain down to `to`
/// (exclusive; `None`: to the root, inclusive): every changeset `from`
/// reaches and `to` does not. `to` must be on the chain.
async fn chain_levels(repo: &Repo, from: ChangesetId, to: Option<ChangesetId>) -> Result<Level> {
    let floor = match to {
        Some(c) => repo.filter_row(c).await?.depth,
        None => 0,
    };
    let mut acc = Level::default();
    let mut y = from;
    while Some(y) != to {
        let row = repo.filter_row(y).await?;
        if row.depth == 0 {
            acc = acc.union(&row.levels[0]);
            break;
        }
        let sk = repo.skiplist_noting_generations(y).await?;
        let span = row
            .depth
            .checked_sub(floor)
            .filter(|&s| s > 0)
            .ok_or_else(|| {
                Error::Storage(format!("{to:?} is not on the first-parent chain of {from}"))
            })?;
        let j = (0..row.levels.len().min(sk.len()))
            .rev()
            .find(|&j| 1u64 << j <= span)
            .ok_or_else(|| Error::Storage(format!("skiplist of {y} is empty")))?;
        acc = acc.union(&row.levels[j]);
        y = sk[j].id;
    }
    Ok(acc)
}

struct Paint {
    reader: Reader,
    /// Queued changesets: their marks and generations. A changeset is
    /// queued at most once, since everything that reaches it ranks above it
    /// and pops first.
    marks: HashMap<ChangesetId, (u8, u64), ByKey>,
    heap: BinaryHeap<(u64, ChangesetId)>,
    /// Queued changesets marked by the sides alone.
    side_only: usize,
}

impl Default for Paint {
    fn default() -> Self {
        Paint {
            // What a walk needs of a changeset is its parents, generation
            // and values, all kept once read.
            reader: Reader::skipping(|repo, id| {
                repo.cached_parents(id).is_some()
                    && repo.cached_generation(id).is_some()
                    && repo.cached_own_fps(id).is_some()
            }),
            marks: HashMap::default(),
            heap: BinaryHeap::new(),
            side_only: 0,
        }
    }
}

impl Paint {
    fn mark(&mut self, id: ChangesetId, generation: u64, m: u8) {
        match self.marks.get_mut(&id) {
            Some((old, _)) => {
                if *old == SIDE && m & FIRST != 0 {
                    self.side_only -= 1;
                }
                *old |= m;
            }
            None => {
                self.marks.insert(id, (m, generation));
                self.heap.push((generation, id));
                if m == SIDE {
                    self.side_only += 1;
                }
            }
        }
    }

    async fn queue(&mut self, repo: &Repo, parents: &[ChangesetId], at: u64, m: u8) -> Result<()> {
        let mut new = Vec::new();
        for &p in parents {
            match self.marks.get(&p) {
                Some(&(_, g)) => self.mark(p, g, m),
                None if !new.contains(&p) => new.push(p),
                None => {}
            }
        }
        let gens = self.reader.generations(repo, &new, at).await?;
        for (p, g) in new.into_iter().zip(gens) {
            self.mark(p, g, m);
        }
        Ok(())
    }
}

impl LogFilterIndex {
    /// The row of `id`, and the name rows it introduces.
    async fn compute(
        repo: &Repo,
        id: ChangesetId,
        cs: &Changeset,
        parents: &[Bytes],
        names: &mut Vec<DerivedRow>,
    ) -> Result<FilterRow> {
        let author = author_fingerprint(&cs.author);
        if repo.name_unwritten(author) {
            names.push(DerivedRow::new(
                NAME_KIND,
                name_key(author),
                Bytes::from(cs.author.to_lowercase()),
            ));
        }
        let mut level0 = Level::of(own_fingerprints(cs));
        let Some(&first) = cs.parents.first() else {
            return Ok(FilterRow {
                depth: 0,
                last_merge: 0,
                levels: vec![level0],
                stops: vec![],
            });
        };
        let p1 = FilterRow::decode(&parents[0])?;
        let depth = p1.depth + 1;
        let last_merge = if cs.parents.len() > 1 {
            repo.generation(id).await?
        } else {
            p1.last_merge
        };
        let mut stops = Vec::new();
        if cs.parents.len() > 1 {
            let budget = repo.log_filter.side_budget();
            let (side, s) = merge_side(repo, first, &cs.parents[1..], budget).await?;
            level0 = level0.union(&side);
            stops = s;
        }
        let mut levels = vec![level0];
        let n = level_count(depth);
        if n > 1 {
            let sk = repo.skiplist_noting_generations(id).await?;
            for j in 1..n {
                let a = sk
                    .get(j - 1)
                    .ok_or_else(|| Error::Storage(format!("skiplist of {id} too short")))?;
                let row = repo.filter_row(a.id).await?;
                let lower = row.levels.get(j - 1).ok_or_else(|| {
                    Error::Storage(format!("{} of {} lacks level {}", Self::NAME, a.id, j - 1))
                })?;
                let next = levels[j - 1].union(lower);
                levels.push(next);
            }
        }
        Ok(FilterRow {
            depth,
            last_merge,
            levels,
            stops,
        })
    }
}

#[async_trait]
impl DerivedKind for LogFilterIndex {
    const NAME: &'static str = "log-filter-v1";

    async fn derive(
        repo: &Repo,
        id: ChangesetId,
        cs: &Changeset,
        parents: &[Bytes],
    ) -> Result<Bytes> {
        let mut names = Vec::new();
        let row = Self::compute(repo, id, cs, parents, &mut names).await?;
        for r in names {
            repo.meta().put_derived(&r.kind, &r.key, r.value).await?;
        }
        Ok(row.encode())
    }

    const STAGED: bool = true;

    const KEEP_SKIPLIST_READS: bool = true;

    async fn derive_staged(
        repo: &Repo,
        id: ChangesetId,
        cs: &Changeset,
        parents: &[Bytes],
        rows: &mut Vec<DerivedRow>,
    ) -> Result<Bytes> {
        Ok(Self::compute(repo, id, cs, parents, rows).await?.encode())
    }

    /// Name rows dropped after an error must be staged again by whichever
    /// derivation next meets the author.
    fn dropped(repo: &Repo, rows: &[DerivedRow]) {
        let mut written = repo.log_filter.written.lock().unwrap();
        for r in rows.iter().filter(|r| r.kind == NAME_KIND) {
            if let Ok(name) = std::str::from_utf8(&r.value) {
                written.remove(&author_fingerprint(name));
            }
        }
    }

    /// Generations of the window and its parents, which every merge's walk
    /// starts from, and the window's skiplists, which name the ancestors
    /// whose levels the upper levels unite: one batch each instead of a read
    /// per changeset.
    async fn prepare(repo: &Repo, window: &[(ChangesetId, &Changeset)]) {
        let own: Vec<ChangesetId> = window.iter().map(|(id, _)| *id).collect();
        let mut ids = own.clone();
        for (id, cs) in window {
            repo.remember_parents(*id, cs.parents.as_slice().into());
            ids.extend(&cs.parents);
        }
        ids.sort();
        ids.dedup();
        repo.prefetch_generations(&ids).await;
        repo.prefetch_skiplists(&own).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rows_round_trip_and_reject_damage() {
        let row = FilterRow {
            depth: 12,
            last_merge: 300,
            stops: vec![None, Some((7, ChangesetId(Hash([3; 32]))))],
            levels: vec![
                Level::of(vec![5, 1, 1 << 62, 3]),
                Level::of(vec![]),
                Level {
                    fps: vec![],
                    overflow: 0b10,
                },
            ],
        };
        let b = row.encode();
        assert_eq!(FilterRow::decode(&b).unwrap(), row);
        assert!(FilterRow::decode(&b[..b.len() - 1]).is_err());
        let mut extra = b.to_vec();
        extra.push(0);
        assert!(FilterRow::decode(&extra).is_err());
        // Depth 12 has two trailing zeros, so three levels.
        let mut wrong = row.clone();
        wrong.levels.pop();
        assert!(FilterRow::decode(&wrong.encode()).is_err());
    }

    /// Fingerprints and the row encoding are persisted: any change here is a
    /// new kind name.
    #[test]
    fn golden_fingerprints_and_bytes() {
        let fps = [
            author_fingerprint("Ann <ann@example.com>"),
            key_fingerprint("agent.model"),
            value_fingerprint("agent.model", "m1"),
        ];
        assert_eq!(
            fps,
            [
                2203352080522216168,
                7087202754490893717,
                11312986384349506023
            ]
        );
        let row = FilterRow {
            depth: 2,
            last_merge: 0,
            stops: vec![],
            levels: vec![Level::of(fps.to_vec()), Level::of(vec![fps[0]])],
        };
        assert_eq!(
            hex(&row.encode()),
            "0200020003e88594d5b1a1f8c91ead85d2effe89bbe343d2c888dba7fabfd23a0001e88594d5b1a1f8c91e00"
        );
        assert_eq!(
            name_key(fps[0]).to_hex(),
            "d436bd780912bc401fd599f8e4db0b542aa7b7aec6000abe3f90721c7b3f03d7"
        );
    }

    fn hex(b: &[u8]) -> String {
        b.iter().map(|x| format!("{x:02x}")).collect()
    }

    #[test]
    fn classes_sort_apart_and_overflow_drops_a_class() {
        let a = author_fingerprint("Ann");
        assert_eq!(a, author_fingerprint("aNN"));
        assert_eq!(class_of(a), CLASS_AUTHOR);
        assert_eq!(class_of(key_fingerprint("agent.model")), CLASS_KEY);
        assert_eq!(class_of(value_fingerprint("agent.model", "m")), CLASS_VALUE);
        let l = Level::of(vec![a, key_fingerprint("k")]);
        assert_eq!(l.class(CLASS_AUTHOR), Some(&[a][..]));
        assert!(l.may_contain_any(CLASS_AUTHOR, &[a]));
        assert!(!l.may_contain_any(CLASS_AUTHOR, &[author_fingerprint("bob")]));
        let many: Vec<u64> = (0..=LEVEL_CAP as u64).map(|i| (2 << 62) | i).collect();
        let big = Level::of(many).union(&l);
        assert_eq!(big.overflow, 1 << CLASS_VALUE);
        assert!(big.class(CLASS_VALUE).is_none());
        assert!(big.may_contain_any(CLASS_VALUE, &[value_fingerprint("x", "y")]));
        assert_eq!(big.class(CLASS_AUTHOR), Some(&[a][..]));
        assert_eq!(big.union(&Level::default()), big);
    }
}
