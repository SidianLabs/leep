//! Bytes of the path-history index (ADR 0045): every node, chunk and chunk
//! list one changeset's derivation writes goes into that changeset's
//! **pack**, cut into parts of about [`PART_BYTES`]. Part 0 is the
//! changeset's `history-tree-v4` value and also names its root; parts 1 and
//! up are `history-node-v4` rows keyed by [`part_key`].
//!
//! Inside a part, changeset ids are indices into the part's table of ids,
//! with 0 standing for the changeset that wrote it: a merge's hundreds of
//! rows named the same few dozen ids thousands of times. Nodes are named by
//! [`NodeRef`] (writer, part, index) instead of a 32-byte hash, so a node
//! the same changeset wrote costs three bytes to point at. Patches name the
//! keyframe entries they replace by position, and names are front-coded.

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use leep_core::{ChangesetId, Error, Hash, Result};

use crate::repo::ByKey;

use super::history_tree::{
    ChunkRef, HistoryEntry, HistoryNode, NodeRef, SplitChange, StoredHistoryNode, NODE_KIND,
    PENDING,
};

/// A part closes once adding the next node would take it past this many
/// bytes, so a lookup in a large merge's pack reads one small row, not the
/// whole pack; most changesets' packs are a single part.
pub const PART_BYTES: usize = 16384;

/// Where packs are cut: in tests, small enough that their small histories
/// have packs of several parts.
#[cfg(not(test))]
const CUT: usize = PART_BYTES;
#[cfg(test)]
const CUT: usize = 128;

/// Bytes of the checksum that starts every part.
const CHECKSUM: usize = 8;

const FLAT: u8 = 0;
const SPLIT: u8 = 1;
const PATCH: u8 = 2;
const SPLIT_PATCH: u8 = 3;

/// The `history-node-v4` key of part `part` (1 and up) of `cs`'s pack.
pub fn part_key(cs: ChangesetId, part: u32) -> Hash {
    let mut b = [0u8; 36];
    b[..32].copy_from_slice(cs.0.as_bytes());
    b[32..].copy_from_slice(&part.to_le_bytes());
    Hash::of_object(NODE_KIND, &b)
}

/// The checksum of a part's bytes after the checksum, bound to its writer
/// and position, so damage and a row stored under the wrong key are both
/// caught on read.
fn checksum(cs: ChangesetId, part: u32, rest: &[u8]) -> [u8; CHECKSUM] {
    let mut b = Vec::with_capacity(36 + rest.len());
    b.extend_from_slice(cs.0.as_bytes());
    b.extend_from_slice(&part.to_le_bytes());
    b.extend_from_slice(rest);
    let h = Hash::of_object(NODE_KIND, &b);
    h.0[..CHECKSUM].try_into().expect("checksum length")
}

fn put_varint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push(v as u8 | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

fn common_prefix(a: &str, b: &str) -> usize {
    a.bytes().zip(b.bytes()).take_while(|(x, y)| x == y).count()
}

/// `name` as the length of the prefix it shares with `prev`, then the rest.
fn put_name(out: &mut Vec<u8>, prev: &str, name: &str) {
    let p = common_prefix(prev, name);
    put_varint(out, p as u64);
    put_varint(out, (name.len() - p) as u64);
    out.extend_from_slice(&name.as_bytes()[p..]);
}

/// Changeset ids a part names, other than its writer's.
struct Table {
    writer: ChangesetId,
    ids: Vec<ChangesetId>,
    index: HashMap<ChangesetId, u64, ByKey>,
}

impl Table {
    fn new(writer: ChangesetId) -> Self {
        Table {
            writer,
            ids: Vec::new(),
            index: HashMap::default(),
        }
    }

    /// 0 for the writer, `i + 1` for the table's `i`th id.
    fn id(&mut self, id: ChangesetId) -> u64 {
        if id == self.writer {
            return 0;
        }
        if let Some(&i) = self.index.get(&id) {
            return i;
        }
        self.ids.push(id);
        let i = self.ids.len() as u64;
        self.index.insert(id, i);
        i
    }

    fn truncate(&mut self, len: usize) {
        for id in self.ids.drain(len..) {
            self.index.remove(&id);
        }
    }

    fn encoded_len(&self) -> usize {
        varint_len(self.ids.len() as u64) + 32 * self.ids.len()
    }
}

fn varint_len(mut v: u64) -> usize {
    let mut n = 1;
    while v >= 0x80 {
        v >>= 7;
        n += 1;
    }
    n
}

/// Where the encoder places a node of the pack being written, by its
/// staging index: the part field and index it is written with.
type SelfRefs<'a> = &'a dyn Fn(u32) -> (u64, u64);

struct Encoder<'a> {
    out: Vec<u8>,
    table: Table,
    self_refs: SelfRefs<'a>,
}

impl Encoder<'_> {
    /// A node: its writer (0 for this pack), then the part (counted from the
    /// referring part for this pack, absolute otherwise) and index.
    fn node_ref(&mut self, r: NodeRef) {
        if r.part == PENDING {
            debug_assert_eq!(
                r.cs, self.table.writer,
                "only this pack's nodes are pending"
            );
            let (part, idx) = (self.self_refs)(r.idx);
            put_varint(&mut self.out, 0);
            put_varint(&mut self.out, part);
            put_varint(&mut self.out, idx);
        } else {
            debug_assert_ne!(
                r.cs, self.table.writer,
                "a pack refers to itself while pending"
            );
            let i = self.table.id(r.cs);
            put_varint(&mut self.out, i);
            put_varint(&mut self.out, r.part as u64);
            put_varint(&mut self.out, r.idx as u64);
        }
    }

    /// An entry after its name: `(ids << 1) | tree`, the ids, the node.
    fn entry(&mut self, e: &HistoryEntry) {
        let tree = matches!(e, HistoryEntry::Tree { .. });
        put_varint(
            &mut self.out,
            ((e.changed().len() as u64) << 1) | tree as u64,
        );
        for id in e.changed() {
            let i = self.table.id(*id);
            put_varint(&mut self.out, i);
        }
        if let HistoryEntry::Tree { node, .. } = e {
            self.node_ref(*node);
        }
    }

    /// A name of a patch: the step from the previous name of the keyframe it
    /// replaces (at least 1), or 0 and the name, front-coded against the
    /// previous new one.
    fn slot<'n>(
        &mut self,
        key: &[&str],
        name: &'n str,
        prev_pos: &mut i64,
        prev_new: &mut &'n str,
    ) {
        match key.binary_search(&name) {
            Ok(p) => {
                put_varint(&mut self.out, (p as i64 - *prev_pos) as u64);
                *prev_pos = p as i64;
            }
            Err(_) => {
                put_varint(&mut self.out, 0);
                put_name(&mut self.out, prev_new, name);
                *prev_new = name;
            }
        }
    }

    fn node(&mut self, node: &StoredHistoryNode, key: Option<&StoredHistoryNode>) {
        match node {
            StoredHistoryNode::Flat(n) => {
                self.out.push(FLAT);
                put_varint(&mut self.out, n.entries.len() as u64);
                let mut prev = "";
                for (name, e) in &n.entries {
                    put_name(&mut self.out, prev, name);
                    prev = name;
                    self.entry(e);
                }
            }
            StoredHistoryNode::Split(refs) => {
                self.out.push(SPLIT);
                put_varint(&mut self.out, refs.len() as u64);
                let mut prev = "";
                for r in refs {
                    put_name(&mut self.out, prev, &r.first);
                    prev = &r.first;
                    self.node_ref(r.node);
                }
            }
            StoredHistoryNode::Patch { base, over } => {
                let names: Vec<&str> = match key {
                    Some(StoredHistoryNode::Flat(k)) => {
                        k.entries.keys().map(|n| n.as_str()).collect()
                    }
                    _ => Vec::new(),
                };
                self.out.push(PATCH);
                self.node_ref(*base);
                put_varint(&mut self.out, over.entries.len() as u64);
                let (mut prev_pos, mut prev_new) = (-1, "");
                for (name, e) in &over.entries {
                    self.slot(&names, name, &mut prev_pos, &mut prev_new);
                    self.entry(e);
                }
            }
            StoredHistoryNode::SplitPatch { base, refs } => {
                let firsts: Vec<&str> = match key {
                    Some(StoredHistoryNode::Split(k)) => {
                        k.iter().map(|r| r.first.as_str()).collect()
                    }
                    _ => Vec::new(),
                };
                self.out.push(SPLIT_PATCH);
                self.node_ref(*base);
                put_varint(&mut self.out, refs.len() as u64);
                let (mut prev_pos, mut prev_new) = (-1, "");
                for (first, change) in refs {
                    self.slot(&firsts, first, &mut prev_pos, &mut prev_new);
                    match change {
                        None => put_varint(&mut self.out, 0),
                        Some(c) => {
                            put_varint(&mut self.out, 1);
                            self.node_ref(c.node);
                        }
                    }
                }
            }
        }
    }
}

/// Bytes `node` costs stored on its own (with an id table of its own, and
/// this pack's nodes three bytes each): what the patch rule compares. A
/// function of the node, its keyframe and its writer only, so the rows a
/// derivation writes do not depend on how its pack is cut.
pub fn cost(
    writer: ChangesetId,
    node: &StoredHistoryNode,
    key: Option<&StoredHistoryNode>,
) -> usize {
    let mut enc = Encoder {
        out: Vec::new(),
        table: Table::new(writer),
        self_refs: &|_| (0, 0),
    };
    enc.node(node, key);
    enc.out.len() + 32 * enc.table.ids.len()
}

/// A node staged for a pack: its content, with this pack's nodes as
/// pending references, and the keyframe a patch names entries of.
#[derive(Clone, Debug, PartialEq)]
struct Staged {
    node: StoredHistoryNode,
    key: Option<Arc<StoredHistoryNode>>,
}

/// The nodes one changeset's derivation writes, in the order written:
/// children before the nodes that name them, so the root comes last.
#[derive(Clone)]
pub struct PackBuilder {
    pub cs: ChangesetId,
    staged: Vec<Staged>,
}

/// A finished pack: part 0 (the `history-tree-v4` value), the other parts
/// as `history-node-v4` rows, and the nodes with their final references.
pub struct Pack {
    pub cs: ChangesetId,
    pub value: Bytes,
    pub rows: Vec<(Hash, Bytes)>,
    pub nodes: Vec<(NodeRef, Arc<StoredHistoryNode>, usize)>,
}

impl PackBuilder {
    pub fn new(cs: ChangesetId) -> Self {
        PackBuilder {
            cs,
            staged: Vec::new(),
        }
    }

    /// Add a node; `key` is the keyframe a patch is on. Returns the node's
    /// pending reference, valid inside this pack until [`PackBuilder::finish`].
    pub fn stage(
        &mut self,
        node: StoredHistoryNode,
        key: Option<Arc<StoredHistoryNode>>,
    ) -> NodeRef {
        self.staged.push(Staged { node, key });
        NodeRef {
            cs: self.cs,
            part: PENDING,
            idx: (self.staged.len() - 1) as u32,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.staged.is_empty()
    }

    pub fn len(&self) -> usize {
        self.staged.len()
    }

    /// The nodes staged from the `from`th on, for tests that compare two
    /// ways of writing the same edit.
    #[cfg(test)]
    pub(crate) fn tail(&self, from: usize) -> Vec<StoredHistoryNode> {
        self.staged[from..].iter().map(|s| s.node.clone()).collect()
    }

    /// Cut the staged nodes into parts, in staging order, and encode them.
    /// Parts are numbered from the last, so the root's is part 0.
    pub fn finish(self, root: NodeRef) -> Pack {
        let cs = self.cs;
        // (creation part, index) of each staged node.
        let mut place: Vec<(u32, u32)> = Vec::with_capacity(self.staged.len());
        // Each part's node bytes, id table and node lengths.
        let mut parts: Vec<(Vec<u8>, Table, Vec<u32>)> = Vec::new();
        let mut cur = (Vec::new(), Table::new(cs), Vec::new());
        for s in &self.staged {
            loop {
                let (len, tlen) = (cur.0.len(), cur.1.ids.len());
                let now = parts.len() as u32;
                let refs = |seq: u32| {
                    let (pc, pi) = place[seq as usize];
                    ((now - pc) as u64, pi as u64)
                };
                let mut enc = Encoder {
                    out: std::mem::take(&mut cur.0),
                    table: std::mem::replace(&mut cur.1, Table::new(cs)),
                    self_refs: &refs,
                };
                enc.node(&s.node, s.key.as_deref());
                cur.0 = enc.out;
                cur.1 = enc.table;
                let size = cur.0.len() + cur.1.encoded_len();
                if !cur.2.is_empty() && size > CUT {
                    cur.0.truncate(len);
                    cur.1.truncate(tlen);
                    let full =
                        std::mem::replace(&mut cur, (Vec::new(), Table::new(cs), Vec::new()));
                    parts.push(full);
                    continue;
                }
                cur.2.push((cur.0.len() - len) as u32);
                break;
            }
            place.push((parts.len() as u32, cur.2.len() as u32 - 1));
        }
        // The root goes with the last part.
        let now = parts.len() as u32;
        let refs = |seq: u32| {
            let (pc, pi) = place[seq as usize];
            ((now - pc) as u64, pi as u64)
        };
        let mut enc = Encoder {
            out: Vec::new(),
            table: std::mem::replace(&mut cur.1, Table::new(cs)),
            self_refs: &refs,
        };
        enc.node_ref(root);
        cur.1 = enc.table;
        let mut root_bytes = enc.out;
        parts.push(cur);
        let n = parts.len() as u32;
        let abs = |c: u32| n - 1 - c;
        let mut value = Bytes::new();
        let mut rows = Vec::with_capacity(parts.len() - 1);
        for (c, (body, table, lens)) in parts.into_iter().enumerate() {
            let part = abs(c as u32);
            let mut rest = Vec::with_capacity(body.len() + table.encoded_len() + 48);
            if part != 0 {
                // A part after the first says whose it is, so a scan of
                // `history-node-v4` rows can check it against its key.
                rest.extend_from_slice(cs.0.as_bytes());
                put_varint(&mut rest, part as u64);
            }
            put_varint(&mut rest, table.ids.len() as u64);
            for id in &table.ids {
                rest.extend_from_slice(id.0.as_bytes());
            }
            if part == 0 {
                rest.append(&mut root_bytes);
            }
            // Each node's length, so a reader decodes only the node it wants.
            put_varint(&mut rest, lens.len() as u64);
            for l in &lens {
                put_varint(&mut rest, *l as u64);
            }
            rest.extend_from_slice(&body);
            let mut bytes = Vec::with_capacity(CHECKSUM + 1 + rest.len());
            bytes.extend_from_slice(&checksum(cs, part, &rest));
            compress_into(&mut bytes, &rest);
            if part == 0 {
                value = Bytes::from(bytes);
            } else {
                rows.push((part_key(cs, part), Bytes::from(bytes)));
            }
        }
        // `rows` runs from the first part written, whose nodes are named by
        // later ones: a failed write of a prefix leaves no dangling name.
        let finalize = |r: NodeRef| -> NodeRef {
            if r.part == PENDING {
                let (c, i) = place[r.idx as usize];
                NodeRef {
                    cs,
                    part: abs(c),
                    idx: i,
                }
            } else {
                r
            }
        };
        let nodes = self
            .staged
            .into_iter()
            .enumerate()
            .map(|(seq, s)| {
                let mut node = s.node;
                map_refs(&mut node, &finalize);
                let at = finalize(NodeRef {
                    cs,
                    part: PENDING,
                    idx: seq as u32,
                });
                let size = cache_size(&node);
                (at, Arc::new(node), size)
            })
            .collect();
        Pack {
            cs,
            value,
            rows,
            nodes,
        }
    }
}

/// About half the bytes `node` takes decoded: what the row caches are
/// given, since they count each value at twice its size. The encoding is
/// several times smaller than the decoded node, so its length would let
/// the caches hold far more than their budget.
pub fn cache_size(node: &StoredHistoryNode) -> usize {
    let entries = |n: &HistoryNode| -> usize {
        n.entries
            .iter()
            .map(|(name, e)| 140 + name.len() + 32 * e.changed().len())
            .sum()
    };
    let bytes = match node {
        StoredHistoryNode::Flat(n) => entries(n),
        StoredHistoryNode::Patch { over, .. } => entries(over),
        StoredHistoryNode::Split(refs) => refs.iter().map(|r| 64 + r.first.len()).sum(),
        StoredHistoryNode::SplitPatch { refs, .. } => refs
            .iter()
            .map(|(f, c)| {
                140 + f.len()
                    + c.as_ref()
                        .map_or(0, |c| c.next.as_ref().map_or(0, |n| n.len()))
            })
            .sum(),
    };
    (64 + bytes) / 2
}

/// [`cache_size`] of a node as decoded.
pub fn raw_cache_size(node: &RawNode) -> usize {
    match node {
        RawNode::Ready(n) => cache_size(n),
        RawNode::Patch { over, .. } => {
            (64 + over
                .iter()
                .map(|(_, e)| 140 + 32 * e.changed().len())
                .sum::<usize>())
                / 2
        }
        RawNode::SplitPatch { refs, .. } => (64 + 120 * refs.len()) / 2,
    }
}

/// Apply `f` to every node reference in `node`.
pub fn map_refs(node: &mut StoredHistoryNode, f: &dyn Fn(NodeRef) -> NodeRef) {
    let entries = |n: &mut HistoryNode| {
        for e in n.entries.values_mut() {
            if let HistoryEntry::Tree { node, .. } = e {
                *node = f(*node);
            }
        }
    };
    match node {
        StoredHistoryNode::Flat(n) => entries(n),
        StoredHistoryNode::Split(refs) => {
            for r in refs {
                r.node = f(r.node);
            }
        }
        StoredHistoryNode::Patch { base, over } => {
            *base = f(*base);
            entries(over);
        }
        StoredHistoryNode::SplitPatch { base, refs } => {
            *base = f(*base);
            for c in refs.values_mut().flatten() {
                c.node = f(c.node);
            }
        }
    }
}

/// Where a patch puts an entry: over the keyframe's entry at a position, or
/// under a name the keyframe lacks.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Slot {
    Pos(u32),
    Name(String),
}

/// A node as decoded from its part: a patch still names keyframe entries by
/// position, until [`RawNode::resolve`] reads them off the keyframe.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RawNode {
    Ready(StoredHistoryNode),
    Patch {
        base: NodeRef,
        over: Vec<(Slot, HistoryEntry)>,
    },
    SplitPatch {
        base: NodeRef,
        refs: Vec<(Slot, Option<NodeRef>)>,
    },
}

/// A decoded part: the root (part 0 only) and the nodes, each with about
/// the bytes it took.
pub struct DecodedPart {
    pub root: Option<NodeRef>,
    /// The changesets the part names besides its writer.
    pub table: Vec<ChangesetId>,
    pub nodes: Vec<(RawNode, usize)>,
}

struct Reader<'a> {
    b: &'a [u8],
    pos: usize,
}

fn bad(why: &str) -> Error {
    Error::Encoding(format!("history pack: {why}"))
}

impl<'a> Reader<'a> {
    fn byte(&mut self) -> Result<u8> {
        let b = *self.b.get(self.pos).ok_or_else(|| bad("truncated"))?;
        self.pos += 1;
        Ok(b)
    }

    fn varint(&mut self) -> Result<u64> {
        let mut v = 0u64;
        for shift in (0..64).step_by(7) {
            let b = self.byte()?;
            v |= ((b & 0x7f) as u64) << shift;
            if b < 0x80 {
                return Ok(v);
            }
        }
        Err(bad("varint too long"))
    }

    fn u32(&mut self) -> Result<u32> {
        u32::try_from(self.varint()?).map_err(|_| bad("number out of range"))
    }

    /// A count of items each at least one byte long.
    fn count(&mut self) -> Result<usize> {
        let n = self.varint()?;
        if n > (self.b.len() - self.pos) as u64 {
            return Err(bad("count past the end"));
        }
        Ok(n as usize)
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        if n > self.b.len() - self.pos {
            return Err(bad("truncated"));
        }
        let s = &self.b[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }

    fn name(&mut self, prev: &str) -> Result<String> {
        let p = self.varint()?;
        if p > prev.len() as u64 {
            return Err(bad("name prefix too long"));
        }
        let p = p as usize;
        let n = self.count()?;
        let mut b = prev.as_bytes()[..p].to_vec();
        b.extend_from_slice(self.take(n)?);
        String::from_utf8(b).map_err(|_| bad("name is not UTF-8"))
    }
}

struct Decoder<'a> {
    r: Reader<'a>,
    cs: ChangesetId,
    part: u32,
    table: &'a [ChangesetId],
}

impl Decoder<'_> {
    fn id(&mut self) -> Result<ChangesetId> {
        match self.r.varint()? {
            0 => Ok(self.cs),
            i => self
                .table
                .get(i as usize - 1)
                .copied()
                .ok_or_else(|| bad("id index out of range")),
        }
    }

    fn node_ref(&mut self) -> Result<NodeRef> {
        let w = self.r.varint()?;
        let part = self.r.u32()?;
        let idx = self.r.u32()?;
        if w == 0 {
            let part = self
                .part
                .checked_add(part)
                .filter(|p| *p != PENDING)
                .ok_or_else(|| bad("part out of range"))?;
            return Ok(NodeRef {
                cs: self.cs,
                part,
                idx,
            });
        }
        let cs = self
            .table
            .get(w as usize - 1)
            .copied()
            .ok_or_else(|| bad("id index out of range"))?;
        if part == PENDING {
            return Err(bad("part out of range"));
        }
        Ok(NodeRef { cs, part, idx })
    }

    fn entry(&mut self) -> Result<HistoryEntry> {
        let h = self.r.varint()?;
        let n = h >> 1;
        if n > (self.r.b.len() - self.r.pos) as u64 {
            return Err(bad("count past the end"));
        }
        let mut changed = Vec::with_capacity(n as usize);
        for _ in 0..n {
            changed.push(self.id()?);
        }
        if !changed.windows(2).all(|w| w[0] < w[1]) {
            return Err(bad("changed set not sorted"));
        }
        Ok(if h & 1 == 1 {
            HistoryEntry::Tree {
                changed,
                node: self.node_ref()?,
            }
        } else {
            HistoryEntry::Leaf { changed }
        })
    }

    fn slot(&mut self, prev_pos: &mut i64, prev_new: &mut String) -> Result<Slot> {
        match self.r.varint()? {
            0 => {
                let name = self.r.name(prev_new)?;
                if !prev_new.is_empty() && name <= *prev_new {
                    return Err(bad("names out of order"));
                }
                *prev_new = name.clone();
                Ok(Slot::Name(name))
            }
            d => {
                let p = *prev_pos + d as i64;
                if p > u32::MAX as i64 {
                    return Err(bad("position out of range"));
                }
                *prev_pos = p;
                Ok(Slot::Pos(p as u32))
            }
        }
    }

    fn node(&mut self) -> Result<RawNode> {
        Ok(match self.r.byte()? {
            FLAT => {
                let n = self.r.count()?;
                let mut node = HistoryNode::default();
                let mut prev = String::new();
                for i in 0..n {
                    let name = self.r.name(&prev)?;
                    if i > 0 && name <= prev {
                        return Err(bad("names out of order"));
                    }
                    let e = self.entry()?;
                    prev = name.clone();
                    node.entries.insert(name, e);
                }
                RawNode::Ready(StoredHistoryNode::Flat(node))
            }
            SPLIT => {
                let n = self.r.count()?;
                let mut refs: Vec<ChunkRef> = Vec::with_capacity(n);
                let mut prev = String::new();
                for i in 0..n {
                    let first = self.r.name(&prev)?;
                    if i > 0 && first <= prev {
                        return Err(bad("names out of order"));
                    }
                    prev = first.clone();
                    refs.push(ChunkRef {
                        first,
                        node: self.node_ref()?,
                    });
                }
                RawNode::Ready(StoredHistoryNode::Split(refs))
            }
            PATCH => {
                let base = self.node_ref()?;
                let n = self.r.count()?;
                let mut over = Vec::with_capacity(n);
                let (mut prev_pos, mut prev_new) = (-1i64, String::new());
                for _ in 0..n {
                    let s = self.slot(&mut prev_pos, &mut prev_new)?;
                    over.push((s, self.entry()?));
                }
                RawNode::Patch { base, over }
            }
            SPLIT_PATCH => {
                let base = self.node_ref()?;
                let n = self.r.count()?;
                let mut refs = Vec::with_capacity(n);
                let (mut prev_pos, mut prev_new) = (-1i64, String::new());
                for _ in 0..n {
                    let s = self.slot(&mut prev_pos, &mut prev_new)?;
                    let change = match self.r.varint()? {
                        0 => None,
                        1 => Some(self.node_ref()?),
                        _ => return Err(bad("bad chunk change")),
                    };
                    refs.push((s, change));
                }
                RawNode::SplitPatch { base, refs }
            }
            _ => return Err(bad("unknown node tag")),
        })
    }
}

/// A part as read: its root (part 0 only), its table of ids and its nodes'
/// bytes, each decoded only when asked for.
pub struct Part {
    pub cs: ChangesetId,
    pub part: u32,
    pub root: Option<NodeRef>,
    pub table: Vec<ChangesetId>,
    body: Vec<u8>,
    nodes: Vec<std::ops::Range<usize>>,
}

impl Part {
    /// Read part `part` of `cs`'s pack, checking its checksum. Damage is a
    /// `corrupt history node` storage error.
    pub fn read(bytes: &[u8], cs: ChangesetId, part: u32) -> Result<Part> {
        let corrupt =
            |why: String| Error::Storage(format!("corrupt history node {cs}/{part}: {why}"));
        let rest = canonical(bytes).map_err(|e| corrupt(e.to_string()))?;
        if bytes[..CHECKSUM] != checksum(cs, part, &rest) {
            return Err(corrupt("checksum mismatch".into()));
        }
        Part::parse(rest.into_owned(), cs, part).map_err(|e| corrupt(e.to_string()))
    }

    /// [`Part::read`] without the checksum.
    pub fn read_unchecked(bytes: &[u8], cs: ChangesetId, part: u32) -> Result<Part> {
        Part::parse(canonical(bytes)?.into_owned(), cs, part)
    }

    fn parse(rest: Vec<u8>, cs: ChangesetId, part: u32) -> Result<Part> {
        let mut r = Reader { b: &rest, pos: 0 };
        if part != 0 {
            let who = r.take(32)?;
            let which = r.u32()?;
            if who != cs.0.as_bytes() || which != part {
                return Err(bad("belongs to another part"));
            }
        }
        let n = r.varint()?;
        if n > (rest.len() / 32) as u64 {
            return Err(bad("table past the end"));
        }
        let mut table = Vec::with_capacity(n as usize);
        for _ in 0..n {
            let id = r.take(32)?;
            table.push(ChangesetId(Hash(id.try_into().expect("32 bytes"))));
        }
        let mut d = Decoder {
            r,
            cs,
            part,
            table: &table,
        };
        let root = if part == 0 { Some(d.node_ref()?) } else { None };
        let count = d.r.count()?;
        let mut lens = Vec::with_capacity(count);
        for _ in 0..count {
            lens.push(d.r.count()?);
        }
        let mut at = d.r.pos;
        let mut nodes = Vec::with_capacity(count);
        for l in lens {
            nodes.push(at..at + l);
            at += l;
        }
        if at != rest.len() {
            return Err(bad("node lengths do not add up"));
        }
        Ok(Part {
            cs,
            part,
            root,
            table,
            body: rest,
            nodes,
        })
    }

    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// Bytes the part holds decoded, for caches.
    pub fn size(&self) -> usize {
        self.body.len() + 32 * self.table.len() + 16 * self.nodes.len()
    }

    /// Node `idx`, as decoded: a patch still names its keyframe's entries by
    /// position. `None` if the part has no such node.
    pub fn node(&self, idx: u32) -> Option<Result<RawNode>> {
        let range = self.nodes.get(idx as usize)?.clone();
        let corrupt = |e: Error| {
            Error::Storage(format!(
                "corrupt history node {}/{}/{idx}: {e}",
                self.cs, self.part
            ))
        };
        let mut d = Decoder {
            r: Reader {
                b: &self.body[..range.end],
                pos: range.start,
            },
            cs: self.cs,
            part: self.part,
            table: &self.table,
        };
        Some(match d.node() {
            Ok(n) if d.r.pos == range.end => Ok(n),
            Ok(_) => Err(corrupt(bad("node shorter than its length"))),
            Err(e) => Err(corrupt(e)),
        })
    }

    /// Every node, decoded, with its length.
    pub fn nodes(&self) -> Result<Vec<(RawNode, usize)>> {
        (0..self.nodes.len() as u32)
            .map(|i| {
                let n = self.node(i).expect("in range")?;
                Ok((n, self.nodes[i as usize].len()))
            })
            .collect()
    }
}

/// Decode part `part` of `cs`'s pack and all its nodes, checking its
/// checksum. Damage is a `corrupt history node` storage error.
pub fn decode_part(bytes: &[u8], cs: ChangesetId, part: u32) -> Result<DecodedPart> {
    let p = Part::read(bytes, cs, part)?;
    Ok(DecodedPart {
        root: p.root,
        nodes: p.nodes()?,
        table: p.table,
    })
}

/// [`decode_part`] without the checksum: what a check of rows whose writer
/// is unknown can do. The writer and part only name this pack's nodes.
pub fn decode_unchecked(bytes: &[u8], cs: ChangesetId, part: u32) -> Result<DecodedPart> {
    let p = Part::read_unchecked(bytes, cs, part)?;
    Ok(DecodedPart {
        root: p.root,
        nodes: p.nodes()?,
        table: p.table,
    })
}

/// Parts at most this long are not compressed.
const RAW_MAX: usize = 64;
/// zstd level parts are compressed with: on 1-16 KB parts it keeps most of
/// what level 19 saves (1.35x against 1.41x on Linux to v2.6.16) at a small
/// fraction of the time.
const ZSTD_LEVEL: i32 = 3;
/// Largest uncompressed part a reader accepts, so a damaged length cannot
/// make it allocate without bound.
const PART_MAX: u64 = 64 << 20;

/// Append `rest` after a part's checksum: `0` and the bytes, or `1`, their
/// length and a zstd frame of them, whichever is shorter.
fn compress_into(out: &mut Vec<u8>, rest: &[u8]) {
    if rest.len() > RAW_MAX {
        thread_local! {
            // A compression context takes longer to set up than a small part
            // takes to compress.
            static Z: std::cell::RefCell<Option<zstd::bulk::Compressor<'static>>> =
                const { std::cell::RefCell::new(None) };
        }
        let z = Z.with(|z| {
            let mut z = z.borrow_mut();
            if z.is_none() {
                *z = zstd::bulk::Compressor::new(ZSTD_LEVEL).ok();
            }
            z.as_mut().and_then(|c| c.compress(rest).ok())
        });
        if let Some(z) = z {
            if z.len() + 1 + varint_len(rest.len() as u64) < rest.len() + 1 {
                out.push(1);
                put_varint(out, rest.len() as u64);
                out.extend_from_slice(&z);
                return;
            }
        }
    }
    out.push(0);
    out.extend_from_slice(rest);
}

/// A part's bytes after the checksum, uncompressed: what the checksum
/// covers, and what two copies of a part are compared by, since zstd
/// versions may compress the same bytes differently.
pub fn canonical(bytes: &[u8]) -> Result<std::borrow::Cow<'_, [u8]>> {
    if bytes.len() < CHECKSUM + 1 {
        return Err(bad("truncated"));
    }
    let body = &bytes[CHECKSUM + 1..];
    match bytes[CHECKSUM] {
        0 => Ok(std::borrow::Cow::Borrowed(body)),
        1 => {
            let mut r = Reader { b: body, pos: 0 };
            let len = r.varint()?;
            if len > PART_MAX {
                return Err(bad("part too long"));
            }
            thread_local! {
                static D: std::cell::RefCell<Option<zstd::bulk::Decompressor<'static>>> =
                    const { std::cell::RefCell::new(None) };
            }
            let raw = D
                .with(|d| {
                    let mut d = d.borrow_mut();
                    if d.is_none() {
                        *d = Some(zstd::bulk::Decompressor::new()?);
                    }
                    d.as_mut()
                        .expect("set above")
                        .decompress(&body[r.pos..], len as usize)
                })
                .map_err(|e| bad(&format!("zstd: {e}")))?;
            if raw.len() as u64 != len {
                return Err(bad("part length mismatch"));
            }
            Ok(std::borrow::Cow::Owned(raw))
        }
        _ => Err(bad("unknown part encoding")),
    }
}

/// Whether two copies of a part hold the same content.
pub fn same_part(a: &[u8], b: &[u8]) -> bool {
    a == b
        || (a.get(..CHECKSUM) == b.get(..CHECKSUM)
            && matches!((canonical(a), canonical(b)), (Ok(x), Ok(y)) if x == y))
}

/// The writer and number a part after the first says it has.
pub fn part_identity(bytes: &[u8]) -> Result<(ChangesetId, u32)> {
    let rest = canonical(bytes)?;
    let mut r = Reader { b: &rest, pos: 0 };
    let cs = ChangesetId(Hash(r.take(32)?.try_into().expect("32 bytes")));
    Ok((cs, r.u32()?))
}

/// The root a `history-tree-v4` value names, checked like any part.
pub fn decode_root(cs: ChangesetId, value: &[u8]) -> Result<NodeRef> {
    Ok(Part::read(value, cs, 0)?.root.expect("part 0 names a root"))
}

impl RawNode {
    /// The keyframe a patch is on.
    pub fn base(&self) -> Option<NodeRef> {
        match self {
            RawNode::Ready(n) => n.base(),
            RawNode::Patch { base, .. } | RawNode::SplitPatch { base, .. } => Some(*base),
        }
    }

    /// This node with a patch's positions read off `key`, the keyframe at
    /// its base, as stored. `Err` says why the two do not fit together.
    pub fn resolve(
        self,
        key: Option<&StoredHistoryNode>,
    ) -> std::result::Result<StoredHistoryNode, String> {
        match self {
            RawNode::Ready(n) => Ok(n),
            raw => raw.resolve_ref(key),
        }
    }

    /// [`RawNode::resolve`] without consuming the node, as a cached patch
    /// is resolved.
    pub fn resolve_ref(
        &self,
        key: Option<&StoredHistoryNode>,
    ) -> std::result::Result<StoredHistoryNode, String> {
        match (self, key) {
            (RawNode::Ready(n), _) => Ok(n.clone()),
            (RawNode::Patch { base, over }, Some(StoredHistoryNode::Flat(k))) => {
                let mut names = k.entries.keys();
                let mut at = 0usize;
                let mut node = HistoryNode::default();
                for (slot, e) in over {
                    let name = match slot {
                        // Positions ascend, so the keyframe's names are
                        // walked once.
                        Slot::Pos(p) if *p as usize >= at => {
                            let n = names.nth(*p as usize - at).ok_or_else(|| {
                                format!("names entry {p} of a keyframe of {}", k.entries.len())
                            })?;
                            at = *p as usize + 1;
                            n.clone()
                        }
                        Slot::Pos(p) => return Err(format!("names entry {p} out of order")),
                        Slot::Name(n) => n.clone(),
                    };
                    if node.entries.insert(name.clone(), e.clone()).is_some() {
                        return Err(format!("sets {name:?} twice"));
                    }
                }
                Ok(StoredHistoryNode::Patch {
                    base: *base,
                    over: node,
                })
            }
            (RawNode::SplitPatch { base, refs }, Some(StoredHistoryNode::Split(k))) => {
                let mut changes: std::collections::BTreeMap<String, Option<SplitChange>> =
                    std::collections::BTreeMap::new();
                for (slot, change) in refs {
                    let first = match slot {
                        Slot::Pos(p) => k
                            .get(*p as usize)
                            .map(|r| r.first.clone())
                            .ok_or_else(|| format!("names chunk {p} of a list of {}", k.len()))?,
                        Slot::Name(n) => n.clone(),
                    };
                    let change = change.map(|node| SplitChange { node, next: None });
                    if changes.insert(first.clone(), change).is_some() {
                        return Err(format!("sets {first:?} twice"));
                    }
                }
                // Each set chunk's successor in the patched list: the next
                // first name of the keyframe's chunks the patch keeps and of
                // the chunks it sets, merged in name order.
                let mut waiting: Option<String> = None;
                let mut next_of: Vec<(String, String)> = Vec::new();
                let mut kept = k
                    .iter()
                    .map(|r| r.first.as_str())
                    .filter(|f| !changes.contains_key(*f))
                    .peekable();
                for (first, change) in &changes {
                    while let Some(f) = kept.next_if(|f| *f < first.as_str()) {
                        if let Some(w) = waiting.take() {
                            next_of.push((w, f.to_string()));
                        }
                    }
                    if change.is_some() {
                        if let Some(w) = waiting.take() {
                            next_of.push((w, first.clone()));
                        }
                        waiting = Some(first.clone());
                    }
                }
                if let (Some(w), Some(f)) = (waiting, kept.next()) {
                    next_of.push((w, f.to_string()));
                }
                for (w, n) in next_of {
                    if let Some(Some(c)) = changes.get_mut(&w) {
                        c.next = Some(n);
                    }
                }
                Ok(StoredHistoryNode::SplitPatch {
                    base: *base,
                    refs: changes,
                })
            }
            (_, Some(_)) => Err("is a patch on a row of another shape".into()),
            (_, None) => Err("is a patch without its base".into()),
        }
    }
}
