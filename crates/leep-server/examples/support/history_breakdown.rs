//! `history_profile --breakdown`: what the bytes of the path-history index
//! are. Every stored node row is attributed to the changeset whose
//! derivation wrote it (the first, in generation order, whose history tree
//! reaches it but whose first parent's does not), and counted by row type,
//! directory width, merge or not, and field.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use bytes::Bytes;
use leep_core::{ChangesetId, Hash, MetaStore};

use self::v3::{HistoryEntry, HistoryNode, StoredHistoryNode};

/// `history-node-v3` rows (ADR 0037), which current servers no longer
/// read, decoded here to measure a database that still holds them.
mod v3 {
    use std::collections::BTreeMap;

    use leep_core::{ChangesetId, Hash};
    use serde::{Deserialize, Serialize};

    pub const TREE_KIND: &str = "history-tree-v3";
    pub const NODE_KIND: &str = "history-node-v3";

    #[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
    pub struct HistoryNode {
        pub entries: BTreeMap<String, HistoryEntry>,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub enum HistoryEntry {
        Leaf {
            changed: Vec<ChangesetId>,
        },
        Tree {
            changed: Vec<ChangesetId>,
            node: Hash,
        },
    }

    impl HistoryEntry {
        pub fn changed(&self) -> &[ChangesetId] {
            match self {
                HistoryEntry::Leaf { changed } | HistoryEntry::Tree { changed, .. } => changed,
            }
        }

        pub fn node(&self) -> Option<Hash> {
            match self {
                HistoryEntry::Leaf { .. } => None,
                HistoryEntry::Tree { node, .. } => Some(*node),
            }
        }
    }

    impl HistoryNode {
        pub fn empty_hash() -> Hash {
            Hash::of_object(NODE_KIND, &[0, 0])
        }
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub enum StoredHistoryNode {
        Flat(HistoryNode),
        Split(Vec<ChunkRef>),
        Patch {
            base: Hash,
            over: HistoryNode,
        },
        SplitPatch {
            base: Hash,
            refs: BTreeMap<String, Option<SplitChange>>,
        },
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct SplitChange {
        pub node: Hash,
        pub next: Option<String>,
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct ChunkRef {
        pub first: String,
        pub node: Hash,
    }

    impl StoredHistoryNode {
        pub fn decode(b: &[u8]) -> Result<Self, postcard::Error> {
            postcard::from_bytes(b)
        }

        pub fn base(&self) -> Option<Hash> {
            match self {
                StoredHistoryNode::Patch { base, .. }
                | StoredHistoryNode::SplitPatch { base, .. } => Some(*base),
                _ => None,
            }
        }

        /// The row with its patch applied to its keyframe `key`.
        pub fn resolve(self, key: Option<&StoredHistoryNode>) -> Result<Self, String> {
            match (self, key) {
                (row @ (StoredHistoryNode::Flat(_) | StoredHistoryNode::Split(_)), None) => Ok(row),
                (StoredHistoryNode::Patch { over, .. }, Some(StoredHistoryNode::Flat(k))) => {
                    let mut n = k.clone();
                    n.entries.extend(over.entries);
                    Ok(StoredHistoryNode::Flat(n))
                }
                (StoredHistoryNode::SplitPatch { refs, .. }, Some(StoredHistoryNode::Split(k))) => {
                    let mut all: BTreeMap<String, Hash> =
                        k.iter().map(|r| (r.first.clone(), r.node)).collect();
                    for (first, c) in refs {
                        match c {
                            Some(c) => {
                                all.insert(first, c.node);
                            }
                            None => {
                                all.remove(&first);
                            }
                        }
                    }
                    Ok(StoredHistoryNode::Split(
                        all.into_iter()
                            .map(|(first, node)| ChunkRef { first, node })
                            .collect(),
                    ))
                }
                _ => Err("does not fit its keyframe".into()),
            }
        }
    }
}

/// Byte counts by field.
#[derive(Default, Clone, Copy)]
struct Fields {
    rows: u64,
    bytes: u64,
    names: u64,
    ids: u64,
    id_count: u64,
    distinct_ids: u64,
    writer_ids: u64,
    child: u64,
    chunk_hash: u64,
    chunk_names: u64,
    base: u64,
    overhead: u64,
    /// Candidate encodings of the same row.
    id_table: u64,
    id_table_front: u64,
    zstd: u64,
}

impl Fields {
    fn add(&mut self, o: &Fields) {
        self.rows += o.rows;
        self.bytes += o.bytes;
        self.names += o.names;
        self.ids += o.ids;
        self.id_count += o.id_count;
        self.distinct_ids += o.distinct_ids;
        self.writer_ids += o.writer_ids;
        self.child += o.child;
        self.chunk_hash += o.chunk_hash;
        self.chunk_names += o.chunk_names;
        self.base += o.base;
        self.overhead += o.overhead;
        self.id_table += o.id_table;
        self.id_table_front += o.id_table_front;
        self.zstd += o.zstd;
    }
}

fn varint_len(n: usize) -> u64 {
    let mut n = n as u64;
    let mut l = 1;
    while n >= 0x80 {
        n >>= 7;
        l += 1;
    }
    l
}

fn str_len(s: &str) -> u64 {
    varint_len(s.len()) + s.len() as u64
}

fn common_prefix(a: &str, b: &str) -> usize {
    a.bytes().zip(b.bytes()).take_while(|(x, y)| x == y).count()
}

/// Bytes of a name front-coded against the previous one.
fn front_len(prev: &str, name: &str) -> u64 {
    let p = common_prefix(prev, name);
    varint_len(p) + str_len(&name[p..])
}

/// Field counts of a node's entries, and the sizes of the candidate
/// encodings of them (an id table per row and front-coded names).
fn node_fields(node: &HistoryNode, f: &mut Fields, table: &mut Vec<ChangesetId>) -> (u64, u64) {
    f.overhead += varint_len(node.entries.len());
    let mut tab = varint_len(node.entries.len());
    let mut front = varint_len(node.entries.len());
    let mut prev = "";
    for (name, e) in &node.entries {
        f.names += str_len(name);
        tab += str_len(name);
        front += front_len(prev, name);
        prev = name;
        f.overhead += 1 + varint_len(e.changed().len());
        tab += 1 + varint_len(e.changed().len());
        front += 1 + varint_len(e.changed().len());
        f.ids += 32 * e.changed().len() as u64;
        f.id_count += e.changed().len() as u64;
        for id in e.changed() {
            let i = match table.iter().position(|t| t == id) {
                Some(i) => i,
                None => {
                    table.push(*id);
                    table.len() - 1
                }
            };
            tab += varint_len(i);
            front += varint_len(i);
        }
        if let HistoryEntry::Tree { .. } = e {
            f.child += 32;
            tab += 32;
            front += 32;
        }
    }
    (tab, front)
}

fn zstd_len(b: &[u8]) -> u64 {
    zstd::bulk::compress(b, 3)
        .map(|c| c.len() as u64)
        .unwrap_or(0)
}

/// Field counts of one stored row.
fn row_fields(row: &StoredHistoryNode, bytes: &[u8], writer: ChangesetId) -> Fields {
    let mut f = Fields {
        rows: 1,
        bytes: bytes.len() as u64,
        overhead: 1,
        zstd: zstd_len(bytes),
        ..Default::default()
    };
    let mut table = Vec::new();
    let (tab, front) = match row {
        StoredHistoryNode::Flat(n) => node_fields(n, &mut f, &mut table),
        StoredHistoryNode::Patch { over, .. } => {
            f.base += 32;
            let (t, fr) = node_fields(over, &mut f, &mut table);
            (t + 32, fr + 32)
        }
        StoredHistoryNode::Split(refs) => {
            f.overhead += varint_len(refs.len());
            let mut fr = varint_len(refs.len());
            let mut prev = "";
            for r in refs {
                f.chunk_names += str_len(&r.first);
                f.chunk_hash += 32;
                fr += front_len(prev, &r.first) + 32;
                prev = &r.first;
            }
            (f.chunk_names + f.chunk_hash + f.overhead - 1, fr)
        }
        StoredHistoryNode::SplitPatch { refs, .. } => {
            f.base += 32;
            f.overhead += varint_len(refs.len());
            let mut fr = 32 + varint_len(refs.len());
            let mut prev = "";
            for (first, c) in refs {
                f.chunk_names += str_len(first);
                fr += front_len(prev, first) + 1;
                prev = first;
                f.overhead += 1;
                if let Some(c) = c {
                    f.chunk_hash += 32;
                    f.overhead += 1;
                    fr += 32 + 1;
                    if let Some(n) = &c.next {
                        f.chunk_names += str_len(n);
                        fr += front_len(first, n);
                    }
                }
            }
            (bytes.len() as u64 - 1, fr)
        }
    };
    let table_bytes = 1 + varint_len(table.len()) + 32 * table.len() as u64;
    f.distinct_ids = table.len() as u64;
    f.writer_ids = table.iter().filter(|t| **t == writer).count() as u64;
    let has_ids = !table.is_empty();
    f.id_table = if has_ids { tab + table_bytes } else { f.bytes };
    f.id_table_front = 1 + front + if has_ids { table_bytes - 1 } else { 0 };
    f
}

/// A version of a node: whole entries or a chunk list, resolved.
enum Resolved {
    Flat(Arc<BTreeMap<String, HistoryEntry>>),
    Split(Vec<(String, Hash)>),
}

struct Rows {
    rows: HashMap<Hash, Bytes>,
    decoded: HashMap<Hash, Arc<StoredHistoryNode>>,
    chunks: HashMap<Hash, Arc<BTreeMap<String, HistoryEntry>>>,
}

impl Rows {
    fn row(&mut self, h: Hash) -> Arc<StoredHistoryNode> {
        if h == HistoryNode::empty_hash() {
            return Arc::new(StoredHistoryNode::Flat(HistoryNode::default()));
        }
        if let Some(r) = self.decoded.get(&h) {
            return r.clone();
        }
        if self.decoded.len() > 200_000 {
            self.decoded.clear();
        }
        let b = self
            .rows
            .get(&h)
            .unwrap_or_else(|| panic!("row {h} missing"));
        let r = Arc::new(StoredHistoryNode::decode(b).unwrap());
        self.decoded.insert(h, r.clone());
        r
    }

    fn resolve(&mut self, h: Hash) -> Resolved {
        let row = self.row(h);
        let key = row.base().map(|b| self.row(b));
        match (*row).clone().resolve(key.as_deref()).unwrap() {
            StoredHistoryNode::Flat(n) => Resolved::Flat(Arc::new(n.entries)),
            StoredHistoryNode::Split(refs) => {
                Resolved::Split(refs.into_iter().map(|r| (r.first, r.node)).collect())
            }
            _ => unreachable!(),
        }
    }

    fn chunk(&mut self, h: Hash) -> Arc<BTreeMap<String, HistoryEntry>> {
        if let Some(c) = self.chunks.get(&h) {
            return c.clone();
        }
        if self.chunks.len() > 100_000 {
            self.chunks.clear();
        }
        let Resolved::Flat(c) = self.resolve(h) else {
            panic!("chunk {h} is not flat")
        };
        self.chunks.insert(h, c.clone());
        c
    }

    fn lookup(&mut self, v: &Resolved, name: &str) -> Option<HistoryEntry> {
        match v {
            Resolved::Flat(n) => n.get(name).cloned(),
            Resolved::Split(refs) => {
                let i = refs.partition_point(|r| r.0.as_str() <= name);
                if i == 0 {
                    return None;
                }
                self.chunk(refs[i - 1].1).get(name).cloned()
            }
        }
    }

    fn width(&mut self, v: &Resolved) -> usize {
        match v {
            Resolved::Flat(n) => n.len(),
            Resolved::Split(refs) => refs.iter().map(|r| self.chunk(r.1).len()).sum(),
        }
    }
}

fn width_bucket(w: usize) -> &'static str {
    match w {
        0..=64 => "a. <=64 (whole)",
        65..=500 => "b. 65-500",
        501..=2000 => "c. 501-2000",
        2001..=5000 => "d. 2001-5000",
        _ => "e. >5000",
    }
}

#[derive(Default)]
struct Report {
    by_type: BTreeMap<&'static str, Fields>,
    by_width: BTreeMap<&'static str, Fields>,
    by_merge: BTreeMap<&'static str, Fields>,
    by_depth: BTreeMap<usize, Fields>,
    by_merge_type: BTreeMap<String, Fields>,
    /// Per changeset: id occurrences, sum of per-row distinct ids, distinct
    /// ids over all its rows, directories written.
    ids_cs: Vec<(u64, u64, u64, u64, bool)>,
    all: Fields,
    per_cs: Vec<(u64, u64, bool, usize)>,
    entries_written: BTreeMap<&'static str, (u64, u64)>,
}

/// Print the breakdown of every `history-node-v3` row reachable from the
/// `history-tree-v3` roots.
pub async fn breakdown_v3(meta: &dyn MetaStore) {
    let (tree_kind, node_kind) = (v3::TREE_KIND, v3::NODE_KIND);
    let mut rows = HashMap::new();
    let mut after = None;
    loop {
        let page = meta.list_derived(node_kind, after, 10_000).await.unwrap();
        let Some(last) = page.last() else { break };
        after = Some(last.0);
        rows.extend(page);
    }
    let mut roots: HashMap<ChangesetId, Hash> = HashMap::new();
    let mut after = None;
    loop {
        let page = meta.list_derived(tree_kind, after, 10_000).await.unwrap();
        let Some(last) = page.last() else { break };
        after = Some(last.0);
        for (k, v) in page {
            roots.insert(ChangesetId(k), Hash(v[..].try_into().unwrap()));
        }
    }
    let ids: Vec<ChangesetId> = roots.keys().copied().collect();
    let mut gens = Vec::with_capacity(ids.len());
    let mut parents: HashMap<ChangesetId, Vec<ChangesetId>> = HashMap::new();
    for batch in ids.chunks(1000) {
        let keys: Vec<Hash> = batch.iter().map(|c| c.0).collect();
        let g = meta.get_derived_many("generation", &keys).await.unwrap();
        let cs = meta.get_changesets(batch).await.unwrap();
        for ((id, g), cs) in batch.iter().zip(g).zip(cs) {
            let g = u64::from_be_bytes(g.expect("generation")[..8].try_into().unwrap());
            gens.push((g, *id));
            parents.insert(*id, cs.expect("changeset").parents);
        }
    }
    gens.sort();
    let total_bytes: u64 = rows.values().map(|v| v.len() as u64).sum();
    println!(
        "breakdown: {} node rows, {} bytes, {} changesets with roots",
        rows.len(),
        total_bytes,
        roots.len()
    );
    let mut store = Rows {
        rows,
        decoded: HashMap::new(),
        chunks: HashMap::new(),
    };
    let mut seen: HashSet<Hash> = HashSet::new();
    seen.insert(HistoryNode::empty_hash());
    let mut rep = Report::default();
    let empty = HistoryNode::empty_hash();
    for (_, c) in &gens {
        let ps = &parents[c];
        let merge = ps.len() > 1;
        let hc = roots[c];
        let hp = ps.first().map(|p| roots[p]).unwrap_or(empty);
        let (mut n_rows, mut n_bytes) = (0u64, 0u64);
        let mut cs_ids: HashSet<ChangesetId> = HashSet::new();
        let (mut occ, mut row_distinct, mut dirs) = (0u64, 0u64, 0u64);
        let mut stack = vec![(0usize, hc, hp)];
        while let Some((depth, a, b)) = stack.pop() {
            if a == b {
                continue;
            }
            dirs += 1;
            let va = store.resolve(a);
            let vb = store.resolve(b);
            let width = store.width(&va);
            // Rows of this version and what kind each is.
            let mut mine: Vec<(Hash, &'static str)> = Vec::new();
            let top = store.row(a);
            match &*top {
                StoredHistoryNode::Flat(_) => mine.push((a, "whole keyframe")),
                StoredHistoryNode::Patch { .. } => mine.push((a, "whole patch")),
                StoredHistoryNode::Split(_) => mine.push((a, "chunk list keyframe")),
                StoredHistoryNode::SplitPatch { .. } => mine.push((a, "chunk list patch")),
            }
            if let Resolved::Split(refs) = &va {
                for (_, h) in refs {
                    if seen.contains(h) {
                        continue;
                    }
                    let kind = match &*store.row(*h) {
                        StoredHistoryNode::Flat(_) => "chunk keyframe",
                        _ => "chunk patch",
                    };
                    mine.push((*h, kind));
                }
            }
            for (h, kind) in mine {
                if !seen.insert(h) {
                    continue;
                }
                let row = store.row(h);
                let f = row_fields(&row, &store.rows[&h], *c);
                n_rows += 1;
                n_bytes += f.bytes;
                rep.by_type.entry(kind).or_default().add(&f);
                rep.by_width.entry(width_bucket(width)).or_default().add(&f);
                rep.by_merge
                    .entry(if merge { "merge" } else { "single parent" })
                    .or_default()
                    .add(&f);
                rep.by_depth.entry(depth.min(6)).or_default().add(&f);
                rep.by_merge_type
                    .entry(format!("{} {kind}", if merge { "M" } else { "S" }))
                    .or_default()
                    .add(&f);
                occ += f.id_count;
                row_distinct += f.distinct_ids;
                collect_ids(&row, &mut cs_ids);
                rep.all.add(&f);
            }
            // Entries that differ, and children to visit.
            let names: Vec<(String, HistoryEntry)> = match (&va, &vb) {
                (Resolved::Split(x), Resolved::Split(y)) => {
                    let ys: HashSet<Hash> = y.iter().map(|r| r.1).collect();
                    let mut out = Vec::new();
                    for (_, h) in x.iter().filter(|r| !ys.contains(&r.1)) {
                        let ch = store.chunk(*h);
                        out.extend(ch.iter().map(|(n, e)| (n.clone(), e.clone())));
                    }
                    out
                }
                (Resolved::Flat(n), _) => n.iter().map(|(n, e)| (n.clone(), e.clone())).collect(),
                (Resolved::Split(x), _) => {
                    let mut out = Vec::new();
                    for (_, h) in x.clone() {
                        let ch = store.chunk(h);
                        out.extend(ch.iter().map(|(n, e)| (n.clone(), e.clone())));
                    }
                    out
                }
            };
            let wb = width_bucket(width);
            for (name, e) in names {
                let old = store.lookup(&vb, &name);
                if old.as_ref() == Some(&e) {
                    continue;
                }
                let w = rep.entries_written.entry(wb).or_default();
                w.0 += 1;
                if e.changed() == [*c] {
                    w.1 += 1;
                }
                if let Some(n) = e.node() {
                    let o = old.and_then(|o| o.node()).unwrap_or(empty);
                    if n != o {
                        stack.push((depth + 1, n, o));
                    }
                }
            }
        }
        rep.per_cs.push((n_rows, n_bytes, merge, ps.len()));
        rep.ids_cs
            .push((occ, row_distinct, cs_ids.len() as u64, dirs, merge));
    }
    let unattributed = store.rows.len() as u64 - (seen.len() as u64 - 1);
    print_report(&rep, unattributed);
}

fn collect_ids(row: &StoredHistoryNode, out: &mut HashSet<ChangesetId>) {
    let node = match row {
        StoredHistoryNode::Flat(n) => n,
        StoredHistoryNode::Patch { over, .. } => over,
        _ => return,
    };
    for e in node.entries.values() {
        out.extend(e.changed().iter().copied());
    }
}

fn print_fields(label: &str, f: &Fields, total: u64) {
    println!(
        "  {label:<22} {:>9} rows {:>12} B {:>5.1}% avg {:>6} | names {:>5.1}% ids {:>5.1}% child {:>5.1}% chunk# {:>5.1}% chunknames {:>5.1}% base {:>5.1}% ovh {:>4.1}% | ids/row {:>5.1} distinct {:>5.1} | idtab {:>4.2}x +front {:>4.2}x zstd {:>4.2}x",
        f.rows,
        f.bytes,
        100.0 * f.bytes as f64 / total.max(1) as f64,
        f.bytes / f.rows.max(1),
        pct(f.names, f.bytes),
        pct(f.ids, f.bytes),
        pct(f.child, f.bytes),
        pct(f.chunk_hash, f.bytes),
        pct(f.chunk_names, f.bytes),
        pct(f.base, f.bytes),
        pct(f.overhead, f.bytes),
        f.id_count as f64 / f.rows.max(1) as f64,
        f.distinct_ids as f64 / f.rows.max(1) as f64,
        f.bytes as f64 / f.id_table.max(1) as f64,
        f.bytes as f64 / f.id_table_front.max(1) as f64,
        f.bytes as f64 / f.zstd.max(1) as f64,
    );
}

fn pct(a: u64, b: u64) -> f64 {
    100.0 * a as f64 / b.max(1) as f64
}

fn print_report(rep: &Report, unattributed: u64) {
    let total = rep.all.bytes;
    println!("all rows (unattributed: {unattributed})");
    print_fields("all", &rep.all, total);
    println!("by row type");
    for (k, f) in &rep.by_type {
        print_fields(k, f, total);
    }
    println!("by directory width (entries in the node version)");
    for (k, f) in &rep.by_width {
        print_fields(k, f, total);
    }
    println!("by changeset");
    for (k, f) in &rep.by_merge {
        print_fields(k, f, total);
    }
    println!("by depth (0 = root, 6 = 6 or more)");
    for (k, f) in &rep.by_depth {
        print_fields(&k.to_string(), f, total);
    }
    println!("by merge and row type");
    for (k, f) in &rep.by_merge_type {
        print_fields(k, f, total);
    }
    for m in [true, false] {
        let v: Vec<_> = rep.ids_cs.iter().filter(|c| c.4 == m).collect();
        let s = |i: usize| -> u64 { v.iter().map(|c| [c.0, c.1, c.2, c.3][i]).sum() };
        println!(
            "{}: id occurrences {} ({:.1}/cs), per-row distinct {} , per-changeset distinct {} ({:.1}/cs), dirs written {:.1}/cs",
            if m { "merges" } else { "others" },
            s(0),
            s(0) as f64 / v.len().max(1) as f64,
            s(1),
            s(2),
            s(2) as f64 / v.len().max(1) as f64,
            s(3) as f64 / v.len().max(1) as f64,
        );
    }
    println!(
        "entries written (changed vs the first parent), by width: total, with changed == [writer]"
    );
    for (k, (n, w)) in &rep.entries_written {
        println!("  {k:<22} {n:>10} {w:>10}");
    }
    let n = rep.per_cs.len() as u64;
    let merges: Vec<_> = rep.per_cs.iter().filter(|c| c.2).collect();
    let singles: Vec<_> = rep.per_cs.iter().filter(|c| !c.2).collect();
    let avg = |v: &[&(u64, u64, bool, usize)]| {
        let r: u64 = v.iter().map(|c| c.0).sum();
        let b: u64 = v.iter().map(|c| c.1).sum();
        (
            r as f64 / v.len().max(1) as f64,
            b as f64 / v.len().max(1) as f64,
        )
    };
    let (ra, ba) = avg(&rep.per_cs.iter().collect::<Vec<_>>());
    let (rm, bm) = avg(&merges);
    let (rs, bs) = avg(&singles);
    println!(
        "per changeset: {n} changesets, {ra:.1} rows, {ba:.0} B; {} merges: {rm:.1} rows, {bm:.0} B; {} others: {rs:.1} rows, {bs:.0} B",
        merges.len(),
        singles.len()
    );
    let mut bytes: Vec<u64> = rep.per_cs.iter().map(|c| c.1).collect();
    bytes.sort_unstable();
    let q = |p: f64| bytes[((bytes.len() - 1) as f64 * p) as usize];
    println!(
        "bytes per changeset: p50 {} p90 {} p99 {} max {}; top 1% of changesets hold {:.1}% of bytes",
        q(0.5),
        q(0.9),
        q(0.99),
        q(1.0),
        {
            let top: u64 = bytes.iter().rev().take(bytes.len() / 100).sum();
            pct(top, bytes.iter().sum())
        }
    );
}

/// Bytes by kind of node in `history-tree-v4` packs.
#[derive(Default, Clone, Copy)]
struct V4Stats {
    changesets: u64,
    rows: u64,
    bytes: u64,
    /// Bytes before compression, of which the headers and id tables.
    raw: u64,
    headers: u64,
    multipart: u64,
    nodes: [(u64, u64); 4],
}

/// Print what the `history-tree-v4` packs and their `history-node-v4` parts
/// hold: bytes per changeset, merges against the rest, nodes by kind, and
/// the part headers and id tables.
pub async fn breakdown_v4(meta: &dyn MetaStore) {
    use leep_derive::derived::history_pack::{canonical, decode_part, part_key, RawNode};
    use leep_derive::derived::{
        DerivedKind, HistoryTree, StoredHistoryNode as S, HISTORY_NODE_KIND,
    };

    let mut rows: HashMap<Hash, Bytes> = HashMap::new();
    let mut after = None;
    loop {
        let page = meta
            .list_derived(HISTORY_NODE_KIND, after, 10_000)
            .await
            .unwrap();
        let Some(last) = page.last() else { break };
        after = Some(last.0);
        rows.extend(page);
    }
    let mut values: Vec<(ChangesetId, Bytes)> = Vec::new();
    let mut after = None;
    loop {
        let page = meta
            .list_derived(HistoryTree::NAME, after, 10_000)
            .await
            .unwrap();
        let Some(last) = page.last() else { break };
        after = Some(last.0);
        values.extend(page.into_iter().map(|(k, v)| (ChangesetId(k), v)));
    }
    let mut merge: HashMap<ChangesetId, bool> = HashMap::new();
    let mut first_parent: HashMap<ChangesetId, ChangesetId> = HashMap::new();
    for batch in values.chunks(1000) {
        let ids: Vec<ChangesetId> = batch.iter().map(|(c, _)| *c).collect();
        for (id, cs) in ids.iter().zip(meta.get_changesets(&ids).await.unwrap()) {
            let cs = cs.expect("changeset");
            merge.insert(*id, cs.parents.len() > 1);
            if let Some(p) = cs.parents.first() {
                first_parent.insert(*id, *p);
            }
        }
    }
    // Ids each pack names, to see how many its first parent's pack names too.
    let mut pack_ids: HashMap<ChangesetId, HashSet<ChangesetId>> = HashMap::new();
    let mut stats: BTreeMap<&str, V4Stats> = BTreeMap::new();
    let mut per_cs: Vec<u64> = Vec::new();
    let mut part_sizes = [0u64; 4];
    let mut reached = 0u64;
    let mut table_dup = [(0u64, 0u64); 2];
    let mut table_use = [0u64; 6];
    for (cs, value) in &values {
        let m = merge[cs];
        let mut parts = vec![(0u32, value.clone())];
        for p in 1.. {
            match rows.get(&part_key(*cs, p)) {
                Some(b) => parts.push((p, b.clone())),
                None => break,
            }
        }
        reached += parts.len() as u64 - 1;
        let mut total = 0;
        let mut distinct: HashSet<ChangesetId> = HashSet::new();
        let mut tables = 0u64;
        for key in [if m { "merge" } else { "single parent" }, "all"] {
            let s = stats.entry(key).or_default();
            s.changesets += 1;
            s.multipart += (parts.len() > 1) as u64;
        }
        for (p, bytes) in &parts {
            let decoded = decode_part(bytes, *cs, *p).expect("part decodes");

            tables += decoded.table.len() as u64;
            distinct.extend(decoded.table.iter().copied());
            let mut by_use = [0u64; 3];
            let mut in_ids: HashSet<ChangesetId> = HashSet::new();
            let mut in_refs: HashSet<ChangesetId> = HashSet::new();
            for (node, _) in &decoded.nodes {
                collect_v4(node, &mut in_ids, &mut in_refs);
            }
            for t in &decoded.table {
                by_use[match (in_ids.contains(t), in_refs.contains(t)) {
                    (true, false) => 0,
                    (false, true) => 1,
                    _ => 2,
                }] += 1;
            }
            for (i, n) in by_use.iter().enumerate() {
                table_use[if m { 3 } else { 0 } + i] += n;
            }
            total += bytes.len() as u64;
            part_sizes[match bytes.len() {
                0..=1024 => 0,
                1025..=2032 => 1,
                2033..=4096 => 2,
                _ => 3,
            }] += 1;
            let mut node_bytes = 0;
            for key in [if m { "merge" } else { "single parent" }, "all"] {
                let s = stats.entry(key).or_default();
                s.rows += 1;
                s.bytes += bytes.len() as u64;
            }
            for (node, size) in &decoded.nodes {
                node_bytes += *size as u64;
                let i = match node {
                    RawNode::Ready(S::Flat(_)) => 0,
                    RawNode::Ready(S::Split(_)) => 1,
                    RawNode::Patch { .. } | RawNode::Ready(S::Patch { .. }) => 2,
                    _ => 3,
                };
                for key in [if m { "merge" } else { "single parent" }, "all"] {
                    let s = stats.entry(key).or_default();
                    s.nodes[i].0 += 1;
                    s.nodes[i].1 += *size as u64;
                }
            }
            let raw = 9 + canonical(bytes).expect("part decodes").len() as u64;
            for key in [if m { "merge" } else { "single parent" }, "all"] {
                let s = stats.entry(key).or_default();
                s.raw += raw;
                s.headers += raw - node_bytes;
            }
        }
        per_cs.push(total);
        pack_ids.insert(*cs, distinct.clone());
        let t = &mut table_dup[m as usize];
        t.0 += tables;
        t.1 += distinct.len() as u64;
    }
    let mut shared = [(0u64, 0u64, 0u64); 2];
    for (cs, ids) in &pack_ids {
        let m = merge[cs] as usize;
        let Some(p) = first_parent.get(cs).and_then(|p| pack_ids.get(p)) else {
            continue;
        };
        shared[m].0 += ids.len() as u64;
        shared[m].1 += ids
            .iter()
            .filter(|i| p.contains(i) || *i == first_parent.get(cs).unwrap())
            .count() as u64;
        shared[m].2 += 1;
    }
    for (m, (n, s, c)) in shared.iter().enumerate() {
        println!(
            "  {}: {n} ids named by {c} packs, {s} of them also by the first parent's pack (or are it)",
            if m == 1 { "merges" } else { "others" }
        );
    }
    for (m, (sum, d)) in table_dup.iter().enumerate() {
        println!(
            "  {}: table entries {sum} in parts, {d} distinct per pack; parts' ids used by changed sets only {}, node refs only {}, both {}",
            if m == 1 { "merges" } else { "others" },
            table_use[3 * m],
            table_use[3 * m + 1],
            table_use[3 * m + 2]
        );
    }
    let all = stats.get("all").copied().unwrap_or_default();
    println!(
        "v4 breakdown: {} tree rows, {} node rows ({} reached), {} bytes in all",
        values.len(),
        rows.len(),
        reached,
        all.bytes
    );
    for (k, s) in &stats {
        let names = [
            "whole/chunk keyframes",
            "chunk lists",
            "patches",
            "list patches",
        ];
        print!(
            "  {k:<14} {:>7} changesets ({} of several parts) {:>8} rows {:>11} B ({:.1} rows, {:.0} B per changeset; {:.2}x compressed) | uncompressed: headers+id tables {:.1}%",
            s.changesets,
            s.multipart,
            s.rows,
            s.bytes,
            s.rows as f64 / s.changesets.max(1) as f64,
            s.bytes as f64 / s.changesets.max(1) as f64,
            s.raw as f64 / s.bytes.max(1) as f64,
            pct(s.headers, s.raw)
        );
        for (n, (c, b)) in names.iter().zip(s.nodes) {
            print!(" | {n} {c} {:.1}%", pct(b, s.raw));
        }
        println!();
    }
    per_cs.sort_unstable();
    let q = |p: f64| per_cs[((per_cs.len() - 1) as f64 * p) as usize];
    println!(
        "bytes per changeset: p50 {} p90 {} p99 {} max {}; parts <=1K {} <=2032 {} <=4K {} >4K {}",
        q(0.5),
        q(0.9),
        q(0.99),
        q(1.0),
        part_sizes[0],
        part_sizes[1],
        part_sizes[2],
        part_sizes[3]
    );
}

fn collect_v4(
    node: &leep_derive::derived::history_pack::RawNode,
    ids: &mut HashSet<ChangesetId>,
    refs: &mut HashSet<ChangesetId>,
) {
    use leep_derive::derived::history_pack::RawNode;
    use leep_derive::derived::{HistoryEntry as E, StoredHistoryNode as S};
    let mut entry = |e: &E| {
        ids.extend(e.changed().iter().copied());
        if let Some(n) = e.node() {
            refs.insert(n.cs);
        }
    };
    match node {
        RawNode::Ready(S::Flat(n)) => n.entries.values().for_each(&mut entry),
        RawNode::Ready(S::Split(r)) => refs.extend(r.iter().map(|c| c.node.cs)),
        RawNode::Patch { base, over } => {
            over.iter().for_each(|(_, e)| entry(e));
            refs.insert(base.cs);
        }
        RawNode::SplitPatch { base, refs: r } => {
            refs.insert(base.cs);
            refs.extend(r.iter().filter_map(|(_, c)| c.map(|c| c.cs)));
        }
        RawNode::Ready(_) => {}
    }
}
