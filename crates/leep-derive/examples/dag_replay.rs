//! Replay a real commit graph with synthetic contents on in-memory stores and
//! time path-history derivation on it, as measured in docs/PERFORMANCE.md
//! ("Path history on Linux"): the graph's shape (criss-cross merges, branches
//! on old bases) is what makes merges expensive, and a graph needs no trees.
//!
//! ```sh
//! git clone --bare --filter=tree:0 <repo> g.git   # commits only
//! git -C g.git rev-list --reverse --topo-order --parents <rev> > dag.txt
//! cargo run --release -p leep-derive --example dag_replay -- dag.txt [limit] [from_gen] [dump]
//! ```
//! Each non-merge commit writes one or two of 9,216 files picked from its id;
//! a merge takes the other parents' changes since their merge base where they
//! differ from the first parent. Commits of generation `from_gen` or less are
//! left out, their place as parents taken by a linear chain of the same
//! generations, so a slice of a large history fits in memory. Derives the
//! head's parents, then times the head alone; `dump` writes every
//! path-history row as `kind key blake3(value)` lines, to compare builds.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use leep_core::memory::{MemoryBlobStore, MemoryMetaStore};
use leep_core::{Changeset, ChangesetId, FileChange, FileKind, RepoPath};
use leep_derive::derived::{self, DerivedKind, HistoryTree};
use leep_derive::Repo;

fn h(s: &str) -> u64 {
    let mut x = 0xcbf29ce484222325u64;
    for b in s.bytes() {
        x ^= b as u64;
        x = x.wrapping_mul(0x100000001b3);
    }
    x
}

fn path_for(sha: &str, k: u64) -> String {
    let v = h(sha).wrapping_add(k.wrapping_mul(0x9e3779b97f4a7c15));
    let v = v ^ (v >> 29);
    format!(
        "a{:02}/b{:02}/f{:02}",
        v % 24,
        (v >> 8) % 16,
        (v >> 16) % 24
    )
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let text = std::fs::read_to_string(&args[0]).unwrap();
    let limit: usize = args.get(1).map_or(usize::MAX, |s| s.parse().unwrap());
    let from_gen: u64 = args.get(2).map_or(0, |s| s.parse().unwrap());
    // Generations, and a linear spine standing in for history at or below
    // `from_gen`.
    let mut gens: HashMap<&str, u64> = HashMap::new();
    for line in text.lines().take(limit) {
        let mut it = line.split(' ');
        let sha = it.next().unwrap();
        let g = 1 + it.map(|p| gens[p]).max().unwrap_or(0);
        gens.insert(sha, g);
    }
    let spine_names: Vec<String> = (0..=from_gen).map(|g| format!("spine{g}")).collect();
    let repo = Repo::new(
        Arc::new(MemoryBlobStore::new()),
        Arc::new(MemoryMetaStore::new()),
    );
    let t = Instant::now();
    let mut last = None;
    let mut last_parents = Vec::new();
    let mut lines: Vec<String> = Vec::new();
    if from_gen > 0 {
        let low = text
            .lines()
            .take(limit)
            .filter(|l| gens[l.split(' ').next().unwrap()] > from_gen)
            .flat_map(|l| l.split(' ').skip(1))
            .map(|p| gens[p])
            .filter(|&g| g <= from_gen)
            .min()
            .unwrap();
        let mut prev: Option<&str> = None;
        for g in low..=from_gen {
            let name = spine_names[g as usize].as_str();
            lines.push(match prev {
                Some(p) => format!("{name} {p}"),
                None => name.to_string(),
            });
            prev = Some(name);
        }
        eprintln!("spine of {} from generation {low}", lines.len());
    }
    for line in text.lines().take(limit) {
        let mut it = line.split(' ');
        let sha = it.next().unwrap();
        if gens[sha] <= from_gen {
            continue;
        }
        let mut l = sha.to_string();
        for p in it {
            l.push(' ');
            if gens[p] <= from_gen {
                l.push_str(&spine_names[gens[p] as usize]);
            } else {
                l.push_str(p);
            }
        }
        lines.push(l);
    }
    let mut ids: HashMap<String, ChangesetId> = HashMap::new();
    for (n, line) in lines.iter().enumerate() {
        let mut it = line.split(' ');
        let sha = it.next().unwrap();
        let mut parents: Vec<ChangesetId> = Vec::new();
        for p in it.map(|p| ids[p]) {
            if !parents.contains(&p) {
                parents.push(p);
            }
        }
        let mut changes = BTreeMap::new();
        if parents.len() <= 1 {
            for k in 0..1 + (h(sha) % 2) {
                let p = path_for(sha, k);
                let data = format!("{sha} {p}\n");
                let content = repo.blobs().put(Bytes::from(data.clone())).await.unwrap();
                changes.insert(
                    RepoPath::new(p).unwrap(),
                    FileChange::Modify {
                        content,
                        kind: FileKind::Regular,
                        size: data.len() as u64,
                    },
                );
            }
        } else {
            let p1 = parents[0];
            for &other in &parents[1..] {
                let mb = repo.merge_base(p1, other).await.unwrap();
                for e in repo.diff(mb, other, "").await.unwrap() {
                    let mine = repo.lookup_file(p1, &e.path).await.unwrap();
                    let same = match (&mine, &e.new) {
                        (Some(a), Some(b)) => a.content == b.content,
                        (None, None) => true,
                        _ => false,
                    };
                    if !same {
                        let c = match e.new {
                            Some(f) => FileChange::Modify {
                                content: f.content,
                                kind: f.kind,
                                size: f.size,
                            },
                            None => FileChange::Delete,
                        };
                        changes.insert(e.path, c);
                    }
                }
            }
        }
        let id = repo
            .create_changeset(Changeset {
                parents: parents.clone(),
                author: "a".into(),
                timestamp: n as i64,
                message: sha.to_string(),
                changes,
                extra: BTreeMap::new(),
            })
            .await
            .unwrap();
        ids.insert(sha.to_string(), id);
        last = Some(id);
        last_parents = parents;
        if n % 50_000 == 0 {
            eprintln!("built {n} in {:.0} s", t.elapsed().as_secs_f64());
        }
    }
    eprintln!("built all in {:.0} s", t.elapsed().as_secs_f64());
    let head = last.unwrap();
    let t = Instant::now();
    for p in &last_parents {
        derived::derive::<HistoryTree>(&repo, *p).await.unwrap();
        eprintln!("derived parent in {:.1} s", t.elapsed().as_secs_f64());
    }
    for _ in 0..2 {
        let fresh = Repo::new(repo.blobs().clone(), repo.meta().clone());
        let t = Instant::now();
        let mb = fresh
            .merge_base(last_parents[0], last_parents[1])
            .await
            .unwrap();
        eprintln!("merge_base {mb:?} cold: {:.3} s", t.elapsed().as_secs_f64());
    }
    let t = Instant::now();
    derived::derive::<HistoryTree>(&repo, head).await.unwrap();
    eprintln!("head alone: {:.3} s", t.elapsed().as_secs_f64());
    if let Some(path) = args.get(3) {
        let mut lines = Vec::new();
        for kind in [HistoryTree::NAME, derived::HISTORY_NODE_KIND] {
            for (k, v) in repo
                .meta()
                .list_derived(kind, None, usize::MAX)
                .await
                .unwrap()
            {
                lines.push(format!("{kind} {k} {}", leep_core::Hash::of_blob(&v)));
            }
        }
        lines.sort();
        std::fs::write(path, lines.join("\n") + "\n").unwrap();
        eprintln!("dumped {} rows", lines.len());
    }
}
