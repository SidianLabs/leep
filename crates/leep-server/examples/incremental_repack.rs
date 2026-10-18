//! Grow a synthetic history in a fresh filesystem store with a repack
//! scheduled every so many commits, and report what each repack cost and how
//! many packs the store holds, as measured in docs/PERFORMANCE.md
//! ("Incremental repack", ADR 0039).
//!
//! ```sh
//! cargo run --release -p leep-server --example incremental_repack -- <dir> [--base N] [--every N] [--rounds N] [--batch N]
//! ```
//! Lands `--base` commits with an incremental repack every `--every`, then
//! `--rounds` rounds of `--batch` commits each followed by an incremental
//! repack, then one full repack for comparison. Each commit edits three files
//! of a tree of 20 directories; one in twenty adds a file. Repacks run with no
//! fence: nothing else uses the store.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use leep_core::{Changeset, ChangesetId, FileChange, FileKind, Hash, RefUpdateContext, RepoPath};
use leep_derive::pack_order::{pack_order, pack_order_loose};
use leep_derive::Repo;
use leep_store::{FsBlobStore, RepackMode, RepackOptions};

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

const WORDS: [&str; 16] = [
    "let", "value", "match", "Some", "None", "return", "self", "fn", "impl", "for", "in", "while",
    "count", "index", "buffer", "result",
];

fn line(rng: &mut Rng) -> String {
    let n = 3 + rng.below(8);
    let mut s = "    ".repeat(rng.below(3));
    for i in 0..n {
        if i > 0 {
            s.push(' ');
        }
        s.push_str(WORDS[rng.below(WORDS.len())]);
    }
    s.push_str(";\n");
    s
}

struct Site {
    repo: Repo,
    store: FsBlobStore,
    files: BTreeMap<String, Vec<String>>,
    tip: Option<ChangesetId>,
    rng: Rng,
}

impl Site {
    async fn commit(&mut self) {
        let mut changes = BTreeMap::new();
        let edit = |path: String, lines: &mut Vec<String>, rng: &mut Rng| {
            let at = rng.below(lines.len() + 1);
            if at < lines.len() && rng.below(2) == 0 {
                lines[at] = line(rng);
            } else {
                lines.insert(at.min(lines.len()), line(rng));
            }
            (path, lines.concat())
        };
        let mut texts = Vec::new();
        if self.files.is_empty() || self.rng.below(20) == 0 {
            let path = format!(
                "dir{:02}/file{:04}.rs",
                self.rng.below(20),
                self.files.len()
            );
            let mut lines: Vec<String> = (0..60).map(|_| line(&mut self.rng)).collect();
            texts.push(edit(path.clone(), &mut lines, &mut self.rng));
            self.files.insert(path, lines);
        }
        for _ in 0..3 {
            let k = self.rng.below(self.files.len());
            let path = self.files.keys().nth(k).unwrap().clone();
            let lines = self.files.get_mut(&path).unwrap();
            texts.push(edit(path, lines, &mut self.rng));
        }
        for (path, text) in texts {
            let content = self.repo.put_blob(Bytes::from(text.clone())).await.unwrap();
            changes.insert(
                RepoPath::new(&path).unwrap(),
                FileChange::Modify {
                    content,
                    kind: FileKind::Regular,
                    size: text.len() as u64,
                },
            );
        }
        let id = self
            .repo
            .create_changeset(Changeset {
                parents: self.tip.into_iter().collect(),
                author: "bench".into(),
                timestamp: 1,
                message: "edit".into(),
                changes,
                extra: BTreeMap::new(),
            })
            .await
            .unwrap();
        self.repo.root_tree(id).await.unwrap();
        let moved = self
            .repo
            .meta()
            .cas_ref_logged("main", self.tip, Some(id), &RefUpdateContext::new("land"))
            .await
            .unwrap();
        assert!(moved);
        self.tip = Some(id);
    }

    /// Bytes an incremental pack would spend on the first object of each
    /// chain whose base is in an older pack: stored whole (what the writer
    /// does), or after a whole copy of the base, as a delta against it.
    async fn base_copy_cost(&self, hints: &[(Hash, Option<Hash>)]) -> (u64, u64, u64) {
        let (mut n, mut whole, mut copy) = (0, 0, 0);
        for (h, b) in hints {
            let Some(b) = b.filter(|b| !self.store.is_loose(b)) else {
                continue;
            };
            let data = self.repo.blobs().get(h).await.unwrap().unwrap();
            let base = self.repo.blobs().get(&b).await.unwrap().unwrap();
            let w = leep_store::pack::compress_whole(&data, 9).unwrap().len() as u64;
            let bw = leep_store::pack::compress_whole(&base, 9).unwrap().len() as u64;
            let d = leep_store::pack::compress_delta(&base, &data, 9)
                .unwrap()
                .map_or(w, |d| (d.len() as u64).min(w));
            n += 1;
            whole += w;
            copy += bw + d;
        }
        (n, whole, copy)
    }

    async fn repack(&self, mode: RepackMode, label: &str, measure_copies: bool) {
        let t = Instant::now();
        let order = match mode {
            RepackMode::Full => pack_order(&self.repo).await.unwrap(),
            RepackMode::Incremental { .. } => {
                let s = self.store.clone();
                pack_order_loose(&self.repo, &move |h| s.is_loose(h))
                    .await
                    .unwrap()
            }
        };
        let walk = t.elapsed();
        let copies = if measure_copies {
            Some(
                self.base_copy_cost(&order.objects.iter().collect::<Vec<_>>())
                    .await,
            )
        } else {
            None
        };
        let t = Instant::now();
        let opts = RepackOptions {
            mode,
            ..RepackOptions::default()
        };
        let p = self
            .store
            .repack_publish(order.objects, opts)
            .await
            .unwrap();
        let write = t.elapsed();
        let r = p.finish().await.unwrap();
        let total = walk + t.elapsed();
        let (pack_bytes, idx_bytes) = pack_dir_bytes(self.store.root());
        println!(
            "{label:>14} walk {:>3} cs {:>7.3}s  write {:>6} obj ({:>5} delta, {:>5} reused, {} merged) {:>7.3}s  total {:>7.3}s  packs {:>2}  packed {:>9} B + idx {:>8} B{}",
            order.changesets,
            walk.as_secs_f64(),
            r.objects,
            r.deltas,
            r.frames_reused,
            r.packs_merged,
            write.as_secs_f64(),
            total.as_secs_f64(),
            r.packs_after,
            pack_bytes,
            idx_bytes,
            copies.map_or(String::new(), |(n, w, c)| format!(
                "  chain heads {n}: whole {w} B, base copy + delta {c} B"
            )),
        );
    }
}

fn pack_dir_bytes(root: &std::path::Path) -> (u64, u64) {
    let (mut p, mut i) = (0, 0);
    for e in std::fs::read_dir(root.join("packs")).unwrap().flatten() {
        let name = e.file_name().to_string_lossy().into_owned();
        let len = e.metadata().unwrap().len();
        if name.ends_with(".pack") {
            p += len;
        } else if name.ends_with(".idx") {
            i += len;
        }
    }
    (p, i)
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let dir = std::path::PathBuf::from(args.first().expect("usage: <dir> [--base N] ..."));
    let num = |flag: &str, default: usize| {
        args.iter()
            .position(|a| a == flag)
            .map_or(default, |i| args[i + 1].parse().unwrap())
    };
    let (base, every, rounds, batch) = (
        num("--base", 10_000),
        num("--every", 500),
        num("--rounds", 20),
        num("--batch", 100),
    );
    std::fs::create_dir_all(dir.join("blobs")).unwrap();
    let meta_url = format!("sqlite://{}?mode=rwc", dir.join("meta.db").display());
    let store = FsBlobStore::new(dir.join("blobs"));
    let repo = Repo::new(
        Arc::new(store.clone()),
        leep_meta::open_meta_store(&meta_url).await.unwrap(),
    );
    let mut site = Site {
        repo,
        store,
        files: BTreeMap::new(),
        tip: None,
        rng: Rng(0x9e37_79b9_7f4a_7c15),
    };
    let incremental = RepackMode::Incremental {
        factor: 2,
        max_packs: 8,
    };
    let t = Instant::now();
    for i in 1..=base {
        site.commit().await;
        if i % every == 0 {
            site.repack(incremental, &format!("base {i}"), false).await;
        }
    }
    eprintln!("base history built in {:.1}s", t.elapsed().as_secs_f64());
    for round in 1..=rounds {
        for _ in 0..batch {
            site.commit().await;
        }
        site.repack(incremental, &format!("round {round}"), true)
            .await;
    }
    site.repack(RepackMode::Full, "full", false).await;
}
