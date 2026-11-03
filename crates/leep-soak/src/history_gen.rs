//! Bulk writer of a long, linear, monorepo-shaped history straight through
//! `Repo` (no HTTP): a wide tree of nested directories, changesets touching
//! one to three files, a few hot files. Measurement tool for deep-history
//! runs; resumes from the target ref's head.

use std::collections::BTreeMap;
use std::time::Instant;

use anyhow::{Context, Result};
use bytes::Bytes;
use clap::Parser;
use leep_core::{Changeset, FileChange, FileKind, Hash, RefStep, RefUpdateContext, RepoPath};
use leep_derive::Repo;

#[derive(Parser)]
struct Args {
    #[arg(long)]
    meta: String,
    #[arg(long)]
    blobs: String,
    #[arg(long, default_value = "main")]
    target: String,
    /// Stop once the head's generation reaches this.
    #[arg(long)]
    until: u64,
    /// Files under src/ in the root changeset.
    #[arg(long, default_value_t = 20_000)]
    files: usize,
    /// Leaf directories under src/ (16 per middle directory).
    #[arg(long, default_value_t = 256)]
    subtrees: usize,
    #[arg(long, default_value_t = 8)]
    hot_files: usize,
    /// Share of file edits that hit a hot file.
    #[arg(long, default_value_t = 0.05)]
    hot_fraction: f64,
    /// Changesets whose blobs are uploaded together (one pack).
    #[arg(long, default_value_t = 512)]
    batch: usize,
    /// Move the ref every this many changesets.
    #[arg(long, default_value_t = 2000)]
    ref_every: u64,
    #[arg(long, default_value_t = 7)]
    seed: u64,
}

struct Rng(u64);
impl Rng {
    fn new(seed: u64) -> Rng {
        Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n.max(1) as u64) as usize
    }
    fn unit(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64
    }
}

fn modify(data: &[u8]) -> FileChange {
    FileChange::Modify {
        content: Hash::of_blob(data),
        kind: FileKind::Regular,
        size: data.len() as u64,
    }
}

struct State {
    /// path -> lines
    files: BTreeMap<String, Vec<String>>,
    src: Vec<String>,
    seeded: Vec<String>,
    hot: Vec<String>,
    warm: Vec<String>,
}

fn leaf_dir(i: usize) -> String {
    format!("src/m{:02}/s{:02}", i / 16, i % 16)
}

fn initial(args: &Args) -> State {
    let mut files = BTreeMap::new();
    let mut src = Vec::new();
    for i in 0..args.files {
        let p = format!("{}/f{}.rs", leaf_dir(i % args.subtrees), i / args.subtrees);
        let lines = (0..40)
            .map(|l| format!("// {p} line {l}: fn item_{l}() {{ agent base }}"))
            .collect();
        files.insert(p.clone(), lines);
        src.push(p);
    }
    let mut seeded = Vec::new();
    for d in 0..10 {
        for f in 0..20 {
            let p = format!("d{d}/f{f}");
            files.insert(p.clone(), vec![p.clone(), "line 2".into(), "line 3".into()]);
            seeded.push(p);
        }
    }
    let mut hot = Vec::new();
    for h in 0..args.hot_files {
        let p = format!("hot/h{h}");
        let lines = (0..200)
            .map(|l| format!("{p} line {l} agent base"))
            .collect();
        files.insert(p.clone(), lines);
        hot.push(p);
    }
    finish(files, src, seeded, hot)
}

fn finish(
    files: BTreeMap<String, Vec<String>>,
    src: Vec<String>,
    seeded: Vec<String>,
    hot: Vec<String>,
) -> State {
    // 1% of src files take about 20% of the src edits.
    let warm = src.iter().step_by(100).cloned().collect();
    State {
        files,
        src,
        seeded,
        hot,
        warm,
    }
}

fn content(lines: &[String]) -> Bytes {
    let mut s = lines.join("\n");
    s.push('\n');
    Bytes::from(s)
}

/// One changeset's edits, applied to `st`: (path, new content).
fn edits(args: &Args, st: &mut State, rng: &mut Rng, n: u64, agent: usize) -> Vec<(String, Bytes)> {
    let k = match rng.below(100) {
        0..=49 => 1,
        50..=84 => 2,
        _ => 3,
    };
    let mut out: BTreeMap<String, Bytes> = BTreeMap::new();
    for _ in 0..k {
        let r = rng.unit();
        let path = if r < args.hot_fraction {
            st.hot[rng.below(st.hot.len())].clone()
        } else if r < args.hot_fraction + 0.002 {
            st.seeded[rng.below(st.seeded.len())].clone()
        } else if r < args.hot_fraction + 0.007 {
            // A new file.
            let p = format!("{}/n{}.rs", leaf_dir(rng.below(args.subtrees)), n);
            st.files.insert(p.clone(), vec![format!("// new in {n}")]);
            st.src.push(p.clone());
            p
        } else if rng.below(5) == 0 {
            st.warm[rng.below(st.warm.len())].clone()
        } else {
            st.src[rng.below(st.src.len())].clone()
        };
        let lines = st.files.get_mut(&path).unwrap();
        let line = format!("agent-{agent} change {n} edits {path}");
        let len = lines.len();
        let op = rng.below(100);
        if (op < 10 && len < 400) || len == 0 {
            lines.insert(rng.below(len + 1), line);
        } else if op < 15 && len > 10 {
            lines.remove(rng.below(len));
        } else {
            let i = rng.below(len);
            lines[i] = line;
        }
        out.insert(path.clone(), content(lines));
    }
    out.into_iter().collect()
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let blobs = leep_store::open_blob_store(&args.blobs).context("blobs")?;
    let meta = leep_meta::open_meta_store(&args.meta)
        .await
        .context("meta")?;
    let repo = Repo::new(blobs, meta.clone());
    let mut rng = Rng::new(args.seed);

    let (mut st, mut head, mut generation) = match meta.get_ref(&args.target).await? {
        None => {
            let st = initial(&args);
            let mut changes = BTreeMap::new();
            let mut datas = Vec::new();
            for (p, lines) in &st.files {
                let d = content(lines);
                changes.insert(RepoPath::new(p.as_str())?, modify(&d));
                datas.push(d);
            }
            for chunk in datas.chunks(4096) {
                repo.put_blobs(chunk.to_vec()).await?;
            }
            let root = Changeset {
                parents: vec![],
                author: "bulk".into(),
                timestamp: 1_700_000_000,
                message: "monorepo root".into(),
                changes,
                extra: BTreeMap::new(),
            };
            let id = repo.create_changeset(root).await?;
            let ctx = RefUpdateContext::new("bulk");
            anyhow::ensure!(
                meta.cas_ref_logged(&args.target, None, Some(id), &ctx)
                    .await?
            );
            let g = repo.generation(id).await?;
            (st, id, g)
        }
        Some(h) => {
            let t = Instant::now();
            let infos = repo.list_files(h, "").await?;
            let mut files = BTreeMap::new();
            let (mut src, mut seeded, mut hot) = (Vec::new(), Vec::new(), Vec::new());
            for i in infos {
                let data = repo
                    .read_content(&i.content)
                    .await?
                    .context("missing blob at resume")?;
                let text = String::from_utf8_lossy(&data);
                let lines: Vec<String> = text.lines().map(str::to_string).collect();
                let p = i.path.as_str().to_string();
                if p.starts_with("src/") {
                    src.push(p.clone());
                } else if p.starts_with("hot/") {
                    hot.push(p.clone());
                } else {
                    seeded.push(p.clone());
                }
                files.insert(p, lines);
            }
            let g = repo.generation(h).await?;
            eprintln!(
                "resumed at generation {g} ({} files) in {:?}",
                files.len(),
                t.elapsed()
            );
            // Different choices after a resume, still deterministic.
            rng = Rng::new(args.seed ^ g);
            (finish(files, src, seeded, hot), h, g)
        }
    };
    let start = Instant::now();
    let start_gen = generation;
    let mut last_ref_gen = generation;
    let mut ref_at = head;
    // One ref log entry per changeset, as lands write them.
    let mut steps: Vec<RefStep> = Vec::new();
    let mut window = Instant::now();
    let mut window_gen = generation;
    while generation < args.until {
        let n_batch = (args.until - generation).min(args.batch as u64);
        let mut planned = Vec::with_capacity(n_batch as usize);
        let mut datas = Vec::new();
        for j in 0..n_batch {
            let n = generation + 1 + j;
            let agent = rng.below(1000);
            let e = edits(&args, &mut st, &mut rng, n, agent);
            for (_, d) in &e {
                datas.push(d.clone());
            }
            planned.push((n, agent, e));
        }
        repo.put_blobs(datas).await?;
        for (n, agent, e) in planned {
            let mut changes = BTreeMap::new();
            let mut first = String::new();
            for (p, d) in &e {
                if first.is_empty() {
                    first = p.clone();
                }
                changes.insert(RepoPath::new(p.as_str())?, modify(d));
            }
            let cs = Changeset {
                parents: vec![head],
                author: format!("agent-{agent} <agent-{agent}@example.com>"),
                timestamp: 1_700_000_000 + 2 * n as i64,
                message: format!("agent-{agent}: edit {first} ({} files)", e.len()),
                changes,
                extra: BTreeMap::from([("agent.model".to_string(), "bulk-gen".to_string())]),
            };
            head = repo.create_changeset(cs).await?;
            generation = n;
            steps.push(RefStep {
                new: Some(head),
                ctx: RefUpdateContext::new("land")
                    .actor(Some(format!("agent-{agent}")))
                    .message(format!("landed {head}")),
            });
            if generation - last_ref_gen >= args.ref_every || generation == args.until {
                let applied = meta
                    .cas_ref_logged_chain(&args.target, Some(ref_at), &steps)
                    .await?;
                anyhow::ensure!(applied == steps.len(), "ref moved under the writer");
                steps.clear();
                last_ref_gen = generation;
                ref_at = head;
            }
            if generation % 10_000 == 0 {
                let w = window.elapsed().as_secs_f64();
                println!(
                    "{} gen {generation} rate {:.0}/s (avg {:.0}/s) head {}",
                    chrono_now(),
                    (generation - window_gen) as f64 / w,
                    (generation - start_gen) as f64 / start.elapsed().as_secs_f64(),
                    head
                );
                window = Instant::now();
                window_gen = generation;
            }
        }
    }
    println!("done gen {generation} head {head}");
    Ok(())
}

fn chrono_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}
