//! Time the queries that are slow on a fresh server over a large packed
//! store (docs/PERFORMANCE.md, "Cold queries"): the full file listing, the
//! first blame of a long-lived file, and a diff across much of history.
//!
//! ```sh
//! cargo run --release -p leep-server --example cold_queries -- build <meta> <blobs> [--commits N]
//! leep-server --meta <meta> --blobs <blobs> repack --full --unsafe-no-fence --fence 0
//! cargo run --release -p leep-server --example cold_queries -- query <meta> <blobs> files|blame PATH|diff BACK
//! ```
//!
//! `build` writes a synthetic history to `main`: a tree shaped like Linux's
//! (thousands of directories, a few of 300 to 1,800 files, tens of thousands
//! of files of real lines), subsystem branches merged into mainline, and a
//! root `Makefile` edited on mainline and on branches. It then derives the
//! path-history index for `main`. Deterministic for a given `--commits`.
//! `finish <meta> <blobs> <head>` points `main` at `head` and derives its
//! index, for a build stopped before it got there.
//!
//! `query` opens the stores as `leep-server` does (a 256 MiB blob cache in
//! front), so each run is a freshly started server; drop the page cache and
//! restart Postgres between runs for a cold disk too. `blame` deletes cached
//! blames first. Prints the time and the store calls made.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::{stream, StreamExt};
use leep_core::{
    BlobMeta, BlobStore, Changeset, ChangesetId, DerivedRow, FileChange, FileKind, Hash, MetaStore,
    RefLogEntry, RefStep, RefUpdateContext, RepoPath, Result,
};
use leep_derive::derived::{self, HistoryTree};
use leep_derive::Repo;
use leep_store::CachedBlobStore;

#[derive(Default)]
struct Tally(Mutex<BTreeMap<String, (u64, u64, Duration)>>);

impl Tally {
    fn add(&self, name: &str, items: usize, d: Duration) {
        let mut m = self.0.lock().unwrap();
        let e = m.entry(name.to_string()).or_default();
        e.0 += 1;
        e.1 += items as u64;
        e.2 += d;
    }
}

macro_rules! timed {
    ($tally:expr, $name:expr, $items:expr, $e:expr) => {{
        let t = Instant::now();
        let r = $e;
        $tally.add(&$name, $items, t.elapsed());
        r
    }};
}

struct Blobs(Arc<dyn BlobStore>, Arc<Tally>);

#[async_trait]
impl BlobStore for Blobs {
    async fn put_with_hash(&self, hash: Hash, data: Bytes) -> Result<()> {
        self.0.put_with_hash(hash, data).await
    }
    async fn get(&self, hash: &Hash) -> Result<Option<Bytes>> {
        timed!(self.1, "blob get", 1, self.0.get(hash).await)
    }
    async fn get_many(&self, hashes: &[Hash]) -> Result<Vec<Option<Bytes>>> {
        timed!(
            self.1,
            "blob get_many",
            hashes.len(),
            self.0.get_many(hashes).await
        )
    }
    async fn contains(&self, hash: &Hash) -> Result<bool> {
        self.0.contains(hash).await
    }
    async fn contains_many(&self, hashes: &[Hash]) -> Result<Vec<bool>> {
        self.0.contains_many(hashes).await
    }
    async fn flush(&self, named: &[Hash]) -> Result<()> {
        self.0.flush(named).await
    }
    async fn list_page(&self, after: Option<Hash>, limit: usize) -> Result<Vec<BlobMeta>> {
        self.0.list_page(after, limit).await
    }
    async fn delete(&self, hash: &Hash) -> Result<()> {
        self.0.delete(hash).await
    }
}

struct Meta(Arc<dyn MetaStore>, Arc<Tally>);

#[async_trait]
impl MetaStore for Meta {
    async fn put_changeset(&self, cs: &Changeset) -> Result<ChangesetId> {
        self.0.put_changeset(cs).await
    }
    async fn get_changeset(&self, id: &ChangesetId) -> Result<Option<Changeset>> {
        timed!(self.1, "get_changeset", 1, self.0.get_changeset(id).await)
    }
    async fn get_changesets(&self, ids: &[ChangesetId]) -> Result<Vec<Option<Changeset>>> {
        timed!(
            self.1,
            "get_changesets",
            ids.len(),
            self.0.get_changesets(ids).await
        )
    }
    async fn list_changesets(
        &self,
        after: Option<ChangesetId>,
        limit: usize,
    ) -> Result<Vec<ChangesetId>> {
        self.0.list_changesets(after, limit).await
    }
    async fn get_ref(&self, name: &str) -> Result<Option<ChangesetId>> {
        self.0.get_ref(name).await
    }
    async fn cas_ref_logged(
        &self,
        name: &str,
        expected: Option<ChangesetId>,
        new: Option<ChangesetId>,
        ctx: &RefUpdateContext,
    ) -> Result<bool> {
        self.0.cas_ref_logged(name, expected, new, ctx).await
    }
    async fn cas_ref_logged_chain(
        &self,
        name: &str,
        expected: Option<ChangesetId>,
        steps: &[RefStep],
    ) -> Result<usize> {
        self.0.cas_ref_logged_chain(name, expected, steps).await
    }
    async fn ref_log(
        &self,
        name: Option<&str>,
        before_seq: Option<u64>,
        limit: usize,
    ) -> Result<Vec<RefLogEntry>> {
        self.0.ref_log(name, before_seq, limit).await
    }
    async fn list_refs(&self) -> Result<BTreeMap<String, ChangesetId>> {
        self.0.list_refs().await
    }
    async fn list_derived(
        &self,
        kind: &str,
        after: Option<Hash>,
        limit: usize,
    ) -> Result<Vec<(Hash, Bytes)>> {
        self.0.list_derived(kind, after, limit).await
    }
    async fn delete_derived_kind(&self, kind: &str) -> Result<u64> {
        self.0.delete_derived_kind(kind).await
    }
    async fn get_derived(&self, kind: &str, key: &Hash) -> Result<Option<Bytes>> {
        timed!(
            self.1,
            format!("get_derived {kind}"),
            1,
            self.0.get_derived(kind, key).await
        )
    }
    async fn get_derived_many(&self, kind: &str, keys: &[Hash]) -> Result<Vec<Option<Bytes>>> {
        timed!(
            self.1,
            format!("get_derived_many {kind}"),
            keys.len(),
            self.0.get_derived_many(kind, keys).await
        )
    }
    async fn put_derived(&self, kind: &str, key: &Hash, value: Bytes) -> Result<()> {
        timed!(
            self.1,
            format!("put_derived {kind}"),
            1,
            self.0.put_derived(kind, key, value).await
        )
    }
    async fn put_derived_many(&self, rows: &[DerivedRow]) -> Result<()> {
        timed!(
            self.1,
            "put_derived_many",
            rows.len(),
            self.0.put_derived_many(rows).await
        )
    }
    async fn put_changeset_with_derived(
        &self,
        cs: &Changeset,
        rows: &[DerivedRow],
    ) -> Result<ChangesetId> {
        self.0.put_changeset_with_derived(cs, rows).await
    }
    async fn schema_version(&self) -> Result<Option<u64>> {
        self.0.schema_version().await
    }
}

struct Lcg(u64);

impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0 >> 33
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }

    fn chance(&mut self, percent: u64) -> bool {
        self.next() % 100 < percent
    }
}

/// A file's text as line ids; the text of a line is a function of its id.
type Text = Arc<Vec<u32>>;

fn render(file: usize, text: &[u32]) -> Bytes {
    let mut s = String::with_capacity(text.len() * 48);
    for id in text {
        s.push_str(&format!(
            "\tline {id:08x} of file {file} lorem ipsum dolor\n"
        ));
    }
    Bytes::from(s)
}

/// Every file, by number; `None` once deleted.
#[derive(Clone)]
struct State(Vec<Option<Text>>);

struct Gen {
    rng: Lcg,
    next_line: u32,
    paths: Vec<String>,
    /// Files of each directory, by number.
    dirs: Vec<Vec<usize>>,
    seq: u64,
}

const MAKEFILE: usize = 0;

impl Gen {
    fn new_text(&mut self, lines: usize) -> Text {
        let t = (0..lines).map(|_| self.line()).collect();
        Arc::new(t)
    }

    fn line(&mut self) -> u32 {
        self.next_line += 1;
        self.next_line
    }

    fn edit(&mut self, text: &Text) -> Text {
        let mut t = (**text).clone();
        for _ in 0..1 + self.rng.below(4) {
            let at = self.rng.below(t.len().max(1));
            match self.rng.below(3) {
                0 if t.len() > 4 => {
                    t.remove(at);
                }
                1 => t.insert(at, self.line()),
                _ if !t.is_empty() => t[at] = self.line(),
                _ => t.push(self.line()),
            }
        }
        Arc::new(t)
    }

    /// Edits of 1-5 files, mostly in `home` directories; sometimes the
    /// Makefile, a new file or a deletion.
    fn work(
        &mut self,
        home: &[usize],
        state: &State,
        makefile: u64,
    ) -> BTreeMap<usize, Option<Text>> {
        let mut out = BTreeMap::new();
        if self.rng.chance(makefile) {
            let t = state.0[MAKEFILE].clone().expect("the Makefile stays");
            out.insert(MAKEFILE, Some(self.edit(&t)));
        }
        for _ in 0..1 + self.rng.below(5) {
            let d = if self.rng.chance(85) {
                home[self.rng.below(home.len())]
            } else {
                self.rng.below(self.dirs.len())
            };
            if self.rng.chance(4) {
                let n = self.paths.len();
                let dir = parent(&self.paths[self.dirs[d][0]]).to_string();
                self.paths.push(format!("{dir}/new{n:06}.c"));
                self.dirs[d].push(n);
                let lines = 20 + self.rng.below(200);
                out.insert(n, Some(self.new_text(lines)));
                continue;
            }
            let f = self.dirs[d][self.rng.below(self.dirs[d].len())];
            let cur = out
                .get(&f)
                .cloned()
                .unwrap_or_else(|| state.0.get(f).cloned().flatten());
            match cur {
                Some(t) if f != MAKEFILE && self.rng.chance(2) => {
                    let _ = t;
                    out.insert(f, None);
                }
                Some(t) => {
                    out.insert(f, Some(self.edit(&t)));
                }
                None => {}
            }
        }
        out
    }
}

fn parent(path: &str) -> &str {
    path.rsplit_once('/').map_or("", |(d, _)| d)
}

struct Line {
    tip: ChangesetId,
    state: State,
    /// Files changed since the line last merged into mainline.
    dirty: std::collections::BTreeSet<usize>,
    home: Vec<usize>,
    commits: usize,
    merge_after: usize,
}

struct Writer<'a> {
    repo: &'a Repo,
}

impl Writer<'_> {
    async fn commit(
        &self,
        g: &mut Gen,
        parents: Vec<ChangesetId>,
        updates: &BTreeMap<usize, Option<Text>>,
    ) -> ChangesetId {
        let blobs: Vec<(usize, Bytes)> = updates
            .iter()
            .filter_map(|(f, t)| t.as_ref().map(|t| (*f, render(*f, t))))
            .collect();
        let hashes: Vec<(usize, Hash, u64)> = stream::iter(blobs)
            .map(|(f, data)| async move {
                let len = data.len() as u64;
                (f, self.repo.blobs().put(data).await.unwrap(), len)
            })
            .buffer_unordered(64)
            .collect()
            .await;
        let mut changes = BTreeMap::new();
        for (f, h, len) in hashes {
            changes.insert(
                RepoPath::new(g.paths[f].clone()).unwrap(),
                FileChange::Modify {
                    content: h,
                    kind: FileKind::Regular,
                    size: len,
                },
            );
        }
        for (f, t) in updates {
            if t.is_none() {
                changes.insert(
                    RepoPath::new(g.paths[*f].clone()).unwrap(),
                    FileChange::Delete,
                );
            }
        }
        g.seq += 1;
        let cs = Changeset {
            parents,
            author: format!("dev{} <dev@example.com>", g.seq % 211),
            timestamp: 1_100_000_000 + g.seq as i64 * 600,
            message: format!("change {}", g.seq),
            changes,
            extra: BTreeMap::new(),
        };
        self.repo.create_changeset(cs).await.unwrap()
    }
}

/// `updates` without deletes of files `state` does not have (made and
/// deleted on a branch, or within one commit).
fn effective(
    state: &State,
    mut updates: BTreeMap<usize, Option<Text>>,
) -> BTreeMap<usize, Option<Text>> {
    updates.retain(|f, t| t.is_some() || state.0.get(*f).is_some_and(|x| x.is_some()));
    updates
}

fn apply(state: &mut State, updates: &BTreeMap<usize, Option<Text>>) {
    for (f, t) in updates {
        if state.0.len() <= *f {
            state.0.resize(*f + 1, None);
        }
        state.0[*f] = t.clone();
    }
}

async fn build(repo: &Repo, commits: usize) -> ChangesetId {
    let mut g = Gen {
        rng: Lcg(0xc01d),
        next_line: 0,
        paths: vec!["Makefile".to_string()],
        dirs: vec![vec![MAKEFILE]],
        seq: 0,
    };
    // Directories four levels deep; one in 40 holds thousands of files.
    let mut dirs: Vec<String> = Vec::new();
    for t in 0..18 {
        let top = format!("t{t:02}");
        dirs.push(top.clone());
        for s in 0..3 + g.rng.below(30) {
            let sub = format!("{top}/sub{s:02}");
            dirs.push(sub.clone());
            for k in 0..g.rng.below(9) {
                let leaf = format!("{sub}/k{k}");
                dirs.push(leaf.clone());
                for m in 0..g.rng.below(4) {
                    dirs.push(format!("{leaf}/m{m}"));
                }
            }
        }
    }
    for d in &dirs {
        let n = if g.rng.chance(1) {
            300 + g.rng.below(1500)
        } else {
            2 + g.rng.below(30)
        };
        let files: Vec<usize> = (0..n)
            .map(|i| {
                g.paths.push(format!("{d}/file{i:04}.c"));
                g.paths.len() - 1
            })
            .collect();
        g.dirs.push(files);
    }
    let mut state = State(Vec::new());
    let mut initial = BTreeMap::new();
    initial.insert(MAKEFILE, Some(g.new_text(1800)));
    for f in 1..g.paths.len() {
        let lines = 10 + g.rng.below(120);
        initial.insert(f, Some(g.new_text(lines)));
    }
    println!("{} directories, {} files", g.dirs.len(), g.paths.len());
    let w = Writer { repo };
    let mut tip: Option<ChangesetId> = None;
    let all: Vec<(usize, Option<Text>)> = initial.into_iter().collect();
    for chunk in all.chunks(3000) {
        let updates: BTreeMap<usize, Option<Text>> = chunk.iter().cloned().collect();
        tip = Some(w.commit(&mut g, tip.into_iter().collect(), &updates).await);
        apply(&mut state, &updates);
    }
    let mut main = Line {
        tip: tip.expect("files to import"),
        state,
        dirty: Default::default(),
        home: (0..g.dirs.len()).collect(),
        commits: 0,
        merge_after: 0,
    };
    let ndirs = g.dirs.len();
    let mut branches: Vec<Line> = (0..10)
        .map(|b| Line {
            tip: main.tip,
            state: main.state.clone(),
            dirty: Default::default(),
            home: (0..6)
                .map(|k| 1 + (b * 97 + k * 31) % (ndirs - 1))
                .collect(),
            commits: 0,
            merge_after: 5 + (b * 13) % 40,
        })
        .collect();
    let start = g.seq;
    let t0 = Instant::now();
    while ((g.seq - start) as usize) < commits {
        if (g.seq - start).is_multiple_of(5000) {
            println!(
                "{:>7} commits, {:.0} s",
                g.seq - start,
                t0.elapsed().as_secs_f64()
            );
        }
        let r = g.rng.below(100);
        if r < 8 {
            let home = main.home.clone();
            let updates = effective(&main.state, g.work(&home, &main.state, 30));
            main.tip = w.commit(&mut g, vec![main.tip], &updates).await;
            apply(&mut main.state, &updates);
            continue;
        }
        let b = g.rng.below(branches.len());
        let line = &mut branches[b];
        if r < 11 {
            // Merge mainline back into the branch: mainline wins where the
            // branch did not change a file.
            let mut updates = BTreeMap::new();
            let n = main.state.0.len().max(line.state.0.len());
            for f in 0..n {
                let m = main.state.0.get(f).cloned().flatten();
                let l = line.state.0.get(f).cloned().flatten();
                let same = match (&m, &l) {
                    (Some(a), Some(b)) => Arc::ptr_eq(a, b),
                    (None, None) => true,
                    _ => false,
                };
                if !same && !line.dirty.contains(&f) {
                    updates.insert(f, m);
                }
            }
            let updates = effective(&line.state, updates);
            line.tip = w.commit(&mut g, vec![line.tip, main.tip], &updates).await;
            apply(&mut line.state, &updates);
        } else {
            let home = line.home.clone();
            let updates = effective(&line.state, g.work(&home, &line.state, 1));
            line.tip = w.commit(&mut g, vec![line.tip], &updates).await;
            apply(&mut line.state, &updates);
            line.dirty.extend(updates.keys().copied());
            line.commits += 1;
        }
        if line.commits >= line.merge_after && !line.dirty.is_empty() {
            let updates: BTreeMap<usize, Option<Text>> = line
                .dirty
                .iter()
                .map(|&f| (f, line.state.0.get(f).cloned().flatten()))
                .collect();
            let updates = effective(&main.state, updates);
            main.tip = w.commit(&mut g, vec![main.tip, line.tip], &updates).await;
            apply(&mut main.state, &updates);
            line.dirty.clear();
            line.commits = 0;
            line.merge_after = 5 + g.rng.below(40);
            if g.rng.chance(40) {
                line.tip = main.tip;
                line.state = main.state.clone();
            }
        }
    }
    main.tip
}

fn usage() -> ! {
    eprintln!(
        "usage: cold_queries build <meta> <blobs> [--commits N]\n       \
         cold_queries finish <meta> <blobs> <head>\n       \
         cold_queries query <meta> <blobs> files|blame PATH|diff BACK"
    );
    std::process::exit(2)
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.len() < 3 {
        usage();
    }
    let raw_blobs = leep_store::open_blob_store(&args[2]).unwrap();
    let raw_meta = leep_meta::open_meta_store(&args[1]).await.unwrap();
    match args[0].as_str() {
        "build" => {
            let commits = args
                .iter()
                .position(|a| a == "--commits")
                .map(|i| args[i + 1].parse().unwrap_or_else(|_| usage()))
                .unwrap_or(60_000);
            let repo = Repo::new(raw_blobs, raw_meta.clone());
            let t = Instant::now();
            let head = build(&repo, commits).await;
            let old = raw_meta.get_ref("main").await.unwrap();
            assert!(raw_meta.cas_ref("main", old, Some(head)).await.unwrap());
            println!("built in {:.1} s, head {head}", t.elapsed().as_secs_f64());
            let t = Instant::now();
            derived::derive::<HistoryTree>(&repo, head).await.unwrap();
            println!("derived path history in {:.1} s", t.elapsed().as_secs_f64());
        }
        "finish" => {
            // Point `main` at a given head and derive its index, for a build
            // that was stopped before it got there.
            let head: ChangesetId = args.get(3).unwrap_or_else(|| usage()).parse().unwrap();
            let repo = Repo::new(raw_blobs, raw_meta.clone());
            let old = raw_meta.get_ref("main").await.unwrap();
            assert!(raw_meta.cas_ref("main", old, Some(head)).await.unwrap());
            let t = Instant::now();
            derived::derive::<HistoryTree>(&repo, head).await.unwrap();
            println!("derived path history in {:.1} s", t.elapsed().as_secs_f64());
        }
        "query" => {
            let tally = Arc::new(Tally::default());
            let cached: Arc<dyn BlobStore> = Arc::new(CachedBlobStore::new(
                Blobs(raw_blobs, tally.clone()),
                256 << 20,
            ));
            let meta: Arc<dyn MetaStore> = Arc::new(Meta(raw_meta.clone(), tally.clone()));
            let repo = Repo::new(cached, meta);
            let head = repo.resolve("main").await.unwrap();
            let t = Instant::now();
            let what = args.get(3).map(String::as_str).unwrap_or_else(|| usage());
            let summary = match what {
                "files" => {
                    let files = repo.list_files(head, "").await.unwrap();
                    format!("{} files", files.len())
                }
                "blame" => {
                    let path =
                        RepoPath::new(args.get(4).unwrap_or_else(|| usage()).clone()).unwrap();
                    raw_meta.delete_derived_kind("blame").await.unwrap();
                    *tally.0.lock().unwrap() = BTreeMap::new();
                    let t = Instant::now();
                    let b = repo.blame(head, &path).await.unwrap();
                    format!(
                        "{} lines from {} changesets in {:.3} s",
                        b.lines.len(),
                        b.changesets.len(),
                        t.elapsed().as_secs_f64()
                    )
                }
                "diff" => {
                    let back: usize = args.get(4).unwrap_or_else(|| usage()).parse().unwrap();
                    // The first-parent ancestor `back` steps down, found
                    // without timing the walk.
                    let mut from = head;
                    for _ in 0..back {
                        let cs = raw_meta.get_changeset(&from).await.unwrap().unwrap();
                        match cs.parents.first() {
                            Some(p) => from = *p,
                            None => break,
                        }
                    }
                    *tally.0.lock().unwrap() = BTreeMap::new();
                    let t = Instant::now();
                    let d = repo.diff(Some(from), head, "").await.unwrap();
                    format!("{} entries in {:.3} s", d.len(), t.elapsed().as_secs_f64())
                }
                _ => usage(),
            };
            println!("{what}: {:.3} s ({summary})", t.elapsed().as_secs_f64());
            for (name, (n, items, d)) in tally.0.lock().unwrap().iter() {
                println!(
                    "  {name:<36} {n:>8} calls {items:>9} items {:>8.2} s",
                    d.as_secs_f64()
                );
            }
        }
        _ => usage(),
    }
}
