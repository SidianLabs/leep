//! Invariant checks, run every few seconds and once more at the end.
//!
//! The checker reads only through the HTTP API, so it works against any
//! server. It keeps every changeset it has fully walked (immutable, so the
//! cache never goes stale) and re-reads samples of them to catch damage that
//! appears later, such as a blob GC should not have deleted.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use leep_core::api::{
    BlameResponse, ChangesetInfo, DiffResponse, FilesResponse, LastChangeResponse, LogResponse,
    MissingBlobsRequest, MissingBlobsResponse, ProvenanceResponse, RefLogResponse, RefsResponse,
    SearchResponse,
};
use leep_core::{ChangesetId, FileChange, Hash};
use reqwest::Method;

use crate::agent::{urlenc, MODELS};
use crate::api::{Api, Fail, R};
use crate::shared::{key_hash, key_of_message, token_for, Shared, Where};
use crate::util::Rng;

/// Changesets of path history kept around for the window check.
const RECENT_KEEP: u64 = 3000;
const WINDOW: u64 = 600;
const BLOB_SAMPLE: usize = 20_000;

/// A path and the content hash it was given (`None` for a delete).
type Change = (String, Option<Hash>);

pub struct Node {
    parents: Vec<ChangesetId>,
    generation: u64,
    key: Option<u64>,
}

pub struct Checker {
    sh: Arc<Shared>,
    api: Api,
    nodes: HashMap<ChangesetId, Node>,
    order: Vec<ChangesetId>,
    /// Paths changed by recent changesets, for the path-history window check.
    recent: HashMap<ChangesetId, Vec<String>>,
    max_gen: u64,
    verified_upto: usize,
    blob_sample: Vec<Hash>,
    blobs_seen: u64,
    rng: Rng,
    last_provenance: std::time::Instant,
    pub counts: BTreeMap<&'static str, u64>,
    pub last_tick_ms: u64,
}

impl Checker {
    pub fn new(sh: Arc<Shared>, api: Api) -> Checker {
        let rng = Rng::new(sh.args.seed ^ 0xC4EC_4E25);
        Checker {
            sh,
            api,
            nodes: HashMap::new(),
            order: Vec::new(),
            recent: HashMap::new(),
            max_gen: 0,
            verified_upto: 0,
            blob_sample: Vec::new(),
            blobs_seen: 0,
            rng,
            last_provenance: std::time::Instant::now(),
            counts: BTreeMap::new(),
            last_tick_ms: 0,
        }
    }

    fn phase(&mut self, name: &'static str, since: std::time::Instant) {
        let ms = since.elapsed().as_millis() as u64;
        let key = match name {
            "walk" => "ms_walk",
            "verify_acks" => "ms_verify_acks",
            "check_acks" => "ms_check_acks",
            "sampled" => "ms_sampled",
            "reflog" => "ms_reflog",
            "deep_reads" => "ms_deep_reads",
            "path_log" => "ms_path_log",
            "blame" => "ms_blame",
            "search" => "ms_search",
            "provenance" => "ms_provenance",
            _ => "ms_full_recheck",
        };
        self.count(key, ms);
    }

    fn count(&mut self, name: &'static str, n: u64) {
        *self.counts.entry(name).or_default() += n;
    }

    pub fn nodes_known(&self) -> usize {
        self.nodes.len()
    }

    async fn fetch_many(&self, ids: Vec<ChangesetId>) -> R<Vec<(ChangesetId, ChangesetInfo)>> {
        let mut out = Vec::with_capacity(ids.len());
        for chunk in ids.chunks(32) {
            let mut set = tokio::task::JoinSet::new();
            for id in chunk {
                let api = self.api.clone();
                let id = *id;
                set.spawn(async move { (id, api.changeset(id).await) });
            }
            while let Some(j) = set.join_next().await {
                let (id, r) = j.expect("fetch task");
                out.push((id, r?));
            }
        }
        Ok(out)
    }

    fn note_blob(&mut self, h: Hash) {
        self.blobs_seen += 1;
        if self.blob_sample.len() < BLOB_SAMPLE {
            self.blob_sample.push(h);
        } else {
            let j = self.rng.next_u64() % self.blobs_seen;
            if (j as usize) < BLOB_SAMPLE {
                self.blob_sample[j as usize] = h;
            }
        }
    }

    /// Fetch every changeset reachable from `heads` that is not known yet,
    /// checking each one as it arrives. Nodes join the cache only once the
    /// walk is complete, so a cached node always has a cached ancestry.
    async fn walk(&mut self, heads: &[ChangesetId]) -> R<()> {
        let mut pending: HashMap<ChangesetId, ChangesetInfo> = HashMap::new();
        let mut frontier: Vec<ChangesetId> = heads
            .iter()
            .copied()
            .filter(|h| !self.nodes.contains_key(h))
            .collect();
        frontier.sort();
        frontier.dedup();
        while !frontier.is_empty() {
            let batch: Vec<ChangesetId> = std::mem::take(&mut frontier);
            let got = self.fetch_many(batch).await?;
            for (id, info) in got {
                if info.id != id || info.changeset.id() != id {
                    self.sh.violate(
                        "changeset_integrity",
                        format!("changeset {id} reads back as {}", info.changeset.id()),
                    );
                    return Err(Fail::Bad);
                }
                for p in &info.changeset.parents {
                    if !self.nodes.contains_key(p) && !pending.contains_key(p) {
                        frontier.push(*p);
                    }
                }
                pending.insert(id, info);
            }
            frontier.sort();
            frontier.dedup();
            frontier.retain(|p| !pending.contains_key(p));
        }
        // Generations: one more than the largest parent's.
        for (id, info) in &pending {
            let mut want = 0;
            for p in &info.changeset.parents {
                let g = self
                    .nodes
                    .get(p)
                    .map(|n| n.generation)
                    .or_else(|| pending.get(p).map(|i| i.generation))
                    .unwrap_or(0);
                want = want.max(g);
            }
            if info.generation != want + 1 {
                self.sh.violate(
                    "generation",
                    format!(
                        "{id} has generation {} but its parents say {}",
                        info.generation,
                        want + 1
                    ),
                );
            }
        }
        let added = pending.len() as u64;
        for (id, info) in pending {
            let key = key_of_message(&info.changeset.message).map(key_hash);
            let paths: Vec<String> = info
                .changeset
                .changes
                .keys()
                .map(|p| p.as_str().to_string())
                .collect();
            for c in info.changeset.changes.values() {
                if let FileChange::Modify { content, .. } = c {
                    self.note_blob(*content);
                }
            }
            self.max_gen = self.max_gen.max(info.generation);
            self.recent.insert(id, paths);
            self.order.push(id);
            self.nodes.insert(
                id,
                Node {
                    parents: info.changeset.parents.clone(),
                    generation: info.generation,
                    key,
                },
            );
        }
        let floor = self.max_gen.saturating_sub(RECENT_KEEP);
        self.recent
            .retain(|id, _| self.nodes.get(id).is_some_and(|n| n.generation > floor));
        self.count("changesets_walked", added);
        Ok(())
    }

    /// Everything reachable from `head`, not descending below `above`.
    fn closure(&self, head: ChangesetId, above: u64) -> HashSet<ChangesetId> {
        let mut seen = HashSet::new();
        let mut stack = vec![head];
        while let Some(id) = stack.pop() {
            let Some(n) = self.nodes.get(&id) else {
                continue;
            };
            if n.generation <= above || !seen.insert(id) {
                continue;
            }
            stack.extend(n.parents.iter().copied());
        }
        seen
    }

    fn key_counts(&self, set: &HashSet<ChangesetId>) -> HashMap<u64, u32> {
        let mut m: HashMap<u64, u32> = HashMap::new();
        for id in set {
            if let Some(k) = self.nodes.get(id).and_then(|n| n.key) {
                *m.entry(k).or_default() += 1;
            }
        }
        m
    }

    async fn refs(&self) -> R<BTreeMap<String, ChangesetId>> {
        let r: RefsResponse = self.api.get_ok("refs", "/v1/refs").await?;
        Ok(r.refs)
    }

    /// One round of checks. `last` adds the exhaustive ones.
    pub async fn tick(&mut self, last: bool) -> R<()> {
        let t0 = std::time::Instant::now();
        // The ledger is read first: whatever it lists was acknowledged before
        // the heads read below, so it must be in them.
        let (n_acks, ver0, tags, branch_base) = {
            let l = self.sh.ledger.0.lock().unwrap();
            (
                l.acks.len(),
                l.version,
                l.tags.clone(),
                l.branch_base.clone(),
            )
        };
        let refs = self.refs().await?;
        let heads: Vec<ChangesetId> = refs.values().copied().collect();
        let t = std::time::Instant::now();
        self.walk(&heads).await?;
        self.phase("walk", t);
        let t = std::time::Instant::now();
        self.verify_new_acks(n_acks).await?;
        self.phase("verify_acks", t);
        let t = std::time::Instant::now();
        self.check_acks(n_acks, ver0, &refs, &branch_base)?;
        self.check_tags(&refs, &tags);
        self.phase("check_acks", t);
        let t = std::time::Instant::now();
        self.sampled(&refs, last).await?;
        self.phase("sampled", t);
        let t = std::time::Instant::now();
        self.check_reflog(last, &refs).await?;
        self.phase("reflog", t);
        if last {
            let t = std::time::Instant::now();
            self.full_recheck().await?;
            self.phase("full_recheck", t);
        }
        self.count("ticks", 1);
        self.last_tick_ms = t0.elapsed().as_millis() as u64;
        Ok(())
    }

    /// First look at each new acknowledgement: the changeset is there with
    /// the message and the paths the agent gave it.
    async fn verify_new_acks(&mut self, n: usize) -> R<()> {
        if self.verified_upto >= n {
            return Ok(());
        }
        struct Todo {
            idx: usize,
            landed: ChangesetId,
            steps: Vec<(String, Vec<Change>)>,
            rebased: bool,
        }
        let todo: Vec<Todo> = {
            let l = self.sh.ledger.0.lock().unwrap();
            (self.verified_upto..n)
                .map(|i| {
                    let a = &l.acks[i];
                    Todo {
                        idx: i,
                        landed: a.landed,
                        steps: a
                            .steps
                            .iter()
                            .map(|s| (s.key.clone(), s.changes.clone()))
                            .collect(),
                        rebased: a.rebased,
                    }
                })
                .collect()
        };
        // The stack is a first-parent chain ending at `landed`.
        let mut want: Vec<ChangesetId> = todo.iter().map(|t| t.landed).collect();
        let mut layer = 0;
        let mut chains: Vec<Vec<ChangesetId>> = todo.iter().map(|t| vec![t.landed]).collect();
        let depth = todo.iter().map(|t| t.steps.len()).max().unwrap_or(1);
        let mut infos: HashMap<ChangesetId, ChangesetInfo> = HashMap::new();
        while layer < depth {
            want.sort();
            want.dedup();
            let got = self.fetch_many(want.clone()).await?;
            let mut next = Vec::new();
            for (id, info) in got {
                infos.insert(id, info);
            }
            for (t, chain) in todo.iter().zip(chains.iter_mut()) {
                if chain.len() == layer + 1 && t.steps.len() > layer + 1 {
                    if let Some(p) = infos
                        .get(chain.last().unwrap())
                        .and_then(|i| i.changeset.parents.first())
                    {
                        chain.push(*p);
                        if !infos.contains_key(p) {
                            next.push(*p);
                        }
                    }
                }
            }
            want = next;
            layer += 1;
        }
        for (t, chain) in todo.iter().zip(&chains) {
            // chain[0] is the top step; steps are stored oldest first.
            for (j, (key, expected)) in t.steps.iter().rev().enumerate() {
                let Some(info) = chain.get(j).and_then(|id| infos.get(id)) else {
                    self.sh.violate(
                        "ack_content",
                        format!("land {} lost step {key}: stack too short", t.landed),
                    );
                    continue;
                };
                let prefix = format!("soak {key} ");
                if !info.changeset.message.starts_with(&prefix) {
                    self.sh.violate(
                        "ack_content",
                        format!(
                            "land {}: step {key} is {:?}",
                            t.landed, info.changeset.message
                        ),
                    );
                    continue;
                }
                if expected.is_empty() {
                    continue;
                }
                self.compare_changes(t.landed, key, info, expected, t.rebased);
            }
        }
        {
            let mut l = self.sh.ledger.0.lock().unwrap();
            for t in &todo {
                let a = &mut l.acks[t.idx];
                a.verified = true;
                for s in &mut a.steps {
                    s.changes = Vec::new();
                }
            }
        }
        self.count("acks_verified", (n - self.verified_upto) as u64);
        self.verified_upto = n;
        Ok(())
    }

    fn compare_changes(
        &self,
        landed: ChangesetId,
        key: &str,
        info: &ChangesetInfo,
        expected: &[Change],
        rebased: bool,
    ) {
        let got = &info.changeset.changes;
        let mut bad = Vec::new();
        for (p, h) in expected {
            let c = got.iter().find(|(gp, _)| gp.as_str() == p).map(|(_, c)| c);
            match (h, c) {
                (Some(h), Some(FileChange::Modify { content, .. })) if content == h => {}
                // A replay merges text, so a file others also changed differs.
                (Some(_), Some(FileChange::Modify { .. })) if rebased => {}
                (None, Some(FileChange::Delete)) => {}
                // A replay may drop a change the new head already contains.
                (_, None) if rebased => {}
                (_, c) => bad.push(format!("{p}: expected {h:?}, found {c:?}")),
            }
        }
        if got.len() > expected.len() {
            bad.push(format!(
                "{} paths changed, expected {}",
                got.len(),
                expected.len()
            ));
        }
        if !bad.is_empty() {
            self.sh.violate(
                "ack_content",
                format!(
                    "land {landed} step {key} (rebased {rebased}): {}",
                    bad.join("; ")
                ),
            );
        }
    }

    /// Every acknowledged key is in the history it was landed on, exactly
    /// once, and nothing forbidden is.
    fn check_acks(
        &mut self,
        n: usize,
        ver0: u64,
        refs: &BTreeMap<String, ChangesetId>,
        base: &HashMap<String, u64>,
    ) -> R<()> {
        let Some(main) = refs.get("main").copied() else {
            self.sh.violate("refs", "main does not exist");
            return Err(Fail::Bad);
        };
        let main_set = self.closure(main, 0);
        let main_counts = self.key_counts(&main_set);
        for (k, c) in &main_counts {
            if *c > 1 {
                self.sh.violate(
                    "history_duplicate",
                    format!("key hash {k:x} appears {c} times in main's history"),
                );
            }
        }
        // Per-branch closures are built on demand and cached for this round.
        let mut branch: HashMap<String, (HashSet<ChangesetId>, HashMap<u64, u32>)> = HashMap::new();
        for name in refs.keys() {
            if name == "main" || name.starts_with("tags/") {
                continue;
            }
            let above = if name == "release" {
                0
            } else {
                base.get(name).copied().unwrap_or(0)
            };
            let set = self.closure(refs[name], above);
            let counts = self.key_counts(&set);
            for (k, c) in &counts {
                if *c > 1 {
                    self.sh.violate(
                        "history_duplicate",
                        format!("key hash {k:x} appears {c} times above {name}'s base"),
                    );
                }
            }
            branch.insert(name.clone(), (set, counts));
        }
        let mut checked = 0u64;
        let mut i = 0;
        while i < n {
            let end = (i + 20_000).min(n);
            let l = self.sh.ledger.0.lock().unwrap();
            for idx in i..end {
                let a = &l.acks[idx];
                // Moved to main or retired after the refs were read: those
                // heads cannot hold it yet, and the next round will see it.
                if a.place_ver > ver0 {
                    continue;
                }
                match &a.place {
                    Where::Retired => continue,
                    Where::Main => {
                        for s in &a.steps {
                            if main_counts.get(&key_hash(&s.key)).copied().unwrap_or(0) != 1 {
                                let c = main_counts.get(&key_hash(&s.key)).copied().unwrap_or(0);
                                self.sh.violate(
                                    "ack_lost",
                                    format!(
                                        "{} (agent {}, merged from a branch) is in main's history {c} times",
                                        s.key, a.agent
                                    ),
                                );
                            }
                        }
                    }
                    Where::Ref(r) if r == "main" => {
                        if !main_set.contains(&a.landed) {
                            self.sh.violate(
                                "ack_lost",
                                format!(
                                    "landed {} ({}) is not in main's history",
                                    a.landed, a.agent
                                ),
                            );
                        }
                        for s in &a.steps {
                            let c = main_counts.get(&key_hash(&s.key)).copied().unwrap_or(0);
                            if c != 1 {
                                self.sh.violate(
                                    "ack_lost",
                                    format!(
                                        "{} (agent {}) is in main's history {c} times",
                                        s.key, a.agent
                                    ),
                                );
                            }
                        }
                    }
                    Where::Ref(r) => match branch.get(r) {
                        Some((set, counts)) => {
                            if !set.contains(&a.landed) {
                                self.sh.violate(
                                    "ack_lost",
                                    format!("landed {} is not in {r}'s history", a.landed),
                                );
                            }
                            for s in &a.steps {
                                let c = counts.get(&key_hash(&s.key)).copied().unwrap_or(0);
                                if c != 1 {
                                    self.sh.violate(
                                        "ack_lost",
                                        format!("{} is in {r}'s history {c} times", s.key),
                                    );
                                }
                            }
                        }
                        None => {
                            // Deleted after the ledger moved it; the live
                            // state tells where it went.
                            self.sh.violate(
                                "ref_vanished",
                                format!("{r} is gone but its lands ({}) still expect it", a.agent),
                            );
                        }
                    },
                }
                checked += 1;
            }
            drop(l);
            i = end;
        }
        let forbidden: Vec<u64> = self
            .sh
            .ledger
            .0
            .lock()
            .unwrap()
            .forbidden
            .iter()
            .copied()
            .collect();
        for k in forbidden {
            if main_counts.contains_key(&k) {
                self.sh.violate(
                    "conflict_resolved_silently",
                    format!(
                        "key hash {k:x} answered conflicts or was abandoned, yet it is in main"
                    ),
                );
            }
        }
        self.count("ack_checks", checked);
        Ok(())
    }

    fn check_tags(
        &mut self,
        refs: &BTreeMap<String, ChangesetId>,
        tags: &HashMap<String, ChangesetId>,
    ) {
        let deleted = self.sh.ledger.0.lock().unwrap().tags_deleted.clone();
        for (name, id) in tags {
            if deleted.contains(name) {
                continue;
            }
            match refs.get(name) {
                Some(t) if t == id => {}
                other => self
                    .sh
                    .violate("tag", format!("{name} should be {id} but is {other:?}")),
            }
        }
        self.count("tag_checks", tags.len() as u64);
    }

    // ---- sampled deep checks ----

    async fn sampled(&mut self, refs: &BTreeMap<String, ChangesetId>, last: bool) -> R<()> {
        let Some(main) = refs.get("main").copied() else {
            return Ok(());
        };
        let t = std::time::Instant::now();
        self.deep_reads(last).await?;
        self.phase("deep_reads", t);
        let t = std::time::Instant::now();
        self.path_log_window(main).await?;
        self.phase("path_log", t);
        let t = std::time::Instant::now();
        self.blame_notes(main).await?;
        self.phase("blame", t);
        let t = std::time::Instant::now();
        self.search_tokens().await?;
        self.phase("search", t);
        // Provenance walks thousands of changesets: once per minute is plenty.
        if last || self.last_provenance.elapsed() > std::time::Duration::from_secs(60) {
            let t = std::time::Instant::now();
            self.provenance(main).await?;
            self.phase("provenance", t);
            self.last_provenance = std::time::Instant::now();
        }
        Ok(())
    }

    async fn deep_reads(&mut self, last: bool) -> R<()> {
        if self.order.is_empty() {
            return Ok(());
        }
        let n = if last { 500 } else { 150 };
        let ids: Vec<ChangesetId> = (0..n)
            .map(|_| self.order[self.rng.below(self.order.len())])
            .collect();
        for (id, info) in self.fetch_many(ids).await? {
            let node = &self.nodes[&id];
            if info.changeset.parents != node.parents || info.generation != node.generation {
                self.sh.violate(
                    "deep_read",
                    format!("{id} reads differently than when it was walked"),
                );
            }
        }
        self.count("deep_reads", n as u64);
        // Whole trees of a few old changesets: every file's blob must exist.
        for _ in 0..2 {
            let id = self.order[self.rng.below(self.order.len())];
            let p = format!("/v1/files/{id}");
            let r: FilesResponse = self.api.get_ok("files", &p).await?;
            // A sample: GC checks and the final recheck cover every blob.
            let hashes: Vec<Hash> = (0..300)
                .filter_map(|_| r.files.get(self.rng.below(r.files.len().max(1))))
                .map(|f| f.content)
                .collect();
            self.expect_present(&hashes, &format!("tree of {id}"))
                .await?;
        }
        for _ in 0..8 {
            if self.blob_sample.is_empty() {
                break;
            }
            let h = self.blob_sample[self.rng.below(self.blob_sample.len())];
            let p = format!("/v1/blobs/{h}");
            let r = self.api.call("blobs.get", Method::GET, &p, None).await?;
            if r.status == 404 {
                self.sh
                    .violate("blob_lost", format!("blob {h} of a changeset is gone"));
            } else {
                let r = self.api.want("blobs.get", &p, r, &[200])?;
                if Hash::of_blob(&r.body) != h {
                    self.sh.violate(
                        "blob_corrupt",
                        format!("blob {h} reads back with other bytes"),
                    );
                }
            }
        }
        Ok(())
    }

    /// `POST /v1/blobs/missing` must come back empty for referenced blobs,
    /// whatever GC is doing: only unreferenced blobs are ever condemned.
    pub async fn expect_present(&mut self, hashes: &[Hash], what: &str) -> R<()> {
        for chunk in hashes.chunks(1000) {
            let q = serde_json::to_vec(&MissingBlobsRequest {
                hashes: chunk.to_vec(),
            })
            .unwrap();
            let r = self
                .api
                .call("blobs.missing", Method::POST, "/v1/blobs/missing", Some(&q))
                .await?;
            let r = self.api.want("blobs.missing", "", r, &[200])?;
            let m: MissingBlobsResponse = self.api.parsed("blobs.missing", "", &r)?;
            if !m.missing.is_empty() {
                self.sh.violate(
                    "blob_lost",
                    format!(
                        "{what}: {} referenced blobs are missing, first {}",
                        m.missing.len(),
                        m.missing[0]
                    ),
                );
            }
        }
        self.count("blobs_checked", hashes.len() as u64);
        Ok(())
    }

    /// After a GC: every ref's files and a sample of every blob any walked
    /// changeset ever referenced are still stored.
    pub async fn verify_reachable(&mut self) -> R<()> {
        let refs = self.refs().await?;
        // Branches share nearly every file with main: ask once per blob.
        let mut all: HashSet<Hash> = HashSet::new();
        for (_, id) in refs
            .iter()
            .filter(|(n, _)| !n.starts_with("tags/"))
            .take(12)
        {
            let p = format!("/v1/files/{id}");
            let r: FilesResponse = self.api.get_ok("files", &p).await?;
            all.extend(r.files.iter().map(|f| f.content));
        }
        let all: Vec<Hash> = all.into_iter().collect();
        self.expect_present(&all, "files at the heads of the refs")
            .await?;
        let sample: Vec<Hash> = self.blob_sample.iter().copied().take(5000).collect();
        self.expect_present(&sample, "blobs of walked changesets")
            .await?;
        self.count("gc_verifications", 1);
        Ok(())
    }

    /// Path history from the index must equal a scan of the newest changesets.
    async fn path_log_window(&mut self, main: ChangesetId) -> R<()> {
        let Some(h) = self.nodes.get(&main) else {
            return Ok(());
        };
        let cutoff = h.generation.saturating_sub(WINDOW);
        let window = self.closure(main, cutoff);
        let mut candidates: Vec<String> = Vec::new();
        let ids: Vec<ChangesetId> = window.iter().copied().collect();
        for _ in 0..3 {
            if let Some(id) = ids.get(self.rng.below(ids.len().max(1))) {
                if let Some(ps) = self.recent.get(id) {
                    if !ps.is_empty() {
                        candidates.push(ps[self.rng.below(ps.len())].clone());
                    }
                }
            }
        }
        candidates.push(crate::repo::src_path(
            self.rng.below(3),
            self.rng.below(100),
        ));
        for path in candidates {
            let want: HashSet<ChangesetId> = window
                .iter()
                .filter(|id| self.nodes[*id].parents.len() <= 1)
                .filter(|id| self.recent.get(id).is_some_and(|ps| ps.contains(&path)))
                .copied()
                .collect();
            let p = format!("/v1/log/{main}?path={path}&limit=10000");
            let r: LogResponse = self.api.get_ok("log", &p).await?;
            let got: HashSet<ChangesetId> = r
                .entries
                .iter()
                .filter(|e| e.generation > cutoff && e.changeset.parents.len() <= 1)
                .map(|e| e.id)
                .collect();
            // A changeset may "modify" a file to what it already was: it
            // lists the path, and the index rightly does not count it.
            let mut want = want;
            let absent: Vec<ChangesetId> = want.difference(&got).copied().take(8).collect();
            for id in absent {
                let Some(parent) = self.nodes[&id].parents.first().copied() else {
                    continue;
                };
                let dp = format!("/v1/diff?from={parent}&to={id}");
                let d: DiffResponse = self.api.get_ok("diff", &dp).await?;
                if !d.entries.iter().any(|e| e.path.as_str() == path) {
                    want.remove(&id);
                    self.count("path_log_noop_changes", 1);
                }
            }
            if want != got {
                let missing: Vec<_> = want.difference(&got).take(3).collect();
                let extra: Vec<_> = got.difference(&want).take(3).collect();
                self.sh.violate(
                    "path_log",
                    format!(
                        "log of {path} at {main} disagrees with a scan of the last {WINDOW} generations: \
                         {} expected, {} returned, missing {missing:?}, extra {extra:?}",
                        want.len(),
                        got.len()
                    ),
                );
            }
            // The newest change is the same answer from the other index path.
            if let Some(newest) = want.iter().max_by_key(|id| self.nodes[id].generation) {
                let lp = format!("/v1/last-change/{main}?path={path}");
                let lr: LastChangeResponse = self.api.get_ok("last-change", &lp).await?;
                let g = self.nodes[newest].generation;
                if lr.changeset.generation < g {
                    self.sh.violate(
                        "last_change",
                        format!(
                            "last change of {path} at {main} is generation {}, but {newest} at {g} changed it",
                            lr.changeset.generation
                        ),
                    );
                }
            }
            self.count("path_log_checks", 1);
        }
        Ok(())
    }

    /// Each token line of an agent's notes file is blamed on the changeset
    /// that wrote it.
    async fn blame_notes(&mut self, main: ChangesetId) -> R<()> {
        let agents = self.sh.args.agents.max(1);
        let label = format!("a{:02}", self.rng.below(agents));
        let t = self
            .api
            .call(
                "tree",
                Method::GET,
                &format!("/v1/tree/{main}?path=notes/{label}"),
                None,
            )
            .await?;
        if t.status == 404 {
            return Ok(());
        }
        let t = self.api.want("tree", "notes", t, &[200])?;
        let t: leep_core::api::TreeResponse = self.api.parsed("tree", "notes", &t)?;
        let names: Vec<&String> = t
            .entries
            .keys()
            .filter(|n| n.starts_with("main-"))
            .collect();
        if names.is_empty() {
            return Ok(());
        }
        let path = format!("notes/{label}/{}", names[self.rng.below(names.len())]);
        let Some(bytes) = self.api.file(main, &path).await? else {
            return Ok(());
        };
        let text = String::from_utf8_lossy(&bytes).into_owned();
        let p = format!("/v1/blame/{main}?path={path}");
        let r: BlameResponse = self.api.get_ok("blame", &p).await?;
        let lines: Vec<&str> = text.lines().collect();
        if r.lines.len() != lines.len() {
            self.sh.violate(
                "blame",
                format!(
                    "{path}: blame has {} lines, file {}",
                    r.lines.len(),
                    lines.len()
                ),
            );
            return Ok(());
        }
        for (line, bl) in lines.iter().zip(&r.lines) {
            let Some(key) = line
                .strip_prefix("<<SOAK:")
                .and_then(|s| s.split(">>").next())
            else {
                continue;
            };
            // Blame follows first parents (as documented), so a line that came
            // in through a merge belongs to the merge commit.
            let ok = r
                .changesets
                .get(&bl.changeset)
                .is_some_and(|c| c.message_first_line.starts_with(&format!("soak {key} ")))
                || self
                    .nodes
                    .get(&bl.changeset)
                    .is_some_and(|n| n.parents.len() > 1);
            if !ok {
                self.sh.violate(
                    "blame",
                    format!("{path}: line with {key} is blamed on {}", bl.changeset),
                );
            }
        }
        self.count("blame_checks", 1);
        Ok(())
    }

    /// A planted token is found exactly once in the changeset that landed
    /// it; one that was never planted is not found.
    async fn search_tokens(&mut self) -> R<()> {
        let picks: Vec<(String, ChangesetId, String)> = {
            let l = self.sh.ledger.0.lock().unwrap();
            let n = self.verified_upto.min(l.acks.len());
            if n == 0 {
                return Ok(());
            }
            (0..5)
                .map(|_| {
                    // Recent changesets: their parents' index rows exist, so
                    // the search costs a delta; old ones cost a whole tree.
                    let lo = if self.rng.pct(85) {
                        n.saturating_sub(150)
                    } else {
                        0
                    };
                    let a = &l.acks[lo + self.rng.below(n - lo)];
                    (
                        a.steps.last().unwrap().key.clone(),
                        a.landed,
                        a.agent.clone(),
                    )
                })
                .collect()
        };
        for (key, landed, agent) in picks {
            let tok = token_for(&key);
            let p = format!("/v1/search/{landed}?q={}&limit=10", urlenc(&tok));
            let r: SearchResponse = self.api.get_ok("search", &p).await?;
            let ok = r.matches.len() == 1
                && r.matches[0].line.contains(&tok)
                && (r.matches[0]
                    .path
                    .as_str()
                    .starts_with(&format!("notes/{agent}/"))
                    || r.matches[0]
                        .path
                        .as_str()
                        .starts_with(&format!("release/{agent}/")));
            if !ok {
                self.sh.violate(
                    "search_token",
                    format!(
                        "{tok} at {landed}: expected one match under notes/{agent}/, got {:?}",
                        r.matches
                            .iter()
                            .map(|m| m.path.to_string())
                            .collect::<Vec<_>>()
                    ),
                );
            }
            self.count("search_checks", 1);
        }
        let none = token_for(&format!("{}:zz:9999999", self.sh.run_id));
        let head = self.api.get_ref("main").await?;
        if let Some(head) = head {
            let p = format!("/v1/search/{head}?q={}&limit=10", urlenc(&none));
            let r: SearchResponse = self.api.get_ok("search", &p).await?;
            if !r.matches.is_empty() {
                self.sh.violate(
                    "search_token",
                    format!("{none} was never planted, yet found"),
                );
            }
        }
        Ok(())
    }

    async fn provenance(&mut self, main: ChangesetId) -> R<()> {
        let p = format!("/v1/provenance/{main}?limit=2000");
        let r: ProvenanceResponse = self.api.get_ok("provenance", &p).await?;
        for a in &r.agents {
            if !MODELS.contains(&a.model.as_str()) {
                self.sh
                    .violate("provenance", format!("unknown agent model {:?}", a.model));
            }
        }
        self.count("provenance_checks", 1);
        Ok(())
    }

    // ---- the ref log ----

    /// The log replays to the refs: for each ref the entries chain (each
    /// `old` is the previous `new`), and the newest `new` is what the ref
    /// holds. Periodic checks read the newest 3000 entries; the last one
    /// reads them all.
    async fn check_reflog(
        &mut self,
        last: bool,
        refs_before: &BTreeMap<String, ChangesetId>,
    ) -> R<()> {
        let mut entries = Vec::new();
        let mut before: Option<u64> = None;
        let max_pages = if last { usize::MAX } else { 3 };
        for _ in 0..max_pages {
            let p = match before {
                Some(b) => format!("/v1/reflog?limit=1000&before={b}"),
                None => "/v1/reflog?limit=1000".to_string(),
            };
            let page: RefLogResponse = self.api.get_ok("reflog", &p).await?;
            let Some(oldest) = page.entries.last().map(|e| e.seq) else {
                break;
            };
            entries.extend(page.entries);
            before = Some(oldest);
        }
        let complete = last || entries.len() < 3000;
        if entries.windows(2).any(|w| w[0].seq <= w[1].seq) {
            self.sh
                .violate("reflog", "sequence numbers do not strictly decrease");
        }
        let mut chain: HashMap<&str, Option<ChangesetId>> = HashMap::new();
        let mut seen: HashMap<&str, ChangesetId> = HashMap::new();
        for e in &entries {
            if let Some(expect_new) = chain.get(e.name.as_str()) {
                if *expect_new != e.new {
                    self.sh.violate(
                        "reflog",
                        format!(
                            "{}: entry {} ends at {:?}, but the next move starts from it as {:?}",
                            e.name, e.seq, e.new, expect_new
                        ),
                    );
                }
            }
            chain.insert(e.name.as_str(), e.old);
            if let Some(n) = e.new {
                seen.entry(e.name.as_str()).or_insert(n);
            }
        }
        if complete {
            for (name, old) in &chain {
                if old.is_some() {
                    self.sh.violate(
                        "reflog",
                        format!("{name}: the oldest entry does not start from nothing"),
                    );
                }
            }
        }
        // A ref read before the log was read has since moved at most forward:
        // its value is the `new` of some entry of that ref.
        if complete {
            let have: HashSet<(&str, ChangesetId)> = entries
                .iter()
                .filter_map(|e| e.new.map(|n| (e.name.as_str(), n)))
                .collect();
            for (name, id) in refs_before {
                if !have.contains(&(name.as_str(), *id)) {
                    self.sh.violate(
                        "reflog",
                        format!("{name} points at {id}, which no log entry moved it to"),
                    );
                }
            }
        }
        if last {
            // Nothing moves any more: the newest entry is the ref, exactly.
            let refs = self.refs().await?;
            for (name, id) in &refs {
                if seen.get(name.as_str()) != Some(id) {
                    self.sh.violate(
                        "reflog",
                        format!(
                            "{name} is {id} but the log ends at {:?}",
                            seen.get(name.as_str())
                        ),
                    );
                }
            }
            for (name, new) in &chain_newest(&entries) {
                if new.is_some() != refs.contains_key(*name) {
                    self.sh.violate(
                        "reflog",
                        format!("{name}: the log says {new:?}, the ref list disagrees"),
                    );
                }
            }
            // A clean acknowledgement names a head the ref really had.
            let moved: HashSet<ChangesetId> = entries
                .iter()
                .filter(|e| e.name == "main")
                .filter_map(|e| e.new)
                .collect();
            let l = self.sh.ledger.0.lock().unwrap();
            let mut missing = 0;
            for a in &l.acks {
                if a.clean
                    && matches!(&a.place, Where::Ref(r) if r == "main")
                    && !moved.contains(&a.landed)
                {
                    missing += 1;
                }
            }
            drop(l);
            if missing > 0 {
                self.sh.violate(
                    "reflog",
                    format!("{missing} acknowledged lands on main are not in the ref log"),
                );
            }
        }
        self.count("reflog_entries_checked", entries.len() as u64);
        Ok(())
    }

    /// The last check: reread every changeset (all of them up to a cap) and
    /// confirm every blob any of them names.
    async fn full_recheck(&mut self) -> R<()> {
        let cap = 400_000usize;
        let step = (self.order.len() / cap).max(1);
        let ids: Vec<ChangesetId> = self.order.iter().copied().step_by(step).collect();
        let mut done = 0u64;
        for chunk in ids.chunks(512) {
            let got = self.fetch_many(chunk.to_vec()).await?;
            let mut hashes = HashSet::new();
            for (id, info) in &got {
                let node = &self.nodes[id];
                if info.changeset.parents != node.parents || info.generation != node.generation {
                    self.sh
                        .violate("deep_read", format!("{id} reads differently at the end"));
                }
                for c in info.changeset.changes.values() {
                    if let FileChange::Modify { content, .. } = c {
                        hashes.insert(*content);
                    }
                }
            }
            let hashes: Vec<Hash> = hashes.into_iter().collect();
            self.expect_present(&hashes, "final recheck").await?;
            done += got.len() as u64;
            if self.sh.stopped() && self.sh.violation_count() > 0 {
                break;
            }
        }
        self.count("full_recheck_changesets", done);
        Ok(())
    }
}

fn chain_newest(entries: &[leep_core::RefLogEntry]) -> BTreeMap<&str, Option<ChangesetId>> {
    let mut m = BTreeMap::new();
    for e in entries {
        m.entry(e.name.as_str()).or_insert(e.new);
    }
    m
}
