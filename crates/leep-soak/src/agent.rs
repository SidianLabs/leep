//! A simulated agent that drives the HTTP API: reads, edits, commits, signs
//! and lands, on main mostly and on its own feature branches sometimes.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use ed25519_dalek::{Signer, SigningKey};
use leep_core::api::{
    AddReviewRequest, AddSignatureRequest, BlameResponse, DiffResponse, LastChangeResponse,
    LogResponse, SearchResponse, SignaturesResponse,
};
use leep_core::review::{Verdict, NEEDS_REVIEW_MARKER};
use leep_core::signature::{format_public_key, signing_message, NOT_SIGNED_MARKER};
use leep_core::{Changeset, ChangesetId, FileChange, FileKind, Hash, RepoPath, TreeEntry};
use reqwest::Method;

use crate::api::{Api, Fail, Landed, R};
use crate::repo::{self, Layout};
use crate::shared::{token_for, Ack, Shared, Step, Where};
use crate::util::Rng;

pub const MODELS: [&str; 3] = ["model-alpha", "model-beta", "model-gamma"];
const NOTES_PER_PART: usize = 40;

pub fn urlenc(s: &str) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || b"-_.~/".contains(&b) {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

pub struct Env {
    pub sh: Arc<Shared>,
    pub layout: Layout,
    pub signers: Arc<Vec<SigningKey>>,
    /// Reviewer API handle, present when the release flow runs.
    pub reviewer: Option<Api>,
}

/// What an agent knows about the notes files it owns on one branch lineage.
#[derive(Clone, Default)]
struct Notes {
    part: usize,
    lines: Vec<String>,
}

struct Feature {
    name: String,
    lineage: String,
    lands: u32,
    target: u32,
}

/// What an agent can roll back when a land is refused for conflicts.
#[derive(Clone)]
struct Snapshot {
    notes: Notes,
    own: BTreeMap<String, Vec<u8>>,
}

pub struct Agent {
    env: Arc<Env>,
    sh: Arc<Shared>,
    api: Api,
    idx: usize,
    label: String,
    rng: Rng,
    seq: u64,
    notes: HashMap<String, Notes>,
    /// Files this agent added to main and may delete or rename.
    own: BTreeMap<String, Vec<u8>>,
    feature: Option<Feature>,
    features_made: u32,
    tags_made: u32,
    own_tags: Vec<(String, ChangesetId)>,
    last_main: Option<ChangesetId>,
    release_n: u32,
}

struct Built {
    cs: Changeset,
    step: Step,
    notes_path: String,
    renamed: Option<(String, String)>,
}

impl Agent {
    pub fn new(env: Arc<Env>, idx: usize, api: Api) -> Agent {
        let sh = env.sh.clone();
        Agent {
            label: format!("a{idx:02}"),
            rng: Rng::new(sh.args.seed ^ ((idx as u64 + 1) * 0x9E37_79B9)),
            env,
            sh,
            api,
            idx,
            seq: 0,
            notes: HashMap::new(),
            own: BTreeMap::new(),
            feature: None,
            features_made: 0,
            tags_made: 0,
            own_tags: Vec::new(),
            last_main: None,
            release_n: 0,
        }
    }

    pub async fn run(mut self) {
        // Stagger the start so fifty agents do not open with the same request.
        let jitter = self.rng.think(self.sh.args.think);
        let _ = tokio::time::sleep(jitter).await;
        while !self.sh.stopped() {
            match self.iteration().await {
                Ok(()) | Err(Fail::Bad) => {}
                Err(Fail::Stop | Fail::Stall) => break,
            }
            self.sh.beat(self.idx);
            let t = self.rng.think(self.sh.args.think);
            let end = std::time::Instant::now() + t;
            while std::time::Instant::now() < end && !self.sh.stopped() {
                tokio::time::sleep(Duration::from_millis(50).min(t)).await;
            }
        }
    }

    fn next_key(&mut self) -> String {
        self.seq += 1;
        format!("{}:{}:{:07}", self.sh.run_id, self.label, self.seq)
    }

    async fn iteration(&mut self) -> R<()> {
        // An agent that has a branch keeps working on it for a few iterations
        // and then merges or drops it: branches live for minutes, not hours,
        // and a land of one is as far behind main as the lands since it began.
        if self.feature.is_some() && self.rng.pct(75) {
            return self.feature_step().await;
        }
        let roll = self.rng.below(100);
        let release_on = self.env.reviewer.is_some();
        match roll {
            0..=11 => self.read_session("main").await,
            12..=73 => {
                self.read_phase("main").await?;
                self.change_and_land("main", "main").await
            }
            74..=79 => self.feature_step().await,
            80..=83 => self.tag_step().await,
            84..=86 => self.orphan_upload().await,
            87..=91 if release_on => self.release_flow().await,
            _ => {
                self.read_phase("main").await?;
                self.change_and_land("main", "main").await
            }
        }
    }

    // ---- reads ----

    async fn read_phase(&mut self, name: &str) -> R<()> {
        for _ in 0..1 + self.rng.below(3) {
            self.read_one(name).await?;
        }
        Ok(())
    }

    async fn read_session(&mut self, name: &str) -> R<()> {
        for _ in 0..2 + self.rng.below(4) {
            self.read_one(name).await?;
        }
        Ok(())
    }

    fn some_src(&mut self) -> (usize, usize) {
        (
            self.rng.below(self.env.layout.dirs),
            self.rng.below(repo::FILES_PER_DIR),
        )
    }

    async fn read_one(&mut self, name: &str) -> R<()> {
        let Some(head) = self.api.get_ref(name).await? else {
            return Ok(());
        };
        match self.rng.below(8) {
            0 => {
                // cat: the bytes must hash to what the directory listing says.
                let (d, _) = self.some_src();
                let dir = repo::src_dir(d);
                let t = self.api.tree(head, &dir).await?;
                let names: Vec<_> = t.entries.iter().collect();
                if names.is_empty() {
                    return Ok(());
                }
                let (n, e) = names[self.rng.below(names.len())];
                if let TreeEntry::File { content, size, .. } = e {
                    let path = format!("{dir}/{n}");
                    match self.api.file(head, &path).await? {
                        Some(b) => {
                            if Hash::of_blob(&b) != *content || b.len() as u64 != *size {
                                self.sh.violate(
                                    "read_integrity",
                                    format!("{path} at {head}: bytes do not match the tree entry"),
                                );
                            }
                        }
                        None => self.sh.violate(
                            "read_integrity",
                            format!("{path} listed at {head} but not readable"),
                        ),
                    }
                }
            }
            1 => {
                let t = self.api.tree(head, "").await?;
                if !t.entries.contains_key("src") {
                    self.sh
                        .violate("read_integrity", format!("root of {head} has no src"));
                }
            }
            2 => {
                let (d, i) = self.some_src();
                let q = format!("handler_{d:02}_{i:03}_");
                let p = format!("/v1/search/{head}?q={}&limit=20", urlenc(&q));
                let r: SearchResponse = self.api.get_ok("search", &p).await?;
                for m in &r.matches {
                    if !m.line.contains(&q) {
                        self.sh.violate(
                            "search",
                            format!(
                                "{} line {} does not contain {q:?}: {:?}",
                                m.path, m.line_number, m.line
                            ),
                        );
                    }
                }
            }
            3 => {
                let (d, i) = self.some_src();
                let p = format!("/v1/log/{head}?path={}&limit=20", repo::src_path(d, i));
                let r: LogResponse = self.api.get_ok("log", &p).await?;
                let mut seen = std::collections::HashSet::new();
                for e in &r.entries {
                    if !seen.insert(e.id) {
                        self.sh
                            .violate("log", format!("{p}: {} listed twice", e.id));
                    }
                }
            }
            4 => {
                // blame: one entry per line of the file, text equal to it.
                let (d, i) = self.some_src();
                let path = repo::src_path(d, i);
                let Some(bytes) = self.api.file(head, &path).await? else {
                    return Ok(());
                };
                let p = format!("/v1/blame/{head}?path={path}");
                let r: BlameResponse = self.api.get_ok("blame", &p).await?;
                let text = String::from_utf8_lossy(&bytes);
                let want: Vec<&str> = text.lines().collect();
                let got: Vec<&str> = r.lines.iter().map(|l| l.text.as_str()).collect();
                if want != got {
                    self.sh.violate(
                        "blame",
                        format!(
                            "{path} at {head}: blame has {} lines, file has {}",
                            got.len(),
                            want.len()
                        ),
                    );
                }
            }
            5 => {
                let (d, i) = self.some_src();
                let path = repo::src_path(d, i);
                let p = format!("/v1/last-change/{head}?path={path}");
                let r = self.api.call("last-change", Method::GET, &p, None).await?;
                if r.status != 404 {
                    let r = self.api.want("last-change", &p, r, &[200])?;
                    let _: LastChangeResponse = self.api.parsed("last-change", &p, &r)?;
                }
            }
            6 => {
                let a = self
                    .api
                    .get_ok::<leep_core::api::RefsResponse>("refs", "/v1/refs")
                    .await?;
                if !a.refs.contains_key("main") {
                    self.sh.violate("refs", "main vanished from the ref list");
                }
            }
            _ => {
                let i = self.rng.below(self.env.layout.docs);
                let path = repo::doc_path(i);
                let _ = self.api.file(head, &path).await?;
            }
        }
        Ok(())
    }

    // ---- changes ----

    fn snapshot(&self, lineage: &str) -> Snapshot {
        Snapshot {
            notes: self.notes.get(lineage).cloned().unwrap_or_default(),
            own: self.own.clone(),
        }
    }

    fn restore(&mut self, lineage: &str, s: Snapshot) {
        self.notes.insert(lineage.to_string(), s.notes);
        self.own = s.own;
    }

    async fn read_at(
        &self,
        head: ChangesetId,
        overlay: &HashMap<String, Option<Vec<u8>>>,
        path: &str,
    ) -> R<Option<Vec<u8>>> {
        if let Some(v) = overlay.get(path) {
            return Ok(v.clone());
        }
        self.api.file(head, path).await
    }

    /// Build a stack of one to three changesets on `head`.
    async fn build_stack(
        &mut self,
        head: ChangesetId,
        lineage: &str,
        on_main: bool,
    ) -> R<(Vec<Built>, Vec<(Hash, Vec<u8>)>)> {
        let n = match self.rng.below(10) {
            0..=6 => 1,
            7..=8 => 2,
            _ => 3,
        };
        let mut overlay: HashMap<String, Option<Vec<u8>>> = HashMap::new();
        let mut parent = None::<ChangesetId>;
        let mut blobs = Vec::new();
        let mut out: Vec<Built> = Vec::new();
        for _ in 0..n {
            let key = self.next_key();
            let mut changes: BTreeMap<String, Option<(Vec<u8>, FileKind)>> = BTreeMap::new();
            let mut desc = Vec::new();
            let mut renamed = None;
            for _ in 0..1 + self.rng.below(3) {
                let roll = self.rng.below(100);
                match roll {
                    0..=57 => {
                        let path = if self.rng.pct(12) {
                            self.rng.pick(&repo::hot_files()).clone()
                        } else {
                            let (d, i) = self.some_src();
                            repo::src_path(d, i)
                        };
                        if changes.contains_key(&path) {
                            continue;
                        }
                        if let Some(old) = self.read_at(head, &overlay, &path).await? {
                            let new =
                                repo::edit_text(&mut self.rng, &String::from_utf8_lossy(&old));
                            overlay.insert(path.clone(), Some(new.clone().into_bytes()));
                            changes
                                .insert(path.clone(), Some((new.into_bytes(), FileKind::Regular)));
                            desc.push(format!("edit {path}"));
                        }
                    }
                    58..=66 => {
                        let path = repo::doc_path(self.rng.below(self.env.layout.docs));
                        if changes.contains_key(&path) {
                            continue;
                        }
                        if let Some(old) = self.read_at(head, &overlay, &path).await? {
                            let mut t = String::from_utf8_lossy(&old).into_owned();
                            t.push_str(&format!("Note {}.\n", self.rng.below(100_000)));
                            overlay.insert(path.clone(), Some(t.clone().into_bytes()));
                            changes.insert(path.clone(), Some((t.into_bytes(), FileKind::Regular)));
                            desc.push(format!("docs {path}"));
                        }
                    }
                    67..=71 => {
                        let path = repo::asset_path(self.rng.below(self.env.layout.assets));
                        if changes.contains_key(&path) {
                            continue;
                        }
                        let b = repo::asset_bytes(&mut self.rng);
                        overlay.insert(path.clone(), Some(b.clone()));
                        changes.insert(path.clone(), Some((b, FileKind::Regular)));
                        desc.push(format!("asset {path}"));
                    }
                    72..=81 if on_main => {
                        let d = self.rng.below(self.env.layout.dirs);
                        let path = format!(
                            "{}/new_{}_{}.rs",
                            repo::src_dir(d),
                            self.label,
                            self.seq * 10 + changes.len() as u64
                        );
                        let t = repo::new_src_text(&mut self.rng, &self.label, self.seq as usize);
                        let kind = if self.rng.pct(5) {
                            FileKind::Executable
                        } else {
                            FileKind::Regular
                        };
                        self.own.insert(path.clone(), t.clone().into_bytes());
                        overlay.insert(path.clone(), Some(t.clone().into_bytes()));
                        changes.insert(path.clone(), Some((t.into_bytes(), kind)));
                        desc.push(format!("add {path}"));
                    }
                    82..=90 if on_main && !self.own.is_empty() => {
                        let keys: Vec<_> = self.own.keys().cloned().collect();
                        let path = self.rng.pick(&keys).clone();
                        if changes.contains_key(&path) {
                            continue;
                        }
                        self.own.remove(&path);
                        overlay.insert(path.clone(), None);
                        changes.insert(path.clone(), None);
                        desc.push(format!("delete {path}"));
                    }
                    91..=99 if on_main && !self.own.is_empty() && renamed.is_none() => {
                        let keys: Vec<_> = self.own.keys().cloned().collect();
                        let from = self.rng.pick(&keys).clone();
                        let to = format!("{}_r{}.rs", from.trim_end_matches(".rs"), self.seq);
                        if changes.contains_key(&from) || changes.contains_key(&to) {
                            continue;
                        }
                        let data = self.own.remove(&from).unwrap();
                        self.own.insert(to.clone(), data.clone());
                        overlay.insert(from.clone(), None);
                        overlay.insert(to.clone(), Some(data.clone()));
                        changes.insert(from.clone(), None);
                        changes.insert(to.clone(), Some((data, FileKind::Regular)));
                        desc.push(format!("rename {from} to {to}"));
                        renamed = Some((from, to));
                    }
                    _ => {}
                }
            }
            // The token that search, blame and path history are checked with.
            let token = token_for(&key);
            let notes = self.notes.entry(lineage.to_string()).or_default();
            if notes.lines.len() >= NOTES_PER_PART {
                notes.part += 1;
                notes.lines.clear();
            }
            notes
                .lines
                .push(format!("{token} {} step {}", self.label, self.seq));
            let notes_path = format!("notes/{}/{lineage}-{:04}.txt", self.label, notes.part);
            let mut body = notes.lines.join("\n");
            body.push('\n');
            overlay.insert(notes_path.clone(), Some(body.clone().into_bytes()));
            changes.insert(
                notes_path.clone(),
                Some((body.into_bytes(), FileKind::Regular)),
            );
            if desc.is_empty() {
                desc.push("notes".into());
            }

            let mut tree_changes = BTreeMap::new();
            let mut step_changes = Vec::new();
            for (p, c) in changes {
                let (fc, h) = match c {
                    Some((data, kind)) => {
                        let h = Hash::of_blob(&data);
                        let fc = repo::change_of(&data, kind);
                        blobs.push((h, data));
                        (fc, Some(h))
                    }
                    None => (FileChange::Delete, None),
                };
                tree_changes.insert(RepoPath::new(p.as_str()).unwrap(), fc);
                step_changes.push((p, h));
            }
            let mut extra = BTreeMap::new();
            extra.insert(
                "agent.model".into(),
                MODELS[self.idx % MODELS.len()].to_string(),
            );
            extra.insert(
                "agent.session".into(),
                format!("{}-{}-{}", self.sh.run_id, self.label, self.seq / 20),
            );
            extra.insert("agent.prompt".into(), format!("soak task {}", self.seq));
            if self.rng.pct(50) {
                extra.insert("agent.tests".into(), "unit tests passed".into());
            }
            let cs = Changeset {
                parents: parent
                    .into_iter()
                    .chain(parent.is_none().then_some(head))
                    .collect(),
                author: format!("soak-agent-{}", self.label),
                timestamp: 1_700_100_000 + self.seq as i64,
                message: format!("soak {key} {}", desc.join("; ")),
                changes: tree_changes,
                extra,
            };
            // The id of this changeset is the next one's parent.
            parent = Some(cs.id());
            out.push(Built {
                cs,
                step: Step {
                    key,
                    changes: step_changes,
                },
                notes_path,
                renamed,
            });
        }
        Ok((out, blobs))
    }

    async fn create_stack(
        &mut self,
        built: &[Built],
        blobs: &[(Hash, Vec<u8>)],
    ) -> R<Vec<ChangesetId>> {
        'again: loop {
            self.api.upload(blobs).await?;
            let mut ids = Vec::new();
            for b in built {
                match self.api.create_changeset(&b.cs).await? {
                    Ok(id) => {
                        debug_assert_eq!(id, b.cs.id());
                        ids.push(id);
                    }
                    // GC condemned a blob between the upload check and now.
                    Err(_) => continue 'again,
                }
            }
            return Ok(ids);
        }
    }

    async fn sign(&self, id: ChangesetId) -> R<()> {
        let key = &self.env.signers[self.idx % self.env.signers.len()];
        let sig = key.sign(&signing_message(&id));
        let body = serde_json::to_vec(&AddSignatureRequest {
            public_key: format_public_key(key.verifying_key().as_bytes()),
            signature: hex::encode(sig.to_bytes()),
        })
        .unwrap();
        let path = format!("/v1/changesets/{id}/signatures");
        let r = self
            .api
            .call("sign", Method::POST, &path, Some(&body))
            .await?;
        self.api.want("sign", &path, r, &[200])?;
        self.sh.stats.inc("signatures_added");
        if self.rng.clone().pct(10) {
            let p: SignaturesResponse = self.api.get_ok("signatures", &path).await?;
            if !p.verified || p.signatures.is_empty() {
                self.sh.violate(
                    "signature",
                    format!("{id}: signature not listed or not verified"),
                );
            }
        }
        Ok(())
    }

    /// Commit a stack on `target`'s head and land it, redoing the work on a
    /// fresh head after a conflict (the server never resolves one itself).
    async fn change_and_land(&mut self, target: &str, lineage: &str) -> R<()> {
        let started = std::time::Instant::now();
        for attempt in 0..5 {
            let snap = self.snapshot(lineage);
            match self.one_attempt(target, lineage, started).await {
                Ok(true) => return Ok(()),
                // Conflicts: a person would look at the other side; the redo
                // does that by starting again from the new head.
                Ok(false) => {
                    self.restore(lineage, snap);
                    if attempt == 4 {
                        self.sh.stats.inc("land_abandoned");
                    }
                }
                // Whatever the stack held never landed: forget its notes lines
                // and files, or the next stack would carry them without a
                // changeset of its own.
                Err(e) => {
                    self.restore(lineage, snap);
                    return Err(e);
                }
            }
        }
        Ok(())
    }

    /// Build, create and land one stack. `Ok(false)` means it conflicted.
    async fn one_attempt(
        &mut self,
        target: &str,
        lineage: &str,
        started: std::time::Instant,
    ) -> R<bool> {
        let on_main = target == "main";
        let Some(head) = self.api.get_ref(target).await? else {
            return Ok(true);
        };
        let (built, blobs) = self.build_stack(head, lineage, on_main).await?;
        let ids = self.create_stack(&built, &blobs).await?;
        if self.rng.pct(15) && !self.sh.args.no_policy && self.env.reviewer.is_some() {
            for id in &ids {
                self.sign(*id).await?;
            }
        }
        let top = *ids.last().unwrap();
        match self.api.land(top, target).await? {
            Landed::Landed { id, rebased, clean } => {
                self.sh.stats.inc(if rebased {
                    "lands_rebased"
                } else {
                    "lands_fast_forward"
                });
                self.sh
                    .stats
                    .0
                    .lock()
                    .unwrap()
                    .land_e2e
                    .push(started.elapsed().as_micros().min(u128::from(u32::MAX)) as u32);
                self.sh.ledger.ack(Ack {
                    agent: self.label.clone(),
                    landed: id,
                    steps: built.iter().map(|b| b.step.clone()).collect(),
                    clean,
                    rebased,
                    place: Where::Ref(target.to_string()),
                    place_ver: 0,
                    verified: false,
                });
                if on_main {
                    self.last_main = Some(id);
                }
                // The land happened whatever these spot checks meet, so their
                // failures (already recorded) must not roll the model back.
                let _ = self.after_land(id, &built, rebased).await;
                Ok(true)
            }
            Landed::Conflicts => {
                self.sh.stats.inc("land_conflicts");
                self.sh
                    .ledger
                    .forbid(&built.iter().map(|b| b.step.key.clone()).collect::<Vec<_>>());
                Ok(false)
            }
            Landed::Rejected(msg) => {
                self.sh.violate("land_rejected", format!("{target}: {msg}"));
                Err(Fail::Bad)
            }
        }
    }

    /// Spot checks of what was just acknowledged.
    async fn after_land(&mut self, landed: ChangesetId, built: &[Built], rebased: bool) -> R<()> {
        let top = built.last().unwrap();
        if self.rng.pct(8) {
            let tok = token_for(&top.step.key);
            let p = format!("/v1/search/{landed}?q={}&limit=10", urlenc(&tok));
            let r: SearchResponse = self.api.get_ok("search", &p).await?;
            let ok = r.matches.len() == 1
                && r.matches[0].path.as_str() == top.notes_path
                && r.matches[0].line.contains(&tok);
            if !ok {
                self.sh.violate(
                    "search_token",
                    format!(
                        "{tok} at {landed}: expected one match in {}, got {:?}",
                        top.notes_path,
                        r.matches
                            .iter()
                            .map(|m| m.path.to_string())
                            .collect::<Vec<_>>()
                    ),
                );
            }
        }
        if let Some((from, to)) = top
            .renamed
            .as_ref()
            .filter(|_| !rebased || built.len() == 1)
        {
            let info = self.api.changeset(landed).await?;
            if let Some(parent) = info.changeset.parents.first() {
                let p = format!("/v1/diff?from={parent}&to={landed}&renames=true");
                let d: DiffResponse = self.api.get_ok("diff", &p).await?;
                let found = d
                    .renames
                    .iter()
                    .any(|r| r.from.as_str() == from && r.to.as_str() == to);
                // An unchanged file moved by delete and add is an exact rename.
                if !found {
                    self.sh.violate(
                        "rename_detection",
                        format!("{from} -> {to} not detected in {parent}..{landed}"),
                    );
                }
            }
        }
        Ok(())
    }

    // ---- branches, tags, garbage ----

    async fn feature_step(&mut self) -> R<()> {
        let Some(f) = self.feature.as_ref() else {
            return self.start_feature().await;
        };
        if f.lands >= f.target {
            return self.finish_feature().await;
        }
        let (name, lineage) = (f.name.clone(), f.lineage.clone());
        self.read_phase(&name).await?;
        self.change_and_land(&name, &lineage).await?;
        if let Some(f) = self.feature.as_mut() {
            f.lands += 1;
        }
        Ok(())
    }

    async fn start_feature(&mut self) -> R<()> {
        let Some(head) = self.api.get_ref("main").await? else {
            return Ok(());
        };
        self.features_made += 1;
        let name = format!("feature/{}-{}", self.label, self.features_made);
        let gen = self.api.changeset(head).await?.generation;
        self.sh
            .ledger
            .0
            .lock()
            .unwrap()
            .branch_base
            .insert(name.clone(), gen);
        if !self.api.cas_ref(&name, None, Some(head)).await? {
            self.sh
                .violate("branch", format!("could not create {name}"));
            return Err(Fail::Bad);
        }
        self.sh.stats.inc("branches_created");
        self.feature = Some(Feature {
            name,
            lineage: format!("fb{}", self.features_made),
            lands: 0,
            target: 1 + self.rng.below(3) as u32,
        });
        Ok(())
    }

    async fn finish_feature(&mut self) -> R<()> {
        let f = self.feature.take().unwrap();
        let Some(head) = self.api.get_ref(&f.name).await? else {
            return Ok(());
        };
        match self.rng.below(100) {
            0..=59 => match self.api.land(head, "main").await? {
                Landed::Landed { .. } => {
                    self.sh.stats.inc("branches_merged");
                    // Marked first: a checker that sees the branch gone must
                    // already see where its commits live.
                    self.sh.ledger.merge_branch(&f.name);
                    if !self.api.cas_ref(&f.name, Some(head), None).await? {
                        self.sh
                            .violate("branch", format!("could not delete {}", f.name));
                    }
                }
                Landed::Conflicts => {
                    self.sh.stats.inc("branch_merge_conflicts");
                    self.retire(&f.name, head).await?;
                }
                Landed::Rejected(m) => {
                    self.sh.violate("land_rejected", format!("main: {m}"));
                    return Err(Fail::Bad);
                }
            },
            60..=84 => self.retire(&f.name, head).await?,
            _ => {}
        }
        Ok(())
    }

    async fn retire(&mut self, name: &str, head: ChangesetId) -> R<()> {
        self.sh.ledger.retire_branch(name);
        self.sh.stats.inc("branches_retired");
        if !self.api.cas_ref(name, Some(head), None).await? {
            self.sh
                .violate("branch", format!("could not delete {name}"));
        }
        Ok(())
    }

    async fn tag_step(&mut self) -> R<()> {
        if !self.own_tags.is_empty() && self.rng.pct(40) {
            let i = self.rng.below(self.own_tags.len());
            let (name, id) = self.own_tags.remove(i);
            self.sh
                .ledger
                .0
                .lock()
                .unwrap()
                .tags_deleted
                .insert(name.clone());
            if !self.api.cas_ref(&name, Some(id), None).await? {
                self.sh.violate("tag", format!("could not delete {name}"));
            }
            return Ok(());
        }
        let Some(id) = self.last_main else {
            return Ok(());
        };
        self.tags_made += 1;
        let name = format!("tags/soak-{}-{}", self.label, self.tags_made);
        if !self.api.cas_ref(&name, None, Some(id)).await? {
            self.sh.violate("tag", format!("could not create {name}"));
            return Err(Fail::Bad);
        }
        self.sh
            .ledger
            .0
            .lock()
            .unwrap()
            .tags
            .insert(name.clone(), id);
        self.own_tags.push((name, id));
        self.sh.stats.inc("tags_created");
        Ok(())
    }

    /// Blobs nobody commits: what GC exists to reclaim.
    async fn orphan_upload(&mut self) -> R<()> {
        let blobs: Vec<(Hash, Vec<u8>)> = (0..1 + self.rng.below(3))
            .map(|_| {
                let n = 512 + self.rng.below(4096);
                let b = self.rng.bytes(n);
                (Hash::of_blob(&b), b)
            })
            .collect();
        self.api.upload(&blobs).await?;
        self.sh.stats.add("orphan_blobs", blobs.len() as u64);
        Ok(())
    }

    // ---- release: signed and reviewed ----

    async fn release_flow(&mut self) -> R<()> {
        let Some(reviewer) = self.env.reviewer.clone() else {
            return Ok(());
        };
        for _ in 0..4 {
            let Some(head) = self.api.get_ref("release").await? else {
                return Ok(());
            };
            self.release_n += 1;
            let key = self.next_key();
            let path = format!("release/{}/note-{:04}.md", self.label, self.release_n);
            let body = format!(
                "# Release note\n\n{} says: {}\n",
                self.label,
                token_for(&key)
            )
            .into_bytes();
            let h = Hash::of_blob(&body);
            let cs = Changeset {
                parents: vec![head],
                author: format!("soak-agent-{}", self.label),
                timestamp: 1_700_200_000 + self.seq as i64,
                message: format!("soak {key} release note"),
                changes: BTreeMap::from([(
                    RepoPath::new(path.as_str()).unwrap(),
                    repo::change_of(&body, FileKind::Regular),
                )]),
                extra: BTreeMap::new(),
            };
            let blobs = vec![(h, body)];
            self.api.upload(&blobs).await?;
            let id = match self.api.create_changeset(&cs).await? {
                Ok(id) => id,
                Err(_) => continue,
            };
            // Unsigned and unreviewed first, as a careless agent would.
            if self.rng.pct(50) {
                match self.api.land(id, "release").await? {
                    Landed::Rejected(m)
                        if m.contains(NOT_SIGNED_MARKER) || m.contains(NEEDS_REVIEW_MARKER) =>
                    {
                        self.sh.stats.inc("policy_refusals");
                    }
                    other => {
                        self.sh.violate(
                            "policy",
                            format!("an unsigned, unreviewed land on release was not refused: {other:?}"),
                        );
                        return Err(Fail::Bad);
                    }
                }
            }
            self.sign(id).await?;
            if self.rng.pct(50) {
                match self.api.land(id, "release").await? {
                    Landed::Rejected(m) if m.contains(NEEDS_REVIEW_MARKER) => {
                        self.sh.stats.inc("policy_refusals");
                    }
                    other => {
                        self.sh.violate(
                            "policy",
                            format!("an unreviewed land on release was not refused: {other:?}"),
                        );
                        return Err(Fail::Bad);
                    }
                }
            }
            let rp = format!("/v1/changesets/{id}/reviews");
            let rb = serde_json::to_vec(&AddReviewRequest {
                verdict: Verdict::Approve,
                comment: "looks fine".into(),
            })
            .unwrap();
            let r = reviewer
                .call("review", Method::POST, &rp, Some(&rb))
                .await?;
            reviewer.want("review", &rp, r, &[200])?;
            match self.api.land(id, "release").await? {
                Landed::Landed {
                    id: landed,
                    clean,
                    rebased,
                } => {
                    self.sh.stats.inc("release_lands");
                    self.sh.ledger.ack(Ack {
                        agent: self.label.clone(),
                        landed,
                        steps: vec![Step {
                            key,
                            changes: vec![(path, Some(h))],
                        }],
                        clean,
                        rebased,
                        place: Where::Ref("release".into()),
                        place_ver: 0,
                        verified: false,
                    });
                    return Ok(());
                }
                Landed::Conflicts => {
                    self.sh
                        .violate("release", "disjoint release notes conflicted");
                    return Err(Fail::Bad);
                }
                Landed::Rejected(m) => {
                    self.sh.violate(
                        "land_rejected",
                        format!("a signed, reviewed release note: {m}"),
                    );
                    return Err(Fail::Bad);
                }
            }
        }
        Ok(())
    }
}
