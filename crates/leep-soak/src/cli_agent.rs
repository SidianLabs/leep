//! An agent that works through real `leep` processes: init, sync, commit,
//! land, branch, merge, revert, cherry-pick and rebase in a working copy.

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::{Duration, Instant};

use leep_core::ChangesetId;
use tokio::process::Command;

use crate::agent::MODELS;
use crate::api::Api;
use crate::repo::{self, Layout};
use crate::shared::{token_for, Ack, Shared, Step, Where};
use crate::util::Rng;

pub struct CliEnv {
    pub sh: Arc<Shared>,
    pub bin_dir: PathBuf,
    pub url: String,
    pub layout: Layout,
    pub work: PathBuf,
    /// Secret key files (mode 0600) of trusted authors; empty without policy.
    pub key_files: Vec<PathBuf>,
}

pub struct Out {
    pub code: i32,
    pub stdout: String,
    pub stderr: String,
}

struct Landed {
    id: ChangesetId,
    rebased: bool,
    clean: bool,
}

enum LandResult {
    Landed(Landed),
    Conflicts,
    /// The server refuses an unlanded merge commit once main has moved: the
    /// answer is to merge the new main into it, as its message says.
    NeedsMerge,
    Failed,
}

pub struct CliAgent {
    env: Arc<CliEnv>,
    sh: Arc<Shared>,
    api: Api,
    token: Option<String>,
    idx: usize,
    beat: usize,
    label: String,
    wc: PathBuf,
    cache: PathBuf,
    rng: Rng,
    seq: u64,
    branches: u32,
    mine: Vec<ChangesetId>,
}

impl CliAgent {
    pub fn new(
        env: Arc<CliEnv>,
        idx: usize,
        beat: usize,
        api: Api,
        token: Option<String>,
    ) -> CliAgent {
        let sh = env.sh.clone();
        let label = format!("c{idx:02}");
        CliAgent {
            wc: env.work.join(format!("wc-{label}")),
            cache: env.work.join(format!("cache-{label}")),
            rng: Rng::new(sh.args.seed ^ ((idx as u64 + 500) * 0x0051_ED27)),
            env,
            sh,
            api,
            token,
            idx,
            beat,
            label,
            seq: 0,
            branches: 0,
            mine: Vec::new(),
        }
    }

    fn next_key(&mut self) -> String {
        self.seq += 1;
        format!("{}:{}:{:07}", self.sh.run_id, self.label, self.seq)
    }

    /// Run `leep` with `args`, waiting out restarts and throttling; anything
    /// else that fails is returned to the caller.
    async fn leep(&self, args: &[&str]) -> Out {
        let t0 = Instant::now();
        let started = self.sh.now_secs();
        loop {
            let mut c = Command::new(self.env.bin_dir.join("leep"));
            c.current_dir(&self.wc)
                .args(args)
                .env("LEEP_SERVER", &self.env.url)
                .env("LEEP_CACHE_DIR", &self.cache)
                .env("LEEP_AUTHOR", format!("soak-cli-{}", self.idx))
                .env("LEEP_AGENT_MODEL", MODELS[self.idx % MODELS.len()])
                .env(
                    "LEEP_AGENT_SESSION",
                    format!("{}-{}-{}", self.sh.run_id, self.label, self.seq / 20),
                )
                .env("LEEP_AGENT_PROMPT", "soak cli task")
                .env_remove("LEEP_AGENT_TESTS")
                .env_remove("LEEP_SIGNING_KEY")
                .env("NO_PROXY", "127.0.0.1,localhost")
                .stdin(Stdio::null())
                .kill_on_drop(true);
            if let Some(t) = &self.token {
                c.env("LEEP_TOKEN", t);
            } else {
                c.env_remove("LEEP_TOKEN");
            }
            let out = match tokio::time::timeout(Duration::from_secs(180), c.output()).await {
                Ok(Ok(o)) => o,
                Ok(Err(e)) => {
                    self.sh
                        .violate("cli_spawn", format!("cannot run leep: {e}"));
                    return Out {
                        code: -1,
                        stdout: String::new(),
                        stderr: e.to_string(),
                    };
                }
                Err(_) => {
                    self.sh.violate(
                        "stall",
                        format!("`leep {}` by {} ran over 180s", args.join(" "), self.label),
                    );
                    return Out {
                        code: -1,
                        stdout: String::new(),
                        stderr: "timeout".into(),
                    };
                }
            };
            let o = Out {
                code: out.status.code().unwrap_or(-1),
                stdout: String::from_utf8_lossy(&out.stdout).into_owned(),
                stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
            };
            let cmd = args
                .iter()
                .find(|a| !a.starts_with('-'))
                .copied()
                .unwrap_or("?");
            self.sh.stats.record(
                cli_route(cmd),
                if o.code == 0 { 200 } else { 599 },
                t0.elapsed().as_micros() as u32,
                0,
            );
            if o.code == 0 {
                return o;
            }
            let low = o.stderr.to_lowercase();
            if low.contains("429 too many") || low.contains("too many requests") {
                self.sh.stats.inc("throttled_429");
                tokio::time::sleep(Duration::from_millis(300)).await;
            } else if low.contains("server busy") && low.contains("nothing was done") {
                // A read shed on each of `leep`'s own tries (ADR 0060).
                self.sh.stats.inc("shed_503");
                tokio::time::sleep(Duration::from_millis(300)).await;
            } else if o.code == 3
                || low.contains("error sending request")
                || low.contains("connection")
                || low.contains("timed out")
                || low.contains("408 request")
            {
                let now = self.sh.now_secs();
                if self.sh.in_chaos(now) || self.sh.in_chaos(started) {
                    self.sh.stats.inc("attributed_to_restart");
                } else if o.code == 3 {
                    self.sh.violate(
                        "cli_server_error",
                        format!(
                            "`leep {}` by {}: {}",
                            args.join(" "),
                            self.label,
                            o.stderr.trim()
                        ),
                    );
                    return o;
                } else {
                    return o;
                }
                tokio::time::sleep(Duration::from_millis(400)).await;
            } else {
                return o;
            }
            if self.sh.stopped() || t0.elapsed() > Duration::from_secs(90) {
                return o;
            }
        }
    }

    /// `switch -c`, tolerating a repeat after a restart: creating a ref is not
    /// idempotent (ADR 0021), so if the first try applied, the name exists
    /// and switching to it finishes the job.
    async fn switch_new(&self, name: &str) -> Out {
        let o = self.leep(&["switch", "-c", name]).await;
        if o.code != 0 && o.stderr.contains("already exists") {
            self.sh.stats.inc("cli_repeat_after_applied");
            return self.leep(&["switch", name]).await;
        }
        o
    }

    fn unexpected(&self, what: &str, o: &Out) {
        self.sh.violate(
            "cli_failure",
            format!(
                "{} by {}: exit {} {} {}",
                what,
                self.label,
                o.code,
                o.stdout.trim().chars().take(200).collect::<String>(),
                o.stderr.trim().chars().take(300).collect::<String>()
            ),
        );
    }

    pub async fn run(mut self) {
        let _ = std::fs::create_dir_all(&self.wc);
        let o = self.leep(&["init", "."]).await;
        if o.code != 0 {
            if !self.sh.stopped() {
                self.unexpected("init", &o);
            }
            return;
        }
        let jitter = self.rng.think(self.sh.args.think);
        tokio::time::sleep(jitter).await;
        while !self.sh.stopped() {
            if !self.iteration().await {
                break;
            }
            self.sh.beat(self.beat);
            let t = self.rng.think(self.sh.args.think * 3);
            let end = Instant::now() + t;
            while Instant::now() < end && !self.sh.stopped() {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
        self.finish().await;
    }

    /// No merge or pick may be left half done.
    async fn finish(&mut self) {
        let o = self.leep(&["--json", "status"]).await;
        if o.code != 0 {
            return;
        }
        match serde_json::from_str::<serde_json::Value>(&o.stdout) {
            Ok(v) => {
                let stuck = !v["merging"].is_null() || !v["picking"].is_null();
                let conflicted = v["changes"]
                    .as_array()
                    .is_some_and(|c| c.iter().any(|e| e["status"] == "conflicted"));
                if stuck || conflicted {
                    self.sh.violate(
                        "conflict_state",
                        format!(
                            "{}'s working copy ends mid-merge or with conflicts: {}",
                            self.label,
                            o.stdout.trim()
                        ),
                    );
                }
            }
            Err(e) => self.sh.violate(
                "cli_failure",
                format!("status --json of {}: {e}", self.label),
            ),
        }
        self.sh.stats.inc("cli_workcopies_checked");
    }

    async fn iteration(&mut self) -> bool {
        // Most of the time start from the newest main, like `leep sync`.
        let stale = self.rng.pct(15);
        if !stale {
            let o = self.leep(&["sync"]).await;
            if o.code != 0 {
                if self.sh.stopped() {
                    return false;
                }
                self.unexpected("sync", &o);
                return self.recover().await;
            }
        }
        let roll = self.rng.below(100);
        match roll {
            0..=47 => self.simple(1, false).await,
            48..=62 => self.simple(2, false).await,
            63..=76 => self.branch_flow().await,
            77..=83 => self.revert_flow().await,
            84..=88 => self.pick_flow().await,
            _ => self.simple(1, true).await,
        }
    }

    /// Throw away local state after something unexpected, so the next
    /// iteration starts clean.
    async fn recover(&mut self) -> bool {
        let o = self.leep(&["checkout", "main", "--force"]).await;
        o.code == 0 || self.sh.stopped()
    }

    fn pick_src(&mut self) -> PathBuf {
        let (d, i) = (
            self.rng.below(self.env.layout.dirs),
            self.rng.below(repo::FILES_PER_DIR),
        );
        self.wc.join(repo::src_path(d, i))
    }

    /// Edit files in the working copy, plant a token and commit. `None` when
    /// the commit failed.
    async fn edit_and_commit(&mut self, lineage: &str, sign: bool) -> Option<String> {
        for _ in 0..1 + self.rng.below(2) {
            let p = self.pick_src();
            if let Ok(text) = std::fs::read_to_string(&p) {
                let new = repo::edit_text(&mut self.rng, &text);
                let _ = std::fs::write(&p, new);
            }
        }
        let key = self.next_key();
        let dir = self.wc.join(format!("notes/{}", self.label));
        let _ = std::fs::create_dir_all(&dir);
        let (path, mut text) = notes_file(&dir, lineage);
        text.push_str(&format!(
            "{} {} step {}\n",
            token_for(&key),
            self.label,
            self.seq
        ));
        let _ = std::fs::write(&path, text);
        let msg = format!("soak {key} cli edit");
        let mut args = vec!["commit", "-m", msg.as_str()];
        let keyfile;
        if sign && !self.env.key_files.is_empty() {
            keyfile = self.env.key_files[self.idx % self.env.key_files.len()]
                .to_string_lossy()
                .into_owned();
            args.extend(["--sign", keyfile.as_str()]);
        }
        let o = self.leep(&args).await;
        if o.code != 0 {
            if !self.sh.stopped() {
                self.unexpected("commit", &o);
            }
            self.sh.ledger.forbid(&[key]);
            return None;
        }
        Some(key)
    }

    async fn land(&mut self, keys: &[String], place: &str) -> LandResult {
        let mut clean = true;
        let t0 = Instant::now();
        loop {
            let o = self.leep(&["--json", "land"]).await;
            let v: Option<serde_json::Value> = serde_json::from_str(&o.stdout).ok();
            let status = v
                .as_ref()
                .and_then(|v| v["status"].as_str())
                .map(str::to_string);
            match (o.code, status.as_deref()) {
                (0, Some("landed")) => {
                    let id: ChangesetId = v.as_ref().unwrap()["landed"]
                        .as_str()
                        .unwrap()
                        .parse()
                        .unwrap();
                    let rebased = v.as_ref().unwrap()["rebased"].as_bool().unwrap_or(false);
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
                        .push(t0.elapsed().as_micros() as u32);
                    return LandResult::Landed(Landed { id, rebased, clean });
                }
                (5, Some("conflicts")) => {
                    self.sh.stats.inc("land_conflicts");
                    self.sh.ledger.forbid(keys);
                    return LandResult::Conflicts;
                }
                _ => {
                    clean = false;
                    if o.code == 1 && o.stderr.contains("is a merge; merge") {
                        self.sh.stats.inc("cli_merge_needs_merge");
                        return LandResult::NeedsMerge;
                    }
                    // Land is idempotent: after a restart ask again.
                    if self.sh.stopped() {
                        return LandResult::Failed;
                    }
                    let low = o.stderr.to_lowercase();
                    let transient = o.code == 3
                        || low.contains("408 request")
                        || low.contains("409 conflict")
                        || low.contains("kept losing")
                        || low.contains("connection")
                        || low.contains("timed out");
                    if transient && t0.elapsed() < Duration::from_secs(60) {
                        self.sh.stats.inc("cli_land_retries");
                        tokio::time::sleep(Duration::from_millis(300)).await;
                        continue;
                    }
                    self.unexpected(&format!("land on {place}"), &o);
                    return LandResult::Failed;
                }
            }
        }
    }

    fn ack(&mut self, keys: &[String], l: &Landed, place: Where) {
        self.sh.ledger.ack(Ack {
            agent: self.label.clone(),
            landed: l.id,
            steps: keys
                .iter()
                .map(|k| Step {
                    key: k.clone(),
                    changes: Vec::new(),
                })
                .collect(),
            clean: l.clean,
            rebased: l.rebased,
            place,
            place_ver: 0,
            verified: false,
        });
    }

    /// Commit `n` times on main's lineage and land. With `rebase`, the
    /// commits are first moved onto the newest main by `leep rebase`.
    async fn simple(&mut self, n: usize, rebase: bool) -> bool {
        let mut keys = Vec::new();
        for _ in 0..n {
            let sign = self.rng.pct(20) && !self.sh.args.no_policy;
            match self.edit_and_commit("main", sign).await {
                Some(k) => keys.push(k),
                None => return self.recover().await,
            }
        }
        if rebase {
            let o = self.leep(&["rebase"]).await;
            if o.code != 0 {
                if self.sh.stopped() {
                    return false;
                }
                // Conflicts on a rebase are a normal outcome: give the work up.
                self.sh.stats.inc("cli_rebase_conflicts");
                self.sh.ledger.forbid(&keys);
                return self.recover().await;
            }
            self.sh.stats.inc("cli_rebases");
        }
        match self.land(&keys, "main").await {
            LandResult::Landed(l) => {
                self.mine.push(l.id);
                self.ack(&keys, &l, Where::Ref("main".into()));
                true
            }
            _ => self.recover().await,
        }
    }

    async fn branch_flow(&mut self) -> bool {
        self.branches += 1;
        let name = format!("feature/{}-{}", self.label, self.branches);
        let lineage = format!("fb{}", self.branches);
        let o = self.switch_new(&name).await;
        if o.code != 0 {
            if !self.sh.stopped() {
                self.unexpected("switch -c", &o);
            }
            return self.recover().await;
        }
        self.sh.stats.inc("branches_created");
        // The branch starts at the working copy's base, which is a main head
        // or an older ancestor of it.
        let Ok(Some(head)) = self.api.get_ref(&name).await else {
            return false;
        };
        if let Ok(info) = self.api.changeset(head).await {
            self.sh
                .ledger
                .0
                .lock()
                .unwrap()
                .branch_base
                .insert(name.clone(), info.generation);
        }
        let lands = 1 + self.rng.below(2);
        let mut all_keys = Vec::new();
        for _ in 0..lands {
            let Some(k) = self.edit_and_commit(&lineage, false).await else {
                return self.recover().await;
            };
            let keys = vec![k];
            match self.land(&keys, &name).await {
                LandResult::Landed(l) => {
                    self.ack(&keys, &l, Where::Ref(name.clone()));
                    all_keys.extend(keys);
                }
                _ => return self.recover().await,
            }
        }
        let o = self.leep(&["switch", "main"]).await;
        if o.code != 0 {
            if !self.sh.stopped() {
                self.unexpected("switch main", &o);
            }
            return self.recover().await;
        }
        match self.rng.below(100) {
            0..=64 => self.merge_branch(&name, &all_keys).await,
            65..=84 => {
                self.sh.ledger.retire_branch(&name);
                self.sh.stats.inc("branches_retired");
                self.delete_branch(&name, true).await
            }
            _ => true,
        }
    }

    async fn merge_branch(&mut self, name: &str, keys: &[String]) -> bool {
        let msg = format!("Merge {name} into main");
        let o = self.leep(&["merge", name, "--no-ff", "-m", &msg]).await;
        if o.code != 0 {
            if self.sh.stopped() {
                return false;
            }
            self.sh.stats.inc("cli_merge_conflicts");
            let _ = self.leep(&["merge", "--abort"]).await;
            self.sh.ledger.retire_branch(name);
            let ok = self.recover().await;
            return self.delete_branch(name, true).await && ok;
        }
        self.sh.stats.inc("cli_merges");
        let mut result = self.land(&[], "main").await;
        for _ in 0..5 {
            if !matches!(result, LandResult::NeedsMerge) || self.sh.stopped() {
                break;
            }
            let o = self.leep(&["merge", "main", "-m", "Merge main"]).await;
            if o.code != 0 {
                let _ = self.leep(&["merge", "--abort"]).await;
                break;
            }
            result = self.land(&[], "main").await;
        }
        match result {
            LandResult::Landed(l) => {
                self.sh.ledger.0.lock().unwrap().unkeyed_lands += 1;
                self.mine.push(l.id);
                let _ = keys;
                self.sh.stats.inc("branches_merged");
                // Before the branch goes: its commits are now main's.
                self.sh.ledger.merge_branch(name);
                self.delete_branch(name, false).await
            }
            _ => {
                self.sh.ledger.retire_branch(name);
                let ok = self.recover().await;
                self.delete_branch(name, true).await && ok
            }
        }
    }

    async fn delete_branch(&mut self, name: &str, force: bool) -> bool {
        let o = self
            .leep(&["branch", if force { "-D" } else { "-d" }, name])
            .await;
        // A repeat after a restart finds the branch already deleted.
        if o.code != 0 && o.stderr.contains("does not exist") {
            self.sh.stats.inc("cli_repeat_after_applied");
            return true;
        }
        if o.code != 0 && !self.sh.stopped() {
            self.unexpected(&format!("branch delete {name}"), &o);
            return false;
        }
        true
    }

    async fn revert_flow(&mut self) -> bool {
        if self.mine.is_empty() {
            return self.simple(1, false).await;
        }
        let target = self.mine[self.rng.below(self.mine.len())].to_string();
        let o = self.leep(&["revert", &target]).await;
        if o.code != 0 {
            if self.sh.stopped() {
                return false;
            }
            self.sh.stats.inc("cli_revert_conflicts");
            let _ = self.leep(&["revert", "--abort"]).await;
            return self.recover().await;
        }
        self.sh.stats.inc("cli_reverts");
        match self.land(&[], "main").await {
            LandResult::Landed(l) => {
                self.sh.ledger.0.lock().unwrap().unkeyed_lands += 1;
                self.mine.push(l.id);
                true
            }
            _ => self.recover().await,
        }
    }

    /// Cherry-pick the one commit of a throwaway branch onto main, then drop
    /// the branch: the picked copy is the only holder of its key.
    async fn pick_flow(&mut self) -> bool {
        self.branches += 1;
        let name = format!("feature/{}-{}", self.label, self.branches);
        let o = self.switch_new(&name).await;
        if o.code != 0 {
            if !self.sh.stopped() {
                self.unexpected("switch -c", &o);
            }
            return self.recover().await;
        }
        self.sh.stats.inc("branches_created");
        let Ok(Some(head)) = self.api.get_ref(&name).await else {
            return false;
        };
        if let Ok(info) = self.api.changeset(head).await {
            self.sh
                .ledger
                .0
                .lock()
                .unwrap()
                .branch_base
                .insert(name.clone(), info.generation);
        }
        let Some(k) = self
            .edit_and_commit(&format!("fb{}", self.branches), false)
            .await
        else {
            return self.recover().await;
        };
        let keys = vec![k];
        match self.land(&keys, &name).await {
            LandResult::Landed(l) => self.ack(&keys, &l, Where::Ref(name.clone())),
            _ => return self.recover().await,
        }
        let o = self.leep(&["switch", "main"]).await;
        if o.code != 0 {
            if !self.sh.stopped() {
                self.unexpected("switch main", &o);
            }
            return self.recover().await;
        }
        let o = self.leep(&["cherry-pick", &name]).await;
        if o.code != 0 {
            if self.sh.stopped() {
                return false;
            }
            self.sh.stats.inc("cli_pick_conflicts");
            let _ = self.leep(&["cherry-pick", "--abort"]).await;
            self.sh.ledger.retire_branch(&name);
            let ok = self.recover().await;
            return self.delete_branch(&name, true).await && ok;
        }
        match self.land(&[], "main").await {
            LandResult::Landed(l) => {
                self.sh.ledger.0.lock().unwrap().unkeyed_lands += 1;
                self.mine.push(l.id);
                self.sh.stats.inc("cli_picks");
                self.sh.ledger.merge_branch(&name);
                self.delete_branch(&name, true).await
            }
            _ => {
                self.sh.ledger.retire_branch(&name);
                let ok = self.recover().await;
                self.delete_branch(&name, true).await && ok
            }
        }
    }
}

/// The newest notes file for `lineage` (or a new one when it has 40 lines).
fn notes_file(dir: &Path, lineage: &str) -> (PathBuf, String) {
    let mut best: Option<(usize, PathBuf)> = None;
    if let Ok(rd) = std::fs::read_dir(dir) {
        for e in rd.flatten() {
            let n = e.file_name().to_string_lossy().into_owned();
            if let Some(num) = n
                .strip_prefix(&format!("{lineage}-"))
                .and_then(|r| r.strip_suffix(".txt"))
                .and_then(|r| r.parse::<usize>().ok())
            {
                if best.as_ref().is_none_or(|(b, _)| num > *b) {
                    best = Some((num, e.path()));
                }
            }
        }
    }
    match best {
        Some((n, p)) => {
            let text = std::fs::read_to_string(&p).unwrap_or_default();
            if text.lines().count() >= 40 {
                (
                    dir.join(format!("{lineage}-{:04}.txt", n + 1)),
                    String::new(),
                )
            } else {
                (p, text)
            }
        }
        None => (dir.join(format!("{lineage}-0000.txt")), String::new()),
    }
}

fn cli_route(cmd: &str) -> &'static str {
    match cmd {
        "init" => "cli.init",
        "sync" => "cli.sync",
        "commit" => "cli.commit",
        "land" => "cli.land",
        "switch" => "cli.switch",
        "merge" => "cli.merge",
        "revert" => "cli.revert",
        "cherry-pick" => "cli.cherry-pick",
        "rebase" => "cli.rebase",
        "branch" => "cli.branch",
        "checkout" => "cli.checkout",
        "status" => "cli.status",
        _ => "cli.other",
    }
}
