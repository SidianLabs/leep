//! State shared by agents, operators, checkers and the reporter.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::Instant;

use leep_core::{ChangesetId, Hash};
use serde::Serialize;

use crate::config::Args;
use crate::util::fnv;

/// Grace after a restart is healthy during which stragglers are still
/// attributed to it (connections made before the kill, retried requests).
const CHAOS_MARGIN_SECS: f64 = 3.0;

#[derive(Clone, Debug, Serialize)]
pub struct Violation {
    pub at_secs: f64,
    pub kind: String,
    pub detail: String,
}

#[derive(Default)]
pub struct RouteStat {
    pub count: u64,
    pub micros: Vec<u32>,
}

#[derive(Default)]
pub struct StatsInner {
    pub routes: BTreeMap<&'static str, RouteStat>,
    pub status: BTreeMap<u16, u64>,
    /// (seconds since start, microseconds) of every land answered 200.
    pub land_samples: Vec<(u32, u32)>,
    /// First land request to acknowledgement, retries and conflict redo included.
    pub land_e2e: Vec<u32>,
    pub counters: BTreeMap<&'static str, u64>,
}

#[derive(Default)]
pub struct Stats(pub Mutex<StatsInner>);

impl Stats {
    pub fn inc(&self, name: &'static str) {
        self.add(name, 1);
    }

    pub fn add(&self, name: &'static str, n: u64) {
        *self.0.lock().unwrap().counters.entry(name).or_default() += n;
    }

    pub fn get(&self, name: &'static str) -> u64 {
        self.0
            .lock()
            .unwrap()
            .counters
            .get(name)
            .copied()
            .unwrap_or(0)
    }

    pub fn record(&self, route: &'static str, status: u16, micros: u32, at_secs: u32) {
        let mut s = self.0.lock().unwrap();
        // `leep` runs are not HTTP exchanges; they keep only a latency row.
        if !route.starts_with("cli.") {
            *s.status.entry(status).or_default() += 1;
        }
        let r = s.routes.entry(route).or_default();
        r.count += 1;
        r.micros.push(micros);
        if route == "land" && status == 200 {
            s.land_samples.push((at_secs, micros));
        }
    }
}

/// Where a changeset key must be found.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Where {
    /// In the history of this ref, exactly once.
    Ref(String),
    /// Its branch was merged into main and deleted: in main, exactly once.
    Main,
    /// Its branch was deleted unmerged: nowhere to look, but never in main.
    Retired,
}

/// One changeset an agent created; `changes` is empty when the agent cannot
/// see it (a `leep` commit), which only skips the content check.
#[derive(Clone, Debug)]
pub struct Step {
    pub key: String,
    pub changes: Vec<(String, Option<Hash>)>,
}

#[derive(Clone, Debug)]
pub struct Ack {
    pub agent: String,
    pub landed: ChangesetId,
    /// Oldest first; the last one is `landed` itself.
    pub steps: Vec<Step>,
    /// The first answer arrived: `landed` is a head the ref really had.
    pub clean: bool,
    /// `landed` is a server-made copy, so file contents may be merged ones.
    pub rebased: bool,
    pub place: Where,
    /// Ledger version when `place` last changed, so a check that read the
    /// refs earlier can tell it has not seen this change.
    pub place_ver: u64,
    pub verified: bool,
}

#[derive(Default)]
pub struct LedgerInner {
    pub acks: Vec<Ack>,
    /// Indexes into `acks` per branch name.
    pub by_ref: HashMap<String, Vec<usize>>,
    /// Keys of changesets whose land answered conflicts and were dropped: no
    /// history may contain them.
    pub forbidden: HashSet<u64>,
    /// Generation of the head a branch started from.
    pub branch_base: HashMap<String, u64>,
    /// Tags agents created, with the id they pointed at.
    pub tags: HashMap<String, ChangesetId>,
    pub tags_deleted: HashSet<String>,
    /// Number of lands whose key could not be told (reverts, picks, merges).
    pub unkeyed_lands: u64,
    /// Bumped by every acknowledgement and every change of an ack's place.
    pub version: u64,
}

#[derive(Default)]
pub struct Ledger(pub Mutex<LedgerInner>);

pub fn key_hash(key: &str) -> u64 {
    fnv(key)
}

impl Ledger {
    pub fn ack(&self, mut ack: Ack) {
        let mut l = self.0.lock().unwrap();
        let idx = l.acks.len();
        if let Where::Ref(r) = &ack.place {
            l.by_ref.entry(r.clone()).or_default().push(idx);
        }
        ack.verified = false;
        l.version += 1;
        ack.place_ver = l.version;
        l.acks.push(ack);
    }

    /// The branch's lands now live in main's history.
    pub fn merge_branch(&self, name: &str) {
        let mut l = self.0.lock().unwrap();
        let idxs = l.by_ref.remove(name).unwrap_or_default();
        l.version += 1;
        let v = l.version;
        for i in idxs {
            l.acks[i].place = Where::Main;
            l.acks[i].place_ver = v;
        }
    }

    /// The branch was deleted without being merged.
    pub fn retire_branch(&self, name: &str) {
        let mut l = self.0.lock().unwrap();
        let idxs = l.by_ref.remove(name).unwrap_or_default();
        let mut keys = Vec::new();
        l.version += 1;
        let v = l.version;
        for i in idxs {
            l.acks[i].place = Where::Retired;
            l.acks[i].place_ver = v;
            keys.extend(l.acks[i].steps.iter().map(|s| key_hash(&s.key)));
        }
        l.forbidden.extend(keys);
    }

    pub fn forbid(&self, keys: &[String]) {
        let mut l = self.0.lock().unwrap();
        l.forbidden.extend(keys.iter().map(|k| key_hash(k)));
    }

    pub fn len(&self) -> usize {
        self.0.lock().unwrap().acks.len()
    }
}

pub struct ChaosEvent {
    pub start: f64,
    pub end: Option<f64>,
    pub kind: String,
}

pub struct Shared {
    pub args: Args,
    pub run_id: String,
    pub start: Instant,
    pub stop: AtomicBool,
    pub stats: Stats,
    pub violations: Mutex<Vec<Violation>>,
    pub chaos: Mutex<Vec<ChaosEvent>>,
    pub ledger: Ledger,
    /// Milliseconds since start of each agent's last completed iteration.
    pub beats: Vec<AtomicU64>,
    pub iterations: AtomicU64,
    /// When the measured run began, in milliseconds since `start`; setup
    /// before it counts as negative time.
    pub run0_ms: AtomicU64,
    /// The final checks run with the swarm stopped; a violation then keeps
    /// them going so the report shows everything that is wrong.
    pub finishing: AtomicBool,
}

impl Shared {
    pub fn new(args: Args, run_id: String, agents: usize) -> Shared {
        Shared {
            args,
            run_id,
            start: Instant::now(),
            stop: AtomicBool::new(false),
            stats: Stats::default(),
            violations: Mutex::new(Vec::new()),
            chaos: Mutex::new(Vec::new()),
            ledger: Ledger::default(),
            beats: (0..agents).map(|_| AtomicU64::new(0)).collect(),
            iterations: AtomicU64::new(0),
            run0_ms: AtomicU64::new(0),
            finishing: AtomicBool::new(false),
        }
    }

    /// Seconds since the measured run began.
    pub fn now_secs(&self) -> f64 {
        self.start.elapsed().as_secs_f64() - self.run0_ms.load(Ordering::Relaxed) as f64 / 1000.0
    }

    /// Setup is over: restart the clock and give every agent a fresh beat.
    pub fn start_run(&self) {
        let now = self.start.elapsed().as_millis() as u64;
        self.run0_ms.store(now, Ordering::Relaxed);
        for b in &self.beats {
            b.store(now, Ordering::Relaxed);
        }
    }

    pub fn stopped(&self) -> bool {
        self.stop.load(Ordering::Relaxed)
    }

    pub fn beat(&self, agent: usize) {
        self.iterations.fetch_add(1, Ordering::Relaxed);
        if let Some(b) = self.beats.get(agent) {
            b.store(self.start.elapsed().as_millis() as u64, Ordering::Relaxed);
        }
    }

    /// Record a broken invariant or threshold. The run stops at once unless
    /// `--no-fail-fast`, so the stores still show what went wrong.
    pub fn violate(&self, kind: &str, detail: impl Into<String>) {
        let detail = detail.into();
        let at = self.now_secs();
        eprintln!("VIOLATION [{kind}] at {at:.1}s: {detail}");
        let mut v = self.violations.lock().unwrap();
        // A run in trouble repeats itself; keep the report readable.
        let same = v.iter().filter(|x| x.kind == kind).count();
        if same < 20 {
            v.push(Violation {
                at_secs: at,
                kind: kind.to_string(),
                detail,
            });
        }
        drop(v);
        self.stats.inc("violations");
        if !self.args.no_fail_fast && !self.finishing.load(Ordering::Relaxed) {
            self.stop.store(true, Ordering::Relaxed);
        }
    }

    pub fn violation_count(&self) -> usize {
        self.violations.lock().unwrap().len()
    }

    pub fn chaos_begin(&self, kind: &str) -> usize {
        let mut c = self.chaos.lock().unwrap();
        c.push(ChaosEvent {
            start: self.now_secs(),
            end: None,
            kind: kind.to_string(),
        });
        c.len() - 1
    }

    pub fn chaos_end(&self, idx: usize) {
        let now = self.now_secs();
        if let Some(e) = self.chaos.lock().unwrap().get_mut(idx) {
            e.end = Some(now);
        }
    }

    /// Is `t` (seconds since start) inside a restart, or just after one?
    pub fn in_chaos(&self, t: f64) -> bool {
        self.chaos
            .lock()
            .unwrap()
            .iter()
            .any(|e| t >= e.start - 0.5 && e.end.is_none_or(|end| t <= end + CHAOS_MARGIN_SECS))
    }
}

/// `<<SOAK:run:agent:n>>`: unique, with delimiters so no token is a prefix of
/// another.
pub fn token_for(key: &str) -> String {
    format!("<<SOAK:{key}>>")
}

/// The key of a changeset message that starts `soak <key> `, if any. Reverts,
/// merges and cherry-picks have other messages and carry no key.
pub fn key_of_message(msg: &str) -> Option<&str> {
    let rest = msg.strip_prefix("soak ")?;
    rest.split_whitespace().next()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ack(key: &str, place: Where) -> Ack {
        Ack {
            agent: "a00".into(),
            landed: ChangesetId(Hash::of_blob(key.as_bytes())),
            steps: vec![Step {
                key: key.into(),
                changes: Vec::new(),
            }],
            clean: true,
            rebased: false,
            place,
            place_ver: 0,
            verified: false,
        }
    }

    #[test]
    fn keys_come_only_from_soak_messages() {
        assert_eq!(
            key_of_message("soak r:a01:0000007 edit x"),
            Some("r:a01:0000007")
        );
        assert_eq!(key_of_message("Revert \"soak r:a01:0000007 edit x\""), None);
        assert_eq!(key_of_message("Merge feature/a into main"), None);
        assert_eq!(token_for("r:a01:7"), "<<SOAK:r:a01:7>>");
    }

    #[test]
    fn branch_lands_move_with_their_branch() {
        let l = Ledger::default();
        l.ack(ack("k1", Where::Ref("feature/a-1".into())));
        l.ack(ack("k2", Where::Ref("feature/a-2".into())));
        l.ack(ack("k3", Where::Ref("main".into())));
        l.merge_branch("feature/a-1");
        l.retire_branch("feature/a-2");
        let g = l.0.lock().unwrap();
        assert_eq!(g.acks[0].place, Where::Main);
        assert_eq!(g.acks[1].place, Where::Retired);
        assert_eq!(g.acks[2].place, Where::Ref("main".into()));
        // Only what was dropped unmerged may never show up in main.
        assert!(g.forbidden.contains(&key_hash("k2")) && !g.forbidden.contains(&key_hash("k1")));
    }

    #[test]
    fn restarts_attribute_a_window_and_a_margin() {
        let args = <crate::config::Args as clap::Parser>::parse_from(["leep-soak"]);
        let sh = Shared::new(args, "r".into(), 1);
        assert!(!sh.in_chaos(sh.now_secs()));
        let i = sh.chaos_begin("SIGKILL");
        assert!(sh.in_chaos(sh.now_secs()));
        sh.chaos_end(i);
        assert!(
            sh.in_chaos(sh.now_secs()),
            "stragglers just after the restart"
        );
        assert!(!sh.in_chaos(sh.now_secs() + 10.0));
    }
}
