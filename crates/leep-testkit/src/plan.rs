use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use leep_core::Error;

use crate::rng::Rng;

/// A storage call a plan can target.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Op {
    // BlobStore
    Put,
    Get,
    Contains,
    ListPage,
    Delete,
    /// [`leep_core::BlobStore::flush`]: making earlier puts durable.
    Flush,
    // MetaStore
    PutChangeset,
    GetChangeset,
    ListChangesets,
    GetRef,
    CasRef,
    RefLog,
    ListRefs,
    GetDerived,
    PutDerived,
    ListDerived,
    DeleteDerivedKind,
}

impl Op {
    /// Calls that change stored state, the only ones where "applied, then
    /// failed" differs from "failed".
    pub fn is_write(self) -> bool {
        matches!(
            self,
            Op::Put
                | Op::Delete
                | Op::PutChangeset
                | Op::CasRef
                | Op::PutDerived
                | Op::DeleteDerivedKind
        )
    }

    /// Reads that return bytes, which [`Fault::Corrupt`] can damage.
    pub fn returns_bytes(self) -> bool {
        matches!(self, Op::Get | Op::GetDerived)
    }

    /// Every blob store call.
    pub const BLOB: &'static [Op] = &[
        Op::Put,
        Op::Get,
        Op::Contains,
        Op::ListPage,
        Op::Delete,
        Op::Flush,
    ];
    /// Every meta store call.
    pub const META: &'static [Op] = &[
        Op::PutChangeset,
        Op::GetChangeset,
        Op::ListChangesets,
        Op::GetRef,
        Op::CasRef,
        Op::RefLog,
        Op::ListRefs,
        Op::GetDerived,
        Op::PutDerived,
        Op::ListDerived,
        Op::DeleteDerivedKind,
    ];
    /// Calls that change state, blob and meta.
    pub const WRITES: &'static [Op] = &[
        Op::Put,
        Op::Delete,
        Op::PutChangeset,
        Op::CasRef,
        Op::PutDerived,
        Op::DeleteDerivedKind,
    ];
}

/// What happens to one call.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Fault {
    /// The backend never sees the call; the caller gets `Error::Storage`.
    Fail,
    /// The backend applies the call and the caller still gets `Error::Storage`:
    /// a lost response or a timeout after commit. For reads this is `Fail`.
    FailAfter,
    /// Sleep (tokio time, so tests may pause the clock) before the call.
    Delay(Duration),
    /// Apply the call, then sleep before answering. A caller that gives up
    /// during the stall has an applied write it never heard about.
    Stall(Duration),
    /// Flip one seeded bit in the bytes a read returns (a bad disk or row).
    /// Treated as `Fail` for calls that return no bytes.
    Corrupt,
}

/// Injection counts so far, to assert a test really exercised its faults.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Injected {
    pub fail: u64,
    pub fail_after: u64,
    pub delay: u64,
    pub stall: u64,
    pub corrupt: u64,
}

impl Injected {
    pub fn total(&self) -> u64 {
        self.fail + self.fail_after + self.delay + self.stall + self.corrupt
    }
}

struct Rule {
    ops: Vec<Op>,
    per_mille: u32,
    fault: Fault,
}

#[derive(Default)]
struct State {
    rules: Vec<Rule>,
    /// (op, call number) -> fault, for exact schedules.
    script: HashMap<(Op, u64), Fault>,
    calls: HashMap<Op, u64>,
    injected: Injected,
    paused: bool,
}

/// Decides which calls fail. Shared (`Arc`) between the wrappers of one test
/// so a single seed drives every store, and so a schedule can span both.
pub struct FaultPlan {
    seed: u64,
    state: Mutex<State>,
    rng: Mutex<Rng>,
    jitter: AtomicU32,
}

impl FaultPlan {
    pub fn new(seed: u64) -> Arc<Self> {
        Arc::new(FaultPlan {
            seed,
            state: Mutex::default(),
            rng: Mutex::new(Rng::new(seed)),
            jitter: AtomicU32::new(0),
        })
    }

    pub fn seed(&self) -> u64 {
        self.seed
    }

    /// Inject `fault` into each call of `ops` with probability
    /// `per_mille / 1000`. Rules are tried in the order added; the first that
    /// fires wins.
    pub fn rate(&self, ops: &[Op], per_mille: u32, fault: Fault) -> &Self {
        self.state.lock().unwrap().rules.push(Rule {
            ops: ops.to_vec(),
            per_mille,
            fault,
        });
        self
    }

    /// Inject `fault` into the `nth` call (1-based, counted from the plan's
    /// creation) of `op`. A schedule beats the random rules.
    pub fn script(&self, op: Op, nth: u64, fault: Fault) -> &Self {
        self.state.lock().unwrap().script.insert((op, nth), fault);
        self
    }

    /// Yield to the scheduler up to `max_yields` times (seeded) before every
    /// call. In-memory stores never suspend, so without this tasks on a
    /// single-threaded runtime would run one after another instead of
    /// interleaving; with it the interleaving varies by seed and replays.
    pub fn jitter(&self, max_yields: u32) -> &Self {
        self.jitter.store(max_yields, Ordering::Relaxed);
        self
    }

    /// Stop injecting (calls still count), for a verification phase that
    /// reads through the same wrappers.
    pub fn pause(&self) {
        self.state.lock().unwrap().paused = true;
    }

    pub fn resume(&self) {
        self.state.lock().unwrap().paused = false;
    }

    /// Drop every rule and schedule entry.
    pub fn clear(&self) {
        let mut s = self.state.lock().unwrap();
        s.rules.clear();
        s.script.clear();
    }

    pub fn injected(&self) -> Injected {
        self.state.lock().unwrap().injected
    }

    pub fn calls(&self, op: Op) -> u64 {
        self.state
            .lock()
            .unwrap()
            .calls
            .get(&op)
            .copied()
            .unwrap_or(0)
    }

    fn decide(&self, op: Op) -> Option<Fault> {
        let mut s = self.state.lock().unwrap();
        let n = {
            let c = s.calls.entry(op).or_default();
            *c += 1;
            *c
        };
        if s.paused {
            return None;
        }
        let mut fault = s.script.get(&(op, n)).copied();
        if fault.is_none() {
            let mut rng = self.rng.lock().unwrap();
            // Roll every matching rule so the random stream does not depend on
            // which rule fires.
            for r in &s.rules {
                if r.ops.contains(&op) && rng.chance(r.per_mille) && fault.is_none() {
                    fault = Some(r.fault);
                }
            }
        }
        let fault = fault?;
        // Faults that cannot apply to this call degrade to `Fail`.
        let fault = match fault {
            Fault::Corrupt if !op.returns_bytes() => Fault::Fail,
            Fault::FailAfter if !op.is_write() => Fault::Fail,
            f => f,
        };
        match fault {
            Fault::Fail => s.injected.fail += 1,
            Fault::FailAfter => s.injected.fail_after += 1,
            Fault::Delay(_) => s.injected.delay += 1,
            Fault::Stall(_) => s.injected.stall += 1,
            Fault::Corrupt => s.injected.corrupt += 1,
        }
        Some(fault)
    }

    /// Flip one seeded bit of `data`.
    pub(crate) fn corrupt(&self, data: &[u8]) -> Vec<u8> {
        let mut out = data.to_vec();
        if !out.is_empty() {
            let r = self.rng.lock().unwrap().next_u64() as usize;
            out[(r / 8) % data.len()] ^= 1 << (r % 8);
        }
        out
    }

    /// Run `call` under whatever fault `op` draws. `damage` turns a read's
    /// result into its corrupted form.
    pub(crate) async fn run<T>(
        &self,
        op: Op,
        call: impl std::future::Future<Output = leep_core::Result<T>>,
        damage: impl FnOnce(&FaultPlan, T) -> T,
    ) -> leep_core::Result<T> {
        let max = self.jitter.load(Ordering::Relaxed);
        if max > 0 {
            let yields = self.rng.lock().unwrap().below(max as usize + 1);
            for _ in 0..yields {
                tokio::task::yield_now().await;
            }
        }
        match self.decide(op) {
            None => call.await,
            Some(Fault::Fail) => Err(injected(op, "failed")),
            Some(Fault::Delay(d)) => {
                tokio::time::sleep(d).await;
                call.await
            }
            Some(Fault::Stall(d)) => {
                let out = call.await;
                tokio::time::sleep(d).await;
                out
            }
            Some(Fault::FailAfter) => {
                call.await?;
                Err(injected(op, "failed after applying"))
            }
            Some(Fault::Corrupt) => call.await.map(|v| damage(self, v)),
        }
    }
}

const PREFIX: &str = "injected fault";

fn injected(op: Op, what: &str) -> Error {
    Error::Storage(format!("{PREFIX}: {op:?} {what}"))
}

/// Whether `e` came from a [`FaultPlan`] rather than a real failure, so a
/// test can tell a tolerated fault from a bug.
pub fn is_injected(e: &Error) -> bool {
    matches!(e, Error::Storage(m) if m.starts_with(PREFIX))
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use leep_core::memory::{MemoryBlobStore, MemoryMetaStore};
    use leep_core::{BlobStore, Hash, MetaStore};

    use super::*;
    use crate::{FaultyBlobStore, FaultyMetaStore};

    fn blobs(plan: &Arc<FaultPlan>) -> (Arc<MemoryBlobStore>, FaultyBlobStore) {
        let mem = Arc::new(MemoryBlobStore::new());
        (mem.clone(), FaultyBlobStore::new(mem, plan.clone()))
    }

    #[tokio::test]
    async fn scripted_faults_hit_the_exact_call() {
        let plan = FaultPlan::new(1);
        plan.script(Op::Put, 2, Fault::Fail)
            .script(Op::Put, 3, Fault::FailAfter);
        let (mem, store) = blobs(&plan);
        let (a, b, c) = (Bytes::from("a"), Bytes::from("b"), Bytes::from("c"));
        store.put(a.clone()).await.unwrap();
        let e = store.put(b.clone()).await.unwrap_err();
        assert!(is_injected(&e));
        assert!(
            !mem.contains(&Hash::of_blob(&b)).await.unwrap(),
            "never applied"
        );
        let e = store.put(c.clone()).await.unwrap_err();
        assert!(is_injected(&e));
        assert!(
            mem.contains(&Hash::of_blob(&c)).await.unwrap(),
            "applied, then failed"
        );
        assert_eq!(plan.injected().fail, 1);
        assert_eq!(plan.injected().fail_after, 1);
        assert_eq!(plan.calls(Op::Put), 3);
    }

    #[tokio::test]
    async fn same_seed_injects_the_same_faults() {
        let run = |seed| async move {
            let plan = FaultPlan::new(seed);
            plan.rate(Op::BLOB, 300, Fault::Fail);
            let (_, store) = blobs(&plan);
            let mut out = Vec::new();
            for n in 0..64 {
                out.push(store.put(Bytes::from(format!("{n}"))).await.is_err());
            }
            out
        };
        assert_eq!(run(5).await, run(5).await);
        assert_ne!(run(5).await, run(6).await);
        assert!(run(5).await.contains(&true) && run(5).await.contains(&false));
    }

    #[tokio::test]
    async fn pause_and_clear_stop_injection_but_calls_still_count() {
        let plan = FaultPlan::new(3);
        plan.rate(Op::BLOB, 1000, Fault::Fail);
        let (_, store) = blobs(&plan);
        assert!(store.put(Bytes::from("x")).await.is_err());
        plan.pause();
        store.put(Bytes::from("x")).await.unwrap();
        plan.resume();
        assert!(store.put(Bytes::from("x")).await.is_err());
        plan.clear();
        store.put(Bytes::from("x")).await.unwrap();
        assert_eq!(plan.calls(Op::Put), 4);
    }

    #[tokio::test]
    async fn corrupt_flips_one_bit_of_a_read() {
        let plan = FaultPlan::new(9);
        let mem = Arc::new(MemoryBlobStore::new());
        let h = mem.put(Bytes::from(vec![0u8; 64])).await.unwrap();
        plan.script(Op::Get, 1, Fault::Corrupt);
        let store = FaultyBlobStore::new(mem, plan.clone());
        let bad = store.get(&h).await.unwrap().unwrap();
        let flipped: u32 = bad.iter().map(|b| b.count_ones()).sum();
        assert_eq!(flipped, 1);
        assert_eq!(store.get(&h).await.unwrap().unwrap(), vec![0u8; 64]);
        // Corrupting a call with no bytes to damage fails it instead.
        plan.script(Op::Contains, 1, Fault::Corrupt);
        assert!(store.contains(&h).await.is_err());
    }

    #[tokio::test]
    async fn ambiguous_cas_moves_the_ref_and_reports_an_error() {
        let plan = FaultPlan::new(2);
        plan.script(Op::CasRef, 1, Fault::FailAfter);
        let mem = Arc::new(MemoryMetaStore::new());
        let store = FaultyMetaStore::new(mem.clone(), plan);
        let id = leep_core::ChangesetId(Hash::of_blob(b"x"));
        assert!(is_injected(
            &store.cas_ref("main", None, Some(id)).await.unwrap_err()
        ));
        assert_eq!(mem.get_ref("main").await.unwrap(), Some(id));
        assert_eq!(mem.ref_log(None, None, 10).await.unwrap().len(), 1);
        // FailAfter on a read is just a failure.
        let plan = FaultPlan::new(2);
        plan.script(Op::GetRef, 1, Fault::FailAfter);
        let store = FaultyMetaStore::new(mem, plan);
        assert!(store.get_ref("main").await.is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn delays_and_stalls_use_tokio_time() {
        let plan = FaultPlan::new(4);
        let d = Duration::from_secs(30);
        plan.script(Op::Put, 1, Fault::Delay(d))
            .script(Op::Put, 2, Fault::Stall(d));
        let (mem, store) = blobs(&plan);
        let t = tokio::time::Instant::now();
        store.put(Bytes::from("a")).await.unwrap();
        assert!(t.elapsed() >= d);
        // A caller that times out during the stall still has its write.
        let h = Hash::of_blob(b"b");
        let r = tokio::time::timeout(Duration::from_secs(1), store.put(Bytes::from("b"))).await;
        assert!(r.is_err(), "timed out");
        assert!(mem.contains(&h).await.unwrap());
    }
}
