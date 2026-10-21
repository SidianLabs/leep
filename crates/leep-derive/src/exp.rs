//! Experiment switches for the land leader (soak/e1-leader-runtime only,
//! never merged). Read once from the environment:
//!
//! - `LEEP_EXP_LEADER_RUNTIME=1`: per-ref land leaders run on a dedicated
//!   multi-thread runtime, so batches never share worker threads with
//!   request handlers.
//! - `LEEP_EXP_LEADER_THREADS=N`: that runtime's worker threads (default 2).
//! - `LEEP_EXP_REPLAY_BLOCKING=1`: each land's replay step runs on a blocking
//!   thread (`spawn_blocking` + `Handle::block_on`), off the async workers.

use std::sync::OnceLock;

use tokio::runtime::{Handle, Runtime};

fn flag(name: &str) -> bool {
    std::env::var(name).is_ok_and(|v| matches!(v.trim(), "1" | "true" | "yes" | "on"))
}

/// Worker threads of the leader runtime.
pub fn leader_threads() -> usize {
    std::env::var("LEEP_EXP_LEADER_THREADS")
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .filter(|n| *n > 0)
        .unwrap_or(2)
}

/// The dedicated leader runtime, when `LEEP_EXP_LEADER_RUNTIME` is on. Never
/// dropped: it lives for the process.
pub fn leader_runtime() -> Option<&'static Handle> {
    static RT: OnceLock<Option<Runtime>> = OnceLock::new();
    RT.get_or_init(|| {
        if !flag("LEEP_EXP_LEADER_RUNTIME") {
            return None;
        }
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(leader_threads())
            .thread_name("leep-land-leader")
            .enable_all()
            .build()
            .expect("building the land leader runtime");
        Some(rt)
    })
    .as_ref()
    .map(|rt| rt.handle())
}

/// Whether replays run on blocking threads.
pub fn replay_blocking() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| flag("LEEP_EXP_REPLAY_BLOCKING"))
}

/// One line for the server's startup log.
pub fn describe() -> String {
    format!(
        "leader_runtime={} leader_threads={} replay_blocking={}",
        leader_runtime().is_some(),
        leader_threads(),
        replay_blocking()
    )
}
