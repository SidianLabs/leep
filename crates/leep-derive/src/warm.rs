//! Background index warming (ADR 0028). Derived kinds are computed on first
//! use, so the first path query on a freshly imported history used to derive
//! the whole path-history index inside one request. The warmer derives the
//! indexes for every ref head as refs move, so queries find them ready.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use leep_core::{ChangesetId, Result};

use crate::derived::{self, HistoryTree, LogFilterIndex};
use crate::Repo;

/// What one [`Warmer::pass`] did.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WarmReport {
    /// Refs looked at.
    pub refs: usize,
    /// Refs whose head was new and got its indexes derived.
    pub warmed: usize,
    /// Refs whose warming failed; they are retried on the next pass.
    pub failed: usize,
}

/// Derives the indexes of ref heads that changed since the last pass.
pub struct Warmer {
    repo: Repo,
    search: bool,
    warmed: HashMap<String, ChangesetId>,
}

impl Warmer {
    /// `search` also derives the code-search index of each head.
    pub fn new(repo: Repo, search: bool) -> Self {
        Warmer {
            repo,
            search,
            warmed: HashMap::new(),
        }
    }

    /// Warm every ref whose head moved since the last pass. Heads of different
    /// refs share most of their history, and derivation skips what is
    /// already stored, so later refs cost only their own changesets.
    pub async fn pass(&mut self) -> Result<WarmReport> {
        let refs = self.repo.meta().list_refs().await?;
        self.warmed.retain(|name, _| refs.contains_key(name));
        let mut report = WarmReport {
            refs: refs.len(),
            ..Default::default()
        };
        for (name, id) in refs {
            if self.warmed.get(&name) == Some(&id) {
                continue;
            }
            let started = Instant::now();
            match warm(&self.repo, id, self.search).await {
                Ok(()) => {
                    let took = started.elapsed();
                    metrics::counter!("leep_warm_refs_total", "result" => "ok").increment(1);
                    metrics::histogram!("leep_warm_duration_seconds").record(took.as_secs_f64());
                    if took > Duration::from_secs(1) {
                        tracing::info!(r#ref = %name, head = %id, seconds = took.as_secs_f64(), "indexes warmed");
                    }
                    self.warmed.insert(name, id);
                    report.warmed += 1;
                }
                Err(e) => {
                    metrics::counter!("leep_warm_refs_total", "result" => "error").increment(1);
                    tracing::warn!(r#ref = %name, head = %id, error = %e, "warming indexes failed; retrying next pass");
                    report.failed += 1;
                }
            }
        }
        Ok(report)
    }

    /// Run passes forever, `interval` apart, starting now.
    pub fn spawn(mut self, interval: Duration) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                if let Err(e) = self.pass().await {
                    tracing::warn!(error = %e, "listing refs to warm failed");
                }
                tokio::time::sleep(interval).await;
            }
        })
    }
}

/// Derive the indexes queries need for `id`: the path-history index (which
/// brings generation, skiplists and last-merge with it), the log-filter
/// index and, with `search`, the code-search index of its tree.
async fn warm(repo: &Repo, id: ChangesetId, search: bool) -> Result<()> {
    derived::derive::<HistoryTree>(repo, id).await?;
    derived::derive::<LogFilterIndex>(repo, id).await?;
    if search {
        repo.derive_search(id).await?;
    }
    Ok(())
}
