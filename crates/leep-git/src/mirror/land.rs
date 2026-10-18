//! Land branches (ADR 0049): a person pushes `<prefix><ref>/<name>` to the
//! remote, and the mirror imports it and lands it onto the Leep ref `<ref>`
//! through `POST /v1/land`, which rebases the branch onto the head exactly as
//! an agent's land is rebased. A landed branch is deleted with a lease on the
//! value that landed; the normal export then pushes the result to `<ref>`'s
//! git branch as a fast-forward. Conflicts and refusals are reported and the
//! branch is left where it is.
//!
//! Crash safety needs no record of progress: importing is content addressed,
//! a land of a changeset the ref already holds (or holds a rebased copy of)
//! answers with what is there instead of landing it again, and a delete with
//! a lease only removes the value that landed.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use gix::ObjectId;

use super::git::{tracking_ref, PushOutcome};
use super::status::{self, LandBranchStatus, LandState};
use super::{crash_point, import, list_paths, log, LandOutcome, Mirror};

pub(super) struct LandBranch {
    pub commit: ObjectId,
    /// The Leep ref it lands on, if its name matches a mirrored ref.
    target: Option<String>,
    next_try: Instant,
    failures: u32,
    /// The last result counted in the target's counters, so a retry of the
    /// same commit is not counted twice.
    counted: Option<LandState>,
    pub st: LandBranchStatus,
}

impl Mirror<'_> {
    /// The Leep ref a land branch names: the longest mirrored branch `R`
    /// with the branch under `<prefix>R/`.
    fn land_target(&self, branch: &str) -> Option<String> {
        let prefix = self.cfg.land_prefix.as_deref()?;
        let rest = branch.strip_prefix("refs/heads/")?.strip_prefix(prefix)?;
        self.refs
            .iter()
            .filter(|r| !r.spec.is_tag())
            .map(|r| r.spec.leep.as_str())
            .filter(|n| {
                rest.strip_prefix(n)
                    .and_then(|t| t.strip_prefix('/'))
                    .is_some_and(|t| !t.is_empty())
            })
            .max_by_key(|n| n.len())
            .map(str::to_string)
    }

    /// Take the land branches the remote has now. A new value is tried at
    /// once; a branch that is gone is forgotten.
    pub(super) fn update_lands(&mut self, found: BTreeMap<String, ObjectId>) {
        let gone: Vec<String> = self
            .lands
            .keys()
            .filter(|n| !found.contains_key(*n))
            .cloned()
            .collect();
        for name in gone {
            self.lands.remove(&name);
            let _ = self.remote.delete_local_ref(&tracking_ref(&name));
        }
        for (name, commit) in found {
            if self.lands.get(&name).is_some_and(|b| b.commit == commit) {
                continue;
            }
            let target = self.land_target(&name);
            log(format!(
                "{name}: land branch at {commit}{}",
                target
                    .as_ref()
                    .map(|t| format!(" for {t}"))
                    .unwrap_or_default()
            ));
            self.lands.insert(
                name.clone(),
                LandBranch {
                    commit,
                    st: LandBranchStatus {
                        branch: name,
                        leep_ref: target.clone(),
                        commit: commit.to_string(),
                        state: LandState::Pending,
                        detail: String::new(),
                        since: status::now(),
                    },
                    target,
                    next_try: Instant::now(),
                    failures: 0,
                    counted: None,
                },
            );
        }
    }

    /// Try every land branch that is due.
    pub(super) fn sync_lands(&mut self) {
        if self.cfg.land_prefix.is_none() {
            return;
        }
        let due: Vec<String> = self
            .lands
            .iter()
            .filter(|(_, b)| Instant::now() >= b.next_try)
            .map(|(n, _)| n.clone())
            .collect();
        for name in due {
            if let Err(e) = self.sync_land(&name) {
                self.count_error("import");
                let max = self.cfg.max_backoff;
                if let Some(b) = self.lands.get_mut(&name) {
                    b.failures += 1;
                    set_land_state(b, LandState::Error, format!("{e:#}"));
                    let secs = 1u64 << b.failures.min(16);
                    b.next_try = Instant::now() + Duration::from_secs(secs).min(max);
                }
            }
        }
    }

    fn sync_land(&mut self, name: &str) -> Result<()> {
        let (commit, target) = {
            let b = &self.lands[name];
            (b.commit, b.target.clone())
        };
        let Some(target) = target else {
            let prefix = self.cfg.land_prefix.clone().unwrap_or_default();
            let refs: Vec<&str> = self
                .refs
                .iter()
                .filter(|r| !r.spec.is_tag())
                .map(|r| r.spec.leep.as_str())
                .collect();
            let b = self.lands.get_mut(name).expect("listed");
            set_land_state(
                b,
                LandState::Refused,
                format!(
                    "names no mirrored ref: push to {prefix}<ref>/<name>, where <ref> is one of {}",
                    refs.join(", ")
                ),
            );
            // Only a new value (or a restart with other refs) changes this.
            b.next_try = Instant::now() + Duration::from_secs(3600);
            return Ok(());
        };
        if !self.ex.repo.has_object(commit) {
            // Fetched by the next remote poll.
            self.remote_stale = true;
            return Ok(());
        }
        let report = import::import(self.leep, &mut self.ex, commit).context("import")?;
        self.status.lock().unwrap().commits_imported += report.commits;
        let head = report
            .head
            .ok_or_else(|| anyhow!("import: {commit} has no changeset after import"))?;
        crash_point("land-imported");
        let outcome = self
            .leep
            .land(head, &target)
            .with_context(|| format!("import: landing {name} onto {target}"))?;
        let max = self.cfg.max_backoff;
        match outcome {
            LandOutcome::Landed(landed) => {
                crash_point("land-landed");
                self.count_land(name, &target, LandState::Pending);
                log(format!(
                    "{name}: landed {commit} onto {target} as {landed} ({} new changesets)",
                    report.commits
                ));
                // Leep moved: read its refs next cycle and export at once.
                self.seq = None;
                if let Some(r) = self.refs.iter_mut().find(|r| r.spec.leep == target) {
                    r.next_try = Instant::now();
                }
                match self.remote.push(name, None, Some(Some(commit))) {
                    Ok(PushOutcome::Ok) => {
                        crash_point("land-deleted");
                        self.lands.remove(name);
                        let _ = self.remote.delete_local_ref(&tracking_ref(name));
                    }
                    Ok(other) => {
                        // Someone pushed to the branch meanwhile: the next
                        // poll sees the new value and lands what is new.
                        self.remote_stale = true;
                        let b = self.lands.get_mut(name).expect("listed");
                        set_land_state(
                            b,
                            LandState::Pending,
                            format!("landed as {landed}; deleting the branch: {other:?}"),
                        );
                    }
                    Err(e) => {
                        return Err(anyhow!("import: landed as {landed}; deleting {name}: {e}"))
                    }
                }
            }
            LandOutcome::Conflicts(paths) => {
                self.count_land(name, &target, LandState::Conflict);
                let git_branch = self
                    .refs
                    .iter()
                    .find(|r| r.spec.leep == target)
                    .map(|r| r.spec.git.trim_start_matches("refs/heads/").to_string())
                    .unwrap_or_else(|| target.clone());
                let b = self.lands.get_mut(name).expect("listed");
                set_land_state(
                    b,
                    LandState::Conflict,
                    format!(
                        "conflicts with {target} in {} {}; rebase it onto origin/{git_branch}, \
                         resolve, and push it again",
                        paths.len(),
                        list_paths(&paths)
                    ),
                );
                b.failures = 0;
                b.next_try = Instant::now() + max.min(Duration::from_secs(30));
            }
            LandOutcome::Refused(why) => {
                self.count_land(name, &target, LandState::Refused);
                let b = self.lands.get_mut(name).expect("listed");
                set_land_state(
                    b,
                    LandState::Refused,
                    format!("Leep refused to land it onto {target}: {why}"),
                );
                b.failures = 0;
                // Policy answers change slowly (a review, a new key).
                b.next_try = Instant::now() + max.min(Duration::from_secs(60));
            }
        }
        Ok(())
    }

    /// Count a result (`Pending` for landed) once per commit of a branch.
    fn count_land(&mut self, name: &str, target: &str, result: LandState) {
        let Some(b) = self.lands.get_mut(name) else {
            return;
        };
        if b.counted == Some(result) {
            return;
        }
        b.counted = Some(result);
        if let Some(r) = self.refs.iter_mut().find(|r| r.spec.leep == target) {
            let c = &mut r.st.land_branches;
            match result {
                LandState::Pending => c.landed += 1,
                LandState::Conflict => c.conflicts += 1,
                LandState::Refused => c.refused += 1,
                LandState::Error => {}
            }
        }
    }
}

fn set_land_state(b: &mut LandBranch, state: LandState, detail: String) {
    let detail = super::redact(&detail);
    if state != b.st.state || detail != b.st.detail {
        log(format!("{}: {}: {detail}", b.st.branch, state.label()));
    }
    b.st.state = state;
    b.st.detail = detail;
}
