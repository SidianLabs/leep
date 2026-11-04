//! Check results and known-good refs (ADR 0091): the checks policy file, the
//! `green/<ref>` land policy, and the `/v1` handlers.
//!
//! The policy file names, per ref, the checks that must pass before the
//! server moves `green/<ref>`. It is read at startup and on SIGHUP (ADR 0024)
//! and is strict like the others:
//!
//! ```json
//! { "refs": { "main": { "required": ["test", "clippy"] } } }
//! ```
//!
//! Who may post a check is the tokens file's business (`"checks"`), never the
//! request body's. The repository logic is in `leep_derive` (`checks.rs`).

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path as FsPath;

use async_trait::async_trait;
use axum::body::Bytes;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use leep_core::api::{error_code, ChecksResponse, PostCheckRequest, PostCheckResponse};
use leep_core::check::{
    check_name, green_ref, is_green, Check, Outcome, PROMOTE_OPERATION, PROMOTION_ONLY_MARKER,
};
use leep_core::review_request::{CreatedBy, CREATED_BY_KIND};
use leep_core::{ChangesetId, Error, RefUpdateContext};
use leep_derive::{LandPolicy, Repo};
use serde::Deserialize;

use crate::{auth, cid, parse_json, ApiResult, Cfg, S};

const NEEDS_AUTH: &str = "check results need authentication (start the server with --tokens-file)";

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PolicyFile {
    #[serde(default)]
    refs: BTreeMap<String, RefPolicy>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RefPolicy {
    required: Vec<String>,
}

/// The parsed checks policy: the checks each ref requires before its
/// `green/<ref>` moves.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ChecksPolicy {
    /// Ref name to its required check names, sorted and unique.
    pub refs: BTreeMap<String, Vec<String>>,
}

impl ChecksPolicy {
    pub fn parse(text: &str) -> Result<ChecksPolicy, String> {
        let file: PolicyFile = serde_json::from_str(text).map_err(|e| format!("bad JSON: {e}"))?;
        let mut refs = BTreeMap::new();
        for (name, p) in file.refs {
            crate::check_protected_refs("checks policy", std::slice::from_ref(&name))?;
            if is_green(&name) || leep_core::refname::is_tag(&name) {
                return Err(format!(
                    "ref {name:?}: known-good refs follow branches, not tags or other \
                     known-good refs"
                ));
            }
            if p.required.is_empty() {
                return Err(format!("ref {name:?}: required lists no checks"));
            }
            let mut required = p.required;
            for c in &required {
                check_name(c).map_err(|e| {
                    format!(
                        "ref {name:?}: {}",
                        e.to_string().trim_start_matches("invalid input: ")
                    )
                })?;
            }
            required.sort();
            required.dedup();
            refs.insert(name, required);
        }
        Ok(ChecksPolicy { refs })
    }

    pub fn from_file(path: &FsPath) -> Result<ChecksPolicy, String> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| format!("cannot read checks policy file {}: {e}", path.display()))?;
        ChecksPolicy::parse(&text)
            .map_err(|e| format!("checks policy file {}: {e}", path.display()))
    }

    /// The checks `name` requires; empty when it has no known-good ref.
    pub fn required(&self, name: &str) -> &[String] {
        self.refs.get(name).map_or(&[], Vec::as_slice)
    }

    /// Every required check name, sorted.
    pub fn check_names(&self) -> Vec<String> {
        let all: BTreeSet<&String> = self.refs.values().flatten().collect();
        all.into_iter().cloned().collect()
    }

    /// The known-good refs, sorted.
    pub fn green_refs(&self) -> Vec<String> {
        self.refs.keys().map(|r| green_ref(r)).collect()
    }
}

/// Covers `green/<ref>` for every ref of the checks policy: forward-only and
/// undeletable like any protected ref, and moved by nobody but the
/// promotion, so `cas_ref_checked` (raw moves, `leep undo`, the git import)
/// and `land` refuse them with 422 `promotion_only`.
pub struct GreenRefs {
    refs: BTreeSet<String>,
}

impl GreenRefs {
    pub fn new(refs: impl IntoIterator<Item = String>) -> Self {
        GreenRefs {
            refs: refs.into_iter().collect(),
        }
    }
}

#[async_trait]
impl LandPolicy for GreenRefs {
    fn applies_to(&self, target_ref: &str) -> bool {
        self.refs.contains(target_ref)
    }

    fn protects(&self, target_ref: &str) -> bool {
        self.applies_to(target_ref)
    }

    fn promotion_only(&self, target_ref: &str) -> bool {
        self.applies_to(target_ref)
    }

    async fn check_land(
        &self,
        _repo: &Repo,
        target_ref: &str,
        _head: Option<ChangesetId>,
        _new_history: &[ChangesetId],
    ) -> leep_core::Result<()> {
        Err(Error::Policy(format!(
            "cannot land on {target_ref:?}: it {PROMOTION_ONLY_MARKER} when every required \
             check passes (ADR 0091); land on the ref it follows"
        )))
    }
}

/// Every check name a result can have: the policy's required ones and every
/// token's allow-list. Results are looked up by name, one row each.
fn known_names(config: &crate::reload::RuntimeConfig) -> Vec<String> {
    let mut all: BTreeSet<String> = BTreeSet::new();
    if let Some(p) = &config.checks {
        all.extend(p.check_names());
    }
    if let Some(a) = &config.auth {
        all.extend(a.check_names());
    }
    all.into_iter().collect()
}

fn forbidden(why: &str) -> Response {
    auth::reject(StatusCode::FORBIDDEN, error_code::FORBIDDEN, why)
}

pub(crate) async fn post(
    State(repo): S,
    Extension(config): Cfg,
    auth::MaybePrincipal(who): auth::MaybePrincipal,
    axum::extract::Path(id): axum::extract::Path<String>,
    body: Bytes,
) -> ApiResult<Response> {
    let id = cid(&id)?;
    let req: PostCheckRequest = parse_json(&body)?;
    let Some(who) = who else {
        return Err(Error::Invalid(NEEDS_AUTH.into()).into());
    };
    // Before any read: who may say what is the tokens file's call.
    if !who.checks.contains(&req.check) {
        return Ok(forbidden(&format!(
            "token may not post check {:?}: its tokens-file entry's \"checks\" does not list it",
            req.check
        )));
    }
    let check = Check {
        check: req.check,
        outcome: req.outcome,
        by: who.name.clone(),
        url: req.url,
        summary: req.summary,
        timestamp: leep_core::store::unix_now(),
    };
    check.validate()?;
    repo.get_changeset(id).await?;
    // A token never attests what it created, even one CI also commits with,
    // nor the copy the server made of it when it landed.
    let mut made = vec![id];
    made.extend(repo.rebase_chain(id).await?);
    let keys: Vec<leep_core::Hash> = made.iter().map(|c| c.0).collect();
    let created = repo.meta().get_derived_many(CREATED_BY_KIND, &keys).await?;
    if created
        .into_iter()
        .flatten()
        .filter_map(|b| CreatedBy::decode(&b).ok())
        .any(|c| c.principal == who.name)
    {
        return Ok(forbidden(
            "a token may not post check results for a changeset it created",
        ));
    }
    repo.put_check(id, &check).await?;
    let mut promoted = Vec::new();
    if check.outcome == Outcome::Pass {
        if let Some(policy) = &config.checks {
            let ctx = RefUpdateContext::new(PROMOTE_OPERATION)
                .actor(Some(who.name.as_str()))
                .message(check.check.as_str());
            for (name, required) in &policy.refs {
                if !required.contains(&check.check) {
                    continue;
                }
                if let Some(p) = repo.promote_green(name, id, required, &ctx).await? {
                    tracing::info!(
                        green = %p.green_ref,
                        to = %p.to,
                        by = %who.name,
                        "promoted known-good ref"
                    );
                    promoted.push(p);
                }
            }
        }
    }
    Ok(Json(PostCheckResponse {
        check: check.check,
        outcome: check.outcome,
        promoted,
    })
    .into_response())
}

pub(crate) async fn list(
    State(repo): S,
    Extension(config): Cfg,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> ApiResult<Json<ChecksResponse>> {
    let id = cid(&id)?;
    repo.get_changeset(id).await?;
    let names = known_names(&config);
    let mut checks: Vec<_> = repo
        .checks_by(&[id], &names)
        .await?
        .remove(&id)
        .unwrap_or_default()
        .into_iter()
        .map(Into::into)
        .collect();
    checks.sort_by(|a: &leep_core::api::CheckEntry, b| a.check.cmp(&b.check));
    let refs: Vec<String> = config
        .checks
        .as_ref()
        .map(|p| p.refs.keys().cloned().collect())
        .unwrap_or_default();
    Ok(Json(ChecksResponse {
        checks,
        green_on: repo.green_on(id, &refs).await?,
    }))
}

/// `GET /v1/refs/{name}/checks`.
pub(crate) async fn for_ref(
    repo: &Repo,
    config: &crate::reload::RuntimeConfig,
    name: &str,
) -> ApiResult<Response> {
    let required = config
        .checks
        .as_ref()
        .map(|p| p.required(name).to_vec())
        .unwrap_or_default();
    let names = if required.is_empty() {
        known_names(config)
    } else {
        required.clone()
    };
    Ok(Json(repo.ref_checks(name, &required, &names).await?).into_response())
}

#[derive(Deserialize)]
pub(crate) struct CulpritsQuery {
    bad: Option<String>,
    good: Option<String>,
}

/// `GET /v1/refs/{name}/culprits?bad=rev&good=rev`: the suspects, the lands
/// in the range, never a verdict on one of them.
pub(crate) async fn culprits(repo: &Repo, name: &str, q: CulpritsQuery) -> ApiResult<Response> {
    let bad = match q.bad.as_deref().filter(|b| !b.is_empty()) {
        Some(rev) => Some(repo.resolve(rev).await?),
        None => None,
    };
    let good = match q.good.as_deref().filter(|g| !g.is_empty()) {
        Some(rev) => Some(repo.resolve(rev).await?),
        None => None,
    };
    Ok(Json(repo.culprits(name, bad, good).await?).into_response())
}

/// The query of a `GET /v1/refs/...` that may be a culprits request.
pub(crate) type MaybeCulprits = Query<CulpritsQuery>;
