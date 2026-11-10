//! `POST /v1/context` (ADR 0097): a context pack for a task.
//!
//! A query, not a write: a read token may ask (`auth::needs_write`), it
//! takes a `search` read-class permit like a code search (it runs one),
//! and the per-token limits of ADR 0020 apply as to any request. The
//! caller's session is the body's `session`, else `Leep-Session`. The
//! required checks come from the checks policy the request started with.
//! The assembly is `leep_derive::context`.

use axum::body::Bytes;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::{Extension, Json};
use leep_core::api::{ContextPack, ContextRequest};
use leep_derive::context::ContextAsk;

use crate::{auth, parse_json, ApiResult, Cfg, S};

pub(crate) async fn pack(
    State(repo): S,
    Extension(config): Cfg,
    auth::MaybePrincipal(who): auth::MaybePrincipal,
    headers: HeaderMap,
    body: Bytes,
) -> ApiResult<Json<ContextPack>> {
    let mut req: ContextRequest = parse_json(&body)?;
    if req.session.as_deref().is_none_or(|s| s.trim().is_empty()) {
        req.session = crate::sessions::session_key(&headers);
    }
    let target = req
        .target_ref
        .as_deref()
        .map(str::trim)
        .filter(|r| !r.is_empty())
        .unwrap_or("main");
    let required_checks = config
        .checks
        .as_ref()
        .map(|p| p.required(target).to_vec())
        .unwrap_or_default();
    let ask = ContextAsk {
        principal: crate::sessions::principal(&who),
        req,
        required_checks,
        check_names: crate::checks::known_names(&config),
    };
    Ok(Json(repo.context_pack(&ask).await?))
}
