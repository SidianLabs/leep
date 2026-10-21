//! Error bodies: the stable `code` of every error (`leep_core::api`, "Error
//! codes"), and JSON bodies for the errors axum and tower answer in plain
//! text (the global body cap, the request timeout, an unknown route), so
//! every error a client sees is an [`ErrorResponse`].

use axum::body::Body;
use axum::extract::Request;
use axum::http::{header, HeaderValue, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::Json;
use http_body_util::{BodyExt, Limited};
use leep_core::api::{
    error_code, ErrorResponse, CURSOR_UNREPRESENTABLE_MARKER, FORWARD_FAILED_MARKER,
};
use leep_core::refname::{MISSING_REF_MARKER, NOT_FORWARD_MARKER};
use leep_core::review::NEEDS_REVIEW_MARKER;
use leep_core::signature::NOT_SIGNED_MARKER;
use leep_core::Error;

/// Texts of the tag errors `leep-derive` raises: a land onto a tag, and a
/// tag move.
const TAG_IMMUTABLE_MARKERS: &[&str] = &["tags are immutable", "is immutable and cannot be moved"];

/// Text of the error for a land id the ref recorded for another changeset.
const LAND_ID_REUSED_MARKER: &str = "; use a new land id";

/// Text of every [`leep_core::refname::validate`] refusal.
const INVALID_NAME_MARKER: &str = "invalid ref name ";

/// Longest plain-text error body kept as the `error` of its JSON form.
const MAX_TEXT_ERROR: usize = 4096;

/// The `code` of `e`, by variant and, for the policy refusals that share a
/// variant, by the stable marker text in the message. User text in these
/// messages is quoted ref names, which cannot hold a marker.
pub(crate) fn code_of(e: &Error) -> &'static str {
    match e {
        Error::Invalid(m) => {
            // First: this message quotes a path, which may hold other markers.
            if m.ends_with(CURSOR_UNREPRESENTABLE_MARKER) {
                error_code::CURSOR_UNREPRESENTABLE
            } else if m.contains(NEEDS_REVIEW_MARKER) {
                error_code::NEEDS_REVIEW
            } else if m.contains(NOT_SIGNED_MARKER) {
                error_code::NOT_SIGNED
            } else if m.contains(NOT_FORWARD_MARKER) {
                error_code::NOT_FORWARD
            } else if TAG_IMMUTABLE_MARKERS.iter().any(|t| m.contains(t)) {
                error_code::TAG_IMMUTABLE
            } else if m.contains("land id ") && m.contains(LAND_ID_REUSED_MARKER) {
                error_code::LAND_ID_REUSED
            } else if m.contains(INVALID_NAME_MARKER) {
                error_code::INVALID_NAME
            } else {
                error_code::INVALID
            }
        }
        Error::LandIdReused { .. } => error_code::LAND_ID_REUSED,
        Error::InvalidPath(..) | Error::InvalidHash(_) => error_code::INVALID,
        Error::NotFound(m) if m.contains(MISSING_REF_MARKER) => error_code::REF_MISSING,
        Error::NotFound(_) => error_code::NOT_FOUND,
        Error::Conflict(m) if m.contains(FORWARD_FAILED_MARKER) => error_code::FORWARD_FAILED,
        Error::Conflict(_) => error_code::CAS_LOST,
        Error::Indexing { .. } => error_code::INDEXING,
        Error::Encoding(_) | Error::Storage(_) | Error::Other(_) => error_code::INTERNAL,
    }
}

/// The code of an error answered without one: by status alone.
pub(crate) fn code_of_status(status: StatusCode) -> &'static str {
    match status {
        StatusCode::UNAUTHORIZED => error_code::UNAUTHORIZED,
        StatusCode::FORBIDDEN => error_code::FORBIDDEN,
        StatusCode::NOT_FOUND => error_code::NOT_FOUND,
        StatusCode::REQUEST_TIMEOUT => error_code::TIMEOUT,
        StatusCode::CONFLICT => error_code::CAS_LOST,
        StatusCode::PAYLOAD_TOO_LARGE => error_code::TOO_LARGE,
        StatusCode::TOO_MANY_REQUESTS => error_code::RATE_LIMITED,
        StatusCode::SERVICE_UNAVAILABLE => error_code::UNAVAILABLE,
        s if s.is_server_error() => error_code::INTERNAL,
        _ => error_code::INVALID,
    }
}

pub(crate) fn is_json(resp: &Response) -> bool {
    resp.headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.starts_with("application/json"))
}

/// Axum middleware giving a plain-text or empty error answer an
/// [`ErrorResponse`] body with its code. JSON answers pass untouched.
pub async fn json_errors(req: Request, next: Next) -> Response {
    let resp = next.run(req).await;
    let status = resp.status();
    if !(status.is_client_error() || status.is_server_error()) || is_json(&resp) {
        return resp;
    }
    let (mut parts, body) = resp.into_parts();
    let text = match Limited::new(body, MAX_TEXT_ERROR).collect().await {
        Ok(b) => String::from_utf8_lossy(&b.to_bytes()).trim().to_string(),
        Err(_) => String::new(),
    };
    let error = if text.is_empty() {
        status
            .canonical_reason()
            .unwrap_or("error")
            .to_ascii_lowercase()
    } else {
        text
    };
    let body = ErrorResponse::new(error).with_code(code_of_status(status));
    parts.headers.remove(header::CONTENT_LENGTH);
    parts.headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    let json = Json(body).into_response().into_body();
    Response::from_parts(parts, Body::new(json))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn variants_and_markers_pick_codes() {
        let inv = |m: &str| code_of(&Error::Invalid(m.into()));
        assert_eq!(
            inv(&format!("changeset x {NEEDS_REVIEW_MARKER}: path a")),
            "needs_review"
        );
        assert_eq!(
            inv(&format!("changeset x {NOT_SIGNED_MARKER}")),
            "not_signed"
        );
        assert_eq!(
            inv(&format!(
                "cannot move \"main\": ... {NOT_FORWARD_MARKER}; ..."
            )),
            "not_forward"
        );
        assert_eq!(
            inv("cannot land onto \"tags/v1\": tags are immutable"),
            "tag_immutable"
        );
        assert_eq!(
            inv("tag \"tags/v1\" is immutable and cannot be moved; delete it"),
            "tag_immutable"
        );
        assert_eq!(
            inv("land id ab already landed cd; use a new land id to land ef"),
            "land_id_reused"
        );
        assert_eq!(
            code_of(&Error::LandIdReused {
                refusal: Box::new(leep_core::api::LandRefusal {
                    land_id: "00112233445566778899aabbccddeeff".parse().unwrap(),
                    changeset: leep_core::ChangesetId(leep_core::Hash([1; 32])),
                    landed: None,
                }),
                requested: leep_core::ChangesetId(leep_core::Hash([2; 32])),
            }),
            "land_id_reused"
        );
        assert_eq!(inv("invalid ref name \"a..b\": has .."), "invalid_name");
        assert_eq!(inv("bad json: eof"), "invalid");
        assert_eq!(
            inv(&format!(
                "path \"tags are immutable\" {CURSOR_UNREPRESENTABLE_MARKER}"
            )),
            "cursor_unrepresentable"
        );
        assert_eq!(
            inv(&format!("ref \"{CURSOR_UNREPRESENTABLE_MARKER}\" bad")),
            "invalid"
        );
        assert_eq!(
            code_of(&Error::NotFound(format!("ref \"x\" {MISSING_REF_MARKER}"))),
            "ref_missing"
        );
        assert_eq!(code_of(&Error::NotFound("x".into())), "not_found");
        assert_eq!(code_of(&Error::Conflict("x".into())), "cas_lost");
        assert_eq!(
            code_of(&Error::Conflict(format!(
                "land onto \"main\" {FORWARD_FAILED_MARKER}; retry"
            ))),
            "forward_failed"
        );
        assert_eq!(
            code_of_status(StatusCode::SERVICE_UNAVAILABLE),
            "unavailable"
        );
        assert_eq!(code_of_status(StatusCode::BAD_GATEWAY), "internal");
        assert_eq!(code_of(&Error::Storage("x".into())), "internal");
        assert_eq!(code_of(&Error::InvalidPath("p".into(), "why")), "invalid");
        for s in [
            StatusCode::BAD_REQUEST,
            StatusCode::UNAUTHORIZED,
            StatusCode::FORBIDDEN,
            StatusCode::NOT_FOUND,
            StatusCode::METHOD_NOT_ALLOWED,
            StatusCode::REQUEST_TIMEOUT,
            StatusCode::CONFLICT,
            StatusCode::PAYLOAD_TOO_LARGE,
            StatusCode::TOO_MANY_REQUESTS,
            StatusCode::INTERNAL_SERVER_ERROR,
            StatusCode::SERVICE_UNAVAILABLE,
        ] {
            assert!(error_code::ALL.contains(&code_of_status(s)), "{s}");
        }
    }

    #[tokio::test]
    async fn not_leader_has_its_code() {
        let resp = crate::coord::not_leader();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let e: ErrorResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(e.code.as_deref(), Some("not_leader"));
    }
}
