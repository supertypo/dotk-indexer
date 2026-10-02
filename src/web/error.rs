use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Serialize;
use utoipa::{IntoResponses, ToSchema};

/// Why a request failed, as a closed set a client can branch on instead of parsing `error`.
/// Each code implies its HTTP status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub(super) enum ErrorCode {
    /// Not a name this registry can hold.
    InvalidName,
    /// Not a Kaspa address, or one whose kind is no owner scheme.
    InvalidAddress,
    /// Not 64 lowercase hex characters: a name key, an owner payload or a spender payload.
    InvalidKey,
    /// Not a scheme this operation accepts.
    InvalidOwnerType,
    /// The query string does not parse, or a value is outside its documented range.
    InvalidQuery,
    /// The name is not registered. Only `/names/{name}` answers this. A free key or an owner
    /// without names is a 200, not this.
    NotFound,
    /// No proven answer exists yet, because the indexer is catching up or the last self-test
    /// did not prove this part. A state, not a fault.
    NotReady,
    /// The last proof is older than twice the self-test interval, so proving is broken.
    StaleProof,
    /// The database or a derivation failed. The only code that blames this indexer.
    Internal,
}

impl ErrorCode {
    fn status(self) -> StatusCode {
        match self {
            Self::InvalidName | Self::InvalidAddress | Self::InvalidKey | Self::InvalidOwnerType | Self::InvalidQuery => {
                StatusCode::BAD_REQUEST
            }
            Self::NotFound => StatusCode::NOT_FOUND,
            Self::NotReady | Self::StaleProof => StatusCode::SERVICE_UNAVAILABLE,
            Self::Internal => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

#[derive(Debug, Serialize, ToSchema)]
pub(super) struct ErrorResponse {
    /// One of a closed set, for branching.
    code: ErrorCode,
    /// The same failure in prose, for a person. Never parsed.
    error: String,
}

/// The 503 every registry operation answers before the first self-test verdict.
#[derive(IntoResponses)]
#[response(status = StatusCode::SERVICE_UNAVAILABLE, description = "No self-test verdict yet")]
#[expect(dead_code, reason = "schema only")]
pub(super) struct NotReady(ErrorResponse);

/// The 500 every operation that reads the database can answer.
#[derive(IntoResponses)]
#[response(status = StatusCode::INTERNAL_SERVER_ERROR, description = "Reading the database failed")]
#[expect(dead_code, reason = "schema only")]
pub(super) struct Internal(ErrorResponse);

pub(super) struct ApiError {
    code: ErrorCode,
    msg: String,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.code.status(), axum::Json(ErrorResponse { code: self.code, error: self.msg })).into_response()
    }
}

pub(super) fn err(code: ErrorCode, msg: impl Into<String>) -> ApiError {
    ApiError { code, msg: msg.into() }
}

/// Logs the detail and serves a generic message, because the detail describes the operator's
/// machine on a public endpoint.
pub(super) fn internal(context: &str, detail: impl std::fmt::Display) -> ApiError {
    log::error!("{context}: {detail:#}");
    err(ErrorCode::Internal, "the indexer cannot answer this request")
}
