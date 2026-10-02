use axum::extract::rejection::{PathRejection, QueryRejection};
use axum::extract::{Path, Query};
use dotk_core::state::OwnerType;

use super::error::{ApiError, ErrorCode, err};
use crate::app::Verdict;
use crate::convert::{hex32, unhex32};

pub(super) async fn ready(verdict: &Verdict) -> Result<(), ApiError> {
    if verdict.judged().await {
        Ok(())
    } else {
        Err(err(ErrorCode::NotReady, "no self-test verdict yet, the indexer is still catching up"))
    }
}

/// A bare `Path<T>` answers with axum's text/plain rejection.
pub(super) fn path<T>(raw: Result<Path<T>, PathRejection>, code: ErrorCode, what: &str) -> Result<T, ApiError> {
    match raw {
        Ok(Path(v)) => Ok(v),
        Err(e) => Err(err(code, format!("{what}: {}", e.body_text()))),
    }
}

/// A bare `Query<T>` answers with axum's text/plain rejection.
pub(super) fn checked<T>(query: Result<Query<T>, QueryRejection>) -> Result<T, ApiError> {
    match query {
        Ok(Query(q)) => Ok(q),
        Err(e) => Err(err(ErrorCode::InvalidQuery, format!("query string: {}", e.body_text()))),
    }
}

pub(super) fn valid_name(raw: &str) -> Result<String, ApiError> {
    let name = dotk_core::names::normalize(raw);
    dotk_core::names::validate(&name).map_err(|e| err(ErrorCode::InvalidName, format!("not a valid name: {e}")))?;
    Ok(name)
}

pub(super) fn page_limit(limit: Option<u32>, default: u32, max: u32) -> Result<u32, ApiError> {
    let limit = limit.unwrap_or(default);
    if limit == 0 || limit > max {
        return Err(err(ErrorCode::InvalidQuery, format!("limit must be between 1 and {max}")));
    }
    Ok(limit)
}

/// Accepts only the spelling every response uses (64 lowercase hex), because `AABB…` and
/// `aabb…` are one key but two URLs at the edge cache.
pub(super) fn path_key(raw: &str) -> Option<[u8; 32]> {
    let key = unhex32(raw).ok()?;
    (raw == hex32(&key)).then_some(key)
}

/// Plain decimal only. `u8::from_str` also accepts `00` and `+0`, which are more URLs for one
/// owner.
pub(super) fn path_owner_type(raw: &str) -> Option<OwnerType> {
    let byte: u8 = raw.parse().ok()?;
    (raw == byte.to_string()).then(|| OwnerType::from_byte(byte)).flatten()
}

pub(super) fn card_cursor(raw: &str) -> Option<([u8; 32], u32)> {
    let (txid, idx) = raw.split_once(':')?;
    let canonical = !idx.is_empty() && idx.bytes().all(|b| b.is_ascii_digit()) && (idx == "0" || !idx.starts_with('0'));
    let idx: u32 = idx.parse().ok().filter(|i| canonical && crate::convert::sql_i32(*i).is_ok())?;
    Some((path_key(txid)?, idx))
}
