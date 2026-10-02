use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Request, State};
use axum::http::{HeaderValue, Method, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use tower_http::cors::{AllowOrigin, Any, CorsLayer};

use super::error::internal;
use super::server::api_base;
use crate::app::{App, Tagged};

/// What a handler wants in `Cache-Control`, carried in the response extensions. Handlers
/// declare it because the path cannot tell `/snapshot` from `/snapshot?proven=false`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CachePolicy {
    NoStore,
    Ttl(Duration),
}

pub(super) fn cached(mut res: Response, policy: CachePolicy) -> Response {
    res.extensions_mut().insert(policy);
    res
}

/// Subtracts the memo's age from the TTL, so a downstream cache never holds the bytes for up to
/// twice the window.
pub(super) fn live_cache_policy(ttl: Duration, age_ms: u64) -> CachePolicy {
    CachePolicy::Ttl(ttl.saturating_sub(Duration::from_millis(age_ms)))
}

pub(super) fn tagged_json(body: &Tagged) -> Response {
    ([(header::CONTENT_TYPE, HeaderValue::from_static("application/json")), (header::ETAG, body.etag.clone())], body.bytes.clone())
        .into_response()
}

/// RFC 9110 §13.1.2: `*` or a list of entity tags, compared weakly, so `W/"x"` selects `"x"`.
fn if_none_match_selects(header: &HeaderValue, etag: &HeaderValue) -> bool {
    let Ok(header) = header.to_str() else { return false };
    let Ok(etag) = etag.to_str() else { return false };
    header.split(',').map(str::trim).any(|candidate| candidate == "*" || candidate.trim_start_matches("W/") == etag)
}

pub(super) async fn cache_control_mw(State(app): State<Arc<App>>, req: Request, next: Next) -> Response {
    let if_none_match = req.headers().get(header::IF_NONE_MATCH).cloned();
    let mut res = next.run(req).await;
    // An undeclared error is never edge-cached, because a cached 404 or 503 outlives its cause.
    let ttl = match res.extensions().get::<CachePolicy>() {
        Some(CachePolicy::Ttl(d)) => d.as_secs(),
        None if res.status().is_success() => app.deployment.args.cache_ttl.as_secs(),
        Some(CachePolicy::NoStore) | None => 0,
    };
    let value = if ttl == 0 {
        HeaderValue::from_static("no-store")
    } else {
        HeaderValue::from_str(&format!("public, max-age={ttl}")).expect("ASCII text is a header value")
    };
    res.headers_mut().insert(header::CACHE_CONTROL, value);

    // After `Cache-Control`, because RFC 9110 §15.4.5 requires a 304 to carry the same caching
    // headers as the 200.
    if res.status() == StatusCode::OK
        && let Some(etag) = res.headers().get(header::ETAG).cloned()
        && if_none_match.is_some_and(|header| if_none_match_selects(&header, &etag))
    {
        let mut not_modified = Response::new(axum::body::Body::empty());
        *not_modified.status_mut() = StatusCode::NOT_MODIFIED;
        not_modified.headers_mut().insert(header::ETAG, etag);
        if let Some(cache_control) = res.headers().get(header::CACHE_CONTROL) {
            not_modified.headers_mut().insert(header::CACHE_CONTROL, cache_control.clone());
        }
        return not_modified;
    }
    res
}

/// `allow_credentials` is never set, because the API uses no cookies or auth and it conflicts
/// with `Any`.
pub(super) fn cors_layer(spec: &str) -> CorsLayer {
    let origins = if spec.trim() == "*" {
        AllowOrigin::any()
    } else {
        // The argument parser already rejects anything that is not `scheme://host[:port]`.
        AllowOrigin::list(spec.split(',').filter_map(|o| HeaderValue::from_str(o.trim()).ok()))
    };
    CorsLayer::new()
        .allow_origin(origins)
        .allow_methods([Method::GET])
        .allow_headers(Any)
        .expose_headers([header::ETAG])
        .max_age(Duration::from_hours(24))
}

pub(super) async fn cache_control_default_mw(req: Request, next: Next) -> Response {
    let mut res = next.run(req).await;
    if !res.headers().contains_key(header::CACHE_CONTROL) {
        res.headers_mut().insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    }
    res
}

/// A request over its budget answers the API's error body, because the client is not the slow
/// party.
pub(super) async fn request_timeout_mw(State(app): State<Arc<App>>, req: Request, next: Next) -> Response {
    let path = req.uri().path().to_string();
    // The memo routes wait for a build that can run to the build cap.
    let api = api_base(&app.deployment.args.base_path);
    let budget = if path == format!("{api}/keyspace") || path == format!("{api}/snapshot") {
        app.deployment.args.web_request_timeout.max(app.deployment.args.web_build_timeout)
    } else {
        app.deployment.args.web_request_timeout
    };
    match tokio::time::timeout(budget, next.run(req)).await {
        Ok(response) => response,
        Err(_) => internal(&format!("answering {path}"), format!("no answer within {} s", budget.as_secs())).into_response(),
    }
}

#[cfg(test)]
#[path = "middleware_tests.rs"]
mod tests;
