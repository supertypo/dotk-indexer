//! The gateway answers `abc.<gateway-domain>` with a `302` redirect to the `url` record of `abc.k`,
//! never a proxy and never a `301`, because a `url` record changes with the card.

use std::fmt::Write;
use std::sync::{Arc, LazyLock};

use axum::extract::{Request, State};
use axum::http::{HeaderValue, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use dotk_core::cards::{self, RecordValue};

use super::error::internal;
use super::middleware::{CachePolicy, cached};
use super::pages::{LOGO_SVG, html};
use crate::app::App;
use crate::audit::{NameLookup, look_up_name};
use crate::db;

const GATEWAY_PAGE: &str = include_str!("../gateway.html");
/// The page inlines its logo and runs no script.
const GATEWAY_CSP: &str =
    "default-src 'none'; style-src 'unsafe-inline'; img-src data:; frame-ancestors 'none'; base-uri 'none'; form-action 'none'";

pub(super) async fn middleware(State(app): State<Arc<App>>, req: Request, next: Next) -> Response {
    if let Some(label) = gateway_label(&app.deployment.args.gateway_domain, req.headers().get(header::HOST)) {
        return answer(&app, &label).await;
    }
    next.run(req).await
}

/// The port is dropped first, because a TLS terminator can forward one. The bare domain is
/// `None`, because it belongs to the router.
fn gateway_label(domain: &str, host: Option<&HeaderValue>) -> Option<String> {
    if domain.is_empty() {
        return None;
    }
    let host = host?.to_str().ok()?.trim().to_ascii_lowercase();
    let host = host.split(':').next().unwrap_or_default().trim_end_matches('.');
    let label = host.strip_suffix(domain)?.strip_suffix('.')?;
    (!label.is_empty()).then(|| label.to_string())
}

async fn answer(app: &App, label: &str) -> Response {
    if dotk_core::names::validate(label).is_err() {
        // A dotted label is a subname, and a subname never carries a `url`.
        return page(app, StatusCode::NOT_FOUND, Page::NotAName, label);
    }
    if !app.verdict.judged().await {
        return page(app, StatusCode::SERVICE_UNAVAILABLE, Page::NotReady, label);
    }
    let mut conn = match app.backends.web_db.acquire().await {
        Ok(c) => c,
        Err(e) => return internal("acquiring a connection for the gateway", e).into_response(),
    };
    let key = match look_up_name(&app.verdict, &mut conn, label).await {
        Ok(NameLookup::Active(key, _)) => key,
        Ok(NameLookup::Held) => return page(app, StatusCode::NOT_FOUND, Page::Held, label),
        Ok(NameLookup::Unproven) => return page(app, StatusCode::SERVICE_UNAVAILABLE, Page::Unproven, label),
        Ok(NameLookup::Free) => return page(app, StatusCode::NOT_FOUND, Page::Free, label),
        Err(e) => return internal("looking a name up for the gateway", e).into_response(),
    };
    let target = match db::live_card_by_key(&mut conn, &key).await {
        Ok(hit) => hit.and_then(|h| cards::decode_records(&h.card.blob).ok()).and_then(|r| url_of(&r)),
        Err(e) => return internal("reading a name's card for the gateway", e).into_response(),
    };
    match target {
        Some(url) => {
            // `Url::to_string` is ASCII, so it is always a valid header value.
            let res = (StatusCode::FOUND, [(header::LOCATION, url)]).into_response();
            cached(res, CachePolicy::Ttl(app.deployment.args.cache_ttl))
        }
        None => page(app, StatusCode::NOT_FOUND, Page::NoSite, label),
    }
}

fn url_of(records: &cards::Records) -> Option<String> {
    match records.get("url")? {
        RecordValue::Text(text) => redirect_target(text),
        _ => None,
    }
}

/// A `url` value either starts with `http://` or `https://`, or is a bare host such as
/// `example.com/page`, which gets `https://`. This matches how clients render a `url` record, so
/// a link and the redirect never disagree. The result must parse as a URL, which also refuses
/// anything whose host a browser reads differently.
pub(super) fn redirect_target(value: &str) -> Option<String> {
    let text = value.trim();
    let starts_with = |prefix: &str| text.get(..prefix.len()).is_some_and(|p| p.eq_ignore_ascii_case(prefix));
    let candidate = if starts_with("http://") || starts_with("https://") {
        text.to_string()
    } else if is_bare_site(text) {
        format!("https://{text}")
    } else {
        return None;
    };
    let url = reqwest::Url::parse(&candidate).ok()?;
    if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
        return None;
    }
    Some(url.to_string())
}

/// `host(.host)+(:port)?(/no-whitespace)?`, with host labels of ASCII `A-Za-z0-9-`.
fn is_bare_site(text: &str) -> bool {
    let (authority, path) = match text.split_once('/') {
        Some((a, p)) => (a, Some(p)),
        None => (text, None),
    };
    let (host, port) = match authority.split_once(':') {
        Some((h, p)) => (h, Some(p)),
        None => (authority, None),
    };
    let label_ok = |l: &str| !l.is_empty() && l.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'-');
    host.split('.').count() >= 2
        && host.split('.').all(label_ok)
        && port.is_none_or(|p| !p.is_empty() && p.bytes().all(|c| c.is_ascii_digit()))
        && path.is_none_or(|p| !p.chars().any(char::is_whitespace))
}

#[derive(Clone, Copy)]
enum Page {
    NotAName,
    Free,
    NoSite,
    Held,
    Unproven,
    NotReady,
}

fn page(app: &App, status: StatusCode, which: Page, label: &str) -> Response {
    let app_url = app.deployment.args.gateway_app_url.trim_end_matches('/');
    let (heading, text, href, link) = page_copy(which, label, app_url);
    let body = GATEWAY_PAGE
        .replace("__LOGO__", &LOGO_DATA_URI)
        .replace("__DOMAIN__", &escape_html(&app.deployment.args.gateway_domain))
        .replace("__HEADING__", &escape_html(&heading))
        .replace("__TEXT__", &escape_html(&text))
        .replace("__HREF__", &escape_html(&href))
        .replace("__LINK__", &escape_html(&link));
    (status, html(body.into(), GATEWAY_CSP)).into_response()
}

/// The heading, text, link target and link text of a page, unescaped.
fn page_copy(which: Page, name: &str, app_url: &str) -> (String, String, String, String) {
    match which {
        Page::NotAName => (
            format!("{name} is not a name"),
            "A name is one label of a-z, 0-9 and hyphen. Nothing below a name has a website of its own.".to_string(),
            app_url.to_string(),
            "open dotk".to_string(),
        ),
        Page::Free => (
            format!("{name}.k is free"),
            "Nobody holds this name. Register it and point it at your website.".to_string(),
            format!("{app_url}/names/{name}"),
            format!("register {name}.k"),
        ),
        Page::NoSite => (
            format!("{name}.k has no website"),
            "The name is held, and its owner has not named a website in its records.".to_string(),
            format!("{app_url}/names/{name}"),
            format!("see {name}.k"),
        ),
        Page::Held => (
            format!("{name}.k is taken"),
            "Someone holds this name, and this service cannot show its owner or a website for it.".to_string(),
            format!("{app_url}/names/{name}"),
            format!("see {name}.k"),
        ),
        Page::Unproven => (
            format!("{name}.k cannot be looked up"),
            "This service cannot prove this name against the chain right now.".to_string(),
            format!("{app_url}/names/{name}"),
            format!("see {name}.k"),
        ),
        Page::NotReady => (
            format!("{name}.k cannot be looked up yet"),
            "This service is still proving its copy of the registry against the chain. Try again in a few minutes.".to_string(),
            format!("{app_url}/names/{name}"),
            format!("see {name}.k"),
        ),
    }
}

/// Inlined, because every path on the gateway host is the gateway, so `/logo.svg` does not exist.
static LOGO_DATA_URI: LazyLock<String> = LazyLock::new(logo_data_uri);

fn logo_data_uri() -> String {
    let mut uri = String::from("data:image/svg+xml,");
    for byte in LOGO_SVG.bytes() {
        match byte {
            b'a'..=b'z'
            | b'A'..=b'Z'
            | b'0'..=b'9'
            | b'-'
            | b'.'
            | b'_'
            | b'~'
            | b' '
            | b'/'
            | b':'
            | b'='
            | b','
            | b';'
            | b'('
            | b')' => {
                uri.push(char::from(byte));
            }
            _ => write!(uri, "%{byte:02X}").expect("a String never fails to write"),
        }
    }
    uri
}

fn escape_html(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#39;"),
            _ => out.push(c),
        }
    }
    out
}

#[cfg(test)]
#[path = "gateway_tests.rs"]
mod tests;
