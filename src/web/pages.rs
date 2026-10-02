use std::sync::Arc;

use axum::Router;
use axum::body::Bytes;
use axum::extract::rejection::QueryRejection;
use axum::extract::{Query, State};
use axum::http::{HeaderValue, header};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::get;

use super::dto::NoQuery;
use super::error::ApiError;
use super::handlers::GENESIS_CACHE_TTL;
use super::middleware::{CachePolicy, cached};
use super::params::checked;
use super::server::api_base;
use crate::app::App;

const STATUS_PAGE: &str = include_str!("../status.html");
const STATUS_SCRIPT: &str = include_str!("../status.js");
pub(super) const LOGO_SVG: &str = include_str!("../logo.svg");
/// The subsets the reference viewer fetches from its vendor's font host when `withDefaultFonts` is on.
const FONTS: [(&str, &[u8]); 5] = [
    ("inter-latin.woff2", include_bytes!("../fonts/inter-latin.woff2")),
    ("inter-latin-ext.woff2", include_bytes!("../fonts/inter-latin-ext.woff2")),
    ("inter-symbols.woff2", include_bytes!("../fonts/inter-symbols.woff2")),
    ("mono-latin.woff2", include_bytes!("../fonts/mono-latin.woff2")),
    ("mono-latin-ext.woff2", include_bytes!("../fonts/mono-latin-ext.woff2")),
];

/// The status page runs its own script and polls its own origin.
const STATUS_CSP: &str = "default-src 'none'; script-src 'self'; style-src 'unsafe-inline'; img-src 'self'; connect-src 'self'; \
                          frame-ancestors 'none'; base-uri 'none'; form-action 'none'";
macro_rules! scalar_url {
    () => {
        "https://cdn.jsdelivr.net/npm/@scalar/api-reference@1.72.1/dist/browser/standalone.js"
    };
}

/// The reference viewer is one file from a CDN. It styles itself and sends its try-it requests
/// to this origin.
pub(super) const DOCS_CSP: &str = concat!(
    "default-src 'none'; script-src ",
    scalar_url!(),
    "; style-src 'unsafe-inline'; img-src 'self' data:; font-src 'self'; connect-src 'self'; frame-ancestors 'none'; ",
    "base-uri 'none'; form-action 'none'"
);

/// The router fills `__LOGO__`, `__FONTS__` and `$spec`. The font faces are the viewer's own,
/// served from here, and `withDefaultFonts` keeps it from fetching them a second time.
pub(super) const DOCS_PAGE: &str = concat!(
    r#"<!doctype html>
<html lang="en">
<head>
    <meta charset="utf-8"/>
    <meta name="viewport" content="width=device-width, initial-scale=1"/>
    <title>API | dotk.name</title>
    <link rel="icon" href="__LOGO__" type="image/svg+xml"/>
    <style>
    @font-face { font-family: "Inter"; font-style: normal; font-weight: 100 900; font-display: swap;
                 src: url(__FONTS__/inter-latin-ext.woff2) format("woff2");
                 unicode-range: U+0100-02AF, U+0304, U+0308, U+0329, U+1E00-1E9F, U+1EF2-1EFF, U+2020, U+20A0-20AB, U+20AD-20C0, U+2113, U+2C60-2C7F, U+A720-A7FF; }
    @font-face { font-family: "Inter"; font-style: normal; font-weight: 100 900; font-display: swap;
                 src: url(__FONTS__/inter-latin.woff2) format("woff2");
                 unicode-range: U+0000-00FF, U+0131, U+0152-0153, U+02BB-02BC, U+02C6, U+02DA, U+02DC, U+0304, U+0308, U+0329, U+2000-206F, U+2074, U+20AC, U+2122, U+2191, U+2193, U+2212, U+2215, U+FEFF, U+FFFD; }
    @font-face { font-family: "Inter"; font-style: normal; font-weight: 100 900; font-display: swap;
                 src: url(__FONTS__/inter-symbols.woff2) format("woff2");
                 unicode-range: U+2190-2193, U+21B5, U+21E7, U+21EA, U+2318, U+2325; }
    @font-face { font-family: "JetBrains Mono"; font-style: normal; font-weight: 400; font-display: swap;
                 src: url(__FONTS__/mono-latin-ext.woff2) format("woff2");
                 unicode-range: U+0100-02AF, U+0304, U+0308, U+0329, U+1E00-1E9F, U+1EF2-1EFF, U+2020, U+20A0-20AB, U+20AD-20C0, U+2113, U+2C60-2C7F, U+A720-A7FF; }
    @font-face { font-family: "JetBrains Mono"; font-style: normal; font-weight: 400; font-display: swap;
                 src: url(__FONTS__/mono-latin.woff2) format("woff2");
                 unicode-range: U+0000-00FF, U+0131, U+0152-0153, U+02BB-02BC, U+02C6, U+02DA, U+02DC, U+0304, U+0308, U+0329, U+2000-206F, U+2074, U+20AC, U+2122, U+2191, U+2193, U+2212, U+2215, U+FEFF, U+FFFD; }
    </style>
</head>
<body>
<script id="api-reference" type="application/json" data-configuration='{"withDefaultFonts":false,"proxyUrl":""}'>
    $spec
</script>
<script src=""#,
    scalar_url!(),
    r#"" integrity="sha384-U11tb2XnKvmwt8RlTvnwUnYgrN+ur4Xyh9htLhjajWNR/Oyl5AX5DEz00qRmlrmK" crossorigin="anonymous"></script>
</body>
</html>
"#
);

pub(super) async fn status_page(
    State(app): State<Arc<App>>,
    query: Result<Query<NoQuery>, QueryRejection>,
) -> Result<Response, ApiError> {
    checked(query)?;
    let base = app.deployment.args.base_path.trim_end_matches('/');
    let page = STATUS_PAGE
        .replace("__BASE__", &api_base(base))
        .replace("__LOGO__", &logo_path(base))
        .replace("__SCRIPT__", &script_path(base));
    Ok(html(page.into(), STATUS_CSP))
}

pub(super) fn html(body: Bytes, csp: &'static str) -> Response {
    ([(header::CONTENT_SECURITY_POLICY, HeaderValue::from_static(csp))], Html(body)).into_response()
}

pub(super) async fn status_script(query: Result<Query<NoQuery>, QueryRejection>) -> Result<Response, ApiError> {
    checked(query)?;
    Ok(([(header::CONTENT_TYPE, "text/javascript; charset=utf-8")], STATUS_SCRIPT).into_response())
}

pub(super) async fn logo(query: Result<Query<NoQuery>, QueryRejection>) -> Result<Response, ApiError> {
    checked(query)?;
    Ok(([(header::CONTENT_TYPE, "image/svg+xml")], LOGO_SVG).into_response())
}

pub(super) fn font_routes(router: Router<Arc<App>>, base_path: &str) -> Router<Arc<App>> {
    let fonts = fonts_path(base_path);
    FONTS.into_iter().fold(router, |router, (name, bytes)| {
        let font = move |query: Result<Query<NoQuery>, QueryRejection>| {
            let font = ([(header::CONTENT_TYPE, "font/woff2")], bytes).into_response();
            std::future::ready(checked(query).map(|NoQuery {}| cached(font, CachePolicy::Ttl(GENESIS_CACHE_TTL))))
        };
        router.route(&format!("{fonts}/{name}"), get(font))
    })
}

pub(super) fn logo_path(base_path: &str) -> String {
    format!("{}/logo.svg", base_path.trim_end_matches('/'))
}

pub(super) fn fonts_path(base_path: &str) -> String {
    format!("{}/fonts", base_path.trim_end_matches('/'))
}

pub(super) fn script_path(base_path: &str) -> String {
    format!("{}/status.js", base_path.trim_end_matches('/'))
}
