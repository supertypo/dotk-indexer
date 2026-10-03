use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use axum::Router;
use axum::body::Bytes;
use axum::extract::Query;
use axum::extract::rejection::QueryRejection;
use axum::http::{HeaderValue, header};
use axum::routing::{MethodRouter, get};
use hyper_util::rt::{TokioIo, TokioTimer};
use hyper_util::server::graceful::GracefulShutdown;
use hyper_util::service::TowerToHyperService;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tower_http::set_header::SetResponseHeaderLayer;
use utoipa::OpenApi;

use super::dto::{
    CardChangeOut, CardOut, DeedOut, GapOut, GapsByWidth, HealthResponse, HistoryEntry, HistoryFeedResponse, HistoryOpOut,
    HistoryResponse, KeyKindOut, KeyResponse, KeysByPrefix, KeyspaceResponse, KeyspaceTotals, Manifest, NameResponse, NeighborGaps,
    NoQuery, OwnerResponse, RecordValueOut, RowKindOut, SelfTestDetail, SelfTestSummary, SpenderCardsResponse, StatusOut,
};
use super::error::{ErrorCode, ErrorResponse};
use super::handlers::{
    self, get_address, get_genesis, get_health, get_history, get_key, get_key_by_name, get_key_history, get_keyspace, get_name,
    get_owner, get_snapshot, get_spender_cards,
};
use super::middleware::{cache_control_default_mw, cache_control_mw, cors_layer, request_timeout_mw, tagged_json};
use super::pages::{DOCS_CSP, DOCS_PAGE, font_routes, fonts_path, html, logo, logo_path, script_path, status_page, status_script};
use super::params::checked;
use crate::app::{App, Tagged};
use crate::model::{OwnerTypeParam, SpenderTypeParam};
use crate::snapshot;

const API_VERSION_PATH: &str = "/v1";

pub(super) const REGISTRY_TAG: &str = "registry";
pub(super) const SNAPSHOT_TAG: &str = "snapshot";
pub(super) const DEPLOYMENT_TAG: &str = "deployment";
pub(super) const HEALTH_TAG: &str = "health";

#[derive(OpenApi)]
#[openapi(
    info(
        title = "dotk.name API",
        description = "The `.k` name registry on Kaspa. A name or key answer carries its deed address and \
                       the registry covenant id, so a caller can check it with one UTXO query to a \
                       Kaspa node.",
        contact(name = "dotk.name", email = "s@dotk.name"),
        license(name = "AGPL-3.0-only", url = "https://www.gnu.org/licenses/agpl-3.0.html")
    ),
    tags(
        (name = REGISTRY_TAG, description = "Name, address and keyspace queries"),
        (name = SNAPSHOT_TAG, description = "Registry snapshot for mirrors and bootstrap"),
        (name = DEPLOYMENT_TAG, description = "The deployment manifest every client boots from"),
        (name = HEALTH_TAG, description = "Liveness and self-test status")
    ),
    paths(
        handlers::get_name,
        handlers::get_owner,
        handlers::get_address,
        handlers::get_spender_cards,
        handlers::get_key,
        handlers::get_key_by_name,
        handlers::get_key_history,
        handlers::get_history,
        handlers::get_keyspace,
        handlers::get_snapshot,
        handlers::get_genesis,
        handlers::get_health
    ),
    components(schemas(
        NameResponse,
        OwnerResponse,
        OwnerTypeParam,
        SpenderCardsResponse,
        SpenderTypeParam,
        CardOut,
        RecordValueOut,
        KeyResponse,
        HistoryResponse,
        HistoryFeedResponse,
        HistoryEntry,
        KeyspaceResponse,
        KeyspaceTotals,
        KeysByPrefix,
        GapsByWidth,
        DeedOut,
        StatusOut,
        RowKindOut,
        KeyKindOut,
        HistoryOpOut,
        CardChangeOut,
        GapOut,
        NeighborGaps,
        HealthResponse,
        SelfTestSummary,
        SelfTestDetail,
        Manifest,
        ErrorCode,
        ErrorResponse,
        snapshot::Snapshot
    ))
)]
struct ApiDoc;

pub(super) fn api_base(base_path: &str) -> String {
    format!("{}{API_VERSION_PATH}", base_path.trim_end_matches('/'))
}

/// The API description with `servers` set to this deployment's base, so relative paths and
/// Scalar's try-it resolve.
pub fn openapi_doc(base_path: &str) -> utoipa::openapi::OpenApi {
    let mut doc = ApiDoc::openapi();
    doc.servers = Some(vec![utoipa::openapi::Server::new(api_base(base_path.trim_end_matches('/')))]);
    doc
}

fn docs_routes(base: &str) -> (MethodRouter<Arc<App>>, MethodRouter<Arc<App>>) {
    let doc = openapi_doc(base);
    let spec = Tagged::new(serde_json::to_vec(&doc).expect("the OpenAPI document serializes"));
    // `<` is escaped, so no string in the document can end the script element.
    let spec_json = String::from_utf8_lossy(&spec.bytes).replace('<', "\\u003c");
    let docs_html = Bytes::from(
        DOCS_PAGE.replace("__LOGO__", &logo_path(base)).replace("__FONTS__", &fonts_path(base)).replace("$spec", &spec_json),
    );
    let openapi = get(move |query: Result<Query<NoQuery>, QueryRejection>| {
        std::future::ready(checked(query).map(|NoQuery {}| tagged_json(&spec)))
    });
    let docs = get(move |query: Result<Query<NoQuery>, QueryRejection>| {
        std::future::ready(checked(query).map(|NoQuery {}| html(docs_html.clone(), DOCS_CSP)))
    });
    (openapi, docs)
}

pub fn router(app: Arc<App>) -> Router {
    let base = app.deployment.args.base_path.trim_end_matches('/').to_string();
    let api = api_base(&base);
    let (openapi, docs) = docs_routes(&base);
    let mut router = font_routes(Router::new(), &base)
        .route(&logo_path(&base), get(logo))
        .route(&script_path(&base), get(status_script))
        .route(&format!("{api}/names/{{name}}"), get(get_name))
        .route(&format!("{api}/names/{{name}}/key"), get(get_key_by_name))
        .route(&format!("{api}/keys/{{key}}"), get(get_key))
        .route(&format!("{api}/keys/{{key}}/history"), get(get_key_history))
        .route(&format!("{api}/history"), get(get_history))
        .route(&format!("{api}/keyspace"), get(get_keyspace))
        .route(&format!("{api}/owners/{{owner_type}}/{{owner}}"), get(get_owner))
        .route(&format!("{api}/addresses/{{address}}"), get(get_address))
        .route(&format!("{api}/spenders/{{spender_type}}/{{spender}}/cards"), get(get_spender_cards))
        .route(&format!("{api}/snapshot"), get(get_snapshot))
        .route(&format!("{api}/genesis"), get(get_genesis))
        .route(&format!("{api}/health"), get(get_health))
        .route(&format!("{api}/openapi.json"), openapi)
        .route(&api, docs.clone())
        .route(&format!("{api}/"), docs);
    router = if base.is_empty() {
        router.route("/", get(status_page))
    } else {
        router.route(&base, get(status_page)).route(&format!("{base}/"), get(status_page))
    };
    // The last `.layer` is outermost. The gateway is innermost, so its answers get the CORS and
    // cache headers. The default cache and security headers are outermost, so a CORS preflight
    // gets them too. A page sets its own policy.
    router
        .layer(axum::middleware::from_fn_with_state(app.clone(), super::gateway::middleware))
        .layer(axum::middleware::from_fn_with_state(app.clone(), cache_control_mw))
        .layer(axum::middleware::from_fn_with_state(app.clone(), request_timeout_mw))
        .layer(cors_layer(&app.deployment.args.allowed_origins))
        .layer(axum::middleware::from_fn(cache_control_default_mw))
        .layer(SetResponseHeaderLayer::if_not_present(header::X_CONTENT_TYPE_OPTIONS, HeaderValue::from_static("nosniff")))
        .layer(SetResponseHeaderLayer::if_not_present(header::REFERRER_POLICY, HeaderValue::from_static("no-referrer")))
        .layer(SetResponseHeaderLayer::if_not_present(
            header::CONTENT_SECURITY_POLICY,
            HeaderValue::from_static("default-src 'none'; frame-ancestors 'none'"),
        ))
        .with_state(app)
}

pub fn bind(listen: &str) -> std::io::Result<std::net::TcpListener> {
    let listener = std::net::TcpListener::bind(listen)?;
    listener.set_nonblocking(true)?;
    Ok(listener)
}

/// Runs on its own runtime (half the cores by default), so a flooded web layer cannot add
/// latency to the pipeline.
pub fn spawn(app: Arc<App>, listener: std::net::TcpListener) -> (std::thread::JoinHandle<()>, tokio::sync::oneshot::Receiver<()>) {
    let workers = app
        .deployment
        .args
        .web_worker_threads
        .unwrap_or_else(|| (std::thread::available_parallelism().map_or(2, std::num::NonZero::get) / 2).max(1));
    let (alive, stopped) = tokio::sync::oneshot::channel::<()>();
    let handle = std::thread::Builder::new()
        .name("web".into())
        .spawn(move || {
            let _alive = alive;
            let rt = match tokio::runtime::Builder::new_multi_thread()
                .worker_threads(workers)
                .thread_name("web-worker")
                .enable_all()
                .build()
            {
                Ok(rt) => rt,
                Err(e) => {
                    log::error!("web: building the runtime: {e}");
                    return;
                }
            };
            rt.block_on(async move {
                let router = router(app.clone());
                let listener = match tokio::net::TcpListener::from_std(listener) {
                    Ok(listener) => listener,
                    Err(e) => {
                        log::error!("web: adopting the listener: {e}");
                        return;
                    }
                };
                let base = app.deployment.args.base_path.trim_end_matches('/');
                log::info!("web: http://{}{base}/ (API + docs at {})", app.deployment.args.listen, api_base(base));
                serve(app, listener, router).await;
            });
        })
        .expect("spawning the web thread");
    (handle, stopped)
}

const ACCEPT_RETRY: Duration = Duration::from_secs(1);

/// Accepts a connection only with a slot in hand, so the file descriptor table is never the
/// cap: the surplus waits in the listen backlog. HTTP/1 only, because its header timer arms on
/// the first poll, so a silent connection, a slow request head and a parked keep-alive
/// connection all free their slot after `--web-header-timeout`, and `Stalling` frees it when a
/// client stops reading.
async fn serve(app: Arc<App>, listener: tokio::net::TcpListener, router: Router) {
    let builder = http1(app.deployment.args.web_header_timeout);
    let service = TowerToHyperService::new(router);
    let max = usize::try_from(app.deployment.args.web_max_connections).unwrap_or(usize::MAX);
    let slots = Arc::new(tokio::sync::Semaphore::new(max));
    let graceful = GracefulShutdown::new();
    loop {
        let permit = tokio::select! {
            permit = slots.clone().acquire_owned() => permit.expect("the semaphore is never closed"),
            () = app.progress.until_shutdown() => break,
        };
        let stream = tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok((stream, _)) => stream,
                Err(e) => {
                    log::warn!("web: accept failed: {e}");
                    tokio::time::sleep(ACCEPT_RETRY).await;
                    continue;
                }
            },
            () = app.progress.until_shutdown() => break,
        };
        let (builder, service, watcher) = (builder.clone(), service.clone(), graceful.watcher());
        let stall = app.deployment.args.web_header_timeout;
        tokio::spawn(async move {
            let conn = builder.serve_connection(TokioIo::new(Stalling::new(stream, stall)), service);
            if let Err(e) = watcher.watch(conn).await {
                log::debug!("web: connection ended: {e}");
            }
            drop(permit);
        });
    }
    // Refuses new connections at once, so a proxy retries elsewhere.
    drop(listener);
    drain(graceful, app.deployment.args.web_shutdown_deadline, || max - slots.available_permits()).await;
}

fn http1(header_timeout: Duration) -> Arc<hyper::server::conn::http1::Builder> {
    let mut builder = hyper::server::conn::http1::Builder::new();
    builder.timer(TokioTimer::new()).header_read_timeout(header_timeout);
    Arc::new(builder)
}

async fn drain(graceful: GracefulShutdown, deadline: Duration, open: impl Fn() -> usize) {
    if open() > 0 {
        log::info!("web: waiting up to {} s for {} open connection(s)", deadline.as_secs(), open());
    }
    tokio::select! {
        () = graceful.shutdown() => {}
        () = tokio::time::sleep(deadline) => {
            log::warn!("web: dropping {} connection(s) still open at the shutdown deadline", open());
        }
    }
}

/// A socket whose writes make no progress for `stall` fails, because hyper has no write timeout
/// and a client that stops reading holds its slot with the response in flight.
struct Stalling<T> {
    io: T,
    stall: Duration,
    deadline: Option<Pin<Box<tokio::time::Sleep>>>,
}

impl<T> Stalling<T> {
    fn new(io: T, stall: Duration) -> Self {
        Self { io, stall, deadline: None }
    }

    fn progress<R>(&mut self, cx: &mut Context<'_>, out: Poll<std::io::Result<R>>) -> Poll<std::io::Result<R>> {
        if out.is_ready() {
            self.deadline = None;
            return out;
        }
        let deadline = self.deadline.get_or_insert_with(|| Box::pin(tokio::time::sleep(self.stall)));
        match deadline.as_mut().poll(cx) {
            Poll::Ready(()) => Poll::Ready(Err(std::io::Error::new(std::io::ErrorKind::TimedOut, "the client stopped reading"))),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<T: AsyncRead + Unpin> AsyncRead for Stalling<T> {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().io).poll_read(cx, buf)
    }
}

impl<T: AsyncWrite + Unpin> AsyncWrite for Stalling<T> {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<std::io::Result<usize>> {
        let this = self.get_mut();
        let out = Pin::new(&mut this.io).poll_write(cx, buf);
        this.progress(cx, out)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        let out = Pin::new(&mut this.io).poll_flush(cx);
        this.progress(cx, out)
    }

    fn poll_write_vectored(self: Pin<&mut Self>, cx: &mut Context<'_>, bufs: &[std::io::IoSlice<'_>]) -> Poll<std::io::Result<usize>> {
        let this = self.get_mut();
        let out = Pin::new(&mut this.io).poll_write_vectored(cx, bufs);
        this.progress(cx, out)
    }

    fn is_write_vectored(&self) -> bool {
        self.io.is_write_vectored()
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().io).poll_shutdown(cx)
    }
}
