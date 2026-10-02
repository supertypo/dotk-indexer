use std::pin::Pin;
use std::sync::Arc;

use anyhow::{Context, Result};
use axum::body::Bytes;
use dotk_core::watch::{GenesisFile, WatchTemplates};
use sqlx::PgPool;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use crate::app::{App, Derived, Progress};
use crate::chain::{self, Kaspad, PooledKaspad};
use crate::config::{self, CliArgs};
use crate::convert::unhex32;
use crate::identity::Identity;
use crate::{audit, db, evictor, identity, snapshot, web};

/// `genesis_raw` is the manifest as `/genesis` serves it.
pub async fn run(args: CliArgs, genesis: GenesisFile, genesis_raw: Bytes) -> Result<()> {
    init_logging(&args);
    log::info!("{} {}", env!("CARGO_PKG_NAME"), config::VERSION);
    let signals = spawn_signal_handler()?;
    let prefix = dotk_core::address::prefix_for(&args.network)?;
    check_evictor_wallet(&args, prefix)?;
    if args.rpc_urls.iter().any(|url| url == dotk_core::net::RESOLVER) {
        log::warn!("--rpc-url resolver: the public resolver network is often overloaded");
    }
    let watch = check_manifest(&args, &genesis)?;
    let derived = Derived { watch, prefix, net_bps: dotk_core::fees::net_bps(&args.network)? };
    let (listener, kaspad, pool) = connect(&args).await?;
    // Also rejects a manifest whose params and bytecode disagree about the devfund, which
    // leaves the pipeline silently deaf.
    let ident = identity::of(&genesis, &derived.watch).context("deployment manifest")?;
    let boot = Boot { args: &args, genesis: &genesis, kaspad: kaspad.as_ref(), pool: &pool, ident: &ident, net_bps: derived.net_bps };
    let start = bootstrap(&boot).await?;

    let app = Arc::new(App::with(args, pool, kaspad, genesis, genesis_raw, derived, start.coverage_floor));
    let _ = signals.send(app.clone());
    let mut tasks = spawn_tasks(&app, listener);
    // The follow loop's future drops with its open transaction before the other tasks are joined.
    let result = {
        let vcp_task = std::pin::pin!(chain::run(app.clone(), start.block));
        supervise(&app.progress, &mut tasks, vcp_task).await
    };
    app.progress.request_shutdown();
    join_tasks(tasks).await;
    result
}

fn init_logging(args: &CliArgs) {
    env_logger::Builder::new().parse_filters(&format!("{},sqlx=warn", args.log_level)).init();
}

/// Fails at boot and not in the evictor task.
fn check_evictor_wallet(args: &CliArgs, prefix: kaspa_addresses::Prefix) -> Result<()> {
    match evictor::Payout::from_args(args, prefix)? {
        None => log::warn!("evictor off: no --evictor-key or --evictor-address"),
        Some(payout) if payout.is_unfunded() => {
            log::warn!("evictor unfunded, paying {}: anyone can take the bounty of its evicts", payout.address);
        }
        Some(_) => {}
    }
    Ok(())
}

fn check_manifest(args: &CliArgs, genesis: &GenesisFile) -> Result<WatchTemplates> {
    anyhow::ensure!(genesis.network == args.network, "the deployment is on {} but --network is {}", genesis.network, args.network);
    anyhow::ensure!(
        genesis.version == dotk_core::watch::GENESIS_VERSION,
        "the deployment manifest is version {} but this build reads version {}",
        genesis.version,
        dotk_core::watch::GENESIS_VERSION
    );
    let watch = genesis.watch_templates().map_err(anyhow::Error::msg).context("deployment manifest")?;
    genesis.verify_genesis_binding().map_err(anyhow::Error::msg).context("deployment manifest")?;
    Ok(watch)
}

async fn connect(args: &CliArgs) -> Result<(std::net::TcpListener, Arc<dyn Kaspad>, PgPool)> {
    let listener = web::bind(&args.listen).with_context(|| format!("binding --listen {}", args.listen))?;
    let kaspad: Arc<dyn Kaspad> = Arc::new(PooledKaspad::connect(&args.network, &args.rpc_urls).await?);
    let pool = db::connect(&args.database_url).await.context("connecting to postgres")?;
    Ok((listener, kaspad, pool))
}

struct Boot<'a> {
    args: &'a CliArgs,
    genesis: &'a GenesisFile,
    kaspad: &'a dyn Kaspad,
    pool: &'a PgPool,
    ident: &'a Identity,
    net_bps: u64,
}

struct Start {
    block: [u8; 32],
    coverage_floor: u64,
}

async fn bootstrap(boot: &Boot<'_>) -> Result<Start> {
    let schema_present = db::schema_present(boot.pool).await.context("checking the schema")?;
    let unfinished =
        schema_present && !boot.args.initialize_db && db::never_synced(boot.pool).await.context("checking for a previous sync")?;
    if unfinished {
        anyhow::ensure!(
            db::ledger_is_ours(boot.pool).await.context("checking the migration ledger")?,
            "the database holds tables this indexer did not create; give it a database of its own, or pass \
             -c/--initialize-db to drop them"
        );
        log::warn!("the database holds no checkpoint and no rows, so bootstrapping again");
    }
    if boot.args.initialize_db || !schema_present || unfinished { initialize(boot, schema_present).await } else { resume(boot).await }
}

async fn initialize(boot: &Boot<'_>, schema_present: bool) -> Result<Start> {
    let source = snapshot::decide(boot.args.snapshot_file.exists(), &boot.args.snapshot_url, &boot.args.network);
    let body = load_body(boot, &source).await?;
    // Checked before any table exists too, so a restart bootstraps again instead of dying at the
    // first poll every time.
    if let Some(snapshot) = &body {
        snapshot::verify_checkpoint(boot.kaspad, snapshot, boot.net_bps)
            .await
            .context("the bootstrap body cannot be resumed from, and nothing was written")?;
    }
    if schema_present {
        if boot.args.initialize_db {
            log::warn!("--initialize-db: dropping all tables");
        }
        db::drop_schema(boot.pool).await.context("dropping the schema")?;
    }
    db::migrate(boot.pool).await.context("running the migrations")?;
    identity::write(boot.pool, boot.ident).await.context("writing the deployment identity")?;
    let Some(snapshot) = body else {
        log::warn!("nothing to import, cold start from the current virtual parent");
        return Ok(Start { block: virtual_parent(boot.kaspad).await?, coverage_floor: 0 });
    };
    let (checkpoint, coverage) = import_body(boot, &source, &snapshot).await?;
    let coverage_floor = log_coverage(coverage);
    // The ordinary self-test proves the imported state, so the file has no further use.
    if source == snapshot::Bootstrap::File {
        let file = boot.args.snapshot_file.display();
        match tokio::fs::remove_file(&boot.args.snapshot_file).await {
            Ok(()) => log::info!("imported into postgres, deleted {file}"),
            Err(e) => log::warn!("imported, but {file} not deleted: {e}"),
        }
    }
    Ok(Start { block: checkpoint, coverage_floor })
}

async fn load_body(boot: &Boot<'_>, source: &snapshot::Bootstrap) -> Result<Option<snapshot::Snapshot>> {
    let file = boot.args.snapshot_file.display();
    match source {
        snapshot::Bootstrap::File => {
            log::info!("importing snapshot {file}");
            let raw = tokio::fs::read_to_string(&boot.args.snapshot_file).await.with_context(|| format!("reading {file}"))?;
            let body = serde_json::from_str(&raw).with_context(|| format!("{file} does not parse as a /snapshot body"))?;
            Ok(Some(body))
        }
        snapshot::Bootstrap::Fetch { url, explicit } => {
            log::info!("no {file}, bootstrapping from {url}");
            if url.starts_with("http:") {
                log::warn!("{url} is plain http, so the body travels in the clear and anyone on the path can replace it");
            }
            let snapshot = snapshot::fetch(url, boot.args.snapshot_max_bytes)
                .await
                .with_context(|| format!("bootstrapping from {url} (a restart tries again)"))?;
            if !*explicit && snapshot.registry_covenant_id != boot.genesis.registry_covenant_id {
                log::warn!("{url} publishes registry {}, not this deployment's", snapshot.registry_covenant_id);
                return Ok(None);
            }
            Ok(Some(snapshot))
        }
        snapshot::Bootstrap::None => {
            log::info!("no {file}, and --snapshot-url is not fetching one");
            Ok(None)
        }
    }
}

async fn import_body(boot: &Boot<'_>, source: &snapshot::Bootstrap, snapshot: &snapshot::Snapshot) -> Result<([u8; 32], Option<u64>)> {
    let imported = snapshot::import(boot.pool, snapshot, boot.genesis).await.map_err(|e| match source {
        snapshot::Bootstrap::Fetch { url, .. } => e.context(format!("importing {url}")),
        _ => e.context(format!("importing {}", boot.args.snapshot_file.display())),
    })?;
    log::info!("imported {} deeds, {} journal events", snapshot.deeds.len(), snapshot.events.len());
    Ok(imported)
}

fn log_coverage(coverage: Option<u64>) -> u64 {
    if let Some(c) = coverage {
        log::info!(
            "imported journal coverage starts at blue score {c}. A reorg reaching below it \
             escalates to a full validation, which is the ordinary cost of a resume section \
             that covers seconds rather than the whole retention window"
        );
        c
    } else {
        log::warn!(
            "imported snapshot carries no journal, so any reorg before the journal rebuilds \
             will escalate to a full validation"
        );
        u64::MAX
    }
}

async fn virtual_parent(kaspad: &dyn Kaspad) -> Result<[u8; 32]> {
    Ok(kaspad.dag_info().await.context("reading the node's DAG info")?.virtual_parent)
}

async fn resume(boot: &Boot<'_>) -> Result<Start> {
    db::migrate(boot.pool).await.context("running the migrations")?;
    // A database from another deployment is fatal, because the audit refutes and deletes every
    // row, and no journal restores them.
    identity::check(boot.pool, boot.ident).await.context("checking the deployment identity")?;
    let mut conn = boot.pool.acquire().await.context("acquiring a postgres connection")?;
    let coverage_floor = db::journal_coverage(&mut conn).await.context("reading the journal coverage")?.unwrap_or(0);
    let block = if let Some(cp) = db::get_var(&mut conn, db::VAR_VCP_CHECKPOINT).await.context("reading the vcp checkpoint")? {
        let block = unhex32(&cp).context("stored vcp checkpoint")?;
        log::info!("resuming from checkpoint {cp}");
        block
    } else {
        log::warn!("schema present but no checkpoint, cold start from the current virtual parent");
        virtual_parent(boot.kaspad).await?
    };
    Ok(Start { block, coverage_floor })
}

struct Tasks {
    web: std::thread::JoinHandle<()>,
    web_stopped: oneshot::Receiver<()>,
    web_done: bool,
    audit: JoinHandle<()>,
    audit_done: bool,
    evictor: Option<JoinHandle<()>>,
}

fn spawn_tasks(app: &Arc<App>, listener: std::net::TcpListener) -> Tasks {
    let (web, web_stopped) = web::spawn(app.clone(), listener);
    let audit = tokio::spawn(audit::run(app.clone()));
    let evictor = if app.deployment.args.evictor_on() { Some(tokio::spawn(evictor::run(app.clone()))) } else { None };
    Tasks { web, web_stopped, web_done: false, audit, audit_done: false, evictor }
}

async fn supervise(progress: &Progress, tasks: &mut Tasks, mut vcp_task: Pin<&mut impl Future<Output = Result<()>>>) -> Result<()> {
    loop {
        tokio::select! {
            r = &mut vcp_task => return r,
            j = &mut tasks.audit, if !tasks.audit_done => {
                tasks.audit_done = true;
                propagate_panic(j);
            }
            j = async { tasks.evictor.as_mut().expect("guarded by is_some").await }, if tasks.evictor.is_some() => {
                tasks.evictor = None;
                propagate_panic(j);
            }
            _ = &mut tasks.web_stopped, if !tasks.web_done => {
                tasks.web_done = true;
                if !progress.is_shutdown() {
                    anyhow::bail!("the web server stopped while the indexer was running");
                }
            }
        }
    }
}

async fn join_tasks(tasks: Tasks) {
    if !tasks.audit_done {
        propagate_panic(tasks.audit.await);
    }
    if let Some(h) = tasks.evictor {
        propagate_panic(h.await);
    }
    if let Err(panic) = tasks.web.join() {
        std::panic::resume_unwind(panic);
    }
}

/// The first SIGINT or SIGTERM asks every task to wind down, and the second exits with 1.
/// The handler must keep receiving, because tokio replaces the default disposition for the
/// life of the process. A task that returns after the first signal makes the process
/// uninterruptible. A forced exit loses nothing, because each batch commits in one transaction.
/// Installed before the node and the database connect, so a signal during boot exits at once. As
/// PID 1 of a container, a process without a handler ignores the signal until the kill.
fn spawn_signal_handler() -> Result<oneshot::Sender<Arc<App>>> {
    let (tx, rx) = oneshot::channel::<Arc<App>>();
    #[cfg(unix)]
    spawn_unix_signal_handler(rx)?;
    #[cfg(not(unix))]
    spawn_ctrl_c_handler(rx);
    Ok(tx)
}

/// Long-lived streams, so a second signal cannot fall between two subscriptions.
#[cfg(unix)]
fn spawn_unix_signal_handler(rx: oneshot::Receiver<Arc<App>>) -> Result<()> {
    use tokio::signal::unix::{SignalKind, signal};
    let mut sigint = signal(SignalKind::interrupt()).context("installing the SIGINT handler")?;
    let mut sigterm = signal(SignalKind::terminate()).context("installing the SIGTERM handler")?;
    tokio::spawn(async move {
        // `rx` first, so a signal pending at the handover shuts down instead of exiting.
        let app = tokio::select! {
            biased;
            app = rx => match app {
                Ok(app) => app,
                Err(_) => return,
            },
            _ = sigint.recv() => exit_at_boot("SIGINT"),
            _ = sigterm.recv() => exit_at_boot("SIGTERM"),
        };
        let mut requested = false;
        loop {
            let name = tokio::select! {
                _ = sigint.recv() => "SIGINT",
                _ = sigterm.recv() => "SIGTERM",
            };
            handle_signal(&app.progress, name, &mut requested);
        }
    });
    Ok(())
}

/// `ctrl_c()` is a future, so a second press between two calls is lost.
#[cfg(not(unix))]
fn spawn_ctrl_c_handler(rx: oneshot::Receiver<Arc<App>>) {
    tokio::spawn(async move {
        let app = tokio::select! {
            pressed = tokio::signal::ctrl_c() => match pressed {
                Ok(()) => exit_at_boot("Ctrl+C"),
                Err(_) => return,
            },
            app = rx => match app {
                Ok(app) => app,
                Err(_) => return,
            },
        };
        let mut requested = false;
        while tokio::signal::ctrl_c().await.is_ok() {
            handle_signal(&app.progress, "Ctrl+C", &mut requested);
        }
    });
}

fn exit_at_boot(name: &str) -> ! {
    log::info!("{name} received during boot, exiting");
    std::process::exit(0);
}

fn handle_signal(progress: &Progress, name: &str, requested: &mut bool) {
    if *requested {
        log::warn!("{name} received again, terminating and abandoning whatever is in flight");
        std::process::exit(1);
    }
    log::info!("{name} received, shutting down (repeat to terminate immediately)");
    progress.request_shutdown();
    *requested = true;
}

fn propagate_panic(join: Result<(), tokio::task::JoinError>) {
    if let Err(e) = join
        && e.is_panic()
    {
        std::panic::resume_unwind(e.into_panic());
    }
}
