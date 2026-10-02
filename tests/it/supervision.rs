use std::sync::Arc;
use std::time::Duration;

use crate::common::sim::SimKaspad;
use crate::common::*;
use dotk_indexer::config::CliArgs;
use kaspa_addresses::Prefix;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[tokio::test]
async fn the_web_servers_end_reaches_its_supervisor() {
    let pool = fresh_pool("supervision").await;
    dotk_indexer::db::migrate(&pool).await.unwrap();
    let (_, genesis_tx) = World::new();
    let app = test_app(pool, Arc::new(SimKaspad::new(Prefix::Testnet, vec![genesis_tx])), fast_args(), 0);
    let listener = dotk_indexer::web::bind("127.0.0.1:0").unwrap();
    let (handle, mut stopped) = dotk_indexer::web::spawn(app.clone(), listener);

    assert!(tokio::time::timeout(Duration::from_millis(300), &mut stopped).await.is_err());
    app.progress.request_shutdown();
    tokio::time::timeout(Duration::from_secs(5), &mut stopped).await.expect("the stop is seen").ok();
    handle.join().unwrap();
}

struct Server {
    app: Arc<dotk_indexer::app::App>,
    addr: String,
    stopped: tokio::sync::oneshot::Receiver<()>,
    handle: std::thread::JoinHandle<()>,
}

async fn serve(name: &str, args: CliArgs) -> Server {
    let pool = fresh_pool(name).await;
    dotk_indexer::db::migrate(&pool).await.unwrap();
    let (_, genesis_tx) = World::new();
    let app = test_app(pool, Arc::new(SimKaspad::new(Prefix::Testnet, vec![genesis_tx])), args, 0);
    let listener = dotk_indexer::web::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let (handle, stopped) = dotk_indexer::web::spawn(app.clone(), listener);
    Server { app, addr, stopped, handle }
}

impl Server {
    async fn stop(mut self) {
        self.app.progress.request_shutdown();
        tokio::time::timeout(Duration::from_secs(5), &mut self.stopped).await.expect("the stop is seen").ok();
        self.handle.join().unwrap();
    }
}

/// 0 once the peer closed or reset, `None` while it is silent.
async fn answered(stream: &mut TcpStream, wait: Duration) -> Option<usize> {
    let mut buf = [0u8; 4096];
    tokio::time::timeout(wait, stream.read(&mut buf)).await.ok().map(|n| n.unwrap_or(0))
}

const GET_GENESIS: &[u8] = b"GET /v1/genesis HTTP/1.1\r\nHost: x\r\n\r\n";

#[tokio::test]
async fn a_request_head_that_never_completes_is_cut() {
    let mut args = fast_args();
    args.web_header_timeout = Duration::from_millis(200);
    let w = serve("web_slow_head", args).await;
    let mut silent = TcpStream::connect(&w.addr).await.unwrap();
    let mut partial = TcpStream::connect(&w.addr).await.unwrap();
    partial.write_all(b"GET /v1/genesis HTTP/1.1\r\nHost: x\r\n").await.unwrap();
    assert_eq!(answered(&mut silent, Duration::from_secs(3)).await, Some(0), "closed by the server, not answered");
    assert_eq!(answered(&mut partial, Duration::from_secs(3)).await, Some(0), "closed by the server, not answered");
    w.stop().await;
}

/// The client pipelines more responses than the socket buffers hold and reads none of them.
#[tokio::test]
async fn a_client_that_stops_reading_frees_its_slot() {
    let mut args = fast_args();
    args.web_max_connections = 1;
    args.web_header_timeout = Duration::from_millis(300);
    let w = serve("web_unread", args).await;
    let mut parked = TcpStream::connect(&w.addr).await.unwrap();
    parked.write_all(&b"GET /v1/openapi.json HTTP/1.1\r\nHost: x\r\n\r\n".repeat(2000)).await.unwrap();
    // One byte of the answer proves the parked connection holds the only slot.
    parked.read_exact(&mut [0u8; 1]).await.unwrap();

    let mut next = TcpStream::connect(&w.addr).await.unwrap();
    next.write_all(GET_GENESIS).await.unwrap();
    assert!(answered(&mut next, Duration::from_secs(3)).await.is_some_and(|n| n > 0), "the stalled slot was freed");
    w.stop().await;
}

#[tokio::test]
async fn a_connection_past_the_cap_waits_for_a_slot() {
    let mut args = fast_args();
    args.web_max_connections = 1;
    args.web_header_timeout = Duration::from_secs(30);
    let w = serve("web_cap", args).await;
    let mut first = TcpStream::connect(&w.addr).await.unwrap();
    first.write_all(GET_GENESIS).await.unwrap();
    assert!(answered(&mut first, Duration::from_secs(3)).await.is_some_and(|n| n > 0));

    let mut second = TcpStream::connect(&w.addr).await.unwrap();
    second.write_all(GET_GENESIS).await.unwrap();
    assert_eq!(answered(&mut second, Duration::from_millis(500)).await, None, "the slot is taken");
    drop(first);
    assert!(answered(&mut second, Duration::from_secs(3)).await.is_some_and(|n| n > 0), "the freed slot serves it");
    w.stop().await;
}

#[tokio::test]
async fn shutdown_drops_a_half_sent_request_at_its_deadline() {
    let mut args = fast_args();
    args.web_header_timeout = Duration::from_secs(30);
    args.web_shutdown_deadline = Duration::from_millis(300);
    let w = serve("web_shutdown", args).await;
    let mut s = TcpStream::connect(&w.addr).await.unwrap();
    s.write_all(b"GET /v1/genesis HTTP/1.1\r\n").await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let started = std::time::Instant::now();
    w.stop().await;
    assert!(started.elapsed() < Duration::from_secs(3), "stopped in {:?}", started.elapsed());
    assert_eq!(answered(&mut s, Duration::from_secs(1)).await, Some(0));
}

/// A loaded node that needs more than the tip deadline for its first answer after an outage
/// otherwise fails every retry.
#[tokio::test]
async fn a_failed_poll_widens_the_next_deadline() {
    use dotk_indexer::chain::{CATCHUP_DEADLINE, TIP_DEADLINE};
    let (h, _) = standard_boot("poll_deadline", fast_args()).await;
    h.wait_until("the tip deadline", || async { h.sim.polls().last().is_some_and(|p| p.0 == TIP_DEADLINE) }).await;
    h.sim.fail_polls(1);
    h.wait_until("two polls after the failed one", || async {
        let polls = h.sim.polls();
        polls.iter().position(|p| p.1).is_some_and(|failed| polls.len() > failed + 2)
    })
    .await;
    let polls = h.sim.polls();
    let failed = polls.iter().position(|p| p.1).unwrap();
    assert_eq!(polls[failed].0, TIP_DEADLINE, "the failed poll was at the tip");
    assert_eq!(polls[failed + 1].0, CATCHUP_DEADLINE, "the poll after it gets the long deadline");
    assert_eq!(polls[failed + 2].0, TIP_DEADLINE, "and an answer brings the tip deadline back");
    h.stop().await;
}

/// The process exits, so a restart reaches another node.
#[tokio::test]
async fn a_node_that_fails_every_poll_past_the_limit_ends_the_pipeline() {
    let pool = fresh_pool("poll_outage").await;
    dotk_indexer::db::migrate(&pool).await.unwrap();
    let (_, genesis_tx) = World::new();
    let sim = Arc::new(SimKaspad::new(Prefix::Testnet, vec![genesis_tx]));
    let start = dotk_indexer::chain::Kaspad::dag_info(&*sim).await.unwrap().virtual_parent;
    let mut args = fast_args();
    args.vcp_outage_limit = Duration::from_millis(200);
    let app = test_app(pool, sim.clone(), args, 0);
    sim.fail_polls(usize::MAX);
    let ended = tokio::time::timeout(Duration::from_secs(20), dotk_indexer::chain::run(app, start)).await.expect("the pipeline ends");
    let err = ended.expect_err("with an error");
    assert!(format!("{err:#}").contains("exits for a restart"), "{err:#}");
}
