use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use super::Memo;

/// A canceled build pins a pool connection per disconnecting client.
#[tokio::test(start_paused = true)]
async fn a_memo_build_outlives_the_caller_that_started_it() {
    let memo = Memo::<u32>::default();
    let builds = Arc::new(AtomicUsize::new(0));
    let build = || {
        let builds = builds.clone();
        async move {
            builds.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(200)).await;
            Ok(7)
        }
    };
    let (ttl, cap) = (Duration::from_secs(60), Duration::from_secs(60));
    assert!(tokio::time::timeout(Duration::from_millis(50), memo.get(ttl, cap, build())).await.is_err(), "the first caller leaves");
    let (value, _) = memo.get(ttl, cap, build()).await.unwrap();
    assert_eq!(*value, 7);
    assert_eq!(builds.load(Ordering::SeqCst), 1, "one build served both callers");
}
