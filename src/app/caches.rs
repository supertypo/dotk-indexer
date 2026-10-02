use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use sqlx::PgPool;
use tokio::sync::RwLock;

use super::App;

/// One `/snapshot` export, with and without the resume section (`?events=false`). Both come
/// from the same export, so they never disagree.
#[derive(Debug)]
pub struct SnapshotBodies {
    pub with_events: Tagged,
    pub without_events: Tagged,
}

impl SnapshotBodies {
    pub fn pick(&self, with_events: bool) -> Tagged {
        if with_events { self.with_events.clone() } else { self.without_events.clone() }
    }
}

/// A response body and its entity tag, hashed once because `/snapshot` bodies are megabytes.
#[derive(Debug, Clone)]
pub struct Tagged {
    pub bytes: axum::body::Bytes,
    pub etag: axum::http::HeaderValue,
}

impl Tagged {
    /// A strong entity tag, because it hashes the exact bytes served.
    pub fn new(bytes: impl Into<axum::body::Bytes>) -> Self {
        let bytes = bytes.into();
        let digest = faster_hex::hex_string(&dotk_core::blake3_32(&bytes));
        let etag = axum::http::HeaderValue::from_str(&format!("\"{digest}\"")).expect("hex in quotes is a header value");
        Self { bytes, etag }
    }
}

/// A value one task builds for every caller that waits. The build outlives the request that
/// started it, so only the build cap can orphan a query, and at most one.
pub struct Memo<T> {
    state: Arc<std::sync::Mutex<MemoState<T>>>,
}

/// Shared, because every caller that waited on a build gets its error.
pub(super) type BuildError = Arc<anyhow::Error>;

type Settled<T> = Option<Result<Arc<T>, BuildError>>;

struct MemoState<T> {
    /// The value and the wall-clock unix ms at which it was built.
    value: Option<(Arc<T>, u64)>,
    build: Option<tokio::sync::watch::Receiver<Settled<T>>>,
}

impl<T: Send + Sync + 'static> Default for Memo<T> {
    fn default() -> Self {
        Self { state: Arc::new(std::sync::Mutex::new(MemoState { value: None, build: None })) }
    }
}

/// A poisoned lock still holds a consistent state, because every critical section only assigns.
fn lock<T>(state: &std::sync::Mutex<MemoState<T>>) -> std::sync::MutexGuard<'_, MemoState<T>> {
    state.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

impl<T: Send + Sync + 'static> Memo<T> {
    /// The value and its age in ms, built by `build` if it is older than `ttl` or absent. A
    /// build past `cap` fails, so a socket that went dead under it cannot hold the memo.
    pub async fn get<F>(&self, ttl: Duration, cap: Duration, build: F) -> Result<(Arc<T>, u64), BuildError>
    where
        F: Future<Output = anyhow::Result<T>> + Send + 'static,
    {
        let mut rx = {
            let mut st = lock(&self.state);
            if let Some((value, made_ms)) = &st.value {
                let age = App::now_ms().saturating_sub(*made_ms);
                if age < crate::convert::millis(ttl) {
                    return Ok((value.clone(), age));
                }
            }
            match &st.build {
                Some(rx) => rx.clone(),
                None => self.spawn_build(&mut st, cap, build),
            }
        };
        let waited = rx.wait_for(Option::is_some).await.map(|settled| settled.clone());
        if let Ok(settled) = waited {
            settled.expect("wait_for returns only a settled build").map(|value| (value, 0))
        } else {
            // The build panicked, so the next caller starts another.
            let mut st = lock(&self.state);
            if st.build.as_ref().is_some_and(|b| b.same_channel(&rx)) {
                st.build = None;
            }
            Err(Arc::new(anyhow::anyhow!("the build ended without a result")))
        }
    }

    fn spawn_build<F>(&self, st: &mut MemoState<T>, cap: Duration, build: F) -> tokio::sync::watch::Receiver<Settled<T>>
    where
        F: Future<Output = anyhow::Result<T>> + Send + 'static,
    {
        let (tx, rx) = tokio::sync::watch::channel(None);
        st.build = Some(rx.clone());
        let state = self.state.clone();
        tokio::spawn(async move {
            let out = match tokio::time::timeout(cap, build).await {
                Ok(out) => out.map(Arc::new).map_err(Arc::new),
                Err(_) => Err(Arc::new(anyhow::anyhow!("the build did not finish within {} s", cap.as_secs()))),
            };
            let mut st = lock(&state);
            if let Ok(value) = &out {
                st.value = Some((value.clone(), App::now_ms()));
            }
            st.build = None;
            let _ = tx.send(Some(out));
        });
        rx
    }

    pub fn clear(&self) {
        lock(&self.state).value = None;
    }
}

pub struct Caches {
    pub live_snapshot: Memo<SnapshotBodies>,
    /// `/keyspace`, built at most once per `--cache-ttl`, because a build scans every row.
    pub keyspace: Memo<Tagged>,
    /// What `/health` reports, counted by the pipeline after every commit that wrote and after
    /// every self-test pass, so `/health` needs no pool connection and a busy pool is no verdict.
    pub counts: RwLock<Option<crate::db::RegistryCounts>>,
    /// Held across a count and its store, so the refresh that starts later writes last.
    pub counting: tokio::sync::Mutex<()>,
    /// A failed refresh, retried at the next commit whether or not it wrote.
    pub counts_stale: AtomicBool,
}

impl Default for Caches {
    fn default() -> Self {
        Self {
            live_snapshot: Memo::default(),
            keyspace: Memo::default(),
            counts: RwLock::new(None),
            counting: tokio::sync::Mutex::new(()),
            counts_stale: AtomicBool::new(false),
        }
    }
}

impl Caches {
    /// Counts on the pipeline's pool. A failure keeps the last counts, because the pipeline
    /// raises its own alarm for a database that is gone.
    pub async fn refresh_counts(&self, db: &PgPool) {
        let _counting = self.counting.lock().await;
        let counts = match db.acquire().await {
            Ok(mut conn) => crate::db::counts(&mut conn).await,
            Err(e) => Err(e.into()),
        };
        match counts {
            Ok(counts) => {
                *self.counts.write().await = Some(counts);
                self.counts_stale.store(false, Ordering::Relaxed);
            }
            Err(e) => {
                self.counts_stale.store(true, Ordering::Relaxed);
                log::warn!("counting the registry for /health failed: {e:#}");
            }
        }
    }
}

#[cfg(test)]
#[path = "caches_tests.rs"]
mod tests;
