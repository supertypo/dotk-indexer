use anyhow::{Context, Result};

use super::Snapshot;
use crate::config::SnapshotUrl;

/// What `--snapshot-url builtin` resolves to. A network with no entry cold starts.
fn builtin_snapshot_url(network: &str) -> Option<&'static str> {
    match network {
        "mainnet" => Some("https://api.dotk.name/v1/snapshot"),
        "testnet-10" => Some("https://api-tn10.dotk.name/v1/snapshot"),
        _ => None,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Bootstrap {
    File,
    Fetch {
        url: String,
        explicit: bool,
    },
    /// Cold start from the current virtual parent.
    None,
}

/// A present file wins over any URL, because the operator put it there on purpose.
pub(crate) fn decide(file_present: bool, url: &SnapshotUrl, network: &str) -> Bootstrap {
    if file_present {
        return Bootstrap::File;
    }
    match url {
        SnapshotUrl::None => Bootstrap::None,
        SnapshotUrl::Url(u) => Bootstrap::Fetch { url: u.clone(), explicit: true },
        SnapshotUrl::Builtin => match builtin_snapshot_url(network) {
            Some(u) => Bootstrap::Fetch { url: u.to_string(), explicit: false },
            None => Bootstrap::None,
        },
    }
}

/// Only a 5xx or a lost connection is retried, such as the 503 an indexer answers before its first proof.
const FETCH_ATTEMPTS: u32 = 3;
const FETCH_RETRY_DELAY: std::time::Duration = std::time::Duration::from_secs(2);

const FETCH_REDIRECTS: usize = 5;

/// Under `rustls-no-provider` reqwest brings no crypto provider, and a preconfigured config carries
/// its own roots and ALPN.
fn tls_config() -> Result<rustls::ClientConfig> {
    let roots = rustls::RootCertStore { roots: webpki_roots::TLS_SERVER_ROOTS.to_vec() };
    let mut config = rustls::ClientConfig::builder_with_provider(std::sync::Arc::new(rustls::crypto::ring::default_provider()))
        .with_safe_default_protocol_versions()?
        .with_root_certificates(roots)
        .with_no_client_auth();
    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    Ok(config)
}

/// Uses the URL verbatim, so a static mirror file and `…/v1/snapshot?proven=false` both work.
pub async fn fetch(url: &str, max_bytes: u64) -> Result<Snapshot> {
    let client = reqwest::Client::builder()
        .tls_backend_preconfigured(tls_config()?)
        .connect_timeout(std::time::Duration::from_secs(10))
        .timeout(std::time::Duration::from_mins(10))
        .user_agent(format!("dotk-indexer/{}", crate::config::VERSION))
        .redirect(reqwest::redirect::Policy::custom(|attempt| {
            let downgraded = attempt.url().scheme() == "http" && attempt.previous().iter().any(|u| u.scheme() == "https");
            if downgraded {
                attempt.error("a redirect from https to http is refused")
            } else if attempt.previous().len() >= FETCH_REDIRECTS {
                attempt.error(format!("more than {FETCH_REDIRECTS} redirects"))
            } else {
                attempt.follow()
            }
        }))
        .build()
        .context("building the http client")?;

    let mut last: Option<anyhow::Error> = None;
    for attempt in 1..=FETCH_ATTEMPTS {
        if attempt > 1 {
            tokio::time::sleep(FETCH_RETRY_DELAY).await;
        }
        match attempt_fetch(&client, url, max_bytes).await {
            Ok(body) => {
                log::info!("fetched {} bytes from {url}", body.len());
                return serde_json::from_slice(&body).with_context(|| format!("{url} does not answer a /snapshot body"));
            }
            Err(FetchError::Transient(e)) => {
                log::warn!("attempt {attempt}/{FETCH_ATTEMPTS} for {url}: {e:#}");
                last = Some(e);
            }
            Err(FetchError::Fatal(e)) => return Err(e),
        }
    }
    Err(last.expect("a loop that fell through retried at least once"))
}

enum FetchError {
    Transient(anyhow::Error),
    Fatal(anyhow::Error),
}

async fn attempt_fetch(client: &reqwest::Client, url: &str, max_bytes: u64) -> std::result::Result<Vec<u8>, FetchError> {
    let mut response =
        client.get(url).send().await.map_err(|e| FetchError::Transient(anyhow::Error::new(e).context(format!("GET {url}"))))?;
    let status = response.status();
    if !status.is_success() {
        let e = anyhow::anyhow!("GET {url}: HTTP {status}");
        return Err(if status.is_server_error() { FetchError::Transient(e) } else { FetchError::Fatal(e) });
    }
    let too_big = |n: String| FetchError::Fatal(anyhow::anyhow!("{url} answers {n} bytes, above --snapshot-max-bytes {max_bytes}"));
    if let Some(n) = response.content_length()
        && n > max_bytes
    {
        return Err(too_big(n.to_string()));
    }
    // A body without a length header is bounded only by what has arrived.
    let mut body = Vec::with_capacity(crate::convert::fit(response.content_length().unwrap_or(0).min(max_bytes)).unwrap_or(0));
    while let Some(chunk) =
        response.chunk().await.map_err(|e| FetchError::Transient(anyhow::Error::new(e).context(format!("reading {url}"))))?
    {
        if body.len() as u64 + chunk.len() as u64 > max_bytes {
            return Err(too_big(format!("more than {max_bytes}")));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

#[cfg(test)]
#[path = "bootstrap_tests.rs"]
mod tests;
