use std::path::PathBuf;
use std::time::Duration;

use clap::Parser;

pub const VERSION: &str = env!("VERGEN_GIT_DESCRIBE");

/// Finality depth (10 bps for 43,200 s, about 12 h) plus margin. Below finality, a removed
/// hash that is absent from the journal does not prove the block eventless.
pub const JOURNAL_RETENTION_FLOOR: u64 = 500_000;

fn parse_duration(s: &str) -> Result<Duration, String> {
    humantime::parse_duration(s).map_err(|e| e.to_string())
}

fn parse_selftest_interval(s: &str) -> Result<Duration, String> {
    let d = parse_duration(s)?;
    if (Duration::from_secs(10)..=Duration::from_mins(60)).contains(&d) {
        Ok(d)
    } else {
        Err(format!("self-test interval must be between 10s and 60m (got {s})"))
    }
}

fn parse_base_path(s: &str) -> Result<String, String> {
    let trimmed = s.trim_end_matches('/');
    if trimmed.is_empty() {
        return Ok(String::new());
    }
    if !trimmed.starts_with('/') {
        return Err(format!("base path must start with '/' (got {s:?})"));
    }
    Ok(trimmed.to_string())
}

/// A scheme, path, port or leading `*.` matches no `Host` header, so it fails at startup.
fn parse_gateway_domain(s: &str) -> Result<String, String> {
    let domain = s.trim().trim_end_matches('.').to_ascii_lowercase();
    if domain.is_empty() {
        return Ok(String::new());
    }
    if domain.contains("://") || domain.contains('/') || domain.contains(':') {
        return Err(format!("{s:?} is not a domain (expected a bare host name such as kaspa.name)"));
    }
    if domain.starts_with("*.") {
        return Err(format!("{s:?}: give the domain without the wildcard label (kaspa.name, not *.kaspa.name)"));
    }
    let label_ok = |l: &str| !l.is_empty() && l.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'-');
    if !domain.split('.').all(label_ok) {
        return Err(format!("{s:?} is not a domain (a label is a-z, 0-9 and hyphen)"));
    }
    Ok(domain)
}

/// Validated at startup, because a typo otherwise surfaces only as a silent browser-side block.
fn parse_allowed_origins(s: &str) -> Result<String, String> {
    let spec = s.trim();
    if spec == "*" {
        return Ok(spec.to_string());
    }
    for origin in spec.split(',') {
        let origin = origin.trim();
        if origin == "*" {
            return Err("'*' cannot be combined with explicit origins, use it alone".into());
        }
        // A browser compares `Origin` byte for byte, so a stray '/' matches nothing.
        let Some((scheme, host)) = origin.split_once("://") else {
            return Err(format!("{origin:?} is not an origin (expected scheme://host[:port])"));
        };
        if scheme.is_empty() || host.is_empty() || host.contains('/') {
            return Err(format!("{origin:?} is not an origin (expected scheme://host[:port], no path or trailing slash)"));
        }
    }
    Ok(spec.to_string())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SnapshotUrl {
    /// Resolved to the published snapshot of the network.
    Builtin,
    /// Never fetch. A present `--snapshot-file` is still imported.
    None,
    Url(String),
}

fn parse_snapshot_url(s: &str) -> Result<SnapshotUrl, String> {
    let spec = s.trim();
    match spec {
        "builtin" => return Ok(SnapshotUrl::Builtin),
        "none" | "" => return Ok(SnapshotUrl::None),
        _ => {}
    }
    let url = reqwest::Url::parse(spec).map_err(|e| format!("{spec:?} is not \"builtin\", \"none\" or a URL: {e}"))?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err(format!("{spec:?} is a {} URL, and a snapshot is fetched over http or https", url.scheme()));
    }
    // Keeps the operator's spelling, so log lines match the command.
    Ok(SnapshotUrl::Url(spec.to_string()))
}

fn parse_journal_retention(s: &str) -> Result<u64, String> {
    let v: u64 = s.parse().map_err(|e: std::num::ParseIntError| e.to_string())?;
    if v != 0 && v < JOURNAL_RETENTION_FLOOR {
        return Err(format!(
            "journal retention must be 0 (forever) or at least {JOURNAL_RETENTION_FLOOR} blue scores, the finality depth plus margin"
        ));
    }
    Ok(v)
}

// No `Debug`, because the arguments hold the database password and the evictor key.
#[derive(Clone, Parser)]
#[expect(clippy::doc_markdown, reason = "the doc comments are the --help text, and backticks print literally")]
#[command(name = "dotk-indexer", version = VERSION, about = "dotk.name: indexer, self-proving registry API, evictor")]
pub struct CliArgs {
    /// Kaspa network. Selects which built-in deployment to serve.
    #[arg(short, long, default_value = "mainnet", value_parser = crate::genesis::NETWORKS, help_heading = "Connection")]
    pub network: String,
    /// Borsh wRPC endpoints, tried in order. Repeat the flag or separate with commas.
    /// "resolver" is the public-node resolver. It lags, so it belongs last.
    #[arg(short = 's', long = "rpc-url", value_name = "URL", value_delimiter = ',', required = true, help_heading = "Connection")]
    pub rpc_urls: Vec<String>,
    /// Postgres connection string.
    #[arg(
        short,
        long,
        env = "DATABASE_URL",
        hide_env_values = true,
        default_value = "postgres://postgres:postgres@localhost:5432/postgres",
        help_heading = "Connection"
    )]
    pub database_url: String,

    /// Drop and recreate all tables, then bootstrap from --snapshot-file or --snapshot-url.
    /// Implicit when the schema is missing.
    #[arg(short = 'c', long, help_heading = "Bootstrap")]
    pub initialize_db: bool,
    /// A /v1/snapshot body to import on initialization. The indexer deletes it after the import.
    #[arg(long, default_value = "./snapshot.json", help_heading = "Bootstrap")]
    pub snapshot_file: PathBuf,
    /// Where to fetch a /v1/snapshot body when initializing without --snapshot-file:
    /// "builtin" (this network's published registry), "none", or a URL.
    ///
    /// A mirror needs no trust, because import refuses another registry's body and the
    /// self-test proves the rest against the UTXO set. A failed fetch is fatal, and the next
    /// start retries.
    #[arg(long, default_value = "builtin", value_parser = parse_snapshot_url, help_heading = "Bootstrap")]
    pub snapshot_url: SnapshotUrl,
    /// Size limit for a fetched snapshot body, in bytes, so a mirror cannot exhaust memory.
    /// A registry of a million names fits well inside the default.
    #[arg(long, default_value_t = 1 << 30, value_parser = clap::value_parser!(u64).range(1..), help_heading = "Bootstrap")]
    pub snapshot_max_bytes: u64,

    /// Poll interval for the virtual-chain follow loop.
    #[arg(long, default_value = "1s", value_parser = parse_duration, help_heading = "Processing")]
    pub vcp_interval: Duration,
    /// Confirmation depth in blue scores. The indexer leaves unread the chain blocks within
    /// this distance below the sink, the node's tip. 0 reads up to the tip, 50 at most.
    #[arg(long, default_value_t = 2, value_parser = clap::value_parser!(u64).range(..=50), help_heading = "Processing")]
    pub vcp_tip_distance: u64,
    /// Journal retention in blue scores. 0 keeps it forever. The indexer refuses a value below
    /// 500000, the finality depth plus margin.
    #[arg(long, default_value_t = JOURNAL_RETENTION_FLOOR, value_parser = parse_journal_retention, help_heading = "Processing")]
    pub journal_retention: u64,
    /// Periodic self-test interval, 10s to 60m. The startup self-test always runs.
    #[arg(long, default_value = "10m", value_parser = parse_selftest_interval, help_heading = "Processing")]
    pub selftest_interval: Duration,
    /// Addresses per getUtxosByAddresses call in the self-test. The node holds its
    /// utxoindex read lock for the whole call, so a large batch stalls its index updates.
    #[arg(long, default_value_t = 400, value_parser = clap::value_parser!(u64).range(1..=100_000), help_heading = "Processing")]
    pub selftest_probe_chunk: u64,

    /// Schnorr private key (64 hex characters) that funds and signs every evict. Its address
    /// receives the bounties (the name bond plus one gap value per evicted deed) and the change.
    /// The deed's DEPOSIT goes to the protocol's development fund, never to the evictor.
    ///
    /// Kaspa sighashes exclude signature scripts, so without a signed funding input nothing
    /// commits to the payout and a miner can take it.
    ///
    /// Without --evictor-key or --evictor-address, the evictor is off.
    #[arg(long, env = "DOTK_EVICTOR_KEY", hide_env_values = true, help_heading = "Evictor")]
    pub evictor_key: Option<String>,
    /// Address that receives evict bounties without a signing key. Every evict goes out
    /// unfunded and unsigned, so anyone can take its bounty. Evictions stay correct, because a
    /// miner who rewrites one can change only who gets the bounty. Excludes --evictor-key.
    #[arg(long, help_heading = "Evictor")]
    pub evictor_address: Option<String>,

    /// Listen address for the API and the status page.
    #[arg(short, long, default_value = "0.0.0.0:7799", help_heading = "Web server")]
    pub listen: String,
    /// Base path for the whole surface: the status page at {base}/, the API and docs at
    /// {base}/v1. Empty serves from the root.
    #[arg(long, default_value = "", value_parser = parse_base_path, help_heading = "Web server")]
    pub base_path: String,
    /// Cache-Control max-age on API responses. 0 sends no-store. /v1/health and errors always
    /// send no-store.
    #[arg(short = 't', long, default_value = "2s", value_parser = parse_duration, help_heading = "Web server")]
    pub cache_ttl: Duration,
    /// Cache-Control max-age for the proven /v1/snapshot, which changes only at self-test
    /// cadence.
    #[arg(long, default_value = "60s", value_parser = parse_duration, help_heading = "Web server")]
    pub cache_ttl_snapshot: Duration,
    /// How long one /v1/snapshot?proven=false export is reused. The body advertises the rest of
    /// that window as max-age, so edges and browsers also cache it that long. 0 exports on
    /// every call and sends no-store.
    #[arg(long, default_value = "2s", value_parser = parse_duration, help_heading = "Web server")]
    pub cache_ttl_snapshot_live: Duration,
    /// Browser origins allowed to call the API: "*" or a comma-separated list of exact
    /// origins. The API is read-only and credential-free, so "*" grants a browser nothing
    /// curl lacks.
    #[arg(long, default_value = "*", value_parser = parse_allowed_origins, help_heading = "Web server")]
    pub allowed_origins: String,
    /// Gateway domain: a request whose Host header is {name}.{domain} redirects to the "url"
    /// record of {name}.k. Every label under the domain is a name, so the domain must serve
    /// nothing else. Empty turns the gateway off.
    #[arg(long, default_value = "", value_parser = parse_gateway_domain, help_heading = "Web server")]
    pub gateway_domain: String,
    /// Where the gateway sends a visitor for a name that is free or has no website, as
    /// {url}/names/{name}.
    #[arg(long, default_value = "https://dotk.name", help_heading = "Web server")]
    pub gateway_app_url: String,
    /// Web runtime worker threads (default: half the cores).
    #[arg(long, value_parser = clap::builder::RangedU64ValueParser::<usize>::new().range(1..=1024), help_heading = "Web server")]
    pub web_worker_threads: Option<usize>,
    /// Postgres connections for the web layer, in a pool of its own, so a flood of requests
    /// cannot starve the pipeline.
    #[arg(long, default_value_t = 10, value_parser = clap::value_parser!(u32).range(1..), help_heading = "Web server")]
    pub web_db_pool_size: u32,
    /// Open connections the web server holds at once. A connection past the cap waits in the
    /// listen backlog for one to close.
    #[arg(long, default_value_t = 512, value_parser = clap::value_parser!(u32).range(1..), help_heading = "Web server")]
    pub web_max_connections: u32,
    /// How long a request head can take to arrive, how long a keep-alive connection can wait
    /// for its next request, and how long a response can go unread.
    #[arg(long, default_value = "10s", value_parser = parse_duration, hide = true)]
    pub web_header_timeout: Duration,
    /// How long a request can take to answer.
    #[arg(long, default_value = "30s", value_parser = parse_duration, hide = true)]
    pub web_request_timeout: Duration,
    /// How long the node may fail every poll before the process exits for a restart.
    #[arg(long, default_value = "10m", value_parser = parse_duration, hide = true)]
    pub vcp_outage_limit: Duration,
    /// How long a /v1/keyspace or /v1/snapshot?proven=false build may run. Any other web
    /// statement runs at most --web-request-timeout.
    #[arg(long, default_value = "2m", value_parser = parse_duration, hide = true)]
    pub web_build_timeout: Duration,
    /// How long shutdown waits for open requests before it drops them.
    #[arg(long, default_value = "5s", value_parser = parse_duration, hide = true)]
    pub web_shutdown_deadline: Duration,

    /// Log filter in env_logger syntax, for example "info" or "dotk_indexer=debug". Color
    /// follows the terminal and the NO_COLOR variable.
    #[arg(long, default_value = "info", help_heading = "Logging")]
    pub log_level: String,

    /// How often the follow loop refreshes the tip blue score and logs catch-up progress.
    #[arg(long, default_value = "10s", value_parser = parse_duration, hide = true)]
    pub progress_interval: Duration,
    // The integration tests shorten these two to run at millisecond speed.
    /// How often the evictor scans for deeds to end.
    #[arg(long, default_value = "10s", value_parser = parse_duration, hide = true)]
    pub evictor_tick: Duration,
    /// Delay between self-test confirmation re-runs.
    #[arg(long, default_value = "10s", value_parser = parse_duration, hide = true)]
    pub selftest_confirm_delay: Duration,
}

impl CliArgs {
    pub fn defaults() -> Self {
        Self::parse_from(["dotk-indexer", "--rpc-url", "ws://127.0.0.1:17110"])
    }

    pub fn evictor_on(&self) -> bool {
        self.evictor_key.is_some() || self.evictor_address.is_some()
    }
}

#[cfg(test)]
#[path = "config_tests.rs"]
mod tests;
