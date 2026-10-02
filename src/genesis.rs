//! The built-in deployment manifests.

use anyhow::{Context, Result, bail};
use axum::body::Bytes;
use dotk_core::watch::GenesisFile;

/// The deployments this build carries.
pub const NETWORKS: [&str; 2] = ["mainnet", "testnet-10"];

/// The parsed manifest and the exact bytes `/genesis` serves.
pub fn builtin(network: &str) -> Result<(GenesisFile, Bytes)> {
    let raw: &'static [u8] = match network {
        "mainnet" => include_bytes!("../genesis/mainnet.json"),
        "testnet-10" => include_bytes!("../genesis/testnet-10.json"),
        _ => bail!("no deployment on network {network}"),
    };
    parse(Bytes::from_static(raw))
}

pub fn parse(raw: impl Into<Bytes>) -> Result<(GenesisFile, Bytes)> {
    let raw = raw.into();
    let genesis = serde_json::from_slice(&raw).context("parsing the deployment manifest")?;
    Ok((genesis, raw))
}

#[cfg(test)]
#[path = "genesis_tests.rs"]
mod tests;
