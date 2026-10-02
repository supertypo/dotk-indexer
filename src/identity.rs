//! Pins the deployment's manifest facts in `vars`, coarsest first, so a database is never reused
//! under another deployment.

use anyhow::{Result, bail};
use dotk_core::watch::{GenesisFile, WatchTemplates};
use sqlx::PgPool;

use crate::db;

const VAR_NETWORK: &str = "network";
const VAR_REGISTRY_COVENANT_ID: &str = "registry_covenant_id";
const VAR_GAP_TEMPLATE_HASH: &str = "gap_template_hash";
const VAR_DEED_TEMPLATE_HASH: &str = "deed_template_hash";
const VAR_DEVFUND_FINGERPRINT: &str = "devfund_fingerprint";

#[derive(Debug)]
struct Field {
    key: &'static str,
    label: &'static str,
    value: String,
}

#[derive(Debug)]
pub struct Identity {
    fields: Vec<Field>,
}

/// Pinning only catches a change, so this also checks that the versioned devfund SPK occurs in
/// the deed template. The pipeline tells an operation from the genesis mint by that
/// fingerprint, so a wrong one indexes nothing while reporting healthy.
pub fn of(genesis: &GenesisFile, watch: &WatchTemplates) -> Result<Identity> {
    let fingerprint = &watch.fingerprint;
    if !watch.deed.bytecode.windows(fingerprint.len().max(1)).any(|w| w == fingerprint.as_slice()) {
        bail!(
            "the built-in manifest's params.devfund_spk does not occur in its deed template bytecode, so \
             the manifest's pricing and its bytecode describe different deployments. The \
             fingerprint is what the pipeline tells an operation by, so this build indexes \
             nothing while it reports healthy. Regenerate the manifest for this deployment."
        );
    }
    Ok(Identity {
        fields: vec![
            Field { key: VAR_NETWORK, label: "network", value: genesis.network.clone() },
            Field { key: VAR_REGISTRY_COVENANT_ID, label: "registryCovenantId", value: genesis.registry_covenant_id.clone() },
            Field { key: VAR_GAP_TEMPLATE_HASH, label: "gapTemplateHash", value: genesis.gap_template_hash.clone() },
            Field { key: VAR_DEED_TEMPLATE_HASH, label: "deedTemplateHash", value: genesis.deed_template_hash.clone() },
            Field { key: VAR_DEVFUND_FINGERPRINT, label: "devfund fingerprint", value: faster_hex::hex_string(fingerprint) },
        ],
    })
}

/// Only for a database that initialization just created or wiped.
pub async fn write(pool: &PgPool, id: &Identity) -> Result<()> {
    let mut tx = pool.begin().await?;
    for f in &id.fields {
        db::set_var(&mut tx, f.key, &f.value).await?;
    }
    tx.commit().await?;
    Ok(())
}

pub async fn check(pool: &PgPool, id: &Identity) -> Result<()> {
    let mut conn = pool.acquire().await?;
    for f in &id.fields {
        match db::get_var(&mut conn, f.key).await? {
            Some(stored) if stored == f.value => {}
            Some(stored) => bail!(
                "this database was built for a different deployment: {} is {stored} in the \
                 database but {} in this deployment. Every deed row was derived under the stored \
                 deployment and means nothing under this one. Check --network, or wipe and re-sync \
                 with -c/--initialize-db.",
                f.label,
                f.value
            ),
            None => bail!(
                "this database carries no {} in its deployment identity, so it is not one this \
                 indexer wrote. Wipe and re-sync with -c/--initialize-db.",
                f.label
            ),
        }
    }
    Ok(())
}
