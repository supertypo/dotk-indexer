//! The evictor's wallet, which is its own payout address, so the change of one exit funds the next.

use anyhow::{Context, Result};
use dotk_core::assemble::{AssembledTx, FundingUtxo};
use dotk_core::intents::Outpoint;
use kaspa_addresses::{Address, Prefix, Version as AddrVersion};

use crate::app::Backends;
use crate::config::CliArgs;
use crate::convert::hex32;

pub(crate) struct Payout {
    pub(crate) address: Address,
    pub(super) spk_hex: String,
    pub(super) secret: Option<[u8; 32]>,
}

impl Payout {
    /// `None` when neither flag is given, which switches the evictor off.
    pub(crate) fn from_args(args: &CliArgs, prefix: Prefix) -> Result<Option<Self>> {
        match (args.evictor_key.as_deref(), args.evictor_address.as_deref()) {
            (Some(_), Some(_)) => {
                anyhow::bail!("--evictor-key excludes --evictor-address, because the key's address collects the bounty")
            }
            (Some(key), None) => Self::from_key(key, prefix).map(Some),
            (None, Some(address)) => Self::unfunded(address, prefix).map(Some),
            (None, None) => Ok(None),
        }
    }

    fn from_key(key_hex: &str, prefix: Prefix) -> Result<Self> {
        let mut secret = [0u8; 32];
        let hex = key_hex.trim();
        anyhow::ensure!(hex.len() == 64, "--evictor-key must be 64 hex characters, got {}", hex.len());
        faster_hex::hex_decode(hex.as_bytes(), &mut secret).context("--evictor-key is not hex")?;
        // A schnorr address carries the x-only key, the compressed key without its parity byte.
        let compressed = dotk_core::sign::compressed_public_key(&secret).context("--evictor-key")?;
        Ok(Self::at(Address::new(prefix, AddrVersion::PubKey, &compressed[1..]), Some(secret)))
    }

    fn unfunded(declared: &str, prefix: Prefix) -> Result<Self> {
        let address: Address = declared.trim().try_into().map_err(|e| anyhow::anyhow!("--evictor-address {declared}: {e}"))?;
        anyhow::ensure!(address.prefix == prefix, "--evictor-address {address} is not on this network ({prefix})");
        Ok(Self::at(address, None))
    }

    fn at(address: Address, secret: Option<[u8; 32]>) -> Self {
        let spk_hex = faster_hex::hex_string(kaspa_txscript::pay_to_address_script(&address).script());
        Self { address, spk_hex, secret }
    }

    pub(crate) fn is_unfunded(&self) -> bool {
        self.secret.is_none()
    }
}

/// An input counts as spent only once a signed transaction names it. The scan order is fixed, so
/// an input charged to a deed the tick only looked at starves every deed behind it.
pub(super) struct Wallet {
    utxos: Vec<FundingUtxo>,
    spent: usize,
}

impl Wallet {
    pub(super) fn peek(&self) -> Option<&FundingUtxo> {
        self.utxos.get(self.spent)
    }

    pub(super) fn commit(&mut self) {
        self.spent += 1;
    }
}

/// One funding UTXO per exit, never chained, because the node rejects an exit while an identical
/// one is queued. `RbfPolicy::Forbidden` likewise rejects a rebuild that pairs inputs differently.
pub(super) async fn wallet_utxos(backends: &Backends, payout: &Payout) -> Result<Wallet> {
    let mut utxos: Vec<FundingUtxo> = backends.kaspad
        .utxos_by_addresses(std::slice::from_ref(&payout.address))
        .await?
        .into_iter()
        // A covenant UTXO spent as funding sits at a seat that no covenant expects.
        .filter(|hit| hit.covenant_id.is_none())
        .map(|hit| FundingUtxo {
            outpoint: Outpoint { transaction_id: hex32(&hit.txid), index: hit.index },
            value: hit.amount,
            spk: payout.spk_hex.clone(),
        })
        .collect();
    utxos.sort_by(|a, b| (&a.outpoint.transaction_id, a.outpoint.index).cmp(&(&b.outpoint.transaction_id, b.outpoint.index)));
    Ok(Wallet { utxos, spent: 0 })
}

/// No exit signs a covenant seat and Kaspa sighashes exclude signature scripts, so only this
/// SIGHASH_ALL signature commits to the outputs. Without it any miner can take the bounty.
pub(super) fn sign_funding(assembled: &mut AssembledTx, secret: &[u8; 32]) -> Result<()> {
    for idx in assembled.unsigned_inputs.clone() {
        let sig = dotk_core::sign::schnorr_sign_input(&assembled.tx, &assembled.entries, idx, secret)?;
        assembled.tx.inputs[idx].signature_script = dotk_core::watch::push_data(&sig);
    }
    assembled.tx.finalize();
    Ok(())
}

#[cfg(test)]
#[path = "payout_tests.rs"]
mod tests;
