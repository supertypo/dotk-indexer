use super::*;

/// The node's text sits under the call's context, and only the whole chain carries it.
#[test]
fn an_unknown_start_hash_is_unresumable_under_the_call_context() {
    let e = anyhow!("cannot find header 00ab").context("getVirtualChainFromBlockV2");
    let e = unresumable(e);
    assert!(e.is::<Unresumable>());
    assert!(e.to_string().contains("cannot find header"), "{e}");
    assert!(!unresumable(anyhow!("timeout").context("getVirtualChainFromBlockV2")).is::<Unresumable>());
}

/// A node that leaves out a field that High verbosity carries misbehaves, and the batch stops
/// rather than read the field as empty.
#[test]
fn a_transaction_without_a_field_high_verbosity_carries_is_refused() {
    use kaspa_consensus_core::tx::{Transaction, TransactionInput, TransactionOutpoint, TransactionOutput};
    let consensus = Transaction::new(
        0,
        vec![TransactionInput::new(TransactionOutpoint::new(7.into(), 0), vec![1, 2], 0, 1)],
        vec![TransactionOutput::new(1, kaspa_consensus_core::tx::ScriptPublicKey::from_vec(0, vec![0x51]))],
        0,
        kaspa_consensus_core::subnets::SUBNETWORK_ID_NATIVE,
        0,
        vec![9],
    );
    let mut whole = kaspa_rpc_core::RpcOptionalTransaction::from(&consensus);
    whole.verbose_data = Some(kaspa_rpc_core::RpcOptionalTransactionVerboseData {
        transaction_id: Some(consensus.id()),
        hash: None,
        compute_mass: None,
        block_hash: None,
        block_time: None,
    });
    let read = accepted_tx(&whole).unwrap();
    assert_eq!((read.payload, read.sig_scripts), (vec![9], vec![vec![1, 2]]));

    let mut no_payload = whole.clone();
    no_payload.payload = None;
    assert!(accepted_tx(&no_payload).unwrap_err().to_string().contains("payload"));
    let mut no_sig_script = whole;
    no_sig_script.inputs[0].signature_script = None;
    assert!(accepted_tx(&no_sig_script).unwrap_err().to_string().contains("signature script"));
}

/// A UTXO answer names the address of each entry, and an entry without one stops the work that
/// asked, rather than read as a missing UTXO.
#[test]
fn a_utxo_entry_without_an_address_is_refused() {
    let entry = |address| kaspa_rpc_core::RpcUtxosByAddressesEntry {
        address,
        outpoint: kaspa_rpc_core::RpcTransactionOutpoint { transaction_id: 7.into(), index: 0 },
        utxo_entry: kaspa_rpc_core::RpcUtxoEntry::new(
            1,
            kaspa_consensus_core::tx::ScriptPublicKey::from_vec(0, vec![0x51]),
            0,
            false,
            None,
        ),
    };
    let address = Address::new(kaspa_addresses::Prefix::Testnet, kaspa_addresses::Version::PubKey, &[1; 32]);
    assert_eq!(utxo_hits(vec![entry(Some(address.clone()))]).unwrap()[0].address, address.to_string());
    assert!(utxo_hits(vec![entry(Some(address)), entry(None)]).unwrap_err().to_string().contains("no address"));
}
