use dotk_core::fees::{assemble_unfunded_evict_with_auto_fee, assemble_with_auto_fee};
use dotk_core::intents::{DeedUtxo, GapUtxo};
use dotk_core::state::{DeedState, GapState};

use super::*;

const KEY: &str = "1111111111111111111111111111111111111111111111111111111111111111";

fn args(key: Option<&str>, address: Option<&str>) -> CliArgs {
    let mut args = CliArgs::defaults();
    args.evictor_key = key.map(str::to_string);
    args.evictor_address = address.map(str::to_string);
    args
}

#[test]
fn the_wallet_flags_pick_the_evictor_mode() {
    let addr = Payout::from_key(KEY, Prefix::Testnet).unwrap().address.to_string();

    assert!(Payout::from_args(&args(None, None), Prefix::Testnet).unwrap().is_none(), "no wallet switches the evictor off");
    assert!(Payout::from_args(&args(Some(KEY), Some(&addr)), Prefix::Testnet).is_err(), "a key plus an address must refuse");

    let unfunded = Payout::from_args(&args(None, Some(&addr)), Prefix::Testnet).unwrap().expect("an address alone runs unfunded");
    assert!(unfunded.secret.is_none(), "unfunded mode has nothing to sign with");
    assert_eq!(unfunded.address.to_string(), addr);
    assert!(Payout::from_args(&args(Some(KEY), None), Prefix::Testnet).unwrap().unwrap().secret.is_some());

    for (key, address) in [(None, None), (Some(KEY), None), (None, Some(addr.as_str()))] {
        let a = args(key, address);
        assert_eq!(Payout::from_args(&a, Prefix::Testnet).unwrap().is_some(), a.evictor_on(), "the spawn follows the mode");
    }

    assert!(Payout::from_args(&args(Some("zz"), None), Prefix::Testnet).is_err(), "a malformed key fails at boot");
    assert!(Payout::from_args(&args(None, Some("kaspatest:nonsense")), Prefix::Testnet).is_err(), "a malformed address fails at boot");
    assert!(Payout::unfunded(&addr, Prefix::Mainnet).is_err(), "the address must be on this network");
    for bad in ["", "kaspatest:nonsense", &addr[..addr.len() - 1]] {
        assert!(Payout::unfunded(bad, Prefix::Testnet).is_err(), "{bad:?} is not an address");
    }
}

fn op(b: u8) -> Outpoint {
    Outpoint { transaction_id: hex32(&[b; 32]), index: 0 }
}

/// An evict of a PENDING `kaspa` on the built-in testnet deployment.
fn evict_intent() -> dotk_core::intents::TxIntent {
    let (genesis, _) = crate::genesis::builtin("testnet-10").unwrap();
    let params = genesis.params.clone();
    let key = dotk_core::names::key_of("kaspa");
    let covenant_id = "cc".repeat(32);
    let gap =
        |lo, hi, b| GapUtxo { outpoint: op(b), value: params.gap_value, state: GapState { lo, hi }, covenant_id: covenant_id.clone() };
    let pred = gap(dotk_core::registry::KEY_MIN, key, 0xa0);
    let succ = gap(key, dotk_core::registry::KEY_MAX, 0xa2);
    let deed = DeedUtxo {
        outpoint: op(0xa1),
        value: params.bond + params.deposit,
        state: DeedState::pending(key, [7u8; 32]),
        covenant_id: covenant_id.clone(),
    };
    genesis.watch_templates().unwrap().build_evict(&pred, &deed, &succ, &params).unwrap()
}

#[test]
fn a_key_funded_exit_that_lost_its_input_is_refused() {
    let payout = Payout::from_key(KEY, Prefix::Testnet).unwrap();
    let refused = super::super::exits::assemble(&payout, "testnet-10", 1.0.into(), &evict_intent(), None);
    assert!(refused.is_err(), "it must fail, not go out unfunded");
}

#[test]
fn an_evict_is_signed_by_its_funding_input() {
    let payout = Payout::from_key(KEY, Prefix::Testnet).unwrap();
    let intent = evict_intent();
    let secret = payout.secret.expect("key mode holds one");
    assert!(
        assemble_with_auto_fee(&intent, &[], &payout.spk_hex, "testnet-10", 1.0).is_err(),
        "an unfunded evict leaves its bounty a bearer value and must not be built by the ordinary door"
    );
    let funding = vec![FundingUtxo { outpoint: op(0xf0), value: 100_000_000, spk: payout.spk_hex.clone() }];
    let mut asm = assemble_with_auto_fee(&intent, &funding, &payout.spk_hex, "testnet-10", 1.0).expect("a funded evict");
    assert_eq!(asm.unsigned_inputs, vec![3], "funding sits strictly after the exit's three seats");
    sign_funding(&mut asm, &secret).expect("sign");
    assert!(asm.unsigned_inputs.iter().all(|&i| !asm.tx.inputs[i].signature_script.is_empty()), "every funding input is signed");

    let sig = dotk_core::sign::schnorr_sign_input(&asm.tx, &asm.entries, 3, &secret).unwrap();
    let mut rewritten = asm.tx.clone();
    rewritten.outputs[2].script_public_key = kaspa_consensus_core::tx::ScriptPublicKey::from_vec(0, vec![0x51]);
    let after = dotk_core::sign::schnorr_sign_input(&rewritten, &asm.entries, 3, &secret).unwrap();
    assert_ne!(sig, after, "SIGHASH_ALL must commit to the payout output");

    let bare = assemble_unfunded_evict_with_auto_fee(&intent, &payout.spk_hex, "testnet-10", 1.0).expect("an unfunded evict");
    assert!(bare.unsigned_inputs.is_empty(), "there is no funding input, so nothing is left to sign");
    assert_eq!(bare.tx.inputs.len(), 3, "the three covenant seats, and only those");
    assert!(bare.tx.inputs.iter().all(|i| !i.signature_script.is_empty()), "every seat still carries its redeem");
    assert_eq!(bare.tx.outputs[0], asm.tx.outputs[0], "the merged gap is the same output the funded evict produces");
    assert_eq!(bare.tx.outputs[1], asm.tx.outputs[1], "and so is the deposit's output: no door past it, funded or not");
    assert_eq!(bare.change.map(|(idx, _)| idx), Some(2), "the bounty rides out at output 2, committed to by nothing");
}
