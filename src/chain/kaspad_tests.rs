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
