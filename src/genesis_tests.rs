use super::*;

#[test]
fn every_builtin_deployment_loads() {
    for network in NETWORKS {
        let (genesis, _) = builtin(network).unwrap();
        assert_eq!(genesis.network, network);
        assert_eq!(genesis.version, dotk_core::watch::GENESIS_VERSION, "{network}");
        let watch = genesis.watch_templates().unwrap_or_else(|e| panic!("{network}: {e}"));
        genesis.verify_genesis_binding().unwrap_or_else(|e| panic!("{network}: {e}"));
        crate::identity::of(&genesis, &watch).unwrap_or_else(|e| panic!("{network}: {e:#}"));
    }
}
