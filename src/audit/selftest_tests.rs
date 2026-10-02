use super::*;

#[test]
fn the_withheld_set_covers_failing_and_flux_gaps_alike() {
    let mut check = CheckResult {
        failing_gaps: BTreeMap::new(),
        failing_deeds: HashSet::new(),
        failing_outpoints: HashMap::new(),
        flux: Flux::default(),
        observed_gaps: vec![],
        gaps_checked: 0,
        deeds_checked: 0,
        pending_checked: 0,
    };
    let [a, b, c, d] = [[0x10; 32], [0x30; 32], [0xc0; 32], [0xe0; 32]];
    check.failing_gaps.insert(c, GapState { lo: c, hi: d });
    check.flux.gaps.insert(a, GapState { lo: a, hi: b });
    let withheld = check.withheld();
    assert!(withheld.withholds_key(&[0xd0; 32]));
    assert!(withheld.withholds_key(&[0x20; 32]));
    assert!(!withheld.withholds_key(&[0x50; 32]));
}
