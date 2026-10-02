use super::*;

fn k(b: u8) -> [u8; 32] {
    [b; 32]
}

fn active(name: &str) -> DeedRow {
    DeedRow::active(name.into(), 0, [1u8; 32], (k(0xaa), 0), 20_000_000)
}

#[test]
fn matches_whole_table_derivation() {
    let rows = vec![
        (k(0x20), active("a")),
        (k(0x40), DeedRow::pending(k(0x99), (k(0x11), 2), 120_000_000, 7)),
        (k(0x60), DeedRow::owner_unknown(Some("c".into()))),
    ];
    let all = derive_gaps(&rows);
    for probe in [k(0x10), k(0x20), k(0x30), k(0x40), k(0x50), k(0x60), k(0x70), k(0xff)] {
        let at = rows.iter().find(|(rk, _)| *rk == probe).map(|(_, r)| r);
        let pred = rows.iter().rfind(|(rk, _)| *rk < probe);
        let succ = rows.iter().find(|(rk, _)| *rk > probe);
        let n = neighborhood(&probe, at, pred, succ);
        if at.is_some() {
            let (lower, upper) = n.neighbors.expect("an occupied key is flanked by two gaps");
            assert!(all.contains(&lower) && all.contains(&upper), "both flanks are real gaps at {probe:?}");
            assert_eq!((lower.hi, upper.lo), (probe, probe), "the key is the seam between them");
            assert!(n.covering.is_none(), "a live key is a gap bound, never inside one");
        } else {
            assert!(n.neighbors.is_none(), "a free key has no flanking pair to spend");
            assert_eq!(n.covering, all.iter().find(|g| g.contains(&probe)).copied(), "covering of {probe:?}");
        }
    }
}

#[test]
fn empty_registry_is_one_gap() {
    assert_eq!(derive_gaps(&[]), vec![GapState { lo: KEY_MIN, hi: KEY_MAX }]);
    let n = neighborhood(&k(0x42), None, None, None);
    assert!(n.deed.is_none() && n.neighbors.is_none());
    assert_eq!(n.covering, Some(GapState { lo: KEY_MIN, hi: KEY_MAX }));
}

#[test]
fn degenerate_bounds_never_invent_a_covering() {
    let rows = [(k(0x30), active("a"))];
    for (probe, pred, succ) in [(KEY_MIN, None, Some(&rows[0])), (KEY_MAX, Some(&rows[0]), None)] {
        let n = neighborhood(&probe, None, pred, succ);
        assert!(n.covering.is_none(), "no gap strictly contains {probe:?}");
        assert!(n.deed.is_none() && n.neighbors.is_none());
    }
}

#[test]
fn an_owner_unknown_row_holds_its_place_without_an_address() {
    let row = DeedRow::owner_unknown(Some("x".into()));
    let n = neighborhood(&k(0x30), Some(&row), None, None);
    assert!(n.deed.is_none(), "no owner, no derivable deed state");
    assert_eq!(n.neighbors, Some((GapState { lo: KEY_MIN, hi: k(0x30) }, GapState { lo: k(0x30), hi: KEY_MAX })));
}
