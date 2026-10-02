use super::*;

fn key_of(name: &str) -> [u8; 32] {
    dotk_core::names::key_of(name)
}

fn row(name: &str) -> ExportRow {
    ExportRow {
        kind: 0,
        name: Some(name.into()),
        owner_type: Some(0),
        owner: Some("11".repeat(32)),
        claim: None,
        outpoint_txid: None,
        outpoint_index: None,
        value: None,
        accepted_daa: None,
    }
}

fn pending_row() -> ExportRow {
    ExportRow {
        kind: 1,
        name: None,
        owner_type: None,
        owner: None,
        claim: Some("22".repeat(32)),
        outpoint_txid: Some("33".repeat(32)),
        outpoint_index: Some(2),
        value: Some(120_000_000),
        accepted_daa: Some(7),
    }
}

fn unknown_row() -> ExportRow {
    ExportRow {
        kind: 2,
        name: None,
        owner_type: None,
        owner: None,
        claim: None,
        outpoint_txid: None,
        outpoint_index: None,
        value: None,
        accepted_daa: None,
    }
}

fn why(r: &ExportRow, key: [u8; 32]) -> String {
    DeedRow::try_from((&key, r)).expect_err("this row must not import").to_string()
}

#[test]
fn a_name_must_hash_to_the_key_its_row_is_filed_under() {
    let mismatch = why(&row("kaspa"), key_of("bitcoin"));
    assert!(mismatch.contains("does not hash to the key"), "{mismatch}");
    let named_unknown = ExportRow { name: Some("kaspa".into()), ..unknown_row() };
    DeedRow::try_from((&key_of("kaspa"), &named_unknown)).expect("an owner-unknown row may keep its name");
    assert!(why(&named_unknown, key_of("bitcoin")).contains("does not hash to the key"));
}

#[test]
fn a_pending_row_may_not_carry_a_name() {
    DeedRow::try_from((&[7u8; 32], &pending_row())).expect("a well-formed pending row imports");
    let smuggled = ExportRow { name: Some("kaspa".into()), ..pending_row() };
    let why = why(&smuggled, key_of("kaspa"));
    assert!(why.contains("must not carry a name"), "{why}");
}

#[test]
fn kind_and_fields_must_agree() {
    let k = key_of("kaspa");
    assert!(why(&ExportRow { claim: Some("44".repeat(32)), ..row("kaspa") }, k).contains("must not carry a claim"));
    for missing in [
        ExportRow { name: None, ..row("kaspa") },
        ExportRow { owner: None, ..row("kaspa") },
        ExportRow { owner_type: None, ..row("kaspa") },
    ] {
        assert!(why(&missing, k).contains("must carry"), "an incomplete active row must be refused");
    }
    // Without a claim a PENDING row has no deed address, and without `accepted_daa` it can
    // never be evicted.
    assert!(why(&ExportRow { claim: None, ..pending_row() }, k).contains("must carry a claim"));
    assert!(why(&ExportRow { accepted_daa: None, ..pending_row() }, k).contains("must carry an accepted DAA score"));
    assert!(why(&ExportRow { owner: Some("55".repeat(32)), ..pending_row() }, k).contains("must not carry an owner"));
    DeedRow::try_from((&k, &unknown_row())).expect("a bare owner-unknown row imports");
    for lying in [
        ExportRow { owner: Some("55".repeat(32)), ..unknown_row() },
        ExportRow { owner_type: Some(0), ..unknown_row() },
        ExportRow { claim: Some("66".repeat(32)), ..unknown_row() },
    ] {
        assert!(why(&lying, k).contains("must not carry"), "an owner-unknown row claims nothing");
    }
    assert!(why(&ExportRow { kind: 9, ..row("kaspa") }, k).contains("unknown row kind"));
}

/// An unknown owner type is a row whose state never derives.
#[test]
fn owner_type_is_validated_at_the_boundary() {
    let k = key_of("kaspa");
    for ot in [0x00u8, 0x03, 0x04, 0x85, 0x86] {
        DeedRow::try_from((&k, &ExportRow { owner_type: Some(ot), ..row("kaspa") })).expect("a supported scheme imports");
    }
    // 0x01 and 0x02 are the P2PKH schemes, which never round-trip. This registry
    // carries 0x05 and 0x06 with bit 7 set.
    for ot in [0x01u8, 0x02, 0x05, 0x06, 0x80, 0x81, 0x84, 0x87, 0xff] {
        assert!(why(&ExportRow { owner_type: Some(ot), ..row("kaspa") }, k).contains("unknown owner type"));
    }
}

#[test]
fn values_that_postgres_would_store_negative_are_refused() {
    let k = key_of("kaspa");
    let over = i64::MAX as u64 + 1;
    assert!(why(&ExportRow { accepted_daa: Some(over), ..pending_row() }, k).contains("above i64::MAX"));
    assert!(why(&ExportRow { value: Some(over), ..pending_row() }, k).contains("above i64::MAX"));
    assert!(bigint_u64(over, "blueScore").is_err());
    DeedRow::try_from((&k, &ExportRow { accepted_daa: Some(i64::MAX as u64), ..pending_row() })).expect("i64::MAX is representable");
    assert_eq!(bigint_u64(i64::MAX as u64, "blueScore").unwrap(), i64::MAX as u64);
}

/// `faster_hex` only requires the source to be at least twice the destination, so an
/// over-long field truncates silently and two rows collide on one primary key.
#[test]
fn over_long_hex_is_not_silently_truncated() {
    let k = key_of("kaspa");
    let long = "11".repeat(64);
    assert!(why(&ExportRow { owner: Some(long.clone()), ..row("kaspa") }, k).contains("64 hex characters"));
    assert!(why(&ExportRow { claim: Some(long.clone()), ..pending_row() }, k).contains("64 hex characters"));
    assert!(why(&ExportRow { outpoint_txid: Some(long.clone()), ..pending_row() }, k).contains("64 hex characters"));
    assert!(unhex32(&long).is_err());
    assert!(unhex32(&"11".repeat(31)).is_err());
    assert!(unhex32(&"11".repeat(32)).is_ok());
}

#[test]
fn gap_images_must_describe_a_real_interval() {
    let g = |lo: &str, hi: Option<&str>| ExportGap {
        lo: lo.repeat(32),
        hi: hi.map(|h| h.repeat(32)),
        outpoint_txid: None,
        outpoint_index: None,
    };
    GapImage::try_from(&g("11", Some("22"))).expect("lo below hi is a real interval");
    GapImage::try_from(&g("11", None)).expect("no row at that lo is a real pre-image: undo turns it into a delete");
    for bad in [g("22", Some("11")), g("11", Some("11"))] {
        let why = GapImage::try_from(&bad).expect_err("an inverted or empty interval must not import").to_string();
        assert!(why.contains("covers no keyspace"), "{why}");
    }
    let half = ExportGap { outpoint_index: Some(0), ..g("11", Some("22")) };
    assert!(GapImage::try_from(&half).expect_err("half an outpoint").to_string().contains("half an outpoint"));
}
