use super::*;

#[test]
fn only_a_pending_row_is_evictable() {
    let key = dotk_core::names::key_of("alice");
    let flanks = (GapState { lo: dotk_core::registry::KEY_MIN, hi: key }, GapState { lo: key, hi: dotk_core::registry::KEY_MAX });
    let n = |deed| derive::Neighborhood { deed, covering: None, neighbors: Some(flanks) };

    let pending = DeedState::pending(key, [7u8; 32]);
    assert_eq!(evictable(&n(Some(pending))), Some((pending, flanks.0, flanks.1)), "the ripe squat, with both flanks");

    let active = DeedState {
        status: Status::Active,
        key,
        owner_type: dotk_core::state::OwnerType::Pubkey,
        owner: [9u8; 32],
        name: dotk_core::names::padded_name("alice"),
    };
    assert_eq!(evictable(&n(Some(active))), None, "the owner activated between the scan and the lookup: a silent no-op");
    assert_eq!(evictable(&n(None)), None, "OWNER-UNKNOWN, or the row is gone");
    assert_eq!(evictable(&derive::Neighborhood { deed: Some(pending), covering: None, neighbors: None }), None);
}
