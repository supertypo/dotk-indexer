use super::{CATCHUP_THRESHOLD, poll_deadline};
use crate::chain::{CATCHUP_DEADLINE, TIP_DEADLINE};

#[test]
fn a_poll_at_the_tip_gets_the_short_deadline() {
    assert_eq!(poll_deadline(None, false), CATCHUP_DEADLINE, "the first poll's size is unknown");
    assert_eq!(poll_deadline(Some(0), false), TIP_DEADLINE, "an empty answer is the tip");
    assert_eq!(poll_deadline(Some(CATCHUP_THRESHOLD - 1), false), TIP_DEADLINE);
    assert_eq!(poll_deadline(Some(CATCHUP_THRESHOLD), false), CATCHUP_DEADLINE, "a catch-up batch");
    assert_eq!(poll_deadline(Some(0), true), CATCHUP_DEADLINE, "a failed poll leaves its answer standing");
}
