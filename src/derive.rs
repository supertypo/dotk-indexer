//! Derives the gap states from the deed key set.

use std::collections::HashSet;

use dotk_core::registry::{self, KEY_MAX, KEY_MIN};
use dotk_core::state::{DeedState, GapState};

use crate::model::DeedRow;

/// `n` sorted deed keys give `n + 1` gaps from `KEY_MIN` to `KEY_MAX`.
pub fn derive_gaps(rows: &[([u8; 32], DeedRow)]) -> Vec<GapState> {
    let entries: Vec<registry::Entry> = rows.iter().map(|(k, r)| r.entry(*k)).collect();
    registry::derive_gaps(&entries)
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct Neighborhood {
    /// `None` for a free key and for an owner-unknown row, which has no state to hash.
    pub deed: Option<DeedState>,
    /// Free keys only: the gap that strictly contains the key, which a split spends.
    pub covering: Option<GapState>,
    /// Occupied keys only: the two flanking gaps, which an exit merge spends at seats 0 and 2.
    pub neighbors: Option<(GapState, GapState)>,
}

/// Gap bounds are exactly `KEY_MIN`, the deed keys and `KEY_MAX`, so the row at `key` and its
/// two nearest neighbors give the same answer as [`derive_gaps`] over the whole table.
pub(crate) fn neighborhood(
    key: &[u8; 32],
    at: Option<&DeedRow>,
    pred: Option<&([u8; 32], DeedRow)>,
    succ: Option<&([u8; 32], DeedRow)>,
) -> Neighborhood {
    let (below, above) = (pred.map(|(k, _)| *k), succ.map(|(k, _)| *k));
    if let Some(row) = at {
        Neighborhood { deed: row.deed_state(*key), covering: None, neighbors: Some(flanks(key, below, above)) }
    } else {
        let gap = span(below, above);
        Neighborhood { deed: None, covering: (gap.lo < *key && *key < gap.hi).then_some(gap), neighbors: None }
    }
}

/// Shared by the point query, the evictor and the repair, so they agree on a deed's neighbors. A missing
/// `below` or `above` stands for the keyspace bound.
pub(crate) fn flanks(key: &[u8; 32], below: Option<[u8; 32]>, above: Option<[u8; 32]>) -> (GapState, GapState) {
    (span(below, Some(*key)), span(Some(*key), above))
}
pub(crate) fn span(below: Option<[u8; 32]>, above: Option<[u8; 32]>) -> GapState {
    GapState { lo: below.unwrap_or(KEY_MIN), hi: above.unwrap_or(KEY_MAX) }
}

/// The nearest keys below and above `key` in the sorted `keys`, skipping `excluded`.
pub(crate) fn around(keys: &[[u8; 32]], key: &[u8; 32], excluded: &HashSet<[u8; 32]>) -> (Option<[u8; 32]>, Option<[u8; 32]>) {
    let below = keys.iter().rev().find(|k| *k < key && !excluded.contains(*k)).copied();
    let above = keys.iter().find(|k| *k > key && !excluded.contains(*k)).copied();
    (below, above)
}

#[cfg(test)]
#[path = "derive_tests.rs"]
mod tests;
