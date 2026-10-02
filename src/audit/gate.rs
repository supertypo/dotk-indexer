//! What the last verdict withholds from lookups, so nothing unproven is served.

use anyhow::Result;
use sqlx::PgConnection;

use crate::app::Verdict;
use crate::db;
use crate::model::DeedRow;

/// Built once per verdict from the keys the pass could not prove. Every list is sorted, because
/// a lookup searches them.
#[derive(Debug, Default)]
pub struct Withheld {
    all: bool,
    /// `[lo, hi]` of every gap that failed or is in flux.
    blind_spots: Vec<[[u8; 32]; 2]>,
    keys: Vec<[u8; 32]>,
    provenance: Vec<[u8; 32]>,
}

impl Withheld {
    /// Before the first verdict, everything.
    pub fn all() -> Self {
        Withheld { all: true, ..Default::default() }
    }
    pub fn new(mut blind_spots: Vec<[[u8; 32]; 2]>, mut keys: Vec<[u8; 32]>, mut provenance: Vec<[u8; 32]>) -> Self {
        blind_spots.sort_unstable();
        keys.sort_unstable();
        provenance.sort_unstable();
        Withheld { all: false, blind_spots, keys, provenance }
    }

    /// A key inside a blind spot, one whose deed failed, or one whose two flanking gaps both
    /// failed, so nothing proves it either way.
    pub fn withholds_key(&self, key: &[u8; 32]) -> bool {
        if self.all || self.keys.binary_search(key).is_ok() {
            return true;
        }
        let i = self.blind_spots.partition_point(|[lo, _]| lo <= key);
        let Some([lo, hi]) = i.checked_sub(1).map(|i| &self.blind_spots[i]) else { return false };
        (lo < key && key < hi) || (lo == key && i > 1 && self.blind_spots[i - 2][1] == *key)
    }
    pub fn withholds_provenance(&self, key: &[u8; 32]) -> bool {
        self.all || self.provenance.binary_search(key).is_ok()
    }
}

#[derive(Debug)]
pub enum NameLookup {
    Active([u8; 32], DeedRow),
    Held,
    Unproven,
    Free,
}

pub async fn look_up_name(verdict: &Verdict, conn: &mut PgConnection, name: &str) -> Result<NameLookup> {
    let key = dotk_core::key_of(name);
    if verdict.withheld().await.withholds_key(&key) {
        return Ok(NameLookup::Unproven);
    }
    if let Some((key, row)) = db::deed_by_name(conn, name).await? {
        return Ok(NameLookup::Active(key, row));
    }
    Ok(if db::get_deed(conn, &key).await?.is_some() { NameLookup::Held } else { NameLookup::Free })
}
