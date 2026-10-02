use dotk_core::cards::CardState;
use dotk_core::names;
use dotk_core::registry::Entry;
use dotk_core::state::{DeedState, GapState, OwnerType, Status};
use serde::Serialize;
use utoipa::ToSchema;

/// A `deeds` row without its key, so it also serves as the journal's `prev` image.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DeedRow {
    pub kind: RowKind,
    pub name: Option<String>,
    pub owner_type: Option<u8>,
    pub owner: Option<[u8; 32]>,
    pub claim: Option<[u8; 32]>,
    /// Required for PENDING. Kept for ACTIVE, so the self-test proves every exported byte.
    pub outpoint_txid: Option<[u8; 32]>,
    pub outpoint_index: Option<u32>,
    pub value: Option<u64>,
    /// DAA score of the split, carried forward by later events. The evictor's maturity clock
    /// on PENDING, and only the registration time on ACTIVE. `None` when the split was never
    /// seen or the row was demoted.
    pub accepted_daa: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[repr(i16)]
pub enum RowKind {
    Active = 0,
    Pending = 1,
    /// A deed the partition proves exists, with a refuted or unobserved owner. It maps no name
    /// to an address, and upgrades in place at the key's next event.
    OwnerUnknown = 2,
}

impl TryFrom<i16> for RowKind {
    type Error = i16;

    fn try_from(v: i16) -> Result<Self, i16> {
        match v {
            0 => Ok(Self::Active),
            1 => Ok(Self::Pending),
            2 => Ok(Self::OwnerUnknown),
            _ => Err(v),
        }
    }
}

impl RowKind {
    pub fn label(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Pending => "pending",
            Self::OwnerUnknown => "ownerUnknown",
        }
    }
}

impl DeedRow {
    pub fn active(name: String, owner_type: u8, owner: [u8; 32], outpoint: ([u8; 32], u32), value: u64) -> Self {
        Self {
            kind: RowKind::Active,
            name: Some(name),
            owner_type: Some(owner_type),
            owner: Some(owner),
            claim: None,
            outpoint_txid: Some(outpoint.0),
            outpoint_index: Some(outpoint.1),
            value: Some(value),
            accepted_daa: None,
        }
    }

    pub fn pending(claim: [u8; 32], outpoint: ([u8; 32], u32), value: u64, accepted_daa: u64) -> Self {
        Self {
            kind: RowKind::Pending,
            name: None,
            owner_type: None,
            owner: None,
            claim: Some(claim),
            outpoint_txid: Some(outpoint.0),
            outpoint_index: Some(outpoint.1),
            value: Some(value),
            accepted_daa: Some(accepted_daa),
        }
    }

    /// The claim must be cleared, because a row with one derives as PENDING at a dead address.
    pub fn owner_unknown(name: Option<String>) -> Self {
        Self {
            kind: RowKind::OwnerUnknown,
            name,
            owner_type: None,
            owner: None,
            claim: None,
            outpoint_txid: None,
            outpoint_index: None,
            value: None,
            accepted_daa: None,
        }
    }

    pub fn entry(&self, key: [u8; 32]) -> Entry {
        match self.claim {
            Some(claim) => Entry { status: Status::Pending, key, claim },
            None => Entry::active(key),
        }
    }

    /// `None` for owner-unknown, and for a corrupt ACTIVE row, which `audit::probe_all` routes
    /// to repair. `try_padded_name` keeps a bad name from panicking the audit task.
    pub fn deed_state(&self, key: [u8; 32]) -> Option<DeedState> {
        match self.kind {
            RowKind::Pending => Some(DeedState::pending(key, self.claim?)),
            RowKind::Active => Some(DeedState {
                status: Status::Active,
                key,
                owner_type: OwnerType::from_byte(self.owner_type?)?,
                owner: self.owner?,
                name: names::try_padded_name(self.name.as_deref()?).ok()?,
            }),
            RowKind::OwnerUnknown => None,
        }
    }
}

/// `Discover` is a key entering the table as owner-unknown. `Sweep` removes a name's records
/// and leaves the deed alone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i16)]
pub enum HistoryOp {
    Register = 0,
    Activate = 1,
    Transfer = 2,
    Release = 3,
    Evict = 4,
    Discover = 5,
    Sweep = 6,
}

impl TryFrom<i16> for HistoryOp {
    type Error = i16;

    fn try_from(v: i16) -> Result<Self, i16> {
        match v {
            0 => Ok(Self::Register),
            1 => Ok(Self::Activate),
            2 => Ok(Self::Transfer),
            3 => Ok(Self::Release),
            4 => Ok(Self::Evict),
            5 => Ok(Self::Discover),
            6 => Ok(Self::Sweep),
            _ => Err(v),
        }
    }
}

/// What one event did to a name's records. The handler records it, because a later purge of
/// swept cards leaves the `cards` table unable to answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i16)]
pub enum CardChange {
    None = 0,
    Set = 1,
    Updated = 2,
    Deleted = 3,
}

impl TryFrom<i16> for CardChange {
    type Error = i16;

    fn try_from(v: i16) -> Result<Self, i16> {
        match v {
            0 => Ok(Self::None),
            1 => Ok(Self::Set),
            2 => Ok(Self::Updated),
            3 => Ok(Self::Deleted),
            _ => Err(v),
        }
    }
}

impl CardChange {
    pub fn of(before: bool, after: bool) -> Self {
        match (before, after) {
            (false, false) => Self::None,
            (false, true) => Self::Set,
            (true, true) => Self::Updated,
            (true, false) => Self::Deleted,
        }
    }
}

/// `state` is the deed after the event, `None` after a release or an evict.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoryRow {
    pub key: [u8; 32],
    pub op: HistoryOp,
    pub seq: i32,
    pub blue_score: u64,
    pub daa_score: u64,
    pub block_time: u64,
    pub block_hash: [u8; 32],
    pub txid: [u8; 32],
    pub state: Option<DeedRow>,
    pub card: CardChange,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct GapRow {
    pub hi: [u8; 32],
    pub outpoint_txid: Option<[u8; 32]>,
    pub outpoint_index: Option<u32>,
}

impl GapRow {
    pub fn observed(hi: [u8; 32], outpoint: ([u8; 32], u32)) -> Self {
        Self { hi, outpoint_txid: Some(outpoint.0), outpoint_index: Some(outpoint.1) }
    }

    pub fn state(&self, lo: [u8; 32]) -> GapState {
        GapState { lo, hi: self.hi }
    }
}

/// Liveness is a join, never a column. A card is live while `txid` is the deed's
/// `outpoint_txid`, `idx` is 1 and `swept_at` is unset.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CardRow {
    pub txid: [u8; 32],
    pub idx: u32,
    pub state: CardState,
    pub blob: Vec<u8>,
    pub value: u64,
    /// Blue score at which the UTXO was seen spent. A later probe that finds the UTXO clears it.
    pub swept_at: Option<u64>,
}

/// A card's journal image. Undo deletes a row with `existed: false`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CardMark {
    pub txid: [u8; 32],
    pub idx: u32,
    pub existed: bool,
    pub swept_at: Option<u64>,
}

/// The owner scheme byte in decimal: 0 p2pk-schnorr, 3 p2sh, 4 covenant, 133/134 p2pk-ecdsa.
#[derive(ToSchema)]
#[repr(u8)]
#[schema(as = OwnerType)]
pub(crate) enum OwnerTypeParam {
    P2pkSchnorr = OwnerType::Pubkey as u8,
    P2sh = OwnerType::ScriptHash as u8,
    Covenant = OwnerType::CovenantId as u8,
    P2pkEcdsaOdd = OwnerType::P2pkEcdsaOdd as u8,
    P2pkEcdsaEven = OwnerType::P2pkEcdsaEven as u8,
}

/// A key-owned scheme byte in decimal: 0 p2pk-schnorr, 133/134 p2pk-ecdsa.
#[derive(ToSchema)]
#[repr(u8)]
#[schema(as = SpenderType)]
pub(crate) enum SpenderTypeParam {
    Schnorr = OwnerType::Pubkey as u8,
    EcdsaOdd = OwnerType::P2pkEcdsaOdd as u8,
    EcdsaEven = OwnerType::P2pkEcdsaEven as u8,
}
