//! Follows the virtual chain of a Kaspa node and applies its registry events.

mod block;
mod events;
mod kaspad;
mod node;
mod vcp;

pub use block::{AcceptedTx, ChainBlock, VccResponse};
pub use events::{ApplyOutcome, apply_block, undo_block};
pub use kaspad::PooledKaspad;
pub use node::{CATCHUP_DEADLINE, DagInfo, Kaspad, Node, TIP_DEADLINE, Unresumable, UtxoHit};
pub use vcp::{process_response, run};
