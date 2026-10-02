//! The self-test proves the registry against the UTXO set and repairs a confirmed deviation.

mod gate;
mod probe;
mod repair;
mod selftest;

pub use gate::{NameLookup, Withheld, look_up_name};
pub(crate) use probe::probe_addresses;
pub use selftest::{Outcome, run, run_once, run_pass};
