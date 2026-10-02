use anyhow::Result;
use dotk_core::state::{DeedState, GapState};
use dotk_core::watch::{GenesisFile, WatchTemplates};
use kaspa_addresses::{Address, Prefix};

use super::Tagged;
use crate::config::CliArgs;

pub struct Deployment {
    pub args: CliArgs,
    pub genesis: GenesisFile,
    /// The manifest bytes `/genesis` serves. Never re-serialized from `genesis`, so fields
    /// this build does not know still reach the client.
    pub genesis_raw: Tagged,
    pub watch: WatchTemplates,
    pub prefix: Prefix,
    pub net_bps: u64,
}

impl Deployment {
    pub fn gap_address(&self, state: &GapState) -> Result<Address> {
        dotk_core::address::gap_address(&self.watch.gap, self.prefix, state)
    }

    pub fn deed_address(&self, state: &DeedState) -> Result<Address> {
        dotk_core::address::deed_address(&self.watch.deed, self.prefix, state)
    }
}
