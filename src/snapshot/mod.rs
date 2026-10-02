//! A `/snapshot` body, also `snapshot.json` on disk, holds a canonical core of `registryCovenantId` and the
//! key-sorted `deeds`, a resume section of `vcpCheckpoint` and the journal tail, and the unswept cards. It holds
//! no gaps, because the importer's probe derives them.

mod bootstrap;
mod export;
mod format;
mod import;

pub use bootstrap::fetch;
pub(crate) use bootstrap::{Bootstrap, decide};
pub use export::{EXPORT_EVENT_CAP, export, export_event_window};
pub(crate) use export::{NoCheckpoint, bodies, export_rows};
pub use format::{ExportDeed, ExportEvent, ExportRow, Snapshot};
pub use import::{import, verify_checkpoint};
