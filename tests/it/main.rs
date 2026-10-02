//! The integration suite, in one binary, so one Postgres container serves every test.
#![allow(clippy::many_single_char_names, reason = "h, w, s and a name the harness, the world, a split and an activate")]

mod common;

mod bootstrap_url;
mod cards;
mod double_spend;
mod evictor_audit;
mod gateway;
mod identity;
mod lineage_gate;
mod pool_isolation;
mod repair_guards;
mod replay;
mod scenarios;
mod snapshot_import;
mod supervision;
mod undo_prop;
mod web_api;
