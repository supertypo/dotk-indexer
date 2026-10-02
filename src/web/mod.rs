//! The HTTP API, the status page and the gateway.

mod dto;
mod error;
mod gateway;
mod handlers;
mod middleware;
mod pages;
mod params;
mod server;

pub use handlers::GENESIS_CACHE_TTL;
pub use server::{bind, openapi_doc, router, spawn};
