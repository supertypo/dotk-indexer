use std::sync::Arc;

use sqlx::PgPool;

use crate::chain::Kaspad;

pub struct Backends {
    pub db: PgPool,
    /// Separate from `db`, so a flood of requests cannot hold a batch back from committing.
    pub web_db: PgPool,
    pub kaspad: Arc<dyn Kaspad>,
}
