//! Conversions of integers and 32-byte hashes to their Postgres and hex forms.

use std::fmt::{self, Display};
use std::time::Duration;

use anyhow::Result;

#[derive(Debug)]
pub struct OutOfRange {
    value: String,
    target: &'static str,
}

impl Display for OutOfRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} does not fit in {}", self.value, self.target)
    }
}

impl std::error::Error for OutOfRange {}

pub(crate) fn fit<T, U>(v: T) -> Result<U>
where
    T: Copy + Display,
    U: TryFrom<T>,
{
    U::try_from(v).map_err(|_| OutOfRange { value: v.to_string(), target: std::any::type_name::<U>() }.into())
}

pub(crate) fn sql_i64(v: u64) -> Result<i64> {
    fit(v)
}

pub(crate) fn sql_i32(v: u32) -> Result<i32> {
    fit(v)
}

pub(crate) fn sql_u64(v: i64) -> Result<u64> {
    fit(v)
}

pub(crate) fn sql_u32(v: i32) -> Result<u32> {
    fit(v)
}

pub(crate) fn sql_u8(v: i16) -> Result<u8> {
    fit(v)
}

/// Saturates, because no duration this crate measures comes near `u64::MAX` ms.
pub(crate) fn millis(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

pub fn hex32(b: &[u8; 32]) -> String {
    faster_hex::hex_string(b)
}

pub fn unhex32(s: &str) -> Result<[u8; 32]> {
    anyhow::ensure!(s.len() == 64, "expected exactly 64 hex characters, got {}", s.len());
    Ok(faster_hex::hex_decode_array(s.as_bytes())?)
}
