//! Checked numeric conversions for the Rust ↔ DSQL type boundary.
//!
//! DSQL stores integers as signed types (SMALLINT = i16, INTEGER = i32,
//! BIGINT = i64) while Tokeira's domain types use unsigned Rust integers.
//! These helpers reject out-of-range values with descriptive errors instead
//! of silently wrapping via `as` casts.

use anyhow::Result;

pub(crate) fn i16_from_u16(value: u16, field: &str) -> Result<i16> {
    i16::try_from(value).map_err(|_| anyhow::anyhow!("{field} {value} exceeds i16 range"))
}

pub(crate) fn i32_from_u32(value: u32, field: &str) -> Result<i32> {
    i32::try_from(value).map_err(|_| anyhow::anyhow!("{field} {value} exceeds i32 range"))
}

pub(crate) fn i64_from_u64(value: u64, field: &str) -> Result<i64> {
    i64::try_from(value).map_err(|_| anyhow::anyhow!("{field} {value} exceeds i64 range"))
}

pub(crate) fn i64_from_usize(value: usize, field: &str) -> Result<i64> {
    i64::try_from(value).map_err(|_| anyhow::anyhow!("{field} {value} exceeds i64 range"))
}

pub(crate) fn u64_from_i64(value: i64, field: &str) -> Result<u64> {
    u64::try_from(value).map_err(|_| anyhow::anyhow!("{field} {value} is negative"))
}

pub(crate) fn u32_from_i32(value: i32, field: &str) -> Result<u32> {
    u32::try_from(value).map_err(|_| anyhow::anyhow!("{field} {value} is negative"))
}
