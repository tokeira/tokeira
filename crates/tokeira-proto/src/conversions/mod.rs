//! Small conversion helpers between generated protobuf types and `tokeira-types`.
//!
//! These helpers are intentionally **small and explicit**. They are not trying to become a giant
//! magical translation layer. Their job is to make the important wire/domain boundaries obvious.

pub mod common;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum ProtoConversionError {
    #[error("invalid UUID in wire value: {0}")]
    InvalidUuid(#[from] uuid::Error),

    #[error("invalid task token: {0}")]
    InvalidTaskToken(String),

    #[error("invalid timestamp nanos: {0}")]
    InvalidTimestamp(String),

    #[error("missing required field: {0}")]
    MissingField(&'static str),
}
