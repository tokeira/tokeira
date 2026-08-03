//! Serde-backed admission of platform-owned configuration structs.

use serde::de::DeserializeOwned;

use crate::{
    author::{LocatedValue, from_located_value},
    error::ConfigError,
};

/// Decode a frontend value and run the platform's pure validation function.
pub fn admit_config<C>(
    value: LocatedValue,
    validate: fn(&C) -> Result<(), ConfigError>,
) -> Result<C, ConfigError>
where
    C: DeserializeOwned,
{
    let root_range = value.range;
    let config = from_located_value(value).map_err(|error| ConfigError {
        message: error.message().to_string(),
        range: error.range().or(root_range),
    })?;
    validate(&config).map_err(|error| error.at(root_range))?;
    Ok(config)
}
