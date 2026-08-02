//! Typed platform configuration admission and pure validation contracts.

use std::{fmt::Debug, marker::PhantomData};

use serde::{Serialize, de::DeserializeOwned};

use crate::{
    author::{AuthorNode, from_author_node},
    error::ConfigError,
};

/// Configuration supplied by one platform and admitted from any definition frontend.
pub trait PlatformConfig:
    Clone + Debug + Serialize + DeserializeOwned + Send + Sync + 'static
{
    /// Enforce cross-field invariants without provider access or ambient discovery.
    fn validate(&self) -> Result<(), ConfigError>;
}

/// Compile-time admission contract for one platform configuration type.
#[derive(Debug, Clone, Copy, Default)]
pub struct ConfigContract<C: PlatformConfig> {
    marker: PhantomData<fn() -> C>,
}

impl<C: PlatformConfig> ConfigContract<C> {
    /// Construct the standard Serde-backed contract.
    pub const fn new() -> Self {
        Self {
            marker: PhantomData,
        }
    }

    /// Decode host-free data and run platform-owned pure validation.
    pub fn admit(&self, node: AuthorNode) -> Result<C, ConfigError> {
        let root_range = node.range;
        let config: C = from_author_node(node).map_err(|error| ConfigError {
            message: error.message().to_string(),
            range: error.range().or(root_range),
        })?;
        config.validate().map_err(|error| error.at(root_range))?;
        Ok(config)
    }
}
