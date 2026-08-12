//! Typed author input for an SSM parameter.

use serde::Deserialize;
use tokeira_platform::{
    error::KindError,
    kind::{Kind, PlacementContext},
};

use crate::resources::ssm_parameter::SsmParameterResource as Resource;

/// Author-visible name of the realized resource type.
pub const TYPE: &str = "SsmParameter";

/// Reusable author input for one SSM parameter.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SsmParameter {
    /// Full parameter name (resource id `ssm-parameter:<name>`).
    pub name: String,
    /// Parameter value.
    pub value: String,
    /// Store as a SecureString.
    #[serde(default)]
    pub secure: bool,
}

impl Kind<Resource> for SsmParameter {
    fn realize(&self, placement: &PlacementContext) -> Result<Resource, KindError> {
        Ok(Resource {
            name: self.name.clone(),
            value: self.value.clone(),
            secure: self.secure,
            module: placement.module.clone(),
        })
    }
}
