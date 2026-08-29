//! Typed author input for a security group and its ingress rules.

use serde::Deserialize;
use tokeira_platform::{
    error::KindError,
    kind::{Kind, PlacementContext},
};

use crate::resources::security_group::{
    SecurityGroup as Resource, SecurityGroupConfig, SecurityRule,
};

/// Author-visible name of the realized resource type.
pub(crate) const TYPE: &str = "SecurityGroup";

/// One authored ingress rule. Mirrors the resource's rule shape so the
/// authored input stays a plain serializable value.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IngressRule {
    /// Human-readable rule purpose.
    pub(crate) description: String,
    /// IP protocol (`tcp`, `udp`, `-1`).
    pub(crate) protocol: String,
    /// Inclusive start of the port range.
    pub(crate) from_port: u16,
    /// Inclusive end of the port range.
    pub(crate) to_port: u16,
    /// CIDR or security-group source.
    pub(crate) source: String,
}

/// Reusable author input for a VPC security group. The owning VPC is the
/// declared dependency; its realized id is taken from placement.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SecurityGroup {
    /// AWS region.
    pub(crate) region: String,
    /// Security-group name (also determines the resource id `sg-<name>`).
    pub(crate) name: String,
    /// Group description.
    pub(crate) description: String,
    /// Authored ingress rules.
    #[serde(default)]
    pub(crate) ingress: Vec<IngressRule>,
}

impl Kind<Resource> for SecurityGroup {
    fn realize(&self, placement: &PlacementContext) -> Result<Resource, KindError> {
        let vpc = super::required_dependency(placement, "Vpc", |id| id.ends_with("-vpc"))?;
        let rctx = super::resource_context(&self.region, placement);
        Ok(Resource::new(
            self.name.clone(),
            SecurityGroupConfig {
                vpc_dependency: vpc,
                description: self.description.clone(),
                ingress_rules: self
                    .ingress
                    .iter()
                    .map(|rule| SecurityRule {
                        description: rule.description.clone(),
                        protocol: rule.protocol.clone(),
                        from_port: rule.from_port,
                        to_port: rule.to_port,
                        source: rule.source.clone(),
                    })
                    .collect(),
                module: placement.module.clone(),
            },
            &rctx,
        ))
    }
}
