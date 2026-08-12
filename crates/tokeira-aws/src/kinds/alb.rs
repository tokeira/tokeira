//! Typed author inputs for the ALB family: load balancer, target group,
//! listener.

use serde::Deserialize;
use tokeira_platform::{
    error::KindError,
    kind::{Kind, PlacementContext},
};

use crate::resources::elbv2::{
    AlbListenerMode, AlbListenerResource, AlbResource, AlbTargetGroupResource,
};

/// Author-visible name of the realized load-balancer resource type.
pub const ALB_TYPE: &str = "Alb";
/// Author-visible name of the realized target-group resource type.
pub const TARGET_GROUP_TYPE: &str = "AlbTargetGroup";
/// Author-visible name of the realized listener resource type.
pub const LISTENER_TYPE: &str = "AlbListener";

/// Reusable author input for the internal ALB. Declares its VPC and
/// security group as dependencies.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Alb {
    /// Load-balancer name (resource id `alb-<name>`).
    pub name: String,
}

impl Kind<AlbResource> for Alb {
    fn realize(&self, placement: &PlacementContext) -> Result<AlbResource, KindError> {
        let vpc = super::required_dependency(placement, "Vpc", |id| id.ends_with("-vpc"))?;
        let security_group =
            super::required_dependency(placement, "SecurityGroup", |id| id.starts_with("sg-"))?;
        Ok(AlbResource::new(
            self.name.clone(),
            placement.module.clone(),
            vpc,
            security_group,
        ))
    }
}

/// Reusable author input for an ALB target group.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AlbTargetGroup {
    /// Target-group name (resource id `alb-tg-<name>`).
    pub name: String,
    /// Traffic port.
    pub port: u16,
    /// Health-check request path.
    pub health_check_path: String,
    /// Health-check interval in seconds.
    pub health_check_interval_secs: u64,
}

impl Kind<AlbTargetGroupResource> for AlbTargetGroup {
    fn realize(&self, placement: &PlacementContext) -> Result<AlbTargetGroupResource, KindError> {
        let vpc = super::required_dependency(placement, "Vpc", |id| id.ends_with("-vpc"))?;
        Ok(AlbTargetGroupResource::new(
            self.name.clone(),
            self.port,
            self.health_check_path.clone(),
            self.health_check_interval_secs,
            placement.module.clone(),
            vpc,
        ))
    }
}

/// Authored listener protocol, mirroring the resource's mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ListenerProtocol {
    /// Plain HTTP/2.
    Http2,
    /// HTTPS; requires a certificate ARN.
    Https,
}

/// Reusable author input for the ALB listener with host-header routing to
/// the two edge target groups. The target groups are named explicitly and
/// matched against the declared dependencies, so the twin never guesses
/// which dependency backs which route.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AlbListener {
    /// Listener name (resource id `alb-listener-<name>`).
    pub name: String,
    /// Listener protocol.
    pub protocol: ListenerProtocol,
    /// Certificate ARN, required for HTTPS.
    #[serde(default)]
    pub certificate_arn: Option<String>,
    /// Private DNS zone for host-header rules.
    pub private_dns_zone: String,
    /// Name of the edge-api target group (a declared dependency).
    pub edge_api_target_group: String,
    /// Name of the edge-poll target group (a declared dependency).
    pub edge_poll_target_group: String,
}

impl Kind<AlbListenerResource> for AlbListener {
    fn realize(&self, placement: &PlacementContext) -> Result<AlbListenerResource, KindError> {
        let alb = super::required_dependency(placement, "Alb", |id| {
            id.starts_with("alb-") && !id.starts_with("alb-tg-") && !id.starts_with("alb-listener-")
        })?;
        let edge_api_id = format!("alb-tg-{}", self.edge_api_target_group);
        let edge_api = super::required_dependency(placement, "edge-api AlbTargetGroup", |id| {
            id == edge_api_id
        })?;
        let edge_poll_id = format!("alb-tg-{}", self.edge_poll_target_group);
        let edge_poll = super::required_dependency(placement, "edge-poll AlbTargetGroup", |id| {
            id == edge_poll_id
        })?;
        Ok(AlbListenerResource::new(
            self.name.clone(),
            match self.protocol {
                ListenerProtocol::Http2 => AlbListenerMode::Http2,
                ListenerProtocol::Https => AlbListenerMode::Https,
            },
            self.certificate_arn.clone(),
            self.private_dns_zone.clone(),
            placement.module.clone(),
            alb,
            edge_api,
            edge_poll,
        ))
    }
}
