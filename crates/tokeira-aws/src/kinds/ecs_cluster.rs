//! Typed author inputs for the ECS cluster capacity family: cluster,
//! launch template, auto-scaling group, capacity provider.

use serde::Deserialize;
use tokeira_platform::{
    error::KindError,
    kind::{Kind, PlacementContext},
};

use crate::resources::ecs_cluster::{
    AsgResource, CapacityProviderResource, EcsClusterResource, LaunchTemplateResource,
};

/// Author-visible name of the realized cluster resource type.
pub(crate) const CLUSTER_TYPE: &str = "EcsCluster";
/// Author-visible name of the realized launch-template resource type.
pub(crate) const LAUNCH_TEMPLATE_TYPE: &str = "LaunchTemplate";
/// Author-visible name of the realized auto-scaling-group resource type.
pub(crate) const ASG_TYPE: &str = "AutoScalingGroup";
/// Author-visible name of the realized capacity-provider resource type.
pub(crate) const CAPACITY_PROVIDER_TYPE: &str = "EcsCapacityProvider";

/// Reusable author input for the ECS cluster.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EcsCluster {
    /// Cluster name.
    pub(crate) name: String,
    /// Service Connect namespace the cluster advertises.
    pub(crate) service_connect_namespace: String,
}

impl Kind<EcsClusterResource> for EcsCluster {
    fn realize(&self, placement: &PlacementContext) -> Result<EcsClusterResource, KindError> {
        Ok(EcsClusterResource::new(
            self.name.clone(),
            self.service_connect_namespace.clone(),
            placement.module.clone(),
        ))
    }
}

/// Reusable author input for a capacity plane's launch template. Declares
/// its security group as a dependency.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LaunchTemplate {
    /// Launch-template name (resource id `lt-<name>`).
    pub(crate) name: String,
    /// Cluster the instances join.
    pub(crate) cluster_name: String,
    /// EC2 instance type for this capacity plane.
    pub(crate) instance_type: String,
    /// Workload attribute stamped into instance ECS attributes.
    pub(crate) workload: String,
    /// IAM instance-profile name the instances assume.
    pub(crate) instance_profile_name: String,
}

impl Kind<LaunchTemplateResource> for LaunchTemplate {
    fn realize(&self, placement: &PlacementContext) -> Result<LaunchTemplateResource, KindError> {
        let security_group =
            super::required_dependency(placement, "SecurityGroup", |id| id.starts_with("sg-"))?;
        Ok(LaunchTemplateResource {
            name: self.name.clone(),
            cluster_name: self.cluster_name.clone(),
            instance_type: self.instance_type.clone(),
            workload: self.workload.clone(),
            instance_profile_name: self.instance_profile_name.clone(),
            security_group_dependency: security_group,
            module: placement.module.clone(),
        })
    }
}

/// Reusable author input for a capacity plane's auto-scaling group.
/// Declares its launch template and the VPC as dependencies.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AutoScalingGroup {
    /// Group name (resource id `asg-<name>`).
    pub(crate) name: String,
    /// Minimum instance count.
    pub(crate) min_size: u32,
    /// Desired instance count.
    pub(crate) desired_capacity: u32,
    /// Maximum instance count.
    pub(crate) max_size: u32,
    /// Protect fresh instances from scale-in (managed draining planes).
    #[serde(default)]
    pub(crate) new_instances_protected_from_scale_in: bool,
}

impl Kind<AsgResource> for AutoScalingGroup {
    fn realize(&self, placement: &PlacementContext) -> Result<AsgResource, KindError> {
        let launch_template =
            super::required_dependency(placement, "LaunchTemplate", |id| id.starts_with("lt-"))?;
        let vpc = super::required_dependency(placement, "Vpc", |id| id.ends_with("-vpc"))?;
        Ok(AsgResource {
            name: self.name.clone(),
            min_size: self.min_size,
            desired_capacity: self.desired_capacity,
            max_size: self.max_size,
            new_instances_protected_from_scale_in: self.new_instances_protected_from_scale_in,
            launch_template_dependency: launch_template,
            vpc_dependency: vpc,
            module: placement.module.clone(),
        })
    }
}

/// Reusable author input for an ECS capacity provider over one ASG.
/// Declares the cluster and the ASG as dependencies.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapacityProvider {
    /// Capacity-provider name (resource id `cp-<name>`).
    pub(crate) name: String,
}

impl Kind<CapacityProviderResource> for CapacityProvider {
    fn realize(&self, placement: &PlacementContext) -> Result<CapacityProviderResource, KindError> {
        let cluster =
            super::required_dependency(placement, "EcsCluster", |id| id == "ecs:cluster")?;
        let asg =
            super::required_dependency(placement, "AutoScalingGroup", |id| id.starts_with("asg-"))?;
        Ok(CapacityProviderResource {
            name: self.name.clone(),
            cluster_dependency: cluster,
            asg_dependency: asg,
            module: placement.module.clone(),
        })
    }
}
