//! Typed author inputs for the platform's derived-policy IAM roles.
//!
//! Policies are the platform's facts, not the operator's: the authored
//! surface is identity coordinates only, and each kind derives its trust
//! and inline policies through the same builders the legacy modules use.

use serde::Deserialize;
use tokeira_platform::{
    error::KindError,
    kind::{Kind, PlacementContext},
};

use crate::{EcsConfig, roles::PlatformRoleResource};

/// Author-visible name of the realized task-role resource type.
pub const TASK_ROLE_TYPE: &str = "EcsTaskRole";
/// Author-visible name of the realized execution-role resource type.
pub const EXECUTION_ROLE_TYPE: &str = "EcsExecutionRole";
/// Author-visible name of the realized storage-role resource type.
pub const STORAGE_ROLE_TYPE: &str = "ObservabilityStorageRole";

fn base_config(placement: &PlacementContext, region: &str) -> EcsConfig {
    EcsConfig {
        project_name: placement.deployment_id.clone(),
        region: region.to_string(),
        ..EcsConfig::default()
    }
}

/// Reusable author input for one service's task role: ECS Exec plus Alloy
/// config read, and — for Grafana — the admin-secret read policy the
/// dashboard container needs.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskRole {
    /// Canonical service name the role serves.
    pub service: String,
    /// AWS region.
    pub region: String,
}

impl Kind<PlatformRoleResource> for TaskRole {
    fn realize(&self, placement: &PlacementContext) -> Result<PlatformRoleResource, KindError> {
        let config = base_config(placement, &self.region);
        let mut role =
            crate::modules::services::service_task_role(&self.service, &config, &placement.module);
        if self.service == "tokeira-grafana" {
            role.config.inline_policies.insert(
                "grafana-admin-secret-read".to_owned(),
                crate::modules::observability::grafana_secret_read_policy(&config),
            );
        }
        Ok(PlatformRoleResource::wrap(role, TASK_ROLE_TYPE))
    }
}

/// Reusable author input for one service's execution role (ECS-agent-side
/// permissions). The definition declares which services carry one — the
/// services whose containers pull through ECR or read secrets.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionRole {
    /// Canonical service name the role serves.
    pub service: String,
    /// AWS region.
    pub region: String,
}

impl Kind<PlatformRoleResource> for ExecutionRole {
    fn realize(&self, placement: &PlacementContext) -> Result<PlatformRoleResource, KindError> {
        let config = base_config(placement, &self.region);
        let role =
            crate::modules::services::execution_role(&self.service, &config, &placement.module);
        Ok(PlatformRoleResource::wrap(role, EXECUTION_ROLE_TYPE))
    }
}

/// Reusable author input for an observability storage role scoped to one
/// bucket.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorageRole {
    /// Full role name.
    pub name: String,
    /// AWS region.
    pub region: String,
    /// Bucket the role's S3 policy is scoped to.
    pub bucket: String,
}

impl Kind<PlatformRoleResource> for StorageRole {
    fn realize(&self, placement: &PlacementContext) -> Result<PlatformRoleResource, KindError> {
        let rctx = tokeira_aws::ResourceContext {
            project: placement.deployment_id.clone(),
            region: self.region.clone(),
            tags: placement.tags.clone().into_iter().collect(),
        };
        let role = crate::modules::observability::storage_role(
            self.name.clone(),
            self.bucket.clone(),
            &rctx,
            &placement.module,
        );
        Ok(PlatformRoleResource::wrap(role, STORAGE_ROLE_TYPE))
    }
}
