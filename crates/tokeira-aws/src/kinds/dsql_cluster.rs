//! Typed author input for an Aurora DSQL cluster resource.

use serde::Deserialize;
use tokeira_platform::{
    error::KindError,
    kind::{Kind, PlacementContext},
};

use crate::resources::dsql_cluster::{DsqlCluster as Resource, DsqlClusterMode};

/// Reusable author input for [`Resource`]. Realizes the resource directly:
/// authored fields carry over flat, placement supplies project scope and
/// tags.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DsqlCluster {
    /// Stable AWS cluster identity.
    pub identity: String,
    /// AWS region.
    pub region: String,
    /// Managed or adopted lifecycle.
    pub mode: DsqlClusterMode,
    /// Required endpoint for an adopted cluster.
    #[serde(default)]
    pub endpoint: Option<String>,
    /// Required ARN for an adopted cluster.
    #[serde(default)]
    pub arn: Option<String>,
}

impl DsqlCluster {
    fn validate(&self) -> Result<(), KindError> {
        if self.identity.is_empty() {
            return Err(KindError::new("DSQL cluster identity cannot be empty"));
        }
        if self.region.is_empty() {
            return Err(KindError::new("DSQL cluster region cannot be empty"));
        }
        match self.mode {
            DsqlClusterMode::Managed if self.endpoint.is_some() || self.arn.is_some() => Err(
                KindError::new("managed DSQL clusters cannot declare an endpoint or ARN"),
            ),
            DsqlClusterMode::Preexisting if self.endpoint.is_none() || self.arn.is_none() => Err(
                KindError::new("preexisting DSQL clusters require both endpoint and ARN"),
            ),
            _ => Ok(()),
        }
    }
}

impl Kind for DsqlCluster {
    fn name(&self) -> &'static str {
        Resource::TYPE
    }

    fn validate_input(&self) -> Result<(), KindError> {
        self.validate()
    }

    fn declared_outputs(&self) -> &'static [&'static str] {
        &["cluster_endpoint", "cluster_arn"]
    }

    fn desired_manifest(&self, placement: &PlacementContext) -> serde_json::Value {
        serde_json::json!({
            "identity": self.identity,
            "region": self.region,
            "mode": match self.mode {
                DsqlClusterMode::Managed => "managed",
                DsqlClusterMode::Preexisting => "preexisting",
            },
            "endpoint": self.endpoint,
            "arn": self.arn,
            "module": placement.module,
        })
    }

    fn realize(
        &self,
        placement: &PlacementContext,
    ) -> Result<Box<dyn tokeira_iac::Resource>, KindError> {
        self.validate()?;
        Ok(Box::new(Resource {
            identity: self.identity.clone(),
            region: self.region.clone(),
            mode: self.mode,
            endpoint: self.endpoint.clone(),
            arn: self.arn.clone(),
            fallback_identifier: None,
            resource_id: None,
            module: placement.module.clone(),
            project: placement.deployment_id.clone(),
            tags: placement.tags.clone().into_iter().collect(),
        }))
    }
}
