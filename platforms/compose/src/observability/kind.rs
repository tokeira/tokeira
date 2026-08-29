//! Authored twin for the platform-owned observability configuration resource.

use serde::{Deserialize, Serialize};
use tokeira_platform::{
    author::LocatedValue,
    definition::Namespace,
    error::KindError,
    kind::{self, DecodedKind, Kind, PlacementContext},
};

use super::{ObservabilityConfigFilesResource, ObservabilityParams};

/// Authored parameters for the observability configuration-files resource.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObservabilityConfiguration {
    /// Metrics scrape host.
    pub(crate) scrape_host: String,
    /// Metrics scrape port.
    pub(crate) scrape_port: u16,
    /// Cluster label.
    pub(crate) cluster: String,
    /// Deployment label.
    pub(crate) deployment: String,
    /// Mimir remote-write endpoint.
    pub(crate) mimir_remote_write: String,
    /// Loki push endpoint.
    pub(crate) loki_push: String,
    /// Mimir HTTP port.
    pub(crate) mimir_http_port: u16,
    /// Loki HTTP port.
    pub(crate) loki_http_port: u16,
    /// Loki retention period.
    pub(crate) retention_hours: u32,
}

impl ObservabilityConfiguration {
    fn params(&self) -> ObservabilityParams {
        ObservabilityParams {
            metrics_target_host: self.scrape_host.clone(),
            metrics_target_port: self.scrape_port,
            cluster: self.cluster.clone(),
            deployment: self.deployment.clone(),
            mimir_remote_write_url: self.mimir_remote_write.clone(),
            loki_push_url: self.loki_push.clone(),
            mimir_http_port: self.mimir_http_port,
            loki_http_port: self.loki_http_port,
            loki_retention_hours: self.retention_hours,
        }
    }
}

impl Kind<ObservabilityConfigFilesResource> for ObservabilityConfiguration {
    fn realize(
        &self,
        placement: &PlacementContext,
    ) -> Result<ObservabilityConfigFilesResource, KindError> {
        Ok(ObservabilityConfigFilesResource::new(
            placement.deployment_dir.clone(),
            placement.definition_dir.clone(),
            self.params(),
        ))
    }
}

/// The normalized crate name definitions import this platform resource from.
pub const NAMESPACE: &str = "tokeira_compose_deployment";

/// Platform-owned resource kinds exposed to frontends.
pub const KINDS: &[&str] = &[ObservabilityConfigFilesResource::TYPE];

/// Decode one platform-owned kind; `None` means the name is not ours.
pub fn decode(name: &str, value: LocatedValue) -> Option<Result<DecodedKind, KindError>> {
    (name == ObservabilityConfigFilesResource::TYPE).then(|| {
        kind::decode_resource::<ObservabilityConfiguration, ObservabilityConfigFilesResource>(
            ObservabilityConfigFilesResource::TYPE,
            value,
        )
    })
}

/// Authoring namespace for the platform-owned resource.
pub fn namespace() -> Namespace {
    Namespace {
        name: NAMESPACE,
        kinds: KINDS,
        defaults: None,
        decode,
    }
}
