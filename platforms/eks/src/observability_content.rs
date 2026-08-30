//! Platform-owned observability configuration content.
//!
//! The catalog stages `observability/` beside each retained definition
//! revision. This namespace turns those revision-bound templates into the
//! ConfigMaps consumed by the Mimir, Loki, Grafana, and Alloy containers.
//! Rendering occurs at realization, so a content edit is visible in desired
//! service manifests and never reaches across to a newer live revision.

use std::path::Path;

use serde::Deserialize;
use tokeira_deploy_engine::{RuntimeError, Service, ServiceContext};
use tokeira_platform::{
    author::LocatedValue,
    definition::Namespace,
    error::KindError,
    kind::{self, DecodedKind, Kind, PlacementContext},
};

use crate::manifests;

/// Normalized namespace definitions import for EKS companion content.
pub const NAMESPACE: &str = "tokeira_eks_content";
/// Author-visible observability content bundle type.
pub const OBSERVABILITY_CONTENT_TYPE: &str = "ObservabilityContent";
/// Complete content-kind vocabulary owned by this package.
pub const KINDS: &[&str] = &[OBSERVABILITY_CONTENT_TYPE];

const CONTENT_SERVICE: &str = "observability-content";
const ALLOY_CONSUMERS: &[&str] = &[
    "tokeirad",
    "tokeira-controller",
    "tokeira-autoscaler",
    "mimir",
    "loki",
    "grafana",
];

/// Authored inputs used to render the revision-bound content templates.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObservabilityContent {
    /// Kubernetes namespace receiving the rendered ConfigMaps.
    pub namespace: String,
    /// Stable deployment/project identity used in bucket names and labels.
    pub project: String,
    /// AWS region used by the S3-backed observability services.
    pub region: String,
    /// Common backend retention policy in days.
    pub retention_days: u32,
}

impl Kind<ObservabilityContentService> for ObservabilityContent {
    fn realize(
        &self,
        placement: &PlacementContext,
    ) -> Result<ObservabilityContentService, KindError> {
        let content_dir = placement.definition_dir.join("observability");
        Ok(ObservabilityContentService {
            namespace: self.namespace.clone(),
            project: self.project.clone(),
            module: placement.module.clone(),
            mimir: render(&content_dir, "mimir.yaml", self)?,
            loki: render(&content_dir, "loki.yaml", self)?,
            grafana: render(&content_dir, "grafana.ini", self)?,
            alloy: render(&content_dir, "config.alloy", self)?,
        })
    }
}

fn render(
    content_dir: &Path,
    name: &str,
    input: &ObservabilityContent,
) -> Result<String, KindError> {
    let path = content_dir.join(name);
    std::fs::read_to_string(&path)
        .map(|template| {
            template
                .replace("{{ project }}", &input.project)
                .replace("{{ region }}", &input.region)
                .replace("{{ retention_days }}", &input.retention_days.to_string())
        })
        .map_err(|error| {
            KindError::new(format!(
                "observability content `{}` cannot be read: {error}",
                path.display()
            ))
        })
}

/// One deploy-plane content service whose manifests are revision-bound
/// ConfigMaps. It owns no workload and therefore has no runtime dependencies.
#[derive(Debug)]
pub struct ObservabilityContentService {
    namespace: String,
    project: String,
    module: String,
    mimir: String,
    loki: String,
    grafana: String,
    alloy: String,
}

impl Service for ObservabilityContentService {
    fn resource_type(&self) -> &'static str {
        OBSERVABILITY_CONTENT_TYPE
    }

    fn name(&self) -> &str {
        CONTENT_SERVICE
    }

    fn module(&self) -> &str {
        &self.module
    }

    fn dependencies(&self) -> Vec<&str> {
        Vec::new()
    }

    fn manifests(&self, _ctx: &ServiceContext) -> Result<Vec<serde_json::Value>, RuntimeError> {
        let mut desired = vec![
            manifests::config_map(
                "mimir-config",
                &self.namespace,
                &self.project,
                "mimir.yaml",
                &self.mimir,
            ),
            manifests::config_map(
                "loki-config",
                &self.namespace,
                &self.project,
                "loki.yaml",
                &self.loki,
            ),
            manifests::config_map(
                "grafana-config",
                &self.namespace,
                &self.project,
                "grafana.ini",
                &self.grafana,
            ),
        ];
        desired.extend(ALLOY_CONSUMERS.iter().map(|service| {
            manifests::config_map(
                &format!("alloy-config-{service}"),
                &self.namespace,
                &self.project,
                "config.alloy",
                &self.alloy,
            )
        }));
        Ok(desired)
    }
}

/// Decode one authored content kind.
pub fn decode(name: &str, value: LocatedValue) -> Option<Result<DecodedKind, KindError>> {
    match name {
        OBSERVABILITY_CONTENT_TYPE => Some(kind::decode_service::<
            ObservabilityContent,
            ObservabilityContentService,
        >(OBSERVABILITY_CONTENT_TYPE, value)),
        _ => None,
    }
}

/// Assemble the platform-owned companion-content namespace.
pub fn namespace() -> Namespace {
    Namespace {
        name: NAMESPACE,
        kinds: KINDS,
        defaults: None,
        decode,
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    // The staged tree is part of the package contract: every template reads
    // and all dynamic tokens are resolved before a manifest reaches apply.
    #[test]
    fn shipped_content_renders_without_unresolved_tokens() {
        let input = ObservabilityContent {
            namespace: "tokeira-system".into(),
            project: "demo".into(),
            region: "eu-west-2".into(),
            retention_days: 30,
        };
        let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("observability");
        for name in ["mimir.yaml", "loki.yaml", "grafana.ini", "config.alloy"] {
            let rendered = render(&dir, name, &input).expect("shipped template renders");
            assert!(!rendered.contains("{{"), "unresolved token in {name}");
        }
    }
}
