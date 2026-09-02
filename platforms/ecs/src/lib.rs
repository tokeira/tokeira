//! ECS platform package: description, integration, and the platform-owned
//! observability content.
//!
//! Realization lives in `tokeira-ecs` (`crates/tokeira-ecs`); this package
//! assembles the definition-driven platform — modular `.tkd` and `.tkdp`
//! source sets describing the same graph, the platform-owned observability
//! kinds, and the one entry point.

use std::sync::Arc;

use tokeira_platform::{
    declaration::{DeploymentRef, ImageOperations, PlatformDeclaration, PlatformIntegration},
    definition::Namespace,
};

mod image_operations;
pub mod observability;
pub mod ops;

pub use tokeira_ecs::{EcsConfig, config, execution::EcsPlatform, gates, modules, services};

// Live operations evaluate through this same function so they cannot drift
// from the namespaces used to admit and realize an ECS definition.
fn namespaces() -> Vec<Namespace> {
    vec![
        tokeira_ecs::kinds::namespace(),
        tokeira_deployment::server_config::namespace(),
        observability::namespace(),
        Namespace {
            name: tokeira_aws::kinds::NAMESPACE,
            kinds: tokeira_aws::kinds::KINDS,
            defaults: None,
            decode: tokeira_aws::kinds::decode,
        },
    ]
}

/// ECS execution integration plus the platform-owned image lifecycle.
///
/// The reusable ECS crate realizes manifests and AWS operations. This
/// package-level adapter additionally owns definition evaluation, which is
/// required before image operations can select authored upstreams.
#[derive(Debug)]
struct EcsIntegration {
    execution: tokeira_ecs::execution::EcsIntegration,
    images: image_operations::EcsImageOperations,
}

#[async_trait::async_trait]
impl PlatformIntegration for EcsIntegration {
    fn image_operations(&self) -> Option<&dyn ImageOperations> {
        Some(&self.images)
    }

    async fn register_infra_extensions(
        &self,
        deployment: &DeploymentRef,
        ctx: &mut tokeira_iac::ProvisionContext,
    ) -> anyhow::Result<()> {
        self.execution
            .register_infra_extensions(deployment, ctx)
            .await
    }

    async fn register_deploy_extensions(
        &self,
        deployment: &DeploymentRef,
        ctx: &mut tokeira_deploy_engine::ServiceContext,
    ) -> anyhow::Result<()> {
        self.execution
            .register_deploy_extensions(deployment, ctx)
            .await
    }

    async fn register_image_extensions(
        &self,
        deployment: &DeploymentRef,
        ctx: &mut tokeira_deploy_engine::ImageContext,
    ) -> anyhow::Result<()> {
        self.execution
            .register_image_extensions(deployment, ctx)
            .await
    }

    fn service_platform(
        &self,
        deployment: &DeploymentRef,
    ) -> anyhow::Result<Box<dyn tokeira_deploy_engine::Platform>> {
        self.execution.service_platform(deployment)
    }
}

/// The ECS platform declaration.
///
/// Construction is pure — no filesystem access, no AWS configuration
/// loading, no network. The `tokeira_aws` namespace's presence is the
/// framework's signal to install the deployment-scoped `AwsClients` bundle;
/// the integration deliberately registers no second bundle.
pub fn platform() -> PlatformDeclaration {
    PlatformDeclaration {
        namespaces: namespaces(),
        ops: Some(Box::new(ops::EcsOps)),
        observability: Some(Box::new(observability::EcsObservabilityCheck)),
        execution: Box::new(tokeira_ecs::execution::EcsExecution),
        implementation: Arc::new(EcsIntegration {
            execution: tokeira_ecs::execution::EcsIntegration,
            images: image_operations::EcsImageOperations,
        }),
    }
}

#[cfg(test)]
mod declaration_tests {
    use super::*;

    // The declaration is pure assembly: four namespaces plus live ops and
    // execution seams, constructed with no I/O to fail.
    #[test]
    fn platform_declares_four_namespaces_and_ops() {
        let declaration = platform();
        let names: Vec<&str> = declaration
            .namespaces
            .iter()
            .map(|namespace| namespace.name)
            .collect();
        assert_eq!(
            names,
            [
                "tokeira_ecs",
                "tokeira_deployment",
                "tokeira_ecs_deployment",
                "tokeira_aws"
            ]
        );
        assert!(declaration.ops.is_some());
        assert!(declaration.observability.is_some());
        assert!(declaration.implementation.image_operations().is_some());
    }

    // Kind names stay collision-free across the declared namespaces — the
    // same invariant the bound platform enforces at process start.
    #[test]
    fn kind_names_do_not_collide_across_namespaces() {
        let declaration = platform();
        let mut seen = std::collections::BTreeSet::new();
        for namespace in &declaration.namespaces {
            for kind in namespace.kinds {
                assert!(
                    seen.insert(*kind),
                    "kind `{kind}` advertised by more than one namespace"
                );
            }
        }
    }
}

/// Content-coupled tests live beside the content they validate: this package
/// ships `observability/{dashboards,alerts}`, so it owns the loader-vs-tree
/// agreement and the style contracts over the shipped artifacts.
#[cfg(test)]
mod content_tests {
    use tokeira_ecs::modules::observability::load_observability_artifacts;

    /// The platform-owned content tree: what a staged deployment carries.
    fn content_dir() -> std::path::PathBuf {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("observability")
    }

    // The content tree and the loader agree: the artifact set is every
    // dashboard, sorted, plus the alert rules last — the same set the crate
    // used to embed.
    #[test]
    fn the_platform_content_tree_loads_completely() {
        let artifacts = load_observability_artifacts(&content_dir()).unwrap();
        let keys: Vec<&str> = artifacts
            .iter()
            .map(|artifact| artifact.key.as_str())
            .collect();
        assert_eq!(
            keys,
            [
                "dashboards/autoscaler.json",
                "dashboards/broker-runtime-health.json",
                "dashboards/dsql-connection-health.json",
                "dashboards/grpc-edge-health.json",
                "dashboards/infrastructure-health.json",
                "dashboards/log-exploration.json",
                "dashboards/occ-contention.json",
                "dashboards/placement-controller.json",
                "dashboards/projection-workers.json",
                "dashboards/storage-projection-health.json",
                "alerts/observability-alerts.yaml",
            ]
        );
        assert!(
            artifacts
                .iter()
                .all(|artifact| !artifact.content.is_empty())
        );
        assert!(
            artifacts
                .iter()
                .take(10)
                .all(|artifact| artifact.content_type == "application/json")
        );
        assert_eq!(artifacts[10].content_type, "application/yaml");
    }

    // The platform owns its observability content, so it owns the style
    // contract over it: every shipped dashboard and alert rule validates.
    #[test]
    fn dashboards_follow_the_style_contract() {
        tokeira_observability::validation::DashboardValidator::validate_directory(
            &content_dir().join("dashboards"),
        )
        .unwrap();
    }

    #[test]
    fn alert_rules_follow_the_style_contract() {
        let repo_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .ancestors()
            .nth(2)
            .expect("platforms/ecs sits two levels below the workspace root")
            .to_path_buf();
        tokeira_observability::validation::AlertRuleValidator::validate_directory(
            &content_dir().join("alerts"),
            &repo_root,
        )
        .unwrap();
    }
}
