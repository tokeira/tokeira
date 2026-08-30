//! ECS platform package: description, integration, and the platform-owned
//! observability content.
//!
//! Realization lives in `tokeira-ecs` (`crates/tokeira-ecs`); this package
//! assembles the definition-driven platform — modular `.tkd` and `.tkdp`
//! source sets describing the same graph, the platform-owned observability
//! kinds, and the one entry point — and re-exports the implementation crate's
//! legacy surface so existing callers keep their import paths.

use std::sync::Arc;

use tokeira_platform::{declaration::PlatformDeclaration, definition::Namespace};

pub mod observability;

pub use tokeira_ecs::{
    EcsConfig, EcsDeployment, config, execution::EcsPlatform, gates, images, modules, services,
};

/// The ECS platform declaration.
///
/// Construction is pure — no filesystem access, no AWS configuration
/// loading, no network. The `tokeira_aws` namespace's presence is the
/// framework's signal to install the deployment-scoped `AwsClients` bundle;
/// the integration deliberately registers no second bundle.
pub fn platform() -> PlatformDeclaration {
    PlatformDeclaration {
        namespaces: vec![
            tokeira_ecs::kinds::namespace(),
            tokeira_deployment::server_config::namespace(),
            observability::namespace(),
            Namespace {
                name: tokeira_aws::kinds::NAMESPACE,
                kinds: tokeira_aws::kinds::KINDS,
                defaults: None,
                decode: tokeira_aws::kinds::decode,
            },
        ],
        // The legacy operational surface needs authored region and cluster
        // coordinates that `DeploymentRef` does not carry; it stays on the
        // preserved legacy implementation until that contract is addressed
        // in its own slice.
        ops: None,
        execution: Box::new(tokeira_ecs::execution::EcsExecution),
        implementation: Arc::new(tokeira_ecs::execution::EcsIntegration),
    }
}

#[cfg(test)]
mod declaration_tests {
    use super::*;

    // The declaration is pure assembly: four namespaces, no ops, and the
    // execution seams — constructed with no I/O to fail.
    #[test]
    fn platform_declares_four_namespaces_and_no_ops() {
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
        assert!(declaration.ops.is_none());
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
    use tokeira_ecs::{
        EcsConfig,
        modules::{ObservabilityModule, observability::load_observability_artifacts},
    };
    use tokeira_iac::{
        InfraState,
        module::{Module, ModuleContext},
    };

    /// The platform-owned content tree: what a staged deployment carries.
    fn content_dir() -> std::path::PathBuf {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("observability")
    }

    fn module_context() -> ModuleContext<'static> {
        let state = Box::leak(Box::new(InfraState::default()));
        let extensions = Box::leak(Box::new(std::collections::HashMap::new()));
        ModuleContext::new(state, extensions)
    }

    #[test]
    fn observability_module_enumerates_storage_alloy_params_and_services() {
        let module = ObservabilityModule::new(EcsConfig::default(), content_dir());
        let resources = module.resources(&module_context()).expect("resources");
        let ids: Vec<String> = resources
            .iter()
            .map(|resource| resource.resource_id().0)
            .collect();

        assert_eq!(
            ids.iter()
                .filter(|id| id.starts_with("ssm-parameter:"))
                .count(),
            10
        );
        assert_eq!(
            ids.iter()
                .filter(|id| id.starts_with("task-definition:tokeira-"))
                .count(),
            3
        );
        assert_eq!(
            ids.iter()
                .filter(|id| id.starts_with("ecs-service:tokeira-"))
                .count(),
            3
        );
        assert_eq!(
            ids.iter().filter(|id| id.starts_with("iam-role-")).count(),
            6
        );
        assert_eq!(
            ids.iter().filter(|id| id.starts_with("s3-object:")).count(),
            11
        );
        assert!(ids.contains(&"secret-tokeira/grafana/admin".to_owned()));
        assert!(ids.iter().any(|id| id.contains("mimir-data")));
        assert!(ids.iter().any(|id| id.contains("loki-data")));
        assert!(ids.iter().any(|id| id.contains("observability-artifacts")));
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
        tokeira_observability::testing::DashboardValidator::validate_directory(
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
        tokeira_observability::testing::AlertRuleValidator::validate_directory(
            &content_dir().join("alerts"),
            &repo_root,
        )
        .unwrap();
    }
}
