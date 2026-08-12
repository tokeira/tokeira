//! ECS platform package: description, integration, and the platform-owned
//! observability content tree.
//!
//! Realization lives in `tokeira-ecs` (`crates/tokeira-ecs`); this package
//! re-exports that crate's legacy surface so existing callers keep their
//! import paths while the definition-driven platform is assembled here.

pub use tokeira_ecs::{EcsConfig, EcsDeployment, config, gates, images, modules, services};

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
