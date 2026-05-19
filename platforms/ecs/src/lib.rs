//! ECS deployment platform scaffold.

pub mod config;
pub mod gates;
pub mod images;
pub mod modules;
pub mod services;

use std::{
    path::Path,
    sync::{Arc, OnceLock},
};

use async_trait::async_trait;
use tokeira_config::TokeiraConfig;
use tokeira_deploy_engine as deploy_engine;
use tokeira_iac as iac;
use tokeira_orchestrator::{
    Ops, PlatformConfig, PortMapping, Result, ServiceReplicas, StorageKind,
};
use tokeira_state::{S3Backend, StateBackend, StateError};

pub use config::EcsConfig;

#[derive(Debug, Clone, Default)]
pub struct EcsDeployment {
    aws_clients: Arc<OnceLock<tokeira_aws::AwsClients>>,
}

impl EcsDeployment {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn default_config_toml() -> String {
        annotate_image_lifecycle_fields(
            tokeira_config::write_config_toml(&EcsConfig::default()).expect("serializes"),
        )
    }

    async fn ensure_aws_clients(&self, config: &EcsConfig) -> Result<&tokeira_aws::AwsClients> {
        if let Some(clients) = self.aws_clients.get() {
            return Ok(clients);
        }
        let aws_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(aws_config::Region::new(config.region.clone()))
            .load()
            .await;
        let clients = tokeira_aws::AwsClients::new(&aws_config);
        let _ = self.aws_clients.set(clients);
        self.aws_clients.get().ok_or_else(|| {
            anyhow::anyhow!("failed to initialize AWS clients for ECS deployment").into()
        })
    }
}

fn annotate_image_lifecycle_fields(toml: String) -> String {
    toml.replace(
        "image = \"tokeirad:latest\"",
        "# populated by `tkr image push`\nimage = \"tokeirad:latest\"",
    )
    .replace(
        "mimir_image = ",
        "# populated by `tkr image mirror`\nmimir_image = ",
    )
    .replace(
        "grafana_image = ",
        "# populated by `tkr image mirror`\ngrafana_image = ",
    )
    .replace(
        "loki_image = ",
        "# populated by `tkr image mirror`\nloki_image = ",
    )
    .replace(
        "alloy_image = ",
        "# populated by `tkr image mirror`\nalloy_image = ",
    )
    .replace(
        "aws_cli_image = ",
        "# populated by `tkr image mirror`\naws_cli_image = ",
    )
    .replace(
        "busybox_image = ",
        "# populated by `tkr image mirror`\nbusybox_image = ",
    )
}

impl PlatformConfig for EcsDeployment {
    fn prototypical_config(_storage: StorageKind) -> String {
        Self::default_config_toml()
    }

    fn prototypical_server_config(storage: StorageKind) -> String {
        let mut config = TokeiraConfig::default();
        if storage == StorageKind::Dsql {
            config.infrastructure.dsql.endpoint = Some("replace-with-dsql-endpoint".to_string());
        }
        config.to_toml().expect("server config serializes")
    }
}

#[async_trait]
impl tokeira_orchestrator::Deployment for EcsDeployment {
    type Config = EcsConfig;

    fn remote_state_module(
        &self,
        config: &Self::Config,
        _deployment_dir: &Path,
    ) -> Box<dyn iac::Module> {
        Box::new(modules::RemoteStateModule::new(config.clone()))
    }

    fn infra_modules(
        &self,
        config: &Self::Config,
        selection: &iac::ModuleSelection,
    ) -> Vec<Box<dyn iac::Module>> {
        let mut modules: Vec<Box<dyn iac::Module>> = Vec::new();
        let candidates: Vec<Box<dyn iac::Module>> = vec![
            Box::new(modules::ImagesModule::new(config.clone())),
            Box::new(modules::NetworkingModule::new(config.clone())),
            Box::new(modules::DsqlModule::new(config.clone())),
            Box::new(modules::ClusterModule::new(config.clone())),
            Box::new(modules::ObservabilityModule::new(config.clone())),
            Box::new(modules::ServicesModule::new(config.clone())),
        ];
        for module in candidates {
            if selection.includes(module.name()) {
                modules.push(module);
            }
        }
        modules
    }

    fn services(&self, config: &Self::Config) -> Vec<Box<dyn deploy_engine::Service>> {
        services::EcsWorkload::build_all(config)
            .into_iter()
            .map(|service| Box::new(service) as Box<dyn deploy_engine::Service>)
            .collect()
    }

    fn images(&self, _config: &Self::Config) -> Vec<Box<dyn deploy_engine::Image>> {
        crate::images::construct()
    }

    fn required_namespaces(&self, _config: &Self::Config) -> Vec<String> {
        vec!["default".into()]
    }

    async fn register_infra_extensions(
        &self,
        config: &Self::Config,
        ctx: &mut iac::ProvisionContext,
    ) -> Result<()> {
        let clients = self.ensure_aws_clients(config).await?;
        ctx.project_name = config.project_name.clone();
        ctx.tags = config.tags.clone();
        ctx.set_extension(clients.clone());
        Ok(())
    }

    async fn register_deploy_extensions(
        &self,
        config: &Self::Config,
        _ctx: &mut deploy_engine::ServiceContext,
    ) -> Result<()> {
        self.ensure_aws_clients(config).await?;
        Ok(())
    }

    async fn register_image_extensions(
        &self,
        config: &Self::Config,
        ctx: &mut deploy_engine::ImageContext,
    ) -> Result<()> {
        ctx.set_extension(config.clone());
        Ok(())
    }

    fn create_infra_store(
        &self,
        config: &Self::Config,
        deployment_dir: &Path,
    ) -> Box<dyn StateBackend> {
        self.s3_state_backend(config, deployment_dir, "infra")
    }

    fn create_deploy_store(
        &self,
        config: &Self::Config,
        deployment_dir: &Path,
    ) -> Box<dyn StateBackend> {
        self.s3_state_backend(config, deployment_dir, "deploy")
    }

    fn hydrate_config(&self, config: &Self::Config, state: &iac::InfraState) -> Self::Config {
        let mut hydrated = config.clone();
        fill_if_missing(
            &mut hydrated.dsql.endpoint,
            state_property(state, "dsql:cluster", "cluster_endpoint"),
        );
        fill_if_missing(
            &mut hydrated.dsql.management_endpoint_id,
            state_property(state, "dsql:management-endpoint", "endpoint_id"),
        );
        fill_if_missing(
            &mut hydrated.dsql.connection_endpoint_id,
            state_property(state, "dsql:connection-endpoint", "endpoint_id"),
        );
        fill_if_missing(
            &mut hydrated.dsql.runtime_role_arn,
            state_property(state, "dsql:runtime-role", "role_arn"),
        );
        fill_if_missing(
            &mut hydrated.dsql.admin_role_arn,
            state_property(state, "dsql:admin-role", "role_arn"),
        );
        hydrated
    }

    fn collect_writeback(
        &self,
        config: &Self::Config,
        state: &iac::InfraState,
    ) -> Vec<(String, String)> {
        let hydrated = self.hydrate_config(config, state);
        let mut writeback = Vec::new();
        push_changed(
            &mut writeback,
            "dsql.endpoint",
            &config.dsql.endpoint,
            &hydrated.dsql.endpoint,
        );
        push_changed(
            &mut writeback,
            "dsql.management_endpoint_id",
            &config.dsql.management_endpoint_id,
            &hydrated.dsql.management_endpoint_id,
        );
        push_changed(
            &mut writeback,
            "dsql.connection_endpoint_id",
            &config.dsql.connection_endpoint_id,
            &hydrated.dsql.connection_endpoint_id,
        );
        push_changed(
            &mut writeback,
            "dsql.runtime_role_arn",
            &config.dsql.runtime_role_arn,
            &hydrated.dsql.runtime_role_arn,
        );
        push_changed(
            &mut writeback,
            "dsql.admin_role_arn",
            &config.dsql.admin_role_arn,
            &hydrated.dsql.admin_role_arn,
        );
        writeback
    }
}

impl EcsDeployment {
    fn s3_state_backend(
        &self,
        config: &EcsConfig,
        _deployment_dir: &Path,
        state_kind: &str,
    ) -> Box<dyn StateBackend> {
        let Some(clients) = self.aws_clients.get() else {
            return Box::new(MissingAwsClientsBackend);
        };
        Box::new(S3Backend::new(
            clients.s3.clone(),
            state_bucket_name(config),
            format!("{}/{state_kind}", state_key_prefix(config)),
        ))
    }
}

#[derive(Debug)]
struct MissingAwsClientsBackend;

#[async_trait]
impl StateBackend for MissingAwsClientsBackend {
    async fn read_manifest(
        &self,
        _key: &str,
    ) -> std::result::Result<Option<(Vec<u8>, String)>, StateError> {
        Err(StateError::Backend(
            "AWS clients were not registered before ECS state store creation".into(),
        ))
    }

    async fn write_manifest(
        &self,
        _key: &str,
        _data: &[u8],
        _expected_version: &str,
    ) -> std::result::Result<(), StateError> {
        Err(StateError::Backend(
            "AWS clients were not registered before ECS state store creation".into(),
        ))
    }

    async fn read_snapshot(&self, _key: &str) -> std::result::Result<Vec<u8>, StateError> {
        Err(StateError::Backend(
            "AWS clients were not registered before ECS state store creation".into(),
        ))
    }

    async fn write_snapshot(
        &self,
        _key: &str,
        _data: &[u8],
    ) -> std::result::Result<(), StateError> {
        Err(StateError::Backend(
            "AWS clients were not registered before ECS state store creation".into(),
        ))
    }

    async fn list_snapshots(&self, _prefix: &str) -> std::result::Result<Vec<String>, StateError> {
        Err(StateError::Backend(
            "AWS clients were not registered before ECS state store creation".into(),
        ))
    }
}

fn state_bucket_name(config: &EcsConfig) -> String {
    format!("{}-state-{}", config.project_name, config.region)
}

fn state_key_prefix(config: &EcsConfig) -> String {
    format!("{}/{}", config.project_name, config.environment)
}

#[async_trait]
impl Ops for EcsDeployment {
    type Config = EcsConfig;

    fn valid_services(&self) -> &[&str] {
        static VALID: [&str; 10] = [
            "edge-api",
            "edge-poll",
            "runtime",
            "projection",
            "controller",
            "autoscaler",
            "admin",
            "mimir",
            "loki",
            "grafana",
        ];
        &VALID
    }

    fn desired_replicas(&self, _config: &Self::Config) -> Vec<ServiceReplicas> {
        Vec::new()
    }

    async fn scale_up(&self, _service: &str, _replicas: u32, _config: &Self::Config) -> Result<()> {
        Err(anyhow::anyhow!("ECS scale operations are not implemented yet").into())
    }

    async fn scale_down(
        &self,
        _service: &str,
        _replicas: u32,
        _config: &Self::Config,
    ) -> Result<()> {
        Err(anyhow::anyhow!("ECS scale operations are not implemented yet").into())
    }

    async fn logs(&self, _service: &str, _config: &Self::Config) -> Result<Vec<String>> {
        Err(anyhow::anyhow!("ECS logs are not implemented yet").into())
    }

    async fn port_mappings(
        &self,
        _service: &str,
        _config: &Self::Config,
    ) -> Result<Vec<PortMapping>> {
        Err(anyhow::anyhow!("ECS port mappings are not implemented yet").into())
    }
}

fn state_property(state: &iac::InfraState, resource_id: &str, property: &str) -> Option<String> {
    state
        .resources
        .get(&iac::ResourceId(resource_id.to_owned()))
        .and_then(|resource| resource.properties.get(property))
        .and_then(|value| value.as_str())
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

fn fill_if_missing(target: &mut Option<String>, value: Option<String>) {
    if target.as_deref().unwrap_or("").is_empty() {
        *target = value;
    }
}

fn push_changed(
    writeback: &mut Vec<(String, String)>,
    key: &str,
    before: &Option<String>,
    after: &Option<String>,
) {
    if before != after {
        writeback.push((key.to_owned(), after.clone().unwrap_or_default()));
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use tokeira_iac::{ResourceId, ResourceState, ResourceType};
    use tokeira_orchestrator::{Deployment, Ops};

    use super::*;
    use crate::config::{AlbListenerProtocol, DsqlClusterMode, required_vpc_endpoints};

    #[test]
    fn default_config_validates_and_round_trips() {
        let config = EcsConfig::default();
        config.validate().expect("default config is valid");

        let toml = tokeira_config::write_config_toml(&config).expect("serialize config");
        let decoded: EcsConfig = toml::from_str(&toml).expect("deserialize config");

        assert_eq!(decoded, config);
    }

    #[test]
    fn unknown_config_fields_are_rejected() {
        let mut toml = tokeira_config::write_config_toml(&EcsConfig::default()).expect("toml");
        toml.push_str("\nunknown_field = true\n");

        let err = toml::from_str::<EcsConfig>(&toml).expect_err("unknown root field rejected");

        assert!(err.to_string().contains("unknown field"));
    }

    #[test]
    fn validation_enforces_https_certificate_and_preexisting_dsql() {
        let mut config = EcsConfig::default();
        config.alb.listener_protocol = AlbListenerProtocol::Https;

        assert!(matches!(
            config.validate(),
            Err(crate::config::EcsConfigError::MissingCertificateArn)
        ));

        let mut config = EcsConfig::default();
        config.dsql.mode = DsqlClusterMode::Preexisting;

        assert!(matches!(
            config.validate(),
            Err(crate::config::EcsConfigError::MissingPreexistingDsqlField(
                "dsql.endpoint"
            ))
        ));
    }

    #[test]
    fn required_endpoints_include_ssm_but_not_cloudwatch_logs() {
        let endpoints = required_vpc_endpoints("eu-west-2");

        assert!(endpoints.contains(&"com.amazonaws.eu-west-2.ssm".to_owned()));
        assert!(endpoints.contains(&"com.amazonaws.eu-west-2.ssmmessages".to_owned()));
        assert!(endpoints.contains(&"com.amazonaws.eu-west-2.ec2messages".to_owned()));
        assert!(!endpoints.contains(&"com.amazonaws.eu-west-2.logs".to_owned()));
    }

    #[test]
    fn valid_services_include_observability_targets() {
        let deployment = EcsDeployment::new();

        assert_eq!(deployment.valid_services().len(), 10);
        assert!(deployment.valid_services().contains(&"mimir"));
        assert!(deployment.valid_services().contains(&"loki"));
        assert!(deployment.valid_services().contains(&"grafana"));
    }

    #[test]
    fn dsql_hydration_and_writeback_use_state_properties() {
        let deployment = EcsDeployment::new();
        let config = EcsConfig::default();
        let mut state = iac::InfraState::default();
        state.resources = BTreeMap::from([
            resource(
                "dsql:cluster",
                "DsqlCluster",
                serde_json::json!({
                    "cluster_endpoint": "abc.dsql.eu-west-2.on.aws",
                }),
            ),
            resource(
                "dsql:management-endpoint",
                "DsqlPrivateLinkEndpoint",
                serde_json::json!({
                    "endpoint_id": "vpce-management",
                }),
            ),
            resource(
                "dsql:connection-endpoint",
                "DsqlPrivateLinkEndpoint",
                serde_json::json!({
                    "endpoint_id": "vpce-connection",
                }),
            ),
            resource(
                "dsql:runtime-role",
                "IamRole",
                serde_json::json!({
                    "role_arn": "arn:aws:iam::123:role/runtime",
                }),
            ),
            resource(
                "dsql:admin-role",
                "IamRole",
                serde_json::json!({
                    "role_arn": "arn:aws:iam::123:role/admin",
                }),
            ),
        ]);

        let hydrated = deployment.hydrate_config(&config, &state);
        let writeback = deployment.collect_writeback(&config, &state);

        assert_eq!(
            hydrated.dsql.endpoint.as_deref(),
            Some("abc.dsql.eu-west-2.on.aws")
        );
        assert!(writeback.contains(&(
            "dsql.connection_endpoint_id".to_owned(),
            "vpce-connection".to_owned()
        )));
        assert_eq!(deployment.hydrate_config(&hydrated, &state), hydrated);
    }

    fn resource(
        id: &str,
        resource_type: &str,
        properties: serde_json::Value,
    ) -> (ResourceId, ResourceState) {
        (
            ResourceId(id.to_owned()),
            ResourceState {
                resource_type: ResourceType::new(resource_type),
                physical_id: id.to_owned(),
                properties,
                dependencies: Vec::new(),
                created_at: String::new(),
                updated_at: String::new(),
                module: "dsql".to_owned(),
            },
        )
    }
}
