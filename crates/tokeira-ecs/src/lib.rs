//! ECS platform implementation: infrastructure resources and services,
//! workload manifests, image sources, and the legacy orchestrator-facing
//! deployment surface.
//!
//! This crate owns ECS *realization*. Description and framework integration
//! live in the `tokeira-ecs-deployment` package (`platforms/ecs`), which
//! re-exports this crate's legacy surface for existing callers.

// Service construction mirrors the broad ECS/ELB API surface, which takes many
// parameters.
#![allow(clippy::too_many_arguments)]

pub mod config;
pub mod gates;
pub mod images;
pub mod modules;
pub mod services;

use std::{
    path::{Path, PathBuf},
    sync::{Arc, OnceLock},
};

use async_trait::async_trait;
use tokeira_config::TokeiraConfig;
use tokeira_deploy_engine as deploy_engine;
use tokeira_iac as iac;
use tokeira_orchestrator::{
    Ops, PlatformConfig, PortMapping, Result, ServiceReplicas, StorageKind,
};
use tokeira_state::{CasStore, DeploymentStore, S3StateStore, StateBackend, StateError, Validate};

pub use config::EcsConfig;

#[derive(Debug, Clone)]
pub struct EcsDeployment {
    aws_clients: Arc<OnceLock<tokeira_aws::AwsClients>>,
    /// The deployment root: anchors runtime-loaded content (the
    /// observability documents under `<dir>/observability/`).
    deployment_dir: PathBuf,
}

impl EcsDeployment {
    pub fn new(deployment_dir: impl Into<PathBuf>) -> Self {
        Self {
            aws_clients: Arc::default(),
            deployment_dir: deployment_dir.into(),
        }
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
            Box::new(modules::ObservabilityModule::new(
                config.clone(),
                self.deployment_dir.join("observability"),
            )),
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
        _deployment_dir: &Path,
    ) -> Box<dyn DeploymentStore<iac::InfraState>> {
        self.s3_state_store(config, "infra")
    }

    fn create_deploy_store(
        &self,
        config: &Self::Config,
        _deployment_dir: &Path,
    ) -> Box<dyn DeploymentStore<iac::RuntimeState>> {
        self.s3_state_store(config, "deploy")
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
    /// Select the S3-native snapshot/lease [`S3StateStore`] for a state kind
    /// (`infra` / `deploy`) — the authoritative remote store for the cloud
    /// platform (task 13.2). Before AWS clients are registered, returns a store
    /// over an erroring backend so state operations fail loudly rather than
    /// silently persisting nowhere.
    fn s3_state_store<T>(&self, config: &EcsConfig, state_kind: &str) -> Box<dyn DeploymentStore<T>>
    where
        T: serde::Serialize
            + serde::de::DeserializeOwned
            + Default
            + Validate
            + Send
            + Sync
            + 'static,
    {
        let Some(clients) = self.aws_clients.get() else {
            return Box::new(CasStore::new(
                Box::new(MissingAwsClientsBackend),
                state_kind.to_string(),
            ));
        };
        Box::new(S3StateStore::new(
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

    fn desired_replicas(&self, config: &Self::Config) -> Vec<ServiceReplicas> {
        vec![
            ServiceReplicas {
                service: "edge-api".into(),
                replicas: config.services.edge_api.desired_count,
            },
            ServiceReplicas {
                service: "edge-poll".into(),
                replicas: config.services.edge_poll.desired_count,
            },
            ServiceReplicas {
                service: "projection".into(),
                replicas: config.services.projection.desired_count,
            },
            ServiceReplicas {
                service: "controller".into(),
                replicas: config.services.controller.desired_count,
            },
            ServiceReplicas {
                service: "autoscaler".into(),
                replicas: config.services.autoscaler.desired_count,
            },
            ServiceReplicas {
                service: "admin".into(),
                replicas: config.services.admin.desired_count,
            },
            ServiceReplicas {
                service: "mimir".into(),
                replicas: 1,
            },
            ServiceReplicas {
                service: "loki".into(),
                replicas: 1,
            },
            ServiceReplicas {
                service: "grafana".into(),
                replicas: 1,
            },
        ]
    }

    async fn scale_up(&self, service: &str, replicas: u32, config: &Self::Config) -> Result<()> {
        reject_observability_scale(service)?;
        let current = self.describe_service_desired_count(service, config).await?;
        self.update_service_desired_count(service, current.saturating_add(replicas), config)
            .await
    }

    async fn scale_down(&self, service: &str, replicas: u32, config: &Self::Config) -> Result<()> {
        reject_observability_scale(service)?;
        let current = self.describe_service_desired_count(service, config).await?;
        self.update_service_desired_count(service, current.saturating_sub(replicas), config)
            .await
    }

    async fn logs(&self, service: &str, config: &Self::Config) -> Result<Vec<String>> {
        validate_service_name(self.valid_services(), service)?;
        // Primary: query Loki via HTTP API at localhost (requires `tkr port-forward loki`)
        let ecs_service = ecs_service_name(service);
        match query_loki_logs(ecs_service, &config.observability.loki_query_url).await {
            Ok(lines) => Ok(lines),
            Err(loki_err) => {
                tracing::debug!(
                    ?loki_err,
                    "Loki query failed, attempting ECS task log fallback"
                );
                // Fallback: list running tasks and retrieve logs via ecs:DescribeTasks
                match self.ecs_task_logs_fallback(service, config).await {
                    Ok(lines) => Ok(lines),
                    Err(_) => Err(anyhow::anyhow!(
                        "could not retrieve logs for '{service}'\n\n\
                         Loki query failed: {loki_err}\n\n\
                         Logs are collected by Alloy sidecars and stored in Loki.\n\
                         To view logs, either:\n  \
                         1. Run `tkr port-forward loki` then retry `tkr logs {service}`\n  \
                         2. Open Grafana: `tkr port-forward grafana` → http://localhost:3000\n  \
                         3. Enable break-glass CloudWatch: `tkr debug logs enable`"
                    )
                    .into()),
                }
            }
        }
    }

    async fn port_mappings(
        &self,
        _service: &str,
        _config: &Self::Config,
    ) -> Result<Vec<PortMapping>> {
        Err(anyhow::anyhow!("ECS port mappings are not implemented yet").into())
    }
}

impl EcsDeployment {
    async fn describe_service_desired_count(
        &self,
        service: &str,
        config: &EcsConfig,
    ) -> Result<u32> {
        validate_service_name(Ops::valid_services(self), service)?;
        let clients = self.ensure_aws_clients(config).await?;
        let ecs_service = ecs_service_name(service);
        let output = clients
            .ecs
            .describe_services()
            .cluster(&config.cluster.name)
            .services(ecs_service)
            .send()
            .await
            .map_err(|err| {
                anyhow::anyhow!("ecs:DescribeServices failed: {}", err.into_service_error())
            })?;
        let service = output
            .services()
            .first()
            .ok_or_else(|| anyhow::anyhow!("ECS service '{ecs_service}' was not found"))?;
        Ok(service.desired_count().max(0) as u32)
    }

    async fn update_service_desired_count(
        &self,
        service: &str,
        desired_count: u32,
        config: &EcsConfig,
    ) -> Result<()> {
        validate_service_name(Ops::valid_services(self), service)?;
        let clients = self.ensure_aws_clients(config).await?;
        let ecs_service = ecs_service_name(service);
        clients
            .ecs
            .update_service()
            .cluster(&config.cluster.name)
            .service(ecs_service)
            .desired_count(desired_count as i32)
            .send()
            .await
            .map_err(|err| {
                anyhow::anyhow!("ecs:UpdateService failed: {}", err.into_service_error())
            })?;
        Ok(())
    }

    /// Fallback log retrieval: list running tasks and return their ARNs
    /// with status info so the operator knows what's running.
    async fn ecs_task_logs_fallback(
        &self,
        service: &str,
        config: &EcsConfig,
    ) -> Result<Vec<String>> {
        let clients = self.ensure_aws_clients(config).await?;
        let ecs_service = ecs_service_name(service);
        let task_list = clients
            .ecs
            .list_tasks()
            .cluster(&config.cluster.name)
            .service_name(ecs_service)
            .desired_status(aws_sdk_ecs::types::DesiredStatus::Running)
            .send()
            .await
            .map_err(|err| anyhow::anyhow!("ecs:ListTasks failed: {}", err.into_service_error()))?;

        let task_arns = task_list.task_arns();
        if task_arns.is_empty() {
            return Err(
                anyhow::anyhow!("no running tasks found for service '{ecs_service}'").into(),
            );
        }

        let describe_output = clients
            .ecs
            .describe_tasks()
            .cluster(&config.cluster.name)
            .set_tasks(Some(task_arns.to_vec()))
            .send()
            .await
            .map_err(|err| {
                anyhow::anyhow!("ecs:DescribeTasks failed: {}", err.into_service_error())
            })?;

        let mut lines = Vec::new();
        lines.push(format!(
            "--- ECS task info for {ecs_service} (Loki unavailable) ---"
        ));
        for task in describe_output.tasks() {
            let task_arn = task.task_arn().unwrap_or("unknown");
            let status = task.last_status().unwrap_or("unknown");
            let started_at = task
                .started_at()
                .map(|t| t.to_string())
                .unwrap_or_else(|| "not started".into());
            lines.push(format!("  task: {task_arn}"));
            lines.push(format!("  status: {status}, started: {started_at}"));
            lines.push(String::new());
        }
        lines.push("hint: logs are collected by Alloy sidecars → Loki".into());
        lines.push(format!(
            "hint: run `tkr port-forward loki` then `tkr logs {service}` for full logs"
        ));
        Ok(lines)
    }
}

/// Query Loki's HTTP API for recent log lines matching the given ECS service name.
///
/// Uses the LogQL query `{container_name=~"<service>.*"}` to match logs
/// collected by Alloy sidecars. The Loki endpoint is typically reachable
/// via `tkr port-forward loki` at localhost:3100.
async fn query_loki_logs(ecs_service: &str, loki_url: &str) -> anyhow::Result<Vec<String>> {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(5))
        .build()?;

    // LogQL query: match logs from the service's containers
    let query = format!("{{container_name=~\"{ecs_service}.*\"}}");
    let url = format!("{loki_url}/loki/api/v1/query_range");

    let response = client
        .get(&url)
        .query(&[
            ("query", query.as_str()),
            ("limit", "100"),
            ("direction", "backward"),
        ])
        .send()
        .await
        .map_err(|err| anyhow::anyhow!("failed to connect to Loki at {loki_url}: {err}"))?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(anyhow::anyhow!("Loki returned {status}: {body}"));
    }

    let body: serde_json::Value = response.json().await?;
    let mut lines = Vec::new();

    // Parse Loki query_range response: data.result[].values[][1]
    if let Some(results) = body
        .get("data")
        .and_then(|d| d.get("result"))
        .and_then(|r| r.as_array())
    {
        for stream in results {
            if let Some(values) = stream.get("values").and_then(|v| v.as_array()) {
                for entry in values {
                    if let Some(log_line) = entry.get(1).and_then(|v| v.as_str()) {
                        lines.push(log_line.to_string());
                    }
                }
            }
        }
    }

    if lines.is_empty() {
        return Err(anyhow::anyhow!(
            "no log entries found for '{ecs_service}' in Loki"
        ));
    }

    // Loki returns newest-first with direction=backward; reverse for chronological order
    lines.reverse();
    Ok(lines)
}

fn validate_service_name(valid: &[&str], service: &str) -> Result<()> {
    if valid.contains(&service) {
        Ok(())
    } else {
        Err(anyhow::anyhow!(
            "unknown service '{service}'. valid services: {}",
            valid.join(", ")
        )
        .into())
    }
}

/// Observability services have their desired-count managed by the
/// observability IaC module. Manual scaling would be overwritten on the
/// next `tkr infra apply` and could break the single-host invariant
/// that `tkr port-forward` relies on.
fn reject_observability_scale(service: &str) -> Result<()> {
    const OBSERVABILITY_SERVICES: &[&str] = &["mimir", "loki", "grafana"];
    if OBSERVABILITY_SERVICES.contains(&service) {
        Err(anyhow::anyhow!(
            "cannot scale '{service}' — its desired-count is managed by the observability module.\n\
             To change capacity, update the `capacity_providers.{service}` section in deployment.toml \
             and run `tkr infra apply`."
        )
        .into())
    } else {
        Ok(())
    }
}

fn ecs_service_name(service: &str) -> &'static str {
    match service {
        "edge-api" => "tokeira-edge-api",
        "edge-poll" => "tokeira-edge-poll",
        "runtime" => "tokeira-runtime",
        "projection" => "tokeira-projection",
        "controller" => "tokeira-controller",
        "autoscaler" => "tokeira-autoscaler",
        "admin" => "tokeira-admin",
        "mimir" => "tokeira-mimir",
        "loki" => "tokeira-loki",
        "grafana" => "tokeira-grafana",
        _ => "unknown",
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
        let deployment = EcsDeployment::new(std::env::temp_dir());

        assert_eq!(deployment.valid_services().len(), 10);
        assert!(deployment.valid_services().contains(&"mimir"));
        assert!(deployment.valid_services().contains(&"loki"));
        assert!(deployment.valid_services().contains(&"grafana"));
    }

    #[test]
    fn observability_services_reject_scale() {
        for service in ["mimir", "loki", "grafana"] {
            let err = reject_observability_scale(service).unwrap_err();
            let msg = err.to_string();
            assert!(
                msg.contains("managed by the observability module"),
                "expected observability rejection for '{service}', got: {msg}"
            );
        }
    }

    #[test]
    fn application_services_pass_observability_check() {
        for service in [
            "edge-api",
            "edge-poll",
            "runtime",
            "projection",
            "controller",
            "autoscaler",
            "admin",
        ] {
            reject_observability_scale(service)
                .unwrap_or_else(|_| panic!("{service} should not be rejected"));
        }
    }

    #[test]
    fn dsql_hydration_and_writeback_use_state_properties() {
        let deployment = EcsDeployment::new(std::env::temp_dir());
        let config = EcsConfig::default();
        let resources = BTreeMap::from([
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
        let state = iac::InfraState {
            resources,
            ..Default::default()
        };

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
