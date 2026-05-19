use std::collections::HashMap;

use tokeira_aws::{
    ResourceContext,
    resources::{
        ecs_service as aws_ecs,
        iam_role::{IamRole, IamRoleConfig},
        s3_bucket::{S3Bucket, S3BucketConfig},
        secrets_manager_secret::{SecretValue, SecretsManagerSecret, SecretsManagerSecretConfig},
        ssm_parameter::SsmParameterResource,
    },
};
use tokeira_iac::{IacError, Module, ModuleContext, Resource, ResourceId};

use crate::{config::EcsConfig, services::EcsWorkload};

#[derive(Debug, Clone)]
pub struct ObservabilityModule {
    config: EcsConfig,
}

impl ObservabilityModule {
    pub fn new(config: EcsConfig) -> Self {
        Self { config }
    }
}

impl Module for ObservabilityModule {
    fn name(&self) -> &str {
        "observability"
    }

    fn dependencies(&self) -> &[&str] {
        &["cluster"]
    }

    fn resources(&self, _ctx: &ModuleContext) -> Result<Vec<Box<dyn Resource>>, IacError> {
        let rctx = resource_context(&self.config);
        let mut resources: Vec<Box<dyn Resource>> = vec![
            Box::new(storage_bucket(
                format!("{}-mimir-data", self.config.project_name),
                &rctx,
                self.name(),
            )),
            Box::new(storage_bucket(
                format!("{}-loki-data", self.config.project_name),
                &rctx,
                self.name(),
            )),
            Box::new(storage_role(
                format!("{}-mimir-s3", self.config.project_name),
                format!("{}-mimir-data", self.config.project_name),
                &rctx,
                self.name(),
            )),
            Box::new(storage_role(
                format!("{}-loki-s3", self.config.project_name),
                format!("{}-loki-data", self.config.project_name),
                &rctx,
                self.name(),
            )),
            Box::new(SecretsManagerSecret::new(
                format!("{}/grafana/admin", self.config.project_name),
                SecretsManagerSecretConfig {
                    value: SecretValue::GeneratedPasswordJson {
                        username: "admin".to_owned(),
                        password_length: 32,
                    },
                    recovery_window_days: Some(7),
                    module: self.name().to_owned(),
                },
                &rctx,
            )),
        ];

        for service_name in all_alloy_services() {
            resources.push(Box::new(SsmParameterResource {
                name: format!("/{}/alloy/sidecar/{service_name}", self.config.project_name),
                value: render_alloy_config(service_name, &self.config),
                secure: true,
                module: self.name().to_owned(),
            }));
        }

        let vpc_id = ResourceId(format!("{}-vpc", self.config.project_name));
        for workload in EcsWorkload::build_observability(&self.config) {
            let mut task_role =
                super::services::service_task_role(&workload.name, &self.config, self.name());
            if workload.name == "tokeira-grafana" {
                task_role.config.inline_policies.insert(
                    "grafana-admin-secret-read".to_owned(),
                    grafana_secret_read_policy(&self.config),
                );
            }
            let task_role_dependency = task_role.resource_id();
            resources.push(Box::new(task_role));
            resources.push(Box::new(aws_ecs::TaskDefinitionResource {
                spec: super::services::to_aws_task_definition(
                    &workload.task_definition,
                    Some(task_role_dependency),
                ),
                module: self.name().to_owned(),
            }));
            resources.push(Box::new(aws_ecs::EcsServiceResource {
                service_name: workload.name.clone(),
                scheduling: super::services::to_aws_scheduling(&workload.scheduling),
                capacity_provider: workload.capacity_provider.clone(),
                service_connect: super::services::to_aws_service(&workload.service_connect),
                placement_constraints: workload
                    .placement_constraints
                    .iter()
                    .map(super::services::to_aws_placement_constraint)
                    .collect(),
                cluster_dependency: ResourceId("ecs:cluster".to_owned()),
                task_definition_dependency: ResourceId(format!(
                    "task-definition:{}",
                    workload.task_definition.family
                )),
                vpc_dependency: vpc_id.clone(),
                security_group_dependency: ResourceId("sg-control".to_owned()),
                module: self.name().to_owned(),
            }));
        }

        Ok(resources)
    }
}

pub fn all_alloy_services() -> [&'static str; 10] {
    [
        "tokeira-edge-api",
        "tokeira-edge-poll",
        "tokeira-runtime",
        "tokeira-projection",
        "tokeira-controller",
        "tokeira-autoscaler",
        "tokeira-admin",
        "tokeira-mimir",
        "tokeira-loki",
        "tokeira-grafana",
    ]
}

fn storage_bucket(name: String, rctx: &ResourceContext, module: &str) -> S3Bucket {
    S3Bucket::new(
        name,
        S3BucketConfig {
            versioning: true,
            module: module.to_owned(),
            key_prefix: None,
        },
        rctx,
    )
}

fn storage_role(
    role_name: String,
    bucket_name: String,
    rctx: &ResourceContext,
    module: &str,
) -> IamRole {
    let mut inline_policies = HashMap::new();
    inline_policies.insert(
        "s3-storage".to_owned(),
        serde_json::json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"],
                "Resource": [
                    format!("arn:aws:s3:::{bucket_name}"),
                    format!("arn:aws:s3:::{bucket_name}/*")
                ]
            }]
        })
        .to_string(),
    );
    IamRole::new(
        role_name,
        IamRoleConfig {
            trust_policy: ecs_tasks_assume_role_policy(),
            inline_policies,
            managed_policy_arns: Vec::new(),
            module: module.to_owned(),
        },
        rctx,
    )
}

fn ecs_tasks_assume_role_policy() -> String {
    serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": { "Service": "ecs-tasks.amazonaws.com" },
            "Action": "sts:AssumeRole"
        }]
    })
    .to_string()
}

fn grafana_secret_read_policy(config: &EcsConfig) -> String {
    serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Action": "secretsmanager:GetSecretValue",
            "Resource": format!(
                "arn:aws:secretsmanager:{}:*:secret:{}/grafana/admin-*",
                config.region, config.project_name
            )
        }]
    })
    .to_string()
}

pub fn render_alloy_config(service_name: &str, config: &EcsConfig) -> String {
    let namespace = &config.networking.private_dns_zone;
    let metrics_port = metrics_port_for(service_name, config);
    format!(
        r#"prometheus.scrape "tokeira" {{
  targets         = [{{ __address__ = "localhost:{metrics_port}" }}]
  forward_to      = [prometheus.remote_write.mimir.receiver]
  scrape_interval = "15s"
  job_name        = "{service_name}"
}}

prometheus.remote_write "mimir" {{
  endpoint {{
    url = "http://mimir.{namespace}:9009/api/v1/push"
  }}
  external_labels = {{
    service_name = "{service_name}"
    environment  = "{}"
    project      = "{}"
  }}
}}

discovery.docker "task" {{
  host = "unix:///var/run/docker.sock"
}}

discovery.relabel "task_logs" {{
  targets = discovery.docker.task.targets
  rule {{
    source_labels = ["__meta_docker_container_label_com_amazonaws_ecs_task_arn"]
    regex         = "TASK_ARN_PLACEHOLDER"
    action        = "keep"
  }}
}}

loki.source.docker "task" {{
  host       = "unix:///var/run/docker.sock"
  targets    = discovery.relabel.task_logs.output
  forward_to = [loki.write.default.receiver]
}}

loki.write "default" {{
  endpoint {{
    url = "http://loki.{namespace}:3100/loki/api/v1/push"
  }}
  external_labels = {{
    service_name = "{service_name}"
    environment  = "{}"
    project      = "{}"
    task_id      = "TASK_ID_PLACEHOLDER"
  }}
}}
"#,
        config.environment, config.project_name, config.environment, config.project_name
    )
}

fn metrics_port_for(service_name: &str, config: &EcsConfig) -> u16 {
    match service_name {
        "tokeira-mimir" => 9009,
        "tokeira-loki" => 3100,
        "tokeira-grafana" => 3000,
        "tokeira-runtime" => config.services.runtime.metrics_port,
        "tokeira-edge-api" => config.services.edge_api.metrics_port,
        "tokeira-edge-poll" => config.services.edge_poll.metrics_port,
        "tokeira-projection" => config.services.projection.metrics_port,
        "tokeira-controller" => config.services.controller.metrics_port,
        "tokeira-autoscaler" => config.services.autoscaler.metrics_port,
        "tokeira-admin" => config.services.admin.metrics_port,
        _ => 9090,
    }
}

fn resource_context(config: &EcsConfig) -> ResourceContext {
    ResourceContext {
        project: config.project_name.clone(),
        region: config.region.clone(),
        tags: config.tags.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokeira_iac::InfraState;

    fn module_context() -> ModuleContext<'static> {
        let state = Box::leak(Box::new(InfraState::default()));
        let extensions = Box::leak(Box::new(HashMap::new()));
        ModuleContext::new(state, extensions)
    }

    #[test]
    fn observability_module_reports_name_and_cluster_dependency() {
        let module = ObservabilityModule::new(EcsConfig::default());

        assert_eq!(module.name(), "observability");
        assert_eq!(module.dependencies(), &["cluster"]);
    }

    #[test]
    fn observability_module_enumerates_storage_alloy_params_and_services() {
        let module = ObservabilityModule::new(EcsConfig::default());
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
            5
        );
        assert!(ids.contains(&"secret-tokeira/grafana/admin".to_owned()));
        assert!(ids.iter().any(|id| id.contains("mimir-data")));
        assert!(ids.iter().any(|id| id.contains("loki-data")));
    }

    #[test]
    fn grafana_task_role_is_scoped_to_admin_secret() {
        let policy: serde_json::Value =
            serde_json::from_str(&grafana_secret_read_policy(&EcsConfig::default()))
                .expect("grafana secret policy");

        assert_eq!(
            policy["Statement"][0]["Action"].as_str(),
            Some("secretsmanager:GetSecretValue")
        );
        assert_eq!(
            policy["Statement"][0]["Resource"].as_str(),
            Some("arn:aws:secretsmanager:us-east-1:*:secret:tokeira/grafana/admin-*")
        );
    }

    #[test]
    fn alloy_config_contains_task_placeholders_and_localhost_scrape() {
        let config = render_alloy_config("tokeira-runtime", &EcsConfig::default());

        assert!(config.contains("localhost:9090"));
        assert!(config.contains("TASK_ARN_PLACEHOLDER"));
        assert!(config.contains("TASK_ID_PLACEHOLDER"));
        assert!(config.contains("loki.source.docker"));
    }
}
