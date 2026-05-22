use std::collections::HashMap;

use tokeira_aws::{
    ResourceContext,
    resources::{
        ecs_service as aws_ecs,
        iam_role::{IamRole, IamRoleConfig},
    },
};
use tokeira_iac::{IacError, Module, ModuleContext, Resource, ResourceId};

use crate::{
    config::EcsConfig,
    services::{
        ContainerSpec, EcsScheduling, EcsWorkload, MountPointSpec, PlacementConstraint,
        PortMappingSpec, ServiceConnectPort, ServiceConnectSpec, TaskDefinitionSpec, VolumeSpec,
    },
};

#[derive(Debug, Clone)]
pub struct ServicesModule {
    config: EcsConfig,
}

impl ServicesModule {
    pub fn new(config: EcsConfig) -> Self {
        Self { config }
    }
}

impl Module for ServicesModule {
    fn name(&self) -> &str {
        "services"
    }

    fn dependencies(&self) -> &[&str] {
        &["observability"]
    }

    fn resources(&self, _ctx: &ModuleContext) -> Result<Vec<Box<dyn Resource>>, IacError> {
        let vpc_id = ResourceId(format!("{}-vpc", self.config.project_name));
        let mut resources: Vec<Box<dyn Resource>> =
            vec![Box::new(aws_ecs::CloudMapNamespaceResource {
                name: self.config.networking.private_dns_zone.clone(),
                vpc_dependency: vpc_id.clone(),
                module: self.name().to_owned(),
            })];

        for workload in EcsWorkload::build_all(&self.config) {
            let task_role = service_task_role(&workload.name, &self.config, self.name());
            let task_role_dependency = task_role.resource_id();
            let task_definition =
                to_aws_task_definition(&workload.task_definition, Some(task_role_dependency));
            let service = to_aws_service(&workload.service_connect);
            resources.push(Box::new(task_role));
            resources.push(Box::new(aws_ecs::TaskDefinitionResource {
                spec: task_definition,
                module: self.name().to_owned(),
            }));
            resources.push(Box::new(aws_ecs::EcsServiceResource {
                service_name: workload.name.clone(),
                scheduling: to_aws_scheduling(&workload.scheduling),
                capacity_provider: workload.capacity_provider.clone(),
                service_connect: service,
                placement_constraints: workload
                    .placement_constraints
                    .iter()
                    .map(to_aws_placement_constraint)
                    .collect(),
                cluster_dependency: ResourceId("ecs:cluster".to_owned()),
                task_definition_dependency: ResourceId(format!(
                    "task-definition:{}",
                    workload.task_definition.family
                )),
                vpc_dependency: vpc_id.clone(),
                security_group_dependency: security_group_for_workload(&workload.name),
                module: self.name().to_owned(),
            }));
        }

        Ok(resources)
    }
}

pub(crate) fn to_aws_scheduling(scheduling: &EcsScheduling) -> aws_ecs::EcsScheduling {
    match scheduling {
        EcsScheduling::Replica { desired_count } => aws_ecs::EcsScheduling::Replica {
            desired_count: *desired_count,
        },
        EcsScheduling::Daemon => aws_ecs::EcsScheduling::Daemon,
    }
}

pub(crate) fn to_aws_task_definition(
    spec: &TaskDefinitionSpec,
    task_role_dependency: Option<ResourceId>,
) -> aws_ecs::TaskDefinitionSpec {
    aws_ecs::TaskDefinitionSpec {
        family: spec.family.clone(),
        cpu: spec.cpu,
        memory_mb: spec.memory_mb,
        containers: spec.containers.iter().map(to_aws_container).collect(),
        volumes: spec.volumes.iter().map(to_aws_volume).collect(),
        task_role_dependency,
    }
}

fn to_aws_container(spec: &ContainerSpec) -> aws_ecs::ContainerSpec {
    aws_ecs::ContainerSpec {
        name: spec.name.clone(),
        image: spec.image.clone(),
        essential: spec.essential,
        cpu: spec.cpu,
        memory_mb: spec.memory_mb,
        command: spec.command.clone(),
        port_mappings: spec.port_mappings.iter().map(to_aws_port_mapping).collect(),
        mount_points: spec.mount_points.iter().map(to_aws_mount_point).collect(),
        environment: spec
            .environment
            .iter()
            .map(|env| aws_ecs::EnvironmentSpec {
                name: env.name.clone(),
                value: env.value.clone(),
            })
            .collect(),
        secrets: spec.secrets.iter().map(to_aws_secret).collect(),
        depends_on: spec
            .depends_on
            .iter()
            .map(|dependency| aws_ecs::ContainerDependencySpec {
                container_name: dependency.container_name.clone(),
                condition: dependency.condition.clone(),
            })
            .collect(),
        init_process_enabled: spec
            .linux_parameters
            .as_ref()
            .map(|params| params.init_process_enabled)
            .unwrap_or(false),
    }
}

fn to_aws_secret(spec: &crate::services::SecretEnvVar) -> aws_ecs::SecretSpec {
    aws_ecs::SecretSpec {
        name: spec.name.clone(),
        value_from: spec.value_from.clone(),
    }
}

fn to_aws_port_mapping(spec: &PortMappingSpec) -> aws_ecs::PortMappingSpec {
    aws_ecs::PortMappingSpec {
        name: spec.name.clone(),
        container_port: spec.container_port,
    }
}

fn to_aws_mount_point(spec: &MountPointSpec) -> aws_ecs::MountPointSpec {
    aws_ecs::MountPointSpec {
        source_volume: spec.source_volume.clone(),
        container_path: spec.container_path.clone(),
        read_only: spec.read_only,
    }
}

fn to_aws_volume(spec: &VolumeSpec) -> aws_ecs::VolumeSpec {
    aws_ecs::VolumeSpec {
        name: spec.name.clone(),
        host_path: spec.host_path.clone(),
    }
}

pub(crate) fn to_aws_service(spec: &ServiceConnectSpec) -> aws_ecs::ServiceConnectSpec {
    aws_ecs::ServiceConnectSpec {
        grpc: spec.grpc.as_ref().map(to_aws_service_connect_port),
        metrics: to_aws_service_connect_port(&spec.metrics),
    }
}

fn to_aws_service_connect_port(spec: &ServiceConnectPort) -> aws_ecs::ServiceConnectPort {
    aws_ecs::ServiceConnectPort {
        port_name: spec.port_name.clone(),
        container_port: spec.container_port,
        discovery_name: spec.discovery_name.clone(),
        dns_name: spec.dns_name.clone(),
    }
}

pub(crate) fn to_aws_placement_constraint(
    spec: &PlacementConstraint,
) -> aws_ecs::PlacementConstraintSpec {
    aws_ecs::PlacementConstraintSpec {
        expression: spec.expression.clone(),
    }
}

fn security_group_for_workload(service: &str) -> ResourceId {
    match service {
        "tokeira-edge-api" | "tokeira-edge-poll" => ResourceId("sg-edge".to_owned()),
        "tokeira-runtime" => ResourceId("sg-runtime".to_owned()),
        "tokeira-projection" => ResourceId("sg-projection".to_owned()),
        _ => ResourceId("sg-control".to_owned()),
    }
}

pub(crate) fn service_task_role(service_name: &str, config: &EcsConfig, module: &str) -> IamRole {
    let mut inline_policies = HashMap::new();
    inline_policies.insert("ecs-exec".to_owned(), ecs_exec_policy());
    inline_policies.insert(
        "alloy-config-read".to_owned(),
        alloy_config_read_policy(config),
    );
    // Both ECS Exec and the Alloy config fetch happen inside task containers,
    // so these permissions must live on the task role rather than the execution
    // role used by the ECS agent.
    IamRole::new(
        service_task_role_name(service_name, config),
        IamRoleConfig {
            trust_policy: ecs_tasks_assume_role_policy(),
            inline_policies,
            managed_policy_arns: Vec::new(),
            module: module.to_owned(),
        },
        &resource_context(config),
    )
}

fn service_task_role_name(service_name: &str, config: &EcsConfig) -> String {
    format!("{}-{}-task", config.project_name, service_name)
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

fn ecs_exec_policy() -> String {
    serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Action": [
                "ssmmessages:CreateControlChannel",
                "ssmmessages:CreateDataChannel",
                "ssmmessages:OpenControlChannel",
                "ssmmessages:OpenDataChannel"
            ],
            "Resource": "*"
        }]
    })
    .to_string()
}

fn alloy_config_read_policy(config: &EcsConfig) -> String {
    serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Action": "ssm:GetParameter",
            "Resource": format!(
                "arn:aws:ssm:{}:*:parameter/{}/alloy/sidecar/*",
                config.region, config.project_name
            )
        }]
    })
    .to_string()
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
        let extensions = Box::leak(Box::new(std::collections::HashMap::new()));
        ModuleContext::new(state, extensions)
    }

    #[test]
    fn services_module_reports_name_and_observability_dependency() {
        let module = ServicesModule::new(EcsConfig::default());

        assert_eq!(module.name(), "services");
        assert_eq!(module.dependencies(), &["observability"]);
    }

    #[test]
    fn services_module_enumerates_namespace_task_definitions_and_services() {
        let module = ServicesModule::new(EcsConfig::default());
        let resources = module.resources(&module_context()).expect("resources");
        let ids: Vec<String> = resources
            .iter()
            .map(|resource| resource.resource_id().0)
            .collect();

        assert_eq!(ids.len(), 22);
        assert!(ids.contains(&"cloudmap:namespace".to_owned()));
        assert_eq!(
            ids.iter().filter(|id| id.starts_with("iam-role-")).count(),
            7
        );
        assert_eq!(
            ids.iter()
                .filter(|id| id.starts_with("task-definition:"))
                .count(),
            7
        );
        assert_eq!(
            ids.iter()
                .filter(|id| id.starts_with("ecs-service:"))
                .count(),
            7
        );
    }

    #[test]
    fn service_task_roles_include_exec_and_alloy_ssm_read_permissions() {
        let config = EcsConfig::default();
        let role = service_task_role("tokeira-runtime", &config, "services");
        let exec_policy: serde_json::Value =
            serde_json::from_str(&role.config.inline_policies["ecs-exec"]).expect("exec policy");
        let ssm_policy: serde_json::Value =
            serde_json::from_str(&role.config.inline_policies["alloy-config-read"])
                .expect("ssm policy");

        let exec_actions = exec_policy["Statement"][0]["Action"]
            .as_array()
            .expect("exec actions");
        assert_eq!(exec_actions.len(), 4);
        assert!(
            exec_actions
                .iter()
                .any(|action| { action.as_str() == Some("ssmmessages:CreateControlChannel") })
        );
        assert_eq!(
            ssm_policy["Statement"][0]["Action"].as_str(),
            Some("ssm:GetParameter")
        );
        assert_eq!(
            ssm_policy["Statement"][0]["Resource"].as_str(),
            Some("arn:aws:ssm:eu-west-2:*:parameter/tokeira/alloy/sidecar/*")
        );
    }

    #[test]
    fn aws_task_definition_preserves_observability_environment() {
        let workload = EcsWorkload::build_all(&EcsConfig::default())
            .into_iter()
            .find(|workload| workload.name == "tokeira-runtime")
            .expect("runtime workload");
        let task_definition = to_aws_task_definition(&workload.task_definition, None);
        let primary = task_definition
            .containers
            .iter()
            .find(|container| container.name == "tokeira-runtime")
            .expect("runtime container");
        let env: HashMap<&str, &str> = primary
            .environment
            .iter()
            .map(|var| (var.name.as_str(), var.value.as_str()))
            .collect();

        assert_eq!(
            env.get("TOKEIRA_OBSERVABILITY_METRICS_ADDR"),
            Some(&"0.0.0.0:9090")
        );
        assert_eq!(env.get("TOKEIRA_OBSERVABILITY_CLUSTER"), Some(&"tokeira"));
    }
}
