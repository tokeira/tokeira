//! ECS task-definition and IAM-role translation for deployment definitions.
//!
//! Definition kinds own service scheduling and dependency topology. This
//! module centralizes provider-specific task and role shapes so those kinds
//! cannot silently diverge in their security or container semantics.

use std::collections::HashMap;

use tokeira_aws::{
    ResourceContext,
    resources::{
        ecs_service as aws_ecs,
        iam_role::{IamRole, IamRoleConfig},
    },
};
use tokeira_iac::ResourceId;

use crate::{
    config::EcsConfig,
    services::{ContainerSpec, MountPointSpec, PortMappingSpec, TaskDefinitionSpec, VolumeSpec},
};

pub(crate) fn to_aws_task_definition(
    spec: &TaskDefinitionSpec,
    task_role_dependency: Option<ResourceId>,
    execution_role_dependency: Option<ResourceId>,
) -> aws_ecs::TaskDefinitionSpec {
    aws_ecs::TaskDefinitionSpec {
        family: spec.family.clone(),
        cpu: spec.cpu,
        memory_mb: spec.memory_mb,
        containers: spec.containers.iter().map(to_aws_container).collect(),
        volumes: spec.volumes.iter().map(to_aws_volume).collect(),
        task_role_dependency,
        execution_role_dependency,
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
            dependent_inline_policies: Vec::new(),
            managed_policy_arns: Vec::new(),
            module: module.to_owned(),
        },
        &resource_context(config),
    )
}

/// Builds the execution role for a definition-owned ECS task.
///
/// The definition names which services carry this role. Keeping that ownership
/// explicit prevents an image-name heuristic from silently changing the IAM
/// graph when a workload is authored differently.
pub(crate) fn execution_role(service_name: &str, config: &EcsConfig, module: &str) -> IamRole {
    let mut inline_policies = HashMap::new();
    inline_policies.insert("ecs-agent-access".to_owned(), ecs_agent_access_policy());
    IamRole::new(
        service_execution_role_name(service_name, config),
        IamRoleConfig {
            trust_policy: ecs_tasks_assume_role_policy(),
            inline_policies,
            dependent_inline_policies: Vec::new(),
            managed_policy_arns: Vec::new(),
            module: module.to_owned(),
        },
        &resource_context(config),
    )
}

pub(crate) fn task_definition_needs_execution_role(spec: &TaskDefinitionSpec) -> bool {
    spec.containers
        .iter()
        .any(|container| !container.secrets.is_empty() || container.image.contains(".dkr.ecr."))
}

fn service_task_role_name(service_name: &str, config: &EcsConfig) -> String {
    format!("{}-{}-task", config.project_name, service_name)
}

fn service_execution_role_name(service_name: &str, config: &EcsConfig) -> String {
    format!("{}-{}-execution", config.project_name, service_name)
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

fn ecs_agent_access_policy() -> String {
    serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "ecr:GetAuthorizationToken",
                    "ecr:BatchCheckLayerAvailability",
                    "ecr:BatchGetImage",
                    "ecr:GetDownloadUrlForLayer"
                ],
                "Resource": "*"
            },
            {
                "Effect": "Allow",
                "Action": "secretsmanager:GetSecretValue",
                "Resource": "*"
            }
        ]
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
    use proptest::prelude::*;

    fn env_var_strategy() -> impl Strategy<Value = crate::services::EnvironmentVar> {
        ("[A-Z_]{1,16}", "[a-zA-Z0-9_:/.-]{0,32}")
            .prop_map(|(name, value)| crate::services::EnvironmentVar { name, value })
    }

    fn container_with_environment(
        environment: Vec<crate::services::EnvironmentVar>,
    ) -> ContainerSpec {
        ContainerSpec {
            name: "svc".into(),
            image: "example/svc:latest".into(),
            essential: true,
            cpu: 128,
            memory_mb: 256,
            command: Vec::new(),
            port_mappings: Vec::new(),
            mount_points: Vec::new(),
            depends_on: Vec::new(),
            linux_parameters: None,
            environment,
            secrets: Vec::new(),
        }
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
        let workload = crate::services::EcsWorkload::build_all(&EcsConfig::default())
            .into_iter()
            .find(|workload| workload.name == "tokeira-runtime")
            .expect("runtime workload");
        let task_definition = to_aws_task_definition(&workload.task_definition, None, None);
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

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]

        #[test]
        fn to_aws_container_preserves_environment_variables(
            environment in proptest::collection::vec(env_var_strategy(), 0..8),
        ) {
            let spec = container_with_environment(environment.clone());

            let aws = to_aws_container(&spec);

            prop_assert_eq!(aws.environment.len(), environment.len());
            for (actual, expected) in aws.environment.iter().zip(&environment) {
                prop_assert_eq!(actual.name.as_str(), expected.name.as_str());
                prop_assert_eq!(actual.value.as_str(), expected.value.as_str());
            }
        }
    }
}
