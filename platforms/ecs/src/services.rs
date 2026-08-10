use serde::Serialize;
use tokeira_deploy_engine as deploy_engine;

use crate::config::{
    ALLOY_CONFIG_INIT_CPU, ALLOY_CONFIG_INIT_MEMORY_MB, DaemonServiceConfig, EcsConfig,
    ReplicaServiceConfig, WAIT_FOR_CPU, WAIT_FOR_MEMORY_MB,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum EcsScheduling {
    Replica { desired_count: u32 },
    Daemon,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct EcsWorkload {
    pub name: String,
    pub scheduling: EcsScheduling,
    pub capacity_provider: String,
    pub task_definition: TaskDefinitionSpec,
    pub service_connect: ServiceConnectSpec,
    pub placement_constraints: Vec<PlacementConstraint>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TaskDefinitionSpec {
    pub family: String,
    pub cpu: u32,
    pub memory_mb: u32,
    pub containers: Vec<ContainerSpec>,
    pub volumes: Vec<VolumeSpec>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ContainerSpec {
    pub name: String,
    pub image: String,
    pub essential: bool,
    pub cpu: u32,
    pub memory_mb: u32,
    pub command: Vec<String>,
    pub port_mappings: Vec<PortMappingSpec>,
    pub mount_points: Vec<MountPointSpec>,
    pub depends_on: Vec<ContainerDependencySpec>,
    pub linux_parameters: Option<LinuxParametersSpec>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub environment: Vec<EnvironmentVar>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub secrets: Vec<SecretEnvVar>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LinuxParametersSpec {
    pub init_process_enabled: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct EnvironmentVar {
    pub name: String,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SecretEnvVar {
    pub name: String,
    pub value_from: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PortMappingSpec {
    pub name: String,
    pub container_port: u16,
    pub protocol: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MountPointSpec {
    pub source_volume: String,
    pub container_path: String,
    pub read_only: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ContainerDependencySpec {
    pub container_name: String,
    pub condition: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct VolumeSpec {
    pub name: String,
    pub host_path: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ServiceConnectSpec {
    pub grpc: Option<ServiceConnectPort>,
    pub metrics: ServiceConnectPort,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ServiceConnectPort {
    pub port_name: String,
    pub container_port: u16,
    pub discovery_name: String,
    pub dns_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PlacementConstraint {
    pub r#type: String,
    pub expression: String,
}

impl EcsWorkload {
    pub fn build_all(config: &EcsConfig) -> Vec<Self> {
        vec![
            replica_workload(
                "tokeira-edge-api",
                "edge-api",
                "cp-edge-api",
                &config.services.edge_api,
                &["tokeira-runtime", "tokeira-controller"],
                config,
            ),
            replica_workload(
                "tokeira-edge-poll",
                "edge-poll",
                "cp-edge-poll",
                &config.services.edge_poll,
                &["tokeira-runtime", "tokeira-controller"],
                config,
            ),
            daemon_workload(
                "tokeira-runtime",
                "runtime",
                "cp-runtime",
                &config.services.runtime,
                &["tokeira-controller"],
                config,
            ),
            replica_workload(
                "tokeira-projection",
                "projection",
                "cp-projection",
                &config.services.projection,
                &["tokeira-runtime"],
                config,
            ),
            replica_workload(
                "tokeira-controller",
                "control",
                "cp-control",
                &config.services.controller,
                &[],
                config,
            ),
            replica_workload(
                "tokeira-autoscaler",
                "control",
                "cp-control",
                &config.services.autoscaler,
                &["tokeira-controller", "tokeira-mimir"],
                config,
            ),
            replica_workload(
                "tokeira-admin",
                "control",
                "cp-control",
                &config.services.admin,
                &[],
                config,
            ),
        ]
    }

    pub fn build_observability(config: &EcsConfig) -> Vec<Self> {
        vec![
            observability_workload(
                "tokeira-mimir",
                "mimir",
                "cp-mimir",
                config.observability.mimir_image.clone(),
                config.observability.mimir_cpu,
                config.observability.mimir_memory_mb,
                Some(9009),
                9009,
                &[],
                config,
            ),
            observability_workload(
                "tokeira-loki",
                "loki",
                "cp-loki",
                config.observability.loki_image.clone(),
                config.observability.loki_cpu,
                config.observability.loki_memory_mb,
                Some(3100),
                3100,
                &[],
                config,
            ),
            observability_workload(
                "tokeira-grafana",
                "grafana",
                "cp-grafana",
                config.observability.grafana_image.clone(),
                config.observability.grafana_cpu,
                config.observability.grafana_memory_mb,
                None,
                3000,
                &["tokeira-mimir", "tokeira-loki"],
                config,
            ),
        ]
    }
}

impl deploy_engine::Service for EcsWorkload {
    fn resource_type(&self) -> &'static str {
        "EcsWorkload"
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn module(&self) -> &str {
        "services"
    }

    fn dependencies(&self) -> Vec<&str> {
        match self.name.as_str() {
            "tokeira-edge-api" | "tokeira-edge-poll" => {
                vec!["tokeira-runtime", "tokeira-controller"]
            }
            "tokeira-runtime" => vec!["tokeira-controller"],
            "tokeira-projection" => vec!["tokeira-runtime"],
            "tokeira-autoscaler" => vec!["tokeira-controller", "tokeira-mimir"],
            _ => Vec::new(),
        }
    }

    fn manifests(
        &self,
        _ctx: &deploy_engine::ServiceContext,
    ) -> Result<Vec<serde_json::Value>, deploy_engine::DeployError> {
        let task_definition = serde_json::json!({
            "kind": "ecs-task-definition",
            "service": self.name,
            "spec": self.task_definition,
        });
        let service = serde_json::json!({
            "kind": "ecs-service",
            "service": self.name,
            "scheduling": self.scheduling,
            "capacity_provider": self.capacity_provider,
            "service_connect": self.service_connect,
            "placement_constraints": self.placement_constraints,
            "enable_execute_command": true,
        });
        Ok(vec![task_definition, service])
    }
}

fn replica_workload(
    name: &str,
    workload: &str,
    capacity_provider: &str,
    service: &ReplicaServiceConfig,
    dependencies: &[&str],
    config: &EcsConfig,
) -> EcsWorkload {
    workload_from_parts(
        name,
        workload,
        capacity_provider,
        EcsScheduling::Replica {
            desired_count: service.desired_count,
        },
        service.image.clone(),
        service.cpu,
        service.memory_mb,
        service.grpc_port,
        service.metrics_port,
        dependencies,
        config,
    )
}

fn daemon_workload(
    name: &str,
    workload: &str,
    capacity_provider: &str,
    service: &DaemonServiceConfig,
    dependencies: &[&str],
    config: &EcsConfig,
) -> EcsWorkload {
    workload_from_parts(
        name,
        workload,
        capacity_provider,
        EcsScheduling::Daemon,
        service.image.clone(),
        service.cpu,
        service.memory_mb,
        Some(service.grpc_port),
        service.metrics_port,
        dependencies,
        config,
    )
}

fn observability_workload(
    name: &str,
    workload: &str,
    capacity_provider: &str,
    image: String,
    cpu: u32,
    memory_mb: u32,
    grpc_port: Option<u16>,
    metrics_port: u16,
    dependencies: &[&str],
    config: &EcsConfig,
) -> EcsWorkload {
    let mut workload = workload_from_parts(
        name,
        workload,
        capacity_provider,
        EcsScheduling::Replica { desired_count: 1 },
        image,
        cpu,
        memory_mb,
        grpc_port,
        metrics_port,
        dependencies,
        config,
    );
    if name == "tokeira-grafana"
        && let Some(primary) = workload
            .task_definition
            .containers
            .iter_mut()
            .find(|container| container.name == "tokeira-grafana")
    {
        primary.secrets.push(SecretEnvVar {
            name: "GRAFANA_ADMIN_PASSWORD".into(),
            // ECS resolves the JSON key at task start; neither the generated
            // password nor a resolved ARN is written into config or manifests.
            value_from: format!("{}/grafana/admin:password::", config.project_name),
        });
    }
    workload
}

fn workload_from_parts(
    name: &str,
    workload: &str,
    capacity_provider: &str,
    scheduling: EcsScheduling,
    image: String,
    cpu: u32,
    memory_mb: u32,
    grpc_port: Option<u16>,
    metrics_port: u16,
    dependencies: &[&str],
    config: &EcsConfig,
) -> EcsWorkload {
    let wait_for = wait_for_containers(dependencies, config);
    let primary_depends_on = wait_for
        .iter()
        .map(|container| ContainerDependencySpec {
            container_name: container.name.clone(),
            condition: "SUCCESS".into(),
        })
        .collect();
    let (alloy_init, alloy) = alloy_containers(name, metrics_port, config);
    let sidecar_cpu = config.observability.alloy_cpu + alloy_init.cpu;
    let sidecar_memory = config.observability.alloy_memory_mb + alloy_init.memory_mb;
    let wait_cpu = wait_for.iter().map(|container| container.cpu).sum::<u32>();
    let wait_memory = wait_for
        .iter()
        .map(|container| container.memory_mb)
        .sum::<u32>();
    // ECS EC2 tasks reserve at the task-definition level. Deducting sidecar and
    // init-container reservations keeps the primary container from silently
    // overcommitting the capacity the operator configured for the service.
    let primary = primary_container(
        name,
        image,
        cpu - (sidecar_cpu + wait_cpu),
        memory_mb - (sidecar_memory + wait_memory),
        grpc_port,
        metrics_port,
        primary_depends_on,
        config,
    );
    let mut containers = wait_for;
    containers.push(alloy_init);
    containers.push(primary);
    containers.push(alloy);

    EcsWorkload {
        name: name.to_owned(),
        scheduling,
        capacity_provider: capacity_provider.to_owned(),
        task_definition: TaskDefinitionSpec {
            family: name.to_owned(),
            cpu,
            memory_mb,
            containers,
            volumes: vec![
                VolumeSpec {
                    name: "alloy-config".into(),
                    host_path: None,
                },
                // The ECS-on-EC2 deployment intentionally keeps primary
                // containers on Docker's json-file driver; Alloy scopes Docker
                // log discovery to the current task ARN through this socket.
                VolumeSpec {
                    name: "docker-sock".into(),
                    host_path: Some("/var/run/docker.sock".into()),
                },
            ],
        },
        service_connect: ServiceConnectSpec {
            grpc: grpc_port.map(|port| service_connect_port("grpc", name, port)),
            metrics: service_connect_port("metrics", &format!("{name}-metrics"), metrics_port),
        },
        placement_constraints: vec![PlacementConstraint {
            r#type: "memberOf".into(),
            expression: format!("attribute:workload == {workload}"),
        }],
    }
}

fn primary_container(
    name: &str,
    image: String,
    cpu: u32,
    memory_mb: u32,
    grpc_port: Option<u16>,
    metrics_port: u16,
    depends_on: Vec<ContainerDependencySpec>,
    config: &EcsConfig,
) -> ContainerSpec {
    let mut port_mappings = vec![PortMappingSpec {
        name: "metrics".into(),
        container_port: metrics_port,
        protocol: "tcp".into(),
    }];
    if let Some(grpc_port) = grpc_port {
        port_mappings.push(PortMappingSpec {
            name: "grpc".into(),
            container_port: grpc_port,
            protocol: "tcp".into(),
        });
    }
    ContainerSpec {
        name: name.to_owned(),
        image,
        essential: true,
        cpu,
        memory_mb,
        command: Vec::new(),
        port_mappings,
        mount_points: Vec::new(),
        depends_on,
        linux_parameters: Some(LinuxParametersSpec {
            init_process_enabled: true,
        }),
        environment: observability_environment(name, metrics_port, config),
        secrets: Vec::new(),
    }
}

fn observability_environment(
    service_name: &str,
    metrics_port: u16,
    config: &EcsConfig,
) -> Vec<EnvironmentVar> {
    vec![
        EnvironmentVar {
            name: "TOKEIRA_OBSERVABILITY_SERVICE".into(),
            value: service_name.to_owned(),
        },
        EnvironmentVar {
            name: "TOKEIRA_OBSERVABILITY_CLUSTER".into(),
            value: config.cluster.name.clone(),
        },
        EnvironmentVar {
            name: "TOKEIRA_OBSERVABILITY_DEPLOYMENT".into(),
            value: config.environment.clone(),
        },
        EnvironmentVar {
            name: "TOKEIRA_OBSERVABILITY_METRICS_ADDR".into(),
            value: format!("0.0.0.0:{metrics_port}"),
        },
        EnvironmentVar {
            name: "TOKEIRA_OBSERVABILITY_LOG_FORMAT".into(),
            value: "json".into(),
        },
    ]
}

fn wait_for_containers(dependencies: &[&str], config: &EcsConfig) -> Vec<ContainerSpec> {
    dependencies
        .iter()
        .map(|dependency| {
            let port = dependency_port(dependency);
            ContainerSpec {
                name: format!("wait-for-{dependency}"),
                image: config.observability.busybox_image.clone(),
                essential: false,
                cpu: WAIT_FOR_CPU,
                memory_mb: WAIT_FOR_MEMORY_MB,
                command: vec![
                    "sh".into(),
                    "-c".into(),
                    format!(
                        "until nc -z {}.{} {}; do sleep 2; done",
                        dependency, config.networking.private_dns_zone, port
                    ),
                ],
                port_mappings: Vec::new(),
                mount_points: Vec::new(),
                depends_on: Vec::new(),
                linux_parameters: None,
                environment: Vec::new(),
                secrets: Vec::new(),
            }
        })
        .collect()
}

fn alloy_containers(
    service_name: &str,
    metrics_port: u16,
    config: &EcsConfig,
) -> (ContainerSpec, ContainerSpec) {
    let param_path = format!("/{}/alloy/sidecar/{service_name}", config.project_name);
    let init = ContainerSpec {
        name: "alloy-config-init".into(),
        image: config.observability.aws_cli_image.clone(),
        essential: false,
        cpu: ALLOY_CONFIG_INIT_CPU,
        memory_mb: ALLOY_CONFIG_INIT_MEMORY_MB,
        command: vec![
            "sh".into(),
            "-c".into(),
            format!(
                // The rendered Alloy template contains placeholders because
                // the task ARN is only available from ECS metadata at runtime.
                "TASK_ARN=$(curl -s \"$ECS_CONTAINER_METADATA_URI_V4/task\" | grep -o '\"TaskARN\":\"[^\"]*' | cut -d'\"' -f4) && \
                 TASK_ID=$(echo \"$TASK_ARN\" | grep -oE '[^/]+$') && \
                 aws ssm get-parameter --name {param_path} --with-decryption --region {} --query 'Parameter.Value' --output text | \
                 sed \"s|TASK_ARN_PLACEHOLDER|$TASK_ARN|g\" | \
                 sed \"s|TASK_ID_PLACEHOLDER|$TASK_ID|g\" > /etc/alloy/config.alloy",
                config.region
            ),
        ],
        port_mappings: Vec::new(),
        mount_points: vec![MountPointSpec {
            source_volume: "alloy-config".into(),
            container_path: "/etc/alloy".into(),
            read_only: false,
        }],
        depends_on: Vec::new(),
        linux_parameters: None,
        environment: Vec::new(),
        secrets: Vec::new(),
    };
    let alloy = ContainerSpec {
        name: "alloy".into(),
        image: config.observability.alloy_image.clone(),
        essential: false,
        cpu: config.observability.alloy_cpu,
        memory_mb: config.observability.alloy_memory_mb,
        command: vec![
            "run".into(),
            "--server.http.listen-addr=0.0.0.0:12345".into(),
            format!("--stability.level=generally-available"),
            format!("--storage.path=/tmp/alloy-{metrics_port}"),
            "/etc/alloy/config.alloy".into(),
        ],
        port_mappings: Vec::new(),
        mount_points: vec![
            MountPointSpec {
                source_volume: "alloy-config".into(),
                container_path: "/etc/alloy".into(),
                read_only: true,
            },
            MountPointSpec {
                source_volume: "docker-sock".into(),
                container_path: "/var/run/docker.sock".into(),
                read_only: true,
            },
        ],
        depends_on: vec![ContainerDependencySpec {
            container_name: "alloy-config-init".into(),
            condition: "SUCCESS".into(),
        }],
        linux_parameters: None,
        environment: Vec::new(),
        secrets: Vec::new(),
    };
    (init, alloy)
}

fn service_connect_port(port_name: &str, discovery_name: &str, port: u16) -> ServiceConnectPort {
    ServiceConnectPort {
        port_name: port_name.into(),
        container_port: port,
        discovery_name: discovery_name.into(),
        dns_name: discovery_name.into(),
    }
}

fn dependency_port(dependency: &str) -> u16 {
    match dependency {
        "tokeira-runtime" => 7241,
        "tokeira-controller" => 7240,
        "tokeira-mimir" => 9009,
        "tokeira-loki" => 3100,
        _ => 9090,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use super::*;
    use tokeira_deploy_engine::Service;

    #[test]
    fn build_all_generates_seven_tokeira_workloads() {
        let workloads = EcsWorkload::build_all(&EcsConfig::default());
        let names: Vec<&str> = workloads.iter().map(|workload| workload.name()).collect();

        assert_eq!(names.len(), 7);
        assert!(names.contains(&"tokeira-edge-api"));
        assert!(names.contains(&"tokeira-runtime"));
        assert!(names.contains(&"tokeira-admin"));
    }

    #[test]
    fn scheduling_and_capacity_provider_assignments_match_design() {
        let workloads = EcsWorkload::build_all(&EcsConfig::default());
        let runtime = workloads
            .iter()
            .find(|workload| workload.name == "tokeira-runtime")
            .expect("runtime workload");
        let edge_api = workloads
            .iter()
            .find(|workload| workload.name == "tokeira-edge-api")
            .expect("edge api workload");

        assert_eq!(runtime.scheduling, EcsScheduling::Daemon);
        assert_eq!(runtime.capacity_provider, "cp-runtime");
        assert_eq!(
            edge_api.scheduling,
            EcsScheduling::Replica { desired_count: 2 }
        );
        assert_eq!(edge_api.capacity_provider, "cp-edge-api");
    }

    #[test]
    fn every_workload_registers_metrics_service_connect_alias() {
        for workload in EcsWorkload::build_all(&EcsConfig::default()) {
            assert_eq!(workload.service_connect.metrics.port_name, "metrics");
            assert_eq!(workload.service_connect.metrics.container_port, 9090);
            assert_eq!(
                workload.service_connect.metrics.discovery_name,
                format!("{}-metrics", workload.name)
            );
        }
    }

    #[test]
    fn primary_container_enables_init_process_and_omits_log_configuration() {
        let workload = EcsWorkload::build_all(&EcsConfig::default())
            .into_iter()
            .find(|workload| workload.name == "tokeira-edge-api")
            .expect("edge api workload");
        let primary = workload
            .task_definition
            .containers
            .iter()
            .find(|container| container.name == "tokeira-edge-api")
            .expect("primary container");
        let manifest = serde_json::to_value(primary).expect("primary serializes");

        assert_eq!(
            primary.linux_parameters,
            Some(LinuxParametersSpec {
                init_process_enabled: true
            })
        );
        assert!(manifest.get("logConfiguration").is_none());
    }

    #[test]
    fn primary_container_includes_observability_environment() {
        let workload = EcsWorkload::build_all(&EcsConfig::default())
            .into_iter()
            .find(|workload| workload.name == "tokeira-runtime")
            .expect("runtime workload");
        let primary = workload
            .task_definition
            .containers
            .iter()
            .find(|container| container.name == "tokeira-runtime")
            .expect("primary container");
        let env: HashMap<&str, &str> = primary
            .environment
            .iter()
            .map(|var| (var.name.as_str(), var.value.as_str()))
            .collect();

        assert_eq!(
            env.get("TOKEIRA_OBSERVABILITY_SERVICE"),
            Some(&"tokeira-runtime")
        );
        assert_eq!(
            env.get("TOKEIRA_OBSERVABILITY_METRICS_ADDR"),
            Some(&"0.0.0.0:9090")
        );
        assert_eq!(env.get("TOKEIRA_OBSERVABILITY_LOG_FORMAT"), Some(&"json"));
    }

    #[test]
    fn primary_resources_subtract_sidecar_and_wait_overhead() {
        let config = EcsConfig::default();
        config.validate().expect("validated config");
        let workload = EcsWorkload::build_all(&config)
            .into_iter()
            .find(|workload| workload.name == "tokeira-edge-api")
            .expect("edge api workload");
        let primary = workload
            .task_definition
            .containers
            .iter()
            .find(|container| container.name == "tokeira-edge-api")
            .expect("primary container");

        assert_eq!(
            primary.cpu,
            config.services.edge_api.cpu
                - (config.observability.alloy_cpu + ALLOY_CONFIG_INIT_CPU + 2 * WAIT_FOR_CPU)
        );
        assert_eq!(
            primary.memory_mb,
            config.services.edge_api.memory_mb
                - (config.observability.alloy_memory_mb
                    + ALLOY_CONFIG_INIT_MEMORY_MB
                    + 2 * WAIT_FOR_MEMORY_MB)
        );
    }

    #[test]
    fn edge_api_includes_wait_for_runtime_and_controller_init_containers() {
        let workload = EcsWorkload::build_all(&EcsConfig::default())
            .into_iter()
            .find(|workload| workload.name == "tokeira-edge-api")
            .expect("edge api workload");
        let wait_names: HashSet<&str> = workload
            .task_definition
            .containers
            .iter()
            .filter(|container| container.name.starts_with("wait-for-"))
            .map(|container| container.name.as_str())
            .collect();

        assert!(wait_names.contains("wait-for-tokeira-runtime"));
        assert!(wait_names.contains("wait-for-tokeira-controller"));
    }

    #[test]
    fn alloy_sidecar_mounts_config_and_docker_socket_volumes() {
        let workload = EcsWorkload::build_all(&EcsConfig::default())
            .into_iter()
            .find(|workload| workload.name == "tokeira-runtime")
            .expect("runtime workload");
        let volumes: HashSet<&str> = workload
            .task_definition
            .volumes
            .iter()
            .map(|volume| volume.name.as_str())
            .collect();
        let alloy = workload
            .task_definition
            .containers
            .iter()
            .find(|container| container.name == "alloy")
            .expect("alloy sidecar");

        assert!(volumes.contains("alloy-config"));
        assert!(volumes.contains("docker-sock"));
        assert!(
            alloy
                .mount_points
                .iter()
                .any(|mount| mount.container_path == "/var/run/docker.sock")
        );
    }

    #[test]
    fn grafana_primary_container_receives_admin_password_from_secrets_manager() {
        let workload = EcsWorkload::build_observability(&EcsConfig::default())
            .into_iter()
            .find(|workload| workload.name == "tokeira-grafana")
            .expect("grafana workload");
        let primary = workload
            .task_definition
            .containers
            .iter()
            .find(|container| container.name == "tokeira-grafana")
            .expect("grafana primary container");

        assert_eq!(
            primary.secrets,
            vec![SecretEnvVar {
                name: "GRAFANA_ADMIN_PASSWORD".to_owned(),
                value_from: "tokeira/grafana/admin:password::".to_owned(),
            }]
        );
    }

    #[test]
    fn service_manifests_are_stable_and_enable_exec() {
        let workload = EcsWorkload::build_all(&EcsConfig::default())
            .into_iter()
            .find(|workload| workload.name == "tokeira-runtime")
            .expect("runtime workload");
        let ctx = deploy_engine::ServiceContext::default();
        let first = workload.manifests(&ctx).expect("first manifests");
        let second = workload.manifests(&ctx).expect("second manifests");

        assert_eq!(first, second);
        assert_eq!(
            second[1]
                .get("enable_execute_command")
                .and_then(|v| v.as_bool()),
            Some(true)
        );
    }

    #[test]
    fn service_dependency_graph_is_acyclic_for_generated_services() {
        let workloads = EcsWorkload::build_all(&EcsConfig::default());
        let names: HashSet<&str> = workloads.iter().map(|workload| workload.name()).collect();
        let deps: HashMap<&str, Vec<&str>> = workloads
            .iter()
            .map(|workload| {
                (
                    workload.name(),
                    workload
                        .dependencies()
                        .iter()
                        .copied()
                        .filter(|dep| names.contains(dep))
                        .collect(),
                )
            })
            .collect();

        for service in &names {
            assert!(!has_cycle(service, service, &deps, &mut HashSet::new()));
        }
    }

    fn has_cycle<'a>(
        start: &'a str,
        current: &'a str,
        deps: &HashMap<&'a str, Vec<&'a str>>,
        seen: &mut HashSet<&'a str>,
    ) -> bool {
        if !seen.insert(current) {
            return false;
        }
        for dep in deps.get(current).into_iter().flatten() {
            if *dep == start || has_cycle(start, dep, deps, seen) {
                return true;
            }
        }
        false
    }
}
