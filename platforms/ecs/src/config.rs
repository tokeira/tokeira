use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum EcsConfigError {
    #[error("networking.availability_zones must contain at least one Availability Zone")]
    EmptySubnets,
    #[error("networking.vpc_cidr must not be empty")]
    EmptyVpcCidr,
    #[error("alb.certificate_arn is required when alb.listener_protocol = \"https\"")]
    MissingCertificateArn,
    #[error("dsql preexisting mode requires {0}")]
    MissingPreexistingDsqlField(&'static str),
    #[error("service {service} must use canonical {field} port {expected}, got {actual}")]
    InvalidCanonicalPort {
        service: &'static str,
        field: &'static str,
        expected: u16,
        actual: u16,
    },
    #[error("service {service} has invalid ECS cpu/memory pair ({cpu}, {memory_mb} MiB); {hint}")]
    InvalidCpuMemory {
        service: &'static str,
        cpu: u32,
        memory_mb: u32,
        hint: String,
    },
    #[error("capacity provider {provider} must have max_capacity >= min_capacity")]
    InvalidCapacityRange { provider: &'static str },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EcsConfig {
    pub project_name: String,
    pub environment: String,
    pub region: String,
    pub tags: HashMap<String, String>,
    pub cluster: ClusterConfig,
    pub networking: NetworkingConfig,
    pub capacity_providers: CapacityProviderConfigs,
    pub services: ServiceConfigs,
    pub autoscaler: AutoscalerConfig,
    pub alb: AlbConfig,
    pub dsql: DsqlConfig,
    pub observability: ObservabilityConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterConfig {
    pub name: String,
    pub service_connect_namespace: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NetworkingConfig {
    pub vpc_cidr: String,
    pub availability_zones: Vec<String>,
    pub private_dns_zone: String,
    pub optional_endpoints: OptionalEndpoints,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OptionalEndpoints {
    pub sts: bool,
    pub kms: bool,
    pub secrets_manager: bool,
    pub cloudwatch_logs: bool,
    pub ec2: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapacityProviderConfigs {
    pub edge_api: CapacityProviderConfig,
    pub edge_poll: CapacityProviderConfig,
    pub runtime: RuntimeCapacityProviderConfig,
    pub projection: CapacityProviderConfig,
    pub control: CapacityProviderConfig,
    pub mimir: CapacityProviderConfig,
    pub loki: CapacityProviderConfig,
    pub grafana: CapacityProviderConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapacityProviderConfig {
    pub instance_type: String,
    pub min_capacity: u32,
    pub desired_capacity: u32,
    pub max_capacity: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeCapacityProviderConfig {
    pub instance_type: String,
    pub min_capacity: u32,
    pub desired_capacity: u32,
    pub max_capacity: u32,
    pub scale_in_protection: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceConfigs {
    pub edge_api: ReplicaServiceConfig,
    pub edge_poll: ReplicaServiceConfig,
    pub runtime: DaemonServiceConfig,
    pub projection: ReplicaServiceConfig,
    pub controller: ReplicaServiceConfig,
    pub autoscaler: ReplicaServiceConfig,
    pub admin: ReplicaServiceConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplicaServiceConfig {
    pub image: String,
    pub desired_count: u32,
    pub cpu: u32,
    pub memory_mb: u32,
    pub grpc_port: Option<u16>,
    pub metrics_port: u16,
    pub http_port: Option<u16>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DaemonServiceConfig {
    pub image: String,
    pub cpu: u32,
    pub memory_mb: u32,
    pub grpc_port: u16,
    pub metrics_port: u16,
    pub http_port: Option<u16>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AutoscalerConfig {
    pub image: String,
    pub polling_interval_secs: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AlbConfig {
    pub name: String,
    pub listener_protocol: AlbListenerProtocol,
    pub certificate_arn: Option<String>,
    pub health_check_path: String,
    pub health_check_interval_secs: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AlbListenerProtocol {
    Http2,
    Https,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DsqlConfig {
    pub mode: DsqlClusterMode,
    pub endpoint: Option<String>,
    pub management_endpoint_id: Option<String>,
    pub connection_endpoint_id: Option<String>,
    pub runtime_role_arn: Option<String>,
    pub admin_role_arn: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DsqlClusterMode {
    Managed,
    Preexisting,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObservabilityConfig {
    #[serde(default = "default_mimir_image")]
    pub mimir_image: String,
    pub mimir_cpu: u32,
    pub mimir_memory_mb: u32,
    #[serde(default = "default_loki_image")]
    pub loki_image: String,
    pub loki_cpu: u32,
    pub loki_memory_mb: u32,
    #[serde(default = "default_grafana_image")]
    pub grafana_image: String,
    pub grafana_cpu: u32,
    pub grafana_memory_mb: u32,
    #[serde(default = "default_alloy_image")]
    pub alloy_image: String,
    pub alloy_cpu: u32,
    pub alloy_memory_mb: u32,
    #[serde(default = "default_aws_cli_image")]
    pub aws_cli_image: String,
    #[serde(default = "default_busybox_image")]
    pub busybox_image: String,
    pub retention_days: u32,
    /// Loki query endpoint for `tkr logs`. Defaults to localhost:3100
    /// (reachable when `tkr port-forward loki` is active).
    #[serde(default = "default_loki_query_url")]
    pub loki_query_url: String,
}

impl EcsConfig {
    pub fn validate(&self) -> Result<(), EcsConfigError> {
        if self.networking.vpc_cidr.is_empty() {
            return Err(EcsConfigError::EmptyVpcCidr);
        }
        if self.networking.availability_zones.is_empty() {
            return Err(EcsConfigError::EmptySubnets);
        }
        if self.alb.listener_protocol == AlbListenerProtocol::Https
            && self.alb.certificate_arn.as_deref().unwrap_or("").is_empty()
        {
            return Err(EcsConfigError::MissingCertificateArn);
        }
        if self.dsql.mode == DsqlClusterMode::Preexisting {
            require_preexisting("dsql.endpoint", &self.dsql.endpoint)?;
            require_preexisting(
                "dsql.management_endpoint_id",
                &self.dsql.management_endpoint_id,
            )?;
            require_preexisting(
                "dsql.connection_endpoint_id",
                &self.dsql.connection_endpoint_id,
            )?;
            require_preexisting("dsql.runtime_role_arn", &self.dsql.runtime_role_arn)?;
            require_preexisting("dsql.admin_role_arn", &self.dsql.admin_role_arn)?;
        }
        validate_service_ports(self)?;
        validate_capacity("cp-edge-api", &self.capacity_providers.edge_api)?;
        validate_capacity("cp-edge-poll", &self.capacity_providers.edge_poll)?;
        validate_runtime_capacity(&self.capacity_providers.runtime)?;
        validate_capacity("cp-projection", &self.capacity_providers.projection)?;
        validate_capacity("cp-control", &self.capacity_providers.control)?;
        validate_capacity("cp-mimir", &self.capacity_providers.mimir)?;
        validate_capacity("cp-loki", &self.capacity_providers.loki)?;
        validate_capacity("cp-grafana", &self.capacity_providers.grafana)?;
        validate_cpu_memory(
            "edge-api",
            self.services.edge_api.cpu,
            self.services.edge_api.memory_mb,
        )?;
        validate_cpu_memory(
            "edge-poll",
            self.services.edge_poll.cpu,
            self.services.edge_poll.memory_mb,
        )?;
        validate_cpu_memory(
            "runtime",
            self.services.runtime.cpu,
            self.services.runtime.memory_mb,
        )?;
        validate_cpu_memory(
            "projection",
            self.services.projection.cpu,
            self.services.projection.memory_mb,
        )?;
        validate_cpu_memory(
            "controller",
            self.services.controller.cpu,
            self.services.controller.memory_mb,
        )?;
        validate_cpu_memory(
            "autoscaler",
            self.services.autoscaler.cpu,
            self.services.autoscaler.memory_mb,
        )?;
        validate_cpu_memory(
            "admin",
            self.services.admin.cpu,
            self.services.admin.memory_mb,
        )?;
        validate_cpu_memory(
            "mimir",
            self.observability.mimir_cpu,
            self.observability.mimir_memory_mb,
        )?;
        validate_cpu_memory(
            "loki",
            self.observability.loki_cpu,
            self.observability.loki_memory_mb,
        )?;
        validate_cpu_memory(
            "grafana",
            self.observability.grafana_cpu,
            self.observability.grafana_memory_mb,
        )?;
        Ok(())
    }
}

impl Default for EcsConfig {
    fn default() -> Self {
        Self {
            project_name: "tokeira".into(),
            environment: "dev".into(),
            region: "us-east-1".into(),
            tags: HashMap::new(),
            cluster: ClusterConfig::default(),
            networking: NetworkingConfig::default(),
            capacity_providers: CapacityProviderConfigs::default(),
            services: ServiceConfigs::default(),
            autoscaler: AutoscalerConfig::default(),
            alb: AlbConfig::default(),
            dsql: DsqlConfig::default(),
            observability: ObservabilityConfig::default(),
        }
    }
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            name: "tokeira".into(),
            service_connect_namespace: "tokeira.local".into(),
        }
    }
}

impl Default for NetworkingConfig {
    fn default() -> Self {
        Self {
            vpc_cidr: "10.0.0.0/16".into(),
            availability_zones: vec!["us-east-1a".into(), "us-east-1b".into()],
            private_dns_zone: "tokeira.local".into(),
            optional_endpoints: OptionalEndpoints::default(),
        }
    }
}

impl Default for CapacityProviderConfigs {
    fn default() -> Self {
        Self {
            edge_api: CapacityProviderConfig::new("c7g.large", 1, 2, 8),
            edge_poll: CapacityProviderConfig::new("c7g.large", 1, 2, 8),
            runtime: RuntimeCapacityProviderConfig {
                instance_type: "c7g.large".into(),
                min_capacity: 1,
                desired_capacity: 3,
                max_capacity: 16,
                scale_in_protection: true,
            },
            projection: CapacityProviderConfig::new("c7g.large", 1, 1, 8),
            control: CapacityProviderConfig::new("c7g.large", 1, 1, 3),
            mimir: CapacityProviderConfig::new("m7g.large", 1, 1, 1),
            loki: CapacityProviderConfig::new("m7g.large", 1, 1, 1),
            grafana: CapacityProviderConfig::new("t4g.medium", 1, 1, 1),
        }
    }
}

impl CapacityProviderConfig {
    fn new(
        instance_type: &str,
        min_capacity: u32,
        desired_capacity: u32,
        max_capacity: u32,
    ) -> Self {
        Self {
            instance_type: instance_type.into(),
            min_capacity,
            desired_capacity,
            max_capacity,
        }
    }
}

impl Default for ServiceConfigs {
    fn default() -> Self {
        Self {
            edge_api: ReplicaServiceConfig::new("tokeirad:latest", 2, Some(7233), 9090, None),
            edge_poll: ReplicaServiceConfig::new("tokeirad:latest", 2, Some(7234), 9090, None),
            runtime: DaemonServiceConfig::new("tokeirad:latest", 7241, 9090, None),
            projection: ReplicaServiceConfig::new("tokeirad:latest", 1, Some(7242), 9090, None),
            controller: ReplicaServiceConfig::new("tokeirad:latest", 1, Some(7240), 9090, None),
            autoscaler: ReplicaServiceConfig::new("tokeirad:latest", 1, Some(7243), 9090, None),
            admin: ReplicaServiceConfig::new("tokeirad:latest", 0, Some(7244), 9090, None),
        }
    }
}

impl ReplicaServiceConfig {
    fn new(
        image: &str,
        desired_count: u32,
        grpc_port: Option<u16>,
        metrics_port: u16,
        http_port: Option<u16>,
    ) -> Self {
        Self {
            image: image.into(),
            desired_count,
            cpu: 512,
            memory_mb: 1024,
            grpc_port,
            metrics_port,
            http_port,
        }
    }
}

impl DaemonServiceConfig {
    fn new(image: &str, grpc_port: u16, metrics_port: u16, http_port: Option<u16>) -> Self {
        Self {
            image: image.into(),
            cpu: 1024,
            memory_mb: 2048,
            grpc_port,
            metrics_port,
            http_port,
        }
    }
}

impl Default for AutoscalerConfig {
    fn default() -> Self {
        Self {
            image: "tokeirad:latest".into(),
            polling_interval_secs: 15,
        }
    }
}

impl Default for AlbConfig {
    fn default() -> Self {
        Self {
            name: "tokeira-internal".into(),
            listener_protocol: AlbListenerProtocol::Http2,
            certificate_arn: None,
            health_check_path: "/healthz".into(),
            health_check_interval_secs: 15,
        }
    }
}

impl Default for DsqlConfig {
    fn default() -> Self {
        Self {
            mode: DsqlClusterMode::Managed,
            endpoint: None,
            management_endpoint_id: None,
            connection_endpoint_id: None,
            runtime_role_arn: None,
            admin_role_arn: None,
        }
    }
}

impl Default for ObservabilityConfig {
    fn default() -> Self {
        Self {
            mimir_image: default_mimir_image(),
            mimir_cpu: 1024,
            mimir_memory_mb: 2048,
            loki_image: default_loki_image(),
            loki_cpu: 1024,
            loki_memory_mb: 2048,
            grafana_image: default_grafana_image(),
            grafana_cpu: 512,
            grafana_memory_mb: 1024,
            alloy_image: default_alloy_image(),
            alloy_cpu: 128,
            alloy_memory_mb: 256,
            aws_cli_image: default_aws_cli_image(),
            busybox_image: default_busybox_image(),
            retention_days: 30,
            loki_query_url: default_loki_query_url(),
        }
    }
}

pub fn required_vpc_endpoints(region: &str) -> Vec<String> {
    [
        "ecs",
        "ecs-agent",
        "ecs-telemetry",
        "ecr.api",
        "ecr.dkr",
        "s3",
        "autoscaling",
        "servicediscovery",
        "ssm",
        "ssmmessages",
        "ec2messages",
        "dsql",
        "dsql-control",
    ]
    .into_iter()
    .map(|service| match service {
        "s3" => format!("com.amazonaws.{region}.s3"),
        other => format!("com.amazonaws.{region}.{other}"),
    })
    .collect()
}

pub fn optional_vpc_endpoints(config: &EcsConfig) -> Vec<String> {
    let mut endpoints = Vec::new();
    if config.networking.optional_endpoints.sts {
        endpoints.push(format!("com.amazonaws.{}.sts", config.region));
    }
    if config.networking.optional_endpoints.kms {
        endpoints.push(format!("com.amazonaws.{}.kms", config.region));
    }
    if config.networking.optional_endpoints.secrets_manager {
        endpoints.push(format!("com.amazonaws.{}.secretsmanager", config.region));
    }
    if config.networking.optional_endpoints.cloudwatch_logs {
        endpoints.push(format!("com.amazonaws.{}.logs", config.region));
    }
    if config.networking.optional_endpoints.ec2 {
        endpoints.push(format!("com.amazonaws.{}.ec2", config.region));
    }
    endpoints
}

pub fn validate_cpu_memory(
    service: &'static str,
    cpu: u32,
    memory_mb: u32,
) -> Result<(), EcsConfigError> {
    let valid = match cpu {
        256 => (512..=2048).step_by(512).any(|m| m == memory_mb),
        512 => (1024..=4096).step_by(1024).any(|m| m == memory_mb),
        1024 => (2048..=8192).step_by(1024).any(|m| m == memory_mb),
        2048 => (4096..=16384).step_by(1024).any(|m| m == memory_mb),
        4096 => (8192..=30720).step_by(1024).any(|m| m == memory_mb),
        _ => false,
    };
    if valid {
        Ok(())
    } else {
        Err(EcsConfigError::InvalidCpuMemory {
            service,
            cpu,
            memory_mb,
            hint: "use one of: 512/1024, 1024/2048, 2048/4096, 4096/8192".into(),
        })
    }
}

fn require_preexisting(field: &'static str, value: &Option<String>) -> Result<(), EcsConfigError> {
    if value.as_deref().unwrap_or("").is_empty() {
        Err(EcsConfigError::MissingPreexistingDsqlField(field))
    } else {
        Ok(())
    }
}

fn validate_capacity(
    provider: &'static str,
    config: &CapacityProviderConfig,
) -> Result<(), EcsConfigError> {
    if config.max_capacity < config.min_capacity || config.desired_capacity > config.max_capacity {
        Err(EcsConfigError::InvalidCapacityRange { provider })
    } else {
        Ok(())
    }
}

fn validate_runtime_capacity(config: &RuntimeCapacityProviderConfig) -> Result<(), EcsConfigError> {
    if config.max_capacity < config.min_capacity || config.desired_capacity > config.max_capacity {
        Err(EcsConfigError::InvalidCapacityRange {
            provider: "cp-runtime",
        })
    } else {
        Ok(())
    }
}

fn validate_service_ports(config: &EcsConfig) -> Result<(), EcsConfigError> {
    expect_port(
        "edge-api",
        "grpc_port",
        config.services.edge_api.grpc_port.unwrap_or_default(),
        7233,
    )?;
    expect_port(
        "edge-poll",
        "grpc_port",
        config.services.edge_poll.grpc_port.unwrap_or_default(),
        7234,
    )?;
    expect_port(
        "controller",
        "grpc_port",
        config.services.controller.grpc_port.unwrap_or_default(),
        7240,
    )?;
    expect_port(
        "runtime",
        "grpc_port",
        config.services.runtime.grpc_port,
        7241,
    )?;
    expect_metrics("edge-api", config.services.edge_api.metrics_port)?;
    expect_metrics("edge-poll", config.services.edge_poll.metrics_port)?;
    expect_metrics("runtime", config.services.runtime.metrics_port)?;
    expect_metrics("projection", config.services.projection.metrics_port)?;
    expect_metrics("controller", config.services.controller.metrics_port)?;
    expect_metrics("autoscaler", config.services.autoscaler.metrics_port)?;
    expect_metrics("admin", config.services.admin.metrics_port)?;
    Ok(())
}

fn expect_metrics(service: &'static str, actual: u16) -> Result<(), EcsConfigError> {
    expect_port(service, "metrics_port", actual, 9090)
}

fn expect_port(
    service: &'static str,
    field: &'static str,
    actual: u16,
    expected: u16,
) -> Result<(), EcsConfigError> {
    if actual == expected {
        Ok(())
    } else {
        Err(EcsConfigError::InvalidCanonicalPort {
            service,
            field,
            expected,
            actual,
        })
    }
}

fn default_mimir_image() -> String {
    "grafana/mimir:3.0.6".into()
}

fn default_grafana_image() -> String {
    "grafana/grafana-oss:12.4.3".into()
}

fn default_loki_image() -> String {
    "grafana/loki:3.7.1".into()
}

fn default_alloy_image() -> String {
    "grafana/alloy:v1.16.0".into()
}

fn default_aws_cli_image() -> String {
    "amazon/aws-cli:latest".into()
}

fn default_busybox_image() -> String {
    "public.ecr.aws/docker/library/busybox:latest".into()
}

fn default_loki_query_url() -> String {
    "http://localhost:3100".into()
}
