# Design Document: ECS Deployment

## Overview

This design implements the ECS on EC2 deployment infrastructure for Tokeira, following the architecture described in [045-autoscaling-on-ecs-ec2](../../../docs/architecture/045-autoscaling-on-ecs-ec2.md). The implementation introduces a new `platforms/ecs/` platform crate, a `tokeira-autoscaler` service crate, and an autoscaler binary entry point. The design follows the existing platform pattern established by `platforms/compose/`.

The implementation is organized into 6 phases:

1. **Phase 1:** ECS platform crate scaffold and configuration model (`platforms/ecs/`)
2. **Phase 2:** Networking IaC module — VPC resources, subnets, security groups, VPC endpoints, internal ALB
3. **Phase 3:** Cluster IaC module — ECS cluster, capacity providers, ASGs, launch templates
4. **Phase 4:** Services IaC module — ECS service definitions, task definitions, Service Connect, Cloud Map
5. **Phase 5:** Autoscaler service — `tokeira-autoscaler` crate, leader lease, scaling loops A/B/C
6. **Phase 6:** CLI integration — `PlatformKind::Ecs`, prototypical config, operations commands

## Architecture

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                           ECS Cluster (per environment)                       │
│                                                                              │
│  ┌─────────────┐  ┌──────────────┐  ┌────────────┐  ┌────────────┐  ┌─────┐│
│  │ cp-edge-api  │  │ cp-edge-poll  │  │ cp-runtime  │  │cp-projection│  │cp-  ││
│  │              │  │              │  │              │  │            │  │ctrl ││
│  │ edge-api     │  │ edge-poll    │  │ runtime     │  │ projection │  │     ││
│  │ (REPLICA)    │  │ (REPLICA)    │  │ (DAEMON)    │  │ (REPLICA)  │  │ctrl ││
│  │              │  │              │  │              │  │            │  │auto ││
│  │              │  │              │  │              │  │            │  │admin││
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘  └────────────┘  └─────┘│
│         │                 │                 │                                  │
│  ┌──────┴─────────────────┴──────┐          │                                  │
│  │      Internal ALB             │          │                                  │
│  │  edge-api.<zone>              │          │                                  │
│  │  edge-poll.<zone>             │          │                                  │
│  └───────────────────────────────┘          │                                  │
│                                             │                                  │
│  ┌──────────────────────────────────────────┴──────────────────────────────┐  │
│  │                    Service Connect Namespace                             │  │
│  │  controller.tokeira  autoscaler.tokeira  projection.tokeira              │  │
│  └──────────────────────────────────────────────────────────────────────────┘  │
│                                                                              │
│  ┌──────────────────────────────────────────────────────────────────────────┐  │
│  │                    VPC Endpoints (private connectivity)                   │  │
│  │  ECS(3) ECR(2) S3(gw) AutoScaling CloudMap DSQL(2) [optional: STS,KMS] │  │
│  └──────────────────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────────────────┘
                                    │
                              ┌─────┴─────┐
                              │   DSQL     │
                              │ (storage)  │
                              └────────────┘
```

### Crate Dependency Graph

| Crate | New Dependencies | Role |
|---|---|---|
| `platforms/ecs` | `tokeira-iac`, `tokeira-deploy-engine`, `tokeira-orchestrator`, `tokeira-config`, `tokeira-aws`, `tokeira-state`, `tokeira-types` | ECS platform: config, IaC modules, service definitions, deployment/ops traits |
| `crates/tokeira-autoscaler` | `tokeira-types`, `tokeira-config`, `tokeira-storage`, `tokeira-proto`, `aws-sdk-ecs`, `aws-sdk-autoscaling`, `reqwest`, `tokio`, `tonic`, `tracing` | Autoscaler library: leader lease, Mimir client, scaling loops, AWS actuators |
| `apps/tokeira-autoscaler` | `tokeira-autoscaler`, `tokeira-config`, `anyhow`, `tokio`, `tracing-subscriber` | Autoscaler binary entry point |
| `tokeira-orchestrator` | *(none new)* | Add `PlatformKind::Ecs` variant |
| `tokeira-aws` | *(none new)* | New AWS resource implementations for ECS-specific resources |

### IaC Module Dependency Graph

```
remote-state → networking → cluster → services → observability
```

- **remote-state**: S3 state bucket (reuses existing `tokeira-aws` S3 resource)
- **networking**: VPC subnets, security groups, VPC endpoints, internal ALB
- **cluster**: ECS cluster, 5 capacity providers, 5 ASGs, launch templates, IAM instance profiles
- **services**: 7 Tokeira ECS service definitions, 7 task definitions (each with Alloy sidecar), Service Connect config, Cloud Map namespace
- **observability**: Mimir, Loki, Grafana ECS services, S3 buckets for metrics/log storage, IAM roles

## Components and Interfaces

### 1. ECS Platform Configuration (`platforms/ecs/src/config.rs`)

```rust
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EcsConfig {
    pub project_name: String,
    pub environment: String,
    pub region: String,
    pub cluster: ClusterConfig,
    pub networking: NetworkingConfig,
    pub capacity_providers: CapacityProviderConfigs,
    pub services: ServiceConfigs,
    pub autoscaler: AutoscalerConfig,
    pub alb: AlbConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterConfig {
    pub name: String,
    pub service_connect_namespace: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NetworkingConfig {
    pub vpc_id: String,
    pub private_subnet_ids: Vec<String>,
    pub availability_zones: Vec<String>,
    pub private_dns_zone: String,
    /// Optional VPC endpoints beyond the required set.
    pub optional_endpoints: OptionalEndpoints,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct OptionalEndpoints {
    pub sts: bool,
    pub kms: bool,
    pub secrets_manager: bool,
    pub ssm: bool,
    pub cloudwatch_logs: bool,
    pub ec2: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapacityProviderConfigs {
    pub edge_api: CapacityProviderConfig,
    pub edge_poll: CapacityProviderConfig,
    pub runtime: RuntimeCapacityProviderConfig,
    pub projection: CapacityProviderConfig,
    pub control: CapacityProviderConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapacityProviderConfig {
    pub instance_type: String,
    pub min_capacity: u32,
    pub max_capacity: u32,
    pub desired_capacity: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeCapacityProviderConfig {
    pub instance_type: String,
    pub min_capacity: u32,
    pub max_capacity: u32,
    pub desired_capacity: u32,
    /// Instance scale-in protection enabled by default.
    pub scale_in_protection: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplicaServiceConfig {
    pub image: String,
    pub desired_count: u32,
    pub cpu: u32,
    pub memory_mb: u32,
    pub grpc_port: Option<u16>,
    pub metrics_port: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DaemonServiceConfig {
    pub image: String,
    pub cpu: u32,
    pub memory_mb: u32,
    pub grpc_port: u16,
    pub metrics_port: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AutoscalerConfig {
    pub polling_interval_secs: u32,
    pub scale_out_consecutive_samples: u32,
    pub scale_in_consecutive_samples: u32,
    pub cooldown_secs: u32,
    pub mimir_endpoint: String,
    pub staleness_threshold_secs: u32,
    pub dsql_connection_budget: u32,
    pub dsql_connection_rate_budget: u32,
    pub per_runtime_reserved_connections: u32,
    pub per_runtime_startup_connection_rate: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AlbConfig {
    pub name: String,
    pub health_check_path: String,
    pub health_check_interval_secs: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObservabilityStackConfig {
    pub mimir_image: String,
    pub mimir_cpu: u32,
    pub mimir_memory_mb: u32,
    pub loki_image: String,
    pub loki_cpu: u32,
    pub loki_memory_mb: u32,
    pub grafana_image: String,
    pub grafana_cpu: u32,
    pub grafana_memory_mb: u32,
    pub alloy_sidecar_image: String,
    pub alloy_sidecar_cpu: u32,
    pub alloy_sidecar_memory_mb: u32,
    pub mimir_s3_bucket: String,
    pub loki_s3_bucket: String,
}

impl Default for EcsConfig {
    fn default() -> Self {
        Self {
            project_name: "tokeira".into(),
            environment: "dev".into(),
            region: "us-east-1".into(),
            cluster: ClusterConfig {
                name: "tokeira-dev".into(),
                service_connect_namespace: "tokeira.local".into(),
            },
            networking: NetworkingConfig {
                vpc_id: "vpc-REPLACE".into(),
                private_subnet_ids: vec!["subnet-REPLACE-a".into(), "subnet-REPLACE-b".into()],
                availability_zones: vec!["us-east-1a".into(), "us-east-1b".into()],
                private_dns_zone: "tokeira.internal".into(),
                optional_endpoints: OptionalEndpoints::default(),
            },
            capacity_providers: CapacityProviderConfigs {
                edge_api: CapacityProviderConfig {
                    instance_type: "c7g.large".into(),
                    min_capacity: 1, max_capacity: 10, desired_capacity: 2,
                },
                edge_poll: CapacityProviderConfig {
                    instance_type: "r7g.large".into(),
                    min_capacity: 1, max_capacity: 10, desired_capacity: 2,
                },
                runtime: RuntimeCapacityProviderConfig {
                    instance_type: "c7g.xlarge".into(),
                    min_capacity: 1, max_capacity: 20, desired_capacity: 2,
                    scale_in_protection: true,
                },
                projection: CapacityProviderConfig {
                    instance_type: "c7g.large".into(),
                    min_capacity: 1, max_capacity: 10, desired_capacity: 1,
                },
                control: CapacityProviderConfig {
                    instance_type: "t4g.medium".into(),
                    min_capacity: 1, max_capacity: 3, desired_capacity: 1,
                },
            },
            services: ServiceConfigs { /* defaults per service */ },
            autoscaler: AutoscalerConfig {
                polling_interval_secs: 15,
                scale_out_consecutive_samples: 2,
                scale_in_consecutive_samples: 8,
                cooldown_secs: 120,
                mimir_endpoint: "http://mimir.tokeira.local:9009".into(),
                staleness_threshold_secs: 60,
                dsql_connection_budget: 8000,
                dsql_connection_rate_budget: 80,
                per_runtime_reserved_connections: 200,
                per_runtime_startup_connection_rate: 10,
            },
            alb: AlbConfig {
                name: "tokeira-dev-alb".into(),
                health_check_path: "/health".into(),
                health_check_interval_secs: 10,
            },
        }
    }
}
```

### 2. IaC Modules (`platforms/ecs/src/modules.rs`)

The ECS platform defines four IaC modules following the same pattern as `ComposeModule`:

#### Networking Module

```rust
#[derive(Debug)]
pub struct NetworkingModule {
    config: NetworkingConfig,
    project_name: String,
    region: String,
}

impl iac::Module for NetworkingModule {
    fn name(&self) -> &str { "networking" }
    fn dependencies(&self) -> &[&str] { &["remote-state"] }
    fn resources(&self, ctx: &iac::ModuleContext) -> Result<Vec<Box<dyn iac::Resource>>, iac::IacError> {
        let mut resources: Vec<Box<dyn iac::Resource>> = Vec::new();
        // Security groups
        resources.push(Box::new(SecurityGroupResource { /* ALB SG */ }));
        resources.push(Box::new(SecurityGroupResource { /* edge SG */ }));
        resources.push(Box::new(SecurityGroupResource { /* runtime SG */ }));
        resources.push(Box::new(SecurityGroupResource { /* control SG */ }));
        resources.push(Box::new(SecurityGroupResource { /* endpoint SG */ }));
        // Required VPC endpoints
        for endpoint in required_vpc_endpoints(&self.region) {
            resources.push(Box::new(VpcEndpointResource { /* ... */ }));
        }
        // Optional VPC endpoints
        for endpoint in optional_vpc_endpoints(&self.config.optional_endpoints, &self.region) {
            resources.push(Box::new(VpcEndpointResource { /* ... */ }));
        }
        // Internal ALB
        resources.push(Box::new(AlbResource { /* ... */ }));
        resources.push(Box::new(AlbTargetGroupResource { /* edge-api TG */ }));
        resources.push(Box::new(AlbTargetGroupResource { /* edge-poll TG */ }));
        resources.push(Box::new(AlbListenerResource { /* ... */ }));
        Ok(resources)
    }
}
```

Required VPC endpoints (always provisioned):

| Service | Type | Count |
|---|---|---|
| `ecs`, `ecs-agent`, `ecs-telemetry` | Interface | 3 |
| `ecr.api`, `ecr.dkr` | Interface | 2 |
| `s3` | Gateway | 1 |
| `autoscaling` | Interface | 1 |
| `servicediscovery` | Interface | 1 |
| DSQL management + connection | PrivateLink | 2 |

#### Cluster Module

```rust
#[derive(Debug)]
pub struct ClusterModule {
    config: EcsConfig,
}

impl iac::Module for ClusterModule {
    fn name(&self) -> &str { "cluster" }
    fn dependencies(&self) -> &[&str] { &["networking"] }
    fn resources(&self, ctx: &iac::ModuleContext) -> Result<Vec<Box<dyn iac::Resource>>, iac::IacError> {
        let mut resources: Vec<Box<dyn iac::Resource>> = Vec::new();
        // ECS cluster
        resources.push(Box::new(EcsClusterResource { /* ... */ }));
        // IAM instance profiles (one per CP)
        // Launch templates (one per CP)
        // Auto Scaling groups (one per CP)
        for (name, cp_config) in capacity_provider_entries(&self.config) {
            resources.push(Box::new(IamInstanceProfileResource { /* ... */ }));
            resources.push(Box::new(LaunchTemplateResource { /* ... */ }));
            resources.push(Box::new(AsgResource { name, config: cp_config, /* ... */ }));
            resources.push(Box::new(CapacityProviderResource { /* ... */ }));
        }
        Ok(resources)
    }
}
```

#### Services Module

```rust
#[derive(Debug)]
pub struct ServicesModule {
    config: EcsConfig,
}

impl iac::Module for ServicesModule {
    fn name(&self) -> &str { "services" }
    fn dependencies(&self) -> &[&str] { &["cluster"] }
    fn resources(&self, ctx: &iac::ModuleContext) -> Result<Vec<Box<dyn iac::Resource>>, iac::IacError> {
        let mut resources: Vec<Box<dyn iac::Resource>> = Vec::new();
        // Cloud Map namespace
        resources.push(Box::new(CloudMapNamespaceResource { /* ... */ }));
        // Task definitions + ECS services for all 7 services
        for service_def in ecs_service_definitions(&self.config) {
            resources.push(Box::new(TaskDefinitionResource { def: service_def.clone() }));
            resources.push(Box::new(EcsServiceResource { def: service_def }));
        }
        Ok(resources)
    }
}
```

### 3. ECS Service Definitions (`platforms/ecs/src/services.rs`)

Each Tokeira service is wrapped as a deploy-engine `Service`:

```rust
#[derive(Debug)]
pub struct EcsWorkload {
    pub name: String,
    pub scheduling: EcsScheduling,
    pub capacity_provider: String,
    pub task_definition: TaskDefinitionSpec,
}

#[derive(Debug, Clone)]
pub enum EcsScheduling {
    Replica { desired_count: u32 },
    Daemon,
}

#[derive(Debug, Clone)]
pub struct TaskDefinitionSpec {
    pub family: String,
    pub image: String,
    pub cpu: u32,
    pub memory_mb: u32,
    pub port_mappings: Vec<PortMapping>,
    pub environment: Vec<EnvVar>,
    pub log_configuration: LogConfiguration,
    pub health_check: Option<HealthCheck>,
}

impl deploy_engine::Service for EcsWorkload {
    fn name(&self) -> &str { &self.name }
    fn module(&self) -> &str { "services" }
    fn dependencies(&self) -> &[&str] {
        match self.name.as_str() {
            "tokeira-edge-api" | "tokeira-edge-poll" => &["tokeira-runtime", "tokeira-controller"],
            "tokeira-projection" => &["tokeira-runtime"],
            "tokeira-autoscaler" => &["tokeira-controller"],
            "tokeira-admin" => &[],
            _ => &[],
        }
    }
    fn manifests(&self, ctx: &deploy_engine::ServiceContext)
        -> Result<Vec<serde_json::Value>, deploy_engine::DeployError>
    {
        Ok(vec![serde_json::to_value(&self.task_definition)?])
    }
}
```

Service definition table:

| Service | Scheduling | Capacity Provider | Default Count | Ingress |
|---|---|---|---|---|
| `tokeira-edge-api` | REPLICA | `cp-edge-api` | 2 | ALB target group (non-poll) |
| `tokeira-edge-poll` | REPLICA | `cp-edge-poll` | 2 | ALB target group (poll) |
| `tokeira-runtime` | DAEMON | `cp-runtime` | N/A (one per host) | None (internal only) |
| `tokeira-projection` | REPLICA | `cp-projection` | 1 | None (internal only) |
| `tokeira-controller` | REPLICA | `cp-control` | 2 | Service Connect |
| `tokeira-autoscaler` | REPLICA | `cp-control` | 2 | None |
| `tokeira-admin` | REPLICA | `cp-control` | 0 (on-demand) | None |
| `tokeira-mimir` | REPLICA | `cp-control` | 1 | Service Connect (`mimir.tokeira.local`) |
| `tokeira-loki` | REPLICA | `cp-control` | 1 | Service Connect (`loki.tokeira.local`) |
| `tokeira-grafana` | REPLICA | `cp-control` | 1 | Internal ALB or Service Connect |

### 4. Autoscaler Service (`crates/tokeira-autoscaler/`)

Internal structure:

```
crates/tokeira-autoscaler/
├── src/
│   ├── lib.rs              — crate root, re-exports
│   ├── config.rs           — autoscaler configuration
│   ├── leader.rs           — DSQL leader lease (single-active-leader)
│   ├── mimir.rs            — Mimir/Prometheus query client
│   ├── actuator.rs         — AWS API actuator (ECS + ASG)
│   ├── loop_a.rs           — REPLICA service scaling loop
│   ├── loop_b.rs           — Runtime scale-out loop
│   ├── loop_c.rs           — Runtime retirement loop
│   ├── envelope.rs         — Connection-aware scaling envelope
│   ├── freshness.rs        — Metric freshness and degraded mode
│   └── reconciler.rs       — Desired-state reconciliation
```

#### Leader Lease (`leader.rs`)

Reuses the `LeaseRepository` trait from `tokeira-storage` with a dedicated lease bundle for the autoscaler (separate from the controller's lease):

```rust
pub struct AutoscalerLeader {
    lease_repo: Arc<dyn LeaseRepository>,
    node_id: String,
    lease_bundle: ShardId,
    current_epoch: Option<ShardEpoch>,
    lease_duration: Duration,
    renewal_interval: Duration,
}

impl AutoscalerLeader {
    pub async fn try_acquire(&mut self) -> Result<bool> { /* ... */ }
    pub async fn renew(&mut self) -> Result<bool> { /* ... */ }
    pub fn is_leader(&self) -> bool { self.current_epoch.is_some() }
}
```

#### Mimir Client (`mimir.rs`)

Queries Mimir's Prometheus-compatible HTTP API for scaling signals:

```rust
pub struct MimirClient {
    endpoint: String,
    client: reqwest::Client,
    staleness_threshold: Duration,
}

pub struct MetricSample {
    pub value: f64,
    pub timestamp: Instant,
}

pub enum MetricFreshness {
    Fresh(MetricSample),
    Stale { last_sample: MetricSample, age: Duration },
    Missing,
}

impl MimirClient {
    pub async fn query_instant(&self, query: &str) -> Result<MetricFreshness> { /* ... */ }
    pub async fn query_range(&self, query: &str, range: Duration, step: Duration)
        -> Result<Vec<MetricSample>> { /* ... */ }
    pub fn is_available(&self) -> bool { /* ... */ }
}
```

#### AWS Actuator (`actuator.rs`)

Wraps ECS and Auto Scaling API calls with idempotency and throttle backoff:

```rust
pub struct AwsActuator {
    ecs_client: aws_sdk_ecs::Client,
    asg_client: aws_sdk_autoscaling::Client,
}

impl AwsActuator {
    /// Update ECS service desired count. No-op if already at target.
    pub async fn update_service_desired_count(
        &self, cluster: &str, service: &str, desired: u32,
    ) -> Result<bool> { /* ... */ }

    /// Set ASG desired capacity. No-op if already at target.
    pub async fn set_asg_desired_capacity(
        &self, asg_name: &str, desired: u32,
    ) -> Result<bool> { /* ... */ }

    /// Set ECS container instance to DRAINING.
    pub async fn drain_container_instance(
        &self, cluster: &str, instance_id: &str,
    ) -> Result<()> { /* ... */ }

    /// Clear instance scale-in protection.
    pub async fn clear_instance_protection(
        &self, asg_name: &str, instance_id: &str,
    ) -> Result<()> { /* ... */ }

    /// Terminate instance with decrement.
    pub async fn terminate_instance_with_decrement(
        &self, instance_id: &str,
    ) -> Result<()> { /* ... */ }

    /// Describe current service state.
    pub async fn describe_service(
        &self, cluster: &str, service: &str,
    ) -> Result<ServiceState> { /* ... */ }

    /// Describe current ASG state.
    pub async fn describe_asg(
        &self, asg_name: &str,
    ) -> Result<AsgState> { /* ... */ }
}
```

#### Connection-Aware Scaling Envelope (`envelope.rs`)

```rust
pub struct ScalingEnvelope {
    pub dsql_connection_budget: u32,
    pub dsql_connection_rate_budget: u32,
    pub per_runtime_reserved_connections: u32,
    pub per_runtime_startup_connection_rate: u32,
    pub configured_max_runtime_hosts: u32,
}

impl ScalingEnvelope {
    /// Compute the effective maximum runtime host count.
    pub fn effective_max_runtime_hosts(&self) -> u32 {
        let by_connections = self.dsql_connection_budget / self.per_runtime_reserved_connections;
        let by_rate = self.dsql_connection_rate_budget / self.per_runtime_startup_connection_rate;
        self.configured_max_runtime_hosts
            .min(by_connections)
            .min(by_rate)
    }

    /// Check if scaling to the target host count is within the envelope.
    pub fn allows_scale_to(&self, target_hosts: u32) -> bool {
        target_hosts <= self.effective_max_runtime_hosts()
    }
}
```

#### Metric Freshness (`freshness.rs`)

```rust
pub struct FreshnessTracker {
    staleness_threshold: Duration,
    /// Per-metric last-seen timestamps.
    last_seen: HashMap<String, Instant>,
}

pub enum ScalingPermission {
    Allowed,
    ScaleOutOnly,
    Frozen { reason: String },
}

impl FreshnessTracker {
    /// Determine what scaling actions are permitted given current metric freshness.
    pub fn scaling_permission(
        &self,
        mimir_available: bool,
        controller_snapshot_age: Option<Duration>,
        dsql_headroom_known: bool,
    ) -> ScalingPermission { /* ... */ }
}
```

#### Desired-State Reconciler (`reconciler.rs`)

```rust
pub struct DesiredState {
    pub service_counts: HashMap<String, u32>,
    pub asg_capacities: HashMap<String, u32>,
    pub drain_intents: HashMap<String, DrainIntent>,
}

pub struct DrainIntent {
    pub instance_id: String,
    pub started_at: Instant,
    pub state: DrainPhase,
}

pub enum DrainPhase {
    ControllerDraining,
    EcsDraining,
    ProtectionCleared,
    Terminated,
}

impl DesiredState {
    /// Reconcile desired state against current AWS state.
    /// Returns the list of actions to take.
    pub fn reconcile(&self, current: &CurrentState) -> Vec<ScalingAction> { /* ... */ }
}
```

### 4b. Alloy Sidecar in Task Definitions

Every Tokeira task definition includes an Alloy sidecar container alongside the primary application container. The sidecar:

- Scrapes Prometheus metrics from `localhost:{metrics_port}` on the primary container
- Forwards metrics to Mimir via remote-write (`http://mimir.tokeira.local:9009/api/v1/push`)
- Collects container stdout/stderr and ships to Loki (`http://loki.tokeira.local:3100/loki/api/v1/push`)
- Uses pinned image `grafana/alloy:v1.16.0`

```rust
fn alloy_sidecar_container(metrics_port: u16) -> ContainerDefinition {
    ContainerDefinition {
        name: "alloy".into(),
        image: "grafana/alloy:v1.16.0".into(),
        essential: false,  // sidecar failure should not kill the primary
        cpu: 64,
        memory_mb: 128,
        environment: vec![
            EnvVar { name: "METRICS_SCRAPE_TARGET".into(), value: format!("localhost:{metrics_port}") },
            EnvVar { name: "MIMIR_REMOTE_WRITE_URL".into(), value: "http://mimir.tokeira.local:9009/api/v1/push".into() },
            EnvVar { name: "LOKI_WRITE_URL".into(), value: "http://loki.tokeira.local:3100/loki/api/v1/push".into() },
        ],
        ..Default::default()
    }
}
```

### 4c. Observability Module (`platforms/ecs/src/modules.rs`)

The observability module provisions Mimir, Loki, and Grafana as ECS services on `cp-control`, plus S3 buckets for metrics and log storage:

```rust
#[derive(Debug)]
pub struct ObservabilityModule {
    config: EcsConfig,
}

impl iac::Module for ObservabilityModule {
    fn name(&self) -> &str { "observability" }
    fn dependencies(&self) -> &[&str] { &["services"] }
    fn resources(&self, ctx: &iac::ModuleContext) -> Result<Vec<Box<dyn iac::Resource>>, iac::IacError> {
        let mut resources: Vec<Box<dyn iac::Resource>> = Vec::new();
        // S3 buckets for Mimir and Loki storage
        resources.push(Box::new(S3BucketResource { name: format!("{}-mimir-data", self.config.project_name) }));
        resources.push(Box::new(S3BucketResource { name: format!("{}-loki-data", self.config.project_name) }));
        // IAM roles for S3 access
        resources.push(Box::new(IamRoleResource { /* mimir S3 access */ }));
        resources.push(Box::new(IamRoleResource { /* loki S3 access */ }));
        // Task definitions + ECS services for Mimir, Loki, Grafana
        resources.push(Box::new(TaskDefinitionResource { /* mimir */ }));
        resources.push(Box::new(EcsServiceResource { /* mimir */ }));
        resources.push(Box::new(TaskDefinitionResource { /* loki */ }));
        resources.push(Box::new(EcsServiceResource { /* loki */ }));
        resources.push(Box::new(TaskDefinitionResource { /* grafana */ }));
        resources.push(Box::new(EcsServiceResource { /* grafana */ }));
        Ok(resources)
    }
}
```

Mimir and Loki both run in single-binary mode with S3 as the long-term storage backend. Grafana is pre-configured with Mimir and Loki as data sources.

### 5. Deployment Trait Implementation (`platforms/ecs/src/lib.rs`)

```rust
pub struct EcsDeployment;

#[async_trait]
impl Deployment for EcsDeployment {
    type Config = EcsConfig;

    fn remote_state_module(&self, config: &Self::Config, deployment_dir: &Path)
        -> Box<dyn iac::Module>
    {
        // S3 state bucket via tokeira-aws S3 resource
        Box::new(S3StateModule { /* ... */ })
    }

    fn infra_modules(&self, config: &Self::Config, selection: &iac::ModuleSelection)
        -> Vec<Box<dyn iac::Module>>
    {
        let mut modules: Vec<Box<dyn iac::Module>> = Vec::new();
        let networking = NetworkingModule::new(config);
        let cluster = ClusterModule::new(config);
        let services = ServicesModule::new(config);
        if selection.includes(networking.name()) { modules.push(Box::new(networking)); }
        if selection.includes(cluster.name()) { modules.push(Box::new(cluster)); }
        if selection.includes(services.name()) { modules.push(Box::new(services)); }
        modules
    }

    fn services(&self, config: &Self::Config) -> Vec<Box<dyn deploy_engine::Service>> {
        ecs_workloads(config)
            .into_iter()
            .map(|w| Box::new(w) as Box<dyn deploy_engine::Service>)
            .collect()
    }

    fn images(&self, config: &Self::Config) -> Vec<Box<dyn deploy_engine::Image>> {
        ecs_images(config)
            .into_iter()
            .map(|i| Box::new(i) as Box<dyn deploy_engine::Image>)
            .collect()
    }

    // ... remaining trait methods follow compose pattern
}

#[async_trait]
impl Ops for EcsDeployment {
    type Config = EcsConfig;

    fn valid_services(&self) -> &[&str] {
        static VALID: [&str; 7] = [
            "tokeira-edge-api", "tokeira-edge-poll", "tokeira-runtime",
            "tokeira-projection", "tokeira-controller", "tokeira-autoscaler",
            "tokeira-admin",
        ];
        &VALID
    }

    fn desired_replicas(&self, config: &Self::Config) -> Vec<ServiceReplicas> {
        // Return desired counts from config for each REPLICA service
    }

    async fn scale_up(&self, service: &str, replicas: u32, config: &Self::Config) -> Result<()> {
        // Call ecs:UpdateService to increase desired count
    }

    async fn scale_down(&self, service: &str, replicas: u32, config: &Self::Config) -> Result<()> {
        // Call ecs:UpdateService to decrease desired count
    }

    async fn logs(&self, service: &str, config: &Self::Config) -> Result<Vec<String>> {
        // Retrieve logs from ECS tasks
    }

    async fn port_mappings(&self, service: &str, config: &Self::Config) -> Result<Vec<PortMapping>> {
        // ECS services use internal networking; return internal endpoints
    }
}
```

## Data Models

### ECS Platform Configuration

| Section | Key Fields | Default |
|---|---|---|
| `cluster` | `name`, `service_connect_namespace` | `tokeira-dev`, `tokeira.local` |
| `networking` | `vpc_id`, `private_subnet_ids`, `availability_zones`, `private_dns_zone`, `optional_endpoints` | Must be provided |
| `capacity_providers.edge_api` | `instance_type`, `min/max/desired_capacity` | `c7g.large`, 1/10/2 |
| `capacity_providers.edge_poll` | `instance_type`, `min/max/desired_capacity` | `r7g.large`, 1/10/2 |
| `capacity_providers.runtime` | `instance_type`, `min/max/desired_capacity`, `scale_in_protection` | `c7g.xlarge`, 1/20/2, true |
| `capacity_providers.projection` | `instance_type`, `min/max/desired_capacity` | `c7g.large`, 1/10/1 |
| `capacity_providers.control` | `instance_type`, `min/max/desired_capacity` | `t4g.medium`, 1/3/1 |
| `autoscaler` | `polling_interval_secs`, `scale_out/in_consecutive_samples`, `cooldown_secs`, `mimir_endpoint`, `staleness_threshold_secs`, `dsql_connection_budget`, `per_runtime_reserved_connections` | 15s, 2/8, 120s, 8000, 200 |
| `alb` | `name`, `health_check_path`, `health_check_interval_secs` | `/health`, 10s |

### Autoscaler Scaling Inputs

| Service | Primary Signals | Secondary Signals |
|---|---|---|
| `tokeira-edge-api` | In-flight non-poll RPCs, p95/p99 latency | Reject rate, CPU |
| `tokeira-edge-poll` | Open long polls, admitted polls/sec | Rejected polls/sec, broker handoff latency, memory |
| `tokeira-runtime` | Runnable transitions/lane, commit latency | Shard imbalance, conflict rate, sweeper backlog, DSQL headroom |
| `tokeira-projection` | Projection lag (seconds), oldest unapplied mutation age | Sink apply latency, failure rate |
| `tokeira-controller` | Static (2 tasks) | N/A |
| `tokeira-autoscaler` | Static (2 tasks) | N/A |

### Metric Freshness Decision Matrix

| Condition | Scale Out | Scale In |
|---|---|---|
| Mimir healthy, metrics fresh | Allowed | Allowed |
| Mimir unavailable | Emergency/manual only | No |
| Metric series missing | Maybe (with fallback) | No |
| Controller snapshot stale | Edge/projection only | No runtime scale-in |
| DSQL headroom unknown | Constrained to floor | No runtime scale-out beyond floor |
| AWS API throttled | Backoff | Backoff |

## Correctness Properties

### Property 1: ECS config TOML round-trip

*For any* valid `EcsConfig`, serializing to TOML and deserializing back SHALL produce an equivalent `EcsConfig`.

**Validates: Requirements 1.1.1, 1.3.1**

### Property 2: Unknown config fields rejected

*For any* valid `EcsConfig` TOML with an additional unknown field inserted, deserialization SHALL fail with an error.

**Validates: Requirements 1.1.3, 1.3.2**

### Property 3: Module dependency graph is a DAG

*For any* `EcsConfig`, the module dependency graph produced by `infra_modules` SHALL be a directed acyclic graph with no cycles and no missing dependencies.

**Validates: Requirements 7.4.1, 7.4.2**

### Property 4: Service dependency graph is a DAG

*For any* `EcsConfig`, the service dependency graph produced by `services()` SHALL be a directed acyclic graph with no cycles and no missing dependencies.

**Validates: Requirements 4.8.1**

### Property 5: Connection-aware scaling envelope monotonicity

*For any* `ScalingEnvelope` with positive parameters, `effective_max_runtime_hosts()` SHALL be less than or equal to `configured_max_runtime_hosts`, and SHALL decrease monotonically as `per_runtime_reserved_connections` increases.

**Validates: Requirements 6.7.1, 6.7.2**

### Property 6: Scaling envelope correctness

*For any* `ScalingEnvelope`, `allows_scale_to(n)` SHALL return true if and only if `n <= effective_max_runtime_hosts()`.

**Validates: Requirements 6.7.1, 6.7.2**

### Property 7: Desired-state reconciliation idempotency

*For any* `DesiredState` and `CurrentState` where desired matches current, `reconcile()` SHALL return an empty action list.

**Validates: Requirements 6.8.1, 6.8.2**

### Property 8: Metric freshness safety

*For any* `FreshnessTracker` state where Mimir is unavailable, `scaling_permission()` SHALL return `Frozen` or `ScaleOutOnly` — never `Allowed` for scale-in.

**Validates: Requirements 6.6.1, 6.6.2, 6.6.5**

### Property 9: Service manifest stability

*For any* unchanged `EcsConfig`, calling `manifests()` on the same `EcsWorkload` twice SHALL produce identical JSON values.

**Validates: Requirements 4.8.3**

### Property 10: Invalid service name rejection

*For any* string that is not in the valid services list, `Ops` methods SHALL return an error containing the invalid name and listing valid alternatives.

**Validates: Requirements 8.3.3**

## Error Handling

### AWS API Errors

- **Throttling** (`ThrottlingException`, `TooManyRequestsException`): Retry with exponential backoff. The autoscaler's reconciliation loop naturally retries on the next iteration.
- **Permission errors**: Fail fast with a descriptive error message including the required IAM permission.
- **Resource not found**: During `describe`, return `None` (resource doesn't exist yet). During `update`/`delete`, return an error.
- **Eventual consistency**: The autoscaler tolerates stale `DescribeServices`/`DescribeAutoScalingGroups` responses by reconciling on each loop iteration.

### Autoscaler Errors

- **Mimir unavailable**: Freeze desired capacity. Log warning. Continue retrying on each loop iteration.
- **Controller unavailable**: Block runtime scale-in. Allow edge/projection scaling from Mimir metrics alone.
- **DSQL leader lease lost**: Stop writing scaling decisions. Revert to standby. Attempt re-acquisition.
- **Scaling action failure**: Log the failure with full context. Retry on next loop iteration. Do not crash.

### IaC Module Errors

- **Resource creation failure**: The IaC engine handles rollback via the `StateSaver` callback. Failed resources are recorded in state for retry.
- **Dependency resolution failure**: The engine rejects cyclic or missing dependencies before any resource lifecycle methods are called.

## Testing Strategy

### Property-Based Tests

| Property | Test Location | Generator Strategy |
|---|---|---|
| Property 1: Config TOML round-trip | `platforms/ecs/src/config.rs` | Generate random valid `EcsConfig` values. Serialize to TOML, deserialize, assert equality. |
| Property 2: Unknown fields rejected | `platforms/ecs/src/config.rs` | Generate valid TOML, insert random unknown key, assert deserialization fails. |
| Property 3: Module DAG | `platforms/ecs/src/modules.rs` | Generate configs, build module graph, verify topological sort succeeds. |
| Property 4: Service DAG | `platforms/ecs/src/services.rs` | Generate configs, build service graph, verify no cycles. |
| Property 5: Envelope monotonicity | `crates/tokeira-autoscaler/src/envelope.rs` | Generate random envelope params, verify monotonicity. |
| Property 6: Envelope correctness | `crates/tokeira-autoscaler/src/envelope.rs` | Generate random envelopes and target counts, verify `allows_scale_to` consistency. |
| Property 7: Reconciliation idempotency | `crates/tokeira-autoscaler/src/reconciler.rs` | Generate matching desired/current states, verify empty action list. |
| Property 8: Freshness safety | `crates/tokeira-autoscaler/src/freshness.rs` | Generate tracker states with Mimir unavailable, verify no scale-in allowed. |
| Property 9: Manifest stability | `platforms/ecs/src/services.rs` | Generate configs, call manifests twice, assert identical output. |
| Property 10: Invalid service rejection | `platforms/ecs/src/lib.rs` | Generate random strings not in valid set, verify error message. |

### Unit Tests

- **Config validation**: Verify default config is valid. Verify invalid combinations produce descriptive errors.
- **Module composition**: Verify `infra_modules` returns correct modules for each `ModuleSelection`. Verify module names and dependencies.
- **Service definitions**: Verify all 7 services are generated. Verify scheduling types match (DAEMON for runtime, REPLICA for others).
- **Scaling envelope**: Verify `effective_max_runtime_hosts` computation with known inputs. Verify edge cases (zero budget, very large budget).
- **Metric freshness**: Verify all cells of the freshness decision matrix.
- **Reconciler**: Verify no-op when desired matches current. Verify correct actions when desired differs.
- **Autoscaler leader**: Verify acquire/renew/revert lifecycle.
- **AWS actuator**: Verify no-op when state matches target. Verify correct API calls when state differs (using mocked clients).

### Integration Tests

- **End-to-end IaC plan**: Load a prototypical config, compose modules, run `plan`, verify expected resource set.
- **End-to-end deploy plan**: Load a prototypical config, generate services, run `plan`, verify expected service set.
- **Autoscaler loop**: Mock Mimir responses and AWS API calls. Verify correct scaling decisions for various metric scenarios.
