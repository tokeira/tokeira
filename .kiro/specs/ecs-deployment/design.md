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
│  │ +alloy       │  │ +alloy       │  │ +alloy       │  │ +alloy     │  │auto ││
│  │              │  │              │  │              │  │            │  │admin││
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘  └────────────┘  │mimir│
│         │                 │                 │                           │loki ││
│  ┌──────┴─────────────────┴──────┐          │                           │graf ││
│  │      Internal ALB             │          │                           └─────┘│
│  │  edge-api.<zone>              │          │                                  │
│  │  edge-poll.<zone>             │          │                                  │
│  └───────────────────────────────┘          │                                  │
│                                             │                                  │
│  ┌──────────────────────────────────────────┴──────────────────────────────┐  │
│  │                    Service Connect Namespace                             │  │
│  │  controller  autoscaler  projection  mimir  loki  grafana                │  │
│  └──────────────────────────────────────────────────────────────────────────┘  │
│                                                                              │
│  ┌──────────────────────────────────────────────────────────────────────────┐  │
│  │                    VPC Endpoints (private connectivity)                   │  │
│  │  ECS(3) ECR(2) S3(gw) AutoScaling CloudMap DSQL(2) [opt: STS,KMS,CWL]  │  │
│  └──────────────────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────────────────┘
                                    │
                    ┌───────────────┼───────────────┐
                    │               │               │
              ┌─────┴─────┐  ┌─────┴─────┐  ┌─────┴─────┐
              │   DSQL     │  │    S3      │  │ Secrets   │
              │ (persist)  │  │ (state,    │  │ Manager   │
              │            │  │  metrics,  │  │ (creds)   │
              └────────────┘  │  logs)     │  └───────────┘
                              └────────────┘
```

**Note on runtime scheduling:** Runtime uses DAEMON scheduling (one task per host) because each runtime process owns bundles and manages shard-local lanes. DAEMON ensures predictable resource envelopes — the entire host's CPU and memory are available to the single runtime task. Scaling the runtime fleet means scaling the ASG (adding/removing hosts), not adjusting a desired count.

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
remote-state → networking → dsql → cluster → observability → services
```

- **remote-state**: S3 state bucket with shared-bucket semantics — snapshot delete prevention policy, versioning enforcement, public access block, adoption of existing buckets, no-op delete. Implemented as a `RemoteStateBucket` resource in `platforms/` (shared across all AWS-backed platforms).
- **networking**: VPC subnets, security groups, VPC endpoints, internal ALB
- **dsql**: DSQL cluster, DSQL PrivateLink endpoints, IAM authentication roles
- **cluster**: ECS cluster, 5 capacity providers, 5 ASGs, launch templates, IAM instance profiles
- **observability**: Mimir, Loki, Grafana ECS services, S3 buckets for metrics/log storage, IAM roles
- **services**: 7 Tokeira ECS service definitions, 7 task definitions (each with Alloy sidecar), Service Connect config, Cloud Map namespace. Depends on observability because Alloy sidecars need Mimir/Loki endpoints.

## Components and Interfaces

### 1. CLI Progress Reporting (Prerequisite)

Before the ECS platform can provide good operator UX during `infra apply`, the IaC engine and CLI output module need progress callback support. This is a prerequisite for all subsequent phases.

#### IaC Engine Progress Callbacks (`tokeira-iac`)

Add three callback registration methods to `ProvisionContext`:

```rust
impl ProvisionContext {
    pub fn set_apply_progress<F>(&mut self, reporter: F)
    where
        F: Fn(&str, &ResourceId, &ResourceType, usize, usize) + Send + Sync + 'static;

    pub fn set_wait_progress<F>(&mut self, reporter: F)
    where
        F: Fn(&ResourceId, &ResourceType, &str, Duration, Duration) + Send + Sync + 'static;

    pub fn set_note_progress<F>(&mut self, reporter: F)
    where
        F: Fn(&ResourceId, &ResourceType, &str) + Send + Sync + 'static;

    pub fn emit_apply_progress(&self, action: &str, rid: &ResourceId, rtype: &ResourceType, current: usize, total: usize);
    pub fn emit_wait_progress(&self, rid: &ResourceId, rtype: &ResourceType, phase: &str, elapsed: Duration, timeout: Duration);
    pub fn emit_note_progress(&self, rid: &ResourceId, rtype: &ResourceType, message: &str);
}
```

The IaC engine calls `emit_apply_progress` before each resource lifecycle operation and `emit_wait_progress` during polling waits.

#### CLI Output Module (`apps/tkr/src/output.rs`)

Replace the current minimal `OutputFormatter` with a full output module:

```rust
use console::Style;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputFormat {
    Human,
    Json,
}

#[derive(Clone)]
pub struct ActionTuiHandle {
    overall: ProgressBar,   // [##--] 3/12 creating VpcEndpoint (ecs)
    detail: ProgressBar,    // spinner for wait phases
}

impl ActionTuiHandle {
    pub fn on_action(&self, msg: &str, current: usize, total: usize);
    pub fn on_wait(&self, msg: &str);
    pub fn finish(&self);
}

pub fn start_action_tui(format: OutputFormat) -> Option<ActionTuiHandle>;
pub fn print_header(format: OutputFormat, title: &str);
pub fn print_status(format: OutputFormat, label: &str, value: &str);
pub fn print_progress(format: OutputFormat, msg: &str);
pub fn print_success(format: OutputFormat, msg: &str);
pub fn print_warning(format: OutputFormat, msg: &str);
pub fn print_error(format: OutputFormat, msg: &str);
pub fn print_changes(format: OutputFormat, changes: &[(String, String, String, Option<String>)]);
pub fn print_deployment_table(format: OutputFormat, rows: &[(String, u32, u32, u32, bool)]);
pub fn print_json<T: Serialize>(data: &T);
```

New dependencies: `console` (ANSI styles, TTY detection), `indicatif` (progress bars, spinners).

#### Wiring in `infra apply`

```rust
let tui = output::start_action_tui(format);
let apply_tui = tui.clone();
engine.context_mut().set_apply_progress(move |action, rid, rtype, current, total| {
    let msg = format!("{action} {} ({})", rid.0, rtype);
    if let Some(tui) = &apply_tui {
        tui.on_action(&msg, current, total);
    } else {
        output::print_progress(format, &format!("[{current}/{total}] {msg}"));
    }
});
```

### 1a. ECS Platform Configuration (`platforms/ecs/src/config.rs`)

```rust
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EcsConfig {
    pub project_name: String,
    pub environment: String,
    pub region: String,
    /// Operator-defined custom tags merged with auto-generated tags on every resource.
    pub tags: HashMap<String, String>,
    pub cluster: ClusterConfig,
    pub networking: NetworkingConfig,
    pub dsql: DsqlConfig,
    pub capacity_providers: CapacityProviderConfigs,
    pub services: ServiceConfigs,
    pub autoscaler: AutoscalerConfig,
    pub alb: AlbConfig,
    pub observability: Option<ObservabilityStackConfig>,
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
pub struct DsqlConfig {
    /// DSQL cluster endpoint (e.g., "cluster.dsql.us-east-1.on.aws").
    pub endpoint: String,
    /// IAM role ARN for runtime DSQL access.
    pub runtime_role_arn: Option<String>,
    /// IAM role ARN for admin/migration DSQL access.
    pub admin_role_arn: Option<String>,
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

#### Resource Tagging

All AWS resources carry auto-generated tags plus operator-defined custom tags:

```rust
fn resource_tags(config: &EcsConfig, resource_name: &str) -> HashMap<String, String> {
    let mut tags = HashMap::new();
    // Auto-generated tags
    tags.insert("Name".into(), resource_name.into());
    tags.insert("Project".into(), config.project_name.clone());
    tags.insert("Environment".into(), config.environment.clone());
    tags.insert("ManagedBy".into(), "tkr-cli".into());
    // Operator custom tags override auto-generated on conflict
    for (k, v) in &config.tags {
        tags.insert(k.clone(), v.clone());
    }
    tags
}
```

Every `Resource` implementation passes `resource_tags(config, name)` to the AWS SDK create/update calls. This applies to VPC resources, security groups, VPC endpoints, ALB, ECS cluster, capacity providers, ASGs, launch templates, IAM roles, S3 buckets, DSQL cluster, Cloud Map namespace, and ECS services/task definitions.

### 2. IaC Modules (`platforms/ecs/src/modules.rs`)

The ECS platform defines five IaC modules following the same pattern as `ComposeModule`:

#### Remote-State Module

The remote-state module provisions a shared S3 bucket for IaC state persistence. This resource has distinct lifecycle semantics from general-purpose S3 buckets.

> **Reference implementation:** The complete `RemoteStateBucket` resource is provided in [`reference/remote_state_bucket.rs`](reference/remote_state_bucket.rs) with all Tokeira import paths applied. Implementers should place this file in a shared location under `platforms/` (not in a platform-specific crate or `tokeira-aws`) because any AWS-backed platform (ECS, EKS, future platforms) needs the same shared-bucket lifecycle semantics.

```rust
/// Shared remote-state bucket with per-project snapshot protection.
///
/// Lives in `platforms/` (shared across all AWS-backed platforms) because
/// any AWS-backed platform (ECS, EKS, future platforms) needs the same
/// shared-bucket lifecycle semantics.
#[derive(Debug)]
pub struct RemoteStateBucket {
    bucket_name: String,
    region: String,
    key_prefix: String,
    module: String,
}
```

Key behaviors that distinguish this from a general S3 bucket:

| Behavior | Rationale |
|---|---|
| **Snapshot delete prevention policy** | Bucket policy with `Deny` on `s3:DeleteObject` scoped to `{key_prefix}/snapshots/*`. Prevents accidental or malicious deletion of state snapshots. |
| **Bucket adoption** | If the bucket already exists (`BucketAlreadyOwnedByYou`), adopts it without error. Marks `managed_snapshot_policy = false` so it doesn't overwrite an existing policy protecting other projects' snapshots. |
| **Public access block enforcement** | Always sets `BlockPublicAcls`, `IgnorePublicAcls`, `BlockPublicPolicy`, `RestrictPublicBuckets`. |
| **Versioning enforcement** | Always enables S3 versioning. Critical for state safety — allows recovery from corrupted writes. |
| **No-op delete** | `delete()` logs a message and returns Ok. The state bucket outlives any single deployment. |
| **Tag-drift tolerance** | `diff()` ignores tag changes (shared bucket may be tagged by other projects). Only versioning and snapshot policy drift trigger updates. |
| **Managed vs adopted policy tracking** | Persists whether this project "owns" the snapshot policy. Adopted buckets don't get their policy overwritten on update. |

The `RemoteStateModule` wraps this resource:

```rust
#[derive(Debug)]
pub struct RemoteStateModule {
    bucket_name: String,
    region: String,
    key_prefix: String,
}

impl iac::Module for RemoteStateModule {
    fn name(&self) -> &str { "remote-state" }
    fn dependencies(&self) -> &[&str] { &[] }
    fn resources(&self, _ctx: &iac::ModuleContext) -> Result<Vec<Box<dyn iac::Resource>>, iac::IacError> {
        Ok(vec![Box::new(RemoteStateBucket::new(
            &self.bucket_name,
            &self.region,
            &self.key_prefix,
            "remote-state",
        ))])
    }
}
```

The bucket name follows the pattern `{project_name}-state-{region}` (e.g., `tokeira-state-us-east-1`). The key prefix is `{project_name}/{environment}` (e.g., `tokeira/dev`), allowing multiple environments to share one bucket with isolated state paths and per-prefix snapshot protection.

#### Networking Module

```rust
#[derive(Debug)]
pub struct NetworkingModule {
    config: NetworkingConfig,
    project_name: String,
    region: String,
    tags: HashMap<String, String>,
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

#### DSQL Module

```rust
#[derive(Debug)]
pub struct DsqlModule {
    config: DsqlConfig,
    project_name: String,
    environment: String,
    tags: HashMap<String, String>,
}

impl iac::Module for DsqlModule {
    fn name(&self) -> &str { "dsql" }
    fn dependencies(&self) -> &[&str] { &["networking"] }
    fn resources(&self, ctx: &iac::ModuleContext) -> Result<Vec<Box<dyn iac::Resource>>, iac::IacError> {
        let mut resources: Vec<Box<dyn iac::Resource>> = Vec::new();
        // DSQL cluster (reuses existing tokeira-aws DsqlCluster resource)
        resources.push(Box::new(DsqlClusterResource { /* ... */ }));
        // IAM roles for runtime and admin DSQL access
        resources.push(Box::new(IamRoleResource { /* runtime DSQL access */ }));
        resources.push(Box::new(IamRoleResource { /* admin DSQL access */ }));
        Ok(resources)
    }
}
```

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
    fn dependencies(&self) -> &[&str] { &["observability"] }
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

### 3a. Infrastructure and Service Sizing Rationale

This section documents the reasoning behind instance types, task resource limits, and capacity provider defaults for each service plane. All instance types are Graviton (ARM64) for cost efficiency. Resource limits are set as requests = limits for guaranteed QoS — no overcommit.

#### Capacity Provider: `cp-edge-api`

| Setting | Value | Rationale |
|---|---|---|
| Instance type | `c7g.large` (2 vCPU, 4 GiB) | Edge-api is CPU-bound: gRPC deserialization, routing lookup, request forwarding. No DSQL connections. Memory demand is low — routing cache is a small `ArcSwap<RoutingSnapshot>`. |
| Min/Max/Desired | 1 / 10 / 2 | Two instances for HA. Max 10 handles ~10k concurrent non-poll RPCs. |

| Task resource | Value | Rationale |
|---|---|---|
| CPU | 1024 (1 vCPU) | One edge-api task per instance leaves headroom for the Alloy sidecar and ECS agent. |
| Memory | 2048 MiB | gRPC buffers, routing cache, connection pools to runtime nodes. 2 GiB is generous for a stateless forwarder. |
| Alloy sidecar | 128 CPU / 256 MiB | Metrics scrape + log shipping. Minimal footprint. |
| Ports | gRPC 7233, metrics 9090 | Standard Temporal-compatible gRPC port. Prometheus metrics on 9090. |

#### Capacity Provider: `cp-edge-poll`

| Setting | Value | Rationale |
|---|---|---|
| Instance type | `r7g.large` (2 vCPU, 16 GiB) | Edge-poll is memory-bound: each long-poll holds a gRPC stream and a broker subscription. At 1000 concurrent polls × ~16 KiB per poll context, memory dominates. `r7g` (memory-optimized) is the right family. |
| Min/Max/Desired | 1 / 10 / 2 | Two instances for HA. Max 10 handles ~10k concurrent long polls. |

| Task resource | Value | Rationale |
|---|---|---|
| CPU | 1024 (1 vCPU) | Poll handling is I/O-bound (waiting on broker notifications), not CPU-bound. |
| Memory | 8192 MiB | 8 GiB accommodates ~5000 concurrent polls per task with headroom for gRPC buffers and broker state. |
| Alloy sidecar | 128 CPU / 256 MiB | Same as edge-api. |
| Ports | gRPC 7234, metrics 9090 | Separate port from edge-api to allow ALB routing by port. |

#### Capacity Provider: `cp-runtime`

| Setting | Value | Rationale |
|---|---|---|
| Instance type | `c7g.xlarge` (4 vCPU, 8 GiB) | Runtime is CPU-bound: kernel transition evaluation, history serialization, DSQL transaction preparation. 4 vCPU supports ~4 lanes of concurrent transition processing. Memory holds the DSQL connection reservoir (50 connections × ~2 MiB each = ~100 MiB), shard owner state, and in-flight actor state. |
| Min/Max/Desired | 1 / 20 / 2 | Two instances for initial bundle distribution. Max 20 supports ~2000 WPS at 100 WPS/node. Scale-in protection enabled. |
| DAEMON scheduling | One task per host | Runtime owns bundles and manages shard-local lanes. The entire host's resources are dedicated to the single runtime process. Scaling the runtime fleet means scaling the ASG. |

| Task resource | Value | Rationale |
|---|---|---|
| CPU | 3584 (3.5 vCPU) | Leaves 0.5 vCPU for Alloy sidecar and ECS agent. DAEMON task gets the full host minus sidecar overhead. |
| Memory | 6656 MiB (6.5 GiB) | Leaves ~1.5 GiB for Alloy, ECS agent, and OS. Accommodates: DSQL reservoir (~100 MiB), lane actor state (~50 MiB per lane × 4 lanes), history buffers, gRPC server buffers. |
| Alloy sidecar | 256 CPU / 512 MiB | Runtime generates more metrics (per-shard, per-lane) and more log volume than edge. Larger sidecar allocation. |
| Ports | gRPC 7235 (internal), metrics 9090 | Internal-only gRPC for edge→runtime forwarding. No ALB registration. |
| DSQL connections | 32 per node (see [060-connection-management](../../../docs/architecture/060-connection-management.md#connection-demand-analysis)) | Control: 2–3, Commit: 15, Read: 10, Projection: 3, Maintenance: 2. |

#### Capacity Provider: `cp-projection`

| Setting | Value | Rationale |
|---|---|---|
| Instance type | `c7g.large` (2 vCPU, 4 GiB) | Projection workers are CPU-bound: deserializing postcard-encoded projection ops, computing search attribute indexes, writing to DSQL visibility tables. Memory demand is moderate — batch buffers and DSQL connection pool. |
| Min/Max/Desired | 1 / 10 / 1 | One instance is sufficient for low-to-moderate WPS. Scales with projection lag. |

| Task resource | Value | Rationale |
|---|---|---|
| CPU | 1024 (1 vCPU) | Projection workers process batches sequentially per partition. One vCPU handles the decode→transform→write pipeline. |
| Memory | 2048 MiB | Batch buffers (~10 MiB), DSQL connection pool (~20 MiB), search attribute index state. |
| Alloy sidecar | 128 CPU / 256 MiB | Standard sidecar. |
| Ports | metrics 9090 | No gRPC ingress — projection workers pull from the projection log. |
| DSQL connections | 5 per task | Projection class only. Reads from projection_log, writes to visibility tables. |

#### Capacity Provider: `cp-control`

| Setting | Value | Rationale |
|---|---|---|
| Instance type | `t4g.medium` (2 vCPU, 4 GiB) | Control plane services (controller, autoscaler, admin, observability stack) are low-throughput. `t4g` (burstable) is cost-appropriate — these services are mostly idle with periodic bursts (snapshot computation, scaling decisions, dashboard queries). |
| Min/Max/Desired | 1 / 3 / 1 | One instance hosts all control-plane tasks. Max 3 for HA during rolling updates. |

Services sharing `cp-control`:

| Service | CPU | Memory | Rationale |
|---|---|---|---|
| `tokeira-controller` (×2) | 256 | 512 MiB | Lightweight: reads DSQL leases, computes snapshots, streams to subscribers. Two replicas for HA. |
| `tokeira-autoscaler` (×2) | 256 | 512 MiB | Lightweight: queries Mimir, computes scaling decisions, calls AWS APIs. Two replicas with leader lease. |
| `tokeira-admin` (×0) | 256 | 512 MiB | On-demand only. Schema migrations and diagnostics. |
| `tokeira-mimir` (×1) | 512 | 1024 MiB | Single-binary mode. Ingests metrics from Alloy sidecars, serves PromQL queries. S3 for long-term storage. |
| `tokeira-loki` (×1) | 256 | 512 MiB | Single-binary mode. Ingests logs from Alloy sidecars. S3 for long-term storage. |
| `tokeira-grafana` (×1) | 256 | 512 MiB | Dashboard rendering. Reads from Mimir and Loki. Low steady-state load. |

Total `cp-control` resource demand at default counts: ~2560 CPU units (2.5 vCPU), ~4096 MiB (4 GiB). Fits on one `t4g.medium` with headroom. A second instance is needed during rolling updates or if Mimir ingestion load grows.

#### Alloy Sidecar Sizing

Every task definition includes an Alloy sidecar. The sidecar's resource allocation varies by service:

| Service plane | Sidecar CPU | Sidecar Memory | Rationale |
|---|---|---|---|
| Edge (api/poll) | 128 | 256 MiB | Low metric cardinality, moderate log volume. |
| Runtime | 256 | 512 MiB | High metric cardinality (per-shard, per-lane, per-class), high log volume during transitions. |
| Projection | 128 | 256 MiB | Low metric cardinality, low log volume. |
| Control plane | 64 | 128 MiB | Minimal metrics and logs. |

#### Why Graviton (ARM64)

All instance types use Graviton processors (`c7g`, `r7g`, `t4g`):
- ~20% better price-performance than equivalent x86 instances
- Tokeira is pure Rust compiled for `aarch64-unknown-linux-gnu` — no x86 dependencies
- Alloy, Mimir, Loki, and Grafana all publish ARM64 container images

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
    fn dependencies(&self) -> &[&str] { &["cluster"] }
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

### 4d. Observability Configuration Details

Each observability component requires a configuration payload embedded in the ECS task definition as an environment variable or mounted via an S3-backed config file. These configs are generated programmatically from `EcsConfig` values — no static YAML files. The ECS platform uses Askama templates (same pattern as the compose platform) for config content that is too verbose for inline Rust strings.

#### Alloy Sidecar Configuration

Each Tokeira task definition includes an Alloy sidecar. On ECS, the sidecar scrapes the co-located primary container on localhost and ships metrics/logs to Mimir/Loki via Service Connect endpoints. Unlike the Kubernetes sidecar pattern (which uses pod-level service discovery), the ECS sidecar uses a static localhost target.

```
// Alloy sidecar config for tokeira-{service}
// Injected as ALLOY_CONFIG environment variable or mounted config file

prometheus.scrape "tokeira" {
  targets         = [{ __address__ = "localhost:{{ metrics_port }}" }]
  forward_to      = [prometheus.remote_write.mimir.receiver]
  scrape_interval = "15s"
  job_name        = "tokeira-{{ service_name }}"
}

prometheus.remote_write "mimir" {
  endpoint {
    url = "http://mimir.{{ service_connect_namespace }}:9009/api/v1/push"
  }
  external_labels = {
    service_name = "tokeira-{{ service_name }}",
    environment  = "{{ environment }}",
    project      = "{{ project_name }}",
  }
}

loki.source.docker "container" {
  host       = "unix:///var/run/docker.sock"
  targets    = []
  forward_to = [loki.write.default.receiver]
}

loki.write "default" {
  endpoint {
    url = "http://loki.{{ service_connect_namespace }}:3100/loki/api/v1/push"
  }
  external_labels = {
    service_name = "tokeira-{{ service_name }}",
    environment  = "{{ environment }}",
    project      = "{{ project_name }}",
  }
}
```

Key design decisions:
- **Static localhost target** — no service discovery needed. The sidecar always scrapes the co-located primary container.
- **External labels** — `service_name`, `environment`, `project` are injected so Mimir/Loki queries can filter by service and environment.
- **15s scrape interval** — matches the autoscaler polling interval. Faster scraping wastes CPU; slower scraping delays scaling decisions.
- **Docker socket for logs** — on ECS EC2, the Alloy sidecar can read container logs via the Docker socket mounted from the host. This avoids the `awslogs` driver dependency.

#### Mimir Configuration

Mimir runs in single-binary mode with S3 as the long-term storage backend. The config is generated from `EcsConfig` observability settings.

```yaml
multitenancy_enabled: false

server:
  http_listen_port: 9009
  grpc_listen_port: 9095
  log_level: warn

blocks_storage:
  backend: s3
  s3:
    bucket_name: {{ mimir_s3_bucket }}
    region: {{ region }}

distributor:
  ring:
    kvstore:
      store: memberlist

ingester:
  ring:
    kvstore:
      store: memberlist
    replication_factor: 1
  chunk_encoding: snappy

compactor:
  data_dir: /tmp/mimir-compactor
  sharding_ring:
    kvstore:
      store: memberlist
  compaction_interval: 30m

store_gateway:
  sharding_ring:
    replication_factor: 1

limits:
  max_global_series_per_user: 500000
  max_global_series_per_metric: 50000
  ingestion_rate: 100000
  ingestion_burst_size: 200000
```

Key design decisions:
- **S3 backend** — no local filesystem for blocks storage. S3 provides durability and allows Mimir to be stateless (restartable without data loss).
- **Single-binary mode** — all Mimir components (distributor, ingester, compactor, store-gateway) run in one process. Appropriate for the expected metric volume (~10 services × ~500 series each = ~5000 active series).
- **Memberlist for ring coordination** — Mimir components discover each other via memberlist gossip. With a single replica, this is trivially satisfied.
- **Replication factor 1** — single-replica deployment. Acceptable for dev/staging; production would increase to 3 with multiple Mimir instances.
- **500k series limit** — generous for the expected cardinality. Tokeira's per-shard, per-lane, per-class metrics produce ~200 series per runtime node. At 20 runtime nodes, that's ~4000 series.
- **Snappy chunk encoding** — reduces S3 storage cost and network transfer with minimal CPU overhead.

#### Loki Configuration

Loki runs in single-binary mode with S3 as the long-term storage backend. The config is generated from `EcsConfig` observability settings.

```yaml
auth_enabled: false

server:
  http_listen_port: 3100
  grpc_listen_port: 9095
  log_level: warn

schema_config:
  configs:
    - from: "2024-01-01"
      store: tsdb
      object_store: s3
      schema: v13
      index:
        prefix: loki_index_
        period: 24h

storage_config:
  tsdb_shipper:
    active_index_directory: /loki/index
    cache_location: /loki/cache
  aws:
    s3: s3://{{ region }}/{{ loki_s3_bucket }}

common:
  replication_factor: 1
  ring:
    kvstore:
      store: inmemory

limits_config:
  retention_period: {{ retention_days }}d
  ingestion_rate_mb: 10
  ingestion_burst_size_mb: 20
  max_streams_per_user: 10000
  max_entries_limit_per_query: 5000

compactor:
  working_directory: /loki/compactor
  compaction_interval: 10m
  retention_enabled: true
  retention_delete_delay: 2h
  delete_request_store: s3

ingester:
  chunk_encoding: snappy
  chunk_idle_period: 5m
  chunk_target_size: 1536000
  max_chunk_age: 1h
```

Key design decisions:
- **S3 backend with TSDB store** — schema v13 with TSDB is Loki's current recommended storage layout. Index files are shipped to S3 periodically; chunks are written directly to S3.
- **Configurable retention** — `retention_days` from `EcsConfig` (default: 7 days for dev, 30 for production). Compactor enforces retention by deleting expired chunks from S3.
- **10 MB/s ingestion rate** — sufficient for ~10 services generating structured logs. Tokeira uses `tracing` with JSON output, so log lines are compact.
- **Snappy chunk encoding** — same rationale as Mimir: reduces S3 cost with minimal CPU.
- **In-memory ring** — single-replica deployment. No external coordination needed.
- **5-minute chunk idle period** — balances write amplification (fewer S3 PUTs) against query latency for recent logs.

#### Grafana Configuration

Grafana is pre-configured with Mimir and Loki as data sources, and pre-provisioned dashboards.

**Data sources** (generated as a provisioning YAML):

```yaml
apiVersion: 1
datasources:
  - name: Prometheus
    type: prometheus
    uid: mimir
    access: proxy
    url: http://mimir.{{ service_connect_namespace }}:9009/prometheus
    isDefault: true
    editable: true

  - name: Loki
    type: loki
    uid: loki
    access: proxy
    url: http://loki.{{ service_connect_namespace }}:3100
    editable: true
```

**Dashboard provisioning** (generated as a provisioning YAML):

```yaml
apiVersion: 1
providers:
  - name: 'Tokeira'
    orgId: 1
    folder: 'Tokeira'
    folderUid: 'tokeira'
    type: file
    disableDeletion: false
    updateIntervalSeconds: 30
    allowUiUpdates: true
    options:
      path: /var/lib/grafana/dashboards/tokeira
```

**Grafana INI** (generated from config):

```ini
[auth.anonymous]
enabled = false

[security]
; Admin credentials sourced from Secrets Manager via ECS secrets
admin_user = ${GRAFANA_ADMIN_USER}
admin_password = ${GRAFANA_ADMIN_PASSWORD}

[log]
level = warn

[users]
default_theme = dark

[dashboards]
default_home_dashboard_path = /var/lib/grafana/dashboards/tokeira/overview.json
```

Key design decisions:
- **Secrets Manager for admin credentials** — Grafana admin user/password are sourced from Secrets Manager via ECS task definition secrets, not hardcoded in config.
- **Pre-provisioned dashboards** — dashboard JSON files are baked into the container image or mounted from S3. The provisioning YAML tells Grafana where to find them.
- **Service Connect URLs** — data source URLs use the Service Connect namespace (`mimir.tokeira.local`, `loki.tokeira.local`) for stable internal routing.
- **Anonymous auth disabled** — all access requires authentication. Grafana is accessible via internal ALB or port-forward only.

#### Config Generation Pattern

All observability configs are generated programmatically from `EcsConfig` values using Askama templates. The template files live in `platforms/ecs/templates/`:

```
platforms/ecs/templates/
├── alloy-sidecar-config.txt.j2
├── mimir-config.yaml.j2
├── loki-config.yaml.j2
├── grafana-datasources.yaml.j2
├── grafana-dashboards.yaml.j2
└── grafana-ini.txt.j2
```

Each template receives a context struct with the relevant `EcsConfig` fields. The generated config is either:
- Injected as an environment variable (Alloy sidecar — small config)
- Stored in S3 and referenced by the task definition (Mimir, Loki, Grafana — larger configs)

This follows the same pattern as the compose platform's config generation, adapted for ECS's config delivery mechanisms.

### 5. Deployment Trait Implementation (`platforms/ecs/src/lib.rs`)

```rust
pub struct EcsDeployment;

#[async_trait]
impl Deployment for EcsDeployment {
    type Config = EcsConfig;

    fn remote_state_module(&self, config: &Self::Config, deployment_dir: &Path)
        -> Box<dyn iac::Module>
    {
        Box::new(RemoteStateModule {
            bucket_name: format!("{}-state-{}", config.project_name, config.region),
            region: config.region.clone(),
            key_prefix: format!("{}/{}", config.project_name, config.environment),
        })
    }

    fn infra_modules(&self, config: &Self::Config, selection: &iac::ModuleSelection)
        -> Vec<Box<dyn iac::Module>>
    {
        let mut modules: Vec<Box<dyn iac::Module>> = Vec::new();
        let networking = NetworkingModule::new(config);
        let dsql = DsqlModule::new(config);
        let cluster = ClusterModule::new(config);
        let observability = ObservabilityModule::new(config);
        let services = ServicesModule::new(config);
        if selection.includes(networking.name()) { modules.push(Box::new(networking)); }
        if selection.includes(dsql.name()) { modules.push(Box::new(dsql)); }
        if selection.includes(cluster.name()) { modules.push(Box::new(cluster)); }
        if selection.includes(observability.name()) { modules.push(Box::new(observability)); }
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
        // Primary: query Loki via HTTP API for the service label
        // Fallback: retrieve logs from ECS tasks directly (if Loki unavailable)
    }

    async fn port_mappings(&self, service: &str, config: &Self::Config) -> Result<Vec<PortMapping>> {
        // ECS services use internal networking; return internal endpoints
    }
}
```

### 6a. Break-Glass Debug Logging

CloudWatch Logs is available only as an operator-selected action, not as a default log destination. The normal log flow is Alloy → Loki.

```rust
/// Enable CloudWatch Logs on all task definitions as a break-glass debug action.
/// Shows the operator what will change and requires confirmation.
pub async fn enable_debug_logs(config: &EcsConfig, actuator: &AwsActuator) -> Result<()> {
    // 1. For each active task definition, register a new revision with
    //    awslogs log driver added alongside the existing Alloy sidecar
    // 2. Update each ECS service to use the new task definition revision
    //    (triggers rolling deployment)
    // Requires cloudwatch_logs VPC endpoint to be provisioned
}

/// Disable CloudWatch Logs by reverting task definitions to Alloy-only.
pub async fn disable_debug_logs(config: &EcsConfig, actuator: &AwsActuator) -> Result<()> {
    // 1. Register new task definition revisions without the awslogs driver
    // 2. Update each ECS service to use the reverted task definition
}
```

### 6b. Zero-Replica Staged Deployment

Services deploy at 0 replicas to avoid crash-loops before schema exists:

```
tkr infra apply          # Provision VPC, DSQL, ECS cluster, ASGs
tkr deploy apply         # Create ECS services at desired_count=0
tkr schema setup         # Run DSQL migrations
tkr scale up             # Scale services in startup order:
                         #   runtime → controller → edge-api → edge-poll
                         #   → projection → autoscaler
                         # Each service waits for ready state before next
```

The `scale up` command reads the configured desired counts from `EcsConfig.services` and applies them in dependency order. Each service is scaled and then polled until ECS reports the desired number of running tasks with passing health checks.

## Data Models

### ECS Platform Configuration

| Section | Key Fields | Default |
|---|---|---|
| `tags` | Operator-defined custom tags | `{}` (empty — auto-generated tags always applied) |
| `cluster` | `name`, `service_connect_namespace` | `tokeira-dev`, `tokeira.local` |
| `networking` | `vpc_id`, `private_subnet_ids`, `availability_zones`, `private_dns_zone`, `optional_endpoints` | Must be provided |
| `dsql` | `endpoint`, `runtime_role_arn`, `admin_role_arn` | Must be provided |
| `capacity_providers.edge_api` | `instance_type`, `min/max/desired_capacity` | `c7g.large`, 1/10/2 |
| `capacity_providers.edge_poll` | `instance_type`, `min/max/desired_capacity` | `r7g.large`, 1/10/2 |
| `capacity_providers.runtime` | `instance_type`, `min/max/desired_capacity`, `scale_in_protection` | `c7g.xlarge`, 1/20/2, true |
| `capacity_providers.projection` | `instance_type`, `min/max/desired_capacity` | `c7g.large`, 1/10/1 |
| `capacity_providers.control` | `instance_type`, `min/max/desired_capacity` | `t4g.medium`, 1/3/1 |
| `autoscaler` | `polling_interval_secs`, `scale_out/in_consecutive_samples`, `cooldown_secs`, `mimir_endpoint`, `staleness_threshold_secs`, `dsql_connection_budget`, `per_runtime_reserved_connections` | 15s, 2/8, 120s, 8000, 200 |
| `alb` | `name`, `health_check_path`, `health_check_interval_secs` | `/health`, 10s |
| `observability` | `mimir_image`, `loki_image`, `grafana_image`, `alloy_sidecar_image`, S3 buckets | Pinned versions from compose platform |

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
