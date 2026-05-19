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
│   Application plane                    Observability plane       Control     │
│  ┌─────────────┐  ┌──────────────┐  ┌────────────┐  ┌────────┐ ┌────────┐   │
│  │ cp-edge-api │  │ cp-edge-poll │  │ cp-runtime │  │cp-proj │ │cp-ctrl │   │
│  │   (c8g)     │  │   (r8g)      │  │   (c8g)    │  │ (c8g)  │ │(c8g)   │   │
│  │ edge-api    │  │ edge-poll    │  │ runtime    │  │ proj   │ │ctrl×2  │   │
│  │ (REPLICA)   │  │ (REPLICA)    │  │ (DAEMON)   │  │(REPL)  │ │autos×2 │   │
│  │ +alloy      │  │ +alloy       │  │ +alloy     │  │+alloy  │ │admin×0 │   │
│  └──────┬──────┘  └──────┬───────┘  └──────┬─────┘  └────────┘ │+alloy  │   │
│         │                │                 │                   └────────┘   │
│  ┌──────┴────────────────┴──────┐          │                                │
│  │   Internal ALB (gRPC HTTP/2) │          │      Observability plane       │
│  │   edge-api.<zone>            │          │  ┌────────┐ ┌────────┐ ┌─────┐ │
│  │   edge-poll.<zone>           │          │  │cp-mimir│ │cp-loki │ │cp-  │ │
│  └──────────────────────────────┘          │  │ (r8g)  │ │ (r8g)  │ │graf │ │
│                                             │  │ mimir  │ │ loki   │ │(c8g)│ │
│                                             │  │+alloy  │ │+alloy  │ │graf │ │
│                                             │  │        │ │        │ │+all │ │
│                                             │  └────────┘ └────────┘ └─────┘ │
│                                             │                                │
│  ┌──────────────────────────────────────────┴───────────────────────────────┐│
│  │                    Service Connect Namespace                              ││
│  │  controller  autoscaler  projection  mimir  loki  grafana                 ││
│  └───────────────────────────────────────────────────────────────────────────┘│
│                                                                              │
│  ┌──────────────────────────────────────────────────────────────────────────┐│
│  │                    VPC Endpoints (private connectivity)                   ││
│  │  ECS(3) ECR(2) S3(gw) AutoScaling CloudMap DSQL(2) SSM(3) [opt: CWL...]  ││
│  └──────────────────────────────────────────────────────────────────────────┘│
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

All EC2 instance types default to Graviton4 (c8g/r8g family) for best price-performance. Graviton4 is based on Neoverse V2 cores and is supported end-to-end by Amazon Linux 2023 on arm64. Regions where c8g/r8g are not yet generally available can override the instance type in config to Graviton3 (c7g/r7g).

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
- **cluster**: ECS cluster, 8 capacity providers (application plane: edge-api, edge-poll, runtime, projection, control; observability plane: mimir, loki, grafana), 8 ASGs, launch templates, IAM instance profiles
- **observability**: Mimir, Loki, Grafana ECS services, S3 buckets for metrics/log storage, IAM roles
- **services**: 7 Tokeira ECS service definitions, 7 task definitions (each with an `alloy-config-init` init container, Alloy sidecar, Docker socket host-volume mount, and no primary-container `logConfiguration`), Service Connect config, Cloud Map namespace. Depends on observability because Alloy sidecars need Mimir/Loki endpoints.

## Components and Interfaces

### 1. Progress Reporting and CLI UX — Consumed from `iac-resource-lifecycle`

The `ProvisionContext` progress callbacks (`set_apply_progress`, `set_complete_progress`, `set_failed_progress`, `set_wait_progress`, `set_note_progress`), the `ActionTuiHandle` implementation, the `OutputFormat` / `ProgressEvent` types, the JSON event schema, and the `--json` flag threading are defined by the [`iac-resource-lifecycle`](../iac-resource-lifecycle/design.md) spec. This ECS spec consumes them.

Notably:
- `set_apply_progress` takes three arguments (`action`, `resource_id`, `resource_type`) — not five. `index`/`total` counters belong on the CLI side (`ActionCounters`), not on the engine callback signature.
- `skipped` is derived from `ChangeKind::NoChange` plan entries, not a callback.
- `ActionTuiHandle` uses the `ActiveSpinners` map pattern (per-resource `SpinnerEntry` keyed by `ResourceId`), not a single `overall` + `detail` pair.
- The test-only `with_terminal_detected(format, is_terminal)` constructor is the deterministic injection seam for unit tests.

New IaC resources in this spec call `ctx.emit_apply_progress` at the start of their lifecycle methods, `ctx.emit_wait_progress` during polling waits (for example while waiting for a DSQL endpoint to become available), and `ctx.emit_note_progress` for informational events (for example "adopting existing DSQL cluster"). They call `ctx.emit_complete_progress` / `ctx.emit_failed_progress` on return via the engine's generic `apply_changes` / `destroy_changes` machinery — resource implementations do not call these directly.

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
    pub observability: ObservabilityStackConfig,
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
    pub cloudwatch_logs: bool,
    pub ec2: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DsqlConfig {
    /// Lifecycle mode: `Managed` (create and own the cluster) or
    /// `Preexisting` (adopt a pre-existing cluster, PrivateLink endpoints,
    /// and IAM roles). Defaults to `Managed`.
    pub mode: DsqlClusterMode,
    /// DSQL cluster endpoint (e.g., "cluster.dsql.us-east-1.on.aws").
    ///
    /// For `Managed`: initially empty or placeholder; populated by post-apply
    /// writeback with the discovered endpoint.
    /// For `Preexisting`: required — operator must supply the endpoint.
    pub endpoint: Option<String>,
    /// PrivateLink management endpoint ID.
    /// Required for `Preexisting`; populated by writeback for `Managed`.
    pub management_endpoint_id: Option<String>,
    /// PrivateLink connection endpoint ID.
    /// Required for `Preexisting`; populated by writeback for `Managed`.
    pub connection_endpoint_id: Option<String>,
    /// IAM role ARN for runtime DSQL access.
    /// Required for `Preexisting`; populated by writeback for `Managed`.
    pub runtime_role_arn: Option<String>,
    /// IAM role ARN for admin/migration DSQL access.
    /// Required for `Preexisting`; populated by writeback for `Managed`.
    pub admin_role_arn: Option<String>,
}

/// DSQL cluster lifecycle mode. Follows the `effective_managed` convention
/// from the `iac-resource-lifecycle` spec so a cluster originally created
/// as `Managed` is still deleted on destroy even if config is later
/// changed to `Preexisting`.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum DsqlClusterMode {
    /// Create and own the cluster lifecycle. Endpoint, endpoint IDs, and
    /// role ARNs are discovered after apply and written back to config.
    #[default]
    Managed,
    /// Adopt an existing cluster. Operator must supply endpoint,
    /// endpoint IDs, and role ARNs. Module never creates or deletes.
    Preexisting,
}

impl DsqlConfig {
    /// Validate that Preexisting mode has all required fields populated.
    /// Called during config loading; produces a descriptive error naming
    /// the first missing field.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.mode == DsqlClusterMode::Preexisting {
            if self.endpoint.is_none() {
                return Err(ConfigError::missing("dsql.endpoint"));
            }
            if self.management_endpoint_id.is_none() {
                return Err(ConfigError::missing("dsql.management_endpoint_id"));
            }
            if self.connection_endpoint_id.is_none() {
                return Err(ConfigError::missing("dsql.connection_endpoint_id"));
            }
            if self.runtime_role_arn.is_none() {
                return Err(ConfigError::missing("dsql.runtime_role_arn"));
            }
            if self.admin_role_arn.is_none() {
                return Err(ConfigError::missing("dsql.admin_role_arn"));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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
    /// Retention for both Mimir metrics and Loki logs. Default 30 days.
    /// Mimir's compactor and Loki's compactor both respect this value.
    pub retention_days: u32,
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
                    instance_type: "c8g.large".into(),  // Graviton4, 2 vCPU, 4 GiB
                    min_capacity: 1, max_capacity: 10, desired_capacity: 2,
                },
                edge_poll: CapacityProviderConfig {
                    instance_type: "r8g.large".into(),  // Graviton4, 2 vCPU, 16 GiB
                    min_capacity: 1, max_capacity: 10, desired_capacity: 2,
                },
                runtime: RuntimeCapacityProviderConfig {
                    instance_type: "c8g.xlarge".into(), // Graviton4, 4 vCPU, 8 GiB
                    min_capacity: 1, max_capacity: 20, desired_capacity: 2,
                    scale_in_protection: true,
                },
                projection: CapacityProviderConfig {
                    instance_type: "c8g.large".into(),  // Graviton4, 2 vCPU, 4 GiB
                    min_capacity: 1, max_capacity: 10, desired_capacity: 1,
                },
                control: CapacityProviderConfig {
                    instance_type: "c8g.large".into(),  // Graviton4, 2 vCPU, 4 GiB
                    min_capacity: 1, max_capacity: 3, desired_capacity: 1,
                },
                // Observability services — each on its own single-host CP.
                // max=1 so tkr port-forward always targets the unique host.
                mimir: CapacityProviderConfig {
                    instance_type: "r8g.large".into(),  // Graviton4, 2 vCPU, 16 GiB
                    min_capacity: 1, max_capacity: 1, desired_capacity: 1,
                },
                loki: CapacityProviderConfig {
                    instance_type: "r8g.large".into(),  // Graviton4, 2 vCPU, 16 GiB
                    min_capacity: 1, max_capacity: 1, desired_capacity: 1,
                },
                grafana: CapacityProviderConfig {
                    instance_type: "c8g.large".into(),  // Graviton4, 2 vCPU, 4 GiB
                    min_capacity: 1, max_capacity: 1, desired_capacity: 1,
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
            observability: ObservabilityStackConfig {
                mimir_image: "grafana/mimir:3.0.6".into(),
                mimir_cpu: 1536,
                mimir_memory_mb: 12_288,
                loki_image: "grafana/loki:3.7.1".into(),
                loki_cpu: 1024,
                loki_memory_mb: 12_288,
                grafana_image: "grafana/grafana-oss:12.4.3".into(),
                grafana_cpu: 1024,
                grafana_memory_mb: 2048,
                alloy_sidecar_image: "grafana/alloy:v1.16.0".into(),
                alloy_sidecar_cpu: 128,
                alloy_sidecar_memory_mb: 256,
                mimir_s3_bucket: "tokeira-dev-mimir".into(),
                loki_s3_bucket: "tokeira-dev-loki".into(),
                retention_days: 30,
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

Every `Resource` implementation passes `resource_tags(config, name)` to the AWS SDK create/update calls. This applies to VPC resources, security groups, VPC endpoints, ALB, ECS cluster, capacity providers, ASGs, launch templates, IAM roles, S3 buckets, DSQL cluster, Cloud Map namespace, ECS services/task definitions, and optional CloudWatch log groups created by break-glass debug logging.

#### ECS Task CPU/Memory Validation

ECS enforces a discrete matrix of valid task-level CPU/memory pairs. Invalid combinations are rejected at `RegisterTaskDefinition` time; we catch them at TOML parse time instead so operators find the problem during `tkr init` / `tkr infra plan`:

```rust
/// Validate an ECS task's (cpu, memory_mb) pair against the ECS matrix.
/// Returns Ok if the pair is valid; Err with the nearest valid pairs otherwise.
pub fn validate_cpu_memory(cpu: u32, memory_mb: u32) -> Result<(), ConfigError> {
    // ECS task CPU values (CPU units; 1024 = 1 vCPU)
    // and the inclusive memory ranges + stride they support.
    const MATRIX: &[(u32, u32, u32, u32)] = &[
        // (cpu, min_memory_mb, max_memory_mb, stride_mb)
        (256,    512,   2048, 512),
        (512,   1024,   4096, 1024),
        (1024,  2048,   8192, 1024),
        (2048,  4096,  16384, 1024),
        (4096,  8192,  30720, 1024),
        (8192, 16384,  61440, 4096),  // Linux on EC2 only
        (16384, 32768, 122880, 8192), // Linux on EC2 only
    ];

    let Some(row) = MATRIX.iter().find(|(c, ..)| *c == cpu) else {
        return Err(ConfigError::invalid_cpu(cpu, MATRIX));
    };
    let (_, min_mem, max_mem, stride) = row;
    if memory_mb < *min_mem || memory_mb > *max_mem {
        return Err(ConfigError::memory_out_of_range(cpu, memory_mb, *min_mem, *max_mem));
    }
    if (memory_mb - min_mem) % stride != 0 {
        return Err(ConfigError::memory_not_on_stride(cpu, memory_mb, *stride));
    }
    Ok(())
}
```

`EcsConfig::validate()` calls `validate_cpu_memory` for every service (`edge_api`, `edge_poll`, `runtime`, `projection`, `controller`, `autoscaler`, `admin`) plus Mimir, Loki, Grafana. Error messages cite the invalid pair and the nearest valid pairs so operators can correct without consulting AWS docs:

```
ecs service `tokeira-runtime` has invalid cpu/memory pair: cpu=3584, memory=6656
  cpu=3584 is not a valid ECS CPU value
  valid CPU values: 256, 512, 1024, 2048, 4096, 8192, 16384
  nearest valid pairs: (2048, 4096..=16384), (4096, 8192..=30720)
```

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
| `ssm`, `ssmmessages`, `ec2messages` | Interface | 3 |
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
        // DSQL PrivateLink endpoints for private-only management and connection traffic.
        // Resource state exposes properties["endpoint_id"] for config hydration.
        resources.push(Box::new(DsqlPrivatelinkEndpointResource { /* management */ }));
        resources.push(Box::new(DsqlPrivatelinkEndpointResource { /* connection */ }));
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
    fn dependencies(&self) -> &[&str] { &["dsql"] }
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
    pub service_connect: ServiceConnectSpec,
    pub placement_constraints: Vec<PlacementConstraint>,
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
    pub init_containers: Vec<InitContainerSpec>,
    pub init_process_enabled: bool, // set true on the primary container for ECS Exec
}

/// Init container that blocks the primary container's start until a
/// dependency's Service Connect endpoint accepts a TCP connection.
#[derive(Debug, Clone)]
pub struct InitContainerSpec {
    pub name: String,            // e.g., "wait-for-tokeira-controller"
    pub image: String,           // "public.ecr.aws/docker/library/busybox:latest"
    pub command: Vec<String>,    // ["sh", "-c", "until nc -z <dep> <port>; do sleep 2; done"]
    pub cpu: u32,                // 32
    pub memory_mb: u32,          // 64
    pub essential: bool,         // false — init containers must exit to let the task run
}

/// Service Connect service registrations. Each service registers both its
/// primary gRPC port (where applicable) and its Prometheus metrics port.
/// Metrics aliases are for diagnostics; Alloy sidecars scrape localhost.
#[derive(Debug, Clone, Default)]
pub struct ServiceConnectSpec {
    pub grpc: Option<ServiceConnectPort>,     // discovery name "<service>"
    pub metrics: Option<ServiceConnectPort>,  // discovery name "<service>-metrics"
}

#[derive(Debug, Clone)]
pub struct ServiceConnectPort {
    pub port_name: String,       // "grpc" or "metrics"
    pub container_port: u16,
    pub discovery_name: String,  // e.g., "tokeira-runtime-metrics"
    pub dns_name: String,        // e.g., "tokeira-runtime-metrics"
}

impl deploy_engine::Service for EcsWorkload {
    fn name(&self) -> &str { &self.name }
    fn module(&self) -> &str { "services" }
    fn dependencies(&self) -> &[&str] {
        match self.name.as_str() {
            "tokeira-edge-api" | "tokeira-edge-poll" => &["tokeira-runtime", "tokeira-controller"],
            "tokeira-runtime" => &["tokeira-controller"],
            "tokeira-projection" => &["tokeira-runtime"],
            // Autoscaler needs Mimir to query metrics and controller for nominate/drain APIs
            "tokeira-autoscaler" => &["tokeira-controller", "tokeira-mimir"],
            // Grafana depends on both data sources
            "tokeira-grafana" => &["tokeira-mimir", "tokeira-loki"],
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

**Wait-for init containers** — `EcsWorkload::build` synthesises one `wait-for-<dep>` init container per upstream dependency from the `dependencies()` list. Each primary container declares a `dependsOn` for its matching init containers. The init image is the public busybox, the command polls the dependency's Service Connect endpoint every 2 seconds until TCP connect succeeds, and CPU/memory reservations (32/64) are deducted from the primary container's budget.

**Service Connect metrics registration** — `ServiceConnectSpec::metrics` registers every service's Prometheus endpoint on Service Connect. This gives operators stable DNS targets (for example `tokeira-runtime-metrics.tokeira.local:9090`) for ad-hoc diagnostics from inside the cluster. It is not the metrics ingestion path: each Alloy sidecar scrapes its own task on `localhost:{metrics_port}` and remote-writes to Mimir.

**ECS Exec readiness** — every `EcsWorkload` sets `enable_execute_command = true` on the service and `linuxParameters.initProcessEnabled = true` on the primary container. See §3d for the cluster-level logging configuration and task role IAM.

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
| `tokeira-mimir` | REPLICA | `cp-mimir` | 1 | Service Connect (`mimir.tokeira.local`) |
| `tokeira-loki` | REPLICA | `cp-loki` | 1 | Service Connect (`loki.tokeira.local`) |
| `tokeira-grafana` | REPLICA | `cp-grafana` | 1 | `tkr port-forward grafana` (no ALB registration) |

### 3a. Infrastructure and Service Sizing Rationale

This section documents the reasoning behind instance types, task resource limits, and capacity provider defaults for each service plane. All instance types are Graviton (ARM64) for cost efficiency. Resource limits are set as requests = limits for guaranteed QoS — no overcommit.

All capacity providers default to Graviton4 (c8g/r8g family) instance types. Graviton4 is Neoverse V2-based and is supported end-to-end by Amazon Linux 2023 arm64. Regions without c8g/r8g availability can override to Graviton3 (c7g/r7g) via config.

#### Capacity Provider: `cp-edge-api`

| Setting | Value | Rationale |
|---|---|---|
| Instance type | `c8g.large` (2 vCPU, 4 GiB, Graviton4) | Edge-api is CPU-bound: gRPC deserialization, routing lookup, request forwarding. No DSQL connections. Memory demand is low — routing cache is a small `ArcSwap<RoutingSnapshot>`. |
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
| Instance type | `r8g.large` (2 vCPU, 16 GiB, Graviton4) | Edge-poll is memory-bound: each long-poll holds a gRPC stream and a broker subscription. At 1000 concurrent polls × ~16 KiB per poll context, memory dominates. `r8g` (memory-optimized Graviton4) is the right family. |
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
| Instance type | `c8g.xlarge` (4 vCPU, 8 GiB, Graviton4) | Runtime is CPU-bound: kernel transition evaluation, history serialization, DSQL transaction preparation. 4 vCPU supports ~4 lanes of concurrent transition processing. Memory holds the DSQL connection reservoir, shard owner state, and in-flight actor state. |
| Min/Max/Desired | 1 / 20 / 2 | Two instances for initial bundle distribution. Max 20 supports ~2000 WPS at 100 WPS/node. Scale-in protection enabled. |
| DAEMON scheduling | One task per host | Runtime owns bundles and manages shard-local lanes. The entire host's resources are dedicated to the single runtime process. Scaling the runtime fleet means scaling the ASG. |

| Task resource | Value | Rationale |
|---|---|---|
| Task CPU | 4096 (4 vCPU) | Matches `c8g.xlarge`. Valid ECS CPU tier. |
| Task Memory | 8192 MiB (8 GiB) | Smallest valid memory for `cpu=4096` (range 8192–30720 MiB). The task claims the host; the ECS agent runs outside the task so the full 8 GiB is available. |
| Primary container CPU | 3712 | 4096 − 256 (Alloy sidecar) − 64 × 2 (two wait-for init containers, which exit before the primary runs but still count at registration). Set as `reservation`; the container may burst up to the task CPU. |
| Primary container memory | 7424 MiB | 8192 − 512 (Alloy) − 64 × 4 (conservative init allowance). Holds DSQL reservoir, lane actor state, history buffers, gRPC server buffers. |
| Alloy sidecar | 256 CPU / 512 MiB | Runtime generates more metrics (per-shard, per-lane, per-class) and more log volume than edge. Larger sidecar allocation. |
| Wait-for init containers | 32 CPU / 64 MiB each | Up to two per task (controller, projection). `essential = false`; exit before primary starts. |
| Ports | gRPC 7235 (internal), metrics 9090 | Internal-only gRPC for edge→runtime forwarding. No ALB registration. |
| DSQL connections | 32 per node (see [060-connection-management](../../../docs/architecture/060-connection-management.md#connection-demand-analysis)) | Control: 2–3, Commit: 15, Read: 10, Projection: 3, Maintenance: 2. |

#### Capacity Provider: `cp-projection`

| Setting | Value | Rationale |
|---|---|---|
| Instance type | `c8g.large` (2 vCPU, 4 GiB, Graviton4) | Projection workers are CPU-bound: deserializing postcard-encoded projection ops, computing search attribute indexes, writing to DSQL visibility tables. Memory demand is moderate — batch buffers and DSQL connection pool. |
| Min/Max/Desired | 1 / 10 / 1 | One instance is sufficient for low-to-moderate WPS. Scales with projection lag. |

| Task resource | Value | Rationale |
|---|---|---|
| CPU | 1024 (1 vCPU) | Projection workers process batches sequentially per partition. One vCPU handles the decode→transform→write pipeline. |
| Memory | 2048 MiB | Batch buffers, DSQL connection pool, search attribute index state. |
| Alloy sidecar | 128 CPU / 256 MiB | Standard sidecar. |
| Ports | metrics 9090 | No gRPC ingress — projection workers pull from the projection log. |
| DSQL connections | 5 per task | Projection class only. Reads from projection_log, writes to visibility tables. |

#### Capacity Provider: `cp-control`

| Setting | Value | Rationale |
|---|---|---|
| Instance type | `c8g.large` (2 vCPU, 4 GiB, Graviton4) | Control-plane services (controller, autoscaler, admin) are lightweight but not burstable — they run continuous loops (snapshot computation every few seconds, autoscaler loops at 15s cadence). A standard (non-burstable) Graviton4 `c8g.large` avoids CPU credit exhaustion. |
| Min/Max/Desired | 1 / 3 / 1 | One instance hosts controller×2, autoscaler×2, admin×0. Max 3 for HA and rolling updates. |

Services on `cp-control`:

| Service | CPU | Memory | Rationale |
|---|---|---|---|
| `tokeira-controller` (×2) | 256 | 512 MiB | Lightweight: reads DSQL leases, computes snapshots, streams to subscribers. Two replicas for HA. |
| `tokeira-autoscaler` (×2) | 256 | 512 MiB | Lightweight: queries Mimir, computes scaling decisions, calls AWS APIs. Two replicas with leader lease. |
| `tokeira-admin` (×0) | 256 | 512 MiB | On-demand only. Schema migrations and diagnostics. |
| Alloy sidecars (×4) | 64 each | 128 MiB each | 1 per live task. |

Steady-state demand at default counts: controller×2 + autoscaler×2 + 4 sidecars = 4×256 + 4×64 = 1280 CPU units (1.25 vCPU), 4×512 + 4×128 = 2560 MiB (2.5 GiB). Leaves ~750 CPU units and ~1.5 GiB for the ECS agent, SSM agent, and OS. A second `c8g.large` is used only during rolling updates.

#### Capacity Provider: `cp-mimir` (Observability — dedicated node)

| Setting | Value | Rationale |
|---|---|---|
| Instance type | `r8g.large` (2 vCPU, 16 GiB, Graviton4) | Mimir ingests metrics from every Alloy sidecar in the cluster and is both memory-bound (in-memory active series index, WAL, compactor state) and bursty on query. Memory-optimised Graviton4 gives Mimir the headroom to hold active-series indexes without swapping. |
| Min/Max/Desired | 1 / 2 / 1 | One dedicated host. Max 2 only during rolling replacement. |

| Task resource | Value | Rationale |
|---|---|---|
| CPU | 1536 (1.5 vCPU) | Mimir single-binary mode runs distributor, ingester, compactor, store-gateway, ruler, and querier in one process. 1.5 vCPU handles ~10k samples/sec ingest plus concurrent PromQL queries. |
| Memory | 12 288 MiB (12 GiB) | Active series (~5k series × 3 KiB per series head chunk), WAL write buffers, query working set, compactor block assembly. Leaves ~4 GiB for the host, Alloy sidecar, and ECS agent. |
| Alloy sidecar | 128 CPU / 256 MiB | Scrapes Mimir's own metrics endpoint. |
| Ports | HTTP 9009, gRPC 9095 | Standard Mimir ports. Registered in Service Connect as `mimir.tokeira.local:9009`. |

#### Capacity Provider: `cp-loki` (Observability — dedicated node)

| Setting | Value | Rationale |
|---|---|---|
| Instance type | `r8g.large` (2 vCPU, 16 GiB, Graviton4) | Loki ingests log streams from every task in the cluster. The TSDB index shipper, in-memory chunk cache, and query frontend are memory-hungry. Memory-optimised Graviton4 matches the workload shape. |
| Min/Max/Desired | 1 / 2 / 1 | One dedicated host. Max 2 during rolling replacement. |

| Task resource | Value | Rationale |
|---|---|---|
| CPU | 1024 (1 vCPU) | Loki ingest is I/O-bound (compressed chunks written to S3). Query path can spike but is rare at this scale. |
| Memory | 12 288 MiB (12 GiB) | Index cache, chunk cache, WAL buffers, compactor working set. Retention-driven compaction runs hourly and needs headroom. |
| Alloy sidecar | 128 CPU / 256 MiB | Scrapes Loki's own metrics endpoint. |
| Ports | HTTP 3100, gRPC 9095 | Standard Loki ports. Registered in Service Connect as `loki.tokeira.local:3100`. |

#### Capacity Provider: `cp-grafana` (Observability — dedicated node)

| Setting | Value | Rationale |
|---|---|---|
| Instance type | `c8g.large` (2 vCPU, 4 GiB, Graviton4) | Grafana is CPU-bound on dashboard render and PromQL proxy. Memory demand is modest — dashboard JSON is small and query results stream through. |
| Min/Max/Desired | 1 / 2 / 1 | One dedicated host. Max 2 during rolling replacement. |

| Task resource | Value | Rationale |
|---|---|---|
| CPU | 1024 (1 vCPU) | Handles ~10 concurrent dashboard users comfortably. |
| Memory | 2048 MiB (2 GiB) | Dashboard cache, query result buffering, provisioning state. |
| Alloy sidecar | 64 CPU / 128 MiB | Small sidecar — Grafana has few internal metrics. |
| Ports | HTTP 3000 | Grafana UI. Reached via `tkr port-forward grafana` (SSM Session Manager) — not registered with the ALB. |

#### Alloy Sidecar Sizing

Every task definition includes an Alloy sidecar. The sidecar's resource allocation varies by service:

| Service plane | Sidecar CPU | Sidecar Memory | Rationale |
|---|---|---|---|
| Edge (api/poll) | 128 | 256 MiB | Low metric cardinality, moderate log volume. |
| Runtime | 256 | 512 MiB | High metric cardinality (per-shard, per-lane, per-class), high log volume during transitions. |
| Projection | 128 | 256 MiB | Low metric cardinality, low log volume. |
| Control plane | 64 | 128 MiB | Minimal metrics and logs. |
| Observability (mimir/loki) | 128 | 256 MiB | Standard — monitors the observability service's own metrics. |
| Observability (grafana) | 64 | 128 MiB | Minimal — Grafana exposes few metrics. |

#### Why Graviton4 (ARM64)

All instance types default to Graviton4 processors (`c8g`, `r8g`):
- Neoverse V2 cores; up to 40% better price-performance than Graviton3 on memory-heavy workloads (Mimir, Loki) and ~30% on CPU-heavy workloads (edge, runtime, controller)
- Supported by Amazon Linux 2023 arm64 end-to-end; the ECS-optimised AMI path is `/aws/service/ecs/optimized-ami/amazon-linux-2023/arm64/recommended/image_id` (no separate Graviton4 variant — the same AMI runs on all current Graviton generations because AL2023 kernel 6.1 includes Neoverse V2 support)
- Tokeira is pure Rust compiled for `aarch64-unknown-linux-gnu` — no x86 dependencies
- Alloy, Mimir, Loki, and Grafana all publish ARM64 container images
- Regions where c8g/r8g are not yet generally available can override to Graviton3 via config

### 3b. Launch Template AMI Resolution

The `LaunchTemplateResource` does not embed an AMI ID. At apply time it resolves the current Amazon Linux 2023 arm64 ECS-optimised AMI from SSM:

```rust
async fn resolve_ecs_optimized_ami(
    ssm: &aws_sdk_ssm::Client,
    region: &str,
) -> Result<String, IacError> {
    let parameter = "/aws/service/ecs/optimized-ami/amazon-linux-2023/arm64/recommended/image_id";
    let out = ssm.get_parameter().name(parameter).send().await?;
    out.parameter
        .and_then(|p| p.value)
        .ok_or_else(|| IacError::Other(anyhow::anyhow!(
            "ECS-optimised AMI parameter missing value in region {region}"
        )))
}
```

This lets the launch template pick up AMI refreshes without requiring the operator to update config. The resolved AMI ID is persisted in `ResourceState.properties["ami_id"]` so `diff()` can detect when AWS publishes a new image and present it as an update.

### 3c. Launch Template User Data and Workload Attributes

Each launch template renders a user-data script that tags the instance with a workload attribute. ECS task placement constraints can then filter by workload, which makes the scheduling invariant explicit on the service side rather than relying on the capacity-provider binding alone:

```bash
#!/bin/bash
echo "ECS_CLUSTER=${cluster_name}" >> /etc/ecs/ecs.config
echo "ECS_ENABLE_CONTAINER_METADATA=true" >> /etc/ecs/ecs.config
echo "ECS_ENABLE_SPOT_INSTANCE_DRAINING=true" >> /etc/ecs/ecs.config
echo "ECS_IMAGE_PULL_BEHAVIOR=always" >> /etc/ecs/ecs.config
echo 'ECS_INSTANCE_ATTRIBUTES={"workload": "${plane}"}' >> /etc/ecs/ecs.config
```

Where `${plane}` is one of `edge-api`, `edge-poll`, `runtime`, `projection`, `control`, `mimir`, `loki`, `grafana`. Each ECS service then declares a matching placement constraint:

```rust
service.placement_constraints = Some(vec![
    PlacementConstraint {
        r#type: "memberOf".into(),
        expression: Some(format!("attribute:workload == {plane}")),
    },
]);
```

This is belt-and-braces — the capacity provider strategy already binds services to the right ASG — but the attribute makes the binding discoverable at placement time and allows `ecs:RunTask` ad-hoc invocations (for example the admin service) to land on the right plane.

### 3d. ECS Exec (`ecs exec`)

ECS Exec is configured at four levels:

**1. Cluster level.** The `EcsClusterResource` configures `execute_command_configuration` with `logging = "NONE"`. This keeps the core exec path dependent only on Session Manager endpoints (`ssm`, `ssmmessages`, `ec2messages`). IAM and CloudTrail provide operator attribution.

```rust
let cluster = ecs.create_cluster()
    .cluster_name(&cluster_name)
    .configuration(ClusterConfiguration::builder()
        .execute_command_configuration(
            ExecuteCommandConfiguration::builder()
                .logging(ExecuteCommandLogging::None)
                .build())
        .build())
    .service_connect_defaults(ClusterServiceConnectDefaultsRequest::builder()
        .namespace(namespace_arn)
        .build())
    .send()
    .await?;
```

**2. Service level.** Every ECS service sets `enable_execute_command = true`. There is no cluster-wide toggle; the per-service flag is mandatory. The design's `EcsWorkload` always sets this to `true`.

**3. Task definition level.** Every primary container declares `linuxParameters.initProcessEnabled = true` so exec sessions can end cleanly without leaving zombie processes:

```rust
ContainerDefinition {
    name: primary_container_name.clone(),
    // ...
    linux_parameters: Some(LinuxParameters {
        init_process_enabled: Some(true),
        ..Default::default()
    }),
}
```

**4. IAM task role level.** The four SSM Messages actions are granted on the task role (not the execution role):

```rust
let policy = json!({
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "ssmmessages:CreateControlChannel",
                "ssmmessages:CreateDataChannel",
                "ssmmessages:OpenControlChannel",
                "ssmmessages:OpenDataChannel",
            ],
            "Resource": "*"
        }
    ]
});
```

**Operator access — `tkr exec`.** The CLI wraps all of this behind a single command:

```rust
/// Open an interactive ECS Exec session against a running task.
/// Discovers a task via ListTasks/DescribeTasks, calls ExecuteCommand,
/// hands off to session-manager-plugin for the data-plane.
pub async fn exec_ecs(
    service: &str,
    container: Option<&str>,
    cmd: &[String],
    config: &EcsConfig,
    ecs: &aws_sdk_ecs::Client,
) -> Result<()> {
    // 1. Resolve the container name. If omitted, default to the primary
    //    container for this service (e.g. "tokeira-runtime" for runtime,
    //    "tokeira-mimir" for mimir). Never default to the Alloy sidecar.
    let container = container.unwrap_or(&primary_container_name_for(service));

    // 2. Find a running task.
    let tasks = ecs.list_tasks()
        .cluster(&config.cluster.name)
        .service_name(&full_service_name(service, config))
        .desired_status(DesiredStatus::Running)
        .send()
        .await?;
    let task_arn = tasks.task_arns().first()
        .ok_or_else(|| anyhow::anyhow!(
            "no running tasks for {service}; run `tkr scale up` or check service status"
        ))?;

    // 3. Request an exec session. AWS returns a session payload that
    //    session-manager-plugin consumes.
    let out = ecs.execute_command()
        .cluster(&config.cluster.name)
        .task(task_arn)
        .container(container)
        .command(&cmd.join(" "))
        .interactive(true)
        .send()
        .await?;
    let session = out.session()
        .ok_or_else(|| anyhow::anyhow!("ExecuteCommand returned no session"))?;

    // 4. Hand off to session-manager-plugin (same as tkr port-forward).
    // The plugin reads the session JSON on stdin.
    let mut child = std::process::Command::new("session-manager-plugin")
        .args([
            &serde_json::to_string(session)?,
            &config.region,
            "StartSession",
        ])
        .spawn()?;
    let status = child.wait()?;
    if !status.success() {
        anyhow::bail!("session-manager-plugin exited with status {status}");
    }
    Ok(())
}
```

The caller typically wraps this as:

```bash
tkr exec runtime -- sh
tkr exec runtime --container alloy -- ps aux
tkr exec grafana -- /bin/bash
```

**Audit trail.** ECS Exec does not write a CloudWatch session log by default. Operator access is authenticated and attributable through IAM and CloudTrail. If full session transcript logging is needed later, it should be added as an optional enhancement rather than a dependency of the core private exec path.

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

### 4b. Alloy Sidecar in Task Definitions (init-container + SSM Parameter Store pattern)

Every Tokeira task definition includes an Alloy sidecar container for metrics and logs plus a small **config init container** that fetches the Alloy config from SSM Parameter Store, injects the current ECS task ARN, and writes the rendered config into a shared task volume. This decouples Alloy configuration from task definition revisions — operators can update Alloy settings by writing a new SSM parameter value; the next task start picks up the change.

The sidecar:

- Scrapes Prometheus metrics from `localhost:{metrics_port}` on the primary container
- Forwards metrics to Mimir via remote-write (`http://mimir.<namespace>:9009/api/v1/push`)
- Reads stdout/stderr logs through the host Docker socket using `discovery.docker` plus a `com.amazonaws.ecs.task-arn` filter scoped to the current task
- Forwards logs to Loki (`http://loki.<namespace>:3100/loki/api/v1/push`)
- Uses pinned image `grafana/alloy:v1.16.0`

```rust
/// Pair of container definitions: the init container that stages the Alloy
/// config, and the Alloy sidecar that consumes it.
fn alloy_containers(service_name: &str, project: &str, region: &str, metrics_port: u16)
    -> (ContainerDefinition, ContainerDefinition)
{
    let param_path = format!("/{project}/alloy/sidecar/{service_name}");

    let init = ContainerDefinition {
        name: "alloy-config-init".into(),
        image: "amazon/aws-cli:latest".into(),
        essential: false,
        cpu: 64,
        memory_mb: 128,
        mount_points: vec![MountPoint {
            source_volume: "alloy-config".into(),
            container_path: "/etc/alloy".into(),
            read_only: false,
        }],
        // Fetch the parameter value, inject task identity, and write it to the shared volume.
        command: vec![
            "sh".into(), "-c".into(),
            format!(
                "TASK_ARN=$(curl -s \"$ECS_CONTAINER_METADATA_URI_V4/task\" \
                   | grep -o '\"TaskARN\":\"[^\"]*' | cut -d'\"' -f4) && \
                 TASK_ID=$(echo \"$TASK_ARN\" | grep -oE '[^/]+$') && \
                 aws ssm get-parameter --name {param_path} --with-decryption --region {region} \
                   --query 'Parameter.Value' --output text \
                   | sed \"s|TASK_ARN_PLACEHOLDER|$TASK_ARN|g\" \
                   | sed \"s|TASK_ID_PLACEHOLDER|$TASK_ID|g\" \
                   > /etc/alloy/config.alloy"
            ),
        ],
        ..Default::default()
    };

    let sidecar = ContainerDefinition {
        name: "alloy".into(),
        image: "grafana/alloy:v1.16.0".into(),
        essential: false,
        cpu: 128,
        memory_mb: 256,
        depends_on: vec![ContainerDependency {
            container_name: "alloy-config-init".into(),
            condition: "SUCCESS".into(),
        }],
        mount_points: vec![MountPoint {
            source_volume: "alloy-config".into(),
            container_path: "/etc/alloy".into(),
            read_only: true,
        }, MountPoint {
            source_volume: "docker-sock".into(),
            container_path: "/var/run/docker.sock".into(),
            read_only: true,
        }],
        command: vec![
            "run".into(), "--server.http.listen-addr=0.0.0.0:12345".into(),
            "/etc/alloy/config.alloy".into(),
        ],
        ..Default::default()
    };

    (init, sidecar)
}
```

The task definition declares a shared `alloy-config` volume and a Docker socket host-path volume:

```rust
task_definition.volumes.push(Volume {
    name: "alloy-config".into(),
    host: None, // ephemeral scratch volume
});
task_definition.volumes.push(Volume {
    name: "docker-sock".into(),
    host: Some(HostVolume { source_path: "/var/run/docker.sock".into() }),
});
```

**Log routing.** Primary application containers do not set `logConfiguration` in the normal task definition. They use Docker's default `json-file` log driver, and the co-located Alloy sidecar reads those logs through `/var/run/docker.sock`. Alloy scopes Docker discovery to the current ECS task by filtering on the `com.amazonaws.ecs.task-arn` Docker label after the init container injects the task ARN into the config. This matches the ECS-on-EC2 production pattern and avoids a separate log-router container.

**Config content.** The full Alloy HCL config is rendered from an Askama template at `infra apply` time (see §4d) and written to SSM by an IaC resource:

```rust
#[derive(Debug)]
pub struct AlloyParameterResource {
    pub service_name: String,
    pub project: String,
    pub config_content: String,  // rendered HCL
}

impl Resource for AlloyParameterResource {
    async fn create(&self, ctx: &ProvisionContext) -> Result<ResourceState, IacError> {
        let ssm = ctx.extension::<aws_sdk_ssm::Client>().unwrap();
        ssm.put_parameter()
            .name(format!("/{}/alloy/sidecar/{}", self.project, self.service_name))
            .value(&self.config_content)
            .r#type(ParameterType::String)
            .overwrite(true)
            .send()
            .await?;
        Ok(state)
    }
    // update/delete/describe follow the same pattern
}
```

Each service's `AlloyParameterResource` is enumerated by the `observability` module alongside Mimir/Loki/Grafana.

**IAM.**
- The **task role** needs `ssm:GetParameter` on `arn:aws:ssm:*:*:parameter/{project}/alloy/sidecar/*` because `alloy-config-init` runs as a normal task container and uses task-role credentials.
- The **execution role** does NOT need this permission for the Alloy config fetch; it is only used by the ECS agent for image pulls, secret injection, and optional log drivers.
- Writing the parameter (at `infra apply` time) is performed by the operator's credentials via the `AlloyParameterResource`, not by any task role.

**Why this pattern.** Two wins over env-var configuration:

1. **Updatable without task definition churn.** Changing scrape intervals, adding labels, or switching remote-write targets becomes `aws ssm put-parameter` + task restart, not a new task definition revision.
2. **Larger configs fit.** Alloy HCL config with multiple scrape jobs, labels, and log processors is far beyond what's sensible in env vars.

### 4c. Observability Module (`platforms/ecs/src/modules.rs`)

The observability module provisions Mimir, Loki, and Grafana as ECS services on dedicated capacity providers (`cp-mimir`, `cp-loki`, and `cp-grafana`), plus S3 buckets for metrics and log storage:

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

Each observability component requires a configuration payload mounted into the ECS task definition, and each Alloy sidecar requires a metrics/logs configuration payload. These configs are generated programmatically from `EcsConfig` values — no static YAML files. The ECS platform uses Askama templates (same pattern as the compose platform) for config content that is too verbose for inline Rust strings.

#### Alloy Sidecar Configuration

Each Tokeira task definition includes an Alloy sidecar. On ECS, the sidecar scrapes the co-located primary container on localhost, remote-writes metrics to Mimir via Service Connect, and tails task-local Docker logs through the host Docker socket using a task-ARN-scoped Docker discovery filter.

```
// Alloy sidecar config for tokeira-{service}
// Mounted from the SSM-populated alloy-config volume

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

discovery.docker "task" {
  host = "unix:///var/run/docker.sock"
}

discovery.relabel "task_logs" {
  targets = discovery.docker.task.targets

  rule {
    source_labels = ["__meta_docker_container_label_com_amazonaws_ecs_task_arn"]
    regex         = "TASK_ARN_PLACEHOLDER"
    action        = "keep"
  }
}

loki.source.docker "task" {
  host       = "unix:///var/run/docker.sock"
  targets    = discovery.relabel.task_logs.output
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
    task_id      = "TASK_ID_PLACEHOLDER",
  }
}
```

Key design decisions:
- **Static localhost target** — no service discovery needed. The sidecar always scrapes the co-located primary container.
- **Task-ARN-scoped log discovery** — Alloy can see Docker metadata through the host socket, but the relabel rule keeps only containers with the current task ARN.
- **Default Docker logging** — primary containers intentionally omit `logConfiguration` so Docker writes stdout/stderr to json-file logs that Alloy can read.
- **External labels** — `service_name`, `environment`, `project` are injected so Mimir and Loki queries can filter by service and environment.
- **15s scrape interval** — matches the autoscaler polling interval. Faster scraping wastes CPU; slower scraping delays scaling decisions.
- **Docker socket mount** — the Alloy sidecar mounts `/var/run/docker.sock` read-only. This is ECS-on-EC2 specific and is not intended to be portable to Fargate.

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
  compactor_blocks_retention_period: {{ retention_days }}d
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
- **Configurable retention** — `retention_days` from `EcsConfig.observability` (default: 30 days; can be overridden per deployment). Loki's compactor enforces retention by deleting expired chunks from S3 after a 2h grace period; Mimir's compactor applies the same retention to its blocks backend.
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
- **Anonymous auth disabled** — all access requires authentication. Grafana is accessible via `tkr port-forward` only; it is not registered with the internal ALB.

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
- Written to SSM Parameter Store and mounted through the `alloy-config-init` volume flow (Alloy sidecar)
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
        static VALID: [&str; 10] = [
            "tokeira-edge-api", "tokeira-edge-poll", "tokeira-runtime",
            "tokeira-projection", "tokeira-controller", "tokeira-autoscaler",
            "tokeira-admin",
            // Observability services — valid targets for tkr logs/port-forward/exec,
            // but NOT for tkr scale (their desired_count is managed by the
            // observability module, not the operator).
            "tokeira-mimir", "tokeira-loki", "tokeira-grafana",
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

### 6b. `tkr port-forward` — Operator Access to Private Services

Tokeira deploys into private-only subnets with no internet gateway. Operators reach Grafana, Mimir, Loki, the controller, and the edge services through AWS Systems Manager Session Manager port forwarding — no bastion, no VPN, no public load balancer.

```rust
/// Discover a running container instance in the target capacity provider
/// and open an SSM port-forwarding session to the container port.
pub async fn port_forward_ecs(
    service: &str,
    local_port: u16,
    config: &EcsConfig,
    ecs: &aws_sdk_ecs::Client,
) -> Result<()> {
    // 1. Resolve capacity provider and target port from service name.
    let (cp_name, remote_port) = service_target(service, config)?;

    // 2. Find a container instance in the capacity provider's ASG.
    //    The capacity provider is backed by an ASG; container instances
    //    inherit the `cp` attribute so we can filter.
    let instances = ecs
        .list_container_instances()
        .cluster(&config.cluster.name)
        .filter(format!("attribute:capacityProvider=={cp_name}"))
        .send()
        .await?;
    let instance_arn = instances.container_instance_arns()
        .first()
        .ok_or_else(|| anyhow::anyhow!(
            "no running container instances in {cp_name}; \
             run `tkr scale up` or check ASG health"
        ))?;
    let described = ecs
        .describe_container_instances()
        .cluster(&config.cluster.name)
        .container_instances(instance_arn)
        .send()
        .await?;
    let ec2_instance_id = described.container_instances()
        .first()
        .and_then(|ci| ci.ec2_instance_id())
        .ok_or_else(|| anyhow::anyhow!("container instance has no EC2 ID"))?;

    // 3. Invoke `aws ssm start-session` as a subprocess.
    //    session-manager-plugin handles the data-plane protocol.
    let status = std::process::Command::new("aws")
        .args([
            "ssm", "start-session",
            "--target", ec2_instance_id,
            "--document-name", "AWS-StartPortForwardingSession",
            "--parameters",
            &format!("portNumber={remote_port},localPortNumber={local_port}"),
        ])
        .status()?;

    if !status.success() {
        anyhow::bail!(
            "ssm start-session exited with status {status}; \
             ensure session-manager-plugin is installed"
        );
    }
    Ok(())
}

/// Map service name to (capacity provider, container port).
fn service_target(service: &str, _config: &EcsConfig) -> Result<(&'static str, u16)> {
    Ok(match service {
        "grafana"    => ("cp-grafana",   3000),
        "mimir"      => ("cp-mimir",     9009),
        "loki"       => ("cp-loki",      3100),
        "edge-api"   => ("cp-edge-api",  7233),
        "edge-poll"  => ("cp-edge-poll", 7234),
        "controller" => ("cp-control",   7240),
        other => anyhow::bail!("unknown service for port-forward: {other}"),
    })
}
```

**Why shell out to `aws ssm start-session`?** The SSM Session Manager data-plane protocol is a WebSocket stream with binary framing that requires the native `session-manager-plugin`. Reimplementing it in Rust would add significant complexity for no operator benefit. This matches the approach used by `dsqld` in the deploy-eks project.

**Required IAM.** Each ASG's instance profile carries the `AmazonSSMManagedInstanceCore` managed policy. The operator's credentials need `ssm:StartSession` on the target EC2 instances and `ssm:TerminateSession` / `ssm:ResumeSession` on their own session IDs.

**Required VPC endpoints for Session Manager.** Add `ssm`, `ssmmessages`, and `ec2messages` to the required VPC endpoint set so Session Manager works without internet egress.

### 6c. DSQL Mode: Managed vs Preexisting

The DSQL module follows the `effective_managed` convention from the `iac-resource-lifecycle` spec. Each resource (cluster, management endpoint, connection endpoint, runtime role, admin role) exposes a resource-level mode enum and decides its own create/delete behaviour:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DsqlClusterMode {
    Managed,
    Preexisting,
}

impl DsqlCluster {
    fn effective_managed(config_mode: DsqlClusterMode, state_mode: &str) -> bool {
        config_mode == DsqlClusterMode::Managed || state_mode == "managed"
    }
}

#[async_trait]
impl Resource for DsqlCluster {
    async fn create(&self, ctx: &ProvisionContext) -> Result<ResourceState, IacError> {
        match self.config.mode {
            DsqlClusterMode::Managed => {
                let arn = dsql_client(ctx).create_cluster().send().await?;
                Ok(ResourceState {
                    properties: json!({
                        "mode": "managed",
                        "cluster_arn": arn,
                        "endpoint": discovered_endpoint,
                    }),
                    ..
                })
            }
            DsqlClusterMode::Preexisting => {
                // Adopt — operator already supplied endpoint/ARN in config.
                Ok(ResourceState {
                    properties: json!({
                        "mode": "preexisting",
                        "cluster_arn": self.config.preexisting_arn,
                        "endpoint": self.config.endpoint,
                    }),
                    ..
                })
            }
        }
    }

    async fn delete(&self, current: &ResourceState, ctx: &ProvisionContext)
        -> Result<(), IacError>
    {
        let state_mode = current.properties.get("mode")
            .and_then(|v| v.as_str()).unwrap_or_default();
        if !Self::effective_managed(self.config.mode, state_mode) {
            // Preexisting in both config and state — never delete operator's cluster.
            return Ok(());
        }
        dsql_client(ctx).delete_cluster().arn(...).send().await?;
        Ok(())
    }
}
```

**Writeback on Managed apply.** When `dsql.mode == Managed`, each successful resource create emits a writeback entry that populates the deployment config. The design uses the same hybrid pattern as the EKS deployment: **state hydration for correctness, config writeback for operator readability**.

```rust
impl Deployment for EcsDeployment {
    /// Called on every engine construction (infra plan/apply, deploy plan/apply,
    /// scale up, schema setup). Populates empty DSQL config fields from
    /// persisted state so downstream code always has the discovered endpoint.
    /// This is the correctness path — config-file writeback is not.
    fn hydrate_config(&self, config: &EcsConfig, state: &InfraState) -> EcsConfig {
        let mut h = config.clone();
        let rid_cluster = ResourceId("dsql:cluster".into());
        let rid_conn_endpoint = ResourceId("dsql:connection-endpoint".into());
        let rid_mgmt_endpoint = ResourceId("dsql:management-endpoint".into());
        let rid_runtime_role = ResourceId("dsql:runtime-role".into());
        let rid_admin_role = ResourceId("dsql:admin-role".into());

        if let Some(rs) = state.resources.get(&rid_cluster) {
            if h.dsql.endpoint.is_none() {
                h.dsql.endpoint = prop_str(rs, "endpoint");
            }
        }
        if let Some(rs) = state.resources.get(&rid_mgmt_endpoint) {
            if h.dsql.management_endpoint_id.is_none() {
                h.dsql.management_endpoint_id = prop_str(rs, "endpoint_id");
            }
        }
        if let Some(rs) = state.resources.get(&rid_conn_endpoint) {
            if h.dsql.connection_endpoint_id.is_none() {
                h.dsql.connection_endpoint_id = prop_str(rs, "endpoint_id");
            }
        }
        if let Some(rs) = state.resources.get(&rid_runtime_role) {
            if h.dsql.runtime_role_arn.is_none() {
                h.dsql.runtime_role_arn = prop_str(rs, "role_arn");
            }
        }
        if let Some(rs) = state.resources.get(&rid_admin_role) {
            if h.dsql.admin_role_arn.is_none() {
                h.dsql.admin_role_arn = prop_str(rs, "role_arn");
            }
        }
        h
    }

    /// Called after infra apply and infra destroy. Returns (dotted_key, value)
    /// pairs the CLI persists to deployment.toml via toml_edit. Convenience
    /// only — downstream commands use hydrate_config as the source of truth.
    /// On destroy, an empty post-destroy state clears previously-written
    /// Managed DSQL values so stale references are not retained.
    fn collect_writeback(&self, config: &EcsConfig, state: &InfraState) -> Vec<(String, String)> {
        let mut entries = Vec::new();
        if config.dsql.mode == DsqlClusterMode::Managed {
            if !state.resources.contains_key(&ResourceId("dsql:cluster".into()))
                && config.dsql.endpoint.is_some()
            {
                entries.push(("dsql.endpoint".into(), String::new()));
                entries.push(("dsql.management_endpoint_id".into(), String::new()));
                entries.push(("dsql.connection_endpoint_id".into(), String::new()));
                entries.push(("dsql.runtime_role_arn".into(), String::new()));
                entries.push(("dsql.admin_role_arn".into(), String::new()));
                return entries;
            }
            let hydrated = self.hydrate_config(config, state);
            // Emit pairs only when the discovered value differs from the
            // current config (or the current config is empty).
            if let Some(ep) = hydrated.dsql.endpoint {
                if config.dsql.endpoint.as_deref() != Some(&ep) {
                    entries.push(("dsql.endpoint".into(), ep));
                }
            }
            // ... similar for management_endpoint_id, connection_endpoint_id,
            //     runtime_role_arn, admin_role_arn
        }
        entries
    }
}

fn prop_str(rs: &ResourceState, key: &str) -> Option<String> {
    rs.properties.get(key)
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_owned())
}
```

**Why both paths.** The EKS deployment uses exactly this pattern because writeback can fail in ways that don't corrupt state:
- Network blip between successful apply and `toml_edit` write → state has the endpoint, config doesn't.
- Operator re-runs `infra apply` from a fresh checkout (committed config is stale) → state hydration makes `deploy apply` work regardless.
- Parallel operators on the same deployment → whoever commits config last wins, but hydration keeps both working.

**Idempotency.** `hydrate_config(hydrate_config(c, s), s) == hydrate_config(c, s)` because each field is populated only when empty. Verified by Property 11 (new; see Correctness Properties).

**Deploy-apply guard.** When `dsql.mode == Managed` and hydration cannot fill `endpoint`, `EcsDeployment::services()` returns an error `infra apply has not run successfully; DSQL endpoint is not yet known`. This prevents downstream commands from using placeholder values after a partial or failed infra apply.

After the first `tkr infra apply` in `Managed` mode, operators see their `deployment.toml` updated in-place (via `toml_edit` preserving comments) with the discovered endpoint and ARNs. Subsequent `tkr infra plan` runs are no-ops because the state already matches.

**Preexisting mode requires complete config up front.** The config loader validates that `endpoint`, `management_endpoint_id`, `connection_endpoint_id`, `runtime_role_arn`, and `admin_role_arn` are all set when `mode = "preexisting"`. Missing fields produce a descriptive error naming the first missing path.


### 6d. Zero-Replica Staged Deployment

Services deploy at 0 replicas to avoid crash-loops before schema exists:

```
tkr infra apply          # Provision VPC, DSQL, ECS cluster, ASGs
tkr deploy apply         # Create ECS services at desired_count=0
tkr schema setup         # Run DSQL migrations
tkr scale up             # Scale services in startup order:
                         #   mimir → loki → grafana →
                         #   runtime → controller → edge-api → edge-poll
                         #   → projection → autoscaler
                         # Each service waits for ready state before next
```

Observability services (Mimir, Loki, Grafana) scale up first so the autoscaler has a metrics backend the moment it starts. Starting the autoscaler before Mimir would cause it to freeze in degraded mode until Mimir came up — noisy and indistinguishable from a real Mimir outage.

The `scale up` command reads the configured desired counts from `EcsConfig.services` and `EcsConfig.observability` and applies them in dependency order. Each service is scaled and then polled until ECS reports the desired number of running tasks with passing health checks.

## Data Models

### ECS Platform Configuration

| Section | Key Fields | Default |
|---|---|---|
| `tags` | Operator-defined custom tags | `{}` (empty — auto-generated tags always applied) |
| `cluster` | `name`, `service_connect_namespace` | `tokeira-dev`, `tokeira.local` |
| `networking` | `vpc_id`, `private_subnet_ids`, `availability_zones`, `private_dns_zone`, `optional_endpoints` | Must be provided |
| `dsql` | `mode` (default `Managed`), plus `endpoint`, `management_endpoint_id`, `connection_endpoint_id`, `runtime_role_arn`, `admin_role_arn` (all `Option<String>` — required for Preexisting, populated from state for Managed) | `mode = "managed"`; discovered fields populated via state hydration and config writeback after `infra apply` |
| `capacity_providers.edge_api` | `instance_type`, `min/max/desired_capacity` | `c8g.large`, 1/10/2 |
| `capacity_providers.edge_poll` | `instance_type`, `min/max/desired_capacity` | `r8g.large`, 1/10/2 |
| `capacity_providers.runtime` | `instance_type`, `min/max/desired_capacity`, `scale_in_protection` | `c8g.xlarge`, 1/20/2, true |
| `capacity_providers.projection` | `instance_type`, `min/max/desired_capacity` | `c8g.large`, 1/10/1 |
| `capacity_providers.control` | `instance_type`, `min/max/desired_capacity` | `c8g.large`, 1/3/1 (max=3 required for rolling-update headroom per Req 3.2.8) |
| `capacity_providers.mimir` | `instance_type`, `min/max/desired_capacity` | `r8g.large`, 1/1/1 |
| `capacity_providers.loki` | `instance_type`, `min/max/desired_capacity` | `r8g.large`, 1/1/1 |
| `capacity_providers.grafana` | `instance_type`, `min/max/desired_capacity` | `c8g.large`, 1/1/1 |
| `autoscaler` | `polling_interval_secs`, `scale_out/in_consecutive_samples`, `cooldown_secs`, `mimir_endpoint`, `staleness_threshold_secs`, `dsql_connection_budget`, `per_runtime_reserved_connections` | 15s, 2/8, 120s, 8000, 200 |
| `alb` | `name`, `listener_protocol`, `health_check_path`, `health_check_interval_secs` | `http2`, `/health`, 10s (gRPC target groups) |
| `observability` | `mimir_image`, `loki_image`, `grafana_image`, `alloy_sidecar_image`, S3 buckets, `retention_days` | Pinned versions from compose platform, 30 days |

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

**Validates: Requirements 9.3.3**

### Property 11: DSQL config hydration idempotency

*For any* valid `EcsConfig` and `InfraState`, `hydrate_config(hydrate_config(config, state), state) == hydrate_config(config, state)`. Hydration is idempotent because each empty field is populated exactly once.

**Validates: Requirements 7.5a.6**

### Property 12: Deploy-apply guard on missing DSQL endpoint

*For any* `EcsConfig` with `dsql.mode == Managed` AND `InfraState` where the DSQL cluster resource is absent, `EcsDeployment::services()` SHALL return an error. This prevents downstream commands from consuming placeholder DSQL values after a partial infra apply.

**Validates: Requirements 7.5a.3**

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
| Property 11: DSQL hydration idempotency | `platforms/ecs/src/lib.rs` | Generate random `EcsConfig` + `InfraState`, apply `hydrate_config` twice, assert equal. |
| Property 12: Deploy-apply guard | `platforms/ecs/src/lib.rs` | Generate `Managed` configs with DSQL cluster absent from state, assert `services()` returns an error. |

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
