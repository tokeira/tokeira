# Requirements Document: ECS Deployment

## Introduction

This document captures the requirements for deploying Tokeira on Amazon ECS on EC2 with private-only networking, a custom autoscaler, and a self-hosted observability stack. Currently Tokeira runs on local bare-process and Docker Compose platforms. This spec introduces the production ECS deployment: cluster and capacity provider provisioning, ECS service definitions for all Tokeira services plus the observability stack (Alloy, Mimir, Loki, Grafana), private-only VPC networking with VPC endpoints, an internal ALB for edge ingress, and a custom autoscaler service that reads Mimir and writes scaling decisions to ECS and EC2 Auto Scaling APIs.

The authoritative architecture document is [045-autoscaling-on-ecs-ec2](../../../docs/architecture/045-autoscaling-on-ecs-ec2.md). Related documents: [035-placement-and-membership](../../../docs/architecture/035-placement-and-membership.md), [060-connection-management](../../../docs/architecture/060-connection-management.md), [090-failover-and-recovery](../../../docs/architecture/090-failover-and-recovery.md).

### Key design principles

1. **Mimir decides, AWS APIs enact.** Scaling decisions come from Tokeira-native metrics in Mimir, not CloudWatch. ECS and EC2 Auto Scaling APIs are used only as capacity actuators.
2. **Private-only networking.** All application and control-plane traffic remains private. No internet gateway dependency for service control paths.
3. **Isolated scaling planes.** Edge-api, edge-poll, runtime, projection, and control services scale independently with separate capacity providers.
4. **Safe runtime scale-in.** Runtime nodes are never terminated while they own bundles. Scale-in follows a drain → relinquish → terminate protocol.
5. **Missing metrics never trigger scale-in.** Absent or stale data is unknown, not zero.
6. **DSQL connection headroom is a hard scaling envelope.** Runtime scale-out must not proceed if projected connections would exceed the safe connection budget.
7. **Autoscaler unavailability must not affect workflow correctness.** It only pauses automatic capacity changes.
8. **Zero-replica staged deployment.** Services deploy at 0 replicas to avoid crash-loops before schema exists. Workflow: `infra apply` → `deploy apply` → `schema setup` → `scale up`. Scale-up follows startup order, waiting for each service to reach ready state.
9. **Cost discipline.** Every AWS resource must justify its existence. No NAT Gateways. S3 for state, metrics, and logs — no managed Prometheus or managed Grafana. DSQL is serverless — no idle cost, but connection management is critical.
10. **No secrets in config files.** Secrets are sourced from Secrets Manager references. Config files contain only non-sensitive settings.
11. **Break-glass debug logging.** CloudWatch Logs is available only as an operator-selected action (`tkr debug logs enable`), not as a default log destination. Normal log flow is Alloy → Loki. CloudWatch is the escape hatch when Loki is unavailable.
12. **Operator empathy in error messages.** Every error surfaces what happened, why, and what to do next. Remediation hints for common failures (VPC endpoint misconfiguration, IAM permission gaps, DSQL connection limits).

### What already exists

- `tokeira-iac` crate with `Module` trait, diff/plan/apply/destroy
- `tokeira-deploy-engine` crate with `Service` trait and `Platform` trait
- `tokeira-orchestrator` crate with `Deployment` and `Ops` traits
- `tokeira-aws` crate with AWS resource implementations (VPC, security groups, VPC endpoints, ECR, IAM roles, DSQL, S3, DynamoDB)
- `tokeira-config` crate with server config and TOML loader
- `tokeira-compose` and `platforms/compose/` as the reference platform implementation
- `platforms/local/` as the bare-process platform
- `tkr` CLI with infra/deploy commands
- `shard-placement-membership` spec (controller, membership, routing — the placement layer this deployment depends on)

### What this spec does NOT cover

- Placement controller implementation (covered by `shard-placement-membership` spec)
- DSQL storage implementation (already complete)
- CI/CD pipeline
- Multi-region deployment

## Glossary

- **ECS_Cluster**: An Amazon ECS cluster that groups services and tasks for one Tokeira environment.
- **Capacity_Provider**: An EC2-backed ECS capacity provider backed by its own Auto Scaling group. Each Tokeira service plane has a dedicated capacity provider.
- **Auto_Scaling_Group**: An EC2 Auto Scaling group that manages the EC2 instances backing a capacity provider.
- **ECS_Service**: An ECS service definition that maintains a desired number of tasks (REPLICA) or one task per instance (DAEMON).
- **Task_Definition**: An ECS task definition specifying the container image, resource limits, environment, and networking for a Tokeira service.
- **VPC_Endpoint**: An AWS VPC endpoint (interface or gateway) providing private connectivity to an AWS service without traversing the internet.
- **Internal_ALB**: An internal Application Load Balancer providing L7 ingress for edge services within the VPC.
- **Autoscaler**: The `tokeira-autoscaler` service that reads Mimir metrics and writes scaling decisions to ECS and EC2 Auto Scaling APIs.
- **Scaling_Loop**: A periodic control loop in the autoscaler that evaluates metrics and adjusts capacity. Three loops: Loop A (REPLICA desired count), Loop B (runtime scale-out), Loop C (runtime retirement).
- **Runtime_Retirement**: The safe scale-in protocol for runtime nodes: choose candidate → mark DRAINING → drain bundles → clear instance protection → terminate with decrement.
- **Instance_Scale_In_Protection**: EC2 Auto Scaling instance-level protection that prevents the ASG from terminating a specific instance during scale-in.
- **Service_Connect**: ECS Service Connect for generic service-to-service connectivity and discovery.
- **Cloud_Map**: AWS Cloud Map used as discovery plumbing for runtime node endpoint registration.
- **Mimir**: Grafana Mimir, the Prometheus-compatible metrics backend that serves as the source of scaling truth.
- **Alloy_Sidecar**: A Grafana Alloy container running as a sidecar in each Tokeira task definition. Scrapes Prometheus metrics from localhost and ships logs to Loki. On ECS, implemented as an additional container in the task definition.
- **Loki**: Grafana Loki, the log aggregation backend. Receives logs from Alloy sidecars and stores them in S3.
- **Grafana**: Grafana OSS, the dashboarding and visualization service. Reads from Mimir (metrics) and Loki (logs).
- **Observability_Module**: An IaC module that provisions the Mimir, Loki, and Grafana ECS services on the `cp-control` capacity provider.
- **ECS_Platform**: The `platforms/ecs/` crate implementing the `Deployment` and `Ops` traits for ECS on EC2.
- **DSQL_Connection_Headroom**: The remaining capacity in the DSQL connection budget after accounting for current and projected connections.

## Requirements

---

## Feature 0: CLI Progress Reporting (Prerequisite)

### Requirement 0.1: IaC Progress Callbacks

**User Story:** As a Tokeira developer, I want the IaC engine to emit progress callbacks during plan/apply/destroy, so that the CLI can render live progress indicators for long-running infrastructure operations.

#### Acceptance Criteria

1. THE `ProvisionContext` in `tokeira-iac` SHALL support `set_apply_progress`, `set_wait_progress`, and `set_note_progress` callback registration.
2. THE `set_apply_progress` callback SHALL receive: action name, resource ID, resource type, current index, and total count.
3. THE `set_wait_progress` callback SHALL receive: resource ID, resource type, phase description, elapsed duration, and timeout duration.
4. THE `set_note_progress` callback SHALL receive: resource ID, resource type, and informational message.
5. THE IaC engine SHALL call `emit_apply_progress` before each resource lifecycle operation during apply/destroy.
6. THE IaC engine SHALL call `emit_wait_progress` during polling waits (e.g., waiting for a resource to become active).
7. THE IaC engine SHALL call `emit_note_progress` for informational events (e.g., "adopting existing bucket").

### Requirement 0.2: CLI Output Module Upgrade

**User Story:** As a Tokeira operator, I want the `tkr` CLI to show colored progress indicators, status tables, and spinners during infrastructure operations, so that I have clear visibility into what is happening.

#### Acceptance Criteria

1. THE `tkr` CLI output module SHALL support two output modes: `Human` (styled terminal) and `Json` (structured machine-readable).
2. THE `Human` mode SHALL use ANSI colors via the `console` crate (auto-disabled when stdout is not a TTY).
3. THE `Human` mode SHALL render an `ActionTuiHandle` with a progress bar and spinner for long-running operations, using the `indicatif` crate.
4. THE `Human` mode SHALL render change tables with colored action indicators: `+` green (create), `~` yellow (update), `-` red (delete).
5. THE `Human` mode SHALL render status messages: `✓` green (success), `⚠` yellow (warning), `✗` red (error), `→` dim (progress step).
6. THE `Json` mode SHALL produce structured JSON for every output that `Human` mode renders as styled text.
7. THE `ActionTuiHandle` SHALL fall back to plain `print_progress` lines when stdout is not a terminal.
8. THE CLI SHALL add `console` and `indicatif` as dependencies.

### Requirement 0.3: Confirmation Prompts

**User Story:** As a Tokeira operator, I want mutating commands to require explicit confirmation, so that I cannot accidentally apply or destroy infrastructure.

#### Acceptance Criteria

1. THE `tkr infra apply`, `tkr infra destroy`, `tkr deploy apply`, `tkr scale up`, `tkr scale down`, `tkr debug logs enable`, and `tkr debug logs disable` commands SHALL require interactive confirmation before proceeding.
2. THE `--yes` flag SHALL bypass the confirmation prompt for automation.
3. WHEN stdout is not a terminal and `--yes` is not provided, THE CLI SHALL reject the command with an error (preventing silent mutations in non-interactive contexts).

---

## Feature 1: ECS Platform Crate and Configuration

### Requirement 1.1: ECS Platform Configuration Model

**User Story:** As a Tokeira operator, I want a TOML-based configuration model for ECS deployments, so that I can define cluster, networking, capacity provider, and service settings in a single file.

#### Acceptance Criteria

1. THE ECS_Platform SHALL define an `EcsConfig` struct loadable from TOML via `tokeira-config`.
2. THE `EcsConfig` SHALL include sections for: cluster settings, VPC/networking, capacity providers (one per plane), service definitions (one per Tokeira service), autoscaler settings, ALB configuration, and resource tagging.
3. THE `EcsConfig` SHALL use `serde(deny_unknown_fields)` on all config structs to reject typos at parse time.
4. THE `EcsConfig` SHALL provide sensible defaults for a single-environment private-only deployment.
5. IF an `EcsConfig` contains an invalid combination of settings, THEN THE config loader SHALL return a descriptive validation error.

### Requirement 1.1a: AWS Resource Tagging

**User Story:** As a Tokeira operator, I want all AWS resources tagged consistently, so that cost allocation, ownership, and lifecycle management are traceable.

#### Acceptance Criteria

1. ALL AWS resources provisioned by the ECS_Platform SHALL carry auto-generated tags: `Name` (resource-specific), `Project` (from `project_name`), `Environment` (from `environment`), and `ManagedBy` (`tkr-cli`).
2. THE `EcsConfig` SHALL include a `[tags]` section for operator-defined custom tags that are merged with auto-generated tags on every resource.
3. WHEN auto-generated and custom tags conflict on the same key, THE custom tag SHALL take precedence.
4. THE tagging SHALL apply to: VPC resources, security groups, VPC endpoints, ALB, ECS cluster, capacity providers, ASGs, launch templates, IAM roles, S3 buckets, DSQL cluster, Cloud Map namespace, and ECS services/task definitions.

### Requirement 1.2: ECS Platform Crate Structure

**User Story:** As a Tokeira developer, I want the ECS platform to follow the same crate structure as the compose platform, so that the deployment framework is consistent across platforms.

#### Acceptance Criteria

1. THE ECS_Platform SHALL be implemented in `platforms/ecs/` with modules: `config.rs`, `modules.rs`, `services.rs`, `lib.rs`.
2. THE ECS_Platform SHALL implement the `Deployment` trait from `tokeira-orchestrator`.
3. THE ECS_Platform SHALL implement the `Ops` trait from `tokeira-orchestrator`.
4. THE ECS_Platform SHALL implement the `PlatformConfig` trait from `tokeira-orchestrator` with prototypical config generation.
5. THE ECS_Platform SHALL be registered as a new `PlatformKind::Ecs` variant in `tokeira-orchestrator`.

### Requirement 1.3: ECS Configuration TOML Round-Trip

**User Story:** As a Tokeira developer, I want the ECS configuration to round-trip through TOML serialization without loss, so that config generation and loading are consistent.

#### Acceptance Criteria

1. FOR ALL valid `EcsConfig` values, serializing to TOML and deserializing back SHALL produce an equivalent `EcsConfig`.
2. WHEN an `EcsConfig` TOML file contains unknown fields, THE config loader SHALL reject it with an error naming the unknown field.

---

## Feature 2: VPC and Private Networking Infrastructure

### Requirement 2.1: Private Subnet Provisioning

**User Story:** As a Tokeira operator, I want ECS tasks and EC2 instances to run in private subnets only, so that no application traffic traverses the public internet.

#### Acceptance Criteria

1. THE ECS_Platform SHALL provision private subnets across multiple Availability Zones.
2. THE ECS_Platform SHALL NOT provision public subnets or an internet gateway for the ECS deployment.
3. THE ECS_Platform SHALL provision subnets as IaC resources implementing the `Resource` trait from `tokeira-iac`.

### Requirement 2.2: Required VPC Endpoints

**User Story:** As a Tokeira operator, I want all required AWS service endpoints provisioned as VPC endpoints, so that ECS control-plane and data-plane traffic remains private.

#### Acceptance Criteria

1. THE ECS_Platform SHALL provision interface VPC endpoints for: `ecs`, `ecs-agent`, `ecs-telemetry` (ECS control plane).
2. THE ECS_Platform SHALL provision interface VPC endpoints for: `ecr.api`, `ecr.dkr` (ECR image pull).
3. THE ECS_Platform SHALL provision a gateway VPC endpoint for S3 (ECR layer transfer).
4. THE ECS_Platform SHALL provision an interface VPC endpoint for `autoscaling` (EC2 Auto Scaling).
5. THE ECS_Platform SHALL provision an interface VPC endpoint for `servicediscovery` (Cloud Map).
6. THE ECS_Platform SHALL provision DSQL PrivateLink endpoints (management and connection) for private DSQL connectivity.
7. WHEN optional endpoints are enabled in config (STS, KMS, Secrets Manager, SSM, CloudWatch Logs, EC2), THE ECS_Platform SHALL provision them as interface VPC endpoints.

### Requirement 2.3: Internal ALB for Edge Ingress

**User Story:** As a Tokeira operator, I want an internal Application Load Balancer for edge ingress, so that API and poll traffic can be routed to the correct edge service within the VPC.

#### Acceptance Criteria

1. THE ECS_Platform SHALL provision an internal ALB in the private subnets.
2. THE Internal_ALB SHALL have separate target groups for `tokeira-edge-api` and `tokeira-edge-poll`.
3. THE Internal_ALB SHALL support split private DNS names: `edge-api.<private-zone>` and `edge-poll.<private-zone>`.
4. THE Internal_ALB SHALL be provisioned as IaC resources implementing the `Resource` trait.

### Requirement 2.4: Security Groups

**User Story:** As a Tokeira operator, I want restrictive security groups for all ECS components, so that only necessary traffic is permitted.

#### Acceptance Criteria

1. THE ECS_Platform SHALL provision security groups for: ALB, edge services, runtime services, projection services, control services, and VPC endpoints.
2. THE security groups SHALL allow only the minimum required ingress and egress for each component.
3. THE security groups SHALL NOT contain `0.0.0.0/0` ingress rules.

---

## Feature 3: ECS Cluster and Capacity Providers

### Requirement 3.1: ECS Cluster Provisioning

**User Story:** As a Tokeira operator, I want one ECS cluster per environment, so that all Tokeira services are grouped under a single management boundary.

#### Acceptance Criteria

1. THE ECS_Platform SHALL provision one ECS_Cluster per environment.
2. THE ECS_Cluster SHALL be provisioned as an IaC resource implementing the `Resource` trait.
3. THE ECS_Cluster SHALL have Service Connect enabled with a default namespace.

### Requirement 3.2: Capacity Provider Provisioning

**User Story:** As a Tokeira operator, I want five EC2-backed capacity providers with dedicated Auto Scaling groups, so that each service plane scales independently.

#### Acceptance Criteria

1. THE ECS_Platform SHALL provision five Capacity_Providers: `cp-edge-api`, `cp-edge-poll`, `cp-runtime`, `cp-projection`, `cp-control`.
2. WHEN a Capacity_Provider is provisioned, THE ECS_Platform SHALL create a backing Auto_Scaling_Group with configurable instance type, min/max/desired capacity, and subnet placement.
3. THE `cp-runtime` Auto_Scaling_Group SHALL have instance scale-in protection enabled by default on all instances.
4. THE Capacity_Providers and Auto_Scaling_Groups SHALL be provisioned as IaC resources implementing the `Resource` trait.
5. THE ECS_Cluster SHALL be associated with all five Capacity_Providers via a cluster capacity provider strategy.

### Requirement 3.3: Auto Scaling Group Launch Configuration

**User Story:** As a Tokeira operator, I want ASG instances to be configured with the ECS agent and proper IAM roles, so that they register with the ECS cluster automatically.

#### Acceptance Criteria

1. THE Auto_Scaling_Group instances SHALL use an ECS-optimized AMI.
2. THE Auto_Scaling_Group instances SHALL have an IAM instance profile with permissions for ECS agent registration, ECR image pull, and CloudWatch Logs (if enabled).
3. THE Auto_Scaling_Group instances SHALL have user data that configures the ECS agent to join the correct ECS_Cluster.

---

## Feature 4: ECS Service Definitions

### Requirement 4.1: Edge API Service

**User Story:** As a Tokeira operator, I want the `tokeira-edge-api` service defined as a REPLICA ECS service on `cp-edge-api`, so that non-poll API traffic is handled by a dedicated fleet.

#### Acceptance Criteria

1. THE ECS_Platform SHALL define a `tokeira-edge-api` ECS_Service with REPLICA scheduling on `cp-edge-api`.
2. THE `tokeira-edge-api` Task_Definition SHALL specify the container image, gRPC port, metrics port, resource limits, and environment variables.
3. THE `tokeira-edge-api` ECS_Service SHALL be registered with the Internal_ALB target group for non-poll traffic.
4. THE `tokeira-edge-api` ECS_Service SHALL use Service_Connect for outbound service-to-service traffic.

### Requirement 4.2: Edge Poll Service

**User Story:** As a Tokeira operator, I want the `tokeira-edge-poll` service defined as a separate REPLICA ECS service on `cp-edge-poll`, so that long-poll worker traffic cannot starve normal API traffic.

#### Acceptance Criteria

1. THE ECS_Platform SHALL define a `tokeira-edge-poll` ECS_Service with REPLICA scheduling on `cp-edge-poll`.
2. THE `tokeira-edge-poll` Task_Definition SHALL specify the container image, gRPC port, metrics port, resource limits, and environment variables.
3. THE `tokeira-edge-poll` ECS_Service SHALL be registered with the Internal_ALB target group for poll traffic.
4. THE `tokeira-edge-poll` ECS_Service SHALL use Service_Connect for outbound service-to-service traffic.

### Requirement 4.3: Runtime Service

**User Story:** As a Tokeira operator, I want the `tokeira-runtime` service defined as a DAEMON ECS service on `cp-runtime`, so that exactly one runtime process runs per host for predictable resource envelopes.

#### Acceptance Criteria

1. THE ECS_Platform SHALL define a `tokeira-runtime` ECS_Service with DAEMON scheduling on `cp-runtime`.
2. THE `tokeira-runtime` Task_Definition SHALL specify the container image, internal gRPC port, metrics port, resource limits, and environment variables.
3. THE `tokeira-runtime` ECS_Service SHALL NOT have external ingress — only owner-targeted internal traffic.
4. THE `tokeira-runtime` ECS_Service SHALL register in a private Cloud_Map namespace for endpoint discovery.
5. THE `tokeira-runtime` ECS_Service SHALL use Service_Connect for outbound service-to-service traffic.

### Requirement 4.4: Projection Service

**User Story:** As a Tokeira operator, I want the `tokeira-projection` service defined as a REPLICA ECS service on `cp-projection`, so that projection workers scale independently of the runtime plane.

#### Acceptance Criteria

1. THE ECS_Platform SHALL define a `tokeira-projection` ECS_Service with REPLICA scheduling on `cp-projection`.
2. THE `tokeira-projection` Task_Definition SHALL specify the container image, internal port, metrics port, resource limits, and environment variables.
3. THE `tokeira-projection` ECS_Service SHALL use Service_Connect for service-to-service traffic.

### Requirement 4.5: Controller Service

**User Story:** As a Tokeira operator, I want the `tokeira-controller` service defined as a REPLICA ECS service on `cp-control`, so that advisory placement and routing publication are available.

#### Acceptance Criteria

1. THE ECS_Platform SHALL define a `tokeira-controller` ECS_Service with REPLICA scheduling on `cp-control` and a default desired count of 2.
2. THE `tokeira-controller` Task_Definition SHALL specify the container image, gRPC port, metrics port, resource limits, and environment variables.
3. THE `tokeira-controller` ECS_Service SHALL use Service_Connect for service-to-service traffic with a stable internal endpoint.

### Requirement 4.6: Autoscaler Service

**User Story:** As a Tokeira operator, I want the `tokeira-autoscaler` service defined as a REPLICA ECS service on `cp-control`, so that scaling decisions are made by a dedicated service.

#### Acceptance Criteria

1. THE ECS_Platform SHALL define a `tokeira-autoscaler` ECS_Service with REPLICA scheduling on `cp-control` and a default desired count of 2.
2. THE `tokeira-autoscaler` Task_Definition SHALL specify the container image, metrics port, resource limits, and environment variables.
3. THE `tokeira-autoscaler` ECS_Service SHALL have IAM permissions for: `ecs:UpdateService`, `ecs:DescribeServices`, `autoscaling:SetDesiredCapacity`, `autoscaling:DescribeAutoScalingGroups`, `autoscaling:TerminateInstanceInAutoScalingGroup`, `autoscaling:SetInstanceProtection`, `ecs:UpdateContainerInstancesState`.
4. THE `tokeira-autoscaler` ECS_Service SHALL use Service_Connect for outbound traffic to the controller and Mimir.

### Requirement 4.7: Admin Service

**User Story:** As a Tokeira operator, I want the `tokeira-admin` service defined as a REPLICA or on-demand ECS service on `cp-control`, so that schema admin and diagnostics are available.

#### Acceptance Criteria

1. THE ECS_Platform SHALL define a `tokeira-admin` ECS_Service with REPLICA scheduling on `cp-control` and a default desired count of 0 (on-demand).
2. THE `tokeira-admin` Task_Definition SHALL specify the container image, port, resource limits, and environment variables.

### Requirement 4.8: Task Definition Generation

**User Story:** As a Tokeira developer, I want task definitions generated programmatically from the ECS config, so that service manifests are stable and reproducible.

#### Acceptance Criteria

1. THE ECS_Platform SHALL generate Task_Definitions as deploy-engine `Service` manifests implementing the `Service` trait from `tokeira-deploy-engine`.
2. FOR ALL ECS services, THE generated Task_Definition SHALL include: container image reference, CPU and memory limits, port mappings, environment variables, log configuration, and health check settings.
3. THE generated manifests SHALL be stable for unchanged desired state — the same config produces the same manifest hash.

---

## Feature 5: Service Discovery and Connectivity

### Requirement 5.1: Service Connect Configuration

**User Story:** As a Tokeira operator, I want Service Connect configured for generic service-to-service traffic, so that services can discover and communicate with each other without manual endpoint management.

#### Acceptance Criteria

1. THE ECS_Platform SHALL configure a Service Connect namespace on the ECS_Cluster.
2. THE ECS_Platform SHALL enable Service Connect on all ECS services that need outbound service-to-service connectivity.
3. THE Service_Connect configuration SHALL provide stable internal endpoints for: controller, projection, and admin services.

### Requirement 5.2: Cloud Map Runtime Discovery

**User Story:** As a Tokeira operator, I want runtime tasks registered in a private Cloud Map namespace, so that the controller can discover runtime node endpoints.

#### Acceptance Criteria

1. THE ECS_Platform SHALL provision a private Cloud_Map namespace for runtime node discovery.
2. THE `tokeira-runtime` ECS_Service SHALL register each task in the Cloud_Map namespace with its private IP and port.
3. THE Cloud_Map registration SHALL be used as discovery plumbing only — the controller-published endpoint registry is authoritative for `node_id → endpoint`.

---

## Feature 6: Autoscaler Service Implementation

### Requirement 6.1: Autoscaler Binary Crate

**User Story:** As a Tokeira developer, I want a `tokeira-autoscaler` binary crate, so that the autoscaler runs as an independent service with its own entry point.

#### Acceptance Criteria

1. THE system SHALL define a `tokeira-autoscaler` library crate in `crates/tokeira-autoscaler/`.
2. THE system SHALL define an autoscaler binary entry point in `apps/tokeira-autoscaler/`.
3. THE `tokeira-autoscaler` crate SHALL depend on: `tokeira-types`, `tokeira-config`, `tokeira-storage` (for DSQL leader lease), and AWS SDK crates for ECS and Auto Scaling.

### Requirement 6.2: DSQL Leader Lease

**User Story:** As a Tokeira developer, I want the autoscaler to use a DSQL leader lease, so that only one autoscaler instance writes scaling decisions at a time.

#### Acceptance Criteria

1. WHEN an Autoscaler instance starts, THE Autoscaler SHALL attempt to acquire a leader lease in DSQL.
2. WHILE the Autoscaler holds the leader lease, THE Autoscaler SHALL renew the lease periodically before expiry.
3. IF the leader lease renewal fails, THEN THE Autoscaler SHALL stop writing scaling decisions and revert to standby.
4. WHEN a standby Autoscaler detects the leader lease has expired, THE Autoscaler SHALL attempt to acquire it.

### Requirement 6.3: Loop A — REPLICA Service Scaling

**User Story:** As a Tokeira operator, I want the autoscaler to adjust REPLICA service desired counts based on Mimir metrics, so that edge, projection, and control services scale with demand.

#### Acceptance Criteria

1. THE Autoscaler SHALL periodically query Mimir for scaling signals for each REPLICA service.
2. WHEN the Autoscaler computes a new desired count for a REPLICA service, THE Autoscaler SHALL call `ecs:UpdateService` to apply it.
3. THE Autoscaler SHALL NOT issue `ecs:UpdateService` if the current desired count already matches the target.
4. THE Autoscaler SHALL enforce per-service minimum and maximum desired count floors and ceilings from config.
5. THE Autoscaler SHALL enforce per-step maximum delta to prevent large jumps.

### Requirement 6.4: Loop B — Runtime Scale-Out

**User Story:** As a Tokeira operator, I want the autoscaler to scale out the runtime ASG based on Mimir metrics and DSQL connection headroom, so that runtime capacity grows with demand.

#### Acceptance Criteria

1. THE Autoscaler SHALL periodically query Mimir for runtime pressure signals.
2. WHEN the Autoscaler determines runtime scale-out is needed, THE Autoscaler SHALL verify DSQL_Connection_Headroom is sufficient before proceeding.
3. WHEN DSQL_Connection_Headroom is sufficient and pressure is broad saturation, THE Autoscaler SHALL call `autoscaling:SetDesiredCapacity` on the runtime Auto_Scaling_Group.
4. THE Autoscaler SHALL NOT issue `autoscaling:SetDesiredCapacity` if the current desired capacity already matches the target.
5. IF DSQL_Connection_Headroom is insufficient, THEN THE Autoscaler SHALL NOT increase runtime ASG desired capacity.
6. IF runtime pressure is hot-bundle imbalance rather than broad saturation, THEN THE Autoscaler SHALL NOT increase runtime ASG desired capacity.

### Requirement 6.5: Loop C — Runtime Retirement

**User Story:** As a Tokeira operator, I want the autoscaler to safely retire runtime nodes through a drain protocol, so that no bundles are lost during scale-in.

#### Acceptance Criteria

1. WHEN the Autoscaler determines the runtime fleet has excess capacity, THE Autoscaler SHALL request scale-in candidates from the controller via `NominateScaleInCandidates`.
2. WHEN the Autoscaler receives candidates, THE Autoscaler SHALL instruct the controller to mark selected nodes as DRAINING via `MarkNodeDraining`.
3. WHILE a runtime node is draining, THE Autoscaler SHALL monitor the node's heartbeat drain state via the controller.
4. WHEN a runtime node reports `safe-to-terminate`, THE Autoscaler SHALL set the ECS container instance to DRAINING.
5. WHEN the ECS container instance is drained, THE Autoscaler SHALL clear instance scale-in protection for the EC2 instance.
6. THE Autoscaler SHALL terminate the instance using `TerminateInstanceInAutoScalingGroup` with `ShouldDecrementDesiredCapacity=true`.
7. THE Autoscaler SHALL NOT reduce the runtime ASG desired capacity directly and allow Auto Scaling to choose an arbitrary instance.

### Requirement 6.6: Metric Freshness and Degraded Autoscaling

**User Story:** As a Tokeira operator, I want the autoscaler to treat stale or missing metrics as unknown, so that scaling decisions are safe under partial observability.

#### Acceptance Criteria

1. THE Autoscaler SHALL treat missing metric series as unknown, not as zero load.
2. WHILE Mimir metrics for a scaling input are older than the staleness threshold, THE Autoscaler SHALL block scale-in for that plane.
3. WHILE the controller snapshot is stale, THE Autoscaler SHALL block runtime scale-in.
4. WHILE DSQL connection headroom data is unknown, THE Autoscaler SHALL constrain runtime scale-out to the configured floor.
5. IF Mimir is unavailable, THEN THE Autoscaler SHALL freeze desired capacity except for explicit operator actions or emergency floor restoration.
6. WHEN some metrics are missing but available signals clearly indicate overload, THE Autoscaler SHALL allow scale-out.

### Requirement 6.7: Connection-Aware Scaling Envelope

**User Story:** As a Tokeira operator, I want DSQL connection headroom to act as a hard guardrail on runtime scale-out, so that adding hosts cannot create a connection death spiral.

#### Acceptance Criteria

1. THE Autoscaler SHALL compute the effective maximum runtime host count as: `min(configured_max, floor(dsql_connection_budget / per_runtime_reserved_connections), floor(dsql_new_connection_rate_budget / per_runtime_startup_connection_rate))`.
2. THE Autoscaler SHALL NOT scale the runtime ASG beyond the effective maximum.
3. THE connection budget parameters SHALL be configurable in the autoscaler config.

### Requirement 6.8: AWS Actuator Reconciliation

**User Story:** As a Tokeira developer, I want the autoscaler to maintain desired state and reconcile on each loop, so that scaling decisions are idempotent and resilient to transient failures.

#### Acceptance Criteria

1. THE Autoscaler SHALL maintain a desired-state map: `service_name → desired_count` and `asg_name → desired_capacity`.
2. THE Autoscaler SHALL NOT issue redundant API calls when the current state already matches the desired state.
3. WHEN an AWS API call is throttled, THE Autoscaler SHALL back off with exponential retry.
4. THE Autoscaler SHALL record every scaling decision with input metrics and reason for auditability.

### Requirement 6.9: Control Loop Timing

**User Story:** As a Tokeira operator, I want configurable control loop timing with asymmetric scale-out/scale-in behavior, so that the system reacts quickly to load increases but slowly to load decreases.

#### Acceptance Criteria

1. THE Autoscaler SHALL use a configurable polling interval (default: 15–30 seconds).
2. THE Autoscaler SHALL require fewer consecutive samples for scale-out (default: 1–2) than for scale-in (default: 5–10).
3. THE Autoscaler SHALL never scale from a single sample for scale-in.
4. THE Autoscaler SHALL enforce configurable cooldown periods after scaling actions.

---

## Feature 7: IaC Module Composition

### Requirement 7.1: Networking Module

**User Story:** As a Tokeira developer, I want a networking IaC module that provisions VPC, subnets, security groups, and VPC endpoints, so that the network foundation is managed as infrastructure.

#### Acceptance Criteria

1. THE ECS_Platform SHALL define a `networking` IaC module implementing the `Module` trait.
2. THE `networking` module SHALL depend on the `remote-state` module.
3. THE `networking` module SHALL enumerate resources for: subnets, security groups, VPC endpoints, and the internal ALB.
4. THE `networking` module resources SHALL implement the `Resource` trait with `create`, `update`, `delete`, `describe`, and `diff` methods.

### Requirement 7.2: Cluster Module

**User Story:** As a Tokeira developer, I want a cluster IaC module that provisions the ECS cluster, capacity providers, and ASGs, so that compute infrastructure is managed as infrastructure.

#### Acceptance Criteria

1. THE ECS_Platform SHALL define a `cluster` IaC module implementing the `Module` trait.
2. THE `cluster` module SHALL depend on the `networking` module.
3. THE `cluster` module SHALL enumerate resources for: ECS cluster, five capacity providers, five Auto Scaling groups, launch templates, and IAM instance profiles.
4. THE `cluster` module resources SHALL implement the `Resource` trait.

### Requirement 7.3: Services Module

**User Story:** As a Tokeira developer, I want a services IaC module that provisions ECS service definitions and task definitions, so that service infrastructure is managed as infrastructure.

#### Acceptance Criteria

1. THE ECS_Platform SHALL define a `services` IaC module implementing the `Module` trait.
2. THE `services` module SHALL depend on the `observability` module (application services include Alloy sidecars that need Mimir and Loki endpoints to be available).
3. THE `services` module SHALL enumerate resources for: seven ECS service definitions, seven task definitions, Service Connect configuration, and Cloud Map namespace.
4. THE `services` module resources SHALL implement the `Resource` trait.

### Requirement 7.4: Module Dependency Ordering

**User Story:** As a Tokeira developer, I want IaC modules to have explicit dependency ordering, so that infrastructure is provisioned in the correct sequence.

#### Acceptance Criteria

1. THE module dependency graph SHALL be: `remote-state` → `networking` → `dsql` → `cluster` → `observability` → `services`.
2. THE module dependency graph SHALL be a DAG with no cycles.
3. THE `infra_modules` method SHALL return modules filtered by `ModuleSelection`.

### Requirement 7.4a: Remote-State Module

**User Story:** As a Tokeira developer, I want the remote-state module to provision a shared S3 bucket with safety guarantees, so that IaC state is durable, protected from accidental deletion, and shareable across environments.

#### Acceptance Criteria

1. THE ECS_Platform SHALL define a `remote-state` IaC module implementing the `Module` trait with no dependencies.
2. THE `remote-state` module SHALL provision a `RemoteStateBucket` resource from a shared location under `platforms/` (shared across all AWS-backed platforms) with shared-bucket lifecycle semantics.
3. THE `RemoteStateBucket` SHALL enforce S3 versioning on every create and update.
4. THE `RemoteStateBucket` SHALL enforce a public access block (all four settings enabled) on create.
5. THE `RemoteStateBucket` SHALL apply a bucket policy with `Deny` on `s3:DeleteObject` and `s3:DeleteObjectVersion` scoped to `{key_prefix}/snapshots/*` to prevent accidental state snapshot deletion.
6. IF the bucket already exists (`BucketAlreadyOwnedByYou`), THE `RemoteStateBucket` SHALL adopt it without error and mark `managed_snapshot_policy = false` to avoid overwriting an existing policy.
7. THE `RemoteStateBucket` `delete()` SHALL be a no-op — the state bucket outlives any single deployment.
8. THE `RemoteStateBucket` `diff()` SHALL ignore tag drift (shared bucket may be tagged by other projects). Only versioning and snapshot policy drift SHALL trigger updates.

### Requirement 7.5: DSQL Module

**User Story:** As a Tokeira developer, I want a DSQL IaC module that provisions the Aurora DSQL cluster and related resources, so that the persistence layer is managed as infrastructure alongside the compute layer.

#### Acceptance Criteria

1. THE ECS_Platform SHALL define a `dsql` IaC module implementing the `Module` trait.
2. THE `dsql` module SHALL depend on the `networking` module (DSQL PrivateLink endpoints require VPC resources).
3. THE `dsql` module SHALL enumerate resources for: DSQL cluster, DSQL PrivateLink endpoints (management and connection), and IAM authentication roles for runtime and admin access.
4. THE `dsql` module resources SHALL implement the `Resource` trait.

---

## Feature 8: Observability Stack

### Requirement 9.1: Alloy Sidecar in Task Definitions

**User Story:** As a Tokeira operator, I want an Alloy sidecar container in each Tokeira task definition, so that metrics are scraped from localhost and logs are shipped to Loki without requiring a separate log driver.

#### Acceptance Criteria

1. EACH Tokeira ECS Task_Definition (edge-api, edge-poll, runtime, projection, controller, autoscaler, admin) SHALL include an Alloy_Sidecar container alongside the primary application container.
2. THE Alloy_Sidecar SHALL scrape Prometheus metrics from the primary container's metrics port on `localhost`.
3. THE Alloy_Sidecar SHALL forward scraped metrics to the Mimir remote-write endpoint (`/api/v1/push`).
4. THE Alloy_Sidecar SHALL collect container stdout/stderr logs and ship them to the Loki push endpoint (`/loki/api/v1/push`).
5. THE Alloy_Sidecar SHALL be configured via environment variables: `MIMIR_REMOTE_WRITE_URL`, `LOKI_WRITE_URL`, and `METRICS_SCRAPE_TARGET` (localhost:metrics_port).
6. THE Alloy_Sidecar SHALL use a pinned image version matching the compose platform (`grafana/alloy:v1.16.0`).
7. THE Alloy_Sidecar SHALL have resource limits configured separately from the primary container (CPU and memory).

### Requirement 9.2: Mimir ECS Service

**User Story:** As a Tokeira operator, I want Mimir deployed as an ECS service, so that the autoscaler and Grafana have a Prometheus-compatible metrics backend.

#### Acceptance Criteria

1. THE ECS_Platform SHALL define a `tokeira-mimir` ECS_Service with REPLICA scheduling on `cp-control`.
2. THE `tokeira-mimir` Task_Definition SHALL specify the Mimir container image (pinned: `grafana/mimir:3.0.6`), gRPC and HTTP ports, resource limits, and S3 storage configuration.
3. THE `tokeira-mimir` ECS_Service SHALL use Service_Connect for service-to-service traffic with a stable internal endpoint (`mimir.tokeira.local`).
4. THE `tokeira-mimir` ECS_Service SHALL have an IAM role with S3 read/write permissions for the metrics storage bucket.
5. THE Mimir configuration SHALL use S3 as the long-term storage backend (single-binary mode).

### Requirement 9.3: Loki ECS Service

**User Story:** As a Tokeira operator, I want Loki deployed as an ECS service, so that logs shipped by Alloy sidecars are stored and queryable.

#### Acceptance Criteria

1. THE ECS_Platform SHALL define a `tokeira-loki` ECS_Service with REPLICA scheduling on `cp-control`.
2. THE `tokeira-loki` Task_Definition SHALL specify the Loki container image (pinned: `grafana/loki:3.7.1`), HTTP port, resource limits, and S3 storage configuration.
3. THE `tokeira-loki` ECS_Service SHALL use Service_Connect for service-to-service traffic with a stable internal endpoint (`loki.tokeira.local`).
4. THE `tokeira-loki` ECS_Service SHALL have an IAM role with S3 read/write permissions for the log storage bucket.
5. THE Loki configuration SHALL use S3 as the long-term storage backend (single-binary mode).

### Requirement 9.4: Grafana ECS Service

**User Story:** As a Tokeira operator, I want Grafana deployed as an ECS service, so that I can visualize metrics and logs through pre-provisioned dashboards.

#### Acceptance Criteria

1. THE ECS_Platform SHALL define a `tokeira-grafana` ECS_Service with REPLICA scheduling on `cp-control`.
2. THE `tokeira-grafana` Task_Definition SHALL specify the Grafana container image (pinned: `grafana/grafana-oss:12.4.3`), HTTP port, resource limits, and data source configuration.
3. THE `tokeira-grafana` ECS_Service SHALL be pre-configured with Mimir and Loki as data sources.
4. THE `tokeira-grafana` ECS_Service SHALL be accessible via the internal ALB or a dedicated internal endpoint.
5. THE Grafana admin credentials SHALL be sourced from Secrets Manager or environment variables, not hardcoded.

### Requirement 9.5: Observability IaC Module

**User Story:** As a Tokeira developer, I want an observability IaC module that provisions Mimir, Loki, and Grafana services, so that the observability stack is managed as infrastructure alongside the application services.

#### Acceptance Criteria

1. THE ECS_Platform SHALL define an `observability` IaC module implementing the `Module` trait.
2. THE `observability` module SHALL depend on the `cluster` module (observability services need the ECS cluster and Service Connect namespace to be provisioned first).
3. THE `observability` module SHALL enumerate resources for: S3 buckets for Mimir and Loki storage, IAM roles for S3 access, Mimir/Loki/Grafana task definitions and ECS services.
4. THE `observability` module resources SHALL implement the `Resource` trait.

### Requirement 9.6: Observability Configuration

**User Story:** As a Tokeira operator, I want observability stack settings in the ECS config, so that I can customize image versions, resource limits, and storage settings.

#### Acceptance Criteria

1. THE `EcsConfig` SHALL include an `observability` section with fields for: Mimir image and resource limits, Loki image and resource limits, Grafana image and resource limits, Alloy sidecar image and resource limits, S3 bucket names for metrics and log storage.
2. THE `observability` section SHALL have sensible defaults matching the pinned versions from the compose platform.
3. THE `observability` section SHALL be optional — if omitted, the observability stack is not deployed, but Alloy sidecars are still included in task definitions (they will fail to connect until Mimir/Loki are available).

---

## Feature 9: CLI Integration

### Requirement 9.1: Platform Kind Registration

**User Story:** As a Tokeira operator, I want to select the ECS platform via the `tkr` CLI, so that I can manage ECS deployments using the same commands as other platforms.

#### Acceptance Criteria

1. THE `PlatformKind` enum SHALL include an `Ecs` variant.
2. WHEN the operator selects the ECS platform, THE `tkr` CLI SHALL load `EcsConfig` from the deployment TOML file.
3. THE `tkr` CLI SHALL support `tkr infra plan`, `tkr infra apply`, `tkr infra destroy` for ECS infrastructure.
4. THE `tkr` CLI SHALL support `tkr deploy plan`, `tkr deploy apply` for ECS service deployment.

### Requirement 9.2: Prototypical Config Generation

**User Story:** As a Tokeira operator, I want `tkr init` to generate a prototypical ECS deployment config, so that I have a starting point for configuration.

#### Acceptance Criteria

1. WHEN the operator runs `tkr init --platform ecs`, THE CLI SHALL generate a prototypical `deployment.toml` with ECS-specific defaults.
2. THE prototypical config SHALL include all required sections with documented defaults.
3. THE prototypical config SHALL include a prototypical `tokeirad.toml` server config for DSQL storage.

### Requirement 9.3: ECS Operations

**User Story:** As a Tokeira operator, I want `tkr` operations commands to work with the ECS platform, so that I can scale, view logs, and manage services.

#### Acceptance Criteria

1. THE `tkr scale up` and `tkr scale down` commands SHALL work with ECS services by calling `ecs:UpdateService`.
2. THE `tkr logs` command SHALL retrieve logs from ECS tasks via Loki query (primary) or ECS task log retrieval (fallback).
3. THE `tkr` CLI SHALL validate service names against the set of valid ECS services.

### Requirement 9.4: Break-Glass Debug Logging

**User Story:** As a Tokeira operator, I want to enable CloudWatch Logs as a break-glass debug action, so that I can diagnose issues when the Loki pipeline is unavailable.

#### Acceptance Criteria

1. THE `tkr debug logs enable` command SHALL update ECS task definitions to add a CloudWatch Logs log driver alongside the Alloy sidecar, and trigger a rolling deployment.
2. THE `tkr debug logs disable` command SHALL remove the CloudWatch Logs log driver from task definitions and trigger a rolling deployment.
3. CloudWatch Logs SHALL NOT be enabled by default — the normal log flow is Alloy → Loki.
4. BOTH `tkr debug logs enable` and `tkr debug logs disable` SHALL show the operator what will change and require explicit confirmation before mutation.
5. THE CloudWatch Logs VPC endpoint SHALL be provisioned as an optional endpoint (enabled via `optional_endpoints.cloudwatch_logs` in config) so that debug logging works without internet access.

### Requirement 9.5: Zero-Replica Staged Deployment

**User Story:** As a Tokeira operator, I want services to deploy at zero replicas initially, so that I can run schema setup before scaling up and avoid crash-loops.

#### Acceptance Criteria

1. WHEN `tkr deploy apply` creates ECS services for the first time, THE services SHALL be created with desired count 0.
2. THE `tkr schema setup` command SHALL run DSQL schema migrations against the provisioned DSQL cluster.
3. THE `tkr scale up` command SHALL scale services in startup order, waiting for each to reach ready state before proceeding to the next: runtime → controller → edge-api → edge-poll → projection → autoscaler.
4. THE `tkr scale up` command SHALL show the operator the planned scaling actions and require confirmation.
